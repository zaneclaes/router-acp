//! Background upkeep for the shared state DB: retention prune, finished-tool
//! detail trim, incremental vacuum and WAL checkpoint.
//!
//! Every `router-acp serve` process on a box shares one SQLite file. Each one
//! ticks every `TICK_EVERY`, but only the holder of the `maintenance_lease`
//! row does any work, so ~30 routers never vacuum at once. A dead holder's
//! lease expires after `LEASE_TTL` and the next ticking router takes it.
//!
//! The worker owns its own connection (not the process-wide state mutex) and
//! waits at most `DEFERRABLE_BUSY_WAIT` for the write lock. Every write is one
//! short statement; a busy database ends the tick and the work resumes on the
//! next one. A tick stops after `TICK_BUDGET`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use crate::state::{BUSY_TIMEOUT, DEFERRABLE_BUSY_WAIT, Retention, StateFile, is_busy};

pub const TICK_EVERY: Duration = Duration::from_secs(5 * 60);
const LEASE_TTL: Duration = Duration::from_secs(15 * 60);
const TICK_BUDGET: Duration = Duration::from_secs(2);
/// Writers waiting on the lock get a turn between batches.
const BATCH_PAUSE: Duration = Duration::from_millis(50);
const SESSIONS_PER_BATCH: i64 = 20;
const LOG_ROWS_PER_BATCH: i64 = 5_000;
const CHILD_ROWS_PER_BATCH: i64 = 2_000;
const TRIM_ROWS_PER_BATCH: i64 = 2_000;
/// Vacuum starts once this many pages (10 MB at 4 KB) are free...
const VACUUM_FREE_PAGES: i64 = 2_560;
/// ...and returns them to the filesystem this many (8 MB) per transaction.
const VACUUM_STEP_PAGES: i64 = 2_048;
/// A WAL past this size is truncated, not just checkpointed, once quiet.
const TRUNCATE_WAL_BYTES: u64 = 64 * 1024 * 1024;
/// "Quiet": no `session_log` row written for this long.
const QUIET_SECS: i64 = 60;
/// `compact` gives up if another router holds the write lock this long.
const COMPACT_LOCK_WAIT: Duration = Duration::from_secs(5);

/// Per-session child tables, deleted before their `sessions` row so the
/// cascade never turns one old session into one long transaction. Each
/// batch re-checks that the session is still expired.
const CHILD_DELETES: [(&str, i64); 3] = [
    (
        "DELETE FROM session_log WHERE id IN (
             SELECT id FROM session_log WHERE router_session_id = ?1 LIMIT ?3)
         AND EXISTS (SELECT 1 FROM sessions WHERE router_session_id = ?1 AND updated_at < ?2)",
        LOG_ROWS_PER_BATCH,
    ),
    (
        "DELETE FROM tool_calls WHERE rowid IN (
             SELECT rowid FROM tool_calls WHERE router_session_id = ?1 LIMIT ?3)
         AND EXISTS (SELECT 1 FROM sessions WHERE router_session_id = ?1 AND updated_at < ?2)",
        CHILD_ROWS_PER_BATCH,
    ),
    (
        "DELETE FROM llm_requests WHERE rowid IN (
             SELECT rowid FROM llm_requests WHERE router_session_id = ?1 LIMIT ?3)
         AND EXISTS (SELECT 1 FROM sessions WHERE router_session_id = ?1 AND updated_at < ?2)",
        CHILD_ROWS_PER_BATCH,
    ),
];

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// What one held tick did. Stored as JSON in `maintenance_lease.last_result`.
#[derive(Debug, Default, Clone, PartialEq, serde::Serialize)]
pub struct TickReport {
    pub sessions_pruned: usize,
    pub tool_details_trimmed: usize,
    pub pages_vacuumed: i64,
    /// `passive`, `truncate`, or `busy` (a writer kept the checkpoint short).
    pub checkpoint: Option<&'static str>,
    /// Another connection held the lock; the rest waits for the next tick.
    pub deferred: bool,
    pub error: Option<String>,
}

/// One router's maintenance worker.
pub struct Maintenance {
    path: PathBuf,
    retention: Retention,
    holder: String,
    conn: Option<Connection>,
    trim_cursor: i64,
    warned_no_vacuum: bool,
}

impl Maintenance {
    pub fn new(path: PathBuf, retention: Retention) -> Self {
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        Self {
            path,
            retention,
            holder: format!("{}-{started}", std::process::id()),
            conn: None,
            trim_cursor: 0,
            warned_no_vacuum: false,
        }
    }

    /// Run one tick at `now`. `None` when another router holds the lease or
    /// the lease could not be read.
    pub fn tick_at(&mut self, now: u64, budget: Duration) -> Option<TickReport> {
        if self.conn.is_none() {
            let opened = StateFile::open_conn(&self.path)
                .and_then(|conn| conn.busy_timeout(DEFERRABLE_BUSY_WAIT).map(|()| conn));
            match opened {
                Ok(conn) => self.conn = Some(conn),
                Err(err) => {
                    tracing::warn!(%err, "state maintenance cannot open the state DB");
                    return None;
                }
            }
        }
        let conn = self.conn.as_ref()?;
        match take_lease(conn, &self.holder, now) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(err) => {
                if !is_busy(&err) {
                    tracing::warn!(%err, "state maintenance lease check failed");
                }
                return None;
            }
        }
        let deadline = Instant::now() + budget;
        let mut report = TickReport::default();
        let steps = prune_expired(conn, self.retention.cutoff(now), Some(deadline))
            .map(|n| report.sessions_pruned = n)
            .and_then(|()| trim_tool_details(conn, deadline, &mut self.trim_cursor))
            .map(|n| report.tool_details_trimmed = n)
            .and_then(|()| incremental_vacuum(conn, deadline, &mut self.warned_no_vacuum))
            .map(|n| report.pages_vacuumed = n);
        if let Err(err) = steps {
            note_error(&mut report, err);
        }
        // A checkpoint never blocks writers, so it runs even after a busy step.
        match checkpoint(conn, &self.path, now as i64) {
            Ok(mode) => report.checkpoint = Some(mode),
            Err(err) => note_error(&mut report, err),
        }
        let result = serde_json::to_string(&report).unwrap_or_default();
        let _ = conn.execute(
            "UPDATE maintenance_lease SET last_tick_at = ?2, last_result = ?3
             WHERE id = 1 AND holder = ?1",
            params![self.holder, now as i64, result],
        );
        if report.error.is_some() {
            tracing::warn!(?report, "state maintenance tick failed");
        } else {
            tracing::debug!(?report, "state maintenance tick");
        }
        Some(report)
    }
}

fn note_error(report: &mut TickReport, err: rusqlite::Error) {
    if is_busy(&err) {
        report.deferred = true;
    } else {
        report.error = Some(err.to_string());
    }
}

/// Tick every `every` until aborted, each tick on a blocking thread.
pub fn spawn(path: PathBuf, retention: Retention, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut worker = Maintenance::new(path, retention);
        loop {
            tokio::time::sleep(every).await;
            let ticked = tokio::task::spawn_blocking(move || {
                worker.tick_at(now_epoch(), TICK_BUDGET);
                worker
            })
            .await;
            match ticked {
                Ok(back) => worker = back,
                Err(_) => return,
            }
        }
    })
}

/// Take or renew the lease. True when this process holds it.
fn take_lease(conn: &Connection, holder: &str, now: u64) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "INSERT INTO maintenance_lease (id, holder, expires_at) VALUES (1, ?1, ?2)
         ON CONFLICT(id) DO UPDATE SET holder = excluded.holder, expires_at = excluded.expires_at
         WHERE maintenance_lease.holder = excluded.holder OR maintenance_lease.expires_at <= ?3",
        params![holder, (now + LEASE_TTL.as_secs()) as i64, now as i64],
    )?;
    Ok(changed == 1)
}

fn out_of_time(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|d| Instant::now() >= d)
}

/// Pause between batches, but only on the paced (maintenance) path.
fn pause(deadline: Option<Instant>) {
    if deadline.is_some() {
        std::thread::sleep(BATCH_PAUSE);
    }
}

/// Delete sessions idle since before `cutoff` with every child row, in
/// small batches. `deadline` paces and bounds the work; `None` runs it all.
pub fn prune_expired(
    conn: &Connection,
    cutoff: i64,
    deadline: Option<Instant>,
) -> rusqlite::Result<usize> {
    // An event the host never accepted within the history window is moot.
    conn.execute(
        "DELETE FROM hook_outbox WHERE created_at < ?1",
        params![cutoff],
    )?;
    let mut pruned = 0;
    'batches: while !out_of_time(deadline) {
        let ids: Vec<String> = conn
            .prepare(
                "SELECT router_session_id FROM sessions
                 WHERE updated_at IS NOT NULL AND updated_at < ?1 LIMIT ?2",
            )?
            .query_map(params![cutoff, SESSIONS_PER_BATCH], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        if ids.is_empty() {
            break;
        }
        for id in &ids {
            for (sql, limit) in CHILD_DELETES {
                loop {
                    if out_of_time(deadline) {
                        break 'batches;
                    }
                    let deleted = conn.execute(sql, params![id, cutoff, limit])?;
                    pause(deadline);
                    if deleted < limit as usize {
                        break;
                    }
                }
            }
            pruned += conn.execute(
                "DELETE FROM sessions WHERE router_session_id = ?1 AND updated_at < ?2",
                params![id, cutoff],
            )?;
        }
    }
    if pruned > 0 {
        tracing::info!(pruned, "pruned sessions past the history window");
    }
    Ok(pruned)
}

/// Drop `detail` from finished tool calls written before it was dropped at
/// write time. The scan is a read; only the matched rows are written.
/// `cursor` is the rowid already scanned, so a finished backfill costs
/// nothing on later ticks. (A row an older router binary finishes below the
/// cursor keeps its detail until the next process restart rescans.)
fn trim_tool_details(
    conn: &Connection,
    deadline: Instant,
    cursor: &mut i64,
) -> rusqlite::Result<usize> {
    let mut trimmed = 0;
    while Instant::now() < deadline {
        let end: i64 = conn.query_row(
            "SELECT COALESCE(MAX(rowid), 0) FROM tool_calls",
            [],
            |row| row.get(0),
        )?;
        let ids: Vec<i64> = conn
            .prepare(
                "SELECT rowid FROM tool_calls
                 WHERE rowid > ?1 AND rowid <= ?2 AND completed_at IS NOT NULL AND detail IS NOT NULL
                 ORDER BY rowid LIMIT ?3",
            )?
            .query_map(params![*cursor, end, TRIM_ROWS_PER_BATCH], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        if !ids.is_empty() {
            trimmed += conn.execute(
                "UPDATE tool_calls SET detail = NULL
                 WHERE rowid IN (SELECT value FROM json_each(?1)) AND completed_at IS NOT NULL",
                params![serde_json::to_string(&ids).unwrap_or_default()],
            )?;
        }
        if (ids.len() as i64) < TRIM_ROWS_PER_BATCH {
            *cursor = end;
            break;
        }
        *cursor = *ids.last().unwrap_or(&end);
        std::thread::sleep(BATCH_PAUSE);
    }
    Ok(trimmed)
}

/// Return free pages to the filesystem in small steps. Needs
/// `auto_vacuum = INCREMENTAL`, which only `compact` can switch on.
fn incremental_vacuum(
    conn: &Connection,
    deadline: Instant,
    warned: &mut bool,
) -> rusqlite::Result<i64> {
    let mode: i64 = conn.pragma_query_value(None, "auto_vacuum", |row| row.get(0))?;
    if mode != 2 {
        if !*warned {
            *warned = true;
            tracing::info!(
                "state DB auto_vacuum is not incremental; freed pages stay in the file \
                 until `router-acp state-compact` runs"
            );
        }
        return Ok(0);
    }
    let freelist = |conn: &Connection| -> rusqlite::Result<i64> {
        conn.pragma_query_value(None, "freelist_count", |row| row.get(0))
    };
    let mut free = freelist(conn)?;
    if free <= VACUUM_FREE_PAGES {
        return Ok(0);
    }
    let start = free;
    while free > 0 && Instant::now() < deadline {
        // Each step of the statement frees pages; run it to completion.
        let mut stmt = conn.prepare(&format!("PRAGMA incremental_vacuum({VACUUM_STEP_PAGES})"))?;
        let mut rows = stmt.query([])?;
        while rows.next()?.is_some() {}
        let after = freelist(conn)?;
        if after >= free {
            break;
        }
        free = after;
        std::thread::sleep(BATCH_PAUSE);
    }
    Ok(start - free)
}

fn wal_path(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push("-wal");
    PathBuf::from(name)
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Checkpoint the WAL without blocking writers; truncate it only when it
/// has grown large and no session has logged for `QUIET_SECS`.
fn checkpoint(conn: &Connection, db: &Path, now: i64) -> rusqlite::Result<&'static str> {
    let last_write: i64 =
        conn.query_row("SELECT COALESCE(MAX(ts), 0) FROM session_log", [], |row| {
            row.get(0)
        })?;
    let truncate = file_len(&wal_path(db)) > TRUNCATE_WAL_BYTES && now - last_write >= QUIET_SECS;
    let (sql, mode) = if truncate {
        ("PRAGMA wal_checkpoint(TRUNCATE)", "truncate")
    } else {
        ("PRAGMA wal_checkpoint(PASSIVE)", "passive")
    };
    let busy: i64 = conn.query_row(sql, [], |row| row.get(0))?;
    Ok(if busy != 0 { "busy" } else { mode })
}

/// Cheap size facts for `router-acp state-stats`: file sizes and pragmas,
/// never row counts (a COUNT(*) on a 10 GB log takes most of a minute).
pub fn stats(db: &Path) -> rusqlite::Result<serde_json::Value> {
    let conn = Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let pragma = |name: &str| -> rusqlite::Result<i64> {
        conn.pragma_query_value(None, name, |row| row.get(0))
    };
    let page_size = pragma("page_size")?;
    let freelist = pragma("freelist_count")?;
    let auto_vacuum = match pragma("auto_vacuum")? {
        1 => "full",
        2 => "incremental",
        _ => "none",
    };
    let lease = conn
        .query_row(
            "SELECT holder, expires_at, last_tick_at, last_result FROM maintenance_lease WHERE id = 1",
            [],
            |row| {
                let result: Option<String> = row.get(3)?;
                Ok(serde_json::json!({
                    "holder": row.get::<_, String>(0)?,
                    "expires_at": row.get::<_, i64>(1)?,
                    "last_tick_at": row.get::<_, Option<i64>>(2)?,
                    "last_result": result.and_then(|r| serde_json::from_str::<serde_json::Value>(&r).ok()),
                }))
            },
        )
        .optional()
        // A DB no maintenance-aware router has opened has no lease table.
        .unwrap_or(None);
    Ok(serde_json::json!({
        "db_bytes": file_len(db),
        "wal_bytes": file_len(&wal_path(db)),
        "page_size": page_size,
        "page_count": pragma("page_count")?,
        "freelist_count": freelist,
        "freelist_bytes": freelist * page_size,
        "auto_vacuum": auto_vacuum,
        "maintenance": lease,
    }))
}

/// One-off: switch the DB to incremental auto_vacuum with a full VACUUM, then
/// truncate the WAL. Holds the write lock for the whole rewrite. Returns the
/// file + WAL bytes before and after.
pub fn compact(db: &Path) -> rusqlite::Result<(u64, u64)> {
    let size = || file_len(db) + file_len(&wal_path(db));
    let before = size();
    let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    conn.busy_timeout(COMPACT_LOCK_WAIT)?;
    conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
    conn.execute_batch("VACUUM")?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
    drop(conn);
    Ok((before, size()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{PersistedSession, StateFile};

    const DAY: u64 = 24 * 60 * 60;

    fn session() -> PersistedSession {
        PersistedSession {
            agent: "claude".into(),
            model: "m".into(),
            downstream_session_id: "d".into(),
            cwd: PathBuf::from("/"),
            kind: "primary".into(),
            ..Default::default()
        }
    }

    fn open(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn
    }

    /// A session with `rows` log rows, tool calls and requests, last
    /// active at `updated_at`.
    fn seed(path: &Path, id: &str, rows: usize, updated_at: i64) {
        let state = StateFile::load(path, Retention::default());
        state.upsert(id.into(), session());
        let conn = open(path);
        conn.execute_batch("BEGIN").unwrap();
        let payload = "x".repeat(2_000);
        for n in 0..rows {
            conn.execute(
                "INSERT INTO session_log (router_session_id, ts, kind, role, summary, detail)
                 VALUES (?1, ?2, 'tool_call', 'tool', 's', ?3)",
                params![id, updated_at, payload],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO tool_calls (router_session_id, tool_call_id, title, status,
                     started_at, updated_at, completed_at, detail)
                 VALUES (?1, ?2, 't', 'completed', ?3, ?3, ?3, ?4)",
                params![id, format!("tool-{n}"), updated_at, payload],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO llm_requests (request_id, router_session_id, agent, protocol,
                     endpoint, pinned_model, model, routing_reason, routing_event, started_at)
                 VALUES (?1, ?2, 'a', 'p', 'e', 'm', 'm', 'r', 'steady', ?3)",
                params![format!("{id}-req-{n}"), id, updated_at],
            )
            .unwrap();
        }
        conn.execute(
            "UPDATE sessions SET updated_at = ?2 WHERE router_session_id = ?1",
            params![id, updated_at],
        )
        .unwrap();
        conn.execute_batch("COMMIT").unwrap();
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn rows_for(conn: &Connection, id: &str) -> i64 {
        ["session_log", "tool_calls", "llm_requests", "sessions"]
            .iter()
            .map(|table| {
                conn.query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE router_session_id = ?1"),
                    params![id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
            })
            .sum()
    }

    #[test]
    fn prune_removes_an_expired_session_and_every_child_row_across_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let now = 100 * DAY;
        seed(&path, "old", 2_500, (now - 40 * DAY) as i64);
        seed(&path, "fresh", 3, (now - DAY) as i64);
        let conn = open(&path);

        // tool_calls and llm_requests take two batches each.
        let pruned = prune_expired(&conn, Retention::default().cutoff(now), None).unwrap();

        assert_eq!(pruned, 1);
        assert_eq!(rows_for(&conn, "old"), 0);
        assert_eq!(rows_for(&conn, "fresh"), 3 * 3 + 1);
    }

    #[test]
    fn a_session_active_again_mid_prune_keeps_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let now = 100 * DAY;
        seed(&path, "old", 3, (now - 40 * DAY) as i64);
        let conn = open(&path);
        conn.execute(
            "UPDATE sessions SET updated_at = ?1 WHERE router_session_id = 'old'",
            params![now as i64],
        )
        .unwrap();

        for (sql, limit) in CHILD_DELETES {
            let deleted = conn
                .execute(sql, params!["old", Retention::default().cutoff(now), limit])
                .unwrap();
            assert_eq!(deleted, 0, "{sql}");
        }
    }

    #[test]
    fn a_busy_database_stops_the_tick_and_the_next_tick_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let now = 100 * DAY;
        seed(&path, "old", 3, (now - 40 * DAY) as i64);
        let conn = open(&path);
        let mut worker = Maintenance::new(path.clone(), Retention::default());

        let writer = open(&path);
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        let started = Instant::now();
        assert_eq!(worker.tick_at(now, TICK_BUDGET), None);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(rows_for(&conn, "old"), 3 * 3 + 1);
        writer.execute_batch("COMMIT").unwrap();

        let report = worker.tick_at(now + 1, TICK_BUDGET).unwrap();
        assert_eq!(report.sessions_pruned, 1, "{report:?}");
        assert_eq!(rows_for(&conn, "old"), 0);
    }

    #[test]
    fn only_one_router_holds_the_lease_until_it_expires() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        StateFile::load(&path, Retention::default());
        let mut first = Maintenance::new(path.clone(), Retention::default());
        let mut second = Maintenance::new(path.clone(), Retention::default());
        second.holder.push_str("-second");
        let now = 100 * DAY;

        assert!(first.tick_at(now, TICK_BUDGET).is_some());
        assert!(second.tick_at(now + 60, TICK_BUDGET).is_none());
        assert!(first.tick_at(now + 300, TICK_BUDGET).is_some());
        // The first router stops ticking; its renewed lease lapses.
        let lapsed = now + 300 + LEASE_TTL.as_secs();
        assert!(second.tick_at(lapsed, TICK_BUDGET).is_some());
        assert!(first.tick_at(lapsed + 1, TICK_BUDGET).is_none());

        let stats = stats(&path).unwrap();
        assert_eq!(stats["maintenance"]["holder"], second.holder);
        assert_eq!(stats["maintenance"]["last_tick_at"], lapsed as i64);
        assert_eq!(stats["maintenance"]["last_result"]["deferred"], false);
    }

    #[test]
    fn finished_tool_calls_lose_detail_and_old_rows_are_backfilled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        seed(&path, "r1", 3, now_epoch() as i64);
        let state = StateFile::load(&path, Retention::default());
        state.record_tool_call(
            "r1",
            "live",
            "cargo test",
            "in_progress",
            None,
            &serde_json::json!({"out": "running"}),
        );
        state.record_tool_call(
            "r1",
            "done",
            "cargo test",
            "completed",
            None,
            &serde_json::json!({"out": "ok"}),
        );
        let conn = open(&path);
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM tool_calls WHERE tool_call_id = 'done' AND detail IS NULL"
            ),
            1
        );

        let mut cursor = 0;
        let trimmed = trim_tool_details(&conn, Instant::now() + TICK_BUDGET, &mut cursor).unwrap();

        assert_eq!(trimmed, 3);
        assert_eq!(
            cursor,
            count(&conn, "SELECT MAX(rowid) FROM tool_calls"),
            "a finished pass starts the next tick past every scanned row"
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM tool_calls WHERE detail IS NOT NULL"
            ),
            1
        );
        let live: String = conn
            .query_row("SELECT detail FROM active_tool_calls", [], |row| row.get(0))
            .unwrap();
        assert_eq!(live, r#"{"out":"running"}"#);
    }

    #[test]
    fn after_compact_a_tick_returns_pruned_pages_to_the_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let now = now_epoch();
        seed(&path, "old", 4_000, (now - 40 * DAY) as i64);
        seed(&path, "fresh", 3, now as i64);
        let (_, compacted) = compact(&path).unwrap();
        assert_eq!(stats(&path).unwrap()["auto_vacuum"], "incremental");

        let mut worker = Maintenance::new(path.clone(), Retention::default());
        let mut vacuumed = 0;
        // Each tick has a two-second budget; a few cover this much data.
        for tick in 0..10 {
            let report = worker.tick_at(now + tick, TICK_BUDGET).unwrap();
            assert_eq!(report.error, None);
            vacuumed += report.pages_vacuumed;
        }
        let conn = open(&path);
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();

        assert!(vacuumed > VACUUM_FREE_PAGES, "vacuumed {vacuumed} pages");
        assert!(
            file_len(&path) < compacted / 2,
            "{} bytes after, {compacted} after compact",
            file_len(&path)
        );
        assert_eq!(rows_for(&conn, "fresh"), 3 * 3 + 1);
    }

    #[test]
    fn the_wal_is_truncated_only_once_writes_are_quiet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let now = now_epoch() as i64;
        StateFile::load(&path, Retention::default());
        let conn = StateFile::open_conn(&path).unwrap();
        // No automatic checkpoint, so the WAL grows past the limit.
        conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        conn.execute_batch(
            "CREATE TABLE filler (b BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 70)
             INSERT INTO filler SELECT zeroblob(1024 * 1024) FROM n;",
        )
        .unwrap();
        StateFile::load(&path, Retention::default()).upsert("r1".into(), session());
        let log = |ts: i64| {
            conn.execute(
                "INSERT INTO session_log (router_session_id, ts, kind, role, summary)
                 VALUES ('r1', ?1, 'k', 'r', 's')",
                params![ts],
            )
            .unwrap();
        };
        log(now);
        assert!(file_len(&wal_path(&path)) > TRUNCATE_WAL_BYTES);

        assert_eq!(checkpoint(&conn, &path, now + 10).unwrap(), "passive");
        assert!(file_len(&wal_path(&path)) > TRUNCATE_WAL_BYTES);

        assert_eq!(
            checkpoint(&conn, &path, now + QUIET_SECS).unwrap(),
            "truncate"
        );
        assert_eq!(file_len(&wal_path(&path)), 0);
    }
}
