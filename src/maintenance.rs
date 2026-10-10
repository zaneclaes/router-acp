//! Background upkeep for the state databases: retention prune, finished-tool
//! detail trim, incremental vacuum and WAL checkpoint, over the legacy file
//! and every cwd shard (`crate::state_layout`).
//!
//! Every `router-acp serve` process on a box ticks every `TICK_EVERY`, but
//! only the process holding a nonblocking exclusive lock on
//! `maintenance.lock` (beside the legacy file) does any work, so ~30 routers
//! never vacuum at once. A loser returns at once. The OS drops the lock when
//! its holder exits, and the next ticking router takes it. The legacy
//! `maintenance_lease` row only reports the holder and its last tick.
//!
//! A tick sweeps the legacy file, then shards oldest-swept first until
//! `TICK_BUDGET` is spent. The order comes from `shard_maintenance` rows in
//! the legacy file, so it survives restarts and holder changes; every shard
//! is stamped when visited, busy or not, so none starves. Each file gets its
//! own connection (never the process-wide state mutex) that waits at most
//! `DEFERRABLE_BUSY_WAIT` for the write lock. Every write is one short
//! statement; a busy file ends that file's sweep and the tick moves on.

use std::collections::HashMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use crate::state::{BUSY_TIMEOUT, DEFERRABLE_BUSY_WAIT, Retention, StateStore, is_busy};
use crate::state_layout::{ShardTag, StateLayout};

pub const TICK_EVERY: Duration = Duration::from_secs(5 * 60);
const TICK_BUDGET: Duration = Duration::from_secs(2);
/// No one shard takes more than this of a tick.
const SHARD_BUDGET: Duration = Duration::from_millis(250);
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

/// What one held tick did. Stored as JSON in `maintenance_lease.last_result`
/// for the whole tick, and in `shard_maintenance.last_result` per shard.
/// The counts cover every file swept; `checkpoint`, `deferred`, and `error`
/// describe the file the row is about (the legacy file, for the lease row).
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
    pub shards_swept: usize,
    /// Shards a writer kept busy; each is retried after the others.
    pub shards_deferred: usize,
}

/// One router's maintenance worker.
pub struct Maintenance {
    layout: StateLayout,
    retention: Retention,
    holder: String,
    /// The election lock, held from the first won tick until this worker drops.
    lock: Option<File>,
    conn: Option<Connection>,
    /// Finished-trim cursor per file; `""` is the legacy file.
    trim_cursors: HashMap<String, i64>,
    warned_no_vacuum: bool,
}

impl Maintenance {
    /// `path` is the legacy `state_file`; shards are found beside it.
    pub fn new(path: PathBuf, retention: Retention) -> Self {
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        Self {
            layout: StateLayout::new(&path),
            retention,
            holder: format!("{}-{started}", std::process::id()),
            lock: None,
            conn: None,
            trim_cursors: HashMap::new(),
            warned_no_vacuum: false,
        }
    }

    /// Hold the election lock, trying once without blocking if not yet held.
    fn hold_lock(&mut self) -> bool {
        if self.lock.is_some() {
            return true;
        }
        let path = self.layout.maintenance_lock_path();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let file = match OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(err) => {
                tracing::warn!(%err, path = %path.display(), "cannot open the maintenance lock");
                return false;
            }
        };
        match file.try_lock() {
            Ok(()) => {
                self.lock = Some(file);
                true
            }
            Err(TryLockError::WouldBlock) => false,
            Err(TryLockError::Error(err)) => {
                tracing::warn!(%err, path = %path.display(), "maintenance lock attempt failed");
                false
            }
        }
    }

    /// Run one tick at `now`. `None` when another router holds the lock or
    /// the legacy file cannot be opened.
    pub fn tick_at(&mut self, now: u64, budget: Duration) -> Option<TickReport> {
        if !self.hold_lock() {
            return None;
        }
        if self.conn.is_none() {
            let opened = StateStore::open_conn(&self.layout.legacy_path)
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
        let cutoff = self.retention.cutoff(now);
        let started = Instant::now();
        let deadline = started + budget;
        let shards = shards_oldest_first(conn, self.layout.list_shards());
        // The legacy file may carry a large backlog; leave shards half the tick.
        let legacy_deadline = if shards.is_empty() {
            deadline
        } else {
            started + budget / 2
        };
        let mut report = sweep(
            conn,
            &self.layout.legacy_path,
            cutoff,
            now,
            legacy_deadline,
            self.trim_cursors.entry(String::new()).or_default(),
            &mut self.warned_no_vacuum,
        );
        for tag in shards {
            let shard_started = Instant::now();
            if shard_started >= deadline {
                break;
            }
            let path = self.layout.shard_path(&tag);
            let shard = match open_existing(&path) {
                Ok(shard_conn) => sweep(
                    &shard_conn,
                    &path,
                    cutoff,
                    now,
                    deadline.min(shard_started + SHARD_BUDGET),
                    self.trim_cursors.entry(tag.to_string()).or_default(),
                    // Shards are born incremental; nothing to warn about.
                    &mut true,
                ),
                Err(err) => {
                    let mut failed = TickReport::default();
                    note_error(&mut failed, err);
                    failed
                }
            };
            report.shards_swept += 1;
            report.shards_deferred += usize::from(shard.deferred);
            report.sessions_pruned += shard.sessions_pruned;
            report.tool_details_trimmed += shard.tool_details_trimmed;
            report.pages_vacuumed += shard.pages_vacuumed;
            if let Err(err) = record_shard(conn, &tag, now, &shard) {
                tracing::debug!(%err, shard = %tag, "shard maintenance row not updated");
            }
        }
        if let Err(err) = record_tick(conn, &self.holder, now, &report) {
            tracing::debug!(%err, "maintenance_lease row not updated");
        }
        if report.error.is_some() {
            tracing::warn!(?report, "state maintenance tick failed");
        } else {
            tracing::debug!(?report, "state maintenance tick");
        }
        Some(report)
    }
}

/// Prune, trim, vacuum, then checkpoint one file until `deadline`.
fn sweep(
    conn: &Connection,
    path: &Path,
    cutoff: i64,
    now: u64,
    deadline: Instant,
    trim_cursor: &mut i64,
    warned_no_vacuum: &mut bool,
) -> TickReport {
    let mut report = TickReport::default();
    let steps = prune_expired(conn, cutoff, Some(deadline))
        .map(|n| report.sessions_pruned = n)
        .and_then(|()| trim_tool_details(conn, deadline, trim_cursor))
        .map(|n| report.tool_details_trimmed = n)
        .and_then(|()| incremental_vacuum(conn, deadline, warned_no_vacuum))
        .map(|n| report.pages_vacuumed = n);
    if let Err(err) = steps {
        note_error(&mut report, err);
    }
    // A checkpoint never blocks writers, so it runs even after a busy step.
    match checkpoint(conn, path, now as i64) {
        Ok(mode) => report.checkpoint = Some(mode),
        Err(err) => note_error(&mut report, err),
    }
    report
}

/// A worker connection to a shard that must already exist.
fn open_existing(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(DEFERRABLE_BUSY_WAIT)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(conn)
}

/// Last sweep time per shard tag, from the legacy file.
fn shard_ticks(conn: &Connection) -> rusqlite::Result<HashMap<String, i64>> {
    conn.prepare("SELECT tag, last_tick_at FROM shard_maintenance")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

/// Never-swept shards first, then the longest since their last sweep.
fn shards_oldest_first(conn: &Connection, mut shards: Vec<ShardTag>) -> Vec<ShardTag> {
    let ticks = shard_ticks(conn).unwrap_or_default();
    shards.sort_by_cached_key(|tag| (ticks.get(tag.as_str()).copied().unwrap_or(0), tag.clone()));
    shards
}

fn record_shard(
    conn: &Connection,
    tag: &ShardTag,
    now: u64,
    report: &TickReport,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO shard_maintenance (tag, last_tick_at, last_result) VALUES (?1, ?2, ?3)
         ON CONFLICT(tag) DO UPDATE SET
             last_tick_at = excluded.last_tick_at, last_result = excluded.last_result",
        params![
            tag.as_str(),
            now as i64,
            serde_json::to_string(report).unwrap_or_default()
        ],
    )?;
    Ok(())
}

/// Report the tick in the legacy `maintenance_lease` row. The row decides
/// nothing; `expires_at` is kept for readers of its old shape.
fn record_tick(
    conn: &Connection,
    holder: &str,
    now: u64,
    report: &TickReport,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO maintenance_lease (id, holder, expires_at, last_tick_at, last_result)
         VALUES (1, ?1, ?2, ?3, ?4)
         ON CONFLICT(id) DO UPDATE SET
             holder = excluded.holder, expires_at = excluded.expires_at,
             last_tick_at = excluded.last_tick_at, last_result = excluded.last_result",
        params![
            holder,
            (now + 3 * TICK_EVERY.as_secs()) as i64,
            now as i64,
            serde_json::to_string(report).unwrap_or_default()
        ],
    )?;
    Ok(())
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
    // Only the legacy file carries the outbox.
    let has_outbox = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hook_outbox'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if has_outbox {
        conn.execute(
            "DELETE FROM hook_outbox WHERE created_at < ?1",
            params![cutoff],
        )?;
    }
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
/// `db` is the legacy file; `shards` lists every shard beside it.
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
    let shard_results: HashMap<String, (i64, Option<String>)> = conn
        .prepare("SELECT tag, last_tick_at, last_result FROM shard_maintenance")
        .and_then(|mut stmt| {
            stmt.query_map([], |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?))))?
                .collect()
        })
        // Older legacy files have no shard_maintenance table.
        .unwrap_or_default();
    let layout = StateLayout::new(db);
    let shards: Vec<_> = layout
        .list_shards()
        .into_iter()
        .map(|tag| {
            let path = layout.shard_path(&tag);
            let (last_tick_at, last_result) = shard_results
                .get(tag.as_str())
                .map(|(at, result)| (Some(*at), result.clone()))
                .unwrap_or_default();
            let (cwd, freelist_bytes) = shard_facts(&path).unwrap_or_default();
            serde_json::json!({
                "tag": tag.as_str(),
                "cwd": cwd,
                "db_bytes": file_len(&path),
                "wal_bytes": file_len(&wal_path(&path)),
                "freelist_bytes": freelist_bytes,
                "last_tick_at": last_tick_at,
                "last_result": last_result.and_then(|r| serde_json::from_str::<serde_json::Value>(&r).ok()),
            })
        })
        .collect();
    Ok(serde_json::json!({
        "db_bytes": file_len(db),
        "wal_bytes": file_len(&wal_path(db)),
        "page_size": page_size,
        "page_count": pragma("page_count")?,
        "freelist_count": freelist,
        "freelist_bytes": freelist * page_size,
        "auto_vacuum": auto_vacuum,
        "maintenance": lease,
        "shards": shards,
    }))
}

/// A shard's recorded checkout and its free bytes, read-only.
fn shard_facts(path: &Path) -> rusqlite::Result<(Option<String>, i64)> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let cwd = conn
        .query_row("SELECT cwd FROM shard_meta WHERE id = 1", [], |row| {
            row.get(0)
        })
        .optional()?;
    let pragma = |name: &str| -> rusqlite::Result<i64> {
        conn.pragma_query_value(None, name, |row| row.get(0))
    };
    Ok((cwd, pragma("page_size")? * pragma("freelist_count")?))
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
    use crate::state::{PersistedSession, StateStore};
    use crate::state_layout::{canonical_cwd, tag_for_cwd};
    use serde_json::json;

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
        fill(path, path, id, rows, updated_at);
    }

    /// `seed` for a session the store at `state` routes to file `db`.
    fn fill(state: &Path, db: &Path, id: &str, rows: usize, updated_at: i64) {
        let state = StateStore::load(state, Retention::default());
        state.upsert(id.into(), session());
        let conn = open(db);
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
    fn a_busy_database_defers_the_tick_and_the_next_tick_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let now = 100 * DAY;
        seed(&path, "old", 3, (now - 40 * DAY) as i64);
        let conn = open(&path);
        let mut worker = Maintenance::new(path.clone(), Retention::default());

        let writer = open(&path);
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        let started = Instant::now();
        let report = worker.tick_at(now, TICK_BUDGET).unwrap();
        assert!(report.deferred, "{report:?}");
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(rows_for(&conn, "old"), 3 * 3 + 1);
        writer.execute_batch("COMMIT").unwrap();

        let report = worker.tick_at(now + 1, TICK_BUDGET).unwrap();
        assert_eq!(report.sessions_pruned, 1, "{report:?}");
        assert_eq!(rows_for(&conn, "old"), 0);
    }

    #[test]
    fn only_the_lock_holder_sweeps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let now = 100 * DAY;
        seed(&path, "old", 3, (now - 40 * DAY) as i64);
        let mut first = Maintenance::new(path.clone(), Retention::default());
        let mut second = Maintenance::new(path.clone(), Retention::default());

        assert!(first.tick_at(now - 40 * DAY, TICK_BUDGET).is_some());
        // The loser does no work, even with an expired session waiting.
        assert_eq!(second.tick_at(now, TICK_BUDGET), None);
        assert_eq!(rows_for(&open(&path), "old"), 3 * 3 + 1);
        assert_eq!(first.tick_at(now, TICK_BUDGET).unwrap().sessions_pruned, 1);
        assert!(path.parent().unwrap().join("maintenance.lock").exists());
    }

    #[test]
    fn a_loser_returns_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        StateStore::load(&path, Retention::default());
        let mut winner = Maintenance::new(path.clone(), Retention::default());
        let mut loser = Maintenance::new(path.clone(), Retention::default());
        assert!(winner.tick_at(100 * DAY, TICK_BUDGET).is_some());
        // Even with the legacy file write-locked, the loser never waits.
        let writer = open(&path);
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();

        let started = Instant::now();
        assert_eq!(loser.tick_at(100 * DAY, TICK_BUDGET), None);
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn a_released_lock_is_reacquired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        StateStore::load(&path, Retention::default());
        let mut first = Maintenance::new(path.clone(), Retention::default());
        let mut second = Maintenance::new(path.clone(), Retention::default());
        assert!(first.tick_at(100 * DAY, TICK_BUDGET).is_some());
        assert_eq!(second.tick_at(100 * DAY, TICK_BUDGET), None);

        // The holder's process exits: the OS releases its lock.
        drop(first);
        assert!(second.tick_at(100 * DAY + 1, TICK_BUDGET).is_some());
    }

    #[test]
    fn health_and_stats_keep_the_maintenance_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let store = StateStore::load(&path, Retention::default());
        let mut worker = Maintenance::new(path.clone(), Retention::default());
        let now = 100 * DAY;
        worker.tick_at(now, TICK_BUDGET).unwrap();

        let stats = stats(&path).unwrap();
        assert_eq!(stats["maintenance"]["holder"], worker.holder);
        assert_eq!(stats["maintenance"]["last_tick_at"], now as i64);
        assert_eq!(stats["maintenance"]["last_result"]["deferred"], false);
        let health = store.health().unwrap().maintenance.unwrap();
        assert_eq!(health.holder, worker.holder);
        assert_eq!(health.last_tick_at, Some(now * 1000));
        assert_eq!(health.last_result.unwrap()["deferred"], false);
    }

    #[test]
    fn finished_tool_calls_lose_detail_and_old_rows_are_backfilled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        seed(&path, "r1", 3, now_epoch() as i64);
        let state = StateStore::load(&path, Retention::default());
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
        StateStore::load(&path, Retention::default());
        let conn = StateStore::open_conn(&path).unwrap();
        // No automatic checkpoint, so the WAL grows past the limit.
        conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        conn.execute_batch(
            "CREATE TABLE filler (b BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 70)
             INSERT INTO filler SELECT zeroblob(1024 * 1024) FROM n;",
        )
        .unwrap();
        StateStore::load(&path, Retention::default()).upsert("r1".into(), session());
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

    /// A cwd shard holding one session seeded like `seed`. Returns the
    /// session id and the shard's path.
    fn seed_shard(legacy: &Path, cwd: &Path, rows: usize, updated_at: i64) -> (String, PathBuf) {
        let store = StateStore::load(legacy, Retention::default());
        let sid = store.new_session_id(cwd).unwrap();
        drop(store);
        let shard = StateLayout::new(legacy).shard_path(&tag_for_cwd(cwd));
        fill(legacy, &shard, &sid, rows, updated_at);
        (sid, shard)
    }

    fn shard_row(legacy: &Path, shard: &Path) -> (i64, serde_json::Value) {
        let tag = shard
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.strip_prefix("sessions-"))
            .unwrap()
            .to_string();
        open(legacy)
            .query_row(
                "SELECT last_tick_at, last_result FROM shard_maintenance WHERE tag = ?1",
                params![tag],
                |row| {
                    let result: String = row.get(1)?;
                    Ok((row.get(0)?, serde_json::from_str(&result).unwrap()))
                },
            )
            .unwrap()
    }

    #[test]
    fn a_busy_shard_does_not_stop_the_tick_pruning_another() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("state.db");
        let now = 100 * DAY;
        let old = (now - 40 * DAY) as i64;
        StateStore::load(&legacy, Retention::default());
        let (a, shard_a) = seed_shard(&legacy, &dir.path().join("a"), 3, old);
        let (b, shard_b) = seed_shard(&legacy, &dir.path().join("b"), 3, old);
        let writer = open(&shard_a);
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();

        let mut worker = Maintenance::new(legacy.clone(), Retention::default());
        let report = worker.tick_at(now, TICK_BUDGET).unwrap();

        assert_eq!(report.shards_swept, 2, "{report:?}");
        assert_eq!(report.shards_deferred, 1, "{report:?}");
        assert_eq!(rows_for(&open(&shard_b), &b), 0);
        assert_eq!(rows_for(&open(&shard_a), &a), 3 * 3 + 1);
        // The busy shard is stamped too, so it waits behind the others.
        let (at, result) = shard_row(&legacy, &shard_a);
        assert_eq!((at, &result["deferred"]), (now as i64, &json!(true)));

        writer.execute_batch("COMMIT").unwrap();
        let report = worker.tick_at(now + 1, TICK_BUDGET).unwrap();
        assert_eq!(report.shards_deferred, 0, "{report:?}");
        assert_eq!(rows_for(&open(&shard_a), &a), 0);
    }

    #[test]
    fn shards_are_swept_oldest_first_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("state.db");
        StateStore::load(&legacy, Retention::default());
        let layout = StateLayout::new(&legacy);
        let tags: Vec<_> = ["x", "y", "z"]
            .iter()
            .map(|name| {
                seed_shard(&legacy, &dir.path().join(name), 0, 1);
                tag_for_cwd(&dir.path().join(name))
            })
            .collect();
        let mut sorted = tags.clone();
        sorted.sort();
        // Never swept: tag order.
        assert_eq!(
            shards_oldest_first(&open(&legacy), layout.list_shards()),
            sorted
        );

        let mut worker = Maintenance::new(legacy.clone(), Retention::default());
        assert_eq!(worker.tick_at(1_000, TICK_BUDGET).unwrap().shards_swept, 3);
        drop(worker);
        let conn = open(&legacy);
        conn.execute(
            "UPDATE shard_maintenance SET last_tick_at = 500 WHERE tag = ?1",
            params![tags[1].as_str()],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM shard_maintenance WHERE tag = ?1",
            params![tags[2].as_str()],
        )
        .unwrap();

        // A restarted (or different) holder reads the same order from disk:
        // never swept, then oldest sweep, then the rest.
        assert_eq!(
            shards_oldest_first(&open(&legacy), layout.list_shards()),
            [tags[2].clone(), tags[1].clone(), tags[0].clone()]
        );
    }

    #[test]
    fn history_prunes_legacy_active_and_inactive_shards_and_keeps_fresh_sessions() {
        for (retention, expired_age, fresh_age) in [
            (Retention::default(), 40 * DAY, 29 * DAY),
            (
                Retention {
                    max_age: Duration::from_secs(2 * DAY),
                },
                3 * DAY,
                DAY,
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let legacy = dir.path().join("state.db");
            let now = 100 * DAY;
            let expired = (now - expired_age) as i64;
            let fresh = (now - fresh_age) as i64;
            seed(&legacy, "legacy-old", 3, expired);
            seed(&legacy, "legacy-fresh", 3, fresh);
            let active_dir = dir.path().join("active");
            let (active_old, active) = seed_shard(&legacy, &active_dir, 3, expired);
            let active_fresh = StateStore::load(&legacy, retention);
            let active_fresh_id = active_fresh.new_session_id(&active_dir).unwrap();
            fill(&legacy, &active, &active_fresh_id, 3, fresh);
            // `active_fresh` keeps the shard open like a live router does.
            assert!(active_fresh.get(&active_fresh_id).is_some());
            let (inactive_old, inactive) =
                seed_shard(&legacy, &dir.path().join("inactive"), 3, expired);
            let inactive_fresh = StateStore::load(&legacy, retention);
            let inactive_fresh_id = inactive_fresh
                .new_session_id(&dir.path().join("inactive"))
                .unwrap();
            drop(inactive_fresh);
            fill(&legacy, &inactive, &inactive_fresh_id, 3, fresh);

            let mut worker = Maintenance::new(legacy.clone(), retention);
            for tick in 0..3 {
                worker.tick_at(now + tick, TICK_BUDGET).unwrap();
            }

            for (db, id) in [
                (&legacy, "legacy-old"),
                (&active, active_old.as_str()),
                (&inactive, inactive_old.as_str()),
            ] {
                assert_eq!(rows_for(&open(db), id), 0, "{id} in {}", db.display());
            }
            for (db, id) in [
                (&legacy, "legacy-fresh"),
                (&active, active_fresh_id.as_str()),
                (&inactive, inactive_fresh_id.as_str()),
            ] {
                assert_eq!(rows_for(&open(db), id), 3 * 3 + 1, "{id}");
            }
        }
    }

    #[test]
    fn a_shard_returns_pruned_pages_without_compact_and_stays_as_a_small_shell() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("state.db");
        let now = now_epoch();
        StateStore::load(&legacy, Retention::default());
        let (old, shard) = seed_shard(
            &legacy,
            &dir.path().join("a"),
            4_000,
            (now - 40 * DAY) as i64,
        );
        open(&shard)
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
        let full = file_len(&shard);

        let mut worker = Maintenance::new(legacy.clone(), Retention::default());
        let mut vacuumed = 0;
        for tick in 0..40 {
            let report = worker.tick_at(now + tick, TICK_BUDGET).unwrap();
            assert_eq!(report.error, None);
            vacuumed += report.pages_vacuumed;
            if report.sessions_pruned == 0 && report.pages_vacuumed == 0 && vacuumed > 0 {
                break;
            }
        }
        let conn = open(&shard);
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();

        assert_eq!(rows_for(&conn, &old), 0);
        assert!(vacuumed > VACUUM_FREE_PAGES, "vacuumed {vacuumed} pages");
        assert!(shard.exists(), "an emptied shard is never unlinked");
        assert!(
            file_len(&shard) < full / 10 && file_len(&shard) < 1024 * 1024,
            "{} bytes after, {full} before",
            file_len(&shard)
        );
    }

    #[test]
    fn stats_list_every_shard_with_its_last_sweep() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("state.db");
        StateStore::load(&legacy, Retention::default());
        let cwd = dir.path().join("a");
        seed_shard(&legacy, &cwd, 1, now_epoch() as i64);
        Maintenance::new(legacy.clone(), Retention::default())
            .tick_at(100 * DAY, TICK_BUDGET)
            .unwrap();

        let stats = stats(&legacy).unwrap();
        let shards = stats["shards"].as_array().unwrap();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0]["tag"], tag_for_cwd(&cwd).as_str());
        assert_eq!(shards[0]["cwd"], canonical_cwd(&cwd).display().to_string());
        assert_eq!(shards[0]["last_tick_at"], (100 * DAY) as i64);
        assert_eq!(shards[0]["last_result"]["deferred"], false);
        assert!(shards[0]["db_bytes"].as_u64().unwrap() > 0);
        assert_eq!(stats["maintenance"]["last_result"]["shards_swept"], 1);
    }
}
