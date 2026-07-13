//! SQLite-backed router state and observability.
//!
//! Replaces the former `sessions.json`. Two tables:
//!
//! * `sessions` — one row per router session with everything the JSON file
//!   held (pin, cwd, title, routing decision + weights, timestamps) plus
//!   `parent_session_id` (set for delegated sub-agent sessions so the
//!   planning/sub-agent/review structure is a tree), an optional `run_label`
//!   for grouping related sessions, and running token/context counters.
//! * `session_log` — every ACP interaction (user prompt, model response,
//!   tool call, permission/fs/terminal callback, router notice) with a token
//!   count; each insert also increments the owning session's counters.
//!
//! Retention: sessions (and their logs, via cascade) older than the `history`
//! window are pruned on open and after each write. This is the only pruning
//! mechanism (it replaces the earlier count/age logic).

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};

/// Retention policy: sessions idle longer than `max_age` are pruned.
#[derive(Debug, Clone, Copy)]
pub struct Retention {
    pub max_age: Duration,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(30 * 24 * 60 * 60),
        }
    }
}

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A persisted router session row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PersistedSession {
    pub agent: String,
    pub model: String,
    pub downstream_session_id: String,
    pub cwd: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub title: Option<String>,
    /// Full routing decision (strategy, candidate, weights, class,
    /// complexity, skipped, cordons) — the `_meta.router_acp` payload.
    pub routing: Option<serde_json::Value>,
    /// Router session id of the parent, for delegated sub-agent sessions.
    pub parent_session_id: Option<String>,
    /// `primary` (a normal pinned session) or `delegate` (a sub-agent
    /// spawned via `delegate_task`).
    pub kind: String,
    /// Optional grouping label (e.g. an orchestration run id) shared by
    /// related sessions.
    pub run_label: Option<String>,
    pub created_at: Option<u64>,
    pub updated_at: Option<u64>,
    /// Running token counters, incremented as `session_log` rows land.
    pub tokens_input: u64,
    pub tokens_output: u64,
    pub tokens_total: u64,
    /// Latest reported context-window usage (from `usage_update`).
    pub context_used: u64,
}

/// One `session_log` row: a single ACP interaction.
#[derive(Debug, Clone, Default)]
pub struct LogEntry {
    /// `user_prompt`, `agent_response`, `agent_thought`, `tool_call`,
    /// `permission`, `fs_read`, `fs_write`, `terminal`, `router_notice`, …
    pub kind: String,
    /// `user`, `agent`, `router`, `tool`, `client`.
    pub role: String,
    /// Short human-readable summary/content.
    pub summary: String,
    /// Optional structured detail (raw params/result).
    pub detail: Option<serde_json::Value>,
    pub tokens_input: u64,
    pub tokens_output: u64,
    /// True when token counts are estimated (protocol gave none).
    pub tokens_estimated: bool,
}

/// SQLite-backed store. Kept behind a `Mutex` in `Shared` (rusqlite
/// `Connection` is `Send` but not `Sync`); every method takes `&self`.
pub struct StateFile {
    conn: Connection,
    retention: Retention,
}

impl std::fmt::Debug for StateFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateFile").finish_non_exhaustive()
    }
}

impl StateFile {
    /// Open (creating/migrating) the database at `path`, then prune.
    pub fn load(path: &Path, retention: Retention) -> Self {
        let conn = Self::open_conn(path).unwrap_or_else(|err| {
            tracing::error!(%err, path = %path.display(), "cannot open state DB; using in-memory");
            Connection::open_in_memory().expect("in-memory sqlite")
        });
        let store = Self { conn, retention };
        store.init_schema();
        // One-time import of a legacy sessions.json sitting next to the DB.
        store.import_legacy_json(path);
        store.prune();
        store
    }

    fn open_conn(path: &Path) -> rusqlite::Result<Connection> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }

    fn init_schema(&self) {
        let sql = r#"
        CREATE TABLE IF NOT EXISTS sessions (
            router_session_id     TEXT PRIMARY KEY,
            agent                 TEXT NOT NULL,
            model                 TEXT NOT NULL,
            downstream_session_id TEXT NOT NULL,
            cwd                   TEXT NOT NULL,
            additional_directories TEXT NOT NULL DEFAULT '[]',
            title                 TEXT,
            routing               TEXT,
            parent_session_id     TEXT,
            kind                  TEXT NOT NULL DEFAULT 'primary',
            run_label             TEXT,
            created_at            INTEGER,
            updated_at            INTEGER,
            tokens_input          INTEGER NOT NULL DEFAULT 0,
            tokens_output         INTEGER NOT NULL DEFAULT 0,
            tokens_total          INTEGER NOT NULL DEFAULT 0,
            context_used          INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated_at);
        CREATE INDEX IF NOT EXISTS idx_sessions_parent  ON sessions(parent_session_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_down     ON sessions(agent, downstream_session_id);
        CREATE TABLE IF NOT EXISTS session_log (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            router_session_id TEXT NOT NULL,
            ts                INTEGER NOT NULL,
            kind              TEXT NOT NULL,
            role              TEXT NOT NULL,
            summary           TEXT NOT NULL,
            detail            TEXT,
            tokens_input      INTEGER NOT NULL DEFAULT 0,
            tokens_output     INTEGER NOT NULL DEFAULT 0,
            tokens_estimated  INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(router_session_id) REFERENCES sessions(router_session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_log_session ON session_log(router_session_id, id);
        "#;
        if let Err(err) = self.conn.execute_batch(sql) {
            tracing::error!(%err, "failed to initialize state schema");
        }
    }

    fn import_legacy_json(&self, db_path: &Path) {
        let json_path = db_path.with_extension("json");
        let already: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))
            .unwrap_or(0);
        if already > 0 {
            return;
        }
        let Ok(text) = std::fs::read_to_string(&json_path) else {
            return;
        };
        let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) else {
            return;
        };
        let Some(sessions) = val.get("sessions").and_then(|s| s.as_object()) else {
            return;
        };
        let now = now_epoch();
        let mut imported = 0;
        for (sid, s) in sessions {
            let ps = PersistedSession {
                agent: s
                    .get("agent")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                model: s
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                downstream_session_id: s
                    .get("downstream_session_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                cwd: PathBuf::from(s.get("cwd").and_then(|v| v.as_str()).unwrap_or("")),
                title: s.get("title").and_then(|v| v.as_str()).map(str::to_string),
                routing: s.get("routing").cloned(),
                created_at: s.get("created_at").and_then(|v| v.as_u64()).or(Some(now)),
                updated_at: s.get("updated_at").and_then(|v| v.as_u64()).or(Some(now)),
                kind: "primary".to_string(),
                ..Default::default()
            };
            self.upsert(sid.clone(), ps);
            imported += 1;
        }
        if imported > 0 {
            let backup = json_path.with_extension("json.imported");
            let _ = std::fs::rename(&json_path, &backup);
            tracing::info!(
                imported,
                "imported legacy sessions.json into SQLite state DB"
            );
        }
    }

    fn row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<PersistedSession> {
        let dirs: String = row.get("additional_directories")?;
        let routing: Option<String> = row.get("routing")?;
        Ok(PersistedSession {
            agent: row.get("agent")?,
            model: row.get("model")?,
            downstream_session_id: row.get("downstream_session_id")?,
            cwd: PathBuf::from(row.get::<_, String>("cwd")?),
            additional_directories: serde_json::from_str(&dirs).unwrap_or_default(),
            title: row.get("title")?,
            routing: routing.and_then(|r| serde_json::from_str(&r).ok()),
            parent_session_id: row.get("parent_session_id")?,
            kind: row.get("kind")?,
            run_label: row.get("run_label")?,
            created_at: row.get::<_, Option<i64>>("created_at")?.map(|v| v as u64),
            updated_at: row.get::<_, Option<i64>>("updated_at")?.map(|v| v as u64),
            tokens_input: row.get::<_, i64>("tokens_input")? as u64,
            tokens_output: row.get::<_, i64>("tokens_output")? as u64,
            tokens_total: row.get::<_, i64>("tokens_total")? as u64,
            context_used: row.get::<_, i64>("context_used")? as u64,
        })
    }

    pub fn get(&self, router_session_id: &str) -> Option<PersistedSession> {
        self.conn
            .query_row(
                "SELECT * FROM sessions WHERE router_session_id = ?1",
                params![router_session_id],
                Self::row_to_session,
            )
            .optional()
            .ok()
            .flatten()
    }

    /// All sessions (id + row), newest activity first. Used by list/CLI/tests.
    pub fn all(&self) -> Vec<(String, PersistedSession)> {
        let mut out = Vec::new();
        let Ok(mut stmt) = self
            .conn
            .prepare("SELECT router_session_id, * FROM sessions ORDER BY updated_at DESC")
        else {
            return out;
        };
        let rows = stmt.query_map([], |row| {
            let id: String = row.get("router_session_id")?;
            Ok((id, Self::row_to_session(row)?))
        });
        if let Ok(rows) = rows {
            for r in rows.flatten() {
                out.push(r);
            }
        }
        out
    }

    pub fn iter(&self) -> impl Iterator<Item = (String, PersistedSession)> {
        self.all().into_iter()
    }

    pub fn find_by_downstream(&self, agent: &str, downstream_session_id: &str) -> Option<String> {
        self.conn
            .query_row(
                "SELECT router_session_id FROM sessions \
                 WHERE agent = ?1 AND downstream_session_id = ?2 LIMIT 1",
                params![agent, downstream_session_id],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .ok()
            .flatten()
    }

    pub fn upsert(&self, router_session_id: String, mut session: PersistedSession) {
        let now = now_epoch();
        // Preserve creation time, title, and accumulated token counters
        // across re-pins (failover) unless fresh values are supplied.
        if let Some(existing) = self.get(&router_session_id) {
            session.created_at = session.created_at.or(existing.created_at);
            if session.title.is_none() {
                session.title = existing.title;
            }
            if session.parent_session_id.is_none() {
                session.parent_session_id = existing.parent_session_id;
            }
            if session.run_label.is_none() {
                session.run_label = existing.run_label;
            }
            session.tokens_input = session.tokens_input.max(existing.tokens_input);
            session.tokens_output = session.tokens_output.max(existing.tokens_output);
            session.tokens_total = session.tokens_total.max(existing.tokens_total);
            session.context_used = session.context_used.max(existing.context_used);
        }
        session.created_at = session.created_at.or(Some(now));
        session.updated_at = Some(now);
        if session.kind.is_empty() {
            session.kind = "primary".to_string();
        }
        let dirs =
            serde_json::to_string(&session.additional_directories).unwrap_or_else(|_| "[]".into());
        let routing = session.routing.as_ref().map(|r| r.to_string());
        let res = self.conn.execute(
            "INSERT INTO sessions (router_session_id, agent, model, downstream_session_id, cwd,
                additional_directories, title, routing, parent_session_id, kind, run_label,
                created_at, updated_at, tokens_input, tokens_output, tokens_total, context_used)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)
             ON CONFLICT(router_session_id) DO UPDATE SET
                agent=excluded.agent, model=excluded.model,
                downstream_session_id=excluded.downstream_session_id, cwd=excluded.cwd,
                additional_directories=excluded.additional_directories, title=excluded.title,
                routing=excluded.routing, parent_session_id=excluded.parent_session_id,
                kind=excluded.kind, run_label=excluded.run_label,
                created_at=excluded.created_at, updated_at=excluded.updated_at,
                tokens_input=excluded.tokens_input, tokens_output=excluded.tokens_output,
                tokens_total=excluded.tokens_total, context_used=excluded.context_used",
            params![
                router_session_id,
                session.agent,
                session.model,
                session.downstream_session_id,
                session.cwd.to_string_lossy(),
                dirs,
                session.title,
                routing,
                session.parent_session_id,
                session.kind,
                session.run_label,
                session.created_at.map(|v| v as i64),
                session.updated_at.map(|v| v as i64),
                session.tokens_input as i64,
                session.tokens_output as i64,
                session.tokens_total as i64,
                session.context_used as i64,
            ],
        );
        if let Err(err) = res {
            tracing::error!(%err, "failed to upsert session");
        }
        self.prune();
    }

    pub fn set_title(&self, router_session_id: &str, title: &str) {
        let title = title.trim();
        if title.is_empty() {
            return;
        }
        let _ = self.conn.execute(
            "UPDATE sessions SET title=?2, updated_at=?3 WHERE router_session_id=?1",
            params![router_session_id, title, now_epoch() as i64],
        );
    }

    /// Refresh last-activity (rate-limited to once a minute per session).
    pub fn touch(&self, router_session_id: &str) {
        let now = now_epoch() as i64;
        let _ = self.conn.execute(
            "UPDATE sessions SET updated_at=?2 \
             WHERE router_session_id=?1 AND (updated_at IS NULL OR ?2 - updated_at >= 60)",
            params![router_session_id, now],
        );
    }

    pub fn remove(&self, router_session_id: &str) -> Option<PersistedSession> {
        let existing = self.get(router_session_id);
        if existing.is_some() {
            let _ = self.conn.execute(
                "DELETE FROM sessions WHERE router_session_id=?1",
                params![router_session_id],
            );
        }
        existing
    }

    /// Append a `session_log` row and increment the session's counters.
    pub fn log(&self, router_session_id: &str, entry: &LogEntry) {
        let now = now_epoch() as i64;
        let detail = entry.detail.as_ref().map(|d| d.to_string());
        let res = self.conn.execute(
            "INSERT INTO session_log
                (router_session_id, ts, kind, role, summary, detail,
                 tokens_input, tokens_output, tokens_estimated)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                router_session_id,
                now,
                entry.kind,
                entry.role,
                entry.summary,
                detail,
                entry.tokens_input as i64,
                entry.tokens_output as i64,
                entry.tokens_estimated as i64,
            ],
        );
        if let Err(err) = res {
            // A log row for an unknown session (FK violation) is non-fatal.
            tracing::debug!(%err, session = router_session_id, "session_log insert skipped");
            return;
        }
        let _ = self.conn.execute(
            "UPDATE sessions SET
                tokens_input = tokens_input + ?2,
                tokens_output = tokens_output + ?3,
                tokens_total = tokens_total + ?4,
                updated_at = ?5
             WHERE router_session_id = ?1",
            params![
                router_session_id,
                entry.tokens_input as i64,
                entry.tokens_output as i64,
                (entry.tokens_input + entry.tokens_output) as i64,
                now,
            ],
        );
    }

    /// Record the latest context-window usage for a session.
    pub fn set_context_used(&self, router_session_id: &str, used: u64) {
        let _ = self.conn.execute(
            "UPDATE sessions SET context_used=?2 WHERE router_session_id=?1",
            params![router_session_id, used as i64],
        );
    }

    /// Recent log entries for a session (chronological).
    pub fn log_for(&self, router_session_id: &str, limit: usize) -> Vec<LogEntry> {
        let mut out = Vec::new();
        let Ok(mut stmt) = self.conn.prepare(
            "SELECT kind, role, summary, detail, tokens_input, tokens_output, tokens_estimated
             FROM session_log WHERE router_session_id=?1 ORDER BY id DESC LIMIT ?2",
        ) else {
            return out;
        };
        let rows = stmt.query_map(params![router_session_id, limit as i64], |row| {
            let detail: Option<String> = row.get("detail")?;
            Ok(LogEntry {
                kind: row.get("kind")?,
                role: row.get("role")?,
                summary: row.get("summary")?,
                detail: detail.and_then(|d| serde_json::from_str(&d).ok()),
                tokens_input: row.get::<_, i64>("tokens_input")? as u64,
                tokens_output: row.get::<_, i64>("tokens_output")? as u64,
                tokens_estimated: row.get::<_, i64>("tokens_estimated")? != 0,
            })
        });
        if let Ok(rows) = rows {
            for r in rows.flatten() {
                out.push(r);
            }
        }
        out.reverse();
        out
    }

    /// Delete sessions (and, by cascade, their logs) idle past `max_age`.
    pub fn prune(&self) -> usize {
        self.prune_at(now_epoch())
    }

    pub fn prune_at(&self, now: u64) -> usize {
        let cutoff = now.saturating_sub(self.retention.max_age.as_secs()) as i64;
        match self.conn.execute(
            "DELETE FROM sessions WHERE updated_at IS NOT NULL AND updated_at < ?1",
            params![cutoff],
        ) {
            Ok(n) => {
                if n > 0 {
                    tracing::info!(pruned = n, "pruned sessions past the history window");
                }
                n
            }
            Err(err) => {
                tracing::error!(%err, "prune failed");
                0
            }
        }
    }
}

/// Cheap token estimate when the protocol provides none: ~4 chars/token.
pub fn estimate_tokens(text: &str) -> u64 {
    (text.chars().count() as u64).div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, StateFile) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let s = StateFile::load(&path, Retention::default());
        (dir, s)
    }

    fn session(agent: &str) -> PersistedSession {
        PersistedSession {
            agent: agent.into(),
            model: "m".into(),
            downstream_session_id: "d".into(),
            cwd: PathBuf::from("/"),
            kind: "primary".into(),
            ..Default::default()
        }
    }

    #[test]
    fn roundtrips_session_with_all_fields() {
        let (_d, s) = store();
        s.upsert(
            "r1".into(),
            PersistedSession {
                agent: "claude".into(),
                model: "sonnet".into(),
                downstream_session_id: "down-1".into(),
                cwd: PathBuf::from("/tmp/p"),
                additional_directories: vec![PathBuf::from("/tmp/o")],
                title: Some("fix login".into()),
                routing: Some(serde_json::json!({"strategy":"auto","weights":{"q":0.7}})),
                kind: "primary".into(),
                ..Default::default()
            },
        );
        let got = s.get("r1").unwrap();
        assert_eq!(got.agent, "claude");
        assert_eq!(got.title.as_deref(), Some("fix login"));
        assert_eq!(got.routing.unwrap()["strategy"], "auto");
        assert_eq!(got.additional_directories, vec![PathBuf::from("/tmp/o")]);
        assert!(got.created_at.is_some() && got.updated_at.is_some());
        assert_eq!(
            s.find_by_downstream("claude", "down-1").as_deref(),
            Some("r1")
        );
    }

    #[test]
    fn sub_agent_links_to_parent() {
        let (_d, s) = store();
        s.upsert("planner".into(), session("claude"));
        s.upsert(
            "sub1".into(),
            PersistedSession {
                parent_session_id: Some("planner".into()),
                kind: "delegate".into(),
                ..session("codex")
            },
        );
        let sub = s.get("sub1").unwrap();
        assert_eq!(sub.parent_session_id.as_deref(), Some("planner"));
        assert_eq!(sub.kind, "delegate");
        // Children discoverable via the parent index.
        let children: Vec<_> = s
            .all()
            .into_iter()
            .filter(|(_, p)| p.parent_session_id.as_deref() == Some("planner"))
            .collect();
        assert_eq!(children.len(), 1);
    }

    #[test]
    fn log_entries_increment_session_tokens() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("a"));
        s.log(
            "r1",
            &LogEntry {
                kind: "user_prompt".into(),
                role: "user".into(),
                summary: "hello".into(),
                tokens_input: 3,
                ..Default::default()
            },
        );
        s.log(
            "r1",
            &LogEntry {
                kind: "agent_response".into(),
                role: "agent".into(),
                summary: "pong".into(),
                tokens_input: 10,
                tokens_output: 42,
                ..Default::default()
            },
        );
        let got = s.get("r1").unwrap();
        assert_eq!(got.tokens_input, 13);
        assert_eq!(got.tokens_output, 42);
        assert_eq!(got.tokens_total, 55);
        let entries = s.log_for("r1", 10);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].kind, "user_prompt");
        assert_eq!(entries[1].tokens_output, 42);
    }

    #[test]
    fn removing_session_cascades_logs() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("a"));
        s.log(
            "r1",
            &LogEntry {
                kind: "x".into(),
                role: "user".into(),
                ..Default::default()
            },
        );
        s.remove("r1");
        assert!(s.get("r1").is_none());
        assert_eq!(s.log_for("r1", 10).len(), 0);
    }

    #[test]
    fn upsert_preserves_created_at_title_and_tokens_across_repin() {
        let (_d, s) = store();
        s.upsert(
            "r1".into(),
            PersistedSession {
                title: Some("orig".into()),
                ..session("a")
            },
        );
        s.log(
            "r1",
            &LogEntry {
                tokens_output: 5,
                ..Default::default()
            },
        );
        let created = s.get("r1").unwrap().created_at;
        // Failover re-pin: fresh record, different agent, no title/tokens.
        s.upsert("r1".into(), session("b"));
        let got = s.get("r1").unwrap();
        assert_eq!(got.agent, "b");
        assert_eq!(got.created_at, created);
        assert_eq!(got.title.as_deref(), Some("orig"));
        assert_eq!(got.tokens_output, 5, "token counters survive re-pin");
    }

    #[test]
    fn prunes_sessions_past_history_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let s = StateFile::load(
            &path,
            Retention {
                max_age: Duration::from_secs(100),
            },
        );
        s.upsert("old".into(), session("a"));
        s.upsert("new".into(), session("b"));
        s.conn
            .execute(
                "UPDATE sessions SET updated_at=1000 WHERE router_session_id='old'",
                [],
            )
            .unwrap();
        s.conn
            .execute(
                "UPDATE sessions SET updated_at=1950 WHERE router_session_id='new'",
                [],
            )
            .unwrap();
        let pruned = s.prune_at(2000);
        assert_eq!(pruned, 1);
        assert!(s.get("old").is_none());
        assert!(s.get("new").is_some());
    }

    #[test]
    fn set_title_and_touch_and_context() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("a"));
        s.set_title("r1", "  Titled  ");
        assert_eq!(s.get("r1").unwrap().title.as_deref(), Some("Titled"));
        s.set_context_used("r1", 12345);
        assert_eq!(s.get("r1").unwrap().context_used, 12345);
    }

    #[test]
    fn imports_legacy_json_once() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("sessions.db");
        let json = dir.path().join("sessions.json");
        std::fs::write(
            &json,
            r#"{"version":1,"sessions":{"r1":{"agent":"claude","model":"sonnet",
               "downstream_session_id":"d","cwd":"/tmp","title":"t"}}}"#,
        )
        .unwrap();
        let s = StateFile::load(&db, Retention::default());
        let got = s.get("r1").expect("legacy row imported");
        assert_eq!(got.model, "sonnet");
        assert_eq!(got.title.as_deref(), Some("t"));
        // JSON was renamed so it doesn't re-import.
        assert!(!json.exists());
        assert!(json.with_extension("json.imported").exists());
    }
}
