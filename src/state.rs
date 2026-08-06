//! SQLite-backed router state and observability.
//!
//! Replaces the former `sessions.json`. Two tables:
//!
//! * `sessions` — one row per router session with everything the JSON file
//!   held (pin, cwd, title, routing decision + weights, timestamps) plus
//!   `parent_session_id` (set for delegated sub-agent sessions so the
//!   planning/sub-agent/review structure is a tree), `prior_session_id` (set
//!   by a mid-session model switch to the downstream session bound before it,
//!   tracing the switch lineage), an optional `run_label` for grouping related
//!   sessions, and running token/context counters.
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
    /// The downstream session id this router session was pinned to *before*
    /// its most recent mid-session model switch (set by `switch_pin`). Traces
    /// the switch lineage; `None` for sessions that never switched.
    pub prior_session_id: Option<String>,
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
    /// Cache-read / cache-write token counters (populated only when the
    /// adapter reports them via `unstable_end_turn_token_usage`; 0 otherwise).
    /// Cache reads dominate the real cost of long sessions, so hiding them
    /// made `tokens_input` useless for cost analysis.
    pub tokens_cache_read: u64,
    pub tokens_cache_write: u64,
    /// Authoritative cumulative cost in USD, as reported by the adapter's
    /// `usage_update.cost` (max seen). 0 if the adapter reports no cost.
    pub cost_usd: f64,
    /// True when `cost_usd` was synthesized from token counts and configured
    /// `pricing` (adapters other than claude report no cost of their own)
    /// rather than reported by the adapter.
    pub cost_estimated: bool,
    /// API-equivalent cost accumulated from interposed provider requests.
    /// Separate from adapter turn cost to avoid mixing granularities.
    pub llm_request_cost_usd: f64,
    pub llm_requests_total: u64,
    /// Count of native (adapter built-in) sub-agent tool calls seen in an
    /// orchestrating session — each one bypasses the router's `delegate_task`,
    /// so a non-zero value means orchestration silently degraded.
    pub native_subagent_calls: u64,
    /// Accumulated model compute time (prompt-sent → response), in ms —
    /// excludes user idle time between turns (unlike updated_at − created_at).
    pub compute_ms: u64,
    /// Git branch / HEAD sha of `cwd` at pin time, for joining a run to its CI
    /// or merge outcome later. `None` when cwd isn't a git repo.
    pub git_branch: Option<String>,
    pub git_sha: Option<String>,
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
    /// Cache-read / cache-write tokens for this turn (0 when the adapter
    /// doesn't report them).
    pub tokens_cache_read: u64,
    pub tokens_cache_write: u64,
    /// True when token counts are estimated (protocol gave none).
    pub tokens_estimated: bool,
    /// The candidate (`agent/model`) that produced this row, for per-turn
    /// model attribution across mid-session switches (set on
    /// `agent_response` rows).
    pub model: Option<String>,
}

/// Insert payload for one interposed provider request.
#[derive(Debug, Clone)]
pub struct LlmRequestStart {
    pub request_id: String,
    pub router_session_id: String,
    pub parent_router_session_id: Option<String>,
    pub agent: String,
    pub protocol: String,
    pub endpoint: String,
    pub pinned_model: String,
    pub model: String,
    pub routing_reason: String,
    pub routing_event: String,
    pub estimated_input_tokens: u64,
}

/// Provider-reported usage extracted from JSON or SSE.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LlmRequestUsage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    /// OpenAI input counts include cached tokens; Anthropic reports cache
    /// buckets separately. This flag prevents cost double-counting.
    pub input_includes_cache: bool,
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
            prior_session_id      TEXT,
            kind                  TEXT NOT NULL DEFAULT 'primary',
            run_label             TEXT,
            created_at            INTEGER,
            updated_at            INTEGER,
            tokens_input          INTEGER NOT NULL DEFAULT 0,
            tokens_output         INTEGER NOT NULL DEFAULT 0,
            tokens_total          INTEGER NOT NULL DEFAULT 0,
            tokens_cache_read     INTEGER NOT NULL DEFAULT 0,
            tokens_cache_write    INTEGER NOT NULL DEFAULT 0,
            context_used          INTEGER NOT NULL DEFAULT 0,
            cost_usd              REAL NOT NULL DEFAULT 0,
            cost_estimated        INTEGER NOT NULL DEFAULT 0,
            llm_request_cost_usd  REAL NOT NULL DEFAULT 0,
            llm_requests_total    INTEGER NOT NULL DEFAULT 0,
            native_subagent_calls INTEGER NOT NULL DEFAULT 0,
            compute_ms            INTEGER NOT NULL DEFAULT 0,
            git_branch            TEXT,
            git_sha               TEXT
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
            tokens_cache_read INTEGER NOT NULL DEFAULT 0,
            tokens_cache_write INTEGER NOT NULL DEFAULT 0,
            tokens_estimated  INTEGER NOT NULL DEFAULT 0,
            model             TEXT,
            FOREIGN KEY(router_session_id) REFERENCES sessions(router_session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_log_session ON session_log(router_session_id, id);
        -- Covering index for time-range token analytics (the kory-code relay's
        -- usage endpoint): range scans read only these narrow index pages
        -- instead of walking every row's multi-KB detail payload.
        CREATE INDEX IF NOT EXISTS idx_log_ts
            ON session_log(ts, router_session_id, tokens_input, tokens_output);
        CREATE TABLE IF NOT EXISTS llm_requests (
            request_id               TEXT PRIMARY KEY,
            router_session_id        TEXT NOT NULL,
            parent_router_session_id TEXT,
            agent                    TEXT NOT NULL,
            protocol                 TEXT NOT NULL,
            endpoint                 TEXT NOT NULL,
            pinned_model             TEXT NOT NULL,
            model                    TEXT NOT NULL,
            routing_reason           TEXT NOT NULL,
            routing_event            TEXT NOT NULL,
            started_at               INTEGER NOT NULL,
            finished_at              INTEGER,
            duration_ms              INTEGER,
            status                   INTEGER,
            estimated_input_tokens   INTEGER NOT NULL DEFAULT 0,
            tokens_input             INTEGER NOT NULL DEFAULT 0,
            tokens_output            INTEGER NOT NULL DEFAULT 0,
            tokens_cache_read        INTEGER NOT NULL DEFAULT 0,
            tokens_cache_write       INTEGER NOT NULL DEFAULT 0,
            cost_usd                 REAL NOT NULL DEFAULT 0,
            error                    TEXT,
            FOREIGN KEY(router_session_id) REFERENCES sessions(router_session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_llm_requests_session
            ON llm_requests(router_session_id, started_at);
        CREATE INDEX IF NOT EXISTS idx_llm_requests_model
            ON llm_requests(model, started_at);
        CREATE INDEX IF NOT EXISTS idx_llm_requests_started
            ON llm_requests(started_at);
        CREATE TABLE IF NOT EXISTS tool_calls (
            router_session_id TEXT NOT NULL,
            tool_call_id      TEXT NOT NULL,
            title             TEXT NOT NULL,
            status            TEXT NOT NULL,
            model             TEXT,
            started_at        INTEGER NOT NULL,
            updated_at        INTEGER NOT NULL,
            completed_at      INTEGER,
            detail            TEXT,
            PRIMARY KEY(router_session_id, tool_call_id),
            FOREIGN KEY(router_session_id) REFERENCES sessions(router_session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_tool_calls_active
            ON tool_calls(completed_at, router_session_id);
        CREATE VIEW IF NOT EXISTS active_tool_calls AS
            SELECT router_session_id, tool_call_id, title, status, model,
                   started_at, updated_at, detail
            FROM tool_calls
            WHERE completed_at IS NULL;
        "#;
        if let Err(err) = self.conn.execute_batch(sql) {
            tracing::error!(%err, "failed to initialize state schema");
        }
        // Migrations for DBs created by older versions: add columns that the
        // `CREATE TABLE IF NOT EXISTS` above skips on an existing table. A
        // duplicate-column error just means the migration already ran.
        for stmt in [
            "ALTER TABLE sessions ADD COLUMN prior_session_id TEXT",
            "ALTER TABLE sessions ADD COLUMN cost_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN native_subagent_calls INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN compute_ms INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN git_branch TEXT",
            "ALTER TABLE sessions ADD COLUMN git_sha TEXT",
            "ALTER TABLE sessions ADD COLUMN tokens_cache_read INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN tokens_cache_write INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN cost_estimated INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN llm_request_cost_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN llm_requests_total INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE session_log ADD COLUMN tokens_cache_read INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE session_log ADD COLUMN tokens_cache_write INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE session_log ADD COLUMN model TEXT",
        ] {
            if let Err(err) = self.conn.execute(stmt, [])
                && !err.to_string().contains("duplicate column")
            {
                tracing::error!(%err, stmt, "session state migration failed");
            }
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
            prior_session_id: row.get("prior_session_id")?,
            kind: row.get("kind")?,
            run_label: row.get("run_label")?,
            created_at: row.get::<_, Option<i64>>("created_at")?.map(|v| v as u64),
            updated_at: row.get::<_, Option<i64>>("updated_at")?.map(|v| v as u64),
            tokens_input: row.get::<_, i64>("tokens_input")? as u64,
            tokens_output: row.get::<_, i64>("tokens_output")? as u64,
            tokens_total: row.get::<_, i64>("tokens_total")? as u64,
            context_used: row.get::<_, i64>("context_used")? as u64,
            tokens_cache_read: row.get::<_, i64>("tokens_cache_read").unwrap_or(0) as u64,
            tokens_cache_write: row.get::<_, i64>("tokens_cache_write").unwrap_or(0) as u64,
            cost_usd: row.get::<_, f64>("cost_usd").unwrap_or(0.0),
            cost_estimated: row.get::<_, i64>("cost_estimated").unwrap_or(0) != 0,
            llm_request_cost_usd: row.get("llm_request_cost_usd").unwrap_or(0.0),
            llm_requests_total: row.get::<_, i64>("llm_requests_total").unwrap_or(0) as u64,
            native_subagent_calls: row.get::<_, i64>("native_subagent_calls").unwrap_or(0) as u64,
            compute_ms: row.get::<_, i64>("compute_ms").unwrap_or(0) as u64,
            git_branch: row.get("git_branch").unwrap_or(None),
            git_sha: row.get("git_sha").unwrap_or(None),
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
            if session.prior_session_id.is_none() {
                session.prior_session_id = existing.prior_session_id;
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
                additional_directories, title, routing, parent_session_id, prior_session_id, kind,
                run_label, created_at, updated_at, tokens_input, tokens_output, tokens_total,
                context_used)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)
             ON CONFLICT(router_session_id) DO UPDATE SET
                agent=excluded.agent, model=excluded.model,
                downstream_session_id=excluded.downstream_session_id, cwd=excluded.cwd,
                additional_directories=excluded.additional_directories, title=excluded.title,
                routing=excluded.routing, parent_session_id=excluded.parent_session_id,
                prior_session_id=excluded.prior_session_id,
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
                session.prior_session_id,
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
                 tokens_input, tokens_output, tokens_cache_read,
                 tokens_cache_write, tokens_estimated, model)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                router_session_id,
                now,
                entry.kind,
                entry.role,
                entry.summary,
                detail,
                entry.tokens_input as i64,
                entry.tokens_output as i64,
                entry.tokens_cache_read as i64,
                entry.tokens_cache_write as i64,
                entry.tokens_estimated as i64,
                entry.model,
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
                tokens_cache_read = tokens_cache_read + ?5,
                tokens_cache_write = tokens_cache_write + ?6,
                updated_at = ?7
             WHERE router_session_id = ?1",
            params![
                router_session_id,
                entry.tokens_input as i64,
                entry.tokens_output as i64,
                (entry.tokens_input + entry.tokens_output) as i64,
                entry.tokens_cache_read as i64,
                entry.tokens_cache_write as i64,
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

    /// Record the adapter-reported cumulative cost (USD); keeps the max seen.
    pub fn set_cost_usd(&self, router_session_id: &str, cost: f64) {
        let _ = self.conn.execute(
            "UPDATE sessions SET cost_usd=MAX(cost_usd, ?2) WHERE router_session_id=?1",
            params![router_session_id, cost],
        );
    }

    /// Accumulate a synthesized per-turn cost (from token counts × configured
    /// `pricing`) for a session whose adapter reports no cost of its own, and
    /// mark the row estimated. Never mixed with adapter-reported cost — the
    /// caller only synthesizes when no `usage_update.cost` was ever seen.
    pub fn add_estimated_cost(&self, router_session_id: &str, delta: f64) {
        let _ = self.conn.execute(
            "UPDATE sessions SET cost_usd = cost_usd + ?2, cost_estimated = 1              WHERE router_session_id=?1",
            params![router_session_id, delta],
        );
    }

    /// Total router-metered spend (`llm_requests.cost_usd`) for an agent
    /// since `since_epoch`, box-wide (the state DB is shared across every
    /// `router-acp serve` process). `models`, when given, restricts to those
    /// exact `"agent/model"` strings (a scoped plan window, e.g. Claude
    /// Fable's own weekly cap, must not count spend on sibling models toward
    /// its estimate). Feeds [`crate::usage::window_remaining_dollars`].
    pub fn llm_cost_since(&self, agent: &str, models: Option<&[String]>, since_epoch: i64) -> f64 {
        let query = match models {
            None => self.conn.query_row(
                "SELECT COALESCE(SUM(cost_usd), 0) FROM llm_requests \
                 WHERE agent = ?1 AND started_at >= ?2",
                params![agent, since_epoch],
                |row| row.get::<_, f64>(0),
            ),
            Some(models) if !models.is_empty() => {
                let placeholders = models.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let sql = format!(
                    "SELECT COALESCE(SUM(cost_usd), 0) FROM llm_requests \
                     WHERE agent = ?1 AND started_at >= ?2 AND model IN ({placeholders})"
                );
                let mut stmt_params: Vec<&dyn rusqlite::ToSql> = vec![&agent, &since_epoch];
                stmt_params.extend(models.iter().map(|m| m as &dyn rusqlite::ToSql));
                self.conn
                    .query_row(&sql, stmt_params.as_slice(), |row| row.get::<_, f64>(0))
            }
            Some(_) => return 0.0,
        };
        query.unwrap_or(0.0)
    }

    /// Start a provider-level request record. Probe/auth traffic has no owning
    /// session and is intentionally not passed here.
    pub fn start_llm_request(&self, request: &LlmRequestStart) {
        let now = now_epoch() as i64;
        let result = self.conn.execute(
            "INSERT INTO llm_requests
                (request_id, router_session_id, parent_router_session_id, agent,
                 protocol, endpoint, pinned_model, model, routing_reason,
                 routing_event, started_at, estimated_input_tokens)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                request.request_id,
                request.router_session_id,
                request.parent_router_session_id,
                request.agent,
                request.protocol,
                request.endpoint,
                request.pinned_model,
                request.model,
                request.routing_reason,
                request.routing_event,
                now,
                request.estimated_input_tokens as i64,
            ],
        );
        if let Err(err) = result {
            tracing::debug!(%err, request = request.request_id, "LLM request insert skipped");
            return;
        }
        let _ = self.conn.execute(
            "UPDATE sessions
             SET llm_requests_total=llm_requests_total+1, updated_at=?2
             WHERE router_session_id=?1",
            params![request.router_session_id, now],
        );
    }

    /// Finish an interposed request and accumulate API-equivalent request cost.
    /// ACP turn token totals are not incremented a second time.
    pub fn finish_llm_request(
        &self,
        request_id: &str,
        status: u16,
        duration_ms: u64,
        usage: &LlmRequestUsage,
        cost_usd: f64,
        error: Option<&str>,
    ) {
        let now = now_epoch() as i64;
        let session_id: Option<String> = self
            .conn
            .query_row(
                "SELECT router_session_id FROM llm_requests WHERE request_id=?1",
                params![request_id],
                |row| row.get(0),
            )
            .optional()
            .unwrap_or(None);
        let _ = self.conn.execute(
            "UPDATE llm_requests SET
                finished_at=?2, duration_ms=?3, status=?4,
                tokens_input=?5, tokens_output=?6, tokens_cache_read=?7,
                tokens_cache_write=?8, cost_usd=?9, error=?10
             WHERE request_id=?1",
            params![
                request_id,
                now,
                duration_ms as i64,
                i64::from(status),
                usage.input as i64,
                usage.output as i64,
                usage.cache_read as i64,
                usage.cache_write as i64,
                cost_usd,
                error,
            ],
        );
        if let Some(session_id) = session_id {
            let _ = self.conn.execute(
                "UPDATE sessions
                 SET llm_request_cost_usd=llm_request_cost_usd+?2, updated_at=?3
                 WHERE router_session_id=?1",
                params![session_id, cost_usd, now],
            );
        }
    }

    /// Upsert one tool's lifecycle. `active_tool_calls` exposes rows without a
    /// terminal completion timestamp.
    pub fn record_tool_call(
        &self,
        router_session_id: &str,
        tool_call_id: &str,
        title: &str,
        status: &str,
        model: Option<&str>,
        detail: &serde_json::Value,
    ) {
        if tool_call_id.is_empty() {
            return;
        }
        let now = now_epoch() as i64;
        let terminal = matches!(
            status.to_ascii_lowercase().as_str(),
            "completed" | "failed" | "cancelled" | "canceled" | "rejected"
        );
        let completed_at = terminal.then_some(now);
        let _ = self.conn.execute(
            "INSERT INTO tool_calls
                (router_session_id, tool_call_id, title, status, model,
                 started_at, updated_at, completed_at, detail)
             VALUES (?1,?2,?3,?4,?5,?6,?6,?7,?8)
             ON CONFLICT(router_session_id, tool_call_id) DO UPDATE SET
                title=excluded.title,
                status=excluded.status,
                model=COALESCE(tool_calls.model, excluded.model),
                updated_at=excluded.updated_at,
                completed_at=COALESCE(excluded.completed_at, tool_calls.completed_at),
                detail=excluded.detail",
            params![
                router_session_id,
                tool_call_id,
                title,
                status,
                model,
                now,
                completed_at,
                detail.to_string(),
            ],
        );
    }

    /// Increment the native-subagent-call counter (orchestration degradation).
    pub fn note_native_subagent(&self, router_session_id: &str) {
        let _ = self.conn.execute(
            "UPDATE sessions SET native_subagent_calls=native_subagent_calls+1 \
             WHERE router_session_id=?1",
            params![router_session_id],
        );
    }

    /// Add model compute time (ms) for a turn (excludes user idle).
    pub fn add_compute_ms(&self, router_session_id: &str, ms: u64) {
        let _ = self.conn.execute(
            "UPDATE sessions SET compute_ms=compute_ms+?2 WHERE router_session_id=?1",
            params![router_session_id, ms as i64],
        );
    }

    /// Tag a session with the git branch/HEAD of its cwd (for CI/merge join).
    pub fn set_git(&self, router_session_id: &str, branch: Option<&str>, sha: Option<&str>) {
        let _ = self.conn.execute(
            "UPDATE sessions SET git_branch=?2, git_sha=?3 WHERE router_session_id=?1",
            params![router_session_id, branch, sha],
        );
    }

    /// Recent log entries for a session (chronological).
    pub fn log_for(&self, router_session_id: &str, limit: usize) -> Vec<LogEntry> {
        let mut out = Vec::new();
        let Ok(mut stmt) = self.conn.prepare(
            "SELECT kind, role, summary, detail, tokens_input, tokens_output,
                    tokens_cache_read, tokens_cache_write, tokens_estimated, model
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
                tokens_cache_read: row.get::<_, i64>("tokens_cache_read").unwrap_or(0) as u64,
                tokens_cache_write: row.get::<_, i64>("tokens_cache_write").unwrap_or(0) as u64,
                tokens_estimated: row.get::<_, i64>("tokens_estimated")? != 0,
                model: row.get("model").unwrap_or(None),
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
    fn prior_session_id_round_trips_and_survives_later_upserts() {
        let (_d, s) = store();
        // Initial pin: no prior session.
        s.upsert("r1".into(), session("a"));
        assert_eq!(s.get("r1").unwrap().prior_session_id, None);
        // A switch records the old downstream session id as the prior session.
        s.upsert(
            "r1".into(),
            PersistedSession {
                prior_session_id: Some("downstream-old".into()),
                ..session("b")
            },
        );
        assert_eq!(
            s.get("r1").unwrap().prior_session_id.as_deref(),
            Some("downstream-old")
        );
        // A subsequent plain upsert (e.g. a token/touch update) must not wipe it.
        s.upsert("r1".into(), session("b"));
        assert_eq!(
            s.get("r1").unwrap().prior_session_id.as_deref(),
            Some("downstream-old"),
            "prior_session_id preserved across later upserts"
        );
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
    fn cost_native_compute_and_git_setters_persist() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("claude"));
        // cost keeps the max seen
        s.set_cost_usd("r1", 0.10);
        s.set_cost_usd("r1", 0.05);
        s.note_native_subagent("r1");
        s.note_native_subagent("r1");
        s.add_compute_ms("r1", 1500);
        s.add_compute_ms("r1", 500);
        s.set_git("r1", Some("feature-x"), Some("abc123"));
        let got = s.get("r1").unwrap();
        assert!((got.cost_usd - 0.10).abs() < 1e-9, "keeps max cost");
        assert_eq!(got.native_subagent_calls, 2);
        assert_eq!(got.compute_ms, 2000);
        assert_eq!(got.git_branch.as_deref(), Some("feature-x"));
        assert_eq!(got.git_sha.as_deref(), Some("abc123"));
    }

    #[test]
    fn cache_tokens_and_model_attribution_round_trip() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("codex"));
        s.log(
            "r1",
            &LogEntry {
                kind: "agent_response".into(),
                role: "agent".into(),
                summary: "done".into(),
                tokens_input: 100,
                tokens_output: 50,
                tokens_cache_read: 4000,
                tokens_cache_write: 200,
                model: Some("codex/gpt-5.5".into()),
                ..Default::default()
            },
        );
        let got = s.get("r1").unwrap();
        assert_eq!(got.tokens_cache_read, 4000);
        assert_eq!(got.tokens_cache_write, 200);
        let entries = s.log_for("r1", 10);
        assert_eq!(entries[0].tokens_cache_read, 4000);
        assert_eq!(entries[0].model.as_deref(), Some("codex/gpt-5.5"));
    }

    #[test]
    fn estimated_cost_accumulates_and_flags_the_row() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("codex"));
        assert!(!s.get("r1").unwrap().cost_estimated);
        s.add_estimated_cost("r1", 0.02);
        s.add_estimated_cost("r1", 0.03);
        let got = s.get("r1").unwrap();
        assert!(
            (got.cost_usd - 0.05).abs() < 1e-9,
            "estimated cost accumulates"
        );
        assert!(got.cost_estimated);
        // Adapter-reported cost keeps max semantics independently.
        s.upsert("r2".into(), session("claude"));
        s.set_cost_usd("r2", 0.10);
        assert!(!s.get("r2").unwrap().cost_estimated);
    }

    #[test]
    fn llm_requests_and_active_tools_are_queryable() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("claude"));
        s.start_llm_request(&LlmRequestStart {
            request_id: "q1".into(),
            router_session_id: "r1".into(),
            parent_router_session_id: None,
            agent: "claude".into(),
            protocol: "anthropic".into(),
            endpoint: "/v1/messages".into(),
            pinned_model: "claude/opus".into(),
            model: "claude/haiku".into(),
            routing_reason: "routine streak".into(),
            routing_event: "demotion".into(),
            estimated_input_tokens: 1000,
        });
        s.finish_llm_request(
            "q1",
            200,
            25,
            &LlmRequestUsage {
                input: 900,
                output: 100,
                cache_read: 800,
                cache_write: 0,
                input_includes_cache: false,
            },
            0.01,
            None,
        );
        let got = s.get("r1").unwrap();
        assert_eq!(got.llm_requests_total, 1);
        assert!((got.llm_request_cost_usd - 0.01).abs() < 1e-9);

        s.record_tool_call(
            "r1",
            "tool-1",
            "cargo test",
            "running",
            Some("claude/haiku"),
            &serde_json::json!({"toolCallId":"tool-1"}),
        );
        let active: (String, String) = s
            .conn
            .query_row(
                "SELECT tool_call_id, model FROM active_tool_calls",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(active, ("tool-1".into(), "claude/haiku".into()));
        s.record_tool_call(
            "r1",
            "tool-1",
            "cargo test",
            "completed",
            Some("claude/opus"),
            &serde_json::json!({"toolCallId":"tool-1","status":"completed"}),
        );
        let active_count: i64 = s
            .conn
            .query_row("SELECT COUNT(*) FROM active_tool_calls", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(active_count, 0);
        let tool_model: String = s
            .conn
            .query_row(
                "SELECT model FROM tool_calls WHERE tool_call_id='tool-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tool_model, "claude/haiku");
    }

    #[test]
    fn llm_cost_since_sums_by_agent_model_and_time() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("claude"));
        // Insert rows directly (bypassing `start_llm_request`'s real-clock
        // `started_at`) so the query can be exercised across a controlled
        // time boundary.
        let insert = |request_id: &str, agent: &str, model: &str, started_at: i64, cost: f64| {
            s.conn
                .execute(
                    "INSERT INTO llm_requests
                        (request_id, router_session_id, agent, protocol, endpoint,
                         pinned_model, model, routing_reason, routing_event,
                         started_at, cost_usd)
                     VALUES (?1, 'r1', ?2, 'anthropic', '/v1/messages', ?3, ?3, 'r', 'e', ?4, ?5)",
                    params![request_id, agent, model, started_at, cost],
                )
                .unwrap();
        };
        insert("q1", "claude", "claude/haiku", 1000, 0.10);
        insert("q2", "claude", "claude/sonnet", 2000, 0.20);
        insert("q3", "claude", "claude/haiku", 500, 0.05); // before the cutoff
        insert("q4", "codex", "codex/gpt-5.5", 1500, 0.30); // different agent

        // Whole-agent total since a cutoff excludes the earlier row and the
        // other agent's spend.
        assert!((s.llm_cost_since("claude", None, 1000) - 0.30).abs() < 1e-9);
        // Model-scoped: only the matching model string counts.
        assert!(
            (s.llm_cost_since("claude", Some(&["claude/haiku".to_string()]), 0) - 0.15).abs()
                < 1e-9
        );
        // Unknown agent / empty window: zero, never an error.
        assert_eq!(s.llm_cost_since("nonexistent", None, 0), 0.0);
        assert_eq!(s.llm_cost_since("claude", Some(&[]), 0), 0.0);
    }

    #[test]
    fn time_range_analytics_indexes_exist_after_load() {
        // Both new and pre-existing DBs get these on load (IF NOT EXISTS in
        // init_schema); the kory-code relay's analytics range queries depend
        // on them staying range-scannable as session_log/llm_requests grow.
        let (_d, s) = store();
        for (table, index, first_col) in [
            ("session_log", "idx_log_ts", "ts"),
            ("llm_requests", "idx_llm_requests_started", "started_at"),
        ] {
            let sql: String = s
                .conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=?1 AND name=?2",
                    params![table, index],
                    |row| row.get(0),
                )
                .unwrap_or_else(|_| panic!("index {index} missing on {table}"));
            let cols = sql.split_once('(').expect("index column list").1;
            assert!(
                cols.trim_start().starts_with(first_col),
                "{index} must lead with {first_col} to serve time-range scans: {sql}"
            );
        }
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
