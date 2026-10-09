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
//! Retention: sessions (and their logs) older than the `history` window are
//! pruned in small batches by the elected maintenance worker
//! (`crate::maintenance`), which also reclaims free pages and checkpoints the
//! WAL. Writes never prune inline.

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};

/// How long a write waits for another router process to release the
/// shared database. Many routers share one file on a workstation.
pub(crate) const BUSY_TIMEOUT: Duration = Duration::from_secs(30);
/// Streaming rows are written together once this many are queued...
const LOG_BATCH_ROWS: usize = 256;
/// ...or once the oldest queued row is this old.
const LOG_FLUSH_WINDOW: Duration = Duration::from_millis(500);
/// A flush that may defer waits this long for the lock. It runs under the
/// process-wide state mutex, so a long wait would stall every state access.
pub(crate) const DEFERRABLE_BUSY_WAIT: Duration = Duration::from_millis(200);
/// After a busy flush, chunks stop retrying inline for this long.
const FLUSH_BACKOFF: Duration = Duration::from_secs(1);
/// Past this many queued rows the oldest are dropped, bounding memory.
const LOG_QUEUE_CAP: usize = 50_000;
/// Deferral and drop warnings repeat at most this often.
const WARN_EVERY: Duration = Duration::from_secs(30);

/// Retention policy: sessions idle longer than `max_age` are pruned.
#[derive(Debug, Clone, Copy)]
pub struct Retention {
    pub max_age: Duration,
}

impl Retention {
    /// Epoch seconds before which an idle session is expired.
    pub fn cutoff(&self, now: u64) -> i64 {
        now.saturating_sub(self.max_age.as_secs()) as i64
    }
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
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize)]
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
    /// Native downstream session configuration checkpoint, used to restore a
    /// provider session after the router restarts.
    pub session_config: Option<serde_json::Value>,
    /// Router session id of the parent, for delegated sub-agent sessions.
    pub parent_session_id: Option<String>,
    /// The downstream session id this router session was pinned to *before*
    /// its most recent mid-session model switch (set by `switch_pin`). Traces
    /// the switch lineage; `None` for sessions that never switched.
    pub prior_session_id: Option<String>,
    /// `primary` (a normal pinned session) or `delegate` (a sub-agent
    /// spawned via `delegate_task`).
    pub kind: String,
    /// Optional grouping label (`[router: label=…]`) shared by
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
    /// Count of native (adapter built-in) sub-agent tool calls seen in a
    /// session told to use only the router's `delegate_task` — each one
    /// bypassed router delegation.
    pub native_subagent_calls: u64,
    /// Number of ordinary delegation directives injected into downstream model
    /// sessions. Detailed candidate/scope data remains in `session_log`.
    pub delegation_directive_injections: u64,
    /// Accumulated model compute time (prompt-sent → response), in ms —
    /// excludes user idle time between turns (unlike updated_at − created_at).
    pub compute_ms: u64,
    /// Git branch / HEAD sha of `cwd` at pin time, for joining a run to its CI
    /// or merge outcome later. `None` when cwd isn't a git repo.
    pub git_branch: Option<String>,
    pub git_sha: Option<String>,
}

/// One `session_log` row: a single ACP interaction.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct LogEntry {
    /// Unix timestamp when the router recorded this interaction.
    pub ts: Option<i64>,
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

/// Version for the supported read-only state-query JSON contract.
pub const STATE_QUERY_VERSION: u32 = 1;

/// Router-owned state boundary. Every caller opens state through this type so
/// future shard selection, retention, and query compatibility stay in the
/// router rather than leaking SQLite details to hosts.
pub struct StateStore {
    path: PathBuf,
    retention: Retention,
    file: StateFile,
}

impl std::fmt::Debug for StateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateStore")
            .field("path", &self.path)
            .field("retention", &self.retention)
            .finish_non_exhaustive()
    }
}

impl StateStore {
    /// Open durable state for router reads and writes. Retention remains an
    /// explicit input because a later sharded store must apply it to every
    /// legacy, active, and inactive shard.
    pub fn load(path: &Path, retention: Retention) -> Self {
        Self::try_load(path, retention)
            .unwrap_or_else(|err| panic!("cannot open state DB at {}: {err}", path.display()))
    }

    pub fn try_load(path: &Path, retention: Retention) -> rusqlite::Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            retention,
            file: StateFile::try_load(path, retention)?,
        })
    }

    /// Open an existing database for query-only inspection. This deliberately
    /// skips schema setup, legacy import, log flushing, and maintenance.
    pub fn open_readonly(path: &Path, retention: Retention) -> rusqlite::Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        Ok(Self {
            path: path.to_path_buf(),
            retention,
            file: StateFile::from_connection(conn, retention),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn retention(&self) -> Retention {
        self.retention
    }

    pub(crate) fn open_conn(path: &Path) -> rusqlite::Result<Connection> {
        StateFile::open_conn(path)
    }

    pub fn session_metadata(&self, router_session_id: &str) -> Option<SessionRecord> {
        self.file
            .get(router_session_id)
            .map(|session| SessionRecord {
                router_session_id: router_session_id.to_string(),
                session,
            })
    }

    /// Delegate-panel rows for every requested parent. This issues one child
    /// query and one log query, never one query per child.
    pub fn delegate_children(
        &self,
        parent_session_ids: &[String],
    ) -> rusqlite::Result<Vec<DelegateSession>> {
        let mut parent_ids = parent_session_ids.to_vec();
        parent_ids.sort();
        parent_ids.dedup();
        if parent_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = std::iter::repeat_n("?", parent_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT router_session_id, * FROM sessions WHERE parent_session_id IN ({placeholders}) \
             ORDER BY created_at, router_session_id"
        );
        let mut stmt = self.file.conn.prepare(&sql)?;
        let children: Vec<SessionRecord> = stmt
            .query_map(rusqlite::params_from_iter(parent_ids.iter()), |row| {
                Ok(SessionRecord {
                    router_session_id: row.get("router_session_id")?,
                    session: StateFile::row_to_session(row)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        if children.is_empty() {
            return Ok(Vec::new());
        }

        let child_ids: Vec<_> = children
            .iter()
            .map(|child| child.router_session_id.clone())
            .collect();
        let placeholders = std::iter::repeat_n("?", child_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT router_session_id, ts, kind, role, summary,
                    CASE WHEN kind IN ('delegate_task', 'delegate_followup', 'agent_response')
                         THEN detail END AS detail
             FROM session_log WHERE router_session_id IN ({placeholders}) ORDER BY router_session_id, id"
        );
        let mut logs = self.file.conn.prepare(&sql)?;
        let mut logs_by_child: std::collections::HashMap<String, Vec<DelegateLog>> =
            std::collections::HashMap::new();
        for row in logs.query_map(rusqlite::params_from_iter(child_ids.iter()), |row| {
            let detail: Option<String> = row.get("detail")?;
            Ok((
                row.get::<_, String>("router_session_id")?,
                DelegateLog {
                    ts: iso_timestamp(row.get("ts")?),
                    kind: row.get("kind")?,
                    role: row.get("role")?,
                    summary: row.get("summary")?,
                    detail: detail.and_then(|detail| serde_json::from_str(&detail).ok()),
                },
            ))
        })? {
            let (session_id, log) = row?;
            logs_by_child.entry(session_id).or_default().push(log);
        }

        Ok(children
            .into_iter()
            .map(|child| {
                let log = logs_by_child
                    .remove(&child.router_session_id)
                    .unwrap_or_default();
                DelegateSession {
                    id: child.router_session_id,
                    title: child.session.title.unwrap_or_default(),
                    agent: non_empty(child.session.agent),
                    model: non_empty(child.session.model),
                    created_at: child
                        .session
                        .created_at
                        .and_then(|seconds| i64::try_from(seconds).ok().and_then(iso_timestamp)),
                    updated_at: child
                        .session
                        .updated_at
                        .and_then(|seconds| i64::try_from(seconds).ok().and_then(iso_timestamp)),
                    tokens_total: child.session.tokens_total,
                    context_used: child.session.context_used,
                    routing: child.session.routing,
                    has_response: log.iter().any(|entry| entry.kind == "agent_response"),
                    log,
                }
            })
            .collect())
    }

    pub fn selected_logs(
        &self,
        router_session_id: &str,
        limit: usize,
        kind: Option<&str>,
    ) -> rusqlite::Result<Vec<LogEntry>> {
        let mut stmt = self.file.conn.prepare(
            "SELECT ts, kind, role, summary, detail, tokens_input, tokens_output,
                    tokens_cache_read, tokens_cache_write, tokens_estimated, model
             FROM session_log
             WHERE router_session_id=?1 AND (?2 IS NULL OR kind=?2)
             ORDER BY id DESC LIMIT ?3",
        )?;
        let mut entries: Vec<_> = stmt
            .query_map(
                params![router_session_id, kind, limit.max(1) as i64],
                StateFile::row_to_log_entry,
            )?
            .collect::<rusqlite::Result<_>>()?;
        entries.reverse();
        Ok(entries)
    }

    pub fn transcript(
        &self,
        router_session_id: &str,
        limit: usize,
    ) -> rusqlite::Result<Vec<LogEntry>> {
        self.selected_logs(router_session_id, limit, None)
    }

    pub fn analytics(&self, range: AnalyticsRange) -> rusqlite::Result<AnalyticsReport> {
        if range
            .from_sec
            .is_some_and(|from| range.to_end_sec.is_some_and(|to| from > to))
        {
            return Err(rusqlite::Error::InvalidParameterName(
                "from_sec must not exceed to_end_sec".into(),
            ));
        }
        let sessions = self
            .file
            .conn
            .prepare(
                "SELECT router_session_id, * FROM sessions
                 WHERE (?1 IS NULL OR created_at >= ?1) AND (?2 IS NULL OR created_at < ?2)
                 ORDER BY created_at, router_session_id",
            )?
            .query_map(params![range.from_sec, range.to_end_sec], |row| {
                let router_session_id: String = row.get("router_session_id")?;
                let session = StateFile::row_to_session(row)?;
                let (class, reason) = routing_class_reason(session.routing.as_ref());
                Ok(AnalyticsSession {
                    id: router_session_id,
                    agent: non_empty(session.agent),
                    model: non_empty(session.model),
                    kind: non_empty(session.kind),
                    run_label: session.run_label,
                    class,
                    reason,
                    created_at: session
                        .created_at
                        .and_then(|seconds| i64::try_from(seconds).ok().and_then(iso_timestamp)),
                    updated_at: session
                        .updated_at
                        .and_then(|seconds| i64::try_from(seconds).ok().and_then(iso_timestamp)),
                    tokens_input: session.tokens_input,
                    tokens_output: session.tokens_output,
                    tokens_total: session.tokens_total,
                    context_used: session.context_used,
                    cost_usd: session.cost_usd,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut daily = std::collections::BTreeMap::new();
        let mut stmt = self.file.conn.prepare(
            "SELECT date(log.ts, 'unixepoch') AS date, session.agent, session.kind, session.routing,
                    SUM(log.tokens_input), SUM(log.tokens_output), COUNT(*)
             FROM session_log AS log
             LEFT JOIN sessions AS session ON session.router_session_id = log.router_session_id
             WHERE (?1 IS NULL OR log.ts >= ?1) AND (?2 IS NULL OR log.ts < ?2)
             GROUP BY date, log.router_session_id, session.agent, session.kind, session.routing
             ORDER BY date, log.router_session_id",
        )?;
        for row in stmt.query_map(params![range.from_sec, range.to_end_sec], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })? {
            let (date, agent, kind, routing, tokens_input, tokens_output, entries) = row?;
            let routing = routing.and_then(|value| serde_json::from_str(&value).ok());
            let (class, _) = routing_class_reason(routing.as_ref());
            let key = (date.clone(), agent.clone(), class.clone(), kind.clone());
            let bucket = daily.entry(key).or_insert_with(|| AnalyticsDaily {
                date,
                agent,
                class,
                kind,
                tokens_input: 0,
                tokens_output: 0,
                entries: 0,
            });
            bucket.tokens_input += tokens_input.max(0) as u64;
            bucket.tokens_output += tokens_output.max(0) as u64;
            bucket.entries += entries.max(0) as u64;
        }

        let llm_requests = self
            .file
            .conn
            .prepare(
                "SELECT pinned_model, model, protocol, started_at, tokens_input, tokens_output,
                        tokens_cache_read, tokens_cache_write
                 FROM llm_requests
                 WHERE (?1 IS NULL OR started_at >= ?1) AND (?2 IS NULL OR started_at < ?2)
                 ORDER BY started_at, request_id",
            )?
            .query_map(params![range.from_sec, range.to_end_sec], |row| {
                Ok(SavingsRequest {
                    pinned_model: row.get(0)?,
                    model: row.get(1)?,
                    protocol: row.get(2)?,
                    started_at: row.get(3)?,
                    tokens_input: row.get::<_, i64>(4)?.max(0) as u64,
                    tokens_output: row.get::<_, i64>(5)?.max(0) as u64,
                    tokens_cache_read: row.get::<_, i64>(6)?.max(0) as u64,
                    tokens_cache_write: row.get::<_, i64>(7)?.max(0) as u64,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(AnalyticsReport {
            sessions,
            daily: daily.into_values().collect(),
            llm_requests,
        })
    }

    pub fn health(&self) -> rusqlite::Result<StateHealth> {
        let (sessions, logs, active_tools): (i64, i64, i64) = self.file.conn.query_row(
            "SELECT (SELECT COUNT(*) FROM sessions), (SELECT COUNT(*) FROM session_log),
                    (SELECT COUNT(*) FROM active_tool_calls)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let pragma = |name| {
            self.file
                .conn
                .pragma_query_value(None, name, |row| row.get::<_, i64>(0))
        };
        let page_size = pragma("page_size")?;
        let freelist_count = pragma("freelist_count")?;
        let auto_vacuum = match pragma("auto_vacuum")? {
            1 => "full",
            2 => "incremental",
            _ => "none",
        };
        let has_lease = self
            .file
            .conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'maintenance_lease'",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        let maintenance = has_lease
            .then(|| {
                self.file.conn.query_row(
                    "SELECT holder, last_tick_at, last_result FROM maintenance_lease WHERE id = 1",
                    [],
                    |row| {
                        let result: Option<String> = row.get(2)?;
                        Ok(StateMaintenance {
                            holder: row.get(0)?,
                            last_tick_at: row.get::<_, Option<i64>>(1)?.map(|value| value.max(0) as u64 * 1000),
                            last_result: result.and_then(|value| serde_json::from_str(&value).ok()),
                        })
                    },
                ).optional()
            })
            .transpose()?
            .flatten();
        Ok(StateHealth {
            db_path: self.path.display().to_string(),
            db_bytes: file_len(&self.path),
            wal_bytes: file_len(&wal_path(&self.path)),
            freelist_bytes: (page_size * freelist_count).max(0) as u64,
            auto_vacuum,
            maintenance,
            retention_seconds: self.retention.max_age.as_secs(),
            sessions: sessions.max(0) as u64,
            log_entries: logs.max(0) as u64,
            active_tool_calls: active_tools.max(0) as u64,
        })
    }

    pub fn delegation_report(&self, limit: usize) -> DelegationReport {
        let all = self.file.all();
        let mut children: std::collections::HashMap<String, Vec<_>> =
            std::collections::HashMap::new();
        for (id, session) in &all {
            if let Some(parent) = &session.parent_session_id {
                children
                    .entry(parent.clone())
                    .or_default()
                    .push((id.clone(), session.clone()));
            }
        }
        let prompted: Vec<_> = all
            .iter()
            .filter(|(_, session)| {
                session.kind == "primary" && session.delegation_directive_injections > 0
            })
            .collect();
        let effective_cost = |session: &PersistedSession| {
            if session.llm_requests_total > 0 && session.llm_request_cost_usd > 0.0 {
                session.llm_request_cost_usd
            } else {
                session.cost_usd
            }
        };
        let adopted = prompted
            .iter()
            .filter(|(id, _)| children.get(id).is_some_and(|kids| !kids.is_empty()))
            .count() as u64;
        let directive_injections = prompted
            .iter()
            .map(|(_, s)| s.delegation_directive_injections)
            .sum();
        let native_bypass_calls = prompted.iter().map(|(_, s)| s.native_subagent_calls).sum();
        let parent_cost_usd = prompted.iter().map(|(_, s)| effective_cost(s)).sum();
        let delegate_cost_usd = prompted
            .iter()
            .flat_map(|(id, _)| children.get(id).into_iter().flatten())
            .map(|(_, s)| effective_cost(s))
            .sum();
        let sessions = prompted
            .iter()
            .take(limit.max(1))
            .map(|(id, s)| DelegationSessionReport {
                router_session_id: (*id).clone(),
                agent: s.agent.clone(),
                model: s.model.clone(),
                directive_injections: s.delegation_directive_injections,
                delegates: children.get(id).map_or(0, Vec::len) as u64,
                native_bypass_calls: s.native_subagent_calls,
            })
            .collect();
        DelegationReport {
            prompted_sessions: prompted.len() as u64,
            sessions_that_delegated: adopted,
            directive_injections,
            native_bypass_calls,
            parent_cost_usd,
            delegate_cost_usd,
            total_cost_usd: parent_cost_usd + delegate_cost_usd,
            sessions,
        }
    }

    pub fn get(&self, router_session_id: &str) -> Option<PersistedSession> {
        self.file.get(router_session_id)
    }

    pub fn all(&self) -> Vec<(String, PersistedSession)> {
        self.file.all()
    }

    pub fn iter(&self) -> impl Iterator<Item = (String, PersistedSession)> {
        self.all().into_iter()
    }

    pub fn find_by_downstream(&self, agent: &str, downstream_session_id: &str) -> Option<String> {
        self.file.find_by_downstream(agent, downstream_session_id)
    }

    pub fn upsert(&self, router_session_id: String, session: PersistedSession) {
        self.file.upsert(router_session_id, session);
    }

    pub fn upsert_checked(
        &self,
        router_session_id: String,
        session: PersistedSession,
    ) -> rusqlite::Result<()> {
        self.file.upsert_checked(router_session_id, session)
    }

    pub fn set_session_config(
        &self,
        router_session_id: &str,
        value: &serde_json::Value,
    ) -> rusqlite::Result<()> {
        self.file.set_session_config(router_session_id, value)
    }

    pub fn set_title(&self, router_session_id: &str, title: &str) {
        self.file.set_title(router_session_id, title);
    }

    pub fn touch(&self, router_session_id: &str) {
        self.file.touch(router_session_id);
    }

    pub fn remove(&self, router_session_id: &str) -> Option<PersistedSession> {
        self.file.remove(router_session_id)
    }

    pub fn log(&self, router_session_id: &str, entry: &LogEntry) {
        self.file.log(router_session_id, entry);
    }

    pub fn log_checked(&self, router_session_id: &str, entry: &LogEntry) -> rusqlite::Result<()> {
        self.file.log_checked(router_session_id, entry)
    }

    pub fn log_buffered(&self, router_session_id: &str, entry: LogEntry) {
        self.file.log_buffered(router_session_id, entry);
    }

    pub fn flush_log(&self) -> rusqlite::Result<()> {
        self.file.flush_log()
    }

    pub fn flush_log_final(&self) -> rusqlite::Result<()> {
        self.file.flush_log_final()
    }

    pub fn flush_log_or_warn(&self) {
        self.file.flush_log_or_warn();
    }

    pub fn pending_log_rows(&self) -> usize {
        self.file.pending_log_rows()
    }

    pub fn set_context_used(&self, router_session_id: &str, used: u64) {
        self.file.set_context_used(router_session_id, used);
    }

    pub fn set_cost_usd(&self, router_session_id: &str, cost: f64) {
        self.file.set_cost_usd(router_session_id, cost);
    }

    pub fn add_estimated_cost(&self, router_session_id: &str, delta: f64) {
        self.file.add_estimated_cost(router_session_id, delta);
    }

    pub fn llm_cost_since(&self, agent: &str, models: Option<&[String]>, since_epoch: i64) -> f64 {
        self.file.llm_cost_since(agent, models, since_epoch)
    }

    pub fn start_llm_request(&self, request: &LlmRequestStart) {
        self.file.start_llm_request(request);
    }

    pub fn finish_llm_request(
        &self,
        request_id: &str,
        status: u16,
        duration_ms: u64,
        usage: &LlmRequestUsage,
        cost_usd: f64,
        error: Option<&str>,
    ) {
        self.file
            .finish_llm_request(request_id, status, duration_ms, usage, cost_usd, error);
    }

    pub fn record_tool_call(
        &self,
        router_session_id: &str,
        tool_call_id: &str,
        title: &str,
        status: &str,
        model: Option<&str>,
        detail: &serde_json::Value,
    ) {
        self.file.record_tool_call(
            router_session_id,
            tool_call_id,
            title,
            status,
            model,
            detail,
        );
    }

    pub fn note_native_subagent(&self, router_session_id: &str) {
        self.file.note_native_subagent(router_session_id);
    }

    pub fn note_delegation_directive(&self, router_session_id: &str) {
        self.file.note_delegation_directive(router_session_id);
    }

    pub fn add_compute_ms(&self, router_session_id: &str, ms: u64) {
        self.file.add_compute_ms(router_session_id, ms);
    }

    pub fn set_git(&self, router_session_id: &str, branch: Option<&str>, sha: Option<&str>) {
        self.file.set_git(router_session_id, branch, sha);
    }

    pub fn log_for_all(&self, router_session_id: &str) -> rusqlite::Result<Vec<LogEntry>> {
        self.file.log_for_all(router_session_id)
    }

    pub fn log_for(&self, router_session_id: &str, limit: usize) -> Vec<LogEntry> {
        self.file.log_for(router_session_id, limit)
    }

    /// Keep legacy single-file pruning behavior behind the StateStore. The
    /// sharding follow-up can replace this with cross-shard coordination.
    pub fn prune(&self) -> usize {
        self.file.prune()
    }

    pub fn prune_at(&self, now: u64) -> usize {
        self.file.prune_at(now)
    }

    pub fn outbox_push(&self, payload: &str) -> Option<i64> {
        self.file.outbox_push(payload)
    }

    pub fn outbox_pending(&self, limit: usize) -> Vec<(i64, String)> {
        self.file.outbox_pending(limit)
    }

    pub fn outbox_done(&self, id: i64) {
        self.file.outbox_done(id);
    }

    pub fn outbox_attempted(&self, id: i64) {
        self.file.outbox_attempted(id);
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SessionRecord {
    pub router_session_id: String,
    #[serde(flatten)]
    pub session: PersistedSession,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DelegateLog {
    pub ts: Option<String>,
    pub kind: String,
    pub role: String,
    pub summary: String,
    pub detail: Option<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DelegateSession {
    pub id: String,
    pub title: String,
    pub agent: Option<String>,
    pub model: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub tokens_total: u64,
    pub context_used: u64,
    pub routing: Option<serde_json::Value>,
    pub has_response: bool,
    pub log: Vec<DelegateLog>,
}

#[derive(Debug, Clone, Copy)]
pub struct AnalyticsRange {
    /// Inclusive Unix-second start of the range.
    pub from_sec: Option<i64>,
    /// Exclusive Unix-second end of the range.
    pub to_end_sec: Option<i64>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsSession {
    pub id: String,
    pub agent: Option<String>,
    pub model: Option<String>,
    pub kind: Option<String>,
    pub run_label: Option<String>,
    pub class: Option<String>,
    pub reason: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub tokens_input: u64,
    pub tokens_output: u64,
    pub tokens_total: u64,
    pub context_used: u64,
    pub cost_usd: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsDaily {
    pub date: String,
    pub agent: Option<String>,
    pub class: Option<String>,
    pub kind: Option<String>,
    pub tokens_input: u64,
    pub tokens_output: u64,
    pub entries: u64,
}

/// Fields consumed by the existing router savings aggregation. Field names
/// deliberately match its prior SQLite row shape.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SavingsRequest {
    pub pinned_model: String,
    pub model: String,
    pub protocol: String,
    pub started_at: i64,
    pub tokens_input: u64,
    pub tokens_output: u64,
    pub tokens_cache_read: u64,
    pub tokens_cache_write: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsReport {
    pub sessions: Vec<AnalyticsSession>,
    pub daily: Vec<AnalyticsDaily>,
    pub llm_requests: Vec<SavingsRequest>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateMaintenance {
    pub holder: String,
    pub last_tick_at: Option<u64>,
    pub last_result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateHealth {
    pub db_path: String,
    pub db_bytes: u64,
    pub wal_bytes: u64,
    pub freelist_bytes: u64,
    pub auto_vacuum: &'static str,
    pub maintenance: Option<StateMaintenance>,
    pub retention_seconds: u64,
    pub sessions: u64,
    pub log_entries: u64,
    pub active_tool_calls: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DelegationSessionReport {
    pub router_session_id: String,
    pub agent: String,
    pub model: String,
    pub directive_injections: u64,
    pub delegates: u64,
    pub native_bypass_calls: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DelegationReport {
    pub prompted_sessions: u64,
    pub sessions_that_delegated: u64,
    pub directive_injections: u64,
    pub native_bypass_calls: u64,
    pub parent_cost_usd: f64,
    pub delegate_cost_usd: f64,
    pub total_cost_usd: f64,
    pub sessions: Vec<DelegationSessionReport>,
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn wal_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", path.display()))
}

fn iso_timestamp(seconds: i64) -> Option<String> {
    (seconds > 0)
        .then(|| chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0))
        .flatten()
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn routing_class_reason(routing: Option<&serde_json::Value>) -> (Option<String>, Option<String>) {
    let class = routing
        .and_then(|value| value.get("class"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let reason = routing
        .and_then(|value| value.get("reason"))
        .and_then(serde_json::Value::as_str)
        .map(|value| value.chars().take(200).collect());
    (class, reason)
}

/// Raw single-file SQLite implementation. `StateStore` is the public
/// boundary; this remains private to the router so future shard layout does
/// not leak to Kory Code or other hosts.
struct StateFile {
    conn: Connection,
    retention: Retention,
    /// Streaming `session_log` rows not yet written, oldest first.
    pending_log: RefCell<Vec<PendingLog>>,
    flush_backoff_until: Cell<Option<Instant>>,
    defer_warned_at: Cell<Option<Instant>>,
    drop_warned_at: Cell<Option<Instant>>,
}

struct PendingLog {
    router_session_id: String,
    ts: i64,
    queued_at: Instant,
    entry: LogEntry,
}

impl std::fmt::Debug for StateFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateFile").finish_non_exhaustive()
    }
}

impl StateFile {
    /// Open durable state or fail. A disposable fallback would return session
    /// ids that cannot survive the next router process.
    pub fn try_load(path: &Path, retention: Retention) -> rusqlite::Result<Self> {
        let conn = Self::open_conn(path)?;
        let store = Self {
            conn,
            retention,
            pending_log: RefCell::default(),
            flush_backoff_until: Cell::default(),
            defer_warned_at: Cell::default(),
            drop_warned_at: Cell::default(),
        };
        store.init_schema()?;
        // One-time import of a legacy sessions.json sitting next to the DB.
        store.import_legacy_json(path);
        Ok(store)
    }

    fn from_connection(conn: Connection, retention: Retention) -> Self {
        Self {
            conn,
            retention,
            pending_log: RefCell::default(),
            flush_backoff_until: Cell::default(),
            defer_warned_at: Cell::default(),
            drop_warned_at: Cell::default(),
        }
    }

    pub(crate) fn open_conn(path: &Path) -> rusqlite::Result<Connection> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)?;
        // Concurrent router startups can contend while enabling WAL. Install
        // the wait first so a transient lock cannot select the memory fallback.
        conn.busy_timeout(BUSY_TIMEOUT)?;
        // SQLite can refuse a journal-mode lock upgrade without invoking its
        // busy handler. Retry the statement after concurrent startup releases it.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match conn.pragma_update(None, "journal_mode", "WAL") {
                Ok(()) => break,
                Err(rusqlite::Error::SqliteFailure(error, _))
                    if matches!(
                        error.code,
                        rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                    ) && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(error) => return Err(error),
            }
        }
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(conn)
    }

    fn init_schema(&self) -> rusqlite::Result<()> {
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
            session_config        TEXT,
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
            delegation_directive_injections INTEGER NOT NULL DEFAULT 0,
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
        -- Lifecycle-hook events (`delegate_stop`, `parent_repinned`) not yet
        -- accepted by the host. Every router process sharing this DB flushes
        -- it, so an event survives a hook failure or a router restart.
        CREATE TABLE IF NOT EXISTS hook_outbox (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            payload    TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            attempts   INTEGER NOT NULL DEFAULT 0
        );
        -- One row: which router process runs maintenance, until when, and
        -- what its last tick did. See `crate::maintenance`.
        CREATE TABLE IF NOT EXISTS maintenance_lease (
            id           INTEGER PRIMARY KEY CHECK (id = 1),
            holder       TEXT NOT NULL,
            expires_at   INTEGER NOT NULL,
            last_tick_at INTEGER,
            last_result  TEXT
        );
        "#;
        self.conn.execute_batch(sql)?;
        // Migrations for DBs created by older versions: add columns that the
        // `CREATE TABLE IF NOT EXISTS` above skips on an existing table. A
        // duplicate-column error just means the migration already ran.
        for stmt in [
            "ALTER TABLE sessions ADD COLUMN prior_session_id TEXT",
            "ALTER TABLE sessions ADD COLUMN cost_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN native_subagent_calls INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN delegation_directive_injections INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN compute_ms INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN git_branch TEXT",
            "ALTER TABLE sessions ADD COLUMN git_sha TEXT",
            "ALTER TABLE sessions ADD COLUMN tokens_cache_read INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN tokens_cache_write INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN cost_estimated INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN llm_request_cost_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN llm_requests_total INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sessions ADD COLUMN session_config TEXT",
            "ALTER TABLE session_log ADD COLUMN tokens_cache_read INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE session_log ADD COLUMN tokens_cache_write INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE session_log ADD COLUMN model TEXT",
        ] {
            if let Err(err) = self.conn.execute(stmt, []) {
                if err.to_string().contains("duplicate column") {
                    continue;
                }
                return Err(err);
            }
        }
        Ok(())
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
        let session_config: Option<String> = row.get("session_config")?;
        Ok(PersistedSession {
            agent: row.get("agent")?,
            model: row.get("model")?,
            downstream_session_id: row.get("downstream_session_id")?,
            cwd: PathBuf::from(row.get::<_, String>("cwd")?),
            additional_directories: serde_json::from_str(&dirs).unwrap_or_default(),
            title: row.get("title")?,
            routing: routing.and_then(|r| serde_json::from_str(&r).ok()),
            session_config: session_config.and_then(|config| serde_json::from_str(&config).ok()),
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
            delegation_directive_injections: row
                .get::<_, i64>("delegation_directive_injections")
                .unwrap_or(0) as u64,
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

    pub fn upsert(&self, router_session_id: String, session: PersistedSession) {
        if let Err(err) = self.upsert_checked(router_session_id, session) {
            tracing::error!(%err, "failed to upsert session");
        }
    }

    pub fn upsert_checked(
        &self,
        router_session_id: String,
        mut session: PersistedSession,
    ) -> rusqlite::Result<()> {
        let now = now_epoch();
        // Preserve creation time, title, session configuration, and accumulated token counters
        // across re-pins (failover) unless fresh values are supplied.
        if let Some(existing) = self.get(&router_session_id) {
            session.created_at = session.created_at.or(existing.created_at);
            if session.title.is_none() {
                session.title = existing.title;
            }
            if session.session_config.is_none() {
                session.session_config = existing.session_config;
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
        let session_config = session
            .session_config
            .as_ref()
            .map(|config| config.to_string());
        self.conn.execute(
            "INSERT INTO sessions (router_session_id, agent, model, downstream_session_id, cwd,
                additional_directories, title, routing, session_config, parent_session_id, prior_session_id, kind,
                run_label, created_at, updated_at, tokens_input, tokens_output, tokens_total,
                context_used)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)
             ON CONFLICT(router_session_id) DO UPDATE SET
                agent=excluded.agent, model=excluded.model,
                downstream_session_id=excluded.downstream_session_id, cwd=excluded.cwd,
                additional_directories=excluded.additional_directories, title=excluded.title,
                routing=excluded.routing, session_config=excluded.session_config,
                parent_session_id=excluded.parent_session_id,
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
                session_config,
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
        )?;
        Ok(())
    }

    /// Persist a native downstream session configuration checkpoint.
    pub fn set_session_config(
        &self,
        router_session_id: &str,
        value: &serde_json::Value,
    ) -> rusqlite::Result<()> {
        let changed = self.conn.execute(
            "UPDATE sessions SET session_config=?2, updated_at=?3 WHERE router_session_id=?1",
            params![router_session_id, value.to_string(), now_epoch() as i64],
        )?;
        if changed == 0 {
            return Err(rusqlite::Error::QueryReturnedNoRows);
        }
        Ok(())
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
        if let Err(err) = self.log_checked(router_session_id, entry) {
            tracing::debug!(%err, session = router_session_id, "session_log insert skipped");
        }
    }

    /// Append a row now, after any queued streaming rows so the log keeps
    /// arrival order. All of them commit in one transaction.
    pub fn log_checked(&self, router_session_id: &str, entry: &LogEntry) -> rusqlite::Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let written = self.write_pending(&tx)?;
        Self::insert_log(&tx, router_session_id, now_epoch() as i64, entry)?;
        tx.commit()?;
        self.pending_log.borrow_mut().drain(..written);
        self.flush_backoff_until.set(None);
        Ok(())
    }

    /// Queue a streaming row (raw ACP update, tool progress). It is written
    /// with the next batch, so one SQLite write covers many chunks.
    pub fn log_buffered(&self, router_session_id: &str, entry: LogEntry) {
        let mut pending = self.pending_log.borrow_mut();
        pending.push(PendingLog {
            router_session_id: router_session_id.to_string(),
            ts: now_epoch() as i64,
            queued_at: Instant::now(),
            entry,
        });
        if pending.len() > LOG_QUEUE_CAP {
            let dropped = pending.len() - LOG_QUEUE_CAP;
            pending.drain(..dropped);
            if throttle(&self.drop_warned_at) {
                tracing::warn!(
                    dropped,
                    "session_log queue full; dropping the oldest streaming rows"
                );
            }
        }
        let due =
            pending.len() >= LOG_BATCH_ROWS || pending[0].queued_at.elapsed() >= LOG_FLUSH_WINDOW;
        drop(pending);
        let backing_off = self
            .flush_backoff_until
            .get()
            .is_some_and(|until| Instant::now() < until);
        if due && !backing_off {
            self.flush_log_or_warn();
        }
    }

    /// Write every queued streaming row in one transaction, waiting only
    /// briefly for the lock. When the database is busy the rows stay queued,
    /// in order, and inline flushes pause for `FLUSH_BACKOFF`.
    pub fn flush_log(&self) -> rusqlite::Result<()> {
        self.flush_log_waiting(DEFERRABLE_BUSY_WAIT)
    }

    /// Final flush at shutdown: nothing else is waiting on the state mutex,
    /// so wait the full lock timeout rather than lose the rows.
    pub fn flush_log_final(&self) -> rusqlite::Result<()> {
        self.flush_log_waiting(BUSY_TIMEOUT)
    }

    fn flush_log_waiting(&self, wait: Duration) -> rusqlite::Result<()> {
        if self.pending_log.borrow().is_empty() {
            return Ok(());
        }
        self.conn.busy_timeout(wait)?;
        let result = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .and_then(|tx| {
                let written = self.write_pending(&tx)?;
                tx.commit()?;
                Ok(written)
            });
        self.conn.busy_timeout(BUSY_TIMEOUT)?;
        match result {
            Ok(written) => {
                self.pending_log.borrow_mut().drain(..written);
                self.flush_backoff_until.set(None);
                Ok(())
            }
            Err(err) => {
                if is_busy(&err) {
                    self.flush_backoff_until
                        .set(Some(Instant::now() + FLUSH_BACKOFF));
                }
                Err(err)
            }
        }
    }

    /// `flush_log`, warning at most once per `WARN_EVERY` when it defers.
    pub fn flush_log_or_warn(&self) {
        if let Err(err) = self.flush_log()
            && throttle(&self.defer_warned_at)
        {
            tracing::warn!(
                %err,
                queued = self.pending_log_rows(),
                "session_log batch deferred; retrying on the next flush"
            );
        }
    }

    /// Number of streaming rows waiting for a flush.
    pub fn pending_log_rows(&self) -> usize {
        self.pending_log.borrow().len()
    }

    /// Insert the queued rows into `tx` and return how many it covered. A
    /// busy/locked error aborts so the caller keeps them; a row SQLite rejects
    /// outright (its session was deleted) can never succeed and is skipped.
    fn write_pending(&self, tx: &Transaction<'_>) -> rusqlite::Result<usize> {
        let pending = self.pending_log.borrow();
        for row in pending.iter() {
            match Self::insert_log(tx, &row.router_session_id, row.ts, &row.entry) {
                Ok(()) => {}
                Err(err) if is_busy(&err) => return Err(err),
                Err(err) => tracing::warn!(
                    %err,
                    session = row.router_session_id,
                    "session_log row dropped"
                ),
            }
        }
        Ok(pending.len())
    }

    fn insert_log(
        conn: &Connection,
        router_session_id: &str,
        now: i64,
        entry: &LogEntry,
    ) -> rusqlite::Result<()> {
        let detail = entry.detail.as_ref().map(|d| d.to_string());
        conn.execute(
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
        )?;
        conn.execute(
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
        )?;
        Ok(())
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
    /// terminal completion timestamp. A finished call keeps no `detail`: only
    /// the in-flight view reads it, and `session_log` already holds the output.
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
        let detail = (!terminal).then(|| detail.to_string());
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
                detail,
            ],
        );
    }

    /// Increment the native-subagent-call counter (router delegation bypassed).
    pub fn note_native_subagent(&self, router_session_id: &str) {
        let _ = self.conn.execute(
            "UPDATE sessions SET native_subagent_calls=native_subagent_calls+1 \
             WHERE router_session_id=?1",
            params![router_session_id],
        );
    }

    /// Increment the cheap session-level counter used by delegation reports.
    /// The corresponding detailed event is stored separately in session_log.
    pub fn note_delegation_directive(&self, router_session_id: &str) {
        let _ = self.conn.execute(
            "UPDATE sessions SET \
             delegation_directive_injections=delegation_directive_injections+1 \
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

    fn row_to_log_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<LogEntry> {
        let detail: Option<String> = row.get("detail")?;
        Ok(LogEntry {
            ts: row.get("ts").ok(),
            kind: row.get("kind")?,
            role: row.get("role")?,
            summary: row.get("summary")?,
            detail: detail.and_then(|detail| serde_json::from_str(&detail).ok()),
            tokens_input: row.get::<_, i64>("tokens_input")? as u64,
            tokens_output: row.get::<_, i64>("tokens_output")? as u64,
            tokens_cache_read: row.get::<_, i64>("tokens_cache_read").unwrap_or(0) as u64,
            tokens_cache_write: row.get::<_, i64>("tokens_cache_write").unwrap_or(0) as u64,
            tokens_estimated: row.get::<_, i64>("tokens_estimated")? != 0,
            model: row.get("model").unwrap_or(None),
        })
    }

    /// Every log entry for a session, in chronological order.
    pub fn log_for_all(&self, router_session_id: &str) -> rusqlite::Result<Vec<LogEntry>> {
        self.flush_log_or_warn();
        let mut stmt = self.conn.prepare(
            "SELECT ts, kind, role, summary, detail, tokens_input, tokens_output,
                    tokens_cache_read, tokens_cache_write, tokens_estimated, model
             FROM session_log WHERE router_session_id=?1 ORDER BY id",
        )?;
        stmt.query_map(params![router_session_id], Self::row_to_log_entry)?
            .collect()
    }

    /// Recent log entries for a session (chronological).
    pub fn log_for(&self, router_session_id: &str, limit: usize) -> Vec<LogEntry> {
        let mut out = Vec::new();
        self.flush_log_or_warn();
        let Ok(mut stmt) = self.conn.prepare(
            "SELECT ts, kind, role, summary, detail, tokens_input, tokens_output,
                    tokens_cache_read, tokens_cache_write, tokens_estimated, model
             FROM session_log WHERE router_session_id=?1 ORDER BY id DESC LIMIT ?2",
        ) else {
            return out;
        };
        let rows = stmt.query_map(
            params![router_session_id, limit as i64],
            Self::row_to_log_entry,
        );
        if let Ok(rows) = rows {
            for r in rows.flatten() {
                out.push(r);
            }
        }
        out.reverse();
        out
    }

    /// Delete sessions idle past `max_age`, with their rows in every table.
    pub fn prune(&self) -> usize {
        self.prune_at(now_epoch())
    }

    /// Queue a lifecycle-hook event; returns its outbox id.
    pub fn outbox_push(&self, payload: &str) -> Option<i64> {
        match self.conn.execute(
            "INSERT INTO hook_outbox (payload, created_at) VALUES (?1, ?2)",
            params![payload, now_epoch() as i64],
        ) {
            Ok(_) => Some(self.conn.last_insert_rowid()),
            Err(err) => {
                tracing::error!(%err, "cannot queue lifecycle-hook event");
                None
            }
        }
    }

    /// Undelivered events, oldest first.
    pub fn outbox_pending(&self, limit: usize) -> Vec<(i64, String)> {
        let Ok(mut stmt) = self
            .conn
            .prepare("SELECT id, payload FROM hook_outbox ORDER BY id LIMIT ?1")
        else {
            return Vec::new();
        };
        stmt.query_map(params![limit as i64], |row| Ok((row.get(0)?, row.get(1)?)))
            .map(|rows| rows.flatten().collect())
            .unwrap_or_default()
    }

    pub fn outbox_done(&self, id: i64) {
        let _ = self
            .conn
            .execute("DELETE FROM hook_outbox WHERE id = ?1", params![id]);
    }

    pub fn outbox_attempted(&self, id: i64) {
        let _ = self.conn.execute(
            "UPDATE hook_outbox SET attempts = attempts + 1 WHERE id = ?1",
            params![id],
        );
    }

    /// `prune` against a fixed clock, in the maintenance worker's batches.
    pub fn prune_at(&self, now: u64) -> usize {
        let cutoff = self.retention.cutoff(now);
        match crate::maintenance::prune_expired(&self.conn, cutoff, None) {
            Ok(n) => n,
            Err(err) => {
                tracing::error!(%err, "prune failed");
                0
            }
        }
    }
}

/// True at most once per `WARN_EVERY` for the warning tracked by `last`.
fn throttle(last: &Cell<Option<Instant>>) -> bool {
    if last.get().is_some_and(|at| at.elapsed() < WARN_EVERY) {
        return false;
    }
    last.set(Some(Instant::now()));
    true
}

/// Another connection holds the lock; the same write can succeed later.
pub(crate) fn is_busy(err: &rusqlite::Error) -> bool {
    matches!(
        err.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

/// Cheap token estimate when the protocol provides none: ~4 chars/token.
pub fn estimate_tokens(text: &str) -> u64 {
    (text.chars().count() as u64).div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_first_open_keeps_every_connection_on_the_shared_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(12));
        let threads: Vec<_> = (0..12)
            .map(|_| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let connection = StateStore::open_conn(&path).unwrap();
                    let mode: String = connection
                        .pragma_query_value(None, "journal_mode", |row| row.get(0))
                        .unwrap();
                    assert_eq!(mode, "wal");
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn opening_state_waits_for_a_concurrent_startup_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let writer = Connection::open(&path).unwrap();
        writer.execute_batch("BEGIN EXCLUSIVE").unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let ready = barrier.clone();
        let opening = std::thread::spawn(move || {
            ready.wait();
            StateStore::open_conn(&path)
        });
        barrier.wait();
        std::thread::sleep(Duration::from_millis(100));
        writer.execute_batch("COMMIT").unwrap();
        let conn = opening.join().unwrap().expect("startup waits for SQLite");
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }

    #[test]
    fn durable_state_open_failure_never_falls_back_to_memory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        std::fs::create_dir(&path).unwrap();

        let err = StateStore::try_load(&path, Retention::default()).unwrap_err();

        assert!(
            err.to_string().contains("unable to open database file"),
            "unexpected SQLite error: {err}"
        );
    }

    fn store() -> (tempfile::TempDir, StateStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let s = StateStore::load(&path, Retention::default());
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
    fn log_for_all_replays_every_entry_in_order_without_cross_session_rows() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("a"));
        s.upsert("r2".into(), session("b"));
        for i in 0..501 {
            s.log(
                "r1",
                &LogEntry {
                    kind: "event".into(),
                    role: "router".into(),
                    summary: i.to_string(),
                    ..Default::default()
                },
            );
        }
        s.log(
            "r2",
            &LogEntry {
                kind: "other".into(),
                role: "router".into(),
                summary: "not r1".into(),
                ..Default::default()
            },
        );

        let entries = s.log_for_all("r1").unwrap();
        assert_eq!(entries.len(), 501);
        assert_eq!(entries.first().unwrap().summary, "0");
        assert_eq!(entries.last().unwrap().summary, "500");
        assert!(entries.iter().all(|entry| entry.kind == "event"));
    }

    fn chunk(text: &str) -> LogEntry {
        LogEntry {
            kind: "session_update".into(),
            role: "agent".into(),
            detail: Some(serde_json::json!({
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": text},
            })),
            ..Default::default()
        }
    }

    /// Chunk text, or the summary for a row without a chunk.
    fn texts(entries: &[LogEntry]) -> Vec<String> {
        entries
            .iter()
            .map(|e| match &e.detail {
                Some(detail) => detail["content"]["text"].as_str().unwrap().to_string(),
                None => e.summary.clone(),
            })
            .collect()
    }

    #[test]
    fn batched_chunks_keep_order_and_content_around_direct_rows() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("a"));
        s.log_buffered("r1", chunk("one"));
        s.log_buffered("r1", chunk("two"));
        s.log_checked(
            "r1",
            &LogEntry {
                kind: "user_steer".into(),
                role: "user".into(),
                summary: "steer".into(),
                ..Default::default()
            },
        )
        .unwrap();
        s.log_buffered("r1", chunk("three"));
        assert_eq!(s.pending_log_rows(), 1, "one row still waits for its batch");

        let entries = s.log_for_all("r1").unwrap();

        assert_eq!(s.pending_log_rows(), 0);
        assert_eq!(texts(&entries), ["one", "two", "steer", "three"]);
        assert_eq!(entries[0].kind, "session_update");
        assert_eq!(entries[0].detail, chunk("one").detail);
    }

    #[test]
    fn a_full_batch_is_written_without_an_explicit_flush() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("a"));
        for i in 0..LOG_BATCH_ROWS {
            s.log_buffered("r1", chunk(&i.to_string()));
        }
        assert_eq!(s.pending_log_rows(), 0);
    }

    /// A store plus a second connection holding the write lock.
    fn locked_store() -> (tempfile::TempDir, StateStore, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let s = StateStore::load(&path, Retention::default());
        s.upsert("r1".into(), session("a"));
        let other = Connection::open(&path).unwrap();
        other.execute_batch("BEGIN IMMEDIATE").unwrap();
        (dir, s, other)
    }

    #[test]
    fn a_locked_database_keeps_chunks_queued_until_a_later_flush() {
        let (_d, s, other) = locked_store();
        for i in 0..LOG_BATCH_ROWS - 1 {
            s.log_buffered("r1", chunk(&i.to_string()));
        }

        // The row that completes a batch triggers a flush; it must not stall
        // the caller for the 30 s lock timeout.
        let started = Instant::now();
        s.log_buffered("r1", chunk("last"));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(s.pending_log_rows(), LOG_BATCH_ROWS);
        let err = s.flush_log().unwrap_err();
        assert!(is_busy(&err), "unexpected error: {err}");

        other.execute_batch("COMMIT").unwrap();
        s.flush_log().unwrap();
        let saved = texts(&s.log_for_all("r1").unwrap());
        assert_eq!(saved.len(), LOG_BATCH_ROWS);
        assert_eq!(saved.first().unwrap(), "0");
        assert_eq!(saved.last().unwrap(), "last");
    }

    #[test]
    fn a_full_queue_drops_its_oldest_rows_and_keeps_order() {
        let (_d, s, other) = locked_store();
        let extra = 5;
        for i in 0..LOG_QUEUE_CAP + extra {
            s.log_buffered("r1", chunk(&i.to_string()));
        }
        assert_eq!(s.pending_log_rows(), LOG_QUEUE_CAP);

        other.execute_batch("COMMIT").unwrap();
        s.flush_log().unwrap();
        let saved = texts(&s.log_for_all("r1").unwrap());
        assert_eq!(saved.len(), LOG_QUEUE_CAP);
        assert_eq!(saved.first().unwrap(), &extra.to_string());
        assert_eq!(
            saved.last().unwrap(),
            &(LOG_QUEUE_CAP + extra - 1).to_string()
        );
        assert!(
            saved
                .windows(2)
                .all(|w| w[0].parse::<usize>().unwrap() + 1 == w[1].parse::<usize>().unwrap())
        );
    }

    #[test]
    fn a_chunk_for_a_deleted_session_is_dropped_without_blocking_others() {
        let (_d, s) = store();
        s.upsert("r1".into(), session("a"));
        s.log_buffered("gone", chunk("orphan"));
        s.log_buffered("r1", chunk("kept"));

        s.flush_log().unwrap();

        assert_eq!(s.pending_log_rows(), 0);
        assert_eq!(texts(&s.log_for_all("r1").unwrap()), ["kept"]);
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
                session_config: Some(serde_json::json!({"mode": "native"})),
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
        assert_eq!(
            got.session_config,
            Some(serde_json::json!({"mode": "native"}))
        );
    }

    #[test]
    fn session_config_survives_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let s = StateStore::load(&path, Retention::default());
            s.upsert("r1".into(), session("a"));
            s.set_session_config(
                "r1",
                &serde_json::json!({"approval": "on-request", "attempt": 3}),
            )
            .unwrap();
        }

        let reopened = StateStore::load(&path, Retention::default());
        assert_eq!(
            reopened.get("r1").unwrap().session_config,
            Some(serde_json::json!({"approval": "on-request", "attempt": 3}))
        );
    }

    #[test]
    fn explicit_session_config_with_null_replaces_the_prior_checkpoint() {
        let (_d, s) = store();
        s.upsert(
            "r1".into(),
            PersistedSession {
                session_config: Some(serde_json::json!({"mode": "native", "resume": true})),
                ..session("a")
            },
        );
        s.upsert(
            "r1".into(),
            PersistedSession {
                session_config: Some(serde_json::json!({"mode": null, "resume": false})),
                ..session("b")
            },
        );

        assert_eq!(
            s.get("r1").unwrap().session_config,
            Some(serde_json::json!({"mode": null, "resume": false}))
        );
    }

    #[test]
    fn migrates_old_sessions_schema_with_nullable_session_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (
                router_session_id TEXT PRIMARY KEY,
                agent TEXT NOT NULL,
                model TEXT NOT NULL,
                downstream_session_id TEXT NOT NULL,
                cwd TEXT NOT NULL,
                additional_directories TEXT NOT NULL DEFAULT '[]',
                title TEXT,
                routing TEXT,
                parent_session_id TEXT,
                prior_session_id TEXT,
                kind TEXT NOT NULL DEFAULT 'primary',
                run_label TEXT,
                created_at INTEGER,
                updated_at INTEGER,
                tokens_input INTEGER NOT NULL DEFAULT 0,
                tokens_output INTEGER NOT NULL DEFAULT 0,
                tokens_total INTEGER NOT NULL DEFAULT 0,
                tokens_cache_read INTEGER NOT NULL DEFAULT 0,
                tokens_cache_write INTEGER NOT NULL DEFAULT 0,
                context_used INTEGER NOT NULL DEFAULT 0,
                cost_usd REAL NOT NULL DEFAULT 0,
                cost_estimated INTEGER NOT NULL DEFAULT 0,
                llm_request_cost_usd REAL NOT NULL DEFAULT 0,
                llm_requests_total INTEGER NOT NULL DEFAULT 0,
                native_subagent_calls INTEGER NOT NULL DEFAULT 0,
                delegation_directive_injections INTEGER NOT NULL DEFAULT 0,
                compute_ms INTEGER NOT NULL DEFAULT 0,
                git_branch TEXT,
                git_sha TEXT
            );
            INSERT INTO sessions
                (router_session_id, agent, model, downstream_session_id, cwd)
            VALUES ('legacy', 'a', 'm', 'd', '/');",
        )
        .unwrap();
        drop(conn);

        let s = StateStore::load(&path, Retention::default());
        assert_eq!(s.get("legacy").unwrap().session_config, None);
        s.set_session_config("legacy", &serde_json::json!({"resume": null}))
            .unwrap();
        assert_eq!(
            s.get("legacy").unwrap().session_config,
            Some(serde_json::json!({"resume": null}))
        );
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
            .file
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
            .file
            .conn
            .query_row("SELECT COUNT(*) FROM active_tool_calls", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(active_count, 0);
        let tool_model: String = s
            .file
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
            s.file
                .conn
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
                .file
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
        let s = StateStore::load(
            &path,
            Retention {
                max_age: Duration::from_secs(100),
            },
        );
        s.upsert("old".into(), session("a"));
        s.upsert("new".into(), session("b"));
        s.file
            .conn
            .execute(
                "UPDATE sessions SET updated_at=1000 WHERE router_session_id='old'",
                [],
            )
            .unwrap();
        s.file
            .conn
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
    fn readonly_store_never_creates_or_initializes_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.db");

        assert!(StateStore::open_readonly(&path, Retention::default()).is_err());
        assert!(!path.exists());
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
        let s = StateStore::load(&db, Retention::default());
        let got = s.get("r1").expect("legacy row imported");
        assert_eq!(got.model, "sonnet");
        assert_eq!(got.title.as_deref(), Some("t"));
        // JSON was renamed so it doesn't re-import.
        assert!(!json.exists());
        assert!(json.with_extension("json.imported").exists());
    }
}
