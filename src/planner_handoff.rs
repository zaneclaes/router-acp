//! Hot replacement of the planner's downstream conversation. The logical run,
//! child sessions, custody, event queues and watcher processes never change.
//! Policy comes from the normal router.yaml/project-overlay configuration.

use std::sync::{Arc, Mutex, Weak, atomic::AtomicBool};
use std::time::{Duration, Instant};

use agent_client_protocol::schema::v1::SetSessionModeRequest;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::planner_workflow::{self, Operation, PlannerRun, RunStatus};
use crate::session::{self, DownstreamRoute, OpenedSession, PinInfo, Shared};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub enabled: bool,
    pub prewarm_percent: u8,
    pub opportunity_percent: u8,
    pub backstop_percent: u8,
    pub prepare_timeout_seconds: u64,
    pub cutover_timeout_seconds: u64,
    pub retry_seconds: u64,
    pub checkpoint_interval_seconds: u64,
    pub opportunities: Opportunities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Opportunities {
    pub operations: Vec<OperationPoint>,
    pub release_stages: Vec<crate::planner_release::Stage>,
    /// Host-authoritative observation keys, e.g. ci:staging. A new nonempty
    /// revision after the initial baseline is an opportunity, not a CI verdict.
    pub observation_revisions: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationPoint {
    Review,
    Integrate,
    Disposition,
}

impl Default for Opportunities {
    fn default() -> Self {
        use crate::planner_release::Stage;
        Self {
            operations: vec![OperationPoint::Integrate, OperationPoint::Disposition],
            release_stages: vec![
                Stage::StagingDeployed,
                Stage::StagingPassed,
                Stage::ProductionMerged,
                Stage::ProductionPassed,
            ],
            observation_revisions: Vec::new(),
        }
    }
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            enabled: false,
            prewarm_percent: 65,
            opportunity_percent: 75,
            backstop_percent: 95,
            prepare_timeout_seconds: 60,
            cutover_timeout_seconds: 60,
            retry_seconds: 60,
            checkpoint_interval_seconds: 30,
            opportunities: Default::default(),
        }
    }
}

impl Policy {
    pub fn validate(&self) -> Result<(), String> {
        if self.prewarm_percent == 0
            || self.prewarm_percent > self.opportunity_percent
            || self.opportunity_percent >= self.backstop_percent
            || self.backstop_percent > 100
        {
            return Err("routers.planner.handoff requires 0 < prewarm_percent <= opportunity_percent < backstop_percent <= 100".into());
        }
        if [
            self.prepare_timeout_seconds,
            self.cutover_timeout_seconds,
            self.retry_seconds,
            self.checkpoint_interval_seconds,
        ]
        .contains(&0)
        {
            return Err("routers.planner.handoff intervals must be positive".into());
        }
        if self
            .opportunities
            .observation_revisions
            .iter()
            .any(|key| key.trim().is_empty())
        {
            return Err("handoff observation keys must be nonempty".into());
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct Runtime {
    pub(crate) checkpoint_lock: Arc<tokio::sync::Mutex<()>>,
    policy: Option<Policy>,
    next_reload: Option<Instant>,
    used: u64,
    size: u64,
    due: Option<String>,
    preparing: bool,
    prepared: Option<Prepared>,
    pub(crate) cutting_over: bool,
    retry_at: Option<Instant>,
    dirty: bool,
    saving: bool,
    last_save: Option<Instant>,
    observations: std::collections::BTreeMap<String, String>,
    last_error: Option<String>,
}

impl Runtime {
    fn at(&self, percent: u8) -> bool {
        self.size > 0 && u128::from(self.used) * 100 >= u128::from(self.size) * u128::from(percent)
    }

    pub(crate) fn usage(&mut self, used: u64, size: u64) {
        if size == 0 {
            return;
        }
        self.used = used;
        self.size = size;
        if self
            .policy
            .as_ref()
            .is_some_and(|p| p.enabled && self.at(p.backstop_percent))
        {
            // Sticky: provider compaction cannot erase an already due handoff.
            self.due = Some("context backstop".into());
        }
    }

    fn opportunity(&mut self, reason: String) {
        self.dirty = true;
        if self
            .policy
            .as_ref()
            .is_some_and(|p| p.enabled && self.at(p.opportunity_percent))
        {
            self.due.get_or_insert(reason);
        }
    }
}

struct Prepared {
    shared: Weak<Shared>,
    old: PinInfo,
    opened: OpenedSession,
    token: String,
    mode: Option<String>,
    committed: bool,
}

impl Drop for Prepared {
    fn drop(&mut self) {
        if !self.committed
            && let Some(shared) = self.shared.upgrade()
        {
            session::close_downstream_session(
                &shared,
                &self.opened.process_key,
                &self.opened.downstream_sid,
            );
            shared.delegate_tokens.lock().unwrap().remove(&self.token);
        }
    }
}

/// Called only after the operation's durable CAS succeeded, never on a failed
/// operation, replayed receipt, speculative merge or model claim.
pub(crate) fn operated(shared: &Shared, sid: &str, operation: &Operation) {
    shared.with_session(sid, |s| {
        let h = &mut s.parent_handoff;
        h.dirty = true;
        let Some(p) = &h.policy else {
            return;
        };
        let point = match operation {
            Operation::Review { .. } => Some(OperationPoint::Review),
            Operation::Integrate { .. } => Some(OperationPoint::Integrate),
            Operation::Disposition { .. } => Some(OperationPoint::Disposition),
            _ => None,
        };
        let eligible = point.is_some_and(|point| p.opportunities.operations.contains(&point))
            || matches!(operation, Operation::Release { checkpoint, .. }
                if p.opportunities.release_stages.contains(&checkpoint.stage));
        if eligible {
            h.opportunity("committed planner transition".into());
        }
    });
}

pub(crate) fn status(shared: &Shared, sid: &str) -> serde_json::Value {
    shared
        .with_session(sid, |s| {
            let h = &s.parent_handoff;
            json!({"enabled":h.policy.as_ref().is_some_and(|p| p.enabled),
            "used":h.used,"capacity":h.size,"usage_known":h.size > 0,
            "due":h.due,"preparing":h.preparing,"prepared":h.prepared.is_some(),
            "cutting_over":h.cutting_over,"last_error":h.last_error})
        })
        .unwrap_or(serde_json::Value::Null)
}

fn error(shared: &Arc<Shared>, sid: &str, message: &str) {
    let changed = shared
        .with_session(sid, |s| {
            let h = &mut s.parent_handoff;
            let changed = h.last_error.as_deref() != Some(message);
            h.last_error = Some(message.into());
            changed
        })
        .unwrap_or(false);
    if changed {
        session::notify_user(
            shared,
            sid,
            format!(
                "router-acp · parent handoff deferred: {message}; current parent and workers retained"
            ),
        );
    }
}

/// Called by the existing wake loop after its worker/watch/heartbeat work.
/// Returns true only while the parent route is reserved for a bounded cutover.
pub(crate) fn tick(shared: &Arc<Shared>, sid: &str, run: &PlannerRun) -> bool {
    if !run.parent_role() || run.status != RunStatus::Running {
        return false;
    }
    let now = Instant::now();
    let reload = shared
        .with_session(sid, |s| {
            let h = &mut s.parent_handoff;
            if h.next_reload.is_some_and(|at| at > now) {
                return false;
            }
            h.next_reload = Some(now + Duration::from_secs(5));
            true
        })
        .unwrap_or(false);
    if reload {
        let cfg = shared.cfg.source_path.as_ref().map_or_else(
            || Ok(shared.runtime_config()),
            |path| crate::config::Config::from_file(path),
        );
        match cfg {
            Ok(cfg) => {
                shared.with_session(sid, |s| {
                    s.parent_handoff.policy = Some(cfg.routers.planner.handoff);
                    let h = &mut s.parent_handoff;
                    h.usage(h.used, h.size);
                });
            }
            Err(_) => {
                shared.with_session(sid, |s| s.parent_handoff.policy = None);
                error(
                    shared,
                    sid,
                    "router.yaml is invalid; fix configuration before retrying",
                );
                return false;
            }
        }
    }
    let Some(policy) = shared
        .with_session(sid, |s| s.parent_handoff.policy.clone())
        .flatten()
    else {
        return false;
    };
    if !policy.enabled {
        let prepared = shared.with_session(sid, |s| {
            s.parent_handoff.due = None;
            s.parent_handoff.prepared.take()
        });
        drop(prepared);
        return false;
    }
    // No startup or historical revision is treated as a newly occurring point.
    shared.with_session(sid, |s| {
        for key in &policy.opportunities.observation_revisions {
            if let Some(o) = run.observations.get(key).filter(|o| !o.revision.is_empty()) {
                let previous = s
                    .parent_handoff
                    .observations
                    .insert(key.clone(), o.revision.clone());
                if previous.is_some_and(|revision| revision != o.revision) {
                    s.parent_handoff
                        .opportunity(format!("{key} revision advanced"));
                }
            }
        }
    });
    // Dispose of a standby if another routing decision changed the active pin.
    let stale = shared.with_session(sid, |s| {
        let stale = s.parent_handoff.prepared.as_ref().is_some_and(|p| {
            !s.pin.as_ref().is_some_and(|pin| same_pin(pin, &p.old)) || s.applied_mode != p.mode
        });
        if stale {
            s.parent_handoff.prepared.take()
        } else {
            None
        }
    });
    drop(stale);
    let prepare = shared
        .with_session(sid, |s| {
            let h = &mut s.parent_handoff;
            if h.preparing
                || h.prepared.is_some()
                || h.cutting_over
                || s.pin.is_none()
                || h.retry_at.is_some_and(|at| at > now)
                || (!h.at(policy.prewarm_percent) && h.due.is_none())
            {
                return false;
            }
            h.preparing = true;
            true
        })
        .unwrap_or(false);
    if prepare {
        let shared = shared.clone();
        let sid = sid.to_string();
        let policy = policy.clone();
        tokio::spawn(async move {
            let result = tokio::time::timeout(
                Duration::from_secs(policy.prepare_timeout_seconds),
                prepare_parent(&shared, &sid),
            )
            .await;
            match result {
                Ok(Ok(prepared)) => {
                    shared.with_session(&sid, |s| {
                        s.parent_handoff.preparing = false;
                        s.parent_handoff.prepared = Some(prepared);
                    });
                }
                _ => {
                    shared.with_session(&sid, |s| {
                        s.parent_handoff.preparing = false;
                        s.parent_handoff.retry_at =
                            Some(Instant::now() + Duration::from_secs(policy.retry_seconds));
                    });
                    error(
                        &shared,
                        &sid,
                        "standby preparation failed or exceeded its budget",
                    );
                }
            }
        });
    }
    let start = run.approval_waits.is_empty()
        && !crate::planner_watchdog::tools_in_flight(shared, sid)
        && shared
            .with_session(sid, |s| {
                let h = &mut s.parent_handoff;
                if h.cutting_over
                    || h.saving
                    || h.due.is_none()
                    || h.prepared.is_none()
                    || h.retry_at.is_some_and(|at| at > now)
                    || s.prompt_activity != 0
                    || s.pinning
                    || s.pending_switch.is_some()
                    || s.capturing_summary.is_some()
                {
                    return false;
                }
                h.cutting_over = true;
                true
            })
            .unwrap_or(false);
    if start {
        let shared = shared.clone();
        let sid = sid.to_string();
        let policy = policy.clone();
        tokio::spawn(async move {
            let outcome = tokio::time::timeout(
                Duration::from_secs(policy.cutover_timeout_seconds),
                cutover(&shared, &sid),
            )
            .await;
            let succeeded = matches!(outcome, Ok(Ok(())));
            shared.with_session(&sid, |s| {
                s.parent_handoff.cutting_over = false;
                if !succeeded {
                    s.parent_handoff.retry_at =
                        Some(Instant::now() + Duration::from_secs(policy.retry_seconds));
                }
            });
            if !succeeded {
                error(
                    &shared,
                    &sid,
                    "checkpoint or cutover failed or exceeded its budget",
                );
            }
        });
    } else {
        save_continuously(shared, sid, run, &policy);
    }
    shared
        .with_session(sid, |s| s.parent_handoff.cutting_over)
        .unwrap_or(false)
}

fn same_pin(a: &PinInfo, b: &PinInfo) -> bool {
    a.process_key == b.process_key
        && a.downstream_sid == b.downstream_sid
        && a.candidate == b.candidate
}

struct TokenCleanup {
    shared: Arc<Shared>,
    token: String,
    retained: bool,
}
impl Drop for TokenCleanup {
    fn drop(&mut self) {
        if !self.retained {
            self.shared
                .delegate_tokens
                .lock()
                .unwrap()
                .remove(&self.token);
        }
    }
}

async fn prepare_parent(shared: &Arc<Shared>, sid: &str) -> Result<Prepared, String> {
    let (old, cwd, dirs, mcp, mode) = shared
        .with_session(sid, |s| {
            (
                s.pin.clone(),
                s.cwd.clone(),
                s.additional_directories.clone(),
                s.mcp_servers.clone(),
                s.applied_mode.clone(),
            )
        })
        .ok_or("parent missing")?;
    let old = old.ok_or("parent is unpinned")?;
    // Use the already-running process. Never repair/restart a shared adapter.
    if shared.target_conn(&old.process_key).is_none() {
        return Err("adapter unavailable".into());
    }
    let mut servers = session::mcp_servers_without_parent(shared, sid, &old.candidate, &mcp)
        .map_err(|e| e.to_string())?;
    let (server, token) = crate::delegate_mcp::prepared_parent_server(shared, sid, &old.candidate)
        .ok_or("parent tools unavailable")?;
    servers.push(server);
    let mut cleanup = TokenCleanup {
        shared: shared.clone(),
        token: token.clone(),
        retained: false,
    };
    let opened = session::open_downstream_session_at(
        shared,
        &old.candidate,
        old.process_key.clone(),
        cwd,
        dirs,
        servers,
        DownstreamRoute::PreClass {
            capture: Arc::new(Mutex::new(String::new())),
            violation: Arc::new(AtomicBool::new(false)),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let prepared = Prepared {
        shared: Arc::downgrade(shared),
        old,
        opened,
        token,
        mode,
        committed: false,
    };
    cleanup.retained = true;
    if let Some(mode) = &prepared.mode {
        let modes = prepared
            .opened
            .modes
            .as_ref()
            .map(|m| {
                m.available_modes
                    .iter()
                    .map(|m| m.id.0.to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let id = session::resolve_mode_id(shared, &prepared.old.candidate.agent, mode, &modes)
            .ok_or("standby does not support the active permission mode")?;
        prepared
            .opened
            .conn
            .send_request(SetSessionModeRequest::new(
                prepared.opened.downstream_sid.clone(),
                id,
            ))
            .block_task()
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(prepared)
}

async fn checkpoint(
    shared: &Shared,
    sid: &str,
    run: &PlannerRun,
    event: &str,
) -> Result<Vec<String>, String> {
    let cfg = shared
        .cfg
        .routers
        .planner
        .shutdown
        .clone()
        .unwrap_or_default();
    let (paths, _, saved) =
        crate::planner_shutdown::save_checkpoint(shared, sid, run, run.status, &cfg, event).await?;
    if !saved {
        return Err("checkpoint host must atomically save the configured continuation".into());
    }
    Ok(paths)
}

/// Repository-neutral exporter for installations without a custom shutdown
/// host. Stable, atomic files bound disk usage; history stays in the state DB.
pub(crate) fn native_checkpoint(
    cwd: &std::path::Path,
    sid: &str,
    continuation: &str,
    run: &PlannerRun,
    event: &str,
) -> Result<(Vec<String>, String, bool), String> {
    use std::path::{Component, Path};
    let relative = Path::new(continuation);
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err("continuation must be a repository-relative file".into());
    }
    let directory = cwd
        .join(".router-acp/checkpoints")
        .join(crate::planner_skills::content_hash(sid));
    std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    let file = cwd.join(relative);
    std::fs::create_dir_all(file.parent().ok_or("continuation has no directory")?)
        .map_err(|e| e.to_string())?;
    let raw = directory.join("current.json");
    let notes = directory.join("coordinator-notes.md");
    let previous = match std::fs::read_to_string(&file) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e.to_string()),
    };
    const MARKER: &str = "<!-- router-owned-continuation -->";
    if !previous.is_empty() && !previous.starts_with(MARKER) {
        atomic_write(&notes, previous.as_bytes())?;
    }
    let snapshot = json!({"event":event,"session_id":sid,"run_revision":run.revision,"saved_at":chrono::Utc::now().to_rfc3339(),"native_state":run});
    atomic_write(
        &raw,
        &serde_json::to_vec(&snapshot).map_err(|e| e.to_string())?,
    )?;
    let lifecycle = if event == "shutdown" {
        "Shutdown checkpoint. Resume the durable run using the native implement command."
    } else {
        "Live checkpoint: children, watchers and the logical parent remain running. A hot handoff must not invoke shutdown, adoption or worker startup."
    };
    let text = format!(
        "{MARKER}\n# CONTINUE\n\nParent `{sid}`, recorded revision `{}`.\n{lifecycle}\n\nLatest snapshot: `{}`.\nPreserved coordinator notes, if present: `{}`.\nUse planner_workflow status and targeted work/release reads before mutation; newer native state wins.\nSource cleanliness and CI verdicts were not rechecked.\nAfter an actual process interruption, use the native implement command to resume.\n",
        run.revision,
        raw.display(),
        notes.display()
    );
    atomic_write(&file, text.as_bytes())?;
    Ok((
        vec![continuation.into(), raw.to_string_lossy().into()],
        "deterministic checkpoint saved".into(),
        true,
    ))
}

fn atomic_write(path: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    let temporary = path.with_extension(format!("checkpoint-{}", uuid::Uuid::new_v4()));
    let result = (|| {
        std::fs::write(&temporary, bytes)?;
        std::fs::rename(&temporary, path)
    })();
    let _ = std::fs::remove_file(&temporary);
    result.map_err(|e| e.to_string())
}

fn save_continuously(shared: &Arc<Shared>, sid: &str, run: &PlannerRun, policy: &Policy) {
    let start = shared
        .with_session(sid, |s| {
            let h = &mut s.parent_handoff;
            if h.cutting_over
                || h.saving
                || !h.dirty
                || h.last_save.is_some_and(|at| {
                    at.elapsed() < Duration::from_secs(policy.checkpoint_interval_seconds)
                })
            {
                return false;
            }
            h.saving = true;
            h.dirty = false;
            h.last_save = Some(Instant::now());
            true
        })
        .unwrap_or(false);
    if !start {
        return;
    }
    let shared = shared.clone();
    let sid = sid.to_string();
    let run = run.clone();
    let timeout = Duration::from_secs(policy.cutover_timeout_seconds);
    tokio::spawn(async move {
        let success = matches!(
            tokio::time::timeout(timeout, checkpoint(&shared, &sid, &run, "checkpoint")).await,
            Ok(Ok(_))
        );
        shared.with_session(&sid, |s| {
            s.parent_handoff.saving = false;
            s.parent_handoff.dirty |= !success;
        });
        if !success {
            error(&shared, &sid, "continuous checkpoint failed");
        }
    });
}

async fn cutover(shared: &Arc<Shared>, sid: &str) -> Result<(), String> {
    let run = planner_workflow::load(shared, sid)?.ok_or("run missing")?;
    if run.status != RunStatus::Running || !run.approval_waits.is_empty() {
        return Err("run is not available for handoff".into());
    }
    let paths = checkpoint(shared, sid, &run, "handoff").await?;
    // Reload after export: children and watchers may have advanced during it.
    // Never save the old snapshot back over live state or require workers to stop.
    let latest = planner_workflow::load(shared, sid)?.ok_or("run missing")?;
    if latest.status != RunStatus::Running || !latest.approval_waits.is_empty() {
        return Err("run changed during checkpoint".into());
    }
    let mut prepared = shared
        .with_session(sid, |s| s.parent_handoff.prepared.take())
        .flatten()
        .ok_or("standby missing")?;
    let still_current = shared
        .with_session(sid, |s| {
            s.pin
                .as_ref()
                .is_some_and(|pin| same_pin(pin, &prepared.old))
                && s.applied_mode == prepared.mode
                && s.pending_switch.is_none()
        })
        .unwrap_or(false);
    if !still_current {
        return Err("parent route changed".into());
    }
    let seed = format!(
        "Router-owned hot handoff. Same logical parent {sid}; existing children, custody, queues and watchers are still running. Do not run /implement, adopt, drain or relaunch them. Read {} and use planner_workflow status for current state before mutation. Checkpoint revision {} is a snapshot, not authority over newer events. Saved paths: {}. Continue the existing authorized run using its current role policy; do not replay transcript history.",
        shared.cfg.routers.planner.continuation,
        run.revision,
        paths.join(", ")
    );
    // Schedule a continuation before changing the route. A failed persistence
    // leaves the old parent available to reconcile this idempotent wake.
    planner_workflow::mutate(shared, sid, |run| {
        run.wakes.insert(format!("handoff:{}", prepared.opened.downstream_sid), planner_workflow::Wake { reason: "Reconcile the current run and deterministic continuation after a pending context handoff".into(), ..Default::default() });
        Ok(())
    })?;
    // Everything after this point is synchronous. Input queues while the fence
    // is held; callbacks from the old route are unregistered before activation.
    let mut persisted = shared
        .state
        .lock()
        .unwrap()
        .get(sid)
        .ok_or("parent persistence missing")?;
    persisted.prior_session_id = Some(prepared.old.downstream_sid.clone());
    persisted.downstream_session_id = prepared.opened.downstream_sid.clone();
    let mut routing = persisted.routing.take().unwrap_or_else(|| json!({}));
    routing["parent_handoff"] = json!({"checkpoint_revision":run.revision,
        "from":prepared.old.downstream_sid,"to":prepared.opened.downstream_sid});
    persisted.routing = Some(routing);
    if !shared
        .delegate_tokens
        .lock()
        .unwrap()
        .contains_key(&prepared.token)
    {
        return Err("standby credentials expired".into());
    }
    shared
        .state
        .lock()
        .unwrap()
        .upsert_checked(sid.into(), persisted)
        .map_err(|e| e.to_string())?;
    if !crate::delegate_mcp::activate_parent_token(shared, sid, &prepared.token) {
        return Err("standby credentials expired".into());
    }
    shared.unregister_route(&prepared.old.process_key, &prepared.old.downstream_sid);
    shared.register_route(
        &prepared.opened.process_key,
        &prepared.opened.downstream_sid,
        DownstreamRoute::Primary {
            router_sid: sid.into(),
        },
    );
    shared.with_session(sid, |s| {
        s.pin = Some(PinInfo {
            candidate: prepared.old.candidate.clone(),
            process_key: prepared.opened.process_key.clone(),
            downstream_sid: prepared.opened.downstream_sid.clone(),
            available_modes: prepared
                .opened
                .modes
                .as_ref()
                .map(|m| {
                    m.available_modes
                        .iter()
                        .map(|m| m.id.0.to_string())
                        .collect()
                })
                .unwrap_or_default(),
        });
        s.delegate_token = Some(prepared.token.clone());
        s.pending_context = Some(seed);
        s.pending_history.clear();
        s.native_effort_options = prepared.opened.config_options.clone();
        session::refresh_pinned_effort(&shared.runtime_config(), &shared.scores, s);
        s.parent_handoff.used = 0;
        s.parent_handoff.size = 0;
        s.parent_handoff.due = None;
        s.parent_handoff.retry_at = None;
        s.parent_handoff.last_error = None;
    });
    prepared.committed = true;
    session::close_downstream_session(
        shared,
        &prepared.old.process_key,
        &prepared.old.downstream_sid,
    );
    let (router_pid, router_started_at_ms) = crate::delegate_hook::process_identity();
    crate::delegate_hook::deliver(
        shared,
        &crate::delegate_hook::RepinEvent {
            event: "parent_repinned",
            parent_router_session_id: sid.into(),
            previous_downstream_session_id: prepared.old.downstream_sid.clone(),
            previous_candidate: prepared.old.candidate.to_string(),
            downstream_session_id: prepared.opened.downstream_sid.clone(),
            candidate: prepared.old.candidate.to_string(),
            lineage: session::agent_lineage(
                &shared.runtime_config(),
                &prepared.old.candidate.agent,
            ),
            reason: "deterministic context handoff".into(),
            router_pid,
            router_started_at_ms,
        },
    );
    session::notify_user(
        shared,
        sid,
        "router-acp · parent context refreshed; existing children and watchers retained",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn thresholds_use_window_occupancy_and_backstop_survives_compaction() {
        let mut h = Runtime {
            policy: Some(Policy {
                enabled: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        h.usage(749, 1000);
        h.opportunity("merge".into());
        assert!(h.due.is_none());
        h.usage(750, 1000);
        assert!(h.due.is_none());
        h.opportunity("merge".into());
        assert_eq!(h.due.as_deref(), Some("merge"));
        h.due = None;
        h.usage(949, 1000);
        assert!(h.due.is_none());
        h.usage(950, 1000);
        assert_eq!(h.due.as_deref(), Some("context backstop"));
        h.usage(30, 1000);
        assert!(h.due.is_some());
        h.usage(u64::MAX, u64::MAX);
        assert!(h.at(95));
    }
    #[test]
    fn policy_rejects_bad_thresholds_and_unknown_points() {
        for p in [
            Policy {
                opportunity_percent: 96,
                ..Default::default()
            },
            Policy {
                prewarm_percent: 0,
                ..Default::default()
            },
            Policy {
                cutover_timeout_seconds: 0,
                ..Default::default()
            },
        ] {
            assert!(p.validate().is_err());
        }
        assert!(
            serde_yaml::from_str::<Policy>("opportunities:\n  operations: [invented]").is_err()
        );
        assert!(serde_yaml::from_str::<Policy>("opportunity_precent: 75").is_err());
        assert!(Policy::default().validate().is_ok());
    }
}
