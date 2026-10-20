//! Router-enforced worker capacity for planner runs.
//!
//! A parent that leaves worker slots empty while admissible work exists has
//! failed, and prompt text alone has never stopped it. So the router enforces
//! it mechanically:
//!
//! 1. A repository-owned capacity host (`routers.planner.capacity`) lists
//!    the admissible candidates, in priority order, as `WorkSpec`s with a
//!    worker brief. A failed, slow or malformed answer is "unknown", never
//!    "no work".
//! 2. A candidate may stay unadmitted only behind a 30-minute `hold` with one
//!    of exactly three reasons ([`HOLD_REASONS`]). A fourth, `production-failure`,
//!    holds no candidate: it declares a known production failure under repair,
//!    which the audit host verifies against preserved evidence.
//! 3. A parent below `delegation.max_concurrent` with an unheld
//!    candidate gets a capacity wake every `wake_after_secs`, with no give-up.
//! 4. After `auto_admit_after_wakes` such wakes, or `auto_admit_after_secs`
//!    of idleness with a free slot, the router admits and starts the
//!    candidates itself through the normal admit / allocate / delegate path.
//! 5. `complete` is refused while an unheld candidate exists or the host is
//!    unavailable.
//! 6. Workers resume with backoff while recovery can make progress. Missing
//!    prerequisites and repeated unchanged failures require reconciliation.
//! 7. `planner_workflow status` and every wake carry the same census.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::config::{PlannerCapacityConfig, PlannerPhase};
use crate::planner_workflow::{self, Operation, PlannerRun, RunStatus, Wake, WorkSpec, WorkStatus};
use crate::session::Shared;

/// The only reasons a capacity-host candidate may stay out of a free slot.
pub const HOLD_REASONS: [&str; 4] = [
    "source-conflict",
    "dependency",
    "disk-floor",
    "production-failure",
];
/// How long a hold keeps its candidate unadmitted.
pub const HOLD_MS: i64 = 30 * 60 * 1000;
/// Delay before each successive resumption of a worker that stopped early.
pub const RESUME_BACKOFF_SECS: [i64; 3] = [30, 120, 300];
pub const SAME_FAILURE_LIMIT: u32 = 3;
/// The prompt a worker that stopped early is resumed with.
pub const CONTINUE_PROMPT: &str = "Your work is not finished: continue until its required checks \
     pass and you record the finished receipt; poll your own branch build in the turn. Never \
     wait for a staging or production deploy: releases are the parent's, so a worker whose \
     branch is pushed and green reports its artifact and hands back.";
/// Handoff kinds that ask the parent for something only it can give. Any
/// other ended attempt without a receipt (including `waiting`) is resumed.
const PARENT_HANDOFFS: [&str; 7] = [
    "commit_paths_ready",
    "lease_requested",
    "ownership_requested",
    "staging_gate_request",
    "round_verified",
    "round_failed",
    "reassignment_required",
];
/// Wake id prefixes the router owns. They are informational, are recreated
/// while their condition holds, and are acknowledged once delivered.
pub const ROUTER_WAKE_PREFIXES: [&str; 6] = [
    "capacity:",
    "auto-admit:",
    "inflight:",
    "audit:",
    "gate:",
    "adopt-native:",
];

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapacityState {
    /// An explicit implement overlapped the previous owner's drain. Retry its
    /// handoff before admitting anything against an incomplete work inventory.
    #[serde(default)]
    pub pending_adoption: Option<String>,
    #[serde(default)]
    pub last_adoption_retry_at: i64,
    /// Active holds by `plan_id` (a hold may also name a `work_id`).
    #[serde(default)]
    pub holds: BTreeMap<String, Hold>,
    /// The capacity host's last answer.
    #[serde(default)]
    pub host: Option<HostSnapshot>,
    /// Capacity wakes issued for the current unfilled condition.
    #[serde(default)]
    pub unfilled_wakes: u32,
    /// Start of the current unfilled condition; parent tool activity does not reset it.
    #[serde(default)]
    pub unfilled_since: Option<i64>,
    #[serde(default)]
    pub last_wake_at: i64,
    #[serde(default)]
    pub last_inflight_wake_at: i64,
    #[serde(default)]
    pub auto_admit_runs: u32,
    #[serde(default)]
    pub last_auto_admission: Option<AutoAdmission>,
    /// Backoff state of workers being resumed, by work id.
    #[serde(default)]
    pub resumes: BTreeMap<String, Resume>,
    /// The audit host's last answer (`routers.planner.audit`).
    #[serde(default)]
    pub audit: Option<crate::planner_guard::AuditSnapshot>,
    #[serde(default)]
    pub last_audit_wake_at: i64,
    /// The parent turn-end gate's backoff.
    #[serde(default)]
    pub gate: crate::planner_guard::GateState,
    /// The workspace host's `adopt-native` answer, asked once per run.
    #[serde(default)]
    pub native_adoption: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hold {
    pub plan_id: String,
    pub reason: String,
    pub detail: String,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostSnapshot {
    pub at_ms: i64,
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub candidates: Vec<Candidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candidate {
    pub work: WorkSpec,
    pub task: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoAdmission {
    pub at_ms: i64,
    pub run: u32,
    pub free: usize,
    pub admitted: Vec<String>,
    pub held: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resume {
    /// Resumptions already started; selects the next backoff step.
    pub count: u32,
    /// When the pending resumption is due; `None` until the worker is seen
    /// stopped again.
    pub due_at: Option<i64>,
    /// The open sub-session the last resumption followed up on, and its turn
    /// count then; a follow-up that completed no turn is not repeated.
    #[serde(default)]
    pub followup: Option<(String, u32)>,
    #[serde(default)]
    pub failure_fingerprint: Option<String>,
    #[serde(default)]
    pub same_failure_count: u32,
    /// Suspension does not mark work complete or pause unrelated work. A
    /// changed prerequisite or explicit parent retry can recover it.
    #[serde(default)]
    pub suspended: Option<String>,
}

#[derive(Deserialize)]
struct HostReply {
    candidates: Vec<Candidate>,
}

/// The configured capacity timing, or its defaults when no host is set.
pub fn timing(shared: &Shared) -> PlannerCapacityConfig {
    shared
        .cfg
        .routers
        .planner
        .capacity
        .clone()
        .unwrap_or_else(PlannerCapacityConfig::timing_defaults)
}

fn host_configured(shared: &Shared) -> bool {
    shared.cfg.routers.planner.capacity.is_some()
}

pub(crate) fn slots(shared: &Shared, run: &PlannerRun) -> (usize, usize, usize) {
    let max = shared.delegate_limit();
    let live = crate::planner_wake::live_workers(run);
    (live, max, max.saturating_sub(live))
}

fn terminal(status: WorkStatus) -> bool {
    matches!(
        status,
        WorkStatus::Integrated | WorkStatus::Blocked | WorkStatus::Watching | WorkStatus::Retired
    )
}

/// Whether a candidate's work still needs a slot: not admitted yet, admitted
/// but never started, or refused a workspace and never given one.
fn open(run: &PlannerRun, work_id: &str) -> bool {
    match run.works.get(work_id) {
        None => true,
        Some(w) => {
            !w.paused
                && w.attempt.as_ref().is_none_or(|a| a.ended)
                && (w.status == WorkStatus::Admitted
                    || (w.status == WorkStatus::Interrupted && w.workspace.is_none()))
        }
    }
}

fn hold_for<'a>(run: &'a PlannerRun, candidate: &Candidate, now_ms: i64) -> Option<&'a Hold> {
    [&candidate.work.plan_id, &candidate.work.work_id]
        .into_iter()
        .filter_map(|id| run.capacity.holds.get(id.as_str()))
        .find(|h| h.expires_at_ms > now_ms)
}

/// The host's candidates that still need a slot and no unexpired hold covers,
/// in the host's order.
pub fn unheld_candidates(run: &PlannerRun, now_ms: i64) -> Vec<Candidate> {
    let Some(host) = run.capacity.host.as_ref().filter(|h| h.ok) else {
        return Vec::new();
    };
    host.candidates
        .iter()
        .filter(|c| open(run, &c.work.work_id) && hold_for(run, c, now_ms).is_none())
        .cloned()
        .collect()
}

/// Validate and record a parent's `hold`. Only three reasons exist, and each
/// must carry its evidence in `detail`.
pub fn record_hold(
    run: &mut PlannerRun,
    plan_id: &str,
    reason: &str,
    detail: &str,
    now_ms: i64,
) -> Result<(), String> {
    if plan_id.trim().is_empty() {
        return Err("hold requires the plan_id (or work_id) of the candidate it keeps out".into());
    }
    let detail = detail.trim();
    match reason {
        "source-conflict" => {
            let live = run
                .works
                .iter()
                .filter(|(_, w)| !terminal(w.status))
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>();
            if !live.iter().any(|id| detail.contains(id)) {
                return Err(format!(
                    "a source-conflict hold must name, in detail, the live work_id it conflicts \
                     with; live works: [{}]",
                    live.join(", ")
                ));
            }
        }
        "dependency" => {
            if detail.is_empty() {
                return Err(
                    "a dependency hold must name, in detail, the unmet work or plan it waits for"
                        .into(),
                );
            }
        }
        "disk-floor" => {
            if !detail.chars().any(|c| c.is_ascii_digit()) {
                return Err(
                    "a disk-floor hold must carry, in detail, the measured free bytes".into(),
                );
            }
        }
        // A known production failure under repair: the audit host's hold_check verifies the
        // production SHA and the preserved FULL FAIL / ROUND FAILED evidence it names.
        "production-failure" => {
            if !detail
                .split(|c: char| !c.is_ascii_hexdigit())
                .any(|w| w.len() >= 7)
            {
                return Err(
                    "a production-failure hold must name, in detail, the production SHA and the \
                     preserved evidence file of the observed failure"
                        .into(),
                );
            }
        }
        other => {
            return Err(format!(
                "hold reason `{other}` is not allowed. A candidate may stay unadmitted only for \
                 exactly one of three reasons: `source-conflict` (detail names the live work_id \
                 it conflicts with), `dependency` (detail names the unmet work or plan), or \
                 `disk-floor` (detail carries the measured free bytes). Anything else is no \
                 reason to leave a worker slot empty: admit the candidate."
            ));
        }
    }
    run.capacity.holds.retain(|_, h| h.expires_at_ms > now_ms);
    run.capacity.holds.insert(
        plan_id.to_string(),
        Hold {
            plan_id: plan_id.to_string(),
            reason: reason.to_string(),
            detail: detail.to_string(),
            expires_at_ms: now_ms + HOLD_MS,
        },
    );
    Ok(())
}

/// Ask the capacity host for candidates. `None` when no host is configured.
/// Any failure is logged loudly and recorded as an unknown answer.
pub async fn query_host(
    shared: &Shared,
    sid: &str,
    run: &PlannerRun,
    now_ms: i64,
) -> Option<HostSnapshot> {
    let cfg = shared.cfg.routers.planner.capacity.clone()?;
    let (_, max, free) = slots(shared, run);
    let works = crate::planner_guard::works_json(run);
    let holds = crate::planner_guard::active_holds(run, now_ms);
    let answer = match shared.with_session(sid, |s| s.cwd.clone()) {
        Some(cwd) => {
            let input = json!({"event":"capacity","cwd":cwd,"free":free,"max":max,
                "works":works,"holds":holds});
            run_host(&cfg, &cwd, &input).await
        }
        None => Err("the parent session is not loaded".into()),
    };
    Some(match answer {
        Ok(candidates) => HostSnapshot {
            at_ms: now_ms,
            ok: true,
            error: None,
            candidates,
        },
        Err(error) => {
            tracing::error!(
                session = sid,
                %error,
                "planner capacity host unavailable; its answer is unknown, not \"no work\""
            );
            HostSnapshot {
                at_ms: now_ms,
                ok: false,
                error: Some(error),
                candidates: Vec::new(),
            }
        }
    })
}

async fn run_host(
    cfg: &PlannerCapacityConfig,
    cwd: &Path,
    input: &Value,
) -> Result<Vec<Candidate>, String> {
    let output = crate::planner_guard::run_host(
        "capacity host",
        &cfg.command,
        &cfg.args,
        cfg.timeout_ms,
        cwd,
        input,
    )
    .await?;
    if output.code != Some(0) {
        return Err(format!("capacity host exited with {}", output.describe()));
    }
    serde_json::from_slice::<HostReply>(&output.stdout)
        .map(|reply| reply.candidates)
        .map_err(|e| format!("capacity host returned invalid JSON: {e}"))
}

/// `complete` is refused while the host is unavailable or lists a candidate
/// no hold covers.
pub async fn completion_allowed(
    shared: &Shared,
    sid: &str,
    run: &PlannerRun,
) -> Result<(), String> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let Some(snapshot) = query_host(shared, sid, run, now_ms).await else {
        return Ok(());
    };
    if !snapshot.ok {
        return Err(format!(
            "completion refused: the capacity host is unavailable ({}). An unknown answer is \
             not \"no work\"; fix the host and retry.",
            snapshot.error.unwrap_or_default()
        ));
    }
    let mut probe = run.clone();
    probe.capacity.host = Some(snapshot);
    let unheld = unheld_candidates(&probe, now_ms);
    if !unheld.is_empty() {
        return Err(format!(
            "completion refused: the capacity host lists admissible work no hold covers: [{}]. \
             Admit each, or record a `hold` with source-conflict, dependency or disk-floor.",
            list(&unheld)
        ));
    }
    Ok(())
}

fn list(candidates: &[Candidate]) -> String {
    candidates
        .iter()
        .map(|c| match &c.work.external_id {
            Some(external) => format!("{} ({external})", c.work.work_id),
            None => c.work.work_id.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether capacity wakes, rather than the 20-minute heartbeat, own this
/// idle parent: the host is configured and either is unavailable or lists an
/// unheld candidate. The heartbeat stays for "no candidates", so blockers are
/// still re-checked.
pub fn supersedes_heartbeat(shared: &Shared, run: &PlannerRun, now_ms: i64) -> bool {
    host_configured(shared)
        && run
            .capacity
            .host
            .as_ref()
            .is_some_and(|h| !h.ok || !unheld_candidates(run, now_ms).is_empty())
}

/// The `capacity` block of `planner_workflow status`.
pub fn status_block(shared: &Shared, run: &PlannerRun, now_ms: i64) -> Value {
    let (live, max, free) = slots(shared, run);
    let host = run.capacity.host.as_ref().map(|h| {
        json!({"at_ms":h.at_ms,"ok":h.ok,"error":h.error,
            "candidates":h.candidates.iter().map(|c| json!({
                "work_id":c.work.work_id,"plan_id":c.work.plan_id,"external_id":c.work.external_id,
                "open":open(run, &c.work.work_id),"held":hold_for(run, c, now_ms).is_some()
            })).collect::<Vec<_>>()})
    });
    let mut block = json!({"live":live,"max":max,"free":free,"host_configured":host_configured(shared),
        "configured_at_start":shared.cfg.delegation.max_concurrent,"effective_limit":max,
        "host":host,
        "unheld":unheld_candidates(run, now_ms).iter().map(|c| &c.work.work_id).collect::<Vec<_>>(),
        "holds":run.capacity.holds.values().filter(|h| h.expires_at_ms > now_ms).collect::<Vec<_>>(),
        "unfilled_wakes":run.capacity.unfilled_wakes,
        "last_auto_admission":run.capacity.last_auto_admission,
        "resumes":run.capacity.resumes,"pending_adoption":run.capacity.pending_adoption});
    for (key, value) in crate::planner_guard::status_fields(shared, run) {
        block[key] = value;
    }
    block
}

/// The host part of the census line every wake carries.
pub fn census(shared: &Shared, run: &PlannerRun, now_ms: i64) -> String {
    if !host_configured(shared) {
        return String::new();
    }
    let mut line = match &run.capacity.host {
        None => " Capacity host: not queried yet.".to_string(),
        Some(h) if !h.ok => format!(
            " Capacity host UNAVAILABLE ({}): an unknown answer is not \"no work\", and \
             completion is refused until it answers.",
            h.error.as_deref().unwrap_or("no answer")
        ),
        Some(_) => {
            let unheld = unheld_candidates(run, now_ms);
            if unheld.is_empty() {
                " Capacity host: no unheld candidates.".to_string()
            } else {
                format!(
                    " Capacity host candidates no hold covers, in order: {}.",
                    list(&unheld)
                )
            }
        }
    };
    let holds = run
        .capacity
        .holds
        .values()
        .filter(|h| h.expires_at_ms > now_ms)
        .map(|h| format!("{} ({})", h.plan_id, h.reason))
        .collect::<Vec<_>>();
    if !holds.is_empty() {
        line.push_str(&format!(" Holds: {}.", holds.join(", ")));
    }
    if let Some(auto) = &run.capacity.last_auto_admission {
        line.push_str(&format!(
            " Last router auto-admission: [{}].",
            auto.admitted.join(", ")
        ));
    }
    line
}

fn ack_router_wakes(run: &mut PlannerRun, prefix: &str) {
    for (_, wake) in run
        .wakes
        .iter_mut()
        .filter(|(id, w)| id.starts_with(prefix) && !w.acknowledged)
    {
        wake.acknowledged = true;
    }
}

fn pending_wake(run: &PlannerRun, prefix: &str) -> bool {
    run.wakes
        .iter()
        .any(|(id, w)| id.starts_with(prefix) && !w.acknowledged)
}

fn sessions_in_tick() -> &'static Mutex<HashSet<String>> {
    static TICKS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    TICKS.get_or_init(Default::default)
}

/// Run one [`tick`] for `sid` on its own task unless one is still running.
pub fn spawn_tick(shared: &Arc<Shared>, sid: &str, now: i64) {
    if !sessions_in_tick().lock().unwrap().insert(sid.to_string()) {
        return;
    }
    let shared = shared.clone();
    let sid = sid.to_string();
    tokio::spawn(async move {
        tick(&shared, &sid, now).await;
        sessions_in_tick().lock().unwrap().remove(&sid);
    });
}

/// One capacity pass over a planner run at `now` (unix seconds): resume
/// workers that stopped early, then check capacity even during coordination.
/// Capacity wakes reach busy parents; auto-admission and in-flight wakes stay idle-only.
pub async fn tick(shared: &Arc<Shared>, sid: &str, now: i64) {
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return;
    };
    if run.status != RunStatus::Running
        || run.phase != PlannerPhase::Implementation
        || run.execution_request.is_none()
    {
        return;
    }
    if run.capacity.pending_adoption.is_some() {
        let _ = crate::planner_adopt::retry_pending_at(shared.as_ref(), sid, now);
    }
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return;
    };
    if let Some(wake) = run.wakes.get("adoption:handoff") {
        let notify = crate::planner_guard::with_runtime(shared, sid, |rt| {
            if rt.adoption_notice.as_deref() == Some(&wake.reason) {
                return false;
            }
            rt.adoption_notice = Some(wake.reason.clone());
            true
        });
        if notify {
            crate::session::notify_user(shared, sid, format!("router-acp · {}", wake.reason));
        }
    }
    let timing = timing(shared);
    resume_workers(shared, sid, &run, now).await;
    // The audit runs whatever the free slots and the parent's activity.
    crate::planner_guard::audit_tick(shared, sid, now).await;
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return;
    };
    if run.status != RunStatus::Running {
        return;
    }
    // Failed environment findings must reach coordination even if ordinary
    // worker wakes keep the parent busy continuously. This creates a wake only;
    // operator turns and queued input retain their existing delivery protections.
    if crate::planner_wake::failed_environment(&run.observations)
        || run
            .wakes
            .iter()
            .any(|(id, wake)| id.starts_with("audit:release:") && !wake.acknowledged)
    {
        crate::planner_guard::audit_wake(shared, sid, &run, now, now);
    }
    // Custody fences the affected assignment, not the entire queue. Keep its
    // recovery wake without starving independent capacity or release/audit wakes.
    // Admission and dispatch still enforce source ownership and dependencies.
    if run.works.iter().any(|(id, work)| {
        work.reconciliation.required && !run.wakes.contains_key(&format!("recovery:{id}"))
    }) {
        let _ = planner_workflow::mutate(shared, sid, |current| {
            for (id, work) in &current.works {
                if work.reconciliation.required {
                    current.wakes.entry(format!("recovery:{id}")).or_insert_with(|| Wake {
                        work_id: id.clone(),
                        reason: "Parent reconciliation required before resuming this adopted assignment: review current scope, source and CONTINUE, then record planner_workflow reconcile. Independent admissions and releases continue under their normal ownership, dependency and evidence gates.".into(),
                        ..Default::default()
                    });
                }
            }
            Ok(())
        });
    }
    let Some((parent_idle, pinning)) =
        shared.with_session(sid, |s| (s.prompt_activity == 0, s.pinning))
    else {
        return;
    };
    if pinning || !run.approval_waits.is_empty() || !run.parent_queue.is_empty() {
        return;
    }
    let Some((last, _)) = shared.state.lock().unwrap().last_activity(sid) else {
        return;
    };
    let idle = now - last;
    let filled = if run.capacity.pending_adoption.is_none() {
        fill_slots(shared, sid, run, now, last, parent_idle, &timing).await
    } else {
        false
    };
    if !parent_idle {
        return;
    }
    if !filled {
        inflight_wake(shared, sid, now, idle, &timing);
    }
    // The turn-end gate re-prompts at once; the audit wake repeats while
    // findings stay open.
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return;
    };
    crate::planner_guard::gate_tick(shared, sid, &run, now);
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return;
    };
    crate::planner_guard::audit_wake(shared, sid, &run, now, last);
}

/// Returns whether this pass issued a capacity wake or auto-admitted.
async fn fill_slots(
    shared: &Arc<Shared>,
    sid: &str,
    mut run: PlannerRun,
    now: i64,
    last: i64,
    parent_idle: bool,
    timing: &PlannerCapacityConfig,
) -> bool {
    if !host_configured(shared) {
        return false;
    }
    let now_ms = now * 1000;
    let (live, max, free) = slots(shared, &run);
    if free == 0 {
        reset_unfilled(shared, sid, &run);
        return false;
    }
    // Re-ask at most every 30 s. Continuous parent tool calls must neither
    // suppress capacity checks nor turn them into one host process per tick.
    let query_ms = (timing.wake_after_secs as i64).clamp(5, 30) * 1000;
    let fresh = run
        .capacity
        .host
        .as_ref()
        .is_some_and(|h| now_ms - h.at_ms < query_ms);
    if !fresh {
        let Some(snapshot) = query_host(shared, sid, &run, now_ms).await else {
            return false;
        };
        match planner_workflow::mutate(shared, sid, |run| {
            run.capacity.host = Some(snapshot.clone());
            Ok(())
        }) {
            Ok(saved) => run = saved,
            Err(error) => {
                tracing::warn!(session = sid, %error, "capacity snapshot not saved");
                return false;
            }
        }
    }
    let host = run.capacity.host.clone().expect("snapshot present");
    let unheld = unheld_candidates(&run, now_ms);
    if host.ok && unheld.is_empty() {
        reset_unfilled(shared, sid, &run);
        return false;
    }
    if run.capacity.unfilled_since.is_none() {
        let since = if parent_idle { last } else { now };
        match planner_workflow::mutate(shared, sid, |current| {
            current.capacity.unfilled_since.get_or_insert(since);
            Ok(())
        }) {
            Ok(saved) => run = saved,
            Err(error) => {
                tracing::warn!(session = sid, %error, "capacity condition not saved");
                return false;
            }
        }
    }
    let idle = now - last;
    let pinned = shared
        .with_session(sid, |s| s.pin.is_some())
        .unwrap_or(false);
    if parent_idle
        && host.ok
        && pinned
        && !pending_wake(&run, "capacity:")
        && (run.capacity.unfilled_wakes >= timing.auto_admit_after_wakes
            || idle >= timing.auto_admit_after_secs as i64)
    {
        auto_admit(shared, sid, &run, unheld, live, max, now).await;
        return true;
    }
    let since = run
        .capacity
        .unfilled_since
        .unwrap_or(now)
        .max(run.capacity.last_wake_at);
    if now - since < timing.wake_after_secs as i64 || pending_wake(&run, "capacity:") {
        return false;
    }
    let reason = if host.ok {
        let mut text = format!(
            "capacity: {live} of {max} worker slots are in use, {free} free, and the capacity \
             host lists {} admissible candidate(s) no hold covers, in priority order:\n",
            unheld.len()
        );
        for (i, c) in unheld.iter().enumerate() {
            text.push_str(&format!(
                "{}. work_id {} (external_id {}, plan {})\n",
                i + 1,
                c.work.work_id,
                c.work.external_id.as_deref().unwrap_or("none"),
                c.work.plan_id
            ));
        }
        text.push_str(&format!(
            "Admit up to {free} of them NOW, in this order, and start each one \
             (delegate_task {{work_id, task, background: true}}), or record planner_workflow \
             `hold` {{plan_id, reason, detail}} for each one you skip; the only reasons are \
             source-conflict, dependency and disk-floor. Breadth is always permitted: \
             depth-first ordering never leaves a slot empty. This is capacity wake {} of {}; \
             after that the router admits and starts the unheld candidates itself.",
            run.capacity.unfilled_wakes + 1,
            timing.auto_admit_after_wakes
        ));
        text
    } else {
        format!(
            "capacity: {live} of {max} worker slots are in use, {free} free, and the capacity \
             host is UNAVAILABLE: {}. An unknown answer is not \"no work\": completion is \
             refused until the host answers. Fix the host (its command, exit status and JSON) \
             now, and fill the free slots from the plan source meanwhile.",
            host.error.as_deref().unwrap_or("no answer")
        )
    };
    let counted = host.ok;
    let result = planner_workflow::mutate(shared, sid, |run| {
        if pending_wake(run, "capacity:") {
            return Ok(());
        }
        if counted {
            run.capacity.unfilled_wakes += 1;
        }
        run.capacity.last_wake_at = now;
        run.wakes.insert(
            format!("capacity:{}", uuid::Uuid::new_v4()),
            Wake {
                reason: reason.clone(),
                work_id: sid.to_string(),
                ..Default::default()
            },
        );
        Ok(())
    });
    if let Err(error) = result {
        tracing::warn!(session = sid, %error, "capacity wake not recorded");
    }
    true
}

fn reset_unfilled(shared: &Shared, sid: &str, run: &PlannerRun) {
    if run.capacity.unfilled_since.is_none()
        && run.capacity.unfilled_wakes == 0
        && !pending_wake(run, "capacity:")
    {
        return;
    }
    let _ = planner_workflow::mutate(shared, sid, |run| {
        run.capacity.unfilled_wakes = 0;
        run.capacity.unfilled_since = None;
        ack_router_wakes(run, "capacity:");
        Ok(())
    });
}

/// Admit and start up to `max - live` unheld candidates, in host order,
/// through the parent's own admit and delegate path. A refused start
/// becomes a recorded hold, never a silent drop.
async fn auto_admit(
    shared: &Arc<Shared>,
    sid: &str,
    run: &PlannerRun,
    unheld: Vec<Candidate>,
    live: usize,
    max: usize,
    now: i64,
) {
    let free = max.saturating_sub(live);
    let round = run.capacity.auto_admit_runs + 1;
    let mut admitted = Vec::new();
    let mut held = Vec::new();
    for candidate in unheld.into_iter().take(free) {
        let work_id = candidate.work.work_id.clone();
        let known = planner_workflow::load(shared, sid)
            .ok()
            .flatten()
            .is_some_and(|run| run.works.contains_key(&work_id));
        if !known
            && let Err(error) = planner_workflow::operate(
                shared,
                sid,
                None,
                Operation::Admit {
                    key: format!("auto-admit:{work_id}:{round}"),
                    work: candidate.work.clone(),
                },
            )
            .await
        {
            held.push(format!("{work_id} (admission refused: {error})"));
            continue;
        }
        match crate::delegate_mcp::start_planner_worker(shared, sid, &work_id, candidate.task).await
        {
            Ok(_) => admitted.push(work_id),
            Err(error) => {
                let reason = if error.contains("dependencies") {
                    "dependency"
                } else {
                    "disk-floor"
                };
                let detail = format!("router auto-admission of {work_id} refused: {error}");
                let plan_id = candidate.work.plan_id.clone();
                let _ = planner_workflow::mutate(shared, sid, |run| {
                    run.capacity.holds.insert(
                        plan_id.clone(),
                        Hold {
                            plan_id: plan_id.clone(),
                            reason: reason.into(),
                            detail: detail.clone(),
                            expires_at_ms: now * 1000 + HOLD_MS,
                        },
                    );
                    Ok(())
                });
                held.push(format!("{work_id} ({reason}: {error})"));
            }
        }
    }
    let mut reason = format!(
        "auto-admit: router admitted {} to fill {free} free slots; review them as normal.",
        if admitted.is_empty() {
            "nothing".to_string()
        } else {
            admitted.join(", ")
        }
    );
    if !held.is_empty() {
        reason.push_str(&format!(
            " Not started (recorded as holds): {}.",
            held.join("; ")
        ));
    }
    let record = AutoAdmission {
        at_ms: now * 1000,
        run: round,
        free,
        admitted: admitted.clone(),
        held: held.clone(),
    };
    let result = planner_workflow::mutate(shared, sid, |run| {
        run.capacity.auto_admit_runs = round;
        run.capacity.last_auto_admission = Some(record.clone());
        run.capacity.unfilled_wakes = 0;
        run.capacity.last_wake_at = now;
        ack_router_wakes(run, "capacity:");
        run.wakes.insert(
            format!("auto-admit:{round}"),
            Wake {
                reason: reason.clone(),
                work_id: sid.to_string(),
                ..Default::default()
            },
        );
        Ok(())
    });
    if let Err(error) = result {
        tracing::warn!(session = sid, %error, "auto-admission not recorded");
    }
    tracing::warn!(session = sid, admitted = ?admitted, held = ?held, "router auto-admitted planner work to fill free worker slots");
    crate::session::notify_user(shared, sid, format!("router-acp · {reason}"));
}

fn recovery_prerequisite(run: &PlannerRun, work: &planner_workflow::Work) -> Option<String> {
    if work.reconciliation.required {
        return Some("parent reconciliation required: record current scope and preserved source with planner_workflow reconcile".into());
    }
    let workspace = work.workspace.as_ref()?;
    if let Err(error) = planner_workflow::workspace_available(workspace) {
        return Some(error);
    }
    planner_workflow::dependencies_ready(run, work).err()
}

fn failure_fingerprint(work: &planner_workflow::Work, prerequisite: &Option<String>) -> String {
    crate::planner_skills::content_hash(
        &json!({
            "reason":work.reason,"prerequisite":prerequisite,"artifact":work.artifact,
            "workspace":work.workspace.as_ref().map(|w| (&w.path, &w.lease)),
            "review":work.review
        })
        .to_string(),
    )
}

fn stopped_early(work: &planner_workflow::Work) -> bool {
    work.attempt.as_ref().is_some_and(|a| a.ended)
        && work.status == WorkStatus::Interrupted
        && !work.paused
        && work.finish.is_none()
        && work.workspace.is_some()
        && work
            .handoff
            .as_ref()
            .is_none_or(|h| !PARENT_HANDOFFS.contains(&h.kind.as_str()))
}

/// Opt-in exact observation identity in a waiting handoff. A stale or failed
/// observer never parks work indefinitely; normal reconciliation resumes.
pub(crate) fn waiting_on_build(run: &PlannerRun, work: &planner_workflow::Work, now: i64) -> bool {
    let id = format!("ci:{}", work.spec.work_id);
    run.capacity.audit.as_ref().is_some_and(|audit| {
        audit.ok
            && audit.observed.contains(&id)
            && now * 1000 >= audit.at_ms
            && now * 1000 - audit.at_ms < 120_000
    }) && run.observations.get(&id).is_some_and(|observation| {
        observation.state == crate::planner_guard::ObservationState::Pending
            && work.handoff.as_ref().is_some_and(|handoff| {
                handoff.kind == "waiting"
                    && handoff
                        .message
                        .split_whitespace()
                        .any(|word| word == format!("{id}@{}", observation.revision))
            })
    })
}

/// Retry transient interruptions, but do not allocate another model to an
/// unchanged failure or a missing workspace. The parent owns reconciliation.
async fn resume_workers(shared: &Arc<Shared>, sid: &str, run: &PlannerRun, now: i64) {
    let (mut live, max, _) = slots(shared, run);
    let pinned = shared
        .with_session(sid, |s| s.pin.is_some())
        .unwrap_or(false);
    for (id, work) in &run.works {
        let Some(attempt) = &work.attempt else {
            continue;
        };
        let state = run.capacity.resumes.get(id);
        if !stopped_early(work) {
            if state.is_some()
                && matches!(work.status, WorkStatus::Finished | WorkStatus::Integrated)
            {
                let _ = planner_workflow::mutate(shared, sid, |run| {
                    if run.works.get(id).is_some_and(|w| {
                        matches!(w.status, WorkStatus::Finished | WorkStatus::Integrated)
                    }) {
                        run.capacity.resumes.remove(id);
                    }
                    Ok(())
                });
            }
            continue;
        }
        let prerequisite = recovery_prerequisite(run, work);
        if prerequisite.is_none() && waiting_on_build(run, work, now) {
            continue;
        }
        let fingerprint = failure_fingerprint(work, &prerequisite);
        let changed = state.is_none_or(|s| s.failure_fingerprint.as_ref() != Some(&fingerprint));
        if changed || state.is_some_and(|s| s.due_at.is_none() && s.suspended.is_none()) {
            let count = if changed {
                1
            } else {
                state.map_or(1, |s| s.same_failure_count.saturating_add(1))
            };
            let suspended = prerequisite.clone().or_else(|| (count >= SAME_FAILURE_LIMIT).then(||
                format!("{count} consecutive unchanged failures; reconcile this work's evidence and prerequisite before explicitly retrying")));
            let _ = planner_workflow::mutate(shared, sid, |current| {
                let current_work = current.works.get(id).ok_or("work disappeared")?;
                if !stopped_early(current_work)
                    || current_work.attempt.as_ref().map(|a| &a.id) != Some(&attempt.id)
                    || failure_fingerprint(
                        current_work,
                        &recovery_prerequisite(current, current_work),
                    ) != fingerprint
                {
                    return Err("recovery observation changed".into());
                }
                let state = current.capacity.resumes.entry(id.clone()).or_default();
                state.failure_fingerprint = Some(fingerprint.clone());
                state.same_failure_count = count;
                state.suspended = suspended.clone();
                let step =
                    RESUME_BACKOFF_SECS[(state.count as usize).min(RESUME_BACKOFF_SECS.len() - 1)];
                state.due_at = suspended.is_none().then_some(now + step);
                let wake_id = format!("recovery:{id}");
                if let Some(reason) = &suspended {
                    current.wakes.insert(wake_id, Wake {
                        work_id: id.clone(),
                        reason: format!("recovery required: {reason}. Automatic retries are suspended for this unchanged condition; other work continues. No receipt or completion was inferred."),
                        ..Default::default()
                    });
                } else if let Some(wake) = current.wakes.get_mut(&wake_id) {
                    wake.acknowledged = true;
                }
                Ok(())
            });
            continue;
        }
        if state.is_none_or(|s| s.suspended.is_some() || s.due_at.is_none_or(|at| at > now))
            || !pinned
            || live >= max
        {
            continue;
        }
        let open = attempt.delegate_id.as_ref().and_then(|delegate| {
            shared
                .live_delegates
                .lock()
                .unwrap()
                .get(delegate)
                .map(|d| (delegate.clone(), d.turns))
        });
        let prior = state.and_then(|s| s.followup.as_ref());
        let followup = open.filter(|(delegate, turns)| {
            prior.is_none_or(|(was, at_turns)| was != delegate || turns > at_turns)
        });
        let saved = planner_workflow::mutate(shared, sid, |current| {
            let current_work = current.works.get(id).ok_or("work disappeared")?;
            if current.status != RunStatus::Running
                || current.phase != PlannerPhase::Implementation
                || !stopped_early(current_work)
                || current_work.attempt.as_ref().map(|a| &a.id) != Some(&attempt.id)
                || failure_fingerprint(current_work, &recovery_prerequisite(current, current_work))
                    != fingerprint
            {
                return Err("recovery observation changed".into());
            }
            let state = current
                .capacity
                .resumes
                .get_mut(id)
                .ok_or("recovery state disappeared")?;
            if state.suspended.is_some() || state.due_at.is_none_or(|at| at > now) {
                return Err("recovery already claimed".into());
            }
            state.count += 1;
            state.due_at = None;
            state.followup = followup.clone();
            Ok(())
        });
        if saved.is_err() {
            continue;
        }
        live += 1;
        let result = match followup {
            Some((delegate_id, _)) => crate::delegate_mcp::resume_planner_worker(
                shared,
                sid,
                delegate_id,
                CONTINUE_PROMPT.to_string(),
            ),
            None => {
                crate::delegate_mcp::start_planner_worker(
                    shared,
                    sid,
                    id,
                    format!("{CONTINUE_PROMPT}\nAssigned scope: {}", work.spec.scope),
                )
                .await
            }
        };
        match result {
            Ok(_) => tracing::info!(
                session = sid,
                work_id = id,
                "resumed a worker that stopped early"
            ),
            Err(error) => {
                tracing::warn!(session = sid, work_id = id, %error, "worker resumption failed; reconciliation applies before another retry")
            }
        }
    }
}

/// An idle parent with a release work, or any running work, mid-flight gets
/// a wake after `wake_after_secs` of idleness, then at most every
/// `inflight_wake_secs`, so it never sits idle after a staging deploy.
fn inflight_wake(
    shared: &Arc<Shared>,
    sid: &str,
    now: i64,
    idle: i64,
    timing: &PlannerCapacityConfig,
) {
    let inflight = |run: &PlannerRun| {
        let mut inflight = run
            .works
            .iter()
            .filter(|(_, w)| {
                (w.spec.plan_id == "release" && !terminal(w.status))
                    || w.status == WorkStatus::Running
            })
            .map(|(id, w)| format!("{id} ({:?})", w.status))
            .collect::<Vec<_>>();
        inflight.extend(
            run.releases
                .iter()
                .filter(|(_, release)| release.active())
                .map(|(id, release)| format!("{id} ({:?})", release.stage())),
        );
        let due = !inflight.is_empty()
            && idle >= timing.wake_after_secs as i64
            && now - run.capacity.last_inflight_wake_at >= timing.inflight_wake_secs as i64
            && !run.wakes.values().any(|w| !w.acknowledged);
        due.then_some(inflight)
    };
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return;
    };
    if inflight(&run).is_none() {
        return;
    }
    let _ = planner_workflow::mutate(shared, sid, |run| {
        let Some(inflight) = inflight(run) else {
            return Ok(());
        };
        run.capacity.last_inflight_wake_at = now;
        run.wakes.insert(
            format!("inflight:{}", uuid::Uuid::new_v4()),
            Wake {
                reason: format!(
                    "inflight: {} mid-flight and the parent has been idle for {idle} s. Do not \
                     sit idle on a release or deploy: poll its build and rollout yourself, run \
                     the next step it owes (a staging deploy is followed by its smokescreen), \
                     review any receipt, and fill every free worker slot.",
                    inflight.join(", ")
                ),
                work_id: sid.to_string(),
                ..Default::default()
            },
        );
        Ok(())
    });
}
