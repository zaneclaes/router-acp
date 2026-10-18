//! The native implement-all watcher's guarantees, enforced by the router for
//! planner runs (beside the capacity enforcement in `planner_capacity`):
//!
//! - **Audit** (`routers.planner.audit`): a repository host reports the run's
//!   defects. The capacity tick asks it at most every `interval_secs` and at
//!   every parent turn end; any defect, or a host that cannot answer, wakes
//!   the idle parent with an `audit:` wake that never gives up.
//! - **Turn-end gate**: when the parent's turn ends while audit defects exist,
//!   or while free slots and unheld capacity candidates exist, the router
//!   re-prompts it at once (`gate:`), then after 30 s, 2 min and every 5 min,
//!   with no cap: the native Stop guard.
//! - **Completion, holds, integration**: `complete` needs a clean
//!   `completing` audit; a `hold` needs the audit's `hold_check`; `integrate`
//!   and a blocked/watching `disposition` need `routers.planner.integrate_check`.
//! - **Planner children**: a `delegate_stop` the lifecycle host refuses with
//!   exit 2 (dirty or unpushed source) interrupts the work so it is resumed to
//!   commit and push, and is accepted rather than redelivered forever.
//! - **Native adoption**: a run's first `planner_allocate` first asks the
//!   workspace host to take over an active native implement-all run.
//!
//! A host that fails, times out or answers malformed JSON is never read as
//! clean.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use crate::config::PlannerWorkspaceConfig;
use crate::planner_capacity::{self, HOLD_MS};
use crate::planner_workflow::{self, PlannerRun, RunStatus, Wake, WorkStatus};
use crate::session::Shared;

/// Delay before each turn-end re-prompt of a parent that keeps ending its
/// turn on the same defects; the last entry repeats with no cap.
pub const GATE_BACKOFF_SECS: [i64; 4] = [0, 30, 120, 300];
/// Delay before each host-requested continuation of a planner child; the
/// last entry repeats with no cap.
pub const CHILD_CONTINUE_BACKOFF_SECS: [u64; 4] = [0, 30, 120, 300];
/// Redelivery spacing of an unacknowledged planner wake after its first,
/// second and later deliveries. Wakes are never dropped.
pub const WAKE_RETRY_SECS: [i64; 3] = [30, 120, 300];
/// Attempts at the workspace host's `adopt-native` (one try, three retries).
const ADOPT_ATTEMPTS: usize = 4;
const ADOPT_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// When an unacknowledged wake delivered `deliveries` times is due again.
pub fn wake_retry_secs(deliveries: u32) -> i64 {
    WAKE_RETRY_SECS[(deliveries.saturating_sub(1) as usize).min(WAKE_RETRY_SECS.len() - 1)]
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Defect {
    pub code: String,
    #[serde(default)]
    pub detail: String,
}

/// The audit host's last answer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditSnapshot {
    pub at_ms: i64,
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub defects: Vec<Defect>,
    #[serde(default)]
    pub observed: BTreeSet<String>,
    #[serde(default)]
    pub completed_guides: Option<BTreeSet<String>>,
}

/// Turn-end gate state: the backoff step of the next re-prompt. (Which turn
/// end was last judged is in-process, like the turn ends themselves.)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GateState {
    #[serde(default)]
    pub count: u32,
    #[serde(default)]
    pub last_at: i64,
}

#[derive(Deserialize)]
struct AuditReply {
    #[serde(default)]
    defects: Vec<Defect>,
    #[serde(default)]
    hold_valid: Option<bool>,
    #[serde(default)]
    reason: Option<String>,
    /// Optional observations from the repository's existing CI/rollout
    /// readers. They wake the parent; they never satisfy a release gate.
    #[serde(default)]
    observations: BTreeMap<String, Observation>,
    #[serde(default)]
    completed_guides: Option<BTreeSet<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub revision: String,
    pub reference: String,
    pub state: ObservationState,
    pub evidence: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObservationState {
    Pending,
    Passed,
    Failed,
    Unknown,
}

fn validate_observations(observations: &BTreeMap<String, Observation>) -> Result<(), String> {
    if observations.len() > 128 {
        return Err("audit returned more than 128 observations".into());
    }
    for (id, observation) in observations {
        if id.trim().is_empty()
            || id.len() > 256
            || observation.revision.len() != 40
            || !observation.revision.bytes().all(|b| b.is_ascii_hexdigit())
            || !planner_workflow::valid_evidence(&observation.reference)
            || !planner_workflow::valid_evidence(&observation.evidence)
        {
            return Err(
                "audit observation requires an id, full revision, exact reference and evidence"
                    .into(),
            );
        }
    }
    Ok(())
}

fn record_observations(
    run: &mut PlannerRun,
    sid: &str,
    observations: &BTreeMap<String, Observation>,
) {
    for (id, observation) in observations {
        if run.observations.get(id) == Some(observation) {
            continue;
        }
        // One pending wake per observed target; the model sees the latest
        // actionable delta. Existing conversation history is not pruned.
        run.observations.insert(id.clone(), observation.clone());
        let wake_id = format!("observation:{id}");
        if observation.state != ObservationState::Pending {
            run.wakes.insert(wake_id, Wake {
                work_id: sid.into(),
                reason: format!("Repository observation {id}: {:?} for {} at {}. Evidence: {}. Reconcile the exact revision and advance its next required step; this observation is not an acceptance, smoke or completion receipt.", observation.state, observation.reference, observation.revision, observation.evidence),
                ..Default::default()
            });
        } else if let Some(wake) = run.wakes.get_mut(&wake_id) {
            wake.acknowledged = true;
        }
    }
}

/// In-process state of one planner session that never needs to survive the
/// router: its parent's turn ends, the child sessions a drain may cancel and
/// an active drain.
#[derive(Default)]
pub struct SessionRuntime {
    pub turn_ends: u64,
    pub turn_ended_at: i64,
    /// The last turn end the gate judged.
    pub gate_seen: u64,
    /// A router-owned turn (wake delivery) holds the parent now; operator
    /// input preempts it (`crate::planner_preempt`).
    pub router_turn: bool,
    /// That router-owned turn was cancelled for operator input: its wakes
    /// stay pending and are redelivered after the operator's turn.
    pub preempted: bool,
    /// The stuck-turn watchdog cancelled or released the current turn
    /// (`crate::planner_watchdog`): its wakes go again at once.
    pub stalled: bool,
    /// Each work's latest child session (process key, downstream session id).
    pub children: HashMap<String, (crate::downstream::ProcessKey, String)>,
    pub drain: Option<crate::planner_shutdown::Drain>,
    /// Every `_router/shutdown_progress` this session sent, in order.
    pub progress: Vec<Value>,
}

pub fn with_runtime<T>(shared: &Shared, sid: &str, f: impl FnOnce(&mut SessionRuntime) -> T) -> T {
    f(shared
        .planner_runtime
        .lock()
        .unwrap()
        .entry(sid.to_string())
        .or_default())
}

/// The parent's turn ended at `at` (unix seconds): the gate evaluates it and
/// the next tick audits.
pub fn parent_turn_ended(shared: &Shared, sid: &str, at: i64) {
    with_runtime(shared, sid, |rt| {
        rt.turn_ends += 1;
        rt.turn_ended_at = at;
    });
}

fn turn_info(shared: &Shared, sid: &str) -> (u64, i64) {
    shared
        .planner_runtime
        .lock()
        .unwrap()
        .get(sid)
        .map_or((0, 0), |rt| (rt.turn_ends, rt.turn_ended_at))
}

/// Remember the child session a planner work's turn runs on, so a drain can
/// cancel it.
pub fn track_child(
    shared: &Shared,
    sid: &str,
    work_id: &str,
    process_key: crate::downstream::ProcessKey,
    downstream_sid: String,
) {
    with_runtime(shared, sid, |rt| {
        rt.children
            .insert(work_id.to_string(), (process_key, downstream_sid));
    });
}

pub struct HostOutput {
    pub code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: String,
}

impl HostOutput {
    pub fn describe(&self) -> String {
        let status = self
            .code
            .map_or_else(|| "a signal".to_string(), |c| format!("exit status {c}"));
        let stderr = self.stderr.chars().take(500).collect::<String>();
        if stderr.is_empty() {
            status
        } else {
            format!("{status}: {stderr}")
        }
    }
}

/// Launch a repository host like `workspace`: no shell, cwd = the session
/// repository, one JSON object on stdin. `Err` is a spawn failure or timeout.
pub async fn run_host(
    label: &str,
    command: &str,
    args: &[String],
    timeout_ms: u64,
    cwd: &Path,
    input: &Value,
) -> Result<HostOutput, String> {
    let mut child = tokio::process::Command::new(command)
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("{label} `{command}` failed to start: {e}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        // A host that never reads stdin must not fail the call.
        let _ = stdin.write_all(input.to_string().as_bytes()).await;
    }
    let output = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        child.wait_with_output(),
    )
    .await
    .map_err(|_| format!("{label} timed out after {timeout_ms} ms"))?
    .map_err(|e| format!("{label} failed: {e}"))?;
    Ok(HostOutput {
        code: output.status.code(),
        stdout: output.stdout,
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn session_cwd(shared: &Shared, sid: &str) -> Result<std::path::PathBuf, String> {
    shared
        .with_session(sid, |s| s.cwd.clone())
        .ok_or_else(|| "the parent session is not loaded".to_string())
}

/// The works every host request carries.
pub fn works_json(run: &PlannerRun) -> Vec<Value> {
    run.works
        .iter()
        .map(|(id, w)| {
            json!({"work_id":id,"plan_id":w.spec.plan_id,"status":w.status,
                "external_id":w.spec.external_id,
                "revision":w.artifact.as_ref().map(|a| &a.revision)})
        })
        .collect()
}

pub fn active_holds(run: &PlannerRun, now_ms: i64) -> Vec<&planner_capacity::Hold> {
    run.capacity
        .holds
        .values()
        .filter(|h| h.expires_at_ms > now_ms)
        .collect()
}

fn audit_configured(shared: &Shared) -> bool {
    shared.cfg.routers.planner.audit.is_some()
}

/// Ask the audit host. `None` when none is configured; `Err` when it could
/// not answer (never a clean audit).
async fn audit(
    shared: &Shared,
    sid: &str,
    run: &PlannerRun,
    now_ms: i64,
    completing: bool,
    hold_check: Option<Value>,
) -> Option<Result<AuditReply, String>> {
    let cfg = shared.cfg.routers.planner.audit.clone()?;
    let cwd = match session_cwd(shared, sid) {
        Ok(cwd) => cwd,
        Err(error) => return Some(Err(error)),
    };
    let (_, max, free) = planner_capacity::slots(shared, run);
    let mut input = json!({"event":"audit","cwd":cwd,"session_id":sid,"free":free,"max":max,
        "run_status":run.status,"completing":completing,"works":works_json(run),
        "holds":active_holds(run, now_ms),"releases":run.releases,"observations":run.observations});
    if let Some(check) = hold_check {
        input["hold_check"] = check;
    }
    let answer = match run_host(
        "audit host",
        &cfg.command,
        &cfg.args,
        cfg.timeout_ms,
        &cwd,
        &input,
    )
    .await
    {
        Ok(out) if out.code == Some(0) => serde_json::from_slice::<AuditReply>(&out.stdout)
            .map_err(|e| format!("audit host returned invalid JSON: {e}"))
            .and_then(|reply| {
                validate_observations(&reply.observations)?;
                Ok(reply)
            }),
        Ok(out) => Err(format!("audit host exited with {}", out.describe())),
        Err(error) => Err(error),
    };
    if let Err(error) = &answer {
        tracing::error!(session = sid, %error, "planner audit host unavailable; an unknown audit is not a clean one");
    }
    Some(answer)
}

/// The capacity tick's audit: at most every `interval_secs`, and again after
/// every parent turn end, whatever the free slots.
pub async fn audit_tick(shared: &Arc<Shared>, sid: &str, now: i64) {
    let Some(cfg) = shared.cfg.routers.planner.audit.clone() else {
        return;
    };
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return;
    };
    let now_ms = now * 1000;
    let (_, ended_at) = turn_info(shared, sid);
    let due = run.capacity.audit.as_ref().is_none_or(|a| {
        now_ms - a.at_ms >= cfg.interval_secs as i64 * 1000 || ended_at * 1000 > a.at_ms
    });
    if !due {
        return;
    }
    let Some(answer) = audit(shared, sid, &run, now_ms, false, None).await else {
        return;
    };
    let observations = answer
        .as_ref()
        .ok()
        .map(|reply| reply.observations.clone())
        .unwrap_or_default();
    let snapshot = match answer {
        Ok(reply) => AuditSnapshot {
            at_ms: now_ms,
            ok: true,
            error: None,
            defects: reply.defects,
            observed: reply.observations.keys().cloned().collect(),
            completed_guides: reply.completed_guides,
        },
        Err(error) => AuditSnapshot {
            at_ms: now_ms,
            ok: false,
            error: Some(error),
            defects: Vec::new(),
            observed: BTreeSet::new(),
            completed_guides: None,
        },
    };
    if let Err(error) = planner_workflow::mutate(shared, sid, |run| {
        record_observations(run, sid, &observations);
        if run.completed_guides_at_start.is_none() {
            run.completed_guides_at_start = snapshot.completed_guides.clone();
        }
        run.capacity.audit = Some(snapshot.clone());
        Ok(())
    }) {
        tracing::warn!(session = sid, %error, "audit snapshot not saved");
    }
}

/// The audit's open findings, one line each; an unavailable host is one.
pub fn audit_findings(run: &PlannerRun) -> Vec<String> {
    match &run.capacity.audit {
        None => Vec::new(),
        Some(a) if !a.ok => vec![format!(
            "audit host UNAVAILABLE ({}): an unavailable audit is never a clean one",
            a.error.as_deref().unwrap_or("no answer")
        )],
        Some(a) => a
            .defects
            .iter()
            .map(|d| format!("{}: {}", d.code, d.detail))
            .collect(),
    }
}

/// Whether the last audit left anything open (defects, or no answer).
pub fn audit_open(run: &PlannerRun) -> bool {
    !audit_findings(run).is_empty()
}

fn pending_wake(run: &PlannerRun, prefix: &str) -> bool {
    run.wakes
        .iter()
        .any(|(id, w)| id.starts_with(prefix) && !w.acknowledged)
}

fn insert_wake(run: &mut PlannerRun, prefix: &str, sid: &str, reason: String) {
    run.wakes.insert(
        format!("{prefix}{}", uuid::Uuid::new_v4()),
        Wake {
            reason,
            work_id: sid.to_string(),
            ..Default::default()
        },
    );
}

/// An idle parent with open audit findings gets an `audit:` wake after
/// `wake_after_secs`, and another every `wake_after_secs` while they remain,
/// with no limit. A pending turn-end gate already carries them.
pub fn audit_wake(shared: &Shared, sid: &str, run: &PlannerRun, now: i64, last: i64) {
    if !audit_configured(shared) {
        return;
    }
    let findings = audit_findings(run);
    if findings.is_empty() {
        if pending_wake(run, "audit:") {
            let _ = planner_workflow::mutate(shared, sid, |run| {
                for (_, w) in run
                    .wakes
                    .iter_mut()
                    .filter(|(id, w)| id.starts_with("audit:") && !w.acknowledged)
                {
                    w.acknowledged = true;
                }
                Ok(())
            });
        }
        return;
    }
    let wait = planner_capacity::timing(shared).wake_after_secs as i64;
    if pending_wake(run, "audit:")
        || pending_wake(run, "gate:")
        || now - last.max(run.capacity.last_audit_wake_at) < wait
    {
        return;
    }
    let unavailable = run.capacity.audit.as_ref().is_some_and(|a| !a.ok);
    let reason = if unavailable {
        format!(
            "audit: the audit host is UNAVAILABLE: {}. An unavailable audit is never a clean \
             one: fix the host (its command, exit status and JSON) now; `complete` is refused \
             until it answers. This wake repeats every {wait} s.",
            run.capacity
                .audit
                .as_ref()
                .and_then(|a| a.error.as_deref())
                .unwrap_or("no answer")
        )
    } else {
        format!(
            "audit: the run auditor reports {} defect(s) the native implement-all watcher \
             would not let this run sit on:\n- {}\nEach is a defect in this run, not a state to \
             wait in: act on every one now (start the release or smoke it owes, record the \
             disposition, fix the root or disk). This wake repeats every {wait} s while any \
             remains.",
            findings.len(),
            findings.join("\n- ")
        )
    };
    let _ = planner_workflow::mutate(shared, sid, |run| {
        if pending_wake(run, "audit:") {
            return Ok(());
        }
        run.capacity.last_audit_wake_at = now;
        insert_wake(run, "audit:", sid, reason.clone());
        Ok(())
    });
}

/// The native Stop guard: a parent whose turn ended while audit findings
/// remain, or while a slot is free and the capacity host lists an unheld
/// candidate, is re-prompted at once, then after 30 s, 2 min and every 5 min
/// of further turn ends, with no cap.
pub fn gate_tick(shared: &Shared, sid: &str, run: &PlannerRun, now: i64) {
    let (turns, ended_at) = turn_info(shared, sid);
    let seen = with_runtime(shared, sid, |rt| rt.gate_seen);
    if turns == 0 || turns <= seen {
        return;
    }
    let judged = |shared: &Shared| with_runtime(shared, sid, |rt| rt.gate_seen = turns);
    // Judge the turn on an audit taken after it ended.
    if audit_configured(shared)
        && run
            .capacity
            .audit
            .as_ref()
            .is_none_or(|a| a.at_ms < ended_at * 1000)
    {
        return;
    }
    let findings = audit_findings(run);
    let (live, max, free) = planner_capacity::slots(shared, run);
    let unheld = if free > 0 {
        planner_capacity::unheld_candidates(run, now * 1000)
    } else {
        Vec::new()
    };
    if findings.is_empty() && unheld.is_empty() {
        judged(shared);
        if run.capacity.gate.count != 0 {
            let _ = planner_workflow::mutate(shared, sid, |run| {
                run.capacity.gate.count = 0;
                Ok(())
            });
        }
        return;
    }
    let step = (run.capacity.gate.count as usize).min(GATE_BACKOFF_SECS.len() - 1);
    if now < ended_at + GATE_BACKOFF_SECS[step] || pending_wake(run, "gate:") {
        return;
    }
    let next = GATE_BACKOFF_SECS[(step + 1).min(GATE_BACKOFF_SECS.len() - 1)];
    let mut text = String::from(
        "gate: your turn ended while this run still owes work the native implement-all Stop \
         guard never lets a parent stop on.\n",
    );
    if !findings.is_empty() {
        text.push_str(&format!("Audit defects:\n- {}\n", findings.join("\n- ")));
    }
    if !unheld.is_empty() {
        text.push_str(&format!(
            "{live} of {max} worker slots are in use, {free} free, and the capacity host lists \
             unheld candidates, in priority order:\n"
        ));
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
            "Admit up to {free} of them now, in order, and start each one, or record a `hold` \
             (source-conflict, dependency or disk-floor) for each one you skip.\n"
        ));
    }
    text.push_str(&format!(
        "Do not end your turn on these. This is turn-end re-prompt {}; while any remains, the \
         next comes {next} s after your next turn end (immediately, then 30 s, 2 min, then \
         every 5 min, with no limit).",
        run.capacity.gate.count + 1
    ));
    let saved = planner_workflow::mutate(shared, sid, |run| {
        if pending_wake(run, "gate:") {
            return Ok(());
        }
        run.capacity.gate.count += 1;
        run.capacity.gate.last_at = now;
        insert_wake(run, "gate:", sid, text.clone());
        Ok(())
    });
    if saved.is_ok() {
        judged(shared);
    }
}

/// `complete` needs a `completing` audit with no defects.
pub async fn completion_audit(shared: &Shared, sid: &str, run: &PlannerRun) -> Result<(), String> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    match audit(shared, sid, run, now_ms, true, None).await {
        None => Ok(()),
        Some(Err(error)) => Err(format!(
            "completion refused: the audit host is UNAVAILABLE ({error}). An unavailable audit \
             is not a clean one; fix the host and retry."
        )),
        Some(Ok(reply)) if reply.defects.is_empty() => Ok(()),
        Some(Ok(reply)) => Err(format!(
            "completion refused: the run auditor reports {} defect(s): {}",
            reply.defects.len(),
            reply
                .defects
                .iter()
                .map(|d| format!("{}: {}", d.code, d.detail))
                .collect::<Vec<_>>()
                .join("; ")
        )),
    }
}

/// A `hold` is verified by the audit host's `hold_check`.
pub async fn verify_hold(
    shared: &Shared,
    sid: &str,
    run: &PlannerRun,
    plan_id: &str,
    reason: &str,
    detail: &str,
) -> Result<(), String> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let check = json!({"plan_id":plan_id,"reason":reason,"detail":detail.trim(),
        "expires_at_ms":now_ms + HOLD_MS});
    match audit(shared, sid, run, now_ms, false, Some(check)).await {
        None => Ok(()),
        Some(Err(error)) => Err(format!(
            "hold not recorded: the audit host is UNAVAILABLE ({error}), so the hold cannot be \
             verified. This is retryable: retry the same hold once the host answers."
        )),
        Some(Ok(reply)) if reply.hold_valid == Some(false) => Err(format!(
            "hold refused: the audit host could not verify it: {}",
            reply.reason.as_deref().unwrap_or("no reason given")
        )),
        Some(Ok(_)) => Ok(()),
    }
}

/// `integrate`, and a `disposition` to blocked/watching, are passed to the
/// integrate-check host first: exit 0 accepts, exit 2 rejects with its
/// stderr, and anything else is a retryable refusal.
pub async fn integrate_check(shared: &Shared, sid: &str, operation: &Value) -> Result<(), String> {
    let Some(cfg) = shared.cfg.routers.planner.integrate_check.clone() else {
        return Ok(());
    };
    let action = operation
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("operation")
        .to_string();
    let unavailable = |why: String| {
        format!(
            "{action} not recorded: the integrate-check host is unavailable ({why}), so the \
             receipt was not judged. This is retryable: retry the same operation once the host \
             answers."
        )
    };
    let cwd = session_cwd(shared, sid).map_err(unavailable)?;
    match run_host(
        "integrate-check host",
        &cfg.command,
        &cfg.args,
        cfg.timeout_ms,
        &cwd,
        operation,
    )
    .await
    {
        Ok(out) if out.code == Some(0) => {
            if matches!(action.as_str(), "finalize" | "release" | "retire")
                && serde_json::from_slice::<Value>(&out.stdout)
                    .ok()
                    .and_then(|v| {
                        v.get("verified_action")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
                    .as_deref()
                    != Some(&action)
            {
                return Err(format!(
                    "{action} not recorded: integrate-check host did not explicitly acknowledge this action; update the repository host before using it"
                ));
            }
            Ok(())
        }
        Ok(out) if out.code == Some(2) => Err(format!(
            "{action} rejected by the integrate-check host: {}",
            if out.stderr.is_empty() {
                "no reason given"
            } else {
                &out.stderr
            }
        )),
        Ok(out) => Err(unavailable(format!("exited with {}", out.describe()))),
        Err(error) => Err(unavailable(error)),
    }
}

/// On a run's first `planner_allocate`, ask the workspace host to take over
/// an active native implement-all run (`{"event":"adopt-native"}`, retried on
/// a non-zero exit), and report the answer to the parent in a wake.
pub async fn adopt_native_once(shared: &Shared, sid: &str, allocator: &PlannerWorkspaceConfig) {
    let mut first = false;
    let claimed = planner_workflow::mutate(shared, sid, |run| {
        first = run.capacity.native_adoption.is_none();
        if first {
            run.capacity.native_adoption = Some(json!({"pending":true}));
        }
        Ok(())
    });
    if claimed.is_err() || !first {
        return;
    }
    let mut answer = Err("the parent session is not loaded".to_string());
    if let Ok(cwd) = session_cwd(shared, sid) {
        let input = json!({"event":"adopt-native","session_id":sid});
        for attempt in 0..ADOPT_ATTEMPTS {
            answer = match run_host(
                "workspace host",
                &allocator.command,
                &allocator.args,
                allocator.timeout_ms,
                &cwd,
                &input,
            )
            .await
            {
                Ok(out) if out.code == Some(0) => adoption_reply(&out.stdout),
                Ok(out) => Err(format!("exited with {}", out.describe())),
                Err(error) => Err(error),
            };
            if answer.is_ok() || attempt + 1 == ADOPT_ATTEMPTS {
                break;
            }
            tokio::time::sleep(ADOPT_RETRY_DELAY).await;
        }
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let (record, reason) = match &answer {
        Ok(reply) => {
            let adopted = reply.get("adopted").cloned().unwrap_or(Value::Null);
            let reason = if adopted.is_null() {
                "adopt-native: no native implement-all run was active in this checkout; there \
                 was nothing to take over."
                    .to_string()
            } else {
                format!(
                    "adopt-native: the router took over an active native implement-all run \
                     before this run's first workspace allocation. The workspace host cancelled \
                     it and handed its workers over:\n{}\nReconcile each handed-over worker now: \
                     admit its plan under the same work_id so its tree and branch are adopted, \
                     or retire it; never leave its source unowned.",
                    serde_json::to_string_pretty(&adopted).unwrap_or_default()
                )
            };
            (json!({"at_ms":now_ms,"ok":true,"adopted":adopted}), reason)
        }
        Err(error) => (
            json!({"at_ms":now_ms,"ok":false,"error":error}),
            format!(
                "adopt-native: the workspace host could not report a native implement-all run \
                 after {ADOPT_ATTEMPTS} tries ({error}). A native run may still be active in this \
                 checkout: check it (ImplementAllGuard status) and stop it before its workers and \
                 these collide."
            ),
        ),
    };
    let _ = planner_workflow::mutate(shared, sid, |run| {
        run.capacity.native_adoption = Some(record.clone());
        insert_wake(run, "adopt-native:", sid, reason.clone());
        Ok(())
    });
}

fn adoption_reply(stdout: &[u8]) -> Result<Value, String> {
    serde_json::from_slice::<Value>(stdout)
        .ok()
        .filter(|v| v.get("adopted").is_some())
        .ok_or_else(|| "workspace host answered adopt-native without `adopted`".to_string())
}

/// A lifecycle host refused a planner child's `delegate_stop` with exit 2:
/// the worker stopped with dirty or unpushed source. Interrupt the work so
/// worker resumption brings it back to commit and push, and accept the event
/// (returns `true`) instead of redelivering it forever. `false` for anything
/// that is not a planner child's stop.
pub fn planner_stop_refused(shared: &Shared, payload: &str, stderr: &str) -> bool {
    let Ok(event) = serde_json::from_str::<Value>(payload) else {
        return false;
    };
    if event.get("event").and_then(Value::as_str) != Some("delegate_stop") {
        return false;
    }
    let (Some(sid), Some(worker)) = (
        event
            .get("parent_router_session_id")
            .and_then(Value::as_str),
        event.get("worker_id").and_then(Value::as_str),
    ) else {
        return false;
    };
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return false;
    };
    let Some(work_id) = run
        .works
        .iter()
        .find(|(_, w)| w.child_id == worker)
        .map(|(id, _)| id.clone())
    else {
        return false;
    };
    let outcome = event
        .get("outcome")
        .and_then(Value::as_str)
        .unwrap_or("stopped");
    let reason = format!(
        "worker stopped ({outcome}) with dirty or unpushed source: {}; it is resumed to commit \
         and push",
        stderr.chars().take(300).collect::<String>()
    );
    let saved = planner_workflow::mutate(shared, sid, |run| {
        let Some(work) = run.works.get_mut(&work_id) else {
            return Ok(());
        };
        // A live replacement attempt owns the work; its own stop is judged.
        if work.paused
            || work.attempt.as_ref().is_some_and(|a| !a.ended)
            || matches!(
                work.status,
                WorkStatus::Finished
                    | WorkStatus::Integrated
                    | WorkStatus::Blocked
                    | WorkStatus::Watching
            )
        {
            return Ok(());
        }
        work.status = WorkStatus::Interrupted;
        work.reason = Some(reason.clone());
        // A handoff would keep resumption away; the source is owed first.
        work.handoff = None;
        Ok(())
    });
    match saved {
        Ok(_) => {
            tracing::warn!(
                session = sid,
                work_id,
                "planner worker stopped with dirty or unpushed source; it will be resumed"
            );
            true
        }
        Err(error) => {
            tracing::warn!(session = sid, work_id, %error, "planner stop refusal not recorded; the event stays queued");
            false
        }
    }
}

/// Whether a planner child may take another host-requested continuation.
pub fn child_may_continue(
    shared: &Shared,
    sid: &str,
    identity: &planner_workflow::WorkerIdentity,
) -> bool {
    if crate::planner_shutdown::cancelled(shared, sid, &identity.work_id) {
        return false;
    }
    planner_workflow::load(shared, sid)
        .ok()
        .flatten()
        .is_some_and(|run| {
            matches!(run.status, RunStatus::Running | RunStatus::Draining)
                && run.works.get(&identity.work_id).is_some_and(|w| {
                    !w.paused
                        && w.attempt
                            .as_ref()
                            .is_some_and(|a| a.id == identity.attempt_id && !a.ended)
                })
        })
}

/// Wait out a planner child's continuation backoff (`continued` earlier
/// continuations). `None`: stop continuing it (its run stopped, its work was
/// paused or a drain cancelled it). `Some(Some(message))`: a shutdown drain's
/// message, which goes at once instead of the host's. `Some(None)`: send the
/// host's message.
pub async fn child_continuation(
    shared: &Shared,
    sid: &str,
    identity: &planner_workflow::WorkerIdentity,
    continued: u32,
) -> Option<Option<String>> {
    let step = (continued as usize).min(CHILD_CONTINUE_BACKOFF_SECS.len() - 1);
    let until = std::time::Instant::now()
        + std::time::Duration::from_secs(CHILD_CONTINUE_BACKOFF_SECS[step]);
    loop {
        if !child_may_continue(shared, sid, identity) {
            return None;
        }
        if let Some(message) =
            crate::planner_shutdown::take_injection(shared, sid, &identity.work_id)
        {
            return Some(Some(message));
        }
        if std::time::Instant::now() >= until {
            return Some(None);
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// The audit part of the census line every wake carries.
pub fn census(shared: &Shared, run: &PlannerRun) -> String {
    if !audit_configured(shared) {
        return String::new();
    }
    match &run.capacity.audit {
        None => " Audit: not run yet.".into(),
        Some(a) if !a.ok => format!(
            " Audit host UNAVAILABLE ({}): not a clean audit.",
            a.error.as_deref().unwrap_or("no answer")
        ),
        Some(a) if a.defects.is_empty() => " Audit: no defects.".into(),
        Some(a) => format!(
            " Audit defects: {}.",
            a.defects
                .iter()
                .map(|d| d.code.as_str())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// The audit, gate and adoption part of `status`'s capacity block.
pub fn status_fields(shared: &Shared, run: &PlannerRun) -> BTreeMap<&'static str, Value> {
    BTreeMap::from([
        ("audit_configured", json!(audit_configured(shared))),
        ("audit", json!(run.capacity.audit)),
        ("audit_findings", json!(audit_findings(run))),
        ("gate", json!(run.capacity.gate)),
        ("native_adoption", json!(run.capacity.native_adoption)),
    ])
}
