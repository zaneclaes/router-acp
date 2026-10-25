//! Durable planner assignments and revision-bound receipts. MCP callers can
//! report evidence, but cannot manufacture user authorization or change roles.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use agent_client_protocol::schema::v1::{ContentBlock, PromptRequest};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use crate::config::PlannerPhase;
use crate::planner_skills::{PlannerRole, ResolvedPlanner, ResolvedSkill, content_hash};
use crate::session::Shared;

pub const TOOL_NAME: &str = "planner_workflow";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannerRun {
    #[serde(skip)]
    pub revision: u64,
    pub policy: ResolvedPlanner,
    #[serde(default)]
    pub prior_policies: Vec<ResolvedPlanner>,
    pub phase: PlannerPhase,
    pub coordinator: bool,
    pub status: RunStatus,
    pub execution_request: Option<String>,
    #[serde(default)]
    pub execution_input_id: Option<String>,
    pub commands: BTreeSet<String>,
    pub works: BTreeMap<String, Work>,
    #[serde(default)]
    pub releases: BTreeMap<String, crate::planner_release::Release>,
    #[serde(default)]
    pub observations: BTreeMap<String, crate::planner_guard::Observation>,
    #[serde(default)]
    pub completed_guides_at_start: Option<BTreeSet<String>>,
    pub inputs: BTreeMap<String, InputReceipt>,
    #[serde(default)]
    pub parent_queue: Vec<QueuedParentInput>,
    pub wakes: BTreeMap<String, Wake>,
    #[serde(default)]
    pub approval_waits: BTreeSet<String>,
    pub receipts: BTreeMap<String, String>,
    /// Heartbeat wakes sent since a worker last ran; spaces the next one out.
    #[serde(default)]
    pub idle_heartbeats: u32,
    /// Router-enforced worker capacity: host candidates, holds, capacity
    /// wakes, auto-admissions and worker resumptions.
    #[serde(default)]
    pub capacity: crate::planner_capacity::CapacityState,
    /// The router process that owns this run (it has the parent session
    /// loaded). A run whose owner died can be resumed by a new `/implement`
    /// in the same checkout (`crate::planner_adopt`).
    #[serde(default)]
    pub owner: Option<RunOwner>,
    /// The session that adopted this run's works, holds, queue and wakes.
    #[serde(default)]
    pub superseded_by: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOwner {
    pub pid: u32,
    pub started_at_ms: u64,
}

impl RunOwner {
    pub fn current() -> Self {
        let (pid, started_at_ms) = process_identity();
        Self { pid, started_at_ms }
    }

    pub fn alive(&self) -> bool {
        owner_alive(&Attempt {
            id: String::new(),
            router_pid: self.pid,
            router_started_at_ms: self.started_at_ms,
            delegate_id: None,
            ended: false,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunStatus {
    Running,
    Paused,
    Cancelled,
    Complete,
    /// A `_router/planner_shutdown` drain is stopping the run. Nothing is
    /// admitted or resumed; it ends `paused`. A drain cut short by a router
    /// exit stays here, and `/implement` resumes it like `paused`.
    Draining,
    /// Another session adopted this run (`superseded_by`); it continues
    /// there and never resumes here.
    Superseded,
}

impl PlannerRun {
    /// The session is this run's durable parent and keeps the planning pool.
    /// Runs recorded before the parent role was stored are recognized by an
    /// execution request that an implementation command or the accepted-plan
    /// handoff admitted.
    pub fn parent_role(&self) -> bool {
        self.coordinator
            || (self.phase == PlannerPhase::Implementation
                && self.execution_input_id.as_ref().is_some_and(|id| {
                    self.commands.contains(id)
                        || self.inputs.get(id).is_some_and(|input| {
                            input.origin == "human"
                                && parent_handoff(&PromptRequest::new(
                                    id.clone(),
                                    vec![ContentBlock::from(input.text.clone())],
                                ))
                        })
                }))
    }
}

/// A run's stored `draining` that no drain in this router owns: the router
/// that drained it died, so it resumes like `paused`.
fn stale_drain(shared: &Shared, sid: &str, status: RunStatus) -> bool {
    status == RunStatus::Draining && !crate::planner_shutdown::draining(shared, sid)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkSpec {
    pub work_id: String,
    pub plan_id: String,
    pub scope: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
    /// Exact accepted upstream revisions sufficient for source work. These
    /// never satisfy the existing integrated dependencies or release gates.
    #[serde(default)]
    pub ready_dependencies: BTreeMap<String, String>,
    /// Explicit file or directory ownership for coordinated parallel tracks.
    #[serde(default)]
    pub owned_paths: Vec<String>,
    #[serde(default)]
    pub required_checks: Vec<String>,
    #[serde(default)]
    pub required_integration_evidence: Vec<String>,
    /// Repository-defined ticket or artifact identity. The core treats it as
    /// opaque; a host lifecycle gate enforces service-specific constraints.
    pub external_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkStatus {
    Retired,
    Admitted,
    Allocating,
    Running,
    ReviewPending,
    Corrections,
    Accepted,
    Finished,
    Integrated,
    Blocked,
    Watching,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Work {
    #[serde(default)]
    pub reconciliation: Reconciliation,
    pub spec: WorkSpec,
    pub child_id: String,
    pub status: WorkStatus,
    pub workspace: Option<Workspace>,
    pub attempt: Option<Attempt>,
    pub artifact: Option<Artifact>,
    pub review: Option<Review>,
    pub finish: Option<Receipt>,
    pub integration: Option<Receipt>,
    pub reason: Option<String>,
    pub last_output: Option<String>,
    #[serde(default)]
    pub paused: bool,
    /// Unknown for work adopted from older routers; never invent an age.
    #[serde(default)]
    pub ready_at_ms: Option<i64>,
    /// The structured handoff the current attempt recorded (`worker_handoff`),
    /// so its end reads as that handoff rather than as a lost attempt.
    #[serde(default)]
    pub handoff: Option<crate::delegate_hook::Handoff>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Reconciliation {
    pub required: bool,
    pub source_revision: Option<String>,
    pub evidence: Vec<String>,
    pub prior_specs: Vec<WorkSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub path: PathBuf,
    pub lease: String,
    /// Host allocator supplies per-child runtime context, never model prose.
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    /// Client-specific tools rebound by the trusted host allocator.
    #[serde(default)]
    pub mcp_servers: Vec<agent_client_protocol::schema::v1::McpServer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub id: String,
    pub router_pid: u32,
    pub router_started_at_ms: u64,
    pub delegate_id: Option<String>,
    pub ended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub revision: String,
    pub changed_paths: Vec<String>,
    pub checks: BTreeMap<String, String>,
    pub evidence: Vec<String>,
    #[serde(default)]
    pub unresolved: Vec<String>,
    /// Proof recipes owned by integration, rather than worker acceptance.
    #[serde(default)]
    pub integration_requirements: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Review {
    pub revision: String,
    pub accepted: bool,
    pub evidence: Vec<String>,
    pub corrections: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub revision: String,
    pub evidence: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputReceipt {
    pub origin: String,
    /// Earlier-session input delivered again solely to reconcile its receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replayed_from: Option<String>,
    pub text: String,
    pub attachments: Vec<Value>,
    pub owner: Option<String>,
    pub acknowledged: bool,
    #[serde(default)]
    pub delivered_to: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputWithdrawal {
    pub at_ms: i64,
    pub reason: String,
}

impl InputReceipt {
    pub fn resolved(&self) -> bool {
        self.acknowledged
    }
}

/// Explicit reconciliation closes both records in one revision. Delivery
/// alone is never acknowledgment, nor proof that the requested work succeeded.
fn acknowledge_input(
    run: &mut PlannerRun,
    sid: &str,
    id: &str,
    owner: Option<String>,
) -> Result<(), String> {
    if owner
        .as_ref()
        .is_some_and(|owner| !run.works.contains_key(owner))
    {
        return Err("input owner is not an admitted work identity".into());
    }
    if crate::planner_input::withdrawal(run, id).is_some() {
        return Ok(()); // already explicitly reconciled by withdrawal
    }
    let input = run.inputs.get_mut(id).ok_or("unknown input receipt")?;
    if input.owner.is_some() && input.owner != owner {
        return Err("input correction must retain its original work owner".into());
    }
    if !input.delivered_to.contains(owner.as_deref().unwrap_or(sid)) {
        return Err(
            "original input and attachments have no delivery receipt for this owner".into(),
        );
    }
    input.owner = owner;
    input.acknowledged = true;
    if let Some(wake) = run.wakes.get_mut(&format!("input:{id}")) {
        wake.acknowledged = true;
    }
    run.parent_queue.retain(|queued| queued.input_id != id);
    Ok(())
}

/// Direct/steered input used to remain unacknowledged silently until adoption
/// recreated it as fresh work. Demand reconciliation at the ordinary turn end.
pub fn reconcile_delivered_inputs(shared: &Shared, sid: &str) -> Result<(), String> {
    if load(shared, sid)?.is_none() {
        return Ok(());
    }
    mutate(shared, sid, |run| {
        if !matches!(run.status, RunStatus::Running | RunStatus::Draining) { return Ok(()) }
        for (id, input) in &run.inputs {
            if run.commands.contains(id) || input.resolved() || input.owner.is_some() || !input.delivered_to.contains(sid) { continue }
            let wake = run.wakes.entry(format!("input:{id}")).or_default();
            // Keep a live delivery's claim and retry/backoff. A separate wake
            // acknowledgment cannot make an unresolved input disappear.
            wake.work_id = sid.into();
            wake.acknowledged = false;
            wake.reason = format!("Input {id} (origin: {}) was delivered but your turn ended without its acknowledgment. Reconcile the action already taken, record acknowledge-input, and continue useful work. Do not repeat a completed action; acknowledgment records handling, not a release or completion verdict.", input.origin);
        }
        Ok(())
    }).map(|_| ())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedParentInput {
    pub input_id: String,
    pub request: PromptRequest,
}

#[derive(Clone)]
pub struct PendingDelivery {
    pub parent_sid: String,
    pub owner: String,
    pub input_ids: Vec<String>,
    pub persistence_failed: bool,
}

pub fn queue_delivery(
    shared: &Shared,
    state_sid: &str,
    parent_sid: &str,
    owner: &str,
    ids: Vec<String>,
) {
    if !ids.is_empty() && load(shared, parent_sid).ok().flatten().is_some() {
        shared.planner_deliveries.lock().unwrap().insert(
            state_sid.into(),
            PendingDelivery {
                parent_sid: parent_sid.into(),
                owner: owner.into(),
                input_ids: ids,
                persistence_failed: false,
            },
        );
    }
}

pub fn confirm_delivery(shared: &Shared, state_sid: &str) -> Result<(), String> {
    let delivery = {
        let mut deliveries = shared.planner_deliveries.lock().unwrap();
        if deliveries
            .get(state_sid)
            .is_some_and(|delivery| delivery.persistence_failed)
        {
            return Ok(());
        }
        deliveries.remove(state_sid)
    };
    if let Some(delivery) = delivery {
        for id in &delivery.input_ids {
            if let Err(error) =
                mark_input_delivered(shared, &delivery.parent_sid, id, &delivery.owner)
            {
                shared
                    .planner_deliveries
                    .lock()
                    .unwrap()
                    .insert(state_sid.into(), delivery);
                return Err(error);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Wake {
    pub reason: String,
    pub work_id: String,
    pub acknowledged: bool,
    #[serde(default)]
    pub deliveries: u32,
    #[serde(default)]
    pub retry_after: i64,
    #[serde(default)]
    pub claim_pid: u32,
    #[serde(default)]
    pub claim_started_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Operation {
    Status {
        #[serde(default)]
        offset: usize,
        #[serde(default)]
        work_id: Option<String>,
    },
    Admit {
        key: String,
        work: WorkSpec,
    },
    /// Explicit parent handoff of the current scope and preserved source after recovery.
    Reconcile {
        key: String,
        work: WorkSpec,
        source_revision: String,
        evidence: Vec<String>,
    },
    Retire {
        key: String,
        work_id: String,
        reason: String,
        evidence: BTreeMap<String, String>,
    },
    Artifact {
        key: String,
        work_id: String,
        attempt_id: String,
        artifact: Artifact,
    },
    Review {
        key: String,
        work_id: String,
        revision: String,
        accepted: bool,
        evidence: Vec<String>,
        corrections: Option<String>,
    },
    Finished {
        key: String,
        work_id: String,
        attempt_id: String,
        receipt: Receipt,
    },
    /// Parent records a mechanical handoff only when the trusted repository
    /// host verifies custody and closed resources. Review stays independent.
    Finalize {
        key: String,
        work_id: String,
        receipt: Receipt,
    },
    Integrate {
        key: String,
        work_id: String,
        receipt: Receipt,
    },
    Release {
        key: String,
        release_id: String,
        checkpoint: crate::planner_release::Checkpoint,
    },
    Disposition {
        key: String,
        work_id: String,
        status: WorkStatus,
        reason: String,
    },
    AcknowledgeInput {
        key: String,
        input_id: String,
        owner: Option<String>,
    },
    AcknowledgeWake {
        key: String,
        wake_id: String,
    },
    /// Keep one capacity-host candidate unadmitted for 30 minutes. The only
    /// way a candidate may stay out of a free slot.
    Hold {
        key: String,
        plan_id: String,
        reason: String,
        detail: String,
    },
    Complete {
        key: String,
        queue_evidence: String,
    },
}

#[derive(Debug, Clone)]
pub struct WorkerIdentity {
    pub work_id: String,
    pub attempt_id: String,
}

pub fn load(shared: &Shared, sid: &str) -> Result<Option<PlannerRun>, String> {
    shared
        .state
        .lock()
        .unwrap()
        .planner_run(sid)?
        .map(|(revision, value)| {
            let mut run: PlannerRun = serde_json::from_value(value).map_err(|e| e.to_string())?;
            run.revision = revision;
            Ok(run)
        })
        .transpose()
}

pub fn save(shared: &Shared, sid: &str, run: &mut PlannerRun) -> Result<(), String> {
    let state = shared.state.lock().unwrap();
    let previous = state.planner_run(sid)?;
    let mut events = Vec::new();
    if shared.cfg.delegation.lifecycle_hook.is_some() {
        for (wake_id, wake) in &run.wakes {
            if previous
                .as_ref()
                .and_then(|(_, v)| v.get("wakes"))
                .and_then(|w| w.get(wake_id))
                .is_none()
            {
                events.push(wake_event(sid, run, wake_id, wake).to_string());
            }
        }
        // A work that reached `integrated` in this save: the router no longer
        // needs its workspace, so the host may give it back.
        let previous_works = previous.as_ref().and_then(|(_, v)| v.get("works"));
        for (work_id, work) in &run.works {
            let was_integrated = previous_works
                .and_then(|w| w.get(work_id))
                .and_then(|w| w.get("status"))
                .and_then(Value::as_str)
                == Some("integrated");
            if work.status == WorkStatus::Integrated && !was_integrated {
                events.push(work_finished_event(sid, work_id, work).to_string());
            }
        }
    }
    run.revision = state.save_planner_run_with_wakes(
        sid,
        run.revision,
        &serde_json::to_value(&run).map_err(|e| e.to_string())?,
        &events,
    )?;
    Ok(())
}

fn wake_event(sid: &str, run: &PlannerRun, wake_id: &str, wake: &Wake) -> Value {
    let (pid, started) = process_identity();
    json!({"event":"planner_wake","wake_id":wake_id,"parent_router_session_id":sid,
        "work_id":wake.work_id,"reason":wake.reason,"revision":run.revision+1,
        "attempt_id":run.works.get(&wake.work_id).and_then(|w| w.attempt.as_ref()).map(|a| &a.id),
        "router_pid":pid,"router_started_at_ms":started})
}

/// `work_finished`: a work reached its terminal `integrated` state (only then:
/// `integrate` still verifies the workspace HEAD, so a `finished` work keeps
/// it). Sent once per transition, through the same outbox as `planner_wake`,
/// so a host can retire the workspace. `worker_id` is the work's child id
/// (what the host's allocation lease records) and `cwd` its workspace path.
pub fn work_finished_event(sid: &str, work_id: &str, work: &Work) -> Value {
    let (pid, started) = process_identity();
    json!({"event":"work_finished","work_id":work_id,"plan_id":work.spec.plan_id,
        "external_id":work.spec.external_id,"status":work.status,
        "parent_router_session_id":sid,"worker_id":work.child_id,"child_id":work.child_id,
        "cwd":work.workspace.as_ref().map(|w| w.path.display().to_string()),
        "lease":work.workspace.as_ref().map(|w| &w.lease),
        "attempt_id":work.attempt.as_ref().map(|a| &a.id),
        "attempt_ended":work.attempt.as_ref().is_none_or(|a| a.ended),
        "revision":work.integration.as_ref().map(|r| &r.revision),
        "router_pid":pid,"router_started_at_ms":started})
}

fn concurrent(error: &str) -> bool {
    error.starts_with("planner state changed concurrently")
}

/// Reapply a typed synchronous mutation against the latest revision. A
/// conflict must never discard user input or the only child-stop receipt.
pub fn mutate(
    shared: &Shared,
    sid: &str,
    mut apply: impl FnMut(&mut PlannerRun) -> Result<(), String>,
) -> Result<PlannerRun, String> {
    for _ in 0..16 {
        let mut run = load(shared, sid)?.ok_or("planner state missing")?;
        apply(&mut run)?;
        match save(shared, sid, &mut run) {
            Ok(()) => return Ok(run),
            Err(e) if concurrent(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    Err("planner mutation remained contended after bounded reconciliation; retry this same operation".into())
}

pub fn ensure(shared: &Shared, sid: &str) -> Result<PlannerRun, String> {
    if let Some(run) = load(shared, sid)? {
        return Ok(run);
    }
    let (cwd, coordinator) = shared
        .with_session(sid, |s| (s.cwd.clone(), s.coordinator))
        .ok_or("planner session no longer exists")?;
    let mut run = PlannerRun {
        revision: 0,
        policy: crate::planner_skills::resolve(&shared.cfg.routers.planner, &cwd)?,
        prior_policies: Vec::new(),
        phase: PlannerPhase::Planning,
        coordinator,
        status: RunStatus::Running,
        execution_request: None,
        execution_input_id: None,
        commands: BTreeSet::new(),
        works: BTreeMap::new(),
        releases: BTreeMap::new(),
        observations: BTreeMap::new(),
        completed_guides_at_start: None,
        inputs: BTreeMap::new(),
        parent_queue: Vec::new(),
        wakes: BTreeMap::new(),
        approval_waits: BTreeSet::new(),
        receipts: BTreeMap::new(),
        idle_heartbeats: 0,
        capacity: Default::default(),
        owner: Some(RunOwner::current()),
        superseded_by: None,
    };
    if let Err(e) = save(shared, sid, &mut run) {
        if concurrent(&e) {
            return load(shared, sid)?.ok_or(e);
        }
        return Err(e);
    }
    Ok(run)
}

/// ACP prompt requests originate at the client. Clients sending model or
/// automatic input must mark it, and internal role invocations are excluded.
pub fn human_prompt(req: &PromptRequest) -> bool {
    let meta = req.meta.as_ref().and_then(|m| m.get("router_acp"));
    !meta.is_some_and(|m| {
        m.get("planner_role").is_some()
            || m.get("agent_origin").and_then(Value::as_bool) == Some(true)
            || m.get("origin")
                .and_then(Value::as_str)
                .is_some_and(|s| s != "human" && s != "user")
    })
}

pub fn command(req: &PromptRequest) -> Option<PlannerPhase> {
    leading_command(req).map(|c| c.phase)
}

/// A slash `/plan` or `/implement` (or a client's expansion of one) opts a
/// session on any strategy into the planner workflow. Prose such as
/// "implement the login fix" is never a command: only the typed slash
/// command states the intent.
pub fn explicit_command(req: &PromptRequest) -> bool {
    leading_command(req).is_some()
}

/// The operator's leading mode command, the text block that carries it, and
/// its arguments.
struct LeadingCommand {
    phase: PlannerPhase,
    block: usize,
    args: String,
}

/// goose adds a `<turn-context>` span (its own block or inside the typed one),
/// and when a typed `/plan` or `/implement` names a repository skill it sends
/// that skill's expansion in its place: `# Loaded Skill: implement (skill)`,
/// the skill text, then `ARGUMENTS: <typed arguments>`. Both are the same
/// authentic leading command.
fn leading_command(req: &PromptRequest) -> Option<LeadingCommand> {
    if !human_prompt(req) {
        return None;
    }
    let (block, text) = req.prompt.iter().enumerate().find_map(|(i, b)| match b {
        ContentBlock::Text(t) => {
            let typed = crate::accounts::without_turn_context(&t.text);
            (!typed.trim().is_empty()).then_some(Some((i, typed)))
        }
        _ => Some(None),
    })??;
    let typed = text.trim_start();
    if let Some(loaded) = typed.strip_prefix("# Loaded Skill: ") {
        let name = loaded.split_whitespace().next()?;
        if !loaded[name.len()..].trim_start().starts_with("(skill)") {
            return None;
        }
        let phase = match name {
            "plan" => PlannerPhase::Planning,
            "implement" => PlannerPhase::Implementation,
            _ => return None,
        };
        let content = typed
            .rsplit_once("\n## Supporting Files")
            .map_or(typed, |(c, _)| c);
        let args = content
            .rsplit_once("\nARGUMENTS:")
            .map(|(_, a)| a.lines().next().unwrap_or("").trim().to_string())
            .unwrap_or_default();
        return Some(LeadingCommand { phase, block, args });
    }
    let (verb, args) = typed
        .split_once(char::is_whitespace)
        .map_or((typed, ""), |(v, a)| (v, a.trim_start()));
    let phase = match verb {
        "/plan" => PlannerPhase::Planning,
        "/implement" => PlannerPhase::Implementation,
        _ => return None,
    };
    Some(LeadingCommand {
        phase,
        block,
        args: args.trim_end().to_string(),
    })
}

/// Run before ticket/skill expansion. The original text and attachments stay
/// in the input receipt. The delivered command becomes an internal role
/// invocation so a provider cannot recursively execute the slash command.
pub fn user_command(
    shared: &Arc<Shared>,
    sid: &str,
    req: &mut PromptRequest,
) -> Result<Option<PlannerPhase>, String> {
    let original = req.clone();
    user_command_from_original(shared, sid, &original, req, false)
}

pub(crate) fn user_command_from_original(
    shared: &Arc<Shared>,
    sid: &str,
    original: &PromptRequest,
    req: &mut PromptRequest,
    queued: bool,
) -> Result<Option<PlannerPhase>, String> {
    if command(req).is_some() && prompt_input_id(req).is_none() {
        let meta = req.meta.get_or_insert_with(Default::default);
        meta.entry("router_acp").or_insert_with(|| json!({}))["input_id"] =
            json!(uuid::Uuid::new_v4().to_string());
    }
    let mut original = original.clone();
    if prompt_input_id(&original).is_none() {
        original.meta = req.meta.clone();
    }
    for _ in 0..16 {
        let mut shaped = req.clone();
        match user_command_once(shared, sid, &original, &mut shaped, queued) {
            Err(e) if concurrent(&e) => continue,
            Ok(result) => {
                *req = shaped;
                return Ok(result);
            }
            Err(e) => return Err(e),
        }
    }
    Err("planner command remained contended; retry the same input identity".into())
}

fn user_command_once(
    shared: &Arc<Shared>,
    sid: &str,
    original: &PromptRequest,
    req: &mut PromptRequest,
    queued: bool,
) -> Result<Option<PlannerPhase>, String> {
    let Some(LeadingCommand { phase, block, args }) = leading_command(req) else {
        return Ok(None);
    };
    let mut run = ensure(shared, sid)?;
    if run.status == RunStatus::Superseded {
        return Err(format!(
            "this planner run was resumed by session {}; continue it there",
            run.superseded_by.as_deref().unwrap_or("(unknown)")
        ));
    }
    if run.status == RunStatus::Draining && crate::planner_shutdown::draining(shared, sid) {
        return Err(
            "the planner run is shutting down; once its drain reports `drained`, /implement \
             resumes it"
                .into(),
        );
    }
    let input_id = record_input(&mut run, original)?;
    let first_admission = run.commands.insert(input_id.clone());
    if !first_admission && !queued {
        return Err(format!(
            "planner command {input_id} was already admitted; resume the recorded run instead of replaying it"
        ));
    }
    let resuming = matches!(
        run.status,
        RunStatus::Paused | RunStatus::Cancelled | RunStatus::Draining
    );
    // Ordinary load/resume retains the immutable policy snapshot. A new
    // explicit implementation command after a completed drain is the safe
    // boundary for installing revised repository skills, with old policy
    // retained for audit and all work/review/release records preserved.
    if first_admission
        && !queued
        && resuming
        && phase == PlannerPhase::Implementation
        && run
            .works
            .values()
            .all(|work| work.attempt.as_ref().is_none_or(|attempt| attempt.ended))
    {
        let cwd = shared
            .with_session(sid, |s| s.cwd.clone())
            .ok_or("planner session missing")?;
        let policy = crate::planner_skills::resolve(&shared.cfg.routers.planner, &cwd)?;
        if policy.identity != run.policy.identity {
            run.prior_policies
                .push(std::mem::replace(&mut run.policy, policy));
        }
        for work in run.works.values_mut() {
            mark_reconciliation_required(work);
        }
    }
    run.phase = phase;
    // An implementation command makes this session the run's durable parent.
    // The parent selects, assigns and reviews work, so its model pool is the
    // planning pool whatever the workflow phase; children keep theirs.
    run.coordinator |= phase == PlannerPhase::Implementation
        || shared.with_session(sid, |s| s.coordinator).unwrap_or(false);
    if phase == PlannerPhase::Implementation || run.status == RunStatus::Complete {
        if resuming {
            run.approval_waits.clear();
        }
        run.status = RunStatus::Running;
    }
    if first_admission && !queued {
        for wake in run.wakes.values_mut().filter(|w| !w.acknowledged) {
            wake.deliveries = 0;
            wake.retry_after = 0;
        }
    }
    if phase == PlannerPhase::Implementation {
        run.execution_request = Some(display_text(original));
        run.execution_input_id = Some(input_id.clone());
    }
    let role = crate::planner_wake::parent_role(&run, true, &[]);
    let entrypoint_name = match phase {
        PlannerPhase::Planning => "plan",
        PlannerPhase::Implementation => "implement",
    };
    let cwd = shared
        .with_session(sid, |s| s.cwd.clone())
        .ok_or("planner session missing")?;
    let entrypoint = crate::planner_skills::repository_skill(&cwd, entrypoint_name)?;
    let mut instructions = run.policy.instructions(role);
    // The entrypoint is an explicit invocation, not an additional role lookup.
    // Mapped identical files execute once under their owning role.
    if let Some(skill) = entrypoint
        && !run.policy.roles.values().any(|s| s.source == skill.source)
    {
        if phase == PlannerPhase::Planning {
            instructions.push_str(&format!(
                "\n[Explicit repository entrypoint: {}]\n{}",
                skill.name, skill.text
            ));
        } else {
            instructions.push_str("\nThe explicit repository implementation entrypoint is retained for worker assignments. Continue with the parent role above.");
            run.policy_entrypoint(&input_id, &skill);
        }
    }
    if let Some(roadmap) = &run.policy.roadmap {
        instructions.push_str(&format!("\nConfigured roadmap: {}", roadmap.display()));
    }
    // The command's own block becomes the role invocation, so a client's
    // expanded skill text is not sent to the provider a second time.
    if let Some(ContentBlock::Text(first)) = req.prompt.get_mut(block) {
        first.text = format!(
            "{instructions}\n[Original user command: {entrypoint_name}, input: {input_id}]\nArguments: {args}"
        );
    }
    let meta = req.meta.get_or_insert_with(Default::default);
    let router_meta = meta.entry("router_acp").or_insert_with(|| json!({}));
    router_meta["planner_role"] = json!(role);
    router_meta["planner_input_id"] = json!(input_id);
    save(shared, sid, &mut run)?;
    shared.with_session(sid, |s| s.coordinator |= run.coordinator);
    let typed = format!("{entrypoint_name} {args}");
    shared.with_session(sid, |s| {
        s.planner_command_text = Some(typed.trim_end().to_string());
        s.planner_explicit_phase = Some(phase);
    });
    Ok(Some(phase))
}

fn display_text(req: &PromptRequest) -> String {
    req.prompt
        .iter()
        .filter_map(|b| {
            if let ContentBlock::Text(t) = b {
                Some(t.text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn record_input(run: &mut PlannerRun, req: &PromptRequest) -> Result<String, String> {
    let id = req
        .meta
        .as_ref()
        .and_then(|m| m.get("router_acp"))
        .and_then(|m| m.get("input_id"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let receipt = InputReceipt {
        origin: if human_prompt(req) {
            "human"
        } else if delegated_approval(req) {
            "coordinator"
        } else {
            "agent"
        }
        .into(),
        replayed_from: None,
        text: display_text(req),
        attachments: req
            .prompt
            .iter()
            .filter(|b| !matches!(b, ContentBlock::Text(_)))
            .filter_map(|b| serde_json::to_value(b).ok())
            .collect(),
        owner: None,
        acknowledged: false,
        delivered_to: BTreeSet::new(),
    };
    if let Some(previous) = run.inputs.get(&id) {
        if previous.text != receipt.text
            || previous.attachments != receipt.attachments
            || previous.origin != receipt.origin
        {
            return Err("input identity already names a different original message".into());
        }
    } else {
        run.inputs.insert(id.clone(), receipt);
    }
    Ok(id)
}

impl PlannerRun {
    fn policy_entrypoint(&mut self, input_id: &str, skill: &ResolvedSkill) {
        // Persist user-invoked implementation guidance with its original input,
        // independently of the exact-name role resolution.
        self.receipts.insert(
            format!("entrypoint:{input_id}"),
            serde_json::to_string(skill).expect("resolved skill serializes"),
        );
    }
}

/// A human `[router: phase=…]` directive is the same explicit phase choice as
/// `/plan` or `/implement`. Kory Code's accepted-plan handoff sends one. A
/// client-attested delegated approval carries the same authority for the
/// implementation phase only.
fn directed_phase(req: &PromptRequest) -> Option<PlannerPhase> {
    let human = human_prompt(req);
    if !human && !delegated_approval(req) {
        return None;
    }
    crate::session::parse_prompt_directives(&req.prompt)
        .ok()
        .flatten()
        .and_then(|(directives, _)| directives.phase)
        .filter(|phase| human || *phase == PlannerPhase::Implementation)
}

/// A coordinator's plan approval for its checkpointed child, attested by the
/// client under a human coordinator grant
/// (`_meta.router_acp.delegated_approval.by == "coordinator"`). The prompt is
/// agent-origin, but its `[router: phase=implementation]` directive is the
/// authentic implementation request the child's run needs to admit work.
fn delegated_approval(req: &PromptRequest) -> bool {
    req.meta
        .as_ref()
        .and_then(|m| m.get("router_acp"))
        .and_then(|m| m.get("delegated_approval"))
        .and_then(|a| a.get("by"))
        .and_then(Value::as_str)
        == Some("coordinator")
}

/// Kory Code's accepted-plan handoff ("Proceed with implementation" and
/// Create EPIC): a human `[router: phase=implementation]` directive followed
/// by the `[planner-handoff]` tag. The session it reaches admits the work and
/// delegates children, so it becomes the run's durable parent.
pub(crate) fn parent_handoff(req: &PromptRequest) -> bool {
    // A delegated approval reaches a child; only a person's handoff makes a parent.
    if !human_prompt(req) || directed_phase(req) != Some(PlannerPhase::Implementation) {
        return false;
    }
    let Ok(Some((_, stripped))) = crate::session::parse_prompt_directives(&req.prompt) else {
        return false;
    };
    stripped
        .iter()
        .find_map(|b| match b {
            ContentBlock::Text(t) => {
                let typed = crate::accounts::without_turn_context(&t.text);
                (!typed.trim().is_empty()).then(|| {
                    typed
                        .trim_start()
                        .get(..PLANNER_HANDOFF_TAG.len())
                        .is_some_and(|tag| tag.eq_ignore_ascii_case(PLANNER_HANDOFF_TAG))
                })
            }
            _ => Some(false),
        })
        .unwrap_or(false)
}

const PLANNER_HANDOFF_TAG: &str = "[planner-handoff]";

pub fn observe_input(shared: &Arc<Shared>, sid: &str, req: &PromptRequest) -> Result<(), String> {
    if load(shared, sid)?.is_none() {
        return Ok(());
    }
    let directed = directed_phase(req);
    let human = human_prompt(req);
    // An explicit phase routes from that phase's pool only, as the command does.
    if let Some(phase) = directed {
        shared.with_session(sid, |s| s.planner_explicit_phase = Some(phase));
    }
    mutate(shared, sid, |run| {
        if run.status == RunStatus::Complete && !human {
            return Err("planner run is complete; use an explicit /plan or /implement command to start new scope".into());
        }
        // A person's follow-up continues a finished or paused run in its last
        // phase: a question or an interrupted turn never ends the run. A live
        // drain keeps its run until it is drained.
        if human {
            crate::planner_capacity::clear_human_hold(run);
        }
        if human
            && (matches!(run.status, RunStatus::Complete | RunStatus::Paused)
                || stale_drain(shared, sid, run.status))
        {
            run.approval_waits.clear();
            run.status = RunStatus::Running;
        }
        let input_id = if req.meta.as_ref().and_then(|m| m.get("router_acp")).and_then(|m| m.get("planner_input_id")).is_none() {
            Some(record_input(run, req)?)
        } else {
            prompt_input_id(req).map(str::to_string)
        };
        // Mirrors an explicit command: the run follows the human's phase, and
        // an implementation choice resumes a stopped run with the authentic
        // request work admission needs.
        if let Some(phase) = directed
            && !(run.status == RunStatus::Draining && crate::planner_shutdown::draining(shared, sid))
        {
            run.phase = phase;
            if phase == PlannerPhase::Implementation {
                if matches!(
                    run.status,
                    RunStatus::Paused | RunStatus::Cancelled | RunStatus::Draining
                ) {
                    run.approval_waits.clear();
                }
                run.status = RunStatus::Running;
                run.execution_request = Some(display_text(req));
                run.execution_input_id = input_id;
                run.coordinator |= parent_handoff(req);
            }
        }
        if let Some(wake) = req.meta.as_ref().and_then(|m| m.get("router_acp")).and_then(|m| m.get("planner_wake_ack")).and_then(Value::as_str) {
            run.wakes.get_mut(wake).ok_or("unknown planner wake acknowledgement")?.acknowledged = true;
        }
        Ok(())
    }).map(|_| ())
}

pub(crate) fn enqueue_parent_input(
    shared: &Arc<Shared>,
    sid: &str,
    req: &PromptRequest,
) -> Result<(), String> {
    // Validate before retaining the request, but do not apply its controls to
    // the active turn. The original request is the durable FIFO queue entry.
    let parsed = crate::session::parse_prompt_directives(&req.prompt)?;
    let mut command_request = req.clone();
    if let Some((_, stripped)) = parsed {
        command_request.prompt = stripped;
    }
    let phase = command(&command_request);
    ensure(shared, sid)?;
    mutate(shared, sid, |run| {
        if run.status == RunStatus::Complete {
            if phase.is_none() && !human_prompt(req) {
                return Err(
                    "planner run is complete; use /plan or /implement for new scope".into(),
                );
            }
            run.status = RunStatus::Running;
        }
        queue_parent_request(run, sid, req)?;
        // A return to planning stops new child dispatch immediately. Model,
        // effort, and command execution wait for this input's own turn.
        if phase == Some(PlannerPhase::Planning) {
            run.phase = PlannerPhase::Planning;
        }
        Ok(())
    })
    .map(|_| ())
}

/// Shared durable queue primitive for ACP input and the local monitor CLI.
pub(crate) fn queue_parent_request(
    run: &mut PlannerRun,
    sid: &str,
    req: &PromptRequest,
) -> Result<String, String> {
    let input_id = record_input(run, req)?;
    if run.inputs[&input_id].resolved() {
        return Ok(input_id);
    }
    if !run
        .parent_queue
        .iter()
        .any(|input| input.input_id == input_id)
    {
        run.parent_queue.push(QueuedParentInput {
            input_id: input_id.clone(),
            request: req.clone(),
        });
    }
    run.wakes
        .entry(format!("input:{input_id}"))
        .or_insert(Wake {
            reason: "Original parent input awaits its own turn".into(),
            work_id: sid.into(),
            ..Default::default()
        });
    Ok(input_id)
}

/// Operator input steered into a running parent turn: its receipt is
/// recorded as delivered to the parent, so the parent acknowledges it like
/// any other input. A session without a planner run records nothing.
pub fn record_steered_input(
    shared: &Arc<Shared>,
    sid: &str,
    req: &PromptRequest,
) -> Result<(), String> {
    if load(shared, sid)?.is_none() {
        return Ok(());
    }
    mutate(shared, sid, |run| {
        let input_id = record_input(run, req)?;
        if let Some(input) = run.inputs.get_mut(&input_id) {
            input.delivered_to.insert(sid.to_string());
        }
        Ok(())
    })
    .map(|_| ())
}

pub fn mark_input_delivered(
    shared: &Shared,
    sid: &str,
    input_id: &str,
    owner: &str,
) -> Result<(), String> {
    mutate(shared, sid, |run| {
        let input = run
            .inputs
            .get_mut(input_id)
            .ok_or("unknown input receipt")?;
        if owner != sid {
            if input
                .owner
                .as_deref()
                .is_some_and(|previous| previous != owner)
            {
                return Err("input correction must retain its original owner".into());
            }
            if !run.commands.contains(input_id) {
                input.owner = Some(owner.to_string());
            }
        }
        input.delivered_to.insert(owner.to_string());
        Ok(())
    })
    .map(|_| ())
}

pub fn prompt_input_id(req: &PromptRequest) -> Option<&str> {
    let meta = req.meta.as_ref()?.get("router_acp")?;
    meta.get("planner_input_id")
        .or_else(|| meta.get("input_id"))?
        .as_str()
}

/// Persistent provenance for an admitted run, independent of paginated inputs
/// and receipt acknowledgment. This reports the human request, not a new grant:
/// later restrictions and the run's normal holds still apply.
pub(crate) fn execution_context(run: &PlannerRun) -> Option<Value> {
    let id = run.execution_input_id.as_ref()?;
    let input = run.inputs.get(id)?;
    if input.origin != "human" || crate::planner_input::withdrawal(run, id).is_some() {
        return None;
    }
    let request = PromptRequest::new(id.clone(), vec![ContentBlock::from(input.text.clone())]);
    if let Some(command) = leading_command(&request)
        && command.phase == PlannerPhase::Implementation
        && run.commands.contains(id)
    {
        // Keep typed scope, without replaying an entire expanded skill on each wake.
        return Some(json!({"input_id":id,"origin":input.origin,
            "command":"implement","arguments":command.args}));
    }
    if parent_handoff(&request) {
        return Some(json!({"input_id":id,"origin":input.origin,
            "request":input.text}));
    }
    None
}

pub fn parent_input_blocks(
    shared: &Shared,
    sid: &str,
    ids: &[String],
) -> Result<Vec<ContentBlock>, String> {
    let run = load(shared, sid)?.ok_or("planner state missing")?;
    let mut blocks = Vec::new();
    for id in ids {
        let input = run.inputs.get(id).ok_or("unknown original input receipt")?;
        if input.resolved() {
            continue;
        }
        let header = if let Some(source) = &input.replayed_from {
            format!(
                "[HISTORICAL input: {id}, origin: {}, replayed_from: {source}]\nHISTORICAL: the earlier parent already received and acted on this; acknowledge it, do not answer or act on it again. This is router receipt reconciliation, not new operator input.",
                input.origin
            )
        } else {
            format!("[Original queued input: {id}, origin: {}]", input.origin)
        };
        blocks.push(ContentBlock::from(format!("{header}\n{}", input.text)));
        for attachment in &input.attachments {
            blocks.push(
                serde_json::from_value(attachment.clone())
                    .map_err(|e| format!("invalid original attachment: {e}"))?,
            );
        }
    }
    Ok(blocks)
}

/// Deliver stored original input, including attachments, rather than a
/// model's paraphrase. Corrections keep their first assigned owner.
pub fn input_blocks(
    shared: &Shared,
    sid: &str,
    actor: &WorkerIdentity,
    ids: &[String],
) -> Result<(Vec<ContentBlock>, Vec<String>), String> {
    let run = load(shared, sid)?.ok_or("planner state missing")?;
    let ids = if ids.is_empty() {
        run.execution_input_id.iter().cloned().collect::<Vec<_>>()
    } else {
        ids.to_vec()
    };
    let mut blocks = Vec::new();
    let mut delivered = Vec::new();
    for id in ids {
        let input = run
            .inputs
            .get(&id)
            .ok_or("unknown original input receipt")?;
        if crate::planner_input::withdrawal(&run, &id).is_some() {
            continue;
        }
        if input
            .owner
            .as_deref()
            .is_some_and(|owner| owner != actor.work_id)
        {
            return Err("input correction must retain its original work owner".into());
        }
        if input.delivered_to.contains(&actor.work_id) {
            continue;
        }
        if !run.commands.contains(&id) {
            mutate(shared, sid, |run| {
                let input = run.inputs.get_mut(&id).ok_or("input receipt disappeared")?;
                if input
                    .owner
                    .as_deref()
                    .is_some_and(|owner| owner != actor.work_id)
                {
                    return Err("input correction must retain its original work owner".into());
                }
                input.owner = Some(actor.work_id.clone());
                Ok(())
            })?;
        }
        blocks.push(ContentBlock::from(format!(
            "[Original input: {id}, origin: {}]\n{}",
            input.origin, input.text
        )));
        for attachment in &input.attachments {
            blocks.push(
                serde_json::from_value(attachment.clone())
                    .map_err(|e| format!("invalid original attachment: {e}"))?,
            );
        }
        delivered.push(id);
    }
    Ok((blocks, delivered))
}

pub fn tool_definition() -> Value {
    json!({ "name": TOOL_NAME, "description": "Record durable planner work and revision-bound evidence. Parent actions include review, host-verified finalize, release checkpoints, integrate, disposition and complete. Workers report artifact/finished only for their assignment. Status accepts work_id for a compact targeted view. WorkSpec owned_paths declares literal source ownership; ready_dependencies binds reviewed source SHAs, while dependencies requires integration. Artifact integration_requirements maps release-owned proof keys to recipes, all required at integration. Reconcile suspended recovery before retrying. Fill free slots with admissible independent work or record a supported hold; when the human has not authorized the work, hold `awaiting-human` with their quoted words instead of starting it. Every mutation requires a unique idempotency key. This tool never grants merge/deploy authority.",
        "inputSchema": { "type": "object", "properties": {
            "action": {"type":"string", "enum":["status","admit","reconcile","retire","artifact","review","finished","finalize","integrate","release","disposition","acknowledge-input","acknowledge-wake","hold","complete"]},
            "source_revision":{"type":"string", "description":"Full preserved source commit, already carried in the assigned worktree HEAD. Reconcile requires the complete current WorkSpec and parent evidence; it never expands operator authorization."},
            "release_id":{"type":"string"}, "checkpoint":{"type":"object"},
            "plan_id":{"type":"string"}, "detail":{"type":"string"},
            "key":{"type":"string"}, "work":{"type":"object"}, "work_id":{"type":"string"}, "attempt_id":{"type":"string"},
            "artifact":{"type":"object"}, "revision":{"type":"string"}, "accepted":{"type":"boolean"}, "evidence":{"oneOf":[{"type":"array","items":{"type":"string"}},{"type":"object","additionalProperties":{"type":"string"}}]},
            "corrections":{"type":["string","null"]}, "receipt":{"type":"object"}, "status":{"type":"string"}, "reason":{"type":"string"},
            "input_id":{"type":"string"}, "owner":{"type":["string","null"]}, "wake_id":{"type":"string"}, "queue_evidence":{"type":"string"}, "offset":{"type":"integer","minimum":0}
        }, "required":["action"] } })
}

fn require_worker<'a>(
    run: &'a mut PlannerRun,
    work_id: &str,
    attempt_id: &str,
    actor: Option<&WorkerIdentity>,
) -> Result<&'a mut Work, String> {
    let actor =
        actor.ok_or("only the assigned child can report its artifact or finishing receipt")?;
    if actor.work_id != work_id || actor.attempt_id != attempt_id {
        return Err("worker identity does not match the assignment attempt".into());
    }
    let work = run.works.get_mut(work_id).ok_or("unknown work identity")?;
    let attempt = work.attempt.as_ref().ok_or("work has no active attempt")?;
    if attempt.id != attempt_id
        || attempt.ended
        || (attempt.router_pid, attempt.router_started_at_ms) != process_identity()
    {
        return Err("stale or ended worker attempt is fenced out".into());
    }
    Ok(work)
}

fn accepted_revision(work: &Work, revision: &str) -> Result<(), String> {
    if !work
        .review
        .as_ref()
        .is_some_and(|r| r.accepted && r.revision == revision)
        || work
            .artifact
            .as_ref()
            .is_none_or(|a| a.revision != revision)
    {
        return Err("current artifact has no acceptance bound to this revision".into());
    }
    Ok(())
}

// A manual repair admission may omit ready_dependencies. Preserve the parent's
// explicit selection, but do not hide already-reviewed source from its new worker.
fn related_ready_sources(run: &PlannerRun, work: &Work) -> Vec<Value> {
    run.works.values().filter_map(|source| {
        if source.spec.work_id == work.spec.work_id
            || work.spec.plan_id.trim().is_empty()
            || source.spec.plan_id != work.spec.plan_id
            || !matches!(source.status, WorkStatus::Accepted | WorkStatus::Finished)
            || source.attempt.as_ref().is_some_and(|a| !a.ended)
        {
            return None;
        }
        let artifact = source.artifact.as_ref()?;
        accepted_revision(source, &artifact.revision).ok()?;
        Some(json!({
            "work_id": source.spec.work_id,
            "revision": artifact.revision,
            "scope": source.spec.scope,
            "changed_paths": artifact.changed_paths,
            "evidence": artifact.evidence,
            "selected": work.spec.ready_dependencies.get(&source.spec.work_id) == Some(&artifact.revision),
        }))
    }).collect()
}

fn related_source_context(run: &PlannerRun, work: &Work) -> String {
    let sources = related_ready_sources(run, work);
    if sources.is_empty() {
        return String::new();
    }
    format!(
        "\n[Related accepted source]\n{}\nCompare these same-plan accepted heads with your scope before recreating work. Discovery does not select dependencies or authorize merging unselected source; ask the parent to reconcile applicable sources through ready_dependencies.\n",
        Value::Array(sources)
    )
}

pub(crate) fn valid_evidence(value: &str) -> bool {
    !value.trim().is_empty()
        && !["unknown", "pending", "unverified"]
            .contains(&value.trim().to_ascii_lowercase().as_str())
}

/// The token's actor determines the allowed operations. Role declarations in
/// model-authored arguments never broaden that actor's authority.
pub async fn operate(
    shared: &Arc<Shared>,
    sid: &str,
    actor: Option<&WorkerIdentity>,
    operation: Operation,
) -> Result<String, String> {
    for _ in 0..16 {
        match operate_once(shared, sid, actor, operation.clone()).await {
            Err(e) if concurrent(&e) => continue,
            result => return result,
        }
    }
    Err("planner operation remained contended; retry with the same idempotency key".into())
}

async fn operate_once(
    shared: &Arc<Shared>,
    sid: &str,
    actor: Option<&WorkerIdentity>,
    operation: Operation,
) -> Result<String, String> {
    let mut run = ensure(shared, sid)?;
    if let Operation::Status {
        offset,
        ref work_id,
    } = operation
    {
        let visible = |id: &str| {
            actor.is_none_or(|a| a.work_id == id)
                && work_id.as_ref().is_none_or(|wanted| wanted == id)
        };
        let matched_works = run.works.keys().filter(|id| visible(id)).count();
        let works = run.works.iter().filter(|(id, _)| visible(id)).skip(offset).take(32)
            .map(|(id, work)| json!({"work_id":id,"plan_id":work.spec.plan_id,"child_id":work.child_id,"status":work.status,
                "attempt":work.attempt,"workspace":work.workspace.as_ref().map(|w| json!({"path":w.path,"lease":w.lease})),"external_id":work.spec.external_id,
                "revision":work.artifact.as_ref().map(|a| &a.revision),"review":work.review.as_ref().map(|r| json!({"revision":r.revision,"accepted":r.accepted})),
                "paused":work.paused,"recovery":run.capacity.resumes.get(id),"reconciliation":work.reconciliation,
                "spec":work_id.as_ref().map(|_| &work.spec),
                "handoff":work_id.as_ref().and(work.handoff.as_ref()),
                "related_ready_sources":work_id.as_ref().map(|_| related_ready_sources(&run, work)),
                "ready_at_ms":work.ready_at_ms,"ready_age_secs":work.ready_at_ms.map(|at| (chrono::Utc::now().timestamp_millis() - at).max(0) / 1000),
                "required_checks":work.spec.required_checks,"required_integration_evidence":work.spec.required_integration_evidence,
                "unresolved":work.artifact.as_ref().map(|a| &a.unresolved),
                "integration_requirements":work.artifact.as_ref().map(|a| &a.integration_requirements),"integration":work.integration,
                "reason":work.reason.as_ref().map(|s| s.chars().take(500).collect::<String>())})).collect::<Vec<_>>();
        let inputs = run.inputs.iter().filter(|(_, input)| actor.is_none_or(|a| input.owner.as_deref() == Some(&a.work_id))
                && work_id.as_ref().is_none_or(|wanted| input.owner.as_ref() == Some(wanted)))
            .skip(offset).take(32).map(|(id, input)| json!({"input_id":id,"origin":input.origin,"replayed_from":input.replayed_from,"owner":input.owner,"acknowledged":input.acknowledged && crate::planner_input::withdrawal(&run, id).is_none(),"withdrawn":crate::planner_input::withdrawal(&run, id),
                "text":input.text.chars().take(500).collect::<String>(),"attachment_count":input.attachments.len(),"delivered_to":input.delivered_to})).collect::<Vec<_>>();
        let wakes = run
            .wakes
            .iter()
            .filter(|(_, w)| visible(&w.work_id))
            .skip(offset)
            .take(32)
            .collect::<BTreeMap<_, _>>();
        let mut capacity = crate::planner_capacity::status_block(
            shared,
            &run,
            chrono::Utc::now().timestamp_millis(),
        );
        if (work_id.is_some() || actor.is_some())
            && let Some(fields) = capacity.as_object_mut()
        {
            fields.retain(|key, _| {
                [
                    "live",
                    "max",
                    "free",
                    "configured_at_start",
                    "effective_limit",
                ]
                .contains(&key.as_str())
            });
        }
        return serde_json::to_string(&json!({"revision":run.revision,"policy":run.policy.identity,"phase":run.phase,"status":run.status,
            "works":works,"inputs":inputs,"wakes":wakes,"approval_waits":run.approval_waits,
            "execution":actor.is_none().then(|| execution_context(&run)).flatten(),
            "capacity":capacity,
            "progress":progress(&run),"releases":crate::planner_release::status(&run, actor.map(|a| a.work_id.as_str()).or(work_id.as_deref())),
            "offset":offset,"next_offset":offset+32,"matched_works":matched_works,"counts":{"works":run.works.len(),"inputs":run.inputs.len(),"wakes":run.wakes.len()}})).map_err(|e| e.to_string());
    }
    let value = serde_json::to_value(&operation).map_err(|e| e.to_string())?;
    let key = value
        .get("key")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or("mutation requires an idempotency key")?;
    let fingerprint = content_hash(&value.to_string());
    if let Some(previous) = run.receipts.get(key) {
        if previous != &fingerprint {
            return Err("idempotency key already names a different operation".into());
        }
        return Ok(json!({"replayed":true,"revision":run.revision}).to_string());
    }
    let worker_operation = matches!(
        operation,
        Operation::Artifact { .. } | Operation::Finished { .. }
    );
    if let Some(source) = &run.capacity.pending_adoption
        && matches!(operation, Operation::Admit { .. })
    {
        return Err(format!(
            "planner adoption of {source} is pending; the capacity cycle retries the handoff before new admissions"
        ));
    }
    if actor.is_some() && !worker_operation {
        return Err(
            "implementation children cannot coordinate, accept, integrate, or retire plans".into(),
        );
    }
    // A draining run takes no new scope, but its parent and workers still
    // record what they have (artifacts, reviews, receipts, dispositions).
    if run.status == RunStatus::Draining
        && matches!(
            operation,
            Operation::Admit { .. }
                | Operation::Reconcile { .. }
                | Operation::Hold { .. }
                | Operation::Complete { .. }
        )
    {
        return Err(
            "run is shutting down: admit nothing; record the exact resume point in \
             Docs/CONTINUE.md and your registers, then end your turn"
                .into(),
        );
    }
    if !matches!(run.status, RunStatus::Running | RunStatus::Draining)
        && !matches!(
            operation,
            Operation::AcknowledgeInput { .. } | Operation::AcknowledgeWake { .. }
        )
    {
        return Err(format!(
            "planner run is {:?}; dependent automatic actions are suspended",
            run.status
        ));
    }
    let mut next_role = None;
    match operation.clone() {
        Operation::Status { .. } => unreachable!(),
        Operation::Retire {
            work_id,
            reason,
            evidence,
            ..
        } => {
            if shared.cfg.routers.planner.integrate_check.is_none() {
                return Err("retire requires a configured host to verify obsolete scope and parent takeover".into());
            }
            let work = run.works.get(&work_id).ok_or("unknown work identity")?;
            if reason.trim().is_empty()
                || evidence.is_empty()
                || work.attempt.as_ref().is_some_and(|a| !a.ended)
                || matches!(work.status, WorkStatus::Integrated | WorkStatus::Finished)
                || run
                    .releases
                    .values()
                    .any(|release| release.works.contains_key(&work_id))
            {
                return Err("retire requires an ended obsolete assignment, parent takeover evidence and no frozen release membership".into());
            }
            let mut check = value.clone();
            check["assignment"] = serde_json::to_value(work).map_err(|e| e.to_string())?;
            crate::planner_guard::integrate_check(shared, sid, &check).await?;
            let work = run.works.get_mut(&work_id).unwrap();
            work.status = WorkStatus::Retired;
            work.reason = Some(reason);
            work.paused = true;
            work.reconciliation.required = false;
            run.capacity.resumes.remove(&work_id);
            for wake in run
                .wakes
                .values_mut()
                .filter(|wake| wake.work_id == work_id)
            {
                wake.acknowledged = true;
            }
        }
        Operation::Reconcile {
            work,
            source_revision,
            evidence,
            ..
        } => {
            let existing = run
                .works
                .get(&work.work_id)
                .ok_or("unknown reconciliation work")?
                .clone();
            if run.execution_request.is_none()
                || work.plan_id != existing.spec.plan_id
                || work.scope.trim().is_empty()
                || evidence.is_empty()
                || evidence.iter().any(|e| e.trim().is_empty())
            {
                return Err("reconcile requires the same admitted plan, complete current scope and parent custody evidence".into());
            }
            if existing
                .attempt
                .as_ref()
                .is_some_and(|a| !a.ended && owner_alive(a))
            {
                return Err("reconcile requires the owning attempt to stop first; never change a live worker's assignment".into());
            }
            if matches!(
                existing.status,
                WorkStatus::Finished
                    | WorkStatus::Integrated
                    | WorkStatus::Watching
                    | WorkStatus::Retired
            ) || run
                .releases
                .values()
                .any(|release| release.works.contains_key(&work.work_id))
            {
                return Err("reconcile cannot rewrite finished work or a frozen release; use its owning repair".into());
            }
            if existing.handoff.as_ref().is_some_and(|handoff| {
                matches!(
                    handoff.kind.as_str(),
                    "ownership_requested" | "reassignment_required"
                )
            }) && work == existing.spec
            {
                return Err("resolve the ownership/reassignment request with a corrected scope or source ownership in WorkSpec; unchanged custody evidence cannot resolve it".into());
            }
            // An allocator refusal can leave an admitted identity without a workspace.
            // Validate the corrected brief first, then retry its supported allocator;
            // existing allocations still require their original source to be restored.
            let mut validation = run.clone();
            validation.works.remove(&work.work_id);
            validate_owned_paths(&validation, &work)?;
            if work.dependencies.contains(&work.work_id)
                || work.ready_dependencies.contains_key(&work.work_id)
            {
                return Err("work cannot depend on itself".into());
            }
            let workspace = match existing.workspace.clone() {
                Some(workspace) => workspace,
                None => {
                    let mut allocation = existing.clone();
                    allocation.spec = work.clone();
                    allocate_workspace(shared, sid, &allocation, &run.policy).await?
                }
            };
            verify_source_revision(&workspace.path, &source_revision).await?;
            let changed = work != existing.spec
                || existing
                    .artifact
                    .as_ref()
                    .is_some_and(|a| a.revision != source_revision);
            let current = run.works.get_mut(&work.work_id).unwrap();
            if work != current.spec {
                current
                    .reconciliation
                    .prior_specs
                    .push(current.spec.clone());
            }
            current.spec = work;
            current.workspace = Some(workspace);
            current.reconciliation.required = false;
            current.reconciliation.source_revision = Some(source_revision);
            current.reconciliation.evidence = evidence;
            // Fence the prior child and its obsolete briefing. A fresh attempt receives the current spec.
            current.attempt = None;
            current.handoff = None;
            current.reason = None;
            current.paused = false;
            if changed {
                current.artifact = None;
                current.review = None;
                current.finish = None;
            }
            current.status = if current.review.as_ref().is_some_and(|r| r.accepted) {
                WorkStatus::Accepted
            } else {
                WorkStatus::Admitted
            };
            run.capacity.resumes.remove(&current.spec.work_id);
            if let Some(wake) = run
                .wakes
                .get_mut(&format!("recovery:{}", current.spec.work_id))
            {
                wake.acknowledged = true;
            }
        }
        Operation::Admit { work, .. } => {
            if run.execution_request.is_none() || run.phase != PlannerPhase::Implementation {
                return Err("work admission requires an authentic implementation request".into());
            }
            if work.work_id.trim().is_empty()
                || work.plan_id.trim().is_empty()
                || work.scope.trim().is_empty()
            {
                return Err("assignment requires stable work_id, plan_id and bounded scope".into());
            }
            if let Some(existing) = run.works.get(&work.work_id) {
                if existing.spec != work {
                    return Err("work identity already names a different assignment".into());
                }
            } else {
                if work.dependencies.contains(&work.work_id)
                    || work.ready_dependencies.contains_key(&work.work_id)
                {
                    return Err("work cannot depend on itself".into());
                }
                validate_owned_paths(&run, &work)?;
                let child_id = format!(
                    "child-{}",
                    &content_hash(&format!("{sid}/{}", work.work_id))[..24]
                );
                run.works.insert(
                    work.work_id.clone(),
                    Work {
                        reconciliation: Reconciliation::default(),
                        spec: work,
                        child_id,
                        status: WorkStatus::Admitted,
                        ready_at_ms: None,
                        workspace: None,
                        attempt: None,
                        artifact: None,
                        review: None,
                        finish: None,
                        integration: None,
                        reason: None,
                        last_output: None,
                        paused: false,
                        handoff: None,
                    },
                );
            }
        }
        Operation::Artifact {
            work_id,
            attempt_id,
            artifact,
            ..
        } => {
            let work = require_worker(&mut run, &work_id, &attempt_id, actor)?;
            let workspace = work
                .workspace
                .as_ref()
                .ok_or("assignment has no isolated workspace")?;
            verify_revision(&workspace.path, &artifact.revision).await?;
            if let Some(source) = &work.reconciliation.source_revision {
                verify_source_revision(&workspace.path, source).await?;
            }
            if artifact.evidence.is_empty() || artifact.evidence.iter().any(|v| !valid_evidence(v))
            {
                return Err("artifact requires usable evidence references".into());
            }
            if artifact
                .integration_requirements
                .iter()
                .any(|(key, recipe)| key.trim().is_empty() || !valid_evidence(recipe))
            {
                return Err(
                    "integration requirements need a stable check name and usable proof recipe"
                        .into(),
                );
            }
            if artifact.changed_paths.iter().any(|p| {
                Path::new(p).is_absolute()
                    || Path::new(p)
                        .components()
                        .any(|c| matches!(c, std::path::Component::ParentDir))
            }) {
                return Err("artifact paths must stay relative to the workspace".into());
            }
            for check in &work.spec.required_checks {
                if artifact.checks.get(check).is_none_or(|r| r != "pass") {
                    return Err(format!("required check `{check}` is not proven pass"));
                }
            }
            // Reconnecting a worker may replay the exact packet with a new
            // operation key. Only a changed packet invalidates its review.
            if work.artifact.as_ref() != Some(&artifact) {
                work.artifact = Some(artifact);
                work.review = None;
                work.ready_at_ms = None;
                work.finish = None;
                work.integration = None;
                work.status = WorkStatus::ReviewPending;
                next_role = Some(PlannerRole::ReviewWork);
            }
        }
        Operation::Review {
            work_id,
            revision,
            accepted,
            evidence,
            corrections,
            ..
        } => {
            let work = run.works.get_mut(&work_id).ok_or("unknown work identity")?;
            let artifact = work.artifact.as_ref().ok_or("no artifact to review")?;
            if artifact.revision != revision
                || !matches!(
                    work.status,
                    WorkStatus::ReviewPending | WorkStatus::Accepted | WorkStatus::Corrections
                )
            {
                return Err("review refers to a stale artifact or invalid lifecycle state".into());
            }
            verify_revision(
                &work.workspace.as_ref().ok_or("workspace missing")?.path,
                &revision,
            )
            .await?;
            if evidence.is_empty() || evidence.iter().any(|v| !valid_evidence(v)) {
                return Err("review requires independent evidence".into());
            }
            if accepted && !artifact.unresolved.is_empty() {
                return Err("unresolved artifact requirements prevent acceptance".into());
            }
            if !accepted && corrections.as_deref().is_none_or(|s| s.trim().is_empty()) {
                return Err("rejected review requires bounded corrections".into());
            }
            work.review = Some(Review {
                revision,
                accepted,
                evidence,
                corrections,
            });
            work.finish = None;
            work.integration = None;
            work.status = if accepted {
                WorkStatus::Accepted
            } else {
                WorkStatus::Corrections
            };
            next_role = Some(if accepted {
                PlannerRole::FinishWork
            } else {
                PlannerRole::ImplementWork
            });
        }
        Operation::Finished {
            work_id,
            attempt_id,
            receipt,
            ..
        } => {
            let work = require_worker(&mut run, &work_id, &attempt_id, actor)?;
            accepted_revision(work, &receipt.revision)?;
            verify_revision(
                &work.workspace.as_ref().ok_or("workspace missing")?.path,
                &receipt.revision,
            )
            .await?;
            if receipt.evidence.is_empty() || receipt.evidence.values().any(|v| !valid_evidence(v))
            {
                return Err("finishing requires durable handoff and custody evidence".into());
            }
            work.finish = Some(receipt);
            work.status = WorkStatus::Finished;
            work.ready_at_ms
                .get_or_insert_with(|| chrono::Utc::now().timestamp_millis());
            next_role = Some(PlannerRole::IntegratePlan);
        }
        Operation::Finalize {
            work_id, receipt, ..
        } => {
            if shared.cfg.routers.planner.integrate_check.is_none() {
                return Err("parent finalization requires a trusted integrate-check host; use the child's finished receipt otherwise".into());
            }
            let work = run.works.get_mut(&work_id).ok_or("unknown work identity")?;
            accepted_revision(work, &receipt.revision)?;
            if !matches!(work.status, WorkStatus::Accepted | WorkStatus::Interrupted)
                || work.attempt.as_ref().is_some_and(|a| !a.ended)
            {
                return Err(
                    "parent finalization requires an accepted revision and an ended attempt".into(),
                );
            }
            verify_revision(
                &work.workspace.as_ref().ok_or("workspace missing")?.path,
                &receipt.revision,
            )
            .await?;
            if receipt.evidence.is_empty() || receipt.evidence.values().any(|v| !valid_evidence(v))
            {
                return Err("finalization requires durable handoff and custody evidence".into());
            }
            let mut check = value.clone();
            check["assignment"] = serde_json::to_value(&*work).map_err(|e| e.to_string())?;
            crate::planner_guard::integrate_check(shared, sid, &check).await?;
            work.finish = Some(receipt);
            work.status = WorkStatus::Finished;
            work.ready_at_ms
                .get_or_insert_with(|| chrono::Utc::now().timestamp_millis());
            next_role = Some(PlannerRole::IntegratePlan);
        }
        Operation::Integrate {
            work_id, receipt, ..
        } => {
            crate::planner_release::integration_allowed(&run, &work_id, &receipt)?;
            let work = run.works.get_mut(&work_id).ok_or("unknown work identity")?;
            accepted_revision(work, &receipt.revision)?;
            if work.status != WorkStatus::Finished
                || work
                    .finish
                    .as_ref()
                    .is_none_or(|r| r.revision != receipt.revision)
            {
                return Err("integration requires the finished accepted revision".into());
            }
            if work.attempt.as_ref().is_some_and(|a| !a.ended) {
                return Err("integration waits until the child has handed back its turn".into());
            }
            verify_revision(
                &work.workspace.as_ref().ok_or("workspace missing")?.path,
                &receipt.revision,
            )
            .await?;
            if receipt.evidence.is_empty() || receipt.evidence.values().any(|v| !valid_evidence(v))
            {
                return Err("integration requires verified disposition evidence".into());
            }
            for required in work.spec.required_integration_evidence.iter().chain(
                work.artifact
                    .iter()
                    .flat_map(|a| a.integration_requirements.keys()),
            ) {
                if !receipt
                    .evidence
                    .get(required)
                    .is_some_and(|v| valid_evidence(v))
                {
                    return Err(format!(
                        "required integration evidence `{required}` is missing"
                    ));
                }
            }
            crate::planner_guard::integrate_check(shared, sid, &value).await?;
            let work = run.works.get_mut(&work_id).ok_or("unknown work identity")?;
            work.integration = Some(receipt);
            work.status = WorkStatus::Integrated;
            next_role = Some(PlannerRole::SelectPlan);
        }
        Operation::Release {
            release_id,
            checkpoint,
            ..
        } => {
            if shared.cfg.routers.planner.integrate_check.is_none() {
                return Err("release checkpoints require a trusted integrate-check host".into());
            }
            crate::planner_release::record(&mut run, &release_id, &checkpoint)?;
            let mut check = value.clone();
            check["release_record"] =
                serde_json::to_value(&run.releases[&release_id]).map_err(|e| e.to_string())?;
            check["release_record"]["plans"] = json!(
                run.releases[&release_id]
                    .works
                    .keys()
                    .filter_map(|id| run
                        .works
                        .get(id)
                        .and_then(|work| work.spec.external_id.as_ref())
                        .map(|path| (id, path)))
                    .collect::<BTreeMap<_, _>>()
            );
            check["release_record"]["source_dependencies"] = json!(
                run.releases[&release_id]
                    .works
                    .keys()
                    .filter_map(|id| run.works.get(id))
                    .flat_map(|work| work.spec.ready_dependencies.iter())
                    .collect::<BTreeMap<_, _>>()
            );
            crate::planner_guard::integrate_check(shared, sid, &check).await?;
        }
        Operation::Disposition {
            work_id,
            status,
            reason,
            ..
        } => {
            if !matches!(status, WorkStatus::Blocked | WorkStatus::Watching)
                || reason.trim().is_empty()
            {
                return Err("disposition requires blocked/watching with a concrete reason".into());
            }
            let work = run.works.get_mut(&work_id).ok_or("unknown work identity")?;
            if work.attempt.as_ref().is_some_and(|a| !a.ended) {
                return Err("cannot dispose active worker scope".into());
            }
            if work.status == WorkStatus::Integrated {
                return Err("integrated work cannot be silently reclassified".into());
            }
            let mut check = value.clone();
            check["assignment"] = serde_json::to_value(&*work).map_err(|e| e.to_string())?;
            crate::planner_guard::integrate_check(shared, sid, &check).await?;
            let work = run.works.get_mut(&work_id).ok_or("unknown work identity")?;
            work.status = status;
            work.reason = Some(reason);
        }
        Operation::AcknowledgeInput {
            input_id, owner, ..
        } => {
            acknowledge_input(&mut run, sid, &input_id, owner)?;
        }
        Operation::AcknowledgeWake { wake_id, .. } => {
            if let Some(id) = wake_id.strip_prefix("input:") {
                let owner = run
                    .inputs
                    .get(id)
                    .ok_or("unknown input receipt")?
                    .owner
                    .clone();
                acknowledge_input(&mut run, sid, id, owner)?;
            }
            run.wakes
                .get_mut(&wake_id)
                .ok_or("unknown wake receipt")?
                .acknowledged = true;
        }
        Operation::Hold {
            plan_id,
            reason,
            detail,
            ..
        } => {
            // The router's own evidence rules first, then the host's check.
            let mut probe = run.clone();
            crate::planner_capacity::record_hold(
                &mut probe,
                &plan_id,
                &reason,
                &detail,
                chrono::Utc::now().timestamp_millis(),
            )?;
            // The audit host verifies candidate evidence; a human's withheld
            // authorization is the human's to lift, not the host's to check.
            if reason != crate::planner_capacity::AWAITING_HUMAN {
                crate::planner_guard::verify_hold(shared, sid, &run, &plan_id, &reason, &detail)
                    .await?;
            }
            crate::planner_capacity::record_hold(
                &mut run,
                &plan_id,
                &reason,
                &detail,
                chrono::Utc::now().timestamp_millis(),
            )?;
        }
        Operation::Complete { queue_evidence, .. } => {
            if !valid_evidence(&queue_evidence) {
                return Err("completion requires a verified queue/disposition result".into());
            }
            if run
                .releases
                .values()
                .any(crate::planner_release::Release::active)
            {
                return Err("completion requires every release's verified plan disposition".into());
            }
            let pending = run
                .works
                .iter()
                .filter(|(_, w)| {
                    !matches!(w.status, WorkStatus::Integrated | WorkStatus::Retired)
                        || w.attempt.as_ref().is_some_and(|a| !a.ended)
                })
                .map(|(id, w)| format!("{id} ({:?})", w.status))
                .collect::<Vec<_>>();
            if !pending.is_empty()
                || run.inputs.values().any(|i| !i.resolved())
                || run.wakes.values().any(|w| !w.acknowledged)
            {
                return Err(format!(
                    "completion requires all admitted scope, input and wakes to be reconciled; blocked/watching scope remains explicitly pending. Unfinished works: [{}]",
                    pending.join(", ")
                ));
            }
            // The capacity host decides whether admissible work remains; an
            // unknown answer is never "no work".
            crate::planner_capacity::completion_allowed(shared, sid, &run).await?;
            // The auditor's completing pass (CONTINUE finished, every lane
            // and disposition closed) has the last word.
            crate::planner_guard::completion_audit(shared, sid, &run).await?;
            run.status = RunStatus::Complete;
        }
    }
    run.receipts.insert(key.to_string(), fingerprint);
    save(shared, sid, &mut run)?;
    // Deliver the queued `work_finished` now rather than on the next flusher
    // tick; a host that refuses it keeps it queued, never this operation.
    if matches!(operation, Operation::Integrate { .. })
        && let Some(hook) = shared.cfg.delegation.lifecycle_hook.clone()
    {
        let shared = shared.clone();
        tokio::spawn(async move {
            crate::delegate_hook::flush(&shared, &hook).await;
        });
    }
    Ok(json!({"revision":run.revision,"next_role":next_role,"instructions":next_role.map(|r| run.policy.instructions(r))}).to_string())
}

async fn git(path: &Path, args: &[&str]) -> Result<String, String> {
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .await
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.first().unwrap_or(&""),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn verify_revision(path: &Path, revision: &str) -> Result<(), String> {
    if git(path, &["rev-parse", "HEAD"]).await? != revision {
        return Err("workspace HEAD changed; submit its current artifact for fresh review".into());
    }
    if !git(path, &["status", "--porcelain", "--untracked-files=normal"])
        .await?
        .is_empty()
    {
        return Err(
            "workspace has uncommitted changes; reviewed artifact is not the current workspace"
                .into(),
        );
    }
    Ok(())
}

pub(crate) fn process_identity() -> (u32, u64) {
    #[cfg(target_os = "linux")]
    if let Some(started) = linux_process_start_identity(std::process::id()) {
        return (std::process::id(), started);
    }
    crate::delegate_hook::process_identity()
}

#[cfg(target_os = "linux")]
fn linux_process_start_identity(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = stat
        .rsplit_once(')')?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    fields.get(19)?.parse().ok()
}

/// Whether a process with `pid` exists, by signal 0 rather than a spawned
/// `kill`, so file-descriptor or process exhaustion cannot read as a death.
/// Only "no such process" is absence; a permission refusal is a live process.
fn pid_exists(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return false;
        };
        // SAFETY: signal 0 performs only the existence and permission check.
        if unsafe { libc::kill(pid, 0) } == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

pub(crate) fn owner_alive(attempt: &Attempt) -> bool {
    if attempt.router_pid == 0 {
        return false;
    }
    // This router's own attempts need no probe. A probe that fails for any
    // reason but a missing process must not declare a live owner dead: the
    // wake loop ends every attempt it judges orphaned.
    if attempt.router_pid == std::process::id() {
        return (attempt.router_pid, attempt.router_started_at_ms) == process_identity();
    }
    if !pid_exists(attempt.router_pid) {
        return false;
    }

    #[cfg(target_os = "linux")]
    {
        linux_process_start_identity(attempt.router_pid)
            .is_none_or(|start| start == attempt.router_started_at_ms)
    }
    #[cfg(not(target_os = "linux"))]
    {
        true
    }
}

pub(crate) fn workspace_available(workspace: &Workspace) -> Result<(), String> {
    if !workspace.path.is_dir() {
        return Err(format!(
            "planner workspace unavailable: {}. Reconcile the allocation lease and restore the workspace before retrying; changing models cannot repair a missing directory",
            workspace.path.display()
        ));
    }
    Ok(())
}

fn continuation_role(work: &Work) -> PlannerRole {
    if matches!(work.status, WorkStatus::Accepted | WorkStatus::Interrupted)
        && work.artifact.as_ref().is_some_and(|artifact| {
            work.review
                .as_ref()
                .is_some_and(|review| review.accepted && review.revision == artifact.revision)
        })
    {
        PlannerRole::FinishWork
    } else {
        PlannerRole::ImplementWork
    }
}

pub(crate) fn mark_reconciliation_required(work: &mut Work) {
    if matches!(
        work.status,
        WorkStatus::Admitted
            | WorkStatus::Allocating
            | WorkStatus::Running
            | WorkStatus::Interrupted
            | WorkStatus::Corrections
            | WorkStatus::Accepted
    ) && work.workspace.is_some()
    {
        work.reconciliation.required = true;
    }
}

async fn verify_source_revision(path: &Path, revision: &str) -> Result<(), String> {
    if revision.len() != 40 || !revision.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("reconciliation source_revision must be a full commit SHA".into());
    }
    git(path, &["merge-base", "--is-ancestor", revision, "HEAD"]).await
        .map_err(|_| "reconciled source is not carried in workspace HEAD; preserve and integrate it before proceeding".to_string())?;
    Ok(())
}

fn validate_owned_paths(run: &PlannerRun, work: &WorkSpec) -> Result<(), String> {
    for path in &work.owned_paths {
        if path.trim().is_empty()
            || Path::new(path).is_absolute()
            || path
                .trim_end_matches('/')
                .split('/')
                .any(|p| matches!(p, "" | "." | ".."))
            || path.contains(['\\', '*', '?', '[', ']'])
        {
            return Err(
                "owned_paths must name relative files or directories without traversal".into(),
            );
        }
        for other in run.works.values().filter(|w| {
            matches!(
                w.status,
                WorkStatus::Admitted
                    | WorkStatus::Allocating
                    | WorkStatus::Running
                    | WorkStatus::Corrections
                    | WorkStatus::Interrupted
            )
        }) {
            if other
                .spec
                .owned_paths
                .iter()
                .any(|owned| paths_overlap(path, owned))
            {
                return Err(format!(
                    "source ownership conflict: {path} is owned by {}",
                    other.spec.work_id
                ));
            }
        }
    }
    Ok(())
}

fn paths_overlap(left: &str, right: &str) -> bool {
    let left = left.trim_end_matches('/');
    let right = right.trim_end_matches('/');
    left == right
        || left.starts_with(&format!("{right}/"))
        || right.starts_with(&format!("{left}/"))
}

pub(crate) fn dependencies_ready(run: &PlannerRun, work: &Work) -> Result<(), String> {
    for other in run.works.values().filter(|other| {
        other.spec.work_id != work.spec.work_id
            && matches!(
                other.status,
                WorkStatus::Admitted
                    | WorkStatus::Allocating
                    | WorkStatus::Running
                    | WorkStatus::Corrections
                    | WorkStatus::Interrupted
            )
    }) {
        if let Some(path) = work.spec.owned_paths.iter().find(|path| {
            other
                .spec
                .owned_paths
                .iter()
                .any(|owned| paths_overlap(path, owned))
        }) {
            return Err(format!(
                "source ownership conflict: {path} is owned by {}",
                other.spec.work_id
            ));
        }
    }
    for id in &work.spec.dependencies {
        if !run
            .works
            .get(id)
            .is_some_and(|w| w.status == WorkStatus::Integrated)
        {
            return Err(format!("assignment dependencies are not integrated: {id}"));
        }
    }
    for (id, revision) in &work.spec.ready_dependencies {
        if !run.works.get(id).is_some_and(|w| {
            matches!(
                w.status,
                WorkStatus::Accepted | WorkStatus::Finished | WorkStatus::Integrated
            ) && w
                .review
                .as_ref()
                .is_some_and(|r| r.accepted && &r.revision == revision)
                && w.artifact.as_ref().is_some_and(|a| &a.revision == revision)
                && w.attempt.as_ref().is_none_or(|a| a.ended)
        }) {
            return Err(format!(
                "accepted source dependency {id}@{revision} is not ready"
            ));
        }
    }
    Ok(())
}

/// These are work counts, never a claim that a repository plan is complete.
pub fn progress(run: &PlannerRun) -> Value {
    let mut states = BTreeMap::<String, usize>::new();
    for work in run.works.values() {
        let state = serde_json::to_value(work.status).expect("work status is serializable");
        *states
            .entry(state.as_str().unwrap().to_string())
            .or_default() += 1;
    }
    let completed = run
        .capacity
        .audit
        .as_ref()
        .filter(|audit| audit.ok)
        .and_then(|audit| audit.completed_guides.as_ref());
    let newly_completed = completed
        .zip(run.completed_guides_at_start.as_ref())
        .map(|(current, initial)| current.difference(initial).collect::<Vec<_>>());
    json!({"work_states":states,
        "repository_completed_guides":completed.map(BTreeSet::len),"new_completed_guides_since_first_audit":newly_completed,
        "observed_at_ms":run.capacity.audit.as_ref().map(|audit| audit.at_ms),
        "awaiting_review":run.works.values().filter(|w| w.status == WorkStatus::ReviewPending).count(),
        "awaiting_integration":run.works.values().filter(|w| w.status == WorkStatus::Finished).count(),
        "integrated":run.works.values().filter(|w| w.status == WorkStatus::Integrated).count(),
        "recovery_required":run.capacity.resumes.iter().filter(|(_, r)| r.suspended.is_some()).map(|(id, _)| id).collect::<Vec<_>>()})
}

pub async fn begin_work(
    shared: &Arc<Shared>,
    sid: &str,
    work_id: &str,
) -> Result<(WorkerIdentity, Workspace, String, String), String> {
    for _ in 0..16 {
        match begin_work_once(shared, sid, work_id).await {
            Err(e) if concurrent(&e) => continue,
            result => return result,
        }
    }
    Err("assignment claim remained contended; retry the same work identity".into())
}

async fn begin_work_once(
    shared: &Arc<Shared>,
    sid: &str,
    work_id: &str,
) -> Result<(WorkerIdentity, Workspace, String, String), String> {
    let mut run = ensure(shared, sid)?;
    if run.status != RunStatus::Running
        || run.execution_request.is_none()
        || run.phase != PlannerPhase::Implementation
    {
        return Err("planner execution is not authorized or is suspended".into());
    }
    let snapshot = run
        .works
        .get(work_id)
        .ok_or("admit this work identity before dispatch")?
        .clone();
    if snapshot.reconciliation.required {
        return Err("parent reconciliation required: record current scope and preserved source with planner_workflow reconcile before resuming".into());
    }
    if snapshot.paused {
        return Err(
            "assignment was stopped by the client; explicit child resume is required".into(),
        );
    }
    dependencies_ready(&run, &snapshot)?;
    if snapshot
        .attempt
        .as_ref()
        .is_some_and(|a| !a.ended && owner_alive(a))
    {
        return Err(
            "assignment already has a live owning attempt; send corrections to that child".into(),
        );
    }
    if !matches!(
        snapshot.status,
        WorkStatus::Admitted
            | WorkStatus::Allocating
            | WorkStatus::Corrections
            | WorkStatus::Interrupted
            | WorkStatus::Accepted
            | WorkStatus::Blocked
            | WorkStatus::Running
    ) {
        return Err("assignment is waiting for parent review or integration, not another implementation attempt".into());
    }
    if let Some(workspace) = &snapshot.workspace {
        workspace_available(workspace)?;
        if let Some(source) = &snapshot.reconciliation.source_revision {
            verify_source_revision(&workspace.path, source).await?;
        }
    }
    let role = continuation_role(&snapshot);
    if role == PlannerRole::FinishWork {
        let artifact = snapshot
            .artifact
            .as_ref()
            .ok_or("accepted artifact missing")?;
        verify_revision(
            &snapshot.workspace.as_ref().ok_or("workspace missing")?.path,
            &artifact.revision,
        )
        .await?;
    }
    let (router_pid, router_started_at_ms) = process_identity();
    let attempt_id = uuid::Uuid::new_v4().to_string();
    let work = run.works.get_mut(work_id).unwrap();
    work.attempt = Some(Attempt {
        id: attempt_id.clone(),
        router_pid,
        router_started_at_ms,
        delegate_id: None,
        ended: false,
    });
    work.status = WorkStatus::Allocating;
    // A new attempt starts clean; the previous failure stays in `last_output`.
    work.reason = None;
    work.handoff = None;
    save(shared, sid, &mut run)?;
    let allocation = async {
        let workspace = match snapshot.workspace.clone() {
            Some(previous) if run.policy.workspace.is_some() => {
                // A saved workspace owns source and a lease, not an eternal snapshot
                // of its launch environment. Let the repository refresh tooling on
                // every attempt, including recovery after a router/host crash.
                let refreshed = allocate_workspace(shared, sid, &snapshot, &run.policy).await?;
                if refreshed.path != previous.path || refreshed.lease != previous.lease {
                    return Err(
                        "workspace refresh changed the existing source path or lease".into(),
                    );
                }
                refreshed
            }
            Some(workspace) => workspace,
            None => allocate_workspace(shared, sid, &snapshot, &run.policy).await?,
        };
        workspace_available(&workspace)?;
        shared.state.lock().unwrap().claim_planner_workspace(
            &workspace.path,
            sid,
            work_id,
            &workspace.lease,
        )?;
        Ok::<_, String>(workspace)
    }
    .await;
    let workspace = match allocation {
        Ok(workspace) => workspace,
        Err(error) => {
            mutate(shared, sid, |run| {
                let work = run.works.get_mut(work_id).ok_or("assignment disappeared")?;
                if let Some(attempt) = &mut work.attempt
                    && attempt.id == attempt_id
                {
                    attempt.ended = true;
                    work.status = WorkStatus::Interrupted;
                    work.reason = Some(error.clone());
                }
                Ok(())
            })?;
            return Err(error);
        }
    };
    // Allocation runs without a DB lock. Reconcile any input admitted while
    // it ran instead of overwriting that input with the earlier snapshot.
    let run = mutate(shared, sid, |run| {
        let suspended =
            run.status != RunStatus::Running || run.phase != PlannerPhase::Implementation;
        let work = run.works.get_mut(work_id).ok_or("assignment disappeared")?;
        let attempt = work
            .attempt
            .as_mut()
            .ok_or("allocation attempt disappeared")?;
        if attempt.id != attempt_id {
            return Err("workspace allocation lost its ownership fence".into());
        }
        work.workspace = Some(workspace.clone());
        if suspended {
            attempt.ended = true;
            work.status = WorkStatus::Interrupted;
        } else {
            work.status = if role == PlannerRole::FinishWork {
                WorkStatus::Accepted
            } else {
                WorkStatus::Running
            };
            acknowledge_attempt_notifications(run, work_id);
        }
        Ok(())
    })?;
    if run.status != RunStatus::Running || run.phase != PlannerPhase::Implementation {
        return Err("planner suspended during workspace allocation".into());
    }
    let mut instructions = run.policy.instructions(role);
    if role == PlannerRole::ImplementWork
        && let Some((_, text)) = run
            .receipts
            .iter()
            .rev()
            .find(|(key, _)| key.starts_with("entrypoint:"))
    {
        let skill: ResolvedSkill = serde_json::from_str(text).map_err(|e| e.to_string())?;
        if skill.source != run.policy.roles[&role].source {
            instructions.push_str(&format!(
                "\n[Explicit repository entrypoint: {}]\n{}",
                skill.name, skill.text
            ));
        }
    }
    instructions.push_str(&format!("\n[Durable assignment]\nwork_id: {work_id}\nattempt_id: {attempt_id}\nchild_id: {}\nplan_id: {}\nAssigned scope: {}\nRequired checks: {:?}\nPrior evidence: {:?}\nPrior corrections: {:?}\nPrior output: {:?}\n", snapshot.child_id, snapshot.spec.plan_id, snapshot.spec.scope, snapshot.spec.required_checks, snapshot.artifact, snapshot.review, snapshot.last_output));
    instructions.push_str(&format!("\n[Current source custody]\n{}\nThis current assignment supersedes older briefs. Preserve and carry its source revision; do not reset transferred changes as foreign work.\n", serde_json::to_string(&snapshot.reconciliation).map_err(|e| e.to_string())?));
    if role == PlannerRole::ImplementWork {
        instructions.push_str(&related_source_context(&run, &snapshot));
    }
    Ok((
        WorkerIdentity {
            work_id: work_id.into(),
            attempt_id,
        },
        workspace,
        snapshot.child_id,
        instructions,
    ))
}

async fn allocate_workspace(
    shared: &Shared,
    sid: &str,
    work: &Work,
    policy: &ResolvedPlanner,
) -> Result<Workspace, String> {
    let cwd = shared
        .with_session(sid, |s| s.cwd.clone())
        .ok_or("parent session missing")?;
    let workspace = if let Some(allocator) = &policy.workspace {
        // The run's first allocation first takes over an active native
        // implement-all run, so its workers and these never collide.
        crate::planner_guard::adopt_native_once(shared, sid, allocator).await;
        let mut child = tokio::process::Command::new(&allocator.command)
            .args(&allocator.args)
            .current_dir(&cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("workspace allocator failed to start: {e}"))?;
        let mut stdin = child.stdin.take().ok_or("allocator stdin missing")?;
        stdin.write_all(json!({"event":"planner_allocate","session_id":sid,"work":work.spec,"child_id":work.child_id}).to_string().as_bytes()).await.map_err(|e| e.to_string())?;
        drop(stdin);
        let output = tokio::time::timeout(
            std::time::Duration::from_millis(allocator.timeout_ms),
            child.wait_with_output(),
        )
        .await
        .map_err(|_| "workspace allocator timed out; reconcile its lease before retrying")?
        .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "workspace allocator refused assignment: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        serde_json::from_slice::<Workspace>(&output.stdout)
            .map_err(|e| format!("invalid allocator receipt: {e}"))?
    } else {
        let path = shared
            .cfg
            .state_file
            .parent()
            .ok_or("state DB requires a parent directory")?
            .join("planner-workspaces")
            .join(&work.child_id);
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .map_err(|e| e.to_string())?;
        if !path.exists() {
            let output = tokio::process::Command::new("git")
                .args(["clone", "--no-hardlinks", "--quiet"])
                .arg(&cwd)
                .arg(&path)
                .output()
                .await
                .map_err(|e| e.to_string())?;
            if !output.status.success() {
                return Err(format!(
                    "cannot allocate isolated clone: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ));
            }
            // Clone from the session repository, then restore its upstream.
            // Pushing must never target the parent's non-bare checkout.
            match git(&cwd, &["remote", "get-url", "origin"]).await {
                Ok(origin) => {
                    git(&path, &["remote", "set-url", "origin", &origin]).await?;
                }
                Err(_) => {
                    git(&path, &["remote", "remove", "origin"]).await?;
                }
            }
        }
        Workspace {
            path,
            lease: work.child_id.clone(),
            environment: BTreeMap::new(),
            mcp_servers: Vec::new(),
        }
    };
    let path = std::fs::canonicalize(&workspace.path)
        .map_err(|e| format!("allocated workspace is not accessible: {e}"))?;
    if path == std::fs::canonicalize(&cwd).map_err(|e| e.to_string())?
        || workspace.lease.trim().is_empty()
    {
        return Err(
            "allocator must return an isolated workspace and nonempty durable lease".into(),
        );
    }
    git(&path, &["rev-parse", "--show-toplevel"]).await?;
    Ok(Workspace {
        path,
        lease: workspace.lease,
        environment: workspace.environment,
        mcp_servers: workspace.mcp_servers,
    })
}

/// Keep the handoff a planner worker just recorded on its own attempt, so the
/// attempt's end names it. A stale attempt's handoff is ignored.
pub fn record_handoff(
    shared: &Arc<Shared>,
    sid: &str,
    actor: &WorkerIdentity,
    handoff: crate::delegate_hook::Handoff,
) -> Result<(), String> {
    mutate(shared, sid, |run| {
        let Some(work) = run.works.get_mut(&actor.work_id) else {
            return Ok(());
        };
        if work
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.id == actor.attempt_id && !attempt.ended)
        {
            work.handoff = Some(handoff.clone());
            if matches!(
                handoff.kind.as_str(),
                "ownership_requested" | "reassignment_required"
            ) {
                // Replaying the same brief cannot resolve missing ownership.
                // Use the existing parent reconciliation path before dispatch.
                mark_reconciliation_required(work);
            }
        }
        Ok(())
    })
    .map(|_| ())
}

pub fn attempt_ended(
    shared: &Arc<Shared>,
    sid: &str,
    actor: &WorkerIdentity,
    delegate_id: Option<String>,
    output: String,
    failed: bool,
) -> Result<(), String> {
    let run = mutate(shared, sid, |run| {
        let work = run
            .works
            .get_mut(&actor.work_id)
            .ok_or("unknown work identity")?;
        let attempt = work.attempt.as_mut().ok_or("missing attempt")?;
        if attempt.id != actor.attempt_id {
            return Err("stale worker stop cannot alter a replacement attempt".into());
        }
        let new_completion = !attempt.ended;
        attempt.ended = true;
        attempt.delegate_id = delegate_id.clone();
        work.last_output = Some(output.clone());
        if work.status == WorkStatus::Running || work.status == WorkStatus::Allocating {
            work.status = WorkStatus::Interrupted;
            work.reason = Some(match &work.handoff {
                // The worker handed back on purpose (waiting, a question, a
                // lease); say so rather than reporting a lost attempt.
                Some(handoff) => {
                    let first = handoff
                        .message
                        .lines()
                        .find(|l| !l.trim().is_empty())
                        .unwrap_or("")
                        .trim();
                    let first: String = first.chars().take(300).collect();
                    format!("handed off `{}`: {first}", handoff.kind)
                }
                None => "attempt stopped without a valid artifact/finishing receipt; reconcile before resuming"
                    .into(),
            });
        }
        if failed {
            // Keep the cause (a host refusal, a provider error) in the reason the
            // parent reads; the full text stays in `last_output`.
            let cause = output
                .lines()
                .find(|l| !l.trim().is_empty())
                .unwrap_or("")
                .trim();
            let cause: String = cause.chars().take(300).collect();
            work.reason = Some(if cause.is_empty() {
                "child turn failed; preserve recorded evidence and reconcile before retry".into()
            } else {
                format!(
                    "child turn failed: {cause}; preserve recorded evidence and reconcile before retry"
                )
            });
        }
        let wake_id = format!("{}:{}", actor.work_id, actor.attempt_id);
        let wake = run.wakes.entry(wake_id.clone()).or_insert(Wake {
            work_id: actor.work_id.clone(),
            reason: format!("child state: {:?}", work.status),
            acknowledged: false,
            deliveries: 0,
            retry_after: 0,
            claim_pid: 0,
            claim_started_at_ms: 0,
        });
        if new_completion {
            wake.reason = format!("child state: {:?}", work.status);
            wake.acknowledged = false;
            wake.deliveries = 0;
            wake.retry_after = 0;
            wake.claim_pid = 0;
            wake.claim_started_at_ms = 0;
        }
        Ok(())
    })?;
    let wake_id = format!("{}:{}", actor.work_id, actor.attempt_id);
    let event = wake_event(sid, &run, &wake_id, &run.wakes[&wake_id]);
    if let Some(hook) = shared.cfg.delegation.lifecycle_hook.clone() {
        let shared = shared.clone();
        tokio::spawn(async move {
            crate::delegate_hook::flush(&shared, &hook).await;
        });
    }
    let caps = serde_json::to_value(shared.upstream_client_capabilities()).unwrap_or_default();
    if caps
        .pointer("/_meta/router_acp/planner_wake")
        .and_then(Value::as_bool)
        == Some(true)
        && let Some(upstream) = shared.upstream()
    {
        let notification =
            agent_client_protocol::UntypedMessage::new("router-acp/planner_wake", event)
                .map_err(|e| e.to_string())?;
        upstream
            .send_notification(notification)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub fn begin_followup(
    shared: &Arc<Shared>,
    sid: &str,
    actor: &WorkerIdentity,
) -> Result<String, String> {
    let mut context = String::new();
    mutate(shared, sid, |run| {
        // A drain's shutdown follow-up is the one turn a draining run gives.
        if !matches!(run.status, RunStatus::Running | RunStatus::Draining)
            || run.phase != PlannerPhase::Implementation
        {
            return Err("planner run is suspended".into());
        }
        let work = run
            .works
            .get(&actor.work_id)
            .ok_or("unknown work identity")?;
        dependencies_ready(run, work)?;
        if work.reconciliation.required {
            return Err("parent reconciliation required before a child follow-up".into());
        }
        let work = run
            .works
            .get_mut(&actor.work_id)
            .ok_or("unknown work identity")?;
        let role = continuation_role(work);
        if let Some(workspace) = &work.workspace {
            workspace_available(workspace)?;
        }
        let attempt = work.attempt.as_mut().ok_or("assignment has no attempt")?;
        if work.paused {
            return Err(
                "assignment was stopped by the client; explicit child resume is required".into(),
            );
        }
        if attempt.id != actor.attempt_id || !attempt.ended {
            return Err("attempt is stale or already busy".into());
        }
        if !matches!(
            work.status,
            WorkStatus::Accepted | WorkStatus::Corrections | WorkStatus::Interrupted
        ) {
            return Err(
                "parent review must accept or return corrections before another child turn".into(),
            );
        }
        attempt.ended = false;
        work.status = if role == PlannerRole::FinishWork {
            WorkStatus::Accepted
        } else {
            WorkStatus::Running
        };
        context = format!(
            "{}\nwork_id: {}\nattempt_id: {}\nAcceptance/corrections: {:?}\nCurrent assignment: {:?}\nSource custody: {:?}",
            run.policy.instructions(role),
            actor.work_id,
            actor.attempt_id,
            work.review,
            work.spec,
            work.reconciliation
        );
        if role == PlannerRole::ImplementWork {
            context.push_str(&related_source_context(run, &run.works[&actor.work_id]));
        }
        acknowledge_attempt_notifications(run, &actor.work_id);
        Ok(())
    })?;
    Ok(context)
}

/// A successful resume/claim disposes of prior child-stop notifications. Keep the
/// records and their evidence, but do not repeatedly wake the parent about an
/// interruption whose ownership already advanced. Review, custody and allocation
/// fences must pass before calling this; the next stop re-arms its own notice.
fn acknowledge_attempt_notifications(run: &mut PlannerRun, work_id: &str) {
    let prefix = format!("{work_id}:");
    for (id, wake) in &mut run.wakes {
        if wake.work_id == work_id
            && wake.reason.starts_with("child state: ")
            && id
                .strip_prefix(&prefix)
                .is_some_and(|attempt| uuid::Uuid::parse_str(attempt).is_ok())
        {
            wake.acknowledged = true;
        }
    }
}

#[cfg(test)]
mod liveness_tests {
    use super::*;

    #[test]
    fn this_routers_attempts_are_alive_without_a_probe() {
        let (router_pid, router_started_at_ms) = process_identity();
        let attempt = |pid, started| Attempt {
            id: "a".into(),
            router_pid: pid,
            router_started_at_ms: started,
            delegate_id: None,
            ended: false,
        };
        assert!(owner_alive(&attempt(router_pid, router_started_at_ms)));
        assert!(
            !owner_alive(&attempt(router_pid, router_started_at_ms + 1)),
            "a reused pid is not this router"
        );
        assert!(!owner_alive(&attempt(0, 0)));
    }

    #[cfg(unix)]
    #[test]
    fn a_missing_process_is_absent_and_an_unsignalable_one_is_present() {
        assert!(pid_exists(std::process::id()));
        assert!(pid_exists(1), "launchd/init refuses signals but exists");
        assert!(!pid_exists(i32::MAX as u32), "no such process");
    }
}
