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
    pub phase: PlannerPhase,
    pub coordinator: bool,
    pub status: RunStatus,
    pub execution_request: Option<String>,
    #[serde(default)]
    pub execution_input_id: Option<String>,
    pub commands: BTreeSet<String>,
    pub works: BTreeMap<String, Work>,
    pub inputs: BTreeMap<String, InputReceipt>,
    #[serde(default)]
    pub parent_queue: Vec<QueuedParentInput>,
    pub wakes: BTreeMap<String, Wake>,
    #[serde(default)]
    pub approval_waits: BTreeSet<String>,
    pub receipts: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunStatus {
    Running,
    Paused,
    Cancelled,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkSpec {
    pub work_id: String,
    pub plan_id: String,
    pub scope: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
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
    pub text: String,
    pub attachments: Vec<Value>,
    pub owner: Option<String>,
    pub acknowledged: bool,
    #[serde(default)]
    pub delivered_to: BTreeSet<String>,
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
            },
        );
    }
}

pub fn confirm_delivery(shared: &Shared, state_sid: &str) -> Result<(), String> {
    let delivery = shared.planner_deliveries.lock().unwrap().remove(state_sid);
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
    },
    Admit {
        key: String,
        work: WorkSpec,
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
    Integrate {
        key: String,
        work_id: String,
        receipt: Receipt,
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
        phase: PlannerPhase::Planning,
        coordinator,
        status: RunStatus::Running,
        execution_request: None,
        execution_input_id: None,
        commands: BTreeSet::new(),
        works: BTreeMap::new(),
        inputs: BTreeMap::new(),
        parent_queue: Vec::new(),
        wakes: BTreeMap::new(),
        approval_waits: BTreeSet::new(),
        receipts: BTreeMap::new(),
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
    if !human_prompt(req) {
        return None;
    }
    let ContentBlock::Text(first) = req.prompt.first()? else {
        return None;
    };
    match first.text.split_whitespace().next()? {
        "/plan" => Some(PlannerPhase::Planning),
        "/implement" => Some(PlannerPhase::Implementation),
        verb if verb.eq_ignore_ascii_case("implement") => Some(PlannerPhase::Implementation),
        _ => None,
    }
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
    let Some(phase) = command(req) else {
        return Ok(None);
    };
    let mut run = ensure(shared, sid)?;
    let input_id = record_input(&mut run, original)?;
    let first_admission = run.commands.insert(input_id.clone());
    if !first_admission && !queued {
        return Err(format!(
            "planner command {input_id} was already admitted; resume the recorded run instead of replaying it"
        ));
    }
    let resuming = matches!(run.status, RunStatus::Paused | RunStatus::Cancelled);
    run.phase = phase;
    run.coordinator |= shared.with_session(sid, |s| s.coordinator).unwrap_or(false);
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
    let role = match phase {
        PlannerPhase::Planning => PlannerRole::CreatePlan,
        PlannerPhase::Implementation => PlannerRole::SelectPlan,
    };
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
            instructions.push_str("\nThe explicit repository implementation entrypoint is retained for the selected implementation assignment. Selection must complete before invoking it.");
            run.policy_entrypoint(&input_id, &skill);
        }
    }
    if let Some(roadmap) = &run.policy.roadmap {
        instructions.push_str(&format!("\nConfigured roadmap: {}", roadmap.display()));
    }
    if let Some(ContentBlock::Text(first)) = req.prompt.first_mut() {
        let args = first
            .text
            .trim_start()
            .split_once(char::is_whitespace)
            .map(|(_, s)| s.trim_start())
            .unwrap_or("");
        first.text = format!(
            "{instructions}\n[Original user command: {entrypoint_name}, input: {input_id}]\nArguments: {args}"
        );
    }
    let meta = req.meta.get_or_insert_with(Default::default);
    let router_meta = meta.entry("router_acp").or_insert_with(|| json!({}));
    router_meta["planner_role"] = json!(role);
    router_meta["planner_input_id"] = json!(input_id);
    save(shared, sid, &mut run)?;
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

fn record_input(run: &mut PlannerRun, req: &PromptRequest) -> Result<String, String> {
    let id = req
        .meta
        .as_ref()
        .and_then(|m| m.get("router_acp"))
        .and_then(|m| m.get("input_id"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let receipt = InputReceipt {
        origin: if human_prompt(req) { "human" } else { "agent" }.into(),
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

pub fn observe_input(shared: &Arc<Shared>, sid: &str, req: &PromptRequest) -> Result<(), String> {
    if load(shared, sid)?.is_none() {
        return Ok(());
    }
    mutate(shared, sid, |run| {
        if run.status == RunStatus::Complete {
            return Err("planner run is complete; use an explicit /plan or /implement command to start new scope".into());
        }
        if req.meta.as_ref().and_then(|m| m.get("router_acp")).and_then(|m| m.get("planner_input_id")).is_none() {
            record_input(run, req)?;
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
            if phase.is_none() {
                return Err(
                    "planner run is complete; use /plan or /implement for new scope".into(),
                );
            }
            run.status = RunStatus::Running;
        }
        let input_id = record_input(run, req)?;
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
        // A return to planning stops new child dispatch immediately. Model,
        // effort, and command execution wait for this input's own turn.
        if phase == Some(PlannerPhase::Planning) {
            run.phase = PlannerPhase::Planning;
        }
        run.wakes
            .entry(format!("input:{input_id}"))
            .or_insert(Wake {
                reason: "Original parent input awaits its own turn".into(),
                work_id: sid.into(),
                ..Default::default()
            });
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

pub fn parent_input_blocks(
    shared: &Shared,
    sid: &str,
    ids: &[String],
) -> Result<Vec<ContentBlock>, String> {
    let run = load(shared, sid)?.ok_or("planner state missing")?;
    let mut blocks = Vec::new();
    for id in ids {
        let input = run.inputs.get(id).ok_or("unknown original input receipt")?;
        blocks.push(ContentBlock::from(format!(
            "[Original queued input: {id}, origin: {}]\n{}",
            input.origin, input.text
        )));
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
    json!({ "name": TOOL_NAME, "description": "Record durable planner work and revision-bound evidence. Parent actions: status, admit, review, integrate, disposition, acknowledge-input, acknowledge-wake, complete. Worker actions: artifact and finished for its own assignment. Use an idempotency key for every mutation. This tool never grants merge/deploy authority.",
        "inputSchema": { "type": "object", "properties": {
            "action": {"type":"string", "enum":["status","admit","artifact","review","finished","integrate","disposition","acknowledge-input","acknowledge-wake","complete"]},
            "key":{"type":"string"}, "work":{"type":"object"}, "work_id":{"type":"string"}, "attempt_id":{"type":"string"},
            "artifact":{"type":"object"}, "revision":{"type":"string"}, "accepted":{"type":"boolean"}, "evidence":{"type":"array","items":{"type":"string"}},
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
        || !work
            .artifact
            .as_ref()
            .is_some_and(|a| a.revision == revision)
    {
        return Err("current artifact has no acceptance bound to this revision".into());
    }
    Ok(())
}

fn valid_evidence(value: &str) -> bool {
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
    if let Operation::Status { offset } = operation {
        let works = run.works.iter().filter(|(id, _)| actor.is_none_or(|a| a.work_id == **id)).skip(offset).take(32)
            .map(|(id, work)| json!({"work_id":id,"plan_id":work.spec.plan_id,"child_id":work.child_id,"status":work.status,
                "attempt":work.attempt,"workspace":work.workspace,"external_id":work.spec.external_id,
                "revision":work.artifact.as_ref().map(|a| &a.revision),"review":work.review.as_ref().map(|r| json!({"revision":r.revision,"accepted":r.accepted})),
                "reason":work.reason.as_ref().map(|s| s.chars().take(500).collect::<String>())})).collect::<Vec<_>>();
        let inputs = run.inputs.iter().filter(|(_, input)| actor.is_none_or(|a| input.owner.as_deref() == Some(&a.work_id)))
            .skip(offset).take(32).map(|(id, input)| json!({"input_id":id,"origin":input.origin,"owner":input.owner,"acknowledged":input.acknowledged,
                "text":input.text.chars().take(500).collect::<String>(),"attachment_count":input.attachments.len(),"delivered_to":input.delivered_to})).collect::<Vec<_>>();
        let wakes = run
            .wakes
            .iter()
            .filter(|(_, w)| actor.is_none_or(|a| w.work_id == a.work_id))
            .skip(offset)
            .take(32)
            .collect::<BTreeMap<_, _>>();
        return serde_json::to_string(&json!({"revision":run.revision,"policy":run.policy.identity,"phase":run.phase,"status":run.status,
            "works":works,"inputs":inputs,"wakes":wakes,"approval_waits":run.approval_waits,
            "offset":offset,"next_offset":offset+32,"counts":{"works":run.works.len(),"inputs":run.inputs.len(),"wakes":run.wakes.len()}})).map_err(|e| e.to_string());
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
    if actor.is_some() && !worker_operation {
        return Err(
            "implementation children cannot coordinate, accept, integrate, or retire plans".into(),
        );
    }
    if run.status != RunStatus::Running
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
                if work.dependencies.contains(&work.work_id) {
                    return Err("work cannot depend on itself".into());
                }
                let child_id = format!(
                    "child-{}",
                    &content_hash(&format!("{sid}/{}", work.work_id))[..24]
                );
                run.works.insert(
                    work.work_id.clone(),
                    Work {
                        spec: work,
                        child_id,
                        status: WorkStatus::Admitted,
                        workspace: None,
                        attempt: None,
                        artifact: None,
                        review: None,
                        finish: None,
                        integration: None,
                        reason: None,
                        last_output: None,
                        paused: false,
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
            if artifact.evidence.is_empty() || artifact.evidence.iter().any(|v| !valid_evidence(v))
            {
                return Err("artifact requires usable evidence references".into());
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
                if !artifact.checks.get(check).is_some_and(|r| r == "pass") {
                    return Err(format!("required check `{check}` is not proven pass"));
                }
            }
            // Even equal revisions require fresh review if evidence changes.
            work.artifact = Some(artifact);
            work.review = None;
            work.finish = None;
            work.integration = None;
            work.status = WorkStatus::ReviewPending;
            next_role = Some(PlannerRole::ReviewWork);
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
            next_role = Some(PlannerRole::IntegratePlan);
        }
        Operation::Integrate {
            work_id, receipt, ..
        } => {
            let work = run.works.get_mut(&work_id).ok_or("unknown work identity")?;
            accepted_revision(work, &receipt.revision)?;
            if work.status != WorkStatus::Finished
                || !work
                    .finish
                    .as_ref()
                    .is_some_and(|r| r.revision == receipt.revision)
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
            for required in &work.spec.required_integration_evidence {
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
            work.integration = Some(receipt);
            work.status = WorkStatus::Integrated;
            next_role = Some(PlannerRole::SelectPlan);
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
            work.status = status;
            work.reason = Some(reason);
        }
        Operation::AcknowledgeInput {
            input_id, owner, ..
        } => {
            if let Some(owner) = &owner
                && !run.works.contains_key(owner)
            {
                return Err("input owner is not an admitted work identity".into());
            }
            let input = run
                .inputs
                .get_mut(&input_id)
                .ok_or("unknown input receipt")?;
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
        }
        Operation::AcknowledgeWake { wake_id, .. } => {
            run.wakes
                .get_mut(&wake_id)
                .ok_or("unknown wake receipt")?
                .acknowledged = true;
        }
        Operation::Complete { queue_evidence, .. } => {
            if !valid_evidence(&queue_evidence) {
                return Err("completion requires a verified queue/disposition result".into());
            }
            if run.works.values().any(|w| {
                w.status != WorkStatus::Integrated || w.attempt.as_ref().is_some_and(|a| !a.ended)
            }) || run.inputs.values().any(|i| !i.acknowledged)
                || run.wakes.values().any(|w| !w.acknowledged)
            {
                return Err("completion requires all admitted scope, input and wakes to be reconciled; blocked/watching scope remains explicitly pending".into());
            }
            run.status = RunStatus::Complete;
        }
    }
    run.receipts.insert(key.to_string(), fingerprint);
    save(shared, sid, &mut run)?;
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
    let pid = std::process::id();
    #[cfg(target_os = "linux")]
    if let Some(started) = linux_process_start_identity(pid) {
        return (pid, started);
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

pub(crate) fn owner_alive(attempt: &Attempt) -> bool {
    if attempt.router_pid == 0 {
        return false;
    }
    let pid_alive = std::process::Command::new("kill")
        .args(["-0", &attempt.router_pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if !pid_alive {
        return false;
    }

    #[cfg(target_os = "linux")]
    {
        linux_process_start_identity(attempt.router_pid)
            .is_none_or(|start| start == attempt.router_started_at_ms)
    }
    #[cfg(not(target_os = "linux"))]
    {
        attempt.router_pid != std::process::id()
            || (attempt.router_pid, attempt.router_started_at_ms) == process_identity()
    }
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
    if snapshot.paused {
        return Err(
            "assignment was stopped by the client; explicit child resume is required".into(),
        );
    }
    if snapshot.spec.dependencies.iter().any(|id| {
        !run.works
            .get(id)
            .is_some_and(|w| w.status == WorkStatus::Integrated)
    }) {
        return Err("assignment dependencies are not integrated".into());
    }
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
    let role = if snapshot.status == WorkStatus::Accepted {
        PlannerRole::FinishWork
    } else {
        PlannerRole::ImplementWork
    };
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
    save(shared, sid, &mut run)?;
    let allocation = async {
        let workspace = match snapshot.workspace.clone() {
            Some(workspace) => workspace,
            None => allocate_workspace(shared, sid, &snapshot, &run.policy).await?,
        };
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
            work.reason = Some(
            "attempt stopped without a valid artifact/finishing receipt; reconcile before resuming"
                .into(),
        );
        }
        if failed {
            work.reason = Some(
                "child turn failed; preserve recorded evidence and reconcile before retry".into(),
            );
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
        if run.status != RunStatus::Running || run.phase != PlannerPhase::Implementation {
            return Err("planner run is suspended".into());
        }
        let work = run
            .works
            .get_mut(&actor.work_id)
            .ok_or("unknown work identity")?;
        let attempt = work.attempt.as_mut().ok_or("assignment has no attempt")?;
        if work.paused {
            return Err(
                "assignment was stopped by the client; explicit child resume is required".into(),
            );
        }
        if attempt.id != actor.attempt_id || !attempt.ended {
            return Err("attempt is stale or already busy".into());
        }
        let role = match work.status {
            WorkStatus::Accepted => PlannerRole::FinishWork,
            WorkStatus::Corrections | WorkStatus::Interrupted => PlannerRole::ImplementWork,
            _ => {
                return Err(
                    "parent review must accept or return corrections before another child turn"
                        .into(),
                );
            }
        };
        attempt.ended = false;
        if role == PlannerRole::ImplementWork {
            work.status = WorkStatus::Running;
        }
        context = format!(
            "{}\nwork_id: {}\nattempt_id: {}\nAcceptance/corrections: {:?}",
            run.policy.instructions(role),
            actor.work_id,
            actor.attempt_id,
            work.review
        );
        Ok(())
    })?;
    Ok(context)
}
