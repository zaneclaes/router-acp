//! Local monitor messages use the existing durable parent queue. No adapter,
//! session takeover, provider credential, or second delivery loop is created.

use std::path::Path;

use agent_client_protocol::schema::v1::PromptRequest;
use serde::Serialize;
use serde_json::json;

use crate::planner_adopt::RunStore;
use crate::planner_workflow::{self, PlannerRun, RunStatus};
use crate::state::StateStore;

#[derive(Debug, Serialize)]
pub struct InputStatus {
    pub session: String,
    pub input_id: String,
    pub queued: bool,
    pub delivered: bool,
    pub acknowledged: bool,
    pub run_status: RunStatus,
    pub awaiting_approval: bool,
    pub owner_alive: bool,
    pub origin: String,
    pub withdrawn: Option<planner_workflow::InputWithdrawal>,
}

fn report(run: &PlannerRun, sid: &str, id: &str) -> Result<InputStatus, String> {
    let input = run.inputs.get(id).ok_or("unknown planner input")?;
    Ok(InputStatus {
        session: sid.into(),
        input_id: id.into(),
        queued: run.parent_queue.iter().any(|q| q.input_id == id),
        delivered: input.delivered_to.contains(sid),
        acknowledged: input.acknowledged && withdrawal(run, id).is_none(),
        run_status: run.status,
        awaiting_approval: !run.approval_waits.is_empty(),
        owner_alive: run.owner.is_some_and(|owner| owner.alive()),
        origin: input.origin.clone(),
        withdrawn: withdrawal(run, id),
    })
}

pub fn status(store: &StateStore, sid: &str, id: &str) -> Result<InputStatus, String> {
    let run = store.load(sid)?.ok_or("planner run missing")?;
    report(&run, sid, id)
}

// Use the existing terminal flag and receipt map so cleanup survives writes
// by a running router installed before this command. Public reports distinguish
// withdrawal from parent acknowledgment; original content stays in the input.
pub fn withdrawal(run: &PlannerRun, id: &str) -> Option<planner_workflow::InputWithdrawal> {
    run.receipts
        .get(&format!("input-withdrawal:{id}"))
        .and_then(|value| serde_json::from_str(value).ok())
}

/// Retire every receipt of the repair hold `hold` (`repair-hold:<slug>:…`) that is still in force,
/// with `reason` as its withdrawal. The release ledger calls this when a release bound to that
/// hold records its staging pass, so a lifted hold never outlives its lift and nothing outside
/// the run has to clear it (Chordzy, 2026-11-01: a hold lifted on 10-31 blocked every later
/// selection until another agent intervened). Returns the retired input ids.
pub(crate) fn retire_lifted_hold(run: &mut PlannerRun, hold: &str, reason: &str) -> Vec<String> {
    let Some(slug) = hold
        .strip_prefix("repair-hold:")
        .and_then(|rest| rest.split(':').next())
        .filter(|slug| !slug.is_empty())
    else {
        return Vec::new();
    };
    let prefix = format!("repair-hold:{slug}:");
    let ids = run
        .inputs
        .keys()
        .filter(|key| key.starts_with(&prefix) && withdrawal(run, key).is_none())
        .cloned()
        .collect::<Vec<_>>();
    for id in &ids {
        let receipt = planner_workflow::InputWithdrawal {
            at_ms: chrono::Utc::now().timestamp_millis(),
            reason: reason.into(),
        };
        if let Ok(value) = serde_json::to_string(&receipt) {
            run.receipts.insert(format!("input-withdrawal:{id}"), value);
        }
        if let Some(input) = run.inputs.get_mut(id) {
            input.acknowledged = true;
        }
        if let Some(wake) = run.wakes.get_mut(&format!("input:{id}")) {
            wake.acknowledged = true;
            wake.claim_pid = 0;
        }
    }
    run.parent_queue
        .retain(|queued| !ids.contains(&queued.input_id));
    ids
}

/// Metadata only: no historical prompt text or attachments in a queue census.
pub fn list(store: &StateStore, sid: &str) -> Result<Vec<InputStatus>, String> {
    let run = store.load(sid)?.ok_or("planner run missing")?;
    run.inputs
        .iter()
        .filter(|(_, input)| !input.resolved())
        .map(|(id, _)| report(&run, sid, id))
        .collect()
}

/// Remove selected pending delivery/reconciliation work, retaining the original
/// input and an explicit withdrawal reason. Preview and apply use the same
/// selection; agent-only is the default. Never fake the parent's acknowledgment.
/// With `id`, an already acknowledged input is retired too: it keeps its
/// acknowledgment and gains the withdrawal receipt that marks it no longer in
/// force.
pub fn withdraw(
    store: &StateStore,
    sid: &str,
    cwd: &Path,
    id: Option<&str>,
    include_human: bool,
    reason: &str,
    apply: bool,
) -> Result<Vec<InputStatus>, String> {
    let session = store.get(sid).ok_or("router session missing")?;
    if session.cwd.canonicalize().map_err(|e| e.to_string())?
        != cwd.canonicalize().map_err(|e| e.to_string())?
    {
        return Err("session checkout does not match --cwd".into());
    }
    if apply && (reason.trim().is_empty() || reason.len() > 4096) {
        return Err("withdrawal requires a reason of 1–4096 bytes".into());
    }
    for _ in 0..16 {
        let mut run = store.load(sid)?.ok_or("planner run missing")?;
        if run.superseded_by.is_some() {
            return Err("run was superseded; use its current session".into());
        }
        if let Some(id) = id {
            let input = run.inputs.get(id).ok_or("unknown planner input")?;
            if input.origin != "agent" && !include_human {
                return Err("human-origin input requires --include-human; origin is never inferred from its text".into());
            }
        }
        // A bulk drain takes only unresolved input. An input named by id may
        // also be retired after its acknowledgment, so a standing instruction
        // that is no longer in force (a lifted repair hold, a served
        // correction) stops counting as active. On 2026-11-01 Chordzy's host
        // refused every release selection because a hold lifted on 10-31 and
        // a served correction could not be withdrawn once acknowledged.
        let ids = run
            .inputs
            .iter()
            .filter(|(key, input)| {
                id.is_none_or(|id| id == key.as_str())
                    && (!input.resolved() || (id.is_some() && withdrawal(&run, key).is_none()))
                    && (include_human || input.origin == "agent")
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if !apply {
            return ids.iter().map(|id| report(&run, sid, id)).collect();
        }
        // Validate the entire batch before changing any record.
        for id in &ids {
            if let Some(wake) = run.wakes.get(&format!("input:{id}"))
                && wake.claim_pid != 0
                && planner_workflow::owner_alive(&planner_workflow::Attempt {
                    id: id.clone(),
                    router_pid: wake.claim_pid,
                    router_started_at_ms: wake.claim_started_at_ms,
                    delegate_id: None,
                    ended: false,
                })
            {
                return Err(format!(
                    "input {id} has an active delivery; retry after its turn ends"
                ));
            }
        }
        for id in &ids {
            let receipt = planner_workflow::InputWithdrawal {
                at_ms: chrono::Utc::now().timestamp_millis(),
                reason: reason.into(),
            };
            run.receipts.insert(
                format!("input-withdrawal:{id}"),
                serde_json::to_string(&receipt).map_err(|e| e.to_string())?,
            );
            run.inputs.get_mut(id).unwrap().acknowledged = true;
            if let Some(wake) = run.wakes.get_mut(&format!("input:{id}")) {
                wake.acknowledged = true;
                wake.claim_pid = 0;
            }
        }
        run.parent_queue
            .retain(|queued| !ids.contains(&queued.input_id));
        match store.save(sid, &mut run) {
            Ok(()) => return ids.iter().map(|id| report(&run, sid, id)).collect(),
            Err(e) if e.starts_with("planner state changed concurrently") => continue,
            Err(e) => return Err(e),
        }
    }
    Err("planner state remained contended; retry the withdrawal".into())
}

/// The caller has the same local state-file access as the operator. Messages
/// are nevertheless agent-origin: they cannot authorize scope or change phase.
/// A caller-supplied id makes uncertain delivery safely retryable.
pub fn send(
    store: &StateStore,
    sid: &str,
    cwd: &Path,
    id: &str,
    message: &str,
) -> Result<InputStatus, String> {
    send_from(store, sid, cwd, id, message, None, false)
}

/// An interrupting input cancels the running turn, and the agent's client words each cancelled tool call as a refusal
/// ("The user doesn't want to take this action right now"). On 2026-11-10 an implement parent read ten such cancelled
/// `planner_workflow` acknowledgements as "wake acknowledgements are paused". The frame says what happened.
pub const INTERRUPT_NOTICE: &str = "\nThis input interrupted your previous turn. Any tool call it cancelled, including \
     one reported as \"The user doesn't want to take this action right now\", was cancelled by this interrupt, not \
     refused or paused: repeat the ones you still need.";

/// The text an input is delivered as: the sender frame, the interrupt notice when it interrupts, then the message.
pub fn framed(sender: Option<&str>, interrupt: bool, message: &str) -> String {
    format!(
        "{}{}\n\n{message}",
        crate::external_message::agent_frame(sender),
        if interrupt { INTERRUPT_NOTICE } else { "" }
    )
}

/// `send` naming its sender (`send-input --from`), which the operator sees in
/// the client and the agent sees in the frame. `interrupt` steers it into the
/// running parent turn at once, past in-flight tool calls and the steering
/// interval (`send-input --interrupt`; see `planner_wake::steer_pending`).
pub fn send_from(
    store: &StateStore,
    sid: &str,
    cwd: &Path,
    id: &str,
    message: &str,
    sender: Option<&str>,
    interrupt: bool,
) -> Result<InputStatus, String> {
    let sender = sender
        .map(crate::external_message::validate_sender)
        .transpose()?;
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
    {
        return Err("input id must be 1–128 ASCII letters, digits, -, _, . or :".into());
    }
    if message.trim().is_empty() || message.len() > 64 * 1024 {
        return Err("message must contain 1–65536 bytes of text".into());
    }
    let session = store.get(sid).ok_or("router session missing")?;
    if session.cwd.canonicalize().map_err(|e| e.to_string())?
        != cwd.canonicalize().map_err(|e| e.to_string())?
    {
        return Err("session checkout does not match --cwd".into());
    }
    let mut router = json!({"agent_origin": true, "input_id": id});
    if let Some(sender) = &sender {
        router["sender"] = json!(sender);
    }
    if interrupt {
        router["interrupt"] = json!(true);
    }
    let req: PromptRequest = serde_json::from_value(json!({
        "sessionId": sid,
        "prompt": [{"type": "text", "text": framed(sender.as_deref(), interrupt, message)}],
        "_meta": {"router_acp": router}
    }))
    .map_err(|e| e.to_string())?;
    for _ in 0..16 {
        let mut run = store.load(sid)?.ok_or("planner run missing")?;
        // Validate identity even after delivery. Retrying must never requeue
        // an already delivered input or silently replace its contents.
        if run.inputs.contains_key(id) {
            planner_workflow::record_input(&mut run, &req)?;
            return report(&run, sid, id);
        }
        if run.status != RunStatus::Running || run.superseded_by.is_some() {
            return Err(
                "target planner run is not running; select its current active session".into(),
            );
        }
        if !run.owner.is_some_and(|owner| owner.alive()) {
            return Err("target planner run has no live owner; message was not queued".into());
        }
        planner_workflow::queue_parent_request(&mut run, sid, &req)?;
        match store.save(sid, &mut run) {
            Ok(()) => return report(&run, sid, id),
            Err(e) if e.starts_with("planner state changed concurrently") => continue,
            Err(e) => return Err(e),
        }
    }
    Err("planner state remained contended; retry with the same input id".into())
}
