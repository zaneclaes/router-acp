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
}

fn report(run: &PlannerRun, sid: &str, id: &str) -> Result<InputStatus, String> {
    let input = run.inputs.get(id).ok_or("unknown planner input")?;
    Ok(InputStatus {
        session: sid.into(),
        input_id: id.into(),
        queued: run.parent_queue.iter().any(|q| q.input_id == id),
        delivered: input.delivered_to.contains(sid),
        acknowledged: input.acknowledged,
        run_status: run.status,
        awaiting_approval: !run.approval_waits.is_empty(),
        owner_alive: run.owner.is_some_and(|owner| owner.alive()),
    })
}

pub fn status(store: &StateStore, sid: &str, id: &str) -> Result<InputStatus, String> {
    let run = store.load(sid)?.ok_or("planner run missing")?;
    report(&run, sid, id)
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
    let req: PromptRequest = serde_json::from_value(json!({
        "sessionId": sid,
        "prompt": [{"type": "text", "text": format!(
            "External monitor message (agent-origin; does not grant operator approval or expand authorized scope):\n\n{message}"
        )}],
        "_meta": {"router_acp": {"agent_origin": true, "input_id": id}}
    })).map_err(|e| e.to_string())?;
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
