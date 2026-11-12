//! `_router/delegate_message`: the operator types a message into a child session's input box
//! in the client (kory shows a child the way it shows the session itself).
//!
//! Request params: `{sessionId, delegateSessionId, text}`, where `sessionId` is the parent
//! router session and `delegateSessionId` the child's state row (`<parent>::delegate-<id>`,
//! the id `state-query delegates` reports). The answer says how it was delivered:
//!
//! - `steered`: the child was working and its agent took the message into the running turn
//!   (`_session/steering`).
//! - `input`: an idle planner worker. The message becomes durable human input owned by the
//!   worker's assignment, through the same path a client's planner-child prompt takes, so the
//!   parent reconciles it under the run's own rules.
//! - `turn`: any other idle delegate gets a follow-up turn, run in the background like a
//!   parent's `delegate_followup` with `background: true`.
//!
//! A child this router does not run (ended, or another router's) takes no messages.
//!
//! `send-input` to a delegate (`crate::session_input`) delivers through the same targets,
//! steering and delivery (`steer_into`, `deliver_text`, `worker_input`), and cancels a running
//! child turn with `cancel_turn` when `--interrupt` cannot be steered.

use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::CancelNotification;
use serde_json::{Value, json};

use crate::session::Shared;

pub const METHOD: &str = "_router/delegate_message";
/// How long a child's agent gets to accept a steer.
const STEER_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BYTES: usize = 64 * 1024;

/// The live delegate a client names: its router delegate id (absent while a delegate's first
/// turn runs: `live_delegates` holds it only between turns), connection key, downstream
/// session and planner assignment.
struct Target {
    delegate_id: Option<String>,
    process_key: crate::downstream::ProcessKey,
    downstream_sid: String,
    work_id: Option<String>,
}

fn target(shared: &Shared, parent: &str, child: &str) -> Option<Target> {
    let live = shared
        .live_delegates
        .lock()
        .unwrap()
        .iter()
        .find(|(_, delegate)| delegate.parent_sid == parent && delegate.sub_sid == child)
        .map(|(id, delegate)| Target {
            delegate_id: Some(id.clone()),
            process_key: delegate.process_key.clone(),
            downstream_sid: delegate.downstream_sid.clone(),
            work_id: delegate.planner.as_ref().map(|p| p.work_id.clone()),
        });
    if live.is_some() {
        return live;
    }
    // A delegate in its first turn: the parent's open delegate sessions name it.
    let down = child.strip_prefix(&format!("{parent}::delegate-"))?;
    let process_key = shared
        .with_session(parent, |s| {
            s.delegates
                .iter()
                .find(|handle| handle.downstream_sid == down)
                .map(|handle| handle.process_key.clone())
        })
        .flatten()?;
    let work_id = shared
        .planner_child_routes
        .lock()
        .unwrap()
        .get(&(process_key.clone(), down.to_string()))
        .filter(|route| route.parent_sid == parent)
        .map(|route| route.work_id.clone());
    Some(Target {
        delegate_id: None,
        process_key,
        downstream_sid: down.to_string(),
        work_id,
    })
}

/// Whether this router runs the child `child` of `parent`.
pub(crate) fn runs(shared: &Shared, parent: &str, child: &str) -> bool {
    target(shared, parent, child).is_some()
}

/// The planner assignment (`work_id`) the child works on, if it is a planner worker.
pub(crate) fn work_id(shared: &Shared, parent: &str, child: &str) -> Option<String> {
    target(shared, parent, child)?.work_id
}

/// The request's text, checked: present, not blank, at most 64 KiB.
fn text(params: &Value) -> Result<&str, String> {
    let text = params
        .get("text")
        .and_then(Value::as_str)
        .ok_or("text required")?;
    if text.trim().is_empty() || text.len() > MAX_BYTES {
        return Err("message must contain 1–65536 bytes of text".into());
    }
    Ok(text)
}

pub async fn send(shared: &Arc<Shared>, params: &Value) -> Result<Value, String> {
    let parent = params
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or("exact parent sessionId required")?;
    let child = params
        .get("delegateSessionId")
        .and_then(Value::as_str)
        .ok_or("exact delegateSessionId required")?;
    let text = text(params)?;
    deliver_text(shared, parent, child, text).await
}

fn log(shared: &Shared, child: &str, text: &str, delivery: &str) {
    shared.state.lock().unwrap().log(
        child,
        &crate::state::LogEntry {
            kind: "operator_message".into(),
            role: "user".into(),
            summary: text.chars().take(200).collect(),
            detail: Some(json!({"text": text, "delivery": delivery})),
            tokens_estimated: true,
            ..Default::default()
        },
    );
}

/// Deliver `text` to the child: steered into its running turn, else as durable input of an
/// idle planner worker, else as a background follow-up turn. The answer's `delivery` says
/// which.
pub(crate) async fn deliver_text(
    shared: &Arc<Shared>,
    parent: &str,
    child: &str,
    text: &str,
) -> Result<Value, String> {
    let target = target(shared, parent, child)
        .ok_or("this child is not running in this router, so it takes no messages")?;
    if steer(shared, &target, text).await {
        log(shared, child, text, "steered");
        return Ok(json!({"delivery": "steered"}));
    }
    if let Some(work_id) = &target.work_id {
        let reply = worker_input(shared, parent, work_id, text).await?;
        log(shared, child, text, "input");
        return Ok(json!({"delivery": "input", "reply": reply}));
    }
    let delegate_id = target
        .delegate_id
        .ok_or("this child is in its first turn and declined the message")?;
    let args = crate::delegate_mcp::DelegateFollowupArgs {
        delegate_id,
        message: text.to_string(),
        background: true,
        ..Default::default()
    };
    let job = crate::delegate_mcp::start_background_followup(shared, parent, args)?;
    log(shared, child, text, "turn");
    Ok(json!({"delivery": "turn", "job": job}))
}

/// Durable input for the planner assignment `work_id`, through the same path a client's
/// planner-child prompt takes. It also reaches an assignment whose delegate session ended
/// (an interrupted first turn): the run resumes the same durable child with it.
pub(crate) async fn worker_input(
    shared: &Arc<Shared>,
    parent: &str,
    work_id: &str,
    text: &str,
) -> Result<Value, String> {
    let run =
        crate::planner_workflow::load(shared, parent)?.ok_or("planner parent is not loaded")?;
    let child_id = run
        .works
        .get(work_id)
        .map(|work| work.child_id.clone())
        .ok_or("assignment disappeared")?;
    crate::planner_client::act(
        shared,
        json!({
            "sessionId": parent,
            "child_id": child_id,
            "action": "prompt",
            "prompt": [{"type": "text", "text": text}],
            "_meta": {"router_acp": {"input_id": uuid::Uuid::new_v4().to_string()}},
        }),
    )
    .await
}

/// Steer `text` into the child's running turn; false when it is idle, not ours, or its agent
/// declined.
pub(crate) async fn steer_into(shared: &Shared, parent: &str, child: &str, text: &str) -> bool {
    match target(shared, parent, child) {
        Some(target) => steer(shared, &target, text).await,
        None => false,
    }
}

/// End the child's running turn, the way a parent's cancel ends its delegates' turns
/// (`session/cancel` to the child's downstream session). False when this router does not
/// run the child or its agent connection is gone.
pub(crate) fn cancel_turn(shared: &Shared, parent: &str, child: &str) -> bool {
    let Some(target) = target(shared, parent, child) else {
        return false;
    };
    let Some(conn) = shared.target_conn(&target.process_key) else {
        return false;
    };
    conn.send_notification(CancelNotification::new(target.downstream_sid))
        .is_ok()
}

/// Into the child's running turn; false when it is idle or its agent declined.
async fn steer(shared: &Shared, target: &Target, text: &str) -> bool {
    let Some(conn) = shared.target_conn(&target.process_key) else {
        return false;
    };
    let Ok(request) = agent_client_protocol::UntypedMessage::new(
        "_session/steering",
        json!({"sessionId": target.downstream_sid,
            "prompt": [{"type": "text", "text": text}],
            "_meta": {"steering": {"idleBehavior": "promptRequired"}}}),
    ) else {
        return false;
    };
    let reply = tokio::time::timeout(STEER_TIMEOUT, conn.send_request(request).block_task()).await;
    let outcome = match &reply {
        Ok(Ok(value)) => value.get("outcome").and_then(Value::as_str),
        _ => None,
    };
    matches!(outcome, Some("injected" | "startedNewTurn"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_must_have_text_within_the_limit() {
        assert!(text(&json!({})).is_err());
        assert!(text(&json!({"text": "  \n"})).is_err());
        assert!(text(&json!({"text": "x".repeat(MAX_BYTES + 1)})).is_err());
        assert_eq!(text(&json!({"text": "keep going"})).unwrap(), "keep going");
    }
}
