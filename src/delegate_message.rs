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

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use crate::session::Shared;

pub const METHOD: &str = "_router/delegate_message";
/// How long a child's agent gets to accept a steer.
const STEER_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BYTES: usize = 64 * 1024;

/// The live delegate a client names: its router delegate id, connection key, downstream
/// session and planner identity.
struct Target {
    delegate_id: String,
    process_key: crate::downstream::ProcessKey,
    downstream_sid: String,
    planner: Option<crate::planner_workflow::WorkerIdentity>,
}

fn target(shared: &Shared, parent: &str, child: &str) -> Option<Target> {
    shared
        .live_delegates
        .lock()
        .unwrap()
        .iter()
        .find(|(_, delegate)| delegate.parent_sid == parent && delegate.sub_sid == child)
        .map(|(id, delegate)| Target {
            delegate_id: id.clone(),
            process_key: delegate.process_key.clone(),
            downstream_sid: delegate.downstream_sid.clone(),
            planner: delegate.planner.clone(),
        })
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
    let target = target(shared, parent, child)
        .ok_or("this child is not running in this router, so it takes no messages")?;
    let log = |delivery: &str| {
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
    };

    if steer(shared, &target, text).await {
        log("steered");
        return Ok(json!({"delivery": "steered"}));
    }
    if let Some(identity) = &target.planner {
        let run =
            crate::planner_workflow::load(shared, parent)?.ok_or("planner parent is not loaded")?;
        let child_id = run
            .works
            .get(&identity.work_id)
            .map(|work| work.child_id.clone())
            .ok_or("assignment disappeared")?;
        let reply = crate::planner_client::act(
            shared,
            json!({
                "sessionId": parent,
                "child_id": child_id,
                "action": "prompt",
                "prompt": [{"type": "text", "text": text}],
                "_meta": {"router_acp": {"input_id": uuid::Uuid::new_v4().to_string()}},
            }),
        )
        .await?;
        log("input");
        return Ok(json!({"delivery": "input", "reply": reply}));
    }
    let args = crate::delegate_mcp::DelegateFollowupArgs {
        delegate_id: target.delegate_id,
        message: text.to_string(),
        background: true,
        ..Default::default()
    };
    let job = crate::delegate_mcp::start_background_followup(shared, parent, args)?;
    Ok(json!({"delivery": "turn", "job": job}))
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
