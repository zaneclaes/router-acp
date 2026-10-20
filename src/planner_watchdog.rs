//! Turns that finished downstream but never returned.
//!
//! Live failure (2026-10-16, parent `rtr-…-4b0fc8eb`): a router-owned wake
//! turn streamed its final answer at 16:34:07 and the adapter's last frame was
//! a cost-bearing `usage_update` stamped `_claude/origin: task-notification`
//! at 16:34:10. claude-agent-acp had folded a background task's notification
//! into the turn's last model cycle, so the cycle's `result` carried the
//! notification's (autonomous) origin. The adapter files autonomous results
//! away from the user turn and absorbs the trailing `idle` as that result's
//! debt, so the `session/prompt` it owed the router was never answered.
//! `session::send_primary_prompt` waited on it without bound, the wake task
//! never dropped its `PromptActivity`, and for three hours every wake was held
//! and every operator prompt queued behind it.
//!
//! Two defenses live here:
//!
//! 1. **Folded-result settle** (every primary turn): a turn whose adapter has
//!    sent an autonomous-origin result frame, and nothing after it for a short
//!    grace, is over. The router sends `session/cancel` (which settles the
//!    adapter's orphaned turn) and ends the turn with the output it streamed.
//! 2. **Stuck-turn watchdog** (every planner parent and child turn): a turn
//!    with no downstream progress — no session update, callback, tool call or
//!    LLM request/response — for `routers.planner.stuck_turn_secs` is
//!    cancelled; if it does not end within `stuck_turn_cancel_secs` it is
//!    released (the client's prompt answers `cancelled`, the `PromptActivity`
//!    drops) and its adapter process is restarted. Queued operator input then
//!    runs first and pending wakes after, through the ordinary wake loop. A
//!    tool call that keeps reporting is progress; one silent for the limit is
//!    not.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_client_protocol::Error as AcpError;
use agent_client_protocol::schema::v1::{CancelNotification, PromptResponse, StopReason};
use serde_json::Value;

use crate::session::Shared;

#[derive(Debug, Clone)]
pub struct TurnProgress {
    pub at: Instant,
    /// Actual agent output or tool activity in this prompt, excluding adapter notices.
    pub agent_activity: bool,
    /// When the adapter last reported a result frame attributed to an
    /// autonomous cycle, with nothing after it.
    pub autonomous_result_at: Option<Instant>,
    /// Foreground ACP tool calls that have not reported a terminal status.
    pub active_tools: BTreeSet<String>,
}

impl TurnProgress {
    fn new(at: Instant) -> Self {
        Self {
            at,
            agent_activity: false,
            autonomous_result_at: None,
            active_tools: BTreeSet::new(),
        }
    }

    fn note_tool(&mut self, params: &Value) {
        let Some(update) = params.get("update") else {
            return;
        };
        let kind = update.get("sessionUpdate").and_then(Value::as_str);
        if !matches!(kind, Some("tool_call" | "tool_call_update")) {
            return;
        }
        let Some(id) = update.get("toolCallId").and_then(Value::as_str) else {
            return;
        };
        match update.get("status").and_then(Value::as_str) {
            Some("completed" | "failed" | "cancelled") => {
                self.active_tools.remove(id);
            }
            Some("pending" | "in_progress") => {
                self.active_tools.insert(id.into());
            }
            None if kind == Some("tool_call") => {
                self.active_tools.insert(id.into());
            }
            _ => {}
        }
    }
}

/// Any downstream progress for `sid` (a router or delegate session id).
pub fn note_progress(shared: &Shared, sid: &str) {
    let now = Instant::now();
    let mut progress = shared.turn_progress.lock().unwrap();
    let current = progress
        .entry(sid.to_string())
        .or_insert_with(|| TurnProgress::new(now));
    current.at = now;
    current.autonomous_result_at = None;
}

/// A `session/update` from the downstream. A cost-bearing `usage_update`
/// is the adapter's result frame; its `_claude/origin` says whose cycle it
/// closed.
pub fn note_update(shared: &Shared, sid: &str, params: &Value) {
    let autonomous = is_autonomous_result(params);
    let now = Instant::now();
    let mut progress = shared.turn_progress.lock().unwrap();
    let current = progress
        .entry(sid.to_string())
        .or_insert_with(|| TurnProgress::new(now));
    current.at = now;
    current.agent_activity |= is_agent_activity(params);
    current.autonomous_result_at = autonomous.then_some(now);
    current.note_tool(params);
}

pub(crate) fn tools_in_flight(shared: &Shared, sid: &str) -> bool {
    shared
        .turn_progress
        .lock()
        .unwrap()
        .get(sid)
        .is_some_and(|p| !p.active_tools.is_empty())
}

fn is_agent_activity(params: &Value) -> bool {
    params.get("update").is_some_and(|update| {
        match update.get("sessionUpdate").and_then(Value::as_str) {
            Some("tool_call" | "tool_call_update") => true,
            Some("agent_message_chunk" | "agent_thought_chunk") => {
                update
                    .pointer("/_meta/claudeCode/kind")
                    .and_then(Value::as_str)
                    != Some("informational")
            }
            _ => false,
        }
    })
}

pub fn had_agent_activity(shared: &Shared, sid: &str) -> bool {
    shared
        .turn_progress
        .lock()
        .unwrap()
        .get(sid)
        .is_some_and(|p| p.agent_activity)
}

fn is_autonomous_result(params: &Value) -> bool {
    params.get("update").is_some_and(|u| {
        u.get("sessionUpdate").and_then(Value::as_str) == Some("usage_update")
            && u.get("cost").is_some()
            && u.get("_meta")
                .and_then(|m| m.get("_claude/origin"))
                .and_then(|o| o.get("kind"))
                .and_then(Value::as_str)
                .is_some_and(|kind| kind != "human")
    })
}

/// The turn for `sid` starts now: earlier frames belong to earlier turns.
pub fn begin(shared: &Shared, sid: &str) {
    shared
        .turn_progress
        .lock()
        .unwrap()
        .insert(sid.to_string(), TurnProgress::new(Instant::now()));
}

pub fn last_progress(shared: &Shared, sid: &str) -> Option<Instant> {
    shared.turn_progress.lock().unwrap().get(sid).map(|p| p.at)
}

/// How long `sid` has made no downstream progress, counted from `started`
/// at the earliest.
pub fn quiet_for(shared: &Shared, sid: &str, started: Instant) -> Duration {
    last_progress(shared, sid)
        .map_or(started, |at| at.max(started))
        .elapsed()
}

/// The silence after an autonomous result frame that ends the turn. Short
/// enough to beat the stuck-turn watchdog, long enough for a real followup
/// cycle to start streaming.
pub fn folded_result_grace(shared: &Shared) -> Duration {
    let stuck = shared.cfg.routers.planner.stuck_turn_secs;
    let secs = if stuck == 0 {
        30
    } else {
        (stuck / 2).clamp(1, 30)
    };
    Duration::from_secs(secs)
}

/// Whether the adapter closed this turn's last cycle under an autonomous
/// origin (sent after `since`) and has been silent for the grace since.
pub fn folded_result(shared: &Shared, sid: &str, since: Instant) -> bool {
    let grace = folded_result_grace(shared);
    shared
        .turn_progress
        .lock()
        .unwrap()
        .get(sid)
        .and_then(|p| p.autonomous_result_at)
        .is_some_and(|at| at >= since && at.elapsed() >= grace)
}

pub fn stuck_limit(shared: &Shared) -> Option<Duration> {
    let secs = shared.cfg.routers.planner.stuck_turn_secs;
    (secs != 0).then(|| Duration::from_secs(secs))
}

pub fn cancel_grace(shared: &Shared) -> Duration {
    Duration::from_secs(shared.cfg.routers.planner.stuck_turn_cancel_secs)
}

/// A stalled watchdog fired on this parent's current turn. The wake loop
/// redelivers that turn's wakes immediately instead of backing off.
pub fn take_stalled(shared: &Shared, sid: &str) -> bool {
    crate::planner_guard::with_runtime(shared, sid, |rt| std::mem::take(&mut rt.stalled))
}

/// The operator is owed an answer to a permission request: the turn waits
/// on them, not on the adapter.
fn awaiting_operator(shared: &Shared, sid: &str) -> bool {
    crate::planner_workflow::load(shared, sid)
        .ok()
        .flatten()
        .is_some_and(|run| !run.approval_waits.is_empty())
}

/// Run one planner parent turn under the stuck-turn watchdog.
pub async fn guard_parent_turn<F>(
    shared: &Arc<Shared>,
    sid: &str,
    turn: F,
) -> Result<PromptResponse, AcpError>
where
    F: Future<Output = Result<PromptResponse, AcpError>>,
{
    let Some(limit) = stuck_limit(shared) else {
        return turn.await;
    };
    let started = Instant::now();
    begin(shared, sid);
    crate::planner_guard::with_runtime(shared, sid, |rt| rt.stalled = false);
    tokio::pin!(turn);
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let quiet = loop {
        tokio::select! {
            result = &mut turn => return result,
            _ = tick.tick() => {}
        }
        let quiet = quiet_for(shared, sid, started);
        // A turn parked on its router-managed background terminals waits on
        // the router, not the adapter: that wait has its own completion.
        let parked = shared
            .background_interrupts
            .lock()
            .unwrap()
            .contains_key(sid);
        if quiet >= limit && !parked && !awaiting_operator(shared, sid) {
            break quiet;
        }
    };
    let minutes = quiet.as_secs() / 60;
    let span = if minutes > 0 {
        format!("{minutes} min")
    } else {
        format!("{} s", quiet.as_secs())
    };
    tracing::error!(
        session = sid,
        quiet_secs = quiet.as_secs(),
        "planner parent turn stalled: no downstream progress; cancelling it"
    );
    log_notice(
        shared,
        sid,
        &format!("router-acp · parent turn stalled {span}: cancelling"),
    );
    with_runtime_stalled(shared, sid);
    cancel_downstream(shared, sid);
    if let Ok(result) = tokio::time::timeout(cancel_grace(shared), &mut turn).await {
        return result;
    }
    tracing::error!(
        session = sid,
        "stalled planner parent turn ignored its cancel: releasing it and restarting its adapter"
    );
    log_notice(
        shared,
        sid,
        "router-acp · stalled parent turn ignored its cancel: released; restarting its adapter",
    );
    // The turn future is dropped on return; clear what it would have.
    shared.with_session(sid, |s| s.cancelled = true);
    shared.planner_deliveries.lock().unwrap().remove(sid);
    shared.background_interrupts.lock().unwrap().remove(sid);
    restart_adapter(shared, sid).await;
    Ok(PromptResponse::new(StopReason::Cancelled))
}

fn with_runtime_stalled(shared: &Shared, sid: &str) {
    crate::planner_guard::with_runtime(shared, sid, |rt| rt.stalled = true);
}

fn log_notice(shared: &Arc<Shared>, sid: &str, text: &str) {
    shared.state.lock().unwrap().log(
        sid,
        &crate::state::LogEntry {
            kind: "router_notice".into(),
            role: "router".into(),
            summary: text.to_string(),
            ..Default::default()
        },
    );
    crate::session::notify_user(shared, sid, text);
    crate::session::flush_pending_disclosure(shared, sid);
}

/// `session/cancel` to the turn's adapter. The router's own turn keeps
/// waiting, so only a real answer from the adapter ends it within the grace.
fn cancel_downstream(shared: &Arc<Shared>, sid: &str) {
    let pin = shared.with_session(sid, |s| s.pin.clone()).flatten();
    if let Some(pin) = pin
        && let Some(conn) = shared.target_conn(&pin.process_key)
    {
        let _ = conn.send_notification(CancelNotification::new(pin.downstream_sid.clone()));
    }
}

/// Replace the adapter that holds the stalled turn, then reopen the session
/// on the same model at its next turn (a fresh downstream session with the
/// SQLite history lookup, the dead-downstream path).
async fn restart_adapter(shared: &Arc<Shared>, sid: &str) {
    let Some(pin) = shared.with_session(sid, |s| s.pin.clone()).flatten() else {
        return;
    };
    match crate::downstream::restart_stalled(shared, &pin.process_key).await {
        Ok(()) => {
            tracing::warn!(session = sid, target = %pin.process_key, "restarted the adapter of a stalled turn")
        }
        Err(error) => {
            tracing::error!(session = sid, target = %pin.process_key, %error, "could not restart the adapter of a stalled turn");
            // Still drop the stalled route: its session can never answer.
            shared.mark_target_dead(&pin.process_key, "stalled turn ignored its cancel");
        }
    }
    shared.with_session(sid, |s| {
        if s.pending_switch.is_none() {
            s.pending_switch = Some(crate::session::SwitchRequest {
                target: pin.candidate.clone(),
                reason: "stalled turn: adapter restarted".into(),
                handoff: crate::session::HandoffStyle::Full,
                user_pick: false,
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_warnings_are_not_agent_work_but_tools_and_agent_chunks_are() {
        let warning = serde_json::json!({"update": {"sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": "hook failed"},
            "_meta": {"claudeCode": {"kind": "informational", "level": "warning"}}}});
        assert!(!is_agent_activity(&warning));
        for kind in [
            "tool_call",
            "tool_call_update",
            "agent_message_chunk",
            "agent_thought_chunk",
        ] {
            assert!(is_agent_activity(
                &serde_json::json!({"update": {"sessionUpdate": kind}})
            ));
        }
        assert!(!is_agent_activity(
            &serde_json::json!({"update": {"sessionUpdate": "usage_update"}})
        ));
    }

    #[test]
    fn all_parallel_tools_must_finish_before_automatic_steering() {
        let mut progress = TurnProgress::new(Instant::now());
        let update = |id: &str, status: &str| {
            serde_json::json!({"update": {
            "sessionUpdate":"tool_call_update", "toolCallId":id, "status":status }})
        };
        progress.note_tool(&update("edit", "pending"));
        progress.note_tool(&update("git", "in_progress"));
        progress.note_tool(&serde_json::json!({"update": {
            "sessionUpdate":"tool_call_update", "toolCallId":"edit", "content":[] }}));
        progress.note_tool(&update("edit", "completed"));
        assert_eq!(progress.active_tools, BTreeSet::from(["git".to_string()]));
        progress.note_tool(&update("git", "failed"));
        progress.note_tool(&update("already-finished", "completed"));
        assert!(progress.active_tools.is_empty());
    }

    #[test]
    fn only_a_cost_bearing_autonomous_usage_update_is_a_folded_result() {
        let human = serde_json::json!({"update": {"sessionUpdate": "usage_update", "cost": {"amount": 1.0},
            "_meta": {"_claude/origin": {"kind": "human"}}}});
        let folded = serde_json::json!({"update": {"sessionUpdate": "usage_update", "cost": {"amount": 1.0},
            "_meta": {"_claude/origin": {"kind": "task-notification", "producer": "session-task"}}}});
        let midturn = serde_json::json!({"update": {"sessionUpdate": "usage_update", "used": 1,
            "_meta": {"_claude/origin": {"kind": "task-notification"}}}});
        // The frames as claude-agent-acp 0.86.0 sent them on the live parent.
        assert!(!is_autonomous_result(&human));
        assert!(is_autonomous_result(&folded));
        assert!(!is_autonomous_result(&midturn));
        let chunk = serde_json::json!({"update": {"sessionUpdate": "agent_message_chunk"}});
        assert!(!is_autonomous_result(&chunk));
    }
}
