//! Operator input preempts router-owned planner turns.
//!
//! A planner parent is mostly driven by router-owned wake turns (child
//! wakes, heartbeats, capacity, audit, in-flight and gate wakes). Queuing an
//! operator prompt behind them starved it: on one run the operator's prompts
//! sat unanswered for over an hour while wake turns ran back to back. So a
//! client prompt that arrives while a router-owned turn holds the parent:
//!
//! 1. is steered into that turn with `_session/steering` (claude-agent-acp and
//!    codex-acp inject it at the turn's next step; the request opts into
//!    `promptRequired`, so an adapter whose turn already settled says so
//!    instead of starting a detached turn); otherwise
//! 2. cancels the router-owned turn and runs as the very next turn. The
//!    cancelled turn's wakes stay pending (router-owned ones are not
//!    acknowledged) and are redelivered right after it.
//!
//! An operator turn in flight keeps the queue (see `session::on_prompt`), and
//! the wake loop always delivers queued operator input before any wake.

use std::time::Duration;

use agent_client_protocol::schema::v1::{CancelNotification, ContentBlock};
use serde_json::{Value, json};

use crate::planner_guard::with_runtime;
use crate::session::Shared;

/// How long a steer may take before the router cancels instead.
const STEER_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a cancelled router-owned turn may take to end.
const CANCEL_TIMEOUT: Duration = Duration::from_secs(60);

/// A wake delivery is about to hold the parent.
pub fn begin_router_turn(shared: &Shared, sid: &str) {
    with_runtime(shared, sid, |rt| {
        rt.router_turn = true;
        rt.preempted = false;
    });
}

/// The router-owned turn is over.
pub fn end_router_turn(shared: &Shared, sid: &str) {
    with_runtime(shared, sid, |rt| {
        rt.router_turn = false;
        rt.preempted = false;
        rt.steered.clear();
    });
}

pub fn router_turn_in_flight(shared: &Shared, sid: &str) -> bool {
    shared
        .planner_runtime
        .lock()
        .unwrap()
        .get(sid)
        .is_some_and(|rt| rt.router_turn)
}

/// Whether the router-owned turn now ending was cancelled for operator input.
pub fn preempted(shared: &Shared, sid: &str) -> bool {
    shared
        .planner_runtime
        .lock()
        .unwrap()
        .get(sid)
        .is_some_and(|rt| rt.preempted)
}

/// Steer operator input into the running router-owned turn. `true` when the
/// downstream took it.
pub async fn steer(shared: &Shared, sid: &str, prompt: &[ContentBlock]) -> bool {
    let Some((conn, down_sid, _)) = shared.pinned_route(sid) else {
        return false;
    };
    let Ok(request) = agent_client_protocol::UntypedMessage::new(
        "_session/steering",
        json!({"sessionId": down_sid, "prompt": prompt,
            "_meta": {"steering": {"idleBehavior": "promptRequired"}}}),
    ) else {
        return false;
    };
    let reply = tokio::time::timeout(STEER_TIMEOUT, conn.send_request(request).block_task()).await;
    let outcome = match &reply {
        Ok(Ok(value)) => value
            .get("outcome")
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    };
    tracing::info!(
        session = sid,
        ?outcome,
        "operator input steered at a router-owned planner turn"
    );
    // `startedNewTurn` (an adapter that ignored the opt-in) also delivered it.
    matches!(outcome.as_deref(), Some("injected" | "startedNewTurn"))
}

/// Cancel the router-owned turn for operator input and wait for it to end.
/// `false` when it did not end in time (the input must then queue).
pub async fn preempt(shared: &Shared, sid: &str) -> bool {
    with_runtime(shared, sid, |rt| rt.preempted = true);
    let pin = shared
        .with_session(sid, |s| {
            s.cancelled = true;
            s.pin.clone()
        })
        .flatten();
    if let Some(interrupt) = shared.background_interrupts.lock().unwrap().remove(sid) {
        let _ = interrupt.send(crate::session::BackgroundInterrupt::Cancel);
    }
    if let Some(pin) = pin
        && let Some(conn) = shared.target_conn(&pin.process_key)
    {
        let _ = conn.send_notification(CancelNotification::new(pin.downstream_sid.clone()));
    }
    tracing::warn!(
        session = sid,
        "operator input preempts a router-owned planner turn: cancelled it; the operator's prompt runs next"
    );
    let started = std::time::Instant::now();
    while router_turn_in_flight(shared, sid) {
        if started.elapsed() >= CANCEL_TIMEOUT {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    true
}
