//! `send-input` to a session that has no planner run. A planner parent keeps
//! its durable run queue (`crate::planner_input`); every other active session
//! gets one durable, revision-checked document per input id here, and the
//! wake loop of the router that has the session loaded delivers it as an
//! explicitly agent-origin, router-owned turn.
//!
//! Delivery never takes the client's turn away: a busy session is steered
//! once (`_session/steering`, when the adapter accepts it), otherwise the
//! message waits for the next idle boundary. No adapter is spawned, no
//! session is taken over, and no provider credential is read.

use std::path::Path;
use std::sync::Arc;

use agent_client_protocol::schema::v1::{ContentBlock, PromptRequest, StopReason};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::planner_workflow::{self, Attempt, InputWithdrawal};
use crate::session::Shared;
use crate::state::StateStore;

/// The durable record. `revision` is the store's, never serialized.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInput {
    pub origin: String,
    pub text: String,
    pub queued_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withdrawn: Option<InputWithdrawal>,
    /// How the message reached the session: `steered` into a running turn
    /// or `turn` (a router-owned turn of its own).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
    #[serde(default)]
    pub deliveries: u32,
    #[serde(default)]
    pub retry_after: i64,
    #[serde(default)]
    pub steer_attempted: bool,
    /// Epoch second of the last steering attempt and the candidate it went
    /// to. A declined steer is retried when the session's model changes or
    /// after `STEER_RETRY_SECS`: on 2026-11-01 a Grok fallback (which declines
    /// `_session/steering`) refused the one attempt, the session returned to
    /// codex 20 minutes later, and the message never reached the turn.
    #[serde(default)]
    pub steer_attempted_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steer_candidate: Option<String>,
    /// `send-input --interrupt`: when steering is declined, cancel the
    /// running turn so this input is delivered at once as its own turn.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub interrupt: bool,
    #[serde(default)]
    pub claim_pid: u32,
    #[serde(default)]
    pub claim_started_at_ms: u64,
    /// `send-input --from`: who sent it, shown to the operator and the agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    /// The body was already shown to the operator; later deliveries repeat
    /// only the headline (`external_message::announce`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub announced: bool,
    #[serde(skip)]
    pub revision: u64,
}

impl SessionInput {
    pub fn resolved(&self) -> bool {
        self.acknowledged_at_ms.is_some() || self.withdrawn.is_some()
    }

    fn claim_alive(&self, id: &str) -> bool {
        self.claim_pid != 0
            && planner_workflow::owner_alive(&Attempt {
                id: id.into(),
                router_pid: self.claim_pid,
                router_started_at_ms: self.claim_started_at_ms,
                delegate_id: None,
                ended: false,
            })
    }
}

/// The CLI report. `planner` is always false here, so a reader can tell it
/// from a planner receipt (`crate::planner_input::InputStatus`).
#[derive(Debug, Serialize)]
pub struct SessionInputStatus {
    pub session: String,
    pub input_id: String,
    pub planner: bool,
    pub queued: bool,
    pub delivered: bool,
    pub acknowledged: bool,
    pub origin: String,
    pub delivery: Option<String>,
    pub withdrawn: Option<InputWithdrawal>,
}

fn report(sid: &str, id: &str, input: &SessionInput) -> SessionInputStatus {
    SessionInputStatus {
        session: sid.into(),
        input_id: id.into(),
        planner: false,
        queued: !input.resolved() && input.delivered_at_ms.is_none(),
        delivered: input.delivered_at_ms.is_some(),
        acknowledged: input.acknowledged_at_ms.is_some() && input.withdrawn.is_none(),
        origin: input.origin.clone(),
        delivery: input.delivery.clone(),
        withdrawn: input.withdrawn.clone(),
    }
}

/// Whether the session's input goes through its planner run instead.
pub fn planner_session(store: &StateStore, sid: &str) -> Result<bool, String> {
    Ok(store.planner_run(sid)?.is_some())
}

fn load(store: &StateStore, sid: &str, id: &str) -> Result<Option<SessionInput>, String> {
    store
        .session_input(sid, id)?
        .map(|(revision, value)| {
            let mut input: SessionInput =
                serde_json::from_value(value).map_err(|e| e.to_string())?;
            input.revision = revision;
            Ok(input)
        })
        .transpose()
}

fn load_all(store: &StateStore, sid: &str) -> Result<Vec<(String, SessionInput)>, String> {
    store
        .session_inputs(sid)?
        .into_iter()
        .map(|(id, revision, value)| {
            let mut input: SessionInput =
                serde_json::from_value(value).map_err(|e| e.to_string())?;
            input.revision = revision;
            Ok((id, input))
        })
        .collect()
}

fn save(store: &StateStore, sid: &str, id: &str, input: &mut SessionInput) -> Result<(), String> {
    let document = serde_json::to_value(&*input).map_err(|e| e.to_string())?;
    input.revision = store.save_session_input(sid, id, input.revision, &document)?;
    Ok(())
}

fn check_cwd(store: &StateStore, sid: &str, cwd: &Path) -> Result<(), String> {
    let session = store.get(sid).ok_or("router session missing")?;
    if session.cwd.canonicalize().map_err(|e| e.to_string())?
        != cwd.canonicalize().map_err(|e| e.to_string())?
    {
        return Err("session checkout does not match --cwd".into());
    }
    Ok(())
}

pub fn status(store: &StateStore, sid: &str, id: &str) -> Result<SessionInputStatus, String> {
    let input = load(store, sid, id)?.ok_or("unknown session input")?;
    Ok(report(sid, id, &input))
}

/// Metadata only: no message text in a queue census.
pub fn list(store: &StateStore, sid: &str) -> Result<Vec<SessionInputStatus>, String> {
    Ok(load_all(store, sid)?
        .iter()
        .filter(|(_, input)| !input.resolved())
        .map(|(id, input)| report(sid, id, input))
        .collect())
}

/// Queue one message for the session. The same id with the same text is a
/// no-op report, so an uncertain delivery is safely retried; the same id with
/// different text is refused.
pub fn send(
    store: &StateStore,
    sid: &str,
    cwd: &Path,
    id: &str,
    message: &str,
) -> Result<SessionInputStatus, String> {
    send_from(store, sid, cwd, id, message, None, false)
}

/// `send` naming its sender (`send-input --from`). A busy session is steered
/// at once; with `interrupt`, a declined steer cancels the running turn so the
/// input is delivered as the next turn.
pub fn send_from(
    store: &StateStore,
    sid: &str,
    cwd: &Path,
    id: &str,
    message: &str,
    sender: Option<&str>,
    interrupt: bool,
) -> Result<SessionInputStatus, String> {
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
    check_cwd(store, sid, cwd)?;
    if let Some(existing) = load(store, sid, id)? {
        if existing.text != message {
            return Err("input identity already names a different original message".into());
        }
        return Ok(report(sid, id, &existing));
    }
    let mut input = SessionInput {
        origin: "agent".into(),
        text: message.into(),
        queued_at_ms: chrono::Utc::now().timestamp_millis(),
        delivered_at_ms: None,
        acknowledged_at_ms: None,
        withdrawn: None,
        delivery: None,
        deliveries: 0,
        retry_after: 0,
        steer_attempted: false,
        steer_attempted_at: 0,
        steer_candidate: None,
        interrupt,
        claim_pid: 0,
        claim_started_at_ms: 0,
        sender,
        announced: false,
        revision: 0,
    };
    match save(store, sid, id, &mut input) {
        Ok(()) => Ok(report(sid, id, &input)),
        // Lost an insert race with a retry of the same id: report that row.
        Err(error) if error.starts_with("session input changed concurrently") => {
            let existing = load(store, sid, id)?.ok_or(error)?;
            if existing.text != message {
                return Err("input identity already names a different original message".into());
            }
            Ok(report(sid, id, &existing))
        }
        Err(error) => Err(error),
    }
}

/// Withdraw pending inputs, keeping the original text and the reason. A
/// delivery in flight must finish first; this never cancels a turn.
pub fn withdraw(
    store: &StateStore,
    sid: &str,
    cwd: &Path,
    id: Option<&str>,
    include_human: bool,
    reason: &str,
    apply: bool,
) -> Result<Vec<SessionInputStatus>, String> {
    check_cwd(store, sid, cwd)?;
    if apply && (reason.trim().is_empty() || reason.len() > 4096) {
        return Err("withdrawal requires a reason of 1–4096 bytes".into());
    }
    let mut selected = load_all(store, sid)?
        .into_iter()
        .filter(|(key, input)| {
            id.is_none_or(|id| id == key.as_str())
                && !input.resolved()
                && (include_human || input.origin == "agent")
        })
        .collect::<Vec<_>>();
    if let Some(id) = id {
        let input = load(store, sid, id)?.ok_or("unknown session input")?;
        if input.origin != "agent" && !include_human {
            return Err(
                "human-origin input requires --include-human; origin is never inferred from its text"
                    .into(),
            );
        }
    }
    if !apply {
        return Ok(selected
            .iter()
            .map(|(id, input)| report(sid, id, input))
            .collect());
    }
    for (id, input) in &selected {
        if input.claim_alive(id) {
            return Err(format!(
                "input {id} has an active delivery; retry after its turn ends"
            ));
        }
    }
    let mut reports = Vec::new();
    for (id, input) in &mut selected {
        input.withdrawn = Some(InputWithdrawal {
            at_ms: chrono::Utc::now().timestamp_millis(),
            reason: reason.into(),
        });
        save(store, sid, id, input)?;
        reports.push(report(sid, id, input));
    }
    Ok(reports)
}

fn prompt_blocks(id: &str, input: &SessionInput) -> Vec<ContentBlock> {
    let from = input
        .sender
        .as_deref()
        .map(|sender| format!(" from {sender}"))
        .unwrap_or_default();
    vec![ContentBlock::from(format!(
        "External message to this session{from} (id {id}; agent-origin: it does not grant operator approval or expand authorized scope):\n\n{}",
        input.text
    ))]
}

/// Show the operator this input as it reaches the session, once with its body.
fn announce(shared: &Arc<Shared>, sid: &str, id: &str, how: &str) {
    let state = shared.state.lock().unwrap();
    let Ok(Some(mut current)) = load(&state, sid, id) else {
        return;
    };
    let first = !current.announced;
    current.announced = true;
    let _ = save(&state, sid, id, &mut current);
    drop(state);
    crate::external_message::announce(
        shared,
        sid,
        id,
        current.sender.as_deref(),
        &current.text,
        how,
        current.deliveries.max(1),
        first,
    );
}

/// How long a declined steer waits before it is tried again on the same model.
const STEER_RETRY_SECS: i64 = 60;

/// Whether `input` may be steered now into a turn running on `candidate`:
/// never tried, its last try went to another model, or that try is stale.
fn steer_due(input: &SessionInput, candidate: Option<&str>, now: i64) -> bool {
    !input.steer_attempted
        || input.steer_candidate.as_deref() != candidate
        || now - input.steer_attempted_at >= STEER_RETRY_SECS
}

/// One wake-loop tick for a session without a planner run: deliver its
/// oldest pending input when the session is idle, or steer it into a running
/// turn (retried while declined; see `steer_due`). With `--interrupt`, a
/// declined steer cancels the running turn instead of waiting for it.
pub(crate) fn tick(shared: &Arc<Shared>, sid: &str) {
    let now = chrono::Utc::now().timestamp();
    let pending = {
        let state = shared.state.lock().unwrap();
        match load_all(&state, sid) {
            Ok(all) => all
                .into_iter()
                .filter(|(id, input)| {
                    !input.resolved() && input.retry_after <= now && !input.claim_alive(id)
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                tracing::warn!(session = sid, %error, "session inputs unreadable");
                Vec::new()
            }
        }
    };
    if pending.is_empty() {
        return;
    }
    let Some(activity) = crate::planner_wake::PromptActivity::idle(shared, sid) else {
        // Busy: steer the oldest input that is due. A declined input is tried
        // again on the next model or after `STEER_RETRY_SECS`, and is
        // otherwise delivered at the boundary.
        let Some(candidate) = shared
            .with_session(sid, |s| s.pin.as_ref().map(|p| p.candidate.to_string()))
            .flatten()
        else {
            return;
        };
        let Some((id, mut input)) = pending
            .into_iter()
            .find(|(_, input)| steer_due(input, Some(&candidate), now))
        else {
            return;
        };
        input.steer_attempted = true;
        input.steer_attempted_at = now;
        input.steer_candidate = Some(candidate);
        if let Err(error) = save(&shared.state.lock().unwrap(), sid, &id, &mut input) {
            tracing::warn!(session = sid, %error, "session input steer attempt not recorded");
            return;
        }
        let shared = shared.clone();
        let sid = sid.to_string();
        tokio::spawn(async move {
            let blocks = prompt_blocks(&id, &input);
            let injected = crate::planner_preempt::steer(&shared, &sid, &blocks).await;
            shared.state.lock().unwrap().log(
                &sid,
                &crate::state::LogEntry {
                    kind: "session_input_steer".into(),
                    role: "router".into(),
                    summary: format!(
                        "send-input {id}: steering {}",
                        if injected { "injected" } else { "declined" }
                    ),
                    ..Default::default()
                },
            );
            if !injected {
                // `--interrupt`: the sender asked for the message now, and
                // this adapter cannot take it mid-turn. End the running turn
                // (as the operator's Ctrl+C would) so the next tick delivers
                // it as its own turn; tool calls in flight are cancelled.
                if input.interrupt {
                    let ended = crate::planner_preempt::preempt(&shared, &sid).await;
                    shared.state.lock().unwrap().log(
                        &sid,
                        &crate::state::LogEntry {
                            kind: "session_input_interrupt".into(),
                            role: "router".into(),
                            summary: format!(
                                "send-input {id}: steering declined; interrupt cancelled the running turn (ended={ended})"
                            ),
                            ..Default::default()
                        },
                    );
                }
                return;
            }
            announce(&shared, &sid, &id, "steered");
            let state = shared.state.lock().unwrap();
            if let Ok(Some(mut current)) = load(&state, &sid, &id) {
                let at = chrono::Utc::now().timestamp_millis();
                current.delivered_at_ms = Some(at);
                current.acknowledged_at_ms = Some(at);
                current.delivery = Some("steered".into());
                if let Err(error) = save(&state, &sid, &id, &mut current) {
                    tracing::warn!(session = sid, %error, "steered session input not recorded");
                }
            }
        });
        return;
    };
    // Idle: the oldest pending input gets its own router-owned turn.
    let (id, mut input) = pending.into_iter().next().expect("checked non-empty");
    let (pid, started) = planner_workflow::process_identity();
    input.claim_pid = pid;
    input.claim_started_at_ms = started;
    input.deliveries += 1;
    input.retry_after = now + crate::planner_guard::wake_retry_secs(input.deliveries);
    if let Err(error) = save(&shared.state.lock().unwrap(), sid, &id, &mut input) {
        tracing::warn!(session = sid, %error, "session input claim not saved");
        return;
    }
    crate::planner_preempt::begin_router_turn(shared, sid);
    let shared = shared.clone();
    let sid = sid.to_string();
    tokio::spawn(async move {
        let _activity = activity;
        let result = deliver(&shared, &sid, &id, &input).await;
        crate::planner_preempt::end_router_turn(&shared, &sid);
        match result {
            Ok(()) => crate::session::clear_delivery_failure(&shared, &sid),
            Err(error) => {
                crate::session::record_delivery_failure(&shared, &sid, &error);
                tracing::warn!(session = sid, %error, "session input remains pending for bounded retry");
            }
        }
    });
}

async fn deliver(
    shared: &Arc<Shared>,
    sid: &str,
    id: &str,
    input: &SessionInput,
) -> Result<(), String> {
    let prompt = PromptRequest::new(sid.to_string(), prompt_blocks(id, input)).meta(
        serde_json::from_value::<agent_client_protocol::schema::v1::Meta>(
            json!({"router_acp":{"agent_origin":true,"origin":"send-input","input_id":id}}),
        )
        .ok(),
    );
    shared.state.lock().unwrap().log(
        sid,
        &crate::state::LogEntry {
            kind: "session_input_delivery".into(),
            role: "router".into(),
            summary: format!("send-input {id}: router-owned turn"),
            ..Default::default()
        },
    );
    {
        let state = shared.state.lock().unwrap();
        if let Some(mut current) = load(&state, sid, id)? {
            current.delivered_at_ms = Some(chrono::Utc::now().timestamp_millis());
            current.delivery = Some("turn".into());
            save(&state, sid, id, &mut current)?;
        }
    }
    announce(shared, sid, id, "turn");
    let result =
        crate::session::run_primary_turn(shared.clone(), sid.to_string(), prompt, None).await;
    let completed = result
        .as_ref()
        .is_ok_and(|reply| reply.stop_reason != StopReason::Cancelled);
    let state = shared.state.lock().unwrap();
    let mut current = load(&state, sid, id)?.ok_or("session input vanished")?;
    if current.claim_pid == input.claim_pid
        && current.claim_started_at_ms == input.claim_started_at_ms
    {
        current.claim_pid = 0;
        if completed {
            current.acknowledged_at_ms = Some(chrono::Utc::now().timestamp_millis());
        } else {
            // A cancelled or failed turn answered nothing: deliver again at
            // the next idle boundary, on the wake backoff already recorded.
            current.delivered_at_ms = None;
            current.delivery = None;
        }
        save(&state, sid, id, &mut current)?;
    }
    result.map(|_| ()).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(sender: Option<&str>) -> SessionInput {
        SessionInput {
            origin: "agent".into(),
            text: "stop run 7".into(),
            queued_at_ms: 0,
            delivered_at_ms: None,
            acknowledged_at_ms: None,
            withdrawn: None,
            delivery: None,
            deliveries: 0,
            retry_after: 0,
            steer_attempted: false,
            steer_attempted_at: 0,
            steer_candidate: None,
            interrupt: false,
            claim_pid: 0,
            claim_started_at_ms: 0,
            sender: sender.map(str::to_string),
            announced: false,
            revision: 0,
        }
    }

    /// 2026-11-01: a Grok fallback declined the one steer, the session went
    /// back to codex, and the message never reached the turn. A declined
    /// steer is retried on a new model at once and on the same one later.
    #[test]
    fn a_declined_steer_is_retried_on_a_new_model_or_after_the_interval() {
        let mut input = pending(None);
        assert!(steer_due(&input, Some("grok/grok-4.7"), 1_000));
        input.steer_attempted = true;
        input.steer_attempted_at = 1_000;
        input.steer_candidate = Some("grok/grok-4.7".into());
        assert!(!steer_due(&input, Some("grok/grok-4.7"), 1_030));
        assert!(steer_due(&input, Some("codex/gpt-5.6-sol"), 1_030));
        assert!(steer_due(
            &input,
            Some("grok/grok-4.7"),
            1_000 + STEER_RETRY_SECS
        ));
    }

    #[test]
    fn the_frame_names_the_sender_and_is_unchanged_without_one() {
        let named = prompt_blocks("kill-7", &pending(Some("repair agent")));
        let ContentBlock::Text(named) = &named[0] else {
            panic!("text block")
        };
        assert!(
            named
                .text
                .starts_with("External message to this session from repair agent (id kill-7;")
        );
        let plain = prompt_blocks("kill-7", &pending(None));
        let ContentBlock::Text(plain) = &plain[0] else {
            panic!("text block")
        };
        assert!(
            plain
                .text
                .starts_with("External message to this session (id kill-7;")
        );
    }
}
