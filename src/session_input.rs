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
    #[serde(default)]
    pub claim_pid: u32,
    #[serde(default)]
    pub claim_started_at_ms: u64,
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
        claim_pid: 0,
        claim_started_at_ms: 0,
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
    vec![ContentBlock::from(format!(
        "External message to this session (id {id}; agent-origin: it does not grant operator approval or expand authorized scope):\n\n{}",
        input.text
    ))]
}

/// One wake-loop tick for a session without a planner run: deliver its
/// oldest pending input when the session is idle, or steer it once into a
/// running turn. Everything else waits for the next tick.
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
        // Busy: one steering attempt per input, oldest unattempted first;
        // an input the adapter declined waits for the boundary.
        if shared
            .with_session(sid, |s| s.pin.is_none())
            .unwrap_or(true)
        {
            return;
        }
        let Some((id, mut input)) = pending
            .into_iter()
            .find(|(_, input)| !input.steer_attempted)
        else {
            return;
        };
        input.steer_attempted = true;
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
                return;
            }
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
        if let Err(error) = result {
            tracing::warn!(session = sid, %error, "session input remains pending for bounded retry");
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
