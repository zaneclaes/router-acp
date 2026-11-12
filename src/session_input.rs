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
//!
//! A delegate (`<parent>::delegate-<id>`, a planner worker included) takes
//! `send-input` too: the router that runs its parent delivers it through
//! `crate::delegate_message` (see `tick_delegates`).

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
    /// A delegate's input only: `--interrupt` could not be steered, so the
    /// router cancelled the child's running turn. The delivery that follows
    /// is recorded as `interrupted`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cancelled_turn: bool,
    /// A delegate's input only: the planner assignment (`work_id`) the child
    /// worked on when its turn was cancelled. A cancelled first turn closes
    /// the child's session, so the input goes to that assignment instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
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

/// The parent router session of a delegate's state row (`<parent>::delegate-<id>`).
pub fn delegate_parent(sid: &str) -> Option<&str> {
    sid.split_once("::delegate-")
        .filter(|(parent, child)| !parent.is_empty() && !child.is_empty())
        .map(|(parent, _)| parent)
}

/// `--cwd` must name the session's checkout. A delegate also accepts its
/// parent's, which is the checkout its sender (the parent) knows.
fn check_cwd(store: &StateStore, sid: &str, cwd: &Path) -> Result<(), String> {
    let session = store.get(sid).ok_or("router session missing")?;
    let wanted = cwd.canonicalize().map_err(|e| e.to_string())?;
    let parent_cwd = delegate_parent(sid)
        .and_then(|parent| store.get(parent))
        .map(|parent| parent.cwd);
    let matches = |checkout: &Path| checkout.canonicalize().is_ok_and(|c| c == wanted);
    if !matches(&session.cwd) && !parent_cwd.as_deref().is_some_and(matches) {
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
        cancelled_turn: false,
        worker: None,
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

// ---- delegates (`<parent>::delegate-<id>`) ----
//
// A delegate is not a router session of its own, so the wake loop never ticks
// it. The router that runs the parent does (`tick_delegates`, called for every
// loaded session), through `crate::delegate_message`'s targets and delivery:
// a running child is steered; an idle one gets the input as planner worker
// input or a follow-up turn; with `--interrupt`, a declined steer cancels the
// child's running turn and the input is delivered once that turn has ended.
// A child this router does not run keeps its input queued.

/// What `send-input` needs from the delegates this router runs. The router's
/// is `RouterPort` (`crate::delegate_message`); tests stand in for a child.
pub(crate) trait DelegatePort {
    fn runs(&self, parent: &str, child: &str) -> bool;
    fn busy(&self, child: &str) -> bool;
    fn work_id(&self, parent: &str, child: &str) -> Option<String>;
    async fn steer(&self, parent: &str, child: &str, text: &str) -> bool;
    fn cancel(&self, parent: &str, child: &str) -> bool;
    /// Deliver to an idle child (or, with `work_id`, to the planner
    /// assignment whose child session ended). Returns how: `steered`,
    /// `input` or `turn`.
    async fn deliver(
        &self,
        parent: &str,
        child: &str,
        work_id: Option<&str>,
        text: &str,
    ) -> Result<String, String>;
}

struct RouterPort(Arc<Shared>);

impl DelegatePort for RouterPort {
    fn runs(&self, parent: &str, child: &str) -> bool {
        crate::delegate_message::runs(&self.0, parent, child)
    }
    fn busy(&self, child: &str) -> bool {
        crate::delegate_mcp::delegate_turn_running(&self.0, child)
    }
    fn work_id(&self, parent: &str, child: &str) -> Option<String> {
        crate::delegate_message::work_id(&self.0, parent, child)
    }
    async fn steer(&self, parent: &str, child: &str, text: &str) -> bool {
        crate::delegate_message::steer_into(&self.0, parent, child, text).await
    }
    fn cancel(&self, parent: &str, child: &str) -> bool {
        crate::delegate_message::cancel_turn(&self.0, parent, child)
    }
    async fn deliver(
        &self,
        parent: &str,
        child: &str,
        work_id: Option<&str>,
        text: &str,
    ) -> Result<String, String> {
        if let Some(work_id) = work_id {
            crate::delegate_message::worker_input(&self.0, parent, work_id, text).await?;
            return Ok("input".into());
        }
        let reply = crate::delegate_message::deliver_text(&self.0, parent, child, text).await?;
        Ok(reply["delivery"].as_str().unwrap_or("turn").to_string())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum DelegateStep {
    /// Into the running child turn.
    Steer,
    /// At the child's boundary.
    Deliver,
}

/// What a delegate's oldest pending input does now. `None` waits: a child
/// this router does not run (unless an interrupt left its assignment to
/// deliver to), a running turn already cancelled for it, or a declined steer
/// not yet due again.
fn delegate_step(input: &SessionInput, runs: bool, busy: bool, now: i64) -> Option<DelegateStep> {
    if !runs {
        return input.worker.is_some().then_some(DelegateStep::Deliver);
    }
    if !busy {
        return Some(DelegateStep::Deliver);
    }
    if input.cancelled_turn {
        return None;
    }
    steer_due(input, None, now).then_some(DelegateStep::Steer)
}

fn update(
    shared: &Shared,
    sid: &str,
    id: &str,
    change: impl FnOnce(&mut SessionInput),
) -> Result<(), String> {
    let state = shared.state.lock().unwrap();
    let mut current = load(&state, sid, id)?.ok_or("session input vanished")?;
    change(&mut current);
    save(&state, sid, id, &mut current)
}

fn note(shared: &Shared, sid: &str, kind: &str, summary: String) {
    shared.state.lock().unwrap().log(
        sid,
        &crate::state::LogEntry {
            kind: kind.into(),
            role: "router".into(),
            summary,
            ..Default::default()
        },
    );
}

/// One step for the delegate `child` of `parent`: its oldest pending input is
/// steered, delivered, or (`--interrupt`, steer declined) the child's turn is
/// cancelled. `release` runs once the step's bookkeeping is saved, before
/// any wait on the child. Returns what happened, for the log and tests.
pub(crate) async fn step_delegate<P: DelegatePort>(
    shared: &Shared,
    port: &P,
    parent: &str,
    child: &str,
    release: impl FnOnce(),
) -> Option<&'static str> {
    let now = chrono::Utc::now().timestamp();
    let pending = {
        let state = shared.state.lock().unwrap();
        load_all(&state, child)
    };
    let (id, mut input) = match pending {
        Ok(all) => all.into_iter().find(|(id, input)| {
            !input.resolved() && input.retry_after <= now && !input.claim_alive(id)
        })?,
        Err(error) => {
            tracing::warn!(session = child, %error, "delegate inputs unreadable");
            return None;
        }
    };
    let runs = port.runs(parent, child);
    let busy = runs && port.busy(child);
    let step = delegate_step(&input, runs, busy, now)?;
    let text = crate::planner_input::framed(input.sender.as_deref(), input.interrupt, &input.text);
    match step {
        DelegateStep::Steer => {
            input.steer_attempted = true;
            input.steer_attempted_at = now;
            if let Err(error) = save(&shared.state.lock().unwrap(), child, &id, &mut input) {
                tracing::warn!(session = child, %error, "delegate input steer attempt not recorded");
                return None;
            }
            release();
            if port.steer(parent, child, &text).await {
                let at = chrono::Utc::now().timestamp_millis();
                if let Err(error) = update(shared, child, &id, |current| {
                    current.delivered_at_ms = Some(at);
                    current.acknowledged_at_ms = Some(at);
                    current.delivery = Some("steered".into());
                }) {
                    tracing::warn!(session = child, %error, "steered delegate input not recorded");
                }
                note(
                    shared,
                    child,
                    "session_input_steer",
                    format!("send-input {id}: steered into the running delegate turn"),
                );
                return Some("steered");
            }
            if !input.interrupt {
                note(
                    shared,
                    child,
                    "session_input_steer",
                    format!("send-input {id}: steering declined; delivered at the turn's end"),
                );
                return Some("declined");
            }
            // `--interrupt`: the sender needs the child redirected now and its
            // agent cannot take the message mid-turn. End the running turn (as
            // a parent's cancel ends its delegates'); the input is delivered
            // once it has ended. A cancelled first turn closes the child's
            // session, so keep its planner assignment to deliver to.
            let work_id = port.work_id(parent, child);
            if let Err(error) = update(shared, child, &id, |current| {
                current.cancelled_turn = true;
                current.worker = work_id;
            }) {
                tracing::warn!(session = child, %error, "delegate interrupt not recorded");
                return None;
            }
            let cancelled = port.cancel(parent, child);
            note(
                shared,
                child,
                "session_input_interrupt",
                format!(
                    "send-input {id}: steering declined; interrupt cancelled the running delegate turn (sent={cancelled})"
                ),
            );
            Some("cancelled")
        }
        DelegateStep::Deliver => {
            let (pid, started) = planner_workflow::process_identity();
            input.claim_pid = pid;
            input.claim_started_at_ms = started;
            input.deliveries += 1;
            input.retry_after = now + crate::planner_guard::wake_retry_secs(input.deliveries);
            if let Err(error) = save(&shared.state.lock().unwrap(), child, &id, &mut input) {
                tracing::warn!(session = child, %error, "delegate input claim not saved");
                return None;
            }
            release();
            let work_id = if runs { None } else { input.worker.as_deref() };
            let result = port.deliver(parent, child, work_id, &text).await;
            let at = chrono::Utc::now().timestamp_millis();
            let interrupted = input.cancelled_turn;
            let outcome = result.as_ref().map(|how| {
                if interrupted {
                    "interrupted".to_string()
                } else {
                    how.clone()
                }
            });
            if let Err(error) = update(shared, child, &id, |current| {
                if current.claim_pid != input.claim_pid
                    || current.claim_started_at_ms != input.claim_started_at_ms
                {
                    return;
                }
                current.claim_pid = 0;
                if let Ok(how) = &outcome {
                    current.delivered_at_ms = Some(at);
                    current.acknowledged_at_ms = Some(at);
                    current.delivery = Some(how.clone());
                }
            }) {
                tracing::warn!(session = child, %error, "delegate input delivery not recorded");
            }
            match result {
                Ok(how) => {
                    note(
                        shared,
                        child,
                        "session_input_delivery",
                        format!("send-input {id}: delivered to the delegate ({how})"),
                    );
                    Some(if interrupted {
                        "interrupted"
                    } else {
                        "delivered"
                    })
                }
                Err(error) => {
                    tracing::warn!(session = child, %error, "delegate input remains pending for bounded retry");
                    Some("failed")
                }
            }
        }
    }
}

/// One wake-loop tick for the delegates of `parent`, which this router has
/// loaded: one step per child with pending `send-input`.
pub(crate) fn tick_delegates(shared: &Arc<Shared>, parent: &str) {
    let children = match shared.state.lock().unwrap().delegate_input_sessions(parent) {
        Ok(children) => children,
        Err(error) => {
            tracing::debug!(session = parent, %error, "delegate inputs unreadable");
            return;
        }
    };
    for child in children {
        if !shared
            .delegate_inputs_in_flight
            .lock()
            .unwrap()
            .insert(child.clone())
        {
            continue;
        }
        let shared = shared.clone();
        let parent = parent.to_string();
        tokio::spawn(async move {
            let port = RouterPort(shared.clone());
            let release = || {
                shared
                    .delegate_inputs_in_flight
                    .lock()
                    .unwrap()
                    .remove(&child);
            };
            step_delegate(&shared, &port, &parent, &child, release).await;
            shared
                .delegate_inputs_in_flight
                .lock()
                .unwrap()
                .remove(&child);
        });
    }
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
            cancelled_turn: false,
            worker: None,
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

    // ---- delegates ----

    const PARENT: &str = "rtr-parent";
    const CHILD: &str = "rtr-parent::delegate-down-1";

    /// A child the router may run: whether it is running a turn, whether its
    /// agent takes a steer, and every call the step made.
    #[derive(Default)]
    struct FakeChild {
        runs: bool,
        busy: bool,
        accepts_steer: bool,
        work_id: Option<String>,
        calls: std::sync::Mutex<Vec<String>>,
    }

    impl FakeChild {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl DelegatePort for FakeChild {
        fn runs(&self, _: &str, _: &str) -> bool {
            self.runs
        }
        fn busy(&self, _: &str) -> bool {
            self.busy
        }
        fn work_id(&self, _: &str, _: &str) -> Option<String> {
            self.work_id.clone()
        }
        async fn steer(&self, parent: &str, child: &str, text: &str) -> bool {
            self.calls
                .lock()
                .unwrap()
                .push(format!("steer {parent} {child} {text}"));
            self.accepts_steer
        }
        fn cancel(&self, parent: &str, child: &str) -> bool {
            self.calls
                .lock()
                .unwrap()
                .push(format!("cancel {parent} {child}"));
            true
        }
        async fn deliver(
            &self,
            parent: &str,
            child: &str,
            work_id: Option<&str>,
            text: &str,
        ) -> Result<String, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("deliver {parent} {child} {work_id:?} {text}"));
            Ok(if work_id.is_some() { "input" } else { "turn" }.into())
        }
    }

    fn shared() -> (tempfile::TempDir, Arc<Shared>) {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            "state_file: {}\nagents:\n  - name: mock\n    command: {{ type: stdio, command: mock-agent }}\n    \
             model_selection: {{ type: config-option }}\n    models:\n      - {{ id: m, cost_rank: 1 }}\n",
            dir.path().join("state.db").display(),
        );
        let shared = Shared::new(crate::config::Config::from_yaml(&yaml).unwrap()).unwrap();
        (dir, shared)
    }

    fn queue(shared: &Shared, id: &str, interrupt: bool) {
        let mut input = pending(Some("parent session"));
        input.text = "stop and switch tasks".into();
        input.interrupt = interrupt;
        save(&shared.state.lock().unwrap(), CHILD, id, &mut input).unwrap();
    }

    fn receipt(shared: &Shared, id: &str) -> SessionInputStatus {
        status(&shared.state.lock().unwrap(), CHILD, id).unwrap()
    }

    #[test]
    fn a_delegate_id_names_its_parent() {
        assert_eq!(delegate_parent(CHILD), Some(PARENT));
        assert_eq!(delegate_parent(PARENT), None);
        assert_eq!(delegate_parent("::delegate-x"), None);
    }

    #[test]
    fn a_delegate_input_steers_a_running_child_and_waits_for_a_declined_steer() {
        let input = pending(None);
        assert_eq!(
            delegate_step(&input, true, true, 1_000),
            Some(DelegateStep::Steer)
        );
        assert_eq!(
            delegate_step(&input, true, false, 1_000),
            Some(DelegateStep::Deliver)
        );
        // Not this router's child: it stays queued.
        assert_eq!(delegate_step(&input, false, false, 1_000), None);
        let mut tried = pending(None);
        tried.steer_attempted = true;
        tried.steer_attempted_at = 1_000;
        assert_eq!(delegate_step(&tried, true, true, 1_030), None);
        let mut cancelled = pending(None);
        cancelled.cancelled_turn = true;
        assert_eq!(delegate_step(&cancelled, true, true, 1_000), None);
        cancelled.worker = Some("work-1".into());
        assert_eq!(
            delegate_step(&cancelled, false, false, 1_000),
            Some(DelegateStep::Deliver)
        );
    }

    #[tokio::test]
    async fn a_running_child_is_steered_with_the_sender_frame() {
        let (_dir, shared) = shared();
        queue(&shared, "redirect-1", false);
        assert_eq!(
            shared
                .state
                .lock()
                .unwrap()
                .delegate_input_sessions(PARENT)
                .unwrap(),
            vec![CHILD.to_string()]
        );
        let child = FakeChild {
            runs: true,
            busy: true,
            accepts_steer: true,
            ..Default::default()
        };
        let outcome = step_delegate(&shared, &child, PARENT, CHILD, || {}).await;
        assert_eq!(outcome, Some("steered"));
        let calls = child.calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].starts_with(&format!(
            "steer {PARENT} {CHILD} External monitor message from parent session"
        )));
        assert!(calls[0].ends_with("stop and switch tasks"));
        let receipt = receipt(&shared, "redirect-1");
        assert!(receipt.delivered && receipt.acknowledged && !receipt.queued);
        assert_eq!(receipt.delivery.as_deref(), Some("steered"));
        assert!(
            shared
                .state
                .lock()
                .unwrap()
                .delegate_input_sessions(PARENT)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn an_idle_child_gets_the_input_at_its_boundary() {
        let (_dir, shared) = shared();
        queue(&shared, "redirect-2", false);
        let child = FakeChild {
            runs: true,
            ..Default::default()
        };
        assert_eq!(
            step_delegate(&shared, &child, PARENT, CHILD, || {}).await,
            Some("delivered")
        );
        let calls = child.calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].starts_with(&format!("deliver {PARENT} {CHILD} None ")));
        let receipt = receipt(&shared, "redirect-2");
        assert!(receipt.delivered && receipt.acknowledged);
        assert_eq!(receipt.delivery.as_deref(), Some("turn"));
    }

    #[tokio::test]
    async fn a_declined_steer_without_interrupt_waits_for_the_boundary() {
        let (_dir, shared) = shared();
        queue(&shared, "redirect-3", false);
        let child = FakeChild {
            runs: true,
            busy: true,
            ..Default::default()
        };
        assert_eq!(
            step_delegate(&shared, &child, PARENT, CHILD, || {}).await,
            Some("declined")
        );
        // Not retried within the interval, never cancelled.
        assert_eq!(
            step_delegate(&shared, &child, PARENT, CHILD, || {}).await,
            None
        );
        assert_eq!(child.calls().len(), 1);
        assert!(receipt(&shared, "redirect-3").queued);
    }

    #[tokio::test]
    async fn a_child_this_router_does_not_run_keeps_its_input_queued() {
        let (_dir, shared) = shared();
        queue(&shared, "redirect-4", true);
        let child = FakeChild::default();
        assert_eq!(
            step_delegate(&shared, &child, PARENT, CHILD, || {}).await,
            None
        );
        assert!(child.calls().is_empty());
        assert!(receipt(&shared, "redirect-4").queued);
    }

    /// `--interrupt` to a planner delegate whose agent declines the steer: the router
    /// cancels its running turn, then delivers the input to the same planner
    /// assignment once the cancelled first turn closed the child's session.
    #[tokio::test]
    async fn an_interrupt_cancels_a_running_child_that_declines_the_steer() {
        let (_dir, shared) = shared();
        queue(&shared, "redirect-5", true);
        let running = FakeChild {
            runs: true,
            busy: true,
            work_id: Some("work-7".into()),
            ..Default::default()
        };
        assert_eq!(
            step_delegate(&shared, &running, PARENT, CHILD, || {}).await,
            Some("cancelled")
        );
        let calls = running.calls();
        assert_eq!(calls.len(), 2);
        assert!(
            calls[0].starts_with("steer ")
                && calls[0].contains(crate::planner_input::INTERRUPT_NOTICE)
        );
        assert_eq!(calls[1], format!("cancel {PARENT} {CHILD}"));
        assert!(receipt(&shared, "redirect-5").queued);
        // Still running: it is not cancelled twice.
        assert_eq!(
            step_delegate(&shared, &running, PARENT, CHILD, || {}).await,
            None
        );
        let closed = FakeChild::default();
        assert_eq!(
            step_delegate(&shared, &closed, PARENT, CHILD, || {}).await,
            Some("interrupted")
        );
        let calls = closed.calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].starts_with(&format!("deliver {PARENT} {CHILD} Some(\"work-7\") ")));
        let receipt = receipt(&shared, "redirect-5");
        assert!(receipt.delivered && receipt.acknowledged);
        assert_eq!(receipt.delivery.as_deref(), Some("interrupted"));
    }
}
