//! `/goal`: an objective the session keeps working on until it is met.
//!
//! The router owns the goal for every agent and enforces it the same way for all of them,
//! and hands it to the agent's own goal mode where the agent has one:
//!
//! - **Claude** (claude-agent-acp) and **Grok** run `/goal <objective>` natively when the
//!   prompt *is* that command, so the turn that sets the goal is sent as exactly that text.
//! - **Codex** (codex-acp) sets its thread goal through the `_session/goal` extension
//!   request (`{sessionId, action: "set" | "clear", objective}`), then works on it.
//! - **Kimi** and any other agent have no goal mode; the router's own enforcement is the
//!   whole of it.
//!
//! Enforcement: while a goal is active, a turn that ends normally is not the end of the
//! work. Unless the agent's reply ends by affirming the goal, with `GOAL: MET` (or
//! `GOAL: UNMEETABLE`) on its own line followed by `Proof:` and the evidence, the router
//! sends the agent a continuation inside the same client turn and does not answer the
//! client until it does, up to `MAX_CONTINUATIONS` times. An affirmed goal ends `met` or
//! `unmeetable`; one never affirmed in that many continuations ends `stalled`, so the user
//! hears about it instead of a loop running forever. A cancelled turn is never continued.
//!
//! The client is told of every change with a `_router/goal` notification:
//! `{sessionId, goal: {objective, status, continuations} | null}`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::session::Shared;

pub const METHOD: &str = "_router/goal";
/// The Claude and Codex adapters' goal control request.
pub const CONTROL_METHOD: &str = "_session/goal";
/// Continuations of one client turn before an unaffirmed goal is reported stalled.
pub const MAX_CONTINUATIONS: u32 = 12;
/// Proof shorter than this is no proof.
const MIN_PROOF_CHARS: usize = 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Active,
    Met,
    Unmeetable,
    Stalled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Goal {
    pub objective: String,
    pub status: Status,
    /// Continuations sent in the current client turn.
    #[serde(default)]
    pub continuations: u32,
    /// The next turn sets the goal and starts work on it (natively where the agent can).
    #[serde(default)]
    pub kickoff: bool,
}

impl Goal {
    pub fn new(objective: &str) -> Self {
        Self {
            objective: objective.trim().to_string(),
            status: Status::Active,
            continuations: 0,
            kickoff: true,
        }
    }

    pub fn active(&self) -> bool {
        self.status == Status::Active
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Show,
    Clear,
    Set(String),
}

/// A prompt that is a `/goal` command.
pub fn command(text: &str) -> Option<Command> {
    let text = text.trim();
    let rest = text.strip_prefix("/goal")?;
    if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
        return None;
    }
    Some(match rest.trim() {
        "" => Command::Show,
        "clear" | "off" | "none" | "stop" | "cancel" => Command::Clear,
        objective => Command::Set(objective.to_string()),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    /// `/goal <objective>` as the prompt.
    SlashCommand,
    /// `_session/goal` control request.
    ControlRequest,
    /// No goal mode of its own.
    None,
}

/// How an agent (`claude@…`, `codex@…`, …) takes a goal natively.
pub fn family(agent: &str) -> Family {
    match agent.split('@').next().unwrap_or_default() {
        "claude" | "grok" => Family::SlashCommand,
        "codex" => Family::ControlRequest,
        _ => Family::None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Met(String),
    Unmeetable(String),
    /// The reply does not affirm the goal (or affirms it without proof).
    Open,
}

/// What the agent's reply says about the goal: the last `GOAL: MET|UNMEETABLE` line, and
/// the proof after it.
pub fn verdict(reply: &str) -> Verdict {
    let lines: Vec<&str> = reply.lines().collect();
    let Some(at) = lines.iter().rposition(|line| affirmation(line).is_some()) else {
        return Verdict::Open;
    };
    let met = affirmation(lines[at]) == Some(true);
    let proof = lines[at + 1..].join("\n");
    let proof = proof.trim();
    let proof = proof
        .strip_prefix("Proof:")
        .or_else(|| proof.strip_prefix("**Proof:**"))
        .or_else(|| proof.strip_prefix("PROOF:"))
        .unwrap_or(proof)
        .trim()
        .to_string();
    if proof.chars().filter(|c| !c.is_whitespace()).count() < MIN_PROOF_CHARS {
        return Verdict::Open;
    }
    if met {
        Verdict::Met(proof)
    } else {
        Verdict::Unmeetable(proof)
    }
}

/// `Some(true)` for a `GOAL: MET` line, `Some(false)` for `GOAL: UNMEETABLE`.
fn affirmation(line: &str) -> Option<bool> {
    let line = line.trim().trim_matches('*').trim_matches('`').trim();
    let rest = line
        .strip_prefix("GOAL:")
        .or_else(|| line.strip_prefix("Goal:"))?
        .trim()
        .trim_matches('*')
        .trim();
    let word = rest.split(|c: char| !c.is_ascii_alphabetic()).next()?;
    match word.to_ascii_uppercase().as_str() {
        "MET" => Some(true),
        "UNMEETABLE" => Some(false),
        _ => None,
    }
}

/// What every goal turn ends with: how the agent says it is done.
pub fn protocol(objective: &str) -> String {
    format!(
        "[router-acp goal]\nThe session's goal: {objective}\n\
         Keep working until this goal is fully met. Do not end your turn with the goal \
         unmet: take the next step yourself instead of reporting what remains or asking \
         whether to continue. End your turn only when one of these is true, and then end \
         your reply with exactly one of:\n\
         `GOAL: MET` on its own line, then `Proof:` and the concrete evidence that the goal \
         is met (what you ran or checked and what it showed);\n\
         `GOAL: UNMEETABLE` on its own line, then `Proof:` and why no remaining path can \
         meet it (what you tried and what rules each path out).\n\
         The router keeps the session working until you do."
    )
}

/// The prompt that sets the goal and starts work, for an agent of `family`.
pub fn kickoff_text(objective: &str, family: Family) -> String {
    match family {
        Family::SlashCommand => format!("/goal {objective}"),
        Family::ControlRequest | Family::None => {
            format!(
                "Start working toward this goal now.\n\n{}",
                protocol(objective)
            )
        }
    }
}

/// The router's prompt when a turn ended without affirming the goal. The turn appends the
/// goal's terms (`protocol`) as it does to every goal turn.
pub fn continuation_text(objective: &str, continuation: u32) -> String {
    format!(
        "[router-acp goal · continuation {continuation} of {MAX_CONTINUATIONS}]\n\
         Your turn ended, but you did not affirm that the goal is met or cannot be met. \
         The goal is: {objective}\n\
         Continue the work now. If it is already met, verify that and reply with the \
         affirmation and its proof."
    )
}

pub fn notification(router_sid: &str, goal: Option<&Goal>) -> Value {
    json!({
        "sessionId": router_sid,
        "goal": goal.map(|goal| json!({
            "objective": goal.objective,
            "status": goal.status,
            "continuations": goal.continuations,
        })),
    })
}

/// Tell the client what the session's goal is now.
pub fn publish(shared: &Arc<Shared>, router_sid: &str, goal: Option<&Goal>) {
    let params = notification(router_sid, goal);
    if let Some(upstream) = shared.upstream()
        && let Ok(message) = agent_client_protocol::UntypedMessage::new(METHOD, params)
    {
        let _ = upstream.send_notification(message);
    }
}

/// A prompt's text when every block is text (router turn context removed).
pub fn prompt_text(prompt: &[agent_client_protocol::schema::v1::ContentBlock]) -> Option<String> {
    use agent_client_protocol::schema::v1::ContentBlock;
    let mut typed = String::new();
    for block in prompt {
        let ContentBlock::Text(text) = block else {
            return None;
        };
        typed.push_str(&crate::accounts::without_turn_context(&text.text));
        typed.push('\n');
    }
    Some(typed.trim().to_string())
}

/// What a client prompt that is a `/goal` command needs from the session loop.
pub enum Handling {
    /// Answered here; the turn ends without the agent.
    Answered,
    /// The prompt goes to the agent as a goal turn (set, or a native clear).
    Dispatch,
}

/// A `/goal` command from the client: the goal changes here, and the reply or the goal
/// turn follows. `None` when the prompt is not one.
pub fn handle_command(
    shared: &Arc<Shared>,
    router_sid: &str,
    prompt: &[agent_client_protocol::schema::v1::ContentBlock],
) -> Option<Handling> {
    let command = command(&prompt_text(prompt)?)?;
    let pinned_family = shared
        .with_session(router_sid, |s| {
            s.pin.as_ref().map(|p| family(&p.candidate.agent))
        })
        .flatten();
    match command {
        Command::Show => {
            let text = shared
                .with_session(router_sid, |s| s.goal.clone())
                .flatten()
                .map(|goal| {
                    format!(
                        "Current goal ({}): {}",
                        serde_json::to_value(goal.status)
                            .ok()
                            .and_then(|v| v.as_str().map(str::to_string))
                            .unwrap_or_default(),
                        goal.objective
                    )
                })
                .unwrap_or_else(|| "No goal set. `/goal <objective>` sets one.".to_string());
            crate::accounts::emit(shared, router_sid, &text);
            Some(Handling::Answered)
        }
        Command::Clear => {
            let had = shared
                .with_session(router_sid, |s| {
                    let had = s.goal.take().is_some();
                    s.goal_clear_pending = had && pinned_family == Some(Family::SlashCommand);
                    had
                })
                .unwrap_or(false);
            publish(shared, router_sid, None);
            crate::accounts::emit(
                shared,
                router_sid,
                if had {
                    "Goal cleared. The agent will finish normally."
                } else {
                    "No goal was set."
                },
            );
            if had && pinned_family == Some(Family::ControlRequest) {
                send_control(shared, router_sid, None);
            }
            // Claude and Grok clear their own goal mode from the prompt.
            Some(
                if shared
                    .with_session(router_sid, |s| s.goal_clear_pending)
                    .unwrap_or(false)
                {
                    Handling::Dispatch
                } else {
                    Handling::Answered
                },
            )
        }
        Command::Set(objective) => {
            let goal = Goal::new(&objective);
            shared.with_session(router_sid, |s| s.goal = Some(goal.clone()));
            publish(shared, router_sid, Some(&goal));
            Some(Handling::Dispatch)
        }
    }
}

/// Send the pinned Codex-family agent its `_session/goal` control (set or clear).
pub fn send_control(shared: &Arc<Shared>, router_sid: &str, objective: Option<&str>) {
    let Some((conn, down_sid, _)) = shared.pinned_route(router_sid) else {
        return;
    };
    let Ok(request) = agent_client_protocol::UntypedMessage::new(
        CONTROL_METHOD,
        control_params(&down_sid, objective),
    ) else {
        return;
    };
    let session = router_sid.to_string();
    let task_conn = conn.clone();
    let _ = conn.spawn(async move {
        if let Err(error) = task_conn.send_request(request).block_task().await {
            tracing::warn!(session, %error, "agent refused its native goal control");
        }
        Ok(())
    });
}

/// After a turn: whether the goal sends the agent back to work, and with what.
pub enum Followup {
    Done,
    Continue(String),
}

/// The end-of-turn check: an active goal the reply did not affirm (with proof) continues.
pub fn after_turn(shared: &Arc<Shared>, router_sid: &str, ended_normally: bool) -> Followup {
    let Some((goal, output)) = shared
        .with_session(router_sid, |s| {
            s.goal
                .clone()
                .filter(|goal| goal.active())
                .map(|goal| (goal, s.turn_output.clone()))
        })
        .flatten()
    else {
        return Followup::Done;
    };
    if !ended_normally {
        return Followup::Done;
    }
    let (status, notice, next) = match verdict(&output) {
        Verdict::Met(_) => (Status::Met, "router-acp · goal met".to_string(), None),
        Verdict::Unmeetable(_) => (
            Status::Unmeetable,
            "router-acp · goal reported unmeetable".to_string(),
            None,
        ),
        Verdict::Open if goal.continuations >= MAX_CONTINUATIONS => (
            Status::Stalled,
            format!(
                "router-acp · goal not affirmed after {MAX_CONTINUATIONS} continuations; \
                 stopping so you can decide (`/goal` to see it, `/goal clear` to drop it)"
            ),
            None,
        ),
        Verdict::Open => {
            let continuation = goal.continuations + 1;
            (
                Status::Active,
                format!(
                    "router-acp · goal not yet affirmed — continuing ({continuation}/{MAX_CONTINUATIONS})"
                ),
                Some(continuation_text(&goal.objective, continuation)),
            )
        }
    };
    let updated = shared
        .with_session(router_sid, |s| {
            let goal = s.goal.as_mut()?;
            goal.status = status;
            if next.is_some() {
                goal.continuations += 1;
            }
            Some(goal.clone())
        })
        .flatten();
    publish(shared, router_sid, updated.as_ref());
    crate::session::notify_user(shared, router_sid, notice);
    match next {
        Some(text) => Followup::Continue(text),
        None => Followup::Done,
    }
}

/// The `_session/goal` request a Codex-family agent takes.
pub fn control_params(down_sid: &str, goal: Option<&str>) -> Value {
    match goal {
        Some(objective) => json!({"sessionId": down_sid, "action": "set", "objective": objective}),
        None => json!({"sessionId": down_sid, "action": "clear"}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn goal_commands() {
        assert_eq!(command("/goal"), Some(Command::Show));
        assert_eq!(command("  /goal clear "), Some(Command::Clear));
        assert_eq!(command("/goal off"), Some(Command::Clear));
        assert_eq!(
            command("/goal ship the release\nwith smokes green"),
            Some(Command::Set("ship the release\nwith smokes green".into()))
        );
        assert_eq!(command("/goals"), None);
        assert_eq!(command("set a /goal"), None);
    }

    #[test]
    fn families_take_goals_their_own_way() {
        assert_eq!(family("claude@fea5"), Family::SlashCommand);
        assert_eq!(family("grok@3d9b"), Family::SlashCommand);
        assert_eq!(family("codex@d16a"), Family::ControlRequest);
        assert_eq!(family("kimi@1"), Family::None);
        assert_eq!(kickoff_text("x", Family::SlashCommand), "/goal x");
        assert!(kickoff_text("x", Family::None).contains("GOAL: MET"));
    }

    #[test]
    fn only_an_affirmation_with_proof_ends_the_goal() {
        assert_eq!(verdict("I fixed most of it."), Verdict::Open);
        assert_eq!(verdict("Done.\nGOAL: MET"), Verdict::Open, "no proof");
        assert_eq!(
            verdict("GOAL: MET\nProof: ok"),
            Verdict::Open,
            "proof too thin"
        );
        let met = verdict(
            "Summary…\n\n**GOAL: MET**\nProof: staging build 7650 passed every smoke and \
             production build 7580 rolled out with the Datadog gate green.",
        );
        assert!(matches!(met, Verdict::Met(proof) if proof.starts_with("staging build 7650")));
        let unmeetable = verdict(
            "GOAL: UNMEETABLE\nProof: the API key was revoked by the vendor and every \
             remaining path needs a new key that only the operator can issue.",
        );
        assert!(matches!(unmeetable, Verdict::Unmeetable(_)));
        assert_eq!(
            verdict(
                "GOAL: MET\nProof: all 443 unit tests pass in the suite as run today.\nGOAL: NOT YET"
            ),
            Verdict::Met(
                "all 443 unit tests pass in the suite as run today.\nGOAL: NOT YET".into()
            ),
            "an unknown word is no affirmation; the last real one counts"
        );
    }

    #[test]
    fn a_goal_persists_without_its_turn_state() {
        let mut goal = Goal::new(" ship it ");
        assert_eq!(goal.objective, "ship it");
        goal.kickoff = false;
        let back: Goal = serde_json::from_value(serde_json::json!({
            "objective": "ship it", "status": "active"
        }))
        .unwrap();
        assert_eq!(
            back,
            Goal {
                continuations: 0,
                ..goal
            }
        );
        assert_eq!(notification("s", Some(&back))["goal"]["status"], "active");
        assert_eq!(notification("s", None)["goal"], serde_json::Value::Null);
    }
}
