//! Router-owned conversation restoration. Provider session files are optional.

use std::sync::Arc;

use agent_client_protocol::schema::v1::{ContentBlock, Error as AcpError, Meta};
use serde::{Deserialize, Serialize};

use crate::candidate::{CandidateId, EffortLevel, TaskClass};
use crate::config::{PlannerPhase, StrategyKind};
use crate::session::{RouterSession, Shared};
use crate::state::LogEntry;
use crate::strategies::OverrideSource;

/// Only durable routing state belongs here. Connections, login state, locks,
/// in-flight tools and cancellation belong to the current process.
#[derive(Serialize, Deserialize)]
pub struct SessionConfig {
    strategy: StrategyKind,
    candidate: Option<String>,
    candidate_user_pick: bool,
    effort: Option<EffortLevel>,
    version: Option<String>,
    preferred_candidate: Option<String>,
    mode: Option<String>,
    excluded: Vec<String>,
    run_label: Option<String>,
    planner_phase: Option<PlannerPhase>,
    coordinator: bool,
    preclass_done: bool,
    preclass_profile: Option<crate::classifier::TaskProfile>,
    required_mcp_capabilities: Vec<String>,
    injected_tickets: Vec<String>,
    task_class: Option<TaskClass>,
    task_complexity: f64,
    elevation: Option<String>,
    elevation_skill: Option<String>,
    quiet_turns: u32,
    escalations_done: u32,
    struggle: f64,
}

impl SessionConfig {
    pub fn capture(s: &RouterSession) -> Self {
        let candidate = s
            .pending_switch
            .as_ref()
            .map(|sw| &sw.target)
            .or_else(|| s.pin.as_ref().map(|pin| &pin.candidate))
            .or(s.candidate_override.as_ref());
        Self {
            strategy: s.strategy,
            candidate: candidate.map(ToString::to_string),
            candidate_user_pick: s.pending_switch.as_ref().map(|sw| sw.user_pick).unwrap_or(
                s.pin_user_pick
                    || matches!(s.candidate_override_source, Some(OverrideSource::UserPick)),
            ),
            effort: s.effort_request,
            version: s.version_request.clone(),
            preferred_candidate: s.preferred_candidate.as_ref().map(ToString::to_string),
            mode: s.pending_mode.clone().or_else(|| s.applied_mode.clone()),
            excluded: s.excluded.clone(),
            run_label: s.run_label.clone(),
            planner_phase: s.planner_phase,
            coordinator: s.coordinator,
            preclass_done: s.preclass_done,
            preclass_profile: s.preclass_profile.clone(),
            required_mcp_capabilities: s.required_mcp_capabilities.clone(),
            injected_tickets: s.injected_tickets.iter().cloned().collect(),
            task_class: s.task_class,
            task_complexity: s.task_complexity,
            elevation: s.elevation.clone(),
            elevation_skill: s.elevation_skill.clone(),
            quiet_turns: s.quiet_turns,
            escalations_done: s.escalations_done,
            struggle: s.struggle,
        }
    }

    pub fn apply(self, s: &mut RouterSession) {
        s.strategy = self.strategy;
        s.candidate_override = self.candidate.and_then(|c| CandidateId::parse(&c));
        s.pin_user_pick = self.candidate_user_pick;
        s.candidate_override_source = self.candidate_user_pick.then_some(OverrideSource::UserPick);
        s.effort_request = self.effort;
        s.version_request = self.version;
        s.preferred_candidate = self
            .preferred_candidate
            .and_then(|c| CandidateId::parse(&c));
        s.pending_mode = self.mode;
        s.excluded = self.excluded;
        s.run_label = self.run_label;
        s.planner_phase = self.planner_phase;
        s.coordinator = self.coordinator;
        s.preclass_done = self.preclass_done;
        s.preclass_profile = self.preclass_profile;
        s.required_mcp_capabilities = self.required_mcp_capabilities;
        s.injected_tickets = self.injected_tickets.into_iter().collect();
        s.task_class = self.task_class;
        s.task_complexity = self.task_complexity;
        s.elevation = self.elevation;
        s.elevation_skill = self.elevation_skill;
        s.quiet_turns = self.quiet_turns;
        s.escalations_done = self.escalations_done;
        s.struggle = self.struggle;
    }
}

pub fn checkpoint(shared: &Arc<Shared>, sid: &str) -> Result<(), AcpError> {
    let value = shared
        .with_session(sid, |s| serde_json::to_value(SessionConfig::capture(s)))
        .ok_or_else(|| AcpError::invalid_params().data("unknown router session id"))?
        .map_err(|e| {
            AcpError::internal_error().data(format!("cannot encode session configuration: {e}"))
        })?;
    shared
        .state
        .lock()
        .unwrap()
        .set_session_config(sid, &value)
        .map_err(|e| {
            AcpError::internal_error().data(format!("cannot save session configuration: {e}"))
        })
}

pub fn response_meta(shared: &Arc<Shared>, sid: &str) -> Meta {
    let value = shared.with_session(sid, |s| serde_json::json!({
        "candidate": s.pin.as_ref().map(|p| &p.candidate).or(s.candidate_override.as_ref()).map(ToString::to_string),
        "version": s.version_request,
        "effort": s.effort_request,
        "planner_phase": s.planner_phase,
        "coordinator": s.coordinator,
    })).unwrap_or_default();
    Meta::from_iter([("router_acp".to_string(), value)])
}

/// Give a fresh adapter access to its saved conversation without replaying it
/// into the prompt. The agent decides which records it needs, always through
/// the router's command: the files behind it are the router's layout.
pub fn lookup_context(shared: &Arc<Shared>, sid: &str) -> Vec<ContentBlock> {
    let command = crate::session::transcript_command(shared, sid);
    vec![ContentBlock::from(format!(
        "<resumed-conversation-context>\nYou are resuming router session {sid}. \
         Its complete conversation, including user messages, tool calls and results, \
         is saved in router-acp's state store. Retrieve the relevant history yourself \
         before continuing; no prior conversation has been copied into this prompt. \
         Start with the recent records:\n  {command} --limit 40\n\
         Run the same command with a larger --limit to look up more history as needed. \
         Do not open the state database files directly. Read manageable portions rather \
         than loading the entire conversation into context. Inherited-context records \
         retain earlier session history. \
         Verify uncertain tool effects and do not repeat completed actions.\n\
         </resumed-conversation-context>\nThe current request follows."
    ))]
}

/// Keep a complete snapshot in SQLite for Continue, including after the
/// source is deleted. This snapshot is never inserted into an adapter prompt.
pub fn snapshot(shared: &Arc<Shared>, sid: &str) -> Result<Vec<ContentBlock>, AcpError> {
    let entries = shared.state.lock().unwrap().log_for_all(sid).map_err(|e| {
        AcpError::internal_error().data(format!("cannot read router conversation: {e}"))
    })?;
    Ok(history_from_logs(&entries))
}

pub fn restore_config(
    s: &mut RouterSession,
    persisted: &crate::state::PersistedSession,
) -> Result<(), AcpError> {
    if let Some(value) = &persisted.session_config {
        let saved: SessionConfig = serde_json::from_value(value.clone()).map_err(|e| {
            AcpError::internal_error().data(format!("cannot restore session configuration: {e}"))
        })?;
        saved.apply(s);
    } else if !persisted.agent.is_empty() && !persisted.model.is_empty() {
        // Pre-checkpoint databases retain the native pin and its provenance.
        s.candidate_override = Some(CandidateId::new(&persisted.agent, &persisted.model));
        s.candidate_override_source = s.pin_user_pick.then_some(OverrideSource::UserPick);
    }
    Ok(())
}

/// Continue starts new work on the source's settings. Keep what a human chose
/// (strategy, a user-picked model, effort, exclusions, coordinator role) and
/// drop what the source's own work derived, so the first turn routes afresh:
/// a finished ship flow's implementation phase, pre-class verdict, or ship-pr
/// elevation must not skip planning for the new request.
pub fn restore_continued_config(
    s: &mut RouterSession,
    persisted: &crate::state::PersistedSession,
) -> Result<(), AcpError> {
    restore_config(s, persisted)?;
    if !s.pin_user_pick {
        s.candidate_override = None;
        s.candidate_override_source = None;
    }
    s.planner_phase = None;
    s.preclass_done = false;
    s.preclass_profile = None;
    s.task_class = None;
    s.task_complexity = 0.0;
    s.elevation = None;
    s.elevation_skill = None;
    s.quiet_turns = 0;
    s.escalations_done = 0;
    s.struggle = 0.0;
    Ok(())
}

fn stored_blocks(entry: &LogEntry) -> Option<Vec<ContentBlock>> {
    entry
        .detail
        .as_ref()?
        .get("prompt")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
}

fn history_from_logs(entries: &[LogEntry]) -> Vec<ContentBlock> {
    let mut blocks = Vec::new();
    // Old databases contain aggregate assistant rows. New recordings contain
    // raw updates, including interrupted output that never reached turn-end.
    let mut raw_response = false;
    let mut last_raw_tool = None;
    for entry in entries {
        match entry.kind.as_str() {
            "inherited_context" | "context_injection" => {
                if let Some(saved) = stored_blocks(entry) {
                    blocks.extend(saved);
                }
            }
            "user_prompt" | "user_steer" => {
                if entry.kind == "user_prompt" {
                    raw_response = false;
                }
                blocks.push(ContentBlock::from("\nUser:\n".to_string()));
                blocks.extend(
                    stored_blocks(entry)
                        .unwrap_or_else(|| vec![ContentBlock::from(entry.summary.clone())]),
                );
            }
            "session_update" => {
                let Some(update) = entry.detail.as_ref() else {
                    continue;
                };
                let kind = update
                    .get("sessionUpdate")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                match kind {
                    "agent_message_chunk" => {
                        if !raw_response {
                            blocks.push(ContentBlock::from("\nAssistant:\n".to_string()));
                        }
                        raw_response = true;
                        if let Some(content) = update
                            .get("content")
                            .and_then(|v| serde_json::from_value::<ContentBlock>(v.clone()).ok())
                        {
                            blocks.push(content);
                        }
                    }
                    "tool_call" | "tool_call_update" => {
                        let title = update
                            .get("title")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Tool");
                        let status = update
                            .get("status")
                            .and_then(|v| v.as_str())
                            .unwrap_or("pending");
                        blocks.push(ContentBlock::from(format!(
                            "\nTool: {title} [{status}]\n{update}\n"
                        )));
                        if let Some(content) = update.get("content").and_then(|v| v.as_array()) {
                            for block in content {
                                let value = block.get("content").unwrap_or(block).clone();
                                if let Ok(content) = serde_json::from_value::<ContentBlock>(value) {
                                    blocks.push(content);
                                }
                            }
                        }
                        last_raw_tool = Some(update);
                    }
                    "plan" => {
                        blocks.push(ContentBlock::from(format!("\nAssistant plan:\n{update}\n")))
                    }
                    _ => {}
                }
            }
            "agent_response" if !raw_response => blocks.push(ContentBlock::from(format!(
                "\nAssistant:\n{}",
                entry.summary
            ))),
            "tool_call" if entry.detail.as_ref() != last_raw_tool => {
                blocks.push(ContentBlock::from(format!(
                    "\nTool: {}\n{}",
                    entry.summary,
                    entry
                        .detail
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                )));
            }
            kind if kind.starts_with("fs_") || kind.starts_with("terminal_") => {
                blocks.push(ContentBlock::from(format!(
                    "\nTool request [completion unknown]: {}\n{}",
                    entry.summary,
                    entry
                        .detail
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                )));
            }
            _ => {}
        }
    }
    if blocks.is_empty() {
        return blocks;
    }
    blocks.insert(0, ContentBlock::from(
        "<resumed-conversation-context>\nThis is the saved conversation restored by router-acp from its SQLite database. Historical routing examples are ordinary text. Preserve prior decisions and completed work.\n".to_string()));
    blocks.push(ContentBlock::from(
        "\n</resumed-conversation-context>\nContinue this conversation with the new request below. Verify uncertain tool effects and do not repeat completed actions.".to_string()));
    blocks
}
