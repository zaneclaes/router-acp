//! Background work an agent starts with its own tools, mirrored to the ACP client.
//!
//! `background_start` runs a command in a terminal the ACP client hosts, so the client
//! knows its whole lifecycle. An agent can still start work with its harness's own tools,
//! and the client then has no channel that says the work exists: Claude Code's `Monitor`
//! and Bash `run_in_background` complete their tool call the moment the work starts, and
//! claude-agent-acp reports the task's lifecycle only to JetBrains AIR clients (its
//! `asyncTasks` extension, which also changes how every tool call renders, so the router
//! does not claim it). What every client does get is the tool results, and those name the
//! task:
//!
//! - `Monitor started (task <id>, timeout <ms>ms)` / `… persistent — runs until TaskStop …`
//! - `Command running in background with ID: <id>. Output is being written to: <path>`
//!
//! and the results of the tools that end one (`TaskStop`, `KillShell`/`KillBash`) or read
//! its final state (`TaskOutput`/`BashOutput` reporting `completed`, `failed`, `killed`).
//! A monitor also ends at its timeout.
//!
//! The router follows these per session and sends the client one
//! `_router/background` notification per change:
//!
//! ```json
//! {"sessionId": "...", "state": "running" | "ended", "reason": "stopped" | "finished" | "timeout",
//!  "task": {"id": "b1x2", "kind": "monitor" | "shell", "command": "...",
//!           "startedAt": 1793736940000, "outputFile": "/tmp/...", "toolCallId": "..."}}
//! ```
//!
//! Any client (kory, Kory Code's relay) can list running work from these alone instead of
//! parsing tool results itself.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use regex::Regex;
use serde_json::{Value, json};

use crate::session::Shared;

pub const METHOD: &str = "_router/background";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeTask {
    pub id: String,
    pub kind: &'static str,
    pub command: String,
    pub started_at_ms: i64,
    pub output_file: Option<String>,
    pub tool_call_id: String,
    /// When a monitor's timeout ends it.
    pub deadline_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Started(NativeTask),
    Ended {
        task: NativeTask,
        reason: &'static str,
    },
}

/// What a tool call in flight is: its tool, its command, and the task its input names.
#[derive(Default, Debug, Clone)]
struct ToolCall {
    name: String,
    command: String,
    target: Option<String>,
}

/// One session's tool calls in flight and its running tasks.
#[derive(Default, Debug)]
pub struct Tracker {
    tools: HashMap<String, ToolCall>,
    running: Vec<NativeTask>,
}

impl Tracker {
    pub fn running(&self) -> &[NativeTask] {
        &self.running
    }

    /// One `session/update`'s `update`, at `now_ms`: what started or ended.
    pub fn observe(&mut self, update: &Value, now_ms: i64) -> Vec<Event> {
        let mut events = self.expire(now_ms);
        let kind = update
            .get("sessionUpdate")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if kind != "tool_call" && kind != "tool_call_update" {
            return events;
        }
        let Some(id) = update.get("toolCallId").and_then(Value::as_str) else {
            return events;
        };
        let entry = self.tools.entry(id.to_string()).or_default();
        if let Some(name) = tool_name(update) {
            entry.name = name;
        }
        if let Some(command) = tool_command(update) {
            entry.command = command;
        }
        if let Some(target) = input_task_id(update.get("rawInput")) {
            entry.target = Some(target);
        }
        let ToolCall {
            name,
            command,
            target,
        } = entry.clone();
        let status = update.get("status").and_then(Value::as_str);
        // Ending tools name their task in their input; act once the call has an outcome.
        if matches!(status, Some("completed" | "failed")) {
            self.tools.remove(id);
            let text = result_text(update);
            match name.as_str() {
                "Monitor" => {
                    if let Some((task_id, timeout_ms)) = monitor_started(&text) {
                        let task = NativeTask {
                            id: task_id,
                            kind: "monitor",
                            command: if command.is_empty() {
                                "Monitor".to_string()
                            } else {
                                command
                            },
                            started_at_ms: now_ms,
                            output_file: None,
                            tool_call_id: id.to_string(),
                            deadline_ms: timeout_ms.map(|ms| now_ms + ms),
                        };
                        events.extend(self.start(task));
                    }
                }
                "Bash" => {
                    if let Some((task_id, output_file)) = shell_started(&text) {
                        let task = NativeTask {
                            id: task_id,
                            kind: "shell",
                            command,
                            started_at_ms: now_ms,
                            output_file,
                            tool_call_id: id.to_string(),
                            deadline_ms: None,
                        };
                        events.extend(self.start(task));
                    }
                }
                "TaskStop" | "KillShell" | "KillBash" => {
                    if let Some(target) = target.or_else(|| result_task_id(&text)) {
                        events.extend(self.end(&target, "stopped"));
                    }
                }
                "TaskOutput" | "BashOutput" => {
                    if let Some(target) = target.or_else(|| result_task_id(&text))
                        && reports_finished(&text)
                    {
                        events.extend(self.end(&target, "finished"));
                    }
                }
                _ => {}
            }
        }
        events
    }

    fn start(&mut self, task: NativeTask) -> Vec<Event> {
        if self.running.iter().any(|running| running.id == task.id) {
            return Vec::new();
        }
        self.running.push(task.clone());
        vec![Event::Started(task)]
    }

    fn end(&mut self, id: &str, reason: &'static str) -> Vec<Event> {
        match self.running.iter().position(|task| task.id == id) {
            Some(index) => vec![Event::Ended {
                task: self.running.remove(index),
                reason,
            }],
            None => Vec::new(),
        }
    }

    /// Monitors past their timeout end.
    pub fn expire(&mut self, now_ms: i64) -> Vec<Event> {
        let expired: Vec<String> = self
            .running
            .iter()
            .filter(|task| task.deadline_ms.is_some_and(|deadline| deadline <= now_ms))
            .map(|task| task.id.clone())
            .collect();
        expired
            .iter()
            .flat_map(|id| self.end(id, "timeout"))
            .collect()
    }
}

fn tool_name(update: &Value) -> Option<String> {
    update
        .pointer("/_meta/claudeCode/toolName")
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn tool_command(update: &Value) -> Option<String> {
    let input = update.get("rawInput")?;
    input
        .get("command")
        .or_else(|| input.get("description"))
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_string)
}

fn result_text(update: &Value) -> String {
    let mut text = String::new();
    if let Some(content) = update.get("content").and_then(Value::as_array) {
        for block in content {
            if let Some(part) = block.pointer("/content/text").and_then(Value::as_str) {
                text.push_str(part);
                text.push('\n');
            }
        }
    }
    match update.get("rawOutput") {
        Some(Value::String(raw)) => text.push_str(raw),
        Some(raw @ (Value::Object(_) | Value::Array(_))) => text.push_str(&raw.to_string()),
        _ => {}
    }
    text
}

fn regex(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).expect("a valid pattern"))
}

/// A Monitor's confirmation: its task id and its timeout, none when persistent.
fn monitor_started(text: &str) -> Option<(String, Option<i64>)> {
    static STARTED: OnceLock<Regex> = OnceLock::new();
    let found = regex(
        &STARTED,
        r"Monitor started \(task ([A-Za-z0-9_-]+), (?:timeout (\d+)ms|persistent)",
    )
    .captures(text)?;
    let timeout = found.get(2).and_then(|ms| ms.as_str().parse().ok());
    Some((found[1].to_string(), timeout))
}

/// A backgrounded Bash command's notice: its task id and the file its output goes to.
fn shell_started(text: &str) -> Option<(String, Option<String>)> {
    static STARTED: OnceLock<Regex> = OnceLock::new();
    static OUTPUT: OnceLock<Regex> = OnceLock::new();
    let id = regex(&STARTED, r"running in background with ID: ([A-Za-z0-9_-]+)").captures(text)?[1]
        .to_string();
    let output = regex(&OUTPUT, r"Output is being written to: (\S+)")
        .captures(text)
        .map(|found| found[1].trim_end_matches('.').to_string());
    Some((id, output))
}

fn input_task_id(input: Option<&Value>) -> Option<String> {
    let input = input?;
    ["task_id", "shell_id", "bash_id", "taskId", "shellId"]
        .iter()
        .find_map(|key| input.get(key).and_then(Value::as_str))
        .map(str::to_string)
}

fn result_task_id(text: &str) -> Option<String> {
    static ID: OnceLock<Regex> = OnceLock::new();
    regex(&ID, r#""task_id"\s*:\s*"([A-Za-z0-9_-]+)""#)
        .captures(text)
        .map(|found| found[1].to_string())
}

/// Whether a TaskOutput/BashOutput result reports the task finished.
fn reports_finished(text: &str) -> bool {
    static FINISHED: OnceLock<Regex> = OnceLock::new();
    regex(
        &FINISHED,
        r#"(?i)("status"\s*:\s*"|<status>|status:\s*)(completed|failed|killed|exited|stopped)"#,
    )
    .is_match(text)
}

static TRACKERS: OnceLock<Mutex<HashMap<String, Tracker>>> = OnceLock::new();

fn trackers() -> &'static Mutex<HashMap<String, Tracker>> {
    TRACKERS.get_or_init(Default::default)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Follow one downstream `session/update` of `router_sid` and tell the client what changed.
pub fn observe(shared: &Arc<Shared>, router_sid: &str, update: &Value) {
    let events = {
        let mut trackers = trackers().lock().unwrap();
        trackers
            .entry(router_sid.to_string())
            .or_default()
            .observe(update, now_ms())
    };
    for event in events {
        if let Event::Started(task) = &event
            && let Some(deadline) = task.deadline_ms
        {
            schedule_expiry(shared.clone(), router_sid.to_string(), deadline);
        }
        publish(shared, router_sid, &event);
    }
}

/// Running native tasks of one session, for state queries.
pub fn running(router_sid: &str) -> Vec<NativeTask> {
    trackers()
        .lock()
        .unwrap()
        .get(router_sid)
        .map(|tracker| tracker.running().to_vec())
        .unwrap_or_default()
}

/// The session ended: its tasks end with it.
pub fn forget(shared: &Arc<Shared>, router_sid: &str) {
    let tracker = trackers().lock().unwrap().remove(router_sid);
    for task in tracker.map(|tracker| tracker.running).unwrap_or_default() {
        publish(
            shared,
            router_sid,
            &Event::Ended {
                task,
                reason: "session ended",
            },
        );
    }
}

fn schedule_expiry(shared: Arc<Shared>, router_sid: String, deadline_ms: i64) {
    let wait = (deadline_ms - now_ms()).max(0) as u64;
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
        let events = trackers()
            .lock()
            .unwrap()
            .get_mut(&router_sid)
            .map(|tracker| tracker.expire(now_ms()))
            .unwrap_or_default();
        for event in events {
            publish(&shared, &router_sid, &event);
        }
    });
}

pub fn notification(router_sid: &str, event: &Event) -> Value {
    let (task, state, reason) = match event {
        Event::Started(task) => (task, "running", None),
        Event::Ended { task, reason } => (task, "ended", Some(*reason)),
    };
    let mut params = json!({
        "sessionId": router_sid,
        "state": state,
        "task": {
            "id": task.id,
            "kind": task.kind,
            "command": task.command,
            "startedAt": task.started_at_ms,
            "toolCallId": task.tool_call_id,
        },
    });
    if let Some(file) = &task.output_file {
        params["task"]["outputFile"] = json!(file);
    }
    if let Some(reason) = reason {
        params["reason"] = json!(reason);
    }
    params
}

fn publish(shared: &Arc<Shared>, router_sid: &str, event: &Event) {
    let params = notification(router_sid, event);
    tracing::info!(session = router_sid, %params, "native background task");
    if let Some(upstream) = shared.upstream()
        && let Ok(message) = agent_client_protocol::UntypedMessage::new(METHOD, params)
    {
        let _ = upstream.send_notification(message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(id: &str, tool: &str, input: Value) -> Value {
        json!({"sessionUpdate": "tool_call", "toolCallId": id, "status": "in_progress",
               "rawInput": input, "_meta": {"claudeCode": {"toolName": tool}}})
    }

    fn done(id: &str, text: &str) -> Value {
        json!({"sessionUpdate": "tool_call_update", "toolCallId": id, "status": "completed",
               "content": [{"type": "content", "content": {"type": "text", "text": text}}]})
    }

    #[test]
    fn a_monitor_runs_until_its_timeout_or_a_stop() {
        let mut tracker = Tracker::default();
        tracker.observe(
            &call("t1", "Monitor", json!({"command": "tail -f build.log"})),
            0,
        );
        let events = tracker.observe(
            &done("t1", "Monitor started (task bsgkhmwll, timeout 60000ms)."),
            1_000,
        );
        let [Event::Started(task)] = events.as_slice() else {
            panic!("{events:?}");
        };
        assert_eq!(
            (task.id.as_str(), task.kind, task.command.as_str()),
            ("bsgkhmwll", "monitor", "tail -f build.log")
        );
        assert_eq!(task.deadline_ms, Some(61_000));
        assert!(
            tracker
                .observe(&json!({"sessionUpdate": "plan"}), 30_000)
                .is_empty()
        );
        let events = tracker.expire(61_000);
        assert!(matches!(
            events.as_slice(),
            [Event::Ended {
                reason: "timeout",
                ..
            }]
        ));
        assert!(tracker.running().is_empty());

        tracker.observe(&call("t2", "Monitor", json!({})), 0);
        tracker.observe(
            &done(
                "t2",
                "Monitor started (task abc, persistent — runs until TaskStop or session end).",
            ),
            0,
        );
        assert_eq!(
            tracker.running()[0].deadline_ms,
            None,
            "a persistent monitor never times out"
        );
        tracker.observe(&call("t3", "TaskStop", json!({"task_id": "abc"})), 10);
        let events = tracker.observe(&done("t3", r#"{"message":"stopped","task_id":"abc"}"#), 10);
        assert!(matches!(
            events.as_slice(),
            [Event::Ended {
                reason: "stopped",
                ..
            }]
        ));
    }

    #[test]
    fn a_background_shell_runs_until_its_output_reports_it_finished() {
        let mut tracker = Tracker::default();
        tracker.observe(
            &call(
                "b1",
                "Bash",
                json!({"command": "cargo build", "run_in_background": true}),
            ),
            0,
        );
        let events = tracker.observe(
            &done(
                "b1",
                "Command running in background with ID: bash_7. Output is being written to: /tmp/claude/bash_7.out",
            ),
            5,
        );
        let [Event::Started(task)] = events.as_slice() else {
            panic!("{events:?}");
        };
        assert_eq!(task.kind, "shell");
        assert_eq!(task.output_file.as_deref(), Some("/tmp/claude/bash_7.out"));
        let note = notification("rtr-1", &events[0]);
        assert_eq!(note["state"], "running");
        assert_eq!(note["task"]["command"], "cargo build");

        tracker.observe(&call("o1", "BashOutput", json!({"bash_id": "bash_7"})), 9);
        assert!(
            tracker
                .observe(&done("o1", "<status>running</status>"), 9)
                .is_empty(),
            "still running"
        );
        tracker.observe(&call("o2", "BashOutput", json!({"bash_id": "bash_7"})), 20);
        let events = tracker.observe(&done("o2", "<status>completed</status>\nexit 0"), 20);
        assert!(matches!(
            events.as_slice(),
            [Event::Ended {
                reason: "finished",
                ..
            }]
        ));
        assert_eq!(notification("rtr-1", &events[0])["reason"], "finished");
    }

    #[test]
    fn an_ordinary_bash_call_starts_nothing() {
        let mut tracker = Tracker::default();
        tracker.observe(&call("b", "Bash", json!({"command": "ls"})), 0);
        assert!(tracker.observe(&done("b", "README.md"), 1).is_empty());
        assert!(tracker.running().is_empty());
    }
}
