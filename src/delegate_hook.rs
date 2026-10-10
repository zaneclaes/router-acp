//! `delegation.lifecycle_hook`: let a host account for router-owned workers.
//!
//! A host that tracks its own workers (registration, worktree ownership,
//! orphan cleanup) cannot see router delegates: they are ordinary top-level
//! sessions inside a downstream adapter, so no provider subagent hook fires in
//! the parent. The router therefore runs a configured command — no shell —
//! with one JSON event on stdin:
//!
//! - `delegate_start`, after the delegate session opens and before its prompt
//!   is sent. A non-zero exit or timeout refuses the delegate.
//! - `delegate_turn_end`, when each delegate turn ends. Exit 2 with a message
//!   on stderr sends that message back to the same delegate as its next prompt
//!   (at most `max_continuations` times), the way a provider's subagent-stop
//!   hook keeps a worker going. Any other result releases the turn.
//! - `delegate_stop` (`completed`, `cancelled`, `failed`, `closed`,
//!   `aborted`) and `parent_repinned` (the parent session moved to another
//!   model or account, so its provider session id changed). These go through
//!   the state DB's outbox: a failed or interrupted delivery is retried by any
//!   router process sharing that DB until the host accepts it.
//!
//! Every event carries the router's pid and start time so a host can treat a
//! dead router's delegates as stopped even when no stop event ever arrives.

use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

use crate::config::DelegateLifecycleHook;
use crate::session::Shared;

/// This process's pid and start time (Unix ms), fixed on first use. `serve`
/// calls it at startup so the time is the router's own start.
pub fn process_identity() -> (u32, u64) {
    static IDENTITY: OnceLock<(u32, u64)> = OnceLock::new();
    *IDENTITY.get_or_init(|| {
        let started = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_default();
        (std::process::id(), started)
    })
}

/// A worker's structured handoff, recorded through the `worker_handoff` tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handoff {
    pub kind: String,
    pub message: String,
}

/// The handoff kinds `worker_handoff` accepts.
pub const HANDOFF_KINDS: [&str; 8] = [
    "commit_paths_ready",
    "waiting",
    "lease_requested",
    "ownership_requested",
    "staging_gate_request",
    "round_verified",
    "round_failed",
    "reassignment_required",
];

/// One delegate lifecycle event, serialized as the hook's stdin.
#[derive(Debug, Clone, Serialize)]
pub struct DelegateEvent {
    /// `delegate_start`, `delegate_turn_end` or `delegate_stop`.
    pub event: &'static str,
    /// Stable worker id: the `b-…` job id for a background delegate,
    /// otherwise a router-generated `w-…` id. The delegate's own prompt opens
    /// with it, and `worker_whoami` returns it.
    pub worker_id: String,
    pub parent_router_session_id: String,
    /// The parent's downstream (provider) session id — what the provider's
    /// own hooks report as the parent's session id.
    pub parent_downstream_session_id: String,
    pub parent_candidate: String,
    pub candidate: String,
    /// The delegate's provider company (`agents[].lineage`, default the agent
    /// name without any `@account`).
    pub lineage: String,
    pub downstream_session_id: String,
    /// The delegate's row id in the router state DB.
    pub state_session_id: String,
    pub cwd: String,
    pub effort: Option<String>,
    pub background: bool,
    pub keep_open: bool,
    pub task_summary: String,
    pub router_pid: u32,
    pub router_started_at_ms: u64,
    /// Turn end: 1-based turn count for this delegate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn: Option<u32>,
    /// Turn end: the text the delegate produced this turn.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message: Option<String>,
    /// Turn end: the delegate's latest `worker_handoff`, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handoff: Option<Handoff>,
    /// Stop only: `completed`, `cancelled`, `failed`, `closed` or `aborted`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl DelegateEvent {
    /// The stop event for this delegate, keeping every identity field.
    pub fn stopped(&self, outcome: &str, detail: Option<String>) -> Self {
        Self {
            event: "delegate_stop",
            outcome: Some(outcome.to_string()),
            detail,
            turn: None,
            last_message: None,
            handoff: None,
            ..self.clone()
        }
    }

    /// The turn-end event for this delegate's `turn`.
    pub fn turn_ended(&self, turn: u32, text: &str, handoff: Option<Handoff>) -> Self {
        Self {
            event: "delegate_turn_end",
            turn: Some(turn),
            last_message: Some(text.to_string()),
            handoff,
            outcome: None,
            detail: None,
            ..self.clone()
        }
    }
}

/// The parent session moved to another candidate (failover, an explicit
/// switch, escalation, demotion, ...), so its provider session id changed.
#[derive(Debug, Clone, Serialize)]
pub struct RepinEvent {
    pub event: &'static str,
    pub parent_router_session_id: String,
    pub previous_downstream_session_id: String,
    pub previous_candidate: String,
    pub downstream_session_id: String,
    pub candidate: String,
    pub lineage: String,
    pub reason: String,
    pub router_pid: u32,
    pub router_started_at_ms: u64,
}

/// The identity line prepended to a delegate's prompt when a lifecycle hook
/// is configured, so the worker knows the id its host registered.
pub fn identity_line(event: &DelegateEvent) -> String {
    format!(
        "[router-acp delegate] worker id: {} · model: {} · parent session: {}",
        event.worker_id, event.candidate, event.parent_downstream_session_id
    )
}

struct HookOutput {
    code: Option<i32>,
    stderr: String,
}

async fn invoke(hook: &DelegateLifecycleHook, payload: &[u8]) -> Result<HookOutput, String> {
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;

    let mut cmd = tokio::process::Command::new(&hook.command);
    // The hook sees the same router identity the adapters do.
    for (k, v) in crate::transport::router_env() {
        cmd.env(k, v);
    }
    cmd.args(&hook.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("could not start `{}`: {e}", hook.command))?;
    if let Some(mut stdin) = child.stdin.take() {
        // A hook that ignores stdin may close it early; that is not a failure.
        let _ = stdin.write_all(payload).await;
        let _ = stdin.write_all(b"\n").await;
        let _ = stdin.shutdown().await;
    }
    let output = tokio::time::timeout(
        std::time::Duration::from_millis(hook.timeout_ms),
        child.wait_with_output(),
    )
    .await
    .map_err(|_| format!("timed out after {} ms", hook.timeout_ms))?
    .map_err(|e| format!("wait failed: {e}"))?;
    Ok(HookOutput {
        code: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn describe(output: &HookOutput) -> String {
    let status = output
        .code
        .map(|c| format!("exit status: {c}"))
        .unwrap_or_else(|| "a signal".to_string());
    if output.stderr.is_empty() {
        format!("exited with {status}")
    } else {
        format!("exited with {status}: {}", output.stderr)
    }
}

/// Run the hook for one event. `Err` carries a human-readable reason (exit
/// status plus the hook's stderr, or a timeout/spawn failure).
pub async fn run(hook: &DelegateLifecycleHook, event: &impl Serialize) -> Result<(), String> {
    let payload = serde_json::to_vec(event).map_err(|e| format!("encode event: {e}"))?;
    run_payload(hook, &payload).await
}

async fn run_payload(hook: &DelegateLifecycleHook, payload: &[u8]) -> Result<(), String> {
    let output = invoke(hook, payload).await?;
    if output.code == Some(0) {
        Ok(())
    } else {
        Err(describe(&output))
    }
}

/// What the host decided at the end of a delegate turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnVerdict {
    /// The turn is finished; return its result to the parent.
    Release,
    /// Send this message back to the same delegate as its next prompt.
    Continue(String),
}

/// Ask the host whether a delegate's turn may end. Only exit 2 with a message
/// keeps it going; a failing or silent hook releases the turn (and is logged)
/// so a broken hook can never trap a worker in a loop.
pub async fn turn_end(hook: &DelegateLifecycleHook, event: &DelegateEvent) -> TurnVerdict {
    let payload = match serde_json::to_vec(event) {
        Ok(payload) => payload,
        Err(_) => return TurnVerdict::Release,
    };
    match invoke(hook, &payload).await {
        Ok(output) if output.code == Some(2) && !output.stderr.is_empty() => {
            TurnVerdict::Continue(output.stderr)
        }
        Ok(output) if output.code == Some(0) => TurnVerdict::Release,
        Ok(output) => {
            tracing::warn!(
                worker = %event.worker_id,
                result = describe(&output),
                "delegate turn-end hook failed; releasing the turn"
            );
            TurnVerdict::Release
        }
        Err(err) => {
            tracing::warn!(worker = %event.worker_id, %err, "delegate turn-end hook failed; releasing the turn");
            TurnVerdict::Release
        }
    }
}

/// Queue an event in the outbox and deliver it in the background; a failed
/// delivery stays queued for the flusher.
pub fn deliver(shared: &Arc<Shared>, event: &impl Serialize) {
    let Some((id, payload, hook)) = enqueue(shared, event) else {
        return;
    };
    let shared = shared.clone();
    tokio::spawn(async move { attempt(&shared, &hook, id, &payload).await });
}

/// Queue an event and wait for its first delivery attempt, so the host has
/// seen it before the caller continues (it stays queued if that fails).
pub async fn deliver_now(shared: &Arc<Shared>, event: &impl Serialize) {
    if let Some((id, payload, hook)) = enqueue(shared, event) {
        attempt(shared, &hook, id, &payload).await;
    }
}

fn enqueue(
    shared: &Arc<Shared>,
    event: &impl Serialize,
) -> Option<(i64, String, DelegateLifecycleHook)> {
    let hook = shared.cfg.delegation.lifecycle_hook.clone()?;
    let payload = serde_json::to_string(event).ok()?;
    let id = shared.state.lock().unwrap().outbox_push(&payload)?;
    Some((id, payload, hook))
}

async fn attempt(shared: &Arc<Shared>, hook: &DelegateLifecycleHook, id: i64, payload: &str) {
    match run_payload(hook, payload.as_bytes()).await {
        Ok(()) => shared.state.lock().unwrap().outbox_done(id),
        Err(err) => {
            tracing::warn!(outbox = id, %err, "lifecycle hook did not accept an event; it stays queued");
            shared.state.lock().unwrap().outbox_attempted(id);
        }
    }
}

/// Redeliver queued events now and then every `interval`, including any left
/// by a router process that exited before its hook accepted them.
pub fn spawn_outbox_flusher(
    shared: &Arc<Shared>,
    interval: std::time::Duration,
) -> Option<tokio::task::JoinHandle<()>> {
    let hook = shared.cfg.delegation.lifecycle_hook.clone()?;
    let shared = shared.clone();
    Some(tokio::spawn(async move {
        loop {
            flush(&shared, &hook).await;
            tokio::time::sleep(interval).await;
        }
    }))
}

pub async fn flush(shared: &Arc<Shared>, hook: &DelegateLifecycleHook) {
    let pending = shared.state.lock().unwrap().outbox_pending(100);
    for (id, payload) in pending {
        attempt(shared, hook, id, &payload).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> DelegateEvent {
        DelegateEvent {
            event: "delegate_start",
            worker_id: "b-1234abcd".into(),
            parent_router_session_id: "rtr-1".into(),
            parent_downstream_session_id: "thread-1".into(),
            parent_candidate: "codex/gpt-6.1-sol".into(),
            candidate: "grok/grok-4.7".into(),
            lineage: "xai".into(),
            downstream_session_id: "grok-1".into(),
            state_session_id: "rtr-1::delegate-grok-1".into(),
            cwd: "/repo".into(),
            effort: Some("low".into()),
            background: true,
            keep_open: true,
            task_summary: "Implement the plan".into(),
            router_pid: 42,
            router_started_at_ms: 1,
            turn: None,
            last_message: None,
            handoff: None,
            outcome: None,
            detail: None,
        }
    }

    fn sh(script: &str) -> DelegateLifecycleHook {
        DelegateLifecycleHook {
            command: "/bin/sh".into(),
            args: vec!["-c".into(), script.into()],
            timeout_ms: 5_000,
            max_continuations: 3,
        }
    }

    #[test]
    fn derived_events_keep_identity() {
        let stop = serde_json::to_value(event().stopped("completed", None)).unwrap();
        assert_eq!(stop["event"], "delegate_stop");
        assert_eq!(stop["worker_id"], "b-1234abcd");
        assert_eq!(stop["outcome"], "completed");
        assert_eq!(stop["router_pid"], 42);
        let handoff = Handoff {
            kind: "waiting".into(),
            message: "CI build 12".into(),
        };
        let turn = serde_json::to_value(event().turn_ended(2, "text", Some(handoff))).unwrap();
        assert_eq!(turn["event"], "delegate_turn_end");
        assert_eq!(turn["turn"], 2);
        assert_eq!(turn["handoff"]["kind"], "waiting");
        assert!(turn.get("outcome").is_none());
        let start = serde_json::to_value(event()).unwrap();
        assert!(start.get("outcome").is_none() && start.get("turn").is_none());
    }

    #[tokio::test]
    async fn hook_receives_the_event_on_stdin() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("event.json");
        let hook = sh(&format!("cat > '{}'", out.display()));
        run(&hook, &event()).await.unwrap();
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out).unwrap()).unwrap();
        assert_eq!(written["candidate"], "grok/grok-4.7");
        assert_eq!(written["parent_downstream_session_id"], "thread-1");
    }

    #[tokio::test]
    async fn failing_hook_reports_its_stderr() {
        let err = run(&sh("echo refused >&2; exit 3"), &event())
            .await
            .unwrap_err();
        assert!(err.contains("refused"), "{err}");
    }

    #[tokio::test]
    async fn slow_hook_times_out() {
        let mut hook = sh("sleep 5");
        hook.timeout_ms = 100;
        let err = run(&hook, &event()).await.unwrap_err();
        assert!(err.contains("timed out"), "{err}");
    }

    #[tokio::test]
    async fn a_refused_event_stays_queued_until_the_host_accepts_it() {
        let dir = tempfile::tempdir().unwrap();
        let flag = dir.path().join("accepting");
        let log = dir.path().join("events.jsonl");
        let script = format!(
            "[ -f '{}' ] || exit 1; cat >> '{}'",
            flag.display(),
            log.display()
        );
        let yaml = format!(
            "state_file: {}\ndelegation:\n  lifecycle_hook: {{ command: /bin/sh, args: [\"-c\", {}] }}\n\
             agents:\n  - name: mock\n    command: {{ type: stdio, command: mock-agent }}\n    \
             model_selection: {{ type: config-option }}\n    models:\n      - {{ id: m, cost_rank: 1 }}\n",
            dir.path().join("state.db").display(),
            serde_json::to_string(&script).unwrap()
        );
        let shared = Shared::new(crate::config::Config::from_yaml(&yaml).unwrap()).unwrap();
        let hook = shared.cfg.delegation.lifecycle_hook.clone().unwrap();
        let stop = event().stopped("completed", None);

        deliver_now(&shared, &stop).await;
        assert_eq!(shared.state.lock().unwrap().outbox_pending(10).len(), 1);
        assert!(!log.exists(), "the refusing host recorded nothing");

        std::fs::write(&flag, "").unwrap();
        flush(&shared, &hook).await;
        assert!(shared.state.lock().unwrap().outbox_pending(10).is_empty());
        let delivered: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(&log).unwrap().trim()).unwrap();
        assert_eq!(delivered["event"], "delegate_stop");
        assert_eq!(delivered["worker_id"], "b-1234abcd");
    }

    #[tokio::test]
    async fn only_exit_two_with_a_message_continues_a_turn() {
        let ended = event().turn_ended(1, "done", None);
        assert_eq!(
            turn_end(&sh("echo 'push your branch' >&2; exit 2"), &ended).await,
            TurnVerdict::Continue("push your branch".into())
        );
        assert_eq!(turn_end(&sh("exit 0"), &ended).await, TurnVerdict::Release);
        assert_eq!(turn_end(&sh("exit 2"), &ended).await, TurnVerdict::Release);
        assert_eq!(
            turn_end(&sh("echo broken >&2; exit 1"), &ended).await,
            TurnVerdict::Release
        );
    }
}
