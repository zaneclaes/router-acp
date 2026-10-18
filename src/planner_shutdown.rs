//! `_router/planner_shutdown`: a graceful drain of a planner run.
//!
//! kory sends the extension request `{"sessionId"}`; the router answers
//! `{"accepted":true}` at once (an error when the session has no planner run)
//! and then, reporting each step as `_router/shutdown_progress`:
//!
//! 1. sets the run `draining`: admissions, capacity and audit wakes,
//!    auto-admission, worker resumptions, in-flight wakes and turn-end gates
//!    all stop (each of them runs only for a `running` run);
//! 2. saves the resume state through `routers.planner.shutdown` (`saving`,
//!    then `saved` or `failed`; a failed save does not stop the drain);
//! 3. prompts the parent once to record its exact resume point, and waits up
//!    to 10 minutes for that turn;
//! 4. tells every live worker to commit, push, record its resume point and
//!    hand off `waiting`, and waits for each to stop, up to
//!    `drain_timeout_secs`, after which it is cancelled (`draining`);
//! 5. sets the run `paused` (`drained`), which the next `/implement` resumes.
//!
//! A router that dies mid-drain leaves the run `draining`; `/implement`
//! treats that like `paused`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_client_protocol::schema::v1::{CancelNotification, ContentBlock, PromptRequest};
use serde_json::{Value, json};

use crate::planner_guard::{self, with_runtime};
use crate::planner_workflow::{self, PlannerRun, RunStatus, WorkStatus};
use crate::session::Shared;

pub const METHOD: &str = "_router/planner_shutdown";
pub const PROGRESS_METHOD: &str = "_router/shutdown_progress";
pub const STATUS_METHOD: &str = "_router/planner_status";

/// A read-only census for the CLI after cancelling a turn or before exit.
pub fn status(shared: &Shared, params: &Value) -> Result<Value, String> {
    let sid = params
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or("planner status requires sessionId")?;
    let run = planner_workflow::load(shared, sid)?.ok_or("session has no planner run")?;
    let capacity =
        crate::planner_capacity::status_block(shared, &run, chrono::Utc::now().timestamp_millis());
    let admitting = run.status == RunStatus::Running
        && run.phase == crate::config::PlannerPhase::Implementation
        && run.execution_request.is_some()
        && run.capacity.pending_adoption.is_none();
    let admissible = if !admitting {
        Some(0)
    } else {
        run.capacity
            .host
            .as_ref()
            .filter(|host| host.ok)
            .map(|_| capacity["unheld"].as_array().map_or(0, Vec::len))
    };
    Ok(json!({"status":run.status,"live":capacity["live"],"admissible":admissible}))
}
pub const PARENT_PROMPT: &str = "Shutdown requested by the operator: rewrite Docs/CONTINUE.md and \
     your registers now with the exact resume point; admit nothing; end your turn.";
pub const WORKER_PROMPT: &str = "Shutdown requested: commit and push your branch now, record your \
     status notes and exact resume point, then hand off `waiting` with that resume point and \
     stop.";
const PARENT_TURN_LIMIT: Duration = Duration::from_secs(10 * 60);
const POLL: Duration = Duration::from_millis(500);

/// One active drain, in process.
#[derive(Default)]
pub struct Drain {
    /// Mid-turn workers whose next turn end delivers [`WORKER_PROMPT`].
    pub pending: HashSet<String>,
    /// Workers that received [`WORKER_PROMPT`] at a turn end.
    pub injected: HashSet<String>,
    /// Workers cancelled after their drain bound.
    pub cancelled: HashSet<String>,
}

/// Handle `_router/planner_shutdown`.
pub fn request(shared: &Arc<Shared>, params: &Value) -> Result<Value, String> {
    let sid = params
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or("_router/planner_shutdown requires sessionId")?;
    if planner_workflow::load(shared, sid)?.is_none() {
        return Err(format!("session {sid} has no planner run to shut down"));
    }
    let start = with_runtime(shared, sid, |rt| {
        if rt.drain.is_some() {
            return false;
        }
        rt.drain = Some(Drain::default());
        true
    });
    if start {
        let shared = shared.clone();
        let sid = sid.to_string();
        tokio::spawn(async move { run_drain(&shared, &sid).await });
    }
    Ok(json!({"accepted":true}))
}

/// Whether this router is draining `sid` now.
pub fn draining(shared: &Shared, sid: &str) -> bool {
    shared
        .planner_runtime
        .lock()
        .unwrap()
        .get(sid)
        .is_some_and(|rt| rt.drain.is_some())
}

/// The shutdown message a mid-turn worker gets at its turn end, once.
pub fn take_injection(shared: &Shared, sid: &str, work_id: &str) -> Option<String> {
    let mut runtimes = shared.planner_runtime.lock().unwrap();
    let drain = runtimes.get_mut(sid)?.drain.as_mut()?;
    drain.pending.remove(work_id).then(|| {
        drain.injected.insert(work_id.to_string());
        WORKER_PROMPT.to_string()
    })
}

pub fn cancelled(shared: &Shared, sid: &str, work_id: &str) -> bool {
    shared
        .planner_runtime
        .lock()
        .unwrap()
        .get(sid)
        .and_then(|rt| rt.drain.as_ref())
        .is_some_and(|d| d.cancelled.contains(work_id))
}

/// Every `_router/shutdown_progress` sent for `sid`, in order.
pub fn progress(shared: &Shared, sid: &str) -> Vec<Value> {
    with_runtime(shared, sid, |rt| rt.progress.clone())
}

fn notify(
    shared: &Shared,
    sid: &str,
    phase: &str,
    message: &str,
    saved: &[String],
    workers: &BTreeMap<String, String>,
) {
    let stopped = workers
        .values()
        .filter(|s| matches!(s.as_str(), "stopped" | "cancelled"))
        .count();
    let params = json!({"sessionId":sid,"phase":phase,"message":message,"saved":saved,
        "workersTotal":workers.len(),"workersStopped":stopped,
        "workers":workers.iter().map(|(id, status)| json!({"workId":id,"status":status})).collect::<Vec<_>>()});
    tracing::warn!(session = sid, phase, message, "planner shutdown");
    with_runtime(shared, sid, |rt| rt.progress.push(params.clone()));
    if let Some(upstream) = shared.upstream()
        && let Ok(notification) =
            agent_client_protocol::UntypedMessage::new(PROGRESS_METHOD, params)
    {
        let _ = upstream.send_notification(notification);
    }
}

/// Drain one planner run (see the module docs). Runs to the end whatever
/// fails on the way.
pub async fn run_drain(shared: &Arc<Shared>, sid: &str) {
    let cfg = shared
        .cfg
        .routers
        .planner
        .shutdown
        .clone()
        .unwrap_or_default();
    let mut workers = BTreeMap::new();
    let mut saved = Vec::new();
    let mut prior = RunStatus::Running;
    let run = match planner_workflow::mutate(shared, sid, |run| {
        prior = run.status;
        run.status = RunStatus::Draining;
        Ok(())
    }) {
        Ok(run) => run,
        Err(error) => {
            notify(
                shared,
                sid,
                "failed",
                &format!("the run could not be set draining: {error}"),
                &saved,
                &workers,
            );
            with_runtime(shared, sid, |rt| rt.drain = None);
            return;
        }
    };

    notify(
        shared,
        sid,
        "saving",
        "admissions stopped; saving the run's resume state",
        &saved,
        &workers,
    );
    match save_state(shared, sid, &run, prior, &cfg).await {
        Ok((paths, summary)) => {
            saved = paths;
            notify(shared, sid, "saved", &summary, &saved, &workers);
        }
        Err(error) => notify(
            shared,
            sid,
            "failed",
            &format!("the resume state was not saved ({error}); the drain continues"),
            &saved,
            &workers,
        ),
    }

    if matches!(prior, RunStatus::Running | RunStatus::Draining) {
        let outcome = prompt_parent(shared, sid, &saved, &workers).await;
        notify(shared, sid, "draining", &outcome, &saved, &workers);
    }

    let live = live_workers(shared, sid);
    workers = live
        .iter()
        .map(|id| (id.clone(), "running".to_string()))
        .collect();
    notify(
        shared,
        sid,
        "draining",
        &format!(
            "asking {} live worker(s) to commit, push and stop",
            live.len()
        ),
        &saved,
        &workers,
    );
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    for work_id in live {
        let shared = shared.clone();
        let sid = sid.to_string();
        let tx = tx.clone();
        let bound = Duration::from_secs(cfg.drain_timeout_secs);
        tokio::spawn(async move { drain_worker(&shared, &sid, work_id, bound, tx).await });
    }
    drop(tx);
    while let Some((work_id, status)) = rx.recv().await {
        if workers.get(&work_id) != Some(&status) {
            let message = format!("{work_id}: {status}");
            workers.insert(work_id, status);
            notify(shared, sid, "draining", &message, &saved, &workers);
        }
    }

    let end = match prior {
        RunStatus::Complete | RunStatus::Cancelled => prior,
        _ => RunStatus::Paused,
    };
    let paused = planner_workflow::mutate(shared, sid, |run| {
        run.status = end;
        Ok(())
    });
    with_runtime(shared, sid, |rt| rt.drain = None);
    match paused {
        Ok(_) => notify(
            shared,
            sid,
            "drained",
            "every worker has stopped and the run is paused; /implement resumes it",
            &saved,
            &workers,
        ),
        Err(error) => notify(
            shared,
            sid,
            "failed",
            &format!(
                "every worker has stopped, but the run could not be set paused ({error}); it \
                 stays draining, which /implement also resumes"
            ),
            &saved,
            &workers,
        ),
    }
}

async fn save_state(
    shared: &Shared,
    sid: &str,
    run: &PlannerRun,
    prior: RunStatus,
    cfg: &crate::config::PlannerShutdownConfig,
) -> Result<(Vec<String>, String), String> {
    let Some(command) = &cfg.command else {
        return Ok((
            Vec::new(),
            "no routers.planner.shutdown host is configured: nothing was saved".into(),
        ));
    };
    let cwd = shared
        .with_session(sid, |s| s.cwd.clone())
        .ok_or("the parent session is not loaded")?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let input = json!({"event":"shutdown","session_id":sid,"cwd":cwd,"run_status":prior,
        "works":planner_guard::works_json(run),"holds":planner_guard::active_holds(run, now_ms)});
    let out = planner_guard::run_host(
        "shutdown host",
        command,
        &cfg.args,
        cfg.timeout_ms,
        &cwd,
        &input,
    )
    .await?;
    if out.code != Some(0) {
        return Err(format!("shutdown host exited with {}", out.describe()));
    }
    let reply: Value = serde_json::from_slice(&out.stdout)
        .map_err(|e| format!("shutdown host returned invalid JSON: {e}"))?;
    let paths = reply
        .get("saved")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let summary = reply
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("resume state saved")
        .to_string();
    Ok((paths, summary))
}

/// Prompt the parent once, then wait for that turn, all within 10 minutes.
async fn prompt_parent(
    shared: &Arc<Shared>,
    sid: &str,
    saved: &[String],
    workers: &BTreeMap<String, String>,
) -> String {
    let prompt = prompt_parent_once(shared, sid, saved, workers);
    tokio::pin!(prompt);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            outcome = &mut prompt => return outcome,
            _ = heartbeat.tick() => notify(shared, sid, "draining",
                "waiting for the parent's shutdown handoff; operator turns are allowed to finish", saved, workers),
        }
    }
}

async fn prompt_parent_once(
    shared: &Arc<Shared>,
    sid: &str,
    saved: &[String],
    workers: &BTreeMap<String, String>,
) -> String {
    if shared
        .with_session(sid, |s| s.pin.is_none())
        .unwrap_or(true)
    {
        return "the parent session is not loaded, so it was not prompted; the saved resume \
                state stands in for its notes"
            .into();
    }
    let started = Instant::now();
    if crate::planner_preempt::router_turn_in_flight(shared, sid) {
        notify(
            shared,
            sid,
            "draining",
            "cancelling the router-owned parent turn for shutdown",
            saved,
            workers,
        );
        if !crate::planner_preempt::preempt(shared, sid).await {
            return "the router-owned parent turn did not stop within its cancellation bound; the saved resume state stands and workers drain anyway".into();
        }
    }
    let activity = loop {
        if let Some(activity) = crate::planner_wake::PromptActivity::idle(shared, sid) {
            break activity;
        }
        if started.elapsed() >= PARENT_TURN_LIMIT {
            return "the parent stayed busy for 10 minutes, so it was not prompted".into();
        }
        tokio::time::sleep(POLL).await;
    };
    notify(
        shared,
        sid,
        "draining",
        "prompting the parent to record its shutdown resume point",
        saved,
        workers,
    );
    let prompt = PromptRequest::new(sid.to_string(), vec![ContentBlock::from(PARENT_PROMPT)]).meta(
        serde_json::from_value::<agent_client_protocol::schema::v1::Meta>(
            json!({"router_acp":{"agent_origin":true,"origin":"planner-shutdown"}}),
        )
        .ok(),
    );
    let task_shared = shared.clone();
    let task_sid = sid.to_string();
    let turn = tokio::spawn(async move {
        let _activity = activity;
        crate::session::run_primary_turn(task_shared, task_sid, prompt, None).await
    });
    let remaining = PARENT_TURN_LIMIT.saturating_sub(started.elapsed());
    match tokio::time::timeout(remaining, turn).await {
        Ok(Ok(Ok(_))) => "the parent recorded its resume point and ended its turn".into(),
        Ok(Ok(Err(error))) => format!("the parent's shutdown turn failed: {error}"),
        Ok(Err(error)) => format!("the parent's shutdown turn failed: {error}"),
        Err(_) => "the parent's shutdown turn ran past 10 minutes; the workers drain anyway".into(),
    }
}

fn open_delegate(shared: &Shared, sid: &str, work_id: &str, attempt_id: &str) -> Option<String> {
    shared
        .live_delegates
        .lock()
        .unwrap()
        .iter()
        .find(|(_, d)| {
            d.parent_sid == sid
                && d.planner
                    .as_ref()
                    .is_some_and(|p| p.work_id == work_id && p.attempt_id == attempt_id)
        })
        .map(|(id, _)| id.clone())
}

/// Works with a worker that can still act: a turn running, or an open
/// sub-session for the current attempt.
fn live_workers(shared: &Shared, sid: &str) -> Vec<String> {
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return Vec::new();
    };
    run.works
        .iter()
        .filter(|(_, w)| {
            !matches!(
                w.status,
                WorkStatus::Finished
                    | WorkStatus::Integrated
                    | WorkStatus::Blocked
                    | WorkStatus::Watching
            )
        })
        .filter(|(id, w)| {
            w.attempt
                .as_ref()
                .is_some_and(|a| !a.ended || open_delegate(shared, sid, id, &a.id).is_some())
        })
        .map(|(id, _)| id.clone())
        .collect()
}

fn set_pending(shared: &Shared, sid: &str, work_id: &str, pending: bool) -> bool {
    with_runtime(shared, sid, |rt| {
        let Some(drain) = rt.drain.as_mut() else {
            return false;
        };
        let injected = drain.injected.contains(work_id);
        if pending && !injected {
            drain.pending.insert(work_id.to_string());
        } else {
            drain.pending.remove(work_id);
        }
        injected
    })
}

/// Stop one worker: a mid-turn worker gets [`WORKER_PROMPT`] at its turn end
/// (through the turn gate); an idle one with an open sub-session gets it as a
/// follow-up turn. Each update goes to `tx`; past `bound` it is cancelled.
async fn drain_worker(
    shared: &Arc<Shared>,
    sid: &str,
    work_id: String,
    bound: Duration,
    tx: tokio::sync::mpsc::UnboundedSender<(String, String)>,
) {
    let deadline = Instant::now() + bound;
    let send = |status: &str| {
        let _ = tx.send((work_id.clone(), status.to_string()));
    };
    loop {
        if Instant::now() >= deadline {
            cancel_worker(shared, sid, &work_id);
            send("cancelled");
            return;
        }
        let attempt = planner_workflow::load(shared, sid)
            .ok()
            .flatten()
            .and_then(|run| run.works.get(&work_id).and_then(|w| w.attempt.clone()));
        let Some(attempt) = attempt else {
            send("stopped");
            return;
        };
        if !attempt.ended {
            let injected = set_pending(shared, sid, &work_id, true);
            send(if injected { "notified" } else { "running" });
            tokio::time::sleep(POLL).await;
            continue;
        }
        if set_pending(shared, sid, &work_id, false) {
            // It took the shutdown message and its turn has ended.
            send("stopped");
            return;
        }
        let Some(delegate_id) = open_delegate(shared, sid, &work_id, &attempt.id) else {
            send("stopped");
            return;
        };
        send("notified");
        let task_shared = shared.clone();
        let task_sid = sid.to_string();
        let followup = tokio::spawn(async move {
            crate::delegate_mcp::run_delegate_followup(
                &task_shared,
                &task_sid,
                crate::delegate_mcp::DelegateFollowupArgs {
                    delegate_id,
                    message: WORKER_PROMPT.to_string(),
                    ..Default::default()
                },
            )
            .await
        });
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, followup).await {
            Ok(result) => {
                if let Ok(Err(error)) = result {
                    tracing::warn!(session = sid, work_id, %error, "shutdown follow-up refused; the worker is idle");
                }
                send("stopped");
            }
            Err(_) => {
                cancel_worker(shared, sid, &work_id);
                send("cancelled");
            }
        }
        return;
    }
}

/// Cancel a worker past its drain bound: its running turn, then any open
/// sub-session it holds.
fn cancel_worker(shared: &Arc<Shared>, sid: &str, work_id: &str) {
    let child = with_runtime(shared, sid, |rt| {
        if let Some(drain) = rt.drain.as_mut() {
            drain.cancelled.insert(work_id.to_string());
            drain.pending.remove(work_id);
        }
        rt.children.get(work_id).cloned()
    });
    if let Some((process_key, downstream_sid)) = child
        && let Some(conn) = shared.target_conn(&process_key)
    {
        let _ = conn.send_notification(CancelNotification::new(downstream_sid));
    }
    let open = shared
        .live_delegates
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, d)| {
            d.parent_sid == sid && d.planner.as_ref().is_some_and(|p| p.work_id == work_id)
        })
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    for delegate_id in open {
        let _ = crate::delegate_mcp::run_delegate_close(
            shared,
            sid,
            crate::delegate_mcp::DelegateCloseArgs { delegate_id },
        );
    }
    tracing::warn!(
        session = sid,
        work_id,
        "planner worker cancelled after its shutdown drain bound"
    );
}
