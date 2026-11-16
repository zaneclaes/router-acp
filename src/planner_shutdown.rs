//! `_router/planner_shutdown`: a graceful drain of a planner run.
//!
//! kory sends the extension request `{"sessionId"}`; the router answers
//! `{"accepted":true}` at once (an error when the session has no planner run)
//! and then, reporting each step as `_router/shutdown_progress`:
//!
//! 1. sets the run `draining`: admissions, capacity and audit wakes,
//!    auto-admission, worker resumptions, in-flight wakes and turn-end gates
//!    all stop (each of them runs only for a `running` run);
//! 2. interrupts only active worker attempts to record their resume state;
//!    ended attempts are already recorded and never get a new turn;
//! 3. saves the final state through `routers.planner.shutdown` (`saving`,
//!    then `saved` or `failed`; a failed save does not stop the drain);
//! 4. prompts the parent once to update its continuation from that final state;
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
use crate::planner_workflow::{self, PlannerRun, RunStatus};
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
/// Where the parent records its resume point is the pluggable `routers.planner.continuation`
/// (default the Markdown layout's `Docs/CONTINUE.md`), never a fixed project path.
pub fn parent_prompt(planner: &crate::config::PlannerRouterConfig) -> String {
    format!(
        "Shutdown requested by the operator: the live children have stopped. Update {} now \
         with their final recorded state and the exact resume point. Use the state already \
         recorded; do not start or resume children, audit, build, commit, push, or wait for CI. \
         Make this one brief state-saving turn, then stop.",
        planner.continuation
    )
}
pub const WORKER_PROMPT: &str = "Shutdown requested: interrupt your current work, record your \
     current state and exact resume point, including any uncommitted or unpushed changes, \
     then hand off `waiting` and stop. Do not build, commit, push, or wait for CI.";
const PARENT_TURN_LIMIT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(500);

/// One active drain, in process.
#[derive(Default)]
pub struct Drain {
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
    match planner_workflow::mutate(shared, sid, |run| {
        prior = run.status;
        run.status = RunStatus::Draining;
        Ok(())
    }) {
        Ok(_) => (),
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

    // Stop the coordinator's current work while its children save state. Only
    // the final continuation update may run after their state is collected.
    if shared
        .with_session(sid, |s| s.prompt_activity > 0)
        .unwrap_or(false)
    {
        with_runtime(shared, sid, |rt| rt.preempted = true);
        shared.with_session(sid, |s| s.cancelled = true);
        if let Some(interrupt) = shared.background_interrupts.lock().unwrap().remove(sid) {
            let _ = interrupt.send(crate::session::BackgroundInterrupt::Cancel);
        }
        if let Some((conn, child, _)) = shared.pinned_route(sid) {
            let _ = conn.send_notification(CancelNotification::new(child));
        }
        notify(
            shared,
            sid,
            "draining",
            "interrupting the parent turn for shutdown",
            &saved,
            &workers,
        );
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
            "interrupting {} live worker(s) to save state and stop",
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

    notify(
        shared,
        sid,
        "saving",
        "saving the final recorded worker state",
        &saved,
        &workers,
    );
    let final_run = planner_workflow::load(shared, sid);
    let result = match final_run {
        Ok(Some(run)) => save_state(shared, sid, &run, prior, &cfg).await,
        Ok(None) => Err("planner run disappeared".into()),
        Err(error) => Err(error),
    };
    match result {
        Ok((paths, summary)) => {
            saved = paths;
            notify(shared, sid, "saved", &summary, &saved, &workers);
        }
        Err(error) => notify(
            shared,
            sid,
            "failed",
            &format!("final resume save failed: {error}"),
            &saved,
            &workers,
        ),
    }
    if matches!(prior, RunStatus::Running | RunStatus::Draining) {
        let outcome = prompt_parent(shared, sid, &saved, &workers).await;
        notify(shared, sid, "draining", &outcome, &saved, &workers);
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
            "native run state is persisted; no additional shutdown host is configured".into(),
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

/// One short final continuation update, with cancellation on timeout.
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
                "waiting for the parent's final continuation update", saved, workers),
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
    if shared
        .with_session(sid, |s| s.prompt_activity > 0)
        .unwrap_or(false)
    {
        notify(
            shared,
            sid,
            "draining",
            "interrupting the parent turn for shutdown",
            saved,
            workers,
        );
        if !tokio::time::timeout(
            PARENT_TURN_LIMIT,
            crate::planner_preempt::preempt(shared, sid),
        )
        .await
        .unwrap_or(false)
        {
            return "the router-owned parent turn did not stop within its cancellation bound; the saved resume state stands and workers drain anyway".into();
        }
    }
    let activity = loop {
        if let Some(activity) = crate::planner_wake::PromptActivity::idle(shared, sid) {
            break activity;
        }
        if started.elapsed() >= PARENT_TURN_LIMIT {
            return "the parent did not settle within the shutdown bound; the saved state remains available".into();
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
    let prompt = PromptRequest::new(
        sid.to_string(),
        vec![ContentBlock::from(parent_prompt(
            &shared.cfg.routers.planner,
        ))],
    )
    .meta(
        serde_json::from_value::<agent_client_protocol::schema::v1::Meta>(
            json!({"router_acp":{"agent_origin":true,"origin":"planner-shutdown"}}),
        )
        .ok(),
    );
    let task_shared = shared.clone();
    let task_sid = sid.to_string();
    let mut turn = tokio::spawn(async move {
        let _activity = activity;
        crate::session::run_primary_turn(task_shared, task_sid, prompt, None).await
    });
    let remaining = PARENT_TURN_LIMIT.saturating_sub(started.elapsed());
    match tokio::time::timeout(remaining, &mut turn).await {
        Ok(Ok(Ok(_))) => "the parent recorded its resume point and ended its turn".into(),
        Ok(Ok(Err(error))) => format!("the parent's shutdown turn failed: {error}"),
        Ok(Err(error)) => format!("the parent's shutdown turn failed: {error}"),
        Err(_) => {
            if let Some((conn, down_sid, _)) = shared.pinned_route(sid) {
                let _ = conn.send_notification(CancelNotification::new(down_sid));
            }
            turn.abort();
            let _ = turn.await;
            "the final parent update exceeded 30 seconds and was cancelled; the saved worker state remains available".into()
        }
    }
}

/// Only unfinished attempts need shutdown. An idle keep-open session is not work.
fn live_workers(shared: &Shared, sid: &str) -> Vec<String> {
    let Some(run) = planner_workflow::load(shared, sid).ok().flatten() else {
        return Vec::new();
    };
    run.works
        .iter()
        .filter(|(_, w)| w.attempt.as_ref().is_some_and(|a| !a.ended))
        .map(|(id, _)| id.clone())
        .collect()
}

/// Interrupt one existing worker. Never create a delegate or restart an ended attempt.
/// Steering lets the active child save its state; an adapter which cannot steer is
/// cancelled, preserving the state already recorded throughout the attempt.
async fn drain_worker(
    shared: &Arc<Shared>,
    sid: &str,
    work_id: String,
    bound: Duration,
    tx: tokio::sync::mpsc::UnboundedSender<(String, String)>,
) {
    let deadline = Instant::now() + bound;
    let mut notified = false;
    let send = |status: &str| {
        let _ = tx.send((work_id.clone(), status.to_string()));
    };
    loop {
        if Instant::now() >= deadline {
            cancel_worker(shared, sid, &work_id).await;
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
            if !notified {
                let route = with_runtime(shared, sid, |rt| rt.children.get(&work_id).cloned());
                if let Some((process, child)) = route {
                    let delivered = if let Some(conn) = shared.target_conn(&process) {
                        let request = agent_client_protocol::UntypedMessage::new(
                            "_session/steering",
                            json!({"sessionId":child,"prompt":[ContentBlock::from(WORKER_PROMPT)],
                                "_meta":{"steering":{"idleBehavior":"promptRequired"}}}),
                        )
                        .expect("shutdown steering serializes");
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        matches!(tokio::time::timeout(remaining.min(Duration::from_secs(3)),
                            conn.send_request(request).block_task()).await,
                            Ok(Ok(reply)) if reply["outcome"] == "injected")
                    } else {
                        false
                    };
                    if !delivered {
                        cancel_worker(shared, sid, &work_id).await;
                        send("cancelled");
                        return;
                    }
                    notified = true;
                }
            }
            send(if notified { "notified" } else { "running" });
            tokio::time::sleep(POLL).await;
            continue;
        }
        send("stopped");
        return;
    }
}

/// Cancel a worker past its drain bound: its running turn, then any open
/// sub-session it holds.
async fn cancel_worker(shared: &Arc<Shared>, sid: &str, work_id: &str) {
    let child = with_runtime(shared, sid, |rt| {
        if let Some(drain) = rt.drain.as_mut() {
            drain.cancelled.insert(work_id.to_string());
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
    // Let the cancelled turn persist its end before the final host snapshot.
    // A stuck adapter cannot hold the whole shutdown; its native attempt then
    // remains visibly unfinished for adoption instead of being fabricated done.
    let settle = Instant::now() + Duration::from_secs(3);
    while Instant::now() < settle
        && with_runtime(shared, sid, |rt| rt.children.contains_key(work_id))
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tracing::warn!(
        session = sid,
        work_id,
        "planner worker received shutdown cancellation; recorded attempt state retained"
    );
}
