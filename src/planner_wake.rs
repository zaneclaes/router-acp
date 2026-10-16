//! One router-owned idle-parent wake loop. Pending wakes survive process exit,
//! but execution requires a running router and an explicitly loaded session.

use std::sync::Arc;

use agent_client_protocol::schema::v1::{ContentBlock, PromptRequest};
use serde_json::{Value, json};

use crate::planner_skills::PlannerRole;
use crate::planner_workflow::{self, RunStatus, WorkerIdentity};
use crate::session::Shared;

pub(crate) struct PromptActivity {
    shared: Arc<Shared>,
    sid: String,
    wake: bool,
}

impl PromptActivity {
    pub(crate) fn client(shared: &Arc<Shared>, sid: &str) -> Self {
        shared.with_session(sid, |s| s.prompt_activity += 1);
        Self {
            shared: shared.clone(),
            sid: sid.into(),
            wake: false,
        }
    }

    fn idle(shared: &Arc<Shared>, sid: &str) -> Option<Self> {
        shared
            .with_session(sid, |s| {
                // A cancel ends the turn it interrupted, not the run: once that
                // turn is over, wakes resume with a fresh turn.
                if s.prompt_activity != 0
                    || s.pinning
                    || s.pin.is_none()
                    || s.capturing_summary.is_some()
                {
                    return false;
                }
                s.prompt_activity += 1;
                s.planner_wake_active = true;
                s.cancelled = false;
                true
            })
            .filter(|idle| *idle)
            .map(|_| Self {
                shared: shared.clone(),
                sid: sid.into(),
                wake: true,
            })
    }
}

impl Drop for PromptActivity {
    fn drop(&mut self) {
        self.shared.with_session(&self.sid, |s| {
            s.prompt_activity = s.prompt_activity.saturating_sub(1);
            if self.wake {
                s.planner_wake_active = false;
            }
        });
    }
}

/// Persist unanswered permission/question callbacks. A disconnect is not an
/// answer, so recovery cannot silently treat the approval as granted.
pub(crate) fn approval_started(shared: &Arc<Shared>, sid: &str, id: &str) -> Result<(), String> {
    if planner_workflow::load(shared, sid)?.is_none() {
        return Ok(());
    }
    planner_workflow::mutate(shared, sid, |run| {
        run.approval_waits.insert(id.into());
        Ok(())
    })
    .map(|_| ())
}

pub(crate) fn approval_answered(shared: &Arc<Shared>, sid: &str, id: &str) -> Result<(), String> {
    if planner_workflow::load(shared, sid)?.is_none() {
        return Ok(());
    }
    planner_workflow::mutate(shared, sid, |run| {
        run.approval_waits.remove(id);
        Ok(())
    })
    .map(|_| ())
}

pub(crate) fn spawn(shared: &Arc<Shared>) -> tokio::task::JoinHandle<()> {
    let shared = shared.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tick.tick().await;
            let ids = shared
                .sessions
                .lock()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for sid in ids {
                let Some(run) = planner_workflow::load(&shared, &sid).ok().flatten() else {
                    continue;
                };
                // A missed stop caused by router death is reconciled before
                // parent work resumes. Never infer that the child survived.
                for (id, work) in &run.works {
                    if let Some(attempt) = &work.attempt
                        && !attempt.ended
                        && !planner_workflow::owner_alive(attempt)
                    {
                        let actor = WorkerIdentity {
                            work_id: id.clone(),
                            attempt_id: attempt.id.clone(),
                        };
                        let _ = planner_workflow::attempt_ended(&shared, &sid, &actor, None,
                                "Owning router exited. Reconcile preserved workspace/evidence before replacement.".into(), true);
                    }
                }
                close_stale_delegates(&shared, &sid, &run);
                let now = chrono::Utc::now().timestamp();
                heartbeat(&shared, &sid, &run, now);
                crate::planner_capacity::spawn_tick(&shared, &sid, now);
                let queued_input = run.parent_queue.first().is_some_and(|input| {
                    run.wakes
                        .get(&format!("input:{}", input.input_id))
                        .is_some_and(|wake| {
                            wake.deliveries < 3
                                && wake.retry_after <= chrono::Utc::now().timestamp()
                        })
                });
                if !run.parent_queue.is_empty() && !queued_input {
                    continue;
                }
                if !run.approval_waits.is_empty()
                    || (!queued_input
                        && (run.status != RunStatus::Running
                            || !run
                                .wakes
                                .values()
                                .any(|w| !w.acknowledged && w.deliveries < 3)))
                {
                    continue;
                }
                let Some(activity) = PromptActivity::idle(&shared, &sid) else {
                    continue;
                };
                let shared = shared.clone();
                tokio::spawn(async move {
                    let _activity = activity;
                    if let Err(error) = deliver(&shared, &sid).await {
                        tracing::warn!(session = sid, %error, "planner wake remains pending for bounded retry or explicit resume");
                    }
                });
            }
        }
    })
}

/// A planner worker's sub-session is kept open after its turn so the same
/// attempt can take a follow-up turn with its context. Each attempt runs in its
/// own agent process, so once the attempt can take no further turn (a newer
/// attempt replaced it, or its work is finished, integrated, blocked or
/// watching) the open session only holds memory: a long run left over a
/// hundred idle agent processes and their MCP servers behind. Close those.
pub fn close_stale_delegates(shared: &Arc<Shared>, sid: &str, run: &planner_workflow::PlannerRun) {
    use planner_workflow::WorkStatus;
    let stale = shared
        .live_delegates
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, d)| d.parent_sid == sid)
        .filter_map(|(id, d)| {
            let identity = d.planner.as_ref()?;
            let current = run.works.get(&identity.work_id).is_some_and(|w| {
                w.attempt.as_ref().is_some_and(|a| {
                    a.id == identity.attempt_id
                        && (!a.ended
                            || !matches!(
                                w.status,
                                WorkStatus::Finished
                                    | WorkStatus::Integrated
                                    | WorkStatus::Blocked
                                    | WorkStatus::Watching
                            ))
                })
            });
            (!current).then(|| id.clone())
        })
        .collect::<Vec<_>>();
    for delegate_id in stale {
        // A completing turn or an explicit close may get there first.
        let _ = crate::delegate_mcp::run_delegate_close(
            shared,
            sid,
            crate::delegate_mcp::DelegateCloseArgs { delegate_id },
        );
    }
}

/// A parent idle this long with a free worker slot gets a heartbeat wake, and
/// another every time it stays idle that long again. There is no backoff: a
/// run only ends when its work is done, and a parent that ended its turn on a
/// reason of its own is woken to act on it, not left to wait (a release once
/// sat overnight behind a "blocker" that was a defect in its own build).
const HEARTBEAT_SECS: i64 = 20 * 60;

pub fn live_workers(run: &planner_workflow::PlannerRun) -> usize {
    run.works
        .values()
        .filter(|w| w.attempt.as_ref().is_some_and(|a| !a.ended))
        .count()
}

/// The worker census every wake carries, so a parent below its limit sees the
/// free slots and the interrupted works waiting for a retry.
pub fn capacity_line(shared: &Arc<Shared>, run: &planner_workflow::PlannerRun) -> String {
    let cap = shared.cfg.delegation.max_concurrent;
    let live = live_workers(run);
    let mut interrupted = run
        .works
        .iter()
        .filter(|(_, w)| w.status == planner_workflow::WorkStatus::Interrupted)
        .map(|(id, _)| id.as_str())
        .collect::<Vec<_>>();
    interrupted.sort_unstable();
    let mut line = format!(
        "[Capacity] {live} of {cap} worker slots in use, {} free.",
        cap.saturating_sub(live)
    );
    if !interrupted.is_empty() {
        line.push_str(&format!(
            " Interrupted, awaiting retry: {}.",
            interrupted.join(", ")
        ));
    }
    if live < cap {
        line.push_str(
            " Re-read the plan source for work that became runnable since your last turn (a \
             changed roadmap, a cleared blocker, recovered host memory or disk), retry \
             interrupted works, and fill the free slots; a reason recorded in an earlier turn \
             does not hold a slot without being re-checked now. Stopping is correct only when \
             all of the work is done; a claimed blocker is a defect to investigate.",
        );
    }
    line.push_str(&crate::planner_capacity::census(
        shared,
        run,
        chrono::Utc::now().timestamp_millis(),
    ));
    line
}

/// A parent that ended its turn blocked on something outside the run (a
/// sign-in, host memory, a person, a roadmap that has since changed) may have
/// no child event coming that would wake it when that clears. While any
/// worker slot is free, wake it on a backed-off timer so it re-checks.
pub fn heartbeat(shared: &Arc<Shared>, sid: &str, run: &planner_workflow::PlannerRun, now: i64) {
    if run.status != RunStatus::Running
        || !run.approval_waits.is_empty()
        || !run.parent_queue.is_empty()
        || run
            .wakes
            .values()
            .any(|w| !w.acknowledged && w.deliveries < 3)
        // Capacity wakes own an idle parent while the host lists unheld
        // candidates or cannot answer; the heartbeat stays for "no work".
        || crate::planner_capacity::supersedes_heartbeat(shared, run, now * 1000)
    {
        return;
    }
    let live = live_workers(run);
    let cap = shared.cfg.delegation.max_concurrent;
    if live >= cap {
        if run.idle_heartbeats != 0 {
            let _ = planner_workflow::mutate(shared, sid, |run| {
                run.idle_heartbeats = 0;
                Ok(())
            });
        }
        return;
    }
    // An unloaded (unpinned) parent keeps the heartbeat pending until it is
    // loaded; delivery itself waits for that.
    if shared
        .with_session(sid, |s| s.prompt_activity != 0)
        .unwrap_or(true)
    {
        return;
    }
    let Some((last, _)) = shared.state.lock().unwrap().last_activity(sid) else {
        return;
    };
    let idle = now - last;
    if idle < HEARTBEAT_SECS {
        return;
    }
    let reason = format!(
        "heartbeat: {live} of {cap} worker slots are in use and the parent has been idle for {} \
         min. A run ends only when all of its work is done; any other reason to stop is a \
         hypothesis to disprove now, not a state to wait in. For each blocker your last turn \
         reported, prove where it is: reproduce it, check whether the previous build or path \
         worked, and probe the system yourself. Anything in our own code, build, config or \
         environment is a defect: admit a worker to fix it now. Even a proven human-only action \
         holds only its own item. Fill every free slot with other runnable work this turn.",
        idle / 60
    );
    let _ = planner_workflow::mutate(shared, sid, |run| {
        if run
            .wakes
            .values()
            .any(|w| !w.acknowledged && w.deliveries < 3)
        {
            return Ok(());
        }
        run.idle_heartbeats += 1;
        run.wakes.insert(
            format!("heartbeat:{}", uuid::Uuid::new_v4()),
            planner_workflow::Wake {
                reason: reason.clone(),
                ..Default::default()
            },
        );
        Ok(())
    });
}

async fn deliver(shared: &Arc<Shared>, sid: &str) -> Result<(), String> {
    let run = planner_workflow::load(shared, sid)?.ok_or("planner state missing")?;
    if let Some(input) = run.parent_queue.first() {
        return deliver_queued_input(shared, sid, input.clone()).await;
    }
    let now = chrono::Utc::now().timestamp();
    let (pid, started) = planner_workflow::process_identity();
    let mut ids = Vec::new();
    let run = planner_workflow::mutate(shared, sid, |run| {
        ids.clear();
        if run.status != RunStatus::Running || !run.approval_waits.is_empty() {
            return Ok(());
        }
        for (id, wake) in &mut run.wakes {
            if wake.acknowledged || wake.deliveries >= 3 || wake.retry_after > now {
                continue;
            }
            if wake.claim_pid != 0 {
                let claim = crate::planner_workflow::Attempt {
                    id: id.clone(),
                    router_pid: wake.claim_pid,
                    router_started_at_ms: wake.claim_started_at_ms,
                    delegate_id: None,
                    ended: false,
                };
                if planner_workflow::owner_alive(&claim) {
                    continue;
                }
            }
            wake.claim_pid = pid;
            wake.claim_started_at_ms = started;
            wake.deliveries += 1;
            wake.retry_after = (chrono::Utc::now() + chrono::Duration::seconds(30)).timestamp();
            ids.push(id.clone());
            if ids.len() == 32 {
                break;
            }
        }
        Ok(())
    })?;
    if ids.is_empty() {
        return Ok(());
    }
    let wakes = ids.iter().map(|id| json!({"wake_id":id,"work_id":run.wakes[id].work_id,"reason":run.wakes[id].reason})).collect::<Vec<_>>();
    let input_ids = ids
        .iter()
        .filter_map(|id| id.strip_prefix("input:").map(str::to_string))
        .collect::<Vec<_>>();
    let role = if input_ids.iter().any(|id| run.commands.contains(id)) {
        match run.phase {
            crate::config::PlannerPhase::Planning => PlannerRole::CreatePlan,
            crate::config::PlannerPhase::Implementation => PlannerRole::SelectPlan,
        }
    } else {
        PlannerRole::ReviewWork
    };
    let text = format!(
        "{}\n[Router-owned child wake]\n{}\n{}\nReconcile these exact work identities and their evidence. Acknowledge each wake through planner_workflow after reconciliation. Preserve human authorization. Continue independent authorized work, or report the concrete waiting reason.",
        run.policy.instructions(role),
        serde_json::to_string(&wakes).map_err(|e| e.to_string())?,
        capacity_line(shared, &run)
    );
    let mut blocks = vec![ContentBlock::from(text)];
    blocks.extend(planner_workflow::parent_input_blocks(
        shared, sid, &input_ids,
    )?);
    planner_workflow::queue_delivery(shared, sid, sid, sid, input_ids);
    let prompt = PromptRequest::new(sid.to_string(), blocks).meta(
        serde_json::from_value::<agent_client_protocol::schema::v1::Meta>(
            json!({"router_acp":{"agent_origin":true,"origin":"planner-wake","planner_role":role}}),
        )
        .ok(),
    );
    shared.state.lock().unwrap().log(
        sid,
        &crate::state::LogEntry {
            kind: "planner_wake_delivery".into(),
            role: "router".into(),
            summary: "Resume exact idle parent for child reconciliation".into(),
            detail: Some(Value::from(wakes)),
            ..Default::default()
        },
    );
    let result =
        crate::session::run_primary_turn(shared.clone(), sid.to_string(), prompt, None).await;
    planner_workflow::mutate(shared, sid, |run| {
        for id in &ids {
            if let Some(wake) = run.wakes.get_mut(id)
                && wake.claim_pid == pid
                && wake.claim_started_at_ms == started
            {
                wake.claim_pid = 0;
                // Router-owned capacity/in-flight wakes are recreated while
                // their condition holds; one delivery answers each.
                if crate::planner_capacity::ROUTER_WAKE_PREFIXES
                    .iter()
                    .any(|prefix| id.starts_with(prefix))
                {
                    wake.acknowledged = true;
                }
            }
        }
        Ok(())
    })?;
    result.map(|_| ()).map_err(|e| e.to_string())
}

async fn deliver_queued_input(
    shared: &Arc<Shared>,
    sid: &str,
    input: planner_workflow::QueuedParentInput,
) -> Result<(), String> {
    let wake_id = format!("input:{}", input.input_id);
    let (pid, started) = planner_workflow::process_identity();
    planner_workflow::mutate(shared, sid, |run| {
        if run.parent_queue.first().map(|queued| &queued.input_id) != Some(&input.input_id) {
            return Err("original parent input is no longer first in the queue".into());
        }
        let wake = run
            .wakes
            .get_mut(&wake_id)
            .ok_or("queued input wake missing")?;
        if wake.claim_pid != 0
            && planner_workflow::owner_alive(&planner_workflow::Attempt {
                id: wake_id.clone(),
                router_pid: wake.claim_pid,
                router_started_at_ms: wake.claim_started_at_ms,
                delegate_id: None,
                ended: false,
            })
        {
            return Err("original parent input already has a live delivery owner".into());
        }
        wake.claim_pid = pid;
        wake.claim_started_at_ms = started;
        wake.deliveries += 1;
        wake.retry_after = (chrono::Utc::now() + chrono::Duration::seconds(30)).timestamp();
        Ok(())
    })?;
    let result = match crate::session::prepare_queued_parent_prompt(
        shared.clone(),
        sid.into(),
        input.request,
    ) {
        Ok(req) => crate::session::run_primary_turn(shared.clone(), sid.into(), req, None).await,
        Err(error) => Err(error),
    };
    planner_workflow::mutate(shared, sid, |run| {
        if let Some(wake) = run.wakes.get_mut(&wake_id)
            && wake.claim_pid == pid
            && wake.claim_started_at_ms == started
        {
            wake.claim_pid = 0;
            if result.is_ok()
                && run.parent_queue.first().map(|queued| &queued.input_id) == Some(&input.input_id)
            {
                run.parent_queue.remove(0);
            }
        }
        Ok(())
    })?;
    if let Err(error) = &result {
        crate::session::notify_user(
            shared,
            sid,
            format!(
                "router-acp · original queued input {} remains pending: {error}",
                input.input_id
            ),
        );
    }
    result.map(|_| ()).map_err(|e| e.to_string())
}
