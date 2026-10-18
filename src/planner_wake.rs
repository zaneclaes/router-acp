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

    pub(crate) fn idle(shared: &Arc<Shared>, sid: &str) -> Option<Self> {
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
        let ended = self.shared.with_session(&self.sid, |s| {
            s.prompt_activity = s.prompt_activity.saturating_sub(1);
            if self.wake {
                s.planner_wake_active = false;
            }
            s.prompt_activity == 0
        });
        // The parent's turn is over: the turn-end gate judges it.
        if ended == Some(true) {
            crate::planner_guard::parent_turn_ended(
                &self.shared,
                &self.sid,
                chrono::Utc::now().timestamp(),
            );
        }
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
                // This router has the parent loaded: it owns the run. A run
                // whose owner died is what a new `/implement` resumes
                // (`crate::planner_adopt`).
                let me = planner_workflow::RunOwner::current();
                if run.owner != Some(me) && run.status != RunStatus::Superseded {
                    let _ = planner_workflow::mutate(&shared, &sid, |run| {
                        run.owner = Some(me);
                        Ok(())
                    });
                }
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
                        .is_some_and(|wake| wake.retry_after <= chrono::Utc::now().timestamp())
                });
                if !run.parent_queue.is_empty() && !queued_input {
                    continue;
                }
                // Planner wakes never give up: an undelivered or unanswered
                // wake is retried on a capped backoff, not dropped.
                if !run.approval_waits.is_empty()
                    || (!queued_input
                        && (run.status != RunStatus::Running
                            || !run.wakes.values().any(|w| !w.acknowledged)))
                {
                    continue;
                }
                let Some(activity) = PromptActivity::idle(&shared, &sid) else {
                    steer_pending(&shared, &sid, &run);
                    continue;
                };
                // Agent corrections, like wakes, must yield to live operator
                // input. Only an actual operator request owns a protected turn.
                let router_owned = run
                    .parent_queue
                    .first()
                    .is_none_or(|input| !planner_workflow::human_prompt(&input.request));
                if router_owned {
                    crate::planner_preempt::begin_router_turn(&shared, &sid);
                }
                let shared = shared.clone();
                tokio::spawn(async move {
                    let _activity = activity;
                    if let Err(error) = deliver(&shared, &sid).await {
                        tracing::warn!(session = sid, %error, "planner wake remains pending for bounded retry or explicit resume");
                    }
                    if router_owned {
                        crate::planner_preempt::end_router_turn(&shared, &sid);
                    }
                });
            }
        }
    })
}

/// A router-owned turn is in flight and something actionable is pending. A
/// parent that spends half an hour on receipts otherwise learns of a failed CI
/// job, a release transition or a monitor correction only at its next turn
/// boundary (2026-10-18: three staging smoke jobs failed and two corrections
/// queued while the parent rewrote CONTINUE; nothing reached it for 25 minutes).
/// Steer one pending item per tick into the running turn with
/// `_session/steering`, exactly as operator input is steered, once per item per
/// turn. The durable wake or queued input is still delivered and acknowledged at
/// the boundary; the steer only lets the parent act on it now. An operator turn
/// is never steered into (`router_turn_in_flight` is false for it), and an item
/// a delivery already claims is left to that delivery.
fn steer_pending(shared: &Arc<Shared>, sid: &str, run: &planner_workflow::PlannerRun) {
    if !crate::planner_preempt::router_turn_in_flight(shared, sid) {
        return;
    }
    let steered = crate::planner_guard::with_runtime(shared, sid, |rt| rt.steered.clone());
    let Some((key, text)) = pending_steer(&run.parent_queue, &run.wakes, &steered) else {
        return;
    };
    let claimed = crate::planner_guard::with_runtime(shared, sid, |rt| {
        if rt.steering {
            false
        } else {
            rt.steering = true;
            true
        }
    });
    if !claimed {
        return;
    }
    let shared = shared.clone();
    let sid = sid.to_string();
    tokio::spawn(async move {
        let blocks = vec![ContentBlock::from(text.clone())];
        let injected = crate::planner_preempt::steer(&shared, &sid, &blocks).await;
        crate::planner_guard::with_runtime(&shared, &sid, |rt| {
            rt.steering = false;
            if injected && rt.router_turn {
                rt.steered.insert(key.clone());
            }
        });
        if injected {
            shared.state.lock().unwrap().log(
                &sid,
                &crate::state::LogEntry {
                    kind: "planner_steer".into(),
                    role: "router".into(),
                    summary: format!("Steered pending {key} into the running parent turn"),
                    detail: Some(Value::from(text)),
                    ..Default::default()
                },
            );
        }
    });
}

/// The one pending item to steer now: the queued agent correction first, then
/// the first unacknowledged release/observation wake, skipping anything already
/// steered this turn or claimed by a live delivery. Pure, for tests.
pub(crate) fn pending_steer(
    queue: &[planner_workflow::QueuedParentInput],
    wakes: &std::collections::BTreeMap<String, planner_workflow::Wake>,
    steered: &std::collections::HashSet<String>,
) -> Option<(String, String)> {
    fn claimed(wake: &planner_workflow::Wake, id: &str) -> bool {
        wake.claim_pid != 0
            && planner_workflow::owner_alive(&planner_workflow::Attempt {
                id: id.to_string(),
                router_pid: wake.claim_pid,
                router_started_at_ms: wake.claim_started_at_ms,
                delegate_id: None,
                ended: false,
            })
    }
    if let Some(input) = queue.iter().find(|input| {
        !wakes
            .get(&format!("input:{}", input.input_id))
            .is_some_and(|w| w.acknowledged)
    }) {
        let key = format!("input:{}", input.input_id);
        let live_delivery = wakes.get(&key).is_some_and(|w| claimed(w, &key));
        if !steered.contains(&key)
            && !live_delivery
            && !planner_workflow::human_prompt(&input.request)
        {
            let body = input
                .request
                .prompt
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Text(t) => Some(t.text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            return Some((
                key,
                format!(
                    "[Router steer] Agent correction `{}` is queued for this parent. It is redelivered for \
                     acknowledgment when this turn ends; read it now and act on it in this turn if it changes \
                     what you are doing:\n{body}",
                    input.input_id
                ),
            ));
        }
    }
    let (id, wake) = wakes.iter().find(|(id, w)| {
        !w.acknowledged
            && wake_priority(id) <= 1
            && !id.starts_with("input:")
            && !steered.contains(*id)
            && !claimed(w, id)
    })?;
    Some((
        id.clone(),
        format!(
            "[Router steer] Pending wake `{id}` (work {}): {}. It is redelivered when this turn ends; a failed \
             CI job or a release transition is actionable now, in this turn.",
            wake.work_id, wake.reason
        ),
    ))
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
    let cap = shared.delegate_limit();
    let live = live_workers(run);
    let mut interrupted = run
        .works
        .iter()
        .filter(|(id, w)| {
            w.status == planner_workflow::WorkStatus::Interrupted
                && !w.paused
                && run
                    .capacity
                    .resumes
                    .get(*id)
                    .is_none_or(|r| r.suspended.is_none())
        })
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
    let recovery = run
        .capacity
        .resumes
        .iter()
        .filter(|(_, state)| state.suspended.is_some())
        .map(|(id, _)| id.as_str())
        .collect::<Vec<_>>();
    if !recovery.is_empty() {
        line.push_str(&format!(" Reconciliation required (automatic retries suspended): {}. Repair the prerequisite before an explicit retry; do not re-delegate an unchanged failure.", recovery.join(", ")));
    }
    let ready = run
        .works
        .values()
        .filter(|w| w.status == planner_workflow::WorkStatus::Finished)
        .count();
    let review = run
        .works
        .values()
        .filter(|w| w.status == planner_workflow::WorkStatus::ReviewPending)
        .count();
    line.push_str(&format!(" [Progress] {ready} work packets await integration; {review} await review. These are not completed plans. Prioritize actionable release gates and dispositions, while independent workers continue."));
    if let Some(at) = run
        .works
        .values()
        .filter(|work| work.status == planner_workflow::WorkStatus::Finished)
        .filter_map(|work| work.ready_at_ms)
        .min()
    {
        line.push_str(&format!(
            " Oldest measured ready packet: {}s.",
            (chrono::Utc::now().timestamp_millis() - at).max(0) / 1000
        ));
    }
    for (id, release) in run.releases.iter().filter(|(_, release)| release.active()) {
        line.push_str(&format!(" Release {id}: {:?}.", release.stage()));
    }
    if let Some(current) = run
        .capacity
        .audit
        .as_ref()
        .filter(|audit| audit.ok)
        .and_then(|audit| audit.completed_guides.as_ref())
        && let Some(initial) = &run.completed_guides_at_start
    {
        line.push_str(&format!(" Repository host confirms {} guides now in Completed on staging; {} new paths since this run's first audit.", current.len(), current.difference(initial).count()));
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
    line.push_str(&crate::planner_guard::census(shared, run));
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
        || run.wakes.values().any(|w| !w.acknowledged)
        // Capacity wakes own an idle parent while the host lists unheld
        // candidates or cannot answer; the heartbeat stays for "no work".
        || crate::planner_capacity::supersedes_heartbeat(shared, run, now * 1000)
    {
        return;
    }
    let live = live_workers(run);
    let cap = shared.delegate_limit();
    // Full slots excuse the heartbeat only while the audit is clean.
    if live >= cap && !crate::planner_guard::audit_open(run) {
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
        if run.wakes.values().any(|w| !w.acknowledged) {
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

fn wake_priority(id: &str) -> u8 {
    if id.starts_with("input:") {
        0
    } else if id.starts_with("release:") || id.starts_with("observation:") {
        1
    } else if id.starts_with("recovery:") {
        2
    } else if id.starts_with("capacity:")
        || id.starts_with("inflight:")
        || id.starts_with("gate:")
        || id.starts_with("audit:")
    {
        4
    } else {
        3
    }
}

async fn deliver(shared: &Arc<Shared>, sid: &str) -> Result<(), String> {
    let run = planner_workflow::load(shared, sid)?.ok_or("planner state missing")?;
    if let Some(input) = run.parent_queue.first() {
        if planner_workflow::human_prompt(&input.request) {
            // Actual operator turns stay protected; monitor input does not.
            crate::planner_preempt::end_router_turn(shared, sid);
        }
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
        // Keep every durable wake, but actionable release/review transitions
        // lead repetitive capacity reminders. Operator input always wins.
        let mut ordered = run.wakes.keys().cloned().collect::<Vec<_>>();
        ordered.sort_by_key(|id| wake_priority(id));
        for id in ordered {
            let wake = run.wakes.get_mut(&id).expect("listed above");
            if wake.acknowledged || wake.retry_after > now {
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
            wake.retry_after = now + crate::planner_guard::wake_retry_secs(wake.deliveries);
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
    // Cancelled for operator input, or by the stuck-turn watchdog: this turn
    // answered nothing, so its wakes stay pending and go again right after
    // the next turn (queued operator input first).
    let stalled = crate::planner_watchdog::take_stalled(shared, sid);
    let preempted = crate::planner_preempt::preempted(shared, sid) || stalled;
    planner_workflow::mutate(shared, sid, |run| {
        for id in &ids {
            if let Some(wake) = run.wakes.get_mut(id)
                && wake.claim_pid == pid
                && wake.claim_started_at_ms == started
            {
                wake.claim_pid = 0;
                if preempted {
                    wake.retry_after = 0;
                    continue;
                }
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
    let agent_input = !planner_workflow::human_prompt(&input.request);
    let wake_id = format!("input:{}", input.input_id);
    let (pid, started) = planner_workflow::process_identity();
    planner_workflow::mutate(shared, sid, |run| {
        if run
            .inputs
            .get(&input.input_id)
            .is_some_and(|i| i.resolved())
        {
            return Err("original parent input is already resolved".into());
        }
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
        wake.retry_after =
            chrono::Utc::now().timestamp() + crate::planner_guard::wake_retry_secs(wake.deliveries);
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
    // Cancellation can return Ok(Cancelled). Keep an interrupted correction
    // durable until a subsequent delivery finishes, just as for normal wakes.
    let stalled = crate::planner_watchdog::take_stalled(shared, sid);
    let interrupted = agent_input
        && (crate::planner_preempt::preempted(shared, sid)
            || stalled
            || result.as_ref().is_ok_and(|reply| {
                reply.stop_reason == agent_client_protocol::schema::v1::StopReason::Cancelled
            }));
    planner_workflow::mutate(shared, sid, |run| {
        if let Some(wake) = run.wakes.get_mut(&wake_id)
            && wake.claim_pid == pid
            && wake.claim_started_at_ms == started
        {
            wake.claim_pid = 0;
            if interrupted {
                wake.retry_after = 0;
            } else if result.is_ok()
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

#[cfg(test)]
mod steer_tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    #[test]
    fn resolved_input_is_not_steered_even_if_a_stale_queue_entry_remains() {
        let queue = vec![input("withdrawn", true), input("pending", true)];
        let wakes = BTreeMap::from([(
            "input:withdrawn".into(),
            planner_workflow::Wake {
                acknowledged: true,
                ..Default::default()
            },
        )]);
        assert!(pending_steer(&queue[..1], &wakes, &HashSet::new()).is_none());
        assert_eq!(
            pending_steer(&queue, &wakes, &HashSet::new()).unwrap().0,
            "input:pending"
        );
    }

    fn input(id: &str, agent: bool) -> planner_workflow::QueuedParentInput {
        let meta = if agent {
            json!({"router_acp": {"agent_origin": true, "input_id": id}})
        } else {
            json!({})
        };
        planner_workflow::QueuedParentInput {
            input_id: id.into(),
            request: PromptRequest::new(
                "s".to_string(),
                vec![ContentBlock::from(format!("body of {id}"))],
            )
            .meta(serde_json::from_value(meta).ok()),
        }
    }

    fn wake(reason: &str) -> planner_workflow::Wake {
        planner_workflow::Wake {
            reason: reason.into(),
            work_id: "w".into(),
            ..Default::default()
        }
    }

    #[test]
    fn agent_correction_steers_first_once_and_never_an_operator_prompt() {
        let mut wakes = BTreeMap::new();
        wakes.insert("observation:ci".to_string(), wake("ci job failed"));
        let queue = vec![input("fix-now", true)];
        let (key, text) =
            pending_steer(&queue, &wakes, &HashSet::new()).expect("steers the correction");
        assert_eq!(key, "input:fix-now");
        assert!(text.contains("body of fix-now") && text.contains("redelivered"));
        let steered = HashSet::from([key]);
        let (next, _) = pending_steer(&queue, &wakes, &steered).expect("then the observation");
        assert_eq!(next, "observation:ci");
        let operator = vec![input("typed", false)];
        let (key, _) =
            pending_steer(&operator, &wakes, &HashSet::new()).expect("skips the operator prompt");
        assert_eq!(
            key, "observation:ci",
            "an operator prompt is never steered by the router loop"
        );
    }

    #[test]
    fn only_release_and_observation_wakes_steer_and_acknowledged_or_claimed_ones_do_not() {
        let mut wakes = BTreeMap::new();
        wakes.insert("capacity:1".to_string(), wake("slots free"));
        wakes.insert("audit:2".to_string(), wake("defect"));
        assert!(
            pending_steer(&[], &wakes, &HashSet::new()).is_none(),
            "routine wakes wait for the boundary"
        );
        let mut done = wake("round passed");
        done.acknowledged = true;
        wakes.insert("release:r".to_string(), done);
        assert!(pending_steer(&[], &wakes, &HashSet::new()).is_none());
        let mut live = wake("ci job failed");
        live.claim_pid = std::process::id();
        live.claim_started_at_ms = planner_workflow::process_identity().1;
        wakes.insert("observation:claimed".to_string(), live);
        assert!(
            pending_steer(&[], &wakes, &HashSet::new()).is_none(),
            "a wake a live delivery claims is its"
        );
        wakes.insert("observation:open".to_string(), wake("ci job failed"));
        let (id, text) = pending_steer(&[], &wakes, &HashSet::new()).expect("the open observation");
        assert_eq!(id, "observation:open");
        assert!(text.contains("ci job failed"));
    }
}
