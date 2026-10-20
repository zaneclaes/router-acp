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
/// At most one steer per session per `STEER_INTERVAL_SECS`, and only the urgent
/// items: on 2026-10-18 the first version steered 47 routine `observation:ci:<work>`
/// wakes into one parent turn in 47 seconds, one per tick, and the parent read
/// the flood as operator speech.
const STEER_INTERVAL_SECS: u64 = 60;

fn claim_steer(rt: &mut crate::planner_guard::SessionRuntime, now: std::time::Instant) -> bool {
    if rt.steering
        || rt
            .last_steer
            .is_some_and(|last| now.saturating_duration_since(last).as_secs() < STEER_INTERVAL_SECS)
    {
        return false;
    }
    rt.steering = true;
    rt.last_steer = Some(now);
    true
}

fn steer_pending(shared: &Arc<Shared>, sid: &str, run: &planner_workflow::PlannerRun) {
    if !crate::planner_preempt::router_turn_in_flight(shared, sid) {
        return;
    }
    let steered = crate::planner_guard::with_runtime(shared, sid, |rt| rt.steered.clone());
    let Some((key, text)) = pending_steer(&run.parent_queue, &run.wakes, &steered, &run.works)
    else {
        return;
    };
    let ownership = run
        .wakes
        .get(&key)
        .is_some_and(|wake| ownership_handoff(&key, wake, &run.works));
    let claimed = crate::planner_guard::with_runtime(shared, sid, |rt| {
        claim_steer(rt, std::time::Instant::now())
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
        } else if (key.starts_with("input:") || ownership)
            && crate::planner_preempt::router_turn_in_flight(&shared, &sid)
            && planner_workflow::load(&shared, &sid)
                .ok()
                .flatten()
                .filter(|current| {
                    current.status == RunStatus::Running
                        && current.approval_waits.is_empty()
                        && current
                            .parent_queue
                            .first()
                            .is_none_or(|input| !planner_workflow::human_prompt(&input.request))
                })
                .and_then(|current| {
                    pending_steer(
                        &current.parent_queue,
                        &current.wakes,
                        &Default::default(),
                        &current.works,
                    )
                })
                .is_some_and(|(pending, _)| pending == key)
        {
            // Native adapters need not implement `_session/steering`. Use the
            // existing cancellation/queued-delivery lifecycle instead of
            // stranding a correction or ownership handoff behind a router-owned turn. Recheck
            // after the request: withdrawal, acknowledgment, a live delivery or
            // queued operator input may have superseded this correction.
            let ended = crate::planner_preempt::preempt(&shared, &sid).await;
            shared.state.lock().unwrap().log(
                &sid,
                &crate::state::LogEntry {
                    kind: if ownership {
                        "planner_handoff_preempt"
                    } else {
                        "planner_input_preempt"
                    }
                    .into(),
                    role: "router".into(),
                    summary: format!(
                        "Queued parent item {key}: steering declined; router turn ended={ended}"
                    ),
                    ..Default::default()
                },
            );
        }
    });
}

fn ownership_handoff(
    id: &str,
    wake: &planner_workflow::Wake,
    works: &std::collections::BTreeMap<String, planner_workflow::Work>,
) -> bool {
    works.get(&wake.work_id).is_some_and(|work| {
        work.status == planner_workflow::WorkStatus::Interrupted
            && work.attempt.as_ref().is_some_and(|attempt| {
                attempt.ended && id == format!("{}:{}", wake.work_id, attempt.id)
            })
            && work.handoff.as_ref().is_some_and(|handoff| {
                matches!(
                    handoff.kind.as_str(),
                    "ownership_requested" | "reassignment_required"
                )
            })
    })
}

/// The one pending item to steer now: the queued agent correction first, then
/// an urgent release/failed-job/ownership wake, then capacity, skipping anything already
/// steered this turn or claimed by a live delivery. Pure, for tests.
pub(crate) fn pending_steer(
    queue: &[planner_workflow::QueuedParentInput],
    wakes: &std::collections::BTreeMap<String, planner_workflow::Wake>,
    steered: &std::collections::HashSet<String>,
    works: &std::collections::BTreeMap<String, planner_workflow::Work>,
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
                    "[Router steer] This is router text, not operator input. Agent correction `{}` is queued for this parent. It is redelivered for \
                     acknowledgment when this turn ends; read it now and act on it in this turn if it changes \
                     what you are doing:\n{body}",
                    input.input_id
                ),
            ));
        }
    }
    // Capacity starvation, release transitions and failed CI jobs can affect
    // coordination now. Routine branch observations still wait for the boundary.
    let (id, wake) = wakes
        .iter()
        .filter(|(id, w)| {
            !w.acknowledged
                && (id.starts_with("release:")
                    || id.starts_with("observation:ci-job-failures")
                    || id.starts_with("capacity:")
                    || ownership_handoff(id, w, works))
                && !steered.contains(*id)
                && !claimed(w, id)
        })
        .min_by_key(|(id, wake)| {
            if ownership_handoff(id, wake, works) {
                1
            } else {
                wake_priority(id)
            }
        })?;
    Some((
        id.clone(),
        format!(
            "[Router steer] This is router text, not operator input. Pending wake `{id}` (work {}): {}. It is redelivered when this turn ends; a failed \
             CI job, release transition, ownership handoff or unfilled capacity is actionable now, in this turn.",
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

fn environment_wake(id: &str) -> bool {
    matches!(
        id,
        "observation:ci:staging"
            | "observation:ci:production"
            | "observation:ci-job-failures:staging"
            | "observation:ci-job-failures:production"
            | "observation:smoke:staging"
            | "observation:smoke:production"
    )
}

fn integration_pending(
    observations: &std::collections::BTreeMap<String, crate::planner_guard::Observation>,
    mut work_states: impl Iterator<Item = planner_workflow::WorkStatus>,
    ids: &[String],
) -> bool {
    work_states.any(|state| state == planner_workflow::WorkStatus::Finished)
        || ids.iter().any(|id| environment_wake(id))
        || ["staging", "production"].into_iter().any(|environment| {
            observations
                .get(&format!("ci:{environment}"))
                .is_some_and(|observation| {
                    observation.reference == environment
                        && observation.state == crate::planner_guard::ObservationState::Failed
                })
        })
}

fn wake_priority(id: &str) -> u8 {
    if id.starts_with("input:") {
        0
    } else if id.starts_with("release:") || environment_wake(id) {
        1
    } else if id.starts_with("observation:") {
        2
    } else if id.starts_with("recovery:") {
        3
    } else if id.starts_with("capacity:")
        || id.starts_with("inflight:")
        || id.starts_with("gate:")
        || id.starts_with("audit:")
    {
        5
    } else {
        4
    }
}

fn wake_role<'a>(
    phase: crate::config::PlannerPhase,
    explicit_command: bool,
    mut releases: impl Iterator<Item = &'a crate::planner_release::Release>,
    environment_changed: bool,
) -> PlannerRole {
    if phase == crate::config::PlannerPhase::Implementation
        && (environment_changed || releases.any(crate::planner_release::Release::active))
    {
        // Implementation commands resume the deployment obligation too. Choosing
        // SelectPlan unconditionally here or at command admission demotes an
        // adopted failed release to general refill for the entire first turn.
        PlannerRole::IntegratePlan
    } else if explicit_command {
        match phase {
            crate::config::PlannerPhase::Planning => PlannerRole::CreatePlan,
            crate::config::PlannerPhase::Implementation => PlannerRole::SelectPlan,
        }
    } else {
        PlannerRole::ReviewWork
    }
}

pub(crate) fn parent_role(
    run: &planner_workflow::PlannerRun,
    explicit_command: bool,
    ids: &[String],
) -> PlannerRole {
    wake_role(
        run.phase,
        explicit_command,
        run.releases.values(),
        integration_pending(
            &run.observations,
            run.works.values().map(|work| work.status),
            ids,
        ),
    )
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
    let role = parent_role(
        &run,
        input_ids.iter().any(|id| run.commands.contains(id)),
        &ids,
    );
    let task = if role == PlannerRole::IntegratePlan {
        "First advance the open release or reconcile its concrete repair/build prerequisite. Failed staging/production recovery takes priority over routine review and bulk refill; independent WIP workers continue. Then reconcile the supplied work identities as that prerequisite permits, acknowledging wakes only after their own reconciliation. Preserve human authorization and all release gates."
    } else {
        "Reconcile these exact work identities and their evidence. Acknowledge each wake through planner_workflow after reconciliation. Preserve human authorization. Continue independent authorized work, or report the concrete waiting reason."
    };
    let text = format!(
        "{}\n[Router-owned child wake]\n{}\n{}\n{task}",
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

    fn pending_steer(
        queue: &[planner_workflow::QueuedParentInput],
        wakes: &BTreeMap<String, planner_workflow::Wake>,
        steered: &HashSet<String>,
    ) -> Option<(String, String)> {
        super::pending_steer(queue, wakes, steered, &BTreeMap::new())
    }

    #[test]
    fn ownership_steering_requires_current_ended_attempt_and_pending_delivery() {
        let mut work: planner_workflow::Work = serde_json::from_value(json!({
            "spec": {"work_id":"repair", "plan_id":"repair", "scope":"repair"},
            "child_id":"child", "status":"interrupted",
            "attempt":{"id":"a", "router_pid":0, "router_started_at_ms":0, "ended":true},
            "handoff":{"kind":"ownership_requested", "message":"parent must grant source"}
        }))
        .unwrap();
        let wakes = BTreeMap::from([(
            "repair:a".into(),
            planner_workflow::Wake {
                work_id: "repair".into(),
                ..Default::default()
            },
        )]);
        let selected = |work: &planner_workflow::Work,
                        wakes: &BTreeMap<String, planner_workflow::Wake>| {
            super::pending_steer(
                &[],
                wakes,
                &HashSet::new(),
                &BTreeMap::from([("repair".into(), work.clone())]),
            )
        };
        assert!(selected(&work, &wakes).is_some());
        work.attempt.as_mut().unwrap().ended = false;
        assert!(selected(&work, &wakes).is_none());
        work.attempt.as_mut().unwrap().ended = true;
        work.attempt.as_mut().unwrap().id = "replacement".into();
        assert!(selected(&work, &wakes).is_none());
        work.attempt.as_mut().unwrap().id = "a".into();
        work.handoff.as_mut().unwrap().kind = "waiting".into();
        assert!(selected(&work, &wakes).is_none());
        work.handoff.as_mut().unwrap().kind = "reassignment_required".into();
        assert!(selected(&work, &wakes).is_some());
        let mut done = wakes.clone();
        done.get_mut("repair:a").unwrap().acknowledged = true;
        assert!(selected(&work, &done).is_none());
        let mut claimed = wakes.clone();
        let claim = claimed.get_mut("repair:a").unwrap();
        claim.claim_pid = std::process::id();
        claim.claim_started_at_ms = planner_workflow::process_identity().1;
        assert!(selected(&work, &claimed).is_none());
    }

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
    fn unfinished_releases_select_integration_without_changing_worker_lifecycle() {
        use crate::config::PlannerPhase;
        use crate::planner_release::{Checkpoint, Release, Stage};
        for stage in [
            Stage::Selected,
            Stage::StagingDeployed,
            Stage::ProductionMerged,
            Stage::ProductionDeployed,
            Stage::Failed,
            Stage::ProductionPassed,
        ] {
            let mut release = Release {
                candidate: "a".repeat(40),
                works: BTreeMap::new(),
                superseded_by: None,
                checkpoints: vec![Checkpoint {
                    stage,
                    revision: "a".repeat(40),
                    works: BTreeMap::new(),
                    replaces: None,
                    evidence: BTreeMap::new(),
                }],
            };
            assert_eq!(
                wake_role(
                    PlannerPhase::Implementation,
                    false,
                    [&release].into_iter(),
                    false
                ),
                PlannerRole::IntegratePlan,
                "release stage {stage:?} was sent to routine review"
            );
            assert_eq!(
                wake_role(
                    PlannerPhase::Implementation,
                    true,
                    [&release].into_iter(),
                    false
                ),
                PlannerRole::IntegratePlan
            );
            assert_eq!(
                wake_role(PlannerPhase::Planning, false, [&release].into_iter(), false),
                PlannerRole::ReviewWork
            );
            release.superseded_by = Some("replacement".into());
            assert_eq!(
                wake_role(
                    PlannerPhase::Implementation,
                    false,
                    [&release].into_iter(),
                    false
                ),
                PlannerRole::ReviewWork
            );
            release.superseded_by = None;
            release.checkpoints[0].stage = Stage::Disposed;
            assert_eq!(
                wake_role(
                    PlannerPhase::Implementation,
                    false,
                    [&release].into_iter(),
                    false
                ),
                PlannerRole::ReviewWork
            );
            release.checkpoints[0].stage = Stage::Abandoned;
            assert_eq!(
                wake_role(
                    PlannerPhase::Implementation,
                    false,
                    [&release].into_iter(),
                    false
                ),
                PlannerRole::ReviewWork
            );
        }
    }

    #[test]
    fn acknowledged_environment_failure_keeps_repair_priority_on_later_wakes() {
        use crate::config::PlannerPhase;
        use crate::planner_guard::{Observation, ObservationState};
        // The failed deploy predates any release record. Its notification was
        // acknowledged; a stopped repair now asks the parent for ownership while
        // unrelated branch results keep arriving. Acknowledgment is not recovery.
        let ids = vec![
            "done:repair-needs-ownership".into(),
            "observation:ci:other-worker".into(),
        ];
        for environment in ["staging", "production"] {
            let mut observations = BTreeMap::from([(
                format!("ci:{environment}"),
                Observation {
                    revision: "a".repeat(40),
                    reference: environment.into(),
                    state: ObservationState::Failed,
                    evidence: "exact required build failed; repair handoff pending".into(),
                },
            )]);
            let role = |observations: &BTreeMap<String, Observation>| {
                wake_role(
                    PlannerPhase::Implementation,
                    false,
                    std::iter::empty(),
                    integration_pending(observations, std::iter::empty(), &ids),
                )
            };
            assert_eq!(
                role(&observations),
                PlannerRole::IntegratePlan,
                "acknowledged {environment} failure was demoted to unrelated packet review"
            );
            for state in [
                ObservationState::Passed,
                ObservationState::Pending,
                ObservationState::Unknown,
            ] {
                observations
                    .get_mut(&format!("ci:{environment}"))
                    .unwrap()
                    .state = state;
                assert_eq!(role(&observations), PlannerRole::ReviewWork);
            }
            observations
                .get_mut(&format!("ci:{environment}"))
                .unwrap()
                .state = ObservationState::Failed;
            assert_eq!(
                wake_role(
                    PlannerPhase::Implementation,
                    true,
                    std::iter::empty(),
                    integration_pending(&observations, std::iter::empty(), &ids)
                ),
                PlannerRole::IntegratePlan
            );
            assert_eq!(
                wake_role(
                    PlannerPhase::Planning,
                    false,
                    std::iter::empty(),
                    integration_pending(&observations, std::iter::empty(), &ids)
                ),
                PlannerRole::ReviewWork
            );
            observations
                .get_mut(&format!("ci:{environment}"))
                .unwrap()
                .reference = "wip/other-worker".into();
            assert_eq!(role(&observations), PlannerRole::ReviewWork);
        }
        for key in ["ci:other-worker", "ci-job-failures:staging"] {
            let observations = BTreeMap::from([(
                key.into(),
                Observation {
                    revision: "a".repeat(40),
                    reference: "staging".into(),
                    state: ObservationState::Failed,
                    evidence: "advisory or unrelated failure".into(),
                },
            )]);
            assert!(!integration_pending(
                &observations,
                std::iter::empty(),
                &ids
            ));
        }
    }

    #[test]
    fn finished_source_advances_the_pipeline_before_unrelated_reviews() {
        use crate::config::PlannerPhase;
        use planner_workflow::WorkStatus;
        let observations = BTreeMap::new();
        let ids = vec!["observation:ci:unrelated-worker".into()];
        let role = |state| {
            wake_role(
                PlannerPhase::Implementation,
                false,
                std::iter::empty(),
                integration_pending(&observations, [state].into_iter(), &ids),
            )
        };
        assert_eq!(
            role(WorkStatus::Finished),
            PlannerRole::IntegratePlan,
            "ready source was left behind routine review because no release had been selected yet"
        );
        for state in [
            WorkStatus::Running,
            WorkStatus::ReviewPending,
            WorkStatus::Accepted,
            WorkStatus::Interrupted,
            WorkStatus::Integrated,
            WorkStatus::Watching,
        ] {
            assert_eq!(role(state), PlannerRole::ReviewWork);
        }
    }

    #[test]
    fn environment_recovery_precedes_unrelated_branch_observations_and_refill() {
        use crate::config::PlannerPhase;
        // An environment can fail before its parent has recorded a release checkpoint.
        for id in [
            "observation:ci-job-failures:staging",
            "observation:ci-job-failures:production",
        ] {
            assert_eq!(
                wake_role(
                    PlannerPhase::Implementation,
                    false,
                    std::iter::empty(),
                    environment_wake(id)
                ),
                PlannerRole::IntegratePlan
            );
        }
        assert_eq!(
            wake_role(
                PlannerPhase::Implementation,
                false,
                std::iter::empty(),
                environment_wake("observation:ci:ordinary-worker")
            ),
            PlannerRole::ReviewWork
        );
        let mut ids = [
            "capacity:1",
            "observation:ci:a-worker",
            "done:worker",
            "observation:ci-job-failures:staging",
            "release:corrective",
            "input:operator",
        ];
        ids.sort_by_key(|id| wake_priority(id));
        assert_eq!(
            &ids[..3],
            &[
                "input:operator",
                "observation:ci-job-failures:staging",
                "release:corrective"
            ]
        );
    }

    #[test]
    fn agent_correction_steers_first_once_and_never_an_operator_prompt() {
        let mut wakes = BTreeMap::new();
        wakes.insert(
            "observation:ci-job-failures:staging".to_string(),
            wake("ci job failed"),
        );
        let queue = vec![input("fix-now", true)];
        let (key, text) =
            pending_steer(&queue, &wakes, &HashSet::new()).expect("steers the correction");
        assert_eq!(key, "input:fix-now");
        assert!(text.contains("body of fix-now") && text.contains("redelivered"));
        assert!(text.starts_with("[Router steer] This is router text, not operator input."));
        let steered = HashSet::from([key]);
        let (next, _) = pending_steer(&queue, &wakes, &steered).expect("then the observation");
        assert_eq!(next, "observation:ci-job-failures:staging");
        let operator = vec![input("typed", false)];
        let (key, _) =
            pending_steer(&operator, &wakes, &HashSet::new()).expect("skips the operator prompt");
        assert_eq!(
            key, "observation:ci-job-failures:staging",
            "an operator prompt is never steered by the router loop"
        );
    }

    #[test]
    fn routine_wakes_wait_and_acknowledged_or_claimed_urgent_ones_do_not_steer() {
        let mut wakes = BTreeMap::new();
        wakes.insert("audit:2".to_string(), wake("defect"));
        wakes.insert(
            "observation:ci:dashboard-r1".to_string(),
            wake("branch build passed"),
        );
        assert!(
            pending_steer(&[], &wakes, &HashSet::new()).is_none(),
            "routine wakes, including a branch build observation, wait for the boundary"
        );
        let mut done = wake("round passed");
        done.acknowledged = true;
        wakes.insert("release:r".to_string(), done);
        assert!(pending_steer(&[], &wakes, &HashSet::new()).is_none());
        let mut live = wake("ci job failed");
        live.claim_pid = std::process::id();
        live.claim_started_at_ms = planner_workflow::process_identity().1;
        wakes.insert("observation:ci-job-failures:claimed".to_string(), live);
        assert!(
            pending_steer(&[], &wakes, &HashSet::new()).is_none(),
            "a wake a live delivery claims is its"
        );
        wakes.insert(
            "observation:ci-job-failures:open".to_string(),
            wake("ci job failed"),
        );
        let (id, text) = pending_steer(&[], &wakes, &HashSet::new()).expect("the open observation");
        assert_eq!(id, "observation:ci-job-failures:open");
        assert!(text.contains("ci job failed"));
        assert!(text.starts_with("[Router steer] This is router text, not operator input."));
    }

    #[test]
    fn capacity_wake_reaches_busy_parent_once_without_steering_routine_observations() {
        let mut wakes = BTreeMap::from([
            (
                "capacity:1".to_string(),
                wake("admissible work with free slots"),
            ),
            (
                "observation:ci:work".to_string(),
                wake("branch build passed"),
            ),
        ]);
        let (id, text) =
            pending_steer(&[], &wakes, &HashSet::new()).expect("capacity must reach busy parent");
        assert_eq!(id, "capacity:1");
        assert!(text.starts_with("[Router steer] This is router text, not operator input."));
        assert!(pending_steer(&[], &wakes, &HashSet::from([id])).is_none());
        wakes.insert("release:ready".to_string(), wake("release transition"));
        assert_eq!(
            pending_steer(&[], &wakes, &HashSet::new()).unwrap().0,
            "release:ready"
        );
        wakes.remove("release:ready");
        wakes.get_mut("capacity:1").unwrap().acknowledged = true;
        assert!(pending_steer(&[], &wakes, &HashSet::new()).is_none());
    }

    #[test]
    fn second_steer_waits_sixty_seconds_even_across_turns() {
        let mut rt = crate::planner_guard::SessionRuntime::default();
        let now = std::time::Instant::now();
        assert!(claim_steer(&mut rt, now));
        rt.steering = false;
        rt.steered.clear();
        assert!(!claim_steer(
            &mut rt,
            now + std::time::Duration::from_secs(59)
        ));
        assert!(claim_steer(
            &mut rt,
            now + std::time::Duration::from_secs(60)
        ));
        assert!(!claim_steer(
            &mut rt,
            now + std::time::Duration::from_secs(120)
        ));
        let mut other_session = crate::planner_guard::SessionRuntime::default();
        assert!(claim_steer(&mut other_session, now));
    }
}
