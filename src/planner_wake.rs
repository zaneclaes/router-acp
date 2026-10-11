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

    fn idle(shared: &Arc<Shared>, sid: &str, queued_input: bool) -> Option<Self> {
        shared
            .with_session(sid, |s| {
                if s.prompt_activity != 0
                    || s.pinning
                    || (s.cancelled && !queued_input)
                    || s.pin.is_none()
                    || s.capturing_summary.is_some()
                {
                    return false;
                }
                s.prompt_activity += 1;
                s.planner_wake_active = true;
                if queued_input {
                    s.cancelled = false;
                }
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
                let Some(activity) = PromptActivity::idle(&shared, &sid, queued_input) else {
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
        "{}\n[Router-owned child wake]\n{}\nReconcile these exact work identities and their evidence. Acknowledge each wake through planner_workflow after reconciliation. Preserve human authorization. Continue independent authorized work, or report the concrete waiting reason.",
        run.policy.instructions(role),
        serde_json::to_string(&wakes).map_err(|e| e.to_string())?
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
