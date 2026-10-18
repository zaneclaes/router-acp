//! Resume a planner run whose router died.
//!
//! A planner run lives with its parent session. When the operator restarts
//! kory, the new session's `/implement` used to start an empty run while the
//! old one, its works, holds, queued operator input and pending wakes, sat
//! with no router to drive it (live: `rtr-…-4b0fc8eb` kept 42 works and 4
//! unacknowledged operator inputs after its router exited, and the new
//! `rtr-…-7e0b07cc` started from nothing). Now:
//!
//! - `/implement` adopts the latest earlier run of the same checkout when it
//!   is `running`, `draining` or cleanly `paused` and has no live owner
//!   (`adopt_on_implement`); ordinary session loading never resumes it;
//! - `router-acp adopt-run --from <old> --into <new>` does the same for two
//!   named sessions, against the state database, while the new router runs.
//!
//! Adoption moves works (an id the new run already has stays with the old run
//! and is reported), holds, unacknowledged wakes and queued operator input
//! into the new run, puts that input at the front of the new parent's queue,
//! turns operator input the old parent received but never acknowledged into
//! `input:` wakes (delivered ahead of other wakes), and marks the old run
//! `superseded` with `superseded_by`. Every save is revision-checked, so a
//! router writing the same run concurrently only makes the step retry.

use std::collections::BTreeSet;

use agent_client_protocol::schema::v1::PromptRequest;
use serde::Serialize;

use crate::planner_workflow::{PlannerRun, RunStatus, Wake};

/// With no recorded owner (a run saved before owners were recorded), no
/// live attempt or wake claim and no session activity for this long, the
/// run's router is gone.
const LEGACY_QUIET_SECS: i64 = 600;

/// Where planner runs are read and written: the router (`Shared`) or the
/// state database alone (the `adopt-run` command).
pub trait RunStore {
    fn load(&self, sid: &str) -> Result<Option<PlannerRun>, String>;
    fn save(&self, sid: &str, run: &mut PlannerRun) -> Result<(), String>;
    /// Unix seconds of the session's latest log row or tool update.
    fn last_activity(&self, sid: &str) -> Option<i64>;
    fn updated_at(&self, sid: &str) -> Option<u64>;
    fn run_ids_beside(&self, sid: &str) -> Result<Vec<String>, String>;
    /// The session is loaded in this process (so its owner is alive).
    fn loaded_here(&self, sid: &str) -> bool;
}

impl RunStore for crate::session::Shared {
    fn load(&self, sid: &str) -> Result<Option<PlannerRun>, String> {
        crate::planner_workflow::load(self, sid)
    }
    fn save(&self, sid: &str, run: &mut PlannerRun) -> Result<(), String> {
        crate::planner_workflow::save(self, sid, run)
    }
    fn last_activity(&self, sid: &str) -> Option<i64> {
        self.state
            .lock()
            .unwrap()
            .last_activity(sid)
            .map(|(at, _)| at)
    }
    fn updated_at(&self, sid: &str) -> Option<u64> {
        self.state
            .lock()
            .unwrap()
            .get(sid)
            .and_then(|s| s.updated_at)
    }
    fn run_ids_beside(&self, sid: &str) -> Result<Vec<String>, String> {
        self.state.lock().unwrap().planner_run_ids_beside(sid)
    }
    fn loaded_here(&self, sid: &str) -> bool {
        self.sessions.lock().unwrap().contains_key(sid)
    }
}

impl RunStore for crate::state::StateStore {
    fn load(&self, sid: &str) -> Result<Option<PlannerRun>, String> {
        self.planner_run(sid)?
            .map(|(revision, value)| {
                let mut run: PlannerRun =
                    serde_json::from_value(value).map_err(|e| e.to_string())?;
                run.revision = revision;
                Ok(run)
            })
            .transpose()
    }
    fn save(&self, sid: &str, run: &mut PlannerRun) -> Result<(), String> {
        let document = serde_json::to_value(&*run).map_err(|e| e.to_string())?;
        run.revision = self.save_planner_run_with_wakes(sid, run.revision, &document, &[])?;
        Ok(())
    }
    fn last_activity(&self, sid: &str) -> Option<i64> {
        crate::state::StateStore::last_activity(self, sid).map(|(at, _)| at)
    }
    fn updated_at(&self, sid: &str) -> Option<u64> {
        self.get(sid).and_then(|s| s.updated_at)
    }
    fn run_ids_beside(&self, sid: &str) -> Result<Vec<String>, String> {
        self.planner_run_ids_beside(sid)
    }
    fn loaded_here(&self, _sid: &str) -> bool {
        false
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct AdoptReport {
    pub from: String,
    pub into: String,
    /// Works moved into the new run, with their status.
    pub works: Vec<(String, crate::planner_workflow::WorkStatus)>,
    /// Works the new run already had under the same id: left with the old run.
    pub conflicting_works: Vec<String>,
    pub holds: Vec<String>,
    pub releases: Vec<String>,
    pub wakes: Vec<String>,
    /// Operator inputs surfaced to the new parent first, in delivery order.
    pub operator_inputs: Vec<String>,
}

impl AdoptReport {
    pub fn summary(&self) -> String {
        let mut by_status = std::collections::BTreeMap::<String, usize>::new();
        for (_, status) in &self.works {
            *by_status
                .entry(
                    serde_json::to_value(status)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_string))
                        .unwrap_or_default(),
                )
                .or_default() += 1;
        }
        let statuses = by_status
            .iter()
            .map(|(status, n)| format!("{n} {status}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut line = format!(
            "resumed planner run {}: adopted {} works ({}), {} holds, {} wakes and {} unacknowledged operator inputs",
            self.from,
            self.works.len(),
            if statuses.is_empty() {
                "none"
            } else {
                &statuses
            },
            self.holds.len(),
            self.wakes.len(),
            self.operator_inputs.len()
        );
        if !self.conflicting_works.is_empty() {
            line.push_str(&format!(
                "; kept with the old run because this run already has them: {}",
                self.conflicting_works.join(", ")
            ));
        }
        line
    }
}

fn concurrent(error: &str) -> bool {
    error.starts_with("planner state changed concurrently")
}

/// Why `sid`'s run still has a live owner, if it does.
pub fn live_owner(store: &dyn RunStore, sid: &str, run: &PlannerRun, now: i64) -> Option<String> {
    if store.loaded_here(sid) {
        return Some("its session is loaded in this router".into());
    }
    if let Some(owner) = &run.owner {
        return owner
            .alive()
            .then(|| format!("its owning router (pid {}) is alive", owner.pid));
    }
    if let Some((id, attempt)) = run.works.iter().find_map(|(id, w)| {
        w.attempt
            .as_ref()
            .filter(|a| !a.ended && crate::planner_workflow::owner_alive(a))
            .map(|a| (id, a))
    }) {
        return Some(format!(
            "work {id} has a live attempt owner (pid {})",
            attempt.router_pid
        ));
    }
    if let Some((id, wake)) = run.wakes.iter().find(|(id, w)| {
        w.claim_pid != 0
            && crate::planner_workflow::owner_alive(&crate::planner_workflow::Attempt {
                id: (*id).clone(),
                router_pid: w.claim_pid,
                router_started_at_ms: w.claim_started_at_ms,
                delegate_id: None,
                ended: false,
            })
    }) {
        return Some(format!(
            "wake {id} is claimed by a live router (pid {})",
            wake.claim_pid
        ));
    }
    if let Some(last) = store.last_activity(sid)
        && now - last < LEGACY_QUIET_SECS
    {
        return Some(format!(
            "it records no owner and was active {} s ago",
            now - last
        ));
    }
    None
}

fn adoptable(store: &dyn RunStore, sid: &str, run: &PlannerRun, now: i64) -> Result<(), String> {
    if !matches!(
        run.status,
        RunStatus::Running | RunStatus::Draining | RunStatus::Paused
    ) {
        return Err(format!(
            "planner run {sid} is {:?}, not running, draining or paused",
            run.status
        ));
    }
    if run.status == RunStatus::Paused
        && run
            .works
            .values()
            .any(|work| work.attempt.as_ref().is_some_and(|attempt| !attempt.ended))
    {
        return Err(format!(
            "paused planner run {sid} still has unfinished attempts; finish its drain first"
        ));
    }
    if let Some(reason) = live_owner(store, sid, run, now) {
        return Err(format!(
            "planner run {sid} still has a live owner: {reason}"
        ));
    }
    Ok(())
}

/// Move `from`'s adoptable state into `into` (in memory).
fn merge(
    into: &mut PlannerRun,
    from: &PlannerRun,
    from_sid: &str,
    into_sid: &str,
) -> Result<AdoptReport, String> {
    let mut report = AdoptReport {
        from: from_sid.into(),
        into: into_sid.into(),
        ..Default::default()
    };
    for policy in from
        .prior_policies
        .iter()
        .chain(std::iter::once(&from.policy))
    {
        if policy.identity != into.policy.identity
            && !into
                .prior_policies
                .iter()
                .any(|p| p.identity == policy.identity)
        {
            into.prior_policies.push(policy.clone());
        }
    }
    if into.completed_guides_at_start.is_none() {
        into.completed_guides_at_start = from.completed_guides_at_start.clone();
    }
    for (id, work) in &from.works {
        if let Some(existing) = into.works.get(id) {
            if existing.child_id == work.child_id
                && (existing.spec == work.spec
                    || existing.reconciliation.prior_specs.contains(&work.spec))
            {
                // The destination save may have survived a crash before the
                // source was retired. Never overwrite its subsequent progress.
                report.works.push((id.clone(), existing.status));
            } else {
                report.conflicting_works.push(id.clone());
            }
        } else {
            let mut adopted = work.clone();
            crate::planner_workflow::mark_reconciliation_required(&mut adopted);
            into.works.insert(id.clone(), adopted);
            if let Some(recovery) = from.capacity.resumes.get(id) {
                into.capacity.resumes.insert(id.clone(), recovery.clone());
            }
            report.works.push((id.clone(), work.status));
        }
    }
    for (id, release) in &from.releases {
        if release
            .works
            .keys()
            .any(|work| report.conflicting_works.contains(work))
        {
            return Err(format!(
                "release {id} includes a conflicting work identity; reconcile it before adoption"
            ));
        }
        if let Some(existing) = into.releases.get(id) {
            if existing.candidate != release.candidate
                || existing.works != release.works
                || !existing.checkpoints.starts_with(&release.checkpoints)
                || release
                    .superseded_by
                    .as_ref()
                    .is_some_and(|replacement| existing.superseded_by.as_ref() != Some(replacement))
            {
                return Err(format!(
                    "release {id} already names different evidence; reconcile it before adoption"
                ));
            }
        } else {
            into.releases.insert(id.clone(), release.clone());
        }
        report.releases.push(id.clone());
    }
    for (plan, hold) in &from.capacity.holds {
        if !into.capacity.holds.contains_key(plan) {
            into.capacity.holds.insert(plan.clone(), hold.clone());
            report.holds.push(plan.clone());
        }
    }
    for (id, observation) in &from.observations {
        into.observations
            .entry(id.clone())
            .or_insert_with(|| observation.clone());
    }
    for (id, input) in &from.inputs {
        into.inputs
            .entry(id.clone())
            .or_insert_with(|| input.clone());
    }
    // Operator input the old parent received but never acknowledged: an
    // `input:` wake, which leads the next wake batch.
    let queued = from
        .parent_queue
        .iter()
        .map(|q| q.input_id.clone())
        .collect::<BTreeSet<_>>();
    for (id, input) in &from.inputs {
        if input.acknowledged
            || input.origin != "human"
            || from.commands.contains(id)
            || queued.contains(id)
        {
            continue;
        }
        let wake_id = format!("input:{id}");
        if into.wakes.get(&wake_id).is_some_and(|w| !w.acknowledged) {
            continue;
        }
        into.wakes.insert(
            wake_id.clone(),
            Wake {
                reason: format!(
                    "Operator input adopted from the superseded planner run {from_sid}: the \
                     earlier parent received it but never acknowledged it. Handle it if it still \
                     applies, then acknowledge it."
                ),
                work_id: into_sid.into(),
                ..Default::default()
            },
        );
        report.wakes.push(wake_id);
        report.operator_inputs.push(id.clone());
    }
    // Queued operator input runs first, in its original order, ahead of the
    // new run's own queue.
    let mut front = Vec::new();
    for queued in &from.parent_queue {
        if into
            .parent_queue
            .iter()
            .any(|q| q.input_id == queued.input_id)
        {
            continue;
        }
        let mut entry = queued.clone();
        entry.request = PromptRequest::new(into_sid.to_string(), queued.request.prompt.clone())
            .meta(queued.request.meta.clone());
        into.wakes
            .entry(format!("input:{}", queued.input_id))
            .or_insert(Wake {
                reason: format!(
                    "Original parent input awaits its own turn (adopted from {from_sid})"
                ),
                work_id: into_sid.into(),
                ..Default::default()
            });
        report.operator_inputs.push(queued.input_id.clone());
        front.push(entry);
    }
    front.extend(std::mem::take(&mut into.parent_queue));
    into.parent_queue = front;
    for (id, wake) in &from.wakes {
        // A wake about a work the new run kept its own of describes the old
        // attempt, not the new one.
        let stale_work = report.conflicting_works.contains(&wake.work_id)
            || id
                .split_once(':')
                .is_some_and(|(work, _)| report.conflicting_works.iter().any(|c| c == work));
        if wake.acknowledged || into.wakes.contains_key(id) || stale_work {
            continue;
        }
        into.wakes.insert(
            id.clone(),
            Wake {
                reason: wake.reason.clone(),
                work_id: if wake.work_id == from_sid {
                    into_sid.into()
                } else {
                    wake.work_id.clone()
                },
                acknowledged: false,
                ..Default::default()
            },
        );
        report.wakes.push(id.clone());
    }
    into.idle_heartbeats = 0;
    Ok(report)
}

/// What stays with the superseded run: everything not moved.
fn retire(from: &mut PlannerRun, report: &AdoptReport) {
    for (id, _) in &report.works {
        from.works.remove(id);
        from.capacity.resumes.remove(id);
    }
    for id in &report.releases {
        from.releases.remove(id);
    }
    for plan in &report.holds {
        from.capacity.holds.remove(plan);
    }
    for id in &report.wakes {
        if let Some(wake) = from.wakes.get_mut(id) {
            wake.acknowledged = true;
        }
    }
    from.parent_queue
        .retain(|q| !report.operator_inputs.contains(&q.input_id));
    for id in &report.operator_inputs {
        if let Some(wake) = from.wakes.get_mut(&format!("input:{id}")) {
            wake.acknowledged = true;
        }
    }
    from.status = RunStatus::Superseded;
    from.superseded_by = Some(report.into.clone());
}

/// Adopt `from_sid`'s run into `into_sid`'s. Both runs must exist; `from`
/// must be running, draining or cleanly paused with no live owner.
pub fn adopt(store: &dyn RunStore, from_sid: &str, into_sid: &str) -> Result<AdoptReport, String> {
    if from_sid == into_sid {
        return Err("a run cannot adopt itself".into());
    }
    let mut report = None;
    for _ in 0..16 {
        let from = store
            .load(from_sid)?
            .ok_or_else(|| format!("no planner run for session {from_sid}"))?;
        let mut into = store.load(into_sid)?.ok_or_else(|| {
            format!("no planner run for session {into_sid}; run /implement there first")
        })?;
        if into.status == RunStatus::Superseded {
            return Err(format!("planner run {into_sid} is itself superseded"));
        }
        adoptable(store, from_sid, &from, chrono::Utc::now().timestamp())?;
        let merged = merge(&mut into, &from, from_sid, into_sid)?;
        match store.save(into_sid, &mut into) {
            Ok(()) => {
                report = Some(merged);
                break;
            }
            Err(e) if concurrent(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    let report = report.ok_or("the new run stayed contended; retry adopt-run")?;
    for _ in 0..16 {
        let mut from = store
            .load(from_sid)?
            .ok_or_else(|| format!("planner run {from_sid} disappeared"))?;
        retire(&mut from, &report);
        match store.save(from_sid, &mut from) {
            Ok(()) => {
                adoption_pending(store, into_sid, None, &report.summary())?;
                return Ok(report);
            }
            Err(e) if concurrent(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(format!(
        "adopted into {into_sid}, but marking {from_sid} superseded stayed contended; rerun adopt-run to finish"
    ))
}

/// `/implement` in `into_sid`: resume the checkout's latest earlier planner
/// run when it is running, draining or cleanly paused with no live owner. Older runs are what
/// that latest one replaced and are left alone.
pub fn adopt_on_implement(store: &dyn RunStore, into_sid: &str) -> Option<AdoptReport> {
    let ids = store.run_ids_beside(into_sid).ok()?;
    let latest = ids
        .into_iter()
        .filter(|id| id != into_sid && !id.contains("::"))
        .max_by_key(|id| store.updated_at(id).unwrap_or(0))?;
    let run = store.load(&latest).ok().flatten()?;
    if run.superseded_by.is_some()
        || !matches!(
            run.status,
            RunStatus::Running | RunStatus::Draining | RunStatus::Paused
        )
    {
        return None;
    }
    // The old router may still be finishing shutdown when its replacement
    // receives implement. Persist this handoff instead of silently starting
    // from zero works and forgetting the previous run at the next tick.
    let reason = format!(
        "Awaiting adoption of planner run {latest}; the ordinary capacity cycle retries after its owner exits. New admissions and release mutations wait for the preserved inventory."
    );
    if let Err(error) = adoption_pending(store, into_sid, Some(&latest), &reason) {
        tracing::warn!(from = latest, into = into_sid, %error, "could not record planner adoption");
        return None;
    }
    match retry_pending(store, into_sid) {
        Ok(report) => report,
        Err(error) => {
            tracing::warn!(from = latest, into = into_sid, %error, "could not resume the earlier planner run");
            None
        }
    }
}

fn adoption_pending(
    store: &dyn RunStore,
    sid: &str,
    source: Option<&str>,
    reason: &str,
) -> Result<(), String> {
    for _ in 0..16 {
        let mut run = store.load(sid)?.ok_or("planner destination disappeared")?;
        run.capacity.pending_adoption = source.map(str::to_string);
        run.wakes.insert(
            "adoption:handoff".into(),
            Wake {
                work_id: sid.into(),
                reason: reason.into(),
                ..Default::default()
            },
        );
        match store.save(sid, &mut run) {
            Ok(()) => return Ok(()),
            Err(e) if concurrent(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    Err("planner adoption remained contended".into())
}

/// Retry only the handoff authorized by the explicit implement command.
/// Ordinary session loading never discovers or adopts a different run.
pub fn retry_pending(store: &dyn RunStore, into_sid: &str) -> Result<Option<AdoptReport>, String> {
    let into = store
        .load(into_sid)?
        .ok_or("planner destination disappeared")?;
    let Some(source) = into.capacity.pending_adoption else {
        return Ok(None);
    };
    let from = store
        .load(&source)?
        .ok_or("pending planner source disappeared")?;
    if from.superseded_by.as_deref() == Some(into_sid) {
        adoption_pending(
            store,
            into_sid,
            None,
            "Planner adoption completed; reconcile the preserved assignments before resuming them.",
        )?;
        return Ok(Some(AdoptReport {
            from: source,
            into: into_sid.into(),
            ..Default::default()
        }));
    }
    if live_owner(store, &source, &from, chrono::Utc::now().timestamp()).is_some() {
        return Ok(None);
    }
    adopt(store, &source, into_sid).map(Some)
}
