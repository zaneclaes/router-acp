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
//!
//! # Ownership is a lease, not a process
//!
//! A run's `owner` is the router process that has its parent session loaded.
//! That process existing proves host liveness, not coordinator activity: the
//! same router serves many sessions and keeps running after the operator
//! closes one, and the stamp used to outlive the session (`lifecycle::close`
//! never cleared it), so a replacement `/implement` could wait forever on a
//! host that drove nothing (live, 2026-10-26: `rtr-…-56d975bf` sat with zero
//! works on "its owning router (pid 90176) is alive" while its release audits
//! kept demanding admissions the pending adoption refused). So ownership is a
//! lease the wake loop renews ([`heartbeat`], every [`OWNER_HEARTBEAT_SECS`])
//! and `session/close` or `session/delete` hands back at once ([`release`]).
//! [`live_owner`] treats an owner as living only while its process exists
//! AND its lease is fresh (or the session itself was active within
//! [`OWNER_STALE_SECS`]); a lease a pre-heartbeat router stamped (`0`) is
//! trusted while that process exists, because unknown activity never
//! authorizes a takeover. Live worker attempts and wake claims fence the run
//! on their own, whatever the parent's lease says.

use std::collections::BTreeSet;

use agent_client_protocol::schema::v1::PromptRequest;
use serde::Serialize;

use crate::planner_workflow::{PlannerRun, RunStatus, Wake};

/// With no recorded owner (a run saved before owners were recorded), no
/// live attempt or wake claim and no session activity for this long, the
/// run's router is gone.
const LEGACY_QUIET_SECS: i64 = 600;

/// The owning router renews its lease on a run this often (the wake loop
/// calls [`heartbeat`] on its one-second tick; it writes only when due).
pub const OWNER_HEARTBEAT_SECS: i64 = 30;

/// An owner whose process exists but whose lease is this old, with no session
/// activity for as long, no longer drives the run: its parent session was
/// unloaded (or its wake loop is gone). A surviving host is not a living
/// coordinator, and `/implement` elsewhere may adopt the run.
pub const OWNER_STALE_SECS: i64 = 180;

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
    fn session_exists(&self, sid: &str) -> bool;
}

impl RunStore for crate::session::Shared {
    fn session_exists(&self, sid: &str) -> bool {
        self.state.lock().unwrap().get(sid).is_some()
    }
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
    fn session_exists(&self, sid: &str) -> bool {
        self.get(sid).is_some()
    }
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

/// This router has `sid` loaded: take or renew the run's ownership lease.
/// `run` is the document the caller already loaded; nothing is written while
/// the lease is this router's and younger than [`OWNER_HEARTBEAT_SECS`]. A run
/// another session adopted meanwhile is left alone.
pub fn heartbeat(
    store: &dyn RunStore,
    sid: &str,
    run: &PlannerRun,
    now: i64,
) -> Result<(), String> {
    let me = crate::planner_workflow::RunOwner::current();
    if run.status == RunStatus::Superseded
        || (run.owner == Some(me)
            && run.owner_heartbeat_at != 0
            && now - run.owner_heartbeat_at < OWNER_HEARTBEAT_SECS)
    {
        return Ok(());
    }
    for _ in 0..16 {
        let Some(mut run) = store.load(sid)? else {
            return Ok(());
        };
        if run.status == RunStatus::Superseded {
            return Ok(());
        }
        run.owner = Some(me);
        run.owner_heartbeat_at = now;
        match store.save(sid, &mut run) {
            Ok(()) => return Ok(()),
            Err(e) if concurrent(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    Err("planner ownership heartbeat remained contended".into())
}

/// This router no longer has `sid` loaded (`session/close`, `session/delete`):
/// hand the run's ownership back so a replacement `/implement` in the same
/// checkout can adopt it now rather than after this process exits. Only this
/// router's own lease is released; live attempts and wake claims keep
/// fencing the run on their own.
pub fn release(store: &dyn RunStore, sid: &str) -> Result<(), String> {
    let me = crate::planner_workflow::RunOwner::current();
    for _ in 0..16 {
        let Some(mut run) = store.load(sid)? else {
            return Ok(());
        };
        if run.owner != Some(me) {
            return Ok(());
        }
        run.owner = None;
        run.owner_heartbeat_at = 0;
        match store.save(sid, &mut run) {
            Ok(()) => return Ok(()),
            Err(e) if concurrent(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    Err("planner ownership release remained contended".into())
}

/// What a pending ownership recovery means for the parent, for audits, the
/// turn-end gate and refused operations: the source run, why the handoff is
/// waiting, and what the run refuses meanwhile. `None` when nothing is pending.
pub fn pending_notice(run: &PlannerRun) -> Option<String> {
    let source = run.capacity.pending_adoption.as_ref()?;
    let waiting = run
        .wakes
        .get("adoption:handoff")
        .map(|wake| format!(" Current state: {}", wake.reason))
        .unwrap_or_default();
    Some(format!(
        "Ownership recovery is pending: this run is adopting planner run {source}, whose          assignments are not transferred yet.{waiting} Until the handoff completes the router          refuses new admissions and dispatch of work identities that still belong to that run;          the capacity cycle retries the handoff every 15 seconds. This is router state, not a          defect you can act on: record the dispositions, holds and release evidence you can,          and do not re-admit or duplicate that run's work."
    ))
}

/// The `Admit` refusal while adoption of `source` is pending.
pub fn admission_refusal(run: &PlannerRun, source: &str) -> String {
    let mut text = format!(
        "planner adoption of {source} is pending; the capacity cycle retries the handoff before new admissions"
    );
    if let Some(wake) = run.wakes.get("adoption:handoff") {
        text.push_str(&format!(" ({})", wake.reason));
    }
    text
}

/// The dispatch refusal for a work identity this run does not hold.
pub fn dispatch_refusal(run: &PlannerRun, work_id: &str) -> String {
    match &run.capacity.pending_adoption {
        None => "admit this work identity before dispatch".into(),
        Some(source) => {
            let mut text = format!(
                "work {work_id} is not in this run: planner adoption of {source} is pending and                  its assignments are not transferred yet; dispatch it after the handoff                  completes and do not re-admit it here"
            );
            if let Some(wake) = run.wakes.get("adoption:handoff") {
                text.push_str(&format!(" ({})", wake.reason));
            }
            text
        }
    }
}

/// Why `sid`'s run still has a live owner, if it does.
///
/// A living owner is a router that has the parent loaded *and* drives it: its
/// process exists and its lease is fresh, or the session itself was active
/// within [`OWNER_STALE_SECS`]. A process that merely exists is a surviving
/// host; after its lease and the session go quiet it blocks nothing. A lease a
/// pre-heartbeat router stamped (`owner_heartbeat_at == 0`) stays live while
/// that process exists, because unknown activity never authorizes a takeover.
pub fn live_owner(store: &dyn RunStore, sid: &str, run: &PlannerRun, now: i64) -> Option<String> {
    if store.loaded_here(sid) {
        return Some("its session is loaded in this router".into());
    }
    let last_activity = store.last_activity(sid);
    if let Some(owner) = &run.owner
        && owner.alive()
    {
        if run.owner_heartbeat_at == 0 {
            return Some(format!(
                "its owning router (pid {}) is alive and predates ownership heartbeats; adoption waits for that process to exit or close the session",
                owner.pid
            ));
        }
        let lease_age = now - run.owner_heartbeat_at;
        if lease_age < OWNER_STALE_SECS {
            return Some(format!(
                "its owning router (pid {}) confirmed it drives this run {lease_age} s ago",
                owner.pid
            ));
        }
        if let Some(last) = last_activity
            && now - last < OWNER_STALE_SECS
        {
            return Some(format!(
                "its owning router (pid {}) last confirmed this run {lease_age} s ago but the session was active {} s ago",
                owner.pid,
                now - last
            ));
        }
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
    if run.owner.is_none()
        && let Some(last) = last_activity
        && now - last < LEGACY_QUIET_SECS
    {
        return Some(format!(
            "it records no owner and its last activity was at unix second {last}; waiting for the legacy quiet period"
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
    // The adopting session continues the earlier parent's role.
    into.coordinator |= from.parent_role();
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
        if !into.inputs.contains_key(id)
            && let Some(receipt) = from.receipts.get(&format!("input-withdrawal:{id}"))
        {
            into.receipts
                .insert(format!("input-withdrawal:{id}"), receipt.clone());
        }
        into.inputs.entry(id.clone()).or_insert_with(|| {
            let mut adopted = input.clone();
            // Never turn a genuinely undelivered queued request into an
            // acknowledgment-only replay. Already delivered queue entries
            // (e.g. a crash before cleanup) become historical wakes below.
            if !input.resolved()
                && input.origin == "human"
                && !from.commands.contains(id)
                && (!from.parent_queue.iter().any(|q| &q.input_id == id)
                    || input.delivered_to.contains(from_sid))
            {
                adopted.replayed_from.get_or_insert_with(|| from_sid.into());
            }
            adopted
        });
    }
    // Operator input the old parent received but never acknowledged: an
    // `input:` wake, which leads the next wake batch.
    let queued = from
        .parent_queue
        .iter()
        .map(|q| q.input_id.clone())
        .collect::<BTreeSet<_>>();
    for (id, input) in &from.inputs {
        if input.resolved()
            || input.origin != "human"
            || from.commands.contains(id)
            || (queued.contains(id) && !input.delivered_to.contains(from_sid))
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
                    "HISTORICAL input adopted from {from_sid}: reconcile the earlier parent's \
                     missing acknowledgment in this turn. Do not answer or act on the old message again; \
                     it is not new operator input."
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
            .inputs
            .get(&queued.input_id)
            .is_some_and(|input| input.resolved() || input.replayed_from.is_some())
        {
            continue;
        }
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
        if id
            .strip_prefix("input:")
            .and_then(|id| into.inputs.get(id))
            .is_some_and(|input| input.resolved())
        {
            continue;
        }
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
///
/// Three revision-checked phases, so an owner that comes back mid-flight can
/// never end up coordinating beside the adopter:
///
/// 1. **claim** — re-check liveness on the exact revision being written and
///    mark `from` `superseded` by `into_sid` while it still holds everything.
///    From this save on the old router is fenced: its heartbeat skips the run,
///    `planner_workflow::operate` refuses mutations and its wake loop says so.
///    A lease renewed between the check and the save conflicts the save, and
///    the retry sees the live owner and refuses.
/// 2. **merge** — move the claimed content into `into` (idempotent: content
///    the destination already holds is reported, not duplicated).
/// 3. **retire** — strip the moved content from `from`.
///
/// A crash between phases leaves `from` claimed with its content intact;
/// rerunning (the capacity cycle's `retry_pending`, or `adopt-run` again)
/// resumes from the claim instead of declaring an empty adoption complete.
pub fn adopt(store: &dyn RunStore, from_sid: &str, into_sid: &str) -> Result<AdoptReport, String> {
    adopt_at(store, from_sid, into_sid, chrono::Utc::now().timestamp())
}

/// [`adopt`] judging liveness at unix second `now` (the capacity cycle passes
/// its tick time so one clock governs the whole retry).
pub fn adopt_at(
    store: &dyn RunStore,
    from_sid: &str,
    into_sid: &str,
    now: i64,
) -> Result<AdoptReport, String> {
    if from_sid == into_sid {
        return Err("a run cannot adopt itself".into());
    }
    let mut claimed = None;
    for _ in 0..16 {
        let mut from = store
            .load(from_sid)?
            .ok_or_else(|| format!("no planner run for session {from_sid}"))?;
        let into = store.load(into_sid)?.ok_or_else(|| {
            format!("no planner run for session {into_sid}; run /implement there first")
        })?;
        if into.status == RunStatus::Superseded {
            return Err(format!("planner run {into_sid} is itself superseded"));
        }
        if from.status == RunStatus::Superseded && from.superseded_by.as_deref() == Some(into_sid) {
            // Claimed by this destination earlier (a router died between the
            // claim and the retire): finish that adoption.
            claimed = Some(from);
            break;
        }
        adoptable(store, from_sid, &from, now)?;
        // Prove the merge before anything is written: a release conflict must
        // surface while `from` is still its owner's to drive.
        merge(&mut into.clone(), &from, from_sid, into_sid)?;
        from.status = RunStatus::Superseded;
        from.superseded_by = Some(into_sid.to_string());
        match store.save(from_sid, &mut from) {
            Ok(()) => {
                claimed = Some(from);
                break;
            }
            Err(e) if concurrent(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    let from = claimed.ok_or("the earlier run stayed contended; retry adopt-run")?;
    let mut report = None;
    for _ in 0..16 {
        let mut into = store
            .load(into_sid)?
            .ok_or_else(|| format!("planner run {into_sid} disappeared"))?;
        if into.status == RunStatus::Superseded {
            return Err(format!(
                "planner run {into_sid} became superseded while adopting {from_sid}; rerun adopt-run from {from_sid} into its successor"
            ));
        }
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
    let report = report.ok_or("the new run stayed contended; rerun adopt-run to finish")?;
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
        "adopted into {into_sid}, but retiring {from_sid} stayed contended; rerun adopt-run to finish"
    ))
}

/// Content a claimed source still holds for its adopter: the claim happened,
/// the merge and retire did not (yet).
fn holds_claimed_content(from: &PlannerRun) -> bool {
    !from.works.is_empty()
        || !from.releases.is_empty()
        || !from.capacity.holds.is_empty()
        || !from.parent_queue.is_empty()
}

/// Whether `run` is an implement run rather than the empty planning run every
/// coordinator session gets. A plain chat in the same checkout must never
/// stand in for the run it would otherwise shadow (live: a title-bar chat's
/// empty `Planning` run was newer than a paused run with 266 works, so
/// `/implement` adopted the chat and orphaned the real run).
fn is_implement_run(run: &PlannerRun) -> bool {
    run.execution_request.is_some() || !run.works.is_empty() || !run.releases.is_empty()
}

/// `/implement` in `into_sid`: resume the checkout's latest earlier implement
/// run when it is running, draining or cleanly paused with no live owner. Older
/// implement runs are what that latest one replaced and are left alone.
pub fn adopt_on_implement(store: &dyn RunStore, into_sid: &str) -> Option<AdoptReport> {
    let mut ids: Vec<String> = store
        .run_ids_beside(into_sid)
        .ok()?
        .into_iter()
        .filter(|id| id != into_sid && !id.contains("::"))
        .collect();
    ids.sort_by_key(|id| std::cmp::Reverse(store.updated_at(id).unwrap_or(0)));
    let (latest, run) = ids.into_iter().find_map(|id| {
        let run = store.load(&id).ok().flatten()?;
        is_implement_run(&run).then_some((id, run))
    })?;
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
        "Awaiting adoption of planner run {latest}; the capacity cycle retries every 15 seconds. Only new admissions wait for the preserved inventory."
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
        if run.capacity.pending_adoption.as_deref() == source
            && run
                .wakes
                .get("adoption:handoff")
                .is_some_and(|w| w.reason == reason)
        {
            return Ok(());
        }
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
    retry_pending_at(store, into_sid, chrono::Utc::now().timestamp())
}

pub fn retry_pending_at(
    store: &dyn RunStore,
    into_sid: &str,
    now: i64,
) -> Result<Option<AdoptReport>, String> {
    // Claim the retry before reading the potentially large source document.
    let mut source = None;
    for _ in 0..16 {
        let mut into = store
            .load(into_sid)?
            .ok_or("planner destination disappeared")?;
        if into.capacity.pending_adoption.is_none()
            || (into.capacity.last_adoption_retry_at != 0
                && now - into.capacity.last_adoption_retry_at < 15)
        {
            return Ok(None);
        }
        into.capacity.last_adoption_retry_at = now;
        match store.save(into_sid, &mut into) {
            Ok(()) => {
                source = into.capacity.pending_adoption;
                break;
            }
            Err(e) if concurrent(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    let source = source.ok_or("planner adoption retry remained contended")?;
    match retry_source(store, into_sid, &source, now) {
        Ok(report) => Ok(report),
        Err(error) => {
            adoption_pending(
                store,
                into_sid,
                Some(&source),
                &format!(
                    "Adoption of {source} is waiting: {error}. Retrying in 15 seconds; worker recovery and audits continue."
                ),
            )?;
            Err(error)
        }
    }
}

fn retry_source(
    store: &dyn RunStore,
    into_sid: &str,
    source: &str,
    now: i64,
) -> Result<Option<AdoptReport>, String> {
    let into = store
        .load(into_sid)?
        .ok_or("planner destination disappeared")?;
    if into.capacity.pending_adoption.as_deref() != Some(source) {
        return Ok(None);
    }
    let from = if store.session_exists(source) {
        store.load(source)?
    } else {
        None
    };
    let Some(from) = from else {
        adoption_pending(
            store,
            into_sid,
            None,
            &format!(
                "Adoption of {source} abandoned: its run or session row disappeared. New admissions may continue."
            ),
        )?;
        return Ok(None);
    };
    if from.superseded_by.as_deref() == Some(into_sid) {
        if from.status == RunStatus::Superseded && holds_claimed_content(&from) {
            // Claimed but never merged: the adopter died between phases. Finish.
            return adopt_at(store, source, into_sid, now).map(Some);
        }
        adoption_pending(
            store,
            into_sid,
            None,
            "Planner adoption completed; reconcile the preserved assignments before resuming them.",
        )?;
        return Ok(Some(AdoptReport {
            from: source.into(),
            into: into_sid.into(),
            ..Default::default()
        }));
    }
    let terminal = from
        .superseded_by
        .as_ref()
        .map(|sid| format!("superseded by session {sid}"))
        .or_else(|| {
            (!matches!(
                from.status,
                RunStatus::Running | RunStatus::Draining | RunStatus::Paused
            ))
            .then(|| format!("source is {:?}", from.status))
        });
    if let Some(reason) = terminal {
        adoption_pending(
            store,
            into_sid,
            None,
            &format!("Adoption of {source} abandoned: {reason}. New admissions may continue."),
        )?;
        return Ok(None);
    }
    if let Some(reason) = live_owner(store, source, &from, now) {
        adoption_pending(
            store,
            into_sid,
            Some(source),
            &format!(
                "Adoption of {source} is waiting: {reason}. Retrying in 15 seconds; worker recovery and audits continue."
            ),
        )?;
        return Ok(None);
    }
    adopt_at(store, source, into_sid, now).map(Some)
}
