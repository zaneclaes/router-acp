//! Durable release checkpoints. The parent and repository host retain all
//! deployment authority; these records prevent a restart from losing the
//! frozen candidate, outstanding evidence, or disposition step.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::planner_workflow::{PlannerRun, Receipt, WorkStatus, finished_revision};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Stage {
    Selected,
    CandidatePassed,
    StagingDeployed,
    StagingPassed,
    ProductionMerged,
    ProductionDeployed,
    ProductionPassed,
    Disposed,
    Failed,
    Abandoned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub stage: Stage,
    pub revision: String,
    #[serde(default)]
    pub works: BTreeMap<String, String>,
    #[serde(default)]
    pub replaces: Option<String>,
    pub evidence: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Release {
    pub candidate: String,
    pub works: BTreeMap<String, String>,
    pub checkpoints: Vec<Checkpoint>,
    pub superseded_by: Option<String>,
}

impl Release {
    pub fn stage(&self) -> Stage {
        self.checkpoints.last().map_or(Stage::Selected, |c| c.stage)
    }

    pub fn active(&self) -> bool {
        self.superseded_by.is_none() && !matches!(self.stage(), Stage::Disposed | Stage::Abandoned)
    }
}

fn full_sha(sha: &str) -> bool {
    sha.len() == 40 && sha.bytes().all(|b| b.is_ascii_hexdigit())
}

fn evidence_key(stage: Stage) -> &'static str {
    match stage {
        Stage::Selected => "selection",
        Stage::CandidatePassed => "candidate-build",
        Stage::StagingDeployed => "staging-rollout",
        Stage::StagingPassed => "staging-smoke",
        Stage::ProductionMerged => "production-pr",
        Stage::ProductionDeployed => "production-rollout",
        Stage::ProductionPassed => "production-smoke",
        Stage::Disposed => "disposition-commit",
        Stage::Failed => "failure",
        Stage::Abandoned => "abandonment",
    }
}

pub fn record(run: &mut PlannerRun, id: &str, checkpoint: &Checkpoint) -> Result<(), String> {
    if id.trim().is_empty() || !full_sha(&checkpoint.revision) {
        return Err("release checkpoint requires a release id and full revision SHA".into());
    }
    if checkpoint
        .evidence
        .values()
        .any(|value| !crate::planner_workflow::valid_evidence(value))
        || !checkpoint
            .evidence
            .get(evidence_key(checkpoint.stage))
            .is_some_and(|value| crate::planner_workflow::valid_evidence(value))
    {
        return Err(format!(
            "release checkpoint requires {} evidence",
            evidence_key(checkpoint.stage)
        ));
    }
    if let Some(release) = run.releases.get(id) {
        if release.checkpoints.last() == Some(checkpoint) {
            return Ok(());
        }
        if release.superseded_by.is_some()
            || release.stage() == Stage::Disposed
            || release.stage() == Stage::Failed
            || release.stage() == Stage::Abandoned
        {
            return Err(
                "release is closed or failed; select a new bounded repair candidate".into(),
            );
        }
        if !checkpoint.works.is_empty() || checkpoint.replaces.is_some() {
            return Err(
                "release selection is frozen; works and replaces belong only to selection".into(),
            );
        }
        let next = match release.stage() {
            Stage::Selected => Stage::CandidatePassed,
            Stage::CandidatePassed => Stage::StagingDeployed,
            Stage::StagingDeployed => Stage::StagingPassed,
            Stage::StagingPassed => Stage::ProductionMerged,
            Stage::ProductionMerged => Stage::ProductionDeployed,
            Stage::ProductionDeployed => Stage::ProductionPassed,
            Stage::ProductionPassed => Stage::Disposed,
            _ => unreachable!(),
        };
        let abandoning =
            checkpoint.stage == Stage::Abandoned && release.stage() < Stage::ProductionMerged;
        if checkpoint.stage != next && checkpoint.stage != Stage::Failed && !abandoning {
            return Err(format!("release must record {next:?} before advancing"));
        }
        if checkpoint.stage != Stage::ProductionMerged
            && checkpoint.revision != release.checkpoints.last().unwrap().revision
        {
            return Err(
                "release checkpoint revision does not match the candidate or production merge"
                    .into(),
            );
        }
        if checkpoint.stage == Stage::ProductionMerged
            && run.releases.iter().any(|(other_id, other)| {
                other_id != id
                    && other.active()
                    && matches!(
                        other.stage(),
                        Stage::ProductionMerged | Stage::ProductionDeployed | Stage::Failed
                    )
            })
        {
            return Err(
                "the preceding production release still needs a passing smoke or repair".into(),
            );
        }
        if checkpoint.stage == Stage::Disposed
            && release.works.iter().any(|(work_id, revision)| {
                !run.works.get(work_id).is_some_and(|work| {
                    work.status == WorkStatus::Integrated
                        && work
                            .integration
                            .as_ref()
                            .is_some_and(|receipt| &receipt.revision == revision)
                })
            })
        {
            return Err(
                "release disposition requires every selected work's verified integration receipt"
                    .into(),
            );
        }
        run.releases
            .get_mut(id)
            .unwrap()
            .checkpoints
            .push(checkpoint.clone());
        return Ok(());
    }
    if checkpoint.stage != Stage::Selected || checkpoint.works.is_empty() {
        return Err("a release starts with a nonempty, frozen selection".into());
    }
    if run.releases.iter().any(|(other_id, release)| {
        release.active()
            && (release.stage() < Stage::ProductionMerged || release.stage() == Stage::Failed)
            && checkpoint.replaces.as_ref() != Some(other_id)
    }) {
        return Err("the staging lane already holds a candidate or unreconciled failure".into());
    }
    if let Some(previous) = &checkpoint.replaces {
        let release = run
            .releases
            .get(previous)
            .ok_or("replaced release does not exist")?;
        if release.stage() != Stage::Failed || release.superseded_by.is_some() {
            return Err("only an unreplaced failed release can be replaced".into());
        }
    }
    for (work_id, revision) in &checkpoint.works {
        if run.releases.iter().any(|(other_id, other)| {
            other.active()
                && checkpoint.replaces.as_ref() != Some(other_id)
                && other.works.get(work_id) == Some(revision)
        }) {
            return Err(format!(
                "work {work_id} is already selected in an active release"
            ));
        }
        let work = run
            .works
            .get(work_id)
            .ok_or("release selected an unknown work")?;
        for (dependency, source) in &work.spec.ready_dependencies {
            let integrated = run.works.get(dependency).is_some_and(|upstream| {
                upstream.status == WorkStatus::Integrated
                    && upstream
                        .integration
                        .as_ref()
                        .is_some_and(|receipt| &receipt.revision == source)
            });
            let preceding = run.releases.values().any(|other| {
                other.superseded_by.is_none()
                    && matches!(
                        other.stage(),
                        Stage::ProductionMerged
                            | Stage::ProductionDeployed
                            | Stage::ProductionPassed
                            | Stage::Disposed
                    )
                    && other.works.get(dependency) == Some(source)
            });
            if checkpoint.works.get(dependency) != Some(source) && !integrated && !preceding {
                return Err(format!(
                    "release must carry reviewed source dependency {dependency}@{source}"
                ));
            }
        }
        if !finished_revision(work, revision) {
            return Err(format!(
                "release work {work_id} must have an ended, accepted and finished exact revision"
            ));
        }
    }
    let waiting = waiting_heads(run, checkpoint);
    if checkpoint.works.len() < ROUND_LIMIT && !waiting.is_empty() {
        let oldest = waiting
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "release selection under-batches: {} accepted finished heads would wait another round \
             (oldest: {oldest}). A ready head waits at most one round, and only the first corrective \
             round after a failure may isolate its repair. Carry them oldest first, up to \
             {ROUND_LIMIT} works, or record `deferred:<work>` selection evidence naming the merge \
             conflict that sends one back to its worker",
            waiting.len()
        ));
    }
    if let Some(previous) = &checkpoint.replaces {
        run.releases.get_mut(previous).unwrap().superseded_by = Some(id.into());
    }
    run.releases.insert(
        id.into(),
        Release {
            candidate: checkpoint.revision.clone(),
            works: checkpoint.works.clone(),
            checkpoints: vec![checkpoint.clone()],
            superseded_by: None,
        },
    );
    Ok(())
}

/// A clean round carries every releasable head, up to this many works.
const ROUND_LIMIT: usize = 12;

/// Releasable heads a new selection leaves out, oldest first. Only the first
/// corrective round after a failure (it replaces a release that was not
/// itself a replacement) may isolate its repair; otherwise consecutive
/// failures would keep every finished head out of staging indefinitely.
fn waiting_heads(run: &PlannerRun, checkpoint: &Checkpoint) -> Vec<String> {
    let replaced = checkpoint
        .replaces
        .as_ref()
        .and_then(|id| run.releases.get_key_value(id));
    if replaced.is_some_and(|(_, release)| {
        release
            .checkpoints
            .first()
            .is_none_or(|first| first.replaces.is_none())
    }) {
        return Vec::new();
    }
    let ready = |id: &str, revision: &str| {
        run.works.get(id).is_some_and(|work| {
            work.status == WorkStatus::Integrated
                && work
                    .integration
                    .as_ref()
                    .is_some_and(|receipt| receipt.revision == revision)
        }) || checkpoint
            .works
            .get(id)
            .is_some_and(|selected| selected == revision)
    };
    let mut waiting = run
        .works
        .iter()
        .filter(|(id, work)| {
            let Some(revision) = work.finish.as_ref().map(|finish| &finish.revision) else {
                return false;
            };
            finished_revision(work, revision)
                && !checkpoint.works.contains_key(*id)
                && !checkpoint.evidence.contains_key(&format!("deferred:{id}"))
                && !run.releases.iter().any(|(other_id, other)| {
                    (other.active() || replaced.is_some_and(|(r, _)| r == other_id))
                        && other.works.contains_key(*id)
                })
                && work
                    .spec
                    .ready_dependencies
                    .iter()
                    .all(|(dependency, source)| ready(dependency, source))
        })
        .map(|(id, work)| (work.ready_at_ms.unwrap_or(0), id.clone()))
        .collect::<Vec<_>>();
    waiting.sort();
    waiting.into_iter().map(|(_, id)| id).collect()
}

pub fn integration_allowed(
    run: &PlannerRun,
    work_id: &str,
    receipt: &Receipt,
) -> Result<(), String> {
    let selected = run
        .releases
        .values()
        .any(|r| r.works.get(work_id) == Some(&receipt.revision));
    let Some(id) = receipt.evidence.get("release-round") else {
        return if selected {
            Err("selected work needs its recorded release-round".into())
        } else {
            Ok(())
        };
    };
    let Some(release) = run.releases.get(id) else {
        return if selected {
            Err("selected work cannot bypass its recorded release".into())
        } else {
            Ok(())
        };
    };
    if release.superseded_by.is_some()
        || !matches!(release.stage(), Stage::ProductionPassed | Stage::Disposed)
        || release.works.get(work_id) != Some(&receipt.revision)
    {
        return Err("integration requires the selected revision and a passing production release checkpoint".into());
    }
    Ok(())
}

pub fn status(run: &PlannerRun, work_id: Option<&str>) -> Value {
    json!(run.releases.iter().filter(|(_, release)| work_id.map_or_else(|| release.active(), |id| release.works.contains_key(id))).map(|(id, release)| json!({
        "release_id":id,"stage":release.stage(),"candidate":release.candidate,
        "works":release.works,"latest":release.checkpoints.last(),"superseded_by":release.superseded_by
    })).collect::<Vec<_>>())
}
