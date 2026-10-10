use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use agent_client_protocol::schema::v1::{ContentBlock, PromptRequest, ResourceLink};
use router_acp::config::{Config, DelegateLifecycleHook, PlannerWorkspaceConfig};
use router_acp::planner_workflow::{
    self, Artifact, Operation, Receipt, RunStatus, WorkSpec, WorkStatus,
};
use router_acp::session::{RouterSession, Shared};
use router_acp::state::{PersistedSession, Retention, StateStore};
use serde_json::json;
use tempfile::TempDir;

const SID: &str = "planner-session";

struct Fixture {
    _tmp: TempDir,
    repo: PathBuf,
    state_path: PathBuf,
    shared: Arc<Shared>,
}

fn git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("git {:?}: {error}", args));
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn repository(tmp: &TempDir) -> PathBuf {
    let repo = tmp.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    git(
        &repo,
        &["config", "user.email", "planner-test@example.invalid"],
    );
    git(&repo, &["config", "user.name", "Planner Test"]);
    std::fs::write(repo.join("README.md"), "initial\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "initial"]);

    let origin = tmp.path().join("origin.git");
    std::fs::create_dir(&origin).unwrap();
    git(&origin, &["init", "--bare", "--quiet"]);
    git(
        &repo,
        &["remote", "add", "origin", origin.to_str().unwrap()],
    );
    repo
}

fn base_config(state_path: &Path) -> Config {
    let state = serde_json::to_string(&state_path).unwrap();
    Config::from_yaml(&format!(
        "router: planner\nstate_file: {state}\nagents:\n  - name: test\n    command:\n      type: stdio\n      command: /bin/true\n    model_selection:\n      type: config-option\n    models:\n      - id: model\n        cost_rank: 1\n"
    ))
    .unwrap()
}

fn shared_for(repo: &Path, state_path: &Path, configure: impl FnOnce(&mut Config)) -> Arc<Shared> {
    let mut cfg = base_config(state_path);
    configure(&mut cfg);
    let shared = Shared::new(cfg.clone()).unwrap();
    let persisted = PersistedSession {
        agent: "test".into(),
        model: "model".into(),
        downstream_session_id: "downstream".into(),
        cwd: repo.to_path_buf(),
        kind: "primary".into(),
        ..PersistedSession::default()
    };
    shared
        .state
        .lock()
        .unwrap()
        .upsert(SID.to_string(), persisted.clone());
    shared.sessions.lock().unwrap().insert(
        SID.to_string(),
        RouterSession::rehydrated(&cfg, &persisted, Vec::new()),
    );
    shared
}

fn fixture(configure: impl FnOnce(&mut Config)) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let repo = repository(&tmp);
    let state_path = tmp.path().join("state.db");
    let shared = shared_for(&repo, &state_path, configure);
    Fixture {
        _tmp: tmp,
        repo,
        state_path,
        shared,
    }
}

#[test]
fn parent_credentials_are_replaced_and_revoked_on_close() {
    let f = fixture(|_| {});
    f.shared
        .delegate_socket
        .set(f.repo.join("delegate.sock"))
        .unwrap();
    let candidate = router_acp::candidate::CandidateId::parse("test/model").unwrap();
    router_acp::delegate_mcp::delegate_server_entry(&f.shared, SID, &candidate).unwrap();
    let first = f
        .shared
        .with_session(SID, |s| s.delegate_token.clone())
        .flatten()
        .unwrap();
    router_acp::delegate_mcp::delegate_server_entry(&f.shared, SID, &candidate).unwrap();
    let second = f
        .shared
        .with_session(SID, |s| s.delegate_token.clone())
        .flatten()
        .unwrap();
    assert_ne!(first, second);
    assert!(
        !f.shared
            .delegate_tokens
            .lock()
            .unwrap()
            .contains_key(&first)
    );
    assert_eq!(f.shared.delegate_tokens.lock().unwrap().len(), 1);
    router_acp::session::close_live_delegates_for(&f.shared, SID);
    assert!(f.shared.delegate_tokens.lock().unwrap().is_empty());
}

fn authorize(shared: &Arc<Shared>) {
    let mut run = planner_workflow::ensure(shared, SID).unwrap();
    run.execution_request = Some("implement the admitted plan".into());
    run.phase = router_acp::config::PlannerPhase::Implementation;
    planner_workflow::save(shared, SID, &mut run).unwrap();
}

fn admit(shared: &Arc<Shared>, work: WorkSpec) {
    authorize(shared);
    futures::executor::block_on(planner_workflow::operate(
        shared,
        SID,
        None,
        Operation::Admit {
            key: format!("admit-{}", work.work_id),
            work,
        },
    ))
    .unwrap();
}

fn work(work_id: &str) -> WorkSpec {
    WorkSpec {
        work_id: work_id.into(),
        plan_id: "plan-1".into(),
        scope: "change README.md and run the checks".into(),
        dependencies: Vec::new(),
        required_checks: Vec::new(),
        required_integration_evidence: Vec::new(),
        external_id: Some("external-1".into()),
    }
}

fn artifact(revision: &str) -> Artifact {
    Artifact {
        revision: revision.into(),
        changed_paths: vec!["README.md".into()],
        checks: BTreeMap::new(),
        evidence: vec!["check-log.txt".into()],
        unresolved: Vec::new(),
    }
}

fn receipt(revision: &str, evidence_key: &str) -> Receipt {
    Receipt {
        revision: revision.into(),
        evidence: BTreeMap::from([(evidence_key.into(), "verified-log.txt".into())]),
    }
}

fn current_revision(path: &Path) -> String {
    git(path, &["rev-parse", "HEAD"])
}

fn input_request(input_id: Option<&str>, text: &str, attachment: bool) -> PromptRequest {
    let mut content = vec![ContentBlock::from(text.to_string())];
    if attachment {
        content.push(ContentBlock::ResourceLink(ResourceLink::new(
            "attachment.txt",
            "file:///tmp/attachment.txt",
        )));
    }
    let mut request = PromptRequest::new("downstream", content);
    if let Some(input_id) = input_id {
        request.meta = serde_json::from_value(json!({
            "router_acp": {"input_id": input_id}
        }))
        .ok();
    }
    request
}

#[test]
fn policy_snapshot_survives_policy_source_and_config_change() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = repository(&tmp);
    std::fs::write(repo.join("policy.md"), "policy version one\n").unwrap();
    let state_path = tmp.path().join("state.db");
    let first = shared_for(&repo, &state_path, |cfg| {
        cfg.routers.planner.profile = Some("policy.md".into());
    });
    let saved = planner_workflow::ensure(&first, SID).unwrap();
    assert_eq!(saved.policy.policy.text, "policy version one\n");
    drop(first);

    std::fs::write(repo.join("policy.md"), "policy version two\n").unwrap();
    std::fs::write(repo.join("new-policy.md"), "new configured policy\n").unwrap();
    let second = shared_for(&repo, &state_path, |cfg| {
        cfg.routers.planner.profile = Some("new-policy.md".into());
    });
    let resumed = planner_workflow::load(&second, SID).unwrap().unwrap();
    assert_eq!(resumed.policy.policy.text, "policy version one\n");
    assert_eq!(resumed.policy.identity, saved.policy.identity);
}

#[tokio::test]
async fn return_to_planning_preserves_active_work_but_stops_new_dispatch() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (actor, _, child, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    let mut planning = input_request(Some("planning-input"), "/plan refine scope", false);
    planner_workflow::user_command(&fixture.shared, SID, &mut planning).unwrap();
    let run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    assert_eq!(run.works["work-1"].child_id, child);
    assert!(!run.works["work-1"].attempt.as_ref().unwrap().ended);
    assert!(
        planner_workflow::begin_work(&fixture.shared, SID, "work-1")
            .await
            .unwrap_err()
            .contains("not authorized")
    );
    planner_workflow::attempt_ended(
        &fixture.shared,
        SID,
        &actor,
        None,
        "paused for refinement".into(),
        true,
    )
    .unwrap();
    let mut implementation = input_request(
        Some("implementation-input"),
        "/implement resume scope",
        false,
    );
    planner_workflow::user_command(&fixture.shared, SID, &mut implementation).unwrap();
    let (replacement, _, same_child, _) =
        planner_workflow::begin_work(&fixture.shared, SID, "work-1")
            .await
            .unwrap();
    assert_eq!(same_child, child);
    assert_ne!(replacement.attempt_id, actor.attempt_id);
}

#[test]
fn stale_cas_update_is_rejected_across_two_state_files() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("state.db");
    let first = StateStore::load(&path, Retention::default());
    let second = StateStore::load(&path, Retention::default());
    let initial = json!({"status": "running"});
    assert_eq!(first.save_planner_run(SID, 0, &initial).unwrap(), 1);
    let (_, stale) = second.planner_run(SID).unwrap().unwrap();
    assert_eq!(
        first.save_planner_run(SID, 1, &json!({"status": "updated"})),
        Ok(2)
    );
    let error = second.save_planner_run(SID, 1, &stale).unwrap_err();
    assert!(error.contains("changed concurrently"), "{error}");
}

#[test]
fn planner_delete_fences_revision_and_releases_only_owned_workspace_claims() {
    let fixture = fixture(|_| {});
    let owned_workspace = fixture._tmp.path().join("owned-workspace");
    let other_workspace = fixture._tmp.path().join("other-workspace");
    let repository_artifact = fixture.repo.join("planner-artifact.txt");
    std::fs::write(&repository_artifact, "keep this repository artifact\n").unwrap();

    let state = fixture.shared.state.lock().unwrap();
    state.upsert(
        "other-session".into(),
        PersistedSession {
            agent: "test".into(),
            model: "model".into(),
            downstream_session_id: "other-downstream".into(),
            cwd: fixture.repo.clone(),
            kind: "primary".into(),
            ..PersistedSession::default()
        },
    );
    assert_eq!(
        state.save_planner_run(SID, 0, &json!({"status": "running"})),
        Ok(1)
    );
    state
        .claim_planner_workspace(&owned_workspace, SID, "work-1", "owned-lease")
        .unwrap();
    state
        .claim_planner_workspace(&other_workspace, "other-session", "work-2", "other-lease")
        .unwrap();
    assert_eq!(
        state.save_planner_run(SID, 1, &json!({"status": "complete"})),
        Ok(2)
    );

    let error = state.remove_planner_session(SID, 1).unwrap_err();
    assert!(error.contains("changed concurrently"), "{error}");
    assert_eq!(
        state.planner_run(SID).unwrap(),
        Some((2, json!({"status": "complete"})))
    );
    assert!(state.get(SID).is_some());
    assert!(
        state
            .claim_planner_workspace(&owned_workspace, "other-session", "work-3", "other-lease")
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(&repository_artifact).unwrap(),
        "keep this repository artifact\n"
    );

    state.remove_planner_session(SID, 2).unwrap();
    assert_eq!(state.planner_run(SID).unwrap(), None);
    assert!(state.get(SID).is_none());
    state
        .claim_planner_workspace(&owned_workspace, "other-session", "work-3", "new-lease")
        .unwrap();
    assert!(
        state
            .claim_planner_workspace(&other_workspace, "third-session", "work-4", "third-lease")
            .is_err()
    );
    assert!(state.get("other-session").is_some());
    assert_eq!(
        std::fs::read_to_string(&repository_artifact).unwrap(),
        "keep this repository artifact\n"
    );
}

#[test]
fn native_rehydration_keeps_planner_routing_without_claiming_old_effort() {
    let fixture = fixture(|_| {});
    fixture
        .shared
        .state
        .lock()
        .unwrap()
        .patch_session_routing(
            SID,
            &json!({
                "planner_phase": "implementation",
                "coordinator": true,
                "effort": {
                    "requested": "high",
                    "explicit": true,
                    "confirmed": true
                }
            }),
        )
        .unwrap();

    let persisted = fixture.shared.state.lock().unwrap().get(SID).unwrap();
    let restored = RouterSession::rehydrated(&fixture.shared.cfg, &persisted, Vec::new());
    assert_eq!(
        restored.planner_phase,
        Some(router_acp::config::PlannerPhase::Implementation)
    );
    assert!(restored.coordinator);
    assert_eq!(
        restored.effort_request,
        Some(router_acp::candidate::EffortLevel::High)
    );
    assert!(
        restored.resolved_effort.is_none(),
        "a fresh native adapter must confirm effort again"
    );
}

#[tokio::test]
async fn close_keeps_terminal_planner_runs_and_durable_assignment() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (_, workspace, child_id, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    fixture.shared.with_session(SID, |session| {
        session.delegates.push(router_acp::session::DelegateHandle {
            process_key: router_acp::downstream::ProcessKey("test".into()),
            downstream_sid: "running-child".into(),
        });
    });

    for terminal in [RunStatus::Complete, RunStatus::Cancelled] {
        planner_workflow::mutate(&fixture.shared, SID, |run| {
            run.status = terminal;
            Ok(())
        })
        .unwrap();
        router_acp::session::close_live_delegates_for(&fixture.shared, SID);

        let run = planner_workflow::load(&fixture.shared, SID)
            .unwrap()
            .unwrap();
        assert_eq!(run.status, terminal);
        assert_eq!(run.works["work-1"].child_id, child_id);
        assert_eq!(
            run.works["work-1"].workspace.as_ref().unwrap().path,
            workspace.path
        );
    }
    assert!(
        fixture
            .shared
            .with_session(SID, |session| session.cancelled)
            .unwrap()
    );
    assert!(
        fixture
            .shared
            .with_session(SID, |session| session.delegates.is_empty())
            .unwrap()
    );
}

#[test]
fn duplicate_admission_is_idempotent_but_conflicting_identity_fails() {
    let fixture = fixture(|_| {});
    let mut spec = work("work-1");
    admit(&fixture.shared, spec.clone());
    let first = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    let child_id = first.works["work-1"].child_id.clone();

    let replay = futures::executor::block_on(planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Admit {
            key: "admit-replay".into(),
            work: spec.clone(),
        },
    ))
    .unwrap();
    assert!(!replay.contains("replayed"));
    let same = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    assert_eq!(same.works.len(), 1);
    assert_eq!(same.works["work-1"].child_id, child_id);

    spec.scope = "a different scope".into();
    let error = futures::executor::block_on(planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Admit {
            key: "admit-conflict".into(),
            work: spec,
        },
    ))
    .unwrap_err();
    assert!(error.contains("different assignment"), "{error}");
}

#[tokio::test]
async fn worker_cannot_review_integrate_or_retire_parent_state() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (identity, _, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    let review = Operation::Review {
        key: "worker-review".into(),
        work_id: "work-1".into(),
        revision: current_revision(&fixture.repo),
        accepted: true,
        evidence: vec!["review.txt".into()],
        corrections: None,
    };
    let error = planner_workflow::operate(&fixture.shared, SID, Some(&identity), review)
        .await
        .unwrap_err();
    assert!(error.contains("cannot coordinate"), "{error}");

    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Integrate {
            key: "worker-integrate".into(),
            work_id: "work-1".into(),
            receipt: receipt(&current_revision(&fixture.repo), "merge"),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("cannot coordinate"), "{error}");

    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Disposition {
            key: "worker-retire".into(),
            work_id: "work-1".into(),
            status: WorkStatus::Blocked,
            reason: "waiting for parent decision".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("cannot coordinate"), "{error}");
}

#[tokio::test]
async fn stale_attempt_is_fenced_and_child_workspace_stay_stable() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (old, first_workspace, first_child, _) =
        planner_workflow::begin_work(&fixture.shared, SID, "work-1")
            .await
            .unwrap();
    planner_workflow::attempt_ended(
        &fixture.shared,
        SID,
        &old,
        None,
        "worker stopped".into(),
        true,
    )
    .unwrap();
    let (current, second_workspace, second_child, _) =
        planner_workflow::begin_work(&fixture.shared, SID, "work-1")
            .await
            .unwrap();
    assert_ne!(old.attempt_id, current.attempt_id);
    assert_eq!(first_workspace.path, second_workspace.path);
    assert_eq!(first_child, second_child);

    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&old),
        Operation::Artifact {
            key: "stale-artifact".into(),
            work_id: "work-1".into(),
            attempt_id: old.attempt_id.clone(),
            artifact: artifact(&current_revision(&second_workspace.path)),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("stale or ended"), "{error}");
}

#[tokio::test]
async fn corrections_reuse_child_and_workspace_before_new_attempt() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (identity, workspace, child, _) =
        planner_workflow::begin_work(&fixture.shared, SID, "work-1")
            .await
            .unwrap();
    let revision = current_revision(&workspace.path);
    planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Artifact {
            key: "artifact".into(),
            work_id: "work-1".into(),
            attempt_id: identity.attempt_id.clone(),
            artifact: artifact(&revision),
        },
    )
    .await
    .unwrap();
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Review {
            key: "corrections".into(),
            work_id: "work-1".into(),
            revision: revision.clone(),
            accepted: false,
            evidence: vec!["review.txt".into()],
            corrections: Some("add a regression test".into()),
        },
    )
    .await
    .unwrap();
    planner_workflow::attempt_ended(
        &fixture.shared,
        SID,
        &identity,
        None,
        "returned for corrections".into(),
        false,
    )
    .unwrap();
    let followup = planner_workflow::begin_followup(&fixture.shared, SID, &identity).unwrap();
    assert!(followup.contains(&identity.attempt_id));
    let after_followup = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    assert_eq!(after_followup.works["work-1"].child_id, child);
    assert_eq!(
        after_followup.works["work-1"]
            .workspace
            .as_ref()
            .unwrap()
            .path,
        workspace.path
    );

    planner_workflow::attempt_ended(
        &fixture.shared,
        SID,
        &identity,
        None,
        "stopped again".into(),
        true,
    )
    .unwrap();
    let (new_attempt, new_workspace, new_child, _) =
        planner_workflow::begin_work(&fixture.shared, SID, "work-1")
            .await
            .unwrap();
    assert_ne!(new_attempt.attempt_id, identity.attempt_id);
    assert_eq!(new_workspace.path, workspace.path);
    assert_eq!(new_child, child);
}

#[tokio::test]
async fn acknowledged_wake_is_rearmed_for_same_attempt_followup() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (identity, _, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    planner_workflow::attempt_ended(
        &fixture.shared,
        SID,
        &identity,
        None,
        "returned for corrections".into(),
        false,
    )
    .unwrap();
    let wake_id = format!("{}:{}", identity.work_id, identity.attempt_id);
    planner_workflow::mutate(&fixture.shared, SID, |run| {
        let wake = run.wakes.get_mut(&wake_id).unwrap();
        wake.acknowledged = true;
        wake.deliveries = 3;
        wake.retry_after = i64::MAX;
        wake.claim_pid = 42;
        wake.claim_started_at_ms = 7;
        Ok(())
    })
    .unwrap();

    planner_workflow::begin_followup(&fixture.shared, SID, &identity).unwrap();
    planner_workflow::attempt_ended(
        &fixture.shared,
        SID,
        &identity,
        None,
        "stopped again".into(),
        true,
    )
    .unwrap();

    let wake = &planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap()
        .wakes[&wake_id];
    assert!(!wake.acknowledged);
    assert_eq!(wake.deliveries, 0);
    assert_eq!(wake.retry_after, 0);
    assert_eq!(wake.claim_pid, 0);
    assert_eq!(wake.claim_started_at_ms, 0);
}

#[tokio::test]
async fn dead_and_reused_process_identity_allows_attempt_replacement() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (identity, _, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();

    planner_workflow::mutate(&fixture.shared, SID, |run| {
        let attempt = run
            .works
            .get_mut("work-1")
            .unwrap()
            .attempt
            .as_mut()
            .unwrap();
        attempt.router_pid = std::process::id();
        attempt.router_started_at_ms = u64::MAX;
        Ok(())
    })
    .unwrap();
    let (reused, _, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    assert_ne!(reused.attempt_id, identity.attempt_id);

    planner_workflow::mutate(&fixture.shared, SID, |run| {
        let attempt = run
            .works
            .get_mut("work-1")
            .unwrap()
            .attempt
            .as_mut()
            .unwrap();
        attempt.router_pid = 0;
        attempt.router_started_at_ms = 1;
        Ok(())
    })
    .unwrap();
    let (dead, _, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    assert_ne!(dead.attempt_id, reused.attempt_id);
}

#[test]
fn planner_status_schema_advertises_offset() {
    let schema = planner_workflow::tool_definition();
    assert_eq!(
        schema["inputSchema"]["properties"]["offset"]["type"],
        "integer"
    );
}

#[tokio::test]
async fn required_checks_and_missing_evidence_block_acceptance_and_integration() {
    let fixture = fixture(|_| {});
    let mut spec = work("work-1");
    spec.required_checks = vec!["tests".into()];
    spec.required_integration_evidence = vec!["merge".into()];
    admit(&fixture.shared, spec);
    let (identity, workspace, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    let revision = current_revision(&workspace.path);

    let mut missing_check = artifact(&revision);
    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Artifact {
            key: "missing-check".into(),
            work_id: "work-1".into(),
            attempt_id: identity.attempt_id.clone(),
            artifact: missing_check.clone(),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("required check"), "{error}");

    missing_check.checks.insert("tests".into(), "pass".into());
    missing_check.evidence.clear();
    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Artifact {
            key: "missing-artifact-evidence".into(),
            work_id: "work-1".into(),
            attempt_id: identity.attempt_id.clone(),
            artifact: missing_check.clone(),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("usable evidence"), "{error}");

    missing_check.evidence = vec!["check-log.txt".into()];
    planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Artifact {
            key: "valid-artifact".into(),
            work_id: "work-1".into(),
            attempt_id: identity.attempt_id.clone(),
            artifact: missing_check,
        },
    )
    .await
    .unwrap();
    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Review {
            key: "unknown-review-evidence".into(),
            work_id: "work-1".into(),
            revision: revision.clone(),
            accepted: true,
            evidence: vec!["unknown".into()],
            corrections: None,
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("independent evidence"), "{error}");
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Review {
            key: "accepted-review".into(),
            work_id: "work-1".into(),
            revision: revision.clone(),
            accepted: true,
            evidence: vec!["review.txt".into()],
            corrections: None,
        },
    )
    .await
    .unwrap();
    planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Finished {
            key: "finished".into(),
            work_id: "work-1".into(),
            attempt_id: identity.attempt_id.clone(),
            receipt: receipt(&revision, "handoff"),
        },
    )
    .await
    .unwrap();
    planner_workflow::attempt_ended(
        &fixture.shared,
        SID,
        &identity,
        None,
        "finished".into(),
        false,
    )
    .unwrap();
    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Integrate {
            key: "missing-integration-evidence".into(),
            work_id: "work-1".into(),
            receipt: receipt(&revision, "other"),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("required integration evidence"), "{error}");
}

#[tokio::test]
async fn changing_head_after_artifact_invalidates_review() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (identity, workspace, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    let revision = current_revision(&workspace.path);
    planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Artifact {
            key: "artifact".into(),
            work_id: "work-1".into(),
            attempt_id: identity.attempt_id.clone(),
            artifact: artifact(&revision),
        },
    )
    .await
    .unwrap();
    std::fs::write(workspace.path.join("README.md"), "changed after artifact\n").unwrap();
    git(&workspace.path, &["add", "README.md"]);
    git(
        &workspace.path,
        &["commit", "--quiet", "-m", "changed-head"],
    );
    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Review {
            key: "review-after-head-change".into(),
            work_id: "work-1".into(),
            revision,
            accepted: true,
            evidence: vec!["review.txt".into()],
            corrections: None,
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("HEAD changed"), "{error}");
}

#[tokio::test]
async fn completion_requires_acknowledged_inputs_wakes_and_no_active_work() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let mut request = PromptRequest::new(
        "downstream",
        vec![ContentBlock::from("/implement".to_string())],
    );
    request.meta = serde_json::from_value(json!({"router_acp": {"input_id": "input-1"}})).ok();
    planner_workflow::user_command(&fixture.shared, SID, &mut request).unwrap();
    let mut duplicate_request = PromptRequest::new(
        "downstream",
        vec![ContentBlock::from("/implement".to_string())],
    );
    duplicate_request.meta =
        serde_json::from_value(json!({"router_acp": {"input_id": "input-1"}})).ok();
    let duplicate =
        planner_workflow::user_command(&fixture.shared, SID, &mut duplicate_request).unwrap_err();
    assert!(duplicate.contains("already admitted"), "{duplicate}");

    let (identity, workspace, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    let (blocks, receipts) =
        planner_workflow::input_blocks(&fixture.shared, SID, &identity, &["input-1".into()])
            .unwrap();
    assert!(!blocks.is_empty());
    for id in receipts {
        planner_workflow::mark_input_delivered(&fixture.shared, SID, &id, &identity.work_id)
            .unwrap();
    }
    let revision = current_revision(&workspace.path);
    planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Artifact {
            key: "artifact".into(),
            work_id: "work-1".into(),
            attempt_id: identity.attempt_id.clone(),
            artifact: artifact(&revision),
        },
    )
    .await
    .unwrap();
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Review {
            key: "review".into(),
            work_id: "work-1".into(),
            revision: revision.clone(),
            accepted: true,
            evidence: vec!["review.txt".into()],
            corrections: None,
        },
    )
    .await
    .unwrap();
    planner_workflow::operate(
        &fixture.shared,
        SID,
        Some(&identity),
        Operation::Finished {
            key: "finished".into(),
            work_id: "work-1".into(),
            attempt_id: identity.attempt_id.clone(),
            receipt: receipt(&revision, "handoff"),
        },
    )
    .await
    .unwrap();
    planner_workflow::attempt_ended(&fixture.shared, SID, &identity, None, "done".into(), false)
        .unwrap();
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Integrate {
            key: "integrate".into(),
            work_id: "work-1".into(),
            receipt: receipt(&revision, "merge"),
        },
    )
    .await
    .unwrap();
    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Complete {
            key: "complete-before-acks".into(),
            queue_evidence: "queue verified".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("input and wakes"), "{error}");

    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::AcknowledgeInput {
            key: "ack-input".into(),
            input_id: "input-1".into(),
            owner: Some("work-1".into()),
        },
    )
    .await
    .unwrap();
    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::AcknowledgeInput {
            key: "wrong-owner".into(),
            input_id: "input-1".into(),
            owner: None,
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("original work owner"), "{error}");

    let wake_id = format!("{}:{}", identity.work_id, identity.attempt_id);
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::AcknowledgeWake {
            key: "ack-wake".into(),
            wake_id,
        },
    )
    .await
    .unwrap();
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Complete {
            key: "complete".into(),
            queue_evidence: "queue verified".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        planner_workflow::load(&fixture.shared, SID)
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Complete
    );
}

#[tokio::test]
async fn paused_run_blocks_dependent_actions() {
    let fixture = fixture(|_| {});
    authorize(&fixture.shared);
    let mut run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    run.status = RunStatus::Paused;
    planner_workflow::save(&fixture.shared, SID, &mut run).unwrap();
    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Admit {
            key: "paused-admit".into(),
            work: work("work-1"),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("suspended"), "{error}");
}

#[tokio::test]
async fn allocator_rejects_parent_workspace_and_claim_lease_is_unique() {
    let fixture = fixture(|cfg| {
        let parent = cfg.state_file.clone();
        let _ = parent;
    });
    let parent = fixture.repo.to_string_lossy().replace('"', "\\\"");
    let mut cfg = base_config(&fixture.state_path);
    cfg.routers.planner.workspace = Some(PlannerWorkspaceConfig {
        command: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            format!("cat >/dev/null; printf '%s' '{{\"path\":\"{parent}\",\"lease\":\"shared\"}}'"),
        ],
        timeout_ms: 5_000,
    });
    let shared = Shared::new(cfg.clone()).unwrap();
    let persisted = PersistedSession {
        agent: "test".into(),
        model: "model".into(),
        downstream_session_id: "downstream".into(),
        cwd: fixture.repo.clone(),
        kind: "primary".into(),
        ..PersistedSession::default()
    };
    shared
        .state
        .lock()
        .unwrap()
        .upsert(SID.to_string(), persisted.clone());
    shared.sessions.lock().unwrap().insert(
        SID.to_string(),
        RouterSession::rehydrated(&cfg, &persisted, Vec::new()),
    );
    admit(&shared, work("work-1"));
    let error = planner_workflow::begin_work(&shared, SID, "work-1")
        .await
        .unwrap_err();
    assert!(error.contains("isolated workspace"), "{error}");

    let path = fixture._tmp.path().join("claimed");
    let first = StateStore::load(
        &fixture._tmp.path().join("leases.db"),
        Retention::default(),
    );
    let second = StateStore::load(
        &fixture._tmp.path().join("leases.db"),
        Retention::default(),
    );
    first
        .claim_planner_workspace(&path, SID, "work-1", "lease-1")
        .unwrap();
    let error = second
        .claim_planner_workspace(&path, "other-session", "work-2", "lease-2")
        .unwrap_err();
    assert!(error.contains("held by another"), "{error}");
}

#[tokio::test]
async fn allocation_failure_ends_interruption_and_can_retry_in_same_router() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = repository(&tmp);
    let state_path = tmp.path().join("state.db");
    let marker = tmp.path().join("allocator-failed-once");
    let workspace = tmp.path().join("retry-workspace");
    let shared = shared_for(&repo, &state_path, |cfg| {
        cfg.routers.planner.workspace = Some(PlannerWorkspaceConfig {
            command: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "cat >/dev/null; if [ -e \"$1\" ]; then git clone --quiet \"$2\" \"$3\"; printf '{\"path\":\"%s\",\"lease\":\"retry\"}' \"$3\"; else touch \"$1\"; exit 1; fi".into(),
                "allocator".into(),
                marker.display().to_string(),
                repo.display().to_string(),
                workspace.display().to_string(),
            ],
            timeout_ms: 5_000,
        });
    });
    admit(&shared, work("work-1"));

    let first = planner_workflow::begin_work(&shared, SID, "work-1").await;
    assert!(first.is_err(), "the first allocator call must fail");
    let interrupted = planner_workflow::load(&shared, SID).unwrap().unwrap();
    let failed_work = &interrupted.works["work-1"];
    assert_eq!(failed_work.status, WorkStatus::Interrupted);
    assert!(
        failed_work
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.ended)
    );

    let retry = planner_workflow::begin_work(&shared, SID, "work-1")
        .await
        .unwrap();
    assert_eq!(retry.1.path, std::fs::canonicalize(workspace).unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_input_and_attempt_stop_retain_both_receipts() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (identity, _, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    let input_shared = (0..8)
        .map(|_| shared_for(&fixture.repo, &fixture.state_path, |_| {}))
        .collect::<Vec<_>>();
    let stop_shared = shared_for(&fixture.repo, &fixture.state_path, |_| {});

    std::thread::scope(|scope| {
        let barrier = Arc::new(std::sync::Barrier::new(input_shared.len() + 1));
        let input_threads = input_shared
            .into_iter()
            .enumerate()
            .map(|(index, input_shared)| {
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let input = input_request(
                        Some(&format!("racing-input-{index}")),
                        "a prompt racing a worker stop",
                        false,
                    );
                    planner_workflow::observe_input(&input_shared, SID, &input)
                })
            })
            .collect::<Vec<_>>();
        let stop_barrier = barrier.clone();
        let stop_identity = identity.clone();
        let stop_thread = scope.spawn(move || {
            stop_barrier.wait();
            planner_workflow::attempt_ended(
                &stop_shared,
                SID,
                &stop_identity,
                None,
                "worker stopped while input was arriving".into(),
                true,
            )
        });
        for input_thread in input_threads {
            input_thread.join().unwrap().unwrap();
        }
        stop_thread.join().unwrap().unwrap();
    });

    let run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    for index in 0..8 {
        assert!(run.inputs.contains_key(&format!("racing-input-{index}")));
    }
    assert!(
        run.wakes
            .contains_key(&format!("{}:{}", identity.work_id, identity.attempt_id))
    );
}

#[tokio::test]
async fn input_owner_acknowledgment_requires_delivery_receipt() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let request = input_request(Some("attachment-input"), "deliver this attachment", true);
    planner_workflow::observe_input(&fixture.shared, SID, &request).unwrap();

    let error = planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::AcknowledgeInput {
            key: "ack-undelivered".into(),
            input_id: "attachment-input".into(),
            owner: Some("work-1".into()),
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_ascii_lowercase().contains("deliver"), "{error}");
    assert!(
        !planner_workflow::load(&fixture.shared, SID)
            .unwrap()
            .unwrap()
            .inputs["attachment-input"]
            .acknowledged
    );
}

#[tokio::test]
async fn unacknowledged_wake_redelivers_after_resume() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = repository(&tmp);
    let state_path = tmp.path().join("state.db");
    let accepting = tmp.path().join("accepting");
    let events = tmp.path().join("events.jsonl");
    let script = format!(
        "[ -e '{}' ] || exit 1; cat >> '{}'",
        accepting.display(),
        events.display()
    );
    let shared = shared_for(&repo, &state_path, |cfg| {
        cfg.delegation.lifecycle_hook = Some(DelegateLifecycleHook {
            command: "/bin/sh".into(),
            args: vec!["-c".into(), script.clone()],
            timeout_ms: 5_000,
            max_continuations: 3,
        });
    });
    admit(&shared, work("work-1"));
    let (identity, _, _, _) = planner_workflow::begin_work(&shared, SID, "work-1")
        .await
        .unwrap();
    planner_workflow::attempt_ended(&shared, SID, &identity, None, "worker stopped".into(), true)
        .unwrap();

    for _ in 0..100 {
        if !shared.state.lock().unwrap().outbox_pending(10).is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(!shared.state.lock().unwrap().outbox_pending(10).is_empty());
    assert!(
        !planner_workflow::load(&shared, SID).unwrap().unwrap().wakes
            [&format!("{}:{}", identity.work_id, identity.attempt_id)]
            .acknowledged
    );

    let resumed = shared_for(&repo, &state_path, |cfg| {
        cfg.delegation.lifecycle_hook = Some(DelegateLifecycleHook {
            command: "/bin/sh".into(),
            args: vec!["-c".into(), script.clone()],
            timeout_ms: 5_000,
            max_continuations: 3,
        });
    });
    std::fs::write(&accepting, "").unwrap();
    let hook = resumed.cfg.delegation.lifecycle_hook.clone().unwrap();
    router_acp::delegate_hook::flush(&resumed, &hook).await;

    assert!(resumed.state.lock().unwrap().outbox_pending(10).is_empty());
    let delivered = std::fs::read_to_string(events).unwrap();
    assert!(delivered.lines().any(|line| {
        serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|event| {
                event
                    .get("event")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            })
            .as_deref()
            == Some("planner_wake")
    }));
}

#[tokio::test]
async fn post_completion_input_is_rejected_without_new_unresolved_input() {
    let fixture = fixture(|_| {});
    authorize(&fixture.shared);
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Complete {
            key: "complete-empty-run".into(),
            queue_evidence: "queue verified".into(),
        },
    )
    .await
    .unwrap();

    let error = planner_workflow::observe_input(
        &fixture.shared,
        SID,
        &input_request(Some("late-input"), "arrived after completion", false),
    )
    .unwrap_err();
    assert!(error.to_ascii_lowercase().contains("complete"), "{error}");
    let run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    assert_eq!(run.status, RunStatus::Complete);
    assert!(!run.inputs.contains_key("late-input"));
}

#[test]
fn identical_human_messages_without_client_ids_remain_distinct_inputs() {
    let fixture = fixture(|_| {});
    planner_workflow::ensure(&fixture.shared, SID).unwrap();
    let first = input_request(None, "repeat this exact human message", false);
    let second = input_request(None, "repeat this exact human message", false);
    planner_workflow::observe_input(&fixture.shared, SID, &first).unwrap();
    planner_workflow::observe_input(&fixture.shared, SID, &second).unwrap();

    let run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    assert_eq!(run.inputs.len(), 2);
    assert!(
        run.inputs
            .values()
            .all(|input| input.text == "repeat this exact human message")
    );
}
