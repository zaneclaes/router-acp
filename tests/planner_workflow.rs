use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use agent_client_protocol::schema::v1::{ContentBlock, PromptRequest, ResourceLink};
use router_acp::config::{
    Config, DelegateLifecycleHook, PlannerAuditConfig, PlannerCapacityConfig, PlannerHostConfig,
    PlannerShutdownConfig, PlannerWorkspaceConfig,
};
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

/// Integration is the work's terminal state: the router queues exactly one
/// `work_finished` naming the work, its child id and workspace, so a host can
/// retire the tree. A refusing host keeps it queued and never fails the
/// `integrate` operation; a later save does not repeat it.
#[tokio::test]
async fn integration_queues_one_work_finished_event_for_the_host() {
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
    let (identity, workspace, _, _) = planner_workflow::begin_work(&shared, SID, "work-1")
        .await
        .unwrap();
    let revision = current_revision(&workspace.path);
    planner_workflow::operate(
        &shared,
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
        &shared,
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
        &shared,
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
    planner_workflow::attempt_ended(&shared, SID, &identity, None, "done".into(), false).unwrap();
    let finished_events = |shared: &Arc<Shared>| {
        shared
            .state
            .lock()
            .unwrap()
            .outbox_pending(100)
            .into_iter()
            .filter_map(|(_, payload)| serde_json::from_str::<serde_json::Value>(&payload).ok())
            .filter(|event| event["event"] == "work_finished")
            .collect::<Vec<_>>()
    };
    assert!(
        finished_events(&shared).is_empty(),
        "a finished but unintegrated work still needs its workspace"
    );

    planner_workflow::operate(
        &shared,
        SID,
        None,
        Operation::Integrate {
            key: "integrate".into(),
            work_id: "work-1".into(),
            receipt: receipt(&revision, "merge"),
        },
    )
    .await
    .expect("a refusing host never fails integration");
    let queued = finished_events(&shared);
    assert_eq!(queued.len(), 1, "{queued:?}");
    let event = &queued[0];
    let run = planner_workflow::load(&shared, SID).unwrap().unwrap();
    let integrated = &run.works["work-1"];
    assert_eq!(integrated.status, WorkStatus::Integrated);
    assert_eq!(event["work_id"], "work-1");
    assert_eq!(event["plan_id"], "plan-1");
    assert_eq!(event["external_id"], "external-1");
    assert_eq!(event["status"], "integrated");
    assert_eq!(event["parent_router_session_id"], SID);
    assert_eq!(event["worker_id"], integrated.child_id.as_str());
    assert_eq!(event["child_id"], integrated.child_id.as_str());
    assert_eq!(event["cwd"], workspace.path.display().to_string());
    assert_eq!(event["attempt_id"], identity.attempt_id.as_str());
    assert_eq!(event["attempt_ended"], true);
    assert_eq!(event["revision"], revision.as_str());
    assert_eq!(event["router_pid"], std::process::id());

    // A later save of the integrated work queues nothing more.
    let mut run = run;
    planner_workflow::save(&shared, SID, &mut run).unwrap();
    assert_eq!(finished_events(&shared).len(), 1);

    // Let the immediate delivery `integrate` spawned finish being refused.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    std::fs::write(&accepting, "").unwrap();
    let hook = shared.cfg.delegation.lifecycle_hook.clone().unwrap();
    router_acp::delegate_hook::flush(&shared, &hook).await;
    assert!(finished_events(&shared).is_empty());
    let delivered = std::fs::read_to_string(&events).unwrap();
    let finished = delivered
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|event| event["event"] == "work_finished")
        .collect::<Vec<_>>();
    assert_eq!(finished.len(), 1, "{delivered}");
    assert_eq!(finished[0]["work_id"], "work-1");
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
    let first = StateStore::load(&fixture._tmp.path().join("leases.db"), Retention::default());
    let second = StateStore::load(&fixture._tmp.path().join("leases.db"), Retention::default());
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
                "case \"$(cat)\" in *adopt-native*) echo '{\"adopted\":null}'; exit 0;; esac; if [ -e \"$1\" ]; then git clone --quiet \"$2\" \"$3\"; printf '{\"path\":\"%s\",\"lease\":\"retry\"}' \"$3\"; else touch \"$1\"; exit 1; fi".into(),
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

/// Kory Code's accepted-plan handoff is a human `[router: phase=…]`
/// directive, not a slash command. On a finished planning run it must reopen
/// the run in implementation, as `/implement` does, so its work can be admitted.
#[tokio::test]
async fn human_phase_directive_reopens_a_complete_run_for_implementation() {
    let fixture = fixture(|_| {});
    authorize(&fixture.shared);
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Complete {
            key: "complete-planning".into(),
            queue_evidence: "queue verified".into(),
        },
    )
    .await
    .unwrap();
    let mut run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    run.phase = router_acp::config::PlannerPhase::Planning;
    run.execution_request = None;
    planner_workflow::save(&fixture.shared, SID, &mut run).unwrap();

    let handoff = "[router: phase=implementation]\n[planner-handoff] The user accepted the plan.";
    let mut agent = input_request(Some("agent-handoff"), handoff, false);
    agent.meta = serde_json::from_value(json!({
        "router_acp": {"input_id": "agent-handoff", "agent_origin": true}
    }))
    .ok();
    let error = planner_workflow::observe_input(&fixture.shared, SID, &agent).unwrap_err();
    assert!(
        error.contains("complete"),
        "an agent cannot reopen the run: {error}"
    );
    assert_eq!(
        fixture
            .shared
            .with_session(SID, |s| s.planner_explicit_phase)
            .flatten(),
        None,
        "an agent's directive does not choose the routing pool"
    );

    planner_workflow::observe_input(
        &fixture.shared,
        SID,
        &input_request(Some("human-handoff"), handoff, false),
    )
    .unwrap();
    let run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    assert_eq!(run.status, RunStatus::Running);
    assert_eq!(run.phase, router_acp::config::PlannerPhase::Implementation);
    assert_eq!(run.execution_input_id.as_deref(), Some("human-handoff"));
    assert_eq!(
        fixture
            .shared
            .with_session(SID, |s| s.planner_explicit_phase)
            .flatten(),
        Some(router_acp::config::PlannerPhase::Implementation),
        "a human phase directive routes from that phase's pool, as the command does"
    );
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Admit {
            key: "admit-after-handoff".into(),
            work: work("work-1"),
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn post_completion_agent_input_is_rejected_without_new_unresolved_input() {
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

    let mut late = input_request(Some("late-input"), "arrived after completion", false);
    late.meta = serde_json::from_value(json!({
        "router_acp": {"input_id": "late-input", "agent_origin": true}
    }))
    .ok();
    let error = planner_workflow::observe_input(&fixture.shared, SID, &late).unwrap_err();
    assert!(error.to_ascii_lowercase().contains("complete"), "{error}");
    let run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    assert_eq!(run.status, RunStatus::Complete);
    assert!(!run.inputs.contains_key("late-input"));
}

/// A person's plain follow-up to a finished run (e.g. "edit the plan") is the
/// next turn of the conversation, not an error; it reopens the run in its last
/// phase and stays unresolved until the planner reconciles it.
#[tokio::test]
async fn post_completion_human_follow_up_reopens_the_run_in_its_phase() {
    let fixture = fixture(|_| {});
    authorize(&fixture.shared);
    planner_workflow::operate(
        &fixture.shared,
        SID,
        None,
        Operation::Complete {
            key: "complete-planning".into(),
            queue_evidence: "queue verified".into(),
        },
    )
    .await
    .unwrap();
    let mut run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    run.phase = router_acp::config::PlannerPhase::Planning;
    planner_workflow::save(&fixture.shared, SID, &mut run).unwrap();

    planner_workflow::observe_input(
        &fixture.shared,
        SID,
        &input_request(
            Some("follow-up"),
            "Boundaries are approved; edit the plan.",
            false,
        ),
    )
    .unwrap();
    let run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    assert_eq!(run.status, RunStatus::Running);
    assert_eq!(run.phase, router_acp::config::PlannerPhase::Planning);
    assert!(!run.inputs["follow-up"].acknowledged);
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

/// goose sends a typed `/implement` that names a repository skill as that
/// skill's expansion, with its `<turn-context>` span in a separate block.
fn goose_skill_expansion(skill: &str, args: &str) -> Vec<ContentBlock> {
    vec![
        ContentBlock::from(
            "<turn-context>\n<t>2026-10-12</t>\nCurrent tasks and notes: (none)\n</turn-context>"
                .to_string(),
        ),
        ContentBlock::from(format!(
            "# Loaded Skill: {skill} (skill)\nThe skill description.\n\n## Content\n\n# {skill}\n\nARGUMENTS: inside the skill text\n\nBody.\n\nARGUMENTS: {args}\n\n## Supporting Files\n\nSkill directory: /repo/.agents/skills/{skill}\n"
        )),
    ]
}

#[test]
fn goose_skill_expansions_are_the_authentic_mode_commands() {
    let request = |prompt: Vec<ContentBlock>| PromptRequest::new("downstream", prompt);
    let implement = request(goose_skill_expansion("implement", "Docs/Plans/X.md extra"));
    assert_eq!(
        planner_workflow::command(&implement),
        Some(router_acp::config::PlannerPhase::Implementation)
    );
    assert!(planner_workflow::explicit_command(&implement));
    assert_eq!(
        planner_workflow::command(&request(goose_skill_expansion("plan", ""))),
        Some(router_acp::config::PlannerPhase::Planning)
    );
    // Another skill's expansion, or prose that only mentions one, is no command.
    assert_eq!(
        planner_workflow::command(&request(goose_skill_expansion("review", ""))),
        None
    );
    assert_eq!(
        planner_workflow::command(&request(vec![ContentBlock::from(
            "please # Loaded Skill: implement (skill)".to_string()
        )])),
        None
    );
    // Slash commands behind a turn-context block are explicit; the bare verb is
    // a command only for a session already on the planner strategy.
    let behind_context = request(vec![
        ContentBlock::from("<turn-context>x</turn-context>".to_string()),
        ContentBlock::from("/implement".to_string()),
    ]);
    assert!(planner_workflow::explicit_command(&behind_context));
    let bare = request(vec![ContentBlock::from(
        "implement the login fix".to_string(),
    )]);
    assert_eq!(
        planner_workflow::command(&bare),
        Some(router_acp::config::PlannerPhase::Implementation)
    );
    assert!(!planner_workflow::explicit_command(&bare));
    // An internal role invocation never re-selects a mode.
    let mut internal = request(goose_skill_expansion("implement", ""));
    internal.meta =
        serde_json::from_value(json!({"router_acp": {"planner_role": "select-plan"}})).ok();
    assert_eq!(planner_workflow::command(&internal), None);
}

#[test]
fn a_goose_expansion_becomes_the_role_invocation_with_its_typed_arguments() {
    let fixture = fixture(|_| {});
    let mut request = PromptRequest::new(
        "downstream",
        goose_skill_expansion("implement", "Docs/Plans/X.md extra"),
    );
    let phase = planner_workflow::user_command(&fixture.shared, SID, &mut request).unwrap();
    assert_eq!(
        phase,
        Some(router_acp::config::PlannerPhase::Implementation)
    );
    let ContentBlock::Text(context) = &request.prompt[0] else {
        panic!("turn context block changed type")
    };
    assert!(
        context.text.starts_with("<turn-context>"),
        "{}",
        context.text
    );
    let ContentBlock::Text(invocation) = &request.prompt[1] else {
        panic!("command block changed type")
    };
    assert!(
        invocation
            .text
            .contains("[router-acp planner role: select-plan]"),
        "{}",
        invocation.text
    );
    assert!(
        invocation
            .text
            .ends_with("Arguments: Docs/Plans/X.md extra"),
        "{}",
        invocation.text
    );
    assert!(
        !invocation.text.contains("# Loaded Skill"),
        "{}",
        invocation.text
    );
}

#[tokio::test]
async fn an_attempt_that_handed_off_reads_as_that_handoff() {
    let fixture = fixture(|_| {});
    admit(&fixture.shared, work("work-1"));
    let (actor, _, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    planner_workflow::record_handoff(
        &fixture.shared,
        SID,
        &actor,
        router_acp::delegate_hook::Handoff {
            kind: "waiting".into(),
            message: "WAITING build 3729 is still running\nmore detail".into(),
        },
    )
    .unwrap();
    planner_workflow::attempt_ended(&fixture.shared, SID, &actor, None, "done".into(), false)
        .unwrap();
    let run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    let work = &run.works["work-1"];
    assert_eq!(work.status, planner_workflow::WorkStatus::Interrupted);
    assert_eq!(
        work.reason.as_deref(),
        Some("handed off `waiting`: WAITING build 3729 is still running")
    );

    // A new attempt forgets the old handoff; ending it without one reads as before.
    let (next, _, _, _) = planner_workflow::begin_work(&fixture.shared, SID, "work-1")
        .await
        .unwrap();
    planner_workflow::attempt_ended(&fixture.shared, SID, &next, None, "gone".into(), false)
        .unwrap();
    let run = planner_workflow::load(&fixture.shared, SID)
        .unwrap()
        .unwrap();
    assert!(
        run.works["work-1"]
            .reason
            .as_deref()
            .unwrap()
            .starts_with("attempt stopped without a valid artifact")
    );
}

/// A parent that ended its turn blocked on something outside the run has no
/// child left to wake it. With no worker running it gets a heartbeat wake every
/// 20 idle minutes, with no backoff: only finished work ends a run, so a parent
/// that stopped on a reason of its own is woken to act on it.
#[tokio::test]
async fn idle_parent_with_no_running_worker_gets_heartbeats_without_backoff() {
    // One slot, so the running worker below fills the run.
    let fixture = fixture(|cfg| cfg.delegation.max_concurrent = 1);
    let shared = &fixture.shared;
    admit(shared, work("work-1"));
    let (identity, _, _, _) = planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    let heartbeats = || {
        planner_workflow::load(shared, SID)
            .unwrap()
            .unwrap()
            .wakes
            .into_iter()
            .filter(|(id, _)| id.starts_with("heartbeat:"))
            .collect::<Vec<_>>()
    };
    let beat = |now: i64| {
        let run = planner_workflow::load(shared, SID).unwrap().unwrap();
        router_acp::planner_wake::heartbeat(shared, SID, &run, now);
    };
    let acknowledge_all = || {
        planner_workflow::mutate(shared, SID, |run| {
            run.wakes.values_mut().for_each(|w| w.acknowledged = true);
            Ok(())
        })
        .unwrap();
    };
    shared.state.lock().unwrap().log(
        SID,
        &router_acp::state::LogEntry {
            kind: "agent_response".into(),
            role: "agent".into(),
            summary: "blocked".into(),
            ..Default::default()
        },
    );
    let (last, _) = shared.state.lock().unwrap().last_activity(SID).unwrap();

    // A run whose every slot holds a running worker will wake the parent itself.
    beat(last + 3 * 3600);
    assert!(heartbeats().is_empty());

    planner_workflow::attempt_ended(shared, SID, &identity, None, "stopped".into(), true).unwrap();
    // The worker's own wake is still unacknowledged.
    beat(last + 3 * 3600);
    assert!(heartbeats().is_empty());
    acknowledge_all();

    beat(last + 19 * 60);
    assert!(heartbeats().is_empty());
    beat(last + 21 * 60);
    assert_eq!(heartbeats().len(), 1);
    let reason = heartbeats()[0].1.reason.clone();
    assert!(reason.starts_with("heartbeat:"));
    assert!(reason.contains("ends only when all of its work is done"));
    assert!(!reason.contains("same waiting reason"));
    // An unacknowledged heartbeat is not doubled.
    beat(last + 25 * 60);
    assert_eq!(heartbeats().len(), 1);
    acknowledge_all();
    // The next one comes at once: no backoff.
    beat(last + 22 * 60);
    assert_eq!(heartbeats().len(), 2);
    assert_eq!(
        planner_workflow::load(shared, SID)
            .unwrap()
            .unwrap()
            .idle_heartbeats,
        2
    );
}

/// A parent that left worker slots empty (host memory was short, the roadmap
/// had nothing runnable) gets no child event when that changes while its other
/// workers run quietly. A free slot alone earns it a heartbeat, and every wake
/// carries the census of used and free slots.
#[tokio::test]
async fn idle_parent_below_its_worker_limit_gets_capacity_heartbeats() {
    let fixture = fixture(|cfg| cfg.delegation.max_concurrent = 3);
    let shared = &fixture.shared;
    admit(shared, work("work-1"));
    admit(shared, work("work-2"));
    planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    planner_workflow::mutate(shared, SID, |run| {
        run.works.get_mut("work-2").unwrap().status = planner_workflow::WorkStatus::Interrupted;
        Ok(())
    })
    .unwrap();
    shared.state.lock().unwrap().log(
        SID,
        &router_acp::state::LogEntry {
            kind: "agent_response".into(),
            role: "agent".into(),
            summary: "waiting on host memory".into(),
            ..Default::default()
        },
    );
    let (last, _) = shared.state.lock().unwrap().last_activity(SID).unwrap();
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();

    let census = router_acp::planner_wake::capacity_line(shared, &run);
    assert!(census.starts_with("[Capacity] 1 of 3 worker slots in use, 2 free."));
    assert!(census.contains("Interrupted, awaiting retry: work-2."));
    assert!(census.contains("fill the free slots"));

    router_acp::planner_wake::heartbeat(shared, SID, &run, last + 19 * 60);
    let beats = |run: &planner_workflow::PlannerRun| {
        run.wakes
            .iter()
            .filter(|(id, _)| id.starts_with("heartbeat:"))
            .map(|(_, w)| w.reason.clone())
            .collect::<Vec<_>>()
    };
    assert!(beats(&planner_workflow::load(shared, SID).unwrap().unwrap()).is_empty());
    router_acp::planner_wake::heartbeat(shared, SID, &run, last + 21 * 60);
    let reasons = beats(&planner_workflow::load(shared, SID).unwrap().unwrap());
    assert_eq!(reasons.len(), 1);
    assert!(reasons[0].starts_with("heartbeat: 1 of 3 worker slots are in use"));
}

/// Each planner attempt runs in its own agent process, kept open after its
/// turn for a follow-up. Once the attempt can take no further turn its
/// session is closed, so finished workers stop holding memory.
#[tokio::test]
async fn sessions_of_attempts_that_cannot_take_another_turn_are_closed() {
    let fixture = fixture(|_| {});
    let shared = &fixture.shared;
    for id in ["finished", "accepted", "superseded"] {
        admit(shared, work(id));
    }
    let mut identities = BTreeMap::new();
    for id in ["finished", "accepted", "superseded"] {
        let (identity, _, _, _) = planner_workflow::begin_work(shared, SID, id).await.unwrap();
        identities.insert(id, identity);
    }
    planner_workflow::mutate(shared, SID, |run| {
        for (id, status) in [
            ("finished", planner_workflow::WorkStatus::Finished),
            ("accepted", planner_workflow::WorkStatus::Accepted),
            ("superseded", planner_workflow::WorkStatus::Running),
        ] {
            let work = run.works.get_mut(id).unwrap();
            work.status = status;
            work.attempt.as_mut().unwrap().ended = id != "superseded";
        }
        // A newer attempt replaced the one the open session belongs to.
        run.works
            .get_mut("superseded")
            .unwrap()
            .attempt
            .as_mut()
            .unwrap()
            .id = "newer".into();
        Ok(())
    })
    .unwrap();
    for (id, identity) in &identities {
        shared.live_delegates.lock().unwrap().insert(
            format!("d-{id}"),
            router_acp::session::LiveDelegate {
                parent_sid: SID.into(),
                process_key: router_acp::downstream::ProcessKey(format!("key-{id}")),
                downstream_sid: format!("down-{id}"),
                candidate: router_acp::candidate::CandidateId::parse("test/model").unwrap(),
                capture: Arc::default(),
                sub_sid: format!("sub-{id}"),
                lifecycle: None,
                worker_id: format!("w-{id}"),
                planner: Some(identity.clone()),
                turns: 1,
            },
        );
    }
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    router_acp::planner_wake::close_stale_delegates(shared, SID, &run);
    let mut open = shared
        .live_delegates
        .lock()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    open.sort();
    assert_eq!(open, vec!["d-accepted".to_string()]);
}

/// A planner parent that awaits its background workers holds its own turn,
/// and with it every wake and review, until the slowest worker finishes (a
/// live run sat 25 minutes in one await). Planner work delegates report
/// through router wakes, so the await skips them and says so.
#[tokio::test]
async fn awaiting_background_work_never_waits_on_planner_workers() {
    let fixture = fixture(|_| {});
    let shared = &fixture.shared;
    let job = |planner: bool, result: Option<Result<String, String>>| {
        router_acp::session::BackgroundDelegate {
            parent_sid: SID.into(),
            summary: "job".into(),
            started: std::time::Instant::now(),
            result,
            planner,
        }
    };
    let await_all = || {
        router_acp::delegate_mcp::run_delegate_await(
            shared,
            SID,
            router_acp::delegate_mcp::DelegateAwaitArgs {
                delegate_ids: vec![],
                timeout_seconds: Some(5),
            },
        )
    };
    shared
        .background_delegates
        .lock()
        .unwrap()
        .insert("b-worker".into(), job(true, None));
    let started = std::time::Instant::now();
    let out = await_all().await.unwrap();
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
    assert!(out.starts_with("Planner work delegates are not awaited"));

    // An ordinary background job is still collected, with the note beside it.
    shared
        .background_delegates
        .lock()
        .unwrap()
        .insert("b-other".into(), job(false, Some(Ok("done".into()))));
    let out = await_all().await.unwrap();
    assert!(out.contains("=== delegate b-other — done ==="));
    assert!(out.contains("Planner work delegates are not awaited"));
    assert!(
        shared
            .background_delegates
            .lock()
            .unwrap()
            .contains_key("b-worker")
    );
}

// ---------------------------------------------------------------------------
// Router-enforced capacity (`routers.planner.capacity`)
// ---------------------------------------------------------------------------

fn candidate_json(id: &str) -> serde_json::Value {
    json!({"work":{"work_id":format!("work-{id}"),"plan_id":format!("plan-{id}"),
        "scope":format!("implement plan-{id}"),"external_id":format!("EXT-{id}")},
        "task":format!("Implement plan-{id} end to end.")})
}

/// A fake capacity host: copies its stdin to `stdin_copy` and answers with
/// the given candidates, in order.
fn host_script(ids: &[&str], stdin_copy: &Path) -> String {
    let reply = json!({"candidates": ids.iter().map(|id| candidate_json(id)).collect::<Vec<_>>()});
    format!("cat > '{}'; printf '%s' '{}'", stdin_copy.display(), reply)
}

fn capacity_host(
    cfg: &mut Config,
    script: String,
    auto_admit_after_wakes: u32,
    auto_admit_after_secs: u64,
) {
    cfg.routers.planner.capacity = Some(PlannerCapacityConfig {
        command: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        timeout_ms: 5_000,
        wake_after_secs: 60,
        auto_admit_after_wakes,
        auto_admit_after_secs,
        inflight_wake_secs: 300,
    });
}

/// The parent's turn ended at the returned unix second.
fn parent_turn_ended(shared: &Arc<Shared>) -> i64 {
    shared.state.lock().unwrap().log(
        SID,
        &router_acp::state::LogEntry {
            kind: "agent_response".into(),
            role: "agent".into(),
            summary: "turn ended".into(),
            ..Default::default()
        },
    );
    shared.state.lock().unwrap().last_activity(SID).unwrap().0
}

fn pin_parent(shared: &Arc<Shared>) {
    shared.with_session(SID, |s| {
        s.pin = Some(router_acp::session::PinInfo {
            candidate: router_acp::candidate::CandidateId::parse("test/model").unwrap(),
            process_key: router_acp::downstream::ProcessKey("parent".into()),
            downstream_sid: "downstream".into(),
            available_modes: Vec::new(),
        });
    });
}

fn wakes_with(shared: &Arc<Shared>, prefix: &str) -> Vec<(String, planner_workflow::Wake)> {
    planner_workflow::load(shared, SID)
        .unwrap()
        .unwrap()
        .wakes
        .into_iter()
        .filter(|(id, _)| id.starts_with(prefix))
        .collect()
}

/// What a delivery does to router-owned wakes.
fn deliver_all(shared: &Arc<Shared>) {
    planner_workflow::mutate(shared, SID, |run| {
        run.wakes.values_mut().for_each(|w| {
            w.deliveries += 1;
            w.acknowledged = true;
        });
        Ok(())
    })
    .unwrap();
}

async fn tick(shared: &Arc<Shared>, now: i64) {
    router_acp::planner_capacity::tick(shared, SID, now).await;
}

#[tokio::test]
async fn idle_parent_below_its_limit_gets_capacity_wakes_that_never_give_up() {
    let tmp = tempfile::tempdir().unwrap();
    let stdin_copy = tmp.path().join("host-stdin.json");
    let script = host_script(&["a", "b"], &stdin_copy);
    let fixture = fixture(|cfg| {
        cfg.delegation.max_concurrent = 3;
        capacity_host(cfg, script, 1_000, 1_000_000);
    });
    let shared = &fixture.shared;
    authorize(shared);
    let last = parent_turn_ended(shared);

    tick(shared, last + 30).await;
    assert!(
        wakes_with(shared, "capacity:").is_empty(),
        "not idle long enough"
    );
    let asked: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&stdin_copy).unwrap()).unwrap();
    assert_eq!(asked["event"], "capacity");
    assert_eq!(asked["free"], 3);
    assert_eq!(asked["max"], 3);
    assert_eq!(asked["cwd"], json!(fixture.repo));

    tick(shared, last + 61).await;
    let wakes = wakes_with(shared, "capacity:");
    assert_eq!(wakes.len(), 1);
    let reason = &wakes[0].1.reason;
    assert!(reason.contains("3 free"), "{reason}");
    assert!(
        reason.contains("1. work_id work-a (external_id EXT-a"),
        "{reason}"
    );
    assert!(
        reason.contains("2. work_id work-b (external_id EXT-b"),
        "{reason}"
    );
    assert!(reason.contains("`hold`"), "{reason}");
    assert!(reason.contains("Breadth is always permitted"), "{reason}");

    // An undelivered capacity wake is not doubled.
    tick(shared, last + 200).await;
    assert_eq!(wakes_with(shared, "capacity:").len(), 1);

    // The heartbeat yields to capacity wakes while candidates exist.
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    deliver_all(shared);
    router_acp::planner_wake::heartbeat(shared, SID, &run, last + 21 * 60);
    assert!(wakes_with(shared, "heartbeat:").is_empty());

    // Every wake_after_secs, with no give-up after any number of deliveries.
    let mut now = last + 200;
    for delivered in 1..=8 {
        now += 61;
        tick(shared, now).await;
        assert_eq!(wakes_with(shared, "capacity:").len(), delivered + 1);
        deliver_all(shared);
    }
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    assert_eq!(run.capacity.unfilled_wakes, 9);
    assert!(
        run.works.is_empty(),
        "auto-admission is configured far away"
    );

    // Status and the census carry the same block.
    let status: serde_json::Value = serde_json::from_str(
        &planner_workflow::operate(shared, SID, None, Operation::Status { offset: 0 })
            .await
            .unwrap(),
    )
    .unwrap();
    let capacity = &status["capacity"];
    assert_eq!(capacity["live"], 0);
    assert_eq!(capacity["max"], 3);
    assert_eq!(capacity["free"], 3);
    assert_eq!(capacity["host"]["ok"], true);
    assert_eq!(capacity["unheld"], json!(["work-a", "work-b"]));
    let census = router_acp::planner_wake::capacity_line(shared, &run);
    assert!(
        census.contains(
            "Capacity host candidates no hold covers, in order: work-a (EXT-a), work-b (EXT-b)."
        ),
        "{census}"
    );
}

#[tokio::test]
async fn unanswered_capacity_wakes_make_the_router_admit_and_start_exactly_the_free_slots() {
    let tmp = tempfile::tempdir().unwrap();
    let script = host_script(&["a", "b", "c"], &tmp.path().join("stdin.json"));
    let fixture = fixture(|cfg| {
        cfg.delegation.max_concurrent = 2;
        capacity_host(cfg, script, 2, 1_000_000);
    });
    let shared = &fixture.shared;
    authorize(shared);
    pin_parent(shared);
    let last = parent_turn_ended(shared);

    tick(shared, last + 61).await;
    assert_eq!(wakes_with(shared, "capacity:").len(), 1);
    deliver_all(shared);
    tick(shared, last + 122).await;
    assert_eq!(wakes_with(shared, "capacity:").len(), 2);
    assert!(
        planner_workflow::load(shared, SID)
            .unwrap()
            .unwrap()
            .works
            .is_empty()
    );
    deliver_all(shared);

    // Two capacity wakes went unanswered: the router fills the slots itself.
    tick(shared, last + 130).await;
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    assert_eq!(
        run.works.keys().cloned().collect::<Vec<_>>(),
        vec!["work-a".to_string(), "work-b".to_string()],
        "exactly the free count, in host order"
    );
    for id in ["work-a", "work-b"] {
        let work = &run.works[id];
        assert!(run.receipts.contains_key(&format!("auto-admit:{id}:1")));
        assert!(work.attempt.is_some(), "{id} was started");
        let workspace = work
            .workspace
            .as_ref()
            .expect("allocated through the workspace path");
        assert_ne!(
            workspace.path,
            std::fs::canonicalize(&fixture.repo).unwrap()
        );
        assert_eq!(
            work.spec.external_id.as_deref(),
            Some(&*format!("EXT-{}", &id[5..]))
        );
    }
    let jobs = shared.background_delegates.lock().unwrap();
    assert_eq!(jobs.values().filter(|j| j.planner).count(), 2);
    drop(jobs);
    let auto = run.capacity.last_auto_admission.as_ref().unwrap();
    assert_eq!(auto.admitted, vec!["work-a", "work-b"]);
    assert_eq!(auto.free, 2);
    let notice = &run.wakes["auto-admit:1"].reason;
    assert!(
        notice.contains(
            "router admitted work-a, work-b to fill 2 free slots; review them as normal."
        ),
        "{notice}"
    );
    assert_eq!(run.capacity.unfilled_wakes, 0);
}

#[tokio::test]
async fn a_parent_idle_past_the_limit_is_auto_admitted_without_wakes() {
    let tmp = tempfile::tempdir().unwrap();
    let script = host_script(&["a"], &tmp.path().join("stdin.json"));
    let fixture = fixture(|cfg| {
        cfg.delegation.max_concurrent = 2;
        capacity_host(cfg, script, 1_000, 300);
    });
    let shared = &fixture.shared;
    authorize(shared);
    pin_parent(shared);
    let last = parent_turn_ended(shared);
    tick(shared, last + 301).await;
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    assert_eq!(run.works.keys().collect::<Vec<_>>(), vec!["work-a"]);
    assert!(run.wakes.contains_key("auto-admit:1"));
}

#[tokio::test]
async fn only_three_hold_reasons_keep_a_candidate_out_and_holds_expire() {
    let tmp = tempfile::tempdir().unwrap();
    let script = host_script(&["a", "b"], &tmp.path().join("stdin.json"));
    let fixture = fixture(|cfg| {
        cfg.delegation.max_concurrent = 3;
        capacity_host(cfg, script, 1_000, 1_000_000);
    });
    let shared = &fixture.shared;
    authorize(shared);
    let hold = |key: &str, plan_id: &str, reason: &str, detail: &str| {
        planner_workflow::operate(
            shared,
            SID,
            None,
            Operation::Hold {
                key: key.into(),
                plan_id: plan_id.into(),
                reason: reason.into(),
                detail: detail.into(),
            },
        )
    };
    let error = hold("h0", "plan-a", "waiting-for-review", "later")
        .await
        .unwrap_err();
    for reason in ["source-conflict", "dependency", "disk-floor", "not allowed"] {
        assert!(error.contains(reason), "{error}");
    }
    let error = hold("h1", "plan-a", "source-conflict", "touches the same files")
        .await
        .unwrap_err();
    assert!(error.contains("live work_id"), "{error}");
    let error = hold("h2", "plan-a", "disk-floor", "low").await.unwrap_err();
    assert!(error.contains("free bytes"), "{error}");
    hold("h3", "plan-a", "disk-floor", "free 1073741824 bytes")
        .await
        .unwrap();

    let last = parent_turn_ended(shared);
    tick(shared, last + 61).await;
    let wakes = wakes_with(shared, "capacity:");
    assert_eq!(wakes.len(), 1);
    assert!(
        !wakes[0].1.reason.contains("work-a"),
        "held: {}",
        wakes[0].1.reason
    );
    assert!(wakes[0].1.reason.contains("work-b"));
    deliver_all(shared);

    // With every candidate held the parent gets no capacity wake, and the
    // 20-minute heartbeat re-checks the blockers instead.
    hold("h4", "plan-b", "dependency", "waits for plan-a")
        .await
        .unwrap();
    let last = parent_turn_ended(shared);
    tick(shared, last + 61).await;
    assert_eq!(wakes_with(shared, "capacity:").len(), 1);
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    router_acp::planner_wake::heartbeat(shared, SID, &run, last + 21 * 60);
    assert_eq!(wakes_with(shared, "heartbeat:").len(), 1);

    // Holds expire after 30 minutes.
    let now_ms = chrono::Utc::now().timestamp_millis();
    assert!(router_acp::planner_capacity::unheld_candidates(&run, now_ms).is_empty());
    let later: Vec<_> =
        router_acp::planner_capacity::unheld_candidates(&run, now_ms + 31 * 60 * 1000)
            .into_iter()
            .map(|c| c.work.work_id)
            .collect();
    assert_eq!(later, vec!["work-a", "work-b"]);
}

#[tokio::test]
async fn a_failed_host_is_unknown_never_no_work() {
    for (script, expected) in [
        ("cat >/dev/null; echo boom >&2; exit 3", "boom"),
        ("cat >/dev/null; printf 'not json'", "invalid JSON"),
    ] {
        let fixture = fixture(|cfg| {
            cfg.delegation.max_concurrent = 2;
            capacity_host(cfg, script.into(), 1, 1);
        });
        let shared = &fixture.shared;
        authorize(shared);
        pin_parent(shared);
        let last = parent_turn_ended(shared);
        let mut now = last;
        for _ in 0..3 {
            now += 61;
            tick(shared, now).await;
            deliver_all(shared);
        }
        let wakes = wakes_with(shared, "capacity:");
        assert_eq!(wakes.len(), 3, "an unknown answer keeps waking the parent");
        assert!(wakes.iter().all(|(_, w)| w.reason.contains("UNAVAILABLE")
            && w.reason.contains(expected)
            && w.reason.contains("not \"no work\"")));
        let run = planner_workflow::load(shared, SID).unwrap().unwrap();
        assert!(
            run.works.is_empty(),
            "nothing is auto-admitted from an unknown answer"
        );
        assert!(!run.capacity.host.as_ref().unwrap().ok);
        let error = planner_workflow::operate(
            shared,
            SID,
            None,
            Operation::Complete {
                key: format!("complete-{expected}"),
                queue_evidence: "queue-census.md".into(),
            },
        )
        .await
        .unwrap_err();
        assert!(error.contains("capacity host is unavailable"), "{error}");
        assert!(error.contains(expected), "{error}");
    }
}

#[tokio::test]
async fn completion_is_refused_while_an_unheld_candidate_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let script = host_script(&["a"], &tmp.path().join("stdin.json"));
    let fixture = fixture(|cfg| capacity_host(cfg, script, 1_000, 1_000_000));
    let shared = &fixture.shared;
    authorize(shared);
    let complete = |key: &'static str| {
        planner_workflow::operate(
            shared,
            SID,
            None,
            Operation::Complete {
                key: key.into(),
                queue_evidence: "queue-census.md".into(),
            },
        )
    };
    let error = complete("complete-1").await.unwrap_err();
    assert!(error.contains("work-a (EXT-a)"), "{error}");
    planner_workflow::operate(
        shared,
        SID,
        None,
        Operation::Hold {
            key: "hold-a".into(),
            plan_id: "plan-a".into(),
            reason: "dependency".into(),
            detail: "plan-z must land first".into(),
        },
    )
    .await
    .unwrap();
    complete("complete-2").await.unwrap();
    assert_eq!(
        planner_workflow::load(shared, SID).unwrap().unwrap().status,
        RunStatus::Complete
    );
}

#[tokio::test]
async fn completion_lists_unfinished_works() {
    let fixture = fixture(|_| {});
    let shared = &fixture.shared;
    admit(shared, work("work-1"));
    let error = planner_workflow::operate(
        shared,
        SID,
        None,
        Operation::Complete {
            key: "complete".into(),
            queue_evidence: "queue-census.md".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("work-1 (Admitted)"), "{error}");
}

/// Wait for a background attempt to end (the test agent cannot serve it).
async fn attempt_settled(shared: &Arc<Shared>, work_id: &str) {
    for _ in 0..200 {
        let run = planner_workflow::load(shared, SID).unwrap().unwrap();
        let work = &run.works[work_id];
        if work.status == WorkStatus::Interrupted && work.attempt.as_ref().is_some_and(|a| a.ended)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("attempt of {work_id} never ended");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_that_stops_without_a_receipt_is_resumed_with_backoff_and_no_cap() {
    let fixture = fixture(|cfg| cfg.delegation.max_concurrent = 2);
    let shared = &fixture.shared;
    admit(shared, work("work-1"));
    admit(shared, work("asks-parent"));
    pin_parent(shared);
    let (identity, _, _, _) = planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    planner_workflow::attempt_ended(
        shared,
        SID,
        &identity,
        None,
        "stopped to wait".into(),
        false,
    )
    .unwrap();
    // A handoff that asks the parent for something is the parent's to answer.
    let (asks, _, _, _) = planner_workflow::begin_work(shared, SID, "asks-parent")
        .await
        .unwrap();
    planner_workflow::record_handoff(
        shared,
        SID,
        &asks,
        router_acp::delegate_hook::Handoff {
            kind: "lease_requested".into(),
            message: "need the unity lease".into(),
        },
    )
    .unwrap();
    planner_workflow::attempt_ended(shared, SID, &asks, None, "handed off".into(), false).unwrap();

    let resumes = |id: &str| {
        planner_workflow::load(shared, SID)
            .unwrap()
            .unwrap()
            .capacity
            .resumes
            .get(id)
            .cloned()
    };
    let mut now = chrono::Utc::now().timestamp() + 10_000;
    let mut previous_attempt = identity.attempt_id.clone();
    for (round, delay) in [30, 120, 300, 300, 300].into_iter().enumerate() {
        tick(shared, now).await;
        let state = resumes("work-1").unwrap();
        assert_eq!(state.count, round as u32);
        assert_eq!(state.due_at, Some(now + delay), "backoff step {round}");
        tick(shared, now + delay - 1).await;
        assert_eq!(
            resumes("work-1").unwrap().count,
            round as u32,
            "not before its backoff"
        );
        tick(shared, now + delay).await;
        assert_eq!(resumes("work-1").unwrap().count, round as u32 + 1);
        // The resumption is a fresh attempt on the same work and workspace.
        attempt_settled(shared, "work-1").await;
        let run = planner_workflow::load(shared, SID).unwrap().unwrap();
        let attempt = run.works["work-1"].attempt.as_ref().unwrap().id.clone();
        assert_ne!(attempt, previous_attempt);
        previous_attempt = attempt;
        now += delay + 1;
    }
    assert!(resumes("asks-parent").is_none());
    assert_eq!(
        planner_workflow::load(shared, SID).unwrap().unwrap().works["asks-parent"]
            .attempt
            .as_ref()
            .unwrap()
            .id,
        asks.attempt_id
    );
}

#[tokio::test]
async fn an_idle_parent_with_a_release_in_flight_is_woken() {
    let fixture = fixture(|_| {});
    let shared = &fixture.shared;
    let mut release = work("release-1");
    release.plan_id = "release".into();
    admit(shared, release);
    planner_workflow::begin_work(shared, SID, "release-1")
        .await
        .unwrap();
    let last = parent_turn_ended(shared);
    tick(shared, last + 59).await;
    assert!(wakes_with(shared, "inflight:").is_empty());
    tick(shared, last + 61).await;
    let wakes = wakes_with(shared, "inflight:");
    assert_eq!(wakes.len(), 1);
    assert!(
        wakes[0].1.reason.contains("release-1"),
        "{}",
        wakes[0].1.reason
    );
    assert!(wakes[0].1.reason.contains("smokescreen"));
    deliver_all(shared);
    tick(shared, last + 200).await;
    assert_eq!(
        wakes_with(shared, "inflight:").len(),
        1,
        "spaced by inflight_wake_secs"
    );
    tick(shared, last + 361).await;
    assert_eq!(wakes_with(shared, "inflight:").len(), 2);
}

#[tokio::test]
async fn a_workspace_refusal_during_auto_admission_becomes_a_recorded_hold() {
    let tmp = tempfile::tempdir().unwrap();
    let script = host_script(&["a"], &tmp.path().join("stdin.json"));
    let fixture = fixture(|cfg| {
        cfg.delegation.max_concurrent = 2;
        capacity_host(cfg, script, 1_000, 300);
        cfg.routers.planner.workspace = Some(PlannerWorkspaceConfig {
            command: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "cat >/dev/null; echo 'disk floor: 1048576 bytes free' >&2; exit 1".into(),
            ],
            timeout_ms: 5_000,
        });
    });
    let shared = &fixture.shared;
    authorize(shared);
    pin_parent(shared);
    let last = parent_turn_ended(shared);
    tick(shared, last + 301).await;
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    let hold = &run.capacity.holds["plan-a"];
    assert_eq!(hold.reason, "disk-floor");
    assert!(
        hold.detail.contains("1048576 bytes free"),
        "{}",
        hold.detail
    );
    let notice = &run.wakes["auto-admit:1"].reason;
    assert!(
        notice.contains("Not started (recorded as holds): work-a (disk-floor"),
        "{notice}"
    );
    // The held candidate is no longer counted as unfilled work.
    let now_ms = (last + 302) * 1000;
    assert!(router_acp::planner_capacity::unheld_candidates(&run, now_ms).is_empty());
    assert!(
        !router_acp::planner_capacity::unheld_candidates(&run, now_ms + 31 * 60 * 1000).is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_follow_up_resumption_falls_back_to_a_fresh_attempt() {
    let fixture = fixture(|cfg| cfg.delegation.max_concurrent = 2);
    let shared = &fixture.shared;
    admit(shared, work("work-1"));
    pin_parent(shared);
    let (identity, _, _, _) = planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    shared.live_delegates.lock().unwrap().insert(
        "d-1".into(),
        router_acp::session::LiveDelegate {
            parent_sid: SID.into(),
            process_key: router_acp::downstream::ProcessKey("key-1".into()),
            downstream_sid: "down-1".into(),
            candidate: router_acp::candidate::CandidateId::parse("test/model").unwrap(),
            capture: Arc::default(),
            sub_sid: "sub-1".into(),
            lifecycle: None,
            worker_id: "w-1".into(),
            planner: Some(identity.clone()),
            turns: 1,
        },
    );
    planner_workflow::attempt_ended(
        shared,
        SID,
        &identity,
        Some("d-1".into()),
        "stopped".into(),
        false,
    )
    .unwrap();
    let now = chrono::Utc::now().timestamp() + 10_000;
    tick(shared, now).await;
    tick(shared, now + 30).await;
    let state = planner_workflow::load(shared, SID)
        .unwrap()
        .unwrap()
        .capacity
        .resumes["work-1"]
        .clone();
    assert_eq!(
        state.followup,
        Some(("d-1".to_string(), 1)),
        "first resumption follows up"
    );
    attempt_settled(shared, "work-1").await;
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    assert_eq!(
        run.works["work-1"].attempt.as_ref().unwrap().id,
        identity.attempt_id
    );

    // That follow-up completed no turn, so the next resumption is a fresh attempt.
    tick(shared, now + 31).await;
    tick(shared, now + 31 + 120).await;
    attempt_settled(shared, "work-1").await;
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    assert_ne!(
        run.works["work-1"].attempt.as_ref().unwrap().id,
        identity.attempt_id
    );
    assert_eq!(run.capacity.resumes["work-1"].count, 2);
}

// ---------------------------------------------------------------------------
// The native implement-all watcher's guarantees (`planner_guard`) and the
// `/shutdown` drain (`planner_shutdown`)
// ---------------------------------------------------------------------------

fn audit_host(cfg: &mut Config, script: String) {
    cfg.routers.planner.audit = Some(PlannerAuditConfig {
        command: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        timeout_ms: 5_000,
        interval_secs: 60,
    });
}

/// A fake audit host: appends each request to `log` (one per line) and
/// answers whatever `reply` holds at that moment.
fn audit_script(reply: &Path, log: &Path) -> String {
    format!(
        "cat >> '{log}'; echo >> '{log}'; cat '{reply}'",
        log = log.display(),
        reply = reply.display()
    )
}

fn host_calls(log: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn defects(codes: &[&str]) -> String {
    json!({"defects": codes
        .iter()
        .map(|code| json!({"code": code, "detail": format!("{code} detail")}))
        .collect::<Vec<_>>()})
    .to_string()
}

async fn status_json(shared: &Arc<Shared>) -> serde_json::Value {
    serde_json::from_str(
        &planner_workflow::operate(shared, SID, None, Operation::Status { offset: 0 })
            .await
            .unwrap(),
    )
    .unwrap()
}

fn set_status(shared: &Arc<Shared>, status: RunStatus) {
    planner_workflow::mutate(shared, SID, |run| {
        run.status = status;
        Ok(())
    })
    .unwrap();
}

#[tokio::test]
async fn audit_defects_wake_the_idle_parent_whatever_the_free_slots_and_never_give_up() {
    let tmp = tempfile::tempdir().unwrap();
    let (reply, log) = (
        tmp.path().join("reply.json"),
        tmp.path().join("audit.jsonl"),
    );
    std::fs::write(&reply, defects(&["STAGING_IDLE"])).unwrap();
    let script = audit_script(&reply, &log);
    let fixture = fixture(|cfg| {
        cfg.delegation.max_concurrent = 1;
        audit_host(cfg, script);
    });
    let shared = &fixture.shared;
    admit(shared, work("work-1"));
    planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    let last = parent_turn_ended(shared);

    tick(shared, last + 10).await;
    let calls = host_calls(&log);
    assert_eq!(calls.len(), 1, "audited with every slot full");
    assert_eq!(calls[0]["event"], "audit");
    assert_eq!(calls[0]["completing"], false);
    assert_eq!(calls[0]["run_status"], "running");
    assert_eq!(calls[0]["free"], 0);
    assert_eq!(calls[0]["max"], 1);
    assert_eq!(calls[0]["works"][0]["work_id"], "work-1");
    assert_eq!(calls[0]["works"][0]["status"], "running");
    assert!(calls[0].get("hold_check").is_none());
    assert!(
        wakes_with(shared, "audit:").is_empty(),
        "not idle long enough"
    );

    tick(shared, last + 40).await;
    assert_eq!(host_calls(&log).len(), 1, "at most every interval_secs");
    tick(shared, last + 61).await;
    let wakes = wakes_with(shared, "audit:");
    assert_eq!(wakes.len(), 1);
    assert!(
        wakes[0]
            .1
            .reason
            .contains("STAGING_IDLE: STAGING_IDLE detail"),
        "{}",
        wakes[0].1.reason
    );
    tick(shared, last + 71).await;
    assert_eq!(host_calls(&log).len(), 2);
    assert_eq!(wakes_with(shared, "audit:").len(), 1, "not doubled");

    let mut now = last + 71;
    for delivered in 1..=6 {
        deliver_all(shared);
        now += 61;
        tick(shared, now).await;
        assert_eq!(
            wakes_with(shared, "audit:").len(),
            delivered + 1,
            "no give-up"
        );
    }
    let status = status_json(shared).await;
    assert_eq!(status["capacity"]["audit"]["ok"], true);
    assert_eq!(
        status["capacity"]["audit"]["defects"][0]["code"],
        "STAGING_IDLE"
    );
    assert_eq!(
        status["capacity"]["audit_findings"],
        json!(["STAGING_IDLE: STAGING_IDLE detail"])
    );
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    let census = router_acp::planner_wake::capacity_line(shared, &run);
    assert!(census.contains("Audit defects: STAGING_IDLE."), "{census}");

    // A clean audit ends the wakes.
    std::fs::write(&reply, defects(&[])).unwrap();
    deliver_all(shared);
    now += 61;
    tick(shared, now).await;
    let settled = wakes_with(shared, "audit:").len();
    now += 61;
    tick(shared, now).await;
    assert_eq!(wakes_with(shared, "audit:").len(), settled);
}

#[tokio::test]
async fn an_unavailable_audit_host_is_never_a_clean_audit() {
    for (script, expected) in [
        ("cat >/dev/null; echo crashed >&2; exit 4", "crashed"),
        ("cat >/dev/null; printf nope", "invalid JSON"),
    ] {
        let fixture = fixture(|cfg| audit_host(cfg, script.into()));
        let shared = &fixture.shared;
        authorize(shared);
        let last = parent_turn_ended(shared);
        tick(shared, last + 61).await;
        let wakes = wakes_with(shared, "audit:");
        assert_eq!(wakes.len(), 1);
        assert!(
            wakes[0].1.reason.contains("UNAVAILABLE") && wakes[0].1.reason.contains(expected),
            "{}",
            wakes[0].1.reason
        );
        let run = planner_workflow::load(shared, SID).unwrap().unwrap();
        assert!(!run.capacity.audit.as_ref().unwrap().ok);
        assert!(router_acp::planner_guard::audit_open(&run));
        deliver_all(shared);
        let error = planner_workflow::operate(
            shared,
            SID,
            None,
            Operation::Complete {
                key: "complete".into(),
                queue_evidence: "queue-census.md".into(),
            },
        )
        .await
        .unwrap_err();
        assert!(
            error.contains("audit host is UNAVAILABLE") && error.contains(expected),
            "{error}"
        );
    }
}

#[tokio::test]
async fn a_parent_turn_end_with_audit_defects_is_re_prompted_at_once_then_on_backoff_without_limit()
{
    let tmp = tempfile::tempdir().unwrap();
    let (reply, log) = (
        tmp.path().join("reply.json"),
        tmp.path().join("audit.jsonl"),
    );
    std::fs::write(&reply, defects(&["PROD_SMOKE_OVERDUE"])).unwrap();
    let script = audit_script(&reply, &log);
    let fixture = fixture(|cfg| audit_host(cfg, script));
    let shared = &fixture.shared;
    authorize(shared);
    let last = parent_turn_ended(shared);
    let end = |at: i64| router_acp::planner_guard::parent_turn_ended(shared, SID, at);

    let mut at = last + 1;
    end(at);
    tick(shared, at).await;
    let gates = wakes_with(shared, "gate:");
    assert_eq!(gates.len(), 1, "re-prompted at once");
    let reason = &gates[0].1.reason;
    assert!(
        reason.contains("PROD_SMOKE_OVERDUE: PROD_SMOKE_OVERDUE detail"),
        "{reason}"
    );
    assert!(reason.contains("re-prompt 1"), "{reason}");
    assert!(reason.contains("Do not end your turn"), "{reason}");
    tick(shared, at + 1).await;
    assert_eq!(
        wakes_with(shared, "gate:").len(),
        1,
        "one turn end, one re-prompt"
    );

    // Each further turn end on the same defects: 30 s, 2 min, then every
    // 5 min, with no limit.
    for (n, delay) in [30, 120, 300, 300, 300].into_iter().enumerate() {
        deliver_all(shared);
        at += 400;
        end(at);
        tick(shared, at + delay - 1).await;
        assert_eq!(
            wakes_with(shared, "gate:").len(),
            n + 1,
            "not before its {delay} s backoff"
        );
        tick(shared, at + delay).await;
        assert_eq!(wakes_with(shared, "gate:").len(), n + 2, "after {delay} s");
    }
    assert!(
        host_calls(&log).len() >= 6,
        "every turn end was audited afresh"
    );

    // A turn end the audit finds clean releases the gate and resets it.
    std::fs::write(&reply, defects(&[])).unwrap();
    deliver_all(shared);
    let before = wakes_with(shared, "gate:").len();
    at += 400;
    end(at);
    tick(shared, at).await;
    tick(shared, at + 600).await;
    assert_eq!(wakes_with(shared, "gate:").len(), before);
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    assert_eq!(run.capacity.gate.count, 0);
}

#[tokio::test]
async fn a_parent_turn_end_with_a_free_slot_and_an_unheld_candidate_is_re_prompted() {
    let tmp = tempfile::tempdir().unwrap();
    let script = host_script(&["a"], &tmp.path().join("stdin.json"));
    let fixture = fixture(|cfg| {
        cfg.delegation.max_concurrent = 2;
        capacity_host(cfg, script, 1_000, 1_000_000);
    });
    let shared = &fixture.shared;
    authorize(shared);
    let last = parent_turn_ended(shared);
    let end = |at: i64| router_acp::planner_guard::parent_turn_ended(shared, SID, at);
    end(last + 1);
    tick(shared, last + 1).await;
    let gates = wakes_with(shared, "gate:");
    assert_eq!(gates.len(), 1);
    let reason = &gates[0].1.reason;
    assert!(reason.contains("2 free"), "{reason}");
    assert!(
        reason.contains("1. work_id work-a (external_id EXT-a"),
        "{reason}"
    );
    assert!(reason.contains("`hold`"), "{reason}");

    // A held candidate no longer gates the turn end.
    planner_workflow::operate(
        shared,
        SID,
        None,
        Operation::Hold {
            key: "hold-a".into(),
            plan_id: "plan-a".into(),
            reason: "dependency".into(),
            detail: "plan-z must land first".into(),
        },
    )
    .await
    .unwrap();
    deliver_all(shared);
    end(last + 100);
    tick(shared, last + 100).await;
    tick(shared, last + 500).await;
    assert_eq!(wakes_with(shared, "gate:").len(), 1);
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    assert_eq!(run.capacity.gate.count, 0);
}

#[tokio::test]
async fn planner_wakes_are_never_dropped_after_any_number_of_deliveries() {
    use router_acp::planner_guard::wake_retry_secs;
    assert_eq!(
        [1, 2, 3, 4, 40].map(wake_retry_secs),
        [30, 120, 300, 300, 300]
    );
    let fixture = fixture(|_| {});
    let shared = &fixture.shared;
    authorize(shared);
    planner_workflow::mutate(shared, SID, |run| {
        run.wakes.insert(
            "work-9:attempt".into(),
            planner_workflow::Wake {
                reason: "child state: Interrupted".into(),
                work_id: "work-9".into(),
                deliveries: 9,
                ..Default::default()
            },
        );
        Ok(())
    })
    .unwrap();
    let last = parent_turn_ended(shared);
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    router_acp::planner_wake::heartbeat(shared, SID, &run, last + 21 * 60);
    assert!(
        wakes_with(shared, "heartbeat:").is_empty(),
        "a wake delivered nine times is still owed, so nothing stands in for it"
    );
}

#[tokio::test]
async fn full_worker_slots_do_not_silence_the_heartbeat_while_the_audit_has_defects() {
    let tmp = tempfile::tempdir().unwrap();
    let (reply, log) = (
        tmp.path().join("reply.json"),
        tmp.path().join("audit.jsonl"),
    );
    std::fs::write(&reply, defects(&["DISK_FLOOR"])).unwrap();
    let script = audit_script(&reply, &log);
    let fixture = fixture(|cfg| {
        cfg.delegation.max_concurrent = 1;
        audit_host(cfg, script);
    });
    let shared = &fixture.shared;
    admit(shared, work("work-1"));
    planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    let last = parent_turn_ended(shared);
    let acknowledge_all = || {
        planner_workflow::mutate(shared, SID, |run| {
            run.wakes.values_mut().for_each(|w| w.acknowledged = true);
            Ok(())
        })
        .unwrap();
    };
    tick(shared, last + 10).await;
    acknowledge_all();
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    router_acp::planner_wake::heartbeat(shared, SID, &run, last + 21 * 60);
    assert_eq!(wakes_with(shared, "heartbeat:").len(), 1);

    std::fs::write(&reply, defects(&[])).unwrap();
    tick(shared, last + 80).await;
    acknowledge_all();
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    router_acp::planner_wake::heartbeat(shared, SID, &run, last + 50 * 60);
    assert_eq!(
        wakes_with(shared, "heartbeat:").len(),
        1,
        "a clean audit lets full slots excuse it again"
    );
}

#[tokio::test]
async fn a_planner_stop_refused_for_unpushed_source_interrupts_the_work_and_is_accepted() {
    let script = "e=$(cat); case \"$e\" in *'\"event\":\"delegate_stop\"'*) \
                  echo 'ROUTER_WORKER_STOPPED_UNPUSHED work-1: has uncommitted files' >&2; \
                  exit 2;; esac; exit 0";
    let fixture = fixture(|cfg| {
        cfg.delegation.lifecycle_hook = Some(DelegateLifecycleHook {
            command: "/bin/sh".into(),
            args: vec!["-c".into(), script.into()],
            timeout_ms: 5_000,
            max_continuations: 3,
        });
    });
    let shared = &fixture.shared;
    admit(shared, work("work-1"));
    let (identity, workspace, child, _) = planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    planner_workflow::record_handoff(
        shared,
        SID,
        &identity,
        router_acp::delegate_hook::Handoff {
            kind: "waiting".into(),
            message: "CI build 12".into(),
        },
    )
    .unwrap();
    planner_workflow::attempt_ended(shared, SID, &identity, None, "waiting".into(), false).unwrap();
    let stop = |worker: &str| router_acp::delegate_hook::DelegateEvent {
        event: "delegate_stop",
        worker_id: worker.into(),
        parent_router_session_id: SID.into(),
        parent_downstream_session_id: "downstream".into(),
        parent_candidate: "test/model".into(),
        candidate: "test/model".into(),
        lineage: "test".into(),
        downstream_session_id: "child-downstream".into(),
        state_session_id: format!("{SID}::delegate-child"),
        cwd: workspace.path.display().to_string(),
        effort: None,
        background: true,
        keep_open: true,
        task_summary: "work-1".into(),
        router_pid: std::process::id(),
        router_started_at_ms: 1,
        turn: None,
        last_message: None,
        handoff: None,
        outcome: Some("closed".into()),
        detail: None,
    };
    let stops = || {
        shared
            .state
            .lock()
            .unwrap()
            .outbox_pending(100)
            .into_iter()
            .filter(|(_, payload)| payload.contains("\"delegate_stop\""))
            .count()
    };
    router_acp::delegate_hook::deliver_now(shared, &stop(&child)).await;
    assert_eq!(stops(), 0, "accepted, not redelivered forever");
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    let work = &run.works["work-1"];
    assert_eq!(work.status, WorkStatus::Interrupted);
    let reason = work.reason.as_deref().unwrap();
    assert!(
        reason.contains("dirty or unpushed source") && reason.contains("has uncommitted files"),
        "{reason}"
    );
    assert!(
        work.handoff.is_none(),
        "the owed source outranks the `waiting` handoff"
    );

    // Worker resumption brings it back to commit and push.
    tick(shared, chrono::Utc::now().timestamp() + 10_000).await;
    let run = planner_workflow::load(shared, SID).unwrap().unwrap();
    assert!(run.capacity.resumes["work-1"].due_at.is_some());

    // Any other delegate's refused stop stays queued for redelivery.
    router_acp::delegate_hook::deliver_now(shared, &stop("w-ordinary")).await;
    assert_eq!(stops(), 1);
}

#[tokio::test]
async fn a_planner_child_may_keep_continuing_until_its_run_or_attempt_stops() {
    assert_eq!(
        router_acp::planner_guard::CHILD_CONTINUE_BACKOFF_SECS,
        [0, 30, 120, 300]
    );
    let fixture = fixture(|_| {});
    let shared = &fixture.shared;
    admit(shared, work("work-1"));
    let (identity, _, _, _) = planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    let may = || router_acp::planner_guard::child_may_continue(shared, SID, &identity);
    assert!(may());
    set_status(shared, RunStatus::Draining);
    assert!(may(), "a drain lets a worker finish committing and pushing");
    set_status(shared, RunStatus::Paused);
    assert!(!may());
    set_status(shared, RunStatus::Running);
    planner_workflow::attempt_ended(shared, SID, &identity, None, "done".into(), false).unwrap();
    assert!(!may());
}

#[tokio::test]
async fn completion_requires_a_clean_completing_audit() {
    let tmp = tempfile::tempdir().unwrap();
    let (reply, log) = (
        tmp.path().join("reply.json"),
        tmp.path().join("audit.jsonl"),
    );
    std::fs::write(&reply, defects(&["CONTINUE_NOT_FINISHED"])).unwrap();
    let script = audit_script(&reply, &log);
    let fixture = fixture(|cfg| audit_host(cfg, script));
    let shared = &fixture.shared;
    authorize(shared);
    let complete = |key: &'static str| {
        planner_workflow::operate(
            shared,
            SID,
            None,
            Operation::Complete {
                key: key.into(),
                queue_evidence: "queue-census.md".into(),
            },
        )
    };
    let error = complete("complete-1").await.unwrap_err();
    assert!(
        error.contains("CONTINUE_NOT_FINISHED: CONTINUE_NOT_FINISHED detail"),
        "{error}"
    );
    let calls = host_calls(&log);
    assert_eq!(calls.last().unwrap()["completing"], true);
    std::fs::write(&reply, defects(&[])).unwrap();
    complete("complete-2").await.unwrap();
    assert_eq!(
        planner_workflow::load(shared, SID).unwrap().unwrap().status,
        RunStatus::Complete
    );
}

/// Take `work-1` to `finished` on its current workspace revision.
async fn finished_work(shared: &Arc<Shared>) -> String {
    admit(shared, work("work-1"));
    let (identity, workspace, _, _) = planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    let revision = current_revision(&workspace.path);
    planner_workflow::operate(
        shared,
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
        shared,
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
        shared,
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
    planner_workflow::attempt_ended(shared, SID, &identity, None, "done".into(), false).unwrap();
    revision
}

#[tokio::test]
async fn integration_and_blocked_dispositions_pass_the_integrate_check_host() {
    let tmp = tempfile::tempdir().unwrap();
    let (verdict, stdin) = (tmp.path().join("verdict"), tmp.path().join("stdin.json"));
    let script = format!(
        "cat > '{stdin}'; v=$(cat '{verdict}'); \
         if [ \"$v\" = ok ]; then exit 0; fi; \
         if [ \"$v\" = reject ]; then echo 'a plan integrates with disposition evidence naming its guide outcome' >&2; exit 2; fi; \
         echo host-crashed >&2; exit 1",
        stdin = stdin.display(),
        verdict = verdict.display()
    );
    let fixture = fixture(|cfg| {
        cfg.routers.planner.integrate_check = Some(PlannerHostConfig {
            command: "/bin/sh".into(),
            args: vec!["-c".into(), script],
            timeout_ms: 5_000,
        });
    });
    let shared = &fixture.shared;
    let revision = finished_work(shared).await;
    let integrate = || {
        planner_workflow::operate(
            shared,
            SID,
            None,
            Operation::Integrate {
                key: "integrate".into(),
                work_id: "work-1".into(),
                receipt: receipt(&revision, "merge"),
            },
        )
    };
    let asked = || -> serde_json::Value {
        serde_json::from_str(&std::fs::read_to_string(&stdin).unwrap()).unwrap()
    };

    std::fs::write(&verdict, "reject").unwrap();
    let error = integrate().await.unwrap_err();
    assert!(
        error.contains("rejected by the integrate-check host")
            && error.contains("naming its guide outcome"),
        "{error}"
    );
    assert_eq!(asked()["action"], "integrate");
    assert_eq!(asked()["work_id"], "work-1");
    assert_eq!(asked()["receipt"]["revision"], revision.as_str());
    assert_eq!(asked()["receipt"]["evidence"]["merge"], "verified-log.txt");
    let status = |id: &str| planner_workflow::load(shared, SID).unwrap().unwrap().works[id].status;
    assert_eq!(status("work-1"), WorkStatus::Finished);

    std::fs::write(&verdict, "crash").unwrap();
    let error = integrate().await.unwrap_err();
    assert!(
        error.contains("unavailable")
            && error.contains("retryable")
            && error.contains("host-crashed"),
        "{error}"
    );
    assert_eq!(status("work-1"), WorkStatus::Finished);

    std::fs::write(&verdict, "ok").unwrap();
    integrate().await.unwrap();
    assert_eq!(status("work-1"), WorkStatus::Integrated);

    admit(shared, work("work-2"));
    let dispose = || {
        planner_workflow::operate(
            shared,
            SID,
            None,
            Operation::Disposition {
                key: "dispose".into(),
                work_id: "work-2".into(),
                status: WorkStatus::Blocked,
                reason: "waits on Docs/BLOCKED.md#gated-key".into(),
            },
        )
    };
    std::fs::write(&verdict, "reject").unwrap();
    let error = dispose().await.unwrap_err();
    assert!(error.contains("disposition rejected"), "{error}");
    assert_eq!(asked()["action"], "disposition");
    assert_eq!(asked()["status"], "blocked");
    assert_eq!(asked()["reason"], "waits on Docs/BLOCKED.md#gated-key");
    assert_eq!(status("work-2"), WorkStatus::Admitted);
    std::fs::write(&verdict, "ok").unwrap();
    dispose().await.unwrap();
    assert_eq!(status("work-2"), WorkStatus::Blocked);
}

#[tokio::test]
async fn a_hold_is_verified_by_the_audit_hosts_hold_check() {
    let tmp = tempfile::tempdir().unwrap();
    let (reply, log) = (
        tmp.path().join("reply.json"),
        tmp.path().join("audit.jsonl"),
    );
    let script = audit_script(&reply, &log);
    let fixture = fixture(|cfg| audit_host(cfg, script));
    let shared = &fixture.shared;
    authorize(shared);
    let hold = |key: &'static str, reason: &'static str| {
        planner_workflow::operate(
            shared,
            SID,
            None,
            Operation::Hold {
                key: key.into(),
                plan_id: "plan-a".into(),
                reason: reason.into(),
                detail: "plan-z must land first".into(),
            },
        )
    };
    let holds = || {
        planner_workflow::load(shared, SID)
            .unwrap()
            .unwrap()
            .capacity
            .holds
    };

    std::fs::write(
        &reply,
        json!({"defects":[],"hold_valid":false,"reason":"plan-z is already integrated"})
            .to_string(),
    )
    .unwrap();
    let error = hold("hold-1", "dependency").await.unwrap_err();
    assert!(
        error.contains("hold refused") && error.contains("plan-z is already integrated"),
        "{error}"
    );
    let check = host_calls(&log).last().unwrap()["hold_check"].clone();
    assert_eq!(check["plan_id"], "plan-a");
    assert_eq!(check["reason"], "dependency");
    assert_eq!(check["detail"], "plan-z must land first");
    assert!(holds().is_empty());

    // The router's own rules still refuse an invalid reason without asking.
    let asked = host_calls(&log).len();
    let error = hold("hold-2", "waiting-for-review").await.unwrap_err();
    assert!(error.contains("not allowed"), "{error}");
    assert_eq!(host_calls(&log).len(), asked);

    // A host that cannot answer refuses the hold, retryably.
    std::fs::write(&reply, "nope").unwrap();
    let error = hold("hold-3", "dependency").await.unwrap_err();
    assert!(
        error.contains("UNAVAILABLE") && error.contains("retryable"),
        "{error}"
    );
    assert!(holds().is_empty());

    std::fs::write(&reply, json!({"defects":[],"hold_valid":true}).to_string()).unwrap();
    hold("hold-4", "dependency").await.unwrap();
    assert_eq!(holds()["plan-a"].reason, "dependency");
}

#[tokio::test]
async fn the_first_allocation_takes_over_an_active_native_run_and_reports_it() {
    let tmp = tempfile::tempdir().unwrap();
    let (log, busy) = (tmp.path().join("adopt.jsonl"), tmp.path().join("busy-once"));
    let fixture_tmp = tempfile::tempdir().unwrap();
    let repo = repository(&fixture_tmp);
    let adopted = json!({"adopted":{"run":"native-7","session_id":"codex-1","runtime":"codex",
        "cancelled":true,"workers":[{"agent_id":"w1","worktree":"/repo/.scratch/plan-a"}]}});
    let script = format!(
        "e=$(cat); case \"$e\" in *adopt-native*) printf '%s\\n' \"$e\" >> '{log}'; \
         if [ ! -e '{busy}' ]; then touch '{busy}'; echo 'RAM guard busy' >&2; exit 1; fi; \
         printf '%s' '{adopted}'; exit 0;; esac; \
         child=$(printf '%s' \"$e\" | sed -n 's/.*\"child_id\":\"\\([^\"]*\\)\".*/\\1/p'); \
         d='{root}'/ws-$child; [ -d \"$d\" ] || git clone --quiet '{repo}' \"$d\" >/dev/null 2>&1; \
         printf '{{\"path\":\"%s\",\"lease\":\"lease-%s\"}}' \"$d\" \"$child\"",
        log = log.display(),
        busy = busy.display(),
        root = tmp.path().display(),
        repo = repo.display(),
    );
    let state_path = fixture_tmp.path().join("state.db");
    let shared = shared_for(&repo, &state_path, |cfg| {
        cfg.routers.planner.workspace = Some(PlannerWorkspaceConfig {
            command: "/bin/sh".into(),
            args: vec!["-c".into(), script],
            timeout_ms: 10_000,
        });
    });
    admit(&shared, work("work-1"));
    admit(&shared, work("work-2"));
    planner_workflow::begin_work(&shared, SID, "work-1")
        .await
        .unwrap();
    let calls = host_calls(&log);
    assert_eq!(calls.len(), 2, "retried after the RAM guard's refusal");
    assert!(calls.iter().all(|c| c["event"] == "adopt-native"));
    let wakes = wakes_with(&shared, "adopt-native:");
    assert_eq!(wakes.len(), 1);
    let reason = &wakes[0].1.reason;
    assert!(
        reason.contains("native-7") && reason.contains("w1") && reason.contains("Reconcile"),
        "{reason}"
    );
    let status = status_json(&shared).await;
    assert_eq!(status["capacity"]["native_adoption"]["ok"], true);
    assert_eq!(
        status["capacity"]["native_adoption"]["adopted"]["run"],
        "native-7"
    );

    planner_workflow::begin_work(&shared, SID, "work-2")
        .await
        .unwrap();
    assert_eq!(host_calls(&log).len(), 2, "asked once per run");
}

/// Wait until the run reaches `status`.
async fn run_reaches(shared: &Arc<Shared>, status: RunStatus) {
    for _ in 0..400 {
        if planner_workflow::load(shared, SID).unwrap().unwrap().status == status {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the run never reached {status:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_saves_the_resume_state_stops_every_worker_and_pauses_the_run() {
    let tmp = tempfile::tempdir().unwrap();
    let stdin = tmp.path().join("shutdown-stdin.json");
    let answer = json!({"saved":["Docs/Plans/data/implementation/resume.md",".scratch/router-host/shutdown/x.json"],
        "summary":"3 works, 1 with unpushed or uncommitted source"});
    let script = format!("cat > '{}'; printf '%s' '{answer}'", stdin.display());
    let fixture = fixture(|cfg| {
        cfg.delegation.max_concurrent = 3;
        cfg.routers.planner.shutdown = Some(PlannerShutdownConfig {
            command: Some("/bin/sh".into()),
            args: vec!["-c".into(), script],
            timeout_ms: 5_000,
            drain_timeout_secs: 2,
        });
    });
    let shared = &fixture.shared;
    for id in ["work-1", "work-2", "work-3"] {
        admit(shared, work(id));
    }
    // work-1 is mid-turn and never ends it: cancelled at its drain bound.
    planner_workflow::begin_work(shared, SID, "work-1")
        .await
        .unwrap();
    // work-2 is idle on an open sub-session: it gets the shutdown follow-up
    // (which fails at once here: its agent process is gone).
    let (second, _, _, _) = planner_workflow::begin_work(shared, SID, "work-2")
        .await
        .unwrap();
    shared.live_delegates.lock().unwrap().insert(
        "d-2".into(),
        router_acp::session::LiveDelegate {
            parent_sid: SID.into(),
            process_key: router_acp::downstream::ProcessKey("gone".into()),
            downstream_sid: "down-2".into(),
            candidate: router_acp::candidate::CandidateId::parse("test/model").unwrap(),
            capture: Arc::default(),
            sub_sid: "sub-2".into(),
            lifecycle: None,
            worker_id: "w-2".into(),
            planner: Some(second.clone()),
            turns: 1,
        },
    );
    planner_workflow::attempt_ended(
        shared,
        SID,
        &second,
        Some("d-2".into()),
        "waiting".into(),
        false,
    )
    .unwrap();
    // work-3 was admitted and never started: no live worker.

    use router_acp::planner_shutdown;
    assert!(planner_shutdown::request(shared, &json!({"sessionId":"no-such-session"})).is_err());
    let reply = planner_shutdown::request(shared, &json!({"sessionId": SID})).unwrap();
    assert_eq!(reply, json!({"accepted":true}));
    run_reaches(shared, RunStatus::Draining).await;
    assert_eq!(
        planner_shutdown::request(shared, &json!({"sessionId": SID})).unwrap(),
        json!({"accepted":true}),
        "a second request joins the drain"
    );
    let error = planner_workflow::operate(
        shared,
        SID,
        None,
        Operation::Admit {
            key: "admit-4".into(),
            work: work("work-4"),
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("run is shutting down"), "{error}");
    // Capacity, audit and resumption are all off while draining.
    tick(shared, chrono::Utc::now().timestamp() + 10_000).await;
    assert!(
        planner_workflow::load(shared, SID)
            .unwrap()
            .unwrap()
            .capacity
            .resumes
            .is_empty()
    );
    // A `/implement` cannot reopen a run mid-drain.
    let mut implement = input_request(Some("mid-drain"), "/implement continue", false);
    assert!(
        planner_workflow::user_command(shared, SID, &mut implement)
            .unwrap_err()
            .contains("shutting down")
    );

    run_reaches(shared, RunStatus::Paused).await;
    let progress = planner_shutdown::progress(shared, SID);
    for update in &progress {
        for key in [
            "sessionId",
            "phase",
            "message",
            "saved",
            "workersTotal",
            "workersStopped",
            "workers",
        ] {
            assert!(update.get(key).is_some(), "{key} missing from {update}");
        }
        assert_eq!(update["sessionId"], SID);
    }
    let phases = progress
        .iter()
        .map(|p| p["phase"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(phases[..2], ["saving", "saved"], "{phases:?}");
    assert_eq!(*phases.last().unwrap(), "drained", "{phases:?}");
    assert!(phases[2..phases.len() - 1].iter().all(|p| *p == "draining"));
    assert_eq!(
        progress[1]["saved"],
        json!([
            "Docs/Plans/data/implementation/resume.md",
            ".scratch/router-host/shutdown/x.json"
        ])
    );
    assert_eq!(
        progress[1]["message"],
        "3 works, 1 with unpushed or uncommitted source"
    );
    assert!(
        progress[2]["message"]
            .as_str()
            .unwrap()
            .contains("not loaded"),
        "an unloaded parent is not prompted: {}",
        progress[2]
    );
    let done = progress.last().unwrap();
    assert_eq!(done["workersTotal"], 2);
    assert_eq!(done["workersStopped"], 2);
    assert_eq!(
        done["workers"],
        json!([{"workId":"work-1","status":"cancelled"},{"workId":"work-2","status":"stopped"}])
    );
    let asked: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&stdin).unwrap()).unwrap();
    assert_eq!(asked["event"], "shutdown");
    assert_eq!(asked["session_id"], SID);
    assert_eq!(asked["run_status"], "running");
    assert_eq!(asked["works"].as_array().unwrap().len(), 3);

    // The next `/implement` resumes the paused run.
    let mut resume = input_request(Some("after-drain"), "/implement continue", false);
    planner_workflow::user_command(shared, SID, &mut resume).unwrap();
    assert_eq!(
        planner_workflow::load(shared, SID).unwrap().unwrap().status,
        RunStatus::Running
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_resume_save_is_reported_and_the_drain_still_finishes() {
    let fixture = fixture(|cfg| {
        cfg.routers.planner.shutdown = Some(PlannerShutdownConfig {
            command: Some("/bin/sh".into()),
            args: vec![
                "-c".into(),
                "cat >/dev/null; echo 'disk full' >&2; exit 1".into(),
            ],
            timeout_ms: 5_000,
            drain_timeout_secs: 1,
        });
    });
    let shared = &fixture.shared;
    authorize(shared);
    router_acp::planner_shutdown::request(shared, &json!({"sessionId": SID})).unwrap();
    run_reaches(shared, RunStatus::Paused).await;
    let progress = router_acp::planner_shutdown::progress(shared, SID);
    assert_eq!(progress[1]["phase"], "failed");
    assert!(
        progress[1]["message"]
            .as_str()
            .unwrap()
            .contains("disk full")
    );
    assert_eq!(progress.last().unwrap()["phase"], "drained");
}

#[tokio::test]
async fn a_run_left_draining_by_a_dead_router_resumes_on_the_next_implement() {
    let fixture = fixture(|_| {});
    let shared = &fixture.shared;
    authorize(shared);
    set_status(shared, RunStatus::Draining);
    let mut request = input_request(Some("resume"), "/implement continue", false);
    planner_workflow::user_command(shared, SID, &mut request).unwrap();
    assert_eq!(
        planner_workflow::load(shared, SID).unwrap().unwrap().status,
        RunStatus::Running
    );
}
