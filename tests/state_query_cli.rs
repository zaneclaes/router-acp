//! Stable, read-only JSON state-query contract exercised through the binary.

use std::path::Path;

use router_acp::state::{LogEntry, PersistedSession, Retention, StateStore};
use serde_json::{Value, json};

fn write_config(path: &Path, state: &Path) {
    std::fs::write(
        path,
        format!(
            "state_file: {}\nhistory: 7d\nagents:\n  - name: mock\n    command:\n      type: stdio\n      command: mock-agent\n    model_selection: {{ type: config-option }}\n    models:\n      - {{ id: m1, cost_rank: 1 }}\n",
            state.display()
        ),
    )
    .unwrap();
}

fn session(parent: Option<&str>, kind: &str) -> PersistedSession {
    PersistedSession {
        agent: "mock".into(),
        model: "m1".into(),
        downstream_session_id: "downstream".into(),
        cwd: "/work".into(),
        parent_session_id: parent.map(str::to_string),
        kind: kind.into(),
        title: Some("A stable title".into()),
        delegation_directive_injections: (kind == "primary") as u64,
        ..Default::default()
    }
}

fn query(config: &Path, args: &[&str]) -> Value {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_router-acp"))
        .args(["state-query", "--config", config.to_str().unwrap(), "v1"])
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "query failed: {output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

fn assert_envelope(value: &Value, command: &str) {
    assert_eq!(value["version"], json!(1));
    assert_eq!(value["command"], command);
    assert!(value["data"].is_object());
}

#[test]
fn v1_state_queries_keep_the_schema_and_retention_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state.db");
    let config = dir.path().join("router.yaml");
    write_config(&config, &state);

    let store = StateStore::load(
        &state,
        Retention {
            max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60),
        },
    );
    store.upsert("parent".into(), session(None, "primary"));
    store.note_delegation_directive("parent");
    store.upsert("child".into(), session(Some("parent"), "delegate"));
    store.log(
        "parent",
        &LogEntry {
            kind: "user_prompt".into(),
            role: "user".into(),
            summary: "inspect state".into(),
            tokens_input: 3,
            ..Default::default()
        },
    );
    store.log(
        "parent",
        &LogEntry {
            kind: "agent_response".into(),
            role: "agent".into(),
            summary: "state inspected".into(),
            tokens_output: 5,
            ..Default::default()
        },
    );
    drop(store);

    let session = query(&config, &["session", "--session", "parent"]);
    assert_envelope(&session, "session");
    assert_eq!(session["data"]["session"]["router_session_id"], "parent");
    assert_eq!(session["data"]["session"]["title"], "A stable title");

    let title = query(&config, &["title", "--session", "parent"]);
    assert_envelope(&title, "title");
    assert_eq!(title["data"]["title"], "A stable title");

    let delegates = query(&config, &["delegates", "--session", "parent"]);
    assert_envelope(&delegates, "delegates");
    assert_eq!(
        delegates["data"]["children"][0]["router_session_id"],
        "child"
    );

    let logs = query(
        &config,
        &["logs", "--session", "parent", "--kind", "agent_response"],
    );
    assert_envelope(&logs, "logs");
    assert_eq!(logs["data"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(logs["data"]["entries"][0]["kind"], "agent_response");

    let analytics = query(&config, &["analytics", "--from", "0"]);
    assert_envelope(&analytics, "analytics");
    assert_eq!(analytics["data"]["log_entries"], 2);
    assert_eq!(analytics["data"]["tokens_input"], 3);
    assert_eq!(analytics["data"]["tokens_output"], 5);

    let health = query(&config, &["health"]);
    assert_envelope(&health, "health");
    assert_eq!(health["data"]["retention_seconds"], 7 * 24 * 60 * 60);
    assert_eq!(health["data"]["sessions"], 2);

    let report = query(&config, &["delegation-report"]);
    assert_envelope(&report, "delegation_report");
    assert_eq!(report["data"]["prompted_sessions"], 1);
    assert_eq!(report["data"]["sessions_that_delegated"], 1);

    let transcript = query(&config, &["transcript", "--session", "parent"]);
    assert_envelope(&transcript, "transcript");
    assert_eq!(transcript["data"]["entries"].as_array().unwrap().len(), 2);
}
