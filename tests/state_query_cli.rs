//! Stable, read-only JSON state-query contract exercised through the binary.
//! The fixtures mirror the relay's former direct SQLite consumers.

use std::path::Path;

use router_acp::state::{LogEntry, PersistedSession, Retention, StateStore};
use router_acp::state_layout::{StateLayout, tag_for_cwd};
use serde_json::{Value, json};

const DAY_START: i64 = 1_767_225_600; // 2026-01-01T00:00:00Z

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

fn session(parent: Option<&str>, kind: &str, title: &str, created_at: i64) -> PersistedSession {
    PersistedSession {
        agent: "mock".into(),
        model: "m1".into(),
        downstream_session_id: "downstream".into(),
        cwd: "/work".into(),
        parent_session_id: parent.map(str::to_string),
        kind: kind.into(),
        title: Some(title.into()),
        created_at: Some(created_at as u64),
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
fn v1_state_queries_keep_kory_consumer_shapes_and_retention_boundary() {
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
    let mut parent = session(None, "primary", "Parent title", DAY_START);
    parent.context_used = 13;
    store.upsert("parent".into(), parent);
    store.set_cost_usd("parent", 0.25);
    store.note_delegation_directive("parent");

    let mut child = session(Some("parent"), "delegate", "Child title", DAY_START + 60);
    child.context_used = 34;
    child.routing = Some(json!({"class":"Ops", "reason":"delegate state review"}));
    store.upsert("child".into(), child);
    store.upsert(
        "second-parent".into(),
        session(None, "primary", "Second parent", DAY_START + 120),
    );
    store.upsert(
        "out-of-range".into(),
        session(None, "other", "Out of range", DAY_START + 86_400),
    );

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
            detail: Some(json!({"text":"parent response"})),
            tokens_output: 5,
            ..Default::default()
        },
    );
    store.log(
        "child",
        &LogEntry {
            kind: "delegate_task".into(),
            role: "router".into(),
            summary: "inspect child state".into(),
            detail: Some(json!({"task":"inspect"})),
            tokens_input: 1,
            ..Default::default()
        },
    );
    store.log(
        "out-of-range",
        &LogEntry {
            kind: "user_prompt".into(),
            role: "user".into(),
            summary: "must be outside the exclusive range".into(),
            tokens_input: 99,
            ..Default::default()
        },
    );
    store.log(
        "child",
        &LogEntry {
            kind: "tool_call".into(),
            role: "tool".into(),
            summary: "large tool output".into(),
            detail: Some(json!({"large":"must not reach the delegate panel"})),
            ..Default::default()
        },
    );
    store.log(
        "child",
        &LogEntry {
            kind: "agent_response".into(),
            role: "agent".into(),
            summary: "child response".into(),
            detail: Some(json!({"text":"child response"})),
            tokens_output: 2,
            ..Default::default()
        },
    );
    drop(store);

    let conn = rusqlite::Connection::open(&state).unwrap();
    for (session_id, created_at, updated_at) in [
        ("parent", DAY_START, DAY_START + 20),
        ("child", DAY_START + 60, DAY_START + 50),
        ("second-parent", DAY_START + 120, DAY_START + 120),
        ("out-of-range", DAY_START + 86_400, DAY_START + 86_400),
    ] {
        conn.execute(
            "UPDATE sessions SET created_at=?2, updated_at=?3 WHERE router_session_id=?1",
            rusqlite::params![session_id, created_at, updated_at],
        )
        .unwrap();
    }
    for (session_id, kind, ts) in [
        ("parent", "user_prompt", DAY_START + 10),
        ("parent", "agent_response", DAY_START + 20),
        ("child", "delegate_task", DAY_START + 30),
        ("child", "tool_call", DAY_START + 40),
        ("child", "agent_response", DAY_START + 50),
        ("out-of-range", "user_prompt", DAY_START + 86_400),
    ] {
        conn.execute(
            "UPDATE session_log SET ts=?3 WHERE router_session_id=?1 AND kind=?2",
            rusqlite::params![session_id, kind, ts],
        )
        .unwrap();
    }
    conn.execute(
        "INSERT INTO llm_requests (request_id, router_session_id, agent, protocol, endpoint,
             pinned_model, model, routing_reason, routing_event, started_at,
             tokens_input, tokens_output, tokens_cache_read, tokens_cache_write)
         VALUES ('request-1', 'parent', 'mock', 'openai', 'responses',
             'mock/m1', 'mock/m1', 'routine', 'selected', ?1, 3, 5, 1, 2)",
        [DAY_START + 20],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO llm_requests (request_id, router_session_id, agent, protocol, endpoint,
             pinned_model, model, routing_reason, routing_event, started_at)
         VALUES ('request-2', 'out-of-range', 'mock', 'openai', 'responses',
             'mock/m1', 'mock/m1', 'routine', 'selected', ?1)",
        [DAY_START + 86_400],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO maintenance_lease (id, holder, expires_at, last_tick_at, last_result)
         VALUES (1, 'router-1', ?1, ?2, ?3)",
        rusqlite::params![DAY_START + 900, DAY_START + 30, r#"{"deferred":false}"#],
    )
    .unwrap();
    drop(conn);

    let session = query(&config, &["session", "--session", "parent"]);
    assert_envelope(&session, "session");
    assert_eq!(session["data"]["session"]["router_session_id"], "parent");
    assert_eq!(session["data"]["session"]["title"], "Parent title");

    let title = query(&config, &["title", "--session", "parent"]);
    assert_envelope(&title, "title");
    assert_eq!(
        title["data"],
        json!({"session_id":"parent", "title":"Parent title"})
    );

    // Matches relay/router-state.mjs: one call accepts every resumed parent,
    // has chronological logs, and only the panel detail allowlist survives.
    let delegates = query(
        &config,
        &[
            "delegates",
            "--session",
            "parent",
            "--session",
            "second-parent",
        ],
    );
    assert_envelope(&delegates, "delegates");
    assert_eq!(
        delegates["data"],
        json!({"children":[{
            "id":"child", "title":"Child title", "agent":"mock", "model":"m1",
            "createdAt":"2026-01-01T00:01:00Z", "updatedAt":"2026-01-01T00:00:50Z",
            "tokensTotal":3, "contextUsed":34,
            "routing":{"class":"Ops", "reason":"delegate state review"},
            "hasResponse":true,
            "log":[
                {"ts":"2026-01-01T00:00:30Z", "kind":"delegate_task", "role":"router", "summary":"inspect child state", "detail":{"task":"inspect"}},
                {"ts":"2026-01-01T00:00:40Z", "kind":"tool_call", "role":"tool", "summary":"large tool output", "detail":null},
                {"ts":"2026-01-01T00:00:50Z", "kind":"agent_response", "role":"agent", "summary":"child response", "detail":{"text":"child response"}}
            ]
        }]})
    );

    let logs = query(
        &config,
        &["logs", "--session", "parent", "--kind", "agent_response"],
    );
    assert_envelope(&logs, "logs");
    assert_eq!(logs["data"]["entries"][0]["ts"], DAY_START + 20);
    assert_eq!(logs["data"]["entries"][0]["kind"], "agent_response");

    // Matches relay/analytics.mjs: sessions and daily buckets retain the
    // inclusive start / exclusive end range, then llm_requests feeds savings.
    let analytics = query(
        &config,
        &[
            "analytics",
            "--from-sec",
            &DAY_START.to_string(),
            "--to-end-sec",
            &(DAY_START + 86_400).to_string(),
        ],
    );
    assert_envelope(&analytics, "analytics");
    assert_eq!(
        analytics["data"],
        json!({
            "sessions":[
                {"id":"parent", "agent":"mock", "model":"m1", "kind":"primary", "runLabel":null,
                 "class":null, "reason":null, "createdAt":"2026-01-01T00:00:00Z", "updatedAt":"2026-01-01T00:00:20Z",
                 "tokensInput":3, "tokensOutput":5, "tokensTotal":8, "contextUsed":13, "costUsd":0.25},
                {"id":"child", "agent":"mock", "model":"m1", "kind":"delegate", "runLabel":null,
                 "class":"Ops", "reason":"delegate state review", "createdAt":"2026-01-01T00:01:00Z", "updatedAt":"2026-01-01T00:00:50Z",
                 "tokensInput":1, "tokensOutput":2, "tokensTotal":3, "contextUsed":34, "costUsd":0.0},
                {"id":"second-parent", "agent":"mock", "model":"m1", "kind":"primary", "runLabel":null,
                 "class":null, "reason":null, "createdAt":"2026-01-01T00:02:00Z", "updatedAt":"2026-01-01T00:02:00Z",
                 "tokensInput":0, "tokensOutput":0, "tokensTotal":0, "contextUsed":0, "costUsd":0.0}
            ],
            "daily":[
                {"date":"2026-01-01", "agent":"mock", "class":"Ops", "kind":"delegate", "tokensInput":1, "tokensOutput":2, "entries":3},
                {"date":"2026-01-01", "agent":"mock", "class":null, "kind":"primary", "tokensInput":3, "tokensOutput":5, "entries":2}
            ],
            "llmRequests":[{
                "pinned_model":"mock/m1", "model":"mock/m1", "protocol":"openai", "started_at":DAY_START + 20,
                "tokens_input":3, "tokens_output":5, "tokens_cache_read":1, "tokens_cache_write":2
            }]
        })
    );

    // Matches relay/status.mjs without a table-wide row count on each refresh.
    let health = query(&config, &["health"]);
    assert_envelope(&health, "health");
    let data = health["data"].as_object().unwrap();
    let mut keys: Vec<_> = data.keys().cloned().collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "autoVacuum",
            "dbBytes",
            "dbPath",
            "freelistBytes",
            "maintenance",
            "retentionSeconds",
            "walBytes",
        ]
    );
    assert_eq!(health["data"]["retentionSeconds"], 7 * 24 * 60 * 60);
    assert!(health["data"]["dbBytes"].as_u64().unwrap() > 0);
    assert_eq!(
        health["data"]["maintenance"],
        json!({
            "holder":"router-1", "lastTickAt":(DAY_START + 30) * 1000,
            "lastResult":{"deferred":false}
        })
    );

    let report = query(&config, &["delegation-report"]);
    assert_envelope(&report, "delegation_report");
    assert_eq!(report["data"]["prompted_sessions"], 1);
    assert_eq!(report["data"]["sessions_that_delegated"], 1);

    let transcript = query(&config, &["transcript", "--session", "parent"]);
    assert_envelope(&transcript, "transcript");
    assert_eq!(transcript["data"]["entries"].as_array().unwrap().len(), 2);
    assert_eq!(transcript["data"]["entries"][0]["ts"], DAY_START + 10);
}

/// Keys only: two outputs with this shape differ in values, never in schema.
fn shape(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), shape(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.first().map(shape).into_iter().collect()),
        _ => Value::Null,
    }
}

fn populate(store: &StateStore, parent: &str) {
    let child = format!("{parent}::delegate-down");
    store.upsert(parent.into(), session(None, "primary", "Parent", DAY_START));
    store.note_delegation_directive(parent);
    store.upsert(
        child.clone(),
        session(Some(parent), "delegate", "Child", DAY_START + 60),
    );
    for (sid, kind) in [
        (parent, "user_prompt"),
        (child.as_str(), "delegate_task"),
        (child.as_str(), "agent_response"),
    ] {
        store.log(
            sid,
            &LogEntry {
                kind: kind.into(),
                role: "router".into(),
                summary: kind.into(),
                detail: Some(json!({"text": kind})),
                tokens_input: 1,
                ..Default::default()
            },
        );
    }
}

#[test]
fn v1_shapes_match_for_legacy_and_sharded_sessions() {
    let week = Retention {
        max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60),
    };
    let plain = tempfile::tempdir().unwrap();
    let plain_config = plain.path().join("router.yaml");
    write_config(&plain_config, &plain.path().join("state.db"));
    populate(
        &StateStore::load(&plain.path().join("state.db"), week),
        "rtr-parent",
    );

    let sharded = tempfile::tempdir().unwrap();
    let sharded_state = sharded.path().join("state.db");
    let sharded_config = sharded.path().join("router.yaml");
    write_config(&sharded_config, &sharded_state);
    let store = StateStore::load(&sharded_state, week);
    let parent = store
        .new_session_id(&sharded.path().join("checkout"))
        .unwrap();
    populate(&store, &parent);
    drop(store);
    let shard =
        StateLayout::new(&sharded_state).shard_path(&tag_for_cwd(&sharded.path().join("checkout")));
    assert!(shard.exists());

    let range = [
        "analytics".to_string(),
        "--from-sec".into(),
        DAY_START.to_string(),
        "--to-end-sec".into(),
        (DAY_START + 86_400).to_string(),
    ];
    for (plain_args, sharded_args) in ["session", "title", "delegates", "logs", "transcript"]
        .iter()
        .map(|command| {
            (
                vec![command.to_string(), "--session".into(), "rtr-parent".into()],
                vec![command.to_string(), "--session".into(), parent.clone()],
            )
        })
        .chain(
            [
                vec!["health".to_string()],
                vec!["delegation-report".into()],
                range.to_vec(),
            ]
            .into_iter()
            .map(|args| (args.clone(), args)),
        )
    {
        fn args(list: &[String]) -> Vec<&str> {
            list.iter().map(String::as_str).collect()
        }
        let expected = query(&plain_config, &args(&plain_args));
        let actual = query(&sharded_config, &args(&sharded_args));
        assert_eq!(shape(&actual), shape(&expected), "{sharded_args:?}");
        assert_ne!(
            actual["data"],
            json!({}),
            "{sharded_args:?} found the shard: {actual}"
        );
    }

    let delegates = query(&sharded_config, &["delegates", "--session", &parent]);
    assert_eq!(delegates["data"]["children"][0]["hasResponse"], true);
    let health = query(&sharded_config, &["health"]);
    let size = |path: &Path| std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(
        health["data"]["dbBytes"],
        size(&sharded_state) + size(&shard),
        "{health}"
    );
    assert_eq!(
        health["data"]["dbPath"],
        sharded_state.display().to_string()
    );
}
