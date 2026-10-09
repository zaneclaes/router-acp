//! Native SQLite restoration through the installed-shaped ACP executable.
//!
//! The mock has no lifecycle capability. A restart must therefore reconstruct
//! an adapter session from router-owned SQLite state.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(5);

struct Client {
    child: Child,
    input: Option<ChildStdin>,
    output: tokio::io::Lines<BufReader<ChildStdout>>,
    events: Vec<Value>,
    next_id: u64,
}

impl Client {
    async fn send(&mut self, method: &str, params: Value) -> u64 {
        self.next_id += 1;
        let id = self.next_id;
        let frame = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        self.input
            .as_mut()
            .expect("router stdin is open")
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .expect("write ACP request");
        self.input
            .as_mut()
            .expect("router stdin is open")
            .flush()
            .await
            .expect("flush ACP request");
        id
    }

    async fn response(&mut self, id: u64) -> Value {
        tokio::time::timeout(RPC_TIMEOUT, async {
            loop {
                let line = self
                    .output
                    .next_line()
                    .await
                    .expect("read ACP frame")
                    .expect("router closed stdout before its response");
                let frame: Value = serde_json::from_str(&line).expect("valid ACP JSON");
                if frame.get("id") == Some(&json!(id)) {
                    return frame;
                }
                self.events.push(frame);
            }
        })
        .await
        .expect("bounded ACP response")
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.send(method, params).await;
        self.response(id).await
    }

    async fn initialize(&mut self) {
        assert_success(
            &self
                .request(
                    "initialize",
                    json!({"protocolVersion": 1, "clientCapabilities": {}}),
                )
                .await,
            "initialize",
        );
    }

    async fn new_session(&mut self, cwd: &Path, meta: Option<Value>) -> Value {
        let mut params = json!({"cwd": cwd, "mcpServers": []});
        if let Some(meta) = meta {
            params["_meta"] = meta;
        }
        self.request("session/new", params).await
    }

    async fn resume(&mut self, session_id: &str, cwd: &Path) -> Value {
        self.request(
            "session/resume",
            json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
        )
        .await
    }

    async fn set_option(&mut self, session_id: &str, id: &str, value: &str) -> Value {
        self.request(
            "session/set_config_option",
            json!({
                "sessionId": session_id,
                "configId": id,
                "value": value,
            }),
        )
        .await
    }

    async fn prompt(&mut self, session_id: &str, text: &str, meta: Option<Value>) -> Value {
        let mut params = json!({
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": text}],
        });
        if let Some(meta) = meta {
            params["_meta"] = meta;
        }
        self.request("session/prompt", params).await
    }

    async fn close(mut self) {
        self.input.take();
        let status = tokio::time::timeout(EXIT_TIMEOUT, self.child.wait())
            .await
            .expect("router exits when its ACP client disconnects")
            .expect("wait for router");
        assert!(
            status.success(),
            "router failed after ACP disconnect: {status}"
        );
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    config: PathBuf,
    state: PathBuf,
    home: PathBuf,
    mock_log: PathBuf,
}

fn fixture(name: &str) -> Fixture {
    let dir = tempfile::tempdir().expect("temporary fixture");
    let home = dir.path().join("private-home");
    let state = dir.path().join("private-state.db");
    let config = dir.path().join("private-router.yaml");
    let mock_log = dir.path().join("mock.jsonl");
    std::fs::create_dir(&home).expect("private HOME");

    // No MOCK_SUPPORTS_LIFECYCLE: no test may use provider-local resume.
    std::fs::write(
        &config,
        format!(
            r#"state_file: {}
delegation: {{ enabled: false }}
auto_upgrade: {{ enabled: false }}
failover: {{ enabled: false }}
router: planner
routers:
  planner:
    planning_candidates: ["mock/m2"]
    implementation_candidates: ["mock/m1"]
pinned_versions:
  "mock/m2": m2-saved
agents:
  - name: mock
    command:
      type: stdio
      command: {}
      env:
        - {{ name: HOME, value: {} }}
        - {{ name: MOCK_NAME, value: restore-{} }}
        - {{ name: MOCK_MODELS, value: "m1,m2" }}
        - {{ name: MOCK_CAPS_IMAGE, value: "1" }}
        - {{ name: MOCK_LOG, value: {} }}
    model_selection: {{ type: config-option }}
    models:
      - {{ id: m1, cost_rank: 1 }}
      - {{ id: m2, cost_rank: 2, versions: [{{ api_model: m2-saved }}] }}
"#,
            state.display(),
            env!("CARGO_BIN_EXE_mock-agent"),
            home.display(),
            name,
            mock_log.display(),
        ),
    )
    .expect("write private config");

    Fixture {
        _dir: dir,
        config,
        state,
        home,
        mock_log,
    }
}

fn spawn(fixture: &Fixture) -> Client {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_router-acp"));
    command
        .args([
            "serve",
            "--config",
            fixture.config.to_str().expect("UTF-8 config path"),
        ])
        .env("HOME", &fixture.home)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("spawn router executable");
    Client {
        input: child.stdin.take(),
        output: BufReader::new(child.stdout.take().expect("router stdout")).lines(),
        child,
        events: Vec::new(),
        next_id: 0,
    }
}

fn assert_success(frame: &Value, action: &str) {
    assert!(frame.get("result").is_some(), "{action} failed: {frame}");
}

fn session_id(frame: &Value) -> String {
    frame["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("session/new did not return a router id: {frame}"))
        .to_string()
}

fn state(fixture: &Fixture) -> router_acp::state::StateFile {
    router_acp::state::StateFile::load(&fixture.state, router_acp::state::Retention::default())
}

fn mock_prompts(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event["event"] == "prompt")
        .filter_map(|event| event["text"].as_str().map(str::to_string))
        .collect()
}

fn meta_for(events: &[Value], session_id: &str) -> Value {
    events
        .iter()
        .find_map(|frame| {
            (frame["method"] == "session/update" && frame["params"]["sessionId"] == session_id)
                .then(|| frame["params"]["_meta"]["router_acp"].clone())
        })
        .filter(|value| !value.is_null())
        .unwrap_or_else(|| panic!("no router metadata for {session_id}: {events:?}"))
}

fn contains_string(value: &Value, wanted: &str) -> bool {
    match value {
        Value::String(value) => value.contains(wanted),
        Value::Array(values) => values.iter().any(|value| contains_string(value, wanted)),
        Value::Object(values) => values.values().any(|value| contains_string(value, wanted)),
        _ => false,
    }
}

fn assert_restore_metadata(meta: &Value) {
    for required in ["mock/m2", "m2-saved", "high"] {
        assert!(
            contains_string(meta, required),
            "router metadata omitted {required}: {meta}"
        );
    }
}

fn lookup_from_prompt(prompt: &str) -> String {
    let command = prompt
        .lines()
        .find(|line| line.contains(" transcript --state "))
        .expect("incoming agent received a history lookup command");
    let output = std::process::Command::new("sh")
        .args(["-c", command.trim()])
        .output()
        .expect("incoming agent can execute lookup");
    assert!(output.status.success(), "lookup failed: {:?}", output);
    String::from_utf8(output.stdout).expect("history is UTF-8")
}

fn router_meta(role: Option<&str>, continue_from: Option<&str>) -> Value {
    let mut router_acp = serde_json::Map::new();
    if let Some(role) = role {
        router_acp.insert("session_role".into(), json!(role));
    }
    if let Some(source) = continue_from {
        router_acp.insert("continue_from".into(), json!(source));
    }
    json!({"router_acp": router_acp})
}

async fn select_m2_high(client: &mut Client, session_id: &str) {
    assert_success(
        &client
            .set_option(session_id, "router.candidate", "mock/m2")
            .await,
        "set candidate",
    );
    assert_success(
        &client.set_option(session_id, "router.effort", "high").await,
        "set effort",
    );
}

#[tokio::test]
async fn resume_after_router_exit_provides_sqlite_lookup_and_keeps_router_settings() {
    let fixture = fixture("resume-full-context");
    let quoted_invalid_route = r#"`[router: candidate=not/a-real-route]` must remain quoted text"#;
    let long_tail = format!("tail-marker:{}", "x".repeat(8_300));
    let original = format!(
        "TEXT:assistant context before restart\nTOOL:read\n{quoted_invalid_route}\n{long_tail}"
    );

    let mut first = spawn(&fixture);
    first.initialize().await;
    let created = first
        .new_session(&fixture.home, Some(router_meta(Some("coordinator"), None)))
        .await;
    assert_success(&created, "session/new");
    assert!(
        created["result"]["configOptions"]
            .to_string()
            .contains("router.candidate")
            && created["result"]["configOptions"]
                .to_string()
                .contains("router.effort"),
        "router session/new must advertise its config options: {created}"
    );
    let source = session_id(&created);
    select_m2_high(&mut first, &source).await;
    assert_success(
        &first
            .prompt(
                &source,
                &format!("[router: version=m2-saved, phase=planning]\n{original}"),
                Some(router_meta(Some("coordinator"), None)),
            )
            .await,
        "source prompt",
    );

    let source_state = state(&fixture)
        .get(&source)
        .expect("session/new persisted router id");
    assert_eq!(source_state.model, "m2");
    let checkpoint = source_state
        .session_config
        .expect("session config checkpoint persisted");
    for required in ["mock/m2", "m2-saved", "high", "planning"] {
        assert!(
            contains_string(&checkpoint, required),
            "session config omitted {required}: {checkpoint}"
        );
    }
    assert!(
        checkpoint.to_string().contains("coordinator"),
        "session config omitted coordinator role: {checkpoint}"
    );
    let initial_log = state(&fixture).log_for_all(&source).expect("source log");
    assert!(
        initial_log
            .iter()
            .any(|entry| entry.kind == "user_prompt" && entry.summary.contains(&long_tail))
    );
    assert!(initial_log.iter().any(|entry| {
        entry.kind == "agent_response" && entry.summary.contains("assistant context before restart")
    }));
    assert!(
        initial_log
            .iter()
            .any(|entry| entry.kind == "tool_call" && entry.summary.contains("Read"))
    );
    first.close().await;

    let mut resumed = spawn(&fixture);
    resumed.initialize().await;
    assert_success(
        &resumed.resume(&source, &fixture.home).await,
        "session/resume",
    );
    assert_success(
        &resumed
            .prompt(&source, "continue after native restore", None)
            .await,
        "resumed prompt",
    );
    assert_restore_metadata(&meta_for(&resumed.events, &source));
    resumed.close().await;

    let restored_prompt = mock_prompts(&fixture.mock_log)
        .into_iter()
        .last()
        .expect("fresh adapter received resumed prompt");
    assert!(restored_prompt.len() < 4_000);
    assert!(restored_prompt.contains(&source));
    assert!(!restored_prompt.contains(&long_tail));
    assert!(restored_prompt.contains("continue after native restore"));
    let retrieved = lookup_from_prompt(&restored_prompt);
    for required in [
        quoted_invalid_route,
        long_tail.as_str(),
        "assistant context before restart",
        "Read",
        "continue after native restore",
    ] {
        assert!(
            retrieved.contains(required),
            "fresh adapter did not receive reconstructed context {required:?}"
        );
    }
    assert!(
        !std::fs::read_to_string(&fixture.mock_log)
            .unwrap_or_default()
            .contains("\"event\":\"session_resume\""),
        "native restoration must not require unsupported downstream lifecycle"
    );

    let restored = state(&fixture);
    let restored_source = restored.get(&source).expect("same router row");
    assert_eq!(restored_source.model, "m2");
    let restored_checkpoint = restored_source
        .session_config
        .expect("restart retained session config checkpoint");
    for required in ["mock/m2", "m2-saved", "high", "planning", "coordinator"] {
        assert!(
            restored_checkpoint.to_string().contains(required),
            "restart lost {required} from session config: {restored_checkpoint}"
        );
    }
    let user_prompts: Vec<_> = restored
        .log_for(&source, 100)
        .into_iter()
        .filter(|entry| entry.kind == "user_prompt")
        .collect();
    assert_eq!(user_prompts.len(), 2, "replay must not be a new user turn");
    assert_eq!(user_prompts[1].summary, "continue after native restore");
    assert!(!user_prompts[1].summary.contains(&long_tail));
}

#[tokio::test]
async fn unprompted_unknown_and_concurrent_resume_ids_stay_isolated() {
    let fixture = fixture("resume-isolation");
    let mut first = spawn(&fixture);
    first.initialize().await;

    let unprompted = session_id(&first.new_session(&fixture.home, None).await);
    let alpha = session_id(&first.new_session(&fixture.home, None).await);
    let bravo = session_id(&first.new_session(&fixture.home, None).await);
    for session_id in [&alpha, &bravo] {
        select_m2_high(&mut first, session_id).await;
    }
    assert_success(
        &first.prompt(&alpha, "alpha-only source", None).await,
        "alpha prompt",
    );
    assert_success(
        &first.prompt(&bravo, "bravo-only source", None).await,
        "bravo prompt",
    );
    assert!(
        state(&fixture)
            .get(&unprompted)
            .expect("unprompted router id is persisted")
            .downstream_session_id
            .is_empty(),
        "an unprompted router session must not need a provider session id"
    );
    first.close().await;

    let mut unprompted_client = spawn(&fixture);
    unprompted_client.initialize().await;
    assert_success(
        &unprompted_client.resume(&unprompted, &fixture.home).await,
        "resume unprompted router id",
    );
    assert_success(
        &unprompted_client
            .prompt(&unprompted, "first prompt after resume", None)
            .await,
        "prompt unprompted resume",
    );
    unprompted_client.close().await;

    let before_unknown = state(&fixture).all().len();
    let mut unknown_client = spawn(&fixture);
    unknown_client.initialize().await;
    let unknown = unknown_client
        .resume("rtr-not-a-saved-session", &fixture.home)
        .await;
    assert!(
        unknown.get("error").is_some(),
        "unknown id made a session: {unknown}"
    );
    unknown_client.close().await;
    assert_eq!(
        state(&fixture).all().len(),
        before_unknown,
        "unknown resume created state"
    );

    let (alpha_text, bravo_text) = tokio::join!(
        resume_and_prompt(&fixture, &alpha, "alpha follow-up"),
        resume_and_prompt(&fixture, &bravo, "bravo follow-up"),
    );
    for (own_source, other_source, text) in [
        ("alpha-only source", "bravo-only source", alpha_text),
        ("bravo-only source", "alpha-only source", bravo_text),
    ] {
        let text = lookup_from_prompt(&text);
        assert!(
            text.contains(own_source),
            "missing own restored history: {text}"
        );
        assert!(
            !text.contains(other_source),
            "concurrent restored sessions contaminated each other: {text}"
        );
    }
}

async fn resume_and_prompt(fixture: &Fixture, session_id: &str, prompt: &str) -> String {
    let mut client = spawn(fixture);
    client.initialize().await;
    assert_success(
        &client.resume(session_id, &fixture.home).await,
        "concurrent resume",
    );
    assert_success(
        &client.prompt(session_id, prompt, None).await,
        "concurrent prompt",
    );
    client.close().await;

    mock_prompts(&fixture.mock_log)
        .into_iter()
        .rev()
        .find(|text| text.contains(prompt))
        .unwrap_or_else(|| panic!("fresh adapter did not receive {prompt:?}"))
}

#[tokio::test]
async fn continue_from_creates_a_new_restorable_router_session_without_kory_context() {
    let fixture = fixture("continue-from");
    let source_text = "source-only history for native continue_from";

    let mut source_client = spawn(&fixture);
    source_client.initialize().await;
    let source = session_id(&source_client.new_session(&fixture.home, None).await);
    select_m2_high(&mut source_client, &source).await;
    assert_success(
        &source_client
            .prompt(
                &source,
                &format!("[router: version=m2-saved]\nTEXT:source assistant reply\n{source_text}"),
                None,
            )
            .await,
        "source prompt",
    );
    source_client.close().await;

    let mut child_client = spawn(&fixture);
    child_client.initialize().await;
    let child_created = child_client
        .new_session(&fixture.home, Some(router_meta(None, Some(&source))))
        .await;
    assert_success(&child_created, "session/new continue_from");
    assert_eq!(
        child_created["result"]["_meta"]["router_acp"]["version"],
        "m2-saved"
    );
    let child = session_id(&child_created);
    assert_ne!(child, source, "continue_from must allocate a new router id");
    assert!(
        state(&fixture).get(&child).is_some(),
        "child router id must persist before its first prompt"
    );
    // The child has never reached an adapter, so no downstream id can carry
    // inherited history across this restart.
    child_client.close().await;

    // The child's native snapshot must survive deletion of the source.
    let mut deletion = spawn(&fixture);
    deletion.initialize().await;
    assert_success(
        &deletion
            .request("session/delete", json!({"sessionId":source}))
            .await,
        "delete source after child snapshot",
    );
    deletion.close().await;

    let mut restored_child = spawn(&fixture);
    restored_child.initialize().await;
    assert_success(
        &restored_child.resume(&child, &fixture.home).await,
        "resume unprompted continue_from child",
    );
    assert_success(
        &restored_child
            .prompt(&child, "child first prompt", None)
            .await,
        "child first prompt",
    );
    assert_restore_metadata(&meta_for(&restored_child.events, &child));
    restored_child.close().await;

    let delivered = mock_prompts(&fixture.mock_log)
        .into_iter()
        .rev()
        .find(|text| text.contains("child first prompt"))
        .expect("child adapter prompt");
    assert!(!delivered.contains(source_text));
    assert!(delivered.contains(&child));
    let retrieved = lookup_from_prompt(&delivered);
    for required in [source_text, "source assistant reply", "child first prompt"] {
        assert!(
            retrieved.contains(required),
            "continue_from did not reconstruct source history {required:?}: {delivered}"
        );
    }
    let child_log = state(&fixture).log_for(&child, 100);
    assert!(
        child_log
            .iter()
            .any(|entry| entry.kind == "user_prompt" && entry.summary == "child first prompt")
    );
}

#[tokio::test]
async fn rich_content_and_long_assistant_output_survive_restart_without_replay_duplicates() {
    let fixture = fixture("rich-content");
    let image = json!({"type":"image", "mimeType":"image/png", "data":"aW1hZ2U="});
    let assistant = format!("assistant-full-{}-end", "y".repeat(9_000));
    let mut client = spawn(&fixture);
    client.initialize().await;
    let sid = session_id(&client.new_session(&fixture.home, None).await);
    select_m2_high(&mut client, &sid).await;
    assert_success(
        &client
            .request(
                "session/prompt",
                json!({
                    "sessionId":sid,
                    "prompt":[{"type":"text", "text":format!("TEXT:{assistant}")},image]
                }),
            )
            .await,
        "rich prompt",
    );
    client.close().await;
    let entries = state(&fixture).log_for_all(&sid).unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.kind == "agent_response" && e.summary.contains(&assistant))
    );
    let mut resumed = spawn(&fixture);
    resumed.initialize().await;
    assert_success(&resumed.resume(&sid, &fixture.home).await, "rich resume");
    assert!(
        resumed.events.is_empty(),
        "Resume must not replay the UI transcript"
    );
    assert_success(
        &resumed.prompt(&sid, "follow-up-rich", None).await,
        "rich follow-up",
    );
    resumed.close().await;
    let events: Vec<Value> = std::fs::read_to_string(&fixture.mock_log)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let prompt = events
        .iter()
        .rev()
        .find(|e| e["event"] == "prompt")
        .unwrap();
    assert!(!prompt["content"].as_array().unwrap().contains(&image));
    let delivered = prompt["text"].as_str().unwrap();
    assert!(!delivered.contains(&assistant));
    let retrieved = lookup_from_prompt(delivered);
    assert!(retrieved.contains(&assistant) && retrieved.contains("aW1hZ2U="));
    assert_eq!(
        state(&fixture)
            .log_for_all(&sid)
            .unwrap()
            .iter()
            .filter(|e| e.kind == "user_prompt")
            .count(),
        2
    );
}

#[tokio::test]
async fn a_canceled_first_delivery_keeps_restored_context_for_the_next_prompt() {
    let fixture = fixture("cancel-restore");
    let mut client = spawn(&fixture);
    client.initialize().await;
    let sid = session_id(&client.new_session(&fixture.home, None).await);
    select_m2_high(&mut client, &sid).await;
    assert_success(
        &client
            .prompt(&sid, "source-before-cancellation", None)
            .await,
        "source prompt",
    );
    client.close().await;

    let mut resumed = spawn(&fixture);
    resumed.initialize().await;
    assert_success(&resumed.resume(&sid, &fixture.home).await, "resume");
    let prompt_id = resumed
        .send(
            "session/prompt",
            json!({
                "sessionId":sid,
                "prompt":[{"type":"text", "text":"TEXT:cancellation-marker\nSLEEP:5000"}]
            }),
        )
        .await;
    tokio::time::timeout(RPC_TIMEOUT, async {
        loop {
            let line = resumed.output.next_line().await.unwrap().unwrap();
            let frame: Value = serde_json::from_str(&line).unwrap();
            if contains_string(&frame, "cancellation-marker") {
                break;
            }
            resumed.events.push(frame);
        }
    })
    .await
    .expect("first restored delivery reaches adapter");
    let cancel = json!({"jsonrpc":"2.0", "method":"session/cancel", "params":{"sessionId":sid}});
    resumed
        .input
        .as_mut()
        .unwrap()
        .write_all(format!("{cancel}\n").as_bytes())
        .await
        .unwrap();
    resumed.input.as_mut().unwrap().flush().await.unwrap();
    let result = resumed.response(prompt_id).await;
    assert_eq!(result["result"]["stopReason"], "cancelled", "{result}");
    assert_success(
        &resumed.prompt(&sid, "retry-after-cancellation", None).await,
        "retry",
    );
    resumed.close().await;
    let prompts = mock_prompts(&fixture.mock_log);
    assert!(lookup_from_prompt(prompts.last().unwrap()).contains("source-before-cancellation"));
}

#[tokio::test]
async fn tool_only_load_deduplicates_notifications_and_restores_typed_tool_content() {
    let fixture = fixture("tool-only-load");
    let mut client = spawn(&fixture);
    client.initialize().await;
    let sid = session_id(&client.new_session(&fixture.home, None).await);
    // These are preexisting native transcript records, with no assistant text.
    let update = json!({
        "sessionUpdate":"tool_call", "toolCallId":"rich-tool", "title":"Rich tool", "status":"completed",
        "content":[{"type":"content", "content":{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}}]
    });
    let db = state(&fixture);
    for kind in ["session_update", "tool_call"] {
        db.log_checked(
            &sid,
            &router_acp::state::LogEntry {
                kind: kind.into(),
                detail: Some(update.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    client.close().await;
    let mut loaded = spawn(&fixture);
    loaded.initialize().await;
    assert_success(
        &loaded
            .request(
                "session/load",
                json!({"sessionId":sid,"cwd":fixture.home,"mcpServers":[]}),
            )
            .await,
        "load tool-only history",
    );
    let tools = loaded
        .events
        .iter()
        .filter(|e| e["params"]["update"]["toolCallId"] == "rich-tool")
        .count();
    assert_eq!(
        tools, 1,
        "a raw update and its aggregate must make one UI tool"
    );
    assert_success(
        &loaded.prompt(&sid, "after-tool-only-load", None).await,
        "prompt",
    );
    loaded.close().await;
    let delivered = mock_prompts(&fixture.mock_log).pop().unwrap();
    let retrieved = lookup_from_prompt(&delivered);
    assert!(retrieved.contains("Rich tool") && retrieved.contains("aW1hZ2U="));
}

#[tokio::test]
async fn a_rejected_streaming_row_still_saves_the_turns_answer() {
    let fixture = fixture("write-failure");
    let mut client = spawn(&fixture);
    client.initialize().await;
    let sid = session_id(&client.new_session(&fixture.home, None).await);
    select_m2_high(&mut client, &sid).await;
    let db = rusqlite::Connection::open(&fixture.state).unwrap();
    db.execute_batch("CREATE TRIGGER synthetic_write_failure BEFORE INSERT ON session_log WHEN NEW.kind = 'session_update' BEGIN SELECT RAISE(FAIL, 'synthetic transcript write failure'); END;").unwrap();
    let response = client.prompt(&sid, "TEXT:still-durable", None).await;
    assert_success(&response, "prompt");
    client.close().await;
    // Restore falls back to the turn's answer row when its stream is missing.
    let entries = state(&fixture).log_for_all(&sid).unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.kind == "agent_response" && e.summary.contains("still-durable")),
        "{entries:?}"
    );
}

#[tokio::test]
async fn accepted_mid_turn_messages_survive_restart_without_recording_declined_steers() {
    let fixture = fixture("steering-history");
    let mut client = spawn(&fixture);
    client.initialize().await;
    let sid = session_id(&client.new_session(&fixture.home, None).await);
    select_m2_high(&mut client, &sid).await;
    assert_success(
        &client.prompt(&sid, "original request", None).await,
        "prompt",
    );
    for (text, outcome) in [
        ("accepted steering message", "injected"),
        ("DECLINED-STEER", "declined"),
    ] {
        let result = client
            .request(
                "_session/steering",
                json!({
                    "sessionId": sid,
                    "prompt": [{"type": "text", "text": text}],
                }),
            )
            .await;
        assert_success(&result, "steering");
        assert_eq!(result["result"]["outcome"], outcome);
    }
    client.close().await;
    let entries = state(&fixture).log_for_all(&sid).unwrap();
    assert_eq!(entries.iter().filter(|e| e.kind == "user_steer").count(), 1);
    let delivered = resume_and_prompt(&fixture, &sid, "after steering restart").await;
    assert!(!delivered.contains("accepted steering message"));
    let retrieved = lookup_from_prompt(&delivered);
    assert!(retrieved.contains("accepted steering message"));
    assert!(!retrieved.contains("DECLINED-STEER"));
}
