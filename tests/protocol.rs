//! Protocol tests: router-acp against scripted mock ACP downstreams.
//!
//! Each test runs the router in-process over a duplex channel while the mock
//! downstream agents run as real subprocesses (`mock-agent` binary).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    AuthenticateRequest, CancelNotification, ClientCapabilities, CloseSessionRequest, ContentBlock,
    CreateElicitationRequest, CreateElicitationResponse, CreateTerminalRequest,
    CreateTerminalResponse, ElicitationAcceptAction, ElicitationAction, ElicitationCapabilities,
    ElicitationFormCapabilities, Error as AcpError, ImageContent, InitializeRequest,
    InitializeResponse, McpServer, McpServerStdio, NewSessionRequest, NewSessionResponse,
    PromptRequest, PromptResponse, ReadTextFileRequest, ReadTextFileResponse,
    RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionConfigKind, SessionConfigOptionValue,
    SessionConfigSelectOptions, SessionNotification, SessionUpdate, SetSessionConfigOptionRequest,
    StopReason,
};
use agent_client_protocol::{
    Agent as AgentPeer, Channel, Client as ClientPeer, ConnectionTo, Dispatch, Handled, Responder,
    on_receive_dispatch, on_receive_notification, on_receive_request,
};

use router_acp::candidate::CandidateId;
use router_acp::config::Config;
use router_acp::session::{Shared, serve_shared};

fn mock_agent_exe() -> &'static str {
    env!("CARGO_BIN_EXE_mock-agent")
}

fn router_exe() -> &'static str {
    env!("CARGO_BIN_EXE_router-acp")
}

fn claude_credential_fixture(outcome: Option<&str>) -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    let value = serde_json::json!({"claudeAiOauth":{"accessToken":"synthetic-access",
        "refreshToken":"synthetic-refresh","expiresAt":4070908800000u64}});
    std::fs::write(
        directory.path().join(".credentials.json"),
        value.to_string(),
    )
    .unwrap();
    if let Some(outcome) = outcome {
        use sha2::{Digest, Sha256};
        let generation = format!(
            "{:x}",
            Sha256::digest(value["claudeAiOauth"].to_string().as_bytes())
        );
        std::fs::write(
            directory.path().join(".router-acp-auth.json"),
            serde_json::json!({"generation":generation,"epoch":"fixture","outcome":outcome,
                "attempted_at":chrono::Utc::now().to_rfc3339()})
            .to_string(),
        )
        .unwrap();
    }
    directory
}

#[tokio::test]
async fn native_auth_error_reply_does_not_cordon_an_unconfirmed_logout() {
    let state = temp_state_file("auth-reply");
    let credential = claude_credential_fixture(None);
    let file = credential.path().join(".credentials.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    value["claudeAiOauth"]
        .as_object_mut()
        .unwrap()
        .remove("refreshToken");
    std::fs::write(&file, value.to_string()).unwrap();
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nauto_upgrade: {{enabled: false}}\nfailover: {{enabled: false}}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "claude",
            &[("m1", 1)],
            &[
                (
                    "CLAUDE_CONFIG_DIR",
                    &credential.path().display().to_string()
                ),
                (
                    "MOCK_REPLY_TEXT",
                    "Failed to authenticate: OAuth session expired and could not be refreshed"
                ),
            ]
        ),
    );
    let config = Config::from_yaml(&yaml).unwrap();
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        assert!(
            prompt_text(&cx, &sid, "[router: candidate=claude/m1]\nstart")
                .await
                .is_err()
        );
        assert_eq!(
            router_acp::credentials::availability(&config.agents[0]),
            router_acp::auth::AuthAvailability::Unknown
        );
        let text = agent_text(&observed, &sid);
        assert!(text.contains("authentication unavailable"), "{text}");
        assert!(!text.contains("definitively rejected"), "{text}");
        let session = new_session(&cx).await?;
        let options = session.config_options.unwrap();
        let candidate = options
            .iter()
            .find(|o| o.id.0.as_ref() == "router.candidate")
            .unwrap();
        let SessionConfigKind::Select(select) = &candidate.kind else {
            panic!("router.candidate must be a select");
        };
        let choices: Vec<_> = match &select.options {
            SessionConfigSelectOptions::Grouped(groups) => {
                groups.iter().flat_map(|g| &g.options).collect()
            }
            SessionConfigSelectOptions::Ungrouped(options) => options.iter().collect(),
            _ => vec![],
        };
        let account = choices
            .iter()
            .find(|o| o.value.0.as_ref() == "claude/m1")
            .unwrap();
        let value = serde_json::to_value(account).unwrap();
        assert_ne!(
            value.pointer("/_meta/router_acp/available"),
            Some(&serde_json::json!(false))
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn personal_and_team_accounts_prompt_concurrently() {
    let state = temp_state_file("account-concurrency");
    let personal = claude_credential_fixture(None);
    let team = claude_credential_fixture(None);
    let personal_log = temp_log("personal-concurrent");
    let team_log = temp_log("team-concurrent");
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nauto_upgrade: {{enabled: false}}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "claude@personal",
            &[("m1", 1)],
            &[
                ("CLAUDE_CONFIG_DIR", &personal.path().display().to_string()),
                ("MOCK_LOG", &personal_log.display().to_string()),
            ]
        ),
        agent_yaml(
            "claude@team",
            &[("m1", 1)],
            &[
                ("CLAUDE_CONFIG_DIR", &team.path().display().to_string()),
                ("MOCK_LOG", &team_log.display().to_string()),
            ]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let first = new_session(&cx).await?.session_id.0.to_string();
        let second = new_session(&cx).await?.session_id.0.to_string();
        let (personal_result, team_result) = tokio::join!(
            prompt_text(
                &cx,
                &first,
                "[router: candidate=claude@personal/m1]\nSLEEP:10000"
            ),
            async {
                tokio::time::timeout(Duration::from_secs(3), async {
                    while !read_log(&personal_log)
                        .iter()
                        .any(|e| e["event"] == "prompt")
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    prompt_text(
                        &cx,
                        &second,
                        "[router: candidate=claude@team/m1]\nteam work",
                    )
                    .await
                })
                .await
                .expect("team account waited for the personal account's turn to end")
                .inspect(|_| {
                    let _ = cx.send_notification(CancelNotification::new(first.clone()));
                })
            }
        );
        assert_eq!(personal_result?.stop_reason, StopReason::Cancelled);
        assert_eq!(team_result?.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &second);
        assert!(text.contains("claude@team/m1"), "{text}");
        assert_eq!(
            open_state(&state).get(&first).unwrap().agent,
            "claude@personal"
        );
        assert_eq!(
            open_state(&state).get(&second).unwrap().agent,
            "claude@team"
        );
        Ok(())
    })
    .await;
}

/// Events observed by the test client.
#[derive(Default)]
struct Observed {
    updates: Vec<SessionNotification>,
    permission_session_ids: Vec<String>,
    read_session_ids: Vec<String>,
    elicitation_session_ids: Vec<String>,
    xai_ask_session_ids: Vec<String>,
    terminal_creates: Vec<(String, String, Vec<String>, Option<PathBuf>)>,
}

type ObservedHandle = Arc<Mutex<Observed>>;

/// Run the router over a duplex channel and drive it with a test client.
async fn run_test<F>(cfg_yaml: String, test_fn: F)
where
    F: AsyncFnOnce(ConnectionTo<AgentPeer>, ObservedHandle) -> Result<(), AcpError>,
{
    run_test_shared(cfg_yaml, async |cx, obs, _shared| test_fn(cx, obs).await).await;
}

/// Like `run_test`, but also hands the test closure the router's `Arc<Shared>`
/// so it can inspect/inject internal state (e.g. usage cordons).
async fn run_test_shared<F>(cfg_yaml: String, test_fn: F)
where
    F: AsyncFnOnce(
        ConnectionTo<AgentPeer>,
        ObservedHandle,
        std::sync::Arc<router_acp::session::Shared>,
    ) -> Result<(), AcpError>,
{
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "router_acp=warn".into()),
        )
        .with_writer(std::io::stderr)
        .try_init();
    let mut cfg = Config::from_yaml(&cfg_yaml).expect("test config parses");
    // Mock adapters must never observe or publish state for a real login.
    let credential_home = tempfile::tempdir().unwrap();
    for agent in &mut cfg.agents {
        if !agent.command.env.iter().any(|entry| entry.name == "HOME") {
            agent.command.env.push(router_acp::config::EnvVarConfig {
                name: "HOME".into(),
                value: credential_home.path().to_string_lossy().into(),
            });
        }
    }
    let shared = Shared::new(cfg).expect("shared state builds");
    let shared_for_test = shared.clone();
    let (channel_a, channel_b) = Channel::duplex();
    let router = tokio::spawn(serve_shared(shared, channel_a));

    let observed: ObservedHandle = Arc::new(Mutex::new(Observed::default()));
    let o_updates = observed.clone();
    let o_perm = observed.clone();
    let o_read = observed.clone();
    let o_elicit = observed.clone();
    let o_xai = observed.clone();
    let o_terminal = observed.clone();

    let client_result = tokio::time::timeout(
        Duration::from_secs(120),
        ClientPeer
            .builder()
            .name("test-client")
            .on_receive_notification(
                move |n: SessionNotification, _cx| {
                    let observed = o_updates.clone();
                    async move {
                        observed.lock().unwrap().updates.push(n);
                        Ok(())
                    }
                },
                on_receive_notification!(),
            )
            .on_receive_request(
                move |req: RequestPermissionRequest,
                      responder: Responder<RequestPermissionResponse>,
                      _cx| {
                    let observed = o_perm.clone();
                    async move {
                        observed
                            .lock()
                            .unwrap()
                            .permission_session_ids
                            .push(req.session_id.0.to_string());
                        responder.respond(RequestPermissionResponse::new(
                            RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(
                                "allow",
                            )),
                        ))
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                move |req: ReadTextFileRequest, responder: Responder<ReadTextFileResponse>, _cx| {
                    let observed = o_read.clone();
                    async move {
                        observed
                            .lock()
                            .unwrap()
                            .read_session_ids
                            .push(req.session_id.0.to_string());
                        responder.respond(ReadTextFileResponse::new(format!(
                            "FILECONTENT:{}",
                            req.path.display()
                        )))
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                move |req: CreateElicitationRequest,
                      responder: Responder<CreateElicitationResponse>,
                      _cx| {
                    let observed = o_elicit.clone();
                    async move {
                        let session_id = match req.mode.scope() {
                            agent_client_protocol::schema::v1::ElicitationScope::Session(s) => {
                                s.session_id.0.to_string()
                            }
                            _ => String::new(),
                        };
                        observed
                            .lock()
                            .unwrap()
                            .elicitation_session_ids
                            .push(session_id);
                        let mut content = std::collections::BTreeMap::new();
                        content.insert(
                            "question_0".to_string(),
                            agent_client_protocol::schema::v1::ElicitationContentValue::String(
                                "yes".to_string(),
                            ),
                        );
                        responder.respond(CreateElicitationResponse::new(
                            ElicitationAction::Accept(
                                ElicitationAcceptAction::new().content(Some(content)),
                            ),
                        ))
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                move |req: CreateTerminalRequest,
                      responder: Responder<CreateTerminalResponse>,
                      _cx| {
                    let observed = o_terminal.clone();
                    async move {
                        observed.lock().unwrap().terminal_creates.push((
                            req.session_id.0.to_string(),
                            req.command,
                            req.args,
                            req.cwd,
                        ));
                        responder.respond(CreateTerminalResponse::new("managed-terminal-1"))
                    }
                },
                on_receive_request!(),
            )
            .on_receive_dispatch(
                move |message: Dispatch, _cx| {
                    let observed = o_xai.clone();
                    async move {
                        match message {
                            Dispatch::Request(msg, responder) => {
                                if msg.method() == "_x.ai/ask_user_question" {
                                    let sid = msg
                                        .params()
                                        .get("sessionId")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or_default()
                                        .to_string();
                                    observed.lock().unwrap().xai_ask_session_ids.push(sid);
                                    let mut answers = serde_json::Map::new();
                                    if let Some(questions) =
                                        msg.params().get("questions").and_then(|v| v.as_array())
                                    {
                                        for question in questions {
                                            let Some(text) =
                                                question.get("question").and_then(|v| v.as_str())
                                            else {
                                                continue;
                                            };
                                            let label = question
                                                .get("options")
                                                .and_then(|v| v.as_array())
                                                .and_then(|arr| arr.first())
                                                .and_then(|v| v.get("label"))
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("Yes");
                                            answers.insert(
                                                text.to_string(),
                                                serde_json::json!([label]),
                                            );
                                        }
                                    }
                                    responder.respond(serde_json::json!({
                                        "outcome": "accepted",
                                        "answers": answers
                                    }))?;
                                    return Ok(Handled::Yes);
                                }
                                Ok(Handled::No {
                                    message: Dispatch::Request(msg, responder),
                                    retry: false,
                                })
                            }
                            other => Ok(Handled::No {
                                message: other,
                                retry: false,
                            }),
                        }
                    }
                },
                on_receive_dispatch!(),
            )
            .connect_with(channel_b, async |cx| {
                test_fn(cx, observed.clone(), shared_for_test.clone()).await
            }),
    )
    .await;

    router.abort();
    match client_result {
        Ok(result) => result.expect("test client failed"),
        Err(_) => panic!("test timed out"),
    }
}

/// Initialize the router and return the response.
async fn init(cx: &ConnectionTo<AgentPeer>) -> Result<InitializeResponse, AcpError> {
    cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
}

async fn init_with_terminals(cx: &ConnectionTo<AgentPeer>) -> Result<InitializeResponse, AcpError> {
    cx.send_request(
        InitializeRequest::new(ProtocolVersion::V1)
            .client_capabilities(ClientCapabilities::new().terminal(true)),
    )
    .block_task()
    .await
}

async fn new_session(cx: &ConnectionTo<AgentPeer>) -> Result<NewSessionResponse, AcpError> {
    cx.send_request(NewSessionRequest::new(std::env::temp_dir()))
        .block_task()
        .await
}

async fn prompt_text(
    cx: &ConnectionTo<AgentPeer>,
    session_id: &str,
    text: &str,
) -> Result<PromptResponse, AcpError> {
    cx.send_request(PromptRequest::new(
        session_id.to_string(),
        vec![ContentBlock::from(text.to_string())],
    ))
    .block_task()
    .await
}

/// All agent text chunks observed for a session, concatenated.
fn agent_text(observed: &ObservedHandle, session_id: &str) -> String {
    observed
        .lock()
        .unwrap()
        .updates
        .iter()
        .filter(|n| n.session_id.0.as_ref() == session_id)
        .filter_map(|n| match &n.update {
            SessionUpdate::AgentMessageChunk(chunk) => match &chunk.content {
                ContentBlock::Text(t) => Some(t.text.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

/// The `_meta.router_acp` routing-disclosure object attached to a session's
/// pin — rides the FIRST forwarded `agent_message_chunk` of a generation
/// (see session.rs's ordering note). Per-tool proxy attribution carries a
/// narrower `router_acp` meta of its own, but on `tool_call` updates, not
/// `agent_message_chunk`, so filtering to the latter is enough to land on
/// the full disclosure.
fn routing_meta_for(observed: &ObservedHandle, session_id: &str) -> Option<serde_json::Value> {
    observed
        .lock()
        .unwrap()
        .updates
        .iter()
        .filter(|n| n.session_id.0.as_ref() == session_id)
        .filter(|n| matches!(n.update, SessionUpdate::AgentMessageChunk(_)))
        .find_map(|n| n.meta.as_ref()?.get("router_acp").cloned())
}

fn temp_state_file(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "router-acp-test-{tag}-{}.db",
        uuid::Uuid::new_v4().simple()
    ))
}

/// Open the state DB the router wrote (read-only view via the lib API).
fn open_state(path: &std::path::Path) -> router_acp::state::StateFile {
    router_acp::state::StateFile::load(path, router_acp::state::Retention::default())
}

fn temp_log(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "router-acp-mocklog-{tag}-{}.jsonl",
        uuid::Uuid::new_v4().simple()
    ))
}

fn read_log(path: &PathBuf) -> Vec<serde_json::Value> {
    match std::fs::read_to_string(path) {
        Ok(text) => text
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// One config-option agent with the given name, models, extra env pairs.
fn agent_yaml(name: &str, models: &[(&str, u32)], env: &[(&str, &str)]) -> String {
    let mut out = format!(
        "  - name: {name}\n    command:\n      type: stdio\n      command: {}\n      env:\n",
        mock_agent_exe()
    );
    let model_ids: Vec<&str> = models.iter().map(|(id, _)| *id).collect();
    out.push_str(&format!(
        "        - {{ name: MOCK_NAME, value: {name} }}\n        - {{ name: MOCK_MODELS, value: \"{}\" }}\n",
        model_ids.join(",")
    ));
    for (k, v) in env {
        // Escape for a double-quoted YAML scalar (JSON env values need this).
        let escaped = v
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");
        out.push_str(&format!(
            "        - {{ name: {k}, value: \"{escaped}\" }}\n"
        ));
    }
    let is_preclass = env.iter().any(|(key, _)| {
        *key == "MOCK_PRECLASS_JSON"
            || *key == "MOCK_PRECLASS_TOOL"
            || *key == "MOCK_PRECLASS_CALLBACK"
            || *key == "MOCK_PRECLASS_HANG"
    });
    if is_preclass {
        out.push_str("        - { name: MOCK_SESSION_MODES, value: preclass }\n");
    }
    out.push_str("    model_selection: { type: config-option }\n");
    if is_preclass {
        out.push_str("    mode_map: { preclass: preclass }\n");
    }
    out.push_str("    models:\n");
    for (id, cost) in models {
        out.push_str(&format!("      - {{ id: \"{id}\", cost_rank: {cost} }}\n"));
    }
    out
}

// ======================================================================
// Milestone 1: transport passthrough
// ======================================================================

#[tokio::test]
async fn passthrough_prompt_roundtrip_with_disclosure_and_remapping() {
    let state = temp_state_file("passthrough");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, observed| {
        let init_resp = init(&cx).await?;
        assert_eq!(init_resp.protocol_version, ProtocolVersion::V1);

        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        assert!(
            sid.starts_with("rtr-"),
            "router-owned session id, got {sid}"
        );

        let resp = prompt_text(&cx, &sid, "hello world").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);

        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("router-acp"),
            "routing disclosure chunk expected, got: {text}"
        );
        assert!(
            text.contains("echo:m1:hello world"),
            "echoed prompt expected, got: {text}"
        );
        // Every update was remapped to the router session id.
        for n in observed.lock().unwrap().updates.iter() {
            assert_eq!(n.session_id.0.as_ref(), sid);
        }
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn permission_and_fs_callbacks_remap_to_router_session() {
    let state = temp_state_file("callbacks");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let resp = prompt_text(&cx, &sid, "PERM\nREADFILE:/tmp/some-file.txt").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);

        let text = agent_text(&observed, &sid);
        assert!(text.contains("perm:selected:allow"), "got: {text}");
        assert!(
            text.contains("read:FILECONTENT:/tmp/some-file.txt"),
            "got: {text}"
        );

        let observed = observed.lock().unwrap();
        assert_eq!(observed.permission_session_ids, vec![sid.clone()]);
        assert_eq!(observed.read_session_ids, vec![sid.clone()]);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn managed_background_tool_calls_upstream_terminal_without_delegation() {
    let state = temp_state_file("managed-background");
    // The helper exe is this crate's router-acp binary.
    // SAFETY: test-scoped env mutation; tests using this run in one process
    // but the value is identical everywhere.
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init_with_terminals(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let resp = prompt_text(&cx, &sid, "BACKGROUND_START:echo managed").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("background-start:Background terminal managed-terminal-1 started"),
            "managed terminal tool result expected, got: {text}"
        );
        assert_eq!(
            observed.lock().unwrap().terminal_creates,
            vec![(
                sid,
                "/bin/sh".to_string(),
                vec!["-lc".to_string(), "echo managed".to_string()],
                None,
            )]
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn grok_combined_terminal_create_is_split_before_the_client() {
    // Grok sends `/bin/bash -lc '…'` as the `command` string with no args.
    // A spec-compliant client spawn()s that whole string as the executable
    // and ENOENTs. The router must split it on the way through.
    let state = temp_state_file("grok-terminal");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init_with_terminals(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let resp = prompt_text(&cx, &sid, "TERMINAL_GROK:echo hello-repro").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("terminal:managed-terminal-1"),
            "grok-style terminal create should succeed, got: {text}"
        );
        assert_eq!(
            observed.lock().unwrap().terminal_creates,
            vec![(
                sid,
                "/bin/bash".to_string(),
                vec!["-lc".to_string(), "echo hello-repro".to_string()],
                None,
            )]
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn plan_updates_pass_through_unmodified() {
    // The router never special-cases `sessionUpdate: "plan"` — this pins that
    // TodoWrite-style snapshots survive the generic notification forwarding
    // (Milestone-1-era passthrough), which is what claude-agent-acp/codex-acp
    // emit for the agent's todo list.
    let state = temp_state_file("plan");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let resp = prompt_text(&cx, &sid, "PLAN:write tests,fix bug!,ship it#").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);

        let plans: Vec<Vec<(String, String)>> = observed
            .lock()
            .unwrap()
            .updates
            .iter()
            .filter(|n| n.session_id.0.as_ref() == sid)
            .filter_map(|n| match &n.update {
                SessionUpdate::Plan(plan) => Some(
                    plan.entries
                        .iter()
                        .map(|e| (e.content.clone(), format!("{:?}", e.status)))
                        .collect(),
                ),
                _ => None,
            })
            .collect();
        assert_eq!(
            plans,
            vec![vec![
                ("write tests".to_string(), "Pending".to_string()),
                ("fix bug".to_string(), "InProgress".to_string()),
                ("ship it".to_string(), "Completed".to_string()),
            ]],
            "plan entries/statuses must round-trip verbatim"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn elicitation_capability_round_trips_and_request_forwards() {
    // The client's advertised elicitation support must survive the typed
    // `initialize` round-trip to the downstream agent (this is what was
    // silently dropped before `unstable_elicitation` was enabled), and a
    // resulting `elicitation/create` request/response must forward like any
    // other client-directed callback.
    let state = temp_state_file("elicit");
    let log = temp_log("elicit");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[("MOCK_LOG", log.to_str().unwrap())])
    );
    run_test(yaml, async |cx, observed| {
        cx.send_request(
            InitializeRequest::new(ProtocolVersion::V1).client_capabilities(
                ClientCapabilities::new()
                    .elicitation(ElicitationCapabilities::new().form(ElicitationFormCapabilities::new())),
            ),
        )
        .block_task()
        .await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let resp = prompt_text(&cx, &sid, "ELICIT:Do you want fries with that?").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);

        let text = agent_text(&observed, &sid);
        assert!(text.contains(r#"elicit:accept:"yes""#), "got: {text}");

        let logged = read_log(&log);
        assert!(
            logged
                .iter()
                .any(|e| e["event"] == "initialize" && e["client_elicitation_form"] == true),
            "router must forward the real client's elicitation capability to the downstream agent, got: {logged:?}"
        );

        assert_eq!(observed.lock().unwrap().elicitation_session_ids, vec![sid]);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn xai_ask_user_question_translates_to_elicitation_when_client_has_form() {
    // Grok emits `_x.ai/ask_user_question`. A form-capable client must see
    // `elicitation/create` under the router session id; the mock agent gets
    // Grok's `{outcome, answers}` shape back.
    let state = temp_state_file("xai-ask");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, observed| {
        cx.send_request(
            InitializeRequest::new(ProtocolVersion::V1).client_capabilities(
                ClientCapabilities::new().elicitation(
                    ElicitationCapabilities::new().form(ElicitationFormCapabilities::new()),
                ),
            ),
        )
        .block_task()
        .await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let resp = prompt_text(&cx, &sid, "XAI_ASK:Which color?").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);

        let text = agent_text(&observed, &sid);
        assert!(
            text.contains(r#""outcome":"accepted""#),
            "mock should echo Grok's accepted outcome, got: {text}"
        );
        assert!(
            text.contains("Which color?"),
            "accepted answers should be keyed by the question text, got: {text}"
        );

        let observed = observed.lock().unwrap();
        assert_eq!(observed.elicitation_session_ids, vec![sid.clone()]);
        assert!(
            observed.xai_ask_session_ids.is_empty(),
            "form-capable client must not see the raw vendor method, got: {:?}",
            observed.xai_ask_session_ids
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn xai_ask_user_question_forwards_raw_without_form_capability() {
    // Clients that do not advertise form elicitation keep the raw Grok method.
    let state = temp_state_file("xai-ask-raw");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let resp = prompt_text(&cx, &sid, "XAI_ASK:Which color?").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);

        let text = agent_text(&observed, &sid);
        assert!(
            text.contains(r#""outcome":"accepted""#),
            "raw-forwarded vendor method should still complete, got: {text}"
        );

        let observed = observed.lock().unwrap();
        assert!(
            observed.elicitation_session_ids.is_empty(),
            "client without form elicitation must not receive elicitation/create, got: {:?}",
            observed.elicitation_session_ids
        );
        assert_eq!(observed.xai_ask_session_ids, vec![sid]);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn cancel_returns_cancelled_promptly() {
    let state = temp_state_file("cancel");
    let log = temp_log("cancel-peer");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[]),
        agent_yaml(
            "peer",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        for activity in ["", "TEXT:partial-before-cancel", "TOOL:edit"] {
            let sid = new_session(&cx).await?.session_id.0.to_string();
            let sent = cx.send_request(PromptRequest::new(
                sid.clone(),
                vec![ContentBlock::from(format!(
                    "[router: candidate=mock/m1]\n{activity}\nSLEEP:30000"
                ))],
            ));
            // Give routing time to pin and produce the requested activity.
            let cx2 = cx.clone();
            let sid2 = sid.clone();
            cx.spawn(async move {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                let _ = cx2.send_notification(CancelNotification::new(sid2));
                Ok(())
            })?;
            let resp = sent.block_task().await?;
            assert_eq!(resp.stop_reason, StopReason::Cancelled);
            let text = agent_text(&observed, &sid);
            if activity.starts_with("TEXT:") {
                assert!(
                    text.contains("partial-before-cancel"),
                    "cancellation follows streamed text"
                );
            } else if activity.starts_with("TOOL:") {
                assert!(
                    observed
                        .lock()
                        .unwrap()
                        .updates
                        .iter()
                        .any(|n| n.session_id.0.as_ref() == sid
                            && matches!(&n.update, SessionUpdate::ToolCall(_))),
                    "cancellation follows a completed tool"
                );
            }
            assert!(!text.contains("failover:"));
            assert!(
                !read_log(&log)
                    .iter()
                    .any(|entry| entry["event"] == "prompt")
            );
            let mut headroom = shared.headroom.lock().unwrap();
            assert!(headroom.cordon_active("mock").is_none());
            assert!(!headroom.is_quarantined(&CandidateId::new("mock", "m1")));
        }
        Ok(())
    })
    .await;
}

// ======================================================================
// Milestone 2: config, auth, candidate verification
// ======================================================================

#[tokio::test]
async fn auth_pending_agent_becomes_routeable_after_authenticate() {
    let state = temp_state_file("auth");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[("MOCK_AUTH_REQUIRED", "1")])
    );
    run_test(yaml, async |cx, observed| {
        let init_resp = init(&cx).await?;
        let ids: Vec<String> = init_resp
            .auth_methods
            .iter()
            .map(|m| m.id().0.to_string())
            .collect();
        assert_eq!(
            ids,
            vec!["mock/mock-login".to_string()],
            "namespaced auth methods"
        );

        // Management stays available before login. Model prompts still fail.
        let manage = new_session(&cx).await?;
        let manage_sid = manage.session_id.0.to_string();
        prompt_text(&cx, &manage_sid, "/usage").await?;
        assert!(agent_text(&observed, &manage_sid).contains("Usage"));
        assert!(!agent_text(&observed, &manage_sid).contains("echo:"));

        // Relay authenticate, then the candidate verifies and routing works.
        cx.send_request(AuthenticateRequest::new("mock/mock-login".to_string()))
            .block_task()
            .await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "after auth").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(agent_text(&observed, &sid).contains("echo:m1:after auth"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn native_account_priorities_drain_in_order_despite_sibling_headroom() {
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nrouter: auto\nagents:\n{}\n    accounts:\n      - {{name: first, priority: 0}}\n      - {{name: second, priority: 1}}\n      - {{name: third, priority: 2}}\n",
        temp_state_file("account-priority").display(),
        agent_yaml("claude", &[("sonnet", 2)], &[]).trim_end()
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        shared
            .headroom
            .lock()
            .unwrap()
            .set_polled_availability(std::collections::HashMap::from([
                (
                    CandidateId::new("claude@first", "sonnet"),
                    router_acp::headroom::SeatAvailability {
                        plan_headroom: 0.05,
                        on_overage: false,
                        plan_remaining_dollars: None,
                        overage_headroom: None,
                        overage_remaining_dollars: None,
                        source: "poll",
                    },
                ),
                (
                    CandidateId::new("claude@second", "sonnet"),
                    router_acp::headroom::SeatAvailability {
                        plan_headroom: 0.95,
                        on_overage: false,
                        plan_remaining_dollars: None,
                        overage_headroom: None,
                        overage_remaining_dollars: None,
                        source: "poll",
                    },
                ),
            ]));
        for name in ["first", "second", "third"] {
            let sid = new_session(&cx).await?.session_id.0.to_string();
            prompt_text(&cx, &sid, "fix a typo").await?;
            let selected = shared
                .with_session(&sid, |s| s.pin.as_ref().unwrap().candidate.agent.clone())
                .unwrap();
            assert_eq!(selected, format!("claude@{name}"));
            shared.headroom.lock().unwrap().cordon(
                &selected,
                Some(Duration::from_secs(300)),
                "account reserve ceiling reached",
            );
        }
        let sid = new_session(&cx).await?.session_id.0.to_string();
        assert!(prompt_text(&cx, &sid, "fix a typo").await.is_err());
        prompt_text(&cx, &sid, "/login").await?;
        assert!(agent_text(&observed, &sid).contains("claude (3 accounts)"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn router_login_text_menu_and_usage_never_prompt_a_model() {
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nagents:\n{}\n    accounts: [{{name: expired}}]\n",
        temp_state_file("native-menu").display(),
        agent_yaml("claude", &[("sonnet", 2)], &[]).trim_end()
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        shared.auth.lock().unwrap().set(
            "claude@expired",
            router_acp::auth::AuthAvailability::Unauthenticated {
                reason: "logged out".into(),
            },
        );
        prompt_text(&cx, &sid, "/login").await?;
        prompt_text(&cx, &sid, "1").await?;
        prompt_text(&cx, &sid, "1").await?;
        prompt_text(&cx, &sid, "/usage").await?;
        let text = agent_text(&observed, &sid);
        assert!(text.contains("claude (1 account)"));
        assert!(text.contains("Add Account"));
        assert!(text.contains("Re-login"));
        assert!(text.contains("Set priority"));
        // No usage source on this agent, so no reserve to set.
        assert!(!text.contains("Set reserve capacity"));
        assert!(text.contains("Delete account"));
        assert!(text.contains("auth: unknown"));
        assert!(!text.contains("auth: rejected"));
        assert!(!text.contains("echo:"));
        assert!(shared.with_session(&sid, |s| s.pin.is_none()).unwrap());
        prompt_text(&cx, &sid, "/cancel").await?;
        assert!(shared.account_menus.lock().unwrap().is_empty());
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn account_commands_survive_goose_turn_context_block() {
    // goose sends the typed command, then its `<turn-context>` preamble as a
    // second text block.
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nagents:\n{}\n    accounts: [{{name: work}}]\n",
        temp_state_file("native-menu-goose").display(),
        agent_yaml("claude", &[("sonnet", 2)], &[]).trim_end()
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        for command in ["/login", "1", "/usage"] {
            cx.send_request(PromptRequest::new(
                sid.clone(),
                vec![
                    ContentBlock::from(command.to_string()),
                    ContentBlock::from(
                        "<turn-context>\n<current-time>2026-10-07</current-time>\n</turn-context>"
                            .to_string(),
                    ),
                ],
            ))
            .block_task()
            .await?;
        }
        let text = agent_text(&observed, &sid);
        assert!(text.contains("claude (1 account)"), "{text}");
        assert!(text.contains("Add Account"), "{text}");
        assert!(!text.contains("echo:"), "a model was prompted: {text}");
        assert!(shared.with_session(&sid, |s| s.pin.is_none()).unwrap());
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn declared_model_missing_downstream_is_removed() {
    let state = temp_state_file("missing-model");
    // Config declares m1 and bogus; mock only offers m1.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1), ("bogus", 2)], &[]).replace(
            "MOCK_MODELS, value: \"m1,bogus\"",
            "MOCK_MODELS, value: \"m1\""
        )
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        // The router.candidate select must offer m1 but not bogus.
        let options = session.config_options.clone().unwrap_or_default();
        let candidate_opt = options
            .iter()
            .find(|o| o.id.0.as_ref() == "router.candidate")
            .expect("router.candidate option");
        let SessionConfigKind::Select(select) = &candidate_opt.kind else {
            panic!("router.candidate must be a select");
        };
        let values: Vec<String> = match &select.options {
            SessionConfigSelectOptions::Grouped(groups) => groups
                .iter()
                .flat_map(|g| g.options.iter().map(|o| o.value.0.to_string()))
                .collect(),
            SessionConfigSelectOptions::Ungrouped(opts) => {
                opts.iter().map(|o| o.value.0.to_string()).collect()
            }
            _ => vec![],
        };
        assert!(
            values.contains(&"mock/m1".to_string()),
            "values: {values:?}"
        );
        assert!(
            !values.contains(&"mock/bogus".to_string()),
            "values: {values:?}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn opus_1m_catalog_routes_when_selector_offers_bare_opus() {
    let state = temp_state_file("opus-1m-synonym");
    let log = temp_log("opus-1m-synonym");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nrouter: static\n\
         routers:\n  static: {{ candidate: \"mock/opus[1m]\" }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("opus[1m]", 4)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
        .replace(
            "MOCK_MODELS, value: \"opus[1m]\"",
            "MOCK_MODELS, value: \"opus\""
        )
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let options = session.config_options.clone().unwrap_or_default();
        let candidate_opt = options
            .iter()
            .find(|o| o.id.0.as_ref() == "router.candidate")
            .expect("router.candidate option");
        let SessionConfigKind::Select(select) = &candidate_opt.kind else {
            panic!("router.candidate must be a select");
        };
        let values: Vec<String> = match &select.options {
            SessionConfigSelectOptions::Grouped(groups) => groups
                .iter()
                .flat_map(|g| g.options.iter().map(|o| o.value.0.to_string()))
                .collect(),
            SessionConfigSelectOptions::Ungrouped(opts) => {
                opts.iter().map(|o| o.value.0.to_string()).collect()
            }
            _ => vec![],
        };
        assert!(
            values.contains(&"mock/opus[1m]".to_string()),
            "catalog id stays on the picker: {values:?}"
        );
        let sid = session.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "hello").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(
            agent_text(&observed, &sid).contains("echo:opus:hello"),
            "got: {}",
            agent_text(&observed, &sid)
        );
        let set_events: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "set_config_option")
            .collect();
        assert!(
            set_events.iter().any(|e| e["value"] == "opus"),
            "adapter must receive the offered spelling `opus`, not `opus[1m]`: {set_events:?}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn static_unrouteable_error_names_selector_miss() {
    let state = temp_state_file("opus-selector-miss");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nrouter: static\n\
         routers:\n  static: {{ candidate: \"mock/opus[1m]\" }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("opus[1m]", 4), ("sonnet", 2)], &[]).replace(
            "MOCK_MODELS, value: \"opus[1m],sonnet\"",
            "MOCK_MODELS, value: \"sonnet\""
        )
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        let err = prompt_text(&cx, &sid, "should fail").await.unwrap_err();
        let text = format!("{err}");
        assert!(
            text.contains("not offered by downstream model selector"),
            "expected selector-miss reason, got: {text}"
        );
        assert!(
            !text.to_lowercase().contains("authenticate")
                && !text.to_lowercase().contains("sign in"),
            "must not suggest re-auth when the miss is a selector spelling: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn session_new_advertises_candidate_effort_capabilities() {
    let state = temp_state_file("effort-capabilities");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-effort-capabilities-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "candidates:\n\
         \x20 - { pattern: 'mock/m1', effort_levels: [low, high, max], effort_mapping: { low: minimal, high: intensive, max: exhaustive } }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        scores.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let options = serde_json::to_string(&session.config_options).unwrap();
        assert!(
            options.contains(
                "\"capabilities\":{\"effort\":{\"supported\":[\"low\",\"high\",\"max\"]}}"
            ),
            "effort capabilities advertised: {options}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn zero_routeable_candidates_fails_initialize() {
    let state = temp_state_file("zero");
    let yaml = format!(
        "state_file: {}\nprobe_timeout_ms: 5000\ndelegation: {{ enabled: false }}\n\
         agents:\n  - name: broken\n    command: {{ type: stdio, command: /nonexistent-binary-xyz }}\n    \
         model_selection: {{ type: config-option }}\n    models: [{{ id: m1, cost_rank: 1 }}]\n",
        state.display(),
    );
    run_test(yaml, async |cx, _observed| {
        let err = init(&cx).await.unwrap_err();
        let text = format!("{err}");
        assert!(
            text.contains("zero routeable") || text.contains("routeable"),
            "expected config error, got: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn spawn_config_targets_one_process_per_model() {
    let state = temp_state_file("spawncfg");
    // spawn-config agent: each model gets its own process whose MOCK_MODELS
    // is fixed to that model via the process template.
    let yaml = format!(
        r#"state_file: {}
delegation: {{ enabled: false }}
router: static
routers:
  static: {{ candidate: codexish/m2 }}
agents:
  - name: codexish
    command:
      type: stdio
      command: {}
      env:
        - {{ name: MOCK_NAME, value: codexish }}
    model_selection:
      type: spawn-config
      process_template:
        env:
          MOCK_MODELS: "${{model_id}}"
    models:
      - {{ id: m1, cost_rank: 1 }}
      - {{ id: m2, cost_rank: 2 }}
"#,
        state.display(),
        mock_agent_exe()
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "spawned").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        // The m2 process target answered: its only model is m2.
        assert!(
            agent_text(&observed, &sid).contains("echo:m2:spawned"),
            "got: {}",
            agent_text(&observed, &sid)
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Milestone 3: lazy pin and router config options
// ======================================================================

#[tokio::test]
async fn no_downstream_session_before_first_prompt() {
    let state = temp_state_file("lazy");
    let log = temp_log("lazy");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let news = read_log(&log)
            .iter()
            .filter(|e| e["event"] == "session_new")
            .count();
        assert_eq!(
            news, 1,
            "only the probe session exists before the first prompt"
        );

        prompt_text(&cx, &sid, "now pin").await?;
        let news = read_log(&log)
            .iter()
            .filter(|e| e["event"] == "session_new")
            .count();
        assert_eq!(
            news, 2,
            "first prompt created exactly one downstream session"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn model_selection_is_applied_and_verified() {
    let state = temp_state_file("model-select");
    let log = temp_log("model-select");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nrouter: static\n\
         routers:\n  static: {{ candidate: mock/m2 }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1), ("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "which model").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(
            agent_text(&observed, &sid).contains("echo:m2:which model"),
            "downstream ran with the selected model: {}",
            agent_text(&observed, &sid)
        );
        let set_events: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "set_config_option")
            .collect();
        assert!(
            set_events.iter().any(|e| e["value"] == "m2"),
            "set_config_option with m2 expected: {set_events:?}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn silent_noop_model_selection_fails_cleanly() {
    let state = temp_state_file("noop-select");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nrouter: static\n\
         routers:\n  static: {{ candidate: mock/m2 }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1), ("m2", 2)],
            &[("MOCK_IGNORE_SET_CONFIG", "1")]
        )
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        let err = prompt_text(&cx, &sid, "should fail").await.unwrap_err();
        let text = format!("{err}");
        assert!(
            text.contains("silent no-op") || text.contains("verification"),
            "verification failure expected, got: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn pre_pin_candidate_override_and_post_pin_rejection() {
    let state = temp_state_file("override");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1), ("m2", 2)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        // Pre-pin: choose a concrete candidate via router.candidate.
        let set = cx
            .send_request(SetSessionConfigOptionRequest::new(
                sid.clone(),
                "router.candidate".to_string(),
                SessionConfigOptionValue::value_id("mock/m2"),
            ))
            .block_task()
            .await?;
        let candidate_opt = set
            .config_options
            .iter()
            .find(|o| o.id.0.as_ref() == "router.candidate")
            .expect("router.candidate present");
        let SessionConfigKind::Select(select) = &candidate_opt.kind else {
            panic!("select expected")
        };
        assert_eq!(select.current_value.0.as_ref(), "mock/m2");

        // Unknown config id errors.
        let err = cx
            .send_request(SetSessionConfigOptionRequest::new(
                sid.clone(),
                "no.such.option".to_string(),
                SessionConfigOptionValue::value_id("x"),
            ))
            .block_task()
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("unknown config option"),
            "got {err}"
        );

        prompt_text(&cx, &sid, "explicit").await?;
        assert!(agent_text(&observed, &sid).contains("echo:m2:explicit"));

        // Post-pin candidate changes are rejected.
        let err = cx
            .send_request(SetSessionConfigOptionRequest::new(
                sid.clone(),
                "router.candidate".to_string(),
                SessionConfigOptionValue::value_id("mock/m1"),
            ))
            .block_task()
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("already pinned"),
            "post-pin rejection expected, got {err}"
        );
        Ok(())
    })
    .await;
}

// goose sets its configured model on the session before the first prompt and
// aborts the run if the set is refused, which took down every scheduled recipe.
// `default`/`auto` carries no preference (router-acp picks), but a concrete
// candidate must be honored rather than silently dropped.
#[tokio::test]
async fn client_model_option_defers_to_router_or_pins_a_candidate() {
    let state = temp_state_file("client-model");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1), ("m2", 2)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;

        // `default` is accepted and leaves the router's own choice intact.
        let sid = new_session(&cx).await?.session_id.0.to_string();
        cx.send_request(SetSessionConfigOptionRequest::new(
            sid.clone(),
            "model".to_string(),
            SessionConfigOptionValue::value_id("default"),
        ))
        .block_task()
        .await?;

        // A concrete candidate is honored, exactly as router.candidate would be.
        let sid2 = new_session(&cx).await?.session_id.0.to_string();
        cx.send_request(SetSessionConfigOptionRequest::new(
            sid2.clone(),
            "model".to_string(),
            SessionConfigOptionValue::value_id("mock/m2"),
        ))
        .block_task()
        .await?;
        prompt_text(&cx, &sid2, "which model").await?;
        assert!(
            agent_text(&observed, &sid2).contains("echo:m2:which model"),
            "client-set model should pin the candidate: {}",
            agent_text(&observed, &sid2)
        );

        // An unusable value is refused rather than silently ignored.
        let sid3 = new_session(&cx).await?.session_id.0.to_string();
        let err = cx
            .send_request(SetSessionConfigOptionRequest::new(
                sid3,
                "model".to_string(),
                SessionConfigOptionValue::value_id("not-a-candidate"),
            ))
            .block_task()
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("candidate id"),
            "bad model value should be refused, got {err}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn explicit_effort_precedes_automatic_and_is_reresolved_on_failover() {
    let state = temp_state_file("effort-failover");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-effort-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "candidates:\n\
         \x20 - { pattern: 'a/*', default_quality: 3.0, effort_levels: [low, high], effort_mapping: { low: tiny, high: deep } }\n\
         \x20 - { pattern: 'b/*', default_quality: 1.0, effort_levels: [max], effort_mapping: { max: tiny } }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\nrouters:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 2)], &[("MOCK_FAIL_PROMPT_MSG", "rate limit")]),
        agent_yaml("b", &[("m2", 1)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        cx.send_request(SetSessionConfigOptionRequest::new(
            sid.clone(),
            "router.candidate".to_string(),
            SessionConfigOptionValue::value_id("a/m1"),
        ))
        .block_task()
        .await?;
        cx.send_request(SetSessionConfigOptionRequest::new(
            sid.clone(),
            "router.effort".to_string(),
            SessionConfigOptionValue::value_id("max"),
        ))
        .block_task()
        .await?;
        prompt_text(&cx, &sid, "a tiny typo").await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("effort: max → high"),
            "initial resolution: {text}"
        );
        assert!(
            text.contains("effort: max → max"),
            "failover resolution: {text}"
        );
        let routing = open_state(&state).get(&sid).unwrap().routing.unwrap();
        assert_eq!(routing["effort"]["requested"], "max");
        assert_eq!(routing["effort"]["resolved"], "max");
        assert_eq!(routing["effort"]["provider_value"], "tiny");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn auto_model_routing_requires_the_explicit_effort_level() {
    let state = temp_state_file("effort-auto-constraint");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-effort-auto-constraint-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "candidates:\n\
         \x20 - { pattern: 'a/*', default_quality: 3.0, effort_levels: [low], effort_mapping: { low: low } }\n\
         \x20 - { pattern: 'b/*', default_quality: 1.0, effort_levels: [high], effort_mapping: { high: high } }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\nrouters:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 2)], &[]),
        agent_yaml("b", &[("m2", 1)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        cx.send_request(SetSessionConfigOptionRequest::new(
            sid.clone(),
            "router.effort".to_string(),
            SessionConfigOptionValue::value_id("high"),
        ))
        .block_task()
        .await?;
        prompt_text(&cx, &sid, "implement this").await?;
        assert!(
            agent_text(&observed, &sid).contains("auto → b/m2"),
            "auto routing must exclude the higher-quality low-effort-only model"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn auto_model_routing_errors_when_no_candidate_supports_explicit_effort() {
    let state = temp_state_file("effort-auto-none");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-effort-auto-none-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "candidates:\n\
         \x20 - { pattern: 'mock/*', effort_levels: [low], effort_mapping: { low: low } }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        scores.display(),
        agent_yaml("mock", &[("m1", 1)], &[]),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        cx.send_request(SetSessionConfigOptionRequest::new(
            sid.clone(),
            "router.effort".to_string(),
            SessionConfigOptionValue::value_id("high"),
        ))
        .block_task()
        .await?;
        let err = prompt_text(&cx, &sid, "implement this").await.unwrap_err();
        assert!(
            format!("{err}")
                .contains("no routeable candidates support the explicitly requested effort `high`"),
            "clear no-compatible error: {err}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclassifier_omitted_effort_falls_back_to_task_depth_recommendation() {
    let state = temp_state_file("preclass-effort-fallback");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-preclass-effort-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "candidates:\n\
         \x20 - { pattern: 'mock/*', effort_levels: [xhigh], effort_mapping: { xhigh: deep } }\n",
    )
    .unwrap();
    let preclass = r#"{"routing":{"task_class":"Architecture","complexity":0.72,"confidence":0.9,"reason":"cross-system"}}"#;
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\npre_classifier:\n  enabled: true\n  evaluator: [\"*m1*\"]\nagents:\n{}",
        state.display(),
        scores.display(),
        agent_yaml("mock", &[("m1", 1)], &[("MOCK_PRECLASS_JSON", preclass)]),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "design the architecture").await?;
        let routing = open_state(&state).get(&sid).unwrap().routing.unwrap();
        assert_eq!(routing["effort"]["requested"], "xhigh");
        assert_eq!(routing["effort"]["provider_value"], "deep");
        Ok(())
    })
    .await;
}

/// A client that never sends `router-acp/delegate_mcp_catalogs` (goose running
/// a recipe holds no live connection into the session it created) used to fail
/// every pin whose pre-classifier asked for a capability. The host-supplied
/// env seed makes the same session pin and receive the catalog's servers.
#[tokio::test]
async fn env_seeded_catalogs_pin_a_session_that_never_registers_any() {
    let state = temp_state_file("seeded-catalogs");
    let log = temp_log("seeded-catalogs");
    let preclass = r#"{"routing":{"task_class":"Ops","complexity":0.2,"confidence":0.9,"reason":"needs telemetry"},"host_tools":{"confidence":0.9,"required_capabilities":["metrics"]}}"#;
    let yaml = format!(
        "state_file: {}\n\
         delegation:\n  enabled: false\n  mcp_catalogs:\n    - catalog: telemetry\n      capabilities: [metrics]\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*m1*\"]\n  dimensions:\n    - id: host_tools\n      description: host-defined tools\n      min_confidence: 0.7\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1)],
            &[
                ("MOCK_PRECLASS_JSON", preclass),
                ("MOCK_LOG", log.to_str().unwrap()),
            ],
        ),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        // Goose already supplies this server in session/new from its native
        // extension config. The separate catalog registration is still absent.
        let client_mcp = McpServer::Stdio(McpServerStdio::new("telemetry-mcp", "/bin/true"));
        let new_with_client_mcp = || {
            cx.send_request(
                NewSessionRequest::new(std::env::temp_dir()).mcp_servers(vec![client_mcp.clone()]),
            )
            .block_task()
        };

        // Today's behavior with client MCP but no registration or seed: the
        // catalog gate ignores the available server and refuses the pin.
        let unseeded = new_with_client_mcp().await?.session_id.0.to_string();
        let err = prompt_text(&cx, &unseeded, "check the error rate")
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("MCP catalog `telemetry` is not available in this session"),
            "unseeded session still fails closed: {err}"
        );

        // Same router process, same client, same absent notification — only the
        // host's env seed differs.
        let seed = serde_json::json!({ "telemetry": [client_mcp] }).to_string();
        unsafe { std::env::set_var("ROUTER_ACP_MCP_CATALOGS", seed) };
        let seeded = new_with_client_mcp().await?.session_id.0.to_string();
        prompt_text(&cx, &seeded, "check the error rate").await?;
        unsafe { std::env::remove_var("ROUTER_ACP_MCP_CATALOGS") };

        assert!(!open_state(&state).get(&seeded).unwrap().agent.is_empty());
        let attached = read_log(&log).into_iter().find(|event| {
            event["event"] == "session_new"
                && event["mcpServers"]
                    .as_array()
                    .is_some_and(|servers| servers.iter().any(|s| s == "telemetry-mcp"))
        });
        let servers = attached
            .and_then(|event| event["mcpServers"].as_array().cloned())
            .expect("the seeded catalog's server reached the agent");
        assert_eq!(
            servers
                .iter()
                .filter(|server| *server == "telemetry-mcp")
                .count(),
            1,
            "the client and catalog copies are structurally deduplicated"
        );
        Ok(())
    })
    .await;
}

/// The evaluator used to be allowed to invent arbitrary base-routing
/// capabilities. A prompt that literally named `mcp__datadog__` caused it to
/// emit `mcp_datadog`; with no matching host catalog, the router rejected the
/// turn before the already-attached client MCP could reach the downstream
/// agent. Only configured dimensions may create catalog requirements.
#[tokio::test]
async fn evaluator_cannot_invent_a_catalog_gate_for_client_mcp() {
    let state = temp_state_file("no-invented-catalog");
    let log = temp_log("no-invented-catalog");
    let preclass = r#"{"routing":{"task_class":"Ops","complexity":0.2,"confidence":0.9,"reason":"call the requested tool","required_capabilities":["mcp_datadog"]}}"#;
    let yaml = format!(
        "state_file: {}\n\
         delegation:\n  enabled: false\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*m1*\"]\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1)],
            &[
                ("MOCK_PRECLASS_JSON", preclass),
                ("MOCK_LOG", log.to_str().unwrap()),
            ],
        ),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let datadog = McpServer::Stdio(McpServerStdio::new("datadog", "/bin/true"));
        let sid = cx
            .send_request(NewSessionRequest::new(std::env::temp_dir()).mcp_servers(vec![datadog]))
            .block_task()
            .await?
            .session_id
            .0
            .to_string();

        prompt_text(&cx, &sid, "Call mcp__datadog__search_datadog_monitors").await?;

        let attached = read_log(&log).into_iter().find(|event| {
            event["event"] == "session_new"
                && event["mcpServers"]
                    .as_array()
                    .is_some_and(|servers| servers.iter().any(|server| server == "datadog"))
        });
        assert!(
            attached.is_some(),
            "the client Datadog MCP must reach the agent"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn post_pin_effort_config_and_directives_apply_without_ignore_notices() {
    let state = temp_state_file("post-pin-effort");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-post-pin-effort-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "candidates:\n\
         \x20 - { pattern: 'mock/*', effort_levels: [medium, high], effort_mapping: { medium: normal, high: deep } }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        scores.display(),
        agent_yaml("mock", &[("m1", 1)], &[]),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "first task").await?;
        for _ in 0..2 {
            cx.send_request(SetSessionConfigOptionRequest::new(
                sid.clone(),
                "router.effort".to_string(),
                SessionConfigOptionValue::value_id("high"),
            ))
            .block_task()
            .await?;
        }
        let resolution = shared
            .with_session(&sid, |session| session.resolved_effort.clone())
            .flatten()
            .expect("post-pin effort must resolve");
        assert_eq!(
            resolution.requested,
            router_acp::candidate::EffortLevel::High
        );
        assert_eq!(resolution.provider_value.as_deref(), Some("deep"));

        prompt_text(&cx, &sid, "[router: effort=high] follow-up one").await?;
        prompt_text(&cx, &sid, "[router: effort=high] follow-up two").await?;
        assert!(
            !agent_text(&observed, &sid).contains("routing directive ignored"),
            "idempotent post-pin effort directives must be silent"
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Milestone 4: classifier + auto
// ======================================================================

#[tokio::test]
async fn auto_pure_quality_routes_to_best_candidate() {
    let state = temp_state_file("auto-q");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml("fancy", &[("opus", 3)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        prompt_text(&cx, &sid, "hi").await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("fancy/opus"),
            "disclosure names fancy/opus: {text}"
        );
        assert!(text.contains("echo:opus:hi"), "opus answered: {text}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn auto_pure_cost_routes_to_cheapest() {
    let state = temp_state_file("auto-c");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 10, complexity_floor: 0.99 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml("fancy", &[("opus", 3)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        prompt_text(&cx, &sid, "hi").await?;
        let text = agent_text(&observed, &sid);
        assert!(text.contains("echo:haiku:hi"), "haiku answered: {text}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn capability_required_prompt_filters_candidates() {
    let state = temp_state_file("caps");
    // cheap lacks image support; fancy has it. Pure-cost routing would pick
    // cheap, but the image prompt filters it out.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 10, complexity_floor: 0.99 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml("fancy", &[("opus", 3)], &[("MOCK_CAPS_IMAGE", "1")])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        let resp = cx
            .send_request(PromptRequest::new(
                sid.clone(),
                vec![
                    ContentBlock::from("describe this image".to_string()),
                    ContentBlock::Image(ImageContent::new("aGk=", "image/png")),
                ],
            ))
            .block_task()
            .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:opus:"),
            "image-capable candidate answered: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn empty_capability_filtered_pool_is_clean_error() {
    let state = temp_state_file("caps-empty");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[])
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        let err = cx
            .send_request(PromptRequest::new(
                sid.clone(),
                vec![ContentBlock::Image(ImageContent::new("aGk=", "image/png"))],
            ))
            .block_task()
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("capabilities"),
            "capability error expected, got: {err}"
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Milestone 5: pareto-code and pre-prompt fallbacks
// ======================================================================

#[tokio::test]
async fn pareto_code_picks_cheapest_high_tier() {
    let state = temp_state_file("pareto");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nrouter: pareto-code\nagents:\n{}{}{}",
        state.display(),
        agent_yaml("a", &[("haiku", 1)], &[]),
        agent_yaml("b", &[("sonnet", 2)], &[]),
        agent_yaml("c", &[("opus", 3)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        prompt_text(&cx, &sid, "code stuff").await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:sonnet:"),
            "sonnet is the cheapest high-tier candidate: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn pre_prompt_failure_walks_fallback_chain() {
    let state = temp_state_file("fallback");
    // fancy ranks first (pure quality) but fails session/new after its probe;
    // routing must fall back to cheap and still answer.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml("fancy", &[("opus", 3)], &[("MOCK_FAIL_NEW_AFTER", "1")])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "resilient").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:haiku:resilient"),
            "fallback answered: {text}"
        );
        assert!(
            text.contains("cheap/haiku"),
            "disclosure shows the fallback: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn downstream_crash_after_pin_surfaces_error() {
    let state = temp_state_file("crash");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[("MOCK_EXIT_ON_PROMPT", "1")])
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        let err = prompt_text(&cx, &sid, "boom").await.unwrap_err();
        // No mid-session reroute: the crash surfaces as an error.
        assert!(!format!("{err}").is_empty());
        Ok(())
    })
    .await;
}

// ======================================================================
// Milestone 6: delegation
// ======================================================================

#[tokio::test]
async fn question_guidance_is_injected_on_every_prompt() {
    let log = temp_log("question-guidance");
    let yaml = format!(
        "agents:\n{}",
        agent_yaml(
            "mock",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "first task").await?;
        prompt_text(&cx, &sid, "second task").await?;

        let prompts: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|event| event["event"] == "prompt")
            .filter_map(|event| event["text"].as_str().map(str::to_string))
            .collect();
        assert_eq!(
            prompts.len(),
            2,
            "expected both downstream prompts: {prompts:?}"
        );
        assert!(
            prompts.iter().all(|text| {
                text.contains("[router-acp questions]")
                    && text.contains("structured question-asking tool/mechanism")
                    && text.contains("Do not ask the user only in prose")
            }),
            "question guidance must survive every turn: {prompts:?}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn ordinary_delegation_directive_is_scoped_one_shot_and_reinjected_after_switch() {
    let state = temp_state_file("delegate-prompt");
    let log = temp_log("delegate-prompt");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: true, inject_prompt: true }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1), ("m2", 2), ("m3", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    let state_path = state.clone();
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=mock/m2]\nfirst task").await?;
        prompt_text(&cx, &sid, "second turn").await?;
        prompt_text(&cx, &sid, "[router: switch=mock/m3]\nthird turn").await?;

        let prompted: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|event| event["event"] == "prompt")
            .filter_map(|event| event["text"].as_str().map(str::to_string))
            .filter(|text| text.contains("[router-acp delegation]"))
            .collect();
        assert_eq!(
            prompted.len(),
            2,
            "one directive per downstream model session: {prompted:?}"
        );
        assert!(
            prompted.iter().all(|text| {
                text.contains("bounded, independent work")
                    && text.contains("briefing and verification overhead")
                    && text.contains("never use provider-native Task/spawn/subagent tools")
            }),
            "directive carries the safety scope: {prompted:?}"
        );
        let db = open_state(&state_path);
        assert_eq!(
            db.get(&sid)
                .expect("parent row")
                .delegation_directive_injections,
            2,
            "telemetry records both real injections"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn ordinary_delegation_directive_requires_a_cheaper_available_worker() {
    let state = temp_state_file("delegate-prompt-cheapest");
    let log = temp_log("delegate-prompt-cheapest");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: true, inject_prompt: true }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1), ("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    let state_path = state.clone();
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=mock/m1]\ncheap task").await?;
        let prompts: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|event| event["event"] == "prompt")
            .filter_map(|event| event["text"].as_str().map(str::to_string))
            .collect();
        assert!(
            prompts
                .iter()
                .all(|text| !text.contains("[router-acp delegation]")),
            "no directive without an attached delegation tool: {prompts:?}"
        );
        assert!(
            open_state(&state_path)
                .get(&sid)
                .expect("parent row")
                .delegation_directive_injections
                == 0,
            "no injection telemetry without an injection"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn ordinary_native_subagent_bypass_is_reported() {
    let state = temp_state_file("delegate-prompt-bypass");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: true, inject_prompt: true }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1), ("m2", 2)], &[])
    );
    let state_path = state.clone();
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid,
            "[router: candidate=mock/m2]\nwork directly\nTOOL:mcp:Task",
        )
        .await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("delegation bypassed"),
            "bypass disclosed: {text}"
        );
        assert_eq!(
            open_state(&state_path)
                .get(&sid)
                .expect("parent row")
                .native_subagent_calls,
            1,
            "bypass recorded on the prompted parent"
        );
        Ok(())
    })
    .await;
}

fn delegation_yaml(
    state: &std::path::Path,
    log: &std::path::Path,
    max_concurrent: usize,
) -> String {
    let cheap = agent_yaml(
        "cheap",
        &[("haiku", 1)],
        &[
            ("MOCK_LOG", &log.display().to_string()),
            ("MOCK_SESSION_MODES", "default,bypassPermissions"),
        ],
    )
    .replace(
        "    models:\n",
        "    mode_map: { auto: bypassPermissions }\n    models:\n",
    );
    format!(
        "state_file: {}\ndelegation: {{ enabled: true, max_concurrent: {max_concurrent} }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        cheap,
        agent_yaml("fancy", &[("opus", 3)], &[])
    )
}

#[tokio::test]
async fn delegate_task_routes_to_lower_cost_candidate() {
    let state = temp_state_file("delegate");
    let log = temp_log("delegate");
    // The helper exe is this crate's router-acp binary.
    // SAFETY: test-scoped env mutation; tests using this run in one process
    // but the value is identical everywhere.
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(delegation_yaml(&state, &log, 3), async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let resp = prompt_text(
            &cx,
            &sid,
            "hard integration work\nDELEGATE:fix button color",
        )
        .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("delegate:[delegated to cheap/haiku]"),
            "tool result names the lower-cost candidate: {text}"
        );
        assert!(
            text.contains("echo:haiku:fix button color"),
            "sub-agent output captured into the tool result: {text}"
        );
        // The sub-agent transcript must not stream into the parent transcript
        // as its own updates: the echo text appears only inside the parent's
        // reply (delegate: line), not as a separate haiku echo update.
        let events = read_log(&log);
        let delegate_new = events
            .iter()
            .find(|e| e["event"] == "session_new" && e["count"] == 2)
            .expect("delegate session created on cheap agent");
        let servers: Vec<String> = delegate_new["mcpServers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(
            !servers.contains(&"router-delegate".to_string()),
            "no recursive delegate injection: {servers:?}"
        );
        assert!(
            events.iter().any(|e| {
                e["event"] == "set_mode"
                    && e["sessionId"] == delegate_new["sessionId"]
                    && e["modeId"] == "bypassPermissions"
            }),
            "delegate session receives the configured auto-mode mapping: {events:?}"
        );
        let db = open_state(&state);
        let delegate = db
            .all()
            .into_iter()
            .find(|(_, row)| row.parent_session_id.as_deref() == Some(&sid))
            .expect("delegate state row");
        let entries = db.log_for(&delegate.0, 50);
        assert!(entries.iter().any(|e| {
            e.kind == "delegate_task"
                && e.detail.as_ref().and_then(|d| d["task"].as_str()) == Some("fix button color")
        }));
        assert!(entries.iter().any(|e| e.kind == "agent_progress"));
        assert!(entries.iter().any(|e| {
            e.kind == "agent_response"
                && e.detail
                    .as_ref()
                    .and_then(|d| d["text"].as_str())
                    .is_some_and(|text| text.contains("echo:haiku:fix button color"))
        }));
        Ok(())
    })
    .await;
}

/// A frontier parent (`fancy/opus`, rank 3), a same-tier peer from another
/// agent (`peer/sonnet`, rank 3), and a cheaper sibling (`cheap/haiku`).
/// `extra` is spliced into the `delegation:` block.
fn hint_mode_yaml(state: &std::path::Path, extra: &str) -> String {
    format!(
        "state_file: {}\ndelegation: {{ enabled: true, max_concurrent: 3{extra} }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml("peer", &[("sonnet", 3)], &[]),
        agent_yaml("fancy", &[("opus", 3)], &[]),
    )
}

/// Hook events appended to `path`, one JSON object per line. Stop events are
/// fired after the tool call returns, so wait (bounded) for `want` of them.
async fn hook_events(path: &std::path::Path, want: usize) -> Vec<serde_json::Value> {
    for _ in 0..100 {
        let events: Vec<serde_json::Value> = std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        if events.len() >= want {
            return events;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!(
        "lifecycle hook did not record {want} events in {}",
        path.display()
    );
}

#[tokio::test]
async fn prefer_hints_never_leave_the_cost_scoped_pool() {
    let state = temp_state_file("hint-prefer");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(hint_mode_yaml(&state, ""), async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid,
            "hard integration work\nDELEGATE_HINT:peer/sonnet\nDELEGATE:review the patch",
        )
        .await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("delegate:[delegated to cheap/haiku]"),
            "default `prefer` keeps the same-tier hint out of the cheaper pool: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn exact_hints_reach_a_same_tier_model_or_fail_without_substituting() {
    let state = temp_state_file("hint-exact");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(
        hint_mode_yaml(&state, ", candidate_hints: exact"),
        async |cx, observed| {
            init(&cx).await?;
            let session = new_session(&cx).await?;
            let sid = session.session_id.0.to_string();
            prompt_text(
                &cx,
                &sid,
                "hard integration work\nDELEGATE_HINT:peer/sonnet\nDELEGATE:implement the plan",
            )
            .await?;
            let text = agent_text(&observed, &sid);
            assert!(
                text.contains("delegate:[delegated to peer/sonnet]")
                    && text.contains("echo:sonnet:implement the plan"),
                "exact hint reaches the same-tier peer of another agent: {text}"
            );

            let second = new_session(&cx).await?;
            let sid2 = second.session_id.0.to_string();
            prompt_text(
                &cx,
                &sid2,
                "hard integration work\nDELEGATE_HINT:peer/missing\nDELEGATE:implement the plan",
            )
            .await?;
            let text = agent_text(&observed, &sid2);
            assert!(
                text.contains("delegate-error:") && text.contains("not substituting"),
                "an unavailable exact hint fails instead of falling back: {text}"
            );
            assert!(
                !text.contains("[delegated to"),
                "no other model ran the task: {text}"
            );
            Ok(())
        },
    )
    .await;
}

#[tokio::test]
async fn lifecycle_hook_registers_each_delegate_before_it_runs() {
    let state = temp_state_file("hook");
    let events = std::env::temp_dir().join(format!(
        "router-acp-hook-{}.jsonl",
        uuid::Uuid::new_v4().simple()
    ));
    let hook = format!(
        ", candidate_hints: exact, lifecycle_hook: {{ command: /bin/sh, \
         args: [\"-c\", \"cat >> '{}'; echo >> '{}'\"] }}",
        events.display(),
        events.display()
    );
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(hint_mode_yaml(&state, &hook), async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid,
            "hard integration work\nDELEGATE_HINT:peer/sonnet\nDELEGATE_EFFORT:low\n\
             DELEGATE:implement the plan",
        )
        .await?;
        let text = agent_text(&observed, &sid);
        let recorded = hook_events(&events, 3).await;
        let (start, turn_end, stop) = (&recorded[0], &recorded[1], &recorded[2]);
        assert_eq!(start["event"], "delegate_start");
        assert_eq!(turn_end["event"], "delegate_turn_end");
        assert_eq!(turn_end["turn"], 1);
        assert!(
            turn_end["last_message"]
                .as_str()
                .is_some_and(|t| t.contains("echo:sonnet")),
            "turn end carries the worker's text: {turn_end}"
        );
        assert_eq!(stop["event"], "delegate_stop");
        assert_eq!(stop["outcome"], "completed");
        assert!(start["router_pid"].as_u64().is_some_and(|pid| pid > 0));
        assert!(
            start["router_started_at_ms"]
                .as_u64()
                .is_some_and(|t| t > 0)
        );
        let worker = start["worker_id"].as_str().unwrap();
        assert!(worker.starts_with("w-"), "foreground worker id: {worker}");
        assert_eq!(stop["worker_id"], start["worker_id"]);
        assert_eq!(start["candidate"], "peer/sonnet");
        assert_eq!(start["parent_candidate"], "fancy/opus");
        assert_eq!(start["effort"], "low");
        let parent_downstream = open_state(&state)
            .all()
            .into_iter()
            .find(|(id, _)| id == &sid)
            .map(|(_, row)| row.downstream_session_id)
            .expect("parent row");
        assert_eq!(start["parent_downstream_session_id"], parent_downstream);
        assert!(
            text.contains(&format!("[delegated to peer/sonnet, worker {worker}]"))
                && text.contains(&format!("[router-acp delegate] worker id: {worker}")),
            "the worker sees and reports its registered id: {text}"
        );
        Ok(())
    })
    .await;
}

/// A lifecycle hook that appends each event it accepts to `events` and runs
/// `gate` (shell) first; the gate may `exit 2` with a message.
fn recording_hook(events: &std::path::Path, gate: &str) -> String {
    let script = format!(
        "e=$(cat); {gate} printf '%s\\n' \"$e\" >> '{}'",
        events.display()
    );
    format!(
        ", candidate_hints: exact, lifecycle_hook: {{ command: /bin/sh, args: [\"-c\", {}] }}",
        serde_json::to_string(&script).unwrap()
    )
}

fn temp_events(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "router-acp-hook-{tag}-{}.jsonl",
        uuid::Uuid::new_v4().simple()
    ))
}

#[tokio::test]
async fn turn_end_hook_sends_the_worker_back_until_it_releases() {
    let state = temp_state_file("turn-gate");
    let events = temp_events("turn-gate");
    // Return the first turn; release every later one.
    let gate = "case \"$e\" in *'\"event\":\"delegate_turn_end\"'*'\"turn\":1,'*) \
                echo 'push your branch' >&2; exit 2;; esac;";
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(
        hint_mode_yaml(&state, &recording_hook(&events, gate)),
        async |cx, observed| {
            init(&cx).await?;
            let sid = new_session(&cx).await?.session_id.0.to_string();
            prompt_text(
                &cx,
                &sid,
                "hard integration work\nDELEGATE_HINT:peer/sonnet\nDELEGATE:implement the plan",
            )
            .await?;
            let text = agent_text(&observed, &sid);
            assert!(
                text.contains("the host returned the turn: push your branch")
                    && text.contains("echo:sonnet:push your branch"),
                "the worker received the host's message and answered it: {text}"
            );
            let recorded = hook_events(&events, 3).await;
            let kinds: Vec<_> = recorded.iter().map(|e| e["event"].clone()).collect();
            assert_eq!(
                kinds,
                ["delegate_start", "delegate_turn_end", "delegate_stop"],
                "turn 1 was returned (not recorded); turn 2 released: {recorded:?}"
            );
            assert_eq!(recorded[1]["turn"], 2);
            Ok(())
        },
    )
    .await;
}

#[tokio::test]
async fn workers_name_themselves_hand_off_and_are_followed_up_in_the_background() {
    let state = temp_state_file("worker-tools");
    let events = temp_events("worker-tools");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(
        hint_mode_yaml(&state, &recording_hook(&events, "")),
        async |cx, observed| {
            init(&cx).await?;
            let sid = new_session(&cx).await?.session_id.0.to_string();
            let task = "MCP_CALL:router-worker worker_whoami {}\n\
                        MCP_CALL:router-worker worker_handoff {\"kind\":\"waiting\",\"message\":\"CI build 12\"}";
            let call = serde_json::json!({
                "task": task,
                "hints": {"candidate": "peer/sonnet"},
                "keep_open": true,
            });
            prompt_text(
                &cx,
                &sid,
                &format!("hard integration work\nMCP_CALL:router-delegate delegate_task {call}"),
            )
            .await?;
            let text = agent_text(&observed, &sid);
            let worker = regex::Regex::new(r"worker id: (w-[0-9a-f]+)")
                .unwrap()
                .captures(&text)
                .map(|c| c[1].to_string())
                .unwrap_or_else(|| panic!("worker_whoami answered: {text}"));
            assert!(
                text.contains("Recorded `waiting` handoff"),
                "worker_handoff accepted: {text}"
            );
            let delegate_id = regex::Regex::new(r"delegate_id: (d-[0-9a-f]+)")
                .unwrap()
                .captures(&text)
                .map(|c| c[1].to_string())
                .expect("keep_open returns a delegate_id");
            let recorded = hook_events(&events, 2).await;
            assert_eq!(recorded[1]["event"], "delegate_turn_end");
            assert_eq!(recorded[1]["worker_id"], worker.as_str());
            assert_eq!(recorded[1]["handoff"]["kind"], "waiting");
            assert_eq!(recorded[1]["handoff"]["message"], "CI build 12");

            // Answer it in the background, collect it, then re-read it.
            let followup = serde_json::json!({
                "delegate_id": delegate_id, "message": "carry on", "background": true,
            });
            let result = serde_json::json!({"delegate_id": delegate_id});
            prompt_text(
                &cx,
                &sid,
                &format!(
                    "MCP_CALL:router-delegate delegate_followup {followup}\n\
                     MCP_CALL:router-delegate delegate_await {{}}\n\
                     MCP_CALL:router-delegate delegate_result {result}\n\
                     MCP_CALL:router-delegate delegate_close {result}"
                ),
            )
            .await?;
            let text = agent_text(&observed, &sid);
            assert!(
                text.contains("mcp:delegate_followup:[background delegate b-"),
                "background follow-up acked at once: {text}"
            );
            assert!(
                text.contains("echo:sonnet:carry on"),
                "delegate_await collected the follow-up turn: {text}"
            );
            assert!(
                text.contains(&format!("delegate {delegate_id} on peer/sonnet — open (keep_open)")),
                "delegate_result re-reads the latest output: {text}"
            );
            let recorded = hook_events(&events, 4).await;
            assert_eq!(recorded[2]["event"], "delegate_turn_end");
            assert_eq!(recorded[2]["turn"], 2);
            assert!(recorded[2].get("handoff").is_none(), "a new turn clears the handoff");
            let stop = hook_events(&events, 4).await;
            assert_eq!(stop[3]["event"], "delegate_stop");
            assert_eq!(stop[3]["outcome"], "closed");
            Ok(())
        },
    )
    .await;
}

#[tokio::test]
async fn a_failover_reports_the_parents_new_provider_session() {
    let state = temp_state_file("repin");
    let events = temp_events("repin");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: true{} }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        recording_hook(&events, ""),
        agent_yaml(
            "fancy",
            &[("opus", 3)],
            &[(
                "MOCK_FAIL_PROMPT_MSG",
                "rate limit reached, try again in 30 minutes"
            )]
        ),
        agent_yaml("peer", &[("sonnet", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "hard integration work").await?;
        let text = agent_text(&observed, &sid);
        assert!(text.contains("echo:sonnet"), "failed over: {text}");
        let recorded = hook_events(&events, 1).await;
        let repin = &recorded[0];
        assert_eq!(repin["event"], "parent_repinned");
        assert_eq!(repin["parent_router_session_id"], sid.as_str());
        assert_eq!(repin["previous_candidate"], "fancy/opus");
        assert_eq!(repin["candidate"], "peer/sonnet");
        assert_eq!(repin["reason"], "failover");
        assert_ne!(
            repin["previous_downstream_session_id"],
            repin["downstream_session_id"]
        );
        let pinned = open_state(&state).get(&sid).unwrap().downstream_session_id;
        assert_eq!(repin["downstream_session_id"], pinned.as_str());
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn max_delegates_bounds_one_agents_concurrent_workers() {
    let state = temp_state_file("agent-slots");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let yaml = hint_mode_yaml(&state, ", candidate_hints: exact")
        .replace("  - name: peer\n", "  - name: peer\n    max_delegates: 1\n");
    assert!(yaml.contains("max_delegates: 1"), "{yaml}");
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let started = std::time::Instant::now();
        prompt_text(
            &cx,
            &sid,
            "hard integration work\nDELEGATE_HINT:peer/sonnet\nDELEGATE:SLEEP:400\nDELEGATE:SLEEP:400",
        )
        .await?;
        let text = agent_text(&observed, &sid);
        assert_eq!(text.matches("slept:400").count(), 2, "{text}");
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(800),
            "one slot runs the two workers one after the other ({:?})",
            started.elapsed()
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn lifecycle_hook_refusal_fails_the_delegation() {
    let state = temp_state_file("hook-refuse");
    let hook =
        ", lifecycle_hook: { command: /bin/sh, args: [\"-c\", \"echo no-slot >&2; exit 3\"] }";
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(hint_mode_yaml(&state, hook), async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid,
            "hard integration work\nDELEGATE:fix button color",
        )
        .await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("delegate-error:")
                && text.contains("refused")
                && text.contains("no-slot"),
            "a refused start fails the call with the hook's reason: {text}"
        );
        assert!(
            !text.contains("echo:haiku"),
            "the delegate never ran: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn parent_lists_delegate_tools_during_session_new_before_pin() {
    // Codex lists MCP tools during session/new, which finishes before the
    // router commits session.pin. If tools/list keyed off the pin, the parent
    // would see only background_start.
    let state = temp_state_file("delegate-tools-list");
    let log = temp_log("delegate-tools-list");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let cheap = agent_yaml(
        "cheap",
        &[("haiku", 1)],
        &[("MOCK_LOG", &log.display().to_string())],
    );
    let fancy = agent_yaml(
        "fancy",
        &[("opus", 3)],
        &[
            ("MOCK_LOG", &log.display().to_string()),
            ("MOCK_LIST_DELEGATE_TOOLS", "1"),
        ],
    );
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: true, max_concurrent: 3 }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        cheap,
        fancy
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        // Pin opens the downstream session; Codex lists MCP tools inside
        // that session/new, before the router commits session.pin.
        prompt_text(&cx, &sid, "hard integration work").await?;
        let events = read_log(&log);
        // Probe sessions do not receive the delegate MCP server; the pinned
        // session/new does, and that is the call that races the pin commit.
        let listed = events
            .iter()
            .filter(|e| e["event"] == "delegate_tools")
            .find(|e| e.get("error").is_none())
            .expect("pinned session listed router-delegate tools during session/new");
        let names: Vec<&str> = listed["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        for required in [
            "delegate_task",
            "delegate_await",
            "delegate_followup",
            "delegate_close",
        ] {
            assert!(
                names.contains(&required),
                "session/new tools/list omitted {required}: {names:?}"
            );
        }
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn delegate_without_required_auto_mode_fails_closed() {
    let state = temp_state_file("delegate-mode-required");
    let log = temp_log("delegate-mode-required");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    // Cheap advertises modes but none resolve to a non-interactive `auto`
    // (no mode_map, no exact "auto" id). That is the hang risk: the agent
    // would prompt for approval. Agents that advertise *no* modes at all
    // (Grok) are allowed through — there is no permission gate to arm.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: true, max_concurrent: 1 }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "cheap",
            &[("haiku", 1)],
            &[
                ("MOCK_LOG", &log.display().to_string()),
                ("MOCK_SESSION_MODES", "default,plan"),
            ],
        ),
        agent_yaml("fancy", &[("opus", 3)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid,
            "hard integration work\nDELEGATE:fix button color",
        )
        .await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("delegate-error:") && text.contains("no configured auto mode"),
            "delegate must fail instead of prompting under a default mode: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn five_bug_acceptance_scenario() {
    // First prompt routes by auto to the high-quality parent; the parent
    // delegates three isolated fixes which run on the lower-cost candidate
    // under bounded concurrency; the parent integrates the results.
    let state = temp_state_file("fivebug");
    let log = temp_log("fivebug");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(delegation_yaml(&state, &log, 2), async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let prompt = "fix these 5 bugs across the architecture\n\
                      DELEGATE:fix css bug one\n\
                      DELEGATE:fix copy bug two\n\
                      DELEGATE:fix css bug three";
        let resp = prompt_text(&cx, &sid, prompt).await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);

        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("fancy/opus"),
            "parent routed to high quality: {text}"
        );
        for bug in ["fix css bug one", "fix copy bug two", "fix css bug three"] {
            assert!(
                text.contains(&format!("echo:haiku:{bug}")),
                "delegated fix `{bug}` completed on the cheap candidate: {text}"
            );
        }
        // All three delegated subtasks ran in ephemeral sessions on the
        // cheap agent (probe + 3 delegates = 4 session_new events).
        let news = read_log(&log)
            .iter()
            .filter(|e| e["event"] == "session_new")
            .count();
        assert_eq!(news, 4, "three ephemeral delegate sessions were created");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn background_delegates_run_in_parallel_and_await_collects() {
    // Two `background: true` delegations return immediately and execute
    // CONCURRENTLY (each sub-agent sleeps ~900ms; a serial run would take
    // ≥1800ms just sleeping), then one `delegate_await` collects both results.
    let state = temp_state_file("delegate-bg");
    let log = temp_log("delegate-bg");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(delegation_yaml(&state, &log, 3), async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let prompt = "parallel work\n\
                      DELEGATE_BG:SLEEP:900\n\
                      DELEGATE_BG:SLEEP:901\n\
                      AWAIT_DELEGATES";
        let resp = prompt_text(&cx, &sid, prompt).await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);

        let text = agent_text(&observed, &sid);
        assert_eq!(
            text.matches("delegate-bg:[background delegate b-").count(),
            2,
            "both background starts acked immediately: {text}"
        );
        for slept in ["slept:900", "slept:901"] {
            assert!(
                text.contains(&format!("— done ===\n[delegated to cheap/haiku]\n{slept}")),
                "await collected `{slept}`: {text}"
            );
        }
        assert!(
            text.contains("All requested background delegates have completed"),
            "{text}"
        );
        // The parallelism assertion proper: both delegate prompts begin
        // together. This excludes debug helper-process startup from the
        // measurement while still failing a serial implementation (~900ms
        // between starts).
        let delegate_starts: Vec<u64> = read_log(&log)
            .iter()
            .filter(|entry| {
                entry["event"] == "prompt"
                    && entry["text"]
                        .as_str()
                        .is_some_and(|text| text == "SLEEP:900" || text == "SLEEP:901")
            })
            .filter_map(|entry| entry["startedAtMs"].as_u64())
            .collect();
        assert_eq!(delegate_starts.len(), 2, "{delegate_starts:?}");
        let start_gap = delegate_starts[0].abs_diff(delegate_starts[1]);
        assert!(
            start_gap < 300,
            "background delegates must overlap (start gap {start_gap}ms)"
        );

        // Everything was consumed — a bare await now reports the misuse.
        let resp = prompt_text(&cx, &sid, "AWAIT_DELEGATES").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("await-error:no background delegates are pending"),
            "{text}"
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Token limits, outages, failover, disclosures
// ======================================================================

#[tokio::test]
async fn disclosure_explains_why_the_model_was_chosen() {
    let state = temp_state_file("why");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml("fancy", &[("opus", 3)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        prompt_text(&cx, &sid, "hello").await?;
        let text = agent_text(&observed, &sid);
        // The console line names the strategy, candidate, task class, AND
        // the strategy math explaining the choice.
        assert!(text.contains("router-acp · auto → "), "got: {text}");
        assert!(text.contains("utility"), "why-math shown: {text}");
        assert!(text.contains("quality"), "why-math shown: {text}");
        assert!(
            text.contains("task CodingGeneral"),
            "task class shown: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn rate_limit_with_reset_time_cordons_agent_and_fails_over() {
    let state = temp_state_file("ratelimit");
    let log = temp_log("ratelimit");
    // fancy wins on quality but every prompt hits a token limit whose error
    // reports a reset one hour out (Claude Code's `...|<epoch>` format).
    let reset_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "cheap",
            &[("haiku", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "fancy",
            &[("opus", 3)],
            &[(
                "MOCK_FAIL_PROMPT_MSG",
                &format!("Claude AI usage limit reached|{reset_epoch}")
            )]
        )
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        // Prompt: fancy is chosen, hits the limit, cheap answers.
        let resp = prompt_text(&cx, &sid, "first question").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("fancy/opus unavailable — token/usage limit"),
            "user told about the limit: {text}"
        );
        assert!(
            text.contains("model reports reset in ~"),
            "reset time surfaced: {text}"
        );
        assert!(text.contains("failing over"), "failover announced: {text}");
        assert!(
            text.contains("failover: auto → cheap/haiku"),
            "fallback choice disclosed: {text}"
        );
        assert!(
            text.contains("context from earlier turns does not transfer"),
            "context caveat shown: {text}"
        );
        assert!(
            text.contains("echo:haiku:first question"),
            "answer came: {text}"
        );

        // A NEW session must route straight to cheap: fancy is cordoned
        // until its reset time, and the user is told.
        let session2 = new_session(&cx).await?;
        let sid2 = session2.session_id.0.to_string();
        prompt_text(&cx, &sid2, "second question").await?;
        let text2 = agent_text(&observed, &sid2);
        assert!(
            text2.contains("auto → cheap/haiku"),
            "cordoned agent skipped at ranking: {text2}"
        );
        assert!(
            text2.contains("fancy is cordoned: token/usage limit"),
            "cordon disclosed on later sessions: {text2}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn outage_fails_over_and_respawn_allows_recovery() {
    let state = temp_state_file("outage");
    // fancy crashes on every prompt; respawn cooldown 0 lets each new pin
    // revive it (proving respawn works), then failover lands on cheap.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         failover: {{ respawn_cooldown_secs: 0 }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml("fancy", &[("opus", 3)], &[("MOCK_EXIT_ON_PROMPT", "1")])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;

        // Session 1: fancy pins, crashes, failover to cheap.
        let s1 = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &s1, "one").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &s1);
        assert!(
            text.contains("fancy/opus unavailable — outage"),
            "got: {text}"
        );
        assert!(text.contains("failover: auto → cheap/haiku"), "got: {text}");
        assert!(text.contains("echo:haiku:one"), "got: {text}");

        // Session 2: the router revived fancy (respawn), pins it again,
        // it crashes again, and failover still lands on cheap. Pinning
        // fancy a second time is only possible because respawn worked.
        let s2 = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &s2, "two").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text2 = agent_text(&observed, &s2);
        assert!(
            text2.contains("auto → fancy/opus"),
            "fancy revived and re-pinned after respawn: {text2}"
        );
        assert!(text2.contains("echo:haiku:two"), "got: {text2}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn mid_session_rate_limit_fails_over_on_later_prompt() {
    // The pinned model works for the first turn, then hits its token limit
    // on the second: the RELAY path (not just the first-prompt pin path)
    // must cordon it and fail the session over.
    let state = temp_state_file("mid-limit");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml(
            "fancy",
            &[("opus", 3)],
            &[
                (
                    "MOCK_FAIL_PROMPT_MSG",
                    "rate limit reached, try again in 30 minutes"
                ),
                ("MOCK_FAIL_PROMPT_AFTER", "1"),
            ]
        )
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();

        // Turn 1 works on fancy.
        let resp = prompt_text(&cx, &sid, "turn one").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(agent_text(&observed, &sid).contains("echo:opus:turn one"));

        // Turn 2 hits the limit mid-session and fails over to cheap.
        let resp = prompt_text(&cx, &sid, "turn two").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("fancy/opus unavailable — token/usage limit"),
            "mid-session limit disclosed: {text}"
        );
        assert!(
            text.contains("model reports reset in ~30m"),
            "parsed reset time shown: {text}"
        );
        assert!(
            text.contains("failover: auto → cheap/haiku"),
            "mid-session failover disclosed: {text}"
        );
        // The replacement is seeded with the transcript, then the turn.
        let answer = text.rsplit("echo:haiku:").next().unwrap_or_default();
        assert!(
            answer.contains("turn one") && answer.contains("turn two"),
            "answer arrived with prior context: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn context_overflow_fails_over_to_a_larger_window_and_carries_a_transcript() {
    // The dead-end this reproduces: a resumed session pinned on a small-window
    // model hits `Compacting failed: too_few_groups` then `Prompt is too long`.
    // Before ContextOverflow existed this classified as `Other`, which
    // suppresses failover outright — the turn just errored. It must now fail
    // over, and specifically toward the candidate with the ROOMIER window
    // (never the same-size one, which would hit the identical wall), seeding
    // the fresh session with a transcript so it isn't blind.
    let state = temp_state_file("ctx-overflow");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"fancy/*\", context_window: 50000, default_quality: 0.9 }\n\
         \x20 - { pattern: \"cheap/*\", context_window: 1000000, default_quality: 0.1 }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml(
            "fancy",
            &[("opus", 3)],
            &[
                ("MOCK_FAIL_PROMPT_MSG", "Compacting failed: too_few_groups"),
                ("MOCK_FAIL_PROMPT_AFTER", "1"),
            ]
        ),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();

        // Turn 1 works on fancy (the higher-quality, smaller-window pick).
        let resp = prompt_text(&cx, &sid, "turn one").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(agent_text(&observed, &sid).contains("echo:opus:turn one"));

        // Turn 2 overflows fancy's window and must fail over to cheap — the
        // ONLY other candidate, and it happens to have the larger window, so
        // this also exercises the larger-context preference.
        let resp = prompt_text(&cx, &sid, "turn two").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("fancy/opus could not fit this turn in its context window"),
            "overflow disclosed with the right symptom, not generic \"unavailable\": {text}"
        );
        assert!(
            text.contains("context overflow"),
            "human-readable reason surfaced: {text}"
        );
        assert!(
            !text.contains("token/usage limit"),
            "must not be cordoned as a usage limit — the model is healthy: {text}"
        );
        assert!(
            text.contains("failover: auto → cheap/haiku"),
            "failed over to the larger-window candidate: {text}"
        );
        assert!(
            text.contains("carrying a truncated transcript"),
            "fresh session told it isn't starting blind: {text}"
        );
        assert!(
            text.contains("prior context carried over as a truncated transcript"),
            "the failover note reflects that context DID transfer here: {text}"
        );
        assert!(
            !text.contains("context from earlier turns does not"),
            "the generic cold-failover note must not contradict the handoff: {text}"
        );
        // The new pin answered, and what it received began with the handoff
        // block carrying the earlier turn — not a blind `turn two`.
        assert!(text.contains("echo:haiku:"), "answer arrived: {text}");
        assert!(
            text.contains("Assistant: echo:opus:turn one"),
            "the earlier turn reached the new model: {text}"
        );

        // fancy must not be cordoned/quarantined by the overflow: a later
        // session should still be able to pin it fresh (no accumulated
        // transcript, so no reason to avoid it).
        let sid2 = new_session(&cx).await?.session_id.0.to_string();
        let resp2 = prompt_text(&cx, &sid2, "fresh session").await?;
        assert_eq!(resp2.stop_reason, StopReason::EndTurn);
        assert!(
            agent_text(&observed, &sid2).contains("auto → fancy/opus"),
            "fancy still routeable after an overflow (not cordoned): {}",
            agent_text(&observed, &sid2)
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn hot_failover_after_output_carries_partial_text_and_completed_tools() {
    let state = temp_state_file("hot-failover");
    let log = temp_log("hot-failover");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "cheap",
            &[("haiku", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml("fancy", &[("opus", 3)], &[("MOCK_EXIT_AFTER_OUTPUT", "1")])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let response =
            prompt_text(&cx, &sid, "Complete the change once\nTOOL:exec:write-file").await?;
        assert_eq!(response.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(text.contains("partial output before crash"), "got: {text}");
        assert!(
            text.contains("failover: auto → cheap/haiku"),
            "hot failover disclosed: {text}"
        );
        let prompts: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "prompt")
            .collect();
        assert_eq!(
            prompts.len(),
            1,
            "replacement should receive one continuation"
        );
        let handoff = prompts[0]["text"].as_str().unwrap();
        assert!(
            handoff.contains("Hot failover:")
                && handoff.contains("Do not repeat completed actions")
                && handoff.contains("partial output before crash"),
            "successor receives partial text and continuation instructions: {handoff}"
        );
        assert!(
            handoff.contains("Tool: Bash [completed]") && handoff.contains("write-file"),
            "tool evidence carried: {handoff}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn chained_hot_failover_preserves_completed_tools_across_a_silent_failure() {
    let state = temp_state_file("chained-hot-failover");
    let log = temp_log("chained-hot-failover");
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nheadroom: {{quarantine_failures: 1}}\nrouters:\n  auto: {{cost_quality_tradeoff: 0}}\nagents:\n{}{}{}",
        state.display(),
        agent_yaml("a", &[("opus", 3)], &[("MOCK_EXIT_AFTER_TOOLS", "1")]),
        agent_yaml("b", &[("sonnet", 2)], &[("MOCK_EXIT_ON_PROMPT", "1")]),
        agent_yaml(
            "c",
            &[("haiku", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let response =
            prompt_text(&cx, &sid, "Complete the change once\nTOOL:exec:write-file").await?;
        assert_eq!(response.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("failover: auto → b/sonnet") && text.contains("failover: auto → c/haiku"),
            "both failovers occurred: {text}"
        );
        let prompts: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|entry| entry["event"] == "prompt")
            .collect();
        assert_eq!(prompts.len(), 1);
        let handoff = prompts[0]["text"].as_str().unwrap();
        assert!(
            handoff.contains("Hot failover:")
                && handoff.contains("Tool: Bash [completed]")
                && handoff.contains("write-file"),
            "third model retains the original tool evidence: {handoff}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn downstream_death_between_turns_fails_over_with_prior_context() {
    let state = temp_state_file("idle-death");
    let log = temp_log("idle-death-peer");
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nfailover: {{respawn_cooldown_secs: 3600}}\nagents:\n{}{}",
        state.display(),
        agent_yaml("a", &[("opus", 3)], &[]),
        agent_yaml(
            "b",
            &[("sonnet", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/opus]\nremember 4271").await?;
        // Simulate the shared adapter dying while the original session is idle.
        let key = shared
            .candidate_runtime(&CandidateId::new("a", "opus"))
            .unwrap()
            .process_key;
        shared.mark_target_dead(&key, "adapter exited while idle");

        let response = prompt_text(&cx, &sid, "second task").await?;
        assert_eq!(response.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("failover: auto → b/sonnet"),
            "idle death disclosed: {text}"
        );
        let prompts: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "prompt")
            .collect();
        let handoff = prompts.last().unwrap()["text"].as_str().unwrap();
        assert!(
            handoff.contains("Assistant: echo:opus:remember 4271")
                && handoff.contains("second task"),
            "replacement receives both turns: {handoff}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn delegate_choice_is_disclosed_to_the_user() {
    let state = temp_state_file("delegate-why");
    let log = temp_log("delegate-why");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    run_test(delegation_yaml(&state, &log, 3), async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "big work\nDELEGATE:tweak the css").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("router-acp · delegate_task → cheap/haiku"),
            "delegate routing disclosed: {text}"
        );
        assert!(
            text.contains("tweak the css"),
            "delegated task summarized: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn routing_scales_with_task_complexity_and_prefers_claude() {
    // Replicates the real-world regression: a hello-world session and an
    // hour-long PR/ticket investigation must NOT both land on a mini-class
    // model. Config mirrors the recommended goose setup: claude preferred
    // (+0.05), quality-leaning tradeoff 3, complexity scaling on (default).
    let state = temp_state_file("scaling");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 3 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "claude",
            &[
                ("haiku", 1),
                ("sonnet", 2),
                ("opus", 4),
                ("claude-fable-5", 5)
            ],
            &[]
        )
        .replace(
            "    model_selection:",
            "    preference: 0.05\n    model_selection:"
        ),
        agent_yaml("codex", &[("gpt-5.4-mini", 1), ("gpt-5.5", 3)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;

        // Session 1: trivial prompt -> the minimal claude model, not codex.
        let s1 = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &s1, "hello world").await?;
        let text1 = agent_text(&observed, &s1);
        assert!(
            text1.contains("auto → claude/haiku"),
            "trivial prompt does not spend unused frontier capability: {text1}"
        );

        // Session 2: cross-system investigation -> a frontier claude model.
        let s2 = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(
            &cx,
            &s2,
            "Analyze the pull request and the linear ticket, then leave a comment on the \
             ticket with the status of the current work and the remaining work",
        )
        .await?;
        let text2 = agent_text(&observed, &s2);
        assert!(
            text2.contains("auto → claude/claude-fable-5") || text2.contains("auto → claude/opus"),
            "complex investigation routes to a frontier model: {text2}"
        );
        assert!(
            !text2.contains("mini"),
            "mini-class models must not win complex work: {text2}"
        );
        assert!(
            text2.contains("complexity-scaled"),
            "disclosure explains the tradeoff scaling: {text2}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn manager_rejection_is_not_spawned_and_auto_routes_live_peer() {
    let state = temp_state_file("auth-preflight");
    let claude_log = temp_log("auth-preflight-claude");
    let grok_log = temp_log("auth-preflight-grok");
    let credential = claude_credential_fixture(Some("Rejected"));
    let preclass = r#"{"routing":{"task_class":"Ops","complexity":0.08,"confidence":0.95,"reason":"trivial ops"}}"#;
    let claude = agent_yaml(
        "claude",
        &[("haiku", 1), ("sonnet", 2), ("opus", 4)],
        &[("MOCK_LOG", &claude_log.display().to_string()),
          ("CLAUDE_CONFIG_DIR", &credential.path().display().to_string())],
    )
    .replace(
        "    model_selection:",
        "    auth_probe:\n      command: /bin/sh\n      args: [\"-c\", \"echo Claude is not signed in >&2; exit 1\"]\n    model_selection:",
    );
    let grok = agent_yaml(
        "grok",
        &[("grok-4.5", 5)],
        &[
            ("MOCK_LOG", &grok_log.display().to_string()),
            ("MOCK_PRECLASS_JSON", preclass),
        ],
    );
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\npre_classifier: {{ enabled: true }}\nagents:\n{}{}",
        state.display(),
        claude,
        grok,
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        assert!(
            read_log(&claude_log).is_empty(),
            "definitely logged-out Claude must not be spawned"
        );

        let session = new_session(&cx).await?;
        let options = serde_json::to_value(&session.config_options).unwrap();
        let text = options.to_string();
        for model in ["claude/haiku", "claude/sonnet", "claude/opus"] {
            let pos = text
                .find(model)
                .unwrap_or_else(|| panic!("missing {model}: {text}"));
            let tail = &text[pos..text.len().min(pos + 500)];
            assert!(tail.contains("\"available\":false"), "{model}: {tail}");
            assert!(tail.contains("definitively rejected"), "{model}: {tail}");
            assert!(
                !tail.contains("resets_at"),
                "auth has no reset timestamp: {tail}"
            );
        }

        let sid = session.session_id.0.to_string();
        prompt_text(&cx, &sid, "What tickets are assigned to me?").await?;
        let routed = agent_text(&observed, &sid);
        assert!(routed.contains("auto → grok/grok-4.5"), "{routed}");
        assert!(
            read_log(&claude_log).is_empty(),
            "pre-class and primary routing must both skip Claude"
        );
        let grok_events = read_log(&grok_log);
        assert!(
            grok_events.iter().any(|e| e["event"] == "prompt"),
            "Grok handled classifier and primary prompt: {grok_events:?}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn static_pin_to_unauthenticated_agent_reports_auth_required_with_live_peer() {
    let state = temp_state_file("auth-static-pin");
    let claude_log = temp_log("auth-static-pin-claude");
    let codex_log = temp_log("auth-static-pin-codex");
    let credential = claude_credential_fixture(Some("Rejected"));
    let claude = agent_yaml(
        "claude",
        &[("fable", 5)],
        &[("MOCK_LOG", &claude_log.display().to_string()),
          ("CLAUDE_CONFIG_DIR", &credential.path().display().to_string())],
    )
    .replace(
        "    model_selection:",
        "    auth_probe:\n      command: /bin/sh\n      args: [\"-c\", \"echo Claude is not signed in >&2; exit 1\"]\n    model_selection:",
    );
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        claude,
        agent_yaml(
            "codex",
            &[("gpt", 2)],
            &[("MOCK_LOG", &codex_log.display().to_string())],
        ),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        cx.send_request(SetSessionConfigOptionRequest::new(
            sid.clone(),
            "router.candidate".to_string(),
            SessionConfigOptionValue::value_id("claude/fable"),
        ))
        .block_task()
        .await?;
        let err = prompt_text(&cx, &sid, "use fable").await.unwrap_err();
        assert_eq!(err.code, AcpError::auth_required().code, "got: {err}");
        let text = format!("{err}");
        assert!(text.contains("claude/fable"), "candidate named: {text}");
        assert!(
            text.contains("sign in to `claude`"),
            "heal path named: {text}"
        );
        assert!(
            read_log(&claude_log).is_empty(),
            "definitely logged-out Claude must not be spawned"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn authenticated_agent_remains_eligible_for_preclass_and_ops_pin() {
    let state = temp_state_file("auth-preflight-ok");
    let claude_log = temp_log("auth-preflight-ok-claude");
    let grok_log = temp_log("auth-preflight-ok-grok");
    let preclass = r#"{"routing":{"task_class":"Ops","complexity":0.08,"confidence":0.95,"reason":"trivial ops"}}"#;
    let claude = agent_yaml(
        "claude",
        &[("haiku", 1), ("sonnet", 2)],
        &[
            ("MOCK_LOG", &claude_log.display().to_string()),
            ("MOCK_PRECLASS_JSON", preclass),
        ],
    )
    .replace(
        "    model_selection:",
        "    auth_probe:\n      command: /bin/true\n    model_selection:",
    );
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\npre_classifier: {{ enabled: true }}\nagents:\n{}{}",
        state.display(),
        claude,
        agent_yaml(
            "grok",
            &[("grok-4.5", 5)],
            &[("MOCK_LOG", &grok_log.display().to_string())],
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "List my tickets").await?;
        let routed = agent_text(&observed, &sid);
        assert!(routed.contains("auto → claude/haiku"), "{routed}");
        assert!(
            read_log(&claude_log)
                .iter()
                .filter(|e| e["event"] == "prompt")
                .count()
                >= 2,
            "Claude handled preclass and primary prompt"
        );
        assert!(
            !read_log(&grok_log).iter().any(|e| e["event"] == "prompt"),
            "preferred authenticated Claude prevents Grok widening"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn session_auth_error_without_failed_repair_does_not_cordon() {
    let state = temp_state_file("auth-reactive");
    let claude_log = temp_log("auth-reactive-claude");
    let grok_log = temp_log("auth-reactive-grok");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "claude",
            &[("haiku", 1), ("sonnet", 2)],
            &[
                ("MOCK_LOG", &claude_log.display().to_string()),
                ("MOCK_FAIL_PROMPT_MSG", "Authentication required"),
                ("MOCK_FAIL_PROMPT_TIMES", "1"),
            ],
        ),
        agent_yaml(
            "grok",
            &[("grok-4.5", 5)],
            &[("MOCK_LOG", &grok_log.display().to_string())],
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let first = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &first, "First Ops prompt").await?;
        assert!(
            agent_text(&observed, &first).contains("echo:sonnet:"),
            "an unconfirmed session error may try another model on the same account"
        );
        let claude_prompts = read_log(&claude_log)
            .iter()
            .filter(|e| e["event"] == "prompt")
            .count();

        let second_session = new_session(&cx).await?;
        let opts = serde_json::to_string(&second_session.config_options).unwrap();
        assert!(
            opts.contains("claude/haiku") && !opts.contains("definitively rejected"),
            "an unknown repair must not create a logout indication: {opts}"
        );
        assert!(shared.auth_rejection("claude").is_none());
        assert!(
            shared
                .headroom
                .lock()
                .unwrap()
                .cordon_active("claude")
                .is_none()
        );
        let second = second_session.session_id.0.to_string();
        prompt_text(&cx, &second, "Second Ops prompt").await?;
        assert!(
            agent_text(&observed, &second).contains("auto → claude/haiku"),
            "the next session can still use the account"
        );
        assert_eq!(
            read_log(&claude_log)
                .iter()
                .filter(|e| e["event"] == "prompt")
                .count(),
            claude_prompts + 1,
            "next decision never probes another Claude model"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn state_db_records_routing_diagnostics_title_and_tokens() {
    let state = temp_state_file("diagnostics");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1), ("m2", 2)], &[])
    );
    let state_path = state.clone();
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid,
            "Fix the login bug on the settings page and add a regression test",
        )
        .await?;

        {
            let db = open_state(&state_path);
            let entry = db.get(&sid).expect("session row");
            assert_eq!(entry.agent, "mock");
            assert_eq!(entry.kind, "primary");
            assert_eq!(
                entry.title.as_deref(),
                Some("Fix the login bug on the settings page and add a regression test")
            );
            let routing = entry.routing.expect("routing recorded");
            assert_eq!(routing["strategy"], "auto");
            assert_eq!(routing["candidate"], "mock/m1");
            assert!(routing["reason"].as_str().unwrap().contains("utility"));
            assert!(routing["weights"]["quality_weight"].is_number());
            assert_eq!(routing["class"], "BugFix");
            assert!(entry.created_at.is_some() && entry.updated_at.is_some());
            // Token accounting: a user_prompt and an agent_response were logged
            // and the session counters were incremented.
            assert!(
                entry.tokens_total > 0,
                "tokens accrued: {}",
                entry.tokens_total
            );
            let log = db.log_for(&sid, 10);
            assert!(
                log.iter()
                    .any(|e| e.kind == "user_prompt" && e.role == "user")
            );
            assert!(
                log.iter()
                    .any(|e| e.kind == "agent_response" && e.role == "agent")
            );
        }

        // A downstream session_info_update replaces the placeholder title.
        prompt_text(&cx, &sid, "TITLE:Login Bug Investigation").await?;
        let db = open_state(&state_path);
        assert_eq!(
            db.get(&sid).unwrap().title.as_deref(),
            Some("Login Bug Investigation")
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn state_db_prunes_by_history_window() {
    // history: 1s → after a session goes idle past 1s, a later prune drops it.
    let state = temp_state_file("history");
    let yaml = format!(
        "state_file: {}\nhistory: 1s\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    let state_path = state.clone();
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "keep me briefly").await?;
        assert!(open_state(&state_path).get(&sid).is_some());
        Ok(())
    })
    .await;
    // Reopen with the same 1s window after idling; load-time prune removes it.
    // Sleep well past the window — updated_at has second granularity, so a
    // margin under ~2s can round to "not yet expired".
    tokio::time::sleep(Duration::from_secs(3)).await;
    let db = router_acp::state::StateFile::load(
        &state,
        router_acp::state::Retention {
            max_age: std::time::Duration::from_secs(1),
        },
    );
    assert_eq!(db.all().len(), 0, "history window pruned the idle session");
}

#[tokio::test]
async fn title_generation_does_not_hijack_the_pin() {
    // goose sends a "Generate a short title" meta-prompt first; it must NOT
    // pin the session (which would ignore the real prompt's directive).
    let state = temp_state_file("titlepin");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1), ("m2", 2)], &[])
    );
    let state_path = state.clone();
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();

        // Title-gen meta prompt (no directive) — served but does not pin.
        prompt_text(
            &cx,
            &sid,
            "---BEGIN USER MESSAGES--- do stuff ---END USER MESSAGES---               Generate a short title for the above messages.",
        )
        .await?;
        assert!(
            open_state(&state_path).get(&sid).is_none(),
            "title-gen must not create a pinned session row"
        );

        // Real prompt WITH a directive now pins per the directive, not the
        // default the title-gen would have caused.
        let resp = prompt_text(&cx, &sid, "[router: candidate=mock/m2]\nthe real task").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(text.contains("static → mock/m2"), "directive won the pin: {text}");
        assert!(text.contains("echo:m2:the real task"), "ran on m2: {text}");
        assert_eq!(open_state(&state_path).get(&sid).unwrap().model, "m2");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn delegate_subagent_gets_linked_state_row() {
    let state = temp_state_file("delegtree");
    let log = temp_log("delegtree");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let state_path = state.clone();
    run_test(delegation_yaml(&state, &log, 3), async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "big work\nDELEGATE:tweak the css").await?;

        let db = open_state(&state_path);
        let parent = db.get(&sid).expect("parent row");
        assert_eq!(parent.kind, "primary");
        // The delegated sub-agent has its own row linked to the parent.
        let children: Vec<_> = db
            .all()
            .into_iter()
            .filter(|(_, s)| s.parent_session_id.as_deref() == Some(sid.as_str()))
            .collect();
        assert_eq!(children.len(), 1, "one delegate row linked to the parent");
        let (_, child) = &children[0];
        assert_eq!(child.kind, "delegate");
        assert_eq!(child.agent, "cheap"); // routed to the lower-cost lineage
        assert!(
            child
                .title
                .as_deref()
                .unwrap_or("")
                .contains("tweak the css")
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Prompt routing directives
// ======================================================================

#[tokio::test]
async fn directive_pins_explicit_candidate_and_is_stripped() {
    let state = temp_state_file("directive");
    let log = temp_log("directive");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1), ("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "[router: candidate=mock/m2]\ndo the thing").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("static → mock/m2"),
            "directive pinned the candidate: {text}"
        );
        assert!(text.contains("echo:m2:do the thing"), "got: {text}");
        // The downstream model never saw the directive line.
        let prompts: Vec<String> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "prompt")
            .map(|e| e["text"].as_str().unwrap().to_string())
            .collect();
        assert!(
            prompts.iter().any(|p| {
                p.starts_with("do the thing\n[router-acp questions]")
                    && p.contains("Do not ask the user only in prose")
            }),
            "directive stripped: {prompts:?}"
        );
        assert!(
            prompts.iter().all(|p| !p.contains("[router:")),
            "no directive leaked downstream: {prompts:?}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn directive_exclude_removes_a_lineage() {
    let state = temp_state_file("directive-excl");
    // fancy (opus) would win pure-quality routing; excluding its agent
    // forces the other lineage — the reviewer-vs-planner separation.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml("fancy", &[("opus", 3)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "[router: exclude=fancy]\nreview this work").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("auto → cheap/haiku"),
            "excluded lineage avoided: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn invalid_directive_fails_loudly_and_post_pin_is_ignored() {
    let state = temp_state_file("directive-bad");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Unknown key: loud error so recipes get fixed.
        let err = prompt_text(&cx, &sid, "[router: modell=mock/m1]\nhi")
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("unknown routing directive key"),
            "got: {err}"
        );
        // Valid prompt pins the session…
        prompt_text(&cx, &sid, "hello").await?;
        // …and a later directive is stripped + ignored with a notice.
        let resp = prompt_text(&cx, &sid, "[router: candidate=mock/m1]\nfollow-up").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("routing directive ignored (session already pinned"),
            "got: {text}"
        );
        assert!(text.contains("echo:m1:follow-up"), "got: {text}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn switch_directive_hands_off_to_new_model_mid_session() {
    let state = temp_state_file("switch");
    let log = temp_log("switch");
    // Two lineages so the switch target is a genuinely different candidate.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Pin the session to a/m1.
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nstart the work").await?;
        // Now switch to b/m2 mid-session — directive and task on the SAME line,
        // the way a user actually types it.
        let resp = prompt_text(&cx, &sid, "[router: switch=b/m2] continue the work").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "switch disclosed: {text}"
        );
        // The continuation ran on the new model.
        assert!(text.contains("echo:m2:"), "new model answered: {text}");
        // The summarization turn on the old model must NOT be relayed as its
        // own client message. The client should see exactly two agent chunks:
        // the first prompt's answer and the post-switch answer — not a third
        // chunk carrying the captured summary.
        let chunk_count = observed
            .lock()
            .unwrap()
            .updates
            .iter()
            .filter(|n| n.session_id.0.as_ref() == sid)
            .filter(|n| matches!(n.update, SessionUpdate::AgentMessageChunk(_)))
            .count();
        assert_eq!(chunk_count, 2, "summary turn was not suppressed: {text}");

        let prompts: Vec<(String, String)> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "prompt")
            .map(|e| {
                (
                    e["model"].as_str().unwrap_or("").to_string(),
                    e["text"].as_str().unwrap_or("").to_string(),
                )
            })
            .collect();
        // The old model was asked to summarize.
        assert!(
            prompts
                .iter()
                .any(|(m, t)| m == "m1" && t.contains("hand this conversation off")),
            "old model summarized: {prompts:?}"
        );
        // The new model received the handoff context + the user's message.
        assert!(
            prompts.iter().any(|(m, t)| m == "m2"
                && t.contains("Handoff context")
                && t.contains("continue the work")),
            "new model got handoff + prompt: {prompts:?}"
        );
        // The switch recorded its lineage: prior_session_id = the downstream
        // session bound before the switch, distinct from the current one.
        let row = open_state(&state).get(&sid).expect("session row");
        let prior = row
            .prior_session_id
            .expect("prior_session_id set on switch");
        assert!(
            prior.starts_with("a-sess-"),
            "prior points to old downstream: {prior}"
        );
        assert!(
            row.downstream_session_id.starts_with("b-sess-"),
            "current downstream is the new model's: {}",
            row.downstream_session_id
        );
        assert_ne!(prior, row.downstream_session_id);
        Ok(())
    })
    .await;
}

/// A managed account whose shared credential manager rejected refresh.
fn logged_out_at_boot_yaml(
    state: &std::path::Path,
    a_log: &std::path::Path,
    b_log: &std::path::Path,
    credential_dir: &std::path::Path,
    b_env: &[(&str, &str)],
) -> String {
    let b_log = b_log.display().to_string();
    let credential_dir = credential_dir.display().to_string();
    let mut env = vec![
        ("MOCK_LOG", b_log.as_str()),
        ("CLAUDE_CONFIG_DIR", credential_dir.as_str()),
    ];
    env.extend_from_slice(b_env);
    let b = agent_yaml("claude@cold", &[("m2", 2)], &env).replace(
        "    model_selection:",
        "    auth_probe:\n      command: /bin/sh\n      args: [\"-c\", \"echo not signed in >&2; exit 1\"]\n    model_selection:",
    );
    format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &a_log.display().to_string())]
        ),
        b,
    )
}

#[tokio::test]
async fn switch_starts_a_target_skipped_at_boot_and_hands_off() {
    // The live failure: the router booted while Claude was logged out, so
    // Claude was never spawned. After sign-in, a chip pick of Opus failed
    // "no live downstream process" after the outgoing model's summary.
    let state = temp_state_file("switch-boot-skipped");
    let a_log = temp_log("switch-boot-skipped-a");
    let b_log = temp_log("switch-boot-skipped-b");
    let credential = claude_credential_fixture(Some("Rejected"));
    let yaml = logged_out_at_boot_yaml(&state, &a_log, &b_log, credential.path(), &[]);
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        assert!(read_log(&b_log).is_empty(), "b not spawned at boot");
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nstart the work").await?;
        // Native login has replaced the manager's rejected generation.
        std::fs::remove_file(credential.path().join(".router-acp-auth.json")).unwrap();
        let resp = prompt_text(
            &cx,
            &sid,
            "[router: switch=claude@cold/m2] continue the work",
        )
        .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → claude@cold/m2"),
            "switched: {text}"
        );
        assert!(text.contains("echo:m2:"), "new model answered: {text}");
        assert!(!text.contains("no live downstream"), "{text}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn switch_to_a_still_signed_out_target_fails_before_the_summary() {
    let state = temp_state_file("switch-boot-signed-out");
    let a_log = temp_log("switch-boot-signed-out-a");
    let b_log = temp_log("switch-boot-signed-out-b");
    let credential = claude_credential_fixture(Some("Rejected"));
    let yaml = logged_out_at_boot_yaml(
        &state,
        &a_log,
        &b_log,
        credential.path(),
        &[("MOCK_AUTH_REQUIRED", "1")],
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nstart the work").await?;

        let failed = prompt_text(
            &cx,
            &sid,
            "[router: switch=claude@cold/m2] continue the work",
        )
        .await;
        assert!(failed.is_err(), "turn fails: {failed:?}");
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switch to claude@cold/m2 failed"),
            "failure named: {text}"
        );
        assert!(
            text.contains("authentication") || text.contains("refresh"),
            "sign-in named: {text}"
        );
        assert!(!text.contains("no live downstream"), "{text}");
        assert!(
            !read_log(&a_log).iter().any(|e| e["event"] == "prompt"
                && e["text"]
                    .as_str()
                    .unwrap_or("")
                    .contains("hand this conversation off")),
            "a was not asked to summarize"
        );

        // Still on a/m1.
        prompt_text(&cx, &sid, "still here").await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:m1:still here"),
            "stayed on a/m1: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn switch_directive_alone_switches_and_the_new_model_responds() {
    // A bare `[router: switch=…]` with no task must still switch and produce a
    // response from the new model (a synthesized continuation prompt).
    let state = temp_state_file("switch-only");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nstart").await?;
        // Directive only — no task text at all.
        let resp = prompt_text(&cx, &sid, "[router: switch=b/m2]").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "bare switch disclosed: {text}"
        );
        assert!(text.contains("echo:m2:"), "new model responded: {text}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn model_shorthand_switches_mid_session_and_leaves_prose_alone() {
    let state = temp_state_file("shorthand");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nstart").await?;

        // `m2: …` shorthand (bare model id) switches and runs the task on b/m2.
        let resp = prompt_text(&cx, &sid, "m2: continue the work").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "shorthand switched: {text}"
        );
        assert!(text.contains("echo:m2:"), "task ran on b/m2: {text}");
        assert!(text.contains("continue the work"), "task forwarded: {text}");
        // The `m2:` shorthand prefix was stripped, not leaked to the model.
        assert!(
            !text.contains("m2: continue the work"),
            "shorthand prefix stripped: {text}"
        );

        // Prose that merely starts with `word:` (no matching model) is left
        // untouched — no switch, forwarded verbatim to the current model.
        let resp = prompt_text(&cx, &sid, "Note: this is just a note").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:m2:Note: this is just a note"),
            "prose passed through unchanged: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn switch_to_unknown_candidate_fails_the_turn_and_keeps_the_session() {
    // `switch=a/bogus` (undeclared) must not break the session: no wasted
    // summary turn and a clear notice. The picked-away-from model must not
    // silently answer the turn, but it keeps serving the next one.
    let state = temp_state_file("switch-bad");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("a", &[("m1", 1), ("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nstart").await?;
        let failed = prompt_text(&cx, &sid, "[router: switch=a/bogus] keep going").await;
        assert!(failed.is_err(), "failed pick fails the turn: {failed:?}");
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switch to a/bogus failed"),
            "failure disclosed: {text}"
        );
        assert!(
            !text.contains("echo:m1:keep going"),
            "turn not run on the old model: {text}"
        );
        let resp = prompt_text(&cx, &sid, "next").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(agent_text(&observed, &sid).contains("echo:m1:next"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn switch_falls_back_to_log_transcript_when_summary_fails() {
    // When the outgoing model can't summarize (here: a/m1 fails every prompt
    // after the first — a token limit / outage), the handoff is reconstructed
    // from the state-DB logs and seeded into the new model.
    let state = temp_state_file("switch-fallback");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        // a/m1: first prompt (the real turn) succeeds; the summary prompt that
        // switch_pin sends next fails, forcing the transcript fallback.
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[
                ("MOCK_FAIL_PROMPT_MSG", "token limit reached"),
                ("MOCK_FAIL_PROMPT_AFTER", "1"),
            ],
        ),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // A real turn whose content we can later look for in the handoff.
        prompt_text(
            &cx,
            &sid,
            "[router: candidate=a/m1]\nremember the secret code is 4271",
        )
        .await?;
        // Switch: a/m1's summary attempt will fail → log-transcript fallback.
        let resp = prompt_text(&cx, &sid, "[router: switch=b/m2] what was the code?").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(text.contains("switched a/m1 → b/m2"), "switched: {text}");
        assert!(text.contains("echo:m2:"), "ran on b/m2: {text}");
        // The fallback transcript carried the prior turn's content to b/m2.
        assert!(
            text.contains("4271"),
            "log transcript carried prior context: {text}"
        );
        // The disclosure explains that the fallback (not a model summary) was used.
        let lower = text.to_lowercase();
        assert!(
            lower.contains("transcript") || lower.contains("recovered from logs"),
            "fallback disclosed: {text}"
        );
        Ok(())
    })
    .await;
}

const CODEX_SPEND_CAP: &str = r#"{"message":"You hit your spend cap set by the owner of your workspace.","codexErrorInfo":"usageLimitExceeded"}"#;

/// Wait until `id`'s downstream process is gone (the watchdog marks it dead).
async fn wait_for_dead(shared: &Shared, id: &CandidateId) {
    let key = shared.candidate_runtime(id).unwrap().process_key;
    for _ in 0..100 {
        if shared.target_conn(&key).is_none() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("{id} never died");
}

#[tokio::test]
async fn model_shorthand_revives_a_dead_target_and_switches_to_it() {
    // The live deadlock: the picked model's process had died, so `opus:` did
    // not resolve (dead targets are in no routing view) and `switch=` failed
    // with "no live downstream process". Both left the session on the model
    // the human was leaving. A pick revives the process, cooldown or not.
    let state = temp_state_file("switch-revive");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         failover: {{ respawn_cooldown_secs: 3600 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nremember 4271").await?;

        // Crash b from another session, inside its respawn cooldown so no
        // automatic revive brings it back.
        let b = CandidateId::parse("b/m2").unwrap();
        let b_key = shared.candidate_runtime(&b).unwrap().process_key;
        shared
            .targets
            .lock()
            .unwrap()
            .get_mut(&b_key)
            .unwrap()
            .last_respawn = Some(std::time::Instant::now());
        let other = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &other, "[router: candidate=b/m2]\nEXIT_NOW:m2").await?;
        wait_for_dead(&shared, &b).await;

        let resp = prompt_text(&cx, &sid, "m2: what was the code?").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(text.contains("switched a/m1 → b/m2"), "switched: {text}");
        assert!(text.contains("echo:m2:"), "ran on b/m2: {text}");
        assert!(
            text.contains("what was the code?"),
            "prompt continued: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn spend_capped_model_is_cordoned_and_handoff_uses_the_transcript() {
    // The live failure: Sol's plan hit codex's workspace spend cap, so its
    // handoff summary errored. The switch must still land, seeded from the
    // log transcript, and the capped model must be cordoned.
    let state = temp_state_file("switch-spend-cap");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[
                ("MOCK_FAIL_PROMPT_MSG", CODEX_SPEND_CAP),
                ("MOCK_FAIL_PROMPT_AFTER", "1"),
            ],
        ),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid,
            "[router: candidate=a/m1]\nremember the code is 4271",
        )
        .await?;

        let resp = prompt_text(&cx, &sid, "m2: what was the code?").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(text.contains("switched a/m1 → b/m2"), "switched: {text}");
        assert!(text.contains("echo:m2:"), "ran on b/m2: {text}");
        assert!(text.contains("4271"), "transcript carried context: {text}");
        let a = CandidateId::parse("a/m1").unwrap();
        assert!(
            shared.headroom.lock().unwrap().usage_cordon(&a).is_some(),
            "spend-capped model cordoned"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn spend_cap_failover_carries_the_transcript_to_the_new_model() {
    // Failover off a capped pin used to start the replacement blind (only a
    // context overflow seeded the transcript), so it could not continue.
    let state = temp_state_file("failover-transcript");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml(
            "fancy",
            &[("opus", 3)],
            &[
                ("MOCK_FAIL_PROMPT_MSG", CODEX_SPEND_CAP),
                ("MOCK_FAIL_PROMPT_AFTER", "1"),
            ],
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "remember the code is 4271").await?;
        assert!(agent_text(&observed, &sid).contains("echo:opus:"));

        let resp = prompt_text(&cx, &sid, "what was the code?").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(text.contains("spend limit"), "cap recognized: {text}");
        let answer = text.rsplit("echo:haiku:").next().unwrap_or_default();
        assert!(
            answer.contains("4271"),
            "replacement got the transcript: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn pick_of_a_model_that_cannot_serve_fails_loudly_not_on_the_old_model() {
    // b refuses every session (not signed in): the pick cannot land, and the
    // turn must say so instead of quietly running on the model left behind.
    let state = temp_state_file("switch-cannot-serve");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[("MOCK_AUTH_REQUIRED", "1")]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nstart").await?;

        let failed = prompt_text(&cx, &sid, "[router: switch=b/m2] keep going").await;
        assert!(failed.is_err(), "turn fails: {failed:?}");
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switch to b/m2 failed"),
            "failure named: {text}"
        );
        assert!(
            !text.contains("echo:m1:keep going"),
            "never ran on the old model: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn skill_routing_switches_pinned_session_to_required_class() {
    let state = temp_state_file("skill");
    let log = temp_log("skill");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         skill_routing:\n  - pattern: ship-pr\n    candidates: [\"*m2*\"]\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Pin to a/m1, a model NOT in the skill's required class.
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;
        // Invoking ship-pr must steer the session onto the *m2* class.
        let resp = prompt_text(&cx, &sid, "please run ship-pr on this branch").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "skill forced the switch: {text}"
        );
        assert!(text.contains("echo:m2:"), "ran on required class: {text}");
        Ok(())
    })
    .await;
}

/// Live bug: ship-pr candidates include both the current pin (`*opus*`) and a
/// fallback (`*grok*`). When the pin is usage-cordoned (plan at 100%, no
/// overage), skill_routing used to treat the pin as already-ok because the
/// id still matched the skill globs — and stayed on the dead seat. It must
/// switch to the next still-eligible skill candidate.
#[tokio::test]
async fn skill_routing_switches_off_usage_cordoned_pin() {
    let state = temp_state_file("skill-cordon");
    let log = temp_log("skill-cordon");
    // Both m1 and m2 are in the skill class (m2 preferred when free). Poller
    // disabled so we inject the cordon ourselves.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\ncordon: {{ enabled: false }}\n\
         skill_routing:\n  - pattern: ship-pr\n    candidates: [\"*m2*\", \"*m1*\"]\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Pin to a/m1 (in the skill class).
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;

        // Cordon a/m1 as if its plan hit 100% with no overage headroom.
        let mut cordons = std::collections::HashMap::new();
        cordons.insert(
            router_acp::candidate::CandidateId::new("a", "m1"),
            router_acp::headroom::UsageCordon {
                reason: "5-hour usage limit reached".to_string(),
                resets_at: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
                resets_at_rfc3339: "2099-01-01T00:00:00+00:00".to_string(),
            },
        );
        shared.headroom.lock().unwrap().set_usage_cordons(cordons);

        // ship-pr must leave the dead m1 pin for the still-eligible m2.
        let resp = prompt_text(&cx, &sid, "please run ship-pr on this branch").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2") || text.contains("echo:m2:"),
            "skill must leave cordoned pin for eligible skill candidate: {text}"
        );
        assert!(
            !text.contains("echo:m1:please run ship-pr"),
            "must not stay on usage-cordoned m1 for ship-pr: {text}"
        );
        Ok(())
    })
    .await;
}

/// Live bug (measured over 7 days of Kory Code sessions: 19 of 26 skill-forced
/// switches): a session already pinned to a model that is *better* than
/// anything in the skill's candidate list got force-switched anyway, purely
/// because its pin was absent from that list — a full summarize + re-pin,
/// losing the live context, to land on a LESSER model. `also_acceptable` names
/// pins that are fine to stay on without making them switch targets.
#[tokio::test]
async fn skill_routing_leaves_also_acceptable_pin_alone() {
    let state = temp_state_file("skill-also-ok");
    let log = temp_log("skill-also-ok");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         skill_routing:\n  - pattern: ship-pr\n    candidates: [\"*m2*\"]\n\
         \x20   also_acceptable: [\"*m3*\"]\n\
         agents:\n{}{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "c",
            &[("m3", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Pin to c/m3 — NOT a switch target, but declared already-acceptable.
        prompt_text(&cx, &sid, "[router: candidate=c/m3]\nwarm up").await?;
        let resp = prompt_text(&cx, &sid, "please run ship-pr on this branch").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            !text.contains("switched"),
            "also_acceptable pin must not be re-pinned: {text}"
        );
        assert!(
            text.contains("echo:m3:please run ship-pr"),
            "ship-pr must run on the existing m3 pin: {text}"
        );
        Ok(())
    })
    .await;
}

/// The other half of the split: `also_acceptable` is an acceptance set ONLY.
/// A pin in neither list still switches, and the target comes from
/// `candidates` — never from `also_acceptable`, however high it scores. This
/// is what makes the two lists worth having: merging them would fix the
/// leave-it-alone case above by turning the expensive tier into the default
/// switch target for every genuine switch.
#[tokio::test]
async fn skill_routing_never_switches_to_an_also_acceptable_model() {
    let state = temp_state_file("skill-also-target");
    let log = temp_log("skill-also-target");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         skill_routing:\n  - pattern: ship-pr\n    candidates: [\"*m2*\"]\n\
         \x20   also_acceptable: [\"*m3*\"]\n\
         agents:\n{}{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "c",
            &[("m3", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Pin to a/m1 — in neither list, so a switch is genuinely warranted.
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;
        let resp = prompt_text(&cx, &sid, "please run ship-pr on this branch").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "switch target must come from candidates: {text}"
        );
        assert!(
            !text.contains("c/m3") && !text.contains("echo:m3:"),
            "also_acceptable must never be a switch target: {text}"
        );
        Ok(())
    })
    .await;
}

/// Live bug: grok was the pin, `/finalize-pr` matched `already_ok` (grok is
/// in `candidates`), then a leftover auto-upgrade to Sol (`also_acceptable`)
/// still fired in `send_prompt_with_failover`. Re-invoking an already-ok
/// skill must drop that queued switch.
#[tokio::test]
async fn skill_routing_already_ok_cancels_queued_switch_to_also_acceptable() {
    let state = temp_state_file("skill-cancel-upgrade");
    let log = temp_log("skill-cancel-upgrade");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"c/*\", default_quality: 0.90 }\n\
         \x20 - { pattern: \"b/*\", default_quality: 0.70 }\n\
         \x20 - { pattern: \"a/*\", default_quality: 0.40 }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         skill_routing:\n  - pattern: finalize-pr\n    candidates: [\"*m1*\", \"*m2*\"]\n\
         \x20   also_acceptable: [\"*m3*\"]\n\
         agents:\n{}{}{}",
        state.display(),
        scores.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "c",
            &[("m3", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Pin the skill-approved but under-powered model. Auto-upgrade will
        // queue a switch to c/m3 (highest score, also_acceptable only).
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;
        let resp = prompt_text(&cx, &sid, "please run finalize-pr on this branch").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            !text.contains("switched a/m1 → c/m3") && !text.contains("echo:m3:"),
            "skill already-ok must not consume a queued switch onto also_acceptable: {text}"
        );
        assert!(
            text.contains("echo:m1:please run finalize-pr"),
            "finalize-pr must stay on the already-ok m1 pin: {text}"
        );
        Ok(())
    })
    .await;
}

/// `selection: first-match` picks the FIRST candidates glob with an eligible
/// seat behind it, even when a later glob scores strictly higher quality —
/// the whole point of the setting. Without it (the default `best-quality`),
/// list order is only a tie-break and the higher-quality m3 would win instead.
#[tokio::test]
async fn skill_routing_first_match_selection_overrides_quality_order() {
    let state = temp_state_file("skill-first-match");
    let log = temp_log("skill-first-match");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-first-match-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    // b/m2 is listed first in `candidates` but scores lower than c/m3.
    // best-quality would pick c/m3; first-match must pick b/m2.
    std::fs::write(
        &scores,
        "candidates:\n\
         \x20 - { pattern: 'b/m2', default_quality: 1.0 }\n\
         \x20 - { pattern: 'c/m3', default_quality: 3.0 }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         skill_routing:\n  - pattern: ship-pr\n    selection: first-match\n\
         \x20   candidates: [\"*m2*\", \"*m3*\"]\n\
         agents:\n{}{}{}",
        state.display(),
        scores.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "c",
            &[("m3", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;
        let resp = prompt_text(&cx, &sid, "please run ship-pr on this branch").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "first-match must land on the FIRST listed glob, not the highest-quality one: {text}"
        );
        Ok(())
    })
    .await;
}

/// Live bug (Kory Code spawn): `POST /api/agent/sessions` with
/// `model: claude/sonnet` (rendered as `[router: candidate=…]`) plus a brief
/// that names ship-pr/finalize-pr applied UserPick, then the pre-pin skill
/// steer overwrote it because `current` pin was still None. Explicit
/// candidate must win on prompt 1.
#[tokio::test]
async fn skill_routing_does_not_overwrite_pre_pin_user_pick() {
    let state = temp_state_file("skill-userpick-prepin");
    let log = temp_log("skill-userpick-prepin");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         skill_routing:\n  - pattern: ship-pr\n    selection: first-match\n\
         \x20   candidates: [\"*m2*\", \"*m3*\"]\n\
         agents:\n{}{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "c",
            &[("m3", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // First prompt: explicit candidate AND a skill mention. m1 is in
        // neither skill list, so the old pre-pin branch would steer to m2.
        let resp = prompt_text(
            &cx,
            &sid,
            "[router: candidate=a/m1, phase=implementation]\nplease run ship-pr on this branch",
        )
        .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:m1:"),
            "explicit UserPick must pin a/m1 on prompt 1: {text}"
        );
        assert!(
            !text.contains("echo:m2:") && !text.contains("skill `ship-pr` steering"),
            "pre-pin skill steer must not overwrite UserPick: {text}"
        );
        Ok(())
    })
    .await;
}

/// The other half: a plain `/ship-pr` (or skill token) on an unpinned session
/// with no UserPick still follows skill routing. Do not skip the pre-pin
/// steer just because the session has not pinned yet.
#[tokio::test]
async fn skill_routing_still_steers_unpinned_session_without_user_pick() {
    let state = temp_state_file("skill-unpinned-steer");
    let log = temp_log("skill-unpinned-steer");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         skill_routing:\n  - pattern: ship-pr\n    selection: first-match\n\
         \x20   candidates: [\"*m2*\", \"*m3*\"]\n\
         agents:\n{}{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "c",
            &[("m3", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "please run ship-pr on this branch").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:m2:") || text.contains("skill `ship-pr` steering"),
            "unpinned /ship-pr with no UserPick must still skill-steer: {text}"
        );
        assert!(
            !text.contains("echo:m1:please run ship-pr"),
            "must not fall through to the default first agent: {text}"
        );
        Ok(())
    })
    .await;
}

/// `terse_handoff: true` sends the terse briefing instruction instead of the
/// full summary prompt, and the new model's seeded context is framed as a
/// terse briefing (not "a summary of the conversation").
#[tokio::test]
async fn skill_routing_terse_handoff_sends_brief_instruction() {
    let state = temp_state_file("skill-terse");
    let log = temp_log("skill-terse");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         skill_routing:\n  - pattern: ship-pr\n    candidates: [\"*m2*\"]\n\
         \x20   terse_handoff: true\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;
        let resp = prompt_text(&cx, &sid, "please run ship-pr on PR 42").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "skill switch happened: {text}"
        );

        let prompts: Vec<(String, String)> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "prompt")
            .map(|e| {
                (
                    e["model"].as_str().unwrap_or("").to_string(),
                    e["text"].as_str().unwrap_or("").to_string(),
                )
            })
            .collect();
        // The old model got the TERSE instruction, not the full-summary one.
        assert!(
            prompts
                .iter()
                .any(|(m, t)| m == "m1" && t.contains("Do not summarize the conversation")),
            "old model got the terse briefing instruction, not the full summary prompt: {prompts:?}"
        );
        assert!(
            !prompts
                .iter()
                .any(|(m, t)| m == "m1"
                    && t.contains("Write a concise but complete handoff summary")),
            "old model must NOT get the full-summary instruction on a terse route: {prompts:?}"
        );
        // The new model's seeded context is framed as a terse briefing and
        // points at the transcript command, not framed as "a summary".
        assert!(
            prompts.iter().any(|(m, t)| m == "m2"
                && t.contains("deliberately TERSE briefing")
                && t.contains("transcript --state")
                && t.contains("please run ship-pr on PR 42")),
            "new model got the terse frame + transcript pointer + prompt: {prompts:?}"
        );
        assert!(
            !prompts
                .iter()
                .any(|(m, t)| m == "m2" && t.contains("a summary of the conversation so far")),
            "new model must not be told this is a full summary: {prompts:?}"
        );
        Ok(())
    })
    .await;
}

/// Live bug (Kory Code session `cda39082…`, PR #7328): a mid-flight
/// `/ship-pr` demoted the session to `codex/gpt-5.6-terra` — outside the
/// skill's own `candidates` — because demotion picks the globally
/// highest-quality cheaper candidate with no awareness of the skill pool
/// that elevated the pin. `b` here plays that role: cheaper than the current
/// pin, outside the skill's `["*a*", "*c*"]` target pool, and scored HIGHER
/// than every in-pool alternative, so an unrestricted demotion always prefers
/// it. `d` is cheaper still and has the highest score, but belongs only to
/// `also_acceptable`, whose contract says it is never a switch target. The fix
/// must land the demotion on `c` (in-pool) instead of either one.
#[tokio::test]
async fn demotion_of_a_skill_elevated_pin_stays_inside_the_skill_pool() {
    let state = temp_state_file("skill-demote-pool");
    let scores = skill_pool_demotion_scores("skill-demote-pool");
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\ndemotion: {{ after_quiet_turns: 1 }}\n\
         skill_routing:\n  - pattern: ship-pr\n    candidates: [\"*a*\", \"*c*\"]\n\
         \x20   also_acceptable: [\"*d*\"]\n\
         agents:\n{}{}{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 5)], &[]), // current pin: expensive, low quality
        agent_yaml("b", &[("m2", 4)], &[]), // OUTSIDE the skill pool, highest quality
        agent_yaml("c", &[("m3", 3)], &[]), // in-pool, cheapest
        agent_yaml("d", &[("m4", 2)], &[]), // acceptable to stay on, never a target
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;
        // ship-pr elevates (a/m1 already matches the skill pool, so this is
        // the "already_ok" path, not a switch). `after_quiet_turns: 1` means
        // this very turn's own (unstruggled) completion already satisfies
        // the clock, queuing the demotion for the next prompt.
        prompt_text(&cx, &sid, "please run ship-pr on this branch").await?;
        let resp = prompt_text(&cx, &sid, "poll status").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → c/m3"),
            "demotion must land inside the skill's own candidate pool: {text}"
        );
        assert!(
            !text.contains("→ b/m2"),
            "demotion must not pick the highest-quality candidate outside the skill pool: {text}"
        );
        assert!(
            !text.contains("→ d/m4"),
            "demotion must not turn also_acceptable into a switch target: {text}"
        );
        Ok(())
    })
    .await;
}

fn skill_pool_demotion_scores(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "router-acp-skillpoolscores-{tag}-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &p,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"b/*\", default_quality: 0.90 }\n\
         \x20 - { pattern: \"d/*\", default_quality: 0.95 }\n\
         \x20 - { pattern: \"c/*\", default_quality: 0.60 }\n\
         \x20 - { pattern: \"a/*\", default_quality: 0.40 }\n",
    )
    .unwrap();
    p
}

/// Live bug companion: re-invoking `ship-pr` on an already-compliant pin was
/// a complete no-op, so the demotion clock kept counting from the FIRST
/// invocation — a second `/ship-pr` mid-flow did nothing to stop an elevated
/// pin expiring under it. Re-invocation must reset the quiet-turn clock.
#[tokio::test]
async fn skill_reinvocation_rearms_the_demotion_clock() {
    let state = temp_state_file("skill-rearm");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\ndemotion: {{ after_quiet_turns: 3 }}\n\
         skill_routing:\n  - pattern: ship-pr\n    selection: first-match\n\
         \x20   candidates: [\"*a*\", \"*c*\"]\n\
         agents:\n{}{}{}",
        state.display(),
        agent_yaml("a", &[("m1", 2)], &[]),
        agent_yaml("b", &[("m2", 3)], &[]), // starting pin, outside the skill pool
        agent_yaml("c", &[("m3", 1)], &[]), // in-pool demotion target
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Start outside the skill pool, so the FIRST ship-pr is a real steer
        // (the path that sets the elevation), matching the live session.
        prompt_text(&cx, &sid, "[router: candidate=b/m2]\nwarm up").await?;
        // T2: ship-pr switches to a/m1 and elevates. The clock starts at 0,
        // but this turn's own (unstruggled) completion already ticks it to 1.
        prompt_text(&cx, &sid, "please run ship-pr on this branch").await?;
        // T3: a quiet turn — clock at 2.
        prompt_text(&cx, &sid, "CI is still running").await?;
        // T4: re-invoke ship-pr. Without the fix this is a no-op and the
        // clock (already at 2) ticks to 3 at THIS turn's own completion —
        // one turn early. With the fix it re-arms to 0, then ticks to 1.
        prompt_text(&cx, &sid, "please run ship-pr on this branch again").await?;
        // T5: clock at 2 (with the fix) / already demoted (without it).
        prompt_text(&cx, &sid, "still waiting on CI").await?;
        // T6: clock at 3 (with the fix) — demotion queues at THIS turn's
        // completion, to fire on the next prompt.
        let r6 = prompt_text(&cx, &sid, "checking again").await?;
        assert_eq!(r6.stop_reason, StopReason::EndTurn);
        let text_by_t6 = agent_text(&observed, &sid);
        assert!(
            !text_by_t6.contains("switched a/m1"),
            "re-invocation must reset the quiet-turn clock, not no-op \
             (a broken clock would have already demoted by now): {text_by_t6}"
        );
        // T7: the queued demotion fires before forwarding this prompt.
        let r7 = prompt_text(&cx, &sid, "one more poll").await?;
        assert_eq!(r7.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → c/m3"),
            "demotion fires on schedule from the re-armed clock: {text}"
        );
        Ok(())
    })
    .await;
}

/// A pin that becomes usage-cordoned mid-session is proactively switched off
/// even when the prompt does not invoke a skill (no waiting for a rate-limit
/// error to trip reactive failover).
#[tokio::test]
async fn usage_cordoned_pin_switches_proactively() {
    let state = temp_state_file("proactive-cordon");
    let log = temp_log("proactive-cordon");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\ncordon: {{ enabled: false }}\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;

        let mut cordons = std::collections::HashMap::new();
        cordons.insert(
            router_acp::candidate::CandidateId::new("a", "m1"),
            router_acp::headroom::UsageCordon {
                reason: "Weekly usage limit reached".to_string(),
                resets_at: std::time::SystemTime::now() + std::time::Duration::from_secs(7200),
                resets_at_rfc3339: "2099-01-01T00:00:00+00:00".to_string(),
            },
        );
        shared.headroom.lock().unwrap().set_usage_cordons(cordons);

        let resp = prompt_text(&cx, &sid, "continue the work").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("usage cordon") || text.contains("switched a/m1 → b/m2"),
            "proactive switch disclosed: {text}"
        );
        assert!(
            text.contains("echo:m2:"),
            "turn must land on non-cordoned b/m2: {text}"
        );
        Ok(())
    })
    .await;
}

/// Live bug: a Fable pin-rewrite 400 cordoned the incumbent, then the
/// proactive escape used `*` + max-quality and landed on Sol — two
/// equal-quality models. Escaping a dead seat must skip quality-peers when
/// a real alternative exists.
#[tokio::test]
async fn usage_cordon_escape_skips_equal_quality_peer() {
    let state = temp_state_file("proactive-cordon-peer");
    let log = temp_log("proactive-cordon-peer");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    // a/m1 (incumbent) and b/m2 (peer, +0.02 raw ≈ 0.007 normalized) sit in
    // the same quality band. c/m3 is a real step down. Cordon m1 → must
    // land on m3, not the quality-max peer m2.
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"b/*\", default_quality: 2.92 }\n\
         \x20 - { pattern: \"a/*\", default_quality: 2.90 }\n\
         \x20 - { pattern: \"c/*\", default_quality: 2.00 }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\ncordon: {{ enabled: false }}\n\
         agents:\n{}{}{}",
        state.display(),
        scores.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "c",
            &[("m3", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;

        let mut cordons = std::collections::HashMap::new();
        cordons.insert(
            router_acp::candidate::CandidateId::new("a", "m1"),
            router_acp::headroom::UsageCordon {
                reason: "provider rejected pin rewrite".to_string(),
                resets_at: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
                resets_at_rfc3339: "2099-01-01T00:00:00+00:00".to_string(),
            },
        );
        shared.headroom.lock().unwrap().set_usage_cordons(cordons);

        let resp = prompt_text(&cx, &sid, "continue the work").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            !text.contains("echo:m2:") && !text.contains("switched a/m1 → b/m2"),
            "must not swap onto an equal-quality peer: {text}"
        );
        assert!(
            text.contains("echo:m3:") || text.contains("switched a/m1 → c/m3"),
            "escape must land on the non-peer alternative: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn delegate_children_inherit_run_label_and_parent() {
    // A labelled session that delegates gets a linked sub-session row that
    // carries the parent's run label.
    let state = temp_state_file("label-deleg");
    let log = temp_log("label-deleg");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: true, max_concurrent: 3 }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "cheap",
            &[("haiku", 1)],
            &[
                ("MOCK_LOG", &log.display().to_string()),
                ("MOCK_SESSION_MODES", "default,bypassPermissions"),
            ]
        )
        .replace(
            "    models:\n",
            "    mode_map: { auto: bypassPermissions }\n    models:\n",
        ),
        agent_yaml("fancy", &[("opus", 3)], &[]),
    );
    let state_path = state.clone();
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // A label directive whose prompt also drives the mock to actually call
        // delegate_task.
        prompt_text(
            &cx,
            &sid,
            "[router: label=nightly]\nhandle it\nDELEGATE:do the subtask",
        )
        .await?;
        let db = open_state(&state_path);
        let parent = db.get(&sid).expect("parent row");
        assert_eq!(parent.run_label.as_deref(), Some("nightly"));
        let children: Vec<_> = db
            .all()
            .into_iter()
            .filter(|(_, s)| s.parent_session_id.as_deref() == Some(sid.as_str()))
            .collect();
        assert_eq!(children.len(), 1, "one delegate row linked to the parent");
        let (_, child) = &children[0];
        assert_eq!(child.kind, "delegate");
        assert_eq!(
            child.run_label.as_deref(),
            Some("nightly"),
            "delegate child inherits the run label"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn low_confidence_pin_auto_upgrades_to_a_more_capable_model() {
    let state = temp_state_file("upgrade");
    // Score table: a/m1 is under-powered (0.40, below the 0.55 default
    // threshold), b/m2 is capable (0.90). Pinning a/m1 should trigger an
    // auto-upgrade to b/m2 on the following prompt.
    let scores = std::env::temp_dir().join(format!(
        "router-acp-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"b/*\", default_quality: 0.90 }\n\
         \x20 - { pattern: \"a/*\", default_quality: 0.40 }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: true }}\nagents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Pin the under-powered model explicitly.
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nfirst turn").await?;
        // Next prompt: the queued auto-upgrade fires before forwarding.
        let resp = prompt_text(&cx, &sid, "second turn").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("below threshold"),
            "auto-upgrade disclosed: {text}"
        );
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "upgraded to the capable model: {text}"
        );
        assert!(
            text.contains("echo:m2:"),
            "ran on the capable model: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn low_confidence_pin_upgrades_across_a_compressed_gap_when_nothing_else_qualifies() {
    let state = temp_state_file("upgrade-compressed");
    // Mirrors a declared score-table compression pair (Fable/Opus, Sol/Terra):
    // b/m2 sits only 0.02 above a/m1 on the raw 0.5..3.5 scale — normalized
    // ~0.007, well under the +0.05 real-upgrade margin. a/m1's 0.85 puts the
    // pinned session's confidence at (0.85−0.5)/(1.2−0.5) = 0.50, below the
    // 0.55 default threshold, so auto-upgrade fires — and its only possible
    // target is the compressed sibling.
    let scores = std::env::temp_dir().join(format!(
        "router-acp-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"b/*\", default_quality: 0.87 }\n\
         \x20 - { pattern: \"a/*\", default_quality: 0.85 }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: true }}\nagents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nfirst turn").await?;
        let resp = prompt_text(&cx, &sid, "second turn").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "upgraded across the compressed gap despite missing the +0.05 margin: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn auto_upgrade_is_off_by_default_and_keeps_the_pinned_model() {
    // Live bug: a Grok pin was auto-upgraded to Opus on an automated relay
    // nudge ("confidence 0.25 below threshold 0.40") — a full summarize +
    // re-pin that dropped the live context onto a model the provider then
    // rejected. The engine is opt-in now: no `auto_upgrade` key, no switch.
    let state = temp_state_file("noupgrade");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"b/*\", default_quality: 0.90 }\n\
         \x20 - { pattern: \"a/*\", default_quality: 0.40 }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nfirst turn").await?;
        let resp = prompt_text(&cx, &sid, "second turn").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            !text.contains("below threshold") && !text.contains("switched"),
            "no upgrade happened: {text}"
        );
        assert!(text.contains("echo:m1:second turn"), "stayed on m1: {text}");
        Ok(())
    })
    .await;
}

// ======================================================================
// escalation router
// ======================================================================

/// Score table making `b/*` strong (0.90) and `a/*` weak (0.40), so `b` is a
/// valid escalation target above `a`. Optionally a mid-tier `c/*` (0.70).
fn escalation_scores(tag: &str, with_mid: bool) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "router-acp-escscores-{tag}-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    let mid = if with_mid {
        "  - { pattern: \"c/*\", default_quality: 0.70 }\n"
    } else {
        ""
    };
    std::fs::write(
        &p,
        format!(
            "version: 1\ncandidates:\n\
             \x20 - {{ pattern: \"b/*\", default_quality: 0.90 }}\n{mid}\
             \x20 - {{ pattern: \"a/*\", default_quality: 0.40 }}\n"
        ),
    )
    .unwrap();
    p
}

#[tokio::test]
async fn escalation_router_starts_on_the_cheapest_candidate() {
    let state = temp_state_file("esc-start");
    let yaml = format!(
        "state_file: {}\nrouter: escalation\ndelegation: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("a", &[("m1", 1)], &[]), // cheapest by cost rank
        agent_yaml("b", &[("m2", 3)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "just say hi").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("escalation → a/m1"),
            "started cheapest: {text}"
        );
        assert!(
            text.contains("echo:m1:"),
            "ran on the cheapest model: {text}"
        );
        Ok(())
    })
    .await;
}

/// True when a mock log recorded at least one `prompt` event (the agent ran a
/// real turn), i.e. the router actually forwarded a prompt to it.
fn got_prompt(log: &PathBuf) -> bool {
    read_log(log).iter().any(|e| e["event"] == "prompt")
}

#[tokio::test]
async fn escalation_escalates_mid_turn_and_leap_skips_the_intermediate() {
    let state = temp_state_file("esc-mid");
    let scores = escalation_scores("mid", true); // a=0.4, c=0.7, b=0.9
    let (logc, logb) = (temp_log("esc-mid-c"), temp_log("esc-mid-b"));
    let yaml = format!(
        "state_file: {}\nscore_table: {}\nrouter: escalation\ndelegation: {{ enabled: false }}\n\
         routers:\n  escalation:\n    escalation_path: leap\n    escalate_after_reads: 3\n\
         agents:\n{}{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml(
            "c",
            &[("m3", 2)],
            &[("MOCK_LOG", &logc.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 3)],
            &[("MOCK_LOG", &logb.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // A task that looks simple but investigates heavily (5 reads > threshold
        // 3) before producing any output → mid-turn escalation.
        let resp = prompt_text(
            &cx,
            &sid,
            "READFILE:/a\nREADFILE:/b\nREADFILE:/c\nREADFILE:/d\nREADFILE:/e\ninvestigate this",
        )
        .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(text.contains("escalation → a/m1"), "started cheap: {text}");
        // `leap` jumps straight to the strongest, skipping the mid tier.
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "leaped mid-turn to the strongest model: {text}"
        );
        assert!(got_prompt(&logb), "strongest model b/m2 ran the replay");
        assert!(!got_prompt(&logc), "leap skipped the intermediate tier c");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn escalation_before_side_effects_false_disables_mid_turn() {
    let state = temp_state_file("esc-noeff");
    let scores = escalation_scores("noeff", false);
    let (loga, logb) = (temp_log("esc-noeff-a"), temp_log("esc-noeff-b"));
    let yaml = format!(
        "state_file: {}\nscore_table: {}\nrouter: escalation\ndelegation: {{ enabled: false }}\n\
         routers:\n  escalation:\n    escalate_before_side_effects: false\n    escalate_after_reads: 3\n\
         agents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &loga.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &logb.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(
            &cx,
            &sid,
            "READFILE:/a\nREADFILE:/b\nREADFILE:/c\nREADFILE:/d\ninvestigate this",
        )
        .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        // Reads alone must NOT escalate when mid-turn is off; stays on cheap.
        assert!(!text.contains("switched"), "no mid-turn escalation: {text}");
        assert!(got_prompt(&loga), "cheap model a/m1 ran");
        assert!(!got_prompt(&logb), "strong model b/m2 never ran");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn escalation_ladder_steps_one_tier_up() {
    let state = temp_state_file("esc-ladder");
    let scores = escalation_scores("ladder", true); // a=0.4, c=0.7, b=0.9
    let (logc, logb) = (temp_log("esc-ladder-c"), temp_log("esc-ladder-b"));
    let yaml = format!(
        "state_file: {}\nscore_table: {}\nrouter: escalation\ndelegation: {{ enabled: false }}\n\
         routers:\n  escalation:\n    escalation_path: ladder\n    escalate_after_reads: 3\n    max_escalations: 1\n\
         agents:\n{}{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml(
            "c",
            &[("m3", 2)],
            &[("MOCK_LOG", &logc.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 3)],
            &[("MOCK_LOG", &logb.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(
            &cx,
            &sid,
            "READFILE:/a\nREADFILE:/b\nREADFILE:/c\nREADFILE:/d\ninvestigate this",
        )
        .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        // Ladder goes to the NEXT tier up (c/0.70), not straight to b/0.90.
        // Capped at one escalation so it stops at c (proving the single step).
        assert!(
            text.contains("switched a/m1 → c/m3"),
            "ladder stepped one tier up: {text}"
        );
        assert!(got_prompt(&logc), "mid-tier c ran the replay");
        assert!(
            !got_prompt(&logb),
            "ladder stepped to c, not straight to the top b"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn escalation_post_turn_on_max_tokens_stop() {
    let state = temp_state_file("esc-post");
    let scores = escalation_scores("post", false);
    let yaml = format!(
        "state_file: {}\nscore_table: {}\nrouter: escalation\ndelegation: {{ enabled: false }}\n\
         routers:\n  escalation:\n    escalation_path: leap\n    escalate_before_side_effects: false\n\
         agents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // First turn ends with max-tokens on the cheap model → queues escalation.
        let r1 = prompt_text(&cx, &sid, "MAXTOKENS do the thing").await?;
        assert_eq!(r1.stop_reason, StopReason::MaxTokens);
        // Next turn fires the queued escalation before forwarding.
        let r2 = prompt_text(&cx, &sid, "continue").await?;
        assert_eq!(r2.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "post-turn escalation happened: {text}"
        );
        assert!(text.contains("echo:m2:"), "ran on the strong model: {text}");
        assert!(
            text.contains("continue"),
            "the continuation prompt was forwarded: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn demotion_expires_an_escalated_pin_after_quiet_turns() {
    let state = temp_state_file("demote");
    let scores = escalation_scores("demote", false);
    let yaml = format!(
        "state_file: {}\nscore_table: {}\nrouter: escalation\ndelegation: {{ enabled: false }}\n\
         demotion: {{ after_quiet_turns: 2 }}\n\
         routers:\n  escalation:\n    escalation_path: leap\n    escalate_before_side_effects: false\n\
         agents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Turn 1: max-tokens on the cheap model → queues an escalation.
        let r1 = prompt_text(&cx, &sid, "MAXTOKENS do the thing").await?;
        assert_eq!(r1.stop_reason, StopReason::MaxTokens);
        // Turn 2: the escalation fires; a clean turn on the strong model
        // (quiet 1 of 2).
        let r2 = prompt_text(&cx, &sid, "continue").await?;
        assert_eq!(r2.stop_reason, StopReason::EndTurn);
        // Turn 3: second clean turn — the demotion clock expires the verdict.
        let r3 = prompt_text(&cx, &sid, "keep going").await?;
        assert_eq!(r3.stop_reason, StopReason::EndTurn);
        // Turn 4: the queued demotion fires before forwarding; the prompt
        // lands back on the cheap model.
        let r4 = prompt_text(&cx, &sid, "poll status").await?;
        assert_eq!(r4.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "escalation happened first: {text}"
        );
        assert!(
            text.contains("demoting to a/m1"),
            "demotion was disclosed: {text}"
        );
        assert!(
            text.contains("switched b/m2 → a/m1"),
            "demotion switch happened: {text}"
        );
        assert!(
            text.contains("echo:m1:") && text.contains("poll status"),
            "the post-demotion prompt ran on the cheap model: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn demotion_disabled_by_default_keeps_the_escalated_pin() {
    let state = temp_state_file("demote-off");
    let scores = escalation_scores("demote-off", false);
    let yaml = format!(
        "state_file: {}\nscore_table: {}\nrouter: escalation\ndelegation: {{ enabled: false }}\n\
         routers:\n  escalation:\n    escalation_path: leap\n    escalate_before_side_effects: false\n\
         agents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let _ = prompt_text(&cx, &sid, "MAXTOKENS do the thing").await?;
        for p in ["continue", "keep going", "poll status", "one more"] {
            let r = prompt_text(&cx, &sid, p).await?;
            assert_eq!(r.stop_reason, StopReason::EndTurn);
        }
        let text = agent_text(&observed, &sid);
        assert!(
            !text.contains("demoting to"),
            "no demotion without `demotion.after_quiet_turns`: {text}"
        );
        Ok(())
    })
    .await;
}

/// Build an escalation config that isolates one trigger (others disabled).
fn esc_yaml(state: &std::path::Path, scores: &std::path::Path, body: &str, logb: &str) -> String {
    format!(
        "state_file: {}\nscore_table: {}\nrouter: escalation\ndelegation: {{ enabled: false }}\n\
         routers:\n  escalation:\n    escalation_path: leap\n{body}\
         agents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml("a", &[("m1", 1)], &[]),
        agent_yaml("b", &[("m2", 2)], &[("MOCK_LOG", logb)]),
    )
}

// These drive the REAL `session/update` tool_call path (via the mock's `TOOL:`
// directive), the path claude-agent-acp actually uses — the coverage gap that
// let the mid-turn bug ship.

#[tokio::test]
async fn escalation_mid_turn_on_tool_call_reads() {
    let state = temp_state_file("esc-treads");
    let scores = escalation_scores("treads", false);
    let logb = temp_log("esc-treads-b");
    // read-volume only: activity/failure off.
    let body = "    escalate_after_reads: 3\n    escalate_after_tool_calls: 0\n    escalate_after_tool_failures: 0\n    escalate_on_max_tokens: false\n    escalate_on_refusal: false\n";
    let yaml = esc_yaml(&state, &scores, body, &logb.display().to_string());
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // 8 genuine tool_call reads (no side effect) → escalate at 3.
        let prompt = "TOOL:read\n".repeat(8) + "investigate";
        let resp = prompt_text(&cx, &sid, &prompt).await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "read-volume escalated: {text}"
        );
        assert!(got_prompt(&logb), "strong model ran");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn escalation_read_only_bash_counts_as_investigation() {
    // Regression for the hickory-ai6 bug: `ls … 2>/dev/null || echo` is
    // read-only and must count toward the read trigger, not close the window.
    let state = temp_state_file("esc-robash");
    let scores = escalation_scores("robash", false);
    let logb = temp_log("esc-robash-b");
    let body = "    escalate_after_reads: 3\n    escalate_after_tool_calls: 0\n    escalate_after_tool_failures: 0\n    escalate_on_max_tokens: false\n    escalate_on_refusal: false\n";
    let yaml = esc_yaml(&state, &scores, body, &logb.display().to_string());
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let prompt = "TOOL:exec:ls -la /tmp 2>/dev/null || echo nope\n".repeat(8) + "investigate";
        let resp = prompt_text(&cx, &sid, &prompt).await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "read-only shell counted as investigation and escalated: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn escalation_mid_turn_on_tool_call_volume_despite_side_effects() {
    // The hickory-ai6 case: a turn grinding through many tool calls (with
    // edits/side effects) must escalate on total volume, not need pre-side-
    // effect reads.
    let state = temp_state_file("esc-vol");
    let scores = escalation_scores("vol", false);
    let logb = temp_log("esc-vol-b");
    let body = "    escalate_after_tool_calls: 5\n    escalate_after_reads: 0\n    escalate_after_tool_failures: 0\n    escalate_on_max_tokens: false\n    escalate_on_refusal: false\n";
    let yaml = esc_yaml(&state, &scores, body, &logb.display().to_string());
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // 12 edits (all side effects) → activity trigger fires at 5.
        let prompt = "TOOL:edit\n".repeat(12) + "keep grinding";
        let resp = prompt_text(&cx, &sid, &prompt).await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "tool-call volume escalated despite side effects: {text}"
        );
        assert!(got_prompt(&logb), "strong model ran");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn escalation_mid_turn_on_tool_failures() {
    let state = temp_state_file("esc-fail");
    let scores = escalation_scores("fail", false);
    let logb = temp_log("esc-fail-b");
    let body = "    escalate_after_tool_failures: 3\n    escalate_after_reads: 0\n    escalate_after_tool_calls: 0\n    escalate_on_max_tokens: false\n    escalate_on_refusal: false\n";
    let yaml = esc_yaml(&state, &scores, body, &logb.display().to_string());
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let prompt = "TOOL:fail\n".repeat(6) + "thrash";
        let resp = prompt_text(&cx, &sid, &prompt).await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "tool-failure churn escalated mid-turn: {text}"
        );
        assert!(got_prompt(&logb), "strong model ran");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn escalation_escalates_across_a_compressed_gap_when_nothing_else_qualifies() {
    // Mirrors a declared score-table compression pair (Fable/Opus, Sol/Terra):
    // b/m2 sits only 0.02 above a/m1 on the raw scale — normalized well under
    // the +0.05 real-upgrade margin `escalation_target` prefers — so a bug
    // that survives multiple rounds of thrashing must still be able to climb
    // onto the compressed-ahead sibling instead of getting stuck.
    let state = temp_state_file("esc-fail-compressed");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-escscores-compressed-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"b/*\", default_quality: 1.42 }\n\
         \x20 - { pattern: \"a/*\", default_quality: 1.40 }\n",
    )
    .unwrap();
    let logb = temp_log("esc-fail-compressed-b");
    let body = "    escalate_after_tool_failures: 3\n    escalate_after_reads: 0\n    escalate_after_tool_calls: 0\n    escalate_on_max_tokens: false\n    escalate_on_refusal: false\n";
    let yaml = esc_yaml(&state, &scores, body, &logb.display().to_string());
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let prompt = "TOOL:fail\n".repeat(6) + "thrash";
        let resp = prompt_text(&cx, &sid, &prompt).await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched a/m1 → b/m2"),
            "tool-failure churn escalated across the compressed gap: {text}"
        );
        assert!(got_prompt(&logb), "compressed-ahead model ran");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn escalation_initial_router_delegates_the_start() {
    // `initial_router: auto` starts on auto's pick (quality-weighted) instead
    // of the cheapest, then escalation applies from there.
    let state = temp_state_file("esc-init");
    // auto with pure quality picks the highest-quality candidate.
    let yaml = format!(
        "state_file: {}\nrouter: escalation\ndelegation: {{ enabled: false }}\n\
         routers:\n  escalation:\n    initial_router: auto\n  auto: {{ cost_quality_tradeoff: 0 }}\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml("cheap", &[("haiku", 1)], &[]),
        agent_yaml("strong", &[("opus", 3)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "hello").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        // Delegated to auto (pure quality) → starts on the STRONG model, not cheapest.
        assert!(
            text.contains("escalation → strong/opus"),
            "delegated start to auto's pick: {text}"
        );
        assert!(
            text.contains("delegated to `auto`"),
            "disclosure notes delegation: {text}"
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Milestone 7: lifecycle odds and ends
// ======================================================================

#[tokio::test]
async fn close_unpinned_session_removes_router_state() {
    let state = temp_state_file("close");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        cx.send_request(CloseSessionRequest::new(sid.clone()))
            .block_task()
            .await?;
        // The session is gone: prompting it errors.
        let err = prompt_text(&cx, &sid, "gone").await.unwrap_err();
        assert!(format!("{err}").contains("unknown session"), "got: {err}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn pre_pin_set_mode_is_deferred_and_applied_at_pin() {
    // goose sets its GOOSE_MODE via session/set_mode immediately after
    // session/new, before any prompt. The router must accept it and apply it
    // to the downstream session once pinned.
    let state = temp_state_file("mode-defer");
    let log = temp_log("mode-defer");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1)],
            &[
                ("MOCK_SESSION_MODES", "default,auto"),
                ("MOCK_LOG", &log.display().to_string()),
            ],
        )
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        // Pre-pin set_mode must succeed (goose aborts the session otherwise).
        cx.send_request(
            agent_client_protocol::schema::v1::SetSessionModeRequest::new(
                sid.clone(),
                "auto".to_string(),
            ),
        )
        .block_task()
        .await?;
        // Nothing reached the downstream yet.
        assert!(!read_log(&log).iter().any(|e| e["event"] == "set_mode"));

        let resp = prompt_text(&cx, &sid, "after mode").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(agent_text(&observed, &sid).contains("echo:m1:after mode"));

        // The deferred mode was applied to the pinned downstream session.
        let set_modes: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "set_mode")
            .collect();
        assert_eq!(set_modes.len(), 1, "{set_modes:?}");
        assert_eq!(set_modes[0]["modeId"], "auto");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn pre_pin_set_mode_with_mode_map_translation() {
    let state = temp_state_file("mode-map");
    let log = temp_log("mode-map");
    // The client asks for `auto`; this agent only has `yolo`, and the config
    // translates auto -> yolo.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1)],
            &[
                ("MOCK_SESSION_MODES", "default,yolo"),
                ("MOCK_LOG", &log.display().to_string()),
            ],
        )
        .replace(
            "    model_selection:",
            "    mode_map: { auto: yolo }\n    model_selection:"
        )
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        cx.send_request(
            agent_client_protocol::schema::v1::SetSessionModeRequest::new(
                sid.clone(),
                "auto".to_string(),
            ),
        )
        .block_task()
        .await?;
        prompt_text(&cx, &sid, "mapped").await?;
        let set_modes: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "set_mode")
            .collect();
        assert_eq!(set_modes.len(), 1, "{set_modes:?}");
        assert_eq!(set_modes[0]["modeId"], "yolo");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn unsupported_mode_is_skipped_without_failing_the_session() {
    let state = temp_state_file("mode-skip");
    let log = temp_log("mode-skip");
    // Downstream advertises no modes at all: set_mode still succeeds
    // upstream (pre- and post-pin), and nothing is forwarded.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())],
        )
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        cx.send_request(
            agent_client_protocol::schema::v1::SetSessionModeRequest::new(
                sid.clone(),
                "auto".to_string(),
            ),
        )
        .block_task()
        .await?;
        let resp = prompt_text(&cx, &sid, "still works").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(agent_text(&observed, &sid).contains("echo:m1:still works"));
        // Post-pin unsupported mode is also lenient.
        cx.send_request(
            agent_client_protocol::schema::v1::SetSessionModeRequest::new(
                sid.clone(),
                "auto".to_string(),
            ),
        )
        .block_task()
        .await?;
        assert!(!read_log(&log).iter().any(|e| e["event"] == "set_mode"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn a_reloaded_session_gets_its_router_tools_back() {
    let state = temp_state_file("reload-tools");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: true, candidate_hints: exact }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("fancy", &[("opus", 3)], &[("MOCK_SUPPORTS_LIFECYCLE", "1")]),
        agent_yaml("peer", &[("sonnet", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "hard integration work").await?;
        cx.send_request(CloseSessionRequest::new(sid.clone()))
            .block_task()
            .await?;
        cx.send_request(agent_client_protocol::schema::v1::LoadSessionRequest::new(
            sid.clone(),
            std::env::temp_dir(),
        ))
        .block_task()
        .await?;
        prompt_text(
            &cx,
            &sid,
            "MCP_CALL:router-delegate delegate_task {\"task\":\"after reload\",\"hints\":{\"candidate\":\"peer/sonnet\"}}",
        )
        .await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:sonnet:after reload"),
            "the reloaded parent can still delegate: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prompt_command_answers_without_a_client_and_finds_sessions_by_provider_id() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("router.yaml");
    std::fs::write(
        &config,
        format!(
            "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
            dir.path().join("state.db").display(),
            agent_yaml("mock", &[("m1", 1)], &[("MOCK_SUPPORTS_LIFECYCLE", "1")])
        ),
    )
    .unwrap();
    let run = |args: &[&str]| {
        std::process::Command::new(router_exe())
            .arg("prompt")
            .arg("--config")
            .arg(&config)
            .args(args)
            .output()
            .unwrap()
    };
    let first = run(&["--message", "hello there"]);
    let stdout = String::from_utf8_lossy(&first.stdout);
    let stderr = String::from_utf8_lossy(&first.stderr);
    assert!(first.status.success(), "{stderr}");
    assert!(
        stdout.contains("hello there"),
        "the turn streams to stdout: {stdout}"
    );
    let router_sid = regex::Regex::new(r"router-acp session: (rtr-[0-9a-f-]+)")
        .unwrap()
        .captures(&stderr)
        .map(|c| c[1].to_string())
        .unwrap_or_else(|| panic!("session id reported: {stderr}"));

    // A supervisor knows only the provider session id its hooks recorded.
    let downstream = router_acp::state::StateFile::load(
        &dir.path().join("state.db"),
        router_acp::state::Retention::default(),
    )
    .get(&router_sid)
    .unwrap()
    .downstream_session_id;
    let second = run(&["--session", &downstream, "--message", "again"]);
    let stderr = String::from_utf8_lossy(&second.stderr);
    // The mock keeps sessions only in its own process, so this process's mock
    // cannot reload it; the router still resolved the right session first.
    assert!(
        stderr.contains(&format!("router-acp session: {router_sid}"))
            || stderr.contains("unknown session"),
        "resolved by provider id: {stderr}"
    );
    let unknown = run(&["--session", "nope", "--message", "x"]);
    assert!(!unknown.status.success());
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("no router session matches `nope`"));
}

#[tokio::test]
async fn lifecycle_list_load_delete_roundtrip() {
    let state = temp_state_file("lifecycle");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[("MOCK_SUPPORTS_LIFECYCLE", "1")])
    );
    run_test(yaml, async |cx, observed| {
        let init_resp = init(&cx).await?;
        // Downstream supports lifecycle -> the router advertises it.
        assert!(init_resp.agent_capabilities.load_session);
        assert!(
            init_resp
                .agent_capabilities
                .session_capabilities
                .list
                .is_some()
        );

        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();
        prompt_text(&cx, &sid, "persist me").await?;

        // session/list merges downstream lists rewritten to router ids.
        let list = cx
            .send_request(agent_client_protocol::schema::v1::ListSessionsRequest::new())
            .block_task()
            .await?;
        let listed: Vec<String> = list
            .sessions
            .iter()
            .map(|s| s.session_id.0.to_string())
            .collect();
        assert!(listed.contains(&sid), "router id in list: {listed:?}");
        // Downstream ids (mock-sess-*) never leak upstream.
        assert!(listed.iter().all(|s| s.starts_with("rtr-")), "{listed:?}");

        // Close the live session, then load it back by router id.
        cx.send_request(CloseSessionRequest::new(sid.clone()))
            .block_task()
            .await?;
        let load = cx
            .send_request(agent_client_protocol::schema::v1::LoadSessionRequest::new(
                sid.clone(),
                std::env::temp_dir(),
            ))
            .block_task()
            .await?;
        assert!(load.config_options.is_some());
        // The replayed transcript update relays under the router id.
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("replayed:mock-sess-"),
            "replay relayed: {text}"
        );

        // The rehydrated pin routes follow-up prompts to the same session.
        let resp = prompt_text(&cx, &sid, "after load").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(agent_text(&observed, &sid).contains("echo:m1:after load"));

        // Delete removes downstream and router state.
        cx.send_request(agent_client_protocol::schema::v1::DeleteSessionRequest::new(sid.clone()))
            .block_task()
            .await?;
        let err = prompt_text(&cx, &sid, "deleted").await.unwrap_err();
        assert!(format!("{err}").contains("unknown session"), "got: {err}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn mock_lifecycle_capabilities_not_advertised_when_unsupported() {
    let state = temp_state_file("caps-adv");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, _observed| {
        let init_resp = init(&cx).await?;
        let caps = &init_resp.agent_capabilities;
        assert!(!caps.load_session);
        assert!(caps.session_capabilities.list.is_none());
        assert!(caps.session_capabilities.resume.is_none());
        assert!(caps.session_capabilities.close.is_none());
        // The mock advertises embedded_context: the union carries it.
        assert!(caps.prompt_capabilities.embedded_context);
        assert!(!caps.prompt_capabilities.image);
        Ok(())
    })
    .await;
}

// ======================================================================
// Provider usage-cap cordons
// ======================================================================

fn ordered_accounts_yaml(name: &str, availability: bool) -> String {
    let state = temp_state_file(name);
    let agents = ["first", "second", "third"]
        .iter()
        .enumerate()
        .map(|(priority, name)| {
            let mut agent = agent_yaml(&format!("claude@{name}"), &[("sonnet", 2)], &[]);
            agent.push_str(&format!("    account_priority: {priority}\n"));
            agent
        })
        .collect::<String>();
    format!(
        "state_file: {}\ndelegation: {{enabled: false}}\ncordon: {{enabled: false}}\navailability_preference: {{enabled: {availability}}}\nagents:\n{agents}",
        state.display()
    )
}

fn plan_availability(free: f64, paid: bool) -> router_acp::headroom::SeatAvailability {
    router_acp::headroom::SeatAvailability {
        plan_headroom: free,
        plan_remaining_dollars: None,
        on_overage: paid,
        overage_headroom: paid.then_some(1.0),
        overage_remaining_dollars: paid.then_some(100.0),
        source: "poll",
    }
}

#[tokio::test]
async fn account_priority_drains_first_then_second_then_third_without_bypassing_cordons() {
    for availability in [true, false] {
        let yaml = ordered_accounts_yaml("account-priority", availability);
        run_test_shared(yaml, async |cx, _observed, shared| {
            init(&cx).await?;
            shared.headroom.lock().unwrap().set_polled_availability(
                [
                    (
                        CandidateId::new("claude@first", "sonnet"),
                        plan_availability(0.01, false),
                    ),
                    (
                        CandidateId::new("claude@second", "sonnet"),
                        plan_availability(0.80, false),
                    ),
                    (
                        CandidateId::new("claude@third", "sonnet"),
                        plan_availability(1.0, false),
                    ),
                ]
                .into(),
            );
            let sid = new_session(&cx).await?.session_id.0.to_string();
            for (turn, name) in ["first", "first", "second", "third"]
                .into_iter()
                .enumerate()
            {
                prompt_text(&cx, &sid, "Continue the task").await?;
                assert_eq!(
                    shared
                        .with_session(&sid, |s| s.pin.as_ref().unwrap().candidate.agent.clone())
                        .unwrap(),
                    format!("claude@{name}")
                );
                if turn > 0 {
                    shared.headroom.lock().unwrap().cordon(
                        &format!("claude@{name}"),
                        Some(Duration::from_secs(3600)),
                        "capacity reserved",
                    );
                }
            }
            let error = prompt_text(&cx, &sid, "Continue the task")
                .await
                .expect_err("all accounts are cordoned");
            assert!(
                format!("{error}").contains("cordoned")
                    || format!("{error}").contains("no available")
            );
            Ok(())
        })
        .await;
    }
}

#[tokio::test]
async fn account_priority_honors_exclusions_and_only_user_overrides_bypass_order() {
    let yaml = ordered_accounts_yaml("account-priority-overrides", true);
    run_test_shared(yaml, async |cx, _observed, shared| {
        init(&cx).await?;
        for source in [
            router_acp::strategies::OverrideSource::Skill("review".into()),
            router_acp::strategies::OverrideSource::UserPick,
        ] {
            let user = source == router_acp::strategies::OverrideSource::UserPick;
            let sid = new_session(&cx).await?.session_id.0.to_string();
            shared.with_session(&sid, |s| {
                s.candidate_override = Some(CandidateId::new("claude@second", "sonnet"));
                s.candidate_override_source = Some(source);
            });
            prompt_text(&cx, &sid, "Complete the task").await?;
            let expected = if user {
                "claude@second"
            } else {
                "claude@first"
            };
            assert_eq!(
                shared
                    .with_session(&sid, |s| s.pin.as_ref().unwrap().candidate.agent.clone())
                    .unwrap(),
                expected
            );
        }
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid,
            "[router: exclude=claude@first]\nComplete the task",
        )
        .await?;
        assert_eq!(
            shared
                .with_session(&sid, |s| s.pin.as_ref().unwrap().candidate.agent.clone())
                .unwrap(),
            "claude@second"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn account_priority_spends_later_included_plan_before_earlier_overage() {
    for availability in [true, false] {
        let yaml = ordered_accounts_yaml("account-priority-overage", availability);
        run_test_shared(yaml, async |cx, _observed, shared| {
            init(&cx).await?;
            shared.headroom.lock().unwrap().set_polled_availability(
                [
                    (
                        CandidateId::new("claude@first", "sonnet"),
                        plan_availability(0.0, true),
                    ),
                    (
                        CandidateId::new("claude@second", "sonnet"),
                        plan_availability(0.25, false),
                    ),
                    (
                        CandidateId::new("claude@third", "sonnet"),
                        plan_availability(1.0, false),
                    ),
                ]
                .into(),
            );
            let sid = new_session(&cx).await?.session_id.0.to_string();
            prompt_text(&cx, &sid, "Complete the task").await?;
            assert_eq!(
                shared
                    .with_session(&sid, |s| s.pin.as_ref().unwrap().candidate.agent.clone())
                    .unwrap(),
                "claude@second"
            );
            let sid = new_session(&cx).await?.session_id.0.to_string();
            prompt_text(
                &cx,
                &sid,
                "[router: candidate=claude@first/sonnet]\nComplete the task",
            )
            .await?;
            assert_eq!(
                shared
                    .with_session(&sid, |s| s.pin.as_ref().unwrap().candidate.agent.clone())
                    .unwrap(),
                "claude@first"
            );
            Ok(())
        })
        .await;
    }
}

#[tokio::test]
async fn account_priority_filters_skill_models_before_plan_and_account_order() {
    for selection in ["best-quality", "first-match"] {
        for earlier_opus_overage in [false, true] {
            let first_model = if earlier_opus_overage {
                "opus"
            } else {
                "sonnet"
            };
            let second_model = if earlier_opus_overage {
                "sonnet"
            } else {
                "opus"
            };
            let expected = if earlier_opus_overage {
                "claude@first"
            } else {
                "claude@second"
            };
            let other = if earlier_opus_overage {
                "claude@second"
            } else {
                "claude@first"
            };
            let mut first = agent_yaml("claude@first", &[(first_model, 3)], &[]);
            first.push_str("    account_priority: 0\n");
            let mut second = agent_yaml("claude@second", &[(second_model, 3)], &[]);
            second.push_str("    account_priority: 1\n");
            let yaml = format!(
                "state_file: {}\ndelegation: {{enabled: false}}\ncordon: {{enabled: false}}\n\
                 auto_upgrade: {{enabled: false}}\nskill_routing:\n  - pattern: review-branch\n\
                 \x20   selection: {selection}\n    candidates: [\"*opus*\"]\nagents:\n{first}{second}",
                temp_state_file("priority-skill-models").display()
            );
            run_test_shared(yaml, async |cx, _observed, shared| {
                init(&cx).await?;
                shared.headroom.lock().unwrap().set_polled_availability(
                    [
                        (
                            CandidateId::new("claude@first", first_model),
                            plan_availability(
                                if earlier_opus_overage { 0.0 } else { 1.0 },
                                earlier_opus_overage,
                            ),
                        ),
                        (
                            CandidateId::new("claude@second", second_model),
                            plan_availability(0.25, false),
                        ),
                    ]
                    .into(),
                );
                let sid = new_session(&cx).await?.session_id.0.to_string();
                prompt_text(&cx, &sid, "Run /review-branch").await?;
                assert_eq!(
                    shared
                        .with_session(&sid, |session| session
                            .pin
                            .as_ref()
                            .unwrap()
                            .candidate
                            .clone())
                        .unwrap(),
                    CandidateId::new(expected, "opus")
                );

                let sid = new_session(&cx).await?.session_id.0.to_string();
                prompt_text(
                    &cx,
                    &sid,
                    &format!("[router: candidate={other}/sonnet]\nwarm up"),
                )
                .await?;
                prompt_text(&cx, &sid, "Run /review-branch").await?;
                assert_eq!(
                    shared
                        .with_session(&sid, |session| session
                            .pin
                            .as_ref()
                            .unwrap()
                            .candidate
                            .clone())
                        .unwrap(),
                    CandidateId::new(expected, "opus")
                );
                Ok(())
            })
            .await;
        }
    }
}

#[tokio::test]
async fn native_accounts_fail_over_without_cordoning_the_other_login() {
    let state = temp_state_file("accounts");
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nagents:\n{}    accounts:\n      - name: work\n        env: [{{name: MOCK_FAIL_PROMPT_MSG, value: 'HTTP 429 usage limit reached'}}]\n      - name: personal\n        env: []\n",
        state.display(),
        agent_yaml("claude", &[("sonnet", 2)], &[])
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let response = prompt_text(
            &cx,
            &sid,
            "[router: candidate=claude@work/sonnet]\nComplete the task",
        )
        .await?;
        assert_eq!(response.stop_reason, StopReason::EndTurn);
        assert!(agent_text(&observed, &sid).contains("failover: auto → claude@personal/sonnet"));
        let mut headroom = shared.headroom.lock().unwrap();
        assert!(headroom.cordon_active("claude@work").is_some());
        assert!(headroom.cordon_active("claude@personal").is_none());
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn provider_terms_gate_fails_over_to_another_native_account() {
    let state = temp_state_file("account-terms");
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\nagents:\n{}    accounts:\n      - name: gated\n        env: [{{name: MOCK_FAIL_PROMPT_MSG, value: \"API Error: 400 We've updated our Consumer Terms and Privacy Policy. You'll need to accept them in claude.ai to continue.\"}}]\n      - name: ready\n        env: []\n",
        state.display(),
        agent_yaml("claude", &[("sonnet", 2), ("opus", 3)], &[])
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let response = prompt_text(
            &cx,
            &sid,
            "[router: candidate=claude@gated/sonnet]\nComplete the task",
        )
        .await?;
        assert_eq!(response.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("failover: auto → claude@ready/"),
            "terms-gated account must fail over: {text}"
        );
        let mut headroom = shared.headroom.lock().unwrap();
        assert!(headroom.cordon_active("claude@gated").is_some());
        assert!(headroom.cordon_active("claude@ready").is_none());
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn all_usage_cordoned_returns_unavailable_without_prompting_any_model() {
    let state = temp_state_file("all-cordoned");
    let log = temp_log("all-cordoned");
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\ncordon: {{enabled: false}}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("opus", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("sonnet", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test_shared(yaml, async |cx, _observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let cordons = [("a", "opus"), ("b", "sonnet")]
            .into_iter()
            .map(|(agent, model)| {
                (
                    CandidateId::new(agent, model),
                    router_acp::headroom::UsageCordon {
                        reason: "Weekly usage limit reached (10% capacity reserved)".into(),
                        resets_at: std::time::SystemTime::now() + Duration::from_secs(3600),
                        resets_at_rfc3339: "2099-01-01T00:00:00Z".into(),
                    },
                )
            })
            .collect();
        shared.headroom.lock().unwrap().set_usage_cordons(cordons);
        let err = prompt_text(&cx, &sid, "Complete the task")
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("no available candidate"),
            "clear inability: {err}"
        );
        assert!(
            !read_log(&log)
                .iter()
                .any(|entry| entry["event"] == "prompt"),
            "cordoned models must never be prompted"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn cordon_during_active_tool_turn_interrupts_and_continues_on_peer() {
    let state = temp_state_file("active-cordon");
    let log = temp_log("active-cordon-peer");
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\ncordon: {{enabled: false}}\nrouters:\n  auto: {{cost_quality_tradeoff: 0}}\nagents:\n{}{}",
        state.display(),
        agent_yaml("a", &[("opus", 3)], &[]),
        agent_yaml(
            "b",
            &[("sonnet", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let prompt = prompt_text(
            &cx,
            &sid,
            "[router: candidate=a/opus]\nTOOL:edit\nSLEEP:3000",
        );
        let cordon = async {
            for _ in 0..200 {
                if agent_text(&observed, &sid).contains("tool:")
                    || observed
                        .lock()
                        .unwrap()
                        .updates
                        .iter()
                        .any(|n| matches!(&n.update, SessionUpdate::ToolCall(_)))
                {
                    shared.headroom.lock().unwrap().cordon(
                        "a",
                        Some(Duration::from_secs(3600)),
                        "capacity reserved",
                    );
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("tool activity was never observed");
        };
        let (response, ()) = tokio::join!(prompt, cordon);
        assert_eq!(response?.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("failover: auto → b/sonnet"),
            "active cordon must fail over: {text}"
        );
        let prompts: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|entry| entry["event"] == "prompt")
            .collect();
        assert_eq!(prompts.len(), 1, "one continuation reaches the peer");
        let handoff = prompts[0]["text"].as_str().unwrap();
        assert!(
            handoff.contains("Tool: Edit [completed]"),
            "handoff preserves tool status: {handoff}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn cordon_after_terminal_callback_without_text_uses_hot_continuation() {
    let state = temp_state_file("callback-cordon");
    let log = temp_log("callback-cordon-peer");
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\ncordon: {{enabled: false}}\nagents:\n{}{}",
        state.display(),
        agent_yaml("a", &[("opus", 3)], &[]),
        agent_yaml(
            "b",
            &[("sonnet", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        )
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let prompt = prompt_text(
            &cx,
            &sid,
            "[router: candidate=a/opus]\nTERMINAL_GROK:run-once\nSLEEP:3000",
        );
        let cordon = async {
            for _ in 0..200 {
                if !observed.lock().unwrap().terminal_creates.is_empty() {
                    shared.headroom.lock().unwrap().cordon(
                        "a",
                        Some(Duration::from_secs(3600)),
                        "capacity reserved",
                    );
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("terminal callback never arrived");
        };
        let (response, ()) = tokio::join!(prompt, cordon);
        assert_eq!(response?.stop_reason, StopReason::EndTurn);
        let prompts: Vec<_> = read_log(&log)
            .into_iter()
            .filter(|e| e["event"] == "prompt")
            .collect();
        let handoff = prompts[0]["text"].as_str().unwrap();
        assert!(
            handoff.contains("Hot failover:")
                && handoff.contains("Tool: terminal/create [requested; completion unknown]"),
            "callback side effects must reach the handoff: {handoff}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn agent_cordon_between_turns_never_sends_another_prompt_to_that_account() {
    let state = temp_state_file("between-cordon");
    let log = temp_log("between-cordon");
    let yaml = format!(
        "state_file: {}\ndelegation: {{enabled: false}}\ncordon: {{enabled: false}}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("opus", 3)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml("b", &[("sonnet", 2)], &[])
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "[router: candidate=a/opus]\nfirst task").await?;
        shared.headroom.lock().unwrap().cordon(
            "a",
            Some(Duration::from_secs(3600)),
            "capacity reserved",
        );
        prompt_text(&cx, &sid, "second task").await?;
        assert!(agent_text(&observed, &sid).contains("failover: auto → b/sonnet"));
        assert_eq!(
            read_log(&log)
                .iter()
                .filter(|entry| entry["event"] == "prompt")
                .count(),
            1,
            "no prompt or summary should hit the cordoned account"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn usage_cordon_excludes_advertises_and_redirects() {
    let state = temp_state_file("cordon");
    let log = temp_log("cordon");
    // cordon.enabled=false so the real poller never runs; the test injects the
    // cordon it would have computed. Pure-quality routing (tradeoff 0) so
    // a/fable-5 (higher quality) would win but for the cordon.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\ncordon: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("fable-5", 5)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("sonnet", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;

        // Compute the cordon from a real-shaped payload: Fable weekly-scoped at
        // 100% critical, overage exhausted. Far-future reset so it stays active.
        let payload = serde_json::json!({
            "extra_usage": { "is_enabled": true, "utilization": 100.0 },
            "spend": { "enabled": true, "percent": 100 },
            "limits": [{
                "kind": "weekly_scoped", "percent": 100, "severity": "critical",
                "is_active": true, "resets_at": "2099-01-01T00:00:00+00:00",
                "scope": { "model": { "id": serde_json::Value::Null, "display_name": "Fable" } }
            }]
        });
        let cands = vec![
            (
                router_acp::candidate::CandidateId::new("a", "fable-5"),
                "Fable".to_string(),
            ),
            (
                router_acp::candidate::CandidateId::new("b", "sonnet"),
                "Sonnet".to_string(),
            ),
        ];
        let cordons =
            router_acp::usage::anthropic_cordons(&payload, &cands, std::time::SystemTime::now());
        assert!(!cordons.is_empty(), "payload should cordon fable");
        shared.headroom.lock().unwrap().set_usage_cordons(cordons);

        // (a) Advertised: a/fable-5 is marked available:false with a reason.
        let sess = new_session(&cx).await?;
        let opts = serde_json::to_string(&sess.config_options).unwrap();
        assert!(
            opts.contains("\"available\":false")
                && opts.contains("Weekly Fable limit reached")
                && opts.contains("\"capabilities\":{\"effort\""),
            "cordoned candidate advertised unavailable: {opts}"
        );

        // (b) Auto excludes the cordoned candidate → routes to b/sonnet even
        // though pure-quality routing would otherwise pick a/fable-5.
        let sid = sess.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "do the thing").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:sonnet:"),
            "routed to non-cordoned b/sonnet: {text}"
        );

        // (b') The turn's routing metadata carries the full active usage-cordon
        // set (not just a redirect), so a client that cached the candidate list
        // at session/new can refresh availability mid-session.
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|s| s.routing)
            .expect("routing recorded");
        let cordoned: Vec<String> = routing["usage_cordons"]
            .as_array()
            .expect("usage_cordons array in metadata")
            .iter()
            .map(|c| c["candidate"].as_str().unwrap_or_default().to_string())
            .collect();
        assert!(
            cordoned.iter().any(|c| c == "a/fable-5"),
            "per-turn metadata lists the cordoned candidate: {routing}"
        );

        // (c) Explicit pin to the cordoned candidate is refused → falls back to
        // b/sonnet with a cordon disclosure line in the failover format.
        let sid2 = new_session(&cx).await?.session_id.0.to_string();
        let resp2 = prompt_text(&cx, &sid2, "[router: candidate=a/fable-5]\ndo it").await?;
        assert_eq!(resp2.stop_reason, StopReason::EndTurn);
        let text2 = agent_text(&observed, &sid2);
        assert!(
            text2.contains("failover: cordon"),
            "cordon redirect disclosed in failover format: {text2}"
        );
        assert!(
            text2.contains("echo:sonnet:"),
            "redirected to b/sonnet: {text2}"
        );

        // (d) Headroom payload → nothing cordoned → fable available again.
        let ok_payload = serde_json::json!({
            "extra_usage": { "is_enabled": true, "utilization": 40.0 },
            "spend": { "enabled": true, "percent": 40 },
            "limits": []
        });
        let ok =
            router_acp::usage::anthropic_cordons(&ok_payload, &cands, std::time::SystemTime::now());
        assert!(ok.is_empty(), "headroom → no cordons");
        shared.headroom.lock().unwrap().set_usage_cordons(ok);
        let sess3 = new_session(&cx).await?;
        let opts3 = serde_json::to_string(&sess3.config_options).unwrap();
        assert!(
            !opts3.contains("\"available\":false"),
            "no cordon advertised: {opts3}"
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Availability hints → dynamic preference
// ======================================================================

#[tokio::test]
async fn account_overage_permission_gates_native_readings_without_quota_hints() {
    for scaling in [true, false] {
        let state = temp_state_file("account-consent");
        let log = temp_log("account-consent");
        let yaml = format!(
            "state_file: {}\nrouter: static\nrouters:\n  static: {{ candidate: 'claude@personal/sonnet', allow_fallback: true }}\n\
             delegation: {{ enabled: false }}\ncordon: {{ enabled: false }}\n\
             availability_preference: {{ enabled: {scaling} }}\nagents:\n{}{}{}",
            state.display(),
            agent_yaml("claude", &[("sonnet", 2)], &[]),
            agent_yaml(
                "claude@personal",
                &[("sonnet", 2)],
                &[("MOCK_LOG", &log.display().to_string())]
            ),
            agent_yaml("codex", &[("gpt-5.6", 2)], &[]),
        );
        run_test_shared(yaml, async |cx, observed, shared| {
            init(&cx).await?;
            let id = router_acp::candidate::CandidateId::new("claude@personal", "sonnet");
            let home = router_acp::candidate::CandidateId::new("claude", "sonnet");
            let paying = router_acp::headroom::SeatAvailability {
                plan_headroom: 0.0,
                plan_remaining_dollars: None,
                on_overage: true,
                overage_headroom: Some(0.9),
                overage_remaining_dollars: Some(90.0),
                source: "poll",
            };
            shared.headroom.lock().unwrap().set_polled_availability(
                std::collections::HashMap::from([
                    (id.clone(), paying.clone()),
                    (home.clone(), paying.clone()),
                ]),
            );
            cx.send_notification(agent_client_protocol::UntypedMessage::new(
                "router-acp/availability_hint",
                serde_json::json!({"ttl_secs": 1, "agents": [
                    {"agent": "claude", "overage_allowed": false},
                    {"agent": "claude@personal", "overage_allowed": false},
                ]}),
            )?)?;
            for _ in 0..50 {
                if shared.headroom.lock().unwrap().seat_exhausted(&id) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(shared.headroom.lock().unwrap().seat_exhausted(&id));
            // A native refresh must not erase consent or borrow sibling quota.
            shared.headroom.lock().unwrap().set_polled_availability(
                std::collections::HashMap::from([
                    (id.clone(), paying.clone()),
                    (home.clone(), paying),
                ]),
            );
            let sid = new_session(&cx).await?.session_id.0.to_string();
            prompt_text(&cx, &sid, "hello").await?;
            assert!(agent_text(&observed, &sid).contains("echo:gpt-5.6:"));

            for (permission, expires, exhausted) in [
                (serde_json::json!(true), "2099-01-01T00:00:00Z", false),
                (serde_json::json!(true), "2000-01-01T00:00:00Z", true),
                (serde_json::Value::Null, "", false),
            ] {
                cx.send_notification(agent_client_protocol::UntypedMessage::new(
                    "router-acp/availability_hint",
                    serde_json::json!({"agents": [{
                        "agent": "claude@personal", "overage_allowed": permission,
                        "overage_expires_at": expires,
                    }]}),
                )?)?;
                for _ in 0..50 {
                    if shared.headroom.lock().unwrap().seat_exhausted(&id) == exhausted {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                assert_eq!(
                    shared.headroom.lock().unwrap().seat_exhausted(&id),
                    exhausted
                );
                let sid = new_session(&cx).await?.session_id.0.to_string();
                prompt_text(&cx, &sid, "hello").await?;
                let expected = if exhausted {
                    "echo:gpt-5.6:"
                } else {
                    "echo:sonnet:"
                };
                assert!(agent_text(&observed, &sid).contains(expected));
            }
            assert!(shared.headroom.lock().unwrap().seat_exhausted(&home));
            Ok(())
        })
        .await;
    }
}

#[tokio::test]
async fn account_overage_grant_outlives_short_quota_hint_ttl() {
    let state = temp_state_file("account-grant-expiry");
    let log = temp_log("account-grant-expiry");
    let yaml = format!(
        "state_file: {}\nrouter: static\nrouters:\n  static: {{ candidate: 'claude@personal/sonnet', allow_fallback: true }}\n\
         delegation: {{ enabled: false }}\ncordon: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "claude@personal",
            &[("sonnet", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml("codex", &[("gpt-5.6", 2)], &[]),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let id = CandidateId::new("claude@personal", "sonnet");
        let paying = router_acp::headroom::SeatAvailability {
            plan_headroom: 0.0,
            plan_remaining_dollars: None,
            on_overage: true,
            overage_headroom: Some(0.9),
            overage_remaining_dollars: Some(90.0),
            source: "poll",
        };
        shared
            .headroom
            .lock()
            .unwrap()
            .set_polled_availability(std::collections::HashMap::from([(id.clone(), paying)]));

        cx.send_notification(agent_client_protocol::UntypedMessage::new(
            "router-acp/availability_hint",
            serde_json::json!({
                "ttl_secs": 1,
                "agents": [{
                    "agent": "claude@personal",
                    "windows": [{ "percent": 100, "scope": serde_json::Value::Null, "active": true }],
                    "overage": { "enabled": true, "percent": 10 },
                    "overage_allowed": true,
                    "overage_expires_at": "2099-01-01T00:00:00Z"
                }]
            }),
        )?)?;

        for _ in 0..50 {
            if shared
                .headroom
                .lock()
                .unwrap()
                .availability(&id)
                .is_some_and(|a| a.source == "hint")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            shared
                .headroom
                .lock()
                .unwrap()
                .availability(&id)
                .map(|a| a.source),
            Some("hint"),
            "the quota hint should apply before its TTL expires"
        );

        for _ in 0..100 {
            if shared
                .headroom
                .lock()
                .unwrap()
                .availability(&id)
                .is_some_and(|a| a.source == "poll")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let availability = shared.headroom.lock().unwrap().availability(&id);
        assert_eq!(
            availability.as_ref().map(|a| a.source),
            Some("poll"),
            "quota hint must expire on its own TTL"
        );
        assert!(
            !shared.headroom.lock().unwrap().seat_exhausted(&id),
            "the explicit grant must survive the shorter quota hint TTL"
        );

        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "hello").await?;
        assert!(agent_text(&observed, &sid).contains("echo:sonnet:"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn explicit_pin_to_spent_account_fails_over_to_available_sibling() {
    let state = temp_state_file("spent-account-pin");
    let yaml = format!(
        "state_file: {}\nrouter: auto\nrouters:\n  static: {{ candidate: 'claude@home/sonnet', allow_fallback: false }}\n\
         delegation: {{ enabled: false }}\ncordon: {{ enabled: false }}\nagents:\n{}{}",
        state.display(),
        agent_yaml("claude@home", &[("sonnet", 2)], &[]),
        agent_yaml("claude@backup", &[("sonnet", 2)], &[]),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let home = CandidateId::new("claude@home", "sonnet");
        shared
            .headroom
            .lock()
            .unwrap()
            .set_polled_availability(std::collections::HashMap::from([(
                home.clone(),
                router_acp::headroom::SeatAvailability {
                    plan_headroom: 0.0,
                    plan_remaining_dollars: None,
                    on_overage: true,
                    overage_headroom: Some(0.9),
                    overage_remaining_dollars: Some(90.0),
                    source: "poll",
                },
            )]));
        cx.send_notification(agent_client_protocol::UntypedMessage::new(
            "router-acp/availability_hint",
            serde_json::json!({"agents": [{"agent": "claude@home", "overage_allowed": false}]}),
        )?)?;
        for _ in 0..50 {
            if shared.headroom.lock().unwrap().seat_exhausted(&home) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(shared.headroom.lock().unwrap().seat_exhausted(&home));
        assert!(
            shared
                .headroom
                .lock()
                .unwrap()
                .usage_cordon(&home)
                .is_none()
        );

        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid,
            "[router: candidate=claude@home/sonnet] continue the work",
        )
        .await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("claude@backup/sonnet"),
            "fallback must be disclosed: {text}"
        );
        assert!(
            text.contains("account plan exhausted with no usable overage"),
            "the exhausted pin must be explained: {text}"
        );
        assert!(
            text.contains("echo:sonnet:"),
            "the sibling must answer: {text}"
        );
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|s| s.routing)
            .expect("routing recorded");
        assert_eq!(routing["candidate"], "claude@backup/sonnet");
        assert_eq!(routing["cordon_redirect"]["from"], "claude@home/sonnet");
        assert!(routing["cordon_redirect"]["resets_at"].is_null());
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn availability_hint_penalizes_overage_seat() {
    let state = temp_state_file("avail-hint");
    let log = temp_log("avail-hint");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"a/*\", default_quality: 1.2 }\n\
         \x20 - { pattern: \"b/*\", default_quality: 1.1 }\n",
    )
    .unwrap();
    // Pure-quality routing: a narrowly beats b — until a client hint reports
    // agent `a` past its cap and burning paid overage. The default 0.25
    // penalty hands the win to the comparable free seat.
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         cordon: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml(
            "a",
            &[("fable-5", 5)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("sonnet", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;

        // Baseline: quality routing picks a/fable-5.
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "do the thing").await?;
        assert!(
            agent_text(&observed, &sid).contains("echo:fable-5:"),
            "baseline routes to the higher-quality candidate"
        );

        // Client hint: agent `a` weekly cap saturated, overage absorbing.
        let hint = agent_client_protocol::UntypedMessage::new(
            "router-acp/availability_hint",
            serde_json::json!({
                "ttl_secs": 300,
                "agents": [{
                    "agent": "a",
                    "windows": [{ "percent": 100, "scope": serde_json::Value::Null, "active": true }],
                    "overage": { "enabled": true, "percent": 40 }
                }]
            }),
        )?;
        cx.send_notification(hint)?;

        // The notification is processed asynchronously; wait for it to land.
        let fable = router_acp::candidate::CandidateId::new("a", "fable-5");
        for _ in 0..50 {
            if shared
                .headroom
                .lock()
                .unwrap()
                .availability(&fable)
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let avail = shared
            .headroom
            .lock()
            .unwrap()
            .availability(&fable)
            .expect("hint applied");
        assert!(avail.on_overage && avail.source == "hint", "{avail:?}");

        // New sessions now route to the free seat despite the quality gap.
        let sid2 = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid2, "do the thing").await?;
        assert!(
            agent_text(&observed, &sid2).contains("echo:sonnet:"),
            "overage-penalized seat loses to the free seat: {}",
            agent_text(&observed, &sid2)
        );

        // The pin metadata discloses the availability inputs.
        let routing = open_state(&state)
            .get(&sid2)
            .and_then(|s| s.routing)
            .expect("routing recorded");
        let availability = routing["availability"]
            .as_array()
            .expect("availability array in metadata");
        assert!(
            availability.iter().any(|a| a["candidate"] == "a/fable-5"
                && a["on_overage"] == true
                && a["source"] == "hint"),
            "availability disclosed: {routing}"
        );
        Ok(())
    })
    .await;
}

// Live routing regression (session `rtr-067e9f43`, 2026-08-06), dollar-
// normalized: two seats both past their included plan and paying overage,
// at equal quality, must rank on real remaining DOLLARS, not on the
// fraction of each seat's own (differently-sized) cap. A percent-only
// comparison — the first version of this fix — ranks `a` ahead here (its
// fraction is higher); the fix must still route to `b` because $900 is more
// real budget than $300.
#[tokio::test]
async fn overage_seat_with_more_dollars_left_beats_higher_fraction_smaller_cap() {
    let state = temp_state_file("avail-overage-grade");
    let log = temp_log("avail-overage-grade");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"*\", default_quality: 2.0 }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         cordon: {{ enabled: false }}\n\
         availability_preference: {{ headroom_scale_dollars: 2000 }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 10 }}\nagents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml(
            "a",
            &[("worker", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("worker", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test_shared(yaml, async |cx, _observed, shared| {
        init(&cx).await?;

        // Both seats saturated and paying overage. `a` reports a HIGHER free
        // overage FRACTION (10%) but its cap is small ($3,000 → $300 left);
        // `b` reports a lower fraction (3%) on a much bigger cap ($30,000 →
        // $900 left). A percent-only comparison would pick `a`;
        // dollar-normalized ranking must pick `b`. The config's
        // `headroom_scale_dollars: 2000` keeps both figures below the
        // saturation ceiling so the comparison stays dollar-driven, not a
        // tie at 1.0.
        let hint = agent_client_protocol::UntypedMessage::new(
            "router-acp/availability_hint",
            serde_json::json!({
                "ttl_secs": 300,
                "agents": [
                    {
                        "agent": "a",
                        "windows": [{ "percent": 100, "scope": serde_json::Value::Null, "active": true }],
                        "overage": { "enabled": true, "percent": 90, "remaining_dollars": 300.0 }
                    },
                    {
                        "agent": "b",
                        "windows": [{ "percent": 100, "scope": serde_json::Value::Null, "active": true }],
                        "overage": { "enabled": true, "percent": 97, "remaining_dollars": 900.0 }
                    }
                ]
            }),
        )?;
        cx.send_notification(hint)?;

        let a = router_acp::candidate::CandidateId::new("a", "worker");
        for _ in 0..50 {
            if shared.headroom.lock().unwrap().availability(&a).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Both flagged on_overage with identical (zeroed) plan_headroom, and
        // `a` has the HIGHER overage fraction — a percent-only comparison
        // would rank `a` ahead. The dollar figures invert that.
        let (avail_a, avail_b) = {
            let headroom = shared.headroom.lock().unwrap();
            (
                headroom.availability(&a).expect("a hinted"),
                headroom
                    .availability(&router_acp::candidate::CandidateId::new("b", "worker"))
                    .expect("b hinted"),
            )
        };
        assert!(avail_a.on_overage && avail_b.on_overage);
        assert!((avail_a.plan_headroom - avail_b.plan_headroom).abs() < 1e-9);
        assert!(
            avail_a.overage_headroom.unwrap() > avail_b.overage_headroom.unwrap(),
            "a's fraction must be HIGHER than b's, so the eventual win for b proves this \
             isn't a fraction-driven outcome: a={avail_a:?} b={avail_b:?}"
        );
        const SCALE: f64 = 2000.0;
        assert!(
            avail_a.seat_budget(SCALE) < avail_b.seat_budget(SCALE),
            "a's smaller dollar pool must read as less budget despite its higher fraction: \
             a={avail_a:?} b={avail_b:?}"
        );

        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "do the thing").await?;
        assert_eq!(
            open_state(&state)
                .get(&sid)
                .map(|session| session.agent.clone()),
            Some("b".to_string()),
            "the seat with more real overage dollars left must win, even with a lower fraction"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn partial_plan_headroom_changes_effective_cost() {
    let state = temp_state_file("avail-headroom");
    let log = temp_log("avail-headroom");
    let scores = std::env::temp_dir().join(format!(
        "router-acp-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &scores,
        "version: 1\ncandidates:\n\
         \x20 - { pattern: \"*\", default_quality: 2.0 }\n",
    )
    .unwrap();
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         cordon: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 10 }}\nagents:\n{}{}",
        state.display(),
        scores.display(),
        agent_yaml(
            "a",
            &[("worker", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("worker", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;

        // Equal quality, rank, preference, and local usage: config order wins.
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "small task").await?;
        assert!(agent_text(&observed, &sid).contains("echo:worker:"));
        assert_eq!(
            open_state(&state)
                .get(&sid)
                .map(|session| session.agent.clone()),
            Some("a".to_string())
        );

        // Both seats remain inside their plans, but b has four times the
        // weekly headroom. That makes b effectively cheaper before overage.
        let hint = agent_client_protocol::UntypedMessage::new(
            "router-acp/availability_hint",
            serde_json::json!({
                "ttl_secs": 300,
                "agents": [
                    {
                        "agent": "a",
                        "windows": [{ "percent": 80, "scope": serde_json::Value::Null }]
                    },
                    {
                        "agent": "b",
                        "windows": [{ "percent": 20, "scope": serde_json::Value::Null }]
                    }
                ]
            }),
        )?;
        cx.send_notification(hint)?;

        let a = router_acp::candidate::CandidateId::new("a", "worker");
        for _ in 0..50 {
            if shared.headroom.lock().unwrap().availability(&a).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let sid2 = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid2, "small task").await?;
        assert_eq!(
            open_state(&state)
                .get(&sid2)
                .map(|session| session.agent.clone()),
            Some("b".to_string()),
            "the candidate with more included-plan headroom should be cheaper"
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Ticket-context loading
// ======================================================================

#[tokio::test]
async fn ticket_reference_enriches_prompt() {
    let state = temp_state_file("ticket-enrich");
    let state_path = state.clone();
    let log = temp_log("ticket-enrich");
    // The fetch command emits the ticket body, which is injected into the
    // bare prompt "Fix HAI-1234" before it reaches the downstream model.
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         ticket_context:\n\
         \x20 - prefix: \"HAI-\"\n\
         \x20   command: [\"/bin/sh\", \"-c\", \"printf '# %s: upgrade pipeline\\n1. add extractor\\n2. wire routes\\n3. add tests\\n' $TICKET\"]\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml(
            "b",
            &[("m2", 2)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, _shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Bare mention — no list in the user's own text.
        let resp = prompt_text(&cx, &sid, "Fix HAI-1234").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("loaded ticket HAI-1234"),
            "ticket load disclosed: {text}"
        );
        assert!(
            text.contains("HAI-1234: upgrade pipeline"),
            "ticket content reached the downstream model: {text}"
        );
        assert_eq!(
            open_state(&state_path)
                .get(&sid)
                .expect("session persisted")
                .title
                .as_deref(),
            Some("Fix HAI-1234"),
            "the placeholder title must use the user prompt, not injected ticket context"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn ticket_fetch_failure_fails_open() {
    let state = temp_state_file("ticket-fail");
    let log = temp_log("ticket-fail");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         ticket_context:\n\
         \x20 - prefix: \"HAI-\"\n\
         \x20   command: [\"/bin/sh\", \"-c\", \"echo $TICKET >&2; exit 3\"]\n\
         agents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "Fix HAI-77 please").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn, "turn still succeeds");
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("could not load ticket HAI-77"),
            "failure disclosed: {text}"
        );
        assert!(
            text.contains("echo:m1:Fix HAI-77 please"),
            "original prompt passed through unchanged: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn ticket_enrichment_chars_rides_the_pin_and_resets_next_turn() {
    // A host measuring fixed per-turn context (e.g. a startup-cost check)
    // needs to tell "the ticket was big" apart from an actual settings
    // regression — that's what this field exists for.
    let state = temp_state_file("ticket-enrich-chars");
    let log = temp_log("ticket-enrich-chars");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         ticket_context:\n\
         \x20 - prefix: \"ABC-\"\n\
         \x20   command: [\"/bin/sh\", \"-c\", \"printf 'body of $TICKET'\"]\n\
         agents:\n{}",
        state.display(),
        agent_yaml(
            "mock",
            &[("m1", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();

        prompt_text(&cx, &sid, "Fix ABC-500").await?;
        let expected_chars = router_acp::tickets::frame_ticket("ABC-500", "body of ABC-500").len();
        let meta = routing_meta_for(&observed, &sid).expect("first chunk carries routing meta");
        assert_eq!(
            meta.get("ticket_enrichment_chars"),
            Some(&serde_json::json!(expected_chars)),
            "meta: {meta:?}"
        );

        // Same session, a turn that references no ticket at all. The full
        // disclosure only rides a (re)pin, so assert the reset on session
        // state directly: were a switch/failover to rebuild `details` on this
        // turn, this is the value it would read.
        prompt_text(&cx, &sid, "now just chatting").await?;
        let pending = shared
            .with_session(&sid, |s| s.pending_ticket_enrichment_chars)
            .expect("session exists");
        assert_eq!(
            pending, None,
            "must not carry the prior turn's enrichment forward"
        );
        Ok(())
    })
    .await;
}

// ---------------------------------------------------------------------------
// Pre-classifier (HAI-7056)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn preclass_mode_less_evaluator_runs_without_set_mode() {
    let state = temp_state_file("preclass-mode-less");
    let log = temp_log("preclass-mode-less");
    let preclass_json = r#"{"routing":{"task_class":"BugFix","complexity":0.4,"confidence":0.9,"reason":"fallback"}}"#;
    let agent = agent_yaml(
        "grok",
        &[("grok-4.5", 1)],
        &[
            ("MOCK_LOG", &log.display().to_string()),
            ("MOCK_PRECLASS_JSON", preclass_json),
        ],
    )
    .replace(
        "        - { name: MOCK_SESSION_MODES, value: preclass }\n",
        "",
    )
    .replace("    mode_map: { preclass: preclass }\n", "");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\npre_classifier:\n  enabled: true\n  evaluator: [\"*grok*\"]\nagents:\n{}",
        state.display(),
        agent
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "classify this").await?;
        let events = read_log(&log);
        assert!(
            events.iter().any(|event| event["event"] == "prompt"),
            "mode-less evaluator must receive the classifier prompt: {events:?}"
        );
        assert!(
            !events.iter().any(|event| event["event"] == "set_mode"),
            "mode-less evaluator must not receive set_mode: {events:?}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_mode_less_tool_violation_falls_back_to_static_classifier() {
    // A mode-less evaluator (Grok) that fires a tool_call during pre-class is
    // a structural miss, not a service failure: cancel the tool, do not cordon,
    // and fall back to the static keyword classifier so session start is not
    // hard-blocked. The same seat remains eligible to take the real turn.
    let state = temp_state_file("preclass-modeless-tool");
    let log = temp_log("preclass-modeless-tool");
    let agent = agent_yaml(
        "grok",
        &[("grok-4.5", 1)],
        &[
            ("MOCK_LOG", &log.display().to_string()),
            ("MOCK_PRECLASS_TOOL", "1"),
        ],
    )
    .replace(
        "        - { name: MOCK_SESSION_MODES, value: preclass }\n",
        "",
    )
    .replace("    mode_map: { preclass: preclass }\n", "");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nauto_upgrade: {{ enabled: false }}\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*grok*\"]\n  disclose: true\n\
         agents:\n{}",
        state.display(),
        agent
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let response = prompt_text(
            &cx,
            &sid,
            "Fix the login bug on the settings page and add a regression test",
        )
        .await?;
        assert_eq!(response.stop_reason, StopReason::EndTurn);
        assert!(
            read_log(&log)
                .iter()
                .any(|event| event["event"] == "cancel"),
            "tool attempt must cancel the evaluator"
        );
        let entry = open_state(&state).get(&sid).expect("session row");
        assert_eq!(
            entry.agent, "grok",
            "session must pin to the mode-less seat"
        );
        let routing = entry.routing.expect("static-fallback routing persisted");
        assert_eq!(routing["candidate"], "grok/grok-4.5", "{routing}");
        assert!(routing.get("class").is_some(), "{routing}");
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("static fallback"),
            "disclosure must mention static fallback: {text}"
        );
        assert!(
            !text.contains("could not classify"),
            "mode-less tool violation must not hard-fail session start: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_stalled_evaluator_fails_over_instead_of_hanging() {
    // Regression: an evaluator that opens fine, then never answers and streams
    // nothing, used to hang the prompt forever — the turn never started and the
    // session showed no activity at all. It must now be bounded by
    // `stall_timeout_ms`, cordoned, and failed over to the next evaluator.
    let state = temp_state_file("preclass-stall");
    let hang_log = temp_log("preclass-stall-hang");
    let ok_log = temp_log("preclass-stall-ok");
    let preclass_json = r#"{"routing":{"task_class":"Ops","complexity":0.31,"confidence":0.9,"reason":"after stall failover"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*m1*\"]\n  stall_timeout_ms: 1500\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "hang",
            &[("m1", 1)],
            &[
                ("MOCK_LOG", &hang_log.display().to_string()),
                ("MOCK_PRECLASS_HANG", "1"),
            ]
        ),
        agent_yaml(
            "ok",
            &[("m2", 2)],
            &[
                ("MOCK_LOG", &ok_log.display().to_string()),
                ("MOCK_PRECLASS_JSON", preclass_json),
            ]
        ),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "fix the failing build").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        // The stalled evaluator got its safe mode set (so Phase 1 succeeded and
        // the wedge really was in the generation phase) and was prompted.
        let hang_events = read_log(&hang_log);
        assert!(
            hang_events
                .iter()
                .any(|event| event["event"] == "set_mode" && event["modeId"] == "preclass"),
            "stalled evaluator must have opened successfully: {hang_events:?}"
        );
        assert!(
            hang_events.iter().any(|event| event["event"] == "prompt"),
            "stalled evaluator must have been prompted: {hang_events:?}"
        );
        // Failover landed on the healthy evaluator, whose classification stuck.
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|session| session.routing)
            .expect("routing persisted after stall failover");
        assert_eq!(routing["class"], "Ops", "{routing}");
        assert_eq!(routing["complexity"], 0.31, "{routing}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_dead_evaluator_peer_fails_over_promptly() {
    // Characterization, not a regression: unlike the stall case above, this
    // path already resolved correctly before `guard_turn` existed —
    // `send_request(..).block_task()`'s wait runs on the *upstream*
    // connection, so a downstream peer dying surfaces as an error result
    // directly, without the dropped-consuming-task hang `relay_request`
    // documents for `forward_response_to`. `stall_timeout_ms` is set far
    // longer than the test can afford to wait, so passing proves the turn
    // ended via peer death, not via the (much later) stall guard tripping.
    let state = temp_state_file("preclass-dead");
    let ok_log = temp_log("preclass-dead-ok");
    let preclass_json = r#"{"routing":{"task_class":"BugFix","complexity":0.44,"confidence":0.9,"reason":"after peer death"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*m1*\"]\n  stall_timeout_ms: 600000\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml("dead", &[("m1", 1)], &[("MOCK_EXIT_ON_PROMPT", "1")]),
        agent_yaml(
            "ok",
            &[("m2", 2)],
            &[
                ("MOCK_LOG", &ok_log.display().to_string()),
                ("MOCK_PRECLASS_JSON", preclass_json),
            ]
        ),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "fix the failing build").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|session| session.routing)
            .expect("routing persisted after peer-death failover");
        assert_eq!(routing["class"], "BugFix", "{routing}");
        assert_eq!(routing["complexity"], 0.44, "{routing}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_preferred_evaluator_beats_grok_fallback() {
    let state = temp_state_file("preclass-preferred");
    let codex_log = temp_log("preclass-preferred-codex");
    let grok_log = temp_log("preclass-preferred-grok");
    let preclass_json = r#"{"routing":{"task_class":"BugFix","complexity":0.4,"confidence":0.9,"reason":"preferred"}}"#;
    let codex = agent_yaml(
        "codex",
        &[("gpt-5.4-mini", 1)],
        &[
            ("MOCK_LOG", &codex_log.display().to_string()),
            ("MOCK_PRECLASS_JSON", preclass_json),
        ],
    );
    let grok = agent_yaml(
        "grok",
        &[("grok-4.5", 2)],
        &[
            ("MOCK_LOG", &grok_log.display().to_string()),
            ("MOCK_PRECLASS_JSON", preclass_json),
        ],
    )
    .replace(
        "        - { name: MOCK_SESSION_MODES, value: preclass }\n",
        "",
    )
    .replace("    mode_map: { preclass: preclass }\n", "");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\npre_classifier:\n  enabled: true\n  evaluator: [\"*mini*\"]\nagents:\n{}{}",
        state.display(),
        codex,
        grok
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "classify this").await?;
        let codex_events = read_log(&codex_log);
        assert!(
            codex_events.iter().any(|event| event["event"] == "prompt"),
            "preferred Codex evaluator must run"
        );
        assert!(
            codex_events
                .iter()
                .any(|event| event["event"] == "set_mode"),
            "preferred Codex evaluator must enter its explicit preclass mode"
        );
        assert!(
            !read_log(&grok_log)
                .iter()
                .any(|event| event["event"] == "prompt"),
            "Grok must remain a fallback when preferred Codex succeeds"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_requires_an_explicit_advertised_safe_mode_before_prompting() {
    let state = temp_state_file("preclass-no-safe-mode");
    let log = temp_log("preclass-no-safe-mode");
    let agent = agent_yaml(
        "a",
        &[("m1", 1)],
        &[
            ("MOCK_LOG", &log.display().to_string()),
            ("MOCK_SESSION_MODES", "preclass"),
        ],
    )
    .replace("    mode_map: { preclass: preclass }\n", "");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\npre_classifier:\n  enabled: true\n  evaluator: [\"*m1*\"]\nagents:\n{}",
        state.display(),
        agent
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "classify this").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert!(
            !read_log(&log).iter().any(|event| {
                event["event"] == "prompt"
                    && event["text"]
                        .as_str()
                        .is_some_and(|text| text.contains("[router-acp pre-classifier]"))
            }),
            "no classifier prompt may be sent without mode_map.preclass"
        );
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|session| session.routing)
            .expect("static-fallback routing persisted");
        assert!(routing.get("class").is_some(), "{routing}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_tool_attempt_is_cancelled_and_fails_over() {
    let state = temp_state_file("preclass-tool-violation");
    let bad_log = temp_log("preclass-tool-violation-bad");
    let good = r#"{"routing":{"task_class":"BugFix","complexity":0.4,"confidence":0.9,"reason":"fallback"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nauto_upgrade: {{ enabled: false }}\npre_classifier:\n  enabled: true\n  evaluator: [\"*bad*\", \"*good*\"]\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("bad", 1)],
            &[
                ("MOCK_LOG", &bad_log.display().to_string()),
                ("MOCK_PRECLASS_TOOL", "1"),
                ("MOCK_PRECLASS_CALLBACK", "1"),
                ("MOCK_PRECLASS_JSON", good),
            ],
        ),
        agent_yaml("b", &[("good", 2)], &[("MOCK_PRECLASS_JSON", good)]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let response = prompt_text(&cx, &sid, "fix it").await?;
        assert_eq!(response.stop_reason, StopReason::EndTurn);
        assert!(
            read_log(&bad_log)
                .iter()
                .any(|event| event["event"] == "cancel"),
            "tool attempt must cancel the evaluator"
        );
        assert!(
            observed.lock().unwrap().permission_session_ids.is_empty(),
            "pre-class callback must not be forwarded to the parent client"
        );
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|session| session.routing)
            .expect("fallback routing persisted");
        assert_eq!(routing["class"], "BugFix");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_no_routing_falls_back_to_static_classifier() {
    // Valid JSON with no `routing` block: the LLM walk yields no classification.
    // Session start proceeds on the static keyword classifier rather than
    // hard-failing.
    let state = temp_state_file("preclass-no-routing");
    let preclass_json = r#"{"notes":{"reason":"no routing block"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*m1*\"]\n  disclose: true\n\
         agents:\n{}",
        state.display(),
        agent_yaml("a", &[("m1", 1)], &[("MOCK_PRECLASS_JSON", preclass_json)]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(
            &cx,
            &sid,
            "Fix the login bug on the settings page and add a regression test",
        )
        .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|session| session.routing)
            .expect("static-fallback routing persisted");
        assert!(routing.get("class").is_some(), "{routing}");
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("static fallback"),
            "disclosure must mention static fallback: {text}"
        );
        assert!(
            !text.contains("could not classify"),
            "missing routing must not hard-fail: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_fails_over_to_next_evaluator() {
    // A down / erroring evaluator must not sink pre-classification: the router
    // fails over to the next eligible evaluator (its known failure handling),
    // and the turn proceeds on that evaluator's classification. If failover were
    // broken this would instead hard-fail.
    let state = temp_state_file("preclass-failover");
    let good = r#"{"routing":{"task_class":"BugFix","task_classes":["BugFix"],"categories":["backend"],"complexity":0.4,"confidence":0.9,"reason":"targeted fix"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*opus*\", \"*haiku*\"]\n  disclose: true\n\
         agents:\n{}{}",
        state.display(),
        // opus ranks first for the evaluator pool but errors its prompt turn.
        agent_yaml(
            "a",
            &[("opus", 3)],
            &[(
                "MOCK_FAIL_PROMPT_MSG",
                "rate limit exceeded; retry after 30s"
            )]
        ),
        // haiku is the fallback evaluator and returns a real classification.
        agent_yaml("b", &[("haiku", 1)], &[("MOCK_PRECLASS_JSON", good)]),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // Must NOT hard-fail: failover produced a classification.
        let resp = prompt_text(&cx, &sid, "fix the crash").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|session| session.routing)
            .expect("routing persisted from the fallback evaluator");
        assert_eq!(routing["class"], "BugFix", "{routing}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_widens_to_any_model_when_preferred_unavailable() {
    // Preferred evaluator globs (haiku/mini/flash) match nothing, but another
    // model is available. That model MUST be used as the evaluator — "no
    // evaluator candidate" is only legal when zero models of any kind exist.
    // Regression: without the any-model widen, this session would skip the
    // classifier (or hard-fail) while still being able to pin Grok for the turn.
    let state = temp_state_file("preclass-any-fallback");
    let good = r#"{"routing":{"task_class":"BugFix","task_classes":["BugFix"],"categories":["backend"],"complexity":0.35,"confidence":0.9,"reason":"targeted fix"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*haiku*\", \"*mini*\", \"*flash*\"]\n  disclose: true\n\
         agents:\n{}",
        state.display(),
        // Only Grok is configured — does not match preferred globs.
        agent_yaml("a", &[("grok", 2)], &[("MOCK_PRECLASS_JSON", good)]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "fix the crash").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("pre-class"),
            "pre-class must run on any-model fallback: {text}"
        );
        assert!(
            !text.contains("no evaluator candidate"),
            "must not report no evaluator when a model exists: {text}"
        );
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|session| session.routing)
            .expect("routing persisted from any-model evaluator fallback");
        assert_eq!(routing["class"], "BugFix", "{routing}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_widens_after_preferred_pool_exhausted() {
    // Preferred evaluator is present but fails; a non-preferred model must still
    // be tried (widen to any), not "no evaluator candidate".
    let state = temp_state_file("preclass-widen-after-fail");
    let good = r#"{"routing":{"task_class":"Feature","task_classes":["Feature"],"categories":["backend"],"complexity":0.5,"confidence":0.9,"reason":"feature work"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*haiku*\"]\n  disclose: true\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("haiku", 1)],
            &[(
                "MOCK_FAIL_PROMPT_MSG",
                "rate limit exceeded; retry after 30s"
            )]
        ),
        agent_yaml("b", &[("grok", 2)], &[("MOCK_PRECLASS_JSON", good)]),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "build the widget").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|session| session.routing)
            .expect("routing persisted after preferred exhausted + any-model widen");
        assert_eq!(routing["class"], "Feature", "{routing}");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_dimension_injects_ui_planning() {
    let state = temp_state_file("preclass-ui");
    let log = temp_log("preclass-ui");
    let preclass_json = r#"{"routing":{"task_class":"UiTweak","task_classes":["UiTweak","Feature"],"categories":["UX","frontend"],"complexity":0.55,"confidence":0.92,"reason":"new UI surface"},"ui_planning":{"mode":"planning","confidence":0.88,"reason":"redesign request"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*m1*\"]\n  timeout_ms: 5000\n\
         \x20 disclose: true\n\
         \x20 dimensions:\n\
         \x20   - id: ui_planning\n\
         \x20     description: UI mockup planning\n\
         \x20     min_confidence: 0.70\n\
         \x20     act_when: {{ field: mode, equals: planning }}\n\
         \x20     inject_prompt: |\n\
         \x20       [kory-code] PLANNING INJECT\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[
                ("MOCK_LOG", &log.display().to_string()),
                ("MOCK_PRECLASS_JSON", preclass_json),
            ]
        ),
        agent_yaml("b", &[("m2", 2)], &[]),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "Redesign the settings page with better density").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(text.contains("pre-class"), "pre-class disclosure: {text}");
        assert!(
            text.contains("[kory-code] PLANNING INJECT"),
            "ui_planning inject must ride the prompt: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn preclass_unparseable_reply_falls_back_to_static_classifier() {
    // An unparseable evaluator reply yields no classification from the LLM
    // walk. With no other evaluator to fail over to, routing falls back to the
    // static keyword classifier instead of hard-failing the turn.
    let state = temp_state_file("preclass-badjson");
    let malformed_reply = "Shipping PR regression sentinel\n\n## Rollout\n\n- Verify the canary\n- Watch the deploy logs";
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*m1*\"]\n  disclose: true\n\
         agents:\n{}",
        state.display(),
        agent_yaml(
            "a",
            &[("m1", 1)],
            &[("MOCK_PRECLASS_JSON", malformed_reply)],
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(&cx, &sid, "add a flag and wire it up").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let routing = open_state(&state)
            .get(&sid)
            .and_then(|session| session.routing)
            .expect("static-fallback routing persisted");
        assert!(routing.get("class").is_some(), "{routing}");
        let parent_transcript = agent_text(&observed, &sid);
        assert!(
            !parent_transcript.contains("could not classify"),
            "unparseable classifier reply must not hard-fail: {parent_transcript}"
        );
        assert!(
            parent_transcript.contains("static fallback"),
            "disclosure must mention static fallback: {parent_transcript}"
        );
        assert!(
            !parent_transcript.contains("raw reply:"),
            "raw evaluator reply marker must stay out of parent transcript: {parent_transcript}"
        );
        assert!(
            !parent_transcript.contains("Shipping PR regression sentinel"),
            "raw evaluator body must stay out of parent transcript: {parent_transcript}"
        );
        assert!(
            parent_transcript.contains("parse failed: JSON parse failed"),
            "compact parse-failure metadata must remain in parent transcript: {parent_transcript}"
        );
        assert!(
            !parent_transcript.contains("parse failure: evaluator reply was not valid JSON"),
            "bare parse-failure note must stay out of parent transcript: {parent_transcript}"
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Explicit-only candidates and `pinned_versions`
// ======================================================================

/// Like `agent_yaml`, but the model entries are written out verbatim (so they
/// can carry `auto_eligible` / `versions`) and the mock's model selector
/// advertises exactly `advertised` — which is how a real adapter behaves: it
/// offers a fixed alias set and refuses any other value.
fn agent_yaml_models(
    name: &str,
    advertised: &[&str],
    env: &[(&str, &str)],
    model_lines: &str,
) -> String {
    let mut out = format!(
        "  - name: {name}\n    command:\n      type: stdio\n      command: {}\n      env:\n",
        mock_agent_exe()
    );
    out.push_str(&format!(
        "        - {{ name: MOCK_NAME, value: {name} }}\n\
         \x20       - {{ name: MOCK_MODELS, value: \"{}\" }}\n",
        advertised.join(",")
    ));
    for (key, value) in env {
        let escaped = value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");
        out.push_str(&format!(
            "        - {{ name: {key}, value: \"{escaped}\" }}\n"
        ));
    }
    out.push_str("    model_selection: { type: config-option }\n    models:\n");
    out.push_str(model_lines);
    out
}

/// A score table making `legacy` the strongest candidate, so a pure-quality
/// `auto` would pick it if it were in the pool at all.
fn legacy_scores(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "router-acp-{tag}-scores-{}.yaml",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(
        &path,
        "candidates:\n\
         \x20 - { pattern: 'a/legacy', default_quality: 3.0 }\n\
         \x20 - { pattern: 'a/*', default_quality: 1.0 }\n",
    )
    .unwrap();
    path
}

#[tokio::test]
async fn auto_ineligible_candidate_is_advertised_and_explicitly_pinnable_but_never_auto_chosen() {
    let state = temp_state_file("auto-ineligible");
    let scores = legacy_scores("auto-ineligible");
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}",
        state.display(),
        scores.display(),
        agent_yaml_models(
            "a",
            &["m1", "legacy"],
            &[],
            "      - { id: m1, cost_rank: 1 }\n\
             \x20     - { id: legacy, cost_rank: 1, auto_eligible: false }\n",
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;

        // Advertised in the picker (explicit selection is its only route in),
        // flagged so a client can present it as manual-only.
        let session = new_session(&cx).await?;
        let options = serde_json::to_value(&session.config_options)
            .unwrap()
            .to_string();
        let legacy_at = options
            .find("a/legacy")
            .unwrap_or_else(|| panic!("a/legacy must be advertised: {options}"));
        assert!(
            options[legacy_at..options.len().min(legacy_at + 400)]
                .contains("\"auto_eligible\":false"),
            "a/legacy must carry auto_eligible:false — {options}"
        );
        let m1_at = options.find("a/m1").expect("a/m1 advertised");
        assert!(
            !options[m1_at..legacy_at.max(m1_at)].contains("auto_eligible"),
            "an auto-eligible candidate omits the key entirely: {options}"
        );

        // Pure-quality auto would pick `legacy` (quality 3.0 vs 1.0); it must
        // not even be a candidate.
        let sid = session.session_id.0.to_string();
        prompt_text(&cx, &sid, "implement the feature").await?;
        let routed = agent_text(&observed, &sid);
        assert!(
            routed.contains("auto → a/m1"),
            "auto must never choose an auto_eligible:false candidate — {routed}"
        );

        // Explicitly naming it still works.
        let sid2 = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(
            &cx,
            &sid2,
            "[router: candidate=a/legacy]\nimplement the feature",
        )
        .await?;
        let routed2 = agent_text(&observed, &sid2);
        assert!(
            routed2.contains("static → a/legacy"),
            "an explicit directive must reach the legacy candidate — {routed2}"
        );
        assert_eq!(open_state(&state).get(&sid2).unwrap().model, "legacy");
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn pinned_version_keeps_the_candidate_and_discloses_the_version() {
    let state = temp_state_file("pinned-versions");
    let scores = legacy_scores("pinned-versions");
    let yaml = format!(
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         pinned_versions:\n  \"a/m1\": m1-old\nagents:\n{}",
        state.display(),
        scores.display(),
        agent_yaml_models(
            "a",
            &["m1"],
            &[],
            "      - { id: m1, cost_rank: 1, versions: [ { api_model: m1-old } ] }\n",
        ),
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let options = serde_json::to_value(&session.config_options)
            .unwrap()
            .to_string();
        assert!(
            options.contains(r#""api_model":"m1""#) && options.contains(r#""versions":["m1-old"]"#),
            "the picker is told which versions it can select — {options}"
        );
        let sid = session.session_id.0.to_string();
        prompt_text(&cx, &sid, "implement the feature").await?;
        let routed = agent_text(&observed, &sid);
        assert!(
            routed.contains("auto → a/m1"),
            "routing never sees the pin — {routed}"
        );
        assert!(
            routed.contains("pinned version m1-old"),
            "the disclosure names the version that serves — {routed}"
        );
        assert_eq!(open_state(&state).get(&sid).unwrap().model, "m1");

        // A live pin changes version on the next message, without a switch.
        prompt_text(&cx, &sid, "[router: version=default]\nkeep going").await?;
        let routed = agent_text(&observed, &sid);
        assert!(
            routed.contains("version: a/m1 runs m1"),
            "the version change is disclosed — {routed}"
        );
        prompt_text(&cx, &sid, "[router: version=m9]\nkeep going").await?;
        let routed = agent_text(&observed, &sid);
        assert!(
            routed.contains("m9 is not a version of a/m1; it runs m1-old"),
            "an undeclared version is named, not hidden — {routed}"
        );
        assert_eq!(open_state(&state).get(&sid).unwrap().model, "m1");
        Ok(())
    })
    .await;
}

// ======================================================================
// Planner strategy: plan-first protocol and phase-directive handoff
// ======================================================================

fn planner_user_prompts(log: &PathBuf) -> Vec<String> {
    read_log(log)
        .into_iter()
        .filter(|event| event["event"] == "prompt")
        .filter_map(|event| event["text"].as_str().map(str::to_string))
        .filter(|text| !text.contains("[router-acp pre-classifier]"))
        .collect()
}

#[tokio::test]
async fn planner_plan_ready_false_injects_protocol_not_current_plan_question() {
    // First-turn path that reproduced the defect: pre-class says
    // implementation + plan_ready=false, so maybe_update_planner_phase used
    // to inject "proceed with the current plan?" and return before host
    // planning_instructions. Both the built-in protocol and the host text
    // must land, and the premature question must not.
    let state = temp_state_file("planner-plan-ready-false");
    let log = temp_log("planner-plan-ready-false");
    let preclass = r#"{"routing":{"task_class":"BugFix","complexity":0.4,"confidence":0.9,"reason":"fix the bug"},"planner_phase":{"phase":"implementation","confidence":0.9,"plan_ready":false,"reason":"no plan attached"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         router: planner\n\
         routers:\n  planner:\n    planning_candidates: [\"*sol*\"]\n    \
         implementation_candidates: [\"*opus*\"]\n    \
         planning_instructions: HOST-PLAN-INSTRUCTIONS-MARKER\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*sol*\"]\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "plan",
            &[("sol", 1)],
            &[
                ("MOCK_PRECLASS_JSON", preclass),
                ("MOCK_LOG", log.to_str().unwrap()),
            ]
        ),
        agent_yaml(
            "work",
            &[("opus", 2)],
            &[("MOCK_LOG", log.to_str().unwrap())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "fix the login bug").await?;

        let phase = shared.with_session(&sid, |s| s.planner_phase).flatten();
        assert_eq!(
            phase,
            Some(router_acp::config::PlannerPhase::Planning),
            "implementation + plan_ready=false must stay in planning"
        );

        let prompts = planner_user_prompts(&log);
        assert_eq!(prompts.len(), 1, "one downstream user prompt: {prompts:?}");
        let prompt = &prompts[0];
        assert!(
            prompt.contains(router_acp::strategies::planner::PLAN_PROTOCOL_HEADER),
            "built-in plan-first protocol missing: {prompt}"
        );
        assert!(
            prompt.contains(router_acp::strategies::planner::HANDOFF_PROCEED),
            "stable proceed choice missing: {prompt}"
        );
        assert!(
            prompt.contains(router_acp::strategies::planner::HANDOFF_REFINE),
            "stable refine choice missing: {prompt}"
        );
        assert!(
            prompt.contains(router_acp::strategies::planner::HANDOFF_CREATE_EPIC),
            "stable Create EPIC choice missing: {prompt}"
        );
        assert!(
            prompt.contains("HOST-PLAN-INSTRUCTIONS-MARKER"),
            "host planning_instructions skipped: {prompt}"
        );
        assert!(
            !prompt.contains("with the current plan"),
            "premature current-plan question must be absent: {prompt}"
        );
        assert!(
            !prompt.contains("should we proceed to implementation"),
            "premature approval question must be absent: {prompt}"
        );

        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:sol:"),
            "planning candidate should serve the first turn: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn planner_planning_preclass_also_injects_protocol_and_host_instructions() {
    let state = temp_state_file("planner-planning-preclass");
    let log = temp_log("planner-planning-preclass");
    let preclass = r#"{"routing":{"task_class":"Feature","complexity":0.5,"confidence":0.9,"reason":"design first"},"planner_phase":{"phase":"planning","confidence":0.95,"plan_ready":false,"reason":"user asked for a plan"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         router: planner\n\
         routers:\n  planner:\n    planning_candidates: [\"*sol*\"]\n    \
         implementation_candidates: [\"*opus*\"]\n    \
         planning_instructions: HOST-PLAN-INSTRUCTIONS-MARKER\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*sol*\"]\n\
         agents:\n{}",
        state.display(),
        agent_yaml(
            "plan",
            &[("sol", 1)],
            &[
                ("MOCK_PRECLASS_JSON", preclass),
                ("MOCK_LOG", log.to_str().unwrap()),
            ]
        ),
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "draft a plan for the feature").await?;
        let prompts = planner_user_prompts(&log);
        let prompt = prompts.last().expect("downstream prompt");
        assert!(
            prompt.contains(router_acp::strategies::planner::PLAN_PROTOCOL_HEADER),
            "planning preclass must still inject the protocol: {prompt}"
        );
        assert!(
            prompt.contains("HOST-PLAN-INSTRUCTIONS-MARKER"),
            "planning preclass must still inject host instructions: {prompt}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn planner_phase_implementation_directive_switches_pinned_session() {
    // The Kory handoff queues `[router: phase=implementation]` after the
    // planning turn. That directive used to set the phase and then skip
    // maybe_update_planner_phase (explicit_routing), so the pin stayed on
    // the planning model. It must summarize-and-re-pin onto implementation.
    let state = temp_state_file("planner-phase-directive-switch");
    let log = temp_log("planner-phase-directive-switch");
    let preclass = r#"{"routing":{"task_class":"BugFix","complexity":0.4,"confidence":0.9,"reason":"fix the bug"},"planner_phase":{"phase":"planning","confidence":0.9,"plan_ready":false,"reason":"needs a plan"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         router: planner\n\
         routers:\n  planner:\n    planning_candidates: [\"*sol*\"]\n    \
         implementation_candidates: [\"*opus*\"]\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*sol*\"]\n\
         agents:\n{}{}",
        state.display(),
        agent_yaml(
            "plan",
            &[("sol", 1)],
            &[
                ("MOCK_PRECLASS_JSON", preclass),
                ("MOCK_LOG", log.to_str().unwrap()),
            ]
        ),
        agent_yaml(
            "work",
            &[("opus", 2)],
            &[("MOCK_LOG", log.to_str().unwrap())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "fix the login bug").await?;
        assert_eq!(
            shared.with_session(&sid, |s| s.planner_phase).flatten(),
            Some(router_acp::config::PlannerPhase::Planning)
        );
        assert!(
            agent_text(&observed, &sid).contains("echo:sol:"),
            "first turn on the planning candidate"
        );

        prompt_text(
            &cx,
            &sid,
            "[router: phase=implementation]\n[planner-handoff] start the work",
        )
        .await?;
        assert_eq!(
            shared.with_session(&sid, |s| s.planner_phase).flatten(),
            Some(router_acp::config::PlannerPhase::Implementation)
        );
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:opus:"),
            "phase=implementation must switch onto the implementation candidate: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn planner_ready_implementation_plan_picks_boosted_implementation_candidate() {
    // Live-shaped: router: planner, implementation pool terra/opus/grok with a
    // +2 grok boost and planning on sol. An implementation-phase, plan_ready
    // verdict must enter the implementation phase and route to grok, not sol.
    let state = temp_state_file("planner-impl-boost");
    let log = temp_log("planner-impl-boost");
    let preclass = r#"{"routing":{"task_class":"Architecture","task_classes":["Architecture"],"categories":["backend"],"complexity":0.55,"confidence":0.9,"reason":"implementation-ready ticket"},"planner_phase":{"phase":"implementation","confidence":0.95,"plan_ready":true,"reason":"ticket is ready to build"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         router: planner\n\
         routers:\n  planner:\n    planning_candidates: [\"*sol*\"]\n    \
         implementation_candidates: [\"*terra*\", \"*opus*\", \"*grok*\"]\n    \
         model_boosts:\n      - {{ pattern: \"*grok*\", implementation: 2.0, planning: 0.0 }}\n    \
         phase_upgrade_confidence: 0.7\n    apex_complexity: 0.85\n    floor_complexity: 0.15\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*sol*\"]\n  disclose: true\n\
         agents:\n{}{}{}",
        state.display(),
        agent_yaml(
            "codex",
            &[("sol", 1)],
            &[
                ("MOCK_PRECLASS_JSON", preclass),
                ("MOCK_LOG", log.to_str().unwrap()),
            ]
        ),
        agent_yaml(
            "claude",
            &[("terra", 2), ("opus", 3)],
            &[("MOCK_LOG", log.to_str().unwrap())]
        ),
        agent_yaml(
            "grok",
            &[("grok", 4)],
            &[("MOCK_LOG", log.to_str().unwrap())]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "Implement the attached ticket").await?;

        let phase = shared
            .with_session(&sid, |s| s.planner_phase)
            .expect("session");
        assert_eq!(
            phase,
            Some(router_acp::config::PlannerPhase::Implementation),
            "ready implementation plan must enter implementation phase"
        );

        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:grok:"),
            "implementation + grok boost must pin grok, not sol: {text}"
        );
        assert!(
            !text.contains("echo:sol:"),
            "the planning candidate must not take an implementation turn: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn planner_hard_prefix_pins_fable_not_opus() {
    let state = temp_state_file("planner-hard-prefix");
    let preclass = r#"{"routing":{"task_class":"Architecture","complexity":0.4,"confidence":0.9,"reason":"plan"},"planner_phase":{"phase":"planning","confidence":0.9,"plan_ready":false,"reason":"no plan"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         router: planner\n\
         routers:\n  planner:\n    planning_candidates: [\"*opus*\", \"*fable*\"]\n    \
         implementation_candidates: [\"*opus*\"]\n    \
         easy_planning_candidates: [\"*opus*\"]\n    \
         hard_planning_candidates: [\"*fable*\"]\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*opus*\"]\n\
         agents:\n{}",
        state.display(),
        agent_yaml(
            "claude",
            &[("opus", 4), ("fable", 5)],
            &[("MOCK_PRECLASS_JSON", preclass)]
        ),
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "hard: redesign the auth stack").await?;
        let pin = shared
            .with_session(&sid, |s| s.pin.as_ref().map(|p| p.candidate.to_string()))
            .flatten();
        assert!(
            pin.as_deref().is_some_and(|p| p.contains("fable")),
            "hard: must pin fable, got {pin:?}"
        );
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("`hard:` prefix"),
            "disclosure should name the prefix: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn planner_easy_prefix_pins_opus_even_at_apex_complexity() {
    let state = temp_state_file("planner-easy-prefix");
    let preclass = r#"{"routing":{"task_class":"Architecture","complexity":0.95,"confidence":0.9,"reason":"hard plan"},"planner_phase":{"phase":"planning","confidence":0.9,"plan_ready":false,"reason":"no plan"}}"#;
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         router: planner\n\
         routers:\n  auto:\n    apex_complexity: 0.9\n  planner:\n    \
         planning_candidates: [\"*opus*\", \"*fable*\"]\n    \
         implementation_candidates: [\"*opus*\"]\n    \
         easy_planning_candidates: [\"*opus*\"]\n    \
         hard_planning_candidates: [\"*fable*\"]\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*opus*\"]\n\
         agents:\n{}",
        state.display(),
        agent_yaml(
            "claude",
            &[("opus", 4), ("fable", 5)],
            &[("MOCK_PRECLASS_JSON", preclass)]
        ),
    );
    run_test_shared(yaml, async |cx, _observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "easy: list the endpoints").await?;
        let pin = shared
            .with_session(&sid, |s| s.pin.as_ref().map(|p| p.candidate.to_string()))
            .flatten();
        assert!(
            pin.as_deref().is_some_and(|p| p.contains("opus")),
            "easy: must pin opus even at apex complexity, got {pin:?}"
        );
        Ok(())
    })
    .await;
}

// ======================================================================
// Coordinator role: `_meta.router_acp.session_role = "coordinator"` pins
// the session to Planning / planning_candidates on every automatic path.
// Only an explicit human pick moves it.
// ======================================================================

fn coordinator_meta() -> agent_client_protocol::schema::v1::Meta {
    let mut meta = agent_client_protocol::schema::v1::Meta::new();
    meta.insert(
        "router_acp".to_string(),
        serde_json::json!({"session_role": "coordinator"}),
    );
    meta
}

async fn new_coordinator_session(cx: &ConnectionTo<AgentPeer>) -> Result<String, AcpError> {
    let resp = cx
        .send_request(NewSessionRequest::new(std::env::temp_dir()).meta(coordinator_meta()))
        .block_task()
        .await?;
    Ok(resp.session_id.0.to_string())
}

async fn coordinator_prompt(
    cx: &ConnectionTo<AgentPeer>,
    session_id: &str,
    text: &str,
) -> Result<PromptResponse, AcpError> {
    cx.send_request(
        PromptRequest::new(
            session_id.to_string(),
            vec![ContentBlock::from(text.to_string())],
        )
        .meta(coordinator_meta()),
    )
    .block_task()
    .await
}

/// `router: planner` with a planning `plan/sol` and an implementation-only
/// `work/opus`. `extra` is appended top-level config; `preclass` drives the
/// evaluator (served by sol).
fn coordinator_yaml(name: &str, preclass: &str, extra: &str, sol_env: &[(&str, &str)]) -> String {
    let state = temp_state_file(name);
    let mut env = vec![("MOCK_PRECLASS_JSON", preclass)];
    env.extend_from_slice(sol_env);
    format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         router: planner\n\
         routers:\n  planner:\n    planning_candidates: [\"*sol*\", \"*fable*\"]\n    \
         implementation_candidates: [\"*opus*\"]\n\
         pre_classifier:\n  enabled: true\n  evaluator: [\"*sol*\", \"*fable*\"]\n\
         {extra}agents:\n{}{}",
        state.display(),
        agent_yaml("plan", &[("sol", 1)], &env),
        agent_yaml("work", &[("opus", 2)], &[("MOCK_PRECLASS_JSON", preclass)]),
    )
}

const IMPL_READY_PRECLASS: &str = r#"{"routing":{"task_class":"BugFix","complexity":0.5,"confidence":0.9,"reason":"review"},"planner_phase":{"phase":"implementation","confidence":0.9,"plan_ready":true,"reason":"plan is ready"}}"#;
const PLANNING_PRECLASS: &str = r#"{"routing":{"task_class":"BugFix","complexity":0.5,"confidence":0.9,"reason":"plan"},"planner_phase":{"phase":"planning","confidence":0.9,"plan_ready":false,"reason":"needs a plan"}}"#;

#[tokio::test]
async fn coordinator_ignores_implementation_preclass_directive_and_heuristic() {
    // The live incident: an automated "Review Sub-tasks" nudge classified
    // implementation / plan_ready=true and re-pinned the parent onto the
    // workhorse. A coordinator must stay in Planning on every such signal.
    let yaml = coordinator_yaml("coord-phase", IMPL_READY_PRECLASS, "", &[]);
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_coordinator_session(&cx).await?;
        let planning = Some(router_acp::config::PlannerPhase::Planning);

        coordinator_prompt(&cx, &sid, "Review Sub-tasks: two children have PRs").await?;
        assert_eq!(
            shared.with_session(&sid, |s| s.planner_phase).flatten(),
            planning
        );

        coordinator_prompt(&cx, &sid, "[router: phase=implementation]\nstart the work").await?;
        assert_eq!(
            shared.with_session(&sid, |s| s.planner_phase).flatten(),
            planning
        );

        coordinator_prompt(&cx, &sid, "ok, implement the plan").await?;
        assert_eq!(
            shared.with_session(&sid, |s| s.planner_phase).flatten(),
            planning
        );

        let text = agent_text(&observed, &sid);
        assert!(
            !text.contains("echo:opus:"),
            "coordinator reached the workhorse: {text}"
        );
        assert!(
            text.contains("coordinator: rejected [router: phase=implementation]"),
            "the rejected directive is disclosed: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn non_coordinator_control_upgrades_on_the_same_preclass() {
    // Control for the test above: without the role, the same pre-class
    // moves the session onto the implementation pool.
    let yaml = coordinator_yaml("coord-control", IMPL_READY_PRECLASS, "", &[]);
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "Review Sub-tasks: two children have PRs").await?;
        assert_eq!(
            shared.with_session(&sid, |s| s.planner_phase).flatten(),
            Some(router_acp::config::PlannerPhase::Implementation)
        );
        assert!(agent_text(&observed, &sid).contains("echo:opus:"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn coordinator_role_on_a_prompt_is_sticky_and_pulls_back_to_planning() {
    // Untagged session/new; the first tagged prompt sets the role, and a
    // later untagged prompt does not clear it.
    let yaml = coordinator_yaml("coord-sticky", PLANNING_PRECLASS, "", &[]);
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        coordinator_prompt(&cx, &sid, "plan the epic").await?;
        assert!(shared.with_session(&sid, |s| s.coordinator).unwrap());
        prompt_text(&cx, &sid, "go ahead and build it").await?;
        assert!(shared.with_session(&sid, |s| s.coordinator).unwrap());
        assert_eq!(
            shared.with_session(&sid, |s| s.planner_phase).flatten(),
            Some(router_acp::config::PlannerPhase::Planning)
        );
        assert!(!agent_text(&observed, &sid).contains("echo:opus:"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn coordinator_refuses_a_skill_route_to_an_implementation_model() {
    let yaml = coordinator_yaml(
        "coord-skill",
        PLANNING_PRECLASS,
        "skill_routing:\n  - pattern: ship-pr\n    candidates: [\"*opus*\"]\n    \
         marks_implementation_phase: true\n",
        &[],
    );
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_coordinator_session(&cx).await?;
        coordinator_prompt(&cx, &sid, "plan the epic").await?;
        coordinator_prompt(&cx, &sid, "please run ship-pr on the child").await?;
        let text = agent_text(&observed, &sid);
        assert!(
            !text.contains("echo:opus:"),
            "skill moved the coordinator: {text}"
        );
        assert!(
            text.contains("coordinator: refused switch to work/opus"),
            "refusal is disclosed: {text}"
        );
        assert_eq!(
            shared.with_session(&sid, |s| s.planner_phase).flatten(),
            Some(router_acp::config::PlannerPhase::Planning)
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn coordinator_honors_explicit_human_picks() {
    // The model chip (`[router: switch=…]`) and the `model:` shorthand are
    // human picks and still move a coordinator anywhere.
    let yaml = coordinator_yaml("coord-user-pick", PLANNING_PRECLASS, "", &[]);
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_coordinator_session(&cx).await?;
        coordinator_prompt(&cx, &sid, "plan the epic").await?;
        coordinator_prompt(&cx, &sid, "opus: take this one yourself").await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched plan/sol → work/opus — requested via `opus:` shorthand"),
            "shorthand pick honored: {text}"
        );
        coordinator_prompt(&cx, &sid, "[router: switch=plan/sol]\nback to planning").await?;
        coordinator_prompt(&cx, &sid, "[router: switch=work/opus]\nagain").await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched plan/sol → work/opus — requested via [router: switch=…]"),
            "switch= pick honored: {text}"
        );
        assert!(shared.with_session(&sid, |s| s.coordinator).unwrap());
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn coordinator_honors_a_pre_pin_model_chip() {
    let yaml = coordinator_yaml("coord-chip", PLANNING_PRECLASS, "", &[]);
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_coordinator_session(&cx).await?;
        cx.send_request(SetSessionConfigOptionRequest::new(
            sid.clone(),
            "router.candidate".to_string(),
            SessionConfigOptionValue::value_id("work/opus"),
        ))
        .block_task()
        .await?;
        coordinator_prompt(&cx, &sid, "plan the epic").await?;
        assert!(agent_text(&observed, &sid).contains("echo:opus:"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn coordinator_failover_fails_visibly_instead_of_leaving_the_planning_pool() {
    // The only planning candidate is rate-limited. A normal session would
    // fail over to work/opus; a coordinator errors instead.
    let yaml = coordinator_yaml(
        "coord-failover",
        PLANNING_PRECLASS,
        "",
        &[("MOCK_FAIL_PROMPT_MSG", "rate limit")],
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_coordinator_session(&cx).await?;
        let result = coordinator_prompt(&cx, &sid, "plan the epic").await;
        let text = agent_text(&observed, &sid);
        assert!(
            !text.contains("echo:opus:"),
            "failover left the pool: {text}"
        );
        let err = result.expect_err("no planner candidate → the turn fails");
        assert!(
            format!("{err:?}").contains("rate limit")
                || format!("{err:?}").contains("no planning candidate"),
            "{err:?}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn non_coordinator_control_fails_over_to_the_workhorse() {
    // Control for the failover test above: without the role the same
    // rate-limited planner fails over onto work/opus.
    let yaml = coordinator_yaml(
        "coord-failover-control",
        PLANNING_PRECLASS,
        "",
        &[("MOCK_FAIL_PROMPT_MSG", "rate limit")],
    );
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "plan the epic").await?;
        assert!(agent_text(&observed, &sid).contains("echo:opus:"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn coordinator_keeps_a_human_pick_across_later_prompts() {
    // The model chip is a one-message `[router: switch=…]`. Later coordinator
    // prompts (nudges included) must not pull the session back.
    let yaml = coordinator_yaml("coord-pick-sticks", PLANNING_PRECLASS, "", &[]);
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_coordinator_session(&cx).await?;
        coordinator_prompt(&cx, &sid, "plan the epic").await?;
        coordinator_prompt(&cx, &sid, "[router: switch=work/opus]\nyou take it").await?;
        coordinator_prompt(&cx, &sid, "[router: phase=planning]\nReview Sub-tasks").await?;
        coordinator_prompt(&cx, &sid, "and another nudge").await?;
        let text = agent_text(&observed, &sid);
        assert!(
            !text.contains("work/opus → plan/sol"),
            "human pick undone: {text}"
        );
        assert!(shared.with_session(&sid, |s| s.pin_user_pick).unwrap());
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn coordinator_role_pulls_an_automatic_workhorse_pin_back_to_planning() {
    // The incident shape: an untagged turn auto-upgraded onto work/opus. The
    // next tagged prompt switches back to the planning pool.
    let yaml = coordinator_yaml("coord-pull-back", IMPL_READY_PRECLASS, "", &[]);
    run_test_shared(yaml, async |cx, observed, shared| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &sid, "Review Sub-tasks").await?;
        assert!(agent_text(&observed, &sid).contains("echo:opus:"));
        coordinator_prompt(&cx, &sid, "Review Sub-tasks again").await?;
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switched work/opus → plan/sol — coordinator returns"),
            "coordinator pulled back: {text}"
        );
        assert_eq!(
            shared.with_session(&sid, |s| s.planner_phase).flatten(),
            Some(router_acp::config::PlannerPhase::Planning)
        );
        Ok(())
    })
    .await;
}
