//! Protocol tests: router-acp against scripted mock ACP downstreams.
//!
//! Each test runs the router in-process over a duplex channel while the mock
//! downstream agents run as real subprocesses (`mock-agent` binary).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    AuthenticateRequest, CancelNotification, CloseSessionRequest, ContentBlock, Error as AcpError,
    ImageContent, InitializeRequest, InitializeResponse, NewSessionRequest, NewSessionResponse,
    PromptRequest, PromptResponse, ReadTextFileRequest, ReadTextFileResponse,
    RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionConfigKind, SessionConfigOptionValue,
    SessionConfigSelectOptions, SessionNotification, SessionUpdate, SetSessionConfigOptionRequest,
    StopReason,
};
use agent_client_protocol::{
    Agent as AgentPeer, Channel, Client as ClientPeer, ConnectionTo, Responder,
    on_receive_notification, on_receive_request,
};

use router_acp::config::Config;
use router_acp::session::{Shared, serve_shared};

fn mock_agent_exe() -> &'static str {
    env!("CARGO_BIN_EXE_mock-agent")
}

fn router_exe() -> &'static str {
    env!("CARGO_BIN_EXE_router-acp")
}

/// Events observed by the test client.
#[derive(Default)]
struct Observed {
    updates: Vec<SessionNotification>,
    permission_session_ids: Vec<String>,
    read_session_ids: Vec<String>,
}

type ObservedHandle = Arc<Mutex<Observed>>;

/// Run the router over a duplex channel and drive it with a test client.
async fn run_test<F>(cfg_yaml: String, test_fn: F)
where
    F: AsyncFnOnce(ConnectionTo<AgentPeer>, ObservedHandle) -> Result<(), AcpError>,
{
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "router_acp=warn".into()),
        )
        .with_writer(std::io::stderr)
        .try_init();
    let cfg = Config::from_yaml(&cfg_yaml).expect("test config parses");
    let shared = Shared::new(cfg).expect("shared state builds");
    let (channel_a, channel_b) = Channel::duplex();
    let router = tokio::spawn(serve_shared(shared, channel_a));

    let observed: ObservedHandle = Arc::new(Mutex::new(Observed::default()));
    let o_updates = observed.clone();
    let o_perm = observed.clone();
    let o_read = observed.clone();

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
            .connect_with(channel_b, async |cx| test_fn(cx, observed.clone()).await),
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
        out.push_str(&format!("        - {{ name: {k}, value: \"{v}\" }}\n"));
    }
    out.push_str("    model_selection: { type: config-option }\n    models:\n");
    for (id, cost) in models {
        out.push_str(&format!("      - {{ id: {id}, cost_rank: {cost} }}\n"));
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
async fn cancel_returns_cancelled_promptly() {
    let state = temp_state_file("cancel");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\nagents:\n{}",
        state.display(),
        agent_yaml("mock", &[("m1", 1)], &[])
    );
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let session = new_session(&cx).await?;
        let sid = session.session_id.0.to_string();

        let sent = cx.send_request(PromptRequest::new(
            sid.clone(),
            vec![ContentBlock::from("SLEEP:30000".to_string())],
        ));
        // Give routing time to pin and forward, then cancel.
        let cx2 = cx.clone();
        let sid2 = sid.clone();
        cx.spawn(async move {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            let _ = cx2.send_notification(CancelNotification::new(sid2));
            Ok(())
        })?;
        let resp = sent.block_task().await?;
        assert_eq!(resp.stop_reason, StopReason::Cancelled);
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

        // session/new before auth: auth_required.
        let err = new_session(&cx).await.unwrap_err();
        assert_eq!(err.code, AcpError::auth_required().code, "got: {err}");

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

fn delegation_yaml(
    state: &std::path::Path,
    log: &std::path::Path,
    max_concurrent: usize,
) -> String {
    format!(
        "state_file: {}\ndelegation: {{ enabled: true, max_concurrent: {max_concurrent} }}\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "cheap",
            &[("haiku", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
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
        assert!(
            text.contains("echo:haiku:turn two"),
            "answer arrived: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn no_failover_after_output_streamed() {
    let state = temp_state_file("no-failover");
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
        // fancy streams a chunk and THEN crashes: retrying could duplicate
        // side effects, so the error must surface instead of failing over.
        let err = prompt_text(&cx, &sid, "CHUNK_THEN_EXIT").await.unwrap_err();
        assert!(!format!("{err}").is_empty());
        let text = agent_text(&observed, &sid);
        assert!(text.contains("partial output before crash"), "got: {text}");
        assert!(
            !text.contains("failover: "),
            "no failover after visible output: {text}"
        );
        assert!(
            text.contains("not failing over because this turn already produced output"),
            "user told why: {text}"
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

        // Session 1: trivial prompt -> a mid-tier claude model, not codex.
        let s1 = new_session(&cx).await?.session_id.0.to_string();
        prompt_text(&cx, &s1, "hello world").await?;
        let text1 = agent_text(&observed, &s1);
        assert!(
            text1.contains("auto → claude/sonnet"),
            "trivial prompt routes to a cheap-ish claude model: {text1}"
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
// Prompt routing directives (orchestration support)
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
            prompts.iter().any(|p| p == "do the thing"),
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
async fn switch_to_unknown_candidate_stays_put_and_keeps_working() {
    // `switch=a/bogus` (undeclared) must not break the session: no wasted
    // summary turn, a clear notice, and the pinned model keeps answering.
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
        let resp = prompt_text(&cx, &sid, "[router: switch=a/bogus] keep going").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("switch to a/bogus failed"),
            "failure disclosed: {text}"
        );
        // Still on the original model, and the task text was forwarded to it.
        assert!(text.contains("echo:m1:keep going"), "stayed on m1: {text}");
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

#[tokio::test]
async fn orchestration_pins_planner_and_injects_protocol_on_a_list() {
    let state = temp_state_file("orch-pin");
    let log = temp_log("orch-pin");
    // Planner = b/m2. A multi-part list on a fresh session must pin the planner
    // and prepend the orchestration protocol (visible in the mock's echo).
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         orchestration:\n  enabled: true\n  min_items: 2\n  planner: [\"*m2*\"]\n\
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
    let state_path = state.clone();
    run_test(yaml, async |cx, observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        let resp = prompt_text(
            &cx,
            &sid,
            "Please handle these:\n1. add a flag\n2. wire it up\n3. document it",
        )
        .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("orchestrating a 3-part task"),
            "orchestration disclosed: {text}"
        );
        assert!(text.contains("echo:m2:"), "pinned the planner b/m2: {text}");
        assert!(
            text.contains("you are the ORCHESTRATOR"),
            "orchestration protocol injected into the prompt: {text}"
        );
        // Must forbid the model's built-in sub-agent tool (which stays in-lineage
        // and is invisible to the router).
        assert!(
            text.contains("Do NOT use any built-in sub-agent"),
            "protocol forbids the native Task tool: {text}"
        );
        // Must name a concrete cross-lineage reviewer (a/m1, lineage a ≠ planner b).
        assert!(
            text.contains("a/m1") && text.contains("DIFFERENT lineage"),
            "protocol pins the review to a different lineage: {text}"
        );
        // The session is grouped under the orchestrate run label.
        let db = open_state(&state_path);
        let row = db.get(&sid).expect("session row");
        assert_eq!(row.run_label.as_deref(), Some("orchestrate"));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn orchestration_delegate_children_inherit_run_label_and_parent() {
    // An orchestrating session that actually delegates gets a linked, labelled
    // sub-session row — the observability the DB was missing when the planner
    // used its built-in (router-invisible) sub-agent tool instead.
    let state = temp_state_file("orch-deleg");
    let log = temp_log("orch-deleg");
    unsafe { std::env::set_var("ROUTER_ACP_HELPER_EXE", router_exe()) };
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: true, max_concurrent: 3 }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         orchestration:\n  enabled: true\n  min_items: 2\n  planner: [\"*opus*\"]\n\
         routers:\n  auto: {{ cost_quality_tradeoff: 0 }}\nagents:\n{}{}",
        state.display(),
        agent_yaml(
            "cheap",
            &[("haiku", 1)],
            &[("MOCK_LOG", &log.display().to_string())]
        ),
        agent_yaml("fancy", &[("opus", 3)], &[]),
    );
    let state_path = state.clone();
    run_test(yaml, async |cx, _observed| {
        init(&cx).await?;
        let sid = new_session(&cx).await?.session_id.0.to_string();
        // A list (orchestration triggers → pins fancy/opus planner) whose prompt
        // also drives the mock to actually call delegate_task.
        prompt_text(
            &cx,
            &sid,
            "Handle these:\n1. first thing\n2. second thing\nDELEGATE:do the subtask",
        )
        .await?;
        let db = open_state(&state_path);
        let parent = db.get(&sid).expect("parent row");
        assert_eq!(parent.run_label.as_deref(), Some("orchestrate"));
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
            Some("orchestrate"),
            "delegate child inherits the run label"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn explicit_directive_suppresses_auto_orchestration() {
    let state = temp_state_file("orch-suppress");
    let log = temp_log("orch-suppress");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         orchestration:\n  enabled: true\n  min_items: 2\n  planner: [\"*m2*\"]\n\
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
        // A list, but the user forces a candidate — orchestration must NOT fire.
        let resp = prompt_text(
            &cx,
            &sid,
            "[router: candidate=a/m1]\nDo these:\n1. one\n2. two\n3. three",
        )
        .await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("echo:m1:"),
            "honored explicit pin a/m1: {text}"
        );
        assert!(
            !text.contains("you are the ORCHESTRATOR"),
            "no orchestration protocol injected: {text}"
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn orchestration_switches_pinned_session_on_a_list() {
    let state = temp_state_file("orch-switch");
    let log = temp_log("orch-switch");
    let yaml = format!(
        "state_file: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\n\
         orchestration:\n  enabled: true\n  min_items: 2\n  planner: [\"*m2*\"]\n\
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
        // Pin a/m1 with a plain (non-list) prompt.
        prompt_text(&cx, &sid, "[router: candidate=a/m1]\nwarm up").await?;
        // A follow-up list must switch the live session onto the planner.
        let resp = prompt_text(&cx, &sid, "Now: (1) refactor (2) add tests (3) ship").await?;
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        let text = agent_text(&observed, &sid);
        assert!(
            text.contains("orchestrating a 3-part task"),
            "orchestration disclosed: {text}"
        );
        assert!(
            text.contains("echo:m2:"),
            "switched to the planner b/m2: {text}"
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
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\nagents:\n{}{}",
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
async fn auto_upgrade_disabled_keeps_the_pinned_model() {
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
        "state_file: {}\nscore_table: {}\ndelegation: {{ enabled: false }}\n\
         auto_upgrade: {{ enabled: false }}\nagents:\n{}{}",
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
        assert!(!text.contains("switched"), "no upgrade happened: {text}");
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
