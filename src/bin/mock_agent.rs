//! Scripted ACP downstream agent for router-acp protocol tests.
//!
//! Behavior is controlled by environment variables:
//!
//! - `MOCK_NAME`: agent name (default `mock`)
//! - `MOCK_MODELS`: comma-separated model ids exposed as a `category: model`
//!   select config option (default `m1,m2`); the first is current
//! - `MOCK_AUTH_REQUIRED=1`: `session/new` returns `auth_required` until
//!   `authenticate` is called (any method id; advertised as `mock-login`)
//! - `MOCK_FAIL_NEW_AFTER=<n>`: `session/new` fails after the first n
//!   successes (probe succeeds, pin-time creation fails)
//! - `MOCK_IGNORE_SET_CONFIG=1`: `set_config_option` silently keeps the old
//!   model (verification must catch this)
//! - `MOCK_EXIT_AFTER_INIT=1`: process exits shortly after initialize
//! - `MOCK_EXIT_ON_PROMPT=1`: process exits on receiving a prompt
//! - `MOCK_CAPS_IMAGE=1`: advertise the image prompt capability
//! - `MOCK_LOG`: append JSONL events (initialize/authenticate/new/
//!   set_config/prompt/cancel) for test assertions
//!
//! Prompt text directives:
//! - `PERM` — request permission from the client, echo the outcome
//! - `READFILE:<path>` — fs/read_text_file via the client, echo contents
//! - `SLEEP:<ms>` — wait, honoring `session/cancel`
//! - `DELEGATE:<task>` — call the `delegate_task` tool on the MCP server
//!   named `router-delegate` passed in session/new (may repeat; all
//!   delegations run concurrently)
//! - `DELEGATE_BG:<task>` — call `delegate_task` with `background: true`
//!   (returns the immediate `b-…` ack; may repeat)
//! - `AWAIT_DELEGATES` — call `delegate_await` (all pending jobs); an
//!   optional `:<secs>` suffix sets `timeout_seconds`
//! - otherwise — echo `echo:<model>:<text>`

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use agent_client_protocol::schema::v1::{
    AgentCapabilities, AuthMethod, AuthMethodAgent, AuthenticateRequest, AuthenticateResponse,
    CancelNotification, ContentBlock, ContentChunk, Error as AcpError, Implementation,
    InitializeRequest, InitializeResponse, McpServer, NewSessionRequest, NewSessionResponse,
    PermissionOption, PermissionOptionKind, PromptCapabilities, PromptRequest, PromptResponse,
    ReadTextFileRequest, RequestPermissionOutcome, RequestPermissionRequest, SessionConfigOption,
    SessionConfigOptionCategory, SessionConfigOptionValue, SessionConfigSelectOption,
    SessionNotification, SessionUpdate, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, StopReason, ToolCall, ToolCallStatus, ToolCallUpdate, ToolKind,
};
use agent_client_protocol::{
    Agent as AgentRole, Client as ClientRole, ConnectionTo, Responder, on_receive_notification,
    on_receive_request,
};

struct SessionState {
    model: String,
    cancelled: bool,
    mcp_servers: Vec<McpServer>,
}

struct MockState {
    authed: bool,
    new_successes: u32,
    prompt_failures: u32,
    prompt_successes: u32,
    sessions: HashMap<String, SessionState>,
    next_id: u32,
}

struct Mock {
    name: String,
    supports_lifecycle: bool,
    session_modes: Vec<String>,
    models: Vec<String>,
    auth_required: bool,
    fail_new_after: Option<u32>,
    ignore_set_config: bool,
    exit_after_init: bool,
    exit_on_prompt: bool,
    /// When set, prompts fail with this error message (data field).
    /// `MOCK_FAIL_PROMPT_TIMES` bounds how many prompts fail (default: all).
    fail_prompt_msg: Option<String>,
    fail_prompt_times: Option<u32>,
    /// Only start failing prompts after this many successes.
    fail_prompt_after: u32,
    caps_image: bool,
    log_path: Option<String>,
    state: Mutex<MockState>,
}

impl Mock {
    fn from_env() -> Arc<Self> {
        let models: Vec<String> = std::env::var("MOCK_MODELS")
            .unwrap_or_else(|_| "m1,m2".into())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Arc::new(Self {
            name: std::env::var("MOCK_NAME").unwrap_or_else(|_| "mock".into()),
            supports_lifecycle: std::env::var("MOCK_SUPPORTS_LIFECYCLE").is_ok(),
            session_modes: std::env::var("MOCK_SESSION_MODES")
                .map(|v| {
                    v.split(',')
                        .map(|m| m.trim().to_string())
                        .filter(|m| !m.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            models,
            auth_required: std::env::var("MOCK_AUTH_REQUIRED").is_ok(),
            fail_new_after: std::env::var("MOCK_FAIL_NEW_AFTER")
                .ok()
                .and_then(|v| v.parse().ok()),
            ignore_set_config: std::env::var("MOCK_IGNORE_SET_CONFIG").is_ok(),
            exit_after_init: std::env::var("MOCK_EXIT_AFTER_INIT").is_ok(),
            exit_on_prompt: std::env::var("MOCK_EXIT_ON_PROMPT").is_ok(),
            fail_prompt_msg: std::env::var("MOCK_FAIL_PROMPT_MSG").ok(),
            fail_prompt_times: std::env::var("MOCK_FAIL_PROMPT_TIMES")
                .ok()
                .and_then(|v| v.parse().ok()),
            fail_prompt_after: std::env::var("MOCK_FAIL_PROMPT_AFTER")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            caps_image: std::env::var("MOCK_CAPS_IMAGE").is_ok(),
            log_path: std::env::var("MOCK_LOG").ok(),
            state: Mutex::new(MockState {
                authed: false,
                new_successes: 0,
                prompt_failures: 0,
                prompt_successes: 0,
                sessions: HashMap::new(),
                next_id: 0,
            }),
        })
    }

    fn log(&self, event: Value) {
        let Some(path) = &self.log_path else { return };
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let mut line = event.to_string();
            line.push('\n');
            let _ = f.write_all(line.as_bytes());
        }
    }

    fn model_option(&self, current: &str) -> SessionConfigOption {
        SessionConfigOption::select(
            "model",
            "Model",
            current.to_string(),
            self.models
                .iter()
                .map(|m| SessionConfigSelectOption::new(m.clone(), m.clone()))
                .collect::<Vec<_>>(),
        )
        .category(SessionConfigOptionCategory::Model)
    }
}

fn prompt_text(prompt: &[ContentBlock]) -> String {
    let mut out = String::new();
    for block in prompt {
        if let ContentBlock::Text(t) = block {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&t.text);
        }
    }
    out
}

fn chunk(session_id: &str, text: String) -> SessionNotification {
    SessionNotification::new(
        session_id.to_string(),
        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(text))),
    )
}

/// Minimal MCP-over-stdio client used to exercise the router's delegate
/// tool. Speaks newline-delimited JSON-RPC to a spawned server process.
struct McpClient {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    reader: tokio::io::BufReader<tokio::process::ChildStdout>,
    next_id: u64,
}

impl McpClient {
    async fn spawn(server: &McpServer) -> Result<Self, String> {
        let McpServer::Stdio(stdio) = server else {
            return Err("only stdio MCP servers supported".into());
        };
        let mut cmd = tokio::process::Command::new(&stdio.command);
        cmd.args(&stdio.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        for env in &stdio.env {
            cmd.env(&env.name, &env.value);
        }
        let mut child = cmd.spawn().map_err(|e| format!("spawn mcp server: {e}"))?;
        let stdin = child.stdin.take().ok_or("no stdin")?;
        let stdout = child.stdout.take().ok_or("no stdout")?;
        let mut client = Self {
            child,
            stdin,
            reader: tokio::io::BufReader::new(stdout),
            next_id: 1,
        };
        client
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "mock-agent", "version": "0"}
                }),
            )
            .await?;
        client
            .notify("notifications/initialized", json!({}))
            .await?;
        Ok(client)
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<(), String> {
        use tokio::io::AsyncWriteExt;
        let msg = json!({"jsonrpc": "2.0", "method": method, "params": params});
        let line = format!("{msg}\n");
        self.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        self.stdin.flush().await.map_err(|e| e.to_string())
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, String> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let id = self.next_id;
        self.next_id += 1;
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let line = format!("{msg}\n");
        self.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        self.stdin.flush().await.map_err(|e| e.to_string())?;
        loop {
            let mut line = String::new();
            let n = self
                .reader
                .read_line(&mut line)
                .await
                .map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("mcp server closed".into());
            }
            let value: Value = match serde_json::from_str(line.trim()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if value.get("id").and_then(|i| i.as_u64()) == Some(id) {
                if let Some(err) = value.get("error") {
                    return Err(err.to_string());
                }
                return Ok(value.get("result").cloned().unwrap_or(Value::Null));
            }
        }
    }

    async fn shutdown(mut self) {
        let _ = self.child.kill().await;
    }
}

async fn run_prompt(
    mock: Arc<Mock>,
    req: PromptRequest,
    responder: Responder<PromptResponse>,
    cx: ConnectionTo<ClientRole>,
) -> Result<(), AcpError> {
    let session_id = req.session_id.0.to_string();
    let text = prompt_text(&req.prompt);
    let (model, mcp_servers) = {
        let state = mock.state.lock().unwrap();
        match state.sessions.get(&session_id) {
            Some(s) => (s.model.clone(), s.mcp_servers.clone()),
            None => {
                return responder
                    .respond_with_error(AcpError::invalid_params().data("unknown session"));
            }
        }
    };
    let started_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    mock.log(json!({
        "event": "prompt",
        "sessionId": session_id,
        "text": text,
        "model": model,
        "startedAtMs": started_at_ms,
    }));

    if mock.exit_on_prompt {
        // Simulate a crash mid-turn.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        std::process::exit(1);
    }

    // Scripted prompt failure (e.g. a token-limit error with a reset time).
    if let Some(msg) = &mock.fail_prompt_msg {
        let should_fail = {
            let mut state = mock.state.lock().unwrap();
            let allowed = mock.fail_prompt_times.unwrap_or(u32::MAX);
            if state.prompt_successes < mock.fail_prompt_after {
                state.prompt_successes += 1;
                false
            } else if state.prompt_failures < allowed {
                state.prompt_failures += 1;
                true
            } else {
                false
            }
        };
        if should_fail {
            return responder.respond_with_error(AcpError::internal_error().data(msg.clone()));
        }
    }

    // Stream a chunk first, then crash: failover must NOT trigger.
    if text.contains("CHUNK_THEN_EXIT") {
        let _ = cx.send_notification(chunk(&session_id, "partial output before crash".into()));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        std::process::exit(1);
    }

    let mut reply = Vec::new();
    let mut tool_seq = 0u32;

    for line in text.lines() {
        let line = line.trim();
        // Cancel-aware between steps (so a mid-turn escalation's session/cancel
        // stops the investigation promptly, like a real adapter would).
        if mock
            .state
            .lock()
            .unwrap()
            .sessions
            .get(&session_id)
            .map(|s| s.cancelled)
            .unwrap_or(false)
        {
            return responder.respond(PromptResponse::new(StopReason::Cancelled));
        }
        if let Some(spec) = line.strip_prefix("TOOL:") {
            // Emit a real `session/update` tool_call, the way claude-agent-acp
            // does. Forms: `read` | `exec:<cmd>` | `edit` | `fail` | `mcp:<name>`.
            tool_seq += 1;
            let id = format!("tc-{tool_seq}");
            let tc = if spec == "read" {
                ToolCall::new(id, "Read")
                    .kind(ToolKind::Read)
                    .status(ToolCallStatus::Completed)
            } else if spec == "edit" {
                ToolCall::new(id, "Edit")
                    .kind(ToolKind::Edit)
                    .status(ToolCallStatus::Completed)
            } else if spec == "fail" {
                ToolCall::new(id, "Bash")
                    .kind(ToolKind::Execute)
                    .status(ToolCallStatus::Failed)
            } else if let Some(cmd) = spec.strip_prefix("exec:") {
                ToolCall::new(id, "Bash")
                    .kind(ToolKind::Execute)
                    .status(ToolCallStatus::Completed)
                    .raw_input(serde_json::json!({ "command": cmd }))
            } else if let Some(name) = spec.strip_prefix("mcp:") {
                ToolCall::new(id, name.to_string())
                    .kind(ToolKind::Other)
                    .status(ToolCallStatus::Completed)
            } else {
                ToolCall::new(id, "tool").kind(ToolKind::Other)
            };
            let _ = cx.send_notification(SessionNotification::new(
                session_id.clone(),
                SessionUpdate::ToolCall(tc),
            ));
            reply.push(format!("tool:{spec}"));
        } else if line == "PERM" {
            let perm = RequestPermissionRequest::new(
                session_id.clone(),
                ToolCallUpdate::new("mock-tool-call", Default::default()),
                vec![
                    PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
                    PermissionOption::new("deny", "Deny", PermissionOptionKind::RejectOnce),
                ],
            );
            match cx.send_request(perm).block_task().await {
                Ok(resp) => {
                    let outcome = match resp.outcome {
                        RequestPermissionOutcome::Selected(sel) => {
                            format!("selected:{}", sel.option_id.0)
                        }
                        RequestPermissionOutcome::Cancelled => "cancelled".to_string(),
                        _ => "other".to_string(),
                    };
                    reply.push(format!("perm:{outcome}"));
                }
                Err(err) => reply.push(format!("perm-error:{err}")),
            }
        } else if let Some(path) = line.strip_prefix("READFILE:") {
            let read = ReadTextFileRequest::new(session_id.clone(), path.to_string());
            match cx.send_request(read).block_task().await {
                Ok(resp) => reply.push(format!("read:{}", resp.content)),
                Err(err) => reply.push(format!("read-error:{err}")),
            }
        } else if let Some(title) = line.strip_prefix("TITLE:") {
            // Emit a session_info_update naming the conversation, like real
            // adapters do once they generate a title.
            let update = agent_client_protocol::schema::v1::SessionInfoUpdate::new()
                .title(title.to_string());
            let _ = cx.send_notification(SessionNotification::new(
                session_id.clone(),
                SessionUpdate::SessionInfoUpdate(update),
            ));
            reply.push(format!("titled:{title}"));
        } else if let Some(ms) = line.strip_prefix("SLEEP:") {
            let ms: u64 = ms.parse().unwrap_or(100);
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(ms);
            loop {
                if mock
                    .state
                    .lock()
                    .unwrap()
                    .sessions
                    .get(&session_id)
                    .map(|s| s.cancelled)
                    .unwrap_or(false)
                {
                    return responder.respond(PromptResponse::new(StopReason::Cancelled));
                }
                if std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            reply.push(format!("slept:{ms}"));
        }
    }

    // Run all DELEGATE directives concurrently, one MCP client each; the
    // router caps concurrency on its side.
    let delegate_tasks: Vec<&str> = text
        .lines()
        .filter_map(|l| l.trim().strip_prefix("DELEGATE:"))
        .collect();
    if !delegate_tasks.is_empty() {
        let delegate_server = mcp_servers
            .iter()
            .find(|s| matches!(s, McpServer::Stdio(stdio) if stdio.name == "router-delegate"));
        match delegate_server {
            Some(server) => {
                let futures: Vec<_> = delegate_tasks
                    .iter()
                    .map(|task| {
                        let server = server.clone();
                        let task = task.to_string();
                        async move {
                            let mut client = McpClient::spawn(&server).await?;
                            let result = client
                                .request(
                                    "tools/call",
                                    json!({"name": "delegate_task", "arguments": {"task": task}}),
                                )
                                .await;
                            client.shutdown().await;
                            result
                        }
                    })
                    .collect();
                let results = futures::future::join_all(futures).await;
                for result in results {
                    match result {
                        Ok(value) => {
                            let text = value["content"][0]["text"].as_str().unwrap_or("");
                            let is_error = value["isError"].as_bool().unwrap_or(false);
                            reply.push(format!(
                                "delegate{}:{text}",
                                if is_error { "-error" } else { "" }
                            ));
                        }
                        Err(err) => reply.push(format!("delegate-error:{err}")),
                    }
                }
            }
            None => reply.push("delegate-error:no router-delegate MCP server".to_string()),
        }
    }

    // Background delegation + collection: fire every DELEGATE_BG (immediate
    // acks), then AWAIT_DELEGATES to gather the results — one prompt exercises
    // the whole parallel flow.
    let bg_tasks: Vec<&str> = text
        .lines()
        .filter_map(|l| l.trim().strip_prefix("DELEGATE_BG:"))
        .collect();
    let await_spec = text
        .lines()
        .find_map(|l| l.trim().strip_prefix("AWAIT_DELEGATES").map(str::to_string));
    if !bg_tasks.is_empty() || await_spec.is_some() {
        let delegate_server = mcp_servers
            .iter()
            .find(|s| matches!(s, McpServer::Stdio(stdio) if stdio.name == "router-delegate"));
        match delegate_server {
            Some(server) => {
                let mut client = match McpClient::spawn(server).await {
                    Ok(client) => client,
                    Err(err) => {
                        reply.push(format!("delegate-bg-error:{err}"));
                        return finish_prompt(&text, model, session_id, reply, &cx, responder);
                    }
                };
                for task in &bg_tasks {
                    let result = client
                        .request(
                            "tools/call",
                            json!({
                                "name": "delegate_task",
                                "arguments": {"task": task, "background": true},
                            }),
                        )
                        .await;
                    match result {
                        Ok(value) => {
                            let text = value["content"][0]["text"].as_str().unwrap_or("");
                            let is_error = value["isError"].as_bool().unwrap_or(false);
                            reply.push(format!(
                                "delegate-bg{}:{text}",
                                if is_error { "-error" } else { "" }
                            ));
                        }
                        Err(err) => reply.push(format!("delegate-bg-error:{err}")),
                    }
                }
                if let Some(spec) = await_spec {
                    let mut arguments = json!({});
                    if let Some(secs) = spec.strip_prefix(':').and_then(|s| s.parse::<u64>().ok()) {
                        arguments["timeout_seconds"] = json!(secs);
                    }
                    let result = client
                        .request(
                            "tools/call",
                            json!({"name": "delegate_await", "arguments": arguments}),
                        )
                        .await;
                    match result {
                        Ok(value) => {
                            let text = value["content"][0]["text"].as_str().unwrap_or("");
                            let is_error = value["isError"].as_bool().unwrap_or(false);
                            reply.push(format!(
                                "await{}:{text}",
                                if is_error { "-error" } else { "" }
                            ));
                        }
                        Err(err) => reply.push(format!("await-error:{err}")),
                    }
                }
                client.shutdown().await;
            }
            None => reply.push("delegate-bg-error:no router-delegate MCP server".to_string()),
        }
    }

    finish_prompt(&text, model, session_id, reply, &cx, responder)
}

fn finish_prompt(
    text: &str,
    model: String,
    session_id: String,
    mut reply: Vec<String>,
    cx: &ConnectionTo<ClientRole>,
    responder: Responder<PromptResponse>,
) -> Result<(), AcpError> {
    if reply.is_empty() {
        reply.push(format!("echo:{model}:{text}"));
    }

    let _ = cx.send_notification(chunk(&session_id, reply.join("\n")));
    // `MAXTOKENS` / `REFUSE` directives end the turn with that stop reason, so
    // tests can exercise post-turn escalation triggers.
    let stop = if text.contains("MAXTOKENS") {
        StopReason::MaxTokens
    } else if text.contains("REFUSE") {
        StopReason::Refusal
    } else {
        StopReason::EndTurn
    };
    responder.respond(PromptResponse::new(stop))
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let mock = Mock::from_env();

    let m_init = mock.clone();
    let m_auth = mock.clone();
    let m_new = mock.clone();
    let m_set = mock.clone();
    let m_prompt = mock.clone();
    let m_cancel = mock.clone();
    let m_setmode = mock.clone();
    let m_load = mock.clone();
    let m_list = mock.clone();
    let m_close = mock.clone();
    let m_delete = mock.clone();
    let m_resume = mock.clone();

    let result = AgentRole
        .builder()
        .name("mock-agent")
        .on_receive_request(
            move |req: InitializeRequest,
                  responder: Responder<InitializeResponse>,
                  cx: ConnectionTo<ClientRole>| {
                let mock = m_init.clone();
                async move {
                    mock.log(json!({"event": "initialize"}));
                    let session_caps = if mock.supports_lifecycle {
                        agent_client_protocol::schema::v1::SessionCapabilities::new()
                            .list(Some(Default::default()))
                            .close(Some(Default::default()))
                            .delete(Some(Default::default()))
                            .resume(Some(Default::default()))
                    } else {
                        Default::default()
                    };
                    let caps = AgentCapabilities::new()
                        .load_session(mock.supports_lifecycle)
                        .prompt_capabilities(
                            PromptCapabilities::new()
                                .image(mock.caps_image)
                                .embedded_context(true),
                        )
                        .session_capabilities(session_caps);
                    let mut resp = InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(caps)
                        .agent_info(Implementation::new(mock.name.clone(), "0.1"));
                    if mock.auth_required {
                        resp = resp.auth_methods(vec![AuthMethod::Agent(AuthMethodAgent::new(
                            "mock-login",
                            "Mock login",
                        ))]);
                    }
                    if mock.exit_after_init {
                        cx.spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            std::process::exit(1);
                        })?;
                    }
                    responder.respond(resp)
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |_req: AuthenticateRequest, responder: Responder<AuthenticateResponse>, _cx| {
                let mock = m_auth.clone();
                async move {
                    mock.log(json!({"event": "authenticate"}));
                    mock.state.lock().unwrap().authed = true;
                    responder.respond(AuthenticateResponse::new())
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |req: NewSessionRequest, responder: Responder<NewSessionResponse>, _cx| {
                let mock = m_new.clone();
                async move {
                    let (result, count) = {
                        let mut state = mock.state.lock().unwrap();
                        if mock.auth_required && !state.authed {
                            (Err(AcpError::auth_required()), state.new_successes)
                        } else if mock
                            .fail_new_after
                            .map(|n| state.new_successes >= n)
                            .unwrap_or(false)
                        {
                            (
                                Err(AcpError::internal_error()
                                    .data("mock: session creation disabled")),
                                state.new_successes,
                            )
                        } else {
                            state.next_id += 1;
                            state.new_successes += 1;
                            let sid = format!("{}-sess-{}", mock.name, state.next_id);
                            state.sessions.insert(
                                sid.clone(),
                                SessionState {
                                    model: mock.models[0].clone(),
                                    cancelled: false,
                                    mcp_servers: req.mcp_servers.clone(),
                                },
                            );
                            (Ok(sid), state.new_successes)
                        }
                    };
                    match result {
                        Ok(sid) => {
                            mock.log(json!({
                                "event": "session_new",
                                "sessionId": sid,
                                "count": count,
                                "mcpServers": req.mcp_servers.iter().map(|s| match s {
                                    McpServer::Stdio(st) => st.name.clone(),
                                    _ => "other".to_string(),
                                }).collect::<Vec<_>>(),
                            }));
                            let option = mock.model_option(&mock.models[0]);
                            let mut resp =
                                NewSessionResponse::new(sid).config_options(vec![option]);
                            if !mock.session_modes.is_empty() {
                                use agent_client_protocol::schema::v1::{
                                    SessionMode, SessionModeState,
                                };
                                resp = resp.modes(SessionModeState::new(
                                    mock.session_modes[0].clone(),
                                    mock.session_modes
                                        .iter()
                                        .map(|m| SessionMode::new(m.clone(), m.clone()))
                                        .collect::<Vec<_>>(),
                                ));
                            }
                            responder.respond(resp)
                        }
                        Err(err) => {
                            mock.log(json!({"event": "session_new_failed"}));
                            responder.respond_with_error(err)
                        }
                    }
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |req: SetSessionConfigOptionRequest,
                  responder: Responder<SetSessionConfigOptionResponse>,
                  _cx| {
                let mock = m_set.clone();
                async move {
                    let sid = req.session_id.0.to_string();
                    let value = match &req.value {
                        SessionConfigOptionValue::ValueId { value } => value.0.to_string(),
                        _ => String::new(),
                    };
                    mock.log(json!({
                        "event": "set_config_option",
                        "sessionId": sid,
                        "configId": req.config_id.0.to_string(),
                        "value": value,
                    }));
                    if req.config_id.0.as_ref() != "model" {
                        return responder.respond_with_error(
                            AcpError::invalid_params().data("unknown config id"),
                        );
                    }
                    if !mock.models.contains(&value) {
                        return responder
                            .respond_with_error(AcpError::invalid_params().data("unknown model"));
                    }
                    let current = {
                        let mut state = mock.state.lock().unwrap();
                        let Some(session) = state.sessions.get_mut(&sid) else {
                            return responder.respond_with_error(
                                AcpError::invalid_params().data("unknown session"),
                            );
                        };
                        if !mock.ignore_set_config {
                            session.model = value.clone();
                        }
                        session.model.clone()
                    };
                    let option = mock.model_option(&current);
                    responder.respond(SetSessionConfigOptionResponse::new(vec![option]))
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |req: PromptRequest,
                  responder: Responder<PromptResponse>,
                  cx: ConnectionTo<ClientRole>| {
                let mock = m_prompt.clone();
                async move {
                    // Heavy work happens in a task so cancel notifications
                    // keep flowing.
                    cx.spawn(run_prompt(mock, req, responder, cx.clone()))
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |req: agent_client_protocol::schema::v1::LoadSessionRequest,
                  responder: Responder<agent_client_protocol::schema::v1::LoadSessionResponse>,
                  cx: ConnectionTo<ClientRole>| {
                let mock = m_load.clone();
                async move {
                    let sid = req.session_id.0.to_string();
                    mock.log(json!({"event": "session_load", "sessionId": sid}));
                    let known = {
                        let mut state = mock.state.lock().unwrap();
                        if let Some(s) = state.sessions.get_mut(&sid) {
                            s.mcp_servers = req.mcp_servers.clone();
                            true
                        } else {
                            false
                        }
                    };
                    if !known {
                        return responder.respond_with_error(
                            AcpError::invalid_params().data("unknown session"),
                        );
                    }
                    // Replay one transcript update before responding.
                    let _ = cx.send_notification(chunk(&sid, format!("replayed:{sid}")));
                    responder.respond(
                        agent_client_protocol::schema::v1::LoadSessionResponse::new()
                            .config_options(vec![mock.model_option(&mock.models[0])]),
                    )
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |_req: agent_client_protocol::schema::v1::ListSessionsRequest,
                  responder: Responder<agent_client_protocol::schema::v1::ListSessionsResponse>,
                  _cx| {
                let mock = m_list.clone();
                async move {
                    let sessions: Vec<agent_client_protocol::schema::v1::SessionInfo> = mock
                        .state
                        .lock()
                        .unwrap()
                        .sessions
                        .keys()
                        .map(|sid| {
                            agent_client_protocol::schema::v1::SessionInfo::new(
                                sid.clone(),
                                std::env::temp_dir(),
                            )
                        })
                        .collect();
                    responder.respond(
                        agent_client_protocol::schema::v1::ListSessionsResponse::new(sessions),
                    )
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |req: agent_client_protocol::schema::v1::CloseSessionRequest,
                  responder: Responder<agent_client_protocol::schema::v1::CloseSessionResponse>,
                  _cx| {
                let mock = m_close.clone();
                async move {
                    let sid = req.session_id.0.to_string();
                    mock.log(json!({"event": "session_close", "sessionId": sid}));
                    // Closing ends the live session but keeps it loadable.
                    responder
                        .respond(agent_client_protocol::schema::v1::CloseSessionResponse::new())
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |req: agent_client_protocol::schema::v1::DeleteSessionRequest,
                  responder: Responder<
                agent_client_protocol::schema::v1::DeleteSessionResponse,
            >,
                  _cx| {
                let mock = m_delete.clone();
                async move {
                    let sid = req.session_id.0.to_string();
                    mock.log(json!({"event": "session_delete", "sessionId": sid}));
                    let removed = mock.state.lock().unwrap().sessions.remove(&sid).is_some();
                    if removed {
                        responder.respond(
                            agent_client_protocol::schema::v1::DeleteSessionResponse::new(),
                        )
                    } else {
                        responder
                            .respond_with_error(AcpError::invalid_params().data("unknown session"))
                    }
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |req: agent_client_protocol::schema::v1::ResumeSessionRequest,
                  responder: Responder<
                agent_client_protocol::schema::v1::ResumeSessionResponse,
            >,
                  _cx| {
                let mock = m_resume.clone();
                async move {
                    let sid = req.session_id.0.to_string();
                    mock.log(json!({"event": "session_resume", "sessionId": sid}));
                    let known = mock.state.lock().unwrap().sessions.contains_key(&sid);
                    if known {
                        responder.respond(
                            agent_client_protocol::schema::v1::ResumeSessionResponse::new(),
                        )
                    } else {
                        responder
                            .respond_with_error(AcpError::invalid_params().data("unknown session"))
                    }
                }
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |req: agent_client_protocol::schema::v1::SetSessionModeRequest,
                  responder: Responder<
                agent_client_protocol::schema::v1::SetSessionModeResponse,
            >,
                  _cx| {
                let mock = m_setmode.clone();
                async move {
                    let sid = req.session_id.0.to_string();
                    let mode = req.mode_id.0.to_string();
                    mock.log(json!({"event": "set_mode", "sessionId": sid, "modeId": mode}));
                    if !mock.session_modes.contains(&mode) {
                        return responder
                            .respond_with_error(AcpError::invalid_params().data("unknown mode"));
                    }
                    responder
                        .respond(agent_client_protocol::schema::v1::SetSessionModeResponse::new())
                }
            },
            on_receive_request!(),
        )
        .on_receive_notification(
            move |notif: CancelNotification, _cx| {
                let mock = m_cancel.clone();
                async move {
                    let sid = notif.session_id.0.to_string();
                    mock.log(json!({"event": "cancel", "sessionId": sid}));
                    if let Some(s) = mock.state.lock().unwrap().sessions.get_mut(&sid) {
                        s.cancelled = true;
                    }
                    Ok(())
                }
            },
            on_receive_notification!(),
        )
        .connect_to(router_acp::transport::stdio_lines())
        .await;

    if let Err(err) = result {
        if router_acp::transport::is_disconnect(&err) {
            return;
        }
        eprintln!("mock-agent exited with error: {err}");
        std::process::exit(1);
    }
}
