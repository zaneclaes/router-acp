//! Router-owned `delegate_task` MCP tool.
//!
//! ACP `McpServer` entries must be concrete transports, so the tool is
//! exposed as a real stdio helper process (`router-acp mcp-delegate --socket
//! ... --token ...`) that bridges its stdio to the parent router over a
//! Unix-domain socket. The per-session random token maps the MCP invocation
//! to the owning router session.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use agent_client_protocol::schema::v1::{
    CancelNotification, ContentBlock, Error as AcpError, McpServer, McpServerStdio, PromptRequest,
    ResourceLink, StopReason,
};
use agent_client_protocol::{
    ByteStreams, Responder, UntypedRole, on_receive_notification, on_receive_request,
};

use crate::candidate::{CandidateId, RequiredCaps, TaskClass};
use crate::classifier::{ClassifyInput, classify_heuristic};
use crate::config::StrategyKind;
use crate::session::{
    DelegateHandle, DownstreamRoute, Shared, close_downstream_session, open_downstream_session,
};
use crate::strategies::{RouteContext, make_strategy};

pub const DELEGATE_SERVER_NAME: &str = "router-delegate";
pub const DELEGATE_TOOL_NAME: &str = "delegate_task";
pub const DELEGATE_FOLLOWUP_TOOL_NAME: &str = "delegate_followup";
pub const DELEGATE_CLOSE_TOOL_NAME: &str = "delegate_close";
pub const DELEGATE_AWAIT_TOOL_NAME: &str = "delegate_await";

const TOOL_DESCRIPTION: &str = "Delegate a small, self-contained subtask to a lower-cost agent \
     running in its own ephemeral session. Delegate only subtasks that do not need this \
     session's full hidden context: simple UI tweaks, mechanical edits, isolated bug fixes, \
     and focused research. Do not delegate integration decisions or tasks requiring the \
     parent conversation's context. Returns the sub-agent's final answer as text — unless \
     `background: true`, which returns a `b-…` id immediately so independent subtasks run in \
     PARALLEL (clients execute tool calls serially, so plain calls serialize the subtasks); \
     collect background results with `delegate_await`.";

const AWAIT_TOOL_DESCRIPTION: &str = "Collect the results of background delegate_task jobs \
     (`background: true`). Waits up to `timeout_seconds` (default 600) for the given \
     `delegate_ids` (default: all of this session's pending jobs); returns every finished \
     job's output and lists the ones still running — call again until none remain. Finished \
     results are consumed: each is returned exactly once.";

// ----------------------------------------------------------------------
// MCP wire types (minimal, hand-typed over the SDK's JSON-RPC layer)
// ----------------------------------------------------------------------

/// Give a request/notification type a `Deserialize` that accepts `null`, a
/// missing value, or `{}` (falling back to `Default`). Real MCP clients
/// (claude-agent-acp, codex-acp) send `tools/list`, `ping`, and
/// `notifications/initialized` with `params: null` or no params at all; serde's
/// derived struct impl rejects `null` ("invalid type: null, expected struct"),
/// which made the adapter's `tools/list` error out and see NONE of the delegate
/// tools — so `delegate_task` never appeared. These handlers ignore their params
/// anyway, so leniently mapping null → default is exactly right.
macro_rules! lenient_params {
    ($t:ty) => {
        impl<'de> serde::Deserialize<'de> for $t {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                // IgnoredAny accepts any JSON value (including null); we discard
                // it and construct defaults.
                serde::de::IgnoredAny::deserialize(deserializer)?;
                Ok(<$t>::default())
            }
        }
    };
}

#[derive(Debug, Clone, Default, Serialize, agent_client_protocol::JsonRpcRequest)]
#[request(method = "initialize", response = McpInitializeResult)]
pub struct McpInitializeRequest {
    #[serde(default, rename = "protocolVersion")]
    pub protocol_version: Option<String>,
    #[serde(default)]
    pub capabilities: Value,
    #[serde(default, rename = "clientInfo")]
    pub client_info: Value,
}
lenient_params!(McpInitializeRequest);

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcResponse)]
#[serde(rename_all = "camelCase")]
pub struct McpInitializeResult {
    pub protocol_version: String,
    pub capabilities: Value,
    pub server_info: Value,
}

#[derive(Debug, Clone, Default, Serialize, agent_client_protocol::JsonRpcNotification)]
#[notification(method = "notifications/initialized")]
pub struct McpInitializedNotification {}
lenient_params!(McpInitializedNotification);

#[derive(Debug, Clone, Default, Serialize, agent_client_protocol::JsonRpcRequest)]
#[request(method = "ping", response = McpPingResult)]
pub struct McpPingRequest {}
lenient_params!(McpPingRequest);

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcResponse)]
pub struct McpPingResult {}

#[derive(Debug, Clone, Default, Serialize, agent_client_protocol::JsonRpcRequest)]
#[request(method = "tools/list", response = McpToolsListResult)]
pub struct McpToolsListRequest {
    #[serde(default)]
    pub cursor: Option<String>,
}
lenient_params!(McpToolsListRequest);

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcResponse)]
pub struct McpToolsListResult {
    pub tools: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcRequest)]
#[request(method = "tools/call", response = McpToolsCallResult)]
pub struct McpToolsCallRequest {
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcResponse)]
#[serde(rename_all = "camelCase")]
pub struct McpToolsCallResult {
    pub content: Vec<Value>,
    pub is_error: bool,
}

/// `delegate_task` tool input.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateTaskArgs {
    pub task: String,
    #[serde(default)]
    pub context_files: Vec<String>,
    #[serde(default)]
    pub hints: DelegateHints,
    /// Keep the sub-session open after this turn so the orchestrator can send
    /// follow-up instructions to the same sub-agent (context preserved) via
    /// `delegate_followup`. Returns a `delegate_id` to reference it.
    #[serde(default)]
    pub keep_open: bool,
    /// Run the subtask as a background job: the call returns a `b-…` id
    /// immediately and the result is collected later via `delegate_await`.
    /// This is how independent subtasks actually run in parallel — MCP
    /// clients execute tool calls one at a time, so foreground calls
    /// serialize. Composes with `keep_open` (the collected result carries the
    /// `delegate_id`).
    #[serde(default)]
    pub background: bool,
}

/// `delegate_await` tool input.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateAwaitArgs {
    /// Background job ids to wait for; empty means all of this session's
    /// pending jobs.
    #[serde(default)]
    pub delegate_ids: Vec<String>,
    /// How long to wait before returning a partial status (finished results
    /// so far + still-running list). Clamped to 5..=1500 seconds.
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

/// `delegate_followup` tool input.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateFollowupArgs {
    pub delegate_id: String,
    pub message: String,
}

/// `delegate_close` tool input.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateCloseArgs {
    pub delegate_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateHints {
    #[serde(default)]
    pub task_class: Option<String>,
    #[serde(default)]
    pub min_quality: Option<f64>,
    #[serde(default)]
    pub candidate: Option<String>,
}

fn tool_definition() -> Value {
    json!({
        "name": DELEGATE_TOOL_NAME,
        "description": TOOL_DESCRIPTION,
        "inputSchema": {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "Complete, self-contained instructions for the subtask."
                },
                "context_files": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "File paths the sub-agent should look at."
                },
                "hints": {
                    "type": "object",
                    "properties": {
                        "task_class": { "type": "string" },
                        "min_quality": { "type": "number" },
                        "candidate": {
                            "type": "string",
                            "description": "Preferred agent/model candidate id."
                        }
                    }
                },
                "keep_open": {
                    "type": "boolean",
                    "description": "Keep the sub-session open for follow-ups (returns a delegate_id)."
                },
                "background": {
                    "type": "boolean",
                    "description": "Return a b-… id immediately and run the subtask concurrently; \
                         collect the result with delegate_await. Use for every independent \
                         subtask so they run in parallel."
                }
            },
            "required": ["task"]
        }
    })
}

fn await_tool_definition() -> Value {
    json!({
        "name": DELEGATE_AWAIT_TOOL_NAME,
        "description": AWAIT_TOOL_DESCRIPTION,
        "inputSchema": {
            "type": "object",
            "properties": {
                "delegate_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Background job ids to wait for (default: all pending)."
                },
                "timeout_seconds": {
                    "type": "number",
                    "description": "Max seconds to wait before returning a partial status \
                         (default 600, clamped to 5..=1500)."
                }
            }
        }
    })
}

fn followup_tool_definition() -> Value {
    json!({
        "name": DELEGATE_FOLLOWUP_TOOL_NAME,
        "description": "Send a follow-up instruction to a sub-agent previously started with \
             delegate_task(keep_open=true), preserving that sub-session's context. Use for \
             review→fix→re-review loops. Returns the sub-agent's reply.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "delegate_id": {
                    "type": "string",
                    "description": "The delegate_id returned by delegate_task."
                },
                "message": {
                    "type": "string",
                    "description": "The follow-up instruction for the sub-agent."
                }
            },
            "required": ["delegate_id", "message"]
        }
    })
}

fn close_tool_definition() -> Value {
    json!({
        "name": DELEGATE_CLOSE_TOOL_NAME,
        "description": "Close a sub-session opened with delegate_task(keep_open=true) once you are \
             done sending it follow-ups. Frees the seat.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "delegate_id": {
                    "type": "string",
                    "description": "The delegate_id to close."
                }
            },
            "required": ["delegate_id"]
        }
    })
}

// ----------------------------------------------------------------------
// Injection decision
// ----------------------------------------------------------------------

/// Build the delegate MCP server entry for a session being pinned to
/// `candidate`, or `None` when delegation would be pointless (disabled, one
/// candidate, or no lower-cost candidate for this parent).
pub fn delegate_server_entry(
    shared: &Arc<Shared>,
    router_sid: &str,
    candidate: &CandidateId,
) -> Option<McpServer> {
    if !shared.cfg.delegation.enabled {
        return None;
    }
    let socket = shared.delegate_socket.get()?.clone();
    let routeable = shared.routeable_candidates();
    if routeable.len() <= 1 {
        return None;
    }
    let parent_cost = routeable.iter().find(|c| &c.id == candidate)?.cost_rank;
    // Ordinary sessions only get the tool when a strictly-cheaper candidate
    // exists (delegation sheds cost). Orchestrating sessions get it whenever
    // there is any other candidate, so the planner can delegate to same-/higher-
    // tier peers (e.g. a cross-lineage reviewer).
    let orchestrating = shared
        .with_session(router_sid, |s| s.orchestrating)
        .unwrap_or(false);
    if !orchestrating && !routeable.iter().any(|c| c.cost_rank < parent_cost) {
        return None;
    }

    let token = uuid::Uuid::new_v4().to_string();
    shared
        .delegate_tokens
        .lock()
        .unwrap()
        .insert(token.clone(), router_sid.to_string());
    shared.with_session(router_sid, |s| s.delegate_token = Some(token.clone()));

    let exe = std::env::var("ROUTER_ACP_HELPER_EXE")
        .map(PathBuf::from)
        .or_else(|_| std::env::current_exe())
        .ok()?;
    let stdio = McpServerStdio::new(DELEGATE_SERVER_NAME, exe).args(vec![
        "mcp-delegate".to_string(),
        "--socket".to_string(),
        socket.display().to_string(),
        "--token".to_string(),
        token,
    ]);
    Some(McpServer::Stdio(stdio))
}

/// Strip the router's own delegate server from a session's MCP server list
/// (delegated sessions never receive the delegate tool: depth is capped at 1).
pub fn strip_delegate_server(servers: &[McpServer]) -> Vec<McpServer> {
    servers
        .iter()
        .filter(|s| !matches!(s, McpServer::Stdio(stdio) if stdio.name == DELEGATE_SERVER_NAME))
        .cloned()
        .collect()
}

// ----------------------------------------------------------------------
// Socket listener and MCP serving
// ----------------------------------------------------------------------

fn default_socket_path() -> PathBuf {
    // Unique per router instance: multiple routers (or tests) can share a
    // process. Keep it short — macOS caps sun_path at ~104 bytes.
    let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
    std::env::temp_dir().join(format!("router-acp-{}-{suffix}.sock", std::process::id()))
}

/// Bind the delegate Unix socket and start accepting helper connections.
pub fn bind_listener(shared: &Arc<Shared>) -> Result<tokio::task::JoinHandle<()>, String> {
    let path = shared
        .cfg
        .delegation
        .socket_path
        .clone()
        .unwrap_or_else(default_socket_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("cannot create socket dir: {e}"))?;
    }
    let _ = std::fs::remove_file(&path);
    let listener =
        UnixListener::bind(&path).map_err(|e| format!("cannot bind {}: {e}", path.display()))?;
    shared
        .delegate_socket
        .set(path.clone())
        .map_err(|_| "delegate socket already bound".to_string())?;
    tracing::info!(socket = %path.display(), "delegate MCP socket bound");

    let shared = shared.clone();
    Ok(tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let shared = shared.clone();
                    tokio::spawn(async move {
                        if let Err(err) = serve_mcp_connection(shared, stream).await {
                            tracing::debug!(%err, "delegate MCP connection ended");
                        }
                    });
                }
                Err(err) => {
                    tracing::warn!(%err, "delegate socket accept failed");
                    break;
                }
            }
        }
    }))
}

async fn serve_mcp_connection(shared: Arc<Shared>, stream: UnixStream) -> Result<(), String> {
    let (read_half, write_half) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(read_half);

    // Handshake: the first line carries the per-session token.
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .map_err(|e| format!("handshake read failed: {e}"))?;
    let hello: Value =
        serde_json::from_str(line.trim()).map_err(|e| format!("bad handshake: {e}"))?;
    let token = hello
        .get("token")
        .and_then(|t| t.as_str())
        .ok_or("handshake missing token")?;
    let router_sid = shared
        .delegate_tokens
        .lock()
        .unwrap()
        .get(token)
        .cloned()
        .ok_or("unknown delegate token")?;
    tracing::debug!(session = router_sid, "delegate MCP helper connected");

    let transport = ByteStreams::new(write_half.compat_write(), reader.compat());

    let call_shared = shared.clone();
    let call_sid = router_sid.clone();
    UntypedRole
        .builder()
        .name("delegate-mcp")
        .on_receive_request(
            |_req: McpInitializeRequest, responder: Responder<McpInitializeResult>, _cx| async move {
                responder.respond(McpInitializeResult {
                    protocol_version: "2025-06-18".to_string(),
                    capabilities: json!({ "tools": {} }),
                    server_info: json!({
                        "name": "router-acp-delegate",
                        "version": env!("CARGO_PKG_VERSION"),
                    }),
                })
            },
            on_receive_request!(),
        )
        .on_receive_notification(
            |_notif: McpInitializedNotification, _cx| async move { Ok(()) },
            on_receive_notification!(),
        )
        .on_receive_request(
            |_req: McpPingRequest, responder: Responder<McpPingResult>, _cx| async move {
                responder.respond(McpPingResult {})
            },
            on_receive_request!(),
        )
        .on_receive_request(
            |_req: McpToolsListRequest, responder: Responder<McpToolsListResult>, _cx| async move {
                responder.respond(McpToolsListResult {
                    tools: vec![
                        tool_definition(),
                        await_tool_definition(),
                        followup_tool_definition(),
                        close_tool_definition(),
                    ],
                })
            },
            on_receive_request!(),
        )
        .on_receive_request(
            move |req: McpToolsCallRequest,
                  responder: Responder<McpToolsCallResult>,
                  cx: agent_client_protocol::ConnectionTo<UntypedRole>| {
                let shared = call_shared.clone();
                let router_sid = call_sid.clone();
                async move {
                    // Tool calls can take minutes; run them off the MCP
                    // dispatch loop so pings keep working.
                    match req.name.as_str() {
                        DELEGATE_TOOL_NAME => {
                            let args: DelegateTaskArgs = match serde_json::from_value(req.arguments) {
                                Ok(args) => args,
                                Err(err) => {
                                    return responder.respond(text_result(
                                        format!("invalid delegate_task arguments: {err}"),
                                        true,
                                    ));
                                }
                            };
                            if args.background {
                                // Start the job and ack immediately — the
                                // subtask runs on its own tokio task so
                                // serially-executed tool calls still yield
                                // parallel subtasks.
                                let result = start_background_delegate(&shared, &router_sid, args);
                                return responder.respond(match result {
                                    Ok(text) => text_result(text, false),
                                    Err(msg) => text_result(msg, true),
                                });
                            }
                            cx.spawn(async move {
                                let result = run_delegate_task(&shared, &router_sid, args).await;
                                let _ = responder.respond(match result {
                                    Ok(text) => text_result(text, false),
                                    Err(msg) => text_result(msg, true),
                                });
                                Ok(())
                            })
                        }
                        DELEGATE_AWAIT_TOOL_NAME => {
                            let args: DelegateAwaitArgs = match serde_json::from_value(req.arguments)
                            {
                                Ok(args) => args,
                                Err(err) => {
                                    return responder.respond(text_result(
                                        format!("invalid delegate_await arguments: {err}"),
                                        true,
                                    ));
                                }
                            };
                            cx.spawn(async move {
                                let result = run_delegate_await(&shared, &router_sid, args).await;
                                let _ = responder.respond(match result {
                                    Ok(text) => text_result(text, false),
                                    Err(msg) => text_result(msg, true),
                                });
                                Ok(())
                            })
                        }
                        DELEGATE_FOLLOWUP_TOOL_NAME => {
                            let args: DelegateFollowupArgs =
                                match serde_json::from_value(req.arguments) {
                                    Ok(args) => args,
                                    Err(err) => {
                                        return responder.respond(text_result(
                                            format!("invalid delegate_followup arguments: {err}"),
                                            true,
                                        ));
                                    }
                                };
                            cx.spawn(async move {
                                let result =
                                    run_delegate_followup(&shared, &router_sid, args).await;
                                let _ = responder.respond(match result {
                                    Ok(text) => text_result(text, false),
                                    Err(msg) => text_result(msg, true),
                                });
                                Ok(())
                            })
                        }
                        DELEGATE_CLOSE_TOOL_NAME => {
                            let args: DelegateCloseArgs = match serde_json::from_value(req.arguments)
                            {
                                Ok(args) => args,
                                Err(err) => {
                                    return responder.respond(text_result(
                                        format!("invalid delegate_close arguments: {err}"),
                                        true,
                                    ));
                                }
                            };
                            let result = run_delegate_close(&shared, &router_sid, args);
                            responder.respond(match result {
                                Ok(text) => text_result(text, false),
                                Err(msg) => text_result(msg, true),
                            })
                        }
                        other => responder.respond_with_error(
                            AcpError::invalid_params().data(format!("unknown tool `{other}`")),
                        ),
                    }
                }
            },
            on_receive_request!(),
        )
        .connect_to(transport)
        .await
        .map_err(|e| e.to_string())
}

fn text_result(text: String, is_error: bool) -> McpToolsCallResult {
    McpToolsCallResult {
        content: vec![json!({ "type": "text", "text": text })],
        is_error,
    }
}

// ----------------------------------------------------------------------
// Delegate orchestration
// ----------------------------------------------------------------------

/// Start a `background: true` delegate job: register it, spawn
/// `run_delegate_task` on its own tokio task, and return the `b-…` id
/// immediately. The `delegate_semaphore` inside `run_delegate_task` still
/// bounds how many jobs actually execute at once.
fn start_background_delegate(
    shared: &Arc<Shared>,
    router_sid: &str,
    args: DelegateTaskArgs,
) -> Result<String, String> {
    if shared.with_session(router_sid, |_| ()).is_none() {
        return Err("parent session no longer exists".to_string());
    }
    let job_id = format!("b-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let mut summary = args.task.replace('\n', " ");
    if summary.len() > 60 {
        summary.truncate(57);
        summary.push_str("...");
    }
    shared.background_delegates.lock().unwrap().insert(
        job_id.clone(),
        crate::session::BackgroundDelegate {
            parent_sid: router_sid.to_string(),
            summary: summary.clone(),
            started: std::time::Instant::now(),
            result: None,
        },
    );
    let task_shared = shared.clone();
    let task_sid = router_sid.to_string();
    let task_job_id = job_id.clone();
    tokio::spawn(async move {
        let result = run_delegate_task(&task_shared, &task_sid, args).await;
        // The parent may have closed while we ran (its jobs are dropped from
        // the registry) — only record a result somebody can still collect.
        let mut jobs = task_shared.background_delegates.lock().unwrap();
        if let Some(job) = jobs.get_mut(&task_job_id) {
            job.result = Some(result);
        }
        drop(jobs);
        task_shared.background_notify.notify_waiters();
    });
    Ok(format!(
        "[background delegate {job_id} started — \"{summary}\"]\n\
         The subtask is running concurrently. Collect its result with `delegate_await`; do NOT \
         assume or invent an outcome before collecting it."
    ))
}

/// Wait for background delegate jobs and return their results. Finished
/// results are consumed (returned exactly once); on timeout a partial status
/// is returned so the caller can keep polling without ever holding a tool
/// call open long enough to trip client idle timeouts.
pub async fn run_delegate_await(
    shared: &Arc<Shared>,
    router_sid: &str,
    args: DelegateAwaitArgs,
) -> Result<String, String> {
    // Validate explicit ids up front: they must exist and belong to us.
    {
        let jobs = shared.background_delegates.lock().unwrap();
        for id in &args.delegate_ids {
            match jobs.get(id) {
                Some(j) if j.parent_sid == router_sid => {}
                Some(_) => {
                    return Err(format!("delegate `{id}` does not belong to this session"));
                }
                None => {
                    return Err(format!(
                        "unknown background delegate `{id}` (already collected, or never started?)"
                    ));
                }
            }
        }
        if args.delegate_ids.is_empty() && !jobs.values().any(|j| j.parent_sid == router_sid) {
            return Err("no background delegates are pending for this session".to_string());
        }
    }
    let timeout =
        std::time::Duration::from_secs(args.timeout_seconds.unwrap_or(600).clamp(5, 1500));
    let deadline = tokio::time::Instant::now() + timeout;
    let mut collected: Vec<(String, Result<String, String>)> = Vec::new();
    loop {
        // Arm the wakeup BEFORE inspecting state so a completion between the
        // check and the await can't be missed.
        let notified = shared.background_notify.notified();
        tokio::pin!(notified);
        let mut running: Vec<(String, String, u64)> = Vec::new();
        {
            let mut jobs = shared.background_delegates.lock().unwrap();
            let targets: Vec<String> = jobs
                .iter()
                .filter(|(id, j)| {
                    j.parent_sid == router_sid
                        && (args.delegate_ids.is_empty() || args.delegate_ids.contains(id))
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in targets {
                let done = jobs.get(&id).is_some_and(|j| j.result.is_some());
                if done {
                    if let Some(job) = jobs.remove(&id) {
                        collected.push((id, job.result.expect("checked above")));
                    }
                } else if let Some(job) = jobs.get(&id) {
                    running.push((id, job.summary.clone(), job.started.elapsed().as_secs()));
                }
            }
        }
        if running.is_empty() {
            return Ok(render_await(&collected, &[]));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(render_await(&collected, &running));
        }
        tokio::select! {
            _ = &mut notified => {}
            _ = tokio::time::sleep_until(deadline) => {}
        }
    }
}

fn render_await(
    collected: &[(String, Result<String, String>)],
    running: &[(String, String, u64)],
) -> String {
    let mut out = String::new();
    for (id, result) in collected {
        match result {
            Ok(text) => out.push_str(&format!("=== delegate {id} — done ===\n{text}\n\n")),
            Err(msg) => out.push_str(&format!("=== delegate {id} — FAILED ===\n{msg}\n\n")),
        }
    }
    if running.is_empty() {
        if collected.is_empty() {
            out.push_str("No pending background delegates.");
        } else {
            out.push_str("All requested background delegates have completed.");
        }
    } else {
        let list = running
            .iter()
            .map(|(id, summary, secs)| format!("{id} (\"{summary}\", {secs}s elapsed)"))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "Still running: {list}. Call `delegate_await` again to collect them; do NOT assume \
             their outcomes."
        ));
    }
    out.trim_end().to_string()
}

/// Run one delegated subtask in an ephemeral downstream session on a
/// lower-cost eligible candidate. Returns the sub-agent's collected output.
pub async fn run_delegate_task(
    shared: &Arc<Shared>,
    router_sid: &str,
    args: DelegateTaskArgs,
) -> Result<String, String> {
    // Bounded concurrency across all delegated sessions.
    let _permit = shared
        .delegate_semaphore
        .acquire()
        .await
        .map_err(|_| "router shutting down".to_string())?;

    let (pin, cwd, dirs, client_mcp, strategy) = shared
        .with_session(router_sid, |s| {
            (
                s.pin.clone(),
                s.cwd.clone(),
                s.additional_directories.clone(),
                s.mcp_servers.clone(),
                s.strategy,
            )
        })
        .ok_or("parent session no longer exists")?;
    let pin = pin.ok_or("parent session is not pinned")?;
    let parent_cost = shared
        .candidate_runtime(&pin.candidate)
        .map(|c| c.cost_rank)
        .ok_or("parent candidate unknown")?;

    if shared
        .with_session(router_sid, |s| s.cancelled)
        .unwrap_or(false)
    {
        return Err("parent session was cancelled".to_string());
    }

    // Classify the subtask (heuristic only; hints are filters).
    let input = ClassifyInput {
        text: args.task.clone(),
        mentioned_paths: args.context_files.clone(),
        resource_count: args.context_files.len(),
        ..Default::default()
    };
    let mut profile = classify_heuristic(&shared.rules, &input);
    if let Some(class) = args.hints.task_class.as_deref().and_then(TaskClass::parse) {
        profile.class = class;
    }

    // Scope the pool. Ordinary delegation is cheaper-than-parent only (cost
    // shedding); an orchestrating session may delegate to any eligible peer,
    // including same-/higher-tier, so cross-lineage review is routeable.
    let orchestrating = shared
        .with_session(router_sid, |s| s.orchestrating)
        .unwrap_or(false);
    let mut pool = shared.eligible_views(&RequiredCaps::default(), profile.class);
    if !orchestrating {
        pool.retain(|v| v.cost_rank < parent_cost);
    }
    if let Some(min_quality) = args.hints.min_quality {
        pool.retain(|v| v.quality >= min_quality);
    }
    if let Some(hinted) = args.hints.candidate.as_deref().and_then(CandidateId::parse) {
        // Honor the hint only when it is a valid lower-cost candidate.
        if pool.iter().any(|v| v.id == hinted) {
            pool.retain(|v| v.id == hinted);
        }
    }
    if pool.is_empty() {
        return Err(
            "no lower-cost candidate is available for delegation; do the subtask yourself"
                .to_string(),
        );
    }

    // Rank with the session's strategy; `static` has no meaning over the
    // scoped pool, so fall back to `auto` semantics there.
    let strategy_kind = match strategy {
        StrategyKind::Static => StrategyKind::Auto,
        other => other,
    };
    let ctx = RouteContext {
        profile: profile.clone(),
        required_caps: RequiredCaps::default(),
        explicit_candidate: None,
    };
    let ranked = make_strategy(strategy_kind, &shared.cfg)
        .rank(&ctx, &pool)
        .map_err(|e| format!("delegate routing failed: {e}"))?;

    // Depth cap 1: the ephemeral session gets the client's MCP servers
    // without the router delegate entry.
    let sub_mcp = strip_delegate_server(&client_mcp);
    let capture = Arc::new(Mutex::new(String::new()));

    let mut last_err = None;
    for rc in ranked {
        let candidate = rc.candidate.clone();
        match open_downstream_session(
            shared,
            &candidate,
            cwd.clone(),
            dirs.clone(),
            sub_mcp.clone(),
            DownstreamRoute::Delegate {
                parent_router_sid: router_sid.to_string(),
                capture: capture.clone(),
            },
        )
        .await
        {
            Ok(opened) => {
                tracing::info!(
                    parent = router_sid,
                    candidate = %candidate,
                    class = profile.class.as_str(),
                    "delegated subtask routed"
                );
                // Tell the user which model got the subtask and why.
                let mut task_summary = args.task.replace('\n', " ");
                if task_summary.len() > 60 {
                    task_summary.truncate(57);
                    task_summary.push_str("...");
                }
                crate::session::notify_user(
                    shared,
                    router_sid,
                    format!(
                        "router-acp · delegate_task → {candidate} · task {} · {} · \"{}\"",
                        profile.class.as_str(),
                        rc.reason,
                        task_summary
                    ),
                );
                let handle = DelegateHandle {
                    process_key: opened.process_key.clone(),
                    downstream_sid: opened.downstream_sid.clone(),
                };
                shared.with_session(router_sid, |s| s.delegates.push(handle.clone()));

                // Record the sub-agent as its own state-DB row, linked to the
                // parent, so the delegation tree is observable. It shares the
                // parent's run_label for grouping.
                let sub_sid = format!("{router_sid}::delegate-{}", opened.downstream_sid);
                let parent_label = shared
                    .with_session(router_sid, |s| s.run_label.clone())
                    .flatten();
                shared.state.lock().unwrap().upsert(
                    sub_sid.clone(),
                    crate::state::PersistedSession {
                        agent: candidate.agent.clone(),
                        model: candidate.model.clone(),
                        downstream_session_id: opened.downstream_sid.clone(),
                        cwd: cwd.clone(),
                        additional_directories: dirs.clone(),
                        title: Some(task_summary.clone()),
                        routing: Some(serde_json::json!({
                            "strategy": "delegate",
                            "candidate": candidate.to_string(),
                            "class": profile.class.as_str(),
                            "reason": rc.reason,
                            "parent": router_sid,
                        })),
                        parent_session_id: Some(router_sid.to_string()),
                        kind: "delegate".to_string(),
                        run_label: parent_label,
                        ..Default::default()
                    },
                );
                shared.state.lock().unwrap().log(
                    &sub_sid,
                    &crate::state::LogEntry {
                        kind: "delegate_task".to_string(),
                        role: "user".to_string(),
                        summary: task_summary.clone(),
                        detail: Some(serde_json::json!({"context_files": args.context_files})),
                        tokens_input: crate::state::estimate_tokens(&args.task),
                        tokens_estimated: true,
                        ..Default::default()
                    },
                );

                // If the parent was cancelled while we were opening, cancel
                // immediately instead of running the subtask.
                if shared
                    .with_session(router_sid, |s| s.cancelled)
                    .unwrap_or(false)
                {
                    let _ = opened
                        .conn
                        .send_notification(CancelNotification::new(opened.downstream_sid.clone()));
                }

                let mut content: Vec<ContentBlock> = vec![ContentBlock::from(args.task.clone())];
                for file in &args.context_files {
                    let uri = if file.contains("://") {
                        file.clone()
                    } else {
                        format!("file://{file}")
                    };
                    let name = file.rsplit('/').next().unwrap_or(file).to_string();
                    content.push(ContentBlock::ResourceLink(ResourceLink::new(name, uri)));
                }
                let prompt = PromptRequest::new(opened.downstream_sid.clone(), content);
                {
                    let mut headroom = shared.headroom.lock().unwrap();
                    headroom.record_session(&candidate.agent);
                    headroom.record_prompt(&candidate.agent);
                }
                let turn_start = std::time::Instant::now();
                let result = opened.conn.send_request(prompt).block_task().await;
                shared
                    .state
                    .lock()
                    .unwrap()
                    .add_compute_ms(&sub_sid, turn_start.elapsed().as_millis() as u64);

                // Tear down (remove the handle, close the session) — used on
                // every path except a successful `keep_open` delegation.
                let teardown = || {
                    shared.with_session(router_sid, |s| {
                        s.delegates.retain(|d| {
                            d.downstream_sid != handle.downstream_sid
                                || d.process_key != handle.process_key
                        });
                    });
                    close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);
                };

                return match result {
                    Ok(resp) => {
                        let text = capture.lock().unwrap().clone();
                        let text = if text.trim().is_empty() {
                            "(delegate produced no text output)".to_string()
                        } else {
                            text
                        };
                        // Log the sub-agent's response with token usage.
                        let (ti, to, est) = crate::session::turn_tokens(&resp, &text);
                        shared.state.lock().unwrap().log(
                            &sub_sid,
                            &crate::state::LogEntry {
                                kind: "agent_response".to_string(),
                                role: "agent".to_string(),
                                summary: text.chars().take(200).collect(),
                                tokens_input: ti,
                                tokens_output: to,
                                tokens_estimated: est,
                                ..Default::default()
                            },
                        );
                        match resp.stop_reason {
                            StopReason::EndTurn | StopReason::MaxTurnRequests => {
                                if args.keep_open {
                                    // Keep the sub-session alive for follow-ups.
                                    // The handle stays in `s.delegates` so parent
                                    // cancel still propagates to it.
                                    let delegate_id = format!(
                                        "d-{}",
                                        &uuid::Uuid::new_v4().simple().to_string()[..8]
                                    );
                                    shared.live_delegates.lock().unwrap().insert(
                                        delegate_id.clone(),
                                        crate::session::LiveDelegate {
                                            parent_sid: router_sid.to_string(),
                                            process_key: opened.process_key.clone(),
                                            downstream_sid: opened.downstream_sid.clone(),
                                            candidate: candidate.clone(),
                                            capture: capture.clone(),
                                            sub_sid: sub_sid.clone(),
                                        },
                                    );
                                    Ok(format!(
                                        "[delegated to {candidate}] [delegate_id: {delegate_id} — \
                                         send more instructions to this same sub-agent with \
                                         `delegate_followup`, then `delegate_close` when done]\n\
                                         {text}"
                                    ))
                                } else {
                                    teardown();
                                    Ok(format!("[delegated to {candidate}]\n{text}"))
                                }
                            }
                            StopReason::Cancelled => {
                                teardown();
                                Err(format!("delegated subtask on {candidate} was cancelled"))
                            }
                            other => {
                                teardown();
                                Err(format!(
                                    "delegated subtask on {candidate} stopped early ({other:?}); \
                                     partial output:\n{text}"
                                ))
                            }
                        }
                    }
                    Err(err) => {
                        teardown();
                        Err(format!("delegated prompt on {candidate} failed: {err}"))
                    }
                };
            }
            Err(err) => {
                tracing::warn!(candidate = %candidate, error = %err, "delegate candidate failed");
                let class = crate::limits::classify_failure(&err);
                let human = crate::session::apply_failure(shared, &candidate, &err, &class);
                crate::session::notify_user(
                    shared,
                    router_sid,
                    format!(
                        "router-acp · delegate candidate {candidate} unavailable — {human}; \
                         trying next"
                    ),
                );
                last_err = Some(format!("{candidate}: {err}"));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| "no delegate candidate could open a session".to_string()))
}

/// Send a follow-up instruction to a delegate sub-session kept alive by an
/// earlier `delegate_task(keep_open=true)`, preserving that sub-agent's context.
pub async fn run_delegate_followup(
    shared: &Arc<Shared>,
    router_sid: &str,
    args: DelegateFollowupArgs,
) -> Result<String, String> {
    let _permit = shared
        .delegate_semaphore
        .acquire()
        .await
        .map_err(|_| "router shutting down".to_string())?;

    // Look up the live delegate and verify it belongs to this parent session.
    let (process_key, downstream_sid, candidate, capture, sub_sid) = {
        let live = shared.live_delegates.lock().unwrap();
        let d = live.get(&args.delegate_id).ok_or_else(|| {
            format!(
                "unknown delegate_id `{}` (already closed?)",
                args.delegate_id
            )
        })?;
        if d.parent_sid != router_sid {
            return Err("delegate_id does not belong to this session".to_string());
        }
        (
            d.process_key.clone(),
            d.downstream_sid.clone(),
            d.candidate.clone(),
            d.capture.clone(),
            d.sub_sid.clone(),
        )
    };

    if shared
        .with_session(router_sid, |s| s.cancelled)
        .unwrap_or(false)
    {
        return Err("parent session was cancelled".to_string());
    }
    let Some(conn) = shared.target_conn(&process_key) else {
        return Err(format!(
            "delegate sub-session on {candidate} is no longer reachable (process died)"
        ));
    };

    // Reset the capture buffer so we collect only this turn's output.
    capture.lock().unwrap().clear();

    // Log the follow-up.
    shared.state.lock().unwrap().log(
        &sub_sid,
        &crate::state::LogEntry {
            kind: "delegate_followup".to_string(),
            role: "user".to_string(),
            summary: args.message.chars().take(200).collect(),
            tokens_input: crate::state::estimate_tokens(&args.message),
            tokens_estimated: true,
            ..Default::default()
        },
    );
    shared
        .headroom
        .lock()
        .unwrap()
        .record_prompt(&candidate.agent);

    let prompt = PromptRequest::new(
        downstream_sid.clone(),
        vec![ContentBlock::from(args.message.clone())],
    );
    let turn_start = std::time::Instant::now();
    let result = conn.send_request(prompt).block_task().await;
    shared
        .state
        .lock()
        .unwrap()
        .add_compute_ms(&sub_sid, turn_start.elapsed().as_millis() as u64);
    match result {
        Ok(resp) => {
            let text = capture.lock().unwrap().clone();
            let text = if text.trim().is_empty() {
                "(delegate produced no text output)".to_string()
            } else {
                text
            };
            let (ti, to, est) = crate::session::turn_tokens(&resp, &text);
            shared.state.lock().unwrap().log(
                &sub_sid,
                &crate::state::LogEntry {
                    kind: "agent_response".to_string(),
                    role: "agent".to_string(),
                    summary: text.chars().take(200).collect(),
                    tokens_input: ti,
                    tokens_output: to,
                    tokens_estimated: est,
                    ..Default::default()
                },
            );
            match resp.stop_reason {
                StopReason::EndTurn | StopReason::MaxTurnRequests => Ok(format!(
                    "[{candidate}, delegate {}]\n{text}",
                    args.delegate_id
                )),
                StopReason::Cancelled => Err(format!("follow-up on {candidate} was cancelled")),
                other => Err(format!(
                    "follow-up on {candidate} stopped early ({other:?}); partial output:\n{text}"
                )),
            }
        }
        Err(err) => Err(format!("follow-up on {candidate} failed: {err}")),
    }
}

/// Close a delegate sub-session opened with `keep_open=true`.
pub fn run_delegate_close(
    shared: &Arc<Shared>,
    router_sid: &str,
    args: DelegateCloseArgs,
) -> Result<String, String> {
    let removed = {
        let mut live = shared.live_delegates.lock().unwrap();
        match live.get(&args.delegate_id) {
            Some(d) if d.parent_sid == router_sid => live.remove(&args.delegate_id),
            Some(_) => return Err("delegate_id does not belong to this session".to_string()),
            None => None,
        }
    };
    let Some(d) = removed else {
        return Err(format!(
            "unknown delegate_id `{}` (already closed?)",
            args.delegate_id
        ));
    };
    shared.with_session(router_sid, |s| {
        s.delegates
            .retain(|h| h.downstream_sid != d.downstream_sid || h.process_key != d.process_key);
    });
    close_downstream_session(shared, &d.process_key, &d.downstream_sid);
    Ok(format!(
        "closed delegate {} ({})",
        args.delegate_id, d.candidate
    ))
}

// ----------------------------------------------------------------------
// Helper subcommand: `router-acp mcp-delegate --socket ... --token ...`
// ----------------------------------------------------------------------

/// Bridge stdio to the router's delegate socket. Runs inside the helper
/// process spawned by the downstream agent as a stdio MCP server.
pub async fn run_helper(socket: &Path, token: &str) -> std::io::Result<()> {
    let mut stream = UnixStream::connect(socket).await?;
    let hello = format!("{}\n", json!({ "token": token }));
    stream.write_all(hello.as_bytes()).await?;
    stream.flush().await?;
    let (mut sock_read, mut sock_write) = stream.into_split();

    let stdin_to_sock = async move {
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 8192];
        loop {
            let n = stdin.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            sock_write.write_all(&buf[..n]).await?;
            sock_write.flush().await?;
        }
        Ok::<(), std::io::Error>(())
    };
    let sock_to_stdout = async move {
        let mut stdout = tokio::io::stdout();
        let mut buf = [0u8; 8192];
        loop {
            let n = sock_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            stdout.write_all(&buf[..n]).await?;
            stdout.flush().await?;
        }
        Ok::<(), std::io::Error>(())
    };

    tokio::select! {
        r = stdin_to_sock => r,
        r = sock_to_stdout => r,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_only_the_delegate_server() {
        let servers = vec![
            McpServer::Stdio(McpServerStdio::new("user-tool", "/bin/tool")),
            McpServer::Stdio(McpServerStdio::new(DELEGATE_SERVER_NAME, "/bin/router-acp")),
        ];
        let stripped = strip_delegate_server(&servers);
        assert_eq!(stripped.len(), 1);
        assert!(matches!(&stripped[0], McpServer::Stdio(s) if s.name == "user-tool"));
    }

    #[test]
    fn tool_definition_shape() {
        let def = tool_definition();
        assert_eq!(def["name"], DELEGATE_TOOL_NAME);
        assert_eq!(def["inputSchema"]["required"][0], "task");
        assert!(def["inputSchema"]["properties"]["hints"]["properties"]["candidate"].is_object());
        assert!(def["inputSchema"]["properties"]["background"].is_object());
    }

    #[test]
    fn await_tool_definition_shape() {
        let def = await_tool_definition();
        assert_eq!(def["name"], DELEGATE_AWAIT_TOOL_NAME);
        assert!(def["inputSchema"]["properties"]["delegate_ids"].is_object());
        assert!(def["inputSchema"]["properties"]["timeout_seconds"].is_object());
    }

    #[test]
    fn delegate_args_background_defaults_off() {
        use serde_json::json;
        let args: DelegateTaskArgs = serde_json::from_value(json!({"task": "do a thing"})).unwrap();
        assert!(!args.background);
        let args: DelegateTaskArgs =
            serde_json::from_value(json!({"task": "do a thing", "background": true})).unwrap();
        assert!(args.background);
        let await_args: DelegateAwaitArgs = serde_json::from_value(json!({})).unwrap();
        assert!(await_args.delegate_ids.is_empty());
        assert!(await_args.timeout_seconds.is_none());
    }

    #[test]
    fn render_await_reports_done_failed_and_running() {
        let collected = vec![
            ("b-1".to_string(), Ok("all good".to_string())),
            ("b-2".to_string(), Err("it broke".to_string())),
        ];
        let running = vec![("b-3".to_string(), "slow task".to_string(), 42u64)];
        let text = render_await(&collected, &running);
        assert!(
            text.contains("=== delegate b-1 — done ===\nall good"),
            "{text}"
        );
        assert!(
            text.contains("=== delegate b-2 — FAILED ===\nit broke"),
            "{text}"
        );
        assert!(
            text.contains("Still running: b-3 (\"slow task\", 42s elapsed)"),
            "{text}"
        );
        let done = render_await(&collected, &[]);
        assert!(
            done.contains("All requested background delegates have completed"),
            "{done}"
        );
    }

    #[test]
    fn mcp_request_params_accept_null_and_missing() {
        // Real MCP clients (claude-agent-acp, codex-acp) send tools/list, ping,
        // and notifications/initialized with `params: null` or no params. These
        // must deserialize, or the adapter's tools/list errors and it sees NONE
        // of the delegate tools (the bug that made delegation never work live).
        use serde_json::{Value, json};
        serde_json::from_value::<McpToolsListRequest>(Value::Null).unwrap();
        serde_json::from_value::<McpPingRequest>(Value::Null).unwrap();
        serde_json::from_value::<McpInitializedNotification>(Value::Null).unwrap();
        serde_json::from_value::<McpInitializeRequest>(Value::Null).unwrap();
        // Also from {} and from a populated object (fields ignored).
        serde_json::from_value::<McpToolsListRequest>(json!({})).unwrap();
        serde_json::from_value::<McpToolsListRequest>(json!({"cursor": "abc"})).unwrap();
        serde_json::from_value::<McpInitializeRequest>(json!({"protocolVersion": "1"})).unwrap();
    }
}
