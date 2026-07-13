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

const TOOL_DESCRIPTION: &str = "Delegate a small, self-contained subtask to a lower-cost agent \
     running in its own ephemeral session. Delegate only subtasks that do not need this \
     session's full hidden context: simple UI tweaks, mechanical edits, isolated bug fixes, \
     and focused research. Do not delegate integration decisions or tasks requiring the \
     parent conversation's context. Returns the sub-agent's final answer as text.";

// ----------------------------------------------------------------------
// MCP wire types (minimal, hand-typed over the SDK's JSON-RPC layer)
// ----------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcRequest)]
#[request(method = "initialize", response = McpInitializeResult)]
pub struct McpInitializeRequest {
    #[serde(default, rename = "protocolVersion")]
    pub protocol_version: Option<String>,
    #[serde(default)]
    pub capabilities: Value,
    #[serde(default, rename = "clientInfo")]
    pub client_info: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcResponse)]
#[serde(rename_all = "camelCase")]
pub struct McpInitializeResult {
    pub protocol_version: String,
    pub capabilities: Value,
    pub server_info: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcNotification)]
#[notification(method = "notifications/initialized")]
pub struct McpInitializedNotification {}

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcRequest)]
#[request(method = "ping", response = McpPingResult)]
pub struct McpPingRequest {}

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcResponse)]
pub struct McpPingResult {}

#[derive(Debug, Clone, Serialize, Deserialize, agent_client_protocol::JsonRpcRequest)]
#[request(method = "tools/list", response = McpToolsListResult)]
pub struct McpToolsListRequest {
    #[serde(default)]
    pub cursor: Option<String>,
}

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
                }
            },
            "required": ["task"]
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
    if !routeable.iter().any(|c| c.cost_rank < parent_cost) {
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
                    tools: vec![tool_definition()],
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
                    if req.name != DELEGATE_TOOL_NAME {
                        return responder.respond_with_error(
                            AcpError::invalid_params().data(format!("unknown tool `{}`", req.name)),
                        );
                    }
                    let args: DelegateTaskArgs = match serde_json::from_value(req.arguments) {
                        Ok(args) => args,
                        Err(err) => {
                            return responder.respond(text_result(
                                format!("invalid delegate_task arguments: {err}"),
                                true,
                            ));
                        }
                    };
                    // Tool calls can take minutes; run them off the MCP
                    // dispatch loop so pings keep working.
                    cx.spawn(async move {
                        let result = run_delegate_task(&shared, &router_sid, args).await;
                        let response = match result {
                            Ok(text) => text_result(text, false),
                            Err(msg) => text_result(msg, true),
                        };
                        let _ = responder.respond(response);
                        Ok(())
                    })
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

    // Scope the pool to candidates strictly cheaper than the parent.
    let mut pool = shared.eligible_views(&RequiredCaps::default(), profile.class);
    pool.retain(|v| v.cost_rank < parent_cost);
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
                let result = opened.conn.send_request(prompt).block_task().await;

                // Tear down: remove the handle, close the ephemeral session.
                shared.with_session(router_sid, |s| {
                    s.delegates.retain(|d| {
                        d.downstream_sid != handle.downstream_sid
                            || d.process_key != handle.process_key
                    });
                });
                close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);

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
                                Ok(format!("[delegated to {candidate}]\n{text}"))
                            }
                            StopReason::Cancelled => {
                                Err(format!("delegated subtask on {candidate} was cancelled"))
                            }
                            other => Err(format!(
                                "delegated subtask on {candidate} stopped early ({other:?}); \
                                 partial output:\n{text}"
                            )),
                        }
                    }
                    Err(err) => Err(format!("delegated prompt on {candidate} failed: {err}")),
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
    }
}
