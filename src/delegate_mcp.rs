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
    CancelNotification, ContentBlock, CreateTerminalRequest, Error as AcpError, McpServer,
    McpServerStdio, PromptRequest, ResourceLink, SetSessionModeRequest, StopReason,
};
use agent_client_protocol::{
    ByteStreams, Responder, UntypedRole, on_receive_notification, on_receive_request,
};

use crate::candidate::EffortLevel;
use crate::candidate::{CandidateId, RequiredCaps, TaskClass};
use crate::classifier::{ClassifyInput, classify_heuristic};
use crate::config::{CandidateHintMode, StrategyKind};
use crate::delegate_hook::DelegateEvent;
use crate::session::{
    DelegateHandle, DownstreamRoute, Shared, close_downstream_session, open_downstream_session,
    resolve_mode_id,
};
use crate::strategies::{CandidateView, RouteContext, make_strategy};

pub const DELEGATE_SERVER_NAME: &str = "router-delegate";
pub const DELEGATE_TOOL_NAME: &str = "delegate_task";
pub const DELEGATE_FOLLOWUP_TOOL_NAME: &str = "delegate_followup";
pub const DELEGATE_CLOSE_TOOL_NAME: &str = "delegate_close";
pub const DELEGATE_AWAIT_TOOL_NAME: &str = "delegate_await";
pub const BACKGROUND_START_TOOL_NAME: &str = "background_start";
pub const DELEGATE_RESULT_TOOL_NAME: &str = "delegate_result";
/// MCP server given to host-directed delegates (a lifecycle hook is set).
pub const WORKER_SERVER_NAME: &str = "router-worker";
pub const WORKER_WHOAMI_TOOL_NAME: &str = "worker_whoami";
pub const WORKER_HANDOFF_TOOL_NAME: &str = "worker_handoff";

/// Per-session binding for the stdio helper handshake.
///
/// `delegation_enabled` is frozen when the MCP server is *injected* into
/// downstream `session/new`, not when the helper later serves `tools/list`.
/// Adapters (notably Codex) list MCP tools during `session/new`, which
/// completes *before* the router commits `session.pin`. Reading the pin at
/// list time therefore advertised only `background_start` (the server is
/// still attached because the client has terminals) and omitted every
/// `delegate_*` tool — even though cheaper workers existed and the router
/// had already queued the delegation directive.
#[derive(Clone, Debug)]
pub struct DelegateBinding {
    pub router_sid: String,
    pub delegation_enabled: bool,
    /// Set for a `router-worker` connection: the delegate it serves.
    pub worker: Option<WorkerBinding>,
}

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

const BACKGROUND_START_TOOL_DESCRIPTION: &str = "Start a long-running shell, watcher, server, \
     or monitor in the ACP client's managed background-terminal lifecycle. Use this instead of \
     provider-native run_in_background/background shell modes: the client can keep it alive \
     across relay restarts and lets the user inspect its output or cancel it independently. \
     Returns immediately after the process starts. By default router-acp keeps the current prompt \
     open and resumes the agent when the process exits. Persistent servers must set wake_on_exit \
     false.";

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
    /// Optional durable planner assignment. Omitted keeps legacy lightweight
    /// delegation semantics and its shared read-only context.
    #[serde(default)]
    pub work_id: Option<String>,
    #[serde(skip)]
    pub planner_identity: Option<crate::planner_workflow::WorkerIdentity>,
    #[serde(skip)]
    pub planner_role: Option<crate::planner_skills::PlannerRole>,
    #[serde(skip)]
    pub planner_workspace: Option<PathBuf>,
    #[serde(skip)]
    pub planner_environment: std::collections::BTreeMap<String, String>,
    #[serde(skip)]
    pub classification_text: Option<String>,
    #[serde(default)]
    pub context_files: Vec<String>,
    #[serde(default)]
    pub input_ids: Vec<String>,
    #[serde(skip)]
    pub planner_input_blocks: Vec<ContentBlock>,
    #[serde(skip)]
    pub planner_receipt_ids: Vec<String>,
    #[serde(default)]
    pub hints: DelegateHints,
    /// Opaque capabilities required by this bounded subtask. The router maps
    /// these through the host-configured catalog policy; agents never name
    /// endpoints, credentials, or catalog identities.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// Keep the sub-session open after this turn so the parent can send
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
    /// Router-assigned worker id: the `b-…` job id of a background delegate.
    /// Not part of the tool input; foreground delegates get a fresh `w-…` id.
    #[serde(skip)]
    pub worker_id: Option<String>,
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
    #[serde(default)]
    pub input_ids: Vec<String>,
    #[serde(skip)]
    pub planner_input_blocks: Vec<ContentBlock>,
    #[serde(skip)]
    pub planner_receipt_ids: Vec<String>,
    #[serde(skip)]
    pub planner_identity: Option<crate::planner_workflow::WorkerIdentity>,
    #[serde(skip)]
    pub planner_role: Option<crate::planner_skills::PlannerRole>,
    /// Return a `b-…` id immediately and run the follow-up turn concurrently;
    /// collect it with `delegate_await`.
    #[serde(default)]
    pub background: bool,
}

/// `delegate_result` tool input.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateResultArgs {
    pub delegate_id: String,
}

/// `worker_handoff` tool input.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkerHandoffArgs {
    pub kind: String,
    #[serde(default)]
    pub message: String,
}

/// `delegate_close` tool input.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateCloseArgs {
    pub delegate_id: String,
}

/// `background_start` tool input. Environment inherits from the ACP client;
/// callers that need overrides can invoke `/usr/bin/env` explicitly.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BackgroundStartArgs {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub output_byte_limit: Option<u64>,
    #[serde(default = "default_true")]
    pub wake_on_exit: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateHints {
    #[serde(default)]
    pub task_class: Option<String>,
    #[serde(default)]
    pub min_quality: Option<f64>,
    #[serde(default)]
    pub candidate: Option<String>,
    /// Reasoning effort for this delegate (`low` … `max`), applied by the LLM
    /// proxy in place of the parent session's effort.
    #[serde(default)]
    pub effort: Option<String>,
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
                "work_id": { "type": "string", "description": "Admitted durable planner work identity. Allocates an isolated workspace and preserves the child identity across attempts. Omit for lightweight delegation." },
                "input_ids": {"type":"array","items":{"type":"string"},"description":"Original durable planner input receipts to deliver, including attachments."},
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
                        },
                        "effort": {
                            "type": "string",
                            "enum": ["low", "medium", "high", "xhigh", "max"],
                            "description": "Reasoning effort for this sub-agent (applied by the \
                                 router's LLM proxy; otherwise it inherits this session's effort)."
                        }
                    }
                },
                "required_capabilities": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Host-defined capabilities needed by this bounded subtask. The router attaches matching registered MCP catalogs only to this delegate."
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
                "input_ids": {"type":"array","items":{"type":"string"},"description":"Original durable planner input receipts for this work owner."},
                "message": {
                    "type": "string",
                    "description": "The follow-up instruction for the sub-agent."
                },
                "background": {
                    "type": "boolean",
                    "description": "Return a b-… id immediately and run the follow-up turn \
                         concurrently; collect it with delegate_await. Use it to answer one \
                         long-running sub-agent while supervising others."
                }
            },
            "required": ["delegate_id", "message"]
        }
    })
}

fn result_tool_definition() -> Value {
    json!({
        "name": DELEGATE_RESULT_TOOL_NAME,
        "description": "Re-read a sub-agent's latest output and status by its b-… job id, \
             w-… worker id or d-… delegate id — including results delegate_await already \
             returned, e.g. after this conversation was compacted or resumed.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "delegate_id": {
                    "type": "string",
                    "description": "A b-…, w-… or d-… id from an earlier delegate_task."
                }
            },
            "required": ["delegate_id"]
        }
    })
}

/// Tools of the `router-worker` server a host-directed delegate receives.
fn worker_tools() -> Vec<Value> {
    vec![
        json!({
            "name": WORKER_WHOAMI_TOOL_NAME,
            "description": "Your worker id, model and parent session, as your host registered \
                 them. Use the worker id wherever your instructions ask for your own id.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": WORKER_HANDOFF_TOOL_NAME,
            "description": "Record how you are handing this turn back to your parent, then end \
                 the turn. Your host checks the handoff when the turn ends and may send you \
                 back with what is still missing.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": crate::delegate_hook::HANDOFF_KINDS,
                        "description": "commit_paths_ready, waiting, lease_requested, \
                             ownership_requested, staging_gate_request, round_verified, \
                             round_failed or reassignment_required."
                    },
                    "message": {
                        "type": "string",
                        "description": "The evidence, request or wait your parent needs: paths \
                             and SHAs, the lease or gate you need, or what you are waiting on \
                             and when you will check it next."
                    }
                },
                "required": ["kind", "message"]
            }
        }),
    ]
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

fn listed_tools(delegation_enabled: bool, terminal_enabled: bool) -> Vec<Value> {
    let mut tools = Vec::new();
    if delegation_enabled {
        tools.extend([
            tool_definition(),
            await_tool_definition(),
            followup_tool_definition(),
            close_tool_definition(),
            result_tool_definition(),
        ]);
        tools.push(crate::planner_workflow::tool_definition());
    }
    if terminal_enabled {
        tools.push(background_start_tool_definition());
    }
    tools
}

fn background_start_tool_definition() -> Value {
    json!({
        "name": BACKGROUND_START_TOOL_NAME,
        "description": BACKGROUND_START_TOOL_DESCRIPTION,
        "inputSchema": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "Executable to launch (not a shell command string)."
                },
                "args": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Arguments passed directly to the executable."
                },
                "cwd": {
                    "type": "string",
                    "description": "Optional absolute working directory; defaults to the session checkout."
                },
                "output_byte_limit": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Optional retained-output byte limit."
                },
                "wake_on_exit": {
                    "type": "boolean",
                    "default": true,
                    "description": "Resume the agent with exit status and output. Set false only for a persistent server."
                }
            },
            "required": ["command"]
        }
    })
}

// ----------------------------------------------------------------------
// Injection decision
// ----------------------------------------------------------------------

/// Whether a session pinned to `candidate` can use router delegation.
pub fn delegation_available(shared: &Arc<Shared>, candidate: &CandidateId) -> bool {
    if !shared.cfg.delegation.enabled {
        return false;
    }
    let routeable = shared.routeable_candidates();
    if routeable.len() <= 1 {
        return false;
    }
    let Some(parent_cost) = routeable
        .iter()
        .find(|c| &c.id == candidate)
        .map(|c| c.cost_rank)
    else {
        return false;
    };
    // Sessions get the tool when a strictly-cheaper candidate exists
    // (delegation sheds cost), or with `candidate_hints: exact`, where the
    // parent can name any other model.
    //
    // Only auto-eligible candidates count as delegation targets — the
    // delegate pool is built from `eligible_views`, so an explicit-only
    // candidate cannot serve a subtask and must not be what advertises the tool.
    let delegatable: Vec<_> = routeable
        .iter()
        .filter(|c| c.auto_eligible && c.id != *candidate)
        .collect();
    if delegatable.is_empty() {
        return false;
    }
    let exact_hints = shared.cfg.delegation.candidate_hints == CandidateHintMode::Exact;
    exact_hints || delegatable.iter().any(|c| c.cost_rank < parent_cost)
}

/// Build the router-owned MCP server entry for a pinned session. The server is
/// useful when either delegation is available or the upstream ACP client owns
/// managed terminals; `tools/list` exposes only the applicable tools.
pub fn delegate_server_entry(
    shared: &Arc<Shared>,
    router_sid: &str,
    candidate: &CandidateId,
) -> Option<McpServer> {
    let planner_session = shared
        .with_session(router_sid, |s| s.strategy == StrategyKind::Planner)
        .unwrap_or(false);
    if !delegation_available(shared, candidate)
        && !planner_session
        && !shared.upstream_client_capabilities().terminal
    {
        return None;
    }
    let socket = shared.delegate_socket.get()?.clone();
    let exe = std::env::var("ROUTER_ACP_HELPER_EXE")
        .map(PathBuf::from)
        .or_else(|_| std::env::current_exe())
        .ok()?;

    let token = uuid::Uuid::new_v4().to_string();
    // Freeze the decision against the candidate being pinned, not against
    // `session.pin` — that pin is committed only *after* `session/new`
    // returns, and tools/list runs inside that call.
    let delegation_enabled = delegation_available(shared, candidate)
        || (planner_session && shared.cfg.delegation.enabled);
    drop_parent_tokens(shared, router_sid);
    shared.delegate_tokens.lock().unwrap().insert(
        token.clone(),
        DelegateBinding {
            router_sid: router_sid.to_string(),
            delegation_enabled,
            worker: None,
        },
    );
    shared.with_session(router_sid, |s| s.delegate_token = Some(token.clone()));

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
    let binding = shared
        .delegate_tokens
        .lock()
        .unwrap()
        .get(token)
        .cloned()
        .ok_or("unknown delegate token")?;
    let router_sid = binding.router_sid;
    let delegation_enabled = binding.delegation_enabled;
    // A `router-worker` connection serves only the worker's own tools.
    let worker = binding.worker;
    let worker_connection = worker.is_some();
    tracing::debug!(session = router_sid, "delegate MCP helper connected");

    let transport = ByteStreams::new(write_half.compat_write(), reader.compat());

    let call_shared = shared.clone();
    let call_sid = router_sid.clone();
    let call_worker = worker.clone();
    let planner_worker = worker.as_ref().is_some_and(|w| w.planner.is_some());
    let terminal_enabled = shared.upstream_client_capabilities().terminal;
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
            move |_req: McpToolsListRequest,
                  responder: Responder<McpToolsListResult>,
                  _cx| async move {
                responder.respond(McpToolsListResult {
                    tools: if worker_connection {
                        let mut tools = worker_tools();
                        if planner_worker { tools.push(crate::planner_workflow::tool_definition()); }
                        tools
                    } else {
                        listed_tools(delegation_enabled, terminal_enabled)
                    },
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
                let worker = call_worker.clone();
                async move {
                    if req.name == crate::planner_workflow::TOOL_NAME {
                        if worker.as_ref().is_some_and(|w| w.planner.is_none()) {
                            return responder.respond(text_result("lightweight worker has no planner assignment".into(), true));
                        }
                        let operation = match serde_json::from_value::<crate::planner_workflow::Operation>(req.arguments) {
                            Ok(operation) => operation,
                            Err(err) => return responder.respond(text_result(format!("invalid planner_workflow arguments: {err}"), true)),
                        };
                        let actor = worker.as_ref().and_then(|w| w.planner.clone());
                        return cx.spawn(async move {
                            let result = crate::planner_workflow::operate(&shared, &router_sid, actor.as_ref(), operation).await;
                            let _ = responder.respond(match result { Ok(text) => text_result(text, false), Err(err) => text_result(err, true) });
                            Ok(())
                        });
                    }
                    // A worker's connection serves only its own tools.
                    if let Some(worker) = &worker {
                        return responder.respond(match req.name.as_str() {
                            WORKER_WHOAMI_TOOL_NAME => text_result(worker_whoami(worker), false),
                            WORKER_HANDOFF_TOOL_NAME => {
                                match serde_json::from_value::<WorkerHandoffArgs>(req.arguments) {
                                    Ok(args) => match run_worker_handoff(&shared, worker, args) {
                                        Ok(text) => text_result(text, false),
                                        Err(msg) => text_result(msg, true),
                                    },
                                    Err(err) => text_result(
                                        format!("invalid worker_handoff arguments: {err}"),
                                        true,
                                    ),
                                }
                            }
                            other => text_result(format!("unknown tool `{other}`"), true),
                        });
                    }
                    // Tool calls can take minutes; run them off the MCP
                    // dispatch loop so pings keep working.
                    match req.name.as_str() {
                        DELEGATE_RESULT_TOOL_NAME => {
                            let result = serde_json::from_value::<DelegateResultArgs>(req.arguments)
                                .map_err(|err| format!("invalid delegate_result arguments: {err}"))
                                .and_then(|args| run_delegate_result(&shared, &router_sid, args));
                            responder.respond(match result {
                                Ok(text) => text_result(text, false),
                                Err(msg) => text_result(msg, true),
                            })
                        }
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
                            if args.background {
                                let result = start_background_followup(&shared, &router_sid, args);
                                return responder.respond(match result {
                                    Ok(text) => text_result(text, false),
                                    Err(msg) => text_result(msg, true),
                                });
                            }
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
                        BACKGROUND_START_TOOL_NAME => {
                            let args: BackgroundStartArgs =
                                match serde_json::from_value(req.arguments) {
                                    Ok(args) => args,
                                    Err(err) => {
                                        return responder.respond(text_result(
                                            format!("invalid background_start arguments: {err}"),
                                            true,
                                        ));
                                    }
                                };
                            cx.spawn(async move {
                                let result = run_background_start(&shared, &router_sid, args).await;
                                let _ = responder.respond(match result {
                                    Ok(text) => text_result(text, false),
                                    Err(msg) => text_result(msg, true),
                                });
                                Ok(())
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

async fn run_background_start(
    shared: &Arc<Shared>,
    router_sid: &str,
    args: BackgroundStartArgs,
) -> Result<String, String> {
    if args.command.trim().is_empty() {
        return Err("command must not be empty".to_string());
    }
    if !shared.upstream_client_capabilities().terminal {
        return Err("the connected ACP client does not support managed terminals".to_string());
    }
    if shared.with_session(router_sid, |_| ()).is_none() {
        return Err("parent session no longer exists".to_string());
    }
    let upstream = shared
        .upstream()
        .ok_or_else(|| "ACP client connection is unavailable".to_string())?;
    let mut request = CreateTerminalRequest::new(router_sid.to_string(), args.command)
        .args(args.args)
        .meta(
            json!({"router_acp": {"managed_background": true}})
                .as_object()
                .cloned(),
        );
    if let Some(cwd) = args.cwd {
        request = request.cwd(cwd);
    }
    if let Some(limit) = args.output_byte_limit {
        request = request.output_byte_limit(limit);
    }
    let response = upstream
        .send_request(request)
        .block_task()
        .await
        .map_err(|err| format!("ACP terminal/create failed: {err}"))?;
    if args.wake_on_exit {
        shared
            .managed_backgrounds
            .lock()
            .unwrap()
            .entry(router_sid.to_string())
            .or_default()
            .push(response.terminal_id.0.to_string());
    }
    Ok(format!(
        "Background terminal {} started. It continues independently; the user can inspect or cancel it from the client.",
        response.terminal_id
    ))
}

// ----------------------------------------------------------------------
// Delegate execution
// ----------------------------------------------------------------------

/// Start a `background: true` delegate job: register it, spawn
/// `run_delegate_task` on its own tokio task, and return the `b-…` id
/// immediately. The `delegate_semaphore` inside `run_delegate_task` still
/// bounds how many jobs actually execute at once.
fn start_background_delegate(
    shared: &Arc<Shared>,
    router_sid: &str,
    mut args: DelegateTaskArgs,
) -> Result<String, String> {
    let job_id = format!("b-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    args.worker_id = Some(job_id.clone());
    let summary = args.task.clone();
    let task_shared = shared.clone();
    let task_sid = router_sid.to_string();
    start_background_job(shared, router_sid, job_id, &summary, async move {
        run_delegate_task(&task_shared, &task_sid, args).await
    })
}

/// `delegate_followup` with `background: true`: the follow-up turn runs as a
/// background job collected by `delegate_await`.
fn start_background_followup(
    shared: &Arc<Shared>,
    router_sid: &str,
    args: DelegateFollowupArgs,
) -> Result<String, String> {
    let job_id = format!("b-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let summary = format!("follow-up to {}: {}", args.delegate_id, args.message);
    let task_shared = shared.clone();
    let task_sid = router_sid.to_string();
    start_background_job(shared, router_sid, job_id, &summary, async move {
        run_delegate_followup(&task_shared, &task_sid, args).await
    })
}

/// Register a background job, run `job` on its own tokio task, and return
/// its `b-…` id immediately. The semaphores inside the job still bound how
/// many jobs execute at once.
fn start_background_job(
    shared: &Arc<Shared>,
    router_sid: &str,
    job_id: String,
    summary: &str,
    job: impl std::future::Future<Output = Result<String, String>> + Send + 'static,
) -> Result<String, String> {
    if shared.with_session(router_sid, |_| ()).is_none() {
        return Err("parent session no longer exists".to_string());
    }
    let mut summary = summary.replace('\n', " ");
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
    let task_job_id = job_id.clone();
    tokio::spawn(async move {
        let result = job.await;
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

/// `delegation.candidate_hints: exact`: the pool is exactly the named
/// candidate, or the call fails with the reason it cannot run there.
fn exact_hint_pool(
    eligible: Vec<CandidateView>,
    raw: &str,
    hinted: Option<&CandidateId>,
) -> Result<Vec<CandidateView>, String> {
    let Some(hinted) = hinted else {
        return Err(format!(
            "hints.candidate `{raw}` is not an agent/model candidate id"
        ));
    };
    let pool: Vec<CandidateView> = eligible.into_iter().filter(|v| &v.id == hinted).collect();
    if pool.is_empty() {
        return Err(format!(
            "hinted delegate candidate `{hinted}` is not available (unknown, not offered by its \
             adapter, signed out, cordoned or quarantined); not substituting another model"
        ));
    }
    Ok(pool)
}

/// Parse delegate hints. Omitted effort uses the bounded assignment baseline.
fn parse_effort_hint(raw: Option<&str>) -> Result<Option<EffortLevel>, String> {
    match raw.map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(None),
        Some(text) => match EffortLevel::parse(text) {
            Some(EffortLevel::Auto) => Ok(None),
            Some(level) => Ok(Some(level)),
            None => Err(format!(
                "hints.effort `{text}` is not one of low, medium, high, xhigh, max"
            )),
        },
    }
}

/// Scope a delegate candidate pool by cost: strictly cheaper than the parent
/// (cost shedding; an empty result is the caller's "do the subtask yourself"
/// error), same agent first. Reaching a same- or higher-tier model, or
/// another agent, takes `delegation.candidate_hints: exact`.
fn scope_delegate_pool(
    pool: Vec<CandidateView>,
    parent_cost: u32,
    parent_agent: &str,
) -> Vec<CandidateView> {
    let cheaper: Vec<CandidateView> = pool
        .iter()
        .filter(|v| v.cost_rank < parent_cost)
        .cloned()
        .collect();
    // A main-session model switch changes the natural worker family too:
    // Sol delegates to cheaper Codex siblings (Terra/Luna/etc.), not a stale
    // Claude candidate. Fall back globally only for agents such as Grok that
    // have no cheaper sibling at all.
    let same_agent: Vec<CandidateView> = cheaper
        .iter()
        .filter(|view| view.id.agent == parent_agent)
        .cloned()
        .collect();
    if same_agent.is_empty() {
        cheaper
    } else {
        same_agent
    }
}

/// Synthesize a delegate turn's cost from configured `pricing` when the
/// adapter reported none of its own. Delegate sub-sessions have no in-memory
/// `RouterSession` to carry a saw-adapter-cost flag, so the guard is the
/// state row itself: an adapter that reports cost (claude) streams
/// `usage_update.cost` during the turn, so the row is non-zero by the time
/// the response lands; a zero row means no adapter cost exists to mix with.
fn synth_delegate_cost(
    shared: &Arc<Shared>,
    sub_sid: &str,
    candidate: &CandidateId,
    usage: &crate::session::TurnUsage,
) {
    let Some(delta) = crate::session::synth_turn_cost(&shared.cfg, candidate, usage) else {
        return;
    };
    if delta <= 0.0 {
        return;
    }
    let st = shared.state.lock().unwrap();
    let reported = st.get(sub_sid).map(|p| p.cost_usd).unwrap_or(0.0);
    if reported == 0.0 {
        st.add_estimated_cost(sub_sid, delta);
    }
}

/// Run one delegated subtask in an ephemeral downstream session on a
/// lower-cost eligible candidate. Returns the sub-agent's collected output.
pub async fn run_delegate_task(
    shared: &Arc<Shared>,
    router_sid: &str,
    mut args: DelegateTaskArgs,
) -> Result<String, String> {
    if let Some(work_id) = args.work_id.clone() {
        args.planner_role = Some(
            if crate::planner_workflow::load(shared, router_sid)?
                .and_then(|run| run.works.get(&work_id).map(|w| w.status))
                == Some(crate::planner_workflow::WorkStatus::Accepted)
            {
                crate::planner_skills::PlannerRole::FinishWork
            } else {
                crate::planner_skills::PlannerRole::ImplementWork
            },
        );
        let (identity, workspace, child_id, instructions) =
            crate::planner_workflow::begin_work(shared, router_sid, &work_id).await?;
        args.classification_text = Some(args.task.clone());
        args.task = format!("{instructions}\n[Assignment briefing]\n{}", args.task);
        args.planner_identity = Some(identity.clone());
        args.planner_workspace = Some(workspace.path);
        args.planner_environment = workspace.environment;
        args.worker_id = Some(child_id);
        args.keep_open = true;
        let original_inputs =
            crate::planner_workflow::input_blocks(shared, router_sid, &identity, &args.input_ids);
        let result = match original_inputs {
            Ok((blocks, ids)) => {
                args.planner_input_blocks = blocks;
                args.planner_receipt_ids = ids;
                run_delegate_task_inner(shared, router_sid, args).await
            }
            Err(error) => Err(error),
        };
        let delegate_id = shared
            .live_delegates
            .lock()
            .unwrap()
            .iter()
            .find(|(_, live)| {
                live.planner.as_ref().is_some_and(|p| {
                    p.work_id == identity.work_id && p.attempt_id == identity.attempt_id
                })
            })
            .map(|(id, _)| id.clone());
        crate::planner_workflow::attempt_ended(
            shared,
            router_sid,
            &identity,
            delegate_id,
            result.clone().unwrap_or_else(|e| e),
            result.is_err(),
        )?;
        crate::planner_client::turn_state(shared, router_sid, &identity, "idle")?;
        return result;
    }
    run_delegate_task_inner(shared, router_sid, args).await
}

async fn run_delegate_task_inner(
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
    crate::auth::refresh_before_selection(shared).await;

    let (pin, mut cwd, mut dirs, client_mcp, delegate_mcp_catalogs, strategy) = shared
        .with_session(router_sid, |s| {
            (
                s.pin.clone(),
                s.cwd.clone(),
                s.additional_directories.clone(),
                s.mcp_servers.clone(),
                s.delegate_mcp_catalogs.clone(),
                s.strategy,
            )
        })
        .ok_or("parent session no longer exists")?;
    let pin = pin.ok_or("parent session is not pinned")?;
    if let Some(workspace) = &args.planner_workspace {
        cwd = workspace.clone();
        dirs.clear();
    }
    if !args.required_capabilities.is_empty() && shared.cfg.delegation.mcp_catalogs.is_empty() {
        return Err("delegate MCP catalogs are disabled by router configuration".to_string());
    }
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
        text: args
            .classification_text
            .clone()
            .unwrap_or_else(|| args.task.clone()),
        mentioned_paths: args.context_files.clone(),
        resource_count: args.context_files.len(),
        ..Default::default()
    };
    let mut profile = classify_heuristic(&shared.rules, &input);
    if let Some(class) = args.hints.task_class.as_deref().and_then(TaskClass::parse) {
        profile.class = class;
    }
    // The parent (often a frontier planner) has already decomposed the work
    // into a fully-specified brief, and the classifier reads long, detailed
    // briefs as maximum complexity — which zeroes the cost term and trips the
    // p75 quality gate, routing every subtask to the most expensive
    // candidate. Cap it so spec'd subtasks route on cost again.
    profile.complexity = profile
        .complexity
        .min(shared.cfg.delegation.complexity_cap.clamp(0.0, 1.0));

    // Scope the pool by cost (see `scope_delegate_pool`).
    // The hint is a STATED reference (a parent model names a candidate by the
    // id it knows), so resolve it through the version-pin map — otherwise a
    // hint naming the stable default id matches nothing in the pool and is
    // silently dropped.
    let hinted = args.hints.candidate.as_deref().and_then(CandidateId::parse);
    let hinted_effort = parse_effort_hint(args.hints.effort.as_deref())?;
    // A model-generated hint is an automatic recommendation, never a human
    // override. Resolve each bounded assignment from its own scope.
    let baseline_effort = profile.effort.unwrap_or(EffortLevel::Medium);
    let effort = crate::session::session_effort(
        &shared.cfg,
        None,
        Some(
            hinted_effort
                .map(|hint| hint.min(baseline_effort))
                .unwrap_or(baseline_effort),
        ),
    );
    let exact = shared.cfg.delegation.candidate_hints == CandidateHintMode::Exact;
    let mut pool = match (&args.hints.candidate, &hinted) {
        // `exact`: the named model or an error — never a substitute, whatever
        // its tier or agent relative to the parent.
        (Some(raw), _) if exact => exact_hint_pool(
            shared.eligible_views(&RequiredCaps::default(), profile.class),
            raw,
            hinted.as_ref(),
        )?,
        _ if args.planner_identity.is_some() => {
            shared.eligible_views(&RequiredCaps::default(), profile.class)
        }
        _ => scope_delegate_pool(
            shared.eligible_views(&RequiredCaps::default(), profile.class),
            parent_cost,
            &pin.candidate.agent,
        ),
    };
    if args.planner_identity.is_some() {
        let run =
            crate::planner_workflow::load(shared, router_sid)?.ok_or("planner state missing")?;
        let role = args
            .planner_role
            .unwrap_or(crate::planner_skills::PlannerRole::ImplementWork);
        let skill = &run.policy.roles[&role];
        if let Some(route) = crate::session::detect_skill_route(
            &shared.cfg,
            &[ContentBlock::from(format!("/{}", skill.name))],
        ) {
            pool.retain(|candidate| {
                route
                    .candidates
                    .iter()
                    .any(|pattern| crate::session::candidate_matches(pattern, &candidate.id))
            });
        }
    }
    if let Some(min_quality) = args.hints.min_quality {
        pool.retain(|v| v.quality >= min_quality);
    }
    if let Some(hinted) = &hinted {
        // Honor the hint only when it survives the cost scoping.
        if pool.iter().any(|v| &v.id == hinted) {
            pool.retain(|v| &v.id == hinted);
        }
    }
    if pool.is_empty() && exact && hinted.is_some() {
        return Err(format!(
            "hinted delegate candidate `{}` is below hints.min_quality",
            args.hints.candidate.as_deref().unwrap_or_default()
        ));
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
        explicit_source: None,
        planner_phase: args
            .work_id
            .as_ref()
            .map(|_| crate::config::PlannerPhase::Implementation),
        planner_difficulty: None,
    };
    let ranked = make_strategy(strategy_kind, &shared.cfg)
        .rank(&ctx, &pool)
        .map_err(|e| format!("delegate routing failed: {e}"))?;

    // Depth cap 1: the ephemeral session gets the client's MCP servers
    // without the router delegate entry, plus only explicitly requested
    // host-registered bundles. The host—not the model—owns all definitions
    // and credentials in the catalog.
    let mut sub_mcp = strip_delegate_server(&client_mcp);
    let catalog_names = crate::session::resolve_mcp_catalogs(
        &shared.cfg,
        &args.required_capabilities,
        &delegate_mcp_catalogs,
    )?;
    for name in &catalog_names {
        let Some(servers) = delegate_mcp_catalogs.get(name) else {
            return Err(format!(
                "delegate MCP catalog `{name}` is not available in this session"
            ));
        };
        sub_mcp.extend(servers.iter().cloned());
    }
    let capture = Arc::new(Mutex::new(String::new()));
    // One worker id per delegation, whichever candidate ends up running it:
    // the background job id when there is one, so the parent and the host
    // name the worker the same way.
    let worker_id = args
        .worker_id
        .clone()
        .unwrap_or_else(|| format!("w-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]));
    let lifecycle_hook = shared.cfg.delegation.lifecycle_hook.clone();

    let mut last_err = None;
    let ranked_len = ranked.len();
    for (index, rc) in ranked.into_iter().enumerate() {
        if let Some(identity) = &args.planner_identity {
            let run = crate::planner_workflow::load(shared, router_sid)?
                .ok_or("planner state missing")?;
            if run.status != crate::planner_workflow::RunStatus::Running
                || run.works[&identity.work_id].paused
            {
                return Err("planner assignment is suspended; refusing automatic dispatch".into());
            }
        }
        let candidate = rc.candidate.clone();
        // `agents[].max_delegates`: skip a full agent while another ranked
        // candidate remains, otherwise wait for its next free slot. The permit
        // is held for as long as this turn runs.
        let _agent_slot = match shared.agent_delegate_slots.get(&candidate.agent) {
            None => None,
            Some(slots) => match slots.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) if index + 1 < ranked_len => {
                    last_err = Some(format!("{candidate}: every delegate slot is busy"));
                    continue;
                }
                Err(_) => Some(
                    slots
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| "router shutting down".to_string())?,
                ),
            },
        };
        let request_generation = crate::auth::request_access_generation(shared, &candidate);
        // Host-directed workers get the router's worker tools (identity and
        // structured handoffs), bound to this worker id.
        let mut session_mcp = sub_mcp.clone();
        if (lifecycle_hook.is_some() || args.planner_identity.is_some())
            && let Some(entry) = worker_server_entry(
                shared,
                router_sid,
                WorkerBinding {
                    worker_id: worker_id.clone(),
                    candidate: candidate.to_string(),
                    parent_downstream_sid: pin.downstream_sid.clone(),
                    planner: args.planner_identity.clone(),
                },
            )
        {
            session_mcp.push(entry);
        }
        let opening = if let Some(identity) = &args.planner_identity {
            crate::planner_client::open_child(
                shared,
                router_sid,
                identity,
                &candidate,
                cwd.clone(),
                dirs.clone(),
                session_mcp,
                capture.clone(),
            )
            .await
        } else {
            open_downstream_session(
                shared,
                &candidate,
                cwd.clone(),
                dirs.clone(),
                session_mcp,
                DownstreamRoute::Delegate {
                    parent_router_sid: router_sid.to_string(),
                    capture: capture.clone(),
                },
            )
            .await
        };
        match opening {
            Ok(opened) => {
                let mut resolution = effort.map(|level| {
                    shared
                        .scores
                        .lookup_exact(&candidate)
                        .resolve_automatic_effort(level)
                });
                if let Some(resolution) = &mut resolution
                    && let Err(error) = crate::session::apply_native_effort(
                        shared,
                        &candidate,
                        &opened.conn,
                        &opened.downstream_sid,
                        &opened.config_options,
                        resolution,
                        false,
                    )
                    .await
                {
                    close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);
                    drop_worker_tokens(shared, &worker_id);
                    last_err = Some(error.to_string());
                    continue;
                }
                let effective_effort = resolution.as_ref().and_then(|r| r.resolved);
                // Delegates have no upstream client to send the set_mode that
                // primary sessions receive. Apply the same configured `auto`
                // mapping explicitly so Claude/Codex delegates inherit the
                // parent's dangerous, non-interactive permission behavior.
                //
                // Agents that advertise *no* session modes (Grok today — empty
                // `available_modes`) have no permission gate to arm; requiring
                // `auto` there hard-fails every delegate onto that seat and
                // strands the parent when Claude/Codex are usage-cordoned.
                // Fail closed only when the agent *does* advertise modes but
                // none resolve to a non-interactive auto equivalent.
                let available_modes: Vec<String> = opened
                    .modes
                    .as_ref()
                    .map(|m| {
                        m.available_modes
                            .iter()
                            .map(|mode| mode.id.0.to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                if available_modes.is_empty() {
                    tracing::info!(
                        parent = router_sid,
                        candidate = %candidate,
                        "delegate candidate advertises no session modes; proceeding without set_mode"
                    );
                } else {
                    match resolve_mode_id(shared, &candidate.agent, "auto", &available_modes) {
                        Some(mode_id) => {
                            let set = SetSessionModeRequest::new(
                                opened.downstream_sid.clone(),
                                mode_id.clone(),
                            );
                            if let Err(err) = opened.conn.send_request(set).block_task().await {
                                tracing::warn!(
                                    parent = router_sid,
                                    candidate = %candidate,
                                    %err,
                                    "delegate session mode rejected; trying the next candidate"
                                );
                                last_err = Some(format!(
                                    "delegate {candidate} rejected required auto mode: {err}"
                                ));
                                close_downstream_session(
                                    shared,
                                    &opened.process_key,
                                    &opened.downstream_sid,
                                );
                                drop_worker_tokens(shared, &worker_id);
                                continue;
                            } else {
                                tracing::info!(
                                    parent = router_sid,
                                    candidate = %candidate,
                                    applied = mode_id,
                                    "delegate session mode applied"
                                );
                            }
                        }
                        None => {
                            tracing::warn!(
                                parent = router_sid,
                                candidate = %candidate,
                                ?available_modes,
                                "delegate candidate has no required auto mode; trying the next candidate"
                            );
                            last_err = Some(format!(
                                "delegate {candidate} has no configured auto mode among {available_modes:?}"
                            ));
                            close_downstream_session(
                                shared,
                                &opened.process_key,
                                &opened.downstream_sid,
                            );
                            drop_worker_tokens(shared, &worker_id);
                            continue;
                        }
                    }
                }
                let sub_sid = format!("{router_sid}::delegate-{}", opened.downstream_sid);
                let mut task_summary = args.task.replace('\n', " ");
                if task_summary.len() > 60 {
                    task_summary.truncate(57);
                    task_summary.push_str("...");
                }
                // A host that accounts for its workers registers this one
                // before it runs; a refusal fails the delegation outright
                // rather than trying another model.
                let (router_pid, router_started_at_ms) = crate::delegate_hook::process_identity();
                let lifecycle = lifecycle_hook.as_ref().map(|_| DelegateEvent {
                    event: "delegate_start",
                    worker_id: worker_id.clone(),
                    parent_router_session_id: router_sid.to_string(),
                    parent_downstream_session_id: pin.downstream_sid.clone(),
                    parent_candidate: pin.candidate.to_string(),
                    candidate: candidate.to_string(),
                    lineage: crate::session::agent_lineage(
                        &shared.runtime_config(),
                        &candidate.agent,
                    ),
                    downstream_session_id: opened.downstream_sid.clone(),
                    state_session_id: sub_sid.clone(),
                    cwd: cwd.display().to_string(),
                    effort: effective_effort.map(|level| level.as_str().to_string()),
                    background: args.background,
                    keep_open: args.keep_open,
                    task_summary: task_summary.clone(),
                    router_pid,
                    router_started_at_ms,
                    turn: None,
                    last_message: None,
                    handoff: None,
                    outcome: None,
                    detail: None,
                });
                if let (Some(hook), Some(event)) = (&lifecycle_hook, &lifecycle)
                    && let Err(err) = crate::delegate_hook::run(hook, event).await
                {
                    tracing::warn!(
                        parent = router_sid,
                        candidate = %candidate,
                        worker = %event.worker_id,
                        %err,
                        "delegate start hook refused the delegate"
                    );
                    close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);
                    drop_worker_tokens(shared, &worker_id);
                    // A start that timed out may still have registered the
                    // worker on the host's side; tell it the worker never ran.
                    crate::delegate_hook::deliver(
                        shared,
                        &event.stopped("aborted", Some(err.clone())),
                    );
                    return Err(format!(
                        "delegation.lifecycle_hook refused delegate {} on {candidate}: {err}",
                        event.worker_id
                    ));
                }
                if let Some(level) = effective_effort {
                    shared
                        .delegate_effort
                        .lock()
                        .unwrap()
                        .insert(sub_sid.clone(), level);
                }
                tracing::info!(
                    parent = router_sid,
                    candidate = %candidate,
                    class = profile.class.as_str(),
                    "delegated subtask routed"
                );
                // Tell the user which model got the subtask and why.
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
                            "worker_id": worker_id,
                            "background_id": args.worker_id,
                            "effort": resolution.as_ref().map(|r| json!({"requested":r.requested.as_str(),"resolved":r.resolved.map(EffortLevel::as_str),"provider_value":r.provider_value,"confirmed":r.confirmed})),
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
                        detail: Some(serde_json::json!({
                            "task": args.task,
                            "context_files": args.context_files,
                            "required_capabilities": args.required_capabilities,
                            "work_id": args.work_id,
                            "attempt_id": args.planner_identity.as_ref().map(|p| &p.attempt_id),
                            "effort": {"requested": hinted_effort.map(EffortLevel::as_str), "effective": effort.map(EffortLevel::as_str), "source":"assigned-scope", "reason":"bounded task baseline; model hint cannot exceed automatic policy"},
                        })),
                        tokens_input: crate::state::estimate_tokens(&args.task),
                        tokens_estimated: true,
                        ..Default::default()
                    },
                );

                // If the parent was cancelled while we were opening, cancel
                // immediately instead of running the subtask.
                if shared
                    .with_session(router_sid, |s| s.cancelled)
                    .unwrap_or(true)
                    || args.planner_identity.as_ref().is_some_and(|identity| {
                        crate::planner_workflow::load(shared, router_sid)
                            .ok()
                            .flatten()
                            .is_none_or(|run| {
                                run.status != crate::planner_workflow::RunStatus::Running
                                    || run.works[&identity.work_id].paused
                            })
                    })
                {
                    let _ = opened
                        .conn
                        .send_notification(CancelNotification::new(opened.downstream_sid.clone()));
                    close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);
                    shared.delegate_effort.lock().unwrap().remove(&sub_sid);
                    drop_worker_tokens(shared, &worker_id);
                    if let Some(event) = &lifecycle {
                        crate::delegate_hook::deliver(shared, &event.stopped("cancelled", None));
                    }
                    return Err("planner parent or child stopped during session opening".into());
                }

                let mut content: Vec<ContentBlock> = Vec::new();
                if let Some(event) = &lifecycle {
                    // The worker names itself with the id its host registered.
                    content.push(ContentBlock::from(crate::delegate_hook::identity_line(
                        event,
                    )));
                }
                content.push(ContentBlock::from(args.task.clone()));
                content.extend(args.planner_input_blocks.clone());
                for file in &args.context_files {
                    let uri = if file.contains("://") {
                        file.clone()
                    } else {
                        format!("file://{file}")
                    };
                    let name = file.rsplit('/').next().unwrap_or(file).to_string();
                    content.push(ContentBlock::ResourceLink(ResourceLink::new(name, uri)));
                }
                let mut prompt = PromptRequest::new(opened.downstream_sid.clone(), content);
                if let Some(identity) = &args.planner_identity {
                    prompt = prompt.meta(serde_json::from_value(serde_json::json!({"router_acp":{"planner_role":args.planner_role,"work_id":identity.work_id,"attempt_id":identity.attempt_id,"agent_origin":true}})).ok());
                }
                {
                    let mut headroom = shared.headroom.lock().unwrap();
                    headroom.record_session(&candidate.agent);
                    headroom.record_prompt(&candidate.agent);
                }
                let turn_start = std::time::Instant::now();
                if let Some(identity) = &args.planner_identity {
                    crate::planner_client::turn_state(shared, router_sid, identity, "started")?;
                    crate::planner_workflow::queue_delivery(
                        shared,
                        &sub_sid,
                        router_sid,
                        &identity.work_id,
                        args.planner_receipt_ids.clone(),
                    );
                }
                let _llm_turn = shared.llm_proxy.begin_turn(
                    opened.process_key.clone(),
                    router_sid.to_string(),
                    sub_sid.clone(),
                    opened.downstream_sid.clone(),
                    candidate.clone(),
                    profile.class,
                    None,
                );
                let prompt_generation = crate::auth::request_access_generation(shared, &candidate);
                let result = opened.conn.send_request(prompt).block_task().await;
                if result.is_ok() {
                    crate::planner_workflow::confirm_delivery(shared, &sub_sid)?;
                }
                shared.planner_deliveries.lock().unwrap().remove(&sub_sid);
                // The host may send the worker back to finish before the turn
                // returns to the parent (`delegate_turn_end`).
                let mut turns = 1;
                let result = match result {
                    Ok(resp) => {
                        gate_turns(
                            shared,
                            &TurnGate {
                                lifecycle: lifecycle.as_ref(),
                                conn: &opened.conn,
                                downstream_sid: &opened.downstream_sid,
                                sub_sid: &sub_sid,
                                worker_id: &worker_id,
                                capture: &capture,
                            },
                            resp,
                            &mut turns,
                        )
                        .await
                    }
                    Err(err) => Err(err),
                };
                let result = result.and_then(|resp| {
                    if resp.stop_reason != StopReason::Cancelled
                        && crate::auth::response_is_auth_error(&capture.lock().unwrap())
                    {
                        Err(AcpError::auth_required()
                            .data("Provider reported an authentication error"))
                    } else {
                        Ok(resp)
                    }
                });
                if result
                    .as_ref()
                    .is_err_and(crate::downstream::is_auth_required)
                {
                    crate::auth::note_auth_failure_for_request(
                        shared,
                        &candidate.agent,
                        "Authentication unavailable",
                        prompt_generation.as_deref(),
                    )
                    .await;
                }
                shared
                    .state
                    .lock()
                    .unwrap()
                    .add_compute_ms(&sub_sid, turn_start.elapsed().as_millis() as u64);
                let worker = lifecycle
                    .as_ref()
                    .map(|event| format!(", worker {}", event.worker_id))
                    .unwrap_or_default();

                // Tear down (remove the handle, close the session, report the
                // stop) — used on every path except a successful `keep_open`
                // delegation, whose stop is reported when it is closed.
                let teardown = |outcome: &str, detail: Option<String>| {
                    shared.with_session(router_sid, |s| {
                        s.delegates.retain(|d| {
                            d.downstream_sid != handle.downstream_sid
                                || d.process_key != handle.process_key
                        });
                    });
                    close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);
                    shared.delegate_effort.lock().unwrap().remove(&sub_sid);
                    shared.worker_handoffs.lock().unwrap().remove(&worker_id);
                    drop_worker_tokens(shared, &worker_id);
                    if let Some(event) = &lifecycle {
                        crate::delegate_hook::deliver(shared, &event.stopped(outcome, detail));
                    }
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
                        let tu = crate::session::turn_tokens(&resp, &text);
                        shared.state.lock().unwrap().log(
                            &sub_sid,
                            &crate::state::LogEntry {
                                ts: None,
                                kind: "agent_response".to_string(),
                                role: "agent".to_string(),
                                summary: text.chars().take(200).collect(),
                                detail: Some(serde_json::json!({"text": text})),
                                tokens_input: tu.input,
                                tokens_output: tu.output,
                                tokens_cache_read: tu.cache_read,
                                tokens_cache_write: tu.cache_write,
                                tokens_estimated: tu.estimated,
                                model: Some(candidate.to_string()),
                            },
                        );
                        synth_delegate_cost(shared, &sub_sid, &candidate, &tu);
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
                                            lifecycle: lifecycle.clone(),
                                            worker_id: worker_id.clone(),
                                            planner: args.planner_identity.clone(),
                                            turns,
                                        },
                                    );
                                    Ok(format!(
                                        "[delegated to {candidate}{worker}] [delegate_id: \
                                         {delegate_id} — send more instructions to this same \
                                         sub-agent with `delegate_followup`, then \
                                         `delegate_close` when done]\n{text}"
                                    ))
                                } else {
                                    teardown("completed", None);
                                    Ok(format!("[delegated to {candidate}{worker}]\n{text}"))
                                }
                            }
                            StopReason::Cancelled => {
                                teardown("cancelled", None);
                                Err(format!("delegated subtask on {candidate} was cancelled"))
                            }
                            other => {
                                teardown("failed", Some(format!("stopped early ({other:?})")));
                                Err(format!(
                                    "delegated subtask on {candidate} stopped early ({other:?}); \
                                     partial output:\n{text}"
                                ))
                            }
                        }
                    }
                    Err(err) => {
                        teardown("failed", Some(err.to_string()));
                        Err(format!("delegated prompt on {candidate} failed: {err}"))
                    }
                };
            }
            Err(err) => {
                drop_worker_tokens(shared, &worker_id);
                tracing::warn!(candidate = %candidate, error = %err, "delegate candidate failed");
                if crate::downstream::is_auth_required(&err) {
                    crate::auth::note_auth_failure_for_request(
                        shared,
                        &candidate.agent,
                        format!("{} is not signed in", candidate.agent),
                        request_generation.as_deref(),
                    )
                    .await;
                }
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

/// What `gate_turns` needs to continue one delegate session.
struct TurnGate<'a> {
    lifecycle: Option<&'a DelegateEvent>,
    conn: &'a agent_client_protocol::ConnectionTo<agent_client_protocol::Agent>,
    downstream_sid: &'a str,
    sub_sid: &'a str,
    worker_id: &'a str,
    capture: &'a Arc<Mutex<String>>,
}

/// Ask the host whether a delegate turn may end (`delegate_turn_end`). While
/// it answers `Continue`, send its message back to the same sub-session —
/// at most `max_continuations` times — the way a provider's subagent-stop
/// hook keeps a worker going. Without a lifecycle hook, or once the turn did
/// not end normally, the response is returned unchanged.
async fn gate_turns(
    shared: &Arc<Shared>,
    gate: &TurnGate<'_>,
    mut resp: agent_client_protocol::schema::v1::PromptResponse,
    turns: &mut u32,
) -> Result<agent_client_protocol::schema::v1::PromptResponse, AcpError> {
    let (Some(hook), Some(event)) = (&shared.cfg.delegation.lifecycle_hook, gate.lifecycle) else {
        return Ok(resp);
    };
    let mut continued = 0;
    loop {
        if !matches!(
            resp.stop_reason,
            StopReason::EndTurn | StopReason::MaxTurnRequests
        ) {
            return Ok(resp);
        }
        let text = gate.capture.lock().unwrap().clone();
        let handoff = shared
            .worker_handoffs
            .lock()
            .unwrap()
            .get(gate.worker_id)
            .cloned();
        let message =
            match crate::delegate_hook::turn_end(hook, &event.turn_ended(*turns, &text, handoff))
                .await
            {
                crate::delegate_hook::TurnVerdict::Release => return Ok(resp),
                crate::delegate_hook::TurnVerdict::Continue(message) => message,
            };
        if continued >= hook.max_continuations {
            gate.capture.lock().unwrap().push_str(&format!(
                "\n\n[router-acp · the host still returned this turn after {continued} \
                 continuations: {message}]"
            ));
            return Ok(resp);
        }
        continued += 1;
        *turns += 1;
        shared
            .worker_handoffs
            .lock()
            .unwrap()
            .remove(gate.worker_id);
        gate.capture.lock().unwrap().push_str(&format!(
            "\n\n[router-acp · the host returned the turn: {message}]\n\n"
        ));
        shared.state.lock().unwrap().log(
            gate.sub_sid,
            &crate::state::LogEntry {
                kind: "delegate_continue".to_string(),
                role: "user".to_string(),
                summary: message.chars().take(200).collect(),
                detail: Some(serde_json::json!({"message": message})),
                tokens_input: crate::state::estimate_tokens(&message),
                tokens_estimated: true,
                ..Default::default()
            },
        );
        let prompt = PromptRequest::new(
            gate.downstream_sid.to_string(),
            vec![ContentBlock::from(message)],
        );
        resp = gate.conn.send_request(prompt).block_task().await?;
    }
}

/// The worker a router-worker MCP connection belongs to.
#[derive(Debug, Clone)]
pub struct WorkerBinding {
    pub worker_id: String,
    pub candidate: String,
    pub parent_downstream_sid: String,
    pub planner: Option<crate::planner_workflow::WorkerIdentity>,
}

/// The `router-worker` MCP server for one host-directed delegate: its
/// identity and structured-handoff tools, over the same socket as the parent's
/// delegate tools but bound to the worker by its own token.
fn worker_server_entry(
    shared: &Arc<Shared>,
    router_sid: &str,
    worker: WorkerBinding,
) -> Option<McpServer> {
    let socket = shared.delegate_socket.get()?.clone();
    let exe = std::env::var("ROUTER_ACP_HELPER_EXE")
        .map(PathBuf::from)
        .or_else(|_| std::env::current_exe())
        .ok()?;
    let token = uuid::Uuid::new_v4().to_string();
    shared.delegate_tokens.lock().unwrap().insert(
        token.clone(),
        DelegateBinding {
            router_sid: router_sid.to_string(),
            delegation_enabled: false,
            worker: Some(worker),
        },
    );
    let stdio = McpServerStdio::new(WORKER_SERVER_NAME, exe).args(vec![
        "mcp-delegate".to_string(),
        "--socket".to_string(),
        socket.display().to_string(),
        "--token".to_string(),
        token,
    ]);
    Some(McpServer::Stdio(stdio))
}

/// Forget a finished worker's MCP tokens.
pub fn drop_worker_tokens(shared: &Shared, worker_id: &str) {
    shared
        .delegate_tokens
        .lock()
        .unwrap()
        .retain(|_, b| b.worker.as_ref().is_none_or(|w| w.worker_id != worker_id));
}

/// Revoke the parent's credentials when its MCP binding is replaced or closed.
pub fn drop_parent_tokens(shared: &Shared, router_sid: &str) {
    shared
        .delegate_tokens
        .lock()
        .unwrap()
        .retain(|_, b| b.router_sid != router_sid || b.worker.is_some());
    shared.with_session(router_sid, |s| s.delegate_token = None);
}

fn run_worker_handoff(
    shared: &Shared,
    worker: &WorkerBinding,
    args: WorkerHandoffArgs,
) -> Result<String, String> {
    if !crate::delegate_hook::HANDOFF_KINDS.contains(&args.kind.as_str()) {
        return Err(format!(
            "unknown handoff kind `{}`; use one of {}",
            args.kind,
            crate::delegate_hook::HANDOFF_KINDS.join(", ")
        ));
    }
    shared.worker_handoffs.lock().unwrap().insert(
        worker.worker_id.clone(),
        crate::delegate_hook::Handoff {
            kind: args.kind.clone(),
            message: args.message,
        },
    );
    Ok(format!(
        "Recorded `{}` handoff for worker {}. End your turn now; your host checks it when the \
         turn ends and may send you back with what is still missing.",
        args.kind, worker.worker_id
    ))
}

fn worker_whoami(worker: &WorkerBinding) -> String {
    format!(
        "worker id: {}\nmodel: {}\nparent session: {}",
        worker.worker_id, worker.candidate, worker.parent_downstream_sid
    )
}

/// `delegate_result`: re-read a delegate's latest output from the state DB,
/// by background job id (`b-…`), worker id (`w-…`) or live delegate id
/// (`d-…`), even after `delegate_await` consumed it.
fn run_delegate_result(
    shared: &Shared,
    router_sid: &str,
    args: DelegateResultArgs,
) -> Result<String, String> {
    let id = args.delegate_id.trim();
    let live_sub = shared
        .live_delegates
        .lock()
        .unwrap()
        .get(id)
        .filter(|d| d.parent_sid == router_sid)
        .map(|d| d.sub_sid.clone());
    let running = shared
        .background_delegates
        .lock()
        .unwrap()
        .get(id)
        .is_some_and(|j| j.parent_sid == router_sid && j.result.is_none());
    let state = shared.state.lock().unwrap();
    let row = state.all().into_iter().find(|(row_id, row)| {
        row.parent_session_id.as_deref() == Some(router_sid)
            && (live_sub.as_deref() == Some(row_id.as_str())
                || row.routing.as_ref().is_some_and(|r| {
                    r["worker_id"].as_str() == Some(id) || r["background_id"].as_str() == Some(id)
                }))
    });
    let Some((sub_sid, row)) = row else {
        return Err(format!(
            "no delegate `{id}` in this session (ids are b-…, w-… or d-…)"
        ));
    };
    let latest = state
        .log_for(&sub_sid, 200)
        .into_iter()
        .rev()
        .find(|e| e.kind == "agent_response")
        .and_then(|e| {
            e.detail
                .and_then(|d| d["text"].as_str().map(str::to_string))
        });
    let status = if running {
        "still running"
    } else if live_sub.is_some() {
        "open (keep_open)"
    } else {
        "finished"
    };
    Ok(format!(
        "delegate {id} on {}/{} — {status}\n{}",
        row.agent,
        row.model,
        latest.unwrap_or_else(|| "(no output recorded yet)".to_string())
    ))
}

/// Send a follow-up instruction to a delegate sub-session kept alive by an
/// earlier `delegate_task(keep_open=true)`, preserving that sub-agent's context.
pub async fn run_delegate_followup(
    shared: &Arc<Shared>,
    router_sid: &str,
    args: DelegateFollowupArgs,
) -> Result<String, String> {
    let planner = shared
        .live_delegates
        .lock()
        .unwrap()
        .get(&args.delegate_id)
        .filter(|d| d.parent_sid == router_sid)
        .and_then(|d| d.planner.clone());
    if let Some(identity) = planner {
        let candidate = shared
            .live_delegates
            .lock()
            .unwrap()
            .get(&args.delegate_id)
            .map(|d| d.candidate.clone())
            .ok_or("child disappeared")?;
        let run =
            crate::planner_workflow::load(shared, router_sid)?.ok_or("planner state missing")?;
        let role = if run.works[&identity.work_id].status
            == crate::planner_workflow::WorkStatus::Accepted
        {
            crate::planner_skills::PlannerRole::FinishWork
        } else {
            crate::planner_skills::PlannerRole::ImplementWork
        };
        let skill = &run.policy.roles[&role];
        if let Some(route) = crate::session::detect_skill_route(
            &shared.cfg,
            &[ContentBlock::from(format!("/{}", skill.name))],
        ) && !route
            .candidates
            .iter()
            .chain(&route.also_acceptable)
            .any(|pattern| crate::session::candidate_matches(pattern, &candidate))
        {
            return Err("mapped child role requires another model; close this delegate and dispatch the same work_id to preserve durable child, workspace and evidence".into());
        }
        let context = crate::planner_workflow::begin_followup(shared, router_sid, &identity)?;
        let mut scoped = args.clone();
        scoped.planner_identity = Some(identity.clone());
        scoped.planner_role = Some(role);
        let delivery =
            crate::planner_workflow::input_blocks(shared, router_sid, &identity, &args.input_ids);
        let (blocks, ids) = match delivery {
            Ok(delivery) => delivery,
            Err(error) => {
                crate::planner_workflow::attempt_ended(
                    shared,
                    router_sid,
                    &identity,
                    Some(args.delegate_id),
                    error.clone(),
                    true,
                )?;
                return Err(error);
            }
        };
        scoped.planner_input_blocks = blocks;
        scoped.planner_receipt_ids = ids;
        scoped.message = format!(
            "{context}\n[Parent correction or finish instruction]\n{}",
            args.message
        );
        let result = run_delegate_followup_inner(shared, router_sid, scoped).await;
        crate::planner_workflow::attempt_ended(
            shared,
            router_sid,
            &identity,
            Some(args.delegate_id),
            result.clone().unwrap_or_else(|e| e),
            result.is_err(),
        )?;
        crate::planner_client::turn_state(shared, router_sid, &identity, "idle")?;
        return result;
    }
    run_delegate_followup_inner(shared, router_sid, args).await
}

async fn run_delegate_followup_inner(
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
    let (process_key, downstream_sid, candidate, capture, sub_sid, lifecycle, worker_id, turns) = {
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
            d.lifecycle.clone(),
            d.worker_id.clone(),
            d.turns,
        )
    };
    let _agent_slot = match shared.agent_delegate_slots.get(&candidate.agent) {
        Some(slots) => Some(
            slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| "router shutting down".to_string())?,
        ),
        None => None,
    };
    // A new turn starts: its handoff is whatever the worker records now.
    shared.worker_handoffs.lock().unwrap().remove(&worker_id);

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
            detail: Some(serde_json::json!({"message": args.message})),
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

    let mut content = vec![ContentBlock::from(args.message.clone())];
    content.extend(args.planner_input_blocks.clone());
    let mut prompt = PromptRequest::new(downstream_sid.clone(), content);
    if let Some(identity) = &args.planner_identity {
        let role = args.planner_role;
        prompt = prompt.meta(serde_json::from_value(json!({"router_acp":{"planner_role":role,"work_id":identity.work_id,"attempt_id":identity.attempt_id,"agent_origin":true}})).ok());
    }
    let turn_start = std::time::Instant::now();
    if let Some(identity) = &args.planner_identity {
        crate::planner_client::turn_state(shared, router_sid, identity, "started")?;
        crate::planner_workflow::queue_delivery(
            shared,
            &sub_sid,
            router_sid,
            &identity.work_id,
            args.planner_receipt_ids.clone(),
        );
    }
    let _llm_turn = shared.llm_proxy.begin_turn(
        process_key.clone(),
        router_sid.to_string(),
        sub_sid.clone(),
        downstream_sid.clone(),
        candidate.clone(),
        TaskClass::CodingGeneral,
        None,
    );
    let request_generation = crate::auth::request_access_generation(shared, &candidate);
    let result = conn.send_request(prompt).block_task().await;
    if result.is_ok() {
        crate::planner_workflow::confirm_delivery(shared, &sub_sid)?;
    }
    shared.planner_deliveries.lock().unwrap().remove(&sub_sid);
    let mut turns = turns + 1;
    let result = match result {
        Ok(resp) => {
            gate_turns(
                shared,
                &TurnGate {
                    lifecycle: lifecycle.as_ref(),
                    conn: &conn,
                    downstream_sid: &downstream_sid,
                    sub_sid: &sub_sid,
                    worker_id: &worker_id,
                    capture: &capture,
                },
                resp,
                &mut turns,
            )
            .await
        }
        Err(err) => Err(err),
    };
    let result = result.and_then(|resp| {
        if resp.stop_reason != StopReason::Cancelled
            && crate::auth::response_is_auth_error(&capture.lock().unwrap())
        {
            Err(AcpError::auth_required().data("Provider reported an authentication error"))
        } else {
            Ok(resp)
        }
    });
    if result
        .as_ref()
        .is_err_and(crate::downstream::is_auth_required)
    {
        crate::auth::note_auth_failure_for_request(
            shared,
            &candidate.agent,
            "Authentication unavailable",
            request_generation.as_deref(),
        )
        .await;
    }
    if let Some(live) = shared
        .live_delegates
        .lock()
        .unwrap()
        .get_mut(&args.delegate_id)
    {
        live.turns = turns;
    }
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
            let tu = crate::session::turn_tokens(&resp, &text);
            shared.state.lock().unwrap().log(
                &sub_sid,
                &crate::state::LogEntry {
                    ts: None,
                    kind: "agent_response".to_string(),
                    role: "agent".to_string(),
                    summary: text.chars().take(200).collect(),
                    detail: Some(serde_json::json!({"text": text})),
                    tokens_input: tu.input,
                    tokens_output: tu.output,
                    tokens_cache_read: tu.cache_read,
                    tokens_cache_write: tu.cache_write,
                    tokens_estimated: tu.estimated,
                    model: Some(candidate.to_string()),
                },
            );
            synth_delegate_cost(shared, &sub_sid, &candidate, &tu);
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
    d.finish(shared, "closed");
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
    fn listed_tools_include_delegate_family_without_a_pin() {
        // Codex lists MCP tools during session/new, before session.pin exists.
        // The list must still expose the four delegate tools when injection
        // decided delegation was available.
        let names = |tools: &[serde_json::Value]| -> Vec<String> {
            tools
                .iter()
                .filter_map(|tool| tool["name"].as_str().map(str::to_string))
                .collect()
        };
        let both = names(&listed_tools(true, true));
        for required in [
            DELEGATE_TOOL_NAME,
            DELEGATE_AWAIT_TOOL_NAME,
            DELEGATE_FOLLOWUP_TOOL_NAME,
            DELEGATE_CLOSE_TOOL_NAME,
            BACKGROUND_START_TOOL_NAME,
        ] {
            assert!(
                both.iter().any(|n| n == required),
                "pre-pin tools/list omitted {required}: {both:?}"
            );
        }
        assert_eq!(
            names(&listed_tools(true, false)),
            [
                DELEGATE_TOOL_NAME,
                DELEGATE_AWAIT_TOOL_NAME,
                DELEGATE_FOLLOWUP_TOOL_NAME,
                DELEGATE_CLOSE_TOOL_NAME,
                DELEGATE_RESULT_TOOL_NAME,
                crate::planner_workflow::TOOL_NAME
            ]
        );
        assert_eq!(
            names(&listed_tools(false, true)),
            [BACKGROUND_START_TOOL_NAME]
        );
    }

    #[test]
    fn background_start_tool_definition_requires_an_executable() {
        let def = background_start_tool_definition();
        assert_eq!(def["name"], BACKGROUND_START_TOOL_NAME);
        assert_eq!(def["inputSchema"]["required"][0], "command");
        assert_eq!(
            def["inputSchema"]["properties"]["args"]["items"]["type"],
            "string"
        );
        assert!(
            def["description"]
                .as_str()
                .is_some_and(|text| text.contains("run_in_background"))
        );
        assert_eq!(
            def["inputSchema"]["properties"]["wake_on_exit"]["default"],
            true
        );
        let default_args: BackgroundStartArgs =
            serde_json::from_value(json!({"command": "/bin/true"})).unwrap();
        assert!(default_args.wake_on_exit);
        let server_args: BackgroundStartArgs = serde_json::from_value(json!({
            "command": "/usr/bin/server",
            "wake_on_exit": false
        }))
        .unwrap();
        assert!(!server_args.wake_on_exit);
    }

    #[test]
    fn delegate_args_background_defaults_off() {
        use serde_json::json;
        let args: DelegateTaskArgs = serde_json::from_value(json!({"task": "do a thing"})).unwrap();
        assert!(!args.background);
        let args: DelegateTaskArgs =
            serde_json::from_value(json!({"task": "do a thing", "background": true})).unwrap();
        assert!(args.background);
        let args: DelegateTaskArgs = serde_json::from_value(json!({
            "task": "inspect telemetry",
            "required_capabilities": ["metrics"]
        }))
        .unwrap();
        assert_eq!(args.required_capabilities, ["metrics"]);
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

    fn pool3() -> Vec<crate::strategies::CandidateView> {
        use crate::candidate::CodingTier;
        let view = |agent: &str, model: &str, cost_rank: u32, idx: usize| {
            crate::strategies::CandidateView {
                id: CandidateId::new(agent, model),
                cost_rank,
                config_index: idx,
                quality: 0.8,
                coding_tier: CodingTier::High,
                headroom: 1.0,
                plan_headroom: None,
                on_overage: false,
                preference: 0.0,
            }
        };
        vec![
            view("claude", "haiku", 1, 0),
            view("claude", "sonnet", 2, 1),
            view("claude", "fable", 5, 2),
        ]
    }

    #[test]
    fn exact_hints_address_any_tier_or_fail() {
        let fable = CandidateId::new("claude", "fable");
        let pool = exact_hint_pool(pool3(), "claude/fable", Some(&fable)).unwrap();
        assert_eq!(pool.len(), 1);
        assert_eq!(pool[0].id, fable);
        let missing = CandidateId::new("grok", "grok-4.7");
        let err = exact_hint_pool(pool3(), "grok/grok-4.7", Some(&missing)).unwrap_err();
        assert!(err.contains("not substituting"), "{err}");
        assert!(exact_hint_pool(pool3(), "nonsense", None).is_err());
    }

    #[test]
    fn effort_hints_parse_strictly() {
        assert_eq!(parse_effort_hint(None).unwrap(), None);
        assert_eq!(parse_effort_hint(Some("auto")).unwrap(), None);
        assert_eq!(
            parse_effort_hint(Some(" LOW ")).unwrap(),
            Some(EffortLevel::Low)
        );
        assert!(parse_effort_hint(Some("turbo")).is_err());
    }

    #[test]
    fn ordinary_delegation_is_strictly_cheaper() {
        let scoped = scope_delegate_pool(pool3(), 5, "claude");
        assert!(scoped.iter().all(|v| v.cost_rank < 5));
        assert_eq!(scoped.len(), 2);
        // Parent already cheapest → empty pool → the caller's error path.
        assert!(scope_delegate_pool(pool3(), 1, "claude").is_empty());
    }

    #[test]
    fn sol_delegation_prefers_cheaper_codex_siblings() {
        let mut pool = pool3();
        let mut codex = pool3();
        codex[0].id = CandidateId::new("codex", "luna");
        codex[0].cost_rank = 2;
        codex[1].id = CandidateId::new("codex", "terra");
        codex[1].cost_rank = 4;
        codex[2].id = CandidateId::new("codex", "sol");
        codex[2].cost_rank = 5;
        pool.extend(codex);

        let scoped = scope_delegate_pool(pool, 5, "codex");
        let ids: Vec<String> = scoped.into_iter().map(|view| view.id.to_string()).collect();
        assert_eq!(ids, vec!["codex/luna", "codex/terra"]);
    }
}
