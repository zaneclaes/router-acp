//! The router's MCP OAuth broker: one sign-in per remote MCP server, shared by
//! every downstream session of every agent family.
//!
//! The router defines no servers. A session's project `.mcp.json`
//! (`mcp_oauth::project_servers`) names them; every remote streamable-HTTP
//! entry without its own credential that the router has signed in to
//! (`router-acp mcp-login <name>`) is brokered. Everything else in the file
//! (stdio, legacy SSE, entries with static headers, servers not signed in) is
//! left to the adapters exactly as before: Claude reads the file itself, the
//! other adapters never do.
//!
//! Ported from Kory Code's relay (`datadog-mcp-proxy.mjs`, `mcp-proxy-http.mjs`,
//! `mcp-discover.mjs`). `serve` binds a loopback listener on a random port and
//! every downstream `session/new` (parent pins, switches, delegates, workers)
//! receives each brokered server as
//! `{type: http, name, url: http://127.0.0.1:<port>/mcp/<entry>,
//!   headers: [Authorization: Bearer <process secret>]}`, replacing any
//! same-named server the client sent. Sessions never receive an upstream
//! bearer: a copied access token dies after its lifetime while the session
//! keeps sending it. The proxy reads the current token on every request
//! (`mcp_oauth::TokenSource`), refreshes inside the refresh window, and on an
//! authentication rejection forces one refresh and retries; only a replacement
//! the upstream also refuses is marked "reconnect needed". `<entry>` names an
//! upstream URL the router itself read from a project file, so the process
//! secret cannot be used to send the token anywhere else.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use agent_client_protocol::schema::v1::{HttpHeader, McpServer, McpServerHttp};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use axum::routing::any;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::downstream::ProcessKey;
use crate::mcp_oauth::{AccessError, RemoteServer, StatusState, TokenSource};

const PROXY_PATH: &str = "/mcp/";
const MAX_BODY: usize = 4 * 1024 * 1024;
/// Bounds time to headers plus buffering; a streamed GET body is not cut off.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const FORWARD_HEADERS: [&str; 4] = [
    "content-type",
    "mcp-protocol-version",
    "mcp-session-id",
    "last-event-id",
];
/// `www-authenticate` is deliberately not relayed: it would invite an adapter
/// to start its own OAuth against the upstream, which only the router may do.
const RESPONSE_HEADERS: [&str; 3] = ["content-type", "mcp-session-id", "mcp-protocol-version"];

/// One proxied upstream: the URL a project file named, and its sign-in.
struct Entry {
    name: String,
    upstream: String,
    source: Arc<TokenSource>,
    /// The router session the entry was issued to (none for `mcp-call` and
    /// delegate workers): its tool results may fill that session's updates.
    session: Option<String>,
}

/// A tool result the broker carried for a session, kept briefly so a
/// completed tool update the agent reported empty can show it.
struct Capture {
    server: String,
    tool: String,
    text: String,
    at: std::time::Instant,
}

/// Results older than this no longer fill an update.
const CAPTURE_TTL: Duration = Duration::from_secs(120);
/// Results kept per session.
const CAPTURE_MAX: usize = 20;
/// Characters of one result written into an update.
const CAPTURE_TEXT_MAX: usize = 80_000;

#[derive(Default)]
struct Captures {
    results: HashMap<String, std::collections::VecDeque<Capture>>,
    /// Tool-call titles by (session, toolCallId): updates may omit the title.
    titles: HashMap<(String, String), String>,
}

/// A Claude runtime the broker may write private settings into.
struct ClaudeRuntime {
    root: PathBuf,
    config: PathBuf,
    suppressed: BTreeSet<String>,
}

pub struct Broker {
    root: PathBuf,
    /// The host's fallback credential helper (`mcp_credential_command`).
    helper: Option<crate::config::McpCredentialCommand>,
    secret: String,
    addr: OnceLock<SocketAddr>,
    http: reqwest::Client,
    /// Token sources by store key: every entry for one server shares one
    /// single-flight refresh.
    sources: Mutex<HashMap<String, Arc<TokenSource>>>,
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    captures: Mutex<Captures>,
    runtimes: Mutex<HashMap<ProcessKey, ClaudeRuntime>>,
}

impl Broker {
    pub fn new(root: PathBuf, helper: Option<crate::config::McpCredentialCommand>) -> Arc<Self> {
        Arc::new(Self {
            root,
            helper,
            secret: crate::mcp_oauth::random_token(32),
            addr: OnceLock::new(),
            http: reqwest::Client::new(),
            sources: Mutex::default(),
            entries: Mutex::default(),
            captures: Mutex::default(),
            runtimes: Mutex::default(),
        })
    }

    fn bearer(&self) -> String {
        format!("Bearer {}", self.secret)
    }

    /// Bind the loopback listener. On failure the broker stays unbound: no
    /// session receives a broker entry and nothing is suppressed.
    pub async fn bind(self: &Arc<Self>) -> Result<tokio::task::JoinHandle<()>, String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("cannot bind the MCP broker: {e}"))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("cannot read the MCP broker address: {e}"))?;
        self.addr
            .set(addr)
            .map_err(|_| "the MCP broker is already bound".to_string())?;
        let app = Router::new()
            .route("/mcp/{entry}", any(handle))
            .layer(DefaultBodyLimit::max(MAX_BODY))
            .with_state(self.clone());
        tracing::debug!(%addr, "MCP broker listening");
        Ok(tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app).await {
                tracing::warn!(%err, "MCP broker stopped");
            }
        }))
    }

    fn source_for(&self, server: &RemoteServer) -> Arc<TokenSource> {
        self.sources
            .lock()
            .unwrap()
            .entry(server.store_key())
            .or_insert_with(|| {
                Arc::new(
                    TokenSource::new(server.store(&self.root), self.http.clone()).with_supplier(
                        crate::mcp_oauth::Supplier::for_server(self.helper.as_ref(), server),
                    ),
                )
            })
            .clone()
    }

    fn entry_for(&self, server: &RemoteServer) -> Entry {
        Entry {
            name: server.name.clone(),
            upstream: server.url.clone(),
            source: self.source_for(server),
            session: None,
        }
    }

    /// `tools/call` with the router's sign-in. A refused bearer gets one
    /// forced refresh and one retry; a refreshed bearer the upstream also
    /// refuses marks the server reconnect-needed, as the proxy does.
    async fn call(
        &self,
        entry: &Entry,
        tool: &str,
        arguments: &Value,
        timeout: Duration,
    ) -> Result<ToolCall, CallFailure> {
        let source = &entry.source;
        let mut token = source
            .access_token(false, None)
            .await
            .map_err(|e| access_failure_cli(entry, e))?;
        if source.denies(&token, tool).await {
            return Err(CallFailure::SignedOut(denied_message(&entry.name, tool)));
        }
        match call_once(
            &self.http,
            &entry.upstream,
            &token,
            tool,
            arguments,
            timeout,
        )
        .await
        {
            Err(RpcError::Refused) => {}
            outcome => return outcome.map_err(rpc_failure),
        }
        let rejected = std::mem::take(&mut token);
        token = source
            .access_token(true, Some(&rejected))
            .await
            .map_err(|e| access_failure_cli(entry, e))?;
        match call_once(
            &self.http,
            &entry.upstream,
            &token,
            tool,
            arguments,
            timeout,
        )
        .await
        {
            Err(RpcError::Refused) => {
                source.mark_access_rejected(&token).await;
                Err(CallFailure::SignedOut(format!(
                    "{} MCP reconnect needed: sign in with `router-acp mcp-login {}`",
                    entry.name, entry.name
                )))
            }
            outcome => outcome.map_err(rpc_failure),
        }
    }

    /// The broker entries for a session in `cwd`: each brokered project server
    /// the router is signed in to. A server that needs a human sign-in is left
    /// to the adapters as before (Kory likewise omits a hard disconnect and
    /// still starts the session).
    pub fn session_servers(&self, cwd: &FsPath, session: Option<&str>) -> Vec<McpServer> {
        let Some(addr) = self.addr.get() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for project in crate::mcp_oauth::project_servers(cwd) {
            let Some(remote) = project.remote() else {
                continue;
            };
            let source = self.source_for(&remote);
            if !source.injectable() {
                continue;
            }
            let id: String = Sha256::digest(
                format!(
                    "{}\n{}\n{}",
                    remote.name,
                    remote.url,
                    session.unwrap_or_default()
                )
                .as_bytes(),
            )[..8]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            self.entries.lock().unwrap().insert(
                id.clone(),
                Arc::new(Entry {
                    name: remote.name.clone(),
                    upstream: remote.url.clone(),
                    source,
                    session: session.map(String::from),
                }),
            );
            out.push(McpServer::Http(
                McpServerHttp::new(remote.name, format!("http://{addr}{PROXY_PATH}{id}"))
                    .headers(vec![HttpHeader::new("Authorization", self.bearer())]),
            ));
        }
        out
    }

    fn capture(&self, entry: &Entry, tool: &str, result: &Value) {
        let Some(session) = &entry.session else {
            return;
        };
        let text = result_text(result);
        if text.is_empty() {
            return;
        }
        let mut captures = self.captures.lock().unwrap();
        let list = captures.results.entry(session.clone()).or_default();
        list.retain(|c| c.at.elapsed() < CAPTURE_TTL);
        if list.len() >= CAPTURE_MAX {
            list.pop_front();
        }
        list.push_back(Capture {
            server: entry.name.clone(),
            tool: tool.to_string(),
            text: text.chars().take(CAPTURE_TEXT_MAX).collect(),
            at: std::time::Instant::now(),
        });
    }

    /// Fill a completed tool update the agent reported without output, from
    /// the result this broker carried for that session's matching tool call.
    /// Some adapters drop MCP output from the update they send the client.
    /// Remembers `tool_call` titles for later updates that omit them. True
    /// when `update` changed.
    pub fn fill_tool_result(&self, session: &str, update: &mut Value) -> bool {
        let kind = update
            .get("sessionUpdate")
            .and_then(Value::as_str)
            .unwrap_or("");
        let id = update
            .get("toolCallId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let title = update
            .get("title")
            .and_then(Value::as_str)
            .map(String::from);
        let mut captures = self.captures.lock().unwrap();
        let key = (session.to_string(), id);
        if kind == "tool_call" {
            if let Some(title) = title {
                captures.titles.insert(key, title);
            }
            return false;
        }
        if kind != "tool_call_update"
            || update.get("status").and_then(Value::as_str) != Some("completed")
        {
            return false;
        }
        let Some(title) = title.or_else(|| captures.titles.get(&key).cloned()) else {
            return false;
        };
        captures.titles.remove(&key);
        let Some(list) = captures.results.get_mut(session) else {
            return false;
        };
        list.retain(|c| c.at.elapsed() < CAPTURE_TTL);
        let Some(found) = list
            .iter()
            .position(|c| title_names(&title, &c.server, &c.tool))
        else {
            return false;
        };
        // A reported result is the source of truth; still consume the capture
        // so a later empty update cannot pick up this call's leftover.
        let capture = list.remove(found).unwrap();
        let empty = |field: &str| {
            update
                .get(field)
                .is_none_or(|v| v.is_null() || v.as_array().is_some_and(Vec::is_empty))
        };
        if !empty("content") || !empty("rawOutput") {
            return false;
        }
        update["content"] =
            json!([{"type": "content", "content": {"type": "text", "text": capture.text}}]);
        true
    }

    /// True for an entry this broker produced.
    pub fn is_broker_entry(&self, server: &McpServer) -> bool {
        let Some(addr) = self.addr.get() else {
            return false;
        };
        let prefix = format!("http://{addr}{PROXY_PATH}");
        matches!(server, McpServer::Http(http) if http.url.starts_with(&prefix))
    }

    /// A process's Claude runtime (root, mirrored config dir), or None for a
    /// process without one. Called on every spawn; a respawn starts clean.
    pub fn register_runtime(&self, key: &ProcessKey, claude: Option<(PathBuf, PathBuf)>) {
        let mut runtimes = self.runtimes.lock().unwrap();
        match claude {
            Some((root, config)) => {
                runtimes.insert(
                    key.clone(),
                    ClaudeRuntime {
                        root,
                        config,
                        suppressed: BTreeSet::new(),
                    },
                );
            }
            None => {
                runtimes.remove(key);
            }
        }
    }

    /// Before a Claude `session/new`: disable the project `.mcp.json` copies
    /// of the broker entries it is about to receive, in that runtime's private
    /// settings only. The set only grows: a name one project brokers is, in
    /// another project, a server whose project copy cannot authenticate inside
    /// a router runtime anyway.
    pub fn suppress_duplicates(&self, key: &ProcessKey, servers: &[McpServer]) {
        let names: BTreeSet<String> = servers
            .iter()
            .filter(|s| self.is_broker_entry(s))
            .filter_map(|s| match s {
                McpServer::Http(http) => Some(http.name.clone()),
                _ => None,
            })
            .collect();
        if names.is_empty() {
            return;
        }
        let mut runtimes = self.runtimes.lock().unwrap();
        let Some(runtime) = runtimes.get_mut(key) else {
            return;
        };
        // Claude keeps a runtime-wide, name-keyed "needs auth" cache. A Claude
        // the adapter started before this suppression existed tried the
        // project copies, failed, and cached these names; every later session
        // would then skip the working broker entry as needing auth.
        if let Err(err) = crate::credentials::forget_claude_mcp_needs_auth(&runtime.root, &names) {
            tracing::warn!(%key, "could not clear Claude's MCP needs-auth cache: {err}");
        }
        if names.is_subset(&runtime.suppressed) {
            return;
        }
        runtime.suppressed.extend(names);
        let all: Vec<String> = runtime.suppressed.iter().cloned().collect();
        if let Err(err) =
            crate::credentials::write_claude_mcp_suppression(&runtime.config, &runtime.root, &all)
        {
            tracing::warn!(%key, "could not suppress project MCP duplicates: {err}");
        }
    }
}

/// Replace same-named servers with `entries` (the broker is authoritative).
pub fn merge_authoritative(servers: &mut Vec<McpServer>, entries: Vec<McpServer>) {
    for entry in entries {
        if let McpServer::Http(http) = &entry {
            let name = http.name.clone();
            servers.retain(|server| server_name(server) != Some(name.as_str()));
        }
        servers.push(entry);
    }
}

fn server_name(server: &McpServer) -> Option<&str> {
    match server {
        McpServer::Http(server) => Some(&server.name),
        McpServer::Sse(server) => Some(&server.name),
        McpServer::Stdio(server) => Some(&server.name),
        _ => None,
    }
}

fn json_response(status: StatusCode, body: Value) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn rpc_error(status: StatusCode, code: i64, message: String) -> Response {
    json_response(status, json!({"error": {"code": code, "message": message}}))
}

async fn handle(
    State(broker): State<Arc<Broker>>,
    Path(entry): Path<String>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let presented = headers
        .get(header::AUTHORIZATION)
        .map(HeaderValue::as_bytes)
        .unwrap_or_default();
    if !crate::mcp_oauth::constant_time_eq(presented, broker.bearer().as_bytes()) {
        return json_response(StatusCode::UNAUTHORIZED, json!({"error": "unauthorized"}));
    }
    if !matches!(method, Method::GET | Method::POST | Method::DELETE) {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header(header::ALLOW, "GET, POST, DELETE")
            .body(Body::empty())
            .unwrap();
    }
    let Some(entry) = broker.entries.lock().unwrap().get(&entry).cloned() else {
        return json_response(
            StatusCode::NOT_FOUND,
            json!({"error": "unknown MCP server"}),
        );
    };
    proxy(&broker, &entry, method, &headers, body).await
}

fn access_failure(entry: &Entry, error: AccessError) -> Response {
    let name = &entry.name;
    let message = match error {
        AccessError::SignedOut(label) => {
            format!("{name} MCP {label}: sign in with `router-acp mcp-login {name}`")
        }
        AccessError::Unavailable(why) => {
            tracing::warn!(server = name, "MCP broker credential unavailable: {why}");
            entry
                .source
                .note(StatusState::Degraded, "status unavailable");
            format!("{name} MCP temporarily unavailable")
        }
    };
    rpc_error(StatusCode::SERVICE_UNAVAILABLE, -32001, message)
}

async fn proxy(
    broker: &Broker,
    entry: &Entry,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let source = &entry.source;
    let mut token = match source.access_token(false, None).await {
        Ok(token) => token,
        Err(error) => return access_failure(entry, error),
    };
    let call = tool_call(&body);
    if let Some((id, tool)) = &call
        && source.denies(&token, tool).await
    {
        return json_response(
            StatusCode::OK,
            json!({"jsonrpc": "2.0", "id": id, "result": {"isError": true,
                "content": [{"type": "text", "text": denied_message(&entry.name, tool)}]}}),
        );
    }
    // Grok and goose open with `server/discover`, which these upstreams
    // answer 404; reply locally so the client negotiates the legacy protocol.
    if let Some(reply) = legacy_discover_response(&body, &entry.name) {
        return json_response(StatusCode::OK, reply);
    }
    let unavailable = || {
        source.note(StatusState::Degraded, "upstream unavailable");
        rpc_error(
            StatusCode::BAD_GATEWAY,
            -32002,
            format!("{} MCP temporarily unavailable", entry.name),
        )
    };
    let Ok(mut upstream) = forward(broker, entry, &method, headers, &body, &token).await else {
        return unavailable();
    };
    if upstream.rejects_auth() {
        let rejected = std::mem::take(&mut token);
        token = match source.access_token(true, Some(&rejected)).await {
            Ok(token) => token,
            Err(error) => return access_failure(entry, error),
        };
        upstream = match forward(broker, entry, &method, headers, &body, &token).await {
            Ok(upstream) => upstream,
            Err(()) => return unavailable(),
        };
        if upstream.rejects_auth() {
            tracing::warn!(
                server = entry.name,
                "MCP upstream refused a refreshed bearer"
            );
            source.mark_access_rejected(&token).await;
        }
    }
    if upstream.status.is_success() && !upstream.rejects_auth() {
        source.note(StatusState::SignedIn, "signed in");
        if let (Some((_, tool)), UpstreamBody::Buffered(bytes)) = (&call, &upstream.body) {
            let content_type = upstream
                .headers
                .iter()
                .find(|(name, _)| *name == "content-type")
                .and_then(|(_, v)| v.to_str().ok())
                .unwrap_or("");
            if let Some(result) = parse_rpc(content_type, &String::from_utf8_lossy(bytes))
                .and_then(|m| m.get("result").cloned())
            {
                broker.capture(entry, tool, &result);
            }
        }
    }
    upstream.into_response()
}

/// The JSON-RPC id and tool name of a `tools/call` request body.
fn tool_call(body: &[u8]) -> Option<(Value, String)> {
    let message: Value = serde_json::from_slice(body).ok()?;
    if message.get("method")?.as_str()? != "tools/call" {
        return None;
    }
    let tool = message.pointer("/params/name")?.as_str()?.to_string();
    Some((message.get("id").cloned().unwrap_or(Value::Null), tool))
}

/// A tool the host's credential may not call (the helper's `denyTools`).
fn denied_message(server: &str, tool: &str) -> String {
    format!(
        "{server} MCP: `{tool}` needs your own sign-in; the host credential may not call it. \
         Sign in with /mcp or `router-acp mcp-login {server}`."
    )
}

/// A tool result's text, as the agent would have shown it.
fn result_text(result: &Value) -> String {
    match result.get("content").and_then(Value::as_array) {
        Some(blocks) => blocks
            .iter()
            .filter_map(|b| b.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        None => String::new(),
    }
}

/// Whether an adapter's tool-call title names `tool` on `server`: the bare
/// name, `server__tool` (goose), or `mcp__server__tool` (Claude, Grok).
fn title_names(title: &str, server: &str, tool: &str) -> bool {
    title == tool
        || title == format!("{server}__{tool}")
        || title == format!("mcp__{server}__{tool}")
}

enum UpstreamBody {
    Buffered(Bytes),
    Streaming(reqwest::Response),
}

struct Upstream {
    status: StatusCode,
    headers: Vec<(&'static str, HeaderValue)>,
    body: UpstreamBody,
}

impl Upstream {
    fn rejects_auth(&self) -> bool {
        let body = match &self.body {
            UpstreamBody::Buffered(bytes) => Some(bytes.as_ref()),
            UpstreamBody::Streaming(_) => None,
        };
        response_rejects_auth(self.status, body)
    }

    fn into_response(self) -> Response {
        let mut builder = Response::builder()
            .status(self.status)
            .header(header::CACHE_CONTROL, "no-store");
        for (name, value) in self.headers {
            builder = builder.header(name, value);
        }
        let body = match self.body {
            UpstreamBody::Buffered(bytes) => Body::from(bytes),
            UpstreamBody::Streaming(response) => Body::from_stream(response.bytes_stream()),
        };
        builder.body(body).unwrap()
    }
}

/// Forward one streamable-HTTP request with the current upstream bearer. GET
/// (the optional long-lived SSE channel) streams; POST/DELETE are buffered so
/// an authorization error is seen before anything reaches the session.
async fn forward(
    broker: &Broker,
    entry: &Entry,
    method: &Method,
    headers: &HeaderMap,
    body: &Bytes,
    token: &str,
) -> Result<Upstream, ()> {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json, text/event-stream");
    let mut request = broker
        .http
        .request(method.clone(), &entry.upstream)
        .header(header::ACCEPT, accept)
        .bearer_auth(token);
    for name in FORWARD_HEADERS {
        if let Some(value) = headers.get(name) {
            request = request.header(name, value);
        }
    }
    if !body.is_empty() {
        request = request.body(body.clone());
    }
    let streaming = method == Method::GET;
    if !streaming {
        request = request.timeout(FORWARD_TIMEOUT);
    }
    let response = match tokio::time::timeout(FORWARD_TIMEOUT, request.send()).await {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            tracing::warn!(
                server = entry.name,
                "MCP upstream request failed: {}",
                err.without_url()
            );
            return Err(());
        }
        Err(_) => {
            tracing::warn!(server = entry.name, "MCP upstream timed out");
            return Err(());
        }
    };
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let headers = RESPONSE_HEADERS
        .iter()
        .filter_map(|name| Some((*name, response.headers().get(*name)?.clone())))
        .collect();
    let body = if streaming {
        UpstreamBody::Streaming(response)
    } else {
        UpstreamBody::Buffered(response.bytes().await.map_err(|_| ())?)
    };
    Ok(Upstream {
        status,
        headers,
        body,
    })
}

fn auth_error_text(text: &str) -> bool {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex::Regex::new(
            r"(?i)requires? re-?authorization|token (?:has )?expired|invalid (?:access )?token|unauthorized",
        )
        .unwrap()
    })
    .is_match(text)
}

/// 401/403, or a JSON-RPC error / tool error (plain JSON or SSE `data:`
/// frames) that says the bearer was refused.
pub(crate) fn response_rejects_auth(status: StatusCode, body: Option<&[u8]>) -> bool {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return true;
    }
    let Some(text) = body.and_then(|b| std::str::from_utf8(b).ok()) else {
        return false;
    };
    let mut candidates = vec![text];
    candidates.extend(
        text.lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim),
    );
    candidates.into_iter().any(|candidate| {
        let Ok(message) = serde_json::from_str::<Value>(candidate) else {
            return false;
        };
        if let Some(error) = message.get("error")
            && auth_error_text(&error.to_string())
        {
            return true;
        }
        let result = message.get("result");
        result
            .and_then(|r| r.get("isError"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
            && auth_error_text(
                &result
                    .and_then(|r| r.get("content"))
                    .or(result)
                    .map(Value::to_string)
                    .unwrap_or_default(),
            )
    })
}

/// Legacy protocol versions the upstreams serve; advertising only these makes
/// a `server/discover` client fall back to `initialize`.
const LEGACY_PROTOCOL_VERSIONS: [&str; 4] =
    ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

pub(crate) fn legacy_discover_response(body: &[u8], server_name: &str) -> Option<Value> {
    let message: Value = serde_json::from_slice(body).ok()?;
    if message.get("method").and_then(Value::as_str) != Some("server/discover") {
        return None;
    }
    Some(json!({
        "jsonrpc": "2.0",
        "id": message.get("id").cloned().unwrap_or(Value::Null),
        "result": {
            "resultType": "complete",
            "supportedVersions": LEGACY_PROTOCOL_VERSIONS,
            "capabilities": {"tools": {"listChanged": true}},
            "ttlMs": 3_600_000,
            "cacheScope": "public",
            "_meta": {"io.modelcontextprotocol/serverInfo": {"name": server_name, "version": "0"}},
        },
    }))
}

// ----------------------------------------------------------------------
// Verification probe (mcp-login)
// ----------------------------------------------------------------------

const PROBE_PROTOCOL: &str = "2025-06-18";

fn parse_rpc(content_type: &str, body: &str) -> Option<Value> {
    if content_type.contains("text/event-stream") {
        let mut last = None;
        for frame in body.split("\n\n") {
            let data: Vec<_> = frame
                .lines()
                .filter_map(|l| l.strip_prefix("data:"))
                .map(str::trim)
                .collect();
            if let Ok(message) = serde_json::from_str::<Value>(&data.join("\n"))
                && (message.get("result").is_some() || message.get("error").is_some())
            {
                last = Some(message);
            }
        }
        return last;
    }
    serde_json::from_str(body).ok()
}

fn initialize_request() -> Value {
    json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": PROBE_PROTOCOL, "capabilities": {},
        "clientInfo": {"name": crate::mcp_oauth::CLIENT_NAME, "version": env!("CARGO_PKG_VERSION")}}})
}

/// One streamable-HTTP MCP session: every POST carries the bearer and the
/// `Mcp-Session-Id` the upstream issued.
struct Rpc<'a> {
    http: &'a reqwest::Client,
    url: &'a str,
    token: &'a str,
    session: Option<String>,
}

/// Why one POST failed. Only `Refused` is an authorization failure.
enum RpcError {
    Refused,
    /// Unreachable, timed out, 408, 429 or 5xx: worth trying again later.
    Transient(String),
    Other(String),
}

impl RpcError {
    fn message(self) -> String {
        match self {
            Self::Refused => "the MCP server refused the token".into(),
            Self::Transient(why) | Self::Other(why) => why,
        }
    }
}

impl Rpc<'_> {
    /// POST one JSON-RPC message; the reply's message when it has one.
    async fn post(
        &mut self,
        payload: &Value,
        timeout: Duration,
    ) -> Result<Option<Value>, RpcError> {
        let mut request = self
            .http
            .post(self.url)
            .bearer_auth(self.token)
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header("MCP-Protocol-Version", PROBE_PROTOCOL)
            .json(payload)
            .timeout(timeout);
        if let Some(id) = &self.session {
            request = request.header("Mcp-Session-Id", id);
        }
        let response = request
            .send()
            .await
            .map_err(|_| RpcError::Transient("MCP server unreachable".into()))?;
        let status = response.status();
        if let Some(id) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
        {
            self.session = Some(id.to_string());
        }
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let text = response.text().await.unwrap_or_default();
        let message = parse_rpc(&content_type, &text);
        let code = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        if response_rejects_auth(code, Some(text.as_bytes())) {
            return Err(RpcError::Refused);
        }
        if !status.is_success() {
            let code = status.as_u16();
            let message = format!("MCP server answered HTTP {code}");
            return Err(if code == 408 || code == 429 || status.is_server_error() {
                RpcError::Transient(message)
            } else {
                RpcError::Other(message)
            });
        }
        Ok(message)
    }
}

/// `initialize` → `notifications/initialized` → `tools/list` against the
/// upstream; the number of tools, or why it failed (no response bodies).
pub async fn probe_tools(http: &reqwest::Client, url: &str, token: &str) -> Result<usize, String> {
    let mut rpc = Rpc {
        http,
        url,
        token,
        session: None,
    };
    let timeout = Duration::from_secs(15);
    let init = rpc
        .post(&initialize_request(), timeout)
        .await
        .map_err(RpcError::message)?;
    if init.as_ref().and_then(|m| m.get("result")).is_none() {
        return Err("MCP initialize returned no result".into());
    }
    rpc.post(
        &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        timeout,
    )
    .await
    .map_err(RpcError::message)?;
    let list = rpc
        .post(
            &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
            timeout,
        )
        .await
        .map_err(RpcError::message)?;
    list.as_ref()
        .and_then(|m| m.pointer("/result/tools"))
        .and_then(Value::as_array)
        .map(Vec::len)
        .ok_or_else(|| "MCP tools/list returned no tools".to_string())
}

// ----------------------------------------------------------------------
// CLI: mcp-call
// ----------------------------------------------------------------------

/// The result of one `tools/call` the upstream answered.
#[derive(Debug)]
pub struct ToolCall {
    /// The JSON-RPC `result` object.
    pub result: Value,
    /// The tool reported `isError: true`.
    pub is_error: bool,
}

/// Why `mcp-call` returned no tool result. Each variant has one exit status.
#[derive(Debug)]
pub enum CallFailure {
    /// Not signed in, or the sign-in needs repair (exit 2).
    SignedOut(String),
    /// Network, token endpoint, 5xx or timeout (exit 3).
    Transient(String),
    /// Anything else (exit 1).
    Other(String),
}

impl CallFailure {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::SignedOut(_) => 2,
            Self::Transient(_) => 3,
            Self::Other(_) => 1,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::SignedOut(why) | Self::Transient(why) | Self::Other(why) => why,
        }
    }
}

fn rpc_failure(error: RpcError) -> CallFailure {
    match error {
        RpcError::Refused => CallFailure::SignedOut("the MCP server refused the token".into()),
        RpcError::Transient(why) => CallFailure::Transient(why),
        RpcError::Other(why) => CallFailure::Other(why),
    }
}

fn access_failure_cli(entry: &Entry, error: AccessError) -> CallFailure {
    match error {
        AccessError::SignedOut(label) => CallFailure::SignedOut(format!(
            "{} MCP {label}: sign in with `router-acp mcp-login {}`",
            entry.name, entry.name
        )),
        AccessError::Unavailable(why) => CallFailure::Transient(why),
    }
}

/// One MCP session in which the upstream runs `tools/call`.
async fn call_once(
    http: &reqwest::Client,
    url: &str,
    token: &str,
    tool: &str,
    arguments: &Value,
    timeout: Duration,
) -> Result<ToolCall, RpcError> {
    let mut rpc = Rpc {
        http,
        url,
        token,
        session: None,
    };
    let init = rpc.post(&initialize_request(), timeout).await?;
    if init.as_ref().and_then(|m| m.get("result")).is_none() {
        return Err(RpcError::Other("MCP initialize returned no result".into()));
    }
    rpc.post(
        &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        timeout,
    )
    .await?;
    let reply = rpc
        .post(
            &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": tool, "arguments": arguments}}),
            timeout,
        )
        .await?;
    let message =
        reply.ok_or_else(|| RpcError::Other("MCP tools/call returned no result".into()))?;
    if let Some(error) = message.get("error") {
        let code = error
            .get("code")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        return Err(RpcError::Other(format!(
            "MCP tools/call failed (JSON-RPC error {code})"
        )));
    }
    let result = message
        .get("result")
        .cloned()
        .ok_or_else(|| RpcError::Other("MCP tools/call returned no result".into()))?;
    let is_error = result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(ToolCall { result, is_error })
}

/// `router-acp mcp-call`: one `tools/call` on a project server, through the
/// router's own sign-in. The whole call is bounded by `timeout`.
pub async fn call_tool(
    root: &FsPath,
    helper: Option<crate::config::McpCredentialCommand>,
    server: &RemoteServer,
    tool: &str,
    arguments: &Value,
    timeout: Duration,
) -> Result<ToolCall, CallFailure> {
    let broker = Broker::new(root.to_path_buf(), helper);
    let entry = broker.entry_for(server);
    match tokio::time::timeout(timeout, broker.call(&entry, tool, arguments, timeout)).await {
        Ok(outcome) => outcome,
        Err(_) => Err(CallFailure::Transient(format!(
            "timed out after {} ms",
            timeout.as_millis()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_oauth::tests::{serve, server, signed_in, token_endpoint, tokens};
    use axum::routing::post;
    use std::sync::Mutex;

    /// (body, Authorization, Mcp-Session-Id) per request.
    type Seen = Arc<Mutex<Vec<(String, Option<String>, Option<String>)>>>;

    #[derive(Clone, Default)]
    struct Upstream {
        seen: Seen,
        /// Bearers the fake upstream refuses.
        refuse: Arc<Mutex<Vec<String>>>,
    }

    /// Port of Kory's fake MCP server, over streamable HTTP: records the
    /// Authorization and session headers, answers JSON-RPC by method.
    async fn fake_upstream() -> (String, Upstream) {
        let state = Upstream::default();
        let app = Router::new()
            .route(
                "/mcp",
                post(
                    |State(state): State<Upstream>, headers: HeaderMap, body: String| async move {
                        let auth = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .map(String::from);
                        let sid = headers
                            .get("mcp-session-id")
                            .and_then(|v| v.to_str().ok())
                            .map(String::from);
                        state.seen.lock().unwrap().push((body.clone(), auth.clone(), sid));
                        let refused = auth.as_deref().is_some_and(|a| {
                            state.refuse.lock().unwrap().iter().any(|r| a == format!("Bearer {r}"))
                        });
                        if refused {
                            return Response::builder()
                                .status(401)
                                .header("www-authenticate", "Bearer resource_metadata=x")
                                .body(Body::from("{}"))
                                .unwrap();
                        }
                        let message: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                        let reply = match message["method"].as_str() {
                            Some("initialize") => json!({"jsonrpc":"2.0","id":message["id"],
                                "result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fake"}}}),
                            Some("tools/list") => json!({"jsonrpc":"2.0","id":message["id"],
                                "result":{"tools":[{"name":"search"},{"name":"get"}]}}),
                            // `sse` answers as an event stream, `broken` as a tool error.
                            Some("tools/call") if message["params"]["name"] == "broken" => {
                                json!({"jsonrpc":"2.0","id":message["id"],"result":{
                                    "content":[{"type":"text","text":"no such monitor"}],"isError":true}})
                            }
                            Some("tools/call") => json!({"jsonrpc":"2.0","id":message["id"],"result":{
                                "content":[{"type":"text","text":"2 hits"}],"isError":false}}),
                            _ => json!({"jsonrpc":"2.0","id":message["id"],"result":{}}),
                        };
                        let (content_type, text) = if message["params"]["name"] == "sse" {
                            ("text/event-stream", format!(
                                "event: message\ndata: {{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{{}}}}\n\ndata: {reply}\n\n"))
                        } else {
                            ("application/json", reply.to_string())
                        };
                        Response::builder()
                            .status(200)
                            .header("content-type", content_type)
                            .header("mcp-session-id", "upstream-session")
                            .body(Body::from(text))
                            .unwrap()
                    },
                ),
            )
            .with_state(state.clone());
        (format!("{}/mcp", serve(app).await), state)
    }

    /// A project whose `.mcp.json` names `fake` → `upstream_url`, plus a
    /// stdio entry the broker must leave alone.
    pub(crate) fn project(upstream_url: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(
            dir.path().join(".mcp.json"),
            json!({"mcpServers": {"fake": {"type": "http", "url": upstream_url},
                "local": {"command": "true"}}})
            .to_string(),
        )
        .unwrap();
        dir
    }

    async fn broker_for(
        upstream_url: &str,
        token_url: &str,
        root: &std::path::Path,
        stored: crate::mcp_oauth::Tokens,
    ) -> (Arc<Broker>, tempfile::TempDir) {
        signed_in(root, &server("fake", upstream_url), token_url, &stored);
        let broker = Broker::new(root.to_path_buf(), None);
        broker.bind().await.unwrap();
        (broker, project(upstream_url))
    }

    fn entry(broker: &Broker, project: &tempfile::TempDir) -> (String, String) {
        let servers = broker.session_servers(project.path(), None);
        let [McpServer::Http(http)] = servers.as_slice() else {
            panic!("one http entry: {servers:?}");
        };
        assert_eq!(http.name, "fake");
        (http.url.clone(), http.headers[0].value.clone())
    }

    fn only_source(broker: &Broker) -> Arc<TokenSource> {
        broker
            .sources
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .clone()
    }

    #[tokio::test]
    async fn sessions_get_only_signed_in_project_servers() {
        let (upstream_url, _) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        let broker = Broker::new(root.path().to_path_buf(), None);
        let dir = project(&upstream_url);
        assert!(
            broker.session_servers(dir.path(), None).is_empty(),
            "unbound: nothing"
        );
        broker.bind().await.unwrap();
        assert!(
            broker.session_servers(dir.path(), None).is_empty(),
            "not signed in: left to adapters"
        );
        signed_in(
            root.path(),
            &server("fake", &upstream_url),
            "http://127.0.0.1:1/token",
            &tokens("t", 3_600_000),
        );
        assert_eq!(broker.session_servers(dir.path(), None).len(), 1);
        let elsewhere = tempfile::tempdir().unwrap();
        std::fs::create_dir(elsewhere.path().join(".git")).unwrap();
        assert!(
            broker.session_servers(elsewhere.path(), None).is_empty(),
            "no project file: nothing"
        );
    }

    #[tokio::test]
    async fn the_proxy_requires_its_bearer_and_injects_the_stored_token() {
        let (upstream_url, upstream) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        let (broker, dir) = broker_for(
            &upstream_url,
            "http://127.0.0.1:1/token",
            root.path(),
            tokens("stored-access", 3_600_000),
        )
        .await;
        let (url, bearer) = entry(&broker, &dir);
        assert!(url.starts_with("http://127.0.0.1:"), "{url}");
        assert!(!url.contains(&upstream_url), "{url}");
        assert!(
            !bearer.contains("stored-access"),
            "sessions never see the upstream token"
        );
        let http = reqwest::Client::new();
        let init = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}});

        let denied = http.post(&url).json(&init).send().await.unwrap();
        assert_eq!(denied.status(), 401);
        let wrong = http
            .post(&url)
            .header("authorization", "Bearer nope")
            .json(&init)
            .send()
            .await
            .unwrap();
        assert_eq!(wrong.status(), 401);
        assert!(
            upstream.seen.lock().unwrap().is_empty(),
            "nothing reaches the upstream unauthenticated"
        );

        let ok = http
            .post(&url)
            .header("authorization", &bearer)
            .header("mcp-session-id", "client-session")
            .json(&init)
            .send()
            .await
            .unwrap();
        assert_eq!(ok.status(), 200);
        assert_eq!(ok.headers()["mcp-session-id"], "upstream-session");
        let seen = upstream.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].1.as_deref(), Some("Bearer stored-access"));
        assert_eq!(seen[0].2.as_deref(), Some("client-session"));
        assert_eq!(
            probe_tools(&http, &upstream_url, "stored-access").await,
            Ok(2)
        );
    }

    /// One `tools/call` through the broker's entry for `session`.
    async fn call_through(
        broker: &Broker,
        dir: &tempfile::TempDir,
        session: &str,
        tool: &str,
    ) -> Value {
        let servers = broker.session_servers(dir.path(), Some(session));
        let [McpServer::Http(http)] = servers.as_slice() else {
            panic!("one http entry: {servers:?}");
        };
        reqwest::Client::new()
            .post(&http.url)
            .header("authorization", &http.headers[0].value)
            .json(&json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":tool,"arguments":{}}}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    fn completed(id: &str, title: Option<&str>, content: Value) -> Value {
        let mut update = json!({"sessionUpdate":"tool_call_update","toolCallId":id,
            "status":"completed","content":content});
        if let Some(title) = title {
            update["title"] = json!(title);
        }
        update
    }

    #[tokio::test]
    async fn an_empty_completed_tool_update_shows_the_result_the_broker_carried() {
        let (upstream_url, _) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        let (broker, dir) = broker_for(
            &upstream_url,
            "http://127.0.0.1:1/token",
            root.path(),
            tokens("t", 3_600_000),
        )
        .await;
        // The title arrives on the announcement; the update omits it.
        let mut announce = json!({"sessionUpdate":"tool_call","toolCallId":"c1",
            "title":"mcp__fake__search","status":"pending"});
        assert!(!broker.fill_tool_result("s1", &mut announce));
        call_through(&broker, &dir, "s1", "search").await;
        // Another session's update never takes this session's result.
        let mut other = completed("c1", Some("mcp__fake__search"), json!([]));
        assert!(!broker.fill_tool_result("s2", &mut other));
        let mut update = completed("c1", None, json!([]));
        assert!(broker.fill_tool_result("s1", &mut update));
        assert_eq!(update["content"][0]["content"]["text"], "2 hits");
        // Used once: a second empty update for the same tool stays empty.
        let mut again = completed("c2", Some("fake__search"), json!([]));
        assert!(!broker.fill_tool_result("s1", &mut again));
    }

    #[tokio::test]
    async fn a_reported_tool_result_is_never_replaced() {
        let (upstream_url, _) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        let (broker, dir) = broker_for(
            &upstream_url,
            "http://127.0.0.1:1/token",
            root.path(),
            tokens("t", 3_600_000),
        )
        .await;
        call_through(&broker, &dir, "s1", "search").await;
        let own = json!([{"type":"content","content":{"type":"text","text":"agent's own"}}]);
        let mut update = completed("c1", Some("search"), own.clone());
        assert!(!broker.fill_tool_result("s1", &mut update));
        assert_eq!(update["content"], own);
        // ...and it consumed the capture, so a later empty one stays empty.
        let mut later = completed("c2", Some("search"), Value::Null);
        assert!(!broker.fill_tool_result("s1", &mut later));
    }

    #[tokio::test]
    async fn a_helper_token_is_refused_for_the_tools_it_denies() {
        let (upstream_url, upstream) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        let scripts = tempfile::tempdir().unwrap();
        let supplied = helper(
            scripts.path(),
            r#"echo '{"token":"box-jwt","denyTools":["mail_*","compose_brief"]}'"#,
        );
        let broker = Broker::new(root.path().to_path_buf(), Some(supplied.clone()));
        broker.bind().await.unwrap();
        let dir = project(&upstream_url);
        let refused = call_through(&broker, &dir, "s1", "mail_send").await;
        assert_eq!(refused["id"], 7);
        assert_eq!(refused["result"]["isError"], true);
        assert!(
            refused["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("needs your own sign-in"),
            "{refused}"
        );
        assert!(
            !upstream
                .seen
                .lock()
                .unwrap()
                .iter()
                .any(|s| s.0.contains("mail_send")),
            "a denied tool never reaches the upstream"
        );
        let allowed = call_through(&broker, &dir, "s1", "search").await;
        assert_eq!(allowed["result"]["content"][0]["text"], "2 hits");
        for (tool, ok) in [("compose_brief", false), ("search", true)] {
            let outcome = call_tool(
                root.path(),
                Some(supplied.clone()),
                &server("fake", &upstream_url),
                tool,
                &json!({}),
                Duration::from_secs(5),
            )
            .await;
            assert_eq!(outcome.is_ok(), ok, "{tool}");
        }
    }

    #[tokio::test]
    async fn the_routers_own_sign_in_ignores_a_helpers_deny_list() {
        let (upstream_url, _) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        signed_in(
            root.path(),
            &server("fake", &upstream_url),
            "http://127.0.0.1:1/token",
            &tokens("mine", 3_600_000),
        );
        let scripts = tempfile::tempdir().unwrap();
        let supplied = helper(
            scripts.path(),
            r#"echo '{"token":"box-jwt","denyTools":["*"]}'"#,
        );
        let broker = Broker::new(root.path().to_path_buf(), Some(supplied));
        broker.bind().await.unwrap();
        let dir = project(&upstream_url);
        let answer = call_through(&broker, &dir, "s1", "mail_send").await;
        assert_eq!(answer["result"]["content"][0]["text"], "2 hits");
    }

    #[tokio::test]
    async fn a_rejected_bearer_is_refreshed_and_retried_once() {
        let (upstream_url, upstream) = fake_upstream().await;
        upstream.refuse.lock().unwrap().push("old".into());
        let (token_url, calls) =
            token_endpoint((200, json!({"access_token": "new", "expires_in": 3600}))).await;
        let root = tempfile::tempdir().unwrap();
        let (broker, dir) = broker_for(
            &upstream_url,
            &token_url,
            root.path(),
            tokens("old", 3_600_000),
        )
        .await;
        let (url, bearer) = entry(&broker, &dir);
        let http = reqwest::Client::new();
        let response = http
            .post(&url)
            .header("authorization", &bearer)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(response.headers().get("www-authenticate").is_none());
        assert_eq!(calls.lock().unwrap().len(), 1);
        let auths: Vec<_> = upstream
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.1.clone().unwrap())
            .collect();
        assert_eq!(auths, vec!["Bearer old", "Bearer new"]);

        // The replacement is refused too: reconnect needed, and later sessions omit it.
        upstream.refuse.lock().unwrap().push("new".into());
        let (token_url2, _) =
            token_endpoint((200, json!({"access_token": "newer", "expires_in": 3600}))).await;
        let source = only_source(&broker);
        let mut client = source.store.client().unwrap();
        client.endpoints.token = token_url2;
        source.store.write_client(&client).unwrap();
        upstream.refuse.lock().unwrap().push("newer".into());
        let refused = http
            .post(&url)
            .header("authorization", &bearer)
            .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), 401);
        assert_eq!(source.status().state, StatusState::SignedOut);
        assert!(broker.session_servers(dir.path(), None).is_empty());
        let after = http
            .post(&url)
            .header("authorization", &bearer)
            .json(&json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}))
            .send()
            .await
            .unwrap();
        assert_eq!(after.status(), 503);
        let text = after.text().await.unwrap();
        assert!(text.contains("router-acp mcp-login fake"), "{text}");
        assert!(!text.contains("newer"), "{text}");
    }

    #[tokio::test]
    async fn server_discover_is_answered_locally() {
        let (upstream_url, upstream) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        let (broker, dir) = broker_for(
            &upstream_url,
            "http://127.0.0.1:1/token",
            root.path(),
            tokens("t", 3_600_000),
        )
        .await;
        let (url, bearer) = entry(&broker, &dir);
        let reply: Value = reqwest::Client::new()
            .post(&url)
            .header("authorization", &bearer)
            .json(&json!({"jsonrpc":"2.0","id":7,"method":"server/discover"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(reply["id"], 7);
        assert_eq!(reply["result"]["supportedVersions"][0], "2025-11-25");
        assert!(upstream.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn auth_rejection_is_read_from_status_json_and_sse_frames() {
        assert!(response_rejects_auth(StatusCode::UNAUTHORIZED, None));
        assert!(response_rejects_auth(StatusCode::FORBIDDEN, None));
        assert!(!response_rejects_auth(
            StatusCode::OK,
            Some(b"{\"result\":{}}")
        ));
        assert!(response_rejects_auth(
            StatusCode::OK,
            Some(br#"{"error":{"code":-32001,"message":"Token has expired"}}"#)
        ));
        let sse = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"isError\":true,\"content\":[{\"type\":\"text\",\"text\":\"requires re-authorization\"}]}}\n\n";
        assert!(response_rejects_auth(StatusCode::OK, Some(sse)));
        let other = b"data: {\"result\":{\"isError\":true,\"content\":[{\"text\":\"no such monitor\"}]}}\n\n";
        assert!(!response_rejects_auth(StatusCode::OK, Some(other)));
    }

    #[test]
    fn broker_entries_replace_same_named_client_servers() {
        let mut servers = vec![
            McpServer::Http(McpServerHttp::new(
                "datadog",
                "https://mcp.datadoghq.com/v1/mcp",
            )),
            McpServer::Http(McpServerHttp::new("other", "https://other.test/mcp")),
        ];
        merge_authoritative(
            &mut servers,
            vec![McpServer::Http(McpServerHttp::new(
                "datadog",
                "http://127.0.0.1:9/mcp/datadog",
            ))],
        );
        let urls: Vec<_> = servers
            .iter()
            .map(|s| match s {
                McpServer::Http(h) => h.url.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(
            urls,
            vec!["https://other.test/mcp", "http://127.0.0.1:9/mcp/datadog"]
        );
    }

    #[tokio::test]
    async fn mcp_call_returns_the_tool_result_from_json_and_sse_upstreams() {
        let (upstream_url, upstream) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        signed_in(
            root.path(),
            &server("fake", &upstream_url),
            "http://127.0.0.1:1/token",
            &tokens("live", 3_600_000),
        );
        for tool in ["search", "sse"] {
            let call = call_tool(
                root.path(),
                None,
                &server("fake", &upstream_url),
                tool,
                &json!({"q": "x"}),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert!(!call.is_error);
            assert_eq!(call.result["content"][0]["text"], "2 hits");
        }
        let seen = upstream.seen.lock().unwrap().clone();
        let calls: Vec<_> = seen.iter().filter(|s| s.0.contains("tools/call")).collect();
        assert_eq!(calls.len(), 2);
        assert!(
            calls[0].0.contains(r#""arguments":{"q":"x"}"#),
            "{}",
            calls[0].0
        );
        assert_eq!(calls[0].1.as_deref(), Some("Bearer live"));
        assert_eq!(calls[0].2.as_deref(), Some("upstream-session"));
    }

    #[tokio::test]
    async fn mcp_call_refreshes_and_retries_once_on_a_refused_bearer() {
        let (upstream_url, upstream) = fake_upstream().await;
        upstream.refuse.lock().unwrap().push("old".into());
        let (token_url, calls) =
            token_endpoint((200, json!({"access_token": "new", "expires_in": 3600}))).await;
        let root = tempfile::tempdir().unwrap();
        signed_in(
            root.path(),
            &server("fake", &upstream_url),
            &token_url,
            &tokens("old", 3_600_000),
        );
        let call = call_tool(
            root.path(),
            None,
            &server("fake", &upstream_url),
            "search",
            &json!({}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(call.result["content"][0]["text"], "2 hits");
        assert_eq!(calls.lock().unwrap().len(), 1);
        let auths: Vec<_> = upstream
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.1.clone().unwrap())
            .collect();
        assert_eq!(auths.first().map(String::as_str), Some("Bearer old"));
        assert_eq!(auths.last().map(String::as_str), Some("Bearer new"));
    }

    #[tokio::test]
    async fn mcp_call_marks_reconnect_needed_when_the_refreshed_bearer_is_refused() {
        let (upstream_url, upstream) = fake_upstream().await;
        upstream
            .refuse
            .lock()
            .unwrap()
            .extend(["old".into(), "new".into()]);
        let (token_url, _) =
            token_endpoint((200, json!({"access_token": "new", "expires_in": 3600}))).await;
        let root = tempfile::tempdir().unwrap();
        let store = signed_in(
            root.path(),
            &server("fake", &upstream_url),
            &token_url,
            &tokens("old", 3_600_000),
        );
        let failure = call_tool(
            root.path(),
            None,
            &server("fake", &upstream_url),
            "search",
            &json!({}),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
        assert_eq!(failure.exit_code(), 2);
        assert!(failure.message().contains("router-acp mcp-login fake"));
        assert!(!failure.message().contains("new"));
        assert_eq!(
            crate::mcp_oauth::status(&store).state,
            StatusState::SignedOut
        );
    }

    #[tokio::test]
    async fn mcp_call_without_a_sign_in_or_a_reachable_server_has_its_exit_status() {
        let root = tempfile::tempdir().unwrap();
        let unsigned = call_tool(
            root.path(),
            None,
            &server("fake", "http://127.0.0.1:1/mcp"),
            "search",
            &json!({}),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
        assert_eq!(unsigned.exit_code(), 2);
        assert!(
            unsigned.message().contains("not signed in"),
            "{}",
            unsigned.message()
        );

        signed_in(
            root.path(),
            &server("fake", "http://127.0.0.1:1/mcp"),
            "http://127.0.0.1:1/token",
            &tokens("live", 3_600_000),
        );
        let unreachable = call_tool(
            root.path(),
            None,
            &server("fake", "http://127.0.0.1:1/mcp"),
            "search",
            &json!({}),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
        assert_eq!(unreachable.exit_code(), 3);
    }

    #[tokio::test]
    async fn mcp_call_returns_a_tool_error_as_a_result_for_exit_status_four() {
        let (upstream_url, _) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        signed_in(
            root.path(),
            &server("fake", &upstream_url),
            "http://127.0.0.1:1/token",
            &tokens("live", 3_600_000),
        );
        let call = call_tool(
            root.path(),
            None,
            &server("fake", &upstream_url),
            "broken",
            &json!({}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert!(call.is_error);
        assert_eq!(call.result["content"][0]["text"], "no such monitor");
    }

    /// A credential helper script: prints `answer` (or sleeps, or declines).
    fn helper(dir: &FsPath, body: &str) -> crate::config::McpCredentialCommand {
        let path = dir.join("helper.sh");
        std::fs::write(
            &path,
            format!("#!/bin/sh\ncat > {}/request.json\n{body}\n", dir.display()),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        crate::config::McpCredentialCommand {
            command: path.display().to_string(),
            args: Vec::new(),
            timeout_ms: 2_000,
            servers: Vec::new(),
        }
    }

    #[tokio::test]
    async fn a_helper_token_serves_a_server_the_router_never_signed_in_to() {
        let (upstream_url, upstream) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        let scripts = tempfile::tempdir().unwrap();
        let supplied = helper(scripts.path(), r#"echo '{"token":"box-jwt"}'"#);
        let call = call_tool(
            root.path(),
            Some(supplied),
            &server("fake", &upstream_url),
            "search",
            &json!({}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(call.result["content"][0]["text"], "2 hits");
        let calls: Vec<_> = upstream
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.1.clone())
            .collect();
        assert!(
            calls.iter().all(|a| a.as_deref() == Some("Bearer box-jwt")),
            "{calls:?}"
        );
        let request: Value = serde_json::from_str(
            &std::fs::read_to_string(scripts.path().join("request.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(request["name"], "fake");
        assert_eq!(request["url"], upstream_url);
    }

    #[tokio::test]
    async fn a_declining_or_silent_helper_leaves_the_server_signed_out() {
        let root = tempfile::tempdir().unwrap();
        for body in ["exit 1", "sleep 5"] {
            let scripts = tempfile::tempdir().unwrap();
            let mut supplied = helper(scripts.path(), body);
            supplied.timeout_ms = 300;
            let failure = call_tool(
                root.path(),
                Some(supplied),
                &server("fake", "http://127.0.0.1:1/mcp"),
                "search",
                &json!({}),
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();
            assert_eq!(failure.exit_code(), 2, "{body}");
        }
    }

    #[tokio::test]
    async fn the_routers_own_sign_in_wins_over_the_helper() {
        let (upstream_url, upstream) = fake_upstream().await;
        let root = tempfile::tempdir().unwrap();
        signed_in(
            root.path(),
            &server("fake", &upstream_url),
            "http://127.0.0.1:1/token",
            &tokens("live", 3_600_000),
        );
        let scripts = tempfile::tempdir().unwrap();
        let supplied = helper(scripts.path(), r#"echo '{"token":"box-jwt"}'"#);
        call_tool(
            root.path(),
            Some(supplied),
            &server("fake", &upstream_url),
            "search",
            &json!({}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let calls: Vec<_> = upstream
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.1.clone())
            .collect();
        assert!(
            calls.iter().all(|a| a.as_deref() == Some("Bearer live")),
            "{calls:?}"
        );
        assert!(
            !scripts.path().join("request.json").exists(),
            "the helper is never asked"
        );
    }

    #[tokio::test]
    async fn a_refused_helper_token_is_asked_for_again_once() {
        let (upstream_url, upstream) = fake_upstream().await;
        upstream.refuse.lock().unwrap().push("stale".into());
        let root = tempfile::tempdir().unwrap();
        let scripts = tempfile::tempdir().unwrap();
        let count = scripts.path().join("count");
        let supplied = helper(
            scripts.path(),
            &format!(
                r#"if [ -f {c} ]; then echo '{{"token":"fresh"}}'; else touch {c}; echo '{{"token":"stale"}}'; fi"#,
                c = count.display()
            ),
        );
        let call = call_tool(
            root.path(),
            Some(supplied),
            &server("fake", &upstream_url),
            "search",
            &json!({}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(call.result["content"][0]["text"], "2 hits");
        let last = upstream
            .seen
            .lock()
            .unwrap()
            .last()
            .and_then(|s| s.1.clone());
        assert_eq!(last.as_deref(), Some("Bearer fresh"));
    }

    #[tokio::test]
    async fn a_helper_serves_only_the_servers_it_names() {
        let root = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        std::fs::write(
            project.path().join(".mcp.json"),
            r#"{"mcpServers":{"kory":{"type":"http","url":"https://kory.example/mcp"},
                "datadog":{"type":"http","url":"https://mcp.datadog.example/v1/mcp"}}}"#,
        )
        .unwrap();
        let scripts = tempfile::tempdir().unwrap();
        let mut supplied = helper(scripts.path(), r#"echo '{"token":"box-jwt"}'"#);
        supplied.servers = vec!["kory".into()];
        let broker = Broker::new(root.path().to_path_buf(), Some(supplied.clone()));
        broker.bind().await.unwrap();
        let names: Vec<String> = broker
            .session_servers(project.path(), None)
            .iter()
            .map(|s| match s {
                McpServer::Http(h) => h.name.clone(),
                _ => unreachable!(),
            })
            .collect();
        // A server nobody signed in to stays off sessions unless the helper names it.
        assert_eq!(names, vec!["kory"]);
        let status =
            crate::mcp_oauth::project_status_json(project.path(), root.path(), Some(&supplied))
                .await;
        let state = |name: &str| {
            status["servers"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["name"] == name)
                .unwrap()["state"]
                .clone()
        };
        assert_eq!(state("kory"), "signed-in");
        assert_eq!(state("datadog"), "signed-out");
    }

    #[tokio::test]
    async fn sessions_get_a_helper_served_server_and_status_says_provided() {
        let root = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        std::fs::write(
            project.path().join(".mcp.json"),
            r#"{"mcpServers":{"kory":{"type":"http","url":"https://kory.example/mcp"}}}"#,
        )
        .unwrap();
        let scripts = tempfile::tempdir().unwrap();
        let supplied = helper(scripts.path(), r#"echo '{"token":"box-jwt"}'"#);
        let broker = Broker::new(root.path().to_path_buf(), Some(supplied.clone()));
        broker.bind().await.unwrap();
        assert_eq!(broker.session_servers(project.path(), None).len(), 1);
        let status =
            crate::mcp_oauth::project_status_json(project.path(), root.path(), Some(&supplied))
                .await;
        assert_eq!(status["servers"][0]["state"], "signed-in");
        assert_eq!(status["servers"][0]["label"], "provided credential");
        let own = crate::mcp_oauth::project_status_json(project.path(), root.path(), None).await;
        assert_eq!(own["servers"][0]["state"], "signed-out");
    }
}
