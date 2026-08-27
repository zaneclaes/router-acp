//! Per-LLM-request HTTP interposition and routing.
//!
//! Adapter base URLs are process-scoped, while ACP sessions are not. The
//! router therefore attributes an inference request to the active downstream
//! prompt on that process. A request is rewritten only when attribution is
//! unambiguous (or an adapter-provided session marker identifies one active
//! prompt); ambiguous traffic passes through unchanged.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, Request, Response, StatusCode, Uri};
use axum::routing::any;
use futures::StreamExt;
#[cfg(test)]
use serde_json::value::RawValue;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::candidate::{CandidateId, TaskClass};
use crate::config::{AgentLlmProxyConfig, LlmWireProtocol};
use crate::downstream::{ProcessKey, ProcessTargetSpec};
use crate::session::Shared;
use crate::state::{LlmRequestStart, LlmRequestUsage, LogEntry};

#[derive(Debug, Clone)]
struct ProxyTarget {
    key: ProcessKey,
    agent: String,
    config: AgentLlmProxyConfig,
}

#[derive(Debug, Clone)]
struct ActiveTurn {
    registration_id: u64,
    parent_router_sid: String,
    state_sid: String,
    downstream_sid: String,
    candidate: CandidateId,
    class: TaskClass,
    automation: bool,
}

#[derive(Debug, Default)]
struct RequestPolicyState {
    pinned_candidate: Option<CandidateId>,
    request_seq: u64,
    current_model: Option<String>,
    requests_on_model: u32,
    routine_streak: u32,
    elevated_until_request: u64,
    elevated_until_time: Option<Instant>,
    pending_difficulty: Option<String>,
    last_test_fingerprint: Option<String>,
    repeated_test_output: u32,
    /// Tool-result count seen on the previous request, so a repeated fingerprint
    /// is only judged "stuck" when the agent actually acted again.
    last_tool_result_count: usize,
    /// Upstream api model values the provider rejected outright (a 404 means the
    /// configured `api_model` is not a model id it serves). Remembered so the
    /// rewrite is not re-attempted every request — each attempt costs a wasted
    /// round trip and is logged as a non-delegation, making it invisible.
    rejected_api_models: std::collections::HashSet<String>,
    /// `{event}:{model}` pairs already disclosed to the user this session, so an
    /// anomaly is surfaced once rather than on every request that hits it.
    disclosed_events: std::collections::HashSet<String>,
    /// Candidate (`agent/model`) and reason of the most recent decision, read by
    /// the relay path to attribute each tool call to the model that produced it.
    /// This is the structured replacement for the per-request prose disclosure:
    /// the client renders it inside the tool-call card instead of as chat text.
    last_candidate: Option<String>,
    last_reason: Option<String>,
}

/// Shared proxy runtime. Constructed with `Shared`, bound when `serve_shared`
/// starts, and consulted by downstream process creation and prompt dispatch.
pub struct LlmProxyRuntime {
    enabled: bool,
    listen_addr: OnceLock<std::net::SocketAddr>,
    targets_by_token: HashMap<String, ProxyTarget>,
    token_by_key: HashMap<ProcessKey, String>,
    active: Mutex<HashMap<ProcessKey, Vec<ActiveTurn>>>,
    policy: Mutex<HashMap<String, RequestPolicyState>>,
    next_registration: AtomicU64,
    client: reqwest::Client,
}

impl std::fmt::Debug for LlmProxyRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmProxyRuntime")
            .field("enabled", &self.enabled)
            .field("listen_addr", &self.listen_addr.get())
            .field("targets", &self.targets_by_token.len())
            .finish()
    }
}

impl LlmProxyRuntime {
    pub fn new(
        cfg: &crate::config::Config,
        specs: &[ProcessTargetSpec],
    ) -> Result<Arc<Self>, String> {
        let mut targets_by_token = HashMap::new();
        let mut token_by_key = HashMap::new();
        for spec in specs {
            let Some(agent) = cfg.agents.iter().find(|a| a.name == spec.agent_name) else {
                continue;
            };
            let Some(proxy) = agent.llm_proxy.clone() else {
                continue;
            };
            let token = target_token(&spec.key);
            targets_by_token.insert(
                token.clone(),
                ProxyTarget {
                    key: spec.key.clone(),
                    agent: agent.name.clone(),
                    config: proxy,
                },
            );
            token_by_key.insert(spec.key.clone(), token);
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("cannot build proxy HTTP client: {e}"))?;
        Ok(Arc::new(Self {
            enabled: cfg.llm_proxy.enabled && !targets_by_token.is_empty(),
            listen_addr: OnceLock::new(),
            targets_by_token,
            token_by_key,
            active: Mutex::new(HashMap::new()),
            policy: Mutex::new(HashMap::new()),
            next_registration: AtomicU64::new(1),
            client,
        }))
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Bind the loopback listener. A bind failure is returned to the caller,
    /// which leaves adapter environments untouched (ACP-turn routing remains).
    pub async fn bind(
        self: &Arc<Self>,
        shared: Arc<Shared>,
    ) -> Result<tokio::task::JoinHandle<()>, String> {
        if !self.enabled {
            return Err("LLM proxy is disabled".to_string());
        }
        let listener = tokio::net::TcpListener::bind(&shared.cfg.llm_proxy.listen)
            .await
            .map_err(|e| {
                format!(
                    "cannot bind LLM proxy at {}: {e}",
                    shared.cfg.llm_proxy.listen
                )
            })?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("cannot read LLM proxy address: {e}"))?;
        self.listen_addr
            .set(addr)
            .map_err(|_| "LLM proxy listener was already bound".to_string())?;
        let state = ProxyServerState {
            shared,
            runtime: self.clone(),
        };
        let app = Router::new()
            .route("/proxy/{token}", any(proxy_root))
            .route("/proxy/{token}/{*rest}", any(proxy_with_path))
            .with_state(state);
        tracing::info!(%addr, targets = self.targets_by_token.len(), "LLM request proxy listening");
        Ok(tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app).await {
                tracing::warn!(%err, "LLM request proxy stopped");
            }
        }))
    }

    /// Apply the adapter's process-level base URL override after the listener
    /// has bound. If no listener is available, return the original env.
    pub fn process_env(&self, spec: &ProcessTargetSpec) -> Vec<(String, String)> {
        let mut env = spec.env.clone();
        let Some(addr) = self.listen_addr.get() else {
            return env;
        };
        let Some(token) = self.token_by_key.get(&spec.key) else {
            return env;
        };
        let Some(target) = self.targets_by_token.get(token) else {
            return env;
        };
        let Ok(upstream) = reqwest::Url::parse(&target.config.upstream_base_url) else {
            return env;
        };
        let upstream_path = upstream.path().trim_end_matches('/');
        let proxy_base = format!("http://{addr}/proxy/{token}{upstream_path}");
        env.retain(|(name, _)| name != &target.config.base_url_env);
        env.push((target.config.base_url_env.clone(), proxy_base.clone()));
        if target.config.codex_chatgpt_provider {
            env = codex_proxy_env(env, &proxy_base);
        }
        if target.config.protocol == LlmWireProtocol::Anthropic {
            env = anthropic_proxy_env(env);
        }
        tracing::info!(
            target = %spec.key,
            env = target.config.base_url_env,
            base_url = proxy_base,
            upstream = target.config.upstream_base_url,
            "interposing adapter inference traffic"
        );
        env
    }

    #[allow(clippy::too_many_arguments)]
    pub fn begin_turn(
        self: &Arc<Self>,
        process_key: ProcessKey,
        parent_router_sid: String,
        state_sid: String,
        downstream_sid: String,
        candidate: CandidateId,
        class: TaskClass,
        meta: Option<&agent_client_protocol::schema::v1::Meta>,
    ) -> LlmTurnGuard {
        if !self.enabled || !self.token_by_key.contains_key(&process_key) {
            return LlmTurnGuard {
                runtime: None,
                process_key,
                registration_id: 0,
            };
        }
        // A main-session repin can change providers before the new adapter
        // emits its first inference request. Reset attribution here, not lazily
        // in `select_request_model`, so tool frames can never combine the new
        // agent with a stale model from the prior provider.
        {
            let mut policy = self.policy.lock().unwrap();
            let state = policy.entry(state_sid.clone()).or_default();
            if state.pinned_candidate.as_ref() != Some(&candidate) {
                let rejected = std::mem::take(&mut state.rejected_api_models);
                *state = RequestPolicyState {
                    pinned_candidate: Some(candidate.clone()),
                    current_model: Some(candidate.model.clone()),
                    last_candidate: Some(candidate.to_string()),
                    last_reason: Some("session-pinned model; awaiting inference decision".into()),
                    rejected_api_models: rejected,
                    ..Default::default()
                };
            } else if state.last_candidate.is_none() {
                state.last_candidate = Some(candidate.to_string());
                state.last_reason =
                    Some("session-pinned model; awaiting inference decision".into());
            }
        }
        let registration_id = self.next_registration.fetch_add(1, Ordering::Relaxed);
        let active = ActiveTurn {
            registration_id,
            parent_router_sid,
            state_sid,
            downstream_sid,
            candidate,
            class,
            automation: automation_hint(meta),
        };
        self.active
            .lock()
            .unwrap()
            .entry(process_key.clone())
            .or_default()
            .push(active);
        LlmTurnGuard {
            runtime: Some(self.clone()),
            process_key,
            registration_id,
        }
    }

    /// Candidate (`agent/model`) and reason of the most recent routing decision
    /// for a session, for attributing a tool call to the model that produced it.
    /// `None` before the first inference request, which the client renders as a
    /// pending state rather than a wrong model.
    pub fn last_attribution(&self, state_sid: &str) -> Option<(String, String)> {
        let policy = self.policy.lock().unwrap();
        let state = policy.get(state_sid)?;
        let candidate = state.last_candidate.clone()?;
        Some((candidate, state.last_reason.clone().unwrap_or_default()))
    }

    pub fn current_model(&self, state_sid: &str) -> Option<String> {
        self.policy
            .lock()
            .unwrap()
            .get(state_sid)
            .and_then(|s| s.current_model.clone())
    }

    fn unregister(&self, key: &ProcessKey, registration_id: u64) {
        let mut active = self.active.lock().unwrap();
        if let Some(turns) = active.get_mut(key) {
            turns.retain(|t| t.registration_id != registration_id);
            if turns.is_empty() {
                active.remove(key);
            }
        }
    }

    fn attribute(&self, key: &ProcessKey, body: &Value) -> Option<ActiveTurn> {
        let active = self.active.lock().unwrap();
        let turns = active.get(key)?;
        if turns.len() == 1 {
            return turns.first().cloned();
        }
        let hints = request_session_hints(body);
        let matches: Vec<_> = turns
            .iter()
            .filter(|turn| {
                hints.iter().any(|hint| {
                    hint == &turn.downstream_sid
                        || hint == &turn.state_sid
                        || hint == &turn.parent_router_sid
                })
            })
            .cloned()
            .collect();
        (matches.len() == 1).then(|| matches[0].clone())
    }

    fn note_response_difficulty(&self, state_sid: &str, reason: Option<String>) {
        if let Some(reason) = reason {
            self.policy
                .lock()
                .unwrap()
                .entry(state_sid.to_string())
                .or_default()
                .pending_difficulty = Some(reason);
        }
    }
}

/// Keeps one downstream prompt registered for exact HTTP attribution.
pub struct LlmTurnGuard {
    runtime: Option<Arc<LlmProxyRuntime>>,
    process_key: ProcessKey,
    registration_id: u64,
}

/// Codex ChatGPT OAuth does not honor `OPENAI_BASE_URL` on its preferred
/// WebSocket path. Give codex-acp an explicit HTTP Responses provider instead.
/// Existing `CODEX_CONFIG` keys are preserved; only this provider and the
/// selected provider id are replaced.
fn codex_proxy_env(mut env: Vec<(String, String)>, proxy_base: &str) -> Vec<(String, String)> {
    const PROVIDER: &str = "router-acp-proxy";
    let existing = env
        .iter()
        .rev()
        .find(|(name, _)| name == "CODEX_CONFIG")
        .and_then(|(_, value)| serde_json::from_str::<Value>(value).ok());
    let mut config = existing
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let providers = config
        .entry("model_providers".to_string())
        .or_insert_with(|| json!({}));
    if !providers.is_object() {
        *providers = json!({});
    }
    providers.as_object_mut().unwrap().insert(
        PROVIDER.to_string(),
        json!({
            "name": "router-acp local proxy",
            "base_url": proxy_base,
            "wire_api": "responses",
            "requires_openai_auth": true,
            "supports_websockets": false,
        }),
    );
    config.insert("model_provider".to_string(), json!(PROVIDER));

    env.retain(|(name, _)| name != "CODEX_CONFIG" && name != "MODEL_PROVIDER");
    env.push((
        "CODEX_CONFIG".to_string(),
        Value::Object(config).to_string(),
    ));
    env.push(("MODEL_PROVIDER".to_string(), PROVIDER.to_string()));
    env
}

/// Claude Code disables MCP tool search whenever `ANTHROPIC_BASE_URL` is not a
/// first-party Anthropic host, because a proxy that drops `tool_reference`
/// blocks would leave the model unable to load the tools it asked for. It then
/// falls back to sending every MCP tool schema eagerly — on a session with a
/// few connectors that is ~150k tokens of context before the first reply.
///
/// Interposing that base URL is exactly what `process_env` just did, and this
/// proxy forwards request and response bodies verbatim (only the top-level
/// `model` is ever rewritten), so the condition Claude Code asks about holds.
/// Only the proxy can answer that question, so answer it here rather than
/// making every deployment set the variable by hand.
///
/// An explicit operator setting always wins — including `false`, which is how
/// you opt out if a future body rewrite ever stops being transparent.
fn anthropic_proxy_env(mut env: Vec<(String, String)>) -> Vec<(String, String)> {
    const TOOL_SEARCH: &str = "ENABLE_TOOL_SEARCH";
    if !env.iter().any(|(name, _)| name == TOOL_SEARCH) {
        env.push((TOOL_SEARCH.to_string(), "true".to_string()));
    }
    env
}

impl Drop for LlmTurnGuard {
    fn drop(&mut self) {
        if let Some(runtime) = &self.runtime {
            runtime.unregister(&self.process_key, self.registration_id);
        }
    }
}

#[derive(Clone)]
struct ProxyServerState {
    shared: Arc<Shared>,
    runtime: Arc<LlmProxyRuntime>,
}

async fn proxy_root(
    State(state): State<ProxyServerState>,
    Path(token): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    proxy_request(state, token, String::new(), request).await
}

async fn proxy_with_path(
    State(state): State<ProxyServerState>,
    Path((token, rest)): Path<(String, String)>,
    request: Request<Body>,
) -> Response<Body> {
    proxy_request(state, token, rest, request).await
}

async fn proxy_request(
    state: ProxyServerState,
    token: String,
    rest: String,
    request: Request<Body>,
) -> Response<Body> {
    let Some(target) = state.runtime.targets_by_token.get(&token).cloned() else {
        return error_response(StatusCode::NOT_FOUND, "unknown proxy target");
    };
    let (parts, incoming_body) = request.into_parts();
    let body = match to_bytes(incoming_body, state.shared.cfg.llm_proxy.max_request_bytes).await {
        Ok(body) => body,
        Err(err) => {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!("cannot buffer inference request: {err}"),
            );
        }
    };
    let parsed = serde_json::from_slice::<Value>(&body).ok();
    let attributed = parsed
        .as_ref()
        .and_then(|value| state.runtime.attribute(&target.key, value));
    let mut decision = match (&parsed, &attributed) {
        (Some(value), Some(active))
            if parts.method == Method::POST
                && is_inference_endpoint(target.config.protocol, &rest) =>
        {
            Some(select_request_model(&state.shared, active, value))
        }
        _ => None,
    };

    let outbound_body = match (decision.as_ref(), parsed.as_ref()) {
        (Some(decision), Some(value)) => shape_provider_request(
            value,
            decision.rewrite.then_some(decision.model.as_str()),
            &decision.effort,
            target.config.protocol,
        )
        .unwrap_or_else(|| body.to_vec()),
        _ => body.to_vec(),
    };

    let upstream_url = match upstream_url(&target, &rest, &parts.uri) {
        Ok(url) => url,
        Err(err) => return error_response(StatusCode::BAD_GATEWAY, &err),
    };
    let request_id = format!("llm-{}", uuid::Uuid::new_v4());
    let started = Instant::now();
    let first_request = build_upstream_request(
        &state.runtime.client,
        &parts.method,
        upstream_url.clone(),
        &parts.headers,
        outbound_body,
    );
    let mut upstream = match first_request.send().await {
        Ok(response) => response,
        Err(err) => {
            let completion = start_request_record(
                &state,
                &target,
                attributed.as_ref(),
                decision.as_ref(),
                &rest,
                &body,
                &request_id,
                started,
            );
            if let Some(context) = completion {
                complete_request(
                    &state.shared,
                    &state.runtime,
                    context,
                    StatusCode::BAD_GATEWAY.as_u16(),
                    &[],
                    Some(format!("upstream request failed: {err}")),
                );
            }
            return error_response(
                StatusCode::BAD_GATEWAY,
                &format!("upstream request failed: {err}"),
            );
        }
    };

    if upstream.status().is_client_error() || upstream.status().is_server_error() {
        let retry_pinned = decision.as_ref().is_some_and(|decision| decision.rewrite);
        if retry_pinned {
            let failed_status = upstream.status();
            // 404/400 on a rewritten request means this target cannot serve it —
            // an api model id the provider doesn't recognise, or a parameter it
            // rejects. Remember it so the next request routes around it instead of
            // paying another failed round trip. Transient codes (429, 5xx) say
            // nothing about the target's suitability and are not recorded.
            if let (StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST, Some(active), Some(decision)) =
                (failed_status, attributed.as_ref(), decision.as_ref())
            {
                tracing::warn!(
                    target = %target.key,
                    model = decision.model,
                    %failed_status,
                    "alternate model rejected; routing around it for this session"
                );
                state
                    .runtime
                    .policy
                    .lock()
                    .unwrap()
                    .entry(active.state_sid.clone())
                    .or_default()
                    .rejected_api_models
                    .insert(decision.model.clone());
            }
            let retry = build_upstream_request(
                &state.runtime.client,
                &parts.method,
                upstream_url,
                &parts.headers,
                body.to_vec(),
            )
            .send()
            .await;
            match retry {
                Ok(response) => {
                    if let Some(active) = attributed.as_ref() {
                        let estimated_input = decision
                            .as_ref()
                            .map(|decision| decision.estimated_input)
                            .unwrap_or_else(|| estimate_request_tokens(&body));
                        // Name the model that was rejected. Without it the record
                        // read "alternate model returned HTTP 404" with from and to
                        // both showing the pinned model — true but useless for
                        // finding the misconfigured `api_model`.
                        let attempted = decision
                            .as_ref()
                            .map(|decision| decision.model.clone())
                            .unwrap_or_else(|| "<unknown>".to_string());
                        decision = Some(RequestDecision {
                            model: parsed
                                .as_ref()
                                .and_then(|value| value.get("model"))
                                .and_then(Value::as_str)
                                .map(ToString::to_string)
                                .unwrap_or_else(|| {
                                    configured_api_model(&state.shared, &active.candidate)
                                }),
                            selected_model: active.candidate.model.clone(),
                            rewrite: false,
                            reason: format!(
                                "alternate model `{attempted}` returned HTTP {failed_status}; \
                                 retried unchanged on {} (not retried again this session)",
                                active.candidate
                            ),
                            event: "proxy-fallback".to_string(),
                            estimated_input,
                            effort: request_effort(
                                &state.shared,
                                &active.parent_router_sid,
                                &active.candidate,
                            ),
                        });
                    }
                    upstream = response;
                }
                Err(err) => {
                    tracing::warn!(
                        target = %target.key,
                        %failed_status,
                        %err,
                        "pinned-model proxy fallback failed; returning the alternate response"
                    );
                }
            }
        }
    }

    let completion = start_request_record(
        &state,
        &target,
        attributed.as_ref(),
        decision.as_ref(),
        &rest,
        &body,
        &request_id,
        started,
    );
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let max_capture = state.shared.cfg.llm_proxy.max_capture_bytes;
    let shared = state.shared.clone();
    let runtime = state.runtime.clone();
    let mut upstream_stream = upstream.bytes_stream();
    let mut completion_guard = CompletionGuard::new(shared, runtime, completion, status.as_u16());
    let response_stream = async_stream::stream! {
        while let Some(item) = upstream_stream.next().await {
            match item {
                Ok(chunk) => {
                    completion_guard.capture(&chunk, max_capture);
                    yield Ok::<Bytes, std::io::Error>(chunk);
                }
                Err(err) => {
                    completion_guard.fail(format!("upstream response stream failed: {err}"));
                    yield Err(std::io::Error::other(err));
                    return;
                }
            }
        }
        completion_guard.complete();
    };
    let mut response = Response::builder().status(status);
    if let Some(response_headers) = response.headers_mut() {
        copy_response_headers(&headers, response_headers);
    }
    response
        .body(Body::from_stream(response_stream))
        .unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY, "cannot build proxy response"))
}

fn build_upstream_request(
    client: &reqwest::Client,
    method: &Method,
    url: reqwest::Url,
    headers: &HeaderMap,
    body: Vec<u8>,
) -> reqwest::RequestBuilder {
    let mut request = client
        .request(method.clone(), url)
        .header("accept-encoding", "identity")
        .body(body);
    for (name, value) in headers {
        if !is_hop_by_hop(name.as_str())
            && !matches!(name.as_str(), "host" | "content-length" | "accept-encoding")
        {
            request = request.header(name, value);
        }
    }
    request
}

#[allow(clippy::too_many_arguments)]
fn start_request_record(
    state: &ProxyServerState,
    target: &ProxyTarget,
    active: Option<&ActiveTurn>,
    decision: Option<&RequestDecision>,
    rest: &str,
    body: &[u8],
    request_id: &str,
    started: Instant,
) -> Option<CompletionContext> {
    let active = active?;
    let (model, reason, event, estimated_input) = decision
        .map(|decision| {
            (
                decision.selected_model.clone(),
                decision.reason.clone(),
                decision.event.clone(),
                decision.estimated_input,
            )
        })
        .unwrap_or_else(|| {
            (
                active.candidate.model.clone(),
                "transparent pass-through".to_string(),
                "pass-through".to_string(),
                estimate_request_tokens(body),
            )
        });
    let model_id = CandidateId::new(&target.agent, &model);
    state
        .shared
        .state
        .lock()
        .unwrap()
        .start_llm_request(&LlmRequestStart {
            request_id: request_id.to_string(),
            router_session_id: active.state_sid.clone(),
            parent_router_session_id: (active.parent_router_sid != active.state_sid)
                .then(|| active.parent_router_sid.clone()),
            agent: target.agent.clone(),
            protocol: protocol_name(target.config.protocol).to_string(),
            endpoint: format!("/{rest}"),
            pinned_model: active.candidate.to_string(),
            model: model_id.to_string(),
            routing_reason: reason.clone(),
            routing_event: event.clone(),
            estimated_input_tokens: estimated_input,
        });
    state.shared.state.lock().unwrap().log(
        &active.state_sid,
        &LogEntry {
            kind: "llm_request".to_string(),
            role: "router".to_string(),
            summary: format!("router-acp · LLM request → {model_id} — {reason}"),
            detail: Some(json!({
                "request_id": request_id,
                "event": event,
                "endpoint": format!("/{rest}"),
                "pinned_model": active.candidate.to_string(),
                "estimated_input_tokens": estimated_input,
            })),
            model: Some(model_id.to_string()),
            ..Default::default()
        },
    );
    // Per-request routing does NOT belong in the model's prose. There is one
    // inference request per tool-call turn, so disclosing each one put a router
    // block between every pair of messages for a whole session — and the same
    // information is already carried per tool call (each call records the model
    // that served it) and in full in the `llm_requests` table behind the
    // analytics page. Routine events (demotion / escalation / verdict) are
    // therefore silent here.
    //
    // An alternate the provider REJECTED is different: it is an anomaly the
    // operator should see. Disclose it once per (event, model) per session — the
    // rejection is also recorded so the model is not retried, but the dedup
    // keeps a pre-fix binary or a transient rejection from flooding the
    // transcript regardless.
    if event == "proxy-fallback" {
        let first_time = state
            .runtime
            .policy
            .lock()
            .unwrap()
            .entry(active.state_sid.clone())
            .or_default()
            .disclosed_events
            .insert(format!("{event}:{model_id}"));
        if first_time {
            state
                .shared
                .with_session(&active.parent_router_sid, |session| {
                    session
                        .pending_disclosure
                        .push(format!("router-acp · {reason}"));
                });
        }
    }
    Some(CompletionContext {
        request_id: request_id.to_string(),
        state_sid: active.state_sid.clone(),
        model: model_id,
        protocol: target.config.protocol,
        started,
    })
}

fn upstream_url(target: &ProxyTarget, rest: &str, incoming: &Uri) -> Result<reqwest::Url, String> {
    let mut url = reqwest::Url::parse(&target.config.upstream_base_url)
        .map_err(|e| format!("invalid configured upstream URL: {e}"))?;
    let path = if rest.is_empty() {
        url.path().to_string()
    } else {
        format!("/{}", rest.trim_start_matches('/'))
    };
    url.set_path(&path);
    let query = match (url.query(), incoming.query()) {
        (Some(configured), Some(incoming)) => Some(format!("{configured}&{incoming}")),
        (Some(configured), None) => Some(configured.to_string()),
        (None, Some(incoming)) => Some(incoming.to_string()),
        (None, None) => None,
    };
    url.set_query(query.as_deref());
    Ok(url)
}

fn is_inference_endpoint(protocol: LlmWireProtocol, path: &str) -> bool {
    let path = path.trim_end_matches('/');
    match protocol {
        LlmWireProtocol::Anthropic => path.ends_with("/messages"),
        LlmWireProtocol::Openai => {
            path.ends_with("/responses")
                || path.ends_with("/chat/completions")
                || path.ends_with("/completions")
        }
    }
}

#[cfg(test)]
fn rewrite_top_level_model(body: &[u8], model: &str) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(body).ok()?;
    let fields: HashMap<String, &RawValue> = serde_json::from_str(text).ok()?;
    let raw_model = fields.get("model")?.get();
    serde_json::from_str::<String>(raw_model).ok()?;
    let start = raw_model.as_ptr() as usize - text.as_ptr() as usize;
    let end = start + raw_model.len();
    let encoded = serde_json::to_string(model).ok()?;
    let mut rewritten = Vec::with_capacity(body.len() + encoded.len());
    rewritten.extend_from_slice(&body[..start]);
    rewritten.extend_from_slice(encoded.as_bytes());
    rewritten.extend_from_slice(&body[end..]);
    Some(rewritten)
}

/// Whether the request explicitly turns thinking off. An absent `thinking` is
/// NOT disabled — the newer Anthropic models think by default — so only the
/// explicit opt-out counts.
fn thinking_disabled(body: &Value) -> bool {
    body.get("thinking")
        .and_then(|thinking| thinking.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|kind| kind.eq_ignore_ascii_case("disabled"))
}

/// Anthropic rejects `output_config.effort` above `high` on a request that
/// disables thinking, so the router's own effort cannot ride such a request
/// verbatim. Claude Code issues its WebFetch/WebSearch model calls with
/// thinking disabled: injecting a session effort of `max` there 400s the tool
/// while the main loop, which thinks, is unaffected.
fn anthropic_effort_for_thinking(value: &str, thinking_disabled: bool) -> &str {
    if thinking_disabled && matches!(value, "xhigh" | "max") {
        "high"
    } else {
        value
    }
}

/// Shape the provider request after routing. This is intentionally distinct
/// from `WireShape::effort`, which remains a boolean compatibility gate for
/// already-shaped requests during request-level model rerouting.
fn shape_provider_request(
    body: &Value,
    model: Option<&str>,
    effort: &EffortShape,
    protocol: LlmWireProtocol,
) -> Option<Vec<u8>> {
    let thinking_off = thinking_disabled(body);
    let mut body = body.clone();
    let object = body.as_object_mut()?;
    if let Some(model) = model {
        object.insert("model".into(), Value::String(model.into()));
    }
    match (protocol, effort) {
        (_, EffortShape::Preserve) => {}
        (LlmWireProtocol::Anthropic, EffortShape::Omit) => {
            if let Some(config) = object
                .get_mut("output_config")
                .and_then(Value::as_object_mut)
            {
                config.remove("effort");
            }
        }
        (LlmWireProtocol::Openai, EffortShape::Omit) => {
            if let Some(reasoning) = object.get_mut("reasoning").and_then(Value::as_object_mut) {
                reasoning.remove("effort");
            }
        }
        (LlmWireProtocol::Anthropic, EffortShape::Set(value)) => {
            let value = anthropic_effort_for_thinking(value, thinking_off);
            let config = object
                .entry("output_config")
                .or_insert_with(|| Value::Object(Default::default()));
            let config = config.as_object_mut()?;
            config.insert("effort".into(), Value::String(value.into()));
        }
        (LlmWireProtocol::Openai, EffortShape::Set(value)) => {
            let reasoning = object
                .entry("reasoning")
                .or_insert_with(|| Value::Object(Default::default()));
            let reasoning = reasoning.as_object_mut()?;
            reasoning.insert("effort".into(), Value::String(value.clone()));
        }
    }
    serde_json::to_vec(&body).ok()
}

fn copy_response_headers(from: &HeaderMap, to: &mut HeaderMap) {
    for (name, value) in from {
        if !is_hop_by_hop(name.as_str()) && name.as_str() != "content-length" {
            to.append(name.clone(), value.clone());
        }
    }
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn error_response(status: StatusCode, message: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"error": {"message": message, "type": "router_acp_proxy_error"}}).to_string(),
        ))
        .unwrap()
}

#[derive(Debug, PartialEq, Eq)]
struct RequestSignals {
    original_model: Option<String>,
    estimated_input: u64,
    routine: bool,
    difficulty: Option<String>,
    test_fingerprint: Option<String>,
    tool_result_count: usize,
    wire: WireShape,
}

/// Request features a reroute target must accept. Only the top-level `model` is
/// rewritten, so every other byte — the thinking config, `output_config.effort`,
/// `max_tokens` — reaches whichever model is named. A model that rejects one of
/// them (Haiku 4.5 takes neither adaptive thinking nor `effort`) turns a demotion
/// into a 400, so these gate the candidate set alongside the context window.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct WireShape {
    adaptive_thinking: bool,
    effort: bool,
    max_output: u64,
}

fn request_wire_shape(body: &Value) -> WireShape {
    WireShape {
        adaptive_thinking: body
            .get("thinking")
            .and_then(|thinking| thinking.get("type"))
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("adaptive")),
        effort: body
            .get("output_config")
            .and_then(|config| config.get("effort"))
            .is_some(),
        max_output: max_output_tokens(body).unwrap_or(0),
    }
}

#[derive(Debug)]
struct RequestDecision {
    /// Exact model value sent upstream (left untouched on the pinned route).
    model: String,
    /// Configured candidate model used for attribution/pricing.
    selected_model: String,
    rewrite: bool,
    reason: String,
    event: String,
    estimated_input: u64,
    /// Candidate-resolved provider-native effort handling for this request.
    effort: EffortShape,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EffortShape {
    /// No router-owned effort exists, so leave the adapter request untouched.
    Preserve,
    /// The router owns the effort but this candidate has no compatible mapping.
    Omit,
    /// Replace the adapter value with this candidate's provider-native value.
    Set(String),
}

fn request_effort(
    shared: &Shared,
    parent_router_sid: &str,
    candidate: &CandidateId,
) -> EffortShape {
    let requested = shared
        .with_session(parent_router_sid, |session| {
            session
                .resolved_effort
                .as_ref()
                .map(|resolution| resolution.requested)
        })
        .flatten();
    match requested {
        None => EffortShape::Preserve,
        Some(level) => shared
            .scores
            .lookup(candidate)
            .resolve_effort(level)
            .provider_value
            .map(EffortShape::Set)
            .unwrap_or(EffortShape::Omit),
    }
}

#[derive(Clone)]
struct ModelOption {
    id: CandidateId,
    api_model: String,
    cost_rank: u32,
    quality: f64,
    context_window: Option<u64>,
    max_output_tokens: Option<u64>,
    adaptive_thinking: bool,
    effort: bool,
}

impl ModelOption {
    /// Whether this model accepts the request as it stands. The proxy forwards
    /// every byte but the model name, so an unsupported parameter is a 400.
    fn accepts(&self, wire: &WireShape) -> bool {
        if wire.adaptive_thinking && !self.adaptive_thinking {
            return false;
        }
        if wire.effort && !self.effort {
            return false;
        }
        if wire.max_output > 0
            && self
                .max_output_tokens
                .is_some_and(|limit| wire.max_output > limit)
        {
            return false;
        }
        true
    }
}

fn select_request_model(
    shared: &Arc<Shared>,
    active: &ActiveTurn,
    body: &Value,
) -> RequestDecision {
    let signals = inspect_request(body);
    let candidates = shared.routeable_candidates();
    let mut headroom = shared.headroom.lock().unwrap();
    let mut models: Vec<ModelOption> = candidates
        .into_iter()
        .filter(|candidate| candidate.id.agent == active.candidate.agent)
        .filter(|candidate| {
            !headroom.is_quarantined(&candidate.id)
                && headroom.cordon_active(&candidate.id.agent).is_none()
                && headroom.usage_cordon(&candidate.id).is_none()
                && !headroom.seat_exhausted(&candidate.id)
        })
        .map(|candidate| {
            let scores = shared.scores.lookup(&candidate.id);
            let api_model = configured_api_model(shared, &candidate.id);
            ModelOption {
                id: candidate.id,
                api_model,
                cost_rank: candidate.cost_rank,
                quality: scores.quality(active.class),
                context_window: scores.context_window,
                max_output_tokens: scores.max_output_tokens,
                adaptive_thinking: scores.adaptive_thinking,
                effort: scores.effort,
            }
        })
        .collect();
    // The pin is normally kept in the pool unconditionally (below): leaving the
    // adapter's own choice alone is always valid. That stops being true once the
    // pin itself is out of budget — re-adding it there forces every remaining
    // request of the session onto a model that can only answer with a limit
    // error.
    let pin_routeable = !headroom.is_quarantined(&active.candidate)
        && headroom.cordon_active(&active.candidate.agent).is_none()
        && headroom.usage_cordon(&active.candidate).is_none()
        && !headroom.seat_exhausted(&active.candidate);
    drop(headroom);
    if pin_routeable && !models.iter().any(|model| model.id == active.candidate) {
        let cost_rank = shared
            .candidate_runtime(&active.candidate)
            .map(|candidate| candidate.cost_rank)
            .unwrap_or(u32::MAX);
        let scores = shared.scores.lookup(&active.candidate);
        models.push(ModelOption {
            id: active.candidate.clone(),
            api_model: configured_api_model(shared, &active.candidate),
            cost_rank,
            quality: scores.quality(active.class),
            context_window: scores.context_window,
            max_output_tokens: scores.max_output_tokens,
            adaptive_thinking: scores.adaptive_thinking,
            effort: scores.effort,
        });
    }
    let mut policy = shared.llm_proxy.policy.lock().unwrap();
    let state = policy.entry(active.state_sid.clone()).or_default();
    if state.pinned_candidate.as_ref() != Some(&active.candidate) {
        // A repin starts fresh, but which upstream model ids the provider rejects
        // is a durable fact about the config — carry it across so the 404 retry
        // loop doesn't restart on every repin.
        let rejected = std::mem::take(&mut state.rejected_api_models);
        *state = RequestPolicyState {
            pinned_candidate: Some(active.candidate.clone()),
            current_model: Some(active.candidate.model.clone()),
            rejected_api_models: rejected,
            ..Default::default()
        };
    }
    state.request_seq += 1;
    let request_seq = state.request_seq;

    // An alternate has to fit the context AND accept the request's shape AND not
    // already have been rejected upstream. The pinned candidate is always kept:
    // it is what the adapter asked for, and leaving the route untouched is
    // always valid.
    let compatible: Vec<ModelOption> = models
        .iter()
        .filter(|model| {
            model.id == active.candidate
                || (model.context_window.is_some_and(|window| {
                    signals.estimated_input
                        <= (window as f64 * shared.cfg.llm_proxy.context_window_fraction) as u64
                }) && model.accepts(&signals.wire)
                    && !state.rejected_api_models.contains(&model.api_model))
        })
        .cloned()
        .collect();
    // Per-request routing is a cost optimization, not a second quality ladder:
    // a request may move below the session pin, but a difficulty signal must
    // never spend above it. Keep a separate ceilinged pool for escalation and
    // verdict decisions. If the pin is unavailable, the normal failover path
    // below may still use the best surviving model.
    let pin_cost_rank = if pin_routeable {
        models
            .iter()
            .find(|model| model.id == active.candidate)
            .map(|model| model.cost_rank)
            .unwrap_or(u32::MAX)
    } else {
        u32::MAX
    };
    let escalation_compatible: Vec<ModelOption> = if pin_routeable {
        compatible
            .iter()
            .filter(|model| model.cost_rank <= pin_cost_rank)
            .cloned()
            .collect()
    } else {
        compatible.clone()
    };

    let mut difficulty = state.pending_difficulty.take().or(signals.difficulty);
    // "Stuck" means the agent acted again and got back byte-identical failing test
    // output. Both halves are load-bearing: without the advanced check, re-sending
    // the same turn reads as stagnation; and the fingerprint is cleared once the
    // failure goes away, so the escalation ends with the failure instead of
    // persisting for the rest of the session.
    let advanced = signals.tool_result_count > state.last_tool_result_count;
    match signals.test_fingerprint {
        Some(fingerprint) => {
            if state.last_test_fingerprint.as_ref() == Some(&fingerprint) {
                if advanced {
                    state.repeated_test_output += 1;
                }
            } else {
                state.repeated_test_output = 0;
            }
            state.last_test_fingerprint = Some(fingerprint);
            if state.repeated_test_output >= 1 {
                difficulty = Some("test output stagnated across consecutive actions".to_string());
            }
        }
        None => {
            state.repeated_test_output = 0;
            state.last_test_fingerprint = None;
        }
    }
    state.last_tool_result_count = signals.tool_result_count;

    let request_expired =
        state.elevated_until_request > 0 && request_seq > state.elevated_until_request;
    let time_expired = state
        .elevated_until_time
        .is_some_and(|deadline| Instant::now() > deadline);
    let was_elevated = state.elevated_until_request > 0 || state.elevated_until_time.is_some();
    let elevated_active = was_elevated && !request_expired && !time_expired;
    if was_elevated && !elevated_active {
        state.elevated_until_request = 0;
        state.elevated_until_time = None;
    }

    let (mut desired, mut reason, mut event, mut emergency) = if let Some(reason) = difficulty {
        state.routine_streak = 0;
        state.elevated_until_request = if shared.cfg.llm_proxy.verdict_ttl_requests == 0 {
            0
        } else {
            request_seq + u64::from(shared.cfg.llm_proxy.verdict_ttl_requests)
        };
        state.elevated_until_time = if shared.cfg.llm_proxy.verdict_ttl_secs == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_secs(shared.cfg.llm_proxy.verdict_ttl_secs))
        };
        let strongest =
            strongest_model(&escalation_compatible).unwrap_or_else(|| active.candidate.clone());
        (
            strongest,
            format!("difficulty signal: {reason}"),
            "escalation".to_string(),
            true,
        )
    } else if elevated_active {
        let strongest =
            strongest_model(&escalation_compatible).unwrap_or_else(|| active.candidate.clone());
        (
            strongest,
            "prior difficulty verdict still active".to_string(),
            "verdict".to_string(),
            false,
        )
    } else {
        if signals.routine {
            state.routine_streak += 1;
        } else {
            state.routine_streak = 0;
        }
        if active.automation {
            let cheap = cheapest_model(&compatible).unwrap_or_else(|| active.candidate.clone());
            (
                cheap,
                "automation/CI request hint".to_string(),
                "automation".to_string(),
                true,
            )
        } else if state.routine_streak >= shared.cfg.llm_proxy.routine_streak {
            let cheap = cheapest_model(&compatible).unwrap_or_else(|| active.candidate.clone());
            // Cache-aware gate: demoting mid-session forfeits the incumbent's
            // warm prompt cache and re-primes the prefix on the cheaper model at
            // its (much higher) cache-write rate. Only pay that once the routine
            // run is long enough for the per-turn read-rate savings to recoup it
            // — otherwise a short routine blip that escalates right back is a net
            // loss (and thrash pays the write twice). No-op when cache pricing is
            // absent (OpenAI/Responses wire).
            let break_even =
                cache_reprime_break_even(shared, &active.candidate, &cheap).unwrap_or(0) as u32;
            let required = shared.cfg.llm_proxy.routine_streak.max(break_even);
            if cheap == active.candidate {
                (
                    active.candidate.clone(),
                    "session-pinned model (no cheaper compatible candidate)".to_string(),
                    "steady".to_string(),
                    false,
                )
            } else if state.routine_streak < required {
                (
                    active.candidate.clone(),
                    format!(
                        "routine streak {} < cache re-prime break-even {} — holding the warm \
                         pinned model (demoting to {} would not amortize its cache write)",
                        state.routine_streak, required, cheap.model
                    ),
                    "cache-hold".to_string(),
                    false,
                )
            } else {
                (
                    cheap,
                    format!(
                        "routine tool-result streak {} ≥ cache-amortized threshold {}",
                        state.routine_streak, required
                    ),
                    "demotion".to_string(),
                    false,
                )
            }
        } else {
            (
                active.candidate.clone(),
                if was_elevated {
                    "difficulty verdict expired; returning to the pinned model".to_string()
                } else {
                    "session-pinned model (no sustained routine signal)".to_string()
                },
                if was_elevated {
                    "expiry".to_string()
                } else {
                    "steady".to_string()
                },
                false,
            )
        }
    };

    // Every "hold the pin" branch above resolves to the pinned candidate by
    // name, so an out-of-budget pin survives them all. Hand the request to the
    // strongest sibling that still has budget instead, and treat it as an
    // emergency so the minimum-dwell counter can't hold us on the exhausted
    // model. With no routeable sibling the pin stands — this proxy only rewrites
    // a model id, it cannot decline the request.
    if !pin_routeable
        && desired == active.candidate
        && let Some(alternate) = strongest_model(&compatible).filter(|alt| alt != &active.candidate)
    {
        reason = format!("{reason}; pinned model is out of plan budget");
        event = "exhausted-repin".to_string();
        emergency = true;
        desired = alternate;
    }

    let current_model = state
        .current_model
        .clone()
        .unwrap_or_else(|| active.candidate.model.clone());
    let allowed_by_dwell = emergency
        || current_model == desired.model
        || state.requests_on_model >= shared.cfg.llm_proxy.minimum_dwell_requests;
    let selected = if allowed_by_dwell {
        if current_model != desired.model {
            state.current_model = Some(desired.model.clone());
            state.requests_on_model = 1;
        } else {
            state.requests_on_model = state.requests_on_model.saturating_add(1);
        }
        desired
    } else {
        let selected = escalation_compatible
            .iter()
            .find(|model| model.id.model == current_model)
            .map(|model| model.id.clone())
            .unwrap_or_else(|| active.candidate.clone());
        reason = format!(
            "{reason}; minimum dwell {}/{} requests",
            state.requests_on_model, shared.cfg.llm_proxy.minimum_dwell_requests
        );
        event = "dwell".to_string();
        state.requests_on_model = state.requests_on_model.saturating_add(1);
        selected
    };
    // Record the decision for per-tool-call attribution in the client.
    state.last_candidate = Some(selected.to_string());
    state.last_reason = Some(reason.clone());
    let upstream_model = if selected == active.candidate {
        signals
            .original_model
            .clone()
            .unwrap_or_else(|| configured_api_model(shared, &selected))
    } else {
        models
            .iter()
            .find(|model| model.id == selected)
            .map(|model| model.api_model.clone())
            .unwrap_or_else(|| configured_api_model(shared, &selected))
    };
    RequestDecision {
        rewrite: signals
            .original_model
            .as_ref()
            .map_or(selected != active.candidate, |original| {
                original != &upstream_model
            }),
        model: upstream_model,
        selected_model: selected.model.clone(),
        reason,
        event,
        estimated_input: signals.estimated_input,
        effort: request_effort(shared, &active.parent_router_sid, &selected),
    }
}

fn configured_api_model(shared: &Shared, candidate: &CandidateId) -> String {
    shared
        .cfg
        .agents
        .iter()
        .find(|agent| agent.name == candidate.agent)
        .and_then(|agent| {
            agent
                .models
                .iter()
                .find(|model| model.id == candidate.model)
        })
        .and_then(|model| model.api_model.clone())
        .unwrap_or_else(|| candidate.model.clone())
}

fn strongest_model(models: &[ModelOption]) -> Option<CandidateId> {
    models
        .iter()
        .max_by(|a, b| {
            a.quality
                .total_cmp(&b.quality)
                .then_with(|| a.cost_rank.cmp(&b.cost_rank))
                .then_with(|| b.id.cmp(&a.id))
        })
        .map(|model| model.id.clone())
}

fn cheapest_model(models: &[ModelOption]) -> Option<CandidateId> {
    models
        .iter()
        .min_by(|a, b| {
            a.cost_rank
                .cmp(&b.cost_rank)
                .then_with(|| b.quality.total_cmp(&a.quality))
                .then_with(|| a.id.cmp(&b.id))
        })
        .map(|model| model.id.clone())
}

fn inspect_request(body: &Value) -> RequestSignals {
    // This is intentionally a pure local classifier. Per-request routing must
    // not spend a provider call asking an LLM to classify another provider call.
    let serialized = serde_json::to_string(body).unwrap_or_default();
    let estimated_input =
        estimate_request_tokens(serialized.as_bytes()) + max_output_tokens(body).unwrap_or(0);
    // Classify from the LATEST tool-result block (and the tool that produced it),
    // located structurally. A flat character-tail of the serialized body is not
    // reliable across wire formats: on the Anthropic wire the request carries a
    // large `system`/`tools` prelude and the whole conversation history in
    // `messages`, so the current turn's tool_result routinely falls outside any
    // fixed-size tail — which silently defeated routine/difficulty detection for
    // Claude while it worked for Codex's Responses shape. When no structured tool
    // block is present (e.g. a plain prompt), fall back to a character tail.
    let latest = latest_tool_context(body);
    let lower_tail = || {
        serialized
            .chars()
            .rev()
            .take(24_000)
            .collect::<String>()
            .chars()
            .rev()
            .collect::<String>()
            .to_ascii_lowercase()
    };
    let recent = latest.content.clone().unwrap_or_else(lower_tail);
    let recent_name = latest.tool_name.clone().unwrap_or_default();
    let structural_error = latest.error;
    // Deliberately narrow. A bare `error:` substring matches ordinary developer
    // tool output — grep hits, type-checker noise, log tails, source text that
    // merely contains the word — and escalated ~8% of real requests on its own.
    // Genuine tool failure is carried by the STRUCTURAL signal below (`is_error`,
    // a failed status, a non-zero exit code), which needs no substring guessing;
    // these markers only cover shapes the structure misses.
    let failure_markers = [
        "test failed",
        "tests failed",
        "traceback",
        "panic:",
        "command failed",
        "exit code 1",
        "exit_code\":1",
        "context_length_exceeded",
        "maximum context length",
    ];
    let difficulty = if structural_error {
        Some("tool result reported failure".to_string())
    } else {
        failure_markers
            .iter()
            .find(|marker| recent.contains(**marker))
            .map(|marker| format!("execution trace contains `{marker}`"))
    };
    let has_tool_result = latest.any_tool_result;
    // Routine when the producing tool is a read/search/edit-class tool, or its
    // output carries a benign completion marker.
    // `bash`/`shell` matter as much as the read tools: shell commands are the
    // highest-volume tool in a coding session, and on the Anthropic wire their
    // tool_result is plain text with no `exit_code` field for the content markers
    // below to match — so without the name they never counted routine and the
    // streak could not form. A FAILING shell command is still excluded, because
    // `routine` requires `difficulty.is_none()`.
    let name_markers = [
        "read",
        "grep",
        "find",
        "glob",
        "list",
        "ls",
        "cat",
        "view",
        "search",
        "write",
        "edit",
        "apply_patch",
        "bash",
        "shell",
        "fetch",
        "todo",
    ];
    let content_markers = [
        "git status",
        "ci still running",
        "checks pending",
        "nothing to commit",
        "working tree clean",
        "\"status\":\"completed\"",
        "\"exit_code\":0",
    ];
    let routine_tool = name_markers
        .iter()
        .any(|marker| recent_name.contains(marker))
        || content_markers.iter().any(|marker| recent.contains(marker));
    RequestSignals {
        original_model: body
            .get("model")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        estimated_input,
        routine: difficulty.is_none() && has_tool_result && routine_tool,
        difficulty,
        test_fingerprint: test_fingerprint(latest.content.as_deref()),
        tool_result_count: latest.result_count,
        wire: request_wire_shape(body),
    }
}

#[derive(Default)]
struct LatestTool {
    /// Lowercased content of the most recent tool-result-like block.
    content: Option<String>,
    /// Lowercased name of the most recent tool invocation (tool_use / function_call).
    tool_name: Option<String>,
    /// Whether the request contains any tool-result-like block at all.
    any_tool_result: bool,
    /// How many tool-result blocks the request carries. Compared across requests
    /// to tell "the agent acted again and got the same output" (a stuck loop)
    /// from "the same turn was re-sent" (no new action to judge).
    result_count: usize,
    /// Whether the most recent tool-result block reported a structural failure
    /// (`is_error`, a failed/error status, or a non-zero exit code).
    error: bool,
}

/// Locate, in document order, the latest tool invocation name and the latest
/// tool-result content anywhere in the request — independent of wire format
/// (Anthropic `tool_use`/`tool_result`, OpenAI `function_call`/
/// `function_call_output`, or a `role: "tool"` message). Used instead of a
/// character tail so detection is robust to large `system`/`tools` preludes.
fn latest_tool_context(body: &Value) -> LatestTool {
    fn visit(value: &Value, out: &mut LatestTool) {
        match value {
            Value::Object(object) => {
                let kind = object
                    .get("type")
                    .or_else(|| object.get("role"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                match kind {
                    "tool_use" | "function_call" | "tool_call" => {
                        if let Some(name) = object
                            .get("name")
                            .or_else(|| object.get("function").and_then(|f| f.get("name")))
                            .and_then(Value::as_str)
                        {
                            out.tool_name = Some(name.to_ascii_lowercase());
                        }
                    }
                    "tool_result" | "function_call_output" | "tool" | "computer_output" => {
                        out.any_tool_result = true;
                        out.result_count += 1;
                        let content = object
                            .get("content")
                            .or_else(|| object.get("output"))
                            .map(|value| match value {
                                Value::String(text) => text.clone(),
                                other => serde_json::to_string(other).unwrap_or_default(),
                            })
                            .unwrap_or_default();
                        out.content = Some(content.to_ascii_lowercase());
                        out.error = contains_true_key(value, "is_error")
                            || contains_string_value(value, "status", &["failed", "error"])
                            || contains_nonzero_exit(value);
                    }
                    _ => {}
                }
                for nested in object.values() {
                    visit(nested, out);
                }
            }
            Value::Array(values) => {
                for nested in values {
                    visit(nested, out);
                }
            }
            _ => {}
        }
    }

    let mut out = LatestTool::default();
    visit(body, &mut out);
    out
}

fn estimate_request_tokens(body: &[u8]) -> u64 {
    (body.len() as u64).div_ceil(4)
}

fn max_output_tokens(body: &Value) -> Option<u64> {
    ["max_tokens", "max_output_tokens", "max_completion_tokens"]
        .iter()
        .find_map(|key| body.get(*key).and_then(Value::as_u64))
}

fn contains_true_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(object) => {
            object.get(key).and_then(Value::as_bool) == Some(true)
                || object.values().any(|value| contains_true_key(value, key))
        }
        Value::Array(values) => values.iter().any(|value| contains_true_key(value, key)),
        _ => false,
    }
}

fn contains_string_value(value: &Value, key: &str, needles: &[&str]) -> bool {
    match value {
        Value::Object(object) => {
            object
                .get(key)
                .and_then(Value::as_str)
                .is_some_and(|value| {
                    needles
                        .iter()
                        .any(|needle| value.eq_ignore_ascii_case(needle))
                })
                || object
                    .values()
                    .any(|value| contains_string_value(value, key, needles))
        }
        Value::Array(values) => values
            .iter()
            .any(|value| contains_string_value(value, key, needles)),
        _ => false,
    }
}

fn contains_nonzero_exit(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.iter().any(|(key, value)| {
                matches!(key.as_str(), "exit_code" | "exitCode")
                    && value.as_i64().is_some_and(|code| code != 0)
            }) || object.values().any(contains_nonzero_exit)
        }
        Value::Array(values) => values.iter().any(contains_nonzero_exit),
        _ => false,
    }
}

/// Fingerprint the CURRENT turn's tool output when it reads as a failing test
/// run. Takes the latest tool-result content (already located structurally by
/// `latest_tool_context`) rather than walking the whole body.
///
/// Scoping matters: the previous implementation searched the entire request and
/// kept the last *matching* block anywhere in it. On the Anthropic wire the whole
/// conversation history is resent every turn, so one historical failure made the
/// fingerprint byte-identical on every subsequent request — the repeat counter
/// latched and escalated the rest of the session. Fingerprinting only the latest
/// result means a new tool result necessarily changes (or clears) it.
///
/// Markers are narrow for the same reason as `failure_markers`: a bare ` failed`
/// or `error:` matches ordinary tool output, and this signal escalates to the
/// most expensive model in the fleet.
fn test_fingerprint(latest_output: Option<&str>) -> Option<String> {
    let output = latest_output?;
    const TEST_FAILURE_MARKERS: [&str; 5] = [
        "test result: failed",
        "tests failed",
        "test failed",
        "failures:",
        "assertion failed",
    ];
    if !TEST_FAILURE_MARKERS
        .iter()
        .any(|marker| output.contains(marker))
    {
        return None;
    }
    Some(format!("{:x}", Sha256::digest(output.trim().as_bytes())))
}

fn request_session_hints(body: &Value) -> Vec<String> {
    let mut hints = Vec::new();
    collect_named_strings(
        body,
        &["prompt_cache_key", "session_id", "sessionId", "user_id"],
        &mut hints,
    );
    hints
}

fn collect_named_strings(value: &Value, keys: &[&str], out: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if keys.contains(&key.as_str())
                    && let Some(value) = value.as_str()
                {
                    out.push(value.to_string());
                }
                collect_named_strings(value, keys, out);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_named_strings(value, keys, out);
            }
        }
        _ => {}
    }
}

fn automation_hint(meta: Option<&agent_client_protocol::schema::v1::Meta>) -> bool {
    let Some(router) = meta.and_then(|meta| meta.get("router_acp")) else {
        return false;
    };
    let hint = router
        .get("request_hint")
        .and_then(Value::as_str)
        .unwrap_or_default();
    matches!(hint, "automation" | "ci-poll" | "ship-nudge")
}

fn target_token(key: &ProcessKey) -> String {
    let digest = format!("{:x}", Sha256::digest(key.0.as_bytes()));
    digest[..20].to_string()
}

fn protocol_name(protocol: LlmWireProtocol) -> &'static str {
    match protocol {
        LlmWireProtocol::Anthropic => "anthropic",
        LlmWireProtocol::Openai => "openai",
    }
}

struct CompletionContext {
    request_id: String,
    state_sid: String,
    model: CandidateId,
    protocol: LlmWireProtocol,
    started: Instant,
}

struct CompletionGuard {
    shared: Arc<Shared>,
    runtime: Arc<LlmProxyRuntime>,
    context: Option<CompletionContext>,
    status: u16,
    captured: Vec<u8>,
    error: Option<String>,
}

impl CompletionGuard {
    fn new(
        shared: Arc<Shared>,
        runtime: Arc<LlmProxyRuntime>,
        context: Option<CompletionContext>,
        status: u16,
    ) -> Self {
        Self {
            shared,
            runtime,
            context,
            status,
            captured: Vec::new(),
            error: None,
        }
    }

    fn capture(&mut self, chunk: &[u8], max: usize) {
        if self.captured.len() + chunk.len() <= max {
            self.captured.extend_from_slice(chunk);
            return;
        }
        // Usage starts in Anthropic's first SSE event and finishes in the last
        // event for every supported wire. Retain both ends of a large stream.
        let head_len = (max / 2).min(self.captured.len());
        let separator = b"\n\n";
        let tail_cap = max.saturating_sub(head_len + separator.len());
        let mut tail = self.captured[head_len..].to_vec();
        tail.extend_from_slice(chunk);
        if tail.len() > tail_cap {
            tail.drain(..tail.len() - tail_cap);
        }
        self.captured.truncate(head_len);
        self.captured.extend_from_slice(separator);
        self.captured.extend_from_slice(&tail);
    }

    fn fail(&mut self, error: String) {
        self.error = Some(error);
        self.finish();
    }

    fn complete(&mut self) {
        self.finish();
    }

    fn finish(&mut self) {
        if let Some(context) = self.context.take() {
            complete_request(
                &self.shared,
                &self.runtime,
                context,
                self.status,
                &self.captured,
                self.error.take(),
            );
        }
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if self.context.is_some() {
            self.error = Some("adapter disconnected before the response stream completed".into());
            self.finish();
        }
    }
}

fn complete_request(
    shared: &Arc<Shared>,
    runtime: &Arc<LlmProxyRuntime>,
    context: CompletionContext,
    status: u16,
    captured: &[u8],
    error: Option<String>,
) {
    let usage = parse_response_usage(captured, context.protocol);
    let cost = request_cost(shared, &context.model, context.protocol, &usage);
    let response_difficulty = response_difficulty(captured, status);
    let duration_ms = context.started.elapsed().as_millis() as u64;
    {
        let state = shared.state.lock().unwrap();
        state.finish_llm_request(
            &context.request_id,
            status,
            duration_ms,
            &usage,
            cost,
            error.as_deref(),
        );
        state.log(
            &context.state_sid,
            &LogEntry {
                kind: "llm_response".to_string(),
                role: "router".to_string(),
                summary: format!("LLM response {} · {} · ${cost:.6}", context.model, status),
                detail: Some(json!({
                    "request_id": context.request_id,
                    "status": status,
                    "duration_ms": duration_ms,
                    "usage": {
                        "input": usage.input,
                        "output": usage.output,
                        "cache_read": usage.cache_read,
                        "cache_write": usage.cache_write,
                    },
                    "cost_usd": cost,
                    "error": error,
                })),
                model: Some(context.model.to_string()),
                ..Default::default()
            },
        );
    }
    runtime.note_response_difficulty(&context.state_sid, response_difficulty);
}

fn parse_response_usage(body: &[u8], protocol: LlmWireProtocol) -> LlmRequestUsage {
    let text = String::from_utf8_lossy(body);
    let mut usage = LlmRequestUsage::default();
    if let Ok(value) = serde_json::from_str::<Value>(&text) {
        collect_usage(&value, protocol, &mut usage);
    } else {
        for line in text.lines() {
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data.trim();
            if data == "[DONE]" {
                continue;
            }
            if let Ok(value) = serde_json::from_str::<Value>(data) {
                collect_usage(&value, protocol, &mut usage);
            }
        }
    }
    usage
}

fn collect_usage(value: &Value, protocol: LlmWireProtocol, usage: &mut LlmRequestUsage) {
    match value {
        Value::Object(object) => {
            if object.contains_key("input_tokens")
                || object.contains_key("prompt_tokens")
                || object.contains_key("output_tokens")
                || object.contains_key("completion_tokens")
            {
                let input = object
                    .get("input_tokens")
                    .or_else(|| object.get("prompt_tokens"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let output = object
                    .get("output_tokens")
                    .or_else(|| object.get("completion_tokens"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let cache_read = object
                    .get("cache_read_input_tokens")
                    .and_then(Value::as_u64)
                    .or_else(|| {
                        object
                            .get("input_tokens_details")
                            .or_else(|| object.get("prompt_tokens_details"))
                            .and_then(|details| details.get("cached_tokens"))
                            .and_then(Value::as_u64)
                    })
                    .unwrap_or(0);
                let cache_write = object
                    .get("cache_creation_input_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                usage.input = usage.input.max(input);
                usage.output = usage.output.max(output);
                usage.cache_read = usage.cache_read.max(cache_read);
                usage.cache_write = usage.cache_write.max(cache_write);
                if protocol == LlmWireProtocol::Openai {
                    usage.input_includes_cache = true;
                }
            }
            for nested in object.values() {
                collect_usage(nested, protocol, usage);
            }
        }
        Value::Array(values) => {
            for nested in values {
                collect_usage(nested, protocol, usage);
            }
        }
        _ => {}
    }
}

fn response_difficulty(body: &[u8], status: u16) -> Option<String> {
    if status >= 400 {
        return Some(format!("upstream returned HTTP {status}"));
    }
    let lower = String::from_utf8_lossy(body).to_ascii_lowercase();
    if lower.contains("\"stop_reason\":\"max_tokens\"")
        || lower.contains("\"finish_reason\":\"length\"")
        || lower.contains("context_length_exceeded")
    {
        Some("model reached a token/context ceiling".to_string())
    } else if lower.contains("\"stop_reason\":\"refusal\"")
        || lower.contains("\"finish_reason\":\"content_filter\"")
        || lower.contains("\"refusal\":")
    {
        Some("model refusal/content filter".to_string())
    } else {
        None
    }
}

/// How many warm cache-read turns the cheaper model must serve before its
/// one-time prompt-cache re-prime pays for itself. Prompt caching is keyed per
/// model, so demoting mid-session forfeits the incumbent's warm cache: the
/// switch turn re-primes the whole prefix on the target at its cache-WRITE rate
/// (instead of the incumbent's cache-READ rate), and only later warm turns bank
/// the read-rate difference. break-even ≈ (target_write − pinned_read) /
/// (pinned_read − target_read), turns. Returns `None` when either model lacks
/// explicit cache pricing (e.g. the OpenAI/Responses wire, where cached input is
/// auto-discounted with no separate write cost) — the gate then does not apply,
/// so Codex/Grok/Kimi behavior is unchanged.
fn cache_reprime_break_even(
    shared: &Shared,
    pinned: &CandidateId,
    target: &CandidateId,
) -> Option<u64> {
    if pinned == target {
        return Some(0);
    }
    let pricing = |candidate: &CandidateId| {
        shared
            .cfg
            .agents
            .iter()
            .find(|agent| agent.name == candidate.agent)
            .and_then(|agent| {
                agent
                    .models
                    .iter()
                    .find(|model| model.id == candidate.model)
            })
            .and_then(|model| model.pricing.clone())
    };
    let pinned = pricing(pinned)?;
    let target = pricing(target)?;
    let pinned_read = pinned.cache_read_per_mtok?;
    let target_read = target.cache_read_per_mtok?;
    let target_write = target.cache_write_per_mtok?;
    let extra_on_switch = target_write - pinned_read;
    let saved_per_warm_turn = pinned_read - target_read;
    if extra_on_switch <= 0.0 || saved_per_warm_turn <= 0.0 {
        return Some(0);
    }
    Some((extra_on_switch / saved_per_warm_turn).ceil() as u64)
}

fn request_cost(
    shared: &Arc<Shared>,
    candidate: &CandidateId,
    protocol: LlmWireProtocol,
    usage: &LlmRequestUsage,
) -> f64 {
    let Some(pricing) = shared
        .cfg
        .agents
        .iter()
        .find(|agent| agent.name == candidate.agent)
        .and_then(|agent| {
            agent
                .models
                .iter()
                .find(|model| model.id == candidate.model)
        })
        .and_then(|model| model.pricing.as_ref())
    else {
        return 0.0;
    };
    let cache_read_rate = pricing
        .cache_read_per_mtok
        .unwrap_or(pricing.input_per_mtok * 0.1);
    let cache_write_rate = pricing
        .cache_write_per_mtok
        .unwrap_or(pricing.input_per_mtok * 1.25);
    let uncached_input = if protocol == LlmWireProtocol::Openai && usage.input_includes_cache {
        usage.input.saturating_sub(usage.cache_read)
    } else {
        usage.input
    };
    (uncached_input as f64 * pricing.input_per_mtok
        + usage.output as f64 * pricing.output_per_mtok
        + usage.cache_read as f64 * cache_read_rate
        + usage.cache_write as f64 * cache_write_rate)
        / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::EffortLevel;
    use crate::session::RouterSession;
    use crate::state::PersistedSession;

    fn policy_shared(minimum_dwell_requests: u32) -> (tempfile::TempDir, Arc<Shared>) {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
  routine_streak: 1
  minimum_dwell_requests: {minimum_dwell_requests}
  verdict_ttl_requests: 1
  verdict_ttl_secs: 0
agents:
  - name: mock
    command: {{ type: stdio, command: mock-agent }}
    model_selection: {{ type: config-option }}
    llm_proxy:
      protocol: anthropic
      base_url_env: MOCK_BASE_URL
      upstream_base_url: http://127.0.0.1:9
    models:
      - {{ id: haiku, cost_rank: 1 }}
      - {{ id: sonnet, cost_rank: 2 }}
      - {{ id: opus, cost_rank: 3 }}
"#,
            dir.path().join("state.db").display()
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg).unwrap();
        shared.set_models_routeable(
            &ProcessKey("mock".to_string()),
            vec!["haiku".into(), "sonnet".into(), "opus".into()],
        );
        (dir, shared)
    }

    /// Same harness with Anthropic-style cache pricing (Fable→Sonnet rates on
    /// opus→haiku) so the cache re-prime gate is active.
    fn policy_shared_priced(minimum_dwell_requests: u32) -> (tempfile::TempDir, Arc<Shared>) {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
  routine_streak: 1
  minimum_dwell_requests: {minimum_dwell_requests}
  verdict_ttl_requests: 1
  verdict_ttl_secs: 0
agents:
  - name: mock
    command: {{ type: stdio, command: mock-agent }}
    model_selection: {{ type: config-option }}
    llm_proxy:
      protocol: anthropic
      base_url_env: MOCK_BASE_URL
      upstream_base_url: http://127.0.0.1:9
    models:
      - id: haiku
        cost_rank: 1
        pricing: {{ input_per_mtok: 1.0, output_per_mtok: 5.0, cache_read_per_mtok: 0.30, cache_write_per_mtok: 3.75 }}
      - id: opus
        cost_rank: 3
        pricing: {{ input_per_mtok: 10.0, output_per_mtok: 50.0, cache_read_per_mtok: 1.00, cache_write_per_mtok: 12.50 }}
"#,
            dir.path().join("state.db").display()
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg).unwrap();
        shared.set_models_routeable(
            &ProcessKey("mock".to_string()),
            vec!["haiku".into(), "opus".into()],
        );
        (dir, shared)
    }

    /// The real production shape: the `claude` agent with the four shipped
    /// candidates, their real `api_model` values, and Anthropic's real cache rates
    /// (read = 0.1x input, write = 1.25x input) so the re-prime gate is live.
    /// Score data (context windows, wire features) comes from the shipped
    /// `data/scores.yaml`, not from a test stub.
    fn kory_code_shared() -> (tempfile::TempDir, Arc<Shared>) {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
  routine_streak: 3
  minimum_dwell_requests: 0
  verdict_ttl_requests: 6
  verdict_ttl_secs: 900
agents:
  - name: claude
    command: {{ type: stdio, command: claude-agent-acp }}
    model_selection: {{ type: config-option }}
    llm_proxy:
      protocol: anthropic
      base_url_env: ANTHROPIC_BASE_URL
      upstream_base_url: https://api.anthropic.com
    models:
      - id: haiku
        cost_rank: 1
        api_model: claude-haiku-4-5
        pricing: {{ input_per_mtok: 1.0, output_per_mtok: 5.0, cache_read_per_mtok: 0.10, cache_write_per_mtok: 1.25 }}
      - id: sonnet
        cost_rank: 2
        api_model: claude-sonnet-5
        pricing: {{ input_per_mtok: 3.0, output_per_mtok: 15.0, cache_read_per_mtok: 0.30, cache_write_per_mtok: 3.75 }}
      - id: "opus[1m]"
        cost_rank: 4
        api_model: claude-opus-5
        pricing: {{ input_per_mtok: 5.0, output_per_mtok: 25.0, cache_read_per_mtok: 0.50, cache_write_per_mtok: 6.25 }}
      - id: "claude-fable-5[1m]"
        cost_rank: 5
        api_model: claude-fable-5
        pricing: {{ input_per_mtok: 10.0, output_per_mtok: 50.0, cache_read_per_mtok: 1.00, cache_write_per_mtok: 12.50 }}
"#,
            dir.path().join("state.db").display()
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg).unwrap();
        shared.set_models_routeable(
            &ProcessKey("claude".to_string()),
            vec![
                "haiku".into(),
                "sonnet".into(),
                "opus[1m]".into(),
                "claude-fable-5[1m]".into(),
            ],
        );
        (dir, shared)
    }

    fn claude_active(candidate: &str) -> ActiveTurn {
        ActiveTurn {
            candidate: CandidateId::new("claude", candidate),
            ..active("haiku")
        }
    }

    /// A request shaped like the ones `claude-agent-acp` actually emits, captured
    /// off the wire: bare api model id, adaptive thinking, `output_config.effort`,
    /// `max_tokens: 64000`, a large system + tools prelude, and the whole
    /// conversation resent every turn. `turns` controls how much history is
    /// replayed, which is what pushes a real session past 180k tokens.
    fn kory_code_request(turns: usize, latest_tool_output: &str) -> Value {
        let tools: Vec<Value> = (0..29)
            .map(|i| {
                json!({
                    "name": format!("Tool{i}"),
                    "description": "x".repeat(1_200),
                    "input_schema": {"type":"object","properties":{"a":{"type":"string"}}}
                })
            })
            .collect();
        let mut messages = Vec::new();
        for turn in 0..turns {
            messages.push(json!({
                "role":"assistant",
                "content":[{"type":"tool_use","name":"Bash","id":format!("t{turn}"),"input":{"command":"ls"}}]
            }));
            messages.push(json!({
                "role":"user",
                "content":[{"type":"tool_result","tool_use_id":format!("t{turn}"),
                    "content": format!("$ ls\n{}", "src/file.rs\n".repeat(500))}]
            }));
        }
        // The turn under judgement, last in document order.
        messages.push(json!({
            "role":"assistant",
            "content":[{"type":"tool_use","name":"Bash","id":"latest","input":{"command":"ls"}}]
        }));
        messages.push(json!({
            "role":"user",
            "content":[{"type":"tool_result","tool_use_id":"latest","content":latest_tool_output}]
        }));
        json!({
            "model": "claude-fable-5",
            "max_tokens": 64000,
            "stream": true,
            "thinking": {"type":"adaptive","display":"omitted"},
            "output_config": {"effort":"high"},
            "system": [{"type":"text","text":"y".repeat(20_000)}],
            "tools": tools,
            "messages": messages
        })
    }

    #[test]
    fn real_sized_kory_code_request_demotes_fable_to_sonnet() {
        let (_dir, shared) = kory_code_shared();
        let turn = claude_active("claude-fable-5[1m]");

        // Sized like a mid-session Kory Code request. The measured median was
        // ~214k actual input tokens; anything over 180k was previously excluded
        // from every cheaper candidate by the mis-scored 200k windows.
        let body = kory_code_request(120, "$ ls\nsrc/main.rs\nsrc/lib.rs");
        let estimated = inspect_request(&body).estimated_input;
        assert!(
            (180_000..900_000).contains(&estimated),
            "fixture must sit above the old 180k gate and inside Sonnet's 1M window, got {estimated}"
        );

        // Two gates run in series before a demotion is allowed: the configured
        // routine_streak (3), then the Fable->Sonnet cache re-prime break-even (4).
        // So streaks 1-2 are simply steady, streak 3 is held back to amortize the
        // cache write, and streak 4 demotes. Sonnet, not Haiku: Haiku is both too
        // small (200k) and rejects adaptive thinking + effort, so the cheapest
        // COMPATIBLE candidate is Sonnet.
        let events: Vec<(String, String)> = (0..4)
            .map(|_| {
                let decision = select_request_model(&shared, &turn, &body);
                (decision.event, decision.model)
            })
            .collect();
        let observed: Vec<(&str, &str)> = events
            .iter()
            .map(|(event, model)| (event.as_str(), model.as_str()))
            .collect();
        assert_eq!(
            observed,
            vec![
                ("steady", "claude-fable-5"),
                ("steady", "claude-fable-5"),
                ("cache-hold", "claude-fable-5"),
                ("demotion", "claude-sonnet-5"),
            ]
        );
    }

    #[test]
    fn real_sized_request_escalates_and_recovers_instead_of_latching() {
        // The measured production failure: 46% of requests carried
        // "test output stagnated", and once escalated a session never came back.
        // A genuine repeat escalates; a clean following turn returns to steady.
        let (_dir, shared) = kory_code_shared();
        let turn = claude_active("claude-fable-5[1m]");
        const FAILING: &str = "running 2 tests\ntest result: FAILED. 1 passed; 1 failed";

        assert_ne!(
            select_request_model(&shared, &turn, &kory_code_request(20, FAILING)).event,
            "escalation",
            "first sight of a failure is not yet stagnation"
        );
        let stuck = select_request_model(&shared, &turn, &kory_code_request(21, FAILING));
        assert_eq!(stuck.event, "escalation");
        // Fable is the compressed pair's ahead member (top of the table), so a
        // struggling Fable-pinned session has nowhere higher to go and the
        // escalation stays in place.
        assert_eq!(stuck.model, "claude-fable-5");

        // Verdict TTL is 6 requests, so the next few stay elevated by design; once
        // it expires with clean output the session must return to the pinned model
        // and become demotable again rather than staying escalated forever.
        let mut events = Vec::new();
        for turns in 22..34 {
            let decision = select_request_model(
                &shared,
                &turn,
                &kory_code_request(turns, "$ ls\nsrc/main.rs"),
            );
            events.push(decision.event);
        }
        assert!(
            events.iter().any(|event| event == "demotion"),
            "a recovered session must become demotable again, saw {events:?}"
        );
    }

    fn active(candidate: &str) -> ActiveTurn {
        ActiveTurn {
            registration_id: 1,
            parent_router_sid: "r1".to_string(),
            state_sid: "r1".to_string(),
            downstream_sid: "d1".to_string(),
            candidate: CandidateId::new("mock", candidate),
            class: TaskClass::CodingGeneral,
            automation: false,
        }
    }

    #[test]
    fn request_inspection_finds_routine_and_failure_signals() {
        let routine = json!({
            "model": "opus",
            "messages": [{"role":"user","content":[{
                "type":"tool_result","content":"git status clean","status":"completed"
            }]}]
        });
        let signal = inspect_request(&routine);
        assert!(signal.routine);
        assert!(signal.difficulty.is_none());

        let failed = json!({
            "model": "sonnet",
            "input": [{"type":"function_call_output","exit_code":1,"output":"tests failed"}]
        });
        assert!(inspect_request(&failed).difficulty.is_some());
    }

    #[test]
    fn request_inspection_is_deterministic_and_local() {
        let body = json!({
            "model": "sonnet",
            "messages": [{"role":"user","content":[{
                "type":"tool_result","content":"git status clean","status":"completed"
            }]}]
        });
        assert_eq!(inspect_request(&body), inspect_request(&body));
    }

    #[test]
    fn routine_detection_survives_large_anthropic_tools_prelude() {
        // Reproduces the Claude/Anthropic wire shape: the current turn's
        // tool_result sits inside `messages`, ahead of a large `system` and
        // `tools` prelude. A fixed character-tail of the serialized body lands
        // in the tool schemas and misses the tool_result entirely; structural
        // detection must still classify it as routine.
        let big_tool_schema: String = (0..4000)
            .map(|i| {
                format!("{{\"name\":\"synthetic_field_{i}\",\"desc\":\"padding schema entry\"}}")
            })
            .collect::<Vec<_>>()
            .join(",");
        let body = json!({
            "model": "claude-fable-5",
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "toolu_1",
                    "content": "$ git status\nnothing to commit, working tree clean"
                }]
            }],
            "system": [{"type":"text","text":"You are Claude Code, Anthropic's official CLI for Claude."}],
            // Big enough that the serialized tail is entirely tool schemas.
            "tools": serde_json::from_str::<Value>(&format!("[{big_tool_schema}]")).unwrap(),
        });
        // The tool_result is nowhere near the serialized tail.
        let serialized = serde_json::to_string(&body).unwrap();
        assert!(!serialized[serialized.len() - 24_000..].contains("git status"));

        let signal = inspect_request(&body);
        assert!(
            signal.routine,
            "buried tool_result must still read as routine"
        );
        assert!(signal.difficulty.is_none());
        assert!(inspect_request(&body).original_model.as_deref() == Some("claude-fable-5"));
    }

    #[test]
    fn routine_detection_uses_tool_name_when_output_has_no_marker() {
        // A Read tool result whose content carries no textual marker: routine
        // must be inferred from the invoking tool's name (the assistant's
        // tool_use), which structural detection tracks across the turn.
        let body = json!({
            "messages": [
                {"role":"assistant","content":[{"type":"tool_use","name":"Read","id":"toolu_2","input":{"path":"a.txt"}}]},
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_2","content":"note 1\nowner: team-1\npriority: 2"}]}
            ]
        });
        let signal = inspect_request(&body);
        assert!(signal.routine, "a Read tool call must read as routine");
        assert!(signal.difficulty.is_none());
    }

    #[test]
    fn stale_history_failure_does_not_pin_difficulty_on_the_current_turn() {
        // The whole conversation is resent each turn on the Anthropic wire; a
        // failure from an earlier turn must not keep escalating once the latest
        // tool result is clean.
        let body = json!({
            "messages": [
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"tests failed","is_error":true}]},
                {"role":"assistant","content":[{"type":"tool_use","name":"Bash","id":"t2","input":{}}]},
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"t2","content":"$ git status\nworking tree clean"}]}
            ]
        });
        let signal = inspect_request(&body);
        assert!(
            signal.difficulty.is_none(),
            "recovered turn must not stay escalated"
        );
        assert!(signal.routine);
    }

    #[test]
    fn repeated_failing_test_output_escalates_when_the_agent_acted_again() {
        let (_dir, shared) = policy_shared(0);
        const FAILING: &str = "test result: FAILED. 1 passed; 1 failed";
        let first = json!({
            "model":"haiku",
            "input":[{"type":"function_call_output","output":FAILING}]
        });
        // The agent acted and re-ran the suite: the trace now carries a SECOND
        // output, byte-identical to the first. That is the stuck signal.
        let second = json!({
            "model":"haiku",
            "input":[
                {"type":"function_call_output","output":FAILING},
                {"role":"assistant","content":"I changed src/lib.rs"},
                {"type":"function_call_output","output":FAILING}
            ]
        });
        assert_eq!(
            select_request_model(&shared, &active("haiku"), &first).event,
            "steady"
        );
        let decision = select_request_model(&shared, &active("haiku"), &second);
        assert_eq!(decision.event, "escalation");
        assert_eq!(decision.selected_model, "haiku");
        assert_eq!(decision.model, "haiku");
        assert!(decision.reason.contains("stagnated"));
    }

    #[test]
    fn last_attribution_reports_the_model_that_served_the_request() {
        // The structured per-tool-call attribution the client renders in the
        // call's own card. Absent before the first request (rendered as pending,
        // never as a wrong model), and it follows the selection — not the pin —
        // so a demoted call is attributed to the model that actually served it.
        let (_dir, shared) = kory_code_shared();
        let turn = claude_active("claude-fable-5[1m]");
        assert!(shared.llm_proxy.last_attribution("r1").is_none());

        let body = kory_code_request(120, "$ ls\nsrc/main.rs");
        let mut last = None;
        for _ in 0..4 {
            let decision = select_request_model(&shared, &turn, &body);
            last = Some(decision.event);
        }
        assert_eq!(last.as_deref(), Some("demotion"));
        let (candidate, reason) = shared.llm_proxy.last_attribution("r1").unwrap();
        assert_eq!(candidate, "claude/sonnet");
        assert!(reason.contains("routine tool-result streak"), "{reason}");
    }

    #[test]
    fn provider_request_shaping_sets_omits_or_preserves_effort_as_resolved() {
        let anthropic = shape_provider_request(
            &json!({"model": "old", "output_config": {"effort": "stale"}}),
            Some("new"),
            &EffortShape::Set("intensive".to_string()),
            LlmWireProtocol::Anthropic,
        )
        .unwrap();
        let anthropic: Value = serde_json::from_slice(&anthropic).unwrap();
        assert_eq!(anthropic["model"], "new");
        assert_eq!(anthropic["output_config"]["effort"], "intensive");

        let openai = shape_provider_request(
            &json!({"model": "gpt", "reasoning": {"effort": "high"}}),
            None,
            &EffortShape::Preserve,
            LlmWireProtocol::Openai,
        )
        .unwrap();
        let openai: Value = serde_json::from_slice(&openai).unwrap();
        assert_eq!(openai["reasoning"]["effort"], "high");
        let anthropic_omitted = shape_provider_request(
            &json!({"model": "claude", "output_config": {"effort": "high"}}),
            None,
            &EffortShape::Omit,
            LlmWireProtocol::Anthropic,
        )
        .unwrap();
        let anthropic_omitted: Value = serde_json::from_slice(&anthropic_omitted).unwrap();
        assert!(anthropic_omitted["output_config"].get("effort").is_none());
        assert!(request_wire_shape(&anthropic).effort);
    }

    #[test]
    fn anthropic_effort_is_capped_at_high_when_the_request_disables_thinking() {
        // Claude Code's WebFetch/WebSearch model calls disable thinking, and
        // Anthropic 400s `output_config.effort` above `high` on those. A session
        // resolved to `max` used to take both tools out for the whole session.
        for requested in ["max", "xhigh"] {
            let capped = shape_provider_request(
                &json!({"model": "claude", "thinking": {"type": "disabled"}}),
                None,
                &EffortShape::Set(requested.to_string()),
                LlmWireProtocol::Anthropic,
            )
            .unwrap();
            let capped: Value = serde_json::from_slice(&capped).unwrap();
            assert_eq!(capped["output_config"]["effort"], "high");
        }

        // Thinking on, or simply unset (the newer models think by default):
        // the resolved effort rides the request untouched.
        for thinking in [json!({"type": "adaptive"}), Value::Null] {
            let mut body = json!({"model": "claude"});
            if !thinking.is_null() {
                body["thinking"] = thinking;
            }
            let kept = shape_provider_request(
                &body,
                None,
                &EffortShape::Set("max".to_string()),
                LlmWireProtocol::Anthropic,
            )
            .unwrap();
            let kept: Value = serde_json::from_slice(&kept).unwrap();
            assert_eq!(kept["output_config"]["effort"], "max");
        }

        // The cap is Anthropic's rule; OpenAI-shaped requests keep their value.
        let openai = shape_provider_request(
            &json!({"model": "gpt", "thinking": {"type": "disabled"}}),
            None,
            &EffortShape::Set("max".to_string()),
            LlmWireProtocol::Openai,
        )
        .unwrap();
        let openai: Value = serde_json::from_slice(&openai).unwrap();
        assert_eq!(openai["reasoning"]["effort"], "max");
    }

    #[test]
    fn request_level_routing_resolves_effort_for_the_selected_subtool_model() {
        let dir = tempfile::tempdir().unwrap();
        let scores = dir.path().join("scores.yaml");
        std::fs::write(
            &scores,
            "candidates:\n\
             \x20 - { pattern: 'mock/parent', default_quality: 3.0, context_window: 400000, effort_levels: [max], effort_mapping: { max: parent-max } }\n\
             \x20 - { pattern: 'mock/subtool', default_quality: 1.0, context_window: 400000, effort_levels: [high], effort_mapping: { high: subtool-high } }\n",
        )
        .unwrap();
        let yaml = format!(
            r#"
state_file: {}
score_table: {}
llm_proxy:
  enabled: true
  routine_streak: 1
  minimum_dwell_requests: 0
agents:
  - name: mock
    command: {{ type: stdio, command: mock-agent }}
    model_selection: {{ type: config-option }}
    llm_proxy:
      protocol: anthropic
      base_url_env: MOCK_BASE_URL
      upstream_base_url: http://127.0.0.1:9
    models:
      - {{ id: subtool, cost_rank: 1 }}
      - {{ id: parent, cost_rank: 3 }}
"#,
            dir.path().join("state.db").display(),
            scores.display(),
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg.clone()).unwrap();
        shared.set_models_routeable(
            &ProcessKey("mock".to_string()),
            vec!["subtool".into(), "parent".into()],
        );
        let mut session = RouterSession::rehydrated(&cfg, &PersistedSession::default(), Vec::new());
        session.resolved_effort = Some(
            shared
                .scores
                .lookup(&CandidateId::new("mock", "parent"))
                .resolve_effort(EffortLevel::Max),
        );
        shared
            .sessions
            .lock()
            .unwrap()
            .insert("r1".to_string(), session);

        let body = json!({
            "model": "parent",
            "output_config": {"effort": "parent-max"},
            "messages": [{"role":"user","content":[{
                "type":"tool_result","content":"git status clean","status":"completed"
            }]}]
        });
        let decision = select_request_model(&shared, &active("parent"), &body);
        assert_eq!(decision.selected_model, "subtool");
        assert_eq!(
            decision.effort,
            EffortShape::Set("subtool-high".to_string())
        );

        let shaped = shape_provider_request(
            &body,
            Some(&decision.model),
            &decision.effort,
            LlmWireProtocol::Anthropic,
        )
        .unwrap();
        let shaped: Value = serde_json::from_slice(&shaped).unwrap();
        assert_eq!(shaped["model"], "subtool");
        assert_eq!(shaped["output_config"]["effort"], "subtool-high");
    }

    #[test]
    fn resending_the_same_turn_is_not_read_as_stagnation() {
        // Same body twice: the failing output is unchanged but the agent has not
        // acted again, so there is no new attempt to judge as stuck.
        let (_dir, shared) = policy_shared(0);
        let body = json!({
            "model":"haiku",
            "input":[{"type":"function_call_output","output":"test result: FAILED. 1 failed"}]
        });
        assert_eq!(
            select_request_model(&shared, &active("haiku"), &body).event,
            "steady"
        );
        assert_eq!(
            select_request_model(&shared, &active("haiku"), &body).event,
            "steady"
        );
    }

    #[test]
    fn historical_test_failure_does_not_latch_escalation_on_later_turns() {
        // The regression that produced 0 demotions in real sessions: on the
        // Anthropic wire the whole conversation is resent every turn, so an early
        // failing test result stayed visible forever. Fingerprinting the whole body
        // made it byte-identical on every subsequent request and escalated the rest
        // of the session. Only the CURRENT turn's output may drive the verdict.
        let (_dir, shared) = policy_shared(0);
        let turn = |latest: &str| {
            json!({
                "model":"haiku",
                "messages":[
                    {"role":"user","content":[{"type":"tool_result","tool_use_id":"t0",
                        "content":"test result: FAILED. 1 passed; 1 failed"}]},
                    {"role":"assistant","content":[{"type":"tool_use","name":"Read","id":"t1","input":{}}]},
                    {"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":latest}]}
                ]
            })
        };
        for latest in ["fn main() {}", "working tree clean", "ok"] {
            let decision = select_request_model(&shared, &active("haiku"), &turn(latest));
            assert_ne!(
                decision.event, "escalation",
                "clean current turn must not inherit a historical failure: {}",
                decision.reason
            );
        }
    }

    #[test]
    fn haiku_is_excluded_when_the_request_carries_adaptive_thinking_and_effort() {
        // Real Kory Code requests carry `thinking: {type: adaptive}` and
        // `output_config.effort`; Haiku 4.5 rejects both. Since the proxy forwards
        // every byte but the model name, demoting to Haiku would 400 — it must not
        // be offered as an alternate for such a request.
        let wire = request_wire_shape(&json!({
            "thinking": {"type": "adaptive", "display": "omitted"},
            "output_config": {"effort": "high"},
            "max_tokens": 64000
        }));
        assert!(wire.adaptive_thinking);
        assert!(wire.effort);
        assert_eq!(wire.max_output, 64000);

        let haiku = ModelOption {
            id: CandidateId::new("claude", "haiku"),
            api_model: "claude-haiku-4-5".to_string(),
            cost_rank: 1,
            quality: 0.55,
            context_window: Some(200_000),
            max_output_tokens: Some(64_000),
            adaptive_thinking: false,
            effort: false,
        };
        let sonnet = ModelOption {
            adaptive_thinking: true,
            effort: true,
            context_window: Some(1_000_000),
            id: CandidateId::new("claude", "sonnet"),
            api_model: "claude-sonnet-5".to_string(),
            ..haiku.clone()
        };
        assert!(!haiku.accepts(&wire));
        assert!(sonnet.accepts(&wire));

        // A request with neither feature is fine on Haiku.
        let plain = request_wire_shape(&json!({"max_tokens": 4096}));
        assert!(haiku.accepts(&plain));
    }

    #[test]
    fn scored_context_windows_match_the_shipped_model_generation() {
        // The gate that blocked every demotion: a 214k-token request (the real
        // median in a Kory Code session) is only reroutable if the score table
        // knows Opus 5 and Sonnet 5 hold 1M tokens. Haiku 4.5 really is 200k.
        let table = crate::candidate::ScoreTable::builtin();
        for (model, expected) in [
            ("claude-fable-5[1m]", 1_000_000),
            ("opus[1m]", 1_000_000),
            ("sonnet", 1_000_000),
            ("haiku", 200_000),
        ] {
            let scores = table.lookup(&CandidateId::new("claude", model));
            assert_eq!(
                scores.context_window,
                Some(expected),
                "context window for {model}"
            );
        }
        let haiku = table.lookup(&CandidateId::new("claude", "haiku"));
        assert!(!haiku.adaptive_thinking && !haiku.effort);
        let sonnet = table.lookup(&CandidateId::new("claude", "sonnet"));
        assert!(sonnet.adaptive_thinking && sonnet.effort);
    }

    #[test]
    fn usage_parses_anthropic_and_openai_sse() {
        let anthropic = b"event: message_start\ndata: {\"message\":{\"usage\":{\"input_tokens\":100,\"cache_read_input_tokens\":80,\"cache_creation_input_tokens\":5}}}\n\nevent: message_delta\ndata: {\"usage\":{\"output_tokens\":20}}\n\n";
        assert_eq!(
            parse_response_usage(anthropic, LlmWireProtocol::Anthropic),
            LlmRequestUsage {
                input: 100,
                output: 20,
                cache_read: 80,
                cache_write: 5,
                input_includes_cache: false,
            }
        );
        let openai = br#"{"usage":{"input_tokens":100,"output_tokens":20,"input_tokens_details":{"cached_tokens":80}}}"#;
        let usage = parse_response_usage(openai, LlmWireProtocol::Openai);
        assert_eq!(usage.cache_read, 80);
        assert!(usage.input_includes_cache);
    }

    #[test]
    fn automation_hint_accepts_relay_tags() {
        let mut meta = agent_client_protocol::schema::v1::Meta::new();
        meta.insert("router_acp".to_string(), json!({"request_hint": "ci-poll"}));
        assert!(automation_hint(Some(&meta)));
    }

    #[test]
    fn codex_proxy_env_installs_http_provider_without_losing_config() {
        let env = codex_proxy_env(
            vec![
                (
                    "CODEX_CONFIG".into(),
                    r#"{"model":"gpt-5.6-sol","sandbox_mode":"workspace-write"}"#.into(),
                ),
                ("MODEL_PROVIDER".into(), "legacy".into()),
            ],
            "http://127.0.0.1:4321/proxy/token/backend-api/codex",
        );
        let provider = env
            .iter()
            .find(|(name, _)| name == "MODEL_PROVIDER")
            .map(|(_, value)| value.as_str());
        assert_eq!(provider, Some("router-acp-proxy"));
        let raw = env
            .iter()
            .find(|(name, _)| name == "CODEX_CONFIG")
            .map(|(_, value)| value)
            .unwrap();
        let config: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(config["model"], "gpt-5.6-sol");
        assert_eq!(config["sandbox_mode"], "workspace-write");
        assert_eq!(
            config["model_providers"]["router-acp-proxy"]["wire_api"],
            "responses"
        );
        assert_eq!(
            config["model_providers"]["router-acp-proxy"]["requires_openai_auth"],
            true
        );
        assert_eq!(
            config["model_providers"]["router-acp-proxy"]["supports_websockets"],
            false
        );
    }

    #[test]
    fn anthropic_proxy_env_enables_tool_search() {
        let env = anthropic_proxy_env(vec![(
            "ANTHROPIC_BASE_URL".into(),
            "http://127.0.0.1:4321/proxy/token/v1".into(),
        )]);
        let tool_search = env
            .iter()
            .find(|(name, _)| name == "ENABLE_TOOL_SEARCH")
            .map(|(_, value)| value.as_str());
        assert_eq!(tool_search, Some("true"));
    }

    #[test]
    fn anthropic_proxy_env_keeps_an_explicit_operator_setting() {
        let env = anthropic_proxy_env(vec![
            ("ANTHROPIC_BASE_URL".into(), "http://127.0.0.1:4321".into()),
            ("ENABLE_TOOL_SEARCH".into(), "false".into()),
        ]);
        let values: Vec<&str> = env
            .iter()
            .filter(|(name, _)| name == "ENABLE_TOOL_SEARCH")
            .map(|(_, value)| value.as_str())
            .collect();
        assert_eq!(values, vec!["false"]);
    }

    #[test]
    fn begin_turn_resets_stale_model_attribution_on_repin() {
        let (_dir, shared) = policy_shared(0);
        let first = shared.llm_proxy.begin_turn(
            ProcessKey("mock".into()),
            "r1".into(),
            "r1".into(),
            "d1".into(),
            CandidateId::new("claude", "sonnet"),
            TaskClass::CodingGeneral,
            None,
        );
        assert_eq!(
            shared.llm_proxy.last_attribution("r1").unwrap().0,
            "claude/sonnet"
        );
        drop(first);

        let second = shared.llm_proxy.begin_turn(
            ProcessKey("mock".into()),
            "r1".into(),
            "r1".into(),
            "d2".into(),
            CandidateId::new("codex", "gpt-5.6-sol"),
            TaskClass::CodingGeneral,
            None,
        );
        assert_eq!(
            shared.llm_proxy.last_attribution("r1").unwrap().0,
            "codex/gpt-5.6-sol"
        );
        assert_eq!(
            shared.llm_proxy.current_model("r1").as_deref(),
            Some("gpt-5.6-sol")
        );
        drop(second);
    }

    #[test]
    fn only_inference_endpoints_are_rewritten() {
        assert!(is_inference_endpoint(
            LlmWireProtocol::Anthropic,
            "v1/messages"
        ));
        assert!(!is_inference_endpoint(
            LlmWireProtocol::Anthropic,
            "v1/messages/count_tokens"
        ));
        assert!(is_inference_endpoint(
            LlmWireProtocol::Openai,
            "backend-api/codex/responses"
        ));
        assert!(is_inference_endpoint(
            LlmWireProtocol::Openai,
            "v1/chat/completions"
        ));
        assert!(!is_inference_endpoint(LlmWireProtocol::Openai, "v1/models"));
    }

    #[test]
    fn upstream_url_preserves_configured_and_incoming_queries() {
        let target = ProxyTarget {
            key: ProcessKey("mock".into()),
            agent: "mock".into(),
            config: AgentLlmProxyConfig {
                protocol: LlmWireProtocol::Openai,
                base_url_env: "MOCK_URL".into(),
                upstream_base_url: "https://example.test/v1?api-version=1".into(),
                codex_chatgpt_provider: false,
            },
        };
        let incoming: Uri = "/proxy/token/v1/responses?trace=2".parse().unwrap();
        let url = upstream_url(&target, "v1/responses", &incoming).unwrap();
        assert_eq!(
            url.as_str(),
            "https://example.test/v1/responses?api-version=1&trace=2"
        );
    }

    #[test]
    fn model_rewrite_preserves_every_other_request_byte() {
        let body = br#"{ "z":1, "model" : "opus", "tools": [{"input_schema":{"b":2,"a":1}}] }"#;
        let rewritten = rewrite_top_level_model(body, "claude-haiku-test").unwrap();
        assert_eq!(
            rewritten,
            br#"{ "z":1, "model" : "claude-haiku-test", "tools": [{"input_schema":{"b":2,"a":1}}] }"#
        );
    }

    #[test]
    fn policy_demotes_routine_requests_and_escalates_failures() {
        let (_dir, shared) = policy_shared(0);
        let active = active("opus");
        let routine = json!({
            "model": "opus",
            "messages": [{"role":"user","content":[{
                "type":"tool_result","content":"git status clean","status":"completed"
            }]}]
        });
        let decision = select_request_model(&shared, &active, &routine);
        assert_eq!(decision.model, "haiku");
        assert_eq!(decision.event, "demotion");

        let failed = json!({
            "model": "opus",
            "messages": [{"role":"user","content":[{
                "type":"tool_result","content":"tests failed","is_error":true
            }]}]
        });
        let decision = select_request_model(&shared, &active, &failed);
        assert_eq!(decision.model, "opus");
        assert_eq!(decision.event, "escalation");
    }

    #[test]
    fn difficulty_escalation_cannot_exceed_session_pin() {
        let (_dir, shared) = kory_code_shared();
        let active = claude_active("sonnet");
        let mut failed = kory_code_request(20, "tests failed");
        failed["model"] = json!("claude-sonnet-5");

        let decision = select_request_model(&shared, &active, &failed);
        assert_eq!(decision.event, "escalation");
        assert_eq!(decision.model, "claude-sonnet-5");
        assert_eq!(decision.selected_model, "sonnet");

        let neutral =
            json!({"model":"claude-sonnet-5","messages":[{"role":"user","content":"continue"}]});
        let verdict = select_request_model(&shared, &active, &neutral);
        assert_eq!(verdict.event, "verdict");
        assert_eq!(verdict.model, "claude-sonnet-5");
        assert_eq!(verdict.selected_model, "sonnet");
    }

    #[test]
    fn dwell_cannot_retain_a_previously_over_ceiling_model() {
        let (_dir, shared) = policy_shared(12);
        let active = active("sonnet");
        shared.llm_proxy.policy.lock().unwrap().insert(
            "r1".to_string(),
            RequestPolicyState {
                pinned_candidate: Some(active.candidate.clone()),
                current_model: Some("opus".to_string()),
                ..Default::default()
            },
        );

        let decision = select_request_model(&shared, &active, &json!({"model":"sonnet"}));
        assert_eq!(decision.event, "dwell");
        assert_eq!(decision.model, "sonnet");
        assert_eq!(decision.selected_model, "sonnet");
    }

    #[test]
    fn cache_reprime_break_even_matches_configured_rates() {
        let (_d, shared) = policy_shared_priced(0);
        // (target_write 3.75 − pinned_read 1.00) / (pinned_read 1.00 − target_read 0.30)
        // = 2.75 / 0.70 = 3.93 → 4 warm turns.
        assert_eq!(
            cache_reprime_break_even(
                &shared,
                &CandidateId::new("mock", "opus"),
                &CandidateId::new("mock", "haiku"),
            ),
            Some(4)
        );
        // The OpenAI-style harness has no cache pricing → gate disabled.
        let (_d2, plain) = policy_shared(0);
        assert_eq!(
            cache_reprime_break_even(
                &plain,
                &CandidateId::new("mock", "opus"),
                &CandidateId::new("mock", "haiku"),
            ),
            None
        );
    }

    #[test]
    fn demotion_waits_for_cache_reprime_break_even_then_demotes() {
        let (_d, shared) = policy_shared_priced(0);
        let active = active("opus");
        let routine = json!({
            "model": "opus",
            "messages": [{"role":"user","content":[{
                "type":"tool_result","content":"git status clean","status":"completed"
            }]}]
        });
        // routine_streak config is 1, but the Fable-like re-prime break-even is 4:
        // the first three routine turns hold on the warm pinned model, and only the
        // fourth — once the run is long enough to amortize haiku's cache write —
        // demotes.
        for _ in 0..3 {
            let decision = select_request_model(&shared, &active, &routine);
            assert_eq!(decision.model, "opus");
            assert_eq!(decision.event, "cache-hold");
        }
        let decision = select_request_model(&shared, &active, &routine);
        assert_eq!(decision.model, "haiku");
        assert_eq!(decision.event, "demotion");
    }

    #[test]
    fn policy_enforces_dwell_and_expires_difficulty_verdicts() {
        let (_dir, shared) = policy_shared(2);
        let active = active("opus");
        let routine = json!({
            "model": "opus",
            "messages": [{"role":"user","content":[{
                "type":"tool_result","content":"git status clean","status":"completed"
            }]}]
        });
        assert_eq!(
            select_request_model(&shared, &active, &routine).event,
            "dwell"
        );
        assert_eq!(
            select_request_model(&shared, &active, &routine).event,
            "dwell"
        );
        assert_eq!(
            select_request_model(&shared, &active, &routine).model,
            "haiku"
        );

        let failed = json!({
            "model": "opus",
            "input": [{"type":"function_call_output","exit_code":1}]
        });
        assert_eq!(
            select_request_model(&shared, &active, &failed).event,
            "escalation"
        );
        let neutral = json!({"model":"opus","messages":[{"role":"user","content":"continue"}]});
        assert_eq!(
            select_request_model(&shared, &active, &neutral).event,
            "verdict"
        );
        assert_eq!(
            select_request_model(&shared, &active, &neutral).event,
            "expiry"
        );
    }

    #[test]
    fn policy_resets_when_the_acp_session_is_repinned() {
        let (_dir, shared) = policy_shared(0);
        let routine = json!({
            "model": "opus",
            "messages": [{"role":"user","content":[{
                "type":"tool_result","content":"git status clean","status":"completed"
            }]}]
        });
        assert_eq!(
            select_request_model(&shared, &active("opus"), &routine).selected_model,
            "haiku"
        );

        let neutral = json!({"model":"sonnet","messages":[{"role":"user","content":"continue"}]});
        let decision = select_request_model(&shared, &active("sonnet"), &neutral);
        assert_eq!(decision.selected_model, "sonnet");
        assert_eq!(decision.event, "steady");
        assert_eq!(
            shared.llm_proxy.current_model("r1").as_deref(),
            Some("sonnet")
        );
    }

    #[tokio::test]
    async fn proxy_streams_rewrites_preserves_auth_and_accounts_requests() {
        #[derive(Clone)]
        struct Seen(Arc<Mutex<Vec<(String, String, String)>>>);

        async fn upstream(
            State(seen): State<Seen>,
            headers: HeaderMap,
            axum::Json(body): axum::Json<Value>,
        ) -> Response<Body> {
            seen.0.lock().unwrap().push((
                body["model"].as_str().unwrap_or("").to_string(),
                headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_string(),
                headers
                    .get("accept-encoding")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_string(),
            ));
            if body["model"] == "claude-haiku-test"
                && body.to_string().contains("force-proxy-fallback")
            {
                return error_response(StatusCode::BAD_REQUEST, "unsupported alternate model");
            }
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(
                    "event: message_start\n\
                     data: {\"message\":{\"usage\":{\"input_tokens\":100,\"cache_read_input_tokens\":80}}}\n\n\
                     event: message_delta\n\
                     data: {\"usage\":{\"output_tokens\":20}}\n\n",
                ))
                .unwrap()
        }

        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let seen = Seen(Arc::new(Mutex::new(Vec::new())));
        let upstream_app = Router::new()
            .route("/v1/messages", axum::routing::post(upstream))
            .with_state(seen.clone());
        let upstream_task = tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("state.db");
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
  routine_streak: 1
  minimum_dwell_requests: 0
agents:
  - name: mock
    command: {{ type: stdio, command: mock-agent }}
    model_selection: {{ type: config-option }}
    llm_proxy:
      protocol: anthropic
      base_url_env: MOCK_BASE_URL
      upstream_base_url: http://{upstream_addr}/v1
    models:
      - id: haiku
        api_model: claude-haiku-test
        cost_rank: 1
        pricing: {{ input_per_mtok: 1, output_per_mtok: 5 }}
      - {{ id: opus, cost_rank: 3 }}
"#,
            state_path.display()
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg).unwrap();
        shared.set_models_routeable(
            &ProcessKey("mock".to_string()),
            vec!["haiku".into(), "opus".into()],
        );
        shared.state.lock().unwrap().upsert(
            "r1".to_string(),
            PersistedSession {
                agent: "mock".to_string(),
                model: "opus".to_string(),
                downstream_session_id: "d1".to_string(),
                cwd: dir.path().to_path_buf(),
                kind: "primary".to_string(),
                ..Default::default()
            },
        );
        let proxy_task = shared.llm_proxy.bind(shared.clone()).await.unwrap();
        let spec = shared.target_spec(&ProcessKey("mock".to_string())).unwrap();
        let process_env = shared.llm_proxy.process_env(&spec);
        // An interposed anthropic target also carries the tool-search override.
        assert_eq!(
            process_env
                .iter()
                .find(|(name, _)| name == "ENABLE_TOOL_SEARCH")
                .map(|(_, value)| value.as_str()),
            Some("true")
        );
        let proxy_base = process_env
            .into_iter()
            .find(|(name, _)| name == "MOCK_BASE_URL")
            .unwrap()
            .1;
        let _turn = shared.llm_proxy.begin_turn(
            ProcessKey("mock".to_string()),
            "r1".to_string(),
            "r1".to_string(),
            "d1".to_string(),
            CandidateId::new("mock", "opus"),
            TaskClass::CodingGeneral,
            None,
        );

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{proxy_base}/messages"))
            .header("authorization", "Bearer secret")
            .json(&json!({
                "model": "opus",
                "messages": [{"role":"user","content":[{
                    "type":"tool_result","content":"git status clean","status":"completed"
                }]}]
            }))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let response_text = response.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{response_text}");
        assert!(response_text.contains("message_start"));

        let response = client
            .post(format!("{proxy_base}/messages"))
            .header("authorization", "Bearer secret")
            .json(&json!({
                "model": "opus",
                "messages": [{"role":"user","content":[{
                    "type":"tool_result",
                    "content":"git status force-proxy-fallback",
                    "status":"completed"
                }]}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.text().await.unwrap();

        let seen = seen.0.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![
                (
                    "claude-haiku-test".into(),
                    "Bearer secret".into(),
                    "identity".into(),
                ),
                (
                    "claude-haiku-test".into(),
                    "Bearer secret".into(),
                    "identity".into(),
                ),
                ("opus".into(), "Bearer secret".into(), "identity".into()),
            ]
        );
        let db = rusqlite::Connection::open(&state_path).unwrap();
        let (model, event, input, cache, cost): (String, String, i64, i64, f64) = db
            .query_row(
                "SELECT model, routing_event, tokens_input, tokens_cache_read, cost_usd
                 FROM llm_requests LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(model, "mock/haiku");
        assert_eq!(event, "demotion");
        assert_eq!((input, cache), (100, 80));
        assert!(cost > 0.0);
        let fallback: (String, String) = db
            .query_row(
                "SELECT model, routing_event FROM llm_requests
                 WHERE routing_event='proxy-fallback'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(fallback, ("mock/opus".into(), "proxy-fallback".into()));

        proxy_task.abort();
        upstream_task.abort();
    }
}
