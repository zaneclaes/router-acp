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

    let outbound_body = match decision.as_ref() {
        Some(decision) if decision.rewrite => {
            rewrite_top_level_model(&body, &decision.model).unwrap_or_else(|| body.to_vec())
        }
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
                                "alternate model returned HTTP {failed_status}; retried unchanged \
                                 on the pinned model"
                            ),
                            event: "proxy-fallback".to_string(),
                            estimated_input,
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
    if event != "steady" && event != "pass-through" && event != "dwell" {
        state
            .shared
            .with_session(&active.parent_router_sid, |session| {
                session.pending_disclosure.push(format!(
                    "router-acp · request {event}: {} → {model_id} — {reason}",
                    active.candidate
                ));
            });
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

#[derive(Debug)]
struct RequestSignals {
    original_model: Option<String>,
    estimated_input: u64,
    routine: bool,
    difficulty: Option<String>,
    test_fingerprint: Option<String>,
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
}

#[derive(Clone)]
struct ModelOption {
    id: CandidateId,
    api_model: String,
    cost_rank: u32,
    quality: f64,
    context_window: Option<u64>,
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
            }
        })
        .collect();
    drop(headroom);
    if !models.iter().any(|model| model.id == active.candidate) {
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
        });
    }
    let compatible: Vec<ModelOption> = models
        .iter()
        .filter(|model| {
            model.id == active.candidate
                || model.context_window.is_some_and(|window| {
                    signals.estimated_input
                        <= (window as f64 * shared.cfg.llm_proxy.context_window_fraction) as u64
                })
        })
        .cloned()
        .collect();

    let mut policy = shared.llm_proxy.policy.lock().unwrap();
    let state = policy.entry(active.state_sid.clone()).or_default();
    if state.pinned_candidate.as_ref() != Some(&active.candidate) {
        *state = RequestPolicyState {
            pinned_candidate: Some(active.candidate.clone()),
            current_model: Some(active.candidate.model.clone()),
            ..Default::default()
        };
    }
    state.request_seq += 1;
    let request_seq = state.request_seq;

    let mut difficulty = state.pending_difficulty.take().or(signals.difficulty);
    if let Some(fingerprint) = signals.test_fingerprint {
        if state.last_test_fingerprint.as_ref() == Some(&fingerprint) {
            state.repeated_test_output += 1;
        } else {
            state.repeated_test_output = 0;
            state.last_test_fingerprint = Some(fingerprint);
        }
        if state.repeated_test_output >= 1 {
            difficulty = Some("test output stagnated across consecutive actions".to_string());
        }
    }

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

    let (desired, mut reason, mut event, emergency) = if let Some(reason) = difficulty {
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
        let strongest = strongest_model(&compatible).unwrap_or_else(|| active.candidate.clone());
        (
            strongest,
            format!("difficulty signal: {reason}"),
            "escalation".to_string(),
            true,
        )
    } else if elevated_active {
        let strongest = strongest_model(&compatible).unwrap_or_else(|| active.candidate.clone());
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
            (
                cheap,
                format!("routine tool-result streak {}", state.routine_streak),
                "demotion".to_string(),
                false,
            )
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
        let selected = models
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
        selected_model: selected.model,
        reason,
        event,
        estimated_input: signals.estimated_input,
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
    let failure_markers = [
        "test failed",
        "tests failed",
        "error:",
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
        test_fingerprint: test_fingerprint(body),
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

fn test_fingerprint(value: &Value) -> Option<String> {
    fn visit(value: &Value, latest: &mut Option<String>) {
        match value {
            Value::Object(object) => {
                let kind = object
                    .get("type")
                    .or_else(|| object.get("role"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if matches!(
                    kind,
                    "tool_result" | "function_call_output" | "tool" | "computer_output"
                ) {
                    let output = object
                        .get("output")
                        .or_else(|| object.get("content"))
                        .map(|value| match value {
                            Value::String(text) => text.clone(),
                            other => serde_json::to_string(other).unwrap_or_default(),
                        })
                        .unwrap_or_default()
                        .to_ascii_lowercase();
                    if [
                        "test result: failed",
                        "tests failed",
                        "failures:",
                        " failed",
                        "error:",
                    ]
                    .iter()
                    .any(|marker| output.contains(marker))
                    {
                        *latest = Some(output);
                    }
                }
                for nested in object.values() {
                    visit(nested, latest);
                }
            }
            Value::Array(values) => {
                for nested in values {
                    visit(nested, latest);
                }
            }
            _ => {}
        }
    }

    let mut latest = None;
    visit(value, &mut latest);
    latest.map(|output| format!("{:x}", Sha256::digest(output.trim().as_bytes())))
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
    fn routine_detection_survives_large_anthropic_tools_prelude() {
        // Reproduces the Claude/Anthropic wire shape: the current turn's
        // tool_result sits inside `messages`, ahead of a large `system` and
        // `tools` prelude. A fixed character-tail of the serialized body lands
        // in the tool schemas and misses the tool_result entirely; structural
        // detection must still classify it as routine.
        let big_tool_schema: String = (0..4000)
            .map(|i| format!("{{\"name\":\"synthetic_field_{i}\",\"desc\":\"padding schema entry\"}}"))
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
        assert!(signal.routine, "buried tool_result must still read as routine");
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
        assert!(signal.difficulty.is_none(), "recovered turn must not stay escalated");
        assert!(signal.routine);
    }

    #[test]
    fn repeated_failing_test_output_escalates_despite_new_trace_items() {
        let (_dir, shared) = policy_shared(0);
        let first = json!({
            "model":"haiku",
            "input":[{
                "type":"function_call_output",
                "output":"test result: FAILED. 1 passed; 1 failed"
            }]
        });
        let second = json!({
            "model":"haiku",
            "input":[
                {"role":"assistant","content":"I changed src/lib.rs"},
                {"type":"function_call_output","output":"test result: FAILED. 1 passed; 1 failed"}
            ]
        });
        assert_eq!(
            select_request_model(&shared, &active("haiku"), &first).event,
            "steady"
        );
        let decision = select_request_model(&shared, &active("haiku"), &second);
        assert_eq!(decision.event, "escalation");
        assert_eq!(decision.selected_model, "opus");
        assert!(decision.reason.contains("stagnated"));
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
        let proxy_base = shared
            .llm_proxy
            .process_env(&spec)
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
