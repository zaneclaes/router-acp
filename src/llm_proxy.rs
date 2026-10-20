//! Per-LLM-request HTTP interposition and routing.
//!
//! Adapter base URLs are process-scoped, while ACP sessions are not. The
//! router therefore attributes an inference request to the active downstream
//! prompt on that process. A request is rewritten only when attribution is
//! unambiguous (or an adapter-provided session marker identifies one active
//! prompt); ambiguous traffic passes through unchanged.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, Request, Response, StatusCode, Uri};
use axum::routing::any;
use futures::StreamExt;
#[cfg(test)]
use serde_json::value::RawValue;
use serde_json::{Value, json};

use crate::candidate::{CandidateId, EffortLevel, TaskClass};
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
    response_status: Arc<AtomicU16>,
    parent_router_sid: String,
    state_sid: String,
    downstream_sid: String,
    candidate: CandidateId,
    class: TaskClass,
}

#[derive(Debug, Default)]
struct RequestPolicyState {
    pinned_candidate: Option<CandidateId>,
    current_model: Option<String>,
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
    /// The latest thinking-enabled request asked for `thinking.display:
    /// "omitted"`. Such a response returns the model's reasoning as empty,
    /// signature-only blocks, so the only thinking text it can carry is the
    /// server's narration: its summary of the prose the model wrote for the
    /// user between tool calls. Read by the relay path to forward that text as
    /// a message instead of a thought.
    thinking_text_is_narration: bool,
}

/// Shared proxy runtime. Constructed with `Shared`, bound when `serve_shared`
/// starts, and consulted by downstream process creation and prompt dispatch.
pub struct LlmProxyRuntime {
    enabled: bool,
    listen_addr: OnceLock<std::net::SocketAddr>,
    targets_by_token: Mutex<HashMap<String, ProxyTarget>>,
    token_by_key: Mutex<HashMap<ProcessKey, String>>,
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
            .field("targets", &self.targets_by_token.lock().unwrap().len())
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
            let token = uuid::Uuid::new_v4().to_string();
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
            targets_by_token: Mutex::new(targets_by_token),
            token_by_key: Mutex::new(token_by_key),
            active: Mutex::new(HashMap::new()),
            policy: Mutex::new(HashMap::new()),
            next_registration: AtomicU64::new(1),
            client,
        }))
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn register_agent(
        &self,
        agent: &crate::config::AgentConfig,
        specs: &[ProcessTargetSpec],
    ) {
        let Some(config) = &agent.llm_proxy else {
            return;
        };
        for spec in specs {
            // The route grants access to managed account credentials. Preserve
            // it for existing adapters, but never derive it from a public key.
            let token = self
                .token_by_key
                .lock()
                .unwrap()
                .get(&spec.key)
                .cloned()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            self.targets_by_token.lock().unwrap().insert(
                token.clone(),
                ProxyTarget {
                    key: spec.key.clone(),
                    agent: agent.name.clone(),
                    config: config.clone(),
                },
            );
            self.token_by_key
                .lock()
                .unwrap()
                .insert(spec.key.clone(), token);
        }
    }

    pub(crate) fn remove_agent(&self, agent: &str) {
        let keys: Vec<_> = {
            let mut targets = self.targets_by_token.lock().unwrap();
            let keys = targets
                .values()
                .filter(|t| t.agent == agent)
                .map(|t| t.key.clone())
                .collect();
            targets.retain(|_, target| target.agent != agent);
            keys
        };
        self.token_by_key
            .lock()
            .unwrap()
            .retain(|key, _| !keys.contains(key));
        self.active
            .lock()
            .unwrap()
            .retain(|key, _| !keys.contains(key));
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
        tracing::info!(%addr, targets = self.targets_by_token.lock().unwrap().len(), "LLM request proxy listening");
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
        let Some(token) = self.token_by_key.lock().unwrap().get(&spec.key).cloned() else {
            return env;
        };
        let Some(target) = self.targets_by_token.lock().unwrap().get(&token).cloned() else {
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
    ) -> LlmTurnGuard {
        if !self.enabled || !self.token_by_key.lock().unwrap().contains_key(&process_key) {
            return LlmTurnGuard {
                runtime: None,
                process_key,
                registration_id: 0,
                response_status: None,
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
        let response_status = Arc::new(AtomicU16::new(0));
        let active = ActiveTurn {
            registration_id,
            response_status: response_status.clone(),
            parent_router_sid,
            state_sid,
            downstream_sid,
            candidate,
            class,
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
            response_status: Some(response_status),
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

    /// Whether thinking text streamed for this session is the server's
    /// narration of the model's user-facing prose rather than its reasoning
    /// (see `RequestPolicyState::thinking_text_is_narration`).
    pub fn thinking_text_is_narration(&self, state_sid: &str) -> bool {
        self.policy
            .lock()
            .unwrap()
            .get(state_sid)
            .is_some_and(|s| s.thinking_text_is_narration)
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
}

/// Keeps one downstream prompt registered for exact HTTP attribution.
pub struct LlmTurnGuard {
    runtime: Option<Arc<LlmProxyRuntime>>,
    process_key: ProcessKey,
    registration_id: u64,
    response_status: Option<Arc<AtomicU16>>,
}

impl LlmTurnGuard {
    pub fn last_response_status(&self) -> Option<u16> {
        self.response_status
            .as_ref()
            .map(|status| status.load(Ordering::Acquire))
            .filter(|status| *status != 0)
    }
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
    let target = state
        .runtime
        .targets_by_token
        .lock()
        .unwrap()
        .get(&token)
        .cloned();
    let Some(target) = target else {
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
            decision.fold_system_turns,
        )
        .unwrap_or_else(|| body.to_vec()),
        _ => body.to_vec(),
    };
    // A version without system turns gets neither the turns nor the beta
    // tokens that announce them — a beta the model lacks is itself a 400.
    let mut outbound_headers = if decision.as_ref().is_some_and(|d| d.fold_system_turns) {
        strip_system_turn_betas(&parts.headers)
    } else {
        parts.headers.clone()
    };
    let mut wire_effort = decision.as_ref().map(|d| {
        confirmed_wire_effort(
            &state.shared,
            &CandidateId::new(&target.agent, &d.selected_model),
            &outbound_body,
            target.config.protocol,
        )
    });

    let upstream_url = match upstream_url(&target, &rest, &parts.uri) {
        Ok(url) => url,
        Err(err) => return error_response(StatusCode::BAD_GATEWAY, &err),
    };
    // Authentication belongs to the account, even when concurrent ACP turns
    // make model attribution ambiguous. Never use the adapter's launch token
    // as a fallback for a removed or unreadable managed credential.
    let managed_agent = state.shared.agent_configs().into_iter().find(|agent| {
        agent.name == target.agent
            && target.config.protocol == LlmWireProtocol::Anthropic
            && crate::accounts::provider(agent) == Some("claude")
    });
    let mut observed = None;
    if let Some(agent) = &managed_agent {
        match crate::credentials::claude_access(agent).await {
            Ok(access) => {
                if set_claude_authorization(&mut outbound_headers, &access.token).is_err() {
                    return credential_error(crate::credentials::RepairOutcome::Unknown);
                }
                observed = Some(access.generation);
            }
            Err(outcome) => {
                crate::auth::sync_from_manager(&state.shared, agent);
                return credential_error(outcome);
            }
        }
    }
    let request_id = format!("llm-{}", uuid::Uuid::new_v4());
    let started = Instant::now();
    let first_request = build_upstream_request(
        &state.runtime.client,
        &parts.method,
        upstream_url.clone(),
        &outbound_headers,
        outbound_body.clone(),
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

    // Retry only the rejected HTTP request, before any response is streamed.
    // Release that response first: repair takes the shared credential lock and
    // must not pin an upstream connection. A sibling may already have rotated
    // the credential. The manager compares the marker from the exact token sent
    // and reuses that repair. A failed repair is not a second provider call.
    if upstream.status() == StatusCode::UNAUTHORIZED
        && let Some(agent) = &managed_agent
    {
        drop(upstream);
        let outcome = crate::credentials::repair(agent, observed.as_deref()).await;
        crate::auth::sync_from_manager(&state.shared, agent);
        if outcome != crate::credentials::RepairOutcome::Repaired {
            record_proxy_result(
                &state,
                &target,
                attributed.as_ref(),
                decision.as_ref(),
                &rest,
                &body,
                &request_id,
                started,
                outcome_status(&outcome),
                &[],
                Some("credential repair did not replace the rejected request".into()),
            );
            return credential_error(outcome);
        }
        match crate::credentials::claude_access(agent).await {
            Ok(access) => {
                if set_claude_authorization(&mut outbound_headers, &access.token).is_err() {
                    return credential_error(crate::credentials::RepairOutcome::Unknown);
                }
                match build_upstream_request(
                    &state.runtime.client,
                    &parts.method,
                    upstream_url.clone(),
                    &outbound_headers,
                    outbound_body,
                )
                .send()
                .await
                {
                    Ok(response) => upstream = response,
                    Err(err) => {
                        record_proxy_result(
                            &state,
                            &target,
                            attributed.as_ref(),
                            decision.as_ref(),
                            &rest,
                            &body,
                            &request_id,
                            started,
                            StatusCode::BAD_GATEWAY,
                            &[],
                            Some(format!("credential retry failed: {err}")),
                        );
                        return error_response(StatusCode::BAD_GATEWAY, "credential retry failed");
                    }
                }
            }
            Err(outcome) => return credential_error(outcome),
        }
    }

    if (upstream.status().is_client_error() || upstream.status().is_server_error())
        && upstream.status() != StatusCode::UNAUTHORIZED
    {
        // A rejected PIN rewrite is never recovered by retrying the original
        // body. The original names whatever the downstream alias resolves to,
        // so the retry would serve a different model while the session stays
        // attributed and priced as the pinned one — and because the pinned
        // candidate bypasses the `rejected_api_models` filter, it would do
        // that on every single request. Fail loudly and once instead: cordon
        // the candidate with the provider's own reason (so the picker shows it
        // unavailable, automatic routing drops it, and a pin to it is
        // redirected with a visible disclosure), tell the session, and hand
        // the provider's actionable error back to the adapter.
        if let (Some(active), Some(pin_decision)) = (
            attributed.as_ref(),
            decision.as_ref().filter(|decision| decision.honors_pin),
        ) {
            return reject_pinned_rewrite(
                &state,
                &target,
                active,
                pin_decision,
                upstream,
                &rest,
                &body,
                &request_id,
                started,
            )
            .await;
        }
        let retry_alternate = decision.as_ref().is_some_and(|decision| decision.rewrite);
        if retry_alternate {
            let failed_status = upstream.status();
            let alternate_tried =
                attributed
                    .as_ref()
                    .zip(decision.as_ref())
                    .map(|(active, decision)| {
                        CandidateId::new(&active.candidate.agent, &decision.selected_model)
                    });
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
            // The original body may carry system turns, so it needs the
            // original beta headers. Keep the account's current authorization.
            let mut retry_headers = parts.headers.clone();
            if let Some(authorization) = outbound_headers.get(axum::http::header::AUTHORIZATION)
                && managed_agent.is_some()
            {
                retry_headers.insert(axum::http::header::AUTHORIZATION, authorization.clone());
                retry_headers.remove("x-api-key");
            }
            let retry = build_upstream_request(
                &state.runtime.client,
                &parts.method,
                upstream_url,
                &retry_headers,
                body.to_vec(),
            )
            .send()
            .await;
            match retry {
                Ok(response) => {
                    if let Some(active) = attributed.as_ref() {
                        wire_effort = Some(confirmed_wire_effort(
                            &state.shared,
                            &active.candidate,
                            &body,
                            target.config.protocol,
                        ));
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
                        // A 5xx says the alternate model is down: cordon it on
                        // every account for the outage period. A 400/404 is a
                        // durable rejection, recorded above for the session.
                        let alternate = decision.as_ref().map(|decision| {
                            CandidateId::new(&active.candidate.agent, &decision.selected_model)
                        });
                        let outage = failed_status.is_server_error();
                        if outage && let Some(alternate) = &alternate {
                            state.shared.cordon_model_outage(
                                alternate,
                                &format!("provider outage (HTTP {failed_status})"),
                            );
                        }
                        let afterwards = if outage {
                            format!(
                                "model cordoned for {}",
                                crate::limits::humanize(std::time::Duration::from_secs(
                                    state.shared.cfg.failover.outage_cordon_secs
                                ))
                            )
                        } else {
                            "not retried again this session".to_string()
                        };
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
                            honors_pin: false,
                            reason: format!(
                                "alternate model `{attempted}` returned HTTP {failed_status}; \
                                 retried unchanged on {} ({afterwards})",
                                active.candidate
                            ),
                            event: "proxy-fallback".to_string(),
                            fold_system_turns: false,
                            estimated_input,
                            effort: request_effort(
                                &state.shared,
                                &active.parent_router_sid,
                                &active.state_sid,
                                state
                                    .shared
                                    .with_session(&active.state_sid, |s| s.version_request.clone())
                                    .flatten()
                                    .as_deref(),
                                &active.candidate,
                            ),
                        });
                    }
                    if let (Some(active), Some(fallback)) = (attributed.as_ref(), decision.as_ref())
                        && let Some(alternate) = alternate_tried.as_ref()
                    {
                        let first_time = state
                            .runtime
                            .policy
                            .lock()
                            .unwrap()
                            .entry(active.state_sid.clone())
                            .or_default()
                            .disclosed_events
                            .insert(format!("proxy-fallback:{alternate}"));
                        if first_time {
                            crate::session::queue_proxy_fallback(
                                &state.shared,
                                &active.parent_router_sid,
                                alternate,
                                &active.candidate,
                                failed_status.is_server_error(),
                                &fallback.reason,
                            );
                        }
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
    if status.is_success()
        && let (Some(active), Some(effort)) = (attributed.as_ref(), wire_effort.as_ref())
    {
        publish_effective_effort(&state.shared, active, effort);
    }
    let headers = upstream.headers().clone();
    let max_capture = state.shared.cfg.llm_proxy.max_capture_bytes;
    let shared = state.shared.clone();
    let mut upstream_stream = upstream.bytes_stream();
    let mut completion_guard = CompletionGuard::new(shared, completion, status.as_u16());
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

fn set_claude_authorization(headers: &mut HeaderMap, token: &str) -> Result<(), ()> {
    let mut value =
        axum::http::HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| ())?;
    value.set_sensitive(true);
    headers.insert(axum::http::header::AUTHORIZATION, value);
    headers.remove("x-api-key");
    Ok(())
}

fn credential_error(outcome: crate::credentials::RepairOutcome) -> Response<Body> {
    if outcome == crate::credentials::RepairOutcome::Rejected {
        error_response(
            StatusCode::UNAUTHORIZED,
            "Credential refresh rejected. Sign in again with /login.",
        )
    } else {
        error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Managed credential temporarily unavailable",
        )
    }
}

fn outcome_status(outcome: &crate::credentials::RepairOutcome) -> StatusCode {
    if *outcome == crate::credentials::RepairOutcome::Rejected {
        StatusCode::UNAUTHORIZED
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[allow(clippy::too_many_arguments)]
fn record_proxy_result(
    state: &ProxyServerState,
    target: &ProxyTarget,
    active: Option<&ActiveTurn>,
    decision: Option<&RequestDecision>,
    rest: &str,
    body: &[u8],
    request_id: &str,
    started: Instant,
    status: StatusCode,
    captured: &[u8],
    error: Option<String>,
) {
    if let Some(context) = start_request_record(
        state, target, active, decision, rest, body, request_id, started,
    ) {
        complete_request(&state.shared, context, status.as_u16(), captured, error);
    }
}

/// A provider rejection of a PIN rewrite: cordon the pinned candidate with the
/// provider's own reason, tell the session, record the request, and return the
/// provider's response verbatim.
///
/// The two invariants this exists for: never serve a model other than the
/// explicit pin (so no original-body retry), and never do it again next
/// request (so the candidate is cordoned rather than left in rotation). The
/// cordon reuses the reactive per-candidate machinery, so the candidate is
/// dropped from automatic pools, advertised unavailable with its reason in the
/// model picker, and an explicit pin to it is redirected with a disclosure.
///
/// Only 400/404 cordon: those say the provider will not serve this model id.
/// Transient codes (429, 5xx) say nothing about its suitability, so they are
/// surfaced without sidelining the candidate — but they are still never
/// silently retried on a different model. A 400 that rejects a FEATURE of the
/// request rather than the model ([`is_request_shape_rejection`]) does not
/// cordon either: the adapter owns the body and re-sends it without the
/// feature, and the pin serves that retry.
#[allow(clippy::too_many_arguments)]
async fn reject_pinned_rewrite(
    state: &ProxyServerState,
    target: &ProxyTarget,
    active: &ActiveTurn,
    decision: &RequestDecision,
    upstream: reqwest::Response,
    rest: &str,
    body: &[u8],
    request_id: &str,
    started: Instant,
) -> Response<Body> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    // Read the error body: it carries the message the user can act on (for a
    // version-gated model id, literally "run `claude update`"), and we are
    // returning it rather than streaming a replacement.
    let payload = upstream.bytes().await.unwrap_or_default();
    let detail = provider_error_message(&payload);
    let shape_rejection = status == StatusCode::BAD_REQUEST && is_request_shape_rejection(&detail);
    let cordoned =
        !shape_rejection && matches!(status, StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST);

    if shape_rejection {
        tracing::info!(
            target = %target.key,
            candidate = %active.candidate,
            api_model = decision.model,
            detail,
            "pinned model rejected a request feature; the adapter retries without it — pin kept"
        );
    } else if cordoned {
        let reset = std::time::Duration::from_secs(state.shared.cfg.headroom.cordon_default_secs);
        let reason = format!("provider rejected `{}`: {detail}", decision.model);
        state.shared.headroom.lock().unwrap().cordon_candidate(
            &active.candidate,
            reason,
            std::time::SystemTime::now() + reset,
        );
        state.shared.publish_config_options();
        tracing::warn!(
            target = %target.key,
            candidate = %active.candidate,
            api_model = decision.model,
            %status,
            cordon_secs = reset.as_secs(),
            "pinned model rejected by provider; cordoned instead of serving a \
             different model"
        );
    } else {
        tracing::warn!(
            target = %target.key,
            candidate = %active.candidate,
            api_model = decision.model,
            %status,
            "pinned model request failed; surfacing the provider error unchanged"
        );
    }

    let tail = if shape_rejection {
        " — a request feature this version lacks, not the model; the adapter retries \
         without it and the pin is kept"
            .to_string()
    } else if cordoned {
        format!(
            " — cordoned for {} so the next turn routes elsewhere instead of \
             silently serving a different version",
            crate::limits::humanize(std::time::Duration::from_secs(
                state.shared.cfg.headroom.cordon_default_secs
            ))
        )
    } else {
        String::new()
    };
    crate::session::notify_user(
        &state.shared,
        &active.parent_router_sid,
        format!(
            "router-acp · {} rejected by the provider (HTTP {}): {detail}{tail}",
            active.candidate,
            status.as_u16(),
        ),
    );

    let completion = start_request_record(
        state,
        target,
        Some(active),
        Some(decision),
        rest,
        body,
        request_id,
        started,
    );
    if let Some(context) = completion {
        complete_request(
            &state.shared,
            context,
            status.as_u16(),
            &[],
            Some(format!(
                "provider rejected pinned model `{}`: {detail}",
                decision.model
            )),
        );
    }

    let mut response = Response::builder().status(status);
    if let Some(response_headers) = response.headers_mut() {
        copy_response_headers(&headers, response_headers);
    }
    response
        .body(Body::from(payload))
        .unwrap_or_else(|_| error_response(status, &detail))
}

/// A 400 whose message says the REQUEST used a feature this model lacks — not
/// that the model id is unavailable. Claude Code probes newer wire features
/// (mid-conversation `role: "system"` turns, per-turn effort statements, tool
/// additions) as betas and heals this exact rejection itself: it re-sends the
/// body without the feature and sticky-disables the beta for the session. So
/// the pin serves the retry fine — cordoning it instead handed every healed
/// retry to a different model for `cordon_default_secs` on every new session
/// pinned to a legacy version (observed: Opus 4.6 on `role 'system' is not
/// supported on this model`, once per session, 14 times in three days).
/// The version-gate rejection ("… does not support this model … run `claude
/// update`") is about the model id and keeps cordoning.
fn is_request_shape_rejection(detail: &str) -> bool {
    let lower = detail.to_lowercase();
    lower.contains("not supported on this model")
        || lower.contains("requires a model that supports")
        || lower.contains("role 'system'")
        || lower.contains("unexpected role")
        || lower.contains("input tag 'tool_")
        || lower.contains("cache_control")
}

/// The human-readable message out of a provider error body
/// (`{"error":{"message":…}}` on both wires), else a truncated raw body.
fn provider_error_message(payload: &[u8]) -> String {
    let text = String::from_utf8_lossy(payload);
    let message = serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| text.trim().to_string());
    let mut message = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if message.is_empty() {
        message = "no message in the provider response".to_string();
    }
    message.truncate(300);
    message
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
    crate::planner_watchdog::note_progress(&state.shared, &active.state_sid);
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
    // Disclosed once per (alternate, session) where the fallback happens; see
    // `session::queue_proxy_fallback`.
    Some(CompletionContext {
        request_id: request_id.to_string(),
        response_status: active.response_status.clone(),
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

/// Whether a thinking-enabled request omits the reasoning text from its
/// response (`thinking.display: "omitted"`, which Claude Code sends). `None`
/// when the request says nothing about it: no `thinking` config, or thinking
/// disabled (Claude Code's WebFetch/WebSearch side calls), whose responses
/// carry no thinking blocks at all.
fn thinking_display_omitted(body: &Value) -> Option<bool> {
    let thinking = body.get("thinking")?;
    if thinking_disabled(body) {
        return None;
    }
    Some(thinking.get("display").and_then(Value::as_str) == Some("omitted"))
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
/// Whether an Anthropic-wire request carries a mid-conversation `role:
/// "system"` turn (Claude Code's `mid-conversation-system` beta).
fn has_system_turn(body: &Value) -> bool {
    body.get("messages")
        .and_then(Value::as_array)
        .is_some_and(|messages| messages.iter().any(is_system_turn))
}

fn is_system_turn(message: &Value) -> bool {
    message.get("role").and_then(Value::as_str) == Some("system")
}

/// `anthropic-beta` tokens that only mean anything alongside `role: "system"`
/// turns: the turns themselves, and the per-turn effort statement that rides
/// on one. Dropped together with the turns.
const SYSTEM_TURN_BETA_PREFIXES: &[&str] = &["mid-conversation-system", "per-turn-control"];

/// Fold Claude Code's mid-conversation `role: "system"` turns into the shape
/// a model without that capability accepts.
///
/// Claude Code picks the wire shape from the alias it was configured with
/// (`opus[1m]` → Opus 5, which has the capability); the proxy then rewrites
/// the model to a pinned legacy version that does not. Left alone, every new
/// session pinned to such a version opened with a 400 that the adapter had to
/// probe, absorb and retry. Instead: an empty turn (a per-turn effort
/// statement — `output_config` on the turn — carries no text) is dropped, and
/// a text-only turn becomes user text, appended to the preceding user turn
/// when there is one, otherwise a user turn of its own. Per-turn
/// `output_config` is dropped with the turn; the request's own top-level
/// effort stands. A turn carrying non-text blocks (tool additions) has no
/// user-turn equivalent, so the body is left untouched and the provider's
/// rejection drives the adapter's own fallback.
fn fold_system_turns_into_user(object: &mut serde_json::Map<String, Value>) -> bool {
    let Some(messages) = object.get("messages").and_then(Value::as_array) else {
        return false;
    };
    if !messages.iter().any(is_system_turn) {
        return false;
    }
    let text_only = |message: &Value| match message.get("content") {
        None | Some(Value::String(_)) => true,
        Some(Value::Array(blocks)) => blocks
            .iter()
            .all(|block| block.get("type").and_then(Value::as_str) == Some("text")),
        Some(_) => false,
    };
    if !messages.iter().filter(|m| is_system_turn(m)).all(text_only) {
        return false;
    }
    let mut folded: Vec<Value> = Vec::with_capacity(messages.len());
    for message in messages.clone() {
        if !is_system_turn(&message) {
            folded.push(message);
            continue;
        }
        let blocks: Vec<Value> = match message.get("content") {
            Some(Value::String(text)) if !text.trim().is_empty() => {
                vec![json!({"type": "text", "text": text})]
            }
            Some(Value::Array(blocks)) => blocks
                .iter()
                .filter(|block| {
                    block
                        .get("text")
                        .and_then(Value::as_str)
                        .is_some_and(|text| !text.trim().is_empty())
                })
                .cloned()
                .collect(),
            _ => Vec::new(),
        };
        if blocks.is_empty() {
            continue;
        }
        match folded.last_mut().and_then(Value::as_object_mut) {
            Some(previous) if previous.get("role").and_then(Value::as_str) == Some("user") => {
                let mut content = match previous.remove("content") {
                    Some(Value::String(text)) => vec![json!({"type": "text", "text": text})],
                    Some(Value::Array(existing)) => existing,
                    _ => Vec::new(),
                };
                content.extend(blocks);
                previous.insert("content".into(), Value::Array(content));
            }
            _ => folded.push(json!({"role": "user", "content": blocks})),
        }
    }
    object.insert("messages".into(), Value::Array(folded));
    true
}

/// The request headers with the system-turn beta tokens removed from every
/// `anthropic-beta` value (a header left empty is dropped).
fn strip_system_turn_betas(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        if name.as_str().eq_ignore_ascii_case("anthropic-beta") {
            let kept: Vec<&str> = value
                .to_str()
                .unwrap_or("")
                .split(',')
                .map(str::trim)
                .filter(|token| {
                    !token.is_empty()
                        && !SYSTEM_TURN_BETA_PREFIXES
                            .iter()
                            .any(|prefix| token.starts_with(prefix))
                })
                .collect();
            if kept.is_empty() {
                continue;
            }
            if let Ok(joined) = axum::http::HeaderValue::from_str(&kept.join(",")) {
                out.append(name.clone(), joined);
            }
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

fn shape_provider_request(
    body: &Value,
    model: Option<&str>,
    effort: &EffortShape,
    protocol: LlmWireProtocol,
    fold_system_turns: bool,
) -> Option<Vec<u8>> {
    let thinking_off = thinking_disabled(body);
    let mut body = body.clone();
    let object = body.as_object_mut()?;
    if let Some(model) = model {
        object.insert("model".into(), Value::String(model.into()));
    }
    if fold_system_turns && protocol == LlmWireProtocol::Anthropic {
        fold_system_turns_into_user(object);
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
    /// The request carries a mid-conversation `role: "system"` turn.
    system_turns: bool,
    max_output: u64,
}

fn request_wire_shape(body: &Value) -> WireShape {
    WireShape {
        system_turns: has_system_turn(body),
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
    /// The rewrite exists only to put the SESSION PIN's own `api_model` on the
    /// wire — the model the session is attributed and priced as — rather than
    /// to route to a router-chosen alternate.
    ///
    /// It gates the alternate-fallback path: retrying the untouched original
    /// body is a legitimate recovery for a rejected ALTERNATE (the pinned
    /// model is what the adapter asked for anyway), but on a pin rewrite the
    /// original names whatever the downstream ALIAS resolves to, so retrying
    /// it would serve a different model under the pin's identity.
    honors_pin: bool,
    reason: String,
    event: String,
    estimated_input: u64,
    /// Candidate-resolved provider-native effort handling for this request.
    effort: EffortShape,
    /// The model on the wire predates mid-conversation `role: "system"` turns:
    /// fold them into user turns and drop the beta tokens before sending.
    fold_system_turns: bool,
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

/// Report the value on the accepted wire, including provider clamps and an
/// unchanged fallback body. An omitted field cannot establish provider effort.
fn confirmed_wire_effort(
    shared: &Shared,
    candidate: &CandidateId,
    body: &[u8],
    protocol: LlmWireProtocol,
) -> Value {
    let body = serde_json::from_slice::<Value>(body).unwrap_or(Value::Null);
    let value = match protocol {
        LlmWireProtocol::Anthropic => body.pointer("/output_config/effort"),
        LlmWireProtocol::Openai => body
            .pointer("/reasoning/effort")
            .or_else(|| body.get("reasoning_effort")),
    }
    .and_then(Value::as_str);
    let scores = shared.scores.lookup_exact(candidate);
    let resolved = value.and_then(|value| {
        EffortLevel::parse(value)
            .filter(|level| *level != EffortLevel::Auto)
            .or_else(|| {
                scores
                    .effort_mapping
                    .iter()
                    .find_map(|(level, mapped)| (mapped == value).then_some(*level))
            })
    });
    json!({"resolved":resolved.map(EffortLevel::as_str),"provider_value":value,"source":"provider-request","confirmed":resolved.is_some()})
}

fn publish_effective_effort(shared: &Shared, active: &ActiveTurn, confirmed: &Value) {
    let effort = {
        let state = shared.state.lock().unwrap();
        let mut effort = state
            .get(&active.state_sid)
            .and_then(|s| s.routing)
            .and_then(|r| r.get("effort").cloned())
            .unwrap_or_else(|| json!({}));
        if !effort.is_object() {
            effort = json!({});
        }
        effort
            .as_object_mut()
            .unwrap()
            .extend(confirmed.as_object().unwrap().clone());
        if let Err(error) =
            state.patch_session_routing(&active.state_sid, &json!({"effort":effort}))
        {
            tracing::warn!(session = active.state_sid, %error, "cannot persist confirmed effort");
        }
        effort
    };
    shared.with_session(&active.state_sid, |s| {
        let pending = s.pending_meta_disclosure.get_or_insert_with(|| json!({}));
        pending["effort"] = effort.clone();
    });
    let child = shared
        .planner_child_routes
        .lock()
        .unwrap()
        .values()
        .find(|r| r.state_sid == active.state_sid)
        .cloned();
    if active.state_sid != active.parent_router_sid && child.is_none() {
        return;
    }
    if let Some(upstream) = shared.upstream() {
        let params = json!({"sessionId":child.as_ref().map(|r| r.child_id.as_str()).unwrap_or(&active.state_sid),
            "attempt_id":child.as_ref().map(|r| &r.attempt_id),"effort":effort});
        if let Ok(msg) =
            agent_client_protocol::UntypedMessage::new("router-acp/effort-update", params)
        {
            let _ = upstream.send_notification(msg);
        }
    }
}

fn request_effort(
    shared: &Shared,
    parent_router_sid: &str,
    state_sid: &str,
    version: Option<&str>,
    candidate: &CandidateId,
) -> EffortShape {
    // A delegate's own `hints.effort` wins over the parent session's effort.
    let delegate = shared
        .delegate_effort
        .lock()
        .unwrap()
        .get(state_sid)
        .copied();
    let requested = delegate.or_else(|| {
        shared
            .with_session(parent_router_sid, |session| {
                session
                    .resolved_effort
                    .as_ref()
                    .map(|resolution| resolution.requested)
            })
            .flatten()
    });
    let explicit = delegate.is_none()
        && shared
            .with_session(parent_router_sid, |s| s.effort_request.is_some())
            .unwrap_or(false);
    match requested {
        None => EffortShape::Preserve,
        Some(level) => {
            let scores = turn_scores(shared, version, candidate);
            let resolution = if explicit {
                scores.resolve_effort(level)
            } else {
                scores.resolve_automatic_effort(level)
            };
            resolution
                .provider_value
                .map(EffortShape::Set)
                .unwrap_or(EffortShape::Omit)
        }
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
    system_turns: bool,
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
        if wire.system_turns && !self.system_turns {
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
    // A coordinator's requests only move within `planning_candidates`; its
    // own pin is re-added below regardless (a human may have picked it).
    let (coordinator, version) = shared
        .with_session(&active.state_sid, |s| {
            (s.coordinator, s.version_request.clone())
        })
        .unwrap_or((false, None));
    let version = version.as_deref();
    let candidates = shared.routeable_candidates();
    let lineage = shared.runtime_config().lineage_of(&active.candidate.agent);
    let mut headroom = shared.headroom.lock().unwrap();
    let mut models: Vec<ModelOption> = candidates
        .into_iter()
        .filter(|candidate| candidate.id.agent == active.candidate.agent)
        // Per-request routing is automatic, so a pin-only legacy version is not
        // an alternate here. The session's own pin is re-added below whatever
        // this filter says, so an explicit pin still serves its own requests.
        .filter(|candidate| candidate.auto_eligible)
        .filter(|candidate| !coordinator || shared.in_planning_pool(&candidate.id))
        .filter(|candidate| {
            !headroom.is_quarantined(&candidate.id)
                && headroom.cordon_active(&candidate.id.agent).is_none()
                && headroom.usage_cordon(&candidate.id).is_none()
                && headroom
                    .model_outage(&lineage, &candidate.id.model)
                    .is_none()
                && !headroom.seat_exhausted(&candidate.id)
        })
        .map(|candidate| {
            let scores = turn_scores(shared, version, &candidate.id);
            let api_model = turn_api_model(shared, version, &candidate.id);
            ModelOption {
                id: candidate.id,
                api_model,
                cost_rank: candidate.cost_rank,
                quality: scores.quality(active.class),
                context_window: scores.context_window,
                max_output_tokens: scores.max_output_tokens,
                adaptive_thinking: scores.adaptive_thinking,
                effort: scores.effort,
                system_turns: scores.system_turns,
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
        let scores = turn_scores(shared, version, &active.candidate);
        models.push(ModelOption {
            id: active.candidate.clone(),
            api_model: turn_api_model(shared, version, &active.candidate),
            cost_rank,
            quality: scores.quality(active.class),
            context_window: scores.context_window,
            max_output_tokens: scores.max_output_tokens,
            adaptive_thinking: scores.adaptive_thinking,
            effort: scores.effort,
            system_turns: scores.system_turns,
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
    // Requests stay on the session pin. The proxy moves a request only when the
    // pin is out of plan budget: then it hands the request to the strongest
    // sibling that still has budget. With no routeable sibling the pin stands —
    // this proxy only rewrites a model id, it cannot decline the request.
    let (selected, reason, event) = match strongest_model(&compatible)
        .filter(|alternate| !pin_routeable && alternate != &active.candidate)
    {
        Some(alternate) => (
            alternate,
            "pinned model is out of plan budget".to_string(),
            "exhausted-repin".to_string(),
        ),
        None => (
            active.candidate.clone(),
            "session-pinned model".to_string(),
            "steady".to_string(),
        ),
    };
    state.current_model = Some(selected.model.clone());
    // Record the decision for per-tool-call attribution in the client.
    state.last_candidate = Some(selected.to_string());
    state.last_reason = Some(reason.clone());
    if let Some(omitted) = thinking_display_omitted(body) {
        state.thinking_text_is_narration = omitted;
    }
    let upstream_model = if selected == active.candidate {
        // Staying on the session's own model is NOT a licence to pass the
        // adapter's model string through: the adapter names whatever its
        // alias resolves to on ITS side, which for a pinned version is a
        // DIFFERENT model from the one the session is attributed and priced
        // as. So a declared wire id (pinned version or `api_model`) is
        // authoritative and always written; the incoming string is only
        // trusted when neither is declared, where it is the sole valid wire
        // id we have (rewriting to a bare CLI alias would 404).
        declared_api_model(shared, version, &selected).unwrap_or_else(|| {
            signals
                .original_model
                .clone()
                .unwrap_or_else(|| selected.model.clone())
        })
    } else {
        models
            .iter()
            .find(|model| model.id == selected)
            .map(|model| model.api_model.clone())
            .unwrap_or_else(|| turn_api_model(shared, version, &selected))
    };
    let rewrite = signals
        .original_model
        .as_ref()
        .map_or(selected != active.candidate, |original| {
            original != &upstream_model
        });
    RequestDecision {
        rewrite,
        honors_pin: rewrite && selected == active.candidate,
        model: upstream_model,
        selected_model: selected.model.clone(),
        reason,
        event,
        estimated_input: signals.estimated_input,
        effort: request_effort(
            shared,
            &active.parent_router_sid,
            &active.state_sid,
            version,
            &selected,
        ),
        fold_system_turns: !turn_scores(shared, version, &selected).system_turns,
    }
}

/// The candidate's DECLARED wire id — its pinned version, else its
/// `api_model` — or `None` when it declares neither.
///
/// The distinction from [`configured_api_model`] is load-bearing: "no declared
/// api_model" means the router has no authoritative wire id for this candidate
/// and must not invent one from the ACP selector id, which is often an alias
/// the provider API rejects.
fn declared_api_model(
    shared: &Shared,
    version: Option<&str>,
    candidate: &CandidateId,
) -> Option<String> {
    let cfg = shared.runtime_config();
    if let Some(version) = cfg.resolve_version(candidate, version) {
        return Some(version.api_model.clone());
    }
    cfg.model_config(candidate)
        .and_then(|model| model.api_model.clone())
}

fn configured_api_model(shared: &Shared, candidate: &CandidateId) -> String {
    shared.runtime_config().wire_api_model(candidate)
}

/// The wire model `candidate` runs under this turn's `[router: version=…]`.
fn turn_api_model(shared: &Shared, version: Option<&str>, candidate: &CandidateId) -> String {
    let cfg = shared.runtime_config();
    match cfg.resolve_version(candidate, version) {
        Some(v) => v.api_model.clone(),
        None => cfg.wire_api_model_unpinned(candidate),
    }
}

/// Scores of the version `candidate` runs under this turn's request.
fn turn_scores(
    shared: &Shared,
    version: Option<&str>,
    candidate: &CandidateId,
) -> crate::candidate::ResolvedScores {
    match shared.runtime_config().resolve_version(candidate, version) {
        Some(v) => shared
            .scores
            .lookup_exact(&CandidateId::new(&candidate.agent, &v.api_model)),
        None => shared.scores.lookup_exact(candidate),
    }
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

fn inspect_request(body: &Value) -> RequestSignals {
    let serialized = serde_json::to_string(body).unwrap_or_default();
    RequestSignals {
        original_model: body
            .get("model")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        estimated_input: estimate_request_tokens(serialized.as_bytes())
            + max_output_tokens(body).unwrap_or(0),
        wire: request_wire_shape(body),
    }
}

fn estimate_request_tokens(body: &[u8]) -> u64 {
    (body.len() as u64).div_ceil(4)
}

fn max_output_tokens(body: &Value) -> Option<u64> {
    ["max_tokens", "max_output_tokens", "max_completion_tokens"]
        .iter()
        .find_map(|key| body.get(*key).and_then(Value::as_u64))
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

fn protocol_name(protocol: LlmWireProtocol) -> &'static str {
    match protocol {
        LlmWireProtocol::Anthropic => "anthropic",
        LlmWireProtocol::Openai => "openai",
    }
}

struct CompletionContext {
    request_id: String,
    response_status: Arc<AtomicU16>,
    state_sid: String,
    model: CandidateId,
    protocol: LlmWireProtocol,
    started: Instant,
}

struct CompletionGuard {
    shared: Arc<Shared>,
    context: Option<CompletionContext>,
    status: u16,
    captured: Vec<u8>,
    error: Option<String>,
}

impl CompletionGuard {
    fn new(shared: Arc<Shared>, context: Option<CompletionContext>, status: u16) -> Self {
        Self {
            shared,
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
    context: CompletionContext,
    status: u16,
    captured: &[u8],
    error: Option<String>,
) {
    context.response_status.store(status, Ordering::Release);
    // Some adapters retain an unanswered ACP prompt after consuming a quota
    // refusal. Feed the observed inference failure to the existing availability
    // machinery now; its turn watcher owns cancellation and normal failover.
    // Only a complete, attributed limit response is evidence. Successful prose,
    // authentication errors and incomplete transports must not cordon a seat.
    if matches!(status, 402 | 429) && error.is_none() {
        let refusal = agent_client_protocol::Error::internal_error()
            .data(String::from_utf8_lossy(captured).into_owned());
        let class = crate::limits::classify_failure(&refusal);
        if matches!(class, crate::limits::FailureClass::RateLimited { .. }) {
            crate::session::apply_failure(shared, &context.model, &refusal, &class);
        }
    }
    let usage = parse_response_usage(captured, context.protocol);
    let cost = request_cost(
        shared,
        &context.state_sid,
        &context.model,
        context.protocol,
        &usage,
    );
    let duration_ms = context.started.elapsed().as_millis() as u64;
    crate::planner_watchdog::note_progress(shared, &context.state_sid);
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

fn request_cost(
    shared: &Arc<Shared>,
    state_sid: &str,
    candidate: &CandidateId,
    protocol: LlmWireProtocol,
    usage: &LlmRequestUsage,
) -> f64 {
    let Some(pricing) = shared.pricing_for(state_sid, candidate) else {
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

    // Two independent router runtimes share only the canonical store. The
    // upstream rotates it while rejecting the old access token, like a sibling
    // router completing refresh during an in-flight request.
    #[tokio::test]
    async fn managed_claude_proxy_reloads_credentials_without_replaying_the_turn() {
        #[derive(Clone)]
        struct Fixture {
            path: std::path::PathBuf,
            calls: Arc<Mutex<Vec<(String, Value)>>>,
            reject_new: Arc<std::sync::atomic::AtomicBool>,
        }
        async fn upstream(
            State(fixture): State<Fixture>,
            headers: HeaderMap,
            axum::Json(body): axum::Json<Value>,
        ) -> Response<Body> {
            let authorization = headers["authorization"].to_str().unwrap().to_string();
            assert!(!headers.contains_key("x-api-key"));
            fixture
                .calls
                .lock()
                .unwrap()
                .push((authorization.clone(), body));
            if authorization == "Bearer synthetic-old" {
                let temp = fixture
                    .path
                    .with_extension(uuid::Uuid::new_v4().to_string());
                std::fs::write(
                    &temp,
                    json!({"claudeAiOauth": {
                        "accessToken": "synthetic-new", "expiresAt": 4070908800000u64
                    }})
                    .to_string(),
                )
                .unwrap();
                std::fs::rename(temp, &fixture.path).unwrap();
                return error_response(StatusCode::UNAUTHORIZED, "expired");
            }
            assert_eq!(authorization, "Bearer synthetic-new");
            if fixture.reject_new.load(Ordering::SeqCst) {
                return error_response(StatusCode::UNAUTHORIZED, "still rejected");
            }
            Response::new(Body::from("accepted"))
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".credentials.json");
        std::fs::write(
            &path,
            json!({"claudeAiOauth": {
                "accessToken":"synthetic-old", "expiresAt":4070908800000u64
            }})
            .to_string(),
        )
        .unwrap();
        let fixture = Fixture {
            path,
            calls: Arc::new(Mutex::new(vec![])),
            reject_new: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/v1/messages", axum::routing::post(upstream))
            .with_state(fixture.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let make_state = |name: &str| {
            let yaml = format!(
                r#"
state_file: {state}
llm_proxy: {{ enabled: true }}
agents:
  - name: claude@test
    command:
      type: stdio
      command: mock-agent
      env: [{{ name: CLAUDE_CONFIG_DIR, value: {directory} }}]
    model_selection: {{ type: config-option }}
    models: [{{ id: opus, cost_rank: 1 }}]
    llm_proxy:
      protocol: anthropic
      base_url_env: ANTHROPIC_BASE_URL
      upstream_base_url: http://{addr}/v1
"#,
                state = dir.path().join(name).display(),
                directory = dir.path().display()
            );
            let shared = Shared::new(crate::config::Config::from_yaml(&yaml).unwrap()).unwrap();
            ProxyServerState {
                runtime: shared.llm_proxy.clone(),
                shared,
            }
        };
        let states = [make_state("one.db"), make_state("two.db")];
        assert_ne!(
            states[0].runtime.token_by_key.lock().unwrap()[&ProcessKey("claude@test".into())],
            states[1].runtime.token_by_key.lock().unwrap()[&ProcessKey("claude@test".into())],
            "each router must grant an unguessable, independent account route"
        );
        let body = json!({"model":"opus", "messages":[{"role":"user","content":[
            {"type":"tool_result", "tool_use_id":"already-completed", "content":"done"}
        ]}]});
        let requests = (0..8).map(|i| {
            let state = states[i % 2].clone();
            let body = body.clone();
            async move {
                let token = state.runtime.token_by_key.lock().unwrap()
                    [&ProcessKey("claude@test".into())]
                    .clone();
                let request = Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("authorization", "Bearer synthetic-launch-snapshot")
                    .header("x-api-key", "synthetic-wrong-key")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap();
                let response = proxy_request(state, token, "v1/messages".into(), request).await;
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(
                    to_bytes(response.into_body(), 1024).await.unwrap(),
                    "accepted"
                );
            }
        });
        futures::future::join_all(requests).await;
        {
            let calls = fixture.calls.lock().unwrap();
            assert!(calls.len() > 8 && calls.len() <= 16);
            assert_eq!(
                calls
                    .iter()
                    .filter(|(auth, _)| auth == "Bearer synthetic-new")
                    .count(),
                8
            );
            assert!(calls.iter().all(|(_, sent)| sent == &body));
        }
        fixture.calls.lock().unwrap().clear();
        fixture.reject_new.store(true, Ordering::SeqCst);
        std::fs::write(
            &fixture.path,
            json!({"claudeAiOauth": {
                "accessToken":"synthetic-old", "expiresAt":4070908800000u64
            }})
            .to_string(),
        )
        .unwrap();
        let token = states[0].runtime.token_by_key.lock().unwrap()
            [&ProcessKey("claude@test".into())]
            .clone();
        let request = || {
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("authorization", "Bearer synthetic-launch-snapshot")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let response = proxy_request(
            states[0].clone(),
            token.clone(),
            "v1/messages".into(),
            request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(fixture.calls.lock().unwrap().len(), 2, "one retry only");
        let agent = &states[0].shared.agent_configs()[0];
        assert!(!matches!(
            crate::credentials::availability(agent),
            crate::auth::AuthAvailability::Unauthenticated { .. }
        ));
        std::fs::remove_file(&fixture.path).unwrap();
        let response =
            proxy_request(states[0].clone(), token, "v1/messages".into(), request()).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            fixture.calls.lock().unwrap().len(),
            2,
            "missing credentials never fall back to the launch token"
        );
        server.abort();
    }
    use crate::candidate::EffortLevel;
    use crate::session::RouterSession;
    use crate::state::PersistedSession;

    /// Wire tests name a Claude adapter, so the proxy reads canonical
    /// credentials. Keep that store synthetic and inside the test directory.
    fn claude_credential_dir(dir: &std::path::Path) -> std::path::PathBuf {
        let store = dir.join("claude-home");
        std::fs::create_dir_all(&store).unwrap();
        std::fs::write(
            store.join(".credentials.json"),
            r#"{"claudeAiOauth":{"accessToken":"synthetic-access","refreshToken":"synthetic-refresh","expiresAt":4070908800000}}"#,
        )
        .unwrap();
        store
    }

    /// Put `candidate` out of plan budget, the one case the proxy reroutes.
    fn cordon(shared: &Shared, candidate: &CandidateId) {
        shared
            .headroom
            .lock()
            .unwrap()
            .set_usage_cordons(HashMap::from([(
                candidate.clone(),
                crate::headroom::UsageCordon {
                    reason: "weekly limit".into(),
                    resets_at: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
                    resets_at_rfc3339: String::new(),
                    soft: false,
                },
            )]));
    }

    fn policy_shared() -> (tempfile::TempDir, Arc<Shared>) {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
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
    fn policy_shared_priced() -> (tempfile::TempDir, Arc<Shared>) {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
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
        kory_code_shared_with("")
    }

    /// `extra` is appended as top-level config (e.g. a `pinned_versions`
    /// block), so pin behaviour can be exercised on the production shape.
    fn kory_code_shared_with(extra: &str) -> (tempfile::TempDir, Arc<Shared>) {
        let dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
agents:
  - name: claude
    command: {{ type: stdio, command: claude-agent-acp, env: [{{ name: CLAUDE_CONFIG_DIR, value: {} }}] }}
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
      # Older versions `pinned_versions` may select. Their whole point is that
      # the wire model differs from what the selector alias resolves to.
      - id: "opus[1m]"
        cost_rank: 4
        api_model: claude-opus-5
        pricing: {{ input_per_mtok: 5.0, output_per_mtok: 25.0, cache_read_per_mtok: 0.50, cache_write_per_mtok: 6.25 }}
        versions:
          - api_model: claude-opus-4-6
            pricing: {{ input_per_mtok: 6.0, output_per_mtok: 30.0, cache_read_per_mtok: 0.60, cache_write_per_mtok: 7.50 }}
      - id: "claude-fable-5[1m]"
        cost_rank: 5
        api_model: claude-fable-5
        pricing: {{ input_per_mtok: 10.0, output_per_mtok: 50.0, cache_read_per_mtok: 1.00, cache_write_per_mtok: 12.50 }}
        versions:
          - api_model: claude-fable-5-1
            pricing: {{ input_per_mtok: 10.0, output_per_mtok: 50.0, cache_read_per_mtok: 0.25, cache_write_per_mtok: 12.50 }}
{extra}"#,
            dir.path().join("state.db").display(),
            claude_credential_dir(dir.path()).display()
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

    fn active(candidate: &str) -> ActiveTurn {
        ActiveTurn {
            registration_id: 1,
            response_status: Arc::new(AtomicU16::new(0)),
            parent_router_sid: "r1".to_string(),
            state_sid: "r1".to_string(),
            downstream_sid: "d1".to_string(),
            candidate: CandidateId::new("mock", candidate),
            class: TaskClass::CodingGeneral,
        }
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
    fn last_attribution_reports_the_model_that_served_the_request() {
        // The structured per-tool-call attribution the client renders in the
        // call's own card. Absent before the first request (rendered as pending,
        // never as a wrong model), and it follows the selection — not the pin —
        // so a failed-over call is attributed to the model that actually served it.
        let (_dir, shared) = kory_code_shared();
        let turn = claude_active("claude-fable-5[1m]");
        assert!(shared.llm_proxy.last_attribution("r1").is_none());

        cordon(&shared, &turn.candidate);
        let body = kory_code_request(120, "$ ls\nsrc/main.rs");
        let decision = select_request_model(&shared, &turn, &body);
        assert_eq!(decision.event, "exhausted-repin");
        let (candidate, reason) = shared.llm_proxy.last_attribution("r1").unwrap();
        assert_eq!(candidate, format!("claude/{}", decision.selected_model));
        assert_ne!(candidate, "claude/claude-fable-5[1m]");
        assert!(reason.contains("out of plan budget"), "{reason}");
    }

    #[test]
    fn omitted_thinking_display_marks_thinking_text_as_narration() {
        // Claude Code asks for `display: "omitted"`, so the only thinking text
        // its responses carry is the server's narration of the model's prose.
        let (_dir, shared) = kory_code_shared();
        let turn = claude_active("claude-fable-5[1m]");
        assert!(!shared.llm_proxy.thinking_text_is_narration("r1"));

        select_request_model(&shared, &turn, &kory_code_request(1, "ok"));
        assert!(shared.llm_proxy.thinking_text_is_narration("r1"));

        // A WebFetch-style side call with thinking disabled says nothing about
        // the main loop's thinking, so it leaves the flag alone.
        let mut side = kory_code_request(1, "ok");
        side["thinking"] = json!({"type": "disabled"});
        select_request_model(&shared, &turn, &side);
        assert!(shared.llm_proxy.thinking_text_is_narration("r1"));

        // A request that returns reasoning text makes thinking text reasoning.
        let mut summarized = kory_code_request(1, "ok");
        summarized["thinking"] = json!({"type": "adaptive", "display": "summarized"});
        select_request_model(&shared, &turn, &summarized);
        assert!(!shared.llm_proxy.thinking_text_is_narration("r1"));
    }

    #[test]
    fn confirmed_effort_uses_the_shaped_or_unchanged_provider_wire() {
        let (_dir, shared) = kory_code_shared();
        let candidate = CandidateId::new("claude", "sonnet");
        let capped = shape_provider_request(
            &json!({"thinking":{"type":"disabled"}}),
            None,
            &EffortShape::Set("max".into()),
            LlmWireProtocol::Anthropic,
            false,
        )
        .unwrap();
        let confirmed =
            confirmed_wire_effort(&shared, &candidate, &capped, LlmWireProtocol::Anthropic);
        assert_eq!(confirmed["resolved"], "high");
        assert_eq!(confirmed["provider_value"], "high");
        assert_eq!(confirmed["confirmed"], true);
        let omitted = shape_provider_request(
            &json!({"output_config":{"effort":"max"}}),
            None,
            &EffortShape::Omit,
            LlmWireProtocol::Anthropic,
            false,
        )
        .unwrap();
        assert!(confirmed_wire_effort(&shared, &candidate, &omitted, LlmWireProtocol::Anthropic)["resolved"].is_null());
        assert_eq!(
            confirmed_wire_effort(&shared, &candidate, &omitted, LlmWireProtocol::Anthropic)["confirmed"],
            false
        );
        for missing in [
            br#"{}"#.as_slice(),
            br#"{"reasoning_effort":"auto"}"#,
            br#"{"reasoning_effort":"unrecognized"}"#,
        ] {
            assert_eq!(
                confirmed_wire_effort(&shared, &candidate, missing, LlmWireProtocol::Openai)["confirmed"],
                false
            );
        }
        let unchanged = serde_json::to_vec(&json!({"reasoning":{"effort":"medium"}})).unwrap();
        assert_eq!(
            confirmed_wire_effort(&shared, &candidate, &unchanged, LlmWireProtocol::Openai)["resolved"],
            "medium"
        );
        assert!(
            confirmed_wire_effort(
                &shared,
                &candidate,
                br#"{"reasoning":{"effort":"auto"}}"#,
                LlmWireProtocol::Openai
            )["resolved"]
                .is_null()
        );
    }

    #[test]
    fn provider_request_shaping_sets_omits_or_preserves_effort_as_resolved() {
        let anthropic = shape_provider_request(
            &json!({"model": "old", "output_config": {"effort": "stale"}}),
            Some("new"),
            &EffortShape::Set("intensive".to_string()),
            LlmWireProtocol::Anthropic,
            false,
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
            false,
        )
        .unwrap();
        let openai: Value = serde_json::from_slice(&openai).unwrap();
        assert_eq!(openai["reasoning"]["effort"], "high");
        let anthropic_omitted = shape_provider_request(
            &json!({"model": "claude", "output_config": {"effort": "high"}}),
            None,
            &EffortShape::Omit,
            LlmWireProtocol::Anthropic,
            false,
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
                false,
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
                false,
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
            false,
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
        cordon(&shared, &CandidateId::new("mock", "parent"));
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
            false,
        )
        .unwrap();
        let shaped: Value = serde_json::from_slice(&shaped).unwrap();
        assert_eq!(shaped["model"], "subtool");
        assert_eq!(shaped["output_config"]["effort"], "subtool-high");
    }

    #[test]
    fn a_delegate_effort_hint_replaces_the_parent_effort() {
        let dir = tempfile::tempdir().unwrap();
        let scores = dir.path().join("scores.yaml");
        std::fs::write(
            &scores,
            "candidates:\n\
             \x20 - { pattern: 'mock/worker', default_quality: 3.0, context_window: 400000, effort_levels: [low, max], effort_mapping: { low: worker-low, max: worker-max } }\n",
        )
        .unwrap();
        let yaml = format!(
            "state_file: {}\nscore_table: {}\nagents:\n  - name: mock\n    command: {{ type: stdio, command: mock-agent }}\n    model_selection: {{ type: config-option }}\n    models:\n      - {{ id: worker, cost_rank: 1 }}\n",
            dir.path().join("state.db").display(),
            scores.display(),
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg.clone()).unwrap();
        let mut parent = RouterSession::rehydrated(&cfg, &PersistedSession::default(), Vec::new());
        parent.resolved_effort = Some(
            shared
                .scores
                .lookup(&CandidateId::new("mock", "worker"))
                .resolve_effort(EffortLevel::Max),
        );
        shared
            .sessions
            .lock()
            .unwrap()
            .insert("r1".to_string(), parent);
        let worker = CandidateId::new("mock", "worker");
        // Without a hint the delegate inherits the parent's max.
        assert_eq!(
            request_effort(&shared, "r1", "r1::delegate-d1", None, &worker),
            EffortShape::Set("worker-max".to_string())
        );
        shared
            .delegate_effort
            .lock()
            .unwrap()
            .insert("r1::delegate-d1".to_string(), EffortLevel::Low);
        assert_eq!(
            request_effort(&shared, "r1", "r1::delegate-d1", None, &worker),
            EffortShape::Set("worker-low".to_string())
        );
        // The parent's own requests are unaffected.
        assert_eq!(
            request_effort(&shared, "r1", "r1", None, &worker),
            EffortShape::Set("worker-max".to_string())
        );
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
            system_turns: true,
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
    fn proxy_quota_evidence_preserves_failure_scope_and_exclusions() {
        for (status, body, incomplete, scope) in [
            (402, "Grok Build usage balance exhausted", false, Some(true)),
            (429, "usage limit; retry-after: 10", false, Some(true)),
            (402, "monthly spend limit reached", false, Some(false)),
            (200, "quoted usage limit", false, None),
            (401, "usage limit", false, None),
            (500, "usage limit", false, None),
            (402, "unknown failure", false, None),
            (429, "Selected model is at capacity", false, None),
            (402, "usage limit", true, None),
        ] {
            let (_dir, shared) = policy_shared();
            let candidate = CandidateId::new("mock", "sonnet");
            complete_request(
                &shared,
                CompletionContext {
                    request_id: "quota-evidence".into(),
                    response_status: Arc::new(AtomicU16::new(0)),
                    state_sid: "r1".into(),
                    model: candidate.clone(),
                    protocol: LlmWireProtocol::Openai,
                    started: Instant::now(),
                },
                status,
                body.as_bytes(),
                incomplete.then(|| "incomplete response".into()),
            );
            let mut headroom = shared.headroom.lock().unwrap();
            assert_eq!(
                headroom.cordon_active("mock").is_some(),
                scope == Some(true),
                "{status} {body}"
            );
            assert_eq!(
                headroom.usage_cordon(&candidate).is_some(),
                scope == Some(false),
                "{status} {body}"
            );
        }
    }

    #[test]
    fn begin_turn_resets_stale_model_attribution_on_repin() {
        let (_dir, shared) = policy_shared();
        let first = shared.llm_proxy.begin_turn(
            ProcessKey("mock".into()),
            "r1".into(),
            "r1".into(),
            "d1".into(),
            CandidateId::new("claude", "sonnet"),
            TaskClass::CodingGeneral,
        );
        assert_eq!(
            shared.llm_proxy.last_attribution("r1").unwrap().0,
            "claude/sonnet"
        );
        let old_response = first.response_status.as_ref().unwrap().clone();
        old_response.store(503, Ordering::Release);
        assert_eq!(first.last_response_status(), Some(503));
        drop(first);

        let second = shared.llm_proxy.begin_turn(
            ProcessKey("mock".into()),
            "r1".into(),
            "r1".into(),
            "d2".into(),
            CandidateId::new("codex", "gpt-5.6-sol"),
            TaskClass::CodingGeneral,
        );
        assert_eq!(
            shared.llm_proxy.last_attribution("r1").unwrap().0,
            "codex/gpt-5.6-sol"
        );
        assert_eq!(
            shared.llm_proxy.current_model("r1").as_deref(),
            Some("gpt-5.6-sol")
        );
        assert_eq!(second.last_response_status(), None);
        // A late completion from the old turn cannot poison the new turn.
        old_response.store(502, Ordering::Release);
        assert_eq!(second.last_response_status(), None);
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
    fn routine_and_failing_requests_stay_on_the_pin() {
        let (_dir, shared) = policy_shared();
        let active = active("opus");
        for content in [
            json!({"type":"tool_result","content":"git status clean","status":"completed"}),
            json!({"type":"tool_result","content":"tests failed","is_error":true}),
        ] {
            let body = json!({"model":"opus","messages":[{"role":"user","content":[content]}]});
            let decision = select_request_model(&shared, &active, &body);
            assert_eq!(decision.selected_model, "opus");
            assert_eq!(decision.event, "steady");
            assert!(!decision.rewrite);
        }
    }

    #[test]
    fn out_of_budget_pin_fails_over_to_the_strongest_sibling() {
        let (_dir, shared) = kory_code_shared();
        let active = claude_active("sonnet");
        cordon(&shared, &active.candidate);
        let body =
            json!({"model":"claude-sonnet-5","messages":[{"role":"user","content":"continue"}]});
        let decision = select_request_model(&shared, &active, &body);
        assert_eq!(decision.event, "exhausted-repin");
        assert_ne!(decision.selected_model, "sonnet");
        assert!(decision.rewrite);
    }

    /// The request `claude-agent-acp` emits for a session configured via an
    /// ALIAS: the body names whatever that alias resolves to on the adapter's
    /// side, which for a pinned version is not the model the router pinned.
    fn aliased_request(adapter_model: &str) -> Value {
        json!({
            "model": adapter_model,
            "messages": [{"role":"user","content":"continue"}]
        })
    }

    const OPUS_PINNED: &str = "pinned_versions:\n  \"claude/opus[1m]\": claude-opus-4-6\n";

    #[test]
    fn pinned_version_rewrites_the_wire_model_off_the_selector_alias() {
        let (_dir, shared) = kory_code_shared_with(OPUS_PINNED);
        // The adapter resolves `opus[1m]` to `claude-opus-5`. Passing that
        // through would serve Opus 5 while the session is priced as 4.6.
        let decision = select_request_model(
            &shared,
            &claude_active("opus[1m]"),
            &aliased_request("claude-opus-5"),
        );
        assert_eq!(
            decision.model, "claude-opus-4-6",
            "the pinned version must reach the wire, not the alias's resolution"
        );
        assert!(
            decision.rewrite,
            "the body has to be rewritten to get there"
        );
        assert_eq!(
            decision.selected_model, "opus[1m]",
            "the candidate never changes"
        );
    }

    #[test]
    fn pinned_fable_5_1_rewrites_the_wire_model_off_its_selector_alias() {
        let (_dir, shared) = kory_code_shared_with(
            "pinned_versions:\n  \"claude/claude-fable-5[1m]\": claude-fable-5-1\n",
        );
        let decision = select_request_model(
            &shared,
            &claude_active("claude-fable-5[1m]"),
            &aliased_request("claude-fable-5"),
        );
        assert_eq!(decision.model, "claude-fable-5-1");
        assert!(decision.rewrite);
    }

    #[test]
    fn a_session_version_takes_effect_on_the_next_request() {
        let (_dir, shared) = kory_code_shared();
        let cfg = shared.cfg.clone();
        let session = RouterSession::rehydrated(&cfg, &PersistedSession::default(), Vec::new());
        shared
            .sessions
            .lock()
            .unwrap()
            .insert("r1".to_string(), session);
        let opus = claude_active("opus[1m]");
        let before = select_request_model(&shared, &opus, &aliased_request("claude-opus-5"));
        assert_eq!(before.model, "claude-opus-5");
        shared.with_session("r1", |s| s.version_request = Some("claude-opus-4-6".into()));
        let after = select_request_model(&shared, &opus, &aliased_request("claude-opus-5"));
        assert_eq!(after.model, "claude-opus-4-6");
        assert!(after.rewrite && after.honors_pin);
        assert_eq!(
            shared
                .pricing_for("r1", &opus.candidate)
                .unwrap()
                .input_per_mtok,
            6.0
        );
    }

    #[test]
    fn a_pinned_version_is_priced_as_itself() {
        let (_dir, shared) = kory_code_shared_with(OPUS_PINNED);
        let opus = CandidateId::new("claude", "opus[1m]");
        assert_eq!(shared.cfg.model_pricing(&opus).unwrap().input_per_mtok, 6.0);
        assert_eq!(configured_api_model(&shared, &opus), "claude-opus-4-6");
        let (_dir, unpinned) = kory_code_shared();
        assert_eq!(
            unpinned.cfg.model_pricing(&opus).unwrap().input_per_mtok,
            5.0
        );
    }

    #[test]
    fn unaliased_pin_still_passes_its_own_api_model_through_unrewritten() {
        let (_dir, shared) = kory_code_shared();
        // The ordinary case: `opus[1m]` declares api_model `claude-opus-5` and
        // the adapter already emits exactly that. Same value either way, so no
        // rewrite — the fix must not start touching bodies it used to leave alone.
        let decision = select_request_model(
            &shared,
            &claude_active("opus[1m]"),
            &aliased_request("claude-opus-5"),
        );
        assert_eq!(decision.model, "claude-opus-5");
        assert!(!decision.rewrite);
    }

    #[test]
    fn candidate_without_api_model_keeps_the_adapters_own_model_string() {
        // No `api_model` declared means the router has no authoritative wire
        // id: the incoming string is the only valid one, and inventing
        // `candidate.model` from an ACP selector alias would 404.
        let (_d, shared) = policy_shared_priced();
        let decision = select_request_model(
            &shared,
            &active("opus"),
            &aliased_request("some-provider-side-opus-variant"),
        );
        assert_eq!(decision.model, "some-provider-side-opus-variant");
        assert!(!decision.rewrite);
    }

    /// The discriminator the fallback path keys on. A router-chosen alternate
    /// keeps the legitimate retry-the-original recovery (see
    /// `proxy_streams_rewrites_preserves_auth_and_accounts_requests`, which
    /// exercises it end to end); only a pin rewrite loses it.
    #[test]
    fn only_a_pin_rewrite_is_flagged_as_honoring_the_pin() {
        let (_dir, shared) = kory_code_shared_with(OPUS_PINNED);
        // Pin rewrite: staying on the session's own candidate, but its
        // pinned version differs from the alias string the adapter emitted.
        let pin = select_request_model(
            &shared,
            &claude_active("opus[1m]"),
            &aliased_request("claude-opus-5"),
        );
        assert!(pin.rewrite && pin.honors_pin);

        // Router-chosen alternate: a failover off an out-of-budget pin.
        // Rewritten too, but the original body is still a valid request for the
        // pinned model, so the alternate-fallback path stays available.
        let fable = claude_active("claude-fable-5[1m]");
        cordon(&shared, &fable.candidate);
        let alternate = select_request_model(&shared, &fable, &aliased_request("claude-fable-5"));
        assert_eq!(
            alternate.event, "exhausted-repin",
            "reason: {}",
            alternate.reason
        );
        assert!(
            alternate.rewrite && !alternate.honors_pin,
            "an alternate rewrite must keep its fallback: {alternate:?}"
        );
    }

    #[test]
    fn policy_resets_when_the_acp_session_is_repinned() {
        let (_dir, shared) = policy_shared();
        cordon(&shared, &CandidateId::new("mock", "opus"));
        let neutral_opus =
            json!({"model":"opus","messages":[{"role":"user","content":"continue"}]});
        assert_ne!(
            select_request_model(&shared, &active("opus"), &neutral_opus).selected_model,
            "opus"
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
        );
        cordon(&shared, &CandidateId::new("mock", "opus"));

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
        assert_eq!(event, "exhausted-repin");
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

    /// A provider rejection of a PIN rewrite must never be "recovered" by
    /// replaying the untouched original body: that body names the downstream
    /// ALIAS's own model, so the retry would serve a different version under
    /// the pin's identity — and, because the pinned candidate bypasses the
    /// `rejected_api_models` filter, it would do so on every request.
    #[test]
    fn system_turns_fold_into_user_turns_and_effort_only_turns_drop() {
        let mut body = json!({
            "model": "claude-opus-5",
            "messages": [
                {"role":"user","content":"continue"},
                {"role":"system","content":[],"output_config":{"effort":"high"}},
                {"role":"system","content":[{"type":"text","text":"Tools changed.","cache_control":{"type":"ephemeral"}}]},
                {"role":"assistant","content":"ok"},
                {"role":"system","content":"final note"}
            ]
        });
        assert!(fold_system_turns_into_user(body.as_object_mut().unwrap()));
        assert_eq!(
            body["messages"],
            json!([
                {"role":"user","content":[
                    {"type":"text","text":"continue"},
                    {"type":"text","text":"Tools changed.","cache_control":{"type":"ephemeral"}}
                ]},
                {"role":"assistant","content":"ok"},
                {"role":"user","content":[{"type":"text","text":"final note"}]}
            ])
        );
        assert!(
            body.get("output_config").is_none(),
            "per-turn effort is dropped"
        );

        // Nothing to fold → untouched, reports false.
        let mut plain = json!({"messages": [{"role":"user","content":"hi"}]});
        assert!(!fold_system_turns_into_user(plain.as_object_mut().unwrap()));

        // A structured block has no user-turn equivalent → left for the
        // provider to reject and the adapter to heal.
        let mut structured = json!({"messages": [
            {"role":"user","content":"hi"},
            {"role":"system","content":[{"type":"tool_addition","name":"Read"}]}
        ]});
        let before = structured.clone();
        assert!(!fold_system_turns_into_user(
            structured.as_object_mut().unwrap()
        ));
        assert_eq!(structured, before);
    }

    #[test]
    fn system_turn_betas_are_stripped_from_the_beta_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "anthropic-beta",
            "mid-conversation-system-2026-04-07, per-turn-control-2026-07-01,context-1m-2025-08-07"
                .parse()
                .unwrap(),
        );
        headers.insert("x-other", "keep".parse().unwrap());
        let stripped = strip_system_turn_betas(&headers);
        assert_eq!(stripped["anthropic-beta"], "context-1m-2025-08-07");
        assert_eq!(stripped["x-other"], "keep");

        let mut only = HeaderMap::new();
        only.insert(
            "anthropic-beta",
            "mid-conversation-system-2026-04-07".parse().unwrap(),
        );
        assert!(
            strip_system_turn_betas(&only)
                .get("anthropic-beta")
                .is_none()
        );
    }

    #[test]
    fn request_shape_rejections_are_told_apart_from_model_rejections() {
        // Verbatim provider messages from live sessions.
        assert!(is_request_shape_rejection(
            "role 'system' is not supported on this model"
        ));
        assert!(!is_request_shape_rejection(
            "Claude Code 2.1.246 does not support this model; version 2.1.251 or newer \
             is required. Run `claude update`."
        ));
        assert!(!is_request_shape_rejection(
            "model: claude-opus-4-6 not found"
        ));
    }

    /// Live bug: every new session pinned to Opus 4.6 opened with a 400 —
    /// Claude Code's mid-conversation `role: "system"` beta, which that
    /// version lacks. Claude Code shapes the request for the alias it was
    /// configured with (Opus 5); the proxy, which knows the wire model, must
    /// fold the turn away and drop the beta token so the pinned version is
    /// never asked something it cannot do.
    #[tokio::test]
    async fn legacy_version_pin_folds_system_turns_before_sending() {
        #[derive(Clone)]
        struct Seen(Arc<Mutex<Vec<(String, bool, String)>>>);

        async fn upstream(
            State(seen): State<Seen>,
            headers: HeaderMap,
            axum::Json(body): axum::Json<Value>,
        ) -> Response<Body> {
            let model = body["model"].as_str().unwrap_or("").to_string();
            let has_system_turn = body["messages"]
                .as_array()
                .is_some_and(|m| m.iter().any(|msg| msg["role"] == "system"));
            let betas = headers
                .get("anthropic-beta")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            seen.0
                .lock()
                .unwrap()
                .push((model.clone(), has_system_turn, betas));
            if model == "claude-opus-4-6" && has_system_turn {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"type":"error","error":{"type":"invalid_request_error","message":"role 'system' is not supported on this model"}}"#,
                    ))
                    .unwrap();
            }
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(
                    "event: message_start\n\
                     data: {\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n",
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
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
agents:
  - name: claude
    command: {{ type: stdio, command: claude-agent-acp, env: [{{ name: CLAUDE_CONFIG_DIR, value: {} }}] }}
    model_selection: {{ type: config-option }}
    llm_proxy:
      protocol: anthropic
      base_url_env: ANTHROPIC_BASE_URL
      upstream_base_url: http://{upstream_addr}/v1
    models:
      - {{ id: "opus[1m]", cost_rank: 4, api_model: claude-opus-5,
           pricing: {{ input_per_mtok: 5, output_per_mtok: 25 }},
           versions: [ {{ api_model: claude-opus-4-6,
                          pricing: {{ input_per_mtok: 5, output_per_mtok: 25 }} }} ] }}
pinned_versions:
  "claude/opus[1m]": claude-opus-4-6
"#,
            dir.path().join("state.db").display(),
            claude_credential_dir(dir.path()).display()
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg.clone()).unwrap();
        shared.set_models_routeable(&ProcessKey("claude".to_string()), vec!["opus[1m]".into()]);
        let pinned = CandidateId::new("claude", "opus[1m]");
        shared.state.lock().unwrap().upsert(
            "r1".to_string(),
            PersistedSession {
                agent: "claude".to_string(),
                model: pinned.model.clone(),
                downstream_session_id: "d1".to_string(),
                cwd: dir.path().to_path_buf(),
                kind: "primary".to_string(),
                ..Default::default()
            },
        );
        shared.sessions.lock().unwrap().insert(
            "r1".to_string(),
            RouterSession::rehydrated(&cfg, &PersistedSession::default(), Vec::new()),
        );
        let proxy_task = shared.llm_proxy.bind(shared.clone()).await.unwrap();
        let spec = shared
            .target_spec(&ProcessKey("claude".to_string()))
            .unwrap();
        let proxy_base = shared
            .llm_proxy
            .process_env(&spec)
            .into_iter()
            .find(|(name, _)| name == "ANTHROPIC_BASE_URL")
            .unwrap()
            .1;

        // Claude Code's first request, shaped for Opus 5: a per-turn effort
        // statement (empty system turn) and a text system turn, plus the betas
        // that announce them.
        let shaped_for_opus_5 = json!({
            "model": "claude-opus-5",
            "messages": [
                {"role":"user","content":"continue"},
                {"role":"system","content":[],"output_config":{"effort":"high"}},
                {"role":"system","content":[{"type":"text","text":"Tools changed: Read added."}]}
            ]
        });
        let _turn = shared.llm_proxy.begin_turn(
            ProcessKey("claude".to_string()),
            "r1".to_string(),
            "r1".to_string(),
            "d1".to_string(),
            pinned.clone(),
            TaskClass::CodingGeneral,
        );
        let response = reqwest::Client::new()
            .post(format!("{proxy_base}/messages"))
            .header("authorization", "Bearer secret")
            .header(
                "anthropic-beta",
                "mid-conversation-system-2026-04-07,per-turn-control-2026-07-01,context-1m-2025-08-07",
            )
            .json(&shaped_for_opus_5)
            .send()
            .await
            .unwrap();

        // The pinned version was asked only what it can do: no system turn,
        // no system-turn betas, first try.
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            seen.0.lock().unwrap().clone(),
            vec![(
                "claude-opus-4-6".to_string(),
                false,
                "context-1m-2025-08-07".to_string()
            )],
            "system turn folded and betas stripped before the wire"
        );
        assert!(
            shared
                .headroom
                .lock()
                .unwrap()
                .usage_cordon(&pinned)
                .is_none(),
            "nothing to cordon: the request never failed"
        );

        drop(proxy_task);
        upstream_task.abort();
    }

    /// A system turn the proxy cannot fold (a structured `tool_addition`
    /// block has no user-turn equivalent) still reaches the provider and is
    /// rejected. The adapter heals that itself — it retries without the turn
    /// and sticky-disables the beta — so the proxy must relay the 400 and
    /// keep the pin, not cordon the version for 15 minutes and serve the
    /// healed retry on a different model (the live failure before the fold).
    #[tokio::test]
    async fn unfoldable_request_feature_rejection_keeps_the_pin_for_the_adapters_retry() {
        #[derive(Clone)]
        struct Seen(Arc<Mutex<Vec<(String, bool)>>>);

        async fn upstream(
            State(seen): State<Seen>,
            axum::Json(body): axum::Json<Value>,
        ) -> Response<Body> {
            let model = body["model"].as_str().unwrap_or("").to_string();
            let has_system_turn = body["messages"]
                .as_array()
                .is_some_and(|m| m.iter().any(|msg| msg["role"] == "system"));
            seen.0
                .lock()
                .unwrap()
                .push((model.clone(), has_system_turn));
            if model == "claude-opus-4-6" && has_system_turn {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"type":"error","error":{"type":"invalid_request_error","message":"role 'system' is not supported on this model"}}"#,
                    ))
                    .unwrap();
            }
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(
                    "event: message_start\n\
                     data: {\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n",
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
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
agents:
  - name: claude
    command: {{ type: stdio, command: claude-agent-acp, env: [{{ name: CLAUDE_CONFIG_DIR, value: {} }}] }}
    model_selection: {{ type: config-option }}
    llm_proxy:
      protocol: anthropic
      base_url_env: ANTHROPIC_BASE_URL
      upstream_base_url: http://{upstream_addr}/v1
    models:
      - {{ id: "opus[1m]", cost_rank: 4, api_model: claude-opus-5,
           pricing: {{ input_per_mtok: 5, output_per_mtok: 25 }},
           versions: [ {{ api_model: claude-opus-4-6,
                          pricing: {{ input_per_mtok: 5, output_per_mtok: 25 }} }} ] }}
pinned_versions:
  "claude/opus[1m]": claude-opus-4-6
"#,
            dir.path().join("state.db").display(),
            claude_credential_dir(dir.path()).display()
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg.clone()).unwrap();
        shared.set_models_routeable(&ProcessKey("claude".to_string()), vec!["opus[1m]".into()]);
        let pinned = CandidateId::new("claude", "opus[1m]");
        shared.state.lock().unwrap().upsert(
            "r1".to_string(),
            PersistedSession {
                agent: "claude".to_string(),
                model: pinned.model.clone(),
                downstream_session_id: "d1".to_string(),
                cwd: dir.path().to_path_buf(),
                kind: "primary".to_string(),
                ..Default::default()
            },
        );
        shared.sessions.lock().unwrap().insert(
            "r1".to_string(),
            RouterSession::rehydrated(&cfg, &PersistedSession::default(), Vec::new()),
        );
        let proxy_task = shared.llm_proxy.bind(shared.clone()).await.unwrap();
        let spec = shared
            .target_spec(&ProcessKey("claude".to_string()))
            .unwrap();
        let proxy_base = shared
            .llm_proxy
            .process_env(&spec)
            .into_iter()
            .find(|(name, _)| name == "ANTHROPIC_BASE_URL")
            .unwrap()
            .1;
        let post = |base: String, body: Value| async move {
            reqwest::Client::new()
                .post(format!("{base}/messages"))
                .header("authorization", "Bearer secret")
                .json(&body)
                .send()
                .await
                .unwrap()
        };

        // A system turn carrying a late tool addition: not foldable.
        let with_system_turn = json!({
            "model": "claude-opus-5",
            "messages": [
                {"role":"user","content":"continue"},
                {"role":"system","content":[{"type":"tool_addition","name":"Read","input_schema":{}}]}
            ]
        });
        let turn = shared.llm_proxy.begin_turn(
            ProcessKey("claude".to_string()),
            "r1".to_string(),
            "r1".to_string(),
            "d1".to_string(),
            pinned.clone(),
            TaskClass::CodingGeneral,
        );
        let response = post(proxy_base.clone(), with_system_turn).await;

        // (a) The provider's rejection reaches the adapter unchanged — it is
        //     what triggers the adapter's own beta fallback.
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let text = response.text().await.unwrap();
        assert!(
            text.contains("role 'system'"),
            "provider error relayed: {text}"
        );

        // (b) The pin is NOT cordoned: the model is fine, the request wasn't.
        assert!(
            shared
                .headroom
                .lock()
                .unwrap()
                .usage_cordon(&pinned)
                .is_none(),
            "a request-feature rejection must not cordon the pinned version"
        );

        // (c) The session is told, without being told the model is gone.
        let notice = shared
            .with_session("r1", |session| session.pending_disclosure.join("\n"))
            .unwrap_or_default();
        assert!(
            notice.contains("pin is kept") && !notice.contains("cordoned"),
            "user notified that the pin survives: {notice}"
        );

        // (d) The adapter's healed retry — same alias, no system turn — is
        //     served by the pinned version itself, not by an alternate.
        drop(turn);
        let _turn2 = shared.llm_proxy.begin_turn(
            ProcessKey("claude".to_string()),
            "r1".to_string(),
            "r1".to_string(),
            "d1".to_string(),
            pinned.clone(),
            TaskClass::CodingGeneral,
        );
        let healed = json!({
            "model": "claude-opus-5",
            "messages": [{"role":"user","content":"continue"}]
        });
        let second = post(proxy_base, healed).await;
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(
            seen.0.lock().unwrap().clone(),
            vec![
                ("claude-opus-4-6".to_string(), true),
                ("claude-opus-4-6".to_string(), false)
            ],
            "both the probe and the healed retry went to the pinned version"
        );

        drop(proxy_task);
        upstream_task.abort();
    }

    #[tokio::test]
    async fn rejected_pinned_rewrite_cordons_instead_of_serving_the_alias() {
        #[derive(Clone)]
        struct Seen(Arc<Mutex<Vec<String>>>);

        async fn upstream(
            State(seen): State<Seen>,
            axum::Json(body): axum::Json<Value>,
        ) -> Response<Body> {
            let model = body["model"].as_str().unwrap_or("").to_string();
            seen.0.lock().unwrap().push(model.clone());
            // The real below-the-floor rejection, verbatim in shape.
            if model == "claude-fable-5-1" {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"type":"error","error":{"type":"invalid_request_error","message":"Claude Code 2.1.246 does not support this model; version 2.1.251 or newer is required. Run `claude update`."}}"#,
                    ))
                    .unwrap();
            }
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(
                    "event: message_start\n\
                     data: {\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n",
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
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
agents:
  - name: claude
    command: {{ type: stdio, command: claude-agent-acp, env: [{{ name: CLAUDE_CONFIG_DIR, value: {} }}] }}
    model_selection: {{ type: config-option }}
    llm_proxy:
      protocol: anthropic
      base_url_env: ANTHROPIC_BASE_URL
      upstream_base_url: http://{upstream_addr}/v1
    models:
      - {{ id: sonnet, cost_rank: 2, api_model: claude-sonnet-5,
           pricing: {{ input_per_mtok: 2, output_per_mtok: 10 }} }}
      - {{ id: "claude-fable-5[1m]", cost_rank: 5, api_model: claude-fable-5,
           pricing: {{ input_per_mtok: 10, output_per_mtok: 50 }},
           versions: [ {{ api_model: claude-fable-5-1,
                          pricing: {{ input_per_mtok: 10, output_per_mtok: 50 }} }} ] }}
pinned_versions:
  "claude/claude-fable-5[1m]": claude-fable-5-1
"#,
            dir.path().join("state.db").display(),
            claude_credential_dir(dir.path()).display()
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg.clone()).unwrap();
        shared.set_models_routeable(
            &ProcessKey("claude".to_string()),
            vec!["sonnet".into(), "claude-fable-5[1m]".into()],
        );
        let pinned = CandidateId::new("claude", "claude-fable-5[1m]");
        shared.state.lock().unwrap().upsert(
            "r1".to_string(),
            PersistedSession {
                agent: "claude".to_string(),
                model: pinned.model.clone(),
                downstream_session_id: "d1".to_string(),
                cwd: dir.path().to_path_buf(),
                kind: "primary".to_string(),
                ..Default::default()
            },
        );
        // A live session so the user-facing notice has somewhere to land.
        shared.sessions.lock().unwrap().insert(
            "r1".to_string(),
            RouterSession::rehydrated(&cfg, &PersistedSession::default(), Vec::new()),
        );
        let proxy_task = shared.llm_proxy.bind(shared.clone()).await.unwrap();
        let spec = shared
            .target_spec(&ProcessKey("claude".to_string()))
            .unwrap();
        let proxy_base = shared
            .llm_proxy
            .process_env(&spec)
            .into_iter()
            .find(|(name, _)| name == "ANTHROPIC_BASE_URL")
            .unwrap()
            .1;

        // Exactly what claude-agent-acp emits for the alias it was configured
        // with; the proxy rewrites it to the pinned api_model.
        let adapter_body = json!({
            "model": "claude-fable-5",
            "messages": [{"role":"user","content":"continue"}]
        });
        let post = |base: String, body: Value| async move {
            reqwest::Client::new()
                .post(format!("{base}/messages"))
                .header("authorization", "Bearer secret")
                .json(&body)
                .send()
                .await
                .unwrap()
        };

        let turn = shared.llm_proxy.begin_turn(
            ProcessKey("claude".to_string()),
            "r1".to_string(),
            "r1".to_string(),
            "d1".to_string(),
            pinned.clone(),
            TaskClass::CodingGeneral,
        );
        let response = post(proxy_base.clone(), adapter_body.clone()).await;

        // (a) The provider's actionable error reaches the adapter unchanged
        //     rather than being swallowed by a silent retry.
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let text = response.text().await.unwrap();
        assert!(
            text.contains("claude update"),
            "provider error relayed: {text}"
        );

        // (b) No original-body retry: the alias's model never went upstream.
        assert_eq!(
            seen.0.lock().unwrap().clone(),
            vec!["claude-fable-5-1".to_string()],
            "a rejected pin must not fall back onto the alias's own model"
        );

        // (c) The candidate is cordoned, with the provider's reason, so the
        //     picker shows it unavailable and routing drops it.
        let cordon = shared
            .headroom
            .lock()
            .unwrap()
            .usage_cordon(&pinned)
            .cloned()
            .expect("pinned candidate cordoned after the rejection");
        assert!(
            cordon.reason.contains("claude update"),
            "cordon carries the provider reason: {}",
            cordon.reason
        );

        // (d) The session is told, in terms it can act on.
        let notice = shared
            .with_session("r1", |session| session.pending_disclosure.join("\n"))
            .unwrap_or_default();
        assert!(
            notice.contains("rejected by the provider") && notice.contains("claude update"),
            "user notified: {notice}"
        );

        // (e) No attempt storm: the next request does not pay another failing
        //     round trip on the pinned model. It routes to a real alternate,
        //     which is a DISCLOSED switch (selected_model changes), not the
        //     pin silently serving something else.
        drop(turn);
        let _turn2 = shared.llm_proxy.begin_turn(
            ProcessKey("claude".to_string()),
            "r1".to_string(),
            "r1".to_string(),
            "d1".to_string(),
            pinned.clone(),
            TaskClass::CodingGeneral,
        );
        let second = post(proxy_base, adapter_body).await;
        assert_eq!(second.status(), StatusCode::OK);
        let _ = second.text().await.unwrap();
        let models = seen.0.lock().unwrap().clone();
        assert_eq!(
            models
                .iter()
                .filter(|model| *model == "claude-fable-5-1")
                .count(),
            1,
            "the rejected pinned model must be tried once, not per request: {models:?}"
        );

        proxy_task.abort();
        upstream_task.abort();
    }

    /// End-to-end at the WIRE: a session pinned to a version that rides a
    /// downstream alias must have the provider see that version's `api_model`,
    /// not the model string the adapter emitted for the alias.
    ///
    /// This is the one assertion that cannot be satisfied by a correct
    /// selector alias: the adapter is configured with `claude-fable-5[1m]` and
    /// sends `claude-fable-5`, and only the proxy rewrite decides what the
    /// provider actually runs.
    #[tokio::test]
    async fn proxy_puts_the_pinned_versions_api_model_on_the_wire() {
        #[derive(Clone)]
        struct Seen(Arc<Mutex<Vec<String>>>);

        async fn upstream(
            State(seen): State<Seen>,
            axum::Json(body): axum::Json<Value>,
        ) -> Response<Body> {
            seen.0
                .lock()
                .unwrap()
                .push(body["model"].as_str().unwrap_or("").to_string());
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(
                    "event: message_start\n\
                     data: {\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n",
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
        let yaml = format!(
            r#"
state_file: {}
llm_proxy:
  enabled: true
agents:
  - name: claude
    command: {{ type: stdio, command: claude-agent-acp, env: [{{ name: CLAUDE_CONFIG_DIR, value: {} }}] }}
    model_selection: {{ type: config-option }}
    llm_proxy:
      protocol: anthropic
      base_url_env: ANTHROPIC_BASE_URL
      upstream_base_url: http://{upstream_addr}/v1
    models:
      - {{ id: sonnet, cost_rank: 2, api_model: claude-sonnet-5,
           pricing: {{ input_per_mtok: 2, output_per_mtok: 10 }} }}
      - {{ id: "claude-fable-5[1m]", cost_rank: 5, api_model: claude-fable-5,
           pricing: {{ input_per_mtok: 10, output_per_mtok: 50 }},
           versions: [ {{ api_model: claude-fable-5-1,
                          pricing: {{ input_per_mtok: 10, output_per_mtok: 50 }} }} ] }}
pinned_versions:
  "claude/claude-fable-5[1m]": claude-fable-5-1
"#,
            dir.path().join("state.db").display(),
            claude_credential_dir(dir.path()).display()
        );
        let cfg = crate::config::Config::from_yaml(&yaml).unwrap();
        let shared = Shared::new(cfg).unwrap();
        shared.set_models_routeable(
            &ProcessKey("claude".to_string()),
            vec!["sonnet".into(), "claude-fable-5[1m]".into()],
        );
        shared.state.lock().unwrap().upsert(
            "r1".to_string(),
            PersistedSession {
                agent: "claude".to_string(),
                model: "claude-fable-5-1".to_string(),
                downstream_session_id: "d1".to_string(),
                cwd: dir.path().to_path_buf(),
                kind: "primary".to_string(),
                ..Default::default()
            },
        );
        let proxy_task = shared.llm_proxy.bind(shared.clone()).await.unwrap();
        let spec = shared
            .target_spec(&ProcessKey("claude".to_string()))
            .unwrap();
        let proxy_base = shared
            .llm_proxy
            .process_env(&spec)
            .into_iter()
            .find(|(name, _)| name == "ANTHROPIC_BASE_URL")
            .unwrap()
            .1;
        let _turn = shared.llm_proxy.begin_turn(
            ProcessKey("claude".to_string()),
            "r1".to_string(),
            "r1".to_string(),
            "d1".to_string(),
            CandidateId::new("claude", "claude-fable-5[1m]"),
            TaskClass::CodingGeneral,
        );

        let response = reqwest::Client::new()
            .post(format!("{proxy_base}/messages"))
            .header("authorization", "Bearer secret")
            // Exactly what claude-agent-acp emits for the `claude-fable-5[1m]`
            // alias it was configured with.
            .json(&json!({
                "model": "claude-fable-5",
                "messages": [{"role":"user","content":"continue"}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.text().await.unwrap();

        assert_eq!(
            seen.0.lock().unwrap().clone(),
            vec!["claude-fable-5-1".to_string()],
            "the provider must run the pinned version, not the alias's resolution"
        );

        proxy_task.abort();
        upstream_task.abort();
    }
}
