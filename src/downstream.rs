//! Downstream process lifecycle: spawn, initialize, probe, model discovery
//! and verification.

use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    AuthMethod, AuthenticateRequest, Error as AcpError, InitializeRequest, InitializeResponse,
    NewSessionRequest, NewSessionResponse, SessionConfigId, SessionConfigKind, SessionConfigOption,
    SessionConfigOptionCategory,
};
use agent_client_protocol::{Client as ClientRole, on_receive_dispatch};

use crate::transport::ProcessTransport;

use crate::config::{AgentConfig, Config, ModelConfig, ModelSelectionConfig, interpolate};
use crate::session::{Shared, handle_downstream_dispatch};

/// Key identifying one downstream process target. Config-option agents get
/// one target per agent (`claude`); spawn-config agents get one per model
/// (`codex#gpt-5.1`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcessKey(pub String);

impl std::fmt::Display for ProcessKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionKind {
    /// Model chosen per session via `session/set_config_option`.
    ConfigOption,
    /// Model fixed by the process target's argv/env.
    SpawnConfig,
}

/// A fully-resolved downstream process target.
#[derive(Debug, Clone)]
pub struct ProcessTargetSpec {
    pub key: ProcessKey,
    pub agent_name: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    /// Models served by this target (all declared models for config-option;
    /// exactly one for spawn-config).
    pub models: Vec<ModelConfig>,
    pub selection: SelectionKind,
}

/// Expand one agent config into its process targets.
pub(crate) fn agent_targets(agent: &AgentConfig) -> Vec<ProcessTargetSpec> {
    match &agent.model_selection {
        ModelSelectionConfig::ConfigOption => vec![ProcessTargetSpec {
            key: ProcessKey(agent.name.clone()),
            agent_name: agent.name.clone(),
            command: agent.command.command.clone(),
            args: agent.command.args.clone(),
            env: agent
                .command
                .env
                .iter()
                .map(|e| (e.name.clone(), e.value.clone()))
                .collect(),
            models: agent.models.clone(),
            selection: SelectionKind::ConfigOption,
        }],
        ModelSelectionConfig::SpawnConfig { process_template } => agent
            .models
            .iter()
            .map(|model| {
                let subst = |s: &str| {
                    interpolate(s, &|name| (name == "model_id").then(|| model.id.clone()))
                };
                let mut args = agent.command.args.clone();
                args.extend(process_template.args.iter().map(|a| subst(a)));
                let mut env: Vec<(String, String)> = agent
                    .command
                    .env
                    .iter()
                    .map(|e| (e.name.clone(), e.value.clone()))
                    .collect();
                for (k, v) in &process_template.env {
                    match (k.as_str(), v.as_str()) {
                        (Some(name), Some(value)) => env.push((name.to_string(), subst(value))),
                        _ => tracing::warn!(
                            agent = agent.name,
                            "ignoring non-string process_template env entry"
                        ),
                    }
                }
                ProcessTargetSpec {
                    key: ProcessKey(format!("{}#{}", agent.name, model.id)),
                    agent_name: agent.name.clone(),
                    command: agent.command.command.clone(),
                    args,
                    env,
                    models: vec![model.clone()],
                    selection: SelectionKind::SpawnConfig,
                }
            })
            .collect(),
    }
}

/// Expand the whole config into process targets, preserving config order.
pub fn build_targets(cfg: &Config) -> Vec<ProcessTargetSpec> {
    cfg.agents.iter().flat_map(agent_targets).collect()
}

/// Build the spawnable process transport for a target.
pub fn make_process_transport(shared: &Arc<Shared>, spec: &ProcessTargetSpec) -> ProcessTransport {
    ProcessTransport {
        name: spec.key.0.clone(),
        command: spec.command.clone(),
        args: spec.args.clone(),
        env: shared.llm_proxy.process_env(spec),
        scrub_auth_env: false,
    }
}

/// Spawn the downstream process and its client connection as a contained
/// task on the upstream connection. Failures are recorded in shared state
/// instead of tearing down the router.
pub async fn start_downstream(shared: &Arc<Shared>, key: &ProcessKey) -> Result<(), AcpError> {
    let spec = shared
        .target_spec(key)
        .ok_or_else(|| AcpError::internal_error().data(format!("unknown target {key}")))?;
    let upstream = shared
        .upstream()
        .ok_or_else(|| AcpError::internal_error().data("upstream not connected"))?;
    let mut acp_agent = make_process_transport(shared, &spec);
    let configured = shared
        .agent_configs()
        .into_iter()
        .find(|a| a.name == spec.agent_name);
    let credential = match configured.as_ref() {
        Some(agent) => {
            acp_agent.scrub_auth_env = crate::accounts::provider(agent).is_some();
            if crate::accounts::provider(agent) == Some("codex")
                && let Some(home) = crate::credentials::directory(agent)
            {
                crate::codex_logs::rotate_if_oversized(&home);
            }
            crate::credentials::runtime(agent, &mut acp_agent.env)
                .await
                .map_err(|message| AcpError::internal_error().data(message))?
        }
        None => None,
    };

    let relay_shared = shared.clone();
    let relay_key = key.clone();
    let builder = ClientRole
        .builder()
        .name(format!("downstream:{key}"))
        .on_receive_dispatch(
            move |message, _cx| {
                let shared = relay_shared.clone();
                let key = relay_key.clone();
                async move { handle_downstream_dispatch(&shared, &key, message) }
            },
            on_receive_dispatch!(),
        );

    let (conn_tx, conn_rx) = futures::channel::oneshot::channel();
    let task_shared = shared.clone();
    let task_key = key.clone();
    let (stop, stopped) = {
        let mut targets = shared.targets.lock().unwrap();
        let target = targets.get_mut(key).unwrap();
        target.credential_generation = credential.as_ref().map(|c| c.generation.clone());
        if shared
            .account_login
            .lock()
            .unwrap()
            .contains(&spec.agent_name)
            || shared
                .agent_configs()
                .iter()
                .any(|a| a.name == spec.agent_name && a.account_disabled)
        {
            return Err(AcpError::invalid_params().data("This account is signing in"));
        }
        target.stop = Default::default();
        target.stopped = Arc::default();
        (target.stop.clone(), target.stopped.clone())
    };
    upstream.spawn(async move {
        let _credential = credential;
        let connect = builder.connect_with(acp_agent, async |cx| {
            let _ = conn_tx.send(cx.clone());
            std::future::pending::<Result<(), AcpError>>().await
        });
        let (result, planned_stop) = tokio::select! {
            result = connect => (result, false),
            () = stop.cancelled() => (Err(AcpError::internal_error()), true),
        };
        let reason = match (result, planned_stop) {
            (_, true) => "Account adapter restarting".to_string(),
            (Ok(()), false) => "downstream connection closed".to_string(),
            (Err(err), false) => format!("downstream connection failed: {err}"),
        };
        task_shared.mark_target_dead_if_current(&task_key, &stopped, &reason);
        stopped.notify_one();
        // Contained: a downstream death must not tear down the router.
        Ok(())
    })?;

    let conn = conn_rx
        .await
        .map_err(|_| AcpError::internal_error().data(format!("failed to start target {key}")))?;
    shared.set_target_conn(key, conn);
    Ok(())
}

/// Probe result for one target.
#[derive(Debug)]
pub enum ProbeOutcome {
    Routeable,
    AuthPending,
    Failed(String),
}

/// Initialize a downstream target and probe `session/new` to verify its
/// configured models. Updates candidate statuses in shared state.
pub async fn probe_target(shared: &Arc<Shared>, key: &ProcessKey) -> ProbeOutcome {
    let generation = shared
        .targets
        .lock()
        .unwrap()
        .get(key)
        .and_then(|t| t.credential_generation.clone());
    let outcome = probe_target_once(shared, key).await;
    let Some(agent) = shared.target_spec(key).and_then(|t| {
        shared
            .agent_configs()
            .into_iter()
            .find(|a| a.name == t.agent_name)
    }) else {
        return outcome;
    };
    if crate::accounts::provider(&agent).is_none() {
        if matches!(outcome, ProbeOutcome::AuthPending) {
            shared.set_target_auth_pending(key);
        }
        return outcome;
    }
    if matches!(outcome, ProbeOutcome::Routeable) {
        if let Some(observed) = generation.as_deref() {
            crate::credentials::observe_request_success(&agent, observed).await;
        }
        crate::auth::sync_from_manager(shared, &agent);
        return outcome;
    }
    if !matches!(outcome, ProbeOutcome::AuthPending) {
        return outcome;
    }
    // Grok's external credential provider is an ACP authentication method.
    // A valid managed credential therefore still needs one `authenticate`
    // request before the adapter will accept `session/new`.
    if crate::accounts::provider(&agent) == Some("grok")
        && authenticate_from_manager(shared, key).await.is_ok()
    {
        let retried = probe_target_once(shared, key).await;
        if !matches!(retried, ProbeOutcome::AuthPending) {
            return retried;
        }
    }
    let repaired = crate::credentials::repair(&agent, generation.as_deref()).await;
    crate::auth::sync_from_manager(shared, &agent);
    if repaired == crate::credentials::RepairOutcome::Repaired
        && restart_target(shared, key).await.is_ok()
    {
        let retried = probe_target_once(shared, key).await;
        if !matches!(retried, ProbeOutcome::AuthPending) {
            return retried;
        }
    }
    if matches!(
        crate::credentials::availability(&agent),
        crate::auth::AuthAvailability::Unauthenticated { .. }
    ) {
        shared.set_target_auth_pending(key);
        ProbeOutcome::AuthPending
    } else {
        // Unconfirmed failures remain retryable process failures, never logout.
        if let Some(target) = shared.targets.lock().unwrap().get(key) {
            target.stop.cancel();
        }
        shared.mark_target_dead(key, "Authentication status unavailable");
        ProbeOutcome::Failed(
            "Authentication status unavailable; credential repair could not confirm logout".into(),
        )
    }
}

async fn authenticate_from_manager(shared: &Arc<Shared>, key: &ProcessKey) -> Result<(), AcpError> {
    let conn = shared
        .target_conn(key)
        .ok_or_else(AcpError::internal_error)?;
    let init = shared
        .target_init(key)
        .ok_or_else(AcpError::internal_error)?;
    let method = init
        .auth_methods
        .iter()
        .find_map(|method| match method {
            AuthMethod::Agent(method) => Some(method.id.0.to_string()),
            _ => None,
        })
        .ok_or_else(AcpError::internal_error)?;
    conn.send_request(AuthenticateRequest::new(method))
        .block_task()
        .await?;
    Ok(())
}

async fn restart_target(shared: &Arc<Shared>, key: &ProcessKey) -> Result<(), AcpError> {
    let stopped = {
        let targets = shared.targets.lock().unwrap();
        let target = targets.get(key).ok_or_else(AcpError::internal_error)?;
        if target.conn.is_some() {
            target.stop.cancel();
            Some(target.stopped.clone())
        } else {
            None
        }
    };
    if let Some(stopped) = stopped {
        tokio::time::timeout(std::time::Duration::from_secs(5), stopped.notified())
            .await
            .map_err(|_| {
                AcpError::internal_error()
                    .data("This account's adapter did not stop after credential repair")
            })?;
    }
    start_downstream(shared, key).await
}

pub(crate) async fn restart_after_repair(
    shared: &Arc<Shared>,
    key: &ProcessKey,
) -> Result<(), AcpError> {
    let gate = shared
        .targets
        .lock()
        .unwrap()
        .get(key)
        .map(|t| t.start_gate.clone())
        .ok_or_else(AcpError::internal_error)?;
    let _gate = tokio::time::timeout(std::time::Duration::from_secs(30), gate.lock())
        .await
        .map_err(|_| {
            AcpError::internal_error().data("Timed out waiting for this account's adapter restart")
        })?;
    let agent = shared
        .target_spec(key)
        .and_then(|t| {
            shared
                .agent_configs()
                .into_iter()
                .find(|a| a.name == t.agent_name)
        })
        .ok_or_else(AcpError::internal_error)?;
    let current = crate::credentials::request_generation(&agent);
    let ready = shared.targets.lock().unwrap().get(key).is_some_and(|t| {
        t.conn.is_some()
            && t.init.is_some()
            && t.credential_generation == current
            && !t.auth_pending
            && (t.spec.selection == SelectionKind::SpawnConfig || t.model_config_id.is_some())
    });
    if ready {
        return Ok(());
    }
    restart_target(shared, key).await?;
    match probe_target_once(shared, key).await {
        ProbeOutcome::Routeable => {
            crate::auth::sync_from_manager(shared, &agent);
            Ok(())
        }
        _ => Err(AcpError::internal_error()
            .data("This account's adapter remains unavailable after credential repair")),
    }
}

async fn probe_target_once(shared: &Arc<Shared>, key: &ProcessKey) -> ProbeOutcome {
    let timeout = Duration::from_millis(shared.cfg.probe_timeout_ms);
    match tokio::time::timeout(timeout, probe_target_inner(shared, key)).await {
        Ok(outcome) => outcome,
        Err(_) => {
            let reason = format!(
                "target {key} did not finish initialize/probe within {}ms",
                shared.cfg.probe_timeout_ms
            );
            shared.set_target_failed(key, &reason);
            ProbeOutcome::Failed(reason)
        }
    }
}

async fn probe_target_inner(shared: &Arc<Shared>, key: &ProcessKey) -> ProbeOutcome {
    let Some(conn) = shared.target_conn(key) else {
        return ProbeOutcome::Failed(format!("target {key} has no live connection"));
    };
    let spec = shared.target_spec(key).expect("spec exists");

    // 1. initialize, passing through the upstream client's capabilities so
    //    fs/terminal callbacks relay cleanly.
    let init_req = InitializeRequest::new(ProtocolVersion::V1)
        .client_capabilities(shared.upstream_client_capabilities());
    let init: InitializeResponse = match conn.send_request(init_req).block_task().await {
        Ok(resp) => resp,
        Err(err) => {
            let reason = format!("initialize failed: {err}");
            shared.set_target_failed(key, &reason);
            return ProbeOutcome::Failed(reason);
        }
    };
    shared.set_target_init(key, init.clone());

    // 2. Probe session/new. `auth_required` marks the whole target
    //    auth-pending; its candidates are declared but not routeable yet.
    let probe_req = NewSessionRequest::new(shared.probe_cwd.clone());
    let probe: NewSessionResponse = match conn.send_request(probe_req).block_task().await {
        Ok(resp) => resp,
        Err(err) if is_auth_required(&err) => {
            return ProbeOutcome::AuthPending;
        }
        Err(err) => {
            let reason = format!("probe session/new failed: {err}");
            shared.set_target_failed(key, &reason);
            return ProbeOutcome::Failed(reason);
        }
    };

    // 3. Model discovery/validation.
    match spec.selection {
        SelectionKind::SpawnConfig => {
            shared.set_models_routeable(key, spec.models.iter().map(|m| m.id.clone()).collect());
        }
        SelectionKind::ConfigOption => {
            let options = probe.config_options.clone().unwrap_or_default();
            match find_model_option(&options) {
                Some(option) => {
                    let values = select_values(&option);
                    let mut routeable = Vec::new();
                    // Claude has dropped `[1m]` from natively-1M families
                    // (`opus[1m]` → `opus`); accept either spelling so a
                    // server-side picker rename cannot empty the pool.
                    for model in &spec.models {
                        let wanted = model.id.as_str();
                        if offered_selector_value(&values, wanted).is_some() {
                            routeable.push(model.id.clone());
                        } else {
                            if model.auto_eligible {
                                tracing::error!(
                                    agent = spec.agent_name,
                                    model = model.id,
                                    available = ?values,
                                    "auto-eligible model not offered by downstream model selector; \
                                     removing candidate from the pool"
                                );
                            } else {
                                tracing::warn!(
                                    agent = spec.agent_name,
                                    model = model.id,
                                    available = ?values,
                                    "declared model not offered by downstream model selector; \
                                     removing candidate from the pool"
                                );
                            }
                            shared.set_model_invalid(
                                key,
                                &model.id,
                                "not offered by downstream model selector",
                            );
                        }
                    }
                    shared.set_target_model_config_id(key, option.id.clone());
                    shared.set_models_routeable(key, routeable);
                }
                None => {
                    let reason = format!(
                        "target {key} advertises no `category: model` select config option; \
                         cannot verify model selection (agent `{}` uses model_selection: \
                         config-option)",
                        spec.agent_name
                    );
                    shared.set_target_failed(key, &reason);
                    return ProbeOutcome::Failed(reason);
                }
            }
        }
    }

    // 4. Best-effort close of the probe session when supported.
    if init.agent_capabilities.session_capabilities.close.is_some() {
        let close =
            agent_client_protocol::schema::v1::CloseSessionRequest::new(probe.session_id.clone());
        conn.send_request(close).detach();
    }

    ProbeOutcome::Routeable
}

/// Selector spellings a catalog `id` may be offered as.
/// Claude dropped the `[1m]` suffix from natively-1M families (`opus[1m]` → `opus`).
pub fn selector_aliases(id: &str) -> [String; 2] {
    let alt = match id.strip_suffix("[1m]") {
        Some(stripped) => stripped.to_string(),
        None => format!("{id}[1m]"),
    };
    [id.to_string(), alt]
}

/// First value in `values` that names `wanted` (exact match, then `[1m]` synonym).
pub fn offered_selector_value<'a>(values: &'a [String], wanted: &str) -> Option<&'a str> {
    for alias in selector_aliases(wanted) {
        if let Some(v) = values.iter().find(|v| v.as_str() == alias) {
            return Some(v.as_str());
        }
    }
    None
}

/// Value to send to a config-option model selector for this catalog entry.
/// Prefers a spelling the selector actually listed; falls back to `wanted`
/// so a miss still fails verification instead of inventing another id.
pub fn selector_value_to_send(values: &[String], wanted: &str) -> String {
    offered_selector_value(values, wanted)
        .unwrap_or(wanted)
        .to_string()
}

/// Find the model selector among config options: a `select` option with
/// `category: "model"`.
pub fn find_model_option(options: &[SessionConfigOption]) -> Option<SessionConfigOption> {
    options
        .iter()
        .find(|o| {
            matches!(o.category, Some(SessionConfigOptionCategory::Model))
                && matches!(o.kind, SessionConfigKind::Select(_))
        })
        .cloned()
}

/// All selectable value ids of a select option (grouped or not).
pub fn select_values(option: &SessionConfigOption) -> Vec<String> {
    use agent_client_protocol::schema::v1::SessionConfigSelectOptions;
    let SessionConfigKind::Select(select) = &option.kind else {
        return Vec::new();
    };
    match &select.options {
        SessionConfigSelectOptions::Ungrouped(opts) => {
            opts.iter().map(|o| o.value.0.to_string()).collect()
        }
        SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|g| g.options.iter().map(|o| o.value.0.to_string()))
            .collect(),
        _ => Vec::new(),
    }
}

/// The current value of a select option.
pub fn select_current(option: &SessionConfigOption) -> Option<String> {
    match &option.kind {
        SessionConfigKind::Select(select) => Some(select.current_value.0.to_string()),
        _ => None,
    }
}

pub fn is_auth_required(err: &AcpError) -> bool {
    err.code == AcpError::auth_required().code
        || crate::auth::error_is_auth_rejection(&format!(
            "{} {}",
            err.message,
            err.data.clone().unwrap_or_default()
        ))
}

/// Heuristic detection of rate-limit/quota errors from downstream adapters.
pub fn is_rate_limitish(err: &AcpError) -> bool {
    let text = format!("{} {}", err.message, err.data.clone().unwrap_or_default());
    let lower = text.to_lowercase();
    lower.contains("rate limit")
        || lower.contains("rate-limit")
        || lower.contains("ratelimit")
        || lower.contains("quota")
        || lower.contains("429")
        || lower.contains("overloaded")
        || lower.contains("usage limit")
}

/// Verify a `set_config_option` response reports the requested model as
/// current. The response is authoritative; we do not depend on a follow-up
/// `config_option_update` notification.
pub fn verify_model_selected(
    options: &[SessionConfigOption],
    config_id: &SessionConfigId,
    model_id: &str,
) -> Result<(), String> {
    let Some(option) = options.iter().find(|o| &o.id == config_id) else {
        return Err(format!(
            "set_config_option response omits config option `{config_id}`"
        ));
    };
    match select_current(option) {
        Some(current) if current == model_id => Ok(()),
        Some(current) => Err(format!(
            "model selection was a silent no-op: requested `{model_id}` but downstream \
             reports `{current}` as current"
        )),
        None => Err(format!(
            "config option `{config_id}` is not a select option"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use agent_client_protocol::schema::v1::{SessionConfigSelectGroup, SessionConfigSelectOption};

    fn spawn_config_yaml() -> &'static str {
        r#"
agents:
  - name: claude
    command: { type: stdio, command: mock-agent, args: ["--flag"] }
    model_selection: { type: config-option }
    models:
      - { id: sonnet, cost_rank: 2 }
      - { id: opus, cost_rank: 3 }
  - name: codex
    command: { type: stdio, command: mock-codex }
    model_selection:
      type: spawn-config
      process_template:
        env:
          CODEX_CONFIG: '{"model":"${model_id}"}'
        args: ["--model", "${model_id}"]
    models:
      - { id: gpt-5.1, cost_rank: 3 }
      - { id: gpt-5-mini, cost_rank: 1 }
"#
    }

    #[test]
    fn config_option_agent_gets_one_target() {
        let cfg = Config::from_yaml(spawn_config_yaml()).unwrap();
        let targets = build_targets(&cfg);
        assert_eq!(targets.len(), 3);
        assert_eq!(targets[0].key.0, "claude");
        assert_eq!(targets[0].models.len(), 2);
        assert_eq!(targets[0].selection, SelectionKind::ConfigOption);
    }

    #[test]
    fn spawn_config_agent_gets_target_per_model_with_substitution() {
        let cfg = Config::from_yaml(spawn_config_yaml()).unwrap();
        let targets = build_targets(&cfg);
        let t1 = &targets[1];
        assert_eq!(t1.key.0, "codex#gpt-5.1");
        assert_eq!(t1.args, vec!["--model", "gpt-5.1"]);
        assert_eq!(
            t1.env,
            vec![(
                "CODEX_CONFIG".to_string(),
                r#"{"model":"gpt-5.1"}"#.to_string()
            )]
        );
        assert_eq!(t1.models.len(), 1);
        let t2 = &targets[2];
        assert_eq!(t2.key.0, "codex#gpt-5-mini");
        assert_eq!(t2.args, vec!["--model", "gpt-5-mini"]);
    }

    #[test]
    fn model_option_discovery_and_verification() {
        let option = SessionConfigOption::select(
            "model",
            "Model",
            "sonnet",
            vec![SessionConfigSelectGroup::new(
                "anthropic",
                "Anthropic",
                vec![
                    SessionConfigSelectOption::new("sonnet", "Sonnet"),
                    SessionConfigSelectOption::new("opus", "Opus"),
                ],
            )],
        )
        .category(SessionConfigOptionCategory::Model);
        let options = vec![option];
        let found = find_model_option(&options).unwrap();
        assert_eq!(select_values(&found), vec!["sonnet", "opus"]);
        assert_eq!(select_current(&found).unwrap(), "sonnet");
        let config_id = SessionConfigId::new("model");
        assert!(verify_model_selected(&options, &config_id, "sonnet").is_ok());
        let err = verify_model_selected(&options, &config_id, "opus").unwrap_err();
        assert!(err.contains("silent no-op"));
    }

    #[test]
    fn mode_selects_are_not_model_options() {
        let option = SessionConfigOption::select(
            "mode",
            "Mode",
            "code",
            vec![SessionConfigSelectOption::new("code", "Code")],
        )
        .category(SessionConfigOptionCategory::Mode);
        assert!(find_model_option(&[option]).is_none());
    }

    #[test]
    fn selector_aliases_round_trip_the_1m_suffix() {
        assert_eq!(
            selector_aliases("opus[1m]"),
            ["opus[1m]".to_string(), "opus".to_string()]
        );
        assert_eq!(
            selector_aliases("opus"),
            ["opus".to_string(), "opus[1m]".to_string()]
        );
        assert_eq!(
            selector_aliases("claude-fable-5-1[1m]"),
            [
                "claude-fable-5-1[1m]".to_string(),
                "claude-fable-5-1".to_string()
            ]
        );
    }

    #[test]
    fn offered_selector_value_prefers_exact_then_1m_synonym() {
        let both = vec!["opus[1m]".into(), "opus".into(), "sonnet".into()];
        assert_eq!(offered_selector_value(&both, "opus[1m]"), Some("opus[1m]"));
        assert_eq!(offered_selector_value(&both, "opus"), Some("opus"));
        let bare = vec!["opus".into(), "sonnet".into()];
        assert_eq!(offered_selector_value(&bare, "opus[1m]"), Some("opus"));
        let suffixed = vec!["opus[1m]".into(), "sonnet".into()];
        assert_eq!(offered_selector_value(&suffixed, "opus"), Some("opus[1m]"));
        assert_eq!(offered_selector_value(&bare, "haiku"), None);
    }

    #[test]
    fn selector_value_to_send_uses_the_offered_spelling() {
        let bare = vec!["opus".into(), "sonnet".into()];
        assert_eq!(selector_value_to_send(&bare, "opus[1m]"), "opus");
        assert_eq!(selector_value_to_send(&bare, "opus"), "opus");
        let missing = vec!["sonnet".into()];
        assert_eq!(selector_value_to_send(&missing, "opus[1m]"), "opus[1m]");
    }
}
