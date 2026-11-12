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

/// Claude Code's native self-updating install (`claude install`), when present.
fn native_claude_code() -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let path = std::path::Path::new(&std::env::var_os("HOME")?).join(".local/bin/claude");
    let meta = std::fs::metadata(&path).ok()?;
    (meta.is_file() && meta.permissions().mode() & 0o111 != 0).then(|| path.display().to_string())
}

/// Claude Code settings a `claude-agent-acp` target gets when its configuration
/// leaves them unset. An operator value always wins.
///
/// - `CLAUDE_CODE_EXECUTABLE`: the adapter otherwise runs the exact Claude Code
///   release it bundles, which lags the native install that updates itself, so
///   a model released today stays unknown until the adapter ships again.
/// - `ANTHROPIC_CUSTOM_MODEL_OPTION`: for an inference-only login Anthropic's
///   model list still carries a Fable row for an older version, which hides
///   Claude Code's own `fable` row. Claude Code then lists one custom row,
///   named for the Fable model the catalog chooses for this agent (the pinned
///   version, else the newest). Every Fable version is selected through it.
fn claude_code_env(
    agent: &AgentConfig,
    mut env: Vec<(String, String)>,
    native: Option<&str>,
) -> Vec<(String, String)> {
    let adapter = std::path::Path::new(&agent.command.command)
        .file_name()
        .is_some_and(|name| name == "claude-agent-acp");
    if !adapter {
        return env;
    }
    let fable_models = || {
        agent
            .models
            .iter()
            .filter(|m| m.family.as_deref() == Some("fable"))
    };
    let fable = fable_models()
        .find(|m| m.auto_eligible)
        .or_else(|| fable_models().next())
        .map(|m| m.api_model.as_deref().unwrap_or(&m.id));
    for (name, value) in [
        ("CLAUDE_CODE_EXECUTABLE", native),
        ("ANTHROPIC_CUSTOM_MODEL_OPTION", fable),
    ] {
        if let Some(value) = value
            && !env.iter().any(|(n, _)| n == name)
        {
            env.push((name.to_string(), value.to_string()));
        }
    }
    env
}

/// Expand one agent config into its process targets.
pub(crate) fn agent_targets(agent: &AgentConfig) -> Vec<ProcessTargetSpec> {
    match &agent.model_selection {
        ModelSelectionConfig::ConfigOption => vec![ProcessTargetSpec {
            key: ProcessKey(agent.name.clone()),
            agent_name: agent.name.clone(),
            command: agent.command.command.clone(),
            args: agent.command.args.clone(),
            env: claude_code_env(
                agent,
                agent
                    .command
                    .env
                    .iter()
                    .map(|e| (e.name.clone(), e.value.clone()))
                    .collect(),
                native_claude_code().as_deref(),
            ),
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

/// `bypass_hook_trust` for a Codex adapter: codex-acp passes `CODEX_CONFIG` to
/// Codex's `thread/start` as per-thread overrides, and Codex reads this key
/// there as `--dangerously-bypass-hook-trust`. Every other key is kept.
fn codex_hook_trust_env(env: &mut Vec<(String, String)>) {
    let mut config = env
        .iter()
        .rev()
        .find(|(name, _)| name == "CODEX_CONFIG")
        .and_then(|(_, value)| serde_json::from_str::<serde_json::Value>(value).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    config.insert("bypass_hook_trust".into(), serde_json::Value::Bool(true));
    env.retain(|(name, _)| name != "CODEX_CONFIG");
    env.push((
        "CODEX_CONFIG".into(),
        serde_json::Value::Object(config).to_string(),
    ));
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
            if crate::accounts::provider(agent) == Some("codex") && agent.bypass_hook_trust {
                codex_hook_trust_env(&mut acp_agent.env);
            }
            crate::credentials::runtime(agent, &mut acp_agent.env)
                .await
                .map_err(|message| AcpError::internal_error().data(message))?
        }
        None => None,
    };
    // The adapter inherits the router's working directory, which is the
    // probe cwd.
    shared.mcp_broker.register_runtime(
        key,
        &shared.probe_cwd,
        credential
            .as_ref()
            .and_then(crate::credentials::RuntimeCredential::claude_settings),
    );

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
    // request before the adapter will accept `session/new`. Without saved
    // material Grok treats that request as an interactive sign-in and never
    // answers, so only `/login` may start one.
    if crate::accounts::provider(&agent) == Some("grok")
        && crate::credentials::present(&agent)
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

const MANAGED_AUTHENTICATE_TIMEOUT_MS: u64 = 15_000;

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
    // Bounded: router initialize waits on every probe, so a silent adapter
    // here would block the handshake for every provider.
    let timeout = Duration::from_millis(
        shared
            .cfg
            .probe_timeout_ms
            .min(MANAGED_AUTHENTICATE_TIMEOUT_MS),
    );
    tokio::time::timeout(
        timeout,
        conn.send_request(AuthenticateRequest::new(method))
            .block_task(),
    )
    .await
    .map_err(|_| {
        AcpError::internal_error().data(format!(
            "target {key} did not answer managed authentication"
        ))
    })??;
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

/// Replace an adapter process that holds a stalled turn it will not end
/// (`crate::planner_watchdog`). Stopping it marks the target dead, which drops
/// its session routes; the pinned session then reopens on the replacement
/// through the dead-downstream path (a fresh session plus SQLite history
/// lookup), exactly as after an adapter crash.
pub(crate) async fn restart_stalled(
    shared: &Arc<Shared>,
    key: &ProcessKey,
) -> Result<(), AcpError> {
    let gate = shared
        .targets
        .lock()
        .unwrap()
        .get(key)
        .map(|t| t.start_gate.clone())
        .ok_or_else(|| AcpError::internal_error().data(format!("unknown target {key}")))?;
    let _gate = tokio::time::timeout(std::time::Duration::from_secs(30), gate.lock())
        .await
        .map_err(|_| {
            AcpError::internal_error().data("timed out waiting for this adapter's startup gate")
        })?;
    restart_target(shared, key).await?;
    match probe_target_once(shared, key).await {
        ProbeOutcome::Routeable => Ok(()),
        _ => Err(AcpError::internal_error()
            .data("the replacement adapter did not become routeable after a stalled turn")),
    }
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
                    // The catalog decides which models exist. A model is
                    // flagged missing only when the selector offers no row
                    // for it and none for its family.
                    for model in &spec.models {
                        if selector_row(&values, model, &spec.models).is_some() {
                            routeable.push(model.id.clone());
                        } else {
                            tracing::warn!(
                                agent = spec.agent_name,
                                model = model.id,
                                available = ?values,
                                "model selector offers no row for this model or its family; \
                                 flagging it missing"
                            );
                            shared.set_model_invalid(key, &model.id, MISSING_FROM_BINARY);
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

/// Claude model families the adapter's selector may list by bare family name.
const CLAUDE_SELECTOR_FAMILIES: [&str; 4] = ["opus", "sonnet", "haiku", "fable"];

/// Selector spellings a catalog `id` may be offered as, in preference order:
/// the id itself, the id without a legacy `[1m]` suffix, then the bare family
/// alias for a full Claude id (`claude-fable-5-1` → `fable`). An alias is
/// only ever the bare family name; no spelling here carries a `[1m]` suffix
/// the id did not already have. The alias names no version; the LLM proxy
/// still sends the pin's `api_model`.
pub fn selector_aliases(id: &str) -> Vec<String> {
    let base = id.strip_suffix("[1m]").unwrap_or(id);
    let mut aliases = vec![id.to_string()];
    if base != id {
        aliases.push(base.to_string());
    }
    if let Some(family) = base.strip_prefix("claude-").and_then(|rest| {
        CLAUDE_SELECTOR_FAMILIES
            .into_iter()
            .find(|family| rest.starts_with(&format!("{family}-")))
    }) {
        aliases.push(family.to_string());
    }
    aliases
}

/// First value in `values` that names `wanted` (exact match, then `[1m]` and
/// family synonyms), else the entry's exact `api_model`. A family candidate
/// (`fable`) whose bare row the adapter hides still selects the row named for
/// the exact model it sends, never a row for another version.
pub fn offered_selector_value<'a>(
    values: &'a [String],
    wanted: &str,
    api_model: Option<&str>,
) -> Option<&'a str> {
    selector_aliases(wanted)
        .into_iter()
        .chain(api_model.map(str::to_string))
        .find_map(|alias| values.iter().find(|v| **v == alias))
        .map(String::as_str)
}

/// Why a candidate is flagged missing: its provider binary offers no selector
/// row for the model or for its family. Advertised as `missing` in
/// `router.candidate`, never dropped from it.
pub const MISSING_FROM_BINARY: &str = "Missing from binary";

/// The selector row that serves `model`: its own row (`offered_selector_value`),
/// else the row of another version of its family. The family row only opens the
/// session; the per-request proxy puts the model's exact id on the wire, and a
/// provider that rejects that id cordons the candidate with its reason.
pub fn selector_row<'a>(
    values: &'a [String],
    model: &ModelConfig,
    models: &[ModelConfig],
) -> Option<&'a str> {
    let own = |m: &ModelConfig| offered_selector_value(values, &m.id, m.api_model.as_deref());
    own(model).or_else(|| {
        let family = model.family.as_deref()?;
        models
            .iter()
            .filter(|m| m.id != model.id && m.family.as_deref() == Some(family))
            .find_map(own)
    })
}

/// Value to send to a config-option model selector for this catalog entry.
/// Prefers a spelling the selector actually listed; falls back to `wanted`
/// so a miss still fails verification instead of inventing another id.
pub fn selector_value_to_send(values: &[String], wanted: &str, api_model: Option<&str>) -> String {
    offered_selector_value(values, wanted, api_model)
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
    fn hook_trust_bypass_joins_the_existing_codex_config() {
        let mut env = vec![
            ("PATH".to_string(), "/bin".to_string()),
            (
                "CODEX_CONFIG".to_string(),
                r#"{"model":"gpt-5.1","model_provider":"router-acp-proxy"}"#.to_string(),
            ),
        ];
        codex_hook_trust_env(&mut env);
        let configs: Vec<_> = env.iter().filter(|(n, _)| n == "CODEX_CONFIG").collect();
        assert_eq!(configs.len(), 1, "one CODEX_CONFIG reaches the adapter");
        let value: serde_json::Value = serde_json::from_str(&configs[0].1).unwrap();
        assert_eq!(value["bypass_hook_trust"], serde_json::Value::Bool(true));
        assert_eq!(value["model"], "gpt-5.1");
        assert_eq!(value["model_provider"], "router-acp-proxy");
        assert!(env.iter().any(|(n, v)| n == "PATH" && v == "/bin"));

        let mut bare = Vec::new();
        codex_hook_trust_env(&mut bare);
        assert_eq!(
            bare,
            vec![(
                "CODEX_CONFIG".to_string(),
                r#"{"bypass_hook_trust":true}"#.to_string()
            )]
        );
    }

    #[test]
    fn hook_trust_bypass_is_off_unless_set() {
        let cfg = Config::from_yaml(spawn_config_yaml()).unwrap();
        assert!(cfg.agents.iter().all(|a| !a.bypass_hook_trust));
        let set = spawn_config_yaml().replace(
            "  - name: codex\n",
            "  - name: codex\n    bypass_hook_trust: true\n",
        );
        let cfg = Config::from_yaml(&set).unwrap();
        assert!(
            cfg.agents
                .iter()
                .find(|a| a.name == "codex")
                .unwrap()
                .bypass_hook_trust
        );
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
    fn selector_aliases_are_bare_family_names() {
        assert_eq!(selector_aliases("opus"), ["opus".to_string()]);
        assert_eq!(selector_aliases("fable"), ["fable".to_string()]);
        assert_eq!(
            selector_aliases("claude-fable-5-2"),
            ["claude-fable-5-2".to_string(), "fable".to_string()]
        );
        // A legacy configured `[1m]` id still matches its bare spelling, but
        // no alias ever gains a `[1m]` suffix.
        assert_eq!(
            selector_aliases("opus[1m]"),
            ["opus[1m]".to_string(), "opus".to_string()]
        );
        assert_eq!(
            selector_aliases("claude-fable-5-1[1m]"),
            [
                "claude-fable-5-1[1m]".to_string(),
                "claude-fable-5-1".to_string(),
                "fable".to_string(),
            ]
        );
        for id in [
            "opus",
            "fable",
            "claude-fable-5-2",
            "claude-opus-4-6",
            "sonnet",
        ] {
            assert!(selector_aliases(id).iter().all(|a| !a.ends_with("[1m]")));
        }
    }

    #[test]
    fn full_claude_ids_match_the_selector_family_alias() {
        // claude-agent-acp 0.99.1 lists Fable as `fable`, like `opus`/`sonnet`.
        let family = vec!["default".into(), "opus".into(), "fable".into()];
        assert_eq!(
            offered_selector_value(&family, "claude-fable-5-1[1m]", None),
            Some("fable")
        );
        assert_eq!(
            selector_value_to_send(&family, "claude-fable-5-1[1m]", None),
            "fable"
        );
        let full = vec!["claude-fable-5-1".into(), "fable".into()];
        assert_eq!(
            offered_selector_value(&full, "claude-fable-5-1[1m]", None),
            Some("claude-fable-5-1")
        );
        assert_eq!(
            offered_selector_value(&family, "claude-sonnet-5", None),
            None
        );
        assert_eq!(
            offered_selector_value(&family, "claude-fablet-1", None),
            None
        );
        assert_eq!(offered_selector_value(&family, "fable-5", None), None);
    }

    #[test]
    fn offered_selector_value_prefers_exact_then_bare_spelling() {
        let both = vec!["opus[1m]".into(), "opus".into(), "sonnet".into()];
        assert_eq!(
            offered_selector_value(&both, "opus[1m]", None),
            Some("opus[1m]")
        );
        assert_eq!(offered_selector_value(&both, "opus", None), Some("opus"));
        let bare = vec!["opus".into(), "sonnet".into()];
        assert_eq!(
            offered_selector_value(&bare, "opus[1m]", None),
            Some("opus")
        );
        // A bare id never matches a `[1m]`-suffixed selector entry.
        let suffixed = vec!["opus[1m]".into(), "sonnet".into()];
        assert_eq!(offered_selector_value(&suffixed, "opus", None), None);
        assert_eq!(offered_selector_value(&bare, "haiku", None), None);
    }

    #[test]
    fn claude_code_env_fills_unset_settings_for_the_claude_adapter() {
        let cfg = Config::from_yaml("agents:\n  - name: claude\n").unwrap();
        let agent = &cfg.agents[0];
        let env = claude_code_env(agent, vec![], Some("/home/u/.local/bin/claude"));
        assert_eq!(
            env,
            [
                (
                    "CLAUDE_CODE_EXECUTABLE".to_string(),
                    "/home/u/.local/bin/claude".to_string()
                ),
                (
                    "ANTHROPIC_CUSTOM_MODEL_OPTION".to_string(),
                    "claude-fable-5-2".to_string()
                ),
            ]
        );
        // Operator values win, and no native install means no executable.
        let set = vec![
            (
                "CLAUDE_CODE_EXECUTABLE".to_string(),
                "/opt/claude".to_string(),
            ),
            ("ANTHROPIC_CUSTOM_MODEL_OPTION".to_string(), "x".to_string()),
        ];
        assert_eq!(claude_code_env(agent, set.clone(), Some("/n")), set);
        assert_eq!(
            claude_code_env(agent, vec![], None),
            [(
                "ANTHROPIC_CUSTOM_MODEL_OPTION".to_string(),
                "claude-fable-5-2".to_string()
            )]
        );
    }

    #[test]
    fn claude_code_env_names_the_pinned_fable_version() {
        let cfg = Config::from_yaml(
            "agents:\n  - name: claude\npinned_versions:\n  claude/fable: claude-fable-5-1\n",
        )
        .unwrap();
        assert_eq!(
            claude_code_env(&cfg.agents[0], vec![], None),
            [(
                "ANTHROPIC_CUSTOM_MODEL_OPTION".to_string(),
                "claude-fable-5-1".to_string()
            )]
        );
    }

    /// The selector a real Claude Code lists (2026-11-12): families by bare
    /// name, no `fable` row, and the custom row the router asked for.
    fn claude_selector() -> Vec<String> {
        [
            "default",
            "sonnet",
            "claude-fable-5-1[1m]",
            "opus",
            "haiku",
            "claude-fable-5-2",
        ]
        .map(String::from)
        .to_vec()
    }

    #[test]
    fn every_claude_catalog_model_has_a_selector_row() {
        let cfg = Config::from_yaml("agents:\n  - name: claude\n").unwrap();
        let models = &cfg.agents[0].models;
        let values = claude_selector();
        for model in models {
            assert!(
                selector_row(&values, model, models).is_some(),
                "{} has no selector row",
                model.id
            );
        }
        let row = |id: &str| {
            let model = models.iter().find(|m| m.id == id).unwrap();
            selector_row(&values, model, models).unwrap()
        };
        assert_eq!(row("claude-fable-5-2"), "claude-fable-5-2");
        // An unlisted version opens on its family's row; the proxy sends its id.
        assert_eq!(row("claude-fable-5-1"), "claude-fable-5-2");
        assert_eq!(row("claude-opus-4-6"), "opus");
    }

    #[test]
    fn a_model_with_no_row_for_itself_or_its_family_has_none() {
        let cfg = Config::from_yaml("agents:\n  - name: claude\n").unwrap();
        let models = &cfg.agents[0].models;
        let fable = models.iter().find(|m| m.id == "claude-fable-5-1").unwrap();
        // No Fable row at all: another family's row never stands in.
        let values: Vec<String> = ["default", "sonnet", "opus", "haiku"]
            .map(String::from)
            .to_vec();
        assert_eq!(selector_row(&values, fable, models), None);
        // A model with no family (a custom row) needs its own row.
        let custom: ModelConfig = serde_yaml::from_str("{id: gpt-5.5, cost_rank: 2}").unwrap();
        let codex: Vec<String> = ["gpt-6.1-sol", "gpt-5.6-luna"].map(String::from).to_vec();
        assert_eq!(
            selector_row(&codex, &custom, std::slice::from_ref(&custom)),
            None
        );
    }

    #[test]
    fn claude_code_env_leaves_other_adapters_alone() {
        let cfg = Config::from_yaml(
            "agents:\n  - name: codex\n    command: {type: stdio, command: codex-acp}\n    model_selection: {type: config-option}\n    models:\n      - {id: fable, cost_rank: 5, api_model: claude-fable-5-2}\n",
        )
        .unwrap();
        assert!(claude_code_env(&cfg.agents[0], vec![], Some("/n")).is_empty());
    }

    #[test]
    fn family_candidate_selects_its_exact_api_model_row() {
        // Anthropic's Fable 5.1 row hides Claude Code's own `fable` row; the
        // exact 5.2 row (a custom picker option) is selected instead.
        let custom: Vec<String> = [
            "default",
            "claude-fable-5-1[1m]",
            "opus",
            "claude-fable-5-2",
        ]
        .map(String::from)
        .to_vec();
        assert_eq!(
            offered_selector_value(&custom, "fable", Some("claude-fable-5-2")),
            Some("claude-fable-5-2")
        );
        assert_eq!(
            selector_value_to_send(&custom, "fable", Some("claude-fable-5-2")),
            "claude-fable-5-2"
        );
        // The bare row wins when the adapter lists it.
        let bare: Vec<String> = ["fable", "claude-fable-5-2"].map(String::from).to_vec();
        assert_eq!(
            offered_selector_value(&bare, "fable", Some("claude-fable-5-2")),
            Some("fable")
        );
        // Another version's row never stands in for the candidate's model.
        let old = vec!["claude-fable-5-1[1m]".to_string()];
        assert_eq!(
            offered_selector_value(&old, "fable", Some("claude-fable-5-2")),
            None
        );
    }

    #[test]
    fn selector_value_to_send_uses_the_offered_spelling() {
        let bare = vec!["opus".into(), "sonnet".into()];
        assert_eq!(selector_value_to_send(&bare, "opus[1m]", None), "opus");
        assert_eq!(selector_value_to_send(&bare, "opus", None), "opus");
        let missing = vec!["sonnet".into()];
        assert_eq!(
            selector_value_to_send(&missing, "opus[1m]", None),
            "opus[1m]"
        );
    }
}
