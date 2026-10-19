//! Router-owned login menus, account configuration, and ordered selection.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::Responder;
use agent_client_protocol::schema::v1::{
    AvailableCommand, AvailableCommandsUpdate, ContentBlock, ContentChunk,
    CreateElicitationRequest, ElicitationAction, ElicitationContentValue, ElicitationFormMode,
    ElicitationSchema, ElicitationSessionScope, EnumOption, Error as AcpError, PromptRequest,
    PromptResponse, SessionNotification, SessionUpdate, StopReason, StringPropertySchema,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::account_scope::{Paths, Scope};
use crate::candidate::CandidateId;
use crate::config::{AgentConfig, Config, EnvVarConfig, UsageSourceConfig};
use crate::session::{CandidateRuntime, CandidateStatus, Shared, TargetRuntime};
use crate::strategies::CandidateView;

const PROVIDERS: [&str; 4] = ["claude", "codex", "grok", "kimi"];
/// Reserve percentages offered by both account reserve menus.
const RESERVE_STEPS: [u32; 11] = [0, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100];

pub fn advertise(shared: &Arc<Shared>, sid: &str) {
    if let Some(cx) = shared.upstream() {
        let commands = vec![
            AvailableCommand::new("login", "Manage provider accounts"),
            AvailableCommand::new("usage", "Show cached usage for every account"),
            AvailableCommand::new("mcp", "Manage the router's MCP server sign-ins"),
        ];
        let _ = cx.send_notification(SessionNotification::new(
            sid.to_string(),
            SessionUpdate::AvailableCommandsUpdate(AvailableCommandsUpdate::new(commands)),
        ));
    }
}

pub(crate) fn merge_commands(
    message: agent_client_protocol::UntypedMessage,
) -> Result<agent_client_protocol::UntypedMessage, AcpError> {
    if message.method() != "session/update"
        || message
            .params()
            .pointer("/update/sessionUpdate")
            .and_then(Value::as_str)
            != Some("available_commands_update")
    {
        return Ok(message);
    }
    let mut params = message.params().clone();
    if let Some(commands) = params
        .pointer_mut("/update/availableCommands")
        .and_then(Value::as_array_mut)
    {
        commands.retain(|c| {
            !matches!(
                c.get("name").and_then(Value::as_str),
                Some("login" | "usage" | "mcp")
            )
        });
        commands.extend([
            serde_json::json!({"name":"login", "description":"Manage provider accounts"}),
            serde_json::json!({"name":"usage", "description":"Show cached usage for every account"}),
            serde_json::json!({"name":"mcp", "description":"Manage the router's MCP server sign-ins"}),
        ]);
    }
    agent_client_protocol::UntypedMessage::new(message.method(), params)
}

pub fn provider(agent: &AgentConfig) -> Option<&'static str> {
    match agent.usage_source {
        Some(UsageSourceConfig::AnthropicOauth) => Some("claude"),
        Some(UsageSourceConfig::CodexRollout) => Some("codex"),
        _ => PROVIDERS
            .into_iter()
            .find(|p| agent.name.split('@').next() == Some(p))
            .or_else(
                || match Path::new(&agent.command.command).file_name()?.to_str()? {
                    "claude-agent-acp" | "claude-acp" => Some("claude"),
                    "codex-acp" => Some("codex"),
                    "grok" => Some("grok"),
                    "kimi" => Some("kimi"),
                    _ => None,
                },
            ),
    }
}

fn directory(agent: &AgentConfig) -> Option<PathBuf> {
    crate::credentials::directory(agent)
}

fn read_json(path: &Path) -> Option<Value> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

pub fn identity(agent: &AgentConfig) -> (String, Option<String>) {
    let fallback = agent.name.clone();
    let Some(dir) = directory(agent) else {
        return (fallback, None);
    };
    match provider(agent) {
        Some("claude") => {
            let meta = read_json(&dir.join(".claude.json")).or_else(|| {
                let home = agent.env_var("HOME")?;
                (dir == Path::new(&home).join(".claude"))
                    .then(|| read_json(&Path::new(&home).join(".claude.json")))
                    .flatten()
            });
            let creds = read_json(&dir.join(".credentials.json"));
            let label = meta
                .as_ref()
                .and_then(|v| v.pointer("/oauthAccount/emailAddress"))
                .and_then(Value::as_str)
                .unwrap_or(&fallback)
                .to_string();
            let plan = creds
                .as_ref()
                .and_then(|v| {
                    // Subscription ownership is the plan shown to the user.
                    // A Team account can still carry a Max-shaped rate tier.
                    v.pointer("/claudeAiOauth/subscriptionType")
                        .or_else(|| v.pointer("/claudeAiOauth/rateLimitTier"))
                })
                .and_then(Value::as_str)
                .map(|plan| plan_label("claude", plan));
            (label, plan)
        }
        Some("codex") => {
            let auth = read_json(&dir.join("auth.json"));
            // ID token claims are metadata only; they are never printed whole.
            let claims = auth
                .as_ref()
                .and_then(|v| v.pointer("/tokens/id_token"))
                .and_then(Value::as_str)
                .and_then(jwt_claims);
            let label = claims
                .as_ref()
                .and_then(|v| v.get("email"))
                .or_else(|| auth.as_ref().and_then(|v| v.get("email")))
                .and_then(Value::as_str)
                .unwrap_or(&fallback)
                .to_string();
            let plan = claims
                .as_ref()
                .and_then(|v| v.get("https://api.openai.com/auth"))
                .and_then(|v| v.get("chatgpt_plan_type"))
                .or_else(|| auth.as_ref().and_then(|v| v.get("plan_type")))
                .and_then(Value::as_str)
                .map(|plan| plan_label("codex", plan));
            (label, plan)
        }
        Some("grok") => {
            let auth = read_json(&dir.join("auth.json"));
            (
                auth.as_ref()
                    .and_then(crate::credentials::grok_auth)
                    .and_then(|v| v.get("email"))
                    .and_then(Value::as_str)
                    .unwrap_or(&fallback)
                    .to_string(),
                agent.account_plan.clone(),
            )
        }
        _ => (fallback, agent.account_plan.clone()),
    }
}

fn plan_label(provider: &str, plan: &str) -> String {
    let lower = plan.to_ascii_lowercase();
    if provider == "claude" && lower.contains("max") {
        return if lower.contains("20x") {
            "Personal 20x Max"
        } else if lower.contains("5x") {
            "Personal 5x Max"
        } else {
            "Personal Max"
        }
        .to_string();
    }
    match lower.as_str() {
        "pro" => {
            if provider == "claude" {
                "Personal Pro"
            } else {
                "Pro"
            }
        }
        "plus" => "Plus",
        "team" | "default_claude_team" => "Team",
        "enterprise" | "default_claude_enterprise" => "Enterprise",
        "business" => "Business",
        "free" => "Free",
        "edu" => "Edu",
        _ => plan,
    }
    .to_string()
}

pub(crate) fn jwt_claims(token: &str) -> Option<Value> {
    // JWT payloads use base64url. No validation is implied by this label read.
    let encoded = token.split('.').nth(1)?;
    let mut bits = 0u32;
    let mut count = 0;
    let mut bytes = Vec::new();
    for c in encoded.bytes().take_while(|c| *c != b'=') {
        let value = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        bits = (bits << 6) | u32::from(value);
        count += 6;
        if count >= 8 {
            count -= 8;
            bytes.push((bits >> count) as u8);
        }
    }
    serde_json::from_slice(&bytes).ok()
}

pub(crate) fn registered(agent: &AgentConfig) -> bool {
    // Membership is configuration, never the presence of an OAuth token.
    // Provider logout can delete its credential file without deleting a login.
    !agent.account_disabled && provider(agent).is_some()
}

pub(crate) fn routing_disabled(agent: &AgentConfig) -> bool {
    agent.account_disabled
        || agent.reserve_capacity.weekly >= 100.0
        || agent.reserve_capacity.session >= 100.0
}

fn accounts(shared: &Arc<Shared>, group: &str) -> Vec<AgentConfig> {
    let mut agents: Vec<_> = shared
        .agent_configs()
        .into_iter()
        .filter(|a| provider(a) == Some(group) && registered(a))
        .collect();
    agents.sort_by_key(|a| a.account_priority.unwrap_or(u32::MAX));
    agents
}

/// Account order is an eligibility gate, before any strategy's utility math.
/// An explicit account pick may override order, while cordons still apply.
pub fn prioritize(
    views: &mut Vec<CandidateView>,
    agents: &[AgentConfig],
    admit: Option<&CandidateId>,
) {
    let group = |agent: &AgentConfig| {
        provider(agent)
            .unwrap_or_else(|| agent.name.split('@').next().unwrap())
            .to_string()
    };
    // Spend included capacity across accounts before using paid overage.
    let included: Vec<_> = views
        .iter()
        .filter(|v| v.plan_headroom.is_some_and(|p| p > 0.0))
        .filter_map(|v| agents.iter().find(|a| a.name == v.id.agent).map(&group))
        .collect();
    views.retain(|v| {
        admit == Some(&v.id)
            || !v.on_overage
            || !agents
                .iter()
                .find(|a| a.name == v.id.agent)
                .is_some_and(|a| included.contains(&group(a)))
    });
    let mut first: HashMap<String, u32> = HashMap::new();
    for agent in agents {
        if let Some(priority) = agent.account_priority
            && views.iter().any(|v| v.id.agent == agent.name)
        {
            first
                .entry(group(agent))
                .and_modify(|p| *p = (*p).min(priority))
                .or_insert(priority);
        }
    }
    views.retain(|v| {
        if admit == Some(&v.id) {
            return true;
        }
        let Some(agent) = agents.iter().find(|a| a.name == v.id.agent) else {
            return true;
        };
        agent
            .account_priority
            .is_none_or(|p| first.get(&group(agent)) == Some(&p))
    });
}

#[derive(Clone, Debug)]
pub enum Menu {
    Providers,
    Provider(String, Vec<String>),
    Account(String),
    AddScope(String),
    Scope(String),
    Priority(String),
    Reserve(String, Scope),
    ReserveWindow(String, Scope, ReserveWindow),
    Delete(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReserveWindow {
    Weekly,
    Session,
}

impl ReserveWindow {
    pub(crate) fn label(self) -> &'static str {
        match self {
            ReserveWindow::Weekly => "weekly",
            ReserveWindow::Session => "session",
        }
    }
}

const RELOGIN: &str = "Re-login";
const SET_PRIORITY: &str = "Set priority";
const SYSTEM_RESERVE: &str = "Reserve Capacity (System)";
const PROJECT_RESERVE: &str = "Reserve Capacity (Project)";
const SET_SCOPE: &str = "Change account scope";
const DELETE: &str = "Delete account";
const BACK: &str = "Back";

fn account_options(_agent: &AgentConfig) -> Vec<String> {
    // Re-login and Delete keep their original numbers (1 and 2).
    let mut options = vec![
        RELOGIN.to_string(),
        DELETE.to_string(),
        SET_PRIORITY.to_string(),
    ];
    // 100% disables even providers that do not report numeric usage.
    options.extend([
        SYSTEM_RESERVE.into(),
        PROJECT_RESERVE.into(),
        SET_SCOPE.into(),
    ]);
    options.push(BACK.to_string());
    options
}

fn account_label(agent: &AgentConfig) -> String {
    let (label, plan) = identity(agent);
    plan.map(|p| format!("{label} ({p})")).unwrap_or(label)
}

fn provider_menu(shared: &Arc<Shared>, p: &str) -> Menu {
    Menu::Provider(
        p.to_string(),
        accounts(shared, p).into_iter().map(|a| a.name).collect(),
    )
}

#[derive(Clone, Debug)]
enum LoginStatus {
    Pending,
    Browser(String),
    Success(String),
    Error(String),
}

#[derive(Clone)]
pub struct LoginFlow {
    provider: &'static str,
    input: tokio::sync::mpsc::Sender<String>,
    cancel: CancellationToken,
    status: Arc<Mutex<LoginStatus>>,
    changed: Arc<tokio::sync::Notify>,
}

impl LoginFlow {
    fn set(&self, status: LoginStatus) {
        *self.status.lock().unwrap() = status;
        self.changed.notify_one();
    }
}

/// The user's typed text when the prompt is text only. goose adds its
/// `<turn-context>…</turn-context>` preamble as a second text block (or inside
/// the first), so that span is dropped before a command is recognized.
fn text(prompt: &[ContentBlock]) -> Option<String> {
    let mut typed = String::new();
    for block in prompt {
        let ContentBlock::Text(t) = block else {
            return None;
        };
        typed.push_str(&without_turn_context(&t.text));
        typed.push('\n');
    }
    Some(typed.trim().to_string())
}

pub(crate) fn without_turn_context(text: &str) -> String {
    const OPEN: &str = "<turn-context>";
    const CLOSE: &str = "</turn-context>";
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find(OPEN) {
        out.push_str(&rest[..start]);
        match rest[start..].find(CLOSE) {
            Some(end) => rest = &rest[start + end + CLOSE.len()..],
            None => return out,
        }
    }
    out.push_str(rest);
    out
}

pub fn intercepts(shared: &Arc<Shared>, sid: &str, prompt: &[ContentBlock]) -> bool {
    let Some(text) = text(prompt) else {
        return false;
    };
    if text == "/usage" || text == "/login" || text.starts_with("/login ") {
        shared.mcp_menus.lock().unwrap().remove(sid);
        return true;
    }
    if crate::mcp_menu::intercepts(shared, sid, &text) {
        shared.account_menus.lock().unwrap().remove(sid);
        return true;
    }
    let mut menus = shared.account_menus.lock().unwrap();
    if menus.contains_key(sid) {
        if text.parse::<usize>().is_ok() || text == "/back" || text == "/cancel" {
            return true;
        }
        menus.remove(sid);
    }
    false
}

pub fn cancel_login(shared: &Arc<Shared>, sid: &str) {
    crate::mcp_menu::cancel(shared, sid);
    if let Some(cancel) = shared.account_cancellations.lock().unwrap().get(sid) {
        cancel.cancel();
    }
    if let Some(flow) = shared.account_flows.lock().unwrap().remove(sid) {
        flow.cancel.cancel();
    }
    shared.account_menus.lock().unwrap().remove(sid);
}

pub fn cancel_all(shared: &Arc<Shared>) {
    for cancel in shared.account_cancellations.lock().unwrap().values() {
        cancel.cancel();
    }
    for flow in shared
        .account_flows
        .lock()
        .unwrap()
        .drain()
        .map(|(_, flow)| flow)
    {
        flow.cancel.cancel();
    }
    shared.account_menus.lock().unwrap().clear();
    shared.mcp_menus.lock().unwrap().clear();
    for (_, pending) in shared.mcp_logins.lock().unwrap().drain() {
        pending.cancel();
    }
}

pub(crate) fn emit(shared: &Arc<Shared>, sid: &str, message: &str) {
    if let Some(cx) = shared.upstream() {
        let _ = cx.send_notification(SessionNotification::new(
            sid.to_string(),
            SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(
                message.to_string(),
            ))),
        ));
    }
}

fn menu(shared: &Arc<Shared>, sid: &str, state: &Menu) -> (String, Vec<String>) {
    match state {
        Menu::Providers => (
            "Manage logins".into(),
            PROVIDERS
                .iter()
                .map(|p| {
                    let count = accounts(shared, p).len();
                    format!(
                        "{p} ({count} {})",
                        if count == 1 { "account" } else { "accounts" }
                    )
                })
                .chain(std::iter::once("MCP accounts".into()))
                .collect(),
        ),
        Menu::Provider(p, names) => {
            let agents = shared.agent_configs();
            let mut labels: Vec<_> = names
                .iter()
                .map(|name| {
                    agents
                        .iter()
                        .find(|a| &a.name == name)
                        .map(account_label)
                        .unwrap_or_else(|| "Account removed".into())
                })
                .collect();
            labels.extend(["Add Account".into(), "Back".into()]);
            (format!("{p}: accounts in priority order"), labels)
        }
        Menu::Account(name) => match shared.agent_configs().iter().find(|a| &a.name == name) {
            Some(a) => (
                format!(
                    "{}\nAccount scope: {}",
                    crate::account_usage::summarize_account(shared, a),
                    Paths::for_session(shared, sid)
                        .map(|p| p.scope(name).label())
                        .unwrap_or("Unavailable")
                ),
                account_options(a),
            ),
            None => ("Account was removed".into(), vec![BACK.into()]),
        },
        Menu::AddScope(provider) => (
            format!("Where should the new {provider} account be available?"),
            vec![
                "System-level account (all projects)".into(),
                "Project-level account (this project only)".into(),
                BACK.into(),
            ],
        ),
        Menu::Scope(name) => (
            format!(
                "Move {name} to which scope? Project moves remove system membership. Credentials are retained; other running routers pick up the change when restarted."
            ),
            vec![
                "System-level account (all projects)".into(),
                "Project-level account (this project only)".into(),
                BACK.into(),
            ],
        ),
        Menu::Priority(name) => {
            let group = account_group(shared, name);
            let agents = shared.agent_configs();
            let label = |n: &String| {
                agents
                    .iter()
                    .find(|a| &a.name == n)
                    .map(account_label)
                    .unwrap_or_else(|| n.clone())
            };
            let mut options: Vec<String> = group
                .iter()
                .enumerate()
                .map(|(i, n)| {
                    if n == name {
                        format!("Position {} (current)", i + 1)
                    } else {
                        format!("Position {} (now {})", i + 1, label(n))
                    }
                })
                .collect();
            options.push(BACK.into());
            (
                format!(
                    "Move {} to which position? Position 1 is used first.",
                    label(name)
                ),
                options,
            )
        }
        Menu::Reserve(name, scope) => {
            let agent = shared.agent_configs().into_iter().find(|a| &a.name == name);
            let label = agent.as_ref().map(account_label).unwrap_or(name.clone());
            let reserve = Paths::for_session(shared, sid)
                .and_then(|p| p.reserve(name, *scope))
                .unwrap_or_default();
            (
                format!(
                    "Reserve Capacity ({}) for {label}: the share of each included-plan window kept \
                     unused. The router moves to another account at {}% weekly or {}% session usage.",
                    scope.label(),
                    100.0 - reserve.weekly,
                    100.0 - reserve.session
                ),
                vec![
                    format!("Weekly reserve ({}%)", reserve.weekly),
                    format!("Session reserve ({}%)", reserve.session),
                    BACK.into(),
                ],
            )
        }
        Menu::ReserveWindow(name, scope, window) => {
            let agent = shared.agent_configs().into_iter().find(|a| &a.name == name);
            let label = agent.as_ref().map(account_label).unwrap_or(name.clone());
            let current = Paths::for_session(shared, sid)
                .and_then(|p| p.reserve(name, *scope))
                .ok()
                .map(|reserve| match window {
                    ReserveWindow::Weekly => reserve.weekly,
                    ReserveWindow::Session => reserve.session,
                })
                .unwrap_or_default();
            let mut options: Vec<String> = RESERVE_STEPS
                .iter()
                .map(|&p| {
                    let label = if p == 100 {
                        "100% (disabled)".into()
                    } else {
                        format!("{p}%")
                    };
                    if f64::from(p) == current {
                        format!("{label} (current)")
                    } else {
                        label
                    }
                })
                .collect();
            if *scope == Scope::Project {
                options.push("Use system reserve".into());
            }
            options.push(BACK.into());
            (
                format!(
                    "Keep how much of {label}'s {} window unused?",
                    window.label()
                ),
                options,
            )
        }
        Menu::Delete(name) => (
            format!("Delete {name} from the router? Existing logouts keep accounts registered."),
            vec!["Keep account".into(), "Delete account".into()],
        ),
    }
}

pub(crate) async fn ask(
    shared: &Arc<Shared>,
    sid: &str,
    title: &str,
    options: &[String],
) -> Result<Option<String>, AcpError> {
    let mut property = StringPropertySchema::new().title(title.to_string());
    if !options.is_empty() {
        property = property.one_of(
            options
                .iter()
                .enumerate()
                .map(|(i, label)| EnumOption::new((i + 1).to_string(), label.clone()))
                .collect::<Vec<_>>(),
        );
    }
    let schema = ElicitationSchema::new().property("choice", property, true);
    let request = CreateElicitationRequest::new(
        ElicitationFormMode::new(ElicitationSessionScope::new(sid.to_string()), schema),
        title.to_string(),
    );
    let response = shared
        .upstream()
        .ok_or_else(AcpError::internal_error)?
        .send_request(request)
        .block_task()
        .await?;
    match response.action {
        ElicitationAction::Accept(answer) => Ok(answer
            .content
            .and_then(|v| v.get("choice").cloned())
            .and_then(|v| match v {
                ElicitationContentValue::String(s) => Some(s),
                _ => None,
            })),
        _ => Ok(None),
    }
}

pub async fn handle_prompt(
    shared: Arc<Shared>,
    sid: String,
    req: PromptRequest,
    responder: Responder<PromptResponse>,
) -> Result<(), AcpError> {
    let command = text(&req.prompt).unwrap_or_default();
    let cancel = responder.cancellation();
    let menu_cancel = CancellationToken::new();
    shared
        .account_cancellations
        .lock()
        .unwrap()
        .insert(sid.clone(), menu_cancel.clone());
    let result = tokio::select! {
        result = handle(&shared, &sid, &command) => result,
        () = async { tokio::select! { () = cancel.cancelled() => {}, () = menu_cancel.cancelled() => {} } } => {
            if let Some(flow) = shared.account_flows.lock().unwrap().remove(&sid) { flow.cancel.cancel(); }
            shared.account_menus.lock().unwrap().remove(&sid);
            Ok(())
        }
    };
    shared.account_cancellations.lock().unwrap().remove(&sid);
    if let Err(err) = result {
        emit(&shared, &sid, &format!("router-acp: {err}"));
    }
    let _ = responder.respond(PromptResponse::new(
        if cancel.is_cancelled() || menu_cancel.is_cancelled() {
            StopReason::Cancelled
        } else {
            StopReason::EndTurn
        },
    ));
    Ok(())
}

async fn handle(shared: &Arc<Shared>, sid: &str, command: &str) -> Result<(), AcpError> {
    // `/mcp`, or a reply to an open `/mcp` menu (intercepts routed it here).
    if command == "/mcp"
        || command.starts_with("/mcp ")
        || shared.mcp_menus.lock().unwrap().contains_key(sid)
    {
        let structured = shared
            .upstream_client_capabilities()
            .elicitation
            .as_ref()
            .is_some_and(|c| c.form.is_some());
        return crate::mcp_menu::handle(shared, sid, command, structured).await;
    }
    if command == "/usage" {
        emit(shared, sid, &crate::account_usage::summarize(shared));
        return Ok(());
    }
    if command == "/login cancel" || command == "/cancel" {
        if let Some(flow) = shared.account_flows.lock().unwrap().remove(sid) {
            flow.cancel.cancel();
        }
        shared.account_menus.lock().unwrap().remove(sid);
        emit(
            shared,
            sid,
            "Login management closed. Accounts remain registered.",
        );
        return Ok(());
    }
    if let Some(code) = command.strip_prefix("/login code ") {
        let flow = shared
            .account_flows
            .lock()
            .unwrap()
            .get(sid)
            .cloned()
            .ok_or_else(|| AcpError::invalid_params().data("No login awaiting a code"))?;
        if !valid_code(code) {
            return Err(AcpError::invalid_params().data("Invalid authorization code"));
        }
        flow.input
            .send(code.to_string())
            .await
            .map_err(|_| AcpError::internal_error().data("Login ended"))?;
        return finish_login(shared, sid, flow, false).await;
    }
    if command == "/login" {
        let flow = shared.account_flows.lock().unwrap().get(sid).cloned();
        if let Some(flow) = flow {
            return finish_login(shared, sid, flow, false).await;
        }
    }
    let structured = shared
        .upstream_client_capabilities()
        .elicitation
        .as_ref()
        .is_some_and(|c| c.form.is_some());
    let mut state = if command == "/login" {
        Menu::Providers
    } else {
        shared
            .account_menus
            .lock()
            .unwrap()
            .get(sid)
            .cloned()
            .unwrap_or(Menu::Providers)
    };
    let mut typed_choice = command.parse::<usize>().ok();
    if command == "/back" {
        state = Menu::Providers;
    }
    loop {
        let (body, options) = menu(shared, sid, &state);
        let choice = match typed_choice.take() {
            Some(c) => c,
            None if structured => {
                let Some(value) = ask(shared, sid, &body, &options).await? else {
                    shared.account_menus.lock().unwrap().remove(sid);
                    emit(
                        shared,
                        sid,
                        "Login management closed. Accounts remain registered.",
                    );
                    return Ok(());
                };
                value.parse().unwrap_or(0)
            }
            None => {
                let numbered = options
                    .iter()
                    .enumerate()
                    .map(|(i, s)| format!("{}. {s}", i + 1))
                    .collect::<Vec<_>>()
                    .join("\n");
                emit(
                    shared,
                    sid,
                    &format!("{body}\n{numbered}\nReply with a number. /cancel closes this menu."),
                );
                shared
                    .account_menus
                    .lock()
                    .unwrap()
                    .insert(sid.to_string(), state);
                return Ok(());
            }
        };
        match state.clone() {
            Menu::Providers if choice == PROVIDERS.len() + 1 => {
                shared.account_menus.lock().unwrap().remove(sid);
                return crate::mcp_menu::handle(shared, sid, "/mcp", structured).await;
            }
            Menu::Providers if (1..=PROVIDERS.len()).contains(&choice) => {
                state = provider_menu(shared, PROVIDERS[choice - 1])
            }
            Menu::Provider(p, names) => {
                if choice > 0 && choice <= names.len() {
                    state = Menu::Account(names[choice - 1].clone());
                } else if choice == names.len() + 1 {
                    state = Menu::AddScope(p);
                } else if choice == names.len() + 2 {
                    state = Menu::Providers;
                } else {
                    emit(shared, sid, "Choose a listed number.");
                }
            }
            Menu::AddScope(p) => {
                if choice == 1 || choice == 2 {
                    let scope = if choice == 1 {
                        Scope::System
                    } else {
                        Scope::Project
                    };
                    let paths = Paths::for_session(shared, sid)
                        .map_err(|e| AcpError::invalid_params().data(e))?;
                    if scope == Scope::Project {
                        paths
                            .prepare_project(shared)
                            .map_err(|e| AcpError::invalid_params().data(e))?;
                    }
                    let (agent, source) = new_account(shared, &p)?;
                    let flow = start_login(
                        shared,
                        sid,
                        agent,
                        Some((source, paths.path(scope).to_path_buf())),
                    )?;
                    shared.account_menus.lock().unwrap().remove(sid);
                    return finish_login(shared, sid, flow, structured).await;
                } else {
                    state = provider_menu(shared, &p);
                }
            }
            Menu::Scope(name) => {
                if choice == 1 || choice == 2 {
                    let scope = if choice == 1 {
                        Scope::System
                    } else {
                        Scope::Project
                    };
                    match crate::account_scope::move_account(shared, sid, &name, scope).await {
                        Ok(()) => emit(
                            shared,
                            sid,
                            &format!("Account moved to {} scope. Login retained.", scope.label()),
                        ),
                        Err(error) => emit(shared, sid, &format!("Scope change failed: {error}")),
                    }
                }
                state = Menu::Account(name);
            }
            Menu::Account(name) => {
                let agent = shared
                    .agent_configs()
                    .into_iter()
                    .find(|a| a.name == name)
                    .ok_or_else(|| AcpError::invalid_params().data("Account no longer exists"))?;
                let options = account_options(&agent);
                match choice.checked_sub(1).and_then(|i| options.get(i)) {
                    Some(o) if o == RELOGIN => {
                        let flow = start_login(shared, sid, agent, None)?;
                        shared.account_menus.lock().unwrap().remove(sid);
                        return finish_login(shared, sid, flow, structured).await;
                    }
                    Some(o) if o == SET_PRIORITY => state = Menu::Priority(name),
                    Some(o) if o == SYSTEM_RESERVE => state = Menu::Reserve(name, Scope::System),
                    Some(o) if o == PROJECT_RESERVE => state = Menu::Reserve(name, Scope::Project),
                    Some(o) if o == SET_SCOPE => state = Menu::Scope(name),
                    Some(o) if o == DELETE => state = Menu::Delete(name),
                    Some(_) => state = provider_menu(shared, provider(&agent).unwrap()),
                    None => emit(shared, sid, "Choose a listed number."),
                }
            }
            Menu::Priority(name) => {
                let group = account_group(shared, &name);
                if choice > 0 && choice <= group.len() {
                    match set_priority(shared, &name, choice - 1).await {
                        Ok(()) => {
                            emit(shared, sid, &format!("Account moved to position {choice}."))
                        }
                        Err(e) => emit(shared, sid, &format!("router-acp: {e}")),
                    }
                    state = Menu::Account(name);
                } else if choice == group.len() + 1 {
                    state = Menu::Account(name);
                } else {
                    emit(shared, sid, "Choose a listed number.");
                }
            }
            Menu::Reserve(name, scope) => match choice {
                1 => state = Menu::ReserveWindow(name, scope, ReserveWindow::Weekly),
                2 => state = Menu::ReserveWindow(name, scope, ReserveWindow::Session),
                3 => state = Menu::Account(name),
                _ => emit(shared, sid, "Choose a listed number."),
            },
            Menu::ReserveWindow(name, scope, window) => {
                if choice > 0 && choice <= RESERVE_STEPS.len() {
                    let percent = RESERVE_STEPS[choice - 1];
                    match crate::account_scope::set_reserve(
                        shared,
                        sid,
                        &name,
                        scope,
                        window,
                        Some(percent),
                    )
                    .await
                    {
                        Ok(()) => emit(
                            shared,
                            sid,
                            &format!("{} reserve set to {percent}%.", window.label()),
                        ),
                        Err(e) => emit(shared, sid, &format!("router-acp: {e}")),
                    }
                    state = Menu::Reserve(name, scope);
                } else if scope == Scope::Project && choice == RESERVE_STEPS.len() + 1 {
                    match crate::account_scope::set_reserve(shared, sid, &name, scope, window, None)
                        .await
                    {
                        Ok(()) => emit(
                            shared,
                            sid,
                            "Project override removed; using system reserve.",
                        ),
                        Err(error) => emit(shared, sid, &format!("Reserve change failed: {error}")),
                    }
                    state = Menu::Reserve(name, scope);
                } else if choice == options.len() {
                    state = Menu::Reserve(name, scope);
                } else {
                    emit(shared, sid, "Choose a listed number.");
                }
            }
            Menu::Delete(name) if choice == 2 => {
                let message = delete_account(shared, &name).await?;
                emit(shared, sid, &message);
                state = Menu::Providers;
            }
            Menu::Delete(name) => state = Menu::Account(name),
            _ => emit(shared, sid, "Choose a listed number."),
        }
    }
}

fn valid_code(code: &str) -> bool {
    (4..=256).contains(&code.len())
        && code
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"#_-".contains(&c))
}

fn new_account(shared: &Arc<Shared>, p: &str) -> Result<(AgentConfig, String), AcpError> {
    if shared.cfg.source_path.is_none() {
        return Err(AcpError::invalid_params()
            .data("Account management needs a file-backed router configuration"));
    }
    let mut agent = shared
        .agent_configs()
        .into_iter()
        .find(|a| provider(a) == Some(p))
        .ok_or_else(|| {
            AcpError::invalid_params().data(format!(
                "Configure the {p} ACP adapter before adding its login"
            ))
        })?;
    let source = agent.name.clone();
    let root = agent.name.split('@').next().unwrap().to_string();
    agent.name = format!("{root}@{}", uuid::Uuid::new_v4().simple());
    agent.account_disabled = false;
    agent.lineage = Some(agent.lineage.clone().unwrap_or(root));
    agent.account_priority = Some(
        shared
            .agent_configs()
            .iter()
            .filter(|a| provider(a) == Some(p))
            .filter_map(|a| a.account_priority)
            .max()
            // Priorities are 1-based; an unset original is given 1 on save.
            .unwrap_or(1)
            .saturating_add(1),
    );
    let home = agent
        .env_var("HOME")
        .ok_or_else(|| AcpError::invalid_params().data("HOME is unavailable"))?;
    let dir = Path::new(&home)
        .join(".config/router-acp/accounts")
        .join(&agent.name);
    std::fs::create_dir_all(&dir).map_err(|e| AcpError::internal_error().data(e.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| AcpError::internal_error().data(e.to_string()))?;
    }
    let variable = directory_variable(p);
    agent.command.env.retain(|e| {
        e.name != variable
            && !(p == "kimi" && e.name == "KIMI_CODE_HOME")
            && !AUTH_ENV.contains(&e.name.as_str())
    });
    agent.command.env.push(EnvVarConfig {
        name: variable.into(),
        value: dir.to_string_lossy().into_owned(),
    });
    if p == "kimi" {
        agent.command.env.push(EnvVarConfig {
            name: "KIMI_CODE_HOME".into(),
            value: dir.to_string_lossy().into_owned(),
        });
    }
    Ok((agent, source))
}

pub(crate) const AUTH_ENV: [&str; 6] = [
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "OPENAI_API_KEY",
    "XAI_API_KEY",
    "KIMI_API_KEY",
];

pub(crate) fn isolated_environment(env: &[(String, String)]) -> bool {
    env.iter().any(|(name, _)| {
        matches!(
            name.as_str(),
            "CLAUDE_CONFIG_DIR"
                | "CODEX_HOME"
                | "GROK_HOME"
                | "GROK_AUTH_PROVIDER_COMMAND"
                | "KIMI_SHARE_DIR"
                | "KIMI_CODE_HOME"
        )
    })
}

fn start_login(
    shared: &Arc<Shared>,
    sid: &str,
    agent: AgentConfig,
    source: Option<(String, PathBuf)>,
) -> Result<LoginFlow, AcpError> {
    let adding = source.is_some();
    let p = provider(&agent).ok_or_else(AcpError::invalid_params)?;
    if !shared
        .account_login
        .lock()
        .unwrap()
        .insert(agent.name.clone())
    {
        return Err(AcpError::invalid_params().data("This account already has a login in progress"));
    }
    // Retire only this account's processes. Sibling sessions can continue.
    let stopped: Vec<_> = shared
        .targets
        .lock()
        .unwrap()
        .values()
        .filter(|t| t.spec.agent_name == agent.name && t.conn.is_some())
        .map(|t| {
            t.stop.cancel();
            t.stopped.clone()
        })
        .collect();
    let (input, rx) = tokio::sync::mpsc::channel(1);
    let flow = LoginFlow {
        provider: p,
        input,
        cancel: CancellationToken::new(),
        status: Arc::new(Mutex::new(LoginStatus::Pending)),
        changed: Arc::default(),
    };
    shared
        .account_flows
        .lock()
        .unwrap()
        .insert(sid.to_string(), flow.clone());
    let shared = shared.clone();
    let runner = flow.clone();
    tokio::spawn(async move {
        let retire = futures::future::join_all(stopped.iter().map(|done| done.notified()));
        let result = match tokio::time::timeout(Duration::from_secs(5), retire).await {
            Ok(_) => run_login(&agent, &runner, rx).await,
            Err(_) => {
                Err("This account's previous adapter did not stop. Login was not started.".into())
            }
        };
        let result = match result {
            Ok(()) if runner.cancel.is_cancelled() => {
                Err("Login cancelled. Existing accounts remain registered.".into())
            }
            Ok(()) if crate::credentials::request_generation(&agent).is_none() => Err(format!(
                "{p} reported a successful sign-in, but did not publish a usable credential. Retry sign-in."
            )),
            Ok(()) if adding => {
                let (source, path) = source.as_ref().unwrap();
                publish_added_at(&shared, &agent, source, &runner.cancel, Some(path)).await
            }
            other => other,
        };
        // Hold only adapter-start gates during restoration. Publish handoffs
        // before making this account available, so no prompt sees a stale pin
        // between the replacement adapter's probe and its handoff notification.
        let gates: Vec<_> = shared
            .targets
            .lock()
            .unwrap()
            .values()
            .filter(|t| t.spec.agent_name == agent.name)
            .map(|t| t.start_gate.clone())
            .collect();
        let permits = tokio::time::timeout(Duration::from_secs(5), async {
            let mut permits = Vec::new();
            for gate in gates {
                permits.push(gate.lock_owned().await);
            }
            permits
        })
        .await;
        if !adding {
            queue_relogin_handoffs(&shared, &agent.name);
        }
        shared.account_login.lock().unwrap().remove(&agent.name);
        let Ok(_permits) = permits else {
            runner.set(LoginStatus::Error(format!(
                "Timed out restoring {}: its adapter startup is still in progress. Retry the session.",
                agent.name
            )));
            return;
        };
        match result {
            Ok(()) => {
                crate::credentials::record_success(&agent);
                crate::auth::note_authenticated(&shared.auth, &agent.name);
                for key in shared.target_keys_for_agent(&agent.name) {
                    if crate::downstream::start_downstream(&shared, &key)
                        .await
                        .is_ok()
                    {
                        crate::downstream::probe_target(&shared, &key).await;
                    }
                }
                runner.set(LoginStatus::Success(format!(
                    "Signed in as {}. Account registration survives logout.",
                    identity(&agent).0
                )));
            }
            Err(error) => {
                if adding && let Some(dir) = directory(&agent) {
                    let _ = std::fs::remove_dir_all(dir);
                }
                if !adding {
                    // A failed sign-in need not invalidate the old credentials.
                    // Restore availability and let stale pins reopen with handoff.
                    for key in shared.target_keys_for_agent(&agent.name) {
                        if crate::downstream::start_downstream(&shared, &key)
                            .await
                            .is_ok()
                        {
                            crate::downstream::probe_target(&shared, &key).await;
                        }
                    }
                }
                runner.set(LoginStatus::Error(error));
            }
        }
    });
    Ok(flow)
}

fn queue_relogin_handoffs(shared: &Arc<Shared>, agent: &str) {
    for session in shared.sessions.lock().unwrap().values_mut() {
        if let Some(pin) = &session.pin
            && pin.candidate.agent == agent
        {
            session
                .pending_switch
                .get_or_insert(crate::session::SwitchRequest {
                    target: pin.candidate.clone(),
                    reason: "Account authentication changed".into(),
                    handoff: crate::session::HandoffStyle::Full,
                    user_pick: false,
                });
        }
    }
}

async fn run_login(
    agent: &AgentConfig,
    flow: &LoginFlow,
    mut input: tokio::sync::mpsc::Receiver<String>,
) -> Result<(), String> {
    let _credential_lock = crate::credentials::lock(agent).await?;
    if flow.cancel.is_cancelled() {
        return Err("Login cancelled. Existing accounts remain registered.".into());
    }
    let canonical = agent;
    let (login, _runtime) = crate::credentials::login_runtime(agent)?;
    let agent = &login;
    let p = provider(agent).unwrap();
    let default_args: &[&str] = match p {
        "claude" => &["auth", "login", "--claudeai"],
        "codex" => &["login", "--device-auth"],
        _ => &["login"],
    };
    let command = agent
        .login_command
        .as_ref()
        .map(|c| c.command.as_str())
        .unwrap_or(p);
    let args: Vec<&str> = agent
        .login_command
        .as_ref()
        .map(|c| c.args.iter().map(String::as_str).collect())
        .unwrap_or_else(|| default_args.to_vec());
    let mut cmd = tokio::process::Command::new(command);
    cmd.args(args)
        .envs(
            agent
                .login_command
                .iter()
                .flat_map(|c| c.env.iter().map(|v| (&v.name, &v.value))),
        )
        .envs(agent.command.env.iter().map(|v| (&v.name, &v.value)))
        .env("NO_BROWSER", "1")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    if let Some(dir) = directory(agent) {
        match p {
            "claude" => {
                cmd.env("CLAUDE_CONFIG_DIR", dir);
                // The login writes the manager's `.credentials.json` on every
                // platform, never a macOS Keychain item the manager cannot see.
                let base = agent
                    .login_command
                    .iter()
                    .flat_map(|c| c.env.iter())
                    .chain(agent.command.env.iter())
                    .find(|v| v.name == "PATH")
                    .map(|v| v.value.as_str());
                cmd.env(
                    "PATH",
                    crate::credentials::claude_file_store_path(agent, base)?,
                );
            }
            "codex" => {
                cmd.env("CODEX_HOME", dir);
            }
            "kimi" => {
                cmd.env("KIMI_SHARE_DIR", &dir).env("KIMI_CODE_HOME", dir);
            }
            "grok" => {
                cmd.env("GROK_HOME", dir)
                    .env_remove("GROK_AUTH_PROVIDER_COMMAND");
            }
            _ => {}
        }
    }
    #[cfg(unix)]
    cmd.process_group(0);
    for key in AUTH_ENV {
        cmd.env_remove(key);
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Could not start {p} login: {e}"))?;
    let _process = crate::transport::DownstreamPidGuard::new(child.id());
    let mut stdin = child.stdin.take().unwrap();
    let (output, mut chunks) = tokio::sync::mpsc::channel::<String>(16);
    let out = child.stdout.take().unwrap();
    let err = child.stderr.take().unwrap();
    for mut reader in [
        Box::new(out) as Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        Box::new(err),
    ] {
        let output = output.clone();
        tokio::spawn(async move {
            let mut bytes = [0; 2048];
            while let Ok(n) = reader.read(&mut bytes).await {
                if n == 0
                    || output
                        .send(String::from_utf8_lossy(&bytes[..n]).into())
                        .await
                        .is_err()
                {
                    break;
                }
            }
        });
    }
    drop(output);
    let deadline = tokio::time::sleep(Duration::from_secs(600));
    tokio::pin!(deadline);
    let mut transcript = String::new();
    loop {
        tokio::select! {
            status = child.wait() => return match status {
                Ok(s) if s.success() => crate::credentials::publish_login(agent, canonical),
                Ok(s) => Err(format!("{p} login failed ({s}). Re-login keeps the account registered.")),
                Err(e) => Err(format!("{p} login failed: {e}")),
            },
            chunk = chunks.recv(), if !chunks.is_closed() || !chunks.is_empty() => if let Some(chunk) = chunk {
                if transcript.len() < 16 * 1024 { transcript.push_str(&chunk); }
                if let Some(browser) = browser_message(&transcript, p) {
                    flow.set(LoginStatus::Browser(browser));
                }
            },
            code = input.recv() => if let Some(code) = code {
                stdin.write_all(format!("{code}\n").as_bytes()).await.map_err(|e| e.to_string())?;
                stdin.flush().await.map_err(|e| e.to_string())?;
            },
            () = flow.cancel.cancelled() => { let _ = child.kill().await; return Err("Login cancelled. Existing accounts remain registered.".into()); },
            () = &mut deadline => { let _ = child.kill().await; return Err("Login timed out. Existing accounts remain registered.".into()); },
        }
    }
}

fn browser_message(output: &str, p: &str) -> Option<String> {
    // Claude prints its sign-in link as an OSC 8 hyperlink,
    // `ESC]8;;<url> BEL <url> ESC]8;; BEL`. Strip OSC sequences as well as CSI
    // styling, or the match runs through the BEL into the repeated URL and the
    // page echoes that corrupted `state` back inside the authorization code.
    let clean = regex::Regex::new(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b\[[0-?]*[ -/]*[@-~]")
        .ok()?
        .replace_all(output, "");
    // Do not publish a URL while its final bytes are still arriving.
    let matched = regex::Regex::new(r#"https://[^\s\"'<>]+"#)
        .ok()?
        .find(&clean)?;
    if matched.end() == clean.len() {
        return None;
    }
    let url = matched.as_str().to_string();
    let code = if p == "claude" {
        None
    } else {
        regex::Regex::new(r"\b[A-Z0-9]{4,10}(?:-[A-Z0-9]{4,10})+\b")
            .ok()?
            .find(&clean)
            .filter(|m| m.end() < clean.len() && clean[m.end()..].starts_with(char::is_whitespace))
            .map(|m| m.as_str().to_string())
    };
    if !matches!(p, "claude" | "kimi") && code.is_none() {
        return None;
    }
    Some(
        code.map(|c| format!("Open {url}\nDevice code: {c}"))
            .unwrap_or_else(|| format!("Open {url}")),
    )
}

async fn finish_login(
    shared: &Arc<Shared>,
    sid: &str,
    flow: LoginFlow,
    structured: bool,
) -> Result<(), AcpError> {
    let mut code_sent = false;
    let mut shown: Option<String> = None;
    loop {
        let status = flow.status.lock().unwrap().clone();
        match status {
            LoginStatus::Success(message) | LoginStatus::Error(message) => {
                shared.account_flows.lock().unwrap().remove(sid);
                emit(shared, sid, &message);
                return Ok(());
            }
            LoginStatus::Browser(message) if shown.as_ref() != Some(&message) => {
                emit(
                    shared,
                    sid,
                    &format!(
                        "{message}\nComplete sign-in in your browser. /login cancel stops this login."
                    ),
                );
                shown = Some(message);
                if !structured {
                    emit(
                        shared,
                        sid,
                        "For a pasted Claude authorization code, use /login code <code>. Type /login to check progress.",
                    );
                    return Ok(());
                }
                // Device-auth commands poll themselves; Claude waits for stdin.
                if flow.provider == "claude" && !code_sent {
                    let Some(code) =
                        ask(shared, sid, "Paste the Claude authorization code", &[]).await?
                    else {
                        flow.cancel.cancel();
                        shared.account_flows.lock().unwrap().remove(sid);
                        return Ok(());
                    };
                    if !valid_code(&code) {
                        flow.cancel.cancel();
                        return Err(AcpError::invalid_params().data("Invalid authorization code"));
                    }
                    flow.input
                        .send(code)
                        .await
                        .map_err(|_| AcpError::internal_error().data("Login ended"))?;
                    code_sent = true;
                }
            }
            _ => {}
        }
        flow.changed.notified().await;
    }
}

async fn write_config(
    shared: &Arc<Shared>,
    edit: impl FnOnce(&mut Value) -> Result<(), String>,
) -> Result<Config, String> {
    let path = crate::account_scope::active_path(shared)?;
    write_config_at(shared, &path, edit).await
}

async fn write_config_at(
    shared: &Arc<Shared>,
    path: &Path,
    edit: impl FnOnce(&mut Value) -> Result<(), String>,
) -> Result<Config, String> {
    let _guard = shared.account_write.lock().await;
    let _file_lock = ConfigLock::acquire(path.to_path_buf())?;
    // Edit the uninterpolated document, so env secrets never become file values.
    let mut document = crate::account_scope::document(path)?;
    edit(&mut document)?;
    save_document(path, &document)
}

/// Validate `document` as a router configuration and replace the file at
/// `path` with it atomically (private temp file, fsync, rename). The caller
/// holds the accounts lock.
pub(crate) fn save_document(path: &Path, document: &Value) -> Result<Config, String> {
    let yaml = serde_yaml::to_string(document).map_err(|e| e.to_string())?;
    let mut cfg = Config::from_yaml(&yaml).map_err(|e| e.to_string())?;
    cfg.source_path = Some(path.to_path_buf());
    let stored = crate::project_config::for_write(path, document)?;
    let yaml = serde_yaml::to_string(&stored).map_err(|e| e.to_string())?;
    save_private(path, yaml.as_bytes())?;
    Ok(cfg)
}

pub(crate) fn save_private(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    // Resolve existing file symlinks before atomic replacement.
    let destination = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let tmp = destination.with_extension(format!("{}.tmp", uuid::Uuid::new_v4().simple()));
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        use std::io::Write;
        let mut file = options.open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, &destination)
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_file(tmp);
        return Err(e.to_string());
    }
    Ok(())
}

pub(crate) struct ConfigLock(Vec<std::fs::File>);
impl ConfigLock {
    pub(crate) fn acquire(path: PathBuf) -> Result<Self, String> {
        let mut locks = Vec::new();
        // Every project writer first locks the shared base, then its overlay.
        // Computing the overlay against a concurrently changed base could
        // otherwise persist stale inherited values as project overrides.
        for path in crate::project_config::lock_paths(&path)? {
            locks.push(Self::acquire_one(path)?);
        }
        Ok(Self(locks))
    }

    fn acquire_one(path: PathBuf) -> Result<std::fs::File, String> {
        crate::local_state::retire_lock(&path.with_extension("accounts.lock"))?;
        let dir = crate::local_state::directory("config-locks", &path)?;
        crate::local_state::private_dir(&dir)?;
        let path = dir.join("write.lock");
        let mut options = std::fs::OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path).map_err(|e| e.to_string())?;
        file.try_lock().map_err(|e| {
            format!(
                "Account configuration is locked at {}. Retry after the other write finishes: {e}",
                path.display()
            )
        })?;
        Ok(file)
    }
}
impl Drop for ConfigLock {
    fn drop(&mut self) {
        for file in &self.0 {
            let _ = file.unlock();
        }
    }
}

/// The environment variable that names a provider's config directory.
pub(crate) fn directory_variable(provider: &str) -> &'static str {
    match provider {
        "claude" => "CLAUDE_CONFIG_DIR",
        "codex" => "CODEX_HOME",
        "grok" => "GROK_HOME",
        "kimi" => "KIMI_SHARE_DIR",
        _ => "HOME",
    }
}

/// The entries of a provider's config directory that are the login itself.
/// Repair records and coordination belong in machine-local state.
fn credential_entries(provider: &str) -> &'static [&'static str] {
    match provider {
        "claude" => &[".credentials.json"],
        "codex" | "grok" => &["auth.json"],
        "kimi" => &["credentials"],
        _ => &[],
    }
}

/// Every seat is an account: `<agent>@<id>`, owning
/// `~/.config/router-acp/accounts/<agent>@<id>`, which holds only its login
/// and account identity. Usage caches live in local state. Skills, plugins and settings
/// stay the computer-wide CLI configuration its adapters read
/// (`credentials::configuration_directory`). A seat still on its CLI's default
/// directory (`~/.claude`, `~/.codex`, `~/.grok`, `~/.kimi`) becomes one once:
///
/// - it is renamed `<agent>@<id>`, as an added account is named; a plain
///   `<agent>/<model>` reference still resolves to it;
/// - its login is copied (private, `0600`); the CLI keeps its own and stays
///   signed in. From then on each side refreshes its own copy, so a provider
///   that rotates refresh tokens may later ask one of them to sign in again;
/// - the configuration is saved under the accounts lock, and a failed save
///   removes the new directory.
///
/// A seat with its own directory (an added account, a configured volume) is
/// left alone. Another router holding the lock defers this to a later start.
/// Returns one line per seat that became an account or could not.
pub fn adopt_seat_directories(config_path: &Path) -> Result<Vec<String>, String> {
    let Ok(_lock) = ConfigLock::acquire(config_path.to_path_buf()) else {
        return Ok(vec![
            "seat directories: configuration busy; retried at the next start".into(),
        ]);
    };
    let raw = crate::project_config::read(config_path)?.ok_or("Missing router configuration")?;
    let inherited = crate::project_config::inherited_names(config_path)?;
    let cfg = Config::from_yaml(&raw).map_err(|e| e.to_string())?;
    let mut document: Value = serde_yaml::from_str(&raw).map_err(|e| e.to_string())?;
    let mut report = Vec::new();
    // Directories this call created, removed again if the configuration is not saved.
    let mut created: Vec<PathBuf> = Vec::new();
    // (old seat name, new seat name), applied to the references below.
    let mut renamed: Vec<(String, String)> = Vec::new();
    let Some(entries) = document.get_mut("agents").and_then(Value::as_array_mut) else {
        return Ok(report);
    };
    for entry in entries.iter_mut() {
        // The expanded multi-account form names each account's directory itself.
        if entry.get("accounts").is_some() {
            continue;
        }
        let Some(name) = entry
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        let Some(agent) = cfg.agents.iter().find(|a| a.name == name) else {
            continue;
        };
        if inherited.contains(&name) {
            continue;
        }
        let Some(p) = provider(agent) else { continue };
        let variable = directory_variable(p);
        let configured = |v: &str| agent.env_var(v).is_some_and(|value| !value.is_empty());
        if configured(variable) || (p == "kimi" && configured("KIMI_CODE_HOME")) {
            continue;
        }
        let Some(shared_dir) = crate::credentials::directory(agent) else {
            continue;
        };
        // Under the HOME the seat itself runs with, as an added account's is,
        // so a seat and the directory it leaves always belong to one user.
        let Some(home) = agent.env_var("HOME") else {
            continue;
        };
        let account = format!("{name}@{}", uuid::Uuid::new_v4().simple());
        let seat = PathBuf::from(home)
            .join(".config/router-acp/accounts")
            .join(&account);
        match adopt_seat(p, &shared_dir, &seat) {
            Ok(()) => {
                created.push(seat.clone());
                let object = entry.as_object_mut().ok_or("Invalid agent entry")?;
                object.insert("name".into(), Value::String(account.clone()));
                object
                    .entry("lineage")
                    .or_insert_with(|| Value::String(name.clone()));
                let env = object
                    .entry("command")
                    .or_insert_with(|| serde_json::json!({}))
                    .as_object_mut()
                    .ok_or("Missing command")?
                    .entry("env")
                    .or_insert_with(|| serde_json::json!([]))
                    .as_array_mut()
                    .ok_or("Invalid command environment")?;
                env.push(serde_json::json!({"name": variable, "value": seat.to_string_lossy()}));
                // The renamed seat keeps only what its preset does not imply.
                if let Ok(serde_yaml::Value::Mapping(mut small)) = serde_yaml::to_value(&*entry) {
                    crate::presets::minimize_agent(&mut small, &crate::config::agent_defaults());
                    if let Ok(small) = serde_json::to_value(small) {
                        *entry = small;
                    }
                }
                report.push(format!(
                    "{name}: now account {account} in {}",
                    seat.display()
                ));
                renamed.push((name, account));
            }
            Err(why) => report.push(format!("{name}: kept {} ({why})", shared_dir.display())),
        }
    }
    if renamed.is_empty() {
        return Ok(report);
    }
    rename_candidate_references(&mut document, &renamed);
    if let Err(e) = save_document(config_path, &document) {
        created.iter().for_each(|dir| remove_entry(dir));
        return Err(format!(
            "seat accounts were not saved; seats keep their shared directories: {e}"
        ));
    }
    Ok(report)
}

/// Point the configuration's exact candidate references (`<seat>/<model>`) at
/// the renamed seats: version pins and the static router's candidate. Globs
/// and other references match the new names already.
fn rename_candidate_references(document: &mut Value, renamed: &[(String, String)]) {
    let rename = |id: &str| -> Option<String> {
        let (agent, model) = id.split_once('/')?;
        renamed
            .iter()
            .find(|(old, _)| old == agent)
            .map(|(_, new)| format!("{new}/{model}"))
    };
    if let Some(pins) = document
        .get_mut("pinned_versions")
        .and_then(Value::as_object_mut)
    {
        let entries: Vec<(String, Value)> = std::mem::take(pins).into_iter().collect();
        for (key, value) in entries {
            pins.insert(rename(&key).unwrap_or(key), value);
        }
    }
    if let Some(candidate) = document.pointer_mut("/routers/static/candidate")
        && let Some(new) = candidate.as_str().and_then(rename)
    {
        *candidate = Value::String(new);
    }
}

/// Create the account directory `seat` and copy only the login into it.
/// On failure the directory is removed again.
fn adopt_seat(provider: &str, shared: &Path, seat: &Path) -> Result<(), String> {
    std::fs::create_dir_all(seat).map_err(|e| format!("cannot create the seat directory: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(seat, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("cannot protect the seat directory: {e}"))?;
    }
    for name in credential_entries(provider) {
        let from = shared.join(name);
        if std::fs::symlink_metadata(&from).is_err() {
            continue;
        }
        if let Err(e) = copy_private(&from, &seat.join(name)) {
            remove_entry(seat);
            return Err(format!("cannot copy {name}: {e}"));
        }
    }
    // Claude names the signed-in account in its client state (`.claude.json`
    // in the config directory, or `~/.claude.json` for the default one). The
    // account keeps that identity and nothing else of that state.
    if provider == "claude" {
        let identity =
            crate::credentials::claude_identity(&shared.join(".claude.json")).or_else(|| {
                (shared.file_name()? == ".claude")
                    .then(|| {
                        crate::credentials::claude_identity(&shared.parent()?.join(".claude.json"))
                    })
                    .flatten()
            });
        if let Some(identity) = identity
            && let Err(e) =
                crate::credentials::write_claude_identity(&seat.join(".claude.json"), identity)
        {
            remove_entry(seat);
            return Err(format!("cannot copy the account identity: {e}"));
        }
    }
    Ok(())
}

/// Copy a login file, or a directory of them, readable only by the owner.
fn copy_private(from: &Path, to: &Path) -> std::io::Result<()> {
    if std::fs::metadata(from)?.is_dir() {
        std::fs::create_dir(to)?;
        for item in std::fs::read_dir(from)? {
            let item = item?;
            copy_private(&item.path(), &to.join(item.file_name()))?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(to, std::fs::Permissions::from_mode(0o700))?;
        }
        return Ok(());
    }
    std::fs::copy(from, to)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(to, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn remove_entry(path: &Path) {
    let _ = std::fs::remove_dir_all(path).or_else(|_| std::fs::remove_file(path));
}

#[cfg(test)]
async fn publish_added(
    shared: &Arc<Shared>,
    agent: &AgentConfig,
    source: &str,
    cancel: &CancellationToken,
) -> Result<(), String> {
    publish_added_at(shared, agent, source, cancel, None).await
}

async fn publish_added_at(
    shared: &Arc<Shared>,
    agent: &AgentConfig,
    source: &str,
    cancel: &CancellationToken,
    destination: Option<&Path>,
) -> Result<(), String> {
    let active = crate::account_scope::active_path(shared)?;
    let templates = crate::account_scope::document(&active)?;
    let path = destination.unwrap_or(&active);
    write_config_at(shared, path, |doc| {
        if cancel.is_cancelled() {
            return Err("Login cancelled. Existing accounts remain registered.".into());
        }
        let agents = doc
            .get_mut("agents")
            .and_then(Value::as_array_mut)
            .ok_or("Missing agents configuration")?;
        let template = templates["agents"]
            .as_array()
            .ok_or("Missing provider templates")?
            .iter()
            .find(|a| a.get("name").and_then(Value::as_str) == Some(source))
            .or_else(|| {
                templates["agents"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|a| a.get("name").and_then(Value::as_str) == source.split('@').next())
            })
            .ok_or("Provider template was removed during login")?;
        let mut added = template.clone();
        let account_env = source
            .split_once('@')
            .and_then(|(_, account)| {
                template
                    .get("accounts")
                    .and_then(Value::as_array)?
                    .iter()
                    .find(|a| a.get("name").and_then(Value::as_str) == Some(account))?
                    .get("env")
                    .and_then(Value::as_array)
            })
            .cloned()
            .unwrap_or_default();
        added["name"] = Value::String(agent.name.clone());
        added.as_object_mut().unwrap().remove("accounts");
        added.as_object_mut().unwrap().remove("account_disabled");
        if crate::project_config::is_project(path) {
            added["account_disabled"] = serde_json::json!(false);
        }
        added["account_priority"] = serde_json::json!(agent.account_priority);
        added["lineage"] = serde_json::json!(agent.lineage);
        added["reserve_capacity"] = serde_json::json!(agent.reserve_capacity);
        let variable = directory_variable(provider(agent).unwrap_or(""));
        // A template that inherits its command from the provider preset has
        // no `command` mapping of its own.
        if added.get("command").is_none() {
            added["command"] = serde_json::json!({});
        }
        let env = added["command"]
            .as_object_mut()
            .ok_or("Missing command")?
            .entry("env")
            .or_insert_with(|| serde_json::json!([]))
            .as_array_mut()
            .ok_or("Invalid command environment")?;
        env.extend(account_env);
        env.retain(|v| {
            v.get("name").and_then(Value::as_str).is_none_or(|n| {
                n != variable
                    && !(provider(agent) == Some("kimi") && n == "KIMI_CODE_HOME")
                    && !AUTH_ENV.contains(&n)
            })
        });
        env.push(serde_json::json!({"name": variable, "value": agent.env_var(variable)}));
        if provider(agent) == Some("kimi") {
            env.push(
                serde_json::json!({"name": "KIMI_CODE_HOME", "value": agent.env_var(variable)}),
            );
        }
        // Keep the original account ahead of the newly added one by default.
        for a in agents.iter_mut().filter(|a| {
            a.get("name")
                .and_then(Value::as_str)
                .is_some_and(|n| n.split('@').next() == agent.name.split('@').next())
        }) {
            if a.get("account_priority").is_none() {
                a["account_priority"] = serde_json::json!(1);
            }
        }
        // Write only what the new seat changes; presets supply the rest.
        if let Ok(serde_yaml::Value::Mapping(mut entry)) = serde_yaml::to_value(&added) {
            crate::presets::minimize_agent(&mut entry, &crate::config::agent_defaults());
            if let Ok(small) = serde_json::to_value(entry) {
                added = small;
            }
        }
        if crate::project_config::is_project(path) {
            added["account_disabled"] = serde_json::json!(false);
        }
        agents.push(added);
        if let Some(pins) = doc
            .get_mut("pinned_versions")
            .and_then(Value::as_object_mut)
        {
            let prefix = format!("{source}/");
            let inherited: Vec<_> = pins
                .iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix(&prefix)
                        .map(|model| (format!("{}/{model}", agent.name), value.clone()))
                })
                .collect();
            pins.extend(inherited);
        }
        Ok(())
    })
    .await?;
    let cfg = Config::from_file(&active).map_err(|e| e.to_string())?;
    let saved = cfg
        .agents
        .iter()
        .find(|a| a.name == agent.name)
        .ok_or("New account was not registered")?
        .clone();
    *shared.account_config.lock().unwrap() = cfg;
    register(shared, &saved);
    Ok(())
}

fn register(shared: &Arc<Shared>, agent: &AgentConfig) {
    shared
        .headroom
        .lock()
        .unwrap()
        .register_agent(&agent.name, agent.budget_prompts_5h);
    let specs = crate::downstream::agent_targets(agent);
    shared.llm_proxy.register_agent(agent, &specs);
    let mut targets = shared.targets.lock().unwrap();
    let mut candidates = shared.candidates.lock().unwrap();
    for spec in specs {
        for model in &spec.models {
            let index = candidates.len();
            candidates.push(CandidateRuntime {
                id: CandidateId::new(&agent.name, &model.id),
                display_name: model
                    .display_name
                    .clone()
                    .unwrap_or_else(|| model.id.clone()),
                cost_rank: model.cost_rank,
                config_index: index,
                process_key: spec.key.clone(),
                status: CandidateStatus::Unverified,
                auto_eligible: model.auto_eligible,
            });
        }
        targets.insert(
            spec.key.clone(),
            TargetRuntime {
                credential_generation: None,
                spec,
                conn: None,
                init: None,
                model_config_id: None,
                auth_pending: false,
                dead: None,
                last_respawn: None,
                start_gate: Arc::default(),
                stop: Default::default(),
                stopped: Arc::default(),
            },
        );
    }
}

/// The registered accounts sharing `name`'s provider, in priority order.
pub(crate) fn account_group(shared: &Arc<Shared>, name: &str) -> Vec<String> {
    shared
        .agent_configs()
        .iter()
        .find(|a| a.name == name)
        .and_then(provider)
        .map(|p| accounts(shared, p).into_iter().map(|a| a.name).collect())
        .unwrap_or_default()
}

/// Set fields on an account wherever the document declares it: a standalone
/// `agents[]` entry (what `/login` → Add Account writes) and/or an entry in
/// its base agent's `accounts:` list.
pub(crate) fn set_account_fields(
    doc: &mut Value,
    name: &str,
    standalone: &[(&str, Value)],
    listed: &[(&str, Value)],
) -> Result<(), String> {
    let agents = doc
        .get_mut("agents")
        .and_then(Value::as_array_mut)
        .ok_or("Missing agents configuration")?;
    let mut found = false;
    for a in agents
        .iter_mut()
        .filter(|a| a.get("name").and_then(Value::as_str) == Some(name))
    {
        for (key, value) in standalone {
            a[*key] = value.clone();
        }
        found = true;
    }
    if let Some((base, account)) = name.split_once('@') {
        for list in agents
            .iter_mut()
            .filter(|a| a.get("name").and_then(Value::as_str) == Some(base))
            .filter_map(|a| a.get_mut("accounts").and_then(Value::as_array_mut))
        {
            for a in list
                .iter_mut()
                .filter(|a| a.get("name").and_then(Value::as_str) == Some(account))
            {
                for (key, value) in listed {
                    a[*key] = value.clone();
                }
                found = true;
            }
        }
    }
    if found {
        Ok(())
    } else {
        Err(format!("{name} is not in the router configuration"))
    }
}

/// Move `name` to `position` (0 = drained first) and renumber its whole
/// provider group 1..=N, so every account gets an explicit, distinct priority.
async fn set_priority(shared: &Arc<Shared>, name: &str, position: usize) -> Result<(), String> {
    let mut order = account_group(shared, name);
    order.retain(|n| n != name);
    order.insert(position.min(order.len()), name.to_string());
    let cfg = write_config(shared, |doc| {
        for (index, account) in order.iter().enumerate() {
            let p = serde_json::json!(index + 1);
            set_account_fields(
                doc,
                account,
                &[("account_priority", p.clone())],
                &[("priority", p)],
            )?;
        }
        Ok(())
    })
    .await?;
    *shared.account_config.lock().unwrap() = cfg;
    Ok(())
}

async fn delete_account(shared: &Arc<Shared>, name: &str) -> Result<String, AcpError> {
    if shared.account_login.lock().unwrap().contains(name) {
        return Err(AcpError::invalid_params().data("Cancel this account's login first"));
    }
    let agents = shared.agent_configs();
    let agent = agents
        .iter()
        .find(|a| a.name == name)
        .ok_or_else(|| AcpError::invalid_params().data("Account no longer exists"))?
        .clone();
    // Removal uses the same authority lock as login and refresh.
    let _credential_lock = crate::credentials::lock(&agent)
        .await
        .map_err(|message| AcpError::internal_error().data(message))?;
    let shared_store = directory(&agent).is_some_and(|dir| {
        agents
            .iter()
            .any(|a| a.name != name && registered(a) && directory(a).as_ref() == Some(&dir))
    });
    let cfg = write_config(shared, |doc| {
        let agents = doc
            .get_mut("agents")
            .and_then(Value::as_array_mut)
            .ok_or("Missing agents configuration")?;
        for a in agents
            .iter_mut()
            .filter(|a| a.get("name").and_then(Value::as_str) == Some(name))
        {
            a["account_disabled"] = serde_json::json!(true);
        }
        if let Some((base, account)) = name.split_once('@') {
            for agent in agents
                .iter_mut()
                .filter(|a| a.get("name").and_then(Value::as_str) == Some(base))
            {
                if let Some(list) = agent.get_mut("accounts").and_then(Value::as_array_mut) {
                    for a in list
                        .iter_mut()
                        .filter(|a| a.get("name").and_then(Value::as_str) == Some(account))
                    {
                        a["disabled"] = serde_json::json!(true);
                    }
                }
            }
        }
        if let Some(pins) = doc
            .get_mut("pinned_versions")
            .and_then(Value::as_object_mut)
        {
            pins.retain(|key, _| key.split('/').next() != Some(name));
        }
        Ok(())
    })
    .await
    .map_err(|e| AcpError::invalid_params().data(e))?;
    *shared.account_config.lock().unwrap() = cfg;
    let mut stopped = Vec::new();
    for key in shared.target_keys_for_agent(name) {
        if let Some(target) = shared.targets.lock().unwrap().remove(&key) {
            target.stop.cancel();
            if target.conn.is_some() {
                stopped.push(target.stopped);
            }
        }
        shared.mark_target_dead(&key, "Account removed");
    }
    shared
        .candidates
        .lock()
        .unwrap()
        .retain(|c| c.id.agent != name);
    shared.llm_proxy.remove_agent(name);
    if shared
        .runtime_config()
        .source_path
        .as_deref()
        .is_some_and(crate::project_config::is_project)
    {
        return Ok(
            "Account removed from this project. Its shared login credentials were retained.".into(),
        );
    }
    if shared_store {
        return Ok("Account removed. Credentials were retained because another configured account shares that directory.".into());
    }
    tokio::time::timeout(
        Duration::from_secs(5),
        futures::future::join_all(stopped.iter().map(|done| done.notified())),
    )
    .await
    .map_err(|_| {
        AcpError::internal_error()
            .data("Account removed, but its adapter did not stop. Credentials were retained.")
    })?;
    remove_credentials(&agent).map_err(|e| {
        AcpError::internal_error().data(format!(
            "Account removed, but saved credentials could not be deleted: {e}"
        ))
    })?;
    Ok(
        "Account removed and its saved credentials deleted. Other accounts keep their credentials."
            .into(),
    )
}

fn remove_credentials(agent: &AgentConfig) -> Result<(), String> {
    let Some(dir) = directory(agent) else {
        return Ok(());
    };
    if provider(agent) == Some("claude") {
        // Claude stores unrelated MCP credentials in the same JSON file.
        remove_json_field(&dir.join(".credentials.json"), "claudeAiOauth")?;
        remove_json_field(&dir.join(".claude.json"), "oauthAccount")?;
        if agent
            .env_var("HOME")
            .is_some_and(|home| dir == Path::new(&home).join(".claude"))
        {
            remove_json_field(
                &Path::new(&agent.env_var("HOME").unwrap()).join(".claude.json"),
                "oauthAccount",
            )?;
        }
    } else {
        let path = dir.join(if provider(agent) == Some("kimi") {
            "credentials/kimi-code.json"
        } else {
            "auth.json"
        });
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

fn remove_json_field(path: &Path, field: &str) -> Result<(), String> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let mut doc: Value = serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
    if doc
        .as_object_mut()
        .is_none_or(|v| v.remove(field).is_none())
    {
        return Ok(());
    }
    let tmp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4().simple()));
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        use std::io::Write;
        let mut file = options.open(&tmp)?;
        file.write_all(serde_json::to_string(&doc)?.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_file(tmp);
        return Err(e.to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A router config whose `claude` seat uses `<home>/.claude` (shared with
    /// the CLI) and whose `claude@work` seat already has its own directory.
    fn seat_fixture() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().canonicalize().unwrap();
        let shared = home.join(".claude");
        std::fs::create_dir_all(shared.join("plugins")).unwrap();
        std::fs::write(
            shared.join(".credentials.json"),
            r#"{"claudeAiOauth":{"accessToken":"a","refreshToken":"r"}}"#,
        )
        .unwrap();
        std::fs::write(shared.join("settings.json"), "{}").unwrap();
        std::fs::write(
            shared.join(".claude.json"),
            r#"{"oauthAccount":{"emailAddress":"base@example.test"},"projects":{"/repo":{}},"machineID":"m"}"#,
        )
        .unwrap();
        let work = home.join("work-claude");
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(work.join(".credentials.json"), "{}").unwrap();
        let config = home.join("router.yaml");
        std::fs::write(
            &config,
            format!(
                "routers:\n  static: {{candidate: claude/sonnet}}\npinned_versions:\n  claude/sonnet: default\nagents:\n  - name: claude\n    command: {{type: stdio, command: mock-agent, env: [{{name: HOME, value: {h}}}]}}\n    model_selection: {{type: config-option}}\n    models: [{{id: sonnet, cost_rank: 1}}]\n  - name: claude@work\n    command: {{type: stdio, command: mock-agent, env: [{{name: HOME, value: {h}}}, {{name: CLAUDE_CONFIG_DIR, value: {w}}}]}}\n    model_selection: {{type: config-option}}\n    models: [{{id: sonnet, cost_rank: 1}}]\n",
                h = home.display(),
                w = work.display()
            ),
        )
        .unwrap();
        // The seat's own HOME decides where its directory goes, never the
        // HOME of the process running the router.
        let accounts = home.join(".config/router-acp/accounts");
        (root, config, shared, accounts)
    }

    #[test]
    fn a_shared_default_seat_becomes_an_account_holding_only_its_login() {
        let (_root, config, shared, accounts) = seat_fixture();
        let original = std::fs::read(shared.join(".credentials.json")).unwrap();
        let report = adopt_seat_directories(&config).unwrap();
        let cfg = Config::from_file(&config).unwrap();
        let claude = cfg
            .agents
            .iter()
            .find(|a| a.name.starts_with("claude@") && a.name != "claude@work")
            .expect("the base seat is now an account");
        let id = claude.name.strip_prefix("claude@").unwrap();
        assert!(
            id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "{}",
            claude.name
        );
        let seat = accounts.join(&claude.name);
        assert_eq!(
            report,
            [format!(
                "claude: now account {} in {}",
                claude.name,
                seat.display()
            )]
        );
        assert!(cfg.agents.iter().all(|a| a.name != "claude"));
        assert_eq!(claude.lineage.as_deref(), Some("claude"));
        // Exact references follow the rename.
        let pins = std::fs::read_to_string(&config).unwrap();
        assert!(pins.contains(&format!("{}/sonnet", claude.name)), "{pins}");
        assert!(!pins.contains("claude/sonnet"), "{pins}");
        // The folder holds the login and nothing else.
        let mut entries: Vec<_> = std::fs::read_dir(&seat)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        entries.sort();
        assert_eq!(entries, [".claude.json", ".credentials.json"]);
        // Of Claude's client state, the account keeps only who signed in.
        let state: Value =
            serde_json::from_slice(&std::fs::read(seat.join(".claude.json")).unwrap()).unwrap();
        assert_eq!(
            state,
            serde_json::json!({"oauthAccount": {"emailAddress": "base@example.test"}})
        );
        assert_eq!(identity(claude).0, "base@example.test");
        // Copied: the native CLI keeps its own login and stays signed in.
        assert_eq!(
            std::fs::read(shared.join(".credentials.json")).unwrap(),
            original
        );
        assert_eq!(
            std::fs::read(seat.join(".credentials.json")).unwrap(),
            original
        );
        assert!(
            std::fs::symlink_metadata(seat.join(".credentials.json"))
                .unwrap()
                .is_file()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(seat.join(".credentials.json"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        assert_eq!(
            claude.env_var("CLAUDE_CONFIG_DIR").as_deref(),
            Some(seat.to_str().unwrap())
        );
        assert_eq!(crate::credentials::directory(claude), Some(seat.clone()));
        // Its adapters still read the computer-wide configuration.
        assert_eq!(
            crate::credentials::configuration_directory(claude, &seat),
            shared
        );
        // A seat that already had its own directory is untouched.
        let work = cfg.agents.iter().find(|a| a.name == "claude@work").unwrap();
        assert!(
            work.env_var("CLAUDE_CONFIG_DIR")
                .unwrap()
                .ends_with("work-claude")
        );
        assert!(!accounts.join("claude@work").exists());
        // Idempotent: the next start finds nothing to move.
        let before = std::fs::read_to_string(&config).unwrap();
        assert!(adopt_seat_directories(&config).unwrap().is_empty());
        assert_eq!(std::fs::read_to_string(&config).unwrap(), before);
    }

    #[cfg(unix)]
    #[test]
    fn an_unsaved_configuration_removes_the_copied_login() {
        use std::os::unix::fs::PermissionsExt;
        let (_root, original, shared, accounts) = seat_fixture();
        // The atomic save writes a temp file beside the config; forbid only
        // that. Its lock lives outside the configuration directory.
        let dir = original.parent().unwrap().join("conf");
        std::fs::create_dir(&dir).unwrap();
        let config = dir.join("router.yaml");
        std::fs::rename(&original, &config).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let result = adopt_seat_directories(&config);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(result.is_err(), "{result:?}");
        assert!(
            shared.join(".credentials.json").is_file(),
            "the CLI's own login is untouched"
        );
        assert!(
            no_accounts(&accounts),
            "the new account directory was removed"
        );
    }

    fn no_accounts(accounts: &Path) -> bool {
        std::fs::read_dir(accounts).map_or(true, |mut entries| entries.next().is_none())
    }

    #[test]
    fn a_busy_configuration_defers_the_move() {
        let (_root, config, shared, accounts) = seat_fixture();
        let _held = ConfigLock::acquire(config.clone()).unwrap();
        let report = adopt_seat_directories(&config).unwrap();
        assert!(report[0].contains("busy"), "{report:?}");
        assert!(shared.join(".credentials.json").is_file());
        assert!(
            no_accounts(&accounts),
            "nothing changes while another router holds the lock"
        );
    }

    #[cfg(unix)]
    #[test]
    fn configuration_aliases_share_a_lock_outside_config() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("router.yaml");
        let alias = root.path().join("alias.yaml");
        std::fs::write(&config, "agents: []").unwrap();
        std::os::unix::fs::symlink(&config, &alias).unwrap();
        let held = ConfigLock::acquire(config.clone()).unwrap();
        assert!(ConfigLock::acquire(alias.clone()).is_err());
        assert!(!config.with_extension("accounts.lock").exists());
        assert!(!alias.with_extension("accounts.lock").exists());
        drop(held);
        assert!(ConfigLock::acquire(alias).is_ok());
    }

    #[test]
    fn commands_ignore_goose_turn_context_in_any_block() {
        let ctx = "<turn-context>\n<t>2026</t>\n</turn-context>";
        let blocks = |parts: &[&str]| -> Vec<ContentBlock> {
            parts
                .iter()
                .map(|p| ContentBlock::from(p.to_string()))
                .collect()
        };
        assert_eq!(text(&blocks(&["/usage"])).as_deref(), Some("/usage"));
        assert_eq!(text(&blocks(&["/usage", ctx])).as_deref(), Some("/usage"));
        assert_eq!(text(&blocks(&[ctx, "/login"])).as_deref(), Some("/login"));
        assert_eq!(
            text(&blocks(&[&format!("{ctx}\n\n/login code abc")])).as_deref(),
            Some("/login code abc")
        );
        assert_eq!(text(&blocks(&[&format!("2\n{ctx}")])).as_deref(), Some("2"));
        assert_eq!(
            text(&blocks(&["/usage", "and explain it"])).as_deref(),
            Some("/usage\nand explain it")
        );
    }

    #[test]
    fn browser_output_waits_for_complete_urls_and_device_codes() {
        assert!(browser_message("https://example.test/de", "claude").is_none());
        assert_eq!(
            browser_message("https://example.test/device\n", "claude").as_deref(),
            Some("Open https://example.test/device")
        );
        assert!(browser_message("https://example.test/device\nABCD-EFGH", "codex").is_none());
        assert_eq!(
            browser_message("https://example.test/device\nABCD-EFGH-4242\n", "codex").as_deref(),
            Some("Open https://example.test/device\nDevice code: ABCD-EFGH-4242")
        );
        assert_eq!(
            browser_message(
                "\x1b[94mhttps://auth.openai.com/codex/device\x1b[0m\n\x1b[94mLWD3-2FXYY\x1b[0m\n",
                "codex",
            )
            .as_deref(),
            Some("Open https://auth.openai.com/codex/device\nDevice code: LWD3-2FXYY")
        );
        // Claude 2.1 prints the manual sign-in link as an OSC 8 hyperlink.
        let url = "https://claude.com/cai/oauth/authorize?code=true&client_id=c&state=s1";
        assert_eq!(
            browser_message(
                &format!("Opening browser to sign in\nIf the browser didn't open, visit: \x1b]8;;{url}\x07{url}\x1b]8;;\x07\nPaste code here if prompted >"),
                "claude",
            )
            .as_deref(),
            Some(format!("Open {url}").as_str())
        );
        assert_eq!(
            browser_message(
                &format!("visit: \x1b]8;;{url}\x1b\\{url}\x1b]8;;\x1b\\\n"),
                "claude"
            )
            .as_deref(),
            Some(format!("Open {url}").as_str()),
            "the ST terminator form strips too"
        );
        assert_eq!(
            plan_label("claude", "default_claude_max_20x"),
            "Personal 20x Max"
        );
    }

    #[test]
    fn claude_subscription_ownership_precedes_its_rate_tier() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("claude");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(
            dir.join(".credentials.json"),
            r#"{"claudeAiOauth":{"subscriptionType":"team","rateLimitTier":"default_claude_max_5x"}}"#,
        )
        .unwrap();
        std::fs::write(
            dir.join(".claude.json"),
            r#"{"oauthAccount":{"emailAddress":"team@example.test"}}"#,
        )
        .unwrap();
        let agent: AgentConfig = serde_yaml::from_str(&format!(
            "name: claude@team\ncommand: {{type: stdio, command: mock, env: [{{name: CLAUDE_CONFIG_DIR, value: {}}}]}}\nmodel_selection: {{type: config-option}}\nmodels: [{{id: opus, cost_rank: 3}}]\n",
            dir.display()
        ))
        .unwrap();
        assert_eq!(
            identity(&agent),
            ("team@example.test".into(), Some("Team".into()))
        );
    }

    #[test]
    fn native_commands_survive_downstream_command_updates() {
        let frame = agent_client_protocol::UntypedMessage::new("session/update", serde_json::json!({"sessionId":"s", "update":{"sessionUpdate":"available_commands_update","availableCommands":[{"name":"help","description":"Provider help"},{"name":"login","description":"Provider login"},{"name":"mcp","description":"Provider MCP"}]}})).unwrap();
        let merged = merge_commands(frame).unwrap();
        let commands = merged
            .params()
            .pointer("/update/availableCommands")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(
            commands
                .iter()
                .map(|c| c["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["help", "login", "usage", "mcp"]
        );
        assert_eq!(commands[1]["description"], "Manage provider accounts");
        assert_eq!(
            commands[3]["description"],
            "Manage the router's MCP server sign-ins"
        );
    }

    #[tokio::test]
    async fn set_priority_moves_an_added_account_first_and_saves_it() {
        // The shape `/login` → Add Account leaves: the original standalone
        // agent plus a standalone `claude@<id>` copy.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("router.yaml");
        let agent = |name: &str, priority: u32| {
            format!(
                "  - name: {name}\n    account_priority: {priority}\n    command: {{type: stdio, command: mock, env: [{{name: CLAUDE_CONFIG_DIR, value: {}}}]}}\n    model_selection: {{type: config-option}}\n    models: [{{id: opus, cost_rank: 3}}]\n",
                dir.path().join(name).display()
            )
        };
        let yaml = format!(
            "state_file: {}\nagents:\n{}{}",
            dir.path().join("state.db").display(),
            agent("claude", 0),
            agent("claude@added", 1),
        );
        std::fs::write(&path, &yaml).unwrap();
        let shared = Shared::new(Config::from_file(&path).unwrap()).unwrap();
        assert_eq!(account_group(&shared, "claude"), ["claude", "claude@added"]);
        set_priority(&shared, "claude@added", 0).await.unwrap();
        assert_eq!(account_group(&shared, "claude"), ["claude@added", "claude"]);
        let saved = Config::from_file(&path).unwrap();
        let priority = |name: &str| {
            saved
                .agents
                .iter()
                .find(|a| a.name == name)
                .unwrap()
                .account_priority
        };
        assert_eq!(priority("claude@added"), Some(1));
        assert_eq!(priority("claude"), Some(2));
    }

    #[test]
    fn reserve_edit_targets_standalone_and_listed_accounts() {
        let mut doc: Value = serde_yaml::from_str(
            "agents:\n  - name: claude\n  - name: claude@added\n  - name: codex\n    accounts: [{name: work}, {name: home}]\n",
        )
        .unwrap();
        let reserve = serde_json::json!({"weekly": 10.0, "session": 20.0});
        set_account_fields(
            &mut doc,
            "claude@added",
            &[("reserve_capacity", reserve.clone())],
            &[("reserve_capacity", reserve.clone())],
        )
        .unwrap();
        set_account_fields(
            &mut doc,
            "codex@home",
            &[("account_priority", serde_json::json!(0))],
            &[("priority", serde_json::json!(0))],
        )
        .unwrap();
        assert_eq!(doc["agents"][1]["reserve_capacity"], reserve);
        assert!(doc["agents"][0].get("reserve_capacity").is_none());
        assert_eq!(doc["agents"][2]["accounts"][1]["priority"], 0);
        assert!(doc["agents"][2].get("account_priority").is_none());
        assert!(set_account_fields(&mut doc, "codex@gone", &[], &[]).is_err());
    }

    #[tokio::test]
    async fn added_account_inherits_runtime_pins_scores_pricing_and_lineage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("router.yaml");
        let yaml = format!(
            r#"
state_file: {}
pinned_versions: {{claude/opus: claude-opus-4-6}}
agents:
  - name: claude
    lineage: anthropic
    command: {{type: stdio, command: mock, env: [{{name: HOME, value: {}}}]}}
    model_selection: {{type: config-option}}
    models:
      - id: opus
        cost_rank: 3
        versions: [{{api_model: claude-opus-4-6, pricing: {{input_per_mtok: 5, output_per_mtok: 25}}}}]
"#,
            dir.path().join("state.db").display(),
            dir.path().display()
        );
        std::fs::write(&path, yaml).unwrap();
        let shared = Shared::new(Config::from_file(&path).unwrap()).unwrap();
        let (agent, source) = new_account(&shared, "claude").unwrap();
        publish_added(&shared, &agent, &source, &CancellationToken::new())
            .await
            .unwrap();
        let candidate = CandidateId::new(&agent.name, "opus");
        let cfg = shared.runtime_config();
        assert_eq!(cfg.wire_api_model(&candidate), "claude-opus-4-6");
        assert!(
            cfg.declared_version(&candidate, "claude-opus-4-6")
                .is_some()
        );
        assert_eq!(
            shared
                .pricing_for("new", &candidate)
                .unwrap()
                .input_per_mtok,
            5.0
        );
        assert_eq!(
            shared.scores_for("new", &candidate).context_window,
            shared
                .scores
                .lookup_exact(&CandidateId::new(&agent.name, "claude-opus-4-6"))
                .context_window
        );
        assert_eq!(
            crate::session::agent_lineage(&cfg, &agent.name),
            "anthropic"
        );
        assert!(shared.candidate_runtime(&candidate).is_some());
        assert_eq!(
            Config::from_file(&path).unwrap().wire_api_model(&candidate),
            "claude-opus-4-6"
        );
    }
}
