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

use crate::candidate::CandidateId;
use crate::config::{AgentConfig, Config, EnvVarConfig, UsageSourceConfig};
use crate::session::{CandidateRuntime, CandidateStatus, Shared, TargetRuntime};
use crate::strategies::CandidateView;

const PROVIDERS: [&str; 3] = ["claude", "codex", "grok"];
/// Reserve percentages offered by `/login` → account → Set reserve capacity.
const RESERVE_STEPS: [u32; 10] = [0, 5, 10, 15, 20, 25, 30, 40, 50, 75];

pub fn advertise(shared: &Arc<Shared>, sid: &str) {
    if let Some(cx) = shared.upstream() {
        let commands = vec![
            AvailableCommand::new("login", "Manage provider accounts"),
            AvailableCommand::new("usage", "Show cached usage for every account"),
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
                Some("login" | "usage")
            )
        });
        commands.extend([
            serde_json::json!({"name":"login", "description":"Manage provider accounts"}),
            serde_json::json!({"name":"usage", "description":"Show cached usage for every account"}),
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
            .find(|p| agent.name.split('@').next() == Some(p)),
    }
}

fn directory(agent: &AgentConfig) -> Option<PathBuf> {
    match provider(agent)? {
        "claude" => agent.config_dir("CLAUDE_CONFIG_DIR", ".claude"),
        "codex" => agent.config_dir("CODEX_HOME", ".codex"),
        "grok" => agent
            .env_var("HOME")
            .map(|h| PathBuf::from(h).join(".grok")),
        _ => None,
    }
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
                    v.pointer("/claudeAiOauth/rateLimitTier")
                        .or_else(|| v.pointer("/claudeAiOauth/subscriptionType"))
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
        _ => {
            let auth = read_json(&dir.join("auth.json"));
            (
                auth.as_ref()
                    .and_then(|v| v.get("email"))
                    .and_then(Value::as_str)
                    .unwrap_or(&fallback)
                    .to_string(),
                None,
            )
        }
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

fn jwt_claims(token: &str) -> Option<Value> {
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
    Priority(String),
    Reserve(String),
    ReserveWindow(String, ReserveWindow),
    Delete(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReserveWindow {
    Weekly,
    Session,
}

impl ReserveWindow {
    fn label(self) -> &'static str {
        match self {
            ReserveWindow::Weekly => "weekly",
            ReserveWindow::Session => "session",
        }
    }
}

const RELOGIN: &str = "Re-login";
const SET_PRIORITY: &str = "Set priority";
const SET_RESERVE: &str = "Set reserve capacity";
const DELETE: &str = "Delete account";
const BACK: &str = "Back";

fn account_options(agent: &AgentConfig) -> Vec<String> {
    // Re-login and Delete keep their original numbers (1 and 2).
    let mut options = vec![
        RELOGIN.to_string(),
        DELETE.to_string(),
        SET_PRIORITY.to_string(),
    ];
    // Reserves cordon from usage readings, so they need a usage source.
    if agent.usage_source.is_some() {
        options.push(SET_RESERVE.to_string());
    }
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

fn without_turn_context(text: &str) -> String {
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
}

fn emit(shared: &Arc<Shared>, sid: &str, message: &str) {
    if let Some(cx) = shared.upstream() {
        let _ = cx.send_notification(SessionNotification::new(
            sid.to_string(),
            SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(
                message.to_string(),
            ))),
        ));
    }
}

fn menu(shared: &Arc<Shared>, state: &Menu) -> (String, Vec<String>) {
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
                crate::account_usage::summarize_account(shared, a),
                account_options(a),
            ),
            None => ("Account was removed".into(), vec![BACK.into()]),
        },
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
        Menu::Reserve(name) => {
            let agent = shared.agent_configs().into_iter().find(|a| &a.name == name);
            let label = agent.as_ref().map(account_label).unwrap_or(name.clone());
            let reserve = agent.map(|a| a.reserve_capacity).unwrap_or_default();
            (
                format!(
                    "Reserve capacity for {label}: the share of each included-plan window kept \
                     unused. The router moves to another account at {}% weekly or {}% session usage.",
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
        Menu::ReserveWindow(name, window) => {
            let agent = shared.agent_configs().into_iter().find(|a| &a.name == name);
            let label = agent.as_ref().map(account_label).unwrap_or(name.clone());
            let current = agent
                .map(|a| match window {
                    ReserveWindow::Weekly => a.reserve_capacity.weekly,
                    ReserveWindow::Session => a.reserve_capacity.session,
                })
                .unwrap_or_default();
            let mut options: Vec<String> = RESERVE_STEPS
                .iter()
                .map(|&p| {
                    if f64::from(p) == current {
                        format!("{p}% (current)")
                    } else {
                        format!("{p}%")
                    }
                })
                .collect();
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

async fn ask(
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
        let (body, options) = menu(shared, &state);
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
            Menu::Providers if (1..=PROVIDERS.len()).contains(&choice) => {
                state = provider_menu(shared, PROVIDERS[choice - 1])
            }
            Menu::Provider(p, names) => {
                if choice > 0 && choice <= names.len() {
                    state = Menu::Account(names[choice - 1].clone());
                } else if choice == names.len() + 1 {
                    let (agent, source) = new_account(shared, &p)?;
                    let flow = start_login(shared, sid, agent, Some(source))?;
                    shared.account_menus.lock().unwrap().remove(sid);
                    return finish_login(shared, sid, flow, structured).await;
                } else if choice == names.len() + 2 {
                    state = Menu::Providers;
                } else {
                    emit(shared, sid, "Choose a listed number.");
                }
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
                    Some(o) if o == SET_RESERVE => state = Menu::Reserve(name),
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
            Menu::Reserve(name) => match choice {
                1 => state = Menu::ReserveWindow(name, ReserveWindow::Weekly),
                2 => state = Menu::ReserveWindow(name, ReserveWindow::Session),
                3 => state = Menu::Account(name),
                _ => emit(shared, sid, "Choose a listed number."),
            },
            Menu::ReserveWindow(name, window) => {
                if choice > 0 && choice <= RESERVE_STEPS.len() {
                    let percent = RESERVE_STEPS[choice - 1];
                    match set_reserve(shared, &name, window, percent).await {
                        Ok(()) => emit(
                            shared,
                            sid,
                            &format!("{} reserve set to {percent}%.", window.label()),
                        ),
                        Err(e) => emit(shared, sid, &format!("router-acp: {e}")),
                    }
                    state = Menu::Reserve(name);
                } else if choice == RESERVE_STEPS.len() + 1 {
                    state = Menu::Reserve(name);
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
    let variable = match p {
        "claude" => "CLAUDE_CONFIG_DIR",
        "codex" => "CODEX_HOME",
        _ => "HOME",
    };
    agent
        .command
        .env
        .retain(|e| e.name != variable && !AUTH_ENV.contains(&e.name.as_str()));
    agent.command.env.push(EnvVarConfig {
        name: variable.into(),
        value: dir.to_string_lossy().into_owned(),
    });
    Ok((agent, source))
}

pub(crate) const AUTH_ENV: [&str; 5] = [
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "OPENAI_API_KEY",
    "XAI_API_KEY",
];

pub(crate) fn isolated_environment(env: &[(String, String)]) -> bool {
    env.iter()
        .any(|(name, _)| name == "CLAUDE_CONFIG_DIR" || name == "CODEX_HOME")
}

fn start_login(
    shared: &Arc<Shared>,
    sid: &str,
    agent: AgentConfig,
    source: Option<String>,
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
            Ok(()) if adding => {
                publish_added(&shared, &agent, source.as_deref().unwrap(), &runner.cancel).await
            }
            other => other,
        };
        shared.account_login.lock().unwrap().remove(&agent.name);
        match result {
            Ok(()) => {
                crate::auth::note_authenticated(&shared.auth, &agent.name);
                for key in shared.target_keys_for_agent(&agent.name) {
                    if crate::downstream::start_downstream(&shared, &key)
                        .await
                        .is_ok()
                    {
                        crate::downstream::probe_target(&shared, &key).await;
                    }
                }
                if !adding {
                    queue_relogin_handoffs(&shared, &agent.name);
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
                    queue_relogin_handoffs(&shared, &agent.name);
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
    if flow.cancel.is_cancelled() {
        return Err("Login cancelled. Existing accounts remain registered.".into());
    }
    let p = provider(agent).unwrap();
    let default_args: &[&str] = if p == "claude" {
        &["auth", "login", "--claudeai"]
    } else {
        &["login", "--device-auth"]
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
            }
            "codex" => {
                cmd.env("CODEX_HOME", dir);
            }
            _ => {
                cmd.env("HOME", dir.parent().unwrap());
            }
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
                Ok(s) if s.success() => Ok(()),
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
    // Do not publish a URL while its final bytes are still arriving.
    let matched = regex::Regex::new(r#"https://[^\s\x1b\"'<>]+"#)
        .ok()?
        .find(output)?;
    if matched.end() == output.len() {
        return None;
    }
    let url = matched.as_str().to_string();
    let code = if p == "claude" {
        None
    } else {
        regex::Regex::new(r"\b[A-Z0-9]{4,10}(?:-[A-Z0-9]{4,10})+\b")
            .ok()?
            .find(output)
            .filter(|m| {
                m.end() < output.len() && output[m.end()..].starts_with(char::is_whitespace)
            })
            .map(|m| m.as_str().to_string())
    };
    if p != "claude" && code.is_none() {
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
    let _guard = shared.account_write.lock().await;
    let path = shared
        .cfg
        .source_path
        .as_ref()
        .ok_or("Account management needs a file-backed router configuration")?;
    let lock_path = path.with_extension("accounts.lock");
    let _file_lock = ConfigLock::acquire(lock_path)?;
    // Edit the uninterpolated document, so env secrets never become file values.
    let raw = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let mut document: Value = serde_yaml::from_str(&raw).map_err(|e| e.to_string())?;
    edit(&mut document)?;
    let yaml = serde_yaml::to_string(&document).map_err(|e| e.to_string())?;
    let mut cfg = Config::from_yaml(&yaml).map_err(|e| e.to_string())?;
    cfg.source_path = Some(path.clone());
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
        file.write_all(yaml.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_file(tmp);
        return Err(e.to_string());
    }
    Ok(cfg)
}

struct ConfigLock(std::fs::File);
impl ConfigLock {
    fn acquire(path: PathBuf) -> Result<Self, String> {
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
        Ok(Self(file))
    }
}
impl Drop for ConfigLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

async fn publish_added(
    shared: &Arc<Shared>,
    agent: &AgentConfig,
    source: &str,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let cfg = write_config(shared, |doc| {
        if cancel.is_cancelled() {
            return Err("Login cancelled. Existing accounts remain registered.".into());
        }
        let agents = doc
            .get_mut("agents")
            .and_then(Value::as_array_mut)
            .ok_or("Missing agents configuration")?;
        let template = agents
            .iter()
            .find(|a| a.get("name").and_then(Value::as_str) == Some(source))
            .or_else(|| {
                agents
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
        added["account_priority"] = serde_json::json!(agent.account_priority);
        added["lineage"] = serde_json::json!(agent.lineage);
        added["reserve_capacity"] = serde_json::json!(agent.reserve_capacity);
        let variable = match provider(agent) {
            Some("claude") => "CLAUDE_CONFIG_DIR",
            Some("codex") => "CODEX_HOME",
            _ => "HOME",
        };
        let env = added["command"]
            .as_object_mut()
            .ok_or("Missing command")?
            .entry("env")
            .or_insert_with(|| serde_json::json!([]))
            .as_array_mut()
            .ok_or("Invalid command environment")?;
        env.extend(account_env);
        env.retain(|v| {
            v.get("name")
                .and_then(Value::as_str)
                .is_none_or(|n| n != variable && !AUTH_ENV.contains(&n))
        });
        env.push(serde_json::json!({"name": variable, "value": agent.env_var(variable)}));
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
fn set_account_fields(
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

/// Set one reserve window, keeping the account's other window as it is.
async fn set_reserve(
    shared: &Arc<Shared>,
    name: &str,
    window: ReserveWindow,
    percent: u32,
) -> Result<(), String> {
    let mut reserve = shared
        .agent_configs()
        .into_iter()
        .find(|a| a.name == name)
        .ok_or("Account no longer exists")?
        .reserve_capacity;
    match window {
        ReserveWindow::Weekly => reserve.weekly = f64::from(percent),
        ReserveWindow::Session => reserve.session = f64::from(percent),
    }
    let value = serde_json::json!(reserve);
    let cfg = write_config(shared, |doc| {
        set_account_fields(
            doc,
            name,
            &[("reserve_capacity", value.clone())],
            &[("reserve_capacity", value)],
        )
    })
    .await?;
    *shared.account_config.lock().unwrap() = cfg;
    // Re-read usage now, so the new cordon threshold applies before the next poll.
    crate::usage::refresh_after_turn(shared, name);
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
        match std::fs::remove_file(dir.join("auth.json")) {
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
            plan_label("claude", "default_claude_max_20x"),
            "Personal 20x Max"
        );
    }

    #[test]
    fn native_commands_survive_downstream_command_updates() {
        let frame = agent_client_protocol::UntypedMessage::new("session/update", serde_json::json!({"sessionId":"s", "update":{"sessionUpdate":"available_commands_update","availableCommands":[{"name":"help","description":"Provider help"},{"name":"login","description":"Provider login"}]}})).unwrap();
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
            ["help", "login", "usage"]
        );
        assert_eq!(commands[1]["description"], "Manage provider accounts");
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
