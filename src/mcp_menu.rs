//! `/mcp`: manage the router's project MCP sign-ins from any ACP client, the
//! way `/login` manages provider accounts. Lists the session's `.mcp.json`
//! servers with their state and signs one in or out.
//!
//! Sign-in uses the router's OAuth flow with a loopback redirect. A browser on
//! this machine completes it on its own. A browser elsewhere lands on a page
//! that cannot load: the user pastes that address with `/mcp callback <url>`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::Error as AcpError;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::accounts::{ask, emit};
use crate::mcp_oauth::{RemoteServer, StatusState};
use crate::session::Shared;

/// How long a started sign-in waits for its browser redirect.
const SIGN_IN_TTL: Duration = Duration::from_secs(10 * 60);
const SIGN_IN: &str = "Sign in";
const SIGN_IN_AGAIN: &str = "Sign in again";
const SIGN_OUT: &str = "Sign out";
const BACK: &str = "Back";
const CLOSE: &str = "Close";

#[derive(Clone, Debug)]
pub enum McpMenu {
    Servers,
    Server(String),
}

/// A started sign-in for one session: the server and its loopback listener.
pub struct PendingMcpLogin {
    id: String,
    server: RemoteServer,
    cancel: CancellationToken,
}

impl PendingMcpLogin {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

/// Whether `text` is an `/mcp` command, or a numbered reply to an open menu.
pub fn intercepts(shared: &Arc<Shared>, sid: &str, text: &str) -> bool {
    if text == "/mcp" || text.starts_with("/mcp ") {
        return true;
    }
    let mut menus = shared.mcp_menus.lock().unwrap();
    if menus.contains_key(sid) {
        if text.parse::<usize>().is_ok() || text == "/back" || text == "/cancel" {
            return true;
        }
        menus.remove(sid);
    }
    false
}

pub fn cancel(shared: &Arc<Shared>, sid: &str) {
    shared.mcp_menus.lock().unwrap().remove(sid);
    if let Some(pending) = shared.mcp_logins.lock().unwrap().remove(sid) {
        pending.cancel.cancel();
    }
}

fn session_dir(shared: &Arc<Shared>, sid: &str) -> PathBuf {
    shared
        .with_session(sid, |s| s.cwd.clone())
        .filter(|cwd| !cwd.as_os_str().is_empty())
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default())
}

/// `mcp-status` rows for the session's project, credential helper included.
async fn rows(shared: &Arc<Shared>, sid: &str) -> (Option<String>, Vec<Value>) {
    let status = crate::mcp_oauth::project_status_json(
        &session_dir(shared, sid),
        &crate::mcp_oauth::store_root(),
        shared.cfg.mcp_credential_command.as_ref(),
    )
    .await;
    let file = status["project"].as_str().map(String::from);
    let servers = status["servers"].as_array().cloned().unwrap_or_default();
    (file, servers)
}

fn row_label(row: &Value) -> String {
    let name = row["name"].as_str().unwrap_or("?");
    match row["label"].as_str() {
        Some(label) => format!("{name}: {label}"),
        None => format!(
            "{name}: {}",
            row["handling"].as_str().unwrap_or("not brokered")
        ),
    }
}

fn remote(shared: &Arc<Shared>, sid: &str, name: &str) -> Option<RemoteServer> {
    crate::mcp_oauth::project_servers(&session_dir(shared, sid))
        .into_iter()
        .find(|s| s.name == name)
        .and_then(|s| s.remote())
}

/// The router's own sign-in exists (a helper-served row reports signed-in too).
fn router_signed_in(server: &RemoteServer) -> bool {
    crate::mcp_oauth::status(&server.store(&crate::mcp_oauth::store_root())).state
        != StatusState::SignedOut
}

async fn menu(shared: &Arc<Shared>, sid: &str, state: &McpMenu) -> (String, Vec<String>) {
    let (file, servers) = rows(shared, sid).await;
    match state {
        McpMenu::Servers => {
            let mut options: Vec<String> = servers.iter().map(row_label).collect();
            options.push(CLOSE.into());
            let title = match file {
                Some(file) => format!("MCP servers in {file}"),
                None => "No .mcp.json from this session's directory up to its git root".into(),
            };
            (title, options)
        }
        McpMenu::Server(name) => {
            let Some(row) = servers.iter().find(|r| r["name"] == name.as_str()) else {
                return (
                    format!("{name} is no longer in .mcp.json"),
                    vec![BACK.into()],
                );
            };
            let mut body = format!(
                "{}\nURL: {}",
                row_label(row),
                row["url"].as_str().unwrap_or("none")
            );
            let options = match remote(shared, sid, name) {
                Some(server) => {
                    let mine = router_signed_in(&server);
                    let mut options = vec![if mine { SIGN_IN_AGAIN } else { SIGN_IN }.to_string()];
                    if mine {
                        options.push(SIGN_OUT.into());
                    }
                    options.push(BACK.into());
                    options
                }
                None => {
                    body.push_str(
                        "\nThe router does not sign in to this entry; agents use it as configured.",
                    );
                    vec![BACK.into()]
                }
            };
            (body, options)
        }
    }
}

pub async fn handle(
    shared: &Arc<Shared>,
    sid: &str,
    command: &str,
    structured: bool,
) -> Result<(), AcpError> {
    if command == "/cancel" || command == "/mcp cancel" {
        cancel(shared, sid);
        emit(shared, sid, "MCP management closed.");
        return Ok(());
    }
    if let Some(url) = command.strip_prefix("/mcp callback ") {
        return finish_pasted(shared, sid, url.trim()).await;
    }
    let mut state = if command == "/mcp" || command == "/back" {
        McpMenu::Servers
    } else {
        shared
            .mcp_menus
            .lock()
            .unwrap()
            .get(sid)
            .cloned()
            .unwrap_or(McpMenu::Servers)
    };
    let mut typed_choice = command.parse::<usize>().ok();
    loop {
        let (body, options) = menu(shared, sid, &state).await;
        let choice = match typed_choice.take() {
            Some(c) => c,
            None if structured => {
                let Some(value) = ask(shared, sid, &body, &options).await? else {
                    shared.mcp_menus.lock().unwrap().remove(sid);
                    emit(shared, sid, "MCP management closed.");
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
                    .mcp_menus
                    .lock()
                    .unwrap()
                    .insert(sid.to_string(), state);
                return Ok(());
            }
        };
        let picked = choice.checked_sub(1).and_then(|i| options.get(i)).cloned();
        match (state.clone(), picked.as_deref()) {
            (_, None) => emit(shared, sid, "Choose a listed number."),
            (McpMenu::Servers, Some(CLOSE)) => {
                shared.mcp_menus.lock().unwrap().remove(sid);
                return Ok(());
            }
            (McpMenu::Servers, Some(_)) => {
                let (_, servers) = rows(shared, sid).await;
                match servers.get(choice - 1).and_then(|r| r["name"].as_str()) {
                    Some(name) => state = McpMenu::Server(name.to_string()),
                    None => emit(shared, sid, "Choose a listed number."),
                }
            }
            (McpMenu::Server(_), Some(BACK)) => state = McpMenu::Servers,
            (McpMenu::Server(name), Some(SIGN_OUT)) => {
                if let Some(server) = remote(shared, sid, &name) {
                    let message = crate::mcp_oauth::logout(&server)
                        .unwrap_or_else(|why| format!("Sign-out failed: {why}"));
                    emit(shared, sid, &message);
                }
            }
            (McpMenu::Server(name), Some(_)) => {
                let server = remote(shared, sid, &name).ok_or_else(|| {
                    AcpError::invalid_params().data("Server is no longer brokered")
                })?;
                shared.mcp_menus.lock().unwrap().remove(sid);
                return start_sign_in(shared, sid, server).await;
            }
        }
    }
}

/// Start the OAuth flow with a loopback redirect and show its URL. The
/// listener finishes it in the background; `/mcp callback` finishes it by hand.
async fn start_sign_in(
    shared: &Arc<Shared>,
    sid: &str,
    server: RemoteServer,
) -> Result<(), AcpError> {
    let fail = |why: String| AcpError::internal_error().data(why);
    let (redirect, received) = crate::mcp_oauth::loopback_redirect(server.callback_port)
        .await
        .map_err(fail)?;
    let start = crate::mcp_oauth::login_start(&server, &redirect)
        .await
        .map_err(fail)?;
    let url = start["url"].as_str().unwrap_or_default().to_string();
    let cancel = CancellationToken::new();
    let id = uuid::Uuid::new_v4().to_string();
    if let Some(previous) = shared.mcp_logins.lock().unwrap().insert(
        sid.to_string(),
        PendingMcpLogin {
            id: id.clone(),
            server: server.clone(),
            cancel: cancel.clone(),
        },
    ) {
        previous.cancel.cancel();
    }
    emit(
        shared,
        sid,
        &format!(
            "Sign in to `{}` in your browser:\n{url}\n\nIf the browser runs on another machine, the \
             page it lands on will not load. Copy that page's full address and send \
             /mcp callback <address>. /mcp cancel stops this sign-in.",
            server.name
        ),
    );
    let (shared, sid) = (shared.clone(), sid.to_string());
    tokio::spawn(async move {
        let callback = tokio::select! {
            () = cancel.cancelled() => return,
            got = tokio::time::timeout(SIGN_IN_TTL, received) => got,
        };
        let Ok(Ok(callback)) = callback else {
            return;
        };
        let still_ours = shared
            .mcp_logins
            .lock()
            .unwrap()
            .get(&sid)
            .is_some_and(|p| p.id == id);
        if still_ours {
            shared.mcp_logins.lock().unwrap().remove(&sid);
            emit(&shared, &sid, &finish(&server, &callback).await);
        }
    });
    Ok(())
}

async fn finish_pasted(shared: &Arc<Shared>, sid: &str, url: &str) -> Result<(), AcpError> {
    let pending = shared.mcp_logins.lock().unwrap().remove(sid);
    let Some(pending) = pending else {
        return Err(
            AcpError::invalid_params().data("No MCP sign-in is waiting. Start one with /mcp.")
        );
    };
    pending.cancel.cancel();
    emit(shared, sid, &finish(&pending.server, url).await);
    Ok(())
}

async fn finish(server: &RemoteServer, callback_url: &str) -> String {
    match crate::mcp_oauth::login_finish(server, callback_url).await {
        Ok(status) => format!(
            "Signed in to `{}` ({}). Every agent's sessions now share this sign-in.",
            server.name,
            status["label"].as_str().unwrap_or("signed in")
        ),
        Err(why) => format!("Sign-in to `{}` failed: {why}", server.name),
    }
}

/// `/mcp` state per session, beside `/login`'s.
pub type McpMenus = std::sync::Mutex<HashMap<String, McpMenu>>;
pub type McpLogins = std::sync::Mutex<HashMap<String, PendingMcpLogin>>;
