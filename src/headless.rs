//! `router-acp prompt`: send one message to a router session with no ACP
//! client attached.
//!
//! A host supervisor uses it to wake a parent session whose interactive
//! client (goose) is gone. The router runs in-process and this module plays
//! the client: it reloads the session — so the parent keeps its provider
//! session, its transcript and the router's delegate tools — or opens a new
//! one, applies a session mode, sends the message, and streams the agent's
//! text to stdout. Permission requests are answered with the first "allow"
//! option, the same non-interactive behavior a delegate gets. No `fs` or
//! terminal capability is advertised, so adapters do their own file and shell
//! work.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeRequest, LoadSessionRequest, NewSessionRequest, PermissionOptionKind,
    PromptRequest, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    ResumeSessionRequest, SelectedPermissionOutcome, SessionNotification, SessionUpdate,
    SetSessionModeRequest, StopReason,
};
use agent_client_protocol::{
    Channel, Client as ClientPeer, Responder, on_receive_notification, on_receive_request,
};

use crate::config::Config;
use crate::session::{Shared, serve_shared};

pub struct PromptOptions {
    /// A router session id (`rtr-…`) or the provider session id one of the
    /// router's sessions is pinned to. `None` opens a new session.
    pub session: Option<String>,
    pub cwd: PathBuf,
    pub message: String,
    /// Client session mode to apply first (e.g. `auto`), mapped through
    /// `agents[].mode_map` like goose's.
    pub mode: Option<String>,
}

/// Resolve `--session`: a router session id, or the downstream (provider)
/// session id of a primary router session — what a provider's own hooks
/// recorded for the parent.
pub fn resolve_session(shared: &Shared, id: &str) -> Option<String> {
    let state = shared.state.lock().unwrap();
    if state.get(id).is_some() {
        return Some(id.to_string());
    }
    state
        .all()
        .into_iter()
        .filter(|(_, row)| row.parent_session_id.is_none() && row.downstream_session_id == id)
        .map(|(router_sid, _)| router_sid)
        .next()
}

/// Run one prompt. Returns the turn's stop reason; agent text goes to stdout
/// and progress to stderr.
pub async fn run(cfg: Config, opts: PromptOptions) -> Result<StopReason, String> {
    let shared = Shared::new(cfg).map_err(|e| format!("router setup failed: {e}"))?;
    let resumed = match &opts.session {
        Some(id) => Some(
            resolve_session(&shared, id)
                .ok_or_else(|| format!("no router session matches `{id}` in the state DB"))?,
        ),
        None => None,
    };
    let (router_side, client_side) = Channel::duplex();
    let router = tokio::spawn(serve_shared(shared, router_side));

    // Replayed history from session/load is not part of this turn's output.
    let printing = Arc::new(AtomicBool::new(false));
    let print_updates = printing.clone();
    let result = ClientPeer
        .builder()
        .name("router-acp-prompt")
        .on_receive_notification(
            move |n: SessionNotification, _cx| {
                let printing = print_updates.clone();
                async move {
                    if printing.load(Ordering::Relaxed)
                        && let SessionUpdate::AgentMessageChunk(chunk) = &n.update
                        && let ContentBlock::Text(text) = &chunk.content
                    {
                        use std::io::Write;
                        let mut out = std::io::stdout();
                        let _ = out.write_all(text.text.as_bytes());
                        let _ = out.flush();
                    }
                    Ok(())
                }
            },
            on_receive_notification!(),
        )
        .on_receive_request(
            |req: RequestPermissionRequest,
             responder: Responder<RequestPermissionResponse>,
             _cx| async move {
                let allow = req
                    .options
                    .iter()
                    .find(|o| {
                        matches!(
                            o.kind,
                            PermissionOptionKind::AllowAlways | PermissionOptionKind::AllowOnce
                        )
                    })
                    .or_else(|| req.options.first());
                responder.respond(RequestPermissionResponse::new(match allow {
                    Some(option) => RequestPermissionOutcome::Selected(
                        SelectedPermissionOutcome::new(option.option_id.clone()),
                    ),
                    None => RequestPermissionOutcome::Cancelled,
                }))
            },
            on_receive_request!(),
        )
        .connect_with(client_side, async |cx| {
            cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let sid = match &resumed {
                Some(sid) => {
                    // Resume when the adapter supports it (no replay), else load.
                    let resumed = cx
                        .send_request(ResumeSessionRequest::new(sid.clone(), opts.cwd.clone()))
                        .block_task()
                        .await;
                    if resumed.is_err() {
                        cx.send_request(LoadSessionRequest::new(sid.clone(), opts.cwd.clone()))
                            .block_task()
                            .await?;
                    }
                    sid.clone()
                }
                None => cx
                    .send_request(NewSessionRequest::new(opts.cwd.clone()))
                    .block_task()
                    .await?
                    .session_id
                    .0
                    .to_string(),
            };
            eprintln!("router-acp session: {sid}");
            if let Some(mode) = &opts.mode
                && let Err(err) = cx
                    .send_request(SetSessionModeRequest::new(sid.clone(), mode.clone()))
                    .block_task()
                    .await
            {
                eprintln!("router-acp: mode `{mode}` not applied: {err}");
            }
            printing.store(true, Ordering::Relaxed);
            let response = cx
                .send_request(PromptRequest::new(
                    sid,
                    vec![ContentBlock::from(opts.message.clone())],
                ))
                .block_task()
                .await?;
            Ok(response.stop_reason)
        })
        .await;
    router.abort();
    println!();
    result.map_err(|e| e.to_string())
}
