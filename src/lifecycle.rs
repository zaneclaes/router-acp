//! Router-owned session lifecycle, backed by SQLite rather than adapter files.

use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    CloseSessionRequest, CloseSessionResponse, ContentBlock, ContentChunk, DeleteSessionRequest,
    DeleteSessionResponse, Error as AcpError, LoadSessionRequest, LoadSessionResponse, McpServer,
    Meta, ResumeSessionRequest, ResumeSessionResponse, SessionNotification, SessionUpdate,
};
use agent_client_protocol::{Client as ClientPeer, ConnectionTo, Responder};

use crate::session::{
    RouterSession, Shared, close_downstream_session, meta_marks_coordinator, sid_str,
};
use crate::state::PersistedSession;

fn lookup_persisted(shared: &Arc<Shared>, router_sid: &str) -> Result<PersistedSession, AcpError> {
    shared.state.lock().unwrap().get(router_sid).ok_or_else(|| {
        AcpError::invalid_params().data(format!("unknown router session id `{router_sid}`"))
    })
}

/// Restore logical state now. Pin a fresh adapter lazily on the next prompt,
/// after the client can change its model and before native auth repair runs.
fn restore(
    shared: &Arc<Shared>,
    sid: &str,
    cwd: std::path::PathBuf,
    mcp_servers: Vec<McpServer>,
    meta: Option<&Meta>,
) -> Result<(), AcpError> {
    let persisted = lookup_persisted(shared, sid)?;
    let mut session = RouterSession::rehydrated(&shared.cfg, &persisted, mcp_servers.clone());
    crate::restoration::restore_config(&mut session, &persisted)?;
    session.pending_history = crate::restoration::lookup_context(shared, sid);
    session.cwd = cwd.clone();
    session.coordinator |= meta_marks_coordinator(meta);
    {
        let mut sessions = shared.sessions.lock().unwrap();
        if let Some(live) = sessions.get_mut(sid) {
            if live.pinning {
                return Err(AcpError::invalid_request()
                    .data("session is already restoring or serving a prompt"));
            }
            // Reattachment must not discard a live adapter's conversation.
            live.cwd = cwd;
            live.mcp_servers = mcp_servers;
            live.coordinator |= session.coordinator;
        } else {
            sessions.insert(sid.to_string(), session);
        }
    }
    crate::restoration::checkpoint(shared, sid)
}

pub fn on_session_resume(
    shared: Arc<Shared>,
    req: ResumeSessionRequest,
    responder: Responder<ResumeSessionResponse>,
    _cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    let sid = sid_str(&req.session_id);
    match restore(&shared, &sid, req.cwd, req.mcp_servers, req.meta.as_ref()) {
        Ok(()) => responder.respond(
            ResumeSessionResponse::new()
                .config_options(shared.router_config_options(&sid))
                .meta(crate::restoration::response_meta(&shared, &sid)),
        ),
        Err(err) => responder.respond_with_error(err),
    }
}

pub fn on_session_load(
    shared: Arc<Shared>,
    req: LoadSessionRequest,
    responder: Responder<LoadSessionResponse>,
    cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    let sid = sid_str(&req.session_id);
    if let Err(err) = restore(&shared, &sid, req.cwd, req.mcp_servers, req.meta.as_ref()) {
        return responder.respond_with_error(err);
    }
    let entries = match shared.state.lock().unwrap().log_for_all(&sid) {
        Ok(entries) => entries,
        Err(err) => {
            return responder.respond_with_error(AcpError::internal_error().data(err.to_string()));
        }
    };
    let mut raw_response = false;
    let mut last_raw_tool = None;
    for entry in entries {
        if entry.kind == "router_notice" {
            let mut notif = SessionNotification::new(
                sid.clone(),
                SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(
                    entry.summary,
                ))),
            );
            if let Some(notices) = entry
                .detail
                .as_ref()
                .and_then(|d| d.get("notices"))
                .filter(|n| n.as_array().is_some_and(|a| !a.is_empty()))
            {
                let mut meta = serde_json::Map::new();
                meta.insert(
                    "router_acp".into(),
                    serde_json::json!({ "notices": notices }),
                );
                notif = notif.meta(meta);
            }
            cx.send_notification(notif)?;
            continue;
        }
        let updates = match entry.kind.as_str() {
            "user_prompt" | "user_steer" => {
                if entry.kind == "user_prompt" {
                    raw_response = false;
                }
                let blocks: Vec<ContentBlock> = entry
                    .detail
                    .as_ref()
                    .and_then(|d| d.get("prompt"))
                    .and_then(|p| serde_json::from_value(p.clone()).ok())
                    .unwrap_or_else(|| vec![ContentBlock::from(entry.summary)]);
                blocks
                    .into_iter()
                    .map(|b| SessionUpdate::UserMessageChunk(ContentChunk::new(b)))
                    .collect()
            }
            "session_update" => {
                let update = entry
                    .detail
                    .as_ref()
                    .and_then(|v| serde_json::from_value::<SessionUpdate>(v.clone()).ok());
                if matches!(update, Some(SessionUpdate::AgentMessageChunk(_))) {
                    raw_response = true;
                }
                if matches!(
                    update,
                    Some(SessionUpdate::ToolCall(_)) | Some(SessionUpdate::ToolCallUpdate(_))
                ) {
                    last_raw_tool = entry.detail.clone();
                }
                // The client's prompt is already replayed from user_prompt.
                update
                    .filter(|u| !matches!(u, SessionUpdate::UserMessageChunk(_)))
                    .into_iter()
                    .collect()
            }
            "agent_response" if !raw_response => vec![SessionUpdate::AgentMessageChunk(
                ContentChunk::new(entry.summary.into()),
            )],
            "tool_call" if entry.detail != last_raw_tool => entry
                .detail
                .and_then(|v| serde_json::from_value(v).ok())
                .into_iter()
                .collect(),
            _ => Vec::new(),
        };
        for update in updates {
            cx.send_notification(SessionNotification::new(sid.clone(), update))?;
        }
    }
    responder.respond(
        LoadSessionResponse::new()
            .config_options(shared.router_config_options(&sid))
            .meta(crate::restoration::response_meta(&shared, &sid)),
    )
}

pub fn on_session_delete(
    shared: Arc<Shared>,
    req: DeleteSessionRequest,
    responder: Responder<DeleteSessionResponse>,
    _cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    let sid = sid_str(&req.session_id);
    if let Err(err) = lookup_persisted(&shared, &sid) {
        return responder.respond_with_error(err);
    }
    close(&shared, &sid);
    shared.state.lock().unwrap().remove(&sid);
    responder.respond(DeleteSessionResponse::new())
}

fn close(shared: &Arc<Shared>, sid: &str) {
    // The conversation and checkpoint survive close. Provider-local storage
    // is never needed to reopen it.
    let session = shared.sessions.lock().unwrap().remove(sid);
    crate::accounts::cancel_login(shared, sid);
    crate::session::close_live_delegates_for(shared, sid);
    if let Some(pin) = session.and_then(|s| s.pin) {
        close_downstream_session(shared, &pin.process_key, &pin.downstream_sid);
    }
}

pub fn on_session_close(
    shared: Arc<Shared>,
    req: CloseSessionRequest,
    responder: Responder<CloseSessionResponse>,
) -> Result<(), AcpError> {
    let sid = sid_str(&req.session_id);
    if let Err(err) = lookup_persisted(&shared, &sid) {
        return responder.respond_with_error(err);
    }
    if shared.with_session(&sid, |_| ()).is_some()
        && let Err(err) = crate::restoration::checkpoint(&shared, &sid)
    {
        return responder.respond_with_error(err);
    }
    close(&shared, &sid);
    responder.respond(CloseSessionResponse::new())
}
