//! Session lifecycle methods: `session/load`, `session/resume`,
//! `session/delete`, `session/close`.
//!
//! All of these require a known router session id in the state file, route
//! only to the owning downstream, and remap ids before/after forwarding.

use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    CloseSessionRequest, CloseSessionResponse, DeleteSessionRequest, DeleteSessionResponse,
    Error as AcpError, LoadSessionRequest, LoadSessionResponse, ResumeSessionRequest,
    ResumeSessionResponse,
};
use agent_client_protocol::{Agent as AgentPeer, Client as ClientPeer, ConnectionTo, Responder};

use crate::candidate::CandidateId;
use crate::downstream::ProcessKey;
use crate::session::{
    DownstreamRoute, PinInfo, RouterSession, Shared, close_downstream_session, sid_str,
};
use crate::state::PersistedSession;

/// Resolve the owning downstream for a persisted session: the target, its
/// connection, and whether it advertises the given capability.
fn owning_target(
    shared: &Arc<Shared>,
    persisted: &PersistedSession,
    cap: impl Fn(&agent_client_protocol::schema::v1::AgentCapabilities) -> bool,
    cap_name: &str,
) -> Result<(ProcessKey, ConnectionTo<AgentPeer>), AcpError> {
    let candidate = CandidateId::new(&persisted.agent, &persisted.model);
    let runtime = shared.candidate_runtime(&candidate).ok_or_else(|| {
        AcpError::invalid_params().data(format!(
            "session belongs to `{candidate}` which is no longer configured"
        ))
    })?;
    let key = runtime.process_key;
    let init = shared.target_init(&key).ok_or_else(|| {
        AcpError::internal_error().data(format!(
            "downstream `{}` is not initialized; authenticate or check its process",
            persisted.agent
        ))
    })?;
    if !cap(&init.agent_capabilities) {
        return Err(AcpError::method_not_found().data(format!(
            "downstream agent `{}` does not support {cap_name}",
            persisted.agent
        )));
    }
    let conn = shared.target_conn(&key).ok_or_else(|| {
        AcpError::internal_error().data(format!(
            "downstream process for `{}` is not running",
            persisted.agent
        ))
    })?;
    Ok((key, conn))
}

fn lookup_persisted(shared: &Arc<Shared>, router_sid: &str) -> Result<PersistedSession, AcpError> {
    shared.state.lock().unwrap().get(router_sid).ok_or_else(|| {
        AcpError::invalid_params().data(format!("unknown router session id `{router_sid}`"))
    })
}

/// Rehydrate the in-memory session record and pin before any prompt.
fn rehydrate(
    shared: &Arc<Shared>,
    router_sid: &str,
    persisted: &PersistedSession,
    key: &ProcessKey,
    mcp_servers: Vec<agent_client_protocol::schema::v1::McpServer>,
) {
    let candidate = CandidateId::new(&persisted.agent, &persisted.model);
    let mut sessions = shared.sessions.lock().unwrap();
    let session = sessions
        .entry(router_sid.to_string())
        .or_insert_with(|| RouterSession::rehydrated(&shared.cfg, persisted, mcp_servers.clone()));
    session.pin = Some(PinInfo {
        candidate,
        process_key: key.clone(),
        downstream_sid: persisted.downstream_session_id.clone(),
        // Loaded/resumed sessions rediscover modes lazily; exact-id mode
        // relays still work because unknown ids fall back leniently.
        available_modes: Vec::new(),
    });
    session.pinning = false;
}

pub fn on_session_load(
    shared: Arc<Shared>,
    req: LoadSessionRequest,
    responder: Responder<LoadSessionResponse>,
    cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    let router_sid = sid_str(&req.session_id);
    let persisted = match lookup_persisted(&shared, &router_sid) {
        Ok(p) => p,
        Err(e) => return responder.respond_with_error(e),
    };
    let (key, conn) = match owning_target(
        &shared,
        &persisted,
        |caps| caps.load_session,
        "session/load",
    ) {
        Ok(v) => v,
        Err(e) => return responder.respond_with_error(e),
    };

    // Register the route BEFORE forwarding so replayed session/update
    // notifications relay to the client under the router id.
    shared.register_route(
        &key,
        &persisted.downstream_session_id,
        DownstreamRoute::Primary {
            router_sid: router_sid.clone(),
        },
    );
    rehydrate(
        &shared,
        &router_sid,
        &persisted,
        &key,
        req.mcp_servers.clone(),
    );

    let fwd = LoadSessionRequest::new(persisted.downstream_session_id.clone(), req.cwd.clone())
        .mcp_servers(req.mcp_servers.clone())
        .meta(req.meta.clone());

    cx.spawn(async move {
        match conn.send_request(fwd).block_task().await {
            Ok(resp) => {
                if let Some(modes) = &resp.modes {
                    let ids: Vec<String> = modes
                        .available_modes
                        .iter()
                        .map(|m| m.id.0.to_string())
                        .collect();
                    shared.with_session(&router_sid, |s| {
                        if let Some(pin) = &mut s.pin {
                            pin.available_modes = ids;
                        }
                    });
                }
                let _ = responder.respond(resp);
            }
            Err(err) => {
                shared.unregister_route(&key, &persisted.downstream_session_id);
                shared.with_session(&router_sid, |s| s.pin = None);
                let _ = responder.respond_with_error(err);
            }
        }
        Ok(())
    })
}

pub fn on_session_resume(
    shared: Arc<Shared>,
    req: ResumeSessionRequest,
    responder: Responder<ResumeSessionResponse>,
    cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    let router_sid = sid_str(&req.session_id);
    let persisted = match lookup_persisted(&shared, &router_sid) {
        Ok(p) => p,
        Err(e) => return responder.respond_with_error(e),
    };
    let (key, conn) = match owning_target(
        &shared,
        &persisted,
        |caps| caps.session_capabilities.resume.is_some(),
        "session/resume",
    ) {
        Ok(v) => v,
        Err(e) => return responder.respond_with_error(e),
    };

    shared.register_route(
        &key,
        &persisted.downstream_session_id,
        DownstreamRoute::Primary {
            router_sid: router_sid.clone(),
        },
    );
    rehydrate(
        &shared,
        &router_sid,
        &persisted,
        &key,
        req.mcp_servers.clone(),
    );

    let fwd = ResumeSessionRequest::new(persisted.downstream_session_id.clone(), req.cwd.clone())
        .mcp_servers(req.mcp_servers.clone())
        .meta(req.meta.clone());

    cx.spawn(async move {
        match conn.send_request(fwd).block_task().await {
            Ok(resp) => {
                if let Some(modes) = &resp.modes {
                    let ids: Vec<String> = modes
                        .available_modes
                        .iter()
                        .map(|m| m.id.0.to_string())
                        .collect();
                    shared.with_session(&router_sid, |s| {
                        if let Some(pin) = &mut s.pin {
                            pin.available_modes = ids;
                        }
                    });
                }
                let _ = responder.respond(resp);
            }
            Err(err) => {
                shared.unregister_route(&key, &persisted.downstream_session_id);
                shared.with_session(&router_sid, |s| s.pin = None);
                let _ = responder.respond_with_error(err);
            }
        }
        Ok(())
    })
}

pub fn on_session_delete(
    shared: Arc<Shared>,
    req: DeleteSessionRequest,
    responder: Responder<DeleteSessionResponse>,
    cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    let router_sid = sid_str(&req.session_id);
    let persisted = match lookup_persisted(&shared, &router_sid) {
        Ok(p) => p,
        Err(e) => return responder.respond_with_error(e),
    };
    let (key, conn) = match owning_target(
        &shared,
        &persisted,
        |caps| caps.session_capabilities.delete.is_some(),
        "session/delete",
    ) {
        Ok(v) => v,
        Err(e) => return responder.respond_with_error(e),
    };

    let fwd =
        DeleteSessionRequest::new(persisted.downstream_session_id.clone()).meta(req.meta.clone());
    cx.spawn(async move {
        match conn.send_request(fwd).block_task().await {
            Ok(resp) => {
                shared.unregister_route(&key, &persisted.downstream_session_id);
                shared.sessions.lock().unwrap().remove(&router_sid);
                shared.state.lock().unwrap().remove(&router_sid);
                let _ = responder.respond(resp);
            }
            Err(err) => {
                let _ = responder.respond_with_error(err);
            }
        }
        Ok(())
    })
}

pub fn on_session_close(
    shared: Arc<Shared>,
    req: CloseSessionRequest,
    responder: Responder<CloseSessionResponse>,
) -> Result<(), AcpError> {
    let router_sid = sid_str(&req.session_id);
    let pin = shared
        .sessions
        .lock()
        .unwrap()
        .get(&router_sid)
        .and_then(|s| s.pin.clone());
    match pin {
        Some(pin) => {
            // Close the live downstream session when supported; state-file
            // entries survive so resume/load keep working if the downstream
            // persists sessions.
            close_downstream_session(&shared, &pin.process_key, &pin.downstream_sid);
            shared.sessions.lock().unwrap().remove(&router_sid);
            responder.respond(CloseSessionResponse::new())
        }
        None => {
            // Unpinned session: remove only router state.
            let existed = shared
                .sessions
                .lock()
                .unwrap()
                .remove(&router_sid)
                .is_some();
            if existed {
                responder.respond(CloseSessionResponse::new())
            } else {
                responder.respond_with_error(AcpError::invalid_params().data("unknown session id"))
            }
        }
    }
}
