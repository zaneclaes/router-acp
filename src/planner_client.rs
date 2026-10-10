//! Negotiated client presentation and controls for durable children. The
//! core owns work state; clients keep their own UI and authorization policy.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::v1::{
    CancelNotification, ContentBlock, McpServer, PromptRequest,
};
use serde_json::{Value, json};

use crate::candidate::CandidateId;
use crate::downstream::ProcessKey;
use crate::planner_workflow::{self, WorkerIdentity};
use crate::session::{DownstreamRoute, OpenedSession, Shared, TargetRuntime};

#[derive(Clone)]
pub struct ChildRoute {
    pub parent_sid: String,
    pub work_id: String,
    pub child_id: String,
    pub attempt_id: String,
    pub candidate: String,
    pub state_sid: String,
}

pub fn supported(shared: &Shared) -> bool {
    serde_json::to_value(shared.upstream_client_capabilities())
        .ok()
        .and_then(|v| {
            v.pointer("/_meta/router_acp/planner_children")
                .and_then(Value::as_bool)
        })
        == Some(true)
}

#[allow(clippy::too_many_arguments)] // ACP session inputs plus durable attempt identity.
pub(crate) async fn open_child(
    shared: &Arc<Shared>,
    sid: &str,
    identity: &WorkerIdentity,
    candidate: &CandidateId,
    cwd: PathBuf,
    dirs: Vec<PathBuf>,
    mut servers: Vec<McpServer>,
    capture: Arc<Mutex<String>>,
) -> Result<OpenedSession, agent_client_protocol::schema::v1::Error> {
    use agent_client_protocol::schema::v1::Error;
    let run = planner_workflow::load(shared, sid)
        .map_err(|e| Error::internal_error().data(e))?
        .ok_or_else(Error::internal_error)?;
    let work = &run.works[&identity.work_id];
    let base_key = shared
        .candidate_runtime(candidate)
        .ok_or_else(Error::invalid_params)?
        .process_key;
    let mut spec = shared
        .target_spec(&base_key)
        .ok_or_else(Error::internal_error)?;
    let key = ProcessKey(format!(
        "planner-{}-{}",
        identity.attempt_id,
        &planner_workflow_hash(candidate)[..12]
    ));
    spec.key = key.clone();
    if let Some(workspace) = &work.workspace {
        for server in &workspace.mcp_servers {
            let value = serde_json::to_value(server)
                .map_err(|e| Error::invalid_params().data(e.to_string()))?;
            let name = value
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(Error::invalid_params)?;
            if matches!(name, "router-worker" | "router-delegate") {
                return Err(
                    Error::invalid_params().data("allocator cannot replace router lifecycle tools")
                );
            }
            servers.retain(|s| {
                serde_json::to_value(s)
                    .ok()
                    .and_then(|v| v.get("name").cloned())
                    .as_ref()
                    .and_then(Value::as_str)
                    != Some(name)
            });
            servers.push(server.clone());
        }
        for (name, value) in &workspace.environment {
            if name.is_empty() || name.contains('=') || name.contains('\0') || value.contains('\0')
            {
                return Err(
                    Error::invalid_params().data("allocator returned invalid child environment")
                );
            }
            spec.env.retain(|(key, _)| key != name);
            spec.env.push((name.clone(), value.clone()));
        }
    }
    if let Some(agent) = shared
        .agent_configs()
        .iter()
        .find(|a| a.name == candidate.agent)
    {
        shared
            .llm_proxy
            .register_agent(agent, std::slice::from_ref(&spec));
    }
    shared
        .planner_child_targets
        .lock()
        .unwrap()
        .insert(key.clone());
    shared.targets.lock().unwrap().insert(
        key.clone(),
        TargetRuntime {
            spec,
            conn: None,
            init: None,
            model_config_id: None,
            auth_pending: false,
            credential_generation: None,
            dead: None,
            last_respawn: None,
            start_gate: Arc::default(),
            stop: Default::default(),
            stopped: Arc::default(),
        },
    );
    let result = crate::session::open_downstream_session_at(
        shared,
        candidate,
        key.clone(),
        cwd,
        dirs,
        servers,
        DownstreamRoute::Delegate {
            parent_router_sid: sid.into(),
            capture,
        },
    )
    .await;
    match result {
        Ok(opened) => {
            let route = ChildRoute {
                parent_sid: sid.into(),
                work_id: identity.work_id.clone(),
                child_id: work.child_id.clone(),
                attempt_id: identity.attempt_id.clone(),
                candidate: candidate.to_string(),
                state_sid: format!("{sid}::delegate-{}", opened.downstream_sid),
            };
            shared
                .planner_child_routes
                .lock()
                .unwrap()
                .insert((key, opened.downstream_sid.clone()), route.clone());
            if let Err(error) = notify(shared, &route, "allocated", None) {
                crate::session::close_downstream_session(
                    shared,
                    &opened.process_key,
                    &opened.downstream_sid,
                );
                return Err(error);
            }
            Ok(opened)
        }
        Err(error) => {
            crate::session::close_downstream_session(shared, &key, "");
            Err(error)
        }
    }
}

fn planner_workflow_hash(candidate: &CandidateId) -> String {
    crate::planner_skills::content_hash(&candidate.to_string())
}

pub(crate) fn turn_state(
    shared: &Shared,
    sid: &str,
    actor: &WorkerIdentity,
    state: &str,
) -> Result<(), String> {
    let route = shared
        .planner_child_routes
        .lock()
        .unwrap()
        .values()
        .find(|r| {
            r.parent_sid == sid && r.work_id == actor.work_id && r.attempt_id == actor.attempt_id
        })
        .cloned();
    if let Some(route) = route {
        notify(shared, &route, state, None).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn confirmed_effort(shared: &Shared, state_sid: &str) -> Option<Value> {
    shared
        .state
        .lock()
        .unwrap()
        .get(state_sid)
        .and_then(|s| s.routing)
        .filter(|r| r.pointer("/effort/confirmed").and_then(Value::as_bool) == Some(true))
        .and_then(|r| r.pointer("/effort/resolved").cloned())
}

pub(crate) fn notify(
    shared: &Shared,
    route: &ChildRoute,
    state: &str,
    update: Option<Value>,
) -> Result<(), agent_client_protocol::schema::v1::Error> {
    if !supported(shared) {
        return Ok(());
    }
    if let Some(upstream) = shared.upstream() {
        let revision = planner_workflow::load(shared, &route.parent_sid)
            .map_err(|e| agent_client_protocol::schema::v1::Error::internal_error().data(e))?
            .map(|run| run.revision);
        let effort = confirmed_effort(shared, &route.state_sid);
        let effort_confirmed = effort.is_some();
        let msg = agent_client_protocol::UntypedMessage::new(
            "router-acp/planner-child-update",
            json!({
                "sessionId":route.parent_sid,"child_id":route.child_id,"work_id":route.work_id,"attempt_id":route.attempt_id,
                "revision":revision,"state_session_id":route.state_sid,"candidate":route.candidate,"effort":effort,"effort_confirmed":effort_confirmed,"state":state,"update":update
            }),
        )?;
        upstream.send_notification(msg)?;
    }
    Ok(())
}

/// Stop the current process without deleting the durable work or child identity.
pub(crate) fn stop_child(shared: &Arc<Shared>, sid: &str, child: &str) {
    let ids = shared
        .live_delegates
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, d)| d.parent_sid == sid && d.worker_id == child)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    for delegate_id in ids {
        // A completing turn may close the same delegate first.
        let _ = crate::delegate_mcp::run_delegate_close(
            shared,
            sid,
            crate::delegate_mcp::DelegateCloseArgs { delegate_id },
        );
    }
    let routes = shared
        .planner_child_routes
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, r)| r.parent_sid == sid && r.child_id == child)
        .map(|((key, down), _)| (key.clone(), down.clone()))
        .collect::<Vec<_>>();
    for (key, down) in routes {
        if let Some(conn) = shared.target_conn(&key) {
            let _ = conn.send_notification(CancelNotification::new(down.clone()));
        }
        shared.with_session(sid, |s| {
            s.delegates
                .retain(|h| h.process_key != key || h.downstream_sid != down);
        });
        crate::session::close_downstream_session(shared, &key, &down);
    }
    crate::delegate_mcp::drop_worker_tokens(shared, child);
}

pub async fn control(shared: &Arc<Shared>, params: Value) -> Result<Value, String> {
    if !supported(shared) {
        return Err("client did not negotiate planner children".into());
    }
    let sid = params
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or("exact parent sessionId required")?;
    let child = params
        .get("child_id")
        .and_then(Value::as_str)
        .ok_or("exact durable child_id required")?;
    let run = planner_workflow::load(shared, sid)?.ok_or("planner parent is not loaded")?;
    let (work_id, work) = run
        .works
        .iter()
        .find(|(_, work)| work.child_id == child)
        .ok_or("child does not belong to this parent")?;
    let action = params
        .get("action")
        .and_then(Value::as_str)
        .ok_or("child action required")?;
    if action == "status" {
        let active = work.attempt.as_ref().is_some_and(|attempt| {
            !attempt.ended
                && (attempt.router_pid, attempt.router_started_at_ms)
                    == planner_workflow::process_identity()
        });
        let route = shared
            .planner_child_routes
            .lock()
            .unwrap()
            .values()
            .find(|route| {
                route.parent_sid == sid
                    && route.child_id == child
                    && work
                        .attempt
                        .as_ref()
                        .is_some_and(|attempt| attempt.id == route.attempt_id)
            })
            .cloned();
        let effort = if active {
            route
                .as_ref()
                .and_then(|route| confirmed_effort(shared, &route.state_sid))
        } else {
            None
        };
        return Ok(json!({
            "sessionId": sid, "child_id": child, "work_id": work_id,
            "attempt_id": work.attempt.as_ref().map(|attempt| &attempt.id),
            "revision": run.revision, "active": active, "candidate": route.map(|route| route.candidate),
            "effort_confirmed": effort.is_some(), "effort": effort
        }));
    }
    if matches!(action, "cancel" | "close") {
        planner_workflow::mutate(shared, sid, |run| {
            run.works
                .get_mut(work_id)
                .ok_or("assignment disappeared")?
                .paused = true;
            Ok(())
        })?;
        stop_child(shared, sid, child);
        return Ok(json!({"stopped":true}));
    }
    if action != "prompt" {
        return Err("unsupported planner child action".into());
    }
    let blocks: Vec<ContentBlock> = serde_json::from_value(
        params
            .get("prompt")
            .cloned()
            .ok_or("original prompt blocks required")?,
    )
    .map_err(|e| e.to_string())?;
    let mut request = PromptRequest::new(sid.to_string(), blocks);
    request.meta =
        serde_json::from_value(params.get("_meta").cloned().unwrap_or_else(
            || json!({"router_acp":{"input_id":uuid::Uuid::new_v4().to_string()}}),
        ))
        .ok();
    if planner_workflow::prompt_input_id(&request).is_none() {
        return Err("stable original input_id required for child prompt retries".into());
    }
    let human = planner_workflow::human_prompt(&request);
    planner_workflow::observe_input(shared, sid, &request)?;
    let input_id = planner_workflow::prompt_input_id(&request)
        .unwrap()
        .to_string();
    planner_workflow::mutate(shared, sid, |run| {
        let input = run
            .inputs
            .get_mut(&input_id)
            .ok_or("original input missing")?;
        if input.owner.as_deref().is_some_and(|owner| owner != work_id) {
            return Err("correction must retain original owner".into());
        }
        input.owner = Some(work_id.clone());
        if human {
            run.works.get_mut(work_id).unwrap().paused = false;
        }
        run.wakes
            .entry(format!("input:{input_id}"))
            .or_insert(planner_workflow::Wake {
                reason: "new input for existing child owner".into(),
                work_id: work_id.clone(),
                ..Default::default()
            });
        Ok(())
    })?;
    // Active work and pending review keep one owner. The parent decides the
    // bounded correction after reconciling its new input wake.
    if work.attempt.as_ref().is_some_and(|a| !a.ended)
        || work.status == planner_workflow::WorkStatus::ReviewPending
    {
        return Ok(json!({"stopReason":"end_turn","queued":true,"input_id":input_id}));
    }
    let live = shared
        .live_delegates
        .lock()
        .unwrap()
        .iter()
        .find(|(_, d)| d.parent_sid == sid && d.worker_id == child)
        .map(|(id, d)| (id.clone(), d.process_key.clone()));
    let live_id = if let Some((id, key)) = live {
        if shared.target_conn(&key).is_some() {
            Some(id)
        } else {
            crate::delegate_mcp::run_delegate_close(
                shared,
                sid,
                crate::delegate_mcp::DelegateCloseArgs { delegate_id: id },
            )?;
            None
        }
    } else {
        None
    };
    if let Some(delegate_id) = live_id {
        let args = serde_json::from_value(json!({"delegate_id":delegate_id,"message":"Reconcile the original client input receipt.","input_ids":[input_id]})).map_err(|e| e.to_string())?;
        crate::delegate_mcp::run_delegate_followup(shared, sid, args).await?;
    } else {
        let args = serde_json::from_value(json!({"work_id":work_id,"task":"Resume the same durable assignment and reconcile original input.","input_ids":[input_id],"keep_open":true})).map_err(|e| e.to_string())?;
        crate::delegate_mcp::run_delegate_task(shared, sid, args).await?;
    }
    Ok(json!({"stopReason":"end_turn","input_id":input_id}))
}
