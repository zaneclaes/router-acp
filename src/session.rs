//! Router session state, pin map, id remapping, callback forwarding, and the
//! upstream ACP agent surface.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::json;

use agent_client_protocol::schema::ProtocolVersion;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, AuthMethod, AuthMethodAgent, AuthenticateRequest, AuthenticateResponse,
    CancelNotification, ClientCapabilities, CloseSessionRequest, CloseSessionResponse,
    ContentBlock, ContentChunk, DeleteSessionRequest, Error as AcpError, Implementation,
    InitializeRequest, InitializeResponse, ListSessionsRequest, ListSessionsResponse,
    LoadSessionRequest, McpCapabilities, McpServer, NewSessionRequest, NewSessionResponse,
    PromptCapabilities, PromptRequest, PromptResponse, ResumeSessionRequest, SessionCapabilities,
    SessionConfigId, SessionConfigOption, SessionConfigOptionCategory, SessionConfigOptionValue,
    SessionConfigSelectGroup, SessionConfigSelectOption, SessionNotification, SessionUpdate,
    SetSessionConfigOptionRequest, SetSessionConfigOptionResponse, SetSessionModeRequest,
    StopReason,
};
use agent_client_protocol::{
    Agent as AgentPeer, Client as ClientPeer, ConnectTo, ConnectionTo, Dispatch, Handled,
    RequestCancellation, Responder, on_receive_dispatch, on_receive_notification,
    on_receive_request,
};

use crate::candidate::{CandidateId, RequiredCaps, ScoreTable, TaskClass};
use crate::classifier::{ClassifierRules, ClassifyInput, classify, cwd_language_fingerprint};
use crate::config::{Config, DisclosureMode, EscalationPath, SkillRoute, StrategyKind};
use crate::downstream::{
    ProcessKey, ProcessTargetSpec, SelectionKind, build_targets, is_auth_required, probe_target,
    start_downstream, verify_model_selected,
};
use crate::headroom::HeadroomTracker;
use crate::relay;
use crate::state::{PersistedSession, StateFile};
use crate::strategies::{CandidateView, RouteContext, make_strategy};

/// Status of one `(agent, model)` candidate.
#[derive(Debug, Clone, PartialEq)]
pub enum CandidateStatus {
    Unverified,
    Routeable,
    AuthPending,
    Invalid(String),
    /// The owning downstream process died (outage). Unlike `Invalid`, this
    /// is recoverable: the router respawns and re-probes the target on the
    /// next routing decision (subject to `failover.respawn_cooldown_secs`).
    Down(String),
}

#[derive(Debug, Clone)]
pub struct CandidateRuntime {
    pub id: CandidateId,
    pub display_name: String,
    pub cost_rank: u32,
    pub config_index: usize,
    pub process_key: ProcessKey,
    pub status: CandidateStatus,
}

/// Runtime state for one downstream process target.
pub struct TargetRuntime {
    pub spec: ProcessTargetSpec,
    pub conn: Option<ConnectionTo<AgentPeer>>,
    pub init: Option<InitializeResponse>,
    pub model_config_id: Option<SessionConfigId>,
    pub auth_pending: bool,
    pub dead: Option<String>,
    /// Last respawn attempt for a dead target (cooldown bookkeeping).
    pub last_respawn: Option<std::time::Instant>,
}

/// Where messages from a downstream session should be routed.
#[derive(Clone)]
pub enum DownstreamRoute {
    /// A pinned (or loading) primary session: relay to the client under the
    /// router session id.
    Primary { router_sid: String },
    /// A delegated sub-session: capture agent output, forward
    /// permission/fs/terminal callbacks under the parent's session id.
    Delegate {
        parent_router_sid: String,
        capture: Arc<Mutex<String>>,
    },
}

#[derive(Debug, Clone)]
pub struct PinInfo {
    pub candidate: CandidateId,
    pub process_key: ProcessKey,
    pub downstream_sid: String,
    /// Mode ids the pinned downstream session advertised at creation.
    pub available_modes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DelegateHandle {
    pub process_key: ProcessKey,
    pub downstream_sid: String,
}

/// A delegate sub-session kept open across multiple turns so the orchestrator
/// can send follow-up instructions to the same sub-agent (preserving its
/// context) rather than re-briefing a fresh session each time.
pub struct LiveDelegate {
    pub parent_sid: String,
    pub process_key: ProcessKey,
    pub downstream_sid: String,
    pub candidate: CandidateId,
    /// The sub-agent's captured output buffer (cleared before each follow-up).
    pub capture: Arc<Mutex<String>>,
    /// State-DB row id for this delegate, for follow-up logging.
    pub sub_sid: String,
}

pub struct RouterSession {
    pub cwd: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub mcp_servers: Vec<McpServer>,
    pub strategy: StrategyKind,
    pub candidate_override: Option<CandidateId>,
    /// Soft preference from `[router: prefer=...]`: tried first at pin time,
    /// but falls through to the normal strategy ranking if unavailable.
    pub preferred_candidate: Option<CandidateId>,
    pub pin: Option<PinInfo>,
    pub pinning: bool,
    pub cancelled: bool,
    pub delegate_token: Option<String>,
    /// Route details to attach under `_meta.router_acp` on the first
    /// forwarded update.
    pub pending_meta_disclosure: Option<serde_json::Value>,
    /// Router notice lines queued to ride the model's next response chunk
    /// (routing disclosure, failover/cordon notices). See `notify_user`.
    pub pending_disclosure: Vec<String>,
    /// A `session/set_mode` received before the pin (some clients, e.g.
    /// goose, set a mode right after `session/new`). Deferred and applied
    /// to the downstream session at pin time.
    pub pending_mode: Option<String>,
    /// The client-requested mode id last successfully applied downstream;
    /// re-resolved and re-applied when a failover re-pins the session.
    pub applied_mode: Option<String>,
    /// Candidate/agent exclusion patterns from a `[router: exclude=...]`
    /// prompt directive. Session-scoped; also honored by failover re-pins.
    pub excluded: Vec<String>,
    /// Optional grouping label from `[router: label=...]` — shared by the
    /// planner/subtask/review sessions of one orchestration run.
    pub run_label: Option<String>,
    /// Whether any downstream output was relayed to the client during the
    /// current prompt turn. Failover is only safe while this is false
    /// (retrying after visible output risks duplicated side effects).
    pub turn_saw_output: bool,
    /// Accumulated agent text this turn, for token estimation + logging.
    pub turn_output: String,
    pub delegates: Vec<DelegateHandle>,
    // ---- mid-session model switching / auto-upgrade ----
    /// Score-table quality of the current pin for `task_class` — the base of
    /// the session confidence estimate.
    pub pinned_quality: f64,
    /// The session's classified task class (set at pin), for choosing an
    /// upgrade target.
    pub task_class: Option<crate::candidate::TaskClass>,
    /// Accumulated "struggle" signal (max-tokens/refusal stops, tool-call
    /// failures); subtracted from `pinned_quality` to get confidence.
    pub struggle: f64,
    /// Failed tool calls seen this turn (reset each turn).
    pub turn_tool_failures: u32,
    /// Tool-call ids already counted as investigation this turn (a tool emits
    /// several update frames; count it once). Reset each turn.
    pub turn_counted_tools: HashSet<String>,
    /// Tool-call ids already counted as failed this turn. Reset each turn.
    pub turn_failed_tools: HashSet<String>,
    /// Distinct tool calls issued this turn (any kind) — the `escalation`
    /// router's "grinding without finishing" signal. Reset each turn.
    pub turn_tool_calls: u32,
    /// Investigation events (file reads / searches) seen this turn — the
    /// `escalation` router's read-volume signal (reset each turn).
    pub turn_reads: u32,
    /// Set once this turn produces a side effect (output streamed, or a
    /// write/exec tool call): the point past which mid-turn escalation is no
    /// longer safe (reroute could double-apply). Reset each turn.
    pub turn_side_effect: bool,
    /// Number of escalations this session has already performed (bounds
    /// ladder thrash against `escalation.max_escalations`).
    pub escalations_done: u32,
    /// A model switch requested for the next prompt: explicit
    /// `[router: switch=...]`, a skill-class requirement, or an auto-upgrade.
    pub pending_switch: Option<SwitchRequest>,
    /// A mid-turn escalation requested by the `escalation` router while the
    /// current turn is still side-effect-free: the failover loop performs it
    /// (switch + replay) as soon as the interrupted turn returns.
    pub escalation_requested: Option<SwitchRequest>,
    /// Summary text from the previous model, prepended to the next prompt
    /// sent to the new model after a switch.
    pub pending_context: Option<String>,
    /// When set, agent text on the pinned session is captured here instead
    /// of relayed (used to collect a summary during a switch).
    pub capturing_summary: Option<Arc<Mutex<String>>>,
    // ---- auto-orchestration ----
    /// Set once a prompt is detected as a multi-part task list and the session
    /// is put into orchestration mode: relaxes delegation to allow same-/higher-
    /// tier peers (for cross-lineage review), and marks the session as an
    /// orchestrator in disclosures/state.
    pub orchestrating: bool,
    /// One-shot orchestration protocol instructions to prepend to the next
    /// prompt (taken once, like `pending_context`).
    pub pending_orchestration: Option<String>,
    /// Whether the native-subagent-usage warning has fired this turn (an
    /// orchestrating session using the adapter's built-in `Task` tool instead of
    /// `delegate_task`). Reset each turn so it warns at most once per turn.
    pub turn_native_subagent_warned: bool,
    /// Ticket ids already injected into this session's context (a re-mention
    /// doesn't re-inject the same ticket).
    pub injected_tickets: HashSet<String>,
}

/// A requested mid-session model switch and why.
#[derive(Debug, Clone)]
pub struct SwitchRequest {
    pub target: CandidateId,
    pub reason: String,
}

impl RouterSession {
    /// Rebuild a session record from persisted state (session/load,
    /// session/resume).
    pub fn rehydrated(
        cfg: &Config,
        persisted: &crate::state::PersistedSession,
        mcp_servers: Vec<McpServer>,
    ) -> Self {
        Self {
            cwd: persisted.cwd.clone(),
            additional_directories: persisted.additional_directories.clone(),
            mcp_servers,
            strategy: cfg.router,
            candidate_override: None,
            preferred_candidate: None,
            pin: None,
            pinning: false,
            cancelled: false,
            delegate_token: None,
            pending_meta_disclosure: None,
            pending_disclosure: Vec::new(),
            pending_mode: None,
            applied_mode: None,
            excluded: Vec::new(),
            run_label: None,
            turn_saw_output: false,
            turn_output: String::new(),
            delegates: Vec::new(),
            pinned_quality: 0.0,
            task_class: None,
            struggle: 0.0,
            turn_tool_failures: 0,
            turn_counted_tools: HashSet::new(),
            turn_failed_tools: HashSet::new(),
            turn_tool_calls: 0,
            turn_reads: 0,
            turn_side_effect: false,
            escalations_done: 0,
            pending_switch: None,
            escalation_requested: None,
            pending_context: None,
            capturing_summary: None,
            orchestrating: false,
            pending_orchestration: None,
            turn_native_subagent_warned: false,
            injected_tickets: HashSet::new(),
        }
    }

    fn new(cfg: &Config, req: &NewSessionRequest) -> Self {
        Self {
            cwd: req.cwd.clone(),
            additional_directories: req.additional_directories.clone(),
            mcp_servers: req.mcp_servers.clone(),
            strategy: cfg.router,
            candidate_override: None,
            preferred_candidate: None,
            pin: None,
            pinning: false,
            cancelled: false,
            delegate_token: None,
            pending_meta_disclosure: None,
            pending_disclosure: Vec::new(),
            pending_mode: None,
            applied_mode: None,
            excluded: Vec::new(),
            run_label: None,
            turn_saw_output: false,
            turn_output: String::new(),
            delegates: Vec::new(),
            pinned_quality: 0.0,
            task_class: None,
            struggle: 0.0,
            turn_tool_failures: 0,
            turn_counted_tools: HashSet::new(),
            turn_failed_tools: HashSet::new(),
            turn_tool_calls: 0,
            turn_reads: 0,
            turn_side_effect: false,
            escalations_done: 0,
            pending_switch: None,
            escalation_requested: None,
            pending_context: None,
            capturing_summary: None,
            orchestrating: false,
            pending_orchestration: None,
            turn_native_subagent_warned: false,
            injected_tickets: HashSet::new(),
        }
    }
}

/// Router-wide shared state. All mutexes are short-lived and never held
/// across await points.
pub struct Shared {
    pub cfg: Config,
    pub scores: ScoreTable,
    pub rules: ClassifierRules,
    pub state: Mutex<StateFile>,
    pub headroom: Mutex<HeadroomTracker>,
    pub sessions: Mutex<HashMap<String, RouterSession>>,
    pub targets: Mutex<HashMap<ProcessKey, TargetRuntime>>,
    pub candidates: Mutex<Vec<CandidateRuntime>>,
    sid_map: Mutex<HashMap<(ProcessKey, String), DownstreamRoute>>,
    pub delegate_tokens: Mutex<HashMap<String, String>>,
    /// Delegate sub-sessions kept alive for follow-up turns (orchestration),
    /// keyed by the short `delegate_id` returned to the orchestrator.
    pub live_delegates: Mutex<HashMap<String, LiveDelegate>>,
    /// Short-TTL cache of fetched ticket content (ticket id → (fetched-at,
    /// body)), so concurrent sessions share one fetch.
    pub ticket_cache: Mutex<HashMap<String, (std::time::Instant, String)>>,
    pub delegate_semaphore: Arc<tokio::sync::Semaphore>,
    pub delegate_socket: OnceLock<PathBuf>,
    upstream: OnceLock<ConnectionTo<ClientPeer>>,
    client_caps: OnceLock<ClientCapabilities>,
    initialized: AtomicBool,
    pub probe_cwd: PathBuf,
}

impl Shared {
    pub fn new(cfg: Config) -> Result<Arc<Self>, AcpError> {
        let scores = match &cfg.score_table {
            Some(path) => {
                ScoreTable::from_file(path).map_err(|e| AcpError::invalid_params().data(e))?
            }
            None => ScoreTable::builtin(),
        };
        let rules = match &cfg.classifier.rules_file {
            Some(path) => {
                ClassifierRules::from_file(path).map_err(|e| AcpError::invalid_params().data(e))?
            }
            None => ClassifierRules::builtin(),
        };
        let budgets = cfg
            .agents
            .iter()
            .map(|a| (a.name.clone(), a.budget_prompts_5h))
            .collect();
        let headroom = HeadroomTracker::new(&cfg.headroom, budgets);
        let state = StateFile::load(&cfg.state_file, cfg.retention());

        let specs = build_targets(&cfg);
        let mut targets = HashMap::new();
        let mut candidates = Vec::new();
        let mut config_index = 0usize;
        for agent in &cfg.agents {
            for model in &agent.models {
                let process_key = match &agent.model_selection {
                    crate::config::ModelSelectionConfig::ConfigOption => {
                        ProcessKey(agent.name.clone())
                    }
                    crate::config::ModelSelectionConfig::SpawnConfig { .. } => {
                        ProcessKey(format!("{}#{}", agent.name, model.id))
                    }
                };
                candidates.push(CandidateRuntime {
                    id: CandidateId::new(&agent.name, &model.id),
                    display_name: model
                        .display_name
                        .clone()
                        .unwrap_or_else(|| model.id.clone()),
                    cost_rank: model.cost_rank,
                    config_index,
                    process_key,
                    status: CandidateStatus::Unverified,
                });
                config_index += 1;
            }
        }
        for spec in specs {
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
                },
            );
        }

        let max_concurrent = cfg.delegation.max_concurrent;
        Ok(Arc::new(Self {
            cfg,
            scores,
            rules,
            state: Mutex::new(state),
            headroom: Mutex::new(headroom),
            sessions: Mutex::new(HashMap::new()),
            targets: Mutex::new(targets),
            candidates: Mutex::new(candidates),
            sid_map: Mutex::new(HashMap::new()),
            delegate_tokens: Mutex::new(HashMap::new()),
            live_delegates: Mutex::new(HashMap::new()),
            ticket_cache: Mutex::new(HashMap::new()),
            delegate_semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            delegate_socket: OnceLock::new(),
            upstream: OnceLock::new(),
            client_caps: OnceLock::new(),
            initialized: AtomicBool::new(false),
            probe_cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")),
        }))
    }

    // ------------------------------------------------------------------
    // Target/candidate accessors used by downstream.rs
    // ------------------------------------------------------------------

    pub fn upstream(&self) -> Option<ConnectionTo<ClientPeer>> {
        self.upstream.get().cloned()
    }

    pub fn upstream_client_capabilities(&self) -> ClientCapabilities {
        self.client_caps.get().cloned().unwrap_or_default()
    }

    pub fn target_keys(&self) -> Vec<ProcessKey> {
        let mut keys: Vec<ProcessKey> = self.targets.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }

    pub fn target_keys_for_agent(&self, agent: &str) -> Vec<ProcessKey> {
        let mut keys: Vec<ProcessKey> = self
            .targets
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, t)| t.spec.agent_name == agent)
            .map(|(k, _)| k.clone())
            .collect();
        keys.sort();
        keys
    }

    pub fn target_spec(&self, key: &ProcessKey) -> Option<ProcessTargetSpec> {
        self.targets
            .lock()
            .unwrap()
            .get(key)
            .map(|t| t.spec.clone())
    }

    pub fn target_conn(&self, key: &ProcessKey) -> Option<ConnectionTo<AgentPeer>> {
        self.targets
            .lock()
            .unwrap()
            .get(key)
            .and_then(|t| t.conn.clone())
    }

    pub fn target_init(&self, key: &ProcessKey) -> Option<InitializeResponse> {
        self.targets
            .lock()
            .unwrap()
            .get(key)
            .and_then(|t| t.init.clone())
    }

    pub fn set_target_conn(&self, key: &ProcessKey, conn: ConnectionTo<AgentPeer>) {
        if let Some(t) = self.targets.lock().unwrap().get_mut(key) {
            t.conn = Some(conn);
            t.dead = None;
        }
    }

    pub fn set_target_init(&self, key: &ProcessKey, init: InitializeResponse) {
        if let Some(t) = self.targets.lock().unwrap().get_mut(key) {
            t.init = Some(init);
        }
    }

    pub fn set_target_model_config_id(&self, key: &ProcessKey, id: SessionConfigId) {
        if let Some(t) = self.targets.lock().unwrap().get_mut(key) {
            t.model_config_id = Some(id);
        }
    }

    pub fn set_target_auth_pending(&self, key: &ProcessKey) {
        if let Some(t) = self.targets.lock().unwrap().get_mut(key) {
            t.auth_pending = true;
        }
        self.update_candidates(key, |c| {
            if !matches!(c.status, CandidateStatus::Invalid(_)) {
                c.status = CandidateStatus::AuthPending;
            }
        });
    }

    pub fn set_target_failed(&self, key: &ProcessKey, reason: &str) {
        tracing::warn!(target = %key, reason, "downstream target failed verification");
        let reason = reason.to_string();
        self.update_candidates(key, move |c| {
            c.status = CandidateStatus::Invalid(reason.clone());
        });
    }

    pub fn mark_target_dead(&self, key: &ProcessKey, reason: &str) {
        tracing::warn!(target = %key, reason, "downstream target died");
        if let Some(t) = self.targets.lock().unwrap().get_mut(key) {
            t.conn = None;
            t.dead = Some(reason.to_string());
        }
        let reason = reason.to_string();
        self.update_candidates(key, move |c| {
            if !matches!(c.status, CandidateStatus::Invalid(_)) {
                c.status = CandidateStatus::Down(reason.clone());
            }
        });
    }

    pub fn set_models_routeable(&self, key: &ProcessKey, model_ids: Vec<String>) {
        if let Some(t) = self.targets.lock().unwrap().get_mut(key) {
            t.auth_pending = false;
        }
        self.update_candidates(key, move |c| {
            if model_ids.iter().any(|m| m == &c.id.model) {
                c.status = CandidateStatus::Routeable;
            }
        });
    }

    pub fn set_model_invalid(&self, key: &ProcessKey, model_id: &str, reason: &str) {
        let model_id = model_id.to_string();
        let reason = reason.to_string();
        self.update_candidates(key, move |c| {
            if c.id.model == model_id {
                c.status = CandidateStatus::Invalid(reason.clone());
            }
        });
    }

    fn update_candidates(&self, key: &ProcessKey, f: impl Fn(&mut CandidateRuntime)) {
        for c in self
            .candidates
            .lock()
            .unwrap()
            .iter_mut()
            .filter(|c| &c.process_key == key)
        {
            f(c);
        }
    }

    pub fn routeable_candidates(&self) -> Vec<CandidateRuntime> {
        self.candidates
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.status == CandidateStatus::Routeable)
            .cloned()
            .collect()
    }

    pub fn has_auth_pending(&self) -> bool {
        self.candidates
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.status == CandidateStatus::AuthPending)
    }

    pub fn candidate_runtime(&self, id: &CandidateId) -> Option<CandidateRuntime> {
        self.candidates
            .lock()
            .unwrap()
            .iter()
            .find(|c| &c.id == id)
            .cloned()
    }

    // ------------------------------------------------------------------
    // Downstream session routing
    // ------------------------------------------------------------------

    pub fn register_route(&self, key: &ProcessKey, downstream_sid: &str, route: DownstreamRoute) {
        self.sid_map
            .lock()
            .unwrap()
            .insert((key.clone(), downstream_sid.to_string()), route);
    }

    pub fn unregister_route(&self, key: &ProcessKey, downstream_sid: &str) {
        self.sid_map
            .lock()
            .unwrap()
            .remove(&(key.clone(), downstream_sid.to_string()));
    }

    pub fn route_for(&self, key: &ProcessKey, downstream_sid: &str) -> Option<DownstreamRoute> {
        self.sid_map
            .lock()
            .unwrap()
            .get(&(key.clone(), downstream_sid.to_string()))
            .cloned()
    }

    /// The pinned downstream connection and session id for a router session.
    pub fn pinned_route(
        &self,
        router_sid: &str,
    ) -> Option<(ConnectionTo<AgentPeer>, String, CandidateId)> {
        let pin = self
            .sessions
            .lock()
            .unwrap()
            .get(router_sid)
            .and_then(|s| s.pin.clone())?;
        let conn = self.target_conn(&pin.process_key)?;
        Some((conn, pin.downstream_sid, pin.candidate))
    }

    pub fn with_session<R>(
        &self,
        router_sid: &str,
        f: impl FnOnce(&mut RouterSession) -> R,
    ) -> Option<R> {
        self.sessions.lock().unwrap().get_mut(router_sid).map(f)
    }

    pub fn take_meta_disclosure(&self, router_sid: &str) -> Option<serde_json::Value> {
        self.sessions
            .lock()
            .unwrap()
            .get_mut(router_sid)
            .and_then(|s| s.pending_meta_disclosure.take())
    }

    // ------------------------------------------------------------------
    // Routing pool
    // ------------------------------------------------------------------

    /// Candidates that are routeable, unquarantined, and satisfy the prompt's
    /// required capabilities, as strategy views.
    pub fn eligible_views(&self, required: &RequiredCaps, class: TaskClass) -> Vec<CandidateView> {
        self.eligible_views_inner(required, class, false)
    }

    /// Like `eligible_views` but keeps usage-cordoned candidates in the pool.
    /// Used only for the all-cordoned "least-bad" fallback, where every
    /// candidate is usage-cordoned and the turn would otherwise fail.
    pub fn eligible_views_relaxed(
        &self,
        required: &RequiredCaps,
        class: TaskClass,
    ) -> Vec<CandidateView> {
        self.eligible_views_inner(required, class, true)
    }

    fn eligible_views_inner(
        &self,
        required: &RequiredCaps,
        class: TaskClass,
        ignore_usage_cordons: bool,
    ) -> Vec<CandidateView> {
        let candidates = self.routeable_candidates();
        let mut headroom = self.headroom.lock().unwrap();
        let targets = self.targets.lock().unwrap();
        let mut views = Vec::new();
        for c in candidates {
            let Some(target) = targets.get(&c.process_key) else {
                continue;
            };
            if target.conn.is_none() {
                continue;
            }
            let caps_ok = target
                .init
                .as_ref()
                .map(|i| required.satisfied_by(&i.agent_capabilities.prompt_capabilities))
                .unwrap_or(false);
            if !caps_ok || headroom.is_quarantined(&c.id) {
                continue;
            }
            // Agents cordoned by a token/usage limit sit out until reset.
            if headroom.cordon_active(&c.id.agent).is_some() {
                continue;
            }
            // Candidates proactively cordoned by the provider's usage API (cap
            // exhausted, no overage headroom) sit out until their reset.
            if !ignore_usage_cordons && headroom.usage_cordon(&c.id).is_some() {
                continue;
            }
            let scores = self.scores.lookup(&c.id);
            let static_preference = self
                .cfg
                .agents
                .iter()
                .find(|a| a.name == c.id.agent)
                .map(|a| a.preference)
                .unwrap_or(0.0);
            // Dynamic preference scaling: the configured bonus fades with the
            // seat's free plan budget, and a seat running on paid overage
            // takes a penalty — so the "free" seat wins among comparable
            // candidates.
            let preference = match headroom.availability(&c.id) {
                Some(a) if self.cfg.availability_preference.enabled => {
                    static_preference * a.plan_headroom.clamp(0.0, 1.0)
                        - if a.on_overage {
                            self.cfg.availability_preference.overage_penalty
                        } else {
                            0.0
                        }
                }
                _ => static_preference,
            };
            views.push(CandidateView {
                headroom: headroom.headroom(&c.id.agent),
                quality: scores.quality(class),
                coding_tier: scores.coding_tier,
                cost_rank: c.cost_rank,
                config_index: c.config_index,
                preference,
                id: c.id,
            });
        }
        views
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    // ------------------------------------------------------------------
    // Router-owned session config options
    // ------------------------------------------------------------------

    pub fn router_config_options(&self, router_sid: &str) -> Vec<SessionConfigOption> {
        let (strategy, override_) = self
            .with_session(router_sid, |s| (s.strategy, s.candidate_override.clone()))
            .unwrap_or((self.cfg.router, None));

        let strategy_option = SessionConfigOption::select(
            "router.strategy",
            "Routing strategy",
            strategy.as_str(),
            vec![
                SessionConfigSelectOption::new("auto", "Auto")
                    .description("Quality/cost utility routing".to_string()),
                SessionConfigSelectOption::new("pareto-code", "Pareto (code)")
                    .description("Coding tier, then cheapest available".to_string()),
                SessionConfigSelectOption::new("static", "Static")
                    .description("Always the configured candidate".to_string()),
            ],
        )
        .description("How router-acp picks the candidate at first prompt".to_string());

        let mut groups = vec![SessionConfigSelectGroup::new(
            "router",
            "Router",
            vec![SessionConfigSelectOption::new(
                "auto",
                "Auto (strategy decides)",
            )],
        )];
        // Snapshot usage cordons so cordoned candidates can be advertised as
        // unavailable (kept in the list, not dropped, so the client shows them
        // disabled with a reason).
        let usage_cordons: std::collections::HashMap<_, _> = self
            .headroom
            .lock()
            .unwrap()
            .active_usage_cordons()
            .into_iter()
            .collect();
        for agent in &self.cfg.agents {
            let options: Vec<SessionConfigSelectOption> = self
                .candidates
                .lock()
                .unwrap()
                .iter()
                .filter(|c| c.id.agent == agent.name && c.status == CandidateStatus::Routeable)
                .map(|c| {
                    let opt =
                        SessionConfigSelectOption::new(c.id.to_string(), c.display_name.clone());
                    // available defaults to true/absent; a cordoned candidate is
                    // advertised unavailable with a reason and reset time under
                    // `_meta.router_acp`.
                    match usage_cordons.get(&c.id) {
                        Some(cordon) => {
                            let mut meta = serde_json::Map::new();
                            meta.insert(
                                "router_acp".to_string(),
                                json!({
                                    "available": false,
                                    "unavailable_reason": cordon.reason,
                                    "resets_at": cordon.resets_at_rfc3339,
                                }),
                            );
                            opt.meta(meta)
                        }
                        None => opt,
                    }
                })
                .collect();
            if !options.is_empty() {
                groups.push(SessionConfigSelectGroup::new(
                    agent.name.clone(),
                    agent.name.clone(),
                    options,
                ));
            }
        }
        let current = override_
            .map(|c| c.to_string())
            .unwrap_or_else(|| "auto".to_string());
        let candidate_option =
            SessionConfigOption::select("router.candidate", "Model", current, groups)
                .category(SessionConfigOptionCategory::Model)
                .description(
                    "Pin this session to a specific (agent, model) candidate; \
             `auto` lets the strategy decide"
                        .to_string(),
                );

        vec![strategy_option, candidate_option]
    }
}

/// Turn a `SessionId` into its string form.
pub fn sid_str(sid: &agent_client_protocol::schema::v1::SessionId) -> String {
    sid.0.to_string()
}

// ----------------------------------------------------------------------
// Downstream -> upstream relay
// ----------------------------------------------------------------------

/// Extract streamed agent text from a raw `session/update` params object.
fn agent_chunk_text(params: &serde_json::Value) -> Option<String> {
    let update = params.get("update")?;
    if update.get("sessionUpdate")?.as_str()? != "agent_message_chunk" {
        return None;
    }
    let content = update.get("content")?;
    if content.get("type")?.as_str()? != "text" {
        return None;
    }
    Some(content.get("text")?.as_str()?.to_string())
}

/// Concatenate the text of a prompt for logging/estimation.
fn prompt_display_text(prompt: &[ContentBlock]) -> String {
    let mut out = String::new();
    for b in prompt {
        if let ContentBlock::Text(t) = b {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&t.text);
        }
    }
    out
}

/// Log downstream tool-use / callback session updates that arrive on a
/// primary session (best-effort observability of "tool usage").
fn log_downstream_event(shared: &Arc<Shared>, router_sid: &str, params: &serde_json::Value) {
    let Some(update) = params.get("update") else {
        return;
    };
    let kind = update
        .get("sessionUpdate")
        .and_then(|k| k.as_str())
        .unwrap_or("");
    let entry = match kind {
        "tool_call" | "tool_call_update" => {
            let title = update
                .get("title")
                .and_then(|t| t.as_str())
                .or_else(|| update.get("toolCallId").and_then(|t| t.as_str()))
                .unwrap_or("tool")
                .to_string();
            let status = update.get("status").and_then(|s| s.as_str()).unwrap_or("");
            Some(crate::state::LogEntry {
                kind: "tool_call".to_string(),
                role: "tool".to_string(),
                summary: if status.is_empty() {
                    title
                } else {
                    format!("{title} [{status}]")
                },
                detail: Some(update.clone()),
                tokens_estimated: true,
                ..Default::default()
            })
        }
        "usage_update" => {
            let st = shared.state.lock().unwrap();
            if let Some(used) = update.get("used").and_then(|u| u.as_u64()) {
                st.set_context_used(router_sid, used);
            }
            // The adapter reports authoritative cumulative cost in USD — capture
            // it instead of relying on text-estimated tokens.
            if let Some(cost) = update
                .get("cost")
                .and_then(|c| c.get("amount"))
                .and_then(|a| a.as_f64())
            {
                st.set_cost_usd(router_sid, cost);
            }
            None
        }
        _ => None,
    };
    if let Some(entry) = entry {
        shared.state.lock().unwrap().log(router_sid, &entry);
    }
}

/// Handle one message arriving from a downstream agent connection.
/// How a tool call bears on the escalation router's mid-turn window.
enum ToolClass {
    /// Pure investigation (file read, read-only shell, read-only MCP tool) —
    /// counts toward the read-volume trigger and does NOT close the window.
    Investigation,
    /// A mutation / command / write — closes the mid-turn window.
    SideEffect,
    /// Not yet classifiable on this frame (e.g. an `execute` whose command
    /// isn't populated until a later frame); neither count nor close.
    Defer,
}

/// True when a shell command is (conservatively) read-only: its leading tool
/// is a known reader and it contains no redirection or mutating token. Errs
/// toward `false` (treat as a side effect) when unsure — the safe default.
fn is_read_only_command(cmd: &str) -> bool {
    let mut lc = cmd.trim().to_lowercase();
    if lc.is_empty() {
        return false;
    }
    // Strip harmless redirects (to /dev/null, stderr merges) so they don't trip
    // the `>` mutator check — `ls … 2>/dev/null || echo x` is read-only.
    for harmless in [
        "2>/dev/null",
        "2>>/dev/null",
        ">/dev/null",
        "1>/dev/null",
        "&>/dev/null",
        "2>&1",
        "1>&2",
    ] {
        lc = lc.replace(harmless, " ");
    }
    const MUTATORS: &[&str] = &[
        ">",
        ">>",
        " rm ",
        "rm -",
        "rmdir",
        " mv ",
        " cp ",
        "mkdir",
        "touch ",
        "sed -i",
        " tee ",
        "|tee",
        "install",
        "chmod",
        "chown",
        " ln ",
        " dd ",
        "kill ",
        "git commit",
        "git push",
        "git add",
        "git checkout",
        "git reset",
        "git merge",
        "git rebase",
        "git stash",
        "git apply",
        "git restore",
        "git switch",
        "git clean",
        "npm run",
        "cargo build",
        "cargo run",
        "cargo test",
        "cargo install",
        "cargo fix",
        "make ",
        "docker ",
        "curl -o",
        "wget ",
        "brew install",
        "pip install",
        "apply",
        "delete",
    ];
    if MUTATORS.iter().any(|m| lc.contains(m)) {
        return false;
    }
    const READERS: &[&str] = &[
        "ls",
        "cat",
        "grep",
        "rg",
        "find",
        "head",
        "tail",
        "pwd",
        "echo",
        "which",
        "wc",
        "stat",
        "tree",
        "file",
        "du",
        "df",
        "env",
        "date",
        "whoami",
        "hostname",
        "ps",
        "less",
        "more",
        "sed ",
        "awk ",
        "jq ",
        "sort",
        "uniq",
        "diff",
        "git status",
        "git log",
        "git diff",
        "git show",
        "git branch",
        "git remote",
        "git rev-parse",
        "git ls-files",
        "git blame",
        "git describe",
        "git config --get",
    ];
    READERS
        .iter()
        .any(|r| lc == *r || lc.starts_with(&format!("{r} ")))
}

/// True when an MCP / meta tool is (conservatively) read-only, judged by verbs
/// in its name. Write verbs win; then read verbs; unknown → false.
fn is_read_only_mcp(tool_name: &str) -> bool {
    let n = tool_name.to_lowercase();
    if n.contains("toolsearch") {
        return true;
    }
    const WRITE_VERBS: &[&str] = &[
        "send", "create", "update", "write", "delete", "post", "add", "remove", "schedule",
        "apply", "label", "draft", "canvas", "set_", "put",
    ];
    if WRITE_VERBS.iter().any(|w| n.contains(w)) {
        return false;
    }
    const READ_VERBS: &[&str] = &[
        "search", "read", "list", "get", "view", "fetch", "find", "lookup", "query", "describe",
    ];
    READ_VERBS.iter().any(|r| n.contains(r))
}

/// Classify a tool-call update for the escalation router.
fn classify_tool(update: &serde_json::Value) -> ToolClass {
    match update.get("kind").and_then(|k| k.as_str()).unwrap_or("") {
        "read" | "search" | "fetch" | "think" => ToolClass::Investigation,
        "execute" => match update
            .get("rawInput")
            .and_then(|r| r.get("command"))
            .and_then(|c| c.as_str())
        {
            Some(cmd) if is_read_only_command(cmd) => ToolClass::Investigation,
            Some(_) => ToolClass::SideEffect,
            None => ToolClass::Defer, // command not on this frame yet
        },
        "other" => {
            let name = update
                .get("_meta")
                .and_then(|m| m.get("claudeCode"))
                .and_then(|c| c.get("toolName"))
                .and_then(|n| n.as_str())
                .or_else(|| update.get("title").and_then(|t| t.as_str()))
                .unwrap_or("");
            if name.is_empty() {
                ToolClass::Defer
            } else if is_read_only_mcp(name) {
                ToolClass::Investigation
            } else {
                ToolClass::SideEffect
            }
        }
        "" => ToolClass::Defer,     // a status-only frame with no kind
        _ => ToolClass::SideEffect, // edit / delete / move / switch_mode …
    }
}

/// True when a tool_call frame is the adapter's built-in sub-agent tool (e.g.
/// Claude Code's `Task`) — as opposed to the router's own `delegate_task`. Uses
/// the authoritative `_meta.claudeCode.toolName` when present, else the title.
fn is_native_subagent_tool(update: &serde_json::Value) -> bool {
    let name = update
        .get("_meta")
        .and_then(|m| m.get("claudeCode"))
        .and_then(|c| c.get("toolName"))
        .and_then(|n| n.as_str())
        .or_else(|| update.get("title").and_then(|t| t.as_str()))
        .unwrap_or("")
        .trim()
        .to_lowercase();
    // The router's own tools must never match.
    if name.contains("delegate_task")
        || name.contains("delegate_followup")
        || name.contains("delegate_close")
    {
        return false;
    }
    name == "task"
        || name.starts_with("task ")
        || name.starts_with("task:")
        || name == "dispatch_agent"
        || name.contains("subagent")
        || name.contains("sub-agent")
        || name.contains("spawn_agent")
}

/// Count one tool call and, for an `escalation` session, request a mid-turn
/// escalation once the turn's tool-call count crosses the threshold — the
/// "grinding without finishing" signal. Robust to read/edit interleaving (it
/// ignores `turn_side_effect`); the handoff is a transcript continue.
fn note_tool_activity(shared: &Arc<Shared>, key: &ProcessKey, down_sid: &str, router_sid: &str) {
    let cfg = shared.cfg.routers.escalation.clone();
    let cross = shared
        .with_session(router_sid, |s| {
            s.turn_tool_calls += 1;
            s.strategy == StrategyKind::Escalation
                && cfg.escalate_after_tool_calls > 0
                && s.escalation_requested.is_none()
                && s.escalations_done < cfg.max_escalations
                && s.turn_tool_calls >= cfg.escalate_after_tool_calls
        })
        .unwrap_or(false);
    if !cross {
        return;
    }
    let Some(target) = escalation_target(shared, router_sid, cfg.escalation_path) else {
        return;
    };
    let n = cfg.escalate_after_tool_calls;
    shared.with_session(router_sid, |s| {
        s.escalation_requested = Some(SwitchRequest {
            target: target.clone(),
            reason: format!("escalation: {n}+ tool calls in one turn without finishing"),
        });
    });
    if let Some(conn) = shared.target_conn(key) {
        let _ = conn.send_notification(CancelNotification::new(down_sid.to_string()));
    }
    tracing::info!(session = router_sid, %target, "mid-turn escalation requested (tool-call volume)");
}

/// Count one failed tool call and, for an `escalation` session, request a
/// mid-turn escalation once failures reach the threshold — the "model is
/// struggling" signal. Unlike the read trigger, this is NOT gated on
/// `turn_side_effect`: failures happen mid-action, and the switch hands off a
/// transcript (the new model continues, it does not blindly replay).
fn note_tool_failure(shared: &Arc<Shared>, key: &ProcessKey, down_sid: &str, router_sid: &str) {
    let cfg = shared.cfg.routers.escalation.clone();
    let cross = shared
        .with_session(router_sid, |s| {
            s.turn_tool_failures += 1;
            s.strategy == StrategyKind::Escalation
                && cfg.escalate_after_tool_failures > 0
                && s.escalation_requested.is_none()
                && s.escalations_done < cfg.max_escalations
                && s.turn_tool_failures >= cfg.escalate_after_tool_failures
        })
        .unwrap_or(false);
    if !cross {
        return;
    }
    let Some(target) = escalation_target(shared, router_sid, cfg.escalation_path) else {
        return;
    };
    let n = cfg.escalate_after_tool_failures;
    shared.with_session(router_sid, |s| {
        s.escalation_requested = Some(SwitchRequest {
            target: target.clone(),
            reason: format!("escalation: {n}+ tool failures mid-turn — the model is struggling"),
        });
    });
    if let Some(conn) = shared.target_conn(key) {
        let _ = conn.send_notification(CancelNotification::new(down_sid.to_string()));
    }
    tracing::info!(session = router_sid, %target, "mid-turn escalation requested (tool failures)");
}

/// The escalation target for a session under the `escalation` router, given
/// the configured path. `ladder` = the next-higher-capability eligible
/// candidate; `leap` = the strongest. `None` if nothing more capable is
/// eligible.
fn escalation_target(
    shared: &Arc<Shared>,
    router_sid: &str,
    path: EscalationPath,
) -> Option<CandidateId> {
    let (class, current, current_q, excluded) = shared.with_session(router_sid, |s| {
        (
            s.task_class.unwrap_or(TaskClass::CodingGeneral),
            s.pin.as_ref().map(|p| p.candidate.clone()),
            s.pinned_quality,
            s.excluded.clone(),
        )
    })?;
    let current = current?;
    let mut pool = shared.eligible_views(&RequiredCaps::default(), class);
    pool.retain(|v| {
        v.id != current && !is_excluded(&v.id, &excluded) && v.quality > current_q + 0.05
    });
    let pick = match path {
        EscalationPath::Leap => pool.into_iter().max_by(|a, b| {
            a.quality
                .partial_cmp(&b.quality)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        EscalationPath::Ladder => pool.into_iter().min_by(|a, b| {
            a.quality
                .partial_cmp(&b.quality)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
    };
    pick.map(|v| v.id)
}

/// Count one investigation event for an `escalation` session and, if the
/// read-volume threshold is crossed while the turn is still side-effect-free,
/// request a mid-turn escalation: flag it and cancel the in-flight cheap turn
/// so the failover loop escalates + replays.
fn note_investigation(shared: &Arc<Shared>, key: &ProcessKey, down_sid: &str, router_sid: &str) {
    let cfg = shared.cfg.routers.escalation.clone();
    let cross = shared
        .with_session(router_sid, |s| {
            s.turn_reads += 1;
            s.strategy == StrategyKind::Escalation
                && cfg.escalate_before_side_effects
                && cfg.escalate_after_reads > 0
                && !s.turn_side_effect
                && s.escalation_requested.is_none()
                && s.capturing_summary.is_none()
                && s.escalations_done < cfg.max_escalations
                && s.turn_reads >= cfg.escalate_after_reads
        })
        .unwrap_or(false);
    if !cross {
        return;
    }
    let Some(target) = escalation_target(shared, router_sid, cfg.escalation_path) else {
        return;
    };
    let reads = cfg.escalate_after_reads;
    shared.with_session(router_sid, |s| {
        s.escalation_requested = Some(SwitchRequest {
            target: target.clone(),
            reason: format!(
                "escalation: {reads}+ investigation reads before any output — deeper than it looked"
            ),
        });
    });
    // Interrupt the in-flight cheap turn; the failover loop takes over.
    if let Some(conn) = shared.target_conn(key) {
        let _ = conn.send_notification(CancelNotification::new(down_sid.to_string()));
    }
    tracing::info!(session = router_sid, %target, "mid-turn escalation requested");
}

pub fn handle_downstream_dispatch(
    shared: &Arc<Shared>,
    key: &ProcessKey,
    message: Dispatch,
) -> Result<Handled<Dispatch>, AcpError> {
    // Responses route to their SentRequest via the default path.
    if matches!(message, Dispatch::Response(..)) {
        return Ok(Handled::No {
            message,
            retry: false,
        });
    }
    let Some(down_sid) = message.message().and_then(relay::session_id_of) else {
        return Ok(Handled::No {
            message,
            retry: false,
        });
    };
    let Some(route) = shared.route_for(key, &down_sid) else {
        // Unknown downstream session (e.g. probe session updates): drop
        // notifications, reject requests.
        return match message {
            Dispatch::Notification(_) => Ok(Handled::Yes),
            other => Ok(Handled::No {
                message: other,
                retry: false,
            }),
        };
    };
    let upstream = shared
        .upstream()
        .ok_or_else(|| AcpError::internal_error().data("upstream not connected"))?;

    match route {
        DownstreamRoute::Primary { router_sid } => match message {
            Dispatch::Notification(msg) => {
                // Mid-session switch: while the outgoing model writes its
                // handoff summary, buffer everything it emits instead of
                // relaying it — the client should not see the summary turn.
                if msg.method() == "session/update"
                    && let Some(buf) = shared
                        .with_session(&router_sid, |s| s.capturing_summary.clone())
                        .flatten()
                {
                    if let Some(text) = agent_chunk_text(msg.params()) {
                        buf.lock().unwrap().push_str(&text);
                    }
                    return Ok(Handled::Yes);
                }
                let mut fwd = relay::with_session_id(&msg, &router_sid)?;
                if msg.method() == "session/update" {
                    // Escalation router: classify each tool-call frame and drive
                    // mid-turn escalation. Investigation (file read, read-only
                    // shell/MCP) feeds the read-volume trigger and keeps the
                    // window open; anything mutating closes it; a failed tool
                    // call feeds the "model is struggling" trigger. Both the
                    // read count and the failure count feed struggle/auto-upgrade
                    // too (via `turn_tool_failures`).
                    if let Some(update) = msg.params().get("update") {
                        let su = update
                            .get("sessionUpdate")
                            .and_then(|k| k.as_str())
                            .unwrap_or("");
                        if su == "tool_call" || su == "tool_call_update" {
                            let tool_id = update
                                .get("toolCallId")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string();
                            // Total tool-call volume this turn: one increment
                            // per distinct tool (on its initial announcement).
                            if su == "tool_call" {
                                note_tool_activity(shared, key, &down_sid, &router_sid);
                                // Orchestration degradation: an orchestrating
                                // planner using the adapter's built-in sub-agent
                                // tool (Claude's `Task`) instead of the router's
                                // `delegate_task` — sub-work stays in-lineage and
                                // invisible to routing (no cross-lineage review,
                                // no per-subtask model selection). Record it and
                                // warn once per turn.
                                if is_native_subagent_tool(update)
                                    && shared
                                        .with_session(&router_sid, |s| s.orchestrating)
                                        .unwrap_or(false)
                                {
                                    shared
                                        .state
                                        .lock()
                                        .unwrap()
                                        .note_native_subagent(&router_sid);
                                    let warn = shared
                                        .with_session(&router_sid, |s| {
                                            if s.turn_native_subagent_warned {
                                                false
                                            } else {
                                                s.turn_native_subagent_warned = true;
                                                true
                                            }
                                        })
                                        .unwrap_or(false);
                                    if warn {
                                        notify_user(
                                            shared,
                                            &router_sid,
                                            "router-acp · orchestration degraded: the planner used \
                                             its built-in sub-agent tool instead of `delegate_task` \
                                             — sub-tasks stay in the planner's lineage and are \
                                             invisible to the router (no cross-lineage review, no \
                                             per-subtask model routing, no delegate rows recorded)",
                                        );
                                    }
                                }
                            }
                            match classify_tool(update) {
                                ToolClass::SideEffect => {
                                    shared.with_session(&router_sid, |s| s.turn_side_effect = true);
                                }
                                ToolClass::Investigation => {
                                    // A tool emits several frames; count it once.
                                    let fresh = tool_id.is_empty()
                                        || shared
                                            .with_session(&router_sid, |s| {
                                                s.turn_counted_tools.insert(tool_id.clone())
                                            })
                                            .unwrap_or(true);
                                    if fresh {
                                        note_investigation(shared, key, &down_sid, &router_sid);
                                    }
                                }
                                ToolClass::Defer => {}
                            }
                            if update.get("status").and_then(|s| s.as_str()) == Some("failed") {
                                let fresh = tool_id.is_empty()
                                    || shared
                                        .with_session(&router_sid, |s| {
                                            s.turn_failed_tools.insert(tool_id.clone())
                                        })
                                        .unwrap_or(true);
                                if fresh {
                                    note_tool_failure(shared, key, &down_sid, &router_sid);
                                }
                            }
                        }
                    }
                    // Ride the queued router disclosure on the model's first
                    // text chunk this turn (embeds it in the model's own
                    // message — the channel goose renders). Best effort:
                    // goose splits text runs on tool calls, so on agentic
                    // turns the disclosure shows early rather than beside the
                    // final answer.
                    if relay::is_agent_text_chunk(&fwd) {
                        let pending = shared
                            .with_session(&router_sid, |s| {
                                std::mem::take(&mut s.pending_disclosure)
                            })
                            .unwrap_or_default();
                        if !pending.is_empty() {
                            fwd = relay::prepend_agent_text(&fwd, &router_block(&pending))?;
                        }
                        if let Some(details) = shared.take_meta_disclosure(&router_sid) {
                            fwd = relay::with_router_meta(&fwd, details)?;
                        }
                    }
                    // Downstream output reached the client this turn: from
                    // here on a failover could duplicate side effects.
                    let chunk_text = agent_chunk_text(msg.params());
                    shared.with_session(&router_sid, |s| {
                        s.turn_saw_output = true;
                        if let Some(t) = &chunk_text {
                            s.turn_output.push_str(t);
                            // Streamed model output is a commit point: no more
                            // mid-turn escalation for the escalation router.
                            s.turn_side_effect = true;
                        }
                    });
                    // Adopt the downstream's conversation title when it
                    // names the session (diagnostics in the state file).
                    let update = msg.params().get("update");
                    if update
                        .and_then(|u| u.get("sessionUpdate"))
                        .and_then(|k| k.as_str())
                        == Some("session_info_update")
                        && let Some(title) =
                            update.and_then(|u| u.get("title")).and_then(|t| t.as_str())
                    {
                        shared.state.lock().unwrap().set_title(&router_sid, title);
                    }
                    // Log tool calls / usage updates for observability.
                    log_downstream_event(shared, &router_sid, msg.params());
                }
                upstream.send_notification(fwd)?;
                Ok(Handled::Yes)
            }
            Dispatch::Request(msg, responder) => {
                // Log client-directed callbacks (permission, fs, terminal).
                let method = msg.method().to_string();
                // Escalation router: a file read is investigation; a write or
                // a terminal command is a side effect that locks out mid-turn
                // escalation.
                if method == "fs/read_text_file" {
                    note_investigation(shared, key, &down_sid, &router_sid);
                } else if method == "fs/write_text_file" || method.starts_with("terminal/") {
                    shared.with_session(&router_sid, |s| s.turn_side_effect = true);
                }
                if method.starts_with("fs/")
                    || method.starts_with("terminal/")
                    || method == "session/request_permission"
                {
                    shared.state.lock().unwrap().log(
                        &router_sid,
                        &crate::state::LogEntry {
                            kind: method.replace('/', "_"),
                            role: "tool".to_string(),
                            summary: method.clone(),
                            tokens_estimated: true,
                            ..Default::default()
                        },
                    );
                }
                let fwd = relay::with_session_id(&msg, &router_sid)?;
                upstream.send_request(fwd).forward_response_to(responder)?;
                Ok(Handled::Yes)
            }
            Dispatch::Response(..) => unreachable!("responses handled above"),
        },
        DownstreamRoute::Delegate {
            parent_router_sid,
            capture,
        } => match message {
            Dispatch::Notification(msg) => {
                // Sub-agent transcript streaming is not interleaved into the
                // parent transcript; capture agent text for the tool result.
                if msg.method() == "session/update" {
                    if let Some(text) = agent_chunk_text(msg.params()) {
                        capture.lock().unwrap().push_str(&text);
                        let sub_sid = format!("{parent_router_sid}::delegate-{down_sid}");
                        shared.state.lock().unwrap().log(
                            &sub_sid,
                            &crate::state::LogEntry {
                                kind: "agent_progress".to_string(),
                                role: "agent".to_string(),
                                summary: text,
                                ..Default::default()
                            },
                        );
                    }
                    let sub_sid = format!("{parent_router_sid}::delegate-{down_sid}");
                    log_downstream_event(shared, &sub_sid, msg.params());
                    // Attribute the delegate's cost/context to its own state row
                    // (id mirrors run_delegate_task's `sub_sid`).
                    if let Some(update) = msg.params().get("update")
                        && update.get("sessionUpdate").and_then(|k| k.as_str())
                            == Some("usage_update")
                    {
                        let sub_sid = format!("{parent_router_sid}::delegate-{down_sid}");
                        let st = shared.state.lock().unwrap();
                        if let Some(used) = update.get("used").and_then(|u| u.as_u64()) {
                            st.set_context_used(&sub_sid, used);
                        }
                        if let Some(cost) = update
                            .get("cost")
                            .and_then(|c| c.get("amount"))
                            .and_then(|a| a.as_f64())
                        {
                            st.set_cost_usd(&sub_sid, cost);
                        }
                    }
                }
                Ok(Handled::Yes)
            }
            Dispatch::Request(msg, responder) => {
                // Permission/fs/terminal callbacks go live to the client
                // under the parent router session id.
                let method = msg.method().to_string();
                // Permission callbacks are forwarded to the parent relay for
                // silent compatibility handling, but never persisted as
                // visible delegate activity. Dangerous mode should make them
                // exceptional transport plumbing, not conversation content.
                if method.starts_with("fs/") || method.starts_with("terminal/") {
                    let sub_sid = format!("{parent_router_sid}::delegate-{down_sid}");
                    shared.state.lock().unwrap().log(
                        &sub_sid,
                        &crate::state::LogEntry {
                            kind: method.replace('/', "_"),
                            role: "tool".to_string(),
                            summary: method,
                            detail: Some(msg.params().clone()),
                            tokens_estimated: true,
                            ..Default::default()
                        },
                    );
                }
                let fwd = relay::with_session_id(&msg, &parent_router_sid)?;
                upstream.send_request(fwd).forward_response_to(responder)?;
                Ok(Handled::Yes)
            }
            Dispatch::Response(..) => unreachable!("responses handled above"),
        },
    }
}

/// Relay a request to a downstream connection, answering the upstream
/// responder with the result.
///
/// This must NOT use `forward_response_to`: that spawns the consuming task
/// on the downstream connection's task actor, and if the downstream process
/// dies mid-request the task is dropped and the upstream request would hang
/// forever. Instead the wait runs on the upstream connection, where a dead
/// downstream simply surfaces as an error result.
pub fn relay_request_to_downstream<Req>(
    shared: &Arc<Shared>,
    conn: ConnectionTo<AgentPeer>,
    req: Req,
    responder: Responder<Req::Response>,
) -> Result<(), AcpError>
where
    Req: agent_client_protocol::JsonRpcRequest + 'static,
    Req::Response: Send + 'static,
{
    let upstream = shared
        .upstream()
        .ok_or_else(|| AcpError::internal_error().data("upstream not connected"))?;
    upstream.spawn(async move {
        let sent = conn
            .send_request(req)
            .forward_cancellation_from(responder.cancellation());
        let result = sent.block_task().await;
        let _ = responder.respond_with_result(result);
        Ok(())
    })
}

/// Resolve a client-requested mode id against a downstream's advertised
/// modes: the agent's configured `mode_map` wins, then an exact id match.
/// `None` means the downstream has no equivalent mode.
pub(crate) fn resolve_mode_id(
    shared: &Arc<Shared>,
    agent_name: &str,
    requested: &str,
    available: &[String],
) -> Option<String> {
    let mapped = shared
        .cfg
        .agents
        .iter()
        .find(|a| a.name == agent_name)
        .and_then(|a| a.mode_map.get(requested))
        .cloned();
    if let Some(mapped) = mapped {
        if available.iter().any(|m| m == &mapped) {
            return Some(mapped);
        }
        tracing::warn!(
            agent = agent_name,
            requested,
            mapped,
            ?available,
            "mode_map target is not advertised by the downstream; ignoring the mapping"
        );
    }
    available
        .iter()
        .any(|m| m == requested)
        .then(|| requested.to_string())
}

// ----------------------------------------------------------------------
// Opening downstream sessions (shared by pinning and delegation)
// ----------------------------------------------------------------------

pub struct OpenedSession {
    pub conn: ConnectionTo<AgentPeer>,
    pub process_key: ProcessKey,
    pub downstream_sid: String,
    /// Session modes advertised by the downstream at creation.
    pub modes: Option<agent_client_protocol::schema::v1::SessionModeState>,
}

/// Create a downstream session for `candidate`, verify model selection, and
/// register the routing entry. On any failure the partial session is closed
/// (best effort) and unregistered.
pub async fn open_downstream_session(
    shared: &Arc<Shared>,
    candidate: &CandidateId,
    cwd: PathBuf,
    additional_directories: Vec<PathBuf>,
    mcp_servers: Vec<McpServer>,
    route: DownstreamRoute,
) -> Result<OpenedSession, AcpError> {
    let runtime = shared
        .candidate_runtime(candidate)
        .ok_or_else(|| AcpError::invalid_params().data(format!("unknown candidate {candidate}")))?;
    let key = runtime.process_key.clone();
    let conn = shared.target_conn(&key).ok_or_else(|| {
        AcpError::internal_error().data(format!("no live downstream process for {candidate}"))
    })?;
    let (selection, config_id) = {
        let targets = shared.targets.lock().unwrap();
        let t = targets
            .get(&key)
            .ok_or_else(|| AcpError::internal_error().data("target vanished"))?;
        (t.spec.selection.clone(), t.model_config_id.clone())
    };

    let new_req = NewSessionRequest::new(cwd)
        .additional_directories(additional_directories)
        .mcp_servers(mcp_servers);

    // Register the sid route inside the response callback: the downstream
    // dispatch loop waits for this callback (ack), so no session/update can
    // race past before the mapping exists.
    let (tx, rx) = futures::channel::oneshot::channel();
    let reg_shared = shared.clone();
    let reg_key = key.clone();
    conn.send_request(new_req)
        .on_receiving_result(move |result| {
            let result: Result<NewSessionResponse, AcpError> = result;
            async move {
                if let Ok(resp) = &result {
                    reg_shared.register_route(&reg_key, &sid_str(&resp.session_id), route);
                }
                let _ = tx.send(result);
                Ok(())
            }
        })?;
    let timeout = std::time::Duration::from_millis(shared.cfg.probe_timeout_ms);
    let resp = tokio::time::timeout(timeout, rx)
        .await
        .map_err(|_| {
            AcpError::internal_error().data(format!("session/new on {candidate} timed out"))
        })?
        .map_err(|_| AcpError::internal_error().data("downstream connection closed"))??;
    let downstream_sid = sid_str(&resp.session_id);
    let modes = resp.modes.clone();

    // Apply and verify model selection for config-option targets. The
    // set_config_option response is authoritative; no notification needed.
    if selection == SelectionKind::ConfigOption {
        let Some(config_id) = config_id else {
            cleanup_failed_session(shared, &key, &conn, &downstream_sid);
            return Err(AcpError::internal_error()
                .data(format!("no model config option discovered for {candidate}")));
        };
        let set_req = SetSessionConfigOptionRequest::new(
            resp.session_id.clone(),
            config_id.clone(),
            SessionConfigOptionValue::value_id(candidate.model.clone()),
        );
        match conn.send_request(set_req).block_task().await {
            Ok(set_resp) => {
                if let Err(msg) =
                    verify_model_selected(&set_resp.config_options, &config_id, &candidate.model)
                {
                    cleanup_failed_session(shared, &key, &conn, &downstream_sid);
                    return Err(AcpError::internal_error()
                        .data(format!("model verification failed for {candidate}: {msg}")));
                }
            }
            Err(err) => {
                cleanup_failed_session(shared, &key, &conn, &downstream_sid);
                return Err(err);
            }
        }
    }

    Ok(OpenedSession {
        conn,
        process_key: key,
        downstream_sid,
        modes,
    })
}

fn cleanup_failed_session(
    shared: &Arc<Shared>,
    key: &ProcessKey,
    conn: &ConnectionTo<AgentPeer>,
    downstream_sid: &str,
) {
    shared.unregister_route(key, downstream_sid);
    let supports_close = shared
        .target_init(key)
        .map(|i| i.agent_capabilities.session_capabilities.close.is_some())
        .unwrap_or(false);
    if supports_close {
        conn.send_request(CloseSessionRequest::new(downstream_sid.to_string()))
            .detach();
    }
}

/// Best-effort close of a downstream session.
pub fn close_downstream_session(shared: &Arc<Shared>, key: &ProcessKey, downstream_sid: &str) {
    shared.unregister_route(key, downstream_sid);
    if let Some(conn) = shared.target_conn(key) {
        let supports_close = shared
            .target_init(key)
            .map(|i| i.agent_capabilities.session_capabilities.close.is_some())
            .unwrap_or(false);
        if supports_close {
            conn.send_request(CloseSessionRequest::new(downstream_sid.to_string()))
                .detach();
        }
    }
}

/// Best-effort `(branch, sha)` for a working directory that is a git repo.
/// Returns `(None, None)` when git is unavailable or `cwd` isn't a repo.
fn git_head(cwd: &std::path::Path) -> (Option<String>, Option<String>) {
    let run = |args: &[&str]| -> Option<String> {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        (!s.is_empty()).then_some(s)
    };
    (
        run(&["rev-parse", "--abbrev-ref", "HEAD"]),
        run(&["rev-parse", "HEAD"]),
    )
}

/// Close any delegate sub-sessions kept open (`keep_open`) under a parent
/// session, when that parent is closed or deleted, so they don't leak.
pub fn close_live_delegates_for(shared: &Arc<Shared>, router_sid: &str) {
    let orphans: Vec<LiveDelegate> = {
        let mut live = shared.live_delegates.lock().unwrap();
        let ids: Vec<String> = live
            .iter()
            .filter(|(_, d)| d.parent_sid == router_sid)
            .map(|(id, _)| id.clone())
            .collect();
        ids.into_iter().filter_map(|id| live.remove(&id)).collect()
    };
    for d in orphans {
        close_downstream_session(shared, &d.process_key, &d.downstream_sid);
    }
}

// ----------------------------------------------------------------------
// First-prompt routing (lazy pin)
// ----------------------------------------------------------------------

/// Routing directives embedded on their own line in a prompt:
/// `[router: candidate=claude/sonnet]`, `[router: strategy=pareto-code]`,
/// `[router: exclude=claude|codex/gpt-5.4-mini]` (patterns separated by `|`;
/// each is an agent name or a candidate glob). Keys combine with commas.
///
/// This is how recipe/script authors steer routing from clients that cannot
/// set ACP session config options (e.g. the goose CLI). The directive line
/// is stripped before classification and never reaches the downstream model.
/// It is matched on any line of the first text block, not just the first —
/// goose prepends a `<turn-context>…</turn-context>` preamble to prompts, so
/// requiring line 1 would miss it.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct PromptDirectives {
    pub candidate: Option<CandidateId>,
    pub prefer: Option<CandidateId>,
    /// `[router: switch=agent/model]` — switch a pinned session to this
    /// candidate mid-conversation (summarize + re-pin). Pre-pin it behaves
    /// like `candidate=`.
    pub switch: Option<CandidateId>,
    pub strategy: Option<StrategyKind>,
    pub exclude: Vec<String>,
    pub label: Option<String>,
}

/// Parse (and strip) a routing directive from the prompt.
/// Returns `Ok(None)` when no directive is present; `Err` describes an
/// invalid directive (the prompt fails loudly so recipes get fixed).
///
/// The directive is matched on any line of ANY text block — goose both wraps
/// prompts in a `<turn-context>` preamble AND may split it into a separate
/// content block, so neither "line 1" nor "first block" is safe. Only the
/// directive line is removed; the surrounding text (preamble + task) is kept.
pub fn parse_prompt_directives(
    prompt: &[ContentBlock],
) -> Result<Option<(PromptDirectives, Vec<ContentBlock>)>, String> {
    // Locate the `[router:` directive within any text block, then
    // bracket-match to its closing `]`. Depth tracking means nested brackets
    // in model ids (`opus[1m]`) are handled, and the directive may sit
    // anywhere — on its own line, after a `<turn-context>` preamble, or inline
    // with task text before/after it on the same line.
    fn find_ci_ascii(haystack: &str, needle: &str) -> Option<usize> {
        let (hb, nb) = (haystack.as_bytes(), needle.as_bytes());
        if nb.is_empty() || hb.len() < nb.len() {
            return None;
        }
        (0..=hb.len() - nb.len()).find(|&i| hb[i..i + nb.len()].eq_ignore_ascii_case(nb))
    }
    const OPEN: &str = "[router:";
    let mut found: Option<(usize, String, usize)> = None; // (block_idx, text, start)
    for (i, b) in prompt.iter().enumerate() {
        if let ContentBlock::Text(t) = b
            && let Some(pos) = find_ci_ascii(&t.text, OPEN)
        {
            found = Some((i, t.text.clone(), pos));
            break;
        }
    }
    let Some((block_idx, text, start)) = found else {
        return Ok(None);
    };
    // Scan from the opening `[` for the matching `]`, tracking bracket depth.
    let mut depth = 0usize;
    let mut end = None;
    for (idx, &c) in text.as_bytes().iter().enumerate().skip(start) {
        match c {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(idx);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(end) = end else {
        return Err("routing directive is missing its closing `]`".to_string());
    };
    let inner = &text[start + OPEN.len()..end];

    let mut directives = PromptDirectives::default();
    for pair in inner.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let Some((key, value)) = pair.split_once('=') else {
            return Err(format!(
                "routing directive `{pair}` is not key=value (keys: candidate, strategy, exclude)"
            ));
        };
        let value = value.trim();
        match key.trim().to_lowercase().as_str() {
            "candidate" => {
                directives.candidate = Some(CandidateId::parse(value).ok_or_else(|| {
                    format!("directive candidate `{value}` must have the form `agent/model-id`")
                })?);
            }
            "prefer" => {
                directives.prefer = Some(CandidateId::parse(value).ok_or_else(|| {
                    format!("directive prefer `{value}` must have the form `agent/model-id`")
                })?);
            }
            "switch" => {
                directives.switch = Some(CandidateId::parse(value).ok_or_else(|| {
                    format!("directive switch `{value}` must have the form `agent/model-id`")
                })?);
            }
            "strategy" => {
                directives.strategy = Some(StrategyKind::parse(value).ok_or_else(|| {
                    format!("directive strategy `{value}` must be auto, pareto-code, or static")
                })?);
            }
            "exclude" => {
                directives.exclude.extend(
                    value
                        .split('|')
                        .map(|p| p.trim().to_string())
                        .filter(|p| !p.is_empty()),
                );
            }
            "label" => {
                if !value.is_empty() {
                    directives.label = Some(value.to_string());
                }
            }
            other => {
                return Err(format!(
                    "unknown routing directive key `{other}` \
                     (keys: candidate, prefer, switch, strategy, exclude, label)"
                ));
            }
        }
    }

    // Strip just the `[router:…]` span; keep any surrounding preamble/task.
    // Trim the outer whitespace the removed span left behind (leading newline
    // when the directive had its own line, leading space when inline).
    let mut remainder = String::with_capacity(text.len());
    remainder.push_str(&text[..start]);
    remainder.push_str(&text[end + 1..]);
    let remainder = remainder.trim();
    let mut stripped: Vec<ContentBlock> = prompt.to_vec();
    if remainder.is_empty() {
        stripped.remove(block_idx);
    } else {
        stripped[block_idx] = ContentBlock::from(remainder.to_string());
    }
    // A directive-only prompt (empty remainder) is allowed — e.g. a bare
    // `[router: switch=…]`. The caller decides: post-pin it synthesizes a
    // continuation, pre-pin it errors (nothing to route/classify).
    Ok(Some((directives, stripped)))
}

/// True when a prompt carries no meaningful task content — an empty block
/// list, or only blank text blocks (e.g. after stripping a directive-only
/// prompt). A non-text block (image, resource) counts as content.
fn prompt_is_empty(prompt: &[ContentBlock]) -> bool {
    prompt
        .iter()
        .all(|b| matches!(b, ContentBlock::Text(t) if t.text.trim().is_empty()))
}

/// True when a candidate matches any exclusion pattern (an agent name or a
/// candidate-id glob).
fn is_excluded(candidate: &CandidateId, patterns: &[String]) -> bool {
    let full = candidate.to_string();
    patterns
        .iter()
        .any(|p| p.eq_ignore_ascii_case(&candidate.agent) || crate::candidate::glob_match(p, &full))
}

enum PinOutcome {
    Pinned,
    Cancelled,
}

/// Send a visible status line to the client for a session. Used for routing
/// disclosures, cordon notices, and failover events.
/// Render router notice line(s) as a markdown blockquote block that flushes
/// cleanly (leading marker per line, trailing blank line).
pub fn router_block(lines: &[String]) -> String {
    let mut out = String::new();
    for line in lines {
        for sub in line.split('\n') {
            out.push_str("> ");
            out.push_str(sub);
            out.push('\n');
        }
    }
    out.push('\n'); // paragraph break flushes the block before model text
    out
}

/// Queue router notice line(s) to ride the model's next response chunk.
///
/// We do NOT send a separate `session/update` for router notices: goose (and
/// similar clients) drop router-originated interim updates — a preceding
/// `agent_message_chunk` collapses into the model's message and is lost, a
/// thought chunk and even a completed tool call never surface in goose's
/// output (verified against goose 1.41 across interactive, `run`, and
/// json/stream-json modes). The reliable channel is the model's OWN response:
/// the queued lines are **prepended to the first `agent_message_chunk`** the
/// downstream emits this turn (see [`handle_downstream_dispatch`]), so they
/// render as part of the exact message the client displays. If the turn ends
/// with no text produced, the queue is flushed as a standalone final chunk
/// (see [`flush_pending_disclosure`]).
pub fn notify_user(shared: &Arc<Shared>, router_sid: &str, text: impl Into<String>) {
    queue_notice(shared, router_sid, vec![text.into()]);
}

pub fn queue_notice(shared: &Arc<Shared>, router_sid: &str, lines: Vec<String>) {
    shared.with_session(router_sid, |s| s.pending_disclosure.extend(lines));
}

/// Derive `(input, output, estimated)` token counts for a completed turn.
/// Uses the downstream's reported `usage` when present (the
/// `unstable_end_turn_token_usage` ACP capability); otherwise estimates the
/// output from the collected text (input unknown → 0, flagged estimated).
pub fn turn_tokens(resp: &PromptResponse, output_text: &str) -> (u64, u64, bool) {
    if let Some(usage) = &resp.usage {
        (usage.input_tokens, usage.output_tokens, false)
    } else {
        (0, crate::state::estimate_tokens(output_text), true)
    }
}

/// Emit the queued router disclosure as a trailing `agent_message_chunk`
/// after the model's final answer.
///
/// Placement matters: clients like goose reset their text run on tool calls,
/// so a disclosure prepended to the model's FIRST chunk gets orphaned into an
/// early message that `goose run` discards (it prints only the final
/// message) and that interactive mode buries above the tool activity. The
/// model's LAST text run is the answer the client always shows, and a
/// trailing text chunk with no intervening tool call is appended to it — so
/// the disclosure renders at the end of the answer. Called right before the
/// prompt response is returned.
pub fn flush_pending_disclosure(shared: &Arc<Shared>, router_sid: &str) {
    let lines = shared
        .with_session(router_sid, |s| std::mem::take(&mut s.pending_disclosure))
        .unwrap_or_default();
    if lines.is_empty() {
        return;
    }
    if let Some(upstream) = shared.upstream() {
        let notif = SessionNotification::new(
            router_sid.to_string(),
            SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(router_block(
                &lines,
            )))),
        );
        let _ = upstream.send_notification(notif);
    }
}

/// Record a downstream failure against its candidate/agent and return a
/// human-readable reason for user-facing notices.
///
/// Token/usage limits cordon the whole agent until the reset time the model
/// reported (or `headroom.cordon_default_secs` when it reported none).
/// Outages count toward candidate quarantine; process death is already
/// tracked by the connection watchdog.
pub(crate) fn apply_failure(
    shared: &Arc<Shared>,
    candidate: &CandidateId,
    err: &AcpError,
    class: &crate::limits::FailureClass,
) -> String {
    use crate::limits::{FailureClass, humanize};
    match class {
        FailureClass::RateLimited { retry_after } => {
            let effective = retry_after.unwrap_or(std::time::Duration::from_secs(
                shared.cfg.headroom.cordon_default_secs,
            ));
            let reason = if retry_after.is_some() {
                format!(
                    "token/usage limit (model reports reset in {})",
                    humanize(effective)
                )
            } else {
                format!(
                    "token/usage limit (no reset time reported; retrying in {})",
                    humanize(effective)
                )
            };
            shared.headroom.lock().unwrap().cordon(
                &candidate.agent,
                Some(effective),
                reason.clone(),
            );
            tracing::warn!(
                agent = candidate.agent,
                cordon_secs = effective.as_secs(),
                "agent cordoned by token/usage limit"
            );
            reason
        }
        FailureClass::Outage => {
            shared
                .headroom
                .lock()
                .unwrap()
                .record_pre_prompt_failure(candidate);
            let mut msg = format!("{err}");
            msg.truncate(160);
            format!("outage ({msg})")
        }
        FailureClass::Other => {
            let mut msg = format!("{err}");
            msg.truncate(160);
            msg
        }
    }
}

/// Respawn downstream processes that died, subject to the failover respawn
/// cooldown. Called before routing decisions so an agent that recovered from
/// an outage rejoins the candidate pool.
async fn revive_dead_targets(shared: &Arc<Shared>) {
    let cooldown = std::time::Duration::from_secs(shared.cfg.failover.respawn_cooldown_secs);
    let now = std::time::Instant::now();
    let keys: Vec<ProcessKey> = {
        let mut targets = shared.targets.lock().unwrap();
        targets
            .iter_mut()
            .filter(|(_, t)| t.conn.is_none() && t.dead.is_some())
            .filter(|(_, t)| {
                t.last_respawn
                    .map(|at| now.duration_since(at) >= cooldown)
                    .unwrap_or(true)
            })
            .map(|(k, t)| {
                t.last_respawn = Some(now);
                k.clone()
            })
            .collect()
    };
    for key in keys {
        tracing::info!(target = %key, "attempting downstream respawn after outage");
        match start_downstream(shared, &key).await {
            Ok(()) => {
                probe_target(shared, &key).await;
            }
            Err(err) => {
                tracing::warn!(target = %key, %err, "downstream respawn failed");
            }
        }
    }
}

/// Pick a candidate for this session, open + verify its downstream session,
/// commit the pin, apply any deferred/previous session mode, persist, and
/// disclose the decision (and every skipped candidate) to the user.
///
/// `exclude` removes the just-failed candidate during a failover re-pin.
async fn pin_session(
    shared: &Arc<Shared>,
    router_sid: &str,
    prompt: &[ContentBlock],
    cancellation: &RequestCancellation,
    exclude: Option<&CandidateId>,
    is_failover: bool,
) -> Result<PinOutcome, AcpError> {
    let (cwd, dirs, client_mcp, strategy, override_, run_label) = shared
        .with_session(router_sid, |s| {
            (
                s.cwd.clone(),
                s.additional_directories.clone(),
                s.mcp_servers.clone(),
                s.strategy,
                s.candidate_override.clone(),
                s.run_label.clone(),
            )
        })
        .ok_or_else(|| AcpError::invalid_params().data("unknown session"))?;
    // pin_session only ever creates primary (top-level) sessions; delegated
    // sub-agent rows are written by the delegation path with a parent link.
    let parent_session_id: Option<String> = None;
    let session_kind = "primary";

    // 0. Give agents that died a chance to come back before routing.
    revive_dead_targets(shared).await;

    // 1. Build the route context from the first prompt.
    let cwd_langs = cwd_language_fingerprint(&shared.rules, &cwd);
    let input = ClassifyInput::from_prompt(prompt, cwd_langs);
    let profile = classify(&shared.cfg.classifier, &shared.rules, &input).await;
    let required = RequiredCaps::from_prompt(prompt);
    // A failover must not keep re-selecting the failed candidate even when
    // it was explicitly pinned via router.candidate.
    let override_ = match (&override_, exclude) {
        (Some(o), Some(x)) if o == x => None,
        _ => override_,
    };
    // An explicit pin to a usage-cordoned candidate is not honored: drop the
    // override so routing picks the best non-cordoned candidate, and record the
    // redirect for the disclosure.
    let mut cordon_redirect: Option<(CandidateId, String, String)> = None;
    let override_ = match override_ {
        Some(cand) => {
            let cordon = shared.headroom.lock().unwrap().usage_cordon(&cand).cloned();
            match cordon {
                Some(c) => {
                    cordon_redirect = Some((cand, c.reason, c.resets_at_rfc3339));
                    None
                }
                None => Some(cand),
            }
        }
        None => None,
    };
    let ctx = RouteContext {
        profile: profile.clone(),
        required_caps: required,
        explicit_candidate: override_.clone(),
    };

    // Cordons active right now (shown to the user so exclusions are visible).
    let cordons: Vec<(String, std::time::Duration, String)> =
        shared.headroom.lock().unwrap().active_cordons();

    // 2. Filter candidates.
    let excluded_patterns = shared
        .with_session(router_sid, |s| s.excluded.clone())
        .unwrap_or_default();
    let mut pool = shared.eligible_views(&required, profile.class);
    if let Some(exclude) = exclude {
        pool.retain(|v| &v.id != exclude);
    }
    if !excluded_patterns.is_empty() {
        pool.retain(|v| !is_excluded(&v.id, &excluded_patterns));
    }
    // All-cordoned "least-bad" fallback: if the pool is empty only because every
    // candidate is usage-cordoned, route to the one whose cordon resets soonest
    // rather than failing the turn.
    let mut all_cordoned_fallback: Option<String> = None;
    if pool.is_empty() {
        let mut relaxed = shared.eligible_views_relaxed(&required, profile.class);
        if let Some(exclude) = exclude {
            relaxed.retain(|v| &v.id != exclude);
        }
        if !excluded_patterns.is_empty() {
            relaxed.retain(|v| !is_excluded(&v.id, &excluded_patterns));
        }
        let soonest = {
            let hr = shared.headroom.lock().unwrap();
            relaxed
                .into_iter()
                .filter_map(|v| {
                    hr.usage_cordon(&v.id).map(|c| {
                        (
                            c.resets_at,
                            c.reason.clone(),
                            c.resets_at_rfc3339.clone(),
                            v,
                        )
                    })
                })
                .min_by_key(|(resets, ..)| *resets)
        };
        if let Some((_, reason, resets_str, view)) = soonest {
            all_cordoned_fallback = Some(format!("{} ({reason}, resets {resets_str})", view.id));
            pool = vec![view];
        }
    }
    if pool.is_empty() {
        return Err(if shared.has_auth_pending() {
            AcpError::auth_required()
                .data("no routeable candidates yet; authenticate a downstream agent first")
        } else if !cordons.is_empty() {
            let list: Vec<String> = cordons
                .iter()
                .map(|(agent, remaining, reason)| {
                    format!(
                        "{agent}: {reason} ({} left)",
                        crate::limits::humanize(*remaining)
                    )
                })
                .collect();
            AcpError::internal_error().data(format!(
                "all agents are cordoned by token/usage limits — {}",
                list.join("; ")
            ))
        } else if !required.is_empty() {
            AcpError::invalid_params().data(
                "no routeable candidate supports the capabilities this prompt requires \
                 (image/audio/embedded context)",
            )
        } else {
            AcpError::internal_error().data("no routeable candidates available")
        });
    }

    // 3. Run the strategy for a full ranked fallback chain. An explicit
    //    `router.candidate` makes routing static for this session.
    let strategy_kind = if override_.is_some() {
        StrategyKind::Static
    } else {
        strategy
    };
    let mut ranked = make_strategy(strategy_kind, &shared.cfg)
        .rank(&ctx, &pool)
        .map_err(|e| AcpError::invalid_params().data(e.to_string()))?;

    // Soft preference (`[router: prefer=...]`): if the preferred candidate
    // survived filtering, move it to the front of the fallback chain; if it
    // didn't (cordoned/down/excluded), the normal ranking already handles
    // the fallback — no error, unlike a hard `candidate=` pin.
    let preferred = shared
        .with_session(router_sid, |s| s.preferred_candidate.clone())
        .flatten();
    if let Some(pref) = &preferred
        && let Some(pos) = ranked.iter().position(|r| &r.candidate == pref)
    {
        let mut rc = ranked.remove(pos);
        rc.note = Some(match rc.note.take() {
            Some(n) => format!("preferred candidate; {n}"),
            None => "preferred candidate".to_string(),
        });
        ranked.insert(0, rc);
    }

    // 4. Walk the ranked list until a candidate opens and verifies,
    //    remembering why each earlier candidate was skipped.
    let mut skipped: Vec<(CandidateId, String)> = Vec::new();
    let mut last_err: Option<AcpError> = None;
    for rc in ranked {
        if cancellation.is_cancelled()
            || shared
                .with_session(router_sid, |s| s.cancelled)
                .unwrap_or(false)
        {
            return Ok(PinOutcome::Cancelled);
        }
        let candidate = rc.candidate.clone();
        let mcp_servers = mcp_servers_for_pin(shared, router_sid, &candidate, &client_mcp);
        match open_downstream_session(
            shared,
            &candidate,
            cwd.clone(),
            dirs.clone(),
            mcp_servers,
            DownstreamRoute::Primary {
                router_sid: router_sid.to_string(),
            },
        )
        .await
        {
            Ok(opened) => {
                // 5-6. Commit the pin only now; persist.
                let available_modes: Vec<String> = opened
                    .modes
                    .as_ref()
                    .map(|m| {
                        m.available_modes
                            .iter()
                            .map(|mode| mode.id.0.to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                let pin_quality = shared.scores.lookup(&candidate).quality(profile.class);
                let mode_to_apply = shared
                    .with_session(router_sid, |s| {
                        s.pin = Some(PinInfo {
                            candidate: candidate.clone(),
                            process_key: opened.process_key.clone(),
                            downstream_sid: opened.downstream_sid.clone(),
                            available_modes: available_modes.clone(),
                        });
                        // Confidence baseline for this pin; reset struggle.
                        s.pinned_quality = pin_quality;
                        s.task_class = Some(profile.class);
                        s.struggle = 0.0;
                        // Deferred pre-pin mode wins; on failover re-apply
                        // whatever the client had set for this session.
                        s.pending_mode.take().or_else(|| s.applied_mode.clone())
                    })
                    .flatten();

                // Apply the session mode (deferred pre-pin, or carried across
                // a failover). Best effort: an unsupported mode leaves the
                // downstream in its default.
                if let Some(requested) = mode_to_apply {
                    match resolve_mode_id(shared, &candidate.agent, &requested, &available_modes) {
                        Some(mode_id) => {
                            let set = SetSessionModeRequest::new(
                                opened.downstream_sid.clone(),
                                mode_id.clone(),
                            );
                            match opened.conn.send_request(set).block_task().await {
                                Ok(_) => {
                                    shared.with_session(router_sid, |s| {
                                        s.applied_mode = Some(requested.clone());
                                    });
                                    tracing::info!(
                                        session = router_sid,
                                        requested,
                                        applied = mode_id,
                                        "session mode applied to pinned candidate"
                                    );
                                }
                                Err(err) => tracing::warn!(
                                    session = router_sid,
                                    requested,
                                    %err,
                                    "session mode rejected by downstream; continuing in its \
                                     default mode"
                                ),
                            }
                        }
                        None => tracing::warn!(
                            session = router_sid,
                            requested,
                            ?available_modes,
                            "pinned candidate has no matching session mode; continuing in its \
                             default mode (declare agents[].mode_map to translate)"
                        ),
                    }
                }
                // Title: opening words of the first prompt; replaced later by
                // the downstream's own session_info_update title if one comes.
                let prompt_title = prompt.iter().find_map(|b| match b {
                    ContentBlock::Text(t) => {
                        let one_line = t.text.split_whitespace().collect::<Vec<_>>().join(" ");
                        let mut title: String = one_line.chars().take(80).collect();
                        if one_line.chars().count() > 80 {
                            title.push('…');
                        }
                        (!title.is_empty()).then_some(title)
                    }
                    _ => None,
                });
                shared
                    .headroom
                    .lock()
                    .unwrap()
                    .record_session(&candidate.agent);
                tracing::info!(
                    session = router_sid,
                    candidate = %candidate,
                    strategy = strategy_kind.as_str(),
                    class = profile.class.as_str(),
                    complexity = profile.complexity,
                    failover = is_failover,
                    "session pinned"
                );

                // 7. Routing disclosure: what was chosen and WHY, plus every
                //    candidate that was skipped along the way.
                let skipped_json: Vec<serde_json::Value> = skipped
                    .iter()
                    .map(|(c, why)| json!({ "candidate": c.to_string(), "reason": why }))
                    .collect();
                let cordons_json: Vec<serde_json::Value> = cordons
                    .iter()
                    .map(|(agent, remaining, reason)| {
                        json!({
                            "agent": agent,
                            "reason": reason,
                            "remaining_secs": remaining.as_secs(),
                        })
                    })
                    .collect();
                // The full set of currently usage-cordoned candidates rides every
                // turn's metadata (not just a redirect), so a client that cached
                // the candidate list at session/new can refresh availability
                // mid-session instead of offering a model the router will refuse.
                let usage_cordons_json: Vec<serde_json::Value> = shared
                    .headroom
                    .lock()
                    .unwrap()
                    .active_usage_cordons()
                    .into_iter()
                    .map(|(id, c)| {
                        json!({
                            "candidate": id.to_string(),
                            "reason": c.reason,
                            "resets_at": c.resets_at_rfc3339,
                        })
                    })
                    .collect();
                // Known seat availability (poll or client hint) — the inputs
                // behind any dynamic preference scaling in `weights`.
                let availability_json: Vec<serde_json::Value> = shared
                    .headroom
                    .lock()
                    .unwrap()
                    .availabilities()
                    .into_iter()
                    .map(|(id, a)| {
                        json!({
                            "candidate": id.to_string(),
                            "plan_headroom": (a.plan_headroom * 100.0).round() / 100.0,
                            "on_overage": a.on_overage,
                            "source": a.source,
                        })
                    })
                    .collect();
                let details = json!({
                    "strategy": strategy_kind.as_str(),
                    "candidate": candidate.to_string(),
                    "class": profile.class.as_str(),
                    "complexity": (profile.complexity * 100.0).round() / 100.0,
                    "languages": profile.languages,
                    "reason": rc.reason,
                    "weights": rc.weights,
                    "note": rc.note,
                    "failover": is_failover,
                    "skipped": skipped_json,
                    "cordoned": cordons_json,
                    "usage_cordons": usage_cordons_json,
                    "availability": availability_json,
                    "excluded": excluded_patterns,
                    "cordon_redirect": cordon_redirect.as_ref().map(|(from, reason, resets)| json!({
                        "from": from.to_string(),
                        "reason": reason,
                        "resets_at": resets,
                    })),
                    "all_cordoned_fallback": all_cordoned_fallback,
                });

                shared.state.lock().unwrap().upsert(
                    router_sid.to_string(),
                    PersistedSession {
                        agent: candidate.agent.clone(),
                        model: candidate.model.clone(),
                        downstream_session_id: opened.downstream_sid.clone(),
                        cwd: cwd.clone(),
                        additional_directories: dirs.clone(),
                        title: prompt_title,
                        routing: Some(details.clone()),
                        parent_session_id: parent_session_id.clone(),
                        kind: session_kind.to_string(),
                        run_label: run_label.clone(),
                        ..Default::default()
                    },
                );

                // Tag the run with its git branch/HEAD (best-effort) so it can be
                // joined to a CI/merge outcome later.
                let (branch, sha) = git_head(&cwd);
                if branch.is_some() || sha.is_some() {
                    shared.state.lock().unwrap().set_git(
                        router_sid,
                        branch.as_deref(),
                        sha.as_deref(),
                    );
                }

                // When an explicit pin was redirected off a cordoned candidate,
                // lead with the failover-format line the spec mandates (so
                // existing clients parse it unchanged); otherwise the normal
                // routing line.
                let mut lines = vec![match &cordon_redirect {
                    Some((_from, reason, resets)) => format!(
                        "router-acp · failover: cordon → {} · task {} ({reason}, resets {})",
                        candidate,
                        profile.class.as_str(),
                        resets.split('T').next().unwrap_or(resets),
                    ),
                    None => format!(
                        "router-acp · {}{} → {} · task {} (complexity {:.2})",
                        if is_failover { "failover: " } else { "" },
                        strategy_kind.as_str(),
                        candidate,
                        profile.class.as_str(),
                        profile.complexity,
                    ),
                }];
                lines.push(format!("why: {}", rc.reason));
                if let Some(note) = &rc.note {
                    lines.push(format!("note: {note}"));
                }
                if let Some(fallback) = &all_cordoned_fallback {
                    lines.push(format!(
                        "note: all candidates usage-cordoned; using least-bad {fallback}"
                    ));
                }
                for (skipped_candidate, why) in &skipped {
                    lines.push(format!("skipped {skipped_candidate}: {why}"));
                }
                for (agent, remaining, reason) in &cordons {
                    lines.push(format!(
                        "{agent} is cordoned: {reason} ({} left)",
                        crate::limits::humanize(*remaining)
                    ));
                }
                if is_failover {
                    lines.push(
                        "note: conversation context from earlier turns does not \
                         transfer to the new model"
                            .to_string(),
                    );
                }

                // Queue the human-readable disclosure to ride the model's
                // first response chunk (Chunk mode). Metadata always rides
                // under `_meta.router_acp` on that same chunk. A cordon
                // redirect / all-cordoned fallback is always surfaced visibly.
                let force_notice =
                    is_failover || cordon_redirect.is_some() || all_cordoned_fallback.is_some();
                match shared.cfg.disclosure {
                    DisclosureMode::Chunk => {
                        queue_notice(shared, router_sid, lines.clone());
                    }
                    DisclosureMode::Meta => {
                        if force_notice {
                            queue_notice(shared, router_sid, lines.clone());
                        }
                    }
                }
                shared.with_session(router_sid, |s| {
                    s.pending_meta_disclosure = Some(details);
                });

                if cancellation.is_cancelled()
                    || shared
                        .with_session(router_sid, |s| s.cancelled)
                        .unwrap_or(false)
                {
                    return Ok(PinOutcome::Cancelled);
                }
                return Ok(PinOutcome::Pinned);
            }
            Err(err) => {
                tracing::warn!(
                    candidate = %candidate,
                    error = %err,
                    "candidate failed pre-prompt; walking fallback chain"
                );
                let why = if is_auth_required(&err) {
                    if let Some(rt) = shared.candidate_runtime(&candidate) {
                        shared.set_target_auth_pending(&rt.process_key);
                    }
                    shared
                        .headroom
                        .lock()
                        .unwrap()
                        .record_exhausted(&candidate.agent);
                    "authentication required".to_string()
                } else {
                    let class = crate::limits::classify_failure(&err);
                    apply_failure(shared, &candidate, &err, &class)
                };
                skipped.push((candidate, why));
                last_err = Some(err);
            }
        }
    }
    // Even total failure is disclosed: the user needs to know every
    // candidate was tried and why each one was skipped.
    if !skipped.is_empty() {
        let mut lines = Vec::new();
        lines.push("router-acp · every candidate failed before the prompt".to_string());
        for (skipped_candidate, why) in &skipped {
            lines.push(format!("skipped {skipped_candidate}: {why}"));
        }
        notify_user(shared, router_sid, lines.join("\n"));
    }
    Err(last_err.unwrap_or_else(|| {
        AcpError::internal_error().data("all ranked candidates failed before the prompt")
    }))
}

/// The MCP servers to hand a new pinned session: the client's own servers
/// plus the router delegate endpoint when delegation is enabled and useful.
fn mcp_servers_for_pin(
    shared: &Arc<Shared>,
    router_sid: &str,
    candidate: &CandidateId,
    client_mcp: &[McpServer],
) -> Vec<McpServer> {
    let mut servers = client_mcp.to_vec();
    if let Some(entry) = crate::delegate_mcp::delegate_server_entry(shared, router_sid, candidate) {
        servers.push(entry);
    }
    servers
}

/// Forward a prompt to the pinned downstream, failing over to the next best
/// candidate when the pinned model is rate-limited or down — as long as no
/// output has streamed for this turn and the client has not cancelled.
async fn send_prompt_with_failover(
    shared: Arc<Shared>,
    router_sid: String,
    req: PromptRequest,
    responder: Responder<PromptResponse>,
) -> Result<(), AcpError> {
    use crate::limits::{FailureClass, classify_failure};

    // A requested/auto-detected model switch fires here, before this turn is
    // forwarded, so the prompt lands on the new model. The current model first
    // summarizes the work; that summary becomes `pending_context`.
    if let Some(sw) = shared
        .with_session(&router_sid, |s| s.pending_switch.take())
        .flatten()
    {
        match switch_pin(&shared, &router_sid, &sw.target, &sw.reason).await {
            Ok(lines) if !lines.is_empty() => queue_notice(&shared, &router_sid, lines),
            Ok(_) => {}
            Err(e) => notify_user(
                &shared,
                &router_sid,
                format!(
                    "router-acp · switch to {} failed — {e}; staying on the current model",
                    sw.target
                ),
            ),
        }
    }

    // Loop budget covers both failover attempts and escalation replays.
    let max_attempts = shared.cfg.failover.max_attempts.max(1);
    let max_iters = max_attempts.max(shared.cfg.routers.escalation.max_escalations + 1);
    for attempt in 1..=max_iters {
        let Some((conn, down_sid, candidate)) = shared.pinned_route(&router_sid) else {
            return responder.respond_with_error(
                AcpError::internal_error()
                    .data("session has no live downstream (its process may have died)"),
            );
        };
        // Fresh per-turn state (also for the strong model after an escalation).
        shared.with_session(&router_sid, |s| {
            s.turn_saw_output = false;
            s.turn_output.clear();
            s.turn_reads = 0;
            s.turn_side_effect = false;
            s.turn_tool_failures = 0;
            s.turn_counted_tools.clear();
            s.turn_failed_tools.clear();
            s.turn_tool_calls = 0;
            s.escalation_requested = None;
            s.turn_native_subagent_warned = false;
        });
        shared.state.lock().unwrap().touch(&router_sid);
        // Log the user prompt (once, on the first attempt).
        if attempt == 1 {
            let prompt_text = prompt_display_text(&req.prompt);
            shared.state.lock().unwrap().log(
                &router_sid,
                &crate::state::LogEntry {
                    kind: "user_prompt".to_string(),
                    role: "user".to_string(),
                    summary: prompt_text.chars().take(500).collect(),
                    tokens_input: crate::state::estimate_tokens(&prompt_text),
                    tokens_estimated: true,
                    ..Default::default()
                },
            );
        }
        // A pending handoff block (from a switch performed just before this
        // attempt — pre-loop pending_switch, or a mid-turn escalation on the
        // previous iteration) is prepended, consumed once. It is already fully
        // framed by `switch_pin` (summary or log-transcript fallback).
        let (orchestration, handoff) = shared
            .with_session(&router_sid, |s| {
                (s.pending_orchestration.take(), s.pending_context.take())
            })
            .unwrap_or((None, None));
        let effective_prompt = {
            let mut blocks = Vec::new();
            // Orchestration protocol first (role framing), then any switch
            // handoff context, then the user's actual task.
            if let Some(instr) = orchestration {
                blocks.push(ContentBlock::from(instr));
            }
            if let Some(ctx) = handoff {
                blocks.push(ContentBlock::from(ctx));
            }
            blocks.extend(req.prompt.clone());
            blocks
        };
        shared
            .headroom
            .lock()
            .unwrap()
            .record_prompt(&candidate.agent);
        let fwd = PromptRequest::new(down_sid.clone(), effective_prompt).meta(req.meta.clone());
        let sent = conn
            .send_request(fwd)
            .forward_cancellation_from(responder.cancellation());
        // Compute-time = the model's actual turn (excludes user idle between
        // turns, unlike updated_at − created_at).
        let turn_start = std::time::Instant::now();
        let result = sent.block_task().await;
        shared
            .state
            .lock()
            .unwrap()
            .add_compute_ms(&router_sid, turn_start.elapsed().as_millis() as u64);

        // Mid-turn escalation: the relay flagged it (and interrupted this turn)
        // because investigation revealed hidden depth while still side-effect
        // free. Switch to the stronger model and replay the same prompt.
        if let Some(esc) = shared
            .with_session(&router_sid, |s| s.escalation_requested.take())
            .flatten()
        {
            shared.with_session(&router_sid, |s| s.escalations_done += 1);
            match switch_pin(&shared, &router_sid, &esc.target, &esc.reason).await {
                Ok(lines) if !lines.is_empty() => queue_notice(&shared, &router_sid, lines),
                Ok(_) => {}
                Err(e) => notify_user(
                    &shared,
                    &router_sid,
                    format!(
                        "router-acp · escalation to {} failed — {e}; continuing on the current model",
                        esc.target
                    ),
                ),
            }
            continue; // replay on the new pin (or the old one if the switch failed)
        }

        match result {
            Ok(resp) => {
                // If the model produced no text to carry the disclosure,
                // flush it now as its own chunk so it still shows.
                flush_pending_disclosure(&shared, &router_sid);
                // Log the assistant response with token usage.
                let output = shared
                    .with_session(&router_sid, |s| s.turn_output.clone())
                    .unwrap_or_default();
                let (ti, to, est) = turn_tokens(&resp, &output);
                shared.state.lock().unwrap().log(
                    &router_sid,
                    &crate::state::LogEntry {
                        kind: "agent_response".to_string(),
                        role: "agent".to_string(),
                        summary: output.chars().take(500).collect(),
                        detail: Some(
                            serde_json::json!({"stop_reason": format!("{:?}", resp.stop_reason)}),
                        ),
                        tokens_input: ti,
                        tokens_output: to,
                        tokens_estimated: est,
                    },
                );
                // Update the session's confidence from how this turn went and,
                // if it has fallen below the configured threshold, queue an
                // auto-upgrade to a more capable model for the next prompt.
                update_confidence_and_maybe_upgrade(&shared, &router_sid, &resp);
                // The turn just changed real usage — nudge the shared usage
                // snapshot (self-throttled and fire-and-forget; never delays
                // the turn).
                crate::usage::refresh_after_turn(&shared, &candidate.agent);
                return responder.respond(resp);
            }
            Err(err) => {
                let saw_output = shared
                    .with_session(&router_sid, |s| s.turn_saw_output)
                    .unwrap_or(false);
                let cancelled = responder.cancellation().is_cancelled()
                    || shared
                        .with_session(&router_sid, |s| s.cancelled)
                        .unwrap_or(false);
                let class = classify_failure(&err);
                let human = apply_failure(&shared, &candidate, &err, &class);

                let can_fail_over = shared.cfg.failover.enabled
                    && !cancelled
                    && !saw_output
                    && !matches!(class, FailureClass::Other)
                    && attempt < max_attempts;

                if !can_fail_over {
                    if !matches!(class, FailureClass::Other) {
                        let detail = if saw_output {
                            "; not failing over because this turn already produced output"
                        } else {
                            ""
                        };
                        notify_user(
                            &shared,
                            &router_sid,
                            format!("router-acp · {candidate} unavailable — {human}{detail}"),
                        );
                    }
                    // The turn ends in error, so no model chunk will carry the
                    // queued notice — flush it as its own chunk now.
                    flush_pending_disclosure(&shared, &router_sid);
                    return responder.respond_with_error(err);
                }

                tracing::warn!(
                    session = router_sid,
                    candidate = %candidate,
                    attempt,
                    error = %err,
                    "pinned candidate failed; attempting failover"
                );
                notify_user(
                    &shared,
                    &router_sid,
                    format!("router-acp · {candidate} unavailable — {human}; failing over…"),
                );

                // Tear down the failed downstream session and re-pin.
                let old_key = shared
                    .with_session(&router_sid, |s| {
                        s.pin.as_ref().map(|p| p.process_key.clone())
                    })
                    .flatten();
                if let Some(key) = old_key {
                    close_downstream_session(&shared, &key, &down_sid);
                }
                match pin_session(
                    &shared,
                    &router_sid,
                    &req.prompt,
                    &responder.cancellation(),
                    Some(&candidate),
                    true,
                )
                .await
                {
                    Ok(PinOutcome::Pinned) => continue,
                    Ok(PinOutcome::Cancelled) => {
                        return responder.respond(PromptResponse::new(StopReason::Cancelled));
                    }
                    Err(pin_err) => {
                        notify_user(
                            &shared,
                            &router_sid,
                            format!("router-acp · no fallback candidate available — {pin_err}"),
                        );
                        flush_pending_disclosure(&shared, &router_sid);
                        return responder.respond_with_error(err);
                    }
                }
            }
        }
    }
    responder.respond_with_error(
        AcpError::internal_error().data("all failover attempts exhausted for this prompt"),
    )
}

/// True when `pattern` (an exact `agent/model` id, a glob like `*opus*`, or a
/// bare agent name) designates `candidate`.
fn candidate_matches(pattern: &str, candidate: &CandidateId) -> bool {
    pattern.eq_ignore_ascii_case(&candidate.agent)
        || crate::candidate::glob_match(pattern, &candidate.to_string())
}

/// The best eligible candidate matching any of `patterns` — not cordoned,
/// quarantined, or excluded. Patterns are candidate globs, so a skill can name
/// a model *class* (`*opus*`) rather than a specific id. The patterns define
/// the POOL; the pick within it follows the deterministic-routing tie-break
/// order (preference-adjusted quality → pattern order → config order), so
/// `agents[].preference` actually biases planner/skill steering — pattern
/// order alone used to win, which made `preference` a no-op here.
fn first_eligible_candidate(
    shared: &Arc<Shared>,
    patterns: &[String],
    class: TaskClass,
    excluded: &[String],
) -> Option<CandidateId> {
    let views = shared.eligible_views(&RequiredCaps::default(), class);
    views
        .iter()
        .filter(|v| !is_excluded(&v.id, excluded))
        .filter_map(|v| {
            patterns
                .iter()
                .position(|pat| candidate_matches(pat, &v.id))
                .map(|pat_idx| (v, pat_idx))
        })
        .max_by(|(a, a_pat), (b, b_pat)| {
            (a.quality + a.preference)
                .partial_cmp(&(b.quality + b.preference))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(b_pat.cmp(a_pat))
                .then(b.config_index.cmp(&a.config_index))
        })
        .map(|(v, _)| v.id.clone())
}

/// Resolve a loose model reference (from the `model:` shorthand) to the best
/// eligible candidate: an exact `agent/model` id, a bare model id (`sonnet`),
/// a family/prefix (`gpt`, `claude/opus`), or any substring — highest quality
/// wins on ambiguity. `None` = nothing eligible matches (so the caller leaves
/// the prompt untouched rather than mis-routing prose).
fn resolve_candidate_ref(
    shared: &Arc<Shared>,
    reference: &str,
    class: TaskClass,
    excluded: &[String],
) -> Option<CandidateId> {
    let views = shared.eligible_views(&RequiredCaps::default(), class);
    // Exact `agent/model` id wins outright.
    if let Some(id) = CandidateId::parse(reference)
        && views
            .iter()
            .any(|v| v.id == id && !is_excluded(&id, excluded))
    {
        return Some(id);
    }
    let needle = reference.to_lowercase();
    let has_slash = reference.contains('/');
    views
        .iter()
        .filter(|v| !is_excluded(&v.id, excluded))
        .filter(|v| {
            let full = v.id.to_string().to_lowercase();
            let model = v.id.model.to_lowercase();
            if has_slash {
                full == needle
                    || crate::candidate::glob_match(&needle, &full)
                    || crate::candidate::glob_match(&format!("{needle}*"), &full)
            } else {
                model == needle
                    || full == needle
                    || crate::candidate::glob_match(&format!("*{needle}*"), &model)
                    || crate::candidate::glob_match(&format!("*{needle}*"), &full)
            }
        })
        .max_by(|a, b| {
            a.quality
                .partial_cmp(&b.quality)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|v| v.id.clone())
}

/// If the prompt begins (after any goose `<turn-context>` preamble) with a
/// `<token>:` model shorthand — e.g. `opus: fix the bug`, `codex/gpt-5.5:`, or
/// a bare `sonnet:` — return `(token, prompt-with-the-prefix-removed)`.
/// Resolving the token to a real candidate is the caller's job (and its gate:
/// an unresolved token means this was ordinary prose, not a switch).
fn split_model_shorthand(prompt: &[ContentBlock]) -> Option<(String, Vec<ContentBlock>)> {
    // goose splits a prompt into multiple content blocks — typically a
    // `<turn-context>…</turn-context>` block followed by the user's message in
    // a SEPARATE block. So walk blocks in order: skip ones that are only
    // preamble/blank, and test the shorthand against the first block that
    // carries real user content.
    for (block_idx, b) in prompt.iter().enumerate() {
        let ContentBlock::Text(t) = b else {
            // A non-text block (image/resource) is real content, not a
            // shorthand — the message doesn't start with `model:`.
            return None;
        };
        let text = &t.text;
        // Skip a leading <turn-context>…</turn-context> preamble within the block.
        let user_start = text
            .find("</turn-context>")
            .map(|p| p + "</turn-context>".len())
            .unwrap_or(0);
        let preamble = &text[..user_start];
        let user = text[user_start..].trim_start();
        if user.is_empty() {
            continue; // preamble-only block; the message is in a later block
        }
        // Leading token up to the first ':' with no intervening whitespace.
        let head: String = user
            .chars()
            .take_while(|c| !c.is_whitespace() && *c != ':')
            .collect();
        let after = user.get(head.len()..)?;
        if head.is_empty() || !after.starts_with(':') {
            return None; // first real content isn't a `model:` shorthand
        }
        let rest = after[1..].trim_start();
        let remainder = if preamble.trim().is_empty() {
            rest.to_string()
        } else if rest.is_empty() {
            String::new() // bare shorthand → empty task (a continuation is synthesized)
        } else {
            format!("{}\n\n{}", preamble.trim_end(), rest)
        };
        let mut stripped = prompt.to_vec();
        if remainder.trim().is_empty() {
            stripped.remove(block_idx);
        } else {
            stripped[block_idx] = ContentBlock::from(remainder);
        }
        return Some((head, stripped));
    }
    None
}

/// Remove inline code spans and fenced code blocks (anything between backtick
/// runs) so a skill *named* in code/examples — e.g. describing an autocomplete
/// for `` `/ship-pr` `` — isn't mistaken for *invoking* the skill. Balanced or
/// not, everything inside backticks is dropped; each run becomes a separator so
/// surrounding tokens don't merge.
fn strip_code_spans(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_code = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '`' {
            while chars.peek() == Some(&'`') {
                chars.next();
            }
            in_code = !in_code;
            out.push(' ');
        } else if !in_code {
            out.push(c);
        }
    }
    out
}

/// True when the prompt invokes `pattern` — either as a `/slash-command` or as
/// a standalone token (so "ship-pr" matches "run ship-pr" and "/ship-pr" but
/// not "membership-provider"). The caller passes text with code spans already
/// stripped, so a skill name mentioned inside backticks does not count.
fn prompt_mentions_skill(text_lower: &str, pattern: &str) -> bool {
    let p = pattern.to_lowercase();
    if p.is_empty() {
        return false;
    }
    if text_lower.contains(&format!("/{p}")) {
        return true;
    }
    text_lower
        .split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_')
        .any(|tok| tok == p)
}

/// The first configured skill route whose pattern the prompt invokes. Code spans
/// are stripped first so a skill *mentioned* in code/examples (e.g. a UI prompt
/// describing `` `/ship-pr` `` autocomplete) does not hijack routing.
fn detect_skill_route<'a>(cfg: &'a Config, prompt: &[ContentBlock]) -> Option<&'a SkillRoute> {
    if cfg.skill_routing.is_empty() {
        return None;
    }
    let text = strip_code_spans(&prompt_display_text(prompt)).to_lowercase();
    cfg.skill_routing
        .iter()
        .find(|r| prompt_mentions_skill(&text, &r.pattern))
}

/// The company lineage of an agent: the configured `agents[].lineage` tag, or
/// the agent name when none is declared. Cross-lineage review compares THIS —
/// the goal is a reviewer from a **different company** (different failure
/// modes), so two agents backed by the same vendor share a lineage.
pub fn agent_lineage(cfg: &Config, agent: &str) -> String {
    cfg.agents
        .iter()
        .find(|a| a.name == agent)
        .and_then(|a| a.lineage.clone())
        .unwrap_or_else(|| agent.to_string())
}

/// Resolve concrete reviewer candidates of a DIFFERENT lineage (company) than
/// the planner (preferring the configured `reviewer` globs, then any
/// other-lineage candidate). Empty only when the planner's lineage is the sole
/// one available.
fn resolve_reviewers(
    shared: &Arc<Shared>,
    cfg: &crate::config::OrchestrationConfig,
    planner: &CandidateId,
    class: TaskClass,
    excluded: &[String],
) -> Vec<CandidateId> {
    let planner_lineage = agent_lineage(&shared.cfg, &planner.agent);
    let views = shared.eligible_views(&RequiredCaps::default(), class);
    let mut out: Vec<CandidateId> = Vec::new();
    // 1. Configured reviewer globs, restricted to a different lineage.
    for pat in &cfg.reviewer {
        for v in &views {
            if agent_lineage(&shared.cfg, &v.id.agent) != planner_lineage
                && candidate_matches(pat, &v.id)
                && !is_excluded(&v.id, excluded)
                && !out.contains(&v.id)
            {
                out.push(v.id.clone());
            }
        }
    }
    // 2. Fallback: any eligible candidate of a different lineage.
    if out.is_empty() {
        for v in &views {
            if agent_lineage(&shared.cfg, &v.id.agent) != planner_lineage
                && !is_excluded(&v.id, excluded)
                && !out.contains(&v.id)
            {
                out.push(v.id.clone());
            }
        }
    }
    out.truncate(3);
    out
}

/// The orchestration protocol prepended to the planner model's prompt when a
/// multi-part task list is auto-detected. This recreates the goose orchestrate
/// recipe entirely in-process: the planner decomposes the task, drives
/// `delegate_task` sub-sessions (each routed per-complexity), has a
/// different-lineage peer review the net result, adjudicates fixes, and submits
/// per the configured gate. It is guidance to the model, not a hard state
/// machine — the delegation pool relaxation (peer/same-tier) is what makes the
/// cross-lineage review routeable, and the explicit reviewer ids + the ban on
/// the model's built-in sub-agent tool are what keep the review off the
/// planner's own lineage.
fn build_orchestration_instructions(
    cfg: &Config,
    parts: usize,
    forced: bool,
    planner: &CandidateId,
    reviewers: &[CandidateId],
) -> String {
    let o = &cfg.orchestration;
    let lineage = agent_lineage(cfg, &planner.agent);
    let intro = if forced && parts < 2 {
        "The user explicitly requested orchestration for the task below.".to_string()
    } else {
        format!("The user's message below is a multi-part task ({parts} parts detected).")
    };
    let review_line = if reviewers.is_empty() {
        format!(
            "No candidate of a different lineage than your own (`{lineage}`) is currently \
             available, so review on the most capable OTHER model you can reach via \
             `delegate_task` (still not yourself). Note this constraint in your report."
        )
    } else {
        let ids = reviewers
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "You are lineage `{lineage}`. The review MUST run on a DIFFERENT lineage — call \
             `delegate_task` with `hints.candidate` set to one of: {ids}. Do NOT review on your \
             own `{lineage}` lineage."
        )
    };
    let submit_line = match o.submit.as_str() {
        "never" => {
            "Do NOT push, open a PR, or merge. Report the branch (if any) and the review verdict."
        }
        "branch" => {
            "Once the review approves, commit the work on a fresh branch off HEAD (never commit to \
             main/master). Do not open a PR or merge."
        }
        "pr" => {
            "Once the review approves, commit on a fresh branch and open/update a PR with `gh pr \
             create` (body: the success criteria, a subtask→model table, and the reviewer \
             verdict). Do NOT merge."
        }
        "merge" => {
            "Once — and only once — the review approves, commit on a fresh branch, open/update the \
             PR, and as the FINAL action merge it (`gh pr merge`, honoring branch protection). \
             Never merge before an approving review; never force-push; never push to main directly."
        }
        _ => "",
    };
    let rounds = o.max_fix_rounds;
    format!(
        "[router-acp orchestration — you are the ORCHESTRATOR]\n\
         {intro} Do NOT implement \
         it all yourself in one pass.\n\
         CRITICAL — HOW TO DELEGATE: use the router's `delegate_task` tool for EVERY subtask and \
         for the review. Do NOT use any built-in sub-agent tool (e.g. `Task`, `dispatch_agent`, \
         `spawn`) for these: those run inside your own model lineage and are invisible to the \
         router, which defeats both per-subtask model routing and the cross-lineage review. If \
         `delegate_task` is not loaded yet, load it first, then use it. For a subtask you want to \
         iterate on across review→fix rounds, call `delegate_task` with `keep_open: true` and \
         reuse the returned `delegate_id` via `delegate_followup` (and `delegate_close` when done).\n\
         Run this pipeline, disclosing your progress as you go:\n\
         1. PLAN. Investigate just enough (read-only) to restate the task as concrete, verifiable \
         success criteria and to split it into self-contained subtasks. Subtasks MUST NOT overlap \
         in the files they edit — merge any that would.\n\
         2. DELEGATE. Dispatch each subtask with `delegate_task`. Give each a fully self-contained \
         `task` (file paths, current vs. desired behavior, constraints, and acceptance checks) — \
         the router routes each subtask by reading its prompt, so describe difficulty honestly. \
         Pass relevant paths in `context_files`. Independent subtasks may be delegated \
         concurrently; respect dependencies. Do a piece yourself only if it genuinely needs your \
         full context.\n\
         3. REVIEW (independent, different lineage). After integrating, delegate a REVIEW via \
         `delegate_task`. {review_line} Hand the reviewer the ORIGINAL task verbatim and have it \
         re-derive the criteria itself, inspect the diff, and run the tests — returning a verdict \
         plus any blocking issues.\n\
         4. ADJUDICATE. For each blocking issue, delegate a targeted fix and re-review. At most \
         {rounds} fix rounds; if still not approved, stop and report what remains.\n\
         5. SUBMIT. {submit_line} Any skill or command the task names for the END of the work \
         (shipping, opening/merging a PR, deploying) runs ONLY here — after the work is done and \
         the review approves — never up front; a half-done change is not shippable.\n\
         Finally, report: the success criteria and how each is met; per-subtask outcomes (and \
         which model the router chose for each); the review verdict history; and what was \
         submitted (or why not).\n\
         [end orchestration protocol — the user's task follows]"
    )
}

/// True when the previous agent turn appears to have asked the user
/// question(s)/decisions — so a list in the user's reply is *answers*, not a new
/// multi-part task, and must not trigger orchestration. Deliberately
/// conservative (it only fires on clear solicitations) so genuine follow-up task
/// lists still orchestrate.
fn previous_turn_solicited_answers(prev_agent_text: &str) -> bool {
    let text = prev_agent_text.trim();
    if text.is_empty() {
        return false;
    }
    let lower = text.to_lowercase();
    // Phrases that clearly solicit a decision/answer, low false-positive.
    const SOLICIT_PHRASES: &[&str] = &[
        "open question",
        "open decision",
        "open item",
        "please confirm",
        "please decide",
        "to decide",
        "need to decide",
        "decisions:",
        "questions:",
        "options:",
        "your call",
        "up to you",
        "which of",
        "do you want",
        "do you prefer",
        "would you like",
        "would you prefer",
        "how would you like",
        "let me know which",
        "let me know how",
        "which would you",
    ];
    let has_phrase = SOLICIT_PHRASES.iter().any(|p| lower.contains(p));
    let questions = text.matches('?').count();
    // Did the agent itself enumerate the options/questions?
    let enumerated = crate::tasklist::detect_task_list(text).is_some();
    has_phrase || questions >= 2 || (questions >= 1 && enumerated)
}

/// If auto-orchestration is enabled and the prompt reads as a multi-part task
/// list, put the session into orchestration mode: steer/switch it to a planner
/// model and queue the orchestration protocol for the next prompt. Returns
/// `true` when it fired. A no-op (returns `false`) when disabled, when the list
/// is too small, when no planner is eligible, or when the list is the user
/// answering the model's own questions. Runs BEFORE `skill_routing`, so a
/// multi-part task orchestrates even if it names a skill — the planner decides
/// when to invoke that skill (end-of-work skills like shipping run last).
fn maybe_trigger_orchestration(
    shared: &Arc<Shared>,
    router_sid: &str,
    prompt: &[ContentBlock],
    forced: bool,
) -> bool {
    let cfg = &shared.cfg.orchestration;
    // An explicit `orchestrate:` prefix overrides every auto-detection gate,
    // including `enabled` — the user asked for it by name.
    if !forced && !cfg.enabled {
        return false;
    }
    let text = prompt_display_text(prompt);
    let parts = crate::tasklist::detect_task_list(&text).unwrap_or(1);
    if !forced {
        if parts < cfg.min_items.max(2) {
            return false;
        }
        // Exception: don't orchestrate a list that answers the model's
        // questions. `turn_output` still holds the previous agent turn here (it
        // is cleared later, inside `send_prompt_with_failover`); empty pre-pin,
        // so a first prompt is never mistaken for an answer.
        let prev_agent_turn = shared
            .with_session(router_sid, |s| s.turn_output.clone())
            .unwrap_or_default();
        if previous_turn_solicited_answers(&prev_agent_turn) {
            tracing::debug!(
                session = router_sid,
                "auto-orchestration skipped: prompt looks like answers to the model's questions"
            );
            return false;
        }
    }

    let (class, excluded, current) = shared
        .with_session(router_sid, |s| {
            (
                s.task_class.unwrap_or(TaskClass::CodingGeneral),
                s.excluded.clone(),
                s.pin.as_ref().map(|p| p.candidate.clone()),
            )
        })
        .unwrap_or((TaskClass::CodingGeneral, Vec::new(), None));

    // A capable planner is required; if none is eligible, route normally.
    let Some(planner) = first_eligible_candidate(shared, &cfg.planner, class, &excluded) else {
        notify_user(
            shared,
            router_sid,
            format!(
                "router-acp · detected a {parts}-part task but no orchestration planner ({:?}) is \
                 available; routing normally",
                cfg.planner
            ),
        );
        return false;
    };

    let current_is_planner = current
        .as_ref()
        .map(|c| cfg.planner.iter().any(|g| candidate_matches(g, c)))
        .unwrap_or(false);
    let reviewers = resolve_reviewers(shared, cfg, &planner, class, &excluded);
    if reviewers.is_empty() {
        notify_user(
            shared,
            router_sid,
            format!(
                "router-acp · note: no candidate of a different lineage (company) than the \
                 planner ({}) is available for review; orchestrating anyway",
                agent_lineage(&shared.cfg, &planner.agent)
            ),
        );
    }
    let instructions =
        build_orchestration_instructions(&shared.cfg, parts, forced, &planner, &reviewers);

    shared.with_session(router_sid, |s| {
        s.orchestrating = true;
        s.pending_orchestration = Some(instructions);
        // Group this run (planner + its delegated subtasks/review) under a
        // shared label unless the caller already set one.
        if s.run_label.is_none() {
            s.run_label = Some("orchestrate".to_string());
        }
    });

    let (what, why) = if forced && parts < 2 {
        ("the task".to_string(), "orchestrate: requested")
    } else if forced {
        (format!("a {parts}-part task"), "orchestrate: requested")
    } else {
        (format!("a {parts}-part task"), "auto-detected list")
    };
    match &current {
        // Pre-pin: steer the imminent pin onto the planner.
        None => {
            let planner2 = planner.clone();
            shared.with_session(router_sid, |s| {
                if s.candidate_override.is_none() {
                    s.candidate_override = Some(planner2);
                }
            });
            notify_user(
                shared,
                router_sid,
                format!("router-acp · orchestrating {what} on {planner} ({why})"),
            );
        }
        // Already on a planner-class model: orchestrate in place.
        Some(cur) if current_is_planner => {
            notify_user(
                shared,
                router_sid,
                format!("router-acp · orchestrating {what} on {cur} ({why})"),
            );
        }
        // Post-pin on a weaker model: switch to the planner (summarize + hand off).
        Some(_) => {
            let planner2 = planner.clone();
            shared.with_session(router_sid, |s| {
                s.pending_switch = Some(SwitchRequest {
                    target: planner2,
                    reason: format!("orchestration of {what} ({why})"),
                });
            });
            notify_user(
                shared,
                router_sid,
                format!("router-acp · orchestrating {what}; switching to {planner} ({why})"),
            );
        }
    }
    true
}

/// Estimate a session's confidence in [0, 1]: the pinned model's quality for
/// the task class, minus accumulated struggle. Low = the model looks
/// under-powered for how the session is actually going.
fn session_confidence(shared: &Arc<Shared>, router_sid: &str) -> f64 {
    shared
        .with_session(router_sid, |s| {
            (s.pinned_quality - s.struggle).clamp(0.0, 1.0)
        })
        .unwrap_or(1.0)
}

/// The best eligible candidate strictly more capable (higher quality for the
/// session's task class) than the current pin — the auto-upgrade target.
fn upgrade_target(shared: &Arc<Shared>, router_sid: &str) -> Option<CandidateId> {
    let (class, current, current_q, excluded) = shared.with_session(router_sid, |s| {
        (
            s.task_class.unwrap_or(TaskClass::CodingGeneral),
            s.pin.as_ref().map(|p| p.candidate.clone()),
            s.pinned_quality,
            s.excluded.clone(),
        )
    })?;
    let current = current?;
    let mut pool = shared.eligible_views(&RequiredCaps::default(), class);
    pool.retain(|v| v.id != current && !is_excluded(&v.id, &excluded));
    pool.into_iter()
        .filter(|v| v.quality > current_q + 0.05)
        .max_by(|a, b| {
            a.quality
                .partial_cmp(&b.quality)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|v| v.id)
}

/// After a turn, fold its outcome into the session's `struggle` score and, if
/// auto-upgrade is enabled and confidence has dropped below the configured
/// threshold, queue a switch to the best more-capable candidate for the next
/// prompt. Deterministic: struggle rises on token exhaustion, refusals, and
/// repeated tool failures within a turn.
fn update_confidence_and_maybe_upgrade(
    shared: &Arc<Shared>,
    router_sid: &str,
    resp: &PromptResponse,
) {
    let tool_failures = shared
        .with_session(router_sid, |s| std::mem::take(&mut s.turn_tool_failures))
        .unwrap_or(0);
    let mut delta = 0.0;
    match resp.stop_reason {
        StopReason::MaxTokens => delta += 0.3,
        StopReason::Refusal => delta += 0.5,
        _ => {}
    }
    if tool_failures >= 3 {
        delta += 0.2;
    }
    if delta > 0.0 {
        shared.with_session(router_sid, |s| {
            s.struggle = (s.struggle + delta).min(1.0);
        });
    }

    // The `escalation` router uses its own post-turn triggers instead of the
    // confidence-threshold auto-upgrade.
    let strategy = shared
        .with_session(router_sid, |s| s.strategy)
        .unwrap_or(StrategyKind::Auto);
    if strategy == StrategyKind::Escalation {
        escalation_post_turn(shared, router_sid, resp, tool_failures);
        return;
    }

    if !shared.cfg.auto_upgrade.enabled {
        return;
    }
    // Only pinned sessions can be upgraded, and only once per pending switch.
    let can_upgrade = shared
        .with_session(router_sid, |s| {
            s.pin.is_some() && s.pending_switch.is_none()
        })
        .unwrap_or(false);
    if !can_upgrade {
        return;
    }
    let confidence = session_confidence(shared, router_sid);
    if confidence >= shared.cfg.auto_upgrade.confidence_threshold {
        return;
    }
    if let Some(target) = upgrade_target(shared, router_sid) {
        let threshold = shared.cfg.auto_upgrade.confidence_threshold;
        shared.with_session(router_sid, |s| {
            s.pending_switch = Some(SwitchRequest {
                target: target.clone(),
                reason: format!(
                    "auto-upgrade: confidence {confidence:.2} below threshold {threshold:.2}"
                ),
            });
        });
        notify_user(
            shared,
            router_sid,
            format!(
                "router-acp · confidence {confidence:.2} below threshold {threshold:.2}; \
                 upgrading to {target} for the next turn"
            ),
        );
        tracing::info!(
            session = router_sid,
            %target,
            confidence,
            "auto-upgrade queued"
        );
    }
}

/// Post-turn escalation for the `escalation` router: if the completed turn's
/// outcome trips a configured trigger (max-tokens/refusal stop, or tool-failure
/// churn), queue an escalation to a stronger model for the next prompt. This
/// complements the mid-turn read-volume trigger, which fires during the turn.
fn escalation_post_turn(
    shared: &Arc<Shared>,
    router_sid: &str,
    resp: &PromptResponse,
    tool_failures: u32,
) {
    let cfg = shared.cfg.routers.escalation.clone();
    let eligible = shared
        .with_session(router_sid, |s| {
            s.pin.is_some()
                && s.pending_switch.is_none()
                && s.escalations_done < cfg.max_escalations
        })
        .unwrap_or(false);
    if !eligible {
        return;
    }
    let mut reasons = Vec::new();
    if cfg.escalate_on_max_tokens && matches!(resp.stop_reason, StopReason::MaxTokens) {
        reasons.push("hit the token ceiling".to_string());
    }
    if cfg.escalate_on_refusal && matches!(resp.stop_reason, StopReason::Refusal) {
        reasons.push("refused".to_string());
    }
    if cfg.escalate_after_tool_failures > 0 && tool_failures >= cfg.escalate_after_tool_failures {
        reasons.push(format!("{tool_failures} tool failures"));
    }
    if reasons.is_empty() {
        return;
    }
    let Some(target) = escalation_target(shared, router_sid, cfg.escalation_path) else {
        return;
    };
    let reason = format!("escalation: {}", reasons.join(", "));
    shared.with_session(router_sid, |s| {
        s.pending_switch = Some(SwitchRequest {
            target: target.clone(),
            reason: reason.clone(),
        });
        s.escalations_done += 1;
    });
    notify_user(
        shared,
        router_sid,
        format!("router-acp · escalating to {target} for the next turn — {reason}"),
    );
    tracing::info!(session = router_sid, %target, reason, "escalation queued (post-turn)");
}

/// The instruction sent to the outgoing model asking it to summarize the
/// session before a handoff.
const HANDOFF_SUMMARY_INSTRUCTION: &str = "You are about to hand this conversation off to a different model. \
     Write a concise but complete handoff summary: the task, key decisions \
     and findings so far, the current state of the work (files changed, \
     commands run), and exactly what remains to be done. Do not continue the \
     task — only summarize.";

/// Strip a leading goose `<turn-context>…</turn-context>` preamble and trim,
/// so a logged user turn reads as the user's actual words.
fn clean_turn_text(s: &str) -> String {
    let body = match s.find("</turn-context>") {
        Some(p) => &s[p + "</turn-context>".len()..],
        None => s,
    };
    body.trim().to_string()
}

/// Reconstruct a truncated transcript of the prior conversation from the state
/// DB logs — the fallback handoff used when the outgoing model cannot
/// summarize (offline, token-limited, refused, or crashed). Each logged turn
/// is already capped (~500 chars), so this is lossy but self-contained and
/// needs nothing from the old model. Returns `""` when there is nothing to
/// carry over. Prefers the most recent turns when over budget.
fn transcript_from_logs(shared: &Arc<Shared>, router_sid: &str) -> String {
    const MAX_TURNS: usize = 40;
    const MAX_CHARS: usize = 12_000;
    let entries = shared.state.lock().unwrap().log_for(router_sid, 500);
    let turns: Vec<String> = entries
        .iter()
        .filter(|e| e.kind == "user_prompt" || e.kind == "agent_response")
        .filter_map(|e| {
            let who = if e.kind == "user_prompt" {
                "User"
            } else {
                "Assistant"
            };
            let text = clean_turn_text(&e.summary);
            (!text.is_empty()).then(|| format!("{who}: {text}"))
        })
        .collect();
    if turns.is_empty() {
        return String::new();
    }
    // Accumulate from the most recent turn backward until a budget is hit.
    let mut chosen: Vec<&String> = Vec::new();
    let mut total = 0usize;
    for turn in turns.iter().rev() {
        if !chosen.is_empty() && (total + turn.len() > MAX_CHARS || chosen.len() >= MAX_TURNS) {
            break;
        }
        total += turn.len();
        chosen.push(turn);
    }
    let dropped = turns.len() - chosen.len();
    chosen.reverse();
    let mut out = String::new();
    if dropped > 0 {
        out.push_str(&format!("[…{dropped} earlier turn(s) omitted…]\n\n"));
    }
    out.push_str(
        &chosen
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n\n"),
    );
    out
}

/// Frame a model-written handoff summary as a context block.
fn frame_summary(from: &CandidateId, summary: &str) -> String {
    format!(
        "[Handoff context — a summary of the conversation so far, written by the previous model \
         ({from}). You are picking up this work; treat it as established context.]\n\n{summary}\n\n\
         [End of handoff context. The user's message follows.]"
    )
}

/// Frame a log-reconstructed transcript as a context block (the fallback when
/// the previous model could not summarize).
fn frame_transcript(from: &CandidateId, transcript: &str) -> String {
    format!(
        "[Handoff context — the previous model ({from}) was unavailable to summarize, so this is \
         a truncated transcript of the prior session reconstructed from router-acp's logs (each \
         turn is capped at ~500 characters, so detail may be lost). Treat it as established \
         context for continuing the work.]\n\n{transcript}\n\n\
         [End of handoff context. The user's message follows.]"
    )
}

/// Switch a pinned session to `target` mid-conversation: ask the current
/// model to summarize the work, open a fresh downstream session on the
/// target, seed it with that summary (prepended to the next prompt), re-pin,
/// and close the old session. Context does not transfer via ACP, so the
/// summary IS the handoff. Returns the switch disclosure lines.
async fn switch_pin(
    shared: &Arc<Shared>,
    router_sid: &str,
    target: &CandidateId,
    reason: &str,
) -> Result<Vec<String>, AcpError> {
    // Read the pin directly (not `pinned_route`, which requires a *live*
    // downstream): an outage may have killed the old process, and we still
    // want to switch away from it using the log-transcript fallback.
    let Some((old_candidate, old_down_sid, old_process_key)) = shared
        .with_session(router_sid, |s| {
            s.pin.as_ref().map(|p| {
                (
                    p.candidate.clone(),
                    p.downstream_sid.clone(),
                    p.process_key.clone(),
                )
            })
        })
        .flatten()
    else {
        return Err(AcpError::internal_error().data("cannot switch: session is not pinned"));
    };
    if target == &old_candidate {
        return Ok(vec![]); // already there
    }
    // Validate the target BEFORE summarizing, so an unknown/dead candidate
    // (e.g. `switch=claude/opus` when only `opus[1m]` is declared) fails fast
    // and the session stays put without a wasted summary turn.
    if shared.candidate_runtime(target).is_none() {
        return Err(AcpError::invalid_params().data(format!(
            "cannot switch to {target}: not a routeable candidate (check the exact `agent/model` \
             id, including any `[1m]` suffix)"
        )));
    }

    // 1. Build the handoff. Preferred path: ask the outgoing model to
    //    summarize (capturing its text instead of relaying it). If that model
    //    is offline/rate-limited/crashed, or refuses, or produces nothing,
    //    fall back to a transcript reconstructed from the state-DB logs — which
    //    needs nothing from the old model.
    let live_conn = shared.target_conn(&old_process_key);
    let summary = if let Some(conn) = &live_conn {
        let buffer = Arc::new(Mutex::new(String::new()));
        shared.with_session(router_sid, |s| s.capturing_summary = Some(buffer.clone()));
        let summary_prompt = PromptRequest::new(
            old_down_sid.clone(),
            vec![ContentBlock::from(HANDOFF_SUMMARY_INSTRUCTION.to_string())],
        );
        let result = conn.send_request(summary_prompt).block_task().await;
        shared.with_session(router_sid, |s| s.capturing_summary = None);
        let captured = buffer.lock().unwrap().clone();
        // Accept only a real summary; a too-short/empty capture or an error
        // means the model didn't actually summarize.
        if result.is_ok() && captured.trim().len() >= 20 {
            Some(captured)
        } else {
            tracing::warn!(
                session = router_sid,
                from = %old_candidate,
                ok = result.is_ok(),
                len = captured.trim().len(),
                "handoff summary failed; falling back to log transcript"
            );
            None
        }
    } else {
        tracing::warn!(
            session = router_sid,
            from = %old_candidate,
            "outgoing model has no live process; using log-transcript handoff"
        );
        None
    };

    // Framed handoff block + a kind tag for the disclosure.
    let (handoff, handoff_note): (Option<String>, &str) = match summary {
        Some(s) => (
            Some(frame_summary(&old_candidate, &s)),
            "summarized by the previous model",
        ),
        None => {
            let transcript = transcript_from_logs(shared, router_sid);
            if transcript.trim().is_empty() {
                (None, "no prior context was available to carry over")
            } else {
                (
                    Some(frame_transcript(&old_candidate, &transcript)),
                    "previous model unavailable — prior context recovered from logs as a truncated transcript",
                )
            }
        }
    };

    // 2. Open a fresh session on the target with the same workspace + MCP.
    let (cwd, dirs, client_mcp, applied_mode) = shared
        .with_session(router_sid, |s| {
            (
                s.cwd.clone(),
                s.additional_directories.clone(),
                s.mcp_servers.clone(),
                s.applied_mode.clone(),
            )
        })
        .ok_or_else(|| AcpError::invalid_params().data("unknown session"))?;
    let mcp_servers = mcp_servers_for_pin(shared, router_sid, target, &client_mcp);
    let opened = open_downstream_session(
        shared,
        target,
        cwd,
        dirs,
        mcp_servers,
        DownstreamRoute::Primary {
            router_sid: router_sid.to_string(),
        },
    )
    .await?;

    // 3. Re-pin, seed the summary as context for the next prompt, reset the
    //    confidence baseline to the new (more capable) model.
    let pin_quality = shared
        .with_session(router_sid, |s| {
            s.task_class
                .map(|c| shared.scores.lookup(target).quality(c))
                .unwrap_or(0.5)
        })
        .unwrap_or(0.5);
    shared.with_session(router_sid, |s| {
        s.pin = Some(PinInfo {
            candidate: target.clone(),
            process_key: opened.process_key.clone(),
            downstream_sid: opened.downstream_sid.clone(),
            available_modes: opened
                .modes
                .as_ref()
                .map(|m| {
                    m.available_modes
                        .iter()
                        .map(|md| md.id.0.to_string())
                        .collect()
                })
                .unwrap_or_default(),
        });
        s.pinned_quality = pin_quality;
        s.struggle = 0.0;
        s.pending_switch = None;
        s.pending_context = handoff.clone();
    });

    // 4. Re-apply the session mode on the new downstream (best effort).
    if let Some(requested) = applied_mode {
        let modes: Vec<String> = opened
            .modes
            .as_ref()
            .map(|m| {
                m.available_modes
                    .iter()
                    .map(|md| md.id.0.to_string())
                    .collect()
            })
            .unwrap_or_default();
        if let Some(mode_id) = resolve_mode_id(shared, &target.agent, &requested, &modes) {
            let set = SetSessionModeRequest::new(opened.downstream_sid.clone(), mode_id);
            let _ = opened.conn.send_request(set).block_task().await;
        }
    }

    // 5. Persist + close the old session.
    shared.state.lock().unwrap().upsert(
        router_sid.to_string(),
        PersistedSession {
            agent: target.agent.clone(),
            model: target.model.clone(),
            downstream_session_id: opened.downstream_sid.clone(),
            cwd: shared
                .with_session(router_sid, |s| s.cwd.clone())
                .unwrap_or_default(),
            additional_directories: shared
                .with_session(router_sid, |s| s.additional_directories.clone())
                .unwrap_or_default(),
            // Record the switch lineage: the downstream session this router
            // session was pinned to before this switch.
            prior_session_id: Some(old_down_sid.clone()),
            routing: Some(serde_json::json!({
                "strategy": "switch",
                "candidate": target.to_string(),
                "from": old_candidate.to_string(),
                "reason": reason,
            })),
            ..Default::default()
        },
    );
    if let Some(old_key) = shared
        .candidate_runtime(&old_candidate)
        .map(|r| r.process_key)
    {
        close_downstream_session(shared, &old_key, &old_down_sid);
    }
    {
        let mut headroom = shared.headroom.lock().unwrap();
        headroom.record_session(&target.agent);
    }
    tracing::info!(session = router_sid, from = %old_candidate, to = %target, reason, handoff = handoff_note, "session model switched");

    Ok(vec![
        format!("router-acp · switched {old_candidate} → {target} — {reason}"),
        format!("note: {handoff_note}; the new model does not see the earlier transcript verbatim"),
    ])
}

/// True when a prompt is goose's session-title/name meta-request rather than
/// real conversational work.
pub fn is_title_generation(prompt: &[ContentBlock]) -> bool {
    let text = prompt_display_text(prompt).to_lowercase();
    text.contains("generate a short title")
        || text.contains("generate a title")
        || text.contains("short title for the above")
}

/// Answer a meta prompt (e.g. goose's title generation) on the cheapest
/// routeable candidate in a throwaway downstream session, WITHOUT pinning the
/// router session. The real first prompt then pins normally with its
/// directive intact. Falls back to a trivial synthesized reply if no
/// candidate can serve it, so goose never blocks on a title.
async fn handle_meta_prompt(
    shared: Arc<Shared>,
    router_sid: String,
    req: PromptRequest,
    responder: Responder<PromptResponse>,
) -> Result<(), AcpError> {
    // Cheapest routeable candidate (title generation is trivial).
    let mut pool = shared.eligible_views(&RequiredCaps::default(), TaskClass::Writing);
    pool.sort_by(|a, b| {
        a.cost_rank
            .cmp(&b.cost_rank)
            .then_with(|| a.config_index.cmp(&b.config_index))
    });
    let (cwd, dirs) = shared
        .with_session(&router_sid, |s| {
            (s.cwd.clone(), s.additional_directories.clone())
        })
        .unwrap_or_default();

    for view in pool {
        match open_downstream_session(
            &shared,
            &view.id,
            cwd.clone(),
            dirs.clone(),
            Vec::new(), // no MCP servers for a title
            DownstreamRoute::Primary {
                router_sid: router_sid.clone(),
            },
        )
        .await
        {
            Ok(opened) => {
                let fwd = PromptRequest::new(opened.downstream_sid.clone(), req.prompt.clone());
                let result = opened.conn.send_request(fwd).block_task().await;
                close_downstream_session(&shared, &opened.process_key, &opened.downstream_sid);
                tracing::debug!(
                    session = router_sid,
                    candidate = %view.id,
                    "title/meta prompt served without pinning"
                );
                return match result {
                    Ok(resp) => responder.respond(resp),
                    Err(err) => responder.respond_with_error(err),
                };
            }
            Err(err) => {
                tracing::debug!(candidate = %view.id, %err, "meta prompt candidate failed");
            }
        }
    }
    // No candidate served it: end the turn cleanly so goose just skips the
    // auto-title (harmless) rather than blocking.
    responder.respond(PromptResponse::new(StopReason::EndTurn))
}

async fn route_and_pin(
    shared: Arc<Shared>,
    router_sid: String,
    req: PromptRequest,
    responder: Responder<PromptResponse>,
) -> Result<(), AcpError> {
    let outcome = pin_session(
        &shared,
        &router_sid,
        &req.prompt,
        &responder.cancellation(),
        None,
        false,
    )
    .await;
    shared.with_session(&router_sid, |s| s.pinning = false);
    match outcome {
        Ok(PinOutcome::Pinned) => {
            send_prompt_with_failover(shared, router_sid, req, responder).await
        }
        Ok(PinOutcome::Cancelled) => responder.respond(PromptResponse::new(StopReason::Cancelled)),
        Err(err) => responder.respond_with_error(err),
    }
}

// ----------------------------------------------------------------------
// Initialize
// ----------------------------------------------------------------------

fn build_initialize_response(shared: &Arc<Shared>) -> InitializeResponse {
    let keys = shared.target_keys();
    let targets = shared.targets.lock().unwrap();
    let mut prompt = PromptCapabilities::new();
    let mut mcp = McpCapabilities::new();
    let mut load_session = false;
    let mut any_list = false;
    let mut any_delete = false;
    let mut any_resume = false;
    let mut any_close = false;
    let mut any_dirs = false;
    let mut auth_methods: Vec<AuthMethod> = Vec::new();

    // Conservative union across targets that initialized (routeable or
    // auth-pending agents).
    let mut seen_agents: Vec<String> = Vec::new();
    for key in keys {
        let Some(t) = targets.get(&key) else { continue };
        let Some(init) = &t.init else { continue };
        let caps = &init.agent_capabilities;
        prompt.image |= caps.prompt_capabilities.image;
        prompt.audio |= caps.prompt_capabilities.audio;
        prompt.embedded_context |= caps.prompt_capabilities.embedded_context;
        mcp.http |= caps.mcp_capabilities.http;
        mcp.sse |= caps.mcp_capabilities.sse;
        load_session |= caps.load_session;
        any_list |= caps.session_capabilities.list.is_some();
        any_delete |= caps.session_capabilities.delete.is_some();
        any_resume |= caps.session_capabilities.resume.is_some();
        any_close |= caps.session_capabilities.close.is_some();
        any_dirs |= caps.session_capabilities.additional_directories.is_some();

        // Namespace downstream auth methods as `<agent>/<methodId>`. Only
        // one target per agent contributes (config-option agents have one
        // target; spawn-config targets share the same auth methods).
        if !seen_agents.contains(&t.spec.agent_name) {
            seen_agents.push(t.spec.agent_name.clone());
            for method in &init.auth_methods {
                match method {
                    AuthMethod::Agent(m) => {
                        let namespaced = AuthMethodAgent::new(
                            format!("{}/{}", t.spec.agent_name, m.id.0),
                            format!("{}: {}", t.spec.agent_name, m.name),
                        )
                        .description(m.description.clone());
                        auth_methods.push(AuthMethod::Agent(namespaced));
                    }
                    other => {
                        tracing::debug!(
                            agent = t.spec.agent_name,
                            "skipping unsupported auth method shape: {other:?}"
                        );
                    }
                }
            }
        }
    }

    let mut session_caps = SessionCapabilities::new();
    if any_list {
        session_caps = session_caps.list(Some(Default::default()));
    }
    if any_delete {
        session_caps = session_caps.delete(Some(Default::default()));
    }
    if any_resume {
        session_caps = session_caps.resume(Some(Default::default()));
    }
    if any_close {
        session_caps = session_caps.close(Some(Default::default()));
    }
    if any_dirs {
        session_caps = session_caps.additional_directories(Some(Default::default()));
    }

    let capabilities = AgentCapabilities::new()
        .load_session(load_session)
        .prompt_capabilities(prompt)
        .mcp_capabilities(mcp)
        .session_capabilities(session_caps);

    InitializeResponse::new(ProtocolVersion::V1)
        .agent_capabilities(capabilities)
        .auth_methods(auth_methods)
        .agent_info(Implementation::new("router-acp", env!("CARGO_PKG_VERSION")))
}

// ----------------------------------------------------------------------
// The upstream agent surface
// ----------------------------------------------------------------------

/// Serve the router as an ACP agent over the given transport (stdio in
/// production, an in-process channel in tests).
pub async fn serve(
    cfg: Config,
    transport: impl ConnectTo<AgentPeer> + 'static,
) -> Result<(), AcpError> {
    let shared = Shared::new(cfg)?;
    serve_shared(shared, transport).await
}

pub async fn serve_shared(
    shared: Arc<Shared>,
    transport: impl ConnectTo<AgentPeer> + 'static,
) -> Result<(), AcpError> {
    // Delegate MCP listener (Unix socket) runs independently of the ACP
    // connection; aborted when serve returns.
    let listener_task = if shared.cfg.delegation.enabled {
        match crate::delegate_mcp::bind_listener(&shared) {
            Ok(task) => Some(task),
            Err(err) => {
                tracing::warn!(%err, "delegation disabled: cannot bind delegate socket");
                None
            }
        }
    } else {
        None
    };

    // Proactive usage-cap cordoning: poll each usage-source agent's provider
    // usage API on an interval and cordon exhausted candidates.
    let usage_task = crate::usage::spawn_usage_poller(&shared);

    let result = build_agent(shared).connect_to(transport).await;

    if let Some(task) = listener_task {
        task.abort();
    }
    if let Some(task) = usage_task {
        task.abort();
    }
    result
}

fn build_agent(
    shared: Arc<Shared>,
) -> agent_client_protocol::Builder<
    AgentPeer,
    impl agent_client_protocol::HandleDispatchFrom<ClientPeer> + 'static,
    impl agent_client_protocol::RunWithConnectionTo<ClientPeer> + 'static,
> {
    let s_init = shared.clone();
    let s_auth = shared.clone();
    let s_new = shared.clone();
    let s_cfg = shared.clone();
    let s_mode = shared.clone();
    let s_prompt = shared.clone();
    let s_cancel = shared.clone();
    let s_list = shared.clone();
    let s_load = shared.clone();
    let s_resume = shared.clone();
    let s_delete = shared.clone();
    let s_close = shared.clone();
    let s_catch = shared.clone();

    AgentPeer
        .builder()
        .name("router-acp")
        // -------------------------------------------------- initialize
        .on_receive_request(
            move |req: InitializeRequest, responder: Responder<InitializeResponse>, cx| {
                let shared = s_init.clone();
                async move { on_initialize(shared, req, responder, cx) }
            },
            on_receive_request!(),
        )
        // -------------------------------------------------- authenticate
        .on_receive_request(
            move |req: AuthenticateRequest, responder: Responder<AuthenticateResponse>, cx| {
                let shared = s_auth.clone();
                async move { on_authenticate(shared, req, responder, cx) }
            },
            on_receive_request!(),
        )
        // -------------------------------------------------- session/new
        .on_receive_request(
            move |req: NewSessionRequest, responder: Responder<NewSessionResponse>, _cx| {
                let shared = s_new.clone();
                async move { on_session_new(shared, req, responder) }
            },
            on_receive_request!(),
        )
        // -------------------------------------- session/set_config_option
        .on_receive_request(
            move |req: SetSessionConfigOptionRequest,
                  responder: Responder<SetSessionConfigOptionResponse>,
                  _cx| {
                let shared = s_cfg.clone();
                async move { on_set_config_option(shared, req, responder) }
            },
            on_receive_request!(),
        )
        // -------------------------------------------------- session/set_mode
        .on_receive_request(
            move |req: SetSessionModeRequest,
                  responder: Responder<
                agent_client_protocol::schema::v1::SetSessionModeResponse,
            >,
                  _cx| {
                let shared = s_mode.clone();
                async move {
                    let sid = sid_str(&req.session_id);
                    let requested = req.mode_id.0.to_string();
                    match shared.pinned_route(&sid) {
                        Some((conn, down_sid, candidate)) => {
                            let available = shared
                                .with_session(&sid, |s| {
                                    s.pin.as_ref().map(|p| p.available_modes.clone())
                                })
                                .flatten()
                                .unwrap_or_default();
                            match resolve_mode_id(&shared, &candidate.agent, &requested, &available)
                            {
                                Some(mode_id) => {
                                    let fwd = SetSessionModeRequest::new(down_sid, mode_id)
                                        .meta(req.meta.clone());
                                    relay_request_to_downstream(&shared, conn, fwd, responder)
                                }
                                None => {
                                    // Lenient: report success so mode-eager
                                    // clients (goose) keep the session alive;
                                    // the downstream stays in its own mode.
                                    tracing::warn!(
                                        session = sid,
                                        requested,
                                        ?available,
                                        "requested session mode has no equivalent on the pinned \
                                         candidate; leaving downstream mode unchanged"
                                    );
                                    responder.respond(
                                        agent_client_protocol::schema::v1::SetSessionModeResponse::new(),
                                    )
                                }
                            }
                        }
                        None => {
                            // Defer: clients like goose set their mode right
                            // after session/new, before any prompt exists.
                            let known = shared
                                .with_session(&sid, |s| {
                                    s.pending_mode = Some(requested.clone());
                                })
                                .is_some();
                            if known {
                                tracing::debug!(
                                    session = sid,
                                    mode = requested,
                                    "session mode deferred until the first prompt pins a candidate"
                                );
                                responder.respond(
                                    agent_client_protocol::schema::v1::SetSessionModeResponse::new(),
                                )
                            } else {
                                responder.respond_with_error(
                                    AcpError::invalid_params().data("unknown session id"),
                                )
                            }
                        }
                    }
                }
            },
            on_receive_request!(),
        )
        // -------------------------------------------------- session/prompt
        .on_receive_request(
            move |req: PromptRequest, responder: Responder<PromptResponse>, cx| {
                let shared = s_prompt.clone();
                async move { on_prompt(shared, req, responder, cx) }
            },
            on_receive_request!(),
        )
        // -------------------------------------------------- session/cancel
        .on_receive_notification(
            move |notif: CancelNotification, _cx| {
                let shared = s_cancel.clone();
                async move { on_cancel(shared, notif) }
            },
            on_receive_notification!(),
        )
        // -------------------------------------------------- session/list
        .on_receive_request(
            move |req: ListSessionsRequest, responder: Responder<ListSessionsResponse>, cx| {
                let shared = s_list.clone();
                async move { on_session_list(shared, req, responder, cx) }
            },
            on_receive_request!(),
        )
        // -------------------------------------------------- session/load
        .on_receive_request(
            move |req: LoadSessionRequest,
                  responder: Responder<agent_client_protocol::schema::v1::LoadSessionResponse>,
                  cx| {
                let shared = s_load.clone();
                async move { crate::lifecycle::on_session_load(shared, req, responder, cx) }
            },
            on_receive_request!(),
        )
        // -------------------------------------------------- session/resume
        .on_receive_request(
            move |req: ResumeSessionRequest,
                  responder: Responder<
                agent_client_protocol::schema::v1::ResumeSessionResponse,
            >,
                  cx| {
                let shared = s_resume.clone();
                async move { crate::lifecycle::on_session_resume(shared, req, responder, cx) }
            },
            on_receive_request!(),
        )
        // -------------------------------------------------- session/delete
        .on_receive_request(
            move |req: DeleteSessionRequest,
                  responder: Responder<
                agent_client_protocol::schema::v1::DeleteSessionResponse,
            >,
                  cx| {
                let shared = s_delete.clone();
                async move { crate::lifecycle::on_session_delete(shared, req, responder, cx) }
            },
            on_receive_request!(),
        )
        // -------------------------------------------------- session/close
        .on_receive_request(
            move |req: CloseSessionRequest, responder: Responder<CloseSessionResponse>, _cx| {
                let shared = s_close.clone();
                async move { crate::lifecycle::on_session_close(shared, req, responder) }
            },
            on_receive_request!(),
        )
        // ------------------------------------------ catch-all (extensions)
        // Relay extension requests/notifications carrying a pinned router
        // session id; everything else falls through to default handling.
        .on_receive_dispatch(
            move |message: Dispatch, _cx| {
                let shared = s_catch.clone();
                async move { on_catch_all(shared, message) }
            },
            on_receive_dispatch!(),
        )
}

fn on_initialize(
    shared: Arc<Shared>,
    req: InitializeRequest,
    responder: Responder<InitializeResponse>,
    cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    if shared.initialized.swap(true, Ordering::SeqCst) {
        // Repeat initialize: answer from current state.
        return responder.respond(build_initialize_response(&shared));
    }
    let _ = shared.upstream.set(cx.clone());
    let _ = shared.client_caps.set(req.client_capabilities.clone());

    let task_shared = shared.clone();
    cx.spawn(async move {
        let keys = task_shared.target_keys();
        // Spawn every downstream process, then probe them concurrently.
        for key in &keys {
            if let Err(err) = start_downstream(&task_shared, key).await {
                task_shared.set_target_failed(key, &format!("failed to start: {err}"));
            }
        }
        let live: Vec<ProcessKey> = keys
            .iter()
            .filter(|k| task_shared.target_conn(k).is_some())
            .cloned()
            .collect();
        futures::future::join_all(live.iter().map(|k| probe_target(&task_shared, k))).await;

        let routeable = task_shared.routeable_candidates();
        let auth_pending = task_shared.has_auth_pending();
        if routeable.is_empty() && !auth_pending {
            let _ = responder.respond_with_error(AcpError::invalid_params().data(
                "router-acp has zero routeable candidates after config/auth/catalog validation; \
                 check agent commands and declared model ids",
            ));
            return Ok(());
        }
        if routeable.len() == 1 {
            tracing::warn!(
                "single routeable candidate: routing and delegation are inert; all sessions pin \
                 to {}",
                routeable[0].id
            );
        }
        let _ = responder.respond(build_initialize_response(&task_shared));
        Ok(())
    })
}

fn on_authenticate(
    shared: Arc<Shared>,
    req: AuthenticateRequest,
    responder: Responder<AuthenticateResponse>,
    cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    let method = req.method_id.0.to_string();
    let Some((agent, downstream_method)) = method.split_once('/') else {
        return responder.respond_with_error(AcpError::invalid_params().data(format!(
            "auth method id `{method}` is not namespaced; expected `<agent>/<methodId>`"
        )));
    };
    let keys = shared.target_keys_for_agent(agent);
    if keys.is_empty() {
        return responder.respond_with_error(
            AcpError::invalid_params().data(format!("unknown agent `{agent}` in auth method id")),
        );
    }
    let downstream_method = downstream_method.to_string();
    let meta = req.meta.clone();
    cx.spawn(async move {
        let mut succeeded = false;
        let mut last_err: Option<AcpError> = None;
        for key in &keys {
            let Some(conn) = shared.target_conn(key) else {
                continue;
            };
            let fwd = AuthenticateRequest::new(downstream_method.clone()).meta(meta.clone());
            match conn.send_request(fwd).block_task().await {
                Ok(_) => succeeded = true,
                Err(err) => {
                    tracing::warn!(target = %key, error = %err, "downstream authenticate failed");
                    last_err = Some(err);
                }
            }
        }
        if !succeeded {
            let _ = responder.respond_with_error(
                last_err.unwrap_or_else(|| AcpError::internal_error().data("no live targets")),
            );
            return Ok(());
        }
        // Re-run probe verification for this agent; success may create the
        // first routeable candidate.
        futures::future::join_all(keys.iter().map(|k| probe_target(&shared, k))).await;
        let _ = responder.respond(AuthenticateResponse::new());
        Ok(())
    })
}

fn on_session_new(
    shared: Arc<Shared>,
    req: NewSessionRequest,
    responder: Responder<NewSessionResponse>,
) -> Result<(), AcpError> {
    if !shared.is_initialized() {
        return responder
            .respond_with_error(AcpError::invalid_request().data("initialize the router first"));
    }
    let routeable = shared.routeable_candidates();
    if routeable.is_empty() {
        return if shared.has_auth_pending() {
            responder.respond_with_error(AcpError::auth_required().data(
                "all candidates are waiting for authentication; call authenticate with a \
                 namespaced method id",
            ))
        } else {
            responder.respond_with_error(
                AcpError::invalid_params()
                    .data("invalid configuration: no routeable or auth-pending candidates remain"),
            )
        };
    }
    let router_sid = format!("rtr-{}", uuid::Uuid::new_v4());
    shared
        .sessions
        .lock()
        .unwrap()
        .insert(router_sid.clone(), RouterSession::new(&shared.cfg, &req));
    let options = shared.router_config_options(&router_sid);
    responder.respond(NewSessionResponse::new(router_sid).config_options(options))
}

fn on_set_config_option(
    shared: Arc<Shared>,
    req: SetSessionConfigOptionRequest,
    responder: Responder<SetSessionConfigOptionResponse>,
) -> Result<(), AcpError> {
    let router_sid = sid_str(&req.session_id);
    let config_id = req.config_id.0.to_string();
    let is_router_option = config_id.starts_with("router.");

    enum Action {
        RouterUpdated,
        AlreadyPinned,
        UnknownSession,
        UnknownConfig,
        BadValue(String),
        Forward(ConnectionTo<AgentPeer>, String),
    }

    let action = {
        let mut sessions = shared.sessions.lock().unwrap();
        match sessions.get_mut(&router_sid) {
            None => Action::UnknownSession,
            Some(session) => {
                if is_router_option {
                    if session.pin.is_some() || session.pinning {
                        Action::AlreadyPinned
                    } else {
                        let value = match &req.value {
                            SessionConfigOptionValue::ValueId { value } => value.0.to_string(),
                            _ => String::new(),
                        };
                        match config_id.as_str() {
                            "router.strategy" => match StrategyKind::parse(&value) {
                                Some(kind) => {
                                    session.strategy = kind;
                                    Action::RouterUpdated
                                }
                                None => Action::BadValue(format!(
                                    "unknown strategy `{value}`; expected auto, pareto-code, \
                                     or static"
                                )),
                            },
                            "router.candidate" => {
                                if value == "auto" {
                                    session.candidate_override = None;
                                    Action::RouterUpdated
                                } else {
                                    match CandidateId::parse(&value) {
                                        Some(id) => {
                                            session.candidate_override = Some(id);
                                            Action::RouterUpdated
                                        }
                                        None => Action::BadValue(format!(
                                            "`{value}` is not `auto` or an `agent/model` \
                                             candidate id"
                                        )),
                                    }
                                }
                            }
                            _ => Action::UnknownConfig,
                        }
                    }
                } else {
                    match &session.pin {
                        Some(pin) => {
                            let conn = shared.target_conn(&pin.process_key);
                            match conn {
                                Some(conn) => Action::Forward(conn, pin.downstream_sid.clone()),
                                None => Action::BadValue(
                                    "downstream process is no longer running".into(),
                                ),
                            }
                        }
                        None => Action::UnknownConfig,
                    }
                }
            }
        }
    };

    match action {
        Action::RouterUpdated => {
            let options = shared.router_config_options(&router_sid);
            responder.respond(SetSessionConfigOptionResponse::new(options))
        }
        Action::AlreadyPinned => responder.respond_with_error(AcpError::invalid_request().data(
            "session already pinned: router strategy/candidate cannot change after the first \
             prompt (ACP has no transcript handoff)",
        )),
        Action::UnknownSession => {
            responder.respond_with_error(AcpError::invalid_params().data("unknown session id"))
        }
        Action::UnknownConfig => responder.respond_with_error(
            AcpError::invalid_params().data(format!("unknown config option id `{config_id}`")),
        ),
        Action::BadValue(msg) => responder.respond_with_error(AcpError::invalid_params().data(msg)),
        Action::Forward(conn, down_sid) => {
            let fwd = SetSessionConfigOptionRequest::new(
                down_sid,
                req.config_id.clone(),
                req.value.clone(),
            )
            .meta(req.meta.clone());
            relay_request_to_downstream(&shared, conn, fwd, responder)
        }
    }
}

fn on_prompt(
    shared: Arc<Shared>,
    mut req: PromptRequest,
    responder: Responder<PromptResponse>,
    cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    let router_sid = sid_str(&req.session_id);

    // goose auto-generates a session title by sending a "Generate a short
    // title…" meta-prompt — with NO routing directive — often as the very
    // first prompt. Letting it pin the session would hijack the pin (to the
    // default strategy) before the recipe's directive-bearing prompt arrives.
    // So while the session is unpinned, answer title-gen on the cheapest
    // candidate as a throwaway that does NOT commit the pin.
    let already_pinned = shared
        .with_session(&router_sid, |s| s.pin.is_some())
        .unwrap_or(false);
    if !already_pinned && is_title_generation(&req.prompt) {
        return cx.spawn(handle_meta_prompt(
            shared.clone(),
            router_sid,
            req,
            responder,
        ));
    }

    // Tracks whether the user steered routing explicitly (a `[router: …]`
    // directive, a `model:` shorthand, or a skill invocation). Any of these
    // suppresses auto-orchestration for this prompt.
    let mut explicit_routing = false;
    // Set by an `orchestrate:` / `orchestrator:` prefix — forces orchestration
    // regardless of list detection.
    let mut force_orchestrate = false;

    // Routing directives: `[router: ...]` anywhere in the prompt. Always
    // stripped (the downstream model never sees them); only applied before
    // the pin.
    match parse_prompt_directives(&req.prompt) {
        Ok(None) => {
            // Model shorthand: a prompt beginning with `model:` (e.g. `opus:`,
            // `codex/gpt-5.5:`, `sonnet: fix this`) is a switch (post-pin) or a
            // pin steer (pre-pin) to the referenced candidate. Resolution gates
            // it — a token that doesn't name an eligible candidate is left as
            // ordinary prose. The reserved tokens `orchestrate:`/`orchestrator:`
            // instead force auto-orchestration on the rest of the prompt.
            if let Some((ref_str, stripped)) = split_model_shorthand(&req.prompt) {
                let lower_ref = ref_str.to_lowercase();
                if lower_ref == "orchestrate" || lower_ref == "orchestrator" {
                    force_orchestrate = true;
                    req =
                        PromptRequest::new(req.session_id.clone(), stripped).meta(req.meta.clone());
                    tracing::info!(session = router_sid, "orchestration forced via prefix");
                } else if let Some(target) = {
                    let (class, excluded) = shared
                        .with_session(&router_sid, |s| {
                            (
                                s.task_class.unwrap_or(TaskClass::CodingGeneral),
                                s.excluded.clone(),
                            )
                        })
                        .unwrap_or((TaskClass::CodingGeneral, Vec::new()));
                    resolve_candidate_ref(&shared, &ref_str, class, &excluded)
                } {
                    let pinned = shared
                        .with_session(&router_sid, |s| s.pin.is_some() || s.pinning)
                        .unwrap_or(false);
                    explicit_routing = true;
                    req =
                        PromptRequest::new(req.session_id.clone(), stripped).meta(req.meta.clone());
                    if pinned {
                        let target = target.clone();
                        shared.with_session(&router_sid, |s| {
                            s.pending_switch = Some(SwitchRequest {
                                target: target.clone(),
                                reason: format!("requested via `{ref_str}:` shorthand"),
                            });
                        });
                    } else {
                        let target = target.clone();
                        shared.with_session(&router_sid, |s| s.candidate_override = Some(target));
                    }
                    tracing::info!(
                        session = router_sid,
                        %target,
                        shorthand = %ref_str,
                        pinned,
                        "model shorthand routing"
                    );
                }
            }
        }
        Ok(Some((directives, stripped))) => {
            explicit_routing = true;
            req = PromptRequest::new(req.session_id.clone(), stripped).meta(req.meta.clone());
            let pinned_now = shared
                .with_session(&router_sid, |s| s.pin.is_some() || s.pinning)
                .unwrap_or(false);
            // `switch=` is the one directive valid mid-session: it re-pins a
            // live session onto a new candidate (summarize + hand off). Before
            // the pin it degrades to `candidate=`.
            if let Some(target) = directives.switch.clone() {
                if pinned_now {
                    shared.with_session(&router_sid, |s| {
                        s.pending_switch = Some(SwitchRequest {
                            target: target.clone(),
                            reason: "requested via [router: switch=…]".to_string(),
                        });
                    });
                    tracing::info!(session = router_sid, %target, "mid-session switch requested");
                } else {
                    shared.with_session(&router_sid, |s| s.candidate_override = Some(target));
                }
            }
            // The remaining directives only shape the (still-unmade) routing
            // decision, so they apply pre-pin only.
            let has_pre_pin_directives = directives.strategy.is_some()
                || directives.candidate.is_some()
                || directives.prefer.is_some()
                || !directives.exclude.is_empty()
                || directives.label.is_some();
            let applied = shared
                .with_session(&router_sid, |s| {
                    if s.pin.is_some() || s.pinning {
                        false
                    } else {
                        if let Some(strategy) = directives.strategy {
                            s.strategy = strategy;
                        }
                        if let Some(candidate) = directives.candidate.clone() {
                            s.candidate_override = Some(candidate);
                        }
                        if let Some(prefer) = directives.prefer.clone() {
                            s.preferred_candidate = Some(prefer);
                        }
                        s.excluded.extend(directives.exclude.clone());
                        if directives.label.is_some() {
                            s.run_label = directives.label.clone();
                        }
                        true
                    }
                })
                .unwrap_or(false);
            if applied {
                tracing::info!(
                    session = router_sid,
                    ?directives,
                    "routing directives applied from prompt"
                );
            } else if has_pre_pin_directives {
                notify_user(
                    &shared,
                    &router_sid,
                    "router-acp · note: routing directive ignored (session already pinned; \
                     use switch= to change models mid-session)",
                );
            }
        }
        Err(msg) => {
            return responder.respond_with_error(
                AcpError::invalid_params().data(format!("invalid routing directive: {msg}")),
            );
        }
    }

    // A prompt that carried only a directive (empty after stripping) is valid
    // mid-session — e.g. a bare `[router: switch=…]`. Post-pin, synthesize a
    // minimal continuation so the (possibly just-switched) model has something
    // to answer; pre-pin there is nothing to route, so it stays an error.
    if prompt_is_empty(&req.prompt) {
        if already_pinned {
            req = PromptRequest::new(
                req.session_id.clone(),
                vec![ContentBlock::from(
                    "(The user sent only a router directive, with no message. If you just took \
                     over this conversation via a model switch, briefly confirm which model you \
                     are and that you have the handoff context, then wait for their next \
                     instruction. Otherwise, ask what they'd like to do next.)"
                        .to_string(),
                )],
            )
            .meta(req.meta.clone());
        } else {
            return responder.respond_with_error(
                AcpError::invalid_params()
                    .data("prompt contains only a routing directive and no actual task"),
            );
        }
    }

    // The rest of prompt handling runs in a spawned task: ticket-context
    // enrichment shells out (async), and orchestration/classification must see
    // the ENRICHED prompt — "Fix HAI-1234" routes on the ticket's real content.
    cx.spawn(async move {
        let req = crate::tickets::enrich_prompt(&shared, &router_sid, req).await;
        dispatch_prompt(
            shared,
            router_sid,
            req,
            responder,
            explicit_routing,
            force_orchestrate,
        )
        .await
    })
}

/// Post-directive prompt handling: auto-orchestration, skill routing, and the
/// relay/pin dispatch. Runs inside a spawned task (never on the dispatch loop);
/// the prompt has already been ticket-enriched.
async fn dispatch_prompt(
    shared: Arc<Shared>,
    router_sid: String,
    req: PromptRequest,
    responder: Responder<PromptResponse>,
    explicit_routing: bool,
    force_orchestrate: bool,
) -> Result<(), AcpError> {
    // Auto-orchestration runs FIRST: a multi-part task list orchestrates even if
    // it names a skill — the planner decides when to invoke that skill, and
    // end-of-work skills (shipping, PRs) run last. Suppressed only by an explicit
    // `[router: …]` directive or `model:` shorthand (they set explicit_routing);
    // FORCED unconditionally by the `orchestrate:` prefix.
    let orchestrating_now = if force_orchestrate || !explicit_routing {
        maybe_trigger_orchestration(&shared, &router_sid, &req.prompt, force_orchestrate)
    } else {
        false
    };

    // Skill routing: certain skills (e.g. ship-pr) demand a capable model class.
    // Only when the prompt is NOT an orchestrated multi-part task: if it invokes
    // a skill, steer routing to its preferred candidates — pre-pin via
    // candidate_override, mid-session via a switch.
    if !orchestrating_now && let Some(route) = detect_skill_route(&shared.cfg, &req.prompt) {
        let (class, excluded, current) = shared
            .with_session(&router_sid, |s| {
                (
                    s.task_class.unwrap_or(TaskClass::CodingGeneral),
                    s.excluded.clone(),
                    s.pin.as_ref().map(|p| p.candidate.clone()),
                )
            })
            .unwrap_or((TaskClass::CodingGeneral, Vec::new(), None));
        let already_ok = current
            .as_ref()
            .map(|c| route.candidates.iter().any(|rc| candidate_matches(rc, c)))
            .unwrap_or(false);
        if !already_ok {
            match first_eligible_candidate(&shared, &route.candidates, class, &excluded) {
                Some(target) => {
                    let pattern = route.pattern.clone();
                    if current.is_some() {
                        shared.with_session(&router_sid, |s| {
                            s.pending_switch = Some(SwitchRequest {
                                target: target.clone(),
                                reason: format!(
                                    "skill `{pattern}` requires a {target}-class model"
                                ),
                            });
                        });
                        tracing::info!(session = router_sid, skill = %pattern, %target, "skill switch queued");
                    } else {
                        shared.with_session(&router_sid, |s| {
                            s.candidate_override = Some(target.clone());
                        });
                        notify_user(
                            &shared,
                            &router_sid,
                            format!(
                                "router-acp · skill `{pattern}` steering this session to {target}"
                            ),
                        );
                        tracing::info!(session = router_sid, skill = %pattern, %target, "skill pin steered");
                    }
                }
                None => notify_user(
                    &shared,
                    &router_sid,
                    format!(
                        "router-acp · skill `{}` prefers {:?} but none are available; \
                         keeping current routing",
                        route.pattern, route.candidates
                    ),
                ),
            }
        }
    }

    enum Action {
        Relay(ConnectionTo<AgentPeer>, String, String),
        Pin,
        Unknown,
        Busy,
    }

    let action = {
        let mut sessions = shared.sessions.lock().unwrap();
        match sessions.get_mut(&router_sid) {
            None => Action::Unknown,
            Some(session) => match &session.pin {
                Some(pin) => match shared.target_conn(&pin.process_key) {
                    Some(conn) => Action::Relay(
                        conn,
                        pin.downstream_sid.clone(),
                        pin.candidate.agent.clone(),
                    ),
                    None => Action::Unknown,
                },
                None if session.pinning => Action::Busy,
                None => {
                    session.pinning = true;
                    session.cancelled = false;
                    Action::Pin
                }
            },
        }
    };

    match action {
        Action::Relay(_conn, _down_sid, _agent) => {
            // A new turn starts: clear the previous turn's cancel flag so it
            // cannot suppress failover for this prompt. Prompt accounting and
            // forwarding happen inside the failover-aware sender.
            shared.with_session(&router_sid, |s| s.cancelled = false);
            send_prompt_with_failover(shared.clone(), router_sid, req, responder).await
        }
        Action::Pin => route_and_pin(shared.clone(), router_sid, req, responder).await,
        Action::Busy => responder.respond_with_error(AcpError::invalid_request().data(
            "a routing decision for this session is already in flight; await the first prompt's \
             response",
        )),
        Action::Unknown => responder.respond_with_error(
            AcpError::invalid_params().data("unknown session id (or its downstream process died)"),
        ),
    }
}

fn on_cancel(shared: Arc<Shared>, notif: CancelNotification) -> Result<(), AcpError> {
    let router_sid = sid_str(&notif.session_id);
    let (pin, delegates) = shared
        .with_session(&router_sid, |s| {
            s.cancelled = true;
            (s.pin.clone(), s.delegates.clone())
        })
        .unwrap_or((None, Vec::new()));
    if let Some(pin) = pin
        && let Some(conn) = shared.target_conn(&pin.process_key)
    {
        let _ = conn.send_notification(
            CancelNotification::new(pin.downstream_sid.clone()).meta(notif.meta.clone()),
        );
    }
    // Parent cancel propagates to all active delegated sub-sessions.
    for d in delegates {
        if let Some(conn) = shared.target_conn(&d.process_key) {
            let _ = conn.send_notification(CancelNotification::new(d.downstream_sid.clone()));
        }
    }
    Ok(())
}

fn on_session_list(
    shared: Arc<Shared>,
    _req: ListSessionsRequest,
    responder: Responder<ListSessionsResponse>,
    cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    cx.spawn(async move {
        let mut merged = Vec::new();
        for key in shared.target_keys() {
            let (conn, agent_name, supports) = {
                let targets = shared.targets.lock().unwrap();
                let Some(t) = targets.get(&key) else { continue };
                let supports = t
                    .init
                    .as_ref()
                    .map(|i| i.agent_capabilities.session_capabilities.list.is_some())
                    .unwrap_or(false);
                (t.conn.clone(), t.spec.agent_name.clone(), supports)
            };
            let Some(conn) = conn else { continue };
            if !supports {
                continue;
            }
            match conn
                .send_request(ListSessionsRequest::new())
                .block_task()
                .await
            {
                Ok(resp) => {
                    for info in resp.sessions {
                        // Rewrite downstream ids to router ids; include only
                        // sessions the router knows how to route back.
                        let down_sid = sid_str(&info.session_id);
                        let router_sid = shared
                            .state
                            .lock()
                            .unwrap()
                            .find_by_downstream(&agent_name, &down_sid);
                        if let Some(router_sid) = router_sid {
                            let mut info = info;
                            info.session_id = router_sid.into();
                            merged.push(info);
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(target = %key, error = %err, "session/list failed downstream");
                }
            }
        }
        let _ = responder.respond(ListSessionsResponse::new(merged));
        Ok(())
    })
}

fn on_catch_all(shared: Arc<Shared>, message: Dispatch) -> Result<Handled<Dispatch>, AcpError> {
    if matches!(message, Dispatch::Response(..)) {
        return Ok(Handled::No {
            message,
            retry: false,
        });
    }
    // Router-owned extension: a client's seat-availability hint (session-less;
    // consumed here, never relayed downstream).
    if let Dispatch::Notification(msg) = &message
        && msg.method() == crate::usage::AVAILABILITY_HINT_METHOD
    {
        crate::usage::apply_availability_hint(&shared, msg.params());
        return Ok(Handled::Yes);
    }
    let Some(router_sid) = message.message().and_then(relay::session_id_of) else {
        return Ok(Handled::No {
            message,
            retry: false,
        });
    };
    let Some((conn, down_sid, _)) = shared.pinned_route(&router_sid) else {
        return Ok(Handled::No {
            message,
            retry: false,
        });
    };
    match message {
        Dispatch::Request(msg, responder) => {
            let fwd = relay::with_session_id(&msg, &down_sid)?;
            relay_request_to_downstream(&shared, conn, fwd, responder)?;
            Ok(Handled::Yes)
        }
        Dispatch::Notification(msg) => {
            let fwd = relay::with_session_id(&msg, &down_sid)?;
            conn.send_notification(fwd)?;
            Ok(Handled::Yes)
        }
        Dispatch::Response(..) => unreachable!(),
    }
}

#[cfg(test)]
mod escalation_signal_tests {
    use super::*;

    #[test]
    fn read_only_commands_are_investigation() {
        for cmd in [
            "git status",
            "git log --oneline -20",
            "ls -la src",
            "grep -rn foo src",
            "rg 'fn main'",
            "find . -type f -name '*.py' | grep profile | head -20",
            "cat Cargo.toml",
            "git diff HEAD~1",
            // stderr / dev-null redirects are harmless (the hickory-ai6 bug):
            "ls -la /some/dir 2>/dev/null || echo \"not found\"",
            "grep -r foo . 2>/dev/null",
            "cat missing 2>&1",
        ] {
            assert!(is_read_only_command(cmd), "should be read-only: {cmd}");
        }
    }

    #[test]
    fn mutating_commands_are_side_effects() {
        for cmd in [
            "rm -rf build",
            "git commit -m x",
            "git push",
            "cargo build --release",
            "echo hi > file.txt",
            "sed -i 's/a/b/' f",
            "mkdir out",
            "npm run build",
            "cat a > b",
        ] {
            assert!(!is_read_only_command(cmd), "should be a side effect: {cmd}");
        }
    }

    #[test]
    fn mcp_read_vs_write_tools() {
        for t in [
            "ToolSearch",
            "mcp__slack__search_channels",
            "mcp__gmail__get_thread",
            "list_files",
        ] {
            assert!(is_read_only_mcp(t), "read-only MCP: {t}");
        }
        for t in [
            "mcp__slack__send_message",
            "create_draft",
            "mcp__x__update_canvas",
            "delete_label",
        ] {
            assert!(!is_read_only_mcp(t), "write MCP: {t}");
        }
    }

    #[test]
    fn classify_tool_covers_kinds_and_deferral() {
        let inv = serde_json::json!({"kind": "read", "toolCallId": "t1"});
        assert!(matches!(classify_tool(&inv), ToolClass::Investigation));

        let ro_bash = serde_json::json!({
            "kind": "execute", "rawInput": {"command": "grep -rn foo ."}, "toolCallId": "t2"
        });
        assert!(matches!(classify_tool(&ro_bash), ToolClass::Investigation));

        let mut_bash = serde_json::json!({
            "kind": "execute", "rawInput": {"command": "git commit -m x"}, "toolCallId": "t3"
        });
        assert!(matches!(classify_tool(&mut_bash), ToolClass::SideEffect));

        // execute with no command yet (initial pending frame) → defer.
        let pending = serde_json::json!({"kind": "execute", "rawInput": {}, "toolCallId": "t4"});
        assert!(matches!(classify_tool(&pending), ToolClass::Defer));

        let edit = serde_json::json!({"kind": "edit", "toolCallId": "t5"});
        assert!(matches!(classify_tool(&edit), ToolClass::SideEffect));

        let ro_mcp = serde_json::json!({
            "kind": "other", "_meta": {"claudeCode": {"toolName": "ToolSearch"}}, "toolCallId": "t6"
        });
        assert!(matches!(classify_tool(&ro_mcp), ToolClass::Investigation));

        // status-only frame (no kind) → defer, never a spurious side effect.
        let status_only = serde_json::json!({"toolCallId": "t7", "status": "completed"});
        assert!(matches!(classify_tool(&status_only), ToolClass::Defer));
    }
}

#[cfg(test)]
mod orchestration_unit_tests {
    use super::is_native_subagent_tool;
    use super::previous_turn_solicited_answers as solicited;
    use serde_json::json;

    #[test]
    fn native_subagent_tool_detected_by_name_not_delegate() {
        // Claude's built-in Task tool (via _meta.claudeCode.toolName).
        assert!(is_native_subagent_tool(
            &json!({"_meta": {"claudeCode": {"toolName": "Task"}}})
        ));
        // Fallback to title.
        assert!(is_native_subagent_tool(&json!({"title": "Task"})));
        assert!(is_native_subagent_tool(&json!({"title": "dispatch_agent"})));
        // The router's own tools must NOT match.
        assert!(!is_native_subagent_tool(
            &json!({"_meta": {"claudeCode": {"toolName": "delegate_task"}}})
        ));
        assert!(!is_native_subagent_tool(
            &json!({"title": "delegate_followup"})
        ));
        // Ordinary tools don't match.
        assert!(!is_native_subagent_tool(&json!({"title": "Read File"})));
        assert!(!is_native_subagent_tool(&json!({"kind": "execute"})));
    }

    #[test]
    fn detects_enumerated_decisions_from_the_agent() {
        assert!(solicited(
            "Open decisions: (1) which database? (2) which auth provider?"
        ));
        assert!(solicited(
            "A few questions:\n1. DB choice\n2. deploy target?"
        ));
    }

    #[test]
    fn detects_multiple_questions() {
        assert!(solicited(
            "What database should we use? What about the auth provider?"
        ));
    }

    #[test]
    fn detects_solicit_phrases_without_question_marks() {
        assert!(solicited(
            "These are up to you: postgres or mysql, oauth or basic."
        ));
        assert!(solicited("Please confirm the plan before I proceed."));
    }

    #[test]
    fn does_not_fire_on_a_plain_completion() {
        assert!(!solicited(
            "Done — I fixed the bug in auth.rs and the tests pass."
        ));
        // A single casual question is not a multi-answer solicitation.
        assert!(!solicited(
            "I refactored the handler. Does that look right?"
        ));
        // No prior turn (fresh session).
        assert!(!solicited(""));
    }
}

#[cfg(test)]
mod directive_tests {
    use super::*;

    fn text(blocks: &[ContentBlock]) -> String {
        blocks
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text(t) => Some(t.text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn directive_on_first_line() {
        let prompt = vec![ContentBlock::from(
            "[router: candidate=claude/sonnet]\ndo the thing".to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.candidate.unwrap().to_string(), "claude/sonnet");
        assert_eq!(text(&stripped), "do the thing");
    }

    #[test]
    fn directive_survives_goose_turn_context_preamble() {
        // goose prepends a <turn-context> block, pushing the directive off
        // line 1. The parser must still find and strip exactly that line —
        // and a bracketed model id like `claude-fable-5[1m]` must parse.
        let prompt = vec![ContentBlock::from(
            "<turn-context>\n<current-time>2026-07-10</current-time>\n</turn-context>\n\
             [router: candidate=claude/claude-fable-5[1m], label=orchestrate]\n\n\
             Orchestrate this task."
                .to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(
            dir.candidate.unwrap().to_string(),
            "claude/claude-fable-5[1m]"
        );
        assert_eq!(dir.label.as_deref(), Some("orchestrate"));
        let out = text(&stripped);
        assert!(!out.contains("[router:"), "directive stripped: {out}");
        assert!(out.contains("<turn-context>"), "preamble preserved: {out}");
        assert!(
            out.contains("Orchestrate this task."),
            "task preserved: {out}"
        );
    }

    #[test]
    fn detects_goose_title_generation() {
        assert!(is_title_generation(&[ContentBlock::from(
            "---BEGIN USER MESSAGES--- hi ---END USER MESSAGES---  Generate a short title for the \
             above messages."
                .to_string()
        )]));
        assert!(!is_title_generation(&[ContentBlock::from(
            "Fix the bug in main.rs".to_string()
        )]));
    }

    #[test]
    fn no_directive_returns_none() {
        let prompt = vec![ContentBlock::from("just a normal prompt".to_string())];
        assert!(parse_prompt_directives(&prompt).unwrap().is_none());
    }

    #[test]
    fn invalid_key_errors() {
        let prompt = vec![ContentBlock::from("[router: bogus=x]\nhi".to_string())];
        assert!(parse_prompt_directives(&prompt).is_err());
    }

    #[test]
    fn parses_prefer_and_switch_directives() {
        let prompt = vec![ContentBlock::from(
            "[router: prefer=codex/gpt-5.5, switch=claude/opus[1m]]\ngo".to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.prefer.unwrap().to_string(), "codex/gpt-5.5");
        assert_eq!(dir.switch.unwrap().to_string(), "claude/opus[1m]");
        assert_eq!(text(&stripped), "go");
    }

    #[test]
    fn directive_and_task_on_the_same_line() {
        // The model id has a nested `[1m]` bracket AND the task follows on the
        // same line — both must be handled by depth-matching the directive.
        let prompt = vec![ContentBlock::from(
            "[router: switch=claude/opus[1m]] now what model are you?".to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.switch.unwrap().to_string(), "claude/opus[1m]");
        assert_eq!(text(&stripped), "now what model are you?");
    }

    #[test]
    fn directive_only_prompt_is_allowed_and_strips_to_empty() {
        let prompt = vec![ContentBlock::from(
            "[router: switch=claude/opus]".to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.switch.unwrap().to_string(), "claude/opus");
        assert!(prompt_is_empty(&stripped), "task is empty: {:?}", stripped);
    }

    #[test]
    fn directive_with_trailing_text_after_bracket() {
        // goose can append its own text after the user's directive line.
        let prompt = vec![ContentBlock::from(
            "[router: candidate=claude/sonnet] please continue\nand also this".to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.candidate.unwrap().to_string(), "claude/sonnet");
        let out = text(&stripped);
        assert!(out.contains("please continue"), "got: {out}");
        assert!(out.contains("and also this"), "got: {out}");
        assert!(!out.contains("[router:"), "directive stripped: {out}");
    }

    #[test]
    fn model_shorthand_splits_token_and_task() {
        // bare model + task
        let (r, s) =
            split_model_shorthand(&[ContentBlock::from("opus: fix the bug".to_string())]).unwrap();
        assert_eq!(r, "opus");
        assert_eq!(text(&s), "fix the bug");

        // full id with a nested suffix, no task
        let (r, s) =
            split_model_shorthand(&[ContentBlock::from("claude/opus[1m]:".to_string())]).unwrap();
        assert_eq!(r, "claude/opus[1m]");
        assert!(prompt_is_empty(&s));

        // survives a goose turn-context preamble
        let (r, s) = split_model_shorthand(&[ContentBlock::from(
            "<turn-context>\n<t>2026</t>\n</turn-context>\n\ngpt-5.5: ship it".to_string(),
        )])
        .unwrap();
        assert_eq!(r, "gpt-5.5");
        let out = text(&s);
        assert!(out.contains("<turn-context>"), "preamble kept: {out}");
        assert!(out.contains("ship it"), "task kept: {out}");
        assert!(!out.contains("gpt-5.5:"), "shorthand stripped: {out}");

        // no colon-token → not a shorthand
        assert!(
            split_model_shorthand(&[ContentBlock::from("just do the thing".to_string())]).is_none()
        );

        // goose layout: preamble in one block, the user's message in the NEXT
        // block. The shorthand must be found in the second block.
        let (r, s) = split_model_shorthand(&[
            ContentBlock::from("<turn-context>\n<t>2026</t>\n</turn-context>".to_string()),
            ContentBlock::from("gpt: what now?".to_string()),
        ])
        .unwrap();
        assert_eq!(r, "gpt");
        let out = text(&s);
        assert!(out.contains("<turn-context>"), "preamble kept: {out}");
        assert!(
            out.contains("what now?") && !out.contains("gpt:"),
            "stripped: {out}"
        );
    }

    #[test]
    fn skill_pattern_matches_slash_and_token_not_substring() {
        // slash-command form
        assert!(prompt_mentions_skill("please run /ship-pr now", "ship-pr"));
        // standalone token
        assert!(prompt_mentions_skill("invoke the ship-pr skill", "ship-pr"));
        // caller lowercases the prompt; pattern casing does not matter
        assert!(prompt_mentions_skill("run ship-pr", "SHIP-PR"));
        // not a loose substring
        assert!(!prompt_mentions_skill(
            "this is a membership-provider thing",
            "ship-pr"
        ));
        assert!(!prompt_mentions_skill("unrelated prompt", "ship-pr"));
    }

    #[test]
    fn candidate_matches_exact_glob_and_agent() {
        let id = CandidateId::parse("claude/opus[1m]").unwrap();
        assert!(candidate_matches("claude/opus[1m]", &id)); // exact
        assert!(candidate_matches("*opus*", &id)); // glob / model class
        assert!(candidate_matches("claude", &id)); // bare agent
        assert!(!candidate_matches("*gpt-5.5*", &id));
        assert!(!candidate_matches("codex", &id));
    }

    #[test]
    fn detect_skill_route_finds_configured_pattern() {
        let cfg = Config::from_yaml(
            "router: auto\n\
             skill_routing:\n\
             \x20 - pattern: ship-pr\n\
             \x20   candidates: [\"*opus*\", \"*gpt-5.5*\"]\n\
             agents:\n\
             \x20 - name: claude\n\
             \x20   command: { type: stdio, command: /bin/true }\n\
             \x20   model_selection: { type: config-option }\n\
             \x20   models:\n\
             \x20     - { id: opus, display_name: Opus, cost_rank: 4 }\n",
        )
        .expect("valid config");
        let hit = vec![ContentBlock::from("let's run ship-pr on this".to_string())];
        let miss = vec![ContentBlock::from("just refactor this".to_string())];
        assert!(detect_skill_route(&cfg, &hit).is_some());
        assert!(detect_skill_route(&cfg, &miss).is_none());

        // A skill NAMED inside backticks (a UI/example mention) must NOT count
        // as invoking it — this is the hickory-ai6 false positive.
        let mention = vec![ContentBlock::from(
            "Add an autocomplete: typing `/` should suggest skills like `/ship-pr`.".to_string(),
        )];
        assert!(
            detect_skill_route(&cfg, &mention).is_none(),
            "a backticked skill mention must not trigger skill routing"
        );
    }

    #[test]
    fn strip_code_spans_removes_inline_and_fenced() {
        let s = strip_code_spans("a `code` b");
        assert!(!s.contains("code") && s.contains('a') && s.contains('b'));
        assert!(!strip_code_spans("see `/ship-pr` here").contains("ship-pr"));
        assert!(
            !strip_code_spans("```\n/ship-pr\n```\ndone").contains("ship-pr"),
            "fenced blocks are stripped too"
        );
        // Text outside code is preserved.
        assert!(strip_code_spans("run ship-pr now").contains("ship-pr"));
    }
}
