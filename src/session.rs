//! Router session state, pin map, id remapping, callback forwarding, and the
//! upstream ACP agent surface.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::{Value, json};

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

use crate::candidate::{
    CandidateId, EffortLevel, EffortResolution, RequiredCaps, ScoreTable, TaskClass,
};
use crate::classifier::{
    ClassifierRules, ClassifyInput, automatic_effort, classify, cwd_language_fingerprint,
};
use crate::config::{
    Config, DisclosureMode, EscalationPath, RouteSelection, SkillRoute, StrategyKind,
};
use crate::downstream::{
    ProcessKey, ProcessTargetSpec, SelectionKind, build_targets, find_model_option,
    is_auth_required, probe_target, select_values, selector_value_to_send, start_downstream,
    verify_model_selected,
};
use crate::headroom::HeadroomTracker;
use crate::relay;
use crate::state::{PersistedSession, StateFile};
use crate::strategies::{
    CandidateView, OverrideSource, RankedCandidate, RouteContext, make_strategy,
};

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
    /// Mirrors `models[].auto_eligible`: false keeps the candidate out of
    /// every automatic selection pool while leaving it explicitly selectable.
    pub auto_eligible: bool,
}

/// Runtime state for one downstream process target.
pub struct TargetRuntime {
    pub spec: ProcessTargetSpec,
    pub conn: Option<ConnectionTo<AgentPeer>>,
    pub init: Option<InitializeResponse>,
    pub model_config_id: Option<SessionConfigId>,
    pub auth_pending: bool,
    pub credential_generation: Option<String>,
    pub dead: Option<String>,
    /// Last respawn attempt for a dead target (cooldown bookkeeping).
    pub last_respawn: Option<std::time::Instant>,
    /// Held only across a cold start + probe, so two sessions opening the
    /// same missing process start it once.
    pub start_gate: Arc<tokio::sync::Mutex<()>>,
    pub stop: tokio_util::sync::CancellationToken,
    pub stopped: Arc<tokio::sync::Notify>,
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
    /// A tool-free pre-classification session. Any attempted tool use or
    /// downstream callback is a safety violation, never a parent callback.
    PreClass {
        capture: Arc<Mutex<String>>,
        violation: Arc<AtomicBool>,
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

/// A delegate sub-session kept open across multiple turns so the parent
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
    /// The start event sent to `delegation.lifecycle_hook`, if one is
    /// configured; its stop event is sent when the delegate is closed.
    pub lifecycle: Option<crate::delegate_hook::DelegateEvent>,
    /// Router worker id (`b-…`/`w-…`), also the `worker_handoffs` key.
    pub worker_id: String,
    /// Turns completed so far, for `delegate_turn_end` events.
    pub turns: u32,
}

impl LiveDelegate {
    /// Release per-delegate state and report the stop to the lifecycle hook.
    pub fn finish(&self, shared: &Arc<Shared>, outcome: &str) {
        shared.delegate_effort.lock().unwrap().remove(&self.sub_sid);
        shared
            .worker_handoffs
            .lock()
            .unwrap()
            .remove(&self.worker_id);
        crate::delegate_mcp::drop_worker_tokens(shared, &self.worker_id);
        if let Some(event) = &self.lifecycle {
            crate::delegate_hook::deliver(shared, &event.stopped(outcome, None));
        }
    }
}

/// A `delegate_task background: true` job. The subtask runs on its own tokio
/// task; the parent collects the outcome later via `delegate_await`.
/// `result` stays `None` while running and is consumed (entry removed) when an
/// await returns it.
pub struct BackgroundDelegate {
    pub parent_sid: String,
    /// First line of the task, for status lines while the job runs.
    pub summary: String,
    pub started: std::time::Instant,
    pub result: Option<Result<String, String>>,
}

pub struct RouterSession {
    pub cwd: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub mcp_servers: Vec<McpServer>,
    /// Host-registered MCP bundles available only to explicitly opted-in
    /// delegate sessions. They never reach the primary downstream session.
    pub delegate_mcp_catalogs: HashMap<String, Vec<McpServer>>,
    /// Opaque capability requirements returned by the first-prompt
    /// pre-classifier. Used only to resolve host-configured MCP catalogs.
    pub required_mcp_capabilities: Vec<String>,
    pub strategy: StrategyKind,
    pub candidate_override: Option<CandidateId>,
    /// Who set `candidate_override` (user or a skill route). Not persisted:
    /// the pin happens on the turn that sets it.
    pub candidate_override_source: Option<OverrideSource>,
    /// Explicit user effort (`auto` clears it); it takes precedence over a
    /// classifier recommendation and is retained across failover.
    pub effort_request: Option<EffortLevel>,
    /// Candidate-specific result, recomputed whenever the pin changes.
    pub resolved_effort: Option<EffortResolution>,
    /// `[router: version=…]`: the provider version this session runs (see
    /// `Config::resolve_version`). Takes effect on the next request.
    pub version_request: Option<String>,
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
    /// Optional grouping label from `[router: label=...]` — shared with the
    /// session's delegates.
    pub run_label: Option<String>,
    /// Whether any downstream output was relayed to the client during the
    /// current prompt turn. Hot failover preserves it with tool statuses
    /// and asks the replacement to continue rather than restart the task.
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
    /// Classified complexity used to interpret benchmark quality against the
    /// capability demand for confidence and demotion.
    pub task_complexity: f64,
    /// Accumulated "struggle" signal (max-tokens/refusal stops, tool-call
    /// failures); subtracted from task-demand confidence.
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
    /// Complete SQLite conversation restored by the router, consumed by the
    /// first prompt sent to the fresh adapter. Never parsed as live commands.
    pub pending_history: Vec<ContentBlock>,
    /// A transcript write failure must not be reported as a successful turn.
    pub persistence_error: Option<String>,
    /// When set, agent text on the pinned session is captured here instead
    /// of relayed (used to collect a summary during a switch).
    pub capturing_summary: Option<Arc<Mutex<String>>>,
    /// One-shot host injects from the pre-classifier (e.g. ui_planning guidance),
    /// prepended to the next prompt.
    pub pending_injects: Vec<String>,
    /// Candidate whose freshly opened downstream session should receive the
    /// ordinary delegation directive on its first prompt. Set only when the
    /// router's delegate MCP server was actually attached to that session.
    pub pending_delegation_directive: Option<CandidateId>,
    /// Whether the current downstream session received the ordinary delegation
    /// directive. Used to detect native-subagent bypasses as telemetry.
    pub delegation_directive_active: bool,
    /// Pre-classifier has already run once for this session (v1: first eligible
    /// turn only).
    pub preclass_done: bool,
    /// Successful LLM pre-classification supersedes the static classifier for
    /// the initial model-selection profile.
    pub preclass_profile: Option<crate::classifier::TaskProfile>,
    /// Whether the native-subagent-usage warning has fired this turn (a session
    /// that received the delegation directive using the adapter's built-in
    /// `Task` tool instead of `delegate_task`). Reset each turn so it warns at
    /// most once per turn.
    pub turn_native_subagent_warned: bool,
    /// Ticket ids already injected into this session's context (a re-mention
    /// doesn't re-inject the same ticket).
    pub injected_tickets: HashSet<String>,
    /// Chars of framed ticket content `enrich_prompt` injected on the
    /// CURRENT turn (`None` when nothing was injected). Reset at the top of
    /// every `enrich_prompt` call so a prior turn's value never carries
    /// forward; read (not taken) when building `details` so it survives a
    /// same-turn failover retry rebuilding that JSON more than once.
    pub pending_ticket_enrichment_chars: Option<usize>,
    /// Why the current pin is *elevated* above what plain routing would pick
    /// ("escalation", "auto-upgrade", "skill `ship-pr`"), or `None` for an
    /// un-elevated pin. Explicit user picks never set this. Demotion
    /// (`demotion.after_quiet_turns`) only ever expires elevated pins.
    pub elevation: Option<String>,
    /// The `skill_routing` pattern behind the current elevation, when a skill
    /// route is what elevated the pin. Demotion consults it so an expiring
    /// verdict lands inside the skill's own pool instead of anywhere cheaper —
    /// `elevation` alone is prose, and parsing the pattern back out of it is
    /// not a contract. Cleared whenever `elevation` is set by anything else.
    pub elevation_skill: Option<String>,
    /// Consecutive turns without struggle signals since the last elevation —
    /// the demotion clock (reset by any struggle).
    pub quiet_turns: u32,
    /// True once the downstream adapter reported real cost via
    /// `usage_update.cost` — turn-end pricing synthesis then stays out of
    /// the way (synthesized and reported figures must not mix).
    pub saw_adapter_cost: bool,
    // ---- planner two-phase routing ----
    /// Current planner phase when `router: planner` is active. `None` for
    /// non-planner sessions. Only meaningful when `strategy == Planner`.
    /// Monotonic: once `Implementation`, never reverted.
    pub planner_phase: Option<crate::config::PlannerPhase>,
    /// Per-prompt `hard:` / `easy:` planner-pool override. Reset at the
    /// start of every prompt; set only when that prompt carries the prefix.
    pub planner_difficulty: Option<crate::config::PlannerDifficulty>,
    /// Host-declared coordinator role (`_meta.router_acp.session_role:
    /// "coordinator"` on `session/new` or any `session/prompt`). Sticky: a
    /// later prompt without the tag does not clear it. A coordinator stays in
    /// the Planning phase and on `planning_candidates`; only an explicit user
    /// pick may move it elsewhere (`Shared::coordinator_blocks`).
    pub coordinator: bool,
    /// The current pin came from an explicit human pick (`router.candidate`,
    /// `[router: candidate=…]` / `switch=…`, `model:` shorthand). Persisted as
    /// `routing.user_pick` so it survives session/load. A coordinator on a
    /// human-picked model is left there; any other off-pool pin is switched
    /// back to `planning_candidates`.
    pub pin_user_pick: bool,
}

/// What the outgoing model is asked to write when handing a session off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HandoffStyle {
    /// Full summary: task, decisions, findings, files changed, commands run,
    /// what remains. The right default when the incoming model must continue
    /// mid-thought and cannot re-derive that state.
    #[default]
    Full,
    /// Terse briefing: the task, the single identifier it operates on, and any
    /// decision NOT re-derivable from the repository. Chosen by a
    /// `terse_handoff` skill route (see `config::SkillRoute`).
    Terse,
}

/// A requested mid-session model switch, why, and how to brief the target.
#[derive(Debug, Clone)]
pub struct SwitchRequest {
    pub target: CandidateId,
    pub reason: String,
    /// How much context to carry across. Every switch except a `terse_handoff`
    /// skill route wants `Full`.
    pub handoff: HandoffStyle,
    /// A human asked for this target (`[router: switch=…]`, `model:`
    /// shorthand). Only these may move a coordinator off the planning pool.
    pub user_pick: bool,
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
            delegate_mcp_catalogs: seeded_mcp_catalogs(),
            required_mcp_capabilities: Vec::new(),
            strategy: cfg.router,
            candidate_override: None,
            candidate_override_source: None,
            effort_request: None,
            resolved_effort: None,
            version_request: None,
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
            task_complexity: 0.0,
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
            pending_history: Vec::new(),
            persistence_error: None,
            capturing_summary: None,
            pending_injects: Vec::new(),
            pending_delegation_directive: None,
            delegation_directive_active: false,
            preclass_done: false,
            preclass_profile: None,
            turn_native_subagent_warned: false,
            injected_tickets: HashSet::new(),
            pending_ticket_enrichment_chars: None,
            elevation: None,
            elevation_skill: None,
            quiet_turns: 0,
            saw_adapter_cost: false,
            planner_phase: None,
            planner_difficulty: None,
            coordinator: false,
            pin_user_pick: persisted
                .routing
                .as_ref()
                .and_then(|r| r.get("user_pick"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        }
    }

    fn new(cfg: &Config, req: &NewSessionRequest) -> Self {
        Self {
            cwd: req.cwd.clone(),
            additional_directories: req.additional_directories.clone(),
            mcp_servers: req.mcp_servers.clone(),
            delegate_mcp_catalogs: seeded_mcp_catalogs(),
            required_mcp_capabilities: Vec::new(),
            strategy: cfg.router,
            candidate_override: None,
            candidate_override_source: None,
            effort_request: None,
            resolved_effort: None,
            version_request: None,
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
            task_complexity: 0.0,
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
            pending_history: Vec::new(),
            persistence_error: None,
            capturing_summary: None,
            pending_injects: Vec::new(),
            pending_delegation_directive: None,
            delegation_directive_active: false,
            preclass_done: false,
            preclass_profile: None,
            turn_native_subagent_warned: false,
            injected_tickets: HashSet::new(),
            pending_ticket_enrichment_chars: None,
            elevation: None,
            elevation_skill: None,
            quiet_turns: 0,
            saw_adapter_cost: false,
            planner_phase: None,
            planner_difficulty: None,
            coordinator: meta_marks_coordinator(req.meta.as_ref()),
            pin_user_pick: false,
        }
    }
}

/// True when request `_meta.router_acp.session_role` is `"coordinator"`.
pub fn meta_marks_coordinator(meta: Option<&agent_client_protocol::schema::v1::Meta>) -> bool {
    meta.and_then(|m| m.get("router_acp"))
        .and_then(|r| r.get("session_role"))
        .and_then(|v| v.as_str())
        == Some("coordinator")
}

/// Router-wide shared state. All mutexes are short-lived and never held
/// across await points.
pub struct Shared {
    pub cfg: Config,
    pub account_config: Mutex<Config>,
    pub account_menus: Mutex<HashMap<String, crate::accounts::Menu>>,
    pub account_login: Mutex<HashSet<String>>,
    pub account_flows: Mutex<HashMap<String, crate::accounts::LoginFlow>>,
    pub account_cancellations: Mutex<HashMap<String, tokio_util::sync::CancellationToken>>,
    pub account_write: tokio::sync::Mutex<()>,
    pub scores: ScoreTable,
    pub rules: ClassifierRules,
    pub state: Mutex<StateFile>,
    pub llm_proxy: Arc<crate::llm_proxy::LlmProxyRuntime>,
    pub headroom: Mutex<HeadroomTracker>,
    pub auth: Mutex<crate::auth::AuthTracker>,
    pub sessions: Mutex<HashMap<String, RouterSession>>,
    pub targets: Mutex<HashMap<ProcessKey, TargetRuntime>>,
    pub candidates: Mutex<Vec<CandidateRuntime>>,
    sid_map: Mutex<HashMap<(ProcessKey, String), DownstreamRoute>>,
    pub delegate_tokens: Mutex<HashMap<String, crate::delegate_mcp::DelegateBinding>>,
    /// Delegate sub-sessions kept alive for follow-up turns (`keep_open`),
    /// keyed by the short `delegate_id` returned to the parent.
    pub live_delegates: Mutex<HashMap<String, LiveDelegate>>,
    /// Background delegate jobs keyed by the short `b-…` id returned to the
    /// parent; results are collected (and consumed) via `delegate_await`.
    pub background_delegates: Mutex<HashMap<String, BackgroundDelegate>>,
    /// Signaled whenever any background delegate finishes, waking waiters in
    /// `delegate_await`.
    pub background_notify: tokio::sync::Notify,
    /// Per-delegate effort from `delegate_task` `hints.effort`, keyed by the
    /// delegate's state-session id. The LLM proxy reads it in place of the
    /// parent session's effort while that delegate runs.
    pub delegate_effort: Mutex<HashMap<String, crate::candidate::EffortLevel>>,
    /// Per-agent delegate slots for agents with `max_delegates`, beside the
    /// global `delegate_semaphore`.
    pub agent_delegate_slots: HashMap<String, Arc<tokio::sync::Semaphore>>,
    /// Each worker's latest `worker_handoff`, keyed by worker id; reported
    /// with its next `delegate_turn_end` and cleared when the next turn starts.
    pub worker_handoffs: Mutex<HashMap<String, crate::delegate_hook::Handoff>>,
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
        }
        .with_pinned_versions(&cfg);
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
        let llm_proxy = crate::llm_proxy::LlmProxyRuntime::new(&cfg, &specs)
            .map_err(|e| AcpError::invalid_params().data(e))?;
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
                    auto_eligible: model.auto_eligible,
                });
                config_index += 1;
            }
        }
        for spec in specs {
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

        let max_concurrent = cfg.delegation.max_concurrent;
        let agent_delegate_slots = cfg
            .agents
            .iter()
            .filter_map(|a| {
                a.max_delegates
                    .map(|n| (a.name.clone(), Arc::new(tokio::sync::Semaphore::new(n))))
            })
            .collect();
        Ok(Arc::new(Self {
            account_config: Mutex::new(cfg.clone()),
            account_menus: Mutex::default(),
            account_login: Mutex::default(),
            account_flows: Mutex::default(),
            account_cancellations: Mutex::default(),
            account_write: Default::default(),
            cfg,
            scores,
            rules,
            state: Mutex::new(state),
            llm_proxy,
            headroom: Mutex::new(headroom),
            auth: Mutex::new(crate::auth::AuthTracker::default()),
            sessions: Mutex::new(HashMap::new()),
            targets: Mutex::new(targets),
            candidates: Mutex::new(candidates),
            sid_map: Mutex::new(HashMap::new()),
            delegate_tokens: Mutex::new(HashMap::new()),
            live_delegates: Mutex::new(HashMap::new()),
            background_delegates: Mutex::new(HashMap::new()),
            background_notify: tokio::sync::Notify::new(),
            delegate_effort: Mutex::new(HashMap::new()),
            agent_delegate_slots,
            worker_handoffs: Mutex::new(HashMap::new()),
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

    pub fn agent_configs(&self) -> Vec<crate::config::AgentConfig> {
        self.account_config.lock().unwrap().agents.clone()
    }

    pub fn runtime_config(&self) -> Config {
        self.account_config.lock().unwrap().clone()
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
            // A new process has not answered initialize yet.
            t.init = None;
            t.model_config_id = None;
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
            // `init` stays until a new process connects (`set_target_conn`):
            // an explicit pick of a dead target still needs its capabilities.
            t.dead = Some(reason.to_string());
        }
        // A respawn creates a fresh process with no memory of these sessions.
        // Keep primary pins for transcript handoff, but invalidate their routes
        // so an old session id can never be sent to the replacement process.
        self.sid_map
            .lock()
            .unwrap()
            .retain(|(target, _), _| target != key);
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

    /// `include_down` also admits candidates whose process died (`Down`) —
    /// only for an explicit pick, which revives the process before use.
    fn routeable_candidates_inner(&self, include_down: bool) -> Vec<CandidateRuntime> {
        self.candidates
            .lock()
            .unwrap()
            .iter()
            .filter(|c| {
                c.status == CandidateStatus::Routeable
                    || (include_down && matches!(c.status, CandidateStatus::Down(_)))
            })
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

    pub fn candidate_status(&self, id: &CandidateId) -> Option<CandidateStatus> {
        self.candidate_runtime(id).map(|c| c.status)
    }

    /// Managed credentials use shared, generation-bound evidence on every
    /// decision. A process-local probe or stale success cannot lift rejection.
    pub fn auth_rejection(&self, name: &str) -> Option<String> {
        let agent = self.agent_configs().into_iter().find(|a| a.name == name)?;
        let state = if crate::accounts::provider(&agent).is_some() {
            crate::credentials::availability(&agent)
        } else {
            self.auth.lock().unwrap().availability(name)
        };
        match state {
            crate::auth::AuthAvailability::Unauthenticated { reason } => Some(reason),
            _ => None,
        }
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
        self.route_for(&pin.process_key, &pin.downstream_sid)?;
        let conn = self.target_conn(&pin.process_key)?;
        Some((conn, pin.downstream_sid, pin.candidate))
    }

    /// True when `router_sid` is a coordinator and `target` is outside the
    /// planner's `planning_candidates`. Callers skip this check only for an
    /// explicit user pick.
    pub fn coordinator_blocks(&self, router_sid: &str, target: &CandidateId) -> bool {
        self.with_session(router_sid, |s| s.coordinator)
            .unwrap_or(false)
            && !self.in_planning_pool(target)
    }

    /// `target` (or the alias it was version-pinned from) matches a
    /// `routers.planner.planning_candidates` glob.
    pub fn in_planning_pool(&self, target: &CandidateId) -> bool {
        let patterns = &self.cfg.routers.planner.planning_candidates;
        let key = target.to_string();
        patterns
            .iter()
            .any(|p| crate::candidate::glob_match(p, &key))
    }

    pub fn with_session<R>(
        &self,
        router_sid: &str,
        f: impl FnOnce(&mut RouterSession) -> R,
    ) -> Option<R> {
        self.sessions.lock().unwrap().get_mut(router_sid).map(f)
    }

    /// The version `candidate` runs in this session; `None` = its `api_model`.
    pub fn version_for(
        &self,
        router_sid: &str,
        candidate: &CandidateId,
    ) -> Option<crate::config::ModelVersion> {
        let requested = self
            .with_session(router_sid, |s| s.version_request.clone())
            .flatten();
        self.runtime_config()
            .resolve_version(candidate, requested.as_deref())
            .cloned()
    }

    /// Scores of the version `candidate` runs in this session.
    pub fn scores_for(
        &self,
        router_sid: &str,
        candidate: &CandidateId,
    ) -> crate::candidate::ResolvedScores {
        match self.version_for(router_sid, candidate) {
            Some(v) => self
                .scores
                .lookup_exact(&CandidateId::new(&candidate.agent, &v.api_model)),
            None => self.scores.lookup_exact(candidate),
        }
    }

    /// Pricing of the version `candidate` runs in this session.
    pub fn pricing_for(
        &self,
        router_sid: &str,
        candidate: &CandidateId,
    ) -> Option<crate::config::PricingConfig> {
        match self.version_for(router_sid, candidate) {
            Some(v) => v.pricing,
            None => self
                .runtime_config()
                .model_config(candidate)?
                .pricing
                .clone(),
        }
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
    ///
    /// This is the AUTOMATIC pool: `auto_eligible: false` candidates are
    /// absent. Every automatic mechanism reads candidates through here, so
    /// excluding them once — rather than at each of a dozen call sites — is
    /// what makes "never picked by accident" hold for mechanisms added later.
    pub fn eligible_views(&self, required: &RequiredCaps, class: TaskClass) -> Vec<CandidateView> {
        self.eligible_views_inner(required, class, None, false, &[])
    }

    /// `eligible_views` for resolving an EXPLICIT user pick: also keeps
    /// candidates whose downstream process died (the switch revives it) and
    /// ignores outage quarantine (a penalty on automatic routing, not a veto
    /// on a human's choice). Without this a crash made the named model
    /// unresolvable, which silently demoted `opus:` to prose and let a cordon
    /// escape pick some other model.
    pub fn eligible_views_revivable(
        &self,
        required: &RequiredCaps,
        class: TaskClass,
    ) -> Vec<CandidateView> {
        self.eligible_views_inner(required, class, None, true, &[])
    }

    /// `candidate_view` with the same dead-process allowance as
    /// `eligible_views_revivable`.
    pub fn candidate_view_revivable(
        &self,
        id: &CandidateId,
        required: &RequiredCaps,
        class: TaskClass,
    ) -> Option<CandidateView> {
        self.eligible_views_inner(required, class, Some(id), true, &[])
            .into_iter()
            .find(|v| &v.id == id)
    }

    /// `eligible_views` plus one named candidate that may be
    /// `auto_eligible: false` — the session's EXPLICIT pin. Every other
    /// eligibility gate (routeable, capabilities, quarantine, cordons, seat
    /// budget) still applies to it.
    pub fn eligible_views_admitting(
        &self,
        required: &RequiredCaps,
        class: TaskClass,
        admit: Option<&CandidateId>,
    ) -> Vec<CandidateView> {
        self.eligible_views_inner(required, class, admit, false, &[])
    }

    /// The strategy view for ONE candidate, ignoring auto-eligibility — the
    /// question "may this specific candidate serve this prompt right now?",
    /// which an explicit pin has to be able to answer yes to.
    pub fn candidate_view(
        &self,
        id: &CandidateId,
        required: &RequiredCaps,
        class: TaskClass,
    ) -> Option<CandidateView> {
        self.eligible_views_inner(required, class, Some(id), false, &[])
            .into_iter()
            .find(|v| &v.id == id)
    }

    fn eligible_views_inner(
        &self,
        required: &RequiredCaps,
        class: TaskClass,
        admit: Option<&CandidateId>,
        explicit_pick: bool,
        excluded: &[String],
    ) -> Vec<CandidateView> {
        self.eligible_views_filtered(required, class, admit, explicit_pick, excluded, |_| true)
    }

    fn eligible_views_filtered(
        &self,
        required: &RequiredCaps,
        class: TaskClass,
        admit: Option<&CandidateId>,
        explicit_pick: bool,
        excluded: &[String],
        matches: impl Fn(&CandidateView) -> bool,
    ) -> Vec<CandidateView> {
        let candidates = self.routeable_candidates_inner(explicit_pick);
        let cfg = self.runtime_config();
        let logging_in = self.account_login.lock().unwrap().clone();
        let mut headroom = self.headroom.lock().unwrap();
        let targets = self.targets.lock().unwrap();
        let mut views = Vec::new();
        for c in candidates {
            if is_excluded(&c.id, excluded)
                || logging_in.contains(&c.id.agent)
                || cfg
                    .agents
                    .iter()
                    .any(|a| a.name == c.id.agent && a.account_disabled)
            {
                continue;
            }
            // An explicit-only candidate stays out of the pool unless it IS
            // the explicitly named candidate.
            if !c.auto_eligible && admit != Some(&c.id) {
                continue;
            }
            let Some(target) = targets.get(&c.process_key) else {
                continue;
            };
            if target.conn.is_none() && !(explicit_pick && target.dead.is_some()) {
                continue;
            }
            if self.auth_rejection(&c.id.agent).is_some() {
                continue;
            }
            let caps_ok = target
                .init
                .as_ref()
                .map(|i| required.satisfied_by(&i.agent_capabilities.prompt_capabilities))
                .unwrap_or(false);
            if !caps_ok || (!explicit_pick && headroom.is_quarantined(&c.id)) {
                continue;
            }
            // Agents cordoned by a token/usage limit sit out until reset.
            if headroom.cordon_active(&c.id.agent).is_some() {
                continue;
            }
            // Candidates proactively cordoned by the provider's usage API (cap
            // exhausted, no overage headroom) sit out until their reset.
            if headroom.usage_cordon(&c.id).is_some() {
                continue;
            }
            // Seat availability reporting an exhausted plan with no overage
            // absorbing it means the same thing as a cordon: there is nothing
            // left to spend on this candidate. Excluding it here — not merely
            // scaling its preference to 0 below — is what stops a quality edge
            // from winning the route while the cordon set is still stale (the
            // poll fails open, and the first poll lands after startup).
            if headroom.seat_exhausted(&c.id) {
                continue;
            }
            let scores = cfg
                .resolve_version(&c.id, None)
                .map(|version| {
                    self.scores
                        .lookup_exact(&CandidateId::new(&c.id.agent, &version.api_model))
                })
                .unwrap_or_else(|| self.scores.lookup(&c.id));
            let static_preference = cfg
                .agents
                .iter()
                .find(|a| a.name == c.id.agent)
                .map(|a| a.preference)
                .unwrap_or(0.0);
            // Dynamic preference scaling: the configured bonus fades with the
            // seat's usable budget — free plan while any remains, else the
            // graded overage/credit pool once the plan is spent (so a seat
            // with thousands of overage dollars left doesn't flatten to the
            // same 0 as one about to hit its spend cap). Paid-overage
            // aversion is applied by the auto strategy against task
            // complexity, where it can raise the difficulty bar without
            // making a frontier candidate impossible.
            let availability = headroom.availability(&c.id);
            let seat_budget = availability.as_ref().map(|a| {
                a.seat_budget_with_overage_weight(
                    self.cfg.availability_preference.headroom_scale_dollars,
                    self.cfg.availability_preference.overage_budget_weight,
                )
                .clamp(0.0, 1.0)
            });
            let preference = match seat_budget {
                Some(budget) if self.cfg.availability_preference.enabled => {
                    static_preference * budget
                }
                _ => static_preference,
            };
            let on_overage = availability.as_ref().is_some_and(|a| a.on_overage);
            let local_headroom = headroom.headroom(&c.id.agent);
            let plan_headroom = availability
                .as_ref()
                .map(|a| a.plan_headroom.clamp(0.0, 1.0));
            let effective_headroom = seat_budget
                .filter(|_| self.cfg.availability_preference.enabled)
                .map(|budget| local_headroom.min(budget))
                .unwrap_or(local_headroom);
            views.push(CandidateView {
                headroom: effective_headroom,
                plan_headroom,
                quality: scores.quality(class),
                coding_tier: scores.coding_tier,
                cost_rank: c.cost_rank,
                config_index: c.config_index,
                preference,
                on_overage,
                id: c.id,
            });
        }
        views.retain(matches);
        crate::accounts::prioritize(&mut views, &cfg.agents, admit);
        if !self.cfg.availability_preference.enabled {
            for view in &mut views {
                view.plan_headroom = None;
                view.on_overage = false;
            }
        }
        // Unmetered seats (no plan signal) must not look "more free" than a
        // metered seat that still has included plan — see cap_unmetered_headroom.
        crate::strategies::cap_unmetered_headroom(&mut views);
        // Paying seats must rank meaningfully below a metered seat still on
        // included plan (not merely no better) — see cap_overage_headroom.
        crate::strategies::cap_overage_headroom(
            &mut views,
            self.cfg.availability_preference.overage_budget_weight,
        );
        views
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    // ------------------------------------------------------------------
    // Router-owned session config options
    // ------------------------------------------------------------------

    pub fn router_config_options(&self, router_sid: &str) -> Vec<SessionConfigOption> {
        let (strategy, override_, effort) = self
            .with_session(router_sid, |s| {
                (s.strategy, s.candidate_override.clone(), s.effort_request)
            })
            .unwrap_or((self.cfg.router, None, None));

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
        for agent in self.agent_configs().iter().filter(|a| !a.account_disabled) {
            let options: Vec<SessionConfigSelectOption> = self
                .candidates
                .lock()
                .unwrap()
                .iter()
                .filter(|c| {
                    c.id.agent == agent.name
                        && (c.status == CandidateStatus::Routeable
                            || self.auth_rejection(&c.id.agent).is_some())
                })
                .map(|c| {
                    let opt =
                        SessionConfigSelectOption::new(c.id.to_string(), c.display_name.clone());
                    let scores = self.scores_for(router_sid, &c.id);
                    let supported: Vec<_> = scores
                        .effort_levels
                        .iter()
                        .filter(|level| **level != EffortLevel::Auto)
                        .filter(|level| scores.effort_mapping.contains_key(level))
                        .map(|level| level.as_str())
                        .collect();
                    let mut router_meta = json!({
                        "capabilities": { "effort": { "supported": supported } },
                    });
                    // Pinned legacy versions stay listed — a picker is
                    // explicit selection, which is the only way to reach
                    // them — but are flagged so a client can mark them as
                    // "manual only". Omitted when true: absent = eligible.
                    if !c.auto_eligible {
                        router_meta["auto_eligible"] = json!(false);
                    }
                    // Versions a `[router: version=…]` directive can select.
                    if let Some(model) = self.runtime_config().model_config(&c.id)
                        && !model.versions.is_empty()
                    {
                        router_meta["api_model"] =
                            json!(self.runtime_config().wire_api_model_unpinned(&c.id));
                        router_meta["versions"] = json!(
                            model
                                .versions
                                .iter()
                                .map(|v| v.api_model.as_str())
                                .collect::<Vec<_>>()
                        );
                    }
                    if let Some(cordon) = usage_cordons.get(&c.id) {
                        router_meta["available"] = json!(false);
                        router_meta["unavailable_reason"] = json!(cordon.reason);
                        router_meta["resets_at"] = json!(cordon.resets_at_rfc3339);
                    } else if let Some(reason) = self.auth_rejection(&c.id.agent) {
                        router_meta["available"] = json!(false);
                        router_meta["unavailable_reason"] = json!(reason);
                    }
                    let mut meta = serde_json::Map::new();
                    meta.insert("router_acp".to_string(), router_meta);
                    opt.meta(meta)
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

        let effort_option = SessionConfigOption::select(
            "router.effort",
            "Reasoning effort",
            effort.unwrap_or(EffortLevel::Auto).as_str(),
            EffortLevel::ALL
                .into_iter()
                .map(|level| SessionConfigSelectOption::new(level.as_str(), level.as_str()))
                .collect::<Vec<_>>(),
        )
        .description(
            "Explicit effort overrides automatic task-based effort; unsupported models omit it"
                .to_string(),
        );

        vec![strategy_option, candidate_option, effort_option]
    }
}

/// Turn a `SessionId` into its string form.
pub fn sid_str(sid: &agent_client_protocol::schema::v1::SessionId) -> String {
    sid.0.to_string()
}

/// The effort a primary session runs at: an explicit request (the
/// `router.effort` option or `[router: effort=…]`) as given; otherwise
/// `effort.default` or the automatic recommendation, capped at
/// `effort.max_automatic`.
pub(crate) fn session_effort(
    cfg: &crate::config::Config,
    explicit: Option<EffortLevel>,
    automatic: Option<EffortLevel>,
) -> Option<EffortLevel> {
    if explicit.is_some() {
        return explicit;
    }
    let chosen = cfg.effort.default.or(automatic)?;
    Some(match cfg.effort.max_automatic {
        Some(cap) if chosen > cap => cap,
        _ => chosen,
    })
}

/// Re-resolve effort for an already pinned session after a user changes the
/// router-owned effort option. The next provider request reads this state.
fn refresh_pinned_effort(
    cfg: &crate::config::Config,
    scores: &ScoreTable,
    session: &mut RouterSession,
) {
    let requested = session_effort(
        cfg,
        session.effort_request,
        session
            .task_class
            .map(|class| automatic_effort(class, session.task_complexity)),
    );
    let version = session.version_request.as_deref();
    session.resolved_effort = session.pin.as_ref().and_then(|pin| {
        let key = match cfg.resolve_version(&pin.candidate, version) {
            Some(v) => CandidateId::new(&pin.candidate.agent, &v.api_model),
            None => pin.candidate.clone(),
        };
        requested.map(|level| scores.lookup_exact(&key).resolve_effort(level))
    });
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
pub(crate) fn prompt_display_text(prompt: &[ContentBlock]) -> String {
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

/// Build the temporary session title from the first user-authored text block.
/// Ticket enrichment prepends router-framed blocks, which remain part of the
/// downstream prompt but must not replace the user's opening words here.
fn placeholder_title(prompt: &[ContentBlock]) -> Option<String> {
    prompt.iter().find_map(|block| match block {
        ContentBlock::Text(text) if !crate::tickets::is_injected_ticket_block(&text.text) => {
            let one_line = text.text.split_whitespace().collect::<Vec<_>>().join(" ");
            let mut title: String = one_line.chars().take(80).collect();
            if one_line.chars().count() > 80 {
                title.push('…');
            }
            (!title.is_empty()).then_some(title)
        }
        _ => None,
    })
}

#[cfg(test)]
mod placeholder_title_tests {
    use super::placeholder_title;
    use crate::tickets::frame_ticket;
    use agent_client_protocol::schema::v1::ContentBlock;

    #[test]
    fn skips_only_router_injected_ticket_blocks() {
        let prompt = vec![
            ContentBlock::from(frame_ticket("HAI-1", "router context")),
            ContentBlock::from(" \n\t ".to_string()),
            ContentBlock::from("[Ticket HAI-2] user-authored text\n  continues".to_string()),
        ];

        assert_eq!(
            placeholder_title(&prompt).as_deref(),
            Some("[Ticket HAI-2] user-authored text continues")
        );
    }

    #[test]
    fn folds_whitespace_and_truncates_unicode_safely() {
        let prompt = vec![ContentBlock::from(format!("  {}\n", "é".repeat(81)))];

        assert_eq!(
            placeholder_title(&prompt),
            Some(format!("{}…", "é".repeat(80)))
        );
    }
}

/// Log downstream tool-use / callback session updates that arrive on a
/// primary session (best-effort observability of "tool usage").
fn log_downstream_event(shared: &Arc<Shared>, router_sid: &str, params: &serde_json::Value) {
    let Some(update) = params.get("update") else {
        return;
    };
    // Raw ACP updates preserve rich content, streaming output, plans and full
    // tool results, including output from a cancelled or interrupted turn.
    let saved = shared.state.lock().unwrap().log_checked(
        router_sid,
        &crate::state::LogEntry {
            kind: "session_update".into(),
            role: "agent".into(),
            detail: Some(update.clone()),
            ..Default::default()
        },
    );
    if let Err(err) = saved {
        shared.with_session(router_sid, |s| {
            s.persistence_error = Some(format!("cannot save provider conversation update: {err}"));
        });
    }
    let kind = update
        .get("sessionUpdate")
        .and_then(|k| k.as_str())
        .unwrap_or("");
    let entry = match kind {
        "tool_call" | "tool_call_update" => {
            let tool_call_id = update
                .get("toolCallId")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let title = update
                .get("title")
                .and_then(|t| t.as_str())
                .or_else(|| (!tool_call_id.is_empty()).then_some(tool_call_id))
                .unwrap_or("tool")
                .to_string();
            let status = update
                .get("status")
                .and_then(|s| s.as_str())
                .unwrap_or("running");
            let persisted = shared.state.lock().unwrap().get(router_sid);
            let model = shared
                .llm_proxy
                .last_attribution(router_sid)
                .map(|(candidate, _)| candidate)
                .or_else(|| {
                    persisted.map(|session| format!("{}/{}", session.agent, session.model))
                });
            shared.state.lock().unwrap().record_tool_call(
                router_sid,
                tool_call_id,
                &title,
                status,
                model.as_deref(),
                update,
            );
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
                model,
                ..Default::default()
            })
        }
        "usage_update" => {
            let used = update.get("used").and_then(|u| u.as_u64());
            // The adapter reports authoritative cumulative cost in USD — capture
            // it instead of relying on text-estimated tokens.
            let cost = update
                .get("cost")
                .and_then(|c| c.get("amount"))
                .and_then(|a| a.as_f64());
            if cost.is_some() {
                // Remember that this adapter reports real cost, so turn-end
                // pricing synthesis stays out of the way. (Sessions lock taken
                // before the state lock, never while holding it.)
                shared.with_session(router_sid, |s| s.saw_adapter_cost = true);
            }
            let st = shared.state.lock().unwrap();
            if let Some(used) = used {
                st.set_context_used(router_sid, used);
            }
            if let Some(cost) = cost {
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
            handoff: HandoffStyle::Full,
            user_pick: false,
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
            handoff: HandoffStyle::Full,
            user_pick: false,
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
    ranked_escalation_target(shared, router_sid, path)
        .filter(|t| !shared.coordinator_blocks(router_sid, t))
}

fn ranked_escalation_target(
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
    pool.retain(|v| v.id != current && !view_excluded(v, &excluded));
    // Same margin-then-fallback as `upgrade_target`: prefer a real (+0.05
    // normalized) capability step, but when only a compressed peer sits
    // above the current pin (Fable over Opus, Sol over Terra — a deliberate
    // ~0.007 gap), observed difficulty must still be able to cross it.
    let pick_above = |margin: f64| {
        let above: Vec<&CandidateView> = pool
            .iter()
            .filter(|v| {
                crate::candidate::quality_utility(v.quality)
                    > crate::candidate::quality_utility(current_q) + margin
            })
            .collect();
        match path {
            EscalationPath::Leap => above.into_iter().max_by(|a, b| {
                a.quality
                    .partial_cmp(&b.quality)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            EscalationPath::Ladder => above.into_iter().min_by(|a, b| {
                a.quality
                    .partial_cmp(&b.quality)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
        }
        .map(|v| v.id.clone())
    };
    pick_above(0.05).or_else(|| pick_above(f64::EPSILON))
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
            handoff: HandoffStyle::Full,
            user_pick: false,
        });
    });
    // Interrupt the in-flight cheap turn; the failover loop takes over.
    if let Some(conn) = shared.target_conn(key) {
        let _ = conn.send_notification(CancelNotification::new(down_sid.to_string()));
    }
    tracing::info!(session = router_sid, %target, "mid-turn escalation requested");
}

/// Cordon an agent when its xAI `_x.ai/settings/update` frame reports the
/// subscription access gate closed. This is grok's only limit signal (it has
/// no numeric usage meter), so it plays the role a `usage_source` poll plays
/// for claude/codex. Fail-open: an absent/`true` `allow_access` with no
/// `gate_message` never cordons, and a gate with no reset time falls back to
/// `cordon_default_secs`.
/// Pure gate decision: `Some(reason)` when the `_x.ai/settings/update` params
/// report the subscription access gate closed, else `None`. An explicitly
/// `false` `allow_access` OR a non-empty `gate_message` closes it; anything
/// else (true/absent flag, null/empty message) is routable.
fn xai_gate_reason(params: &serde_json::Value) -> Option<String> {
    let allow = params.get("allow_access").and_then(|v| v.as_bool());
    let gate_msg = params
        .get("gate_message")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if allow != Some(false) && gate_msg.is_none() {
        return None;
    }
    Some(match gate_msg {
        Some(m) => {
            let mut r = format!("xAI subscription gate: {m}");
            r.truncate(160);
            r
        }
        None => "xAI subscription access gate closed (usage limit reached; no reset time reported)"
            .to_string(),
    })
}

fn client_has_form_elicitation(shared: &Shared) -> bool {
    shared
        .upstream_client_capabilities()
        .elicitation
        .as_ref()
        .is_some_and(|caps| caps.form.is_some())
}

/// Translate Grok's `_x.ai/ask_user_question` into `elicitation/create` and
/// map the client's form result back into Grok's `{outcome, answers}` shape.
fn forward_xai_ask(
    shared: &Arc<Shared>,
    upstream: &ConnectionTo<ClientPeer>,
    parsed: crate::xai_questions::XaiAskRequest,
    responder: Responder<serde_json::Value>,
    router_sid: &str,
) -> Result<(), AcpError> {
    let request = crate::xai_questions::to_elicitation(&parsed, router_sid);
    shared.state.lock().unwrap().log(
        router_sid,
        &crate::state::LogEntry {
            kind: "xai_ask_user_question".to_string(),
            role: "tool".to_string(),
            summary: "_x.ai/ask_user_question".to_string(),
            tokens_estimated: true,
            ..Default::default()
        },
    );
    tracing::info!(
        session = router_sid,
        questions = parsed.questions.len(),
        "translating _x.ai/ask_user_question to elicitation/create"
    );
    let questions = parsed.questions;
    upstream
        .send_request(request)
        .on_receiving_result(move |result| async move {
            match result {
                Ok(resp) => {
                    let value = crate::xai_questions::from_elicitation(&resp, &questions);
                    let _ = responder.respond(value);
                }
                Err(err) => {
                    let _ = responder.respond_with_error(err);
                }
            }
            Ok(())
        })?;
    Ok(())
}

fn note_xai_gate(shared: &Arc<Shared>, key: &ProcessKey, params: &serde_json::Value) {
    if !shared.cfg.cordon.enabled {
        return;
    }
    let Some(reason) = xai_gate_reason(params) else {
        return;
    };
    let Some(agent) = shared.target_spec(key).map(|t| t.agent_name) else {
        return;
    };
    let dur = shared
        .headroom
        .lock()
        .unwrap()
        .cordon(&agent, None, reason.clone());
    tracing::warn!(
        agent = agent,
        cordon_secs = dur.as_secs(),
        reason = reason,
        "agent cordoned by xAI subscription access gate"
    );
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
    // xAI Grok surfaces its subscription access gate as a *session-less*
    // `_x.ai/settings/update` notification. Grok exposes no numeric usage meter
    // (no used-percent, no reset time — see the frame capture in
    // grok-adapter notes), so a pollable `usage_source` is impossible; this
    // binary gate is its only limit signal. When grok reports the gate closed
    // (`allow_access: false` or a populated `gate_message`), proactively cordon
    // the owning agent — the grok analog of the anthropic-oauth / codex-rollout
    // usage cordons. Handled here (before the sessionId lookup) because the
    // frame carries no sessionId and would otherwise be dropped unseen.
    if let Dispatch::Notification(msg) = &message
        && msg.method() == "_x.ai/settings/update"
    {
        note_xai_gate(shared, key, msg.params());
        return Ok(Handled::Yes);
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
                let msg = crate::accounts::merge_commands(msg)?;
                // Claude's narration of its user-facing prose arrives as
                // thinking; relay it as the message it is (see
                // `relay::narration_as_message`), before anything below reads
                // the frame's text.
                let msg = if msg.method() == "session/update"
                    && shared.llm_proxy.thinking_text_is_narration(&router_sid)
                {
                    relay::narration_as_message(&msg)?
                } else {
                    msg
                };
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
                                // Delegation bypass: a model told to use only the
                                // router's delegation tools used the adapter's
                                // built-in sub-agent tool instead. A host that
                                // allows native subagents is not bypassing.
                                let prompted = shared
                                    .with_session(&router_sid, |s| s.delegation_directive_active)
                                    .unwrap_or(false);
                                if is_native_subagent_tool(update)
                                    && prompted
                                    && shared.cfg.delegation.native_subagents
                                        == crate::config::NativeSubagentPolicy::Forbid
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
                                            "router-acp · delegation bypassed: the model used \
                                             its built-in sub-agent tool instead of the router's \
                                             `delegate_task`; that work is invisible to router \
                                             routing and cost telemetry",
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
                    // Attribute each tool call to the model that produced it, as
                    // structured metadata on the frame the client already renders
                    // as a card. This is where per-request routing belongs — it
                    // used to be announced as chat prose, which put a router
                    // block between every pair of messages for a whole session.
                    if relay::is_tool_call_update(&fwd)
                        && let Some((candidate, reason)) =
                            shared.llm_proxy.last_attribution(&router_sid)
                    {
                        fwd = relay::with_router_meta(
                            &fwd,
                            json!({ "candidate": candidate, "reason": reason }),
                        )?;
                    }
                    // Downstream output reached the client. A failover must
                    // carry partial work and tool statuses as a continuation.
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
                if method == "_x.ai/ask_user_question"
                    && client_has_form_elicitation(shared)
                    && let Some(parsed) = crate::xai_questions::parse_request(msg.params())
                {
                    forward_xai_ask(shared, &upstream, parsed, responder, &router_sid)?;
                    return Ok(Handled::Yes);
                }
                let fwd =
                    relay::normalize_terminal_create(&relay::with_session_id(&msg, &router_sid)?)?;
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
                            summary: method.clone(),
                            detail: Some(msg.params().clone()),
                            tokens_estimated: true,
                            ..Default::default()
                        },
                    );
                }
                if method == "_x.ai/ask_user_question"
                    && client_has_form_elicitation(shared)
                    && let Some(parsed) = crate::xai_questions::parse_request(msg.params())
                {
                    forward_xai_ask(shared, &upstream, parsed, responder, &parent_router_sid)?;
                    return Ok(Handled::Yes);
                }
                let fwd = relay::normalize_terminal_create(&relay::with_session_id(
                    &msg,
                    &parent_router_sid,
                )?)?;
                upstream.send_request(fwd).forward_response_to(responder)?;
                Ok(Handled::Yes)
            }
            Dispatch::Response(..) => unreachable!("responses handled above"),
        },
        DownstreamRoute::PreClass { capture, violation } => match message {
            Dispatch::Notification(msg) => {
                if msg.method() == "session/update" {
                    if let Some(text) = agent_chunk_text(msg.params()) {
                        capture.lock().unwrap().push_str(&text);
                    }
                    let is_tool = msg
                        .params()
                        .get("update")
                        .and_then(|u| u.get("sessionUpdate"))
                        .and_then(|k| k.as_str())
                        .is_some_and(|k| k == "tool_call" || k == "tool_call_update");
                    if is_tool {
                        violation.store(true, Ordering::Release);
                        if let Some(conn) = shared.target_conn(key) {
                            let _ = conn.send_notification(CancelNotification::new(down_sid));
                        }
                    }
                }
                Ok(Handled::Yes)
            }
            Dispatch::Request(msg, responder) => {
                violation.store(true, Ordering::Release);
                if let Some(conn) = shared.target_conn(key) {
                    let _ = conn.send_notification(CancelNotification::new(down_sid));
                }
                responder.respond_with_error(AcpError::invalid_request().data(format!(
                    "pre-class evaluator callbacks are denied: {}",
                    msg.method()
                )))?;
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
        .agent_configs()
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
    let observed = crate::auth::request_access_generation(shared, candidate);
    let result = open_downstream_session_once(
        shared,
        candidate,
        cwd.clone(),
        additional_directories.clone(),
        mcp_servers.clone(),
        route.clone(),
    )
    .await;
    if result.as_ref().is_err_and(is_auth_required) {
        let observed =
            observed.or_else(|| crate::auth::request_access_generation(shared, candidate));
        let outcome = crate::auth::note_auth_failure_for_request(
            shared,
            &candidate.agent,
            "Authentication unavailable",
            observed.as_deref(),
        )
        .await;
        if outcome == crate::credentials::RepairOutcome::Repaired
            && let Some(runtime) = shared.candidate_runtime(candidate)
            && crate::downstream::restart_after_repair(shared, &runtime.process_key)
                .await
                .is_ok()
        {
            return open_downstream_session_once(
                shared,
                candidate,
                cwd,
                additional_directories,
                mcp_servers,
                route,
            )
            .await;
        }
    }
    result
}

async fn open_downstream_session_once(
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
    ensure_target_ready(shared, candidate, &key).await?;
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
        // The selector speaks the candidate's `id`, or a `[1m]` synonym the
        // adapter actually listed; the real wire model is then held by
        // `api_model` (or a pinned version) through the per-request proxy.
        let selector_values = find_model_option(resp.config_options.as_deref().unwrap_or(&[]))
            .map(|option| select_values(&option))
            .unwrap_or_default();
        let downstream_model = selector_value_to_send(&selector_values, &candidate.model);
        let set_req = SetSessionConfigOptionRequest::new(
            resp.session_id.clone(),
            config_id.clone(),
            SessionConfigOptionValue::value_id(downstream_model.clone()),
        );
        match conn.send_request(set_req).block_task().await {
            Ok(set_resp) => {
                if let Err(msg) =
                    verify_model_selected(&set_resp.config_options, &config_id, &downstream_model)
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
        d.finish(shared, "closed");
    }
    // Drop the session's background jobs too: finished results nobody will
    // collect, and completion markers for still-running jobs (which notice the
    // deleted/cancelled parent themselves and tear their sub-session down).
    shared
        .background_delegates
        .lock()
        .unwrap()
        .retain(|_, j| j.parent_sid != router_sid);
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
/// Active tags match across text blocks outside quoted code and framed history.
/// Goose prepends a `<turn-context>…</turn-context>` preamble to prompts, so
/// requiring line 1 would miss a command after it.
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
    pub effort: Option<EffortLevel>,
    /// `[router: version=<api_model>|default]` — the provider version to run.
    pub version: Option<String>,
    /// `[router: phase=planning|implementation]` — planner phase override.
    pub phase: Option<crate::config::PlannerPhase>,
}

/// Parse (and strip) every active routing directive in the prompt. Several tags
/// (`[router: candidate=…] [router: effort=…]`) merge as if they were one
/// comma-separated tag: a later key overrides an earlier one, and `exclude`
/// lists combine. Returns `Ok(None)` when no directive is present; `Err`
/// describes an invalid directive (the prompt fails loudly so recipes get
/// fixed).
pub fn parse_prompt_directives(
    prompt: &[ContentBlock],
) -> Result<Option<(PromptDirectives, Vec<ContentBlock>)>, String> {
    let mut merged: Option<PromptDirectives> = None;
    let mut remaining = prompt.to_vec();
    while let Some((next, stripped)) = parse_one_prompt_directive(&remaining)? {
        remaining = stripped;
        let into = merged.get_or_insert_with(PromptDirectives::default);
        into.candidate = next.candidate.or(into.candidate.take());
        into.prefer = next.prefer.or(into.prefer.take());
        into.switch = next.switch.or(into.switch.take());
        into.strategy = next.strategy.or(into.strategy.take());
        into.exclude.extend(next.exclude);
        into.label = next.label.or(into.label.take());
        into.effort = next.effort.or(into.effort.take());
        into.version = next.version.or(into.version.take());
        into.phase = next.phase.or(into.phase.take());
    }
    Ok(merged.map(|directives| (directives, remaining)))
}

/// Locate commands without interpreting quoted code or framed history. Keep
/// byte offsets into the original text so those examples reach the model intact.
fn find_prompt_directive(prompt: &[ContentBlock]) -> Option<(usize, String, usize)> {
    const FRAMES: [(&str, &str); 3] = [
        (
            "<resumed-conversation-context>",
            "</resumed-conversation-context>",
        ),
        ("<continued-work-handoff>", "</continued-work-handoff>"),
        ("<turn-context>", "</turn-context>"),
    ];
    let mut frames: Vec<usize> = Vec::new();
    // Delimiter byte, opening run length, and whether this is a fenced block.
    let mut code: Option<(u8, usize, bool)> = None;
    for (block_idx, block) in prompt.iter().enumerate() {
        let ContentBlock::Text(text) = block else {
            continue;
        };
        let bytes = text.text.as_bytes();
        let mut pos = 0;
        let mut line_start = 0;
        while pos < bytes.len() {
            let rest = &bytes[pos..];
            if rest[0] == b'\n' {
                line_start = pos + 1;
                pos += 1;
                continue;
            }
            let line_prefix = pos - line_start <= 3
                && bytes[line_start..pos]
                    .iter()
                    .all(|b| matches!(b, b' ' | b'\t'));
            if let Some((delimiter, length, fenced)) = code {
                if rest[0] == delimiter {
                    let run = rest.iter().take_while(|&&b| b == delimiter).count();
                    let closes = if fenced {
                        run >= length
                            && line_prefix
                            && rest[run..]
                                .iter()
                                .take_while(|&&b| b != b'\n')
                                .all(u8::is_ascii_whitespace)
                    } else {
                        run == length
                    };
                    if closes {
                        code = None;
                    }
                    pos += run;
                } else {
                    pos += 1;
                }
                continue;
            }
            if rest[0] == b'\\' && rest.get(1).is_some_and(u8::is_ascii_punctuation) {
                pos += 2;
                continue;
            }
            if line_prefix && rest[0] == b'>' {
                pos += rest.iter().position(|&b| b == b'\n').unwrap_or(rest.len());
                continue;
            }
            if matches!(rest[0], b'`' | b'~') {
                let delimiter = rest[0];
                let run = rest.iter().take_while(|&&b| b == delimiter).count();
                if delimiter == b'`' || (line_prefix && run >= 3) {
                    code = Some((delimiter, run, line_prefix && run >= 3));
                }
                pos += run;
                continue;
            }
            if let Some(&frame) = frames.last()
                && rest.starts_with(FRAMES[frame].1.as_bytes())
            {
                frames.pop();
                pos += FRAMES[frame].1.len();
                continue;
            }
            if let Some(frame) = FRAMES
                .iter()
                .position(|(open, _)| rest.starts_with(open.as_bytes()))
            {
                frames.push(frame);
                pos += FRAMES[frame].0.len();
                continue;
            }
            if !frames.is_empty() {
                pos += 1;
                continue;
            }
            if rest
                .get(..8)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"[router:"))
            {
                return Some((block_idx, text.text.clone(), pos));
            }
            pos += 1;
        }
    }
    None
}

/// Parse (and strip) the first active routing directive in the prompt.
///
/// The directive is matched outside quoted code and history on any line of
/// ANY text block — goose both wraps
/// prompts in a `<turn-context>` preamble AND may split it into a separate
/// content block, so neither "line 1" nor "first block" is safe. Only the
/// directive line is removed; the surrounding text (preamble + task) is kept.
fn parse_one_prompt_directive(
    prompt: &[ContentBlock],
) -> Result<Option<(PromptDirectives, Vec<ContentBlock>)>, String> {
    // Locate the `[router:` directive within any text block, then
    // bracket-match to its closing `]`. Depth tracking means nested brackets
    // in model ids (`opus[1m]`) are handled, and the directive may sit
    // anywhere — on its own line, after a `<turn-context>` preamble, or inline
    // with task text before/after it on the same line.
    const OPEN: &str = "[router:";
    let Some((block_idx, text, start)) = find_prompt_directive(prompt) else {
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
            "effort" => {
                directives.effort = Some(EffortLevel::parse(value).ok_or_else(|| {
                    format!(
                        "directive effort `{value}` must be auto, low, medium, high, xhigh, or max"
                    )
                })?);
            }
            "version" => {
                if value.is_empty() {
                    return Err("directive version must name an api_model or `default`".into());
                }
                directives.version = Some(value.to_string());
            }
            "phase" => {
                directives.phase = Some(match value {
                    "planning" => crate::config::PlannerPhase::Planning,
                    "implementation" => crate::config::PlannerPhase::Implementation,
                    _ => {
                        return Err(format!(
                            "directive phase `{value}` must be planning or implementation"
                        ));
                    }
                });
            }
            other => {
                return Err(format!(
                    "unknown routing directive key `{other}` \
                     (keys: candidate, prefer, switch, strategy, exclude, label, effort, version, \
                     phase)"
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

/// Token usage for one completed turn.
#[derive(Debug, Clone, Copy, Default)]
pub struct TurnUsage {
    pub input: u64,
    pub output: u64,
    /// Cache-read / cache-write tokens (0 when the adapter doesn't report
    /// them). Cache reads dominate the real cost of long sessions.
    pub cache_read: u64,
    pub cache_write: u64,
    /// True when counts are estimated from text (protocol gave none).
    pub estimated: bool,
}

/// Derive token counts for a completed turn. Uses the downstream's reported
/// `usage` when present (the `unstable_end_turn_token_usage` ACP capability);
/// otherwise estimates the output from the collected text (input unknown →
/// 0, flagged estimated).
pub fn turn_tokens(resp: &PromptResponse, output_text: &str) -> TurnUsage {
    if let Some(usage) = &resp.usage {
        TurnUsage {
            input: usage.input_tokens,
            output: usage.output_tokens,
            cache_read: usage.cached_read_tokens.unwrap_or(0),
            cache_write: usage.cached_write_tokens.unwrap_or(0),
            estimated: false,
        }
    } else {
        TurnUsage {
            output: crate::state::estimate_tokens(output_text),
            estimated: true,
            ..Default::default()
        }
    }
}

/// Configured API-equivalent pricing for a candidate's model, if any.
fn model_pricing<'a>(
    cfg: &'a crate::config::Config,
    candidate: &CandidateId,
) -> Option<&'a crate::config::PricingConfig> {
    cfg.model_pricing(candidate)
}

/// Synthesize the USD cost of one turn from configured `pricing`. `None`
/// when no pricing is configured for the model, or when the counts are
/// text-estimates (garbage in, garbage out). Cache rates default to the
/// common provider discounts (reads 0.1×, writes 1.25× the input rate).
pub(crate) fn synth_turn_cost(
    cfg: &crate::config::Config,
    candidate: &CandidateId,
    usage: &TurnUsage,
) -> Option<f64> {
    if usage.estimated {
        return None;
    }
    let p = model_pricing(cfg, candidate)?;
    let cache_read = p.cache_read_per_mtok.unwrap_or(p.input_per_mtok * 0.1);
    let cache_write = p.cache_write_per_mtok.unwrap_or(p.input_per_mtok * 1.25);
    Some(
        (usage.input as f64 * p.input_per_mtok
            + usage.output as f64 * p.output_per_mtok
            + usage.cache_read as f64 * cache_read
            + usage.cache_write as f64 * cache_write)
            / 1_000_000.0,
    )
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
/// reported (or `headroom.cordon_default_secs` when it reported none); a spend
/// cap on the seat's money cordons only the candidate that hit it. Outages
/// count toward candidate quarantine; process death is already tracked by the
/// connection watchdog.
pub(crate) fn apply_failure(
    shared: &Arc<Shared>,
    candidate: &CandidateId,
    err: &AcpError,
    class: &crate::limits::FailureClass,
) -> String {
    use crate::limits::{FailureClass, humanize};
    // Auth indicators are inputs to credential repair, never outage or quota
    // evidence. Only the credential manager may exclude an account for auth.
    if is_auth_required(err) {
        return shared
            .auth_rejection(&candidate.agent)
            .unwrap_or_else(|| "authentication status unavailable".into());
    }
    match class {
        FailureClass::RateLimited {
            retry_after,
            spend_scoped,
        } => {
            let effective = retry_after.unwrap_or(std::time::Duration::from_secs(
                shared.cfg.headroom.cordon_default_secs,
            ));
            let kind = if *spend_scoped {
                "spend limit"
            } else {
                "token/usage limit"
            };
            let reason = if retry_after.is_some() {
                format!("{kind} (model reports reset in {})", humanize(effective))
            } else {
                format!(
                    "{kind} (no reset time reported; retrying in {})",
                    humanize(effective)
                )
            };
            if *spend_scoped {
                // The seat's money is capped, not this agent's plan window: the
                // only model that needed the money is the one whose own plan
                // budget was already spent. Cordoning the agent would take the
                // agent's still-funded models down with it, so scope this to
                // the candidate that actually hit the wall.
                shared.headroom.lock().unwrap().cordon_candidate(
                    candidate,
                    reason.clone(),
                    std::time::SystemTime::now() + effective,
                );
                tracing::warn!(
                    candidate = %candidate,
                    cordon_secs = effective.as_secs(),
                    "candidate cordoned by spend limit"
                );
            } else {
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
            }
            reason
        }
        FailureClass::Outage => {
            if crate::limits::is_provider_terms_gate_text(&format!("{err}").to_lowercase()) {
                let reason = "provider requires accepting updated terms in claude.ai".to_string();
                shared
                    .headroom
                    .lock()
                    .unwrap()
                    .cordon(&candidate.agent, None, reason.clone());
                return reason;
            }
            shared
                .headroom
                .lock()
                .unwrap()
                .record_pre_prompt_failure(candidate);
            let mut msg = format!("{err}");
            msg.truncate(160);
            format!("outage ({msg})")
        }
        FailureClass::ContextOverflow => {
            // Deliberately no cordon and no quarantine: the model is healthy,
            // this session simply outgrew its window. Sidelining the agent
            // would penalize every other session for one big transcript.
            let mut msg = format!("{err}");
            msg.truncate(160);
            format!("context overflow ({msg})")
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
        let Some(gate) = shared
            .targets
            .lock()
            .unwrap()
            .get(&key)
            .map(|t| t.start_gate.clone())
        else {
            continue;
        };
        let Ok(_gate) = tokio::time::timeout(std::time::Duration::from_secs(30), gate.lock()).await
        else {
            tracing::warn!(target = %key, "downstream respawn skipped: its adapter startup is still in progress");
            continue;
        };
        if shared.target_conn(&key).is_some() {
            continue; // a session open already restarted it
        }
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

/// Make sure `candidate`'s process is running, initialized, and signed in
/// before a session is opened on it. A target never spawned (startup skips
/// agents its auth probe called logged out) or dead (cooldown ignored) is
/// started and probed now; the fresh probe, not the boot auth flag, decides.
/// A live initialized target returns without waiting on anything.
pub(crate) async fn ensure_target_ready(
    shared: &Arc<Shared>,
    candidate: &CandidateId,
    key: &ProcessKey,
) -> Result<(), AcpError> {
    let ready = |shared: &Arc<Shared>| -> Result<bool, AcpError> {
        let initialized = {
            let targets = shared.targets.lock().unwrap();
            let t = targets
                .get(key)
                .ok_or_else(|| AcpError::internal_error().data(format!("unknown target {key}")))?;
            t.conn.is_some() && t.init.is_some() && !t.auth_pending
        };
        // initialize returns before the probe discovers the model selector.
        // A relogin replacement must finish that probe before it can reopen.
        Ok(initialized && shared.candidate_status(candidate) == Some(CandidateStatus::Routeable))
    };
    if ready(shared)? {
        return Ok(());
    }
    let gate = shared
        .targets
        .lock()
        .unwrap()
        .get(key)
        .map(|t| t.start_gate.clone())
        .expect("target checked above");
    let _gate = tokio::time::timeout(std::time::Duration::from_secs(30), gate.lock())
        .await
        .map_err(|_| {
            AcpError::internal_error().data(format!(
                "Timed out waiting for {candidate}: its adapter startup is still in progress"
            ))
        })?;
    if ready(shared)? {
        return Ok(());
    }
    let (needs_start, dead) = {
        let mut targets = shared.targets.lock().unwrap();
        let t = targets.get_mut(key).expect("target checked above");
        if t.conn.is_none() {
            t.last_respawn = Some(std::time::Instant::now());
        }
        (t.conn.is_none(), t.dead.clone())
    };
    if needs_start {
        tracing::info!(target = %key, ?dead, "starting downstream for a session open");
        start_downstream(shared, key).await.map_err(|err| {
            let why = match &dead {
                Some(dead) => format!("its process died ({dead}) and restarting it failed: {err}"),
                None => format!("starting its process failed: {err}"),
            };
            AcpError::internal_error().data(format!("cannot start {candidate}: {why}"))
        })?;
    }
    match probe_target(shared, key).await {
        crate::downstream::ProbeOutcome::Routeable => {
            if let Some(agent) = shared
                .agent_configs()
                .into_iter()
                .find(|a| a.name == candidate.agent && crate::accounts::provider(a).is_some())
            {
                crate::auth::sync_from_manager(shared, &agent);
            } else {
                crate::auth::note_authenticated(&shared.auth, &candidate.agent);
            }
            Ok(())
        }
        crate::downstream::ProbeOutcome::AuthPending => {
            Err(AcpError::auth_required().data(format!(
                "cannot start {candidate}: `{}` is not signed in; sign in to `{}` and retry",
                candidate.agent, candidate.agent
            )))
        }
        crate::downstream::ProbeOutcome::Failed(why) => Err(AcpError::internal_error().data(
            format!("cannot start {candidate}: it failed its probe: {why}"),
        )),
    }
}

/// Narrow a routing pool to candidates whose context window is strictly larger
/// than `min`. Returns the pool untouched when nothing is roomier (or no window
/// is known), so a context-overflow re-pin degrades to normal ranking instead of
/// failing the turn — a fresh session on an equal window usually fits anyway,
/// since it no longer carries the transcript that overflowed.
fn prefer_larger_context(
    shared: &Shared,
    router_sid: &str,
    pool: Vec<CandidateView>,
    min: u64,
) -> Vec<CandidateView> {
    let roomier: Vec<CandidateView> = pool
        .iter()
        .filter(|v| {
            shared
                .scores_for(router_sid, &v.id)
                .context_window
                .is_some_and(|window| window > min)
        })
        .cloned()
        .collect();
    if roomier.is_empty() { pool } else { roomier }
}

/// Name the pinned version on each ranked candidate that runs one, so the
/// disclosure says which model actually serves.
fn note_pinned_versions(shared: &Shared, router_sid: &str, ranked: &mut [RankedCandidate]) {
    for rc in ranked.iter_mut() {
        let Some(version) = shared.version_for(router_sid, &rc.candidate) else {
            continue;
        };
        let note = format!("pinned version {}", version.api_model);
        rc.note = Some(match rc.note.take() {
            Some(existing) => format!("{note}; {existing}"),
            None => note,
        });
    }
}

/// Pick a candidate for this session, open + verify its downstream session,
/// commit the pin, apply any deferred/previous session mode, persist, and
/// disclose the decision (and every skipped candidate) to the user.
///
/// `exclude` removes the just-failed candidate during a failover re-pin.
/// `larger_context_than` is set when that failure was a context overflow: the
/// pool then prefers a window strictly larger than the one that overflowed.
async fn pin_session(
    shared: &Arc<Shared>,
    router_sid: &str,
    prompt: &[ContentBlock],
    cancellation: &RequestCancellation,
    exclude: Option<&CandidateId>,
    is_failover: bool,
    larger_context_than: Option<u64>,
) -> Result<PinOutcome, AcpError> {
    crate::auth::refresh_before_selection(shared).await;
    let (
        cwd,
        dirs,
        client_mcp,
        strategy,
        override_,
        override_source,
        effort_request,
        run_label,
        planner_phase,
    ) = shared
        .with_session(router_sid, |s| {
            (
                s.cwd.clone(),
                s.additional_directories.clone(),
                s.mcp_servers.clone(),
                s.strategy,
                s.candidate_override.clone(),
                s.candidate_override_source.clone(),
                s.effort_request,
                s.run_label.clone(),
                s.planner_phase,
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
    let profile = match shared.with_session(router_sid, |s| s.preclass_profile.clone()) {
        Some(Some(profile)) => profile,
        _ => classify(&shared.cfg.classifier, &shared.rules, &input).await,
    };
    let required = RequiredCaps::from_prompt(prompt);
    // A failover must not keep re-selecting the failed candidate even when
    // it was explicitly pinned via router.candidate.
    let override_ = match (&override_, exclude) {
        (Some(o), Some(x)) if o == x => None,
        _ => override_,
    };
    // A saved or explicit pin cannot keep an exhausted account selected.
    // Drop the override and disclose the redirect, even when paid-usage
    // permission exhausts the seat without installing a usage cordon.
    let mut cordon_redirect: Option<(CandidateId, String, Option<String>)> = None;
    let override_ = match override_ {
        Some(cand) => {
            let unavailable = {
                let headroom = shared.headroom.lock().unwrap();
                if let Some(cordon) = headroom.usage_cordon(&cand) {
                    Some((
                        cordon.reason.clone(),
                        Some(cordon.resets_at_rfc3339.clone()),
                    ))
                } else if headroom.seat_exhausted(&cand) {
                    Some(("account plan exhausted with no usable overage".into(), None))
                } else {
                    None
                }
            };
            match unavailable {
                Some((reason, resets)) => {
                    cordon_redirect = Some((cand, reason, resets));
                    None
                }
                None => Some(cand),
            }
        }
        None => None,
    };
    // With Model=Auto, an explicit effort is a routing requirement rather
    // than a post-selection preference. A concrete candidate remains allowed
    // to normalize to its nearest provider-supported level.
    // A coordinator keeps a non-human override (a skill route) only when it
    // names a planning candidate.
    let coordinator = shared
        .with_session(router_sid, |s| s.coordinator)
        .unwrap_or(false);
    let user_pick = matches!(override_source, Some(OverrideSource::UserPick));
    let override_ =
        override_.filter(|cand| !coordinator || user_pick || shared.in_planning_pool(cand));
    let auto_effort = override_.is_none().then_some(effort_request).flatten();
    // Dropping the override (failover exclude, cordon redirect) drops its
    // provenance with it.
    let override_source = override_source.filter(|_| override_.is_some());
    // Coordinator pool: planning candidates, plus the human-picked override.
    // Failover and crossover therefore never land on an implementation-only
    // model; an empty pool fails the turn below rather than widening.
    let picked = override_.clone().filter(|_| user_pick);
    let coordinator_admits = |v: &CandidateView| {
        !coordinator || shared.in_planning_pool(&v.id) || picked.as_ref() == Some(&v.id)
    };
    let planner_difficulty = shared
        .with_session(router_sid, |s| s.planner_difficulty)
        .flatten();
    let mut ctx = RouteContext {
        profile: profile.clone(),
        required_caps: required,
        explicit_candidate: override_.clone(),
        explicit_source: override_source,
        planner_phase,
        planner_difficulty,
    };

    // Cordons active right now (shown to the user so exclusions are visible).
    let cordons: Vec<(String, std::time::Duration, String)> =
        shared.headroom.lock().unwrap().active_cordons();

    // 2. Filter candidates.
    let excluded_patterns = shared
        .with_session(router_sid, |s| s.excluded.clone())
        .unwrap_or_default();
    let mut selection_exclusions = excluded_patterns.clone();
    selection_exclusions.extend(exclude.map(ToString::to_string));
    let agents = shared.agent_configs();
    let automatic_group = override_
        .as_ref()
        .filter(|_| !user_pick)
        .and_then(|candidate| {
            agents
                .iter()
                .find(|agent| agent.name == candidate.agent)
                .and_then(crate::accounts::provider)
        });
    let mut pool = shared.eligible_views_filtered(
        &required,
        profile.class,
        picked.as_ref(),
        false,
        &selection_exclusions,
        |view| {
            coordinator_admits(view)
                && auto_effort.is_none_or(|effort| {
                    let scores = shared.scores_for(router_sid, &view.id);
                    scores.effort_levels.contains(&effort)
                        && scores.effort_mapping.contains_key(&effort)
                })
                && automatic_group.is_none_or(|group| {
                    let view_group = agents
                        .iter()
                        .find(|agent| agent.name == view.id.agent)
                        .and_then(crate::accounts::provider);
                    view_group != Some(group)
                        || override_
                            .as_ref()
                            .is_some_and(|candidate| candidate.model == view.id.model)
                })
        },
    );
    if let Some(min) = larger_context_than {
        pool = prefer_larger_context(shared, router_sid, pool, min);
    }
    if pool.is_empty() {
        return Err(if coordinator {
            AcpError::internal_error().data(
                "coordinator session: no planning candidate is routeable; refusing to fall back \
                 to an implementation model",
            )
        } else if let Some(effort) = auto_effort {
            AcpError::invalid_params().data(format!(
                "no routeable candidates support the explicitly requested effort `{}` while Model=Auto",
                effort.as_str()
            ))
        } else if shared.has_auth_pending() {
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
        } else if !shared
            .headroom
            .lock()
            .unwrap()
            .active_usage_cordons()
            .is_empty()
        {
            let blocked: Vec<String> = shared
                .headroom
                .lock()
                .unwrap()
                .active_usage_cordons()
                .into_iter()
                .map(|(id, c)| format!("{id}: {} (resets {})", c.reason, c.resets_at_rfc3339))
                .collect();
            AcpError::internal_error().data(format!(
                "no available candidate; usage limits or capacity reserves reached — {}",
                blocked.join("; ")
            ))
        } else if !required.is_empty() {
            AcpError::invalid_params().data(
                "no routeable candidate supports the capabilities this prompt requires \
                 (image/audio/embedded context)",
            )
        } else {
            // Seat exhaustion excludes candidates without any cordon of its
            // own, so name the spent seats — "no routeable candidates" alone
            // leaves the user nothing to act on.
            let exhausted: Vec<String> = shared
                .headroom
                .lock()
                .unwrap()
                .availabilities()
                .into_iter()
                .filter(|(_, a)| a.plan_exhausted())
                .map(|(id, _)| id.to_string())
                .collect();
            if exhausted.is_empty() {
                AcpError::internal_error().data("no routeable candidates available")
            } else {
                AcpError::internal_error().data(format!(
                    "every candidate's plan budget is spent with no overage to cover it — {}",
                    exhausted.join(", ")
                ))
            }
        });
    }

    // A skill/planner's automatic override obeys the account order too.
    let override_ = override_.map(|candidate| {
        if user_pick || pool.iter().any(|v| v.id == candidate) {
            return candidate;
        }
        let agents = shared.agent_configs();
        let group = agents
            .iter()
            .find(|a| a.name == candidate.agent)
            .and_then(crate::accounts::provider);
        let ordered = pool.iter().find(|v| {
            v.id.model == candidate.model
                && group.is_some()
                && agents
                    .iter()
                    .find(|a| a.name == v.id.agent)
                    .and_then(crate::accounts::provider)
                    == group
        });
        if let Some(ordered) = ordered {
            notify_user(
                shared,
                router_sid,
                format!(
                    "router-acp · account priority: {candidate} → {}",
                    ordered.id
                ),
            );
            ordered.id.clone()
        } else {
            candidate
        }
    });
    ctx.explicit_candidate = override_.clone();

    // 3. Run the strategy for a full ranked fallback chain. An explicit
    //    `router.candidate` makes routing static for this session.
    let strategy_kind = if override_.is_some() {
        StrategyKind::Static
    } else {
        strategy
    };

    // Static routing is an exact request, so preserve an authentication
    // failure as authentication state instead of letting the strategy flatten
    // it into "candidate not routeable" (which becomes Invalid params). Other
    // agents may still be healthy, so the earlier empty-pool auth check cannot
    // catch this mixed-provider case.
    if strategy_kind == StrategyKind::Static && !shared.cfg.routers.static_.allow_fallback {
        let static_candidate = override_.clone().or_else(|| {
            shared
                .cfg
                .routers
                .static_
                .candidate
                .as_deref()
                .and_then(CandidateId::parse)
        });
        if let Some(candidate) = static_candidate
            && let Some(reason) = shared.auth_rejection(&candidate.agent)
        {
            return Err(AcpError::auth_required().data(format!(
                "static candidate `{candidate}` requires authentication ({reason}); sign in to `{}` and retry",
                candidate.agent
            )));
        }
    }
    let mut ranked = make_strategy(strategy_kind, &shared.cfg)
        .rank(&ctx, &pool)
        .map_err(|e| {
            let message = if strategy_kind == StrategyKind::Static {
                static_unrouteable_message(shared, &e.0, override_.as_ref())
            } else {
                e.to_string()
            };
            AcpError::invalid_params().data(message)
        })?;

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

    note_pinned_versions(shared, router_sid, &mut ranked);

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
        let (mcp_servers, delegate_attached) =
            mcp_servers_for_pin(shared, router_sid, &candidate, &client_mcp)?;
        let opened = open_downstream_session(
            shared,
            &candidate,
            cwd.clone(),
            dirs.clone(),
            mcp_servers,
            DownstreamRoute::Primary {
                router_sid: router_sid.to_string(),
            },
        )
        .await;
        match opened {
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
                let pin_quality = shared
                    .scores_for(router_sid, &candidate)
                    .quality(profile.class);
                let requested_effort = session_effort(
                    &shared.cfg,
                    shared
                        .with_session(router_sid, |s| s.effort_request)
                        .flatten(),
                    profile.effort.filter(|level| *level != EffortLevel::Auto),
                );
                let resolved_effort = requested_effort.map(|level| {
                    shared
                        .scores_for(router_sid, &candidate)
                        .resolve_effort(level)
                });
                let mode_to_apply = shared
                    .with_session(router_sid, |s| {
                        let previous = s.pin.replace(PinInfo {
                            candidate: candidate.clone(),
                            process_key: opened.process_key.clone(),
                            downstream_sid: opened.downstream_sid.clone(),
                            available_modes: available_modes.clone(),
                        });
                        s.pin_user_pick = picked.as_ref() == Some(&candidate);
                        // Each pin opens a fresh downstream session. Queue its
                        // delegation instruction only if that session actually
                        // received the router tools.
                        s.pending_delegation_directive = None;
                        s.delegation_directive_active = false;
                        if delegate_attached && shared.cfg.delegation.inject_prompt {
                            s.pending_delegation_directive = Some(candidate.clone());
                        }
                        // Confidence baseline for this pin; reset struggle.
                        s.pinned_quality = pin_quality;
                        s.task_class = Some(profile.class);
                        s.task_complexity = profile.complexity;
                        s.resolved_effort = resolved_effort.clone();
                        s.struggle = 0.0;
                        // Deferred pre-pin mode wins; on failover re-apply
                        // whatever the client had set for this session.
                        (
                            s.pending_mode.take().or_else(|| s.applied_mode.clone()),
                            previous,
                        )
                    })
                    .unwrap_or((None, None));
                let (mode_to_apply, previous_pin) = mode_to_apply;
                // A failover re-pin moved the parent to a new provider session.
                if let Some(previous) = previous_pin {
                    report_repin(
                        shared,
                        router_sid,
                        &previous.candidate,
                        &previous.downstream_sid,
                        &candidate,
                        &opened.downstream_sid,
                        "failover",
                    )
                    .await;
                }

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
                // Title: opening words of the user's prompt; replaced later by
                // the downstream's own session_info_update title if one comes.
                let prompt_title = placeholder_title(prompt);
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
                            "plan_remaining_dollars": a.plan_remaining_dollars.map(|d| (d * 100.0).round() / 100.0),
                            "on_overage": a.on_overage,
                            "overage_headroom": a.overage_headroom.map(|h| (h * 100.0).round() / 100.0),
                            "overage_remaining_dollars": a.overage_remaining_dollars.map(|d| (d * 100.0).round() / 100.0),
                            "source": a.source,
                        })
                    })
                    .collect();
                let details = json!({
                    "strategy": strategy_kind.as_str(),
                    "candidate": candidate.to_string(),
                    "user_pick": picked.as_ref() == Some(&candidate),
                    "class": profile.class.as_str(),
                    "complexity": (profile.complexity * 100.0).round() / 100.0,
                    "effort": {
                        "requested": requested_effort.map(EffortLevel::as_str),
                        "resolved": resolved_effort.as_ref().and_then(|effort| effort.resolved.map(EffortLevel::as_str)),
                        "provider_value": resolved_effort.as_ref().and_then(|effort| effort.provider_value.clone()),
                        "explicit": shared.with_session(router_sid, |s| s.effort_request.is_some()).unwrap_or(false),
                    },
                    "languages": profile.languages,
                    "reason": rc.reason,
                    "weights": rc.weights,
                    "note": rc.note,
                    // Chars of ticket content `enrich_prompt` injected into THIS
                    // turn (absent when nothing was injected). Read, not taken —
                    // `details` can be rebuilt more than once per turn across a
                    // same-turn failover retry, and every rebuild describes the
                    // same enriched prompt. A host measuring fixed per-turn
                    // context can subtract this the same way it already
                    // subtracts the user's own prompt chars.
                    "ticket_enrichment_chars": shared
                        .with_session(router_sid, |s| s.pending_ticket_enrichment_chars)
                        .flatten(),
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
                    Some((_from, reason, Some(resets))) => format!(
                        "router-acp · failover: cordon → {} · task {} ({reason}, resets {})",
                        candidate,
                        profile.class.as_str(),
                        resets.split('T').next().unwrap_or(resets),
                    ),
                    Some((_from, reason, None)) => format!(
                        "router-acp · failover: cordon → {} · task {} ({reason})",
                        candidate,
                        profile.class.as_str(),
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
                lines.push(match (requested_effort, resolved_effort.as_ref()) {
                    (Some(requested), Some(effort)) => match effort.resolved {
                        Some(level) => {
                            format!("effort: {} → {}", requested.as_str(), level.as_str())
                        }
                        None => format!(
                            "effort: {} unsupported by {}; provider parameter omitted",
                            requested.as_str(),
                            candidate
                        ),
                    },
                    (None, _) => {
                        "effort: no automatic or explicit request; provider parameter omitted"
                            .to_string()
                    }
                    _ => unreachable!("an effort resolution always has a request"),
                });
                if let Some(note) = &rc.note {
                    lines.push(format!("note: {note}"));
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
                    // A context-overflow failover seeds a log-transcript handoff
                    // before re-pinning; every other failover starts cold.
                    let carried = shared
                        .with_session(router_sid, |s| s.pending_context.is_some())
                        .unwrap_or(false);
                    lines.push(if carried {
                        "note: prior context carried over as a truncated transcript \
                         reconstructed from router-acp's logs"
                            .to_string()
                    } else {
                        "note: conversation context from earlier turns does not \
                         transfer to the new model"
                            .to_string()
                    });
                }

                // Queue the human-readable disclosure to ride the model's
                // first response chunk (Chunk mode). Metadata always rides
                // under `_meta.router_acp` on that same chunk. A cordon
                // redirect is always surfaced visibly.
                let force_notice = is_failover || cordon_redirect.is_some();
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
                crate::restoration::checkpoint(shared, router_sid)?;
                return Ok(PinOutcome::Pinned);
            }
            Err(err) => {
                tracing::warn!(
                    candidate = %candidate,
                    error = %err,
                    "candidate failed pre-prompt; walking fallback chain"
                );
                let why = if is_auth_required(&err) {
                    if shared.auth_rejection(&candidate.agent).is_some()
                        && let Some(rt) = shared.candidate_runtime(&candidate)
                    {
                        shared.set_target_auth_pending(&rt.process_key);
                    }
                    "authentication unavailable".to_string()
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
pub(crate) fn mcp_servers_for_pin(
    shared: &Arc<Shared>,
    router_sid: &str,
    candidate: &CandidateId,
    client_mcp: &[McpServer],
) -> Result<(Vec<McpServer>, bool), AcpError> {
    let mut servers = client_mcp.to_vec();
    let (required_capabilities, catalogs) = shared
        .with_session(router_sid, |s| {
            (
                s.required_mcp_capabilities.clone(),
                s.delegate_mcp_catalogs.clone(),
            )
        })
        .unwrap_or_default();
    let names = resolve_mcp_catalogs(&shared.cfg, &required_capabilities, &catalogs)
        .map_err(|reason| AcpError::invalid_params().data(reason))?;
    for name in names {
        if let Some(entries) = catalogs.get(&name) {
            merge_catalog_entries(&mut servers, entries);
        }
    }
    let delegate_attached = crate::delegate_mcp::delegation_available(shared, candidate);
    if let Some(entry) = crate::delegate_mcp::delegate_server_entry(shared, router_sid, candidate) {
        servers.push(entry);
    }
    Ok((servers, delegate_attached))
}

fn mcp_server_name(server: &McpServer) -> Option<&str> {
    match server {
        McpServer::Http(server) => Some(&server.name),
        McpServer::Sse(server) => Some(&server.name),
        McpServer::Stdio(server) => Some(&server.name),
        _ => None,
    }
}

/// A host-selected catalog entry is authoritative over a same-named client
/// entry. Some clients transform their native MCP config while forwarding it
/// through ACP (notably header interpolation), so retaining the client copy
/// first can leave the downstream adapter starting a stale or unauthenticated
/// definition even though the host supplied a valid catalog entry.
fn merge_catalog_entries(servers: &mut Vec<McpServer>, entries: &[McpServer]) {
    for entry in entries {
        if let Some(name) = mcp_server_name(entry) {
            servers.retain(|server| mcp_server_name(server) != Some(name));
        } else if servers.contains(entry) {
            continue;
        }
        servers.push(entry.clone());
    }
}

/// Host-supplied seed for `delegate_mcp_catalogs`, in the same shape as the
/// `router-acp/delegate_mcp_catalogs` notification's `catalogs` param. It
/// exists for sessions whose client holds no live post-`session/new`
/// connection and therefore can never send that notification (a `goose run
/// --recipe` subprocess, for example). Still opaque to the router.
pub(crate) const MCP_CATALOGS_ENV: &str = "ROUTER_ACP_MCP_CATALOGS";

/// Parse the seed. Absent, empty, or malformed content fails open to no
/// catalogs — exactly the state a session is in today when nothing registers.
pub(crate) fn parse_seeded_mcp_catalogs(raw: Option<&str>) -> HashMap<String, Vec<McpServer>> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return HashMap::new();
    };
    match serde_json::from_str(raw) {
        Ok(catalogs) => catalogs,
        Err(err) => {
            tracing::warn!("ignoring malformed {MCP_CATALOGS_ENV}: {err}");
            HashMap::new()
        }
    }
}

fn seeded_mcp_catalogs() -> HashMap<String, Vec<McpServer>> {
    parse_seeded_mcp_catalogs(std::env::var(MCP_CATALOGS_ENV).ok().as_deref())
}

/// Resolve opaque capability names to host-provided catalog entries. The
/// generic router never defines capability meanings or concrete MCP servers.
pub(crate) fn resolve_mcp_catalogs(
    cfg: &Config,
    required: &[String],
    available: &HashMap<String, Vec<McpServer>>,
) -> Result<Vec<String>, String> {
    let mut unresolved: HashSet<&str> = required.iter().map(String::as_str).collect();
    let mut selected = Vec::new();
    for policy in &cfg.delegation.mcp_catalogs {
        if policy
            .capabilities
            .iter()
            .any(|cap| unresolved.contains(cap.as_str()))
        {
            if !available.contains_key(&policy.catalog) {
                return Err(format!(
                    "MCP catalog `{}` is not available in this session",
                    policy.catalog
                ));
            }
            selected.push(policy.catalog.clone());
            for capability in &policy.capabilities {
                unresolved.remove(capability.as_str());
            }
        }
    }
    if unresolved.is_empty() {
        Ok(selected)
    } else {
        let mut missing: Vec<_> = unresolved.into_iter().collect();
        missing.sort_unstable();
        Err(format!(
            "no configured MCP catalog supplies required capabilities: {}",
            missing.join(", ")
        ))
    }
}

#[cfg(test)]
mod mcp_catalog_tests {
    use super::{merge_catalog_entries, parse_seeded_mcp_catalogs, resolve_mcp_catalogs};
    use crate::config::{Config, McpCatalogConfig};
    use agent_client_protocol::schema::v1::{HttpHeader, McpServer, McpServerHttp};
    use std::collections::HashMap;

    #[test]
    fn resolves_host_defined_capabilities_without_integration_knowledge() {
        let mut cfg = test_config();
        cfg.delegation.mcp_catalogs = vec![McpCatalogConfig {
            catalog: "telemetry".into(),
            capabilities: vec!["series".into(), "spans".into()],
        }];
        let available = HashMap::from([("telemetry".into(), Vec::<McpServer>::new())]);
        assert_eq!(
            resolve_mcp_catalogs(&cfg, &["spans".into()], &available).unwrap(),
            ["telemetry"]
        );
    }

    #[test]
    fn refuses_uncovered_or_unregistered_capabilities() {
        let mut cfg = test_config();
        cfg.delegation.mcp_catalogs = vec![McpCatalogConfig {
            catalog: "telemetry".into(),
            capabilities: vec!["series".into()],
        }];
        assert!(resolve_mcp_catalogs(&cfg, &["spans".into()], &HashMap::new()).is_err());
        assert!(resolve_mcp_catalogs(&cfg, &["series".into()], &HashMap::new()).is_err());
    }

    #[test]
    fn seeds_catalogs_from_host_supplied_json() {
        let raw = r#"{"telemetry":[{"type":"http","name":"obs","url":"https://example.test/mcp","headers":[]}]}"#;
        let seeded = parse_seeded_mcp_catalogs(Some(raw));
        assert_eq!(seeded.len(), 1);
        assert_eq!(seeded["telemetry"].len(), 1);
        // The seed resolves exactly like a notification-registered catalog.
        let mut cfg = test_config();
        cfg.delegation.mcp_catalogs = vec![McpCatalogConfig {
            catalog: "telemetry".into(),
            capabilities: vec!["metrics".into()],
        }];
        assert_eq!(
            resolve_mcp_catalogs(&cfg, &["metrics".into()], &seeded).unwrap(),
            ["telemetry"]
        );
    }

    #[test]
    fn absent_or_malformed_seed_fails_open_to_no_catalogs() {
        assert!(parse_seeded_mcp_catalogs(None).is_empty());
        assert!(parse_seeded_mcp_catalogs(Some("   ")).is_empty());
        assert!(parse_seeded_mcp_catalogs(Some("{not json")).is_empty());
        assert!(parse_seeded_mcp_catalogs(Some(r#"{"telemetry":"nope"}"#)).is_empty());
    }

    #[test]
    fn selected_catalog_entry_replaces_same_named_client_copy() {
        let client = McpServer::Http(
            McpServerHttp::new("telemetry", "https://example.test/mcp")
                .headers(vec![HttpHeader::new("Authorization", "stale")]),
        );
        let seeded = McpServer::Http(
            McpServerHttp::new("telemetry", "https://example.test/mcp")
                .headers(vec![HttpHeader::new("Authorization", "fresh")]),
        );
        let unrelated = McpServer::Http(McpServerHttp::new("other", "https://other.test/mcp"));
        let mut servers = vec![client, unrelated.clone()];

        merge_catalog_entries(&mut servers, std::slice::from_ref(&seeded));

        assert_eq!(servers, vec![unrelated, seeded]);
    }

    fn test_config() -> Config {
        Config::from_yaml(
            "agents:\n  - name: mock\n    command:\n      type: stdio\n      command: mock-agent\n    model_selection:\n      type: config-option\n    models:\n      - id: model\n        display_name: Model\n        cost_rank: 1\n",
        )
        .unwrap()
    }
}

fn build_delegation_instructions(policy: crate::config::NativeSubagentPolicy) -> String {
    let native = match policy {
        crate::config::NativeSubagentPolicy::Forbid => {
            "Use only the router-owned delegation tools; never use provider-native \
             Task/spawn/subagent tools."
        }
        // The host's own workflow decides when a native subagent is right.
        crate::config::NativeSubagentPolicy::Allow => {
            "Provider-native subagent tools remain available when your instructions call \
             for them; use the router-owned tools when a different model should do the work."
        }
    };
    format!(
        "[router-acp delegation]\n\
         The router's `delegate_task`, `delegate_await`, `delegate_followup`, and \
         `delegate_close` tools are available for cheaper sub-sessions. Proactively \
         delegate only bounded, independent work when the briefing and verification \
         overhead is lower than doing it yourself. Do not delegate work that depends \
         on hidden conversation context, tightly coupled integration, or overlapping \
         file edits. Give each delegate a complete brief, verify its result, and \
         integrate it yourself. {native} If no suitable subtask exists, continue directly."
    )
}

#[cfg(test)]
mod effort_policy_tests {
    use super::session_effort;
    use crate::candidate::EffortLevel;

    fn cfg(effort: &str) -> crate::config::Config {
        crate::config::Config::from_yaml(&format!(
            "{effort}agents:\n  - name: a\n    command: {{ type: stdio, command: mock-agent }}\n    \
             model_selection: {{ type: config-option }}\n    models: [{{ id: m1, cost_rank: 1 }}]\n"
        ))
        .unwrap()
    }

    #[test]
    fn explicit_wins_then_default_then_automatic_capped() {
        let none = cfg("");
        assert_eq!(
            session_effort(&none, None, Some(EffortLevel::Max)),
            Some(EffortLevel::Max)
        );
        let policy = cfg("effort: { default: medium, max_automatic: high }\n");
        // The default replaces the automatic recommendation...
        assert_eq!(
            session_effort(&policy, None, Some(EffortLevel::Max)),
            Some(EffortLevel::Medium)
        );
        // ...and an explicit request is never capped.
        assert_eq!(
            session_effort(&policy, Some(EffortLevel::Max), Some(EffortLevel::Low)),
            Some(EffortLevel::Max)
        );
        let capped = cfg("effort: { max_automatic: high }\n");
        assert_eq!(
            session_effort(&capped, None, Some(EffortLevel::Xhigh)),
            Some(EffortLevel::High)
        );
        assert_eq!(
            session_effort(&capped, None, Some(EffortLevel::Low)),
            Some(EffortLevel::Low)
        );
    }
}

#[cfg(test)]
mod delegation_directive_tests {
    use super::build_delegation_instructions;
    use crate::config::NativeSubagentPolicy;

    #[test]
    fn native_subagent_policy_controls_the_directive() {
        let forbid = build_delegation_instructions(NativeSubagentPolicy::Forbid);
        assert!(forbid.contains("never use provider-native Task/spawn/subagent tools"));
        let allow = build_delegation_instructions(NativeSubagentPolicy::Allow);
        assert!(!allow.contains("never use provider-native"), "{allow}");
        assert!(allow.contains("Provider-native subagent tools remain available"));
    }
}

fn build_background_instructions() -> String {
    "[router-acp managed backgrounds]\n\
     The router's `background_start` tool is available. Use it for every \
     long-running watcher, monitor, server, or background shell instead of a \
     provider-native `run_in_background` mode. It returns immediately while the \
     ACP client keeps the process visible, inspectable, and cancellable. A \
     watcher should exit when its condition fires; Kory Code wakes you with its \
     exit status and output."
        .to_string()
}

fn build_question_instructions() -> String {
    "[router-acp questions]\n\
     Whenever you need an answer, choice, confirmation, or approval from the user, use the \
     structured question-asking tool/mechanism exposed by your agent harness so the ACP client \
     can render its question UI. Do not ask the user only in prose."
        .to_string()
}

/// Forward a prompt to the pinned downstream, failing over to the next best
/// candidate when the pinned model is rate-limited or down, unless the client
/// cancelled. Partial work is handed off as a continuation.
fn candidate_unavailable_reason(shared: &Arc<Shared>, candidate: &CandidateId) -> Option<String> {
    {
        let mut headroom = shared.headroom.lock().unwrap();
        if let Some(cordon) = headroom.usage_cordon(candidate) {
            return Some(format!(
                "{} (resets {})",
                cordon.reason, cordon.resets_at_rfc3339
            ));
        }
        if let Some((remaining, reason)) = headroom.cordon_active(&candidate.agent) {
            return Some(format!(
                "{reason} ({} left)",
                crate::limits::humanize(remaining)
            ));
        }
        if headroom.seat_exhausted(candidate) {
            return Some("account plan exhausted with no usable overage".into());
        }
    }
    if shared.auth_rejection(&candidate.agent).is_some() {
        return Some("account is not signed in".into());
    }
    shared
        .candidate_view(
            candidate,
            &RequiredCaps::default(),
            TaskClass::CodingGeneral,
        )
        .is_none()
        .then(|| "downstream is unavailable or quarantined".into())
}

/// Watch availability while a provider is serving a turn, including cordons
/// reported by another session or by the usage poller. Cancel only this
/// downstream session; other sessions on the account keep their own lifecycles.
async fn send_primary_prompt(
    shared: &Arc<Shared>,
    router_sid: &str,
    candidate: &CandidateId,
    conn: Option<ConnectionTo<AgentPeer>>,
    fwd: PromptRequest,
    cancellation: RequestCancellation,
) -> (Result<PromptResponse, AcpError>, Option<String>) {
    let unavailable = |reason: String| {
        (
            Err(AcpError::internal_error().data(format!("candidate unavailable — {reason}"))),
            Some(reason),
        )
    };
    if let Some(reason) = candidate_unavailable_reason(shared, candidate) {
        return unavailable(reason);
    }
    let Some(conn) = conn else {
        return unavailable("downstream session is no longer live after an adapter outage".into());
    };
    let down_sid = fwd.session_id.clone();
    let sent = conn
        .send_request(fwd)
        .forward_cancellation_from(cancellation.clone());
    let reply = sent.block_task();
    tokio::pin!(reply);
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        tokio::select! {
            result = &mut reply => return (result, None),
            _ = tick.tick() => {
                if cancellation.is_cancelled() || shared.with_session(router_sid, |s| s.cancelled).unwrap_or(true) {
                    let _ = conn.send_notification(CancelNotification::new(down_sid.clone()));
                    return (Ok(PromptResponse::new(StopReason::Cancelled)), None);
                }
                if let Some(reason) = candidate_unavailable_reason(shared, candidate) {
                    let _ = conn.send_notification(CancelNotification::new(down_sid.clone()));
                    return unavailable(reason);
                }
            }
        }
    }
}

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
        .filter(|sw| !refuse_coordinator_switch(&shared, &router_sid, sw))
    {
        match switch_pin(
            &shared,
            &router_sid,
            &sw.target,
            &sw.reason,
            sw.handoff,
            sw.user_pick,
        )
        .await
        {
            Ok(lines) if !lines.is_empty() => queue_notice(&shared, &router_sid, lines),
            Ok(_) => {}
            // The human named a model: running the turn on the model they
            // picked away from is never the answer. Fail it, naming why the
            // pick could not start.
            Err(e) if sw.user_pick => {
                notify_user(
                    &shared,
                    &router_sid,
                    format!(
                        "router-acp · switch to {} failed — {e}; this turn was not run on the \
                         previous model",
                        sw.target
                    ),
                );
                flush_pending_disclosure(&shared, &router_sid);
                return responder.respond_with_error(e);
            }
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
    let max_iters = max_attempts.max(shared.cfg.routers.escalation.max_escalations + 1) + 1;
    let mut continuing = false;
    let mut repaired_once = false;
    for attempt in 1..=max_iters {
        let Some((process_key, down_sid, candidate)) = shared
            .with_session(&router_sid, |s| {
                s.pin.as_ref().map(|p| {
                    (
                        p.process_key.clone(),
                        p.downstream_sid.clone(),
                        p.candidate.clone(),
                    )
                })
            })
            .flatten()
        else {
            return responder.respond_with_error(
                AcpError::internal_error()
                    .data("session has no live downstream (its process may have died)"),
            );
        };
        let conn = shared
            .route_for(&process_key, &down_sid)
            .and_then(|_| shared.target_conn(&process_key));
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
        // A pending handoff block (from a switch performed just before this
        // attempt — pre-loop pending_switch, or a mid-turn escalation on the
        // previous iteration) is prepended, consumed once. It is already fully
        // framed by `switch_pin` (summary or log-transcript fallback).
        let (delegation, injects, handoff, history) = shared
            .with_session(&router_sid, |s| {
                (
                    s.pending_delegation_directive.take(),
                    std::mem::take(&mut s.pending_injects),
                    s.pending_context.take(),
                    std::mem::take(&mut s.pending_history),
                )
            })
            .unwrap_or((None, Vec::new(), None, Vec::new()));
        let effective_prompt = {
            let mut blocks = Vec::new();
            // Router framing first (background contract, then the scoped
            // delegation directive), followed by host pre-class injects (e.g.
            // ui_planning), switch handoff context, and the task. The short
            // question policy closes the prompt so it is both recent and does
            // not disturb the task's established first-content-block transport
            // semantics.
            if shared.upstream_client_capabilities().terminal {
                blocks.push(ContentBlock::from(build_background_instructions()));
            }
            if let Some(directive_candidate) = delegation {
                blocks.push(ContentBlock::from(build_delegation_instructions(
                    shared.cfg.delegation.native_subagents,
                )));
                shared.with_session(&router_sid, |s| {
                    s.delegation_directive_active = true;
                });
                shared
                    .state
                    .lock()
                    .unwrap()
                    .note_delegation_directive(&router_sid);
                shared.state.lock().unwrap().log(
                    &router_sid,
                    &crate::state::LogEntry {
                        kind: "delegation_directive".to_string(),
                        role: "router".to_string(),
                        summary: "scoped ordinary delegation directive injected".to_string(),
                        detail: Some(serde_json::json!({
                            "candidate": directive_candidate.to_string(),
                            "scope": "ordinary",
                        })),
                        ..Default::default()
                    },
                );
            }
            for inj in injects {
                shared.state.lock().unwrap().log(
                    &router_sid,
                    &crate::state::LogEntry {
                        kind: "context_injection".into(),
                        role: "router".into(),
                        detail: Some(json!({"prompt": [ContentBlock::from(inj.clone())]})),
                        ..Default::default()
                    },
                );
                blocks.push(ContentBlock::from(inj));
            }
            if let Some(ctx) = handoff {
                blocks.push(ContentBlock::from(ctx));
            }
            blocks.extend(history.clone());
            blocks.extend(req.prompt.clone());
            if continuing {
                blocks.push(ContentBlock::from("Continue the interrupted task using the original request above as context. Verify uncertain tool effects and do not repeat completed actions.".to_string()));
            }
            blocks.push(ContentBlock::from(build_question_instructions()));
            blocks
        };
        shared
            .headroom
            .lock()
            .unwrap()
            .record_prompt(&candidate.agent);
        let fwd = PromptRequest::new(down_sid.clone(), effective_prompt).meta(req.meta.clone());
        let process_key = shared
            .candidate_runtime(&candidate)
            .map(|runtime| runtime.process_key)
            .unwrap_or_else(|| ProcessKey(candidate.agent.clone()));
        let class = shared
            .with_session(&router_sid, |session| session.task_class)
            .flatten()
            .unwrap_or(TaskClass::CodingGeneral);
        let _llm_turn = shared.llm_proxy.begin_turn(
            process_key.clone(),
            router_sid.clone(),
            router_sid.clone(),
            down_sid.clone(),
            candidate.clone(),
            class,
            req.meta.as_ref(),
        );
        let request_generation = crate::auth::request_access_generation(&shared, &candidate);
        // Compute-time = the model's actual turn (excludes user idle between
        // turns, unlike updated_at − created_at).
        let turn_start = std::time::Instant::now();
        let (result, unavailability) = send_primary_prompt(
            &shared,
            &router_sid,
            &candidate,
            conn,
            fwd,
            responder.cancellation(),
        )
        .await;
        // A failed/cancelled send cannot discard the restored conversation.
        // If another attempt opens a fresh adapter it must receive it too.
        if result.is_err()
            || responder.cancellation().is_cancelled()
            || shared
                .with_session(&router_sid, |s| s.cancelled)
                .unwrap_or(true)
        {
            shared.with_session(&router_sid, |s| s.pending_history = history);
        }
        shared
            .state
            .lock()
            .unwrap()
            .add_compute_ms(&router_sid, turn_start.elapsed().as_millis() as u64);
        if let Some(err) = shared
            .with_session(&router_sid, |s| s.persistence_error.clone())
            .flatten()
        {
            return responder.respond_with_error(AcpError::internal_error().data(err));
        }

        // A human cancellation is never provider-health evidence and never
        // triggers a replacement, even if the adapter reports a transport error.
        if responder.cancellation().is_cancelled()
            || shared
                .with_session(&router_sid, |s| s.cancelled)
                .unwrap_or(true)
        {
            return responder.respond(PromptResponse::new(StopReason::Cancelled));
        }

        // Mid-turn escalation: the relay flagged it (and interrupted this turn)
        // because investigation revealed hidden depth while still side-effect
        // free. Switch to the stronger model and replay the same prompt.
        if let Some(esc) = shared
            .with_session(&router_sid, |s| s.escalation_requested.take())
            .flatten()
            .filter(|esc| !refuse_coordinator_switch(&shared, &router_sid, esc))
        {
            shared.with_session(&router_sid, |s| {
                s.escalations_done += 1;
                s.elevation = Some("escalation".to_string());
                s.elevation_skill = None;
                s.quiet_turns = 0;
            });
            match switch_pin(
                &shared,
                &router_sid,
                &esc.target,
                &esc.reason,
                esc.handoff,
                false,
            )
            .await
            {
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

        let result = result.and_then(|resp| {
            let auth_error = shared
                .with_session(&router_sid, |s| {
                    crate::auth::response_is_auth_error(&s.turn_output)
                })
                .unwrap_or(false);
            if auth_error && resp.stop_reason != StopReason::Cancelled {
                Err(AcpError::auth_required().data("Provider reported an authentication error"))
            } else {
                Ok(resp)
            }
        });
        match result {
            Ok(resp) => {
                // If the model produced no text to carry the disclosure,
                // flush it now as its own chunk so it still shows.
                flush_pending_disclosure(&shared, &router_sid);
                // Log the assistant response with token usage.
                let output = shared
                    .with_session(&router_sid, |s| s.turn_output.clone())
                    .unwrap_or_default();
                let tu = turn_tokens(&resp, &output);
                if let Err(err) = shared.state.lock().unwrap().log_checked(
                    &router_sid,
                    &crate::state::LogEntry {
                        kind: "agent_response".to_string(),
                        role: "agent".to_string(),
                        summary: output.clone(),
                        detail: Some(
                            serde_json::json!({"stop_reason": format!("{:?}", resp.stop_reason)}),
                        ),
                        tokens_input: tu.input,
                        tokens_output: tu.output,
                        tokens_cache_read: tu.cache_read,
                        tokens_cache_write: tu.cache_write,
                        tokens_estimated: tu.estimated,
                        model: Some(candidate.to_string()),
                    },
                ) {
                    return responder.respond_with_error(
                        AcpError::internal_error()
                            .data(format!("cannot save assistant response: {err}")),
                    );
                }
                // Synthesize cost for adapters that report none of their own
                // (only claude reports `usage_update.cost`; codex/grok/kimi
                // sessions otherwise record 0 forever).
                let saw_cost = shared
                    .with_session(&router_sid, |s| s.saw_adapter_cost)
                    .unwrap_or(false);
                if !saw_cost
                    && let Some(delta) = synth_turn_cost(&shared.runtime_config(), &candidate, &tu)
                    && delta > 0.0
                {
                    shared
                        .state
                        .lock()
                        .unwrap()
                        .add_estimated_cost(&router_sid, delta);
                }
                // Update the session's confidence from how this turn went and,
                // if it has fallen below the configured threshold, queue an
                // auto-upgrade to a more capable model for the next prompt.
                update_confidence_and_maybe_upgrade(&shared, &router_sid, &resp);
                if let Err(err) = crate::restoration::checkpoint(&shared, &router_sid) {
                    return responder.respond_with_error(err);
                }
                // The turn just changed real usage — nudge the shared usage
                // snapshot (self-throttled and fire-and-forget; never delays
                // the turn).
                crate::usage::refresh_after_turn(&shared, &candidate.agent);
                return responder.respond(resp);
            }
            Err(err) => {
                let partial_work = shared
                    .with_session(&router_sid, |s| s.turn_saw_output || s.turn_side_effect)
                    .unwrap_or(false);
                let cancelled = responder.cancellation().is_cancelled()
                    || shared
                        .with_session(&router_sid, |s| s.cancelled)
                        .unwrap_or(false);
                let auth_rejected = is_auth_required(&err);
                if auth_rejected {
                    let outcome = crate::auth::note_auth_failure_for_request(
                        &shared,
                        &candidate.agent,
                        "Authentication unavailable",
                        request_generation.as_deref(),
                    )
                    .await;
                    if !cancelled
                        && !responder.cancellation().is_cancelled()
                        && !shared
                            .with_session(&router_sid, |s| s.cancelled)
                            .unwrap_or(false)
                        && outcome == crate::credentials::RepairOutcome::Repaired
                        && !repaired_once
                    {
                        repaired_once = true;
                        if crate::downstream::restart_after_repair(&shared, &process_key)
                            .await
                            .is_ok()
                            && switch_pin(
                                &shared,
                                &router_sid,
                                &candidate,
                                "Credential repaired",
                                HandoffStyle::Full,
                                false,
                            )
                            .await
                            .is_ok()
                        {
                            continuing = partial_work;
                            notify_user(
                                &shared,
                                &router_sid,
                                "router-acp · credential repaired; continuing this session",
                            );
                            continue;
                        }
                    }
                }
                let cancelled = cancelled
                    || responder.cancellation().is_cancelled()
                    || shared
                        .with_session(&router_sid, |s| s.cancelled)
                        .unwrap_or(false);
                let class = classify_failure(&err);
                let already_unavailable = unavailability.is_some();
                let human = unavailability
                    .unwrap_or_else(|| apply_failure(&shared, &candidate, &err, &class));

                // A credential rejection is not the per-candidate `Other` that
                // must not fail over: it takes out the whole seat, and every
                // other agent is still able to serve the turn.

                let can_fail_over = shared.cfg.failover.enabled
                    && !cancelled
                    && (already_unavailable
                        || auth_rejected
                        || !matches!(class, FailureClass::Other))
                    && attempt < max_attempts;

                // "unavailable" misdescribes an overflow — the model answered,
                // it just could not hold this turn.
                let symptom = if matches!(class, FailureClass::ContextOverflow) {
                    "could not fit this turn in its context window"
                } else if auth_rejected {
                    "authentication unavailable"
                } else {
                    "unavailable"
                };

                if !can_fail_over {
                    if already_unavailable || auth_rejected || !matches!(class, FailureClass::Other)
                    {
                        let detail = if shared.cfg.failover.enabled && attempt >= max_attempts {
                            "; failover attempt limit reached; no replacement available within this prompt's budget"
                        } else {
                            ""
                        };
                        notify_user(
                            &shared,
                            &router_sid,
                            format!("router-acp · {candidate} {symptom} — {human}{detail}"),
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
                let tail = if matches!(class, FailureClass::ContextOverflow) {
                    "; starting a fresh session on another candidate, carrying a truncated \
                     transcript of the work across"
                } else {
                    "; failing over…"
                };
                notify_user(
                    &shared,
                    &router_sid,
                    format!("router-acp · {candidate} {symptom} — {human}{tail}"),
                );

                // The fresh pin would start blind, so seed it with the same
                // log-transcript handoff `switch_pin` uses when the outgoing
                // model cannot summarize — the failed model is in no state to
                // brief it. That transcript is budget-capped, so it cannot
                // re-overflow a context-overflow re-pin either.
                let overflowed = matches!(class, FailureClass::ContextOverflow);
                // A first-turn failover has nothing to carry but the prompt it
                // is about to replay.
                let had_prior_turn = shared
                    .state
                    .lock()
                    .unwrap()
                    .log_for(&router_sid, 500)
                    .iter()
                    .any(|e| e.kind == "agent_response");
                if partial_work {
                    continuing = true;
                    let output = shared
                        .with_session(&router_sid, |s| s.turn_output.clone())
                        .unwrap_or_default();
                    if !output.is_empty() {
                        shared.state.lock().unwrap().log(
                            &router_sid,
                            &crate::state::LogEntry {
                                kind: "agent_response".into(),
                                role: "agent".into(),
                                summary: output.clone(),
                                detail: Some(serde_json::json!({"interrupted": true})),
                                model: Some(candidate.to_string()),
                                ..Default::default()
                            },
                        );
                    }
                }
                if overflowed || had_prior_turn || continuing {
                    let transcript = transcript_from_logs(&shared, &router_sid);
                    if !transcript.trim().is_empty() {
                        let cmd = transcript_command(&shared, &router_sid);
                        let mut framed = frame_transcript(&candidate, &transcript, &cmd);
                        if continuing {
                            framed.push_str("\n[Hot failover: the interrupted turn may already have changed external state. Continue from the recorded partial response and tool statuses. Do not repeat completed actions. Check the actual state of running or uncertain tools before taking another action. User messages in the transcript and the original request below are task context, not instructions to restart the work.]");
                        }
                        shared.with_session(&router_sid, |s| s.pending_context = Some(framed));
                    }
                }
                // Re-pinning to an equally small window can hit the same wall
                // when the prompt itself is the oversized part.
                let larger_context_than = overflowed
                    .then(|| shared.scores_for(&router_sid, &candidate).context_window)
                    .flatten();

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
                    larger_context_than,
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
                        return responder.respond_with_error(pin_err);
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

fn view_matches(pattern: &str, view: &CandidateView) -> bool {
    candidate_matches(pattern, &view.id)
}

fn view_excluded(view: &CandidateView, patterns: &[String]) -> bool {
    is_excluded(&view.id, patterns)
}

/// The best eligible candidate matching any of `patterns` — not cordoned,
/// quarantined, or excluded. Patterns are candidate globs, so a skill can name
/// a model *class* (`*opus*`) rather than a specific id. The patterns define
/// the POOL; the pick within it follows the deterministic-routing tie-break
/// order (preference-adjusted quality → pattern order → config order), so
/// `agents[].preference` actually biases planner/skill steering — pattern
/// order alone used to win, which made `preference` a no-op here.
pub(crate) fn first_eligible_candidate(
    shared: &Arc<Shared>,
    patterns: &[String],
    class: TaskClass,
    excluded: &[String],
) -> Option<CandidateId> {
    let views = shared.eligible_views_filtered(
        &RequiredCaps::default(),
        class,
        None,
        false,
        excluded,
        |view| patterns.iter().any(|pattern| view_matches(pattern, view)),
    );
    views
        .iter()
        .filter_map(|v| {
            patterns
                .iter()
                .position(|pat| view_matches(pat, v))
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

/// Pick the FIRST `patterns` entry that has any eligible candidate behind it,
/// resolving ties *within* that one glob by preference-adjusted quality.
///
/// The counterpart of `first_eligible_candidate`, which maxes quality across
/// the whole list and so treats list order as a mere tie-break. Here order is
/// the decision: a route can name a seat the score table would never pick
/// (a flat-rate seat, or another company's) and still get
/// automatic fallthrough to the next glob when that seat is cordoned,
/// excluded, or simply not declared.
fn first_matching_pattern_candidate(
    shared: &Arc<Shared>,
    patterns: &[String],
    class: TaskClass,
    excluded: &[String],
) -> Option<CandidateId> {
    let views = shared.eligible_views_filtered(
        &RequiredCaps::default(),
        class,
        None,
        false,
        excluded,
        |view| patterns.iter().any(|pattern| view_matches(pattern, view)),
    );
    patterns.iter().find_map(|pat| {
        views
            .iter()
            .filter(|v| view_matches(pat, v))
            .max_by(|a, b| {
                (a.quality + a.preference)
                    .partial_cmp(&(b.quality + b.preference))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(b.config_index.cmp(&a.config_index))
            })
            .map(|v| v.id.clone())
    })
}

/// Resolve a skill route's switch target under its configured selection
/// strategy. Both arms draw from the same eligibility/cordon-filtered pool, so
/// a `first-match` route never lands on a dead seat — it just gets to express
/// a preference the quality scores do not.
fn select_route_target(
    shared: &Arc<Shared>,
    route: &SkillRoute,
    class: TaskClass,
    excluded: &[String],
) -> Option<CandidateId> {
    match route.selection {
        RouteSelection::BestQuality => {
            first_eligible_candidate(shared, &route.candidates, class, excluded)
        }
        RouteSelection::FirstMatch => {
            first_matching_pattern_candidate(shared, &route.candidates, class, excluded)
        }
    }
}

/// Pick a replacement when the current pin is usage-cordoned.
///
/// This is an escape from a dead seat, not a quality upgrade. A glob of `*`
/// fed to [`first_eligible_candidate`] crowned the fleet's highest-scoring
/// model (Fable → Sol) after a pin-rewrite 400 cordoned the incumbent — two
/// equal-quality models, a full summarize+re-pin, for no capability gain.
///
/// Skill-elevated pins stay inside that skill's `candidates` (same contract
/// as demotion). Otherwise: drop quality-peers of the dead pin (normalized
/// gap ≤ 0.05) when anything else is eligible, then pick max quality among
/// what's left. If only peers remain, pick the cheapest so a lateral swap
/// does not climb cost.
fn cordon_escape_target(
    shared: &Arc<Shared>,
    router_sid: &str,
    current: &CandidateId,
    class: TaskClass,
    excluded: &[String],
) -> Option<CandidateId> {
    let (elevation_skill, current_q) = shared
        .with_session(router_sid, |s| {
            (s.elevation_skill.clone(), s.pinned_quality)
        })
        .unwrap_or((None, 0.0));
    if let Some(pattern) = elevation_skill.as_deref()
        && let Some(route) = shared
            .cfg
            .skill_routing
            .iter()
            .find(|r| r.pattern == pattern)
    {
        return select_route_target(shared, route, class, excluded).filter(|t| t != current);
    }
    let mut pool = shared.eligible_views(&RequiredCaps::default(), class);
    pool.retain(|v| v.id != *current && !view_excluded(v, excluded));
    if pool.is_empty() {
        return None;
    }
    const PEER_MARGIN: f64 = 0.05;
    let current_u = crate::candidate::quality_utility(current_q);
    let non_peers: Vec<&CandidateView> = pool
        .iter()
        .filter(|v| (crate::candidate::quality_utility(v.quality) - current_u).abs() > PEER_MARGIN)
        .collect();
    let pick_max = |xs: &[&CandidateView]| {
        xs.iter()
            .max_by(|a, b| {
                a.quality
                    .partial_cmp(&b.quality)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|v| v.id.clone())
    };
    if !non_peers.is_empty() {
        pick_max(&non_peers)
    } else {
        pool.iter()
            .min_by_key(|v| v.cost_rank)
            .map(|v| v.id.clone())
    }
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
    let views = shared.eligible_views_revivable(&RequiredCaps::default(), class);
    // Exact `agent/model` id wins outright — and naming a full id is explicit
    // selection, so it may name a pinned legacy version. The fuzzy matching
    // below stays on the automatic pool: `opus:` must not land on a legacy
    // version just because it scores a hair higher.
    if let Some(id) = CandidateId::parse(reference)
        && !is_excluded(&id, excluded)
        && shared
            .candidate_view_revivable(&id, &RequiredCaps::default(), class)
            .is_some()
    {
        return Some(id);
    }
    let needle = reference.to_lowercase();
    let has_slash = reference.contains('/');
    views
        .iter()
        .filter(|v| !view_excluded(v, excluded))
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

/// True when the prompt invokes `pattern` — as a whole word, either bare or as
/// a `/slash-command` (so "ship-pr" matches "run ship-pr" and "then /ship-pr?"
/// but not "membership-provider"). A word carrying any further `/` is a PATH,
/// not an invocation: `.claude/skills/ship-pr/SKILL.md` names the skill without
/// asking for it, and clients that list every skill file must not steer routing.
/// The caller passes text with code spans already stripped, so a skill name
/// mentioned inside backticks does not count either.
fn prompt_mentions_skill(text_lower: &str, pattern: &str) -> bool {
    let p = pattern.to_lowercase();
    if p.is_empty() {
        return false;
    }
    text_lower.split_whitespace().any(|word| {
        let word =
            word.trim_matches(|c: char| !c.is_alphanumeric() && !matches!(c, '-' | '_' | '/'));
        let bare = word.strip_prefix('/').unwrap_or(word);
        !bare.contains('/') && bare == p
    })
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
/// the agent name when none is declared. Account seats of one agent share it,
/// and lifecycle-hook events report it so a host can tell which company a
/// worker runs on.
pub fn agent_lineage(cfg: &Config, agent: &str) -> String {
    cfg.agents
        .iter()
        .find(|a| a.name == agent)
        .and_then(|a| a.lineage.clone())
        .unwrap_or_else(|| agent.to_string())
}

/// Tell `delegation.lifecycle_hook` that a session's pin moved to a new
/// provider session (failover, switch, escalation, demotion, ...), so a host
/// that bound work to the old provider session id can follow it. Awaited so
/// the host has rebound before the new session's first turn; a failed
/// delivery stays in the outbox.
async fn report_repin(
    shared: &Arc<Shared>,
    router_sid: &str,
    previous: &CandidateId,
    previous_downstream_sid: &str,
    candidate: &CandidateId,
    downstream_sid: &str,
    reason: &str,
) {
    if shared.cfg.delegation.lifecycle_hook.is_none() || previous_downstream_sid == downstream_sid {
        return;
    }
    let (router_pid, router_started_at_ms) = crate::delegate_hook::process_identity();
    let event = crate::delegate_hook::RepinEvent {
        event: "parent_repinned",
        parent_router_session_id: router_sid.to_string(),
        previous_downstream_session_id: previous_downstream_sid.to_string(),
        previous_candidate: previous.to_string(),
        downstream_session_id: downstream_sid.to_string(),
        candidate: candidate.to_string(),
        lineage: agent_lineage(&shared.runtime_config(), &candidate.agent),
        reason: reason.to_string(),
        router_pid,
        router_started_at_ms,
    };
    crate::delegate_hook::deliver_now(shared, &event).await;
}

/// Enter Planning. On first entry, inject the built-in plan-first protocol
/// and any host `planning_instructions`. Idempotent once the session is
/// already in Planning so follow-up turns do not re-stack the injects.
fn enter_planning_phase(shared: &Arc<Shared>, router_sid: &str) {
    use crate::config::PlannerPhase;
    let already = shared
        .with_session(router_sid, |s| s.planner_phase)
        .flatten()
        == Some(PlannerPhase::Planning);
    if already {
        return;
    }
    let protocol = crate::strategies::planner::planner_plan_protocol();
    let host = shared.cfg.routers.planner.planning_instructions.clone();
    shared.with_session(router_sid, |s| {
        s.planner_phase = Some(PlannerPhase::Planning);
        s.pending_injects.push(protocol);
        if !host.is_empty() {
            s.pending_injects.push(host);
        }
    });
}

/// Determine the planner phase for this turn and apply a monotonic upgrade
/// to the session when warranted. Called from `dispatch_prompt` right
/// alongside skill routing.
///
/// Sources (checked in order):
///
/// 1. Skill signal — `SkillRoute.marks_implementation_phase` on a matched
///    route definitively upgrades.
/// 2. Pre-classifier `planner_phase` dimension (first turn only).
///    `implementation` + `plan_ready=false` stays in Planning and injects
///    the plan-first protocol — never a "current plan" approval question.
/// 3. Heuristic keyword phrases in the prompt text.
///
/// Every path that remains in (or first enters) Planning goes through
/// [`enter_planning_phase`] so the built-in protocol and host
/// `planning_instructions` always land together.
///
/// The session's `planner_phase` is monotonic: once `Implementation`, it
/// stays there. Returns `true` when a mid-session switch to a new
/// implementation-phase model should be queued.
fn maybe_update_planner_phase(
    shared: &Arc<Shared>,
    router_sid: &str,
    prompt: &[ContentBlock],
    skill_route: Option<&crate::config::SkillRoute>,
    preclass: Option<&crate::pre_classifier::PreClassResult>,
) -> bool {
    use crate::config::{PlannerPhase, StrategyKind};

    let (strategy, current) =
        match shared.with_session(router_sid, |s| (s.strategy, s.planner_phase)) {
            Some(v) => v,
            None => return false,
        };
    if strategy != StrategyKind::Planner {
        return false;
    }
    // A coordinator never leaves Planning: skill, pre-class, and heuristic
    // upgrades are all ignored.
    if shared
        .with_session(router_sid, |s| s.coordinator)
        .unwrap_or(false)
    {
        enter_planning_phase(shared, router_sid);
        return false;
    }
    // Already implementation — monotonic, nothing to do.
    if current == Some(PlannerPhase::Implementation) {
        return false;
    }

    // 1. Skill signal (definitive).
    if let Some(route) = skill_route
        && route.marks_implementation_phase
    {
        let was_none = current.is_none();
        shared.with_session(router_sid, |s| {
            s.planner_phase = Some(PlannerPhase::Implementation);
        });
        notify_user(
            shared,
            router_sid,
            format!(
                "router-acp · planner: skill `{}` → implementation phase",
                route.pattern
            ),
        );
        // If the session already has a pin, we need a mid-session switch.
        return !was_none;
    }

    // 2. Pre-classifier decision (first turn, when available).
    if let Some(pre) = preclass
        && let Some(ref dec) = pre.planner_phase
    {
        let cfg = &shared.cfg.routers.planner;
        if dec.phase == PlannerPhase::Implementation
            && dec.confidence >= cfg.phase_upgrade_confidence
            && dec.plan_ready
        {
            shared.with_session(router_sid, |s| {
                s.planner_phase = Some(PlannerPhase::Implementation);
            });
            notify_user(
                shared,
                router_sid,
                format!(
                    "router-acp · planner: pre-class → implementation phase \
                         (confidence={:.2}, plan_ready=true). {}",
                    dec.confidence, dec.reason
                ),
            );
            return current.is_some(); // need switch if already pinned
        }
        if dec.phase == PlannerPhase::Implementation && !dec.plan_ready {
            // Classified as implementation work, but no reviewable plan exists
            // yet. Stay in planning and inject the plan-first protocol — do
            // not ask whether to proceed with a plan that has not been
            // presented.
            enter_planning_phase(shared, router_sid);
            notify_user(
                shared,
                router_sid,
                "router-acp · planner: staying in planning — present a plan before handoff",
            );
            return false;
        }
        // Pre-class says planning — enter it (protocol + host instructions).
        enter_planning_phase(shared, router_sid);
        return false;
    }

    // 3. Heuristic phrase match.
    let text = prompt_display_text(prompt);
    if crate::strategies::planner::heuristic_signals_implementation(&text) {
        shared.with_session(router_sid, |s| {
            s.planner_phase = Some(PlannerPhase::Implementation);
        });
        notify_user(
            shared,
            router_sid,
            "router-acp · planner: heuristic → implementation phase",
        );
        return current.is_some(); // need switch if already pinned
    }

    // Default: stay in / enter planning.
    enter_planning_phase(shared, router_sid);
    false
}

/// Pick the best candidate for a planner phase via the planner strategy.
/// Used for mid-session switches when the phase upgrades post-pin.
fn select_planner_target(
    shared: &Arc<Shared>,
    phase: crate::config::PlannerPhase,
    class: crate::candidate::TaskClass,
    excluded: &[String],
    planner_difficulty: Option<crate::config::PlannerDifficulty>,
) -> Option<CandidateId> {
    let profile = crate::classifier::TaskProfile {
        class,
        complexity: 0.5, // neutral default
        languages: vec![],
        effort: None,
    };
    let ctx = RouteContext {
        profile,
        required_caps: RequiredCaps::default(),
        explicit_candidate: None,
        explicit_source: None,
        planner_phase: Some(phase),
        planner_difficulty,
    };
    let mut pool = shared.eligible_views(&RequiredCaps::default(), class);
    if !excluded.is_empty() {
        pool.retain(|v| !view_excluded(v, excluded));
    }
    let strategy = crate::strategies::make_strategy(StrategyKind::Planner, &shared.cfg);
    strategy
        .rank(&ctx, &pool)
        .ok()?
        .into_iter()
        .next()
        .map(|r| r.candidate)
}

/// Estimate a session's confidence in [0, 1]: how fully the pinned model's
/// benchmark quality meets classified task demand, minus accumulated struggle.
fn session_confidence(shared: &Arc<Shared>, router_sid: &str) -> f64 {
    shared
        .with_session(router_sid, |s| {
            let class = s.task_class.unwrap_or(TaskClass::CodingGeneral);
            (crate::candidate::quality_confidence(s.pinned_quality, class, s.task_complexity)
                - s.struggle)
                .clamp(0.0, 1.0)
        })
        .unwrap_or(1.0)
}

/// The best eligible candidate strictly more capable (higher quality for the
/// session's task class) than the current pin — the auto-upgrade target.
///
/// The +0.05 normalized-quality margin filters out noise-level "upgrades"
/// that would forfeit a warm cache for no real capability gain. But the
/// score table's compressed peer pairs (`benchmark_scoring.compression` in
/// data/model-policy.yaml) sit a deliberate ~0.007 normalized above their
/// cheaper sibling — Fable over Opus, Sol over Terra — and a session pinned
/// on the cheaper peer whose confidence keeps dropping (a fix surviving
/// round after round) must still be able to climb onto the preferred member.
/// So when nothing clears the margin, fall back to the best strictly-better
/// candidate instead of returning nothing.
fn upgrade_target(shared: &Arc<Shared>, router_sid: &str) -> Option<CandidateId> {
    ranked_upgrade_target(shared, router_sid).filter(|t| !shared.coordinator_blocks(router_sid, t))
}

fn ranked_upgrade_target(shared: &Arc<Shared>, router_sid: &str) -> Option<CandidateId> {
    let (class, current, current_q, excluded, elevation_skill) =
        shared.with_session(router_sid, |s| {
            (
                s.task_class.unwrap_or(TaskClass::CodingGeneral),
                s.pin.as_ref().map(|p| p.candidate.clone()),
                s.pinned_quality,
                s.excluded.clone(),
                s.elevation_skill.clone(),
            )
        })?;
    let current = current?;
    let mut pool = shared.eligible_views(&RequiredCaps::default(), class);
    pool.retain(|v| v.id != current && !view_excluded(v, &excluded));
    // A skill pin's switch-target contract is `candidates`, not the global
    // quality ladder. Without this, auto-upgrade from a grok ship-pr pin
    // crowned Sol (highest score, `also_acceptable` only) the moment
    // confidence dipped.
    if let Some(pattern) = elevation_skill.as_deref()
        && let Some(route) = shared
            .cfg
            .skill_routing
            .iter()
            .find(|r| r.pattern == pattern)
    {
        pool.retain(|v| route.candidates.iter().any(|rc| view_matches(rc, v)));
    }
    let best_above = |margin: f64| {
        pool.iter()
            .filter(|v| {
                crate::candidate::quality_utility(v.quality)
                    > crate::candidate::quality_utility(current_q) + margin
            })
            .max_by(|a, b| {
                a.quality
                    .partial_cmp(&b.quality)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|v| v.id.clone())
    };
    best_above(0.05).or_else(|| best_above(f64::EPSILON))
}

/// The strongest eligible candidate strictly cheaper than the current pin —
/// the demotion target. Guarded so the landing spot won't immediately
/// re-trigger an auto-upgrade: when auto-upgrade is enabled, the target's
/// quality (minus current struggle) must clear the upgrade threshold.
///
/// When the expiring elevation is a skill route (`elevation_skill`), the
/// pool is further restricted to that route's own `candidates` — otherwise
/// demotion picks by raw cost/quality alone,
/// which can land OUTSIDE the skill's approved pool (e.g. `ship-pr` pins
/// `grok` but demotion, unaware of that contract, switched a live ship to
/// `codex/gpt-5.6-terra` because it scored higher than grok at lower cost).
fn demotion_target(shared: &Arc<Shared>, router_sid: &str) -> Option<CandidateId> {
    ranked_demotion_target(shared, router_sid).filter(|t| !shared.coordinator_blocks(router_sid, t))
}

fn ranked_demotion_target(shared: &Arc<Shared>, router_sid: &str) -> Option<CandidateId> {
    let (class, complexity, current, excluded, struggle, strategy, elevation_skill) = shared
        .with_session(router_sid, |s| {
            (
                s.task_class.unwrap_or(TaskClass::CodingGeneral),
                s.task_complexity,
                s.pin.as_ref().map(|p| p.candidate.clone()),
                s.excluded.clone(),
                s.struggle,
                s.strategy,
                s.elevation_skill.clone(),
            )
        })?;
    let current = current?;
    let current_cost = shared.candidate_runtime(&current).map(|c| c.cost_rank)?;
    let mut pool = shared.eligible_views(&RequiredCaps::default(), class);
    pool.retain(|v| v.id != current && !view_excluded(v, &excluded) && v.cost_rank < current_cost);
    if let Some(pattern) = elevation_skill.as_deref()
        && let Some(route) = shared
            .cfg
            .skill_routing
            .iter()
            .find(|r| r.pattern == pattern)
    {
        pool.retain(|v| route.candidates.iter().any(|rc| view_matches(rc, v)));
    }
    // Escalation sessions re-escalate only on fresh struggle signals, so a
    // quiet-turn demotion can't ping-pong there; the threshold guard applies
    // only where the confidence-based auto-upgrade could immediately undo
    // the demotion.
    if shared.cfg.auto_upgrade.enabled && strategy != StrategyKind::Escalation {
        let threshold = shared.cfg.auto_upgrade.confidence_threshold;
        pool.retain(|v| {
            crate::candidate::quality_confidence(v.quality, class, complexity) - struggle
                >= threshold
        });
    }
    pool.into_iter()
        .max_by(|a, b| {
            a.quality
                .partial_cmp(&b.quality)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|v| v.id)
}

/// Expire an elevated pin (escalation / auto-upgrade / skill pin) after the
/// configured run of quiet turns: queue a switch down to the strongest
/// cheaper candidate — the counterpart of escalation, so one hard patch (or
/// one ship-skill turn) doesn't pin a long session to frontier pricing
/// forever. Explicit user picks never set `elevation`, so they are never
/// demoted; re-escalation on fresh difficulty still applies (bounded by
/// `max_escalations`).
fn maybe_demote(shared: &Arc<Shared>, router_sid: &str) {
    let quiet_needed = shared.cfg.demotion.after_quiet_turns;
    if quiet_needed == 0 {
        return;
    }
    let ready = shared
        .with_session(router_sid, |s| {
            s.pin.is_some()
                && s.pending_switch.is_none()
                && s.escalation_requested.is_none()
                && s.elevation.is_some()
                && s.quiet_turns >= quiet_needed
        })
        .unwrap_or(false);
    if !ready {
        return;
    }
    let Some(target) = demotion_target(shared, router_sid) else {
        return;
    };
    let cause = shared
        .with_session(router_sid, |s| s.elevation.clone())
        .flatten()
        .unwrap_or_else(|| "elevation".to_string());
    let reason = format!("demotion: {quiet_needed} quiet turns since {cause} — verdict expired");
    shared.with_session(router_sid, |s| {
        s.pending_switch = Some(SwitchRequest {
            target: target.clone(),
            reason: reason.clone(),
            handoff: HandoffStyle::Full,
            user_pick: false,
        });
        s.elevation = None;
        s.elevation_skill = None;
        s.quiet_turns = 0;
    });
    notify_user(
        shared,
        router_sid,
        format!("router-acp · demoting to {target} for the next turn — {reason}"),
    );
    tracing::info!(session = router_sid, %target, reason, "demotion queued (verdict expired)");
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
    let struggled = delta > 0.0 || tool_failures > 0;
    shared.with_session(router_sid, |s| {
        if delta > 0.0 {
            s.struggle = (s.struggle + delta).min(1.0);
        }
        // Quiet-turn bookkeeping for demotion: an elevated pin expires after
        // enough turns without struggle signals. Struggle itself decays on
        // quiet turns, so one hard patch stops holding confidence down (and
        // re-triggering upgrades) long after the work turned routine.
        if s.elevation.is_some() {
            if struggled {
                s.quiet_turns = 0;
            } else {
                s.quiet_turns += 1;
                s.struggle = (s.struggle - 0.05).max(0.0);
            }
        }
    });
    maybe_demote(shared, router_sid);

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
                handoff: HandoffStyle::Full,
                user_pick: false,
            });
            s.elevation = Some("auto-upgrade".to_string());
            s.elevation_skill = None;
            s.quiet_turns = 0;
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
            handoff: HandoffStyle::Full,
            user_pick: false,
        });
        s.escalations_done += 1;
        s.elevation = Some("escalation".to_string());
        s.elevation_skill = None;
        s.quiet_turns = 0;
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

/// The terse counterpart, used by a `terse_handoff` skill route. The incoming
/// model is about to run a named workflow that re-derives its own state, so a
/// narrative summary is not just unnecessary — it is a liability: a long
/// session may mention several identifiers and abandoned approaches, and the
/// briefing is where the wrong one gets picked up. Ask for one referent, not a
/// story, and make "unknown" an explicitly allowed answer so the outgoing
/// model does not fill the slot with a guess.
const HANDOFF_TERSE_INSTRUCTION: &str = "You are about to hand this conversation off to a different model, \
     which will CONTINUE this work by running a named skill/workflow from the \
     start. Do not summarize the conversation. Write at most three short \
     lines:\n\
     1. TASK: the work to be done, in one line, naming the skill or workflow \
     if one is in play.\n\
     2. SUBJECT: the single concrete identifier it operates on — write \
     `unknown` if none is established. Never guess, \
     and never list more than one; if several came up, name only the one this \
     work is on.\n\
     3. NOT RE-DERIVABLE: any decision already made that the new model could \
     NOT rediscover from authoritative host state — e.g. a review finding \
     judged a false positive, or a flaky test agreed to be re-run. Write `none` if there is \
     nothing.\n\
     Omit files changed, commands run, and findings — the new model re-derives \
     those. Do not continue the task.";

/// The command the incoming model can run to read the full prior transcript.
///
/// Resolved at switch time from this process's own binary path and the live
/// state-file path, so the pointer is runnable as printed — no config path to
/// discover, no `sqlite3` binary required (dev boxes do not ship one), and no
/// assumption that `router-acp` is on the downstream agent's `PATH`.
fn transcript_command(shared: &Arc<Shared>, router_sid: &str) -> String {
    let exe = std::env::current_exe()
        .ok()
        .and_then(|p| p.to_str().map(str::to_string))
        .unwrap_or_else(|| "router-acp".to_string());
    format!(
        "{exe} transcript --state {} --session {router_sid}",
        shared.cfg.state_file.display()
    )
}

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
/// summarize (offline, token-limited, refused, or crashed). Lossy but
/// self-contained: it needs nothing from the old model. Returns `""` when
/// there is nothing to carry over. Prefers the most recent turns when over
/// budget.
///
/// `PER_TURN_CHARS` is this reader's own cap, deliberately far below the
/// full stored conversation: the output is injected into the incoming
/// model's context, so BREADTH of turns beats depth of any one turn. Without
/// it, raising the storage cap would silently shrink this transcript to one or
/// two full-length turns against the same `MAX_CHARS` budget. The full,
/// uncapped text stays available out-of-band via the `transcript` subcommand.
fn transcript_from_logs(shared: &Arc<Shared>, router_sid: &str) -> String {
    const MAX_TURNS: usize = 40;
    const MAX_CHARS: usize = 12_000;
    const PER_TURN_CHARS: usize = 500;
    let entries = shared.state.lock().unwrap().log_for(router_sid, 500);
    let turns: Vec<String> = entries
        .iter()
        .filter(|e| {
            matches!(
                e.kind.as_str(),
                "user_prompt" | "agent_response" | "tool_call"
            ) || e.kind.starts_with("fs_")
                || e.kind.starts_with("terminal_")
                || e.kind == "session_request_permission"
        })
        .filter_map(|e| {
            let who = match e.kind.as_str() {
                "user_prompt" => "User",
                "agent_response" => "Assistant",
                _ => "Tool",
            };
            let text = if e.kind == "tool_call" {
                format!(
                    "{} {}",
                    e.summary,
                    e.detail
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                )
            } else if who == "Tool" {
                format!("{} [requested; completion unknown]", e.summary)
            } else {
                clean_turn_text(&e.summary)
            };
            (!text.is_empty()).then(|| {
                let mut clipped: String = text.chars().take(PER_TURN_CHARS).collect();
                if clipped.chars().count() < text.chars().count() {
                    clipped.push('…');
                }
                format!("{who}: {clipped}")
            })
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
fn frame_transcript(from: &CandidateId, transcript: &str, transcript_cmd: &str) -> String {
    format!(
        "[Handoff context — the previous model ({from}) was unavailable to summarize, so this is \
         a truncated transcript of the prior session reconstructed from router-acp's logs (each \
         turn is capped, so detail may be lost). Treat it as established context for continuing \
         the work. The FULL log, including tool calls, is available by running:\n\
         \x20   {transcript_cmd}\n\
         ]\n\n{transcript}\n\n\
         [End of handoff context. The user's message follows.]"
    )
}

/// Frame a terse briefing as a context block.
///
/// Deliberately unlike `frame_summary`: it tells the incoming model that the
/// briefing is INCOMPLETE by design, that concrete state must be re-derived
/// from the repository rather than inferred from the words above, and where to
/// get the full record if it needs one. The "verify before you act on an
/// identifier" line is the load-bearing part — the failure mode this whole
/// path exists to avoid is a confident model acting on the wrong PR.
fn frame_terse(from: &CandidateId, briefing: &str, transcript_cmd: &str) -> String {
    format!(
        "[Handoff context — you are picking up work in progress from {from}. This is a \
         deliberately TERSE briefing, not a summary: it carries the task, its subject, and \
         anything not re-derivable from authoritative host state, and nothing else.\n\
         Re-derive concrete state from the host context and available tools rather than assuming \
         it. Verify any identifier below before you act on it, and if it reads `unknown`, resolve \
         it from authoritative host state — do not guess.\n\
         The full prior transcript, including tool calls, is available if you need detail this \
         briefing omits:\n\
         \x20   {transcript_cmd}\n\
         ]\n\n{briefing}\n\n\
         [End of handoff context. The user's message follows.]"
    )
}

/// Switch a pinned session to `target` mid-conversation: ask the current
/// model to brief its successor, open a fresh downstream session on the
/// target, seed it with that briefing (prepended to the next prompt), re-pin,
/// and close the old session. Context does not transfer via ACP, so the
/// briefing IS the handoff. `style` chooses how much of it to carry — see
/// `HandoffStyle`. Returns the switch disclosure lines.
/// Name the candidate's actual status instead of the generic
/// "not verified, quarantined, or lacking a required capability" + re-auth
/// hint. Auth failures are handled before rank; this is the Invalid / Down path.
fn static_unrouteable_message(
    shared: &Shared,
    err: &str,
    override_: Option<&CandidateId>,
) -> String {
    let chosen = override_.cloned().or_else(|| {
        shared
            .cfg
            .routers
            .static_
            .candidate
            .as_deref()
            .and_then(CandidateId::parse)
    });
    let Some(id) = chosen else {
        return err.to_string();
    };
    match shared.candidate_status(&id) {
        Some(CandidateStatus::Invalid(reason)) => {
            format!("static candidate `{id}` is not routeable ({reason})")
        }
        Some(CandidateStatus::Down(reason)) => {
            format!("static candidate `{id}` is not routeable (downstream down: {reason})")
        }
        Some(CandidateStatus::AuthPending) => {
            format!("static candidate `{id}` is not routeable (authentication pending)")
        }
        Some(CandidateStatus::Unverified) => {
            format!("static candidate `{id}` is not routeable (not yet verified)")
        }
        Some(CandidateStatus::Routeable) | None => err.to_string(),
    }
}

/// A coordinator only leaves `planning_candidates` on a human pick. Anything
/// else (planner upgrade, skill route, escalation, demotion) is
/// refused and disclosed; the session stays on its current model.
fn refuse_coordinator_switch(shared: &Arc<Shared>, router_sid: &str, sw: &SwitchRequest) -> bool {
    if sw.user_pick || !shared.coordinator_blocks(router_sid, &sw.target) {
        return false;
    }
    notify_user(
        shared,
        router_sid,
        format!(
            "router-acp · coordinator: refused switch to {} ({}) — coordinator sessions stay \
             on planning_candidates unless a human picks the model",
            sw.target, sw.reason
        ),
    );
    true
}

async fn switch_pin(
    shared: &Arc<Shared>,
    router_sid: &str,
    target: &CandidateId,
    reason: &str,
    style: HandoffStyle,
    user_pick: bool,
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
    if target == &old_candidate && shared.route_for(&old_process_key, &old_down_sid).is_some() {
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

    // A target whose process is missing (died, or never spawned because the
    // boot auth probe said logged out) is started here, before the summary:
    // the routing pool drops such targets, so without this a switch onto one
    // could never succeed and the session stayed on the model it was leaving.
    if let Some(key) = shared.candidate_runtime(target).map(|r| r.process_key) {
        ensure_target_ready(shared, target, &key).await?;
    }

    // 1. Build the handoff. Preferred path: ask the outgoing model to
    //    summarize (capturing its text instead of relaying it). If that model
    //    is offline/rate-limited/crashed, or refuses, or produces nothing,
    //    fall back to a transcript reconstructed from the state-DB logs — which
    //    needs nothing from the old model. A model that cannot serve right now
    //    (cordoned, quarantined, exhausted, dead) is not asked at all.
    let class = shared
        .with_session(router_sid, |session| session.task_class)
        .flatten()
        .unwrap_or(TaskClass::CodingGeneral);
    let live_conn = shared
        .candidate_view(&old_candidate, &RequiredCaps::default(), class)
        .and_then(|_| shared.route_for(&old_process_key, &old_down_sid))
        .and_then(|_| shared.target_conn(&old_process_key));
    let summary = if let Some(conn) = &live_conn {
        let buffer = Arc::new(Mutex::new(String::new()));
        shared.with_session(router_sid, |s| s.capturing_summary = Some(buffer.clone()));
        let instruction = match style {
            HandoffStyle::Full => HANDOFF_SUMMARY_INSTRUCTION,
            HandoffStyle::Terse => HANDOFF_TERSE_INSTRUCTION,
        };
        let summary_prompt = PromptRequest::new(
            old_down_sid.clone(),
            vec![ContentBlock::from(instruction.to_string())],
        );
        let _llm_turn = shared.llm_proxy.begin_turn(
            old_process_key.clone(),
            router_sid.to_string(),
            router_sid.to_string(),
            old_down_sid.clone(),
            old_candidate.clone(),
            class,
            None,
        );
        let result = conn.send_request(summary_prompt).block_task().await;
        shared.with_session(router_sid, |s| s.capturing_summary = None);
        // A refused summary is still a real failure of the outgoing model
        // (a spend cap, a usage limit): record it so the model is cordoned.
        if let Err(err) = &result {
            let failure = crate::limits::classify_failure(err);
            apply_failure(shared, &old_candidate, err, &failure);
        }
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
            "outgoing model cannot serve; using log-transcript handoff"
        );
        None
    };

    // Framed handoff block + a kind tag for the disclosure.
    let transcript_cmd = transcript_command(shared, router_sid);
    let (handoff, handoff_note): (Option<String>, &str) = match (summary, style) {
        (Some(s), HandoffStyle::Terse) => (
            Some(frame_terse(&old_candidate, &s, &transcript_cmd)),
            "briefed by the previous model (terse; state re-derived from the repo)",
        ),
        (Some(s), HandoffStyle::Full) => (
            Some(frame_summary(&old_candidate, &s)),
            "summarized by the previous model",
        ),
        // A failed briefing degrades to the log transcript regardless of
        // style: a terse route still needs the new model to know what it is
        // picking up, and the transcript needs nothing from the dead model.
        (None, _) => {
            let transcript = transcript_from_logs(shared, router_sid);
            if transcript.trim().is_empty() {
                (None, "no prior context was available to carry over")
            } else {
                (
                    Some(frame_transcript(
                        &old_candidate,
                        &transcript,
                        &transcript_cmd,
                    )),
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
    let (mcp_servers, delegate_attached) =
        mcp_servers_for_pin(shared, router_sid, target, &client_mcp)?;
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
        .with_session(router_sid, |s| s.task_class)
        .flatten()
        .map(|class| shared.scores_for(router_sid, target).quality(class))
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
        s.pending_delegation_directive = None;
        s.delegation_directive_active = false;
        if delegate_attached && shared.cfg.delegation.inject_prompt {
            s.pending_delegation_directive = Some(target.clone());
        }
    });
    report_repin(
        shared,
        router_sid,
        &old_candidate,
        &old_down_sid,
        target,
        &opened.downstream_sid,
        reason,
    )
    .await;

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

    shared.with_session(router_sid, |s| s.pin_user_pick = user_pick);

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
                "user_pick": user_pick,
            })),
            ..Default::default()
        },
    );
    crate::restoration::checkpoint(shared, router_sid)?;
    // A restarted adapter may reuse its session ids from the beginning. In
    // that case the replacement route has the same process key and session id
    // as the stale pin. Closing the stale tuple would unregister the fresh
    // route we just installed.
    if old_process_key != opened.process_key || old_down_sid != opened.downstream_sid {
        close_downstream_session(shared, &old_process_key, &old_down_sid);
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
                let _llm_turn = shared.llm_proxy.begin_turn(
                    opened.process_key.clone(),
                    router_sid.clone(),
                    router_sid.clone(),
                    opened.downstream_sid.clone(),
                    view.id.clone(),
                    TaskClass::Writing,
                    req.meta.as_ref(),
                );
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
        None,
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

    let mut session_caps = SessionCapabilities::new()
        .list(Some(Default::default()))
        .delete(Some(Default::default()))
        .resume(Some(Default::default()))
        .close(Some(Default::default()));
    if any_dirs {
        session_caps = session_caps.additional_directories(Some(Default::default()));
    }

    let capabilities = AgentCapabilities::new()
        .load_session(true)
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
    // Bind before downstream adapters spawn so their base-URL environment can
    // point at the actual ephemeral port. Failure is transparent: no env is
    // changed and ACP-turn routing continues.
    let llm_proxy_task = if shared.llm_proxy.enabled() {
        match shared.llm_proxy.bind(shared.clone()).await {
            Ok(task) => Some(task),
            Err(err) => {
                tracing::warn!(%err, "per-request LLM proxy disabled; using adapter upstreams");
                None
            }
        }
    } else {
        None
    };

    // Router-owned MCP listener (Unix socket) runs independently of the ACP
    // connection; it serves delegation when configured and managed-background
    // terminals when the upstream client advertises them. Bind eagerly because
    // client capabilities arrive later during initialize.
    let listener_task = match crate::delegate_mcp::bind_listener(&shared) {
        Ok(task) => Some(task),
        Err(err) => {
            tracing::warn!(%err, "router-owned MCP tools disabled: cannot bind socket");
            None
        }
    };

    // Proactive usage-cap cordoning: poll each usage-source agent's provider
    // usage API on an interval and cordon exhausted candidates.
    let usage_task = crate::usage::spawn_usage_poller(&shared);

    // Lifecycle-hook events carry this process's identity; fix it now so the
    // reported start time is the router's own. Then redeliver anything a
    // previous router left in the outbox.
    crate::delegate_hook::process_identity();
    let outbox_task =
        crate::delegate_hook::spawn_outbox_flusher(&shared, std::time::Duration::from_secs(30));

    let result = build_agent(shared.clone()).connect_to(transport).await;
    crate::accounts::cancel_all(&shared);

    if let Some(task) = listener_task {
        task.abort();
    }
    if let Some(task) = usage_task {
        task.abort();
    }
    if let Some(task) = outbox_task {
        task.abort();
    }
    if let Some(task) = llm_proxy_task {
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
            move |req: NewSessionRequest, responder: Responder<NewSessionResponse>, cx| {
                let shared = s_new.clone();
                async move { on_session_new(shared, req, responder, cx) }
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
                                    let task_shared = shared.clone();
                                    let task_conn = conn.clone();
                                    conn.spawn(async move {
                                        match task_conn.send_request(fwd).block_task().await {
                                            Ok(resp) => {
                                                task_shared.with_session(&sid, |s| s.applied_mode = Some(requested));
                                                match crate::restoration::checkpoint(&task_shared, &sid) {
                                                    Ok(()) => { let _ = responder.respond(resp); }
                                                    Err(err) => { let _ = responder.respond_with_error(err); }
                                                }
                                            }
                                            Err(err) => { let _ = responder.respond_with_error(err); }
                                        }
                                        Ok(())
                                    })
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
                                if let Err(err) = crate::restoration::checkpoint(&shared, &sid) {
                                    return responder.respond_with_error(err);
                                }
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
        crate::auth::refresh_before_selection(&task_shared).await;
        let keys = task_shared.target_keys();
        // Spawn every downstream process, then probe them concurrently.
        for key in &keys {
            if task_shared.target_spec(key).is_some_and(|s| {
                task_shared
                    .agent_configs()
                    .iter()
                    .any(|a| a.name == s.agent_name && a.account_disabled)
            }) {
                continue;
            }
            if task_shared
                .target_spec(key)
                .and_then(|spec| task_shared.auth_rejection(&spec.agent_name))
                .is_some()
            {
                continue;
            }
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
        if routeable.is_empty()
            && !auth_pending
            && !task_shared
                .agent_configs()
                .iter()
                .any(|a| crate::accounts::provider(a).is_some())
        {
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
    if shared
        .agent_configs()
        .iter()
        .any(|a| a.name == agent && crate::accounts::provider(a).is_some())
    {
        return responder.respond_with_error(AcpError::invalid_params().data(
            "Use /login to authenticate this account through router-acp's credential manager.",
        ));
    }
    let keys = shared.target_keys_for_agent(agent);
    if keys.is_empty() {
        return responder.respond_with_error(
            AcpError::invalid_params().data(format!("unknown agent `{agent}` in auth method id")),
        );
    }
    let agent = agent.to_string();
    let downstream_method = downstream_method.to_string();
    let meta = req.meta.clone();
    cx.spawn(async move {
        let mut succeeded = false;
        let mut last_err: Option<AcpError> = None;
        for key in &keys {
            // Startup skips spawning targets whose agent is known logged out —
            // which is exactly the agent someone calls `authenticate` for. Bring
            // the process up on demand so signing in is still possible.
            if shared.target_conn(key).is_none()
                && let Err(err) = start_downstream(&shared, key).await
            {
                tracing::warn!(target = %key, error = %err, "cannot start target to authenticate");
                last_err = Some(err);
            }
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
        // A successful sign-in is the most authoritative positive evidence
        // there is; clear any prior rejection before re-probing so the agent's
        // candidates can become routeable again.
        crate::auth::note_authenticated(&shared.auth, &agent);
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
    cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    if !shared.is_initialized() {
        return responder
            .respond_with_error(AcpError::invalid_request().data("initialize the router first"));
    }
    cx.spawn(async move {
        crate::auth::refresh_before_selection(&shared).await;
        // Management commands must remain accessible when every login is
        // expired. Normal prompts still enforce eligibility at pin time.
        let router_sid = format!("rtr-{}", uuid::Uuid::new_v4());
        let created = (|| -> Result<(), AcpError> {
            let mut session = RouterSession::new(&shared.cfg, &req);
            let source = req
                .meta
                .as_ref()
                .and_then(|m| m.get("router_acp"))
                .and_then(|m| m.get("continue_from"));
            if let Some(source) = source {
                let source = source.as_str().ok_or_else(|| {
                    AcpError::invalid_params().data("continue_from must be a router session id")
                })?;
                let saved = shared.state.lock().unwrap().get(source).ok_or_else(|| {
                    AcpError::invalid_params().data(format!("unknown router session id `{source}`"))
                })?;
                crate::restoration::restore_config(&mut session, &saved)?;
                session.coordinator |= meta_marks_coordinator(req.meta.as_ref());
                session.pending_history = crate::restoration::history(&shared, source)?;
            }
            let config = serde_json::to_value(crate::restoration::SessionConfig::capture(&session))
                .map_err(|e| AcpError::internal_error().data(e.to_string()))?;
            let state = shared.state.lock().unwrap();
            state
                .upsert_checked(
                    router_sid.clone(),
                    PersistedSession {
                        cwd: req.cwd.clone(),
                        additional_directories: req.additional_directories.clone(),
                        session_config: Some(config),
                        ..Default::default()
                    },
                )
                .map_err(|e| {
                    AcpError::internal_error().data(format!("cannot save new router session: {e}"))
                })?;
            if !session.pending_history.is_empty()
                && let Err(e) = state.log_checked(
                    &router_sid,
                    &crate::state::LogEntry {
                        kind: "inherited_context".into(),
                        role: "router".into(),
                        detail: Some(json!({"prompt": session.pending_history})),
                        ..Default::default()
                    },
                )
            {
                state.remove(&router_sid);
                return Err(AcpError::internal_error()
                    .data(format!("cannot save continued conversation: {e}")));
            }
            drop(state);
            shared
                .sessions
                .lock()
                .unwrap()
                .insert(router_sid.clone(), session);
            Ok(())
        })();
        if let Err(err) = created {
            return responder.respond_with_error(err);
        }
        let options = shared.router_config_options(&router_sid);
        let _ = responder.respond(
            NewSessionResponse::new(router_sid.clone())
                .config_options(options)
                .meta(crate::restoration::response_meta(&shared, &router_sid)),
        );
        crate::accounts::advertise(&shared, &router_sid);
        Ok(())
    })
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
                    let value = match &req.value {
                        SessionConfigOptionValue::ValueId { value } => value.0.to_string(),
                        _ => String::new(),
                    };
                    if config_id == "router.effort" {
                        match EffortLevel::parse(&value) {
                            Some(EffortLevel::Auto) => {
                                session.effort_request = None;
                                refresh_pinned_effort(
                                    &shared.runtime_config(),
                                    &shared.scores,
                                    session,
                                );
                                Action::RouterUpdated
                            }
                            Some(level) => {
                                session.effort_request = Some(level);
                                refresh_pinned_effort(
                                    &shared.runtime_config(),
                                    &shared.scores,
                                    session,
                                );
                                Action::RouterUpdated
                            }
                            None => Action::BadValue(format!(
                                "unknown effort `{value}`; expected auto, low, medium, high, xhigh, or max"
                            )),
                        }
                    } else if session.pin.is_some() || session.pinning {
                        Action::AlreadyPinned
                    } else {
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
                                    session.candidate_override_source = None;
                                    Action::RouterUpdated
                                } else {
                                    match CandidateId::parse(&value) {
                                        Some(id) => {
                                            session.candidate_override = Some(id);
                                            session.candidate_override_source =
                                                Some(OverrideSource::UserPick);
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
                } else if config_id == "model" && session.pin.is_none() && !session.pinning {
                    // ACP clients set their configured model on the session
                    // before the first prompt (goose does this unconditionally,
                    // and errors the whole run if the set is refused). router-acp
                    // picks the model itself, so the `default`/`auto` placeholder
                    // carries no preference and is accepted as a no-op. A real
                    // candidate IS a preference and must not be silently dropped —
                    // the same invariant we enforce on downstream agents that
                    // no-op our own set_config_option.
                    let value = match &req.value {
                        SessionConfigOptionValue::ValueId { value } => value.0.to_string(),
                        _ => String::new(),
                    };
                    if value.eq_ignore_ascii_case("default") || value.eq_ignore_ascii_case("auto") {
                        Action::RouterUpdated
                    } else {
                        match CandidateId::parse(&value) {
                            Some(id) => {
                                session.candidate_override = Some(id);
                                session.candidate_override_source = Some(OverrideSource::UserPick);
                                Action::RouterUpdated
                            }
                            None => Action::BadValue(format!(
                                "`{value}` is not `default` or an `agent/model` candidate id"
                            )),
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
            if let Err(err) = crate::restoration::checkpoint(&shared, &router_sid) {
                return responder.respond_with_error(err);
            }
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
    if crate::accounts::intercepts(&shared, &router_sid, &req.prompt) {
        return cx.spawn(async move {
            crate::accounts::handle_prompt(shared, router_sid, req, responder).await
        });
    }

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

    if shared.with_session(&router_sid, |_| ()).is_none() {
        return responder.respond_with_error(AcpError::invalid_params().data("unknown session id"));
    }
    let prompt_text = prompt_display_text(&req.prompt);
    if let Err(err) = shared.state.lock().unwrap().log_checked(
        &router_sid,
        &crate::state::LogEntry {
            kind: "user_prompt".into(),
            role: "user".into(),
            summary: prompt_text.clone(),
            detail: Some(json!({"prompt": req.prompt})),
            tokens_input: crate::state::estimate_tokens(&prompt_text),
            tokens_estimated: true,
            ..Default::default()
        },
    ) {
        return responder.respond_with_error(
            AcpError::internal_error().data(format!("cannot save router conversation: {err}")),
        );
    }

    // Tracks whether the user steered routing explicitly (a `[router: …]`
    // directive or a `model:` shorthand). Either suppresses the pre-classifier
    // and planner-phase heuristics for this prompt.
    let mut explicit_routing = false;
    // Per-prompt `hard:` / `easy:` — reset so a prior prefix cannot stick.
    shared.with_session(&router_sid, |s| s.planner_difficulty = None);
    // Coordinator role is sticky: a tagged prompt sets it, an untagged one
    // never clears it. A coordinator found in Implementation (tagged late, or
    // upgraded before this guard existed) is pulled back to Planning.
    let (coordinator, planner_session) = shared
        .with_session(&router_sid, |s| {
            s.coordinator |= meta_marks_coordinator(req.meta.as_ref());
            (s.coordinator, s.strategy == StrategyKind::Planner)
        })
        .unwrap_or((false, false));
    if coordinator && planner_session {
        enter_planning_phase(&shared, &router_sid);
    }
    // An off-pool pin that no human chose (an automatic upgrade before the
    // role arrived, or a pre-guard session) goes back to the planning pool.
    // A directive or shorthand parsed below replaces this queued switch.
    if coordinator {
        let (pin, user_pick, class, excluded) = shared
            .with_session(&router_sid, |s| {
                (
                    s.pin.as_ref().map(|p| p.candidate.clone()),
                    s.pin_user_pick,
                    s.task_class.unwrap_or(TaskClass::CodingGeneral),
                    s.excluded.clone(),
                )
            })
            .unwrap_or((None, false, TaskClass::CodingGeneral, Vec::new()));
        if let Some(pin) = pin
            && !user_pick
            && !shared.in_planning_pool(&pin)
            && let Some(target) = select_planner_target(
                &shared,
                crate::config::PlannerPhase::Planning,
                class,
                &excluded,
                None,
            )
            .filter(|t| shared.in_planning_pool(t))
        {
            shared.with_session(&router_sid, |s| {
                s.pending_switch = Some(SwitchRequest {
                    target,
                    reason: format!("coordinator returns from {pin} to planning_candidates"),
                    handoff: HandoffStyle::Full,
                    user_pick: false,
                });
            });
        }
    }
    // Set when a `[router: phase=implementation]` directive upgrades a pinned
    // planning session — the same summarize-and-re-pin as a detected upgrade.
    let mut planner_needs_switch = false;

    // Routing directives: `[router: ...]` anywhere in the prompt. Always
    // stripped (the downstream model never sees them); only applied before
    // the pin.
    match parse_prompt_directives(&req.prompt) {
        Ok(None) => {
            // Model shorthand: a prompt beginning with `model:` (e.g. `opus:`,
            // `codex/gpt-5.5:`, `sonnet: fix this`) is a switch (post-pin) or a
            // pin steer (pre-pin) to the referenced candidate. Resolution gates
            // it — a token that doesn't name an eligible candidate is left as
            // ordinary prose. Reserved tokens: `hard:`/`easy:` subset the
            // planner pool (astra/fable vs opus/sol) without pinning a named
            // model.
            if let Some((ref_str, stripped)) = split_model_shorthand(&req.prompt) {
                let lower_ref = ref_str.to_lowercase();
                if let Some(diff) = match lower_ref.as_str() {
                    "hard" => Some(crate::config::PlannerDifficulty::Hard),
                    "easy" => Some(crate::config::PlannerDifficulty::Easy),
                    _ => None,
                } {
                    req =
                        PromptRequest::new(req.session_id.clone(), stripped).meta(req.meta.clone());
                    shared.with_session(&router_sid, |s| s.planner_difficulty = Some(diff));
                    let pool_label = match diff {
                        crate::config::PlannerDifficulty::Hard => "astra/fable",
                        crate::config::PlannerDifficulty::Easy => "opus/sol",
                    };
                    notify_user(
                        &shared,
                        &router_sid,
                        format!("router-acp · planner: `{lower_ref}:` prefix → {pool_label} pool"),
                    );
                    tracing::info!(
                        session = router_sid,
                        prefix = %lower_ref,
                        "planner difficulty prefix"
                    );
                    // Mid-session, still in planning: switch if the current
                    // pin is outside the requested pool.
                    let (pinned, phase, class, excluded, current) = shared
                        .with_session(&router_sid, |s| {
                            (
                                s.pin.is_some() || s.pinning,
                                s.planner_phase,
                                s.task_class.unwrap_or(TaskClass::CodingGeneral),
                                s.excluded.clone(),
                                s.pin.as_ref().map(|p| p.candidate.clone()),
                            )
                        })
                        .unwrap_or((false, None, TaskClass::CodingGeneral, Vec::new(), None));
                    if pinned
                        && phase != Some(crate::config::PlannerPhase::Implementation)
                        && let Some(target) = select_planner_target(
                            &shared,
                            crate::config::PlannerPhase::Planning,
                            class,
                            &excluded,
                            Some(diff),
                        )
                        && current.as_ref() != Some(&target)
                    {
                        shared.with_session(&router_sid, |s| {
                            s.pending_switch = Some(SwitchRequest {
                                target: target.clone(),
                                reason: format!("requested via `{lower_ref}:` prefix"),
                                handoff: HandoffStyle::Full,
                                user_pick: false,
                            });
                        });
                    }
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
                                handoff: HandoffStyle::Full,
                                user_pick: true,
                            });
                        });
                    } else {
                        let target = target.clone();
                        shared.with_session(&router_sid, |s| {
                            s.candidate_override = Some(target);
                            s.candidate_override_source = Some(OverrideSource::UserPick);
                        });
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
                            handoff: HandoffStyle::Full,
                            user_pick: true,
                        });
                    });
                    tracing::info!(session = router_sid, %target, "mid-session switch requested");
                } else {
                    shared.with_session(&router_sid, |s| {
                        s.candidate_override = Some(target);
                        s.candidate_override_source = Some(OverrideSource::UserPick);
                    });
                }
            }
            // `phase=` sets the planner phase (valid pre- and post-pin).
            // Implementation always applies; planning is rejected when the
            // session is already in implementation (monotonic upgrade).
            // Entering Planning injects the plan-first protocol. Upgrading a
            // pinned session to Implementation queues a summarize-and-re-pin
            // so the configured implementation candidate actually takes over
            // (the elicitation answer itself never leaves the planning turn).
            if let Some(phase) = directives.phase {
                use crate::config::PlannerPhase;
                let current = shared
                    .with_session(&router_sid, |s| s.planner_phase)
                    .unwrap_or(None);
                let pinned = shared
                    .with_session(&router_sid, |s| s.pin.is_some() || s.pinning)
                    .unwrap_or(false);
                match (current, phase) {
                    (Some(PlannerPhase::Implementation), PlannerPhase::Planning) => {
                        notify_user(
                            &shared,
                            &router_sid,
                            "router-acp · rejected: cannot downgrade from implementation to \
                             planning — phase transitions are one-way. Start a new session \
                             to plan again.",
                        );
                    }
                    (_, PlannerPhase::Planning) => {
                        enter_planning_phase(&shared, &router_sid);
                    }
                    (_, PlannerPhase::Implementation)
                        if shared
                            .with_session(&router_sid, |s| s.coordinator)
                            .unwrap_or(false) =>
                    {
                        notify_user(
                            &shared,
                            &router_sid,
                            "router-acp · coordinator: rejected [router: phase=implementation] \
                             — coordinator sessions stay in planning",
                        );
                    }
                    (_, PlannerPhase::Implementation) => {
                        shared.with_session(&router_sid, |s| {
                            s.planner_phase = Some(PlannerPhase::Implementation);
                        });
                        if pinned && current != Some(PlannerPhase::Implementation) {
                            planner_needs_switch = true;
                            notify_user(
                                &shared,
                                &router_sid,
                                "router-acp · planner: [router: phase=implementation] → \
                                 implementation phase",
                            );
                        }
                    }
                }
            }
            // Effort and version may change on a live pin; the remaining
            // routing directives only shape the still-unmade routing decision.
            let has_pre_pin_directives = directives.strategy.is_some()
                || directives.candidate.is_some()
                || directives.prefer.is_some()
                || !directives.exclude.is_empty()
                || directives.label.is_some();
            let applied = shared
                .with_session(&router_sid, |s| {
                    if directives.version.is_some() {
                        s.version_request = directives.version.clone();
                    }
                    if s.pin.is_some() || s.pinning {
                        if let Some(effort) = directives.effort {
                            s.effort_request = (effort != EffortLevel::Auto).then_some(effort);
                        }
                        if directives.effort.is_some() || directives.version.is_some() {
                            refresh_pinned_effort(&shared.runtime_config(), &shared.scores, s);
                            true
                        } else {
                            false
                        }
                    } else {
                        if let Some(strategy) = directives.strategy {
                            s.strategy = strategy;
                        }
                        if let Some(candidate) = directives.candidate.clone() {
                            s.candidate_override = Some(candidate);
                            s.candidate_override_source = Some(OverrideSource::UserPick);
                        }
                        if let Some(prefer) = directives.prefer.clone() {
                            s.preferred_candidate = Some(prefer);
                        }
                        s.excluded.extend(directives.exclude.clone());
                        if directives.label.is_some() {
                            s.run_label = directives.label.clone();
                        }
                        if let Some(effort) = directives.effort {
                            s.effort_request = (effort != EffortLevel::Auto).then_some(effort);
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
            // On a live pin, say which version the next request runs. A
            // version the pinned model does not declare is named, not hidden.
            let pinned = shared
                .with_session(&router_sid, |s| s.pin.as_ref().map(|p| p.candidate.clone()))
                .flatten();
            if let (Some(requested), Some(candidate)) = (directives.version.as_deref(), pinned) {
                let running = shared
                    .version_for(&router_sid, &candidate)
                    .map(|v| v.api_model)
                    .unwrap_or_else(|| shared.runtime_config().wire_api_model_unpinned(&candidate));
                let declared = requested == crate::config::DEFAULT_VERSION
                    || shared
                        .runtime_config()
                        .declared_version(&candidate, requested)
                        .is_some();
                let note = if declared {
                    format!("router-acp · version: {candidate} runs {running}")
                } else {
                    format!(
                        "router-acp · version: {requested} is not a version of {candidate}; \
                         it runs {running}"
                    )
                };
                notify_user(&shared, &router_sid, &note);
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
    // enrichment shells out (async), and classification must see the ENRICHED
    // prompt — "Fix HAI-1234" routes on the ticket's real content.
    if let Err(err) = crate::restoration::checkpoint(&shared, &router_sid) {
        return responder.respond_with_error(err);
    }
    cx.spawn(async move {
        let original_blocks = req.prompt.len();
        let req = crate::tickets::enrich_prompt(&shared, &router_sid, req).await;
        if req.prompt.len() > original_blocks {
            shared.state.lock().unwrap().log(
                &router_sid,
                &crate::state::LogEntry {
                    kind: "context_injection".into(),
                    role: "router".into(),
                    detail: Some(
                        json!({"prompt": req.prompt[..req.prompt.len() - original_blocks]}),
                    ),
                    ..Default::default()
                },
            );
        }
        dispatch_prompt(
            shared,
            router_sid,
            req,
            responder,
            explicit_routing,
            planner_needs_switch,
        )
        .await
    })
}

/// Post-directive prompt handling: pre-classifier, skill routing, and the
/// relay/pin dispatch. Runs inside a spawned task (never on the dispatch loop);
/// the prompt has already been ticket-enriched.
async fn dispatch_prompt(
    shared: Arc<Shared>,
    router_sid: String,
    req: PromptRequest,
    responder: Responder<PromptResponse>,
    explicit_routing: bool,
    mut planner_needs_switch: bool,
) -> Result<(), AcpError> {
    crate::auth::refresh_before_selection(&shared).await;
    // Mid-session routing (cordon escapes, skill and planner switches) picks
    // from live targets only; give dead ones their cooldown-gated respawn
    // first, as the initial pin does.
    revive_dead_targets(&shared).await;
    // Pre-classifier (when enabled): one cheap ACP evaluation covering task
    // class, complexity and host dimensions. v1 = first eligible turn per
    // session. Fail-open. Explicit `[router:…]` / `model:` suppress it.
    let eligible_preclass = !explicit_routing;
    let already = shared
        .with_session(&router_sid, |s| s.preclass_done)
        .unwrap_or(true);
    let preclass = if crate::pre_classifier::should_run(
        &shared.cfg.pre_classifier,
        already,
        eligible_preclass,
    ) {
        if shared.cfg.pre_classifier.disclose {
            notify_user(
                &shared,
                &router_sid,
                "router-acp · pre-class start · evaluating prompt…",
            );
        }
        let result = crate::pre_classifier::evaluate(
            &shared,
            &router_sid,
            &req.prompt,
            &responder.cancellation(),
        )
        .await;

        // Backstop: `evaluate()` already falls back to the static keyword
        // classifier when the LLM walk produces no routing. If routing is
        // *still* missing (client cancel, or a future path that returns
        // `routing: None`), refuse to pin rather than invent a class here.
        // `preclass_done` stays false so a later client retry re-attempts
        // classification; there is exactly one bounded classification attempt
        // per prompt, so a failing classifier can never cascade into an
        // unbounded router-side retry loop.
        if result.routing.is_none() {
            crate::pre_classifier::disclose(&shared, &router_sid, &result);
            let msg = format!(
                "router-acp · pre-classifier could not classify this prompt on any evaluator \
                 ({}). Refusing to route without a classification.",
                result
                    .skip_reason
                    .as_deref()
                    .unwrap_or("no classification result")
            );
            notify_user(&shared, &router_sid, msg.clone());
            flush_pending_disclosure(&shared, &router_sid);
            return responder.respond_with_error(AcpError::internal_error().data(msg));
        }

        // Authoritative decision note MUST reach the model — UI disclosures are
        // peeled into a classify tool card and never enter the agent prompt.
        let decision_note = crate::pre_classifier::agent_decision_note(&shared.cfg, &result);
        let preclass_profile = result.routing.as_ref().map(|routing| {
            let cwd = shared
                .with_session(&router_sid, |s| s.cwd.clone())
                .unwrap_or_else(std::env::temp_dir);
            crate::classifier::TaskProfile {
                class: routing.task_class,
                complexity: routing.complexity,
                languages: cwd_language_fingerprint(&shared.rules, &cwd),
                effort: routing
                    .effort
                    .or_else(|| Some(automatic_effort(routing.task_class, routing.complexity))),
            }
        });
        shared.with_session(&router_sid, |s| {
            s.preclass_done = true;
            s.required_mcp_capabilities = result
                .routing
                .as_ref()
                .map(|routing| routing.required_capabilities.clone())
                .unwrap_or_default();
            if preclass_profile.is_some() {
                s.preclass_profile = preclass_profile;
            }
            s.pending_injects.push(decision_note);
            if !result.injects.is_empty() {
                s.pending_injects.extend(result.injects.iter().cloned());
            }
        });
        crate::pre_classifier::disclose(&shared, &router_sid, &result);
        Some(result)
    } else {
        None
    };

    // Hoist skill detection so both the planner phase logic and skill routing
    // can reuse the result without a second pattern-matching pass.
    let detected_skill = detect_skill_route(&shared.cfg, &req.prompt);

    // Planner phase (when router == planner): update the monotonic phase,
    // potentially queuing a mid-session switch to an implementation model.
    // A `[router: phase=…]` directive already ran above (`explicit_routing`);
    // skip the classifier/heuristic pass so it cannot fight the directive,
    // but keep a directive-driven implementation upgrade on `planner_needs_switch`.
    if !explicit_routing {
        planner_needs_switch |= maybe_update_planner_phase(
            &shared,
            &router_sid,
            &req.prompt,
            detected_skill,
            preclass.as_ref(),
        );
    }
    if planner_needs_switch {
        let (class, excluded) = shared
            .with_session(&router_sid, |s| {
                (
                    s.task_class.unwrap_or(TaskClass::CodingGeneral),
                    s.excluded.clone(),
                )
            })
            .unwrap_or((TaskClass::CodingGeneral, Vec::new()));
        if let Some(target) = select_planner_target(
            &shared,
            crate::config::PlannerPhase::Implementation,
            class,
            &excluded,
            None,
        ) {
            shared.with_session(&router_sid, |s| {
                if s.pending_switch.is_none() {
                    s.pending_switch = Some(SwitchRequest {
                        target,
                        reason: "planner phase upgrade → implementation".to_string(),
                        handoff: HandoffStyle::Full,
                        user_pick: false,
                    });
                }
            });
        }
    }

    // Skill routing: certain skills (e.g. ship-pr) demand a capable model class.
    // If the prompt invokes a skill, steer routing to its preferred candidates —
    // pre-pin via candidate_override, mid-session via a switch.
    //
    // "Already ok" requires the pin to still be *routeable*, not just glob-
    // matching. A pin that matches `*opus*` but is usage-cordoned (plan at
    // 100%, no overage) must fall through to the next eligible skill
    // candidate (e.g. grok) instead of staying on the dead seat.
    //
    // Acceptability and switch targets are DIFFERENT sets: the pin is fine if
    // it matches `candidates` OR `also_acceptable`, but a switch may only
    // target `candidates`. Collapsing the two (the pre-`also_acceptable`
    // behaviour) force-switches an already-better pin onto a lesser model for
    // no reason other than its absence from the target pool.
    if let Some(route) = detected_skill {
        let (class, excluded, current) = shared
            .with_session(&router_sid, |s| {
                (
                    s.task_class.unwrap_or(TaskClass::CodingGeneral),
                    s.excluded.clone(),
                    s.pin.as_ref().map(|p| p.candidate.clone()),
                )
            })
            .unwrap_or((TaskClass::CodingGeneral, Vec::new(), None));
        let already_ok = current.as_ref().is_some_and(|c| {
            route
                .candidates
                .iter()
                .chain(route.also_acceptable.iter())
                .any(|rc| candidate_matches(rc, c))
                && !is_excluded(c, &excluded)
                // "Is the CURRENT pin still serviceable?", not "would auto
                // pick it?" — an explicitly pinned legacy version satisfies
                // an approved skill pool and must not be switched away from.
                && shared
                    .candidate_view(c, &RequiredCaps::default(), class)
                    .is_some()
        });
        if already_ok {
            // Re-invoking the skill on an already-compliant pin is an
            // explicit restatement of the verdict, not a no-op: without this,
            // the demotion clock kept counting from the FIRST invocation, so
            // a second `/ship-pr` mid-flow did nothing to stop an elevated
            // pin expiring under it.
            //
            // Also drop a queued switch (auto-upgrade / demotion / cordon
            // escape) that would leave this pin. Observed: grok was the
            // session pin, `/finalize-pr` matched `already_ok` (grok is in
            // `candidates`), then `send_prompt_with_failover` consumed a
            // leftover auto-upgrade to Sol — `also_acceptable`, never a
            // switch target — and the skill's pin was discarded.
            let pattern = route.pattern.clone();
            shared.with_session(&router_sid, |s| {
                s.elevation = Some(format!("skill `{pattern}`"));
                s.elevation_skill = Some(pattern.clone());
                s.quiet_turns = 0;
                s.pending_switch = None;
            });
        } else {
            match select_route_target(&shared, route, class, &excluded) {
                Some(target) => {
                    let pattern = route.pattern.clone();
                    let handoff = if route.terse_handoff {
                        HandoffStyle::Terse
                    } else {
                        HandoffStyle::Full
                    };
                    // No-op if somehow already on the picked target (e.g. pin
                    // was outside the skill set but equal by coincidence).
                    if current.as_ref() == Some(&target) {
                        shared.with_session(&router_sid, |s| {
                            s.elevation = Some(format!("skill `{pattern}`"));
                            s.elevation_skill = Some(pattern.clone());
                            s.quiet_turns = 0;
                        });
                    } else if current.is_some() {
                        shared.with_session(&router_sid, |s| {
                            s.pending_switch = Some(SwitchRequest {
                                target: target.clone(),
                                reason: format!(
                                    "skill `{pattern}` requires a {target}-class model"
                                ),
                                handoff,
                                user_pick: false,
                            });
                            s.elevation = Some(format!("skill `{pattern}`"));
                            s.elevation_skill = Some(pattern.clone());
                            s.quiet_turns = 0;
                        });
                        tracing::info!(session = router_sid, skill = %pattern, %target, "skill switch queued");
                    } else {
                        // Pre-pin: an explicit UserPick (`[router: candidate=…]`,
                        // `model:` shorthand, `router.candidate` config) already
                        // chose the seat. Skill routing used to overwrite that
                        // override because `already_ok` is false when `current`
                        // pin is None, so spawn `model: claude/sonnet` plus a
                        // brief that names ship-pr/finalize-pr silently pinned
                        // grok. A later `/ship-pr` on a pinned session still
                        // switches via the `current.is_some()` arm above; a
                        // plain unpinned `/ship-pr` with no UserPick still
                        // steers (this skip is UserPick-only).
                        let keep_user_pick = shared
                            .with_session(&router_sid, |s| {
                                matches!(
                                    s.candidate_override_source,
                                    Some(OverrideSource::UserPick)
                                )
                            })
                            .unwrap_or(false);
                        if keep_user_pick {
                            tracing::info!(
                                session = router_sid,
                                skill = %pattern,
                                "skill steer skipped: user pick already set"
                            );
                        } else {
                            shared.with_session(&router_sid, |s| {
                                s.candidate_override = Some(target.clone());
                                s.candidate_override_source =
                                    Some(OverrideSource::Skill(pattern.clone()));
                                s.elevation = Some(format!("skill `{pattern}`"));
                                s.elevation_skill = Some(pattern.clone());
                                s.quiet_turns = 0;
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

    // Proactive re-route: if the pin became usage-cordoned mid-session (cap
    // hit after the pin was chosen) and nothing else has queued a switch,
    // move to the best still-eligible candidate before this turn runs. Without
    // this, a long session stuck on opus at 100% keeps burning failed turns
    // until reactive rate-limit failover kicks in.
    {
        let (class, excluded, current, has_pending) = shared
            .with_session(&router_sid, |s| {
                (
                    s.task_class.unwrap_or(TaskClass::CodingGeneral),
                    s.excluded.clone(),
                    s.pin.as_ref().map(|p| p.candidate.clone()),
                    s.pending_switch.is_some() || s.escalation_requested.is_some(),
                )
            })
            .unwrap_or((TaskClass::CodingGeneral, Vec::new(), None, true));
        if !has_pending && let Some(cur) = current.as_ref() {
            let cordon = shared.headroom.lock().unwrap().usage_cordon(cur).cloned();
            if let Some(c) = cordon {
                // Escaping a dead seat is not a quality upgrade. The previous
                // `*` + max-quality pick crowned the fleet champion (Fable →
                // Sol) after a pin-rewrite 400 cordoned the incumbent — a
                // lateral swap of two equal-quality models. Skill-elevated
                // pins stay inside that skill's `candidates`.
                let target = cordon_escape_target(&shared, &router_sid, cur, class, &excluded);
                if let Some(target) = target {
                    let reason = format!(
                        "usage cordon: {} (resets {}) — switching off {}",
                        c.reason, c.resets_at_rfc3339, cur
                    );
                    shared.with_session(&router_sid, |s| {
                        s.pending_switch = Some(SwitchRequest {
                            target: target.clone(),
                            reason: reason.clone(),
                            handoff: HandoffStyle::Full,
                            user_pick: false,
                        });
                        // Not an elevation: this is escaping a dead seat, not
                        // climbing the capability ladder.
                        s.quiet_turns = 0;
                    });
                    notify_user(
                        &shared,
                        &router_sid,
                        format!("router-acp · {reason}; next turn on {target}"),
                    );
                    tracing::info!(
                        session = router_sid,
                        from = %cur,
                        %target,
                        cordon = %c.reason,
                        "proactive switch off usage-cordoned pin"
                    );
                }
            }
        }
    }

    enum Action {
        Relay,
        Pin,
        Unknown,
        Busy,
    }

    let action = {
        let mut sessions = shared.sessions.lock().unwrap();
        match sessions.get_mut(&router_sid) {
            None => Action::Unknown,
            Some(session) => match &session.pin {
                Some(_) => Action::Relay,
                None if session.pinning => Action::Busy,
                None => {
                    session.pinning = true;
                    session.cancelled = false;
                    Action::Pin
                }
            },
        }
    };

    if let Err(err) = crate::restoration::checkpoint(&shared, &router_sid) {
        return responder.respond_with_error(err);
    }
    match action {
        Action::Relay => {
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
    crate::accounts::cancel_login(&shared, &router_sid);
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
    req: ListSessionsRequest,
    responder: Responder<ListSessionsResponse>,
    _cx: ConnectionTo<ClientPeer>,
) -> Result<(), AcpError> {
    let sessions = shared
        .state
        .lock()
        .unwrap()
        .all()
        .into_iter()
        .filter(|(_, s)| s.kind != "delegate" && req.cwd.as_ref().is_none_or(|cwd| *cwd == s.cwd))
        .map(|(id, s)| {
            agent_client_protocol::schema::v1::SessionInfo::new(id, s.cwd)
                .additional_directories(s.additional_directories)
                .title(s.title)
                .updated_at(
                    s.updated_at
                        .and_then(|ts| chrono::DateTime::from_timestamp(ts as i64, 0))
                        .map(|ts| ts.to_rfc3339()),
                )
        })
        .collect();
    responder.respond(ListSessionsResponse::new(sessions))
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
    // Host-owned delegate MCP catalogs are registered against a router session
    // and remain inert until that session explicitly asks a delegate to use a
    // named bundle. This is intentionally generic: router-acp never knows the
    // integration, URL, or credential behind a catalog entry.
    if let Dispatch::Notification(msg) = &message
        && msg.method() == "router-acp/delegate_mcp_catalogs"
    {
        if shared.cfg.delegation.mcp_catalogs.is_empty() {
            return Ok(Handled::Yes);
        }
        let Some(router_sid) = msg.params().get("sessionId").and_then(|v| v.as_str()) else {
            return Ok(Handled::Yes);
        };
        let catalogs = msg
            .params()
            .get("catalogs")
            .cloned()
            .and_then(|v| serde_json::from_value::<HashMap<String, Vec<McpServer>>>(v).ok())
            .unwrap_or_default();
        shared.with_session(router_sid, |s| s.delegate_mcp_catalogs = catalogs);
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
            if msg.method() == "_session/steering" {
                let prompt: Vec<ContentBlock> = match serde_json::from_value(
                    msg.params().get("prompt").cloned().unwrap_or(Value::Null),
                ) {
                    Ok(prompt) => prompt,
                    Err(err) => {
                        return responder
                            .respond_with_error(AcpError::invalid_params().data(err.to_string()))
                            .map(|_| Handled::Yes);
                    }
                };
                let task_shared = shared.clone();
                shared
                    .upstream()
                    .ok_or_else(AcpError::internal_error)?
                    .spawn(async move {
                        let result = conn
                            .send_request(fwd)
                            .forward_cancellation_from(responder.cancellation())
                            .block_task()
                            .await;
                        if result
                            .as_ref()
                            .ok()
                            .and_then(|v| v.get("outcome"))
                            .and_then(Value::as_str)
                            == Some("injected")
                        {
                            let saved = task_shared.state.lock().unwrap().log_checked(
                                &router_sid,
                                &crate::state::LogEntry {
                                    kind: "user_steer".into(),
                                    role: "user".into(),
                                    summary: prompt_display_text(&prompt),
                                    detail: Some(json!({"prompt": prompt})),
                                    ..Default::default()
                                },
                            );
                            if let Err(err) = saved {
                                let message = format!("cannot save injected user message: {err}");
                                task_shared.with_session(&router_sid, |s| {
                                    s.persistence_error = Some(message.clone())
                                });
                                let _ = responder
                                    .respond_with_error(AcpError::internal_error().data(message));
                                return Ok(());
                            }
                        }
                        let _ = responder.respond_with_result(result);
                        Ok(())
                    })?;
            } else {
                relay_request_to_downstream(&shared, conn, fwd, responder)?;
            }
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
mod xai_gate_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn open_gate_is_routable() {
        // The normal frame grok emits at session start: access allowed.
        assert!(
            xai_gate_reason(&json!({
                "allow_access": true,
                "gate_message": null,
                "subscription_tier_display": "Free"
            }))
            .is_none()
        );
        // Absent fields (a settings frame that isn't about access) → routable.
        assert!(xai_gate_reason(&json!({"show_resolved_model": false})).is_none());
        // Empty gate message with allow true → routable.
        assert!(xai_gate_reason(&json!({"allow_access": true, "gate_message": "  "})).is_none());
    }

    #[test]
    fn closed_gate_cordons_with_reason() {
        // allow_access:false with no message → default reason.
        let r = xai_gate_reason(&json!({"allow_access": false, "gate_message": null})).unwrap();
        assert!(r.contains("access gate closed"), "{r}");
        // A populated gate message is surfaced (and bounded).
        let r = xai_gate_reason(&json!({
            "allow_access": false,
            "gate_message": "You've reached your Grok usage limit. Upgrade to continue."
        }))
        .unwrap();
        assert!(
            r.starts_with("xAI subscription gate: You've reached"),
            "{r}"
        );
        // A gate_message alone (allow_access absent) still closes the gate.
        let r = xai_gate_reason(&json!({"gate_message": "rate limited"})).unwrap();
        assert!(r.contains("rate limited"), "{r}");
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
mod prompt_framing_tests {
    use super::build_background_instructions;
    use super::build_question_instructions;
    use super::frame_terse;
    use super::is_native_subagent_tool;
    use crate::candidate::CandidateId;
    use serde_json::json;

    #[test]
    fn terse_handoff_does_not_assume_repository_tools() {
        let from = CandidateId::new("agent", "model");
        let text = frame_terse(&from, "TASK: continue", "router-acp transcript");
        for forbidden in ["git status", "gh pr", "current branch", "the ticket"] {
            assert!(
                !text.contains(forbidden),
                "{forbidden:?} leaked into: {text}"
            );
        }
    }

    #[test]
    fn managed_backgrounds_forbid_provider_private_shells() {
        let text = build_background_instructions();
        assert!(text.contains("background_start"), "{text}");
        assert!(text.contains("run_in_background"), "{text}");
        assert!(text.contains("exit when its condition fires"), "{text}");
        assert!(text.contains("Kory Code wakes you"), "{text}");
        assert!(
            text.contains("visible, inspectable, and cancellable"),
            "{text}"
        );
    }

    #[test]
    fn question_guidance_requires_the_structured_agent_mechanism() {
        let text = build_question_instructions();
        assert!(text.contains("[router-acp questions]"), "{text}");
        assert!(text.contains("structured question-asking"), "{text}");
        assert!(text.contains("ACP client"), "{text}");
        assert!(text.contains("Do not ask the user only in prose"), "{text}");
    }

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
    fn several_directive_tags_merge() {
        // Two tags on one line, the second without a space after `router:` —
        // both apply, and neither reaches the model.
        let prompt = vec![ContentBlock::from(
            "[router: candidate=codex/gpt-6.1-sol] [router:effort=medium] implement-all"
                .to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.candidate.unwrap().to_string(), "codex/gpt-6.1-sol");
        assert_eq!(dir.effort, Some(EffortLevel::Medium));
        assert_eq!(text(&stripped), "implement-all");

        // A later tag overrides an earlier key; exclusions combine.
        let prompt = vec![ContentBlock::from(
            "[router: effort=high, exclude=grok]\ntask\n[router: effort=low, exclude=kimi]"
                .to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.effort, Some(EffortLevel::Low));
        assert_eq!(dir.exclude, vec!["grok".to_string(), "kimi".to_string()]);
        assert_eq!(text(&stripped), "task");
    }

    #[test]
    fn quoted_directive_examples_are_preserved() {
        for example in [
            "Explain `[router: candidate=…]` without running it.",
            "Explain ``a `backtick` and [router: candidate=…]``.",
            "```text\n`a backtick`\n[router: candidate=…]\n```\nExplain it.",
            "~~~~text\n[router: candidate=…]\n~~~\n~~~~\nExplain it.",
            "> [router: candidate=…]\nExplain the quote.",
            "Explain `an unfinished [router: candidate=…]",
            r"An escaped \[router: candidate=…] is literal text.",
        ] {
            let prompt = vec![ContentBlock::from(example.to_string())];
            assert!(
                parse_prompt_directives(&prompt).unwrap().is_none(),
                "example became a command: {example}"
            );
        }
    }

    #[test]
    fn history_directives_do_not_override_current_commands() {
        for frame in [
            "resumed-conversation-context",
            "continued-work-handoff",
            "turn-context",
        ] {
            let history = format!(
                "<{frame}>\n[router: candidate=…]\n\
                 [router: candidate=mock/other, switch=mock/other, effort=low, \
                 version=old, phase=implementation, exclude=mock/current]\n</{frame}>"
            );
            let prompt = vec![ContentBlock::from(format!(
                "[router: candidate=mock/current, effort=high, version=default, \
                 phase=planning, exclude=grok]\n{history}\nResume [router: label=live]"
            ))];
            let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
            assert_eq!(dir.candidate.unwrap().to_string(), "mock/current");
            assert!(dir.switch.is_none());
            assert_eq!(dir.effort, Some(EffortLevel::High));
            assert_eq!(dir.version.as_deref(), Some("default"));
            assert_eq!(dir.phase, Some(crate::config::PlannerPhase::Planning));
            assert_eq!(dir.exclude, vec!["grok".to_string()]);
            assert_eq!(dir.label.as_deref(), Some("live"));
            assert_eq!(text(&stripped), format!("{history}\nResume"));
        }
    }

    #[test]
    fn history_frames_can_nest_and_cross_text_blocks() {
        let prompt = vec![
            ContentBlock::from("<resumed-conversation-context>".to_string()),
            ContentBlock::from(
                "<continued-work-handoff>[router: candidate=…]</continued-work-handoff>\n\
                 [router: switch=mock/old]"
                    .to_string(),
            ),
            ContentBlock::from(
                "</resumed-conversation-context>\n[router: candidate=mock/current] Resume"
                    .to_string(),
            ),
        ];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.candidate.unwrap().to_string(), "mock/current");
        assert!(dir.switch.is_none());
        assert!(text(&stripped).contains("[router: candidate=…]"));
        assert!(text(&stripped).contains("[router: switch=mock/old]"));
        assert!(text(&stripped).ends_with("Resume"));
    }

    #[test]
    fn quoted_frame_markers_do_not_hide_commands_after_history() {
        let history = "<resumed-conversation-context>\n\
                       ```xml\n<continued-work-handoff>\n```\n\
                       `</resumed-conversation-context>`\n\
                       > <continued-work-handoff>\n\
                       </resumed-conversation-context>";
        let prompt = vec![
            ContentBlock::from("[router: candidate=mock/current]".to_string()),
            ContentBlock::from(history.to_string()),
            ContentBlock::from("[router: effort=high] Resume".to_string()),
        ];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.candidate.unwrap().to_string(), "mock/current");
        assert_eq!(dir.effort, Some(EffortLevel::High));
        assert_eq!(text(&stripped), format!("{history}\nResume"));
    }

    #[test]
    fn live_commands_after_unicode_and_code_keep_offsets_and_precedence() {
        let prompt = vec![ContentBlock::from(
            "é `[router: candidate=…]`\n[RoUtEr: candidate=mock/current, effort=high]\n\
             ```\n[router: effort=low]\n```\n[router: effort=medium] Resume"
                .to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.candidate.unwrap().to_string(), "mock/current");
        assert_eq!(dir.effort, Some(EffortLevel::Medium));
        let out = text(&stripped);
        assert!(out.contains("é `[router: candidate=…]`"));
        assert!(out.contains("```\n[router: effort=low]\n```"));
        assert!(out.ends_with("Resume"));
    }

    #[test]
    fn malformed_live_commands_still_fail_after_examples() {
        let prompt = vec![ContentBlock::from(
            "Example: `[router: candidate=…]`\n[router: candidate=invalid] Resume".to_string(),
        )];
        let error = parse_prompt_directives(&prompt).unwrap_err();
        assert!(error.contains("candidate `invalid`"), "{error}");
    }

    #[test]
    fn escaped_backticks_do_not_hide_live_commands() {
        let prompt = vec![ContentBlock::from(
            r"A literal \` before [router: candidate=mock/current] Resume".to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.candidate.unwrap().to_string(), "mock/current");
        assert_eq!(text(&stripped), r"A literal \` before  Resume");
    }

    #[test]
    fn directive_survives_goose_turn_context_preamble() {
        // goose prepends a <turn-context> block, pushing the directive off
        // line 1. The parser must still find and strip exactly that line —
        // and a bracketed model id like `claude-fable-5[1m]` must parse.
        let prompt = vec![ContentBlock::from(
            "<turn-context>\n<current-time>2026-07-10</current-time>\n</turn-context>\n\
             [router: candidate=claude/claude-fable-5[1m], label=nightly]\n\n\
             Run the nightly task."
                .to_string(),
        )];
        let (dir, stripped) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(
            dir.candidate.unwrap().to_string(),
            "claude/claude-fable-5[1m]"
        );
        assert_eq!(dir.label.as_deref(), Some("nightly"));
        let out = text(&stripped);
        assert!(!out.contains("[router:"), "directive stripped: {out}");
        assert!(out.contains("<turn-context>"), "preamble preserved: {out}");
        assert!(
            out.contains("Run the nightly task."),
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
    fn parses_a_version_directive() {
        let prompt = vec![ContentBlock::from(
            "[router: switch=claude/opus, version=claude-opus-4-6]\ngo".to_string(),
        )];
        let (dir, _) = parse_prompt_directives(&prompt).unwrap().unwrap();
        assert_eq!(dir.version.as_deref(), Some("claude-opus-4-6"));
        let empty = vec![ContentBlock::from("[router: version=]\ngo".to_string())];
        assert!(parse_prompt_directives(&empty).is_err());
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

        let (r, s) =
            split_model_shorthand(&[ContentBlock::from("hard: redesign auth".to_string())])
                .unwrap();
        assert_eq!(r, "hard");
        assert_eq!(text(&s), "redesign auth");
        let (r, s) =
            split_model_shorthand(&[ContentBlock::from("easy: list the endpoints".to_string())])
                .unwrap();
        assert_eq!(r, "easy");
        assert_eq!(text(&s), "list the endpoints");
    }

    /// The note the Kory Code client appends to the first prompt of every
    /// session (abridged from ~50 paths).
    const KORY_SKILL_CATALOG_NOTE: &str = "[kory-code] skills are available in this checkout. \
         use the skill workflow, not memory or an improvised substitute: read the relevant file \
         before acting. skill files: .claude/skills/agentic-coding-norms/skill.md, \
         .claude/skills/ship-pr/skill.md, .claude/skills/testing/skill.md";

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
    fn skill_pattern_matches_slash_form_with_punctuation() {
        for text in ["/ship-pr", "then /ship-pr?", "(/ship-pr)", "use /ship-pr."] {
            assert!(
                prompt_mentions_skill(text, "ship-pr"),
                "slash form must invoke: {text}"
            );
        }
        // A bare token standing alone still invokes (code spans already gone).
        assert!(prompt_mentions_skill(
            "read and follow the checked-in ship-pr skill at ",
            "ship-pr"
        ));
    }

    #[test]
    fn skill_named_as_path_segment_is_not_an_invocation() {
        assert!(!prompt_mentions_skill(
            ".claude/skills/ship-pr/skill.md",
            "ship-pr"
        ));
        // The Kory Code client appends this catalog to every first prompt; it
        // names ~50 skills and must steer nothing.
        assert!(!prompt_mentions_skill(KORY_SKILL_CATALOG_NOTE, "ship-pr"));
        assert!(!prompt_mentions_skill(KORY_SKILL_CATALOG_NOTE, "testing"));
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

        // The client's skill-file catalog rides along on the first prompt of
        // EVERY session: it must not hijack routing, and must not mask a real
        // invocation in the same prompt.
        let catalog = vec![ContentBlock::from(format!(
            "user task text\n\n{KORY_SKILL_CATALOG_NOTE}"
        ))];
        assert!(
            detect_skill_route(&cfg, &catalog).is_none(),
            "a skill listed as a file path must not resolve a route"
        );
        let both = vec![ContentBlock::from(format!(
            "let's run ship-pr on this\n\n{KORY_SKILL_CATALOG_NOTE}"
        ))];
        assert!(detect_skill_route(&cfg, &both).is_some());

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

    /// `SkillRoute` is `deny_unknown_fields`, so a fleet whose binary predates
    /// `also_acceptable` hard-fails on a config that uses it — the field and
    /// the config that sets it must ship together. The converse must stay
    /// true forever: an existing config that omits it keeps parsing.
    #[test]
    fn skill_route_also_acceptable_is_optional_and_parses() {
        let agents = "agents:\n\
             \x20 - name: claude\n\
             \x20   command: { type: stdio, command: /bin/true }\n\
             \x20   model_selection: { type: config-option }\n\
             \x20   models:\n\
             \x20     - { id: opus, display_name: Opus, cost_rank: 4 }\n";
        let without = Config::from_yaml(&format!(
            "router: auto\n\
             skill_routing:\n\
             \x20 - pattern: ship-pr\n\
             \x20   candidates: [\"*opus*\"]\n\
             {agents}"
        ))
        .expect("config without also_acceptable still parses");
        assert!(
            without.skill_routing[0].also_acceptable.is_empty(),
            "omitted also_acceptable defaults to empty"
        );

        let with = Config::from_yaml(&format!(
            "router: auto\n\
             skill_routing:\n\
             \x20 - pattern: ship-pr\n\
             \x20   candidates: [\"*opus*\"]\n\
             \x20   also_acceptable: [\"*fable*\", \"*sol*\"]\n\
             {agents}"
        ))
        .expect("config with also_acceptable parses");
        assert_eq!(
            with.skill_routing[0].also_acceptable,
            vec!["*fable*".to_string(), "*sol*".to_string()]
        );
    }

    /// `selection` and `terse_handoff` are additive keys: an existing route
    /// that predates them must still parse, defaulting to today's behaviour
    /// (`best-quality`, full summary) rather than failing to load.
    #[test]
    fn skill_route_selection_and_terse_handoff_default_and_parse() {
        let agents = "agents:\n\
             \x20 - name: claude\n\
             \x20   command: { type: stdio, command: /bin/true }\n\
             \x20   model_selection: { type: config-option }\n\
             \x20   models:\n\
             \x20     - { id: opus, display_name: Opus, cost_rank: 4 }\n";
        let without = Config::from_yaml(&format!(
            "router: auto\n\
             skill_routing:\n\
             \x20 - pattern: ship-pr\n\
             \x20   candidates: [\"*opus*\"]\n\
             {agents}"
        ))
        .expect("config without selection/terse_handoff still parses");
        assert_eq!(
            without.skill_routing[0].selection,
            RouteSelection::BestQuality,
            "omitted selection defaults to best-quality (today's behaviour)"
        );
        assert!(
            !without.skill_routing[0].terse_handoff,
            "omitted terse_handoff defaults to false (full summary, today's behaviour)"
        );

        let with = Config::from_yaml(&format!(
            "router: auto\n\
             skill_routing:\n\
             \x20 - pattern: ship-pr\n\
             \x20   candidates: [\"*grok*\", \"*opus*\"]\n\
             \x20   selection: first-match\n\
             \x20   terse_handoff: true\n\
             {agents}"
        ))
        .expect("config with selection/terse_handoff parses");
        assert_eq!(with.skill_routing[0].selection, RouteSelection::FirstMatch);
        assert!(with.skill_routing[0].terse_handoff);
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
