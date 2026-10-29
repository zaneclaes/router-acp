//! YAML configuration: parsing, defaults, env interpolation, validation.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::candidate::CandidateId;

/// Routing strategy selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StrategyKind {
    Static,
    Auto,
    ParetoCode,
    Escalation,
    Planner,
}

impl StrategyKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            StrategyKind::Static => "static",
            StrategyKind::Auto => "auto",
            StrategyKind::ParetoCode => "pareto-code",
            StrategyKind::Escalation => "escalation",
            StrategyKind::Planner => "planner",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "static" => Some(StrategyKind::Static),
            "auto" => Some(StrategyKind::Auto),
            "pareto-code" => Some(StrategyKind::ParetoCode),
            "escalation" => Some(StrategyKind::Escalation),
            "planner" => Some(StrategyKind::Planner),
            _ => None,
        }
    }
}

/// Two-phase routing: planning (frontier) then implementation (workhorse).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlannerPhase {
    Planning,
    Implementation,
}

/// Per-prompt planner-pool override from a `hard:` / `easy:` prefix.
/// Unprefixed prompts keep the full planning pool (auto + apex).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerDifficulty {
    /// Restrict planning to Opus / Sol (`easy: ` prefix).
    Easy,
    /// Restrict planning to Astra / Fable (`hard: ` prefix).
    Hard,
}

/// How far the `escalation` router jumps when it escalates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum EscalationPath {
    /// Move to the next-higher-capability eligible candidate, one step at a time.
    #[default]
    Ladder,
    /// Jump straight to the strongest eligible candidate.
    Leap,
}

/// How the routing decision is disclosed to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DisclosureMode {
    /// Visible `agent_message_chunk` status line before the downstream response.
    #[default]
    Chunk,
    /// Metadata-only: route details under `_meta.router_acp` on the first
    /// forwarded update.
    Meta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassifierConfig {
    #[serde(default = "default_classifier_backend")]
    pub backend: ClassifierBackend,
    #[serde(default)]
    pub local_model: Option<String>,
    #[serde(default = "default_classifier_timeout_ms")]
    pub timeout_ms: u64,
    /// Optional path to a classifier rules YAML overriding the built-in table.
    #[serde(default)]
    pub rules_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClassifierBackend {
    Heuristic,
    LocalModel,
}

fn default_classifier_backend() -> ClassifierBackend {
    ClassifierBackend::Heuristic
}

fn default_classifier_timeout_ms() -> u64 {
    1500
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            backend: ClassifierBackend::Heuristic,
            local_model: None,
            timeout_ms: default_classifier_timeout_ms(),
            rules_file: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Inject a scoped, model-facing directive when the router actually offers
    /// its delegation tools to an ordinary primary session. Disabled by
    /// default so deployments opt into the behavioral change explicitly.
    #[serde(default)]
    pub inject_prompt: bool,
    /// Host-defined MCP catalogs and the opaque capabilities each supplies.
    /// The router only resolves these strings; their meanings stay with the
    /// host's pre-classifier extension.
    #[serde(default)]
    pub mcp_catalogs: Vec<McpCatalogConfig>,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    /// Unix-domain socket path for the delegate MCP helper to connect back on.
    /// Defaults to a per-process path under the state directory.
    #[serde(default)]
    pub socket_path: Option<PathBuf>,
    /// Ceiling on a delegated subtask's classified complexity. The parent
    /// (often a frontier planner) has already decomposed the work into a
    /// fully-specified brief, but the heuristic classifier reads long,
    /// detailed briefs as *maximum* complexity — which both zeroes the
    /// `auto` strategy's complexity-scaled cost term and trips its p75
    /// quality gate, so every subtask routes to the most expensive
    /// candidate. Capping restores cost-aware routing for spec'd subtasks.
    /// 1.0 disables the cap.
    #[serde(default = "default_delegate_complexity_cap")]
    pub complexity_cap: f64,
    /// How a `delegate_task` `hints.candidate` is treated. `prefer` (default)
    /// honors the hint only inside the cost-scoped pool and otherwise ranks
    /// that pool as usual. `exact` lets the hint address any eligible
    /// candidate, whatever its tier or agent, and fails the call instead of
    /// substituting another model — for hosts whose workflow requires a
    /// specific worker model (e.g. a Sol parent with a Grok worker).
    #[serde(default)]
    pub candidate_hints: CandidateHintMode,
    /// Host-owned ceilings for automatic delegate effort, keyed by candidate
    /// pattern. All matching ceilings apply; omitted candidates keep the
    /// assignment baseline. This also covers router-initiated planner work.
    #[serde(default)]
    pub candidate_effort_caps: BTreeMap<String, crate::candidate::EffortLevel>,
    /// Whether the injected delegation directive forbids provider-native
    /// Task/spawn/subagent tools (`forbid`, default) or leaves them to the
    /// host's own workflow (`allow`).
    #[serde(default)]
    pub native_subagents: NativeSubagentPolicy,
    /// Optional external command run when a delegate starts and stops, so a
    /// host can account for router-owned workers it cannot otherwise see.
    #[serde(default)]
    pub lifecycle_hook: Option<DelegateLifecycleHook>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CandidateHintMode {
    #[default]
    Prefer,
    Exact,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeSubagentPolicy {
    #[default]
    Forbid,
    Allow,
}

/// Reasoning effort for primary sessions when no one asked for a level
/// explicitly (the `router.effort` option or `[router: effort=…]`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffortConfig {
    /// Used instead of the automatic class/complexity recommendation. A long,
    /// detailed prompt (a whole skill's instructions) classifies as maximum
    /// complexity, so the automatic level can be far higher than the work
    /// needs; a coordinating parent is better served by a fixed level.
    #[serde(default)]
    pub default: Option<crate::candidate::EffortLevel>,
    /// Ceiling on any level the router picks by itself (automatic or
    /// `default`). Explicit requests are never capped.
    #[serde(default)]
    pub max_automatic: Option<crate::candidate::EffortLevel>,
}

/// A command the router runs (no shell) with one JSON event on stdin (see
/// `delegate_hook`). `delegate_start` runs after the delegate session opens
/// and before its prompt; a non-zero exit or timeout refuses the delegate.
/// `delegate_turn_end` runs when each delegate turn ends; exit 2 sends the
/// hook's stderr back to that delegate as its next prompt, at most
/// `max_continuations` times per turn. `delegate_stop`, `parent_repinned`,
/// and a planner run's `planner_wake` and `work_finished` are delivered
/// durably through the state DB's outbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegateLifecycleHook {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default = "default_lifecycle_hook_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_max_continuations")]
    pub max_continuations: u32,
}

fn default_lifecycle_hook_timeout_ms() -> u64 {
    30_000
}

fn default_max_continuations() -> u32 {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpCatalogConfig {
    pub catalog: String,
    pub capabilities: Vec<String>,
}

fn default_delegate_complexity_cap() -> f64 {
    0.6
}

fn default_true() -> bool {
    true
}

fn default_max_concurrent() -> usize {
    3
}

impl Default for DelegationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            inject_prompt: false,
            mcp_catalogs: Vec::new(),
            max_concurrent: default_max_concurrent(),
            socket_path: None,
            complexity_cap: default_delegate_complexity_cap(),
            candidate_hints: CandidateHintMode::default(),
            candidate_effort_caps: BTreeMap::new(),
            native_subagents: NativeSubagentPolicy::default(),
            lifecycle_hook: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadroomConfig {
    /// Sliding-window length in seconds. Default 5 hours.
    #[serde(default = "default_window_secs")]
    pub window_secs: u64,
    /// Pre-prompt failures within the window before a candidate is quarantined.
    #[serde(default = "default_quarantine_failures")]
    pub quarantine_failures: u32,
    /// Cool-off period in seconds before a quarantined candidate is retried.
    #[serde(default = "default_quarantine_cooloff_secs")]
    pub quarantine_cooloff_secs: u64,
    /// Cordon length in seconds for a rate/usage-limited agent when the
    /// error did not include a parseable reset time.
    #[serde(default = "default_cordon_secs")]
    pub cordon_default_secs: u64,
}

fn default_cordon_secs() -> u64 {
    15 * 60
}

fn default_window_secs() -> u64 {
    5 * 60 * 60
}

fn default_quarantine_failures() -> u32 {
    3
}

fn default_quarantine_cooloff_secs() -> u64 {
    10 * 60
}

impl Default for HeadroomConfig {
    fn default() -> Self {
        Self {
            window_secs: default_window_secs(),
            quarantine_failures: default_quarantine_failures(),
            quarantine_cooloff_secs: default_quarantine_cooloff_secs(),
            cordon_default_secs: default_cordon_secs(),
        }
    }
}

/// Proactive cordoning driven by provider usage APIs: a periodic poll marks a
/// candidate unroutable while the provider reports its usage cap exhausted (and
/// no overage/credit headroom), before the router even tries it. Which agents
/// are polled is set per-agent via `agents[].usage_source`; this only gates the
/// whole mechanism and the poll cadence.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CordonConfig {
    /// Master switch (default on). When off, no usage polling happens and only
    /// the reactive (error-driven) per-agent cordons remain.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Usage poll interval / cache TTL in seconds (default 300, ~5 min to match
    /// the front-end's usage polling).
    #[serde(default = "default_cordon_poll_secs")]
    pub poll_secs: u64,
    /// Box-wide floor between upstream usage-endpoint fetches, in seconds
    /// (default 60). Enforced through the shared snapshot cache
    /// (`usage_cache`), so ALL router-acp processes on the box together make
    /// at most one usage fetch per interval.
    #[serde(default = "default_usage_min_refresh_secs")]
    pub min_refresh_secs: u64,
}

fn default_cordon_poll_secs() -> u64 {
    5 * 60
}

fn default_usage_min_refresh_secs() -> u64 {
    crate::usage_cache::DEFAULT_MIN_REFRESH_SECS
}

impl Default for CordonConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_secs: default_cordon_poll_secs(),
            min_refresh_secs: default_usage_min_refresh_secs(),
        }
    }
}

/// Plan-aware effective cost. Reported candidate plan headroom caps the local
/// sliding-window headroom used by routing. The static `agents[].preference`
/// bonus also fades as free plan budget is consumed, and a seat whose cap is
/// exhausted but remains routable via overage/credits takes a utility penalty.
/// Availability comes from the usage poller (`agents[].usage_source`) and from
/// client `availability_hint` extension notifications (see README).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AvailabilityPreferenceConfig {
    /// Master switch (default on). When off, `agents[].preference` applies
    /// statically and availability hints are ignored.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Aversion to spending paid overage, in [0, 1]. Auto routing subtracts
    /// `cost_aversion * (1 - task_complexity)` from an overage candidate, so
    /// difficult work can still justify frontier spend while 0 means the user
    /// is perfectly happy to pay. `overage_penalty` is accepted as a legacy
    /// alias for existing configs.
    #[serde(default = "default_cost_aversion", alias = "overage_penalty")]
    pub cost_aversion: f64,
    /// How long a client availability hint stays authoritative before the
    /// router falls back to its own poll, in seconds.
    #[serde(default = "default_hint_ttl_secs")]
    pub hint_ttl_secs: u64,
    /// Remaining overage/credit budget, in real dollars, at/above which the
    /// *expensive* term of `seat_budget` reads fully free. Used only for the
    /// overage/credit pool (absolute dollars across differently-sized caps).
    /// Free included-plan ranking uses `plan_headroom` fraction, not dollars
    /// (plan dollars saturated here and made a 6% weekly seat look fully free).
    #[serde(default = "default_headroom_scale_dollars")]
    pub headroom_scale_dollars: f64,
    /// Weight on the expensive (overage) term while free plan remains, in
    /// [0, 1]. `seat_budget = plan_headroom + overage_budget_weight ×
    /// overage_signal` (clamped). Default 0.2 so full overage cannot overturn
    /// a real weekly gap; when plan is empty the overage signal is used at
    /// full scale so two paying seats still separate by remaining dollars.
    #[serde(default = "default_overage_budget_weight")]
    pub overage_budget_weight: f64,
}

fn default_cost_aversion() -> f64 {
    0.1
}

fn default_hint_ttl_secs() -> u64 {
    10 * 60
}

fn default_headroom_scale_dollars() -> f64 {
    200.0
}

fn default_overage_budget_weight() -> f64 {
    crate::headroom::DEFAULT_OVERAGE_BUDGET_WEIGHT
}

impl Default for AvailabilityPreferenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cost_aversion: default_cost_aversion(),
            hint_ttl_secs: default_hint_ttl_secs(),
            headroom_scale_dollars: default_headroom_scale_dollars(),
            overage_budget_weight: default_overage_budget_weight(),
        }
    }
}

/// How to read a provider's usage/rate-limit data for an agent's candidates, so
/// they can be cordoned before their cap is hit. Generic across providers; the
/// specific model caps are discovered from the API response, never hardcoded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UsageSourceConfig {
    /// Anthropic subscription usage via the CLI OAuth token
    /// (`GET https://api.anthropic.com/api/oauth/usage`). The token is read from
    /// `~/.claude/.credentials.json` or the macOS Keychain
    /// (`Claude Code-credentials`), the same credential the adapter uses.
    AnthropicOauth,
    /// Codex/ChatGPT usage polled live via one `codex app-server` JSON-RPC
    /// round-trip (`account/rateLimits/read`), shared box-wide through the
    /// usage snapshot cache (`usage/codex.json`). When the RPC fails (no
    /// binary, signed out), falls back to the Codex CLI's own on-disk
    /// rate-limit snapshots (the newest `~/.codex/sessions/**/rollout-*.jsonl`
    /// — last-known as of Codex's most recent turn, not live). Parses an
    /// undocumented Codex format and fails open if it changes; the reactive
    /// per-agent cordon remains the backstop.
    CodexRollout,
}

/// Parse a duration string like `30d`, `12h`, `90m`, `3600s`, or a bare
/// number (interpreted as days). Returns seconds.
pub fn parse_history(spec: &str) -> Result<u64, String> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err("history must not be empty".into());
    }
    let (num, unit): (&str, u64) = match spec.chars().last() {
        Some('d') | Some('D') => (&spec[..spec.len() - 1], 24 * 60 * 60),
        Some('h') | Some('H') => (&spec[..spec.len() - 1], 60 * 60),
        Some('m') | Some('M') => (&spec[..spec.len() - 1], 60),
        Some('s') | Some('S') => (&spec[..spec.len() - 1], 1),
        Some(c) if c.is_ascii_digit() => (spec, 24 * 60 * 60), // bare number = days
        _ => {
            return Err(format!(
                "unrecognized history unit in `{spec}` (use d/h/m/s)"
            ));
        }
    };
    let value: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid history value `{spec}`"))?;
    let secs = value.saturating_mul(unit);
    if secs == 0 {
        return Err("history must be greater than zero".into());
    }
    Ok(secs)
}

fn default_history() -> String {
    "30d".to_string()
}

/// Auto-upgrade: when a session's estimated confidence drops below the
/// threshold (the pinned model looks under-powered for how the session is
/// going), switch it up to a more capable candidate mid-session.
///
/// Off by default. A live pin should move on a cordon/outage, a skill route,
/// or an explicit request — a quality verdict re-pinning a session is a full
/// summarize + re-pin that forfeits the live context, and in practice it fired
/// on ordinary struggle (a long tool-heavy turn) rather than real trouble.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoUpgradeConfig {
    /// Master switch, off by default. When false, sessions never auto-upgrade
    /// (explicit `[router: switch=...]` still works).
    #[serde(default)]
    pub enabled: bool,
    /// Confidence in [0, 1] below which a session upgrades. Higher = more
    /// eager to upgrade; 1.0 upgrades almost always; 0.0 effectively never.
    #[serde(default = "default_confidence_threshold")]
    pub confidence_threshold: f64,
}

fn default_confidence_threshold() -> f64 {
    0.55
}

impl Default for AutoUpgradeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            confidence_threshold: default_confidence_threshold(),
        }
    }
}

/// Load ticket details into the prompt when a ticket id is referenced. When a
/// prompt mentions `<prefix><digits>` (e.g. `HAI-1234`), the router runs
/// `command` (with `$TICKET` substituted) and prepends its stdout to the prompt
/// before classification — so "Fix HAI-1234" becomes a rich prompt that routes
/// on the ticket's actual content. Fails open: a failed/slow fetch leaves the prompt
/// unchanged.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TicketRule {
    /// Ticket id prefix, matched at a word start and followed by digits
    /// (e.g. `HAI-` matches `HAI-1234`).
    pub prefix: String,
    /// Command argv to print the ticket; every occurrence of `$TICKET` in any
    /// argument is replaced with the full ticket id. Run without a shell.
    /// e.g. `["linear", "issue", "view", "$TICKET"]`.
    pub command: Vec<String>,
}

/// How a route picks among the `candidates` globs that have an eligible
/// candidate behind them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RouteSelection {
    /// Highest `quality + preference` wins, regardless of list order (the
    /// historical behaviour, and right when the globs name interchangeable
    /// tiers and you simply want the best one that is up).
    #[default]
    BestQuality,
    /// The FIRST glob with an eligible candidate wins; quality only breaks
    /// ties *within* that one glob. Use when list order encodes a deliberate
    /// preference the score table does not — e.g. routing a ship flow to a
    /// flat-rate or second-company seat that a quality-max pick would never
    /// select, while still falling through to the next glob when that seat is
    /// cordoned or excluded.
    FirstMatch,
}

/// Force prompts that invoke a given skill onto a specific class of models.
/// When a prompt matches `pattern` (case-insensitive substring, e.g. the
/// skill name) and the pinned candidate is neither in `candidates` nor in
/// `also_acceptable`, the session switches to a candidate from `candidates`
/// chosen per `selection`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillRoute {
    /// Substring matched against prompt text (typically the skill name).
    pub pattern: String,
    /// Switch-TARGET globs (the required "model class"), e.g.
    /// `["*opus*", "*gpt-5.5*"]`. A pin matching one of these is left alone;
    /// otherwise a candidate from this list is switched to, per `selection`.
    pub candidates: Vec<String>,
    /// Globs that are acceptable to STAY on but are never switch targets.
    ///
    /// Without this, `candidates` does double duty — the "is the pin already
    /// fine?" test and the switch-target pool are the same list — so the only
    /// way to stop force-switching a good pin is to widen the target pool,
    /// which then makes every *genuine* switch land on the widest entry.
    /// Models that are already good enough for the skill but that you do not
    /// want to route *to* (typically the expensive top of the range) belong
    /// here instead.
    #[serde(default)]
    pub also_acceptable: Vec<String>,
    /// How to pick among `candidates`. Defaults to `best-quality`, which is
    /// what every route did before this key existed.
    #[serde(default)]
    pub selection: RouteSelection,
    /// Hand the incoming model a TERSE briefing (the task, the one identifier
    /// it operates on, and any decision not re-derivable from the repo)
    /// instead of a full conversation summary.
    ///
    /// A handoff summary costs the outgoing model a full read of its own
    /// context no matter how long the output is, so this is not a token
    /// optimization — it is a fidelity one. A skill that re-derives its own
    /// state (a ship flow resolving its PR from the branch) is better served
    /// by one unambiguous referent than by a narrative that may name three
    /// PRs and two abandoned approaches; it also lands the new session near
    /// empty, which matters when the target's context window is smaller than
    /// the outgoing model's.
    #[serde(default)]
    pub terse_handoff: bool,
    /// When true, invoking this skill upgrades a `planner` session to
    /// `Implementation` phase. Inert for non-planner routers.
    #[serde(default)]
    pub marks_implementation_phase: bool,
}

/// When a host-registered pre-classifier dimension should inject its prompt.
///
/// YAML shapes:
/// - `{ field: mode, equals: planning }` — string field match
/// - `{ warranted: true }` — boolean `warranted` field is true
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ActWhen {
    /// Act when `value[field] == equals` (string compare).
    FieldEquals { field: String, equals: String },
    /// Act when `value.warranted` equals this flag (usually `true`).
    Warranted { warranted: bool },
}

impl Default for ActWhen {
    fn default() -> Self {
        ActWhen::Warranted { warranted: true }
    }
}

/// One host-registered pre-classifier dimension (e.g. Kory's `ui_planning`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreClassDimension {
    pub id: String,
    /// Evaluator instruction for this dimension.
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_dim_min_confidence")]
    pub min_confidence: f64,
    #[serde(default)]
    pub act_when: ActWhen,
    /// Text injected into the user/agent turn when the dimension acts.
    #[serde(default)]
    pub inject_prompt: String,
}

fn default_dim_min_confidence() -> f64 {
    0.70
}

/// Composable LLM pre-classifier: one cheap ACP evaluation returns the task
/// class/complexity, the planner router's phase, and host-registered dimensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreClassifierConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Candidate globs for the evaluator seat, preference order (cheapest first).
    /// These are a cost preference only: when none match an eligible model, the
    /// pre-classifier widens to any available model. When still no evaluator
    /// produces a routing decision (zero eligible models, unparseable reply,
    /// mode-less tool use), the static keyword classifier supplies routing so
    /// session start is not hard-blocked.
    #[serde(default = "default_preclass_evaluator")]
    pub evaluator: Vec<String>,
    /// DEPRECATED and ignored. The classifier LLM call is core infrastructure
    /// that must run to completion, so it has no *total* wall-clock timeout — a
    /// slow-but-working evaluator is allowed to finish. Retained (accepted,
    /// unused) so existing configs still load. The safety role this field used
    /// to serve now belongs to `stall_timeout_ms`, which bounds silence rather
    /// than duration.
    #[serde(default = "default_preclass_timeout_ms")]
    pub timeout_ms: u64,
    /// Max time the evaluator may go with **no streamed progress** before the
    /// attempt is treated as a service failure (cordon → next evaluator →
    /// static classifier). This is deliberately NOT a total wall-clock cap: a
    /// slow evaluator that keeps streaming never trips it, so it preserves
    /// "must be allowed to run to completion" while still bounding a wedge.
    ///
    /// Without this, an evaluator that is alive but silent hangs the prompt
    /// forever: the turn never starts and the session shows no activity at all
    /// (observed 2026-08-10 — a 2h57m wedge on a live session). Set 0 to
    /// disable, which restores the unbounded-hang behavior.
    #[serde(default = "default_preclass_stall_timeout_ms")]
    pub stall_timeout_ms: u64,
    /// Emit `router-acp · pre-class …` disclosure lines.
    #[serde(default = "default_true")]
    pub disclose: bool,
    /// Host extensions (e.g. `ui_planning`). One evaluator call covers all.
    #[serde(default)]
    pub dimensions: Vec<PreClassDimension>,
    /// Working directory for the evaluator's throwaway session, replacing the
    /// classified session's own cwd. Also drops `additional_directories`.
    ///
    /// The classifier reads the user's prompt text and returns JSON, and its
    /// session is already opened tool-less, so it has no reason to load the
    /// project's agent context — but cwd is what every agent uses to find that
    /// context, so it loads anyway and the host pays a second full startup per
    /// session. Measured on a Claude host against a large repo: 38,026 input
    /// tokens for one classification at the repo cwd (its `CLAUDE.md`/
    /// `AGENTS.md`, a 59-entry skills catalog, `.mcp.json`) versus 20,917 at a
    /// directory holding only a settings file — 17,109 tokens to classify a
    /// prompt the repo has no bearing on.
    ///
    /// Point this at a small host-owned directory rather than an empty one:
    /// agent context is not the only thing cwd governs, and on a Claude host
    /// the project settings there are also what suppress unused built-in tool
    /// schemas (worth ~14k tokens by itself). An empty temp dir strips the
    /// context AND that suppression, which nets almost nothing.
    ///
    /// Unset (default) keeps the previous behavior of classifying from the
    /// session's own cwd.
    #[serde(default)]
    pub evaluator_cwd: Option<PathBuf>,
}

fn default_preclass_evaluator() -> Vec<String> {
    vec![
        "*haiku*".to_string(),
        "*mini*".to_string(),
        "*flash*".to_string(),
    ]
}

fn default_preclass_timeout_ms() -> u64 {
    15_000
}

/// Generous: a classification is a small prompt returning small JSON, so 90s of
/// total silence means wedged, not thinking. Sized well above a slow non-
/// streaming adapter (which reports nothing until its final chunk) and far
/// below the multi-hour hang this exists to stop.
fn default_preclass_stall_timeout_ms() -> u64 {
    90_000
}

impl Default for PreClassifierConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            evaluator: default_preclass_evaluator(),
            timeout_ms: default_preclass_timeout_ms(),
            stall_timeout_ms: default_preclass_stall_timeout_ms(),
            disclose: true,
            dimensions: Vec::new(),
            evaluator_cwd: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailoverConfig {
    /// Fail over a pinned session to the next best candidate when the
    /// pinned model is rate-limited, down or cordoned. Partial output and
    /// tool statuses transfer as a continuation. Never fails over on cancel.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Minimum seconds between respawn attempts of a dead downstream
    /// process.
    #[serde(default = "default_respawn_cooldown_secs")]
    pub respawn_cooldown_secs: u64,
    /// Maximum candidates tried per prompt (initial + failovers).
    #[serde(default = "default_failover_attempts")]
    pub max_attempts: u32,
    /// When the provider reports the pinned model at capacity, cordon that
    /// model for `capacity_cordon_secs` and fail over to the eligible model
    /// nearest its quality. A human pick moves to that model for the session.
    #[serde(default = "default_true")]
    pub on_model_capacity: bool,
    /// How long a model reported at capacity stays cordoned.
    #[serde(default = "default_capacity_cordon_secs")]
    pub capacity_cordon_secs: u64,
    /// How long a provider outage cordons the failed model across every
    /// account of its lineage. The same model on another account is down too,
    /// so failover moves to a different model until the cordon ends. 0 keeps
    /// only the per-candidate quarantine.
    #[serde(default = "default_outage_cordon_secs")]
    pub outage_cordon_secs: u64,
    /// When no other model can serve a failed turn, the prompt stays open and
    /// the router retries the old model once its cordon ends — if that is
    /// within this many seconds. A longer wait fails the turn as before.
    #[serde(default = "default_max_wait_secs")]
    pub max_wait_secs: u64,
}

fn default_outage_cordon_secs() -> u64 {
    300
}

fn default_max_wait_secs() -> u64 {
    1800
}

fn default_respawn_cooldown_secs() -> u64 {
    30
}

fn default_capacity_cordon_secs() -> u64 {
    900
}

fn default_failover_attempts() -> u32 {
    3
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            respawn_cooldown_secs: default_respawn_cooldown_secs(),
            max_attempts: default_failover_attempts(),
            on_model_capacity: true,
            capacity_cordon_secs: default_capacity_cordon_secs(),
            outage_cordon_secs: default_outage_cordon_secs(),
            max_wait_secs: default_max_wait_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandConfig {
    #[serde(rename = "type", default = "default_stdio")]
    pub kind: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<EnvVarConfig>,
}

fn default_stdio() -> String {
    "stdio".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvVarConfig {
    pub name: String,
    pub value: String,
}

/// Template applied per model for `spawn-config` agents. `${model_id}` in
/// args/env values is replaced with the model id.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessTemplate {
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: serde_yaml::Mapping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ModelSelectionConfig {
    /// Discover the `category: model` config option, then `set_config_option`.
    ConfigOption,
    /// One process target per model; argv/env supplied by config.
    SpawnConfig {
        #[serde(default)]
        process_template: ProcessTemplate,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    /// Optional provider API model id for per-request proxy rewrites. This is
    /// needed when an ACP selector exposes an adapter alias such as
    /// `opus[1m]` that the provider HTTP API does not accept verbatim.
    /// Defaults to `id`.
    #[serde(default)]
    pub api_model: Option<String>,
    /// Older provider versions of this model that `pinned_versions` may
    /// select instead of `api_model`. The candidate, its selector value and
    /// its downstream process stay the same; only the wire model (and the
    /// pricing and scores that follow it) change.
    #[serde(default)]
    pub versions: Vec<ModelVersion>,
    /// 1 = cheapest/least scarce; larger = more expensive/scarce.
    pub cost_rank: u32,
    /// Whether any AUTOMATIC mechanism may choose this candidate. `false`
    /// removes it from every automatic pool — the `auto`/`pareto-code`
    /// strategies, escalation/demotion targets, pre-classifier evaluators,
    /// skill routes, failover, delegate
    /// scoping and per-request proxy routing — while leaving it explicitly
    /// selectable (`router.candidate`, `[router: candidate=…]`,
    /// `[router: switch=…]`): reachable on purpose, never by accident.
    #[serde(default = "default_true")]
    pub auto_eligible: bool,
    /// Optional API-equivalent pricing. It prices every interposed provider
    /// request and synthesizes ACP-turn `cost_usd` only when the adapter
    /// reports no authoritative cost of its own.
    #[serde(default)]
    pub pricing: Option<PricingConfig>,
}

/// One selectable older version of a model (see `ModelConfig::versions`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelVersion {
    /// Provider API model id the proxy puts on the wire, e.g. `claude-opus-4-6`.
    pub api_model: String,
    #[serde(default)]
    pub pricing: Option<PricingConfig>,
}

/// `pinned_versions` value meaning "the model's current `api_model`".
pub const DEFAULT_VERSION: &str = "default";

/// USD per million tokens, mirroring the provider's published API rates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricingConfig {
    pub input_per_mtok: f64,
    pub output_per_mtok: f64,
    /// Cache-read rate; defaults to `0.1 × input_per_mtok` (the common
    /// provider discount) when unset.
    #[serde(default)]
    pub cache_read_per_mtok: Option<f64>,
    /// Cache-write rate; defaults to `1.25 × input_per_mtok` when unset.
    #[serde(default)]
    pub cache_write_per_mtok: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub name: String,
    pub command: CommandConfig,
    pub model_selection: ModelSelectionConfig,
    #[serde(default = "default_budget")]
    pub budget_prompts_5h: u32,
    pub models: Vec<ModelConfig>,
    /// Tie-break preference added directly to this agent's utility in the
    /// `auto` strategy (and used as a tie-break within a `pareto-code`
    /// tier). Keep it small — ~0.05 prefers this agent when candidates are
    /// otherwise comparable without overriding real quality differences.
    #[serde(default)]
    pub preference: f64,
    /// Optional translation from client-requested session mode ids to this
    /// agent's mode ids (e.g. goose's `auto` -> claude's
    /// `bypassPermissions`). Applied when deferring a pre-pin
    /// `session/set_mode` to the pinned downstream and when relaying
    /// post-pin mode changes.
    #[serde(default)]
    pub mode_map: std::collections::HashMap<String, String>,
    /// Optional provider usage source for proactive cordoning (see
    /// `CordonConfig`). When set, the router periodically reads this provider's
    /// usage caps and cordons this agent's candidates that are exhausted.
    #[serde(default)]
    pub usage_source: Option<UsageSourceConfig>,
    /// Included-plan percentages kept unused, independently for this account.
    #[serde(default)]
    pub reserve_capacity: ReserveCapacityConfig,
    /// Lower numbers drain first within one adapter's account group.
    /// Unset standalone agents keep ordinary strategy ranking.
    #[serde(default)]
    pub account_priority: Option<u32>,
    /// Optional human-facing plan when local provider metadata omits it.
    #[serde(default)]
    pub account_plan: Option<String>,
    /// Deleted membership keeps an adapter template for adding a new login.
    #[serde(default)]
    pub account_disabled: bool,
    /// Codex adapters only: run every enabled hook without Codex's persisted
    /// hook-trust review, the same override as `codex --dangerously-bypass-hook-trust`.
    /// Codex skips hooks it has not trusted, and a delegate's fresh worktree is
    /// never a trusted path, so without this a repository's guard hooks do not
    /// run for delegated work. Off unless set.
    #[serde(default)]
    pub bypass_hook_trust: bool,
    /// Most delegated sub-sessions this agent runs at once, beside the global
    /// `delegation.max_concurrent`. Each account seat gets its own limit, so
    /// adding accounts or agents adds delegate capacity. Unset: only the
    /// global limit applies.
    #[serde(default)]
    pub max_delegates: Option<usize>,
    /// Independently authenticated instances of this adapter. Expanded into
    /// `name@account` agents before validation; empty keeps the original agent.
    #[serde(default)]
    pub accounts: Vec<AccountConfig>,
    /// Optional non-interactive provider login probe. Exit zero is positive
    /// authentication evidence. A non-zero result is negative evidence only
    /// when its output matches `unauthenticated_patterns`; every other error
    /// and timeout is unknown (fail open).
    #[serde(default)]
    pub auth_probe: Option<AuthProbeConfig>,
    /// Optional login CLI override. Omitted uses the provider's login command.
    /// It inherits this account's isolated environment, never the probe argv.
    #[serde(default)]
    pub login_command: Option<CommandConfig>,
    /// Model-company lineage tag (e.g. `anthropic`, `openai`). Defaults to the
    /// agent name. Reported with lifecycle-hook events so a host can tell which
    /// company a worker runs on; two agents backed by the same vendor (e.g. two
    /// Claude seats) should declare the same `lineage` (account seats share it
    /// automatically).
    #[serde(default)]
    pub lineage: Option<String>,
    /// Optional per-request LLM proxy interposition for this adapter. The
    /// global `llm_proxy.enabled` switch must also be on. The upstream is
    /// explicit because subscription/OAuth endpoints often differ from the
    /// providers' public API endpoints.
    #[serde(default)]
    pub llm_proxy: Option<AgentLlmProxyConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveCapacityConfig {
    #[serde(default)]
    pub weekly: f64,
    #[serde(default)]
    pub session: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveCapacityOverride {
    pub weekly: Option<f64>,
    pub session: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountConfig {
    pub name: String,
    #[serde(default)]
    pub priority: Option<u32>,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub env: Vec<EnvVarConfig>,
    #[serde(default)]
    pub reserve_capacity: Option<ReserveCapacityOverride>,
    #[serde(default)]
    pub account_plan: Option<String>,
}

impl AgentConfig {
    /// Match the adapter's environment without mutating the router process.
    pub fn env_var(&self, name: &str) -> Option<String> {
        self.command
            .env
            .iter()
            .rev()
            .find(|v| v.name == name)
            .map(|v| v.value.clone())
            .or_else(|| inherited_var(name))
    }

    pub fn config_dir(&self, variable: &str, default: &str) -> Option<PathBuf> {
        self.env_var(variable)
            .filter(|v| !v.is_empty())
            .map(|v| PathBuf::from(expand_tilde_str(&v)))
            .or_else(|| {
                self.env_var("HOME")
                    .map(|home| PathBuf::from(home).join(default))
            })
    }
}

/// Unit tests give each agent its whole environment in config. A provider
/// directory inherited from the test process (`cargo test` run by a
/// router-spawned agent) would point them at a live credential store.
fn inherited_var(name: &str) -> Option<String> {
    if cfg!(test) && crate::accounts::PROVIDER_DIRECTORY_ENV.contains(&name) {
        return None;
    }
    std::env::var(name).ok()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthProbeConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default = "default_auth_probe_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_auth_failure_patterns")]
    pub unauthenticated_patterns: Vec<String>,
}

fn default_auth_probe_timeout_ms() -> u64 {
    2_000
}

fn default_auth_failure_patterns() -> Vec<String> {
    vec![
        "not logged in".to_string(),
        "not signed in".to_string(),
        "authentication required".to_string(),
        "sign in".to_string(),
        "log in".to_string(),
        "login required".to_string(),
    ]
}

fn default_budget() -> u32 {
    400
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct StaticRouterConfig {
    /// `agent/model-id` of the default candidate.
    #[serde(default)]
    pub candidate: Option<String>,
    /// If true, fall back to remaining candidates in config order when the
    /// chosen candidate is not routeable.
    #[serde(default)]
    pub allow_fallback: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoRouterConfig {
    /// OpenRouter scale: 0 = pure quality, 10 = cheapest surviving candidate.
    #[serde(default = "default_tradeoff")]
    pub cost_quality_tradeoff: f64,
    #[serde(default = "default_complexity_floor")]
    pub complexity_floor: f64,
    #[serde(default = "default_allowed")]
    pub allowed_candidates: Vec<String>,
    /// Scale the tradeoff down as classified complexity rises
    /// (`effective = tradeoff × (1 − complexity)`): cost/scarcity matters
    /// for trivial prompts, quality dominates hard ones. Default on.
    #[serde(default = "default_true")]
    pub complexity_scales_tradeoff: bool,
    /// Floor on the cost weight (0..1 share of the utility) after complexity
    /// scaling. Without it, a complexity-1.0 classification zeroes the cost
    /// term entirely and routing degenerates to pure quality-max — every
    /// prompt lands on the most expensive candidate. The floor keeps a
    /// minimum of cost-awareness in play; it never raises the weight above
    /// the configured `cost_quality_tradeoff`, and 0 disables it (legacy
    /// behavior). Only applies when `complexity_scales_tradeoff` is on.
    #[serde(default = "default_min_cost_weight")]
    pub min_cost_weight: f64,
    /// Classified complexity at/above which ranking goes PURE QUALITY
    /// (tradeoff-0 semantics: cost term dropped, demand cap bypassed, raw
    /// benchmark scores decide). This is the break-glass complement to the
    /// score table's compressed peer pairs: everyday and hard work resolves
    /// tie-breaks toward the cheaper peer, but at the extreme tail —
    /// novel/severe-risk work, a fix that has survived multiple failed
    /// rounds — the preferred member of a pair (Fable over Opus, Sol over
    /// Terra) must remain reachable by `auto` itself, not only via explicit
    /// pins and planner globs. The misrouting cost of a failed frontier task
    /// dwarfs the seat-price difference out there. Values above 1.0 disable
    /// (classified complexity is clamped to 1.0).
    #[serde(default = "default_apex_complexity")]
    pub apex_complexity: f64,
}

fn default_min_cost_weight() -> f64 {
    0.15
}

fn default_apex_complexity() -> f64 {
    0.9
}

fn default_tradeoff() -> f64 {
    7.0
}

fn default_complexity_floor() -> f64 {
    0.7
}

fn default_allowed() -> Vec<String> {
    vec!["*".to_string()]
}

impl Default for AutoRouterConfig {
    fn default() -> Self {
        Self {
            cost_quality_tradeoff: default_tradeoff(),
            complexity_floor: default_complexity_floor(),
            allowed_candidates: default_allowed(),
            complexity_scales_tradeoff: true,
            min_cost_weight: default_min_cost_weight(),
            apex_complexity: default_apex_complexity(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct ParetoCodeRouterConfig {
    /// Mapped to a coding tier by OpenRouter semantics:
    /// >= 0.66 high, >= 0.33 medium, < 0.33 low. Omitted means high.
    #[serde(default)]
    pub min_coding_score: Option<f64>,
}

/// `escalation` router: start on the cheapest capable candidate and escalate
/// to a stronger one only when *observed execution* reveals hidden difficulty
/// (heavy investigation, tool-failure churn, token exhaustion, refusals).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EscalationRouterConfig {
    /// How far each escalation jumps (`ladder` = next tier up, `leap` =
    /// straight to the strongest eligible candidate).
    #[serde(default)]
    pub escalation_path: EscalationPath,
    /// Delegate the *starting* candidate choice to another router (`auto`,
    /// `pareto-code`, `static`) instead of picking the cheapest. Escalation
    /// still applies at runtime from whatever it picks. `None` = start cheapest.
    /// (Must not be `escalation` itself.)
    #[serde(default)]
    pub initial_router: Option<StrategyKind>,
    /// Escalate mid-turn — while the cheap model is still only *investigating*
    /// (no output streamed and no write/exec tool call yet) — instead of only
    /// after the turn completes. The post-turn triggers (below) always apply;
    /// this adds the read-volume trigger inside the pre-side-effect window.
    #[serde(default = "default_true")]
    pub escalate_before_side_effects: bool,
    /// Optional floor on the *starting* candidate's class quality (0 = none),
    /// so the router won't begin on a model too weak to make any headway.
    #[serde(default)]
    pub min_start_score: f64,
    /// Escalate after this many investigation events (file reads / searches)
    /// in a turn. `0` disables the read-volume trigger.
    #[serde(default = "default_escalate_after_reads")]
    pub escalate_after_reads: u32,
    /// Escalate mid-turn once a single turn has issued this many tool calls
    /// without finishing — the "grinding / in over its head" signal, robust to
    /// how the model interleaves reads and edits (unlike `escalate_after_reads`,
    /// which only counts investigation before the first side effect). `0`
    /// disables. Not gated on side effects: the handoff is a transcript
    /// *continue*, so the stronger model picks up where the cheap one left off.
    #[serde(default = "default_escalate_after_tool_calls")]
    pub escalate_after_tool_calls: u32,
    /// Escalate after this many *failed* tool calls in a turn. `0` disables.
    #[serde(default = "default_escalate_after_tool_failures")]
    pub escalate_after_tool_failures: u32,
    /// Escalate when a turn ends with a max-tokens stop reason.
    #[serde(default = "default_true")]
    pub escalate_on_max_tokens: bool,
    /// Escalate when a turn ends with a refusal stop reason.
    #[serde(default = "default_true")]
    pub escalate_on_refusal: bool,
    /// Hard cap on escalations per session (bounds ladder thrash).
    #[serde(default = "default_max_escalations")]
    pub max_escalations: u32,
}

fn default_escalate_after_reads() -> u32 {
    6
}

fn default_escalate_after_tool_calls() -> u32 {
    30
}

fn default_escalate_after_tool_failures() -> u32 {
    3
}

fn default_max_escalations() -> u32 {
    3
}

impl Default for EscalationRouterConfig {
    fn default() -> Self {
        Self {
            escalation_path: EscalationPath::default(),
            initial_router: None,
            escalate_before_side_effects: true,
            min_start_score: 0.0,
            escalate_after_reads: default_escalate_after_reads(),
            escalate_after_tool_calls: default_escalate_after_tool_calls(),
            escalate_after_tool_failures: default_escalate_after_tool_failures(),
            escalate_on_max_tokens: true,
            escalate_on_refusal: true,
            max_escalations: default_max_escalations(),
        }
    }
}

/// Per-model additive score boost for the `planner` router, applied only in
/// the designated phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerModelBoost {
    /// Candidate glob (e.g. `*grok*`).
    pub pattern: String,
    /// Additive utility boost during the `Implementation` phase.
    #[serde(default)]
    pub implementation: f64,
    /// Additive utility boost during the `Planning` phase.
    #[serde(default)]
    pub planning: f64,
}

fn default_planning_candidates() -> Vec<String> {
    // Everyday planning is Opus (currently 5.5) and Sol. Fable/Astra stay in
    // the pool so auto's apex_complexity (pure quality) can reach them for
    // exceptionally hard plans. Ranking is auto, not glob order.
    vec![
        "*opus*".to_string(),
        "*sol*".to_string(),
        "*astra*".to_string(),
        "*fable*".to_string(),
    ]
}

fn default_implementation_candidates() -> Vec<String> {
    vec![
        "*sol*".to_string(),
        "*opus*".to_string(),
        "*grok*".to_string(),
    ]
}

fn default_phase_upgrade_confidence() -> f64 {
    0.7
}

fn default_planner_apex_complexity() -> f64 {
    0.85
}

fn default_planner_floor_complexity() -> f64 {
    0.15
}

fn default_stuck_turn_secs() -> u64 {
    600
}

fn default_stuck_turn_cancel_secs() -> u64 {
    60
}

fn default_easy_planning_candidates() -> Vec<String> {
    vec!["*opus*".to_string(), "*sol*".to_string()]
}

fn default_hard_planning_candidates() -> Vec<String> {
    vec!["*astra*".to_string(), "*fable*".to_string()]
}

/// Two-phase routing: separate candidate pools for planning (frontier) and
/// implementation (workhorse), with complexity-gated crossover and per-model
/// score boosts. Ranking within each pool delegates to `AutoStrategy`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerRouterConfig {
    /// `markdown` uses the bundled repository-neutral workflow. Any other
    /// value is a Markdown policy file, never an inferred organization or an
    /// implicitly selected ticket service: an absolute path is host-installed
    /// and serves every session repository; a relative path resolves inside
    /// the session repository.
    #[serde(default)]
    pub profile: Option<String>,
    /// Optional absolute host-installed skill root of `<name>/SKILL.md`
    /// files. A role skill the session repository lacks resolves here before
    /// the bundled fallback, so explicit mappings work in every repository.
    #[serde(default)]
    pub skills: Option<PathBuf>,
    /// Optional repository-relative roadmap path. No path means selection
    /// must resolve the repository's convention before admitting work.
    #[serde(default)]
    pub roadmap: Option<PathBuf>,
    #[serde(default, rename = "create-plan")]
    pub create_plan: PlannerSkillConfig,
    #[serde(default, rename = "select-plan")]
    pub select_plan: PlannerSkillConfig,
    #[serde(default, rename = "implement-work")]
    pub implement_work: PlannerSkillConfig,
    #[serde(default, rename = "review-work")]
    pub review_work: PlannerSkillConfig,
    #[serde(default, rename = "finish-work")]
    pub finish_work: PlannerSkillConfig,
    #[serde(default, rename = "integrate-plan")]
    pub integrate_plan: PlannerSkillConfig,
    /// Repository-owned allocator. It receives a JSON assignment on stdin
    /// and returns {"path": "/isolated/workspace", "lease": "opaque-id"}.
    /// Omitted allocation uses isolated local clones, never shared writes.
    #[serde(default)]
    pub workspace: Option<PlannerWorkspaceConfig>,
    /// Repository-owned capacity host. While the parent is below
    /// `delegation.max_concurrent`, the router asks it for admissible
    /// candidates and wakes the parent even during coordination. If the idle
    /// parent does not respond, it admits and starts them itself. Omitted keeps
    /// only the 20-minute heartbeat.
    #[serde(default)]
    pub capacity: Option<PlannerCapacityConfig>,
    /// Repository-owned run auditor (the native watcher's guarantees): its
    /// defects wake the idle parent, gate the parent's turn end and refuse
    /// `complete`; a `hold_check` verifies each `hold`. See
    /// `crate::planner_guard`.
    #[serde(default)]
    pub audit: Option<PlannerAuditConfig>,
    /// Repository-owned check of every `integrate`, and of a `disposition` to
    /// blocked/watching: exit 0 accepts, exit 2 rejects with its stderr.
    #[serde(default)]
    pub integrate_check: Option<PlannerHostConfig>,
    /// `_router/planner_shutdown`: the resume-state host and the per-worker
    /// drain bound. See `crate::planner_shutdown`.
    #[serde(default)]
    pub shutdown: Option<PlannerShutdownConfig>,
    /// Candidate globs for the planning (frontier) phase.
    #[serde(default = "default_planning_candidates")]
    pub planning_candidates: Vec<String>,
    /// Candidate globs for the implementation (workhorse) phase.
    #[serde(default = "default_implementation_candidates")]
    pub implementation_candidates: Vec<String>,
    /// Per-model additive score boosts, applied only in the matching phase.
    #[serde(default)]
    pub model_boosts: Vec<PlannerModelBoost>,
    /// Retired: the pre-classifier no longer changes the planner phase.
    /// Accepted so existing configs still parse; the value is ignored.
    #[serde(default = "default_phase_upgrade_confidence")]
    pub phase_upgrade_confidence: f64,
    /// Complexity at or above which implementation-phase routing also admits
    /// planning candidates (frontier for genuinely hard execution work).
    #[serde(default = "default_planner_apex_complexity")]
    pub apex_complexity: f64,
    /// Complexity at or below which planning-phase routing also admits
    /// implementation candidates (workhorse for trivial planning).
    #[serde(default = "default_planner_floor_complexity")]
    pub floor_complexity: f64,
    /// Planning-pool subset for an `easy:` prefix. Intersected with
    /// `planning_candidates`; empty intersection falls back to the full pool.
    #[serde(default = "default_easy_planning_candidates")]
    pub easy_planning_candidates: Vec<String>,
    /// Planning-pool subset for a `hard:` prefix. Same intersection rule.
    #[serde(default = "default_hard_planning_candidates")]
    pub hard_planning_candidates: Vec<String>,
    /// Opaque host-owned instructions injected into the agent's context when
    /// the session enters the planning phase. The router does not interpret
    /// this text; workflow policy (ticket decomposition, session-spawning
    /// protocol, coordination rules) belongs to the host.
    #[serde(default)]
    pub planning_instructions: String,
    /// A planner parent or child turn with no downstream progress (no
    /// session update, tool call or LLM response) for this many seconds is
    /// stalled: the router cancels it and, if it does not end within
    /// `stuck_turn_cancel_secs`, releases it and restarts its adapter.
    /// `0` disables the watchdog. See `crate::planner_watchdog`.
    #[serde(default = "default_stuck_turn_secs")]
    pub stuck_turn_secs: u64,
    /// How long a stalled turn may take to end after its `session/cancel`.
    #[serde(default = "default_stuck_turn_cancel_secs")]
    pub stuck_turn_cancel_secs: u64,
}

impl Default for PlannerRouterConfig {
    fn default() -> Self {
        Self {
            profile: None,
            skills: None,
            roadmap: None,
            create_plan: PlannerSkillConfig::default(),
            select_plan: PlannerSkillConfig::default(),
            implement_work: PlannerSkillConfig::default(),
            review_work: PlannerSkillConfig::default(),
            finish_work: PlannerSkillConfig::default(),
            integrate_plan: PlannerSkillConfig::default(),
            workspace: None,
            capacity: None,
            audit: None,
            integrate_check: None,
            shutdown: None,
            planning_candidates: default_planning_candidates(),
            implementation_candidates: default_implementation_candidates(),
            model_boosts: Vec::new(),
            phase_upgrade_confidence: default_phase_upgrade_confidence(),
            apex_complexity: default_planner_apex_complexity(),
            floor_complexity: default_planner_floor_complexity(),
            easy_planning_candidates: default_easy_planning_candidates(),
            hard_planning_candidates: default_hard_planning_candidates(),
            planning_instructions: String::new(),
            stuck_turn_secs: default_stuck_turn_secs(),
            stuck_turn_cancel_secs: default_stuck_turn_cancel_secs(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerSkillConfig {
    pub skill: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerWorkspaceConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default = "default_lifecycle_hook_timeout_ms")]
    pub timeout_ms: u64,
}

/// `routers.planner.capacity`: launched like `workspace` (cwd = the session
/// repository, JSON on stdin, JSON on stdout), plus the wake and
/// auto-admission timing. See `crate::planner_capacity`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerCapacityConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default = "default_lifecycle_hook_timeout_ms")]
    pub timeout_ms: u64,
    /// Seconds of parent idleness with a free slot and an unheld candidate
    /// before a capacity wake, and between repeated wakes.
    #[serde(default = "default_capacity_wake_after_secs")]
    pub wake_after_secs: u64,
    /// Capacity wakes for one unfilled condition after which the router
    /// admits and starts the candidates itself.
    #[serde(default = "default_capacity_auto_admit_after_wakes")]
    pub auto_admit_after_wakes: u32,
    /// Parent idleness with a free slot after which the router admits and
    /// starts the candidates itself, whatever the wake count.
    #[serde(default = "default_capacity_auto_admit_after_secs")]
    pub auto_admit_after_secs: u64,
    /// Minimum spacing of the in-flight wakes an idle parent gets while a
    /// release or running work is mid-flight (the first comes after
    /// `wake_after_secs` of idleness).
    #[serde(default = "default_capacity_inflight_wake_secs")]
    pub inflight_wake_secs: u64,
}

impl PlannerCapacityConfig {
    /// Timing used when no capacity host is configured: worker resumption and
    /// in-flight wakes still run on these defaults.
    pub fn timing_defaults() -> Self {
        Self {
            command: String::new(),
            args: Vec::new(),
            timeout_ms: default_lifecycle_hook_timeout_ms(),
            wake_after_secs: default_capacity_wake_after_secs(),
            auto_admit_after_wakes: default_capacity_auto_admit_after_wakes(),
            auto_admit_after_secs: default_capacity_auto_admit_after_secs(),
            inflight_wake_secs: default_capacity_inflight_wake_secs(),
        }
    }
}

fn default_capacity_wake_after_secs() -> u64 {
    60
}

fn default_capacity_auto_admit_after_wakes() -> u32 {
    2
}

fn default_capacity_auto_admit_after_secs() -> u64 {
    300
}

fn default_capacity_inflight_wake_secs() -> u64 {
    300
}

/// A repository host launched like `workspace`: no shell, cwd = the session
/// repository, one JSON object on stdin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerHostConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default = "default_lifecycle_hook_timeout_ms")]
    pub timeout_ms: u64,
}

/// `routers.planner.audit`. See `crate::planner_guard`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerAuditConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// One audit fetches; it can take 10–15 s.
    #[serde(default = "default_planner_host_slow_timeout_ms")]
    pub timeout_ms: u64,
    /// The capacity tick audits at most this often (plus at every parent
    /// turn end).
    #[serde(default = "default_audit_interval_secs")]
    pub interval_secs: u64,
}

/// `routers.planner.shutdown`. See `crate::planner_shutdown`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannerShutdownConfig {
    /// The resume-state host; without one nothing is saved, and the drain
    /// still runs.
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default = "default_planner_host_slow_timeout_ms")]
    pub timeout_ms: u64,
    /// How long one live worker gets to commit, push and stop before it is
    /// cancelled.
    #[serde(default = "default_drain_timeout_secs")]
    pub drain_timeout_secs: u64,
}

impl Default for PlannerShutdownConfig {
    fn default() -> Self {
        Self {
            command: None,
            args: Vec::new(),
            timeout_ms: default_planner_host_slow_timeout_ms(),
            drain_timeout_secs: default_drain_timeout_secs(),
        }
    }
}

fn default_planner_host_slow_timeout_ms() -> u64 {
    120_000
}

fn default_audit_interval_secs() -> u64 {
    60
}

fn default_drain_timeout_secs() -> u64 {
    900
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutersConfig {
    #[serde(rename = "static", default)]
    pub static_: StaticRouterConfig,
    #[serde(default)]
    pub auto: AutoRouterConfig,
    #[serde(rename = "pareto-code", default)]
    pub pareto_code: ParetoCodeRouterConfig,
    #[serde(default)]
    pub escalation: EscalationRouterConfig,
    #[serde(default)]
    pub planner: PlannerRouterConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Original file for router-owned account management. Never a YAML key.
    #[serde(skip)]
    pub source_path: Option<PathBuf>,
    #[serde(default = "default_router")]
    pub router: StrategyKind,
    #[serde(default = "default_state_file")]
    pub state_file: PathBuf,
    /// How long to keep sessions in the state database before auto-pruning
    /// (and their logs, by cascade). Duration string: `30d`, `12h`, `90m`,
    /// `3600s`, or a bare number of days. Default `30d`.
    #[serde(default = "default_history")]
    pub history: String,
    /// Optional path to a score-table YAML overriding the built-in table.
    #[serde(default)]
    pub score_table: Option<PathBuf>,
    #[serde(default)]
    pub disclosure: DisclosureMode,
    #[serde(default)]
    pub classifier: ClassifierConfig,
    #[serde(default)]
    pub delegation: DelegationConfig,
    #[serde(default)]
    pub headroom: HeadroomConfig,
    /// Proactive usage-cap cordoning (default on; inert unless an agent has a
    /// `usage_source`).
    #[serde(default)]
    pub cordon: CordonConfig,
    /// Dynamic preference scaling from seat availability (default on; inert
    /// until a usage poll or a client hint reports availability).
    #[serde(default)]
    pub availability_preference: AvailabilityPreferenceConfig,
    #[serde(default)]
    pub failover: FailoverConfig,
    #[serde(default)]
    pub auto_upgrade: AutoUpgradeConfig,
    /// Skill → model-class routing rules.
    #[serde(default)]
    pub skill_routing: Vec<SkillRoute>,
    /// Version pinning: `agent/model-id` → `default` or one of that model's
    /// `versions[].api_model`. Routing never sees it — the candidate stays
    /// `agent/model-id` everywhere — and the proxy sends the pinned version on
    /// the wire, priced and scored as that version.
    #[serde(default)]
    pub pinned_versions: HashMap<String, String>,
    /// Ticket-reference → context-loading rules (prefix + fetch command).
    #[serde(default)]
    pub ticket_context: Vec<TicketRule>,
    /// Removed: auto-orchestration was superseded by the `planner` router.
    /// Accepted only so a stale `orchestration:` block fails validation with a
    /// pointer to its replacement instead of a bare unknown-field error.
    #[serde(default, skip_serializing)]
    pub orchestration: Option<serde_yaml::Value>,
    /// Reasoning-effort policy for primary sessions.
    #[serde(default)]
    pub effort: EffortConfig,
    /// Composable LLM pre-classifier (task class, complexity, host dimensions).
    #[serde(default)]
    pub pre_classifier: PreClassifierConfig,
    /// Expiry of elevated pins (escalations, auto-upgrades, skill pins):
    /// demote back to a cheaper candidate after a run of quiet turns.
    #[serde(default)]
    pub demotion: DemotionConfig,
    /// Per-LLM-request routing proxy. Disabled by default until each adapter's
    /// base-URL override and upstream endpoint are explicitly configured.
    #[serde(default)]
    pub llm_proxy: LlmProxyConfig,
    pub agents: Vec<AgentConfig>,
    #[serde(default)]
    pub routers: RoutersConfig,
    /// Timeout for downstream initialize/probe calls, in milliseconds.
    #[serde(default = "default_probe_timeout_ms")]
    pub probe_timeout_ms: u64,
    /// Fallback credential for project MCP servers the router has no sign-in
    /// for: a host helper that supplies a bearer (`mcp_broker::supply`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_credential_command: Option<McpCredentialCommand>,
    /// Project-selected MCP identities, keyed by resource URL (or server name).
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub mcp_accounts: std::collections::HashMap<String, McpAccountConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpAccountConfig {
    /// None explicitly selects the shared system login.
    pub account: Option<String>,
}

/// A helper process that supplies a bearer for one MCP server. It reads
/// `{name, url, resource}` on stdin and prints `{token, expiresAt?}`, or exits
/// non-zero / prints nothing to decline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpCredentialCommand {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// A helper still running after this long has declined.
    #[serde(default = "default_mcp_credential_timeout_ms")]
    pub timeout_ms: u64,
    /// The `.mcp.json` server names the helper serves; empty serves every
    /// brokered server. A covered server always reaches sessions, so name
    /// only the ones the helper can actually supply.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub servers: Vec<String>,
}

impl McpCredentialCommand {
    pub fn covers(&self, server: &str) -> bool {
        self.servers.is_empty() || self.servers.iter().any(|s| s == server)
    }
}

fn default_mcp_credential_timeout_ms() -> u64 {
    5_000
}

/// HTTP wire shape used by an interposed adapter. Both OpenAI Responses and
/// Chat Completions are covered by `openai`; endpoint paths are passed through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LlmWireProtocol {
    Anthropic,
    Openai,
}

/// Process-level interposition details for one ACP adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentLlmProxyConfig {
    pub protocol: LlmWireProtocol,
    /// Environment variable the adapter reads for its inference base URL
    /// (`ANTHROPIC_BASE_URL`, `OPENAI_BASE_URL`, `KIMI_BASE_URL`, ...).
    pub base_url_env: String,
    /// The real provider/subscription endpoint. Its origin and path are
    /// preserved; only the request body's top-level `model` field may change.
    pub upstream_base_url: String,
    /// Codex ChatGPT OAuth ignores `OPENAI_BASE_URL` for its preferred
    /// WebSocket transport. Install an HTTP Responses custom provider into
    /// `CODEX_CONFIG`/`MODEL_PROVIDER` so inference actually crosses this
    /// proxy while retaining the seat's ChatGPT authentication.
    #[serde(default)]
    pub codex_chatgpt_provider: bool,
}

/// Policy and listener settings for per-request routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmProxyConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Must resolve to a loopback socket. Port 0 asks the OS for a free port.
    #[serde(default = "default_llm_proxy_listen")]
    pub listen: String,
    /// DEPRECATED and ignored. The per-request cost policy (routine demotion,
    /// difficulty escalation, verdict expiry, dwell) was removed; requests stay
    /// on the session pin unless it is out of plan budget. Accepted, unused, so
    /// existing configs still load; `deprecated_keys` reports them.
    #[serde(default, skip_serializing)]
    pub routine_streak: Option<serde_yaml::Value>,
    #[serde(default, skip_serializing)]
    pub minimum_dwell_requests: Option<serde_yaml::Value>,
    #[serde(default, skip_serializing)]
    pub verdict_ttl_requests: Option<serde_yaml::Value>,
    #[serde(default, skip_serializing)]
    pub verdict_ttl_secs: Option<serde_yaml::Value>,
    /// Fraction of a target model's configured context window that a request
    /// may occupy. An out-of-budget pin's failover target must fit it, and a
    /// target with an unknown window is refused.
    #[serde(default = "default_llm_context_window_fraction")]
    pub context_window_fraction: f64,
    /// Maximum buffered JSON request size. Responses remain streamed.
    #[serde(default = "default_llm_max_request_bytes")]
    pub max_request_bytes: usize,
    /// Maximum response bytes retained for usage/stop-reason inspection while
    /// the complete response continues streaming to the adapter.
    #[serde(default = "default_llm_max_capture_bytes")]
    pub max_capture_bytes: usize,
}

fn default_llm_proxy_listen() -> String {
    "127.0.0.1:0".to_string()
}

fn default_llm_context_window_fraction() -> f64 {
    0.9
}

fn default_llm_max_request_bytes() -> usize {
    32 * 1024 * 1024
}

fn default_llm_max_capture_bytes() -> usize {
    4 * 1024 * 1024
}

impl LlmProxyConfig {
    /// Removed per-request policy keys still present in the config, for a
    /// load-time warning.
    pub fn deprecated_keys(&self) -> Vec<&'static str> {
        [
            ("llm_proxy.routine_streak", self.routine_streak.is_some()),
            (
                "llm_proxy.minimum_dwell_requests",
                self.minimum_dwell_requests.is_some(),
            ),
            (
                "llm_proxy.verdict_ttl_requests",
                self.verdict_ttl_requests.is_some(),
            ),
            (
                "llm_proxy.verdict_ttl_secs",
                self.verdict_ttl_secs.is_some(),
            ),
        ]
        .into_iter()
        .filter_map(|(key, set)| set.then_some(key))
        .collect()
    }
}

impl Default for LlmProxyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: default_llm_proxy_listen(),
            routine_streak: None,
            minimum_dwell_requests: None,
            verdict_ttl_requests: None,
            verdict_ttl_secs: None,
            context_window_fraction: default_llm_context_window_fraction(),
            max_request_bytes: default_llm_max_request_bytes(),
            max_capture_bytes: default_llm_max_capture_bytes(),
        }
    }
}

fn default_router() -> StrategyKind {
    StrategyKind::Auto
}

/// Demotion is the counterpart of escalation/auto-upgrade: an *elevated* pin
/// (one the router raised for a skill, an escalation, or an auto-upgrade —
/// never an explicit user pick) expires after enough quiet turns, and the
/// session steps back down to the strongest cheaper candidate. Without it,
/// one hard patch pins a long session (e.g. a ship watcher's CI-poll loop)
/// to frontier pricing forever.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DemotionConfig {
    /// Demote after this many consecutive turns with no struggle signals
    /// (no tool failures, token-ceiling hits, or refusals). 0 disables
    /// demotion (the default). The switch uses the normal handoff-summary
    /// machinery and is disclosed like any other switch; re-escalation on
    /// fresh difficulty still applies (bounded by `max_escalations`).
    #[serde(default)]
    pub after_quiet_turns: u32,
}

fn default_state_file() -> PathBuf {
    PathBuf::from("~/.local/state/router-acp/sessions.db")
}

fn default_probe_timeout_ms() -> u64 {
    120_000
}

/// Interpolate `$VAR` and `${VAR}` environment references in a string. Unknown
/// variables are left intact so later per-model substitution (e.g.
/// `${model_id}` in spawn-config process templates) still sees them.
pub fn interpolate_env(input: &str) -> String {
    interpolate(input, &|name| std::env::var(name).ok())
}

/// Interpolate `$name` and `${name}` references using the provided lookup.
/// Bare names use shell identifier boundaries. Unknown references and `$$`
/// stay intact; inserted values are never expanded again.
pub fn interpolate(input: &str, lookup: &dyn Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find('$') {
        out.push_str(&rest[..start]);
        rest = &rest[start..];
        if let Some(after) = rest.strip_prefix("$$") {
            out.push_str("$$");
            rest = after;
            continue;
        }
        let (name, end) = if let Some(after) = rest.strip_prefix("${") {
            let Some(end) = after.find('}') else {
                break;
            };
            (&after[..end], end + 3)
        } else {
            let len = rest.as_bytes()[1..]
                .iter()
                .enumerate()
                .take_while(|(i, b)| {
                    b.is_ascii_alphabetic() || **b == b'_' || (*i > 0 && b.is_ascii_digit())
                })
                .count();
            if len == 0 {
                out.push('$');
                rest = &rest[1..];
                continue;
            }
            (&rest[1..1 + len], 1 + len)
        };
        match lookup(name) {
            Some(value) => out.push_str(&value),
            None => out.push_str(&rest[..end]),
        }
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

/// Expand a leading `~` to the user's home directory.
pub fn expand_tilde(path: &Path) -> PathBuf {
    if let Ok(stripped) = path.strip_prefix("~")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(stripped);
    }
    path.to_path_buf()
}

/// Expand a leading `~`/`~/` in a string that names a path. Unlike
/// [`expand_tilde`], operates on strings (command paths and args) and only
/// touches a `~` that is the whole value or is followed by a separator — a
/// bare `~word` (e.g. another user's home, which we can't resolve) is left
/// intact. Downstream commands are spawned via `Command::new`, which does NOT
/// invoke a shell, so an unexpanded `~` would be treated as a literal path
/// component and the spawn would fail.
pub fn expand_tilde_str(s: &str) -> String {
    if s == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return home.to_string_lossy().into_owned();
        }
        return s.to_string();
    }
    if let Some(rest) = s.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return format!("{}/{rest}", home.to_string_lossy());
    }
    s.to_string()
}

/// A serialized agent with only its required keys set: every other value is
/// the field's default.
pub fn agent_defaults() -> serde_yaml::Value {
    let agent: AgentConfig = serde_yaml::from_str(
        "{ name: x, command: { type: stdio, command: x }, model_selection: { type: config-option }, models: [] }",
    )
    .expect("the minimal agent is valid");
    serde_yaml::to_value(agent).expect("an agent serializes")
}

/// The first paths (up to ten) where two resolved configurations differ.
fn differences(a: &serde_yaml::Value, b: &serde_yaml::Value, at: String, out: &mut Vec<String>) {
    use serde_yaml::Value;
    if out.len() >= 10 || a == b {
        return;
    }
    match (a, b) {
        (Value::Mapping(x), Value::Mapping(y)) => {
            for key in x.keys().chain(y.keys().filter(|k| !x.contains_key(*k))) {
                let name = key
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("{key:?}"));
                let null = Value::Null;
                differences(
                    x.get(key).unwrap_or(&null),
                    y.get(key).unwrap_or(&null),
                    format!("{at}.{name}"),
                    out,
                );
            }
        }
        (Value::Sequence(x), Value::Sequence(y)) if x.len() == y.len() => {
            for (i, (p, q)) in x.iter().zip(y).enumerate() {
                differences(p, q, format!("{at}[{i}]"), out);
            }
        }
        _ => out.push(if at.is_empty() { ".".into() } else { at }),
    }
}

/// A configuration error with an actionable message.
#[derive(Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "config error: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// The configuration a host presents as editable defaults: provider
    /// presets and serde defaults applied, without environment interpolation.
    /// The uninterpolated form is deliberate because this value may cross a
    /// process or HTTP boundary.
    pub fn effective_document(yaml: &str) -> Result<serde_json::Value, ConfigError> {
        let invalid = |e: serde_yaml::Error| ConfigError(format!("invalid YAML: {e}"));
        let mut document: serde_yaml::Value = serde_yaml::from_str(yaml).map_err(invalid)?;
        crate::presets::apply(&mut document);
        let config: Config = serde_yaml::from_value(document).map_err(invalid)?;
        serde_json::to_value(config)
            .map_err(|e| ConfigError(format!("could not serialize effective configuration: {e}")))
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let interpolated = interpolate_env(yaml);
        let mut document: serde_yaml::Value = serde_yaml::from_str(&interpolated)
            .map_err(|e| ConfigError(format!("invalid YAML: {e}")))?;
        crate::presets::apply(&mut document);
        let mut cfg: Config = serde_yaml::from_value(document)
            .map_err(|e| ConfigError(format!("invalid YAML: {e}")))?;
        for key in cfg.llm_proxy.deprecated_keys() {
            tracing::warn!(
                key,
                "ignored config key: the per-request cost policy was removed"
            );
        }
        cfg.state_file = expand_tilde(&cfg.state_file);
        if let Some(helper) = &mut cfg.mcp_credential_command {
            helper.command = expand_tilde_str(&helper.command);
        }
        if let Some(p) = &cfg.delegation.socket_path {
            cfg.delegation.socket_path = Some(expand_tilde(p));
        }
        let mut agents = Vec::new();
        for mut agent in std::mem::take(&mut cfg.agents) {
            if agent.name.is_empty() {
                return Err(ConfigError("agent name must not be empty".into()));
            }
            let accounts = std::mem::take(&mut agent.accounts);
            if accounts.is_empty() {
                agents.push(agent);
                continue;
            }
            for (index, account) in accounts.into_iter().enumerate() {
                if account.name.is_empty() || account.name.contains(['/', '@']) {
                    return Err(ConfigError(
                        "account name must be nonempty and contain neither `/` nor `@`".into(),
                    ));
                }
                let mut seat = agent.clone();
                seat.name = format!("{}@{}", agent.name, account.name);
                // Priorities are 1-based: list order defaults to 1, 2, 3…
                seat.account_priority = Some(account.priority.unwrap_or(index as u32 + 1));
                seat.account_disabled = account.disabled || agent.account_disabled;
                seat.lineage = Some(agent.lineage.clone().unwrap_or_else(|| agent.name.clone()));
                seat.command.env.extend(account.env);
                if let Some(reserve) = account.reserve_capacity {
                    if let Some(weekly) = reserve.weekly {
                        seat.reserve_capacity.weekly = weekly;
                    }
                    if let Some(session) = reserve.session {
                        seat.reserve_capacity.session = session;
                    }
                }
                if account.account_plan.is_some() {
                    seat.account_plan = account.account_plan;
                }
                agents.push(seat);
            }
        }
        cfg.agents = agents;
        if let Some(hook) = &mut cfg.delegation.lifecycle_hook {
            hook.command = expand_tilde_str(&hook.command);
            for arg in &mut hook.args {
                *arg = expand_tilde_str(arg);
            }
        }
        // Downstream adapters are spawned via `Command::new` (no shell), so a
        // leading `~` in a command path or arg would never be expanded and the
        // spawn would fail — expand it here the same way we do for state paths.
        for agent in &mut cfg.agents {
            agent.command.command = expand_tilde_str(&agent.command.command);
            for arg in &mut agent.command.args {
                *arg = expand_tilde_str(arg);
            }
            for env in &mut agent.command.env {
                if matches!(env.name.as_str(), "CLAUDE_CONFIG_DIR" | "CODEX_HOME") {
                    env.value = expand_tilde_str(&env.value);
                }
            }
            if let Some(probe) = &mut agent.auth_probe {
                probe.command = expand_tilde_str(&probe.command);
                for arg in &mut probe.args {
                    *arg = expand_tilde_str(arg);
                }
            }
            if let Some(login) = &mut agent.login_command {
                login.command = expand_tilde_str(&login.command);
                for arg in &mut login.args {
                    *arg = expand_tilde_str(arg);
                }
            }
            if let ModelSelectionConfig::SpawnConfig { process_template } =
                &mut agent.model_selection
            {
                for arg in &mut process_template.args {
                    *arg = expand_tilde_str(arg);
                }
            }
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// `yaml` reduced to the values that differ from router-acp's defaults:
    /// the provider presets, implied seat directories and section defaults.
    /// The input is the raw file, so `${VAR}` references stay unexpanded. The
    /// result is refused unless it resolves to the same configuration.
    pub fn minimize_yaml(yaml: &str) -> Result<String, ConfigError> {
        let invalid = |e: serde_yaml::Error| ConfigError(format!("invalid YAML: {e}"));
        let document: serde_yaml::Value = serde_yaml::from_str(yaml).map_err(invalid)?;
        let mut defaults =
            serde_yaml::to_value(serde_yaml::from_str::<Config>("agents: []").map_err(invalid)?)
                .map_err(invalid)?;
        if let Some(map) = defaults.as_mapping_mut() {
            map.remove("agents");
        }
        let agent_defaults = agent_defaults();
        let small = serde_yaml::to_string(&crate::presets::minimize(
            &document,
            &defaults,
            &agent_defaults,
        ))
        .map_err(invalid)?;
        let resolved = |text: &str| -> Result<serde_yaml::Value, ConfigError> {
            serde_yaml::to_value(Self::from_yaml(text)?).map_err(invalid)
        };
        let (full, reduced) = (resolved(yaml)?, resolved(&small)?);
        if full != reduced {
            let mut paths = Vec::new();
            differences(&full, &reduced, String::new(), &mut paths);
            return Err(ConfigError(format!(
                "the reduced configuration would resolve differently at {}; it was not produced",
                paths.join(", ")
            )));
        }
        Ok(small)
    }

    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let yaml = crate::project_config::read(path)
            .map_err(ConfigError)?
            .ok_or_else(|| {
                ConfigError(format!("Configuration does not exist: {}", path.display()))
            })?;
        let mut cfg = Self::from_yaml(&yaml)?;
        cfg.source_path = Some(if crate::project_config::is_project(path) {
            std::path::absolute(path).map_err(|e| ConfigError(e.to_string()))?
        } else {
            std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
        });
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.agents.is_empty() {
            return Err(ConfigError(
                "no agents configured; declare at least one agent with one model".into(),
            ));
        }
        let mut names = HashSet::new();
        for agent in &self.agents {
            for (window, reserve) in [
                ("weekly", agent.reserve_capacity.weekly),
                ("session", agent.reserve_capacity.session),
            ] {
                if !(0.0..=100.0).contains(&reserve) {
                    return Err(ConfigError(format!(
                        "agent `{}`: reserve_capacity.{window} must be between 0 and 100",
                        agent.name
                    )));
                }
                if reserve > 0.0
                    && reserve < 100.0
                    && (!self.cordon.enabled || agent.usage_source.is_none())
                {
                    return Err(ConfigError(format!(
                        "agent `{}`: reserve_capacity requires cordon.enabled and a usage_source",
                        agent.name
                    )));
                }
            }
            if agent.name.is_empty() {
                return Err(ConfigError("agent name must not be empty".into()));
            }
            if agent.name.contains('/') {
                return Err(ConfigError(format!(
                    "agent name `{}` must not contain `/` (reserved for candidate ids)",
                    agent.name
                )));
            }
            if !names.insert(agent.name.clone()) {
                return Err(ConfigError(format!(
                    "duplicate agent name `{}`",
                    agent.name
                )));
            }
            if agent.command.kind != "stdio" {
                return Err(ConfigError(format!(
                    "agent `{}`: command type `{}` is not supported; ACP agents must use stdio",
                    agent.name, agent.command.kind
                )));
            }
            if agent.command.command.is_empty() {
                return Err(ConfigError(format!(
                    "agent `{}`: command must not be empty",
                    agent.name
                )));
            }
            if agent.models.is_empty() {
                return Err(ConfigError(format!(
                    "agent `{}` declares no models; every agent needs at least one model",
                    agent.name
                )));
            }
            if let Some(probe) = &agent.auth_probe {
                if probe.command.is_empty() {
                    return Err(ConfigError(format!(
                        "agent `{}`: auth_probe.command must not be empty",
                        agent.name
                    )));
                }
                if probe.timeout_ms == 0 {
                    return Err(ConfigError(format!(
                        "agent `{}`: auth_probe.timeout_ms must be >= 1",
                        agent.name
                    )));
                }
                if probe.unauthenticated_patterns.iter().any(|p| p.is_empty()) {
                    return Err(ConfigError(format!(
                        "agent `{}`: auth_probe.unauthenticated_patterns must not contain an \
                         empty pattern (it would match every output)",
                        agent.name
                    )));
                }
            }
            let mut model_ids = HashSet::new();
            for model in &agent.models {
                if model.id.is_empty() {
                    return Err(ConfigError(format!(
                        "agent `{}`: model id must not be empty",
                        agent.name
                    )));
                }
                if !model_ids.insert(model.id.clone()) {
                    return Err(ConfigError(format!(
                        "agent `{}`: duplicate model id `{}`",
                        agent.name, model.id
                    )));
                }
                if model.cost_rank == 0 {
                    return Err(ConfigError(format!(
                        "agent `{}` model `{}`: cost_rank must be >= 1",
                        agent.name, model.id
                    )));
                }
            }
        }
        for agent in &self.agents {
            if !(0.0..=1.0).contains(&agent.preference) {
                return Err(ConfigError(format!(
                    "agent `{}`: preference must be between 0 and 1 (small values like 0.05 \
                     are recommended)",
                    agent.name
                )));
            }
        }
        if !(0.0..=1.0).contains(&self.availability_preference.cost_aversion) {
            return Err(ConfigError(
                "availability_preference.cost_aversion must be between 0 and 1".into(),
            ));
        }
        if self.availability_preference.headroom_scale_dollars <= 0.0 {
            return Err(ConfigError(
                "availability_preference.headroom_scale_dollars must be > 0".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.availability_preference.overage_budget_weight) {
            return Err(ConfigError(
                "availability_preference.overage_budget_weight must be between 0 and 1".into(),
            ));
        }
        if self.delegation.max_concurrent == 0 {
            return Err(ConfigError("delegation.max_concurrent must be >= 1".into()));
        }
        if !(0.0..=10.0).contains(&self.routers.auto.cost_quality_tradeoff) {
            return Err(ConfigError(
                "routers.auto.cost_quality_tradeoff must be between 0 and 10".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.routers.auto.min_cost_weight) {
            return Err(ConfigError(
                "routers.auto.min_cost_weight must be between 0 and 1".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.delegation.complexity_cap) {
            return Err(ConfigError(
                "delegation.complexity_cap must be between 0 and 1".into(),
            ));
        }
        if let Some(hook) = &self.delegation.lifecycle_hook {
            if hook.command.trim().is_empty() {
                return Err(ConfigError(
                    "delegation.lifecycle_hook.command must not be empty".into(),
                ));
            }
            if hook.timeout_ms == 0 {
                return Err(ConfigError(
                    "delegation.lifecycle_hook.timeout_ms must be positive".into(),
                ));
            }
        }
        if let Some(agent) = self.agents.iter().find(|a| a.max_delegates == Some(0)) {
            return Err(ConfigError(format!(
                "agent `{}`: max_delegates must be positive (omit it for no per-agent limit)",
                agent.name
            )));
        }
        if self.llm_proxy.enabled {
            let listen: std::net::SocketAddr = self.llm_proxy.listen.parse().map_err(|_| {
                ConfigError(format!(
                    "llm_proxy.listen `{}` must be an IP socket address",
                    self.llm_proxy.listen
                ))
            })?;
            if !listen.ip().is_loopback() {
                return Err(ConfigError(
                    "llm_proxy.listen must use a loopback address; provider credentials pass \
                     through this listener"
                        .into(),
                ));
            }
            if !(0.1..=1.0).contains(&self.llm_proxy.context_window_fraction) {
                return Err(ConfigError(
                    "llm_proxy.context_window_fraction must be between 0.1 and 1".into(),
                ));
            }
            if self.llm_proxy.max_request_bytes == 0 || self.llm_proxy.max_capture_bytes == 0 {
                return Err(ConfigError(
                    "llm_proxy max_request_bytes/max_capture_bytes must be greater than zero"
                        .into(),
                ));
            }
            // Enabled is the default; with no proxied agent the proxy simply
            // does not start (`llm_proxy::LlmProxy` needs a target).
        }
        for agent in &self.agents {
            if let Some(proxy) = &agent.llm_proxy {
                if proxy.base_url_env.trim().is_empty() {
                    return Err(ConfigError(format!(
                        "agent `{}`: llm_proxy.base_url_env must not be empty",
                        agent.name
                    )));
                }
                let url = reqwest::Url::parse(&proxy.upstream_base_url).map_err(|e| {
                    ConfigError(format!(
                        "agent `{}`: invalid llm_proxy.upstream_base_url: {e}",
                        agent.name
                    ))
                })?;
                if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                    return Err(ConfigError(format!(
                        "agent `{}`: llm_proxy.upstream_base_url must be an absolute HTTP(S) URL",
                        agent.name
                    )));
                }
                if proxy.codex_chatgpt_provider && proxy.protocol != LlmWireProtocol::Openai {
                    return Err(ConfigError(format!(
                        "agent `{}`: llm_proxy.codex_chatgpt_provider requires protocol `openai`",
                        agent.name
                    )));
                }
            }
            for model in &agent.models {
                if model
                    .api_model
                    .as_ref()
                    .is_some_and(|id| id.trim().is_empty())
                {
                    return Err(ConfigError(format!(
                        "agents.{}.models.{}.api_model must not be empty",
                        agent.name, model.id
                    )));
                }
                let negative = |p: &PricingConfig| {
                    p.input_per_mtok < 0.0
                        || p.output_per_mtok < 0.0
                        || p.cache_read_per_mtok.is_some_and(|v| v < 0.0)
                        || p.cache_write_per_mtok.is_some_and(|v| v < 0.0)
                };
                if model.pricing.as_ref().is_some_and(negative)
                    || model
                        .versions
                        .iter()
                        .any(|v| v.pricing.as_ref().is_some_and(negative))
                {
                    return Err(ConfigError(format!(
                        "agents.{}.models.{}.pricing rates must be non-negative",
                        agent.name, model.id
                    )));
                }
                let current = model.api_model.as_deref().unwrap_or(&model.id);
                let mut seen = std::collections::HashSet::new();
                for version in &model.versions {
                    let api = version.api_model.trim();
                    if api.is_empty()
                        || api == DEFAULT_VERSION
                        || api == current
                        || !seen.insert(api)
                    {
                        return Err(ConfigError(format!(
                            "agents.{}.models.{}.versions: `{}` must be a non-empty, unique \
                             api_model other than `{DEFAULT_VERSION}` and the current `{current}`",
                            agent.name, model.id, version.api_model
                        )));
                    }
                }
            }
        }
        // A typo'd pin must fail the load rather than silently serve latest.
        for (key, version) in &self.pinned_versions {
            let Some(model) = CandidateId::parse(key).and_then(|id| self.model_config(&id)) else {
                return Err(ConfigError(format!(
                    "pinned_versions `{key}` does not match any declared agent/model"
                )));
            };
            if version != DEFAULT_VERSION && !model.versions.iter().any(|v| &v.api_model == version)
            {
                return Err(ConfigError(format!(
                    "pinned_versions `{key}`: `{version}` is not `{DEFAULT_VERSION}` or one of its \
                     declared versions"
                )));
            }
        }
        if self.routers.escalation.initial_router == Some(StrategyKind::Escalation) {
            return Err(ConfigError(
                "routers.escalation.initial_router cannot be `escalation` (it would recurse); \
                 use auto, pareto-code, or static"
                    .into(),
            ));
        }
        if let Some(candidate) = &self.routers.static_.candidate {
            let id = CandidateId::parse(candidate).ok_or_else(|| {
                ConfigError(format!(
                    "routers.static.candidate `{candidate}` must have the form `agent/model-id`"
                ))
            })?;
            let declared = self
                .agents
                .iter()
                .any(|a| a.name == id.agent && a.models.iter().any(|m| m.id == id.model));
            if !declared {
                return Err(ConfigError(format!(
                    "routers.static.candidate `{candidate}` does not match any declared agent/model"
                )));
            }
        } else if self.router == StrategyKind::Static {
            return Err(ConfigError(
                "router is `static` but routers.static.candidate is not set".into(),
            ));
        }
        parse_history(&self.history).map_err(|e| ConfigError(format!("history: {e}")))?;
        if self.classifier.backend == ClassifierBackend::LocalModel
            && self.classifier.local_model.is_none()
        {
            return Err(ConfigError(
                "classifier.backend is `local-model` but classifier.local_model is not set".into(),
            ));
        }
        for rule in &self.ticket_context {
            if rule.prefix.trim().is_empty() {
                return Err(ConfigError(
                    "ticket_context: prefix must not be empty".into(),
                ));
            }
            if rule.command.is_empty() {
                return Err(ConfigError(format!(
                    "ticket_context `{}`: command must not be empty",
                    rule.prefix
                )));
            }
            if !rule.command.iter().any(|a| a.contains("$TICKET")) {
                return Err(ConfigError(format!(
                    "ticket_context `{}`: command must reference $TICKET (else every ticket \
                     fetches the same thing)",
                    rule.prefix
                )));
            }
        }
        if let Some(helper) = &self.mcp_credential_command {
            if helper.command.trim().is_empty() {
                return Err(ConfigError(
                    "mcp_credential_command: command must not be empty".into(),
                ));
            }
            if helper.timeout_ms == 0 {
                return Err(ConfigError(
                    "mcp_credential_command: timeout_ms must be positive".into(),
                ));
            }
        }
        if self.orchestration.is_some() {
            return Err(ConfigError(
                "`orchestration` was removed: auto-orchestration is superseded by the `planner` \
                 router (`router: planner`, ROUTERS.md). Delete the `orchestration:` block."
                    .into(),
            ));
        }
        for (pattern, cap) in &self.delegation.candidate_effort_caps {
            if pattern.trim().is_empty() || *cap == crate::candidate::EffortLevel::Auto {
                return Err(ConfigError("delegation.candidate_effort_caps requires nonempty patterns and concrete effort levels".into()));
            }
        }
        if let (Some(default), Some(cap)) = (self.effort.default, self.effort.max_automatic)
            && default > cap
        {
            return Err(ConfigError(format!(
                "effort.default `{}` is above effort.max_automatic `{}`",
                default.as_str(),
                cap.as_str()
            )));
        }
        for (role, mapping) in [
            ("create-plan", &self.routers.planner.create_plan),
            ("select-plan", &self.routers.planner.select_plan),
            ("implement-work", &self.routers.planner.implement_work),
            ("review-work", &self.routers.planner.review_work),
            ("finish-work", &self.routers.planner.finish_work),
            ("integrate-plan", &self.routers.planner.integrate_plan),
        ] {
            if let Some(skill) = &mapping.skill
                && !crate::planner_skills::valid_skill_name(skill)
            {
                return Err(ConfigError(format!(
                    "routers.planner.{role}.skill must name a repository skill"
                )));
            }
        }
        if self
            .routers
            .planner
            .profile
            .as_deref()
            .is_some_and(|p| p.trim().is_empty())
        {
            return Err(ConfigError(
                "routers.planner.profile must be markdown or a readable policy path".into(),
            ));
        }
        if self
            .routers
            .planner
            .skills
            .as_ref()
            .is_some_and(|p| !p.is_absolute())
        {
            return Err(ConfigError(
                "routers.planner.skills must be an absolute host-installed directory".into(),
            ));
        }
        if self
            .routers
            .planner
            .workspace
            .as_ref()
            .is_some_and(|w| w.command.trim().is_empty() || w.timeout_ms == 0)
        {
            return Err(ConfigError(
                "planner workspace requires a command and positive timeout_ms".into(),
            ));
        }
        if self.pre_classifier.enabled {
            if self.pre_classifier.evaluator.is_empty() {
                return Err(ConfigError(
                    "pre_classifier.enabled is true but pre_classifier.evaluator is empty".into(),
                ));
            }
            // pre_classifier.timeout_ms is deprecated/ignored (the classifier no
            // longer times out); any value — including 0 — is accepted.
            let mut seen = HashSet::new();
            for dim in &self.pre_classifier.dimensions {
                if dim.id.trim().is_empty() {
                    return Err(ConfigError(
                        "pre_classifier.dimensions: id must not be empty".into(),
                    ));
                }
                if !seen.insert(dim.id.clone()) {
                    return Err(ConfigError(format!(
                        "pre_classifier.dimensions: duplicate id `{}`",
                        dim.id
                    )));
                }
                if !(0.0..=1.0).contains(&dim.min_confidence) {
                    return Err(ConfigError(format!(
                        "pre_classifier.dimensions.`{}`.min_confidence must be within 0.0..=1.0",
                        dim.id
                    )));
                }
            }
        }
        let mut catalogs = HashSet::new();
        for catalog in &self.delegation.mcp_catalogs {
            if catalog.catalog.trim().is_empty() {
                return Err(ConfigError(
                    "delegation.mcp_catalogs: catalog must not be empty".into(),
                ));
            }
            if !catalogs.insert(catalog.catalog.clone()) {
                return Err(ConfigError(format!(
                    "delegation.mcp_catalogs: duplicate catalog `{}`",
                    catalog.catalog
                )));
            }
            if catalog.capabilities.is_empty() {
                return Err(ConfigError(format!(
                    "delegation.mcp_catalogs.`{}`: capabilities must not be empty",
                    catalog.catalog
                )));
            }
            let mut capabilities = HashSet::new();
            for capability in &catalog.capabilities {
                if capability.trim().is_empty() {
                    return Err(ConfigError(format!(
                        "delegation.mcp_catalogs.`{}`: capability must not be empty",
                        catalog.catalog
                    )));
                }
                if !capabilities.insert(capability.clone()) {
                    return Err(ConfigError(format!(
                        "delegation.mcp_catalogs.`{}`: duplicate capability `{capability}`",
                        catalog.catalog
                    )));
                }
            }
        }
        Ok(())
    }

    /// State-DB retention derived from `history`.
    pub fn retention(&self) -> crate::state::Retention {
        let secs = parse_history(&self.history).unwrap_or(30 * 24 * 60 * 60);
        crate::state::Retention {
            max_age: std::time::Duration::from_secs(secs),
        }
    }

    /// The declared model entry for a candidate, if it exists.
    /// The model-company lineage of an agent: its declared `lineage`, else
    /// its name. Every account of one provider shares it.
    pub fn lineage_of(&self, agent: &str) -> String {
        self.agents
            .iter()
            .find(|a| a.name == agent)
            .and_then(|a| a.lineage.clone())
            .unwrap_or_else(|| agent.to_string())
    }

    pub fn model_config(&self, id: &CandidateId) -> Option<&ModelConfig> {
        self.agents
            .iter()
            .find(|a| a.name == id.agent)?
            .models
            .iter()
            .find(|m| m.id == id.model)
    }

    /// Whether an automatic mechanism may choose this candidate. Undeclared
    /// candidates read as eligible — the declaration check belongs to the
    /// caller, and defaulting to "ineligible" here would silently empty a
    /// pool on a lookup miss.
    pub fn auto_eligible(&self, id: &CandidateId) -> bool {
        self.model_config(id)
            .map(|m| m.auto_eligible)
            .unwrap_or(true)
    }

    /// The `pinned_versions` entry in force for a candidate; `None` when it
    /// runs its current `api_model`.
    pub fn pinned_version(&self, id: &CandidateId) -> Option<&ModelVersion> {
        self.declared_version(id, self.pinned_versions.get(&id.to_string())?)
    }

    /// One of a candidate's declared `versions`, by `api_model`.
    pub fn declared_version(&self, id: &CandidateId, api_model: &str) -> Option<&ModelVersion> {
        self.model_config(id)?
            .versions
            .iter()
            .find(|v| v.api_model == api_model)
    }

    /// The version a candidate runs given a session's `[router: version=…]`
    /// request: `default` forces the current `api_model`, a version the
    /// candidate declares wins, and anything else (no request, or a version
    /// of another model) falls back to `pinned_versions`.
    pub fn resolve_version(
        &self,
        id: &CandidateId,
        requested: Option<&str>,
    ) -> Option<&ModelVersion> {
        match requested {
            Some(DEFAULT_VERSION) => None,
            Some(v) => self
                .declared_version(id, v)
                .or_else(|| self.pinned_version(id)),
            None => self.pinned_version(id),
        }
    }

    /// The model id the proxy puts on the wire for a candidate.
    pub fn wire_api_model(&self, id: &CandidateId) -> String {
        match self.pinned_version(id) {
            Some(version) => version.api_model.clone(),
            None => self.wire_api_model_unpinned(id),
        }
    }

    /// The candidate's current wire model, ignoring any pinned version.
    pub fn wire_api_model_unpinned(&self, id: &CandidateId) -> String {
        self.model_config(id)
            .and_then(|m| m.api_model.clone())
            .unwrap_or_else(|| id.model.clone())
    }

    /// Pricing of the version a candidate actually runs.
    pub fn model_pricing(&self, id: &CandidateId) -> Option<&PricingConfig> {
        match self.pinned_version(id) {
            Some(version) => version.pricing.as_ref(),
            None => self.model_config(id)?.pricing.as_ref(),
        }
    }

    /// All declared candidate ids in config order.
    pub fn declared_candidates(&self) -> Vec<CandidateId> {
        let mut out = Vec::new();
        for agent in &self.agents {
            for model in &agent.models {
                out.push(CandidateId::new(&agent.name, &model.id));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_yaml() -> &'static str {
        r#"
agents:
  - name: claude
    command:
      type: stdio
      command: mock-agent
    model_selection:
      type: config-option
    models:
      - id: sonnet
        display_name: Claude Sonnet
        cost_rank: 2
"#
    }

    #[test]
    fn a_seat_named_for_its_provider_needs_no_other_keys() {
        let cfg = Config::from_yaml("agents:\n- name: codex@one\n- name: grok@two\n").unwrap();
        let codex = &cfg.agents[0];
        assert!(codex.command.command.ends_with("codex-acp"));
        assert!(codex.models.iter().any(|m| m.id == "gpt-6.1-sol"));
        // Astra is part of the shipped codex preset: a `provider: codex` agent
        // must route to it without declaring it by hand.
        assert!(codex.models.iter().any(|m| m.id == "gpt-6-astra"));
        assert!(matches!(
            codex.usage_source,
            Some(UsageSourceConfig::CodexRollout)
        ));
        assert!(
            codex
                .command
                .env
                .iter()
                .any(|e| e.name == "CODEX_HOME" && e.value.ends_with("/accounts/codex@one"))
        );
        assert_eq!(cfg.agents[1].lineage.as_deref(), Some("xai"));
    }

    #[test]
    fn minimize_reduces_a_full_configuration_to_its_overrides() {
        let home = std::env::var("HOME").unwrap();
        let mut full: serde_yaml::Value =
            serde_yaml::from_str("agents:\n- name: claude@one\n  preference: 0.1\n").unwrap();
        crate::presets::apply(&mut full);
        full["cordon"] = serde_yaml::from_str("{ enabled: true, poll_secs: 120 }").unwrap();
        full["llm_proxy"] = serde_yaml::from_str("{ enabled: true, routine_streak: 3 }").unwrap();
        let small = Config::minimize_yaml(&serde_yaml::to_string(&full).unwrap()).unwrap();
        let small: serde_yaml::Value = serde_yaml::from_str(&small).unwrap();
        let expect: serde_yaml::Value = serde_yaml::from_str(
            "agents:\n- name: claude@one\n  preference: 0.1\ncordon:\n  poll_secs: 120\n",
        )
        .unwrap();
        assert_eq!(
            small, expect,
            "seat dir under {home} and preset values drop"
        );
    }

    #[test]
    fn minimize_refuses_a_reduction_that_would_resolve_differently() {
        // A custom entry runs its own program; reducing it must not let the
        // preset change what it resolves to, and the check proves it.
        let text = "agents:\n- name: claude\n  command: { type: stdio, command: /opt/mock-agent }\n  model_selection: { type: config-option }\n  models: [{ id: m1, cost_rank: 1 }]\n";
        let small = Config::minimize_yaml(text).unwrap();
        assert_eq!(
            serde_yaml::to_value(Config::from_yaml(&small).unwrap()).unwrap(),
            serde_yaml::to_value(Config::from_yaml(text).unwrap()).unwrap()
        );
    }

    #[test]
    fn parses_and_validates_the_mcp_credential_command() {
        let yaml = format!(
            "mcp_credential_command:\n  command: ~/bin/kory-mcp-credential\n{}",
            minimal_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        let helper = cfg.mcp_credential_command.as_ref().unwrap();
        assert!(!helper.command.starts_with('~'), "tilde expands");
        assert_eq!(helper.timeout_ms, 5_000);
        assert!(
            Config::from_yaml(minimal_yaml())
                .unwrap()
                .mcp_credential_command
                .is_none()
        );
        let empty = format!(
            "mcp_credential_command:\n  command: \"\"\n{}",
            minimal_yaml()
        );
        assert!(Config::from_yaml(&empty).is_err());
    }

    #[test]
    fn parses_minimal_config_with_defaults() {
        let cfg = Config::from_yaml(minimal_yaml()).unwrap();
        assert_eq!(cfg.router, StrategyKind::Auto);
        assert_eq!(cfg.delegation.max_concurrent, 3);
        assert!(cfg.delegation.enabled);
        assert!(!cfg.delegation.inject_prompt);
        assert!(cfg.delegation.mcp_catalogs.is_empty());
        assert_eq!(cfg.headroom.window_secs, 5 * 60 * 60);
        assert_eq!(cfg.agents[0].budget_prompts_5h, 400);
        assert_eq!(cfg.routers.auto.cost_quality_tradeoff, 7.0);
        assert!(cfg.effort.default.is_none() && cfg.effort.max_automatic.is_none());
        assert!(
            cfg.llm_proxy.enabled,
            "on by default; idle without a proxied agent"
        );
        assert!(cfg.llm_proxy.deprecated_keys().is_empty());
    }

    #[test]
    fn accepts_removed_llm_proxy_policy_keys_as_deprecated() {
        let yaml = format!(
            "llm_proxy:\n  routine_streak: 3\n  minimum_dwell_requests: 4\n  \
             verdict_ttl_requests: 6\n  verdict_ttl_secs: 900\n{}",
            minimal_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(
            cfg.llm_proxy.deprecated_keys(),
            vec![
                "llm_proxy.routine_streak",
                "llm_proxy.minimum_dwell_requests",
                "llm_proxy.verdict_ttl_requests",
                "llm_proxy.verdict_ttl_secs",
            ]
        );
    }

    #[test]
    fn parses_delegation_prompt_opt_in() {
        let yaml = format!("delegation:\n  inject_prompt: true\n{}", minimal_yaml());
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert!(cfg.delegation.inject_prompt);
    }

    #[test]
    fn parses_delegate_worker_controls() {
        let cfg = Config::from_yaml(minimal_yaml()).unwrap();
        assert_eq!(cfg.delegation.candidate_hints, CandidateHintMode::Prefer);
        assert_eq!(
            cfg.delegation.native_subagents,
            NativeSubagentPolicy::Forbid
        );
        assert!(cfg.delegation.lifecycle_hook.is_none());

        let yaml = format!(
            "delegation:\n  candidate_hints: exact\n  native_subagents: allow\n  \
             lifecycle_hook: {{ command: ~/bin/hook, args: [\"~/x\"] }}\n{}",
            minimal_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(cfg.delegation.candidate_hints, CandidateHintMode::Exact);
        assert_eq!(cfg.delegation.native_subagents, NativeSubagentPolicy::Allow);
        let hook = cfg.delegation.lifecycle_hook.as_ref().unwrap();
        assert!(!hook.command.starts_with('~'), "command is tilde-expanded");
        assert!(!hook.args[0].starts_with('~'), "args are tilde-expanded");
        assert_eq!(hook.timeout_ms, 30_000);
        assert_eq!(hook.max_continuations, 3);

        let empty = format!(
            "delegation:\n  lifecycle_hook: {{ command: \"\" }}\n{}",
            minimal_yaml()
        );
        assert!(Config::from_yaml(&empty).is_err());
    }

    #[test]
    fn max_delegates_is_per_account_seat_and_positive() {
        let yaml = format!(
            "{}\n    max_delegates: 4\n    accounts:\n      - name: work\n      - name: personal\n",
            minimal_yaml().trim_end()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert!(cfg.agents.iter().all(|a| a.max_delegates == Some(4)));
        assert_eq!(cfg.agents.len(), 2, "each seat carries its own limit");
        let zero = format!("{}\n    max_delegates: 0\n", minimal_yaml().trim_end());
        assert!(Config::from_yaml(&zero).is_err());
    }

    #[test]
    fn parses_delegate_mcp_catalog_policy() {
        let yaml = format!(
            "delegation:\n  mcp_catalogs:\n    - catalog: telemetry\n      capabilities: [metrics, traces]\n{}",
            minimal_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(cfg.delegation.mcp_catalogs[0].catalog, "telemetry");
    }

    #[test]
    fn a_stale_orchestration_block_points_to_the_planner_router() {
        for block in ["orchestration:\n  enabled: false\n", "orchestration: {}\n"] {
            let err = Config::from_yaml(&format!("{block}{}", minimal_yaml())).unwrap_err();
            assert!(err.0.contains("`planner` router"), "{}", err.0);
        }
    }

    #[test]
    fn parses_effort_policy() {
        for mapping in ["{ '*/gpt-6.1-sol': auto }", "{ '': low }"] {
            let yaml = format!(
                "delegation: {{ candidate_effort_caps: {mapping} }}\n{}",
                minimal_yaml()
            );
            assert!(Config::from_yaml(&yaml).is_err());
        }
        let yaml = format!(
            "delegation: {{ candidate_effort_caps: {{ '*/gpt-6.1-sol': low }} }}\n{}",
            minimal_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(
            cfg.delegation.candidate_effort_caps["*/gpt-6.1-sol"],
            crate::candidate::EffortLevel::Low
        );
        let yaml = format!(
            "effort: {{ default: medium, max_automatic: high }}\n{}",
            minimal_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(
            cfg.effort.default,
            Some(crate::candidate::EffortLevel::Medium)
        );
        assert_eq!(
            cfg.effort.max_automatic,
            Some(crate::candidate::EffortLevel::High)
        );
        let inverted = format!(
            "effort: {{ default: max, max_automatic: medium }}\n{}",
            minimal_yaml()
        );
        assert!(Config::from_yaml(&inverted).is_err());
    }

    fn versioned_yaml() -> &'static str {
        r#"
agents:
  - name: claude
    command: { type: stdio, command: mock-agent }
    model_selection: { type: config-option }
    models:
      - { id: opus, cost_rank: 4, api_model: claude-opus-5-5,
          pricing: { input_per_mtok: 4.0, output_per_mtok: 20.0 },
          versions: [ { api_model: claude-opus-4-6,
                        pricing: { input_per_mtok: 5.0, output_per_mtok: 25.0 } } ] }
"#
    }

    #[test]
    fn an_unpinned_model_runs_its_current_api_model() {
        let cfg = Config::from_yaml(versioned_yaml()).unwrap();
        let opus = CandidateId::new("claude", "opus");
        assert!(cfg.pinned_version(&opus).is_none());
        assert_eq!(cfg.wire_api_model(&opus), "claude-opus-5-5");
        assert_eq!(cfg.model_pricing(&opus).unwrap().input_per_mtok, 4.0);
        let pinned_default = format!(
            "pinned_versions:\n  claude/opus: default\n{}",
            versioned_yaml()
        );
        let cfg = Config::from_yaml(&pinned_default).unwrap();
        assert_eq!(cfg.wire_api_model(&opus), "claude-opus-5-5");
    }

    #[test]
    fn a_pinned_version_changes_only_the_wire_model_and_its_pricing() {
        let yaml = format!(
            "pinned_versions:\n  claude/opus: claude-opus-4-6\n{}",
            versioned_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        let opus = CandidateId::new("claude", "opus");
        assert_eq!(cfg.declared_candidates(), vec![opus.clone()]);
        assert_eq!(cfg.wire_api_model(&opus), "claude-opus-4-6");
        assert_eq!(cfg.model_pricing(&opus).unwrap().input_per_mtok, 5.0);
    }

    #[test]
    fn a_session_version_request_overrides_the_configured_pin() {
        let yaml = format!(
            "pinned_versions:\n  claude/opus: claude-opus-4-6\n{}",
            versioned_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        let opus = CandidateId::new("claude", "opus");
        let api = |requested| {
            cfg.resolve_version(&opus, requested)
                .map(|v| v.api_model.as_str())
        };
        assert_eq!(
            api(None),
            Some("claude-opus-4-6"),
            "no request: the configured pin"
        );
        assert_eq!(
            api(Some("default")),
            None,
            "default forces the current api_model"
        );
        assert_eq!(api(Some("claude-opus-4-6")), Some("claude-opus-4-6"));
        assert_eq!(
            api(Some("gpt-5.5")),
            Some("claude-opus-4-6"),
            "not opus's: the pin"
        );
    }

    #[test]
    fn rejects_a_pin_to_an_undeclared_version_or_model() {
        let yaml = format!(
            "pinned_versions:\n  claude/opus: claude-opus-4-9\n{}",
            versioned_yaml()
        );
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(err.0.contains("claude-opus-4-9"), "{}", err.0);
        let yaml = format!(
            "pinned_versions:\n  claude/sonnet: default\n{}",
            versioned_yaml()
        );
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(err.0.contains("does not match any declared"), "{}", err.0);
    }

    #[test]
    fn rejects_a_version_that_repeats_the_current_api_model() {
        let yaml =
            versioned_yaml().replace("api_model: claude-opus-4-6", "api_model: claude-opus-5-5");
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(err.0.contains("versions"), "{}", err.0);
    }

    #[test]
    fn rejects_the_retired_downstream_id_field() {
        let yaml = versioned_yaml().replace("cost_rank: 4,", "cost_rank: 4, downstream_id: opus,");
        assert!(Config::from_yaml(&yaml).is_err());
    }

    #[test]
    fn rejects_zero_agents() {
        let err = Config::from_yaml("agents: []").unwrap_err();
        assert!(err.0.contains("no agents"));
    }

    #[test]
    fn parses_and_validates_llm_proxy_config() {
        let yaml = r#"
llm_proxy:
  enabled: true
  listen: 127.0.0.1:0
agents:
  - name: claude
    command: { type: stdio, command: mock-agent }
    model_selection: { type: config-option }
    llm_proxy:
      protocol: anthropic
      base_url_env: ANTHROPIC_BASE_URL
      upstream_base_url: https://api.anthropic.com
    models:
      - id: opus
        api_model: claude-opus-api-id
        cost_rank: 3
"#;
        let cfg = Config::from_yaml(yaml).unwrap();
        assert_eq!(
            cfg.agents[0].models[0].api_model.as_deref(),
            Some("claude-opus-api-id")
        );

        let invalid = yaml.replace("127.0.0.1:0", "0.0.0.0:8080");
        let err = Config::from_yaml(&invalid).unwrap_err();
        assert!(err.0.contains("loopback"), "{}", err.0);
    }

    #[test]
    fn rejects_agent_without_models() {
        let yaml = r#"
agents:
  - name: claude
    command: { type: stdio, command: mock-agent }
    model_selection: { type: config-option }
    models: []
"#;
        let err = Config::from_yaml(yaml).unwrap_err();
        assert!(err.0.contains("no models"));
    }

    #[test]
    fn rejects_static_without_candidate() {
        let yaml = format!("router: static\n{}", minimal_yaml());
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(err.0.contains("static"));
    }

    #[test]
    fn accepts_static_with_declared_candidate() {
        let yaml = format!(
            "router: static\nrouters:\n  static:\n    candidate: claude/sonnet\n{}",
            minimal_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(cfg.router, StrategyKind::Static);
    }

    #[test]
    fn rejects_static_with_undeclared_candidate() {
        let yaml = format!(
            "routers:\n  static:\n    candidate: claude/nonexistent\n{}",
            minimal_yaml()
        );
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(err.0.contains("does not match"));
    }

    #[test]
    fn interpolates_env_vars() {
        // SAFETY: test-local env mutation.
        unsafe { std::env::set_var("ROUTER_ACP_TEST_VAR", "npx") };
        let yaml = r#"
agents:
  - name: claude
    command:
      type: stdio
      command: ${ROUTER_ACP_TEST_VAR}
    model_selection: { type: config-option }
    models:
      - id: sonnet
        cost_rank: 1
"#;
        let cfg = Config::from_yaml(yaml).unwrap();
        assert_eq!(cfg.agents[0].command.command, "npx");
    }

    #[test]
    fn interpolate_leaves_unknown_and_unterminated_intact() {
        let out = interpolate("a${missing}b${unterminated", &|_| None);
        assert_eq!(out, "a${missing}b${unterminated");
    }

    #[test]
    fn interpolates_bare_and_braced_variables_without_shell_evaluation() {
        let lookup = |name: &str| match name {
            "HOME" => Some("/Users/Zane".into()),
            "_BIN2" => Some("bin".into()),
            "LITERAL" => Some("$HOME".into()),
            _ => None,
        };
        assert_eq!(
            interpolate("$HOME/$_BIN2 ${HOME}/hooks $HOME_suffix $LITERAL", &lookup),
            "/Users/Zane/bin /Users/Zane/hooks $HOME_suffix $HOME"
        );
        let untouched = "$missing ${model_id} $0 $$HOME $(whoami) $ €$HOMELESS ${unfinished";
        assert_eq!(interpolate(untouched, &lookup), untouched);
        assert_eq!(interpolate("é$HOME/曲", &lookup), "é/Users/Zane/曲");
    }

    #[test]
    fn home_variables_resolve_hook_paths_and_survive_minimization() {
        let yaml = format!(
            "state_file: $HOME/.local/state/router-acp/sessions.db\ndelegation:\n  lifecycle_hook:\n    command: $HOME/.local/state/router-acp/bin/repo-host\n    args: ['${{HOME}}/workspace', '${{model_id}}']\n{}",
            minimal_yaml()
        );
        let home = std::env::var("HOME").unwrap();
        let cfg = Config::from_yaml(&yaml).unwrap();
        let hook = cfg.delegation.lifecycle_hook.unwrap();
        assert_eq!(
            hook.command,
            format!("{home}/.local/state/router-acp/bin/repo-host")
        );
        assert_eq!(
            hook.args,
            [format!("{home}/workspace"), "${model_id}".into()]
        );
        assert_eq!(
            cfg.state_file,
            PathBuf::from(format!("{home}/.local/state/router-acp/sessions.db"))
        );
        let minimized = Config::minimize_yaml(&yaml).unwrap();
        assert!(minimized.contains("$HOME/.local/state/router-acp/bin/repo-host"));
        assert!(minimized.contains("${HOME}/workspace"));
    }

    #[test]
    fn expand_tilde_str_only_touches_leading_home() {
        // Other tests resolve machine-local state concurrently; never mutate HOME.
        let home = std::env::var("HOME").unwrap();
        assert_eq!(expand_tilde_str("~"), home);
        assert_eq!(expand_tilde_str("~/bin/x"), format!("{home}/bin/x"));
        // A bare ~word (another user's home) is not ours to resolve.
        assert_eq!(expand_tilde_str("~other/x"), "~other/x");
        // Non-leading tildes are untouched.
        assert_eq!(expand_tilde_str("/opt/~/x"), "/opt/~/x");
        assert_eq!(expand_tilde_str("plain"), "plain");
    }

    #[test]
    fn expands_tilde_in_command_path_and_args() {
        let home = std::env::var("HOME").unwrap();
        let yaml = r#"
agents:
  - name: claude
    command:
      type: stdio
      command: ~/nvm/bin/claude-agent-acp
      args: ["--config", "~/cfg.yaml", "--flag"]
    model_selection: { type: config-option }
    models:
      - id: sonnet
        cost_rank: 1
"#;
        let cfg = Config::from_yaml(yaml).unwrap();
        assert_eq!(
            cfg.agents[0].command.command,
            format!("{home}/nvm/bin/claude-agent-acp")
        );
        assert_eq!(
            cfg.agents[0].command.args,
            vec![
                "--config".to_string(),
                format!("{home}/cfg.yaml"),
                "--flag".to_string()
            ]
        );
    }

    /// The shipped examples are not documentation — `router-preferred.yaml` is
    /// fetched at the pinned rev and installed as the box's actual defaults, so a
    /// malformed one breaks every session's provider launch. Nothing used to parse
    /// them, and a duplicate `models:` key introduced while adding a comment block
    /// shipped and reached a box before `check-config` caught it.
    #[test]
    fn shipped_example_configs_parse_and_validate() {
        for (name, yaml) in [
            (
                "router-preferred.yaml",
                include_str!("../examples/router-preferred.yaml"),
            ),
            (
                "router-full.yaml",
                include_str!("../examples/router-full.yaml"),
            ),
            (
                "router-accounts.yaml",
                include_str!("../examples/router-accounts.yaml"),
            ),
        ] {
            let cfg = Config::from_yaml(yaml)
                .unwrap_or_else(|e| panic!("examples/{name} must parse: {e}"));
            cfg.validate()
                .unwrap_or_else(|e| panic!("examples/{name} must validate: {e}"));
            assert!(
                !cfg.agents.is_empty(),
                "examples/{name} must define at least one agent"
            );
        }
    }

    #[test]
    fn expands_accounts_with_independent_reserves_and_shared_lineage() {
        let yaml = format!(
            "{}\n    lineage: anthropic\n    usage_source: {{type: anthropic-oauth}}\n    reserve_capacity: {{weekly: 10, session: 20}}\n    accounts:\n      - name: work\n        env: [{{name: CLAUDE_CONFIG_DIR, value: /tmp/work}}]\n      - name: personal\n        env: [{{name: CLAUDE_CONFIG_DIR, value: /tmp/personal}}]\n        reserve_capacity: {{weekly: 5}}\n",
            minimal_yaml().trim_end()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(cfg.agents.len(), 2);
        assert_eq!(cfg.agents[0].name, "claude@work");
        assert_eq!(cfg.agents[1].name, "claude@personal");
        assert_eq!(cfg.agents[0].lineage, cfg.agents[1].lineage);
        assert_eq!(cfg.agents[0].reserve_capacity.session, 20.0);
        assert_eq!(cfg.agents[1].reserve_capacity.weekly, 5.0);
        assert_eq!(cfg.agents[1].reserve_capacity.session, 20.0);
        assert_ne!(
            cfg.agents[0].config_dir("CLAUDE_CONFIG_DIR", ".claude"),
            cfg.agents[1].config_dir("CLAUDE_CONFIG_DIR", ".claude")
        );
    }

    #[test]
    fn expands_account_plan_from_each_nested_account() {
        let yaml = format!(
            "{}\n    account_plan: pro\n    accounts:\n      - name: work\n        account_plan: team\n      - name: personal\n",
            minimal_yaml().trim_end()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(cfg.agents[0].account_plan.as_deref(), Some("team"));
        assert_eq!(cfg.agents[1].account_plan.as_deref(), Some("pro"));
    }

    #[test]
    fn rejects_invalid_or_unenforceable_reserves_and_duplicate_accounts() {
        for reserve in ["-1", "101", ".nan"] {
            let yaml = format!(
                "{}\n    usage_source: {{type: anthropic-oauth}}\n    reserve_capacity: {{weekly: {reserve}}}\n",
                minimal_yaml().trim_end()
            );
            assert!(
                Config::from_yaml(&yaml)
                    .unwrap_err()
                    .0
                    .contains("reserve_capacity.weekly")
            );
        }
        let yaml = format!(
            "{}\n    reserve_capacity: {{session: 20}}\n",
            minimal_yaml().trim_end()
        );
        assert!(
            Config::from_yaml(&yaml)
                .unwrap_err()
                .0
                .contains("usage_source")
        );
        let yaml = format!(
            "{}\n    accounts: [{{name: work}}, {{name: work}}]\n",
            minimal_yaml().trim_end()
        );
        assert!(
            Config::from_yaml(&yaml)
                .unwrap_err()
                .0
                .contains("duplicate agent")
        );
    }
}
