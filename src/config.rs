//! YAML configuration: parsing, defaults, env interpolation, validation.

use std::collections::HashSet;
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
}

impl StrategyKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            StrategyKind::Static => "static",
            StrategyKind::Auto => "auto",
            StrategyKind::ParetoCode => "pareto-code",
            StrategyKind::Escalation => "escalation",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "static" => Some(StrategyKind::Static),
            "auto" => Some(StrategyKind::Auto),
            "pareto-code" => Some(StrategyKind::ParetoCode),
            "escalation" => Some(StrategyKind::Escalation),
            _ => None,
        }
    }
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
            max_concurrent: default_max_concurrent(),
            socket_path: None,
            complexity_cap: default_delegate_complexity_cap(),
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
}

fn default_cost_aversion() -> f64 {
    0.1
}

fn default_hint_ttl_secs() -> u64 {
    10 * 60
}

impl Default for AvailabilityPreferenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cost_aversion: default_cost_aversion(),
            hint_ttl_secs: default_hint_ttl_secs(),
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoUpgradeConfig {
    /// Master switch. When false, sessions never auto-upgrade (explicit
    /// `[router: switch=...]` still works).
    #[serde(default = "default_true")]
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
            enabled: true,
            confidence_threshold: default_confidence_threshold(),
        }
    }
}

/// Load ticket details into the prompt when a ticket id is referenced. When a
/// prompt mentions `<prefix><digits>` (e.g. `HAI-1234`), the router runs
/// `command` (with `$TICKET` substituted) and prepends its stdout to the prompt
/// before classification and orchestration detection — so "Fix HAI-1234"
/// becomes a rich prompt that routes (and possibly orchestrates) on the
/// ticket's actual content. Fails open: a failed/slow fetch leaves the prompt
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

/// Force prompts that invoke a given skill onto a specific class of models.
/// When a prompt matches `pattern` (case-insensitive substring, e.g. the
/// skill name) and the pinned candidate is not in `candidates`, the session
/// switches to the best routeable candidate that is.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillRoute {
    /// Substring matched against prompt text (typically the skill name).
    pub pattern: String,
    /// Acceptable candidate globs (the required "model class"), e.g.
    /// `["*opus*", "*gpt-5.5*"]`.
    pub candidates: Vec<String>,
}

/// Automatic orchestration. When a prompt reads as a multi-part task list
/// (markdown list, inline `(1)(2)`, or "first … then … finally" ordering), the
/// router runs a plan → delegate → review → submit pipeline entirely in-process:
/// it steers/switches the session to a `planner` model and injects an
/// orchestration protocol instructing that model to decompose the task, delegate
/// each part via `delegate_task` (routed per-complexity in isolated
/// sub-sessions), have a different-lineage `reviewer` verify the net result, and
/// submit per `submit`. Delegation in an orchestrating session is allowed to
/// same-/higher-tier peers (so cross-lineage review works), unlike ordinary
/// cost-shedding delegation. An explicit `[router: …]` directive or `model:`
/// shorthand on the prompt suppresses auto-orchestration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestrationConfig {
    /// Master switch. Off by default so it never surprises a plain session;
    /// turn it on in your router.yaml to get automatic decomposition.
    #[serde(default)]
    pub enabled: bool,
    /// Minimum number of detected parts before a prompt is treated as an
    /// orchestratable list.
    #[serde(default = "default_min_items")]
    pub min_items: usize,
    /// Candidate globs for the planner/orchestrator, best first. The first one
    /// with an eligible candidate wins (like `skill_routing`).
    #[serde(default = "default_planner")]
    pub planner: Vec<String>,
    /// Preferred cross-lineage reviewer candidate globs, best first. Passed to
    /// the orchestrator as guidance; it should pick one of a *different* lineage
    /// than the planner for the review pass.
    #[serde(default = "default_reviewer")]
    pub reviewer: Vec<String>,
    /// Submission gate handed to the orchestrator: `never` | `branch` | `pr` |
    /// `merge`. A merge is only permitted after the review pass approves.
    #[serde(default = "default_submit")]
    pub submit: String,
    /// Maximum review → fix → re-review rounds.
    #[serde(default = "default_max_fix_rounds")]
    pub max_fix_rounds: u32,
    /// Planner self-confidence bar for skipping the review pass. After
    /// integrating, the planner states its confidence (0.0–1.0) that the
    /// implementation is correct; strictly above this bar the review is
    /// skipped with a note. `submit: merge` always reviews regardless.
    #[serde(default = "default_review_confidence")]
    pub review_confidence: f64,
}

fn default_min_items() -> usize {
    2
}

fn default_planner() -> Vec<String> {
    // Opus 5 outranks Grok 4.5; prefer it over gpt-5.5 when both are free.
    vec![
        "*sol*".to_string(),
        "*fable*".to_string(),
        "*opus*".to_string(),
        "*grok*".to_string(),
        "*gpt-5.5*".to_string(),
    ]
}

fn default_reviewer() -> Vec<String> {
    vec![
        "*gpt-5.5*".to_string(),
        "*sol*".to_string(),
        "*opus*".to_string(),
        "*fable*".to_string(),
        "*grok*".to_string(),
    ]
}

fn default_submit() -> String {
    "branch".to_string()
}

fn default_max_fix_rounds() -> u32 {
    2
}

fn default_review_confidence() -> f64 {
    0.8
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_items: default_min_items(),
            planner: default_planner(),
            reviewer: default_reviewer(),
            submit: default_submit(),
            max_fix_rounds: default_max_fix_rounds(),
            review_confidence: default_review_confidence(),
        }
    }
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

/// Composable LLM pre-classifier: one cheap ACP evaluation returns structured
/// decisions for auto-orchestration and host-registered dimensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreClassifierConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Candidate globs for the evaluator seat, preference order (cheapest first).
    #[serde(default = "default_preclass_evaluator")]
    pub evaluator: Vec<String>,
    /// DEPRECATED and ignored. The classifier LLM call is core infrastructure
    /// that must run to completion, so it no longer has a wall-clock timeout: a
    /// failed evaluator is detected as a connection failure and failed over to
    /// the next candidate, and the turn is interrupted only by client
    /// cancellation. Retained (accepted, unused) so existing configs still load.
    #[serde(default = "default_preclass_timeout_ms")]
    pub timeout_ms: u64,
    /// Emit `router-acp · pre-class …` disclosure lines.
    #[serde(default = "default_true")]
    pub disclose: bool,
    /// Minimum confidence to act on the built-in `orchestrate` dimension.
    #[serde(default = "default_orchestrate_min_confidence")]
    pub orchestrate_min_confidence: f64,
    /// Host extensions (e.g. `ui_planning`). One evaluator call covers all.
    #[serde(default)]
    pub dimensions: Vec<PreClassDimension>,
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

fn default_orchestrate_min_confidence() -> f64 {
    0.65
}

impl Default for PreClassifierConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            evaluator: default_preclass_evaluator(),
            timeout_ms: default_preclass_timeout_ms(),
            disclose: true,
            orchestrate_min_confidence: default_orchestrate_min_confidence(),
            dimensions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailoverConfig {
    /// Fail over a pinned session to the next best candidate when the
    /// pinned model is rate-limited or down — only when no output has
    /// streamed for the failing turn (retrying after side effects would
    /// risk duplicating them). Conversation context does not transfer.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Minimum seconds between respawn attempts of a dead downstream
    /// process.
    #[serde(default = "default_respawn_cooldown_secs")]
    pub respawn_cooldown_secs: u64,
    /// Maximum candidates tried per prompt (initial + failovers).
    #[serde(default = "default_failover_attempts")]
    pub max_attempts: u32,
}

fn default_respawn_cooldown_secs() -> u64 {
    30
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
    /// 1 = cheapest/least scarce; larger = more expensive/scarce.
    pub cost_rank: u32,
    /// Optional API-equivalent pricing. It prices every interposed provider
    /// request and synthesizes ACP-turn `cost_usd` only when the adapter
    /// reports no authoritative cost of its own.
    #[serde(default)]
    pub pricing: Option<PricingConfig>,
}

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
    /// Model-company lineage tag (e.g. `anthropic`, `openai`). Defaults to the
    /// agent name. Orchestration's cross-lineage review compares THIS — the
    /// point is a reviewer whose models come from a **different company** (and
    /// thus behave differently), so two agents backed by the same vendor (e.g.
    /// two Claude seats) should declare the same `lineage`.
    #[serde(default)]
    pub lineage: Option<String>,
    /// Optional per-request LLM proxy interposition for this adapter. The
    /// global `llm_proxy.enabled` switch must also be on. The upstream is
    /// explicit because subscription/OAuth endpoints often differ from the
    /// providers' public API endpoints.
    #[serde(default)]
    pub llm_proxy: Option<AgentLlmProxyConfig>,
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
}

fn default_min_cost_weight() -> f64 {
    0.15
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
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
    /// Ticket-reference → context-loading rules (prefix + fetch command).
    #[serde(default)]
    pub ticket_context: Vec<TicketRule>,
    /// Automatic orchestration of multi-part task lists.
    #[serde(default)]
    pub orchestration: OrchestrationConfig,
    /// Composable LLM pre-classifier (orchestration + host dimensions).
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
    #[serde(default)]
    pub enabled: bool,
    /// Must resolve to a loopback socket. Port 0 asks the OS for a free port.
    #[serde(default = "default_llm_proxy_listen")]
    pub listen: String,
    /// Consecutive routine tool-result requests before demotion is considered.
    #[serde(default = "default_llm_routine_streak")]
    pub routine_streak: u32,
    /// Requests a selected model must serve before a non-emergency switch.
    #[serde(default = "default_llm_minimum_dwell_requests")]
    pub minimum_dwell_requests: u32,
    /// Number of subsequent requests for which a difficulty verdict remains
    /// elevated. Zero disables request-count expiry.
    #[serde(default = "default_llm_verdict_ttl_requests")]
    pub verdict_ttl_requests: u32,
    /// Wall-clock expiry for a difficulty verdict. Zero disables time expiry.
    #[serde(default = "default_llm_verdict_ttl_secs")]
    pub verdict_ttl_secs: u64,
    /// Fraction of a target model's configured context window that a request
    /// may occupy. Demotion is refused when the target window is unknown.
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

fn default_llm_routine_streak() -> u32 {
    3
}

fn default_llm_minimum_dwell_requests() -> u32 {
    12
}

fn default_llm_verdict_ttl_requests() -> u32 {
    6
}

fn default_llm_verdict_ttl_secs() -> u64 {
    15 * 60
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

impl Default for LlmProxyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: default_llm_proxy_listen(),
            routine_streak: default_llm_routine_streak(),
            minimum_dwell_requests: default_llm_minimum_dwell_requests(),
            verdict_ttl_requests: default_llm_verdict_ttl_requests(),
            verdict_ttl_secs: default_llm_verdict_ttl_secs(),
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

/// Interpolate `${VAR}` environment references in a string. Unknown
/// variables are left intact so later per-model substitution (e.g.
/// `${model_id}` in spawn-config process templates) still sees them.
pub fn interpolate_env(input: &str) -> String {
    interpolate(input, &|name| std::env::var(name).ok())
}

/// Interpolate `${name}` references using the provided lookup. References
/// the lookup does not resolve are left as-is.
pub fn interpolate(input: &str, lookup: &dyn Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find('}') {
            Some(end) => {
                let name = &after[..end];
                match lookup(name) {
                    Some(value) => out.push_str(&value),
                    None => {
                        out.push_str("${");
                        out.push_str(name);
                        out.push('}');
                    }
                }
                rest = &after[end + 1..];
            }
            None => {
                out.push_str("${");
                rest = after;
            }
        }
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
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let interpolated = interpolate_env(yaml);
        let mut cfg: Config = serde_yaml::from_str(&interpolated)
            .map_err(|e| ConfigError(format!("invalid YAML: {e}")))?;
        cfg.state_file = expand_tilde(&cfg.state_file);
        if let Some(p) = &cfg.delegation.socket_path {
            cfg.delegation.socket_path = Some(expand_tilde(p));
        }
        // Downstream adapters are spawned via `Command::new` (no shell), so a
        // leading `~` in a command path or arg would never be expanded and the
        // spawn would fail — expand it here the same way we do for state paths.
        for agent in &mut cfg.agents {
            agent.command.command = expand_tilde_str(&agent.command.command);
            for arg in &mut agent.command.args {
                *arg = expand_tilde_str(arg);
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

    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let yaml = std::fs::read_to_string(path)
            .map_err(|e| ConfigError(format!("cannot read {}: {e}", path.display())))?;
        Self::from_yaml(&yaml)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.agents.is_empty() {
            return Err(ConfigError(
                "no agents configured; declare at least one agent with one model".into(),
            ));
        }
        let mut names = HashSet::new();
        for agent in &self.agents {
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
            if self.llm_proxy.routine_streak == 0 {
                return Err(ConfigError(
                    "llm_proxy.routine_streak must be at least 1".into(),
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
            if !self.agents.iter().any(|a| a.llm_proxy.is_some()) {
                return Err(ConfigError(
                    "llm_proxy.enabled is true but no agent has agents[].llm_proxy configured"
                        .into(),
                ));
            }
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
                if let Some(pricing) = &model.pricing
                    && (pricing.input_per_mtok < 0.0
                        || pricing.output_per_mtok < 0.0
                        || pricing.cache_read_per_mtok.is_some_and(|v| v < 0.0)
                        || pricing.cache_write_per_mtok.is_some_and(|v| v < 0.0))
                {
                    return Err(ConfigError(format!(
                        "agents.{}.models.{}.pricing rates must be non-negative",
                        agent.name, model.id
                    )));
                }
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
        if self.orchestration.enabled {
            if self.orchestration.planner.is_empty() {
                return Err(ConfigError(
                    "orchestration.enabled is true but orchestration.planner is empty".into(),
                ));
            }
            if self.orchestration.min_items < 2 {
                return Err(ConfigError(
                    "orchestration.min_items must be at least 2".into(),
                ));
            }
            if !matches!(
                self.orchestration.submit.as_str(),
                "never" | "branch" | "pr" | "merge"
            ) {
                return Err(ConfigError(format!(
                    "orchestration.submit must be never|branch|pr|merge, got `{}`",
                    self.orchestration.submit
                )));
            }
            if !(0.0..=1.0).contains(&self.orchestration.review_confidence) {
                return Err(ConfigError(format!(
                    "orchestration.review_confidence must be within 0.0..=1.0, got `{}`",
                    self.orchestration.review_confidence
                )));
            }
        }
        if self.pre_classifier.enabled {
            if self.pre_classifier.evaluator.is_empty() {
                return Err(ConfigError(
                    "pre_classifier.enabled is true but pre_classifier.evaluator is empty".into(),
                ));
            }
            // pre_classifier.timeout_ms is deprecated/ignored (the classifier no
            // longer times out); any value — including 0 — is accepted.
            if !(0.0..=1.0).contains(&self.pre_classifier.orchestrate_min_confidence) {
                return Err(ConfigError(format!(
                    "pre_classifier.orchestrate_min_confidence must be within 0.0..=1.0, got `{}`",
                    self.pre_classifier.orchestrate_min_confidence
                )));
            }
            let mut seen = HashSet::new();
            for dim in &self.pre_classifier.dimensions {
                if dim.id.trim().is_empty() {
                    return Err(ConfigError(
                        "pre_classifier.dimensions: id must not be empty".into(),
                    ));
                }
                if dim.id == "orchestrate" {
                    return Err(ConfigError(
                        "pre_classifier.dimensions: id `orchestrate` is reserved for the built-in dimension"
                            .into(),
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
        Ok(())
    }

    /// State-DB retention derived from `history`.
    pub fn retention(&self) -> crate::state::Retention {
        let secs = parse_history(&self.history).unwrap_or(30 * 24 * 60 * 60);
        crate::state::Retention {
            max_age: std::time::Duration::from_secs(secs),
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
    fn parses_minimal_config_with_defaults() {
        let cfg = Config::from_yaml(minimal_yaml()).unwrap();
        assert_eq!(cfg.router, StrategyKind::Auto);
        assert_eq!(cfg.delegation.max_concurrent, 3);
        assert!(cfg.delegation.enabled);
        assert_eq!(cfg.headroom.window_secs, 5 * 60 * 60);
        assert_eq!(cfg.agents[0].budget_prompts_5h, 400);
        assert_eq!(cfg.routers.auto.cost_quality_tradeoff, 7.0);
        assert_eq!(cfg.orchestration.review_confidence, 0.8);
        assert!(!cfg.llm_proxy.enabled);
        assert_eq!(cfg.llm_proxy.minimum_dwell_requests, 12);
    }

    #[test]
    fn parses_review_confidence_override() {
        let yaml = format!(
            "orchestration:\n  enabled: true\n  review_confidence: 0.95\n{}",
            minimal_yaml()
        );
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(cfg.orchestration.review_confidence, 0.95);
    }

    #[test]
    fn rejects_review_confidence_out_of_range() {
        let yaml = format!(
            "orchestration:\n  enabled: true\n  review_confidence: 1.5\n{}",
            minimal_yaml()
        );
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(err.0.contains("review_confidence"), "{}", err.0);
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
    fn expand_tilde_str_only_touches_leading_home() {
        // SAFETY: test-local env mutation.
        unsafe { std::env::set_var("HOME", "/home/zane") };
        assert_eq!(expand_tilde_str("~"), "/home/zane");
        assert_eq!(expand_tilde_str("~/bin/x"), "/home/zane/bin/x");
        // A bare ~word (another user's home) is not ours to resolve.
        assert_eq!(expand_tilde_str("~other/x"), "~other/x");
        // Non-leading tildes are untouched.
        assert_eq!(expand_tilde_str("/opt/~/x"), "/opt/~/x");
        assert_eq!(expand_tilde_str("plain"), "plain");
    }

    #[test]
    fn expands_tilde_in_command_path_and_args() {
        // SAFETY: test-local env mutation.
        unsafe { std::env::set_var("HOME", "/home/zane") };
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
            "/home/zane/nvm/bin/claude-agent-acp"
        );
        assert_eq!(
            cfg.agents[0].command.args,
            vec!["--config", "/home/zane/cfg.yaml", "--flag"]
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
}
