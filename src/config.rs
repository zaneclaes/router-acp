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
    /// 1 = cheapest/least scarce; larger = more expensive/scarce.
    pub cost_rank: u32,
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
    #[serde(default)]
    pub failover: FailoverConfig,
    #[serde(default)]
    pub auto_upgrade: AutoUpgradeConfig,
    /// Skill → model-class routing rules.
    #[serde(default)]
    pub skill_routing: Vec<SkillRoute>,
    pub agents: Vec<AgentConfig>,
    #[serde(default)]
    pub routers: RoutersConfig,
    /// Timeout for downstream initialize/probe calls, in milliseconds.
    #[serde(default = "default_probe_timeout_ms")]
    pub probe_timeout_ms: u64,
}

fn default_router() -> StrategyKind {
    StrategyKind::Auto
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
        if self.delegation.max_concurrent == 0 {
            return Err(ConfigError("delegation.max_concurrent must be >= 1".into()));
        }
        if !(0.0..=10.0).contains(&self.routers.auto.cost_quality_tradeoff) {
            return Err(ConfigError(
                "routers.auto.cost_quality_tradeoff must be between 0 and 10".into(),
            ));
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
    }

    #[test]
    fn rejects_zero_agents() {
        let err = Config::from_yaml("agents: []").unwrap_err();
        assert!(err.0.contains("no agents"));
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
}
