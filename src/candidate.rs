//! Candidate structs, score table, capability requirements.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

use agent_client_protocol::schema::v1::{ContentBlock, PromptCapabilities};

/// A candidate is an `(agent, model)` pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CandidateId {
    pub agent: String,
    pub model: String,
}

/// Canonical reasoning-effort levels understood by router-acp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffortLevel {
    Auto,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl EffortLevel {
    pub const ALL: [Self; 6] = [
        Self::Auto,
        Self::Low,
        Self::Medium,
        Self::High,
        Self::Xhigh,
        Self::Max,
    ];

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|level| level.as_str().eq_ignore_ascii_case(value))
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

/// The result of resolving a canonical effort request for one candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffortResolution {
    pub requested: EffortLevel,
    pub resolved: Option<EffortLevel>,
    pub provider_value: Option<String>,
}

impl CandidateId {
    pub fn new(agent: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            agent: agent.into(),
            model: model.into(),
        }
    }

    /// Parse `agent/model-id`. Model ids may themselves contain `/`.
    pub fn parse(s: &str) -> Option<Self> {
        let (agent, model) = s.split_once('/')?;
        if agent.is_empty() || model.is_empty() {
            return None;
        }
        Some(Self::new(agent, model))
    }
}

impl fmt::Display for CandidateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.agent, self.model)
    }
}

/// Task classes used by the classifier and score table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum TaskClass {
    UiTweak,
    BugFix,
    Feature,
    Refactor,
    Algorithms,
    Architecture,
    Research,
    Writing,
    Ops,
    CodingGeneral,
}

impl TaskClass {
    pub const ALL: [TaskClass; 10] = [
        TaskClass::UiTweak,
        TaskClass::BugFix,
        TaskClass::Feature,
        TaskClass::Refactor,
        TaskClass::Algorithms,
        TaskClass::Architecture,
        TaskClass::Research,
        TaskClass::Writing,
        TaskClass::Ops,
        TaskClass::CodingGeneral,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            TaskClass::UiTweak => "UiTweak",
            TaskClass::BugFix => "BugFix",
            TaskClass::Feature => "Feature",
            TaskClass::Refactor => "Refactor",
            TaskClass::Algorithms => "Algorithms",
            TaskClass::Architecture => "Architecture",
            TaskClass::Research => "Research",
            TaskClass::Writing => "Writing",
            TaskClass::Ops => "Ops",
            TaskClass::CodingGeneral => "CodingGeneral",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|c| c.as_str() == s)
    }

    pub fn is_coding(&self) -> bool {
        !matches!(self, TaskClass::Research | TaskClass::Writing)
    }
}

/// Coding quality tier following OpenRouter pareto semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum CodingTier {
    Low,
    Medium,
    High,
}

impl CodingTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            CodingTier::Low => "low",
            CodingTier::Medium => "medium",
            CodingTier::High => "high",
        }
    }

    /// Map a `min_coding_score` into a tier by OpenRouter semantics.
    pub fn from_min_score(min_coding_score: Option<f64>) -> Self {
        match min_coding_score {
            None => CodingTier::High,
            Some(s) if s >= 0.66 => CodingTier::High,
            Some(s) if s >= 0.33 => CodingTier::Medium,
            Some(_) => CodingTier::Low,
        }
    }

    /// Tier walk order when the requested tier has no routeable candidates:
    /// the requested tier first, then neighbors nearest-first (preferring the
    /// higher tier on ties).
    pub fn walk_order(&self) -> [CodingTier; 3] {
        match self {
            CodingTier::High => [CodingTier::High, CodingTier::Medium, CodingTier::Low],
            CodingTier::Medium => [CodingTier::Medium, CodingTier::High, CodingTier::Low],
            CodingTier::Low => [CodingTier::Low, CodingTier::Medium, CodingTier::High],
        }
    }
}

/// Capabilities a prompt requires of the downstream agent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequiredCaps {
    pub image: bool,
    pub audio: bool,
    pub embedded_context: bool,
}

impl RequiredCaps {
    pub fn from_prompt(prompt: &[ContentBlock]) -> Self {
        let mut caps = Self::default();
        for block in prompt {
            match block {
                ContentBlock::Image(_) => caps.image = true,
                ContentBlock::Audio(_) => caps.audio = true,
                ContentBlock::Resource(_) => caps.embedded_context = true,
                ContentBlock::Text(_) | ContentBlock::ResourceLink(_) => {}
                _ => {}
            }
        }
        caps
    }

    pub fn satisfied_by(&self, caps: &PromptCapabilities) -> bool {
        (!self.image || caps.image)
            && (!self.audio || caps.audio)
            && (!self.embedded_context || caps.embedded_context)
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// One entry in the score-table YAML, keyed by candidate pattern.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ScoreEntryRaw {
    #[serde(default)]
    pub coding_tier: Option<CodingTier>,
    #[serde(default)]
    pub coding_percentile: Option<f64>,
    #[serde(default)]
    pub context_window: Option<u64>,
    /// Largest `max_tokens` the model accepts. A request asking for more cannot
    /// be rerouted here.
    #[serde(default)]
    pub max_output_tokens: Option<u64>,
    /// Whether the model accepts `thinking: {type: adaptive}`. Older models take
    /// only the fixed-budget form and reject adaptive outright.
    #[serde(default)]
    pub adaptive_thinking: Option<bool>,
    /// Whether the model accepts `output_config.effort`.
    /// This remains a wire-shape compatibility flag; it is deliberately
    /// independent from the canonical level support below.
    #[serde(default)]
    pub effort: Option<bool>,
    /// Canonical effort levels this candidate supports. Omitted means the
    /// candidate does not advertise level-based effort control.
    #[serde(default)]
    pub effort_levels: Option<Vec<EffortLevel>>,
    /// Canonical level to provider-native parameter value.
    #[serde(default)]
    pub effort_mapping: BTreeMap<EffortLevel, String>,
    #[serde(default)]
    pub tags: Vec<String>,
    /// Per-class quality scores in [0, 1], keyed by task class name.
    #[serde(default)]
    pub quality: HashMap<String, f64>,
    /// Fallback quality when a class has no explicit entry.
    #[serde(default)]
    pub default_quality: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScoreTableRaw {
    #[serde(default)]
    pub version: Option<u32>,
    /// Entries are matched in order; the first pattern matching
    /// `agent/model` wins.
    pub candidates: Vec<ScoreTablePatternEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScoreTablePatternEntry {
    pub pattern: String,
    #[serde(flatten)]
    pub entry: ScoreEntryRaw,
}

/// Resolved scores for one candidate.
#[derive(Debug, Clone)]
pub struct ResolvedScores {
    pub coding_tier: CodingTier,
    pub coding_percentile: Option<f64>,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub adaptive_thinking: bool,
    pub effort: bool,
    pub effort_levels: Vec<EffortLevel>,
    pub effort_mapping: BTreeMap<EffortLevel, String>,
    pub tags: Vec<String>,
    quality: HashMap<TaskClass, f64>,
    default_quality: f64,
}

impl ResolvedScores {
    pub fn quality(&self, class: TaskClass) -> f64 {
        self.quality
            .get(&class)
            .copied()
            .unwrap_or(self.default_quality)
    }

    /// Resolve a requested canonical level to this candidate's closest
    /// advertised provider mapping. `None` means omit the provider parameter.
    pub fn resolve_effort(&self, requested: EffortLevel) -> EffortResolution {
        let supported: Vec<_> = self
            .effort_levels
            .iter()
            .copied()
            .filter(|level| *level != EffortLevel::Auto && self.effort_mapping.contains_key(level))
            .collect();
        let resolved = supported.into_iter().min_by_key(|level| {
            let requested_rank = requested as i16;
            let level_rank = *level as i16;
            (requested_rank - level_rank).unsigned_abs()
        });
        let provider_value = resolved.and_then(|level| self.effort_mapping.get(&level).cloned());
        EffortResolution {
            requested,
            resolved,
            provider_value,
        }
    }
}

impl Default for ResolvedScores {
    fn default() -> Self {
        Self {
            coding_tier: CodingTier::Medium,
            coding_percentile: None,
            context_window: None,
            max_output_tokens: None,
            adaptive_thinking: true,
            effort: true,
            effort_levels: Vec::new(),
            effort_mapping: BTreeMap::new(),
            tags: Vec::new(),
            quality: HashMap::new(),
            default_quality: 0.5,
        }
    }
}

/// Score table shipped with the binary and overrideable by path.
#[derive(Debug, Clone)]
pub struct ScoreTable {
    entries: Vec<(String, ResolvedScores)>,
}

pub const BUILTIN_SCORE_TABLE: &str = include_str!("../data/scores.yaml");
pub const QUALITY_MIN: f64 = 0.5;
pub const QUALITY_MAX: f64 = 3.5;

/// Convert the benchmark capability scale into the 0..1 value used by utility
/// and confidence math. Raw scores stay visible for ordering and diagnostics.
pub fn quality_utility(score: f64) -> f64 {
    ((score.clamp(QUALITY_MIN, QUALITY_MAX) - QUALITY_MIN) / (QUALITY_MAX - QUALITY_MIN))
        .clamp(0.0, 1.0)
}

/// Capability demand on the benchmark scale. Editing/operational work starts
/// minimal, implementation slightly higher, and open-ended reasoning at
/// standard capability; classified complexity adds up to two points.
pub fn quality_demand(class: TaskClass, complexity: f64) -> f64 {
    let base = match class {
        TaskClass::UiTweak | TaskClass::Writing | TaskClass::Ops => 1.0,
        TaskClass::BugFix | TaskClass::Feature | TaskClass::Refactor | TaskClass::CodingGeneral => {
            1.2
        }
        TaskClass::Algorithms | TaskClass::Architecture | TaskClass::Research => 1.5,
    };
    (base + 2.0 * complexity.clamp(0.0, 1.0)).min(3.0)
}

/// Confidence that a score meets this task's demand. Meeting or exceeding the
/// demand is full confidence; falling short is measured across the usable
/// 0.5..demand interval before observed struggle is applied.
pub fn quality_confidence(score: f64, class: TaskClass, complexity: f64) -> f64 {
    let demand = quality_demand(class, complexity);
    ((score.clamp(QUALITY_MIN, QUALITY_MAX) - QUALITY_MIN) / (demand - QUALITY_MIN)).clamp(0.0, 1.0)
}

impl ScoreTable {
    pub fn builtin() -> Self {
        Self::from_yaml(BUILTIN_SCORE_TABLE).expect("built-in score table must parse")
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        let raw: ScoreTableRaw =
            serde_yaml::from_str(yaml).map_err(|e| format!("invalid score table YAML: {e}"))?;
        let mut entries = Vec::new();
        for pattern_entry in raw.candidates {
            let raw_entry = pattern_entry.entry;
            let mut quality = HashMap::new();
            for (class_name, score) in &raw_entry.quality {
                match TaskClass::parse(class_name) {
                    Some(class) => {
                        quality.insert(class, score.clamp(QUALITY_MIN, QUALITY_MAX));
                    }
                    None => {
                        return Err(format!(
                            "score table pattern `{}`: unknown task class `{class_name}`",
                            pattern_entry.pattern
                        ));
                    }
                }
            }
            entries.push((
                pattern_entry.pattern,
                ResolvedScores {
                    coding_tier: raw_entry.coding_tier.unwrap_or(CodingTier::Medium),
                    coding_percentile: raw_entry.coding_percentile,
                    context_window: raw_entry.context_window,
                    max_output_tokens: raw_entry.max_output_tokens,
                    adaptive_thinking: raw_entry.adaptive_thinking.unwrap_or(true),
                    effort: raw_entry.effort.unwrap_or(true),
                    effort_levels: raw_entry.effort_levels.unwrap_or_default(),
                    effort_mapping: raw_entry.effort_mapping,
                    tags: raw_entry.tags,
                    quality,
                    default_quality: raw_entry
                        .default_quality
                        .unwrap_or(QUALITY_MIN)
                        .clamp(QUALITY_MIN, QUALITY_MAX),
                },
            ));
        }
        Ok(Self { entries })
    }

    pub fn from_file(path: &Path) -> Result<Self, String> {
        let yaml = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read score table {}: {e}", path.display()))?;
        Self::from_yaml(&yaml)
    }

    /// Look up scores for a candidate; the first matching pattern wins,
    /// falling back to neutral defaults.
    pub fn lookup(&self, id: &CandidateId) -> ResolvedScores {
        let key = id.to_string();
        for (pattern, entry) in &self.entries {
            if glob_match(pattern, &key) {
                return entry.clone();
            }
        }
        ResolvedScores::default()
    }
}

/// Case-insensitive glob match supporting `*` (any run of characters).
pub fn glob_match(pattern: &str, text: &str) -> bool {
    fn inner(p: &[u8], t: &[u8]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some((b'*', rest)) => (0..=t.len()).any(|i| inner(rest, &t[i..])),
            Some((c, rest)) => match t.split_first() {
                Some((tc, trest)) => c.eq_ignore_ascii_case(tc) && inner(rest, trest),
                None => false,
            },
        }
    }
    inner(pattern.as_bytes(), text.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_id_parses_and_displays() {
        let id = CandidateId::parse("claude/claude-sonnet-4.5").unwrap();
        assert_eq!(id.agent, "claude");
        assert_eq!(id.model, "claude-sonnet-4.5");
        assert_eq!(id.to_string(), "claude/claude-sonnet-4.5");
        assert!(CandidateId::parse("nodash").is_none());
        assert!(CandidateId::parse("/x").is_none());
        assert!(CandidateId::parse("x/").is_none());
    }

    #[test]
    fn glob_matching() {
        assert!(glob_match("*", "anything/at-all"));
        assert!(glob_match("claude/*", "claude/sonnet"));
        assert!(glob_match("*sonnet*", "claude/claude-sonnet-4"));
        assert!(!glob_match("codex/*", "claude/sonnet"));
        assert!(glob_match("CLAUDE/Sonnet", "claude/sonnet"));
    }

    #[test]
    fn tier_mapping_follows_openrouter_semantics() {
        assert_eq!(CodingTier::from_min_score(None), CodingTier::High);
        assert_eq!(CodingTier::from_min_score(Some(0.66)), CodingTier::High);
        assert_eq!(CodingTier::from_min_score(Some(0.5)), CodingTier::Medium);
        assert_eq!(CodingTier::from_min_score(Some(0.33)), CodingTier::Medium);
        assert_eq!(CodingTier::from_min_score(Some(0.1)), CodingTier::Low);
    }

    #[test]
    fn builtin_score_table_parses() {
        let table = ScoreTable::builtin();
        let scores = table.lookup(&CandidateId::new("claude", "claude-opus-4"));
        assert_eq!(scores.coding_tier, CodingTier::High);
        let unknown = table.lookup(&CandidateId::new("nobody", "nothing"));
        assert_eq!(unknown.quality(TaskClass::BugFix), 0.5);
    }

    #[test]
    fn benchmark_quality_has_stable_utility_meaning() {
        assert_eq!(quality_utility(0.5), 0.0);
        assert_eq!(quality_utility(2.0), 0.5);
        assert_eq!(quality_utility(3.5), 1.0);
        assert_eq!(quality_utility(-10.0), 0.0);
        assert_eq!(quality_utility(10.0), 1.0);
    }

    #[test]
    fn task_confidence_is_full_when_capability_meets_demand() {
        assert_eq!(quality_confidence(1.3, TaskClass::UiTweak, 0.0), 1.0);
        assert!(quality_confidence(1.3, TaskClass::Architecture, 0.5) < 0.5);
        assert_eq!(quality_demand(TaskClass::Architecture, 1.0), 3.0);
    }

    #[test]
    fn effort_levels_parse_and_normalize_to_supported_provider_values() {
        assert_eq!(EffortLevel::parse("xhigh"), Some(EffortLevel::Xhigh));
        assert_eq!(EffortLevel::parse("AUTO"), Some(EffortLevel::Auto));
        assert_eq!(EffortLevel::parse("turbo"), None);
        let scores = ScoreTable::from_yaml(
            "candidates:\n  - pattern: '*model*'\n    effort: false\n    effort_levels: [low, high]\n    effort_mapping: { low: minimal, high: intensive }\n",
        )
        .unwrap()
        .lookup(&CandidateId::new("test", "model"));
        // The legacy wire flag remains independent of level support.
        assert!(!scores.effort);
        let normalized = scores.resolve_effort(EffortLevel::Xhigh);
        assert_eq!(normalized.resolved, Some(EffortLevel::High));
        assert_eq!(normalized.provider_value.as_deref(), Some("intensive"));
        assert_eq!(
            ResolvedScores::default()
                .resolve_effort(EffortLevel::High)
                .resolved,
            None
        );
    }
}

#[cfg(test)]
mod score_resolution_tests {
    use super::*;

    /// Regression: `*gpt-5*` must not shadow `*mini*` — mini-class models
    /// are medium tier, not opus-grade (this once routed hour-long
    /// investigations to gpt-5.4-mini).
    #[test]
    fn mini_models_resolve_to_medium_tier_not_gpt5_family() {
        let t = ScoreTable::builtin();
        let mini = t.lookup(&CandidateId::parse("codex/gpt-5.4-mini").unwrap());
        let full = t.lookup(&CandidateId::parse("codex/gpt-5.5").unwrap());
        assert_eq!(mini.coding_tier, CodingTier::Medium);
        assert_eq!(mini.context_window, Some(400_000));
        assert!(mini.quality(TaskClass::Research) < full.quality(TaskClass::Research));
        assert_eq!(full.coding_tier, CodingTier::High);
    }

    /// Regression: "gemini" contains the substring "mini", so a `*mini*` entry
    /// above the gemini ones silently scores Gemini Pro as a small mini-class
    /// model. Order in `data/scores.yaml` is the only thing preventing it.
    #[test]
    fn gemini_ids_are_not_shadowed_by_the_mini_pattern() {
        let t = ScoreTable::builtin();
        let pro = t.lookup(&CandidateId::parse("gemini/gemini-3-pro").unwrap());
        let flash = t.lookup(&CandidateId::parse("gemini/gemini-3-flash").unwrap());
        assert_eq!(pro.coding_tier, CodingTier::High, "gemini pro is high tier");
        assert_eq!(pro.context_window, Some(1_000_000), "gemini's own window");
        assert!(
            pro.quality(TaskClass::CodingGeneral) > flash.quality(TaskClass::CodingGeneral),
            "pro outranks flash"
        );
        assert_eq!(flash.context_window, Some(1_000_000), "flash is not mini");
    }

    #[test]
    fn fable_models_score_at_the_top_tier() {
        let t = ScoreTable::builtin();
        let fable = t.lookup(&CandidateId::parse("claude/claude-fable-5[1m]").unwrap());
        assert_eq!(fable.coding_tier, CodingTier::High);
        assert!(fable.quality(TaskClass::Architecture) >= 0.95);
        let sonnet = t.lookup(&CandidateId::parse("claude/sonnet").unwrap());
        assert!(
            fable.quality(TaskClass::Research) > sonnet.quality(TaskClass::Research),
            "fable outranks sonnet"
        );
    }

    /// The claude ladder since the 2026-07-24 Opus 5 card: Opus outscores
    /// Fable (it beats Fable on Terminal-Bench and DeepSWE and trails by
    /// 0.8pt on SWE-Bench-Pro) at half the price and cost_rank 4 vs 5, so
    /// cost-aware `auto` prefers Opus and Fable wins via pins, planner globs,
    /// and escalation. That restores the original routing intent — the narrow
    /// pre-calibration gap existed precisely so Opus won the everyday work —
    /// which the Opus 4.8 benchmark proxy had silently inverted. Grok 4.5
    /// stays below both.
    #[test]
    fn opus5_outranks_grok_and_leads_cost_aware_auto() {
        let t = ScoreTable::builtin();
        let opus = t.lookup(&CandidateId::parse("claude/opus[1m]").unwrap());
        let grok = t.lookup(&CandidateId::parse("grok/grok-4.5").unwrap());
        let fable = t.lookup(&CandidateId::parse("claude/claude-fable-5[1m]").unwrap());
        assert_eq!(opus.coding_tier, CodingTier::High);
        assert!(
            opus.quality(TaskClass::CodingGeneral) > grok.quality(TaskClass::CodingGeneral),
            "opus5 ({}) must beat grok-4.5 ({})",
            opus.quality(TaskClass::CodingGeneral),
            grok.quality(TaskClass::CodingGeneral)
        );
        assert!(
            opus.quality(TaskClass::CodingGeneral) > fable.quality(TaskClass::CodingGeneral),
            "opus5 leads the benchmark-calibrated claude ladder ({} vs fable {})",
            opus.quality(TaskClass::CodingGeneral),
            fable.quality(TaskClass::CodingGeneral)
        );
        // Wire alias `claude-opus-5` and legacy `claude-opus-4` both match `*opus*`.
        let by_api_id = t.lookup(&CandidateId::new("claude", "claude-opus-5"));
        assert_eq!(
            by_api_id.quality(TaskClass::BugFix),
            opus.quality(TaskClass::BugFix)
        );
    }

    #[test]
    fn sol_line_scores_at_the_top_and_beats_generic_gpt5() {
        let t = ScoreTable::builtin();
        // Whatever the adapter names it, a "sol" id matches the frontier entry
        // (not the generic `*gpt-5*` fallback), across id shapes.
        for id in ["codex/sol", "codex/gpt-sol", "codex/gpt-5-sol"] {
            let sol = t.lookup(&CandidateId::parse(id).unwrap());
            assert_eq!(sol.coding_tier, CodingTier::High, "{id} is high tier");
            assert!(
                sol.quality(TaskClass::CodingGeneral) >= 0.95,
                "{id} rated comparable to fable"
            );
        }
        let sol = t.lookup(&CandidateId::parse("codex/gpt-5-sol").unwrap());
        let gpt5 = t.lookup(&CandidateId::parse("codex/gpt-5.5").unwrap());
        assert!(
            sol.quality(TaskClass::CodingGeneral) > gpt5.quality(TaskClass::CodingGeneral),
            "sol is not shadowed by the generic *gpt-5* pattern"
        );
        // Must not collide with sonnet (no "sol" substring).
        let sonnet = t.lookup(&CandidateId::parse("claude/sonnet").unwrap());
        assert!(sonnet.quality(TaskClass::CodingGeneral) < sol.quality(TaskClass::CodingGeneral));
    }

    #[test]
    fn sol_terra_luna_tier_ordering() {
        let t = ScoreTable::builtin();
        let q = |id: &str| {
            t.lookup(&CandidateId::parse(id).unwrap())
                .quality(TaskClass::CodingGeneral)
        };
        // Provisional ordering: sol (frontier) > terra (strong) > luna (small).
        assert!(q("codex/gpt-5.6-sol") > q("codex/gpt-5.6-terra"));
        assert!(q("codex/gpt-5.6-terra") > q("codex/gpt-5.6-luna"));
        // terra is high tier; luna is medium; and none fall through to *gpt-5*.
        assert_eq!(
            t.lookup(&CandidateId::parse("codex/gpt-5.6-terra").unwrap())
                .coding_tier,
            CodingTier::High
        );
        assert_eq!(
            t.lookup(&CandidateId::parse("codex/gpt-5.6-luna").unwrap())
                .coding_tier,
            CodingTier::Medium
        );
        // luna must not collide with any real id, and terra/luna beat the
        // generic gpt-5 fallback only where intended (terra yes, luna no).
        assert!(q("codex/gpt-5.6-terra") > q("codex/gpt-5.5"));
    }

    #[test]
    fn kimi_thinking_outranks_base_and_neither_falls_through() {
        let t = ScoreTable::builtin();
        let base = t.lookup(&CandidateId::parse("kimi/kimi-k2").unwrap());
        let thinking = t.lookup(&CandidateId::parse("kimi/kimi-k2-thinking").unwrap());
        // Both kimi entries are real (not the generic default fallback) and high
        // coding tier; the reasoning variant scores strictly higher.
        assert_eq!(base.coding_tier, CodingTier::High);
        assert_eq!(thinking.coding_tier, CodingTier::High);
        assert!(
            thinking.quality(TaskClass::Algorithms) > base.quality(TaskClass::Algorithms),
            "`*kimi*k2*thinking*` must win over the broad `*kimi*` (first-match order)"
        );
        // A plain kimi id and kimi-latest both resolve to the base kimi entry.
        let latest = t.lookup(&CandidateId::parse("kimi/kimi-latest").unwrap());
        assert_eq!(
            latest.quality(TaskClass::CodingGeneral),
            base.quality(TaskClass::CodingGeneral)
        );
    }
}
