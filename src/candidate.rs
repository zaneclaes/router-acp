//! Candidate structs, score table, capability requirements.

use std::collections::HashMap;
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
}

impl Default for ResolvedScores {
    fn default() -> Self {
        Self {
            coding_tier: CodingTier::Medium,
            coding_percentile: None,
            context_window: None,
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
                        quality.insert(class, score.clamp(0.0, 1.0));
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
                    tags: raw_entry.tags,
                    quality,
                    default_quality: raw_entry.default_quality.unwrap_or(0.5).clamp(0.0, 1.0),
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
        assert_eq!(mini.coding_tier, CodingTier::Medium);
        assert!(
            mini.quality(TaskClass::Research) <= 0.5,
            "mini is not opus-grade"
        );
        let full = t.lookup(&CandidateId::parse("codex/gpt-5.5").unwrap());
        assert_eq!(full.coding_tier, CodingTier::High);
        assert!(full.quality(TaskClass::Research) > 0.7);
    }

    #[test]
    fn fable_models_score_at_the_top() {
        let t = ScoreTable::builtin();
        let fable = t.lookup(&CandidateId::parse("claude/claude-fable-5[1m]").unwrap());
        assert_eq!(fable.coding_tier, CodingTier::High);
        assert!(fable.quality(TaskClass::Architecture) >= 0.95);
        let opus = t.lookup(&CandidateId::parse("claude/opus[1m]").unwrap());
        assert!(
            fable.quality(TaskClass::Research) > opus.quality(TaskClass::Research),
            "fable outranks opus"
        );
    }
}
