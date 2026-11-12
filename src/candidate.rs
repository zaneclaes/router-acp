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
    /// True only after the adapter acknowledges its native effort option.
    pub confirmed: bool,
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

/// The score fields of one `data/providers/<provider>.yaml` row.
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
    /// Whether the model accepts mid-conversation `role: "system"` turns
    /// (Claude Code's `mid-conversation-system` beta). Versions that predate
    /// it 400 on the turn; the proxy folds such turns into user turns before
    /// sending when this is false.
    #[serde(default)]
    pub system_turns: Option<bool>,
    /// Canonical effort levels this candidate supports. Omitted means the
    /// candidate does not advertise level-based effort control.
    #[serde(default)]
    pub effort_levels: Option<Vec<EffortLevel>>,
    /// Canonical level to provider-native parameter value.
    #[serde(default)]
    pub effort_mapping: BTreeMap<EffortLevel, String>,
    #[serde(default)]
    pub tags: Vec<String>,
    /// Per-class quality scores, keyed by task class name.
    #[serde(default)]
    pub quality: HashMap<String, f64>,
    /// Fallback quality when a class has no explicit entry.
    #[serde(default)]
    pub default_quality: Option<f64>,
}

/// The catalog fields of one `data/providers/<provider>.yaml` row: what every agent of
/// `provider` inherits as one of its `models:` (`crate::presets`).
pub const CATALOG_FIELDS: [&str; 8] = [
    "provider",
    "family",
    "version",
    "id",
    "api_model",
    "display_name",
    "cost_rank",
    "pricing",
];

/// One `data/providers/<provider>.yaml` row: one exact model.
#[derive(Debug, Clone)]
pub struct ModelRow {
    pub provider: Option<String>,
    /// The model line this row is a version of.
    pub family: Option<String>,
    /// The row's dotted version within its family.
    pub version: Option<String>,
    pub id: String,
    pub api_model: Option<String>,
    /// Whether the row carries quality scores. A model with no evidence of
    /// its own is unscored and never ranked automatically.
    pub scored: bool,
    /// Rows this one must beat on every task class (`provider/id`, or a bare
    /// id of the same provider).
    pub outranks: Vec<String>,
    /// This model's own published benchmark results.
    pub evidence: Vec<serde_yaml::Value>,
    /// The row's catalog fields, as written ([`CATALOG_FIELDS`] without `provider`).
    pub catalog: serde_yaml::Mapping,
    pub scores: ResolvedScores,
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
    pub system_turns: bool,
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
            .filter(|level| *level != EffortLevel::Auto)
            .collect();
        let resolved = supported.into_iter().min_by_key(|level| {
            let requested_rank = requested as i16;
            let level_rank = *level as i16;
            (requested_rank - level_rank).unsigned_abs()
        });
        let provider_value = resolved.map(|level| self.provider_effort(level));
        EffortResolution {
            requested,
            resolved,
            provider_value,
            confirmed: false,
        }
    }

    /// The provider's name for an advertised level: its own name unless
    /// `effort_mapping` renames it.
    fn provider_effort(&self, level: EffortLevel) -> String {
        self.effort_mapping
            .get(&level)
            .cloned()
            .unwrap_or_else(|| level.as_str().to_string())
    }

    /// Resolve an automatic recommendation without increasing it when this
    /// provider lacks the requested level. Explicit requests keep using
    /// [`Self::resolve_effort`], which preserves its closest-level behavior.
    pub fn resolve_automatic_effort(&self, requested: EffortLevel) -> EffortResolution {
        let resolved = self
            .effort_levels
            .iter()
            .copied()
            .filter(|level| *level != EffortLevel::Auto && *level <= requested)
            .max();
        let provider_value = resolved.map(|level| self.provider_effort(level));
        EffortResolution {
            requested,
            resolved,
            provider_value,
            confirmed: false,
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
            system_turns: true,
            effort_levels: Vec::new(),
            effort_mapping: BTreeMap::new(),
            tags: Vec::new(),
            quality: HashMap::new(),
            default_quality: 0.5,
        }
    }
}

/// The model catalog and its scores (`data/providers/<provider>.yaml`), overrideable by path.
#[derive(Debug, Clone)]
pub struct ScoreTable {
    rows: Vec<ModelRow>,
    /// Candidate → `agent/<pinned api_model>`: a version-pinned candidate is
    /// scored as the version it runs (capabilities, quality, context window).
    pinned: HashMap<CandidateId, CandidateId>,
    /// Candidate → `agent/<api_model>` for a selector alias.
    aliases: HashMap<CandidateId, CandidateId>,
}

/// The provider files of `data/providers/`: each provider's default agent
/// settings and the one definition of every model it supports, grouped by
/// family (`models: { opus: [ { version: 5.5, id: claude-opus-5-5, … } ] }`).
pub const BUILTIN_PROVIDERS: [(&str, &str); 4] = [
    ("claude", include_str!("../data/providers/claude.yaml")),
    ("codex", include_str!("../data/providers/codex.yaml")),
    ("grok", include_str!("../data/providers/grok.yaml")),
    ("kimi", include_str!("../data/providers/kimi.yaml")),
];
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
        Self::from_documents(BUILTIN_PROVIDERS.iter().map(|(_, text)| *text))
            .expect("data/providers must parse")
    }

    /// One provider file (`provider:` and `models:` grouped by family) or a
    /// plain `models:` list of rows.
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        Self::from_documents([yaml])
    }

    /// The rows of one document, each as a mapping carrying its own
    /// `provider`, `family` and `version` keys.
    fn document_rows(yaml: &str) -> Result<Vec<serde_yaml::Mapping>, String> {
        let document: serde_yaml::Value =
            serde_yaml::from_str(yaml).map_err(|e| format!("invalid models YAML: {e}"))?;
        let provider = document.get("provider").cloned();
        let as_row = |row: &serde_yaml::Value| {
            row.as_mapping()
                .cloned()
                .ok_or_else(|| "every model must be a mapping".to_string())
        };
        match document.get("models") {
            Some(serde_yaml::Value::Sequence(rows)) => rows.iter().map(as_row).collect(),
            Some(serde_yaml::Value::Mapping(families)) => {
                let mut out = Vec::new();
                for (family, versions) in families {
                    let versions = versions
                        .as_sequence()
                        .ok_or_else(|| format!("family `{family:?}` must list its versions"))?;
                    for version in versions {
                        let mut row = serde_yaml::Mapping::new();
                        if let Some(provider) = &provider {
                            row.insert("provider".into(), provider.clone());
                        }
                        row.insert("family".into(), family.clone());
                        for (key, value) in as_row(version)? {
                            row.insert(key, value);
                        }
                        out.push(row);
                    }
                }
                Ok(out)
            }
            _ => Err("models YAML needs `models:`".to_string()),
        }
    }

    pub fn from_documents<'a>(
        documents: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, String> {
        let mut rows = Vec::new();
        for document in documents {
            rows.extend(Self::document_rows(document)?);
        }
        let mut parsed: Vec<ModelRow> = Vec::new();
        for mut fields in rows {
            let text = |fields: &serde_yaml::Mapping, key: &str| {
                fields
                    .get(key)
                    .and_then(serde_yaml::Value::as_str)
                    .map(str::to_string)
            };
            let id = text(&fields, "id").ok_or("every `models:` row needs an `id`")?;
            let api_model = text(&fields, "api_model");
            for key in std::iter::once(&id).chain(api_model.as_ref()) {
                if key.contains('*') {
                    return Err(format!(
                        "model `{key}`: a row names one exact model, never a pattern"
                    ));
                }
                if let Some(other) = parsed.iter().find(|r| r.answers_to(key)) {
                    return Err(format!(
                        "model `{key}` is listed twice (rows `{}` and `{id}`)",
                        other.id
                    ));
                }
            }
            let provider = text(&fields, "provider");
            let reference = |key: &str| {
                fields
                    .get(key)
                    .cloned()
                    .map(|v| v.as_str().map(str::to_string))
                    .map(|v| v.ok_or(format!("model `{id}`: `{key}` must be a model id")))
                    .transpose()
            };
            let family = reference("family")?;
            let version = match fields.get("version") {
                None => None,
                Some(serde_yaml::Value::Number(n)) => Some(n.to_string()),
                Some(serde_yaml::Value::String(s)) => Some(s.clone()),
                Some(_) => return Err(format!("model `{id}`: `version` must be a dotted number")),
            };
            let outranks: Vec<String> = match fields.shift_remove("outranks") {
                Some(v) => serde_yaml::from_value(v)
                    .map_err(|e| format!("model `{id}`: `outranks`: {e}"))?,
                None => Vec::new(),
            };
            let evidence: Vec<serde_yaml::Value> = match fields.shift_remove("evidence") {
                Some(v) => serde_yaml::from_value(v)
                    .map_err(|e| format!("model `{id}`: `evidence`: {e}"))?,
                None => Vec::new(),
            };
            fields.shift_remove("rank_exception");
            let scored = fields.contains_key("quality") || fields.contains_key("default_quality");
            let mut catalog = serde_yaml::Mapping::new();
            for key in CATALOG_FIELDS {
                if let Some(value) = fields.shift_remove(key)
                    && key != "provider"
                {
                    catalog.insert(key.into(), value);
                }
            }
            let raw: ScoreEntryRaw = serde_yaml::from_value(serde_yaml::Value::Mapping(fields))
                .map_err(|e| format!("model `{id}`: {e}"))?;
            // Advertise only the levels a model really has; a mapping only renames one. Two
            // levels sent as the same provider value would claim a level the model lacks
            // (operator, 2026-11-12: `xhigh: max` beside `max: max`).
            let levels = raw.effort_levels.clone().unwrap_or_default();
            let mut sent: HashMap<String, EffortLevel> = HashMap::new();
            for level in &levels {
                let value = raw
                    .effort_mapping
                    .get(level)
                    .cloned()
                    .unwrap_or_else(|| level.as_str().to_string());
                if let Some(other) = sent.insert(value.clone(), *level) {
                    return Err(format!(
                        "model `{id}`: effort levels `{}` and `{}` are both sent as `{value}`; \
                         advertise only the levels the model has",
                        other.as_str(),
                        level.as_str()
                    ));
                }
            }
            if let Some(level) = raw
                .effort_mapping
                .keys()
                .find(|level| !levels.contains(level))
            {
                return Err(format!(
                    "model `{id}`: `effort_mapping` renames `{}`, which `effort_levels` does not list",
                    level.as_str()
                ));
            }
            let mut quality = HashMap::new();
            for (class_name, score) in &raw.quality {
                let class = TaskClass::parse(class_name)
                    .ok_or_else(|| format!("model `{id}`: unknown task class `{class_name}`"))?;
                quality.insert(class, score.clamp(QUALITY_MIN, QUALITY_MAX));
            }
            parsed.push(ModelRow {
                provider,
                family,
                version,
                id,
                api_model,
                scored,
                outranks,
                evidence,
                catalog,
                scores: ResolvedScores {
                    coding_tier: raw.coding_tier.unwrap_or(CodingTier::Medium),
                    coding_percentile: raw.coding_percentile,
                    context_window: raw.context_window,
                    max_output_tokens: raw.max_output_tokens,
                    adaptive_thinking: raw.adaptive_thinking.unwrap_or(true),
                    effort: raw.effort.unwrap_or(true),
                    system_turns: raw.system_turns.unwrap_or(true),
                    effort_levels: raw.effort_levels.unwrap_or_default(),
                    effort_mapping: raw.effort_mapping,
                    tags: raw.tags,
                    quality,
                    default_quality: raw
                        .default_quality
                        .unwrap_or(QUALITY_MIN)
                        .clamp(QUALITY_MIN, QUALITY_MAX),
                },
            });
        }
        Ok(Self {
            rows: parsed,
            pinned: HashMap::new(),
            aliases: HashMap::new(),
        })
    }

    /// Every row, in file order.
    pub fn rows(&self) -> &[ModelRow] {
        &self.rows
    }

    /// Score each `pinned_versions` candidate as its pinned version, and each candidate whose
    /// selector is an alias (`opus[1m]`, with an `api_model`) as the API model it sends: a row is
    /// one exact model, so an alias never finds one by its own text.
    pub fn with_pinned_versions(mut self, cfg: &crate::config::Config) -> Self {
        let declared = cfg.declared_candidates();
        self.pinned = declared
            .iter()
            .filter_map(|id| {
                let version = cfg.pinned_version(id)?;
                Some((id.clone(), CandidateId::new(&id.agent, &version.api_model)))
            })
            .collect();
        self.aliases = declared
            .iter()
            .filter_map(|id| {
                let api_model = cfg.model_config(id)?.api_model.clone()?;
                (api_model != id.model)
                    .then(|| (id.clone(), CandidateId::new(&id.agent, &api_model)))
            })
            .collect();
        self
    }

    pub fn from_file(path: &Path) -> Result<Self, String> {
        let yaml = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read score table {}: {e}", path.display()))?;
        Self::from_yaml(&yaml)
    }

    /// Look up scores for a candidate, as the version it is pinned to when
    /// it is, falling back to neutral defaults for a model no row lists.
    pub fn lookup(&self, id: &CandidateId) -> ResolvedScores {
        self.lookup_exact(self.pinned.get(id).unwrap_or(id))
    }

    /// `lookup` without the `pinned_versions` mapping: `id` is matched as is.
    pub fn lookup_exact(&self, id: &CandidateId) -> ResolvedScores {
        self.row(self.aliases.get(id).unwrap_or(id))
            .map(|row| row.scores.clone())
            .unwrap_or_default()
    }

    /// The row for `id`'s model: the one whose `id` or `api_model` is that
    /// model exactly (case-insensitive). The agent does not take part, so an
    /// account seat scores as the model it runs.
    pub fn row(&self, id: &CandidateId) -> Option<&ModelRow> {
        self.rows.iter().find(|row| row.answers_to(&id.model))
    }
}

impl ModelRow {
    /// The row a reference from this one names: `provider/id`, or a bare id
    /// of this row's provider.
    pub fn resolve<'t>(&self, table: &'t ScoreTable, reference: &str) -> Option<&'t ModelRow> {
        let (provider, model) = match reference.split_once('/') {
            Some((provider, model)) => (Some(provider), model),
            None => (self.provider.as_deref(), reference),
        };
        table
            .rows
            .iter()
            .find(|row| row.answers_to(model) && row.provider.as_deref() == provider)
    }

    /// Whether `model` names this row's model exactly.
    pub fn answers_to(&self, model: &str) -> bool {
        self.id.eq_ignore_ascii_case(model)
            || self
                .api_model
                .as_deref()
                .is_some_and(|api| api.eq_ignore_ascii_case(model))
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

    fn one_row(fields: &str) -> ResolvedScores {
        ScoreTable::from_yaml(&format!("models:\n  - id: model\n{fields}"))
            .unwrap()
            .lookup(&CandidateId::new("test", "model"))
    }

    /// 2026-11-12 (operator): "Tests should assert that scores.yaml carry an exact match (no
    /// wildcard) for every single supported model." Every model an agent of a shipped provider
    /// inherits answers to a row that names it exactly, by `id` or `api_model`.
    #[test]
    fn every_supported_model_has_an_exact_row() {
        let cfg = crate::config::Config::from_yaml(
            "agents:\n- name: claude\n- name: codex\n- name: grok\n- name: kimi\n",
        )
        .unwrap();
        let table = ScoreTable::builtin();
        let mut missing = Vec::new();
        for agent in &cfg.agents {
            for model in &agent.models {
                for name in std::iter::once(&model.id).chain(model.api_model.as_ref()) {
                    let id = CandidateId::new(&agent.name, name);
                    if !table.row(&id).is_some_and(|row| row.answers_to(name)) {
                        missing.push(id.to_string());
                    }
                }
            }
        }
        assert!(
            missing.is_empty(),
            "no exact row for:\n{}",
            missing.join("\n")
        );
        assert_eq!(
            cfg.agents.iter().map(|a| a.models.len()).sum::<usize>(),
            table.rows().len(),
            "every row is a model some provider supports"
        );
    }

    #[test]
    fn a_row_names_one_model_never_a_pattern() {
        for yaml in [
            "models:\n  - id: '*sol*'\n",
            "models:\n  - id: sol\n    api_model: 'gpt-*-sol'\n",
        ] {
            let error = ScoreTable::from_yaml(yaml).unwrap_err();
            assert!(error.contains("never a pattern"), "{error}");
        }
        let duplicate =
            "models:\n  - id: opus\n    api_model: claude-opus-5-5\n  - id: claude-opus-5-5\n";
        assert!(
            ScoreTable::from_yaml(duplicate)
                .unwrap_err()
                .contains("listed twice")
        );
    }

    /// A model no row names exactly scores as the neutral default; nothing
    /// borrows a neighbour's row because their ids share a substring.
    #[test]
    fn only_an_exact_id_finds_a_row() {
        let table = ScoreTable::builtin();
        for id in [
            "codex/gpt-5.6-sol-mini",
            "codex/sol",
            "claude/opus-next",
            "grok/grok-4.5",
        ] {
            assert!(
                table.row(&CandidateId::parse(id).unwrap()).is_none(),
                "{id}"
            );
            assert_eq!(
                table
                    .lookup(&CandidateId::parse(id).unwrap())
                    .quality(TaskClass::BugFix),
                QUALITY_MIN
            );
        }
        // A seat and the provider API id both find the model they run.
        let seat = table
            .row(&CandidateId::new("claude@work", "claude-opus-5-5"))
            .unwrap();
        assert_eq!(seat.id, "claude-opus-5-5");
    }

    /// Every row is scored on its own evidence and nothing else: a scored row
    /// carries evidence of its own, and a row without evidence carries no
    /// scores (it is never handed another model's).
    #[test]
    fn a_row_is_scored_exactly_when_it_has_evidence_of_its_own() {
        for row in ScoreTable::builtin().rows() {
            assert_eq!(row.scored, !row.evidence.is_empty(), "{}", row.id);
        }
    }

    /// Every row states its family and version.
    #[test]
    fn every_row_states_its_family_and_version() {
        for row in ScoreTable::builtin().rows() {
            assert!(row.family.is_some(), "{} has no family", row.id);
            assert!(
                row.version
                    .as_deref()
                    .and_then(crate::config::parse_model_version)
                    .is_some(),
                "{} has no dotted version",
                row.id
            );
        }
    }

    /// The ordering contract: every row beats each row it `outranks` on every
    /// task class.
    #[test]
    fn every_declared_order_holds_on_every_task_class() {
        let table = ScoreTable::builtin();
        for row in table.rows() {
            for reference in &row.outranks {
                let below = row
                    .resolve(&table, reference)
                    .unwrap_or_else(|| panic!("{} outranks unknown `{reference}`", row.id));
                assert!(
                    row.scored && below.scored,
                    "{} and {} must both be scored",
                    row.id,
                    below.id
                );
                for class in TaskClass::ALL {
                    assert!(
                        row.scores.quality(class) > below.scores.quality(class),
                        "{} must outrank {} on {class:?}",
                        row.id,
                        below.id
                    );
                }
            }
        }
    }

    /// 2026-11-12 (operator): advertise only the levels a model has; a mapping only renames.
    /// `xhigh: max` beside `max: max` told users xhigh existed while sending max.
    #[test]
    fn an_effort_mapping_only_renames_a_level_the_model_has() {
        for yaml in [
            "models:\n  - { id: m, effort_levels: [xhigh, max], effort_mapping: { xhigh: max } }\n",
            "models:\n  - { id: m, effort_levels: [high], effort_mapping: { max: deep } }\n",
        ] {
            assert!(ScoreTable::from_yaml(yaml).is_err(), "{yaml}");
        }
        for row in ScoreTable::builtin().rows() {
            for (level, value) in &row.scores.effort_mapping {
                assert_ne!(
                    level.as_str(),
                    value.as_str(),
                    "{} maps `{value}` to itself",
                    row.id
                );
            }
        }
    }

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
        let scores = one_row(
            "    effort: false\n    effort_levels: [low, high]\n    effort_mapping: { low: minimal, high: intensive }\n",
        );
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

    #[test]
    fn automatic_effort_resolution_never_rounds_up_but_explicit_stays_compatible() {
        let scores = one_row(
            "    effort_levels: [medium, high, max]\n    effort_mapping: { medium: balanced, high: intensive, max: maximum }\n",
        );

        let automatic_low = scores.resolve_automatic_effort(EffortLevel::Low);
        assert_eq!(automatic_low.resolved, None);
        assert_eq!(automatic_low.provider_value, None);

        let automatic_high = scores.resolve_automatic_effort(EffortLevel::High);
        assert_eq!(automatic_high.resolved, Some(EffortLevel::High));
        assert_eq!(automatic_high.provider_value.as_deref(), Some("intensive"));

        let explicit_low = scores.resolve_effort(EffortLevel::Low);
        assert_eq!(explicit_low.resolved, Some(EffortLevel::Medium));
        assert_eq!(explicit_low.provider_value.as_deref(), Some("balanced"));

        // An unmapped level is sent under its own name.
        let partly_renamed = one_row(
            "    effort_levels: [medium, high]\n    effort_mapping: { medium: balanced }\n",
        );
        let automatic_high = partly_renamed.resolve_automatic_effort(EffortLevel::High);
        assert_eq!(automatic_high.resolved, Some(EffortLevel::High));
        assert_eq!(automatic_high.provider_value.as_deref(), Some("high"));
        let automatic_medium = partly_renamed.resolve_automatic_effort(EffortLevel::Medium);
        assert_eq!(automatic_medium.provider_value.as_deref(), Some("balanced"));
    }
}

#[cfg(test)]
mod score_resolution_tests {
    use super::*;

    fn q(t: &ScoreTable, id: &str) -> ResolvedScores {
        t.lookup(&CandidateId::parse(id).unwrap())
    }

    /// Each version keeps its own wire shape: GPT-6 Sol/Luna take the 1.05M
    /// window and native xhigh, while GPT-5.6 Sol maps xhigh to `high`.
    #[test]
    fn each_version_keeps_its_own_wire_shape() {
        let t = ScoreTable::builtin();
        for id in ["codex/gpt-6-sol", "codex/gpt-6.1-sol", "codex/gpt-6-luna"] {
            let resolved = q(&t, id);
            assert_eq!(resolved.context_window, Some(1_050_000), "{id} window");
            assert_eq!(resolved.max_output_tokens, Some(128_000), "{id} max out");
            let xhigh = resolved.resolve_effort(EffortLevel::Xhigh);
            assert_eq!(
                xhigh.resolved,
                Some(EffortLevel::Xhigh),
                "{id} native xhigh"
            );
            assert_eq!(xhigh.provider_value.as_deref(), Some("xhigh"));
        }
        // GPT-5.6 Sol has no xhigh: an explicit xhigh request resolves to its nearest real level.
        let sol56 = q(&t, "codex/gpt-5.6-sol").resolve_effort(EffortLevel::Xhigh);
        assert_eq!(sol56.resolved, Some(EffortLevel::High));
        assert_eq!(sol56.provider_value.as_deref(), Some("high"));
        let haiku45 = q(&t, "claude/claude-haiku-4-5");
        assert!(!haiku45.adaptive_thinking && !haiku45.effort && !haiku45.system_turns);
        assert_eq!(haiku45.max_output_tokens, Some(64_000));
        assert!(q(&t, "claude/haiku").adaptive_thinking);
    }

    #[test]
    fn grok_47_exposes_native_reasoning_levels() {
        let scores = ScoreTable::builtin().lookup(&CandidateId::new("grok@seat", "grok-4.7"));
        assert_eq!(scores.context_window, Some(500_000));
        for level in [
            EffortLevel::Low,
            EffortLevel::Medium,
            EffortLevel::High,
            EffortLevel::Xhigh,
        ] {
            let resolution = scores.resolve_automatic_effort(level);
            assert_eq!(resolution.resolved, Some(level));
            assert_eq!(resolution.provider_value.as_deref(), Some(level.as_str()));
            assert!(!resolution.confirmed, "mapping alone is not wire evidence");
        }
        assert_eq!(
            scores.resolve_automatic_effort(EffortLevel::Max).resolved,
            Some(EffortLevel::Xhigh)
        );
    }
}
