use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use router_acp::candidate::glob_match;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
struct Pricing {
    input_per_mtok: Option<f64>,
    output_per_mtok: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
struct Version {
    api_model: String,
    pricing: Option<Pricing>,
}

#[derive(Clone, Debug, Deserialize)]
struct Model {
    id: String,
    api_model: Option<String>,
    cost_rank: Option<u8>,
    pricing: Option<Pricing>,
    #[serde(default)]
    versions: Vec<Version>,
}

#[derive(Clone, Debug, Deserialize)]
struct Agent {
    name: String,
    provider: Option<String>,
    #[serde(default)]
    models: Vec<Model>,
}

#[derive(Debug, Deserialize)]
struct RouterConfig {
    #[serde(default)]
    agents: Vec<Agent>,
}

#[derive(Debug, Deserialize)]
struct Preset {
    #[serde(default)]
    models: Vec<Model>,
}

#[derive(Debug, Deserialize)]
struct ScoreRow {
    pattern: String,
}

#[derive(Debug, Deserialize)]
struct Scores {
    candidates: Vec<ScoreRow>,
}

#[derive(Debug, Deserialize)]
struct Policy {
    benchmark_scoring: BenchmarkScoring,
    cost: CostPolicy,
    invariants: Invariants,
    published_pricing: PublishedPricing,
}

#[derive(Debug, Deserialize)]
struct BenchmarkScoring {
    model_evidence: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct CostPolicy {
    ladder: Ladder,
}

#[derive(Debug, Deserialize)]
struct Ladder {
    min: u8,
    max: u8,
}

#[derive(Debug, Deserialize)]
struct Invariants {
    resolves_to: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct PublishedPricing {
    models: BTreeMap<String, Pricing>,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_yaml<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    serde_yaml::from_str(&fs::read_to_string(path).unwrap())
        .unwrap_or_else(|error| panic!("{} must parse: {error}", path.display()))
}

fn shipped_catalogs() -> Vec<(String, Vec<Agent>)> {
    let repo = root();
    let mut paths = fs::read_dir(repo.join("examples"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("router") && name.ends_with(".yaml"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    let mut catalogs = paths
        .into_iter()
        .map(|path| {
            let config: RouterConfig = read_yaml(&path);
            (
                path.strip_prefix(&repo).unwrap().display().to_string(),
                config.agents,
            )
        })
        .collect::<Vec<_>>();

    let preset_dir = repo.join("src/presets");
    let mut presets = fs::read_dir(&preset_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .collect::<Vec<_>>();
    presets.sort();
    catalogs.extend(presets.into_iter().map(|path| {
        let preset: Preset = read_yaml(&path);
        let provider = path.file_stem().unwrap().to_str().unwrap().to_string();
        (
            path.strip_prefix(&repo).unwrap().display().to_string(),
            vec![Agent {
                name: provider,
                provider: None,
                models: preset.models,
            }],
        )
    }));
    catalogs
}

fn first_match<'a>(patterns: &'a [String], candidate: &str) -> Option<&'a str> {
    patterns
        .iter()
        .find(|pattern| glob_match(pattern, candidate))
        .map(String::as_str)
}

fn validate_identity(
    source: &str,
    candidate: &str,
    pricing: Option<&Pricing>,
    score_patterns: &[String],
    evidence_patterns: &[String],
    policy: &Policy,
    errors: &mut Vec<String>,
) {
    let score = first_match(score_patterns, candidate);
    if score.is_none() {
        errors.push(format!("{source}: {candidate} has no score entry"));
    }
    if first_match(evidence_patterns, candidate).is_none() {
        errors.push(format!(
            "{source}: {candidate} has no calibrated benchmark evidence"
        ));
    }
    match policy.invariants.resolves_to.get(candidate) {
        None => errors.push(format!(
            "{source}: {candidate} has no deliberate score resolution"
        )),
        Some(expected) if Some(expected.as_str()) != score => errors.push(format!(
            "{source}: {candidate} resolves to {score:?}, expected {expected}"
        )),
        Some(_) => {}
    }
    if !policy.published_pricing.models.contains_key(candidate) {
        errors.push(format!(
            "{source}: {candidate} has no published-pricing entry"
        ));
    }
    match pricing {
        Some(rates) if rates.input_per_mtok.is_some() && rates.output_per_mtok.is_some() => {}
        _ => errors.push(format!(
            "{source}: {candidate} has incomplete catalog pricing"
        )),
    }
}

fn validate_catalogs(catalogs: &[(String, Vec<Agent>)]) -> Vec<String> {
    let repo = root();
    let scores: Scores = read_yaml(&repo.join("data/scores.yaml"));
    let policy: Policy = read_yaml(&repo.join("data/model-policy.yaml"));
    let score_patterns = scores
        .candidates
        .iter()
        .map(|row| row.pattern.clone())
        .collect::<Vec<_>>();
    let evidence_patterns = policy
        .benchmark_scoring
        .model_evidence
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let mut errors = Vec::new();

    for (source, agents) in catalogs {
        for agent in agents {
            let provider = agent
                .provider
                .as_deref()
                .unwrap_or_else(|| agent.name.split('@').next().unwrap());
            for model in &agent.models {
                let candidate = format!("{provider}/{}", model.id);
                match model.cost_rank {
                    Some(rank)
                        if rank >= policy.cost.ladder.min && rank <= policy.cost.ladder.max => {}
                    _ => errors.push(format!(
                        "{source}: {candidate} has no cost rank in {}..={} ",
                        policy.cost.ladder.min, policy.cost.ladder.max
                    )),
                }
                validate_identity(
                    source,
                    &candidate,
                    model.pricing.as_ref(),
                    &score_patterns,
                    &evidence_patterns,
                    &policy,
                    &mut errors,
                );
                if let Some(api_model) = model.api_model.as_deref().filter(|id| *id != model.id) {
                    validate_identity(
                        source,
                        &format!("{provider}/{api_model}"),
                        model.pricing.as_ref(),
                        &score_patterns,
                        &evidence_patterns,
                        &policy,
                        &mut errors,
                    );
                }
                for version in &model.versions {
                    validate_identity(
                        source,
                        &format!("{provider}/{}", version.api_model),
                        version.pricing.as_ref(),
                        &score_patterns,
                        &evidence_patterns,
                        &policy,
                        &mut errors,
                    );
                }
            }
        }
    }
    errors
}

#[test]
fn every_shipped_model_has_the_complete_cross_file_contract() {
    let catalogs = shipped_catalogs();
    let names = catalogs
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    for expected in [
        "examples/router-accounts.yaml",
        "examples/router-full.yaml",
        "examples/router-preferred.yaml",
        "src/presets/claude.yaml",
        "src/presets/codex.yaml",
        "src/presets/grok.yaml",
        "src/presets/kimi.yaml",
    ] {
        assert!(
            names.contains(&expected),
            "missing shipped catalog {expected}"
        );
    }
    let errors = validate_catalogs(&catalogs);
    assert!(
        errors.is_empty(),
        "model contract violations:\n{}",
        errors.join("\n")
    );
}

#[test]
fn an_enabled_model_cannot_omit_any_companion_configuration() {
    // A model no shipped file knows: every companion check must fire. (GPT-6
    // Astra used to be this fixture, until it got its full contract.)
    let incomplete = vec![(
        "fixture.yaml".to_string(),
        vec![Agent {
            name: "codex".to_string(),
            provider: None,
            models: vec![Model {
                id: "gpt-7-nova".to_string(),
                api_model: None,
                cost_rank: None,
                pricing: None,
                versions: Vec::new(),
            }],
        }],
    )];
    let errors = validate_catalogs(&incomplete).join("\n");
    for required in [
        "has no cost rank",
        "has no score entry",
        "has no calibrated benchmark evidence",
        "has no deliberate score resolution",
        "has no published-pricing entry",
        "has incomplete catalog pricing",
    ] {
        assert!(
            errors.contains(required),
            "missing `{required}` in:\n{errors}"
        );
    }
}
