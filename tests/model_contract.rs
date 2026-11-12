//! The contract of the model catalog. `data/providers/<name>.yaml` is the one
//! definition of each provider and every model it supports (operator,
//! 2026-11-12): each model is one exact API id, versioned within its family,
//! with its pricing, cost rank and its own published benchmark evidence, from
//! which its quality is computed. `data/classifier.yaml` defines the
//! benchmarks and the cost ladder. No shipped example restates a model.

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
struct Pricing {
    input_per_mtok: Option<f64>,
    output_per_mtok: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
struct Evidence {
    benchmark: String,
    #[allow(dead_code)]
    result: f64,
    source: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct Model {
    version: Option<serde_yaml::Value>,
    id: String,
    api_model: Option<String>,
    display_name: Option<String>,
    cost_rank: Option<u8>,
    pricing: Option<Pricing>,
    #[serde(default)]
    evidence: Vec<Evidence>,
    default_quality: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct Ladder {
    min: u8,
    max: u8,
}

#[derive(Debug, Deserialize)]
struct Cost {
    ladder: Ladder,
}

#[derive(Debug, Deserialize)]
struct Scoring {
    benchmarks: serde_yaml::Mapping,
}

#[derive(Debug, Deserialize)]
struct Classifier {
    scoring: Scoring,
    cost: Cost,
}

#[derive(Debug, Deserialize)]
struct Provider {
    provider: String,
    models: std::collections::BTreeMap<String, Vec<Model>>,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_yaml<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    serde_yaml::from_str(&fs::read_to_string(path).unwrap())
        .unwrap_or_else(|error| panic!("{} must parse: {error}", path.display()))
}

fn classifier() -> Classifier {
    read_yaml(&root().join("data/classifier.yaml"))
}

fn providers() -> Vec<(PathBuf, Provider)> {
    let mut files: Vec<PathBuf> = fs::read_dir(root().join("data/providers"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "yaml"))
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let provider = read_yaml(&path);
            (path, provider)
        })
        .collect()
}

/// Every contract violation of one provider's models, each with its family.
fn violations(provider: &str, models: &[(String, Model)], classifier: &Classifier) -> Vec<String> {
    let mut errors = Vec::new();
    let ladder = &classifier.cost.ladder;
    for (index, (family, model)) in models.iter().enumerate() {
        let name = format!("{provider}/{}", model.id);
        if model.id.contains('*') || family.contains('*') {
            errors.push(format!("{name}: a pattern, not a model"));
        }
        if model.api_model.is_some() {
            errors.push(format!(
                "{name}: `id` is the API id; there is no `api_model`"
            ));
        }
        let version = match &model.version {
            Some(serde_yaml::Value::Number(n)) => Some(n.to_string()),
            Some(serde_yaml::Value::String(s)) => Some(s.clone()),
            _ => None,
        };
        if !version.is_some_and(|v| v.split('.').all(|p| p.parse::<u32>().is_ok())) {
            errors.push(format!("{name} has no dotted version"));
        }
        if model.display_name.is_none() {
            errors.push(format!("{name} has no display name"));
        }
        if !model
            .cost_rank
            .is_some_and(|rank| rank >= ladder.min && rank <= ladder.max)
        {
            errors.push(format!(
                "{name} has no cost rank in {}..={}",
                ladder.min, ladder.max
            ));
        }
        if !model
            .pricing
            .as_ref()
            .is_some_and(|p| p.input_per_mtok.is_some() && p.output_per_mtok.is_some())
        {
            errors.push(format!("{name} has incomplete pricing"));
        }
        // Every model is scored, and only on its own evidence.
        if model.evidence.is_empty() {
            errors.push(format!("{name} has no evidence of its own"));
        }
        if model.default_quality.is_none() {
            errors.push(format!("{name} has no computed quality"));
        }
        for result in &model.evidence {
            if !classifier
                .scoring
                .benchmarks
                .contains_key(result.benchmark.as_str())
            {
                errors.push(format!("{name}: unknown benchmark `{}`", result.benchmark));
            }
            if result.source.trim().is_empty() {
                errors.push(format!(
                    "{name}: a `{}` result has no source",
                    result.benchmark
                ));
            }
        }
        if models[..index]
            .iter()
            .any(|(_, other)| other.id == model.id)
        {
            errors.push(format!("{name} is listed twice"));
        }
    }
    errors
}

fn flatten(provider: &Provider) -> Vec<(String, Model)> {
    provider
        .models
        .iter()
        .flat_map(|(family, versions)| versions.iter().map(|m| (family.clone(), m.clone())))
        .collect()
}

#[test]
fn every_model_is_one_exact_version_with_its_whole_contract() {
    let classifier = classifier();
    let mut errors = Vec::new();
    for (path, provider) in providers() {
        if path.file_stem().and_then(|s| s.to_str()) != Some(provider.provider.as_str()) {
            errors.push(format!("{} must be named for its provider", path.display()));
        }
        errors.extend(violations(
            &provider.provider,
            &flatten(&provider),
            &classifier,
        ));
    }
    assert!(
        errors.is_empty(),
        "model contract violations:\n{}",
        errors.join("\n")
    );
}

/// The catalog is the only place a model is defined, and the retired files stay gone.
#[test]
fn no_shipped_example_restates_a_model() {
    let repo = root();
    for retired in [
        "data/scores.yaml",
        "data/models.yaml",
        "data/model-policy.yaml",
        "data/providers.yaml",
        "data/agents",
        "src/presets",
    ] {
        assert!(!repo.join(retired).exists(), "{retired} is retired");
    }
    let mut offenders = Vec::new();
    for entry in fs::read_dir(repo.join("examples")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|e| e != "yaml") {
            continue;
        }
        let document: serde_yaml::Value = read_yaml(&path);
        let agents = document
            .get("agents")
            .and_then(|a| a.as_sequence())
            .cloned()
            .unwrap_or_default();
        let providers: Vec<serde_yaml::Value> = document
            .get("providers")
            .and_then(|p| p.as_mapping())
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        if agents
            .iter()
            .chain(&providers)
            .any(|entry| entry.get("models").is_some())
        {
            offenders.push(path.strip_prefix(&repo).unwrap().display().to_string());
        }
    }
    assert!(offenders.is_empty(), "models restated in: {offenders:?}");
}

#[test]
fn an_incomplete_model_fires_every_companion_check() {
    let model = Model {
        id: "gpt-7-*".to_string(),
        api_model: Some("gpt-7".to_string()),
        evidence: vec![Evidence {
            benchmark: "Vibes-Bench".to_string(),
            result: 1.0,
            source: " ".to_string(),
        }],
        ..Model::default()
    };
    let empty = Model {
        id: "gpt-7-nova".to_string(),
        ..Model::default()
    };
    let errors = violations(
        "codex",
        &[("gpt".to_string(), model), ("gpt".to_string(), empty)],
        &classifier(),
    )
    .join("\n");
    for required in [
        "a pattern",
        "there is no `api_model`",
        "has no dotted version",
        "has no display name",
        "has no cost rank",
        "has incomplete pricing",
        "unknown benchmark",
        "has no source",
        "has no evidence of its own",
        "has no computed quality",
    ] {
        assert!(
            errors.contains(required),
            "missing `{required}` in:\n{errors}"
        );
    }
}
