//! Heuristic classifier and optional local-model classifier.
//!
//! Both backends share one interface: [`classify`] returns a
//! [`TaskProfile`] `{ class, complexity, languages }`. The local-model
//! backend calls a local runtime (e.g. Ollama) with temperature 0 and a
//! timeout, and falls back to the heuristic on timeout, parse failure, or
//! unavailable runtime. The classifier never uses the paid/seat ACP
//! downstream agents.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use serde::{Deserialize, Serialize};

use agent_client_protocol::schema::v1::ContentBlock;

use crate::candidate::TaskClass;
use crate::config::{ClassifierBackend, ClassifierConfig};

/// The classifier's output for one prompt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskProfile {
    pub class: TaskClass,
    /// 0.0 (trivial) to 1.0 (very complex).
    pub complexity: f64,
    pub languages: Vec<String>,
}

impl Default for TaskProfile {
    fn default() -> Self {
        Self {
            class: TaskClass::CodingGeneral,
            complexity: 0.5,
            languages: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassRule {
    pub class: String,
    pub weight: f64,
    pub keywords: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeywordBump {
    pub bump: f64,
    pub keywords: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComplexityRules {
    pub length_full_score_chars: usize,
    pub length_max: f64,
    pub per_file_bump: f64,
    pub max_file_bump: f64,
    pub keyword_bumps: Vec<KeywordBump>,
}

/// Multi-step prompts ("do X and Y, then Z") read as more complex per
/// conjunction/sequencing token, capped.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiStepRules {
    #[serde(default)]
    pub per_token_bump: f64,
    #[serde(default)]
    pub max_bump: f64,
    #[serde(default)]
    pub tokens: Vec<String>,
}

/// The rule tables loaded from `data/classifier.yaml` (or an override file).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassifierRules {
    #[serde(default)]
    pub version: Option<u32>,
    pub classes: Vec<ClassRule>,
    pub complexity: ComplexityRules,
    #[serde(default)]
    pub multi_step: MultiStepRules,
    pub languages: HashMap<String, String>,
}

pub const BUILTIN_CLASSIFIER_RULES: &str = include_str!("../data/classifier.yaml");

impl ClassifierRules {
    pub fn builtin() -> Self {
        Self::from_yaml(BUILTIN_CLASSIFIER_RULES).expect("built-in classifier rules must parse")
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        let rules: ClassifierRules =
            serde_yaml::from_str(yaml).map_err(|e| format!("invalid classifier rules: {e}"))?;
        for rule in &rules.classes {
            if TaskClass::parse(&rule.class).is_none() {
                return Err(format!("classifier rules: unknown class `{}`", rule.class));
            }
        }
        Ok(rules)
    }

    pub fn from_file(path: &Path) -> Result<Self, String> {
        let yaml = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read classifier rules {}: {e}", path.display()))?;
        Self::from_yaml(&yaml)
    }
}

/// Inputs to a classification decision.
#[derive(Debug, Clone, Default)]
pub struct ClassifyInput {
    /// Concatenated text of the first prompt.
    pub text: String,
    /// File paths/names mentioned in the prompt or attached as resources.
    pub mentioned_paths: Vec<String>,
    /// Number of attached resources (embedded or linked).
    pub resource_count: usize,
    /// Total size in bytes of attached embedded resources.
    pub resource_bytes: usize,
    /// Languages detected by scanning the session cwd.
    pub cwd_languages: Vec<String>,
}

impl ClassifyInput {
    /// Build the classification input from prompt content blocks plus a cwd
    /// language fingerprint.
    pub fn from_prompt(prompt: &[ContentBlock], cwd_languages: Vec<String>) -> Self {
        let mut text = String::new();
        let mut mentioned_paths = Vec::new();
        let mut resource_count = 0usize;
        let mut resource_bytes = 0usize;
        for block in prompt {
            match block {
                ContentBlock::Text(t) => {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(&t.text);
                }
                ContentBlock::ResourceLink(link) => {
                    resource_count += 1;
                    mentioned_paths.push(link.uri.clone());
                }
                ContentBlock::Resource(res) => {
                    resource_count += 1;
                    if let Ok(v) = serde_json::to_value(res) {
                        if let Some(uri) = v
                            .get("resource")
                            .and_then(|r| r.get("uri"))
                            .and_then(|u| u.as_str())
                        {
                            mentioned_paths.push(uri.to_string());
                        }
                        if let Some(body) = v
                            .get("resource")
                            .and_then(|r| r.get("text"))
                            .and_then(|t| t.as_str())
                        {
                            resource_bytes += body.len();
                        }
                    }
                }
                _ => {}
            }
        }
        mentioned_paths.extend(extract_path_mentions(&text));
        Self {
            text,
            mentioned_paths,
            resource_count,
            resource_bytes,
            cwd_languages,
        }
    }
}

/// Pull filename-looking tokens (`foo/bar.rs`, `baz.py`) out of prompt text.
fn extract_path_mentions(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in text.split(|c: char| c.is_whitespace() || "()[]{}<>\"'`,;".contains(c)) {
        let token = token.trim_end_matches(&['.', ':', '?', '!'][..]);
        if token.len() < 3 || !token.contains('.') {
            continue;
        }
        let Some((stem, ext)) = token.rsplit_once('.') else {
            continue;
        };
        if stem.is_empty()
            || ext.is_empty()
            || ext.len() > 8
            || !ext.chars().all(|c| c.is_ascii_alphanumeric())
            || !stem.chars().any(|c| c.is_ascii_alphanumeric())
        {
            continue;
        }
        // Reject version numbers like "4.5" and sentence fragments.
        if stem.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        out.push(token.to_string());
    }
    out
}

/// Deterministic heuristic classification from the rule tables.
pub fn classify_heuristic(rules: &ClassifierRules, input: &ClassifyInput) -> TaskProfile {
    let lower = input.text.to_lowercase();

    // Class: highest keyword-weight total wins; ties break by declaration
    // order in TaskClass::ALL.
    let mut totals: HashMap<TaskClass, f64> = HashMap::new();
    for rule in &rules.classes {
        let Some(class) = TaskClass::parse(&rule.class) else {
            continue;
        };
        let hits = rule
            .keywords
            .iter()
            .filter(|k| lower.contains(&k.to_lowercase()))
            .count();
        if hits > 0 {
            *totals.entry(class).or_insert(0.0) += rule.weight * hits as f64;
        }
    }
    let class = TaskClass::ALL
        .iter()
        .copied()
        .max_by(|a, b| {
            let sa = totals.get(a).copied().unwrap_or(0.0);
            let sb = totals.get(b).copied().unwrap_or(0.0);
            sa.partial_cmp(&sb)
                .unwrap_or(std::cmp::Ordering::Equal)
                // On ties prefer the earlier class in ALL: max_by keeps the
                // later element on Equal, so reverse the index comparison.
                .then_with(|| {
                    let ia = TaskClass::ALL.iter().position(|c| c == a).unwrap();
                    let ib = TaskClass::ALL.iter().position(|c| c == b).unwrap();
                    ib.cmp(&ia)
                })
        })
        .filter(|c| totals.get(c).copied().unwrap_or(0.0) > 0.0)
        .unwrap_or(TaskClass::CodingGeneral);

    // Complexity: length ramp + keyword bumps + per-file bumps, clamped.
    let c = &rules.complexity;
    let mut complexity = if c.length_full_score_chars > 0 {
        (input.text.len() as f64 / c.length_full_score_chars as f64).min(1.0) * c.length_max
    } else {
        0.0
    };
    for bump in &c.keyword_bumps {
        let hits = bump
            .keywords
            .iter()
            .filter(|k| lower.contains(&k.to_lowercase()))
            .count();
        // Accumulate per matching keyword, capped at twice the group bump so
        // repetitive prompts cannot dominate the estimate.
        complexity += bump.bump * (hits.min(2) as f64);
    }
    let file_count = input.mentioned_paths.len() + input.resource_count;
    complexity += (file_count as f64 * c.per_file_bump).min(c.max_file_bump);
    // Multi-step structure: each conjunction/sequencing token adds a bump.
    let steps: usize = rules
        .multi_step
        .tokens
        .iter()
        .map(|t| lower.matches(t.as_str()).count())
        .sum();
    complexity += (steps as f64 * rules.multi_step.per_token_bump).min(rules.multi_step.max_bump);
    let complexity = complexity.clamp(0.0, 1.0);

    // Languages: extension mentions plus the cwd fingerprint, deduplicated
    // and sorted for determinism.
    let mut languages: BTreeSet<String> = input.cwd_languages.iter().cloned().collect();
    for path in &input.mentioned_paths {
        if let Some(ext) = path.rsplit_once('.').map(|(_, e)| e.to_lowercase())
            && let Some(lang) = rules.languages.get(&ext)
        {
            languages.insert(lang.clone());
        }
    }

    TaskProfile {
        class,
        complexity,
        languages: languages.into_iter().collect(),
    }
}

/// Scan a working directory for file extensions and map them to languages.
/// Bounded: at most `max_entries` directory entries across two levels.
pub fn cwd_language_fingerprint(rules: &ClassifierRules, cwd: &Path) -> Vec<String> {
    const MAX_ENTRIES: usize = 512;
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut seen = 0usize;
    let mut dirs = vec![(cwd.to_path_buf(), 0u8)];
    while let Some((dir, depth)) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            seen += 1;
            if seen > MAX_ENTRIES {
                break;
            }
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') || name == "node_modules" || name == "target" {
                continue;
            }
            if path.is_dir() {
                if depth < 1 {
                    dirs.push((path, depth + 1));
                }
            } else if let Some(ext) = path.extension().and_then(|e| e.to_str())
                && let Some(lang) = rules.languages.get(&ext.to_lowercase())
            {
                *counts.entry(lang.clone()).or_insert(0) += 1;
            }
        }
        if seen > MAX_ENTRIES {
            break;
        }
    }
    let mut langs: Vec<(String, usize)> = counts.into_iter().collect();
    langs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    langs.into_iter().take(4).map(|(l, _)| l).collect()
}

/// Classify using the configured backend, falling back to the heuristic.
pub async fn classify(
    cfg: &ClassifierConfig,
    rules: &ClassifierRules,
    input: &ClassifyInput,
) -> TaskProfile {
    if cfg.backend == ClassifierBackend::LocalModel
        && let Some(model_spec) = &cfg.local_model
    {
        let timeout = std::time::Duration::from_millis(cfg.timeout_ms);
        match tokio::time::timeout(timeout, classify_local_model(model_spec, &input.text)).await {
            Ok(Ok(profile)) => return profile,
            Ok(Err(err)) => {
                tracing::warn!(%err, "local-model classifier failed; falling back to heuristic");
            }
            Err(_) => {
                tracing::warn!(
                    timeout_ms = cfg.timeout_ms,
                    "local-model classifier timed out; falling back to heuristic"
                );
            }
        }
    }
    classify_heuristic(rules, input)
}

#[derive(Debug, Deserialize)]
struct LocalModelReply {
    class: String,
    complexity: f64,
    #[serde(default)]
    languages: Vec<String>,
}

/// Call a local model runtime. `model_spec` looks like `ollama:qwen3:4b`
/// (optionally `ollama@host:port:model`). Uses the Ollama /api/generate API
/// with temperature 0 over a minimal HTTP/1.1 client so the classifier has
/// no heavyweight dependencies and can never touch seat-backed agents.
async fn classify_local_model(model_spec: &str, text: &str) -> Result<TaskProfile, String> {
    let (host, model) = match model_spec.split_once(':') {
        Some(("ollama", model)) => ("127.0.0.1:11434".to_string(), model.to_string()),
        Some((runtime, model)) if runtime.starts_with("ollama@") => (
            runtime.trim_start_matches("ollama@").to_string(),
            model.to_string(),
        ),
        _ => return Err(format!("unsupported local_model spec `{model_spec}`")),
    };

    let class_names: Vec<&str> = TaskClass::ALL.iter().map(|c| c.as_str()).collect();
    let prompt = format!(
        "Classify this coding-agent task. Reply with ONLY a JSON object \
         {{\"class\": one of {class_names:?}, \"complexity\": number 0..1, \
         \"languages\": [strings]}}.\n\nTask:\n{}",
        &text[..text.len().min(4000)]
    );
    let body = serde_json::json!({
        "model": model,
        "prompt": prompt,
        "stream": false,
        "format": "json",
        "options": { "temperature": 0 }
    })
    .to_string();

    let response_body = http_post_json(&host, "/api/generate", &body).await?;
    let outer: serde_json::Value =
        serde_json::from_str(&response_body).map_err(|e| format!("bad ollama response: {e}"))?;
    let inner = outer
        .get("response")
        .and_then(|r| r.as_str())
        .ok_or_else(|| "ollama response missing `response` field".to_string())?;
    let reply: LocalModelReply =
        serde_json::from_str(inner).map_err(|e| format!("model returned non-JSON profile: {e}"))?;
    let class = TaskClass::parse(&reply.class)
        .ok_or_else(|| format!("model returned unknown class `{}`", reply.class))?;
    Ok(TaskProfile {
        class,
        complexity: reply.complexity.clamp(0.0, 1.0),
        languages: reply.languages,
    })
}

/// Minimal HTTP/1.1 POST returning the response body. Localhost-grade only.
async fn http_post_json(host: &str, path: &str, body: &str) -> Result<String, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(host)
        .await
        .map_err(|e| format!("cannot connect to {host}: {e}"))?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write failed: {e}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|e| format!("read failed: {e}"))?;
    let text = String::from_utf8_lossy(&raw);
    let Some((head, rest)) = text.split_once("\r\n\r\n") else {
        return Err("malformed HTTP response".into());
    };
    let status = head.lines().next().unwrap_or_default();
    if !status.contains("200") {
        return Err(format!("HTTP error: {status}"));
    }
    // Handle chunked transfer encoding crudely by stripping chunk sizes.
    if head.to_lowercase().contains("transfer-encoding: chunked") {
        let mut out = String::new();
        let mut rest = rest;
        while let Some((size_line, tail)) = rest.split_once("\r\n") {
            let Ok(size) = usize::from_str_radix(size_line.trim(), 16) else {
                break;
            };
            if size == 0 {
                break;
            }
            if tail.len() < size {
                out.push_str(tail);
                break;
            }
            out.push_str(&tail[..size]);
            rest = tail[size..].trim_start_matches("\r\n");
        }
        Ok(out)
    } else {
        Ok(rest.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules() -> ClassifierRules {
        ClassifierRules::builtin()
    }

    fn classify_text(text: &str) -> TaskProfile {
        classify_heuristic(
            &rules(),
            &ClassifyInput {
                text: text.to_string(),
                ..Default::default()
            },
        )
    }

    #[test]
    fn classifies_ui_tweak() {
        let p = classify_text("Change the button padding and font color on the login page CSS");
        assert_eq!(p.class, TaskClass::UiTweak);
    }

    #[test]
    fn classifies_bug_fix() {
        let p = classify_text("Fix the crash when parsing an empty file; stack trace attached");
        assert_eq!(p.class, TaskClass::BugFix);
    }

    #[test]
    fn classifies_architecture_with_high_complexity() {
        let p = classify_text(
            "Design the architecture for migrating our monolith to a distributed \
             system across the codebase, considering tradeoffs end-to-end",
        );
        assert_eq!(p.class, TaskClass::Architecture);
        assert!(p.complexity >= 0.5, "complexity {}", p.complexity);
    }

    #[test]
    fn classifies_research() {
        let p = classify_text("Research the options and compare pros and cons of message queues");
        assert_eq!(p.class, TaskClass::Research);
    }

    #[test]
    fn classifies_ops() {
        let p = classify_text("Set up the CI pipeline with docker and kubernetes deploy");
        assert_eq!(p.class, TaskClass::Ops);
    }

    #[test]
    fn defaults_to_coding_general() {
        let p = classify_text("hmm please look at this thing");
        assert_eq!(p.class, TaskClass::CodingGeneral);
    }

    #[test]
    fn trivial_markers_lower_complexity() {
        let p = classify_text("Fix a typo in main.rs, one-line change");
        assert!(p.complexity < 0.3, "complexity {}", p.complexity);
    }

    #[test]
    fn detects_languages_from_mentions() {
        let p = classify_heuristic(
            &rules(),
            &ClassifyInput {
                text: "Fix the bug in src/main.rs and app.py".to_string(),
                mentioned_paths: extract_path_mentions("Fix the bug in src/main.rs and app.py"),
                ..Default::default()
            },
        );
        assert!(p.languages.contains(&"rust".to_string()));
        assert!(p.languages.contains(&"python".to_string()));
    }

    #[test]
    fn cross_system_investigation_scores_complex() {
        // The real-world miss: short prompt, no architecture keywords, but
        // it is an hour-long multi-system investigation.
        let p = classify_text(
            "Analyze the pull request and the linear ticket, then leave a comment on the \
             ticket with the status of the current work and the remaining work",
        );
        assert!(
            p.complexity >= 0.5,
            "cross-system multi-step work must not read as trivial: {}",
            p.complexity
        );
        assert_eq!(p.class, TaskClass::Research);
    }

    #[test]
    fn deterministic_for_identical_inputs() {
        let a = classify_text("Refactor the session module and clean up duplication");
        let b = classify_text("Refactor the session module and clean up duplication");
        assert_eq!(a, b);
    }

    #[test]
    fn path_mention_extraction_skips_versions() {
        let paths = extract_path_mentions("upgrade to 4.5 then edit foo/bar.ts please");
        assert_eq!(paths, vec!["foo/bar.ts".to_string()]);
    }
}
