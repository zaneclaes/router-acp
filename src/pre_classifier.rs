//! Composable LLM pre-classifier: one cheap ACP evaluation of the user prompt
//! returns structured decisions for built-in and host-registered dimensions.
//!
//! When enabled, this is the authority for auto-orchestration (replacing
//! `tasklist::detect_task_list`) and for host injects such as Kory's
//! `ui_planning` dimension. Prefer the configured `evaluator` globs (cheap
//! seats first); if none are eligible, fall back to any available model. The
//! only way evaluation is allowed to report "no evaluator candidate" is when
//! the session has zero eligible models of any kind.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use agent_client_protocol::RequestCancellation;
use agent_client_protocol::schema::v1::{
    ContentBlock, Error as AcpError, McpServer, PromptRequest, SetSessionModeRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::candidate::{CandidateId, EffortLevel, TaskClass};
use crate::config::{ActWhen, Config, PreClassifierConfig};
use crate::session::{
    DownstreamRoute, OpenedSession, Shared, close_downstream_session, first_eligible_candidate,
    open_downstream_session,
};

/// Marker embedded in the evaluator prompt so mock agents (and logs) can
/// recognize a pre-class turn.
pub const PRECLASS_MARKER: &str = "[router-acp pre-classifier]";

/// Overall cap (chars) on the prompt text handed to the evaluator.
const PROMPT_TRUNCATE: usize = 8000;
/// Head kept when the text exceeds `PROMPT_TRUNCATE`.
const ELIDE_HEAD: usize = 3000;
/// Tail kept when the text exceeds `PROMPT_TRUNCATE`.
const ELIDE_TAIL: usize = 5000;
const ELISION_MARKER: &str = "\n[… middle elided for classification …]\n";
/// Per-block budget for an auto-injected ticket block: enough to see what the
/// ticket covers, never enough to crowd out the user's own instruction.
const TICKET_CONTEXT_BUDGET: usize = 1200;
const TICKET_ELISION_MARKER: &str =
    "\n[… ticket context truncated for classification; classify the user's own message below …]";

/// Upper bound on the evaluator *open* phase (process spawn + `session/new`).
/// A cold ACP open routinely takes seconds, so it gets its own generous budget
/// instead of eating the prompt budget: `min(probe_timeout_ms, this)`.
const OPEN_TIMEOUT_CAP_MS: u64 = 30_000;

/// How often the Phase 2 guard samples liveness + streamed progress. Cheap
/// (two mutex reads), so it can be tight enough that a dead peer is noticed
/// promptly without meaningfully polling-taxing a healthy turn.
const GUARD_TICK_MS: u64 = 500;

/// Structured outcome of one pre-class evaluation (success or fail-open).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreClassResult {
    pub ok: bool,
    /// Human-readable skip/error reason when `ok` is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluator: Option<String>,
    pub latency_ms: u64,
    /// Authoritative task class/difficulty for model selection when present.
    /// Missing/invalid output fails open to the static classifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<RoutingDecision>,
    pub orchestrate: Option<OrchestrateDecision>,
    /// Extension dimension id → raw JSON object the evaluator returned.
    #[serde(default)]
    pub dimensions: BTreeMap<String, Value>,
    /// Modes that will act (inject / orchestrate) this turn.
    #[serde(default)]
    pub acted_modes: Vec<String>,
    /// Host inject prompts to prepend this turn (ordered).
    #[serde(default)]
    pub injects: Vec<String>,
    /// Multi-line audit log for disclosure / FE expanded view.
    pub log: String,
    /// Compact structured summary for FE (not only log text).
    pub summary: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingDecision {
    pub task_class: TaskClass,
    /// Other materially involved router task classes, for audit/explanation.
    #[serde(default)]
    pub task_classes: Vec<TaskClass>,
    /// Human-facing domains such as UX, frontend, database, infrastructure.
    #[serde(default)]
    pub categories: Vec<String>,
    /// Opaque host-defined capabilities needed to begin this task. Their
    /// definitions come from a configured classifier extension, never router
    /// source code.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// 0.0 (mechanical/trivial) to 1.0 (long-horizon/high-risk).
    pub complexity: f64,
    pub confidence: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<EffortLevel>,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrateDecision {
    pub warranted: bool,
    pub confidence: f64,
    #[serde(default = "default_parts")]
    pub estimated_parts: usize,
    #[serde(default)]
    pub reason: String,
}

fn default_parts() -> usize {
    1
}

/// The workspace the evaluator's throwaway session should open with: the host's
/// nominated classification directory when configured, else the classified
/// session's own.
///
/// The evaluator classifies the prompt text, not the project, and its session is
/// opened tool-less — but cwd is how every agent finds its project context, so
/// without this it loads that context anyway and the host pays a second full
/// startup per session. `additional_directories` goes with the cwd: nominating a
/// classification directory means "not the project", and leaving the extra
/// directories attached would re-admit part of what was just excluded.
///
/// Pure — no I/O, and it does not check that the directory exists. A bad path is
/// the host's error to see: it surfaces as a `session/new` failure, which the
/// evaluator walk already reports and fails over on, rather than being silently
/// swapped for a fallback that quietly restores the cost this exists to avoid.
pub(crate) fn evaluator_workspace(
    cfg: &PreClassifierConfig,
    session_cwd: PathBuf,
    session_dirs: Vec<PathBuf>,
) -> (PathBuf, Vec<PathBuf>) {
    match cfg.evaluator_cwd.as_ref() {
        Some(dir) => (dir.clone(), Vec::new()),
        None => (session_cwd, session_dirs),
    }
}

/// Whether auto pre-class should run for this prompt.
///
/// v1: first classify event per session (not every mid-session turn). Forced
/// `orchestrate:` still bypasses the auto path without needing pre-class for
/// the force decision; we still run pre-class on the first eligible turn so
/// extension dimensions (e.g. ui_planning) can inject.
pub fn should_run(cfg: &PreClassifierConfig, already_ran: bool, eligible_turn: bool) -> bool {
    cfg.enabled && eligible_turn && !already_ran
}

/// Build the single evaluator user-message text (system rules + dimensions +
/// truncated prompt). Pure — no I/O.
pub fn build_evaluator_prompt(cfg: &Config, user_text: &str) -> String {
    let mut out = String::new();
    out.push_str(PRECLASS_MARKER);
    out.push_str(
        "\nYou are a routing pre-classifier. Reply with ONE JSON object only — no markdown \
         fences, no prose before or after. Temperature 0. No tools.\n\n",
    );

    let mut schema_keys: Vec<String> = Vec::new();

    out.push_str(
        "## Dimension: routing (REQUIRED)\n\
         Assess the work itself, not prompt length or formatting. Choose one primary task_class \
         from: UiTweak, BugFix, Feature, Refactor, Algorithms, Architecture, Research, Writing, \
         Ops, CodingGeneral. UiTweak includes UX/UI/frontend visual or interaction changes; Ops \
         includes CI, deployment, configuration, and operational inspection.\n\
         Return task_classes for other materially involved classes and categories for human \
         domains such as UX, frontend, backend, database, infrastructure, security, docs.\n\
         Score complexity using these anchors:\n\
         - 0.00-0.15: mechanical, localized, obvious change or lookup\n\
         - 0.16-0.35: bounded routine work with a known implementation path\n\
         - 0.36-0.60: multi-file debugging/implementation with meaningful verification\n\
         - 0.61-0.80: ambiguous design, cross-system reasoning, migration, or high blast radius\n\
         - 0.81-1.00: novel/long-horizon work with deep architecture, uncertainty, or severe risk\n\
         Consider ambiguity, reasoning depth, dependencies, blast radius, reversibility, domain \
         novelty, and verification burden. A follow-up after a failed or incomplete fix is strong \
         evidence that the task is harder than it first appeared: phrases such as \"still broken\", \
         \"that didn't work\", \"following up\", a reopened issue, or a prior agent failing the \
         task should score complexity at least 0.70. A bug or fix that has survived MULTIPLE \
         rounds of attempts — \"still failing after N tries\", several prior agents/sessions each \
         reporting the fix didn't hold, a reopened issue with its own history of reopens — is \
         evidence the difficulty itself is severe, not merely that one attempt fell short: score \
         it 0.90-1.00. Do NOT increase complexity merely because a ticket is long, contains many \
         bullets, or supplies detailed context.\n\
         The text may include auto-injected CONTEXT: \"[Ticket …]\" blocks pulled from a ticket \
         system, and bracketed client or transport notes (lines opening with a tag such as \
         \"[client-name]\") listing available skills or conventions. Context is background, NOT \
         the ask — identify the user's own instruction and classify THAT.\n\
         When the instruction delegates to the ticket (\"fix ABC-123\", \"implement this ticket\"), \
         classify the ticket's work. When the instruction is itself a specific bounded action — \
         ship, merge, rebase, or review an existing PR, check out a branch, run or re-run a \
         command or workflow — classify that action: shipping an already-implemented, \
         already-reviewed PR is mechanical Ops, complexity at most 0.20, however hard the \
         underlying ticket was.\n\
         A one-line symptom report (\"page X is slow — fix it\") with no evidence of cross-system \
         spread is typically 0.36-0.60, not higher.\n\
         Work whose deliverable includes documentation but whose substance is deciding policy or \
         enforcement (hooks, permission models, invariants, migration order) is design or \
         implementation work: class it by that substance rather than Writing, and score the \
         decisions, not the prose.\n\
         Also choose routing.effort from exactly: low, medium, high, xhigh, max. Use low for \
         mechanical work, medium for bounded routine work, high for multi-file implementation, \
         xhigh for ambiguous cross-system reasoning, and max only for novel or severe-risk work.\n\
         Return:\n\
         \"routing\": { \"task_class\": string, \"task_classes\": [strings], \
         \"categories\": [strings], \"required_capabilities\": [strings], \"complexity\": 0.0-1.0, \
         \"confidence\": 0.0-1.0, \"effort\": \"low|medium|high|xhigh|max\", \"reason\": string }\n\n",
    );
    schema_keys.push("routing".into());

    if cfg.orchestration.enabled {
        out.push_str(
            "## Dimension: orchestrate\n\
             Decide whether the router should auto-orchestrate this prompt as a multi-track \
             implementation job (plan → parallel subtasks → review).\n\
             YES only when the work decomposes into genuinely INDEPENDENT tracks that different \
             workers could implement in parallel.\n\
             Sequential workflow STAGES of one deliverable (investigate → implement → test → \
             verify → ship; rebase → CI → merge) are ONE track — never count them as parts.\n\
             NO for: enumerated prose, Q&A lists, design discussions, plans/RFCs the user wants \
             discussed (not built yet), single-track tasks, answers to the model's questions, \
             pure research/explanation.\n\
             Shipping or merging an existing PR, a single bug fix (even one needing \
             investigation), and any single-artifact change are all single-track: \
             warranted=false.\n\
             estimated_parts counts parallelizable tracks, not steps.\n\
             Return:\n\
             \"orchestrate\": { \"warranted\": bool, \"confidence\": 0.0-1.0, \
             \"estimated_parts\": int >= 1, \"reason\": string }\n\n",
        );
        schema_keys.push("orchestrate".into());
    }

    for dim in &cfg.pre_classifier.dimensions {
        out.push_str(&format!("## Dimension: {}\n", dim.id));
        out.push_str(dim.description.trim());
        out.push('\n');
        out.push_str(
            "Return a JSON object under this id. Include at least \
             \"confidence\": 0.0-1.0 and any fields your description requires \
             (e.g. \"mode\", \"warranted\", \"reason\").\n\n",
        );
        schema_keys.push(dim.id.clone());
    }

    out.push_str("## Required JSON shape\n{\n");
    for (i, key) in schema_keys.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str(&format!("  \"{key}\": {{ ... }}"));
    }
    if schema_keys.is_empty() {
        out.push_str("  \"ok\": true");
    }
    out.push_str("\n}\n\n## User prompt\n");
    out.push_str(&elide_middle(user_text));
    out
}

/// Flatten the prompt for classification, bounding each auto-injected ticket
/// block so the user's own message always survives. `tickets::enrich_prompt`
/// PREPENDS ticket bodies (up to 16k chars each), so a plain flatten fed the
/// evaluator ticket text and nothing else.
fn build_classified_text(prompt: &[ContentBlock]) -> String {
    let mut out = String::new();
    for b in prompt {
        if let ContentBlock::Text(t) = b {
            if !out.is_empty() {
                out.push('\n');
            }
            if crate::tickets::is_injected_ticket_block(&t.text) {
                out.push_str(&truncate_head(
                    &t.text,
                    TICKET_CONTEXT_BUDGET,
                    TICKET_ELISION_MARKER,
                ));
            } else {
                out.push_str(&t.text);
            }
        }
    }
    out
}

/// Keep the first `max` chars, appending `marker` only when that actually cut.
fn truncate_head(s: &str, max: usize, marker: &str) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push_str(marker);
    out
}

/// Bound `s` to `PROMPT_TRUNCATE` by eliding its MIDDLE. Auto-injected context
/// rides the head while the user's own message rides the tail, so head-only
/// truncation deleted the actual ask.
fn elide_middle(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= PROMPT_TRUNCATE {
        return s.to_string();
    }
    let head: String = chars[..ELIDE_HEAD].iter().collect();
    let tail: String = chars[chars.len() - ELIDE_TAIL..].iter().collect();
    format!("{head}{ELISION_MARKER}{tail}")
}

/// Parse the evaluator's free-text reply into a JSON object. Strips optional
/// markdown fences and leading/trailing prose.
pub fn parse_evaluator_json(raw: &str) -> Result<Value, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("empty evaluator reply".into());
    }
    // Prefer a fenced ```json block if present.
    if let Some(start) = trimmed.find("```") {
        let after = &trimmed[start + 3..];
        let after = after
            .strip_prefix("json")
            .or_else(|| after.strip_prefix("JSON"))
            .unwrap_or(after)
            .trim_start_matches('\n');
        if let Some(end) = after.find("```") {
            let block = after[..end].trim();
            if let Ok(v) = serde_json::from_str::<Value>(block) {
                return Ok(v);
            }
        }
    }
    // Largest {...} span.
    if let (Some(a), Some(b)) = (trimmed.find('{'), trimmed.rfind('}'))
        && a < b
    {
        let slice = &trimmed[a..=b];
        if let Ok(v) = serde_json::from_str::<Value>(slice) {
            return Ok(v);
        }
    }
    serde_json::from_str(trimmed).map_err(|e| format!("JSON parse failed: {e}"))
}

fn parse_orchestrate(v: &Value) -> Option<OrchestrateDecision> {
    let obj = v.as_object()?;
    let warranted = obj
        .get("warranted")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let confidence = obj
        .get("confidence")
        .and_then(|x| x.as_f64())
        .unwrap_or(0.0);
    let estimated_parts = obj
        .get("estimated_parts")
        .and_then(|x| x.as_u64())
        .map(|n| n as usize)
        .unwrap_or(1)
        .max(1);
    let reason = obj
        .get("reason")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    Some(OrchestrateDecision {
        warranted,
        confidence,
        estimated_parts,
        reason,
    })
}

fn parse_routing(v: &Value) -> Option<RoutingDecision> {
    let obj = v.as_object()?;
    let task_class = obj
        .get("task_class")
        .and_then(Value::as_str)
        .and_then(TaskClass::parse)?;
    let complexity = obj.get("complexity").and_then(Value::as_f64)?;
    let confidence = obj.get("confidence").and_then(Value::as_f64)?;
    if !complexity.is_finite() || !confidence.is_finite() {
        return None;
    }
    let effort = match obj.get("effort") {
        Some(Value::String(value)) => Some(EffortLevel::parse(value)?),
        Some(_) => return None,
        None => None,
    };
    let mut task_classes: Vec<TaskClass> = obj
        .get("task_classes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter_map(TaskClass::parse)
        .collect();
    if !task_classes.contains(&task_class) {
        task_classes.insert(0, task_class);
    }
    task_classes.dedup();
    let categories = obj
        .get("categories")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .take(8)
        .map(ToString::to_string)
        .collect();
    let required_capabilities = obj
        .get("required_capabilities")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .take(16)
        .map(ToString::to_string)
        .collect();
    Some(RoutingDecision {
        task_class,
        task_classes,
        categories,
        required_capabilities,
        complexity: complexity.clamp(0.0, 1.0),
        confidence: confidence.clamp(0.0, 1.0),
        effort,
        reason: obj
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    })
}

/// Decide which dimensions act and which inject texts to apply.
pub fn apply_thresholds(
    cfg: &Config,
    parsed: &Value,
    evaluator: Option<&str>,
    latency_ms: u64,
    raw_log: &str,
) -> PreClassResult {
    // Client-visible log stays compact: no "raw reply" dump (that leaked into
    // the chat as a giant JSON code block + dual classify cards). Full raw
    // stays in tracing for operators.
    if !raw_log.is_empty() {
        tracing::debug!(raw = %raw_log, "pre-class evaluator raw reply");
    }
    let mut log = String::new();
    log.push_str(&format!(
        "router-acp · pre-class · evaluator={} · {}ms\n",
        evaluator.unwrap_or("(none)"),
        latency_ms
    ));

    let mut acted_modes = Vec::new();
    let mut injects = Vec::new();
    let mut dimensions = BTreeMap::new();
    let mut summary_dims = serde_json::Map::new();

    let routing = parsed.get("routing").and_then(parse_routing);
    if let Some(ref decision) = routing {
        summary_dims.insert(
            "routing".into(),
            json!({
                "task_class": decision.task_class.as_str(),
                "task_classes": decision.task_classes.iter().map(TaskClass::as_str).collect::<Vec<_>>(),
                "categories": decision.categories,
                "complexity": decision.complexity,
                "confidence": decision.confidence,
                "reason": decision.reason,
                "authoritative": true,
            }),
        );
        log.push_str(&format!(
            "routing: class={} complexity={:.2} conf={:.2} categories={} — {}\n",
            decision.task_class.as_str(),
            decision.complexity,
            decision.confidence,
            if decision.categories.is_empty() {
                "none".to_string()
            } else {
                decision.categories.join(",")
            },
            decision.reason
        ));
    } else {
        summary_dims.insert(
            "routing".into(),
            json!({ "present": false, "authoritative": false, "fallback": "static classifier" }),
        );
        log.push_str("routing: missing/invalid — static classifier fallback\n");
    }

    let orchestrate = parsed.get("orchestrate").and_then(parse_orchestrate);
    if let Some(ref o) = orchestrate {
        let thr = cfg.pre_classifier.orchestrate_min_confidence;
        let acts = o.warranted && o.confidence >= thr;
        summary_dims.insert(
            "orchestrate".into(),
            json!({
                "warranted": o.warranted,
                "confidence": o.confidence,
                "estimated_parts": o.estimated_parts,
                "reason": o.reason,
                "acts": acts,
                "threshold": thr,
            }),
        );
        log.push_str(&format!(
            "orchestrate: warranted={} conf={:.2} parts={} acts={} — {}\n",
            o.warranted, o.confidence, o.estimated_parts, acts, o.reason
        ));
        if acts {
            acted_modes.push("orchestrate".into());
        }
    }

    for dim in &cfg.pre_classifier.dimensions {
        let Some(val) = parsed.get(&dim.id) else {
            summary_dims.insert(dim.id.clone(), json!({ "present": false, "acts": false }));
            log.push_str(&format!("{}: missing from evaluator reply\n", dim.id));
            continue;
        };
        dimensions.insert(dim.id.clone(), val.clone());
        let conf = val
            .get("confidence")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let acts = conf >= dim.min_confidence && act_when_matches(&dim.act_when, val);
        summary_dims.insert(
            dim.id.clone(),
            json!({
                "present": true,
                "confidence": conf,
                "acts": acts,
                "threshold": dim.min_confidence,
                "value": val,
                "injected": acts && !dim.inject_prompt.trim().is_empty(),
            }),
        );
        log.push_str(&format!(
            "{}: conf={:.2} acts={} raw={}\n",
            dim.id, conf, acts, val
        ));
        if acts {
            // FE chip label: ui_planning → "UI"
            let chip = dimension_chip(&dim.id);
            if !acted_modes.iter().any(|m| m == &chip) {
                acted_modes.push(chip);
            }
            if !dim.inject_prompt.trim().is_empty() {
                injects.push(dim.inject_prompt.clone());
            }
        }
    }

    let modes_line = if acted_modes.is_empty() {
        "none".to_string()
    } else {
        acted_modes.join(",")
    };
    log.push_str(&format!(
        "router-acp · pre-class done · modes={modes_line}\n"
    ));

    let summary_modes: Vec<String> = if acted_modes.is_empty() {
        vec!["none".into()]
    } else {
        acted_modes.clone()
    };

    PreClassResult {
        ok: true,
        skip_reason: None,
        evaluator: evaluator.map(|s| s.to_string()),
        latency_ms,
        routing,
        orchestrate,
        dimensions,
        acted_modes,
        injects,
        log,
        summary: json!({
            "ok": true,
            "evaluator": evaluator,
            "latency_ms": latency_ms,
            "acted_modes": summary_modes,
            "dimensions": summary_dims,
        }),
    }
}

fn dimension_chip(id: &str) -> String {
    match id {
        "ui_planning" => "UI".into(),
        other => other.to_string(),
    }
}

fn act_when_matches(act: &ActWhen, val: &Value) -> bool {
    match act {
        ActWhen::FieldEquals { field, equals } => val
            .get(field)
            .and_then(|v| v.as_str())
            .map(|s| s == equals)
            .unwrap_or(false),
        ActWhen::Warranted { warranted } => val
            .get("warranted")
            .and_then(|v| v.as_bool())
            .map(|b| b == *warranted)
            .unwrap_or(false),
    }
}

fn fail_open(
    reason: impl Into<String>,
    evaluator: Option<String>,
    latency_ms: u64,
    extra_log: &str,
) -> PreClassResult {
    let reason = reason.into();
    let mut log = format!(
        "router-acp · pre-class · skip · {reason} · evaluator={} · {}ms\n",
        evaluator.as_deref().unwrap_or("(none)"),
        latency_ms
    );
    if !extra_log.is_empty() {
        log.push_str(extra_log);
        if !extra_log.ends_with('\n') {
            log.push('\n');
        }
    }
    log.push_str("router-acp · pre-class done · modes=none\n");
    PreClassResult {
        ok: false,
        skip_reason: Some(reason.clone()),
        evaluator: evaluator.clone(),
        latency_ms,
        routing: None,
        orchestrate: None,
        dimensions: BTreeMap::new(),
        acted_modes: Vec::new(),
        injects: Vec::new(),
        log,
        summary: json!({
            "ok": false,
            "skip_reason": reason,
            "evaluator": evaluator,
            "latency_ms": latency_ms,
            "acted_modes": ["none"],
        }),
    }
}

/// Run the evaluator as a short-lived tool-less ACP session on the first
/// eligible `evaluator` candidate. Fail-open on any error. Tries each matching
/// evaluator in preference order (a timed-out/broken haiku does not sink the
/// whole pre-class). When the preferred pool is empty or exhausted, widens to
/// **any** eligible model — only reports "no evaluator candidate" when the
/// session has zero models of any kind.
pub async fn evaluate(
    shared: &Arc<Shared>,
    router_sid: &str,
    prompt: &[ContentBlock],
    cancel: &RequestCancellation,
) -> PreClassResult {
    let started = Instant::now();
    let cfg = shared.cfg.clone();
    let pcfg = &cfg.pre_classifier;
    if !pcfg.enabled {
        return fail_open("pre_classifier disabled", None, 0, "");
    }

    let user_text = build_classified_text(prompt);
    let eval_prompt = build_evaluator_prompt(&cfg, &user_text);

    let (class, mut excluded, cwd, dirs) = shared
        .with_session(router_sid, |s| {
            (
                s.task_class.unwrap_or(TaskClass::CodingGeneral),
                s.excluded.clone(),
                s.cwd.clone(),
                s.additional_directories.clone(),
            )
        })
        .unwrap_or((
            TaskClass::CodingGeneral,
            Vec::new(),
            std::env::temp_dir(),
            Vec::new(),
        ));

    let (cwd, dirs) = evaluator_workspace(pcfg, cwd, dirs);

    let mut attempts: Vec<String> = Vec::new();
    let mut last_fail: Option<PreClassResult> = None;
    let any_model = ["*".to_string()];
    let mut widened_to_any = false;

    // Prefer-order walk: first_eligible_candidate ranks by quality within the
    // evaluator glob pool. A classifier failure is a real routing signal, not a
    // reason to give up: a down / out-of-credits evaluator is run through the
    // router's normal failure classification + cordon (`apply_failure`), then
    // excluded so the next eligible evaluator is tried — the same failover the
    // main pin uses. When preferred globs match nothing (or are exhausted),
    // widen to any eligible model — the classifier can run on Grok/Opus/etc.
    // just as well as on Haiku; the prefs are only a cost preference. There is
    // NO total wall-clock timeout on the evaluator's LLM call (a slow model must
    // be allowed to finish); the interrupts are a connection failure, a peer
    // death, a stall (no streamed progress — see `guard_turn`), or the client
    // cancelling the turn. Loop until no eligible model remains (excluded grows
    // each attempt), never a fixed attempt cap.
    loop {
        if cancel.is_cancelled() {
            return fail_open(
                "pre-class cancelled by client",
                None,
                started.elapsed().as_millis() as u64,
                &format!("tried evaluators: {}\n", attempts.join(", ")),
            );
        }
        let Some(candidate) = first_eligible_candidate(shared, &pcfg.evaluator, class, &excluded)
            .or_else(|| {
                // Preferred pool empty/exhausted — any model is a valid evaluator.
                if !widened_to_any {
                    widened_to_any = true;
                    tracing::info!(
                        session = router_sid,
                        preferred = ?pcfg.evaluator,
                        "pre-class: preferred evaluators unavailable; widening to any eligible model"
                    );
                }
                first_eligible_candidate(shared, &any_model, class, &excluded)
            })
        else {
            break;
        };
        let cand_str = candidate.to_string();
        // Skip already-tried exact ids even if the glob still matches.
        if attempts.iter().any(|a| a == &cand_str) {
            excluded.push(cand_str);
            continue;
        }

        let outcome = evaluate_on_candidate(
            shared,
            router_sid,
            &candidate,
            cwd.clone(),
            dirs.clone(),
            &eval_prompt,
            &cfg,
            started,
            cancel,
        )
        .await;
        attempts.push(cand_str.clone());

        match outcome {
            // A classification with a routing decision is the only success —
            // the classifier's core output must be present to proceed.
            Ok(result) if result.routing.is_some() => return result,
            // Model responded but gave no usable classification (parse failure /
            // missing routing): try the next evaluator.
            Ok(result) => {
                tracing::warn!(
                    session = router_sid,
                    evaluator = %cand_str,
                    reason = result.skip_reason.as_deref().unwrap_or("no routing decision"),
                    "pre-class evaluator produced no classification; trying next"
                );
                last_fail = Some(result);
            }
            // Service failure (down / rate-limited / out of credits): hand it to
            // the router's known failure handling (classify + cordon), then fail
            // over to the next evaluator — exactly as a pinned turn would.
            Err(err) => {
                if crate::downstream::is_auth_required(&err) {
                    crate::auth::note_unauthenticated(
                        &shared.auth,
                        &candidate.agent,
                        format!("{} is not signed in", candidate.agent),
                    );
                }
                let class = crate::limits::classify_failure(&err);
                let human = crate::session::apply_failure(shared, &candidate, &err, &class);
                tracing::warn!(
                    session = router_sid,
                    evaluator = %cand_str,
                    %human,
                    "pre-class evaluator failed; cordoned and failing over"
                );
                last_fail = Some(fail_open(
                    format!("evaluator {cand_str} failed: {human}"),
                    Some(cand_str.clone()),
                    started.elapsed().as_millis() as u64,
                    "",
                ));
            }
        }
        excluded.push(cand_str);
    }

    // No evaluator produced a classification. This is a fail result (routing is
    // None); the caller decides the terminal policy (hard-fail when the
    // pre-classifier is enabled — see `dispatch_prompt`). "no evaluator
    // candidate available" is reserved for the zero-models case only.
    let latency_ms = started.elapsed().as_millis() as u64;
    if let Some(mut fail) = last_fail {
        fail.latency_ms = latency_ms;
        fail.log
            .push_str(&format!("tried evaluators: {}\n", attempts.join(", ")));
        return fail;
    }
    fail_open("no evaluator candidate available", None, latency_ms, "")
}

/// One evaluator attempt: open in the safe preclass mode when the adapter has
/// modes, then prompt → close → parse.
///
/// Returns `Ok(PreClassResult)` when the evaluator model responded (the result
/// may still lack a routing decision on a parse failure) or the session could
/// not be established (fail-open, failover to the next evaluator). Returns
/// `Err(AcpError)` when the model's prompt turn itself failed — a real service
/// failure (down / rate-limited / out of credits) that the caller runs through
/// the router's known failure handling and cordons before failing over.
#[allow(clippy::too_many_arguments)]
async fn evaluate_on_candidate(
    shared: &Arc<Shared>,
    router_sid: &str,
    candidate: &CandidateId,
    cwd: std::path::PathBuf,
    dirs: Vec<std::path::PathBuf>,
    eval_prompt: &str,
    cfg: &Config,
    started: Instant,
    cancel: &RequestCancellation,
) -> Result<PreClassResult, AcpError> {
    let cand_str = candidate.to_string();
    let capture = Arc::new(Mutex::new(String::new()));
    let violation = Arc::new(AtomicBool::new(false));

    // Phase 1 — spawn + session/new + explicit preclass mode. This is connection
    // ESTABLISHMENT (process spawn + handshake), not the classifier LLM call, so
    // it keeps a bounded guard purely as a wedged-spawn deadlock backstop; on
    // expiry it fails over to the next evaluator (never a silent proceed).
    let open_timeout_ms = cfg.probe_timeout_ms.clamp(1, OPEN_TIMEOUT_CAP_MS);
    let opened = match tokio::time::timeout(
        Duration::from_millis(open_timeout_ms),
        open_evaluator_session(
            shared,
            router_sid,
            candidate,
            cwd,
            dirs,
            capture.clone(),
            violation.clone(),
        ),
    )
    .await
    {
        Err(_) => {
            return Ok(fail_open(
                format!("evaluator session open timed out after {open_timeout_ms}ms"),
                Some(cand_str),
                started.elapsed().as_millis() as u64,
                "",
            ));
        }
        Ok(Err(err)) => {
            return Ok(fail_open(
                format!("evaluator session open failed: {err}"),
                Some(cand_str),
                started.elapsed().as_millis() as u64,
                "",
            ));
        }
        Ok(Ok(opened)) => opened,
    };

    // Phase 2 — the classifier LLM generation. Still NO *total* wall-clock
    // timeout: core infrastructure that must be allowed to run to completion,
    // so a slow-but-working evaluator is never cut off. What IS bounded now is
    // silence — see `guard_turn`. A downstream peer that dies outright already
    // resolves `block_task()` promptly on its own (verified:
    // `preclass_dead_evaluator_peer_fails_over_promptly`) — `send_request`'s
    // wait runs on the *upstream* connection, not a task spawned on the dying
    // downstream one, so it isn't the `relay_request`-documented hang. The gap
    // this guard closes is the process that stays alive but never answers and
    // never streams: that produces neither a client cancel nor a connection
    // error, so nothing used to end the turn (observed 2026-08-10: a live
    // session wedged 2h57m on exactly this). `guard_turn` also re-checks
    // liveness each tick as cheap belt-and-suspenders, in case a future
    // regression ever makes that path slower to resolve.
    let request = PromptRequest::new(
        opened.downstream_sid.clone(),
        vec![ContentBlock::from(eval_prompt.to_string())],
    );
    let stall_timeout_ms = cfg.pre_classifier.stall_timeout_ms;
    let turn = cancel
        .run_until_cancelled(guard_turn(
            shared,
            router_sid,
            &cand_str,
            &opened,
            &capture,
            request,
            stall_timeout_ms,
        ))
        .await;
    close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);

    let latency_ms = started.elapsed().as_millis() as u64;

    if violation.load(Ordering::Acquire) {
        return Err(AcpError::internal_error().data("evaluator attempted tool use"));
    }

    match turn {
        // Client cancelled the turn — not a service failure; do not cordon.
        Err(_) if cancel.is_cancelled() => Ok(fail_open(
            "evaluator cancelled by client",
            Some(cand_str),
            latency_ms,
            "",
        )),
        // The model's prompt turn failed: surface the error so the caller can
        // classify it (rate-limit / outage / other) and cordon + fail over.
        Err(err) => Err(err),
        Ok(_) => {
            let raw = capture.lock().unwrap().clone();
            // mock-agent echoes `echo:<model>:<text>` — strip that wrapper when present.
            let body = strip_mock_echo(&raw);
            match parse_evaluator_json(&body) {
                Ok(parsed) => {
                    let mut raw_log = format!("raw reply:\n{body}\n");
                    raw_log.push_str(&format!("parsed: {parsed}\n"));
                    Ok(apply_thresholds(
                        cfg,
                        &parsed,
                        Some(&cand_str),
                        latency_ms,
                        &raw_log,
                    ))
                }
                Err(e) => {
                    tracing::debug!(
                        evaluator = %cand_str,
                        error = %e,
                        raw_reply = %body,
                        "pre-class evaluator reply could not be parsed"
                    );
                    Ok(fail_open(
                        format!("parse failed: {e}"),
                        Some(cand_str),
                        latency_ms,
                        "",
                    ))
                }
            }
        }
    }
}

/// Await the evaluator's prompt turn, but never indefinitely.
///
/// Returns the turn's own result when it completes — this already covers
/// outright peer death, which resolves `block_task()` promptly on its own.
/// Returns `Err` — which the caller classifies as a service failure and
/// cordons + fails over — when the process is silent past `stall_timeout_ms`.
/// The primary purpose here is the stall guard; the liveness re-check each
/// tick is a redundant belt-and-suspenders check against the process dying
/// with `guard_turn` itself somehow not yet noticing.
///
/// "Silence" is measured against streamed progress (`capture` growing), not
/// elapsed time, so an evaluator that is slow but still emitting tokens is
/// never cut off. An adapter that streams nothing until its final chunk gets
/// the whole budget as one window, which is why the default is generous.
async fn guard_turn(
    shared: &Arc<Shared>,
    router_sid: &str,
    cand_str: &str,
    opened: &OpenedSession,
    capture: &Arc<Mutex<String>>,
    request: PromptRequest,
    stall_timeout_ms: u64,
) -> Result<agent_client_protocol::schema::v1::PromptResponse, AcpError> {
    let turn = opened.conn.send_request(request).block_task();
    tokio::pin!(turn);

    let mut last_len = capture.lock().unwrap().len();
    let mut last_progress = Instant::now();
    let stall = (stall_timeout_ms > 0).then(|| Duration::from_millis(stall_timeout_ms));

    loop {
        tokio::select! {
            // Bias the turn: on a tie a real response always wins over the tick,
            // so a turn that lands in the same instant is never mis-reported.
            biased;
            result = &mut turn => return result,
            _ = tokio::time::sleep(Duration::from_millis(GUARD_TICK_MS)) => {
                // The peer died: shared state knows even though our future never woke.
                if shared.target_conn(&opened.process_key).is_none() {
                    tracing::warn!(
                        session = router_sid,
                        evaluator = %cand_str,
                        "pre-class evaluator peer died mid-turn; failing over"
                    );
                    return Err(AcpError::internal_error()
                        .data("evaluator connection died mid-turn"));
                }
                let len = capture.lock().unwrap().len();
                if len != last_len {
                    last_len = len;
                    last_progress = Instant::now();
                    continue;
                }
                if let Some(stall) = stall
                    && last_progress.elapsed() >= stall
                {
                    let silent_ms = last_progress.elapsed().as_millis() as u64;
                    tracing::warn!(
                        session = router_sid,
                        evaluator = %cand_str,
                        silent_ms,
                        streamed_bytes = len,
                        "pre-class evaluator stalled (no streamed progress); failing over"
                    );
                    return Err(AcpError::internal_error().data(format!(
                        "evaluator stalled: no progress for {silent_ms}ms"
                    )));
                }
            }
        }
    }
}

fn strip_mock_echo(raw: &str) -> String {
    // mock-agent default: `echo:<model>:<text>` possibly multi-line after second colon.
    if let Some(rest) = raw.strip_prefix("echo:")
        && let Some((_, body)) = rest.split_once(':')
    {
        return body.to_string();
    }
    raw.to_string()
}

/// Spawn the evaluator process if needed and open a tool-less session on it.
/// Agents that advertise session modes must have an explicit
/// `mode_map.preclass` safe mode. Agents that advertise no modes have no
/// permission gate to arm and proceed without a `set_mode` request.
/// Caller owns closing the returned session.
async fn open_evaluator_session(
    shared: &Arc<Shared>,
    router_sid: &str,
    candidate: &CandidateId,
    cwd: std::path::PathBuf,
    dirs: Vec<std::path::PathBuf>,
    capture: Arc<Mutex<String>>,
    violation: Arc<AtomicBool>,
) -> Result<OpenedSession, String> {
    // Ensure the target process is live (same as pin/delegate).
    let runtime = shared
        .candidate_runtime(candidate)
        .ok_or_else(|| format!("unknown candidate {candidate}"))?;
    let key = runtime.process_key.clone();
    if shared.target_conn(&key).is_none() {
        crate::downstream::start_downstream(shared, &key)
            .await
            .map_err(|e| format!("start_downstream: {e}"))?;
    }

    let opened = open_downstream_session(
        shared,
        candidate,
        cwd,
        dirs,
        Vec::<McpServer>::new(), // tool-less
        DownstreamRoute::PreClass { capture, violation },
    )
    .await
    .map_err(|e| format!("session/new: {e}"))?;

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
    if available_modes.is_empty() {
        tracing::info!(
            session = router_sid,
            candidate = %candidate,
            "pre-class evaluator advertises no session modes; proceeding without set_mode"
        );
    } else {
        let mode_id = shared
            .cfg
            .agents
            .iter()
            .find(|agent| agent.name == candidate.agent)
            .and_then(|agent| agent.mode_map.get("preclass"))
            .filter(|mode| available_modes.iter().any(|available| available == *mode))
            .cloned();
        let Some(mode_id) = mode_id else {
            close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);
            return Err(format!(
                "no advertised mode_map.preclass target; modes={available_modes:?}"
            ));
        };
        let set = SetSessionModeRequest::new(opened.downstream_sid.clone(), mode_id.clone());
        if let Err(err) = opened.conn.send_request(set).block_task().await {
            close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);
            return Err(format!(
                "set_mode preclass ({mode_id}) rejected; modes={available_modes:?}: {err}"
            ));
        }
        tracing::info!(
            session = router_sid,
            candidate = %candidate,
            applied = %mode_id,
            "pre-class evaluator session mode applied"
        );
    }

    Ok(opened)
}

/// Emit disclosure lines for a pre-class result (start was optional; done is always).
pub fn disclose(shared: &Arc<Shared>, router_sid: &str, result: &PreClassResult) {
    if !shared.cfg.pre_classifier.disclose {
        return;
    }
    // ONE multi-line block: log + summary. Two separate notify_user calls
    // made the FE peel two classify cards for a single evaluation.
    let mut block = result.log.trim_end().to_string();
    if !block.is_empty() && !block.ends_with('\n') {
        block.push('\n');
    }
    block.push_str(&format!(
        "router-acp · pre-class summary · {}",
        result.summary
    ));
    crate::session::notify_user(shared, router_sid, block);
}

/// Compact, authoritative note prepended to the agent's turn so it cannot
/// re-litigate routing (e.g. re-running `tasklist::detect_task_list` and telling
/// the user "orchestration will fire — override with orchestrate:"). UI-only
/// disclosures are peeled into a tool card and never reach the model.
pub fn agent_decision_note(cfg: &Config, result: &PreClassResult) -> String {
    let thr = cfg.pre_classifier.orchestrate_min_confidence;
    let mut lines = vec![
        "[router-acp pre-class decision — AUTHORITATIVE for this turn]".to_string(),
        "The router already evaluated auto-orchestration and host dimensions.".to_string(),
        "Do NOT re-run tasklist heuristics, do NOT claim orchestration will fire,".to_string(),
        "and do NOT tell the user to `orchestrate:` unless they explicitly want it.".to_string(),
    ];

    if !result.ok {
        lines.push(format!(
            "Pre-class FAIL-OPEN: {} — auto-orchestration was NOT started.",
            result.skip_reason.as_deref().unwrap_or("unknown error")
        ));
    } else if let Some(o) = result.orchestrate.as_ref() {
        let acts = o.warranted && o.confidence >= thr;
        if acts {
            lines.push(format!(
                "Auto-orchestration: WILL RUN (warranted=true, confidence={:.2} ≥ thr={thr:.2}, ~{} parts). Reason: {}",
                o.confidence, o.estimated_parts, o.reason
            ));
        } else {
            lines.push(format!(
                "Auto-orchestration: SUPPRESSED (warranted={}, confidence={:.2}, thr={thr:.2}). Reason: {}",
                o.warranted, o.confidence, o.reason
            ));
            lines.push(
                "A multi-bullet ticket body alone does NOT orchestrate while pre_classifier is on."
                    .to_string(),
            );
        }
    } else if cfg.orchestration.enabled {
        lines.push(
            "Auto-orchestration: SUPPRESSED (no orchestrate decision in evaluator reply)."
                .to_string(),
        );
    }

    if let Some(routing) = result.routing.as_ref() {
        lines.push(format!(
            "Routing assessment: class={} complexity={:.2} confidence={:.2}; categories={}. Reason: {}",
            routing.task_class.as_str(),
            routing.complexity,
            routing.confidence,
            if routing.categories.is_empty() {
                "none".to_string()
            } else {
                routing.categories.join(",")
            },
            routing.reason
        ));
    } else {
        lines.push("Routing assessment: unavailable; static classifier fallback.".to_string());
    }

    for dim in &cfg.pre_classifier.dimensions {
        if let Some(val) = result.dimensions.get(&dim.id) {
            let conf = val
                .get("confidence")
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0);
            let acts = conf >= dim.min_confidence && act_when_matches(&dim.act_when, val);
            let mode = val.get("mode").and_then(|x| x.as_str()).unwrap_or("—");
            let reason = val.get("reason").and_then(|x| x.as_str()).unwrap_or("");
            lines.push(format!(
                "Dimension `{}`: acts={} mode={} conf={:.2} — {}",
                dim.id, acts, mode, conf, reason
            ));
        }
    }

    if !result.injects.is_empty() {
        lines.push(format!(
            "Host injects applied this turn: {}.",
            result.injects.len()
        ));
    }

    lines.join("\n")
}

/// Default description for the built-in orchestrate dimension (docs / tests).
pub fn orchestrate_dimension_description() -> &'static str {
    "Complex multi-track implementation with distinct sub-tracks — not enumerated prose/Q&A/plans."
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActWhen, PreClassDimension, PreClassifierConfig};

    fn cfg_with_dims(dims: Vec<PreClassDimension>) -> Config {
        let yaml = r#"
agents:
  - name: a
    command: { type: stdio, command: mock-agent }
    model_selection: { type: config-option }
    models: [{ id: m1, display_name: M1, cost_rank: 1 }]
"#;
        let mut cfg = Config::from_yaml(yaml).unwrap();
        cfg.orchestration.enabled = true;
        cfg.pre_classifier = PreClassifierConfig {
            enabled: true,
            evaluator: vec!["*m1*".into()],
            timeout_ms: 15_000,
            stall_timeout_ms: 90_000,
            disclose: true,
            orchestrate_min_confidence: 0.65,
            dimensions: dims,
            evaluator_cwd: None,
        };
        cfg
    }

    #[test]
    fn evaluator_workspace_defaults_to_the_classified_session_cwd() {
        let cfg = cfg_with_dims(vec![]);
        assert!(cfg.pre_classifier.evaluator_cwd.is_none());
        let (cwd, dirs) = evaluator_workspace(
            &cfg.pre_classifier,
            PathBuf::from("/repo"),
            vec![PathBuf::from("/extra")],
        );
        // Unset must not change behavior for hosts that never configure it.
        assert_eq!(cwd, PathBuf::from("/repo"));
        assert_eq!(dirs, vec![PathBuf::from("/extra")]);
    }

    #[test]
    fn evaluator_workspace_replaces_cwd_and_drops_extra_dirs() {
        let mut cfg = cfg_with_dims(vec![]);
        cfg.pre_classifier.evaluator_cwd = Some(PathBuf::from("/classify-here"));
        let (cwd, dirs) = evaluator_workspace(
            &cfg.pre_classifier,
            PathBuf::from("/repo"),
            vec![PathBuf::from("/extra")],
        );
        assert_eq!(cwd, PathBuf::from("/classify-here"));
        // The extra directories must go with the cwd: nominating a
        // classification directory means "not the project", and leaving these
        // attached would re-admit part of what was just excluded.
        assert!(dirs.is_empty(), "additional_directories must be dropped");
    }

    #[test]
    fn evaluator_cwd_parses_from_yaml_and_is_optional() {
        // Config carries deny_unknown_fields, so a host on an older binary must
        // not see this key — but a host on this one must be able to set it, and
        // omitting it must still load.
        let with = Config::from_yaml(
            r#"
agents:
  - name: a
    command: { type: stdio, command: mock-agent }
    model_selection: { type: config-option }
    models: [{ id: m1, display_name: M1, cost_rank: 1 }]
pre_classifier:
  enabled: true
  evaluator_cwd: /var/lib/router-acp/classify
"#,
        )
        .unwrap();
        assert_eq!(
            with.pre_classifier.evaluator_cwd,
            Some(PathBuf::from("/var/lib/router-acp/classify"))
        );

        let without = Config::from_yaml(
            r#"
agents:
  - name: a
    command: { type: stdio, command: mock-agent }
    model_selection: { type: config-option }
    models: [{ id: m1, display_name: M1, cost_rank: 1 }]
pre_classifier:
  enabled: true
"#,
        )
        .unwrap();
        assert!(without.pre_classifier.evaluator_cwd.is_none());
    }

    #[test]
    fn build_prompt_includes_marker_and_orchestrate() {
        let cfg = cfg_with_dims(vec![]);
        let p = build_evaluator_prompt(&cfg, "1. do a\n2. do b");
        assert!(p.contains(PRECLASS_MARKER));
        assert!(p.contains("Dimension: routing"));
        assert!(p.contains("Do NOT increase complexity merely because"));
        assert!(p.contains("follow-up after a failed or incomplete fix"));
        assert!(p.contains("at least 0.70"));
        assert!(p.contains("survived MULTIPLE"));
        assert!(p.contains("0.90-1.00"));
        assert!(p.contains("low, medium, high, xhigh, max"));
        assert!(p.contains("xhigh for ambiguous cross-system reasoning"));
        assert!(p.contains("Dimension: orchestrate"));
        assert!(p.contains("1. do a"));
        // Context-vs-ask + bounded-action + symptom-report + docs-deliverable anchors.
        assert!(p.contains("Context is background, NOT the ask"));
        assert!(p.contains("already-reviewed PR is mechanical Ops"));
        assert!(p.contains("typically 0.36-0.60, not higher"));
        assert!(p.contains("score the decisions, not the prose"));
        // Orchestrate: independent tracks, not workflow stages.
        assert!(p.contains("genuinely INDEPENDENT tracks"));
        assert!(p.contains("never count them as parts"));
        assert!(p.contains("estimated_parts counts parallelizable tracks, not steps"));
    }

    #[test]
    fn build_prompt_includes_extension_dimensions() {
        let cfg = cfg_with_dims(vec![PreClassDimension {
            id: "ui_planning".into(),
            description: "Is this UI work needing mockup-first planning?".into(),
            min_confidence: 0.70,
            act_when: ActWhen::FieldEquals {
                field: "mode".into(),
                equals: "planning".into(),
            },
            inject_prompt: "PLAN ME".into(),
        }]);
        let p = build_evaluator_prompt(&cfg, "redesign the dashboard");
        assert!(p.contains("Dimension: ui_planning"));
        assert!(p.contains("mockup-first"));
    }

    #[test]
    fn classified_text_bounds_injected_ticket_and_keeps_the_user_message() {
        let ticket = crate::tickets::frame_ticket("ABC-123", &"ticket body detail. ".repeat(450));
        assert!(
            ticket.chars().count() > 8_000,
            "fixture must exceed the cap"
        );
        let ask = "Ship PR #7009 (https://example) for ABC-123. Check out its branch `x`";
        let text = build_classified_text(&[
            ContentBlock::from(ticket),
            ContentBlock::from(ask.to_string()),
        ]);

        assert!(text.contains(ask), "the user's own message must survive");
        assert!(text.contains("ticket context truncated for classification"));
        // Everything but the user block and the joining newline is ticket-derived.
        let ticket_derived = text.chars().count() - ask.chars().count() - 1;
        assert!(
            ticket_derived <= 1_300,
            "ticket-derived chars: {ticket_derived}"
        );
    }

    #[test]
    fn classified_text_passes_small_prompts_through() {
        let text = build_classified_text(&[
            ContentBlock::from("fix the login redirect".to_string()),
            ContentBlock::from("it 404s on submit".to_string()),
        ]);
        assert_eq!(text, "fix the login redirect\nit 404s on submit");
    }

    #[test]
    fn oversized_prompt_elides_the_middle_not_the_tail() {
        let body = format!("{}{}", "A".repeat(10_000), "Z".repeat(10_000));
        let flattened = build_classified_text(&[ContentBlock::from(body.clone())]);
        assert_eq!(flattened, body, "a non-ticket block passes through whole");

        let out = elide_middle(&flattened);
        let head: String = body.chars().take(ELIDE_HEAD).collect();
        let tail: String = body
            .chars()
            .skip(body.chars().count() - ELIDE_TAIL)
            .collect();
        assert!(out.starts_with(&head));
        assert!(out.ends_with(&tail));
        assert!(out.contains("middle elided for classification"));
        assert!(
            out.chars().count() <= 8_100,
            "chars: {}",
            out.chars().count()
        );
    }

    #[test]
    fn parse_json_strips_fences() {
        let v = parse_evaluator_json(
            "Here you go:\n```json\n{\"orchestrate\":{\"warranted\":false,\"confidence\":0.9,\"estimated_parts\":1,\"reason\":\"qa\"}}\n```\n",
        )
        .unwrap();
        assert_eq!(v["orchestrate"]["warranted"], false);
    }

    #[test]
    fn thresholds_orchestrate_and_ui_inject() {
        let cfg = cfg_with_dims(vec![PreClassDimension {
            id: "ui_planning".into(),
            description: "ui".into(),
            min_confidence: 0.70,
            act_when: ActWhen::FieldEquals {
                field: "mode".into(),
                equals: "planning".into(),
            },
            inject_prompt: "INJECT-UI".into(),
        }]);
        let parsed = json!({
            "routing": {
                "task_class": "UiTweak",
                "task_classes": ["UiTweak", "Feature"],
                "categories": ["UX", "frontend"],
                "complexity": 0.42,
                "confidence": 0.91,
                "reason": "new interactive surface"
            },
            "orchestrate": {
                "warranted": true,
                "confidence": 0.9,
                "estimated_parts": 3,
                "reason": "multi-track impl"
            },
            "ui_planning": {
                "mode": "planning",
                "confidence": 0.85,
                "reason": "redesign"
            }
        });
        let r = apply_thresholds(&cfg, &parsed, Some("a/m1"), 12, "raw\n");
        assert!(r.ok);
        assert_eq!(r.routing.as_ref().unwrap().task_class, TaskClass::UiTweak);
        assert_eq!(r.routing.as_ref().unwrap().complexity, 0.42);
        assert!(r.acted_modes.contains(&"orchestrate".to_string()));
        assert!(r.acted_modes.contains(&"UI".to_string()));
        assert_eq!(r.injects, vec!["INJECT-UI".to_string()]);
        assert!(r.orchestrate.as_ref().unwrap().warranted);
    }

    #[test]
    fn classifier_output_rejects_invalid_effort_and_accepts_canonical_level() {
        let cfg = cfg_with_dims(vec![]);
        let invalid = json!({"routing": {
            "task_class": "BugFix", "complexity": 0.4, "confidence": 0.9, "effort": "turbo"
        }});
        assert!(
            apply_thresholds(&cfg, &invalid, Some("a/m1"), 1, "")
                .routing
                .is_none()
        );

        let valid = json!({"routing": {
            "task_class": "BugFix", "complexity": 0.4, "confidence": 0.9, "effort": "xhigh"
        }});
        assert_eq!(
            apply_thresholds(&cfg, &valid, Some("a/m1"), 1, "")
                .routing
                .unwrap()
                .effort,
            Some(EffortLevel::Xhigh)
        );
    }

    #[test]
    fn thresholds_fail_low_confidence() {
        let cfg = cfg_with_dims(vec![]);
        let parsed = json!({
            "routing": {
                "task_class": "Writing",
                "task_classes": ["Writing"],
                "categories": ["docs"],
                "complexity": 0.12,
                "confidence": 0.9,
                "reason": "bounded prose"
            },
            "orchestrate": {
                "warranted": true,
                "confidence": 0.4,
                "estimated_parts": 3,
                "reason": "maybe"
            }
        });
        let r = apply_thresholds(&cfg, &parsed, Some("a/m1"), 5, "");
        assert!(r.acted_modes.is_empty());
        assert!(r.orchestrate.as_ref().unwrap().warranted);
    }

    #[test]
    fn should_run_once() {
        let pcfg = PreClassifierConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(should_run(&pcfg, false, true));
        assert!(!should_run(&pcfg, true, true));
        assert!(!should_run(&pcfg, false, false));
        let off = PreClassifierConfig::default();
        assert!(!should_run(&off, false, true));
    }

    #[test]
    fn agent_decision_note_suppresses_orchestration_clearly() {
        let cfg = cfg_with_dims(vec![]);
        let parsed = json!({
            "routing": {
                "task_class": "Architecture",
                "task_classes": ["Architecture"],
                "categories": ["backend"],
                "complexity": 0.7,
                "confidence": 0.9,
                "reason": "cross-system design"
            },
            "orchestrate": {
                "warranted": false,
                "confidence": 0.85,
                "estimated_parts": 1,
                "reason": "single-track sequential pipeline"
            }
        });
        let r = apply_thresholds(&cfg, &parsed, Some("claude/haiku"), 100, "");
        let note = agent_decision_note(&cfg, &r);
        assert!(note.contains("AUTHORITATIVE"));
        assert!(note.contains("SUPPRESSED"));
        assert!(note.contains("single-track sequential pipeline"));
        assert!(note.contains("Do NOT"));
        assert!(!note.contains("WILL RUN"));
    }

    #[test]
    fn agent_decision_note_when_orchestrate_acts() {
        let cfg = cfg_with_dims(vec![]);
        let parsed = json!({
            "routing": {
                "task_class": "Feature",
                "task_classes": ["Feature"],
                "categories": ["backend"],
                "complexity": 0.65,
                "confidence": 0.9,
                "reason": "multi-track implementation"
            },
            "orchestrate": {
                "warranted": true,
                "confidence": 0.9,
                "estimated_parts": 3,
                "reason": "multi-track impl"
            }
        });
        let r = apply_thresholds(&cfg, &parsed, Some("a/m1"), 50, "");
        let note = agent_decision_note(&cfg, &r);
        assert!(note.contains("WILL RUN"));
        assert!(note.contains("multi-track impl"));
    }
}
