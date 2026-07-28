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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use agent_client_protocol::RequestCancellation;
use agent_client_protocol::schema::v1::{
    ContentBlock, Error as AcpError, McpServer, PromptRequest, SetSessionModeRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::candidate::{CandidateId, TaskClass};
use crate::config::{ActWhen, Config, PreClassifierConfig};
use crate::session::{
    DownstreamRoute, OpenedSession, Shared, close_downstream_session, first_eligible_candidate,
    open_downstream_session, prompt_display_text,
};

/// Marker embedded in the evaluator prompt so mock agents (and logs) can
/// recognize a pre-class turn.
pub const PRECLASS_MARKER: &str = "[router-acp pre-classifier]";

const PROMPT_TRUNCATE: usize = 4000;

/// Upper bound on the evaluator *open* phase (process spawn + `session/new`).
/// A cold ACP open routinely takes seconds, so it gets its own generous budget
/// instead of eating the prompt budget: `min(probe_timeout_ms, this)`.
const OPEN_TIMEOUT_CAP_MS: u64 = 30_000;

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
    /// 0.0 (mechanical/trivial) to 1.0 (long-horizon/high-risk).
    pub complexity: f64,
    pub confidence: f64,
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
         task should score complexity at least 0.70. Do NOT increase complexity merely because a \
         ticket is long, contains many bullets, or supplies detailed context.\n\
         Return:\n\
         \"routing\": { \"task_class\": string, \"task_classes\": [strings], \
         \"categories\": [strings], \"complexity\": 0.0-1.0, \
         \"confidence\": 0.0-1.0, \"reason\": string }\n\n",
    );
    schema_keys.push("routing".into());

    if cfg.orchestration.enabled {
        out.push_str(
            "## Dimension: orchestrate\n\
             Decide whether the router should auto-orchestrate this prompt as a multi-track \
             implementation job (plan → parallel subtasks → review).\n\
             YES only for complex multi-track *implementation* work with distinct sub-tracks \
             that benefit from parallel specialists.\n\
             NO for: enumerated prose, Q&A lists, design discussions, plans/RFCs the user wants \
             discussed (not built yet), single-track tasks, answers to the model's questions, \
             pure research/explanation.\n\
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
    out.push_str(&truncate_chars(user_text, PROMPT_TRUNCATE));
    out
}

fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
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
    Some(RoutingDecision {
        task_class,
        task_classes,
        categories,
        complexity: complexity.clamp(0.0, 1.0),
        confidence: confidence.clamp(0.0, 1.0),
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

    let user_text = prompt_display_text(prompt);
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
    // NO wall-clock timeout on the evaluator's LLM call (a slow model must be
    // allowed to finish); the only interrupts are a real connection failure
    // (→ failover) or the client cancelling the turn. Loop until no eligible
    // model remains (excluded grows each attempt), never a fixed attempt cap.
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

/// One evaluator attempt: open in the explicit safe preclass mode → prompt → close → parse.
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

    // Phase 2 — the classifier LLM generation. NO wall-clock timeout: core
    // infrastructure that must be allowed to run to completion. The only
    // interrupts are the client cancelling the turn (`run_until_cancelled`) or a
    // real connection failure (a dead process EOFs → `Err`, handled as a
    // service failure by the caller).
    let request = PromptRequest::new(
        opened.downstream_sid.clone(),
        vec![ContentBlock::from(eval_prompt.to_string())],
    );
    let turn = cancel
        .run_until_cancelled(opened.conn.send_request(request).block_task())
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
/// The explicit `mode_map.preclass` mapping must name an advertised safe mode;
/// unlike delegates, evaluators never inherit `auto` or a client chat mode.
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
            "set_mode preclass ({mode_id}) rejected: {err}; modes={available_modes:?}"
        ));
    }
    tracing::info!(
        session = router_sid,
        candidate = %candidate,
        applied = %mode_id,
        "pre-class evaluator session mode applied"
    );

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
            disclose: true,
            orchestrate_min_confidence: 0.65,
            dimensions: dims,
        };
        cfg
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
        assert!(p.contains("Dimension: orchestrate"));
        assert!(p.contains("1. do a"));
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
