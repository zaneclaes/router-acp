//! Composable LLM pre-classifier: one cheap ACP evaluation of the user prompt
//! returns structured decisions for built-in and host-registered dimensions.
//!
//! When enabled, this is the authority for auto-orchestration (replacing
//! `tasklist::detect_task_list`) and for host injects such as Kory's
//! `ui_planning` dimension. Fail-open on timeout / parse / no evaluator.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use agent_client_protocol::schema::v1::{
    ContentBlock, McpServer, PromptRequest, SetSessionModeRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::candidate::{CandidateId, TaskClass};
use crate::config::{ActWhen, Config, PreClassifierConfig};
use crate::session::{
    DownstreamRoute, OpenedSession, Shared, close_downstream_session, first_eligible_candidate,
    open_downstream_session, prompt_display_text, resolve_mode_id,
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
    let confidence = obj.get("confidence").and_then(|x| x.as_f64()).unwrap_or(0.0);
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

/// Decide which dimensions act and which inject texts to apply.
pub fn apply_thresholds(
    cfg: &Config,
    parsed: &Value,
    evaluator: Option<&str>,
    latency_ms: u64,
    raw_log: &str,
) -> PreClassResult {
    let mut log = String::new();
    log.push_str(&format!(
        "router-acp · pre-class · evaluator={} · {}ms\n",
        evaluator.unwrap_or("(none)"),
        latency_ms
    ));
    log.push_str(raw_log);
    if !raw_log.ends_with('\n') {
        log.push('\n');
    }

    let mut acted_modes = Vec::new();
    let mut injects = Vec::new();
    let mut dimensions = BTreeMap::new();
    let mut summary_dims = serde_json::Map::new();

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
            summary_dims.insert(
                dim.id.clone(),
                json!({ "present": false, "acts": false }),
            );
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
            dim.id,
            conf,
            acts,
            val
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
/// whole pre-class).
pub async fn evaluate(shared: &Arc<Shared>, router_sid: &str, prompt: &[ContentBlock]) -> PreClassResult {
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

    // Prefer-order walk: first_eligible_candidate ranks by quality within the
    // evaluator glob pool; after a failure we exclude that exact id and retry.
    for _attempt in 0..pcfg.evaluator.len().max(1) + 2 {
        let Some(candidate) =
            first_eligible_candidate(shared, &pcfg.evaluator, class, &excluded)
        else {
            break;
        };
        let cand_str = candidate.to_string();
        // Skip already-tried exact ids even if the glob still matches.
        if attempts.iter().any(|a| a == &cand_str) {
            excluded.push(cand_str);
            continue;
        }

        let result = evaluate_on_candidate(
            shared,
            router_sid,
            &candidate,
            cwd.clone(),
            dirs.clone(),
            &eval_prompt,
            &cfg,
            started,
        )
        .await;
        attempts.push(cand_str.clone());

        if result.ok {
            return result;
        }
        tracing::warn!(
            session = router_sid,
            evaluator = %cand_str,
            reason = result.skip_reason.as_deref().unwrap_or("?"),
            "pre-class evaluator attempt failed; trying next"
        );
        last_fail = Some(result);
        excluded.push(cand_str);
    }

    let latency_ms = started.elapsed().as_millis() as u64;
    if let Some(mut fail) = last_fail {
        fail.latency_ms = latency_ms;
        fail.log.push_str(&format!(
            "tried evaluators: {}\n",
            attempts.join(", ")
        ));
        return fail;
    }
    fail_open(
        "no evaluator candidate available",
        None,
        latency_ms,
        "",
    )
}

/// One evaluator attempt: open (with auto mode) → prompt → close → parse.
async fn evaluate_on_candidate(
    shared: &Arc<Shared>,
    router_sid: &str,
    candidate: &CandidateId,
    cwd: std::path::PathBuf,
    dirs: Vec<std::path::PathBuf>,
    eval_prompt: &str,
    cfg: &Config,
    started: Instant,
) -> PreClassResult {
    let cand_str = candidate.to_string();
    let pcfg = &cfg.pre_classifier;
    let capture = Arc::new(Mutex::new(String::new()));

    // Phase 1 — spawn + session/new + auto mode. NOT on `timeout_ms`.
    let open_timeout_ms = cfg.probe_timeout_ms.clamp(1, OPEN_TIMEOUT_CAP_MS);
    let opened = match tokio::time::timeout(
        Duration::from_millis(open_timeout_ms),
        open_evaluator_session(shared, router_sid, candidate, cwd, dirs, capture.clone()),
    )
    .await
    {
        Err(_) => {
            return fail_open(
                format!("evaluator session open timed out after {open_timeout_ms}ms"),
                Some(cand_str),
                started.elapsed().as_millis() as u64,
                "",
            );
        }
        Ok(Err(err)) => {
            return fail_open(
                format!("evaluator session open failed: {err}"),
                Some(cand_str),
                started.elapsed().as_millis() as u64,
                "",
            );
        }
        Ok(Ok(opened)) => opened,
    };

    // Phase 2 — the LLM generation, which is what `timeout_ms` budgets.
    let open_ms = started.elapsed().as_millis() as u64;
    let request = PromptRequest::new(
        opened.downstream_sid.clone(),
        vec![ContentBlock::from(eval_prompt.to_string())],
    );
    let turn = tokio::time::timeout(
        Duration::from_millis(pcfg.timeout_ms.max(1)),
        opened.conn.send_request(request).block_task(),
    )
    .await;
    close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);

    let latency_ms = started.elapsed().as_millis() as u64;

    match turn {
        Err(_) => fail_open(
            format!(
                "evaluator prompt timed out after {}ms (session open took {open_ms}ms)",
                pcfg.timeout_ms
            ),
            Some(cand_str),
            latency_ms,
            "",
        ),
        Ok(Err(err)) => fail_open(
            format!("evaluator session failed: session/prompt: {err}"),
            Some(cand_str),
            latency_ms,
            "",
        ),
        Ok(Ok(_)) => {
            let raw = capture.lock().unwrap().clone();
            // mock-agent echoes `echo:<model>:<text>` — strip that wrapper when present.
            let body = strip_mock_echo(&raw);
            match parse_evaluator_json(&body) {
                Ok(parsed) => {
                    let mut raw_log = format!("raw reply:\n{body}\n");
                    raw_log.push_str(&format!("parsed: {parsed}\n"));
                    apply_thresholds(cfg, &parsed, Some(&cand_str), latency_ms, &raw_log)
                }
                Err(e) => fail_open(
                    format!("parse failed: {e}"),
                    Some(cand_str),
                    latency_ms,
                    &format!("raw reply:\n{body}\n"),
                ),
            }
        }
    }
}

fn strip_mock_echo(raw: &str) -> String {
    // mock-agent default: `echo:<model>:<text>` possibly multi-line after second colon.
    if let Some(rest) = raw.strip_prefix("echo:") {
        if let Some((_, body)) = rest.split_once(':') {
            return body.to_string();
        }
    }
    raw.to_string()
}

/// Spawn the evaluator process if needed and open a tool-less session on it.
/// Applies the configured `auto` session mode (bypassPermissions for Claude)
/// so the short evaluator turn never blocks on a permission gate — the same
/// contract delegates use. Caller owns closing the returned session.
async fn open_evaluator_session(
    shared: &Arc<Shared>,
    router_sid: &str,
    candidate: &CandidateId,
    cwd: std::path::PathBuf,
    dirs: Vec<std::path::PathBuf>,
    capture: Arc<Mutex<String>>,
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
        DownstreamRoute::Delegate {
            parent_router_sid: router_sid.to_string(),
            capture,
        },
    )
    .await
    .map_err(|e| format!("session/new: {e}"))?;

    // Critical: without auto/bypass the Claude ACP adapter waits forever on
    // permission prompts even for a tool-less classify turn — observed as a
    // clean prompt-budget timeout with empty capture (rtr-e743ca2c…).
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
        match resolve_mode_id(shared, &candidate.agent, "auto", &available_modes) {
            Some(mode_id) => {
                let set =
                    SetSessionModeRequest::new(opened.downstream_sid.clone(), mode_id.clone());
                if let Err(err) = opened.conn.send_request(set).block_task().await {
                    close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);
                    return Err(format!(
                        "set_mode auto ({mode_id}) rejected: {err}; modes={available_modes:?}"
                    ));
                }
                tracing::info!(
                    session = router_sid,
                    candidate = %candidate,
                    applied = %mode_id,
                    "pre-class evaluator session mode applied"
                );
            }
            None => {
                close_downstream_session(shared, &opened.process_key, &opened.downstream_sid);
                return Err(format!(
                    "no auto mode among advertised modes {available_modes:?}"
                ));
            }
        }
    }

    Ok(opened)
}

/// Emit disclosure lines for a pre-class result (start was optional; done is always).
pub fn disclose(shared: &Arc<Shared>, router_sid: &str, result: &PreClassResult) {
    if !shared.cfg.pre_classifier.disclose {
        return;
    }
    // Multi-line log as a single notification block so FE can parse it.
    crate::session::notify_user(shared, router_sid, result.log.trim_end().to_string());
    // Structured meta-friendly one-liner with JSON summary (stable prefix).
    crate::session::notify_user(
        shared,
        router_sid,
        format!(
            "router-acp · pre-class summary · {}",
            result.summary
        ),
    );
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
        assert!(r.acted_modes.contains(&"orchestrate".to_string()));
        assert!(r.acted_modes.contains(&"UI".to_string()));
        assert_eq!(r.injects, vec!["INJECT-UI".to_string()]);
        assert!(r.orchestrate.as_ref().unwrap().warranted);
    }

    #[test]
    fn thresholds_fail_low_confidence() {
        let cfg = cfg_with_dims(vec![]);
        let parsed = json!({
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
}
