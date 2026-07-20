//! Provider usage-cap polling → proactive per-candidate cordons.
//!
//! Some providers expose the subscription's own usage/rate-limit state. When a
//! model's cap is exhausted (and there's no overage/credit headroom), we cordon
//! that candidate *before* the router tries it, rather than only reacting to a
//! rate-limit error after a failed turn.
//!
//! The mechanism is generic: which agents are polled is set per-agent via
//! `usage_source`, and *which models* are exhausted is discovered entirely from
//! the API response (`limits[].scope.model.display_name`), never hardcoded.
//!
//! Networking is a shelled-out `curl` (token passed via a stdin config so it
//! never lands in argv) — no TLS crate, matching the project's `git`/`/bin/sh`
//! precedent. It fails open: any error means "do not cordon" so a usage-endpoint
//! hiccup can never make a model unroutable (the reactive per-agent cordon is
//! the safety net if an exhausted model is then actually hit).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::candidate::CandidateId;
use crate::config::UsageSourceConfig;
use crate::headroom::UsageCordon;
use crate::session::Shared;

const ANTHROPIC_USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";

/// Spawn the periodic usage poller, or `None` when cordoning is disabled or no
/// agent declares a `usage_source`. The task recomputes the whole per-candidate
/// usage-cordon set each cycle and installs it on `shared.headroom`.
pub fn spawn_usage_poller(shared: &Arc<Shared>) -> Option<tokio::task::JoinHandle<()>> {
    if !shared.cfg.cordon.enabled {
        return None;
    }
    if !shared.cfg.agents.iter().any(|a| a.usage_source.is_some()) {
        return None;
    }
    let interval = Duration::from_secs(shared.cfg.cordon.poll_secs.max(30));
    let shared = shared.clone();
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        // `interval` fires immediately, so the first poll happens at startup.
        loop {
            tick.tick().await;
            let cordons = poll_all(&shared).await;
            let n = cordons.len();
            shared.headroom.lock().unwrap().set_usage_cordons(cordons);
            tracing::debug!(cordoned_candidates = n, "usage cordons refreshed");
        }
    }))
}

/// Poll every agent that has a usage source and merge the results. Agents whose
/// poll errors contribute nothing (fail open).
async fn poll_all(shared: &Arc<Shared>) -> HashMap<CandidateId, UsageCordon> {
    let mut out: HashMap<CandidateId, UsageCordon> = HashMap::new();
    for agent in &shared.cfg.agents {
        let Some(source) = &agent.usage_source else {
            continue;
        };
        let candidates = agent_candidates(shared, &agent.name);
        if candidates.is_empty() {
            continue;
        }
        let cordons = match source {
            UsageSourceConfig::AnthropicOauth => match fetch_anthropic_usage().await {
                Ok(payload) => anthropic_cordons(&payload, &candidates, SystemTime::now()),
                Err(err) => {
                    tracing::debug!(agent = %agent.name, %err, "usage poll failed; failing open");
                    continue;
                }
            },
            UsageSourceConfig::CodexRollout => match latest_codex_rate_limits() {
                Some(rl) => codex_cordons(&rl, &candidates, SystemTime::now()),
                None => {
                    tracing::debug!(agent = %agent.name, "no codex rate-limit snapshot; failing open");
                    continue;
                }
            },
        };
        if !cordons.is_empty() {
            tracing::info!(
                agent = %agent.name,
                count = cordons.len(),
                "usage-cordoned candidates (cap exhausted, no overage headroom)"
            );
        }
        out.extend(cordons);
    }
    out
}

/// `(candidate id, display name)` for every candidate of `agent`.
fn agent_candidates(shared: &Arc<Shared>, agent: &str) -> Vec<(CandidateId, String)> {
    shared
        .candidates
        .lock()
        .unwrap()
        .iter()
        .filter(|c| c.id.agent == agent)
        .map(|c| (c.id.clone(), c.display_name.clone()))
        .collect()
}

// ----------------------------------------------------------------------
// Anthropic OAuth usage
// ----------------------------------------------------------------------

async fn fetch_anthropic_usage() -> Result<Value, String> {
    let token = anthropic_oauth_token().ok_or("no Claude OAuth token found")?;
    // Pass URL + headers via a curl config on stdin so the bearer token never
    // appears in argv (visible via `ps`).
    let config = format!(
        "url = \"{ANTHROPIC_USAGE_URL}\"\n\
         header = \"Authorization: Bearer {token}\"\n\
         header = \"anthropic-beta: oauth-2025-04-20\"\n\
         silent\n\
         show-error\n\
         fail\n\
         max-time = 20\n"
    );
    let body = curl_with_config(&config).await?;
    serde_json::from_str(&body).map_err(|e| format!("bad usage JSON: {e}"))
}

async fn curl_with_config(config: &str) -> Result<String, String> {
    use tokio::io::AsyncWriteExt;
    let mut child = tokio::process::Command::new("curl")
        .arg("-K")
        .arg("-")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("cannot spawn curl: {e}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(config.as_bytes())
            .await
            .map_err(|e| format!("curl stdin: {e}"))?;
        // stdin dropped here → EOF so curl proceeds.
    }
    let out = child
        .wait_with_output()
        .await
        .map_err(|e| format!("curl wait: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "curl failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Read the Claude CLI OAuth access token: first `~/.claude/.credentials.json`
/// (Linux), else the macOS Keychain (`Claude Code-credentials`).
fn anthropic_oauth_token() -> Option<String> {
    if let Some(home) = std::env::var_os("HOME") {
        let path = std::path::Path::new(&home).join(".claude/.credentials.json");
        if let Ok(text) = std::fs::read_to_string(&path)
            && let Some(tok) = token_from_credentials_json(&text)
        {
            return Some(tok);
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("security")
            .args([
                "find-generic-password",
                "-s",
                "Claude Code-credentials",
                "-w",
            ])
            .output()
            && out.status.success()
            && let Some(tok) = token_from_credentials_json(&String::from_utf8_lossy(&out.stdout))
        {
            return Some(tok);
        }
    }
    None
}

fn token_from_credentials_json(text: &str) -> Option<String> {
    let v: Value = serde_json::from_str(text.trim()).ok()?;
    v.get("claudeAiOauth")?
        .get("accessToken")?
        .as_str()
        .map(str::to_string)
}

// ----------------------------------------------------------------------
// Cordon computation (pure — the unit-tested core)
// ----------------------------------------------------------------------

/// Given an Anthropic usage payload and an agent's candidates, decide which are
/// cordoned. A cap only bites when the overage/credit pool has no headroom
/// (credits cover you when you hit a plan limit); then a model-scoped weekly cap
/// cordons its matching candidate, and an all-models/session cap cordons every
/// candidate — each until its reported `resets_at`.
pub fn anthropic_cordons(
    payload: &Value,
    candidates: &[(CandidateId, String)],
    now: SystemTime,
) -> HashMap<CandidateId, UsageCordon> {
    let mut out: HashMap<CandidateId, UsageCordon> = HashMap::new();
    // Overage/credits still available → nothing is unroutable.
    if overage_has_headroom(payload) {
        return out;
    }
    let Some(limits) = payload.get("limits").and_then(|l| l.as_array()) else {
        return out;
    };
    for lim in limits {
        let percent = lim.get("percent").and_then(Value::as_f64).unwrap_or(0.0);
        let severity = lim.get("severity").and_then(Value::as_str).unwrap_or("");
        let is_active = lim
            .get("is_active")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let saturated = percent >= 100.0 || (severity == "critical" && is_active);
        if !saturated {
            continue;
        }
        let Some(resets_str) = lim.get("resets_at").and_then(Value::as_str) else {
            continue;
        };
        let Some(delay) = crate::limits::parse_reset_delay_at(&resets_str.to_lowercase(), now)
        else {
            continue;
        };
        let resets_at = now + delay;

        let scope_model = lim.get("scope").and_then(|s| s.get("model")).and_then(|m| {
            m.get("display_name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .or_else(|| m.get("id").and_then(Value::as_str))
        });

        match scope_model {
            // Model-scoped weekly cap: cordon matching candidate(s) only.
            Some(model) if !model.is_empty() => {
                let reason = format!("Weekly {model} limit reached");
                for (id, display) in candidates {
                    if model_matches(model, id, display) {
                        upsert_latest(&mut out, id, &reason, resets_at, resets_str);
                    }
                }
            }
            // All-models weekly or 5-hour session cap: cordon every candidate.
            _ => {
                let reason = match lim.get("kind").and_then(Value::as_str).unwrap_or("") {
                    "session" => "5-hour usage limit reached".to_string(),
                    _ => "Weekly usage limit reached".to_string(),
                };
                for (id, _) in candidates {
                    upsert_latest(&mut out, id, &reason, resets_at, resets_str);
                }
            }
        }
    }
    out
}

/// True when the overage/credit pool can still absorb usage past plan limits.
fn overage_has_headroom(payload: &Value) -> bool {
    if let Some(eu) = payload.get("extra_usage")
        && eu.get("is_enabled").and_then(Value::as_bool) == Some(true)
        && eu
            .get("utilization")
            .and_then(Value::as_f64)
            .unwrap_or(100.0)
            < 100.0
    {
        return true;
    }
    if let Some(sp) = payload.get("spend")
        && sp.get("enabled").and_then(Value::as_bool) == Some(true)
        && sp.get("percent").and_then(Value::as_f64).unwrap_or(100.0) < 100.0
    {
        return true;
    }
    false
}

/// Match a usage-API model name (e.g. "Fable") to a candidate by substring
/// against its model id or display name (case-insensitive) — so "Fable" matches
/// `claude-fable-5[1m]`.
fn model_matches(api_name: &str, id: &CandidateId, display: &str) -> bool {
    let n = api_name.to_lowercase();
    if n.is_empty() {
        return false;
    }
    id.model.to_lowercase().contains(&n) || display.to_lowercase().contains(&n)
}

/// Insert, or replace only if the new cordon resets later — a candidate hit by
/// several caps stays cordoned until the last one clears.
fn upsert_latest(
    map: &mut HashMap<CandidateId, UsageCordon>,
    id: &CandidateId,
    reason: &str,
    resets_at: SystemTime,
    resets_at_rfc3339: &str,
) {
    let replace = map.get(id).is_none_or(|c| resets_at > c.resets_at);
    if replace {
        map.insert(
            id.clone(),
            UsageCordon {
                reason: reason.to_string(),
                resets_at,
                resets_at_rfc3339: resets_at_rfc3339.to_string(),
            },
        );
    }
}

// ----------------------------------------------------------------------
// Codex rollout (on-disk snapshot)
// ----------------------------------------------------------------------

/// The most recent `rate_limits` object Codex wrote to its session rollouts.
/// Codex has no pollable usage endpoint, but it records a rate-limit snapshot
/// (from response headers) into `~/.codex/sessions/**/rollout-*.jsonl` on each
/// turn; we read the newest one. `None` if nothing is found.
fn latest_codex_rate_limits() -> Option<Value> {
    let home = std::env::var_os("HOME")?;
    let root = std::path::Path::new(&home).join(".codex/sessions");
    // Collect rollout files (sessions/YYYY/MM/DD/rollout-*.jsonl), newest first.
    let mut files: Vec<(SystemTime, std::path::PathBuf)> = Vec::new();
    collect_rollouts(&root, 0, &mut files);
    files.sort_by_key(|(mtime, _)| std::cmp::Reverse(*mtime));
    // Use the newest file that actually contains a rate_limits snapshot.
    for (_, path) in files.iter().take(8) {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        for line in text.lines().rev() {
            if !line.contains("rate_limits") {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<Value>(line)
                && let Some(rl) = v
                    .get("payload")
                    .and_then(|p| p.get("rate_limits"))
                    .filter(|rl| !rl.is_null())
            {
                return Some(rl.clone());
            }
        }
    }
    None
}

/// Recursively collect `rollout-*.jsonl` files under `dir` (depth-bounded: the
/// layout is sessions/YYYY/MM/DD/).
fn collect_rollouts(
    dir: &std::path::Path,
    depth: usize,
    out: &mut Vec<(SystemTime, std::path::PathBuf)>,
) {
    if depth > 4 {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            collect_rollouts(&path, depth + 1, out);
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("rollout-") && n.ends_with(".jsonl"))
        {
            let mtime = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            out.push((mtime, path));
        }
    }
}

/// Given a Codex `rate_limits` snapshot and an agent's candidates, decide which
/// are cordoned. Codex caps are account-wide (not model-scoped), so a saturated
/// window cordons every candidate — but only when credits can't cover it, and
/// only while the window's `resets_at` is still in the future.
pub fn codex_cordons(
    rate_limits: &Value,
    candidates: &[(CandidateId, String)],
    now: SystemTime,
) -> HashMap<CandidateId, UsageCordon> {
    let mut out: HashMap<CandidateId, UsageCordon> = HashMap::new();
    // Credits cover plan limits (the overage equivalent).
    let has_credits = rate_limits
        .get("credits")
        .and_then(|c| c.get("has_credits"))
        .and_then(Value::as_bool)
        == Some(true);
    if has_credits {
        return out;
    }
    for key in ["primary", "secondary"] {
        let Some(win) = rate_limits.get(key).filter(|w| !w.is_null()) else {
            continue;
        };
        let used = win
            .get("used_percent")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        if used < 100.0 {
            continue;
        }
        let Some(epoch) = win.get("resets_at").and_then(Value::as_u64) else {
            continue;
        };
        let resets_at = SystemTime::UNIX_EPOCH + Duration::from_secs(epoch);
        if resets_at <= now {
            continue; // already reset
        }
        let window_minutes = win
            .get("window_minutes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let reason = if window_minutes >= 1440 {
            "Codex weekly usage limit reached".to_string()
        } else {
            "Codex 5-hour usage limit reached".to_string()
        };
        let rfc = epoch_to_rfc3339(epoch);
        for (id, _) in candidates {
            upsert_latest(&mut out, id, &reason, resets_at, &rfc);
        }
    }
    out
}

/// Format a Unix epoch (seconds) as a UTC RFC-3339 timestamp, for advertising.
/// Hand-rolled (no chrono dep); inverse of `limits::iso_to_epoch`'s civil math.
fn epoch_to_rfc3339(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3_600, (rem % 3_600) / 60, rem % 60);
    // days_from_civil inverse (Howard Hinnant).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cands() -> Vec<(CandidateId, String)> {
        vec![
            (
                CandidateId::new("claude", "claude-fable-5[1m]"),
                "Claude Fable 5 1M".to_string(),
            ),
            (
                CandidateId::new("claude", "sonnet"),
                "Claude Sonnet".to_string(),
            ),
        ]
    }

    fn fable() -> CandidateId {
        CandidateId::new("claude", "claude-fable-5[1m]")
    }
    fn sonnet() -> CandidateId {
        CandidateId::new("claude", "sonnet")
    }

    // Mirrors the real API: Fable weekly-scoped at 100% critical, overage exhausted.
    fn exhausted_payload() -> Value {
        json!({
            "extra_usage": { "is_enabled": true, "utilization": 100.0 },
            "spend": { "enabled": true, "percent": 100 },
            "limits": [
                { "kind": "session", "percent": 72, "severity": "normal",
                  "resets_at": "2026-07-20T23:09:59+00:00", "scope": null, "is_active": false },
                { "kind": "weekly_all", "percent": 78, "severity": "warning",
                  "resets_at": "2026-07-22T16:59:59+00:00", "scope": null, "is_active": false },
                { "kind": "weekly_scoped", "percent": 100, "severity": "critical",
                  "resets_at": "2026-07-22T16:59:59+00:00",
                  "scope": { "model": { "id": null, "display_name": "Fable" } },
                  "is_active": true }
            ]
        })
    }

    #[test]
    fn scoped_cap_with_no_overage_cordons_only_that_model() {
        let now = SystemTime::now();
        let c = anthropic_cordons(&exhausted_payload(), &cands(), now);
        assert!(c.contains_key(&fable()), "fable cordoned");
        assert!(!c.contains_key(&sonnet()), "sonnet untouched");
        let f = &c[&fable()];
        assert!(f.reason.contains("Fable"));
        assert!(f.resets_at > now);
        assert_eq!(f.resets_at_rfc3339, "2026-07-22T16:59:59+00:00");
    }

    #[test]
    fn overage_headroom_cordons_nothing() {
        let mut p = exhausted_payload();
        // Credits available → the scoped cap is covered, nothing cordoned.
        p["extra_usage"]["utilization"] = json!(40.0);
        p["spend"]["percent"] = json!(40);
        let c = anthropic_cordons(&p, &cands(), SystemTime::now());
        assert!(c.is_empty(), "overage headroom means no cordons: {c:?}");
    }

    #[test]
    fn all_models_cap_cordons_everyone() {
        let now = SystemTime::now();
        let p = json!({
            "extra_usage": { "is_enabled": true, "utilization": 100.0 },
            "spend": { "enabled": true, "percent": 100 },
            "limits": [
                { "kind": "weekly_all", "percent": 100, "severity": "critical",
                  "resets_at": "2026-07-22T16:59:59+00:00", "scope": null, "is_active": true }
            ]
        });
        let c = anthropic_cordons(&p, &cands(), now);
        assert!(c.contains_key(&fable()) && c.contains_key(&sonnet()));
    }

    #[test]
    fn no_saturated_caps_cordons_nothing() {
        let now = SystemTime::now();
        let p = json!({
            "extra_usage": { "is_enabled": true, "utilization": 100.0 },
            "spend": { "enabled": true, "percent": 100 },
            "limits": [
                { "kind": "weekly_all", "percent": 78, "severity": "warning",
                  "resets_at": "2026-07-22T16:59:59+00:00", "scope": null, "is_active": false }
            ]
        });
        assert!(anthropic_cordons(&p, &cands(), now).is_empty());
    }

    // ---- Codex rollout ----

    fn codex_cands() -> Vec<(CandidateId, String)> {
        vec![
            (CandidateId::new("codex", "gpt-5.5"), "GPT-5.5".to_string()),
            (
                CandidateId::new("codex", "gpt-5.6-sol"),
                "GPT Sol".to_string(),
            ),
        ]
    }

    #[test]
    fn codex_saturated_window_no_credits_cordons_all() {
        let now = SystemTime::now();
        let future = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 86_400;
        let rl = json!({
            "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": future },
            "secondary": serde_json::Value::Null,
            "credits": { "has_credits": false }
        });
        let c = codex_cordons(&rl, &codex_cands(), now);
        assert_eq!(c.len(), 2, "both codex candidates cordoned: {c:?}");
        assert!(c.values().all(|v| v.reason.contains("weekly")));
    }

    #[test]
    fn codex_credits_available_cordons_nothing() {
        let now = SystemTime::now();
        let future = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 86_400;
        let rl = json!({
            "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": future },
            "credits": { "has_credits": true }
        });
        assert!(codex_cordons(&rl, &codex_cands(), now).is_empty());
    }

    #[test]
    fn codex_past_reset_cordons_nothing() {
        let now = SystemTime::now();
        let rl = json!({
            "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": 1 },
            "credits": { "has_credits": false }
        });
        assert!(
            codex_cordons(&rl, &codex_cands(), now).is_empty(),
            "past reset ignored"
        );
    }

    #[test]
    fn epoch_to_rfc3339_known_values() {
        assert_eq!(epoch_to_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(epoch_to_rfc3339(1_000_000_000), "2001-09-09T01:46:40Z");
    }
}
