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
use crate::headroom::{SeatAvailability, UsageCordon};
use crate::session::Shared;

const ANTHROPIC_USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";

/// Extension notification a client may send to share its own (often fresher)
/// view of seat availability — see [`apply_availability_hint`] for the shape.
pub const AVAILABILITY_HINT_METHOD: &str = "router-acp/availability_hint";

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
            refresh_and_install(&shared).await;
        }
    }))
}

/// One poll cycle: recompute the whole per-candidate usage-cordon and
/// availability sets and install them on `shared.headroom`. Shared by the
/// periodic poller tick and the turn-end refresh ([`refresh_after_turn`]).
pub async fn refresh_and_install(shared: &Arc<Shared>) {
    let (cordons, availability) = poll_all(shared).await;
    let n = cordons.len();
    let a = availability.len();
    {
        let mut headroom = shared.headroom.lock().unwrap();
        headroom.set_usage_cordons(cordons);
        headroom.set_polled_availability(availability);
    }
    tracing::debug!(
        cordoned_candidates = n,
        availability_candidates = a,
        "usage cordons refreshed"
    );
}

/// Fire-and-forget usage refresh when a prompt turn completes on an agent
/// that has a usage source — the moment the shared snapshot actually went
/// stale. The cache self-throttles (min-refresh interval + cross-process
/// lock), so this stays at most one upstream read per interval box-wide; and
/// it must never delay or fail the turn itself, so it spawns and swallows
/// every error inside (`poll_all` already fails open).
pub fn refresh_after_turn(shared: &Arc<Shared>, agent: &str) {
    if !shared.cfg.cordon.enabled {
        return;
    }
    let has_source = shared
        .cfg
        .agents
        .iter()
        .any(|a| a.name == agent && a.usage_source.is_some());
    if !has_source {
        return;
    }
    let shared = Arc::clone(shared);
    tokio::spawn(async move {
        refresh_and_install(&shared).await;
    });
}

/// Poll every agent that has a usage source and merge the results: proactive
/// cordons (unroutable) plus graded seat availability (preference scaling).
/// Agents whose poll errors contribute nothing (fail open).
async fn poll_all(
    shared: &Arc<Shared>,
) -> (
    HashMap<CandidateId, UsageCordon>,
    HashMap<CandidateId, SeatAvailability>,
) {
    let mut out: HashMap<CandidateId, UsageCordon> = HashMap::new();
    let mut avail: HashMap<CandidateId, SeatAvailability> = HashMap::new();
    for agent in &shared.cfg.agents {
        let Some(source) = &agent.usage_source else {
            continue;
        };
        let candidates = agent_candidates(shared, &agent.name);
        if candidates.is_empty() {
            continue;
        }
        // The router's own metered spend into a window, for estimating its
        // real dollars left (see `estimate_plan_window_dollars`). Scoped to
        // this agent so one provider's spend never estimates another's
        // window; `models` further scopes to a model-scoped window.
        let spend_lookup = |models: Option<&[String]>, since: SystemTime| -> Option<f64> {
            let since_epoch = since.duration_since(SystemTime::UNIX_EPOCH).ok()?.as_secs() as i64;
            Some(
                shared
                    .state
                    .lock()
                    .unwrap()
                    .llm_cost_since(&agent.name, models, since_epoch),
            )
        };
        let (cordons, availability) = match source {
            UsageSourceConfig::AnthropicOauth => {
                let cached =
                    crate::usage_cache::cached_anthropic_usage(shared.cfg.cordon.min_refresh_secs)
                        .await;
                match cached {
                    Some(payload) => (
                        anthropic_cordons(&payload, &candidates, SystemTime::now()),
                        anthropic_availability_with_spend(
                            &payload,
                            &candidates,
                            Some(&spend_lookup),
                            SystemTime::now(),
                        ),
                    ),
                    None => {
                        tracing::debug!(agent = %agent.name, "no usage snapshot; failing open");
                        continue;
                    }
                }
            }
            UsageSourceConfig::CodexRollout => {
                // Live RPC through the shared cache first; the rollout-file
                // scrape stays as the fallback (an RPC failure or a missing
                // `codex` binary must not lose the passive signal we had).
                let mut snapshots = match crate::usage_cache::cached_codex_usage(
                    shared.cfg.cordon.min_refresh_secs,
                )
                .await
                {
                    Some(payload) => codex_pools_from_payload(&payload),
                    None => Vec::new(),
                };
                if snapshots.is_empty() {
                    snapshots = latest_codex_rate_limits();
                }
                if snapshots.is_empty() {
                    tracing::debug!(agent = %agent.name, "no codex rate-limit snapshot; failing open");
                    continue;
                }
                (
                    codex_cordons(&snapshots, &candidates, SystemTime::now()),
                    codex_availability_with_spend(
                        &snapshots,
                        &candidates,
                        Some(&spend_lookup),
                        SystemTime::now(),
                    ),
                )
            }
        };
        if !cordons.is_empty() {
            tracing::info!(
                agent = %agent.name,
                count = cordons.len(),
                "usage-cordoned candidates (cap exhausted, no overage headroom)"
            );
        }
        out.extend(cordons);
        avail.extend(availability);
    }
    (out, avail)
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

pub(crate) async fn fetch_anthropic_usage(token: &str) -> Result<Value, String> {
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
        // The caller can be aborted mid-fetch (turn-end refresh task, process
        // shutdown); an orphaned curl would outlive the released fetch lock
        // and hit the endpoint concurrently with the next lock holder.
        .kill_on_drop(true)
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

/// The Claude CLI OAuth credentials: the access token authenticates the usage
/// fetch; the refresh token (stable across access-token rotations) is the
/// preferred account-fingerprint input for the shared usage cache.
pub(crate) struct OauthCredentials {
    pub access_token: String,
    pub refresh_token: Option<String>,
}

/// Read the Claude CLI OAuth credentials: first `~/.claude/.credentials.json`
/// (Linux), else the macOS Keychain (`Claude Code-credentials`).
pub(crate) fn anthropic_oauth_credentials() -> Option<OauthCredentials> {
    if let Some(home) = std::env::var_os("HOME") {
        let path = std::path::Path::new(&home).join(".claude/.credentials.json");
        if let Ok(text) = std::fs::read_to_string(&path)
            && let Some(creds) = credentials_from_json(&text)
        {
            return Some(creds);
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
            && let Some(creds) = credentials_from_json(&String::from_utf8_lossy(&out.stdout))
        {
            return Some(creds);
        }
    }
    None
}

fn credentials_from_json(text: &str) -> Option<OauthCredentials> {
    let v: Value = serde_json::from_str(text.trim()).ok()?;
    let oauth = v.get("claudeAiOauth")?;
    Some(OauthCredentials {
        access_token: oauth.get("accessToken")?.as_str()?.to_string(),
        // Node's reader does `refreshToken || accessToken` — an empty string
        // is falsy there, so treat it as absent to keep fingerprints aligned.
        refresh_token: oauth
            .get("refreshToken")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
    })
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
        // Saturation is TRUE exhaustion only (`percent >= 100`). The API's
        // `is_active` merely marks the window currently metering — the running
        // 5-hour window is `is_active` at *any* utilization — and `severity` is
        // advisory color; neither means the cap is reached. Keying on them
        // cordoned healthy seats: a 97%/`is_active` session window with 63%
        // weekly headroom (and overage exhausted, so the early return above
        // didn't fire) read as maxed and locked every candidate out — including
        // Fable, whose binding weekly cap was nowhere near full.
        if percent < 100.0 {
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
// Seat availability (pure — the unit-tested core)
// ----------------------------------------------------------------------

/// One plan window as it feeds availability: how full it is, whether it has
/// hit its cap, which model it covers (`None` = the whole seat), and — when
/// estimable — how many real dollars it has left.
#[derive(Debug, Clone)]
pub struct AvailWindow {
    pub percent: f64,
    pub scope: Option<String>,
    pub saturated: bool,
    /// Estimated real dollars left in this window (see
    /// [`window_remaining_dollars`]), or a client-reported figure on the
    /// hint path. `None` = not estimable; the percent fraction stands in.
    pub remaining_dollars: Option<f64>,
}

/// Below this reported percent the estimate is too noisy to trust: window
/// percents are integer-rounded, so a small numerator makes
/// `spent × (100 − p) / p` swing wildly with ±0.5% of rounding.
const MIN_ESTIMATE_PERCENT: f64 = 15.0;
/// Below this much router-metered spend there isn't enough signal to
/// extrapolate the window's dollar capacity from.
const MIN_ESTIMATE_SPENT_DOLLARS: f64 = 0.50;

/// Estimate the real dollars left in a plan window from the router's own
/// metered spend into it. Neither provider reports plan windows in dollars
/// (only the overage pools carry real amounts), but the router prices every
/// proxied request (`llm_requests.cost_usd`), so `spent / (p/100)` estimates
/// the window's dollar capacity and `spent × (100 − p) / p` what's left.
/// Known skews, both fail-soft: spend by clients outside the router raises
/// `p` without raising `spent`, which UNDER-estimates remaining (the
/// conservative direction); the pricing table vs the provider's internal
/// window weighting can skew either way. Returns `None` below the signal
/// guards — ranking then falls back to the percent fraction.
pub fn window_remaining_dollars(spent: f64, percent: f64) -> Option<f64> {
    if percent < MIN_ESTIMATE_PERCENT || spent < MIN_ESTIMATE_SPENT_DOLLARS {
        return None;
    }
    Some(spent * (100.0 - percent).max(0.0) / percent)
}

/// Router-metered spend lookup: total `cost_usd` for one agent, optionally
/// restricted to specific `"agent/model"` strings (a scoped window), since a
/// given instant. `poll_all` builds the real one over `StateFile::
/// llm_cost_since`; tests that don't care about dollar estimation pass
/// `None` and every plan window falls back to its percent fraction.
pub type SpendLookup<'a> = dyn Fn(Option<&[String]>, SystemTime) -> Option<f64> + 'a;

/// `"agent/model"` strings for the candidates a window's scope covers, for
/// restricting a spend lookup to a scoped window (e.g. Claude Fable's own
/// weekly cap must not count spend on sibling Claude models). `None` scope
/// (the window covers the whole seat) returns `None` — the caller's spend
/// lookup then totals the whole agent.
fn window_scope_models(
    scope: Option<&str>,
    candidates: &[(CandidateId, String)],
) -> Option<Vec<String>> {
    let scope = scope?;
    Some(
        candidates
            .iter()
            .filter(|(id, display)| model_matches(scope, id, display))
            .map(|(id, _)| id.to_string())
            .collect(),
    )
}

/// Dollar estimate for one plan window: resolve its start from `resets_at`
/// and `duration`, total the router's metered spend into it (scoped to the
/// window if it names one), and estimate what's left. `None` at any missing
/// input (no spend lookup, unparseable reset, no known duration) — the
/// window's percent fraction remains the fallback.
fn estimate_plan_window_dollars(
    spend: Option<&SpendLookup>,
    candidates: &[(CandidateId, String)],
    scope: Option<&str>,
    resets_at: SystemTime,
    duration: Duration,
    percent: f64,
) -> Option<f64> {
    let spend = spend?;
    let window_start = resets_at.checked_sub(duration)?;
    let models = window_scope_models(scope, candidates);
    let spent = spend(models.as_deref(), window_start)?;
    window_remaining_dollars(spent, percent)
}

/// Fold plan windows into per-candidate seat availability. A candidate's
/// `plan_headroom` is the minimum free fraction across the windows that cover
/// it; it is `on_overage` when any covering window is saturated AND the
/// overage/credit pool can absorb the excess (without that pool the candidate
/// is cordon territory, not preference territory). `overage_headroom` grades
/// how much of that pool is left — `None` when the pool's size can't be
/// determined (the caller falls back to treating the seat as spent, the
/// pre-grading behavior). Candidates covered by no window get no entry —
/// their static preference applies unscaled.
pub fn availability_from_windows(
    windows: &[AvailWindow],
    overage_available: bool,
    overage_headroom: Option<f64>,
    overage_remaining_dollars: Option<f64>,
    candidates: &[(CandidateId, String)],
    source: &'static str,
) -> HashMap<CandidateId, SeatAvailability> {
    let mut out = HashMap::new();
    for (id, display) in candidates {
        let mut headroom: Option<f64> = None;
        // Binding-window semantics, mirroring the fraction min: the covering
        // window with the fewest dollars left constrains the candidate.
        let mut remaining_dollars: Option<f64> = None;
        let mut saturated = false;
        for w in windows {
            let covers = match &w.scope {
                Some(scope) => model_matches(scope, id, display),
                None => true,
            };
            if !covers {
                continue;
            }
            let free = (1.0 - w.percent / 100.0).clamp(0.0, 1.0);
            headroom = Some(headroom.map_or(free, |h: f64| h.min(free)));
            remaining_dollars = min_option(remaining_dollars, w.remaining_dollars);
            saturated |= w.saturated;
        }
        if let Some(plan_headroom) = headroom {
            out.insert(
                id.clone(),
                SeatAvailability {
                    plan_headroom,
                    plan_remaining_dollars: remaining_dollars,
                    on_overage: saturated && overage_available,
                    overage_headroom,
                    overage_remaining_dollars,
                    source,
                },
            );
        }
    }
    out
}

/// Merge availability maps pessimistically (codex reports several limit
/// pools; the tightest one governs).
fn merge_worst(
    into: &mut HashMap<CandidateId, SeatAvailability>,
    from: HashMap<CandidateId, SeatAvailability>,
) {
    for (id, a) in from {
        into.entry(id)
            .and_modify(|e| {
                e.plan_headroom = e.plan_headroom.min(a.plan_headroom);
                e.plan_remaining_dollars =
                    min_option(e.plan_remaining_dollars, a.plan_remaining_dollars);
                e.on_overage |= a.on_overage;
                e.overage_headroom = min_option(e.overage_headroom, a.overage_headroom);
                e.overage_remaining_dollars =
                    min_option(e.overage_remaining_dollars, a.overage_remaining_dollars);
            })
            .or_insert(a);
    }
}

/// The tighter (smaller) of two optional dollar/fraction figures — `Some`
/// wins over `None` (a pool multiple sources didn't report is not "free").
fn min_option(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, y) => x.or(y),
    }
}

/// A window's nominal duration from its `kind` — `None` (e.g. a
/// model-scoped weekly cap's own `kind` naming, or an unrecognized kind)
/// leaves the window's dollar estimate un-computed; its percent fraction
/// still applies. Anthropic's only documented windows are the rolling
/// 5-hour session and the 7-day weekly (all-models or scoped).
fn anthropic_window_duration(kind: &str) -> Option<Duration> {
    match kind {
        "session" => Some(Duration::from_secs(5 * 3600)),
        "weekly_all" | "weekly_scoped" => Some(Duration::from_secs(7 * 86_400)),
        _ => None,
    }
}

/// Seat availability from an Anthropic usage payload: every reported limit is
/// a window (model-scoped ones cover only their candidate), and overage
/// headroom decides whether a saturated window means "paying" or "cordoned".
/// `spend`, when given, estimates each window's real dollars left from the
/// router's own metered spend (see [`estimate_plan_window_dollars`]); `None`
/// (the shape every existing caller/test uses) leaves dollar estimation off
/// and every window falls back to its percent fraction.
pub fn anthropic_availability(
    payload: &Value,
    candidates: &[(CandidateId, String)],
) -> HashMap<CandidateId, SeatAvailability> {
    anthropic_availability_with_spend(payload, candidates, None, SystemTime::now())
}

pub fn anthropic_availability_with_spend(
    payload: &Value,
    candidates: &[(CandidateId, String)],
    spend: Option<&SpendLookup>,
    now: SystemTime,
) -> HashMap<CandidateId, SeatAvailability> {
    let Some(limits) = payload.get("limits").and_then(|l| l.as_array()) else {
        return HashMap::new();
    };
    let windows: Vec<AvailWindow> = limits
        .iter()
        .filter_map(|lim| {
            let percent = lim.get("percent").and_then(Value::as_f64)?;
            let scope = lim
                .get("scope")
                .and_then(|s| s.get("model"))
                .and_then(|m| {
                    m.get("display_name")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .or_else(|| m.get("id").and_then(Value::as_str))
                })
                .map(str::to_string);
            let remaining_dollars = lim
                .get("kind")
                .and_then(Value::as_str)
                .and_then(anthropic_window_duration)
                .zip(
                    lim.get("resets_at")
                        .and_then(Value::as_str)
                        .and_then(|s| crate::limits::parse_reset_delay_at(&s.to_lowercase(), now))
                        .map(|delay| now + delay),
                )
                .and_then(|(duration, resets_at)| {
                    estimate_plan_window_dollars(
                        spend,
                        candidates,
                        scope.as_deref(),
                        resets_at,
                        duration,
                        percent,
                    )
                });
            Some(AvailWindow {
                percent,
                scope,
                // True exhaustion only — see `anthropic_cordons` on why
                // `is_active`/`severity` must not gate this.
                saturated: percent >= 100.0,
                remaining_dollars,
            })
        })
        .collect();
    availability_from_windows(
        &windows,
        overage_has_headroom(payload),
        anthropic_overage_headroom(payload),
        anthropic_overage_dollars(payload),
        candidates,
        "poll",
    )
}

/// How much of the overage/credit pool is left, in [0, 1] — the magnitude
/// `overage_has_headroom` discards. `extra_usage.utilization` is the paid
/// pool's own percent-used meter (mirrors the free-plan `percent` fields);
/// `spend.percent` is its sibling on the spend-limit path. `None` when
/// neither is reported (a bare `has_credits`-only account has no size to
/// grade — falls back to treating the seat as spent once on overage). This
/// fraction is fallback/disclosure only — `anthropic_overage_dollars` below
/// is what ranking actually compares across seats, since a fraction alone
/// can't tell a $9k cap from a $3k one.
fn anthropic_overage_headroom(payload: &Value) -> Option<f64> {
    if let Some(eu) = payload.get("extra_usage")
        && eu.get("is_enabled").and_then(Value::as_bool) == Some(true)
        && let Some(util) = eu.get("utilization").and_then(Value::as_f64)
    {
        return Some((1.0 - util / 100.0).clamp(0.0, 1.0));
    }
    if let Some(sp) = payload.get("spend")
        && sp.get("enabled").and_then(Value::as_bool) == Some(true)
        && let Some(pct) = sp.get("percent").and_then(Value::as_f64)
    {
        return Some((1.0 - pct / 100.0).clamp(0.0, 1.0));
    }
    None
}

/// Real dollars left in the overage/credit pool. Prefers `spend`
/// (`limit.amount_minor`/`used.amount_minor`, scaled by `exponent` — minor
/// units, e.g. cents when `exponent: 2`) since it's the newer, more complete
/// shape; falls back to the equivalent `extra_usage` fields
/// (`monthly_limit`/`used_credits`, scaled by `decimal_places`). `None` when
/// neither carries a usable cap (a bare `has_credits`-only account has no
/// size to grade — the fraction-only fallback in `seat_budget` applies).
fn anthropic_overage_dollars(payload: &Value) -> Option<f64> {
    if let Some(sp) = payload.get("spend")
        && sp.get("enabled").and_then(Value::as_bool) == Some(true)
        && let Some(limit) = sp.get("limit").and_then(|l| l.get("amount_minor"))
        && let Some(limit) = limit.as_f64()
        && let Some(used) = sp.get("used").and_then(|u| u.get("amount_minor"))
        && let Some(used) = used.as_f64()
    {
        let exponent = sp
            .get("limit")
            .and_then(|l| l.get("exponent"))
            .and_then(Value::as_u64)
            .unwrap_or(2);
        let scale = 10f64.powi(exponent as i32);
        return Some(((limit - used) / scale).max(0.0));
    }
    if let Some(eu) = payload.get("extra_usage")
        && eu.get("is_enabled").and_then(Value::as_bool) == Some(true)
        && let Some(limit) = eu.get("monthly_limit").and_then(Value::as_f64)
        && let Some(used) = eu.get("used_credits").and_then(Value::as_f64)
    {
        let decimal_places = eu
            .get("decimal_places")
            .and_then(Value::as_u64)
            .unwrap_or(2);
        let scale = 10f64.powi(decimal_places as i32);
        return Some(((limit - used) / scale).max(0.0));
    }
    None
}

/// Codex pool objects arrive in two spellings: snake_case from rollout-file
/// scrapes, camelCase from the app-server RPC. Read either; `null` is absent.
fn field<'a>(v: &'a Value, snake: &str, camel: &str) -> Option<&'a Value> {
    v.get(snake)
        .filter(|x| !x.is_null())
        .or_else(|| v.get(camel).filter(|x| !x.is_null()))
}

/// Seat availability from Codex's per-pool rate-limit snapshots. Codex caps
/// are account-wide, so every window covers every candidate; a window whose
/// reset already passed is stale (fresh budget) and contributes nothing.
///
/// The per-member spend limit is the *overage* meter on team plans: it only
/// counts against `plan_headroom` once the team plan window is saturated.
/// Including it while weekly usage is 0% would mark the seat as empty and
/// preference-scale codex away even though the team quota is free.
pub fn codex_availability(
    rate_limits: &[Value],
    candidates: &[(CandidateId, String)],
    now: SystemTime,
) -> HashMap<CandidateId, SeatAvailability> {
    codex_availability_with_spend(rate_limits, candidates, None, now)
}

pub fn codex_availability_with_spend(
    rate_limits: &[Value],
    candidates: &[(CandidateId, String)],
    spend: Option<&SpendLookup>,
    now: SystemTime,
) -> HashMap<CandidateId, SeatAvailability> {
    let mut out = HashMap::new();
    for pool in rate_limits {
        let mut windows = Vec::new();
        for key in ["primary", "secondary"] {
            let Some(win) = pool.get(key).filter(|w| !w.is_null()) else {
                continue;
            };
            let Some(used) = field(win, "used_percent", "usedPercent").and_then(Value::as_f64)
            else {
                continue;
            };
            let resets_epoch = field(win, "resets_at", "resetsAt").and_then(Value::as_u64);
            if let Some(epoch) = resets_epoch
                && SystemTime::UNIX_EPOCH + Duration::from_secs(epoch) <= now
            {
                continue; // already reset
            }
            let remaining_dollars = resets_epoch
                .zip(field(win, "window_minutes", "windowDurationMins").and_then(Value::as_u64))
                .and_then(|(epoch, mins)| {
                    estimate_plan_window_dollars(
                        spend,
                        candidates,
                        None,
                        SystemTime::UNIX_EPOCH + Duration::from_secs(epoch),
                        Duration::from_secs(mins * 60),
                        used,
                    )
                });
            windows.push(AvailWindow {
                percent: used,
                scope: None,
                saturated: used >= 100.0,
                remaining_dollars,
            });
        }
        let plan_headroom = codex_plan_has_headroom(pool, now);
        // Member spend only tightens availability once the plan seat is gone
        // (then it is the overage pool that may still keep the seat alive).
        if !plan_headroom
            && let Some(il) = field(pool, "individual_limit", "individualLimit")
            && let Some(remaining) =
                field(il, "remaining_percent", "remainingPercent").and_then(Value::as_f64)
            && !field(il, "resets_at", "resetsAt")
                .and_then(Value::as_u64)
                .is_some_and(|epoch| SystemTime::UNIX_EPOCH + Duration::from_secs(epoch) <= now)
        {
            // The member limit already reports real dollars (`limit`/`used`)
            // — no estimation needed, unlike the plan windows above.
            let remaining_dollars = il
                .get("limit")
                .and_then(as_dollar_amount)
                .zip(il.get("used").and_then(as_dollar_amount))
                .map(|(limit, used)| (limit - used).max(0.0));
            windows.push(AvailWindow {
                percent: (100.0 - remaining).clamp(0.0, 100.0),
                scope: None,
                saturated: remaining <= 0.0,
                remaining_dollars,
            });
        }
        merge_worst(
            &mut out,
            availability_from_windows(
                &windows,
                codex_overage_usable(pool),
                codex_overage_headroom(pool),
                codex_overage_dollars(pool),
                candidates,
                "poll",
            ),
        );
    }
    out
}

/// How much of Codex's overage pool is left, in [0, 1] — mirrors
/// `codex_overage_usable`'s gates but reports the fraction instead of a
/// bool. The per-member spend limit's `remaining_percent` IS the pool's own
/// meter, so it grades directly; unlimited credits report full headroom.
/// Balance-only credits (no percent, no `unlimited`) have no denominator to
/// grade against, so they report `None` — same fallback as an unmetered pool.
/// Fallback/disclosure only — `codex_overage_dollars` below is what ranking
/// actually compares, since a fraction alone can't distinguish this seat's
/// per-member cap size from another provider's.
fn codex_overage_headroom(rate_limits: &Value) -> Option<f64> {
    if field(rate_limits, "spend_control_reached", "spendControlReached").and_then(Value::as_bool)
        == Some(true)
    {
        return Some(0.0);
    }
    if let Some(il) = field(rate_limits, "individual_limit", "individualLimit")
        && let Some(remaining) =
            field(il, "remaining_percent", "remainingPercent").and_then(Value::as_f64)
    {
        return Some((remaining / 100.0).clamp(0.0, 1.0));
    }
    let credits = rate_limits.get("credits").filter(|c| !c.is_null())?;
    if credits.get("unlimited").and_then(Value::as_bool) == Some(true) {
        return Some(1.0);
    }
    None
}

/// A JSON number or numeric string as an `f64` — Codex's `individual_limit`
/// reports `limit`/`used` as strings (`"3000"`, `"3000.534..."`) while other
/// fields in the same payload are plain numbers; accept either.
fn as_dollar_amount(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Real dollars left in Codex's overage pool. `spend_control_reached` is a
/// hard block regardless of any positive balance elsewhere (see
/// `codex_overage_usable`). `individual_limit.limit`/`used` are the
/// per-member spend cap in real dollars — the primary source. Falls back to
/// a positive credit `balance` when no member limit is reported. Unlimited
/// credits have no dollar ceiling to report (the fraction path handles
/// them); a bare `has_credits` with no balance/limit has no size to grade.
fn codex_overage_dollars(rate_limits: &Value) -> Option<f64> {
    if field(rate_limits, "spend_control_reached", "spendControlReached").and_then(Value::as_bool)
        == Some(true)
    {
        return Some(0.0);
    }
    if let Some(il) = field(rate_limits, "individual_limit", "individualLimit")
        && let Some(limit) = il.get("limit").and_then(as_dollar_amount)
        && let Some(used) = il.get("used").and_then(as_dollar_amount)
    {
        return Some((limit - used).max(0.0));
    }
    let credits = rate_limits.get("credits").filter(|c| !c.is_null())?;
    credits.get("balance").and_then(as_dollar_amount)
}

// ----------------------------------------------------------------------
// Client availability hints
// ----------------------------------------------------------------------

/// Ingest a client `router-acp/availability_hint` extension notification.
/// Clients that watch seat usage themselves (e.g. Kory Code polls both
/// providers every minute) push their view here; a fresh hint outranks the
/// router's own poll for that agent until it expires. Expected params:
///
/// ```json
/// {
///   "ttl_secs": 300,
///   "agents": [
///     { "agent": "claude",
///       "windows": [ { "percent": 72, "scope": null, "active": false },
///                    { "percent": 100, "scope": "Fable", "active": true } ],
///       "overage": { "enabled": true, "percent": 40 } }
///   ]
/// }
/// ```
///
/// Tolerant by design: unknown agents and windowless entries are skipped, and
/// `ttl_secs` falls back to `availability_preference.hint_ttl_secs`. An
/// entry's `windows` express plan-window fullness (`scope` names a
/// model-scoped cap, `active` marks a limit the provider reports as biting);
/// `overage` describes the paid pool that absorbs usage past the cap.
pub fn apply_availability_hint(shared: &Arc<Shared>, params: &Value) {
    if !shared.cfg.availability_preference.enabled {
        return;
    }
    let Some(agents) = params.get("agents").and_then(Value::as_array) else {
        return;
    };
    let ttl = params
        .get("ttl_secs")
        .or_else(|| params.get("ttlSecs"))
        .and_then(Value::as_u64)
        .unwrap_or(shared.cfg.availability_preference.hint_ttl_secs);
    let now = SystemTime::now();
    for entry in agents {
        let Some(agent) = entry.get("agent").and_then(Value::as_str) else {
            continue;
        };
        let candidates = agent_candidates(shared, agent);
        if candidates.is_empty() {
            continue;
        }
        let availability = hint_agent_availability(entry, &candidates);
        tracing::debug!(
            agent,
            candidates = availability.len(),
            ttl_secs = ttl,
            "availability hint applied"
        );
        shared.headroom.lock().unwrap().set_hinted_availability(
            agent,
            availability,
            now + Duration::from_secs(ttl),
        );
    }
}

/// Per-candidate availability from one hint agent entry (pure core of
/// [`apply_availability_hint`]).
pub fn hint_agent_availability(
    entry: &Value,
    candidates: &[(CandidateId, String)],
) -> HashMap<CandidateId, SeatAvailability> {
    let windows: Vec<AvailWindow> = entry
        .get("windows")
        .and_then(Value::as_array)
        .map(|ws| {
            ws.iter()
                .filter_map(|w| {
                    let percent = w.get("percent").and_then(Value::as_f64)?;
                    let active = w.get("active").and_then(Value::as_bool);
                    // Explicit active:false on a full meter is overage accounting
                    // (e.g. Codex member spend maxed while the team weekly window
                    // still has quota). Including it would zero plan_headroom
                    // and preference-scale a usable seat away. Absent `active`
                    // still treats percent>=100 as saturated (fail closed).
                    if percent >= 100.0 && active == Some(false) {
                        return None;
                    }
                    let active = active.unwrap_or(false);
                    let scope = w
                        .get("scope")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string);
                    let remaining_dollars = w.get("remaining_dollars").and_then(Value::as_f64);
                    Some(AvailWindow {
                        percent,
                        scope,
                        saturated: percent >= 100.0 || active,
                        remaining_dollars,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let overage_entry = entry
        .get("overage")
        .filter(|o| o.get("enabled").and_then(Value::as_bool) == Some(true));
    let overage_percent = overage_entry
        .and_then(|o| o.get("percent"))
        .and_then(Value::as_f64)
        .unwrap_or(100.0);
    let overage_available = overage_entry.is_some() && overage_percent < 100.0;
    let overage_headroom = overage_entry.map(|_| (1.0 - overage_percent / 100.0).clamp(0.0, 1.0));
    // A client that already knows real dollars (e.g. Kory Code's own
    // provider polling) can report them directly instead of forcing a
    // percent-of-cap round trip; percent stays the required fallback shape.
    let overage_remaining_dollars = overage_entry
        .and_then(|o| o.get("remaining_dollars"))
        .and_then(Value::as_f64);
    availability_from_windows(
        &windows,
        overage_available,
        overage_headroom,
        overage_remaining_dollars,
        candidates,
        "hint",
    )
}

// ----------------------------------------------------------------------
// Codex usage (app-server RPC, rollout files as fallback)
// ----------------------------------------------------------------------

/// Account fingerprint for the Codex seat, byte-matching the Kory Code
/// relay's recipe: sha256 of `tokens.account_id` (stable across token
/// refreshes), else `tokens.refresh_token`, else `OPENAI_API_KEY`, else the
/// raw `auth.json` text. `None` when signed out (no readable auth.json).
pub(crate) fn codex_account_fingerprint() -> Option<String> {
    let home = std::env::var_os("HOME")?;
    let raw = std::fs::read_to_string(std::path::Path::new(&home).join(".codex/auth.json")).ok()?;
    let v: Value = serde_json::from_str(&raw).ok()?;
    let pick = |v: &Value, path: &[&str]| -> Option<String> {
        let mut cur = v.clone();
        for p in path {
            cur = cur.get(p)?.clone();
        }
        cur.as_str().filter(|s| !s.is_empty()).map(str::to_string)
    };
    let key = pick(&v, &["tokens", "account_id"])
        .or_else(|| pick(&v, &["tokens", "refresh_token"]))
        .or_else(|| pick(&v, &["OPENAI_API_KEY"]))
        .unwrap_or(raw);
    Some(crate::usage_cache::fingerprint(&key))
}

/// Live Codex usage: one JSON-RPC round-trip over `codex app-server` stdio
/// (newline-delimited) — initialize, then `account/rateLimits/read`;
/// notifications are skipped. Returns the raw camelCase result
/// (`{rateLimits, rateLimitsByLimitId, rateLimitResetCredits}`). The child is
/// killed when the answer (or the 20s deadline) lands.
pub(crate) async fn fetch_codex_usage() -> Result<Value, String> {
    tokio::time::timeout(Duration::from_secs(20), codex_rate_limits_rpc())
        .await
        .map_err(|_| "codex app-server timed out".to_string())?
}

async fn codex_rate_limits_rpc() -> Result<Value, String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let mut child = tokio::process::Command::new("codex")
        .arg("app-server")
        // The caller can be aborted mid-RPC (timeout, turn-end refresh task,
        // shutdown); an orphaned app-server would linger forever.
        .kill_on_drop(true)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| format!("cannot spawn codex: {e}"))?;
    let mut stdin = child.stdin.take().ok_or("no codex stdin")?;
    let stdout = child.stdout.take().ok_or("no codex stdout")?;
    let init = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "clientInfo": {
            "name": "router-acp", "title": "router-acp",
            "version": env!("CARGO_PKG_VERSION"),
        } },
    });
    stdin
        .write_all(format!("{init}\n").as_bytes())
        .await
        .map_err(|e| format!("codex stdin: {e}"))?;
    let mut lines = BufReader::new(stdout).lines();
    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|e| format!("codex stdout: {e}"))?
    {
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        match msg.get("id").and_then(Value::as_u64) {
            Some(1) => {
                if let Some(err) = msg.get("error") {
                    return Err(format!("codex initialize failed: {err}"));
                }
                let req = serde_json::json!({
                    "jsonrpc": "2.0", "id": 2,
                    "method": "account/rateLimits/read", "params": {},
                });
                stdin
                    .write_all(format!("{req}\n").as_bytes())
                    .await
                    .map_err(|e| format!("codex stdin: {e}"))?;
            }
            Some(2) => {
                if let Some(err) = msg.get("error") {
                    return Err(format!("codex rate-limit read failed: {err}"));
                }
                return msg
                    .get("result")
                    .cloned()
                    .ok_or_else(|| "codex rate-limit read returned no result".to_string());
            }
            _ => {}
        }
    }
    Err("codex app-server exited early".to_string())
}

/// The per-pool rate-limit objects inside an `account/rateLimits/read`
/// payload: every pool in `rateLimitsByLimitId`, or the top-level
/// `rateLimits` for older shapes that lack the per-pool map. Pool objects
/// pass through raw (camelCase) — the cordon/availability readers accept
/// both spellings via [`field`].
pub fn codex_pools_from_payload(payload: &Value) -> Vec<Value> {
    let mut pools: Vec<Value> = payload
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
        .map(|m| m.values().filter(|v| !v.is_null()).cloned().collect())
        .unwrap_or_default();
    if pools.is_empty()
        && let Some(rl) = payload.get("rateLimits").filter(|v| !v.is_null())
    {
        pools.push(rl.clone());
    }
    pools
}

/// The most recent `rate_limits` snapshot PER LIMIT POOL that Codex wrote to
/// its session rollouts. Codex has no pollable usage endpoint, but it records
/// rate-limit snapshots (from response headers) into
/// `~/.codex/sessions/**/rollout-*.jsonl` on each turn — one per limit pool,
/// tagged `limit_id` ("codex", "premium", …). The pools must be kept separate:
/// the newest line overall is often a pool with `primary: null` (no window
/// data), which used to mask an exhausted sibling pool and let the router keep
/// routing to a dead seat (observed live 2026-07-21: "premium" snapshots hid
/// the "codex" pool sitting at 100% for the week). Empty if nothing is found.
fn latest_codex_rate_limits() -> Vec<Value> {
    let Some(home) = std::env::var_os("HOME") else {
        return Vec::new();
    };
    let root = std::path::Path::new(&home).join(".codex/sessions");
    // Collect rollout files (sessions/YYYY/MM/DD/rollout-*.jsonl), newest first.
    let mut files: Vec<(SystemTime, std::path::PathBuf)> = Vec::new();
    collect_rollouts(&root, 0, &mut files);
    files.sort_by_key(|(mtime, _)| std::cmp::Reverse(*mtime));
    // Files newest-first, lines within each scanned newest-first — so the
    // first snapshot seen for a pool is its most recent.
    let mut pools: HashMap<String, Value> = HashMap::new();
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
                let pool = rl
                    .get("limit_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                pools.entry(pool).or_insert_with(|| rl.clone());
            }
        }
    }
    pools.into_values().collect()
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

/// True when the team's plan windows (primary/secondary) still have usable
/// quota right now. A window whose `resets_at` has passed is already free.
/// No reported window ⇒ treat as having headroom (fail open).
fn codex_plan_has_headroom(pool: &Value, now: SystemTime) -> bool {
    for key in ["primary", "secondary"] {
        let Some(win) = pool.get(key).filter(|w| !w.is_null()) else {
            continue;
        };
        let used = field(win, "used_percent", "usedPercent")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        if let Some(epoch) = field(win, "resets_at", "resetsAt").and_then(Value::as_u64) {
            let resets_at = SystemTime::UNIX_EPOCH + Duration::from_secs(epoch);
            if resets_at <= now {
                continue; // already reset → free budget
            }
        }
        if used >= 100.0 {
            return false;
        }
    }
    // No live windows, or every live window is under 100%.
    true
}

/// Given the newest Codex `rate_limits` snapshot of each limit pool and an
/// agent's candidates, decide which are cordoned. Codex caps are account-wide
/// (not model-scoped), so any pool's saturated plan window cordons every
/// candidate — but only when credits / member overage can't cover it, and only
/// while the window's `resets_at` is still in the future.
///
/// Per-member spend limit (`individual_limit`) is the *overage* pool on team
/// plans, not a second independent hard block when the team seat still has
/// quota. Observed 2026-07-25: weekly `usedPercent: 0` (reset) with member
/// `remainingPercent: 0` / `spendControlReached: true` and
/// `rateLimitReachedType: null` — the seat is usable; cordoning every codex
/// candidate left Kory showing the whole agent unavailable. Member limit only
/// hard-cordons when the plan window is ALSO exhausted (so when the weekly
/// resets first, the member cordon still holds until its own reset — the
/// 2026-07-22 case).
pub fn codex_cordons(
    rate_limits: &[Value],
    candidates: &[(CandidateId, String)],
    now: SystemTime,
) -> HashMap<CandidateId, UsageCordon> {
    let mut out: HashMap<CandidateId, UsageCordon> = HashMap::new();
    for pool in rate_limits {
        let plan_headroom = codex_plan_has_headroom(pool, now);
        // Member limit: hard-block only once the team plan seat is also gone.
        if !plan_headroom
            && let Some(il) = field(pool, "individual_limit", "individualLimit")
            && field(il, "remaining_percent", "remainingPercent")
                .and_then(Value::as_f64)
                .is_some_and(|r| r <= 0.0)
            && let Some(epoch) = field(il, "resets_at", "resetsAt").and_then(Value::as_u64)
        {
            let resets_at = SystemTime::UNIX_EPOCH + Duration::from_secs(epoch);
            if resets_at > now {
                let rfc = epoch_to_rfc3339(epoch);
                for (id, _) in candidates {
                    upsert_latest(
                        &mut out,
                        id,
                        "Codex member usage limit reached",
                        resets_at,
                        &rfc,
                    );
                }
            }
        }
        if codex_overage_usable(pool) {
            continue;
        }
        for key in ["primary", "secondary"] {
            let Some(win) = pool.get(key).filter(|w| !w.is_null()) else {
                continue;
            };
            let used = field(win, "used_percent", "usedPercent")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            if used < 100.0 {
                continue;
            }
            let Some(epoch) = field(win, "resets_at", "resetsAt").and_then(Value::as_u64) else {
                continue;
            };
            let resets_at = SystemTime::UNIX_EPOCH + Duration::from_secs(epoch);
            if resets_at <= now {
                continue; // already reset
            }
            let window_minutes = field(win, "window_minutes", "windowDurationMins")
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
    }
    out
}

/// True when the snapshot's overage pool can actually absorb usage past the
/// plan window. A reached spend control is a per-member hard block that no
/// pool covers. On team plans the per-member spend limit (`individual_limit`)
/// IS the overage pool: positive headroom there keeps the seat working past a
/// saturated plan window — observed live 2026-07-23: weekly window at 100%
/// with `spendControlReached: false` and 66% of the member limit remaining,
/// seat confirmed serving requests; without this the router cordoned a usable
/// seat for days. Absent a member limit, credits decide — `unlimited`, or a
/// positive `balance`. A bare `has_credits: true` is NOT enough: team-plan
/// snapshots report `has_credits: true, balance: null` while the seat is
/// hard-blocked at 100% (observed live 2026-07-21 — Sol turns stalled for
/// four consecutive conversations while this gate failed open on
/// `has_credits`).
fn codex_overage_usable(rate_limits: &Value) -> bool {
    if field(rate_limits, "spend_control_reached", "spendControlReached").and_then(Value::as_bool)
        == Some(true)
    {
        return false;
    }
    if let Some(il) = field(rate_limits, "individual_limit", "individualLimit")
        && let Some(remaining) =
            field(il, "remaining_percent", "remainingPercent").and_then(Value::as_f64)
    {
        return remaining > 0.0;
    }
    let Some(credits) = rate_limits.get("credits").filter(|c| !c.is_null()) else {
        return false;
    };
    if credits.get("unlimited").and_then(Value::as_bool) == Some(true) {
        return true;
    }
    field(credits, "has_credits", "hasCredits").and_then(Value::as_bool) == Some(true)
        && credits
            .get("balance")
            .and_then(Value::as_f64)
            .is_some_and(|b| b > 0.0)
}

/// Format a Unix epoch (seconds) as a UTC RFC-3339 timestamp, for advertising.
/// Hand-rolled (no chrono dep); inverse of `limits::iso_to_epoch`'s civil math.
pub(crate) fn epoch_to_rfc3339(secs: u64) -> String {
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

    #[test]
    fn empty_refresh_token_fingerprints_like_missing() {
        let with_empty = credentials_from_json(
            r#"{"claudeAiOauth": {"accessToken": "acc-tok", "refreshToken": ""}}"#,
        )
        .unwrap();
        let without =
            credentials_from_json(r#"{"claudeAiOauth": {"accessToken": "acc-tok"}}"#).unwrap();
        assert!(
            with_empty.refresh_token.is_none(),
            "empty string is falsy in Node's refreshToken || accessToken"
        );
        let fp = |c: &OauthCredentials| {
            crate::usage_cache::fingerprint(c.refresh_token.as_deref().unwrap_or(&c.access_token))
        };
        assert_eq!(fp(&with_empty), fp(&without));
        // A real refresh token still wins over the access token.
        let real = credentials_from_json(
            r#"{"claudeAiOauth": {"accessToken": "acc-tok", "refreshToken": "ref-tok"}}"#,
        )
        .unwrap();
        assert_eq!(real.refresh_token.as_deref(), Some("ref-tok"));
        assert_ne!(fp(&real), fp(&without));
    }

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

    // The reported outage: session window at 97% is `is_active` (it's the
    // window currently metering) and severity `critical`, weekly at 63%, and
    // overage exhausted (so the early return does NOT fire). No cap is actually
    // reached — nothing may be cordoned, and both candidates keep full plan
    // headroom for preference scaling. Before the fix `severity=="critical" &&
    // is_active` marked the 97% session saturated and, being all-models scope,
    // locked out every candidate including Fable.
    fn near_cap_active_payload() -> Value {
        json!({
            "extra_usage": { "is_enabled": true, "utilization": 100.0 },
            "spend": { "enabled": true, "percent": 100 },
            "limits": [
                { "kind": "session", "percent": 97, "severity": "critical",
                  "resets_at": "2026-07-22T16:59:59+00:00", "scope": null, "is_active": true },
                { "kind": "weekly_all", "percent": 63, "severity": "normal",
                  "resets_at": "2026-07-22T16:59:59+00:00", "scope": null, "is_active": false }
            ]
        })
    }

    #[test]
    fn active_window_below_cap_never_cordons() {
        let now = SystemTime::now();
        let c = anthropic_cordons(&near_cap_active_payload(), &cands(), now);
        assert!(
            c.is_empty(),
            "an active/critical window below 100% is not exhaustion: {c:?}"
        );
    }

    #[test]
    fn active_window_below_cap_keeps_plan_headroom() {
        let a = anthropic_availability(&near_cap_active_payload(), &cands());
        // Binding window is the 97% session (min free across windows = 3%).
        let f = a.get(&fable()).expect("fable has availability");
        assert!((f.plan_headroom - 0.03).abs() < 1e-6, "{f:?}");
        assert!(!f.on_overage, "not saturated → not flagged as paying");
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

    fn future_epoch(now: SystemTime) -> u64 {
        now.duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 86_400
    }

    #[test]
    fn codex_saturated_window_no_credits_cordons_all() {
        let now = SystemTime::now();
        let rl = vec![json!({
            "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": future_epoch(now) },
            "secondary": serde_json::Value::Null,
            "credits": { "has_credits": false }
        })];
        let c = codex_cordons(&rl, &codex_cands(), now);
        assert_eq!(c.len(), 2, "both codex candidates cordoned: {c:?}");
        assert!(c.values().all(|v| v.reason.contains("weekly")));
    }

    #[test]
    fn codex_usable_credits_cordon_nothing() {
        let now = SystemTime::now();
        for credits in [
            json!({ "has_credits": true, "unlimited": true, "balance": serde_json::Value::Null }),
            json!({ "has_credits": true, "unlimited": false, "balance": 12.5 }),
        ] {
            let rl = vec![json!({
                "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": future_epoch(now) },
                "credits": credits
            })];
            assert!(codex_cordons(&rl, &codex_cands(), now).is_empty());
        }
    }

    // The live 2026-07-21 shape: team plan, weekly window at 100%,
    // `has_credits: true` but no usable balance — the seat was hard-blocked,
    // so a bare `has_credits` must NOT fail open.
    #[test]
    fn codex_has_credits_without_balance_still_cordons() {
        let now = SystemTime::now();
        let rl = vec![json!({
            "limit_id": "codex",
            "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": future_epoch(now) },
            "secondary": serde_json::Value::Null,
            "credits": { "has_credits": true, "unlimited": false, "balance": serde_json::Value::Null },
            "plan_type": "team"
        })];
        let c = codex_cordons(&rl, &codex_cands(), now);
        assert_eq!(c.len(), 2, "both codex candidates cordoned: {c:?}");
    }

    // The other half of the live failure: the newest snapshot belongs to a
    // different limit pool ("premium") with no window data at all. The
    // exhausted "codex" pool must still cordon — pools are merged, not
    // shadowed by whichever was written last.
    #[test]
    fn codex_windowless_pool_does_not_mask_exhausted_pool() {
        let now = SystemTime::now();
        let rl = vec![
            json!({
                "limit_id": "premium",
                "primary": serde_json::Value::Null,
                "secondary": serde_json::Value::Null,
                "credits": { "has_credits": true, "unlimited": false, "balance": serde_json::Value::Null }
            }),
            json!({
                "limit_id": "codex",
                "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": future_epoch(now) },
                "secondary": serde_json::Value::Null,
                "credits": { "has_credits": true, "unlimited": false, "balance": serde_json::Value::Null }
            }),
        ];
        let c = codex_cordons(&rl, &codex_cands(), now);
        assert_eq!(
            c.len(),
            2,
            "exhausted pool cordons despite windowless sibling: {c:?}"
        );
    }

    // The live 2026-07-22 RPC shape (camelCase): weekly window saturated AND
    // the per-member spend limit exhausted — but the member limit resets DAYS
    // AFTER the weekly window. The cordon must outlast the weekly reset.
    fn live_rpc_payload(now: SystemTime) -> Value {
        let week = future_epoch(now); // +1 day
        let member = future_epoch(now) + 4 * 86_400; // +5 days
        json!({
            "rateLimits": {
                "limitId": "codex",
                "primary": { "usedPercent": 100.0, "windowDurationMins": 10080, "resetsAt": week },
                "secondary": serde_json::Value::Null,
                "credits": { "hasCredits": true, "unlimited": false, "balance": serde_json::Value::Null },
                "individualLimit": { "limit": "1000", "used": "1021.6", "remainingPercent": 0, "resetsAt": member },
                "spendControlReached": true,
                "planType": "team",
                "rateLimitReachedType": "workspace_member_usage_limit_reached"
            },
            "rateLimitsByLimitId": {
                "codex": {
                    "limitId": "codex",
                    "primary": { "usedPercent": 100.0, "windowDurationMins": 10080, "resetsAt": week },
                    "secondary": serde_json::Value::Null,
                    "credits": { "hasCredits": true, "unlimited": false, "balance": serde_json::Value::Null },
                    "individualLimit": { "limit": "1000", "used": "1021.6", "remainingPercent": 0, "resetsAt": member },
                    "spendControlReached": true,
                    "planType": "team"
                }
            },
            "rateLimitResetCredits": { "availableCount": 3 }
        })
    }

    #[test]
    fn codex_pools_prefer_by_limit_id_and_fall_back_to_top_level() {
        let now = SystemTime::now();
        let payload = live_rpc_payload(now);
        let pools = codex_pools_from_payload(&payload);
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0]["limitId"], "codex");

        // Older shape: no per-pool map → the top-level object is the pool.
        let top_only = json!({ "rateLimits": { "limitId": "codex", "primary": null } });
        assert_eq!(codex_pools_from_payload(&top_only).len(), 1);
        assert!(codex_pools_from_payload(&json!({})).is_empty());
    }

    #[test]
    fn codex_camelcase_rpc_windows_cordon_like_snake_case() {
        let now = SystemTime::now();
        let pools = codex_pools_from_payload(&live_rpc_payload(now));
        let c = codex_cordons(&pools, &codex_cands(), now);
        assert_eq!(c.len(), 2, "both candidates cordoned: {c:?}");
        // The member limit resets later than the weekly window and must win
        // the upsert — otherwise routing resumes into a still-dead seat when
        // the weekly window clears.
        for v in c.values() {
            assert_eq!(v.reason, "Codex member usage limit reached");
            assert!(v.resets_at > now + Duration::from_secs(4 * 86_400));
        }
    }

    #[test]
    fn codex_member_limit_does_not_cordon_when_plan_has_headroom() {
        // Live 2026-07-25: weekly reset to 0%, member spend still maxed /
        // spendControlReached — rateLimitReachedType null, seat is usable.
        let now = SystemTime::now();
        let rl = vec![json!({
            "limitId": "codex",
            "primary": { "usedPercent": 0.0, "windowDurationMins": 10080, "resetsAt": future_epoch(now) },
            "credits": { "hasCredits": true, "unlimited": false, "balance": serde_json::Value::Null },
            "individualLimit": { "limit": "3000", "used": "3000.58", "remainingPercent": 0.0, "resetsAt": future_epoch(now) },
            "spendControlReached": true,
            "planType": "team",
            "rateLimitReachedType": serde_json::Value::Null
        })];
        let c = codex_cordons(&rl, &codex_cands(), now);
        assert!(
            c.is_empty(),
            "team seat free ⇒ member spend alone must not cordon: {c:?}"
        );
        let a = codex_availability(&rl, &codex_cands(), now);
        assert!(
            a.values().all(|s| s.plan_headroom >= 0.99),
            "availability must track the free weekly window, not maxed member spend: {a:?}"
        );
    }

    #[test]
    fn codex_member_limit_cordons_when_plan_also_exhausted() {
        // Weekly at 100% AND member spent out — hard block; member reset may
        // outlast weekly so keep the member reason when it wins upsert.
        let now = SystemTime::now();
        let week = future_epoch(now);
        let member = future_epoch(now) + 4 * 86_400;
        let rl = vec![json!({
            "limitId": "codex",
            "primary": { "usedPercent": 100.0, "windowDurationMins": 10080, "resetsAt": week },
            "credits": { "hasCredits": true, "unlimited": false, "balance": serde_json::Value::Null },
            "individualLimit": { "remainingPercent": 0.0, "resetsAt": member },
            "spendControlReached": true
        })];
        let c = codex_cordons(&rl, &codex_cands(), now);
        assert_eq!(c.len(), 2, "plan+member exhausted must cordon: {c:?}");
        assert!(c.values().all(|v| {
            v.reason.contains("member") || v.reason.contains("weekly")
        }));
    }

    #[test]
    fn codex_spend_control_reached_blocks_credit_headroom() {
        let now = SystemTime::now();
        let rl = vec![json!({
            "primary": { "usedPercent": 100.0, "windowDurationMins": 10080, "resetsAt": future_epoch(now) },
            "credits": { "hasCredits": true, "unlimited": false, "balance": 25.0 },
            "spendControlReached": true
        })];
        let c = codex_cordons(&rl, &codex_cands(), now);
        assert_eq!(c.len(), 2, "positive balance can't cover a spend control");
    }

    // The live 2026-07-23 shape: team plan, weekly window at 100%, no usable
    // credit balance — but the per-member spend limit has 66% headroom and no
    // spend control reached. The member limit is the overage pool: the seat
    // keeps serving, so nothing is cordoned and availability shows overage.
    #[test]
    fn codex_member_limit_headroom_overrides_saturated_weekly() {
        let now = SystemTime::now();
        let rl = vec![json!({
            "limitId": "codex",
            "primary": { "usedPercent": 100.0, "windowDurationMins": 10080, "resetsAt": future_epoch(now) },
            "secondary": serde_json::Value::Null,
            "credits": { "hasCredits": true, "unlimited": false, "balance": serde_json::Value::Null },
            "individualLimit": { "limit": "3000", "used": "1028.08", "remainingPercent": 66.0, "resetsAt": future_epoch(now) },
            "spendControlReached": false,
            "planType": "team",
            "rateLimitReachedType": serde_json::Value::Null
        })];
        assert!(
            codex_cordons(&rl, &codex_cands(), now).is_empty(),
            "member headroom absorbs the saturated weekly window"
        );
        let a = codex_availability(&rl, &codex_cands(), now);
        assert_eq!(a.len(), 2);
        assert!(
            a.values()
                .all(|v| v.on_overage && v.plan_headroom.abs() < 1e-9),
            "saturated weekly window spends into the member limit: {a:?}"
        );
    }

    #[test]
    fn codex_member_limit_exhausted_without_spend_control_still_cordons() {
        let now = SystemTime::now();
        let rl = vec![json!({
            "limitId": "codex",
            "primary": { "usedPercent": 100.0, "windowDurationMins": 10080, "resetsAt": future_epoch(now) },
            "credits": { "hasCredits": true, "unlimited": false, "balance": serde_json::Value::Null },
            "individualLimit": { "remainingPercent": 0.0, "resetsAt": future_epoch(now) + 4 * 86_400 },
            "spendControlReached": false,
            "planType": "team"
        })];
        let c = codex_cordons(&rl, &codex_cands(), now);
        assert_eq!(c.len(), 2, "exhausted member limit cordons: {c:?}");
        assert!(c.values().all(|v| v.reason.contains("member")));
    }

    #[test]
    fn codex_availability_ignores_member_while_plan_has_headroom() {
        // Member remaining is the overage meter — it must not shrink
        // plan_headroom while the weekly/session window still has quota.
        let now = SystemTime::now();
        let rl = vec![json!({
            "limitId": "codex",
            "primary": { "usedPercent": 20.0, "windowDurationMins": 10080, "resetsAt": future_epoch(now) },
            "credits": { "hasCredits": false },
            "individualLimit": { "remainingPercent": 25.0, "resetsAt": future_epoch(now) }
        })];
        let a = codex_availability(&rl, &codex_cands(), now);
        assert!(
            a.values().all(|v| (v.plan_headroom - 0.80).abs() < 1e-9),
            "plan window alone sets headroom while free: {a:?}"
        );
    }

    #[test]
    fn codex_availability_uses_member_when_plan_saturated() {
        // Plan at 100% with member remaining → seat is on overage (usable);
        // plan_headroom stays 0 because the plan window is saturated.
        let now = SystemTime::now();
        let rl = vec![json!({
            "limitId": "codex",
            "primary": { "usedPercent": 100.0, "windowDurationMins": 10080, "resetsAt": future_epoch(now) },
            "credits": { "hasCredits": false },
            "individualLimit": { "remainingPercent": 25.0, "resetsAt": future_epoch(now) },
            "spendControlReached": false
        })];
        let a = codex_availability(&rl, &codex_cands(), now);
        assert!(
            a.values()
                .all(|v| v.on_overage && v.plan_headroom.abs() < 1e-9),
            "member remaining keeps on_overage once plan is gone: {a:?}"
        );
    }

    #[test]
    fn codex_past_reset_cordons_nothing() {
        let now = SystemTime::now();
        let rl = vec![json!({
            "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": 1 },
            "credits": { "has_credits": false }
        })];
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

    // ---- Dollar-normalized headroom ----

    #[test]
    fn window_remaining_dollars_estimates_from_spend_and_percent() {
        // $10 spent at 20% used → $40 window capacity, $40 left (5x spent).
        assert!((window_remaining_dollars(10.0, 20.0).unwrap() - 40.0).abs() < 1e-9);
        // Fully spent: nothing left.
        assert_eq!(window_remaining_dollars(10.0, 100.0), Some(0.0));
    }

    #[test]
    fn window_remaining_dollars_guards_low_signal() {
        // Below the percent guard: too little of the window elapsed to
        // extrapolate reliably (integer rounding noise dominates).
        assert_eq!(window_remaining_dollars(10.0, 14.9), None);
        assert!(window_remaining_dollars(10.0, 15.0).is_some());
        // Below the spend guard: not enough router-metered signal.
        assert_eq!(window_remaining_dollars(0.49, 50.0), None);
        assert!(window_remaining_dollars(0.50, 50.0).is_some());
    }

    #[test]
    fn anthropic_overage_dollars_prefers_spend_over_extra_usage() {
        let both = json!({
            "spend": { "enabled": true, "limit": { "amount_minor": 900_000, "exponent": 2 },
                       "used": { "amount_minor": 234_000, "exponent": 2 } },
            "extra_usage": { "is_enabled": true, "monthly_limit": 500_000,
                              "used_credits": 0.0, "decimal_places": 2 }
        });
        assert!((anthropic_overage_dollars(&both).unwrap() - 6_660.0).abs() < 1e-6);

        let extra_usage_only = json!({
            "extra_usage": { "is_enabled": true, "monthly_limit": 900_000,
                              "used_credits": 234_000.0, "decimal_places": 2 }
        });
        assert!((anthropic_overage_dollars(&extra_usage_only).unwrap() - 6_660.0).abs() < 1e-6);

        // Neither carries a usable cap: no dollar figure to grade.
        assert_eq!(anthropic_overage_dollars(&json!({})), None);
        let disabled = json!({ "spend": { "enabled": false } });
        assert_eq!(anthropic_overage_dollars(&disabled), None);
    }

    #[test]
    fn codex_overage_dollars_from_individual_limit_and_credits() {
        // Real dollar member limit — string amounts, as the live API sends.
        let member_limit = json!({
            "individualLimit": { "limit": "3000", "used": "2910" }
        });
        assert!((codex_overage_dollars(&member_limit).unwrap() - 90.0).abs() < 1e-6);

        // Numeric amounts also accepted.
        let numeric = json!({
            "individual_limit": { "limit": 3000, "used": 2910 }
        });
        assert!((codex_overage_dollars(&numeric).unwrap() - 90.0).abs() < 1e-6);

        // Spend control reached is a hard 0 regardless of a positive limit.
        let blocked = json!({
            "spendControlReached": true,
            "individualLimit": { "limit": "3000", "used": "100" }
        });
        assert_eq!(codex_overage_dollars(&blocked), Some(0.0));

        // No member limit: falls back to a positive credit balance.
        let balance_only = json!({ "credits": { "balance": 42.5 } });
        assert!((codex_overage_dollars(&balance_only).unwrap() - 42.5).abs() < 1e-9);

        // Nothing usable to grade.
        assert_eq!(codex_overage_dollars(&json!({})), None);
        assert_eq!(
            codex_overage_dollars(&json!({ "credits": { "unlimited": true } })),
            None,
            "unlimited has no dollar ceiling — the fraction path (Some(1.0)) handles it"
        );
    }

    #[test]
    fn anthropic_availability_with_spend_estimates_plan_window_dollars() {
        // $50 spent on Fable-scoped requests since the window started, 80%
        // used → $12.50 window capacity, $2.50 left.
        let mut p = exhausted_payload();
        p["limits"][2]["percent"] = json!(80); // the Fable-scoped weekly window
        let now = SystemTime::now();
        let resets_str = "2026-08-12T17:00:00+00:00";
        p["limits"][2]["resets_at"] = json!(resets_str);
        let spend: &SpendLookup = &|_models, _since| Some(50.0);
        let a = anthropic_availability_with_spend(&p, &cands(), Some(spend), now);
        let f = &a[&fable()];
        assert!(
            (f.plan_remaining_dollars.unwrap() - 12.5).abs() < 1.0,
            "fable: {f:?}"
        );
    }

    #[test]
    fn anthropic_availability_falls_back_without_a_spend_lookup() {
        // No spend lookup (the default `anthropic_availability` shape, and
        // every existing test/caller): dollar estimation stays off.
        let a = anthropic_availability(&exhausted_payload(), &cands());
        assert!(a[&fable()].plan_remaining_dollars.is_none());
    }

    #[test]
    fn codex_availability_with_spend_estimates_plan_window_dollars() {
        let now = SystemTime::now();
        let rl = vec![json!({
            "primary": { "usedPercent": 80.0, "windowDurationMins": 10080,
                         "resetsAt": future_epoch(now) },
        })];
        let spend: &SpendLookup = &|_models, _since| Some(40.0);
        let a = codex_availability_with_spend(&rl, &codex_cands(), Some(spend), now);
        let sol = &a[&CandidateId::new("codex", "gpt-5.6-sol")];
        // $40 spent at 80% used → $10 window capacity, $10 left.
        assert!(
            (sol.plan_remaining_dollars.unwrap() - 10.0).abs() < 1.0,
            "sol: {sol:?}"
        );
    }

    #[test]
    fn codex_availability_member_limit_reports_real_dollars_directly() {
        // The member limit already carries real dollars — no estimation
        // needed, and no spend lookup is required for it.
        let now = SystemTime::now();
        let rl = vec![json!({
            "primary": { "usedPercent": 100.0, "windowDurationMins": 10080, "resetsAt": future_epoch(now) },
            "credits": { "hasCredits": true, "unlimited": false, "balance": serde_json::Value::Null },
            "individualLimit": { "limit": "3000", "used": "2910", "remainingPercent": 3.0, "resetsAt": future_epoch(now) },
            "spendControlReached": false,
        })];
        let a = codex_availability(&rl, &codex_cands(), now);
        let sol = &a[&CandidateId::new("codex", "gpt-5.6-sol")];
        assert!((sol.overage_remaining_dollars.unwrap() - 90.0).abs() < 1e-6);
    }

    // ---- Seat availability ----

    #[test]
    fn anthropic_availability_scales_and_scopes() {
        // Fable's scoped cap is saturated; overage still has headroom, so the
        // Fable candidate is on paid overage while Sonnet keeps the free
        // budget implied by the seat-wide windows (min of 72% / 78% used).
        let mut p = exhausted_payload();
        p["extra_usage"]["utilization"] = json!(40.0);
        p["spend"]["percent"] = json!(40);
        let a = anthropic_availability(&p, &cands());
        let f = &a[&fable()];
        assert!(f.on_overage, "fable saturated + overage headroom: {f:?}");
        assert!(f.plan_headroom.abs() < 1e-9, "fable min window is 100%");
        let s = &a[&sonnet()];
        assert!(!s.on_overage);
        assert!(
            (s.plan_headroom - 0.22).abs() < 1e-9,
            "min(28%, 22%) free: {s:?}"
        );
        assert!(a.values().all(|v| v.source == "poll"));
    }

    #[test]
    fn anthropic_availability_without_overage_is_not_on_overage() {
        // Saturated with NO overage headroom is cordon territory, not a
        // preference penalty.
        let a = anthropic_availability(&exhausted_payload(), &cands());
        assert!(!a[&fable()].on_overage);
    }

    #[test]
    fn reported_payload_exhausts_fable_only_and_cordons_it() {
        // The payload from the report, verbatim in shape: Fable's weekly-scoped
        // window at 100% critical, monthly credits AND spend both at 100%,
        // session idle at 0% and weekly_all at 81%. Fable alone has nothing left
        // to spend; Sonnet and Opus keep the 19% the seat-wide window still has,
        // so they must stay routeable — the bug routed Fable here anyway.
        let p = json!({
            "extra_usage": { "is_enabled": true, "utilization": 100.0 },
            "spend": { "enabled": true, "percent": 100, "severity": "critical" },
            "limits": [
                { "kind": "session", "group": "session", "percent": 0, "severity": "normal",
                  "resets_at": null, "scope": null, "is_active": false },
                { "kind": "weekly_all", "group": "weekly", "percent": 81, "severity": "warning",
                  "resets_at": "2026-07-29T16:59:59+00:00", "scope": null, "is_active": false },
                { "kind": "weekly_scoped", "group": "weekly", "percent": 100, "severity": "critical",
                  "resets_at": "2026-07-29T16:59:59+00:00",
                  "scope": { "model": { "id": null, "display_name": "Fable" }, "surface": null },
                  "is_active": true }
            ]
        });
        let opus = CandidateId::new("claude", "opus");
        let mut candidates = cands();
        candidates.push((opus.clone(), "Claude Opus 5".to_string()));

        let a = anthropic_availability(&p, &candidates);
        assert!(a[&fable()].plan_exhausted(), "fable: {:?}", a[&fable()]);
        assert!(!a[&sonnet()].plan_exhausted(), "sonnet: {:?}", a[&sonnet()]);
        assert!(!a[&opus].plan_exhausted(), "opus: {:?}", a[&opus]);
        assert!(
            (a[&sonnet()].plan_headroom - 0.19).abs() < 1e-9,
            "seat-wide window leaves 19% free: {:?}",
            a[&sonnet()]
        );

        // The proactive cordon agrees, and is likewise scoped to Fable.
        let c = anthropic_cordons(&p, &candidates, SystemTime::now());
        assert!(c.contains_key(&fable()));
        assert!(!c.contains_key(&sonnet()) && !c.contains_key(&opus));
    }

    #[test]
    fn exhausted_payload_style_plan_exhausted_is_fable_only() {
        // Mirrors the live anthropic-oauth.json shape: Fable's weekly-scoped
        // window is at 100% with overage/spend both maxed, so Fable alone has
        // nothing left to spend — Sonnet's seat-wide windows (72%/78%) still
        // have headroom and must stay routeable.
        let a = anthropic_availability(&exhausted_payload(), &cands());
        assert!(a[&fable()].plan_exhausted(), "fable: {:?}", a[&fable()]);
        assert!(!a[&sonnet()].plan_exhausted(), "sonnet: {:?}", a[&sonnet()]);
    }

    #[test]
    fn session_and_weekly_saturated_with_overage_maxed_exhausts_every_claude_candidate() {
        // Session (all-models, no scope) AND weekly_all both at 100%, overage
        // and spend both maxed: nothing narrows the exhaustion to one model, so
        // every candidate's seat-wide headroom must read as exhausted.
        let p = json!({
            "extra_usage": { "is_enabled": true, "utilization": 100.0 },
            "spend": { "enabled": true, "percent": 100 },
            "limits": [
                { "kind": "session", "percent": 100, "severity": "critical",
                  "resets_at": "2026-07-22T16:59:59+00:00", "scope": null, "is_active": true },
                { "kind": "weekly_all", "percent": 100, "severity": "critical",
                  "resets_at": "2026-07-29T16:59:59+00:00", "scope": null, "is_active": true }
            ]
        });
        let a = anthropic_availability(&p, &cands());
        assert!(
            a.values().all(|v| v.plan_exhausted()),
            "every claude candidate should be plan-exhausted: {a:?}"
        );
    }

    #[test]
    fn codex_availability_accounts_for_credits_and_reset() {
        let now = SystemTime::now();
        let rl = vec![json!({
            "limit_id": "codex",
            "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": future_epoch(now) },
            "secondary": serde_json::Value::Null,
            "credits": { "has_credits": true, "unlimited": false, "balance": 12.5 }
        })];
        let a = codex_availability(&rl, &codex_cands(), now);
        assert_eq!(a.len(), 2, "account-wide: every candidate covered");
        assert!(
            a.values()
                .all(|v| v.on_overage && v.plan_headroom.abs() < 1e-9)
        );

        // Past-reset windows are stale — no availability data at all.
        let stale = vec![json!({
            "primary": { "used_percent": 100.0, "window_minutes": 10080, "resets_at": 1 },
            "credits": { "has_credits": false }
        })];
        assert!(codex_availability(&stale, &codex_cands(), now).is_empty());
    }

    #[test]
    fn codex_availability_merges_pools_pessimistically() {
        let now = SystemTime::now();
        let rl = vec![
            json!({
                "limit_id": "premium",
                "primary": { "used_percent": 20.0, "window_minutes": 10080, "resets_at": future_epoch(now) },
                "credits": { "has_credits": false }
            }),
            json!({
                "limit_id": "codex",
                "primary": { "used_percent": 90.0, "window_minutes": 10080, "resets_at": future_epoch(now) },
                "credits": { "has_credits": false }
            }),
        ];
        let a = codex_availability(&rl, &codex_cands(), now);
        assert!(
            a.values().all(|v| (v.plan_headroom - 0.10).abs() < 1e-9),
            "tightest pool governs: {a:?}"
        );
    }

    #[test]
    fn hint_availability_parses_windows_and_overage() {
        let entry = json!({
            "agent": "claude",
            "windows": [
                { "percent": 72, "scope": serde_json::Value::Null },
                { "percent": 100, "scope": "Fable", "active": true }
            ],
            "overage": { "enabled": true, "percent": 40 }
        });
        let a = hint_agent_availability(&entry, &cands());
        let f = &a[&fable()];
        assert!(f.on_overage && f.plan_headroom.abs() < 1e-9);
        let s = &a[&sonnet()];
        assert!(!s.on_overage);
        assert!((s.plan_headroom - 0.28).abs() < 1e-9);
        assert!(a.values().all(|v| v.source == "hint"));

        // No windows → no data → nothing to scale.
        let empty = json!({ "agent": "claude", "overage": { "enabled": true, "percent": 0 } });
        assert!(hint_agent_availability(&empty, &cands()).is_empty());

        // Overage disabled (or absent): a saturated seat is not "paying".
        let no_overage = json!({
            "agent": "claude",
            "windows": [ { "percent": 100 } ]
        });
        let a = hint_agent_availability(&no_overage, &cands());
        assert!(
            a.values()
                .all(|v| !v.on_overage && v.plan_headroom.abs() < 1e-9)
        );

        // Full non-binding meter (active:false) must not zero headroom when a
        // free plan window is also present — live Codex member-limit shape.
        let member_not_binding = json!({
            "agent": "codex",
            "windows": [
                { "percent": 0, "active": false },
                { "percent": 100, "active": false }
            ]
        });
        let a = hint_agent_availability(&member_not_binding, &codex_cands());
        assert!(
            a.values().all(|v| (v.plan_headroom - 1.0).abs() < 1e-9),
            "inactive full meter ignored: {a:?}"
        );
    }

    #[test]
    fn hint_overage_headroom_grades_the_pool() {
        let entry = json!({
            "agent": "claude",
            "windows": [ { "percent": 100 } ],
            "overage": { "enabled": true, "percent": 40 }
        });
        let a = hint_agent_availability(&entry, &cands());
        let f = &a[&fable()];
        assert!(f.on_overage);
        assert!(
            (f.overage_headroom.unwrap() - 0.6).abs() < 1e-9,
            "60% of the overage pool free: {f:?}"
        );

        // Overage disabled: no pool to grade.
        let disabled = json!({
            "agent": "claude",
            "windows": [ { "percent": 100 } ]
        });
        let a = hint_agent_availability(&disabled, &cands());
        assert!(a.values().all(|v| v.overage_headroom.is_none()));
    }

    // Regression for the live routing bug (session `rtr-067e9f43`, 2026-08-06):
    // codex's overage pool (member spend) was nearly spent (~2% left) while
    // claude's overage pool (extra_usage) had ~74% left, but both providers'
    // ungraded `on_overage: true` flattened to identical `plan_headroom: 0`
    // availability — so the router had no signal that claude had roughly 37x
    // the remaining budget and routed into the codex seat that ran out
    // minutes later. Grading the pool must surface that gap.
    #[test]
    fn overage_headroom_distinguishes_nearly_spent_from_plenty_left() {
        let now = SystemTime::now();

        // Claude: Fable's scoped weekly window is saturated (per
        // `exhausted_payload`), extra-usage pool is a real $9,000 cap at 26%
        // utilized ($2,340 used, $6,660 free) — the live account shape.
        let mut claude_payload = exhausted_payload();
        claude_payload["extra_usage"]["utilization"] = json!(26.0);
        claude_payload["extra_usage"]["monthly_limit"] = json!(900_000);
        claude_payload["extra_usage"]["used_credits"] = json!(234_000.0);
        claude_payload["extra_usage"]["decimal_places"] = json!(2);
        let claude_avail = anthropic_availability(&claude_payload, &cands());
        let claude_fable = &claude_avail[&fable()];
        assert!(claude_fable.on_overage);
        assert!(
            (claude_fable.overage_remaining_dollars.unwrap() - 6_660.0).abs() < 1e-6,
            "claude: {claude_fable:?}"
        );

        // Codex: weekly plan saturated, member spend is a real $3,000 cap
        // with $2,910 used ($90 free, ~3%) — the live near-spent shape.
        let codex_rl = vec![json!({
            "limitId": "codex",
            "primary": { "usedPercent": 100.0, "windowDurationMins": 10080, "resetsAt": future_epoch(now) },
            "credits": { "hasCredits": true, "unlimited": false, "balance": serde_json::Value::Null },
            "individualLimit": { "limit": "3000", "used": "2910", "remainingPercent": 3.0, "resetsAt": future_epoch(now) },
            "spendControlReached": false,
        })];
        let codex_avail = codex_availability(&codex_rl, &codex_cands(), now);
        let codex_sol = &codex_avail[&CandidateId::new("codex", "gpt-5.6-sol")];
        assert!(codex_sol.on_overage);
        assert!(
            (codex_sol.overage_remaining_dollars.unwrap() - 90.0).abs() < 1e-6,
            "codex: {codex_sol:?}"
        );

        // Both are flagged on_overage with the same plan_headroom (0) — the
        // pre-grading signal was identical for both. seat_budget() must not
        // be, and the ranking must come from the DOLLARS: $6,660 saturates
        // to a fully-free seat at the $200 scale, while $90 reads visibly
        // constrained (0.45).
        assert!((claude_fable.plan_headroom - codex_sol.plan_headroom).abs() < 1e-9);
        const SCALE: f64 = 200.0;
        assert!(
            (claude_fable.seat_budget(SCALE) - 1.0).abs() < 1e-9,
            "claude: {claude_fable:?}"
        );
        assert!(
            (codex_sol.seat_budget(SCALE) - 0.45).abs() < 1e-9,
            "codex: {codex_sol:?}"
        );
    }
}
