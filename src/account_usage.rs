//! Deterministic, read-only formatting for the native `/usage` command.
//!
//! This module deliberately knows only about published usage snapshots and
//! router state. It never starts a provider or refreshes credentials, or
//! prints a credential-shaped field.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use crate::auth::AuthAvailability;
use crate::candidate::CandidateId;
use crate::config::AgentConfig;
use crate::session::Shared;
use crate::usage_cache::Snapshot;

const DEFAULT_CACHE_MAX_AGE_SECS: u64 = 5 * 60;
const BAR_WIDTH: usize = 10;

/// Format all configured runtime accounts, grouped by provider.
pub fn summarize(shared: &Arc<Shared>) -> String {
    let mut accounts: Vec<(usize, AgentConfig, String)> = shared
        .agent_configs()
        .into_iter()
        .filter(crate::accounts::registered)
        .enumerate()
        .map(|(index, agent)| {
            let provider = crate::accounts::provider(&agent)
                .unwrap_or("unknown")
                .to_string();
            (index, agent, provider)
        })
        .collect();

    accounts.sort_by(
        |(left_index, left, left_provider), (right_index, right, right_provider)| {
            provider_order(left_provider)
                .cmp(&provider_order(right_provider))
                .then_with(|| left_provider.cmp(right_provider))
                .then_with(|| {
                    priority_key(left.account_priority).cmp(&priority_key(right.account_priority))
                })
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left_index.cmp(right_index))
        },
    );

    let mut out = String::from("Usage\n");
    let mut current_provider: Option<String> = None;
    for (_, agent, provider) in accounts {
        if current_provider.as_deref() != Some(provider.as_str()) {
            if current_provider.is_some() {
                out.push('\n');
            }
            out.push_str(&format!("{provider}:\n"));
            current_provider = Some(provider);
        }
        let detail = summarize_account(shared, &agent);
        for line in detail.lines() {
            out.push_str("  ");
            out.push_str(line);
            out.push('\n');
        }
    }
    out.trim_end_matches('\n').to_string()
}

/// Format one account for `/login` detail and for the grouped `/usage` view.
pub fn summarize_account(shared: &Arc<Shared>, agent: &AgentConfig) -> String {
    let provider = crate::accounts::provider(agent).unwrap_or("unknown");
    let (identity, plan) = crate::accounts::identity(agent);
    let identity = if identity.trim().is_empty() {
        safe_text(&agent.name)
    } else {
        safe_text(&identity)
    };

    let mut lines = vec![format!("{} ({})", safe_text(&agent.name), identity)];
    if let Some(plan) = plan.as_deref().filter(|plan| !plan.trim().is_empty()) {
        lines.push(format!("plan: {}", safe_text(plan)));
    }
    if agent.account_priority.is_some() {
        lines.push(format!(
            "account priority: {}",
            agent
                .account_priority
                .map(|priority| priority.to_string())
                .unwrap_or_else(|| "unset".to_string())
        ));
    }

    let auth = shared.auth.lock().unwrap().availability(&agent.name);
    match auth {
        AuthAvailability::Authenticated => lines.push("auth: authenticated".to_string()),
        AuthAvailability::Unknown => lines.push("auth: unknown".to_string()),
        AuthAvailability::Unauthenticated { reason } => {
            lines.push(format!("auth: rejected ({})", safe_text(&reason)))
        }
    }

    let (reactive_cordon, model_cordons) = {
        let mut headroom = shared.headroom.lock().unwrap();
        let reactive_cordon = headroom.cordon_active(&agent.name);
        let model_cordons = agent
            .models
            .iter()
            .filter_map(|model| {
                let id = CandidateId::new(&agent.name, &model.id);
                headroom
                    .usage_cordon(&id)
                    .map(|cordon| (model.id.clone(), cordon.clone()))
            })
            .collect::<Vec<_>>();
        (reactive_cordon, model_cordons)
    };

    if let Some((_, reason)) = &reactive_cordon {
        lines.push(format!("cordon: {}", safe_text(reason)));
    }
    for (model, cordon) in model_cordons {
        lines.push(format!(
            "model {} cordon: {} (resets {})",
            safe_text(&model),
            safe_text(&cordon.reason),
            safe_text(&cordon.resets_at_rfc3339)
        ));
    }

    if let Some(reserves) = format_reserves(agent) {
        lines.push(reserves);
    }

    if provider == "grok" {
        lines.push("numeric usage unavailable; Grok exposes a binary access gate".to_string());
        if reactive_cordon.is_none() {
            lines.push("gate: available or not yet observed".to_string());
        }
        return lines.join("\n");
    }

    let snapshot = crate::usage_cache::read_agent_snapshot(agent);
    lines.push(format_snapshot(
        provider,
        snapshot.as_ref(),
        unix_now(),
        shared.cfg.cordon.poll_secs.max(DEFAULT_CACHE_MAX_AGE_SECS),
    ));
    lines.join("\n")
}

/// Format a provider payload without exposing arbitrary JSON fields.
///
/// The helper accepts both the provider's camelCase wire shape and the older
/// snake_case shape used by cached Claude responses.
pub fn format_payload(provider: &str, payload: &Value) -> String {
    match provider {
        "claude" => format_claude(payload),
        "codex" => format_codex(payload),
        "grok" => "numeric usage unavailable; Grok exposes a binary access gate".to_string(),
        _ => "numeric usage unavailable for this provider".to_string(),
    }
}

/// Format cache metadata and its safe, selected usage fields.
pub fn format_snapshot(
    provider: &str,
    snapshot: Option<&Snapshot>,
    now: u64,
    max_age_secs: u64,
) -> String {
    let Some(snapshot) = snapshot else {
        return "cache: unknown (no account-matched snapshot)".to_string();
    };

    let fetched = if snapshot.fetched_at == 0 {
        "unknown".to_string()
    } else {
        epoch_to_timestamp(snapshot.fetched_at)
    };
    let age = now.saturating_sub(snapshot.fetched_at);
    let status = match snapshot.last_error.as_deref() {
        Some(error) => format!("error ({})", safe_text(error)),
        None if snapshot.payload.is_none() => "unknown (no payload)".to_string(),
        None if snapshot.fetched_at == 0 || snapshot.fetched_at > now => "unknown".to_string(),
        None if age > max_age_secs => "expired".to_string(),
        None => "ok".to_string(),
    };

    let mut out = format!("cache: {status}; fetched {fetched}");
    if snapshot.attempted_at > snapshot.fetched_at {
        out.push_str(&format!(
            "; attempted {}",
            epoch_to_timestamp(snapshot.attempted_at)
        ));
    }
    if let Some(payload) = snapshot.payload.as_ref() {
        let body = format_payload(provider, payload);
        if !body.is_empty() {
            out.push('\n');
            out.push_str(&body);
        }
    }
    out
}

fn format_claude(payload: &Value) -> String {
    let mut lines = Vec::new();
    if let Some(limits) = object_array(payload, "limits") {
        for limit in limits {
            let Some(percent) = number(limit, &["percent"]) else {
                continue;
            };
            let kind = string(limit, &["kind"]).unwrap_or_else(|| "limit".to_string());
            let scope = nested_string(
                limit,
                &["scope"],
                &["model"],
                &["display_name", "displayName", "id"],
            );
            let label = claude_label(&kind, scope.as_deref());
            lines.push(window_line(
                &label,
                percent,
                value_string(limit, &["resets_at", "resetsAt"]),
            ));
        }
    }
    if lines.is_empty() {
        for (snake_key, camel_key, label) in [
            ("five_hour", "fiveHour", "Session (5h)"),
            ("seven_day", "sevenDay", "Weekly - all models"),
        ] {
            if let Some(window) = payload
                .get(snake_key)
                .or_else(|| payload.get(camel_key))
                .filter(|v| v.is_object())
                && let Some(percent) = number(window, &["utilization", "percent"])
            {
                lines.push(window_line(
                    label,
                    percent,
                    value_string(window, &["resets_at", "resetsAt"]),
                ));
            }
        }
    }

    if let Some(credits) = claude_credits(payload) {
        lines.push(credits);
    }
    if lines.is_empty() {
        "usage: unknown (payload has no numeric limits)".to_string()
    } else {
        lines.join("\n")
    }
}

fn format_codex(payload: &Value) -> String {
    let mut pools: Vec<(String, Value)> = Vec::new();
    if let Some(map) = object(payload, "rateLimitsByLimitId")
        .or_else(|| object(payload, "rate_limits_by_limit_id"))
    {
        pools.extend(map.iter().map(|(key, value)| (key.clone(), value.clone())));
        pools.sort_by(|left, right| left.0.cmp(&right.0));
    }
    if pools.is_empty()
        && let Some(rate_limits) =
            object(payload, "rateLimits").or_else(|| object(payload, "rate_limits"))
    {
        pools.push((
            map_string(rate_limits, &["limitId", "limit_id"]).unwrap_or_default(),
            Value::Object(rate_limits.clone()),
        ));
    }

    let multi = pools.len() > 1;
    let mut lines = Vec::new();
    for (pool_key, pool) in pools {
        let tag = if multi && !pool_key.is_empty() {
            format!(" - {}", safe_text(&pool_key))
        } else {
            String::new()
        };
        for (slot, fallback) in [("primary", "Usage"), ("secondary", "Secondary usage")] {
            let Some(window) = pool.get(slot).filter(|v| v.is_object()) else {
                continue;
            };
            let Some(percent) = number(window, &["usedPercent", "used_percent"]) else {
                continue;
            };
            let label = codex_label(window, fallback, &tag);
            lines.push(window_line(
                &label,
                percent,
                value_string(window, &["resetsAt", "resets_at"]),
            ));
        }
        if let Some(member) = codex_member_limit(&pool, &tag) {
            lines.push(member);
        }
        if let Some(credits) = codex_credits(&pool, &tag) {
            lines.push(credits);
        }
    }

    if let Some(credits) = object(payload, "rateLimitResetCredits")
        .or_else(|| object(payload, "rate_limit_reset_credits"))
        .and_then(|credits| map_number(credits, &["availableCount", "available_count"]))
    {
        lines.push(format!("credits: {} available", rounded_number(credits)));
    }
    if lines.is_empty() {
        "usage: unknown (payload has no numeric limits)".to_string()
    } else {
        lines.join("\n")
    }
}

fn claude_credits(payload: &Value) -> Option<String> {
    let (credits, enabled) = if let Some(spend) = object(payload, "spend") {
        (spend, map_bool(spend, &["enabled"]) == Some(true))
    } else {
        let extra = object(payload, "extra_usage").or_else(|| object(payload, "extraUsage"))?;
        (
            extra,
            map_bool(extra, &["is_enabled", "isEnabled"]) == Some(true),
        )
    };
    if !enabled {
        return None;
    }

    let decimal_places = map_number(credits, &["decimal_places", "decimalPlaces"]).unwrap_or(2.0);
    let scale = 10f64.powf(decimal_places);
    let used = map_money_value(credits, "used").or_else(|| {
        map_number(credits, &["used_credits", "usedCredits"]).map(|value| value / scale)
    });
    let limit = map_money_value(credits, "limit").or_else(|| {
        map_number(credits, &["monthly_limit", "monthlyLimit"]).map(|value| value / scale)
    });
    let percent = map_number(credits, &["percent", "utilization"]);
    let currency = map_string(credits, &["currency"]).unwrap_or_else(|| "USD".to_string());
    let reset = map_value_string(credits, &["resets_at", "resetsAt"]);

    let mut label = String::from("Extra usage");
    if let (Some(used), Some(limit)) = (used, limit) {
        label.push_str(&format!(
            " - {} / {}",
            money(used, &currency),
            money(limit, &currency)
        ));
    } else if let Some(used) = used {
        label.push_str(&format!(" - {} used", money(used, &currency)));
    }
    let mut detail = label;
    if let Some(percent) = percent {
        detail.push_str(&format!(" {}", meter_suffix(percent, reset)));
    } else if let Some(reset) = reset {
        detail.push_str(&format!("; resets {}", safe_text(&reset)));
    }
    Some(format!("extra usage: {detail}"))
}

fn codex_member_limit(pool: &Value, tag: &str) -> Option<String> {
    let member = object(pool, "individualLimit").or_else(|| object(pool, "individual_limit"))?;
    let remaining = map_number(member, &["remainingPercent", "remaining_percent"])?;
    let percent = (100.0 - remaining).clamp(0.0, 100.0);
    let mut label = format!("Member limit{tag}");
    if let (Some(used), Some(limit)) = (
        string_or_number(member, "used"),
        string_or_number(member, "limit"),
    ) {
        label.push_str(&format!(
            " - ${} / ${}",
            compact_money(&used),
            compact_money(&limit)
        ));
    }
    Some(window_line(
        &label,
        percent,
        map_value_string(member, &["resetsAt", "resets_at"]),
    ))
}

fn codex_credits(pool: &Value, tag: &str) -> Option<String> {
    let credits = object(pool, "credits")?;
    if map_bool(credits, &["unlimited"]) == Some(true) {
        return Some(format!("credits{tag}: unlimited"));
    }
    if let Some(balance) = map_number(credits, &["balance"]) {
        return Some(format!("credits{tag}: ${}", compact_number(balance)));
    }
    if map_bool(credits, &["hasCredits", "has_credits"]) == Some(true) {
        return Some(format!("credits{tag}: available (balance unknown)"));
    }
    None
}

fn format_reserves(agent: &AgentConfig) -> Option<String> {
    let weekly = agent.reserve_capacity.weekly;
    let session = agent.reserve_capacity.session;
    if weekly <= 0.0 && session <= 0.0 {
        return None;
    }
    Some(format!(
        "reserves: weekly {}%, session {}%; effective ceilings: weekly {}%, session {}%",
        compact_number(weekly),
        compact_number(session),
        compact_number((100.0 - weekly).max(0.0)),
        compact_number((100.0 - session).max(0.0)),
    ))
}

fn window_line(label: &str, percent: f64, reset: Option<String>) -> String {
    format!("{}: {}", safe_text(label), meter_suffix(percent, reset))
}

fn meter_suffix(percent: f64, reset: Option<String>) -> String {
    let mut out = format!("{}% {}", rounded_percent(percent), usage_bar(percent));
    if let Some(reset) = reset {
        out.push_str(&format!("; resets {}", safe_text(&reset)));
    }
    out
}

fn claude_label(kind: &str, scope: Option<&str>) -> String {
    let base = match kind {
        "session" => "Session (5h)",
        "weekly_scoped" => "Weekly - scoped",
        kind if kind.starts_with("weekly") => "Weekly - all models",
        _ => kind,
    };
    match scope.filter(|scope| !scope.is_empty()) {
        Some(scope) => format!("{base} - {scope}"),
        None => base.to_string(),
    }
}

fn codex_label(window: &Value, fallback: &str, tag: &str) -> String {
    let minutes = number(window, &["windowDurationMins", "window_duration_mins"]);
    let base = match minutes {
        Some(minutes) if (minutes - 300.0).abs() < f64::EPSILON => "Session (5h)".to_string(),
        Some(minutes) if (minutes - 10_080.0).abs() < f64::EPSILON => "Weekly".to_string(),
        Some(minutes) if minutes < 1_440.0 => {
            format!("Session ({}h)", rounded_number(minutes / 60.0))
        }
        Some(minutes) => format!("{}-day", rounded_number(minutes / 1_440.0)),
        None => fallback.to_string(),
    };
    format!("{base}{tag}")
}

fn object<'a>(value: &'a Value, key: &str) -> Option<&'a Map<String, Value>> {
    value.get(key)?.as_object()
}

fn object_array<'a>(value: &'a Value, key: &str) -> Option<&'a [Value]> {
    value.get(key)?.as_array().map(Vec::as_slice)
}

fn nested_string(value: &Value, first: &[&str], second: &[&str], third: &[&str]) -> Option<String> {
    let mut current = value;
    for key in first.iter().chain(second) {
        current = current.get(*key)?;
    }
    string_from_keys(current, third)
}

fn string(value: &Value, keys: &[&str]) -> Option<String> {
    string_from_keys(value, keys)
}

fn map_string(value: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn string_from_keys(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn value_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        match value {
            Value::String(value) if !value.is_empty() => Some(value.clone()),
            Value::Number(_) => numeric_timestamp(value),
            _ => None,
        }
    })
}

fn map_value_string(value: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        match value {
            Value::String(value) if !value.is_empty() => Some(value.clone()),
            Value::Number(_) => numeric_timestamp(value),
            _ => None,
        }
    })
}

fn string_or_number(value: &Map<String, Value>, key: &str) -> Option<String> {
    let value = value.get(key)?;
    match value {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn numeric_timestamp(value: &Value) -> Option<String> {
    value
        .as_u64()
        .or_else(|| {
            value
                .as_f64()
                .filter(|value| value.is_finite() && *value >= 0.0)
                .map(|value| value as u64)
        })
        .map(epoch_to_timestamp)
}

fn number(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        match value {
            Value::Number(value) => value.as_f64(),
            Value::String(value) => value.parse::<f64>().ok(),
            _ => None,
        }
        .filter(|value| value.is_finite())
    })
}

fn map_number(value: &Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        match value {
            Value::Number(value) => value.as_f64(),
            Value::String(value) => value.parse::<f64>().ok(),
            _ => None,
        }
        .filter(|value| value.is_finite())
    })
}

fn map_bool(value: &Map<String, Value>, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_bool))
}

fn map_money_value(value: &Map<String, Value>, key: &str) -> Option<f64> {
    let amount = value.get(key)?.as_object()?;
    let minor = map_number(amount, &["amount_minor", "amountMinor"])?;
    let exponent = map_number(amount, &["exponent"]).unwrap_or(2.0);
    Some(minor / 10f64.powf(exponent))
}

fn usage_bar(percent: f64) -> String {
    let filled = ((percent.clamp(0.0, 100.0) / 100.0) * BAR_WIDTH as f64).round() as usize;
    format!(
        "[{}{}]",
        "#".repeat(filled.min(BAR_WIDTH)),
        "-".repeat(BAR_WIDTH.saturating_sub(filled)),
    )
}

fn rounded_percent(value: f64) -> String {
    rounded_number(value.clamp(0.0, 100.0))
}

fn rounded_number(value: f64) -> String {
    if (value.round() - value).abs() < 0.01 {
        format!("{:.0}", value)
    } else {
        format!("{:.1}", value)
    }
}

fn compact_number(value: f64) -> String {
    rounded_number(value)
}

fn money(value: f64, currency: &str) -> String {
    let prefix = if currency.eq_ignore_ascii_case("USD") {
        "$"
    } else {
        ""
    };
    format!("{prefix}{}", compact_money(&format!("{value:.2}")))
}

fn compact_money(value: &str) -> String {
    let parsed = value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map(|value| (value * 100.0).round() / 100.0);
    match parsed {
        Some(value) if value.fract().abs() < 0.005 => format!("{value:.0}"),
        Some(value) => format!("{value:.2}"),
        None => safe_text(value),
    }
}

fn priority_key(priority: Option<u32>) -> (u8, u32) {
    match priority {
        Some(priority) => (0, priority),
        None => (1, u32::MAX),
    }
}

fn provider_order(provider: &str) -> u8 {
    match provider {
        "claude" => 0,
        "codex" => 1,
        "grok" => 2,
        _ => 3,
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn epoch_to_timestamp(value: u64) -> String {
    crate::usage::epoch_to_rfc3339(value)
}

fn safe_text(value: &str) -> String {
    let mut output = String::new();
    let mut redact_next = false;
    for word in value.split_whitespace() {
        let lower = word.to_ascii_lowercase();
        let secret_like = redact_next
            || lower == "bearer"
            || lower.starts_with("bearer=")
            || lower.starts_with("sk-")
            || lower.starts_with("rk-")
            || lower.starts_with("rt-")
            || lower.starts_with("eyj")
            || lower.contains("access_token")
            || lower.contains("refresh_token")
            || lower.starts_with("token=")
            || lower.starts_with("api_key=")
            || lower.starts_with("secret=");
        if !output.is_empty() {
            output.push(' ');
        }
        if secret_like {
            output.push_str("[redacted]");
        } else {
            output.push_str(
                &word
                    .chars()
                    .filter(|character| !character.is_control())
                    .collect::<String>(),
            );
        }
        redact_next = lower == "bearer";
    }
    output.chars().take(240).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn formats_claude_limits_scopes_and_spend_in_snake_case() {
        let payload = json!({
            "limits": [{
                "kind": "session",
                "percent": 69.4,
                "resets_at": "2026-10-07T00:00:00Z"
            }, {
                "kind": "weekly_scoped",
                "percent": 100,
                "scope": {"model": {"display_name": "Fable"}},
                "resets_at": "2026-10-08T00:00:00Z"
            }],
            "spend": {
                "enabled": true,
                "percent": 42,
                "used": {"amount_minor": 1250, "currency": "USD", "exponent": 2},
                "limit": {"amount_minor": 5000, "currency": "USD", "exponent": 2}
            }
        });
        let output = format_payload("claude", &payload);
        assert!(output.contains("Session (5h): 69.4%"));
        assert!(output.contains("Weekly - scoped - Fable: 100%"));
        assert!(output.contains("Extra usage - $12.50 / $50"));
        assert!(output.contains("##########"));
    }

    #[test]
    fn formats_claude_fallback_and_camel_case_without_fabricated_zeroes() {
        let payload = json!({
            "fiveHour": {"utilization": 81.4, "resetsAt": 1784600000},
            "sevenDay": {"utilization": null},
            "extra_usage": {"is_enabled": true, "used_credits": 166380}
        });
        let output = format_payload("claude", &payload);
        assert!(output.contains("Session (5h): 81.4%"));
        assert!(!output.contains("Weekly - all models: 0%"));
        assert!(output.contains("Extra usage - $1663.80 used"));
    }

    #[test]
    fn formats_codex_pools_member_limit_credits_and_snake_case() {
        let payload = json!({
            "rate_limits_by_limit_id": {
                "premium": {"limit_id": "premium", "primary": {"used_percent": 20, "window_duration_mins": 10080}},
                "codex": {
                    "primary": {"usedPercent": 90, "windowDurationMins": 300, "resetsAt": 1784600000},
                    "individual_limit": {"remaining_percent": 0, "used": "1021.625", "limit": "1000"},
                    "credits": {"has_credits": true, "balance": null}
                }
            }
        });
        let output = format_payload("codex", &payload);
        assert!(output.contains("Session (5h) - codex: 90%"));
        assert!(output.contains("Weekly - premium: 20%"));
        assert!(output.contains("Member limit - codex - $1021.63 / $1000"));
        assert!(output.contains("credits - codex: available (balance unknown)"));
    }

    #[test]
    fn snapshot_errors_do_not_infer_account_rejection_or_dump_tokens() {
        let snapshot = Snapshot {
            source: "anthropic-oauth".to_string(),
            account: "fingerprint".to_string(),
            access_generation: None,
            fetched_at: 100,
            attempted_at: 110,
            consecutive_failures: 1,
            last_error: Some("HTTP 401 Unauthorized Bearer sk-secret-token".to_string()),
            payload: Some(
                json!({"five_hour": {"utilization": 12}, "access_token": "sk-secret-token"}),
            ),
        };
        let output = format_snapshot("claude", Some(&snapshot), 1_000, 300);
        assert!(output.contains("cache: error (HTTP 401"));
        assert!(!output.contains("rejected"));
        assert!(output.contains("fetched 1970-01-01T00:01:40Z"));
        assert!(!output.contains("sk-secret-token"));
        assert!(!output.contains("access_token"));
    }

    #[test]
    fn missing_snapshot_is_unknown_and_grok_is_binary_only() {
        assert_eq!(
            format_snapshot("claude", None, 0, 300),
            "cache: unknown (no account-matched snapshot)"
        );
        assert!(
            format_payload("grok", &json!({"percent": 99})).contains("numeric usage unavailable")
        );
    }
}
