//! Normalized account usage for hosts, read from the router's own snapshots.
//!
//! `account-status` publishes this view so a host renders meters without
//! knowing provider payload shapes or where the router caches them. A window
//! is `{id, label, percent, resetsAt, severity, active, scope, windowMinutes,
//! reserveKind}` plus `capacityDollars` on whole-seat windows the router has
//! measured, and `amountUsed`/`amountLimit` on a Codex member spend limit.
//! `active` means the limit is reached, never that the window is metering.

use serde_json::{Map, Value, json};

use crate::config::UsageSourceConfig;
use crate::usage_cache::Snapshot;

/// Shown while a matching snapshot does not exist yet.
pub const NO_READING: &str = "No account-matched router usage read yet.";
/// A throttled provider usage read. The last good reading is kept.
pub const RATE_LIMITED: &str = "rate-limited, retrying";

/// `{plan, windows, credits, updatedAt, error}` for one account. `snapshot`
/// is already matched to the account's current credential generation.
pub fn view(
    source: Option<&UsageSourceConfig>,
    snapshot: Option<&Snapshot>,
    capacity: &Value,
) -> Value {
    let reading = snapshot
        .filter(|s| s.fetched_at > 0)
        .and_then(|s| Some((s, s.payload.as_ref()?)));
    let Some((snapshot, payload)) = reading else {
        return json!({"plan": null, "windows": [], "credits": null, "updatedAt": null,
            "error": source.map(|_| NO_READING)});
    };
    let (plan, windows, credits) = match source {
        Some(UsageSourceConfig::AnthropicOauth) => claude(payload),
        Some(UsageSourceConfig::CodexRollout) => codex(payload),
        None => (None, Vec::new(), Value::Null),
    };
    let error = snapshot
        .last_error
        .as_deref()
        .filter(|e| !e.is_empty())
        .map(|e| {
            let lower = e.to_ascii_lowercase();
            if lower.contains("429") || lower.contains("rate limit") || lower.contains("rate-limit")
            {
                RATE_LIMITED.to_string()
            } else {
                e.to_string()
            }
        });
    json!({
        "plan": plan,
        "windows": windows.into_iter().map(|w| with_reserve(w, capacity)).collect::<Vec<_>>(),
        "credits": credits,
        "updatedAt": chrono::DateTime::from_timestamp(snapshot.fetched_at as i64, 0)
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        "error": error,
    })
}

fn round(v: Option<&Value>) -> Option<i64> {
    v.and_then(Value::as_f64)
        .filter(|n| n.is_finite())
        .map(|n| n.round() as i64)
}

fn severity(percent: i64) -> &'static str {
    match percent {
        p if p >= 95 => "critical",
        p if p >= 80 => "warning",
        _ => "normal",
    }
}

struct Window<'a> {
    id: String,
    label: String,
    percent: i64,
    resets_at: Value,
    severity: &'a str,
    active: bool,
    scope: Option<String>,
    minutes: Option<f64>,
}

impl Window<'_> {
    fn into_map(self) -> Map<String, Value> {
        let mut w = Map::new();
        w.insert("id".into(), self.id.into());
        w.insert("label".into(), self.label.into());
        w.insert("percent".into(), self.percent.into());
        w.insert("resetsAt".into(), self.resets_at);
        w.insert("severity".into(), self.severity.into());
        w.insert("active".into(), self.active.into());
        w.insert("scope".into(), self.scope.into());
        w.insert("windowMinutes".into(), json!(self.minutes));
        w
    }
}

type Normalized = (Option<String>, Vec<Map<String, Value>>, Value);

fn claude(data: &Value) -> Normalized {
    let mut windows = Vec::new();
    let limits = data
        .get("limits")
        .and_then(Value::as_array)
        .filter(|l| !l.is_empty());
    if let Some(limits) = limits {
        for limit in limits {
            let Some(percent) = round(limit.get("percent")) else {
                continue;
            };
            let kind = limit.get("kind").and_then(Value::as_str).unwrap_or("limit");
            let scope = limit
                .pointer("/scope/model/display_name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let (id, minutes) = match kind {
                "session" => ("session", Some(300.0)),
                "weekly_all" => ("weekly", Some(10080.0)),
                "weekly_scoped" => ("weekly_scoped", Some(10080.0)),
                other => (other, None),
            };
            let label = match (kind, &scope) {
                ("session", _) => "Session (5h)".to_string(),
                ("weekly_scoped", Some(scope)) => format!("Weekly — {scope}"),
                ("weekly_scoped", None) => "Weekly — scoped".to_string(),
                _ => "Weekly — all models".to_string(),
            };
            let severity = limit
                .get("severity")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(severity(percent));
            windows.push(
                Window {
                    id: id.to_string(),
                    label,
                    percent,
                    resets_at: text_or_null(limit.get("resets_at")),
                    severity,
                    active: percent >= 100,
                    scope,
                    minutes,
                }
                .into_map(),
            );
        }
    } else {
        for (key, id, label, minutes) in [
            ("five_hour", "session", "Session (5h)", 300.0),
            ("seven_day", "weekly", "Weekly — all models", 10080.0),
        ] {
            let Some(percent) = round(data.pointer(&format!("/{key}/utilization"))) else {
                continue;
            };
            windows.push(
                Window {
                    id: id.into(),
                    label: label.into(),
                    percent,
                    resets_at: text_or_null(data.pointer(&format!("/{key}/resets_at"))),
                    severity: severity(percent),
                    active: percent >= 100,
                    scope: None,
                    minutes: Some(minutes),
                }
                .into_map(),
            );
        }
    }
    (None, windows, claude_credits(data))
}

/// The extra-usage pool past plan limits, in dollars.
fn claude_credits(data: &Value) -> Value {
    let currency = |v: Option<&Value>| {
        v.and_then(Value::as_str)
            .filter(|c| !c.is_empty())
            .unwrap_or("USD")
            .to_string()
    };
    if let Some(spend) = data
        .get("spend")
        .filter(|s| s.get("enabled").and_then(Value::as_bool) == Some(true))
        && let (Some(used), Some(limit)) = (
            spend.get("used").filter(|v| v.is_object()),
            spend.get("limit").filter(|v| v.is_object()),
        )
    {
        let dollars = |m: &Value| {
            let minor = m.get("amount_minor").and_then(Value::as_f64)?;
            let exponent = m.get("exponent").and_then(Value::as_i64).unwrap_or(2);
            Some(minor / 10f64.powi(exponent as i32))
        };
        return json!({
            "percent": round(spend.get("percent")),
            "used": dollars(used),
            "limit": dollars(limit),
            "currency": currency(used.get("currency")),
            "resetsAt": text_or_null(spend.get("resets_at")),
        });
    }
    let Some(extra) = data
        .get("extra_usage")
        .filter(|e| e.get("is_enabled").and_then(Value::as_bool) == Some(true))
    else {
        return Value::Null;
    };
    let places = extra
        .get("decimal_places")
        .and_then(Value::as_i64)
        .unwrap_or(2);
    let scaled = |key: &str| {
        extra
            .get(key)
            .and_then(Value::as_f64)
            .map(|n| n / 10f64.powi(places as i32))
    };
    json!({
        "percent": round(extra.get("utilization")),
        "used": scaled("used_credits"),
        "limit": scaled("monthly_limit"),
        "currency": currency(extra.get("currency")),
        "resetsAt": text_or_null(extra.get("resets_at")),
    })
}

fn text_or_null(v: Option<&Value>) -> Value {
    v.and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| Value::String(s.to_string()))
        .unwrap_or(Value::Null)
}

fn epoch_resets_at(v: Option<&Value>) -> Value {
    let Some(n) = v
        .and_then(Value::as_f64)
        .filter(|n| n.is_finite() && *n > 0.0)
    else {
        return Value::Null;
    };
    let millis = if n > 1e12 { n } else { n * 1000.0 };
    chrono::DateTime::from_timestamp_millis(millis as i64)
        .map(|t| Value::String(t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)))
        .unwrap_or(Value::Null)
}

/// The server decides which window rides in which slot, so the label follows
/// the reported duration.
fn codex_label(minutes: Option<f64>, fallback: &str) -> String {
    match minutes.filter(|m| *m > 0.0) {
        None => fallback.to_string(),
        Some(300.0) => "Session (5h)".to_string(),
        Some(10080.0) => "Weekly".to_string(),
        Some(m) if m < 1440.0 => format!("Session ({}h)", (m / 60.0).round()),
        Some(m) => format!("{}-day", (m / 1440.0).round()),
    }
}

/// Every Codex limit pool's rate windows plus its per-member spend limit. A
/// spent member limit is binding only once a plan window is also saturated:
/// before that the seat still has plan quota. `credits` (uncapped, so `limit`
/// is null) reports usable paid overflow unless a member limit is spent out.
fn codex(result: &Value) -> Normalized {
    let mut pools: Vec<&Value> = result
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
        .map(|by_id| by_id.values().filter(|p| p.is_object()).collect())
        .unwrap_or_default();
    if pools.is_empty()
        && let Some(top) = result.get("rateLimits").filter(|p| p.is_object())
    {
        pools.push(top);
    }
    let multi = pools.len() > 1;
    let mut windows: Vec<Map<String, Value>> = Vec::new();
    let mut plan = None;
    let mut member_spent_out_any = false;
    for pool in &pools {
        if plan.is_none() {
            plan = pool
                .get("planType")
                .and_then(Value::as_str)
                .filter(|p| !p.is_empty())
                .map(str::to_string);
        }
        let limit_id = pool
            .get("limitId")
            .and_then(Value::as_str)
            .filter(|id| multi && !id.is_empty());
        let tag = limit_id.map(|id| format!(" — {id}")).unwrap_or_default();
        let key = limit_id.map(|id| format!("{id}:")).unwrap_or_default();
        for (slot, fallback) in [("primary", "Usage"), ("secondary", "Secondary usage")] {
            let Some(w) = pool.get(slot) else { continue };
            let Some(percent) = round(w.get("usedPercent")) else {
                continue;
            };
            let minutes = w
                .get("windowDurationMins")
                .and_then(Value::as_f64)
                .filter(|m| m.is_finite());
            windows.push(
                Window {
                    id: format!("{key}{slot}"),
                    label: codex_label(minutes, fallback) + &tag,
                    percent,
                    resets_at: epoch_resets_at(w.get("resetsAt")),
                    severity: severity(percent),
                    active: percent >= 100,
                    scope: None,
                    minutes,
                }
                .into_map(),
            );
        }
        let plan_slots = [
            format!("{key}primary"),
            format!("{key}secondary"),
            "primary".into(),
            "secondary".into(),
        ];
        let saturated = windows.iter().any(|w| {
            plan_slots.iter().any(|slot| w["id"] == slot.as_str())
                && (w["active"] == true || w["percent"].as_i64().unwrap_or(0) >= 100)
        });
        let Some(member) = pool.get("individualLimit") else {
            continue;
        };
        let Some(remaining) = member
            .get("remainingPercent")
            .and_then(Value::as_f64)
            .filter(|r| r.is_finite())
        else {
            continue;
        };
        let percent = (100.0 - remaining).clamp(0.0, 100.0).round() as i64;
        let spent_out = remaining <= 0.0
            || pool.get("spendControlReached").and_then(Value::as_bool) == Some(true);
        member_spent_out_any |= spent_out;
        // `used`/`limit` arrive as numeric strings (or numbers) of dollars.
        let amount = |key: &str| match member.get(key) {
            Some(Value::String(s)) => s.parse::<f64>().ok(),
            Some(v) => v.as_f64(),
            None => None,
        };
        let (used, limit) = match (amount("used"), amount("limit")) {
            (Some(used), Some(limit)) if used.is_finite() && limit.is_finite() && limit > 0.0 => {
                (Some(used), Some(limit))
            }
            _ => (None, None),
        };
        let detail = match (used, limit) {
            (Some(used), Some(limit)) => format!(" — {} / {}", dollars(used), dollars(limit)),
            _ => String::new(),
        };
        let mut w = Window {
            id: format!("{key}member"),
            label: format!("Member limit{detail}{tag}"),
            percent,
            resets_at: epoch_resets_at(member.get("resetsAt")),
            // The meter stays visible but is not binding while plan remains.
            severity: severity(if saturated { percent } else { percent.min(94) }),
            active: spent_out && saturated,
            scope: None,
            minutes: None,
        }
        .into_map();
        w.insert("amountUsed".into(), json!(used));
        w.insert("amountLimit".into(), json!(limit));
        windows.push(w);
    }
    let has_credits = pools
        .iter()
        .copied()
        .chain(result.get("rateLimits"))
        .any(|p| {
            p.pointer("/credits/hasCredits") == Some(&Value::Bool(true))
                || p.pointer("/credits/unlimited") == Some(&Value::Bool(true))
        });
    let credits = if has_credits && !member_spent_out_any {
        json!({"percent": null, "used": null, "limit": null, "currency": "USD", "resetsAt": null})
    } else {
        Value::Null
    };
    (plan, windows, credits)
}

/// `$1,022`: whole dollars with thousands separators.
fn dollars(n: f64) -> String {
    let digits = (n.round() as i64).abs().to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    format!("{}${out}", if n.round() < 0.0 { "-" } else { "" })
}

/// Which reserve applies to a window: a day or longer is weekly, shorter is
/// session; a Codex slot without a duration takes its slot's default. Spend
/// meters never reserve.
pub fn reserve_kind(id: &str, minutes: Option<f64>) -> Option<&'static str> {
    if id == "member" || id.ends_with(":member") {
        return None;
    }
    let default = if id.ends_with("secondary") {
        10080.0
    } else {
        300.0
    };
    Some(if minutes.unwrap_or(default) >= 1440.0 {
        "weekly"
    } else {
        "session"
    })
}

fn with_reserve(mut w: Map<String, Value>, capacity: &Value) -> Value {
    let kind = reserve_kind(w["id"].as_str().unwrap_or(""), w["windowMinutes"].as_f64());
    w.insert("reserveKind".into(), json!(kind));
    // A model-scoped cap is not the seat's size.
    if let Some(kind) = kind
        && w["scope"].is_null()
        && let Some(d) = capacity
            .pointer(&format!("/{kind}/dollars"))
            .and_then(Value::as_f64)
            .filter(|d| d.is_finite() && *d > 0.0)
    {
        w.insert("capacityDollars".into(), json!(d));
    }
    Value::Object(w)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(payload: Value, last_error: Option<&str>) -> Snapshot {
        Snapshot {
            source: "test".into(),
            account: "a".into(),
            access_generation: None,
            fetched_at: 1_791_331_200,
            attempted_at: 1_791_331_200,
            consecutive_failures: 0,
            last_error: last_error.map(str::to_string),
            payload: Some(payload),
        }
    }

    fn claude_payload() -> Value {
        json!({
            "five_hour": {"utilization": 69, "resets_at": "2026-07-20T18:09:59Z"},
            "limits": [
                {"kind": "session", "percent": 69, "severity": "normal", "resets_at": "2026-07-20T18:09:59Z", "scope": null, "is_active": true},
                {"kind": "weekly_all", "percent": 68, "severity": "normal", "resets_at": "2026-07-22T16:59:59Z", "scope": null},
                {"kind": "weekly_scoped", "percent": 100, "severity": "critical", "resets_at": "2026-07-22T16:59:59Z", "scope": {"model": {"display_name": "Fable"}}},
            ],
            "spend": {"enabled": true, "percent": 98,
                "used": {"amount_minor": 489600, "currency": "USD", "exponent": 2},
                "limit": {"amount_minor": 500000, "currency": "USD", "exponent": 2}},
        })
    }

    fn column(v: &Value, key: &str) -> Vec<Value> {
        v["windows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w[key].clone())
            .collect()
    }

    #[test]
    fn claude_limits_scoped_caps_credits_and_measured_capacity() {
        let capacity = json!({"session": {"dollars": 40.0}, "weekly": {"dollars": 900.0}});
        let v = view(
            Some(&UsageSourceConfig::AnthropicOauth),
            Some(&snap(claude_payload(), None)),
            &capacity,
        );
        assert_eq!(
            column(&v, "id"),
            [json!("session"), json!("weekly"), json!("weekly_scoped")]
        );
        assert_eq!(column(&v, "percent"), [json!(69), json!(68), json!(100)]);
        // The metering session window is not a reached limit.
        assert_eq!(
            column(&v, "active"),
            [json!(false), json!(false), json!(true)]
        );
        assert_eq!(
            column(&v, "reserveKind"),
            [json!("session"), json!("weekly"), json!("weekly")]
        );
        assert_eq!(v["windows"][2]["label"], "Weekly — Fable");
        assert_eq!(v["windows"][0]["capacityDollars"], 40.0);
        assert!(v["windows"][2].get("capacityDollars").is_none());
        assert_eq!(
            v["credits"],
            json!({"percent": 98, "used": 4896.0, "limit": 5000.0, "currency": "USD", "resetsAt": null})
        );
        assert_eq!(v["updatedAt"], "2026-10-07T00:00:00.000Z");
        assert_eq!(v["error"], Value::Null);
    }

    #[test]
    fn claude_legacy_windows_and_extra_usage() {
        let (_, windows, credits) = claude(&json!({
            "five_hour": {"utilization": 81.4, "resets_at": "2026-07-20T18:00:00Z"},
            "seven_day": {"utilization": 12, "resets_at": null},
            "extra_usage": {"is_enabled": true, "utilization": 18.48, "used_credits": 166380,
                "monthly_limit": 900000, "currency": "USD", "decimal_places": 2, "resets_at": "2026-09-01T00:00:00Z"},
        }));
        assert_eq!(windows[0]["percent"], 81);
        assert_eq!(windows[0]["severity"], "warning");
        assert_eq!(windows[1]["id"], "weekly");
        assert_eq!(
            credits,
            json!({"percent": 18, "used": 1663.8, "limit": 9000.0, "currency": "USD", "resetsAt": "2026-09-01T00:00:00Z"})
        );
    }

    #[test]
    fn throttled_reads_and_missing_snapshots_are_distinguishable() {
        let throttled = view(
            Some(&UsageSourceConfig::AnthropicOauth),
            Some(&snap(claude_payload(), Some("HTTP 429"))),
            &Value::Null,
        );
        assert_eq!(throttled["error"], RATE_LIMITED);
        assert_eq!(throttled["windows"].as_array().unwrap().len(), 3);
        let none = view(Some(&UsageSourceConfig::CodexRollout), None, &Value::Null);
        assert_eq!(none["error"], NO_READING);
        assert_eq!(none["updatedAt"], Value::Null);
        // A provider without a usage meter reports no reading and no error.
        assert_eq!(view(None, None, &Value::Null)["error"], Value::Null);
    }

    #[test]
    fn codex_windows_epochs_and_credits() {
        let (plan, windows, credits) = codex(&json!({"rateLimits": {
            "planType": "team",
            "primary": {"usedPercent": 42, "windowDurationMins": 300, "resetsAt": 1784600000},
            "secondary": {"usedPercent": 91, "windowDurationMins": 10080, "resetsAt": 1784600000000u64},
            "credits": {"hasCredits": true, "unlimited": false},
        }}));
        assert_eq!(plan.as_deref(), Some("team"));
        assert_eq!(windows[0]["label"], "Session (5h)");
        assert_eq!(windows[1]["label"], "Weekly");
        assert_eq!(windows[1]["severity"], "warning");
        assert_eq!(windows[0]["resetsAt"], windows[1]["resetsAt"]);
        assert_eq!(credits["limit"], Value::Null);
        assert_eq!(credits["currency"], "USD");
    }

    #[test]
    fn codex_pools_durations_and_slot_fallbacks() {
        let (_, windows, credits) = codex(&json!({
            "rateLimits": {"credits": {"hasCredits": true}},
            "rateLimitsByLimitId": {
                "premium": {"limitId": "premium", "primary": {"usedPercent": 20, "windowDurationMins": 10080}},
                "codex": {"limitId": "codex", "primary": {"usedPercent": 90, "windowDurationMins": 300}},
            },
        }));
        let mut labels: Vec<_> = windows
            .iter()
            .map(|w| {
                format!(
                    "{} {}",
                    w["id"].as_str().unwrap(),
                    w["label"].as_str().unwrap()
                )
            })
            .collect();
        labels.sort();
        assert_eq!(
            labels,
            [
                "codex:primary Session (5h) — codex",
                "premium:primary Weekly — premium"
            ]
        );
        assert!(credits.is_object());
        let (_, windows, _) = codex(&json!({"rateLimits": {
            "primary": {"usedPercent": 5, "windowDurationMins": 720},
            "secondary": {"usedPercent": 6, "windowDurationMins": null},
        }}));
        assert_eq!(windows[0]["label"], "Session (12h)");
        assert_eq!(windows[1]["label"], "Secondary usage");
        assert_eq!(codex_label(Some(20160.0), "x"), "14-day");
        let none = json!({"hasCredits": false, "unlimited": false, "balance": "0"});
        assert_eq!(
            codex(&json!({"rateLimits": {"primary": {"usedPercent": 100}, "credits": none}})).2,
            Value::Null
        );
    }

    #[test]
    fn codex_member_limit_binds_only_with_a_saturated_plan() {
        let (_, windows, credits) = codex(&json!({"rateLimits": {
            "primary": {"usedPercent": 100, "windowDurationMins": 10080, "resetsAt": 1785258629},
            "credits": {"hasCredits": true, "unlimited": false},
            "individualLimit": {"limit": "1000", "used": "1021.6252000331879", "remainingPercent": 0, "resetsAt": 1785542400},
            "spendControlReached": true,
        }}));
        assert_eq!(windows[1]["label"], "Member limit — $1,022 / $1,000");
        assert_eq!(windows[1]["active"], true);
        assert_eq!(windows[1]["amountUsed"], 1021.6252000331879);
        assert_eq!(windows[1]["amountLimit"], 1000.0);
        assert_eq!(credits, Value::Null);

        let (_, windows, _) = codex(&json!({"rateLimits": {
            "primary": {"usedPercent": 0, "windowDurationMins": 10080},
            "individualLimit": {"limit": "3000", "used": "3000.58", "remainingPercent": 0, "resetsAt": 1785542400},
            "spendControlReached": true,
        }}));
        assert_eq!(windows[1]["active"], false);
        assert_eq!(windows[1]["severity"], "warning");
        assert_eq!(windows[1]["resetsAt"], "2026-08-01T00:00:00.000Z");

        let (_, windows, _) = codex(&json!({"rateLimits": {
            "primary": {"usedPercent": 10, "windowDurationMins": 10080},
            "individualLimit": {"limit": null, "used": null, "remainingPercent": 37.5},
        }}));
        assert_eq!(windows[1]["label"], "Member limit");
        assert_eq!(windows[1]["percent"], 63);
        assert_eq!(windows[1]["amountUsed"], Value::Null);
    }

    #[test]
    fn reserve_kind_follows_window_length() {
        assert_eq!(reserve_kind("session", Some(300.0)), Some("session"));
        assert_eq!(reserve_kind("primary", Some(10080.0)), Some("weekly"));
        assert_eq!(reserve_kind("secondary", None), Some("weekly"));
        assert_eq!(reserve_kind("primary", None), Some("session"));
        assert_eq!(reserve_kind("premium:member", None), None);
    }
}
