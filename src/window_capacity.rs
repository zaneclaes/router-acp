//! Each account's measured plan-window size in dollars, kept across resets.
//!
//! Providers report plan windows only as percents, so a host comparing
//! accounts ("how much of ALL my Claude session capacity is used?") needs
//! each account's size. The router meters its own spend per account, so
//! `spent ÷ (percent/100)` estimates a window's dollar capacity — the same
//! signal `usage::window_remaining_dollars` ranks with, and behind the same
//! guards. Early in a window the guards return nothing, so the last good
//! measurement per window kind is cached: a plan's window size is stable
//! across resets, so it stays valid until the account itself changes.
//!
//! Published contract, read by `account-status` (and through it the Kory
//! Code relay): `~/.local/state/router-acp/usage/capacity/<agent>.json`
//!
//! ```json
//! { "account": "<snapshot account fingerprint>",
//!   "session": { "dollars": 41.2, "measured_at": 1753142400 },
//!   "weekly":  { "dollars": 820.0, "measured_at": 1753142400 } }
//! ```
//!
//! Kinds follow the reserve rule: windows a day or longer are `weekly`,
//! shorter ones `session`. Only whole-seat windows count (a model-scoped
//! weekly cap is not the seat's size). Known bias: spend outside the router
//! raises the percent without raising metered spend, so an account also used
//! elsewhere measures smaller than it is.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{AgentConfig, UsageSourceConfig};
use crate::usage::SpendLookup;

const WEEKLY_MIN_MINUTES: u64 = 24 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Measurement {
    pub dollars: f64,
    pub measured_at: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Capacity {
    pub account: String,
    pub session: Option<Measurement>,
    pub weekly: Option<Measurement>,
}

/// One whole-seat window: its kind, percent used, and start.
struct Window {
    weekly: bool,
    percent: f64,
    start: SystemTime,
}

fn anthropic_windows(payload: &Value, now: SystemTime) -> Vec<Window> {
    let Some(limits) = payload.get("limits").and_then(Value::as_array) else {
        return Vec::new();
    };
    limits
        .iter()
        .filter_map(|lim| {
            let (weekly, duration) = match lim.get("kind").and_then(Value::as_str)? {
                "session" => (false, Duration::from_secs(5 * 3600)),
                "weekly_all" => (true, Duration::from_secs(7 * 86_400)),
                _ => return None,
            };
            let resets_at = lim
                .get("resets_at")
                .and_then(Value::as_str)
                .and_then(crate::limits::parse_reset_timestamp)
                .filter(|reset| *reset > now)?;
            Some(Window {
                weekly,
                percent: lim.get("percent").and_then(Value::as_f64)?,
                start: resets_at.checked_sub(duration)?,
            })
        })
        .collect()
}

fn codex_windows(payload: &Value, now: SystemTime) -> Vec<Window> {
    let field = |v: &Value, snake: &str, camel: &str| {
        v.get(snake)
            .filter(|x| !x.is_null())
            .or_else(|| v.get(camel).filter(|x| !x.is_null()))
            .and_then(Value::as_f64)
    };
    crate::usage::codex_pools_from_payload(payload)
        .iter()
        .flat_map(|pool| ["primary", "secondary"].map(|key| pool.get(key).cloned()))
        .flatten()
        .filter_map(|win| {
            let resets_at = SystemTime::UNIX_EPOCH
                + Duration::from_secs(field(&win, "resets_at", "resetsAt")? as u64);
            let minutes = field(&win, "window_minutes", "windowDurationMins")? as u64;
            (resets_at > now).then_some(())?;
            Some(Window {
                weekly: minutes >= WEEKLY_MIN_MINUTES,
                percent: field(&win, "used_percent", "usedPercent")?,
                start: resets_at.checked_sub(Duration::from_secs(minutes * 60))?,
            })
        })
        .collect()
}

/// Measure each kind from its binding (fullest) window — the one with the
/// most signal — and fold the results over the last good reading. A changed
/// account fingerprint discards the old readings: they sized another seat.
pub fn measure(
    previous: Option<Capacity>,
    account: &str,
    windows: &[(bool, f64, SystemTime)],
    spend: &SpendLookup,
    now: SystemTime,
) -> Capacity {
    let mut out = previous
        .filter(|p| p.account == account)
        .unwrap_or_else(|| Capacity {
            account: account.to_string(),
            ..Default::default()
        });
    let measured_at = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    for weekly in [false, true] {
        let Some(&(_, percent, start)) = windows
            .iter()
            .filter(|w| w.0 == weekly)
            .max_by(|a, b| a.1.total_cmp(&b.1))
        else {
            continue;
        };
        let Some(dollars) = spend(None, start).and_then(|spent| {
            crate::usage::window_remaining_dollars(spent, percent).map(|left| spent + left)
        }) else {
            continue;
        };
        let slot = if weekly {
            &mut out.weekly
        } else {
            &mut out.session
        };
        *slot = Some(Measurement {
            dollars,
            measured_at,
        });
    }
    out
}

fn path(agent: &str) -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(
        PathBuf::from(home)
            .join(".local/state/router-acp/usage/capacity")
            .join(format!("{agent}.json")),
    )
}

/// The cached capacity for `agent`, if it was measured on `account`.
pub fn read(agent: &str, account: &str) -> Option<Capacity> {
    let text = std::fs::read_to_string(path(agent)?).ok()?;
    serde_json::from_str::<Capacity>(&text)
        .ok()
        .filter(|c| c.account == account)
}

/// Re-measure `agent` from its published usage snapshot and the router's
/// metered spend; writes only when a reading changed. Fails open.
pub fn record(agent: &AgentConfig, spend: &SpendLookup, now: SystemTime) {
    let Some(snapshot) = crate::usage_cache::read_agent_snapshot(agent) else {
        return;
    };
    let Some(payload) = snapshot.payload.as_ref() else {
        return;
    };
    let windows = match agent.usage_source.as_ref() {
        Some(UsageSourceConfig::AnthropicOauth) => anthropic_windows(payload, now),
        Some(UsageSourceConfig::CodexRollout) => codex_windows(payload, now),
        None => return,
    };
    let windows: Vec<_> = windows
        .iter()
        .map(|w| (w.weekly, w.percent, w.start))
        .collect();
    let previous = read(&agent.name, &snapshot.account);
    let next = measure(previous.clone(), &snapshot.account, &windows, spend, now);
    if previous.as_ref() == Some(&next) || (next.session.is_none() && next.weekly.is_none()) {
        return;
    }
    let Some(target) = path(&agent.name) else {
        return;
    };
    let write = || -> std::io::Result<()> {
        let dir = target.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
        std::fs::create_dir_all(dir)?;
        let tmp = dir.join(format!(".{}.tmp.{}", agent.name, std::process::id()));
        std::fs::write(&tmp, serde_json::to_vec_pretty(&next)?)?;
        std::fs::rename(&tmp, &target)
    };
    if let Err(err) = write() {
        tracing::debug!(agent = %agent.name, %err, "could not cache window capacity");
    }
}

/// `account-status` view: `{session, weekly}`, each `{dollars, measuredAt}`
/// or null.
pub fn status_json(capacity: Option<&Capacity>) -> Value {
    let one = |m: Option<&Measurement>| {
        m.map(|m| {
            serde_json::json!({"dollars": m.dollars,
                "measuredAt": chrono::DateTime::from_timestamp(m.measured_at as i64, 0).map(|t| t.to_rfc3339())})
        })
    };
    serde_json::json!({
        "session": one(capacity.and_then(|c| c.session.as_ref())),
        "weekly": one(capacity.and_then(|c| c.weekly.as_ref())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn measures_capacity_from_spend_and_percent() {
        let spend: &SpendLookup = &|_, _| Some(10.0);
        let c = measure(None, "a", &[(false, 25.0, at(0))], spend, at(100));
        assert_eq!(
            c.session,
            Some(Measurement {
                dollars: 40.0,
                measured_at: 100
            })
        );
        assert_eq!(c.weekly, None);
    }

    #[test]
    fn keeps_last_good_reading_below_the_signal_guards() {
        let good: &SpendLookup = &|_, _| Some(10.0);
        let first = measure(None, "a", &[(true, 50.0, at(0))], good, at(100));
        // A fresh window: 3% used, pennies spent — no estimate, keep the old one.
        let early: &SpendLookup = &|_, _| Some(0.1);
        let next = measure(
            Some(first.clone()),
            "a",
            &[(true, 3.0, at(200))],
            early,
            at(300),
        );
        assert_eq!(next.weekly, first.weekly);
    }

    #[test]
    fn a_different_account_discards_old_readings() {
        let spend: &SpendLookup = &|_, _| Some(10.0);
        let first = measure(None, "a", &[(false, 50.0, at(0))], spend, at(100));
        let none: &SpendLookup = &|_, _| None;
        let next = measure(Some(first), "b", &[], none, at(200));
        assert_eq!(
            next,
            Capacity {
                account: "b".into(),
                ..Default::default()
            }
        );
    }

    #[test]
    fn measures_from_the_fullest_window_of_each_kind() {
        let spend: &SpendLookup = &|_, since| Some(if since == at(1) { 30.0 } else { 1.0 });
        let windows = [(true, 20.0, at(0)), (true, 60.0, at(1))];
        let c = measure(None, "a", &windows, spend, at(100));
        assert_eq!(c.weekly.map(|m| m.dollars), Some(50.0));
    }

    #[test]
    fn reads_whole_seat_anthropic_windows_only() {
        let payload = json!({"limits": [
            {"kind": "session", "percent": 40.0, "resets_at": "2025-07-22T05:00:00Z"},
            {"kind": "weekly_all", "percent": 30.0, "resets_at": "2025-07-25T00:00:00Z"},
            {"kind": "weekly_scoped", "percent": 90.0, "resets_at": "2025-07-25T00:00:00Z",
             "scope": {"model": {"id": "fable"}}},
            {"kind": "session", "percent": 99.0, "resets_at": "2020-01-01T00:00:00Z"}
        ]});
        let now = crate::limits::parse_reset_timestamp("2025-07-22T01:00:00Z").unwrap();
        let w = anthropic_windows(&payload, now);
        let kinds: Vec<_> = w.iter().map(|w| (w.weekly, w.percent)).collect();
        assert_eq!(kinds, vec![(false, 40.0), (true, 30.0)]);
        assert_eq!(
            w[0].start,
            crate::limits::parse_reset_timestamp("2025-07-22T00:00:00Z").unwrap()
        );
    }

    #[test]
    fn reads_codex_windows_by_length() {
        let payload = json!({"rateLimits": {
            "primary": {"usedPercent": 10.0, "windowDurationMins": 300, "resetsAt": 2000},
            "secondary": {"usedPercent": 40.0, "windowDurationMins": 10080, "resetsAt": 700_000}
        }});
        let w = codex_windows(&payload, at(1000));
        let kinds: Vec<_> = w.iter().map(|w| (w.weekly, w.percent)).collect();
        assert_eq!(kinds, vec![(false, 10.0), (true, 40.0)]);
        assert_eq!(w[1].start, at(700_000 - 10080 * 60));
    }

    #[test]
    fn status_json_reports_null_until_measured() {
        let c = Capacity {
            account: "a".into(),
            session: Some(Measurement {
                dollars: 12.5,
                measured_at: 0,
            }),
            weekly: None,
        };
        assert_eq!(
            status_json(Some(&c)),
            json!({"session": {"dollars": 12.5, "measuredAt": "1970-01-01T00:00:00+00:00"}, "weekly": null})
        );
        assert_eq!(status_json(None), json!({"session": null, "weekly": null}));
    }
}
