//! Failure classification for downstream errors: token/usage limits (with
//! reset-time extraction) vs outages vs everything else.
//!
//! Adapters phrase limits differently — Claude Code emits
//! `"Claude AI usage limit reached|1752526800"` (epoch seconds), Codex emits
//! `"Try again in 2 hours 30 minutes"`, HTTP-ish layers use
//! `retry-after: 120` or `resets_at` fields — so parsing is pattern-based
//! and every pattern is unit-tested. When a limit is hit but no reset time
//! can be parsed, callers fall back to `headroom.cordon_default_secs`.

use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_client_protocol::schema::v1::Error as AcpError;
use regex::Regex;

/// What kind of failure a downstream error represents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureClass {
    /// Token/usage/rate limit. `retry_after` is parsed from the error when
    /// the model reported its reset time.
    RateLimited { retry_after: Option<Duration> },
    /// The downstream is unreachable: process died, connection closed,
    /// request timed out, provider overloaded.
    Outage,
    /// Anything else (bad params, refusals, ...): surface to the caller.
    Other,
}

fn error_text(err: &AcpError) -> String {
    format!("{} {}", err.message, err.data.clone().unwrap_or_default())
}

/// Classify a downstream error.
pub fn classify_failure(err: &AcpError) -> FailureClass {
    let text = error_text(err).to_lowercase();
    if is_rate_limit_text(&text) {
        return FailureClass::RateLimited {
            retry_after: parse_reset_delay_at(&text, SystemTime::now()),
        };
    }
    if is_outage_text(&text) {
        return FailureClass::Outage;
    }
    FailureClass::Other
}

pub fn is_rate_limit_text(lower: &str) -> bool {
    lower.contains("rate limit")
        || lower.contains("rate-limit")
        || lower.contains("ratelimit")
        || lower.contains("usage limit")
        || lower.contains("quota")
        || lower.contains("429")
        || lower.contains("too many requests")
        || lower.contains("limit reached")
        || lower.contains("out of credits")
        || lower.contains("credits_exhausted")
}

pub fn is_outage_text(lower: &str) -> bool {
    lower.contains("process exited")
        || lower.contains("connection closed")
        || lower.contains("connection failed")
        || lower.contains("never received")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("overloaded")
        || lower.contains("unavailable")
        || lower.contains("peer disconnected")
        || lower.contains("broken pipe")
        || lower.contains("econnreset")
        || lower.contains("500")
        || lower.contains("502")
        || lower.contains("503")
        || lower.contains("529")
        || lower.contains("internal server error")
        || lower.contains("service is temporarily")
}

struct ResetPatterns {
    epoch_marked: Regex,
    epoch_pipe: Regex,
    retry_after_secs: Regex,
    duration_phrase: Regex,
    iso: Regex,
}

fn patterns() -> &'static ResetPatterns {
    static PATTERNS: OnceLock<ResetPatterns> = OnceLock::new();
    PATTERNS.get_or_init(|| ResetPatterns {
        // "resets at 1752526800", "reset_at: 1752526800", "try again at 1752526800"
        epoch_marked: Regex::new(
            r#"(?:resets?[ _]at|reset_at|try again at|available at)["':=\s]+(\d{10,13})"#,
        )
        .unwrap(),
        // Claude Code: "usage limit reached|1752526800"
        epoch_pipe: Regex::new(r"limit reached\s*\|\s*(\d{10,13})").unwrap(),
        // "retry-after: 120", "retry_after_seconds=120", "resets_in_seconds: 90"
        retry_after_secs: Regex::new(
            r#"(?:retry[-_ ]?after|resets?_?in)(?:[-_ ]?seconds?)?["':=\s]+(\d{1,7})(?:\s|$|[,."'}])"#,
        )
        .unwrap(),
        // "try again in 2 hours 30 minutes", "resets in 2h 5m", "in 45 seconds"
        duration_phrase: Regex::new(
            r"(?:in|after)\s+(?:about\s+|~\s*)?(?:(\d+)\s*(?:days?|d)\s*)?(?:(\d+)\s*(?:hours?|hrs?|h)\s*)?(?:(\d+)\s*(?:minutes?|mins?|m)\s*)?(?:(\d+)\s*(?:seconds?|secs?|s))?\b",
        )
        .unwrap(),
        // "resets at 2026-07-10T02:30:00Z"
        iso: Regex::new(
            r"(\d{4})-(\d{2})-(\d{2})[tT](\d{2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?(z|Z|[+-]\d{2}:?\d{2})?",
        )
        .unwrap(),
    })
}

/// Parse a reset delay out of rate-limit error text, relative to `now`.
pub fn parse_reset_delay_at(lower: &str, now: SystemTime) -> Option<Duration> {
    let p = patterns();
    let now_epoch = now.duration_since(UNIX_EPOCH).ok()?.as_secs();

    let from_epoch = |raw: &str| -> Option<Duration> {
        let mut value: u64 = raw.parse().ok()?;
        if raw.len() == 13 {
            value /= 1000; // milliseconds
        }
        // Reset time already in the past (or nonsense): minimal cordon so we
        // retry soon instead of forever.
        Some(Duration::from_secs(value.saturating_sub(now_epoch).max(30)))
    };

    if let Some(cap) = p.epoch_pipe.captures(lower) {
        return from_epoch(&cap[1]);
    }
    if let Some(cap) = p.epoch_marked.captures(lower) {
        return from_epoch(&cap[1]);
    }
    if let Some(cap) = p.retry_after_secs.captures(lower) {
        let secs: u64 = cap[1].parse().ok()?;
        return Some(Duration::from_secs(secs.max(30)));
    }
    // Duration phrase: require at least one component matched.
    for cap in p.duration_phrase.captures_iter(lower) {
        let days: u64 = cap
            .get(1)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        let hours: u64 = cap
            .get(2)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        let mins: u64 = cap
            .get(3)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        let secs: u64 = cap
            .get(4)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        let total = days * 86_400 + hours * 3_600 + mins * 60 + secs;
        if total > 0 {
            return Some(Duration::from_secs(total.max(30)));
        }
    }
    if let Some(cap) = p.iso.captures(lower)
        && let Some(epoch) = iso_to_epoch(&cap)
    {
        return Some(Duration::from_secs(epoch.saturating_sub(now_epoch).max(30)));
    }
    None
}

/// Convert a matched ISO-8601 timestamp to epoch seconds (UTC). Offsets are
/// honored; a missing zone designator is treated as UTC.
fn iso_to_epoch(cap: &regex::Captures<'_>) -> Option<u64> {
    let year: i64 = cap[1].parse().ok()?;
    let month: i64 = cap[2].parse().ok()?;
    let day: i64 = cap[3].parse().ok()?;
    let hour: i64 = cap[4].parse().ok()?;
    let minute: i64 = cap[5].parse().ok()?;
    let second: i64 = cap
        .get(6)
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(0);

    // Howard Hinnant's days_from_civil.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;

    let mut epoch = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if let Some(zone) = cap.get(7) {
        let z = zone.as_str();
        if z != "z" && z != "Z" {
            let sign = if z.starts_with('-') { -1 } else { 1 };
            let digits: String = z.chars().filter(|c| c.is_ascii_digit()).collect();
            if digits.len() == 4 {
                let oh: i64 = digits[0..2].parse().ok()?;
                let om: i64 = digits[2..4].parse().ok()?;
                epoch -= sign * (oh * 3_600 + om * 60);
            }
        }
    }
    u64::try_from(epoch).ok()
}

/// Human-friendly relative duration: `~2h05m`, `~3m`, `~45s`.
pub fn humanize(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 86_400 {
        format!("~{}d{:02}h", secs / 86_400, (secs % 86_400) / 3_600)
    } else if secs >= 3_600 {
        format!("~{}h{:02}m", secs / 3_600, (secs % 3_600) / 60)
    } else if secs >= 60 {
        format!("~{}m", secs / 60)
    } else {
        format!("~{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(now_epoch: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(now_epoch)
    }

    fn parse(text: &str, now_epoch: u64) -> Option<u64> {
        parse_reset_delay_at(&text.to_lowercase(), at(now_epoch)).map(|d| d.as_secs())
    }

    #[test]
    fn claude_code_pipe_epoch_format() {
        // "Claude AI usage limit reached|<epoch>"
        assert_eq!(
            parse("Claude AI usage limit reached|1752530400", 1_752_526_800),
            Some(3600)
        );
    }

    #[test]
    fn epoch_with_marker_and_milliseconds() {
        assert_eq!(parse("resets at 1752530400", 1_752_526_800), Some(3600));
        assert_eq!(
            parse("reset_at: 1752530400000", 1_752_526_800),
            Some(3600),
            "millisecond epochs are normalized"
        );
        assert_eq!(parse("try again at 1752530400", 1_752_526_800), Some(3600));
    }

    #[test]
    fn retry_after_seconds() {
        assert_eq!(parse("HTTP 429, retry-after: 120", 0), Some(120));
        assert_eq!(parse("retry_after_seconds=90 ", 0), Some(90));
        assert_eq!(parse("resets_in_seconds: 45,", 0), Some(45));
    }

    #[test]
    fn natural_language_durations() {
        assert_eq!(
            parse(
                "You've hit your usage limit. Try again in 2 hours 30 minutes.",
                0
            ),
            Some(9000)
        );
        assert_eq!(parse("rate limited, resets in 2h 5m", 0), Some(7500));
        assert_eq!(parse("try again in 45 seconds", 0), Some(45));
        assert_eq!(parse("try again in 3 minutes", 0), Some(180));
        assert_eq!(parse("try again in 1 day 2 hours", 0), Some(93_600));
    }

    #[test]
    fn iso_timestamps() {
        // 2026-07-10T02:30:00Z == epoch 1783650600
        let now = 1_783_650_600 - 600;
        assert_eq!(parse("resets at 2026-07-10T02:30:00Z", now), Some(600));
        // With a +02:00 offset the same wall time is two hours earlier in UTC.
        assert_eq!(
            parse("resets at 2026-07-10T02:30:00+02:00", now - 7200),
            Some(600)
        );
    }

    #[test]
    fn past_reset_times_clamp_to_minimum() {
        assert_eq!(
            parse("usage limit reached|1752526800", 1_752_530_000),
            Some(30)
        );
    }

    #[test]
    fn no_time_information_is_none() {
        assert_eq!(parse("rate limit exceeded, please slow down", 0), None);
    }

    #[test]
    fn classification() {
        let limit = AcpError::internal_error().data("Claude AI usage limit reached|1752530400");
        assert!(matches!(
            classify_failure(&limit),
            FailureClass::RateLimited {
                retry_after: Some(_)
            }
        ));

        let outage = AcpError::internal_error().data("downstream process exited with signal 9");
        assert_eq!(classify_failure(&outage), FailureClass::Outage);

        let other = AcpError::invalid_params().data("unknown config id");
        assert_eq!(classify_failure(&other), FailureClass::Other);
    }

    #[test]
    fn humanize_formats() {
        assert_eq!(humanize(Duration::from_secs(45)), "~45s");
        assert_eq!(humanize(Duration::from_secs(180)), "~3m");
        assert_eq!(humanize(Duration::from_secs(7500)), "~2h05m");
        assert_eq!(humanize(Duration::from_secs(90_000)), "~1d01h");
    }
}
