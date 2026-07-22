//! Cross-process shared cache of the Anthropic plan-usage snapshot.
//!
//! Every Kory Code conversation spawns its own `router-acp serve`, and each
//! process used to poll `api.anthropic.com/api/oauth/usage` independently —
//! four-plus pollers (plus the relay's own poll) against one per-account
//! budget is exactly how that endpoint starts returning 429. This module makes
//! the snapshot a box-wide resource: one JSON file under the state directory
//! that every process reads, refreshed by whichever process gets there first,
//! at most once per `cordon.min_refresh_secs` (default 60s) box-wide. The file
//! is a published contract — the Kory Code relay reads it instead of fetching
//! upstream itself — so its schema and semantics must stay stable:
//!
//! ```json
//! {
//!   "source": "anthropic-oauth",
//!   "account": "<sha256 hex of the OAuth refresh (or access) token>",
//!   "fetched_at": 1753142400,
//!   "attempted_at": 1753142460,
//!   "consecutive_failures": 0,
//!   "last_error": null,
//!   "payload": {}
//! }
//! ```
//!
//! `fetched_at` is the unix time of the last SUCCESSFUL fetch (which produced
//! `payload`); `attempted_at` covers any outcome; `last_error` is e.g.
//! "HTTP 429" or a curl error, null on success; `payload` is the raw
//! usage-endpoint JSON, null if never fetched.
//!
//! Coordination is deliberately primitive (no daemon, no IPC): atomic
//! write-temp-then-rename for the snapshot, an `O_EXCL` lockfile as the
//! stampede guard, and exponential failure backoff recorded *in* the file so
//! all processes observe the same state. Everything fails open — any
//! filesystem or fetch error degrades to "use whatever payload we last had",
//! and cordon computation is stale-safe because it keys off absolute
//! `resets_at` timestamps.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Floor between upstream fetch attempts, box-wide (the default for
/// `cordon.min_refresh_secs`).
pub const DEFAULT_MIN_REFRESH_SECS: u64 = 60;
/// Ceiling on failure backoff: never wait longer than this to retry.
const MAX_BACKOFF_SECS: u64 = 900;
/// A lockfile older than this is a leak from a dead process — break it.
const LOCK_STALE_SECS: u64 = 60;

const SOURCE: &str = "anthropic-oauth";

/// The on-disk snapshot — see the module doc for field semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub source: String,
    pub account: String,
    pub fetched_at: u64,
    pub attempted_at: u64,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub payload: Option<Value>,
}

// ----------------------------------------------------------------------
// Refresh policy (pure — the unit-tested core)
// ----------------------------------------------------------------------

/// Seconds after `attempted_at` before the next fetch is allowed:
/// `min_refresh * 2^consecutive_failures`, capped at [`MAX_BACKOFF_SECS`]
/// (the cap never cuts below `min_refresh` itself).
pub fn next_attempt_wait(min_refresh: u64, consecutive_failures: u32) -> u64 {
    let mult = 1u64
        .checked_shl(consecutive_failures.min(63))
        .unwrap_or(u64::MAX);
    min_refresh
        .saturating_mul(mult)
        .min(MAX_BACKOFF_SECS.max(min_refresh))
}

/// Whether to hit the upstream endpoint now, given the cached state. A cache
/// for a different account is ignored entirely (payload AND backoff): the
/// credentials changed, so neither its data nor its failure history applies.
pub fn should_fetch(
    cache: Option<&Snapshot>,
    now: u64,
    fingerprint: &str,
    min_refresh: u64,
) -> bool {
    let Some(snap) = cache else {
        return true;
    };
    if snap.account != fingerprint {
        return true;
    }
    now >= snap
        .attempted_at
        .saturating_add(next_attempt_wait(min_refresh, snap.consecutive_failures))
}

/// Snapshot after a successful fetch: fresh payload, failure state cleared.
pub fn on_success(fingerprint: &str, now: u64, payload: Value) -> Snapshot {
    Snapshot {
        source: SOURCE.to_string(),
        account: fingerprint.to_string(),
        fetched_at: now,
        attempted_at: now,
        consecutive_failures: 0,
        last_error: None,
        payload: Some(payload),
    }
}

/// Snapshot after a failed fetch: backoff advances and the error is recorded,
/// but the last-good `payload`/`fetched_at` are preserved (same account). A
/// failure right after an account swap starts a fresh record — the old
/// account's payload must not be served under the new fingerprint.
pub fn on_failure(prev: Option<Snapshot>, fingerprint: &str, now: u64, error: String) -> Snapshot {
    match prev {
        Some(p) if p.account == fingerprint => Snapshot {
            attempted_at: now,
            consecutive_failures: p.consecutive_failures.saturating_add(1),
            last_error: Some(error),
            ..p
        },
        _ => Snapshot {
            source: SOURCE.to_string(),
            account: fingerprint.to_string(),
            fetched_at: 0,
            attempted_at: now,
            consecutive_failures: 1,
            last_error: Some(error),
            payload: None,
        },
    }
}

/// sha256 hex of an OAuth token — the account fingerprint. Byte-matches
/// Node's `createHash("sha256").update(s).digest("hex")` so the relay and the
/// router agree on account identity.
pub fn fingerprint(secret: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(secret.as_bytes());
    h.finalize()
        .iter()
        .fold(String::with_capacity(64), |mut out, b| {
            use std::fmt::Write;
            let _ = write!(out, "{b:02x}");
            out
        })
}

// ----------------------------------------------------------------------
// Snapshot file I/O
// ----------------------------------------------------------------------

/// The published snapshot location: `~/.local/state/router-acp/usage/…` —
/// alongside `sessions.db`. Fixed (not derived from `state_file`) because the
/// relay reads this exact path regardless of any per-process router config.
fn snapshot_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(Path::new(&home).join(".local/state/router-acp/usage/anthropic-oauth.json"))
}

pub fn read_snapshot(path: &Path) -> Option<Snapshot> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

/// Atomic write: temp file in the same directory, then rename over the
/// target — readers never observe a partial snapshot.
pub fn write_snapshot(path: &Path, snap: &Snapshot) -> std::io::Result<()> {
    let dir = path
        .parent()
        .ok_or(std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("snap"),
        std::process::id()
    ));
    std::fs::write(&tmp, serde_json::to_vec_pretty(snap)?)?;
    std::fs::rename(&tmp, path)
}

// ----------------------------------------------------------------------
// Cross-process stampede guard
// ----------------------------------------------------------------------

/// Holds the lockfile together with the unique token this owner wrote into
/// it. Drop unlinks the file only while it still contains our token — after a
/// staleness break, the path may belong to a NEW owner, and deleting their
/// lock would admit extra concurrent fetchers.
struct LockGuard {
    path: PathBuf,
    token: String,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if std::fs::read_to_string(&self.path).is_ok_and(|t| t == self.token) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Try to take the fetch lock. `None` means another process is fetching right
/// now — use the cache as-is. A lockfile whose mtime is older than
/// `stale_after` was leaked by a dead process (a live fetch is bounded by
/// curl's 20s max-time, well under the 60s threshold). Breaking one is
/// ownership-safe: the breaker atomically RENAMES it to a unique scratch name
/// — once the source is gone every other contender's rename fails, so exactly
/// one breaker wins — deletes the renamed file, and retries the normal
/// `O_EXCL` create. A lock we didn't create is never bare-unlinked.
fn acquire_lock(path: &Path, now: SystemTime, stale_after: Duration) -> Option<LockGuard> {
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let token = uuid::Uuid::new_v4().to_string();
    for attempt in 0..2 {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(mut f) => {
                use std::io::Write;
                // Best-effort: a lock whose token failed to write can't be
                // Drop-unlinked; the staleness break reaps it instead.
                let _ = f.write_all(token.as_bytes());
                return Some(LockGuard {
                    path: path.to_path_buf(),
                    token,
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && attempt == 0 => {
                let stale = std::fs::metadata(path)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|mtime| now.duration_since(mtime).ok())
                    .is_some_and(|age| age > stale_after);
                if !stale {
                    return None;
                }
                // Rename-claim: a losing contender's rename fails (source
                // already gone) and it simply falls through to the create
                // retry — someone owns the lock either way.
                let scratch = path.with_file_name(format!(
                    "{}.broken.{token}",
                    path.file_name().and_then(|n| n.to_str()).unwrap_or("lock")
                ));
                if std::fs::rename(path, &scratch).is_ok() {
                    let _ = std::fs::remove_file(&scratch);
                }
                // retry the create_new once
            }
            Err(_) => return None,
        }
    }
    None
}

// ----------------------------------------------------------------------
// Cache-mediated fetch (the one entry point)
// ----------------------------------------------------------------------

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Curl's fail mode reports HTTP errors as e.g. "The requested URL returned
/// error: 429" on stderr (exit 22) — surface the interesting one distinctly.
fn classify_error(err: &str) -> String {
    if err.contains("429") {
        "HTTP 429".to_string()
    } else {
        err.to_string()
    }
}

/// The cache-mediated usage read — the box-wide rate limiter. Returns the
/// freshest payload available (possibly stale on failure or lock contention),
/// or `None` when there's nothing usable (fail open, as before).
pub async fn cached_anthropic_usage(min_refresh_secs: u64) -> Option<Value> {
    let creds = crate::usage::anthropic_oauth_credentials()?;
    let fp = fingerprint(
        creds
            .refresh_token
            .as_deref()
            .unwrap_or(&creds.access_token),
    );
    let path = snapshot_path()?;
    let snap = read_snapshot(&path);
    if !should_fetch(snap.as_ref(), unix_now(), &fp, min_refresh_secs) {
        return snap.and_then(|s| s.payload);
    }
    let lock_path = path.with_extension("json.lock");
    let Some(_lock) = acquire_lock(
        &lock_path,
        SystemTime::now(),
        Duration::from_secs(LOCK_STALE_SECS),
    ) else {
        // Someone else is fetching; whatever the cache holds (even stale)
        // beats a second concurrent hit on the shared budget. A cached
        // payload for a *different* account is never served, though.
        tracing::debug!("usage fetch lock held by another process; using cached snapshot");
        return snap.filter(|s| s.account == fp).and_then(|s| s.payload);
    };
    // Re-read under the lock: another process may have completed its own
    // attempt between our first read and the lock acquisition. Its outcome —
    // not our pre-lock view — is what the fetch decision and any failure
    // merge must run against; otherwise we'd fetch redundantly, and a failure
    // here could overwrite a sibling's fresher success.
    let snap = read_snapshot(&path);
    if !should_fetch(snap.as_ref(), unix_now(), &fp, min_refresh_secs) {
        return snap.and_then(|s| s.payload);
    }
    match crate::usage::fetch_anthropic_usage(&creds.access_token).await {
        Ok(payload) => {
            let s = on_success(&fp, unix_now(), payload);
            if let Err(e) = write_snapshot(&path, &s) {
                tracing::debug!(error = %e, "cannot persist usage snapshot");
            }
            s.payload
        }
        Err(err) => {
            tracing::debug!(%err, "usage fetch failed; keeping last-good snapshot");
            let s = on_failure(snap, &fp, unix_now(), classify_error(&err));
            if let Err(e) = write_snapshot(&path, &s) {
                tracing::debug!(error = %e, "cannot persist usage snapshot");
            }
            s.payload
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snap(account: &str, attempted_at: u64, failures: u32) -> Snapshot {
        Snapshot {
            source: SOURCE.to_string(),
            account: account.to_string(),
            fetched_at: attempted_at,
            attempted_at,
            consecutive_failures: failures,
            last_error: None,
            payload: Some(json!({"limits": []})),
        }
    }

    #[test]
    fn fingerprint_matches_node_sha256_hex() {
        assert_eq!(
            fingerprint("test"),
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }

    #[test]
    fn snapshot_round_trips_through_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("usage/anthropic-oauth.json");
        let s = Snapshot {
            last_error: Some("HTTP 429".into()),
            consecutive_failures: 2,
            ..snap("abc", 1_753_142_400, 0)
        };
        write_snapshot(&path, &s).unwrap();
        let r = read_snapshot(&path).unwrap();
        assert_eq!(r.source, "anthropic-oauth");
        assert_eq!(r.account, "abc");
        assert_eq!(r.fetched_at, 1_753_142_400);
        assert_eq!(r.consecutive_failures, 2);
        assert_eq!(r.last_error.as_deref(), Some("HTTP 429"));
        assert_eq!(r.payload, Some(json!({"limits": []})));
        // No temp file left behind after the rename.
        let leftovers: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name()))
            .collect();
        assert_eq!(leftovers, ["anthropic-oauth.json"]);
    }

    #[test]
    fn missing_or_corrupt_file_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.json");
        assert!(read_snapshot(&path).is_none());
        std::fs::write(&path, "{not json").unwrap();
        assert!(read_snapshot(&path).is_none());
    }

    #[test]
    fn fresh_attempt_skips_fetch_stale_fetches() {
        let s = snap("fp", 1000, 0);
        assert!(!should_fetch(Some(&s), 1000 + 59, "fp", 60), "fresh");
        assert!(should_fetch(Some(&s), 1000 + 60, "fp", 60), "stale");
        assert!(should_fetch(None, 0, "fp", 60), "no cache");
    }

    #[test]
    fn failure_backoff_grows_exponentially_and_caps() {
        assert_eq!(next_attempt_wait(60, 0), 60);
        assert_eq!(next_attempt_wait(60, 1), 120);
        assert_eq!(next_attempt_wait(60, 2), 240);
        assert_eq!(next_attempt_wait(60, 3), 480);
        assert_eq!(next_attempt_wait(60, 4), 900, "960 capped to 900");
        assert_eq!(next_attempt_wait(60, 63), 900, "huge shift still capped");
        // The cap never cuts below the configured floor itself.
        assert_eq!(next_attempt_wait(1200, 0), 1200);

        let s = snap("fp", 1000, 3); // wait = 480
        assert!(!should_fetch(Some(&s), 1000 + 479, "fp", 60));
        assert!(should_fetch(Some(&s), 1000 + 480, "fp", 60));
    }

    #[test]
    fn failure_preserves_last_good_payload() {
        let good = on_success("fp", 1000, json!({"limits": [1]}));
        let failed = on_failure(Some(good), "fp", 1060, "HTTP 429".into());
        assert_eq!(failed.payload, Some(json!({"limits": [1]})), "payload kept");
        assert_eq!(failed.fetched_at, 1000, "success time kept");
        assert_eq!(failed.attempted_at, 1060);
        assert_eq!(failed.consecutive_failures, 1);
        assert_eq!(failed.last_error.as_deref(), Some("HTTP 429"));

        let failed2 = on_failure(Some(failed), "fp", 1180, "curl: timeout".into());
        assert_eq!(failed2.consecutive_failures, 2);
        assert_eq!(failed2.payload, Some(json!({"limits": [1]})));

        let recovered = on_success("fp", 1300, json!({"limits": [2]}));
        assert_eq!(recovered.consecutive_failures, 0);
        assert!(recovered.last_error.is_none());
    }

    #[test]
    fn account_swap_busts_cache_and_backoff() {
        // Deep in backoff for the old account…
        let s = snap("old-account", u64::MAX - 1000, 10);
        // …but a new fingerprint fetches immediately.
        assert!(should_fetch(Some(&s), 1000, "new-account", 60));
        // And a failure under the new account does not inherit the old
        // payload or failure count.
        let f = on_failure(Some(s), "new-account", 1000, "boom".into());
        assert_eq!(f.account, "new-account");
        assert_eq!(f.consecutive_failures, 1);
        assert!(f.payload.is_none());
        assert_eq!(f.fetched_at, 0);
    }

    #[test]
    fn recheck_under_lock_flips_to_no_fetch_and_merges_fresh_state() {
        // Pre-lock view says fetch…
        let stale = snap("fp", 1000, 0);
        assert!(should_fetch(Some(&stale), 2000, "fp", 60));
        // …but another process refreshed while we waited for the lock; the
        // decision recomputed against the re-read snapshot flips to no-fetch.
        let refreshed = on_success("fp", 1995, json!({"limits": [9]}));
        assert!(!should_fetch(Some(&refreshed), 2000, "fp", 60));
        // And a failure merged against the re-read state preserves the
        // sibling's fresh success, not the pre-lock stale view.
        let failed = on_failure(Some(refreshed), "fp", 2100, "HTTP 429".into());
        assert_eq!(failed.payload, Some(json!({"limits": [9]})));
        assert_eq!(failed.fetched_at, 1995, "sibling's success time kept");
    }

    fn set_mtime(path: &Path, t: SystemTime) {
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(t)
            .unwrap();
    }

    #[test]
    fn stale_break_is_ownership_safe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("usage.json.lock");
        let now = SystemTime::now();
        let stale_after = Duration::from_secs(60);

        // A dead process's stale lock is claimed by the first contender via
        // rename; the re-created lock carries the claimer's own token.
        std::fs::write(&path, b"dead-owner-token").unwrap();
        set_mtime(&path, now - Duration::from_secs(120));
        let guard = acquire_lock(&path, now, stale_after).expect("stale lock claimed");
        let content = std::fs::read_to_string(&path).unwrap();
        assert_ne!(content, "dead-owner-token", "lock re-created by claimer");
        assert!(!content.is_empty(), "claimer's token written");
        // The second contender sees a FRESH lock and must not break it.
        assert!(acquire_lock(&path, now, stale_after).is_none());
        // The rename-claim loser: once the winner renamed the stale lock
        // away, a rename of the same source fails — no second breaker.
        let scratch = dir.path().join("usage.json.lock.broken.loser");
        assert!(std::fs::rename(dir.path().join("usage.json.lock.gone"), &scratch).is_err());
        drop(guard);
        assert!(!path.exists(), "own token → drop unlinks");
    }

    #[test]
    fn drop_with_foreign_token_leaves_lock_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("usage.json.lock");
        let guard = acquire_lock(&path, SystemTime::now(), Duration::from_secs(60)).unwrap();
        // Another process claimed the path out from under us (e.g. after
        // judging our lock stale); our Drop must not delete their lock.
        std::fs::write(&path, b"new-owner-token").unwrap();
        drop(guard);
        assert!(path.exists(), "foreign lock preserved");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new-owner-token");
    }

    #[test]
    fn held_lock_skips_stale_lock_breaks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("usage.json.lock");
        let now = SystemTime::now();
        let stale_after = Duration::from_secs(60);

        // Free → acquired; while held, a second acquire is refused.
        let guard = acquire_lock(&path, now, stale_after).expect("acquire free lock");
        assert!(acquire_lock(&path, now, stale_after).is_none(), "held");
        drop(guard);
        assert!(!path.exists(), "guard removed the lockfile");

        // A leaked lock with an old mtime is broken and re-acquired.
        std::fs::write(&path, b"").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(now - Duration::from_secs(120))
            .unwrap();
        assert!(
            acquire_lock(&path, now, stale_after).is_some(),
            "stale lock broken"
        );
    }

    #[test]
    fn classify_error_surfaces_429() {
        assert_eq!(
            classify_error(
                "curl failed (exit status: 22): curl: (22) The requested URL returned error: 429"
            ),
            "HTTP 429"
        );
        assert_eq!(classify_error("curl: (28) timeout"), "curl: (28) timeout");
    }
}
