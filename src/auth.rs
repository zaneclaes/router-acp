//! Agent-level authentication availability.
//!
//! Authentication belongs to a provider seat, not an individual model. The
//! router refreshes all configured probes concurrently before selection and
//! merges those results with authenticated usage reads and reactive ACP
//! failures. Only definite evidence changes eligibility; errors fail open.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::AuthProbeConfig;
use crate::session::Shared;

/// How long a refresh stays fresh. One routing pipeline runs several selection
/// points (session/new, pre-classification, pin, dispatch); this keeps them on
/// a single set of probe results instead of re-spawning CLIs per decision.
const REFRESH_TTL: Duration = Duration::from_secs(5);

/// Reactive negatives decay. An agent with no configured probe is only ever
/// marked unauthenticated by a runtime ACP rejection, and nothing observes the
/// out-of-band `login` that fixes it — without decay it would stay dead for the
/// process lifetime. Probed agents re-assert their state every `REFRESH_TTL`,
/// so the decay never loosens them.
const NEGATIVE_TTL: Duration = Duration::from_secs(900);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthAvailability {
    Authenticated,
    Unauthenticated { reason: String },
    Unknown,
}

#[derive(Debug, Clone)]
struct Entry {
    availability: AuthAvailability,
    at: Instant,
}

#[derive(Debug, Default)]
pub struct AuthTracker {
    agents: HashMap<String, Entry>,
    last_refresh: Option<Instant>,
}

impl AuthTracker {
    pub fn availability(&self, agent: &str) -> AuthAvailability {
        let Some(entry) = self.agents.get(agent) else {
            return AuthAvailability::Unknown;
        };
        match &entry.availability {
            AuthAvailability::Unauthenticated { .. } if entry.at.elapsed() >= NEGATIVE_TTL => {
                AuthAvailability::Unknown
            }
            other => other.clone(),
        }
    }

    pub fn set(&mut self, agent: &str, availability: AuthAvailability) {
        if availability == AuthAvailability::Unknown {
            return;
        }
        self.agents.insert(
            agent.to_string(),
            Entry {
                availability,
                at: Instant::now(),
            },
        );
    }

    pub fn unauthenticated(&self, agent: &str) -> Option<String> {
        match self.availability(agent) {
            AuthAvailability::Unauthenticated { reason } => Some(reason),
            _ => None,
        }
    }

    /// Was this agent rejected by evidence recorded at or after `since`?
    fn rejected_since(&self, agent: &str, since: Instant) -> bool {
        self.agents.get(agent).is_some_and(|entry| {
            entry.at >= since
                && matches!(entry.availability, AuthAvailability::Unauthenticated { .. })
        })
    }

    fn refresh_due(&self) -> bool {
        self.last_refresh
            .is_none_or(|at| at.elapsed() >= REFRESH_TTL)
    }

    fn mark_refreshed(&mut self) {
        self.last_refresh = Some(Instant::now());
    }
}

pub async fn refresh_before_selection(shared: &Arc<Shared>) {
    if !shared.auth.lock().unwrap().refresh_due() {
        return;
    }
    let cycle_start = Instant::now();
    let probes: Vec<_> = shared
        .cfg
        .agents
        .iter()
        .filter_map(|agent| {
            agent
                .auth_probe
                .clone()
                .map(|probe| (agent.name.clone(), probe))
        })
        .collect();
    let probe_results = futures::future::join_all(
        probes
            .into_iter()
            .map(|(agent, probe)| async move { (agent, run_probe(&probe).await) }),
    );
    let (results, ()) = tokio::join!(probe_results, crate::usage::refresh_and_install(shared));
    let mut tracker = shared.auth.lock().unwrap();
    for (agent, result) in results {
        // Evidence precedence within one cycle: an authenticated read that came
        // back with an explicit credential rejection outranks a status command
        // that merely exited zero. Negatives never lose to a same-cycle probe.
        if result == AuthAvailability::Authenticated && tracker.rejected_since(&agent, cycle_start)
        {
            continue;
        }
        tracker.set(&agent, result);
    }
    tracker.mark_refreshed();
}

async fn run_probe(probe: &AuthProbeConfig) -> AuthAvailability {
    let mut cmd = tokio::process::Command::new(&probe.command);
    cmd.args(&probe.args).kill_on_drop(true);
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(probe.timeout_ms),
        cmd.output(),
    )
    .await;
    let Ok(Ok(output)) = result else {
        return AuthAvailability::Unknown;
    };
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .to_lowercase();
    if probe
        .unauthenticated_patterns
        .iter()
        .any(|pattern| text.contains(&pattern.to_lowercase()))
    {
        return AuthAvailability::Unauthenticated {
            reason: "provider is not signed in".to_string(),
        };
    }
    if output.status.success() {
        return AuthAvailability::Authenticated;
    }
    AuthAvailability::Unknown
}

pub fn error_is_auth_rejection(text: &str) -> bool {
    let lower = text.to_lowercase();
    [
        "authentication required",
        "not authenticated",
        "not logged in",
        "not signed in",
        "login required",
        "please sign in",
        "please log in",
        "unauthorized",
        "http 401",
        "error: 401",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern))
}

pub fn note_authenticated(tracker: &Mutex<AuthTracker>, agent: &str) {
    tracker
        .lock()
        .unwrap()
        .set(agent, AuthAvailability::Authenticated);
}

pub fn note_unauthenticated(tracker: &Mutex<AuthTracker>, agent: &str, reason: impl Into<String>) {
    tracker.lock().unwrap().set(
        agent,
        AuthAvailability::Unauthenticated {
            reason: reason.into(),
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_does_not_erase_definite_state() {
        let mut tracker = AuthTracker::default();
        tracker.set("a", AuthAvailability::Authenticated);
        tracker.set("a", AuthAvailability::Unknown);
        assert_eq!(tracker.availability("a"), AuthAvailability::Authenticated);
    }

    #[test]
    fn authenticating_clears_a_prior_rejection() {
        let mut tracker = AuthTracker::default();
        tracker.set(
            "a",
            AuthAvailability::Unauthenticated {
                reason: "a is not signed in".to_string(),
            },
        );
        assert!(tracker.unauthenticated("a").is_some());
        tracker.set("a", AuthAvailability::Authenticated);
        assert_eq!(tracker.availability("a"), AuthAvailability::Authenticated);
    }

    #[test]
    fn a_stale_negative_decays_to_unknown() {
        let mut tracker = AuthTracker::default();
        tracker.set(
            "a",
            AuthAvailability::Unauthenticated {
                reason: "a is not signed in".to_string(),
            },
        );
        assert!(tracker.unauthenticated("a").is_some());
        tracker.agents.get_mut("a").unwrap().at -= NEGATIVE_TTL;
        assert_eq!(tracker.availability("a"), AuthAvailability::Unknown);
    }

    #[test]
    fn a_same_cycle_rejection_outranks_a_probe_success() {
        let mut tracker = AuthTracker::default();
        let cycle_start = Instant::now();
        tracker.set(
            "a",
            AuthAvailability::Unauthenticated {
                reason: "401".to_string(),
            },
        );
        assert!(tracker.rejected_since("a", cycle_start));
        // ...while a rejection recorded before the cycle does not block one.
        assert!(!tracker.rejected_since("a", Instant::now()));
    }

    #[tokio::test]
    async fn probe_is_tri_state_and_fail_open() {
        let cfg = |args: &[&str]| AuthProbeConfig {
            command: "/bin/sh".to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            timeout_ms: 100,
            unauthenticated_patterns: vec!["not signed in".to_string()],
        };
        assert_eq!(
            run_probe(&cfg(&["-c", "exit 0"])).await,
            AuthAvailability::Authenticated
        );
        assert!(matches!(
            run_probe(&cfg(&["-c", "echo not signed in >&2; exit 1"])).await,
            AuthAvailability::Unauthenticated { .. }
        ));
        assert_eq!(
            run_probe(&cfg(&["-c", "echo network error >&2; exit 1"])).await,
            AuthAvailability::Unknown
        );
        assert_eq!(
            run_probe(&cfg(&["-c", "sleep 1"])).await,
            AuthAvailability::Unknown
        );
    }
}
