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
    /// Fingerprint of the access credential that produced this evidence.
    /// Probes and explicit ACP authenticate events have no generation.
    access_generation: Option<String>,
}

#[derive(Debug, Default)]
pub struct AuthTracker {
    agents: HashMap<String, Entry>,
    /// The generation most recently observed by a passive credential read.
    /// Generations are opaque fingerprints, so arrival order cannot be used
    /// to infer which one is newer.
    current_generations: HashMap<String, String>,
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
        // Generation-less evidence cannot replace evidence tied to a specific
        // access credential. Explicit ACP authenticate uses force_set below.
        if self
            .agents
            .get(agent)
            .is_some_and(|entry| entry.access_generation.is_some())
        {
            return;
        }
        self.force_set(agent, availability);
    }

    fn force_set(&mut self, agent: &str, availability: AuthAvailability) {
        self.agents.insert(
            agent.to_string(),
            Entry {
                availability,
                at: Instant::now(),
                access_generation: None,
            },
        );
    }

    /// Record the generation currently present on disk. A changed generation
    /// starts with unknown auth state; its predecessor must not remain live.
    pub fn observe_generation(&mut self, agent: &str, access_generation: &str) {
        let changed = self
            .current_generations
            .get(agent)
            .is_none_or(|current| current != access_generation);
        self.current_generations
            .insert(agent.to_string(), access_generation.to_string());
        if changed
            && self
                .agents
                .get(agent)
                .is_some_and(|entry| entry.access_generation.as_deref() != Some(access_generation))
        {
            self.agents.remove(agent);
        }
    }

    /// Clear the live credential marker and any auth evidence tied to it.
    /// Generation-less evidence, such as a probe result, is retained.
    pub fn clear_current_generation(&mut self, agent: &str) {
        self.current_generations.remove(agent);
        if self
            .agents
            .get(agent)
            .is_some_and(|entry| entry.access_generation.is_some())
        {
            self.agents.remove(agent);
        }
    }

    /// Record evidence tied to the access credential that was actually used.
    /// Delayed old-generation negatives cannot replace newer positive evidence.
    pub fn set_with_generation(
        &mut self,
        agent: &str,
        availability: AuthAvailability,
        access_generation: &str,
    ) {
        if availability == AuthAvailability::Unknown {
            return;
        }
        if self.current_generations.get(agent).map(String::as_str) != Some(access_generation) {
            return;
        }
        self.agents.insert(
            agent.to_string(),
            Entry {
                availability,
                at: Instant::now(),
                access_generation: Some(access_generation.to_string()),
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
    #[cfg(test)]
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
    let probes: Vec<_> = shared
        .agent_configs()
        .into_iter()
        .filter(|agent| {
            !agent.account_disabled && !shared.account_login.lock().unwrap().contains(&agent.name)
        })
        .filter(|agent| agent.auth_probe.is_some())
        .collect();
    let probes = futures::future::join_all(probes.into_iter().map(|agent| async move {
        let observed = crate::credentials::request_generation(&agent);
        let access = crate::credentials::access_generation(&agent);
        let mut env = agent
            .command
            .env
            .iter()
            .map(|e| (e.name.clone(), e.value.clone()))
            .collect();
        let runtime = crate::credentials::runtime(&agent, &mut env).await;
        let observed = runtime
            .as_ref()
            .ok()
            .and_then(|r| r.as_ref())
            .map(|r| r.generation.clone())
            .or(observed);
        let result = match &runtime {
            Ok(_) => {
                let env = env
                    .into_iter()
                    .map(|(name, value)| crate::config::EnvVarConfig { name, value })
                    .collect::<Vec<_>>();
                run_probe_isolated(
                    agent.auth_probe.as_ref().unwrap(),
                    &env,
                    crate::accounts::provider(&agent).is_some(),
                )
                .await
            }
            Err(_) => AuthAvailability::Unknown,
        };
        if crate::accounts::provider(&agent).is_some() {
            match result {
                AuthAvailability::Unauthenticated { .. } => {
                    crate::credentials::repair(&agent, observed.as_deref()).await;
                }
                AuthAvailability::Authenticated => {
                    if let Some(access) = access {
                        crate::credentials::observe_success(&agent, &access).await;
                    }
                }
                AuthAvailability::Unknown => {}
            }
            sync_from_manager(shared, &agent);
        } else {
            // A probe without managed credential material cannot prove logout.
            if result == AuthAvailability::Authenticated {
                shared.auth.lock().unwrap().set(&agent.name, result);
            }
        }
    }));
    tokio::join!(probes, crate::usage::refresh_and_install(shared));
    shared.auth.lock().unwrap().mark_refreshed();
}

#[cfg(test)]
async fn run_probe(
    probe: &AuthProbeConfig,
    env: &[crate::config::EnvVarConfig],
) -> AuthAvailability {
    run_probe_isolated(probe, env, false).await
}

async fn run_probe_isolated(
    probe: &AuthProbeConfig,
    env: &[crate::config::EnvVarConfig],
    isolated: bool,
) -> AuthAvailability {
    let mut cmd = tokio::process::Command::new(&probe.command);
    if isolated
        || env
            .iter()
            .any(|v| v.name == "CLAUDE_CONFIG_DIR" || v.name == "CODEX_HOME")
    {
        for key in crate::accounts::AUTH_ENV {
            cmd.env_remove(key);
        }
    }
    cmd.args(&probe.args)
        .envs(env.iter().map(|v| (&v.name, &v.value)))
        .kill_on_drop(true);
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

/// Some adapters end a successful ACP request with their native auth-error
/// text. Treat only complete, known error replies as repair signals. Ordinary
/// model prose mentioning authentication must never affect account state.
pub fn response_is_auth_error(text: &str) -> bool {
    let text = text.trim();
    matches!(
        text.trim_end_matches('.'),
        "Failed to authenticate: OAuth session expired and could not be refreshed"
            | "Failed to authenticate: OAuth token revoked. Please log in again or contact your administrator"
            | "Authentication required"
            | "Not logged in. Please run /login"
    )
}

/// Whether an error identifies an HTTP 401 response. Authentication wording
/// alone is not enough to retry: a provider may use it for a non-HTTP failure,
/// and only a 401 proves that the access credential was rejected.
pub fn error_is_http_401(text: &str) -> bool {
    let lower = text.to_lowercase();
    let trimmed = lower.trim();
    trimmed == "401"
        || contains_exact_401(&lower, "http 401")
        || contains_exact_401(&lower, "http/1.1 401")
        || contains_exact_401(&lower, "status 401")
        || contains_exact_401(&lower, "status_code: 401")
        || contains_exact_401(&lower, "returned error: 401")
        || contains_exact_401(&lower, "error: 401")
        || contains_exact_401(&lower, "401 unauthorized")
        || contains_exact_401(&lower, "unauthorized (401)")
}

fn contains_exact_401(text: &str, marker: &str) -> bool {
    text.match_indices(marker).any(|(start, _)| {
        let code_start = start + marker.len() - 3;
        !text[code_start + 3..]
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
    })
}

pub fn note_authenticated(tracker: &Mutex<AuthTracker>, agent: &str) {
    tracker
        .lock()
        .unwrap()
        .force_set(agent, AuthAvailability::Authenticated);
}

pub fn note_authenticated_from_usage(tracker: &Mutex<AuthTracker>, agent: &str) {
    tracker
        .lock()
        .unwrap()
        .set(agent, AuthAvailability::Authenticated);
}

pub fn note_authenticated_with_generation(
    tracker: &Mutex<AuthTracker>,
    agent: &str,
    access_generation: &str,
) {
    tracker.lock().unwrap().set_with_generation(
        agent,
        AuthAvailability::Authenticated,
        access_generation,
    );
}

/// Record generation-tied authentication evidence after a live credential
/// read. This is the only path that may establish the current generation.
pub fn note_authenticated_from_usage_with_generation(
    tracker: &Mutex<AuthTracker>,
    agent: &str,
    access_generation: &str,
) {
    let mut tracker = tracker.lock().unwrap();
    tracker.observe_generation(agent, access_generation);
    tracker.set_with_generation(agent, AuthAvailability::Authenticated, access_generation);
}

pub fn note_unauthenticated(tracker: &Mutex<AuthTracker>, agent: &str, reason: impl Into<String>) {
    tracker.lock().unwrap().set(
        agent,
        AuthAvailability::Unauthenticated {
            reason: reason.into(),
        },
    );
}

pub fn note_unauthenticated_with_generation(
    tracker: &Mutex<AuthTracker>,
    agent: &str,
    reason: impl Into<String>,
    access_generation: &str,
) {
    let mut tracker = tracker.lock().unwrap();
    tracker.set_with_generation(
        agent,
        AuthAvailability::Unauthenticated {
            reason: reason.into(),
        },
        access_generation,
    );
}

/// Record generation-tied rejection evidence after a live credential read.
pub fn note_unauthenticated_from_usage_with_generation(
    tracker: &Mutex<AuthTracker>,
    agent: &str,
    reason: impl Into<String>,
    access_generation: &str,
) {
    let mut tracker = tracker.lock().unwrap();
    tracker.observe_generation(agent, access_generation);
    tracker.set_with_generation(
        agent,
        AuthAvailability::Unauthenticated {
            reason: reason.into(),
        },
        access_generation,
    );
}

fn uses_claude_credentials(shared: &Arc<Shared>, agent: &str) -> bool {
    shared.agent_configs().iter().any(|configured| {
        configured.name == agent
            && matches!(
                configured.usage_source,
                Some(crate::config::UsageSourceConfig::AnthropicOauth)
            )
    })
}

/// Capture the credential generation before an ACP request begins. This is a
/// passive local read; it never invokes a provider login/status command.
pub fn request_access_generation(
    shared: &Arc<Shared>,
    candidate: &crate::candidate::CandidateId,
) -> Option<String> {
    if let Some(runtime) = shared.candidate_runtime(candidate)
        && let Some(generation) = shared
            .targets
            .lock()
            .unwrap()
            .get(&runtime.process_key)
            .and_then(|t| t.credential_generation.clone())
    {
        return Some(generation);
    }
    shared
        .agent_configs()
        .iter()
        .find(|a| a.name == candidate.agent)
        .and_then(crate::credentials::request_generation)
}

/// A missing credential store is live negative information, unlike an
/// ordinary usage-fetch failure. Remove only generation-tied state so a
/// generation-less probe result can still be applied afterward.
pub fn clear_generation_if_credentials_missing(shared: &Arc<Shared>, agent: &str) -> bool {
    if !uses_claude_credentials(shared, agent)
        || shared
            .agent_configs()
            .iter()
            .find(|a| a.name == agent)
            .and_then(crate::credentials::request_generation)
            .is_some()
    {
        return false;
    }
    shared.auth.lock().unwrap().clear_current_generation(agent);
    true
}

/// Record an ACP auth failure only when the same Claude access credential is
/// still current. A rotation, unreadable credential, or missing request
/// generation is unknown rather than durable logout evidence.
pub async fn note_auth_failure_for_request(
    shared: &Arc<Shared>,
    agent: &str,
    reason: impl Into<String>,
    request_generation: Option<&str>,
) -> crate::credentials::RepairOutcome {
    let _ = reason.into();
    let Some(configured) = shared.agent_configs().into_iter().find(|a| a.name == agent) else {
        return crate::credentials::RepairOutcome::Unknown;
    };
    let outcome = crate::credentials::repair(&configured, request_generation).await;
    sync_from_manager(shared, &configured);
    outcome
}

pub(crate) fn sync_from_manager(shared: &Arc<Shared>, agent: &crate::config::AgentConfig) {
    let state = crate::credentials::availability(agent);
    {
        let mut tracker = shared.auth.lock().unwrap();
        tracker.clear_current_generation(&agent.name);
        // Clear process-local negatives too: only the shared manager may reject.
        tracker.force_set(&agent.name, state);
    }
    shared.publish_config_options();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_complete_native_auth_errors_trigger_repair() {
        let error = "Failed to authenticate: OAuth session expired and could not be refreshed";
        assert!(response_is_auth_error(error));
        assert!(response_is_auth_error(&format!("  {error}.\n")));
        assert!(!response_is_auth_error(&format!(
            "The previous error was: {error}"
        )));
        assert!(!response_is_auth_error(
            "Here is how to repair Authentication required errors."
        ));
    }

    #[test]
    fn complete_revoked_token_reply_triggers_repair() {
        let error = "Failed to authenticate: OAuth token revoked. \
                     Please log in again or contact your administrator.";
        assert!(response_is_auth_error(error));
        assert!(response_is_auth_error(&format!("\n{error}  ")));
        assert!(!response_is_auth_error(&format!(
            "The previous error was: {error}"
        )));
        assert!(!response_is_auth_error(&format!("\"{error}\"")));
        assert!(!response_is_auth_error(&format!("> {error}")));
        assert!(!response_is_auth_error(
            "Failed to authenticate: OAuth token revoked."
        ));
    }

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

    #[test]
    fn delayed_old_negative_cannot_override_new_generation_positive() {
        let mut tracker = AuthTracker::default();
        tracker.observe_generation("a", "new-access");
        tracker.set_with_generation("a", AuthAvailability::Authenticated, "new-access");
        // This old generation was not present when the positive evidence was
        // recorded. Arrival order must not let it become the newest evidence.
        tracker.set_with_generation(
            "a",
            AuthAvailability::Unauthenticated {
                reason: "delayed old access rejected".to_string(),
            },
            "old-access",
        );
        assert_eq!(tracker.availability("a"), AuthAvailability::Authenticated);
    }

    #[test]
    fn generationless_negative_cannot_override_generation_tied_positive() {
        let mut tracker = AuthTracker::default();
        tracker.observe_generation("a", "current-access");
        tracker.set_with_generation("a", AuthAvailability::Authenticated, "current-access");
        tracker.set(
            "a",
            AuthAvailability::Unauthenticated {
                reason: "probe said signed out".to_string(),
            },
        );
        assert_eq!(tracker.availability("a"), AuthAvailability::Authenticated);
    }

    #[test]
    fn current_generation_rejection_survives_generationless_probe_success() {
        let mut tracker = AuthTracker::default();
        tracker.observe_generation("a", "current-access");
        tracker.set_with_generation(
            "a",
            AuthAvailability::Unauthenticated {
                reason: "current access rejected".to_string(),
            },
            "current-access",
        );
        tracker.set("a", AuthAvailability::Authenticated);
        assert!(tracker.unauthenticated("a").is_some());
    }

    #[test]
    fn missing_live_credentials_clear_generation_tied_auth() {
        let mut tracker = AuthTracker::default();
        tracker.observe_generation("a", "current-access");
        tracker.set_with_generation("a", AuthAvailability::Authenticated, "current-access");
        tracker.clear_current_generation("a");
        assert_eq!(tracker.availability("a"), AuthAvailability::Unknown);
    }

    #[test]
    fn current_passive_rejection_can_follow_a_cleared_generation() {
        let tracker = Mutex::new(AuthTracker::default());
        note_authenticated_from_usage_with_generation(&tracker, "a", "old-access");
        tracker.lock().unwrap().clear_current_generation("a");
        note_unauthenticated_from_usage_with_generation(
            &tracker,
            "a",
            "current access rejected",
            "new-access",
        );
        assert!(tracker.lock().unwrap().unauthenticated("a").is_some());
    }

    #[test]
    fn current_generation_rejection_replaces_prior_generation_positive() {
        let mut tracker = AuthTracker::default();
        tracker.observe_generation("a", "old-access");
        tracker.set_with_generation("a", AuthAvailability::Authenticated, "old-access");
        tracker.observe_generation("a", "new-access");
        tracker.set_with_generation(
            "a",
            AuthAvailability::Unauthenticated {
                reason: "new access rejected".to_string(),
            },
            "new-access",
        );
        assert!(tracker.unauthenticated("a").is_some());
        tracker.set_with_generation("a", AuthAvailability::Authenticated, "old-access");
        assert!(tracker.unauthenticated("a").is_some());
    }

    #[test]
    fn retry_signal_requires_http_401_status() {
        assert!(error_is_http_401("curl: returned error: 401"));
        assert!(error_is_http_401("HTTP 401 Unauthorized"));
        assert!(!error_is_http_401("HTTP 4010 Unauthorized"));
        assert!(!error_is_http_401("HTTP 401abc"));
        assert!(!error_is_http_401("error: 4011"));
        assert!(!error_is_http_401("authentication required"));
        assert!(!error_is_http_401("HTTP 403 Forbidden"));
        assert!(!error_is_http_401("request id 401 was not found"));
    }

    #[tokio::test]
    async fn missing_evidence_never_records_provider_logout() {
        let state = tempfile::tempdir().unwrap();
        let yaml = format!(
            "state_file: {}/state.sqlite\nagents:\n  - name: grok\n    command: {{type: stdio, command: mock-agent, env: [{{name: HOME, value: {}}}]}}\n    model_selection: {{type: config-option}}\n    models: [{{id: grok, cost_rank: 1}}]\n",
            state.path().display(),
            state.path().display()
        );
        let shared = Shared::new(crate::config::Config::from_yaml(&yaml).unwrap()).unwrap();
        assert_eq!(
            note_auth_failure_for_request(&shared, "grok", "Grok is not signed in", None).await,
            crate::credentials::RepairOutcome::Unknown
        );
        assert_eq!(
            shared.auth.lock().unwrap().availability("grok"),
            AuthAvailability::Unknown
        );
    }

    #[tokio::test]
    async fn auth_probe_uses_the_account_environment() {
        let probe = AuthProbeConfig {
            command: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "test \"$ROUTER_ACCOUNT_TEST_LOGIN\" = work".into(),
            ],
            timeout_ms: 1000,
            unauthenticated_patterns: vec!["not signed in".into()],
        };
        let env = vec![crate::config::EnvVarConfig {
            name: "ROUTER_ACCOUNT_TEST_LOGIN".into(),
            value: "work".into(),
        }];
        assert_eq!(
            run_probe(&probe, &env).await,
            AuthAvailability::Authenticated
        );
        assert_eq!(run_probe(&probe, &[]).await, AuthAvailability::Unknown);
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
            run_probe(&cfg(&["-c", "exit 0"]), &[]).await,
            AuthAvailability::Authenticated
        );
        assert!(matches!(
            run_probe(&cfg(&["-c", "echo not signed in >&2; exit 1"]), &[]).await,
            AuthAvailability::Unauthenticated { .. }
        ));
        assert_eq!(
            run_probe(&cfg(&["-c", "echo network error >&2; exit 1"]), &[]).await,
            AuthAvailability::Unknown
        );
        assert_eq!(
            run_probe(&cfg(&["-c", "sleep 1"]), &[]).await,
            AuthAvailability::Unknown
        );
    }
}
