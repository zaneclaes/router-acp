//! Per-agent sliding-window headroom estimation and candidate quarantine.
//!
//! Headroom is an estimate because ACP adapters do not expose subscription
//! seat meters: we count prompts forwarded, sessions opened, and rate-limit
//! failures over a sliding window, normalized against per-agent budgets.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::candidate::CandidateId;
use crate::config::HeadroomConfig;

#[derive(Debug, Default)]
struct AgentWindow {
    prompts: VecDeque<Instant>,
    sessions: VecDeque<Instant>,
    rate_limit_failures: VecDeque<Instant>,
    /// Set when a downstream reported rate-limit/quota exhaustion; cleared
    /// when the window slides past it.
    exhausted_at: Option<Instant>,
}

#[derive(Debug, Default)]
struct CandidateFailures {
    pre_prompt_failures: VecDeque<Instant>,
    quarantined_until: Option<Instant>,
}

/// Sliding-window counters over a configurable window (default 5 hours).
#[derive(Debug)]
pub struct HeadroomTracker {
    window: Duration,
    quarantine_failures: u32,
    quarantine_cooloff: Duration,
    cordon_default: Duration,
    budgets: HashMap<String, u32>,
    agents: HashMap<String, AgentWindow>,
    candidates: HashMap<CandidateId, CandidateFailures>,
    /// Hard per-agent cordons (token/usage limits): the agent is excluded
    /// from routing until the stored instant, with a human-readable reason.
    cordons: HashMap<String, (Instant, String)>,
}

impl HeadroomTracker {
    pub fn new(cfg: &HeadroomConfig, budgets: HashMap<String, u32>) -> Self {
        Self {
            window: Duration::from_secs(cfg.window_secs),
            quarantine_failures: cfg.quarantine_failures,
            quarantine_cooloff: Duration::from_secs(cfg.quarantine_cooloff_secs),
            cordon_default: Duration::from_secs(cfg.cordon_default_secs),
            budgets,
            agents: HashMap::new(),
            candidates: HashMap::new(),
            cordons: HashMap::new(),
        }
    }

    /// Cordon an agent off from routing for `duration` (the parsed
    /// token-limit reset delay, or the configured default when the model
    /// did not report one). Returns the effective duration.
    pub fn cordon(
        &mut self,
        agent: &str,
        duration: Option<Duration>,
        reason: impl Into<String>,
    ) -> Duration {
        self.cordon_at(agent, duration, reason, Instant::now())
    }

    pub fn cordon_at(
        &mut self,
        agent: &str,
        duration: Option<Duration>,
        reason: impl Into<String>,
        now: Instant,
    ) -> Duration {
        let duration = duration.unwrap_or(self.cordon_default);
        let until = now + duration;
        let reason = reason.into();
        // Never shorten an existing cordon.
        let entry = self
            .cordons
            .entry(agent.to_string())
            .or_insert((until, reason.clone()));
        if until > entry.0 {
            *entry = (until, reason);
        }
        duration
    }

    /// Remaining cordon and reason for an agent, if one is active. Expired
    /// cordons are cleared.
    pub fn cordon_active(&mut self, agent: &str) -> Option<(Duration, String)> {
        self.cordon_active_at(agent, Instant::now())
    }

    pub fn cordon_active_at(&mut self, agent: &str, now: Instant) -> Option<(Duration, String)> {
        match self.cordons.get(agent) {
            Some((until, reason)) if *until > now => {
                Some((until.duration_since(now), reason.clone()))
            }
            Some(_) => {
                self.cordons.remove(agent);
                None
            }
            None => None,
        }
    }

    /// All active cordons: `(agent, remaining, reason)`.
    pub fn active_cordons(&mut self) -> Vec<(String, Duration, String)> {
        let now = Instant::now();
        self.cordons.retain(|_, (until, _)| *until > now);
        self.cordons
            .iter()
            .map(|(agent, (until, reason))| {
                (agent.clone(), until.duration_since(now), reason.clone())
            })
            .collect()
    }

    fn trim(window: Duration, now: Instant, events: &mut VecDeque<Instant>) {
        while let Some(front) = events.front() {
            if now.duration_since(*front) > window {
                events.pop_front();
            } else {
                break;
            }
        }
    }

    fn agent_mut(&mut self, agent: &str) -> &mut AgentWindow {
        self.agents.entry(agent.to_string()).or_default()
    }

    pub fn record_prompt(&mut self, agent: &str) {
        self.record_prompt_at(agent, Instant::now());
    }

    pub fn record_prompt_at(&mut self, agent: &str, now: Instant) {
        self.agent_mut(agent).prompts.push_back(now);
    }

    pub fn record_session(&mut self, agent: &str) {
        self.agent_mut(agent).sessions.push_back(Instant::now());
    }

    /// A rate-limit/auth/quota error before first prompt forwarding: headroom
    /// drops to 0 for the rest of the window slice.
    pub fn record_exhausted(&mut self, agent: &str) {
        self.record_exhausted_at(agent, Instant::now());
    }

    pub fn record_exhausted_at(&mut self, agent: &str, now: Instant) {
        let w = self.agent_mut(agent);
        w.rate_limit_failures.push_back(now);
        w.exhausted_at = Some(now);
    }

    /// Headroom estimate in [0, 1]: 1 = fresh budget, 0 = exhausted.
    pub fn headroom(&mut self, agent: &str) -> f64 {
        self.headroom_at(agent, Instant::now())
    }

    pub fn headroom_at(&mut self, agent: &str, now: Instant) -> f64 {
        let window = self.window;
        let budget = self.budgets.get(agent).copied().unwrap_or(400).max(1);
        if self.cordon_active_at(agent, now).is_some() {
            return 0.0;
        }
        let w = self.agent_mut(agent);
        Self::trim(window, now, &mut w.prompts);
        Self::trim(window, now, &mut w.rate_limit_failures);
        if let Some(at) = w.exhausted_at {
            if now.duration_since(at) <= window {
                return 0.0;
            }
            w.exhausted_at = None;
        }
        let used = w.prompts.len() as f64 / budget as f64;
        (1.0 - used).clamp(0.0, 1.0)
    }

    /// Snapshot per-agent headroom for a deterministic ranking pass.
    pub fn snapshot(&mut self, agents: impl IntoIterator<Item = String>) -> HashMap<String, f64> {
        let now = Instant::now();
        agents
            .into_iter()
            .map(|a| {
                let h = self.headroom_at(&a, now);
                (a, h)
            })
            .collect()
    }

    /// Record a pre-prompt failure (spawn/session/model-selection) for a
    /// candidate; quarantine it after N failures in the window.
    pub fn record_pre_prompt_failure(&mut self, candidate: &CandidateId) {
        self.record_pre_prompt_failure_at(candidate, Instant::now());
    }

    pub fn record_pre_prompt_failure_at(&mut self, candidate: &CandidateId, now: Instant) {
        let window = self.window;
        let threshold = self.quarantine_failures;
        let cooloff = self.quarantine_cooloff;
        let c = self.candidates.entry(candidate.clone()).or_default();
        c.pre_prompt_failures.push_back(now);
        Self::trim(window, now, &mut c.pre_prompt_failures);
        if c.pre_prompt_failures.len() as u32 >= threshold {
            c.quarantined_until = Some(now + cooloff);
            tracing::warn!(
                candidate = %candidate,
                failures = c.pre_prompt_failures.len(),
                cooloff_secs = cooloff.as_secs(),
                "candidate quarantined after repeated pre-prompt failures"
            );
        }
    }

    pub fn is_quarantined(&mut self, candidate: &CandidateId) -> bool {
        self.is_quarantined_at(candidate, Instant::now())
    }

    pub fn is_quarantined_at(&mut self, candidate: &CandidateId, now: Instant) -> bool {
        let Some(c) = self.candidates.get_mut(candidate) else {
            return false;
        };
        match c.quarantined_until {
            Some(until) if now < until => true,
            Some(_) => {
                // Cool-off elapsed: clear the strike record so the candidate
                // gets a fresh chance instead of instantly re-quarantining.
                c.quarantined_until = None;
                c.pre_prompt_failures.clear();
                false
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker(budget: u32) -> HeadroomTracker {
        HeadroomTracker::new(
            &HeadroomConfig::default(),
            HashMap::from([("claude".to_string(), budget)]),
        )
    }

    #[test]
    fn headroom_declines_with_prompts() {
        let mut t = tracker(10);
        let now = Instant::now();
        assert_eq!(t.headroom_at("claude", now), 1.0);
        for _ in 0..5 {
            t.record_prompt_at("claude", now);
        }
        assert!((t.headroom_at("claude", now) - 0.5).abs() < 1e-9);
        for _ in 0..10 {
            t.record_prompt_at("claude", now);
        }
        assert_eq!(t.headroom_at("claude", now), 0.0);
    }

    #[test]
    fn window_slides() {
        let mut t = tracker(10);
        let start = Instant::now();
        for _ in 0..10 {
            t.record_prompt_at("claude", start);
        }
        assert_eq!(t.headroom_at("claude", start), 0.0);
        let later = start + Duration::from_secs(5 * 60 * 60 + 1);
        assert_eq!(t.headroom_at("claude", later), 1.0);
    }

    #[test]
    fn exhaustion_zeroes_headroom() {
        let mut t = tracker(100);
        let now = Instant::now();
        t.record_exhausted_at("claude", now);
        assert_eq!(t.headroom_at("claude", now), 0.0);
        let later = now + Duration::from_secs(5 * 60 * 60 + 1);
        assert!(t.headroom_at("claude", later) > 0.9);
    }

    #[test]
    fn quarantine_after_n_failures_and_cooloff() {
        let mut t = tracker(100);
        let id = CandidateId::new("claude", "sonnet");
        let now = Instant::now();
        assert!(!t.is_quarantined_at(&id, now));
        t.record_pre_prompt_failure_at(&id, now);
        t.record_pre_prompt_failure_at(&id, now);
        assert!(!t.is_quarantined_at(&id, now));
        t.record_pre_prompt_failure_at(&id, now);
        assert!(t.is_quarantined_at(&id, now));
        // Cool-off elapses; strikes reset.
        let later = now + Duration::from_secs(10 * 60 + 1);
        assert!(!t.is_quarantined_at(&id, later));
        t.record_pre_prompt_failure_at(&id, later);
        assert!(!t.is_quarantined_at(&id, later));
    }

    #[test]
    fn cordon_excludes_agent_until_reset() {
        let mut t = tracker(100);
        let now = Instant::now();
        assert!(t.cordon_active_at("claude", now).is_none());
        let effective = t.cordon_at(
            "claude",
            Some(Duration::from_secs(3600)),
            "usage limit (resets ~1h)",
            now,
        );
        assert_eq!(effective, Duration::from_secs(3600));
        let (remaining, reason) = t.cordon_active_at("claude", now).unwrap();
        assert!(remaining <= Duration::from_secs(3600));
        assert!(reason.contains("usage limit"));
        assert_eq!(t.headroom_at("claude", now), 0.0, "cordon zeroes headroom");
        // Expired cordon clears.
        let later = now + Duration::from_secs(3601);
        assert!(t.cordon_active_at("claude", later).is_none());
        assert!(t.headroom_at("claude", later) > 0.9);
    }

    #[test]
    fn cordon_without_reset_uses_default_and_never_shrinks() {
        let mut t = tracker(100);
        let now = Instant::now();
        let effective = t.cordon_at("claude", None, "rate limit", now);
        assert_eq!(effective, Duration::from_secs(15 * 60), "config default");
        // A later, longer cordon extends; a shorter one does not shrink.
        t.cordon_at("claude", Some(Duration::from_secs(7200)), "bigger", now);
        let (remaining, reason) = t.cordon_active_at("claude", now).unwrap();
        assert!(remaining > Duration::from_secs(7000));
        assert_eq!(reason, "bigger");
        t.cordon_at("claude", Some(Duration::from_secs(60)), "smaller", now);
        let (remaining, _) = t.cordon_active_at("claude", now).unwrap();
        assert!(remaining > Duration::from_secs(7000), "not shortened");
    }

    #[test]
    fn unknown_agent_defaults_to_full_headroom() {
        let mut t = tracker(10);
        assert_eq!(t.headroom("codex"), 1.0);
    }
}
