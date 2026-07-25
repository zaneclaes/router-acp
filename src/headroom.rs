//! Per-agent sliding-window headroom estimation and candidate quarantine.
//!
//! Headroom is an estimate because ACP adapters do not expose subscription
//! seat meters: we count prompts forwarded, sessions opened, and rate-limit
//! failures over a sliding window, normalized against per-agent budgets.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime};

use crate::candidate::CandidateId;
use crate::config::HeadroomConfig;

/// A proactive per-candidate cordon derived from a provider usage API: the
/// candidate is unroutable until `resets_at`. Unlike the reactive per-agent
/// cordons (which use monotonic `Instant`), these carry an absolute wall-clock
/// reset (the provider reports it) so they self-lift correctly and can be
/// advertised to the client with a real timestamp.
#[derive(Debug, Clone)]
pub struct UsageCordon {
    pub reason: String,
    pub resets_at: SystemTime,
    /// The provider's reset timestamp, verbatim (RFC 3339), for advertising.
    pub resets_at_rfc3339: String,
}

/// Graded seat availability for one candidate — the input to dynamic
/// preference scaling. Unlike a [`UsageCordon`] (binary: unroutable), this
/// describes how much *free* plan budget the candidate's seat still has and
/// whether the seat has tipped into paid overage. Produced by the usage
/// poller and by client `availability_hint` extension notifications.
#[derive(Debug, Clone, PartialEq)]
pub struct SeatAvailability {
    /// Fraction of the seat's plan budget still free for this candidate, in
    /// [0, 1] (min across the plan windows that cover it).
    pub plan_headroom: f64,
    /// The plan cap is exhausted and overage/credits are absorbing usage:
    /// still routable, but every turn now spends real money.
    pub on_overage: bool,
    /// Who reported it: `"poll"` (the router's own usage poller) or `"hint"`
    /// (a client availability hint).
    pub source: &'static str,
}

impl SeatAvailability {
    /// The seat has nothing left to spend on this candidate: its plan budget is
    /// gone and no overage/credit pool is absorbing the excess. Semantically
    /// identical to a [`UsageCordon`] — the candidate is unroutable — but
    /// derived from graded availability, so it also holds in the window before
    /// the poller installs cordons (or when a client hint is the only signal).
    /// Preference scaling alone is not enough there: it leaves the candidate at
    /// preference 0, which a quality edge can still win.
    pub fn plan_exhausted(&self) -> bool {
        self.plan_headroom <= PLAN_HEADROOM_EPSILON && !self.on_overage
    }
}

/// `plan_headroom` is a ratio of reported percentages, so "zero" is a
/// tolerance, not an equality.
const PLAN_HEADROOM_EPSILON: f64 = 1e-9;

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
    /// Proactive per-candidate cordons from provider usage APIs. Recomputed
    /// wholesale by the usage poller each cycle (`set_usage_cordons`); each
    /// entry self-lifts at its absolute `resets_at`.
    usage_cordons: HashMap<CandidateId, UsageCordon>,
    /// Reactive per-candidate cordons from failures the candidate itself hit
    /// (a spend cap denies the model with no plan budget left, while the
    /// agent's other models still have theirs). Held apart from
    /// `usage_cordons` so the poller's wholesale refresh cannot erase them.
    candidate_cordons: HashMap<CandidateId, UsageCordon>,
    /// Seat availability from the router's own usage poller. Recomputed
    /// wholesale each poll cycle.
    availability_poll: HashMap<CandidateId, SeatAvailability>,
    /// Seat availability hinted by the client, per agent, with an absolute
    /// expiry — a fresh hint wins over the poll for that agent's candidates;
    /// an expired one is ignored (the poll remains the floor).
    availability_hints: HashMap<String, (HashMap<CandidateId, SeatAvailability>, SystemTime)>,
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
            usage_cordons: HashMap::new(),
            candidate_cordons: HashMap::new(),
            availability_poll: HashMap::new(),
            availability_hints: HashMap::new(),
        }
    }

    /// Replace the proactive per-candidate usage cordons wholesale (the poller
    /// computes an authoritative snapshot each cycle). A candidate no longer
    /// exhausted simply drops out of the map here and becomes routeable again.
    pub fn set_usage_cordons(&mut self, cordons: HashMap<CandidateId, UsageCordon>) {
        self.usage_cordons = cordons;
    }

    /// Reactively cordon ONE candidate until `resets_at`, for a failure that
    /// implicates only that candidate's budget rather than its agent's — a
    /// monthly/spend cap denies the model whose plan window is already spent
    /// while the agent's other models still have plan budget of their own.
    /// Never shortens an existing cordon.
    pub fn cordon_candidate(
        &mut self,
        id: &CandidateId,
        reason: impl Into<String>,
        resets_at: SystemTime,
    ) {
        if self
            .candidate_cordons
            .get(id)
            .is_some_and(|c| c.resets_at >= resets_at)
        {
            return;
        }
        let epoch = resets_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.candidate_cordons.insert(
            id.clone(),
            UsageCordon {
                reason: reason.into(),
                resets_at,
                resets_at_rfc3339: crate::usage::epoch_to_rfc3339(epoch),
            },
        );
    }

    /// The active usage cordon for a candidate, if any (i.e. `now < resets_at`).
    pub fn usage_cordon(&self, id: &CandidateId) -> Option<&UsageCordon> {
        self.usage_cordon_at(id, SystemTime::now())
    }

    /// The polled and reactive cordons are independent sources; whichever
    /// resets later governs, so a candidate hit by both stays out until the
    /// last one clears.
    pub fn usage_cordon_at(&self, id: &CandidateId, now: SystemTime) -> Option<&UsageCordon> {
        let polled = self.usage_cordons.get(id).filter(|c| c.resets_at > now);
        let reactive = self.candidate_cordons.get(id).filter(|c| c.resets_at > now);
        match (polled, reactive) {
            (Some(polled), Some(reactive)) => Some(if reactive.resets_at > polled.resets_at {
                reactive
            } else {
                polled
            }),
            (polled, reactive) => polled.or(reactive),
        }
    }

    /// All candidates currently usage-cordoned, for advertising and the
    /// all-cordoned "least-bad" fallback.
    pub fn active_usage_cordons(&self) -> Vec<(CandidateId, UsageCordon)> {
        let now = SystemTime::now();
        let mut ids: Vec<CandidateId> = self
            .usage_cordons
            .keys()
            .chain(self.candidate_cordons.keys())
            .cloned()
            .collect();
        ids.sort();
        ids.dedup();
        ids.into_iter()
            .filter_map(|id| {
                self.usage_cordon_at(&id, now)
                    .map(|c| (id.clone(), c.clone()))
            })
            .collect()
    }

    /// Replace the poller's seat-availability snapshot wholesale (like
    /// `set_usage_cordons`, the poller computes an authoritative set each
    /// cycle).
    pub fn set_polled_availability(
        &mut self,
        availability: HashMap<CandidateId, SeatAvailability>,
    ) {
        self.availability_poll = availability;
    }

    /// Install a client availability hint for one agent's candidates, valid
    /// until `expires_at`. Replaces any previous hint for that agent.
    pub fn set_hinted_availability(
        &mut self,
        agent: &str,
        availability: HashMap<CandidateId, SeatAvailability>,
        expires_at: SystemTime,
    ) {
        self.availability_hints
            .insert(agent.to_string(), (availability, expires_at));
    }

    /// Seat availability for a candidate: the client hint while fresh (the
    /// client's view is typically newer than the poll), else the poller's.
    /// `None` means no source has data — the candidate's static preference
    /// applies unscaled.
    pub fn availability(&self, id: &CandidateId) -> Option<SeatAvailability> {
        self.availability_at(id, SystemTime::now())
    }

    pub fn availability_at(&self, id: &CandidateId, now: SystemTime) -> Option<SeatAvailability> {
        if let Some((map, expires_at)) = self.availability_hints.get(&id.agent)
            && *expires_at > now
            && let Some(a) = map.get(id)
        {
            return Some(a.clone());
        }
        self.availability_poll.get(id).cloned()
    }

    /// True when the freshest availability report says this candidate's seat is
    /// spent (see [`SeatAvailability::plan_exhausted`]) — the candidate is
    /// unroutable. Unknown availability is *not* exhaustion: a poll that never
    /// ran or failed must fail open, so `None` keeps the candidate routeable.
    pub fn seat_exhausted(&self, id: &CandidateId) -> bool {
        self.seat_exhausted_at(id, SystemTime::now())
    }

    pub fn seat_exhausted_at(&self, id: &CandidateId, now: SystemTime) -> bool {
        self.availability_at(id, now)
            .is_some_and(|a| a.plan_exhausted())
    }

    /// All candidates with known seat availability (hint-fresh over poll),
    /// for the routing disclosure.
    pub fn availabilities(&self) -> Vec<(CandidateId, SeatAvailability)> {
        let now = SystemTime::now();
        let mut out: HashMap<CandidateId, SeatAvailability> = self.availability_poll.clone();
        for (map, expires_at) in self.availability_hints.values() {
            if *expires_at > now {
                out.extend(map.iter().map(|(id, a)| (id.clone(), a.clone())));
            }
        }
        let mut list: Vec<_> = out.into_iter().collect();
        list.sort_by_key(|(id, _)| id.to_string());
        list
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

    #[test]
    fn availability_prefers_fresh_hint_and_expires_to_poll() {
        let mut t = tracker(10);
        let id = CandidateId::new("claude", "sonnet");
        let now = SystemTime::now();
        assert!(t.availability_at(&id, now).is_none());

        let polled = SeatAvailability {
            plan_headroom: 0.6,
            on_overage: false,
            source: "poll",
        };
        t.set_polled_availability(HashMap::from([(id.clone(), polled.clone())]));
        assert_eq!(t.availability_at(&id, now), Some(polled.clone()));

        let hinted = SeatAvailability {
            plan_headroom: 0.0,
            on_overage: true,
            source: "hint",
        };
        t.set_hinted_availability(
            "claude",
            HashMap::from([(id.clone(), hinted.clone())]),
            now + Duration::from_secs(60),
        );
        assert_eq!(t.availability_at(&id, now), Some(hinted));
        // Hint expired: back to the poll's view.
        let later = now + Duration::from_secs(61);
        assert_eq!(t.availability_at(&id, later), Some(polled));
        // A hint for one agent never affects another's candidates.
        let other = CandidateId::new("codex", "gpt-5.5");
        assert!(t.availability_at(&other, now).is_none());
    }

    #[test]
    fn plan_exhausted_excludes_zero_headroom_without_overage() {
        let exhausted = SeatAvailability {
            plan_headroom: 0.0,
            on_overage: false,
            source: "poll",
        };
        assert!(exhausted.plan_exhausted());

        // Zero headroom but overage/credits are absorbing it: still routable,
        // just paying — not the same as a cordon.
        let paying = SeatAvailability {
            plan_headroom: 0.0,
            on_overage: true,
            source: "poll",
        };
        assert!(!paying.plan_exhausted());

        // Free budget remaining: routable regardless of overage state.
        let free = SeatAvailability {
            plan_headroom: 0.4,
            on_overage: false,
            source: "poll",
        };
        assert!(!free.plan_exhausted());

        // Below the float-compare epsilon still counts as zero.
        let near_zero = SeatAvailability {
            plan_headroom: 1e-10,
            on_overage: false,
            source: "poll",
        };
        assert!(near_zero.plan_exhausted());
    }

    #[test]
    fn seat_exhausted_excludes_the_candidate_from_eligibility() {
        // seat_exhausted is the predicate eligible_views_inner gates on: a
        // candidate whose polled availability is plan-exhausted must report
        // exhausted, and one with headroom (or no report at all) must not.
        let mut t = tracker(10);
        let exhausted_id = CandidateId::new("claude", "claude-fable-5[1m]");
        let has_headroom_id = CandidateId::new("claude", "sonnet");
        let now = SystemTime::now();

        // Unknown availability (poll never ran / not yet installed): fail
        // open, never treat as exhausted.
        assert!(!t.seat_exhausted_at(&exhausted_id, now));

        t.set_polled_availability(HashMap::from([
            (
                exhausted_id.clone(),
                SeatAvailability {
                    plan_headroom: 0.0,
                    on_overage: false,
                    source: "poll",
                },
            ),
            (
                has_headroom_id.clone(),
                SeatAvailability {
                    plan_headroom: 0.22,
                    on_overage: false,
                    source: "poll",
                },
            ),
        ]));
        assert!(t.seat_exhausted_at(&exhausted_id, now));
        assert!(!t.seat_exhausted_at(&has_headroom_id, now));
    }
}
