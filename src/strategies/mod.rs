//! Routing strategies: `static`, `auto`, `pareto-code`.
//!
//! All strategies share the [`RouterStrategy`] trait and return a
//! deterministic ranked fallback chain. Tie-break order everywhere is:
//! higher score, lower effective cost, config order.

mod auto;
mod escalation;
mod pareto_code;
mod static_;

pub use auto::AutoStrategy;
pub use escalation::EscalationStrategy;
pub use pareto_code::ParetoCodeStrategy;
pub use static_::StaticStrategy;

use crate::candidate::{CandidateId, CodingTier, RequiredCaps};
use crate::classifier::TaskProfile;
use crate::config::{Config, StrategyKind};

/// Everything a strategy may consider about one eligible candidate.
/// The pool given to `rank` is already filtered for routeability, required
/// capabilities, quarantine, and allowed-candidate globs.
#[derive(Debug, Clone)]
pub struct CandidateView {
    pub id: CandidateId,
    pub cost_rank: u32,
    /// Position in config declaration order (stable tie-break).
    pub config_index: usize,
    /// Benchmark quality in [0.5, 3.5] for the classified task class.
    pub quality: f64,
    pub coding_tier: CodingTier,
    /// Tightest local/plan headroom estimate in [0, 1].
    pub headroom: f64,
    /// Reported free included-plan fraction when a usage meter covers this
    /// candidate (`Some`), or `None` for unmetered agents (Grok, Kimi — no
    /// `usage_source`). Used by [`cap_unmetered_headroom`].
    pub plan_headroom: Option<f64>,
    /// The candidate has exhausted included-plan headroom and is spending
    /// paid overage/credits. Exhausted overage is filtered before this point.
    pub on_overage: bool,
    /// Configured per-agent tie-break preference (`agents[].preference`).
    pub preference: f64,
}

/// Free-plan residual small enough to treat as exhausted (matches
/// `headroom::SeatAvailability::plan_exhausted` tolerance).
const FREE_PLAN_EPSILON: f64 = 1e-9;

/// Product rule: while **any metered seat still has free included plan**, do
/// not let an **unmetered** candidate keep a fake 100% plan headroom that
/// beats free metered seats on the auto quota term alone.
///
/// Unmetered agents (no `usage_source` / no availability row — Grok, Kimi)
/// otherwise default to local sliding-window headroom ≈ 1.0. With
/// `quota = headroom × (1 − 0.5 × norm_cost_rank)`, a frontier unmetered
/// model at cost_rank 5 still scores quota 0.5 and wins trivial Ops over
/// haiku at 7% or mini at 44% free plan.
///
/// Cap: `unmetered.headroom = min(local, max free metered plan_headroom)`.
/// When every metered free plan is exhausted (or the pool is unmetered-only),
/// unmetered keeps full local headroom so it remains a valid failover.
pub fn cap_unmetered_headroom(views: &mut [CandidateView]) {
    let max_free_metered = views
        .iter()
        .filter(|v| !v.on_overage)
        .filter_map(|v| v.plan_headroom)
        .filter(|&p| p > FREE_PLAN_EPSILON)
        .fold(None, |acc: Option<f64>, p| {
            Some(acc.map_or(p, |a| a.max(p)))
        });
    let Some(cap) = max_free_metered else {
        return;
    };
    for view in views.iter_mut() {
        if view.plan_headroom.is_none() {
            view.headroom = view.headroom.min(cap);
        }
    }
}

/// Context for one routing decision.
#[derive(Debug, Clone)]
pub struct RouteContext {
    pub profile: TaskProfile,
    pub required_caps: RequiredCaps,
    /// Session-level explicit candidate (`router.candidate` config option).
    pub explicit_candidate: Option<CandidateId>,
}

#[derive(Debug, Clone)]
pub struct RankedCandidate {
    pub candidate: CandidateId,
    pub score: f64,
    /// Human-readable explanation of WHY this candidate ranked where it did
    /// (strategy math). Shown to the user in the routing disclosure.
    pub reason: String,
    /// The numeric inputs behind `reason`, persisted to the state file for
    /// post-hoc diagnosis (strategy-specific shape).
    pub weights: serde_json::Value,
    /// Human-readable note carried into the routing disclosure
    /// (e.g. "tier fallback high->medium").
    pub note: Option<String>,
}

#[derive(Debug)]
pub struct RouteError(pub String);

impl std::fmt::Display for RouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RouteError {}

pub trait RouterStrategy: Send + Sync {
    fn rank(
        &self,
        ctx: &RouteContext,
        candidates: &[CandidateView],
    ) -> Result<Vec<RankedCandidate>, RouteError>;
}

/// Instantiate the named strategy from config.
pub fn make_strategy(kind: StrategyKind, cfg: &Config) -> Box<dyn RouterStrategy> {
    match kind {
        StrategyKind::Static => Box::new(StaticStrategy::new(cfg.routers.static_.clone())),
        StrategyKind::Auto => Box::new(AutoStrategy::with_cost_aversion(
            cfg.routers.auto.clone(),
            cfg.availability_preference.cost_aversion,
        )),
        StrategyKind::ParetoCode => {
            Box::new(ParetoCodeStrategy::new(cfg.routers.pareto_code.clone()))
        }
        StrategyKind::Escalation => {
            let ecfg = cfg.routers.escalation.clone();
            match ecfg.initial_router {
                // Delegate the starting pick to another router (never to
                // `escalation` itself — config validation forbids it).
                Some(k) if k != StrategyKind::Escalation => Box::new(
                    EscalationStrategy::with_initial(ecfg, make_strategy(k, cfg)),
                ),
                _ => Box::new(EscalationStrategy::new(ecfg)),
            }
        }
    }
}

/// Effective cost used by quota-aware ranking: scarcity-adjusted cost rank.
pub fn effective_cost(view: &CandidateView) -> f64 {
    const EPSILON: f64 = 0.01;
    view.cost_rank as f64 / view.headroom.max(EPSILON)
}

/// Shared deterministic ordering: score desc, effective cost asc, config
/// order asc.
pub fn sort_ranked(pool: &mut [(f64, CandidateView, Option<String>)]) {
    pool.sort_by(|(sa, ca, _), (sb, cb, _)| {
        sb.partial_cmp(sa)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                effective_cost(ca)
                    .partial_cmp(&effective_cost(cb))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| ca.config_index.cmp(&cb.config_index))
    });
}

pub fn to_ranked(
    pool: Vec<(f64, CandidateView, Option<String>)>,
    reason: impl Fn(f64, &CandidateView) -> String,
    weights: impl Fn(f64, &CandidateView) -> serde_json::Value,
) -> Vec<RankedCandidate> {
    pool.into_iter()
        .map(|(score, view, note)| RankedCandidate {
            reason: reason(score, &view),
            weights: weights(score, &view),
            candidate: view.id,
            score,
            note,
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;
    use crate::candidate::TaskClass;

    pub fn view(
        agent: &str,
        model: &str,
        cost_rank: u32,
        config_index: usize,
        quality: f64,
        tier: CodingTier,
        headroom: f64,
    ) -> CandidateView {
        CandidateView {
            id: CandidateId::new(agent, model),
            cost_rank,
            config_index,
            quality,
            coding_tier: tier,
            headroom,
            // Unit tests that omit plan signals behave as unmetered-only pools
            // (no free-plan cap) — preserves historical ranking fixtures.
            plan_headroom: None,
            on_overage: false,
            preference: 0.0,
        }
    }

    pub fn view_metered(
        agent: &str,
        model: &str,
        cost_rank: u32,
        config_index: usize,
        quality: f64,
        tier: CodingTier,
        headroom: f64,
    ) -> CandidateView {
        let mut v = view(
            agent,
            model,
            cost_rank,
            config_index,
            quality,
            tier,
            headroom,
        );
        v.plan_headroom = Some(headroom);
        v
    }

    #[test]
    fn unmetered_headroom_capped_when_metered_has_free_plan() {
        let mut views = vec![
            view_metered("claude", "haiku", 1, 0, 1.29, CodingTier::Medium, 0.07),
            view_metered(
                "codex",
                "gpt-5.6-luna",
                2,
                1,
                2.02,
                CodingTier::Medium,
                0.44,
            ),
            // Grok: no plan signal, full local headroom (the live bug shape).
            view("grok", "grok-4.5", 5, 2, 1.57, CodingTier::High, 1.0),
        ];
        cap_unmetered_headroom(&mut views);
        let grok = views.iter().find(|v| v.id.agent == "grok").unwrap();
        assert!(
            (grok.headroom - 0.44).abs() < 1e-9,
            "unmetered capped at best free metered plan, got {}",
            grok.headroom
        );
        let haiku = views.iter().find(|v| v.id.model == "haiku").unwrap();
        assert!((haiku.headroom - 0.07).abs() < 1e-9);
    }

    #[test]
    fn unmetered_keeps_full_headroom_when_metered_free_plan_exhausted() {
        let mut views = vec![
            {
                let mut v = view_metered("claude", "haiku", 1, 0, 1.29, CodingTier::Medium, 0.0);
                v.on_overage = true;
                v
            },
            view("grok", "grok-4.5", 5, 1, 1.57, CodingTier::High, 1.0),
        ];
        cap_unmetered_headroom(&mut views);
        let grok = views.iter().find(|v| v.id.agent == "grok").unwrap();
        assert!(
            (grok.headroom - 1.0).abs() < 1e-9,
            "failover path: unmetered must stay at full local headroom, got {}",
            grok.headroom
        );
    }

    #[test]
    fn unmetered_only_pool_is_unchanged() {
        let mut views = vec![
            view("grok", "grok-4.5", 5, 0, 1.57, CodingTier::High, 1.0),
            view("kimi", "kimi-k2", 2, 1, 2.32, CodingTier::High, 0.9),
        ];
        cap_unmetered_headroom(&mut views);
        assert!((views[0].headroom - 1.0).abs() < 1e-9);
        assert!((views[1].headroom - 0.9).abs() < 1e-9);
    }

    pub fn ctx() -> RouteContext {
        RouteContext {
            profile: TaskProfile {
                class: TaskClass::CodingGeneral,
                complexity: 0.5,
                languages: vec![],
            },
            required_caps: RequiredCaps::default(),
            explicit_candidate: None,
        }
    }
}
