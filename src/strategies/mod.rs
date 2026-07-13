//! Routing strategies: `static`, `auto`, `pareto-code`.
//!
//! All strategies share the [`RouterStrategy`] trait and return a
//! deterministic ranked fallback chain. Tie-break order everywhere is:
//! higher score, lower effective cost, config order.

mod auto;
mod pareto_code;
mod static_;

pub use auto::AutoStrategy;
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
    /// Quality score in [0, 1] for the classified task class.
    pub quality: f64,
    pub coding_tier: CodingTier,
    /// Per-agent headroom estimate in [0, 1].
    pub headroom: f64,
    /// Configured per-agent tie-break preference (`agents[].preference`).
    pub preference: f64,
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
        StrategyKind::Auto => Box::new(AutoStrategy::new(cfg.routers.auto.clone())),
        StrategyKind::ParetoCode => {
            Box::new(ParetoCodeStrategy::new(cfg.routers.pareto_code.clone()))
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
            preference: 0.0,
        }
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
