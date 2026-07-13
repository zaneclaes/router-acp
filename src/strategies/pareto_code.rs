//! `pareto-code` strategy following OpenRouter's documented tier +
//! cheapest-available behavior, adapted from API price to seat quota
//! pressure.
//!
//! 1. Map `min_coding_score` into a tier (>= 0.66 high, >= 0.33 medium,
//!    else low; omitted means high).
//! 2. Filter to the requested tier; if empty, step to a neighboring tier in
//!    configured order and note that in the routing disclosure.
//! 3. Pick the lowest effective cost in the tier, where
//!    `effective_cost = cost_rank / max(headroom[agent], epsilon)`, keeping
//!    the next two same-tier candidates as pre-prompt fallbacks.

use crate::candidate::CodingTier;
use crate::config::ParetoCodeRouterConfig;

use super::{
    CandidateView, RankedCandidate, RouteContext, RouteError, RouterStrategy, effective_cost,
};

pub struct ParetoCodeStrategy {
    cfg: ParetoCodeRouterConfig,
}

impl ParetoCodeStrategy {
    pub fn new(cfg: ParetoCodeRouterConfig) -> Self {
        Self { cfg }
    }
}

impl RouterStrategy for ParetoCodeStrategy {
    fn rank(
        &self,
        _ctx: &RouteContext,
        candidates: &[CandidateView],
    ) -> Result<Vec<RankedCandidate>, RouteError> {
        let requested = CodingTier::from_min_score(self.cfg.min_coding_score);

        let mut tier_used = None;
        let mut pool: Vec<&CandidateView> = Vec::new();
        for tier in requested.walk_order() {
            let in_tier: Vec<&CandidateView> = candidates
                .iter()
                .filter(|c| c.coding_tier == tier)
                .collect();
            if !in_tier.is_empty() {
                tier_used = Some(tier);
                pool = in_tier;
                break;
            }
        }
        let Some(tier_used) = tier_used else {
            return Err(RouteError(
                "pareto-code: no routeable candidates in any coding tier".into(),
            ));
        };

        let note = if tier_used != requested {
            Some(format!(
                "tier fallback: no routeable `{}`-tier candidates, using `{}`",
                requested.as_str(),
                tier_used.as_str()
            ))
        } else {
            None
        };

        // Cheapest effective cost first; ties by config order. Keep the top
        // pick plus the next two same-tier candidates as fallbacks.
        let mut pool: Vec<&CandidateView> = pool;
        pool.sort_by(|a, b| {
            effective_cost(a)
                .partial_cmp(&effective_cost(b))
                .unwrap_or(std::cmp::Ordering::Equal)
                // Higher configured preference breaks effective-cost ties.
                .then_with(|| {
                    b.preference
                        .partial_cmp(&a.preference)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| a.config_index.cmp(&b.config_index))
        });
        let ranked = pool
            .into_iter()
            .take(3)
            .enumerate()
            .map(|(i, c)| RankedCandidate {
                candidate: c.id.clone(),
                // Deterministic descending score derived from effective cost.
                score: 1.0 / (1.0 + effective_cost(c)),
                weights: serde_json::json!({
                    "tier": tier_used.as_str(),
                    "effective_cost": effective_cost(c),
                    "cost_rank": c.cost_rank,
                    "headroom": c.headroom,
                    "preference": c.preference,
                }),
                reason: format!(
                    "{} `{}` coding tier by effective cost {:.1} (cost rank {} / headroom \
                     {:.0}%)",
                    if i == 0 { "cheapest in" } else { "fallback in" },
                    tier_used.as_str(),
                    effective_cost(c),
                    c.cost_rank,
                    c.headroom * 100.0,
                ),
                note: note.clone(),
            })
            .collect();
        Ok(ranked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::test_util::{ctx, view};

    fn cfg(min: Option<f64>) -> ParetoCodeRouterConfig {
        ParetoCodeRouterConfig {
            min_coding_score: min,
        }
    }

    #[test]
    fn picks_cheapest_in_high_tier() {
        let s = ParetoCodeStrategy::new(cfg(None));
        let pool = vec![
            view("claude", "haiku", 1, 0, 0.5, CodingTier::Medium, 1.0),
            view("claude", "sonnet", 2, 1, 0.8, CodingTier::High, 1.0),
            view("claude", "opus", 3, 2, 0.9, CodingTier::High, 1.0),
        ];
        let ranked = s.rank(&ctx(), &pool).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/sonnet");
        assert_eq!(ranked.len(), 2, "only same-tier candidates are kept");
        assert!(ranked[0].note.is_none());
    }

    #[test]
    fn quota_pressure_reorders_within_tier() {
        let s = ParetoCodeStrategy::new(cfg(None));
        let pool = vec![
            view("claude", "sonnet", 2, 0, 0.8, CodingTier::High, 0.05),
            view("codex", "gpt", 3, 1, 0.8, CodingTier::High, 1.0),
        ];
        // sonnet: 2 / 0.05 = 40; gpt: 3 / 1.0 = 3 -> gpt wins.
        let ranked = s.rank(&ctx(), &pool).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "codex/gpt");
    }

    #[test]
    fn medium_tier_request() {
        let s = ParetoCodeStrategy::new(cfg(Some(0.5)));
        let pool = vec![
            view("claude", "haiku", 1, 0, 0.5, CodingTier::Medium, 1.0),
            view("claude", "opus", 3, 1, 0.9, CodingTier::High, 1.0),
        ];
        let ranked = s.rank(&ctx(), &pool).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/haiku");
    }

    #[test]
    fn tier_fallback_notes_disclosure() {
        let s = ParetoCodeStrategy::new(cfg(None)); // wants high
        let pool = vec![view("claude", "haiku", 1, 0, 0.5, CodingTier::Medium, 1.0)];
        let ranked = s.rank(&ctx(), &pool).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/haiku");
        assert!(ranked[0].note.as_deref().unwrap().contains("tier fallback"));
    }

    #[test]
    fn caps_fallback_chain_at_three() {
        let s = ParetoCodeStrategy::new(cfg(None));
        let pool: Vec<CandidateView> = (0..5)
            .map(|i| {
                view(
                    "a",
                    &format!("m{i}"),
                    i as u32 + 1,
                    i,
                    0.8,
                    CodingTier::High,
                    1.0,
                )
            })
            .collect();
        let ranked = s.rank(&ctx(), &pool).unwrap();
        assert_eq!(ranked.len(), 3);
        assert_eq!(ranked[0].candidate.to_string(), "a/m0");
    }

    #[test]
    fn empty_pool_is_error() {
        let s = ParetoCodeStrategy::new(cfg(None));
        assert!(s.rank(&ctx(), &[]).is_err());
    }
}
