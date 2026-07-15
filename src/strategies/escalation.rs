//! `escalation` strategy: start on the cheapest capable candidate and let the
//! runtime escalate to a stronger one only when *observed execution* reveals
//! hidden difficulty (heavy investigation, tool-failure churn, token
//! exhaustion, refusals).
//!
//! `rank` chooses only the starting point + fallback chain (for pre-prompt
//! open failures); the escalation itself is a runtime concern — see
//! `session::escalation_target` + `switch_pin`. By default the start is the
//! cheapest capable candidate, but `initial_router` delegates that first pick
//! to another strategy (e.g. `auto`) so a session begins on a *sensible* model
//! and only escalates from there.

use crate::config::EscalationRouterConfig;

use super::{
    CandidateView, RankedCandidate, RouteContext, RouteError, RouterStrategy, effective_cost,
};

pub struct EscalationStrategy {
    cfg: EscalationRouterConfig,
    /// When `initial_router` is set, the strategy that picks the starting pin.
    initial: Option<Box<dyn RouterStrategy>>,
}

impl EscalationStrategy {
    pub fn new(cfg: EscalationRouterConfig) -> Self {
        Self { cfg, initial: None }
    }

    /// Construct with a delegate strategy that chooses the starting candidate
    /// (`routers.escalation.initial_router`).
    pub fn with_initial(cfg: EscalationRouterConfig, initial: Box<dyn RouterStrategy>) -> Self {
        Self {
            cfg,
            initial: Some(initial),
        }
    }
}

impl RouterStrategy for EscalationStrategy {
    fn rank(
        &self,
        ctx: &RouteContext,
        candidates: &[CandidateView],
    ) -> Result<Vec<RankedCandidate>, RouteError> {
        if candidates.is_empty() {
            return Err(RouteError("escalation: no routeable candidates".into()));
        }
        // Delegated start: let another router choose the initial pin (and
        // fallback chain); escalation still applies at runtime from there.
        if let Some(initial) = &self.initial {
            let mut ranked = initial.rank(ctx, candidates)?;
            if let Some(first) = ranked.first_mut() {
                let label = self
                    .cfg
                    .initial_router
                    .map(|k| k.as_str())
                    .unwrap_or("initial_router");
                let note = format!("initial pick delegated to `{label}`; escalates on difficulty");
                first.note = Some(match first.note.take() {
                    Some(existing) => format!("{note}; {existing}"),
                    None => note,
                });
            }
            return Ok(ranked);
        }
        // Apply the optional starting-quality floor, but never route nothing:
        // if the floor would empty the pool, ignore it.
        let floored: Vec<&CandidateView> = candidates
            .iter()
            .filter(|c| c.quality >= self.cfg.min_start_score)
            .collect();
        let mut pool: Vec<&CandidateView> = if floored.is_empty() {
            candidates.iter().collect()
        } else {
            floored
        };

        // Cheapest effective cost first — that is the starting pin. Ties break
        // by higher preference, then config order. The rest are the fallback
        // chain used only if the cheapest fails to *open*.
        pool.sort_by(|a, b| {
            effective_cost(a)
                .partial_cmp(&effective_cost(b))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    b.preference
                        .partial_cmp(&a.preference)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| a.config_index.cmp(&b.config_index))
        });

        let ranked = pool
            .into_iter()
            .enumerate()
            .map(|(i, c)| RankedCandidate {
                candidate: c.id.clone(),
                // Deterministic descending score: cheapest ranks highest.
                score: 1.0 / (1.0 + effective_cost(c)),
                weights: serde_json::json!({
                    "effective_cost": effective_cost(c),
                    "cost_rank": c.cost_rank,
                    "headroom": c.headroom,
                    "quality": c.quality,
                    "preference": c.preference,
                    "start": i == 0,
                }),
                reason: format!(
                    "{} candidate by effective cost {:.1} (cost rank {} / headroom {:.0}%); \
                     escalates to a stronger model if execution reveals difficulty",
                    if i == 0 { "cheapest" } else { "fallback" },
                    effective_cost(c),
                    c.cost_rank,
                    c.headroom * 100.0,
                ),
                note: None,
            })
            .collect();
        Ok(ranked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::CodingTier;
    use crate::strategies::test_util::{ctx, view};

    fn cfg() -> EscalationRouterConfig {
        EscalationRouterConfig::default()
    }

    #[test]
    fn starts_on_cheapest_capable() {
        let s = EscalationStrategy::new(cfg());
        let pool = vec![
            view("claude", "opus", 4, 0, 0.9, CodingTier::High, 1.0),
            view("claude", "haiku", 1, 1, 0.5, CodingTier::Low, 1.0),
            view("claude", "sonnet", 2, 2, 0.8, CodingTier::High, 1.0),
        ];
        let ranked = s.rank(&ctx(), &pool).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/haiku");
        // Fallback chain is cheapest-first for open failures.
        assert_eq!(ranked[1].candidate.to_string(), "claude/sonnet");
        assert_eq!(ranked[2].candidate.to_string(), "claude/opus");
    }

    #[test]
    fn min_start_score_floors_the_starting_pin() {
        let mut c = cfg();
        c.min_start_score = 0.7; // exclude haiku (0.5) as a starting point
        let s = EscalationStrategy::new(c);
        let pool = vec![
            view("claude", "haiku", 1, 0, 0.5, CodingTier::Low, 1.0),
            view("claude", "sonnet", 2, 1, 0.8, CodingTier::High, 1.0),
        ];
        let ranked = s.rank(&ctx(), &pool).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/sonnet");
    }

    #[test]
    fn floor_ignored_rather_than_routing_nothing() {
        let mut c = cfg();
        c.min_start_score = 0.99; // nothing qualifies
        let s = EscalationStrategy::new(c);
        let pool = vec![view("claude", "haiku", 1, 0, 0.5, CodingTier::Low, 1.0)];
        let ranked = s.rank(&ctx(), &pool).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/haiku");
    }

    #[test]
    fn quota_pressure_shifts_the_start() {
        let s = EscalationStrategy::new(cfg());
        let pool = vec![
            // haiku is nominally cheapest but nearly exhausted → looks expensive.
            view("claude", "haiku", 1, 0, 0.5, CodingTier::Low, 0.02),
            view("codex", "mini", 2, 1, 0.6, CodingTier::Medium, 1.0),
        ];
        // haiku: 1/0.02 = 50; mini: 2/1.0 = 2 → mini starts.
        let ranked = s.rank(&ctx(), &pool).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "codex/mini");
    }

    #[test]
    fn empty_pool_is_error() {
        let s = EscalationStrategy::new(cfg());
        assert!(s.rank(&ctx(), &[]).is_err());
    }
}
