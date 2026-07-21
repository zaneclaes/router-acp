//! `auto` strategy: behavioral analogue of the OpenRouter Auto router over
//! local candidates.
//!
//! ```text
//! quality_weight = 1 - cost_quality_tradeoff / 10
//! cost_weight = cost_quality_tradeoff / 10
//! quota_score = headroom[agent] * (1 - normalized_cost_rank(candidate))
//! utility = quality_weight * quality[class] + cost_weight * quota_score
//! ```
//!
//! When `complexity >= complexity_floor`, candidates below the 75th
//! percentile quality score for the task class (among the surviving pool)
//! are dropped first.

use crate::candidate::glob_match;
use crate::config::AutoRouterConfig;

use super::{
    CandidateView, RankedCandidate, RouteContext, RouteError, RouterStrategy, sort_ranked,
    to_ranked,
};

pub struct AutoStrategy {
    cfg: AutoRouterConfig,
}

impl AutoStrategy {
    pub fn new(cfg: AutoRouterConfig) -> Self {
        Self { cfg }
    }
}

/// 75th percentile by the nearest-rank method over the pool's quality scores.
fn quality_p75(pool: &[&CandidateView]) -> f64 {
    let mut scores: Vec<f64> = pool.iter().map(|c| c.quality).collect();
    scores.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if scores.is_empty() {
        return 0.0;
    }
    let rank = ((0.75 * scores.len() as f64).ceil() as usize).clamp(1, scores.len());
    scores[rank - 1]
}

impl RouterStrategy for AutoStrategy {
    fn rank(
        &self,
        ctx: &RouteContext,
        candidates: &[CandidateView],
    ) -> Result<Vec<RankedCandidate>, RouteError> {
        // 1. allowed_candidates globs.
        let mut pool: Vec<&CandidateView> = candidates
            .iter()
            .filter(|c| {
                let key = c.id.to_string();
                self.cfg
                    .allowed_candidates
                    .iter()
                    .any(|p| glob_match(p, &key))
            })
            .collect();
        if pool.is_empty() {
            return Err(RouteError(
                "no candidates survive `auto.allowed_candidates` filters".into(),
            ));
        }

        // 2-3. Complexity gate: drop candidates below the 75th percentile
        // quality for this task class among the surviving pool.
        let mut gate_note = None;
        if ctx.profile.complexity >= self.cfg.complexity_floor && pool.len() > 1 {
            let p75 = quality_p75(&pool);
            let gated: Vec<&CandidateView> =
                pool.iter().copied().filter(|c| c.quality >= p75).collect();
            if !gated.is_empty() {
                if gated.len() != pool.len() {
                    gate_note = Some(format!(
                        "complexity {:.2} >= floor {:.2}: kept top-quality candidates",
                        ctx.profile.complexity, self.cfg.complexity_floor
                    ));
                }
                pool = gated;
            }
        }

        // 4. Utility scoring. The tradeoff optionally scales down with
        //    classified complexity: scarcity matters for trivial prompts,
        //    quality dominates hard ones.
        let min_cost = pool.iter().map(|c| c.cost_rank).min().unwrap_or(1) as f64;
        let max_cost = pool.iter().map(|c| c.cost_rank).max().unwrap_or(1) as f64;
        let base_tradeoff = self.cfg.cost_quality_tradeoff.clamp(0.0, 10.0);
        let tradeoff = if self.cfg.complexity_scales_tradeoff {
            base_tradeoff * (1.0 - ctx.profile.complexity.clamp(0.0, 1.0))
        } else {
            base_tradeoff
        };
        let quality_weight = 1.0 - tradeoff / 10.0;
        let cost_weight = tradeoff / 10.0;

        let scored: Vec<(f64, CandidateView, Option<String>)> = pool
            .into_iter()
            .map(|c| {
                let normalized_cost_rank = if max_cost > min_cost {
                    (c.cost_rank as f64 - min_cost) / (max_cost - min_cost)
                } else {
                    0.0
                };
                let quota_score = c.headroom * (1.0 - normalized_cost_rank);
                let utility = quality_weight * c.quality + cost_weight * quota_score + c.preference;
                (utility, c.clone(), gate_note.clone())
            })
            .collect();

        // 5. Deterministic sort: utility desc, effective cost asc, config order.
        let mut scored = scored;
        sort_ranked(&mut scored);
        let class = ctx.profile.class;
        let scaled_note = if (base_tradeoff - tradeoff).abs() > 0.05 {
            format!(" · tradeoff {base_tradeoff:.0}→{tradeoff:.1} (complexity-scaled)")
        } else {
            String::new()
        };
        Ok(to_ranked(
            scored,
            move |utility, view| {
                // Effective (availability-scaled) preference; negative means
                // the seat is past its plan cap and burning paid overage.
                let pref = if view.preference > 0.0 {
                    format!(" + pref {:.2}", view.preference)
                } else if view.preference < 0.0 {
                    format!(" - pref {:.2} (seat on paid overage)", -view.preference)
                } else {
                    String::new()
                };
                format!(
                    "utility {:.2} = {:.2}×quality {:.2} ({}) + {:.2}×quota (headroom {:.0}%, \
                     cost rank {}){}{}",
                    utility,
                    quality_weight,
                    view.quality,
                    class.as_str(),
                    cost_weight,
                    view.headroom * 100.0,
                    view.cost_rank,
                    pref,
                    scaled_note,
                )
            },
            move |utility, view| {
                serde_json::json!({
                    "utility": utility,
                    "quality_weight": quality_weight,
                    "cost_weight": cost_weight,
                    "base_tradeoff": base_tradeoff,
                    "effective_tradeoff": tradeoff,
                    "quality": view.quality,
                    "headroom": view.headroom,
                    "cost_rank": view.cost_rank,
                    "preference": view.preference,
                })
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::CodingTier;
    use crate::strategies::test_util::{ctx, view};

    fn cfg(tradeoff: f64) -> AutoRouterConfig {
        AutoRouterConfig {
            cost_quality_tradeoff: tradeoff,
            complexity_floor: 0.7,
            allowed_candidates: vec!["*".into()],
            // Tests set complexity explicitly; keep scoring independent of
            // it unless a test opts in.
            complexity_scales_tradeoff: false,
        }
    }

    fn pool() -> Vec<CandidateView> {
        vec![
            view("claude", "haiku", 1, 0, 0.5, CodingTier::Medium, 1.0),
            view("claude", "sonnet", 2, 1, 0.8, CodingTier::High, 1.0),
            view("claude", "opus", 3, 2, 0.9, CodingTier::High, 1.0),
        ]
    }

    #[test]
    fn pure_quality_prefers_best_model() {
        let s = AutoStrategy::new(cfg(0.0));
        let ranked = s.rank(&ctx(), &pool()).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/opus");
    }

    #[test]
    fn pure_cost_prefers_cheapest() {
        let s = AutoStrategy::new(cfg(10.0));
        let ranked = s.rank(&ctx(), &pool()).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/haiku");
    }

    #[test]
    fn complexity_gate_drops_low_quality() {
        let s = AutoStrategy::new(cfg(10.0));
        let mut context = ctx();
        context.profile.complexity = 0.9;
        let ranked = s.rank(&context, &pool()).unwrap();
        // Even at pure-cost tradeoff, the p75 gate removes haiku for a
        // complex task; p75 of [0.5, 0.8, 0.9] is 0.9 -> only opus survives.
        assert_eq!(ranked[0].candidate.to_string(), "claude/opus");
        assert!(!ranked.iter().any(|r| r.candidate.model == "haiku"));
    }

    #[test]
    fn allowed_candidates_glob_filters() {
        let s = AutoStrategy::new(AutoRouterConfig {
            allowed_candidates: vec!["claude/sonnet".into()],
            ..cfg(7.0)
        });
        let ranked = s.rank(&ctx(), &pool()).unwrap();
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].candidate.to_string(), "claude/sonnet");
    }

    #[test]
    fn empty_after_globs_is_an_error() {
        let s = AutoStrategy::new(AutoRouterConfig {
            allowed_candidates: vec!["gemini/*".into()],
            ..cfg(7.0)
        });
        assert!(s.rank(&ctx(), &pool()).is_err());
    }

    #[test]
    fn low_headroom_shifts_choice() {
        let s = AutoStrategy::new(cfg(7.0));
        let mut p = pool();
        p[0].headroom = 0.0; // haiku's agent exhausted
        p[1].headroom = 1.0;
        let ranked = s.rank(&ctx(), &p).unwrap();
        assert_ne!(ranked[0].candidate.to_string(), "claude/haiku");
    }

    #[test]
    fn preference_breaks_ties_between_comparable_candidates() {
        let s = AutoStrategy::new(cfg(7.0));
        let mut p = vec![
            view("codex", "m1", 1, 0, 0.5, CodingTier::Medium, 1.0),
            view("claude", "m2", 1, 1, 0.5, CodingTier::Medium, 1.0),
        ];
        // Identical candidates: codex wins by config order without a
        // preference; a small claude preference flips it.
        let ranked = s.rank(&ctx(), &p).unwrap();
        assert_eq!(ranked[0].candidate.agent, "codex");
        p[1].preference = 0.05;
        let ranked = s.rank(&ctx(), &p).unwrap();
        assert_eq!(ranked[0].candidate.agent, "claude");
        assert!(
            ranked[0].reason.contains("pref 0.05"),
            "{}",
            ranked[0].reason
        );
    }

    #[test]
    fn overage_penalty_prefers_the_free_seat() {
        // The availability-scaled preference arrives here already folded into
        // `view.preference`: claude's 0.1 bonus became a −0.25 penalty (seat
        // on paid overage) while codex still has free plan budget. The free
        // seat must win despite claude's static preference — and the reason
        // string must say why.
        let s = AutoStrategy::new(cfg(3.0));
        let mut p = vec![
            view("claude", "fable", 5, 0, 0.95, CodingTier::High, 1.0),
            view("codex", "sol", 5, 1, 0.93, CodingTier::High, 1.0),
        ];
        p[0].preference = 0.1;
        let ranked = s.rank(&ctx(), &p).unwrap();
        assert_eq!(ranked[0].candidate.agent, "claude");
        p[0].preference = -0.25;
        let ranked = s.rank(&ctx(), &p).unwrap();
        assert_eq!(ranked[0].candidate.agent, "codex");
        let claude = ranked
            .iter()
            .find(|r| r.candidate.agent == "claude")
            .unwrap();
        assert!(
            claude.reason.contains("- pref 0.25 (seat on paid overage)"),
            "{}",
            claude.reason
        );
    }

    #[test]
    fn complexity_scales_tradeoff_toward_quality() {
        let s = AutoStrategy::new(AutoRouterConfig {
            complexity_scales_tradeoff: true,
            // Floor high enough that the p75 gate stays out of the picture:
            // this isolates the tradeoff scaling.
            complexity_floor: 2.0,
            ..cfg(7.0)
        });
        let p = pool();
        // Trivial prompt: cost-heavy, cheap candidate wins.
        let mut context = ctx();
        context.profile.complexity = 0.0;
        let ranked = s.rank(&context, &p).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/haiku");
        // Hard prompt: effective tradeoff 7×(1−0.9)=0.7 -> quality wins.
        context.profile.complexity = 0.9;
        let ranked = s.rank(&context, &p).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/opus");
        assert!(
            ranked[0].reason.contains("complexity-scaled"),
            "{}",
            ranked[0].reason
        );
    }

    #[test]
    fn deterministic_tie_break_by_config_order() {
        let s = AutoStrategy::new(cfg(0.0));
        let p = vec![
            view("a", "m1", 2, 0, 0.8, CodingTier::High, 1.0),
            view("b", "m2", 2, 1, 0.8, CodingTier::High, 1.0),
        ];
        for _ in 0..5 {
            let ranked = s.rank(&ctx(), &p).unwrap();
            assert_eq!(ranked[0].candidate.to_string(), "a/m1");
        }
    }
}
