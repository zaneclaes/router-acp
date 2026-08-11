//! `auto` strategy: behavioral analogue of the OpenRouter Auto router over
//! local candidates.
//!
//! ```text
//! quality_weight = 1 - cost_quality_tradeoff / 10
//! cost_weight = cost_quality_tradeoff / 10
//! quality_demand = min(task_class_base + 2 * complexity, 3)
//! task_fit_quality = normalize(min(quality[class], quality_demand))
//! quota_score = headroom[agent] * (1 - 0.5 * normalized_cost_rank(candidate))
//! utility = quality_weight * task_fit_quality + cost_weight * quota_score
//! ```
//!
//! When `complexity >= complexity_floor`, candidates below the 75th
//! percentile quality score for the task class (among the surviving pool)
//! are dropped first.

use crate::candidate::{QUALITY_MAX, glob_match, quality_demand, quality_utility};
use crate::config::AutoRouterConfig;

use super::{
    CandidateView, RankedCandidate, RouteContext, RouteError, RouterStrategy, sort_ranked,
    to_ranked,
};

pub struct AutoStrategy {
    cfg: AutoRouterConfig,
    cost_aversion: f64,
}

/// Included-plan usage is not marginal dollar spend. Rank remains a bounded
/// pressure against wasting scarce/high-token models, while reported plan
/// headroom and paid-overage penalties carry the real availability economics.
const INCLUDED_PLAN_SCARCITY_WEIGHT: f64 = 0.5;

impl AutoStrategy {
    pub fn new(cfg: AutoRouterConfig) -> Self {
        Self {
            cfg,
            cost_aversion: 0.1,
        }
    }

    pub fn with_cost_aversion(cfg: AutoRouterConfig, cost_aversion: f64) -> Self {
        Self {
            cfg,
            cost_aversion: cost_aversion.clamp(0.0, 1.0),
        }
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

/// Capability above the task's demand is real but has no marginal utility for
/// this routing decision.
fn task_fit_quality(score: f64, demand: f64) -> f64 {
    quality_utility(score.min(demand))
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
            let comparing_paid_overage = pool.iter().any(|candidate| candidate.on_overage);
            // Keep free-plan candidates in the comparison even when they sit
            // below p75. Otherwise the hard quality gate removes Grok before
            // paid-overage aversion can raise the frontier model's difficulty
            // bar, making `cost_aversion` inert exactly when it matters.
            let gated: Vec<&CandidateView> = pool
                .iter()
                .copied()
                .filter(|c| c.quality >= p75 || (comparing_paid_overage && !c.on_overage))
                .collect();
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
        //    quality dominates hard ones. At/above `apex_complexity` the
        //    ranking goes pure quality (tradeoff 0): the score table's
        //    compressed peer pairs hand everyday tie-breaks to the cheaper
        //    member, and the apex is the break-glass where the preferred
        //    member (Fable/Sol) must stay reachable by `auto` itself —
        //    misrouting a novel/severe-risk task costs more than the seat.
        let apex = ctx.profile.complexity.clamp(0.0, 1.0) >= self.cfg.apex_complexity;
        let min_cost = pool.iter().map(|c| c.cost_rank).min().unwrap_or(1) as f64;
        let max_cost = pool.iter().map(|c| c.cost_rank).max().unwrap_or(1) as f64;
        let base_tradeoff = self.cfg.cost_quality_tradeoff.clamp(0.0, 10.0);
        let tradeoff = if apex {
            0.0
        } else if self.cfg.complexity_scales_tradeoff {
            let scaled = base_tradeoff * (1.0 - ctx.profile.complexity.clamp(0.0, 1.0));
            // Floor the scaled tradeoff so a complexity-1.0 classification
            // can't zero the cost term (pure quality-max routes everything
            // to the most expensive candidate). Never raise it above the
            // configured tradeoff.
            let floor = (self.cfg.min_cost_weight.clamp(0.0, 1.0) * 10.0).min(base_tradeoff);
            scaled.max(floor)
        } else {
            base_tradeoff
        };
        let quality_weight = 1.0 - tradeoff / 10.0;
        let cost_weight = tradeoff / 10.0;
        let quality_demand = if cost_weight <= f64::EPSILON {
            QUALITY_MAX
        } else {
            quality_demand(ctx.profile.class, ctx.profile.complexity)
        };

        let scored: Vec<(f64, CandidateView, Option<String>)> = pool
            .into_iter()
            .map(|c| {
                let normalized_cost_rank = if max_cost > min_cost {
                    (c.cost_rank as f64 - min_cost) / (max_cost - min_cost)
                } else {
                    0.0
                };
                let quota_score =
                    c.headroom * (1.0 - INCLUDED_PLAN_SCARCITY_WEIGHT * normalized_cost_rank);
                let normalized_quality = task_fit_quality(c.quality, quality_demand);
                let overage_surcharge = if c.on_overage {
                    self.cost_aversion * (1.0 - ctx.profile.complexity.clamp(0.0, 1.0))
                } else {
                    0.0
                };
                let utility =
                    quality_weight * normalized_quality + cost_weight * quota_score + c.preference
                        - overage_surcharge;
                (utility, c.clone(), gate_note.clone())
            })
            .collect();

        // 5. Deterministic sort: utility desc, effective cost asc, config order.
        let mut scored = scored;
        sort_ranked(&mut scored);
        let class = ctx.profile.class;
        let scaled_note = if apex {
            format!(
                " · apex complexity {:.2} >= {:.2}: pure quality (tradeoff {base_tradeoff:.0}→0)",
                ctx.profile.complexity.clamp(0.0, 1.0),
                self.cfg.apex_complexity
            )
        } else if (base_tradeoff - tradeoff).abs() > 0.05 {
            format!(" · tradeoff {base_tradeoff:.0}→{tradeoff:.1} (complexity-scaled)")
        } else {
            String::new()
        };
        Ok(to_ranked(
            scored,
            move |utility, view| {
                let pref = if view.preference > 0.0 {
                    format!(" + pref {:.2}", view.preference)
                } else if view.preference < 0.0 {
                    format!(" - pref {:.2}", -view.preference)
                } else {
                    String::new()
                };
                let overage = if view.on_overage {
                    let surcharge =
                        self.cost_aversion * (1.0 - ctx.profile.complexity.clamp(0.0, 1.0));
                    format!(" - overage {surcharge:.2}")
                } else {
                    String::new()
                };
                format!(
                    "utility {:.2} = {:.2}×quality {:.2}→{:.2} ({}) + {:.2}×quota (headroom {:.0}%, \
                     cost rank {}){}{}{}",
                    utility,
                    quality_weight,
                    view.quality,
                    task_fit_quality(view.quality, quality_demand),
                    class.as_str(),
                    cost_weight,
                    view.headroom * 100.0,
                    view.cost_rank,
                    pref,
                    overage,
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
                    "apex": apex,
                    "quality": view.quality,
                    "normalized_quality": quality_utility(view.quality),
                    "task_fit_quality": task_fit_quality(view.quality, quality_demand),
                    "quality_demand": quality_demand,
                    "headroom": view.headroom,
                    "cost_rank": view.cost_rank,
                    "preference": view.preference,
                    "on_overage": view.on_overage,
                    "cost_aversion": self.cost_aversion,
                    "overage_surcharge": if view.on_overage {
                        self.cost_aversion * (1.0 - ctx.profile.complexity.clamp(0.0, 1.0))
                    } else {
                        0.0
                    },
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
            min_cost_weight: 0.0,
            // Above any complexity a test sets by default; tests that want
            // the apex path set it explicitly and lower this.
            apex_complexity: 1.1,
        }
    }

    fn pool() -> Vec<CandidateView> {
        vec![
            view("claude", "haiku", 1, 0, 1.0, CodingTier::Medium, 1.0),
            view("claude", "sonnet", 2, 1, 1.4, CodingTier::High, 1.0),
            view("claude", "opus", 3, 2, 2.0, CodingTier::High, 1.0),
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
    fn unused_frontier_capability_has_no_extra_utility() {
        let s = AutoStrategy::new(cfg(3.0));
        let mut context = ctx();
        context.profile.complexity = 0.0;
        let ranked = s.rank(&context, &pool()).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/haiku");
        let trivial = quality_demand(crate::candidate::TaskClass::UiTweak, 0.0);
        let hard = quality_demand(crate::candidate::TaskClass::Architecture, 1.0);
        assert_eq!(
            task_fit_quality(1.0, trivial),
            task_fit_quality(3.5, trivial)
        );
        assert!(task_fit_quality(3.5, hard) > task_fit_quality(1.0, hard));
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
    fn cost_aversion_raises_the_paid_frontier_difficulty_bar() {
        let s = AutoStrategy::with_cost_aversion(
            AutoRouterConfig {
                complexity_scales_tradeoff: true,
                complexity_floor: 0.7,
                min_cost_weight: 0.15,
                ..cfg(3.0)
            },
            0.1,
        );
        let mut p = vec![
            view("claude", "fable", 5, 0, 3.0, CodingTier::High, 0.0),
            view("grok", "grok-4.5", 5, 1, 1.6, CodingTier::High, 1.0),
        ];
        p[0].on_overage = true;

        let mut context = ctx();
        context.profile.class = crate::candidate::TaskClass::Architecture;
        context.profile.complexity = 0.4;
        let ranked = s.rank(&context, &p).unwrap();
        assert_eq!(ranked[0].candidate.agent, "grok");
        let fable = ranked
            .iter()
            .find(|r| r.candidate.agent == "claude")
            .unwrap();
        assert!(fable.reason.contains("- overage 0.06"), "{}", fable.reason);

        context.profile.complexity = 0.9;
        let ranked = s.rank(&context, &p).unwrap();
        assert_eq!(ranked[0].candidate.agent, "claude");

        let no_aversion = AutoStrategy::with_cost_aversion(s.cfg.clone(), 0.0);
        context.profile.complexity = 0.4;
        let ranked = no_aversion.rank(&context, &p).unwrap();
        assert_eq!(ranked[0].candidate.agent, "claude");
        context.profile.complexity = 0.7;
        let ranked = no_aversion.rank(&context, &p).unwrap();
        assert_eq!(ranked[0].candidate.agent, "claude");
    }

    /// The break-glass for a compressed peer pair: below the apex, a real
    /// price gap out-votes a deliberately tiny quality gap; at/above it,
    /// quality alone decides and the compressed-ahead candidate wins even
    /// though it is priced/ranked higher.
    #[test]
    fn apex_complexity_lets_a_compressed_pair_reach_its_ahead_member() {
        let s = AutoStrategy::new(AutoRouterConfig {
            complexity_scales_tradeoff: true,
            complexity_floor: 2.0, // keep the p75 gate out of the picture
            min_cost_weight: 0.25,
            apex_complexity: 0.9,
            ..cfg(3.0)
        });
        // A compressed pair: "ahead" edges "behind" by 0.02, "behind" is
        // twice as cheap (cost_rank 4 vs 5) — mirrors fable/opus and sol/terra.
        let p = vec![
            view("claude", "ahead", 5, 0, 2.66, CodingTier::High, 1.0),
            view("claude", "behind", 4, 1, 2.64, CodingTier::High, 1.0),
        ];
        let mut context = ctx();
        // Below the apex: the compressed gap can't outweigh the price gap.
        context.profile.complexity = 0.5;
        let ranked = s.rank(&context, &p).unwrap();
        assert_eq!(
            ranked[0].candidate.model, "behind",
            "below apex, the cheaper compressed-behind member wins routine work: {}",
            ranked[0].reason
        );

        // At the apex: pure quality, so "ahead" wins despite its cost rank.
        context.profile.complexity = 0.95;
        let ranked = s.rank(&context, &p).unwrap();
        assert_eq!(
            ranked[0].candidate.model, "ahead",
            "at apex complexity, the compressed-ahead member must be reachable by auto \
             itself: {}",
            ranked[0].reason
        );
        assert!(
            ranked[0].reason.contains("apex complexity"),
            "{}",
            ranked[0].reason
        );
    }

    #[test]
    fn apex_complexity_disabled_above_one_never_engages() {
        let s = AutoStrategy::new(AutoRouterConfig {
            complexity_scales_tradeoff: true,
            complexity_floor: 2.0,
            min_cost_weight: 0.25,
            apex_complexity: 1.1, // clamp(complexity, 0, 1) can never reach this
            ..cfg(3.0)
        });
        let p = vec![
            view("claude", "ahead", 5, 0, 2.66, CodingTier::High, 1.0),
            view("claude", "behind", 4, 1, 2.64, CodingTier::High, 1.0),
        ];
        let mut context = ctx();
        context.profile.complexity = 1.0;
        let ranked = s.rank(&context, &p).unwrap();
        assert_eq!(
            ranked[0].candidate.model, "behind",
            "apex_complexity > 1.0 must disable the carve-out entirely: {}",
            ranked[0].reason
        );
        assert!(!ranked[0].reason.contains("apex"), "{}", ranked[0].reason);
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
    fn min_cost_weight_floors_the_scaled_tradeoff() {
        // Complexity 1.0 would scale the tradeoff to 0.0 (pure quality-max);
        // the floor keeps 0.15 of the utility on the cost term, so a
        // near-equal-quality cheaper candidate can still win.
        let s = AutoStrategy::new(AutoRouterConfig {
            complexity_scales_tradeoff: true,
            complexity_floor: 2.0, // keep the p75 gate out of the picture
            min_cost_weight: 0.15,
            ..cfg(3.0)
        });
        let p = vec![
            view("claude", "fable", 5, 0, 2.90, CodingTier::High, 1.0),
            view("claude", "sonnet", 2, 1, 2.88, CodingTier::High, 1.0),
        ];
        let mut context = ctx();
        context.profile.complexity = 1.0;
        let ranked = s.rank(&context, &p).unwrap();
        // normalized quality gap 0.85×(0.02/3) < cost gap 0.15×1.0.
        assert_eq!(ranked[0].candidate.to_string(), "claude/sonnet");
        assert!(
            ranked[0].reason.contains("complexity-scaled"),
            "{}",
            ranked[0].reason
        );
        // Without the floor, fable wins on pure quality — the legacy failure.
        let legacy = AutoStrategy::new(AutoRouterConfig {
            complexity_scales_tradeoff: true,
            complexity_floor: 2.0,
            min_cost_weight: 0.0,
            ..cfg(3.0)
        });
        let ranked = legacy.rank(&context, &p).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/fable");
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

    /// Live regression: Ops complexity 0.18 with Claude at 7% weekly free plan,
    /// Codex at 44%, and unmetered Grok at local 100%. Before the free-plan
    /// cap, Grok won on quota alone despite cost_rank 5. After the cap (applied
    /// in `eligible_views`), Grok's headroom is clipped to 0.44 and a metered
    /// candidate must win.
    #[test]
    fn unmetered_frontier_does_not_beat_free_metered_on_trivial_ops() {
        use crate::candidate::TaskClass;
        use crate::strategies::{cap_unmetered_headroom, test_util::view_metered};

        let s = AutoStrategy::new(AutoRouterConfig {
            cost_quality_tradeoff: 3.0,
            complexity_floor: 0.7, // below 0.18 — no p75 gate
            complexity_scales_tradeoff: true,
            min_cost_weight: 0.15,
            allowed_candidates: vec!["*".into()],
            apex_complexity: 0.9, // above 0.18 — not the apex path
        });
        let mut pool = vec![
            view_metered("claude", "haiku", 1, 0, 1.29, CodingTier::Medium, 0.07),
            view_metered("claude", "sonnet", 2, 1, 1.38, CodingTier::High, 0.07),
            view_metered(
                "codex",
                "gpt-5.4-mini",
                1,
                2,
                1.00,
                CodingTier::Medium,
                0.44,
            ),
            view_metered(
                "codex",
                "gpt-5.6-luna",
                2,
                3,
                2.02,
                CodingTier::Medium,
                0.44,
            ),
            // Unmetered frontier — the pre-fix winner.
            view("grok", "grok-4.5", 5, 4, 1.57, CodingTier::High, 1.0),
        ];
        cap_unmetered_headroom(&mut pool);

        let mut context = ctx();
        context.profile.class = TaskClass::Ops;
        context.profile.complexity = 0.18;
        let ranked = s.rank(&context, &pool).unwrap();
        assert_ne!(
            ranked[0].candidate.to_string(),
            "grok/grok-4.5",
            "Grok must not win trivial Ops while free metered plan remains; got {} ({})",
            ranked[0].candidate,
            ranked[0].reason
        );
        assert!(
            ranked[0].candidate.agent == "codex" || ranked[0].candidate.agent == "claude",
            "expected a metered seat, got {}",
            ranked[0].candidate
        );
        // Without the cap, the same pool pins Grok (documents the bug).
        let mut uncapped = pool.clone();
        if let Some(g) = uncapped.iter_mut().find(|v| v.id.agent == "grok") {
            g.headroom = 1.0;
        }
        let broken = s.rank(&context, &uncapped).unwrap();
        assert_eq!(
            broken[0].candidate.to_string(),
            "grok/grok-4.5",
            "fixture drift: uncapped pool should still prefer Grok"
        );
    }

    /// Live regression (`rtr-7dc8a15f…`, 2026-08-11): Claude's weekly plan
    /// fully spent — every token paid overage — with thousands of overage
    /// dollars left, next to Codex at 99% free weekly. `seat_budget`'s
    /// dollar grading saturated to 1.0, the old ceiling-only cap clipped it
    /// to Codex's 0.99 (a tie), and Claude's +0.1 preference broke the tie:
    /// Opus won a BugFix both Terra and Sol met (demand 2.2 under all three
    /// scores), spending real money with a free equal-fit seat idle. After
    /// the discount, a free codex seat must win; the pre-fix shape is kept
    /// as the documents-the-bug leg.
    #[test]
    fn paid_overage_does_not_beat_untouched_free_plan_on_met_demand() {
        use crate::candidate::TaskClass;
        use crate::strategies::{cap_overage_headroom, test_util::view_metered};

        // Fleet config shape (kory-code/router.yaml).
        let s = AutoStrategy::with_cost_aversion(
            AutoRouterConfig {
                cost_quality_tradeoff: 3.0,
                complexity_floor: 0.7, // above 0.5 — no p75 gate
                complexity_scales_tradeoff: true,
                min_cost_weight: 0.25,
                allowed_candidates: vec!["*".into()],
                apex_complexity: 0.9, // above 0.5 — not the apex path
            },
            0.1,
        );
        // The recorded availability: four Claude seats paying (plan 0.0,
        // saturated budget ceiling-clipped to 0.99, preference 0.1), five
        // Codex seats on an almost untouched weekly plan (0.99).
        let overage_claude = |model: &str, rank, idx, quality| {
            let mut v = view_metered("claude", model, rank, idx, quality, CodingTier::High, 0.0);
            v.on_overage = true;
            v.headroom = 0.99;
            v.preference = 0.1;
            v
        };
        let mut pool = vec![
            overage_claude("haiku", 1, 0, 1.29),
            overage_claude("sonnet", 2, 1, 1.39),
            overage_claude("opus[1m]", 4, 2, 2.97),
            overage_claude("claude-fable-5[1m]", 5, 3, 2.99),
            view_metered(
                "codex",
                "gpt-5.4-mini",
                1,
                4,
                1.00,
                CodingTier::Medium,
                0.99,
            ),
            view_metered(
                "codex",
                "gpt-5.6-luna",
                2,
                5,
                2.02,
                CodingTier::Medium,
                0.99,
            ),
            view_metered("codex", "gpt-5.5", 3, 6, 1.82, CodingTier::High, 0.99),
            view_metered("codex", "gpt-5.6-terra", 4, 7, 2.37, CodingTier::High, 0.99),
            view_metered("codex", "gpt-5.6-sol", 5, 8, 2.39, CodingTier::High, 0.99),
        ];

        let mut context = ctx();
        context.profile.class = TaskClass::BugFix;
        context.profile.complexity = 0.5;

        // The documents-the-bug leg: the pre-discount views (ceiling-only
        // cap already applied) pin Opus on preference + tied headroom.
        let broken = s.rank(&context, &pool).unwrap();
        assert_eq!(
            broken[0].candidate.to_string(),
            "claude/opus[1m]",
            "fixture drift: without the discount the paying seat should win: {}",
            broken[0].reason
        );

        cap_overage_headroom(&mut pool, 0.2);
        let ranked = s.rank(&context, &pool).unwrap();
        assert_eq!(
            ranked[0].candidate.agent, "codex",
            "a free seat meeting the demand must beat a paid-overage seat; got {} ({})",
            ranked[0].candidate, ranked[0].reason
        );
        let opus = ranked
            .iter()
            .find(|r| r.candidate.model == "opus[1m]")
            .unwrap();
        assert!(
            opus.reason.contains("- overage"),
            "paying seat still discloses its surcharge: {}",
            opus.reason
        );
    }
}
