//! `planner` strategy: two-phase (planning → implementation) routing.
//!
//! Each phase has its own candidate pool (frontier planners vs workhorse
//! implementers). A complexity-gated crossover lets the other phase's pool
//! bleed in at the extremes, and per-model `model_boosts` entries bias
//! ordering within a pool. Ranking inside the assembled pool delegates to
//! [`AutoStrategy`], so the quality/cost tradeoff mechanism is shared with
//! `router: auto` (no parallel tuning surface).

use crate::candidate::glob_match;
use crate::config::{AutoRouterConfig, PlannerDifficulty, PlannerPhase, PlannerRouterConfig};

use super::{
    AutoStrategy, CandidateView, RankedCandidate, RouteContext, RouteError, RouterStrategy,
};

pub struct PlannerStrategy {
    planner_cfg: PlannerRouterConfig,
    auto: AutoStrategy,
}

impl PlannerStrategy {
    pub fn new(
        planner_cfg: PlannerRouterConfig,
        auto_cfg: AutoRouterConfig,
        cost_aversion: f64,
    ) -> Self {
        Self {
            planner_cfg,
            auto: AutoStrategy::with_cost_aversion(auto_cfg, cost_aversion),
        }
    }
}

/// Check whether a candidate matches any pattern in `patterns`.
fn matches_any(view: &CandidateView, patterns: &[String]) -> bool {
    let key = view.id.to_string();
    patterns.iter().any(|p| glob_match(p, &key))
}

impl RouterStrategy for PlannerStrategy {
    fn rank(
        &self,
        ctx: &RouteContext,
        candidates: &[CandidateView],
    ) -> Result<Vec<RankedCandidate>, RouteError> {
        let phase = ctx.planner_phase.unwrap_or(PlannerPhase::Planning);
        let complexity = ctx.profile.complexity.clamp(0.0, 1.0);
        let cfg = &self.planner_cfg;

        // 1. Assemble the pool for this phase. A `hard:` / `easy:` prefix
        //    subsets the planning pool and skips floor crossover — the
        //    prefix is an explicit pool choice, not "also admit workers".
        let (primary, crossover_ok, difficulty_tag) = match phase {
            PlannerPhase::Planning => match ctx.planner_difficulty {
                Some(PlannerDifficulty::Easy) => {
                    (&cfg.easy_planning_candidates, false, " · easy: prefix")
                }
                Some(PlannerDifficulty::Hard) => {
                    (&cfg.hard_planning_candidates, false, " · hard: prefix")
                }
                None => {
                    let cross = complexity <= cfg.floor_complexity;
                    (&cfg.planning_candidates, cross, "")
                }
            },
            PlannerPhase::Implementation => {
                let cross = complexity >= cfg.apex_complexity;
                (&cfg.implementation_candidates, cross, "")
            }
        };
        let secondary = match phase {
            PlannerPhase::Planning => &cfg.implementation_candidates,
            PlannerPhase::Implementation => &cfg.planning_candidates,
        };

        let mut pool: Vec<CandidateView> = candidates
            .iter()
            .filter(|c| matches_any(c, primary) || (crossover_ok && matches_any(c, secondary)))
            .cloned()
            .collect();
        // Prefix globs that match nothing in the live catalog (e.g. `*astra*`
        // when Astra isn't configured) must not empty the pool — fall back
        // to the full planning set and say so.
        let mut difficulty_tag = difficulty_tag.to_string();
        if pool.is_empty() && phase == PlannerPhase::Planning && ctx.planner_difficulty.is_some() {
            pool = candidates
                .iter()
                .filter(|c| matches_any(c, &cfg.planning_candidates))
                .cloned()
                .collect();
            difficulty_tag.push_str(" (empty subset; full planning pool)");
        }

        // 2. Apply per-model phase boosts.
        for view in &mut pool {
            for boost in &cfg.model_boosts {
                if glob_match(&boost.pattern, &view.id.to_string()) {
                    let additive = match phase {
                        PlannerPhase::Planning => boost.planning,
                        PlannerPhase::Implementation => boost.implementation,
                    };
                    view.preference += additive;
                }
            }
        }

        // 3. Graceful fallback: if the filtered pool is empty, rank the
        //    full candidate set via AutoStrategy and note the degradation.
        if pool.is_empty() {
            let mut result = self.auto.rank(ctx, candidates)?;
            let note = format!(
                "planner: no {} candidates available; full-pool fallback",
                phase_label(phase)
            );
            for r in &mut result {
                r.reason = format!("{note} · {}", r.reason);
            }
            return Ok(result);
        }

        // 4. Delegate ranking to AutoStrategy.
        let mut result = self.auto.rank(ctx, &pool)?;
        let phase_str = phase_label(phase);
        let crossover_tag = if crossover_ok {
            match phase {
                PlannerPhase::Planning => " + implementation crossover",
                PlannerPhase::Implementation => " + planning crossover",
            }
        } else {
            ""
        };
        for r in &mut result {
            let boost_note = cfg
                .model_boosts
                .iter()
                .find(|b| glob_match(&b.pattern, &r.candidate.to_string()))
                .map(|b| {
                    let val = match phase {
                        PlannerPhase::Planning => b.planning,
                        PlannerPhase::Implementation => b.implementation,
                    };
                    if val.abs() > f64::EPSILON {
                        format!(" + boost {:+.2}", val)
                    } else {
                        String::new()
                    }
                })
                .unwrap_or_default();
            r.reason = format!(
                "planner → {} · phase={phase_str} \
                 (pool: {phase_str}_candidates{crossover_tag}{difficulty_tag}{boost_note}) · {}",
                r.candidate, r.reason
            );
        }
        Ok(result)
    }
}

fn phase_label(phase: PlannerPhase) -> &'static str {
    match phase {
        PlannerPhase::Planning => "planning",
        PlannerPhase::Implementation => "implementation",
    }
}

/// High-precision keyword phrases that strongly signal the user wants
/// execution, not further planning. Matched case-insensitively against the
/// full prompt text. Only phrases that almost never appear in a planning
/// discussion are included. Explicit workflow commands can reverse the phase.
const IMPLEMENTATION_PHRASES: &[&str] = &[
    "implement it",
    "implement this",
    "implement the plan",
    "build it",
    "build this",
    "execute the plan",
    "execute this plan",
    "go ahead and build",
    "start implementing",
    "start building",
    "proceed with implementation",
    "let's implement",
    "let's build",
    "code it up",
    "write the code",
    "ship it",
];

/// Returns true when the prompt text contains a high-confidence signal that
/// the user wants implementation, not planning. Case-insensitive substring.
pub fn heuristic_signals_implementation(text: &str) -> bool {
    let lower = text.to_lowercase();
    IMPLEMENTATION_PHRASES
        .iter()
        .any(|phrase| lower.contains(phrase))
}

/// Stable labels for the planning-phase structured handoff question.
/// The ACP client and Kory Code relay match these strings exactly.
pub const HANDOFF_PROCEED: &str = "Proceed with implementation";
pub const HANDOFF_REFINE: &str = "Refine the plan";
pub const HANDOFF_CREATE_EPIC: &str = "Create EPIC";
/// Legacy synonym for [`HANDOFF_CREATE_EPIC`]. The protocol no longer offers
/// it, but hosts still accept it from older sessions.
pub const HANDOFF_COORDINATE: &str = "Spawn sessions and coordinate";

/// Header the planning-phase inject starts with — tests and log greps key on it.
pub const PLAN_PROTOCOL_HEADER: &str = "[router-acp planner protocol]";

/// Built-in plan-first protocol injected whenever a planner session enters
/// Planning. Host `planning_instructions` are appended separately.
pub fn planner_plan_protocol() -> String {
    format!(
        "{PLAN_PROTOCOL_HEADER}\nYou are in the PLANNING phase. Investigate and present a concrete, reviewable plan. Use the resolved repository policy and create-plan skill. Preserve existing authorization and work state. Do not implement unapproved scope. Mode commands do not cancel assigned children or authorize merge, deployment, or publication."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::{CodingTier, TaskClass};
    use crate::classifier::TaskProfile;
    use crate::config::PlannerModelBoost;
    use crate::strategies::test_util::view;

    fn planner_cfg() -> PlannerRouterConfig {
        PlannerRouterConfig {
            planning_candidates: vec!["*fable*".into(), "*sol*".into()],
            implementation_candidates: vec!["*opus*".into(), "*grok*".into()],
            model_boosts: vec![],
            phase_upgrade_confidence: 0.7,
            apex_complexity: 0.85,
            floor_complexity: 0.15,
            easy_planning_candidates: vec!["*opus*".into(), "*sol*".into()],
            hard_planning_candidates: vec!["*astra*".into(), "*fable*".into()],
            planning_instructions: String::new(),
            ..Default::default()
        }
    }

    fn auto_cfg() -> AutoRouterConfig {
        AutoRouterConfig {
            cost_quality_tradeoff: 3.0,
            complexity_floor: 0.7,
            allowed_candidates: vec!["*".into()],
            complexity_scales_tradeoff: false,
            min_cost_weight: 0.0,
            apex_complexity: 1.1,
        }
    }

    fn ctx_with_phase(phase: PlannerPhase, complexity: f64) -> RouteContext {
        RouteContext {
            profile: TaskProfile {
                class: TaskClass::CodingGeneral,
                complexity,
                languages: vec![],
                effort: None,
            },
            required_caps: Default::default(),
            explicit_candidate: None,
            explicit_source: None,
            planner_phase: Some(phase),
            planner_difficulty: None,
        }
    }

    fn pool() -> Vec<CandidateView> {
        vec![
            view(
                "claude",
                "claude-fable-5",
                5,
                0,
                2.99,
                CodingTier::High,
                1.0,
            ),
            view("codex", "gpt-5.6-sol", 5, 1, 2.81, CodingTier::High, 1.0),
            view("claude", "opus[1m]", 4, 2, 2.97, CodingTier::High, 1.0),
            view("grok", "grok-4.6", 5, 3, 1.60, CodingTier::High, 1.0),
        ]
    }

    #[test]
    fn planning_phase_only_admits_planning_candidates() {
        let s = PlannerStrategy::new(planner_cfg(), auto_cfg(), 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Planning, 0.5);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert!(ranked.iter().all(|r| {
            r.candidate.to_string().contains("fable") || r.candidate.to_string().contains("sol")
        }));
        assert_eq!(ranked.len(), 2);
    }

    #[test]
    fn implementation_phase_only_admits_implementation_candidates() {
        let s = PlannerStrategy::new(planner_cfg(), auto_cfg(), 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Implementation, 0.5);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert!(ranked.iter().all(|r| {
            r.candidate.to_string().contains("opus") || r.candidate.to_string().contains("grok")
        }));
        assert_eq!(ranked.len(), 2);
    }

    #[test]
    fn apex_crossover_admits_planning_candidates_during_implementation() {
        // Disable the auto p75 quality gate so crossover pool membership is
        // the only filter.
        let mut acfg = auto_cfg();
        acfg.complexity_floor = 2.0;
        let s = PlannerStrategy::new(planner_cfg(), acfg, 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Implementation, 0.90);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert_eq!(ranked.len(), 4, "all candidates should be in the pool");
    }

    #[test]
    fn floor_crossover_admits_implementation_candidates_during_planning() {
        let mut acfg = auto_cfg();
        acfg.complexity_floor = 2.0;
        let s = PlannerStrategy::new(planner_cfg(), acfg, 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Planning, 0.10);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert_eq!(ranked.len(), 4, "all candidates should be in the pool");
    }

    #[test]
    fn no_crossover_at_normal_complexity() {
        let s = PlannerStrategy::new(planner_cfg(), auto_cfg(), 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Implementation, 0.5);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert_eq!(ranked.len(), 2);
    }

    #[test]
    fn boost_dominates_ordering() {
        let cfg = PlannerRouterConfig {
            model_boosts: vec![PlannerModelBoost {
                pattern: "*grok*".into(),
                implementation: 2.0,
                planning: 0.0,
            }],
            ..planner_cfg()
        };
        let s = PlannerStrategy::new(cfg, auto_cfg(), 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Implementation, 0.5);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert_eq!(
            ranked[0].candidate.to_string(),
            "grok/grok-4.6",
            "+2.0 boost must outrank any same-pool peer: {}",
            ranked[0].reason
        );
    }

    #[test]
    fn boost_only_applies_in_correct_phase() {
        let cfg = PlannerRouterConfig {
            model_boosts: vec![PlannerModelBoost {
                pattern: "*grok*".into(),
                implementation: 2.0,
                planning: 0.0,
            }],
            ..planner_cfg()
        };
        let s = PlannerStrategy::new(cfg, auto_cfg(), 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Planning, 0.5);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert!(ranked.iter().all(|r| r.candidate.agent != "grok"));
    }

    #[test]
    fn empty_pool_falls_back_to_full_ranking() {
        let cfg = PlannerRouterConfig {
            planning_candidates: vec!["*nonexistent*".into()],
            ..planner_cfg()
        };
        let s = PlannerStrategy::new(cfg, auto_cfg(), 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Planning, 0.5);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert!(!ranked.is_empty());
        assert!(
            ranked[0].reason.contains("full-pool fallback"),
            "{}",
            ranked[0].reason
        );
    }

    fn ctx_with_difficulty(difficulty: PlannerDifficulty) -> RouteContext {
        let mut ctx = ctx_with_phase(PlannerPhase::Planning, 0.5);
        ctx.planner_difficulty = Some(difficulty);
        ctx
    }

    #[test]
    fn easy_prefix_restricts_planning_pool_to_opus_and_sol() {
        let s = PlannerStrategy::new(PlannerRouterConfig::default(), auto_cfg(), 0.1);
        let ranked = s
            .rank(&ctx_with_difficulty(PlannerDifficulty::Easy), &pool())
            .unwrap();
        assert!(
            ranked.iter().all(|r| {
                let id = r.candidate.to_string();
                id.contains("opus") || id.contains("sol")
            }),
            "easy: must not admit fable: {:?}",
            ranked
                .iter()
                .map(|r| r.candidate.to_string())
                .collect::<Vec<_>>()
        );
        assert!(
            ranked.iter().any(|r| r.reason.contains("easy: prefix")),
            "{}",
            ranked[0].reason
        );
        assert!(!ranked.is_empty());
    }

    #[test]
    fn hard_prefix_restricts_planning_pool_to_fable() {
        let s = PlannerStrategy::new(PlannerRouterConfig::default(), auto_cfg(), 0.1);
        let ranked = s
            .rank(&ctx_with_difficulty(PlannerDifficulty::Hard), &pool())
            .unwrap();
        assert_eq!(ranked.len(), 1, "astra isn't in the pool; fable only");
        assert!(ranked[0].candidate.to_string().contains("fable"));
        assert!(
            ranked[0].reason.contains("hard: prefix"),
            "{}",
            ranked[0].reason
        );
    }

    #[test]
    fn no_prefix_keeps_full_planning_pool() {
        let s = PlannerStrategy::new(PlannerRouterConfig::default(), auto_cfg(), 0.1);
        let ranked = s
            .rank(&ctx_with_phase(PlannerPhase::Planning, 0.5), &pool())
            .unwrap();
        assert!(
            ranked
                .iter()
                .any(|r| r.candidate.to_string().contains("opus"))
        );
        assert!(
            ranked
                .iter()
                .any(|r| r.candidate.to_string().contains("fable"))
        );
        assert!(
            ranked
                .iter()
                .all(|r| !r.reason.contains("easy: prefix") && !r.reason.contains("hard: prefix"))
        );
    }

    #[test]
    fn default_planning_pool_includes_opus() {
        let cfg = PlannerRouterConfig::default();
        assert!(
            cfg.planning_candidates.iter().any(|p| p == "*opus*"),
            "opus is an everyday planner: {:?}",
            cfg.planning_candidates
        );
        assert!(
            cfg.implementation_candidates.iter().any(|p| p == "*opus*"),
            "opus remains a workhorse: {:?}",
            cfg.implementation_candidates
        );
        let s = PlannerStrategy::new(cfg, auto_cfg(), 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Planning, 0.5);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert!(
            ranked
                .iter()
                .any(|r| r.candidate.to_string().contains("opus")),
            "planning phase must admit opus: {:?}",
            ranked
                .iter()
                .map(|r| r.candidate.to_string())
                .collect::<Vec<_>>()
        );
        assert!(
            ranked.iter().all(|r| {
                let id = r.candidate.to_string();
                id.contains("opus") || id.contains("fable") || id.contains("sol")
            }),
            "grok stays an implementation worker: {:?}",
            ranked
                .iter()
                .map(|r| r.candidate.to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(ranked.len(), 3);
    }

    #[test]
    fn default_phase_is_planning() {
        let s = PlannerStrategy::new(planner_cfg(), auto_cfg(), 0.1);
        let ctx = RouteContext {
            profile: TaskProfile {
                class: TaskClass::CodingGeneral,
                complexity: 0.5,
                languages: vec![],
                effort: None,
            },
            required_caps: Default::default(),
            explicit_candidate: None,
            explicit_source: None,
            planner_phase: None,
            planner_difficulty: None,
        };
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert!(ranked.iter().all(|r| {
            r.candidate.to_string().contains("fable") || r.candidate.to_string().contains("sol")
        }));
    }

    #[test]
    fn reason_strings_include_phase_and_pool() {
        let s = PlannerStrategy::new(planner_cfg(), auto_cfg(), 0.1);
        let ctx = ctx_with_phase(PlannerPhase::Implementation, 0.5);
        let ranked = s.rank(&ctx, &pool()).unwrap();
        assert!(ranked[0].reason.contains("phase=implementation"));
        assert!(ranked[0].reason.contains("implementation_candidates"));
    }

    #[test]
    fn heuristic_matches_common_phrases() {
        assert!(heuristic_signals_implementation("implement it"));
        assert!(heuristic_signals_implementation("OK, let's build this"));
        assert!(heuristic_signals_implementation("Go ahead and BUILD it"));
        assert!(heuristic_signals_implementation("start implementing now"));
    }

    #[test]
    fn heuristic_does_not_match_planning_text() {
        assert!(!heuristic_signals_implementation(
            "draft a plan for the feature"
        ));
        assert!(!heuristic_signals_implementation(
            "what should we implement?"
        ));
        assert!(!heuristic_signals_implementation(
            "discuss the implementation approach"
        ));
    }

    #[test]
    fn plan_protocol_is_repository_neutral() {
        let protocol = planner_plan_protocol();
        assert!(protocol.starts_with(PLAN_PROTOCOL_HEADER));
        assert!(protocol.contains("resolved repository policy"));
        assert!(!protocol.contains("Linear"));
        assert!(!protocol.contains(HANDOFF_CREATE_EPIC));
        assert!(
            !protocol.contains(HANDOFF_COORDINATE),
            "legacy coordinate label must not be offered: {protocol}"
        );
        assert!(protocol.contains("review"));
        assert!(!protocol.contains("ticket-bound"));
        assert!(
            !protocol.contains("with the current plan"),
            "must not revive the premature current-plan question: {protocol}"
        );
    }
}
