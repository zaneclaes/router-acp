//! `planner` strategy: two-phase (planning → implementation) routing.
//!
//! Each phase has its own candidate pool (frontier planners vs workhorse
//! implementers). A complexity-gated crossover lets the other phase's pool
//! bleed in at the extremes, and per-model `model_boosts` entries bias
//! ordering within a pool. Ranking inside the assembled pool delegates to
//! [`AutoStrategy`], so the quality/cost tradeoff mechanism is shared with
//! `router: auto` (no parallel tuning surface).

use crate::candidate::glob_match;
use crate::config::{AutoRouterConfig, PlannerPhase, PlannerRouterConfig};

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

/// Check whether a candidate matches any pattern in `patterns`, considering
/// both the served id and the version-pin alias.
fn matches_any(view: &CandidateView, patterns: &[String]) -> bool {
    view.ids().any(|id| {
        let key = id.to_string();
        patterns.iter().any(|p| glob_match(p, &key))
    })
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

        // 1. Assemble the pool for this phase.
        let (primary, crossover_ok) = match phase {
            PlannerPhase::Planning => {
                let cross = complexity <= cfg.floor_complexity;
                (&cfg.planning_candidates, cross)
            }
            PlannerPhase::Implementation => {
                let cross = complexity >= cfg.apex_complexity;
                (&cfg.implementation_candidates, cross)
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

        // 2. Apply per-model phase boosts.
        for view in &mut pool {
            for boost in &cfg.model_boosts {
                if view
                    .ids()
                    .any(|id| glob_match(&boost.pattern, &id.to_string()))
                {
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
                 (pool: {phase_str}_candidates{crossover_tag}{boost_note}) · {}",
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
/// discussion are included — false positives pin the session to
/// implementation irreversibly.
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

/// Header the planning-phase inject starts with — tests and log greps key on it.
pub const PLAN_PROTOCOL_HEADER: &str = "[router-acp planner protocol]";

/// Built-in plan-first protocol injected whenever a planner session enters
/// Planning. Host `planning_instructions` are appended separately.
pub fn planner_plan_protocol() -> String {
    format!(
        "{PLAN_PROTOCOL_HEADER}\n\
         You are in the PLANNING phase. Investigate the request and present a \
         concrete, reviewable plan before asking for implementation approval.\n\
         \n\
         Required order:\n\
         1. Investigate (read the code, tickets, and docs) until you can name \
         the files, steps, and risks.\n\
         2. Present that plan in this turn as a reviewable artifact: goal, \
         approach, files/systems to change, sequenced steps, open questions, \
         and what done looks like.\n\
         3. Only AFTER the plan is in the conversation, ask one structured \
         question whose only choices are exactly:\n\
         - \"{HANDOFF_PROCEED}\"\n\
         - \"{HANDOFF_REFINE}\"\n\
         4. Do not ask for implementation approval against a plan you have \
         not presented.\n\
         \n\
         Forbidden in this planning turn:\n\
         - Editing, creating, or deleting implementation files\n\
         - Asking for implementation approval before the plan is presented\n\
         - Starting implementation after a \"{HANDOFF_PROCEED}\" answer. End \
         the turn without editing; a follow-up user prompt performs the \
         router switch onto the implementation model.\n\
         \n\
         Do not paraphrase the choice labels."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::{CandidateId, CodingTier, TaskClass};
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
            planning_instructions: String::new(),
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
    fn plan_protocol_names_stable_handoff_choices() {
        let protocol = planner_plan_protocol();
        assert!(protocol.starts_with(PLAN_PROTOCOL_HEADER));
        assert!(protocol.contains(HANDOFF_PROCEED));
        assert!(protocol.contains(HANDOFF_REFINE));
        assert!(protocol.contains("before asking for implementation approval"));
        assert!(
            !protocol.contains("with the current plan"),
            "must not revive the premature current-plan question: {protocol}"
        );
    }

    #[test]
    fn boost_on_pinned_from_alias() {
        let cfg = PlannerRouterConfig {
            model_boosts: vec![PlannerModelBoost {
                pattern: "*opus[1m]*".into(),
                implementation: 1.5,
                planning: 0.0,
            }],
            ..planner_cfg()
        };
        let s = PlannerStrategy::new(cfg, auto_cfg(), 0.1);
        let mut p = pool();
        p[2].pinned_from = Some(CandidateId::new("claude", "opus[1m]"));
        let ctx = ctx_with_phase(PlannerPhase::Implementation, 0.5);
        let ranked = s.rank(&ctx, &p).unwrap();
        let opus = ranked
            .iter()
            .find(|r| r.candidate.agent == "claude")
            .unwrap();
        assert!(
            opus.reason.contains("boost"),
            "should match via pinned_from: {}",
            opus.reason
        );
    }
}
