//! `static` strategy: pick the explicit session candidate if set, otherwise
//! the configured default. No silent substitution unless
//! `static.allow_fallback = true`.

use crate::candidate::CandidateId;
use crate::config::StaticRouterConfig;

use super::{
    CandidateView, OverrideSource, RankedCandidate, RouteContext, RouteError, RouterStrategy,
};

pub struct StaticStrategy {
    cfg: StaticRouterConfig,
}

impl StaticStrategy {
    pub fn new(cfg: StaticRouterConfig) -> Self {
        Self { cfg }
    }
}

impl RouterStrategy for StaticStrategy {
    fn rank(
        &self,
        ctx: &RouteContext,
        candidates: &[CandidateView],
    ) -> Result<Vec<RankedCandidate>, RouteError> {
        let chosen: Option<CandidateId> = ctx
            .explicit_candidate
            .clone()
            .or_else(|| self.cfg.candidate.as_deref().and_then(CandidateId::parse));
        let Some(chosen) = chosen else {
            return Err(RouteError(
                "static routing selected but no candidate configured: set \
                 `routers.static.candidate` or the `router.candidate` session option"
                    .into(),
            ));
        };

        let explicit = ctx.explicit_candidate.is_some();
        let mut ranked = Vec::new();
        if let Some(view) = candidates.iter().find(|c| c.id == chosen) {
            ranked.push(RankedCandidate {
                candidate: view.id.clone(),
                score: 1.0,
                weights: serde_json::json!({ "explicit": explicit }),
                reason: if explicit {
                    match &ctx.explicit_source {
                        Some(OverrideSource::Skill(name)) => {
                            format!("steered by skill `{name}` routing")
                        }
                        Some(OverrideSource::Planner) => {
                            "orchestration planner (auto-orchestrate)".to_string()
                        }
                        Some(OverrideSource::UserPick) | None => {
                            "explicitly selected via router.candidate".to_string()
                        }
                    }
                } else {
                    "configured static candidate".to_string()
                },
                note: None,
            });
        } else if !self.cfg.allow_fallback {
            return Err(RouteError(format!(
                "static candidate `{chosen}` is not routeable (not verified, quarantined, or \
                 lacking a required capability); set `routers.static.allow_fallback: true` to \
                 allow substitution, or authenticate/fix the candidate"
            )));
        }

        if self.cfg.allow_fallback {
            let mut rest: Vec<&CandidateView> =
                candidates.iter().filter(|c| c.id != chosen).collect();
            rest.sort_by_key(|c| c.config_index);
            ranked.extend(rest.into_iter().map(|c| RankedCandidate {
                candidate: c.id.clone(),
                score: 0.0,
                weights: serde_json::json!({ "explicit": false, "fallback": true }),
                reason: "config-order fallback".to_string(),
                note: Some(format!("fallback: static candidate `{chosen}` unavailable")),
            }));
        }

        if ranked.is_empty() {
            return Err(RouteError(format!(
                "static candidate `{chosen}` is not routeable and no fallback is available"
            )));
        }
        Ok(ranked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::CodingTier;
    use crate::strategies::test_util::{ctx, view};

    fn pool() -> Vec<CandidateView> {
        vec![
            view("claude", "sonnet", 2, 0, 0.8, CodingTier::High, 1.0),
            view("claude", "opus", 3, 1, 0.9, CodingTier::High, 1.0),
        ]
    }

    #[test]
    fn picks_configured_default() {
        let s = StaticStrategy::new(StaticRouterConfig {
            candidate: Some("claude/opus".into()),
            allow_fallback: false,
        });
        let ranked = s.rank(&ctx(), &pool()).unwrap();
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].candidate.to_string(), "claude/opus");
    }

    #[test]
    fn explicit_session_candidate_wins() {
        let s = StaticStrategy::new(StaticRouterConfig {
            candidate: Some("claude/opus".into()),
            allow_fallback: false,
        });
        let mut context = ctx();
        context.explicit_candidate = Some(crate::candidate::CandidateId::new("claude", "sonnet"));
        let ranked = s.rank(&context, &pool()).unwrap();
        assert_eq!(ranked[0].candidate.to_string(), "claude/sonnet");
    }

    /// Reason for an explicit pin, with the given provenance.
    fn explicit_reason(source: Option<OverrideSource>) -> String {
        let s = StaticStrategy::new(StaticRouterConfig {
            candidate: Some("claude/opus".into()),
            allow_fallback: false,
        });
        let mut context = ctx();
        context.explicit_candidate = Some(crate::candidate::CandidateId::new("claude", "sonnet"));
        context.explicit_source = source;
        s.rank(&context, &pool()).unwrap()[0].reason.clone()
    }

    #[test]
    fn user_pick_reason_names_the_session_option() {
        let expected = "explicitly selected via router.candidate";
        assert_eq!(explicit_reason(Some(OverrideSource::UserPick)), expected);
        // No provenance recorded (e.g. rehydrated session): keep the old string.
        assert_eq!(explicit_reason(None), expected);
    }

    #[test]
    fn skill_steered_reason_names_the_skill() {
        assert_eq!(
            explicit_reason(Some(OverrideSource::Skill("ship-pr".into()))),
            "steered by skill `ship-pr` routing"
        );
    }

    #[test]
    fn planner_steered_reason_names_orchestration() {
        assert_eq!(
            explicit_reason(Some(OverrideSource::Planner)),
            "orchestration planner (auto-orchestrate)"
        );
    }

    #[test]
    fn unrouteable_candidate_errors_without_fallback() {
        let s = StaticStrategy::new(StaticRouterConfig {
            candidate: Some("codex/gpt".into()),
            allow_fallback: false,
        });
        let err = s.rank(&ctx(), &pool()).unwrap_err();
        assert!(err.0.contains("codex/gpt"));
        assert!(err.0.contains("allow_fallback"));
    }

    #[test]
    fn fallback_appends_config_order() {
        let s = StaticStrategy::new(StaticRouterConfig {
            candidate: Some("codex/gpt".into()),
            allow_fallback: true,
        });
        let ranked = s.rank(&ctx(), &pool()).unwrap();
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].candidate.to_string(), "claude/sonnet");
        assert_eq!(ranked[1].candidate.to_string(), "claude/opus");
        assert!(ranked[0].note.as_deref().unwrap().contains("fallback"));
    }
}
