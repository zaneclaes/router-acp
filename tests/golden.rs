//! Golden characterization tests for the routing pipeline.
//!
//! These lock router-acp's OWN behavior against regressions: a fixed set of
//! representative prompts is run through the real classifier (`data/
//! classifier.yaml`), the real score table (`data/scores.yaml`), and the real
//! `auto` strategy, and the resulting (class, complexity, winner, utility) is
//! asserted verbatim. Change the data tables or the scoring math and these
//! break loudly, forcing an intentional re-bake (regenerate with the ignored
//! `dump_golden` test below).
//!
//! NOTE ON "OPENROUTER PARITY": these are NOT parity tests against OpenRouter.
//! OpenRouter's Auto Router is a closed, NotDiamond-backed hosted service with
//! no published scoring function and no importable code, so no code-level
//! parity is achievable. router-acp's scoring is its own heuristic (it borrows
//! only the `cost_quality_tradeoff` knob's 0–10 scale and default). These
//! tests treat router-acp as its own source of truth.

use agent_client_protocol::schema::v1::ContentBlock;
use router_acp::candidate::{CandidateId, RequiredCaps, ScoreTable, TaskClass};
use router_acp::classifier::{ClassifierRules, ClassifyInput, classify_heuristic};
use router_acp::config::AutoRouterConfig;
use router_acp::strategies::{AutoStrategy, CandidateView, RouteContext, RouterStrategy};

/// The candidate lineup used for every golden case, mirroring the user's live
/// deployment: claude (preference 0.05, the bigger plan) + codex. Tuple is
/// (agent, model, cost_rank, preference).
const CANDIDATES: &[(&str, &str, u32, f64)] = &[
    ("claude", "haiku", 1, 0.05),
    ("claude", "sonnet", 2, 0.05),
    ("claude", "opus[1m]", 4, 0.05),
    ("claude", "claude-fable-5[1m]", 5, 0.05),
    ("codex", "gpt-5.4-mini", 1, 0.0),
    ("codex", "gpt-5.5", 3, 0.0),
];

/// Fixed `auto` config for the golden cases (mirrors the live router.yaml:
/// tradeoff 3, complexity scaling on).
fn auto_cfg() -> AutoRouterConfig {
    AutoRouterConfig {
        cost_quality_tradeoff: 3.0,
        complexity_floor: 0.7,
        allowed_candidates: vec!["*".to_string()],
        complexity_scales_tradeoff: true,
        min_cost_weight: 0.15,
    }
}

struct Outcome {
    class: TaskClass,
    complexity: f64,
    winner: String,
    utility: f64,
}

/// Run the full real pipeline for one prompt with all seats at full headroom.
fn route(prompt: &str) -> Outcome {
    let rules = ClassifierRules::builtin();
    let input = ClassifyInput::from_prompt(&[ContentBlock::from(prompt.to_string())], vec![]);
    let profile = classify_heuristic(&rules, &input);

    let scores = ScoreTable::builtin();
    let views: Vec<CandidateView> = CANDIDATES
        .iter()
        .enumerate()
        .map(|(idx, (agent, model, cost_rank, preference))| {
            let id = CandidateId::new(*agent, *model);
            let resolved = scores.lookup(&id);
            CandidateView {
                id,
                cost_rank: *cost_rank,
                config_index: idx,
                quality: resolved.quality(profile.class),
                coding_tier: resolved.coding_tier,
                headroom: 1.0,
                // Full free plan on every seat — goldens assume no scarcity.
                plan_headroom: Some(1.0),
                on_overage: false,
                preference: *preference,
            }
        })
        .collect();

    let ctx = RouteContext {
        profile: profile.clone(),
        required_caps: RequiredCaps::default(),
        explicit_candidate: None,
    };
    let ranked = AutoStrategy::new(auto_cfg())
        .rank(&ctx, &views)
        .expect("ranking succeeds");
    let top = &ranked[0];
    Outcome {
        class: profile.class,
        complexity: profile.complexity,
        winner: top.candidate.to_string(),
        utility: top.score,
    }
}

/// One golden expectation. Floats are compared with a small tolerance because
/// they are derived, not authored.
struct Golden {
    prompt: &'static str,
    class: TaskClass,
    complexity: f64,
    winner: &'static str,
    utility: f64,
}

/// The frozen table. Regenerate with `cargo test --test golden dump_golden --
/// --ignored --nocapture` and re-bake if a change to the data tables or the
/// scoring math is intentional.
const GOLDEN: &[Golden] = &[
    Golden {
        prompt: "fix the typo in the button label",
        class: TaskClass::UiTweak,
        complexity: 0.00,
        winner: "claude/haiku",
        utility: 0.47,
    },
    Golden {
        prompt: "add a null check in parse() before dereferencing",
        class: TaskClass::Ops,
        complexity: 0.02,
        winner: "claude/haiku",
        utility: 0.47,
    },
    Golden {
        prompt: "rename the variable foo to bar in utils.py",
        class: TaskClass::Refactor,
        complexity: 0.06,
        winner: "claude/haiku",
        utility: 0.52,
    },
    Golden {
        prompt: "write a haiku about the ocean",
        class: TaskClass::CodingGeneral,
        complexity: 0.01,
        winner: "claude/haiku",
        utility: 0.52,
    },
    Golden {
        prompt: "update the README to document the new --verbose flag",
        class: TaskClass::Writing,
        complexity: 0.02,
        winner: "claude/haiku",
        utility: 0.47,
    },
    Golden {
        prompt: "redesign the dashboard layout with responsive breakpoints, a dark theme, \
                      and a new left nav, then update every affected component",
        class: TaskClass::Architecture,
        complexity: 0.72,
        winner: "claude/claude-fable-5[1m]",
        utility: 0.73,
    },
    Golden {
        prompt: "investigate why the integration suite is flaky: there's a race between the \
                      worker pool and the scheduler that only reproduces under load",
        class: TaskClass::Research,
        complexity: 0.26,
        winner: "codex/gpt-5.5",
        utility: 0.56,
    },
    Golden {
        prompt: "implement an OAuth2 login flow with refresh tokens and secure token storage",
        class: TaskClass::Feature,
        complexity: 0.08,
        winner: "claude/haiku",
        utility: 0.52,
    },
    Golden {
        prompt: "design the architecture for a multi-region event bus with exactly-once \
                      delivery and backpressure",
        class: TaskClass::Architecture,
        complexity: 0.39,
        winner: "claude/claude-fable-5[1m]",
        utility: 0.63,
    },
    Golden {
        prompt: "spend as long as you need investigating this deep cross-cutting bug that \
                      spans the parser, the type checker, and the CLI, then propose a fix plan",
        class: TaskClass::BugFix,
        complexity: 0.23,
        winner: "claude/opus[1m]",
        utility: 0.49,
    },
];

// Tolerance exceeds the 0.005 max rounding error of the 2-dp baked values,
// while staying far tighter than any real classifier/scoring change.
fn approx(a: f64, b: f64) -> bool {
    (a - b).abs() < 0.01
}

#[test]
fn golden_routing_table() {
    for g in GOLDEN {
        let o = route(g.prompt);
        assert_eq!(o.class, g.class, "class for prompt: {}", g.prompt);
        assert!(
            approx(o.complexity, g.complexity),
            "complexity for `{}`: got {:.4}, expected ~{:.2}",
            g.prompt,
            o.complexity,
            g.complexity
        );
        assert_eq!(o.winner, g.winner, "winner for prompt: {}", g.prompt);
        assert!(
            approx(o.utility, g.utility),
            "utility for `{}`: got {:.4}, expected ~{:.2}",
            g.prompt,
            o.utility,
            g.utility
        );
    }
}

/// Regression: a long, cross-cutting investigation must never be routed to a
/// `*mini*` model. This is the exact failure the score-table pattern ordering
/// (specific `*mini*` before broad `*gpt-5*`) exists to prevent.
#[test]
fn long_investigation_never_routes_to_mini() {
    let o = route(
        "spend as long as you need investigating this deep cross-cutting bug that spans the \
         parser, the type checker, and the CLI, then propose a fix plan",
    );
    assert!(
        !o.winner.contains("mini"),
        "a hard investigation routed to a mini model: {}",
        o.winner
    );
}

/// Regression for the same root cause at the data layer: the specific
/// `*mini*` score-table pattern must win over the broad `*gpt-5*` one
/// (first-match-wins), so mini and full gpt get DIFFERENT coding quality.
/// If the broad pattern shadowed mini, these would be equal.
#[test]
fn mini_pattern_not_shadowed_by_broad_gpt_pattern() {
    let scores = ScoreTable::builtin();
    let mini = scores
        .lookup(&CandidateId::new("codex", "gpt-5.4-mini"))
        .quality(TaskClass::CodingGeneral);
    let full = scores
        .lookup(&CandidateId::new("codex", "gpt-5.5"))
        .quality(TaskClass::CodingGeneral);
    assert!(
        mini < full,
        "mini ({mini}) should score below full gpt ({full}); the broad *gpt-5* pattern is \
         shadowing the specific *mini* pattern in data/scores.yaml"
    );
}

// Regenerate the golden table with:
//   cargo test --test golden dump_golden -- --ignored --nocapture
#[test]
#[ignore = "prints golden values for baking into the GOLDEN table"]
fn dump_golden() {
    for g in GOLDEN {
        let o = route(g.prompt);
        println!(
            "{:?}\t{:.2}\t{}\t{:.2}\t{}",
            o.class,
            o.complexity,
            o.winner,
            o.utility,
            &g.prompt[..g.prompt.len().min(50)]
        );
    }
}
