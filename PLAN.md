# PLAN — orchestrated multi-model workflows (July 10, 2026)

> The original router-acp build spec (HAI-6345) was completed and removed;
> see `README.md`/`AGENTS.md` for what shipped. This plan covers the
> follow-on: goose-driven orchestration with cross-lineage review.

## Goal

When the user issues a compound prompt ("Fix the bugs with issue X: (1)…
(2)… (3)…"):

1. A **frontier model** (opus/fable) acts as orchestrator: decomposes the
   work into self-contained subtasks, each with a contextual prompt and
   acceptance checks.
2. Each subtask runs in its **own session** (fresh context), routed by
   router-acp per its actual complexity — mechanical fixes land on cheap
   models, hard ones on frontier models. Independent subtasks run in
   parallel.
3. A **different-lineage frontier model** (codex when claude planned)
   reviews the *net result* against success criteria it re-derives from the
   ORIGINAL prompt — not just "did each subtask finish".
4. Only on an approving verdict do submission steps run (push branch,
   create PR).

Success = planner and reviewer provably on different models/lineages,
router-acp choosing appropriate models per subtask, quality up (independent
cross-model review) and usage down (frontier models only plan/review;
execution is delegated to well-scoped cheap sessions).

## Division of labor

Rely on **goose tooling** for orchestration mechanics; router-acp adds one
primitive. Verified against goose 1.41 source:

* **Subrecipes** (`sub_recipes:` in a recipe) execute in **isolated
  sessions** with their own context; recipes with subrecipes auto-inject
  the `summon` platform extension (the `delegate` tool).
* **Parallelism**: multiple calls of the *same* subrecipe with different
  parameters run concurrently (in-process workers, ≤10); different
  subrecipes run in parallel when the prompt says "in parallel".
* **Provider resolution per spawned task**: `params.provider` → subrecipe
  `settings.goose_provider` → `GOOSE_SUBAGENT_PROVIDER` env → parent
  session's provider. Subtasks therefore inherit the router provider
  (`pi-acp`) by default — every subtask session is routed independently —
  and a subrecipe can hard-pin another provider (e.g. `codex-acp`) when
  needed.
* **Structured outputs**: subrecipes support `response.json_schema`, so the
  reviewer returns a machine-checkable verdict.
* Recipes support parameters with Jinja-style templating, per-recipe
  `settings` (provider/model/temperature/max_turns), and bounded loops via
  instructions (the plan→critique→adjudicate idiom already used in the
  user's qwen-critic recipes).

## Milestone 9 — prompt routing directives (router-acp change) ✅ implemented

Clients like the goose CLI cannot set ACP session config options before the
first prompt, so recipes need an in-band way to steer routing. Directives
sit on the first line of a prompt's first text block:

```
[router: candidate=claude/claude-fable-5[1m]]
[router: strategy=pareto-code]
[router: exclude=claude|codex/gpt-5.4-mini, strategy=auto]
```

Semantics:

* Parsed only by router-acp; the directive line is **stripped** before
  classification and never forwarded to the downstream model.
* `candidate` = explicit pin (same as the `router.candidate` session
  option). `strategy` sets the session strategy. `exclude` adds
  session-scoped exclusion patterns (agent name or candidate glob,
  `|`-separated) filtering the pool for the pin **and any failover
  re-pins** — the "different lineage than the planner" mechanism.
* Applied only pre-pin; post-pin directives are stripped and acknowledged
  with a visible "ignored (session already pinned)" note.
* Invalid directives fail the prompt with a clear `invalid_params` error —
  recipes must be deterministic, never silently mis-routed.
* Applied directives appear in the routing disclosure and the state file's
  `routing.excluded` record.

Tests: explicit-candidate pin via directive (bracketed model ids included),
lineage exclusion under pure-quality ranking, directive stripped from the
downstream prompt (mock log asserted), invalid-key loud failure, post-pin
ignore notice.

## Milestone 10 — soft preference, mid-session switching, auto-upgrade, skill routing ✅ implemented

Four related router-acp changes, all landed and tested:

* **`prefer=` directive** — a *soft* candidate. Unlike `candidate=` (hard pin
  or error), `prefer` moves the named candidate to the front of the ranked
  chain when it's eligible and otherwise falls back to the strategy's normal
  winner. The `review.subrecipe.yaml` reviewer uses
  `prefer={{ reviewer_candidate }}` (default `codex/gpt-5.5`) with
  `exclude={{ planner_lineage }}`, so review targets a specific model but never
  blocks when it's cordoned/down. `orchestrate.yaml` gains a matching
  `reviewer_candidate` param, threaded into the subrecipe values.
* **`switch=` directive + `switch_pin` primitive** — deliberate mid-session
  model change. ACP has no transcript handoff, so `switch_pin` asks the current
  model for a handoff summary (captured, not relayed), opens a fresh downstream
  on the target, prepends the summary to the next prompt (`pending_context`),
  re-pins, and closes the old session. Pre-pin, `switch=` degrades to
  `candidate=`.
* **Auto-upgrade** — after each turn, `confidence = pinned_quality − struggle`
  (struggle rises on MaxTokens/Refusal stop reasons and ≥3 in-turn tool
  failures). Below `auto_upgrade.confidence_threshold` (default `0.55`) the
  router queues an upgrade to the best strictly-more-capable eligible candidate
  and switches on the next prompt. `auto_upgrade.enabled: false` disables it;
  explicit `switch=` still works.
* **`skill_routing`** — `[{pattern, candidates}]` forces prompts that invoke a
  skill (matched as `/name` or a standalone token) onto a class of models
  (candidate globs, preference order). Mid-session it switches; pre-pin it
  steers the initial routing. Degrades gracefully when none are routeable.

Tests: `switch_directive_hands_off_to_new_model_mid_session`,
`skill_routing_switches_pinned_session_to_required_class`,
`low_confidence_pin_auto_upgrades_to_a_more_capable_model`,
`auto_upgrade_disabled_keeps_the_pinned_model`, plus `directive_tests` unit
coverage for `prefer`/`switch` parsing and skill/candidate matching.

## Orchestration recipes (goose-native, shipped in `goose/recipes/`)

* `orchestrate.yaml` — the conductor. Run through the router; its prompt
  opens with `[router: candidate={{ planner_candidate }}]` so planning and
  adjudication happen on the frontier model. Phases: investigate →
  decompose into a plan file (`.plans/orchestrator-*.md`) with restated
  success criteria and per-subtask contextual prompts → fan subtasks out
  via `implement_subtask` (parallel where independent) → `review_work` →
  fix rounds bounded by `{{ max_fix_rounds }}` → gated submission
  (`submit=never|branch|pr`).
* `subrecipes/subtask.subrecipe.yaml` — scoped executor. **No provider
  override**: inherits the router, so router-acp classifies each contextual
  prompt and picks the model. Returns structured JSON (status, summary,
  files_changed, checks).
* `subrecipes/review.subrecipe.yaml` — independent reviewer. Prompt opens
  with `[router: prefer={{ reviewer_candidate }}, exclude={{ planner_lineage }}]`,
  guaranteeing a different lineage than the planner (and preferring a specific
  reviewer model, softly) while keeping router benefits (cordons,
  failover). Re-derives success criteria from the ORIGINAL prompt, verifies
  the net working-tree result (diff review + running checks), returns a
  verdict schema (`approve`/`revise`/`block`, blocking_issues with
  evidence, confidence).

## Non-goals / constraints

* Orchestration phases stay isolated by design: each phase is its own session
  (context isolation between phases is a feature — the reviewer must not inherit
  the planner's reasoning). Mid-session switching (Milestone 10) is a separate,
  opt-in capability for *within* a single conversation; it re-pins via a
  summary rather than transferring a transcript, and the orchestration recipes
  deliberately do not use it across phases.
* Subtask context comes from the orchestrator's written contextual prompts,
  not shared history (subrecipe sessions are isolated by goose design).
* Submission is always gated on the reviewer verdict and never force-pushes.
* router-acp's own `delegate_task` MCP tool remains available *within* any
  session for micro-delegation; the recipes are the macro-orchestration
  layer above it.

## Economics check

Compared to running the whole compound task on fable/opus in one session:
the frontier model spends turns only on planning, adjudication, and
integration review; the N execution legs run on auto-routed (mostly cheap
or mid) models with small, well-scoped contexts; the second frontier model
spends one bounded session on review. Parallel subtasks also cut wall-clock
time. Compared to running everything cheap: the two frontier bookends
catch decomposition and integration errors — the failure mode that made
single-cheap-model runs expensive in practice (redone work).
