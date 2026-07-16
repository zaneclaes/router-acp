# Orchestration: plan → parallel subtasks → cross-lineage review → submit

How to make goose decompose a compound prompt into router-routed
sub-sessions, planned by one frontier model and verified by a *different
lineage* frontier model, with submission gated on the verdict.

```
you: "Fix the bugs with issue X: (1)… (2)… (3)…"
        │
        ▼
┌──────────────────────────── goose session (pi-acp → router-acp) ───────────┐
│ ORCHESTRATOR  [router: candidate=claude/claude-fable-5[1m]]                │
│ investigates, writes plan + success criteria + 3 contextual prompts        │
│        │ delegate (summon) — same subrecipe, 3 param sets ⇒ PARALLEL       │
│        ├─────────────┬─────────────┐                                       │
│        ▼             ▼             ▼      (isolated sessions, inherited    │
│   subtask 1      subtask 2     subtask 3   provider ⇒ each independently   │
│   router picks   router picks  router picks  routed by ITS OWN prompt)    │
│   e.g. haiku     e.g. sonnet   e.g. gpt-5.5                                │
│        └─────────────┴─────────────┘                                       │
│        ▼                                                                   │
│ REVIEWER  [router: exclude=claude]  ⇒ lands on codex lineage               │
│ re-derives criteria from the ORIGINAL prompt, diffs, runs tests,           │
│ returns {verdict, blocking_issues, evidence, confidence}                   │
│        ▼                                                                   │
│ fix rounds (bounded) → on approve: push / open PR / merge (gated)          │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Two ways to run this

There are now **two** implementations of the same plan → subtasks → review →
submit pipeline:

1. **This goose recipe** (`orchestrate.yaml`) — explicit, richest version. Uses
   goose subrecipes (`summon`) for isolated sessions, structured JSON verdicts,
   and the `developer` shell for git/PR/merge. Run it deliberately with
   `goose run --recipe orchestrate.yaml`. Everything below documents this path.
2. **Router-native auto-orchestration** (`orchestration.enabled` in
   `router.yaml`) — implicit. router-acp detects a multi-part **task list** in
   any prompt and recreates the pipeline in-process: it switches the session to
   a planner model and injects the protocol, and the planner drives the router's
   own `delegate_task` / `delegate_followup` tools (with same-/higher-tier peer
   delegation enabled so the cross-lineage review is routeable). No recipe, no
   `summon` — it works from **any** ACP client and plain chat. See
   [`ROUTERS.md`](ROUTERS.md) § *Auto-orchestration of task lists* and the
   `orchestration.*` keys in [`README.md`](README.md).

Use the recipe when you want the structured, submit-gated CI-grade run; rely on
auto-orchestration for the everyday "here's a list of things" prompt. They share
the router's delegation, routing, cordons, and disclosures.

## What this is built from

Everything except one small router feature is **stock goose tooling**:

| capability | goose mechanism |
| --- | --- |
| Decomposition + per-subtask contextual prompts | a **recipe** whose instructions make the model write the plan and call subrecipe tools (`sub_recipes:` auto-injects the `summon` extension's `delegate` tool) |
| Sub-agents in separate sessions | **subrecipes execute in isolated sessions** — no shared history/state with the parent or each other (in-process tokio tasks, own session objects) |
| Parallel execution | same subrecipe called with different parameter sets runs **concurrently across up to 10 workers**; different subrecipes run in parallel when the prompt says "in parallel" |
| Per-phase provider/model | subagent provider resolution: `params.provider` → subrecipe `settings.goose_provider` → `GOOSE_SUBAGENT_PROVIDER` env → **parent's provider**. Subtasks declare nothing, so they inherit the router |
| Structured verdicts | subrecipe `response.json_schema` |
| Bounded improve-loops | recipe instructions (the same draft→critique→adjudicate idiom as your `plan-with-qwen-critic` recipe) |
| Submission tooling | the `developer` extension's shell (git, `gh pr create`, `gh pr merge` — the merge is deferred until the reviewer approves) |

The one router-acp addition (Milestone 9 in `PLAN.md`, implemented and
tested): **prompt routing directives**, because the goose CLI cannot set
ACP session config options before the first prompt:

```
[router: candidate=claude/claude-fable-5[1m]]     # pin this session
[router: prefer=codex/gpt-5.5]                     # soft preference (falls back)
[router: strategy=pareto-code]                    # set session strategy
[router: exclude=claude|codex/gpt-5.4-mini]       # ban lineages/candidates
```

On its own line in a prompt (goose prepends a `<turn-context>` preamble, so
the router matches the directive on any line, not just line 1); parsed and
**stripped by the router** (downstream models never see it); applied pre-pin
only; invalid directives fail loudly;
`exclude` persists for the session including failover re-pins; everything
lands in the disclosure line and `sessions.json`. This is what lets a
recipe deterministically put the planner on fable and force the reviewer
onto a non-claude lineage **while keeping router benefits** (cordons,
headroom, failover, disclosures) for every phase.

## Why each model ends up where it does

- **Planner = fable/opus** — pinned by the orchestrate recipe's
  `[router: candidate={{ planner_candidate }}]` directive.
- **Subtasks = routed per complexity** — subtask sessions inherit the
  router provider with *no* directive, so router-acp classifies each
  contextual prompt on its own merits: "rename this flag in cli.rs" reads
  trivial → haiku/mini; "rework the retry state machine" reads hard →
  frontier. The orchestrator is explicitly instructed to write honest
  prompts because *routing reads them*.
- **Reviewer = preferred non-planner candidate** — the review prompt carries
  `[router: prefer={{ reviewer_candidate }}, exclude={{ planner_lineage }}]`.
  `exclude` removes the whole planner agent from the pool; `prefer`
  (default `codex/gpt-5.5`) then puts that specific reviewer at the front of
  the ranked chain **if it is eligible**. This is a *soft* preference: if the
  preferred reviewer is cordoned (token limit), down, or itself excluded, auto
  routing falls back to the best remaining non-planner candidate rather than
  failing — the review never blocks on one model, but it never silently reviews
  on the planner's own lineage either. Set `reviewer_candidate` to steer it,
  the same way `planner_candidate` steers the planner.
- Each phase is a **separate session by design**: ACP pins one model per
  session, and the reviewer *must not* inherit the planner's context to be
  independent.

## Setup (one-time)

The recipes live in this repo under `goose/recipes/`. Wire them into goose:

```sh
ln -s "$(pwd)/goose/recipes/orchestrate.yaml"  ~/.config/goose/recipes/orchestrate.yaml
ln -s "$(pwd)/goose/recipes/subrecipes"        ~/.config/goose/recipes/subrecipes
```

(or copy them; subrecipe paths are relative to the main recipe). Make sure
the current router-acp is installed — the directives feature needs it:

```sh
cargo install --path . --force
```

## Running it


```sh
exec env goose run -s --recipe "$HOME/.config/goose/recipes/orchestrate.yaml" "$@"
```

> tip: set this as an alias, such as `alias go='exec env goose run -s --recipe "$HOME/.config/goose/recipes/orchestrate.yaml" "$@"'`

- The orchestrator (already pinned to the planner model — the recipe's own
  first prompt carries the routing directive) introduces itself and asks
  for the task.
- You type: `Fix the bugs with issue X: (1)… (2)… (3)…` — your message is
  treated verbatim as the ORIGINAL TASK for planning and review.
- The pipeline runs (plan → parallel subtasks → cross-lineage review →
  gated submission), then `-s` keeps the **normal goose CLI open**: ask
  questions, request changes ("also rename that flag"), and the
  orchestrator handles follow-ups with the same tools — small changes go
  to a targeted subtask, anything that will be (re)submitted goes through
  the reviewer again first.

**Scripted (CI, one-shots).** Pass the task as a param; add `-s` only if
you want the session to stay open afterwards:

```sh
GOOSE_PROVIDER=pi-acp goose run \
  --recipe ~/.config/goose/recipes/orchestrate.yaml \
  --params task='Fix the bugs with issue X: (1) ... (2) ... (3) ...'
```

Optional params:

| param | default | meaning |
| --- | --- | --- |
| `task` | *(asked interactively)* | the compound task; omit it for interactive intake |
| `planner_candidate` | `claude/claude-fable-5[1m]` | who plans/orchestrates |
| `planner_lineage` | `claude` | agent the reviewer must NOT be |
| `reviewer_candidate` | `codex/gpt-5.5` | preferred reviewer (soft; falls back if unavailable) |
| `run_label` | `orchestrate` | grouping label for every session of the run |
| `max_fix_rounds` | `2` | review→fix→re-review budget |
| `submit` | `branch` | `never` \| `branch` \| `pr` \| `merge` (merge only after the reviewer approves; "ship/merge/land" in the task elevates to `merge`) |
| `repo_path` | `.` | target checkout |

You'll see the routing decisions inline — one `[router-acp] … → model ·
task … · utility …` line per session (orchestrator, each subtask, the
reviewer), and each is recorded with weights in
`~/.local/state/router-acp/sessions.json`.

## Why this raises quality while lowering usage

**Quality** comes from three structural properties, not model vibes:

1. **Fresh-context executors.** Each subtask session sees only a
   purpose-written prompt — no 100k-token contaminated context, no
   cross-bug confusion. Small contexts measurably reduce derailment.
2. **Independent cross-lineage review.** The reviewer re-derives the
   success criteria from the *original* prompt before reading the plan, on
   a model family with different failure modes, and verifies empirically
   (diff + running tests). It catches both bad decomposition (planner
   blind spots) and bad integration (three individually-fine fixes that
   fight each other) — the two errors a single-session run can't see from
   inside.
3. **Gated submission.** Nothing is pushed without an approving verdict;
   fix rounds are bounded so it can't loop forever.

**Usage** drops relative to "run the whole thing on fable" because the
frontier model spends turns only on plan/adjudicate/integrate (typically a
small fraction of total turns); the execution legs — the bulk of tool
calls — run on models the router sizes to each leg, in parallel;
`budget_prompts_5h` headroom steering and cordons spread load across both
plans automatically. The two frontier bookends are the *cheap* part of the
budget and prevent the actually-expensive failure mode: redone work.

## Tuning & troubleshooting

- **Subtasks routing too cheap/expensive?** The contextual prompt is the
  routing input — the disclosure line shows the classified class/complexity
  per subtask. Tell the orchestrator (via the plan instructions) to include
  scale words honestly, or tune `cost_quality_tradeoff` in `router.yaml`.
  A subtask prompt may also carry its own `[router: ...]` directive if the
  planner wants to force a model — allowed, disclosed, and recorded.
- **Reviewer landed somewhere odd?** Its session's disclosure shows the
  pool after `exclude` — remember cordons also shrink it. `exclude` takes
  agent names or candidate globs (`exclude=claude|codex/gpt-5.4-mini`
  bans claude entirely plus codex's mini).
- **Want a hard provider pin instead of the router for review?** Set
  `settings.goose_provider: codex-acp` in `review.subrecipe.yaml` — goose
  will use the codex adapter directly (you lose router failover/cordons
  and the disclosure for that phase).
- **Subrecipes are experimental** in goose (their words): behavior may
  shift between versions. This design keeps each piece independently
  useful — the directives work from any client and any script
  (`GOOSE_PROVIDER=pi-acp goose run -t "[router: candidate=…]\n…"`), so a
  plain shell script chaining `goose run` calls is a valid fallback
  orchestrator if the summon layer changes.

## Limits to know about

- Phases share **no conversation state** — by design. Anything the reviewer
  or executors need must be written into their prompts or into files
  (the plan file is the contract).
- The orchestrator must keep subtasks **file-disjoint**; parallel sessions
  editing the same file will race. The recipe instructs it to merge
  overlapping subtasks.
- Parallelism caps at goose's 10 in-process workers.
- `git` operations belong to the orchestrator only (executors are told not
  to touch history), and force-push is forbidden in the recipe.
