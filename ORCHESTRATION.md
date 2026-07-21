# Orchestration: plan → parallel subtasks → cross-lineage review → submit

router-acp orchestrates compound tasks **itself**, in-process. When a prompt
reads as a multi-part task list, the router pins a frontier **planner** model and
injects an orchestration protocol; the planner decomposes the work, fans it out
to router-routed sub-sessions via the `delegate_task` tool, has a
**different-lineage** model review the result, adjudicates fixes, and submits per
a gate. No goose recipe, no `summon` extension, no wrapper script — it works from
any ACP client (goose, Zed, a plain script) and even a plain chat turn.

> This replaces the old goose `orchestrate.yaml` recipe (removed). The recipe
> needed goose subrecipes for isolated sessions and structured verdicts; the
> router now provides the isolated sessions itself (`delegate_task`), so the
> whole pipeline is a router feature you turn on with one config block.

```
you: "Fix the bugs in issue X: (1)… (2)… (3)…"
        │
        ▼
┌──────────────── router-acp session (pinned to the planner) ────────────────┐
│ ORCHESTRATOR  (planner, e.g. claude/claude-fable-5[1m])                     │
│ injected protocol → investigate, write success criteria + subtasks         │
│        │  delegate_task ×N  (peer delegation on; independent ⇒ PARALLEL)    │
│        ├──────────────┬──────────────┐                                      │
│        ▼              ▼              ▼    (each an isolated downstream       │
│   subtask 1       subtask 2      subtask 3  session the router routes by    │
│   → haiku         → sonnet       → gpt-5.5  ITS OWN prompt's complexity)     │
│        └──────────────┴──────────────┘                                      │
│        ▼                                                                    │
│ REVIEW  delegate_task(hints.candidate = a DIFFERENT lineage)                │
│ re-derives criteria from the ORIGINAL task, diffs, runs tests → verdict     │
│        ▼                                                                    │
│ fix rounds (bounded, delegate_followup) → on approve: branch / PR / merge   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## How it triggers

`src/tasklist.rs` classifies the incoming prompt as a multi-part list. It
recognizes markdown numbers (`1. …`), markdown bullets (`- …`), inline
enumeration (`… (1) … (2) …`), and ordered prose ("first … then … finally …").
When it finds at least `orchestration.min_items` parts, the router puts the
session into orchestrator mode.

It fires on **any** prompt — a fresh session (steered onto the planner before the
pin) or mid-session (switched onto the planner via the summarize-and-re-pin
handoff). It is **not** triggered when:

- `orchestration.enabled` is `false` (the default);
- the prompt carries an explicit `[router: …]` directive or a `model:` shorthand
  (you're steering manually);
- the list is **you answering the model's own questions** — if the previous agent
  turn posed questions/decisions (e.g. it said "Open decisions: (1)… (2)…"), your
  enumerated reply is treated as answers and relayed normally.

Auto-orchestration **outranks `skill_routing`**: a multi-part task list
orchestrates even if it names a skill (e.g. `ship-pr`). `skill_routing` only
takes over for a skill invocation that is *not* a multi-part task. A skill the
task wants at the end (shipping, opening/merging a PR) is the planner's to run —
the protocol's step 5 tells it to do that only after the work is done and
reviewed, never up front.

Two ways to feed it more than the literal prompt:

- **Force it** — start the message with `orchestrate:` / `orchestrator:`. This
  overrides every auto-detection gate (list detection, `min_items`, the
  answering-questions exception, even `orchestration.enabled`) and runs the
  pipeline on whatever follows.
- **Ticket context** — with `ticket_context` rules configured (pluggable:
  `prefix` + a command for your ticketing system's CLI), "Fix HAI-1234" loads
  the ticket body into the prompt *before* detection. A ticket whose
  description is a work list orchestrates exactly as if you had pasted it, and
  the planner delegates its parts.

Every trigger is disclosed: `router-acp · orchestrating a N-part task on <model>`.

## The pipeline (what the injected protocol tells the planner)

1. **Plan.** Investigate read-only, restate the task as concrete success
   criteria, and split it into self-contained, **file-disjoint** subtasks.
2. **Delegate.** Dispatch each subtask with `delegate_task`, giving each a fully
   self-contained prompt (paths, current vs. desired behavior, constraints,
   acceptance checks) — the router routes each subtask by reading *its* prompt,
   so the planner is told to describe difficulty honestly. Independent subtasks
   go concurrently.
3. **Review (different lineage).** Delegate a review via `delegate_task`, passing
   `hints.candidate` set to a concrete candidate of a **different lineage** than
   the planner (the router resolves and injects those ids — see below). The
   reviewer gets the ORIGINAL task verbatim and re-derives the criteria itself.
4. **Adjudicate.** Fix blocking issues (a targeted `delegate_task`, or
   `delegate_followup` to iterate on a kept-open sub-agent) and re-review, up to
   `max_fix_rounds`.
5. **Submit.** Per `orchestration.submit` — `never | branch | pr | merge`. A
   merge is only permitted **after** an approving review. Any end-of-work skill
   the task named (shipping, opening/merging a PR, deploying) runs here — last,
   never up front.

## What the router provides vs. what is instruction

The pipeline is mostly the planner model following the injected protocol with its
own tools. The router contributes exactly the mechanisms that make it work and
observable:

| capability | router mechanism |
| --- | --- |
| Isolated routed sub-sessions | the `delegate_task` MCP tool — each call opens an ephemeral downstream session the router routes on its own prompt; permission/fs callbacks relay to the client under the parent id |
| Cross-lineage review is *routeable* | in an orchestrating session the delegate pool is **not** cheaper-only — it may reach same-/higher-tier peers, so a frontier reviewer of another lineage can be delegated to |
| Concrete reviewer ids | `resolve_reviewers` picks eligible candidates whose lineage ≠ the planner's (from `orchestration.reviewer` globs, else any other lineage) and injects them into the protocol |
| Iterating on a subtask | `delegate_task keep_open: true` returns a `delegate_id`; `delegate_followup` sends more turns to that same sub-agent (context preserved); `delegate_close` frees it |
| Observability | the planner and every delegate get state-DB rows sharing `run_label = "orchestrate"`; delegate rows link to the parent via `parent_session_id`; each routing decision is disclosed and recorded |
| Planner selection | steered pre-pin (`candidate_override`) or switched mid-session (`switch_pin`) to the best eligible `orchestration.planner` glob |

## Configuration

```yaml
orchestration:
  enabled: true
  min_items: 2                                        # smallest list treated as multi-part
  planner: ["*fable*", "*opus*", "*sol*", "*gpt-5.5*"] # best first; first eligible wins
  reviewer: ["*sol*", "*gpt-5.5*", "*opus*"]          # preferred; a DIFFERENT lineage is enforced
  submit: branch                                      # never | branch | pr | merge
  max_fix_rounds: 2
```

- **`planner`** / **`reviewer`** are candidate globs in preference order, matched
  against the routeable pool. The reviewer is always forced onto a lineage other
  than the planner's when one is available; if only the planner's lineage is
  routeable, the review runs on the most capable other model it can (disclosed).
- **`submit: merge`** still gates the merge on an approving review — see the
  protocol's step 5. Set `never` to keep everything local.

## Why each model ends up where it does

- **Planner** — the first eligible `planner` glob (a frontier model). Steered/
  switched deterministically, disclosed.
- **Subtasks** — routed per complexity. Each delegated subtask is a fresh session
  the router classifies on its own prompt: "rename a flag in cli.rs" reads
  trivial → haiku/mini; "rework the retry state machine" reads hard → frontier.
  This is why the protocol insists on honest subtask prompts.
- **Reviewer** — a resolved, eligible candidate of a **different lineage** than
  the planner. Lineage means **company**, not agent name: it's the
  `agents[].lineage` tag (default: the agent name), compared in code by
  `resolve_reviewers` — the same `reviewer` glob list therefore yields the
  *opposite* company of whoever planned (fable plans → sol reviews; sol plans →
  fable reviews). Two agents backed by the same vendor (e.g. two Claude seats)
  should declare the same `lineage` so a sibling seat is never mistaken for an
  independent reviewer. Why company: a single-session run can't catch bad
  decomposition (planner blind spots) or bad integration (three
  individually-fine fixes that fight each other); only a model family with
  genuinely **different failure modes** can.

## Why this raises quality while lowering usage

**Quality** comes from structure, not model vibes:

1. **Fresh-context executors.** Each subtask session sees only a purpose-written
   prompt — no contaminated 100k-token context, no cross-bug confusion.
2. **Independent cross-lineage review.** The reviewer re-derives criteria from the
   original task before reading the work, on a different family, and verifies
   empirically (diff + tests).
3. **Gated submission.** Nothing merges without an approving verdict; fix rounds
   are bounded so it can't loop forever.

**Usage** drops relative to "run the whole thing on a frontier model": the
planner spends turns only on plan/adjudicate/integrate (a small fraction of the
total); the execution legs — the bulk of the tool calls — run on models the
router sizes to each leg, in parallel; `budget_prompts_5h` headroom steering and
cordons spread load automatically. The frontier bookends are the cheap part of
the budget and prevent the expensive failure mode: redone work.

> These are the *design intent*. Whether a given deployment realizes them is an
> empirical question — measure it (below), don't assume it.

## Evaluating a run (observability)

```sh
router-acp report --config ~/.config/router-acp/router.yaml
```

Per run it shows: planner vs. delegate **cost** (`cost_usd`, the adapter's own
`usage_update.cost` in USD — authoritative, not a token estimate), **compute
time** (`compute_ms`, the model's turn time excluding user idle), delegate count
and models, whether a **cross-lineage review** ran, and the **degraded%** (runs
where the planner used its built-in `Task` tool instead of `delegate_task`). Each
run is tagged with `git_branch`/`git_sha` at pin.

What each metric can and cannot tell you:

- **Did delegation happen?** Certain — delegate rows (`parent_session_id`) and
  the degraded count. If `delegates: 0` and `native-subagent > 0`, the planner
  bypassed the router and none of the benefits below apply.
- **Cost / speed impact?** `cost_usd` (planner vs. delegate split) and
  `compute_ms` are now real, but proving orchestration is *cheaper/faster* needs
  a baseline — run the same task single-model and compare, or watch the split
  trend across many runs. The token *counters* (`tokens_*`) are text-estimates
  and under-count badly; ignore them for cost.
- **Accuracy impact?** Not answerable from the router alone. Join `git_sha` to
  the outcome that matters — did the PR pass CI, merge without a revert, or need
  follow-up fixes? That downstream signal is the only real accuracy measure; the
  router just gives you the join key.

## Caveats & limits

- **The planner must actually use `delegate_task`.** Sub-session routing, the
  cross-lineage review, and the `parent_session_id`/`run_label` rows all depend
  on it. Some adapters ship a *built-in* sub-agent tool (Claude's `Task`) that
  spawns **same-lineage** sub-agents *inside* the adapter, invisible to the
  router. The injected protocol explicitly forbids that tool and mandates
  `delegate_task` with a concrete cross-lineage reviewer id — but the router
  cannot remove the adapter's own tool. **If a run produces no `kind='delegate'`
  rows in the state DB, the planner used its native tool** and you got
  same-lineage, unobservable orchestration; tighten the prompt or file an issue.
- **It's prose, not a state machine.** The protocol is guidance the model
  follows; a model that ignores it degrades gracefully to a normal (non-
  orchestrated) turn rather than failing.
- **Subtasks must be file-disjoint.** Parallel sub-sessions editing the same file
  will race; the protocol instructs the planner to merge overlapping subtasks.
- **Delegation depth is 1.** Delegated sub-sessions do not themselves get the
  delegate tool, so the tree is one level deep by design.

## See also

- [`ROUTERS.md`](ROUTERS.md) § *Auto-orchestration of task lists* — the
  user-facing summary.
- [`README.md`](README.md) — the `orchestration.*` config reference and the
  in-session delegation details.
