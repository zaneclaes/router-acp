# Routers

router-acp picks one **candidate** — an `(agent, model)` pair like
`claude/sonnet` — for each conversation, at the moment the first prompt
arrives. The picker is called a *router* (or *strategy*). There are four.
This document explains how each one thinks, in plain terms, and which knobs
matter.

Pick the default router with the top-level `router:` key; override it per
session with the `router.strategy` session option (goose Desktop and other
clients that show config selectors) — or, from any client, with a
**prompt directive** on any line of the first prompt:
`[router: strategy=pareto-code]`, `[router: candidate=claude/sonnet]`,
`[router: prefer=codex/gpt-5.5]`, `[router: exclude=claude]` (stripped before
the model sees it). Either way, changes land **before the first prompt**.

After the first prompt the session is pinned to its candidate. Three things
can still change the model:

- **failover** — automatic, when the pinned model goes down or hits its token
  limit before a turn produces output;
- **`[router: switch=agent/model]`** — an explicit request, at any point, to
  hand the conversation to a different model (see *Switching models
  mid-session* below);
- **auto-upgrade** — the router itself switches up to a more capable model
  when a session's confidence drops (tunable; off with `auto_upgrade`).

Whichever router runs, the decision is printed to your console and recorded
in the state file, including the math, so you never have to guess why a
model was chosen.

With the optional **per-request LLM proxy**, the candidate remains the
session's default and failover owner, but it is no longer necessarily the model
for every provider call inside a turn. The loopback proxy observes the live
tool-result trace and may:

- demote to the cheapest same-agent model after `llm_proxy.routine_streak`
  routine requests;
- escalate immediately to the highest-quality compatible same-agent model at or
  below the session pin's cost rank on failures, unchanged test output, refusal,
  or token/context ceilings; per-request routing never spends above the pin;
- return from an escalation when its request/time verdict expires; and
- hold a model for `minimum_dwell_requests` to avoid repeatedly paying cold
  cache costs.

Context-window guards, cordons, and quarantines constrain this pool. Request
signals are extracted structurally and deterministically from the live tool
payload; per-request routing does not call an LLM to classify requests. An
automation hint (`_meta.router_acp.request_hint = "ci-poll"`, `"ship-nudge"`,
or `"automation"`) goes directly to the cheap compatible model. Every
attributed decision is disclosed as `router-acp · request …`, stored in
`session_log`, and recorded with exact model/token/cache/cost fields in
`llm_requests`. The proxy is orthogonal to the four ACP routers below: they
still choose the pinned default.

---

## What every router sees

Before ranking, the router gets a filtered **pool** of candidates. A
candidate is in the pool only if it is:

- **verified** — its process is running and its model id passed startup
  validation against the adapter's own model list;
- **capable** — if your prompt contains an image, only image-capable agents
  survive (same for audio and embedded resources);
- **not cordoned** — an agent that hit its token/usage limit sits out until
  the reset time it reported;
- **not quarantined** — a candidate that repeatedly failed to open sessions
  cools off for a while (`headroom.quarantine_*`).

And for `auto`, each candidate carries three numbers:

- **quality** — a benchmark-calibrated 0.5–3.5 score *for the kind of task you
  asked* (roughly 1 minimal, 2 standard, 3 frontier), from a data
  table ([`data/scores.yaml`](data/scores.yaml), overridable with
  `score_table:`). Patterns are first-match-wins, so specific patterns
  (`*mini*`) must come before broad ones (`*gpt-5*`).
- **cost rank** — your `models[].cost_rank` (1 = cheapest/least scarce).
  With flat-rate seats this models *scarcity*, not dollars.
- **headroom** — the lower of the local sliding-window estimate and the
  candidate's seat budget: while included plan remains, its reported free
  fraction; once a seat is paying overage, its remaining budget is compared in
  real **dollars** (not the fraction of that provider's own cap — a $9k pool
  at 3% free and a $3k pool at 3% free are the same percentage but not the
  same seat), saturated at `availability_preference.headroom_scale_dollars`
  (default $200). Overage pools report real dollars straight from the
  provider API; included-plan windows have no dollar field on either
  provider, so the router estimates them from its own metered spend vs the
  window's percent. Model-scoped caps, such as Claude Fable's weekly window,
  apply only to that model. Agents with no usage meter (Grok, Kimi) have no
  reported plan headroom; while **any metered seat still has free included
  plan**, their effective headroom is capped at that best free metered
  residual so a fake 100% does not beat free Claude/Codex on the quota term.
  When every metered free plan is exhausted, unmetered keeps full local
  headroom (valid failover).

The kind of task comes from a **classifier** that reads your first prompt:
it assigns a task class (BugFix, Research, Architecture, UiTweak, …) and a
**complexity** score from 0 (trivial) to 1 (very hard), using keyword tables,
multi-step structure ("do X and Y, then Z"), mentioned files, and a scan of
your project's languages. Rules live in
[`data/classifier.yaml`](data/classifier.yaml) (`classifier.rules_file` to
override); an optional local-model backend (`classifier.backend:
local-model`, e.g. Ollama) can replace the heuristics — it never uses your
paid seats.

---

## `auto` — the general-purpose router (default)

**In one sentence:** score every candidate by *quality for this task* versus
*how cheap/plentiful it is*, and let difficulty tilt the balance toward
quality.

For each candidate:

```
quality_demand = min(task-class base + 2 × complexity, 3)
quality_value = (min(quality(task class), quality_demand) − 0.5) / 3.0
effective_headroom = min(local headroom, reported plan headroom)
# unmetered (no reported plan): if any metered seat has free plan > 0,
#   effective_headroom = min(local, max free metered plan)
# else keep local (failover when metered free plan is gone)
utility = quality_weight × quality_value
        + cost_weight × effective_headroom × (1 − 0.5 × normalized cost rank)
        + preference
```

Included-plan usage is free at the margin, so rank is only a bounded scarcity
pressure against wasting large-token/frontier models. Reported plan headroom is
the dominant cost signal; paid overage receives a separate penalty. Unmetered
frontiers must not win low-complexity work solely because they lack a meter.
At `cost_quality_tradeoff: 0`, the demand cap is bypassed and raw benchmark
quality wins.

The weights come from one dial, `cost_quality_tradeoff` (0–10):

- `0` = pure quality (always the best model),
- `10` = the cheapest candidate that survived the filters,
- values between blend the two.

Two behaviors make `auto` feel smart:

1. **Task difficulty caps useful capability:** editing/ops classes start at
   demand 1, implementation at 1.2, and open-ended reasoning at 1.5.
   Complexity adds up to two points, capped at frontier demand 3. A stronger
   model keeps its measured quality, but quality above the task's demand has no
   extra utility for that decision.
2. **Complexity scales the dial** (`complexity_scales_tradeoff`, on by
   default): the effective tradeoff is `tradeoff × (1 − complexity)`. A
   "hello world" keeps your configured cost-consciousness; an hour-long
   investigation drives the tradeoff toward 0 so frontier models win.
3. **The complexity gate** (`complexity_floor`, default 0.7): when a prompt
   classifies above the floor, candidates below the 75th-percentile quality
   for that task class are dropped *before* scoring — cheap models can't
   even compete for genuinely hard work.
4. **The apex carve-out** (`apex_complexity`, default 0.9): at/above this
   complexity, ranking goes pure quality (tradeoff forced to 0, demand cap
   bypassed) — the same regime as `cost_quality_tradeoff: 0`, but automatic
   at genuine extremes rather than requiring a global config change. This
   matters most for a **compressed** score-table pair (see
   `data/model-policy.yaml`'s `benchmark_scoring.compression`): two peers
   whose priced tiers differ but whose raw benchmark evidence is within
   noise get a deliberately tiny (`max_gap`, default 0.02) quality gap so
   `cost_rank` decides everyday ties — but that gap is too small to survive
   even a modest cost term, so without the apex carve-out the preferred
   member would only be reachable via explicit pins or planner globs, never
   by `auto` itself. At the apex, the full (still small but now undiluted)
   gap decides.

Ties break deterministically: higher utility, then lower effective cost,
then config order.

**Config that matters:**

```yaml
router: auto
routers:
  auto:
    cost_quality_tradeoff: 3      # 7 is the OpenRouter-parity default;
                                  # 3 suits flat-rate seats (quality-leaning)
    complexity_floor: 0.7         # quality gate threshold
    complexity_scales_tradeoff: true
    apex_complexity: 0.9          # pure-quality carve-out for extreme work
    allowed_candidates: ["*"]     # glob allowlist, e.g. ["claude/*"]

agents:
  - name: claude
    preference: 0.05              # small additive bonus: prefer this agent
                                  # when candidates are otherwise comparable
```

**When a choice looks wrong**, read the disclosure line — it shows every
input:

```
[router-acp] auto → claude/sonnet · task BugFix (complexity 0.35) · utility 0.48 = 0.70×quality 1.38→0.29 (BugFix) + 0.30×quota (headroom 100%, cost rank 2) + pref 0.05 · tradeoff 3→2.0 (complexity-scaled)
```

- routed too cheap on a hard task → `complexity` was scored too low (tune
  the classifier rules or lower `cost_quality_tradeoff`);
- wrong family won → adjust `preference` or `cost_rank`s;
- a model seems generally mis-rated → fix its entry in the score table.

## `pareto-code` — coding tiers, cheapest first

**In one sentence:** decide how good a *coding* model you need (a tier),
then take the cheapest available model inside that tier.

It is router-acp's own tiering scheme — loosely motivated by the
price/quality-frontier idea behind OpenRouter's public model rankings, but
**not** a documented OpenRouter algorithm — adapting the notion from API price
to seat-quota pressure:

1. `min_coding_score` maps to a tier: omitted or ≥ 0.66 → **high**,
   ≥ 0.33 → **medium**, else **low**. Each candidate's tier comes from the
   score table (`coding_tier`).
2. Filter to that tier. If it's empty, step to the neighboring tier — and
   say so in the disclosure.
3. Within the tier, pick the lowest `effective_cost = cost_rank /
   max(headroom, ε)` — so a nearly-exhausted seat looks expensive and the
   router shifts load off it. The next two same-tier candidates are kept as
   fallbacks in case the first fails to open. Ties break by `preference`,
   then config order.

Notice what it ignores: task class, complexity, per-class quality scores.
It's a blunter, very predictable instrument — "give me a high-tier coder,
whichever is most available" — best for uniformly-hard coding sessions.
`auto` remains the better general router; don't use `pareto-code` for
research/writing sessions.

**Config that matters:**

```yaml
router: pareto-code
routers:
  pareto-code:
    min_coding_score: 0.66   # high tier; 0.4 would mean "medium is fine"
```

## `escalation` — start cheap, escalate when the work proves hard

**In one sentence:** begin on the cheapest capable model and hand off to a
stronger one only when *observed execution* reveals the task was harder than it
looked.

`auto` and `pareto-code` decide up front, from the prompt. But some tasks read
as one trivial sentence and only reveal their depth once a model starts digging
through the code. `escalation` doesn't try to predict that — it **watches** and
reacts:

1. **Start cheap — or delegate the start.** By default the first prompt pins the
   cheapest routeable candidate (scarcity-adjusted, like `pareto-code`),
   optionally floored by `min_start_score`. Or set `initial_router: auto` (or
   `pareto-code`/`static`) to delegate the *starting* pick to that router — so a
   session begins on a *sensible* model and escalation only kicks in from there.
2. **Escalate on observed difficulty**, never on a guess. Three mid-turn
   triggers, each firing as soon as its threshold is crossed *during* the turn:
   - **Read volume** — investigation reads (file reads *and* read-only shell like
     `git status`/`grep`/`find`, and read-only MCP tools) crossing
     `escalate_after_reads` *before any side effect*. This one fires while the
     turn is still side-effect-free, so it's a clean pre-work handoff.
   - **Tool-call volume** — `escalate_after_tool_calls` total tool calls in one
     turn without finishing: the robust "grinding / in over its head" signal.
     Unlike read volume it doesn't care about side-effect ordering, so it catches
     the common edit-and-Bash-heavy tasks the read trigger misses.
   - **Tool-failure churn** — `escalate_after_tool_failures` failed tool calls:
     the model is thrashing.
   Plus a **post-turn** trigger on a token-ceiling (`escalate_on_max_tokens`) or
   refusal (`escalate_on_refusal`) stop. The volume and failure triggers fire
   *after* side effects, so instead of replaying they hand off a **transcript**
   and the stronger model *continues* from where the cheap one left off (no
   double-application). `escalate_before_side_effects: false` disables the
   pre-side-effect read trigger specifically.
3. **How far it jumps** is `escalation_path`: `ladder` steps to the
   next-more-capable model (re-evaluating at each step); `leap` goes straight to
   the strongest. Escalations are one-way and capped by `max_escalations`.

The handoff reuses the same summarize-and-re-pin machinery as `[router:
switch=…]`, including the **log-transcript fallback** — so even a mid-turn
escalation, where the cheap model was interrupted and can't summarize, carries
the prior context forward from the state DB.

The pay-off: genuinely trivial tasks finish on the cheap model at zero extra
cost (nothing to escalate), while the "looks easy, turns out hard" tasks get
frontier power the moment they earn it — without you having to predict which is
which.

**Config that matters:**

```yaml
router: escalation
routers:
  escalation:
    escalation_path: ladder            # ladder | leap
    # initial_router: auto             # delegate the starting pick (auto|pareto-code|static)
    escalate_before_side_effects: true # enables the pre-side-effect read trigger
    min_start_score: 0.0               # optional floor on the starting model
    escalate_after_reads: 6            # investigation reads before a side effect (0 = off)
    escalate_after_tool_calls: 30      # total tool calls in one turn without finishing (0 = off)
    escalate_after_tool_failures: 3    # failed tool calls → escalate mid-turn (0 = off)
    escalate_on_max_tokens: true
    escalate_on_refusal: true
    max_escalations: 3
```

## `static` — no routing at all

**In one sentence:** always use the candidate you named.

The session's explicit `router.candidate` selection wins; otherwise
`routers.static.candidate` from config. If that candidate isn't routeable
(unverified, cordoned, missing capability) you get an **actionable error**
rather than a silent substitute — unless you opt into substitution with
`allow_fallback: true`, which appends the remaining candidates in config
order.

**Config that matters:**

```yaml
router: static
routers:
  static:
    candidate: claude/sonnet
    allow_fallback: false
```

Tip: you rarely need `router: static` globally. Setting the
`router.candidate` session option to a concrete candidate makes *that one
session* static while everything else keeps routing.

---

## Switching models mid-session

A pinned session can move to a different model without losing its thread. ACP
does not transfer a live transcript between agents, so the router does the next
best thing: it asks the **current** model to write a handoff summary (the task,
decisions, files changed, what's left), opens a **fresh** downstream session on
the target, seeds that session by prepending the summary to your next prompt,
re-pins, and closes the old session. The summary turn is captured internally —
you never see it — and the switch is disclosed like any other routing decision.

**Fallback when the old model can't summarize.** If the outgoing model is
offline, rate-limited, crashed, or refuses (so it can't produce a summary), the
router doesn't give up the switch — it reconstructs a **truncated transcript of
the prior conversation from its own SQLite logs** (`session_log`, each turn
capped at ~500 chars) and seeds *that* into the new model instead, clearly
labelled as a recovered transcript rather than a written summary. The
disclosure says which path was used. This needs nothing from the dead model, so
a switch (including an auto-upgrade triggered *because* the model is failing)
still goes through.

Three ways it happens:

1. **You ask.** Put `[router: switch=agent/model]` on any line of a prompt in a
   pinned session. The rest of that prompt continues the work on the new model.
   (Before the pin, `switch=` just behaves like `candidate=`.)

   ```
   [router: switch=claude/opus[1m]]
   This is getting hairy — take over and finish the refactor.
   ```

   Or the **`model:` shorthand** — begin a message with a model reference and a
   colon. The reference can be a full id, a bare model id, a family, or a
   suffix-less id, and resolves to the best eligible match:

   ```
   opus: take over and finish the refactor
   gpt-5.5: review this
   sonnet:                      # bare — switch and let the new model greet you
   ```

   A leading `word:` that names no candidate (e.g. `Note:`) is left as ordinary
   prose. Pre-pin, the shorthand steers the initial pin instead of switching.

2. **Auto-upgrade.** After each turn the router estimates the session's
   **confidence** — the fraction of the classified task's capability demand
   met by the pinned model's benchmark quality, minus an accumulated
   **struggle** score (raised by hitting the token ceiling, refusing, or
   repeated tool failures within a turn). A model meeting demand starts at
   full confidence regardless of its absolute tier. When confidence falls
   below a threshold, the router queues an upgrade to the best
   strictly-more-capable eligible candidate and performs it on the next
   prompt. Tunable:

   ```yaml
   auto_upgrade:
     enabled: true               # false disables auto-upgrade entirely
     confidence_threshold: 0.55  # higher = upgrades more eagerly; 0 ≈ never
   ```

   Explicit `switch=` always works even with `auto_upgrade.enabled: false`.

3. **A skill demands a model class.** Some skills should always run on capable
   models. `skill_routing` maps a skill pattern to a preferred set of candidate
   globs; when a prompt invokes that skill (as `/name` or a standalone token)
   and the pinned model is not already acceptable, the session switches to the
   best available match. Before the pin it steers the initial routing instead.

   ```yaml
   skill_routing:
     - pattern: ship-pr            # matches "/ship-pr" or the token "ship-pr"
       candidates: ["*opus*", "*gpt-5.5*"]   # switch TO these
       also_acceptable: ["*fable*", "*sol*"] # already here? leave the pin alone
   ```

   Candidates are candidate globs (a *class*); if none are routeable (cordoned,
   down, excluded) the session keeps its current model and says so, rather than
   blocking.

   **`candidates` and `also_acceptable` are different sets on purpose.** The
   pin is left alone if it matches *either*, but a switch may only target
   `candidates`. Without the split, one list has to answer two questions — "is
   the current pin good enough?" and "what do we switch to?" — and the only way
   to stop force-switching an already-better pin is to add it to the list,
   which then makes it the switch target for every genuine switch. Put models
   that are fine to *stay* on but that you don't want to route *to* — typically
   the expensive top of the range — in `also_acceptable`.

   **`selection` decides how a target is picked from `candidates`.**

   ```yaml
   skill_routing:
     - pattern: ship-pr
       selection: first-match      # default: best-quality
       candidates: ["*grok*", "*opus*", "*gpt-5.5*"]
       also_acceptable: ["*fable*", "*sol*"]
       terse_handoff: true
   ```

   - `best-quality` (default) — highest `quality + preference` wins and list
     order is only a tie-break. Right when the globs name interchangeable tiers
     and you just want the best one that is up.
   - `first-match` — the FIRST glob with an eligible candidate wins; quality
     only breaks ties *within* that glob. Use it when list order encodes a
     preference the score table does not: routing a ship flow to a flat-rate
     seat, or to a different lineage for cross-vendor review, when a
     quality-max pick would never select it. Fallthrough still works — both
     modes draw from the same cordon/eligibility-filtered pool, so a
     `first-match` route lands on the next glob when its preferred seat is
     down.

   **`terse_handoff` changes what the outgoing model writes.** A switch cannot
   transfer context over ACP, so the outgoing model is asked to brief its
   successor. By default that is a full summary. With `terse_handoff: true` it
   is instead three lines — the task, the single identifier it operates on
   (`unknown` is an allowed answer, so the model does not guess), and anything
   *not* re-derivable from the repository — and the incoming model is told to
   re-derive concrete state itself and verify identifiers before acting.

   This is **not** a token optimization: the outgoing model reads its whole
   context to write either one, so the cost is nearly identical. It is a
   fidelity one. For a skill that re-derives its own state (a ship flow
   resolving its PR from the current branch), one unambiguous referent beats a
   narrative that may name three PRs and two abandoned approaches. It also
   lands the new session near-empty, which matters when the target's context
   window is smaller than the outgoing model's. When detail is genuinely
   needed, the briefing carries a runnable `router-acp transcript` command for
   the full prior log (see below).

All three degrade gracefully: if the target is unavailable the session stays
put with a visible note. Each switch is recorded in the state file with its
`from`, `to`, and reason.

---

## Auto-orchestration of task lists

This is orthogonal to the router choice — it works on top of `auto`,
`pareto-code`, `escalation`, or `static`. When `orchestration.enabled` is set
and a prompt reads as a **multi-part task list**, the router turns that session
into an orchestrator instead of answering the list in one turn.

What counts as a list (detection is permissive):

- markdown numbers — `1. … 2. … 3. …`
- markdown bullets — `- … / * … / + …`
- inline numbering — `… (1) do this (2) do that …`
- ordered prose — `First, … Then, … Finally, …`

On a match of at least `min_items` parts — and only when the list is a *new*
task, not you answering the model's own questions (if the previous agent turn
posed questions/decisions, e.g. "Open decisions: (1)… (2)…", the router treats
your enumerated reply as answers and relays normally), and with **no** explicit
`[router: …]` directive or `model:` shorthand on the prompt (those suppress it):

1. The session is steered (pre-pin) or **switched** (mid-session, via the same
   summarize-and-re-pin machinery above) onto the best eligible **`planner`**
   candidate.
2. An orchestration protocol is prepended to the prompt telling the planner to
   **plan → delegate the independent parts in parallel (`delegate_task
   background: true` + `delegate_await`, each routed per-complexity) →
   review on a different lineage (`reviewer`) after all parts are collected —
   skipped with a note when no other lineage is available or the planner's
   stated confidence clears `review_confidence` → adjudicate fixes
   (`max_fix_rounds`) → submit (`submit`)**.
3. For that session, delegation is allowed to **same-/higher-tier peers**, not
   just strictly-cheaper ones — this is what makes the cross-lineage reviewer
   routeable. (Ordinary delegation stays cheaper-only.)

Orchestration **takes precedence over `skill_routing`**: a multi-part task list
orchestrates even if it names a skill (e.g. `ship-pr`). Skill routing only fires
for a skill invocation that is *not* a multi-part task. Inside orchestration the
planner decides when to invoke a named skill — end-of-work skills (shipping,
opening/merging a PR) run last, after the work is done and reviewed.

```yaml
orchestration:
  enabled: true
  min_items: 2
  planner: ["*fable*", "*opus*", "*sol*", "*gpt-5.5*"]   # best first
  reviewer: ["*sol*", "*gpt-5.5*", "*opus*"]             # a different lineage than the planner
  submit: branch                # never | branch | pr | merge (merge only after review approves)
  max_fix_rounds: 2
  review_confidence: 0.8        # planner confidence above this skips the review (never under merge)
```

The planner iterates on a subtask by keeping its sub-agent open
(`delegate_task keep_open: true` → `delegate_followup` → `delegate_close`)
rather than re-briefing a fresh session each round. Every trigger is disclosed
(`router-acp · orchestrating a N-part task on …`). The full pipeline, mechanism,
and config are in [`ORCHESTRATION.md`](ORCHESTRATION.md); because it is built on
the router's own `delegate_task` tool it needs no recipe or `summon` extension,
so it works from any ACP client and plain chat.

### Delegate-only host MCPs

With `delegation.mcp_catalogs: true`, an ACP host may register named MCP
bundles for a router session. They never reach the primary downstream agent.
The primary requests a bundle only for a bounded `delegate_task` via
`mcp_catalogs`; unknown names and disabled catalogs fail closed. The router
remains integration-agnostic: the host provides concrete servers and
credentials, while the model can name only a registered bundle.

---

## Things that apply to every router

- **Pin once, switch deliberately.** The first prompt decides; later prompts
  reuse the same downstream session. Pre-pin `router.*` options (candidate,
  strategy, prefer, exclude) are ignored after the pin with a "session already
  pinned" notice. To change models mid-session use `[router: switch=…]`, which
  summarizes the work and re-pins onto a fresh downstream (see below) — ACP
  can't hand a live transcript to a different agent, so the summary is the
  bridge.
- **Failover is the exception.** If the pinned model hits a token limit or
  goes down before a turn produces output, the router announces it, cordons
  or quarantines the culprit, re-runs *the session's router* over the
  remaining pool, and continues on the winner — with a visible note that
  conversation context does not transfer. See `failover.*` config.
- **Cordons beat everything.** A token/usage-limited agent is out of every
  pool until the reset time parsed from its own error message (or
  `headroom.cordon_default_secs`). Even `static` won't route to it.
- **Delegation reuses the session's router** over a pool restricted to
  candidates strictly cheaper than the pinned one (a static session
  delegates with `auto` semantics, since "the configured candidate" is never
  in the cheaper pool). With `delegation.inject_prompt: true`, an ordinary
  downstream session gets one scoped instruction only when that cheaper-worker
  tool was actually attached; model switches re-establish it, while
  orchestration keeps its stronger protocol.
- **Determinism.** Identical inputs and state produce identical decisions —
  ranking has no randomness, and all tie-breaks are stable.
- **Every decision is disclosed** on the console
  (`disclosure: chunk`, default) or in `_meta.router_acp`
  (`disclosure: meta`), and recorded with its weights in the state file
  (`state_file`, self-pruning per `retention.*`) for post-hoc diagnosis.
