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
the model sees it). Several tags in one prompt merge as if they were one
comma-separated tag: a later key overrides an earlier one, `exclude` lists
combine, and every tag is stripped. Either way, changes land **before the
first prompt**.

After the first prompt the session is pinned to its candidate. Three things
can still change the model:

- **failover** — automatic, when the pinned model goes down or hits its token
  limit, or the account/model becomes cordoned, including after partial output;
- **`[router: switch=agent/model]`** — an explicit request, at any point, to
  hand the conversation to a different model (see *Switching models
  mid-session* below);
- **auto-upgrade** — the router itself switches up to a more capable model
  when a session's confidence drops (opt-in via `auto_upgrade.enabled: true`;
  off by default).

Whichever router runs, the decision is printed to your console and recorded
in the state file, including the math, so you never have to guess why a
model was chosen.

With `agents[].accounts`, candidates also identify the login, for example
`claude@personal/sonnet`. Each account has independent authentication, usage
and optional reserves. Type `/login` to add or repair logins, and `/usage`
to see every account. Lower account priorities drain first before the
strategy ranks models. See [Multiple accounts](README.md#multiple-accounts).

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
- **not cordoned** — an account/model that hit its token/usage limit or
  configured reserve ceiling sits out until its reset;
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
  apply only to that model. Grok's `_x.ai/ask_user_question` is translated to
  ACP `elicitation/create` when the client advertises form elicitation.
  Agents with no usage meter (Grok, Kimi) have no
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

## `planner` — two-phase routing (plan → implement)

The planner separates workflow phase from model role. `/plan` selects planning
and `/implement` selects implementation. Either command works after the other.
Coordinators retain planner models while their children implement assignments.

The `planner` strategy splits a session into two phases, each with its own
candidate pool:

| Phase | Pool globs | When |
|---|---|---|
| **Planning** (default) | `routers.planner.planning_candidates` | Session start; designing, speccing, refining |
| **Implementation** | `routers.planner.implementation_candidates` | Plan is ready; building, shipping |

Within each phase, ranking delegates to `auto` (same `routers.auto` /
`cost_aversion` config — no parallel tuning surface). The phase determines
*which* candidates enter the pool; the quality/cost tradeoff picks *which
one*. Everyday planning is Opus (currently 5.5) and Sol. Fable and Astra
stay in the planning pool so `auto`'s `apex_complexity` (pure quality) can
reach them for exceptionally hard plans. Implementation workers remain
Opus, Terra, and Grok.

Prefix a planning prompt with `hard: ` or `easy: ` to override that
automatic split for that message (the prefix is stripped; the model never
sees it):

- `hard: redesign auth` — planning pool is Astra / Fable only
- `easy: list the endpoints` — planning pool is Opus / Sol only
- no prefix — current auto ranking, Astra / Fable only at apex complexity

A prefix on a pinned planning session switches if the current pin is
outside the requested pool. It is ignored in the implementation phase.

### Crossover

At the extremes, models from the other phase's pool are admitted:

- **Implementation** + `complexity ≥ apex_complexity` → planning candidates
  also enter (frontier for genuinely hard execution).
- **Planning** + `complexity ≤ floor_complexity` → implementation candidates
  also enter (workhorse for trivial planning).

### Model boosts

`model_boosts` adds a per-model additive score to a specific phase. A boost
of +2.0 dwarfs the auto utility range (~0–1.1) and reliably wins.

```yaml
model_boosts:
  - pattern: "*grok*"
    implementation: 2.0  # grok always wins implementation phase
    planning: 0.0
```

### Phase transitions

An authentic leading `/plan` or `/implement` takes precedence over classifier
guesses and skill phase flags. Quoted examples, embedded commands, tool output,
and internal role invocations cannot change the phase. Arguments and attachments
are retained as original input receipts. Commands do not grant arbitrary scope,
merge, deployment, or publication authority.

1. **Skill signal** — `marks_implementation_phase: true` on a
   `skill_routing` entry definitively upgrades (e.g. `/ship-pr`).
2. **Pre-classifier** — the `planner_phase` dimension returns `{phase,
   confidence, plan_ready}`; upgrades when `phase=implementation`,
   `confidence ≥ phase_upgrade_confidence`, and `plan_ready=true`.
   `implementation` + `plan_ready=false` stays in Planning: the work looks
   like a build, but no reviewable plan exists yet.
3. **Heuristic** — high-precision keyword phrases ("implement it", "build
   this", "ship it") in the prompt text.
4. **Directive** — `[router: phase=implementation]` and
   `[router: phase=planning]` select their requested routing phase. Repository
   execution still requires an authentic implementation request and admission.

Planning uses a repository policy snapshot and six role skills. No ticket
service is required by the default Markdown policy. A repository can select a
relative policy file with `routers.planner.profile` and an optional roadmap
with `routers.planner.roadmap`.

| Role | Owner and result |
| --- | --- |
| `create-plan` | Parent produces a grounded, reviewable plan. |
| `select-plan` | Parent admits bounded scope with stable plan/work identities. |
| `implement-work` | Child produces an exact revision, checks, and evidence. |
| `review-work` | Parent accepts that revision or returns bounded corrections. |
| `finish-work` | Same child records the accepted revision's handoff receipts. |
| `integrate-plan` | Parent verifies required integration and disposition evidence. |

Each role resolves an explicit `routers.planner.<role>.skill` mapping, then an
exact repository role name, then bundled Markdown. An invalid explicit mapping
fails visibly. Discovery supports `.agents/skills`, `.claude/skills`, and
`.codex/skills`. Canonical aliases deduplicate. Conflicting files fail resolution.
The source paths, hashes, contents, and effective mapping persist with the run.
Configuration changes cannot reinterpret an active run.

An explicit repository `/plan` or `/implement` skill remains an entrypoint.
Its guidance executes once under its owning role. Implementation entrypoint
guidance reaches the admitted child after selection. Internal role metadata
prevents command recursion.

```text
/plan -> create-plan -> reviewable plan
                          |
/implement -> select-plan -> isolated child -> parent review
                                  ^                |
                                  +-- corrections -+
                                                   |
                                     finish-work -> integrate-plan -> refill/complete
```

A direct human request beginning with `implement`, such as `implement ROADMAP.md`,
enters the same selection workflow. Quoted and agent-origin text cannot grant it.

The parent uses `planner_workflow` for durable assignments and revision-bound
receipts. `delegate_task {work_id, task, keep_open:true}` opens an isolated child.
Plain `delegate_task` remains a cheaper, ephemeral helper. Each durable work
keeps its child and workspace across replacement attempts. Stale attempts and
child-authored parent operations are refused. The default allocator creates
separate clones next to the state database and preserves the repository origin.
An optional `workspace` command receives a JSON assignment on stdin and returns
`path`, `lease`, optional `environment`, and optional ACP `mcp_servers`. Host
rebindings cannot replace the router's lifecycle tools.

Pending input, original attachments, accepted revisions, finishing/integration
receipts, and wake acknowledgements persist in SQLite. An idle-parent wake has
bounded retries and cannot infer approval. Disconnecting or cancelling stops
automatic execution. Resume requires the exact parent identity. `/plan` pauses
new dispatch while preserving running assignments. `/implement` explicitly
resumes admitted execution. An empty runnable queue is not completion.

Native Resume restores the durable phase and coordinator role before opening a fresh adapter.
Effective effort remains unavailable until the adapter or provider confirms it.
Close retains the run and its assignments. Delete requires reconciled assigned work, inputs,
wakes, and approval waits, then removes only that run's state and workspace claims.
Repository artifacts and other sessions' claims remain intact.

Clients can negotiate `_meta.router_acp.planner_children: true` in their
initialize capabilities. They receive `router-acp/planner-child-update` and
child-scoped callbacks. A child update's `effort` is usable only when
`effort_confirmed` is true. Missing confirmation means unavailable effort.
Controls use `router-acp/planner-child` with the exact
parent `sessionId`, durable `child_id`, and `action` of `status`, `prompt`, `cancel`, or
`close`. Status returns the current attempt, owning-router activity, confirmed effort,
and durable revision. Restore that state before accepting child input or replaying
callbacks after client reattachment. Child updates include the durable revision,
so older buffered start notifications cannot replace a restored attempt.
A child presentation must never launch another coordinator.

See [consumer migration assets](docs/migrations/planner-skills.md) for Hickory
and exact Chordzy role templates, rollout, parity tests, drain, and rollback.

### Coordinator sessions

A host marks a session that must only plan (an epic's parent that spawns
and supervises implementation sessions) with
`_meta.router_acp.session_role: "coordinator"` on `session/new` or any
`session/prompt`. The role is sticky: a later prompt without it does not
clear it. A coordinator:

- keeps workflow phase separate from its coordinator role. `/implement` can
  select child execution and `/plan` can select refinement. Neither changes
  the parent's planner model role. Classifier/skill guesses cannot remove it.
- only pins, fails over, crosses over, escalates, demotes, or follows a skill
  route onto a `planning_candidates` match. Per-request
  proxy alternates are filtered the same way. Refusals are disclosed
  (`router-acp · coordinator: refused switch to …`).
- fails the turn (`no planning candidate is routeable`) when nothing in the
  planning pool can serve, instead of widening to an implementation model.
- still honors an **explicit human pick**: `router.candidate`,
  `[router: candidate=…]` / `switch=…`, and the `model:` shorthand. The pick
  is recorded (`routing.user_pick`, survives session/load) and later prompts
  leave it alone. A pin outside the pool that no human chose is switched back
  to the planning pool on the next coordinator prompt.

Because `switch=` counts as a human pick, a host must not send its own
`switch=` to keep a coordinator on the pool. Send the role and let the router
enforce it.

### Empty pool

If the filtered pool for the current phase is empty (every candidate
cordoned, excluded, or not declared), the strategy falls back to ranking the
**full** candidate set via `auto` and notes the degradation.

### Config

```yaml
router: planner
routers:
  planner:
    profile: markdown
    # roadmap: ROADMAP.md
    # finish-work: { skill: ship-pr }  # only if this repository has that skill
    planning_candidates: ["*opus*", "*sol*", "*astra*", "*fable*"]
    implementation_candidates: ["*terra*", "*opus*", "*grok*"]
    easy_planning_candidates: ["*opus*", "*sol*"]
    hard_planning_candidates: ["*astra*", "*fable*"]
    model_boosts:
      - pattern: "*grok*"
        implementation: 2.0
        planning: 0.0
    phase_upgrade_confidence: 0.7
    apex_complexity: 0.85
    floor_complexity: 0.15
```

## `static` — no routing at all

**In one sentence:** always use the candidate you named.

The session's explicit `router.candidate` selection wins; otherwise
`routers.static.candidate` from config. If that candidate's provider is signed
out, the router returns ACP `auth_required`; if it is otherwise unavailable
(unverified, cordoned, missing capability), you get an **actionable error**.
Neither silently substitutes unless you opt into substitution with
`allow_fallback: true`, which appends the remaining candidates in config order.

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
     enabled: true               # opt in — off by default
     confidence_threshold: 0.55  # higher = upgrades more eagerly; 0 ≈ never
   ```

   Off by default: a live pin should move on a cordon/outage, a skill route,
   or an explicit request, and a confidence dip is usually ordinary struggle
   (one long tool-heavy turn), not a session in trouble — yet the switch is a
   full summarize + re-pin that forfeits the live context. Explicit `switch=`
   always works regardless of this setting.

3. **A skill demands a model class.** Some skills should always run on capable
   models. `skill_routing` maps a skill pattern to a preferred set of candidate
   globs; when a prompt invokes that skill (as `/name` or a standalone token)
   and the pinned model is not already acceptable, the session switches to the
   best available match. Before the pin it steers the initial routing instead,
   unless `candidate_override_source` is already `UserPick` (an explicit
   `[router: candidate=…]` / spawn `model` wins on prompt 1). A later skill
   turn on a pinned session still switches.

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

   **The elevation a skill route creates is bounded by `demotion`, and stays
   inside the route's own pool.** A skill pin is an *elevation* like an
   escalation or an auto-upgrade, so `demotion.after_quiet_turns` expires it
   after a run of turns with no struggle signals — a ship flow's CI polls are
   the quietest turns there are, so this fires readily. Two rules keep that
   from fighting the route: re-invoking the skill re-arms the clock even when
   the pin already complies (it is a restatement of the verdict, not a no-op),
   and the demotion target is drawn from the route's `candidates` rather than
   from everything cheaper — so an expiring ship verdict can step down within
   the ship target pool but never lands on a model the route excluded or one
   declared only `also_acceptable`. If no candidate target is cheaper than the
   pin, the session simply stays put.

All three degrade gracefully: if the target is unavailable the session stays
put with a visible note. An explicit human pick (`switch=`, `model:`) does
not: a dead target is revived first, and if it still cannot serve, the turn
fails with that reason instead of running on the model the human left. Each switch is recorded in the state file with its
`from`, `to`, and reason.

---

## Host capability MCPs

The host registers concrete bundles per router session, while
`delegation.mcp_catalogs` maps each catalog to opaque capabilities. A
pluggable pre-classifier extension dimension defines those capability terms
and returns `required_capabilities`; the base routing object cannot invent
them from prompt text. The router resolves them before opening the first
downstream session. Later, the primary requests
`delegate_task.required_capabilities` for a bounded subtask. The router stays
integration-agnostic: it does not define capability meanings, endpoints, or
credentials, and incomplete coverage fails closed.

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
  goes down, or becomes cordoned, the router announces it, cordons
  or quarantines the culprit, re-runs *the session's router* over the
  remaining pool, and continues on the winner. A truncated transcript carries
  prior and partial responses plus tool statuses; the replacement is instructed
  to inspect uncertain effects and avoid repeating completed actions. Client
  cancellation never triggers failover. See `failover.*` config.
- **Cordons beat everything.** A token/usage-limited agent is out of every
  pool until the reset time parsed from its own error message (or
  `headroom.cordon_default_secs`). Positive reserves also hard-cordon at
  `100 - reserve_capacity` percent usage, regardless of overage. Even `static`
  won't route to a cordoned candidate; an empty pool returns an unavailable error.
- **Delegation reuses the session's router** over a pool restricted to
  candidates strictly cheaper than the pinned one (a static session
  delegates with `auto` semantics, since "the configured candidate" is never
  in the cheaper pool). With `delegation.inject_prompt: true`, an ordinary
  downstream session gets one scoped instruction only when that cheaper-worker
  tool was actually attached; model switches re-establish it. With
  `delegation.candidate_hints: exact` a parent may instead name any eligible
  candidate (any tier, agent or account), and the call fails rather than
  substituting another model.
- **Reasoning effort.** An explicit request (the `router.effort` option or
  `[router: effort=…]`) is used as given and never capped. Otherwise the
  router uses Medium for routine work. Bounded trivial UI/writing may use Low.
  High needs concrete difficulty evidence. Labels and prompt length alone do
  not raise effort. Delegates classify their assigned scope instead of inheriting
  a parent's inflated effort. Top-level
  `effort.default` replaces that recommendation, and `effort.max_automatic`
  caps any level the router picks by itself (`default` above `max_automatic`
  is a config error). Automatic capability resolution never rounds effort up.
  Native adapters must confirm their setting. The proxy reports the accepted
  request's wire value through `router-acp/effort-update`, including provider
  clamps and unchanged fallback bodies. Missing metadata establishes no effort.
- **Determinism.** Identical inputs and state produce identical decisions —
  ranking has no randomness, and all tie-breaks are stable.
- **Every decision is disclosed** on the console
  (`disclosure: chunk`, default) or in `_meta.router_acp`
  (`disclosure: meta`), and recorded with its weights in the state file
  (`state_file`, self-pruning per `history`) for post-hoc diagnosis.
