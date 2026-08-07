# What & Why

You have a subscription account for claude, codex, etc... you want to get the most out of each model while keeping cost low.

> Tools like `OpenRouter` would require pay-per-token API calls.

This repo:

- Automatically selects the best starting model
- Hot-switches between models (even between different companies' models)
- Changes models automatically (when out of tokens, during a downtime, or when a task is too complex)
- Works right from the command line (delegating to your existing `claude` / `codex` CLI tools)

# Installation

- Start [`using this tool with goose`](GOOSE.md)
- Copy the [`example config`](examples/router-preferred.yaml) to `~/.config/router-acp/router.yaml`
- Let this tool auto-decide what model to use for any given task
- Hot switch your model via a prompt, i.e., `gpt: continue this work`
- Enable [auto-orchestration](ORCHESTRATION.md) so multi-part task lists are decomposed, routed, and cross-lineage-reviewed automatically

# Router ACP

An [ACP](https://agentclientprotocol.com/) session router over `(agent, model)`
candidates, with bounded in-session delegation.

`router-acp` is a single ACP-compatible agent process. Your ACP client (goose,
Zed, …) connects to it as if it were any coding agent; the router connects
downstream to one or more seat-authenticated ACP agent adapters such as
[`@agentclientprotocol/claude-agent-acp`](https://github.com/agentclientprotocol/claude-agent-acp),
[`@agentclientprotocol/codex-acp`](https://github.com/agentclientprotocol/codex-acp),
xAI's own CLI [`@xai-official/grok`](https://www.npmjs.com/package/@xai-official/grok)
(which speaks ACP natively via `grok agent stdio` — no separate adapter), and
Moonshot's [`kimi-cli`](https://github.com/MoonshotAI/kimi-cli) (likewise native
ACP via `kimi acp`), and provides OpenRouter-*inspired* selection semantics over
them (a local heuristic, not a port of OpenRouter's proprietary router — see below):

```
goose / Zed ──ACP──▶ router-acp ──ACP──▶ claude-agent-acp     (claude/sonnet, claude/opus)
                          │
                          ├──────ACP──▶ codex-acp             (codex/gpt-5.5, codex/gpt-5.6-sol)
                          │
                          ├──────ACP──▶ grok agent stdio      (grok/grok-4.5)
                          │
                          └──────ACP──▶ kimi acp              (kimi/kimi-k2)
```

Every new conversation is pinned to the best **candidate**, where a candidate
is an `(agent, model)` pair like `claude/sonnet`. Candidates need not span
providers: one adapter exposing multiple models yields multiple candidates,
and a one-candidate config is a valid passthrough (routing and delegation are
then inert).

This is intentionally **not** a token-router/LiteLLM architecture. Subscription
seats are accessed through vendor agent CLIs/adapters speaking ACP over stdio;
the router never calls provider model APIs directly.

## How routing works

1. `session/new` creates a router-owned session — **no downstream session
   exists yet**. The response carries router-owned config options
   (`router.strategy`, `router.candidate`) so clients with model pickers get a
   useful control.
2. The **first `session/prompt`** classifies the task (class, complexity,
   languages), filters candidates by routeability + required prompt
   capabilities (image/audio/embedded context) + quarantine + allow-globs,
   ranks them with the chosen strategy, and walks the ranked chain until one
   candidate opens a downstream session and verifies its model selection.
3. The pin is committed only after model verification succeeds, persisted to
   the state file, and disclosed to the client — **what** was chosen and
   **why**, plus every candidate that was skipped and the reason:

   ```
   [router-acp] auto → claude/sonnet · task BugFix (complexity 0.35) · utility 0.63 = 0.3×quality 1.38→0.29 (BugFix) + 0.7×quota (headroom 100%, cost rank 2)
   [router-acp] skipped codex/gpt-5.5: token/usage limit (model reports reset in ~2h05m)
   ```

   (a visible message chunk by default, or `_meta.router_acp` metadata with
   `disclosure: meta`; delegate-task routing is disclosed the same way).
4. All later prompts and callbacks for that session relay to the same
   downstream session. ACP has no transcript-handoff primitive, so a model
   change is never silent: it happens only via failover (on outage/limit) or an
   explicit/auto **switch** that summarizes the work and re-pins onto a fresh
   session — see *Switching models mid-session*.

### Strategies

Plain-language explanations of each router and its knobs live in
[`ROUTERS.md`](ROUTERS.md). In brief — all strategies return a deterministic
ranked fallback chain; ties break by higher score, lower effective cost,
preference, config order.

- **`static`** — the explicit session candidate (`router.candidate`) if set,
  else `routers.static.candidate`. If it isn't routeable you get an actionable
  error instead of silent substitution, unless
  `routers.static.allow_fallback: true`.
- **`auto`** (default) — a local, deterministic heuristic *inspired by* the
  idea behind OpenRouter's Auto Router (classify the prompt, trade cost against
  quality). It is **not** a port: OpenRouter's Auto is a closed,
  NotDiamond-backed hosted service with no published scoring function, so the
  only thing shared is the `cost_quality_tradeoff` knob (same 0–10 scale, same
  default 7). Everything below — the score table, the classifier, the utility
  formula — is router-acp's own:

  ```text
  quality_weight = 1 - cost_quality_tradeoff / 10
  cost_weight    = cost_quality_tradeoff / 10
  quality_demand = min(task_class_base + 2 * complexity, 3)
  quality_value  = (min(quality[task_class], quality_demand) - 0.5) / 3.0
  effective_headroom = min(local_headroom, reported_plan_headroom[candidate])
  quota_score    = effective_headroom * (1 - 0.5 * normalized_cost_rank)
  utility        = quality_weight * quality_value + cost_weight * quota_score
  ```

  Included-plan usage has no marginal dollar cost, so rank applies only a
  bounded 50% scarcity discount. Candidate-specific plan headroom is the larger
  cost signal; paid overage is penalized separately. An explicit
  `cost_quality_tradeoff: 0` bypasses the demand cap and remains true
  quality-max routing.

  When the prompt's classified complexity reaches `complexity_floor`,
  candidates below the 75th-percentile quality for that task class are
  dropped first. `cost_quality_tradeoff` reuses OpenRouter's one published
  knob: same scale (`0` pure quality, `10` cheapest surviving candidate) and
  same default `7`. Two behaviors that are purely router-acp's own (OpenRouter
  documents nothing here): the tradeoff **scales down with classified
  complexity** (`auto.complexity_scales_tradeoff`, default on) so trivial
  prompts go cheap while hard prompts get frontier models, and each agent's
  configured `preference` is added to its utility as a tie-break (prefer
  your bigger seat when candidates are comparable).
- **`pareto-code`** — a router-acp tiering scheme (loosely motivated by the
  price/quality-frontier idea behind OpenRouter's public model rankings, but
  **not** a documented OpenRouter algorithm), adapted from API price to
  seat-quota pressure: map
  `min_coding_score` to a tier (>= 0.66 high, >= 0.33 medium, else low; omitted
  means high), filter to that tier (stepping to a neighbor tier with a note in
  the disclosure when empty), then pick the lowest
  `effective_cost = cost_rank / max(headroom, ε)` with the next two same-tier
  candidates kept as pre-prompt fallbacks. Use it for coding sessions; `auto`
  is the general router.
- **`escalation`** — start on the cheapest capable candidate and escalate to a
  stronger one only when *observed execution* reveals hidden difficulty, rather
  than guessing complexity from the prompt. Triggers are behavioral and fire
  **mid-turn**: crossing `escalate_after_reads` investigation reads (file reads
  and read-only shell/MCP calls) before a side effect; issuing
  `escalate_after_tool_calls` total tool calls in one turn without finishing
  (the robust "grinding" signal for edit/Bash-heavy work); or
  `escalate_after_tool_failures` failed tool calls — plus post-turn on a
  token-ceiling/refusal stop. Mid-turn it interrupts and hands off a transcript
  so the stronger model continues. `escalation_path` is `ladder` (one tier up)
  or `leap` (strongest); `initial_router` optionally delegates the *starting*
  pick to another router instead of the cheapest. Best for "looks like one
  sentence, turns out hard" work — trivial tasks finish cheap at no extra cost.
  See [`ROUTERS.md`](ROUTERS.md).

### Token limits, outages, and failover

The router degrades gracefully when seats run dry or adapters fall over —
and it always tells the user what happened:

- **Token/usage limits cordon the agent until reset.** When a downstream
  reports a rate/usage limit, the router parses the reset time out of the
  error — Claude Code's `usage limit reached|<epoch>`, Codex's
  `try again in 2 hours 30 minutes`, `retry-after: 120`,
  `resets_in_seconds`, epoch and ISO-8601 timestamps are all understood
  (see `src/limits.rs`; every format is unit-tested). The whole agent is
  cordoned off from routing until that reset (or
  `headroom.cordon_default_secs` when no time was reported), and later
  routing disclosures include the cordon and its remaining time.
- **Proactive per-candidate usage cordons.** Beyond reacting to errors, the
  router can read a provider's own usage state and cordon an *exhausted model*
  before it's ever tried. Enable per-agent with `usage_source`:
  `anthropic-oauth` polls the Claude usage API (`GET /api/oauth/usage`);
  `codex-rollout` reads Codex's on-disk rate-limit snapshots (Codex has no
  pollable endpoint — its limits arrive as response headers, so these are the
  last-known snapshots from its rollout files, kept per limit pool, with the
  reactive cordon as backstop). A model-scoped weekly cap at 100% cordons just
  that candidate; an all-models or session cap cordons the whole agent — but
  only when the overage/credit pool has no *usable* headroom (Anthropic:
  overage/spend not exhausted; Codex: `unlimited` credits or a positive
  `balance` — a bare `has_credits` flag doesn't count).
  It's **generic** (which models are exhausted is read from the API, never
  hardcoded), **fails open** (a usage-endpoint hiccup never makes a model
  unroutable), and self-lifts at the reported reset. **Grok** exposes no usage
  meter at all (nothing to poll), so it needs no `usage_source`: instead the
  router watches Grok's own subscription **access gate** in its ACP stream and
  cordons the agent the moment Grok reports the gate closed (over-limit) — the
  same effect, driven by the only signal Grok provides (no reset time, so it
  uses `cordon_default_secs`). Cordoned candidates are
  excluded from `auto`, skipped by failover, and an explicit pin to one is
  refused with a fallback (disclosed in the failover format:
  `router-acp · failover: cordon → claude/sonnet · task … (Weekly Fable limit
  reached, resets …)`). If *every* candidate is cordoned, the one resetting
  soonest is used rather than failing the turn. Each candidate's cordon state
  is advertised on the `router.candidate` picker option
  (`_meta.router_acp.available/unavailable_reason/resets_at`) so a front-end can
  show it disabled. That picker option is only sent at `session/new`, so the
  full current cordon set also rides every turn's routing metadata as
  `_meta.router_acp.usage_cordons` (`[{candidate,reason,resets_at}]`) — a client
  that cached the candidate list can refresh availability mid-session as cordons
  appear or lift. Gate the whole mechanism with `cordon.enabled`.
- **Outage failover.** If the pinned model fails mid-session (process
  death, connection loss, provider overload) or hits a limit, the router
  fails the session over to the next best candidate: the failure and its
  reason are announced in the transcript, the strategy re-ranks the
  remaining pool, a fresh downstream session is opened (mode re-applied),
  and the prompt is retried there. Because ACP has no transcript handoff,
  **conversation context does not transfer** — the disclosure says so
  explicitly. Failover only happens while the failing turn has produced
  **no output** (retrying after visible output could duplicate side
  effects) and never after the client cancelled. Configure with
  `failover.enabled` / `failover.max_attempts`.
- **Automatic respawn.** A downstream process that died is respawned and
  re-probed at the next routing decision (subject to
  `failover.respawn_cooldown_secs`), so a recovered agent rejoins the pool
  without restarting the router.

### Headroom and quarantine

Without provider usage data, headroom falls back to per-agent sliding-window
counters (default 5 h) of prompts and sessions, normalized against
`budget_prompts_5h`. When a provider poll or client hint reports plan
availability, routing uses the lower of that candidate's plan headroom and the
local estimate. A rate-limit/auth/quota error before the first prompt zeroes an
agent's headroom and the router walks the fallback chain; a candidate that
keeps failing pre-prompt is quarantined for a cool-off. Errors after a session
is pinned are surfaced, never rerouted.

### Dynamic preference: route to the free seat

`agents[].preference` is a static tie-break ("this seat has the bigger
plan"). With `availability_preference` (on by default) it tracks reality
instead of staying frozen:

- **Free plan headroom is part of effective cost** — each candidate's quota
  term uses the lower of local sliding-window headroom and the seat's reported
  budget: the free plan fraction while any remains, else the remaining
  overage/credit pool once the seat is paying. The static preference bonus
  fades the same way. Model-scoped weekly caps count only for their
  model, so Claude Fable's separate window does not make other Claude models
  look scarce.
- **Paid overage raises the quality bar.** When a candidate's cap is exhausted
  but it stays routable because the overage/credit pool absorbs usage, its
  utility drops by `cost_aversion × (1 - task complexity)`. The default
  `cost_aversion: 0.1` therefore favors an included-plan fallback for ordinary
  work while still allowing a materially stronger paid model on hard work.
  Set it to `0` to ignore overage cost. A saturated seat with *no* overage
  headroom is a cordon, not a surcharge.
- **Headroom is compared in real dollars, not fractions of each seat's own
  cap.** Caps differ in size across providers and plans — a $9k pool at 3%
  free ($270) and a $3k pool at 3% free ($90) are identical percentages but
  very different seats. So the seat budget above is a *dollar* figure
  wherever one is obtainable, saturated at
  `availability_preference.headroom_scale_dollars` (default `$200`: at or
  above it a seat's quota term reads fully free, below it the seat reads
  proportionally constrained):
  - **Overage pools report real dollars directly.** Anthropic:
    `spend.limit − spend.used` (falling back to `extra_usage.monthly_limit −
    used_credits`); Codex: the per-member spend limit's `limit − used`, or a
    positive credit `balance`. Two saturated seats no longer flatten to the
    same zero — the one with real money left out-ranks the one about to hit
    its spend cap (the live failure this fixes: a codex seat with ~$100 of
    member spend left won over a claude seat with ~$6,600 of extra usage
    remaining, then hit its wall minutes later).
  - **Included plan windows are only reported as percentages** (no provider
    exposes their dollar size), so the router *estimates* them from its own
    metered spend: every proxied request is priced (`llm_requests.cost_usd`),
    and `spent × (100 − p) / p` extrapolates what the window has left. The
    estimate is skipped below a signal floor (window < 15% elapsed or < $0.50
    metered spend) and whenever the LLM proxy isn't metering — the percent
    fraction then stands in. Spend by clients outside the router raises `p`
    without raising metered spend, which under-estimates remaining budget —
    the conservative direction.
  - Percent-only fractions (`overage_headroom`, `plan_headroom`) remain the
    fallback when no dollar figure exists, and a paying seat still never
    looks "more free" than a seat with included plan remaining
    (`cap_overage_headroom`).

Availability comes from two sources:

- the same provider usage polls that drive proactive cordons
  (`agents[].usage_source` + `cordon.enabled`), and
- **client availability hints**: the `router-acp/availability_hint`
  extension notification. A client that watches seat usage itself (Kory Code
  polls both providers every minute, including a live Codex
  `account/rateLimits/read` that beats the router's on-disk snapshots) can
  push its view:

  ```json
  {
    "method": "router-acp/availability_hint",
    "params": {
      "ttl_secs": 300,
      "agents": [
        { "agent": "claude",
          "windows": [ { "percent": 72, "scope": null, "active": false },
                       { "percent": 100, "scope": "Fable", "active": true } ],
          "overage": { "enabled": true, "percent": 40, "remaining_dollars": 5400.0 } },
        { "agent": "codex",
          "windows": [ { "percent": 35, "scope": null } ] }
      ]
    }
  }
  ```

  A fresh hint outranks the router's own poll for that agent until it
  expires (`ttl_secs`, default `availability_preference.hint_ttl_secs`);
  unknown agents and windowless entries are ignored, and hints are
  session-less (send once per connection, not per session). The
  `remaining_dollars` fields (on `overage`, and accepted per window) are
  optional — a client that knows real dollars reports them and gets
  dollar-normalized ranking; percent-only hints keep working on the
  fraction fallback. Effective
  preferences show up in the routing disclosure (`+ pref 0.07` /
  `- pref 0.25 (seat on paid overage)`) and the known availability set rides
  the pin metadata as `_meta.router_acp.availability`
  (`[{candidate, plan_headroom, plan_remaining_dollars, on_overage,
  overage_headroom, overage_remaining_dollars, source}]`).

### Quality data

Per-class quality scores, coding tiers/percentiles, and context windows live
in a versioned YAML table shipped with the binary
([`data/scores.yaml`](data/scores.yaml)) and overridden wholesale with the
`score_table` config key. Entries are keyed by candidate pattern
(`agent/model`, `*` wildcards, first match wins). Updating routing quality is
a data edit, not a code change. Quality uses a benchmark-calibrated 0.5–3.5
scale: about 1 is minimal, 2 is standard, and 3 is frontier. The router maps
that scale linearly to 0–1 before combining it with quota and preference, so a
  one-point quality difference always has a defined utility meaning. The task
  class sets a minimal/implementation/reasoning demand base and complexity adds
  up to two points, capped at frontier demand 3. Capability above what the task
  can use is not rewarded, which prevents spending frontier headroom on work a
  minimal or standard model can reliably finish. Task-class overrides are
  emitted only when at least two relevant benchmark observations support a
  difference of 0.15 or more. The reproducible updater procedure is
documented in [`docs/model-updater.md`](docs/model-updater.md). The heuristic
classifier's rule tables work the same way
([`data/classifier.yaml`](data/classifier.yaml),
`classifier.rules_file`).

## Per-request LLM routing

ACP routing normally sees one `session/prompt`; a coding agent may make
hundreds of provider calls while executing it. The optional `llm_proxy`
interposes those calls. router-acp binds a loopback-only HTTP listener before
spawning adapters, replaces each configured adapter's inference base URL, and
forwards the original method, path, application headers (including
authorization), body, and streamed response. It requests an identity-encoded
upstream response so usage can be accounted while streaming; only an inference
request's top-level `model` field may change.

The request policy is deliberately stateful:

- begin on the ACP session's pinned candidate;
- demote to the cheapest routeable model from the same agent after a sustained
  streak of successful routine tool results (reads, searches, mechanical
  edits, status/CI checks);
- escalate immediately to that agent's highest-quality compatible model at or
  below the session pin's cost rank after a tool failure, repeated unchanged
  test output, refusal, HTTP failure, or token/context ceiling; per-request
  routing never spends above the pin;
- expire difficulty verdicts by request count and wall time; and
- enforce a minimum dwell before ordinary switches to bound cold-cache churn.
  Difficulty and explicit automation hints bypass dwell.

Request signals are extracted structurally and deterministically from the live
tool payload; per-request routing does not call an LLM to classify requests.
Demotion is rejected when the estimated request plus requested output exceeds
`context_window_fraction` of the target's context window. An alternate whose
window is unknown is never selected. Cordons and quarantines also apply.
`api_model` maps an ACP selector alias such as `opus[1m]` to the exact provider
API model id used for rewrites.

```yaml
llm_proxy:
  enabled: true
  listen: 127.0.0.1:0
  routine_streak: 3
  minimum_dwell_requests: 12
  verdict_ttl_requests: 6
  verdict_ttl_secs: 900
  context_window_fraction: 0.9

agents:
  - name: claude
    llm_proxy:
      protocol: anthropic
      base_url_env: ANTHROPIC_BASE_URL
      upstream_base_url: https://api.anthropic.com
    models:
      - id: opus
        api_model: claude-opus-5  # only if the ACP id is an API-invalid alias
        cost_rank: 3
      - { id: sonnet, cost_rank: 2 }
```

The explicit upstream must match the adapter's authentication mode:

| Adapter/auth | Override | Upstream | Wire |
| --- | --- | --- | --- |
| claude-agent-acp | `ANTHROPIC_BASE_URL` | `https://api.anthropic.com` | Anthropic Messages |
| codex-acp, ChatGPT OAuth | `OPENAI_BASE_URL` | `https://chatgpt.com/backend-api/codex` | OpenAI Responses |
| codex-acp, API key | `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI Responses |
| grok, browser login | `GROK_CLI_CHAT_PROXY_BASE_URL` | `https://cli-chat-proxy.grok.com/v1` | OpenAI Chat |
| grok, API key/custom catalog | `GROK_MODELS_BASE_URL` | `https://api.x.ai/v1` (or custom) | OpenAI Chat |
| kimi, browser login | `KIMI_BASE_URL` | `https://api.kimi.com/coding/v1` | OpenAI Chat |

This routes among models accepted by the same adapter/upstream/auth seat; it
does not translate wire formats or credentials across providers. Future agents
only need a base-URL environment variable, an explicit upstream, and either
the `anthropic` or `openai` wire setting.

Attribution is exact for `spawn-config` processes and for a process with one
active ACP prompt. If a shared `config-option` process serves concurrent
prompts, an adapter session marker (`prompt_cache_key`, `session_id`, or
equivalent) resolves the owner; otherwise the request passes through unchanged
rather than risking a cross-session rewrite. Invalid JSON, non-inference
endpoints, ambiguous traffic, or listener bind failure likewise degrade to
transparent ACP-turn routing.

Relay-generated automation can bypass the routine streak by attaching this to
the ACP prompt:

```json
{"_meta":{"router_acp":{"request_hint":"ci-poll"}}}
```

Accepted hints are `ci-poll`, `ship-nudge`, and `automation`.

Every attributed call is written to `llm_requests`, including pinned and
selected model, reason/event, endpoint, latency, HTTP status, exact usage,
cache read/write tokens, and API-equivalent cost. `tool_calls` tracks tool
lifecycle and the `active_tool_calls` view exposes the model currently serving
in-flight tools:

```sql
SELECT router_session_id, tool_call_id, title, model, started_at
FROM active_tool_calls;

SELECT model, routing_event, count(*) AS requests, sum(cost_usd) AS cost
FROM llm_requests
GROUP BY model, routing_event;
```

## Prompt routing directives

Recipes and scripts (or any client that can't set ACP session config
options, like the goose CLI) can steer routing in-band with a `[router: …]`
tag anywhere in the prompt — on its own line, after a `<turn-context>`
preamble, or inline with the task on the same line (the tag is bracket-matched
so nested `[1m]` model ids and trailing text are handled). A bare directive
with no task is fine too:

```
[router: candidate=claude/claude-fable-5[1m]]      # pin this session
[router: prefer=codex/gpt-5.5]                      # soft preference (falls back)
[router: strategy=pareto-code]                      # set session strategy
[router: exclude=claude|codex/gpt-5.4-mini]         # ban lineages/candidates
[router: switch=claude/opus[1m]]                    # change models mid-session
[router: switch=claude/opus[1m]] now what?          # …and prompt it in one line
```

The router strips the line (downstream models never see it), fails loudly on
invalid directives, and records applied directives in the disclosure and
state file. `candidate`, `prefer`, `strategy`, and `exclude` apply **pre-pin
only** (post-pin: stripped + visible "ignored" note); `exclude` persists for
the session, including failover re-pins. `prefer` is a *soft* pin — the named
candidate goes to the front of the ranked chain if it is eligible, otherwise
routing falls back to the strategy's normal winner rather than erroring.

`switch` is the exception that works **mid-session**: it hands the live
conversation to a different model (see below). These directives let any CLI
client steer routing even though it can't set ACP session config options.

## Switching models mid-session

A pinned session isn't stuck. Because ACP can't transfer a live transcript
between agents, the router bridges with a **summary**: it asks the current
model to write a handoff (task, decisions, files touched, what's left), opens a
fresh downstream session on the target, prepends that summary to your next
prompt, re-pins, and closes the old session. The summary turn is captured
internally and never shown; the switch is disclosed like any routing decision
and recorded (`from`, `to`, reason) in the state file. **If the old model can't
summarize** (offline, rate-limited, crashed, refuses), the router falls back to
a truncated transcript reconstructed from its SQLite `session_log` and seeds
that instead — so a switch (including an auto-upgrade fired *because* the model
is failing) still completes without the dead model's help. Three triggers:

- **Explicit** — `[router: switch=agent/model]` on any line of a prompt in a
  pinned session; the rest of the prompt continues on the new model. Or use the
  **`model:` shorthand** — start a message with a model reference and a colon:

  ```
  opus: take over and finish the refactor
  codex/gpt-5.5: review this
  gpt-5.5:                      # bare — switches, then the new model greets you
  ```

  The reference can be a full `agent/model` id (`claude/opus[1m]`), a bare model
  id (`sonnet`, `gpt-5.5`), a family/prefix (`gpt`, `opus`), or a suffix-less id
  (`claude/opus` → `claude/opus[1m]`). It resolves to the best eligible matching
  candidate (highest quality on ambiguity); a token that names no candidate is
  treated as ordinary prose and left alone. Pre-pin the shorthand steers the
  initial pin instead of switching.
- **Auto-upgrade** — after each turn the router scores the session's
  *confidence* (the pinned model's quality for the task class minus a
  *struggle* score that rises on token-ceiling hits, refusals, and repeated
  in-turn tool failures). Below `auto_upgrade.confidence_threshold` it upgrades
  to the best strictly-more-capable eligible candidate on the next prompt. Set
  `auto_upgrade.enabled: false` to disable; explicit `switch=` still works.
- **Skill routing** — `skill_routing` forces skills that need a capable model
  onto a class of candidates. When a prompt invokes a configured skill (as
  `/name` or a standalone token) and the pinned model isn't in the skill's
  candidate globs, the session switches to the best available match (pre-pin it
  steers the initial routing). See [`ROUTERS.md`](ROUTERS.md) for the full model.

## In-session delegation

The pinned primary agent gets a router-provided MCP tool, `delegate_task`,
for small self-contained subtasks (mechanical edits, isolated bug fixes,
focused research). The router runs each delegated subtask in an **ephemeral
downstream session on a strictly lower-`cost_rank` candidate**, preferring a
same-agent sibling before using a cross-lineage fallback. Thus a Sol primary
delegates ordinary work to Terra/Luna when they are eligible. It returns the
sub-agent's output as the tool result, and forwards the sub-session's
permission/fs/terminal callbacks to the original client under the parent
session id — permission UX stays intact while sub-agent transcript streaming
is not interleaved into the parent transcript. Delegates have no upstream
client of their own, so the router explicitly applies each candidate's `auto`
`mode_map` entry at session creation (for example Claude `bypassPermissions`
or Codex `agent-full-access`). Their full task/final response, streamed progress,
and tool activity are recorded in the delegate's `session_log` row for UIs.

Because ACP `mcpServers` entries must be concrete transports, the tool is a
real stdio helper: the router appends
`router-acp mcp-delegate --socket <unix-socket> --token <session-token>` to
the pinned session's MCP servers. The helper bridges its stdio to the parent
router; the random per-session token maps the connection to the owning
session. The delegate server is *not* injected when delegation is disabled,
when only one candidate exists, or when no cheaper candidate exists for the
parent — and never into delegated sessions themselves (depth is capped at 1).

Concurrency is bounded by `delegation.max_concurrent`; `session/cancel` on the
parent cancels the primary prompt and all active sub-sessions.

For **review→fix→re-review** loops, a delegated sub-session can be kept alive:
`delegate_task` with `keep_open: true` returns a `delegate_id`; the caller then
sends more instructions to that same sub-agent (context preserved) with
`delegate_followup`, and frees it with `delegate_close`. These are what let the
orchestrator (below) iterate on a subtask without re-briefing a fresh session.

For **parallel** subtasks there is a background mode: MCP clients execute tool
calls one at a time, so N plain `delegate_task` calls run the subtasks
serially no matter what the router allows. `delegate_task` with
`background: true` instead returns a `b-…` job id immediately and runs the
subtask on its own task; `delegate_await` collects results — it waits up to
`timeout_seconds` (default 600, clamped to 5–1500) for the given ids (default:
all pending), returns every finished job's output exactly once, and lists the
ones still running so the caller polls with short, idle-timeout-safe calls.
`background` composes with `keep_open` (the collected result carries the
`delegate_id`), and `delegation.max_concurrent` still bounds how many jobs
execute at once.

## Auto-orchestration

When a prompt reads as a **multi-part task list** — markdown bullets or numbers,
inline `(1) … (2) …`, or ordered prose ("first … then … finally …") — the
router runs a plan → subtasks → review → submit pipeline entirely in-process. It
steers (pre-pin) or switches (mid-session) the session onto a **planner**
frontier model and injects an orchestration protocol instructing that model to:

1. **Plan** — restate the task as success criteria, split it into
   file-disjoint, self-contained subtasks, and state a confidence (0.0–1.0)
   that the plan will satisfy the criteria.
2. **Delegate** — dispatch independent subtasks **in parallel** via
   `delegate_task background: true` + `delegate_await`, each routed
   per-complexity in its own sub-session.
3. **Review** — after every implementation subtask is collected, delegate an
   independent review to a candidate of a **different lineage** than the
   planner (`reviewer` globs), handing it the original task verbatim. The
   planner skips the review — with a note in its report — when no
   cross-lineage reviewer is available or when its stated confidence exceeds
   `review_confidence`; under `submit: merge` the review is never skipped.
4. **Adjudicate** — fix blocking issues and re-review, bounded by
   `max_fix_rounds`.
5. **Submit** — per the `submit` gate (`never | branch | pr | merge`); a merge
   is only permitted after an approving review.

The one thing this needs beyond ordinary delegation is **peer delegation**: an
orchestrating session may delegate to *same-* or *higher*-tier candidates (not
just strictly-cheaper ones), so the cross-lineage reviewer is actually
routeable. Everything else — the decomposition, the review, the submission gate
— is the planner model following the injected protocol with its own tools plus
`delegate_task`/`delegate_followup`.

Auto-orchestration is **off by default** (`orchestration.enabled`), fires on any
prompt (pre- or post-pin), and **takes precedence over `skill_routing`**: a
multi-part task list orchestrates even if it names a skill (e.g. `ship-pr`) — the
planner decides when to invoke that skill, and end-of-work skills like shipping
run *after* the work is done and reviewed, never up front. It is **suppressed**
only by an explicit `[router: …]` directive or `model:` shorthand (so you can
always opt a prompt out), and also when the list is **you answering the model's
own questions** (e.g. it asked "Open decisions: (1)… (2)…" and you reply with a
matching list — the router sees that the previous agent turn solicited answers
and relays normally). Each trigger is disclosed
(`router-acp · orchestrating a N-part task on …`).

Because the orchestration primitive is the router's own `delegate_task` tool,
this works in **any** ACP client and plain chat session — no recipe, no `summon`
extension, no wrapper. See [`ROUTERS.md`](ROUTERS.md) and
[`ORCHESTRATION.md`](ORCHESTRATION.md).

Two related prompt features:

- **Force it**: start a message with `orchestrate:` (or `orchestrator:`) to run
  the pipeline on any task, list or not — it overrides every auto-detection
  gate (including `orchestration.enabled`).
- **Ticket context** (`ticket_context`, pluggable): when a prompt references a
  ticket id (e.g. "Fix HAI-1234"), the router runs the configured command
  (`linear issue view $TICKET`, `jira …`, `gh issue view …` — any CLI that
  prints the ticket) and prepends the ticket's content to the prompt **before**
  classification and orchestration detection. A bare ticket mention thus routes
  on the ticket's real scope, and a ticket whose body is a work list triggers
  orchestration — with the planner delegating its parts. Fail-open (a bad fetch
  never blocks the turn), once per ticket per session, fetches cached ~5 min.

> **Caveat — the planner must use `delegate_task`.** Sub-session routing, the
> cross-lineage review, and the `parent_session_id`/`run_label` rows all depend
> on the planner calling `delegate_task`. Some adapters ship a *built-in*
> sub-agent tool (Claude's `Task`) that spawns same-lineage sub-agents *inside*
> the adapter — invisible to the router. The injected protocol explicitly forbids
> that tool and mandates `delegate_task` with a concrete different-lineage
> reviewer id, but the router cannot remove the adapter's own tool; a model that
> ignores the instruction degrades to same-lineage, unobservable orchestration.
> If you see no delegate rows in the state DB after an orchestrated run, the
> planner used its native tool — the router now detects this, warns inline
> (`orchestration degraded: …`), and records it (`native_subagent_calls`).

Evaluate orchestrated runs — real per-run cost (from the adapter's own
`usage_update.cost`), delegate split, cross-lineage-review presence, and
degraded% — with:

```sh
router-acp report --config router.yaml
```

## Install and run

```sh
cargo install --path .           # or cargo build --release
router-acp check-config --config router.yaml
router-acp serve --config router.yaml
```

Point your ACP client at the `serve` command. For goose, configure a
`claude-acp`-style provider whose command is
`router-acp serve --config /path/to/router.yaml`; in Zed, add it as a custom
agent server:

```json
{
  "agent_servers": {
    "router-acp": {
      "command": "router-acp",
      "args": ["serve", "--config", "/path/to/router.yaml"]
    }
  }
}
```

Logging goes to stderr (stdout carries the protocol): `RUST_LOG=router_acp=debug`.

## Configuration reference

See [`examples/router-full.yaml`](examples/router-full.yaml) for a complete annotated
example.

| Key | Default | Meaning |
| --- | --- | --- |
| `router` | `auto` | Default strategy: `auto`, `pareto-code`, `escalation`, `static`. |
| `state_file` | `~/.local/state/router-acp/sessions.db` | SQLite database. `sessions` records pins, lineage, token/context totals, and per-request aggregate cost/count. `session_log` records ACP and proxy events. `llm_requests` records each attributed provider request's model, policy event, latency, exact/cache tokens, and cost. `tool_calls` plus `active_tool_calls` expose tool/model lifecycle. A legacy `sessions.json` beside it is imported once. |
| `history` | `30d` | How long to keep sessions before auto-pruning (and their logs, by cascade). Duration string: `30d`, `12h`, `90m`, `3600s`, or a bare number of days. Pruned on open and after each write. |
| `score_table` | built-in | Path to a score-table YAML overriding the shipped data. |
| `disclosure` | `chunk` | `chunk` = visible status line before the first response; `meta` = attach route details under `_meta.router_acp` on the first forwarded update. |
| `probe_timeout_ms` | `120000` | Timeout for downstream initialize/probe/session-open calls. |
| `classifier.backend` | `heuristic` | `heuristic` or `local-model`. |
| `classifier.local_model` | – | e.g. `ollama:qwen3:4b` (or `ollama@host:port:model`); temperature 0, JSON output. Falls back to the heuristic on timeout/parse failure/unavailable runtime. Never uses the seat-backed ACP agents. |
| `classifier.timeout_ms` | `1500` | Local-model call timeout. |
| `classifier.rules_file` | built-in | Path to a classifier rules YAML. |
| `delegation.enabled` | `true` | Offer `delegate_task` to pinned sessions. |
| `delegation.max_concurrent` | `3` | Concurrent delegated sub-sessions. |
| `delegation.socket_path` | temp dir | Unix socket the delegate helper connects back on. |
| `headroom.window_secs` | `18000` | Sliding-window length (5 h). |
| `headroom.quarantine_failures` | `3` | Pre-prompt failures in the window before quarantine. |
| `headroom.quarantine_cooloff_secs` | `600` | Quarantine cool-off. |
| `headroom.cordon_default_secs` | `900` | Cordon length for a rate/usage-limited agent when the error carries no parseable reset time. |
| `cordon.enabled` | `true` | Master switch for proactive usage-cap cordons (inert unless an agent has a `usage_source`). Also gates the usage polling that feeds `availability_preference`. |
| `cordon.poll_secs` | `300` | Usage poll interval / cache TTL. |
| `availability_preference.enabled` | `true` | Plan-aware routing: effective quota headroom is the lower of local headroom and the candidate's seat budget (real dollars where estimable, else the reported plan fraction); effective preference scales the same way. Paid overage also incurs a difficulty-scaled surcharge. Off = local headroom plus static preference, hints ignored. |
| `availability_preference.cost_aversion` | `0.1` | Paid-overage surcharge coefficient. Utility subtracts `cost_aversion × (1 - task complexity)`; `0` means the user is perfectly willing to pay. |
| `availability_preference.hint_ttl_secs` | `600` | How long a client `router-acp/availability_hint` outranks the router's own poll (per agent). |
| `availability_preference.headroom_scale_dollars` | `200` | Remaining budget, in dollars, at/above which a seat's quota term reads fully free. Ranking compares real dollars (overage pools directly from the provider API, plan windows estimated from the router's own metered spend), not the fraction of each provider's own (differently-sized) cap. |
| `agents[].usage_source` | – | Optional provider usage source for proactive cordons. `{ type: anthropic-oauth }` reads the Claude CLI OAuth token (`~/.claude/.credentials.json` or the macOS Keychain) and polls `GET /api/oauth/usage`. `{ type: codex-rollout }` reads Codex's own on-disk rate-limit snapshots (`~/.codex/sessions/**/rollout-*.jsonl`), newest per limit pool — last-known (Codex has no pollable endpoint), reactive cordon backstops it; credits only bypass a saturated window when actually usable (`unlimited` or positive `balance`). |
| `llm_proxy.enabled` | `false` | Interpose configured adapters and route each attributed provider inference request. Bind/config failures leave ACP-turn routing active. |
| `agents[].llm_proxy.codex_chatgpt_provider` | `false` | For an OpenAI-protocol Codex agent, install a custom HTTP Responses provider so ChatGPT-authenticated Codex traffic traverses the proxy instead of bypassing it over WebSocket. |
| `llm_proxy.listen` | `127.0.0.1:0` | Loopback listener; port `0` selects a free port. Non-loopback addresses are rejected because provider credentials pass through it. |
| `llm_proxy.routine_streak` | `3` | Consecutive successful routine tool-result requests required before demotion. |
| `llm_proxy.minimum_dwell_requests` | `12` | Requests a selected model serves before an ordinary switch; difficulty and automation bypass it. |
| `llm_proxy.verdict_ttl_requests` / `verdict_ttl_secs` | `6` / `900` | Request-count and wall-clock expiry for a difficulty escalation verdict; `0` disables that expiry dimension. |
| `llm_proxy.context_window_fraction` | `0.9` | Maximum fraction of an alternate model's known context window that an estimated request may occupy. |
| `llm_proxy.max_request_bytes` / `max_capture_bytes` | `32 MiB` / `4 MiB` | Request buffering limit and head/tail response capture limit. Responses still stream in full. |
| `agents[].llm_proxy` | – | Adapter interposition: `protocol` (`anthropic` or `openai`), `base_url_env`, and the real `upstream_base_url`. |
| `failover.enabled` | `true` | Fail a pinned session over to the next best candidate on limit/outage (only before any output streamed this turn). |
| `failover.respawn_cooldown_secs` | `30` | Minimum interval between respawn attempts of a dead downstream process. |
| `failover.max_attempts` | `3` | Candidates tried per prompt (initial + failovers). |
| `auto_upgrade.enabled` | `true` | Auto-switch a pinned session up to a more capable model when confidence drops. `false` disables it (explicit `[router: switch=…]` still works). |
| `auto_upgrade.confidence_threshold` | `0.55` | Upgrade when confidence (fraction of task capability demand met − struggle) falls below this. Higher = more eager; `0` ≈ never. |
| `skill_routing[]` | `[]` | Rules forcing a skill onto a model class: `pattern` (skill name, matched as `/name` or a standalone token) → `candidates` (candidate globs in preference order). Mid-session it switches; pre-pin it steers routing. |
| `ticket_context[]` | `[]` | Ticket-loading rules: `prefix` (e.g. `HAI-`, matched at a word start + digits) → `command` (argv, `$TICKET` substituted, run without a shell) whose stdout is prepended to the prompt before routing. Pluggable across ticketing systems; fail-open. |
| `orchestration.enabled` | `false` | Auto-orchestrate multi-part task lists: steer/switch to a planner model and inject the decompose→delegate→review→submit protocol. |
| `orchestration.min_items` | `2` | Smallest detected list size treated as a multi-part task. |
| `orchestration.planner[]` | frontier globs | Planner/orchestrator candidate globs — they define the pool; the pick is by preference-adjusted quality (`quality + agents[].preference`), glob order breaking ties. |
| `orchestration.reviewer[]` | frontier globs | Preferred cross-lineage reviewer globs handed to the orchestrator (it should pick a different lineage than the planner). |
| `orchestration.submit` | `branch` | Submission gate given to the orchestrator: `never \| branch \| pr \| merge` (a merge is only permitted after the review approves). |
| `orchestration.max_fix_rounds` | `2` | Max review → fix → re-review rounds. |
| `orchestration.review_confidence` | `0.8` | Planner self-confidence bar (0.0–1.0) for skipping the review pass: strictly above it, the review is skipped with a note. Ignored under `submit: merge` (a merge always requires an approving review). |
| `agents[].name` | – | Unique, no `/`. Candidate ids are `name/model-id`. |
| `agents[].command` | – | `type: stdio` command/args/env for the adapter. `${VAR}` in the whole file interpolates from the environment (unknown names are left intact). |
| `agents[].model_selection.type` | – | `config-option`: one process per agent; the router discovers the `category: model` select option at probe time and applies `session/set_config_option` per session. `spawn-config`: one process per model, built from `process_template` (with `${model_id}` substitution); no universal `-m` flag is assumed. |
| `agents[].budget_prompts_5h` | `400` | Headroom normalization budget. |
| `agents[].models[]` | – | `id` (must exactly match the downstream selector's value for `config-option`), optional `display_name`, `api_model` (provider model id for proxy rewrites; defaults to `id`), `cost_rank` (1 = cheapest/least scarce), and optional API-equivalent `pricing`. |
| `agents[].mode_map` | `{}` | Translate client-requested session mode ids to this agent's ids (e.g. goose's `auto` -> claude's `bypassPermissions`). When `pre_classifier.enabled`, an evaluator that advertises session modes requires an explicit tool-safe `preclass` entry whose target it advertises; it is applied before the classifier prompt. Mode-less adapters run the evaluator without `session/set_mode`. |
| `agents[].lineage` | agent name | Model-company tag (e.g. `anthropic`, `openai`). Orchestration's cross-lineage review requires the reviewer's lineage to differ from the planner's — the intent is a different **company** with different failure modes — so two agents backed by the same vendor should declare the same `lineage`. |
| `agents[].preference` | `0` | Additive utility tie-break for this agent (`auto`) and within-tier tie-break (`pareto-code`). Keep small, e.g. `0.05`. Scaled dynamically by seat availability unless `availability_preference.enabled: false`. |
| `routers.auto.complexity_scales_tradeoff` | `true` | Scale the tradeoff by `1 − complexity`: cost matters for trivial prompts, quality dominates hard ones. |
| `routers.static.candidate` | – | `agent/model` default for `static`. |
| `routers.static.allow_fallback` | `false` | Fall back in config order if the static candidate is unavailable. |
| `routers.auto.*` | see above | `cost_quality_tradeoff` (0–10), `complexity_floor`, `allowed_candidates` globs. |
| `routers.pareto-code.min_coding_score` | high tier | Maps to a router-acp coding tier (≥0.66 high, ≥0.33 medium, else low). |
| `routers.escalation.escalation_path` | `ladder` | `ladder` (next tier up) or `leap` (straight to the strongest) on each escalation. |
| `routers.escalation.initial_router` | *(none)* | Delegate the *starting* candidate to another router (`auto`/`pareto-code`/`static`) instead of the cheapest; escalation still applies from there. Cannot be `escalation`. |
| `routers.escalation.escalate_before_side_effects` | `true` | Escalate mid-turn while still investigating (before any output/edit); `false` = post-turn only. |
| `routers.escalation.escalate_after_reads` | `6` | Mid-turn read-volume trigger; investigation events (incl. read-only shell/MCP) before a side effect (`0` = off). |
| `routers.escalation.escalate_after_tool_calls` | `30` | Mid-turn "grinding" trigger: total tool calls in one turn without finishing (robust to edit/Bash-heavy work; `0` = off). |
| `routers.escalation.escalate_after_tool_failures` | `3` | Post-turn: failed tool calls in a turn before escalating (`0` = off). |
| `routers.escalation.escalate_on_max_tokens` / `escalate_on_refusal` | `true` | Post-turn escalation on a max-tokens / refusal stop. |
| `routers.escalation.min_start_score` | `0` | Optional class-quality floor on the *starting* candidate. |
| `routers.escalation.max_escalations` | `3` | Hard cap on escalations per session. |

## Session modes

Some clients (goose) send `session/set_mode` right after `session/new`,
before the first prompt exists. The router accepts and **defers** the mode,
applying it to the downstream session at pin time: the agent's `mode_map`
translation wins, then an exact id match against the modes the downstream
advertised; with no equivalent the mode is skipped with a warning and the
downstream stays in its default mode. Post-pin mode changes relay with the
same translation, and are likewise lenient when no equivalent exists.
Delegated sessions explicitly apply the candidate's `auto` mapping because
there is no separate upstream client to send them a mode request.

Pre-classifier sessions are intentionally different: an adapter that advertises
session modes requires an explicit `mode_map.preclass` target (for example
`preclass: plan` for Claude or `preclass: read-only` for Codex), and the target
must be advertised by that adapter. The router never reuses `auto` or guesses
`chat`; an adapter with modes but without that safe mapping is skipped before it
receives the user or classifier prompt. An adapter that advertises no modes
(such as Grok) has no mode gate to arm and runs without `session/set_mode`.
Any evaluator tool call or downstream callback is cancelled and rejected.

## First-run authentication

Adapters guard subscription seats behind their own auth. `initialize` is
two-phase:

1. The router spawns and initializes every configured adapter, then probes
   `session/new` on each. Agents whose probe returns `auth_required` are
   marked **auth-pending**: their candidates are declared but not routeable
   yet, and the router still initializes so your client can authenticate.
2. Downstream auth methods are advertised **namespaced** as
   `<agent>/<methodId>` (e.g. `claude/claude-login`). Pick one in your
   client; the router relays `authenticate` to that agent and re-runs probe
   verification. As soon as one candidate verifies, `session/new` works.

While only auth-pending candidates exist, `session/new` returns
`auth_required`. If zero candidates are routeable and none are auth-pending,
`initialize` fails with a configuration error.

Tip: adapters usually persist auth in your home directory, so it is often
easiest to log in once with the vendor CLI (`claude`, `codex login`) before
starting the router.

## Session lifecycle

`session/list`, `session/load`, `session/resume`, `session/delete`, and
`session/close` are implemented end-to-end and advertised **only when at
least one downstream supports them**:

- `list` merges downstream lists, rewriting downstream ids to router ids via
  the state file (sessions the router can't route back are omitted).
- `load`/`resume` require a known router session id in the state file, route
  to the owning downstream only, rehydrate the pin before any prompt, and
  relay replayed transcript updates under the router id.
- `delete` routes to the owning downstream, then removes router state.
- `close` closes the live downstream session (state-file entries survive so
  `load`/`resume` keep working when the downstream persists sessions).

## Troubleshooting

**`initialize` fails with "zero routeable candidates".**
Every agent failed spawn/probe/model validation. Run
`router-acp check-config --config …`, then `RUST_LOG=router_acp=debug` and
read the per-target warnings (spawn failure, probe timeout, missing model
option).

**"declared model not offered by downstream model selector".**
Your `models[].id` doesn't exactly match the adapter's model-selector values.
The candidate is removed from the pool at startup (missing models are never
discovered later at prompt time). Debug logging prints the values the adapter
actually offers; copy the exact id.

**"model verification failed … silent no-op".**
The adapter accepted `session/set_config_option` but still reports another
model as current. The router refuses to pin (the response is authoritative;
no `config_option_update` notification is awaited). Usually this means the
model id exists in the selector but the seat/plan doesn't allow it.

**"target … advertises no `category: model` select config option".**
The agent is configured with `model_selection: config-option` but the adapter
exposes no model selector. Switch to `spawn-config` with a per-model process
template, or declare exactly the adapter's default model.

**Notice: "routing directive ignored (session already pinned)".**
`router.strategy`/`router.candidate`/`prefer`/`exclude` only shape the *initial*
routing decision, so they're ignored after the first prompt. To change models
mid-session use `[router: switch=agent/model]`, which summarizes the work and
re-pins onto a fresh downstream — or open a new session.

**Candidate keeps being skipped.**
It may be quarantined after repeated pre-prompt failures (see
`headroom.quarantine_*`), or its agent's headroom hit 0 after a rate-limit
error; both recover with time. Check the disclosure line/`_meta.router_acp`
for the note explaining tier or fallback decisions.

**Delegation tool never appears.**
The tool is only injected when delegation is enabled, more than one candidate
is routeable, and a strictly cheaper candidate exists for the pinned parent.

## Integration matrix

Automated protocol tests cover a scripted mock downstream (see below).
Manual matrix for real seats:

| Client | Downstream | Notes |
| --- | --- | --- |
| goose | claude-agent-acp | `router-acp serve` as a claude-acp-style provider command. |
| Zed | claude-agent-acp | `agent_servers` entry as above; model picker shows `router.candidate`. |
| goose / Zed | codex-acp | Use `spawn-config` with `CODEX_CONFIG` per model. |
| goose / Zed | grok (`@xai-official/grok`) | Native ACP: `command: grok`, `args: ["agent"]`, `spawn-config` template `["--model", "${model_id}", "stdio"]`. Auth via `grok login`; confirm ids with `grok models`. |
| goose / Zed | kimi (Moonshot `kimi-cli`) | Native ACP: `command: kimi`, no base args, `spawn-config` template `["--model", "${model_id}", "acp"]` (`--model` is a global flag, so it precedes `acp`). Auth via `kimi login` (browser OAuth, auto-configures model ids). |
| any | mixed Claude + Codex, auth-pending at startup | Authenticate via the namespaced method ids. |
| any | single-candidate config | Passthrough; startup logs "routing and delegation are inert". |

## Development

Contributor/agent context (architecture, SDK pitfalls, test infrastructure,
invariants) lives in [`AGENTS.md`](AGENTS.md).

```sh
cargo test                 # unit + protocol tests (spawns mock-agent binaries)
cargo run --bin mock-agent # scripted ACP downstream used by the tests
```

The protocol tests drive the full router in-process against real `mock-agent`
subprocesses: lazy pinning, model verification (including silent no-op
detection), namespaced auth, capability filtering, fallback chains, crash
handling, cancellation, delegation (including the 5-bug acceptance scenario
with bounded concurrency and no recursive delegate injection), and the
lifecycle roundtrip.

Two implementation notes for the curious:

- The router is a terminal ACP agent upstream plus N downstream ACP client
  connections — deliberately not the linear proxy-chain/conductor role.
- Transports are local replacements over the SDK's `Lines` component that
  flush after every line: the SDK's built-in `Stdio`/`AcpAgent` transports
  (agent-client-protocol 1.2.0) buffer writes without flushing, which
  deadlocks small JSON-RPC exchanges.
