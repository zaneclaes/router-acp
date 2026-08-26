# router-acp

## What & Why

You have a subscription account for claude, codex, etc... you want to get the most out of each model while keeping cost low.

> Tools like `OpenRouter` would require pay-per-token API calls. router-acp drives the CLIs you already pay for, so requests spend seat quota instead.

This repo:

- Automatically selects the best starting model
- Hot-switches between models (even between different companies' models)
- Changes models automatically (when out of tokens, during a downtime, or when a task is too complex)
- Works right from the command line (delegating to your existing `claude` / `codex` CLI tools)
- Hands small subtasks to cheaper models, and can run whole task lists itself (plan → parallel subtasks → review by a different company's model)
- Tells you what it picked, why, and what it skipped

## Contents

- [How it fits together](#how-it-fits-together)
- [Install & quick start](#install--quick-start)
- [How routing works](#how-routing-works)
  - [Strategies](#strategies)
  - [Token limits, outages, and failover](#token-limits-outages-and-failover)
  - [Headroom, quarantine, and availability-aware preference](#headroom-quarantine-and-availability-aware-preference)
  - [Task classification](#task-classification)
  - [Quality data](#quality-data)
- [Per-request LLM routing](#per-request-llm-routing)
- [Prompt routing directives](#prompt-routing-directives)
- [Switching models mid-session](#switching-models-mid-session)
- [In-session delegation](#in-session-delegation)
- [Auto-orchestration](#auto-orchestration)
- [Configuration reference](#configuration-reference)
- [Session modes](#session-modes)
- [First-run authentication](#first-run-authentication)
- [Session lifecycle](#session-lifecycle)
- [Troubleshooting](#troubleshooting)
- [Integration matrix](#integration-matrix)
- [Development](#development)

## How it fits together

`router-acp` is a single ACP-compatible agent process — an ACP session router over `(agent, model)` **candidates**, with bounded in-session delegation. Your ACP client connects to it as if it were any coding agent; downstream, the router holds connections open to one or more seat-authenticated ACP adapters: [`@agentclientprotocol/claude-agent-acp`](https://github.com/agentclientprotocol/claude-agent-acp), [`@agentclientprotocol/codex-acp`](https://github.com/agentclientprotocol/codex-acp), xAI's own CLI [`@xai-official/grok`](https://www.npmjs.com/package/@xai-official/grok) (it speaks ACP natively via `grok agent stdio` — no separate adapter needed), and Moonshot's [`kimi-cli`](https://github.com/MoonshotAI/kimi-cli) (likewise native via `kimi acp`):

```
goose / Zed ──ACP──▶ router-acp ──ACP──▶ claude-agent-acp     (claude/sonnet, claude/opus)
                          │
                          ├──────ACP──▶ codex-acp             (codex/gpt-5.5, codex/gpt-5.6-sol)
                          │
                          ├──────ACP──▶ grok agent stdio      (grok/grok-4.6)
                          │
                          └──────ACP──▶ kimi acp              (kimi/kimi-k2)
```

Every new conversation is pinned to the best **candidate** — an `(agent, model)` pair like `claude/sonnet`. Candidates don't have to span providers (one adapter exposing several models yields several candidates), and a single-candidate config is a valid passthrough, with routing and delegation simply inert.

This is intentionally **not** a token-router/LiteLLM architecture. Subscription seats are accessed through vendor agent CLIs/adapters speaking ACP over stdio; the router never calls provider model APIs directly. (The optional [per-request proxy](#per-request-llm-routing) only rewrites the model name on requests the CLI was already making.)

## Install & quick start

```sh
cargo install --path .              # or: cargo build --release
router-acp check-config --config router.yaml
router-acp serve --config router.yaml
```

1. Copy [`examples/router-preferred.yaml`](examples/router-preferred.yaml) to `~/.config/router-acp/router.yaml` as a starting point, and fill in the adapters you actually have installed (see the [integration matrix](#integration-matrix)).
2. Point your ACP client at `router-acp serve --config ~/.config/router-acp/router.yaml`:
   - **goose** has no generic "point at any ACP command" slot — see [`GOOSE.md`](GOOSE.md) for the exact (small) shim-based hookup, still current for goose ≥ 1.41.
   - **Zed** — add it as a custom agent server:
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
3. Send a prompt. The first reply opens with a routing disclosure line, e.g. `[router-acp] auto → claude/sonnet · task BugFix (complexity 0.35) · …` — that line is your proof the router is serving the session.
4. From there: let it auto-decide what model to use for any given task, hot switch via a prompt (`gpt: continue this work`), and enable [auto-orchestration](#auto-orchestration) so multi-part task lists are decomposed, routed, and reviewed automatically.

Logging goes to stderr (stdout carries the ACP protocol): set `RUST_LOG=router_acp=debug` for verbose routing/model-discovery logs.

## How routing works

1. **`session/new`** creates a router-owned session; no downstream session exists yet. The response includes router-owned config options (`router.strategy`, `router.candidate`) so a client with a model picker gets a useful control.
2. **The first `session/prompt`** classifies the task (class, complexity, languages, image/audio/embedded-resource needs), filters candidates by routeability, capability, quarantine, and allow-globs, ranks what's left with the chosen strategy, and walks the ranked chain until one candidate opens a downstream session and its model selection verifies.
3. **The pin is committed** only after that verification succeeds. It's persisted to the state file and disclosed to the client — what was picked, why, and every candidate that was skipped and why:

   ```
   [router-acp] auto → claude/sonnet · task BugFix (complexity 0.35) · utility 0.63 = 0.3×quality 1.38→0.29 (BugFix) + 0.7×quota (headroom 100%, cost rank 2)
   [router-acp] skipped codex/gpt-5.5: token/usage limit (model reports reset in ~2h05m)
   ```

   (a visible message chunk by default, or `_meta.router_acp` metadata under `disclosure: meta`; delegated-task routing is disclosed the same way.)
4. **Every later prompt and callback** for that session relays to the same downstream session. ACP has no transcript-handoff primitive, so a model change is never silent — it only happens through [failover](#token-limits-outages-and-failover) or an explicit/automatic [switch](#switching-models-mid-session), which summarizes the work and re-pins onto a fresh session.

### Strategies

Four routers ("strategies") decide the initial pick. Whichever runs, candidates go through the same eligibility filter first (verified, capability-matched, not cordoned, not quarantined), ties break by higher score → lower effective cost → preference → config order, and the decision is deterministic — identical inputs and state always produce the same pin. Plain-language walkthroughs, the exact formulas, and tuning guidance live in [`ROUTERS.md`](ROUTERS.md); here's the map:

- **`static`** — always use one named candidate (`router.candidate`, or config's `routers.static.candidate`). An unroutable target is an actionable error, not a silent substitution, unless `allow_fallback: true`.
- **`auto`** (the default) — score every candidate on *quality for this task* versus *how cheap/plentiful it is*, blended by one dial, `cost_quality_tradeoff` (`0` = pure quality, `10` = cheapest survivor — the same 0–10 scale and default `7` as OpenRouter's Auto Router, the one thing `auto` actually borrows from it, since OpenRouter's own scoring function is closed). The blend shifts toward quality automatically as classified difficulty rises, and a hard-complexity carve-out goes pure-quality at genuine extremes.
- **`pareto-code`** — pick a coding-capability tier first (high/medium/low, from `min_coding_score`), then the cheapest available candidate inside it. Blunter and more predictable than `auto`; good for uniformly-hard coding sessions, not for research or writing.
- **`escalation`** — start on the cheapest capable candidate and escalate to a stronger one only when *observed execution* (too many investigation reads, too many tool calls without finishing, repeated tool failures, or a token-ceiling/refusal stop) shows the task was harder than it looked. Trivial tasks finish cheap; "looks easy, turns out hard" tasks earn a mid-turn hand-off to a stronger model that carries a transcript forward, so nothing is redone.

Set the default with the top-level `router:` key, override per-session with the `router.strategy` config option, or steer any client with a prompt directive — see [Prompt routing directives](#prompt-routing-directives).

### Token limits, outages, and failover

The router degrades gracefully when seats run dry or adapters fall over, and it always tells the user what happened.

- **Token/usage limits cordon the agent until reset.** When a downstream reports a rate/usage limit, the router parses the reset time out of the error — Claude Code's `usage limit reached|<epoch>`, Codex's `try again in 2 hours 30 minutes`, `retry-after: 120`, `resets_in_seconds`, epoch and ISO-8601 timestamps are all understood and unit-tested (`src/limits.rs`). The agent is cordoned off from routing until that reset (or `headroom.cordon_default_secs` when no time was reported), and later routing disclosures include the cordon and its remaining time.
- **Auth-aware availability.** A provider you're signed out of still advertises its models from a static manifest — *advertisement is not availability*. The router tracks authentication per **agent** (a provider seat, never an individual model) as authenticated / unauthenticated / unknown, where unknown fails open and only definite evidence changes eligibility. Evidence comes from an optional non-interactive `auth_probe`, the authenticated usage read itself (a clean read proves the seat works, an explicit credential rejection disproves it, a timeout or parse error proves nothing), and runtime ACP rejections. All configured probes run concurrently before every routing decision — startup, `session/new`, pre-classification, pin, dispatch, delegate selection — on a 5-second freshness TTL, so one routing pipeline costs one round of probes, not one per decision. A logged-out agent isn't spawned at startup, is excluded everywhere (the pre-classifier's evaluator pool, `auto`, failover, skill routing, delegates), and its candidates show on the `router.candidate` picker as `available: false` with the auth reason and **no** `resets_at` — signing in fixes it, waiting doesn't. A runtime `Authentication required` pulls the whole agent immediately and fails the turn over to a live peer rather than surfacing the provider's sign-in error. Reactive negatives decay after 15 minutes so an out-of-band login on an agent with no configured probe isn't ignored forever, and a successful ACP `authenticate` clears the state outright.
- **Proactive per-candidate usage cordons.** Beyond reacting to errors, the router can read a provider's own usage state and cordon an *exhausted model* before it's ever tried. Enable per-agent with `usage_source`: `anthropic-oauth` polls the Claude usage API (`GET /api/oauth/usage`); `codex-rollout` polls Codex live over one `codex app-server` JSON-RPC round-trip (`account/rateLimits/read`), sharing that result box-wide through a snapshot cache, and falls back to Codex's own on-disk rollout-file snapshots only if the RPC itself fails (no binary, signed out) — either way, the reactive cordon above is the backstop for staleness or a parse miss. A model-scoped weekly cap at 100% cordons just that candidate; an all-models or session cap cordons the whole agent — but only when the overage/credit pool has no *usable* headroom (Anthropic: overage/spend not exhausted; Codex: `unlimited` credits or a positive `balance` — a bare `has_credits` flag doesn't count). It's **generic** (which models are exhausted is read from the API, never hardcoded), **fails open** (a usage-endpoint hiccup never makes a model unroutable), and self-lifts at the reported reset. **Grok** exposes no usage meter at all, so it needs no `usage_source`: the router instead watches Grok's own subscription **access gate** in its ACP stream and cordons the agent the moment Grok reports the gate closed — the same effect, driven by the only signal Grok gives (no reset time, so it uses `cordon_default_secs`). Cordoned candidates are excluded from `auto`, skipped by failover, and an explicit pin to one is refused with a fallback (disclosed as `router-acp · failover: cordon → claude/sonnet · task … (Weekly Fable limit reached, resets …)`). If *every* candidate is cordoned, the one resetting soonest is used rather than failing the turn. Each candidate's cordon state rides the `router.candidate` picker option (`_meta.router_acp.available/unavailable_reason/resets_at`) at `session/new`, and the full current cordon set also rides every turn's routing metadata as `_meta.router_acp.usage_cordons` so a client that cached the candidate list can refresh availability mid-session. Gate the whole mechanism with `cordon.enabled`.
- **Outage failover.** If the pinned model fails mid-session (process death, connection loss, provider overload) or hits a limit, the router fails the session over to the next best candidate: the failure and its reason are announced in the transcript, the strategy re-ranks the remaining pool, a fresh downstream session is opened (mode re-applied), and the prompt is retried there. Because ACP has no transcript handoff, **conversation context does not transfer** — the disclosure says so explicitly. Failover only happens while the failing turn has produced **no output** (retrying after visible output could duplicate side effects) and never after the client cancelled. Configure with `failover.enabled` / `failover.max_attempts`.
- **Automatic respawn.** A downstream process that died is respawned and re-probed at the next routing decision (subject to `failover.respawn_cooldown_secs`), so a recovered agent rejoins the pool without restarting the router.

### Headroom, quarantine, and availability-aware preference

Without provider usage data, headroom falls back to a per-agent sliding-window counter (default 5 h) of prompts and sessions, normalized against `budget_prompts_5h`. A rate-limit/auth/quota error before the first prompt zeroes an agent's headroom; a candidate that keeps failing to open sessions is quarantined for a cool-off (`headroom.quarantine_*`). Errors after a session is pinned are surfaced, never rerouted.

`agents[].preference` is a static tie-break ("this seat has the bigger plan"). With `availability_preference` (on by default) it tracks reality instead of staying frozen:

- **Free plan headroom feeds the quota term directly** — each candidate's quota uses the lower of local sliding-window headroom and the seat's reported budget, and the static preference bonus fades the same way as free plan runs out.
- **Everything is compared in real dollars, not each seat's own percentage.** A $9k pool at 3% free ($270) and a $3k pool at 3% free ($90) are the same percentage but very different seats, so headroom is a *dollar* figure wherever one is obtainable: overage/credit pools report it directly from the provider API, while included-plan windows don't (on either provider), so the router estimates them from its own metered spend against the reported percentage — saturating at `availability_preference.headroom_scale_dollars` (default $200). Two real shipped bugs came from skipping this: grading two saturated seats by percentage alone tied them at zero even though one had ~$6,600 of paid headroom left and the other ~$100; and using an estimated plan-dollar figure for *free*-plan ranking made a 6%-remaining weekly seat look 100% free next to a seat that actually was 100% free on a smaller cap.
- **Paid overage raises the quality bar instead of being free.** Once a seat is past its included cap but still routable via overage or credits, its utility takes a `cost_aversion × (1 - task complexity)` penalty (default `cost_aversion: 0.1`) — cheap enough to still allow a materially stronger paid model on hard work, but enough to prefer an included-plan fallback for ordinary tasks. A seat with *no* overage headroom left is a cordon, not a surcharge.

Availability comes from the same usage polls that drive proactive cordons, plus **client availability hints** — the `router-acp/availability_hint` extension notification. A client that already watches its own seat usage (Kory Code polls both providers every minute, including a live Codex rate-limit read) can push that view directly:

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

A fresh hint outranks the router's own poll for that agent until it expires (`ttl_secs`, default `availability_preference.hint_ttl_secs`); hints are session-less (send once per connection, not per session), unknown agents and windowless entries are ignored, and `remaining_dollars` is optional — percent-only hints still work on the fraction fallback. Effective preferences show up right in the disclosure line (`+ pref 0.07` / `- pref 0.25 (seat on paid overage)`), and the full known set rides the pin metadata as `_meta.router_acp.availability`.

### Task classification

Every strategy except `static` needs to know what kind of task it's routing and how hard it looks. Two layers do that, and either can run alone:

- **The heuristic classifier** (`classifier.backend: heuristic`, the default) reads the first prompt with deterministic, data-driven rule tables — keyword hits, multi-step structure ("do X and Y, then Z"), mentioned files, and a scan of the project's languages — and produces a task class (`BugFix`, `Research`, `Architecture`, `UiTweak`, …) plus a complexity score from 0 (trivial) to 1 (very hard). Rules live in [`data/classifier.yaml`](data/classifier.yaml) (override with `classifier.rules_file`); updating them is a data edit, not a code change. An optional **local-model backend** (`classifier.backend: local-model`, e.g. `ollama:qwen3:4b`) can replace the heuristic with a small locally-run model (temperature 0, JSON output) — it never touches a paid seat, and falls back to the heuristic on timeout, a parse failure, or an unavailable runtime.
- **The pre-classifier** (`pre_classifier.enabled`, off by default) replaces the heuristic's guess with one cheap, tool-less ACP evaluation on a real model — a preferred cheap seat first (`pre_classifier.evaluator`, default `*haiku*`/`*mini*`/`*flash*` globs), widening to any available model if none of those are eligible. It returns the same task class and complexity, now model-derived instead of keyword-derived, plus the `orchestrate` verdict that decides [auto-orchestration](#auto-orchestration) (`warranted`, `confidence`, `estimated_parts`) and any host-registered `dimensions` (`pre_classifier.dimensions`) — all in the same call, so a host like Kory Code can add its own opaque decision (e.g. "does this need UI mockup-first planning?") without a second round-trip. Once enabled, classification becomes required infrastructure for the turn rather than a nice-to-have: a failing evaluator is cordoned and the router fails over to the next eligible one, and only a *total* miss — no evaluator anywhere could classify — hard-fails the turn instead of silently dropping back to the heuristic. `stall_timeout_ms` (default 90s) bounds a *silent* evaluator, not total run time, so a slow-but-streaming model is still allowed to finish. Every pre-class decision is disclosed as `router-acp · pre-class …` (or suppressed with `disclose: false`).

### Quality data

Per-class quality scores, coding tiers/percentiles, and context windows live in a versioned YAML table shipped with the binary ([`data/scores.yaml`](data/scores.yaml)), overridable wholesale with `score_table`. Entries key off a candidate pattern (`agent/model`, `*` wildcards, first match wins — keep specific patterns like `*mini*` above broad ones like `*gpt-5*`). Quality sits on a benchmark-calibrated 0.5–3.5 scale (about 1 minimal, 2 standard, 3 frontier), mapped linearly to 0–1 before combining it with quota and preference, so a one-point quality difference always means the same thing to the router. Capability above what a task can actually use earns no extra utility — which is what stops routine work from spending frontier headroom it doesn't need — and a task-class override is only emitted when at least two relevant benchmarks agree on a difference of 0.15 or more. The classifier's own rule tables work the same way ([`data/classifier.yaml`](data/classifier.yaml), `classifier.rules_file`). The reproducible update procedure — discovery, scoring, validation — is documented in [`docs/model-updater.md`](docs/model-updater.md).

## Per-request LLM routing

ACP routing normally sees one `session/prompt`; a coding agent may make hundreds of provider calls while executing it. The optional `llm_proxy` interposes those calls. router-acp binds a loopback-only HTTP listener before spawning adapters, replaces each configured adapter's inference base URL, and forwards the original method, path, application headers (including authorization), body, and streamed response. It requests an identity-encoded upstream response so usage can be accounted while streaming; only an inference request's top-level `model` field may change.

The request policy is deliberately stateful:

- begin on the ACP session's pinned candidate;
- demote to the cheapest routeable model from the same agent after a sustained streak of successful routine tool results (reads, searches, mechanical edits, status/CI checks) — on the Anthropic wire this is also gated by cache economics: prompt caching is per-model, so a demotion pays the target's cache-*write* rate to re-prime the conversation, and only fires once the streak is long enough that the saved turns beat that one-time cost (`cache_reprime_break_even`, derived from each model's configured cache rates); below that, the router holds the warm pin instead of thrashing;
- escalate immediately to that agent's highest-quality compatible model at or below the session pin's cost rank after a tool failure, repeated unchanged test output, refusal, HTTP failure, or token/context ceiling; per-request routing never spends above the pin;
- expire difficulty verdicts by request count and wall time; and
- enforce a minimum dwell before ordinary switches to bound cold-cache churn. Difficulty and explicit automation hints bypass dwell.

Request signals are extracted structurally and deterministically from the live tool payload; per-request routing does not call an LLM to classify requests. Demotion is rejected when the estimated request plus requested output exceeds `context_window_fraction` of the target's context window. An alternate whose window is unknown is never selected. Cordons and quarantines also apply. `api_model` maps an ACP selector alias such as `opus[1m]` to the exact provider API model id used for rewrites.

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

This routes among models accepted by the same adapter/upstream/auth seat; it does not translate wire formats or credentials across providers. Future agents only need a base-URL environment variable, an explicit upstream, and either the `anthropic` or `openai` wire setting.

Attribution is exact for `spawn-config` processes and for a process with one active ACP prompt. If a shared `config-option` process serves concurrent prompts, an adapter session marker (`prompt_cache_key`, `session_id`, or equivalent) resolves the owner; otherwise the request passes through unchanged rather than risking a cross-session rewrite. Invalid JSON, non-inference endpoints, ambiguous traffic, or listener bind failure likewise degrade to transparent ACP-turn routing.

Relay-generated automation can bypass the routine streak by attaching this to the ACP prompt:

```json
{"_meta":{"router_acp":{"request_hint":"ci-poll"}}}
```

Accepted hints are `ci-poll`, `ship-nudge`, and `automation`.

Every attributed call is written to `llm_requests`, including pinned and selected model, reason/event, endpoint, latency, HTTP status, exact usage, cache read/write tokens, and API-equivalent cost. `tool_calls` tracks tool lifecycle and the `active_tool_calls` view exposes the model currently serving in-flight tools:

```sql
SELECT router_session_id, tool_call_id, title, model, started_at
FROM active_tool_calls;

SELECT model, routing_event, count(*) AS requests, sum(cost_usd) AS cost
FROM llm_requests
GROUP BY model, routing_event;
```

## Prompt routing directives

Recipes and scripts (or any client that can't set ACP session config options, like the goose CLI) can steer routing in-band with a `[router: …]` tag anywhere in the prompt — on its own line, after a `<turn-context>` preamble, or inline with the task on the same line (the tag is bracket-matched, so nested `[1m]` model ids and trailing text are handled). A bare directive with no task is fine too:

```
[router: candidate=claude/claude-fable-5[1m]]      # pin this session
[router: prefer=codex/gpt-5.5]                      # soft preference (falls back)
[router: strategy=pareto-code]                      # set session strategy
[router: exclude=claude|codex/gpt-5.4-mini]         # ban lineages/candidates
[router: switch=claude/opus[1m]]                    # change models mid-session
[router: switch=claude/opus[1m]] now what?          # …and prompt it in one line
```

The router strips the tag (downstream models never see it), fails loudly on invalid directives, and records applied directives in the disclosure and state file. `candidate`, `prefer`, `strategy`, and `exclude` apply **pre-pin only** (post-pin: stripped + visible "ignored" note); `exclude` persists for the session, including failover re-pins. `prefer` is a *soft* pin — the named candidate goes to the front of the ranked chain if it is eligible, otherwise routing falls back to the strategy's normal winner rather than erroring.

`switch` is the exception that works **mid-session**: it hands the live conversation to a different model (see below). These directives let any CLI client steer routing even though it can't set ACP session config options.

## Switching models mid-session

A pinned session isn't stuck. Because ACP can't transfer a live transcript between agents, the router bridges with a **summary**: it asks the current model to write a handoff (task, decisions, files touched, what's left), opens a fresh downstream session on the target, prepends that summary to your next prompt, re-pins, and closes the old session — the summary turn is captured internally, never shown, and the switch is disclosed like any other routing decision. **If the old model can't summarize** (offline, rate-limited, crashed, refuses), the router falls back to a truncated transcript reconstructed from its own SQLite `session_log` and seeds that instead — so a switch, including an auto-upgrade fired *because* the model is failing, still completes without the dead model's help.

Three triggers:

- **Explicit** — `[router: switch=agent/model]` anywhere in a prompt, or the **`model:` shorthand**: start a message with a model reference and a colon.

  ```
  opus: take over and finish the refactor
  codex/gpt-5.5: review this
  gpt-5.5:                      # bare — switches, then the new model greets you
  ```

  The reference can be a full `agent/model` id, a bare model id, a family/prefix, or a suffix-less id (`claude/opus` → `claude/opus[1m]`); it resolves to the best eligible matching candidate (highest quality on ambiguity), and a token that names no candidate is left alone as ordinary prose. Pre-pin, the same syntax steers the initial pin instead of switching.
- **Auto-upgrade** — after each turn the router scores the session's *confidence* (the pinned model's quality for the task class minus a *struggle* score that rises on token-ceiling hits, refusals, and repeated in-turn tool failures). Below `auto_upgrade.confidence_threshold` it upgrades to the best strictly-more-capable eligible candidate on the next prompt. `auto_upgrade.enabled: false` disables this specifically; explicit `switch=` still works.
- **Skill routing** — `skill_routing` forces a named skill onto a class of candidates: when a prompt invokes a configured skill (as `/name` or a standalone token) and the pin isn't already in that skill's `candidates` or `also_acceptable` globs, the session switches to the best `candidates` match (pre-pin, it steers the initial routing instead). `also_acceptable` models are fine to stay pinned on but are never a switch *target* — the split is what stops a session already on something better than `candidates` from being force-downgraded onto it. Re-invoking the skill re-arms the elevation, and an elevated skill pin expires like any other elevation (`demotion.after_quiet_turns`) once the run goes quiet, stepping back down within that skill's own `candidates`.

See [`ROUTERS.md`](ROUTERS.md) for `selection` (`best-quality` vs `first-match`), `terse_handoff`, and the full demotion model.

Read one session's **full** interaction log — including tool calls and their detail, which `sessions --session` omits — with:

```sh
router-acp transcript --state ~/.local/state/router-acp/sessions.db --session rtr-…
```

This takes the state DB path directly rather than a `--config`, so it runs standalone: a model that has just been handed a session (a `terse_handoff` briefing embeds this command already resolved to the running binary and live state file) can read what its briefing omitted, without a router config and without `sqlite3` installed.

## In-session delegation

The pinned primary agent gets a router-provided MCP tool, `delegate_task`, for small self-contained subtasks — mechanical edits, isolated bug fixes, focused research. Each delegated subtask runs in an **ephemeral downstream session on a strictly lower-`cost_rank` candidate** (preferring a same-agent sibling before falling back cross-lineage — so a Sol primary delegates ordinary work to Terra/Luna when they're eligible), returns the sub-agent's output as the tool result, and forwards the sub-session's permission/fs/terminal callbacks to the original client under the parent session id — permission UX stays intact without interleaving sub-agent transcript streaming into the parent's. Delegates have no upstream client of their own, so the router applies each candidate's `auto` `mode_map` entry itself at session creation (for example Claude `bypassPermissions` or Codex `agent-full-access`), and each delegate's full task, final response, streamed progress, and tool activity land in its `session_log` row for UIs.

One routing subtlety: a delegated subtask is usually a long, fully-specified brief, which the classifier would read as *maximum* complexity — routing every subtask to the priciest candidate. `delegation.complexity_cap` (default `0.6`) caps a delegate's classified complexity so cost-aware routing still applies to spec'd work (`1.0` disables it).

Because ACP `mcpServers` entries must be concrete transports, the tool is a real stdio helper: the router appends `router-acp mcp-delegate --socket <unix-socket> --token <session-token>` to the pinned session's MCP servers, bridging its stdio back to the parent router; the random per-session token maps the connection to the owning session. The delegate server is injected only when delegation is enabled, more than one candidate is routeable, and a cheaper candidate actually exists for the parent — never into delegated sessions themselves (depth is capped at 1).

### Host-owned capability MCP catalogs

An ACP host registers concrete named MCP bundles with `router-acp/delegate_mcp_catalogs`; config maps those catalog names to opaque capabilities. The host's pre-classifier extension defines when a first prompt needs those capabilities. Before opening the primary downstream session, the router resolves the requirement and attaches the matching registered catalog. A later bounded `delegate_task` instead asks for `required_capabilities`; the router resolves and attaches them only to that delegate. Unknown capabilities, missing catalog registrations, and incomplete coverage fail closed — the router never defines capability meanings, endpoints, or credentials. When a required catalog is selected, its host-supplied entry replaces any same-named server forwarded by the client; the registration is the authoritative definition for that capability.

Some clients never hold a live post-`session/new` connection into the session they created — a client that spawns the router as a subprocess only to relay its stdout can never send that notification. For those, set `ROUTER_ACP_MCP_CATALOGS` to the same `catalogs` JSON shape (`{"<catalog>": [<McpServer>, ...]}`) before the router process starts; it seeds `delegate_mcp_catalogs` once at session creation. It's an additive fallback only: absent or malformed content fails open to no catalogs, and a later live notification still overwrites the seed as usual.

`delegation.inject_prompt: true` also prepends a one-shot directive to each ordinary downstream session that actually received these tools, asking the parent to delegate only bounded, independent work whose briefing/verification overhead is worthwhile and to verify and integrate the result itself. A model switch creates a fresh downstream session, so the router re-injects there when a cheaper worker remains available; orchestration does not receive this ordinary directive because its stronger protocol already governs delegation. The opt-in defaults to `false`.

Concurrency is bounded by `delegation.max_concurrent`; `session/cancel` on the parent cancels the primary prompt and every active sub-session.

For **review → fix → re-review** loops, a delegated sub-session can be kept alive: `delegate_task` with `keep_open: true` returns a `delegate_id`; send it more instructions with `delegate_followup` (context preserved), and release it with `delegate_close`. These are what let the orchestrator (below) iterate on a subtask without re-briefing a fresh session each round.

For **parallel** subtasks there is a background mode: MCP clients execute tool calls one at a time, so N plain `delegate_task` calls run the subtasks serially no matter what the router allows. `delegate_task` with `background: true` instead returns a `b-…` job id immediately and runs the subtask on its own task; `delegate_await` collects results — it waits up to `timeout_seconds` (default 600, clamped to 5–1500) for the given ids (default: all pending), returns every finished job's output exactly once, and lists the ones still running so the caller polls with short, idle-timeout-safe calls. `background` composes with `keep_open` (the collected result carries the `delegate_id`), and `delegation.max_concurrent` still bounds how many jobs execute at once.

### Managed background terminals

When the upstream ACP client advertises `terminal: true`, the router exposes a
`background_start` MCP tool even when delegation is disabled or no cheaper
candidate exists. The tool forwards its executable, arguments, optional cwd,
and retained-output limit through ACP `terminal/create` and returns immediately.
The router also instructs downstream models to use it instead of an adapter's
private `run_in_background` mode, so the host client can persist the process,
show its output and status, and cancel it independently of foreground turns.

## Auto-orchestration

When a prompt reads as a **multi-part task list**, the router can run an entire plan → parallel-delegate → cross-lineage-review → submit pipeline itself, in-process, instead of answering the list in one turn. Turn it on with `orchestration.enabled` (off by default); it steers (pre-pin) or switches (mid-session) the session onto a **planner** frontier model and injects an orchestration protocol instructing that model to:

1. **Plan** — restate the task as success criteria, split it into file-disjoint, self-contained subtasks, and state a confidence (0.0–1.0) that the plan will satisfy the criteria.
2. **Delegate — in parallel** — dispatch independent subtasks via `delegate_task background: true` + `delegate_await`, each routed per-complexity in its own sub-session.
3. **Review** — after every implementation subtask is collected, delegate an independent review to a candidate of a **different lineage** than the planner (`reviewer` globs), handing it the original task verbatim. Skipped, with a note, when no cross-lineage reviewer is available or the planner's stated confidence exceeds `review_confidence` — except under `submit: merge`, where the review is never skipped.
4. **Adjudicate** — fix blocking issues and re-review, bounded by `max_fix_rounds`.
5. **Submit** — per the `submit` gate (`never | branch | pr | merge`); a merge is only permitted after an approving review.

The one thing this needs beyond ordinary delegation is **peer delegation**: an orchestrating session may delegate to *same-* or *higher*-tier candidates, not just strictly-cheaper ones, so the cross-lineage reviewer is actually reachable. Everything else — the decomposition, the review, the submission gate — is the planner model following the injected protocol with its own tools plus `delegate_task`/`delegate_followup`.

**What decides a prompt is a task list** is one of two paths:

- **The pre-classifier** (recommended — see [Task classification](#task-classification)), when `pre_classifier.enabled`: its `orchestrate` verdict (`warranted`, `confidence`, `estimated_parts`) decides, gated on `confidence >= pre_classifier.orchestrate_min_confidence` (default `0.65`). One evaluation covers this and any host dimensions in the same call.
- **The legacy detector**, when the pre-classifier is off: `src/tasklist.rs` recognizes markdown numbers (`1. …`), markdown bullets (`- …`), inline enumeration (`… (1) … (2) …`), and ordered prose ("first … then … finally …"), triggering once a prompt reaches `orchestration.min_items` parts.

Either way, orchestration fires on **any** prompt (fresh or mid-session), **takes precedence over `skill_routing`** (a multi-part list orchestrates even if it names a skill like `ship-pr` — the planner decides when to invoke that skill, and end-of-work skills like shipping run *after* the work is done and reviewed, never up front), and is **suppressed** by an explicit `[router: …]` directive or `model:` shorthand, and when the "list" is actually you answering the model's own enumerated questions (it asked "Open decisions: (1)… (2)…" and you replied with a matching list). Each trigger is disclosed (`router-acp · orchestrating a N-part task on …`).

Two related prompt features:

- **Force it** — start a message with `orchestrate:` (or `orchestrator:`) to run the pipeline on any task, list or not, overriding every auto-detection gate including `orchestration.enabled` itself.
- **Ticket context** (`ticket_context`, pluggable) — when a prompt references a ticket id (e.g. "Fix ABC-1234"), the router runs the configured command (`linear issue view $TICKET`, `jira …`, `gh issue view …` — any CLI that prints the ticket) and prepends the ticket's content to the prompt **before** classification and orchestration detection, so a bare ticket mention routes on the ticket's real scope, and a ticket whose body is a work list orchestrates — with the planner delegating its parts. Fail-open (a bad fetch never blocks the turn), once per ticket per session, fetches cached ~5 min.

> **Caveat — the planner must actually use `delegate_task`.** Sub-session routing, the cross-lineage review, and the `parent_session_id`/`run_label` rows all depend on it. Some adapters ship a *built-in* sub-agent tool (Claude's `Task`) that spawns same-lineage sub-agents *inside* the adapter — invisible to the router. The injected protocol explicitly forbids that tool and mandates `delegate_task` with a concrete different-lineage reviewer id, but the router cannot remove the adapter's own tool; a model that ignores the instruction degrades to same-lineage, unobservable orchestration. If you see no delegate rows in the state DB after an orchestrated run, the planner used its native tool — the router detects this, warns inline (`orchestration degraded: …`), and records it (`native_subagent_calls`).

Evaluate orchestrated runs — real per-run cost (from the adapter's own `usage_update.cost`), delegate split, cross-lineage-review presence, and degraded% — with:

```sh
router-acp report --config router.yaml
```

See [`ORCHESTRATION.md`](ORCHESTRATION.md) for the full pipeline, the router-mechanism-vs-instruction table, and caveats.

## Configuration reference

See [`examples/router-full.yaml`](examples/router-full.yaml) for a complete annotated example, or [`examples/router-preferred.yaml`](examples/router-preferred.yaml) for a real four-agent (Claude/Codex/Grok/Kimi) starting config.

### Routing & classification

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
| `pre_classifier.enabled` | `false` | Replace the heuristic with one authoritative LLM evaluation per session (task class/complexity, `orchestrate`, host `dimensions`). Once on, classification is mandatory infrastructure: a total miss across every eligible evaluator hard-fails the turn instead of falling back to the heuristic. |
| `pre_classifier.evaluator` | `*haiku*`, `*mini*`, `*flash*` | Preferred cheap-seat globs for the evaluator, in order; widens to any available model if none are eligible. |
| `pre_classifier.stall_timeout_ms` | `90000` | Max time the evaluator may go with **no streamed progress** before it's treated as a failure and the router tries the next evaluator. Bounds silence, not total run time, so a slow-but-streaming model is still allowed to finish. (`timeout_ms` is a deprecated, ignored field kept only so old configs still load.) |
| `pre_classifier.disclose` | `true` | Emit `router-acp · pre-class …` disclosure lines. |
| `pre_classifier.orchestrate_min_confidence` | `0.65` | Minimum `orchestrate.confidence` to act on auto-orchestration (see [Auto-orchestration](#auto-orchestration)). |
| `pre_classifier.dimensions[]` | `[]` | Host-registered extra decisions returned by the same evaluation: `id`, `description` (evaluator instruction), `min_confidence` (default `0.70`), `act_when` (`{warranted: true}` or `{field, equals}`), `inject_prompt` (text injected into the turn when the dimension acts). |
| `pre_classifier.evaluator_cwd` | unset | Working directory for the evaluator's throwaway session, replacing the classified session's own cwd (and dropping `additional_directories`) — avoids paying a full project-context load to classify a prompt the project has no bearing on. Unset keeps classifying from the session's own cwd. |

### Availability, cordons & headroom

| Key | Default | Meaning |
| --- | --- | --- |
| `headroom.window_secs` | `18000` | Sliding-window length (5 h). |
| `headroom.quarantine_failures` | `3` | Pre-prompt failures in the window before quarantine. |
| `headroom.quarantine_cooloff_secs` | `600` | Quarantine cool-off. |
| `headroom.cordon_default_secs` | `900` | Cordon length for a rate/usage-limited agent when the error carries no parseable reset time. |
| `cordon.enabled` | `true` | Master switch for proactive usage-cap cordons (inert unless an agent has a `usage_source`). Also gates the usage polling that feeds `availability_preference`. |
| `cordon.poll_secs` | `300` | Usage poll interval / cache TTL. |
| `cordon.min_refresh_secs` | `60` | Box-wide floor between upstream usage-endpoint fetches, enforced through a shared snapshot cache — every `router-acp` process on the box together makes at most one fetch per interval. |
| `availability_preference.enabled` | `true` | Plan-aware routing: effective quota headroom is the lower of local headroom and the candidate's seat budget (real dollars where estimable, else the reported plan fraction); effective preference scales the same way. Paid overage also incurs a difficulty-scaled surcharge. Off = local headroom plus static preference, hints ignored. |
| `availability_preference.cost_aversion` | `0.1` | Paid-overage surcharge coefficient. Utility subtracts `cost_aversion × (1 - task complexity)`; `0` means the user is perfectly willing to pay. Accepts the legacy alias `overage_penalty`. |
| `availability_preference.hint_ttl_secs` | `600` | How long a client `router-acp/availability_hint` outranks the router's own poll (per agent). |
| `availability_preference.headroom_scale_dollars` | `200` | Remaining overage/credit budget, in dollars, at/above which that pool reads fully free. Free included-plan ranking always uses the plan *fraction*, never a dollar estimate (see [Headroom, quarantine, and availability-aware preference](#headroom-quarantine-and-availability-aware-preference)). |
| `availability_preference.overage_budget_weight` | `0.2` | Weight on the overage-dollar term while free plan still remains (`seat_budget = plan_headroom + overage_budget_weight × overage_signal`); used at full scale once plan is empty. |
| `agents[].usage_source` | – | Optional provider usage source for proactive cordons. `{ type: anthropic-oauth }` reads the Claude CLI OAuth token (`~/.claude/.credentials.json` or the macOS Keychain) and polls `GET /api/oauth/usage`. `{ type: codex-rollout }` polls Codex live over one `codex app-server` JSON-RPC round-trip (`account/rateLimits/read`, shared box-wide via a snapshot cache), falling back to Codex's own on-disk rollout-file snapshots only if that RPC fails; credits only bypass a saturated window when actually usable (`unlimited` or positive `balance`). |
| `agents[].auth_probe` | – | Optional non-interactive login check for this agent's provider seat, e.g. `{ command: claude, args: ["auth", "status"] }`. Run concurrently with every other probe before selection. Output matching `unauthenticated_patterns` (case-insensitive substring) means signed out; otherwise exit zero means signed in; a non-zero exit that matches nothing, a spawn failure, or a timeout is **unknown** and fails open. Omit it for providers with no reliable non-interactive status command — they stay fail-open and reactive. |
| `agents[].auth_probe.timeout_ms` | `2000` | Probe timeout. Exceeding it is unknown, not a failure. |
| `agents[].auth_probe.unauthenticated_patterns` | `not logged in`, `not signed in`, `authentication required`, `sign in`, `log in`, `login required` | Substrings that make the probe's output definite negative evidence. Set explicitly when a provider's logged-*in* output also mentions signing in. |

### Failover & mid-session switching

| Key | Default | Meaning |
| --- | --- | --- |
| `failover.enabled` | `true` | Fail a pinned session over to the next best candidate on limit/outage (only before any output streamed this turn). |
| `failover.respawn_cooldown_secs` | `30` | Minimum interval between respawn attempts of a dead downstream process. |
| `failover.max_attempts` | `3` | Candidates tried per prompt (initial + failovers). |
| `auto_upgrade.enabled` | `true` | Auto-switch a pinned session up to a more capable model when confidence drops. `false` disables it (explicit `[router: switch=…]` still works). |
| `auto_upgrade.confidence_threshold` | `0.55` | Upgrade when confidence (fraction of task capability demand met − struggle) falls below this. Higher = more eager; `0` ≈ never. |
| `demotion.after_quiet_turns` | `0` (disabled) | Steps an *elevated* pin (from a skill route, an escalation, or an auto-upgrade — never an explicit user pick) back down to the strongest cheaper candidate after this many consecutive turns with no struggle signal. `0` keeps an elevated pin for the rest of the session. |
| `skill_routing[]` | `[]` | Rules forcing a skill onto a model class: `pattern` (skill name, matched as `/name` or a standalone token) → `candidates` (switch-target globs in preference order), plus optional `also_acceptable` (globs that are fine to stay pinned on but are never switch targets), `selection` (`best-quality` default, or `first-match`), and `terse_handoff` (a three-line handoff instead of a full summary). The pin is left alone if it matches either list; a switch only ever targets `candidates`. Mid-session it switches; pre-pin it steers routing. |
| `ticket_context[]` | `[]` | Ticket-loading rules: `prefix` (e.g. `ABC-`, matched at a word start + digits) → `command` (argv, `$TICKET` substituted, run without a shell) whose stdout is prepended to the prompt before routing. Pluggable across ticketing systems; fail-open. |

### Delegation & orchestration

| Key | Default | Meaning |
| --- | --- | --- |
| `delegation.enabled` | `true` | Offer `delegate_task` to pinned sessions. |
| `delegation.inject_prompt` | `false` | When the tools are actually attached to an ordinary primary session, inject one scoped instruction to use them for suitable bounded work. Re-injected after a model switch; suppressed for orchestration. |
| `delegation.mcp_catalogs` | `[]` | Host-defined catalog-to-capability policy for first-prompt and bounded-delegate MCP attachment. |
| `delegation.max_concurrent` | `3` | Concurrent delegated sub-sessions. |
| `delegation.socket_path` | temp dir | Unix socket the delegate helper connects back on. |
| `delegation.complexity_cap` | `0.6` | Ceiling on a delegated subtask's classified complexity, so a long, fully-specified brief doesn't misread as maximum difficulty and route every subtask to the priciest candidate. `1.0` disables it. |
| `ROUTER_ACP_MCP_CATALOGS` (env) | unset | JSON seed for `delegate_mcp_catalogs` when no client connection can ever send the `router-acp/delegate_mcp_catalogs` notification. Fails open on absent/malformed content. |
| `orchestration.enabled` | `false` | Auto-orchestrate multi-part task lists: steer/switch to a planner model and inject the decompose→delegate→review→submit protocol. |
| `orchestration.min_items` | `2` | Smallest detected list size treated as a multi-part task (legacy detector only; the pre-classifier decides via `orchestrate_min_confidence` instead). |
| `orchestration.planner[]` | frontier globs | Planner/orchestrator candidate globs — they define the pool; the pick is by preference-adjusted quality (`quality + agents[].preference`), glob order breaking ties. |
| `orchestration.reviewer[]` | frontier globs | Preferred cross-lineage reviewer globs handed to the orchestrator (it should pick a different lineage than the planner). |
| `orchestration.submit` | `branch` | Submission gate given to the orchestrator: `never \| branch \| pr \| merge` (a merge is only permitted after the review approves). |
| `orchestration.max_fix_rounds` | `2` | Max review → fix → re-review rounds. |
| `orchestration.review_confidence` | `0.8` | Planner self-confidence bar (0.0–1.0) for skipping the review pass: strictly above it, the review is skipped with a note. Ignored under `submit: merge` (a merge always requires an approving review). |

### Per-request LLM proxy

| Key | Default | Meaning |
| --- | --- | --- |
| `llm_proxy.enabled` | `false` | Interpose configured adapters and route each attributed provider inference request. Bind/config failures leave ACP-turn routing active. |
| `llm_proxy.listen` | `127.0.0.1:0` | Loopback listener; port `0` selects a free port. Non-loopback addresses are rejected because provider credentials pass through it. |
| `llm_proxy.routine_streak` | `3` | Consecutive successful routine tool-result requests required before demotion (also gated by the cache-reprime break-even on the Anthropic wire — see [Per-request LLM routing](#per-request-llm-routing)). |
| `llm_proxy.minimum_dwell_requests` | `12` | Requests a selected model serves before an ordinary switch; difficulty and automation bypass it. |
| `llm_proxy.verdict_ttl_requests` / `verdict_ttl_secs` | `6` / `900` | Request-count and wall-clock expiry for a difficulty escalation verdict; `0` disables that expiry dimension. |
| `llm_proxy.context_window_fraction` | `0.9` | Maximum fraction of an alternate model's known context window that an estimated request may occupy. |
| `llm_proxy.max_request_bytes` / `max_capture_bytes` | `32 MiB` / `4 MiB` | Request buffering limit and head/tail response capture limit. Responses still stream in full. |
| `agents[].llm_proxy` | – | Adapter interposition: `protocol` (`anthropic` or `openai`), `base_url_env`, and the real `upstream_base_url`. |
| `agents[].llm_proxy.codex_chatgpt_provider` | `false` | For an OpenAI-protocol Codex agent, install a custom HTTP Responses provider so ChatGPT-authenticated Codex traffic traverses the proxy instead of bypassing it over WebSocket. |
| (Anthropic-protocol `agents[].llm_proxy`) | always on | Claude Code disables MCP tool search when `ANTHROPIC_BASE_URL` is not a first-party Anthropic host, eagerly loading every tool schema instead. This proxy forwards `tool_reference` blocks untouched, so `ENABLE_TOOL_SEARCH=true` is set on every anthropic-protocol target unless the process env already sets it explicitly. |

### Strategy-specific knobs

| Key | Default | Meaning |
| --- | --- | --- |
| `routers.static.candidate` | – | `agent/model` default for `static`. |
| `routers.static.allow_fallback` | `false` | Fall back in config order if the static candidate is unavailable. |
| `routers.auto.cost_quality_tradeoff` | `7` | `0` = pure quality, `10` = cheapest survivor; OpenRouter-parity scale and default. |
| `routers.auto.complexity_floor` | `0.7` | Above this classified complexity, candidates below the 75th-percentile quality for the task class are dropped before scoring. |
| `routers.auto.complexity_scales_tradeoff` | `true` | Scale the tradeoff by `1 − complexity`: cost matters for trivial prompts, quality dominates hard ones. |
| `routers.auto.min_cost_weight` | `0.15` | Floor on the cost weight after complexity scaling, so a complexity-1.0 classification can't zero the cost term entirely and send every prompt to the priciest candidate. `0` restores that legacy behavior. |
| `routers.auto.apex_complexity` | `0.9` | At/above this classified complexity, ranking goes pure quality (tradeoff forced to 0, demand cap bypassed) regardless of the configured tradeoff. |
| `routers.auto.allowed_candidates` | `["*"]` | Glob allowlist, e.g. `["claude/*"]`. |
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

### Agents & models

| Key | Default | Meaning |
| --- | --- | --- |
| `agents[].name` | – | Unique, no `/`. Candidate ids are `name/model-id`. |
| `agents[].command` | – | `type: stdio` command/args/env for the adapter. `${VAR}` in the whole file interpolates from the environment (unknown names are left intact). |
| `agents[].model_selection.type` | – | `config-option`: one process per agent; the router discovers the `category: model` select option at probe time and applies `session/set_config_option` per session. `spawn-config`: one process per model, built from `process_template` (with `${model_id}` substitution); no universal `-m` flag is assumed. |
| `agents[].budget_prompts_5h` | `400` | Headroom normalization budget. |
| `agents[].models[]` | – | `id` (must exactly match the downstream selector's value for `config-option`), optional `display_name`, `api_model` (provider model id for proxy rewrites; defaults to `id`), `cost_rank` (1 = cheapest/least scarce), and optional `pricing`. |
| `agents[].models[].pricing` | – | API-equivalent USD per Mtok: `input_per_mtok`, `output_per_mtok`, and optional `cache_read_per_mtok`/`cache_write_per_mtok` (default to `0.1×`/`1.25×` the input rate when unset). Prices every attributed request and drives the LLM-proxy cache-reprime gate. |
| `agents[].mode_map` | `{}` | Translate client-requested session mode ids to this agent's ids (e.g. goose's `auto` -> claude's `bypassPermissions`). When `pre_classifier.enabled`, an evaluator that advertises session modes requires an explicit tool-safe `preclass` entry whose target it advertises; it is applied before the classifier prompt. Mode-less adapters run the evaluator without `session/set_mode`. |
| `agents[].lineage` | agent name | Model-company tag (e.g. `anthropic`, `openai`). Orchestration's cross-lineage review requires the reviewer's lineage to differ from the planner's — the intent is a different **company** with different failure modes — so two agents backed by the same vendor should declare the same `lineage`. |
| `agents[].preference` | `0` | Additive utility tie-break for this agent (`auto`) and within-tier tie-break (`pareto-code`). Keep small, e.g. `0.05`. Scaled dynamically by seat availability unless `availability_preference.enabled: false`. |

## Session modes

Some clients (goose) send `session/set_mode` right after `session/new`, before the first prompt exists. The router accepts and **defers** the mode, applying it to the downstream session at pin time: the agent's `mode_map` translation wins, then an exact id match against the modes the downstream advertised; with no equivalent the mode is skipped with a warning and the downstream stays in its default mode. Post-pin mode changes relay with the same translation, and are likewise lenient when no equivalent exists. Delegated sessions explicitly apply the candidate's `auto` mapping because there is no separate upstream client to send them a mode request.

Pre-classifier sessions are intentionally different: an adapter that advertises session modes requires an explicit `mode_map.preclass` target (for example `preclass: plan` for Claude or `preclass: read-only` for Codex), and the target must be advertised by that adapter. The router never reuses `auto` or guesses `chat`; an adapter with modes but without that safe mapping is skipped before it receives the user or classifier prompt. An adapter that advertises no modes (such as Grok) has no mode gate to arm and runs without `session/set_mode`. Any evaluator tool call or downstream callback is cancelled and rejected.

## First-run authentication

Adapters guard subscription seats behind their own auth. `initialize` is two-phase:

1. The router spawns and initializes every configured adapter, then probes `session/new` on each. Agents whose probe returns `auth_required` are marked **auth-pending**: their candidates are declared but not routeable yet, and the router still initializes so your client can authenticate.
2. Downstream auth methods are advertised **namespaced** as `<agent>/<methodId>` (e.g. `claude/claude-login`). Pick one in your client; the router relays `authenticate` to that agent and re-runs probe verification. As soon as one candidate verifies, `session/new` works.

While only auth-pending candidates exist, `session/new` returns `auth_required`. If zero candidates are routeable and none are auth-pending, `initialize` fails with a configuration error.

Tip: adapters usually persist auth in your home directory, so it is often easiest to log in once with the vendor CLI (`claude`, `codex login`) before starting the router.

## Session lifecycle

`session/list`, `session/load`, `session/resume`, `session/delete`, and `session/close` are implemented end-to-end and advertised **only when at least one downstream supports them**:

- `list` merges downstream lists, rewriting downstream ids to router ids via the state file (sessions the router can't route back are omitted).
- `load`/`resume` require a known router session id in the state file, route to the owning downstream only, rehydrate the pin before any prompt, and relay replayed transcript updates under the router id.
- `delete` routes to the owning downstream, then removes router state.
- `close` closes the live downstream session (state-file entries survive so `load`/`resume` keep working when the downstream persists sessions).

## Troubleshooting

**`initialize` fails with "zero routeable candidates".**
Every agent failed spawn/probe/model validation. Run `router-acp check-config --config …`, then `RUST_LOG=router_acp=debug` and read the per-target warnings (spawn failure, probe timeout, missing model option).

**"declared model not offered by downstream model selector".**
Your `models[].id` doesn't exactly match the adapter's model-selector values. The candidate is removed from the pool at startup (missing models are never discovered later at prompt time). Debug logging prints the values the adapter actually offers; copy the exact id.

**"model verification failed … silent no-op".**
The adapter accepted `session/set_config_option` but still reports another model as current. The router refuses to pin (the response is authoritative; no `config_option_update` notification is awaited). Usually this means the model id exists in the selector but the seat/plan doesn't allow it.

**"target … advertises no `category: model` select config option".**
The agent is configured with `model_selection: config-option` but the adapter exposes no model selector. Switch to `spawn-config` with a per-model process template, or declare exactly the adapter's default model.

**Notice: "routing directive ignored (session already pinned)".**
`router.strategy`/`router.candidate`/`prefer`/`exclude` only shape the *initial* routing decision, so they're ignored after the first prompt. To change models mid-session use `[router: switch=agent/model]`, which summarizes the work and re-pins onto a fresh downstream — or open a new session.

**Candidate keeps being skipped.**
It may be quarantined after repeated pre-prompt failures (see `headroom.quarantine_*`), or its agent's headroom hit 0 after a rate-limit error; both recover with time. Check the disclosure line/`_meta.router_acp` for the note explaining tier or fallback decisions.

**Delegation tool never appears.**
The tool is only injected when delegation is enabled, more than one candidate is routeable, and a strictly cheaper candidate exists for the pinned parent.

**Measure ordinary delegation adoption.**
Each model-facing injection is recorded as a `delegation_directive` session-log event; actual use remains authoritative child rows in `sessions`. Summarize prompted sessions, adoption, native-subagent bypasses, and parent/delegate cost:

```sh
router-acp delegation-report --config router.yaml
```

## Integration matrix

Automated protocol tests cover a scripted mock downstream (see [Development](#development)). Manual matrix for real seats:

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

Contributor/agent context (architecture, SDK pitfalls, test infrastructure, invariants) lives in [`AGENTS.md`](AGENTS.md).

```sh
cargo test                 # unit + protocol tests (spawns mock-agent binaries)
cargo run --bin mock-agent # scripted ACP downstream used by the tests
```

The protocol tests drive the full router in-process against real `mock-agent` subprocesses: lazy pinning, model verification (including silent no-op detection), namespaced auth, capability filtering, fallback chains, crash handling, cancellation, delegation (including the 5-bug acceptance scenario with bounded concurrency and no recursive delegate injection), and the lifecycle roundtrip.

Two implementation notes for the curious:

- The router is a terminal ACP agent upstream plus N downstream ACP client connections — deliberately not the linear proxy-chain/conductor role.
- Transports are local replacements over the SDK's `Lines` component that flush after every line: the SDK's built-in `Stdio`/`AcpAgent` transports (agent-client-protocol 1.2.0) buffer writes without flushing, which deadlocks small JSON-RPC exchanges.
