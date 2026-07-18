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
[`@agentclientprotocol/claude-agent-acp`](https://github.com/agentclientprotocol/claude-agent-acp)
and [`@agentclientprotocol/codex-acp`](https://github.com/agentclientprotocol/codex-acp),
and provides OpenRouter-*inspired* selection semantics over them (a local
heuristic, not a port of OpenRouter's proprietary router — see below):

```
goose / Zed ──ACP──▶ router-acp ──ACP──▶ claude-agent-acp   (claude/sonnet, claude/opus)
                          │
                          └──────ACP──▶ codex-acp           (codex/gpt-5.1-codex)
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
   [router-acp] auto → claude/sonnet · task BugFix (complexity 0.35) · utility 0.82 = 0.3×quality 0.80 (BugFix) + 0.7×quota (headroom 100%, cost rank 2)
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
  quota_score    = headroom[agent] * (1 - normalized_cost_rank)
  utility        = quality_weight * quality[task_class] + cost_weight * quota_score
  ```

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

ACP adapters don't expose seat meters, so headroom is an estimate: per-agent
sliding-window counters (default 5 h) of prompts and sessions, normalized
against `budget_prompts_5h`. A rate-limit/auth/quota error before the first
prompt zeroes an agent's headroom and the router walks the fallback chain; a
candidate that keeps failing pre-prompt is quarantined for a cool-off. Errors
after a session is pinned are surfaced, never rerouted.

### Quality data

Per-class quality scores, coding tiers/percentiles, and context windows live
in a versioned YAML table shipped with the binary
([`data/scores.yaml`](data/scores.yaml)) and overridden wholesale with the
`score_table` config key. Entries are keyed by candidate pattern
(`agent/model`, `*` wildcards, first match wins). Updating routing quality is
a data edit, not a code change. The heuristic classifier's rule tables work
the same way ([`data/classifier.yaml`](data/classifier.yaml),
`classifier.rules_file`).

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
downstream session on a strictly lower-`cost_rank` candidate**, returns the
sub-agent's output as the tool result, and forwards the sub-session's
permission/fs/terminal callbacks to the original client under the parent
session id — permission UX stays intact while sub-agent transcript streaming
is not interleaved into the parent transcript.

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

## Auto-orchestration

When a prompt reads as a **multi-part task list** — markdown bullets or numbers,
inline `(1) … (2) …`, or ordered prose ("first … then … finally …") — the
router runs a plan → subtasks → review → submit pipeline entirely in-process. It
steers (pre-pin) or switches (mid-session) the session onto a **planner**
frontier model and injects an orchestration protocol instructing that model to:

1. **Plan** — restate the task as success criteria and split it into
   file-disjoint, self-contained subtasks.
2. **Delegate** — dispatch each subtask via `delegate_task`, each routed
   per-complexity in its own sub-session (independent ones concurrently).
3. **Review** — delegate an independent review to a candidate of a **different
   lineage** than the planner (`reviewer` globs), handing it the original task
   verbatim.
4. **Adjudicate** — fix blocking issues and re-review, bounded by
   `max_fix_rounds`.
5. **Submit** — per the `submit` gate (`never | branch | pr | merge`); a merge
   is only permitted after the review approves.

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

See [`examples/router.yaml`](examples/router.yaml) for a complete annotated
example.

| Key | Default | Meaning |
| --- | --- | --- |
| `router` | `auto` | Default strategy: `auto`, `pareto-code`, `escalation`, `static`. |
| `state_file` | `~/.local/state/router-acp/sessions.db` | SQLite database. `sessions` table: one row per router session (pin, cwd, title, routing decision + weights, `parent_session_id` for delegated sub-agents, `prior_session_id` (the downstream session bound before a mid-session model switch), `kind`, `run_label`, token/context counters). `session_log` table: every ACP interaction (user prompt, model response, tool call, permission/fs/terminal callback) with token counts. A legacy `sessions.json` beside it is imported once. Inspect with `router-acp sessions --config …`. |
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
| `failover.enabled` | `true` | Fail a pinned session over to the next best candidate on limit/outage (only before any output streamed this turn). |
| `failover.respawn_cooldown_secs` | `30` | Minimum interval between respawn attempts of a dead downstream process. |
| `failover.max_attempts` | `3` | Candidates tried per prompt (initial + failovers). |
| `auto_upgrade.enabled` | `true` | Auto-switch a pinned session up to a more capable model when confidence drops. `false` disables it (explicit `[router: switch=…]` still works). |
| `auto_upgrade.confidence_threshold` | `0.55` | Upgrade when confidence (pinned quality − struggle) falls below this. Higher = more eager; `0` ≈ never. |
| `skill_routing[]` | `[]` | Rules forcing a skill onto a model class: `pattern` (skill name, matched as `/name` or a standalone token) → `candidates` (candidate globs in preference order). Mid-session it switches; pre-pin it steers routing. |
| `orchestration.enabled` | `false` | Auto-orchestrate multi-part task lists: steer/switch to a planner model and inject the decompose→delegate→review→submit protocol. |
| `orchestration.min_items` | `2` | Smallest detected list size treated as a multi-part task. |
| `orchestration.planner[]` | frontier globs | Planner/orchestrator candidate globs, best first (first eligible wins). |
| `orchestration.reviewer[]` | frontier globs | Preferred cross-lineage reviewer globs handed to the orchestrator (it should pick a different lineage than the planner). |
| `orchestration.submit` | `branch` | Submission gate given to the orchestrator: `never \| branch \| pr \| merge` (a merge is only permitted after the review approves). |
| `orchestration.max_fix_rounds` | `2` | Max review → fix → re-review rounds. |
| `agents[].name` | – | Unique, no `/`. Candidate ids are `name/model-id`. |
| `agents[].command` | – | `type: stdio` command/args/env for the adapter. `${VAR}` in the whole file interpolates from the environment (unknown names are left intact). |
| `agents[].model_selection.type` | – | `config-option`: one process per agent; the router discovers the `category: model` select option at probe time and applies `session/set_config_option` per session. `spawn-config`: one process per model, built from `process_template` (with `${model_id}` substitution); no universal `-m` flag is assumed. |
| `agents[].budget_prompts_5h` | `400` | Headroom normalization budget. |
| `agents[].models[]` | – | `id` (must exactly match the downstream selector's value for `config-option`), optional `display_name`, `cost_rank` (1 = cheapest/least scarce). |
| `agents[].mode_map` | `{}` | Translate client-requested session mode ids to this agent's ids (e.g. goose's `auto` -> claude's `bypassPermissions`). |
| `agents[].preference` | `0` | Additive utility tie-break for this agent (`auto`) and within-tier tie-break (`pareto-code`). Keep small, e.g. `0.05`. |
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
