# Installing goose

Don't have goose yet?

- `npm install -g @agentclientprotocol/claude-agent-acp`
- `npm install -g @agentclientprotocol/codex-acp`
- `curl -fsSL https://github.com/block/goose/releases/download/stable/download_cli.sh | bash`

Then run goose with `goose`. Go thru the config flow for claude and/or codex.

# Using router-acp with goose

Exact install steps for goose ≥ 1.41 on macOS, written for a machine that
already uses the built-in `claude-acp` and `codex-acp` goose providers —
**both of which stay untouched**. The router gets its own provider slot, so
you can run plain Claude sessions and routed sessions side by side.

## How the hookup works (read this first)

goose's ACP providers are **built in and spawn fixed binary names** — there is
no config key for a custom ACP agent command. The ACP slots in goose 1.41 are
`claude-acp`, `codex-acp`, `copilot-acp`, `amp-acp`, and `pi-acp`. (goose's
`openrouter` provider is an API-key chat-completions provider, not an ACP
provider, so there is no OpenRouter ACP slot to borrow — the unused Pi slot
is the equivalent move.)

goose resolves each slot's binary through its own search list, in this order:
`GOOSE_SEARCH_PATHS` (config) → `~/.local/bin`, `/usr/local/bin`,
`/opt/homebrew/bin` → npm global bin → your `PATH`.

The **`pi-acp`** slot is ideal to borrow:

- it spawns a binary named exactly **`pi-acp`** with **no arguments and no
  env** (verified in goose 1.41 source), which is precisely the contract of
  `router-acp serve`;
- you don't have Pi installed (`pi-acp` resolves to nothing on this machine),
  so a shim by that name shadows **nothing**;
- `claude-acp` and `codex-acp` keep resolving the real adapters — including
  `GOOSE_PLANNER_PROVIDER: codex-acp`.

Do **not** borrow `codex-acp` (goose passes `-c approval_policy=…` flags that
`router-acp serve` doesn't accept) or `claude-acp` (you want it working
directly). `amp-acp` would also work (no-args contract, binary name
`amp-acp`) if you ever adopt Pi for real.

So the plan: a **shim named `pi-acp`** in a private directory listed in
`GOOSE_SEARCH_PATHS`, exec-ing `router-acp serve`. Selecting the `pi-acp`
provider in goose then runs the router, which fans out to the real Claude and
Codex adapters downstream.

## 1. Install router-acp

```sh
cargo install --path .          # installs ~/.cargo/bin/router-acp
router-acp --version
```

`~/.cargo/bin` does not need to be on goose's search path — only the shim
references it, by absolute path.

## 2. Create the router config

Write `~/.config/router-acp/router.yaml`:

```yaml
router: auto
state_file: ~/.local/state/router-acp/sessions.json

delegation:
  enabled: true
  max_concurrent: 3

agents:
  - name: claude
    command:
      type: stdio
      command: ~/nvm/versions/node/v24.16.0/bin/claude-agent-acp
    model_selection: { type: config-option }
    budget_prompts_5h: 400
    # Larger plan on claude: prefer it over codex when candidates are
    # otherwise comparable (small additive utility bonus).
    preference: 0.05
    models:
      - { id: haiku, display_name: Claude Haiku, cost_rank: 1 }
      - { id: sonnet, display_name: Claude Sonnet, cost_rank: 2 }
      - { id: "opus[1m]", display_name: Claude Opus 1M, cost_rank: 4 }
      - { id: "claude-fable-5[1m]", display_name: Claude Fable 5 1M, cost_rank: 5 }

  - name: codex
    command:
      type: stdio
      command: ~/nvm/versions/node/v24.16.0/bin/codex-acp
    model_selection: { type: config-option }
    budget_prompts_5h: 400
    models:
      - { id: gpt-5.4-mini, display_name: GPT-5.4 Mini, cost_rank: 1 }
      - { id: gpt-5.5, display_name: GPT-5.5, cost_rank: 3 }

routers:
  auto:
    # 0 = pure quality, 10 = cheapest survivor. Seats are flat-rate, so lean
    # quality: trivial prompts still go cheap because the tradeoff scales
    # down with classified complexity — hard prompts get frontier models.
    cost_quality_tradeoff: 3
```

### Tuning: how prompts map to models

`auto` scores every candidate as
`utility = quality_weight×quality(task class) + cost_weight×quota + preference`,
where the quality/cost weights come from `cost_quality_tradeoff` **scaled
down by the prompt's classified complexity** (a hard prompt makes cost
matter less). With the config above you should see roughly:

| prompt | routes to |
| --- | --- |
| "hello world", one-line tweaks | `claude/haiku` or `claude/sonnet` |
| ordinary coding tasks | `claude/sonnet` |
| cross-system investigation, architecture, "analyze the PR + ticket…" | `claude/claude-fable-5[1m]` or `claude/opus[1m]` |

The disclosure line shows the math (`utility 0.86 = 0.86×quality 0.95
(Research) + 0.14×quota … + pref 0.05 · tradeoff 3→1.4
(complexity-scaled)`), so when a choice looks wrong, the line tells you
which factor drove it:

- Routed too cheap on a hard task → the classifier under-scored complexity
  (check `complexity` in the line); lower `cost_quality_tradeoff`, or add
  keywords to a custom `classifier.rules_file`.
- Wrong family winning → adjust `preference` (small values; 0.05
  recommended) or the per-model `cost_rank`s.
- A model looks mis-scored → the quality data lives in `data/scores.yaml`
  (override with `score_table`); patterns are first-match-wins, so specific
  patterns (`*mini*`) must precede broad ones (`*gpt-5*`).

Notes on this file:

- The **absolute adapter paths** point at your current nvm-managed npm
  globals (and can never collide with the shim, whose name is `pi-acp`).
  After a node upgrade (`nvm install`), update the version in both paths, or
  switch to `command: npx` with
  `args: ["-y", "@agentclientprotocol/claude-agent-acp"]` (slower startup,
  version-proof). A leading `~`/`~/` in `command` and `args` is expanded to
  `$HOME` at config load — adapters are spawned without a shell, so this
  expansion is the router's, not the shell's (a bare `~user` is left as-is).
- The **model ids were verified against your installed adapters** (July 9,
  2026): claude-agent-acp offers `default`, `haiku`, `sonnet`, `sonnet[1m]`,
  `opus[1m]`, `claude-fable-5[1m]`; codex-acp offers `gpt-5.4-mini`,
  `gpt-5.4`, `gpt-5.5`. Ids must match the adapter's model selector exactly —
  mismatches are removed from the pool at startup with a warning, never
  guessed at prompt time. To re-discover ids after an adapter update, declare
  a bogus id and read the `available=[…]` warning:

  ```sh
  RUST_LOG=router_acp=debug router-acp serve --config ~/.config/router-acp/router.yaml \
    < /dev/null 2>&1 | grep available
  ```

  (Quote ids containing `[`/`]` in YAML, e.g. `"opus[1m]"`.)

Validate:

```sh
router-acp check-config --config ~/.config/router-acp/router.yaml
```

Expected: `configuration OK: 2 agent(s)` plus the five candidate lines.

## 3. Create the shim goose will spawn

```sh
mkdir -p ~/.config/router-acp/bin
cat > ~/.config/router-acp/bin/pi-acp <<'EOF'
#!/bin/sh
# goose "pi-acp" provider slot -> router-acp
export RUST_LOG="${RUST_LOG:-router_acp=info}"
exec "$HOME/.cargo/bin/router-acp" serve --config "$HOME/.config/router-acp/router.yaml"
EOF
chmod +x ~/.config/router-acp/bin/pi-acp
```

Keeping this directory out of your shell `PATH` is tidy but not critical —
the name `pi-acp` collides with nothing on your machine either way.

## 4. Register the slot with goose

One additions to `~/.config/goose/config.yaml`:

Mark the provider configured**, mirroring the shape of your existing
`claude-acp` entry, plus the legacy flag goose's own setup instructions use:

```yaml
providers:
  claude-acp:            # unchanged
    enabled: true
    model: default
    configured: true
  pi-acp:
    enabled: true
    model: default
    configured: true
```

Leave `active_provider: claude-acp` as is if you want plain Claude to stay
the default. You now have both worlds:

```sh
goose session                              # direct Claude, exactly as before
GOOSE_PROVIDER=pi-acp goose session        # routed via router-acp
```

To make routing the default (so you can drop the `GOOSE_PROVIDER=pi-acp`
prefix), see step 6.

## 5. Verify

```sh
GOOSE_PROVIDER=pi-acp goose session
```

First session start is slower than plain claude-acp: the router spawns *both*
adapters and probes them (initialize + a throwaway `session/new` + model
catalog validation) before answering goose's initialize.

Send any prompt. The first reply must begin with the routing disclosure:

```
[router-acp] auto → claude/sonnet (class CodingGeneral, complexity 0.12)
```

That line is your proof the router is serving the session. Also sanity-check:

- A trivial prompt ("fix a typo in README.md, one-line change") should route
  to a cheap candidate; an architecture-flavored prompt should route to an
  expensive one.
- `router-acp sessions --config ~/.config/router-acp/router.yaml` lists
  recent sessions (candidate, kind, token total, title); add
  `--session <rtr-id>` for the full picture: the routing `why` (weights +
  utility math), token usage (input/output/context), and the `session_log`
  of every prompt, response, and tool call with per-entry token counts.
  It's a SQLite DB (`~/.local/state/router-acp/sessions.db`) — query it
  directly with `sqlite3` if you like. Delegated sub-agents appear as child
  rows linked to their parent; orchestration runs share a `run_label`. The
  DB auto-prunes to the `history` window (default 30d).
- A plain `goose session` (no `GOOSE_PROVIDER`) must still start the real
  Claude adapter with **no** `[router-acp]` line — proving claude-acp was
  left alone.

## 6. Make routing the default (drop the per-run flag)

Once you've verified it works, you don't have to prefix every command with
`GOOSE_PROVIDER=pi-acp`. goose resolves the active provider in this order:

1. the **`GOOSE_PROVIDER` environment variable**,
2. the **`active_provider:`** key in `config.yaml`,
3. a legacy **`GOOSE_PROVIDER:` config key** (last-resort fallback only).

Pick whichever fits:

**a) Environment variable (recommended — most durable).** Add it to your shell
profile so every `goose` invocation picks up the router:

```sh
# ~/.zshrc  (or ~/.bashrc)
export GOOSE_PROVIDER=pi-acp
```

This is just the persistent form of the `GOOSE_PROVIDER=pi-acp` you tested. It's
checked *first*, so it always wins — and because it lives in your shell, not in
`config.yaml`, **goose can't reset it** (see the caveat below). Revert by
removing the line; unset it for a one-off (`GOOSE_PROVIDER= goose session`) to
drop back to plain Claude.

**b) `active_provider` in `config.yaml`.** The goose-native way:

```yaml
active_provider: pi-acp
```

Works, but goose *manages* `config.yaml` — its `/model` command and provider
picker rewrite the file, and can reset `active_provider` back to `claude-acp`.
If you find routing silently reverting, that's why; use option (a) instead.

> **Do NOT set `GOOSE_PROVIDER:` as a key in `config.yaml`.** It's only the
> last-resort fallback (step 3 above) and is *not* the field goose's setup
> check reads, so goose decides no provider is configured and drops you into
> the onboarding flow. Use the environment variable or `active_provider`.

You do **not** need to publish or register the router as a "real" goose
provider — `pi-acp` is already a valid slot once steps 3–4 are done.

## Choosing candidates per session

The router owns two ACP session config options which goose surfaces where it
supports config selectors (goose Desktop's model picker; the CLI does not
support switching models on ACP providers):

- `router.candidate` (category `model`): `auto`, or a concrete candidate like
  `codex/gpt-5.5` — set **before the first prompt** to pin that session
  explicitly. After the first prompt the session is pinned for life; changing
  candidates returns a "session already pinned" error by design (ACP has no
  transcript handoff). Start a new goose session to change models.
- `router.strategy`: `auto`, `pareto-code`, or `static` for that session.

## Session modes (GOOSE_MODE)

goose sets its `GOOSE_MODE` on every ACP session via `session/set_mode`
**immediately after `session/new`** — before the first prompt, i.e. before
the router has picked a candidate. The router accepts this, defers it, and
applies it to the downstream session at pin time; if the pinned candidate has
no matching mode id, the mode is skipped with a logged warning and the
session continues in that agent's default mode (goose still governs
permission prompts client-side either way).

Mode ids on your installed adapters (probed July 9, 2026):

| adapter | mode ids |
| --- | --- |
| claude-agent-acp | `auto`, `default`, `acceptEdits`, `plan`, `dontAsk`, `bypassPermissions` |
| codex-acp | `read-only`, `auto`, `full-access` |

The `pi-acp` slot sends goose modes as `auto`, `approve`, `smart-approve`,
or `chat`. Your `GOOSE_MODE: auto` matches an `auto` mode on **both**
adapters, so it works with no extra config. The other goose modes have no
downstream equivalent; translate them (and/or change what `auto` means) with
an optional per-agent `mode_map` in `router.yaml`:

```yaml
agents:
  - name: claude
    # goose's claude-acp provider historically ran Claude in
    # bypassPermissions; add this line to reproduce that exact behavior
    # instead of Claude's own `auto` (classifier-approved permissions):
    mode_map: { auto: bypassPermissions, approve: default, chat: plan }
    # ...

  - name: codex
    mode_map: { approve: read-only }
    # ...
```

Mapping targets must be ids the adapter actually advertises, or the mapping
is ignored with a warning.

## Token limits and outages: what you'll see

Every routing decision is disclosed as a blockquote riding the model's own
reply (goose drops separate router messages, so the router embeds it in the
answer):

```
> router-acp · auto → claude/sonnet · task BugFix (complexity 0.35)
> why: utility 0.82 = 0.3×quality 0.80 (BugFix) + 0.7×quota (headroom 100%, cost rank 2)
```

On agentic turns (tool calls) goose splits the message stream, so the line
may appear early rather than beside the final answer. The authoritative,
always-available record is the state DB: `router-acp sessions --config
~/.config/router-acp/router.yaml --session <id>` shows the exact model and
`why` for every session.

When Claude or Codex hits its **token/usage limit**, the router parses the
reset time from the model's own error (Claude Code reports
`usage limit reached|<epoch>`; Codex reports `try again in …`), cordons that
agent until the reset, and — if this happens mid-prompt before any output —
fails the session over to the next best candidate:

```
[router-acp] claude/sonnet unavailable — token/usage limit (model reports reset in ~2h05m); failing over…
[router-acp] failover: auto → codex/gpt-5.5 · task BugFix (complexity 0.35) · utility 0.71 = …
[router-acp] note: conversation context from earlier turns does not transfer to the new model
```

Later sessions show active cordons too
(`[router-acp] claude is cordoned: token/usage limit … (~1h58m left)`), and
the cordoned agent automatically rejoins the pool when its limit resets.
**Outages** (adapter crash, connection loss) behave the same way, plus the
dead adapter process is respawned in the background so it can rejoin once
healthy. Failover never happens after the turn has already streamed output
(that could duplicate side effects) — in that case the error is surfaced
with a note explaining why. Tune with `failover.{enabled,max_attempts,
respawn_cooldown_secs}` and `headroom.cordon_default_secs` in `router.yaml`.

## Orchestrated workflows (plan → subtasks → cross-lineage review)

Orchestration is now **built into router-acp** — there is no goose recipe or
wrapper to install. Turn it on once in `router.yaml`:

```yaml
orchestration:
  enabled: true
  planner: ["*fable*", "*opus*", "*sol*", "*gpt-5.5*"]
  reviewer: ["*sol*", "*gpt-5.5*", "*opus*"]
  submit: branch        # never | branch | pr | merge (merge only after review approves)
```

Then just type a multi-part task in any goose session — a markdown list,
`(1)…(2)…`, or "first… then… finally…". The router pins a planner frontier
model, decomposes the task, fans the parts out to routed sub-sessions via its
`delegate_task` tool, has a different-lineage model review the result, and
submits per `submit`. You'll see `router-acp · orchestrating a N-part task on …`
inline; the planner and every subtask are recorded (sharing `run_label
= orchestrate`) in `~/.local/state/router-acp/sessions.db`.

It fires on any prompt and is suppressed by an explicit `[router: …]` directive,
a `model:` shorthand, or when your list is answering the model's own questions.
See [`ORCHESTRATION.md`](ORCHESTRATION.md) for the full pipeline, the
`orchestration.*` config, and caveats.

**Watch for degradation.** The whole benefit depends on the planner using the
router's `delegate_task` tool. If it uses its adapter's *built-in* sub-agent tool
(Claude's `Task`) instead, sub-work stays in one lineage and is invisible to the
router — you'll see `router-acp · orchestration degraded: the planner used its
built-in sub-agent tool …` inline, and no delegate rows appear.

**Review your runs** with the report:

```sh
router-acp report --config ~/.config/router-acp/router.yaml
```

It shows, per run: planner vs. delegate **cost** (the adapter's real USD, not an
estimate), delegate count, whether a cross-lineage review ran, and the
degraded% (native-subagent use). Each run is tagged with its git branch/HEAD so
you can later join outcomes to CI/merge results.

## Notes and caveats

- **Cosmetics:** goose labels the borrowed slot "Pi" in provider lists and
  `goose configure`. Purely a display-name quirk — the slot is just a
  no-args ACP stdio launcher.
- **claude-acp / codex-acp / planner:** all untouched and still talk to the
  real adapters directly (both use different binary names than the shim).
- **Delegation:** in routed sessions the pinned agent gets a `delegate_task`
  MCP tool and may hand small subtasks (mechanical edits, isolated fixes) to
  a cheaper candidate in an ephemeral session; permission prompts for
  delegated work still appear in goose under the parent session. Set
  `delegation: { enabled: false }` in `router.yaml` to turn this off.
- **Auth:** both adapters are already seat-authenticated, so probes pass
  silently. If an adapter loses auth, routed sessions get `auth_required`
  from `session/new`; re-run the vendor CLI login (`claude`, `codex login`)
  and start a new session. (The router also relays ACP `authenticate` with
  method ids namespaced as `claude/…` / `codex/…` for clients that drive
  auth over ACP.)
- **Logs:** the shim's `RUST_LOG=router_acp=info` output lands on stderr and
  is captured under `~/.local/state/goose/logs/`. Bump to
  `router_acp=debug` in the shim when diagnosing routing or model discovery.
- **"Failed to set session mode … modes are unavailable before the first
  prompt"**: this error came from router-acp builds before the deferred-mode
  fix. Rebuild/reinstall (`cargo install --path . --force`) — goose's
  post-`session/new` `set_mode` is now accepted and deferred.
- **Reverting:** remove the `pi-acp` entries and the `GOOSE_SEARCH_PATHS`
  block from `~/.config/goose/config.yaml` (and delete
  `~/.config/router-acp/bin` if you like). Nothing else was changed, so
  there is nothing to restore.
