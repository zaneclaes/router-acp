# AGENTS.md — context for coding agents working on router-acp

router-acp is an ACP (Agent Client Protocol) session router: one terminal
ACP **agent** upstream (the client — goose, Zed — connects to it over stdio)
plus N ACP **client** connections downstream to seat-authenticated adapters
(`claude-agent-acp`, `codex-acp`). It routes each new conversation to the
best `(agent, model)` candidate, pins it, relays everything, and offers a
`delegate_task` MCP tool for cheaper sub-sessions. Built against
`agent-client-protocol` **1.2.0** (Rust SDK). The original build spec
(HAI-6345) was completed and deleted; `PLAN.md` now holds the follow-on
orchestration plan. User-facing docs: `README.md`, `ROUTERS.md`,
`GOOSE.md`, `ORCHESTRATION.md`.

## Build / test / install

```sh
cargo test                    # 113 tests: unit + tests/protocol.rs E2E
cargo clippy --all-targets    # keep at zero warnings
cargo fmt
cargo install --path . --force   # the user's goose shim runs ~/.cargo/bin/router-acp
```

The protocol tests spawn real `mock-agent` subprocesses and complete in a
few seconds. If a test hangs, something is genuinely deadlocked — see the
SDK traps below, which were all discovered the hard way.

## Module map

| file | owns |
| --- | --- |
| `src/session.rs` | the hub: `Shared` state, upstream agent surface (all ACP handlers), pin/failover engine (`pin_session`, `send_prompt_with_failover`), downstream→upstream relay (`handle_downstream_dispatch`), disclosures (`notify_user`), failure accounting (`apply_failure`), respawn (`revive_dead_targets`) |
| `src/downstream.rs` | process targets from config (`config-option` = 1 process/agent; `spawn-config` = 1 process/model with `${model_id}` templating), spawn+contain (`start_downstream`), probe/verify (`probe_target`, model-selector discovery, `verify_model_selected`) |
| `src/transport.rs` | flushing stdio + child-process transports (replaces broken SDK ones); **downstream teardown**: each agent process is spawned in its own process group (`process_group(0)`) and its PID tracked in a global registry; `kill_all_downstreams()` SIGKILLs every group (agent + grandchildren like a Bash mid-`git commit`). `serve` calls it on disconnect AND from a SIGINT/SIGTERM handler — because `kill_on_drop` does NOT run on signal-death or runtime teardown, which once let a Ctrl+C'd agent keep running and commit broken code to `main` |
| `src/strategies/` | `RouterStrategy` trait; `static_`, `auto`, `pareto_code`, `escalation`; each `RankedCandidate` carries `reason` (human) + `weights` (json) for disclosure/state. `escalation::rank` only picks the cheap start + fallback chain; its escalation is runtime (`session::escalation_target`/`note_investigation`/`escalation_post_turn`) |
| `src/classifier.rs` | heuristic task class + complexity (data-driven), cwd language fingerprint, optional Ollama backend (own mini HTTP client; never uses seat agents) |
| `src/candidate.rs` | `CandidateId`, `TaskClass`, `CodingTier`, `RequiredCaps`, score table (`data/scores.yaml`) |
| `src/limits.rs` | failure classification (RateLimited/Outage/Other) + reset-time parsing (regex, every format unit-tested) + `humanize` |
| `src/headroom.rs` | sliding-window seat budgets, candidate quarantine, per-agent **cordons** (reactive, error-driven, monotonic `Instant`), per-candidate **usage cordons** (`UsageCordon`/`usage_cordons`, proactive, absolute wall-clock `resets_at`, replaced wholesale by the poller via `set_usage_cordons`) AND per-candidate **seat availability** (`SeatAvailability`: `plan_headroom` + `on_overage`, poll snapshot via `set_polled_availability` + per-agent TTL'd client hints via `set_hinted_availability`; fresh hint outranks poll in `availability()`) |
| `src/usage.rs` | proactive usage-cap poller: `anthropic-oauth` (CLI OAuth token from `~/.claude/.credentials.json` or macOS Keychain `Claude Code-credentials`, `GET /api/oauth/usage` via shelled-out **curl** with the token on stdin, no TLS dep; `anthropic_cordons` — overage-gated, `limits[].scope.model.display_name` match) and `codex-rollout` (reads Codex's own on-disk rate-limit snapshots from `~/.codex/sessions/**/rollout-*.jsonl`, newest per limit pool; `codex_cordons` — gated on *usable* credits (`unlimited`/positive `balance`, not bare `has_credits`), account-wide, `epoch_to_rfc3339`). Both pure+tested, never a hardcoded model list. Also computes graded **seat availability** for dynamic preference scaling (`anthropic_availability`/`codex_availability`/`availability_from_windows`) and ingests client hints (`apply_availability_hint`, ext notification `router-acp/availability_hint`, consumed session-less in `on_catch_all`). `spawn_usage_poller` (interval loop, fails open, installs cordons + availability) |
| `src/state.rs` | **SQLite** state DB (rusqlite, bundled): `sessions` table (pin + routing diagnostics + `parent_session_id`/`prior_session_id` (switch lineage)/`kind`/`run_label` + token counters + observability metrics: `cost_usd` (authoritative USD from `usage_update.cost`, max), `native_subagent_calls` (orchestration-degradation count), `compute_ms` (model turn time excl. idle), `git_branch`/`git_sha` (for CI/merge join)) and `session_log` table (every ACP interaction + tokens); `history`-window pruning; additive column migrations via guarded `ALTER TABLE`; one-time `sessions.json` import. Setters `set_cost_usd`/`note_native_subagent`/`add_compute_ms`/`set_git` update in place (not via `upsert`, so re-pin preserves them). `StateFile` methods take `&self` (Connection is !Sync → kept behind `Mutex` in `Shared`). |
| `src/lifecycle.rs` | session/list,load,resume,delete,close (route to owning downstream, ids remapped, pin rehydrated) |
| `src/delegate_mcp.rs` | delegate tools: unix-socket listener, token→session binding, minimal MCP server on the SDK's JSON-RPC layer, `run_delegate_task` (cheaper-only pool — except orchestrating sessions, which may delegate to same-/higher-tier peers), multi-turn `delegate_task keep_open` → `run_delegate_followup`/`run_delegate_close` over a `Shared.live_delegates` registry, `mcp-delegate` helper bridge |
| `src/tasklist.rs` | `detect_task_list(text) -> Option<usize>` — recognizes multi-part task lists (markdown numbers/bullets, inline `(1)(2)`, semantic "first…then…finally" ordering) for auto-orchestration; pure + unit-tested |
| `src/tickets.rs` | ticket-context loading: `find_ticket_refs` (configured `prefix` at word start + digits), `fetch_ticket` (rule's argv with `$TICKET` substituted, no shell, 20s timeout, output capped), `enrich_prompt` (prepends framed ticket content BEFORE orchestration detection/classification; per-session dedup via `injected_tickets`, 5-min global `ticket_cache`, fail-open, disclosed). Pluggable across ticketing systems (linear/jira/gh CLIs) |
| `src/relay.rs` | raw `UntypedMessage` sessionId rewriting + `_meta.router_acp` attachment |
| `src/config.rs` | YAML config, env interpolation (`${VAR}`; unknown names left intact for later `${model_id}` substitution), validation |
| `src/bin/mock_agent.rs` | scripted downstream for tests (below) |
| `data/*.yaml` | score table + classifier rules, embedded via `include_str!`, overridable by config path |

## SDK traps (agent-client-protocol 1.2.0) — do not relearn these

1. **Built-in transports never flush.** `Stdio`, `AcpAgent`, `ByteStreams`
   write lines without flushing, and their writers (`blocking::Unblock`,
   `async_process::ChildStdin`) buffer — every small exchange deadlocks.
   Always use `transport::{stdio_lines, lines_transport, ProcessTransport}`
   (they flush per line, report reader EOF as a disconnect error so
   connections actually terminate, and monitor child exit). If you bump the
   SDK, re-test before touching this.
2. **`forward_response_to` onto a downstream connection can strand the
   upstream request.** Its consuming task runs on the *downstream's* task
   actor; if that process dies, the task is dropped and the upstream request
   hangs forever. Relay downstream-bound requests with
   `session::relay_request_to_downstream` (or `block_task` in an
   upstream-spawned task). `forward_response_to` is fine in the other
   direction (downstream request → upstream), because if upstream dies
   everything dies.
3. **`Shared` uses non-reentrant `std::sync::Mutex`.** Never call another
   `Shared` accessor while holding one of its locks (build_initialize_response
   once self-deadlocked on exactly this), and never hold a lock across
   `await`.
4. **Ordering matters when registering session routes.** Register the
   sid_map entry inside `SentRequest::on_receiving_result` of the
   `session/new` call (the dispatch loop waits for that callback's ack, so
   no `session/update` can race past), or before sending for load/resume
   where the downstream sid is already known. `block_task` acks immediately
   and does NOT give this guarantee.
5. **Handler/`consume_with` callbacks must return `Ok`** — a returned `Err`
   tears down the whole connection.
6. **In `ProcessTransport`, side channels must not win the select.** The
   stderr-drain future ends in `pending()`: on child death stderr EOFs too,
   and if it completed the select with `Ok` the connection would linger
   half-dead (was a real flaky test).
7. **Schema structs are `#[non_exhaustive]`** — construct via `::new(...)` +
   builder methods, never struct literals. Unhandled `Dispatch::Response`
   must be returned as `Handled::No` so the SDK routes it to its awaiter;
   unhandled requests get `method_not_found` automatically. Chained handlers
   run in registration order — the untyped catch-all must be registered
   last.
8. **MCP request/notification params are often `null` or omitted.** The
   hand-typed `delegate_mcp` wire structs (`McpToolsListRequest`, `McpPingRequest`,
   `McpInitializedNotification`, `McpInitializeRequest`) must deserialize from
   `null`/missing/`{}` — serde's derived struct impl rejects `null` ("invalid
   type: null, expected struct"). Real adapters (claude-agent-acp) send
   `tools/list`/`ping`/`notifications/initialized` with `params: null`; when
   `tools/list` errored, the adapter saw **zero** delegate tools and
   `delegate_task` silently never appeared — so **delegation never worked live at
   all** (0 rows in the state DB, undetected until orchestration relied on it).
   The mock's delegate path didn't exercise `tools/list`, so unit/protocol tests
   were green while production was broken. Fix: `lenient_params!` macro impls a
   `Deserialize` via `IgnoredAny` → `Default` for those types (handlers ignore
   the params anyway). Regression: `mcp_request_params_accept_null_and_missing`.
   **To verify MCP-exposure changes, drive the REAL adapter** (see
   `scratchpad/probe_delegate.py` pattern) and watch for the
   `delegate MCP helper connected` log with no `Handler errored … tools/list`;
   the mock cannot catch this class of bug.

## Architectural invariants (from PLAN.md; do not break casually)

- Never call provider model APIs; everything goes through ACP adapters.
- Model selection: `session/set_config_option` on the `category: model`
  select option; the **response** is authoritative (silent no-ops must fail
  the pin). No `session/set_model`. No assumed CLI model flags —
  `spawn-config` exists for per-model argv/env.
- Route once per session; pin for life. The deliberate exception (added at
  user request, post-plan): failover when the pinned model is rate-limited
  or down **and the turn has produced no output** (`turn_saw_output`) and
  the client hasn't cancelled. Context loss is disclosed.
- Relaying is raw-JSON with only `sessionId` rewritten; `_meta` is
  preserved; router metadata lives under `_meta.router_acp` only.
- Delegation: strictly lower `cost_rank` than the parent, depth capped at 1
  (the delegate MCP server is stripped from sub-sessions), concurrency
  bounded by a semaphore, parent cancel propagates.
- **Every routing decision must be visible**: console line (chunk) with the
  strategy math + skipped candidates + cordons, mirrored into
  `_meta.router_acp` and persisted into the state file (`routing` field).
  If you add a decision point, disclose it.
- Deterministic routing: no randomness; tie-breaks are score → effective
  cost → preference → config order.
- **Proactive usage cordons** (`cordon.*` + per-agent `usage_source`;
  `src/usage.rs`): a periodic poll (`spawn_usage_poller` in `serve_shared`,
  aborted on return) reads the provider usage API and marks exhausted candidates
  unroutable *before* they're tried. Enforcement seams: `eligible_views`
  excludes them (so `auto`/failover never pick them); an explicit pin to a
  cordoned candidate is refused in `pin_session` (`cordon_redirect` clears the
  override → best non-cordoned candidate, disclosed in the failover-line format
  + `details.cordon_redirect`); if the pool is empty ONLY because everything is
  usage-cordoned, `eligible_views_relaxed` + soonest-`resets_at` picks the
  least-bad rather than failing (`all_cordoned_fallback`); `router_config_options`
  keeps cordoned candidates in the `router.candidate` picker but tags them
  `_meta.router_acp.{available:false,unavailable_reason,resets_at}`; and every
  turn's routing metadata carries `details.usage_cordons`
  (`[{candidate,reason,resets_at}]`, the whole `active_usage_cordons()` set) so a
  client that cached the candidate list at `session/new` can refresh
  availability mid-session — the picker option is only re-advertised at
  session creation, but cordons can appear/lift during a long session. **Invariants:**
  generic (models discovered from the API, never hardcoded — the cordon gate is
  a *scoped weekly cap ≥100%* AND *overage/credit pool has no headroom*);
  **fail-open** (any poll/token/parse error → no cordon; the reactive per-agent
  cordon is the safety net); self-lifts at absolute `resets_at`. Codex has no
  *pollable* usage endpoint (rate limits arrive in HTTP response headers, and
  Cloudflare 403s any non-Codex client — even `/backend-api/me`), so
  `codex-rollout` instead reads Codex's own on-disk snapshot from its rollout
  JSONL (last-known as of Codex's most recent turn, undocumented format → parse
  fails open; the reactive cordon backstops the staleness gap). Codex writes one
  snapshot per limit pool (`limit_id`: "codex", "premium", …), so the reader
  keeps the newest snapshot PER POOL — taking only the newest line let a
  windowless "premium" snapshot mask the "codex" pool sitting at 100% (live
  2026-07-21). Its credits gate requires *usable* credits (`unlimited` or a
  positive `balance`) — a bare `has_credits: true` with `balance: null` is
  reported on team plans whose seat is hard-blocked, and failing open on it
  routed four consecutive conversations to a dead seat. Tests:
  `usage::tests` (pure, both providers) +
  `usage_cordon_excludes_advertises_and_redirects` (enforcement, via
  `run_test_shared`).
- **Dynamic preference scaling** (`availability_preference.*`; same poll +
  client hints): `agents[].preference` is the *static* base — `eligible_views`
  computes the effective preference as `preference × plan_headroom`, minus
  `overage_penalty` when the seat is past its cap but routable via
  overage/credits (spending real money). That keeps the router on whichever
  seat still has FREE plan budget among comparable candidates; a saturated
  seat with no overage headroom is a cordon, never a penalty. Availability
  sources: the usage poller (`set_polled_availability`, wholesale per cycle)
  and the `router-acp/availability_hint` extension notification
  (`apply_availability_hint` — session-less, consumed in `on_catch_all`,
  per-agent TTL `hint_ttl_secs`; a fresh hint outranks the poll, e.g. Kory
  Code's live per-minute view). Disclosure: signed `pref` term in the `auto`
  reason string and `details.availability`
  (`[{candidate,plan_headroom,on_overage,source}]`) on the pin metadata.
  Tests: `usage::tests` (availability + hints), `headroom::tests`
  (hint TTL/fallback), `auto::tests::overage_penalty_prefers_the_free_seat`.
- Don't advertise ACP capabilities (list/load/resume/close/delete) unless at
  least one downstream supports them and the router implements the full
  remap path.
- **Prompt directives**
  (`[router: candidate=…|prefer=…|switch=…|strategy=…|exclude=…|label=…]`)
  are located anywhere in the first text block by finding `[router:` and
  **bracket-matching** to the closing `]` (depth-tracked, so nested `[1m]`
  model ids work) — NOT a per-line/ends-in-`]` parse, which broke on goose's
  `<turn-context>` preamble, on inline `[router: …] task` (directive + task on
  one line), and on goose appending text after the tag (all regression-tested
  in `directive_tests`). Only the `[router:…]` span is stripped (surrounding
  preamble + task preserved, outer whitespace trimmed); a bare directive with
  no task is allowed (post-pin `on_prompt` synthesizes a continuation prompt so
  the switched model responds; pre-pin it errors). Fail loudly when invalid.
  `candidate`/`prefer`/`strategy`/`exclude`/`label` are pre-pin only
  (ignored-with-notice post-pin); `exclude`/`label` persist on the session
  (incl. failover). `prefer` is a *soft* candidate (front of the ranked chain
  if eligible, else graceful fallback — see `pin_session`). `switch` is the
  one **mid-session** directive: it re-pins a live session onto another model
  via `switch_pin` (summarize on the current model → open fresh downstream →
  seed the summary into the next prompt → close old). They exist because CLI
  clients can't set ACP config options.
- **`model:` shorthand** — a prompt beginning (after any `<turn-context>`
  preamble) with `<ref>:` is sugar for `switch=`/`candidate=`.
  `split_model_shorthand` extracts the leading token; `resolve_candidate_ref`
  maps it to the best eligible candidate (exact `agent/model`, bare model id,
  family/prefix glob, or suffix-less id → highest quality on ambiguity).
  **Resolution is the gate**: an unresolved token (`Note:`, `http:`) is left as
  prose, never stripped. Runs only when no `[router:]` directive is present
  (mutually exclusive). Post-pin → `pending_switch`; pre-pin → `candidate_override`.
  Regression-tested in `model_shorthand_switches_mid_session_…` and
  `model_shorthand_splits_token_and_task`.
- **Mid-session model switching** (`switch_pin`) has three triggers, all
  going through the same summarize-and-re-pin primitive: the explicit `switch=`
  directive; **auto-upgrade** (post-turn `update_confidence_and_maybe_upgrade`
  computes `confidence = pinned_quality − struggle`; below
  `auto_upgrade.confidence_threshold` it queues an upgrade to the best
  strictly-more-capable eligible candidate, `auto_upgrade.enabled` gates it);
  and **`skill_routing`** (a prompt invoking a configured skill pattern forces
  the session onto that skill's candidate globs). `detect_skill_route` strips
  code spans (`strip_code_spans`) before matching so a skill *named* in
  backticks/examples doesn't count as invoking it — **LESSON (hickory-ai6):** a
  feature-list prompt describing an autocomplete for `` `/ship-pr` `` matched the
  raw substring, pinned opus via skill_routing, set `explicit_routing`, and thus
  silently suppressed auto-orchestration (the disclosure only said "explicitly
  selected via router.candidate", hiding that skill_routing did it — now a skill
  steer emits its own `notify_user` line). Struggle accrues from
  MaxTokens/Refusal stop reasons and ≥3 in-turn tool failures (counted in the
  Primary relay). The summary turn is captured (`capturing_summary`) and not
  relayed; the fully-framed handoff block is prepended once via
  `pending_context` (the send side no longer wraps it). **Handoff fallback**:
  `switch_pin` reads the pin directly (not `pinned_route`, so a dead old
  process doesn't abort the switch); if the outgoing model has no live conn, or
  the summary request errors / returns <20 chars, it falls back to
  `transcript_from_logs` (reconstructs a truncated transcript from
  `session_log` — user/agent turns only, recent-first budget) framed via
  `frame_transcript`. The disclosure states which path was used. Regression-tested
  in `switch_directive_hands_off_…`, `switch_falls_back_to_log_transcript_when_summary_fails`,
  `low_confidence_pin_auto_upgrades_…`, `auto_upgrade_disabled_…`, `skill_routing_switches_…`.
- **Auto-orchestration** (`orchestration.*`, off by default): the prompt tail
  now runs in an async `dispatch_prompt` task (spawned from `on_prompt`) so
  `tickets::enrich_prompt` executes FIRST — orchestration detection and
  classification see the ticket-ENRICHED prompt ("Fix HAI-1234" routes on the
  ticket's real content). `dispatch_prompt` calls `maybe_trigger_orchestration`
  (returns `bool`) when `!explicit_routing` (a `[router:]` directive or `model:`
  shorthand sets `explicit_routing` and suppresses it); an
  `orchestrate:`/`orchestrator:` prompt prefix (reserved tokens in the shorthand
  tokenizer) FORCES it, bypassing every gate including `enabled` and list
  detection. **Precedence:** it runs BEFORE `skill_routing`, and
  `skill_routing` is gated on `!orchestrating_now` — so a multi-part task list
  *always* orchestrates even if it names a skill; the planner decides when to
  invoke that skill (end-of-work skills like shipping run last, per the injected
  protocol's step 5). Skill routing only fires for a skill invocation that is
  NOT a multi-part task. It is ALSO suppressed when the list answers the model's
  own questions: `previous_turn_solicited_answers` inspects the prior agent turn
  (`s.turn_output`, still the previous turn at `on_prompt` time — cleared later in
  `send_prompt_with_failover`) for question marks / decision phrases / an
  enumerated agent list. `tasklist::detect_task_list` recognizes a multi-part list;
  above `min_items` it sets `s.orchestrating = true`, steers pre-pin
  (`candidate_override`) or switches post-pin (`pending_switch`) to the best
  eligible `planner` glob, and queues `build_orchestration_instructions` into
  `s.pending_orchestration`. That one-shot block is prepended (before any switch
  `pending_context` handoff) in `send_prompt_with_failover`'s `effective_prompt`.
  `orchestrating` relaxes the delegate pool (`run_delegate_task` /
  `delegate_server_entry`) from cheaper-only to any-eligible so the cross-lineage
  reviewer is routeable — this is the ONLY router-level change; the pipeline
  itself is the planner following the injected protocol with `delegate_task` +
  the multi-turn `keep_open`/`delegate_followup`/`delegate_close` tools. It
  implements the plan → delegate → cross-lineage review → submit pipeline
  in-process (the former goose `orchestrate.yaml` recipe was removed — the router
  owns this now), working from any ACP client.
  `close_live_delegates_for` reaps kept-open sub-sessions on
  session/close|delete. `maybe_trigger_orchestration` also sets
  `run_label = "orchestrate"` (so the planner + its delegate rows group) and
  resolves explicit **different-lineage** reviewer candidate ids via
  `resolve_reviewers`. **Lineage = company, not agent name**: compared via
  `agent_lineage` (the `agents[].lineage` config tag, defaulting to the agent
  name) — so the same `reviewer` glob list yields the opposite company of
  whoever planned, and two agents backed by one vendor (tagged with the same
  `lineage`) are never each other's reviewer. Configured `reviewer` globs are
  restricted to different-lineage candidates, else any other-lineage candidate;
  injected into the protocol. Regression: `reviewer_prefers_opposite_lineage_…`
  (symmetry) + `same_company_agents_share_a_lineage_for_review`.
  Tests: `orchestration_*` in `tests/protocol.rs` + `tasklist::tests`. Do NOT
  `include_str!`-style couple this to goose — router-acp still has no notion of
  recipes; it only detects lists and drives delegation.
  **LESSON (shipped bug):** the first live run pinned the fable planner but it
  used claude-agent-acp's **built-in `Task` sub-agent tool** (haiku subtasks,
  opus review) instead of the router's `delegate_task` — so there were NO
  `parent_session_id` rows, the review stayed on the planner's own lineage, and
  nothing was `run_label`led. The native sub-agent tool spawns in-lineage and is
  invisible to the router; router-acp cannot remove it (it's the adapter's, not an
  MCP server). The only lever is the injected protocol, which now **explicitly
  forbids** `Task`/`dispatch_agent`/`spawn` and **mandates** `delegate_task` with
  the concrete cross-lineage reviewer id. This is inherent to the prose-instruction
  approach: a model that ignores the ban silently degrades to same-lineage,
  unobservable orchestration. (Confirmed unfixable at the transport layer:
  ACP `NewSessionRequest` has no tool-suppression field, and `Task` is a native
  adapter tool, not a router-injected MCP server — so the router *cannot* remove
  it.) **Degradation is now detected + surfaced**: `is_native_subagent_tool`
  (matches `_meta.claudeCode.toolName`/title against `Task`/`dispatch_agent`/…,
  never `delegate_*`) fires in `handle_downstream_dispatch`; in an orchestrating
  session it warns once/turn (`turn_native_subagent_warned`) and increments the
  persisted `native_subagent_calls`. **Observability** (added because the first
  evaluation couldn't answer "is this helping"): real `cost_usd` is captured from
  `usage_update.cost` (primary in `log_downstream_event`; delegate in the
  `DownstreamRoute::Delegate` arm, attributed to the `{parent}::delegate-{sid}`
  row); `compute_ms` times each model turn; `git_head` tags the run at pin. The
  `router-acp report` CLI summarizes runs (planner vs delegate cost, delegate
  count, cross-lineage-review present, degraded%). Token *counters*
  (`tokens_*`) remain text-estimates and under-count badly — prefer `cost_usd`
  and `context_used` for cost, `compute_ms` for time (never `updated_at −
  created_at`, which is dominated by user idle).
- **`escalation` router** (start cheap, escalate on *observed* difficulty — not
  a prompt guess): `EscalationStrategy::rank` pins the cheapest capable candidate
  (or, if `initial_router` is set, delegates the starting pick to that strategy —
  built via `make_strategy` recursion in `mod.rs`, guarded against `escalation`
  by config validation). Escalation is runtime via `switch_pin`. Relay-side
  signals (all in `handle_downstream_dispatch`, deduped per turn by
  `turn_counted_tools`/`turn_failed_tools`; `classify_tool` maps each tool frame
  to Investigation/SideEffect/Defer using kind + `is_read_only_command`
  (denylist; strips `2>/dev/null`-style redirects) + `is_read_only_mcp`):
  **read-volume** (`note_investigation`, gated on `!turn_side_effect` — a clean
  pre-side-effect replay), **tool-call volume** (`note_tool_activity` on
  `turn_tool_calls ≥ escalate_after_tool_calls`, NOT side-effect-gated — the
  robust signal for edit/Bash-heavy turns), **tool-failure churn**
  (`note_tool_failure`, not gated). All three set `escalation_requested` + cancel
  the in-flight turn; the failover loop `switch_pin`s and replays (transcript
  handoff = *continue*, not blind replay, which is why the post-side-effect
  triggers are safe). **post-turn** (`escalation_post_turn`) handles
  max-tokens/refusal stops via `pending_switch`. `escalation_path` = `ladder`
  (`min`-quality candidate above current) or `leap` (`max`); one-way, capped by
  `max_escalations`; loop budget `max(failover.max_attempts, max_escalations+1)`.
  **LESSON (a shipped bug):** the read-volume trigger under-fired in production
  because real adapters do investigation via Bash (`execute`) and MCP (`other`),
  not `read`-kind tools, and the first side-effecting tool latched the window.
  The volume/failure triggers + read-only-command classification fix it. Tests
  MUST drive the real `tool_call` `session/update` path (mock `TOOL:` directive),
  not `fs/read_text_file` requests — an fs-only test passed while production
  failed. Tests: `escalation_*` in `tests/protocol.rs` (incl.
  `…on_tool_call_volume_despite_side_effects`, `…read_only_bash_counts…`) +
  `escalation_signal_tests` + `strategies::escalation` units.
- goose sends `session/set_mode` immediately after `session/new` (pre-pin)
  and treats an error as fatal: pre-pin modes are deferred and applied at
  pin (via per-agent `mode_map` translation, then exact id match); post-pin
  unknown modes are answered OK-with-warning, never errored.

## Test infrastructure

`tests/protocol.rs` drives the real router in-process (`serve_shared` over a
`Channel::duplex()`) against `mock-agent` subprocesses; the harness
(`run_test`) provides a scripted ACP client (auto-approves permissions,
serves fs reads, collects `session/update`s). Mock behavior is env-driven:

- `MOCK_NAME`, `MOCK_MODELS` (model-selector values; first = current)
- `MOCK_AUTH_REQUIRED`, `MOCK_FAIL_NEW_AFTER=<n>`, `MOCK_IGNORE_SET_CONFIG`
- `MOCK_EXIT_AFTER_INIT`, `MOCK_EXIT_ON_PROMPT`
- `MOCK_FAIL_PROMPT_MSG` (+ `MOCK_FAIL_PROMPT_TIMES`, `MOCK_FAIL_PROMPT_AFTER=<n successes first>`)
- `MOCK_CAPS_IMAGE`, `MOCK_SUPPORTS_LIFECYCLE`, `MOCK_SESSION_MODES`
- `MOCK_LOG=<path>` — JSONL event log tests assert against

Prompt-text directives the mock obeys: `PERM`, `READFILE:<path>`,
`SLEEP:<ms>` (cancel-aware), `TITLE:<t>` (emits session_info_update),
`DELEGATE:<task>` (spawns and drives the delegate MCP server),
`CHUNK_THEN_EXIT` (output then crash — must NOT fail over). Delegation tests
set `ROUTER_ACP_HELPER_EXE=env!("CARGO_BIN_EXE_router-acp")` because
`current_exe()` is the test binary.

Model ids in tests are chosen to hit score-table patterns (`haiku`,
`sonnet`, `opus`, `claude-fable-5`, `gpt-5.4-mini`, `gpt-5.5`) — renaming
them changes routing outcomes. When asserting on state timestamps remember
they have **second** resolution; `StateFile::upsert` prunes against the real
clock, so unit tests that backdate entries must insert into
`state.sessions` directly.

Handlers must stay thin (they block the connection's dispatch loop): spawn
real work via `cx.spawn`, await with `block_task` only inside spawned
tasks. Plain closures returning `async move` blocks satisfy the SDK's
`AsyncFnMut` bounds; annotate the `cx` parameter's type when inference
fails.

## The user's live deployment (this machine)

- goose ≥ 1.41 consumes the router through the **`pi-acp` provider slot**: a
  shim at `~/.config/router-acp/bin/pi-acp` (found via `GOOSE_SEARCH_PATHS`
  in `~/.config/goose/config.yaml`) execs
  `~/.cargo/bin/router-acp serve --config ~/.config/router-acp/router.yaml`.
  goose's ACP slots spawn fixed binary names; `pi-acp` takes no args (why it
  was chosen), `codex-acp` takes `-c` flags (why it can't be shimmed).
  `claude-acp`/`codex-acp` remain direct. After code changes run
  `cargo install --path . --force` or goose keeps the old binary.
- Real adapters live at `~/nvm/versions/node/v24.16.0/bin/{claude-agent-acp,codex-acp}`
  (nvm-versioned paths — they move on node upgrades). Verified model ids
  (July 2026): claude offers `default, haiku, sonnet, sonnet[1m], opus[1m],
  claude-fable-5[1m]`; codex offers `gpt-5.4-mini, gpt-5.4, gpt-5.5`.
  Claude modes: `auto, default, acceptEdits, plan, dontAsk,
  bypassPermissions`; codex modes: `read-only, auto, full-access`. To
  re-discover ids, declare a bogus model and read the `available=[…]`
  warning under `RUST_LOG=router_acp=debug`.
- The user prefers Claude (bigger plan): `preference: 0.05` on the claude
  agent, `cost_quality_tradeoff: 3`. History: routing once sent an hour-long
  investigation to `gpt-5.4-mini` because the broad `*gpt-5*` score pattern
  shadowed `*mini*` — score-table patterns are first-match-wins; keep
  specific before broad (regression-tested).

## When you change behavior, update the docs

- `README.md` — feature overview + config reference table
- `ROUTERS.md` — user-facing strategy explanations
- `GOOSE.md` — the user's actual install/runbook (mode handling, failover
  visibility, tuning table)
- `examples/router-full.yaml`, `examples/router-preferred.yaml` — annotated configs
- `ORCHESTRATION.md` — the router-native auto-orchestration pipeline
  (plan → delegate → cross-lineage review → submit; there is no longer a goose recipe)
- `PLAN.md` — the active follow-on plan (the original build spec was
  completed and removed); post-spec deviations (failover, complexity-scaled
  tradeoff, preference, directives) are documented in README/AGENTS.
