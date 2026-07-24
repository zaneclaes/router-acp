# Fable expensive-use benchmark

**Run date:** 2026-07-23, updated 2026-07-24 (America/New_York)
**Source:** dev workstation SQLite history plus a live Claude/Anthropic replay
**Status:** history complete; Claude available and the per-request routing now
verified live on the Anthropic wire (a real detector bug was found and fixed);
savings reframed **session-wide**; a fully paired post-fix treatment run was not
recaptured (local goose headless driver went unreliable — see below)

## Executive result

The workstation history confirms that Fable is the dominant cost center:

| Metric | Value |
| --- | ---: |
| Retained sessions | 178 |
| Total recorded cost | $2,052.944706350 |
| Fable sessions | 108 |
| Fable cost | $835.660809000 (40.7055% of total) |
| Fable delegate sessions | 100 |
| Fable delegate cost | $728.549217750 |
| Non-Fable delegate sessions | 27 |
| Non-Fable delegate recorded cost | $32.739038100 |
| Fable share of recorded delegate cost | 95.6995% |

On 2026-07-24 Claude was available again (5-hour window 3%, weekly 53%, Fable
weekly 77% — only the org *overage* pool sat at 100%, which on-plan requests do
not touch). The paired treatment was retried through Claude, and that retry is
where the interesting result is: it surfaced a **real cross-provider bug** in
the per-request router that had been silently disabling all Anthropic demotion.
See "Claude parity" below. This report still makes **no fully-paired measured
treatment savings claim** — but for a different reason than the caps, and it now
reframes the projected savings **session-wide** as requested.

The exact class-based price-ratio projection for the 100 Fable delegates is:

| Historical Fable cost | Projected down-tier cost | Projected saving |
| ---: | ---: | ---: |
| $728.549217750 | $280.874463775 | $447.674753975 (61.4474%) |

That row is a counterfactual only. It applies the configured price ratios to
the original Fable cost and assumes equal token use: routine classes to Sonnet
at 0.30x, harder implementation classes to Opus at 0.50x.

## Fable cost distribution

| Delegate class | Sessions | Recorded cost | Mean cost/session | Proposed target |
| --- | ---: | ---: | ---: | --- |
| Ops | 39 | $261.929032000 | $6.716129026 | Sonnet |
| BugFix | 28 | $204.750623250 | $7.312522259 | Opus |
| UiTweak | 12 | $102.364559000 | $8.530379917 | Sonnet |
| Feature | 6 | $39.056480000 | $6.509413333 | Opus |
| Refactor | 4 | $38.366634000 | $9.591658500 | Opus |
| Writing | 3 | $37.194811500 | $12.398270500 | Sonnet |
| Algorithms | 4 | $29.374755000 | $7.343688750 | Opus |
| Research | 4 | $15.512323000 | $3.878080750 | Sonnet |
| **Total** | **100** | **$728.549217750** | **$7.285492178** | |

Both selected delegate routing reasons had the same failure mode: the configured
cost-quality tradeoff was complexity-scaled to zero, leaving Fable to win on
quality alone.

## Claude parity: making the per-request router work on the Anthropic wire

The task was to confirm the same per-request down-tier system that produced the
Codex benchmark (`../RESULTS.md`, 19.75% session-wide) also works for Claude.
Two Fable-pinned goose sessions were driven through a router configured with
`llm_proxy.protocol: anthropic` (config: [`claude-baseline.yaml`] and
[`claude-treatment.yaml`] under `/private/tmp/router-acp-fable-benchmark`).

**What worked immediately (live).** The proxy transparently interposed Claude
Code's OAuth `/v1/messages` traffic: the baseline session captured 8 real
inference rows, `protocol=anthropic`, all HTTP 200, with correct Anthropic usage
accounting (uncached `input_tokens`, `cache_read_input_tokens`,
`cache_creation_input_tokens`, `output_tokens` → per-model USD). The
demotion-target model ids are valid on the subscription endpoint (a direct OAuth
`/v1/messages` probe returned 200 for `claude-sonnet-5` and `claude-opus-4-8`).

**The bug.** The treatment session — configured to demote routine turns to
Sonnet — **never demoted**: every one of its rows logged `routing_event=steady`,
reproducibly, across multiple runs. Root cause: `inspect_request` classified
routine/difficulty from only the **last 24 KB of the serialized request**. A
Claude `/v1/messages` request carries a large `system` + `tools` prelude and the
entire conversation history in `messages`, so the current turn's `tool_result`
sits far outside that tail — `has_tool_result`/`routine_tool` read false and the
routine streak never advanced. The identical detector worked for Codex only
because the Responses wire places the tool output near the serialized tail.

**The fix.** `inspect_request` now locates the latest tool-result content and
the invoking tool name *structurally* (`latest_tool_context`, walking the JSON
the way `test_fingerprint` already did), and scopes difficulty to that latest
block so a stale historical `is_error` no longer pins escalation forever. This
is wire-format-agnostic and fixes Claude without changing Codex behavior.
Coverage: a regression test reproduces the exact Claude shape (asserts the
tool_result is NOT in the 24 KB tail yet is still read as routine); the existing
end-to-end `protocol: anthropic` proxy test proves demotion + model rewrite +
accounting + the 4xx→pinned fallback. All 205 lib + 78 protocol tests pass,
clippy clean, and the fixed binary is installed at `~/.cargo/bin/router-acp`.

The one thing NOT recaptured is a full paired post-fix treatment run through
goose: after several runs the local headless goose driver stopped starting
sessions (empty output, 0-byte DB) — an isolated local-driver problem, not the
provider cap. The demotion mechanism is proven by the tests above; the numbers
below are therefore a **projection applied to the real captured baseline
tokens**, not a paired measurement.

## Savings are session-wide, not per-tool

The captured baseline session totals **$0.6820** across its 8 Fable requests.
Applying the treatment's demotion policy (routine streak 2, dwell 2 → the first
two turns stay on Fable, the routine remainder demotes to Sonnet) to those exact
captured token counts:

| Framing | Cost | Saving vs Fable |
| --- | ---: | ---: |
| Fable, whole session (baseline) | $0.6820 | — |
| Demoted tool-use turns only (Fable→Sonnet on those turns) | — | **70.0%** |
| **Session-wide, naive** (cache assumed to carry across the switch) | $0.2974 | **56.4%** |
| **Session-wide, cache-realistic** (re-prime on the switch turn) | $0.5602 | **17.9%** |

The 70.0% figure is the trap: it is the saving *on the turns that demoted*, and
it ignores the non-demotable turns entirely. Expressed session-wide the honest
number is lower.

It is lower still because of an **Anthropic-specific cache effect**. Prompt
caching is keyed per model, so demoting mid-session forfeits Fable's warm cache;
the switch turn must re-prime the ~79k-token prefix on Sonnet at the cache-*write*
rate ($3.75/Mtok, 12.5× the read rate), a one-time ~$0.30 that nearly erases the
gain on this short 8-turn session. That cost amortizes only over a long demoted
stretch — which is exactly the shape of the expensive historical Fable delegates
(the sampled BugFix session ran 531 tool calls). So per-request demotion is a
session-wide win on the long, routine-heavy sessions that dominate Fable spend,
and roughly break-even on short ones. Reporting it per-tool would hide both
effects.

## Expensive samples

Two high-cost sessions were selected to represent the dominant implementation
and review shapes:

| Workload | Class | Cost | Context | Tool calls | Compute |
| --- | --- | ---: | ---: | ---: | ---: |
| Frontend session-state debugging | BugFix | $26.254949500 | 268,795 | 531 | 2,476.929s |
| Cross-lineage deployment review | Ops | $25.927204500 | 228,080 | 491 | 911.559s |
| **Combined** | | **$52.182154000** | | **1,022** | **3,388.488s** |

The original Fable implementation reported passing typecheck, ESLint, 152/152
targeted relay tests, and 392/393 full relay tests; the sole failure was named
as pre-existing in the brief. The original Fable reviewer returned `APPROVED`
after actionlint, Ruff, manifest validation, and four passing Python suites.

Applying the proposed whole-session targets to only these two recorded costs
gives:

| Workload | Fable baseline | Target and ratio | Projected cost | Projected saving |
| --- | ---: | --- | ---: | ---: |
| State-machine BugFix | $26.254949500 | Opus, 0.50x | $13.127474750 | $13.127474750 |
| Read-only Ops review | $25.927204500 | Sonnet, 0.30x | $7.778161350 | $18.149043150 |
| **Combined** | **$52.182154000** | | **$20.905636100** | **$31.276517900 (59.9372%)** |

This is also a price-ratio projection, not a treatment measurement.

## Deterministic replay

Private production dependencies were replaced with two checked-in fixtures
preserving the sampled task shapes:

- [`state-machine`](state-machine): optimistic busy reconciliation, pending
  session id promotion, attachment preservation, at-most-once dispatch, and
  partial-history folding.
- [`review`](review): a read-only deployment graph review with five planted
  blocking invariant violations.

The capture-only local baseline pinned every request to GPT-5.5:

| Workload | Requests | Input tokens | Cache-read tokens | Output tokens | Input + output | API-equivalent cost |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| State-machine implementation | 25 | 561,403 | 511,360 | 9,647 | 571,050 | $0.795305000 |
| Deployment review | 16 | 298,567 | 271,360 | 3,476 | 302,043 | $0.375995000 |
| **Combined** | **41** | **859,970** | **782,720** | **13,123** | **873,093** | **$1.171300000** |

All 41 inference rows returned HTTP 200. One row retained complete token/cost
usage while also recording the adapter-stream disconnect diagnostic, so it is
included.

Quality gates passed:

| Workload | Gate |
| --- | --- |
| State machine | 7/7 public Node tests plus 15/15 independent checks |
| Review | Exact five blocking codes, correct files, positive line numbers, sorted output |
| Mutation scope | Only the three allowed state modules changed; review wrote only `review.json` |

The treatment database contains one zero-token HTTP 429 row and no successful
inference. It is excluded from all cost/token calculations.

## Observational controls

The same workstation window contains 27 non-Fable delegates:

| Model | Sessions | Recorded cost |
| --- | ---: | ---: |
| Opus 1M | 17 | $28.713101250 |
| Sonnet | 2 | $4.025936850 |
| GPT Sol | 8 | $0.000000000 |

Sol cost was not accounted in that build, so the zero is not economically
meaningful. The Opus/Sonnet tasks were also shorter and were selected
differently; their lower mean cost is not a causal treatment result.

As a weak quality proxy, 13/100 Fable delegates had multiple user/delegate
turns versus 8/27 non-Fable delegates. That is 13.0000% versus 29.6296%, but
follow-ups can be planned review/fix rounds rather than rejections. The result
does not establish either quality parity or regression.

## Method

The read-only workstation exports are retained outside the repository:

```text
/private/tmp/router-acp-fable-history-2026-07-23.json
/private/tmp/router-acp-nonfable-delegates-2026-07-23.json
```

The live workstation database was opened with SQLite URI `mode=ro`. No
workstation checkout, router config, state row, or service was changed.

The Fable counterfactual uses:

```text
Sonnet projection = original Fable cost * 0.30
Opus projection   = original Fable cost * 0.50
```

Class mapping:

```text
Sonnet: Ops, UiTweak, Research, Writing
Opus:   BugFix, Feature, Refactor, Algorithms
```

Local baseline accounting query:

```sql
SELECT
  s.cwd,
  r.model,
  COUNT(*) AS requests,
  SUM(r.tokens_input) AS input_tokens,
  SUM(r.tokens_cache_read) AS cache_read_tokens,
  SUM(r.tokens_output) AS output_tokens,
  SUM(r.cost_usd) AS cost_usd
FROM llm_requests AS r
JOIN sessions AS s USING (router_session_id)
WHERE r.endpoint LIKE '%/responses'
GROUP BY s.cwd, r.model
ORDER BY s.cwd, r.model;
```

Quality commands:

```sh
npm test
node benchmarks/llm-proxy/fable-sample/state_machine_quality_gate.mjs \
  /private/tmp/router-acp-fable-benchmark/baseline/state-machine
PYENV_VERSION=3.12.13 \
  python benchmarks/llm-proxy/fable-sample/review_quality_gate.py \
  /private/tmp/router-acp-fable-benchmark/baseline/review
```

The live Claude replay used the same driver as the Codex benchmark: a goose
session pinned to `claude/claude-fable-5[1m]` through a router with the Anthropic
proxy enabled, spawned via the existing `pi-acp` shim with a `ROUTER_ACP_CONFIG`
override (the shim was left byte-identical to its original afterward). Baseline
used `routine_streak: 100000` (capture-only); treatment used `routine_streak: 2`,
`minimum_dwell_requests: 2`. Session-wide accounting query:

```sql
SELECT model, routing_event, tokens_input, tokens_cache_read,
       tokens_cache_write, tokens_output, cost_usd
FROM llm_requests ORDER BY started_at;   -- every request in the session
```

The demotion-target ids were checked directly against the subscription endpoint:

```sh
curl -s https://api.anthropic.com/v1/messages \
  -H "authorization: Bearer <oauth>" -H "anthropic-version: 2023-06-01" \
  -H "anthropic-beta: oauth-2025-04-20" \
  -d '{"model":"claude-sonnet-5","max_tokens":16,
       "system":[{"type":"text","text":"You are Claude Code, Anthropic'\''s official CLI for Claude."}],
       "messages":[{"role":"user","content":"say OK"}]}'   # → HTTP 200
```

## Decision

Two conclusions, one measured and one projected.

1. **The per-request router now works on the Anthropic wire (measured/tested).**
   The Codex-only detector gap that suppressed all Claude demotion is fixed and
   regression-tested; live capture confirms interception and accounting. This
   was the actual blocker to "the same systems working for Claude," and it is
   resolved.

2. **Fable remains the right down-tier target, but savings must be read
   session-wide (projected).** The history still supports prioritizing Fable
   (100 delegates, $728.55). Expressed session-wide against real captured
   tokens, per-request demotion to Sonnet saves ~56% naively but only ~18% once
   the mid-session cache re-prime is charged on a short session — the gain grows
   toward the per-tool 70% as the demoted stretch lengthens, which is precisely
   the profile of the expensive long Fable delegates. Do NOT quote the 70%
   per-tool figure as the session saving.

Next: recapture a fully paired post-fix treatment run once the local goose
headless driver is healthy (or drive it from an interactive goose, where it
works), to replace the projection in "Savings are session-wide" with a measured
paired number and to confirm no quality loss on the `state-machine`/`review`
fixtures. Keep Fable downrouting guarded until then.
