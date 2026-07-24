# Per-request LLM routing benchmark

**Run date:** 2026-07-23 (America/New_York)
**Host:** Zane's laptop, Goose -> router-acp -> codex-acp -> ChatGPT OAuth
**Result:** quality gates passed; API-equivalent cost fell **19.7472%**

## Executive result

| Metric | Baseline | Treatment | Change |
| --- | ---: | ---: | ---: |
| API-equivalent cost | $1.297324000 | $1.041138300 | **-$0.256185700 (-19.7472%)** |
| Inference requests | 62 | 61 | -1 (-1.6129%) |
| GPT-5.5 requests | 62 | 36 | -26 |
| GPT-5.4 Mini requests | 0 | 25 | +25 |
| Input tokens, including cached input | 1,298,354 | 1,292,976 | -5,378 (-0.4142%) |
| Cache-read tokens | 1,232,128 | 1,123,200 | -108,928 (-8.8406%) |
| Output tokens | 11,671 | 23,247 | +11,576 (+99.1860%) |
| Input + output tokens | 1,310,025 | 1,316,223 | **+6,198 (+0.4731%)** |

This benchmark shows a cost reduction, not a raw-token reduction. Mini emitted
substantially more output on the maintenance implementation. Its lower token
price still reduced total cost, but the exact token result is a 0.4731%
increase.

## Quality result

No quality loss was observed by the benchmark's deterministic gates:

| Workload | Baseline | Treatment | Equality evidence |
| --- | --- | --- | --- |
| Maintenance implementation | 20/20 public unit tests and 508/508 independent property/integration checks | 20/20 public unit tests and 508/508 independent property/integration checks | Same initial tree `53c99555a790b5bdda9080138c3518c045c07bac`; neither run changed tests or `TASK.md`; both changed only the five allowed TODO files |
| CI watcher aggregation | Exact 20-snapshot semantic gate passed | Exact 20-snapshot semantic gate passed | Canonical JSON SHA-256 was `8c96faf92b242fe28e1fa7a5a236ee0734894650a14c9020ac3b224bf94ecab6` for both |

"No quality loss" here means equality on these executable gates. Two paired
workloads are not enough to establish equal production pass rates with
statistical confidence.

## Workload results

| Workload | Baseline cost | Treatment cost | Cost reduction | Baseline tokens | Treatment tokens | Token change | Treatment model mix |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Maintenance with tests and an error/recovery trace | $0.790533000 | $0.703343250 | **$0.087189750 (11.0292%)** | 758,434 | 783,516 | +25,082 (+3.3071%) | 24 GPT-5.5, 9 Mini |
| Routine CI watcher | $0.506791000 | $0.337795050 | **$0.168995950 (33.3463%)** | 551,591 | 532,707 | -18,884 (-3.4236%) | 12 GPT-5.5, 16 Mini |
| **Combined** | **$1.297324000** | **$1.041138300** | **$0.256185700 (19.7472%)** | **1,310,025** | **1,316,223** | **+6,198 (+0.4731%)** | **36 GPT-5.5, 25 Mini** |

The watcher saved more because it stayed routine after demotion. The
maintenance run correctly escalated from Mini to GPT-5.5 after an error signal,
held the six-request verdict through verification, expired it, and later
demoted routine cleanup again.

## Tool attribution

SQLite associated every benchmark tool call with the model serving that point
in the trace:

| Variant | Workload | Model | Tool calls |
| --- | --- | --- | ---: |
| Baseline | Maintenance | GPT-5.5 | 27 |
| Treatment | Maintenance | GPT-5.5 | 20 |
| Treatment | Maintenance | GPT-5.4 Mini | 7 |
| Baseline | Watcher | GPT-5.5 | 24 |
| Treatment | Watcher | GPT-5.5 | 10 |
| Treatment | Watcher | GPT-5.4 Mini | 13 |

`active_tool_calls` contained zero rows after completion in both databases, as
expected. During the runs it exposed the current tool and selected model.

## Method

The laptop's existing history was used to choose representative traces. It had
141 retained sessions and 61,490 recorded tool calls for 300 user prompts,
roughly 205 tool calls per prompt. It could not be used as the treatment
dataset because it predates `llm_requests`.

Fresh paired worktrees and SQLite databases were used:

- Baseline database: `/private/tmp/router-acp-benchmark/baseline.db`
- Treatment database: `/private/tmp/router-acp-benchmark/treatment.db`
- Maintenance fixture and prompt: [`fixture`](fixture),
  [`PROMPT.md`](PROMPT.md)
- Independent maintenance gate: [`quality_gate.py`](quality_gate.py)
- Watcher fixture and prompt: [`watcher-fixture`](watcher-fixture),
  [`WATCHER_PROMPT.md`](WATCHER_PROMPT.md)
- Independent watcher gate:
  [`watcher_quality_gate.py`](watcher_quality_gate.py)

Both variants pinned the ACP session to `codex/gpt-5.5`. Both passed every
provider request through the proxy for identical accounting.

| Setting | Baseline | Treatment |
| --- | ---: | ---: |
| `routine_streak` | 10,000 (capture-only) | 3 |
| `minimum_dwell_requests` | 12 | 12 |
| `verdict_ttl_requests` | 6 | 6 |
| `verdict_ttl_secs` | 900 | 900 |
| `context_window_fraction` | 0.9 | 0.9 |

The treatment's first demotion occurred at request 13, after the full
12-request dwell. Difficulty bypassed dwell and escalated immediately.

Pricing was fixed before the runs:

| Model | Input / Mtok | Cached input / Mtok | Output / Mtok |
| --- | ---: | ---: | ---: |
| GPT-5.5 | $5.00 | $0.50 | $30.00 |
| GPT-5.4 Mini | $0.75 | $0.075 | $4.50 |

For OpenAI Responses usage, `input_tokens` includes cached input. Per-request
cost was:

```text
((input - cached_input) * input_rate
 + cached_input * cached_rate
 + output * output_rate) / 1,000,000
```

All 123 inference rows returned HTTP 200. Six rows also contain
`adapter disconnected before the response stream completed` (two baseline,
four treatment), but each retained complete input/output/cache usage, the
agents received the result, and all quality gates passed. They remain included
in the totals.

## Reproduction

Installed versions:

```text
goose 1.42.0
router-acp 0.1.0
@agentclientprotocol/codex-acp 1.1.2
codex-cli 0.144.4
```

Quality commands:

```sh
PYENV_VERSION=3.12.13 PYTHONPATH=src \
  python -m unittest discover -s tests -q

PYENV_VERSION=3.12.13 python benchmarks/llm-proxy/quality_gate.py \
  /private/tmp/router-acp-benchmark/{baseline|treatment}

PYENV_VERSION=3.12.13 python benchmarks/llm-proxy/watcher_quality_gate.py \
  /private/tmp/router-acp-benchmark/watcher-{baseline|treatment}
```

Core accounting query (run once per database):

```sql
SELECT
  s.cwd,
  r.model,
  r.routing_event,
  COUNT(*) AS requests,
  SUM(r.tokens_input) AS input_tokens,
  SUM(r.tokens_output) AS output_tokens,
  SUM(r.tokens_cache_read) AS cache_read_tokens,
  SUM(r.cost_usd) AS cost_usd
FROM llm_requests AS r
JOIN sessions AS s USING (router_session_id)
WHERE r.endpoint LIKE '%/responses'
GROUP BY s.cwd, r.model, r.routing_event
ORDER BY s.cwd, r.model, r.routing_event;
```

Tool attribution query:

```sql
SELECT s.cwd, t.model, COUNT(*) AS tool_calls
FROM tool_calls AS t
JOIN sessions AS s USING (router_session_id)
GROUP BY s.cwd, t.model
ORDER BY s.cwd, t.model;
```

## Integration findings and limits

1. **The 50% target failed.** The observed combined saving was 19.7472%.
   The routine watcher reached 33.3463%; the guarded maintenance workload
   reached 11.0292%.
2. **Claude could not be benchmarked successfully.** The proxy intercepted two
   `/v1/messages` calls, but the organization returned HTTP 429 for its monthly
   spend limit.
3. **Codex ChatGPT OAuth needs a custom provider.** `OPENAI_BASE_URL` did not
   interpose inference. Setting `openai_base_url` moved Codex's preferred
   WebSocket to the proxy, but router-acp is HTTP/SSE-only and Codex then fell
   back direct. The successful benchmark used a custom
   `requires_openai_auth=true`, `wire_api=responses` provider via
   `CODEX_CONFIG` and `MODEL_PROVIDER`.
4. **Mini initially could not be selected.** Its score entry had no context
   window, so the policy correctly excluded it. The benchmark adds and tests a
   400,000-token context window.
5. **The relay automation hint was not exercised.** Goose CLI does not expose a
   way to attach `_meta.router_acp.request_hint`; the watcher used the sustained
   routine detector and therefore paid the full 12-request dwell.
6. **Grok and Kimi had no declared down-tier alternatives** in the live config,
   so they could not contribute a treatment model mix.
7. Costs are API-equivalent prices, not marginal subscription invoices.

## Decision

The benchmark passes its deterministic quality guardrail and demonstrates a
real 19.7472% cost reduction, but it does **not** validate the proposed
"frontier quality at half cost" target. Keep the proxy opt-in. Before enabling
it by default:

1. make Codex's ChatGPT-OAuth HTTP provider injection automatic;
2. add relay `_meta` hints so watcher traffic skips dwell;
3. repeat across a larger ticket sample with merge/test outcomes;
4. tune Mini verbosity/reasoning because its output-token count doubled the
   combined benchmark output.
