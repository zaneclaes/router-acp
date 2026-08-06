# Standardize model catalog + score updates for router-acp

## Summary

Add a maintained **model updater** pipeline to [router-acp](https://github.com/zaneclaes/router-acp) that:

1. **Discovers** which models each provider/agent currently exposes (add new releases, drop/retire vanished ones — e.g. opus 4.8 → 5 / fable id renames).
2. **Scores** each model for **quality** (per `TaskClass`) and **cost/scarcity** (`cost_rank` + `pricing`) via a **documented, reproducible procedure** (benchmarks + local telemetry + explicit provisional rules).
3. **Writes** results into the repo surfaces the router actually loads (`data/scores.yaml`, default/example agent configs, docs), then **validates** that preference **ordering** still matches the live design (not bit-identical floats).

Primary validation bar: running the updater against today's catalog should **roughly recreate current settings** — absolute numbers may differ, but general ordering (when haiku vs sonnet vs opus vs fable vs grok is preferred) stays the same.

LLM-heavy steps (mapping new ids → patterns, drafting per-class curves from heterogeneous benchmarks, writing human-readable score comments) should run via a **headless goose session** (`goose run`), not ad-hoc chat.

---

## Context (current system)

Repo checkout on this workstation: `/opt/dev/router-acp` (`origin` = `https://github.com/zaneclaes/router-acp.git`, branch `main` @ ~`48b2ef7`).

### Surfaces an updater must understand

| Path | Role |
|------|------|
| `data/scores.yaml` | Built-in quality table. Embedded at compile time via `include_str!` in `src/candidate.rs`. First-match glob on `"agent/model"`. Overrideable wholesale with config key `score_table:`. |
| `examples/router-preferred.yaml` | Canonical full multi-agent deployment example (claude / codex / grok / kimi). Live-ish model lists, `cost_rank`, `pricing`, orchestration planner/reviewer globs, skill_routing. |
| `examples/router-full.yaml` | Alternate smaller ladder — not the source of truth for production ranks. |
| `src/config.rs` | `ModelConfig { id, display_name?, api_model?, cost_rank, pricing? }`, `AgentConfig.models`, `deny_unknown_fields` everywhere. |
| `src/candidate.rs` | Score table parse/lookup; `TaskClass::ALL`; golden data-layer tests. |
| `src/strategies/auto.rs` | Utility = `quality_weight×quality + cost_weight×quota + preference`; `cost_rank` min-max normalized in surviving pool. |
| `tests/golden.rs` | Routing winners for fixed prompts; exact winner asserts. |
| `GOOSE.md` / `README.md` / `ROUTERS.md` | Operator docs that describe model lists and tuning. |
| `benchmarks/llm-proxy/` | Cost/behavior studies + pass-fail gates — **do not currently feed** `scores.yaml`. |

### Current preferred catalog (snapshot for acceptance)

**claude** (`model_selection: config-option`):  
`haiku` rank 1 · `sonnet` rank 2 · `opus[1m]` rank 4 · `claude-fable-5[1m]` rank 5  

**codex**:  
`gpt-5.4-mini` 1 · `gpt-5.6-luna` 2 · `gpt-5.5` 3 · `gpt-5.6-terra` 4 · `gpt-5.6-sol` 5  

**grok** (`spawn-config`):  
`grok-4.5` rank 5 (code-fast commented)  

**kimi** (`spawn-config`):  
`kimi-k2` rank 2 (thinking commented)  

### Intentional preference ordering (contract)

Encoded by scores + ranks + `auto` defaults in preferred config (`cost_quality_tradeoff: 3`, `min_cost_weight: 0.15`):

| Regime | Expected winner (claude/codex lineup) |
|--------|----------------------------------------|
| Trivial UiTweak / complexity ≈ 0 | `claude/haiku` |
| Small everyday work (complexity ~0.01–0.10) | `claude/sonnet` |
| Moderate Research / Architecture | `claude/opus[1m]` |
| Hard Architecture (gated pool) | `claude/opus[1m]` over fable (cost wins despite slightly lower quality) |
| Hard/long work | never `*mini*` / `*luna*` |
| Cross-lineage / planner pins | globs prefer `*fable*`, `*sol*`, `*grok-4.5*` outside pure `auto` |

**Design note (must document in ticket implementation):** the narrow fable−opus quality gap (~0.02) + `min_cost_weight` means **fable currently does not win pure `auto` goldens**; fable wins via pin / planner / strongest-model paths. Updater must preserve that unless policy is deliberately changed (comment rewrite required).

### Quality table schema (no new keys without Rust change)

```yaml
version: 1
candidates:   # first match wins
  - pattern: "*fable*"          # glob against "agent/model"
    coding_tier: high|medium|low
    coding_percentile: 0.97     # documentation-only today
    context_window: 1000000
    tags: []                    # documentation-only today
    default_quality: 0.87
    quality:
      UiTweak: 0.87
      # exact TaskClass names only:
      # UiTweak, BugFix, Feature, Refactor, Algorithms,
      # Architecture, Research, Writing, Ops, CodingGeneral
```

`ScoreEntryRaw` uses `#[serde(deny_unknown_fields)]` — inventing fields crashes startup.

### Utility / cost semantics

- **`cost_rank`**: ordinal scarcity (1 = cheapest/least scarce … 5 = frontier/scarce). **Not USD.** Seats are flat-rate. `auto` min-max-normalizes within the surviving pool.
- **`pricing`**: separate USD/Mtok for accounting + cache break-even; keep accurate but do not drive ranks alone.
- **Preference**: agent-level tie-break (~0.05–0.1), scaled by availability.

---

## Goal / non-goals

### Goals

- One command (e.g. `./scripts/update_models.py --dry-run`) that produces a reviewable patch for catalog + scores.
- Reproducible quality/cost procedure with provenance comments.
- Validation suite: ordering + margin guards (see Acceptance).
- Goose-backed LLM step for judgment calls (id aliases, provisional curves, comment text).

### Non-goals (v1)

- Automatically merging to main or changing live user configs under `~/.config` without review.
- Replacing operator judgment for scarcity overrides (e.g. grok at rank 5 despite cheap API).
- Building a full multi-benchmark leaderboard product — wire **inputs**, not a new eval farm, unless a thin local gate already exists.
- Changing the `auto` formula / `min_cost_weight` policy.

---

## Implementation plan

### Deliverable layout

```
router-acp/
  scripts/
    update_models.py          # or .sh + python; prefer Python for YAML + JSON
    update_models/
      discover.py             # provider discovery adapters
      score.py                # quality + cost_rank derivation
      write.py                # patch scores.yaml + examples + docs
      validate.py             # ordering / golden checks
      provenance.py           # comment blocks
    recipes/
      update-models-score.yaml  # goose recipe for LLM judgment steps
  docs/ (or README section)
    model-updater.md          # operator runbook (link from GOOSE.md)
  tests/
    # extend golden / add data-order tests as needed
```

No new Cargo binary required for v1 (keep Rust free of network scrape deps). Optional later: `cargo xtask update-models`.

### CLI

```
scripts/update_models.py \
  --config examples/router-preferred.yaml \
  --scores data/scores.yaml \
  --dry-run | --apply \
  --providers claude,codex,grok,kimi \
  --skip-discover | --skip-score | --skip-write \
  --use-goose / --no-goose \
  --report /tmp/model-update-report.md
```

Default: `--dry-run` (write report + unified diffs under `target/model-update/` or `/tmp`, never touch git).

### Phase A — Discover available models

For each configured agent (or `--providers` filter):

| Agent | Discovery mechanism (implement adapters; fail open with clear error) |
|-------|----------------------------------------------------------------------|
| claude | Prefer Anthropic models API / `claude` CLI model list if available; also parse ACP config-option enumeration when an adapter is running. Document exact command after spike. Map API ids ↔ router aliases (`opus[1m]`, `claude-fable-5[1m]`) via `api_model` + display names. |
| codex | Codex model list from CLI or known rollout snapshot; map `gpt-5.x` ids. |
| grok | `grok models` (documented in preferred config). |
| kimi | Account-exposed models from `kimi` config / login artifacts (document path). |

Output artifact: `discovered.json`

```json
{
  "generated_at": "...",
  "agents": {
    "claude": {
      "available": [{"id": "...", "api_model": "...", "display_name": "..."}],
      "configured": ["haiku", "sonnet", "opus[1m]", "claude-fable-5[1m]"],
      "to_add": [],
      "to_remove": [],
      "renames": [{"from": "...", "to": "...", "confidence": 0.9}]
    }
  }
}
```

Rules:

- Never hard-delete a model from examples without a report section; prefer comment-out + `// retired YYYY-MM-DD: reason` for one release when ids vanish.
- New frontier ids default to **disabled (commented)** until scores + pricing exist (match current grok-code / kimi-thinking pattern).
- Renames (e.g. version bumps) are LLM-assisted: goose receives old list + new list and proposes `renames[]` with rationale; human/CI must approve apply.

### Phase B — Standardized quality + cost

#### B1. Quality (→ `data/scores.yaml`)

**Inputs (priority order):**

1. **Local telemetry** (if `state_file` SQLite present): per-model / per-class struggle signals, escalation rates, refusals, max-tokens — from patterns in `benchmarks/llm-proxy/fable-sample/RESULTS.md`.
2. **Published evals as priors** (map into TaskClass):
   - SWE-bench Verified / Aider → BugFix, Feature, Refactor, CodingGeneral
   - LiveCodeBench → Algorithms
   - GPQA / long-horizon agentic → Architecture
   - Terminal-Bench / tool-use → Ops, Research
   - Writing / preference evals → Writing
   - Small-diff success → UiTweak
3. **Anchored normalization** (critical): do **not** min-max within the current candidate set (adding one model would rescore everyone and break goldens). Use fixed anchors, e.g.:
   - `sonnet` coding classes ≈ 0.78–0.80 (hold stable across routine updates)
   - frontier-of-record Architecture ceiling ≈ 0.95–0.97
   - Working band clip **[0.35, 0.97]**, round to 0.01
4. **Compression rule** (policy): same-tier pairs within benchmark noise → force `|Δquality| ≤ 0.02` so `cost_rank` breaks ties (generalizes fable/opus).
5. **Class curve**: frontier models: Architecture ≥ Algorithms ≥ implementation ≥ Research/Writing ≥ UiTweak/Ops; small models invert UiTweak high.
6. **Provisional models** (sol/terra/luna, grok, kimi, gemini): must keep/emit `PROVISIONAL` / `BEST-GUESS` comments with re-benchmark trigger (existing convention).

**Pattern order hygiene:** specific globs before broad (`*mini*` before `*gpt-5*`). Fix latent bug: `*gemini*` must sit **above** `*mini*` because `"gemini"` contains substring `mini` under current glob matcher.

#### B2. Cost / scarcity (→ `models[].cost_rank` + `pricing`)

1. Refresh `pricing` from published API rates (input/output/cache) when discoverable.
2. Compute blended price prior: `input + k·output` with `k` from local output/input ratio (~3) or default 3.
3. Dense-rank into **global ladder 1..5** (do not extend to 6 — rescales `auto` pool norms).
4. Apply **documented scarcity overrides** (YAML sidecar or table in script):
   - Example: `grok-4.5` may remain rank 5 despite cheap API (seat scarcity / cross-lineage reviewer value).
5. Keep ranks **per-family ladder** only if global dense-rank would break preferred ordering goldens; report either way.

#### B3. Goose session for judgment

Use:

```bash
goose run \
  --recipe scripts/recipes/update-models-score.yaml \
  --params DISCOVERED=... \
  --params BENCHMARKS=... \
  --params CURRENT_SCORES=data/scores.yaml \
  -t "Propose score table + cost_rank deltas. Output JSON only."
```

Recipe constraints:

- Read-only tools except writing under `target/model-update/`.
- System prompt encodes TaskClass list, compression rule, anchors, deny_unknown_fields.
- Output must be machine-parseable JSON schema (scores + ranks + comments + risks).
- Deterministic post-processor applies JSON → YAML (goose never hand-edits production YAML blindly in `--apply` without schema validation).

### Phase C — Write + validate

#### C1. Writers

Update (with `--apply`):

1. `data/scores.yaml` — patterns, qualities, context windows, comments/provenance.
2. `examples/router-preferred.yaml` — model ids, display_name, api_model, cost_rank, pricing; orchestration/skill_routing globs only if ids renamed.
3. `examples/router-full.yaml` — keep in sync for shared agents or document intentional lag.
4. Docs snippets in `GOOSE.md` model tables if they list concrete ids.
5. **Do not** rewrite `src/**` unless schema change is required (new score field, etc.) — schema changes are a separate PR.

#### C2. Validation (must pass for "success")

**Data-layer orderings (all TaskClasses where applicable):**

- `fable > opus > sonnet > haiku`
- `sol > terra > luna`
- `sol > gpt-5.5` · `terra > gpt-5.5` · `gpt-5.5 > mini`
- `kimi-thinking > kimi` (if both present)
- `grok-4.5 ≥ grok-code` · `gemini-pro > gemini-flash` (if present)
- Frontier compression: `0.01 ≤ fable[c] − opus[c] ≤ 0.04` ∀ class
- `grok-4.5[c] ≤ opus[c]` ∀ class (current policy)
- Pattern resolution tests for concrete ids (prevent mini-shadowing gemini)

**Routing goldens** (`cargo test --test golden` with preferred-equivalent lineup):

- Winners unchanged for the frozen prompt set in `tests/golden.rs` (haiku / sonnet / opus as today).
- **Do not auto-re-bake** winners; on flip, emit diff and fail for human sign-off.
- Report runner-up utility margin; **warn/fail if margin < 0.02** (fragile).

**"Roughly match current settings"** operational definition:

- Ordering predicates hold.
- Absolute qualities may move ≤ ~0.03 per cell without winner flips.
- Policy changes (gap outside band, fable winning auto, rank ladder >5) require explicit human flag in report.

### Phase D — Operator UX

- Document in `GOOSE.md` or `docs/model-updater.md`: when to run (new model release, pricing change, quarterly re-score).
- Exit codes: 0 dry-run ok, 1 validation fail, 2 discovery fail, 3 goose/schema fail.
- Never commit secrets; discovery must use existing CLI auth on the workstation.

---

## Suggested implementation slices (PR sequence)

1. **Spike:** discovery adapters for grok + claude (document real CLI output shapes); fixture JSON in `tests/fixtures/`.
2. **Validate-only:** implement ordering checks against current `scores.yaml` + golden (should pass green = baseline).
3. **Score pipeline:** anchored quality + cost_rank derivation from fixtures; dry-run report.
4. **Goose recipe:** LLM rename/score proposal → JSON schema → writer.
5. **Apply path + docs:** wire `--apply`, GOOSE.md runbook, CI optional job `model-updater-dry-run` on schedule or manual.

---

## Acceptance criteria (ticket done when…)

- [ ] `scripts/update_models.py --dry-run` runs on a clean checkout and emits a report + diffs.
- [ ] Discovery lists current preferred models without false "remove all" when CLIs are authed.
- [ ] Scoring procedure is documented and encoded (not "ask an LLM to freestyle YAML").
- [ ] Validation suite encodes the ordering table above; **current main passes** as baseline.
- [ ] Dry-run on current main produces **no required changes** or only comment/provenance no-ops (proves the updater recreates today's policy).
- [ ] Introducing a synthetic "opus-next" fixture shows add + score + pattern placement without collapsing ranks to pure quality-max.
- [ ] GOOSE.md (or linked doc) explains how to run after a model launch (e.g. fable/opus upgrade).
- [ ] Latent `*gemini*` vs `*mini*` ordering bug fixed or explicitly ticketed as follow-up with a failing test.

---

## Risks / open decisions

1. **Exact discovery CLIs** for claude/codex need a short spike — document actual commands in the PR.
2. **Sol still scores above rebalanced fable** on most classes while comments say "≈ fable" — updater should flag drift; choose whether to re-normalize sol in the first apply.
3. ~~**Escalation +0.05 gate** vs **≤0.02 frontier compression** means auto-upgrade cannot climb fable from opus~~ — **resolved**: both `upgrade_target` (the confidence-drop auto-upgrade active under `router: auto`) and `escalation_target` (the `router: escalation` ladder/leap) fall back to the best strictly-higher candidate when nothing clears the +0.05 margin, so a struggling Opus/Terra session still reaches Fable/Sol across the compressed gap. The llm_proxy per-request path never had the gate (`strongest_model` is an ungated max). See `kory-code/ROUTING.md` in the hickory-ai monorepo for the implementation record.
4. **Provenance fields** need either comments-only or a Rust schema change (deny_unknown_fields).
5. **Project ownership:** ship in `zaneclaes/router-acp` (not hickory monorepo).

---

## References

- `data/scores.yaml` (quality policy + fable/opus comment)
- `examples/router-preferred.yaml` (canonical agents)
- `src/candidate.rs`, `src/strategies/auto.rs`, `src/config.rs`
- `tests/golden.rs`
- `benchmarks/llm-proxy/RESULTS.md`, `benchmarks/llm-proxy/fable-sample/RESULTS.md`
- Goose: `goose run --recipe …` / `--text` (CLI v1.44+)
