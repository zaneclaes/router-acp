# Model updater — operator runbook

Every model router-acp supports is defined in exactly one place: its entry in
its provider's file, **`data/providers/<name>.yaml`** (`claude`, `codex`,
`grok`, `kimi`). That entry holds its id, version, pricing, cost rank, wire
capabilities, its own published benchmark evidence, and the quality computed
from that evidence. The benchmark → quality procedure (`scoring`) and the
cost-rank ladder (`cost`) live in **`data/classifier.yaml`**, beside the
classifier rules.

There is no other model data file. `data/scores.yaml`, `data/models.yaml`,
`data/providers.yaml`, `data/model-policy.yaml` and `src/presets/*.yaml` no
longer exist: their content is in the provider files and `data/classifier.yaml`.

`scripts/update_models.py` keeps the catalog honest. It answers three questions
in one command:

1. **What does each provider actually expose right now?** (discovery)
2. **Does each model's quality equal what its own evidence computes?** (re-scoring)
3. **Is the catalog well-formed, and does it still order models the way its
   entries claim?** (validation)

It never writes without `--apply`. It writes only the provider files, and in
them only quality lines, retired entries and parked new models. It never
re-bakes the routing goldens.

## The provider file

```yaml
provider: claude              # the provider; the file is named after it
command: { ... }              # the provider's default agent settings
model_selection: { ... }      #   (an agent entry in a router config states only what it changes)
cache_ratio: { read: 0.1, write: 1.25 }   # prices a cache rate a model leaves unset
discovery:                    # how the live model list is obtained
  retire_grace_releases: 1
  method: snapshot
models:                       # family name -> its versions
  opus:
  - version: 5.5              # dotted, compared component by component (4.10 > 4.9)
    id: claude-opus-5-5       # the provider's real API model id; never an alias
    display_name: Claude Opus 5.5
    cost_rank: 4              # ordinal 1..5, see `cost` in data/classifier.yaml
    pricing: { input_per_mtok: 4.0, output_per_mtok: 20.0, cache_read_per_mtok: 0.2, cache_write_per_mtok: 5.0 }
    evidence:                 # THIS model's own published results
      - { benchmark: Terminal-Bench-2.1, result: 87.64, source: "https://..." }
    outranks: [claude-opus-4-6]   # optional: must beat these on every task class
    rank_exception: "why ..."     # optional: why its cost_rank breaks price order
    context_window: 1000000       # wire fields the per-request proxy needs
    effort_levels: [low, medium, high, xhigh, max]
    default_quality: 2.81         # written by the updater, never by hand
    quality: { UiTweak: 2.81, ... }
```

The rules that make it the single source of truth:

- **One exact entry per model, no patterns, no aliases.** A model is found by
  its exact `id` (case-insensitive) within its provider — never a glob, a
  prefix or a family. `id` is the provider's real API model id; there is no
  `api_model`. `*` is an error anywhere a model is named.
- **The provider comes from the file, the family from the group key.** An entry
  never states `provider:` or `family:`, and the updater never derives a family
  or a version from an id string.
- **Every version is its own model, scored only on its own `evidence`.** No
  entry ever takes another model's or another version's numbers (`scored_as`,
  `compressed_behind` and compression pairs are gone; validation rejects them).
  An entry with no evidence of its own is **unscored**: it carries no
  `quality` / `default_quality`, it is chosen only by an explicit pick, and
  every run lists it as a warning.
- **Agents inherit their provider's models.** Every agent of `provider: claude`
  routes on the models in `data/providers/claude.yaml`, so a router config's
  `agents.<x>.models` no longer lists them. A config's own `models:` may only
  **add** a model the provider file lacks or **override fields** of an
  inherited one, matched by `id`.
- **A reference** (`outranks`) is a bare id of the same provider, or
  `provider/id` across providers.
- **`effort_mapping` only renames.** List only the levels a model really has in
  `effort_levels`; an identity entry (`high: high`) is refused.

## When to run it

| Trigger | What to do |
| --- | --- |
| A model published benchmark results | add them to **that model's entry** under `evidence:` in `data/providers/<name>.yaml`, then `scripts/update_models.py --apply` |
| A provider launched or renamed a model | `scripts/update_models.py` (discovery parks it), then fill in the entry |
| A provider changed prices | edit that entry's `pricing`, then `scripts/update_models.py --validate-only` |
| You hand-edited a provider file or `scoring` | `scripts/update_models.py --validate-only` (or `--apply` after a `scoring` change) |
| Looking for evidence for unscored models | `scripts/update_models.py --use-goose` |
| In CI, on every PR | `scripts/update_models.py --validate-only` |

## Re-scoring a model

1. Put the model's published results on **its own entry** in
   `data/providers/<name>.yaml`:

   ```yaml
   evidence:
     - { benchmark: DeepSWE-v1.1, result: 74.1, source: "https://..." }
   ```

   Use only benchmarks declared under `scoring.benchmarks` in
   `data/classifier.yaml` (their anchors are fixed; a new benchmark version
   needs its own calibration there first). Each observation needs a primary
   source, measured on exactly this model id. Never copy a sibling's or an
   earlier version's results onto an entry.
2. Run `scripts/update_models.py --apply`. It rewrites that entry's
   `default_quality` and `quality` lines and touches nothing else: comments,
   other fields (including `coding_tier` / `coding_percentile`, which are
   hand-maintained) and every other model stay byte-identical. No model's score
   depends on another's, so nothing else moves.
3. Run `scripts/update_models.py --validate-only` and `cargo test`, and read
   any broken `outranks` claim or moved golden (below).

Never edit `quality` / `default_quality` by hand: `--validate-only` fails any
entry whose numbers differ from what its evidence computes
(`benchmark-score-drift`), and any entry that carries quality without evidence
(`quality-without-evidence`; `--apply` removes it).

## Quick start

```sh
scripts/update_models.py                  # dry run: report + diffs, writes nothing
scripts/update_models.py --validate-only  # checks only, no probes (fast, CI-friendly)
scripts/update_models.py --apply          # write the changes the dry run showed you
```

The dry run leaves everything in `/tmp/router-acp-model-update/`:
`report.md`, `discovered.json`, and a `<provider>.yaml.diff` per file it would
change.

Useful flags: `--data DIR` (a directory holding `classifier.yaml` and
`providers/*.yaml`; default `data/`), `--providers grok,kimi` (probe a subset),
`--discovery-fixture` (replay a recorded probe instead of shelling out),
`--skip-discover`, `--skip-score`, `--skip-write`, `--use-goose`,
`--today YYYY-MM-DD` (pin the stamp in generated comments), `--out-dir`.

Exit codes: `0` clean · `1` validation failed · `2` discovery failed ·
`3` the goose step failed.

## Phase 1 — discovery

Per-provider adapters declared as `discovery:` in each provider file:

| Provider | Method | Why |
| --- | --- | --- |
| claude | `snapshot` | `claude-agent-acp` enumerates models over an ACP `session/set_config_option` (category `model`) — that needs a live adapter session, so there is no offline list. |
| codex | `snapshot` | no offline model list; ids come from its rollout config. |
| grok | `command` (`grok models`) | real list, needs `grok login`. |
| kimi | `command`, probe-only | `kimi-cli` writes the account's ids into its own config at login; there is no list subcommand. |

"Configured" is the provider's live entries.

**A failed probe can never retire a model.** Only a `status: ok` probe drives
removals; `snapshot` and `unavailable` fall back to the entries as
last-known-good and say so in the report. A `grok login` that expired must not
read as "xAI discontinued everything."

- A vanished model's entry is **commented out** with a `retired <date>` note,
  kept `discovery.retire_grace_releases` release(s) before deletion. If another
  entry `outranks` it, validation says so.
- A newly discovered model lands as a **commented-out entry** at the end of its
  provider's `models:`, under a `TODO-family:` key with `version: TODO`, a
  placeholder `cost_rank` and `pricing: TODO`. Discovery sees only an id, so
  enabling it is a human decision: move it under its family, set its version,
  fill in pricing and evidence, uncomment, rerun `--apply`.

Commented-out entries count as known, so the next run does not propose them
again.

## Phase 2 — scoring

Quality is a benchmark capability scale: **1 = minimal, 2 = standard,
3 = frontier**, with a hard range of **0.5–3.5** (`scoring.quality_band`). Each
benchmark in `scoring.benchmarks` declares fixed raw-result anchors for 1/2/3
and the task classes it informs; piecewise interpolation maps a published
result onto the common scale. A model's `default_quality` is the weighted mean
of its evidence. Nothing is min-maxed over the live models, so adding a model
cannot move any other.

A task-class override is emitted only when at least
`minimum_task_observations` (2) mapped observations agree and their mean
differs from the model's aggregate by at least `meaningful_task_delta` (0.15).

Every number comes from the model's own evidence. There is no compression, no
`scored_as` and no family inheritance: when published results disagree with a
claimed order, the fix is better evidence (or dropping the `outranks` claim),
never a quality copied or derived from another model. A result outside the
anchors clips to the band edge (0.5 or 3.5); a model pinned there by a single
observation deserves a second source.

A routing decision starts capability demand at 1 for editing/ops, 1.2 for
implementation, and 1.5 for open-ended reasoning. Complexity adds up to two
points and demand caps at 3. The benchmark score is unchanged and remains
visible; the cap only says that unused capability should not consume frontier
plan headroom. Explicit pure-quality routing (`cost_quality_tradeoff: 0`)
bypasses the cap.

### Cost rank

`cost_rank` is an **ordinal 1..5 model-effort/scarcity ladder**
(`cost.ladder` in `data/classifier.yaml`), not marginal dollars. It prevents
spending Fable-class capability on work a smaller model can reliably finish. Do
not extend the ladder past 5: that rescales every `auto` pool norm.

The updater does not compute ranks. It **audits** them: within a provider, a
cheaper model must not carry a higher rank than a pricier one (blended price =
`input + 3·output`). An entry that deliberately breaks that states why in its
own `rank_exception:`; an undocumented inversion is an error.

Prices live only in each entry's `pricing`. A stated cache rate is taken as
published (it must be positive); an unset one defaults to the provider's
`cache_ratio` × input.

## Phase 3 — validation

The contract: **every entry is one exact, fully described model, and its
checked-in quality equals what its own evidence computes.**

| Check | Severity | Meaning |
| --- | --- | --- |
| `provider-missing` / `provider-mismatch` | error | a provider file lacks `provider:`, or is not named after it |
| `model-missing-field` | error | an entry lacks version/id/display_name/cost_rank/pricing |
| `bad-version` | error | `version` is not a dotted number |
| `wildcard-key` | error | a `*` in an id, family or `outranks` entry |
| `duplicate-model` | error | an id appears twice in one provider |
| `duplicate-release` | warn | two entries claim the same family and version |
| `retired-field` | error | an entry sets `api_model`, `scored_as`, `compressed_behind`, `family` or `provider` |
| `retired-section` | error | a provider file has a top-level `discovery_retire_grace_releases` |
| `identity-effort-mapping` | error | `effort_mapping` maps a level to itself |
| `unknown-reference` | error | `outranks` names no model |
| `bad-evidence` | error | an observation names an undeclared benchmark or has no source |
| `unscored-models` | warn | the entries with no evidence of their own (listed) |
| `quality-without-evidence` | error | an unscored entry carries quality (a copy); `--apply` removes it |
| `benchmark-score-drift` | error | checked-in quality differs from computed |
| `out-of-band` | error | a quality escaped the band |
| `order-broken` | error | an `outranks` claim fails on some task class |
| `order-unchecked` | warn | an `outranks` claim involves an unscored model |
| `cost-rank-off-ladder` / `rank-price-inversion` | error | rank problems (above) |
| `bad-pricing` | error | a price or stated cache rate is not a positive number |

An `order-broken` finding after a re-score means the evidence no longer
supports a claimed order. The updater never deletes the claim; a human decides
whether the evidence or the claim is wrong.

### The goldens are not re-baked automatically

`tests/golden.rs` asserts exact routing winners. If a change moves one, the
build fails rather than re-baking, because a moved winner is either a policy
decision or a bug and only a human can say which. Re-bake deliberately:

```sh
cargo test --test golden dump_golden -- --ignored --nocapture
```

## Phase 4 — the goose step (optional)

`--use-goose` runs `scripts/recipes/update-models-score.yaml` through
`goose run --recipe … --no-session` for research only: comparable raw benchmark
results measured on exactly the unscored (or newly discovered) model, their
primary sources, and likely id renames. It never assigns a tier or a number.
The report lists what it found; a reviewer copies accepted observations into
the model's `evidence:` and reruns with `--apply`. **Goose never edits the
catalog.**

```sh
goose recipe validate scripts/recipes/update-models-score.yaml
```

## Tests

```sh
python3 scripts/tests/test_update_models.py                          # direct
python3 -m unittest discover -s scripts/tests -t scripts/tests       # discovery
```

Two inputs. The real provider files must round-trip **byte-identically**,
validate without errors, and re-score as a **no-op** (so after recording new
evidence, run `--apply` before the tests). `scripts/fixtures/catalog/` is a
small synthetic data directory (`classifier.yaml`, `providers/alpha.yaml`,
`providers/beta.yaml`) that pins each rule (own evidence only, outranks in and
across providers, rank exceptions, unscored entries, numeric versions, retired
fields) without moving when the real catalog does. Discovery fixtures under
`scripts/fixtures/discovery/` state a probe's answer relative to the live
entries (`releases`, `retires`), so they do not go stale when a model is added.

## Adding a model by hand

1. Add an entry under its family in `data/providers/<name>.yaml` (a new family
   is a new key under `models:`) with `version`, `id` (the provider's real API
   id), `display_name`, `cost_rank` and `pricing`, plus its wire fields
   (`context_window`, `max_output_tokens`, `effort_levels`, ...). Do not touch
   example configs: agents inherit it.
2. If it has published results, add them under `evidence:`. If not, leave it
   unscored — never copy a sibling's quality.
3. Add `outranks` / `rank_exception` only with a reason in a comment above it.
4. `scripts/update_models.py --apply && scripts/update_models.py --validate-only && cargo test`.

## Note for Hickory engineers

`examples/router-preferred.yaml` is the **canonical default config** a Hickory
workstation runs on: `dev-env router-install` fetches it at the pinned rev into
`/usr/local/share/router-acp/router-preferred.yaml`, and the relay deep-merges
`~/.config/router-acp/overrides.json` over it to generate
`~/.config/router-acp/router.yaml`. Its agents inherit their settings and
models from `data/providers/<name>.yaml`, which is compiled into the binary, so
a catalog change reaches workstations when the pinned rev moves — validate it
before it ships. A per-workstation `models:` entry only adds a model or
overrides a field of an inherited one by `id`; the updater never reads or
writes those files.
