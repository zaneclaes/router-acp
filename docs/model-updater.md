# Model updater — operator runbook

`scripts/update_models.py` keeps router-acp's model catalog and its quality/cost
tables honest. It answers three questions in one command:

1. **What does each agent actually expose right now?** (discovery)
2. **Do sourced benchmarks reproduce the quality table?** (scoring)
3. **Does routing still order models the way policy says?** (validation)

It never writes without `--apply`, and it never re-bakes the routing goldens.

## When to run it

| Trigger | Command |
| --- | --- |
| A provider launched or renamed a model | `scripts/update_models.py` |
| A provider changed prices | update `published_pricing` in `data/model-policy.yaml`, then `scripts/update_models.py` |
| You hand-edited `data/scores.yaml` | `scripts/update_models.py --validate-only` |
| Quarterly re-score | `scripts/update_models.py --use-goose` |
| In CI, on every PR | `scripts/update_models.py --validate-only` |

## The three files

| File | Role |
| --- | --- |
| `data/scores.yaml` | The quality table the router loads (`include_str!`). Authoritative, hand-editable, first-match glob on `agent/model`. |
| `examples/router-preferred.yaml` | The canonical catalog: which models each agent exposes, `cost_rank`, `pricing`. |
| `data/model-policy.yaml` | The *procedure*: benchmark calibrations/evidence, invariants, documented rank exceptions, published-price reference. Rust never reads it. |

The split matters: raw benchmark results and their fixed calibrations are the
source of truth; the updater deterministically writes the resulting score table.

## Quick start

```sh
scripts/update_models.py                  # dry run: report + diffs, writes nothing
scripts/update_models.py --validate-only  # invariants only, no probes (fast, CI-friendly)
scripts/update_models.py --apply          # write the changes the dry run showed you
```

The dry run leaves everything in `/tmp/router-acp-model-update/`:
`report.md`, `discovered.json`, and a `.diff` per file it would touch.

Useful flags: `--providers grok,kimi` (probe a subset), `--discovery-fixture`
(replay a recorded probe instead of shelling out), `--skip-discover`,
`--skip-score`, `--skip-write`, `--use-goose`, `--today YYYY-MM-DD` (pin the
stamp in generated comments), `--out-dir`.

Exit codes: `0` clean · `1` validation failed · `2` discovery failed ·
`3` the goose step failed.

## Phase 1 — discovery

Per-agent adapters declared under `discovery.agents` in the policy file:

| Agent | Method | Why |
| --- | --- | --- |
| claude | `snapshot` | `claude-agent-acp` enumerates models over an ACP `session/set_config_option` (category `model`) — that needs a live adapter session, so there is no offline list. |
| codex | `snapshot` | no offline model list; ids come from its rollout config. |
| grok | `command` (`grok models`) | real list, needs `grok login`. |
| kimi | `command`, probe-only | `kimi-cli` writes the account's ids into its own config at login; there is no list subcommand. |

**A failed probe can never retire a model.** Only a `status: ok` probe drives
removals; `snapshot` and `unavailable` fall back to the configured catalog as
last-known-good and say so in the report. That asymmetry is the point — a
`grok login` that expired must not read as "xAI discontinued everything."

Two more safety rules:

- A vanished model is **commented out** with a `retired <date>` note, kept one
  release before deletion (`discovery.retire_grace_releases`).
- A newly discovered model lands **commented out** with a placeholder rank
  (`discovery.new_models_start_disabled`). Enabling it is a human decision that
  needs real benchmark evidence and a real price.

## Phase 2 — scoring

Quality is a benchmark capability scale: **1 = minimal, 2 = standard,
3 = frontier**, with a real range of **0.5–3.5**. Each benchmark declares fixed
raw-result anchors for 1/2/3; piecewise interpolation maps a published result
onto the common scale. A model's default is the weighted mean of its evidence.
Nothing is min-maxed over the live model set, so adding a candidate cannot
rescore its peers.

A routing decision starts capability demand at 1 for editing/ops, 1.2 for
implementation, and 1.5 for open-ended reasoning. Complexity adds up to two
points and demand caps at 3. The benchmark score is unchanged and remains
visible; the cap only says that unused capability should not consume frontier
plan headroom. A hard task can use the full measured difference. Explicit
pure-quality routing (`cost_quality_tradeoff: 0`) bypasses the cap.

A TaskClass override is emitted only when at least two mapped observations
agree and their mean differs from the model's aggregate by at least 0.15. One
benchmark or a small delta is not meaningful enough to steer task routing.

What gets proposed:

- **A model with calibrated evidence** gets a full proposed entry, placed above
  anything that would shadow it.
- **A model without comparable benchmark evidence** stays disabled and
  unscored. Its name, price, and vendor launch prose never become quality.
- **A model that already resolves to a family inherits that family's score** —
  by design, since patterns key on the model *line* so version bumps inherit.
  The report says which score it inherited and warns you to differentiate it if
  the new generation is materially stronger. This is the `opus[1m]` →
  `opus-next[1m]` case: no score churn, one line in the report.

The deterministic pattern for a new model is its **whole id** (`*gpt-6-atlas*`).
That is intentionally the most specific choice: guessing which substring is the
"line" is how `*mini*` came to swallow every Gemini id. Generalizing it to a line
pattern (`*atlas*`) is a review decision, and the goose step can propose one —
it is rejected if it would shadow an existing entry.

### Cost rank

`cost_rank` is an **ordinal 1..5 model-effort/scarcity ladder**, not marginal
dollars. It prevents spending Fable-class capability on work a smaller model
can reliably finish. Because included-plan usage is free at the margin, the
normalized rank discounts quota utility by at most 50%; it is intentionally
weaker than plan headroom or paid overage. Do not extend the ladder past 5.

The router combines that rank with the tightest remaining included-plan
headroom reported for the candidate. Claude's model-scoped Fable weekly window
therefore remains separate from the shared Claude window; Codex's weekly pool
covers its family. A seat on paid overage has zero included headroom plus an
explicit utility penalty. Grok has no numeric meter, so it retains full
headroom until its binary subscription gate cordons it; its benchmark quality
and rank keep it from displacing comparable metered seats, while its always-
included plan makes it the reliable fallback.

The updater does not compute ranks. It **audits** them: within an agent, a
cheaper model must not carry a higher rank than a pricier one (blended price =
`input + 3·output`). Every real exception is documented in
`cost.rank_exceptions` with its reason, and an undocumented inversion is an
error. Today there are three exceptions — Terra held above 5.5, Grok pinned at 5
for cross-lineage review, Kimi floored at 2 — and each one names why.

## Phase 3 — validation

The contract: **checked-in numbers must equal benchmark-derived numbers.** Checks run
through the same first-match glob the router uses, on the concrete witness ids in
`invariants.witnesses`, so a pattern-ordering mistake fails the check instead of
hiding behind a plausible-looking number.

| Check | Meaning |
| --- | --- |
| `pattern-shadowed` | a narrow pattern sits below a broader one that swallows it |
| `wrong-pattern` | a concrete id resolves to the wrong family |
| `order-broken` | a `strict_desc` / `weak_desc` chain is violated on some class |
| `ceiling-broken` | an `at_most` rule is violated (e.g. Grok above Opus) |
| `benchmark-score-drift` | a checked-in value differs from calibrated evidence |
| `model-without-benchmarks` | an enabled model has no sourced evidence |
| `out-of-band` | a quality escaped the working band |
| `unscored-model` | an enabled model resolves to nothing and routes on the 0.5 default |
| `cost-rank-off-ladder` / `rank-price-inversion` | rank problems (above) |
| `pricing-drift` / `cache-rate-off-ratio` | catalog prices disagree with the published reference (warnings) |

### The goldens are not re-baked automatically

`tests/golden.rs` asserts exact routing winners. If a change moves one, the
updater reports it and **fails** rather than re-baking, because a moved winner is
either a policy decision or a bug and only a human can say which. Re-bake
deliberately:

```sh
cargo test --test golden dump_golden -- --ignored --nocapture
```

## Phase 4 — the goose step (optional)

`--use-goose` runs `scripts/recipes/update-models-score.yaml` through
`goose run --recipe … --no-session` for research only: comparable raw benchmark
results, primary-source URLs, and likely id renames. It is forbidden to invent a
tier or base. Reviewed observations are added to
`benchmark_scoring.model_evidence`; deterministic Python computes the scores.
**Goose never edits production files.**

Validate the recipe after editing it:

```sh
goose recipe validate scripts/recipes/update-models-score.yaml
```

## Tests

```sh
python3 scripts/tests/test_update_models.py                          # direct
python3 -m unittest discover -s scripts/tests -t scripts/tests       # discovery
```

They run against the real checked-in files, because the load-bearing claim is
that the updater reproduces today's shipped policy. Two properties worth knowing
about, since they are the licence to touch these files at all: rendering an
unmodified `scores.yaml` or catalog returns the **original bytes**, and a dry run
on the current catalog requires **zero changes**.

## Adding a model by hand

The updater is a safety net, not a gate. To add a model yourself:

1. Add it to the agent's `models:` list with a `cost_rank` and `pricing`.
2. Add a score entry, placing it **above** any broader pattern that matches it.
3. Add its published rates to `published_pricing` in the policy file.
4. Add a witness + a `resolves_to` line if it should be covered by an invariant.
5. `scripts/update_models.py --validate-only && cargo test`.

## Note for Hickory engineers

`examples/router-preferred.yaml` is the **canonical default** a Hickory
workstation runs on: `dev-env router-install` fetches it at the pinned rev into
`/usr/local/share/router-acp/router-preferred.yaml`, and the relay deep-merges
`~/.config/router-acp/overrides.json` over it to generate the
`~/.config/router-acp/router.yaml` the shim actually reads (regenerated on every
relay start and on every Agent Settings save).

Two consequences:

- A catalog change here reaches workstations when the pinned rev moves — so it is
  worth validating before it ships, not after.
- The generated per-workstation file can differ from the defaults wherever a user
  set an override. The updater never touches generated or user files; point
  `--config` at one to audit it read-only:

```sh
scripts/update_models.py --validate-only --config ~/.config/router-acp/router.yaml
```
