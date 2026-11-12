"""Model catalog + quality/cost updater for router-acp.

Entry point: `scripts/update_models.py`. Runbook: `docs/model-updater.md`.

The catalog is `data/providers/<name>.yaml`: each provider file groups its
models by family, one exact entry per version (no patterns, no aliases, no
inherited scores), each carrying its own evidence and the quality computed from
it. The scoring procedure (`scoring`, `cost`) lives in `data/classifier.yaml`.
The updater writes only the quality lines of the provider files.

  * `catalog`     — surgical reader/writer for `data/providers/<name>.yaml`
  * `policy`      — `scoring` / `cost` (classifier.yaml) and each provider's
                    `cache_ratio` / `discovery`
  * `benchmarks`  — a model's own evidence → its quality (nothing else)
  * `score`       — re-score entries in place; rank suggestion for new models
  * `discover`    — per-provider model discovery adapters (fail-open)
  * `validate`    — the shape/evidence/ordering/cost contract
  * `provenance`  — comment notes written above retired/discovered entries
  * `goose`       — headless goose bridge for the evidence-gathering step
  * `report`      — operator-facing markdown report + diffs
"""
