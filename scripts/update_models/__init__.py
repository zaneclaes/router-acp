"""Model catalog + quality/cost updater for router-acp.

Entry point: `scripts/update_models.py`. Runbook: `docs/model-updater.md`.

Module split mirrors the pipeline phases:

  * `policy`      — load benchmark calibrations, evidence, and invariants
  * `patterns`    — first-match glob + pattern-specificity ordering
  * `discover`    — per-agent model discovery adapters (fail-open)
  * `score`       — anchored score proposals for unscored models
  * `scores_file` — surgical reader/writer for `data/scores.yaml`
  * `catalog`     — surgical reader/writer for the agent config catalog
  * `validate`    — the ordering/contract invariants
  * `provenance`  — comment blocks written above generated entries
  * `goose`       — headless goose bridge for the judgment steps
  * `report`      — operator-facing markdown report + diffs
"""
