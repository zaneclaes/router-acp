"""Notes written as comments into `data/providers/<name>.yaml` by the updater.

Provenance lives in comments rather than YAML keys on purpose: the router's
model schema denies unknown fields, so inventing a `provenance:` key would make
it fail to start. A comment costs nothing and is what a human reads first.
"""

from __future__ import annotations


def retirement_note(model_id: str, generated_at: str, grace_releases: int = 1) -> str:
    """The note left above an entry commented out because it vanished."""
    return (
        f"retired {generated_at}: no longer offered by the provider "
        f"(discovery found no `{model_id}`); kept for {grace_releases} release(s) "
        "(`discovery.retire_grace_releases`) before removal."
    )


def new_model_note(model_id: str, generated_at: str) -> str:
    """The note left above a newly discovered, still commented-out entry."""
    return (
        f"discovered {generated_at} by scripts/update_models.py (`{model_id}`): move it "
        "under its family, set its version, fill in pricing, check cost_rank, record its "
        "own published results under `evidence`, uncomment it and rerun with --apply."
    )
