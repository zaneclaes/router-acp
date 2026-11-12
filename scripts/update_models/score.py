"""Re-score models from their evidence, and place newly discovered models.

Re-scoring is deterministic: each model's quality is recomputed from the
evidence on its own entry in `data/providers/<name>.yaml`
(`benchmarks.score_rows`) and written back to that entry only. Nothing here infers quality from a model's name, family or a
vendor's prose, and never copied from a sibling or an earlier version: a model
without evidence of its own stays unscored (no quality lines at all) until
someone records its published results on its row.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import benchmarks
from .catalog import Catalog
from .policy import TASK_CLASSES, Policy


@dataclass
class Rescore:
    """One row whose written quality differs from its computed quality."""

    key: str
    origin: str
    before: dict[str, float]
    after: dict[str, float]
    before_default: float
    after_default: float
    cleared: bool = False
    was_scored: bool = True

    def describe(self) -> str:
        if self.cleared:
            return f"`{self.key}` has no evidence of its own: its quality lines were removed (unscored)"
        if not self.was_scored:
            return f"`{self.key}` newly scored from {self.origin}: base {self.after_default:.2f}"
        moved = [
            f"{task_class} {self.before[task_class]:.2f}→{self.after[task_class]:.2f}"
            for task_class in TASK_CLASSES
            if abs(self.before[task_class] - self.after[task_class]) > 1e-9
        ]
        return (
            f"`{self.key}` re-scored from {self.origin}: base {self.before_default:.2f}"
            f"→{self.after_default:.2f}" + (f" ({', '.join(moved)})" if moved else "")
        )


def rescore(policy: Policy, models: Catalog) -> list[Rescore]:
    """Rewrite every row's quality from its own evidence (in memory).

    A row with no evidence of its own loses any quality lines it carries.
    """
    scored = benchmarks.score_rows(policy, models)
    changes: list[Rescore] = []
    for row in models.rows:
        before, before_default = row.qualities(), float(row.fields.get("default_quality", 0.5))
        was_scored = row.scored
        if row.key in scored.unscored:
            if models.clear_quality(row):
                changes.append(
                    Rescore(row.key, "no evidence", before, {}, before_default, 0.0, cleared=True)
                )
            continue
        computed = scored.rows[row.key]
        if models.set_quality(row, computed.default_quality, computed.quality):
            changes.append(
                Rescore(
                    key=row.key,
                    origin=f"{computed.observations} observation(s) of its own",
                    was_scored=was_scored,
                    before=before,
                    after=dict(computed.quality),
                    before_default=before_default,
                    after_default=computed.default_quality,
                )
            )
    return changes


def rank_suggestion(policy: Policy, pricing: dict[str, Any] | None, peers: list[tuple[int, float]]) -> int:
    """Suggest a `cost_rank` from blended price against already-ranked peers.

    Ordinal, not absolute: the new model takes the rank of the most expensive
    peer it is at least as expensive as, so it slots into the existing ladder
    instead of redefining it. With no price, the ladder floor.
    """
    low, high = policy.ladder
    price = policy.blended_price(pricing)
    if price is None or not peers:
        return low
    suggestion = low
    for rank, peer_price in sorted(peers, key=lambda pair: pair[1]):
        if price >= peer_price:
            suggestion = rank
    return max(low, min(high, suggestion))
