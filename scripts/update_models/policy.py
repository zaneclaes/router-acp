"""The scoring procedure and per-provider catalog settings.

  * `data/classifier.yaml` holds, beside the classifier rules, the two
    non-per-model sections the updater uses:
      - `scoring` — the fixed benchmark calibrations (anchors per benchmark),
        the quality band, rounding and the task-override thresholds: the
        procedure that turns a model's evidence into its quality;
      - `cost` — the cost-rank ladder and the blended-price weight used to
        validate `cost_rank`.
  * each `data/providers/<name>.yaml` holds two catalog-maintenance keys:
    `cache_ratio` (the default for a cache rate a model leaves unset) and
    `discovery` (how its live model list is probed, including
    `retire_grace_releases`).

This module turns those into a `Policy`; it never reads model entries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# TaskClass order, matching `TaskClass::ALL` in src/candidate.rs. Emitted in
# this order so a generated block reads like the hand-written ones.
TASK_CLASSES = [
    "UiTweak",
    "BugFix",
    "Feature",
    "Refactor",
    "Algorithms",
    "Architecture",
    "Research",
    "Writing",
    "Ops",
    "CodingGeneral",
]


class PolicyError(RuntimeError):
    """The scoring sections are missing, malformed, or internally inconsistent."""


@dataclass
class Policy:
    path: Path
    quality_band: tuple[float, float]
    round_to: float
    scoring: dict[str, Any]
    cost: dict[str, Any]
    # provider name -> its parsed data/providers/<name>.yaml
    providers: dict[str, dict[str, Any]] = field(default_factory=dict)

    # -- scoring ----------------------------------------------------------
    @property
    def benchmarks(self) -> dict[str, Any]:
        return self.scoring.get("benchmarks") or {}

    @property
    def minimum_task_observations(self) -> int:
        return int(self.scoring.get("minimum_task_observations", 2))

    @property
    def meaningful_task_delta(self) -> float:
        return float(self.scoring.get("meaningful_task_delta", 0.15))

    # -- cost -------------------------------------------------------------
    @property
    def blended_output_weight(self) -> float:
        return float(self.cost.get("blended_output_weight", 3))

    @property
    def ladder(self) -> tuple[int, int]:
        ladder = self.cost.get("ladder", {}) or {}
        return int(ladder.get("min", 1)), int(ladder.get("max", 5))

    def blended_price(self, pricing: dict[str, Any] | None) -> float | None:
        """input + k·output, the prior used to sanity-check ordinal ranks."""
        if not pricing:
            return None
        try:
            return float(pricing["input_per_mtok"]) + self.blended_output_weight * float(
                pricing["output_per_mtok"]
            )
        except (KeyError, TypeError, ValueError):
            return None

    # -- providers --------------------------------------------------------
    def cache_ratio(self, provider: str) -> dict[str, float] | None:
        """The ratio an UNSET cache rate of this provider's models defaults to."""
        spec = (self.providers.get(provider) or {}).get("cache_ratio") or {}
        return {key: float(value) for key, value in spec.items()} or None

    def discovery_spec(self, provider: str) -> dict[str, Any] | None:
        spec = (self.providers.get(provider) or {}).get("discovery")
        return spec if isinstance(spec, dict) else None

    @property
    def discovery(self) -> dict[str, Any]:
        """`{agents: {provider: spec}}`, the shape `discover.discover` takes."""
        agents: dict[str, Any] = {}
        for name in self.providers:
            spec = self.discovery_spec(name)
            if spec:
                agents[name] = spec
        return {"agents": agents}

    def retire_grace_releases(self, provider: str) -> int:
        return int((self.discovery_spec(provider) or {}).get("retire_grace_releases", 1))


def load_policy(
    classifier_raw: dict[str, Any], path: Path, providers: dict[str, dict[str, Any]] | None = None
) -> Policy:
    """Build the policy from a parsed `data/classifier.yaml` and the provider files."""
    scoring = classifier_raw.get("scoring")
    if not isinstance(scoring, dict):
        raise PolicyError(f"{path} has no `scoring:` section")
    band_raw = scoring.get("quality_band") or {}
    band = (float(band_raw.get("min", 0.5)), float(band_raw.get("max", 3.5)))
    if not band[0] < band[1]:
        raise PolicyError(f"quality_band min must be below max (got {band})")
    if not scoring.get("benchmarks"):
        raise PolicyError(f"{path} `scoring.benchmarks` is empty")
    return Policy(
        path=path,
        quality_band=band,
        round_to=float(scoring.get("round_to", 0.01)),
        scoring=scoring,
        cost=classifier_raw.get("cost") or {},
        providers=dict(providers or {}),
    )
