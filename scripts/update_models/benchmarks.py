"""Deterministic benchmark-to-quality calibration.

Each benchmark in `scoring.benchmarks` defines three fixed result anchors: the
raw result corresponding to minimal (1), standard (2), and frontier (3)
capability. Results interpolate piecewise between those anchors and may extend
only to the 0.5..3.5 guardrails. This keeps model scores stable when the
catalog changes: adding one model never moves another's calibration.

Every model is scored ONLY on its own `evidence:` (operator, 2026-11-12): no
row ever takes another model's or another version's numbers, and no row's
quality is set from another's (there is no compression and no `scored_as`). A
row with no evidence is unscored — it carries no `quality`, the router gives it
the neutral default, and the validator lists it.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import TYPE_CHECKING, Any

from .policy import TASK_CLASSES, Policy, PolicyError

if TYPE_CHECKING:
    from .catalog import Catalog


@dataclass(frozen=True)
class BenchmarkScore:
    key: str
    default_quality: float
    quality: dict[str, float]
    observations: int


def _calibrate(result: float, anchors: dict[str, Any], band: tuple[float, float]) -> float:
    points = [
        (float(anchors["minimal"]), 1.0),
        (float(anchors["standard"]), 2.0),
        (float(anchors["frontier"]), 3.0),
    ]
    if not points[0][0] < points[1][0] < points[2][0]:
        raise PolicyError(f"benchmark anchors must increase: {anchors}")

    if result <= points[1][0]:
        left, right = points[0], points[1]
    else:
        left, right = points[1], points[2]
    quality = left[1] + (result - left[0]) * (right[1] - left[1]) / (right[0] - left[0])
    return min(band[1], max(band[0], quality))


def calibrate_evidence(policy: Policy, key: str, observations: list[dict[str, Any]]) -> BenchmarkScore:
    """Raw quality from one model's observations."""
    definitions = policy.benchmarks
    all_values: list[tuple[float, float]] = []
    by_class: dict[str, list[tuple[float, float]]] = {name: [] for name in TASK_CLASSES}
    for observation in observations:
        if not isinstance(observation, dict):
            raise PolicyError(f"`{key}` evidence entry is not a mapping: {observation!r}")
        name = observation.get("benchmark")
        definition = definitions.get(name)
        if not definition:
            raise PolicyError(f"`{key}` references unknown benchmark `{name}`")
        if not observation.get("source"):
            raise PolicyError(f"`{key}` benchmark `{name}` has no source")
        value = _calibrate(float(observation["result"]), definition.get("anchors") or {}, policy.quality_band)
        weight = float(observation.get("weight", definition.get("weight", 1.0)))
        all_values.append((value, weight))
        for task_class in definition.get("task_classes") or []:
            if task_class not in by_class:
                raise PolicyError(f"benchmark `{name}` has unknown task class `{task_class}`")
            by_class[task_class].append((value, weight))

    if not all_values:
        raise PolicyError(f"`{key}` has no benchmark observations")

    def mean(values: list[tuple[float, float]]) -> float:
        total_weight = sum(weight for _, weight in values)
        return sum(value * weight for value, weight in values) / total_weight

    quantum = Decimal(str(policy.round_to))

    def rounded(value: float) -> float:
        return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP))

    base = rounded(mean(all_values))
    quality = {task_class: base for task_class in TASK_CLASSES}
    for task_class, values in by_class.items():
        if len(values) < policy.minimum_task_observations:
            continue
        candidate = rounded(mean(values))
        if abs(candidate - base) + 1e-9 >= policy.meaningful_task_delta:
            quality[task_class] = candidate
    return BenchmarkScore(key=key, default_quality=base, quality=quality, observations=len(all_values))


@dataclass
class Scored:
    """Every live row's computed quality."""

    # row key -> quality computed from that row's own evidence
    rows: dict[str, BenchmarkScore]
    # rows with no evidence of their own: unscored by design, never copied
    unscored: list[str]


def score_rows(policy: Policy, models: "Catalog") -> Scored:
    """Compute every live row's quality from its own `evidence`, and nothing else."""
    rows: dict[str, BenchmarkScore] = {}
    unscored: list[str] = []
    for row in models.rows:
        if row.evidence:
            rows[row.key] = calibrate_evidence(policy, row.key, row.evidence)
        else:
            unscored.append(row.key)
    return Scored(rows=rows, unscored=unscored)
