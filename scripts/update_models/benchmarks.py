"""Deterministic benchmark-to-quality calibration.

Each benchmark defines three fixed result anchors: the raw result corresponding
to minimal (1), standard (2), and frontier (3) capability. Results interpolate
piecewise between those anchors and may extend only to the policy's 0.5..3.5
guardrails. This keeps model scores stable when the catalog changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from .policy import TASK_CLASSES, Policy, PolicyError
from . import patterns as pat


@dataclass(frozen=True)
class BenchmarkScore:
    pattern: str
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


def derive(policy: Policy) -> dict[str, BenchmarkScore]:
    config = policy.benchmark_scoring
    definitions = config.get("benchmarks") or {}
    evidence = config.get("model_evidence") or {}
    minimum = int(config.get("minimum_task_observations", 2))
    meaningful = float(config.get("meaningful_task_delta", 0.15))
    out: dict[str, BenchmarkScore] = {}

    for pattern, observations in evidence.items():
        all_values: list[tuple[float, float]] = []
        by_class: dict[str, list[tuple[float, float]]] = {name: [] for name in TASK_CLASSES}
        for observation in observations:
            name = observation.get("benchmark")
            definition = definitions.get(name)
            if not definition:
                raise PolicyError(f"`{pattern}` references unknown benchmark `{name}`")
            if not observation.get("source"):
                raise PolicyError(f"`{pattern}` benchmark `{name}` has no source")
            value = _calibrate(
                float(observation["result"]),
                definition.get("anchors") or {},
                policy.quality_band,
            )
            weight = float(observation.get("weight", definition.get("weight", 1.0)))
            all_values.append((value, weight))
            for task_class in definition.get("task_classes") or []:
                if task_class not in by_class:
                    raise PolicyError(f"benchmark `{name}` has unknown task class `{task_class}`")
                by_class[task_class].append((value, weight))

        if not all_values:
            raise PolicyError(f"`{pattern}` has no benchmark observations")

        def mean(values: list[tuple[float, float]]) -> float:
            total_weight = sum(weight for _, weight in values)
            return sum(value * weight for value, weight in values) / total_weight

        quantum = Decimal(str(policy.round_to))

        def rounded(value: float) -> float:
            return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP))

        base = rounded(mean(all_values))
        quality = {task_class: base for task_class in TASK_CLASSES}
        for task_class, values in by_class.items():
            if len(values) < minimum:
                continue
            candidate = rounded(mean(values))
            if abs(candidate - base) + 1e-9 >= meaningful:
                quality[task_class] = candidate
        out[pattern] = BenchmarkScore(
            pattern=pattern,
            default_quality=base,
            quality=quality,
            observations=len(all_values),
        )
    _apply_compression(config, out)
    return out


def _apply_compression(config: dict[str, Any], scores: dict[str, BenchmarkScore]) -> None:
    """Pin declared same-tier peers `max_gap` below their preferred member.

    Peers whose benchmark gaps sit inside measurement noise must not let a
    small, unreliable per-class delta out-vote a real price difference in
    cost-aware routing. Policy declares the pair and its order; the `behind`
    member's per-class output is superseded (its raw observations stay in the
    evidence log), leaving a deterministic `max_gap` residual so ordering-
    sensitive paths still resolve to `ahead`.
    """
    compression = config.get("compression") or {}
    gap = float(compression.get("max_gap", 0.02))
    for pair in compression.get("pairs") or []:
        ahead_pattern, behind_pattern = pair.get("ahead"), pair.get("behind")
        ahead = scores.get(ahead_pattern)
        behind = scores.get(behind_pattern)
        if ahead is None or behind is None:
            missing = behind_pattern if ahead is not None else ahead_pattern
            raise PolicyError(
                f"compression pair references `{missing}`, which has no benchmark evidence"
            )
        scores[behind_pattern] = BenchmarkScore(
            pattern=behind.pattern,
            default_quality=round(ahead.default_quality - gap, 2),
            quality={
                task_class: round(score - gap, 2)
                for task_class, score in ahead.quality.items()
            },
            observations=behind.observations,
        )


def resolve_profile(profiles: dict[str, BenchmarkScore], candidate: str) -> str | None:
    """Resolve benchmark patterns by specificity, independent of YAML order."""
    return pat.resolve(pat.sorted_by_specificity(list(profiles)), candidate)
