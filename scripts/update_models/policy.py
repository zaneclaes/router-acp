"""Load `data/model-policy.yaml` — the encoded scoring procedure.

The policy file is the answer to "where did these numbers come from?": fixed
benchmark calibrations, evidence, the ordering invariants any edit must keep,
and the documented reasons a `cost_rank` diverges from list price.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from . import patterns as pat

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
    """The policy file is missing, malformed, or internally inconsistent."""


@dataclass(frozen=True)
class RankException:
    """A documented reason a cost_rank does not follow list price."""

    candidate: str
    why: str
    over: str | None = None


@dataclass
class Policy:
    path: Path
    raw: dict[str, Any]
    quality_band: tuple[float, float]
    round_to: float
    witnesses: dict[str, str]
    invariants: dict[str, Any]
    cost: dict[str, Any]
    published_pricing: dict[str, Any]
    discovery: dict[str, Any]
    benchmark_scoring: dict[str, Any]

    @property
    def blended_output_weight(self) -> float:
        return float(self.cost.get("blended_output_weight", 3))

    @property
    def ladder(self) -> tuple[int, int]:
        ladder = self.cost.get("ladder", {})
        return int(ladder.get("min", 1)), int(ladder.get("max", 5))

    @property
    def rank_exceptions(self) -> list[RankException]:
        return [
            RankException(
                candidate=entry["candidate"],
                why=" ".join(str(entry.get("why", "")).split()),
                over=entry.get("over"),
            )
            for entry in self.cost.get("rank_exceptions", [])
        ]

    def cache_ratio(self, agent: str) -> dict[str, float] | None:
        ratios = self.cost.get("cache_ratios", {}) or {}
        found = ratios.get(agent)
        return {key: float(value) for key, value in found.items()} if found else None

    def cache_ratio_for(self, agent: str, candidate: str) -> dict[str, float] | None:
        """The agent's cache ratios with any per-candidate override applied.

        The flat per-agent ratio holds for a whole provider until one model
        breaks it (Fable 5.1's 0.025x cache read). An override states only the
        rate that differs and merges over the agent's, so the rest of the
        family keeps being validated against one number.
        """
        base = self.cache_ratio(agent) or {}
        overrides = self.cost.get("cache_ratio_overrides", {}) or {}
        for pattern, ratios in overrides.items():
            if pat.glob_match(pattern, candidate):
                merged = dict(base)
                merged.update({key: float(value) for key, value in ratios.items()})
                return merged or None
        return base or None

    def witness_id(self, name: str) -> str:
        try:
            return self.witnesses[name]
        except KeyError as exc:  # pragma: no cover - policy authoring error
            raise PolicyError(f"invariant references unknown witness `{name}`") from exc

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

def load_policy(path: Path) -> Policy:
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except FileNotFoundError as exc:
        raise PolicyError(f"policy file not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise PolicyError(f"invalid policy YAML in {path}: {exc}") from exc

    anchors = raw.get("anchors") or {}
    band_raw = anchors.get("quality_band") or {}
    band = (float(band_raw.get("min", 0.5)), float(band_raw.get("max", 3.5)))
    if not band[0] < band[1]:
        raise PolicyError(f"quality_band min must be below max (got {band})")

    return Policy(
        path=path,
        raw=raw,
        quality_band=band,
        round_to=float(anchors.get("round_to", 0.01)),
        witnesses=dict(raw.get("witnesses") or {}),
        invariants=raw.get("invariants") or {},
        cost=raw.get("cost") or {},
        published_pricing=raw.get("published_pricing") or {},
        discovery=raw.get("discovery") or {},
        benchmark_scoring=raw.get("benchmark_scoring") or {},
    )
