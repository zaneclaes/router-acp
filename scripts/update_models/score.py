"""Propose benchmark-derived scores for models that do not have them yet.

Deliberately narrow: this proposes curves for **unscored** models (a new
release, a newly enabled id) and leaves every scored family alone. Rescoring a
live family is what flips `tests/golden.rs`, so it only happens when an operator
asks for it by name (`--rescore <pattern>`) — never as a side effect of adding a
model.

A proposal is anchored by fixed benchmark calibrations, never by the live model
pool. Adding a model therefore cannot move any other model's numbers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import patterns as pat
from .policy import TASK_CLASSES, Policy
from .scores_file import ScoreEntry, ScoresFile, render_entry

@dataclass
class ScoreProposal:
    """A new score-table entry awaiting review."""

    candidate: str
    pattern: str
    tier: str
    base: float
    quality: dict[str, float]
    scalars: dict[str, Any]
    rationale: str
    provisional: bool = True
    insert_before: str | None = None
    risks: list[str] = field(default_factory=list)

    def to_entry(self, comment: str) -> ScoreEntry:
        return render_entry(
            pattern=self.pattern,
            scalars=self.scalars,
            default_quality=self.base,
            quality=self.quality,
            comment=comment,
        )


def suggest_pattern(model_id: str) -> str:
    """A glob for a new id, keyed on the WHOLE id.

    Deliberately the most specific choice available. The shipped table keys on
    the model line (`*sol*`, `*terra*`) so version bumps inherit, and that is
    the better pattern — but guessing which substring is the "line" is a
    judgment call, and guessing it wrong silently captures other models
    (`*mini*` swallowing every Gemini id is exactly that mistake). So the
    deterministic default cannot shadow anything, and generalizing it to a line
    pattern is left to the goose step or the reviewer.
    """
    return f"*{model_id.lower()}*"


def _pattern_is_safe(scores: ScoresFile, pattern: str) -> bool:
    """True when adding `pattern` shadows nothing already in the table."""
    if pattern in scores.patterns:
        return False
    return not any(
        pat.is_narrower(existing, pattern) and not pat.is_narrower(pattern, existing)
        for existing in scores.patterns
    )


def insertion_anchor(scores: ScoresFile, pattern: str) -> str | None:
    """The first existing pattern that would shadow `pattern`.

    Inserting immediately before it is the minimal placement that keeps the
    first-match table correct.
    """
    for existing in scores.patterns:
        if pat.is_narrower(pattern, existing) and not pat.is_narrower(existing, pattern):
            return existing
    return None


def propose_from_benchmarks(
    policy: Policy,
    scores: ScoresFile,
    candidate: str,
    pattern: str,
    base: float,
    quality: dict[str, float],
    observations: int,
) -> ScoreProposal:
    """Build a proposal only after benchmark evidence has been calibrated."""
    model_id = candidate.split("/", 1)[1] if "/" in candidate else candidate
    safe_pattern = pattern if _pattern_is_safe(scores, pattern) else suggest_pattern(model_id)
    return ScoreProposal(
        candidate=candidate,
        pattern=safe_pattern,
        tier="benchmark",
        base=base,
        quality=quality,
        scalars={},
        rationale=f"derived from {observations} calibrated benchmark observations",
        insert_before=insertion_anchor(scores, safe_pattern),
        risks=[
            "context_window is unknown — set it from provider documentation before enabling"
        ],
    )


def unscored_candidates(scores: ScoresFile, candidates: list[str]) -> list[str]:
    """Candidates that would fall through to the neutral 0.5 default."""
    return [
        candidate
        for candidate in candidates
        if pat.resolve(scores.patterns, candidate) is None
    ]


def rank_suggestion(policy: Policy, pricing: dict[str, Any] | None, peers: list[tuple[int, float]]) -> int:
    """Suggest a `cost_rank` from blended price against already-ranked peers.

    Ordinal, not absolute: the new model takes the rank of the cheapest peer it
    is at least as expensive as, so it slots into the existing ladder instead of
    redefining it.
    """
    low, high = policy.ladder
    price = policy.blended_price(pricing)
    if price is None or not peers:
        return low
    ranked = sorted(peers, key=lambda pair: pair[1])
    suggestion = low
    for rank, peer_price in ranked:
        if price >= peer_price:
            suggestion = rank
    return max(low, min(high, suggestion))


def summarize(proposals: list[ScoreProposal]) -> str:
    if not proposals:
        return "no score proposals"
    lines = []
    for proposal in proposals:
        window = ", ".join(
            f"{task_class} {proposal.quality[task_class]:.2f}" for task_class in TASK_CLASSES
        )
        lines.append(f"{proposal.candidate} → `{proposal.pattern}` [{proposal.tier}] {window}")
    return "\n".join(lines)
