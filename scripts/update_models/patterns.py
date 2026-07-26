"""Glob matching and pattern-specificity ordering for the score table.

`data/scores.yaml` is a FIRST-MATCH-WINS list, so a broad pattern placed above
a narrow one silently swallows it. The two bugs this module exists to catch:

  * `*gpt-5*` above `*mini*` once routed hour-long investigations to
    gpt-5.4-mini.
  * `*mini*` above `*gemini*pro*` scores every Gemini id as a mini-class model,
    because "gemini" literally contains the substring "mini".

Both are the same shape: pattern A is strictly narrower than pattern B, so A
must come first. `order_violations` derives that relation mechanically instead
of relying on someone noticing the substring.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# A character that cannot occur in an `agent/model` id, used to stand in for a
# `*` when testing one pattern against another's literal skeleton.
_WILDCARD_WITNESS = "\x00"


def glob_match(pattern: str, text: str) -> bool:
    """Case-insensitive whole-string match where `*` is any run of characters.

    Mirrors `glob_match` in `src/candidate.rs`; the Python side must resolve
    candidates exactly the way the router does or the invariants check a
    different table than the one that ships.
    """
    regex = ".*".join(re.escape(part) for part in pattern.split("*"))
    return re.fullmatch(regex, text, flags=re.IGNORECASE | re.DOTALL) is not None


def witness(pattern: str) -> str:
    """The pattern's literal skeleton, with each `*` replaced by a stand-in."""
    return pattern.replace("*", _WILDCARD_WITNESS)


def is_narrower(a: str, b: str) -> bool:
    """True when every id matching `a` also matches `b` (so `a` ⊆ `b`).

    Decided by matching `b` against `a`'s literal skeleton: the stand-in char
    can only be consumed by a `*` in `b`, so a match means `b`'s literal
    segments all sit inside `a`'s literal segments in order — and then any id
    matching `a` necessarily matches `b` too.
    """
    return glob_match(b, witness(a))


@dataclass(frozen=True)
class OrderViolation:
    """`narrow` is shadowed by `broad`, which is listed above it."""

    narrow: str
    broad: str
    narrow_index: int
    broad_index: int

    def describe(self) -> str:
        return (
            f"`{self.narrow}` (position {self.narrow_index + 1}) is shadowed by the broader "
            f"`{self.broad}` (position {self.broad_index + 1}): every id matching "
            f"`{self.narrow}` also matches `{self.broad}`, and first match wins"
        )


def duplicate_patterns(patterns: list[str]) -> list[str]:
    """Patterns listed more than once — the later copy is dead."""
    seen: set[str] = set()
    dupes: list[str] = []
    for pattern in patterns:
        key = pattern.lower()
        if key in seen and pattern not in dupes:
            dupes.append(pattern)
        seen.add(key)
    return dupes


def order_violations(patterns: list[str]) -> list[OrderViolation]:
    """Every (narrow, broad) pair where the broad pattern is listed first."""
    violations: list[OrderViolation] = []
    for broad_index, broad in enumerate(patterns):
        for narrow_index in range(broad_index + 1, len(patterns)):
            narrow = patterns[narrow_index]
            if narrow.lower() == broad.lower():
                continue
            if is_narrower(narrow, broad) and not is_narrower(broad, narrow):
                violations.append(
                    OrderViolation(
                        narrow=narrow,
                        broad=broad,
                        narrow_index=narrow_index,
                        broad_index=broad_index,
                    )
                )
    return violations


def resolve(patterns: list[str], candidate_id: str) -> str | None:
    """The pattern that wins the first-match race for `candidate_id`."""
    for pattern in patterns:
        if glob_match(pattern, candidate_id):
            return pattern
    return None


def sorted_by_specificity(patterns: list[str]) -> list[str]:
    """Reorder so no pattern is shadowed, preserving existing order otherwise.

    A stable insertion sort: each pattern moves up to just before the first
    pattern that would shadow it and stays put relative to everything else, so
    a fix touches only the lines it has to.
    """
    ordered: list[str] = []
    for pattern in patterns:
        insert_at = len(ordered)
        for index, placed in enumerate(ordered):
            if is_narrower(pattern, placed) and not is_narrower(placed, pattern):
                insert_at = index
                break
        ordered.insert(insert_at, pattern)
    return ordered
