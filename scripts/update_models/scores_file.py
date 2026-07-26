"""Surgical reader/writer for `data/scores.yaml`.

The score table is hand-authored and heavily commented — the comments carry the
routing rationale (benchmark provenance, why `*grok*code*` must
precede `*grok*`), so a round trip through a YAML emitter would delete the most
valuable part of the file. This module therefore keeps the file as lines, tracks
which lines belong to which entry (including the blank-line spacing between
them), and only moves or rewrites the lines it must.

`render()` on an unmodified file returns the original bytes — that round trip is
the licence to touch the file at all, and it is asserted in the tests.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .policy import TASK_CLASSES

_ENTRY_RE = re.compile(r'^  - pattern:\s*"(?P<pattern>[^"]*)"\s*(?:#.*)?$')
_CANDIDATES_RE = re.compile(r"^candidates:\s*$")
_COMMENT_OR_BLANK_RE = re.compile(r"^\s*(#.*)?$")

# Key order for a generated entry, matching the shipped hand-written ones.
_SCALAR_KEYS = [
    "coding_tier",
    "coding_percentile",
    "context_window",
    "max_output_tokens",
    "adaptive_thinking",
    "effort",
]


class ScoresFileError(RuntimeError):
    """The score table could not be parsed the way the writer needs."""


def _format_quality(value: float) -> str:
    """Two decimals — the convention the most recent entries use."""
    return f"{value:.2f}"


@dataclass
class ScoreEntry:
    """One `- pattern:` block, plus the comments and spacing above it."""

    pattern: str
    comment_lines: list[str]
    body_lines: list[str]
    fields: dict[str, Any] = field(default_factory=dict)
    # Blank lines separating this entry from what precedes it, preserved so an
    # untouched file renders byte-identically.
    gap_before: int = 1

    @property
    def lines(self) -> list[str]:
        return [*self.comment_lines, *self.body_lines]

    def quality(self, task_class: str) -> float:
        """Resolved quality, falling back to `default_quality` like the router."""
        explicit = (self.fields.get("quality") or {}).get(task_class)
        if explicit is not None:
            return float(explicit)
        return float(self.fields.get("default_quality", 0.5))

    def qualities(self) -> dict[str, float]:
        return {task_class: self.quality(task_class) for task_class in TASK_CLASSES}


@dataclass
class ScoresFile:
    path: Path
    preamble: list[str]
    entries: list[ScoreEntry]
    epilogue: list[str] = field(default_factory=list)
    trailing_newline: bool = True

    @property
    def patterns(self) -> list[str]:
        return [entry.pattern for entry in self.entries]

    def entry(self, pattern: str) -> ScoreEntry | None:
        for existing in self.entries:
            if existing.pattern == pattern:
                return existing
        return None

    def render(self, order: list[str] | None = None) -> str:
        entries = self.entries
        if order is not None:
            by_pattern = {entry.pattern: entry for entry in self.entries}
            missing = [pattern for pattern in order if pattern not in by_pattern]
            if missing or len(order) != len(self.entries):
                raise ScoresFileError(
                    f"reorder must be a permutation of existing patterns: {missing}"
                )
            entries = [by_pattern[pattern] for pattern in order]
            # Spacing belongs to position, not to the entry: the first entry
            # keeps the file's own preamble gap, the rest are separated by one
            # blank line (the shipped convention).
            gaps = [entry.gap_before for entry in self.entries]
            for index, entry in enumerate(entries):
                entry.gap_before = gaps[index]

        out: list[str] = list(self.preamble)
        for entry in entries:
            out.extend([""] * entry.gap_before)
            out.extend(entry.lines)
        out.extend(self.epilogue)
        text = "\n".join(out)
        return text + "\n" if self.trailing_newline else text

    def insert_entry(self, entry: ScoreEntry, before: str | None = None) -> None:
        """Place a new entry before `before` (or at the end)."""
        if self.entry(entry.pattern) is not None:
            raise ScoresFileError(f"pattern already present: {entry.pattern}")
        if before is None:
            self.entries.append(entry)
            return
        for index, existing in enumerate(self.entries):
            if existing.pattern == before:
                self.entries.insert(index, entry)
                return
        raise ScoresFileError(f"anchor pattern not found: {before}")

    def set_quality(
        self,
        pattern: str,
        default_quality: float,
        quality: dict[str, float],
    ) -> bool:
        """Replace only an entry's benchmark-derived quality fields."""
        entry = self.entry(pattern)
        if entry is None:
            raise ScoresFileError(f"pattern not found: {pattern}")
        if (
            abs(float(entry.fields.get("default_quality", 0.5)) - default_quality) < 1e-9
            and entry.qualities() == quality
        ):
            return False

        start = next(
            (index for index, line in enumerate(entry.body_lines) if line.startswith("    default_quality:")),
            None,
        )
        if start is None:
            raise ScoresFileError(f"`{pattern}` has no default_quality")
        end = start + 1
        if end < len(entry.body_lines) and entry.body_lines[end] == "    quality:":
            end += 1
            while end < len(entry.body_lines) and entry.body_lines[end].startswith("      "):
                end += 1
        replacement = [f"    default_quality: {_format_quality(default_quality)}", "    quality:"]
        replacement.extend(
            f"      {task_class}: {_format_quality(quality[task_class])}"
            for task_class in TASK_CLASSES
        )
        entry.body_lines[start:end] = replacement
        entry.fields["default_quality"] = default_quality
        entry.fields["quality"] = dict(quality)
        return True


def _leading_blanks(lines: list[str]) -> int:
    count = 0
    for line in lines:
        if line.strip():
            break
        count += 1
    return count


def _trailing_blanks(lines: list[str]) -> int:
    count = 0
    for line in reversed(lines):
        if line.strip():
            break
        count += 1
    return count


def parse_scores(path: Path) -> ScoresFile:
    return parse_scores_text(path.read_text(), path)


def parse_scores_text(text: str, path: Path) -> ScoresFile:
    """Parse from text, so a proposed rewrite can be validated before it lands."""
    trailing_newline = text.endswith("\n")
    lines = text[:-1].split("\n") if trailing_newline else text.split("\n")

    starts = [index for index, line in enumerate(lines) if _ENTRY_RE.match(line)]
    if not starts:
        raise ScoresFileError(f"no `- pattern:` entries found in {path}")
    candidates_line = next(
        (index for index, line in enumerate(lines) if _CANDIDATES_RE.match(line)), None
    )
    if candidates_line is None:
        raise ScoresFileError(f"no `candidates:` key found in {path}")

    # The comment/blank run above an entry belongs to that entry — except for the
    # first one, where comments between `candidates:` and it document the list
    # itself (the pattern-ordering rule), so they stay in the preamble.
    block_starts: list[int] = []
    for position, start in enumerate(starts):
        if position == 0:
            block_starts.append(start)
            continue
        floor = starts[position - 1]
        index = start
        while index - 1 > floor and _COMMENT_OR_BLANK_RE.match(lines[index - 1]):
            index -= 1
        block_starts.append(index)

    raw = yaml.safe_load(text) or {}
    parsed_by_pattern: dict[str, dict[str, Any]] = {}
    for item in raw.get("candidates") or []:
        fields = dict(item)
        parsed_by_pattern[fields.pop("pattern")] = fields

    entries: list[ScoreEntry] = []
    epilogue: list[str] = []
    for position, start in enumerate(starts):
        block_start = block_starts[position]
        is_last = position + 1 == len(starts)
        block_end = len(lines) if is_last else block_starts[position + 1]
        block = lines[block_start:block_end]

        gap = _leading_blanks(block)
        tail = _trailing_blanks(block) if is_last else 0
        if tail:
            epilogue = block[len(block) - tail :]
            block = block[: len(block) - tail]
        body_offset = start - block_start

        pattern = _ENTRY_RE.match(lines[start]).group("pattern")  # type: ignore[union-attr]
        entries.append(
            ScoreEntry(
                pattern=pattern,
                comment_lines=block[gap:body_offset],
                body_lines=block[body_offset:],
                fields=parsed_by_pattern.get(pattern, {}),
                gap_before=gap,
            )
        )

    return ScoresFile(
        path=path,
        preamble=lines[: block_starts[0]],
        entries=entries,
        epilogue=epilogue,
        trailing_newline=trailing_newline,
    )


def render_entry(
    pattern: str,
    scalars: dict[str, Any],
    default_quality: float,
    quality: dict[str, float],
    comment: str = "",
) -> ScoreEntry:
    """Build a new entry block in the shipped file's style."""
    comment_lines = (
        [line.rstrip() for line in comment.splitlines() if line.strip()] if comment else []
    )

    body = [f'  - pattern: "{pattern}"']
    for key in _SCALAR_KEYS:
        if key in scalars and scalars[key] is not None:
            value = scalars[key]
            rendered = str(value).lower() if isinstance(value, bool) else value
            body.append(f"    {key}: {rendered}")
    body.append(f"    default_quality: {_format_quality(default_quality)}")
    if quality:
        body.append("    quality:")
        for task_class in TASK_CLASSES:
            if task_class in quality:
                body.append(f"      {task_class}: {_format_quality(quality[task_class])}")

    return ScoreEntry(
        pattern=pattern,
        comment_lines=comment_lines,
        body_lines=body,
        fields={
            **{key: value for key, value in scalars.items() if value is not None},
            "default_quality": default_quality,
            "quality": dict(quality),
        },
        gap_before=1,
    )
