"""Surgical reader/writer for the model catalog: `data/providers/<name>.yaml`.

Each provider file is the one definition of that provider: its default agent
settings, the two catalog-maintenance keys the updater reads (`cache_ratio`,
`discovery`), and `models:` — a mapping of family name to that family's
versions. Each version entry is one exact model:

    models:
      opus:
      - version: 5.5
        id: claude-opus-5-5          # the provider's real API model id, never an alias
        display_name: Claude Opus 5.5
        cost_rank: 4
        pricing: { ... }
        evidence: [ ... ]            # its own published results
        outranks: [claude-opus-4-6]  # bare id = same provider, else provider/id
        default_quality: 2.73        # computed from `evidence` by the updater
        quality: { ... }

The provider comes from the file (`provider:`), the family from the group key.
There are no patterns, no aliases and no inherited scores: a model is found by
its exact `id` (case-insensitive) within its provider.

The files are hand-maintained and heavily commented, so a round trip through a
YAML emitter would delete the most valuable part of them. This module keeps
each file as lines, knows which line span each entry occupies, and rewrites
only the lines it must:

  * `set_quality` rewrites an entry's `default_quality` / `quality` lines;
  * `clear_quality` removes them from an entry with no evidence;
  * `disable` comments a vanished entry out with a `retired` note;
  * `add_parked` writes a newly discovered model as a commented-out entry.

`render()` on an unmodified file returns the original bytes — that round trip is
the licence to touch the files at all, and it is asserted in the tests.
"""

from __future__ import annotations

import re
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .policy import TASK_CLASSES, Policy, PolicyError, load_policy

_MODELS_RE = re.compile(r"^models:\s*(?:#.*)?$")
_TOP_LEVEL_RE = re.compile(r"^[^\s#]")
_FAMILY_RE = re.compile(r"^  (?P<family>[^\s#-][^:#]*):\s*(?:#.*)?$")
_ENTRY_RE = re.compile(r"^(?P<indent> *)- version:\s*(?P<text>[^#]*?)\s*(?:#.*)?$")
_COMMENT_OR_BLANK_RE = re.compile(r"^\s*(#.*)?$")
# A commented-out (retired or parked) entry: `# - version: x` then `#   id: y`.
_PARKED_ENTRY_RE = re.compile(r"^\s*#\s*- version:")
_PARKED_ID_RE = re.compile(r"^\s*#\s*id:\s*\"?(?P<id>[^\"\s#]+)\"?")
_VERSION_TEXT_RE = re.compile(r"^\d+(?:\.\d+)*$")

# Fields every entry must carry (`model-missing-field`).
REQUIRED_FIELDS = ("version", "id", "display_name", "cost_rank", "pricing")


class CatalogError(RuntimeError):
    """A provider file could not be parsed the way the writer needs."""


def format_quality(value: float) -> str:
    """Two decimals, the files' convention."""
    return f"{value:.2f}"


def render_quality_map(quality: dict[str, float]) -> str:
    inner = ", ".join(f"{task_class}: {format_quality(quality[task_class])}" for task_class in TASK_CLASSES)
    return "{ " + inner + " }"


def parse_version(text: str) -> tuple[int, ...] | None:
    """A dotted version compared component-wise numerically (`4.10` > `4.9`).

    Always parsed from the scalar's TEXT: YAML reads `5.5` as a float and
    `4.10` as 4.1, so the parsed value is never trusted.
    """
    text = text.strip().strip("'\"")
    if not _VERSION_TEXT_RE.match(text):
        return None
    return tuple(int(part) for part in text.split("."))


def split_ref(ref: str, provider: str) -> tuple[str, str]:
    """A model reference: a bare id means the same provider."""
    if "/" in ref:
        ref_provider, ref_id = ref.split("/", 1)
        return ref_provider, ref_id
    return provider, ref


@dataclass
class ModelEntry:
    """One version entry of a family, and the line span it occupies."""

    file: "ProviderFile"
    family: str
    start: int
    end: int  # exclusive
    fields: dict[str, Any] = field(default_factory=dict)
    version_text: str = ""
    enabled: bool = True

    @property
    def provider(self) -> str:
        return self.file.provider

    @property
    def model_id(self) -> str:
        return str(self.fields.get("id", ""))

    @property
    def key(self) -> str:
        return f"{self.provider}/{self.model_id}"

    @property
    def version(self) -> tuple[int, ...] | None:
        return parse_version(self.version_text)

    @property
    def evidence(self) -> list[dict[str, Any]]:
        return list(self.fields.get("evidence") or [])

    @property
    def outranks(self) -> list[str]:
        value = self.fields.get("outranks") or []
        return [str(item) for item in (value if isinstance(value, list) else [value])]

    @property
    def rank_exception(self) -> str | None:
        value = self.fields.get("rank_exception")
        return " ".join(str(value).split()) if value else None

    @property
    def cost_rank(self) -> int | None:
        rank = self.fields.get("cost_rank")
        try:
            return int(rank) if rank is not None and not isinstance(rank, bool) else None
        except (TypeError, ValueError):
            return None

    @property
    def pricing(self) -> dict[str, Any] | None:
        pricing = self.fields.get("pricing")
        return pricing if isinstance(pricing, dict) else None

    @property
    def scored(self) -> bool:
        return "quality" in self.fields or "default_quality" in self.fields

    def quality(self, task_class: str) -> float:
        """Resolved quality, falling back to `default_quality` like the router."""
        explicit = (self.fields.get("quality") or {}).get(task_class)
        if explicit is not None:
            return float(explicit)
        return float(self.fields.get("default_quality", 0.5))

    def qualities(self) -> dict[str, float]:
        return {task_class: self.quality(task_class) for task_class in TASK_CLASSES}

    @property
    def field_indent(self) -> str:
        line = self.file.lines[self.start]
        return " " * (len(line) - len(line.lstrip()) + 2)


@dataclass
class ProviderFile:
    path: Path
    raw: dict[str, Any]
    lines: list[str]
    entries: list[ModelEntry] = field(default_factory=list)
    # family -> line index just past its last line (where a new entry goes)
    family_ends: dict[str, int] = field(default_factory=dict)
    models_end: int = 0
    parked: list[str] = field(default_factory=list)
    trailing_newline: bool = True

    @property
    def provider(self) -> str:
        return str(self.raw.get("provider") or self.path.stem)

    def render(self) -> str:
        text = "\n".join(self.lines)
        return text + "\n" if self.trailing_newline else text

    def _shift(self, at: int, delta: int) -> None:
        for entry in self.entries:
            if entry.start >= at:
                entry.start += delta
                entry.end += delta
            elif entry.end > at:
                entry.end += delta
        for family, end in self.family_ends.items():
            if end >= at:
                self.family_ends[family] = end + delta
        if self.models_end >= at:
            self.models_end += delta

    def _replace(self, start: int, end: int, new: list[str]) -> None:
        self.lines[start:end] = new
        delta = len(new) - (end - start)
        if delta:
            for entry in self.entries:
                if entry.start >= end:
                    entry.start += delta
                    entry.end += delta
                elif entry.end >= end and entry.start <= start:
                    entry.end += delta
            for family, family_end in self.family_ends.items():
                if family_end >= end:
                    self.family_ends[family] = family_end + delta
            if self.models_end >= end:
                self.models_end += delta


@dataclass
class Catalog:
    """Every provider file, plus the scoring procedure from classifier.yaml."""

    files: list[ProviderFile]
    classifier_path: Path
    classifier_raw: dict[str, Any]

    @property
    def policy(self) -> Policy:
        return load_policy(self.classifier_raw, self.classifier_path, {f.provider: f.raw for f in self.files})

    @property
    def rows(self) -> list[ModelEntry]:
        return [entry for f in self.files for entry in f.entries if entry.enabled]

    def providers(self) -> list[str]:
        return [f.provider for f in self.files]

    def file(self, provider: str) -> ProviderFile | None:
        return next((f for f in self.files if f.provider == provider), None)

    def by_provider(self, provider: str) -> list[ModelEntry]:
        return [row for row in self.rows if row.provider == provider]

    def find(self, model_id: str, provider: str | None = None) -> ModelEntry | None:
        """The entry whose exact `id` is `model_id` (case-insensitive)."""
        for row in self.rows:
            if provider is not None and row.provider.lower() != provider.lower():
                continue
            if row.model_id.lower() == model_id.lower():
                return row
        return None

    def resolve_ref(self, ref: str, provider: str) -> ModelEntry | None:
        ref_provider, ref_id = split_ref(ref, provider)
        return self.find(ref_id, ref_provider)

    def known_ids(self, provider: str) -> list[str]:
        """Every id a provider file names, live or commented-out."""
        found = self.file(provider)
        if found is None:
            return []
        return [entry.model_id for entry in found.entries if entry.enabled] + list(found.parked)

    # -- edits ------------------------------------------------------------
    def set_quality(self, row: ModelEntry, default_quality: float, quality: dict[str, float]) -> bool:
        """Replace only an entry's `default_quality` / `quality` lines."""
        if (
            "quality" in row.fields
            and abs(float(row.fields.get("default_quality", 0.5)) - default_quality) < 1e-9
            and row.qualities() == quality
        ):
            return False
        indent = row.field_indent
        replacement = [
            f"{indent}default_quality: {format_quality(default_quality)}",
            f"{indent}quality: {render_quality_map(quality)}",
        ]
        at = _remove_quality_lines(row)
        if at is None:
            at = row.end
        row.file._replace(at, at, replacement)
        row.fields["default_quality"] = default_quality
        row.fields["quality"] = dict(quality)
        return True

    def clear_quality(self, row: ModelEntry) -> bool:
        """Remove an entry's quality lines (an entry with no evidence is unscored)."""
        if _remove_quality_lines(row) is None:
            return False
        row.fields.pop("default_quality", None)
        row.fields.pop("quality", None)
        return True

    def disable(self, row: ModelEntry, note: str) -> None:
        """Comment an entry out, tagging why — never delete it outright."""
        if not row.enabled:
            return
        lines = row.file.lines
        indent = re.match(r" *", lines[row.start]).group(0)  # type: ignore[union-attr]
        commented = [_comment_out(line) for line in lines[row.start : row.end]]
        row.file._replace(row.start, row.end, [*wrap_comment(note, indent + "  "), *commented])
        row.enabled = False
        row.file.parked.append(row.model_id)

    def add_parked(self, provider: str, model_id: str, lines: list[str]) -> None:
        """Append a commented-out entry at the end of the provider's `models:`."""
        found = self.file(provider)
        if found is None:
            raise CatalogError(f"no data/providers/{provider}.yaml to add `{model_id}` to")
        at = found.models_end
        found.lines[at:at] = lines
        found._shift(at, len(lines))
        found.models_end = at + len(lines)
        found.parked.append(model_id)


def _remove_quality_lines(row: ModelEntry) -> int | None:
    """Drop an entry's quality lines; the index they were at (None if absent)."""
    lines = row.file.lines
    indent = row.field_indent
    doomed: list[int] = []
    for index in range(row.start, row.end):
        line = lines[index]
        if line.startswith(f"{indent}default_quality:") or line.startswith(f"{indent}quality:"):
            doomed.append(index)
            if line.rstrip() == f"{indent}quality:":
                follow = index + 1
                while follow < row.end and lines[follow].startswith(indent + "  "):
                    doomed.append(follow)
                    follow += 1
    if not doomed:
        return None
    first = doomed[0]
    for index in sorted(set(doomed), reverse=True):
        row.file._replace(index, index + 1, [])
    return first


def _comment_out(line: str) -> str:
    if not line.strip():
        return line
    stripped = line.lstrip()
    return f"{line[: len(line) - len(stripped)]}# {stripped}"


def wrap_comment(text: str, indent: str = "  ", width: int = 80) -> list[str]:
    collapsed = " ".join(text.split())
    if not collapsed:
        return []
    return [f"{indent}# {line}" for line in textwrap.wrap(collapsed, width=width - len(indent) - 2)]


def parse_provider_text(text: str, path: Path) -> ProviderFile:
    trailing_newline = text.endswith("\n")
    lines = text[:-1].split("\n") if trailing_newline else text.split("\n")
    try:
        raw = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise CatalogError(f"invalid YAML in {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise CatalogError(f"{path} must be a mapping")
    found = ProviderFile(path=path, raw=raw, lines=lines, trailing_newline=trailing_newline)

    families = raw.get("models") or {}
    if not isinstance(families, dict):
        raise CatalogError(f"{path}: `models:` must map each family name to its versions")
    models_line = next((i for i, line in enumerate(lines) if _MODELS_RE.match(line)), None)
    if models_line is None:
        found.models_end = len(lines)
        return found
    section_end = next(
        (i for i in range(models_line + 1, len(lines)) if _TOP_LEVEL_RE.match(lines[i])), len(lines)
    )
    while section_end - 1 > models_line and not lines[section_end - 1].strip():
        section_end -= 1
    found.models_end = section_end

    # Walk the section: family keys, entry starts, and commented-out entries.
    starts: list[tuple[str, int, str]] = []
    family: str | None = None
    pending_parked = False
    for index in range(models_line + 1, section_end):
        line = lines[index]
        family_match = _FAMILY_RE.match(line)
        if family_match:
            family = family_match.group("family").strip()
            continue
        entry_match = _ENTRY_RE.match(line)
        if entry_match:
            if family is None:
                raise CatalogError(f"{path}:{index + 1}: a model entry outside any family")
            starts.append((family, index, entry_match.group("text").strip("'\"")))
            continue
        if _PARKED_ENTRY_RE.match(line):
            pending_parked = True
            continue
        parked_id = _PARKED_ID_RE.match(line)
        if parked_id and pending_parked:
            found.parked.append(parked_id.group("id"))
            pending_parked = False

    parsed = [(name, item) for name, items in families.items() for item in (items or [])]
    if len(parsed) != len(starts) or [name for name, _ in parsed] != [name for name, _, _ in starts]:
        raise CatalogError(
            f"{path}: found {len(starts)} `- version:` entries under `models:` but YAML has "
            f"{len(parsed)}; every entry must start with `- version:`"
        )

    # Each entry runs to the next entry / family key / section end, minus the
    # comment and blank lines just above that boundary (they introduce what follows).
    boundaries = sorted(
        {index for _, index, _ in starts}
        | {i for i in range(models_line + 1, section_end) if _FAMILY_RE.match(lines[i])}
        | {section_end}
    )
    for (name, start, version_text), (_, fields) in zip(starts, parsed):
        if not isinstance(fields, dict):
            raise CatalogError(f"{path}:{start + 1}: a model entry must be a mapping")
        end = next(boundary for boundary in boundaries if boundary > start)
        while end - 1 > start and _COMMENT_OR_BLANK_RE.match(lines[end - 1]):
            end -= 1
        found.entries.append(
            ModelEntry(file=found, family=name, start=start, end=end, fields=dict(fields), version_text=version_text)
        )
        found.family_ends[name] = end
    return found


def load_catalog(data_dir: Path) -> Catalog:
    """`<data_dir>/providers/*.yaml` plus `<data_dir>/classifier.yaml`."""
    return load_catalog_texts(
        {path: path.read_text() for path in sorted((data_dir / "providers").glob("*.yaml"))},
        data_dir / "classifier.yaml",
    )


def load_catalog_texts(texts: dict[Path, str], classifier_path: Path) -> Catalog:
    """Parse provider files from text, so a proposed rewrite can be validated."""
    if not texts:
        raise CatalogError(f"no provider files beside {classifier_path} (expected providers/*.yaml)")
    try:
        classifier_raw = yaml.safe_load(classifier_path.read_text()) or {}
    except FileNotFoundError as exc:
        raise CatalogError(f"{classifier_path} does not exist") from exc
    except yaml.YAMLError as exc:
        raise CatalogError(f"invalid YAML in {classifier_path}: {exc}") from exc
    files = [parse_provider_text(text, path) for path, text in texts.items()]
    return Catalog(files=files, classifier_path=classifier_path, classifier_raw=classifier_raw)


def render_parked_entry(model_id: str, cost_rank: int, note: str) -> list[str]:
    """A commented-out family block holding a newly discovered model.

    Discovery sees only an id, and the updater never derives a family or a
    version from one, so the entry lands under a `TODO` family at the end of
    `models:`. Enabling it: move it under its family, set its version, fill in
    pricing, check cost_rank, record its evidence, uncomment, rerun --apply.
    """
    quoted = f'"{model_id}"' if any(char in model_id for char in "[]{}:,#") else model_id
    body = [
        "  # TODO-family:",
        "  # - version: TODO",
        f"  #   id: {quoted}",
        f"  #   display_name: {model_id.replace('-', ' ').title()}",
        f"  #   cost_rank: {cost_rank}",
        "  #   pricing: { input_per_mtok: TODO, output_per_mtok: TODO }",
        "  #   evidence: []",
    ]
    return ["", *wrap_comment(note, "  "), *body]


__all__ = [
    "Catalog",
    "CatalogError",
    "ModelEntry",
    "PolicyError",
    "ProviderFile",
    "REQUIRED_FIELDS",
    "load_catalog",
    "load_catalog_texts",
    "parse_version",
    "render_parked_entry",
    "split_ref",
]
