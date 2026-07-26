"""Surgical reader/writer for an agent config's model catalog.

`examples/router-preferred.yaml` is the canonical multi-agent example: which
models each agent exposes, their `cost_rank`, and their `pricing`. It carries
long prose comments explaining load-bearing details (why `api_model` differs
from `id`, why cache rates matter to the demotion gate) and uses flow-style
maps that span lines, so — as with the score table — edits are line-level, never
a re-emit. `render()` on an unmodified file returns the original bytes.

Disabled models (`# - { id: ... }`) are first-class here: the convention is that
a newly discovered model lands commented-out until it has a score and a rank,
and a vanished one is commented-out for a release rather than deleted.
"""

from __future__ import annotations

import re
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_AGENT_NAME_RE = re.compile(r"^  - name:\s*(?P<name>\S+)\s*(?:#.*)?$")
_MODELS_KEY_RE = re.compile(r"^    models:\s*$")
# A model entry, enabled (`      - {`) or disabled (`      # - {`).
_MODEL_START_RE = re.compile(r"^(?P<indent>\s*)(?P<hash>#\s*)?-\s*\{(?P<rest>.*)$")
_ID_RE = re.compile(r'\bid:\s*"?(?P<id>[^",}\s]+)"?')


class CatalogError(RuntimeError):
    """The catalog could not be parsed the way the writer needs."""


def _same_decimals(old: str, value: float) -> str:
    """Format `value` with the decimal places the existing token used."""
    if "." in old:
        return f"{value:.{len(old.split('.')[1])}f}"
    return f"{value:g}"


@dataclass
class CatalogModel:
    agent: str
    model_id: str
    enabled: bool
    start: int
    end: int  # exclusive
    fields: dict[str, Any] = field(default_factory=dict)

    @property
    def candidate(self) -> str:
        return f"{self.agent}/{self.model_id}"

    @property
    def cost_rank(self) -> int | None:
        rank = self.fields.get("cost_rank")
        return int(rank) if rank is not None else None

    @property
    def pricing(self) -> dict[str, Any] | None:
        return self.fields.get("pricing")


@dataclass
class CatalogAgent:
    name: str
    models: list[CatalogModel] = field(default_factory=list)

    @property
    def enabled_models(self) -> list[CatalogModel]:
        return [model for model in self.models if model.enabled]


@dataclass
class Catalog:
    path: Path
    lines: list[str]
    agents: list[CatalogAgent]
    trailing_newline: bool = True

    def render(self) -> str:
        text = "\n".join(self.lines)
        return text + "\n" if self.trailing_newline else text

    def agent(self, name: str) -> CatalogAgent | None:
        return next((agent for agent in self.agents if agent.name == name), None)

    def model(self, candidate: str) -> CatalogModel | None:
        for agent in self.agents:
            for model in agent.models:
                if model.candidate == candidate:
                    return model
        return None

    def all_models(self) -> list[CatalogModel]:
        return [model for agent in self.agents for model in agent.models]

    # -- edits ------------------------------------------------------------
    def set_scalar(self, model: CatalogModel, key: str, value: Any) -> bool:
        """Rewrite `key: <value>` inside a model's span. False if absent."""
        pattern = re.compile(rf"(\b{re.escape(key)}:\s*)(?P<old>[^,}}\s]+)")
        for index in range(model.start, model.end):
            match = pattern.search(self.lines[index])
            if not match:
                continue
            old = match.group("old")
            rendered = _same_decimals(old, float(value)) if isinstance(value, float) else str(value)
            self.lines[index] = (
                self.lines[index][: match.start("old")] + rendered + self.lines[index][match.end("old") :]
            )
            model.fields[key] = value
            return True
        return False

    def set_price(self, model: CatalogModel, key: str, value: float) -> bool:
        """Rewrite one `pricing:` rate, preserving its decimal formatting."""
        if self.set_scalar(model, key, float(value)):
            pricing = dict(model.fields.get("pricing") or {})
            pricing[key] = value
            model.fields["pricing"] = pricing
            return True
        return False

    def disable(self, model: CatalogModel, note: str) -> None:
        """Comment a model out, tagging why — never delete it outright."""
        if not model.enabled:
            return
        for index in range(model.start, model.end):
            stripped = self.lines[index].lstrip()
            indent = self.lines[index][: len(self.lines[index]) - len(stripped)]
            self.lines[index] = f"{indent}# {stripped}" if stripped else self.lines[index]
        comment_indent = self.lines[model.start][
            : len(self.lines[model.start]) - len(self.lines[model.start].lstrip())
        ]
        self.lines.insert(model.start, f"{comment_indent}# {note}")
        self._shift(model.start, 1)
        model.end += 1
        model.enabled = False

    def add_disabled(self, agent_name: str, entry_lines: list[str]) -> None:
        """Append commented-out lines to the end of an agent's model list."""
        agent = self.agent(agent_name)
        if agent is None or not agent.models:
            raise CatalogError(f"cannot add a model to unknown/empty agent `{agent_name}`")
        at = max(model.end for model in agent.models)
        for offset, line in enumerate(entry_lines):
            self.lines.insert(at + offset, line)
        self._shift(at, len(entry_lines))

    def _shift(self, at: int, delta: int) -> None:
        """Keep spans valid after an insert at line `at`."""
        for agent in self.agents:
            for model in agent.models:
                if model.start >= at:
                    model.start += delta
                    model.end += delta
                elif model.end > at:
                    model.end += delta


def parse_catalog(path: Path) -> Catalog:
    text = path.read_text()
    trailing_newline = text.endswith("\n")
    lines = text[:-1].split("\n") if trailing_newline else text.split("\n")

    raw = yaml.safe_load(text) or {}
    parsed_fields: dict[str, dict[str, Any]] = {}
    for agent_spec in raw.get("agents") or []:
        for model_spec in agent_spec.get("models") or []:
            parsed_fields[f"{agent_spec['name']}/{model_spec['id']}"] = dict(model_spec)

    agents: list[CatalogAgent] = []
    current: CatalogAgent | None = None
    in_models = False
    index = 0
    while index < len(lines):
        line = lines[index]
        agent_match = _AGENT_NAME_RE.match(line)
        if agent_match:
            current = CatalogAgent(name=agent_match.group("name"))
            agents.append(current)
            in_models = False
            index += 1
            continue
        if _MODELS_KEY_RE.match(line):
            in_models = True
            index += 1
            continue
        if in_models and current is not None:
            model_match = _MODEL_START_RE.match(line)
            if model_match:
                start = index
                depth = 0
                # A flow map may wrap across lines; consume until braces balance.
                while index < len(lines):
                    depth += lines[index].count("{") - lines[index].count("}")
                    index += 1
                    if depth <= 0:
                        break
                span = "\n".join(lines[start:index])
                id_match = _ID_RE.search(span)
                if id_match is None:
                    raise CatalogError(f"{path}:{start + 1}: model entry has no `id:`")
                model_id = id_match.group("id")
                enabled = model_match.group("hash") is None
                current.models.append(
                    CatalogModel(
                        agent=current.name,
                        model_id=model_id,
                        enabled=enabled,
                        start=start,
                        end=index,
                        fields=parsed_fields.get(f"{current.name}/{model_id}", {})
                        if enabled
                        else {},
                    )
                )
                continue
            # A non-model, non-comment line at or above `models:` indentation
            # ends the list (e.g. the next agent's key).
            if line.strip() and not line.lstrip().startswith("#") and not line.startswith("      "):
                in_models = False
        index += 1

    if not agents:
        raise CatalogError(f"no agents found in {path}")
    return Catalog(path=path, lines=lines, agents=agents, trailing_newline=trailing_newline)


def render_disabled_model(
    indent: str, model_id: str, display_name: str, cost_rank: int, note: str
) -> list[str]:
    """A commented-out catalog line for a newly discovered model."""
    quoted = f'"{model_id}"' if any(char in model_id for char in "[]{}:,") else model_id
    # Wrap the note to the file's comment width instead of emitting one long line.
    width = 88 - len(indent) - 2
    lines = [f"{indent}# {line}" for line in textwrap.wrap(" ".join(note.split()), width=width)]
    lines.append(
        f"{indent}# - {{ id: {quoted}, display_name: {display_name}, cost_rank: {cost_rank} }}"
    )
    return lines
