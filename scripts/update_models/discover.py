"""Discover which models each agent currently exposes.

Design constraint that outranks completeness: **a failed probe must never look
like "the provider retired everything."** Adapters report a `status`, and only a
successful probe is allowed to propose removals. Anything else falls back to the
configured catalog as last-known-good and says so in the report.

Two adapter kinds today:

  * `command` — shell out to the vendor CLI (`grok models`) and parse ids out of
    its output. Requires the operator's existing CLI auth; no keys here.
  * `snapshot` — no offline model list exists (claude-agent-acp enumerates over
    an ACP session; codex reads its rollout config), so the configured catalog
    is the record and drift is a human check. Documented per agent in
    `data/model-policy.yaml`.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Model-id shapes seen across the vendor CLIs, e.g. `grok-4.5`,
# `gpt-5.6-sol`, `claude-opus-5`, `kimi-k2-thinking`.
_ID_RE = re.compile(r"\b[a-z][a-z0-9]*(?:[.-][a-z0-9]+)+\b", re.IGNORECASE)
# Where a CLI starts listing models, if it says so at all.
_MODELS_SECTION_RE = re.compile(r"available models?:", re.IGNORECASE)
# A hostname has the same shape as a model id ("grok.com" vs "grok-4.5").
_TLD_RE = re.compile(r"\.(com|ai|io|net|org|dev|app|co|sh|xyz|cloud)$", re.IGNORECASE)
_PROBE_TIMEOUT_SECS = 20

OK = "ok"
UNAVAILABLE = "unavailable"  # CLI missing or not authed — do not infer removals
SNAPSHOT = "snapshot"  # no machine-readable list exists for this agent
FAILED = "failed"  # probe ran and broke


@dataclass
class AgentDiscovery:
    agent: str
    status: str
    method: str
    available: list[str] = field(default_factory=list)
    # Models the catalog routes to today. Drives removals.
    configured: list[str] = field(default_factory=list)
    # Every id the catalog mentions, including commented-out ones. Suppresses
    # re-adding a model that is already parked awaiting a score, which would
    # otherwise append a duplicate entry on every run.
    known: list[str] = field(default_factory=list)
    note: str = ""
    error: str = ""

    @property
    def trustworthy(self) -> bool:
        """Only an `ok` probe may drive removals."""
        return self.status == OK

    @property
    def to_add(self) -> list[str]:
        if not self.trustworthy:
            return []
        seen = set(self.known) | set(self.configured)
        return [model for model in self.available if model not in seen]

    @property
    def to_remove(self) -> list[str]:
        if not self.trustworthy:
            return []
        return [model for model in self.configured if model not in self.available]

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "method": self.method,
            "available": self.available,
            "configured": self.configured,
            "known": self.known,
            "to_add": self.to_add,
            "to_remove": self.to_remove,
            "note": self.note,
            "error": self.error,
        }


def _run(command: list[str]) -> tuple[bool, str, str]:
    if shutil.which(command[0]) is None:
        return False, "", f"`{command[0]}` is not on PATH"
    try:
        done = subprocess.run(  # noqa: S603 - operator's own CLI, argv form, no shell
            command,
            capture_output=True,
            text=True,
            timeout=_PROBE_TIMEOUT_SECS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "", f"`{' '.join(command)}` timed out after {_PROBE_TIMEOUT_SECS}s"
    except OSError as exc:
        return False, "", f"`{' '.join(command)}` failed to start: {exc}"
    if done.returncode != 0:
        detail = (done.stderr or done.stdout or "").strip().splitlines()
        return False, done.stdout, f"exit {done.returncode}: {detail[0] if detail else 'no output'}"
    return True, done.stdout, ""


def parse_model_ids(text: str, known: list[str] | None = None) -> list[str]:
    """Pull plausible model ids out of CLI output, JSON or plain text.

    Vendor CLIs are not stable interfaces, so this stays permissive: JSON is
    read structurally when present, otherwise ids are matched by shape.
    """
    stripped = text.strip()
    if stripped.startswith(("{", "[")):
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            payload = None
        if payload is not None:
            found: list[str] = []
            items = payload.get("data", payload.get("models", [])) if isinstance(payload, dict) else payload
            for item in items if isinstance(items, list) else []:
                model_id = item.get("id") or item.get("name") if isinstance(item, dict) else item
                if isinstance(model_id, str) and model_id and model_id not in found:
                    found.append(model_id)
            if found:
                return found

    # Vendor CLIs mix prose with the list ("You are logged in with grok.com"),
    # and a hostname has the same shape as a model id. Restrict to the list
    # section when the output marks one, then drop domain-looking tokens.
    section = text
    marker = _MODELS_SECTION_RE.search(text)
    if marker:
        section = text[marker.end() :]

    ids: list[str] = []
    for match in _ID_RE.finditer(section):
        model_id = match.group(0)
        if _TLD_RE.search(model_id) or model_id in ids:
            continue
        ids.append(model_id)

    if known:
        # Ids the catalog already knows come first: they are the safest signal
        # that parsing worked at all. Snapshot the order before sorting, since
        # `list.index` during a sort key reads a half-reordered list.
        original = {model_id: index for index, model_id in enumerate(ids)}
        ids.sort(key=lambda model_id: (model_id not in known, original[model_id]))
    return ids


def discover_agent(
    agent: str, spec: dict[str, Any], configured: list[str], known: list[str] | None = None
) -> AgentDiscovery:
    method = str(spec.get("method", "snapshot"))
    note = " ".join(str(spec.get("note", "")).split())
    known_ids = list(known or configured)

    def result(status: str, **overrides: Any) -> AgentDiscovery:
        # Every non-ok outcome falls back to the configured catalog as
        # last-known-good, which is what makes a failed probe harmless.
        fields: dict[str, Any] = {
            "available": list(configured),
            "configured": list(configured),
            "known": known_ids,
            "note": note,
        }
        fields.update(overrides)
        return AgentDiscovery(agent=agent, status=status, method=method, **fields)

    if method == "snapshot":
        return result(SNAPSHOT)
    if method != "command":
        return result(FAILED, available=[], error=f"unknown discovery method `{method}`")

    command = list(spec.get("command") or [])
    if not command:
        return result(
            FAILED,
            available=[],
            error="discovery method is `command` but no command is configured",
        )

    ok, stdout, error = _run(command)
    if not ok:
        return result(UNAVAILABLE, error=error)
    if spec.get("probe_only"):
        return result(
            SNAPSHOT, note=f"{note} (probe only: CLI present, no list available)".strip()
        )

    found = parse_model_ids(stdout, configured)
    if not found:
        return result(
            UNAVAILABLE,
            error="probe succeeded but no model ids could be parsed from its output",
        )
    return result(OK, available=found)


def discover(
    policy_discovery: dict[str, Any],
    configured_by_agent: dict[str, list[str]],
    only: list[str] | None = None,
    fixture: Path | None = None,
    known_by_agent: dict[str, list[str]] | None = None,
) -> dict[str, AgentDiscovery]:
    """Probe every configured agent (or a `--providers` subset)."""
    known_by_agent = known_by_agent or {}
    if fixture is not None:
        return load_fixture(fixture, configured_by_agent, known_by_agent)

    specs = policy_discovery.get("agents") or {}
    out: dict[str, AgentDiscovery] = {}
    for agent, configured in configured_by_agent.items():
        if only and agent not in only:
            continue
        known = known_by_agent.get(agent) or configured
        spec = specs.get(agent)
        if spec is None:
            out[agent] = AgentDiscovery(
                agent=agent,
                status=SNAPSHOT,
                method="none",
                available=list(configured),
                configured=list(configured),
                known=list(known),
                note="no discovery adapter declared in data/model-policy.yaml",
            )
            continue
        out[agent] = discover_agent(agent, spec, configured, known)
    return out


def load_fixture(
    path: Path,
    configured_by_agent: dict[str, list[str]],
    known_by_agent: dict[str, list[str]] | None = None,
) -> dict[str, AgentDiscovery]:
    """Read a recorded discovery result — how the tests drive the pipeline."""
    payload = json.loads(path.read_text())
    known_by_agent = known_by_agent or {}
    out: dict[str, AgentDiscovery] = {}
    for agent, spec in (payload.get("agents") or {}).items():
        configured = list(spec.get("configured") or configured_by_agent.get(agent) or [])
        out[agent] = AgentDiscovery(
            agent=agent,
            status=str(spec.get("status", OK)),
            method=str(spec.get("method", "fixture")),
            available=list(spec.get("available") or []),
            configured=configured,
            # A fixture records the provider's answer, not the local catalog, so
            # the live catalog's parked ids still suppress duplicate adds.
            known=list(spec.get("known") or known_by_agent.get(agent) or configured),
            note=str(spec.get("note", "")),
            error=str(spec.get("error", "")),
        )
    return out


def to_json(discovered: dict[str, AgentDiscovery], generated_at: str) -> str:
    return json.dumps(
        {
            "generated_at": generated_at,
            "agents": {agent: found.as_dict() for agent, found in discovered.items()},
        },
        indent=2,
        sort_keys=True,
    )
