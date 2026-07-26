"""Run the judgment steps through a headless goose session.

What goose is for: the calls that need reading (a launch post, a benchmark
table, two model lists that renamed a line), not arithmetic. It proposes
sourced benchmark observations; the deterministic calibrator owns quality.
Goose never edits production YAML.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_TIMEOUT_SECS = 600


class GooseError(RuntimeError):
    """Goose is unavailable, or its reply did not satisfy the schema."""


@dataclass(frozen=True)
class GooseJudgment:
    """One model's proposed evidence bundle, as returned by the recipe."""

    candidate: str
    observations: list[dict[str, Any]]
    rationale: str
    risks: list[str]
    # An optional line-pattern proposal (`*sol*` rather than the whole id). The
    # caller re-checks it for shadowing before using it.
    pattern: str | None = None


@dataclass(frozen=True)
class GooseRename:
    """A proposed id rename (a version bump, not a new model)."""

    old: str
    new: str
    confidence: float
    rationale: str


def available() -> bool:
    return shutil.which("goose") is not None


def _extract_json(text: str) -> dict[str, Any]:
    """Pull the JSON object out of a goose transcript.

    The recipe asks for JSON only, but a session can still print a banner or a
    tool line, so the outermost brace-balanced object is extracted rather than
    trusting the whole stream to parse.
    """
    start = text.find("{")
    while start != -1:
        depth = 0
        for index in range(start, len(text)):
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start : index + 1])
                    except json.JSONDecodeError:
                        break
        start = text.find("{", start + 1)
    raise GooseError("goose returned no parseable JSON object")


def parse_judgments(payload: dict[str, Any], band: tuple[float, float]) -> tuple[
    list[GooseJudgment], list[GooseRename]
]:
    """Validate the recipe's reply. Raises on anything unusable."""
    _ = band  # retained for a stable caller API
    judgments: list[GooseJudgment] = []
    for index, item in enumerate(payload.get("models") or []):
        if not isinstance(item, dict):
            raise GooseError(f"models[{index}] is not an object")
        missing = [key for key in ("candidate", "observations") if key not in item]
        if missing:
            raise GooseError(f"models[{index}] is missing {missing}")
        observations = item["observations"]
        if not isinstance(observations, list):
            raise GooseError(f"models[{index}] observations is not a list")
        for observation_index, observation in enumerate(observations):
            if not isinstance(observation, dict) or not all(
                key in observation for key in ("benchmark", "result", "source")
            ):
                raise GooseError(
                    f"models[{index}].observations[{observation_index}] needs "
                    "`benchmark`, `result`, and `source`"
                )
            try:
                float(observation["result"])
            except (TypeError, ValueError) as exc:
                raise GooseError(
                    f"models[{index}].observations[{observation_index}] result is not numeric"
                ) from exc
        proposed_pattern = item.get("pattern")
        judgments.append(
            GooseJudgment(
                candidate=str(item["candidate"]),
                observations=observations,
                rationale=" ".join(str(item.get("rationale", "")).split()),
                risks=[str(risk) for risk in (item.get("risks") or [])],
                pattern=str(proposed_pattern) if proposed_pattern else None,
            )
        )

    renames: list[GooseRename] = []
    for index, item in enumerate(payload.get("renames") or []):
        if not isinstance(item, dict) or "from" not in item or "to" not in item:
            raise GooseError(f"renames[{index}] needs `from` and `to`")
        renames.append(
            GooseRename(
                old=str(item["from"]),
                new=str(item["to"]),
                confidence=float(item.get("confidence", 0.0)),
                rationale=" ".join(str(item.get("rationale", "")).split()),
            )
        )
    return judgments, renames


def run_recipe(
    recipe: Path,
    params: dict[str, str],
    band: tuple[float, float],
    transcript_out: Path | None = None,
) -> tuple[list[GooseJudgment], list[GooseRename]]:
    if not available():
        raise GooseError("`goose` is not on PATH — rerun with --no-goose")
    if not recipe.exists():
        raise GooseError(f"recipe not found: {recipe}")

    command = ["goose", "run", "--recipe", str(recipe), "--no-session"]
    for key, value in params.items():
        command += ["--params", f"{key}={value}"]

    try:
        done = subprocess.run(  # noqa: S603 - fixed argv, no shell
            command, capture_output=True, text=True, timeout=_TIMEOUT_SECS, check=False
        )
    except subprocess.TimeoutExpired as exc:
        raise GooseError(f"goose timed out after {_TIMEOUT_SECS}s") from exc
    except OSError as exc:
        raise GooseError(f"goose failed to start: {exc}") from exc

    transcript = f"{done.stdout}\n{done.stderr}"
    if transcript_out is not None:
        transcript_out.write_text(transcript)
    if done.returncode != 0:
        raise GooseError(f"goose exited {done.returncode}; transcript in {transcript_out}")
    return parse_judgments(_extract_json(transcript), band)
