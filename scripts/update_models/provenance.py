"""Comment blocks written above a generated score entry.

Provenance lives in comments rather than YAML keys on purpose: `ScoreEntryRaw`
in `src/candidate.rs` is `#[serde(deny_unknown_fields)]`, so inventing a
`provenance:` key would make the router fail to start. A comment costs nothing
and is what a human reads first anyway.

The `PROVISIONAL` marker is the existing convention (see the sol/terra/luna and
grok blocks) and is load-bearing: it is the signal that a score is provisional
awaiting real seat benchmarks.
"""

from __future__ import annotations

import textwrap

from .score import ScoreProposal

_WIDTH = 76  # matches the shipped file's comment width


def wrap(text: str, indent: str = "  ") -> list[str]:
    """Wrap prose into `#` comment lines at the file's column width."""
    collapsed = " ".join(text.split())
    if not collapsed:
        return []
    return [
        f"{indent}# {line}"
        for line in textwrap.wrap(collapsed, width=_WIDTH - len(indent) - 2)
    ]


def entry_comment(proposal: ScoreProposal, source: str, generated_at: str) -> str:
    """The comment block introducing a proposed score entry."""
    marker = "PROVISIONAL" if proposal.provisional else "SCORED"
    head = (
        f"{proposal.candidate} — {marker} scores, added {generated_at} by "
        f"scripts/update_models.py ({source})."
    )
    body = [head, proposal.rationale]
    if proposal.insert_before:
        body.append(
            f"Placed above `{proposal.insert_before}` because every id matching "
            f"`{proposal.pattern}` also matches it and first match wins."
        )
    if proposal.provisional:
        body.append(
            "Re-benchmark and tune once the model has real seat traffic; until then "
            "these numbers are a tier placement, not a measurement."
        )
    for risk in proposal.risks:
        body.append(f"RISK: {risk}")

    lines: list[str] = []
    for paragraph in body:
        lines.extend(wrap(paragraph))
    return "\n".join(lines)


def retirement_note(model_id: str, generated_at: str) -> str:
    """The note left above a model commented out because it vanished."""
    return (
        f"retired {generated_at}: no longer offered by the provider "
        f"(discovery found no `{model_id}`); kept for one release before removal."
    )


def new_model_note(model_id: str, generated_at: str) -> str:
    """The note left above a newly discovered, still-disabled catalog model."""
    return (
        f"discovered {generated_at}: enable once it has benchmark evidence, a score, and a "
        f"cost_rank you believe (`{model_id}`)."
    )
