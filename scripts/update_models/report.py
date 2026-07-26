"""The operator-facing report: what changed, what it would change, what broke."""

from __future__ import annotations

import difflib
from dataclasses import dataclass, field
from pathlib import Path

from .discover import AgentDiscovery
from .score import ScoreProposal
from .validate import ERROR, INFO, WARN, Finding


@dataclass
class PendingWrite:
    """A file the run would rewrite, with its diff."""

    path: Path
    before: str
    after: str

    @property
    def changed(self) -> bool:
        return self.before != self.after

    def diff(self) -> str:
        return "".join(
            difflib.unified_diff(
                self.before.splitlines(keepends=True),
                self.after.splitlines(keepends=True),
                fromfile=f"a/{self.path.name}",
                tofile=f"b/{self.path.name}",
            )
        )


@dataclass
class Report:
    generated_at: str
    applied: bool
    discovered: dict[str, AgentDiscovery] = field(default_factory=dict)
    proposals: list[ScoreProposal] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)
    writes: list[PendingWrite] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def changed_writes(self) -> list[PendingWrite]:
        return [write for write in self.writes if write.changed]

    def _by_severity(self, severity: str) -> list[Finding]:
        return [finding for finding in self.findings if finding.severity == severity]

    def render(self) -> str:
        errors = self._by_severity(ERROR)
        warnings = self._by_severity(WARN)
        infos = self._by_severity(INFO)
        mode = "APPLIED" if self.applied else "DRY RUN"

        lines = [
            "# router-acp model update",
            "",
            f"- generated: {self.generated_at}",
            f"- mode: **{mode}**",
            f"- required changes: **{len(self.changed_writes)}**",
            f"- findings: {len(errors)} error / {len(warnings)} warn / {len(infos)} info",
            "",
        ]

        if not self.changed_writes and not errors:
            lines += [
                "**No changes required.** The catalog and score table already satisfy every",
                "invariant in `data/model-policy.yaml`, so the updater reproduces today's",
                "policy exactly.",
                "",
            ]

        lines += ["## Discovery", ""]
        if not self.discovered:
            lines.append("_skipped_")
        else:
            lines += [
                "| agent | status | method | available | configured | add | remove |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
            for agent, found in sorted(self.discovered.items()):
                lines.append(
                    f"| {agent} | {found.status} | {found.method} | {len(found.available)} | "
                    f"{len(found.configured)} | {', '.join(found.to_add) or '—'} | "
                    f"{', '.join(found.to_remove) or '—'} |"
                )
            untrusted = [found for found in self.discovered.values() if not found.trustworthy]
            if untrusted:
                lines += [
                    "",
                    "Removals are only proposed from a trustworthy (`ok`) probe. These agents "
                    "fell back to the configured catalog as last-known-good:",
                    "",
                ]
                for found in untrusted:
                    reason = found.error or found.note or "no machine-readable model list"
                    lines.append(f"- **{found.agent}** ({found.status}): {reason}")
        lines.append("")

        lines += ["## Score proposals", ""]
        if not self.proposals:
            lines.append("_none — every configured model already resolves to a scored family_")
        else:
            for proposal in self.proposals:
                lines += [
                    f"### {proposal.candidate} → `{proposal.pattern}`",
                    "",
                    f"- tier: **{proposal.tier}**, base {proposal.base:.2f}",
                    f"- rationale: {proposal.rationale}",
                ]
                if proposal.insert_before:
                    lines.append(f"- insert before: `{proposal.insert_before}`")
                for risk in proposal.risks:
                    lines.append(f"- **risk**: {risk}")
                lines += [
                    "",
                    "| " + " | ".join(proposal.quality) + " |",
                    "| " + " | ".join("---" for _ in proposal.quality) + " |",
                    "| " + " | ".join(f"{value:.2f}" for value in proposal.quality.values()) + " |",
                    "",
                ]
        lines.append("")

        lines += ["## Findings", ""]
        if not self.findings:
            lines.append("_clean_")
        for severity, bucket in (("error", errors), ("warn", warnings), ("info", infos)):
            if not bucket:
                continue
            lines += [f"### {severity}", ""]
            for finding in bucket:
                lines.append(f"- **{finding.code}** — {finding.message}")
                if finding.detail:
                    lines.append(f"  - {finding.detail}")
            lines.append("")

        lines += ["## Diffs", ""]
        if not self.changed_writes:
            lines.append("_no file changes_")
        for write in self.changed_writes:
            lines += [f"### {write.path}", "", "```diff", write.diff().rstrip("\n"), "```", ""]

        if self.notes:
            lines += ["## Notes", ""]
            lines += [f"- {note}" for note in self.notes]
            lines.append("")

        if errors:
            lines += [
                "## Sign-off required",
                "",
                "Errors above are ordering/contract breaks, not rounding. Routing goldens in",
                "`tests/golden.rs` are NOT re-baked automatically — run `cargo test` and, if a",
                "winner moved, decide whether the policy change is intended before re-baking.",
                "",
            ]

        return "\n".join(lines).rstrip("\n") + "\n"
