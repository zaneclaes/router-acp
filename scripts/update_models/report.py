"""The operator-facing report: what changed, what it would change, what broke."""

from __future__ import annotations

import difflib
from dataclasses import dataclass, field
from pathlib import Path

from .discover import AgentDiscovery
from .score import Rescore
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
    rescores: list[Rescore] = field(default_factory=list)
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
                "**No changes required.** Every model in `data/providers/*.yaml` already",
                "carries the quality its own evidence computes and satisfies every check, so",
                "the updater reproduces today's catalog exactly.",
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

        lines += ["## Re-scored models", ""]
        if not self.rescores:
            lines.append(
                "_none — every model with evidence already carries the quality it computes, "
                "and no model without evidence carries any_"
            )
        else:
            for rescore in self.rescores:
                if rescore.cleared:
                    lines += [f"### {rescore.key}", "", f"- {rescore.describe()}", ""]
                    continue
                lines += [
                    f"### {rescore.key}",
                    "",
                    f"- source: {rescore.origin}",
                    f"- default_quality: {rescore.before_default:.2f} → {rescore.after_default:.2f}",
                    "",
                    "| class | before | after |",
                    "| --- | --- | --- |",
                ]
                lines += [
                    f"| {task_class} | {rescore.before[task_class]:.2f} | {value:.2f} |"
                    for task_class, value in rescore.after.items()
                ]
                lines.append("")
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
