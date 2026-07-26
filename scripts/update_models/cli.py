"""Command line for the model updater. Entry point: `scripts/update_models.py`.

The CLI lives in the package rather than in the script so it is importable (a
top-level `update_models.py` next to the `update_models/` package is shadowed by
the package and can only ever be run, never imported — including by tests).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

import sys

from . import benchmarks
from . import discover as discovery
from . import goose as goose_bridge
from . import patterns as pat
from . import provenance, score, validate
from .catalog import Catalog, parse_catalog, render_disabled_model
from .policy import Policy, PolicyError, load_policy
from .report import PendingWrite, Report
from .scores_file import ScoresFile, parse_scores, parse_scores_text

EXIT_OK = 0
EXIT_VALIDATION = 1
EXIT_DISCOVERY = 2
EXIT_GOOSE = 3

REPO_ROOT = Path(__file__).resolve().parents[2]


class GooseStepFailed(RuntimeError):
    """The goose judgment step could not produce a usable proposal."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="update_models.py",
        description=(
            "Update router-acp's model catalog and quality/cost tables: discover what "
            "each agent exposes, derive scores from calibrated benchmarks, keep the "
            "first-match pattern table correct, and validate that routing ORDER still "
            "matches policy. Runbook: docs/model-updater.md"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--repo", type=Path, default=REPO_ROOT, help="repository root")
    parser.add_argument("--policy", type=Path, help="default: <repo>/data/model-policy.yaml")
    parser.add_argument("--scores", type=Path, help="default: <repo>/data/scores.yaml")
    parser.add_argument(
        "--config", type=Path, help="agent catalog; default: <repo>/examples/router-preferred.yaml"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="report and diff without writing (default)",
    )
    mode.add_argument("--apply", action="store_true", help="write the proposed changes")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="check invariants against what is checked in; no discovery, no proposals",
    )
    parser.add_argument("--providers", help="comma-separated agent subset, e.g. grok,kimi")
    parser.add_argument("--skip-discover", action="store_true")
    parser.add_argument("--skip-score", action="store_true")
    parser.add_argument("--skip-write", action="store_true")
    parser.add_argument(
        "--discovery-fixture",
        type=Path,
        help="read discovery from a recorded JSON fixture instead of probing CLIs",
    )
    goose_flags = parser.add_mutually_exclusive_group()
    goose_flags.add_argument(
        "--use-goose",
        action="store_true",
        help="run the judgment step through `goose run --recipe` (tier + base proposals)",
    )
    goose_flags.add_argument("--no-goose", action="store_true", default=True)
    parser.add_argument(
        "--recipe", type=Path, help="default: <repo>/scripts/recipes/update-models-score.yaml"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("/tmp/router-acp-model-update"),
        help="where the report, discovery JSON and diffs land",
    )
    parser.add_argument("--report", type=Path, help="report path (default: <out-dir>/report.md)")
    parser.add_argument(
        "--today", help="override the stamp used in generated comments (YYYY-MM-DD)"
    )
    return parser.parse_args(argv)


def _resolve_paths(args: argparse.Namespace) -> tuple[Path, Path, Path, Path]:
    repo = args.repo.resolve()
    policy = args.policy or repo / "data" / "model-policy.yaml"
    scores = args.scores or repo / "data" / "scores.yaml"
    config = args.config or repo / "examples" / "router-preferred.yaml"
    return repo, policy, scores, config


def configured_by_agent(catalog: Catalog) -> dict[str, list[str]]:
    """Models the catalog routes to today — the basis for retirements."""
    return {
        agent.name: [model.model_id for model in agent.enabled_models] for agent in catalog.agents
    }


def known_by_agent(catalog: Catalog) -> dict[str, list[str]]:
    """Every id the catalog mentions, including commented-out ones.

    A model parked awaiting a score is already known, so discovery must not
    propose adding it again on the next run.
    """
    return {
        agent.name: [model.model_id for model in agent.models] for agent in catalog.agents
    }


def build_proposals(
    policy: Policy,
    scores: ScoresFile,
    catalog: Catalog,
    discovered: dict[str, discovery.AgentDiscovery],
    use_goose: bool,
    recipe: Path,
    out_dir: Path,
    report: Report,
) -> list[score.ScoreProposal]:
    """Propose scores for candidates that have calibrated evidence.

    Deliberately narrow. A model that already resolves to a family inherits that
    family's score — which is how the table is designed (patterns key on the
    model LINE, so version bumps inherit on purpose). Those are reported, not
    rescored: silently differentiating every new id would rewrite the table on
    each release and flip the routing goldens.
    """
    candidates: list[str] = [model.candidate for model in catalog.all_models() if model.enabled]
    for agent, found in discovered.items():
        for model_id in found.to_add:
            candidate = f"{agent}/{model_id}"
            if candidate not in candidates:
                candidates.append(candidate)
            inherited = pat.resolve(scores.patterns, candidate)
            if inherited is not None:
                report.notes.append(
                    f"`{candidate}` inherits the existing `{inherited}` score. If this "
                    "generation is materially stronger, give it its own entry above that "
                    "pattern — inheriting an older sibling's numbers under-scores it."
                )

    unscored = score.unscored_candidates(scores, candidates)
    if not unscored:
        return []
    derived = benchmarks.derive(policy)

    if use_goose:
        try:
            found_judgments, renames = goose_bridge.run_recipe(
                recipe=recipe,
                params={
                    "UNSCORED": ",".join(unscored),
                    "CURRENT_SCORES": str(scores.path),
                    "POLICY": str(policy.path),
                    "CATALOG": str(catalog.path),
                },
                band=policy.quality_band,
                transcript_out=out_dir / "goose-transcript.txt",
            )
        except goose_bridge.GooseError as exc:
            raise GooseStepFailed(str(exc)) from exc
        for judgment in found_judgments:
            report.notes.append(
                f"goose found {len(judgment.observations)} benchmark observation(s) for "
                f"`{judgment.candidate}`. Add reviewed observations to "
                "`benchmark_scoring.model_evidence`; quality is not inferred from prose."
            )
        for rename in renames:
            report.notes.append(
                f"goose proposes rename `{rename.old}` → `{rename.new}` "
                f"(confidence {rename.confidence:.2f}): {rename.rationale} "
                "— renames are never auto-applied"
            )

    proposals: list[score.ScoreProposal] = []
    for candidate in unscored:
        evidence_pattern = benchmarks.resolve_profile(derived, candidate)
        if evidence_pattern is None:
            report.notes.append(
                f"`{candidate}` remains disabled/unscored: no calibrated benchmark evidence. "
                "The updater never assigns quality from its name or a vendor claim."
            )
            continue
        profile = derived[evidence_pattern]
        proposals.append(
            score.propose_from_benchmarks(
                policy,
                scores,
                candidate,
                evidence_pattern,
                profile.default_quality,
                profile.quality,
                profile.observations,
            )
        )
    return proposals


def catalog_entry_lines(model_id: str, cost_rank: int, stamp: str) -> list[str]:
    return render_disabled_model(
        indent="      ",
        model_id=model_id,
        display_name=model_id.replace("-", " ").title(),
        cost_rank=cost_rank,
        note=provenance.new_model_note(model_id, stamp),
    )


def apply_catalog_changes(
    catalog: Catalog,
    discovered: dict[str, discovery.AgentDiscovery],
    policy: Policy,
    stamp: str,
    report: Report,
) -> None:
    """Retire vanished models, park newly discovered ones, refresh pricing."""
    for agent_name, found in discovered.items():
        agent = catalog.agent(agent_name)
        if not found.trustworthy or agent is None:
            continue
        for model_id in found.to_remove:
            model = catalog.model(f"{agent_name}/{model_id}")
            if model is not None and model.enabled:
                catalog.disable(model, provenance.retirement_note(model_id, stamp))
                report.notes.append(
                    f"`{agent_name}/{model_id}` commented out: discovery no longer offers it "
                    "(kept one release before removal)"
                )
        for model_id in found.to_add:
            peers = [
                (model.cost_rank, policy.blended_price(model.pricing))
                for model in agent.enabled_models
                if model.cost_rank is not None and policy.blended_price(model.pricing) is not None
            ]
            # Discovery reports ids, not prices, so this rank is a floor
            # placeholder until someone fills in `pricing`.
            suggested = score.rank_suggestion(policy, None, peers)  # type: ignore[arg-type]
            catalog.add_disabled(agent_name, catalog_entry_lines(model_id, suggested, stamp))
            report.notes.append(
                f"`{agent_name}/{model_id}` added commented-out at placeholder cost_rank "
                f"{suggested} (discovery reports no price for it); set its real rank and "
                "pricing before enabling it"
            )

    reference = (policy.published_pricing or {}).get("models") or {}
    for candidate, rates in reference.items():
        model = catalog.model(candidate)
        if model is None or not model.enabled or not model.pricing:
            continue
        for key, want in rates.items():
            current = (model.pricing or {}).get(key)
            if current is not None and abs(float(current) - float(want)) > 0.0001:
                if catalog.set_price(model, key, float(want)):
                    report.notes.append(
                        f"`{candidate}` {key}: {current} → {want} (published reference)"
                    )


def main(argv: list[str] | None = None, stdout: TextIO | None = None) -> int:
    out = stdout or sys.stdout
    args = parse_args(argv)
    repo, policy_path, scores_path, config_path = _resolve_paths(args)
    # A provenance stamp is a system timestamp, so UTC — never a naive today().
    stamp = args.today or datetime.now(timezone.utc).date().isoformat()
    applied = bool(args.apply)
    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        policy = load_policy(policy_path)
    except PolicyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_VALIDATION

    if not config_path.exists() and args.config is not None:
        # An explicitly named catalog that is not there is a typo, not a signal
        # to validate nothing: without a catalog every catalog check silently
        # passes, which reads like a clean audit.
        print(f"error: --config {config_path} does not exist", file=sys.stderr)
        return EXIT_VALIDATION

    scores = parse_scores(scores_path)
    catalog = parse_catalog(config_path) if config_path.exists() else None
    report = Report(generated_at=stamp, applied=applied)

    scores_before = scores.render()
    catalog_before = catalog.render() if catalog else ""

    # -- discover ---------------------------------------------------------
    discovered: dict[str, discovery.AgentDiscovery] = {}
    if not args.validate_only and not args.skip_discover and catalog is not None:
        discovered = discovery.discover(
            policy_discovery=policy.discovery,
            configured_by_agent=configured_by_agent(catalog),
            only=[name.strip() for name in args.providers.split(",")] if args.providers else None,
            fixture=args.discovery_fixture,
            known_by_agent=known_by_agent(catalog),
        )
        report.discovered = discovered
        (out_dir / "discovered.json").write_text(discovery.to_json(discovered, stamp))
        failed = [found.agent for found in discovered.values() if found.status == discovery.FAILED]
        if failed:
            print(
                f"error: discovery failed for {', '.join(failed)} — see "
                f"{out_dir / 'discovered.json'}",
                file=sys.stderr,
            )
            return EXIT_DISCOVERY

    # -- score ------------------------------------------------------------
    if not args.validate_only and not args.skip_score and catalog is not None:
        try:
            report.proposals = build_proposals(
                policy=policy,
                scores=scores,
                catalog=catalog,
                discovered=discovered,
                use_goose=bool(args.use_goose),
                recipe=args.recipe or repo / "scripts" / "recipes" / "update-models-score.yaml",
                out_dir=out_dir,
                report=report,
            )
        except GooseStepFailed as exc:
            print(f"error: goose judgment step failed: {exc}", file=sys.stderr)
            return EXIT_GOOSE

    # -- write (in memory; only flushed with --apply) ----------------------
    scores_after = scores_before
    if not args.validate_only and not args.skip_write:
        for pattern, derived in benchmarks.derive(policy).items():
            if scores.set_quality(pattern, derived.default_quality, derived.quality):
                report.notes.append(
                    f"`{pattern}` refreshed from {derived.observations} benchmark observations "
                    f"(base {derived.default_quality:.2f})"
                )
        for proposal in report.proposals:
            comment = provenance.entry_comment(
                proposal,
                source="calibrated benchmarks",
                generated_at=stamp,
            )
            scores.insert_entry(proposal.to_entry(comment), before=proposal.insert_before)

        order = None
        violations = pat.order_violations(scores.patterns)
        if violations:
            order = pat.sorted_by_specificity(scores.patterns)
            report.notes.append(
                f"reordered {len(violations)} shadowed pattern(s) so the first-match table "
                "resolves each id to its own family"
            )
        scores_after = scores.render(order=order)

        if catalog is not None and discovered:
            apply_catalog_changes(catalog, discovered, policy, stamp, report)

    # -- validate the state that would ship -------------------------------
    validated_scores = parse_scores_text(scores_after, scores_path)
    report.findings = validate.validate(policy, validated_scores, catalog)

    report.writes = [PendingWrite(path=scores_path, before=scores_before, after=scores_after)]
    if catalog is not None:
        report.writes.append(
            PendingWrite(path=config_path, before=catalog_before, after=catalog.render())
        )

    # -- emit -------------------------------------------------------------
    report_path = args.report or out_dir / "report.md"
    report_path.write_text(report.render())
    for write in report.changed_writes:
        (out_dir / f"{write.path.name}.diff").write_text(write.diff())

    if applied:
        for write in report.changed_writes:
            write.path.write_text(write.after)

    errors = validate.errors(report.findings)
    changed = report.changed_writes
    print(
        f"{'APPLIED' if applied else 'DRY RUN'}: {len(changed)} file(s) to change, "
        f"{len(errors)} error finding(s)",
        file=out,
    )
    for finding in report.findings:
        if finding.severity != validate.INFO:
            print(f"  {finding}", file=out)
    print(f"report: {report_path}", file=out)
    if changed and not applied:
        for write in changed:
            print(f"  diff: {out_dir / (write.path.name + '.diff')}", file=out)
    return EXIT_VALIDATION if errors else EXIT_OK
