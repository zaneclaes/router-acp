"""Command line for the model updater. Entry point: `scripts/update_models.py`.

The CLI lives in the package rather than in the script so it is importable (a
top-level `update_models.py` next to the `update_models/` package is shadowed by
the package and can only ever be run, never imported — including by tests).

The catalog is `<data>/providers/<name>.yaml` (each provider's models, grouped
by family) and the scoring procedure is `<data>/classifier.yaml` (`scoring`,
`cost`). The updater writes only the provider files, and in them only model
quality lines, retired entries and parked new models. Router configs do not
list models (agents inherit their provider's), so nothing else is touched.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from . import discover as discovery
from . import goose as goose_bridge
from . import provenance, score, validate
from .catalog import Catalog, CatalogError, load_catalog, load_catalog_texts, render_parked_entry
from .policy import Policy, PolicyError
from .report import PendingWrite, Report

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
            "Update router-acp's model catalog, data/providers/<name>.yaml: discover what "
            "each provider exposes, re-score every model from the evidence recorded on it, "
            "and validate the catalog's shape, evidence, ordering and cost contract. "
            "Runbook: docs/model-updater.md"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--repo", type=Path, default=REPO_ROOT, help="repository root")
    parser.add_argument(
        "--data",
        type=Path,
        help="directory holding classifier.yaml and providers/*.yaml; default: <repo>/data",
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
        help="check what is checked in; no discovery, no re-scoring",
    )
    parser.add_argument("--providers", help="comma-separated provider subset, e.g. grok,kimi")
    parser.add_argument("--skip-discover", action="store_true")
    parser.add_argument("--skip-score", action="store_true", help="do not re-score models")
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
        help="ask `goose run --recipe` for published evidence on models that have none",
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


def configured_by_provider(models: Catalog) -> dict[str, list[str]]:
    """Each provider's live models — the basis for retirements."""
    return {provider: [row.model_id for row in models.by_provider(provider)] for provider in models.providers()}


def known_by_provider(models: Catalog) -> dict[str, list[str]]:
    """Every id a provider file names, including commented-out entries, so a
    parked model is not proposed again on the next run."""
    return {provider: models.known_ids(provider) for provider in models.providers()}


def unevidenced(models: Catalog, discovered: dict[str, discovery.AgentDiscovery]) -> list[str]:
    """`provider/id` of every model with no published evidence of its own,
    plus newly discovered models."""
    missing = [row.key for row in models.rows if not row.evidence]
    for provider, found in discovered.items():
        missing += [f"{provider}/{model_id}" for model_id in found.to_add]
    return missing


def gather_evidence(
    models: Catalog,
    policy: Policy,
    candidates: list[str],
    data_dir: Path,
    recipe: Path,
    out_dir: Path,
    report: Report,
) -> None:
    """Ask goose for published evidence. Reported for review, never written."""
    if not candidates:
        return
    try:
        judgments, renames = goose_bridge.run_recipe(
            recipe=recipe,
            params={
                "UNSCORED": ",".join(candidates),
                "PROVIDERS": str(data_dir / "providers"),
                "CLASSIFIER": str(models.classifier_path),
            },
            band=policy.quality_band,
            transcript_out=out_dir / "goose-transcript.txt",
        )
    except goose_bridge.GooseError as exc:
        raise GooseStepFailed(str(exc)) from exc
    for judgment in judgments:
        report.notes.append(
            f"goose found {len(judgment.observations)} benchmark observation(s) for "
            f"`{judgment.candidate}`. Review them and add the accepted ones to that model's "
            "`evidence:` in its data/providers/<name>.yaml; quality is never inferred from prose."
        )
    for rename in renames:
        report.notes.append(
            f"goose proposes rename `{rename.old}` → `{rename.new}` "
            f"(confidence {rename.confidence:.2f}): {rename.rationale} "
            "— renames are never auto-applied"
        )


def apply_discovery(
    models: Catalog,
    discovered: dict[str, discovery.AgentDiscovery],
    policy: Policy,
    stamp: str,
    report: Report,
) -> None:
    """Retire vanished models and park newly discovered ones, in their provider file."""
    for provider, found in discovered.items():
        if not found.trustworthy or models.file(provider) is None:
            continue
        for model_id in found.to_remove:
            row = models.find(model_id, provider)
            if row is not None:
                grace = policy.retire_grace_releases(provider)
                models.disable(row, provenance.retirement_note(model_id, stamp, grace))
                report.notes.append(
                    f"`{provider}/{model_id}` commented out: discovery no longer offers it "
                    f"(kept {grace} release(s) before removal)"
                )
        for model_id in found.to_add:
            peers = [
                (row.cost_rank, policy.blended_price(row.pricing))
                for row in models.by_provider(provider)
                if row.cost_rank is not None and policy.blended_price(row.pricing) is not None
            ]
            # Discovery reports ids, not prices, so this rank is a floor
            # placeholder until someone fills in `pricing`.
            suggested = score.rank_suggestion(policy, None, peers)  # type: ignore[arg-type]
            models.add_parked(
                provider,
                model_id,
                render_parked_entry(model_id, suggested, provenance.new_model_note(model_id, stamp)),
            )
            report.notes.append(
                f"`{provider}/{model_id}` added to data/providers/{provider}.yaml commented out, at "
                f"placeholder cost_rank {suggested}: move it under its family, set its version, fill "
                "in pricing and evidence, uncomment it and rerun with --apply"
            )


def main(argv: list[str] | None = None, stdout: TextIO | None = None) -> int:
    out = stdout or sys.stdout
    args = parse_args(argv)
    repo = args.repo.resolve()
    data_dir: Path = args.data or repo / "data"
    # A provenance stamp is a system timestamp, so UTC — never a naive today().
    stamp = args.today or datetime.now(timezone.utc).date().isoformat()
    applied = bool(args.apply)
    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        models = load_catalog(data_dir)
        policy = models.policy
    except (CatalogError, PolicyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_VALIDATION

    report = Report(generated_at=stamp, applied=applied)
    before = {f.path: f.render() for f in models.files}

    # -- discover ---------------------------------------------------------
    discovered: dict[str, discovery.AgentDiscovery] = {}
    if not args.validate_only and not args.skip_discover:
        discovered = discovery.discover(
            policy_discovery=policy.discovery,
            configured_by_agent=configured_by_provider(models),
            only=[name.strip() for name in args.providers.split(",")] if args.providers else None,
            fixture=args.discovery_fixture,
            known_by_agent=known_by_provider(models),
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

    # -- gather evidence (goose; reported, never written) ------------------
    if not args.validate_only and args.use_goose:
        try:
            gather_evidence(
                models,
                policy,
                unevidenced(models, discovered),
                data_dir,
                args.recipe or repo / "scripts" / "recipes" / "update-models-score.yaml",
                out_dir,
                report,
            )
        except GooseStepFailed as exc:
            print(f"error: goose evidence step failed: {exc}", file=sys.stderr)
            return EXIT_GOOSE

    # -- write (in memory; only flushed with --apply) ----------------------
    if not args.validate_only and not args.skip_write:
        if not args.skip_score:
            try:
                report.rescores = score.rescore(policy, models)
            except PolicyError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return EXIT_VALIDATION
            report.notes += [rescore.describe() for rescore in report.rescores]
        if discovered:
            apply_discovery(models, discovered, policy, stamp, report)
    after = {f.path: f.render() for f in models.files}

    # -- validate the state that would ship -------------------------------
    try:
        report.findings = validate.validate(load_catalog_texts(after, models.classifier_path))
    except CatalogError as exc:
        report.findings = [validate.Finding(validate.ERROR, "unparseable", str(exc))]

    report.writes = [PendingWrite(path=path, before=before[path], after=after[path]) for path in after]

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
