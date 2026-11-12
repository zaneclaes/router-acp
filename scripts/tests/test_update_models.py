"""Tests for the model updater.

    python3 -m unittest discover -s scripts/tests -t scripts/tests

Two inputs:

  * the REAL catalog (`data/providers/*.yaml` + `data/classifier.yaml`) — the
    updater must reproduce today's shipped catalog exactly (byte-identical
    round trip, a re-score that changes nothing, a clean validation);
  * `scripts/fixtures/catalog/` — a small synthetic catalog (providers alpha
    and beta) that pins each rule (own evidence only, outranks in and across
    providers, rank exceptions, unscored entries, numeric versions) on entries
    that do not move when the real catalog does.

Nothing here writes to the repo; the CLI runs in dry-run with its output in a
temp dir, and `--apply` only ever runs on a sandbox copy.
"""

from __future__ import annotations

import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

from update_models import benchmarks, cli, discover, goose, score, validate  # noqa: E402
from update_models.catalog import (  # noqa: E402
    CatalogError,
    load_catalog,
    load_catalog_texts,
    parse_version,
    render_parked_entry,
)
from update_models.policy import TASK_CLASSES, PolicyError  # noqa: E402

DATA = REPO / "data"
SMALL = REPO / "scripts" / "fixtures" / "catalog"
ALPHA = SMALL / "providers" / "alpha.yaml"
BETA = SMALL / "providers" / "beta.yaml"
FIXTURES = REPO / "scripts" / "fixtures" / "discovery"


def _sandbox(source: Path = SMALL) -> Path:
    """A temp copy of a data directory, safe to mutate and --apply to."""
    target = Path(tempfile.mkdtemp(prefix="model-update-sandbox-")) / "data"
    (target / "providers").mkdir(parents=True)
    shutil.copyfile(source / "classifier.yaml", target / "classifier.yaml")
    for path in sorted((source / "providers").glob("*.yaml")):
        shutil.copyfile(path, target / "providers" / path.name)
    return target


def _codes(findings: list[validate.Finding], severity: str | None = None) -> set[str]:
    return {f.code for f in findings if severity is None or f.severity == severity}


def _small(alpha: str | None = None, beta: str | None = None, classifier: str | None = None):
    """The synthetic catalog, with any file's text replaced."""
    classifier_path = SMALL / "classifier.yaml"
    if classifier is not None:
        classifier_path = Path(tempfile.mkdtemp(prefix="classifier-")) / "classifier.yaml"
        classifier_path.write_text(classifier)
    return load_catalog_texts(
        {ALPHA: alpha if alpha is not None else ALPHA.read_text(), BETA: beta if beta is not None else BETA.read_text()},
        classifier_path,
    )


# ---------------------------------------------------------------------------
# The real catalog
# ---------------------------------------------------------------------------
class ShippedCatalogTest(unittest.TestCase):
    def test_round_trip_is_byte_identical(self):
        # The writer only earns the right to touch these files if reassembling
        # an untouched one returns the original bytes.
        for provider_file in load_catalog(DATA).files:
            self.assertEqual(provider_file.render(), provider_file.path.read_text(), provider_file.path.name)

    def test_every_yaml_entry_is_a_parsed_entry(self):
        import yaml

        catalog = load_catalog(DATA)
        for provider_file in catalog.files:
            raw = yaml.safe_load(provider_file.path.read_text())
            expected = [(family, item["id"]) for family, items in raw["models"].items() for item in items]
            self.assertEqual([(e.family, e.model_id) for e in provider_file.entries], expected)

    def test_validates_without_errors(self):
        findings = validate.validate(load_catalog(DATA))
        self.assertEqual([str(f) for f in validate.errors(findings)], [], "the shipped catalog is the baseline")

    def test_rescore_is_a_no_op(self):
        # Every model's quality is exactly what its own evidence computes.
        catalog = load_catalog(DATA)
        changes = score.rescore(catalog.policy, catalog)
        self.assertEqual([change.describe() for change in changes], [])

    def test_no_entry_names_a_pattern_or_an_alias(self):
        for row in load_catalog(DATA).rows:
            for value in (row.model_id, row.family, *row.outranks):
                self.assertNotIn("*", value, row.key)
            self.assertNotIn("api_model", row.fields, row.key)

    def test_every_version_is_numeric(self):
        for row in load_catalog(DATA).rows:
            self.assertIsNotNone(row.version, f"{row.key} version `{row.version_text}`")


# ---------------------------------------------------------------------------
# Reader / writer
# ---------------------------------------------------------------------------
class CatalogTest(unittest.TestCase):
    def setUp(self):
        self.catalog = load_catalog(SMALL)

    def test_round_trip_is_byte_identical(self):
        for provider_file in self.catalog.files:
            self.assertEqual(provider_file.render(), provider_file.path.read_text())

    def test_entries_carry_provider_family_and_fields(self):
        big = self.catalog.find("alpha-big-2-10")
        self.assertEqual((big.provider, big.family), ("alpha", "big"))
        self.assertEqual(big.fields["context_window"], 1000000)
        self.assertEqual(len(big.evidence), 3)
        self.assertAlmostEqual(big.quality("BugFix"), 2.85)
        self.assertEqual(self.catalog.policy.cache_ratio("alpha"), {"read": 0.1, "write": 1.25})
        self.assertIsNone(self.catalog.policy.cache_ratio("beta"))

    def test_version_is_read_from_the_text_not_the_yaml_float(self):
        # YAML reads `2.10` as the float 2.1; the entry must still say 2.10.
        big = self.catalog.find("alpha-big-2-10")
        self.assertEqual(big.fields["version"], 2.1)
        self.assertEqual(big.version_text, "2.10")
        self.assertEqual(big.version, (2, 10))
        self.assertGreater(big.version, self.catalog.find("alpha-big-2-9").version)

    def test_parse_version(self):
        self.assertEqual(parse_version("5.5"), (5, 5))
        self.assertEqual(parse_version("'4.10'"), (4, 10))
        self.assertEqual(parse_version("6"), (6,))
        self.assertIsNone(parse_version("TODO"))
        self.assertIsNone(parse_version("5.x"))

    def test_lookup_is_exact_by_id(self):
        self.assertEqual(self.catalog.find("ALPHA-BIG-2-10").key, "alpha/alpha-big-2-10")
        self.assertIsNone(self.catalog.find("alpha-big"), "no prefix or family matching")
        self.assertIsNone(self.catalog.find("big"), "a family name is not a model")
        self.assertIsNone(self.catalog.find("*big*"), "no glob matching")
        self.assertIsNone(self.catalog.find("alpha-big-2-10", "beta"), "provider-scoped")

    def test_references_are_bare_ids_or_provider_slash_id(self):
        self.assertEqual(self.catalog.resolve_ref("alpha-mid-2", "alpha").key, "alpha/alpha-mid-2")
        self.assertEqual(self.catalog.resolve_ref("alpha/alpha-mid-2", "beta").key, "alpha/alpha-mid-2")
        self.assertIsNone(self.catalog.resolve_ref("alpha-mid-2", "beta"), "a bare id never crosses providers")

    def test_comments_inside_and_between_entries_survive(self):
        big = self.catalog.find("alpha-big-2-10")
        span = big.file.lines[big.start : big.end]
        self.assertTrue(span[0].startswith("  - version: 2.10"))
        self.assertNotIn("earlier version", "\n".join(span), "the next entry's comment is not this one's")

    def test_set_quality_rewrites_only_that_entrys_quality_lines(self):
        before = BETA.read_text().splitlines()
        row = self.catalog.find("beta-two")
        self.assertTrue(self.catalog.set_quality(row, 1.5, {task_class: 1.5 for task_class in TASK_CLASSES}))
        after = row.file.render().splitlines()
        diff = [(old, new) for old, new in zip(before, after) if old != new]
        self.assertEqual(len(before), len(after))
        self.assertEqual(len(diff), 2, diff)
        self.assertTrue(all("quality" in new for _, new in diff))

    def test_set_quality_adds_lines_to_an_unscored_entry_inside_its_span(self):
        row = self.catalog.find("alpha-big-2-9")
        self.catalog.set_quality(row, 1.0, {task_class: 1.0 for task_class in TASK_CLASSES})
        text = row.file.render()
        reparsed = load_catalog_texts({ALPHA: text, BETA: BETA.read_text()}, SMALL / "classifier.yaml")
        self.assertEqual(reparsed.find("alpha-big-2-9").quality("Ops"), 1.0)
        self.assertEqual(reparsed.find("alpha-mid-2").quality("Ops"), 1.73, "the next entry is untouched")

    def test_clear_quality_removes_the_lines(self):
        row = self.catalog.find("beta-two")
        self.assertTrue(self.catalog.clear_quality(row))
        reparsed = _small(beta=row.file.render())
        self.assertFalse(reparsed.find("beta-two").scored)
        self.assertFalse(self.catalog.clear_quality(row), "already clear")

    def test_disable_comments_an_entry_out_and_keeps_it_known(self):
        row = self.catalog.find("beta-two")
        self.catalog.disable(row, "retired 2026-11-12: gone from discovery")
        text = row.file.render()
        self.assertIn("retired 2026-11-12", text)
        reparsed = _small(beta=text)
        self.assertIsNone(reparsed.find("beta-two"))
        self.assertIn("beta-two", reparsed.known_ids("beta"))

    def test_parked_entry_is_commented_out_and_known(self):
        self.catalog.add_parked("alpha", "alpha-next", render_parked_entry("alpha-next", 1, "discovered"))
        text = self.catalog.file("alpha").render()
        reparsed = _small(alpha=text)
        self.assertIsNone(reparsed.find("alpha-next"), "lands disabled")
        self.assertIn("alpha-next", reparsed.known_ids("alpha"))
        self.assertIn("# TODO-family:", text, "the family is never guessed from the id")
        self.assertIn("# - version: TODO", text, "nor the version")
        self.assertGreater(text.index("alpha-next"), text.index("alpha-mid-2"), "after the last family")


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
class ScoringTest(unittest.TestCase):
    def setUp(self):
        self.catalog = load_catalog(SMALL)
        self.policy = self.catalog.policy
        self.scored = benchmarks.score_rows(self.policy, self.catalog)

    def test_written_quality_equals_computed(self):
        for row in self.catalog.rows:
            computed = self.scored.rows.get(row.key)
            if computed is None:
                self.assertFalse(row.scored, row.key)
                continue
            self.assertEqual(row.qualities(), computed.quality, row.key)

    def test_every_entry_is_scored_from_exactly_its_own_evidence(self):
        with_evidence = {row.key for row in self.catalog.rows if row.evidence}
        self.assertEqual(set(self.scored.rows), with_evidence)
        for row in self.catalog.rows:
            if row.evidence:
                own = benchmarks.calibrate_evidence(self.policy, row.key, row.evidence)
                self.assertEqual(self.scored.rows[row.key], own, row.key)

    def test_task_override_needs_repeated_meaningful_evidence(self):
        big = self.scored.rows["alpha/alpha-big-2-10"]
        self.assertEqual(big.default_quality, 2.59)
        self.assertEqual(big.quality["BugFix"], 2.85, "two agreeing code results override")
        two = self.scored.rows["beta/beta-two"]
        self.assertEqual(two.quality["Ops"], two.default_quality, "one observation cannot override")

    def test_an_entry_without_evidence_is_unscored_never_copied(self):
        self.assertIn("alpha/alpha-big-2-9", self.scored.unscored)
        self.assertNotIn("alpha/alpha-big-2-9", self.scored.rows)

    def test_calibration_is_anchored_not_relative(self):
        before = self.scored.rows["alpha/alpha-big-2-10"]
        mutated = _small(beta=BETA.read_text().replace("Term-Bench, result: 50", "Term-Bench, result: 69"))
        after = benchmarks.score_rows(mutated.policy, mutated)
        self.assertEqual(after.rows["alpha/alpha-big-2-10"], before, "another model's evidence cannot move this one")
        self.assertNotEqual(after.rows["beta/beta-two"], self.scored.rows["beta/beta-two"])

    def test_unknown_benchmark_is_a_policy_error(self):
        mutated = _small(beta=BETA.read_text().replace("benchmark: Term-Bench", "benchmark: Nope"))
        with self.assertRaises(PolicyError):
            benchmarks.score_rows(mutated.policy, mutated)

    def test_scores_respect_the_band(self):
        low, high = self.policy.quality_band
        for profile in self.scored.rows.values():
            for value in profile.quality.values():
                self.assertTrue(low <= value <= high)

    def test_rank_suggestion_slots_into_the_existing_ladder(self):
        peers = [(1, 16.0), (2, 48.0), (4, 80.0), (5, 160.0)]
        cheap = {"input_per_mtok": 1.0, "output_per_mtok": 3.0}
        pricey = {"input_per_mtok": 20.0, "output_per_mtok": 60.0}
        middle = {"input_per_mtok": 3.0, "output_per_mtok": 15.0}
        self.assertEqual(score.rank_suggestion(self.policy, cheap, peers), 1)
        self.assertEqual(score.rank_suggestion(self.policy, pricey, peers), 5)
        self.assertEqual(score.rank_suggestion(self.policy, middle, peers), 2)
        self.assertEqual(score.rank_suggestion(self.policy, None, peers), 1)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class ValidationTest(unittest.TestCase):
    def _errors(self, old: str, new: str, path: Path = ALPHA) -> set[str]:
        text = path.read_text()
        self.assertIn(old, text)
        mutated = text.replace(old, new, 1)
        catalog = _small(alpha=mutated) if path == ALPHA else _small(beta=mutated)
        return _codes(validate.validate(catalog), validate.ERROR)

    def test_fixture_is_clean_and_lists_its_unscored_entry(self):
        findings = validate.validate(load_catalog(SMALL))
        self.assertEqual([str(f) for f in validate.errors(findings)], [])
        unscored = next(f for f in findings if f.code == "unscored-models")
        self.assertEqual(unscored.severity, validate.WARN)
        self.assertIn("alpha/alpha-big-2-9", unscored.message)
        self.assertIn("rank-exception-applied", _codes(findings, validate.INFO))

    def test_wildcard_id_is_an_error(self):
        self.assertIn("wildcard-key", self._errors("id: alpha-small-1", "id: alpha-small-*"))

    def test_wildcard_reference_is_an_error(self):
        self.assertIn("wildcard-key", self._errors("outranks: [alpha-small-1]", "outranks: [alpha-*]"))

    def test_missing_required_field_is_an_error(self):
        for line in ("    display_name: Alpha Mid 2\n", "    cost_rank: 3\n"):
            with self.subTest(line=line):
                self.assertIn("model-missing-field", self._errors(line, ""))

    def test_non_numeric_version_is_an_error(self):
        self.assertIn("bad-version", self._errors("version: 2.9", "version: 2.x"))

    def test_duplicate_id_is_an_error(self):
        self.assertIn("duplicate-model", self._errors("id: alpha-mid-2", "id: alpha-small-1"))

    def test_retired_and_alias_fields_are_errors(self):
        for field in ("api_model: alpha-mid", "scored_as: alpha-small-1", "compressed_behind: alpha-big-2-10", "family: mid"):
            with self.subTest(field=field):
                codes = self._errors("    id: alpha-mid-2\n", f"    id: alpha-mid-2\n    {field}\n")
                self.assertIn("retired-field", codes)
        self.assertIn("retired-section", self._errors("provider: alpha\n", "provider: alpha\ndiscovery_retire_grace_releases: 1\n"))

    def test_identity_effort_mapping_is_an_error(self):
        codes = self._errors("    id: alpha-mid-2\n", "    id: alpha-mid-2\n    effort_mapping: { high: high, max: high }\n")
        self.assertIn("identity-effort-mapping", codes)

    def test_provider_must_match_its_file(self):
        self.assertIn("provider-mismatch", self._errors("provider: alpha", "provider: gamma"))

    def test_quality_on_an_entry_without_evidence_is_an_error(self):
        codes = self._errors(
            "    context_window: 1000000\n  mid:",
            "    context_window: 1000000\n    default_quality: 2.59\n  mid:",
        )
        self.assertIn("quality-without-evidence", codes)

    def test_hand_edited_quality_drifts(self):
        self.assertIn("benchmark-score-drift", self._errors("default_quality: 1.67", "default_quality: 1.70", BETA))

    def test_out_of_band_quality_is_an_error(self):
        self.assertIn("out-of-band", self._errors("{ UiTweak: 1.27,", "{ UiTweak: 0.05,"))

    def test_broken_outranks_is_an_error(self):
        # beta-two claims to beat alpha-big-2-10: it does not, on any class.
        codes = self._errors(
            "    coding_tier: medium\n    context_window: 256000",
            "    outranks: [alpha/alpha-big-2-10]\n    coding_tier: medium\n    context_window: 256000",
            BETA,
        )
        self.assertIn("order-broken", codes)

    def test_unknown_reference_is_an_error(self):
        self.assertIn("unknown-reference", self._errors("[alpha-small-1, alpha-mid-2]", "[alpha-small-1, alpha-gone]"))
        self.assertIn("unknown-reference", self._errors("outranks: [alpha/alpha-small-1]", "outranks: [alpha-small-1]", BETA))

    def test_outranks_an_unscored_entry_is_reported_not_checked(self):
        mutated = _small(alpha=ALPHA.read_text().replace("[alpha-small-1, alpha-mid-2]", "[alpha-small-1, alpha-big-2-9]"))
        self.assertIn("order-unchecked", _codes(validate.validate(mutated), validate.WARN))

    def test_undocumented_rank_inversion_is_an_error(self):
        self.assertIn(
            "rank-price-inversion",
            self._errors("    rank_exception: a scarce second-company seat, held at the top rank\n", "", BETA),
        )

    def test_stated_cache_rate_must_be_positive_but_need_not_follow_the_ratio(self):
        # alpha-big's 0.25 read is 0.05x input, off alpha's 0.1 ratio: stated, so fine.
        self.assertIn("bad-pricing", self._errors("cache_read_per_mtok: 0.25", "cache_read_per_mtok: 0"))

    def test_off_ladder_rank_is_an_error(self):
        self.assertIn("cost-rank-off-ladder", self._errors("cost_rank: 1", "cost_rank: 7"))

    def test_missing_scoring_section_is_an_error(self):
        classifier = (SMALL / "classifier.yaml").read_text().replace("scoring:", "not_scoring:", 1)
        self.assertIn("bad-scoring", _codes(validate.validate(_small(classifier=classifier)), validate.ERROR))

    def test_unparseable_entry_layout_is_reported(self):
        with self.assertRaises(CatalogError):
            _small(alpha=ALPHA.read_text().replace("  - version: 2\n    id: alpha-mid-2", "  - id: alpha-mid-2\n    version: 2"))


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
class DiscoveryTest(unittest.TestCase):
    def test_unavailable_probe_never_proposes_removals(self):
        found = discover.load_fixture(FIXTURES / "grok-cli-unauthed.json", {"grok": ["grok-4.7"]})
        grok = found["grok"]
        self.assertFalse(grok.trustworthy)
        self.assertEqual(grok.to_remove, [], "a broken probe must not retire live models")
        self.assertEqual(grok.to_add, [])

    def test_snapshot_agents_are_not_treated_as_authoritative(self):
        found = discover.load_fixture(
            FIXTURES / "all-agents-nominal.json", {"claude": ["claude-opus-5-5"], "grok": ["grok-4.7"]}
        )
        self.assertFalse(found["claude"].trustworthy)
        self.assertEqual(found["claude"].to_remove, [])
        self.assertTrue(found["grok"].trustworthy)
        self.assertEqual(found["grok"].to_remove, [])

    def test_fixture_releases_and_retires_are_relative_to_the_catalog(self):
        found = discover.load_fixture(FIXTURES / "opus-next-release.json", {"claude": ["claude-opus-5-5"]})
        self.assertEqual(found["claude"].to_add, ["opus-next[1m]"])
        self.assertEqual(found["claude"].to_remove, [])
        found = discover.load_fixture(FIXTURES / "grok-retirement.json", {"grok": ["grok-4.7", "grok-4.6"]})
        self.assertEqual(found["grok"].to_remove, ["grok-4.6"])

    def test_missing_cli_is_unavailable_not_failed(self):
        result = discover.discover_agent(
            "nope",
            {"method": "command", "command": ["definitely-not-a-real-cli-xyz", "models"]},
            configured=["a-model"],
        )
        self.assertEqual(result.status, discover.UNAVAILABLE)
        self.assertEqual(result.to_remove, [])
        self.assertIn("not on PATH", result.error)

    def test_discovery_specs_come_from_each_provider_file(self):
        policy = load_catalog(SMALL).policy
        spec = policy.discovery
        self.assertEqual(spec["agents"]["alpha"]["method"], "command")
        self.assertEqual(spec["agents"]["beta"]["method"], "snapshot")
        self.assertEqual(policy.retire_grace_releases("alpha"), 1)

    def test_parses_ids_from_json_and_plain_output(self):
        self.assertEqual(
            discover.parse_model_ids('{"data": [{"id": "grok-4.5"}, {"id": "grok-3"}]}'),
            ["grok-4.5", "grok-3"],
        )
        self.assertIn("grok-4.5", discover.parse_model_ids("Available models:\n  grok-4.5 (default)"))

    def test_real_grok_models_output_yields_only_the_model(self):
        # The login line contains "grok.com", which has exactly the shape of a
        # model id — parsing it as one would invent a phantom model.
        real_output = (
            "You are logged in with grok.com.\n\nDefault model: grok-4.5\n\n"
            "Available models:\n  * grok-4.5 (default)\n"
        )
        self.assertEqual(discover.parse_model_ids(real_output, ["grok-4.5"]), ["grok-4.5"])

    def test_known_ids_are_ordered_first_without_a_sort_crash(self):
        parsed = discover.parse_model_ids("Available models:\n  new-9.9\n  grok-4.5\n", ["grok-4.5"])
        self.assertEqual(parsed[0], "grok-4.5")
        self.assertIn("new-9.9", parsed)


# ---------------------------------------------------------------------------
# Goose bridge
# ---------------------------------------------------------------------------
class GooseBridgeTest(unittest.TestCase):
    BAND = (0.5, 3.5)

    def test_accepts_a_well_formed_reply(self):
        payload = {
            "models": [
                {
                    "candidate": "claude/opus-next",
                    "observations": [
                        {"benchmark": "SWE-Bench-Pro", "result": 71.2, "source": "https://example.test/card"}
                    ],
                    "rationale": "launch post only",
                    "risks": ["no seat benchmarks"],
                }
            ],
            "renames": [{"from": "codex/gpt-5.6-sol", "to": "codex/gpt-5.7-sol", "confidence": 0.8}],
        }
        judgments, renames = goose.parse_judgments(payload, self.BAND)
        self.assertEqual(judgments[0].observations[0]["result"], 71.2)
        self.assertEqual(renames[0].new, "codex/gpt-5.7-sol")

    def test_rejects_an_unsourced_observation(self):
        with self.assertRaises(goose.GooseError):
            goose.parse_judgments(
                {"models": [{"candidate": "a/b", "observations": [{"benchmark": "x", "result": 1}]}]},
                self.BAND,
            )

    def test_rejects_a_non_numeric_result(self):
        with self.assertRaises(goose.GooseError):
            goose.parse_judgments(
                {
                    "models": [
                        {
                            "candidate": "a/b",
                            "observations": [{"benchmark": "x", "result": "high", "source": "test"}],
                        }
                    ]
                },
                self.BAND,
            )

    def test_extracts_json_from_a_chatty_transcript(self):
        transcript = 'starting session...\n{"models": [], "renames": []}\ndone\n'
        self.assertEqual(goose._extract_json(transcript), {"models": [], "renames": []})

    def test_rejects_a_transcript_with_no_json(self):
        with self.assertRaises(goose.GooseError):
            goose._extract_json("I could not determine the tier, sorry.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
class CliTest(unittest.TestCase):
    def _run(self, *args: str, data: Path | None = None) -> tuple[int, Path, str]:
        out_dir = Path(tempfile.mkdtemp(prefix="model-update-test-"))
        argv = ["--repo", str(REPO), "--out-dir", str(out_dir), *args]
        if data is not None:
            argv += ["--data", str(data)]
        captured = io.StringIO()
        code = cli.main(argv, stdout=captured)
        return code, out_dir, captured.getvalue()

    def _snapshot(self, data: Path = DATA) -> dict[str, str]:
        return {path.name: path.read_text() for path in sorted((data / "providers").glob("*.yaml"))}

    def test_validate_only_is_green_on_the_shipped_catalog(self):
        code, _, output = self._run("--validate-only")
        self.assertEqual(code, cli.EXIT_OK, output)

    def test_dry_run_on_the_shipped_catalog_requires_no_changes(self):
        # The acceptance bar: the updater reproduces today's catalog exactly.
        before = self._snapshot()
        code, out_dir, output = self._run(
            "--discovery-fixture", str(FIXTURES / "all-agents-nominal.json"), "--today", "2026-11-12"
        )
        self.assertIn("required changes: **0**", (out_dir / "report.md").read_text(), output)
        self.assertEqual(self._snapshot(), before, "a dry run never writes")

    def test_new_release_is_parked_in_its_provider_file(self):
        before = self._snapshot()
        _, out_dir, output = self._run(
            "--discovery-fixture",
            str(FIXTURES / "opus-next-release.json"),
            "--today",
            "2026-11-12",
            "--skip-score",
        )
        self.assertNotIn("model-missing-field", output, "a parked entry is not a live entry")
        diff = (out_dir / "claude.yaml.diff").read_text()
        added = [line for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")]
        removed = [line for line in diff.splitlines() if line.startswith("-") and not line.startswith("---")]
        self.assertEqual(removed, [], "an add never rewrites an existing entry")
        self.assertTrue(all(line.strip() in ("+",) or line.lstrip("+").lstrip().startswith("#") for line in added))
        self.assertIn('id: "opus-next[1m]"', diff)
        self.assertNotIn("quality", "\n".join(added), "a new model is never given a sibling's scores")
        self.assertEqual(self._snapshot(), before)

    def test_new_model_of_another_provider_is_parked_too(self):
        _, out_dir, output = self._run(
            "--discovery-fixture", str(FIXTURES / "new-model-release.json"), "--today", "2026-11-12", "--skip-score"
        )
        self.assertNotIn("model-missing-field", output)
        self.assertIn("#   id: gpt-6-atlas", (out_dir / "codex.yaml.diff").read_text())

    def test_unauthed_probe_produces_no_retirement_diff(self):
        _, out_dir, _ = self._run(
            "--discovery-fixture", str(FIXTURES / "grok-cli-unauthed.json"), "--providers", "grok", "--skip-score"
        )
        self.assertFalse((out_dir / "grok.yaml.diff").exists(), "a failed probe must not retire anything")
        self.assertIn("last-known-good", (out_dir / "report.md").read_text())

    def test_discovery_json_is_written_for_audit(self):
        _, out_dir, _ = self._run("--discovery-fixture", str(FIXTURES / "all-agents-nominal.json"))
        payload = json.loads((out_dir / "discovered.json").read_text())
        self.assertIn("claude", payload["agents"])
        self.assertEqual(payload["agents"]["grok"]["status"], "ok")

    def test_missing_data_dir_is_an_error(self):
        code, _, _ = self._run("--validate-only", data=Path("/tmp/does-not-exist-router-data"))
        self.assertEqual(code, cli.EXIT_VALIDATION)

    def test_apply_rescores_a_drifted_entry_in_place(self):
        data = _sandbox()
        beta = data / "providers" / "beta.yaml"
        pristine = beta.read_text()
        beta.write_text(pristine.replace("default_quality: 1.67", "default_quality: 1.90", 1))
        code, _, _ = self._run("--validate-only", data=data)
        self.assertEqual(code, cli.EXIT_VALIDATION, "drift fails validation")
        code, out_dir, output = self._run("--skip-discover", "--apply", data=data)
        self.assertEqual(code, cli.EXIT_OK, output)
        self.assertEqual(beta.read_text(), pristine, "only the drifted entry's lines move, back to computed")
        self.assertIn("beta/beta-two", (out_dir / "report.md").read_text())

    def test_apply_after_new_evidence_rewrites_only_that_entry(self):
        data = _sandbox()
        alpha = data / "providers" / "alpha.yaml"
        alpha.write_text(alpha.read_text().replace("Code-Bench, result: 68", "Code-Bench, result: 60", 1))
        code, out_dir, output = self._run("--skip-discover", "--apply", data=data)
        self.assertEqual(code, cli.EXIT_OK, output)
        report = (out_dir / "report.md").read_text()
        section = report[report.index("## Re-scored models") : report.index("## Findings")]
        changed = {line for line in section.splitlines() if line.startswith("### ")}
        self.assertEqual(changed, {"### alpha/alpha-big-2-10"}, "no model's score depends on another's")
        self.assertEqual((data / "providers" / "beta.yaml").read_text(), BETA.read_text())
        _, out_dir, _ = self._run("--skip-discover", data=data)
        self.assertIn("required changes: **0**", (out_dir / "report.md").read_text(), "idempotent")

    def test_apply_strips_quality_copied_onto_an_unscored_entry(self):
        data = _sandbox()
        alpha = data / "providers" / "alpha.yaml"
        pristine = alpha.read_text()
        alpha.write_text(
            pristine.replace(
                "    context_window: 1000000\n  mid:", "    context_window: 1000000\n    default_quality: 2.59\n  mid:"
            )
        )
        code, _, output = self._run("--skip-discover", "--apply", data=data)
        self.assertEqual(code, cli.EXIT_OK, output)
        self.assertEqual(alpha.read_text(), pristine)

    def test_apply_parks_and_retires_inside_a_sandbox(self):
        data = _sandbox()
        fixture = data.parent / "discovery.json"
        fixture.write_text(
            json.dumps({"agents": {"alpha": {"status": "ok", "releases": ["alpha-next"], "retires": ["alpha-small-1"]}}})
        )
        code, _, output = self._run("--discovery-fixture", str(fixture), "--today", "2026-11-12", "--apply", data=data)
        # Retiring alpha-small-1 breaks the `outranks` that name it: the run says so.
        self.assertEqual(code, cli.EXIT_VALIDATION, output)
        self.assertIn("unknown-reference", output)
        written = load_catalog(data)
        self.assertIsNone(written.find("alpha-small-1"))
        self.assertIn("alpha-small-1", written.known_ids("alpha"))
        self.assertIn("alpha-next", written.known_ids("alpha"))
        self.assertIn("retired 2026-11-12", (data / "providers" / "alpha.yaml").read_text())
        # Re-running proposes neither again.
        _, out_dir, _ = self._run("--discovery-fixture", str(fixture), data=data)
        self.assertIn("required changes: **0**", (out_dir / "report.md").read_text())


if __name__ == "__main__":
    unittest.main()
