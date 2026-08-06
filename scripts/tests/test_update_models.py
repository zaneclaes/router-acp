"""Tests for the model updater.

    python3 -m unittest discover -s scripts/tests -t .

These run against the REAL `data/scores.yaml`, `data/model-policy.yaml` and
`examples/router-preferred.yaml` — the point is to prove the updater reproduces
today's shipped policy, so fixtures would defeat it. Nothing here writes to the
repo; the CLI is exercised in dry-run with its output directed at a temp dir.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

from update_models import benchmarks, cli  # noqa: E402
from update_models import patterns as pat  # noqa: E402
from update_models import discover, goose, provenance, score, validate  # noqa: E402
from update_models.catalog import parse_catalog  # noqa: E402
from update_models.policy import TASK_CLASSES, PolicyError, load_policy  # noqa: E402
from update_models.scores_file import parse_scores, parse_scores_text  # noqa: E402

POLICY_PATH = REPO / "data" / "model-policy.yaml"
SCORES_PATH = REPO / "data" / "scores.yaml"
CATALOG_PATH = REPO / "examples" / "router-preferred.yaml"
FIXTURES = REPO / "scripts" / "fixtures" / "discovery"


class PatternSpecificityTest(unittest.TestCase):
    def test_glob_matches_like_the_rust_matcher(self):
        self.assertTrue(pat.glob_match("*", "anything/at-all"))
        self.assertTrue(pat.glob_match("claude/*", "claude/sonnet"))
        self.assertTrue(pat.glob_match("*sonnet*", "claude/claude-sonnet-4"))
        self.assertTrue(pat.glob_match("CLAUDE/Sonnet", "claude/sonnet"))
        self.assertFalse(pat.glob_match("codex/*", "claude/sonnet"))

    def test_gemini_is_narrower_than_mini_because_of_the_substring(self):
        # The whole reason the bug existed: "gemini" contains "mini".
        self.assertTrue(pat.is_narrower("*gemini*pro*", "*mini*"))
        self.assertFalse(pat.is_narrower("*mini*", "*gemini*pro*"))

    def test_known_narrowing_pairs(self):
        self.assertTrue(pat.is_narrower("*gpt-5*codex*", "*gpt-5*"))
        self.assertTrue(pat.is_narrower("*grok*code*", "*grok*"))
        self.assertTrue(pat.is_narrower("*kimi*k2*thinking*", "*kimi*"))
        self.assertFalse(pat.is_narrower("*opus*", "*sonnet*"))
        self.assertFalse(pat.is_narrower("*kimi*k2*thinking*", "*mini*"))

    def test_order_violation_is_detected_and_fixable(self):
        broken = ["*mini*", "*gemini*pro*", "*gpt-5*"]
        violations = pat.order_violations(broken)
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].narrow, "*gemini*pro*")
        self.assertEqual(violations[0].broad, "*mini*")
        fixed = pat.sorted_by_specificity(broken)
        self.assertLess(fixed.index("*gemini*pro*"), fixed.index("*mini*"))
        self.assertEqual(pat.order_violations(fixed), [])

    def test_shipped_table_has_no_shadowed_patterns(self):
        scores = parse_scores(SCORES_PATH)
        self.assertEqual(
            [violation.describe() for violation in pat.order_violations(scores.patterns)],
            [],
        )
        self.assertEqual(pat.duplicate_patterns(scores.patterns), [])


class ScoresFileTest(unittest.TestCase):
    def test_round_trip_is_byte_identical(self):
        # The writer only earns the right to touch this file if reassembling an
        # untouched one returns the original bytes.
        scores = parse_scores(SCORES_PATH)
        self.assertEqual(scores.render(), SCORES_PATH.read_text())

    def test_every_entry_parsed_with_its_fields(self):
        scores = parse_scores(SCORES_PATH)
        fable = scores.entry("*fable*")
        self.assertIsNotNone(fable)
        self.assertEqual(fable.fields["context_window"], 1000000)
        self.assertAlmostEqual(fable.quality("Architecture"), 2.64)
        # `*gpt-5*` has no per-class block, so every class falls back.
        generic = scores.entry("*gpt-5*")
        self.assertAlmostEqual(generic.quality("Architecture"), generic.fields["default_quality"])

    def test_comments_travel_with_their_entry(self):
        scores = parse_scores(SCORES_PATH)
        gemini = scores.entry("*gemini*pro*")
        self.assertTrue(
            any("mini" in line for line in gemini.comment_lines),
            "the gemini entry keeps the comment explaining why it precedes *mini*",
        )

    def test_reorder_moves_only_the_named_entry(self):
        scores = parse_scores(SCORES_PATH)
        order = list(reversed(scores.patterns))
        rendered = scores.render(order=order)
        self.assertEqual(parse_scores_text(rendered, SCORES_PATH).patterns, order)

    def test_generated_entry_parses_as_yaml_and_keeps_class_order(self):
        policy = load_policy(POLICY_PATH)
        scores = parse_scores(SCORES_PATH)
        profile = benchmarks.derive(policy)["*sol*"]
        proposal = score.propose_from_benchmarks(
            policy,
            scores,
            "codex/gpt-6-atlas",
            "*gpt-6-atlas*",
            profile.default_quality,
            profile.quality,
            profile.observations,
        )
        entry = proposal.to_entry(
            provenance.entry_comment(proposal, source="test", generated_at="2026-07-26")
        )
        scores.insert_entry(entry, before=proposal.insert_before)
        reparsed = parse_scores_text(scores.render(), SCORES_PATH)
        added = reparsed.entry(proposal.pattern)
        self.assertIsNotNone(added, "generated entry survives a YAML round trip")
        self.assertEqual(list(added.fields["quality"]), TASK_CLASSES)


class CatalogTest(unittest.TestCase):
    def test_round_trip_is_byte_identical(self):
        catalog = parse_catalog(CATALOG_PATH)
        self.assertEqual(catalog.render(), CATALOG_PATH.read_text())

    def test_reads_enabled_and_disabled_models(self):
        catalog = parse_catalog(CATALOG_PATH)
        ids = {model.candidate for model in catalog.all_models()}
        self.assertIn("claude/opus[1m]", ids)
        self.assertIn("kimi/kimi-k2", ids)
        # Commented-out models are parsed too — that is how retirement and
        # "parked until scored" are represented.
        self.assertIn("grok/grok-code-fast-1", ids)
        self.assertFalse(catalog.model("grok/grok-code-fast-1").enabled)
        self.assertTrue(catalog.model("grok/grok-4.5").enabled)

    def test_multiline_flow_maps_are_one_model(self):
        catalog = parse_catalog(CATALOG_PATH)
        fable = catalog.model("claude/claude-fable-5[1m]")
        self.assertGreater(fable.end - fable.start, 1, "spans its wrapped pricing lines")
        self.assertEqual(fable.cost_rank, 5)
        self.assertAlmostEqual(fable.pricing["output_per_mtok"], 50.0)

    def test_set_price_preserves_decimal_formatting(self):
        catalog = parse_catalog(CATALOG_PATH)
        sonnet = catalog.model("claude/sonnet")
        self.assertTrue(catalog.set_price(sonnet, "input_per_mtok", 3.5))
        self.assertIn("input_per_mtok: 3.50", catalog.render())

    def test_disable_comments_out_every_line_with_a_note(self):
        catalog = parse_catalog(CATALOG_PATH)
        luna = catalog.model("codex/gpt-5.6-luna")
        catalog.disable(luna, "retired 2026-07-26: gone from discovery")
        rendered = catalog.render().splitlines()
        note_line = next(index for index, line in enumerate(rendered) if "retired 2026-07-26" in line)
        self.assertTrue(rendered[note_line + 1].lstrip().startswith("#"))
        # Still parseable, and the model is now disabled rather than deleted.
        reparsed = parse_catalog(_temp_copy(catalog.render(), suffix=".yaml"))
        self.assertFalse(reparsed.model("codex/gpt-5.6-luna").enabled)


class DiscoveryTest(unittest.TestCase):
    def test_unavailable_probe_never_proposes_removals(self):
        found = discover.load_fixture(FIXTURES / "grok-cli-unauthed.json", {"grok": ["grok-4.5"]})
        grok = found["grok"]
        self.assertFalse(grok.trustworthy)
        self.assertEqual(grok.to_remove, [], "a broken probe must not retire live models")
        self.assertEqual(grok.to_add, [])

    def test_snapshot_agents_are_not_treated_as_authoritative(self):
        found = discover.load_fixture(FIXTURES / "all-agents-nominal.json", {})
        self.assertFalse(found["claude"].trustworthy)
        self.assertEqual(found["claude"].to_remove, [])
        self.assertTrue(found["grok"].trustworthy)

    def test_new_release_shows_up_as_an_addition(self):
        found = discover.load_fixture(FIXTURES / "opus-next-release.json", {})
        self.assertEqual(found["claude"].to_add, ["opus-next[1m]"])
        self.assertEqual(found["claude"].to_remove, [])

    def test_missing_cli_is_unavailable_not_failed(self):
        result = discover.discover_agent(
            "nope",
            {"method": "command", "command": ["definitely-not-a-real-cli-xyz", "models"]},
            configured=["a-model"],
        )
        self.assertEqual(result.status, discover.UNAVAILABLE)
        self.assertEqual(result.to_remove, [])
        self.assertIn("not on PATH", result.error)

    def test_parses_ids_from_json_and_plain_output(self):
        self.assertEqual(
            discover.parse_model_ids('{"data": [{"id": "grok-4.5"}, {"id": "grok-3"}]}'),
            ["grok-4.5", "grok-3"],
        )
        self.assertIn("grok-4.5", discover.parse_model_ids("Available models:\n  grok-4.5 (default)"))

    def test_real_grok_models_output_yields_only_the_model(self):
        # Verbatim `grok models` output. The login line contains "grok.com",
        # which has exactly the shape of a model id — parsing it as one would
        # invent a phantom model and propose adding it to the catalog.
        real_output = (
            "You are logged in with grok.com.\n"
            "\n"
            "Default model: grok-4.5\n"
            "\n"
            "Available models:\n"
            "  * grok-4.5 (default)\n"
        )
        self.assertEqual(discover.parse_model_ids(real_output, ["grok-4.5"]), ["grok-4.5"])

    def test_known_ids_are_ordered_first_without_a_sort_crash(self):
        parsed = discover.parse_model_ids(
            "Available models:\n  new-9.9\n  grok-4.5\n", ["grok-4.5"]
        )
        self.assertEqual(parsed[0], "grok-4.5")
        self.assertIn("new-9.9", parsed)


class ScoreProposalTest(unittest.TestCase):
    def setUp(self):
        self.policy = load_policy(POLICY_PATH)
        self.scores = parse_scores(SCORES_PATH)

    def test_benchmarks_reproduce_every_evidenced_family(self):
        for pattern, profile in benchmarks.derive(self.policy).items():
            entry = self.scores.entry(pattern)
            self.assertIsNotNone(entry, pattern)
            self.assertEqual(entry.fields["default_quality"], profile.default_quality)
            self.assertEqual(entry.qualities(), profile.quality)

    def test_benchmark_calibration_is_anchored_not_relative(self):
        before = benchmarks.derive(self.policy)
        self.policy.benchmark_scoring["model_evidence"]["*unrelated*"] = [
            {
                "benchmark": "AA-Coding-Agent-v1.1",
                "result": 71,
                "source": "test",
            }
        ]
        after = benchmarks.derive(self.policy)
        self.assertEqual(before["*sol*"], after["*sol*"])

    def test_benchmark_scores_respect_the_working_band(self):
        low, high = self.policy.quality_band
        for profile in benchmarks.derive(self.policy).values():
            for value in profile.quality.values():
                self.assertGreaterEqual(value, low)
                self.assertLessEqual(value, high)

    def test_task_overrides_require_repeated_meaningful_evidence(self):
        profiles = benchmarks.derive(self.policy)
        sol = profiles["*sol*"]
        sonnet = profiles["*sonnet*"]

        self.assertEqual(sol.default_quality, 2.81)
        self.assertEqual(sol.quality["BugFix"], 2.39)
        self.assertEqual(
            sonnet.quality["BugFix"],
            sonnet.default_quality,
            "one task-relevant observation cannot create an override",
        )

    def test_frontier_labels_do_not_override_measured_results(self):
        profiles = benchmarks.derive(self.policy)
        self.assertGreater(profiles["*sol*"].default_quality, profiles["*fable*"].default_quality)
        self.assertGreater(
            profiles["*fable*"].quality["BugFix"],
            profiles["*sol*"].quality["BugFix"],
            "task evidence may reverse the aggregate order when the delta is meaningful",
        )

    def test_compression_pins_behind_to_ahead_minus_max_gap(self):
        profiles = benchmarks.derive(self.policy)
        compression = self.policy.benchmark_scoring["compression"]
        gap = compression["max_gap"]
        for pair in compression["pairs"]:
            ahead, behind = profiles[pair["ahead"]], profiles[pair["behind"]]
            self.assertAlmostEqual(behind.default_quality, ahead.default_quality - gap, places=2)
            for task_class in TASK_CLASSES:
                self.assertAlmostEqual(
                    behind.quality[task_class],
                    ahead.quality[task_class] - gap,
                    places=2,
                    msg=f"{pair['behind']}/{task_class}",
                )

    def test_compression_supersedes_raw_evidence_not_the_ahead_side(self):
        # The `ahead` member of a declared pair keeps its own calibrated
        # scores untouched; only `behind`'s OUTPUT is overwritten.
        profiles = benchmarks.derive(self.policy)
        raw = dict(self.policy.benchmark_scoring["model_evidence"])
        uncompressed_opus = benchmarks._calibrate(
            79.2, self.policy.benchmark_scoring["benchmarks"]["SWE-Bench-Pro"]["anchors"],
            self.policy.quality_band,
        )
        self.assertNotEqual(
            profiles["*opus*"].default_quality,
            uncompressed_opus,
            "compression must overwrite the behind side's own evidence-derived output",
        )
        self.assertEqual(raw["*opus*"][0]["result"], 79.2, "raw evidence log is untouched")

    def test_compression_pair_referencing_unscored_pattern_is_a_policy_error(self):
        self.policy.benchmark_scoring["compression"]["pairs"].append(
            {"ahead": "*fable*", "behind": "*does-not-exist*"}
        )
        with self.assertRaises(PolicyError):
            benchmarks.derive(self.policy)

    def test_suggested_pattern_can_never_swallow_an_existing_family(self):
        # The deterministic default is the whole id: specific enough that it can
        # never capture another model, which is the mistake `*mini*` made. It may
        # itself need placing above a broader family — that is what the insertion
        # anchor is for — but it must never be the broad side of a violation.
        for model_id in ("gpt-5.7-sol", "kimi-k3-thinking", "opus-next[1m]", "atlas-1"):
            pattern = score.suggest_pattern(model_id)
            self.assertEqual(pattern, f"*{model_id.lower()}*")
            swallowed = [
                violation.narrow
                for violation in pat.order_violations([pattern, *self.scores.patterns])
                if violation.broad == pattern
            ]
            self.assertEqual(swallowed, [], f"{pattern} placed FIRST still swallows nothing")

    def test_insertion_anchor_places_a_new_entry_above_what_would_shadow_it(self):
        anchor = score.insertion_anchor(self.scores, "*opus-next[1m]*")
        self.assertEqual(anchor, "*opus*", "a narrower opus id must sit above `*opus*`")
        self.assertIsNone(
            score.insertion_anchor(self.scores, "*gpt-6-atlas*"),
            "an id no existing pattern matches needs no anchor",
        )

    def test_unbenchmarked_name_never_gets_a_guessed_profile(self):
        profiles = benchmarks.derive(self.policy)
        self.assertIsNone(benchmarks.resolve_profile(profiles, "codex/gpt-6-atlas"))

    def test_rank_suggestion_slots_into_the_existing_ladder(self):
        peers = [(1, 16.0), (2, 48.0), (4, 80.0), (5, 160.0)]
        cheap = {"input_per_mtok": 1.0, "output_per_mtok": 3.0}  # blended 10
        pricey = {"input_per_mtok": 20.0, "output_per_mtok": 60.0}  # blended 200
        middle = {"input_per_mtok": 3.0, "output_per_mtok": 15.0}  # blended 48
        self.assertEqual(score.rank_suggestion(self.policy, cheap, peers), 1)
        self.assertEqual(score.rank_suggestion(self.policy, pricey, peers), 5)
        self.assertEqual(score.rank_suggestion(self.policy, middle, peers), 2)


class ValidationTest(unittest.TestCase):
    def setUp(self):
        self.policy = load_policy(POLICY_PATH)
        self.scores = parse_scores(SCORES_PATH)
        self.catalog = parse_catalog(CATALOG_PATH)

    def test_shipped_tables_pass_every_invariant(self):
        findings = validate.validate(self.policy, self.scores, self.catalog)
        self.assertEqual(
            [str(finding) for finding in validate.errors(findings)],
            [],
            "current main is the baseline: it must validate clean",
        )

    def test_documented_rank_exception_is_reported_as_info_not_error(self):
        findings = validate.validate(self.policy, self.scores, self.catalog)
        codes = {finding.code for finding in findings}
        self.assertIn("rank-exception-applied", codes)
        self.assertNotIn("rank-price-inversion", codes)

    def test_undocumented_rank_inversion_is_an_error(self):
        # Make luna (cheap) outrank terra (pricier) with no exception on file.
        luna = self.catalog.model("codex/gpt-5.6-luna")
        self.catalog.set_scalar(luna, "cost_rank", 5)
        findings = validate.validate(self.policy, self.scores, self.catalog)
        self.assertIn("rank-price-inversion", {finding.code for finding in findings})

    def test_shadowed_gemini_pattern_fails_validation(self):
        # Reproduce the pre-fix table: `*mini*` above the gemini entries.
        patterns = self.scores.patterns
        broken_order = [p for p in patterns if not p.startswith("*gemini")]
        broken_order.insert(broken_order.index("*mini*") + 1, "*gemini*pro*")
        broken_order.insert(broken_order.index("*mini*") + 2, "*gemini*flash*")
        broken = parse_scores_text(self.scores.render(order=broken_order), SCORES_PATH)
        findings = validate.validate(self.policy, broken, self.catalog)
        codes = {finding.code for finding in validate.errors(findings)}
        self.assertIn("pattern-shadowed", codes)
        self.assertIn("wrong-pattern", codes)
        self.assertIn("order-broken", codes, "gemini pro/flash collapse to one curve")

    def test_benchmark_score_drift_is_an_error(self):
        text = SCORES_PATH.read_text().replace("      Architecture: 2.64", "      Architecture: 0.50", 1)
        mutated = parse_scores_text(text, SCORES_PATH)
        codes = {
            finding.code
            for finding in validate.errors(validate.validate(self.policy, mutated, self.catalog))
        }
        self.assertIn("benchmark-score-drift", codes)

    def test_out_of_band_quality_is_an_error(self):
        text = SCORES_PATH.read_text().replace("    default_quality: 0.55", "    default_quality: 0.05")
        mutated = parse_scores_text(text, SCORES_PATH)
        codes = {
            finding.code
            for finding in validate.errors(validate.validate(self.policy, mutated, self.catalog))
        }
        self.assertIn("out-of-band", codes)

    def test_unscored_enabled_model_is_an_error(self):
        catalog = parse_catalog(CATALOG_PATH)
        model = catalog.model("codex/gpt-5.5")
        catalog.set_scalar(model, "id", "unknown-vendor-model")
        model.model_id = "unknown-vendor-model"
        findings = validate.validate(self.policy, self.scores, catalog)
        self.assertIn("unscored-model", {finding.code for finding in validate.errors(findings)})

    def test_patterns_match_the_agent_name_too(self):
        # `agent/model` is what gets matched, so the agent segment counts: a
        # `grok/anything` id still resolves to the `*grok*` family. Worth
        # pinning — it is why a per-agent rename cannot silently unscore a model.
        self.assertEqual(pat.resolve(self.scores.patterns, "grok/whatever-3"), "*grok*")


class GooseBridgeTest(unittest.TestCase):
    BAND = (0.5, 3.5)

    def test_accepts_a_well_formed_reply(self):
        payload = {
            "models": [
                {
                    "candidate": "claude/opus-next[1m]",
                    "observations": [
                        {
                            "benchmark": "SWE-Bench-Pro",
                            "result": 71.2,
                            "source": "https://example.test/model-card",
                        }
                    ],
                    "rationale": "launch post only",
                    "risks": ["no seat benchmarks"],
                }
            ],
            "renames": [
                {"from": "codex/gpt-5.6-sol", "to": "codex/gpt-5.7-sol", "confidence": 0.8}
            ],
        }
        judgments, renames = goose.parse_judgments(payload, self.BAND)
        self.assertEqual(judgments[0].observations[0]["result"], 71.2)
        self.assertEqual(renames[0].new, "codex/gpt-5.7-sol")

    def test_rejects_an_unsourced_observation(self):
        with self.assertRaises(goose.GooseError):
            goose.parse_judgments(
                {
                    "models": [
                        {
                            "candidate": "a/b",
                            "observations": [{"benchmark": "x", "result": 1}],
                        }
                    ]
                },
                self.BAND,
            )

    def test_rejects_a_non_numeric_result(self):
        with self.assertRaises(goose.GooseError):
            goose.parse_judgments(
                {
                    "models": [
                        {
                            "candidate": "a/b",
                            "observations": [
                                {"benchmark": "x", "result": "high", "source": "test"}
                            ],
                        }
                    ]
                },
                self.BAND,
            )

    def test_extracts_json_from_a_chatty_transcript(self):
        transcript = 'starting session...\n{"models": [], "renames": []}\ndone\n'
        payload = goose._extract_json(transcript)
        self.assertEqual(payload, {"models": [], "renames": []})

    def test_rejects_a_transcript_with_no_json(self):
        with self.assertRaises(goose.GooseError):
            goose._extract_json("I could not determine the tier, sorry.")


class CliTest(unittest.TestCase):
    def _run(self, *args: str) -> tuple[int, Path]:
        out_dir = Path(tempfile.mkdtemp(prefix="model-update-test-"))
        code = cli.main(["--repo", str(REPO), "--out-dir", str(out_dir), *args])
        return code, out_dir

    def test_validate_only_is_green_on_current_main(self):
        code, _ = self._run("--validate-only")
        self.assertEqual(code, cli.EXIT_OK)

    def test_dry_run_on_current_catalog_requires_no_changes(self):
        # The acceptance bar: the updater reproduces today's policy exactly.
        code, out_dir = self._run(
            "--discovery-fixture", str(FIXTURES / "all-agents-nominal.json"), "--today", "2026-07-26"
        )
        self.assertEqual(code, cli.EXIT_OK)
        report = (out_dir / "report.md").read_text()
        self.assertIn("required changes: **0**", report)
        self.assertIn("No changes required", report)
        self.assertFalse((out_dir / "scores.yaml.diff").exists())
        self.assertFalse((out_dir / "router-preferred.yaml.diff").exists())
        # And it did not touch the repo.
        self.assertEqual(parse_scores(SCORES_PATH).render(), SCORES_PATH.read_text())

    def test_dry_run_never_writes_even_when_it_has_changes(self):
        before = SCORES_PATH.read_text()
        catalog_before = CATALOG_PATH.read_text()
        self._run("--discovery-fixture", str(FIXTURES / "new-family-release.json"))
        self.assertEqual(SCORES_PATH.read_text(), before)
        self.assertEqual(CATALOG_PATH.read_text(), catalog_before)

    def test_version_bump_release_inherits_its_family_without_score_churn(self):
        # `opus-next[1m]` already matches `*opus*`, which is how the table is
        # designed (patterns key on the line so bumps inherit). It must be parked
        # in the catalog and REPORTED, not silently given a differentiated curve.
        code, out_dir = self._run(
            "--discovery-fixture", str(FIXTURES / "opus-next-release.json"), "--today", "2026-07-26"
        )
        self.assertEqual(code, cli.EXIT_OK, "a new release must not break the invariants")
        report = (out_dir / "report.md").read_text()
        self.assertIn("opus-next[1m]", report)
        self.assertIn("inherits the existing `*opus*` score", report)
        self.assertFalse(
            (out_dir / "scores.yaml.diff").exists(), "inheriting means no score-table churn"
        )

        catalog_diff = (out_dir / "router-preferred.yaml.diff").read_text()
        self.assertRegex(catalog_diff, r"\+\s+# - \{ id: ", "lands commented out, not enabled")
        # The existing ladder is untouched — nothing was rewritten to chase quality.
        self.assertNotIn("-      - { id:", catalog_diff)
        catalog = parse_catalog(CATALOG_PATH)
        self.assertEqual(
            [model.cost_rank for model in catalog.agent("claude").enabled_models], [1, 2, 4, 5]
        )

    def test_new_family_without_benchmarks_is_parked_unscored(self):
        code, out_dir = self._run(
            "--discovery-fixture", str(FIXTURES / "new-family-release.json"), "--today", "2026-07-26"
        )
        self.assertEqual(code, cli.EXIT_OK, "adding a model must not break the invariants")

        self.assertFalse((out_dir / "scores.yaml.diff").exists())
        report = (out_dir / "report.md").read_text()
        self.assertIn("remains disabled/unscored", report)
        self.assertIn("no calibrated benchmark evidence", report)

        catalog_diff = (out_dir / "router-preferred.yaml.diff").read_text()
        self.assertIn("gpt-6-atlas", catalog_diff)
        self.assertRegex(catalog_diff, r"\+\s+# - \{ id: ", "lands commented out, not enabled")
        # No existing model's rank or score moved: this is an add, not a rescore.
        self.assertNotIn("-      - { id:", catalog_diff)
        catalog = parse_catalog(CATALOG_PATH)
        self.assertEqual(
            [model.cost_rank for model in catalog.agent("codex").enabled_models], [1, 3, 2, 4, 5]
        )

    def test_unauthed_probe_produces_no_retirement_diff(self):
        code, out_dir = self._run(
            "--discovery-fixture", str(FIXTURES / "grok-cli-unauthed.json"), "--providers", "grok"
        )
        self.assertEqual(code, cli.EXIT_OK)
        self.assertFalse(
            (out_dir / "router-preferred.yaml.diff").exists(),
            "a failed probe must not retire anything",
        )
        self.assertIn("last-known-good", (out_dir / "report.md").read_text())

    def test_missing_explicit_config_is_an_error_not_a_silent_pass(self):
        # Without a catalog every catalog check trivially passes, which reads as
        # a clean audit. A typo'd --config must fail loudly instead.
        code, _ = self._run("--validate-only", "--config", "/tmp/does-not-exist-router.yaml")
        self.assertEqual(code, cli.EXIT_VALIDATION)

    def test_audits_a_generated_router_config_read_only(self):
        # The documented Hickory case: point --config at a merged/generated file.
        generated = Path(tempfile.mkdtemp(prefix="generated-router-")) / "router.yaml"
        generated.write_text(CATALOG_PATH.read_text())
        code, _ = self._run("--validate-only", "--config", str(generated))
        self.assertEqual(code, cli.EXIT_OK)
        self.assertEqual(generated.read_text(), CATALOG_PATH.read_text(), "audit does not write")

    def test_discovery_json_is_written_for_audit(self):
        _, out_dir = self._run("--discovery-fixture", str(FIXTURES / "all-agents-nominal.json"))
        payload = json.loads((out_dir / "discovered.json").read_text())
        self.assertIn("claude", payload["agents"])
        self.assertEqual(payload["agents"]["grok"]["status"], "ok")

    def test_apply_writes_only_inside_a_sandbox_copy(self):
        # Prove --apply really writes, without touching the checkout: copy the
        # three inputs into a temp repo and apply there.
        sandbox = Path(tempfile.mkdtemp(prefix="model-update-apply-"))
        (sandbox / "data").mkdir()
        (sandbox / "examples").mkdir()
        (sandbox / "data" / "scores.yaml").write_text(SCORES_PATH.read_text())
        (sandbox / "data" / "model-policy.yaml").write_text(POLICY_PATH.read_text())
        (sandbox / "examples" / "router-preferred.yaml").write_text(CATALOG_PATH.read_text())

        code = cli.main(
            [
                "--repo",
                str(sandbox),
                "--out-dir",
                str(sandbox / "out"),
                "--discovery-fixture",
                str(FIXTURES / "new-family-release.json"),
                "--today",
                "2026-07-26",
                "--apply",
            ]
        )
        self.assertEqual(code, cli.EXIT_OK)
        written = (sandbox / "examples" / "router-preferred.yaml").read_text()
        self.assertIn("gpt-6-atlas", written)
        # Applied output is still valid, still ordered, still passing.
        self.assertEqual(
            cli.main(["--repo", str(sandbox), "--out-dir", str(sandbox / "out2"), "--validate-only"]),
            cli.EXIT_OK,
        )
        # Re-running is idempotent: nothing left to change.
        code = cli.main(
            [
                "--repo",
                str(sandbox),
                "--out-dir",
                str(sandbox / "out3"),
                "--discovery-fixture",
                str(FIXTURES / "new-family-release.json"),
                "--today",
                "2026-07-26",
            ]
        )
        self.assertEqual(code, cli.EXIT_OK)
        self.assertIn("required changes: **0**", (sandbox / "out3" / "report.md").read_text())


def _temp_copy(text: str, suffix: str) -> Path:
    handle = tempfile.NamedTemporaryFile("w", suffix=suffix, delete=False)
    handle.write(text)
    handle.close()
    return Path(handle.name)


if __name__ == "__main__":
    unittest.main()
