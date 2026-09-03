"""Invariant checks over the score table + catalog.

The contract this enforces: absolute quality numbers may drift, ORDERING may
not. Everything here runs through the same first-match glob the router uses, on
concrete `agent/model` witness ids, so a pattern-ordering mistake fails the
check rather than hiding behind a plausible-looking number.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from . import patterns as pat
from . import benchmarks
from .catalog import Catalog, CatalogModel
from .policy import TASK_CLASSES, Policy
from .scores_file import ScoresFile

ERROR = "error"
WARN = "warn"
INFO = "info"


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    message: str
    detail: str = ""

    def __str__(self) -> str:
        head = f"[{self.severity.upper()}] {self.code}: {self.message}"
        return f"{head}\n    {self.detail}" if self.detail else head


class Validator:
    def __init__(self, policy: Policy, scores: ScoresFile, catalog: Catalog | None):
        self.policy = policy
        self.scores = scores
        self.catalog = catalog
        self.findings: list[Finding] = []

    # -- helpers ----------------------------------------------------------
    def _add(self, severity: str, code: str, message: str, detail: str = "") -> None:
        self.findings.append(Finding(severity, code, message, detail))

    def _qualities(self, candidate_id: str) -> dict[str, float] | None:
        """Resolved per-class quality for an id, via first-match lookup."""
        pattern = pat.resolve(self.scores.patterns, candidate_id)
        if pattern is None:
            return None
        entry = self.scores.entry(pattern)
        return entry.qualities() if entry else None

    def _witness_qualities(self, name: str) -> tuple[str, dict[str, float]] | None:
        candidate_id = self.policy.witness_id(name)
        qualities = self._qualities(candidate_id)
        if qualities is None:
            self._add(
                ERROR,
                "no-score-entry",
                f"witness `{name}` ({candidate_id}) matches no score-table pattern",
                "it would fall back to the neutral 0.5 default and route unpredictably",
            )
            return None
        return candidate_id, qualities

    # -- checks -----------------------------------------------------------
    def check_pattern_order(self) -> None:
        table_patterns = self.scores.patterns
        for duplicate in pat.duplicate_patterns(table_patterns):
            self._add(
                ERROR,
                "duplicate-pattern",
                f"`{duplicate}` is listed more than once",
                "first match wins, so the later copy is dead",
            )
        violations = pat.order_violations(table_patterns)
        for violation in violations:
            self._add(ERROR, "pattern-shadowed", violation.describe())
        if violations:
            self._add(
                INFO,
                "pattern-order-fix",
                "a non-shadowing order exists",
                " → ".join(pat.sorted_by_specificity(table_patterns)),
            )

    def check_resolution(self) -> None:
        expected: dict[str, str] = self.policy.invariants.get("resolves_to") or {}
        for candidate_id, want in expected.items():
            got = pat.resolve(self.scores.patterns, candidate_id)
            if got != want:
                self._add(
                    ERROR,
                    "wrong-pattern",
                    f"`{candidate_id}` resolves to `{got}`, expected `{want}`",
                    "pattern order in data/scores.yaml sends this id to the wrong family",
                )

    def check_band(self) -> None:
        low, high = self.policy.quality_band
        for entry in self.scores.entries:
            for task_class, value in entry.qualities().items():
                if not low <= value <= high:
                    self._add(
                        ERROR,
                        "out-of-band",
                        f"`{entry.pattern}` {task_class}={value:.2f} is outside "
                        f"the working band [{low:.2f}, {high:.2f}]",
                    )

    def _chain(self, chain: Iterable[str], why: str, strict: bool) -> None:
        resolved: list[tuple[str, str, dict[str, float]]] = []
        for name in chain:
            found = self._witness_qualities(name)
            if found is None:
                return
            resolved.append((name, found[0], found[1]))
        relation = "must score strictly above" if strict else "must not score below"

        def is_broken(above: float, below: float) -> bool:
            return above <= below if strict else above < below

        for (upper_name, upper_id, upper), (lower_name, lower_id, lower) in zip(resolved, resolved[1:]):
            broken = [
                f"{task_class} {upper[task_class]:.2f} vs {lower[task_class]:.2f}"
                for task_class in TASK_CLASSES
                if is_broken(upper[task_class], lower[task_class])
            ]
            if broken:
                self._add(
                    ERROR,
                    "order-broken",
                    f"{upper_name} ({upper_id}) {relation} {lower_name} ({lower_id})",
                    f"{why} — offending classes: {', '.join(broken)}",
                )

    def check_orderings(self) -> None:
        for rule in self.policy.invariants.get("strict_desc") or []:
            self._chain(rule["chain"], " ".join(str(rule.get("why", "")).split()), strict=True)
        for rule in self.policy.invariants.get("weak_desc") or []:
            self._chain(rule["chain"], " ".join(str(rule.get("why", "")).split()), strict=False)

    def check_ceilings(self) -> None:
        for rule in self.policy.invariants.get("at_most") or []:
            lower = self._witness_qualities(rule["a"])
            upper = self._witness_qualities(rule["b"])
            if lower is None or upper is None:
                continue
            why = " ".join(str(rule.get("why", "")).split())
            broken = [
                f"{task_class} {lower[1][task_class]:.2f} > {upper[1][task_class]:.2f}"
                for task_class in TASK_CLASSES
                if lower[1][task_class] > upper[1][task_class]
            ]
            if broken:
                self._add(
                    ERROR,
                    "ceiling-broken",
                    f"{rule['a']} ({lower[0]}) must not score above {rule['b']} ({upper[0]})",
                    f"{why} — offending classes: {', '.join(broken)}",
                )

    def check_benchmark_scores(self) -> None:
        """Checked-in quality must equal the deterministic benchmark result."""
        for pattern, expected in benchmarks.derive(self.policy).items():
            entry = self.scores.entry(pattern)
            if entry is None:
                self._add(ERROR, "benchmark-pattern-missing", f"`{pattern}` has benchmark evidence but no score entry")
                continue
            if (
                abs(float(entry.fields.get("default_quality", 0.5)) - expected.default_quality) > 1e-9
                or entry.qualities() != expected.quality
            ):
                self._add(
                    ERROR,
                    "benchmark-score-drift",
                    f"`{pattern}` does not match its benchmark-derived score",
                    f"run the updater to restore base {expected.default_quality:.2f}",
                )

    # -- catalog ----------------------------------------------------------
    def check_catalog(self) -> None:
        if self.catalog is None:
            return
        low, high = self.policy.ladder
        benchmark_profiles = benchmarks.derive(self.policy)
        for model in self.catalog.all_models():
            if not model.enabled:
                continue
            if pat.resolve(self.scores.patterns, model.candidate) is None:
                self._add(
                    ERROR,
                    "unscored-model",
                    f"`{model.candidate}` is enabled but matches no score-table pattern",
                    "it resolves to the neutral 0.5 default, so `auto` ranks it blind",
                )
            if benchmarks.resolve_profile(benchmark_profiles, model.candidate) is None:
                self._add(
                    ERROR,
                    "model-without-benchmarks",
                    f"`{model.candidate}` is enabled without calibrated benchmark evidence",
                    "disable it or add sourced observations under benchmark_scoring.model_evidence",
                )
            rank = model.cost_rank
            if rank is None:
                self._add(ERROR, "missing-cost-rank", f"`{model.candidate}` has no cost_rank")
            elif not low <= rank <= high:
                self._add(
                    ERROR,
                    "cost-rank-off-ladder",
                    f"`{model.candidate}` cost_rank={rank} is outside the {low}..{high} ladder",
                    "extending the ladder rescales every `auto` pool norm, moving models you did not touch",
                )
            if not model.pricing:
                self._add(
                    WARN,
                    "missing-pricing",
                    f"`{model.candidate}` has no pricing",
                    "sessions on it record $0, so cost comparisons are blind",
                )
        self._check_rank_monotonicity()
        self._check_cache_ratios()
        self._check_published_pricing()

    def _check_rank_monotonicity(self) -> None:
        """Within an agent, a cheaper model must not carry a higher rank."""
        assert self.catalog is not None
        excepted = {
            (exception.candidate, exception.over)
            for exception in self.policy.rank_exceptions
            if exception.over
        }
        blanket = {
            exception.candidate for exception in self.policy.rank_exceptions if not exception.over
        }
        for agent in self.catalog.agents:
            priced = [
                (model, self.policy.blended_price(model.pricing))
                for model in agent.enabled_models
                if model.cost_rank is not None
            ]
            priced = [(model, price) for model, price in priced if price is not None]
            for cheaper, cheap_price in priced:
                for pricier, high_price in priced:
                    if cheap_price >= high_price:
                        continue
                    if cheaper.cost_rank <= pricier.cost_rank:  # type: ignore[operator]
                        continue
                    if (cheaper.candidate, pricier.candidate) in excepted or cheaper.candidate in blanket:
                        self._add(
                            INFO,
                            "rank-exception-applied",
                            f"`{cheaper.candidate}` outranks the pricier `{pricier.candidate}` "
                            "by documented exception",
                        )
                        continue
                    self._add(
                        ERROR,
                        "rank-price-inversion",
                        f"`{cheaper.candidate}` (rank {cheaper.cost_rank}, blended "
                        f"${cheap_price:.2f}/Mtok) outranks the pricier "
                        f"`{pricier.candidate}` (rank {pricier.cost_rank}, ${high_price:.2f})",
                        "either fix the rank or add a documented exception under "
                        "`cost.rank_exceptions` in data/model-policy.yaml",
                    )

    def _check_cache_ratios(self) -> None:
        assert self.catalog is not None
        for agent in self.catalog.agents:
            for model in agent.enabled_models:
                ratios = self.policy.cache_ratio_for(agent.name, model.candidate)
                if not ratios:
                    continue
                pricing = model.pricing or {}
                base = pricing.get("input_per_mtok")
                if base is None:
                    continue
                for kind, key in (("read", "cache_read_per_mtok"), ("write", "cache_write_per_mtok")):
                    if kind not in ratios or key not in pricing:
                        continue
                    want = round(float(base) * ratios[kind], 6)
                    got = float(pricing[key])
                    if abs(want - got) > 0.005:
                        self._add(
                            WARN,
                            "cache-rate-off-ratio",
                            f"`{model.candidate}` {key}={got} but its published "
                            f"{kind} rate is {ratios[kind]}× input (${want:.2f})",
                            "the cache-reprime break-even the demotion gate uses is computed "
                            "from these rates",
                        )

    def _check_published_pricing(self) -> None:
        assert self.catalog is not None
        reference = (self.policy.published_pricing or {}).get("models") or {}
        as_of = (self.policy.published_pricing or {}).get("as_of", "unknown")
        for candidate, rates in reference.items():
            model = self.catalog.model(candidate)
            if model is None or not model.enabled:
                continue
            pricing = model.pricing or {}
            for key, want in rates.items():
                got = pricing.get(key)
                if got is None:
                    self._add(
                        WARN,
                        "pricing-missing-rate",
                        f"`{candidate}` is missing `{key}` (published {as_of}: {want})",
                    )
                elif abs(float(got) - float(want)) > 0.0001:
                    self._add(
                        WARN,
                        "pricing-drift",
                        f"`{candidate}` {key}={got} but the reference rate ({as_of}) is {want}",
                        "either the provider changed price (update both) or the catalog is wrong",
                    )

    # -- entry point ------------------------------------------------------
    def run(self) -> list[Finding]:
        self.check_pattern_order()
        self.check_resolution()
        self.check_band()
        self.check_orderings()
        self.check_ceilings()
        self.check_benchmark_scores()
        self.check_catalog()
        return self.findings


def validate(policy: Policy, scores: ScoresFile, catalog: Catalog | None) -> list[Finding]:
    return Validator(policy, scores, catalog).run()


def errors(findings: Iterable[Finding]) -> list[Finding]:
    return [finding for finding in findings if finding.severity == ERROR]
