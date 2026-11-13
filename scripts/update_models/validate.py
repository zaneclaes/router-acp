"""The contract checks over the model catalog, `data/providers/<name>.yaml`.

The contract: absolute quality numbers may drift, ORDERING may not, and every
entry is one exact, fully described model scored only on its own evidence.
Concretely:

  * shape — `models:` maps each family to its versions; every entry has
    version/id/display_name/cost_rank/pricing; `version` is a dotted number;
    no `*` anywhere a model is named; no id twice in a provider; no
    `api_model` (an `id` is the provider's real API id, never an alias), no
    retired `scored_as` / `compressed_behind`, no identity `effort_mapping`
    entry (only renames are allowed); `discovery.retire_grace_releases` lives
    in the provider's `discovery`;
  * references — `outranks` names existing models (a bare id is the same
    provider, `provider/id` crosses providers);
  * evidence — a model with no `evidence` of its own is UNSCORED: it must
    carry no quality, and the run lists every such model as a warning;
  * quality — inside the band, and equal to what the model's evidence computes
    (`benchmark-score-drift`), so a hand edit cannot bypass the procedure;
  * ordering — each model's `outranks` claims hold on every task class;
  * cost — ranks on the ladder and weakly monotonic in blended price within a
    provider unless the cheaper model states a `rank_exception`; stated prices
    and cache rates are positive (an unset cache rate takes the provider's
    `cache_ratio`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from . import benchmarks
from .catalog import REQUIRED_FIELDS, Catalog
from .policy import TASK_CLASSES, Policy, PolicyError

ERROR = "error"
WARN = "warn"
INFO = "info"

_RETIRED_FIELDS = {
    "scored_as": "every model is scored only on its own `evidence`",
    "compressed_behind": "no model's quality is derived from another's",
    "api_model": "`id` is the provider's real API model id; there are no aliases",
    "family": "the family is the group key under `models:`",
    "provider": "the provider is the file",
}
_RETIRED_TOP_LEVEL = {
    "discovery_retire_grace_releases": "it is `discovery.retire_grace_releases`",
}
_PRICE_KEYS = ("input_per_mtok", "output_per_mtok")
_CACHE_KEYS = ("cache_read_per_mtok", "cache_write_per_mtok")


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    message: str
    detail: str = ""

    def __str__(self) -> str:
        head = f"[{self.severity.upper()}] {self.code}: {self.message}"
        return f"{head}\n    {self.detail}" if self.detail else head


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class Validator:
    def __init__(self, models: Catalog, policy: Policy | None = None):
        self.models = models
        self.policy = policy or models.policy
        self.findings: list[Finding] = []
        self.scored: benchmarks.Scored | None = None

    def _add(self, severity: str, code: str, message: str, detail: str = "") -> None:
        self.findings.append(Finding(severity, code, message, detail))

    # -- shape ------------------------------------------------------------
    def check_shape(self) -> None:
        for provider_file in self.models.files:
            name = provider_file.path.name
            if not provider_file.raw.get("provider"):
                self._add(ERROR, "provider-missing", f"{name} has no top-level `provider:`")
            elif provider_file.provider != provider_file.path.stem:
                self._add(
                    ERROR,
                    "provider-mismatch",
                    f"{name} says `provider: {provider_file.provider}`; the file must be named after it",
                )
            for key, why in _RETIRED_TOP_LEVEL.items():
                if key in provider_file.raw:
                    self._add(ERROR, "retired-section", f"{name} has a top-level `{key}:`", why)

        seen: dict[tuple[str, str], str] = {}
        releases: dict[tuple[str, str, tuple[int, ...]], str] = {}
        for index, row in enumerate(self.models.rows):
            label = row.key if row.model_id else f"{row.provider} entry {index + 1}"
            for name in REQUIRED_FIELDS:
                if row.fields.get(name) in (None, "", {}):
                    self._add(ERROR, "model-missing-field", f"`{label}` has no `{name}`")
            if row.fields.get("version") not in (None, "") and row.version is None:
                self._add(
                    ERROR,
                    "bad-version",
                    f"`{label}` version `{row.version_text}` is not a dotted number (e.g. 5.5, 4.10)",
                )
            if row.version is not None:
                release = (row.provider, row.family, row.version)
                if release in releases:
                    self._add(
                        WARN,
                        "duplicate-release",
                        f"`{releases[release]}` and `{label}` are both {row.provider} "
                        f"{row.family} {row.version_text}",
                    )
                releases.setdefault(release, label)
            for name, why in _RETIRED_FIELDS.items():
                if name in row.fields:
                    self._add(ERROR, "retired-field", f"`{label}` sets `{name}`, which no longer exists", why)
            for name, value in (("id", row.model_id), ("family", row.family), *(("outranks", ref) for ref in row.outranks)):
                if "*" in value:
                    self._add(
                        ERROR,
                        "wildcard-key",
                        f"`{label}` {name} `{value}` contains `*`",
                        "every entry and every reference names one exact model; there are no patterns",
                    )
            mapping = row.fields.get("effort_mapping") or {}
            if isinstance(mapping, dict):
                identity = [str(key) for key, value in mapping.items() if str(key) == str(value)]
                if identity:
                    self._add(
                        ERROR,
                        "identity-effort-mapping",
                        f"`{label}` effort_mapping maps {', '.join(identity)} to itself",
                        "effort_mapping only renames a level; the router refuses identity entries",
                    )
            if row.model_id:
                owner = seen.get((row.provider, row.model_id.lower()))
                if owner is not None:
                    self._add(
                        ERROR,
                        "duplicate-model",
                        f"`{row.model_id}` is listed twice in {row.provider} ({owner} and {label})",
                        "a model is found by its exact id, so each must be unique in its provider",
                    )
                seen.setdefault((row.provider, row.model_id.lower()), label)

    # -- references -------------------------------------------------------
    def check_references(self) -> None:
        for row in self.models.rows:
            for ref in row.outranks:
                if self.models.resolve_ref(ref, row.provider) is None:
                    self._add(
                        ERROR,
                        "unknown-reference",
                        f"`{row.key}` outranks `{ref}`, which names no model",
                        "a bare id means the same provider; write `provider/id` across providers",
                    )

    # -- evidence ---------------------------------------------------------
    def check_evidence(self) -> None:
        try:
            self.scored = benchmarks.score_rows(self.policy, self.models)
        except PolicyError as exc:
            self._add(ERROR, "bad-evidence", str(exc))
            return
        unscored = list(self.scored.unscored)
        for key in unscored:
            row = self.models.resolve_ref(key, "")
            if row is not None and row.scored:
                self._add(
                    ERROR,
                    "quality-without-evidence",
                    f"`{key}` carries quality but has no `evidence` of its own",
                    "a model is scored only on its own evidence, never a sibling's or an earlier "
                    "version's; run `scripts/update_models.py --apply` to remove the copied numbers",
                )
        if unscored:
            self._add(
                WARN,
                "unscored-models",
                f"{len(unscored)} model(s) have no evidence of their own and are unscored: "
                + ", ".join(f"`{key}`" for key in unscored),
                "an unscored model is chosen only by an explicit pick; record its published "
                "results under `evidence:` to score it",
            )

    # -- quality ----------------------------------------------------------
    def check_band(self) -> None:
        low, high = self.policy.quality_band
        for row in self.models.rows:
            if not row.scored:
                continue
            for task_class, value in row.qualities().items():
                if not low <= value <= high:
                    self._add(
                        ERROR,
                        "out-of-band",
                        f"`{row.key}` {task_class}={value:.2f} is outside the band [{low:.2f}, {high:.2f}]",
                    )

    def check_benchmark_scores(self) -> None:
        """Checked-in quality must equal the deterministic benchmark result."""
        if self.scored is None:
            return
        for row in self.models.rows:
            expected = self.scored.rows.get(row.key)
            if expected is None:
                continue
            if (
                "quality" not in row.fields
                or abs(float(row.fields.get("default_quality", 0.5)) - expected.default_quality) > 1e-9
                or row.qualities() != expected.quality
            ):
                self._add(
                    ERROR,
                    "benchmark-score-drift",
                    f"`{row.key}` does not match the score its own evidence computes "
                    f"({expected.observations} observation(s))",
                    f"run `scripts/update_models.py --apply` to write base {expected.default_quality:.2f}",
                )

    def check_outranks(self) -> None:
        for row in self.models.rows:
            for ref in row.outranks:
                other = self.models.resolve_ref(ref, row.provider)
                if other is None:
                    continue  # reported by check_references
                if not row.scored or not other.scored:
                    unscored = row.key if not row.scored else other.key
                    self._add(
                        WARN,
                        "order-unchecked",
                        f"`{row.key}` outranks `{other.key}` cannot be checked: `{unscored}` is unscored",
                    )
                    continue
                upper, lower = row.qualities(), other.qualities()
                broken = [
                    f"{task_class} {upper[task_class]:.2f} vs {lower[task_class]:.2f}"
                    for task_class in TASK_CLASSES
                    if upper[task_class] <= lower[task_class]
                ]
                if broken:
                    self._add(
                        ERROR,
                        "order-broken",
                        f"`{row.key}` must score strictly above `{other.key}` on every task class",
                        f"offending classes: {', '.join(broken)}",
                    )

    # -- cost -------------------------------------------------------------
    def check_cost(self) -> None:
        low, high = self.policy.ladder
        for row in self.models.rows:
            rank = row.cost_rank
            if rank is None:
                if row.fields.get("cost_rank") is not None:
                    self._add(ERROR, "bad-cost-rank", f"`{row.key}` cost_rank is not an integer")
            elif not low <= rank <= high:
                self._add(
                    ERROR,
                    "cost-rank-off-ladder",
                    f"`{row.key}` cost_rank={rank} is outside the {low}..{high} ladder",
                    "extending the ladder rescales every `auto` pool norm, moving models you did not touch",
                )
            pricing = row.pricing
            if pricing is None:
                continue
            for key in (*_PRICE_KEYS, *(key for key in _CACHE_KEYS if key in pricing)):
                value = _number(pricing.get(key))
                if value is None or value <= 0:
                    self._add(
                        ERROR,
                        "bad-pricing",
                        f"`{row.key}` pricing.{key} must be a positive number (got {pricing.get(key)!r})",
                        "an unset cache rate takes the provider's `cache_ratio` × input"
                        if key in _CACHE_KEYS
                        else "",
                    )
        self._check_rank_monotonicity()

    def _check_rank_monotonicity(self) -> None:
        """Within a provider, a cheaper model must not carry a higher rank."""
        for provider in self.models.providers():
            priced = [
                (row, self.policy.blended_price(row.pricing))
                for row in self.models.by_provider(provider)
                if row.cost_rank is not None
            ]
            priced = [(row, price) for row, price in priced if price is not None]
            for cheaper, cheap_price in priced:
                for pricier, high_price in priced:
                    if cheap_price >= high_price or cheaper.cost_rank <= pricier.cost_rank:  # type: ignore[operator]
                        continue
                    if cheaper.rank_exception:
                        self._add(
                            INFO,
                            "rank-exception-applied",
                            f"`{cheaper.key}` outranks the pricier `{pricier.key}` by its rank_exception",
                        )
                        continue
                    self._add(
                        ERROR,
                        "rank-price-inversion",
                        f"`{cheaper.key}` (rank {cheaper.cost_rank}, blended ${cheap_price:.2f}/Mtok) "
                        f"outranks the pricier `{pricier.key}` (rank {pricier.cost_rank}, ${high_price:.2f})",
                        "either fix the rank or state why on the entry: `rank_exception: <reason>`",
                    )

    # -- entry point ------------------------------------------------------
    def run(self) -> list[Finding]:
        self.check_shape()
        self.check_references()
        self.check_evidence()
        self.check_band()
        self.check_benchmark_scores()
        self.check_outranks()
        self.check_cost()
        return self.findings


def validate(models: Catalog) -> list[Finding]:
    try:
        policy = models.policy
    except PolicyError as exc:
        return [Finding(ERROR, "bad-scoring", str(exc))]
    return Validator(models, policy).run()


def errors(findings: Iterable[Finding]) -> list[Finding]:
    return [finding for finding in findings if finding.severity == ERROR]
