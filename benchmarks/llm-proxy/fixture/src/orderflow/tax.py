"""Tax calculation deliberately kept outside the maintenance task."""

from .constants import TAX_BASIS_POINTS


def tax_cents(taxable_cents: int, zone: str) -> int:
    if taxable_cents < 0:
        raise ValueError("taxable_cents must be nonnegative")
    basis_points = TAX_BASIS_POINTS[zone]
    return (taxable_cents * basis_points + 5_000) // 10_000
