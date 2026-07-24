#!/usr/bin/env python3
"""Independent deterministic checks not visible inside benchmark worktrees."""

from __future__ import annotations

import random
import sys
from pathlib import Path


def main() -> int:
    repo = Path(sys.argv[1]).resolve()
    sys.path.insert(0, str(repo / "src"))

    from orderflow.discounts import compute_discount
    from orderflow.models import LineItem, Order
    from orderflow.normalize import normalize_email, normalize_sku
    from orderflow.shipping import shipping_cost
    from orderflow.summary import summarize_order
    from orderflow.validation import validate_order

    rng = random.Random(7013)
    tiers = {"guest": 0, "member": 5, "vip": 10}
    for _ in range(250):
        subtotal = rng.randrange(0, 2_000_000)
        tier = rng.choice(sorted(tiers))
        coupon = rng.randrange(-30, 60)
        percent = max(tiers[tier], min(25, max(0, coupon)))
        expected = (subtotal * percent + 50) // 100
        assert compute_discount(subtotal, tier, coupon) == expected

    zone_rates = {
        "domestic": (500, 125),
        "regional": (900, 250),
        "international": (1800, 500),
    }
    for _ in range(250):
        subtotal = rng.randrange(0, 30_000)
        weight = rng.randrange(0, 20_000)
        zone = rng.choice(sorted(zone_rates))
        base, per_block = zone_rates[zone]
        blocks = (weight + 499) // 500
        expected = 0 if zone == "domestic" and subtotal >= 10_000 else base + blocks * per_block
        assert shipping_cost(subtotal, weight, zone) == expected

    sku_cases = {
        "  a..b///c  ": "A-B-C",
        "mixed_Case 99": "MIXED-CASE-99",
        "A---B": "A-B",
    }
    for raw, expected in sku_cases.items():
        assert normalize_sku(raw) == expected
    assert normalize_email("  X.Y+tag@Example.Co.UK ") == "x.y+tag@example.co.uk"

    invalid = Order(
        "",
        "bad",
        "unknown",
        "unknown",
        99,
        [LineItem(" ", -1, 0, -1)],
    )
    assert validate_order(invalid) == sorted(validate_order(invalid))
    assert len(validate_order(invalid)) == 9

    order = Order(
        "gate-1",
        "gate@example.com",
        "regional",
        "vip",
        7,
        [LineItem("a 1", 3, 333, 201), LineItem("b", 2, 1_001, 400)],
    )
    assert summarize_order(order) == {
        "order_id": "GATE-1",
        "email": "gate@example.com",
        "zone": "regional",
        "tier": "vip",
        "item_count": 5,
        "skus": ["A-1", "B"],
        "subtotal_cents": 3_001,
        "discount_cents": 300,
        "shipping_cents": 1_650,
        "tax_cents": 135,
        "total_cents": 4_486,
    }
    print("QUALITY_GATE_OK: 508 property and integration checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
