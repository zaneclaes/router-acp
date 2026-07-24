"""Public data structures."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LineItem:
    sku: str
    quantity: int
    unit_price_cents: int
    weight_grams: int


@dataclass(frozen=True)
class Order:
    order_id: str
    email: str
    zone: str
    tier: str = "guest"
    coupon_percent: int = 0
    items: list[LineItem] = field(default_factory=list)
