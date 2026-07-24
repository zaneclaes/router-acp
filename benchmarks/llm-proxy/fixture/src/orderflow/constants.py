"""Pricing constants shared by the orderflow modules."""

TIER_DISCOUNT_PERCENT = {
    "guest": 0,
    "member": 5,
    "vip": 10,
}

SHIPPING_BASE_CENTS = {
    "domestic": 500,
    "regional": 900,
    "international": 1800,
}

SHIPPING_PER_500G_CENTS = {
    "domestic": 125,
    "regional": 250,
    "international": 500,
}

TAX_BASIS_POINTS = {
    "domestic": 725,
    "regional": 500,
    "international": 0,
}
