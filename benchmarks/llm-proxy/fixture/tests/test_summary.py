import unittest

from orderflow.models import LineItem, Order
from orderflow.summary import summarize_order


class SummaryTests(unittest.TestCase):
    def test_summary_is_deterministic(self):
        order = Order(
            order_id="  ord-9 ",
            email=" Buyer@Example.COM ",
            zone="domestic",
            tier="member",
            coupon_percent=12,
            items=[
                LineItem("abc / 1", 2, 2_500, 250),
                LineItem("xyz", 1, 5_000, 750),
            ],
        )
        self.assertEqual(
            summarize_order(order),
            {
                "order_id": "ORD-9",
                "email": "buyer@example.com",
                "zone": "domestic",
                "tier": "member",
                "item_count": 3,
                "skus": ["ABC-1", "XYZ"],
                "subtotal_cents": 10_000,
                "discount_cents": 1_200,
                "shipping_cents": 875,
                "tax_cents": 638,
                "total_cents": 10_313,
            },
        )

    def test_invalid_order_is_rejected_with_codes(self):
        with self.assertRaisesRegex(ValueError, r"email, items"):
            summarize_order(Order("A", "bad", "domestic"))


if __name__ == "__main__":
    unittest.main()
