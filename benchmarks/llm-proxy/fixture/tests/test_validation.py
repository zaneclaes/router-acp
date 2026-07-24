import unittest

from orderflow.models import LineItem, Order
from orderflow.validation import validate_order


class ValidationTests(unittest.TestCase):
    def test_valid_order_has_no_errors(self):
        order = Order(
            order_id=" A-100 ",
            email="buyer@example.com",
            zone="domestic",
            tier="member",
            coupon_percent=12,
            items=[LineItem("sku-1", 2, 500, 250)],
        )
        self.assertEqual(validate_order(order), [])

    def test_order_and_item_errors_are_sorted(self):
        order = Order(
            order_id=" ",
            email="invalid",
            zone="moon",
            tier="platinum",
            coupon_percent=30,
            items=[
                LineItem("---", 0, -1, -5),
                LineItem("ok", 1, 1, 0),
            ],
        )
        self.assertEqual(
            validate_order(order),
            [
                "coupon_percent",
                "email",
                "item[0].price",
                "item[0].quantity",
                "item[0].sku",
                "item[0].weight",
                "order_id",
                "tier",
                "zone",
            ],
        )

    def test_items_are_required(self):
        order = Order("A", "a@example.com", "domestic")
        self.assertEqual(validate_order(order), ["items"])


if __name__ == "__main__":
    unittest.main()
