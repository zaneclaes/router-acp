import unittest

from orderflow.discounts import compute_discount


class DiscountTests(unittest.TestCase):
    def test_tier_wins_over_smaller_coupon(self):
        self.assertEqual(compute_discount(12_345, "vip", 3), 1_235)

    def test_coupon_wins_over_tier(self):
        self.assertEqual(compute_discount(10_000, "member", 12), 1_200)

    def test_coupon_is_clamped(self):
        self.assertEqual(compute_discount(10_000, "guest", 80), 2_500)
        self.assertEqual(compute_discount(10_000, "guest", -5), 0)

    def test_unknown_tier_is_rejected(self):
        with self.assertRaises(ValueError):
            compute_discount(100, "platinum")

    def test_negative_subtotal_is_rejected(self):
        with self.assertRaises(ValueError):
            compute_discount(-1, "guest")


if __name__ == "__main__":
    unittest.main()
