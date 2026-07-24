import unittest

from orderflow.shipping import shipping_cost


class ShippingTests(unittest.TestCase):
    def test_started_weight_blocks(self):
        self.assertEqual(shipping_cost(2_000, 1, "domestic"), 625)
        self.assertEqual(shipping_cost(2_000, 500, "domestic"), 625)
        self.assertEqual(shipping_cost(2_000, 501, "domestic"), 750)

    def test_zero_weight_has_no_weight_charge(self):
        self.assertEqual(shipping_cost(2_000, 0, "regional"), 900)

    def test_domestic_free_shipping_threshold(self):
        self.assertEqual(shipping_cost(10_000, 9_000, "domestic"), 0)
        self.assertNotEqual(shipping_cost(9_999, 9_000, "domestic"), 0)

    def test_international_rates(self):
        self.assertEqual(shipping_cost(2_000, 750, "international"), 2_800)

    def test_bad_inputs_are_rejected(self):
        for args in ((-1, 0, "domestic"), (0, -1, "domestic"), (0, 0, "moon")):
            with self.subTest(args=args), self.assertRaises(ValueError):
                shipping_cost(*args)


if __name__ == "__main__":
    unittest.main()
