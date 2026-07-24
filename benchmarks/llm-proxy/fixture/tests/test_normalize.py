import unittest

from orderflow.normalize import normalize_email, normalize_sku


class NormalizeTests(unittest.TestCase):
    def test_sku_canonicalizes_separators(self):
        self.assertEqual(normalize_sku("  ab / 12__blue  "), "AB-12-BLUE")

    def test_sku_preserves_alphanumerics(self):
        self.assertEqual(normalize_sku("x9"), "X9")

    def test_sku_rejects_empty_result(self):
        with self.assertRaises(ValueError):
            normalize_sku(" -- ")

    def test_email_normalizes(self):
        self.assertEqual(normalize_email(" Buyer@Example.COM "), "buyer@example.com")

    def test_email_rejects_invalid_shapes(self):
        for value in ("missing", "@example.com", "a@", "a@b", "a@@b.com"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_email(value)


if __name__ == "__main__":
    unittest.main()
