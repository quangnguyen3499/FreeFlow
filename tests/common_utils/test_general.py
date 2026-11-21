"""Unit tests for general utilities."""

import unittest
from dags.common_utils.general import (
    pretty,
    batch,
    dig,
    flatten,
    compact,
    camel_case,
    md5,
)


class TestGeneralUtils(unittest.TestCase):
    def test_pretty(self):
        data = {"key": "value", "count": 10}
        formatted = pretty(data)
        self.assertIn('"key": "value"', formatted)
        self.assertIn('"count": 10', formatted)

    def test_batch(self):
        items = list(range(10))
        batches = list(batch(items, 3))
        self.assertEqual(len(batches), 4)
        self.assertEqual(batches[0], [0, 1, 2])
        self.assertEqual(batches[-1], [9])

    def test_batch_invalid(self):
        with self.assertRaises(ValueError):
            list(batch([1, 2], 0))

    def test_dig(self):
        nested = {"a": {"b": {"c": 42}}}
        self.assertEqual(dig(nested, "a", "b", "c"), 42)
        self.assertIsNone(dig(nested, "a", "nonexistent"))
        self.assertEqual(dig(nested, "a", "x", default="fallback"), "fallback")

    def test_flatten(self):
        nested = [1, [2, [3, 4], 5], 6]
        self.assertEqual(flatten(nested), [1, 2, 3, 4, 5, 6])

    def test_compact(self):
        raw = [0, 1, False, None, "", "hello", [], [1]]
        # None, "", [] are filtered, but 0 and False remain
        self.assertEqual(compact(raw), [0, 1, False, "hello", [1]])

    def test_camel_case(self):
        self.assertEqual(camel_case("partner_billing_etl"), "partnerBillingEtl")
        self.assertEqual(camel_case("alpha-logistics"), "alphaLogistics")

    def test_md5(self):
        h1 = md5("test_string")
        h2 = md5(b"test_string")
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 32)


if __name__ == "__main__":
    unittest.main()
