"""Unit tests for Finance domain transforms and validators."""

from decimal import Decimal
import unittest
from dags.finance.shared.constants import PARTNER_ALPHA
from dags.finance.shared.transforms import to_decimal, calculate_line_item
from dags.finance.shared.validators import validate_single_record, verify_batch_invariants


class TestFinanceTransformsAndValidators(unittest.TestCase):
    def test_to_decimal(self):
        d1 = to_decimal("150000.555")
        self.assertEqual(d1, Decimal("150000.56"))

        d2 = to_decimal(100)
        self.assertEqual(d2, Decimal("100.00"))

        d3 = to_decimal(100.556)
        self.assertEqual(d3, Decimal("100.56"))

    def test_calculate_line_item(self):
        res = calculate_line_item("100000.00", fee_rate="0.02", tax_rate="0.10")
        self.assertEqual(res["base_amount"], Decimal("100000.00"))
        self.assertEqual(res["fee_amount"], Decimal("2000.00"))
        self.assertEqual(res["tax_amount"], Decimal("10000.00"))
        self.assertEqual(res["total_billed"], Decimal("112000.00"))
        self.assertEqual(res["net_payout"], Decimal("98000.00"))

    def test_validate_single_record_valid(self):
        record = {
            "id": "TXN_001",
            "partner_code": PARTNER_ALPHA,
            "amount": "150000.00",
            "transaction_date": "2026-11-16",
        }
        is_valid, reason = validate_single_record(record)
        self.assertTrue(is_valid)
        self.assertEqual(reason, "")

    def test_validate_single_record_invalid(self):
        # Missing required field
        is_valid, reason = validate_single_record({"id": "TXN_002"})
        self.assertFalse(is_valid)
        self.assertIn("Missing required field", reason)

        # Unsupported partner
        is_valid, reason = validate_single_record({
            "id": "TXN_003",
            "partner_code": "UNKNOWN_PARTNER",
            "amount": "50000.00",
            "transaction_date": "2026-11-16",
        })
        self.assertFalse(is_valid)
        self.assertIn("Unsupported partner", reason)

        # Amount below minimum
        is_valid, reason = validate_single_record({
            "id": "TXN_004",
            "partner_code": PARTNER_ALPHA,
            "amount": "0.50",
            "transaction_date": "2026-11-16",
        })
        self.assertFalse(is_valid)
        self.assertIn("below minimum threshold", reason)

    def test_verify_batch_invariants(self):
        records = [
            {"total_billed": Decimal("112000.00")},
            {"total_billed": Decimal("56000.00")},
        ]
        declared_total = Decimal("168000.00")
        declared_count = 2

        # Valid batch
        is_valid, reason = verify_batch_invariants(records, declared_total, declared_count)
        self.assertTrue(is_valid)

        # Count mismatch
        is_valid, reason = verify_batch_invariants(records, declared_total, 3)
        self.assertFalse(is_valid)
        self.assertIn("Record count mismatch", reason)

        # Financial sum mismatch
        is_valid, reason = verify_batch_invariants(records, Decimal("200000.00"), declared_count)
        self.assertFalse(is_valid)
        self.assertIn("Financial sum mismatch", reason)


if __name__ == "__main__":
    unittest.main()
