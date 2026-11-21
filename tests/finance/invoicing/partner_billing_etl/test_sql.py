"""Unit tests for SQL query generator functions."""

import unittest
from dags.finance.invoicing.partner_billing_etl.sql import (
    get_unbilled_transactions_query,
    get_update_transactions_status_query,
    get_reconciliation_audit_query,
)


class TestPartnerBillingSql(unittest.TestCase):
    def test_unbilled_query_structure(self):
        query = get_unbilled_transactions_query()
        self.assertIn("WITH filtered_transactions AS", query)
        self.assertIn("partner_transactions", query)
        self.assertIn(":partner_code", query)
        self.assertIn(":start_date", query)
        self.assertIn(":end_date", query)
        self.assertIn(":limit", query)
        self.assertIn("PENDING", query)

    def test_update_status_query(self):
        placeholders = "'TXN_1', 'TXN_2'"
        query = get_update_transactions_status_query(placeholders)
        self.assertIn("UPDATE partner_transactions", query)
        self.assertIn(":new_status", query)
        self.assertIn(":batch_id", query)
        self.assertIn("IN ('TXN_1', 'TXN_2')", query)

    def test_reconciliation_audit_query(self):
        query = get_reconciliation_audit_query()
        self.assertIn("SELECT", query)
        self.assertIn("WHERE status IN ('PROCESSING', 'VALIDATED')", query)
        self.assertIn(":threshold_timestamp", query)


if __name__ == "__main__":
    unittest.main()
