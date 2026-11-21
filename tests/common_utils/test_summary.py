"""Unit tests for RunSummary reporter."""

import unittest
from dags.common_utils.summary import RunSummary


class TestRunSummary(unittest.TestCase):
    def test_run_summary_tracking(self):
        summary = RunSummary(dag_id="test_dag", run_id="manual__1")
        summary.register_total(10)
        summary.add_success(8)
        summary.add_skip("REC_009", "Already processed")
        summary.add_failure("REC_010", "Missing VAT rate", stage="gate1_validate")

        self.assertEqual(summary.total_records, 10)
        self.assertEqual(summary.success_count, 8)
        self.assertEqual(summary.skip_count, 1)
        self.assertEqual(summary.failure_count(), 1)
        self.assertTrue(summary.has_failures())

        report = summary.format_report()
        self.assertIn("Total Processed : 10", report)
        self.assertIn("Success         : 8", report)
        self.assertIn("Failed          : 1", report)
        self.assertIn("REC_010", report)
        self.assertIn("Missing VAT rate", report)

    def test_run_summary_clean(self):
        summary = RunSummary(dag_id="test_clean_dag")
        summary.register_total(5)
        summary.add_success(5)
        self.assertFalse(summary.has_failures())
        self.assertEqual(summary.failure_count(), 0)


if __name__ == "__main__":
    unittest.main()
