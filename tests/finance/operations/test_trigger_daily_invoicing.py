"""Unit tests for config-driven trigger DAG operations."""

import unittest
from tests import DagTestBase
from dags.finance.operations.trigger_daily_invoicing.dag import trigger_dag_obj


# docker exec -e AIRFLOW_ENV=test -it airflow-webserver pytest -s -vvv tests/finance/operations/test_trigger_daily_invoicing.py
class TestTriggerDailyInvoicing(DagTestBase):
    def setUp(self):
        self.dag = trigger_dag_obj

    def test_trigger_dag_structure(self):
        self.assertEqual(self.dag.dag_id, "finance_trigger_daily_partner_invoicing")
        self.assertIsNotNone(self.dag.schedule_interval)
        self.assertEqual(self.dag.schedule_interval, "0 8 * * 1-5")

        expected_tasks = [
            "check_business_day",
            "skip_trigger",
            "setup_triggers",
            "trigger_finance_process_partner_billing",
        ]
        for task_id in expected_tasks:
            self.assert_has_task_id(self.dag, task_id)


if __name__ == "__main__":
    unittest.main()
