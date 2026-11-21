"""Unit tests for Partner Billing ETL DAG structure, dependencies, and doc_md."""

from tests import DagTestBase
from dags.finance.invoicing.partner_billing_etl.dag import dag, dag_id


# docker exec -e AIRFLOW_ENV=test -it airflow-webserver pytest -s -vvv tests/finance/invoicing/partner_billing_etl/test_dag_structure.py
class TestPartnerBillingDagStructure(DagTestBase):
    def setUp(self):
        self.dag = dag

    def test_dag_properties(self):
        self.assertEqual(self.dag.dag_id, dag_id)
        # Verify schedule_interval is None (Section 1 & 7.1)
        self.assertIsNone(self.dag.schedule_interval)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_task_presence_and_wiring(self):
        expected_tasks = [
            "setup",
            "extract",
            "gate1_validate",
            "gate2_transform",
            "gate3_verify",
            "deliver",
            "finalize",
        ]
        for task_id in expected_tasks:
            self.assert_has_task_id(self.dag, task_id)

        # Check linear dependency order
        setup_task = self.dag.get_task("setup")
        extract_task = self.dag.get_task("extract")
        val_task = self.dag.get_task("gate1_validate")
        transform_task = self.dag.get_task("gate2_transform")
        verify_task = self.dag.get_task("gate3_verify")
        deliver_task = self.dag.get_task("deliver")
        finalize_task = self.dag.get_task("finalize")

        self.assertIn(extract_task, setup_task.downstream_list)
        self.assertIn(val_task, extract_task.downstream_list)
        self.assertIn(transform_task, val_task.downstream_list)
        self.assertIn(verify_task, transform_task.downstream_list)
        self.assertIn(deliver_task, verify_task.downstream_list)
        self.assertIn(finalize_task, deliver_task.downstream_list)

    def test_dag_doc_md(self):
        self.assertIsNotNone(self.dag.doc_md)
        self.assertIn("finance_process_partner_billing", self.dag.doc_md)
        self.assertIn("curl", self.dag.doc_md)
        self.assertIn("partner_code", self.dag.doc_md)


if __name__ == "__main__":
    import unittest
    unittest.main()
