"""Unit and integration tests for Partner Billing ETL tasks."""

import unittest
from unittest.mock import MagicMock, patch

from tests import DagTestBase
from airflow.exceptions import AirflowFailException  # type: ignore

TASKS = "dags.finance.invoicing.partner_billing_etl.tasks"


# docker exec -e AIRFLOW_ENV=test -it airflow-webserver pytest -s -vvv tests/finance/invoicing/partner_billing_etl/test_tasks.py
class TestPartnerBillingTasks(DagTestBase):
    def setUp(self):
        self.dag = self.get_dag("finance_process_partner_billing")

    @patch(f"{TASKS}.utils.get_db_hook")
    @patch(f"{TASKS}.utils.get_partner_client")
    def test_end_to_end_success_pipeline(self, mock_partner_client, mock_db_hook):
        # 1. Setup mock DB and API client
        mock_db = MagicMock()
        mock_db.fetch_all.return_value = [
            {
                "id": "TXN_001",
                "partner_code": "ALPHA_LOGISTICS",
                "transaction_date": "2026-11-16",
                "amount": "150000.00",
                "currency": "VND",
                "customer_ref": "CUST_1",
                "status": "PENDING",
            },
            {
                "id": "TXN_002",
                "partner_code": "ALPHA_LOGISTICS",
                "transaction_date": "2026-11-16",
                "amount": "250000.00",
                "currency": "VND",
                "customer_ref": "CUST_2",
                "status": "PENDING",
            },
        ]
        mock_db_hook.return_value = mock_db

        client = MagicMock()
        client.submit_invoices_batch.return_value = {"batch_id": "BATCH_1", "status": "ACCEPTED"}
        mock_partner_client.return_value = client

        # 2. Execute pipeline within a fresh DAG run
        with self.new_dag_run(conf={"partner_code": "ALPHA_LOGISTICS", "dry_run": False}):
            # Setup
            setup_res = self.assert_execute_task_successfully("setup")
            self.assertEqual(setup_res["partner_code"], "ALPHA_LOGISTICS")

            # Extract
            extract_res = self.assert_execute_task_successfully("extract")
            self.assertEqual(extract_res["total_extracted"], 2)

            # Gate 1 Validate
            val_res = self.assert_execute_task_successfully("gate1_validate")
            self.assertEqual(val_res["valid_count"], 2)

            # Gate 2 Transform
            transform_res = self.assert_execute_task_successfully("gate2_transform")
            self.assertEqual(transform_res["declared_record_count"], 2)

            # Gate 3 Verify Invariants
            verify_res = self.assert_execute_task_successfully("gate3_verify")
            self.assertTrue(verify_res["verified"])

            # Deliver
            deliver_res = self.assert_execute_task_successfully("deliver")
            self.assertEqual(deliver_res["status"], "DELIVERED")
            self.assertEqual(deliver_res["count"], 2)

            # Finalize
            self.assert_execute_task_successfully("finalize")

        # Verify external calls
        client.submit_invoices_batch.assert_called_once()
        mock_db.execute.assert_called_once()

    @patch(f"{TASKS}.utils.get_db_hook")
    @patch(f"{TASKS}.utils.get_partner_client")
    def test_record_failure_isolation_in_gate1(self, mock_partner_client, mock_db_hook):
        mock_db = MagicMock()
        # 1 valid record, 1 invalid record (missing partner_code)
        mock_db.fetch_all.return_value = [
            {
                "id": "TXN_VALID",
                "partner_code": "ALPHA_LOGISTICS",
                "transaction_date": "2026-11-16",
                "amount": "100000.00",
                "currency": "VND",
                "customer_ref": "CUST_OK",
                "status": "PENDING",
            },
            {
                "id": "TXN_INVALID",
                "partner_code": "",  # Invalid!
                "transaction_date": "2026-11-16",
                "amount": "50000.00",
                "currency": "VND",
                "customer_ref": "CUST_BAD",
                "status": "PENDING",
            },
        ]
        mock_db_hook.return_value = mock_db

        client = MagicMock()
        client.submit_invoices_batch.return_value = {"status": "ACCEPTED"}
        mock_partner_client.return_value = client

        with self.new_dag_run(conf={"partner_code": "ALPHA_LOGISTICS"}):
            self.assert_execute_task_successfully("setup")
            self.assert_execute_task_successfully("extract")

            # Gate 1 isolates failure: valid count is 1, failure is recorded
            val_res = self.assert_execute_task_successfully("gate1_validate")
            self.assertEqual(val_res["valid_count"], 1)

            # Gate 1 isolates the bad row while preserving the valid downstream flow.
            self.assert_execute_task_successfully("gate2_transform")

    @patch(f"{TASKS}.utils.get_db_hook")
    def test_gate3_invariant_breach_triggers_circuit_breaker(self, mock_db_hook):
        mock_db = MagicMock()
        mock_db.fetch_all.return_value = [
            {
                "id": "TXN_CORRUPT",
                "partner_code": "ALPHA_LOGISTICS",
                "transaction_date": "2026-11-16",
                "amount": "100000.00",
                "currency": "VND",
                "customer_ref": "CUST_X",
                "status": "PENDING",
            }
        ]
        mock_db_hook.return_value = mock_db

        with self.new_dag_run():
            self.assert_execute_task_successfully("setup")
            self.assert_execute_task_successfully("extract")
            self.assert_execute_task_successfully("gate1_validate")
            transform_res = self.assert_execute_task_successfully("gate2_transform")

            # Corrupt declared total to trigger Gate 3 breach
            corrupted_transform = dict(transform_res)
            corrupted_transform["declared_total_amount"] = "999999999.00"
            self.modify_task_return_value("gate2_transform", corrupted_transform)

            # Gate 3 must trip circuit breaker and raise AirflowFailException
            self.assert_execute_task_should_raise_exception("gate3_verify", AirflowFailException)


if __name__ == "__main__":
    unittest.main()
