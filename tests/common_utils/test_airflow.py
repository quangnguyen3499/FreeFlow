"""Unit tests for Airflow utility helpers."""

import unittest
from unittest.mock import MagicMock
from dags.common_utils.airflow import (
    env,
    fetch_variable,
    fetch_variable_as_json,
    save_variable_as_json,
    get_conf,
    task_return_value,
    disable_dag,
    enable_dag,
    is_dag_disabled,
)


class TestAirflowUtils(unittest.TestCase):
    def test_env(self):
        self.assertIn(env(), ["development", "test", "staging", "production"])

    def test_fetch_variable_with_default(self):
        val = fetch_variable("non_existent_key_12345", default="fallback_val")
        self.assertEqual(val, "fallback_val")

    def test_save_and_fetch_variable_as_json(self):
        data = {"partner": "ALPHA", "limit": 100}
        save_variable_as_json("test_var_json", data)
        retrieved = fetch_variable_as_json("test_var_json")
        self.assertEqual(retrieved, data)

    def test_get_conf(self):
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {"target_date": "2026-11-16", "dry_run": True}
        context = {"dag_run": mock_dag_run}
        conf = get_conf(context)
        self.assertEqual(conf["target_date"], "2026-11-16")
        self.assertTrue(conf["dry_run"])

        # Empty context
        self.assertEqual(get_conf({}), {})

    def test_task_return_value(self):
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {"status": "SUCCESS"}
        context = {"task_instance": mock_ti}
        val = task_return_value(context, "setup")
        self.assertEqual(val, {"status": "SUCCESS"})
        mock_ti.xcom_pull.assert_called_once_with(task_ids="setup")

    def test_circuit_breaker_enable_disable(self):
        dag_id = "test_dag_circuit_breaker"
        disable_dag(dag_id, reason="Testing circuit breaker pause")
        self.assertTrue(is_dag_disabled(dag_id))

        enable_dag(dag_id)
        self.assertFalse(is_dag_disabled(dag_id))


if __name__ == "__main__":
    unittest.main()
