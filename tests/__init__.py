"""Base test framework for Airflow DAGs, tasks, and operators.

Implements Section 9:
- DagTestBase class providing task execution harnesses, XCom tracking,
  and assertion helpers without spinning up a live Airflow DB.
"""

from __future__ import annotations

import os
import sys
import unittest
from contextlib import contextmanager
from typing import Any, Dict, Optional, Type

# Ensure root and plugins are on sys.path
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Ensure Airflow compatibility
try:
    import airflow  # noqa: F401
except ImportError:
    import plugins.airflow_compat  # noqa: F401

from airflow.exceptions import AirflowSkipException, AirflowFailException, AirflowException  # type: ignore


class MockDagRun:
    """Mock Airflow DagRun."""
    def __init__(self, dag_id: str = "mock_dag", conf: Optional[Dict[str, Any]] = None, run_id: str = "test_run_1"):
        self.dag_id = dag_id
        self.conf = conf or {}
        self.run_id = run_id


class MockTaskInstance:
    """Mock TaskInstance tracking XCom values and task state."""
    def __init__(self, task_id: str, xcom_store: Dict[str, Any]):
        self.task_id = task_id
        self.xcom_store = xcom_store
        self.state: Optional[str] = None

    def xcom_pull(self, task_ids: str, key: str = "return_value", default: Any = None) -> Any:
        return self.xcom_store.get(task_ids, default)

    def xcom_push(self, key: str, value: Any) -> None:
        self.xcom_store[self.task_id] = value


class DagTestBase(unittest.TestCase):
    """Base class providing Airflow DAG test fixtures and execution runner."""

    dag: Any = None
    current_context: Dict[str, Any] = {}
    xcom_store: Dict[str, Any] = {}
    task_states: Dict[str, str] = {}

    def tearDown(self) -> None:
        self.xcom_store = {}
        self.task_states = {}
        self.current_context = {}

    def get_dag(self, dag_id: str) -> Any:
        """Lookup DAG instance by dag_id from globals or module."""
        import importlib

        # Check in common DAG locations
        known_dag_modules = [
            "dags.finance.invoicing.partner_billing_etl.dag",
            "dags.finance.operations.trigger_daily_invoicing.dag",
            "dags.finance.operations.reconciliation_safety_net.dag",
        ]
        for mod_name in known_dag_modules:
            try:
                mod = importlib.import_module(mod_name)
                if hasattr(mod, dag_id):
                    return getattr(mod, dag_id)
                if hasattr(mod, "dag") and getattr(mod.dag, "dag_id", None) == dag_id:
                    return mod.dag
            except Exception:
                pass

        # Check globals() fallback
        if dag_id in globals():
            return globals()[dag_id]

        raise KeyError(f"DAG '{dag_id}' not found in registered modules.")

    @contextmanager
    def new_dag_run(self, dag_id: Optional[str] = None, conf: Optional[Dict[str, Any]] = None, run_id: str = "test_run_01"):
        """Context manager initiating a clean DAG execution context."""
        active_dag = self.dag or (self.get_dag(dag_id) if dag_id else None)
        self.xcom_store = {}
        self.task_states = {}

        mock_dag_run = MockDagRun(
            dag_id=getattr(active_dag, "dag_id", "test_dag"),
            conf=conf or {},
            run_id=run_id,
        )

        self.current_context = {
            "dag": active_dag,
            "dag_run": mock_dag_run,
            "run_id": run_id,
            "params": conf or {},
            "ds": "2026-11-16",
        }

        try:
            yield self.current_context
        finally:
            pass

    def set_conf(self, conf: Dict[str, Any]) -> None:
        """Update dag_run conf in active context."""
        if "dag_run" in self.current_context:
            self.current_context["dag_run"].conf.update(conf)

    def execute_task(self, task_id: str) -> Any:
        """Execute a single task callable within current DAG context and capture XCom."""
        if not self.dag:
            raise ValueError("No active DAG set on test class (set self.dag in setup_method).")

        task = self.dag.get_task(task_id)
        ti = MockTaskInstance(task_id, self.xcom_store)
        self.current_context["task_instance"] = ti
        self.current_context["ti"] = ti

        try:
            result = task.execute(self.current_context)
            self.xcom_store[task_id] = result
            self.task_states[task_id] = "success"
            return result
        except AirflowSkipException:
            self.task_states[task_id] = "skipped"
            raise
        except Exception:
            self.task_states[task_id] = "failed"
            raise

    def assert_execute_task_successfully(self, task_id: str) -> Any:
        """Execute task and assert that it completed with success state."""
        res = self.execute_task(task_id)
        self.assertEqual(self.task_states.get(task_id), "success")
        return res

    def assert_execute_task_should_raise_exception(
        self,
        task_id: str,
        expected_exception: Type[Exception] = Exception,
    ) -> None:
        """Execute task and assert it raises expected exception."""
        with self.assertRaises(expected_exception):
            self.execute_task(task_id)
        self.assertEqual(self.task_states.get(task_id), "failed")

    def assert_task_skipped(self, task_id: str) -> None:
        """Assert task was marked skipped."""
        with self.assertRaises(AirflowSkipException):
            self.execute_task(task_id)
        self.assertEqual(self.task_states.get(task_id), "skipped")

    def modify_task_return_value(self, task_id: str, value: Any) -> None:
        """Manually override an XCom value produced by task_id."""
        self.xcom_store[task_id] = value

    def task_return_value(self, task_id: str) -> Any:
        """Retrieve stored XCom return value for task_id."""
        return self.xcom_store.get(task_id)

    def assert_has_task_id(self, dag: Any, task_id: str) -> None:
        """Assert task_id is present in DAG tasks."""
        self.assertIn(task_id, {task.task_id for task in dag.tasks})
