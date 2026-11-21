"""DAG definition for Reconciliation & Safety Net."""

from __future__ import annotations

import os
from datetime import datetime
from airflow import DAG  # type: ignore

import dags.finance.operations.reconciliation_safety_net.tasks as tasks
from dags.common_utils.schedule import local_cron

dag_id = "finance_reconcile_unsettled_billing"

dag = DAG(
    dag_id=dag_id,
    default_args={"owner": "finance_ops", "depends_on_past": False},
    schedule_interval=local_cron("30 23 * * *"),  # 23:30 every night
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "operations", "safety_net", "audit"],
)

globals()[dag_id] = dag

with dag:
    audit_task = tasks.audit_stuck_transactions_task()

readme_path = os.path.join(os.path.dirname(__file__), "README.md")
if os.path.exists(readme_path):
    with open(readme_path, "r", encoding="utf-8") as f:
        dag.doc_md = f.read()
