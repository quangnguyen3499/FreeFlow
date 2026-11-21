"""DAG definition for Partner Billing ETL pipeline.

Implements Sections 1, 3.1, and 7:
- Thin DAG wiring: logic strictly decoupled in tasks.py and utils.py.
- schedule=None (execution driven via trigger layer or manual conf trigger).
- Dynamic doc_md read from README.md.
- globals()[dag_id] = dag.
"""

from __future__ import annotations

import os
from datetime import datetime
from airflow import DAG  # type: ignore

import dags.finance.invoicing.partner_billing_etl.tasks as tasks

dag_id = "finance_process_partner_billing"

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="Extracts, validates, transforms, verifies, and delivers partner billing invoices.",
    schedule_interval=None,  # Driven by operations trigger layer or REST trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "etl", "invoicing", "partner"],
)

globals()[dag_id] = dag

with dag:
    setup = tasks.setup_task(dag_id)
    extract = tasks.extract_task()
    gate1_validate = tasks.gate1_validate_task()
    gate2_transform = tasks.gate2_transform_task()
    gate3_verify = tasks.gate3_verify_task()
    deliver = tasks.deliver_task()
    finalize = tasks.finalize_task()

    setup >> extract >> gate1_validate >> gate2_transform >> gate3_verify >> deliver >> finalize

readme_path = os.path.join(os.path.dirname(__file__), "README.md")
if os.path.exists(readme_path):
    with open(readme_path, "r", encoding="utf-8") as f:
        dag.doc_md = f.read()
