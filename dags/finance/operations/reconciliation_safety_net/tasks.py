"""Tasks for Reconciliation & Safety Net DAG."""

from __future__ import annotations

import logging
from typing import Any, Dict
from airflow.operators.python import PythonOperator  # type: ignore

from dags.common_utils.airflow import fetch_variable, task_return_value
from dags.common_utils.date import today_iso
from dags.common_utils.notify import post_to_slack
from plugins.hooks.database_hook import DatabaseHook
from dags.finance.invoicing.partner_billing_etl.sql import get_reconciliation_audit_query

logger = logging.getLogger(__name__)


def audit_stuck_transactions_task() -> PythonOperator:
    def audit(**context: Any) -> Dict[str, Any]:
        db_conn = fetch_variable("finance_db_conn_id", "main_db")
        db = DatabaseHook(db_conn)

        query = get_reconciliation_audit_query()
        stuck_records = db.fetch_all(query, {"threshold_timestamp": today_iso()})

        if stuck_records:
            msg = (
                f":warning: *Safety Net Alert*: Found {len(stuck_records)} transactions stuck "
                f"in intermediate state (PROCESSING/VALIDATED).\n"
                f"Sample Record IDs: {[r['id'] for r in stuck_records[:5]]}"
            )
            logger.warning(msg)
            post_to_slack(
                channels=["finance-ops"],
                text=msg,
                level="WARNING",
                title="SAFETY NET RECONCILIATION AUDIT",
            )
        else:
            logger.info("Reconciliation audit clean: No stuck transactions found.")

        return {"stuck_count": len(stuck_records), "records": stuck_records}

    return PythonOperator(task_id="audit_stuck_transactions", python_callable=audit)
