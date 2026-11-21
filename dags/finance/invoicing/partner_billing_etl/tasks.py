"""Task factories for the Partner Billing ETL pipeline.

Implements Sections 3.2, 4, 7.2, and 11:
- Factory functions returning PythonOperator with nested callables.
- Dedicated setup task packaging all configuration into XCom.
- Tasks read configuration exclusively from setup_value.
- Multi-gate architecture (Gate 1 Validation, Gate 2 Transform, Gate 3 Invariant Verification).
- Circuit breaker auto-disables DAG on critical invariant failure.
- Record failure isolation: per-record try/except, aggregated error raised at finalize.
"""

from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, List

from airflow.exceptions import AirflowException, AirflowFailException  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

from dags.common_utils.airflow import (
    disable_dag,
    fetch_variable,
    get_conf,
    task_return_value,
)
from dags.common_utils.date import today_iso, unique_timestamp
from dags.common_utils.file import ensure_dir_exists, load_json, dict_to_csv
from dags.common_utils.general import pretty
from dags.common_utils.notify import post_to_slack
from dags.common_utils.summary import RunSummary
from dags.finance.shared.constants import (
    PARTNER_ALPHA,
    STATUS_BILLED,
    STATUS_PROCESSING,
)
from dags.finance.shared.transforms import calculate_line_item, to_decimal
from dags.finance.shared.validators import validate_single_record, verify_batch_invariants
import dags.finance.invoicing.partner_billing_etl.sql as sql
import dags.finance.invoicing.partner_billing_etl.utils as utils

logger = logging.getLogger(__name__)


def setup_task(dag_id: str) -> PythonOperator:
    """Setup task factory: aggregates all inputs into a single dict."""
    def setup(**context: Any) -> Dict[str, Any]:
        conf = get_conf(context)
        run_date = conf.get("target_date", today_iso())
        partner_code = conf.get("partner_code", PARTNER_ALPHA)
        dry_run = bool(conf.get("dry_run", False))
        max_batch_size = int(conf.get("max_batch_size", fetch_variable("max_batch_size", 500)))

        staging_root = fetch_variable("staging_path", os.path.join(os.getcwd(), "staging"))
        work_dir = os.path.join(staging_root, dag_id, unique_timestamp())
        ensure_dir_exists(work_dir)

        setup_value = {
            "dag_id": dag_id,
            "run_id": context.get("run_id", "manual"),
            "run_date": run_date,
            "partner_code": partner_code,
            "dry_run": dry_run,
            "max_batch_size": max_batch_size,
            "work_dir": work_dir,
            "notify_channels": [fetch_variable("team_slack_channel", "finance-ops")],
            "db_conn_id": fetch_variable("finance_db_conn_id", "main_db"),
            "partner_conn_id": fetch_variable("partner_api_conn_id", "partner_billing_api"),
            "batch_id": f"BATCH_{partner_code}_{unique_timestamp()}",
        }
        logger.info("setup_value initialized:\n%s", pretty(setup_value))
        return setup_value

    return PythonOperator(task_id="setup", python_callable=setup)


def extract_task() -> PythonOperator:
    """Extract task factory: retrieves pending transactions from database."""
    def extract(**context: Any) -> Dict[str, Any]:
        setup_val = task_return_value(context, "setup")
        db = utils.get_db_hook(setup_val["db_conn_id"])

        query = sql.get_unbilled_transactions_query()
        params = {
            "partner_code": setup_val["partner_code"],
            "start_date": "2020-01-01",
            "end_date": setup_val["run_date"],
            "limit": setup_val["max_batch_size"],
        }
        logger.info("Extracting records with parameters: %s", params)
        records = db.fetch_all(query, params)
        logger.info("Extracted %d unbilled records for partner %s.", len(records), setup_val["partner_code"])

        raw_path = os.path.join(setup_val["work_dir"], "raw_records.json")
        import json
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(records, f, default=str)

        return {"raw_path": raw_path, "total_extracted": len(records)}

    return PythonOperator(task_id="extract", python_callable=extract)


def gate1_validate_task() -> PythonOperator:
    """Gate 1: Record-level schema and business rule validation."""
    def validate(**context: Any) -> Dict[str, Any]:
        setup_val = task_return_value(context, "setup")
        extract_res = task_return_value(context, "extract")
        summary = RunSummary(dag_id=setup_val["dag_id"], run_id=setup_val["run_id"])

        raw_records = load_json(extract_res["raw_path"])
        summary.register_total(len(raw_records))

        valid_records: List[Dict[str, Any]] = []

        # Isolate per-record errors with try/except in loop
        for record in raw_records:
            rec_id = str(record.get("id", "UNKNOWN"))
            try:
                is_valid, reason = validate_single_record(record)
                if not is_valid:
                    summary.add_failure(
                        record_id=rec_id,
                        error_message=reason,
                        stage="gate1_validate",
                        context={"record": record},
                    )
                else:
                    valid_records.append(record)
            except Exception as exc:
                summary.add_failure(
                    record_id=rec_id,
                    error_message=f"Unexpected exception during validation: {exc}",
                    stage="gate1_validate",
                )

        logger.info(
            "Gate 1 Validation Complete. Valid: %d | Failed: %d",
            len(valid_records),
            summary.failure_count(),
        )

        validated_path = os.path.join(setup_val["work_dir"], "validated_records.json")
        import json
        with open(validated_path, "w", encoding="utf-8") as f:
            json.dump(valid_records, f, default=str)

        # Store summary dict in XCom for finalize task
        return {
            "validated_path": validated_path,
            "valid_count": len(valid_records),
            "summary_data": summary.to_dict(),
        }

    return PythonOperator(task_id="gate1_validate", python_callable=validate)


def gate2_transform_task() -> PythonOperator:
    """Gate 2: Exact financial calculations and invoice artifact generation."""
    def transform(**context: Any) -> Dict[str, Any]:
        setup_val = task_return_value(context, "setup")
        val_res = task_return_value(context, "gate1_validate")
        records = load_json(val_res["validated_path"])

        line_items: List[Dict[str, Any]] = []
        batch_total = Decimal("0.00")

        for r in records:
            financials = calculate_line_item(r["amount"])
            item = {
                "transaction_id": r["id"],
                "partner_code": r["partner_code"],
                "customer_ref": r.get("customer_ref", ""),
                "transaction_date": r["transaction_date"],
                "base_amount": str(financials["base_amount"]),
                "fee_amount": str(financials["fee_amount"]),
                "tax_amount": str(financials["tax_amount"]),
                "total_billed": str(financials["total_billed"]),
                "net_payout": str(financials["net_payout"]),
            }
            line_items.append(item)
            batch_total += financials["total_billed"]

        batch_payload = utils.build_partner_batch_payload(
            batch_id=setup_val["batch_id"],
            partner_code=setup_val["partner_code"],
            run_date=setup_val["run_date"],
            line_items=line_items,
            total_amount=str(batch_total),
        )

        output_json_path = os.path.join(setup_val["work_dir"], "invoice_batch.json")
        output_csv_path = os.path.join(setup_val["work_dir"], "invoice_batch.csv")

        import json
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(batch_payload, f, indent=2)

        if line_items:
            dict_to_csv(line_items, output_csv_path)

        logger.info(
            "Gate 2 Transform Complete. Total Items: %d | Batch Total Amount: %s",
            len(line_items),
            batch_total,
        )

        return {
            "output_json_path": output_json_path,
            "output_csv_path": output_csv_path,
            "declared_total_amount": str(batch_total),
            "declared_record_count": len(line_items),
        }

    return PythonOperator(task_id="gate2_transform", python_callable=transform)


def gate3_verify_task() -> PythonOperator:
    """Gate 3: Re-read generated output and verify invariants before external dispatch."""
    def verify(**context: Any) -> Dict[str, Any]:
        setup_val = task_return_value(context, "setup")
        transform_res = task_return_value(context, "gate2_transform")

        # Re-read from disk to ensure file consistency and absence of data corruption
        payload = load_json(transform_res["output_json_path"])
        items = payload.get("items", [])
        declared_total = to_decimal(transform_res["declared_total_amount"])
        declared_count = int(transform_res["declared_record_count"])

        is_valid, breach_reason = verify_batch_invariants(
            records=items,
            declared_total_amount=declared_total,
            declared_record_count=declared_count,
        )

        if not is_valid:
            # Circuit breaker: disable DAG and alert
            dag_id = setup_val["dag_id"]
            disable_dag(dag_id, reason=f"Gate 3 Invariant Violation: {breach_reason}")
            post_to_slack(
                channels=setup_val["notify_channels"],
                text=f":rotating_light: Invariant violation in `{dag_id}`: {breach_reason}",
                level="CRITICAL",
                title="CIRCUIT BREAKER ACTIVATED",
            )
            # Fail task without retry
            raise AirflowFailException(f"Gate 3 Invariant Check Failed: {breach_reason}")

        logger.info("Gate 3 Invariant Verification Passed: Invariants 100% verified.")
        return {"verified": True, "total_items": len(items), "batch_id": payload["batch_id"]}

    return PythonOperator(task_id="gate3_verify", python_callable=verify)


def deliver_task() -> PythonOperator:
    """Deliver task: sends batch payload to partner API and updates database state."""
    def deliver(**context: Any) -> Dict[str, Any]:
        setup_val = task_return_value(context, "setup")
        transform_res = task_return_value(context, "gate2_transform")
        batch_payload = load_json(transform_res["output_json_path"])

        if setup_val["dry_run"]:
            logger.info("DRY-RUN mode enabled: skipping partner API dispatch and database commit.")
            return {"status": "DRY_RUN", "delivered": False}

        if not batch_payload.get("items"):
            logger.info("No items to deliver in batch. Skipping partner dispatch.")
            return {"status": "EMPTY", "delivered": False}

        # Dispatch to partner API client
        partner_client = utils.get_partner_client(setup_val["partner_conn_id"])
        api_response = partner_client.submit_invoices_batch(batch_payload)
        logger.info("Partner API response: %s", api_response)

        # Update database transaction status
        db = utils.get_db_hook(setup_val["db_conn_id"])
        item_ids = [item["transaction_id"] for item in batch_payload["items"]]

        placeholders = ", ".join(f"'{iid}'" for iid in item_ids)
        update_sql = sql.get_update_transactions_status_query(placeholders)

        db.execute(
            update_sql,
            {
                "new_status": STATUS_BILLED,
                "batch_id": setup_val["batch_id"],
                "updated_at": today_iso(),
            },
        )
        logger.info("Updated %d transactions to status %s.", len(item_ids), STATUS_BILLED)

        return {"status": "DELIVERED", "api_response": api_response, "count": len(item_ids)}

    return PythonOperator(task_id="deliver", python_callable=deliver)


def finalize_task() -> PythonOperator:
    """Finalize task: generates report, sends Slack notification, raises aggregated failures."""
    def finalize(**context: Any) -> Dict[str, Any]:
        setup_val = task_return_value(context, "setup")
        val_res = task_return_value(context, "gate1_validate")
        deliver_res = task_return_value(context, "deliver")

        summary_data = val_res.get("summary_data", {})
        failures = summary_data.get("failures", [])

        # Construct summary report
        summary = RunSummary(dag_id=setup_val["dag_id"], run_id=setup_val["run_id"])
        summary.register_total(summary_data.get("total_records", 0))
        summary.add_success(deliver_res.get("count", 0))

        for f in failures:
            summary.add_failure(
                record_id=f["record_id"],
                error_message=f["error"],
                stage=f["stage"],
            )

        report = summary.format_report()
        logger.info("\n%s", report)

        level = "ERROR" if summary.has_failures() else "SUCCESS"
        post_to_slack(
            channels=setup_val["notify_channels"],
            text=f"```\n{report}\n```",
            level=level,
            title=f"Partner Billing ETL [{setup_val['partner_code']}]",
        )

        # Raise aggregated failure at the end if any individual records failed
        if summary.has_failures():
            failed_ids = [f["record_id"] for f in failures[:5]]
            raise AirflowException(
                f"Batch completed with {len(failures)} failed records. "
                f"Sample failed IDs: {failed_ids}. See run summary report."
            )

        return {"status": "COMPLETED", "report": report}

    return PythonOperator(task_id="finalize", python_callable=finalize)
