"""Config-driven Trigger DAG for Finance Invoicing operations.

Implements Section 7.1:
- Reads trigger definitions from triggers.yml.
- Evaluates business_day_check.
- Resolves dynamic date expressions like {{ today }} and {{ next_2_business_day }}.
- Triggers downstream decoupled ETL DAGs passing dynamic conf.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from airflow import DAG  # type: ignore
from airflow.operators.python import BranchPythonOperator, PythonOperator  # type: ignore

from dags.common_utils.airflow import trigger_dag
from dags.common_utils.date import is_business_day, resolve_date_expression
from dags.common_utils.general import load_yaml, pretty
from dags.common_utils.schedule import local_cron

logger = logging.getLogger(__name__)

config_path = os.path.join(os.path.dirname(__file__), "triggers.yml")
triggers_config = load_yaml(config_path)

for trigger_key, trigger_meta in triggers_config.items():
    dag_id = trigger_meta.get("dag_id", f"trigger_{trigger_key}")
    schedule_expr = trigger_meta.get("schedule", "0 8 * * 1-5")
    business_day_required = trigger_meta.get("business_day_check", True)
    if_disabled_policy = trigger_meta.get("if_disabled", "skip")
    base_conf = trigger_meta.get("conf", {})
    downstream_dags = trigger_meta.get("dags", {})

    trigger_dag_obj = DAG(
        dag_id=dag_id,
        default_args={"owner": "finance_ops", "depends_on_past": False},
        schedule_interval=local_cron(schedule_expr),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["finance", "operations", "trigger"],
    )

    globals()[dag_id] = trigger_dag_obj

    with trigger_dag_obj:
        def make_branch_callable(bday_check: bool):
            def _branch(**context):
                if bday_check and not is_business_day():
                    logger.info("Today is a non-business day or holiday. Branching to skip.")
                    return "skip_trigger"
                logger.info("Business day check passed. Branching to setup_triggers.")
                return "setup_triggers"
            return _branch

        branch_task = BranchPythonOperator(
            task_id="check_business_day",
            python_callable=make_branch_callable(business_day_required),
        )

        def skip_callable():
            logger.info("Trigger run skipped because today is not a business day.")
            return "SKIPPED"

        skip_task = PythonOperator(
            task_id="skip_trigger",
            python_callable=skip_callable,
        )

        def make_setup_callable(raw_conf: dict):
            def _setup(**context):
                resolved = {}
                for k, v in raw_conf.items():
                    resolved[k] = resolve_date_expression(v) if isinstance(v, str) else v
                logger.info("Resolved trigger conf:\n%s", pretty(resolved))
                return resolved
            return _setup

        setup_task = PythonOperator(
            task_id="setup_triggers",
            python_callable=make_setup_callable(base_conf),
        )

        branch_task >> [skip_task, setup_task]

        # Fan-out to downstream DAGs
        for target_dag_id, target_config in downstream_dags.items():
            def make_trigger_task(t_dag_id: str, t_cfg: dict, policy: str):
                def _trigger(**context):
                    ti = context["ti"]
                    resolved_base = ti.xcom_pull(task_ids="setup_triggers") or {}
                    merged_conf = dict(resolved_base)
                    extra_conf = t_cfg.get("conf", {})
                    for k, v in extra_conf.items():
                        merged_conf[k] = resolve_date_expression(v) if isinstance(v, str) else v

                    logger.info("Dispatching trigger to DAG '%s' with conf: %s", t_dag_id, merged_conf)
                    run_id = trigger_dag(t_dag_id, conf=merged_conf, if_disabled=policy)
                    return {"target_dag": t_dag_id, "run_id": run_id}
                return _trigger

            fanout_task = PythonOperator(
                task_id=f"trigger_{target_dag_id}",
                python_callable=make_trigger_task(target_dag_id, target_config, if_disabled_policy),
            )
            setup_task >> fanout_task
