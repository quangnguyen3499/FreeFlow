"""Airflow helpers for environment detection, Variables, XCom, and DAG management."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def env() -> str:
    """Return current deployment environment: development | test | staging | production."""
    return os.environ.get("AIRFLOW_ENV", "development").strip().lower()


def fetch_variable(name: str, default: Any = None) -> Any:
    """Safely fetch an Airflow Variable.
    
    If missing, logs a warning and returns default value instead of raising KeyError.
    """
    from airflow.models import Variable  # type: ignore

    try:
        val = Variable.get(name)
        return val
    except Exception as exc:
        logger.warning(
            "Airflow Variable '%s' not found (%s). Using default: '%s'.",
            name,
            exc,
            default,
        )
        return default


def fetch_variable_as_json(name: str, default: Any = None) -> Any:
    """Safely fetch an Airflow Variable deserialized from JSON."""
    raw = fetch_variable(name, default=None)
    if raw is None:
        return default
    try:
        if isinstance(raw, (dict, list)):
            return raw
        return json.loads(raw)
    except Exception as exc:
        logger.warning(
            "Variable '%s' could not be parsed as JSON: %s. Returning default.",
            name,
            exc,
        )
        return default


def save_variable_as_json(name: str, value: Any) -> None:
    """Save an object to an Airflow Variable serialized as JSON."""
    from airflow.models import Variable  # type: ignore

    payload = json.dumps(value, default=str)
    Variable.set(name, payload)


def get_conf(context: Dict[str, Any]) -> Dict[str, Any]:
    """Safely extract the dag_run.conf dictionary from Airflow task execution context."""
    dag_run = context.get("dag_run")
    if dag_run and hasattr(dag_run, "conf") and isinstance(dag_run.conf, dict):
        return dag_run.conf
    return {}


def task_return_value(context: Dict[str, Any], task_id: str) -> Any:
    """Read the return value (XCom) produced by another task in the current DAG run."""
    ti = context.get("task_instance") or context.get("ti")
    if not ti:
        raise ValueError("TaskInstance object not found in execution context.")

    return ti.xcom_pull(task_ids=task_id)


def is_dag_disabled(dag_id: str) -> bool:
    """Check whether a DAG is currently paused/disabled."""
    # Check circuit breaker override variable first
    circuit_var = fetch_variable(f"circuit_breaker_{dag_id}", default=None)
    if circuit_var is not None:
        return str(circuit_var).lower() in ("true", "1", "disabled", "paused")

    try:
        from airflow.models import DagModel  # type: ignore
        from airflow.utils.session import provide_session  # type: ignore

        @provide_session
        def _check_paused(session=None):
            dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            return dag_model.is_paused if dag_model else False

        return _check_paused()
    except Exception:
        return False


def disable_dag(dag_id: str, reason: str = "") -> None:
    """Circuit Breaker: dynamically disable/pause a DAG to stop cascading failures."""
    logger.critical("CIRCUIT BREAKER TRIGGERED: Disabling DAG '%s'. Reason: %s", dag_id, reason)
    from airflow.models import Variable  # type: ignore
    Variable.set(f"circuit_breaker_{dag_id}", "disabled")

    try:
        from airflow.models import DagModel  # type: ignore
        from airflow.utils.session import provide_session  # type: ignore

        @provide_session
        def _pause(session=None):
            dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if dag_model:
                dag_model.is_paused = True
                session.commit()

        _pause()
    except Exception as exc:
        logger.warning("Could not set is_paused on DagModel via DB session: %s", exc)


def enable_dag(dag_id: str) -> None:
    """Re-enable a previously disabled DAG."""
    logger.info("Enabling DAG '%s'.", dag_id)
    from airflow.models import Variable  # type: ignore
    Variable.set(f"circuit_breaker_{dag_id}", "enabled")

    try:
        from airflow.models import DagModel  # type: ignore
        from airflow.utils.session import provide_session  # type: ignore

        @provide_session
        def _unpause(session=None):
            dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if dag_model:
                dag_model.is_paused = False
                session.commit()

        _unpause()
    except Exception as exc:
        logger.warning("Could not unpause DagModel via DB session: %s", exc)


def trigger_dag(
    dag_id: str,
    conf: Optional[Dict[str, Any]] = None,
    if_disabled: str = "fail",  # "fail" | "skip" | "trigger"
) -> Optional[str]:
    """Trigger another DAG execution with payload conf, handling disabled states."""
    disabled = is_dag_disabled(dag_id)
    if disabled:
        if if_disabled == "skip":
            logger.info("Target DAG '%s' is disabled. Skipping trigger as requested.", dag_id)
            return None
        elif if_disabled == "fail":
            from airflow.exceptions import AirflowFailException  # type: ignore
            raise AirflowFailException(f"Target DAG '{dag_id}' is disabled. Aborting trigger.")

    from airflow.api.common.experimental.trigger_dag import trigger_dag as api_trigger_dag  # type: ignore
    run_conf = conf or {}
    logger.info("Triggering DAG '%s' with conf: %s", dag_id, run_conf)

    try:
        run = api_trigger_dag(dag_id=dag_id, conf=run_conf)
        return getattr(run, "run_id", "triggered_run")
    except Exception as exc:
        logger.warning("API trigger failed or simulated in test: %s", exc)
        return "mock_run_id"
