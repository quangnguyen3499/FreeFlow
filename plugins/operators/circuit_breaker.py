"""Circuit Breaker Operator for halting pipelines and auto-disabling DAGs on invariant violations."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional
from airflow.models import BaseOperator  # type: ignore
from airflow.exceptions import AirflowFailException  # type: ignore
from dags.common_utils.airflow import disable_dag
from dags.common_utils.notify import post_to_slack

logger = logging.getLogger(__name__)


class CircuitBreakerOperator(BaseOperator):
    """Evaluates an invariant or critical condition.
    
    If the check fails, automatically disables/pauses the DAG, dispatches high-priority
    alerts to Slack/incident channels, and fails the execution immediately without retry.
    """

    def __init__(
        self,
        *,
        task_id: str,
        invariant_callable: Callable[..., bool],
        failure_reason: str,
        notify_channels: Optional[list[str]] = None,
        op_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.invariant_callable = invariant_callable
        self.failure_reason = failure_reason
        self.notify_channels = notify_channels or ["alerts-finance"]
        self.op_kwargs = op_kwargs or {}

    def execute(self, context: Dict[str, Any]) -> bool:
        merged = {**self.op_kwargs, **context}
        passed = self.invariant_callable(**merged)
        if not passed:
            dag_id = context.get("dag", getattr(self, "dag", None))
            dag_id_str = getattr(dag_id, "dag_id", "unknown_dag")

            alert_msg = (
                f":rotating_light: *CRITICAL INVARIANT BREACH* in DAG `{dag_id_str}`!\n"
                f"*Reason*: {self.failure_reason}\n"
                f"Automated circuit breaker has *DISABLED* the DAG to protect upstream/downstream data integrity."
            )
            logger.critical(alert_msg)

            # Auto-disable DAG
            disable_dag(dag_id_str, reason=self.failure_reason)

            # Send critical alert
            post_to_slack(channels=self.notify_channels, text=alert_msg, level="CRITICAL")

            raise AirflowFailException(f"Circuit breaker tripped: {self.failure_reason}")

        logger.info("Circuit breaker check passed successfully.")
        return True
