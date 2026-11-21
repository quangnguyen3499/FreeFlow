"""Python-based sensor operator to poll external conditions or file presence."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Optional
from airflow.models import BaseOperator  # type: ignore
from airflow.exceptions import AirflowException  # type: ignore

logger = logging.getLogger(__name__)


class CustomPythonSensor(BaseOperator):
    """Polls a python callable until it returns True, or timeout is reached."""

    def __init__(
        self,
        *,
        task_id: str,
        python_callable: Callable[..., bool],
        poke_interval: int = 10,
        timeout: int = 300,
        op_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.python_callable = python_callable
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.op_kwargs = op_kwargs or {}

    def execute(self, context: Dict[str, Any]) -> bool:
        start_time = time.time()
        merged = {**self.op_kwargs, **context}

        while True:
            try:
                res = self.python_callable(**merged)
                if res:
                    logger.info("Sensor condition satisfied.")
                    return True
            except Exception as exc:
                logger.warning("Error checking sensor condition: %s", exc)

            elapsed = time.time() - start_time
            if elapsed >= self.timeout:
                raise AirflowException(f"Sensor timed out after {elapsed:.1f}s.")

            time.sleep(self.poke_interval)
