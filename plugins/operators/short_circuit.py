"""ShortCircuitExceptOperator for skipping branches on expected conditions or non-fatal errors."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, Tuple, Type
from airflow.models import BaseOperator  # type: ignore
from airflow.exceptions import AirflowSkipException  # type: ignore

logger = logging.getLogger(__name__)


class ShortCircuitExceptOperator(BaseOperator):
    """Executes a callable. If an acceptable exception type is raised, skips downstream tasks cleanly."""

    def __init__(
        self,
        *,
        task_id: str,
        python_callable: Callable[..., Any],
        acceptable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        op_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.python_callable = python_callable
        self.acceptable_exceptions = acceptable_exceptions
        self.op_kwargs = op_kwargs or {}

    def execute(self, context: Dict[str, Any]) -> Any:
        merged = {**self.op_kwargs, **context}
        try:
            return self.python_callable(**merged)
        except self.acceptable_exceptions as exc:
            logger.info(
                "Acceptable exception caught in task '%s': %s. Skipping downstream tasks.",
                self.task_id,
                exc,
            )
            raise AirflowSkipException(f"Task skipped due to acceptable condition: {exc}")
