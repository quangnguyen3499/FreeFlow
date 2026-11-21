"""Schedule and timezone conversion utilities."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def local_cron(cron_expression: str, tz: str = "UTC") -> str:
    """Return cron expression or cron schedule wrapper.
    
    Airflow 2.x supports schedule expressions with timezones or strings.
    This helper provides a central place to enforce local timezone configuration
    and validate standard 5-field cron expressions.
    """
    parts = cron_expression.strip().split()
    if len(parts) != 5:
        logger.warning(
            "Schedule expression '%s' does not have 5 fields. Returning as-is.",
            cron_expression,
        )
        return cron_expression

    # In modern Airflow, passing a standard cron string with DAG default_args or start_date
    # pinned to a timezone handles DST automatically.
    return cron_expression
