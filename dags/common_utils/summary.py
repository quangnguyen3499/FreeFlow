"""Run summary utility to track successes, skips, and failures with record IDs."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class FailureRecord:
    record_id: str
    error_message: str
    stage: str
    context: Dict[str, Any] = field(default_factory=dict)


class RunSummary:
    """Collects and summarizes execution metrics across batch ETL records."""

    def __init__(self, dag_id: str, run_id: Optional[str] = None):
        self.dag_id = dag_id
        self.run_id = run_id or "manual"
        self.total_records: int = 0
        self.success_count: int = 0
        self.skip_count: int = 0
        self.failures: List[FailureRecord] = []
        self.skipped_records: List[Dict[str, str]] = []

    def register_total(self, count: int) -> None:
        self.total_records = count

    def add_success(self, count: int = 1) -> None:
        self.success_count += count

    def add_skip(self, record_id: str, reason: str) -> None:
        self.skip_count += 1
        self.skipped_records.append({"record_id": str(record_id), "reason": reason})

    def add_failure(
        self,
        record_id: str,
        error_message: str,
        stage: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        rec = FailureRecord(
            record_id=str(record_id),
            error_message=str(error_message),
            stage=stage,
            context=context or {},
        )
        self.failures.append(rec)
        logger.error(
            "Record %s failed at stage '%s': %s",
            record_id,
            stage,
            error_message,
        )

    def has_failures(self) -> bool:
        return len(self.failures) > 0

    def failure_count(self) -> int:
        return len(self.failures)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "total_records": self.total_records,
            "success_count": self.success_count,
            "skip_count": self.skip_count,
            "failure_count": len(self.failures),
            "failures": [
                {
                    "record_id": f.record_id,
                    "stage": f.stage,
                    "error": f.error_message,
                }
                for f in self.failures
            ],
            "skips": self.skipped_records,
        }

    def format_report(self) -> str:
        """Render a readable summary table/report suitable for logs and Slack."""
        lines = [
            f"=== Pipeline Run Summary [{self.dag_id}] ===",
            f"Run ID: {self.run_id}",
            f"Total Processed : {self.total_records}",
            f"Success         : {self.success_count}",
            f"Skipped         : {self.skip_count}",
            f"Failed          : {len(self.failures)}",
        ]

        if self.failures:
            lines.append("\n--- Failure Details ---")
            for idx, fail in enumerate(self.failures[:20], 1):  # Cap to top 20 for readability
                lines.append(f"{idx}. Record ID: {fail.record_id} | Stage: {fail.stage} | Error: {fail.error_message}")
            if len(self.failures) > 20:
                lines.append(f"... and {len(self.failures) - 20} more failed records.")

        if self.skipped_records:
            lines.append("\n--- Skipped Details ---")
            for idx, sk in enumerate(self.skipped_records[:10], 1):
                lines.append(f"{idx}. Record ID: {sk['record_id']} | Reason: {sk['reason']}")
            if len(self.skipped_records) > 10:
                lines.append(f"... and {len(self.skipped_records) - 10} more skipped records.")

        return "\n".join(lines)
