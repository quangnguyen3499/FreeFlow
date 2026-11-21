"""Validation helpers for financial records and batch integrity invariants."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Tuple
from dags.finance.shared.constants import (
    MIN_TRANSACTION_AMOUNT,
    MAX_ALLOWED_BATCH_AMOUNT,
    SUPPORTED_PARTNERS,
)
from dags.finance.shared.transforms import to_decimal


def validate_single_record(record: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate schema and business rules for a single transaction record.
    
    Returns (is_valid, error_reason).
    """
    required_keys = ["id", "partner_code", "amount", "transaction_date"]
    for key in required_keys:
        if not record.get(key):
            return False, f"Missing required field '{key}'"

    partner = record.get("partner_code")
    if partner not in SUPPORTED_PARTNERS:
        return False, f"Unsupported partner code '{partner}'. Allowed: {SUPPORTED_PARTNERS}"

    try:
        amt = to_decimal(record.get("amount", 0))
    except Exception as exc:
        return False, f"Invalid numerical amount: {exc}"

    if amt < MIN_TRANSACTION_AMOUNT:
        return False, f"Amount {amt} is below minimum threshold of {MIN_TRANSACTION_AMOUNT}"

    return True, ""


def verify_batch_invariants(
    records: List[Dict[str, Any]],
    declared_total_amount: Decimal,
    declared_record_count: int,
) -> Tuple[bool, str]:
    """Verify invariants across entire batch before sending out (Gate 3).
    
    Checks:
    1. Actual record count matches declared record count.
    2. Sum of individual line item amounts matches declared batch total exactly.
    3. Total batch amount does not exceed regulatory/business ceiling.
    """
    if len(records) != declared_record_count:
        return (
            False,
            f"Record count mismatch: actual {len(records)} != declared {declared_record_count}",
        )

    computed_sum = Decimal("0.00")
    for r in records:
        computed_sum += to_decimal(r.get("total_billed", 0))

    if computed_sum != declared_total_amount:
        return (
            False,
            f"Financial sum mismatch: computed sum {computed_sum} != declared total {declared_total_amount}",
        )

    if computed_sum > MAX_ALLOWED_BATCH_AMOUNT:
        return (
            False,
            f"Total batch amount {computed_sum} exceeds allowed ceiling {MAX_ALLOWED_BATCH_AMOUNT}",
        )

    return True, ""
