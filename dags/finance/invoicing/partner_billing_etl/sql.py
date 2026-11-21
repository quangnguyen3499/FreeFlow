"""SQL query definitions for Partner Billing ETL pipeline.

Implements Section 3.4:
- One function per query intent.
- CTE-based structure for readability.
- Bind parameters to avoid SQL injection.
- Named constants for status codes and types.
"""

from __future__ import annotations

import logging
from dags.finance.shared.constants import STATUS_PENDING

logger = logging.getLogger(__name__)


def get_unbilled_transactions_query() -> str:
    """Return parameterized CTE query to extract pending transactions for billing.
    
    Bind parameters:
      :partner_code (str)
      :start_date (str YYYY-MM-DD)
      :end_date (str YYYY-MM-DD)
      :limit (int)
    """
    sql = f"""
    WITH filtered_transactions AS (
        SELECT 
            t.id,
            t.partner_code,
            t.transaction_date,
            t.amount,
            t.currency,
            t.customer_ref,
            t.status
        FROM partner_transactions t
        WHERE t.status = '{STATUS_PENDING}'
          AND t.partner_code = :partner_code
          AND t.transaction_date >= :start_date
          AND t.transaction_date <= :end_date
    ),
    ordered_batch AS (
        SELECT 
            id,
            partner_code,
            transaction_date,
            amount,
            currency,
            customer_ref,
            status
        FROM filtered_transactions
        ORDER BY transaction_date ASC, id ASC
        LIMIT :limit
    )
    SELECT * FROM ordered_batch;
    """
    return sql.strip()


def get_update_transactions_status_query(placeholders: str) -> str:
    """Return parameterized query to bulk update transactions status and batch_id.
    
    Parameters:
      :new_status (str)
      :batch_id (str)
      :updated_at (str)
    """
    sql = f"""
    UPDATE partner_transactions
    SET 
        status = :new_status,
        batch_id = :batch_id,
        updated_at = :updated_at
    WHERE id IN ({placeholders});
    """
    return sql.strip()


def get_reconciliation_audit_query() -> str:
    """Return query to audit unbilled transactions stuck in processing status."""
    sql = """
    SELECT 
        id,
        partner_code,
        status,
        updated_at,
        amount
    FROM partner_transactions
    WHERE status IN ('PROCESSING', 'VALIDATED')
      AND updated_at <= :threshold_timestamp;
    """
    return sql.strip()
