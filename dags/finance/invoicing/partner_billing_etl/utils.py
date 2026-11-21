"""Cached hook factories and pure utility functions for Partner Billing ETL."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Dict, List
from plugins.hooks.database_hook import DatabaseHook
from plugins.hooks.partner_api_hook import PartnerApiHook, PartnerApiClient
from plugins.hooks.gateway_hook import ServiceGatewayHook

logger = logging.getLogger(__name__)


@lru_cache()
def get_db_hook(conn_id: str = "main_db") -> DatabaseHook:
    """Cached factory returning DatabaseHook instance."""
    return DatabaseHook(conn_id=conn_id)


@lru_cache()
def get_partner_hook(conn_id: str = "partner_billing_api") -> PartnerApiHook:
    """Cached factory returning PartnerApiHook instance."""
    return PartnerApiHook(conn_id=conn_id)


@lru_cache()
def get_partner_client(conn_id: str = "partner_billing_api") -> PartnerApiClient:
    """Cached factory returning PartnerApiClient instance."""
    return get_partner_hook(conn_id=conn_id).client


@lru_cache()
def get_gateway_hook(conn_id: str = "service_gateway") -> ServiceGatewayHook:
    """Cached factory returning ServiceGatewayHook instance."""
    return ServiceGatewayHook(conn_id=conn_id)


def build_partner_batch_payload(
    batch_id: str,
    partner_code: str,
    run_date: str,
    line_items: List[Dict[str, Any]],
    total_amount: str,
    currency: str = "USD",
) -> Dict[str, Any]:
    """Format batch payload matching Partner REST API contract."""
    return {
        "batch_id": batch_id,
        "partner_code": partner_code,
        "billing_date": run_date,
        "total_amount": total_amount,
        "currency": currency,
        "total_items": len(line_items),
        "items": line_items,
    }
