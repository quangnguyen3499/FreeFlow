"""Partner Billing API Hook and Client.

Implements Section 5.1: Split Hook and Client architecture.
- Hook knows only about Airflow Connection.
- Client is pure Python, handles HTTP endpoints, retries, and serialization.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from airflow.hooks.base import BaseHook  # type: ignore
from plugins.hooks.base_client import BaseHttpClient

logger = logging.getLogger(__name__)


class PartnerApiClient(BaseHttpClient):
    """Domain client for Partner Billing & Settlement REST API."""

    def submit_invoices_batch(self, batch_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a batch of invoices to partner API."""
        logger.info("Submitting invoice batch %s to partner API.", batch_payload.get("batch_id"))
        return self.post("/api/v1/invoices/batch", json_data=batch_payload)

    def get_invoice_status(self, invoice_id: str) -> Dict[str, Any]:
        """Query invoice settlement status by partner invoice id."""
        return self.get(f"/api/v1/invoices/{invoice_id}/status")

    def ping_health(self) -> Dict[str, Any]:
        """Health check endpoint."""
        return self.get("/health")


class PartnerApiHook(BaseHook):
    """Airflow Hook for Partner Billing API.
    
    Reads credentials from Airflow Connection:
    - host: Base URL of partner service (e.g. 'https://api.partner.example.com')
    - password: API token / Secret key
    - extra: Optional timeout or custom routing headers
    """

    conn_name_attr = "conn_id"
    default_conn_name = "partner_billing_api"
    conn_type = "http"
    hook_name = "Partner API Hook"

    def __init__(self, conn_id: str = "partner_billing_api"):
        super().__init__()
        self.conn_id = conn_id
        self._connection = None
        self._client: Optional[PartnerApiClient] = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.get_connection(self.conn_id)
        return self._connection

    @property
    def client(self) -> PartnerApiClient:
        """Instantiate and return the configured PartnerApiClient."""
        if self._client is None:
            conn = self.connection
            host = conn.host or "http://localhost:8080"
            if not host.startswith("http://") and not host.startswith("https://"):
                host = f"https://{host}"

            token = conn.password
            extra = getattr(conn, "extra_dejson", {}) or {}
            timeout = int(extra.get("timeout", 30))

            self._client = PartnerApiClient(
                base_url=host,
                auth_token=token,
                timeout=timeout,
            )
        return self._client
