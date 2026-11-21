"""Internal Service Gateway Hook.

Implements Section 5.2: Shared gateway for internal microservices.
Uses a single Airflow connection and routes to downstream services via header:
'X-Target-Service: <service_name>'
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from airflow.hooks.base import BaseHook  # type: ignore
from plugins.hooks.base_client import BaseHttpClient

logger = logging.getLogger(__name__)


class ServiceGatewayHook(BaseHook):
    """Airflow Hook for routing calls to internal microservices via an API Gateway."""

    conn_name_attr = "conn_id"
    default_conn_name = "service_gateway"
    conn_type = "http"
    hook_name = "Service Gateway Hook"

    def __init__(self, conn_id: str = "service_gateway"):
        super().__init__()
        self.conn_id = conn_id
        self._connection = None
        self._client: Optional[BaseHttpClient] = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.get_connection(self.conn_id)
        return self._connection

    @property
    def client(self) -> BaseHttpClient:
        if self._client is None:
            conn = self.connection
            host = conn.host or "http://localhost:8000"
            if not host.startswith("http://") and not host.startswith("https://"):
                host = f"http://{host}"

            token = conn.password
            extra = getattr(conn, "extra_dejson", {}) or {}
            timeout = int(extra.get("timeout", 20))

            self._client = BaseHttpClient(
                base_url=host,
                auth_token=token,
                timeout=timeout,
            )
        return self._client

    def call_service(
        self,
        service_name: str,
        method: str,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Dispatch request through gateway with routing header."""
        routing_header = {"X-Target-Service": service_name}
        logger.info(
            "Gateway dispatch -> Service: %s, Method: %s, Path: %s",
            service_name,
            method,
            path,
        )
        resp = self.client.request(
            method=method,
            path=path,
            json_data=json_data,
            params=params,
            headers=routing_header,
        )
        return resp.json() if resp.content else {}

    def get_service(self, service_name: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self.call_service(service_name, "GET", path, params=params)

    def post_service(self, service_name: str, path: str, json_data: Optional[Dict[str, Any]] = None) -> Any:
        return self.call_service(service_name, "POST", path, json_data=json_data)
