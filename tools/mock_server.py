#!/usr/bin/env python3
"""Lightweight mock external API server for local testing and development.

Emulates:
- Partner Billing REST API (/api/v1/invoices/batch)
- Internal Service Gateway routes
- Slack incoming webhook endpoint
"""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [MockServer] %(message)s")
logger = logging.getLogger("mock_server")


class MockApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        target_service = self.headers.get("X-Target-Service")
        logger.info("GET %s (Target-Service: %s)", self.path, target_service)

        if self.path == "/health":
            self._send_json(200, {"status": "UP", "service": "mock-api"})
        elif self.path.startswith("/api/v1/invoices/") and self.path.endswith("/status"):
            invoice_id = self.path.split("/")[4]
            self._send_json(200, {"invoice_id": invoice_id, "status": "SETTLED"})
        else:
            self._send_json(200, {"message": "Mock Gateway Response", "path": self.path, "service": target_service})

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_length)
        target_service = self.headers.get("X-Target-Service")

        try:
            payload = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
        except Exception:
            payload = {}

        logger.info("POST %s (Target: %s, Payload Keys: %s)", self.path, target_service, list(payload.keys()))

        if self.path == "/api/v1/invoices/batch":
            batch_id = payload.get("batch_id", "UNKNOWN")
            items = payload.get("items", [])
            self._send_json(200, {
                "batch_id": batch_id,
                "status": "ACCEPTED",
                "items_received": len(items),
                "total_amount": payload.get("total_amount"),
                "partner_ack": "OK",
            })
        elif self.path == "/mock-slack":
            logger.info("Received Slack Notification:\n%s", payload.get("text", ""))
            self._send_json(200, {"ok": True})
        else:
            self._send_json(200, {"status": "SUCCESS", "received_payload": payload})


def run_server(port: int = 8080):
    server_address = ("", port)
    httpd = HTTPServer(server_address, MockApiHandler)
    logger.info("Mock API Server running on port %d...", port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopping mock server.")
        httpd.server_close()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    run_server(port)
