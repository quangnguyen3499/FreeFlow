"""Unit tests for PartnerApiClient endpoints and retries."""

import unittest
from unittest.mock import MagicMock, patch
from plugins.hooks.partner_api_hook import PartnerApiClient


# docker exec -e AIRFLOW_ENV=test -it airflow-webserver pytest -s -vvv tests/plugins/test_partner_client.py
class TestPartnerApiClient(unittest.TestCase):
    def setUp(self):
        self.client = PartnerApiClient(
            base_url="https://api.partner.example.com",
            auth_token="secret_token",
            timeout=10,
        )

    @patch("plugins.hooks.base_client.BaseHttpClient.request")
    def test_submit_invoices_batch(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.content = b'{"batch_id": "BATCH_1", "status": "ACCEPTED"}'
        mock_resp.json.return_value = {"batch_id": "BATCH_1", "status": "ACCEPTED"}
        mock_request.return_value = mock_resp

        payload = {"batch_id": "BATCH_1", "items": []}
        result = self.client.submit_invoices_batch(payload)

        self.assertEqual(result["status"], "ACCEPTED")
        mock_request.assert_called_once_with(
            "POST",
            "/api/v1/invoices/batch",
            json_data=payload,
            headers=None,
        )

    @patch("plugins.hooks.base_client.BaseHttpClient.request")
    def test_ping_health(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.content = b'{"status": "UP"}'
        mock_resp.json.return_value = {"status": "UP"}
        mock_request.return_value = mock_resp

        result = self.client.ping_health()
        self.assertEqual(result["status"], "UP")
        mock_request.assert_called_once_with(
            "GET",
            "/health",
            params=None,
            headers=None,
        )


if __name__ == "__main__":
    unittest.main()
