"""Unit tests for Service Gateway Hook and routing headers."""

import unittest
from unittest.mock import MagicMock, patch
from plugins.hooks.gateway_hook import ServiceGatewayHook


# docker exec -e AIRFLOW_ENV=test -it airflow-webserver pytest -s -vvv tests/plugins/test_gateway_hook.py
class TestServiceGatewayHook(unittest.TestCase):
    @patch("plugins.hooks.gateway_hook.ServiceGatewayHook.client")
    def test_call_service_routes_with_header(self, mock_client):
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "SUCCESS"}
        mock_response.content = b'{"status": "SUCCESS"}'
        mock_client.request.return_value = mock_response

        hook = ServiceGatewayHook("service_gateway")
        hook._client = mock_client

        res = hook.call_service(
            service_name="payment_service",
            method="POST",
            path="/api/charge",
            json_data={"amount": 1000},
        )

        self.assertEqual(res, {"status": "SUCCESS"})
        mock_client.request.assert_called_once_with(
            method="POST",
            path="/api/charge",
            json_data={"amount": 1000},
            params=None,
            headers={"X-Target-Service": "payment_service"},
        )


if __name__ == "__main__":
    unittest.main()
