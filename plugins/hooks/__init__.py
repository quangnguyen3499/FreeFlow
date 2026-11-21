"""Reusable Airflow Hooks and Clients package."""

from plugins.hooks.base_client import BaseHttpClient
from plugins.hooks.database_hook import DatabaseHook
from plugins.hooks.gateway_hook import ServiceGatewayHook
from plugins.hooks.partner_api_hook import PartnerApiHook, PartnerApiClient
from plugins.hooks.storage_hook import StorageHook

__all__ = [
    "BaseHttpClient",
    "DatabaseHook",
    "ServiceGatewayHook",
    "PartnerApiHook",
    "PartnerApiClient",
    "StorageHook",
]
