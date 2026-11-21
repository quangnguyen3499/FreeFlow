"""Notification utilities (Slack, email, webhooks) with status formatting."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

LEVEL_ICONS = {
    "SUCCESS": ":white_check_mark:",
    "INFO": ":information_source:",
    "WARNING": ":warning:",
    "ERROR": ":x:",
    "CRITICAL": ":rotating_light:",
}


def post_to_slack(
    channels: List[str],
    text: str,
    level: str = "INFO",
    title: Optional[str] = None,
    webhook_url: Optional[str] = None,
) -> bool:
    """Send formatted alert notification to Slack channels or incoming webhook."""
    norm_level = level.upper()
    icon = LEVEL_ICONS.get(norm_level, ":information_source:")
    header = f"{icon} *[{norm_level}]* {title if title else ''}\n"
    message_body = f"{header}{text}"

    logger.info("Notification dispatched to %s [%s]:\n%s", channels, norm_level, message_body)

    if not webhook_url:
        from dags.common_utils.airflow import fetch_variable
        webhook_url = fetch_variable("slack_webhook_url", default=None)

    if not webhook_url:
        logger.debug("No slack_webhook_url configured; message logged only.")
        return True

    success = True
    for ch in channels:
        payload: Dict[str, Any] = {
            "channel": ch,
            "text": message_body,
        }
        try:
            try:
                import requests  # type: ignore
                resp = requests.post(webhook_url, json=payload, timeout=10)
                if resp.status_code >= 400:
                    logger.warning("Slack webhook returned error status %d: %s", resp.status_code, resp.text)
                    success = False
            except ImportError:
                import urllib.request
                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if resp.status >= 400:
                        success = False
        except Exception as exc:
            logger.warning("Failed to deliver Slack notification to %s: %s", ch, exc)
            success = False

    return success
