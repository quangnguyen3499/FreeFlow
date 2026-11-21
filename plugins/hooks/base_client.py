"""Resilient pure-Python HTTP Client with retries, timeouts, and structured error handling."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

# Try importing requests; fallback to urllib if not installed in minimal environments
try:
    import requests  # type: ignore
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    import urllib.request
    import urllib.error
    import urllib.parse

    class MockResponse:
        def __init__(self, status_code: int, content: bytes, headers: Optional[Dict[str, str]] = None):
            self.status_code = status_code
            self.content = content
            self.headers = headers or {}

        def json(self) -> Any:
            return json.loads(self.content.decode("utf-8")) if self.content else {}

        @property
        def text(self) -> str:
            return self.content.decode("utf-8")

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise Exception(f"HTTP Error {self.status_code}: {self.text}")

    class MockSession:
        def __init__(self):
            self.headers: Dict[str, str] = {}

        def request(
            self,
            method: str,
            url: str,
            params: Optional[Dict[str, Any]] = None,
            json: Optional[Dict[str, Any]] = None,
            headers: Optional[Dict[str, str]] = None,
            timeout: int = 30,
        ) -> MockResponse:
            merged_headers = dict(self.headers)
            if headers:
                merged_headers.update(headers)

            if params:
                query_string = urllib.parse.urlencode(params)
                url = f"{url}?{query_string}"

            req_data = None
            if json is not None:
                req_data = urllib.request.json.dumps(json).encode("utf-8") if hasattr(urllib.request, "json") else str(json).encode("utf-8")
                import json as _j
                req_data = _j.dumps(json).encode("utf-8")
                merged_headers["Content-Type"] = "application/json"

            req = urllib.request.Request(url, data=req_data, headers=merged_headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return MockResponse(resp.status, resp.read(), dict(resp.headers))
            except urllib.error.HTTPError as err:
                return MockResponse(err.code, err.read(), dict(err.headers))


def retry_call(fn: Callable[..., Any], max_attempts: int = 3, delay_seconds: float = 1.0) -> Any:
    """Retry wrapper. Uses tenacity if available, otherwise native loop."""
    try:
        from tenacity import retry, stop_after_attempt, wait_fixed  # type: ignore

        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_fixed(delay_seconds),
            reraise=True,
        )
        def _exec(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)

        return _exec()
    except ImportError:
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                return fn()
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Request attempt %d/%d failed: %s. Retrying in %ss...",
                    attempt,
                    max_attempts,
                    exc,
                    delay_seconds,
                )
                if attempt < max_attempts:
                    time.sleep(delay_seconds)
        if last_exc:
            raise last_exc


class BaseHttpClient:
    """Base client for interacting with RESTful HTTP services.
    
    Contains pure Python HTTP logic, timeouts on every request, and retry mechanisms.
    No Airflow dependency, allowing testing and usage outside of Airflow.
    """

    def __init__(
        self,
        base_url: str,
        auth_token: Optional[str] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        if HAS_REQUESTS:
            self.session = requests.Session()
        else:
            self.session = MockSession()  # type: ignore

        self.headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.auth_token:
            self.headers["Authorization"] = f"Bearer {self.auth_token}"
        if extra_headers:
            self.headers.update(extra_headers)

        self.session.headers.update(self.headers)

    def _build_url(self, path: str) -> str:
        clean_path = path.lstrip("/")
        return f"{self.base_url}/{clean_path}"

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Execute HTTP request with retries and timeout."""
        url = self._build_url(path)
        req_headers = dict(self.session.headers)
        if headers:
            req_headers.update(headers)

        logger.debug("HTTP %s -> %s (timeout=%ds)", method.upper(), url, self.timeout)

        def _do_request() -> Any:
            resp = self.session.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json_data,
                headers=req_headers,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp

        return retry_call(_do_request, max_attempts=self.max_retries, delay_seconds=self.retry_delay)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
        resp = self.request("GET", path, params=params, headers=headers)
        return resp.json() if resp.content else {}

    def post(self, path: str, json_data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
        resp = self.request("POST", path, json_data=json_data, headers=headers)
        return resp.json() if resp.content else {}

    def put(self, path: str, json_data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
        resp = self.request("PUT", path, json_data=json_data, headers=headers)
        return resp.json() if resp.content else {}
