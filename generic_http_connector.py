"""
Generic HTTP/Webhook Connector for Helix Spirals.

This universal connector enables integration with ANY REST API or webhook endpoint.
With this connector, Helix Spirals can connect to thousands of services without
needing dedicated connectors for each one.

Features:
- Connect to any REST API (GET, POST, PUT, PATCH, DELETE)
- Custom headers and authentication (API Key, Bearer Token, Basic Auth, OAuth2)
- Request/response transformation with JSONPath
- Webhook receiver for incoming events
- Rate limiting and retry logic with exponential backoff
- Request signing (HMAC)

Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from urllib.parse import urlencode, urljoin

import aiohttp

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types supported."""

    NONE = "none"
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer"
    BASIC_AUTH = "basic"
    OAUTH2 = "oauth2"
    CUSTOM_HEADER = "custom_header"
    HMAC_SIGNATURE = "hmac"


class HttpMethod(Enum):
    """HTTP methods."""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class ContentType(Enum):
    """Request content types."""

    JSON = "application/json"
    FORM = "application/x-www-form-urlencoded"
    MULTIPART = "multipart/form-data"
    XML = "application/xml"
    TEXT = "text/plain"


@dataclass
class AuthConfig:
    """Authentication configuration."""

    auth_type: AuthType = AuthType.NONE

    # API Key auth
    api_key: str | None = None
    api_key_header: str = "X-API-Key"
    api_key_prefix: str = ""

    # Bearer token
    bearer_token: str | None = None

    # Basic auth
    username: str | None = None
    password: str | None = None

    # OAuth2
    oauth2_token_url: str | None = None
    oauth2_client_id: str | None = None
    oauth2_client_secret: str | None = None
    oauth2_scope: str | None = None
    oauth2_access_token: str | None = None
    oauth2_refresh_token: str | None = None
    oauth2_expires_at: datetime | None = None

    # Custom header
    custom_header_name: str | None = None
    custom_header_value: str | None = None

    # HMAC signature
    hmac_secret: str | None = None
    hmac_header: str = "X-Signature"
    hmac_algorithm: str = "sha256"


@dataclass
class RequestConfig:
    """HTTP request configuration."""

    url: str
    method: HttpMethod = HttpMethod.GET
    headers: dict[str, str] = field(default_factory=dict)
    query_params: dict[str, str] = field(default_factory=dict)
    body: dict | str | bytes | None = None
    content_type: ContentType = ContentType.JSON
    timeout: int = 30
    follow_redirects: bool = True
    verify_ssl: bool = True


@dataclass
class ResponseConfig:
    """Response handling configuration."""

    data_path: str | None = None  # JSONPath e.g. "$.data.items[*]"
    success_path: str | None = None
    success_value: Any | None = None
    error_path: str | None = None


@dataclass
class RetryConfig:
    """Retry configuration."""

    max_retries: int = 3
    retry_delay: float = 1.0
    retry_multiplier: float = 2.0
    retry_on_status: list[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""

    requests_per_second: float = 10.0
    burst_limit: int = 20


@dataclass
class GenericHttpConfig:
    """Complete configuration for Generic HTTP connector."""

    name: str
    base_url: str
    description: str = ""
    auth: AuthConfig = field(default_factory=AuthConfig)
    default_headers: dict[str, str] = field(default_factory=dict)
    retry: RetryConfig = field(default_factory=RetryConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    timeout: int = 30


@dataclass
class HttpResponse:
    """HTTP response wrapper."""

    status_code: int
    headers: dict[str, str]
    body: Any
    raw_body: bytes
    elapsed_ms: float
    success: bool
    error: str | None = None


class GenericHttpConnector:
    """
    Universal HTTP connector for any REST API.

    Enables Helix Spirals to integrate with ANY service that has a REST API,
    providing unlimited integrations via configurable auth, headers, and
    request/response handling.

    Example:
        config = GenericHttpConfig(
            name="My API",
            base_url="https://api.example.com",
            auth=AuthConfig(auth_type=AuthType.BEARER_TOKEN, bearer_token="tok")
        )
        connector = GenericHttpConnector(config)
        response = await connector.get("/users")
    """

    def __init__(self, config: GenericHttpConfig):
        self.config = config
        self._session: aiohttp.ClientSession | None = None
        self._last_request_time: float = 0
        self._token_bucket: float = config.rate_limit.burst_limit

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    @staticmethod
    def _validate_url_not_internal(url: str) -> None:
        """Block requests to private/internal/metadata addresses (SSRF protection)."""
        import ipaddress
        from urllib.parse import urlparse

        parsed = urlparse(url)
        host = parsed.hostname or ""
        blocked_hosts = {
            "localhost",
            "127.0.0.1",
            "0.0.0.0",
            "::1",
            "metadata.google.internal",  # GCP metadata
            "169.254.169.254",  # AWS/Azure metadata
        }
        if host in blocked_hosts:
            raise ValueError(f"Requests to {host} are blocked for security (SSRF protection).")
        try:
            ip = ipaddress.ip_address(host)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                raise ValueError(f"Requests to private/reserved IP ({host}) are blocked (SSRF protection).")
        except ValueError as e:
            if "SSRF" in str(e):
                raise
            # Not an IP address — hostname, allow (DNS resolved by aiohttp)

    async def close(self):
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _apply_rate_limit(self):
        """Apply rate limiting using token bucket algorithm."""
        now = time.time()
        elapsed = now - self._last_request_time

        self._token_bucket = min(
            self.config.rate_limit.burst_limit,
            self._token_bucket + elapsed * self.config.rate_limit.requests_per_second,
        )

        if self._token_bucket < 1:
            wait_time = (1 - self._token_bucket) / self.config.rate_limit.requests_per_second
            await asyncio.sleep(wait_time)
            self._token_bucket = 1

        self._token_bucket -= 1
        self._last_request_time = now

    def _build_auth_headers(self) -> dict[str, str]:
        """Build authentication headers based on config."""
        headers: dict[str, str] = {}
        auth = self.config.auth

        if auth.auth_type == AuthType.API_KEY and auth.api_key:
            value = f"{auth.api_key_prefix}{auth.api_key}" if auth.api_key_prefix else auth.api_key
            headers[auth.api_key_header] = value

        elif auth.auth_type == AuthType.BEARER_TOKEN and auth.bearer_token:
            headers["Authorization"] = f"Bearer {auth.bearer_token}"

        elif auth.auth_type == AuthType.BASIC_AUTH and auth.username and auth.password:
            credentials = base64.b64encode(f"{auth.username}:{auth.password}".encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"

        elif auth.auth_type == AuthType.OAUTH2 and auth.oauth2_access_token:
            headers["Authorization"] = f"Bearer {auth.oauth2_access_token}"

        elif auth.auth_type == AuthType.CUSTOM_HEADER and auth.custom_header_name:
            headers[auth.custom_header_name] = auth.custom_header_value or ""

        return headers

    def _sign_request(self, body: bytes) -> str | None:
        """Generate HMAC signature for request body."""
        auth = self.config.auth
        if auth.auth_type != AuthType.HMAC_SIGNATURE or not auth.hmac_secret:
            return None

        hash_func = getattr(hashlib, auth.hmac_algorithm, hashlib.sha256)
        return hmac.new(auth.hmac_secret.encode(), body, hash_func).hexdigest()

    async def _refresh_oauth2_token(self) -> bool:
        """Refresh OAuth2 access token using refresh_token grant."""
        auth = self.config.auth
        if not auth.oauth2_token_url or not auth.oauth2_refresh_token:
            return False

        try:
            session = await self._get_session()
            async with session.post(
                auth.oauth2_token_url,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": auth.oauth2_refresh_token,
                    "client_id": auth.oauth2_client_id,
                    "client_secret": auth.oauth2_client_secret,
                },
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    auth.oauth2_access_token = data.get("access_token")
                    auth.oauth2_refresh_token = data.get("refresh_token", auth.oauth2_refresh_token)
                    auth.oauth2_expires_at = datetime.now(UTC)
                    return True
        except Exception as e:
            logger.error("Failed to refresh OAuth2 token: %s", e)

        return False

    async def request(
        self,
        request_cfg: RequestConfig,
        response_config: ResponseConfig | None = None,
    ) -> HttpResponse:
        """
        Make an HTTP request with auth, rate limiting, and retry logic.

        Args:
            request_cfg: Request configuration
            response_config: Optional response handling configuration

        Returns:
            HttpResponse with parsed data
        """
        await self._apply_rate_limit()

        # SSRF protection: block requests to internal/private networks
        full_url = request_cfg.url if request_cfg.url.startswith("http") else f"{self.config.base_url}{request_cfg.url}"
        self._validate_url_not_internal(full_url)

        # Check OAuth2 token expiry
        auth = self.config.auth
        if auth.auth_type == AuthType.OAUTH2 and auth.oauth2_expires_at and datetime.now(UTC) >= auth.oauth2_expires_at:
            await self._refresh_oauth2_token()

        # Build URL
        url = urljoin(self.config.base_url, request_cfg.url)
        if request_cfg.query_params:
            url = f"{url}?{urlencode(request_cfg.query_params)}"

        # Build headers
        headers = {**self.config.default_headers, **request_cfg.headers}
        headers.update(self._build_auth_headers())
        headers["Content-Type"] = request_cfg.content_type.value

        # Prepare body
        body_bytes = b""
        if request_cfg.body:
            if request_cfg.content_type == ContentType.JSON:
                body_bytes = (
                    json.dumps(request_cfg.body).encode()
                    if isinstance(request_cfg.body, dict)
                    else str(request_cfg.body).encode()
                )
            elif request_cfg.content_type == ContentType.FORM:
                body_bytes = (
                    urlencode(request_cfg.body).encode()
                    if isinstance(request_cfg.body, dict)
                    else str(request_cfg.body).encode()
                )
            elif isinstance(request_cfg.body, bytes):
                body_bytes = request_cfg.body
            else:
                body_bytes = str(request_cfg.body).encode()

        # HMAC signature
        signature = self._sign_request(body_bytes)
        if signature:
            headers[auth.hmac_header] = signature

        # Request with retry logic
        retry_config = self.config.retry
        last_error = None

        for attempt in range(retry_config.max_retries + 1):
            try:
                start_time = time.time()
                session = await self._get_session()

                async with session.request(
                    method=request_cfg.method.value,
                    url=url,
                    headers=headers,
                    data=body_bytes if body_bytes else None,
                    timeout=aiohttp.ClientTimeout(total=request_cfg.timeout),
                    ssl=request_cfg.verify_ssl,
                    allow_redirects=request_cfg.follow_redirects,
                ) as response:
                    elapsed_ms = (time.time() - start_time) * 1000
                    raw_body = await response.read()

                    # Parse response body
                    try:
                        content_type = response.headers.get("Content-Type", "")
                        if "application/json" in content_type:
                            body = json.loads(raw_body)
                        else:
                            body = raw_body.decode("utf-8", errors="replace")
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        body = raw_body.decode("utf-8", errors="replace")

                    # Retry on retryable status codes
                    if response.status in retry_config.retry_on_status and attempt < retry_config.max_retries:
                        delay = retry_config.retry_delay * (retry_config.retry_multiplier**attempt)
                        logger.warning(
                            "Request returned %s, retrying in %.1fs",
                            response.status,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    success = 200 <= response.status < 300
                    error = None

                    if response_config and not success and response_config.error_path and isinstance(body, dict):
                        error = self._extract_jsonpath(body, response_config.error_path)

                    if response_config and response_config.data_path and isinstance(body, dict):
                        body = self._extract_jsonpath(body, response_config.data_path)

                    return HttpResponse(
                        status_code=response.status,
                        headers=dict(response.headers),
                        body=body,
                        raw_body=raw_body,
                        elapsed_ms=elapsed_ms,
                        success=success,
                        error=error,
                    )

            except TimeoutError:
                last_error = "Request timeout"
                logger.warning("Request timeout (attempt %s)", attempt + 1)
            except aiohttp.ClientError as e:
                last_error = str(e)
                logger.warning("Request error: %s (attempt %s)", e, attempt + 1)
            except Exception as e:
                last_error = str(e)
                logger.error("Unexpected error: %s", e)
                break

            if attempt < retry_config.max_retries:
                delay = retry_config.retry_delay * (retry_config.retry_multiplier**attempt)
                await asyncio.sleep(delay)

        return HttpResponse(
            status_code=0,
            headers={},
            body=None,
            raw_body=b"",
            elapsed_ms=0,
            success=False,
            error=last_error or "Request failed after retries",
        )

    def _extract_jsonpath(self, data: Any, path: str) -> Any:
        """Simple JSONPath extraction: $.key, $.key.nested, $.array[0], $.array[*]"""
        if not path.startswith("$"):
            return data

        path = path[1:]
        if path.startswith("."):
            path = path[1:]

        current = data
        for part in path.split("."):
            if not current:
                return None

            array_match = re.match(r"(\w+)\[(\d+|\*)\]", part)
            if array_match:
                key, index = array_match.groups()
                if key and isinstance(current, dict):
                    current = current.get(key)
                if isinstance(current, list):
                    if index == "*":
                        return current
                    else:
                        idx = int(index)
                        current = current[idx] if idx < len(current) else None
            elif isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current

    # ---- Convenience methods ----

    async def get(self, url: str, params: dict | None = None) -> HttpResponse:
        """Make a GET request."""
        return await self.request(RequestConfig(url=url, method=HttpMethod.GET, query_params=params or {}))

    async def post(self, url: str, data: dict | None = None) -> HttpResponse:
        """Make a POST request."""
        return await self.request(RequestConfig(url=url, method=HttpMethod.POST, body=data))

    async def put(self, url: str, data: dict | None = None) -> HttpResponse:
        """Make a PUT request."""
        return await self.request(RequestConfig(url=url, method=HttpMethod.PUT, body=data))

    async def patch(self, url: str, data: dict | None = None) -> HttpResponse:
        """Make a PATCH request."""
        return await self.request(RequestConfig(url=url, method=HttpMethod.PATCH, body=data))

    async def delete(self, url: str) -> HttpResponse:
        """Make a DELETE request."""
        return await self.request(RequestConfig(url=url, method=HttpMethod.DELETE))


class WebhookReceiver:
    """
    Universal webhook receiver for incoming events.

    Validates webhook signatures and routes events to registered handlers.
    """

    def __init__(self):
        self._handlers: dict[str, list[Callable]] = {}
        self._signature_configs: dict[str, dict] = {}

    def register_handler(self, event_type: str, handler: Callable):
        """Register a handler for an event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def configure_signature(
        self,
        source: str,
        secret: str,
        header: str = "X-Signature",
        algorithm: str = "sha256",
    ):
        """Configure signature validation for a webhook source."""
        self._signature_configs[source] = {
            "secret": secret,
            "header": header,
            "algorithm": algorithm,
        }

    def validate_signature(self, source: str, payload: bytes, signature: str) -> bool:
        """Validate webhook signature."""
        config = self._signature_configs.get(source)
        if not config:
            return True  # No signature configured — pass through

        expected = hmac.new(
            config["secret"].encode(),
            payload,
            getattr(hashlib, config["algorithm"]),
        ).hexdigest()

        return hmac.compare_digest(expected, signature)

    async def handle_webhook(
        self,
        source: str,
        event_type: str,
        payload: dict,
        headers: dict,
    ) -> list[Any]:
        """Handle incoming webhook and dispatch to registered handlers."""
        results = []

        handlers = self._handlers.get(event_type, [])
        handlers.extend(self._handlers.get("*", []))  # Wildcard handlers

        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    result = await handler(source, event_type, payload)
                else:
                    result = handler(source, event_type, payload)
                results.append(result)
            except Exception as e:
                logger.error("Webhook handler error: %s", e)
                results.append({"error": str(e)})

        return results


# Pre-configured connector templates for common services
CONNECTOR_TEMPLATES = {
    "rest_api": GenericHttpConfig(
        name="Generic REST API",
        base_url="https://api.example.com",
        description="Connect to any REST API with custom authentication",
    ),
    "webhook_sender": GenericHttpConfig(
        name="Webhook Sender",
        base_url="",
        description="Send webhooks to any URL",
        default_headers={"Content-Type": "application/json"},
    ),
    "graphql_api": GenericHttpConfig(
        name="GraphQL API",
        base_url="https://api.example.com/graphql",
        description="Connect to GraphQL APIs",
        default_headers={"Content-Type": "application/json"},
    ),
}


def create_connector(
    name: str,
    base_url: str,
    auth_type: AuthType = AuthType.NONE,
    **auth_kwargs,
) -> GenericHttpConnector:
    """
    Factory function to quickly create a connector.

    Example:
        connector = create_connector(
            name="My API",
            base_url="https://api.example.com",
            auth_type=AuthType.BEARER_TOKEN,
            bearer_token="my-token"
        )
    """
    auth_config = AuthConfig(auth_type=auth_type, **auth_kwargs)
    config = GenericHttpConfig(name=name, base_url=base_url, auth=auth_config)
    return GenericHttpConnector(config)


__all__ = [
    "CONNECTOR_TEMPLATES",
    "AuthConfig",
    "AuthType",
    "ContentType",
    "GenericHttpConfig",
    "GenericHttpConnector",
    "HttpMethod",
    "HttpResponse",
    "RateLimitConfig",
    "RequestConfig",
    "ResponseConfig",
    "RetryConfig",
    "WebhookReceiver",
    "create_connector",
]
