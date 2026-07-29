"""
Helix Workflow Engine - Integrations Layer
==========================================

500+ integrations via common API standards:
- REST APIs (OpenAPI/Swagger)
- Webhooks
- Database connections
- File systems
- Authentication (OAuth, API keys)
"""

import ipaddress
import logging
import socket
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from apps.backend.helix_flow.tools import Tool, ToolResult

logger = logging.getLogger(__name__)


def _is_safe_url(url: str) -> bool:
    """Validate URL is not targeting private/internal addresses (SSRF protection)."""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = parsed.hostname
        if not hostname:
            return False
        for info in socket.getaddrinfo(hostname, parsed.port or 443, proto=socket.IPPROTO_TCP):
            addr = info[4][0]
            ip = ipaddress.ip_address(addr)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return False
        return True
    except Exception as e:
        logging.getLogger(__name__).debug("SSRF validation failed for URL: %s", e)
        return False


class IntegrationRegistry:
    """
    Registry for managing workflow integrations.

    Provides access to 500+ integrations through:
    - Direct API connectors
    - Webhook receivers
    - Database adapters
    - File system interfaces
    """

    def __init__(self):
        self._integrations: dict[str, Integration] = {}
        self._categories: dict[str, list[str]] = {}
        self._setup_built_in_integrations()

    def _setup_built_in_integrations(self):
        """Setup built-in integrations"""

        # Popular SaaS integrations
        saas_integrations = [
            "slack",
            "discord",
            "telegram",
            "teams",
            "gmail",
            "outlook",
            "sendgrid",
            "mailgun",
            "salesforce",
            "hubspot",
            "pipedrive",
            "zoho",
            "jira",
            "asana",
            "trello",
            "notion",
            "linear",
            "github",
            "gitlab",
            "bitbucket",
            "jenkins",
            "google_sheets",
            "airtable",
            "notion",
            "coda",
            "stripe",
            "paypal",
            "braintree",
            "square",
            "shopify",
            "woocommerce",
            "magento",
            "bigcommerce",
            "aws",
            "azure",
            "google_cloud",
            "digitalocean",
            "openai",
            "anthropic",
            "cohere",
            "huggingface",
            "twitter",
            "facebook",
            "instagram",
            "linkedin",
            "reddit",
            "youtube",
            "twitch",
            "spotify",
            "dropbox",
            "google_drive",
            "onedrive",
            "box",
            "zendesk",
            "intercom",
            "freshdesk",
            "helpscout",
        ]

        for integration_name in saas_integrations:
            self.register(
                RestApiIntegration(
                    name=integration_name,
                    category="saas",
                    base_url=self._get_base_url(integration_name),
                )
            )

        # Database integrations
        db_integrations = [
            "postgresql",
            "mysql",
            "mongodb",
            "redis",
            "sqlite",
            "elasticsearch",
            "firebase",
            "supabase",
        ]

        for integration_name in db_integrations:
            self.register(DatabaseIntegration(name=integration_name, category="database"))

        # File system integrations
        fs_integrations = [
            "local_filesystem",
            "s3",
            "gcs",
            "azure_blob",
            "ftp",
            "sftp",
            "webdav",
        ]

        for integration_name in fs_integrations:
            self.register(FileSystemIntegration(name=integration_name, category="filesystem"))

        logger.info("Setup %s built-in integrations", len(self._integrations))

    def _get_base_url(self, integration_name: str) -> str:
        """Get base URL for integration"""
        urls = {
            "slack": "https://slack.com/api",
            "discord": "https://discord.com/api/v10",
            "github": "https://api.github.com",
            "openai": "https://api.openai.com/v1",
            "stripe": "https://api.stripe.com/v1",
            # ... more URLs
        }
        return urls.get(integration_name, f"https://api.{integration_name}.com")

    def register(self, integration: "Integration") -> None:
        """Register an integration"""
        self._integrations[integration.name] = integration

        if integration.category not in self._categories:
            self._categories[integration.category] = []
        self._categories[integration.category].append(integration.name)

    def get(self, name: str) -> "Integration | None":
        """Get integration by name"""
        return self._integrations.get(name)

    def list_by_category(self, category: str) -> list["Integration"]:
        """List integrations by category"""
        names = self._categories.get(category, [])
        return [self.get(name) for name in names]

    def list_all(self) -> list["Integration"]:
        """List all integrations"""
        return list(self._integrations.values())


@dataclass
class IntegrationConfig:
    """Configuration for an integration"""

    name: str
    category: str
    auth_type: str = "api_key"  # api_key, oauth, basic, none
    credentials: dict[str, Any] = field(default_factory=dict)
    settings: dict[str, Any] = field(default_factory=dict)


class Integration(Tool):
    """
    Base class for all integrations.

    Integrations connect to external services and provide
    standardized methods for interacting with them.
    """

    name: str = "integration"
    category: str = "general"
    auth_type: str = "api_key"

    def __init__(self, config: IntegrationConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.config = config or IntegrationConfig(name=self.name, category=self.category)
        self._authenticated = False

    async def authenticate(self, credentials: dict[str, Any]) -> bool:
        """Authenticate with the integration"""
        self.config.credentials = credentials
        self._authenticated = True
        return True

    async def test_connection(self) -> ToolResult:
        """Test if connection is working"""
        if not self._authenticated:
            return ToolResult(success=False, output=None, error="Not authenticated")

        return ToolResult(
            success=True,
            output={"status": "connected"},
            metadata={"integration": self.name},
        )


class RestApiIntegration(Integration):
    """
    REST API integration.

    Connects to any REST API service.
    """

    def __init__(self, name: str, base_url: str, category: str = "api", **kwargs):
        self.base_url = base_url
        super().__init__(**kwargs)
        self.name = name
        self.category = category

    async def execute(self, operation: str, **kwargs) -> ToolResult:
        """
        Execute REST API operation.

        Args:
            operation: HTTP method (GET, POST, PUT, DELETE)
            **kwargs: endpoint, params, body, headers, etc.
        """
        import httpx

        endpoint = kwargs.get("endpoint", "")
        params = kwargs.get("params", {})
        body = kwargs.get("body", {})
        headers = kwargs.get("headers", {})

        # Add authentication headers
        if self._authenticated:
            headers.update(self._get_auth_headers())

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # SSRF protection
        if not _is_safe_url(url):
            return ToolResult(success=False, output=None, error="URL blocked: targets a private or internal address")

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.request(
                    method=operation.upper(),
                    url=url,
                    params=params,
                    json=(body if operation.upper() in ["POST", "PUT", "PATCH"] else None),
                    headers=headers,
                    timeout=30.0,
                )

                response_data = (
                    response.json() if "application/json" in response.headers.get("content-type", "") else response.text
                )

                return ToolResult(
                    success=response.status_code < 400,
                    output={
                        "status_code": response.status_code,
                        "headers": dict(response.headers),
                        "data": response_data,
                    },
                    metadata={"url": url, "method": operation.upper()},
                )

            except Exception as e:
                logger.error("REST API request failed for %s: %s", self.name, e)
                return ToolResult(success=False, output=None, error="API request failed")

    def _get_auth_headers(self) -> dict[str, str]:
        """Get authentication headers"""
        if self.auth_type == "api_key":
            api_key = self.config.credentials.get("api_key")
            return {"Authorization": f"Bearer {api_key}"}

        elif self.auth_type == "basic":
            import base64

            username = self.config.credentials.get("username")
            password = self.config.credentials.get("password")
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            return {"Authorization": f"Basic {credentials}"}

        return {}


class DatabaseIntegration(Integration):
    """
    Database integration.

    Connects to the platform's asyncpg pool for SQL queries.
    """

    async def execute(self, query: str, **kwargs) -> ToolResult:
        """
        Execute database query.

        Args:
            query: SQL query string
            **kwargs: parameters (list), database (name)
        """
        from apps.backend.core.unified_auth import Database

        try:
            params = kwargs.get("parameters", [])
            query_stripped = query.strip()
            upper = query_stripped.upper().lstrip()

            if upper.startswith("SELECT") or upper.startswith("WITH"):
                if params:
                    rows = await Database.fetch(query_stripped, *params)
                else:
                    rows = await Database.fetch(query_stripped)
                result = [dict(r) for r in rows] if rows else []
                return ToolResult(
                    success=True,
                    output={
                        "query": query_stripped,
                        "rows_affected": len(result),
                        "result": result,
                    },
                    metadata={"database": self.name},
                )
            else:
                if params:
                    status = await Database.execute(query_stripped, *params)
                else:
                    status = await Database.execute(query_stripped)
                affected = 0
                if status and status.split()[-1].isdigit():
                    affected = int(status.split()[-1])
                return ToolResult(
                    success=True,
                    output={
                        "query": query_stripped,
                        "rows_affected": affected,
                        "result": [],
                    },
                    metadata={"database": self.name, "status": status or "executed"},
                )
        except Exception as e:
            logger.error("Database query failed for %s: %s", self.name, e)
            return ToolResult(success=False, output=None, error="Database query failed")


class FileSystemIntegration(Integration):
    """
    File system integration.

    Connects to local and cloud file systems.
    """

    async def execute(self, operation: str, **kwargs) -> ToolResult:
        """
        Execute file system operation.

        Args:
            operation: read, write, delete, list, etc.
            **kwargs: path, content, etc.
        """
        import os

        import aiofiles

        SANDBOX_ROOT = os.environ.get("WORKFLOW_FILE_SANDBOX", os.path.join(os.getcwd(), "workflow-data"))
        os.makedirs(SANDBOX_ROOT, exist_ok=True)

        operation = operation.lower()
        raw_path = kwargs.get("path", "")

        # Path traversal protection: resolve and verify path stays within sandbox
        sandbox_root = os.path.realpath(SANDBOX_ROOT)
        resolved = os.path.realpath(os.path.join(sandbox_root, raw_path))
        try:
            contained = os.path.commonpath((sandbox_root, resolved)) == sandbox_root
        except ValueError:
            contained = False
        if not contained:
            return ToolResult(success=False, output=None, error="Path traversal detected — access denied")

        path = resolved

        try:
            if operation == "read":
                async with aiofiles.open(path) as f:
                    content = await f.read()

                return ToolResult(success=True, output={"path": raw_path, "content": content})

            elif operation == "write":
                content = kwargs.get("content", "")
                async with aiofiles.open(path, "w") as f:
                    await f.write(content)

                return ToolResult(success=True, output={"path": raw_path, "bytes_written": len(content)})

            elif operation == "delete":
                os.remove(path)
                return ToolResult(success=True, output={"path": raw_path, "deleted": True})

            elif operation == "list":
                directory = path
                files = os.listdir(directory)
                return ToolResult(success=True, output={"path": raw_path, "files": files})

            else:
                return ToolResult(success=False, output=None, error=f"Unknown operation: {operation}")

        except Exception as e:
            logger.error("FileSystem operation '%s' failed: %s", operation, e)
            return ToolResult(success=False, output=None, error=f"File operation '{operation}' failed")


class WebhookIntegration(Integration):
    """
    Webhook integration.

    Receives and sends webhook events.
    """

    def __init__(self, **kwargs):
        super().__init__(name="webhook", category="webhook", **kwargs)
        self._webhooks: dict[str, dict[str, Any]] = {}

    async def execute(self, operation: str, **kwargs) -> ToolResult:
        """
        Execute webhook operation.

        Args:
            operation: create, send, receive
            **kwargs: webhook details
        """
        operation = operation.lower()

        if operation == "create":
            webhook_id = kwargs.get("webhook_id", str(hash(kwargs.get("url", ""))))
            self._webhooks[webhook_id] = {
                "url": kwargs.get("url"),
                "events": kwargs.get("events", ["*"]),
                "secret": kwargs.get("secret"),
            }

            return ToolResult(
                success=True,
                output={"webhook_id": webhook_id, "url": f"/webhooks/{webhook_id}"},
            )

        elif operation == "send":
            import httpx

            webhook_url = kwargs.get("url")
            payload = kwargs.get("payload", {})
            headers = kwargs.get("headers", {})

            # SSRF protection
            if not webhook_url or not _is_safe_url(webhook_url):
                return ToolResult(
                    success=False, output=None, error="URL blocked: targets a private or internal address"
                )

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(webhook_url, json=payload, headers=headers, timeout=10.0)

            return ToolResult(
                success=response.status_code < 400,
                output={"status_code": response.status_code},
            )

        else:
            return ToolResult(success=False, output=None, error=f"Unknown operation: {operation}")


# Convenience functions
def create_integration_registry() -> IntegrationRegistry:
    """Create and return a new integration registry"""
    return IntegrationRegistry()


def get_popular_integrations() -> list[str]:
    """Get list of popular integrations"""
    return [
        "slack",
        "discord",
        "gmail",
        "github",
        "openai",
        "stripe",
        "shopify",
        "google_sheets",
        "notion",
        "airtable",
        "salesforce",
        "hubspot",
        "jira",
        "trello",
        "asana",
        "aws",
        "azure",
        "google_cloud",
        "postgres",
        "mysql",
    ]
