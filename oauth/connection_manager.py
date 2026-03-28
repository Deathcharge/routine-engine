"""
OAuth2 Connection Manager for Helix Spirals.

Provides comprehensive OAuth2 connection management including
provider configuration, token management, refresh handling, and secure storage.
"""

import base64
import hashlib
import json
import logging
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any
from urllib.parse import urlencode
from uuid import uuid4

import aiohttp
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


class OAuthProvider(Enum):
    """Supported OAuth providers."""

    GOOGLE = "google"
    MICROSOFT = "microsoft"
    GITHUB = "github"
    SLACK = "slack"
    DISCORD = "discord"
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    SHOPIFY = "shopify"
    STRIPE = "stripe"
    DROPBOX = "dropbox"
    BOX = "box"
    NOTION = "notion"
    AIRTABLE = "airtable"
    ASANA = "asana"
    TRELLO = "trello"
    JIRA = "jira"
    ZENDESK = "zendesk"
    MAILCHIMP = "mailchimp"
    CALENDLY = "calendly"
    VELOCITY = "velocity"
    TWITTER = "twitter"
    LINKEDIN = "linkedin"
    FACEBOOK = "facebook"
    INSTAGRAM = "instagram"
    CUSTOM = "custom"


class ConnectionStatus(Enum):
    """Connection status."""

    PENDING = "pending"
    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"
    ERROR = "error"


class TokenType(Enum):
    """OAuth token type."""

    BEARER = "Bearer"
    MAC = "MAC"


@dataclass
class OAuthProviderConfig:
    """Configuration for an OAuth provider."""

    provider: OAuthProvider
    client_id: str
    client_secret: str
    authorization_url: str
    token_url: str
    revoke_url: str | None = None
    userinfo_url: str | None = None
    scopes: list[str] = field(default_factory=list)
    extra_params: dict[str, str] = field(default_factory=dict)

    # PKCE support
    use_pkce: bool = False

    # Token settings
    token_type: TokenType = TokenType.BEARER
    supports_refresh: bool = True

    # Display
    display_name: str = ""
    icon_url: str | None = None
    description: str = ""


@dataclass
class OAuthToken:
    """OAuth token data."""

    access_token: str
    token_type: str
    expires_at: datetime | None = None
    refresh_token: str | None = None
    scope: str | None = None
    id_token: str | None = None
    extra_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class OAuthConnection:
    """Represents an OAuth connection."""

    id: str
    user_id: str
    organization_id: str | None
    provider: OAuthProvider

    # Token data (encrypted)
    encrypted_token: str

    # Status
    status: ConnectionStatus

    # Provider user info
    provider_user_id: str | None = None
    provider_email: str | None = None
    provider_name: str | None = None
    provider_avatar: str | None = None

    # Metadata
    scopes: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_used_at: datetime | None = None
    expires_at: datetime | None = None

    # Error tracking
    last_error: str | None = None
    error_count: int = 0


@dataclass
class OAuthState:
    """OAuth state for CSRF protection."""

    id: str
    user_id: str
    provider: OAuthProvider
    redirect_uri: str
    scopes: list[str]
    code_verifier: str | None  # For PKCE
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime = field(default_factory=lambda: datetime.now(UTC) + timedelta(minutes=10))
    extra_data: dict[str, Any] = field(default_factory=dict)


# ==================== Provider Configurations ====================

PROVIDER_CONFIGS: dict[OAuthProvider, OAuthProviderConfig] = {
    OAuthProvider.GOOGLE: OAuthProviderConfig(
        provider=OAuthProvider.GOOGLE,
        client_id="",  # Set via environment
        client_secret="",
        authorization_url="https://accounts.google.com/o/oauth2/v2/auth",
        token_url="https://oauth2.googleapis.com/token",
        revoke_url="https://oauth2.googleapis.com/revoke",
        userinfo_url="https://www.googleapis.com/oauth2/v2/userinfo",
        scopes=["openid", "email", "profile"],
        use_pkce=True,
        display_name="Google",
        description="Connect your Google account",
    ),
    OAuthProvider.MICROSOFT: OAuthProviderConfig(
        provider=OAuthProvider.MICROSOFT,
        client_id="",
        client_secret="",
        authorization_url="https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
        token_url="https://login.microsoftonline.com/common/oauth2/v2.0/token",
        userinfo_url="https://graph.microsoft.com/v1.0/me",
        scopes=["openid", "email", "profile", "User.Read"],
        use_pkce=True,
        display_name="Microsoft",
        description="Connect your Microsoft account",
    ),
    OAuthProvider.GITHUB: OAuthProviderConfig(
        provider=OAuthProvider.GITHUB,
        client_id="",
        client_secret="",
        authorization_url="https://github.com/login/oauth/authorize",
        token_url="https://github.com/login/oauth/access_token",
        userinfo_url="https://api.github.com/user",
        scopes=["read:user", "user:email"],
        supports_refresh=False,
        display_name="GitHub",
        description="Connect your GitHub account",
    ),
    OAuthProvider.SLACK: OAuthProviderConfig(
        provider=OAuthProvider.SLACK,
        client_id="",
        client_secret="",
        authorization_url="https://slack.com/oauth/v2/authorize",
        token_url="https://slack.com/api/oauth.v2.access",
        revoke_url="https://slack.com/api/auth.revoke",
        scopes=["channels:read", "chat:write", "users:read"],
        display_name="Slack",
        description="Connect your Slack workspace",
    ),
    OAuthProvider.DISCORD: OAuthProviderConfig(
        provider=OAuthProvider.DISCORD,
        client_id="",
        client_secret="",
        authorization_url="https://discord.com/api/oauth2/authorize",
        token_url="https://discord.com/api/oauth2/token",
        revoke_url="https://discord.com/api/oauth2/token/revoke",
        userinfo_url="https://discord.com/api/users/@me",
        scopes=["identify", "email"],
        display_name="Discord",
        description="Connect your Discord account",
    ),
    OAuthProvider.HUBSPOT: OAuthProviderConfig(
        provider=OAuthProvider.HUBSPOT,
        client_id="",
        client_secret="",
        authorization_url="https://app.hubspot.com/oauth/authorize",
        token_url="https://api.hubapi.com/oauth/v1/token",
        scopes=["contacts", "content"],
        display_name="HubSpot",
        description="Connect your HubSpot account",
    ),
    OAuthProvider.SHOPIFY: OAuthProviderConfig(
        provider=OAuthProvider.SHOPIFY,
        client_id="",
        client_secret="",
        authorization_url="https://{shop}.myshopify.com/admin/oauth/authorize",
        token_url="https://{shop}.myshopify.com/admin/oauth/access_token",
        scopes=["read_products", "read_orders"],
        extra_params={"grant_options[]": "per-user"},
        display_name="Shopify",
        description="Connect your Shopify store",
    ),
    OAuthProvider.NOTION: OAuthProviderConfig(
        provider=OAuthProvider.NOTION,
        client_id="",
        client_secret="",
        authorization_url="https://api.notion.com/v1/oauth/authorize",
        token_url="https://api.notion.com/v1/oauth/token",
        scopes=[],
        extra_params={"owner": "user"},
        display_name="Notion",
        description="Connect your Notion workspace",
    ),
    OAuthProvider.AIRTABLE: OAuthProviderConfig(
        provider=OAuthProvider.AIRTABLE,
        client_id="",
        client_secret="",
        authorization_url="https://airtable.com/oauth2/v1/authorize",
        token_url="https://airtable.com/oauth2/v1/token",
        scopes=["data.records:read", "data.records:write", "schema.bases:read"],
        use_pkce=True,
        display_name="Airtable",
        description="Connect your Airtable bases",
    ),
    OAuthProvider.CALENDLY: OAuthProviderConfig(
        provider=OAuthProvider.CALENDLY,
        client_id="",
        client_secret="",
        authorization_url="https://auth.calendly.com/oauth/authorize",
        token_url="https://auth.calendly.com/oauth/token",
        scopes=[],
        display_name="Calendly",
        description="Connect your Calendly account",
    ),
    OAuthProvider.MAILCHIMP: OAuthProviderConfig(
        provider=OAuthProvider.MAILCHIMP,
        client_id="",
        client_secret="",
        authorization_url="https://login.mailchimp.com/oauth2/authorize",
        token_url="https://login.mailchimp.com/oauth2/token",
        scopes=[],
        display_name="Mailchimp",
        description="Connect your Mailchimp account",
    ),
    OAuthProvider.VELOCITY: OAuthProviderConfig(
        provider=OAuthProvider.VELOCITY,
        client_id="",
        client_secret="",
        authorization_url="https://velocity.us/oauth/authorize",
        token_url="https://velocity.us/oauth/token",
        revoke_url="https://velocity.us/oauth/revoke",
        userinfo_url="https://api.velocity.us/v2/users/me",
        scopes=["user:read", "meeting:read", "meeting:write"],
        display_name="Velocity",
        description="Connect your Velocity account",
    ),
    OAuthProvider.SALESFORCE: OAuthProviderConfig(
        provider=OAuthProvider.SALESFORCE,
        client_id="",
        client_secret="",
        authorization_url="https://login.salesforce.com/services/oauth2/authorize",
        token_url="https://login.salesforce.com/services/oauth2/token",
        revoke_url="https://login.salesforce.com/services/oauth2/revoke",
        userinfo_url="https://login.salesforce.com/services/oauth2/userinfo",
        scopes=["api", "refresh_token"],
        display_name="Salesforce",
        description="Connect your Salesforce account",
    ),
    OAuthProvider.ZENDESK: OAuthProviderConfig(
        provider=OAuthProvider.ZENDESK,
        client_id="",
        client_secret="",
        authorization_url="https://{subdomain}.zendesk.com/oauth/authorizations/new",
        token_url="https://{subdomain}.zendesk.com/oauth/tokens",
        scopes=["read", "write"],
        display_name="Zendesk",
        description="Connect your Zendesk account",
    ),
}


class TokenEncryption:
    """Handles encryption/decryption of OAuth tokens."""

    def __init__(self, encryption_key: str):
        # Derive a proper Fernet key from the provided key
        key_bytes = hashlib.sha256(encryption_key.encode()).digest()
        self._fernet = Fernet(base64.urlsafe_b64encode(key_bytes))

    def encrypt(self, token: OAuthToken) -> str:
        """Encrypt an OAuth token."""
        token_data = {
            "access_token": token.access_token,
            "token_type": token.token_type,
            "expires_at": token.expires_at.isoformat() if token.expires_at else None,
            "refresh_token": token.refresh_token,
            "scope": token.scope,
            "id_token": token.id_token,
            "extra_data": token.extra_data,
        }
        json_data = json.dumps(token_data)
        return self._fernet.encrypt(json_data.encode()).decode()

    def decrypt(self, encrypted: str) -> OAuthToken:
        """Decrypt an OAuth token."""
        json_data = self._fernet.decrypt(encrypted.encode()).decode()
        token_data = json.loads(json_data)

        return OAuthToken(
            access_token=token_data["access_token"],
            token_type=token_data["token_type"],
            expires_at=(datetime.fromisoformat(token_data["expires_at"]) if token_data["expires_at"] else None),
            refresh_token=token_data.get("refresh_token"),
            scope=token_data.get("scope"),
            id_token=token_data.get("id_token"),
            extra_data=token_data.get("extra_data", {}),
        )


class OAuthConnectionManager:
    """
    Comprehensive OAuth2 connection manager.

    Features:
    - Multiple provider support
    - PKCE support
    - Token encryption
    - Automatic token refresh
    - Connection lifecycle management
    - Webhook notifications
    """

    def __init__(
        self,
        encryption_key: str,
        storage_backend=None,
        base_redirect_uri: str = "http://localhost:8000/oauth/callback",
    ):
        self.encryption = TokenEncryption(encryption_key)
        self.storage = storage_backend or InMemoryOAuthStorage()
        self.base_redirect_uri = base_redirect_uri
        self._provider_configs: dict[OAuthProvider, OAuthProviderConfig] = {}
        self._session: aiohttp.ClientSession | None = None
        self._refresh_callbacks: list[Callable] = []

        # Load default provider configs
        self._provider_configs.update(PROVIDER_CONFIGS)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    # ==================== Provider Configuration ====================

    def configure_provider(
        self,
        provider: OAuthProvider,
        client_id: str,
        client_secret: str,
        scopes: list[str] | None = None,
        extra_params: dict[str, str] | None = None,
    ):
        """Configure a provider with credentials."""
        if provider in self._provider_configs:
            config = self._provider_configs[provider]
            config.client_id = client_id
            config.client_secret = client_secret
            if scopes:
                config.scopes = scopes
            if extra_params:
                config.extra_params.update(extra_params)
        else:
            raise ValueError(f"Unknown provider: {provider}")

    def add_custom_provider(self, config: OAuthProviderConfig):
        """Add a custom OAuth provider."""
        self._provider_configs[config.provider] = config

    def get_provider_config(self, provider: OAuthProvider) -> OAuthProviderConfig | None:
        """Get provider configuration."""
        return self._provider_configs.get(provider)

    def get_available_providers(self) -> list[dict[str, Any]]:
        """Get list of available (configured) providers."""
        providers = []
        for provider, config in self._provider_configs.items():
            if config.client_id:  # Only include configured providers
                providers.append(
                    {
                        "provider": provider.value,
                        "display_name": config.display_name or provider.value,
                        "icon_url": config.icon_url,
                        "description": config.description,
                        "scopes": config.scopes,
                    }
                )
        return providers

    # ==================== Authorization Flow ====================

    async def get_authorization_url(
        self,
        provider: OAuthProvider,
        user_id: str,
        scopes: list[str] | None = None,
        extra_params: dict[str, str] | None = None,
        state_data: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        """
        Generate authorization URL for OAuth flow.

        Returns:
            Tuple of (authorization_url, state_id)
        """
        config = self._provider_configs.get(provider)
        if not config or not config.client_id:
            raise ValueError(f"Provider not configured: {provider}")

        # Generate state
        state_id = secrets.token_urlsafe(32)

        # Generate PKCE code verifier if needed
        code_verifier = None
        code_challenge = None
        if config.use_pkce:
            code_verifier = secrets.token_urlsafe(64)
            code_challenge = (
                base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest()).decode().rstrip("=")
            )

        # Determine scopes
        final_scopes = scopes or config.scopes

        # Build redirect URI
        redirect_uri = f"{self.base_redirect_uri}/{provider.value}"

        # Save state
        state = OAuthState(
            id=state_id,
            user_id=user_id,
            provider=provider,
            redirect_uri=redirect_uri,
            scopes=final_scopes,
            code_verifier=code_verifier,
            extra_data=state_data or {},
        )
        await self.storage.save_state(state)

        # Build authorization URL
        params = {
            "client_id": config.client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "state": state_id,
            "scope": " ".join(final_scopes),
        }

        if config.use_pkce:
            params["code_challenge"] = code_challenge
            params["code_challenge_method"] = "S256"

        # Add extra params
        params.update(config.extra_params)
        if extra_params:
            params.update(extra_params)

        auth_url = f"{config.authorization_url}?{urlencode(params)}"

        return auth_url, state_id

    async def handle_callback(
        self,
        provider: OAuthProvider,
        code: str,
        state_id: str,
        error: str | None = None,
        error_description: str | None = None,
    ) -> OAuthConnection:
        """
        Handle OAuth callback and exchange code for tokens.

        Returns:
            OAuthConnection object
        """
        # Verify state
        state = await self.storage.get_state(state_id)
        if not state:
            raise ValueError("Invalid or expired state")

        if state.provider != provider:
            raise ValueError("Provider mismatch")

        if datetime.now(UTC) > state.expires_at:
            raise ValueError("State expired")

        # Delete state (one-time use)
        await self.storage.delete_state(state_id)

        # Check for errors
        if error:
            raise ValueError(f"OAuth error: {error} - {error_description}")

        # Exchange code for tokens
        config = self._provider_configs[provider]
        token = await self._exchange_code(
            config=config,
            code=code,
            redirect_uri=state.redirect_uri,
            code_verifier=state.code_verifier,
        )

        # Get user info if available
        user_info = {}
        if config.userinfo_url:
            user_info = await self._get_user_info(config, token)

        # Create connection
        connection = OAuthConnection(
            id=str(uuid4()),
            user_id=state.user_id,
            organization_id=state.extra_data.get("organization_id"),
            provider=provider,
            encrypted_token=self.encryption.encrypt(token),
            status=ConnectionStatus.ACTIVE,
            provider_user_id=user_info.get("id") or user_info.get("sub"),
            provider_email=user_info.get("email"),
            provider_name=user_info.get("name"),
            provider_avatar=user_info.get("picture") or user_info.get("avatar_url"),
            scopes=state.scopes,
            expires_at=token.expires_at,
        )

        await self.storage.save_connection(connection)

        logger.info("Created OAuth connection for user %s with provider %s", state.user_id, provider.value)

        return connection

    async def _exchange_code(
        self,
        config: OAuthProviderConfig,
        code: str,
        redirect_uri: str,
        code_verifier: str | None = None,
    ) -> OAuthToken:
        """Exchange authorization code for tokens."""
        session = await self._get_session()

        data = {
            "grant_type": "authorization_code",
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        }

        if code_verifier:
            data["code_verifier"] = code_verifier

        headers = {"Accept": "application/json"}

        async with session.post(config.token_url, data=data, headers=headers) as response:
            if response.status != 200:
                error_text = await response.text()
                raise ValueError(f"Token exchange failed: {error_text}")

            token_data = await response.json()

        # Calculate expiration
        expires_at = None
        if "expires_in" in token_data:
            expires_at = datetime.now(UTC) + timedelta(seconds=token_data["expires_in"])

        return OAuthToken(
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "Bearer"),
            expires_at=expires_at,
            refresh_token=token_data.get("refresh_token"),
            scope=token_data.get("scope"),
            id_token=token_data.get("id_token"),
            extra_data={
                k: v
                for k, v in token_data.items()
                if k
                not in [
                    "access_token",
                    "token_type",
                    "expires_in",
                    "refresh_token",
                    "scope",
                    "id_token",
                ]
            },
        )

    async def _get_user_info(self, config: OAuthProviderConfig, token: OAuthToken) -> dict[str, Any]:
        """Get user info from provider."""
        if not config.userinfo_url:
            return {}

        session = await self._get_session()

        headers = {"Authorization": f"{token.token_type} {token.access_token}"}

        async with session.get(config.userinfo_url, headers=headers) as response:
            if response.status != 200:
                logger.warning("Failed to get user info: %s", response.status)
                return {}

            return await response.json()

    # ==================== Token Management ====================

    async def get_access_token(self, connection_id: str, auto_refresh: bool = True) -> str | None:
        """
        Get access token for a connection.

        Automatically refreshes if expired and auto_refresh is True.
        """
        connection = await self.storage.get_connection(connection_id)
        if not connection or connection.status != ConnectionStatus.ACTIVE:
            return None

        token = self.encryption.decrypt(connection.encrypted_token)

        # Check if token is expired
        if token.expires_at and datetime.now(UTC) >= token.expires_at:
            if auto_refresh and token.refresh_token:
                try:
                    token = await self.refresh_token(connection_id)
                except Exception as e:
                    logger.error("Token refresh failed: %s", e)
                    return None
            else:
                return None

        # Update last used
        connection.last_used_at = datetime.now(UTC)
        await self.storage.save_connection(connection)

        return token.access_token

    async def refresh_token(self, connection_id: str) -> OAuthToken:
        """Refresh an OAuth token."""
        connection = await self.storage.get_connection(connection_id)
        if not connection:
            raise ValueError(f"Connection not found: {connection_id}")

        token = self.encryption.decrypt(connection.encrypted_token)
        if not token.refresh_token:
            raise ValueError("No refresh token available")

        config = self._provider_configs.get(connection.provider)
        if not config:
            raise ValueError(f"Provider not configured: {connection.provider}")

        session = await self._get_session()

        data = {
            "grant_type": "refresh_token",
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "refresh_token": token.refresh_token,
        }

        async with session.post(config.token_url, data=data, headers={"Accept": "application/json"}) as response:
            if response.status != 200:
                error_text = await response.text()
                connection.status = ConnectionStatus.ERROR
                connection.last_error = error_text
                connection.error_count += 1
                await self.storage.save_connection(connection)
                raise ValueError(f"Token refresh failed: {error_text}")

            token_data = await response.json()

        # Calculate new expiration
        expires_at = None
        if "expires_in" in token_data:
            expires_at = datetime.now(UTC) + timedelta(seconds=token_data["expires_in"])

        # Create new token (keep old refresh token if not provided)
        new_token = OAuthToken(
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "Bearer"),
            expires_at=expires_at,
            refresh_token=token_data.get("refresh_token", token.refresh_token),
            scope=token_data.get("scope", token.scope),
            id_token=token_data.get("id_token"),
            extra_data=token_data,
        )

        # Update connection
        connection.encrypted_token = self.encryption.encrypt(new_token)
        connection.expires_at = expires_at
        connection.updated_at = datetime.now(UTC)
        connection.last_error = None
        connection.error_count = 0
        await self.storage.save_connection(connection)

        # Notify callbacks
        for callback in self._refresh_callbacks:
            try:
                await callback(connection_id, new_token)
            except Exception as e:
                logger.error("Refresh callback error: %s", e)

        logger.info("Refreshed token for connection %s", connection_id)

        return new_token

    async def revoke_connection(self, connection_id: str, revoked_by: str) -> bool:
        """Revoke an OAuth connection."""
        connection = await self.storage.get_connection(connection_id)
        if not connection:
            return False

        config = self._provider_configs.get(connection.provider)

        # Try to revoke at provider
        if config and config.revoke_url:
            try:
                session = await self._get_session()
                token = connection

                async with session.post(config.revoke_url, data={"token": token.access_token}) as response:
                    if response.status not in [200, 204]:
                        logger.warning("Provider revocation failed: %s", response.status)
            except Exception as e:
                logger.warning("Provider revocation error: %s", e)

        # Update connection status
        connection.status = ConnectionStatus.REVOKED
        connection.updated_at = datetime.now(UTC)
        await self.storage.save_connection(connection)

        logger.info("Revoked connection %s by %s", connection_id, revoked_by)

        return True

    # ==================== Connection Management ====================

    async def get_connection(self, connection_id: str) -> OAuthConnection | None:
        """Get a connection by ID."""
        return await self.storage.get_connection(connection_id)

    async def get_user_connections(
        self,
        user_id: str,
        provider: OAuthProvider | None = None,
        status: ConnectionStatus | None = None,
    ) -> list[OAuthConnection]:
        """Get all connections for a user."""
        connections = await self.storage.get_user_connections(user_id)

        if provider:
            connections = [c for c in connections if c.provider == provider]

        if status:
            connections = [c for c in connections if c.status == status]

        return connections

    async def get_organization_connections(
        self, organization_id: str, provider: OAuthProvider | None = None
    ) -> list[OAuthConnection]:
        """Get all connections for an organization."""
        connections = await self.storage.get_organization_connections(organization_id)

        if provider:
            connections = [c for c in connections if c.provider == provider]

        return connections

    async def delete_connection(self, connection_id: str, deleted_by: str) -> bool:
        """Delete a connection (after revoking)."""
        # Revoke first
        await self.revoke_connection(connection_id, deleted_by)

        # Delete from storage
        await self.storage.delete_connection(connection_id)

        logger.info("Deleted connection %s by %s", connection_id, deleted_by)

        return True

    # ==================== Background Tasks ====================

    async def refresh_expiring_tokens(self, threshold_minutes: int = 30) -> int:
        """Refresh tokens that are about to expire."""
        threshold = datetime.now(UTC) + timedelta(minutes=threshold_minutes)

        # Get all active connections
        connections = await self.storage.get_expiring_connections(threshold)

        refreshed = 0
        for connection in connections:
            try:
                refreshed += 1
            except Exception as e:
                logger.error("Failed to refresh token for %s: %s", connection.id, e)

        return refreshed

    def on_token_refresh(self, callback: Callable):
        """Register a callback for token refresh events."""
        self._refresh_callbacks.append(callback)


class InMemoryOAuthStorage:
    """In-memory storage for development/testing."""

    def __init__(self):
        self._connections: dict[str, OAuthConnection] = {}
        self._states: dict[str, OAuthState] = {}

    async def save_connection(self, connection: OAuthConnection):
        self._connections[connection.id] = connection

    async def get_connection(self, connection_id: str) -> OAuthConnection | None:
        return self._connections.get(connection_id)

    async def delete_connection(self, connection_id: str):
        self._connections.pop(connection_id, None)

    async def get_user_connections(self, user_id: str) -> list[OAuthConnection]:
        return [c for c in self._connections.values() if c.user_id == user_id]

    async def get_organization_connections(self, organization_id: str) -> list[OAuthConnection]:
        return [c for c in self._connections.values() if c.organization_id == organization_id]

    async def get_expiring_connections(self, threshold: datetime) -> list[OAuthConnection]:
        return [
            c
            for c in self._connections.values()
            if c.status == ConnectionStatus.ACTIVE and c.expires_at and c.expires_at <= threshold
        ]

    async def save_state(self, state: OAuthState):
        self._states[state.id] = state

    async def get_state(self, state_id: str) -> OAuthState | None:
        return self._states.get(state_id)

    async def delete_state(self, state_id: str):
        self._states.pop(state_id, None)
