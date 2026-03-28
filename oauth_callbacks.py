"""
OAuth Callback Handlers for Helix Spirals Integrations

Handles the OAuth 2.0 callback flow for third-party integrations.
"""

import json
import logging
import os
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import RedirectResponse

from apps.backend.core.exceptions import OAuthTokenError
from apps.backend.core.redis_client import get_redis

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/integrations/oauth", tags=["OAuth Callbacks"])

# OAuth state storage via Redis (key prefix + 600s TTL)
_OAUTH_STATE_PREFIX = "helix:oauth:state:"
_OAUTH_STATE_TTL = 600  # 10 minutes

# OAuth Provider Configuration
OAUTH_PROVIDERS = {
    "slack": {
        "client_id": os.getenv("SLACK_CLIENT_ID", ""),
        "client_secret": os.getenv("SLACK_CLIENT_SECRET", ""),
        "authorize_url": "https://slack.com/oauth/v2/authorize",
        "token_url": "https://slack.com/api/oauth.v2.access",
        "scopes": ["chat:write", "channels:read", "users:read"],
    },
    "google": {
        "client_id": os.getenv("GOOGLE_CLIENT_ID", ""),
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET", ""),
        "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "scopes": [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/gmail.send",
        ],
    },
    "github": {
        "client_id": os.getenv("GITHUB_CLIENT_ID", ""),
        "client_secret": os.getenv("GITHUB_CLIENT_SECRET", ""),
        "authorize_url": "https://github.com/login/oauth/authorize",
        "token_url": "https://github.com/login/oauth/access_token",
        "scopes": ["repo", "user:email"],
    },
    "notion": {
        "client_id": os.getenv("NOTION_CLIENT_ID", ""),
        "client_secret": os.getenv("NOTION_CLIENT_SECRET", ""),
        "authorize_url": "https://api.notion.com/v1/oauth/authorize",
        "token_url": "https://api.notion.com/v1/oauth/token",
        "scopes": [],  # Notion uses owner type
    },
    "hubspot": {
        "client_id": os.getenv("HUBSPOT_CLIENT_ID", ""),
        "client_secret": os.getenv("HUBSPOT_CLIENT_SECRET", ""),
        "authorize_url": "https://app.hubspot.com/oauth/authorize",
        "token_url": "https://api.hubapi.com/oauth/v1/token",
        "scopes": ["crm.objects.contacts.write", "crm.objects.deals.write"],
    },
    "trello": {
        "client_id": os.getenv("TRELLO_API_KEY", ""),  # Trello uses API Key
        "client_secret": os.getenv("TRELLO_SECRET", ""),
        "authorize_url": "https://trello.com/1/authorize",
        "token_url": "https://trello.com/1/OAuthGetAccessToken",
        "scopes": ["read", "write"],
    },
}


def get_redirect_uri(provider: str) -> str:
    """Get the OAuth callback redirect URI."""
    base_url = os.getenv("BASE_URL", "")
    return f"{base_url}/api/integrations/oauth/callback/{provider}"


# =============================================================================
# INITIATE OAUTH FLOW
# =============================================================================


@router.get("/connect/{provider}")
async def initiate_oauth(
    provider: str,
    user_id: str = Query(..., description="User ID initiating the connection"),
    redirect_after: str = Query("/spirals/integrations", description="URL to redirect to after OAuth"),
):
    """
    Initiate OAuth flow for a provider.

    Redirects user to the provider's authorization page.
    """
    # Prevent open redirect: only allow relative paths starting with /
    if not redirect_after.startswith("/") or redirect_after.startswith("//"):
        redirect_after = "/spirals/integrations"

    if provider not in OAUTH_PROVIDERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Provider '{provider}' not supported",
        )

    config = OAUTH_PROVIDERS[provider]

    if not config["client_id"]:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=f"OAuth for {provider} is not currently available",
        )

    # Generate state token for CSRF protection
    state = secrets.token_urlsafe(32)

    # Store state in Redis with TTL
    state_data = {
        "user_id": user_id,
        "provider": provider,
        "redirect_after": redirect_after,
        "created_at": datetime.now(UTC).isoformat(),
    }
    try:
        r = await get_redis()
        if r:
            await r.set(
                "%s%s" % (_OAUTH_STATE_PREFIX, state),
                json.dumps(state_data),
                ex=_OAUTH_STATE_TTL,
            )
        else:
            logger.warning("Redis unavailable — OAuth state will not persist across instances")
    except Exception as exc:
        logger.warning("Failed to store OAuth state in Redis: %s", exc)

    # Build authorization URL
    redirect_uri = get_redirect_uri(provider)

    params = {
        "client_id": config["client_id"],
        "redirect_uri": redirect_uri,
        "state": state,
        "response_type": "code",
    }

    # Provider-specific parameters
    if provider == "google":
        params["scope"] = " ".join(config["scopes"])
        params["access_type"] = "offline"
        params["prompt"] = "consent"
    elif provider == "slack":
        params["scope"] = ",".join(config["scopes"])
    elif provider == "github":
        params["scope"] = " ".join(config["scopes"])
    elif provider == "notion":
        params["owner"] = "user"
    elif provider == "hubspot":
        params["scope"] = " ".join(config["scopes"])
    elif provider == "trello":
        params["key"] = config["client_id"]
        params["name"] = "Helix Spirals"
        params["expiration"] = "never"
        params["scope"] = ",".join(config["scopes"])
        params["callback_method"] = "fragment"

    auth_url = f"{config['authorize_url']}?{urlencode(params)}"

    logger.info("Initiating OAuth for %s, user %s", provider, user_id)

    return RedirectResponse(url=auth_url, status_code=302)


# =============================================================================
# OAUTH CALLBACK
# =============================================================================


@router.get("/callback/{provider}")
async def oauth_callback(
    provider: str,
    code: str = Query(None, description="Authorization code"),
    state: str = Query(None, description="State token"),
    error: str = Query(None, description="Error from provider"),
    error_description: str = Query(None, description="Error description"),
):
    """
    Handle OAuth callback from provider.

    Exchanges authorization code for access token and stores it.
    """
    # Handle errors from provider
    if error:
        logger.error("OAuth error from %s: %s - %s", provider, error, error_description)
        return RedirectResponse(
            url=f"/spirals/integrations?error={error}&provider={provider}",
            status_code=302,
        )

    # Validate state via Redis
    state_data = None
    try:
        r = await get_redis()
        if r:
            raw = await r.get("%s%s" % (_OAUTH_STATE_PREFIX, state))
            if raw:
                state_data = json.loads(raw)
                # Delete after retrieval (one-time use)
                await r.delete("%s%s" % (_OAUTH_STATE_PREFIX, state))
    except Exception as exc:
        logger.warning("Failed to read OAuth state from Redis: %s", exc)

    if state_data is None:
        logger.warning("Invalid OAuth state for %s", provider)
        return RedirectResponse(
            url="/spirals/integrations?error=invalid_state",
            status_code=302,
        )

    # Check state expiry (10 minutes)
    created_at = datetime.fromisoformat(state_data["created_at"])
    if datetime.now(UTC) - created_at > timedelta(minutes=10):
        logger.warning("Expired OAuth state for %s", provider)
        return RedirectResponse(
            url="/spirals/integrations?error=state_expired",
            status_code=302,
        )

    user_id = state_data["user_id"]
    redirect_after = state_data["redirect_after"]

    # Exchange code for tokens
    try:
        tokens = await exchange_code_for_tokens(provider, code)

        if tokens:
            # Store tokens (in production, use encrypted storage)
            await store_user_tokens(user_id, provider, tokens)

            logger.info("Successfully connected %s for user %s", provider, user_id)

            return RedirectResponse(
                url=f"{redirect_after}?success=true&provider={provider}",
                status_code=302,
            )
        else:
            raise OAuthTokenError("Failed to exchange code for tokens")

    except Exception as e:
        logger.exception("OAuth token exchange failed for %s: %s", provider, e)
        return RedirectResponse(
            url=f"{redirect_after}?error=token_exchange_failed&provider={provider}",
            status_code=302,
        )


async def exchange_code_for_tokens(provider: str, code: str) -> dict[str, Any] | None:
    """Exchange authorization code for access tokens."""
    config = OAUTH_PROVIDERS[provider]
    redirect_uri = get_redirect_uri(provider)

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Build token request
        if provider == "slack":
            response = await client.post(
                config["token_url"],
                data={
                    "client_id": config["client_id"],
                    "client_secret": config["client_secret"],
                    "code": code,
                    "redirect_uri": redirect_uri,
                },
            )
        elif provider == "google":
            response = await client.post(
                config["token_url"],
                data={
                    "client_id": config["client_id"],
                    "client_secret": config["client_secret"],
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "grant_type": "authorization_code",
                },
            )
        elif provider == "github":
            response = await client.post(
                config["token_url"],
                headers={"Accept": "application/json"},
                data={
                    "client_id": config["client_id"],
                    "client_secret": config["client_secret"],
                    "code": code,
                    "redirect_uri": redirect_uri,
                },
            )
        elif provider == "notion":
            # Notion uses Basic Auth
            import base64

            credentials = base64.b64encode(f"{config['client_id']}:{config['client_secret']}".encode()).decode()
            response = await client.post(
                config["token_url"],
                headers={
                    "Authorization": f"Basic {credentials}",
                    "Content-Type": "application/json",
                },
                json={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                },
            )
        elif provider == "hubspot":
            response = await client.post(
                config["token_url"],
                data={
                    "grant_type": "authorization_code",
                    "client_id": config["client_id"],
                    "client_secret": config["client_secret"],
                    "redirect_uri": redirect_uri,
                    "code": code,
                },
            )
        else:
            # Generic OAuth 2.0
            response = await client.post(
                config["token_url"],
                data={
                    "client_id": config["client_id"],
                    "client_secret": config["client_secret"],
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "grant_type": "authorization_code",
                },
            )

        if response.status_code == 200:
            return response.json()
        else:
            logger.error("Token exchange failed for %s: %s - %s", provider, response.status_code, response.text)
            return None


async def store_user_tokens(user_id: str, provider: str, tokens: dict[str, Any]) -> None:
    """Store user tokens securely — writes through to Redis."""
    from .integration_routes import _connections_redis_get, _connections_redis_set, _user_connections

    # Load existing connections from Redis if not cached
    if user_id not in _user_connections:
        redis_conns = await _connections_redis_get(user_id)
        _user_connections[user_id] = redis_conns if redis_conns else {}

    _user_connections[user_id][provider] = {
        "credentials": {
            "access_token": tokens.get("access_token"),
            "refresh_token": tokens.get("refresh_token"),
            "expires_in": tokens.get("expires_in"),
            "token_type": tokens.get("token_type", "Bearer"),
        },
        "connected_at": datetime.now(UTC).isoformat(),
    }

    # Write through to Redis for persistence
    await _connections_redis_set(user_id, _user_connections[user_id])

    logger.info("Stored tokens for user %s, provider %s", user_id, provider)


# =============================================================================
# DISCONNECT
# =============================================================================


@router.delete("/disconnect/{provider}")
async def disconnect_oauth(
    provider: str,
    user_id: str = Query(..., description="User ID"),
):
    """
    Disconnect an OAuth integration.

    Removes stored tokens for the provider.
    """
    from .integration_routes import _connections_redis_get, _connections_redis_set, _user_connections

    # Load from Redis if not cached
    if user_id not in _user_connections:
        redis_conns = await _connections_redis_get(user_id)
        if redis_conns:
            _user_connections[user_id] = redis_conns

    if user_id in _user_connections and provider in _user_connections[user_id]:
        del _user_connections[user_id][provider]
        await _connections_redis_set(user_id, _user_connections[user_id])
        logger.info("Disconnected %s for user %s", provider, user_id)
        return {"success": True, "message": f"Disconnected from {provider}"}

    return {"success": False, "message": f"No connection found for {provider}"}


# =============================================================================
# STATUS CHECK
# =============================================================================


@router.get("/status/{provider}")
async def check_oauth_status(
    provider: str,
    user_id: str = Query(..., description="User ID"),
):
    """
    Check if a user has connected a provider.
    """
    from .integration_routes import _user_connections

    connected = user_id in _user_connections and provider in _user_connections[user_id]

    return {
        "provider": provider,
        "connected": connected,
        "user_id": user_id,
    }


@router.get("/providers")
async def list_oauth_providers():
    """
    List all available OAuth providers and their configuration status.
    """
    providers = []
    for provider, config in OAUTH_PROVIDERS.items():
        providers.append(
            {
                "id": provider,
                "name": provider.replace("_", " ").title(),
                "configured": bool(config.get("client_id")),
                "scopes": config.get("scopes", []),
            }
        )
    return {"providers": providers}
