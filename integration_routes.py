"""
Helix Spirals - Integration API Routes

Provides endpoints for:
- Listing available integrations
- Managing user connections
- Executing integration actions
"""

import json
import logging
import secrets
from datetime import UTC
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel

from apps.backend.core.unified_auth import get_current_user

from .integrations import INTEGRATION_REGISTRY, get_available_integrations

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/spirals/integrations", tags=["Spirals Integrations"])


# ============================================================
# MODELS
# ============================================================


class IntegrationInfo(BaseModel):
    id: str
    name: str
    auth_type: str
    actions: list[str]
    description: str


class ConnectionRequest(BaseModel):
    integration_id: str
    credentials: dict[str, str]


class ActionRequest(BaseModel):
    integration_id: str
    action_name: str
    parameters: dict[str, Any]


class ActionResponse(BaseModel):
    success: bool
    result: Any | None = None
    error: str | None = None


# ============================================================
# ENDPOINTS
# ============================================================


@router.get("/available", response_model=list[IntegrationInfo])
async def list_available_integrations():
    """
    List all available integrations.

    Returns integrations that can be connected with their available actions.
    """
    return get_available_integrations()


@router.get("/{integration_id}")
async def get_integration_details(integration_id: str):
    """
    Get details about a specific integration.
    """
    if integration_id not in INTEGRATION_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Integration '{integration_id}' not found",
        )

    info = INTEGRATION_REGISTRY[integration_id]
    return {
        "id": integration_id,
        "name": integration_id.replace("_", " ").title(),
        "auth_type": info["auth_type"],
        "provider": info.get("provider"),
        "actions": info["actions"],
        "description": info["description"],
    }


@router.get("/{integration_id}/actions")
async def list_integration_actions(integration_id: str):
    """
    List available actions for an integration.
    """
    if integration_id not in INTEGRATION_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Integration '{integration_id}' not found",
        )

    info = INTEGRATION_REGISTRY[integration_id]
    integration_class = info["class"]

    # Extract action details from class methods
    actions = []
    for action_name in info["actions"]:
        method = getattr(integration_class, action_name, None)
        if method:
            actions.append(
                {
                    "name": action_name,
                    "description": method.__doc__ or f"Execute {action_name}",
                }
            )

    return {"integration_id": integration_id, "actions": actions}


@router.post("/execute", response_model=ActionResponse)
async def execute_integration_action(request: ActionRequest):
    """
    Execute an action on an integration.

    Requires valid credentials for the integration.
    This is called by the Spirals engine during workflow execution.
    """
    if request.integration_id not in INTEGRATION_REGISTRY:
        return ActionResponse(
            success=False,
            error=f"Integration '{request.integration_id}' not found",
        )

    info = INTEGRATION_REGISTRY[request.integration_id]

    if request.action_name not in info["actions"]:
        return ActionResponse(
            success=False,
            error=f"Action '{request.action_name}' not available for {request.integration_id}",
        )

    try:
        # Get credentials from parameters
        # In production, these would come from OAuthConnectionManager
        credentials = request.parameters.pop("_credentials", {})

        # Instantiate the integration
        integration_class = info["class"]

        if integration_class is None:
            return ActionResponse(
                success=False,
                error=(
                    f"The '{request.integration_id}' connector does not have a direct "
                    "execution class yet. Use the Zapier or MCP integration hub to run "
                    f"'{request.action_name}', or contact support to request priority implementation."
                ),
            )

        # Check if this is the GenericIntegration stub
        from .integrations.base import GenericIntegration

        if issubclass(integration_class, GenericIntegration) and integration_class is GenericIntegration:
            return ActionResponse(
                success=False,
                error=(
                    f"'{request.integration_id}' is registered but its action connector is "
                    "not yet implemented. Route this action through the Zapier/MCP hub, or "
                    "use the HTTP Request integration with a custom endpoint."
                ),
            )

        if info["auth_type"] == "oauth":
            access_token = credentials.get("access_token", "")
            if request.integration_id == "trello":
                api_key = credentials.get("api_key", "")
                integration = integration_class(access_token, api_key)
            else:
                integration = integration_class(access_token)
        elif info["auth_type"] == "api_key":
            if request.integration_id == "twilio":
                integration = integration_class(
                    credentials.get("account_sid", ""),
                    credentials.get("auth_token", ""),
                )
            else:
                api_key = credentials.get("api_key", "")
                integration = integration_class(api_key)
        else:
            return ActionResponse(
                success=False,
                error=f"Unknown auth type: {info['auth_type']}",
            )

        # Execute the action
        method = getattr(integration, request.action_name)
        result = await method(**request.parameters)

        return ActionResponse(success=True, result=result)

    except Exception as e:
        logger.exception("Integration action failed: %s", e)
        return ActionResponse(success=False, error=str(e))


# ============================================================
# USER CONNECTIONS — Redis-backed with in-memory cache
# ============================================================

_CONNECTIONS_REDIS_KEY = "helix:integrations:connections"
_MAX_CONN_CACHE_USERS = 5_000  # Maximum users in local cache

# Write-through cache over Redis
_user_connections: dict[str, dict[str, dict]] = {}


async def _connections_redis_get(user_id: str) -> dict[str, dict] | None:
    """Load user connections from Redis."""
    try:
        from apps.backend.core.redis_client import get_redis

        r = await get_redis()
        if r:
            val = await r.hget(_CONNECTIONS_REDIS_KEY, user_id)
            if val:
                raw = val if isinstance(val, str) else val.decode()
                return json.loads(raw)
    except Exception as e:
        logger.debug("Redis read failed for connections[%s]: %s", user_id, e)
    return None


async def _connections_redis_set(user_id: str, data: dict[str, dict]) -> None:
    """Persist user connections to Redis."""
    try:
        from apps.backend.core.redis_client import get_redis

        r = await get_redis()
        if r:
            await r.hset(_CONNECTIONS_REDIS_KEY, user_id, json.dumps(data))
        else:
            logger.warning("Redis unavailable — connections for user %s will not persist", user_id)
    except Exception as e:
        logger.warning("Redis write failed for connections[%s]: %s", user_id, e)


@router.get("/connections/{user_id}")
async def list_user_connections(user_id: str, current_user: dict = Depends(get_current_user)):
    """
    List all integrations connected by a user.
    """
    # Verify the authenticated user matches the requested user_id
    auth_uid = str(current_user.get("user_id") or current_user.get("id", ""))
    if auth_uid != user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cannot access another user's connections")

    # Load from Redis if not in cache
    if user_id not in _user_connections:
        redis_conns = await _connections_redis_get(user_id)
        if redis_conns:
            _user_connections[user_id] = redis_conns
    connections = _user_connections.get(user_id, {})
    return {
        "user_id": user_id,
        "connections": [
            {
                "integration_id": integration_id,
                "connected": True,
                "connected_at": conn.get("connected_at"),
            }
            for integration_id, conn in connections.items()
        ],
    }


@router.post("/connections/{user_id}/{integration_id}")
async def connect_integration(
    user_id: str,
    integration_id: str,
    credentials: dict[str, str],
    current_user: dict = Depends(get_current_user),
):
    """
    Connect an integration for a user.

    For OAuth integrations, this stores the tokens.
    For API key integrations, this stores the key.
    """
    auth_uid = str(current_user.get("user_id") or current_user.get("id", ""))
    if auth_uid != user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cannot modify another user's connections")
    if integration_id not in INTEGRATION_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Integration '{integration_id}' not found",
        )

    from datetime import datetime

    if user_id not in _user_connections:
        # Load existing connections from Redis before adding new one
        redis_conns = await _connections_redis_get(user_id)
        _user_connections[user_id] = redis_conns if redis_conns else {}

    # FIFO eviction — remove oldest entry if cache is full
    if user_id not in _user_connections and len(_user_connections) >= _MAX_CONN_CACHE_USERS:
        _user_connections.pop(next(iter(_user_connections)))

    _user_connections[user_id][integration_id] = {
        "credentials": credentials,
        "connected_at": datetime.now(UTC).isoformat(),
    }

    # Persist to Redis
    await _connections_redis_set(user_id, _user_connections[user_id])

    return {
        "success": True,
        "message": f"Connected {integration_id} for user {user_id}",
    }


@router.delete("/connections/{user_id}/{integration_id}")
async def disconnect_integration(
    user_id: str, integration_id: str, current_user: dict = Depends(get_current_user)
):
    """
    Disconnect an integration for a user.
    """
    auth_uid = str(current_user.get("user_id") or current_user.get("id", ""))
    if auth_uid != user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cannot modify another user's connections")
    # Load from Redis if not in cache
    if user_id not in _user_connections:
        redis_conns = await _connections_redis_get(user_id)
        if redis_conns:
            _user_connections[user_id] = redis_conns

    if user_id in _user_connections:
        _user_connections[user_id].pop(integration_id, None)
        # Persist updated connections to Redis
        await _connections_redis_set(user_id, _user_connections[user_id])

    return {
        "success": True,
        "message": f"Disconnected {integration_id} for user {user_id}",
    }


# ============================================================
# OAUTH AUTHORIZE — used by Spiral Builder connect buttons
# ============================================================

# Maps Spirals integration IDs to OAuth provider names (in oauth_callbacks.py)
_OAUTH_PROVIDER_MAP: dict[str, str] = {
    "slack": "slack",
    "gmail": "google",
    "google": "google",
    "google_calendar": "google",
    "google_docs": "google",
    "github": "github",
    "notion": "notion",
    "linear": "linear",
    "asana": "asana",
    "jira": "jira",
    "hubspot": "hubspot",
    "salesforce": "salesforce",
}

# State TTL
_OAUTH_STATE_TTL = 600  # 10 minutes


@router.get("/oauth/{integration_id}/authorize")
async def get_oauth_authorize_url(
    request: Request,
    integration_id: str,
    redirect_after: str = Query(
        "/spirals/integrations",
        description="Frontend URL to redirect to after OAuth completes",
    ),
) -> dict[str, Any]:
    """Return the OAuth authorization URL for a native Helix integration.

    Called by IntegrationManager.tsx when the user clicks "Connect" on a
    Slack, Gmail, or other OAuth-backed integration card. Returns JSON with
    `authorization_url` so the frontend can redirect the user.
    """
    provider = _OAUTH_PROVIDER_MAP.get(integration_id.lower())
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Integration '{integration_id}' does not support OAuth. Supported: {', '.join(_OAUTH_PROVIDER_MAP)}"
            ),
        )

    # Resolve provider config via oauth_callbacks
    try:
        from apps.backend.helix_spirals.oauth_callbacks import OAUTH_PROVIDERS, get_redirect_uri
    except ImportError as exc:
        raise HTTPException(status_code=503, detail="OAuth service unavailable") from exc

    if provider not in OAUTH_PROVIDERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"OAuth provider '{provider}' not configured",
        )

    config = OAUTH_PROVIDERS[provider]
    if not config.get("client_id"):
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=(
                f"OAuth for '{provider}' is not configured — set the corresponding client ID environment variable."
            ),
        )

    # Attempt to get user_id from auth header (best-effort; not required)
    user_id = "anonymous"
    try:
        from apps.backend.core.unified_auth import get_current_user

        user = await get_current_user(request)
        if isinstance(user, dict):
            user_id = user.get("user_id") or user.get("id") or "anonymous"
    except Exception as exc:
        logger.debug("OAuth authorize: user auth not available, proceeding as anonymous: %s", exc)

    # Prevent open redirect
    if not redirect_after.startswith("/") or redirect_after.startswith("//"):
        redirect_after = "/spirals/integrations"

    # Generate CSRF state token and persist to Redis
    state = secrets.token_urlsafe(32)
    state_data = {
        "user_id": user_id,
        "provider": provider,
        "integration_id": integration_id,
        "redirect_after": redirect_after,
    }
    try:
        from apps.backend.core.redis_client import get_redis as _get_redis

        r = await _get_redis()
        if r:
            await r.set(f"helix:oauth:state:{state}", json.dumps(state_data), ex=_OAUTH_STATE_TTL)
    except Exception as exc:
        logger.warning("Could not store OAuth state in Redis: %s", exc)

    # Build authorization URL
    redirect_uri = get_redirect_uri(provider)
    params: dict[str, str] = {
        "client_id": config["client_id"],
        "redirect_uri": redirect_uri,
        "state": state,
        "response_type": "code",
    }

    if provider == "google":
        params["scope"] = " ".join(config.get("scopes", []))
        params["access_type"] = "offline"
        params["prompt"] = "consent"
    elif provider == "slack":
        params["scope"] = ",".join(config.get("scopes", []))
    elif provider == "github":
        params["scope"] = " ".join(config.get("scopes", []))
    else:
        if config.get("scopes"):
            params["scope"] = " ".join(config["scopes"])

    authorization_url = f"{config['authorize_url']}?{urlencode(params)}"

    logger.info(
        "OAuth authorize URL generated for integration=%s provider=%s user=%s",
        integration_id,
        provider,
        user_id,
    )
    return {"authorization_url": authorization_url, "provider": provider, "state": state}
