"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

Helix Spirals API Routes
FastAPI routers for Spirals CRUD, Executions, Templates, and Webhooks
"""

import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer as _HTTPBearer,
)

from .models import ExecutionResponse, ExecutionStatus, Spiral, SpiralCreateRequest, SpiralUpdateRequest, WebhookPayload
from .webhooks import WebhookReceiver

logger = logging.getLogger(__name__)

# Optional bearer — auto_error=False so missing token yields None instead of 401
_optional_bearer = _HTTPBearer(auto_error=False)


async def get_optional_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_optional_bearer),
) -> dict[str, Any] | None:
    """Return the authenticated user dict, or None if unauthenticated.
    Allows webhook / scheduled triggers to call these endpoints without auth.
    """
    if not credentials:
        return None
    try:
        from apps.backend.core.unified_auth import get_current_user

        return await get_current_user(request=request, credentials=credentials)
    except Exception:
        logger.debug("Optional auth: token present but invalid, treating as unauthenticated")
        return None


def _require_ownership(spiral: Spiral, user: dict[str, Any] | None) -> None:
    """Raise 403 if the spiral has an owner and the caller is not that owner."""
    if spiral.user_id is None:
        # Anonymous / webhook spiral — no ownership restriction
        return
    if user is None:
        raise HTTPException(status_code=403, detail="Authentication required to modify this spiral")
    caller_id = str(user.get("user_id") or user.get("id") or "")
    if caller_id != spiral.user_id:
        raise HTTPException(status_code=403, detail="You do not own this spiral")


def _get_caller_id(user: dict[str, Any] | None) -> str | None:
    """Extract user ID string from the auth dict, or None."""
    if user is None:
        return None
    return str(user.get("user_id") or user.get("id") or "") or None


async def _require_execution_ownership(execution_id: str, user: dict[str, Any] | None) -> None:
    """Verify the caller owns the spiral that produced this execution."""
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    if not _storage:
        return  # Storage not initialized — nothing to check
    # Look up the execution's spiral and verify ownership
    execution = await _engine.get_execution_status(execution_id) if _engine else None
    if not execution:
        raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
    spiral = await _storage.get_spiral(execution.spiral_id)
    if spiral:
        _require_ownership(spiral, user)


# Storage reference - will be set by main.py
_storage = None
_engine = None
_webhook_receiver = None
_scheduler = None


def set_storage(storage):
    global _storage
    _storage = storage


def set_engine(engine):
    global _engine, _webhook_receiver
    _engine = engine
    # Initialize webhook receiver when engine is set
    if _storage and _engine:
        _webhook_receiver = WebhookReceiver(_engine, _storage)


def set_scheduler(scheduler):
    """Set the SpiralScheduler reference so routes can re-register spirals on trigger updates."""
    global _scheduler
    _scheduler = scheduler


# =============================================================================
# SPIRALS ROUTER
# =============================================================================
spirals_router = APIRouter(tags=["spirals"])


@spirals_router.get("", response_model=list[Spiral])
async def list_spirals(
    enabled: bool | None = None,
    tag: str | None = None,
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """List spirals for the current user with optional filtering"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    user_id = user.get("id") if user else None
    spirals = await _storage.get_all_spirals(user_id=user_id)

    # Apply filters
    if enabled is not None:
        spirals = [s for s in spirals if s.enabled == enabled]
    if tag:
        spirals = [s for s in spirals if tag in (s.tags or [])]

    # Pagination
    return spirals[offset : offset + limit]


@spirals_router.get("/{spiral_id}", response_model=Spiral)
async def get_spiral(
    spiral_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Get a specific spiral by ID"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    # Enforce ownership: if spiral has an owner, require matching auth
    if spiral.user_id is not None:
        if user is None:
            raise HTTPException(status_code=401, detail="Authentication required")
        caller_id = str(user.get("user_id") or user.get("id") or "")
        if caller_id != spiral.user_id:
            raise HTTPException(status_code=403, detail="You do not own this spiral")

    return spiral


@spirals_router.post("", response_model=Spiral, status_code=201)
async def create_spiral(
    request: SpiralCreateRequest,
    user=Depends(get_optional_user),  # Optional auth - allows webhook/public spirals
):
    """
    Create a new spiral

    Usage Limits (for authenticated users):
    - FREE: 5 spirals/month
    - HOBBY: 25 spirals/month
    - STARTER+: Unlimited spiral creation

    Note: Unauthenticated/webhook spiral creation is allowed but not tracked
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    # 🔒 USAGE QUOTA CHECK: Spiral creation (only for authenticated users)
    user_id = None
    if user:
        user_id = str(getattr(user, "id", None))
        if user_id:
            from apps.backend.saas.models.subscription import SubscriptionTier, TierLimits
            from apps.backend.services.usage_service import check_quota, track_usage

            # Get user tier
            user_tier = getattr(user, "subscription_tier", "free").lower()
            try:
                tier = SubscriptionTier(user_tier)
            except ValueError:
                tier = SubscriptionTier.FREE

            # Check spiral creation limit
            spiral_limit = TierLimits.get_limit(tier, "spirals_per_month")

            # Only enforce limits for tiers with finite limits
            if spiral_limit != -1:  # -1 means unlimited
                resource_type = "spirals_per_month"
                allowed, reason, limits_info = await check_quota(user_id, resource_type, 1)

                if not allowed:
                    raise HTTPException(
                        status_code=429,
                        detail=f"Spiral creation limit exceeded. {reason}",
                        headers={
                            "X-Usage-Current": str(limits_info.get("current", 0)),
                            "X-Usage-Limit": str(limits_info.get("limit", 0)),
                            "X-Upgrade-URL": "/marketplace/pricing",
                        },
                    )

    try:
        spiral = await _storage.create_spiral(request, user_id=user_id)

        # 📊 TRACK USAGE: Record spiral creation (only for authenticated users)
        if user_id:
            await track_usage(
                user_id=user_id,
                resource_type="spirals_per_month",
                quantity=1,
                metadata={
                    "spiral_id": spiral.id,
                    "spiral_name": spiral.name,
                    "trigger_type": getattr(request.trigger, "type", "unknown"),
                },
            )

        logger.info("Created spiral: %s - %s", spiral.id, spiral.name)
        return spiral
    except Exception as e:
        logger.error("Failed to create spiral: %s", e)
        raise HTTPException(status_code=400, detail="Failed to create spiral") from e


@spirals_router.put("/{spiral_id}", response_model=Spiral)
async def update_spiral(
    spiral_id: str,
    request: SpiralUpdateRequest,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Update an existing spiral"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    _require_ownership(spiral, user)

    try:
        updated = await _storage.update_spiral(spiral_id, request)
        logger.info("Updated spiral: %s", spiral_id)

        # Re-register with scheduler if trigger config changed
        if request.trigger is not None and _scheduler:
            try:
                await _scheduler.register_spiral(updated)
                logger.info("Re-registered spiral %s with scheduler after trigger update", spiral_id)
            except Exception as sched_err:
                logger.warning("Failed to re-register spiral %s with scheduler: %s", spiral_id, sched_err)

        return updated
    except Exception as e:
        logger.error("Failed to update spiral: %s", e)
        raise HTTPException(status_code=400, detail="Failed to update spiral") from e


@spirals_router.delete("/{spiral_id}", status_code=204)
async def delete_spiral(
    spiral_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Delete a spiral"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    _require_ownership(spiral, user)

    await _storage.delete_spiral(spiral_id)
    logger.info("Deleted spiral: %s", spiral_id)


@spirals_router.post("/{spiral_id}/enable")
async def enable_spiral(
    spiral_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Enable a spiral and drain any held tasks that arrived while it was disabled."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    _require_ownership(spiral, user)

    updated = await _storage.update_spiral(spiral_id, SpiralUpdateRequest(enabled=True))

    drain_results: list[dict] = []
    if _engine:
        try:
            drain_results = await _engine.drain_held_tasks(spiral_id)
        except Exception as e:
            logger.warning("Failed to drain held tasks for spiral %s: %s", spiral_id, e)

    return {
        "spiral": updated.model_dump() if hasattr(updated, "model_dump") else dict(updated),
        "drained_tasks": drain_results,
        "drained_count": len(drain_results),
    }


@spirals_router.post("/{spiral_id}/disable")
async def disable_spiral(
    spiral_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Disable a spiral. Incoming triggers will be buffered as held tasks."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    _require_ownership(spiral, user)

    updated = await _storage.update_spiral(spiral_id, SpiralUpdateRequest(enabled=False))
    return updated


@spirals_router.get("/{spiral_id}/held-tasks")
async def get_held_tasks(
    spiral_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Return held tasks buffered while this spiral was disabled."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    _require_ownership(spiral, user)

    tasks = await _storage.get_held_tasks(spiral_id)
    count = await _storage.count_held_tasks(spiral_id)
    return {"spiral_id": spiral_id, "held_tasks": tasks, "count": count}


@spirals_router.post("/{spiral_id}/clone", response_model=Spiral, status_code=201)
async def clone_spiral(
    spiral_id: str,
    name: str | None = None,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Clone/fork a spiral into a new independent copy.

    The new spiral inherits the trigger, actions, variables, tags, and
    configuration of the source but gets a fresh ID and is disabled by
    default so the user can review before activating.
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    source = await _storage.get_spiral(spiral_id)
    if not source:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    user_id = None
    if user:
        user_id = str(user.get("user_id") or user.get("id") or "")

    clone_name = name or f"Copy of {source.name}"

    request = SpiralCreateRequest(
        name=clone_name,
        description=source.description,
        trigger=source.trigger.model_dump() if hasattr(source.trigger, "model_dump") else dict(source.trigger),
        actions=[a.model_dump() if hasattr(a, "model_dump") else dict(a) for a in source.actions],
        enabled=False,
        tags=[*list(source.tags or []), "cloned"],
        variables=[v.model_dump() if hasattr(v, "model_dump") else dict(v) for v in (source.variables or [])],
    )

    cloned = await _storage.create_spiral(request, user_id=user_id)
    logger.info("Cloned spiral %s -> %s (%s)", spiral_id, cloned.id, clone_name)
    return cloned


@spirals_router.post("/{spiral_id}/dry-run")
async def dry_run_spiral(
    spiral_id: str,
    trigger_data: dict[str, Any] | None = None,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Preview/dry-run a spiral without executing side effects.

    Returns the resolved action sequence, variable bindings, and
    condition evaluation results so users can verify their spiral
    logic before enabling it.
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    _require_ownership(spiral, user)

    preview_data = trigger_data or {}

    # Build resolved variable map
    resolved_vars = {}
    for var in spiral.variables or []:
        var_name = var.name if hasattr(var, "name") else var.get("name", "")
        var_default = var.default_value if hasattr(var, "default_value") else var.get("default_value")
        resolved_vars[var_name] = preview_data.get(var_name, var_default)

    # Walk actions and evaluate condition branches
    steps = []
    for i, action in enumerate(spiral.actions):
        action_dict = action.model_dump() if hasattr(action, "model_dump") else dict(action)
        step = {
            "index": i,
            "id": action_dict.get("id"),
            "name": action_dict.get("name"),
            "type": action_dict.get("type"),
            "would_execute": True,
            "config_keys": list((action_dict.get("config") or {}).keys()),
        }
        # Evaluate conditions if present
        conditions = action_dict.get("conditions") or []
        if conditions:
            step["conditions_count"] = len(conditions)
            step["would_execute"] = True  # Optimistic — real eval requires runtime data

        steps.append(step)

    return {
        "spiral_id": spiral_id,
        "spiral_name": spiral.name,
        "mode": "dry-run",
        "trigger": {
            "type": spiral.trigger.type.value if hasattr(spiral.trigger.type, "value") else str(spiral.trigger.type),
            "name": spiral.trigger.name,
        },
        "resolved_variables": resolved_vars,
        "action_sequence": steps,
        "total_actions": len(steps),
        "estimated_duration_ms": len(steps) * 1000,  # rough estimate
    }


@spirals_router.get("/{spiral_id}/statistics", response_model=dict)
async def get_spiral_statistics(
    spiral_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Get statistics for a specific spiral"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    return await _storage.get_spiral_statistics(spiral_id)


@spirals_router.post("/{spiral_id}/execute")
async def execute_spiral(
    spiral_id: str,
    background_tasks: BackgroundTasks,
    trigger_data: dict[str, Any] | None = None,
    user=Depends(get_optional_user),  # Optional auth - allows webhooks/scheduled triggers
):
    """
    Execute a spiral by ID using SpiralEngine

    Usage Limits (for authenticated manual executions):
    - FREE: 100 executions/month
    - HOBBY: 500 executions/month
    - STARTER: 1,000 executions/month
    - PRO: 10,000 executions/month
    - ENTERPRISE: Unlimited

    Note: Webhook and scheduled triggers are not counted against quota
    """
    if not _engine:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    # 🔒 USAGE QUOTA CHECK: Spiral execution (only for authenticated users)
    # Webhooks and scheduled triggers bypass this check
    user_id = None
    if user:
        user_id = str(getattr(user, "id", None))
        if user_id:
            from apps.backend.services.usage_service import check_quota, track_usage

            resource_type = "spiral_executions_per_month"
            allowed, reason, limits_info = await check_quota(user_id, resource_type, 1)

            if not allowed:
                raise HTTPException(
                    status_code=429,
                    detail=f"Spiral execution limit exceeded. {reason}",
                    headers={
                        "X-Usage-Current": str(limits_info.get("current", 0)),
                        "X-Usage-Limit": str(limits_info.get("limit", 0)),
                        "X-Upgrade-URL": "/marketplace/pricing",
                    },
                )

    context = await _engine.execute(
        spiral_id=spiral_id,
        trigger_type="manual",
        trigger_data=trigger_data or {},
    )

    # 📊 TRACK USAGE: Record spiral execution (only for authenticated users)
    if user_id:
        await track_usage(
            user_id=user_id,
            resource_type="spiral_executions_per_month",
            quantity=1,
            metadata={
                "spiral_id": spiral_id,
                "spiral_name": getattr(spiral, "name", "Unknown"),
                "trigger_type": "manual",
                "execution_id": context.execution_id,
            },
        )

    return {
        "execution_id": context.execution_id,
        "spiral_id": spiral_id,
        "status": (context.status.value if hasattr(context.status, "value") else str(context.status)),
        "started_at": context.started_at,
    }


@spirals_router.get("/nodes")
async def list_nodes(category: str | None = None):
    """List all available node types from the NodeRegistry"""
    try:
        from .advanced_nodes import get_node_registry

        registry = get_node_registry()
        nodes = registry.list_nodes()
        if category:
            nodes = [n for n in nodes if n.get("category", "").lower() == category.lower()]
        return {"nodes": nodes, "count": len(nodes)}
    except ImportError as e:
        raise HTTPException(status_code=503, detail="Node registry not available") from e


@spirals_router.get("/nodes/categories")
async def list_node_categories():
    """List all node categories"""
    try:
        from .advanced_nodes import get_node_registry

        registry = get_node_registry()
        categories = registry.get_categories()
        return {"categories": categories}
    except ImportError as e:
        raise HTTPException(status_code=503, detail="Node registry not available") from e


@spirals_router.post("/nodes/execute")
async def execute_single_node(
    node_type: str,
    inputs: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Execute a single node by type (requires authentication)"""
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required to execute nodes")
    try:
        from .advanced_nodes import get_node_registry

        registry = get_node_registry()
        node = registry.create(node_type)
        if not node:
            raise HTTPException(status_code=404, detail=f"Node type not found: {node_type}")

        result = await node.execute(inputs or {}, context or {})
        return {
            "success": result.success,
            "data": result.data,
            "error": result.error,
            "execution_time_ms": result.execution_time_ms,
            "metadata": result.metadata,
        }
    except ImportError as e:
        raise HTTPException(status_code=503, detail="Node registry not available") from e


@spirals_router.get("/list", response_model=list[Spiral])
async def list_spirals_alias(
    enabled: bool | None = None,
    tag: str | None = None,
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Alias for GET /api/spirals — used by frontend OS page"""
    return await list_spirals(enabled=enabled, tag=tag, limit=limit, offset=offset, user=user)


@spirals_router.post("/import/zapier")
async def import_zapier_workflow(
    request: Request,
    user=Depends(get_optional_user),
):
    """Import Zapier export JSON and convert all Zaps to Helix Spirals.

    Accepts the full Zapier export format (list of Zaps, {zaps: [...]}, or
    a single Zap object) and returns conversion statistics.
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from e

    try:
        from .zapier_import import ZapierImporter

        importer = ZapierImporter(_storage)
        stats = await importer.import_zapier_export(body)
    except Exception as e:
        logger.warning("Zapier import error: %s", e)
        raise HTTPException(status_code=500, detail="Import failed") from e

    # Normalise to the shape the frontend ImportStats type expects
    return {
        "total_zaps": stats.get("total_zaps", 0),
        "converted": stats.get("converted", 0),
        "failed": stats.get("failed", 0),
        "skipped": stats.get("skipped", 0),
        "warnings": stats.get("errors", []),
        "spirals_created": stats.get("spirals_created", []),
        "action_coverage": stats.get("action_types_converted", {}),
        "trigger_coverage": stats.get("trigger_types_converted", {}),
        "coverage_percentage": stats.get("success_rate", 0.0),
    }


# =============================================================================
# EXECUTIONS ROUTER
# =============================================================================
executions_router = APIRouter(tags=["executions"])


@executions_router.get("", response_model=list[ExecutionResponse])
async def list_executions(
    spiral_id: str | None = None,
    status: ExecutionStatus | None = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """List execution history (scoped to authenticated user's spirals)"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    caller_id = _get_caller_id(user)
    executions = await _storage.get_execution_history_list(
        spiral_id=spiral_id, status=status, limit=limit, offset=offset, user_id=caller_id
    )
    return executions


@executions_router.get("/active", response_model=list[ExecutionResponse])
async def list_active_executions():
    """List currently running executions"""
    if not _engine:
        raise HTTPException(status_code=503, detail="Engine not initialized")

    active = await _engine.get_active_executions()
    return [
        ExecutionResponse(
            execution_id=ctx.execution_id,
            spiral_id=ctx.spiral_id,
            status=ctx.status,
            started_at=ctx.started_at,
            completed_at=ctx.completed_at,
            logs=ctx.logs,
        )
        for ctx in active
    ]


@executions_router.get("/{execution_id}", response_model=ExecutionResponse)
async def get_execution(
    execution_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Get execution details (requires ownership of the parent spiral)"""
    if not _engine:
        raise HTTPException(status_code=503, detail="Engine not initialized")

    await _require_execution_ownership(execution_id, user)

    execution = await _engine.get_execution_status(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")

    return ExecutionResponse(
        execution_id=execution.execution_id,
        spiral_id=execution.spiral_id,
        status=execution.status,
        started_at=execution.started_at,
        completed_at=execution.completed_at,
        logs=execution.logs,
    )


@executions_router.post("/{execution_id}/cancel", status_code=204)
async def cancel_execution(
    execution_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Cancel a running execution (requires ownership of the parent spiral)"""
    if not _engine:
        raise HTTPException(status_code=503, detail="Engine not initialized")

    await _require_execution_ownership(execution_id, user)

    success = await _engine.cancel_execution(execution_id)
    if not success:
        raise HTTPException(
            status_code=404,
            detail=f"Execution {execution_id} not found or already completed",
        )


@executions_router.get("/{execution_id}/logs")
async def get_execution_logs(
    execution_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Get logs for an execution (requires ownership of the parent spiral)"""
    if not _engine:
        raise HTTPException(status_code=503, detail="Engine not initialized")

    await _require_execution_ownership(execution_id, user)

    execution = await _engine.get_execution_status(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")

    return {"execution_id": execution_id, "logs": execution.logs}


# ── Time-travel debugging endpoints (P3) ──────────────────────────────────────


@executions_router.get("/{execution_id}/checkpoints")
async def list_step_checkpoints(
    execution_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """List all per-step checkpoints for an execution (time-travel debugging).

    Each entry represents the state of context.variables after one action
    completed.  Use the action_index to rewind the execution.
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    await _require_execution_ownership(execution_id, user)

    checkpoints = await _storage.list_step_checkpoints(execution_id)
    return {"execution_id": execution_id, "checkpoints": checkpoints}


@executions_router.get("/{execution_id}/checkpoints/{action_index}")
async def get_step_checkpoint(
    execution_id: str,
    action_index: int,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Get the full variables snapshot for a specific step checkpoint."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    await _require_execution_ownership(execution_id, user)

    ckpt = await _storage.get_step_checkpoint(execution_id, action_index)
    if not ckpt:
        raise HTTPException(
            status_code=404,
            detail=f"No checkpoint found for execution {execution_id} step {action_index}",
        )
    return ckpt


@executions_router.post("/{execution_id}/rewind")
async def rewind_execution(
    execution_id: str,
    body: dict,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Rewind an execution to a past step and replay forward from there.

    Loads the checkpoint at `action_index`, restores context variables, and
    starts a new execution continuing from that step onward.  Returns the
    new execution_id of the replayed run.

    Body: { "action_index": int }
    """
    if not _engine or not _storage:
        raise HTTPException(status_code=503, detail="Engine not initialized")

    await _require_execution_ownership(execution_id, user)

    action_index = body.get("action_index")
    if action_index is None or not isinstance(action_index, int) or action_index < 0:
        raise HTTPException(status_code=400, detail="action_index (non-negative int) is required")

    # Load checkpoint snapshot
    ckpt = await _storage.get_step_checkpoint(execution_id, action_index)
    if not ckpt:
        raise HTTPException(
            status_code=404,
            detail=f"No checkpoint for execution {execution_id} at step {action_index}",
        )

    # Load the original execution to get spiral_id + trigger
    original = await _storage.get_execution_history(execution_id)
    if not original:
        raise HTTPException(
            status_code=404,
            detail=f"Original execution {execution_id} not found",
        )

    import json as _json
    from uuid import uuid4

    from .models import ExecutionContext, ExecutionStatus

    # Reconstruct context from checkpoint variables snapshot
    variables_snapshot = (
        _json.loads(ckpt["variables_snapshot"])
        if isinstance(ckpt["variables_snapshot"], str)
        else ckpt["variables_snapshot"]
    )

    replay_exec_id = str(uuid4())
    replay_context = ExecutionContext(
        spiral_id=str(original.spiral_id),
        execution_id=replay_exec_id,
        trigger=original.trigger,
        variables=variables_snapshot,
        logs=[],
        status=ExecutionStatus.PENDING,
        started_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        metadata={
            "rewound_from": execution_id,
            "rewind_step": action_index,
        },
    )

    # Load the spiral definition and start execution from action_index
    spiral = await _storage.get_spiral(str(original.spiral_id))
    if not spiral:
        raise HTTPException(
            status_code=404,
            detail=f"Original spiral {original.spiral_id} not found",
        )

    # Trim the action list to start from action_index + 1 (we already have
    # the checkpoint state *after* that action completed)
    replay_spiral = spiral.copy(deep=True) if hasattr(spiral, "copy") else spiral
    replay_spiral.actions = spiral.actions[action_index + 1 :]

    # Queue execution and return immediately
    import asyncio

    _engine.active_executions[replay_exec_id] = asyncio.ensure_future(
        _engine._execute_spiral(replay_spiral, replay_context)
    )
    _engine.execution_queue[replay_exec_id] = replay_context

    return {
        "original_execution_id": execution_id,
        "rewound_from_step": action_index,
        "replay_execution_id": replay_exec_id,
        "remaining_actions": len(replay_spiral.actions),
    }


# ─────────────────────────────────────────────────────────────────────────────


# =============================================================================
# TEMPLATES ROUTER
# =============================================================================
templates_router = APIRouter(tags=["templates"])

# Built-in templates for common automations
BUILTIN_TEMPLATES = [
    {
        "id": "discord-notification",
        "name": "Discord Notification",
        "description": "Send a message to Discord when triggered",
        "category": "notifications",
        "icon": "discord",
        "zapier_equivalent": "Slack/Discord notification zap",
        "steps_consolidated": 3,
    },
    {
        "id": "ucf-alert",
        "name": "UCF Threshold Alert",
        "description": "Alert when UCF metrics cross a threshold",
        "category": "monitoring",
        "icon": "chart",
        "zapier_equivalent": "Multi-step monitoring workflow",
        "steps_consolidated": 5,
    },
    {
        "id": "cycle-scheduler",
        "name": "Scheduled Cycle",
        "description": "Run routines on a schedule",
        "category": "automation",
        "icon": "clock",
        "zapier_equivalent": "Scheduled task zap",
        "steps_consolidated": 2,
    },
    {
        "id": "webhook-to-discord",
        "name": "Webhook to Discord",
        "description": "Receive webhooks and forward to Discord",
        "category": "integration",
        "icon": "webhook",
        "zapier_equivalent": "Webhook catch + Discord send",
        "steps_consolidated": 4,
    },
    {
        "id": "agent-orchestration",
        "name": "Agent Orchestration",
        "description": "Coordinate multiple agents based on events",
        "category": "ai",
        "icon": "robot",
        "zapier_equivalent": "Multi-step AI workflow (10+ zaps)",
        "steps_consolidated": 10,
    },
    {
        "id": "data-sync",
        "name": "Data Synchronization",
        "description": "Sync data between services",
        "category": "integration",
        "icon": "sync",
        "zapier_equivalent": "Multi-app data sync",
        "steps_consolidated": 6,
    },
    {
        "id": "email-notification",
        "name": "Email Notification",
        "description": "Send email notifications via SendGrid",
        "category": "notifications",
        "icon": "mail",
        "zapier_equivalent": "Email notification zap",
        "steps_consolidated": 2,
    },
    {
        "id": "coordination-tracker",
        "name": "Coordination Level Tracker",
        "description": "Track and respond to coordination level changes",
        "category": "spiroutine",
        "icon": "eye",
        "zapier_equivalent": "N/A - Helix exclusive",
        "steps_consolidated": 0,
        "popularity": 75,
    },
    # === NOTIFICATIONS ===
    {
        "id": "slack-channel-alert",
        "name": "Slack Channel Alert",
        "description": "Post alerts to Slack channels based on triggers",
        "category": "notifications",
        "icon": "slack",
        "zapier_equivalent": "Slack message zap",
        "steps_consolidated": 2,
        "popularity": 88,
    },
    # === SALES & MARKETING ===
    {
        "id": "lead-nurturing",
        "name": "Lead Nurturing Sequence",
        "description": "Automatically follow up with new leads over time",
        "category": "sales",
        "icon": "users",
        "zapier_equivalent": "Multi-step lead nurture (5+ zaps)",
        "steps_consolidated": 5,
        "popularity": 94,
    },
    {
        "id": "crm-contact-sync",
        "name": "CRM Contact Sync",
        "description": "Sync contacts between forms and your CRM",
        "category": "sales",
        "icon": "user-plus",
        "zapier_equivalent": "Form to CRM zap",
        "steps_consolidated": 3,
        "popularity": 91,
    },
    {
        "id": "social-media-scheduler",
        "name": "Social Media Scheduler",
        "description": "Schedule and post content across social platforms",
        "category": "sales",
        "icon": "share",
        "zapier_equivalent": "Buffer/Hootsuite integration",
        "steps_consolidated": 4,
        "popularity": 87,
    },
    # === DATA & INTEGRATION ===
    {
        "id": "spreadsheet-sync",
        "name": "Spreadsheet Data Sync",
        "description": "Sync data to/from Google Sheets or Excel",
        "category": "integration",
        "icon": "table",
        "zapier_equivalent": "Google Sheets integration zap",
        "steps_consolidated": 3,
        "popularity": 90,
    },
    {
        "id": "database-backup",
        "name": "Automated Database Backup",
        "description": "Schedule regular database backups to cloud storage",
        "category": "integration",
        "icon": "database",
        "zapier_equivalent": "Scheduled backup workflow",
        "steps_consolidated": 4,
        "popularity": 82,
    },
    # === PRODUCTIVITY ===
    {
        "id": "meeting-notes-summary",
        "name": "Meeting Notes Summary",
        "description": "Summarize meeting notes with AI and distribute",
        "category": "productivity",
        "icon": "file-text",
        "zapier_equivalent": "Otter.ai + email multi-step",
        "steps_consolidated": 4,
        "popularity": 86,
    },
    {
        "id": "task-assignment",
        "name": "Task Auto-Assignment",
        "description": "Automatically assign tasks based on triggers",
        "category": "productivity",
        "icon": "check-square",
        "zapier_equivalent": "Trello/Asana task creation",
        "steps_consolidated": 3,
        "popularity": 84,
    },
    {
        "id": "calendar-sync",
        "name": "Calendar Event Sync",
        "description": "Sync calendar events across platforms",
        "category": "productivity",
        "icon": "calendar",
        "zapier_equivalent": "Google Calendar sync",
        "steps_consolidated": 3,
        "popularity": 83,
    },
    # === MONITORING & ALERTS ===
    {
        "id": "uptime-monitor",
        "name": "Website Uptime Monitor",
        "description": "Monitor website uptime and alert on downtime",
        "category": "monitoring",
        "icon": "activity",
        "zapier_equivalent": "UptimeRobot + Slack integration",
        "steps_consolidated": 3,
        "popularity": 81,
    },
    {
        "id": "error-tracker",
        "name": "Error Tracking & Alerts",
        "description": "Track errors and send alerts to your team",
        "category": "monitoring",
        "icon": "alert-triangle",
        "zapier_equivalent": "Sentry + communication platform",
        "steps_consolidated": 4,
        "popularity": 80,
    },
    # === AI & AUTOMATION ===
    {
        "id": "ai-content-generator",
        "name": "AI Content Generator",
        "description": "Generate content with AI and publish automatically",
        "category": "ai",
        "icon": "pen-tool",
        "zapier_equivalent": "ChatGPT + publishing integration",
        "steps_consolidated": 5,
        "popularity": 91,
    },
    {
        "id": "sentiment-analyzer",
        "name": "Sentiment Analysis Workflow",
        "description": "Analyze sentiment of feedback and route accordingly",
        "category": "ai",
        "icon": "message-circle",
        "zapier_equivalent": "MonkeyLearn + multi-path routing",
        "steps_consolidated": 4,
        "popularity": 79,
    },
]


def _get_unified_templates() -> list[dict]:
    """Load templates from the unified TEMPLATE_REGISTRY, falling back to BUILTIN_TEMPLATES stubs."""
    try:
        from .workflow_templates import TEMPLATE_REGISTRY

        templates = []
        for tpl in TEMPLATE_REGISTRY.values():
            templates.append(tpl.to_dict() if hasattr(tpl, "to_dict") else tpl)
        return templates
    except (ImportError, Exception):
        return BUILTIN_TEMPLATES.copy()


def _get_unified_template_by_id(template_id: str) -> dict | None:
    """Look up a single template from the unified registry, falling back to stubs."""
    try:
        from .workflow_templates import TEMPLATE_REGISTRY

        tpl = TEMPLATE_REGISTRY.get(template_id)
        if tpl:
            return tpl.to_dict() if hasattr(tpl, "to_dict") else tpl

        # Also try matching by id field (stubs use hyphenated IDs)
        for tpl in TEMPLATE_REGISTRY.values():
            tpl_dict = tpl.to_dict() if hasattr(tpl, "to_dict") else tpl
            if tpl_dict.get("id") == template_id:
                return tpl_dict
    except (ImportError, Exception) as e:
        logger.debug("Template registry lookup failed: %s", e)

    # Fallback to BUILTIN_TEMPLATES stubs
    for t in BUILTIN_TEMPLATES:
        if t["id"] == template_id:
            return t

    return None


@templates_router.get("", response_model=list[dict])
async def list_templates(category: str | None = None):
    """List available spiral templates from the unified registry"""
    templates = _get_unified_templates()

    # Load custom templates from storage if available
    if _storage:
        try:
            custom = await _storage.list_custom_templates()
            if custom:
                templates.extend(custom)
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
            logger.warning("Failed to load custom templates from storage: %s", e)

    if category:
        templates = [t for t in templates if t.get("category") == category]

    return templates


@templates_router.get("/categories")
async def list_template_categories():
    """List available template categories"""
    templates = _get_unified_templates()
    categories = {t.get("category") for t in templates}
    return sorted(categories)


@templates_router.get("/{template_id}")
async def get_template(template_id: str):
    """Get a specific template"""
    template = _get_unified_template_by_id(template_id)
    if template:
        return template

    raise HTTPException(status_code=404, detail=f"Template {template_id} not found")


@templates_router.post("/{template_id}/instantiate", response_model=Spiral)
async def instantiate_template(template_id: str, name: str, variables: dict | None = None):
    """Create a new spiral from a template, preserving its real workflow definition."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    template = _get_unified_template_by_id(template_id)

    if not template:
        raise HTTPException(status_code=404, detail=f"Template {template_id} not found")

    # Try using the rich template instantiation from workflow_templates
    try:
        from .workflow_templates import instantiate_template as _tpl_instantiate

        instance = _tpl_instantiate(template_id, variables)
    except Exception as e:
        logger.warning("Template instantiation failed for %s: %s", template_id, e)
        instance = None

    # Build trigger + actions from the template's nodes/connections if available
    nodes = []
    if instance and instance.get("nodes"):
        nodes = instance["nodes"]
    elif template.get("nodes"):
        nodes = template["nodes"]

    # Convert node list into Spiral actions (skip trigger nodes)
    actions = []
    trigger_cfg = {"type": "manual", "name": "Manual Trigger", "config": {"type": "manual"}}
    for node in nodes:
        ntype = (node.get("type") or "").lower()
        if ntype in ("webhook", "trigger", "schedule", "cron"):
            # Use the first trigger-like node as the actual trigger
            trigger_cfg = {
                "type": ntype if ntype in ("webhook", "schedule") else "manual",
                "name": node.get("name", "Template Trigger"),
                "config": node.get("config", {"type": "manual"}),
            }
        else:
            actions.append(
                {
                    "id": node.get("id", ""),
                    "name": node.get("name", ntype),
                    "type": ntype or "webhook",
                    "config": node.get("config", {}),
                }
            )

    request = SpiralCreateRequest(
        name=name,
        description=(
            instance.get("description", "Created from template: {}".format(template.get("name", template_id)))
            if instance
            else "Created from template: {}".format(template.get("name", template_id))
        ),
        trigger=trigger_cfg,
        actions=actions,
        tags=[template.get("category", "general"), "from-template"],
    )

    spiral = await _storage.create_spiral(request)
    logger.info("Instantiated template %s as spiral %s (%d actions)", template_id, spiral.id, len(actions))
    return spiral


@templates_router.post("/custom/create")
async def create_custom_template(
    name: str,
    description: str,
    config: dict,
    user: dict[str, Any] | None = Depends(get_optional_user),
    category: str | None = None,
    icon: str | None = None,
    difficulty: str | None = None,
    estimated_time: str | None = None,
    tags: list[str] | None = None,
    public: bool = False,
):
    """Create a new custom template"""
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    import uuid

    user_id = str(user.get("user_id") or user.get("id") or "")
    template_id = f"custom-{uuid.uuid4()}"

    template = await _storage.create_custom_template(
        template_id=template_id,
        name=name,
        description=description,
        config=config,
        user_id=user_id,
        category=category,
        icon=icon,
        difficulty=difficulty,
        estimated_time=estimated_time,
        tags=tags,
        public=public,
    )

    logger.info("Created custom template %s by user %s", template_id, user_id)
    return template


@templates_router.get("/custom/list")
async def list_custom_templates(
    category: str | None = None,
    public_only: bool = False,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """List custom templates (user's own + public)"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    caller_id = _get_caller_id(user)
    templates = await _storage.list_custom_templates(user_id=caller_id, category=category, public_only=public_only)

    return {"templates": templates, "count": len(templates)}


@templates_router.get("/custom/{template_id}")
async def get_custom_template(
    template_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Get a specific custom template (must be owner or template must be public)"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    template = await _storage.get_custom_template(template_id)

    if not template:
        raise HTTPException(status_code=404, detail="Custom template not found")

    # Allow access to public templates or templates owned by the caller
    caller_id = _get_caller_id(user)
    if not template.get("public") and template.get("user_id") != caller_id:
        raise HTTPException(status_code=403, detail="You do not have access to this template")

    return template


@templates_router.patch("/custom/{template_id}")
async def update_custom_template(
    template_id: str,
    updates: dict,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Update a custom template (owner only)"""
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    # Verify ownership before allowing update
    existing = await _storage.get_custom_template(template_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Custom template not found")
    caller_id = _get_caller_id(user)
    if existing.get("user_id") != caller_id:
        raise HTTPException(status_code=403, detail="You do not own this template")

    template = await _storage.update_custom_template(template_id, updates)

    logger.info("Updated custom template %s by user %s", template_id, caller_id)
    return template


@templates_router.delete("/custom/{template_id}")
async def delete_custom_template(
    template_id: str,
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    """Delete a custom template (owner only)"""
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    # Verify ownership before allowing delete
    existing = await _storage.get_custom_template(template_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Custom template not found")
    caller_id = _get_caller_id(user)
    if existing.get("user_id") != caller_id:
        raise HTTPException(status_code=403, detail="You do not own this template")

    await _storage.delete_custom_template(template_id)

    logger.info("Deleted custom template %s by user %s", template_id, caller_id)
    return {"success": True, "message": "Template deleted"}


@templates_router.post("/custom/{template_id}/use")
async def use_custom_template(template_id: str):
    """Increment usage count for a template"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    await _storage.increment_template_usage(template_id)
    return {"success": True}


# =============================================================================
# WEBHOOK ROUTER
# =============================================================================
webhook_router = APIRouter(prefix="/webhook", tags=["webhooks"])


@webhook_router.post("/{spiral_id}")
async def receive_webhook(
    spiral_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """
    Universal webhook endpoint for triggering spirals.

    Supports any HTTP method and content type.
    Rate limited to 100 requests per hour per spiral.
    """
    global _webhook_receiver

    if not _webhook_receiver:
        # Try to initialize if storage and engine are available
        if _storage and _engine:
            _webhook_receiver = WebhookReceiver(_engine, _storage)
        else:
            raise HTTPException(status_code=503, detail="Webhook receiver not initialized")

    try:
        body = {}
        content_type = request.headers.get("content-type", "")

        if "application/json" in content_type:
            try:
                body = await request.json()
            except Exception as e:
                logger.warning("Failed to parse webhook request JSON: %s", e)
                body = {}
        elif "application/x-www-form-urlencoded" in content_type:
            form_data = await request.form()
            body = dict(form_data)
        else:
            # Try to get raw body as string
            raw_body = await request.body()
            body = {"raw": raw_body.decode("utf-8", errors="ignore")}

        # Build webhook payload
        webhook = WebhookPayload(
            spiral_id=spiral_id,
            method=request.method,
            headers=dict(request.headers),
            body=body,
            query_params=dict(request.query_params),
            client_ip=request.client.host if request.client else "unknown",
        )

        # Process webhook (can be async in background for better performance)
        result = await _webhook_receiver.process_webhook(webhook)

        if result.get("status") == "error":
            raise HTTPException(
                status_code=400,
                detail=result.get("message", "Webhook processing failed"),
            )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Webhook error for spiral %s: %s", spiral_id, e)
        raise HTTPException(status_code=500, detail="Internal webhook error") from e


@webhook_router.get("/{spiral_id}/info")
async def get_webhook_info(spiral_id: str):
    """Get webhook URL and configuration for a spiral"""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    spiral = await _storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail=f"Spiral {spiral_id} not found")

    import os

    base_url = os.getenv("API_BASE_URL", "https://api.helixcollective.io")

    return {
        "spiral_id": spiral_id,
        "spiral_name": spiral.name,
        "webhook_url": f"{base_url}/api/spirals/webhook/{spiral_id}",
        "methods": ["POST", "GET", "PUT", "DELETE"],
        "enabled": spiral.enabled,
        "rate_limit": "100 requests/hour",
    }
