"""
Cost Protection API Endpoints
API endpoints for cost monitoring and management
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from apps.backend.core.unified_auth import get_current_user

from .cost_integration import cost_integration
from .cost_protection import BYOT_REQUIREMENTS, MODEL_COSTS, cost_limiter

logger = logging.getLogger(__name__)

router = APIRouter(tags=["cost"])


# Request/Response Models
class CostCheckRequest(BaseModel):
    model: str = Field(..., description="Model to use")
    input_tokens: int = Field(..., ge=0, description="Number of input tokens")
    output_tokens: int = Field(..., ge=0, description="Number of output tokens")
    check_byot: bool = Field(default=True, description="Check BYOT requirements")


class CostCheckResponse(BaseModel):
    allowed: bool
    reason: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CostReportResponse(BaseModel):
    user_id: str
    tier: str
    tier_price: float
    current_cost: float
    remaining_cost: float
    cost_percentage: float
    request_count: int
    avg_cost_per_request: float
    projected_monthly_cost: dict[str, Any]
    byot_status: dict[str, Any]


class ModelRecommendationRequest(BaseModel):
    task_complexity: str = Field(default="medium", description="Task complexity (simple, medium, complex, expert)")


class ModelRecommendationResponse(BaseModel):
    model: str | None = None
    reason: str | None = None
    error: str | None = None
    upgrade_to: str | None = None


class ModelCostsResponse(BaseModel):
    models: dict[str, dict[str, float]]


# API Endpoints


def _require_user_id(user: dict[str, Any]) -> str:
    user_id = user.get("user_id") or user.get("id") or user.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Authenticated user id unavailable")
    return str(user_id)


@router.post("/check")
async def check_request_cost(
    request: CostCheckRequest,
    user: dict[str, Any] = Depends(get_current_user),
) -> CostCheckResponse:
    """
    Check if a request is allowed based on cost limits.

    This endpoint checks:
    - Rate limits
    - Cost limits
    - Model access
    - Token limits
    - BYOT requirements
    """
    user_id = _require_user_id(user)

    allowed, reason, metadata = await cost_integration.check_request_allowed(
        user_id=user_id,
        model=request.model,
        input_tokens=request.input_tokens,
        output_tokens=request.output_tokens,
        check_byot=request.check_byot,
    )

    return CostCheckResponse(
        allowed=allowed,
        reason=reason,
        metadata=metadata,
    )


@router.get("/report")
async def get_cost_report(
    user: dict[str, Any] = Depends(get_current_user),
) -> CostReportResponse:
    """
    Get comprehensive cost report for user.

    Includes:
    - Current cost
    - Remaining budget
    - Request count
    - Average cost per request
    - Projected monthly cost
    - BYOT recommendations
    """
    user_id = _require_user_id(user)

    report = cost_integration.get_user_cost_report(user_id)

    return CostReportResponse(**report)


@router.post("/recommend-model")
async def get_model_recommendation(
    request: ModelRecommendationRequest,
    user: dict[str, Any] = Depends(get_current_user),
) -> ModelRecommendationResponse:
    """
    Get model recommendation based on user tier and task complexity.

    Recommends the most cost-effective model that can handle the task.
    """
    user_id = _require_user_id(user)

    recommendation = cost_integration.get_model_recommendation(
        user_id=user_id,
        task_complexity=request.task_complexity,
    )

    return ModelRecommendationResponse(**recommendation)


@router.get("/models")
async def get_model_costs() -> ModelCostsResponse:
    """
    Get cost information for all supported models.

    Returns cost per 1M tokens for input and output.
    """
    return ModelCostsResponse(models=MODEL_COSTS)


@router.get("/stats")
async def get_cost_stats(
    user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """
    Get cost statistics for user.

    Includes detailed stats for current month.
    """
    user_id = _require_user_id(user)

    from apps.backend.billing.unified_billing import UnifiedBillingService

    billing_service = UnifiedBillingService()
    tier = billing_service.get_user_tier(user_id)

    stats = cost_limiter.get_user_stats(user_id, tier)

    return stats


@router.get("/alerts")
async def get_cost_alerts(
    user: dict[str, Any] = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """
    Get cost alerts for user.

    Returns alerts for:
    - Approaching cost limit (80%)
    - BYOT requirements
    - Model restrictions
    """
    user_id = _require_user_id(user)

    alerts = []

    # Get user stats
    from apps.backend.billing.unified_billing import UnifiedBillingService
    from apps.backend.config.unified_pricing import Tier

    billing_service = UnifiedBillingService()
    tier = billing_service.get_user_tier(user_id)

    stats = cost_limiter.get_user_stats(user_id, tier)

    # Check if approaching cost limit
    if stats["cost_percentage"] >= 80:
        alerts.append(
            {
                "type": "warning",
                "severity": "high" if stats["cost_percentage"] >= 95 else "medium",
                "message": f"You've used {stats['cost_percentage']:.1f}% of your monthly budget",
                "current_cost": stats["current_cost"],
                "max_cost": stats["max_cost"],
                "remaining_cost": stats["remaining_cost"],
            }
        )

    # Check BYOT requirements
    requirement = BYOT_REQUIREMENTS.get(tier)
    if requirement is not None and requirement > 0:
        if stats["request_count"] >= requirement:
            alerts.append(
                {
                    "type": "byot_required",
                    "severity": "high",
                    "message": f"BYOT required after {requirement} calls/month",
                    "current_usage": stats["request_count"],
                    "threshold": requirement,
                }
            )
        elif stats["request_count"] >= requirement * 0.8:
            alerts.append(
                {
                    "type": "byot_recommended",
                    "severity": "medium",
                    "message": f"BYOT recommended after {requirement} calls/month",
                    "current_usage": stats["request_count"],
                    "threshold": requirement,
                }
            )

    # Check if using expensive model on low tier
    if tier in [Tier.FREE, Tier.HOBBY]:
        alerts.append(
            {
                "type": "tier_model_warning",
                "message": f"Tier '{tier.value}' may have limited access to premium models",
                "severity": "info",
            }
        )

    return alerts


@router.post("/record")
async def record_request_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
    user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """
    Record a request and its cost.

    This endpoint is called after a request completes to update cost tracking.
    """
    user_id = _require_user_id(user)

    result = await cost_integration.record_request(
        user_id=user_id,
        model=model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )

    return result
