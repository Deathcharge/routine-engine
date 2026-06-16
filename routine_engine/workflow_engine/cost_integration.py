"""
Cost Protection Integration Layer
Integrates cost protection with existing rate limiting and billing systems
"""

import logging
from datetime import UTC
from typing import Any, cast

from apps.backend.billing.unified_billing import UnifiedBillingService
from apps.backend.config.unified_pricing import Tier, get_tier_config
from apps.backend.core.unified_rate_limiter import UnifiedRateLimiter
from apps.backend.services.byot_service import get_user_byot_status

from .cost_protection import BYOT_REQUIREMENTS, MODEL_COSTS, TIER_MODEL_RESTRICTIONS, cost_limiter

logger = logging.getLogger(__name__)


class CostProtectionIntegration:
    """
    Integrates cost protection with existing systems.
    Provides a unified interface for checking all cost-related limits.
    """

    def __init__(self):
        self.billing_service = UnifiedBillingService()
        self.rate_limiter = UnifiedRateLimiter()

    async def check_request_allowed(
        self,
        user_id: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        check_byot: bool = True,
    ) -> tuple[bool, str | None, dict[str, Any]]:
        """
        Comprehensive check if request is allowed.
        Checks rate limits, cost limits, model access, token limits, and BYOT requirements.

        Args:
            user_id: User identifier
            model: Model to use
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            check_byot: Whether to check BYOT requirements

        Returns:
            (allowed, reason, metadata) tuple
        """
        metadata: dict[str, Any] = {}

        # Get user tier
        tier = self.billing_service.get_user_tier(user_id)
        metadata["tier"] = tier.value

        # Check rate limits (existing system)
        rate_allowed, rate_reason = await self._check_rate_limit(user_id, tier)
        if not rate_allowed:
            return False, rate_reason, metadata

        # Check model access
        model_allowed, model_reason = cost_limiter.check_model_access(user_id, tier, model)
        if not model_allowed:
            return False, model_reason, metadata

        # Check token limits
        total_tokens = input_tokens + output_tokens
        token_allowed, token_reason = cost_limiter.check_token_limit(user_id, tier, total_tokens)
        if not token_allowed:
            return False, token_reason, metadata

        # Check BYOT requirements
        if check_byot:
            byot_status = await get_user_byot_status(user_id)
            has_byot = bool(getattr(byot_status, "enabled", False))
            byot_allowed, byot_reason = cost_limiter.check_byot_requirement(user_id, tier, has_byot)
            if not byot_allowed:
                return False, byot_reason, metadata
            metadata["has_byot"] = has_byot

        # Calculate estimated cost
        estimated_cost = cost_limiter.calculate_request_cost(model, input_tokens, output_tokens)
        metadata["estimated_cost"] = estimated_cost

        # Check cost limits
        cost_allowed, cost_reason = cost_limiter.check_cost_limit(user_id, tier, estimated_cost)
        if not cost_allowed:
            return False, cost_reason, metadata

        # All checks passed
        return True, None, metadata

    async def _check_rate_limit(
        self,
        user_id: str,
        tier: Tier,
    ) -> tuple[bool, str | None]:
        """
        Check existing rate limits.
        Integrates with UnifiedRateLimiter.
        """
        # This would integrate with the existing rate limiter
        # For now, we'll use the tier's API call limits
        get_tier_config(tier)

        # Check if user has exceeded API call limit
        allowed, reason = self.billing_service.check_usage_limit(
            user_id,
            "api_calls_monthly",
            1,
        )

        if not allowed:
            return False, reason

        return True, None

    async def record_request(
        self,
        user_id: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Record a request and update all tracking systems.

        Args:
            user_id: User identifier
            model: Model used
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            metadata: Optional additional metadata

        Returns:
            Dictionary with request details
        """
        # Record cost
        cost = cost_limiter.record_request(user_id, model, input_tokens, output_tokens)

        # Update billing usage
        tier = self.billing_service.get_user_tier(user_id)
        self.billing_service.check_usage_limit(user_id, "api_calls_monthly", 1)

        # Get user stats
        stats = cost_limiter.get_user_stats(user_id, tier)

        result = {
            "user_id": user_id,
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "cost": cost,
            "tier": tier.value,
            "stats": stats,
        }

        if metadata:
            result["metadata"] = metadata

        logger.info("Recorded request: %s", result)

        return result

    def get_user_cost_report(self, user_id: str) -> dict[str, Any]:
        """
        Get comprehensive cost report for user.

        Args:
            user_id: User identifier

        Returns:
            Dictionary with cost report
        """
        tier = self.billing_service.get_user_tier(user_id)
        stats = cost_limiter.get_user_stats(user_id, tier)

        # Get tier configuration
        tier_config = get_tier_config(tier)

        return {
            "user_id": user_id,
            "tier": tier.value,
            "tier_price": tier_config.price_monthly,
            "current_cost": stats["current_cost"],
            "remaining_cost": stats["remaining_cost"],
            "cost_percentage": stats["cost_percentage"],
            "request_count": stats["request_count"],
            "avg_cost_per_request": stats["avg_cost_per_request"],
            "projected_monthly_cost": self._project_monthly_cost(user_id, tier),
            "byot_status": self._get_byot_recommendation(user_id, tier, stats),
        }

    def _project_monthly_cost(self, user_id: str, tier: Tier) -> dict[str, Any]:
        """
        Project monthly cost based on current usage.
        """
        stats = cost_limiter.get_user_stats(user_id, tier)
        current_cost = stats["current_cost"]

        # Get days in month
        import calendar
        from datetime import datetime

        now = datetime.now(UTC)
        days_in_month = calendar.monthrange(now.year, now.month)[1]
        current_day = now.day

        # Project based on daily average
        if current_day > 0:
            daily_avg_cost = current_cost / current_day
            projected_cost = daily_avg_cost * days_in_month
        else:
            projected_cost = current_cost

        return {
            "current_day": current_day,
            "days_in_month": days_in_month,
            "daily_avg_cost": current_cost / current_day if current_day > 0 else 0,
            "projected_cost": projected_cost,
            "will_exceed_budget": projected_cost > stats["max_cost"],
        }

    def _get_byot_recommendation(
        self,
        user_id: str,
        tier: Tier,
        stats: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Get BYOT recommendation for user.
        """
        requirement = BYOT_REQUIREMENTS.get(tier)

        if requirement is None:
            return {
                "required": False,
                "reason": "BYOT is optional for your tier",
                "savings_potential": 0,
            }

        if requirement == 0:
            return {
                "required": True,
                "reason": f"{tier.value} tier requires BYOT for all usage",
                "savings_potential": stats["current_cost"],
            }

        if stats["request_count"] >= requirement:
            return {
                "required": True,
                "reason": f"BYOT required after {requirement} calls/month",
                "savings_potential": stats["current_cost"],
            }

        # Calculate potential savings if user adopts BYOT
        projected_requests = requirement * 2  # Assume 2x growth
        avg_cost = stats["avg_cost_per_request"]
        potential_cost = projected_requests * avg_cost

        return {
            "required": False,
            "reason": f"BYOT optional, but recommended after {requirement} calls/month",
            "savings_potential": potential_cost,
            "current_usage": stats["request_count"],
            "threshold": requirement,
        }

    def get_model_recommendation(
        self,
        user_id: str,
        task_complexity: str = "medium",
    ) -> dict[str, Any]:
        """
        Recommend best model for user based on tier and task.

        Args:
            user_id: User identifier
            task_complexity: Complexity of task (simple, medium, complex, expert)

        Returns:
            Dictionary with model recommendation
        """
        tier = self.billing_service.get_user_tier(user_id)
        restrictions = TIER_MODEL_RESTRICTIONS.get(tier)

        if not restrictions:
            return {
                "error": f"No restrictions found for tier {tier}",
            }

        allowed_models_value = restrictions["allowed_models"]

        if allowed_models_value == "all":
            # All models available, recommend based on task complexity
            complexity_recommendations = {
                "simple": {"model": "gpt-3.5-turbo", "reason": "Cheapest model for simple tasks"},
                "medium": {"model": "claude-3-sonnet", "reason": "Good balance of cost and quality"},
                "complex": {"model": "gpt-4-turbo", "reason": "Best for complex tasks"},
            }
            return complexity_recommendations.get(
                task_complexity,
                {"model": "gpt-4", "reason": "Best for expert tasks"},
            )

        allowed_models = cast(list[str], allowed_models_value)

        # Filter available models based on complexity
        if task_complexity == "simple":
            # Always use cheapest available
            cheapest = min(
                allowed_models,
                key=lambda m: MODEL_COSTS.get(m, {}).get("input", 999),
            )
            return {
                "model": cheapest,
                "reason": "Cheapest available model for simple tasks",
            }

        elif task_complexity == "medium":
            # Use best value model
            if "claude-3-sonnet" in allowed_models:
                return {"model": "claude-3-sonnet", "reason": "Good balance of cost and quality"}
            elif "gpt-4o" in allowed_models:
                return {"model": "gpt-4o", "reason": "Good balance of cost and quality"}
            else:
                return {"model": allowed_models[0], "reason": "Best available model"}

        elif task_complexity == "complex":
            # Use best available model
            if "gpt-4-turbo" in allowed_models:
                return {"model": "gpt-4-turbo", "reason": "Best available for complex tasks"}
            elif "claude-3-5-sonnet" in allowed_models:
                return {"model": "claude-3-5-sonnet", "reason": "Best available for complex tasks"}
            else:
                return {"model": allowed_models[-1], "reason": "Best available model"}

        else:  # expert
            # Expert tasks may not be available on lower tiers
            if tier in [Tier.FREE, Tier.HOBBY]:
                return {
                    "error": f"Expert tasks not available on {tier.value} tier",
                    "upgrade_to": "builder",
                }
            elif tier == Tier.BUILDER:
                return {
                    "error": f"Expert tasks not available on {tier.value} tier",
                    "upgrade_to": "pro",
                }
            else:
                # Pro or Enterprise
                if "gpt-4" in allowed_models:
                    return {"model": "gpt-4", "reason": "Best for expert tasks"}
                else:
                    return {"model": allowed_models[-1], "reason": "Best available model"}


# Global instance
cost_integration = CostProtectionIntegration()
