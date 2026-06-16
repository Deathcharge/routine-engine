"""
Cost Protection System for Helix Platform
Ensures API costs don't exceed subscription revenue
"""

import logging
from datetime import UTC, datetime
from typing import TypedDict

from apps.backend.config.unified_pricing import Tier, get_tier_config

logger = logging.getLogger(__name__)


# Model cost per 1M tokens (input/output)
MODEL_COSTS = {
    # OpenAI
    "gpt-4": {"input": 30.0, "output": 60.0},
    "gpt-4-turbo": {"input": 10.0, "output": 30.0},
    "gpt-4o": {"input": 2.50, "output": 10.0},
    "gpt-3.5-turbo": {"input": 0.50, "output": 1.50},
    # Anthropic
    "claude-3-opus": {"input": 15.0, "output": 75.0},
    "claude-3-sonnet": {"input": 3.0, "output": 15.0},
    "claude-3-5-sonnet": {"input": 3.0, "output": 15.0},
    "claude-3-haiku": {"input": 0.25, "output": 1.25},
    # Google
    "gemini-pro": {"input": 0.50, "output": 1.50},
    "gemini-1.5-pro": {"input": 1.25, "output": 5.0},
    # Perplexity
    "llama-3.1-sonar-small": {"input": 0.20, "output": 0.20},
    "llama-3.1-sonar-large": {"input": 1.00, "output": 1.00},
}


class TierModelRestrictions(TypedDict):
    allowed_models: list[str] | str
    max_tokens_per_request: int
    max_agents: int


# Tier-specific model restrictions
TIER_MODEL_RESTRICTIONS: dict[Tier, TierModelRestrictions] = {
    Tier.FREE: {
        "allowed_models": ["gpt-3.5-turbo", "claude-3-haiku"],
        "max_tokens_per_request": 1000,
        "max_agents": 1,
    },
    Tier.HOBBY: {
        "allowed_models": ["gpt-3.5-turbo", "claude-3-haiku", "claude-3-sonnet"],
        "max_tokens_per_request": 2000,
        "max_agents": 2,
    },
    Tier.BUILDER: {
        "allowed_models": [
            "gpt-3.5-turbo",
            "gpt-4o",
            "claude-3-haiku",
            "claude-3-sonnet",
            "claude-3-5-sonnet",
            "gemini-pro",
        ],
        "max_tokens_per_request": 4000,
        "max_agents": 5,
    },
    Tier.PRO: {
        "allowed_models": [
            "gpt-3.5-turbo",
            "gpt-4o",
            "gpt-4-turbo",
            "claude-3-haiku",
            "claude-3-sonnet",
            "claude-3-5-sonnet",
            "gemini-pro",
            "gemini-1.5-pro",
        ],
        "max_tokens_per_request": 8000,
        "max_agents": -1,  # Unlimited
    },
    Tier.ENTERPRISE: {
        "allowed_models": "all",  # All models
        "max_tokens_per_request": 32000,
        "max_agents": 50,
    },
}


# BYOT requirements (minimum calls/month before BYOT required)
BYOT_REQUIREMENTS = {
    Tier.FREE: None,  # Optional
    Tier.HOBBY: None,  # Optional
    Tier.BUILDER: 5000,  # Required after 5K calls
    Tier.PRO: 50000,  # Required after 50K calls
    Tier.ENTERPRISE: 0,  # Required for all usage
}


class CostBasedRateLimiter:
    """
    Rate limiter based on actual API costs, not just request count.
    Ensures users don't exceed their subscription revenue.

    Uses Redis for persistence with write-through cache pattern.
    """

    # Redis key prefixes
    _COST_KEY_PREFIX = "cost_limiter:cost:"
    _REQUEST_KEY_PREFIX = "cost_limiter:requests:"
    _MONTH_TTL = 86400 * 35  # 35 days (covers full month + buffer)

    def __init__(self):
        # Write-through cache over Redis
        self._cost_cache: dict[str, dict[str, float]] = {}
        self._request_cache: dict[str, dict[str, int]] = {}

        # Alert thresholds
        self.alert_threshold = 0.8  # Alert at 80% of revenue
        self.cutoff_threshold = 1.0  # Cutoff at 100% of revenue

    def get_month_key(self) -> str:
        """Get current month key for tracking"""
        now = datetime.now(UTC)
        return f"{now.year}-{now.month:02d}"

    async def _get_redis(self):
        """Get Redis client with graceful fallback"""
        try:
            from apps.backend.core.redis_client import get_redis

            return await get_redis()
        except Exception as e:
            logger.warning("Redis unavailable for cost tracking: %s", e)
            return None

    async def _redis_save_cost(self, user_id: str, month_key: str, cost: float) -> None:
        """Persist cost to Redis"""
        redis = await self._get_redis()
        if redis:
            try:
                key = f"{self._COST_KEY_PREFIX}{user_id}:{month_key}"
                await redis.set(key, str(cost), ex=self._MONTH_TTL)
            except Exception as e:
                logger.warning("Failed to persist cost to Redis: %s", e)

    async def _redis_save_requests(self, user_id: str, month_key: str, count: int) -> None:
        """Persist request count to Redis"""
        redis = await self._get_redis()
        if redis:
            try:
                key = f"{self._REQUEST_KEY_PREFIX}{user_id}:{month_key}"
                await redis.set(key, str(count), ex=self._MONTH_TTL)
            except Exception as e:
                logger.warning("Failed to persist request count to Redis: %s", e)

    async def _redis_load_cost(self, user_id: str, month_key: str) -> float:
        """Load cost from Redis"""
        redis = await self._get_redis()
        if redis:
            try:
                key = f"{self._COST_KEY_PREFIX}{user_id}:{month_key}"
                value = await redis.get(key)
                return float(value) if value else 0.0
            except Exception as e:
                logger.warning("Failed to load cost from Redis: %s", e)
        return 0.0

    async def _redis_load_requests(self, user_id: str, month_key: str) -> int:
        """Load request count from Redis"""
        redis = await self._get_redis()
        if redis:
            try:
                key = f"{self._REQUEST_KEY_PREFIX}{user_id}:{month_key}"
                value = await redis.get(key)
                return int(value) if value else 0
            except Exception as e:
                logger.warning("Failed to load request count from Redis: %s", e)
        return 0

    async def get_user_monthly_cost(self, user_id: str) -> float:
        """Get user's cost for current month"""
        month_key = self.get_month_key()

        # Check cache first
        if user_id in self._cost_cache and month_key in self._cost_cache[user_id]:
            return self._cost_cache[user_id][month_key]

        # Load from Redis
        cost = await self._redis_load_cost(user_id, month_key)

        # Update cache
        if user_id not in self._cost_cache:
            self._cost_cache[user_id] = {}
        self._cost_cache[user_id][month_key] = cost

        return cost

    async def get_user_monthly_requests(self, user_id: str) -> int:
        """Get user's request count for current month"""
        month_key = self.get_month_key()

        # Check cache first
        if user_id in self._request_cache and month_key in self._request_cache[user_id]:
            return self._request_cache[user_id][month_key]

        # Load from Redis
        count = await self._redis_load_requests(user_id, month_key)

        # Update cache
        if user_id not in self._request_cache:
            self._request_cache[user_id] = {}
        self._request_cache[user_id][month_key] = count

        return count

    def calculate_request_cost(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
    ) -> float:
        """
        Calculate cost of a single API request.

        Args:
            model: Model name
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens

        Returns:
            Cost in USD
        """
        if model not in MODEL_COSTS:
            logger.warning("Unknown model: %s, using default cost", model)
            # Default to GPT-3.5-turbo pricing
            model = "gpt-3.5-turbo"

        costs = MODEL_COSTS[model]
        input_cost = (input_tokens / 1_000_000) * costs["input"]
        output_cost = (output_tokens / 1_000_000) * costs["output"]

        return input_cost + output_cost

    async def check_cost_limit(
        self,
        user_id: str,
        tier: Tier,
        estimated_cost: float,
    ) -> tuple[bool, str | None]:
        """
        Check if user has cost budget remaining.

        Args:
            user_id: User identifier
            tier: User's subscription tier
            estimated_cost: Estimated cost of request

        Returns:
            (allowed, reason) tuple
        """
        # Get tier configuration
        tier_config = get_tier_config(tier)
        max_cost: float = float(tier_config.price_monthly)

        # For free tier, use a small credit allowance instead of $0 price
        if max_cost <= 0:
            max_cost = 0.50  # $0.50 free tier credit allowance

        # Get current cost
        current_cost = await self.get_user_monthly_cost(user_id)

        # Check if within budget
        if current_cost + estimated_cost > max_cost:
            remaining = max_cost - current_cost
            return False, f"Cost limit exceeded. Remaining budget: ${remaining:.2f}"

        # Check alert threshold
        ratio = (current_cost + estimated_cost) / max_cost
        if ratio >= self.alert_threshold:
            logger.warning(
                "User %s at %.1f%% of cost budget ($%.2f/$%.2f)", user_id, ratio * 100, current_cost, max_cost
            )

        return True, None

    def check_model_access(
        self,
        user_id: str,
        tier: Tier,
        model: str,
    ) -> tuple[bool, str | None]:
        """
        Check if user has access to use a specific model.

        Args:
            user_id: User identifier
            tier: User's subscription tier
            model: Model name

        Returns:
            (allowed, reason) tuple
        """
        restrictions = TIER_MODEL_RESTRICTIONS.get(tier)
        if not restrictions:
            return False, f"No restrictions found for tier {tier}"

        allowed_models = restrictions["allowed_models"]

        if allowed_models == "all":
            return True, None

        if model not in allowed_models:
            return False, f"Model '{model}' not available on {tier.value} tier. Available: {', '.join(allowed_models)}"

        return True, None

    def check_token_limit(
        self,
        user_id: str,
        tier: Tier,
        total_tokens: int,
    ) -> tuple[bool, str | None]:
        """
        Check if request exceeds token limit for tier.

        Args:
            user_id: User identifier
            tier: User's subscription tier
            total_tokens: Total tokens in request

        Returns:
            (allowed, reason) tuple
        """
        restrictions = TIER_MODEL_RESTRICTIONS.get(tier)
        if not restrictions:
            return False, f"No restrictions found for tier {tier}"

        max_tokens = restrictions["max_tokens_per_request"]

        if max_tokens == -1:
            return True, None  # Unlimited

        if total_tokens > max_tokens:
            return False, f"Token limit exceeded. Maximum: {max_tokens}, Requested: {total_tokens}"

        return True, None

    async def check_byot_requirement(
        self,
        user_id: str,
        tier: Tier,
        has_byot: bool,
    ) -> tuple[bool, str | None]:
        """
        Check if user must bring their own API key.

        Args:
            user_id: User identifier
            tier: User's subscription tier
            has_byot: Whether user has BYOT configured

        Returns:
            (allowed, reason) tuple
        """
        requirement = BYOT_REQUIREMENTS.get(tier)

        if requirement is None:
            return True, None  # BYOT optional

        if requirement == 0:
            # BYOT required for all usage
            if not has_byot:
                return False, f"{tier.value} tier requires BYOT (Bring Your Own Token) for all usage"
            return True, None

        # Check if user has exceeded threshold
        request_count = await self.get_user_monthly_requests(user_id)

        if request_count >= requirement and not has_byot:
            return False, f"BYOT required after {requirement} calls/month. Current: {request_count}"

        return True, None

    async def record_request(
        self,
        user_id: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
    ) -> float:
        """
        Record a request and its cost.

        Args:
            user_id: User identifier
            model: Model used
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens

        Returns:
            Cost of the request
        """
        # Calculate cost
        cost = self.calculate_request_cost(model, input_tokens, output_tokens)

        # Get month key
        month_key = self.get_month_key()

        # Update cost tracking (write-through cache)
        if user_id not in self._cost_cache:
            self._cost_cache[user_id] = {}
        current_cost = await self.get_user_monthly_cost(user_id)
        new_cost = current_cost + cost
        self._cost_cache[user_id][month_key] = new_cost
        await self._redis_save_cost(user_id, month_key, new_cost)

        # Update request tracking (write-through cache)
        if user_id not in self._request_cache:
            self._request_cache[user_id] = {}
        current_requests = await self.get_user_monthly_requests(user_id)
        new_requests = current_requests + 1
        self._request_cache[user_id][month_key] = new_requests
        await self._redis_save_requests(user_id, month_key, new_requests)

        logger.info(
            "Recorded request for user %s: model=%s, tokens=%d, cost=$%.4f",
            user_id,
            model,
            input_tokens + output_tokens,
            cost,
        )

        return cost

    async def get_user_stats(self, user_id: str, tier: Tier) -> dict:
        """
        Get comprehensive user statistics.

        Args:
            user_id: User identifier
            tier: User's subscription tier

        Returns:
            Dictionary with user stats
        """
        tier_config = get_tier_config(tier)
        max_cost = tier_config.price_monthly
        current_cost = await self.get_user_monthly_cost(user_id)
        request_count = await self.get_user_monthly_requests(user_id)

        return {
            "user_id": user_id,
            "tier": tier.value,
            "max_cost": max_cost,
            "current_cost": current_cost,
            "remaining_cost": max_cost - current_cost,
            "cost_percentage": (current_cost / max_cost) * 100 if max_cost > 0 else 0,
            "request_count": request_count,
            "avg_cost_per_request": current_cost / request_count if request_count > 0 else 0,
        }


# Global instance
cost_limiter = CostBasedRateLimiter()
