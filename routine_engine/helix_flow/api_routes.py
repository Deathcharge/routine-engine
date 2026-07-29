"""
Helix Flow API Routes
Flow-based chain and agent execution REST API.
"""

import logging
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from apps.backend.core.unified_auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/flows", tags=["Helix Flow"])

_flow_security = HTTPBearer(auto_error=False)
_UNSET = object()
_builder_guard = _UNSET


async def _require_auth(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(_flow_security),
) -> dict[str, Any]:
    return await get_current_user(request=request, credentials=credentials)


async def _require_builder(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(_flow_security),
) -> dict[str, Any]:
    global _builder_guard
    if _builder_guard is _UNSET:
        try:
            from apps.backend.saas.guards import require_builder as _g

            _builder_guard = _g
        except ImportError:
            logger.error("require_builder unavailable — protected flow execution is disabled")
            raise HTTPException(status_code=503, detail="Flow authorization is unavailable") from None
    return await _builder_guard(request=request, credentials=credentials)  # type: ignore[operator]


def _make_llm(user_tier: str | None = None):
    """Return an async callable wrapping resilient_chat for helix_flow agents."""

    async def _call(prompt: str, stop: list[str] | None = None, **kwargs: Any) -> str:
        from apps.backend.services.resilient_llm import resilient_chat

        text, _ = await resilient_chat(
            [{"role": "user", "content": prompt}],
            user_tier=user_tier,
            max_tokens=kwargs.get("max_tokens", 2048),
        )
        return text

    return _call


@router.get("/status")
async def get_flow_status(
    current_user: dict = Depends(_require_auth),
) -> dict[str, Any]:
    """Get Flow engine status and available chain types."""
    return {
        "service": "helix-flow",
        "status": "operational",
        "version": "1.0.0",
        "chain_types": ["agent", "react", "conversational", "sequential", "parallel"],
        "memory_types": ["conversation", "window", "summary", "vector", "entity", "composite"],
        "active_flows": 0,
        "_default": False,
    }


@router.post("/execute")
async def execute_flow(
    body: dict[str, Any],
    current_user: dict = Depends(_require_builder),
) -> dict[str, Any]:
    """
    Execute a Helix Flow agent or chain against the platform LLM.

    Body:
      task        (str, required) — the task or prompt to process
      chain_type  (str)  — "agent" (default) | "react" | "conversational"
      inputs      (dict) — extra {key: value} pairs substituted into {key} placeholders in task
      max_steps   (int)  — max agent iterations, default 5, capped at 10
    """
    task: str | None = body.get("task") or body.get("input") or body.get("text")
    if not task:
        raise HTTPException(status_code=422, detail="'task' field is required")

    chain_type: str = str(body.get("chain_type", "agent")).lower()
    max_steps: int = min(int(body.get("max_steps", 5)), 10)
    extra_inputs: dict[str, Any] = body.get("inputs") or {}
    for k, v in extra_inputs.items():
        task = task.replace(f"{{{k}}}", str(v))

    user_tier = current_user.get("tier") or current_user.get("subscription_tier")
    llm = _make_llm(user_tier=user_tier)
    execution_id = str(uuid.uuid4())[:8]

    try:
        from apps.backend.helix_flow.agents import (
            Agent,
            ConversationalAgent,
            PlanAndExecuteAgent,
            ReActAgent,
        )

        agent: Agent
        if chain_type in ("agent", "plan"):
            agent = PlanAndExecuteAgent(llm=llm, max_iterations=max_steps, verbose=False)
        elif chain_type == "react":
            agent = ReActAgent(llm=llm, max_iterations=max_steps, verbose=False)
        else:
            # "conversational" / "sequential" / anything else → single-turn agent
            agent = ConversationalAgent(llm=llm, max_iterations=1, verbose=False)

        result = await agent.run(task)

        return {
            "success": result.success,
            "execution_id": execution_id,
            "chain_type": chain_type,
            "output": result.output,
            "steps_executed": result.iterations,
            "steps": [s.to_dict() for s in result.steps],
            "error": result.error,
            "execution_time_ms": result.execution_time_ms,
            "_default": False,
        }

    except HTTPException:
        raise
    except ImportError as e:
        logger.error("helix_flow module unavailable: %s", e)
        raise HTTPException(status_code=503, detail="Flow engine temporarily unavailable") from None
    except Exception as e:
        logger.error("Flow execution [%s] failed: %s", execution_id, e)
        raise HTTPException(status_code=500, detail=f"Flow execution failed: {e!s}") from e


@router.get("/chains")
async def list_chains(
    current_user: dict = Depends(_require_auth),
) -> dict[str, Any]:
    """List available chain / agent types with descriptions."""
    return {
        "success": True,
        "chains": [
            {
                "id": "agent",
                "name": "Plan & Execute Agent",
                "description": "Decomposes the task into a plan then executes each step sequentially",
                "tier": "builder",
            },
            {
                "id": "react",
                "name": "ReAct Agent",
                "description": "Interleaves Reasoning and Acting (Thought → Action → Observation loop)",
                "tier": "builder",
            },
            {
                "id": "conversational",
                "name": "Conversational Agent",
                "description": "Single-turn LLM response with optional tool use; good for Q&A",
                "tier": "builder",
            },
            {
                "id": "sequential",
                "name": "Sequential Chain",
                "description": "Execute a series of steps in order, piping output between them",
                "tier": "builder",
            },
            {
                "id": "parallel",
                "name": "Parallel Chain",
                "description": "Execute independent steps concurrently and combine results",
                "tier": "pro",
            },
            {
                "id": "map_reduce",
                "name": "Map-Reduce Chain",
                "description": "Process a collection in parallel then reduce to a final output",
                "tier": "pro",
            },
        ],
        "total": 6,
        "_default": False,
    }


@router.get("/memory/types")
async def get_memory_types(
    current_user: dict = Depends(_require_auth),
) -> dict[str, Any]:
    """Get available memory types for flow agents."""
    return {
        "success": True,
        "types": [
            {"id": "conversation", "name": "Conversation", "description": "Full message history"},
            {"id": "window", "name": "Window", "description": "Sliding window of recent messages"},
            {"id": "summary", "name": "Summary", "description": "LLM-compressed context summary"},
            {"id": "vector", "name": "Vector", "description": "Semantic similarity retrieval"},
            {"id": "entity", "name": "Entity", "description": "Tracks named entities across turns"},
            {"id": "composite", "name": "Composite", "description": "Combines multiple memory types"},
        ],
        "_default": False,
    }
