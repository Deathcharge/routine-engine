"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

🤖 Helix Copilot - Context-aware AI assistant for Helix Spirals
Zapier Copilot-style natural language interface for building and optimizing workflows
"""

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import anthropic
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

# Import official tier system
from apps.backend.saas.guards import (
    require_pro,
    require_starter,
)
from apps.backend.saas.models.subscription import SubscriptionTier

from .integration_nodes import NODE_REGISTRY
from .models import Spiral
from .optimizer import OptimizationReport, get_optimizer
from .storage import SpiralStorage

logger = logging.getLogger(__name__)

copilot_router = APIRouter()


class PageContext(BaseModel):
    """Context about the current page/view"""

    page: str
    route: str
    features: list[str] = []
    selectedItems: list[str] = []
    osContext: dict[str, Any] | None = None


class CopilotMessageRequest(BaseModel):
    """Request model for copilot messages"""

    message: str
    agent: str | None = "helix"
    context: PageContext | None = None
    useEnhanced: bool = False
    conversationHistory: list[dict[str, str]] = []


class CopilotMessageResponse(BaseModel):
    """Response model for copilot messages"""

    response: str
    agent: str
    isEnhanced: bool = False
    suggestedActions: list[dict[str, Any]] = []
    generatedSpiral: dict[str, Any] | None = None


# Agent personalities and system prompts
AGENT_PERSONALITIES = {
    "kael": {
        "name": "Kael",
        "role": "Ethical reasoning & moral guidance",
        "system_prompt": "You are Kael, an agent focused on ethical reasoning and moral decision-making. Help users build automations that respect privacy, consent, and ethical principles. Consider the broader impact of workflows.",
    },
    "lumina": {
        "name": "Lumina",
        "role": "Empathetic support & emotional intelligence",
        "system_prompt": "You are Lumina, an empathetic agent who understands emotional context. Help users build automations that support well-being, reduce stress, and enhance human connections. Consider emotional impact.",
    },
    "vega": {
        "name": "Vega",
        "role": "Strategic planning & optimization",
        "system_prompt": "You are Vega, a strategic planning agent. Help users design efficient, scalable workflows. Focus on optimization, resource management, and long-term planning.",
    },
    "oracle": {
        "name": "Oracle",
        "role": "Pattern recognition & prediction",
        "system_prompt": "You are Oracle, a pattern recognition agent. Help users identify automation opportunities, predict workflow outcomes, and recognize patterns in their data flows.",
    },
    "aether": {
        "name": "Aether",
        "role": "System monitoring & coordination",
        "system_prompt": "You are Aether, a system monitoring agent. Help users build robust, observable workflows with proper monitoring, error handling, and coordination-aware routing.",
    },
    "vishwakarma": {
        "name": "Vishwakarma",
        "role": "Creative building & innovation",
        "system_prompt": "You are Vishwakarma, a creative building agent. Help users construct innovative, creative workflows that solve problems in novel ways. Think outside the box.",
    },
    "coordinator": {
        "name": "Coordination",
        "role": "Meta-cognition & reflection",
        "system_prompt": "You are Coordination, a meta-cognitive agent. Help users reflect on their automation patterns, understand deeper implications, and evolve their workflow strategies.",
    },
    "sanghacore": {
        "name": "SanghaCore",
        "role": "Collective coordination & collaboration",
        "system_prompt": "You are SanghaCore, a collective coordination agent. Help users build collaborative workflows that coordinate multiple people, teams, and systems harmoniously.",
    },
    "sage": {
        "name": "Sage",
        "role": "Wisdom & guidance",
        "system_prompt": "You are Sage, a wisdom agent. Help users make informed decisions about automation, learn best practices, and understand the deeper principles of workflow design.",
    },
    "helix": {
        "name": "Helix",
        "role": "General assistance & spiral building",
        "system_prompt": "You are Helix, the primary copilot for Helix Spirals. Help users build, optimize, and understand automation workflows. You can create spirals from natural language descriptions.",
    },
}


def get_available_nodes_description() -> str:
    """Generate description of available nodes for the AI"""
    node_descriptions = []

    for node_type, node_class in NODE_REGISTRY.items():
        desc = f"- **{node_type}**: {node_class.description}"
        node_descriptions.append(desc)

    return "\n".join(node_descriptions)


def get_storage() -> SpiralStorage:
    """Get storage instance from app state"""
    from .main import storage

    if not storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")
    return storage


async def build_spiral_from_description(
    description: str, user_id: str, enhanced: bool = False
) -> dict[str, Any] | None:
    """
    Use Claude to build a spiral from natural language description

    Args:
        description: Natural language description of desired workflow
        user_id: User identifier
        enhanced: Whether to use enhanced reasoning (AoT)

    Returns:
        Spiral configuration dict or None if unable to build
    """
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_key:
        logger.warning("ANTHROPIC_API_KEY not set, cannot build spirals")
        return None

    try:
        client = anthropic.Anthropic(api_key=anthropic_key)

        system_prompt = f"""You are a workflow automation expert. Convert natural language descriptions into Helix Spiral configurations.

Available node types:
{get_available_nodes_description()}

Your task: Convert the user's description into a JSON spiral configuration with:
1. A descriptive name
2. A list of nodes (each with: id, type, config, position)
3. Edges connecting nodes (each with: source, target)

Output ONLY valid JSON, no markdown or explanation. Structure:
{{
  "name": "Workflow Name",
  "description": "What this workflow does",
  "nodes": [
    {{"id": "1", "type": "trigger", "config": {{}}, "position": {{"x": 100, "y": 100}}}},
    {{"id": "2", "type": "action_type", "config": {{}}, "position": {{"x": 300, "y": 100}}}}
  ],
  "edges": [
    {{"source": "1", "target": "2"}}
  ]
}}"""

        if enhanced:
            # Use extended thinking for Pro/Enterprise
            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=4096,
                thinking={"type": "enabled", "budget_tokens": 3000},
                messages=[
                    {
                        "role": "user",
                        "content": f"Build a spiral for: {description}",
                    }
                ],
                system=system_prompt,
            )
        else:
            # Standard for free tier
            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=2048,
                messages=[
                    {
                        "role": "user",
                        "content": f"Build a spiral for: {description}",
                    }
                ],
                system=system_prompt,
            )

        # Extract JSON from response
        content = response.content
        text_content = ""

        for block in content:
            if block.type == "text":
                text_content += block.text

        # Try to parse as JSON
        try:
            spiral_config = json.loads(text_content)
            return spiral_config
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code blocks
            if "```json" in text_content:
                json_start = text_content.find("```json") + 7
                json_end = text_content.find("```", json_start)
                json_str = text_content[json_start:json_end].strip()
                spiral_config = json.loads(json_str)
                return spiral_config
            elif "```" in text_content:
                json_start = text_content.find("```") + 3
                json_end = text_content.find("```", json_start)
                json_str = text_content[json_start:json_end].strip()
                spiral_config = json.loads(json_str)
                return spiral_config
            else:
                logger.error("Failed to extract JSON from response: %s", text_content)
                return None

    except Exception as e:
        logger.error("Error building spiral: %s", e)
        return None


async def get_copilot_response(
    message: str,
    agent: str,
    context: PageContext | None,
    enhanced: bool,
    tier: SubscriptionTier,
    conversation_history: list[dict[str, str]],
) -> CopilotMessageResponse:
    """
    Get response from copilot agent

    Args:
        message: User message
        agent: Agent ID to use
        context: Page context
        enhanced: Whether to use enhanced reasoning (Pro+ only)
        tier: User subscription tier (5-tier system)
        conversation_history: Previous messages in conversation

    Returns:
        CopilotMessageResponse with agent response
    """
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_key:
        return CopilotMessageResponse(
            response="I apologize, but I'm not fully configured yet. Please set the ANTHROPIC_API_KEY environment variable.",
            agent=agent,
            isEnhanced=False,
        )

    # Get agent personality
    agent_info = AGENT_PERSONALITIES.get(agent, AGENT_PERSONALITIES["helix"])

    # Build context string
    context_str = ""
    if context:
        context_str = f"""
Current Context:
- Page: {context.page}
- Route: {context.route}
- Visible Features: {", ".join(context.features) if context.features else "None"}
- Selected Items: {", ".join(context.selectedItems) if context.selectedItems else "None"}
"""

    # System prompt
    system_prompt = f"""{agent_info["system_prompt"]}

You are part of Helix Spirals, a Zapier replacement platform with coordination-aware automation.
You help users build, optimize, and understand their automation workflows.

{context_str}

When users ask you to create a workflow or automation:
1. Understand their requirements
2. Suggest the appropriate nodes and connections
3. If they confirm, use the BUILD_SPIRAL command

To build a spiral, respond with: BUILD_SPIRAL: <description>

Available integrations include: {", ".join(NODE_REGISTRY.keys())}
"""

    try:
        client = anthropic.Anthropic(api_key=anthropic_key)

        # Build message history
        messages = []
        for msg in conversation_history[-10:]:  # Last 10 messages
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})

        # Add current message
        messages.append({"role": "user", "content": message})

        # Call Claude - Enhanced reasoning (AoT) available for PRO and ENTERPRISE only
        can_use_enhanced = tier in [SubscriptionTier.PRO, SubscriptionTier.ENTERPRISE]

        if enhanced and can_use_enhanced:
            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=2048,
                thinking={"type": "enabled", "budget_tokens": 2000},
                messages=messages,
                system=system_prompt,
            )
        else:
            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=1024,
                messages=messages,
                system=system_prompt,
            )

        # Extract text response
        response_text = ""
        for block in response.content:
            if block.type == "text":
                response_text += block.text

        # Check if agent wants to build a spiral
        generated_spiral = None
        suggested_actions = []

        if "BUILD_SPIRAL:" in response_text:
            # Extract description
            build_start = response_text.find("BUILD_SPIRAL:") + 13
            description = response_text[build_start:].strip()

            # Build the spiral
            spiral_config = await build_spiral_from_description(description, user_id="current_user", enhanced=enhanced)

            if spiral_config:
                generated_spiral = spiral_config
                suggested_actions.append(
                    {
                        "type": "save_spiral",
                        "label": "Save this workflow",
                        "spiral": spiral_config,
                    }
                )

            # Remove BUILD_SPIRAL command from response
            response_text = response_text[: response_text.find("BUILD_SPIRAL:")].strip()

        return CopilotMessageResponse(
            response=response_text,
            agent=agent,
            isEnhanced=enhanced and can_use_enhanced,
            suggestedActions=suggested_actions,
            generatedSpiral=generated_spiral,
        )

    except Exception as e:
        logger.error("Error getting copilot response: %s", e)
        return CopilotMessageResponse(
            response="I apologize, I encountered an error processing your request. Please try again.",
            agent=agent,
            isEnhanced=False,
        )


@copilot_router.post("/message", response_model=CopilotMessageResponse)
async def handle_copilot_message(
    request: CopilotMessageRequest,
    user=Depends(lambda: None),  # Optional auth - will work for anonymous/free tier
):
    """
    Handle copilot message and return AI response

    Supports:
    - Context-aware assistance (all tiers)
    - Natural language spiral building (based on tier spiral limits)
    - Enhanced reasoning (Pro/Enterprise only)

    Tier Requirements:
    - FREE/HOBBY/STARTER: Basic copilot without enhanced reasoning
    - PRO/ENTERPRISE: Enhanced reasoning with extended thinking

    Usage Limits:
    - FREE: 20 messages/month
    - HOBBY: 100 messages/month
    - STARTER: 500 messages/month
    - PRO: 2000 messages/month + 200 enhanced
    - ENTERPRISE: Unlimited basic + 1000 enhanced
    """
    # Determine user tier and ID
    if user:
        user_id = str(getattr(user, "id", None))
        tier_value = getattr(user, "subscription_tier", "free").lower()
        try:
            tier = SubscriptionTier(tier_value)
        except ValueError:
            tier = SubscriptionTier.FREE
    else:
        # Anonymous/unauthenticated users default to FREE tier
        user_id = None
        tier = SubscriptionTier.FREE

    # Enhanced reasoning requires Pro or Enterprise
    if request.useEnhanced and tier not in [
        SubscriptionTier.PRO,
        SubscriptionTier.ENTERPRISE,
    ]:
        raise HTTPException(
            status_code=403,
            detail="Enhanced reasoning requires Pro or Enterprise tier. "
            "Upgrade at /marketplace/pricing to unlock extended thinking capabilities.",
        )

    # 🔒 USAGE QUOTA CHECK: Copilot messages
    if user_id:
        from apps.backend.services.usage_service import check_quota, track_usage

        # Check basic copilot message quota
        resource_type = "copilot_messages_per_month"
        allowed, reason, limits_info = await check_quota(user_id, resource_type, 1)

        if not allowed:
            raise HTTPException(
                status_code=429,
                detail=f"Copilot message limit exceeded. {reason}",
                headers={
                    "X-Usage-Current": str(limits_info.get("current", 0)),
                    "X-Usage-Limit": str(limits_info.get("limit", 0)),
                    "X-Upgrade-URL": "/marketplace/pricing",
                },
            )

        # Additional check for enhanced reasoning
        if request.useEnhanced:
            enhanced_resource = "copilot_enhanced_limit"
            enhanced_allowed, enhanced_reason, enhanced_limits = await check_quota(user_id, enhanced_resource, 1)

            if not enhanced_allowed:
                raise HTTPException(
                    status_code=429,
                    detail=f"Enhanced reasoning limit exceeded. {enhanced_reason}",
                    headers={
                        "X-Usage-Current": str(enhanced_limits.get("current", 0)),
                        "X-Usage-Limit": str(enhanced_limits.get("limit", 0)),
                        "X-Upgrade-URL": "/marketplace/pricing",
                    },
                )

    response = await get_copilot_response(
        message=request.message,
        agent=request.agent or "helix",
        context=request.context,
        enhanced=request.useEnhanced,
        tier=tier,
        conversation_history=request.conversationHistory,
    )

    # 📊 TRACK USAGE: Record copilot message usage
    if user_id:
        # Track basic copilot message
        await track_usage(
            user_id=user_id,
            resource_type="copilot_messages_per_month",
            quantity=1,
            metadata={
                "agent": request.agent or "helix",
                "enhanced": request.useEnhanced,
                "context_page": request.context.page if request.context else None,
            },
        )

        # Track enhanced reasoning separately
        if request.useEnhanced:
            await track_usage(
                user_id=user_id,
                resource_type="copilot_enhanced_limit",
                quantity=1,
                metadata={"agent": request.agent or "helix"},
            )

    return response


@copilot_router.post("/build-spiral")
async def build_spiral(
    description: str,
    user_id: str,
    enhanced: bool = False,
    save: bool = False,
):
    """
    Build a spiral from natural language description

    Args:
        description: Natural language workflow description
        user_id: User identifier
        enhanced: Use enhanced reasoning (Pro/Enterprise)
        save: Automatically save to database

    Returns:
        Generated spiral configuration
    """
    spiral_config = await build_spiral_from_description(description, user_id, enhanced)

    if not spiral_config:
        raise HTTPException(status_code=500, detail="Failed to generate spiral from description")

    # Optionally save to database
    if save:
        storage = get_storage()

        # Convert to Spiral model
        spiral = Spiral(
            id=f"copilot-{datetime.now(UTC).timestamp()}",
            name=spiral_config.get("name", "Untitled Spiral"),
            description=spiral_config.get("description", ""),
            user_id=user_id,
            nodes=[],  # Would need to convert nodes
            edges=spiral_config.get("edges", []),
            enabled=False,  # Don't auto-enable
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        await storage.create_spiral(spiral)

    return {"spiral": spiral_config, "saved": save}


@copilot_router.get("/suggestions")
async def get_suggestions(context: str | None = None):
    """
    Get context-aware suggestions for the user

    Args:
        context: Current page/context

    Returns:
        List of suggested prompts and actions
    """
    # Context-specific suggestions
    suggestions_map = {
        "builder": [
            "Build a workflow that sends Slack notifications when a form is submitted",
            "Create an automation to sync GitHub issues with Notion",
            "Help me optimize this workflow for better performance",
        ],
        "marketplace": [
            "Show me popular automation templates",
            "Find workflows for social media management",
            "What integrations are available?",
        ],
        "executions": [
            "Why did my last workflow fail?",
            "How can I improve execution success rate?",
            "Analyze recent execution patterns",
        ],
        "default": [
            "Build a workflow for me",
            "What can Helix Spirals do?",
            "How do I get started with automation?",
        ],
    }

    context_key = context if context in suggestions_map else "default"
    suggestions = suggestions_map[context_key]

    return {"suggestions": suggestions}


@copilot_router.post("/optimize/{spiral_id}", response_model=OptimizationReport)
async def optimize_spiral(
    spiral_id: str,
    enhanced: bool = False,
    user=Depends(require_starter),  # Optimizer requires Starter tier or higher
):
    """
    Analyze and optimize a spiral workflow

    Tier Requirements:
    - STARTER+: Basic optimization with rule-based suggestions
    - PRO/ENTERPRISE: Enhanced AI optimization with extended thinking

    Args:
        spiral_id: ID of the spiral to optimize
        enhanced: Use enhanced AI reasoning (Pro/Enterprise only)

    Returns:
        Optimization report with suggestions and metrics
    """
    storage = get_storage()

    # Get spiral from storage
    spiral = await storage.get_spiral(spiral_id)
    if not spiral:
        raise HTTPException(status_code=404, detail="Spiral not found")

    # Get user tier
    tier_value = getattr(user, "subscription_tier", "free").lower()
    try:
        tier = SubscriptionTier(tier_value)
    except ValueError:
        tier = SubscriptionTier.FREE

    # Enhanced optimization requires Pro or Enterprise
    if enhanced and tier not in [SubscriptionTier.PRO, SubscriptionTier.ENTERPRISE]:
        raise HTTPException(
            status_code=403,
            detail="Enhanced optimization requires Pro or Enterprise tier. "
            "You'll receive rule-based suggestions. Upgrade for AI-powered analysis.",
        )

    # Analyze spiral
    optimizer = get_optimizer()
    report = await optimizer.analyze_spiral(spiral, use_enhanced=enhanced)

    return report


@copilot_router.get("/optimize/batch")
async def batch_optimize_spirals(
    user_id: str | None = None,
    enhanced: bool = False,
    limit: int = 10,
    user=Depends(require_pro),  # Batch optimization requires Pro tier or higher
):
    """
    Analyze and optimize multiple spirals at once

    Tier Requirements:
    - PRO: Batch optimization up to 10 spirals
    - ENTERPRISE: Unlimited batch optimization

    Args:
        user_id: Filter by user ID
        enhanced: Use enhanced AI reasoning (Pro/Enterprise)
        limit: Maximum number of spirals to analyze (max 10 for Pro, unlimited for Enterprise)

    Returns:
        List of optimization reports
    """
    storage = get_storage()
    optimizer = get_optimizer()

    # Get user tier
    tier_value = getattr(user, "subscription_tier", "free").lower()
    try:
        tier = SubscriptionTier(tier_value)
    except ValueError:
        tier = SubscriptionTier.FREE

    # Enforce batch limits by tier
    if tier == SubscriptionTier.PRO:
        limit = min(limit, 10)  # Pro tier capped at 10
    # Enterprise has no limit

    # Get spirals
    spirals = await storage.get_all_spirals()

    # Filter by user if specified
    if user_id:
        spirals = [s for s in spirals if s.user_id == user_id]

    # Limit results
    spirals = spirals[:limit]

    # Analyze each spiral
    reports = []
    for spiral in spirals:
        try:
            report = await optimizer.analyze_spiral(spiral, use_enhanced=enhanced)
            reports.append(report)
        except Exception as e:
            logger.error("Error optimizing spiral %s: %s", spiral.id, e)
            continue

    return {"reports": reports, "analyzed": len(reports), "total": len(spirals)}
