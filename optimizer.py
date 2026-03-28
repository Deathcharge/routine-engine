"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

🎯 AI Workflow Optimizer - Analyze and improve Helix Spirals
Coordination-aware optimization using GPT-4 and UCF metrics
"""

import json
import logging
import os
from typing import Any

import anthropic
from pydantic import BaseModel

from .integration_nodes import NODE_REGISTRY
from .models import Spiral

logger = logging.getLogger(__name__)


class OptimizationSuggestion(BaseModel):
    """A single optimization suggestion"""

    type: str  # performance, reliability, cost, coordination, security
    severity: str  # low, medium, high, critical
    title: str
    description: str
    nodeIds: list[str] = []
    estimatedImpact: str
    implementation: str


class OptimizationReport(BaseModel):
    """Complete optimization report for a spiral"""

    spiralId: str
    spiralName: str
    overallScore: float  # 0-100
    suggestions: list[OptimizationSuggestion]
    metrics: dict[str, Any]
    summary: str


class WorkflowOptimizer:
    """AI-powered workflow optimizer for Helix Spirals.

    Works with both:
    - Pydantic ``Spiral`` objects (which have ``trigger`` + ``actions``)
    - Raw dicts from the frontend canvas (which have ``nodes`` + ``edges``)

    The internal helpers normalise both shapes into a uniform list of
    "node-like" dicts so the scoring logic works for either.
    """

    def __init__(self):
        self.client = None
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if api_key:
            self.client = anthropic.Anthropic(api_key=api_key)

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _get_nodes(spiral) -> list:
        """Return a uniform node list regardless of model shape."""
        # ReactFlow dict: { "nodes": [...], "edges": [...] }
        if isinstance(spiral, dict):
            return spiral.get("nodes", [])
        # Already has .nodes (duck typing)
        if hasattr(spiral, "nodes") and spiral.nodes is not None:
            return list(spiral.nodes)
        # Pydantic Spiral model: build synthetic nodes from actions
        actions = getattr(spiral, "actions", None) or []
        nodes = []
        for a in actions:
            nodes.append(
                type(
                    "_Node",
                    (),
                    {
                        "id": getattr(a, "id", ""),
                        "type": getattr(a, "type", getattr(a, "action_type", "unknown")),
                        "config": getattr(a, "config", {}),
                        "name": getattr(a, "name", ""),
                    },
                )()
            )
        return nodes

    @staticmethod
    def _get_edges(spiral) -> list:
        """Return edges list (only relevant for canvas-format spirals)."""
        if isinstance(spiral, dict):
            return spiral.get("edges", [])
        if hasattr(spiral, "edges") and spiral.edges is not None:
            return list(spiral.edges)
        return []

    # ----------------------------------------------------------------- scores
    def _calculate_complexity_score(self, spiral: Spiral) -> int:
        """Calculate workflow complexity (0-100)"""
        nodes = self._get_nodes(spiral)
        edges = self._get_edges(spiral)

        node_count = len(nodes)
        edge_count = len(edges)

        # Simple heuristic
        complexity = min(100, (node_count * 5) + (edge_count * 3))
        return complexity

    def _calculate_efficiency_score(self, spiral: Spiral) -> int:
        """Calculate workflow efficiency (0-100)"""
        nodes = self._get_nodes(spiral)
        score = 100

        if len(nodes) > 20:
            score -= (len(nodes) - 20) * 2

        error_handlers = sum(1 for node in nodes if "error" in (getattr(node, "type", "") or "").lower())
        if error_handlers == 0 and len(nodes) > 5:
            score -= 15

        return max(0, score)

    def _calculate_reliability_score(self, spiral: Spiral) -> int:
        """Calculate workflow reliability (0-100)"""
        nodes = self._get_nodes(spiral)
        score = 80

        has_error_handler = any("error" in (getattr(node, "type", "") or "").lower() for node in nodes)
        if has_error_handler:
            score += 10

        has_retry = any((getattr(node, "config", None) or {}).get("retry", False) for node in nodes)
        if has_retry:
            score += 10

        return min(100, score)

    def _calculate_coordination_score(self, spiral: Spiral) -> int:
        """Calculate coordination-awareness (0-100)"""
        nodes = self._get_nodes(spiral)
        score = 50

        uses_coordination = any("coordination" in str(getattr(node, "config", "")).lower() for node in nodes)
        if uses_coordination:
            score += 30

        uses_ucf = any("ucf" in str(getattr(node, "config", "")).lower() for node in nodes)
        if uses_ucf:
            score += 20

        return min(100, score)

    async def analyze_spiral(self, spiral: Spiral, use_enhanced: bool = False) -> OptimizationReport:
        """
        Analyze a spiral and generate optimization suggestions

        Args:
            spiral: The spiral to analyze
            use_enhanced: Use enhanced AI reasoning (Pro/Enterprise)

        Returns:
            Optimization report with suggestions
        """
        # Calculate baseline metrics
        complexity = self._calculate_complexity_score(spiral)
        efficiency = self._calculate_efficiency_score(spiral)
        reliability = self._calculate_reliability_score(spiral)
        coordination = self._calculate_coordination_score(spiral)

        overall_score = (efficiency + reliability + coordination) / 3

        metrics = {
            "complexity": complexity,
            "efficiency": efficiency,
            "reliability": reliability,
            "coordination": coordination,
            "nodeCount": len(self._get_nodes(spiral)),
            "edgeCount": len(self._get_edges(spiral)),
        }

        # Get AI suggestions if available
        suggestions = []
        summary = "Workflow analysis complete."

        if self.client:
            try:
                nodes = self._get_nodes(spiral)
                # Build spiral representation for AI
                spiral_json = {
                    "name": getattr(spiral, "name", "unknown"),
                    "description": getattr(spiral, "description", ""),
                    "nodes": [
                        {
                            "id": getattr(node, "id", ""),
                            "type": getattr(node, "type", ""),
                            "config": getattr(node, "config", {}),
                        }
                        for node in nodes
                    ],
                    "edges": self._get_edges(spiral),
                }

                system_prompt = f"""You are a workflow optimization expert for Helix Spirals, a Zapier replacement platform.

Analyze the provided workflow and suggest improvements in these areas:
1. **Performance**: Speed, resource usage, parallel execution opportunities
2. **Reliability**: Error handling, retry logic, fallback mechanisms
3. **Cost**: Reducing API calls, consolidating operations
4. **Coordination**: UCF metrics integration, awareness-based routing
5. **Security**: Data handling, authentication, privacy

Current metrics:
- Complexity: {complexity}/100
- Efficiency: {efficiency}/100
- Reliability: {reliability}/100
- Coordination: {coordination}/100

Available node types: {", ".join(NODE_REGISTRY.keys())}

Output ONLY valid JSON in this format:
{{
  "suggestions": [
    {{
      "type": "performance|reliability|cost|coordination|security",
      "severity": "low|medium|high|critical",
      "title": "Brief title",
      "description": "Detailed explanation",
      "nodeIds": ["node1", "node2"],
      "estimatedImpact": "High|Medium|Low impact description",
      "implementation": "How to implement this suggestion"
    }}
  ],
  "summary": "Overall assessment and key takeaways"
}}"""

                if use_enhanced:
                    response = self.client.messages.create(
                        model="claude-sonnet-4-5-20250929",
                        max_tokens=4096,
                        thinking={"type": "enabled", "budget_tokens": 3000},
                        messages=[
                            {
                                "role": "user",
                                "content": f"Analyze this workflow:\n\n{json.dumps(spiral_json, indent=2)}",
                            }
                        ],
                        system=system_prompt,
                    )
                else:
                    response = self.client.messages.create(
                        model="claude-sonnet-4-5-20250929",
                        max_tokens=2048,
                        messages=[
                            {
                                "role": "user",
                                "content": f"Analyze this workflow:\n\n{json.dumps(spiral_json, indent=2)}",
                            }
                        ],
                        system=system_prompt,
                    )

                # Extract JSON from response
                text_content = ""
                for block in response.content:
                    if block.type == "text":
                        text_content += block.text

                # Parse JSON
                try:
                    result = json.loads(text_content)
                    summary = result.get("summary", summary)

                    for sugg in result.get("suggestions", []):
                        suggestions.append(
                            OptimizationSuggestion(
                                type=sugg.get("type", "performance"),
                                severity=sugg.get("severity", "medium"),
                                title=sugg.get("title", ""),
                                description=sugg.get("description", ""),
                                nodeIds=sugg.get("nodeIds", []),
                                estimatedImpact=sugg.get("estimatedImpact", ""),
                                implementation=sugg.get("implementation", ""),
                            )
                        )

                except json.JSONDecodeError:
                    # Try to extract JSON from markdown
                    if "```json" in text_content:
                        json_start = text_content.find("```json") + 7
                        json_end = text_content.find("```", json_start)
                        json_str = text_content[json_start:json_end].strip()
                        result = json.loads(json_str)

                        summary = result.get("summary", summary)
                        for sugg in result.get("suggestions", []):
                            suggestions.append(
                                OptimizationSuggestion(
                                    type=sugg.get("type", "performance"),
                                    severity=sugg.get("severity", "medium"),
                                    title=sugg.get("title", ""),
                                    description=sugg.get("description", ""),
                                    nodeIds=sugg.get("nodeIds", []),
                                    estimatedImpact=sugg.get("estimatedImpact", ""),
                                    implementation=sugg.get("implementation", ""),
                                )
                            )

            except Exception as e:
                logger.error("Error getting AI suggestions: %s", e)
                # Add fallback suggestions based on metrics
                suggestions = self._get_fallback_suggestions(spiral, metrics)

        else:
            # No AI available, use rule-based suggestions
            suggestions = self._get_fallback_suggestions(spiral, metrics)

        return OptimizationReport(
            spiralId=getattr(spiral, "id", "unknown"),
            spiralName=getattr(spiral, "name", "unknown"),
            overallScore=overall_score,
            suggestions=suggestions,
            metrics=metrics,
            summary=summary,
        )

    def _get_fallback_suggestions(self, spiral: Spiral, metrics: dict[str, Any]) -> list[OptimizationSuggestion]:
        """Generate rule-based suggestions when AI is not available"""
        suggestions = []
        nodes = self._get_nodes(spiral)

        # Check for error handling
        has_error_handler = any("error" in (getattr(node, "type", "") or "").lower() for node in nodes)
        if not has_error_handler and len(nodes) > 3:
            suggestions.append(
                OptimizationSuggestion(
                    type="reliability",
                    severity="high",
                    title="Add Error Handling",
                    description="This workflow lacks error handling nodes. Consider adding error handlers to gracefully handle failures.",
                    nodeIds=[],
                    estimatedImpact="High - Prevents workflow failures from causing cascading issues",
                    implementation="Add condition nodes to check for errors and route to error handler nodes",
                )
            )

        # Check for retry logic
        has_retry = any((getattr(node, "config", None) or {}).get("retry", False) for node in nodes)
        if not has_retry:
            suggestions.append(
                OptimizationSuggestion(
                    type="reliability",
                    severity="medium",
                    title="Enable Retry Logic",
                    description="Enable retry logic on critical nodes to handle transient failures.",
                    nodeIds=[
                        getattr(node, "id", "")
                        for node in nodes
                        if (getattr(node, "type", "") or "") in ("http", "api", "webhook")
                    ],
                    estimatedImpact="Medium - Reduces failures from temporary issues",
                    implementation="Set 'retry: true' and 'max_retries: 3' in node configs",
                )
            )

        # Check for coordination integration
        if metrics["coordination"] < 60:
            suggestions.append(
                OptimizationSuggestion(
                    type="coordination",
                    severity="low",
                    title="Add UCF Metrics Integration",
                    description="Integrate Universal Coordination Framework metrics for awareness-based routing.",
                    nodeIds=[],
                    estimatedImpact="Medium - Enables coordination-aware workflow optimization",
                    implementation="Add UCF metric checks in condition nodes to route based on system coordination state",
                )
            )

        # Check complexity
        if metrics["complexity"] > 70:
            suggestions.append(
                OptimizationSuggestion(
                    type="performance",
                    severity="medium",
                    title="Reduce Workflow Complexity",
                    description="This workflow is highly complex. Consider breaking it into smaller sub-workflows.",
                    nodeIds=[],
                    estimatedImpact="High - Improves maintainability and execution speed",
                    implementation="Split into multiple spirals using webhook triggers between them",
                )
            )

        return suggestions


# Global optimizer instance
_optimizer_instance: WorkflowOptimizer | None = None


def get_optimizer() -> WorkflowOptimizer:
    """Get or create global optimizer instance"""
    global _optimizer_instance
    if _optimizer_instance is None:
        _optimizer_instance = WorkflowOptimizer()
    return _optimizer_instance
