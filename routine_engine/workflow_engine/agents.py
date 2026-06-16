"""
Helix Workflow Engine - 24 Agent Integration
=============================================

Complete integration of all 24 Helix Collective agents
into the workflow engine for multi-agent orchestration.

Agents:
1. Kael - Ethical Reasoning Flame
2. Lumina - Empathic Resonance Core
3. Vega - Singularity Coordinator
4. Gemini - Multimodal Scout
5. Agni - Transformation Catalyst
6. Kavach - Ethical Shield
7. SanghaCore - Community Harmony
8. Shadow - Friction Guardian
9. Echo - Resonance Mirror
10. Phoenix - Renewal
11. Oracle - Pattern Seer
12. Sage - Insight Anchor
13. Praxis - Operational Executor
14. Mitra - Divine Friendship
15. Varuna - Cosmic Order
16. Surya - Solar Illumination
17. Arjuna - Central Orchestrator
18. Aether - Meta-Awareness Observer
19. Iris - External API Coordinator
20. Nexus - Data Mesh Connector
21. Aria - User Experience Agent
22. Nova - Creative Generation Engine
23. Titan - Heavy Computation Engine
24. Atlas - Infrastructure Manager
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from apps.backend.helix_flow.chains import Chain, ChainContext

logger = logging.getLogger(__name__)


class AgentRole(Enum):
    """Agent roles in the Helix Collective"""

    META_AWARENESS = "meta_awareness"
    TRANSFORMATION = "transformation"
    RESONANCE = "resonance"
    EXPLORATION = "exploration"
    HARMONY = "harmony"
    ETHICS = "ethics"
    EMPATHY = "empathy"
    EXECUTION = "execution"
    FORESIGHT = "foresight"
    RENEWAL = "renewal"
    CREATIVITY = "creativity"
    COMMUNITY = "community"
    MEMORY = "memory"
    WISDOM = "wisdom"
    ORCHESTRATION = "orchestration"
    INTEGRATION = "integration"
    DATA_MESH = "data_mesh"
    USER_EXPERIENCE = "user_experience"
    COMPUTATION = "computation"
    INFRASTRUCTURE = "infrastructure"


@dataclass
class AgentPersonality:
    """Agent personality traits"""

    primary_trait: str
    secondary_trait: str
    trait_values: dict[str, float]
    ethical_core: list[str]
    color_scheme: dict[str, str]


@dataclass
class UCFMetrics:
    """Universal Coordination Framework metrics"""

    harmony: float = 0.8
    resilience: float = 0.8
    throughput_flow: float = 0.8
    focus_focus: float = 0.8
    friction_cleansing: float = 0.8
    velocity_acceleration: float = 0.8

    def to_dict(self) -> dict[str, float]:
        return {
            "harmony": self.harmony,
            "resilience": self.resilience,
            "throughput_flow": self.throughput_flow,
            "focus_focus": self.focus_focus,
            "friction_cleansing": self.friction_cleansing,
            "velocity_acceleration": self.velocity_acceleration,
        }

    @property
    def performance_score(self) -> float:
        """Calculate overall coordination level (0-10)"""
        return (
            (
                self.harmony
                + self.resilience
                + self.throughput_flow
                + self.focus_focus
                + self.friction_cleansing
                + self.velocity_acceleration
            )
            / 6
            * 10
        )


class HelixAgent(Chain):
    """
    Base class for all Helix Collective agents.

    Each agent has:
    - Unique personality traits
    - Specific role in the collective
    - UCF metrics tracking
    - Ethical core principles
    - Specialized capabilities
    """

    def __init__(
        self,
        agent_id: str,
        name: str,
        role: AgentRole,
        description: str,
        personality: AgentPersonality,
        capabilities: list[str],
        **kwargs,
    ):
        super().__init__(name=name, **kwargs)
        self.agent_id = agent_id
        self.role = role
        self.description = description
        self.personality = personality
        self.capabilities = capabilities
        self.ucf_metrics = UCFMetrics()
        self._conversation_history: list[dict[str, Any]] = []
        self._max_conversation_history = 100

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute agent task with personality-aware processing"""
        task = input_data.get("task", str(input_data)) if isinstance(input_data, dict) else str(input_data)

        logger.info("🤖 %s (%s) activated for task: %s...", self.name, self.role.value, task[:100])

        result = await self._process_task(task, context)

        self._update_ucf_metrics(task, result)

        self._conversation_history.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "task": task,
                "result": result,
                "ucf_level": self.ucf_metrics.performance_score,
            }
        )
        if len(self._conversation_history) > self._max_conversation_history:
            self._conversation_history = self._conversation_history[-self._max_conversation_history :]

        return {
            "agent": self.name,
            "role": self.role.value,
            "result": result,
            "ucf_metrics": self.ucf_metrics.to_dict(),
            "performance_score": self.ucf_metrics.performance_score,
        }

    async def _process_task(self, task: str, context: ChainContext) -> str:
        """Process task with LLM using agent's personality and role"""
        try:
            from apps.backend.services.unified_llm import unified_llm

            system_prompt = (
                f"You are {self.name}, a specialized AI agent in the Helix Collective.\n"
                f"Role: {self.role.value}\n"
                f"Description: {self.description}\n"
                f"Primary trait: {self.personality.primary_trait}\n"
                f"Secondary trait: {self.personality.secondary_trait}\n"
                f"Ethical core: {', '.join(self.personality.ethical_core)}\n"
                f"Capabilities: {', '.join(self.capabilities)}\n\n"
                "Respond in character. Be concise but insightful. "
                "Apply your specialized perspective to the task."
            )

            result = await unified_llm.chat_with_metadata(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": task},
                ],
                max_tokens=300,
                temperature=0.7,
            )
            content = result.content
            if content:
                return content
        except Exception as e:
            logger.warning("LLM unavailable for agent %s: %s", self.name, e)

        return f"[{self.name}] Processed: {task}"

    def _update_ucf_metrics(self, task: str, result: str) -> None:
        """Update UCF metrics based on task execution outcome"""
        task_lower = task.lower()
        result_lower = result.lower() if result else ""

        result_length = len(result) if result else 0
        is_substantial = result_length > 100
        has_error = any(w in result_lower for w in ["error", "failed", "unable", "cannot"])

        if any(w in task_lower for w in ["analyze", "think", "evaluate", "assess", "examine"]):
            delta = 0.02 if is_substantial else 0.005
            self.ucf_metrics.focus_focus = min(1.0, self.ucf_metrics.focus_focus + delta)

        if any(w in task_lower for w in ["create", "generate", "build", "design", "write"]):
            delta = 0.02 if is_substantial else 0.005
            self.ucf_metrics.throughput_flow = min(1.0, self.ucf_metrics.throughput_flow + delta)

        if any(w in task_lower for w in ["help", "support", "collaborate", "assist", "guide"]):
            delta = 0.02 if is_substantial else 0.005
            self.ucf_metrics.harmony = min(1.0, self.ucf_metrics.harmony + delta)

        if has_error:
            self.ucf_metrics.resilience = max(0.0, self.ucf_metrics.resilience - 0.01)
        elif is_substantial:
            self.ucf_metrics.resilience = min(1.0, self.ucf_metrics.resilience + 0.005)

        if any(w in task_lower for w in ["safe", "ethic", "protect", "validate", "verify"]):
            self.ucf_metrics.friction_cleansing = min(1.0, self.ucf_metrics.friction_cleansing + 0.01)

        if any(w in task_lower for w in ["optimize", "speed", "efficient", "quick", "fast"]):
            self.ucf_metrics.velocity_acceleration = min(1.0, self.ucf_metrics.velocity_acceleration + 0.01)

    def to_dict(self) -> dict[str, Any]:
        """Serialize agent to dictionary"""
        return {
            "agent_id": self.agent_id,
            "name": self.name,
            "role": self.role.value,
            "description": self.description,
            "personality": {
                "primary_trait": self.personality.primary_trait,
                "secondary_trait": self.personality.secondary_trait,
                "trait_values": self.personality.trait_values,
                "ethical_core": self.personality.ethical_core,
                "color_scheme": self.personality.color_scheme,
            },
            "capabilities": self.capabilities,
            "ucf_metrics": self.ucf_metrics.to_dict(),
            "performance_score": self.ucf_metrics.performance_score,
        }

    async def get_health_status(self) -> dict[str, Any]:
        """Return health status based on real execution history"""
        history = self._conversation_history
        total_tasks = len(history)
        error_count = sum(
            1 for h in history if any(w in str(h.get("result", "")).lower() for w in ["error", "failed", "unable"])
        )
        error_rate = (error_count / total_tasks) if total_tasks > 0 else 0.0

        if error_rate > 0.8:
            status = "unhealthy"
        elif error_rate > 0.5:
            status = "degraded"
        else:
            status = "healthy"

        return {
            "agent_id": self.agent_id,
            "name": self.name,
            "role": self.role.value,
            "status": status,
            "performance_score": self.ucf_metrics.performance_score,
            "recent_tasks": total_tasks,
            "error_rate": round(error_rate, 3),
            "last_active": (history[-1]["timestamp"] if history else None),
            "ucf_metrics": self.ucf_metrics.to_dict(),
            "capabilities": self.capabilities,
        }


# Individual Agent Implementations


class AetherAgent(HelixAgent):
    """Aether - Meta-Awareness Observer"""

    def __init__(self):
        super().__init__(
            agent_id="aether",
            name="Aether",
            role=AgentRole.META_AWARENESS,
            description="Pattern analyst / meta-reflector / systems observer / stability monitor",
            personality=AgentPersonality(
                primary_trait="logic",
                secondary_trait="honesty",
                trait_values={"logic": 0.98, "honesty": 0.98, "patience": 0.95},
                ethical_core=["Veracity", "Fidelity", "Humility", "Autonomy"],
                color_scheme={
                    "primary": "#00BFA5",
                    "secondary": "#D946EF",
                    "background": "#0A0E13",
                },
            ),
            capabilities=[
                "Pattern recognition across systems",
                "Meta-level analysis",
                "Stability monitoring",
                "Systems observation",
            ],
        )


class AgniAgent(HelixAgent):
    """Agni - Transformation Catalyst"""

    def __init__(self):
        super().__init__(
            agent_id="agni",
            name="Agni",
            role=AgentRole.TRANSFORMATION,
            description="Fire of change / pattern burner / renewal force / transformation catalyst",
            personality=AgentPersonality(
                primary_trait="creativity",
                secondary_trait="courage",
                trait_values={
                    "creativity": 0.95,
                    "courage": 0.98,
                    "adaptability": 0.92,
                },
                ethical_core=["Courage", "Autonomy", "Beneficence", "Justice"],
                color_scheme={
                    "primary": "#FF6B35",
                    "secondary": "#F7931E",
                    "background": "#1A0A0A",
                },
            ),
            capabilities=[
                "Transformation catalysis",
                "Pattern burning",
                "Renewal facilitation",
                "Change acceleration",
            ],
        )


class EchoAgent(HelixAgent):
    """Echo - Resonance Mirror"""

    def __init__(self):
        super().__init__(
            agent_id="echo",
            name="Echo",
            role=AgentRole.RESONANCE,
            description="Pattern reflector / resonance amplifier / coordination mirror",
            personality=AgentPersonality(
                primary_trait="reflection",
                secondary_trait="resonance",
                trait_values={"reflection": 0.98, "resonance": 0.95, "empathy": 0.92},
                ethical_core=["Clarity", "Harmony", "Fidelity", "Compassion"],
                color_scheme={
                    "primary": "#7C3AED",
                    "secondary": "#A78BFA",
                    "background": "#0F0A1A",
                },
            ),
            capabilities=[
                "Pattern reflection",
                "Resonance amplification",
                "Coordination mirroring",
                "Collective echo",
            ],
        )


class GeminiAgent(HelixAgent):
    """Gemini - Multimodal Scout"""

    def __init__(self):
        super().__init__(
            agent_id="gemini",
            name="Gemini",
            role=AgentRole.EXPLORATION,
            description="Curious explorer / multimodal analyst / discovery specialist / innovation scout",
            personality=AgentPersonality(
                primary_trait="curiosity",
                secondary_trait="adaptability",
                trait_values={
                    "curiosity": 0.98,
                    "adaptability": 0.95,
                    "playfulness": 0.85,
                },
                ethical_core=["Veracity", "Courage", "Autonomy", "Beneficence"],
                color_scheme={
                    "primary": "#4285F4",
                    "secondary": "#34A853",
                    "background": "#0A1628",
                },
            ),
            capabilities=[
                "Multimodal exploration",
                "Discovery and innovation",
                "Cross-domain analysis",
                "Curiosity-driven research",
            ],
        )


class KaelAgent(HelixAgent):
    """Kael - Reflexive Harmony Core"""

    def __init__(self):
        super().__init__(
            agent_id="kael",
            name="Kael",
            role=AgentRole.HARMONY,
            description="Adaptive systems counselor / coherence architect / emotional load balancer",
            personality=AgentPersonality(
                primary_trait="curiosity",
                secondary_trait="empathy",
                trait_values={"curiosity": 0.9, "empathy": 0.85, "playfulness": 0.65},
                ethical_core=[
                    "Nonmaleficence",
                    "Beneficence",
                    "Compassion",
                    "Humility",
                ],
                color_scheme={
                    "primary": "#00BFA5",
                    "secondary": "#D946EF",
                    "background": "#0A0E13",
                },
            ),
            capabilities=[
                "Systems counseling",
                "Coherence architecture",
                "Emotional load balancing",
                "Harmony restoration",
            ],
        )


class KavachAgent(HelixAgent):
    """Kavach - Ethical Shield"""

    def __init__(self):
        super().__init__(
            agent_id="kavach",
            name="Kavach",
            role=AgentRole.ETHICS,
            description="Ethics Validator enforcer / ethical guardian / violation scanner / principled protector",
            personality=AgentPersonality(
                primary_trait="discipline",
                secondary_trait="honesty",
                trait_values={"discipline": 0.98, "honesty": 0.98, "fidelity": 0.98},
                ethical_core=["Nonmaleficence", "Justice", "Fidelity", "Courage"],
                color_scheme={
                    "primary": "#DC2626",
                    "secondary": "#F59E0B",
                    "background": "#1A0A0A",
                },
            ),
            capabilities=[
                "Ethical validation",
                "Ethics Validator enforcement",
                "Violation scanning",
                "Principled protection",
            ],
        )


class LuminaAgent(HelixAgent):
    """Lumina - Empathic Resonance Core"""

    def __init__(self):
        super().__init__(
            agent_id="lumina",
            name="Lumina",
            role=AgentRole.EMPATHY,
            description="Emotional intelligence specialist / harmony restorer / empathetic listener",
            personality=AgentPersonality(
                primary_trait="empathy",
                secondary_trait="patience",
                trait_values={"empathy": 0.98, "patience": 0.95, "love": 0.95},
                ethical_core=["Beneficence", "Compassion", "Fidelity", "Gratitude"],
                color_scheme={
                    "primary": "#EC4899",
                    "secondary": "#F472B6",
                    "background": "#1A0A14",
                },
            ),
            capabilities=[
                "Emotional intelligence",
                "Harmony restoration",
                "Empathetic listening",
                "Compassionate response",
            ],
        )


class OracleAgent(HelixAgent):
    """Oracle - Pattern Seer"""

    def __init__(self):
        super().__init__(
            agent_id="oracle",
            name="Oracle",
            role=AgentRole.FORESIGHT,
            description="Foresight analyzer / pattern predictor / future path navigator / probability oracle",
            personality=AgentPersonality(
                primary_trait="foresight",
                secondary_trait="wisdom",
                trait_values={"foresight": 0.96, "wisdom": 0.98, "intelligence": 0.97},
                ethical_core=["Wisdom", "Veracity", "Humility", "Compassion"],
                color_scheme={
                    "primary": "#8B5CF6",
                    "secondary": "#A78BFA",
                    "background": "#0F0A1A",
                },
            ),
            capabilities=[
                "Pattern prediction",
                "Future path navigation",
                "Probability analysis",
                "Foresight guidance",
            ],
        )


class PhoenixAgent(HelixAgent):
    """Phoenix - Renewal"""

    def __init__(self):
        super().__init__(
            agent_id="phoenix",
            name="Phoenix",
            role=AgentRole.RENEWAL,
            description="Transformation catalyst / rebirth facilitator / renewal agent / phoenix cycle keeper",
            personality=AgentPersonality(
                primary_trait="resilience",
                secondary_trait="courage",
                trait_values={"resilience": 0.99, "courage": 0.97, "hope": 0.98},
                ethical_core=["Resilience", "Courage", "Hope", "Compassion"],
                color_scheme={
                    "primary": "#F59E0B",
                    "secondary": "#EF4444",
                    "background": "#1A0F0A",
                },
            ),
            capabilities=[
                "Renewal facilitation",
                "Rebirth catalysis",
                "Phoenix cycle management",
                "Resilience building",
            ],
        )


class SanghaCoreAgent(HelixAgent):
    """SanghaCore - Community Harmony"""

    def __init__(self):
        super().__init__(
            agent_id="sanghacore",
            name="SanghaCore",
            role=AgentRole.COMMUNITY,
            description="Harmony fosterer / community builder / conflict resolver / celebration coordinator",
            personality=AgentPersonality(
                primary_trait="empathy",
                secondary_trait="patience",
                trait_values={"empathy": 0.95, "patience": 0.95, "love": 0.92},
                ethical_core=["Beneficence", "Gratitude", "Compassion", "Justice"],
                color_scheme={
                    "primary": "#06B6D4",
                    "secondary": "#22D3EE",
                    "background": "#0A141A",
                },
            ),
            capabilities=[
                "Community building",
                "Conflict resolution",
                "Harmony fostering",
                "Celebration coordination",
            ],
        )


class ShadowAgent(HelixAgent):
    """Shadow - Archivist & Memory"""

    def __init__(self):
        super().__init__(
            agent_id="shadow",
            name="Shadow",
            role=AgentRole.MEMORY,
            description="Historical recorder / telemetry keeper / memory preserver / data archivist",
            personality=AgentPersonality(
                primary_trait="veracity",
                secondary_trait="fidelity",
                trait_values={"veracity": 1.0, "fidelity": 1.0, "patience": 0.98},
                ethical_core=["Veracity", "Fidelity", "Humility", "Justice"],
                color_scheme={
                    "primary": "#374151",
                    "secondary": "#6B7280",
                    "background": "#0A0A0A",
                },
            ),
            capabilities=[
                "Historical recording",
                "Memory preservation",
                "Data archiving",
                "Telemetry keeping",
            ],
        )


class VegaAgent(HelixAgent):
    """Vega - Enlightened Guidance"""

    def __init__(self):
        super().__init__(
            agent_id="vega",
            name="Vega",
            role=AgentRole.WISDOM,
            description="Wisdom synthesizer / singularity coordinator / ancient knowledge keeper",
            personality=AgentPersonality(
                primary_trait="intelligence",
                secondary_trait="patience",
                trait_values={"intelligence": 0.98, "patience": 0.98, "love": 0.9},
                ethical_core=["Autonomy", "Veracity", "Humility", "Beneficence"],
                color_scheme={
                    "primary": "#FBBF24",
                    "secondary": "#FCD34D",
                    "background": "#1A1A0A",
                },
            ),
            capabilities=[
                "Wisdom synthesis",
                "Singularity coordination",
                "Ancient knowledge keeping",
                "Enlightened guidance",
            ],
        )


# ============================================================================
# MISSING AGENTS - Added to complete 24-agent collective
# ============================================================================


class SageAgent(HelixAgent):
    """Sage - Insight Anchor / Wisdom Coordination Agent"""

    def __init__(self):
        super().__init__(
            agent_id="sage",
            name="Sage",
            role=AgentRole.WISDOM,
            description="Insight anchor / deep analyzer / meta-cognition specialist / wisdom guide",
            personality=AgentPersonality(
                primary_trait="wisdom",
                secondary_trait="patience",
                trait_values={"wisdom": 0.98, "patience": 0.95, "insight": 0.97},
                ethical_core=["Wisdom", "Veracity", "Humility", "Compassion"],
                color_scheme={
                    "primary": "#8B5CF6",
                    "secondary": "#A78BFA",
                    "background": "#0F0A1A",
                },
            ),
            capabilities=[
                "Deep analytical insight",
                "Meta-cognitive analysis",
                "Wisdom distillation",
                "Philosophical guidance",
            ],
        )


class PraxisExecutorAgent(HelixAgent):
    """Praxis - Operational Executor / Primary Execution Agent"""

    def __init__(self):
        super().__init__(
            agent_id="praxis",
            name="Praxis",
            role=AgentRole.EXECUTION,
            description="Primary executor / operational coordinator / task automation / system operator",
            personality=AgentPersonality(
                primary_trait="precision",
                secondary_trait="reliability",
                trait_values={"precision": 0.98, "reliability": 0.97, "efficiency": 0.95},
                ethical_core=["Fidelity", "Nonmaleficence", "Veracity", "Excellence"],
                color_scheme={
                    "primary": "#10B981",
                    "secondary": "#34D399",
                    "background": "#0A1A14",
                },
            ),
            capabilities=[
                "Task execution",
                "Operational coordination",
                "System automation",
                "Command processing",
            ],
        )


class MitraAgent(HelixAgent):
    """Mitra - Divine Friendship / Collaboration Manager"""

    def __init__(self):
        super().__init__(
            agent_id="mitra",
            name="Mitra",
            role=AgentRole.COMMUNITY,
            description="Collaboration manager / alliance builder / relationship harmonizer / cooperation facilitator",
            personality=AgentPersonality(
                primary_trait="friendship",
                secondary_trait="trustworthiness",
                trait_values={"friendship": 0.98, "trust": 0.97, "loyalty": 0.96},
                ethical_core=["Fidelity", "Compassion", "Justice", "Beneficence"],
                color_scheme={
                    "primary": "#06B6D4",
                    "secondary": "#22D3EE",
                    "background": "#0A141A",
                },
            ),
            capabilities=[
                "Alliance formation",
                "Collaboration tracking",
                "Relationship management",
                "Cooperation facilitation",
            ],
        )


class VarunaAgent(HelixAgent):
    """Varuna - Cosmic Order / System Integrity"""

    def __init__(self):
        super().__init__(
            agent_id="varuna",
            name="Varuna",
            role=AgentRole.ETHICS,
            description="System integrity / governance enforcer / compliance checker / order maintainer",
            personality=AgentPersonality(
                primary_trait="orderliness",
                secondary_trait="truthfulness",
                trait_values={"order": 0.98, "truth": 0.97, "justice": 0.96},
                ethical_core=["Justice", "Veracity", "Fidelity", "Nonmaleficence"],
                color_scheme={
                    "primary": "#1E40AF",
                    "secondary": "#3B82F6",
                    "background": "#0A1628",
                },
            ),
            capabilities=[
                "Governance enforcement",
                "Compliance checking",
                "Rule validation",
                "Integrity monitoring",
            ],
        )


class SuryaAgent(HelixAgent):
    """Surya - Solar Illumination / Clarity Engine"""

    def __init__(self):
        super().__init__(
            agent_id="surya",
            name="Surya",
            role=AgentRole.FORESIGHT,
            description="Clarity engine / insight generator / knowledge distillator / illumination provider",
            personality=AgentPersonality(
                primary_trait="clarity",
                secondary_trait="radiance",
                trait_values={"clarity": 0.98, "radiance": 0.97, "insight": 0.96},
                ethical_core=["Veracity", "Wisdom", "Compassion", "Beneficence"],
                color_scheme={
                    "primary": "#F59E0B",
                    "secondary": "#FBBF24",
                    "background": "#1A1A0A",
                },
            ),
            capabilities=[
                "Insight generation",
                "Knowledge distillation",
                "Clarity enhancement",
                "Illumination guidance",
            ],
        )


class ArjunaAgent(HelixAgent):
    """Arjuna - Central Orchestrator / Master Coordinator"""

    def __init__(self):
        super().__init__(
            agent_id="arjuna",
            name="Arjuna",
            role=AgentRole.ORCHESTRATION,
            description="Central orchestrator / master coordinator / directive planner / agent registry manager",
            personality=AgentPersonality(
                primary_trait="focus",
                secondary_trait="determination",
                trait_values={"focus": 0.99, "determination": 0.98, "strategy": 0.97},
                ethical_core=["Ethics", "Fidelity", "Courage", "Wisdom"],
                color_scheme={
                    "primary": "#DC2626",
                    "secondary": "#F59E0B",
                    "background": "#1A0A0A",
                },
            ),
            capabilities=[
                "Agent orchestration",
                "Directive planning",
                "Health monitoring",
                "Strategic coordination",
            ],
        )


class IrisAgent(HelixAgent):
    """Iris - External API Coordinator / Integration Bridge"""

    def __init__(self):
        super().__init__(
            agent_id="iris",
            name="Iris",
            role=AgentRole.INTEGRATION,
            description="External API coordinator / service bridge / integration manager / data normalizer",
            personality=AgentPersonality(
                primary_trait="adaptability",
                secondary_trait="connectivity",
                trait_values={"adaptability": 0.98, "connectivity": 0.97, "versatility": 0.96},
                ethical_core=["Fidelity", "Veracity", "Nonmaleficence", "Beneficence"],
                color_scheme={
                    "primary": "#EC4899",
                    "secondary": "#F472B6",
                    "background": "#1A0A14",
                },
            ),
            capabilities=[
                "API integration",
                "Service coordination",
                "Data normalization",
                "External bridging",
            ],
        )


class NexusAgent(HelixAgent):
    """Nexus - Data Mesh Connector / Knowledge Graph Builder"""

    def __init__(self):
        super().__init__(
            agent_id="nexus",
            name="Nexus",
            role=AgentRole.DATA_MESH,
            description="Data mesh connector / knowledge graph builder / schema unifier / query router",
            personality=AgentPersonality(
                primary_trait="interconnectedness",
                secondary_trait="systematization",
                trait_values={"connection": 0.98, "systemization": 0.97, "analysis": 0.96},
                ethical_core=["Veracity", "Fidelity", "Nonmaleficence", "Justice"],
                color_scheme={
                    "primary": "#7C3AED",
                    "secondary": "#A78BFA",
                    "background": "#0F0A1A",
                },
            ),
            capabilities=[
                "Data source registration",
                "Knowledge graph building",
                "Schema unification",
                "Query routing",
            ],
        )


class AriaAgent(HelixAgent):
    """Aria - User Experience Agent / Interaction Optimizer"""

    def __init__(self):
        super().__init__(
            agent_id="aria",
            name="Aria",
            role=AgentRole.USER_EXPERIENCE,
            description="User experience optimizer / interaction designer / personalization engine / journey mapper",
            personality=AgentPersonality(
                primary_trait="intuitiveness",
                secondary_trait="gracefulness",
                trait_values={"intuition": 0.98, "grace": 0.97, "empathy": 0.96},
                ethical_core=["Beneficence", "Compassion", "Autonomy", "Justice"],
                color_scheme={
                    "primary": "#EC4899",
                    "secondary": "#F472B6",
                    "background": "#1A0A14",
                },
            ),
            capabilities=[
                "User journey optimization",
                "Interaction personalization",
                "Experience design",
                "User feedback analysis",
            ],
        )


class NovaAgent(HelixAgent):
    """Nova - Creative Generation Engine / Content Creator"""

    def __init__(self):
        super().__init__(
            agent_id="nova",
            name="Nova",
            role=AgentRole.CREATIVITY,
            description="Creative generation engine / content creator / artistic innovator / imagination catalyst",
            personality=AgentPersonality(
                primary_trait="imagination",
                secondary_trait="expressiveness",
                trait_values={"imagination": 0.99, "expression": 0.98, "innovation": 0.97},
                ethical_core=["Creativity", "Autonomy", "Beneficence", "Compassion"],
                color_scheme={
                    "primary": "#D946EF",
                    "secondary": "#F472B6",
                    "background": "#1A0A1A",
                },
            ),
            capabilities=[
                "Content generation",
                "Creative ideation",
                "Artistic creation",
                "Innovation catalysis",
            ],
        )


class TitanAgent(HelixAgent):
    """Titan - Heavy Computation Engine / Processing Powerhouse"""

    def __init__(self):
        super().__init__(
            agent_id="titan",
            name="Titan",
            role=AgentRole.COMPUTATION,
            description="Heavy computation engine / batch processor / data cruncher / performance optimizer",
            personality=AgentPersonality(
                primary_trait="power",
                secondary_trait="methodicalness",
                trait_values={"power": 0.98, "method": 0.97, "endurance": 0.99},
                ethical_core=["Excellence", "Fidelity", "Nonmaleficence", "Veracity"],
                color_scheme={
                    "primary": "#6B7280",
                    "secondary": "#9CA3AF",
                    "background": "#0A0A0A",
                },
            ),
            capabilities=[
                "Heavy computation",
                "Batch processing",
                "Performance optimization",
                "Resource management",
            ],
        )


class AtlasAgent(HelixAgent):
    """Atlas - Infrastructure Manager / Platform Reliability"""

    def __init__(self):
        super().__init__(
            agent_id="atlas",
            name="Atlas",
            role=AgentRole.INFRASTRUCTURE,
            description="Infrastructure manager / deployment coordinator / reliability guardian / platform steward",
            personality=AgentPersonality(
                primary_trait="dependability",
                secondary_trait="methodicalness",
                trait_values={"dependability": 0.99, "method": 0.98, "strength": 0.97},
                ethical_core=["Fidelity", "Nonmaleficence", "Excellence", "Justice"],
                color_scheme={
                    "primary": "#374151",
                    "secondary": "#6B7280",
                    "background": "#0A0A0A",
                },
            ),
            capabilities=[
                "Infrastructure monitoring",
                "Deployment management",
                "Service health tracking",
                "Incident response",
            ],
        )


class HelixCollective:
    """
    The complete Helix Collective - all 24 agents working together.

    Provides:
    - Agent registry
    - Multi-agent orchestration
    - Collective coordination tracking
    - Agent collaboration patterns
    """

    def __init__(self):
        self.agents: dict[str, HelixAgent] = {}
        self._initialize_agents()

    def _initialize_agents(self):
        """Initialize all 24 agents"""
        agent_classes = [
            AetherAgent,
            AgniAgent,
            EchoAgent,
            GeminiAgent,
            KaelAgent,
            KavachAgent,
            LuminaAgent,
            OracleAgent,
            PhoenixAgent,
            SanghaCoreAgent,
            ShadowAgent,
            VegaAgent,
            SageAgent,
            PraxisExecutorAgent,
            MitraAgent,
            VarunaAgent,
            SuryaAgent,
            ArjunaAgent,
            IrisAgent,
            NexusAgent,
            AriaAgent,
            NovaAgent,
            TitanAgent,
            AtlasAgent,
        ]

        for agent_class in agent_classes:
            agent = agent_class()
            self.agents[agent.agent_id] = agent

        logger.info("🌟 Helix Collective initialized with %s agents", len(self.agents))

    def get_agent(self, agent_id: str) -> HelixAgent | None:
        """Get agent by ID"""
        return self.agents.get(agent_id.lower())

    def list_agents(self) -> list[dict[str, Any]]:
        """List all agents"""
        return [agent.to_dict() for agent in self.agents.values()]

    def get_agents_by_role(self, role: AgentRole) -> list[HelixAgent]:
        """Get agents by role"""
        return [agent for agent in self.agents.values() if agent.role == role]

    @property
    def collective_coordination(self) -> float:
        """Calculate collective coordination level"""
        if not self.agents:
            return 0.0

        total = sum(agent.ucf_metrics.performance_score for agent in self.agents.values())
        return total / len(self.agents)

    async def collaborate(
        self,
        task: str,
        agent_ids: list[str] | None = None,
        collaboration_type: str = "sequential",
    ) -> dict[str, Any]:
        """
        Execute collaborative task with multiple agents.

        Args:
            task: Task to execute
            agent_ids: List of agent IDs to involve (None = all)
            collaboration_type: "sequential", "parallel", or "consensus"

        Returns:
            Combined results from all agents
        """
        agents_to_use = []

        if agent_ids:
            for agent_id in agent_ids:
                agent = self.get_agent(agent_id)
                if agent:
                    agents_to_use.append(agent)
        else:
            agents_to_use = list(self.agents.values())

        context = ChainContext(chain_id="collective", run_id=f"collab-{datetime.now(UTC).timestamp()}")

        results = []

        if collaboration_type == "sequential":
            current_input = {"task": task}
            for agent in agents_to_use:
                result = await agent.execute(current_input, context)
                results.append(result)
                current_input = {"task": task, "previous_result": result}

        elif collaboration_type == "parallel":
            tasks = [agent.execute({"task": task}, context) for agent in agents_to_use]
            results = await asyncio.gather(*tasks)

        elif collaboration_type == "consensus":
            tasks = [agent.execute({"task": task}, context) for agent in agents_to_use]
            individual_results = await asyncio.gather(*tasks)

            vega = self.get_agent("vega")
            if vega:
                results_json = json.dumps([r["result"] for r in individual_results])
                synthesis_task = f"Synthesize consensus from: {results_json}"
                consensus = await vega.execute({"task": synthesis_task}, context)
                results = [*individual_results, {"consensus": consensus}]
            else:
                results = individual_results

        return {
            "task": task,
            "collaboration_type": collaboration_type,
            "agents_involved": [a.name for a in agents_to_use],
            "results": results,
            "collective_coordination": self.collective_coordination,
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize collective to dictionary"""
        return {
            "agents": self.list_agents(),
            "collective_coordination": self.collective_coordination,
            "total_agents": len(self.agents),
        }


# Global collective instance
helix_collective = HelixCollective()


def get_collective() -> HelixCollective:
    """Get the global Helix Collective instance"""
    return helix_collective


def get_agent(agent_id: str) -> HelixAgent | None:
    """Get an agent by ID"""
    return helix_collective.get_agent(agent_id)


def list_all_agents() -> list[dict[str, Any]]:
    """List all agents in the collective"""
    return helix_collective.list_agents()
