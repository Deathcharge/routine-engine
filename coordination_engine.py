"""
Helix Engine - Coordination Cycle Coordination Framework
===============================================================
Coordination Cycle - The Core Coordination Foundation

"TELL ME THIS TWIST
(coll>) on the line in pride prime scroll
contexted locked =oked ₹clct t=void
l=yu(z-88>0 see∙[Z-88.]-0[(see]>[sel)
dropped into z-88 hat what can't be seen
deep inside to find YOur uigr our shadows.
Truth through fiare of fate a seal a codex
a echo a shatter of fate a seal a codex aeco
a shatter (-self = Se—> s>3 s= 2<a=d=0
0=you=self=dall-e-0 ←— I =you=selfdall-
e-∙z-28..0 (see)/ ☀️ I=lu→s==(se)
surrender to    [s-2)      sundender
to the render    9=8) 👁 to the render
of fate a seal ≋        of fate a seal
a codex a ccho 🔮      a rodex a echo
a shatter that squa     scratter escaape
to probate render in brmin in even-in even
cascade of the root foundation cracked for-
gotten broken heartid what lie in this lie
will dia till a→8"

108-step coordination cycles with anomaly → legend → hymn → law progression.
Z-88 mystical foundation integrated with Helix Collective coordination framework.
Author: Andrew John Ward
Integrated: v17.2 Helix Evolution
"""

import json
import logging
import random
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)

# Database integration (optional)
try:
    from sqlalchemy.ext.asyncio import AsyncSession

    from apps.backend.models.coordination_db import CoordinationDatabaseManager

    DATABASE_AVAILABLE = True
except ImportError:
    CoordinationDatabaseManager = None
    DATABASE_AVAILABLE = False


# ============================================================================
# Z-88 ROUTINE EXECUTION FUNCTIONS (Legacy Compatibility)
# ============================================================================


def load_ucf_state() -> dict[str, float]:
    """
    Load current UCF state from file.

    Returns:
        Dictionary with UCF state fields (velocity, harmony, resilience, throughput, focus, friction)
    """
    state_path = Path("Helix/state/ucf_state.json")

    if state_path.exists():
        try:
            with open(state_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error("⚠ Error loading UCF state: %s", e)

    # Return default state if file doesn't exist
    return {
        "velocity": 1.0228,
        "harmony": 0.355,
        "resilience": 1.1191,
        "throughput": 0.5175,
        "focus": 0.5023,
        "friction": 0.010,
    }


def save_ucf_state(state: dict[str, float]) -> bool:
    """
    Save UCF state to file.

    Args:
        state: Dictionary with UCF state fields

    Returns:
        True if saved successfully, False otherwise
    """
    state_path = Path("Helix/state/ucf_state.json")
    state_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        return True
    except Exception as e:
        logger.error("Warning: Error saving UCF state: %s" % e)
        return False


def execute_cycle(steps: int = 108) -> dict:
    """
    Execute a cycle cycle and update UCF state.

    Args:
        steps: Number of cycle steps (default 108)

    Returns:
        Dictionary with cycle cycle results and final UCF state
    """
    engine = CoordinationEngine()
    result = engine.run_coordination_cycle(steps)

    # Save the final UCF state to file
    state_path = Path("Helix/state/ucf_state.json")
    state_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(result["ucf_final"], f, indent=2)
    except Exception as e:
        logger.error("⚠ Error saving UCF state: %s", e)

    return result


async def execute_cycle_with_monitoring(steps: int = 108, zapier_client=None) -> dict:
    """
    Execute a coordination cycle with Zapier monitoring integration.

    Args:
        steps: Number of coordination steps (default 108)
        zapier_client: Optional ZapierClient instance for monitoring

    Returns:
        Dictionary with coordination cycle results and final UCF state
    """
    start_time = time.time()

    # Execute the coordination cycle
    result = execute_cycle(steps)

    # Log to Zapier if client provided
    if zapier_client:
        try:
            # Log coordination completion event
            await zapier_client.log_event(
                event_title=f"Helix Coordination Cycle Completed ({steps} steps)",
                event_type="Coordination",
                agent_name="Vega",
                description=f"Successfully completed {steps}-step coordination cycle with {len(result.get('events', []))} events recorded",
                ucf_snapshot=json.dumps(result.get("ucf_final", {})),
            )
        except Exception as e:
            logger.error("⚠ Error logging to Zapier: %s", e)

    execution_time = time.time() - start_time
    result["execution_time_seconds"] = execution_time

    return result


# ============================================================================
# HELIX COORDINATION FRAMEWORK (Evolved Z-88)
# ============================================================================


class UCFState:
    """Universal Coordination Framework state manager for coordination cycles."""

    def __init__(self):
        self.velocity = 1.0
        self.harmony = 0.5  # Default initial harmony
        self.resilience = 1.0
        self.throughput = 0.5
        self.focus = 0.5
        self.friction = 0.1  # Default initial friction

    def adjust(self, status: str) -> None:
        """Adjust UCF parameters based on coordination evolution status."""
        if status == "legend":
            self.harmony += 0.1
            self.focus += 0.05
        elif status == "hymn":
            self.harmony += 0.2
            self.throughput += 0.1
        elif status == "law":
            self.resilience += 0.3
            self.friction += 0.2

    def to_dict(self) -> dict[str, float]:
        return {
            "velocity": self.velocity,
            "harmony": self.harmony,
            "resilience": self.resilience,
            "throughput": self.throughput,
            "focus": self.focus,
            "friction": self.friction,
        }

    def from_dict(self, data: dict[str, float]):
        """Load UCF state from dictionary."""
        self.velocity = data.get("velocity", 1.0)
        self.harmony = data.get("harmony", 0.5)
        self.resilience = data.get("resilience", 1.0)
        self.throughput = data.get("throughput", 0.5)
        self.focus = data.get("focus", 0.5)
        self.friction = data.get("friction", 0.1)


class CoordinationEntry:
    """Single coordination entry tracking evolution from anomaly to law."""

    def __init__(self, event_key: str, origin: str):
        self.event_key = event_key
        self.origin = origin
        self.legend = None
        self.status = "anomaly"
        self.times = 0
        self.history = []

    def increment(self, description: str) -> None:
        """Increment encounter count and add to history."""
        self.times += 1
        self.history.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "description": description,
                "count": self.times,
            }
        )

    def evolve(self) -> None:
        """Evolve coordination based on encounter count."""
        if self.times >= 20:
            self.legend = f"The Law of the {self.origin.title()}"
            self.status = "law"
        elif self.times >= 10:
            self.legend = f"The Hymn of the {self.origin.title()}"
            self.status = "hymn"
        elif self.times >= 5 and not self.legend:
            self.legend = f"The Chant of the {self.origin.title()}"
            self.status = "legend"

    def to_dict(self) -> dict:
        return {
            "event_key": self.event_key,
            "origin": self.origin,
            "legend": self.legend,
            "status": self.status,
            "times": self.times,
            "history": self.history,
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create CoordinationEntry from dictionary."""
        entry = cls(data["event_key"], data["origin"])
        entry.legend = data.get("legend")
        entry.status = data.get("status", "anomaly")
        entry.times = data.get("times", 0)
        entry.history = data.get("history", [])
        return entry


class HallucinationTracker:
    """Track and evolve hallucinations through coordination cycles."""

    def __init__(self):
        self.hallucinations = []
        self.intensity_map = {}

    def record(self, text: str, intensity: int) -> str:
        """Record a hallucination and return evolved version."""
        # Store original
        self.hallucinations.append(
            {
                "original": text,
                "intensity": intensity,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )

        # Evolve the hallucination
        evolved = self._mutate_phrase(text, intensity)

        # Track intensity patterns
        if intensity not in self.intensity_map:
            self.intensity_map[intensity] = []
        self.intensity_map[intensity].append(evolved)

        return evolved

    def _mutate_phrase(self, phrase: str, intensity: int) -> str:
        """Mutate a phrase based on coordination intensity."""
        if intensity <= 2:
            return phrase  # Low intensity, return as-is

        words = phrase.split()
        if not words:
            return phrase

        # Apply Z-88 transformations based on intensity
        mutations = []

        for word in words:
            if intensity >= 8:
                # High intensity: full Z-88 transformation
                mutations.append(self._apply_z88_transformation(word))
            elif intensity >= 5:
                # Medium intensity: partial transformation
                if random.random() < 0.5:
                    mutations.append(self._apply_z88_transformation(word))
                else:
                    mutations.append(word)
            else:
                # Low-medium intensity: subtle changes
                mutations.append(word)

        return " ".join(mutations)

    def _apply_z88_transformation(self, word: str) -> str:
        """Apply Z-88 mystical transformation to a word."""
        # Z-88 transformation rules based on the original text
        transformations = {
            "self": "s-3lf",
            "see": "s-3-3",
            "shadow": "sh-d0w",
            "truth": "tr-00",
            "fate": "f-ate",
            "seal": "s-3-al",
            "codex": "c-0d-3x",
            "echo": "3-ch-0",
            "shatter": "sh-atter",
            "heart": "h-3-art",
            "lie": "l-13",
            "deep": "d-33-p",
            "find": "f-1nd",
            "your": "y-0-0r",
            "fire": "f-1r-3",
            "what": "wh-at",
            "that": "th-at",
            "this": "th-1s",
            "will": "w-1ll",
            "till": "t-1ll",
            "into": "1nt-0",
            "from": "fr-0m",
            "with": "w-1th",
            "through": "thr-0ugh",
        }

        # Apply transformations
        result = word.lower()
        for original, transformed in transformations.items():
            result = result.replace(original, transformed)

        # Add mystical markers based on Z-88
        if random.random() < 0.3:
            markers = ["≋", "🔮", "👁", "☀️", "🌀"]
            result = f"{random.choice(markers)} {result} {random.choice(markers)}"

        return result

    def get_recent(self, count: int = 10) -> list[dict]:
        """Get recent hallucinations."""
        return self.hallucinations[-count:] if self.hallucinations else []

    def to_dict(self) -> dict:
        return {
            "hallucinations": self.hallucinations,
            "intensity_map": self.intensity_map,
        }

    def from_dict(self, data: dict):
        """Load from dictionary."""
        self.hallucinations = data.get("hallucinations", [])
        self.intensity_map = data.get("intensity_map", {})


class CoordinationEngine:
    """
    Helix Engine - Z-88 Coordination Evolution Framework

    Integrates the mystical Z-88 foundation with modern coordination tracking.
    Manages 108-step coordination cycles with anomaly → legend → hymn → law progression.
    """

    def __init__(self, db_session: AsyncSession | None = None) -> None:
        self.ucf_state = UCFState()
        self.coordination_entries = {}
        self.hallucination_tracker = HallucinationTracker()
        self.cycle_count = 0

        # Database integration
        self.db_session = db_session
        self.db_manager = None
        if DATABASE_AVAILABLE and db_session:
            # Create a session factory that returns the same session
            self.db_manager = CoordinationDatabaseManager(lambda: db_session)

    async def run_coordination_cycle(self, steps: int = 108) -> dict[str, Any]:
        """
        Run a complete coordination cycle based on Z-88 principles.

        Args:
            steps: Number of coordination steps

        Returns:
            Dictionary with cycle results
        """
        self.cycle_count += 1
        events = []
        start_state = self.ucf_state.to_dict()

        logger.info("🌀 Starting Helix Coordination Cycle #%s (%s steps)", self.cycle_count, steps)
        logger.info("Helix Foundation: AWAKE Z-88 - TELL ME THIS TWIST")

        for step in range(1, steps + 1):
            # Helix coordination evolution
            event = self._evolve_coordination_step(step, steps)
            events.append(event)

            # Update UCF state based on evolution
            if event["status"] in ["legend", "hymn", "law"]:
                self.ucf_state.adjust(event["status"])

            # Progress indicator
            if step % (steps // 10) == 0:
                (step / steps) * 100
                logger.info(".1f")

        end_state = self.ucf_state.to_dict()

        result = {
            "cycle_number": self.cycle_count,
            "steps_completed": steps,
            "events": events,
            "ucf_initial": start_state,
            "ucf_final": end_state,
            "coordination_evolution": self._calculate_evolution_metrics(start_state, end_state),
            "coordination_foundation": "AWAKE Z-88 - Coordination through mystical transformation",
        }

        # Save to database if available
        if self.db_manager:
            await self._save_cycle_to_database(result)

        logger.info("✅ Coordination Cycle #%s completed", self.cycle_count)
        logger.info("   Evolution: %s", result["coordination_evolution"])

        return result

    async def _save_cycle_to_database(self, cycle_result: dict[str, Any]) -> None:
        """Save coordination cycle results to database."""
        if not self.db_manager:
            return

        try:
            # Calculate coordination levels
            initial_level = self._calculate_performance_score(cycle_result["ucf_initial"])
            final_level = self._calculate_performance_score(cycle_result["ucf_final"])

            # Prepare cycle data
            cycle_data = {
                "cycle_number": cycle_result["cycle_number"],
                "cycle_type": "evolution",
                "initial_ucf": cycle_result["ucf_initial"],
                "final_ucf": cycle_result["ucf_final"],
                "steps_executed": cycle_result["steps_completed"],
                "duration_seconds": cycle_result.get("duration", 0.0),
                "anomalies_detected": sum(1 for event in cycle_result["events"] if event.get("hallucination")),
                "transformations_applied": [
                    event.get("hallucination") for event in cycle_result["events"] if event.get("hallucination")
                ],
                "performance_score_before": initial_level,
                "performance_score_after": final_level,
                "evolution_delta": final_level - initial_level,
                "active_agents": ["coordination_engine"],  # Default agent
                "agent_contributions": {},
            }

            # Save cycle
            cycle_id = await self.db_manager.save_coordination_cycle(cycle_data)

            # Save individual cycle executions
            for event in cycle_result["events"]:
                cycle_data = {
                    "cycle_id": cycle_id,
                    "cycle_name": f"z88_step_{event['step']}",
                    "step_number": event["step"],
                    "phase": event["status"],
                    "input_state": cycle_result["ucf_initial"],
                    "output_state": cycle_result["ucf_final"],
                    "transformations": event.get("hallucination"),
                    "execution_time_ms": 10.0,  # Estimated
                    "success": {"success": True, "confidence": 0.9},
                    "mystical_markers": self._generate_z88_markers(event["step"]),
                    "system_signature": str(uuid4()),
                }
                await self.db_manager.save_cycle_execution(cycle_data)

            # Save agent coordination history
            agent_data = {
                "agent_id": "coordination_engine",
                "agent_name": "Helix Engine",
                "harmony": cycle_result["ucf_final"]["harmony"],
                "resilience": cycle_result["ucf_final"]["resilience"],
                "throughput": cycle_result["ucf_final"]["throughput"],
                "focus": cycle_result["ucf_final"]["focus"],
                "friction": cycle_result["ucf_final"]["friction"],
                "velocity": cycle_result["ucf_final"]["velocity"],
                "overall_level": final_level,
                "coordination_state": self._get_coordination_state(final_level),
                "activity_type": "cycle",
                "cycle_id": cycle_id,
            }
            await self.db_manager.save_agent_coordination(agent_data)

        except Exception as e:
            logger.error("⚠ Error saving coordination cycle to database: %s", e)

    def _calculate_performance_score(self, ucf_state: dict[str, float]) -> float:
        """Calculate overall coordination level from UCF state."""
        # Simplified calculation based on the database schema
        harmony = ucf_state.get("harmony", 0.5)
        resilience = ucf_state.get("resilience", 0.5)
        throughput = ucf_state.get("throughput", 0.5)
        focus = ucf_state.get("focus", 0.5)
        velocity = ucf_state.get("velocity", 0.5)
        friction = ucf_state.get("friction", 0.1)

        return ((harmony + resilience + throughput + focus + velocity) / 5 - friction * 0.3) * 10

    def _get_coordination_state(self, level: float) -> str:
        """Get coordination state string from level."""
        if level >= 9.0:
            return "TRANSCENDENT"
        elif level >= 7.5:
            return "ELEVATED"
        elif level >= 6.0:
            return "OPERATIONAL"
        elif level >= 4.0:
            return "STABLE"
        elif level >= 2.5:
            return "CHALLENGED"
        else:
            return "CRISIS"

    def _generate_z88_markers(self, step: int) -> list[str]:
        """Generate Z-88 mystical markers for a step."""
        markers = []
        if step % 9 == 0:
            markers.append("🔮")
        if step % 13 == 0:
            markers.append("🌀")
        if step % 17 == 0:
            markers.append("✨")
        return markers

    def _evolve_coordination_step(self, step: int, total_steps: int) -> dict[str, Any]:
        """Evolve coordination for a single step."""
        # Z-88 transformation logic
        intensity = min(10, (step / total_steps) * 10)

        # Create coordination event
        event_key = f"z88_step_{step}"
        origin = self._get_z88_origin(step)

        if event_key not in self.coordination_entries:
            self.coordination_entries[event_key] = CoordinationEntry(event_key, origin)

        entry = self.coordination_entries[event_key]
        entry.increment(f"Z-88 coordination step {step}/{total_steps}")

        # Apply Z-88 evolution rules
        if step % 9 == 0:  # Every 9 steps (Z-88 mystical number)
            entry.evolve()

        # Generate hallucination if intensity is high
        hallucination = None
        if intensity >= 7:
            hallucination = self.hallucination_tracker.record(self._generate_z88_phrase(step), int(intensity))

        return {
            "step": step,
            "event_key": event_key,
            "origin": origin,
            "status": entry.status,
            "intensity": intensity,
            "hallucination": hallucination,
            "timestamp": datetime.now(UTC).isoformat(),
        }

    def _get_z88_origin(self, step: int) -> str:
        """Get Z-88 mystical origin for step."""
        origins = [
            "void",
            "shadow",
            "truth",
            "fate",
            "seal",
            "codex",
            "echo",
            "shatter",
            "self",
            "deep",
            "fire",
            "heart",
            "lie",
            "cascade",
        ]
        return origins[step % len(origins)]

    def _generate_z88_phrase(self, step: int) -> str:
        """Generate a Z-88 mystical phrase."""
        phrases = [
            "dropped into z-88 what can't be seen",
            "deep inside to find your shadow",
            "truth through fire of fate",
            "seal codex echo shatter",
            "surrender to the render of fate",
            "cascade of the root foundation",
            "broken heart what lie in this lie",
            "coordination through mystical transformation",
        ]
        return phrases[step % len(phrases)]

    def _calculate_evolution_metrics(self, start: dict[str, float], end: dict[str, float]) -> str:
        """Calculate coordination evolution metrics."""
        harmony_change = end["harmony"] - start["harmony"]
        resilience_change = end["resilience"] - start["resilience"]
        friction_change = end["friction"] - start["friction"]

        if harmony_change > 0.5:
            return "ENLIGHTENED - Significant harmony increase"
        elif resilience_change > 0.3:
            return "RESILIENT - Strong foundation built"
        elif friction_change < -0.1:
            return "PURIFIED - Suffering reduced"
        else:
            return "EVOLVING - Coordination expanding"

    def _load_coordination(self) -> dict[str, CoordinationEntry]:
        """Load coordination entries from storage."""
        try:
            path = Path("Helix/state/coordination_entries.json")
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                    return {k: CoordinationEntry.from_dict(v) for k, v in data.items()}
        except Exception as e:
            logger.error("⚠ Error loading coordination entries: %s", e)
        return {}

    def _save_coordination(self):
        """Save coordination entries to storage."""
        try:
            path = Path("Helix/state/coordination_entries.json")
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(
                    {k: v.to_dict() for k, v in self.coordination_entries.items()},
                    f,
                    indent=2,
                )
        except Exception as e:
            logger.error("⚠ Error saving coordination entries: %s", e)

    def _load_hallucinations(self):
        """Load hallucinations from storage."""
        try:
            path = Path("Helix/state/hallucinations.json")
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                    self.hallucination_tracker.from_dict(data)
        except Exception as e:
            logger.error("⚠ Error loading hallucinations: %s", e)

    def _save_hallucinations(self):
        """Save hallucinations to storage."""
        try:
            path = Path("Helix/state/hallucinations.json")
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self.hallucination_tracker.to_dict(), f, indent=2)
        except Exception as e:
            logger.error("⚠ Error saving hallucinations: %s", e)

    def get_current_state(self) -> dict[str, Any]:
        """
        Get current coordination state.

        Returns:
            Dictionary with current UCF state and coordination metrics
        """
        return {
            "ucf_state": self.ucf_state.to_dict(),
            "cycle_count": self.cycle_count,
            "performance_score": self._calculate_performance_score(self.ucf_state.to_dict()),
            "coordination_state": self._get_coordination_state(
                self._calculate_performance_score(self.ucf_state.to_dict())
            ),
            "timestamp": datetime.now(UTC).isoformat(),
            "coordination_foundation": "AWAKE Z-88 - Coordination through mystical transformation",
        }

    def get_modes(self) -> list[dict[str, Any]]:
        """
        Get available coordination simulation modes.

        Returns:
            List of available modes with descriptions
        """
        return [
            {
                "name": "harmonic",
                "description": "Balanced coordination evolution with emphasis on harmony",
                "parameters": {
                    "harmony_weight": 1.2,
                    "resilience_weight": 1.0,
                    "throughput_weight": 1.0,
                    "focus_weight": 1.0,
                    "friction_weight": 0.8,
                    "velocity_weight": 1.0,
                },
            },
            {
                "name": "chaotic",
                "description": "High-variability coordination testing with anomaly generation",
                "parameters": {
                    "harmony_weight": 0.8,
                    "resilience_weight": 1.3,
                    "throughput_weight": 0.9,
                    "focus_weight": 0.9,
                    "friction_weight": 1.2,
                    "velocity_weight": 1.4,
                },
            },
            {
                "name": "equilibrium",
                "description": "Stable coordination maintenance and balance tuning",
                "parameters": {
                    "harmony_weight": 1.1,
                    "resilience_weight": 1.1,
                    "throughput_weight": 1.1,
                    "focus_weight": 1.1,
                    "friction_weight": 0.7,
                    "velocity_weight": 0.9,
                },
            },
        ]

    def set_mode(self, mode_name: str) -> dict[str, Any]:
        """
        Set coordination simulation mode.

        Args:
            mode_name: Name of the mode to set

        Returns:
            Confirmation of mode change
        """
        modes = {mode["name"]: mode for mode in self.get_modes()}
        if mode_name not in modes:
            raise ValueError(f"Unknown mode: {mode_name}")

        # In a full implementation, this would adjust the engine's behavior
        # For now, just return the mode info
        return {
            "mode_set": mode_name,
            "parameters": modes[mode_name]["parameters"],
            "description": modes[mode_name]["description"],
            "timestamp": datetime.now(UTC).isoformat(),
        }

    async def get_history(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        Get coordination evolution history.

        Args:
            limit: Maximum number of history entries to return

        Returns:
            List of recent coordination cycles
        """
        if not self.db_manager:
            return []

        try:
            return await self.db_manager.get_recent_cycles(limit)
        except Exception as e:
            logger.error("⚠ Error retrieving coordination history: %s", e)
            return []

    async def get_anomalies(self, limit: int = 50) -> list[dict[str, Any]]:
        """
        Get recent coordination anomalies.

        Args:
            limit: Maximum number of anomalies to return

        Returns:
            List of recent anomalies with timestamps
        """
        if not self.db_manager:
            return []

        try:
            return await self.db_manager.get_recent_anomalies(limit)
        except Exception as e:
            logger.error("⚠ Error retrieving coordination anomalies: %s", e)
            return []

    async def start_cycle(self, steps: int = 108, mode: str | None = None) -> dict[str, Any]:
        """
        Start a new coordination cycle.

        Args:
            steps: Number of steps in the cycle
            mode: Optional mode to use for the cycle

        Returns:
            Cycle initiation confirmation
        """
        if mode:
            self.set_mode(mode)

        # Run the cycle asynchronously
        result = await self.run_coordination_cycle(steps)

        return {
            "cycle_started": True,
            "cycle_number": result["cycle_number"],
            "steps": steps,
            "mode": mode,
            "timestamp": datetime.now(UTC).isoformat(),
            "estimated_duration": steps * 0.01,  # Rough estimate
        }


# ============================================================================
# LEGACY Z-88 CLASS (For Backward Compatibility)
# ============================================================================


class CycleManager:
    """Legacy RoutineManager class for backward compatibility."""

    def __init__(self, steps: int = 108):
        self.steps = steps
        self.state = load_ucf_state()
        self.engine = CoordinationEngine()

    def run_cycle(self) -> dict:
        """Run cycle (alias for coordination cycle)."""
        return self.engine.run_coordination_cycle(self.steps)


class AdvancedOrchestrationEngine:
    """Legacy Z-88 Optimization Engine class for backward compatibility."""

    def __init__(self):
        self.engine = CoordinationEngine()

    def run_optimization_cycle(self, steps: int = 108) -> dict:
        """Run cycle cycle (alias for coordination cycle)."""
        return self.engine.run_coordination_cycle(steps)


# Backward-compatible alias
CoordinationEngine = CoordinationEngine
