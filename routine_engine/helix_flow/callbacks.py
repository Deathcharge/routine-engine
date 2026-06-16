"""
Helix Chains - Callback Handlers
=================================

Callbacks for monitoring and extending chain execution:
- StreamingCallback: Stream tokens to clients
- LoggingCallback: Log execution details
- MetricsCallback: Collect execution metrics
"""

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CallbackEvent:
    """Event emitted by callbacks"""

    type: str
    data: Any
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    metadata: dict[str, Any] = field(default_factory=dict)


class CallbackHandler:
    """
    Base class for callback handlers.

    Callbacks are invoked at various points during chain execution
    to enable monitoring, logging, streaming, and custom behavior.
    """

    def __init__(self, name: str = "callback"):
        self.name = name
        self._events: list[CallbackEvent] = []

    async def on_chain_start(self, chain: Any, inputs: Any) -> None:
        """Called when a chain starts execution"""

    async def on_chain_end(self, chain: Any, outputs: Any) -> None:
        """Called when a chain completes execution"""

    async def on_chain_error(self, chain: Any, error: Exception) -> None:
        """Called when a chain encounters an error"""

    async def on_step_start(self, step: Any, inputs: Any, context: Any) -> None:
        """Called when a step starts execution"""

    async def on_step_end(self, step: Any, outputs: Any, context: Any) -> None:
        """Called when a step completes execution"""

    async def on_step_error(self, step: Any, error: Exception, context: Any) -> None:
        """Called when a step encounters an error"""

    async def on_tool_start(self, tool: Any, inputs: dict[str, Any]) -> None:
        """Called when a tool starts execution"""

    async def on_tool_end(self, tool: Any, outputs: Any) -> None:
        """Called when a tool completes execution"""

    async def on_llm_start(self, llm: Any, prompt: str) -> None:
        """Called when an LLM call starts"""

    async def on_llm_end(self, llm: Any, response: Any) -> None:
        """Called when an LLM call completes"""

    async def on_llm_token(self, token: str) -> None:
        """Called for each token during streaming"""

    async def on_agent_action(self, action: Any) -> None:
        """Called when an agent takes an action"""

    async def on_agent_finish(self, output: Any) -> None:
        """Called when an agent finishes"""

    def _emit_event(self, event_type: str, data: Any, **metadata) -> None:
        """Emit a callback event"""
        event = CallbackEvent(
            type=event_type,
            data=data,
            metadata=metadata,
        )
        self._events.append(event)

    @property
    def events(self) -> list[CallbackEvent]:
        """Get all emitted events"""
        return self._events


class StreamingCallback(CallbackHandler):
    """
    Callback for streaming tokens to clients.

    Example:
        callback = StreamingCallback(
            on_token=lambda t: print(t, end="", flush=True)
        )
        chain = Chain(..., callbacks=[callback])
    """

    def __init__(
        self,
        on_token: Callable[[str], None] | None = None,
        on_complete: Callable[[str], None] | None = None,
        **kwargs,
    ):
        super().__init__(name="streaming", **kwargs)
        self._on_token = on_token
        self._on_complete = on_complete
        self._buffer = ""

    async def on_llm_token(self, token: str) -> None:
        """Handle streaming token"""
        self._buffer += token

        if self._on_token:
            if asyncio.iscoroutinefunction(self._on_token):
                await self._on_token(token)
            else:
                self._on_token(token)

        self._emit_event("token", token)

    async def on_llm_end(self, llm: Any, response: Any) -> None:
        """Handle LLM completion"""
        if self._on_complete:
            if asyncio.iscoroutinefunction(self._on_complete):
                await self._on_complete(self._buffer)
            else:
                self._on_complete(self._buffer)

        self._emit_event("complete", self._buffer)
        self._buffer = ""

    @property
    def buffer(self) -> str:
        """Get current buffer content"""
        return self._buffer


class WebSocketStreamingCallback(StreamingCallback):
    """
    Callback for streaming to WebSocket clients.

    Example:
        callback = WebSocketStreamingCallback(websocket)
        chain = Chain(..., callbacks=[callback])
    """

    def __init__(self, websocket: Any, **kwargs):
        super().__init__(**kwargs)
        self.websocket = websocket

    async def on_llm_token(self, token: str) -> None:
        """Stream token to WebSocket"""
        await super().on_llm_token(token)

        try:
            await self.websocket.send_json(
                {
                    "type": "token",
                    "content": token,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            )
        except Exception as e:
            logger.error("WebSocket send error: %s", e)

    async def on_chain_end(self, chain: Any, outputs: Any) -> None:
        """Send completion message"""
        try:
            await self.websocket.send_json(
                {
                    "type": "complete",
                    "content": str(outputs),
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            )
        except Exception as e:
            logger.error("WebSocket send error: %s", e)


class LoggingCallback(CallbackHandler):
    """
    Callback for logging chain execution.

    Example:
        callback = LoggingCallback(level=logging.DEBUG)
        chain = Chain(..., callbacks=[callback])
    """

    def __init__(
        self,
        level: int = logging.INFO,
        include_inputs: bool = True,
        include_outputs: bool = True,
        **kwargs,
    ):
        super().__init__(name="logging", **kwargs)
        self.level = level
        self.include_inputs = include_inputs
        self.include_outputs = include_outputs

    async def on_chain_start(self, chain: Any, inputs: Any) -> None:
        """Log chain start"""
        msg = f"[Chain:{chain.name}] Starting"
        if self.include_inputs:
            msg += f" with inputs: {str(inputs)[:200]}"
        logger.log(self.level, msg)
        self._emit_event("chain_start", {"chain": chain.name, "inputs": inputs})

    async def on_chain_end(self, chain: Any, outputs: Any) -> None:
        """Log chain end"""
        msg = f"[Chain:{chain.name}] Completed"
        if self.include_outputs:
            msg += f" with outputs: {str(outputs)[:200]}"
        logger.log(self.level, msg)
        self._emit_event("chain_end", {"chain": chain.name, "outputs": outputs})

    async def on_chain_error(self, chain: Any, error: Exception) -> None:
        """Log chain error"""
        logger.error("[Chain:%s] Error: %s", chain.name, error)
        self._emit_event("chain_error", {"chain": chain.name, "error": str(error)})

    async def on_step_start(self, step: Any, inputs: Any, context: Any) -> None:
        """Log step start"""
        msg = f"[Step:{step.name}] Starting"
        if self.include_inputs:
            msg += f" with inputs: {str(inputs)[:100]}"
        logger.log(self.level, msg)

    async def on_step_end(self, step: Any, outputs: Any, context: Any) -> None:
        """Log step end"""
        msg = f"[Step:{step.name}] Completed"
        if self.include_outputs:
            msg += f" with outputs: {str(outputs)[:100]}"
        logger.log(self.level, msg)

    async def on_tool_start(self, tool: Any, inputs: dict[str, Any]) -> None:
        """Log tool start"""
        logger.log(self.level, f"[Tool:{tool.name}] Executing with: {inputs}")

    async def on_tool_end(self, tool: Any, outputs: Any) -> None:
        """Log tool end"""
        logger.log(self.level, f"[Tool:{tool.name}] Result: {str(outputs)[:200]}")

    async def on_llm_start(self, llm: Any, prompt: str) -> None:
        """Log LLM start"""
        logger.log(self.level, f"[LLM:{llm.name}] Calling with prompt: {prompt[:100]}...")

    async def on_llm_end(self, llm: Any, response: Any) -> None:
        """Log LLM end"""
        content = response.content if hasattr(response, "content") else str(response)
        logger.log(self.level, f"[LLM:{llm.name}] Response: {content[:200]}...")

    async def on_agent_action(self, action: Any) -> None:
        """Log agent action"""
        logger.log(self.level, f"[Agent] Action: {action.tool}({action.tool_input})")

    async def on_agent_finish(self, output: Any) -> None:
        """Log agent finish"""
        logger.log(self.level, f"[Agent] Finished with: {str(output)[:200]}")


class MetricsCallback(CallbackHandler):
    """
    Callback for collecting execution metrics.

    Example:
        callback = MetricsCallback()
        chain = Chain(..., callbacks=[callback])
        # After execution
        logger.info(callback.metrics)
    """

    def __init__(self, **kwargs):
        super().__init__(name="metrics", **kwargs)
        self._metrics = {
            "chains": {},
            "steps": {},
            "tools": {},
            "llm_calls": [],
            "total_tokens": 0,
            "total_time_ms": 0,
            "errors": [],
        }
        self._timers: dict[str, float] = {}

    @property
    def metrics(self) -> dict[str, Any]:
        """Get collected metrics"""
        return self._metrics

    async def on_chain_start(self, chain: Any, inputs: Any) -> None:
        """Start chain timer"""
        self._timers[f"chain_{chain.name}"] = time.time()

        if chain.name not in self._metrics["chains"]:
            self._metrics["chains"][chain.name] = {
                "executions": 0,
                "successes": 0,
                "failures": 0,
                "total_time_ms": 0,
            }

        self._metrics["chains"][chain.name]["executions"] += 1

    async def on_chain_end(self, chain: Any, outputs: Any) -> None:
        """Record chain completion and flush UCF dimensions"""
        timer_key = f"chain_{chain.name}"
        if timer_key in self._timers:
            elapsed = (time.time() - self._timers[timer_key]) * 1000
            self._metrics["chains"][chain.name]["total_time_ms"] += elapsed
            self._metrics["chains"][chain.name]["successes"] += 1
            self._metrics["total_time_ms"] += elapsed
            del self._timers[timer_key]

        # Flush derived UCF dimensions to the platform pipeline
        agent_name = chain.name if hasattr(chain, "name") else None
        await self.flush_to_ucf(phase="chain_execution", agent=agent_name)

    async def on_chain_error(self, chain: Any, error: Exception) -> None:
        """Record chain error"""
        self._metrics["chains"][chain.name]["failures"] += 1
        self._metrics["errors"].append(
            {
                "type": "chain",
                "name": chain.name,
                "error": str(error),
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )

    async def on_step_start(self, step: Any, inputs: Any, context: Any) -> None:
        """Start step timer"""
        self._timers[f"step_{step.name}"] = time.time()

        if step.name not in self._metrics["steps"]:
            self._metrics["steps"][step.name] = {
                "executions": 0,
                "total_time_ms": 0,
            }

        self._metrics["steps"][step.name]["executions"] += 1

    async def on_step_end(self, step: Any, outputs: Any, context: Any) -> None:
        """Record step completion"""
        timer_key = f"step_{step.name}"
        if timer_key in self._timers:
            elapsed = (time.time() - self._timers[timer_key]) * 1000
            self._metrics["steps"][step.name]["total_time_ms"] += elapsed
            del self._timers[timer_key]

    async def on_tool_start(self, tool: Any, inputs: dict[str, Any]) -> None:
        """Start tool timer"""
        self._timers[f"tool_{tool.name}"] = time.time()

        if tool.name not in self._metrics["tools"]:
            self._metrics["tools"][tool.name] = {
                "calls": 0,
                "successes": 0,
                "failures": 0,
                "total_time_ms": 0,
            }

        self._metrics["tools"][tool.name]["calls"] += 1

    async def on_tool_end(self, tool: Any, outputs: Any) -> None:
        """Record tool completion"""
        timer_key = f"tool_{tool.name}"
        if timer_key in self._timers:
            elapsed = (time.time() - self._timers[timer_key]) * 1000
            self._metrics["tools"][tool.name]["total_time_ms"] += elapsed

            if hasattr(outputs, "success"):
                if outputs.success:
                    self._metrics["tools"][tool.name]["successes"] += 1
                else:
                    self._metrics["tools"][tool.name]["failures"] += 1

            del self._timers[timer_key]

    async def on_llm_end(self, llm: Any, response: Any) -> None:
        """Record LLM call"""
        usage = response.usage if hasattr(response, "usage") else {}

        self._metrics["llm_calls"].append(
            {
                "model": response.model if hasattr(response, "model") else "unknown",
                "tokens": usage,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )

        self._metrics["total_tokens"] += usage.get("total_tokens", 0)

    def get_summary(self) -> dict[str, Any]:
        """Get metrics summary"""
        return {
            "total_chains": sum(c["executions"] for c in self._metrics["chains"].values()),
            "total_steps": sum(s["executions"] for s in self._metrics["steps"].values()),
            "total_tool_calls": sum(t["calls"] for t in self._metrics["tools"].values()),
            "total_llm_calls": len(self._metrics["llm_calls"]),
            "total_tokens": self._metrics["total_tokens"],
            "total_time_ms": self._metrics["total_time_ms"],
            "error_count": len(self._metrics["errors"]),
        }

    def derive_ucf_dimensions(self) -> dict[str, float]:
        """Derive UCF dimensions from collected execution metrics.

        Maps execution data to UCF dimensions (all 0.0-1.0):
        - throughput (throughput): success rate across all chains
        - friction (friction): error rate (lower is better)
        - focus (focus): step completion consistency
        - harmony: tool success rate
        - resilience: recovery from partial failures
        - velocity (velocity): execution speed (faster = higher)
        """
        chains = self._metrics["chains"]
        tools = self._metrics["tools"]
        errors = self._metrics["errors"]

        # -- throughput: chain success rate --
        total_executions = sum(c["executions"] for c in chains.values())
        total_successes = sum(c["successes"] for c in chains.values())
        throughput = total_successes / max(total_executions, 1)

        # -- friction: error rate (lower value = less friction = better) --
        error_count = len(errors)
        # Scale: 0 errors = 0.0 friction, 10+ errors = 1.0 friction
        friction = min(1.0, error_count / 10.0)

        # -- focus: step completion ratio --
        total_steps = sum(s["executions"] for s in self._metrics["steps"].values())
        # If steps ran, focus is based on having steps to run (> 0 = focused)
        # More steps completed per chain = more systematic/focused
        if total_executions > 0 and total_steps > 0:
            focus = min(1.0, total_steps / max(total_executions, 1) * 0.25)
        else:
            focus = 0.5  # Neutral when no data

        # -- harmony: tool success rate --
        total_tool_calls = sum(t["calls"] for t in tools.values())
        total_tool_successes = sum(t["successes"] for t in tools.values())
        if total_tool_calls > 0:
            harmony = total_tool_successes / total_tool_calls
        else:
            harmony = 0.75  # Neutral when no tool calls

        # -- resilience: chains that had failures but overall system kept running --
        total_failures = sum(c["failures"] for c in chains.values())
        if total_executions > 0:
            failure_rate = total_failures / total_executions
            resilience = 1.0 - failure_rate  # No failures = max resilience
        else:
            resilience = 0.7  # Neutral default

        # -- velocity (velocity): based on avg chain time vs baseline (5000ms) --
        baseline_ms = 5000.0
        if total_executions > 0 and self._metrics["total_time_ms"] > 0:
            avg_time_ms = self._metrics["total_time_ms"] / total_executions
            # Faster than baseline = higher velocity (capped at 1.0)
            velocity = min(1.0, baseline_ms / max(avg_time_ms, 1.0))
        else:
            velocity = 0.5  # Neutral default

        return {
            "harmony": max(0.0, min(1.0, harmony)),
            "resilience": max(0.0, min(1.0, resilience)),
            "throughput": max(0.0, min(1.0, throughput)),
            "focus": max(0.0, min(1.0, focus)),
            "friction": max(0.0, min(1.0, friction)),
            "velocity": max(0.0, min(1.0, velocity)),
        }

    async def flush_to_ucf(self, phase: str = "chain_execution", agent: str | None = None) -> None:
        """Push derived UCF dimensions to the UCF state file and history tracker.

        Call this after chain execution completes to propagate metrics
        into the platform-wide UCF pipeline.
        """
        dims = self.derive_ucf_dimensions()
        summary = self.get_summary()

        # Skip if no meaningful data collected
        if summary["total_chains"] == 0:
            return

        # 1. Update current UCF state (file + Redis cache)
        try:
            from apps.backend.core.ucf_helpers import get_current_ucf, update_ucf_state

            current = get_current_ucf()

            # Blend: 70% existing state + 30% new observation (EMA-style)
            alpha = 0.3
            blended = {}
            for dim in ("harmony", "resilience", "throughput", "focus", "friction", "velocity"):
                old_val = current.get(dim, 0.5)
                new_val = dims[dim]
                blended[dim] = old_val * (1 - alpha) + new_val * alpha

            update_ucf_state(blended, save_to_file=True)
            logger.info(
                "UCF state updated from chain metrics: throughput=%.2f friction=%.2f harmony=%.2f",
                blended["throughput"],
                blended["friction"],
                blended["harmony"],
            )
        except Exception as e:
            logger.warning("Failed to update UCF state from metrics: %s", e)

        # 2. Record to UCF history tracker (SQLite)
        try:
            from apps.backend.coordination.ucf_tracker import UCFTracker

            tracker = UCFTracker()
            tracker.record_metrics(
                harmony=dims["harmony"],
                resilience=dims["resilience"],
                throughput=dims["throughput"],
                focus=dims["focus"],
                friction=dims["friction"],
                velocity=dims["velocity"],
                phase=phase,
                context=f"chains={summary['total_chains']} steps={summary['total_steps']} errors={summary['error_count']}",
                agent=agent,
            )
            logger.debug("UCF history recorded for phase=%s", phase)
        except Exception as e:
            logger.warning("Failed to record UCF history: %s", e)


class CompositeCallback(CallbackHandler):
    """
    Combine multiple callbacks.

    Example:
        callback = CompositeCallback([
            LoggingCallback(),
            MetricsCallback(),
            StreamingCallback(on_token=print)
        ])
    """

    def __init__(self, callbacks: list[CallbackHandler], **kwargs):
        super().__init__(name="composite", **kwargs)
        self.callbacks = callbacks

    async def _call_all(self, method: str, *args, **kwargs) -> None:
        """Call method on all callbacks"""
        for callback in self.callbacks:
            handler = getattr(callback, method, None)
            if handler:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(*args, **kwargs)
                    else:
                        handler(*args, **kwargs)
                except Exception as e:
                    logger.error("Callback %s.%s error: %s", callback.name, method, e)

    async def on_chain_start(self, chain: Any, inputs: Any) -> None:
        await self._call_all("on_chain_start", chain, inputs)

    async def on_chain_end(self, chain: Any, outputs: Any) -> None:
        await self._call_all("on_chain_end", chain, outputs)

    async def on_chain_error(self, chain: Any, error: Exception) -> None:
        await self._call_all("on_chain_error", chain, error)

    async def on_step_start(self, step: Any, inputs: Any, context: Any) -> None:
        await self._call_all("on_step_start", step, inputs, context)

    async def on_step_end(self, step: Any, outputs: Any, context: Any) -> None:
        await self._call_all("on_step_end", step, outputs, context)

    async def on_tool_start(self, tool: Any, inputs: dict[str, Any]) -> None:
        await self._call_all("on_tool_start", tool, inputs)

    async def on_tool_end(self, tool: Any, outputs: Any) -> None:
        await self._call_all("on_tool_end", tool, outputs)

    async def on_llm_start(self, llm: Any, prompt: str) -> None:
        await self._call_all("on_llm_start", llm, prompt)

    async def on_llm_end(self, llm: Any, response: Any) -> None:
        await self._call_all("on_llm_end", llm, response)

    async def on_llm_token(self, token: str) -> None:
        await self._call_all("on_llm_token", token)

    async def on_agent_action(self, action: Any) -> None:
        await self._call_all("on_agent_action", action)

    async def on_agent_finish(self, output: Any) -> None:
        await self._call_all("on_agent_finish", output)
