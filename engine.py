"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

🌀 Helix Spirals Execution Engine
Core execution logic for automation workflows
"""

import asyncio
import logging
import re
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

try:
    from apps.backend.core.redis_client import get_redis
except ImportError:
    get_redis = None  # type: ignore[assignment]

from apps.backend.core.exceptions import WorkflowError

from .actions import ActionExecutor
from .models import (
    Action,
    Condition,
    ConditionOperator,
    ExecutionContext,
    ExecutionError,
    ExecutionLog,
    ExecutionStatus,
    Spiral,
)
from .storage import SpiralStorage

logger = logging.getLogger(__name__)


class RateLimiter:
    """Redis-backed sliding window rate limiter for spiral execution.

    Uses a Redis sorted set keyed by spiral_id so the limit is enforced
    across all Railway replicas, not just within a single process.
    Falls back to allow-all when Redis is unavailable.
    """

    def __init__(self, spiral_id: str, max_executions: int, window_ms: int, strategy: str = "sliding"):
        self.spiral_id = spiral_id
        self.max_executions = max_executions
        self.window_ms = window_ms
        self.strategy = strategy
        self._redis_key = f"spiral:rate:{spiral_id}"

    async def allow(self) -> bool:
        """Check if execution is allowed (async, Redis-backed)."""
        if get_redis is None:
            return True
        try:
            redis = await get_redis()
            if redis is None:
                return True
            now_ms = datetime.now(UTC).timestamp() * 1000
            cutoff_ms = now_ms - self.window_ms
            pipe = redis.pipeline()
            # Remove entries outside the window, count remaining, add this one
            pipe.zremrangebyscore(self._redis_key, "-inf", cutoff_ms)
            pipe.zcard(self._redis_key)
            pipe.zadd(self._redis_key, {str(now_ms): now_ms})
            pipe.pexpire(self._redis_key, int(self.window_ms) + 1000)
            results = await pipe.execute()
            count_before = results[1]
            if count_before >= self.max_executions:
                # Undo the zadd — we're over limit
                await redis.zrem(self._redis_key, str(now_ms))
                return False
            return True
        except Exception as exc:
            logger.warning("RateLimiter Redis error for %s — allowing: %s", self.spiral_id, exc)
            return True


class SpiralEngine:
    """Main execution engine for Helix Spirals"""

    def __init__(self, storage: SpiralStorage, ws_manager=None):
        self.storage = storage
        self.ws_manager = ws_manager
        self.action_executor = ActionExecutor(self)
        self.execution_queue: dict[str, ExecutionContext] = {}
        self.rate_limiters: dict[str, RateLimiter] = {}
        self.active_executions: dict[str, asyncio.Task] = {}

    async def execute(
        self,
        spiral_id: str,
        trigger_type: str,
        trigger_data: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        spiral: "Spiral | None" = None,
    ) -> ExecutionContext:
        """Execute a spiral.

        Args:
            spiral_id: ID used for rate-limiting and logging. Required even when
                       ``spiral`` is provided.
            spiral: Pre-fetched :class:`Spiral` object. When provided, the storage
                    lookup is skipped — useful for tests and replay paths where the
                    caller already holds the spiral definition.
        """
        try:
            if spiral is None:
                if self.storage is None:
                    raise ValueError(
                        "SpiralEngine requires storage to look up spirals by ID; "
                        "pass spiral= directly when running without storage"
                    )
                spiral = await self.storage.get_spiral(spiral_id)
            if not spiral:
                raise ValueError(f"Spiral {spiral_id} not found")

            if not spiral.enabled:
                if self.storage:
                    task_id = await self.storage.hold_task(
                        spiral_id=spiral_id,
                        trigger_type=trigger_type,
                        trigger_data=trigger_data or {},
                        metadata=metadata,
                    )
                    logger.info("Spiral %s disabled — held task %s", spiral_id, task_id)
                    return ExecutionContext(
                        spiral_id=spiral_id,
                        execution_id=task_id,
                        trigger={"type": trigger_type, "data": trigger_data, "held": True},
                        variables={},
                        status=ExecutionStatus.PAUSED,
                    )
                raise ValueError(f"Spiral {spiral_id} is disabled")

            # Check rate limiting
            if spiral.rate_limiting:
                limiter = self._get_rate_limiter(spiral_id, spiral.rate_limiting)
                if not await limiter.allow():
                    raise ValueError(f"Rate limit exceeded for spiral {spiral_id}")

            # Create execution context
            context = ExecutionContext(
                spiral_id=spiral_id,
                execution_id=str(uuid4()),
                trigger={
                    "type": trigger_type,
                    "data": trigger_data,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                variables=self._initialize_variables(spiral, trigger_data),
                status=ExecutionStatus.PENDING,
            )

            # Store in queue
            self.execution_queue[context.execution_id] = context

            # Execute spiral asynchronously
            task = asyncio.create_task(self._execute_spiral(spiral, context))
            self.active_executions[context.execution_id] = task

            # Wait briefly to get initial status
            await asyncio.sleep(0.1)

            return context

        except Exception as e:
            logger.error("Failed to execute spiral %s: %s", spiral_id, e)
            raise

    async def _execute_spiral(self, spiral: Spiral, context: ExecutionContext) -> ExecutionContext:
        """Internal spiral execution logic"""
        try:
            await self._log(context, "info", f"Starting spiral execution: {spiral.name}")

            # Broadcast execution start
            if self.ws_manager:
                await self.ws_manager.broadcast(
                    {
                        "type": "execution_started",
                        "spiralId": context.spiral_id,
                        "executionId": context.execution_id,
                        "timestamp": context.started_at,
                    }
                )

            # Validate trigger conditions
            if spiral.trigger.conditions:
                conditions_met = await self._evaluate_conditions(spiral.trigger.conditions, context)
                if not conditions_met:
                    await self._log(
                        context,
                        "info",
                        "Trigger conditions not met, skipping execution",
                    )
                    context.status = ExecutionStatus.COMPLETED
                    context.completed_at = datetime.now(UTC).isoformat()
                    return context

            # ── Pre-execution planning ────────────────────────────────────
            # If enabled, a cheap model creates an execution plan before
            # actions run.  The plan is injected into context.variables so
            # downstream actions (especially LLM-backed ones) can reference it.
            if spiral.metadata and spiral.metadata.get("enable_planning"):
                plan = await self._generate_execution_plan(spiral, context)
                if plan:
                    context.variables["_execution_plan"] = plan
                    await self._log(context, "info", "Execution plan generated")

            # Execute actions
            for action_index, action in enumerate(spiral.actions):
                context.current_action = action.id

                # Check action conditions
                if action.conditions:
                    conditions_met = await self._evaluate_conditions(action.conditions, context)
                    if not conditions_met:
                        await self._log(
                            context,
                            "info",
                            f"Skipping action {action.name}: conditions not met",
                        )
                        continue

                # Execute action with retry logic
                await self._execute_action_with_retry(action, context)

                # Quality gate validation (if configured)
                if action.quality_gate and context.status != ExecutionStatus.FAILED:
                    await self._run_quality_gate(action, context)

                # ── Time-travel checkpoint (P3) ───────────────────────────
                # Persist a variables snapshot after each action so the
                # execution can be rewound and replayed from any step.
                try:
                    await self.storage.save_step_checkpoint(
                        context=context,
                        action_index=action_index,
                        action_id=str(action.id) if action.id else None,
                        action_name=action.name or "",
                        action_type=action.type.value if hasattr(action.type, "value") else str(action.type),
                    )
                except Exception as _ckpt_err:
                    logger.debug("Checkpoint save non-fatal: %s", _ckpt_err)
                # ─────────────────────────────────────────────────────────

                # Human-in-the-loop: pause if action requested human input
                if context.status == ExecutionStatus.WAITING_INPUT:
                    await self._log(
                        context,
                        "info",
                        f"Execution paused for human input at action: {action.name}",
                    )
                    if self.ws_manager:
                        await self.ws_manager.broadcast(
                            {
                                "type": "execution_paused",
                                "spiralId": context.spiral_id,
                                "executionId": context.execution_id,
                                "action": action.name,
                                "humanInput": context.pending_human_input,
                            }
                        )
                    return context  # Pause — resume via external API

                # Handle continue on error
                if context.status == ExecutionStatus.FAILED and not action.continue_on_error:
                    break

            # Mark as completed if not failed
            if context.status not in (ExecutionStatus.FAILED, ExecutionStatus.WAITING_INPUT):
                context.status = ExecutionStatus.COMPLETED

            context.completed_at = datetime.now(UTC).isoformat()
            await self._log(context, "info", f"Spiral execution completed: {spiral.name}")

            # Update statistics
            await self.storage.update_spiral_statistics(spiral.id, context)

            # Calculate and record UCF impact
            if not context.ucf_impact or not any(context.ucf_impact.values()):
                context.ucf_impact = self._calculate_ucf_impact(context)

            if context.ucf_impact:
                await self._update_ucf_metrics(context.ucf_impact)

            # Broadcast completion
            if self.ws_manager:
                await self.ws_manager.broadcast(
                    {
                        "type": "execution_completed",
                        "spiralId": context.spiral_id,
                        "executionId": context.execution_id,
                        "status": context.status.value,
                        "timestamp": context.completed_at,
                    }
                )

        except Exception as e:
            context.status = ExecutionStatus.FAILED
            context.error = ExecutionError(message=str(e), action_id=context.current_action)
            context.completed_at = datetime.now(UTC).isoformat()
            await self._log(context, "error", f"Spiral execution failed: {e!s}")

            # Calculate and record UCF impact for failure
            if not context.ucf_impact or not any(context.ucf_impact.values()):
                context.ucf_impact = self._calculate_ucf_impact(context)

            if context.ucf_impact:
                await self._update_ucf_metrics(context.ucf_impact)

            # Broadcast failure
            if self.ws_manager:
                await self.ws_manager.broadcast(
                    {
                        "type": "execution_failed",
                        "spiralId": context.spiral_id,
                        "executionId": context.execution_id,
                        "error": str(e),
                        "timestamp": context.completed_at,
                    }
                )

            # Error workflow: trigger a separate spiral on failure
            if (
                context.status == ExecutionStatus.FAILED
                and hasattr(spiral, "error_workflow_id")
                and spiral.error_workflow_id
            ):
                await self._trigger_error_spiral(spiral, context)

        finally:
            # Clean up
            self.execution_queue.pop(context.execution_id, None)
            self.active_executions.pop(context.execution_id, None)

            # Store execution history
            await self.storage.save_execution_history(context)

        return context

    async def _execute_action_with_retry(self, action: Action, context: ExecutionContext):
        """Execute action with retry logic"""
        max_attempts = action.retry_config.max_attempts if action.retry_config else 1
        last_error = None

        for attempt in range(1, max_attempts + 1):
            try:
                await self._log(
                    context,
                    "info",
                    f"Executing action {action.name} (attempt {attempt}/{max_attempts})",
                )

                # Set timeout if specified
                if action.timeout:
                    await asyncio.wait_for(
                        self.action_executor.execute(action, context),
                        timeout=action.timeout / 1000.0,  # Convert ms to seconds
                    )
                else:
                    await self.action_executor.execute(action, context)

                await self._log(
                    context,
                    "info",
                    f"Action {action.name} completed successfully",
                )
                return

            except TimeoutError:
                last_error = "Action timeout"
                await self._log(context, "error", f"Action {action.name} timed out")

            except Exception as e:
                last_error = str(e)
                await self._log(context, "error", f"Action {action.name} failed: {e!s}")

            if attempt < max_attempts:
                delay = self._calculate_retry_delay(attempt, action.retry_config)
                await self._log(
                    context,
                    "info",
                    f"Retrying action {action.name} in {delay}ms",
                )
                await asyncio.sleep(delay / 1000.0)

        # All retries failed
        if last_error and not action.continue_on_error:
            raise WorkflowError(last_error)

    async def _run_quality_gate(self, action: Action, context: ExecutionContext):
        """Run quality gate validation on an action's output.

        When the gate fails and retries are available, the failure reason
        is injected into the execution context so the retried action can
        see WHY it failed and produce a corrected output.
        """
        from apps.backend.services.quality_gate_validator import validate_quality_gate

        gate = action.quality_gate
        output = context.variables.get(f"action_{action.id}_result")
        feedback_on_retry = getattr(gate, "feedback_on_retry", True)

        for attempt in range(1 + gate.max_retries):
            result = await validate_quality_gate(gate, output, action.name)

            if result.passed:
                # Clear any previous feedback on success
                context.variables.pop(f"_quality_gate_feedback_{action.id}", None)
                await self._log(
                    context,
                    "info",
                    f"Quality gate passed for '{action.name}': {result.reason}",
                )
                return

            await self._log(
                context,
                "warning",
                f"Quality gate failed for '{action.name}' (attempt {attempt + 1}/{1 + gate.max_retries}): {result.reason}",
            )

            if attempt < gate.max_retries:
                # Inject failure feedback into context before retry
                if feedback_on_retry:
                    feedback = (
                        "QUALITY GATE FEEDBACK: Your previous output was rejected. "
                        f"Reason: {result.reason}. Please correct your output to address this issue."
                    )
                    context.variables[f"_quality_gate_feedback_{action.id}"] = feedback
                    context.variables[f"_quality_gate_last_rejection_{action.id}"] = result.reason
                    await self._log(
                        context,
                        "info",
                        f"Injecting quality gate feedback for retry of '{action.name}'",
                    )

                # Re-execute the action and re-check
                await self._execute_action_with_retry(action, context)
                output = context.variables.get(f"action_{action.id}_result")

        # All gate retries exhausted
        on_failure = gate.on_failure
        if on_failure == "skip":
            await self._log(context, "warning", f"Quality gate: skipping failed action '{action.name}'")
        elif on_failure == "escalate":
            await self._log(context, "warning", f"Quality gate: escalating failure for '{action.name}'")
            context.variables[f"_quality_gate_escalation_{action.id}"] = result.reason
        else:
            # Default: fail the entire spiral
            context.status = ExecutionStatus.FAILED
            context.error = ExecutionError(
                message=f"Quality gate failed for '{action.name}': {result.reason}",
                action_id=action.id,
            )

    async def _evaluate_conditions(self, conditions: list[Condition], context: ExecutionContext) -> bool:
        """Evaluate conditions"""
        for condition in conditions:
            result = await self._evaluate_condition(condition, context)

            # Handle logical operators
            if condition.logical_operator == "OR" and result:
                return True
            if condition.logical_operator == "AND" and not result:
                return False

            # Handle nested conditions
            if condition.nested_conditions:
                nested_result = await self._evaluate_conditions(condition.nested_conditions, context)
                if condition.logical_operator == "OR" and nested_result:
                    return True
                if condition.logical_operator == "AND" and not nested_result:
                    return False

        return True

    @staticmethod
    def _safe_regex_match(value: object, pattern: object) -> bool:
        """Match a regex pattern with safeguards against ReDoS.

        Limits pattern length and catches malformed patterns.
        """
        pattern_str = str(pattern)
        max_regex_len = 500
        if len(pattern_str) > max_regex_len:
            return False
        try:
            return bool(re.match(pattern_str, str(value)))
        except re.error:
            return False

    async def _evaluate_condition(self, condition: Condition, context: ExecutionContext) -> bool:
        """Evaluate single condition"""
        field_value = self._get_field_value(condition.field, context)
        condition_value = self._resolve_variable(condition.value, context)

        operator_map = {
            ConditionOperator.EQUALS: lambda a, b: a == b,
            ConditionOperator.NOT_EQUALS: lambda a, b: a != b,
            ConditionOperator.GREATER_THAN: lambda a, b: float(a) > float(b),
            ConditionOperator.LESS_THAN: lambda a, b: float(a) < float(b),
            ConditionOperator.CONTAINS: lambda a, b: str(b) in str(a),
            ConditionOperator.STARTS_WITH: lambda a, b: str(a).startswith(str(b)),
            ConditionOperator.ENDS_WITH: lambda a, b: str(a).endswith(str(b)),
            ConditionOperator.REGEX_MATCH: lambda a, b: self._safe_regex_match(a, b),
            ConditionOperator.IN_LIST: lambda a, b: (a in b if isinstance(b, list) else False),
            ConditionOperator.IS_NULL: lambda a, b: a is None,
            ConditionOperator.IS_NOT_NULL: lambda a, b: a is not None,
        }

        evaluator = operator_map.get(condition.operator)
        if not evaluator:
            return False

        try:
            return evaluator(field_value, condition_value)
        except (ValueError, TypeError, KeyError, IndexError, re.error):
            return False

    def _initialize_variables(self, spiral: Spiral, trigger_data: dict[str, Any]) -> dict[str, Any]:
        """Initialize execution variables"""
        variables = {
            "trigger": trigger_data,
            "spiral": {"id": spiral.id, "name": spiral.name, "version": spiral.version},
            "execution": {
                "id": str(uuid4()),
                "timestamp": datetime.now(UTC).isoformat(),
            },
            "performance_score": (spiral.performance_score.value if spiral.performance_score else 5),
        }

        # Add default variables
        if spiral.variables:
            for var in spiral.variables:
                if var.default_value is not None:
                    variables[var.name] = var.default_value

        return variables

    def _get_field_value(self, field: str, context: ExecutionContext) -> Any:
        """Get field value from context using dot notation"""
        parts = field.split(".")
        value = context.dict()

        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None

        return value

    def _resolve_variable(self, value: Any, context: ExecutionContext) -> Any:
        """Resolve variables in value"""
        if isinstance(value, str) and value.startswith("{{") and value.endswith("}}"):
            var_name = value[2:-2].strip()
            return context.variables.get(var_name, value)
        elif isinstance(value, dict):
            return {k: self._resolve_variable(v, context) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._resolve_variable(v, context) for v in value]
        return value

    def _get_rate_limiter(self, spiral_id: str, config: dict[str, Any]) -> RateLimiter:
        """Get or create rate limiter for spiral"""
        if spiral_id not in self.rate_limiters:
            self.rate_limiters[spiral_id] = RateLimiter(
                spiral_id, config.max_executions, config.window_ms, config.strategy
            )
        return self.rate_limiters[spiral_id]

    def _calculate_retry_delay(self, attempt: int, config) -> int:
        """Calculate retry delay based on strategy"""
        if not config:
            return 1000

        base_delay = config.initial_delay

        if config.backoff_strategy == "fixed":
            delay = base_delay
        elif config.backoff_strategy == "linear":
            delay = base_delay * attempt
        elif config.backoff_strategy == "exponential":
            delay = base_delay * (2 ** (attempt - 1))
        else:
            delay = base_delay

        if config.max_delay:
            delay = min(delay, config.max_delay)

        return delay

    # ── Pre-execution planner ─────────────────────────────────────────────

    async def _generate_execution_plan(self, spiral: Spiral, context: ExecutionContext) -> str | None:
        """Use a cheap model to create an execution plan for the spiral.

        The plan summarises each step's purpose, expected inputs/outputs,
        and potential failure points.  It is injected into
        ``context.variables["_execution_plan"]`` so downstream actions
        (especially LLM-backed ones) can reference it for coherence.

        Returns the plan text, or None on failure.
        """
        # Build a concise description of actions for the planner
        step_descriptions = []
        for i, action in enumerate(spiral.actions, 1):
            desc = "Step {}: [{}] {} — {}".format(
                i,
                action.type.value,
                action.name,
                action.description or "no description",
            )
            if action.quality_gate:
                desc += f" (quality gate: {action.quality_gate.mode})"
            step_descriptions.append(desc)

        steps_text = "\n".join(step_descriptions)
        trigger_data = context.variables.get("trigger_data", {})

        prompt = (
            "You are a workflow planning assistant. Given the following automation "
            "workflow, create a concise execution plan.\n\n"
            "Workflow: {name}\n"
            "Description: {desc}\n"
            "Trigger data: {trigger}\n\n"
            "Steps:\n{steps}\n\n"
            "For each step, briefly note:\n"
            "1. What it should accomplish\n"
            "2. What data it needs from previous steps\n"
            "3. What could go wrong\n\n"
            "Keep the plan under 300 words. Be specific, not generic."
        ).format(
            name=spiral.name,
            desc=spiral.description or "N/A",
            trigger=str(trigger_data)[:500],
            steps=steps_text,
        )

        try:
            from apps.backend.services.unified_llm import unified_llm

            plan = await unified_llm.generate(
                prompt=prompt,
                model="openai/gpt-4o-mini",
                max_tokens=512,
                temperature=0.3,
            )
            return plan.strip() if plan else None
        except ImportError:
            logger.debug("UnifiedLLMService not available for planning")
            return None
        except Exception as e:
            logger.warning("Execution plan generation failed: %s", e)
            return None

    async def _trigger_error_spiral(self, spiral: Spiral, context: ExecutionContext) -> None:
        """Trigger a separate error-handling spiral when the primary one fails.

        Passes the failure details so the error spiral can alert, retry,
        or clean up.
        """
        try:
            error_spiral = await self.storage.get_spiral(spiral.error_workflow_id)
            if not error_spiral:
                logger.warning(
                    "Error spiral '%s' not found, skipping",
                    spiral.error_workflow_id,
                )
                return

            error_trigger = {
                "error_source": "spiral",
                "source_spiral_id": spiral.id,
                "source_spiral_name": spiral.name,
                "source_execution_id": context.execution_id,
                "error_message": context.error.message if context.error else "Unknown",
                "error_action_id": context.error.action_id if context.error else None,
                "failed_at": context.completed_at,
            }

            error_context = ExecutionContext(
                spiral_id=error_spiral.id,
                trigger=error_trigger,
                variables={"_is_error_workflow": True},
            )

            logger.info(
                "Triggering error spiral '%s' for failed execution %s",
                spiral.error_workflow_id,
                context.execution_id,
            )
            await self._execute_spiral(error_spiral, error_context)
        except Exception as e:
            logger.error(
                "Error spiral '%s' itself failed: %s",
                spiral.error_workflow_id,
                e,
            )

    async def _log(self, context: ExecutionContext, level: str, message: str):
        """Add log entry to execution context"""
        log_entry = ExecutionLog(
            timestamp=datetime.now(UTC).isoformat(),
            level=level,
            message=message,
            action_id=context.current_action,
        )
        context.logs.append(log_entry)
        logger.log(
            logging.INFO if level == "info" else logging.ERROR,
            f"[{context.execution_id}] {message}",
        )

    def _calculate_ucf_impact(self, context: ExecutionContext) -> dict[str, float]:
        """Calculate UCF impact based on execution results"""
        # Base metrics on execution success/failure
        if context.status == ExecutionStatus.COMPLETED:
            harmony = 0.1  # Successful execution improves harmony
            resilience = 0.05  # Builds system resilience
            throughput = 0.08  # Generates creative energy
            focus = 0.06  # Increases clarity
            friction = -0.02  # Reduces suffering
            velocity = 0.04  # Improves perspective
        elif context.status == ExecutionStatus.FAILED:
            harmony = -0.05  # Failure reduces harmony
            resilience = 0.02  # But builds resilience through challenges
            throughput = -0.03
            focus = -0.02
            friction = 0.05  # Failure increases suffering
            velocity = 0.01
        else:
            # Pending/running/cancelled - minimal impact
            harmony = 0.0
            resilience = 0.0
            throughput = 0.0
            focus = 0.0
            friction = 0.0
            velocity = 0.0

        # Adjust based on execution time (faster is better for throughput/focus)
        if context.started_at and context.completed_at:
            try:
                start = datetime.fromisoformat(context.started_at.replace("Z", "+00:00"))
                end = datetime.fromisoformat(context.completed_at.replace("Z", "+00:00"))
                duration_seconds = (end - start).total_seconds()

                # Faster executions improve throughput and focus
                if duration_seconds < 1.0:
                    throughput += 0.02
                    focus += 0.02
                elif duration_seconds > 10.0:
                    throughput -= 0.01
                    focus -= 0.01
            except (ValueError, AttributeError) as exc:
                logger.debug("Error parsing step duration for UCF metrics: %s", exc)

        # Error count affects friction and harmony
        error_count = sum(1 for log in context.logs if log.level == "error")
        if error_count > 0:
            friction += error_count * 0.01
            harmony -= error_count * 0.01

        return {
            "harmony": max(0.0, min(2.0, harmony)),
            "resilience": max(0.0, min(3.0, resilience)),
            "throughput": max(0.0, min(1.0, throughput)),
            "focus": max(0.0, min(1.0, focus)),
            "friction": max(0.0, min(0.5, friction)),
            "velocity": max(0.0, min(2.0, velocity)),
            "phase": "COHERENT" if harmony > 0 else "TURBULENT",
        }

    async def _update_ucf_metrics(self, ucf_impact: dict[str, float]):
        """Update UCF metrics based on execution impact"""
        try:
            # Import UCF tracker
            from apps.backend.coordination.ucf_tracker import UCFTracker

            # Initialize tracker
            tracker = UCFTracker()

            # Record the metrics
            tracker.record_metrics(
                harmony=ucf_impact.get("harmony", 0.0),
                resilience=ucf_impact.get("resilience", 0.0),
                throughput=ucf_impact.get("throughput", 0.0),
                focus=ucf_impact.get("focus", 0.0),
                friction=ucf_impact.get("friction", 0.0),
                velocity=ucf_impact.get("velocity", 0.0),
                phase=ucf_impact.get("phase", "COHERENT"),
                context="spiral_execution",
                agent="SpiralEngine",
            )

            logger.info("UCF metrics recorded: %s", ucf_impact)
        except Exception as e:
            logger.error("Failed to update UCF metrics: %s", e)

    async def drain_held_tasks(self, spiral_id: str) -> list[dict]:
        """Drain held tasks for a spiral that was just re-enabled."""
        if not self.storage:
            return []
        held = await self.storage.drain_held_tasks(spiral_id)
        results = []
        for task in held:
            try:
                ctx = await self.execute(
                    spiral_id=spiral_id,
                    trigger_type=task["trigger_type"],
                    trigger_data=task.get("trigger_data", {}),
                    metadata={"source": "held_task_drain", "original_held_id": str(task["id"])},
                )
                results.append({"held_id": str(task["id"]), "execution_id": ctx.execution_id, "status": "executing"})
            except Exception as e:
                logger.warning("Failed to drain held task %s: %s", task["id"], e)
                results.append({"held_id": str(task["id"]), "status": "failed", "error": str(e)})
        return results

    async def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running execution"""
        if execution_id in self.active_executions:
            task = self.active_executions[execution_id]
            task.cancel()

            if execution_id in self.execution_queue:
                context = self.execution_queue[execution_id]
                context.status = ExecutionStatus.CANCELLED
                context.completed_at = datetime.now(UTC).isoformat()
                await self.storage.save_execution_history(context)

            return True
        return False

    async def get_active_executions(self) -> list[ExecutionContext]:
        """Get all active executions"""
        return list(self.execution_queue.values())

    async def get_execution_status(self, execution_id: str) -> ExecutionContext | None:
        """Get execution status"""
        if execution_id in self.execution_queue:
            return self.execution_queue[execution_id]

        # Check history
        return await self.storage.get_execution_history(execution_id)
