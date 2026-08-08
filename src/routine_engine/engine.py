"""Bounded, dependency-aware workflow execution."""

from __future__ import annotations

import asyncio
import inspect
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from types import MappingProxyType
from typing import Any

from .errors import ActionRegistrationError, WorkflowValidationError
from .models import (
    ActionContext,
    ExecutionPlan,
    MAX_ERROR_LENGTH,
    MAX_INPUT_BYTES,
    MAX_STEP_OUTPUT_BYTES,
    PlanStep,
    RunResult,
    RunStatus,
    Step,
    StepResult,
    StepStatus,
    Workflow,
    utc_now,
    validate_json_payload,
)
from .storage import JsonStore

Action = Callable[[ActionContext], Any | Awaitable[Any]]
_REFERENCE = re.compile(r"^\{\{\s*(input|steps)\.([A-Za-z0-9_.-]+)\s*\}\}$")


class RoutineEngine:
    """Register trusted Python actions and execute validated workflow DAGs."""

    def __init__(self, *, store: JsonStore | None = None) -> None:
        self._actions: dict[str, Action] = {}
        self._store = store

    @property
    def actions(self) -> tuple[str, ...]:
        return tuple(sorted(self._actions))

    def register(self, name: str, action: Action, *, replace: bool = False) -> None:
        """Register trusted application code under a workflow-visible name."""

        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,127}", name):
            raise ActionRegistrationError("action name is not a valid identifier")
        if not callable(action):
            raise ActionRegistrationError(f"action '{name}' must be callable")
        if name in self._actions and not replace:
            raise ActionRegistrationError(f"action '{name}' is already registered")
        self._actions[name] = action

    def validate(self, workflow: Workflow | Mapping[str, Any]) -> Workflow:
        """Validate structure, dependency graph, and action registrations."""

        definition = workflow if isinstance(workflow, Workflow) else Workflow.from_dict(workflow)
        missing = sorted({step.action for step in definition.steps} - self._actions.keys())
        if missing:
            raise WorkflowValidationError(f"unregistered action(s): {', '.join(missing)}")
        return definition

    def plan(self, workflow: Workflow | Mapping[str, Any]) -> ExecutionPlan:
        """Return stable topological layers without executing any actions."""

        definition = self.validate(workflow)
        complete: set[str] = set()
        layers: list[tuple[PlanStep, ...]] = []
        while len(complete) < len(definition.steps):
            ready = tuple(
                PlanStep(step.id, step.action, step.needs)
                for step in definition.steps
                if step.id not in complete and set(step.needs) <= complete
            )
            layers.append(ready)
            complete.update(step.id for step in ready)
        return ExecutionPlan(
            workflow_id=definition.id,
            schema_version=definition.schema_version,
            max_concurrency=definition.max_concurrency,
            layers=tuple(layers),
        )

    def run(
        self,
        workflow: Workflow | Mapping[str, Any],
        inputs: Mapping[str, Any] | None = None,
        *,
        resume_run_id: str | None = None,
    ) -> RunResult:
        """Run a workflow from synchronous code."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.arun(workflow, inputs, resume_run_id=resume_run_id))
        raise RuntimeError("RoutineEngine.run() cannot be called inside an event loop; use 'await arun(...)'")

    def resume(self, run_id: str) -> RunResult:
        """Resume an interrupted persisted run from its successful checkpoints."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.aresume(run_id))
        raise RuntimeError("RoutineEngine.resume() cannot be called inside an event loop; use 'await aresume(...)'")

    async def aresume(self, run_id: str) -> RunResult:
        """Asynchronously resume an interrupted persisted run."""

        if self._store is None:
            raise WorkflowValidationError("resuming a run requires a JsonStore")
        record = self._store.get_run(run_id)
        workflow = record.get("workflow")
        inputs = record.get("inputs")
        if not isinstance(workflow, Mapping) or not isinstance(inputs, Mapping):
            raise WorkflowValidationError(f"run '{run_id}' does not contain a resumable snapshot")
        return await self.arun(workflow, inputs, resume_run_id=run_id)

    async def arun(
        self,
        workflow: Workflow | Mapping[str, Any],
        inputs: Mapping[str, Any] | None = None,
        *,
        resume_run_id: str | None = None,
    ) -> RunResult:
        """Run a workflow and return exact per-step terminal states."""

        definition = self.validate(workflow)
        run_inputs = validate_json_payload(dict(inputs or {}), "run inputs", MAX_INPUT_BYTES)
        run_id = resume_run_id or uuid.uuid4().hex
        started_at = utc_now()
        results: dict[str, StepResult] = {}
        if resume_run_id is not None:
            if self._store is None:
                raise WorkflowValidationError("resuming a run requires a JsonStore")
            record = self._store.get_run(resume_run_id)
            if record.get("status") != "running":
                raise WorkflowValidationError(f"run '{resume_run_id}' is not resumable")
            if record.get("workflow") != definition.to_dict() or record.get("inputs") != run_inputs:
                raise WorkflowValidationError("resume workflow and inputs do not match the persisted snapshot")
            try:
                started_at = datetime.fromisoformat(record["started_at"])
                stored_steps = record["steps"]
                if not isinstance(stored_steps, Mapping):
                    raise TypeError
                restored = (StepResult.from_dict(value) for value in stored_steps.values())
                results = {
                    item.step_id: item for item in restored if item.status is StepStatus.SUCCESS
                }
            except (KeyError, TypeError, ValueError, WorkflowValidationError) as exc:
                raise WorkflowValidationError(f"run '{resume_run_id}' has invalid checkpoint data") from exc
        pending = {step.id: step for step in definition.steps if step.id not in results}
        running: dict[str, asyncio.Task[StepResult]] = {}

        if self._store is not None:
            if resume_run_id is None:
                self._store.begin_run(run_id, definition, run_inputs, started_at.isoformat())

        try:
            while pending or running:
                for step_id, step in tuple(pending.items()):
                    dependency_results = [results.get(dependency) for dependency in step.needs]
                    if any(
                        result is not None and result.status is not StepStatus.SUCCESS
                        for result in dependency_results
                    ):
                        now = utc_now()
                        results[step_id] = StepResult(
                            step_id=step_id,
                            status=StepStatus.SKIPPED,
                            attempts=0,
                            started_at=now,
                            finished_at=now,
                            error="dependency did not succeed",
                        )
                        if self._store is not None:
                            self._store.record_step(run_id, results[step_id])
                        del pending[step_id]

                capacity = definition.max_concurrency - len(running)
                ready = [
                    step
                    for step in definition.steps
                    if step.id in pending
                    and all(
                        dependency in results and results[dependency].status is StepStatus.SUCCESS
                        for dependency in step.needs
                    )
                ]
                for step in ready[:capacity]:
                    running[step.id] = asyncio.create_task(
                        self._run_step(definition, step, run_id, run_inputs, results),
                        name=f"routine-engine:{run_id}:{step.id}",
                    )
                    del pending[step.id]

                if not running:
                    if pending:
                        raise RuntimeError("validated workflow became unschedulable")
                    break

                done, _ = await asyncio.wait(running.values(), return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    step_result = task.result()
                    results[step_result.step_id] = step_result
                    if self._store is not None:
                        self._store.record_step(run_id, step_result)
                    del running[step_result.step_id]
        except asyncio.CancelledError:
            for task in running.values():
                task.cancel()
            await asyncio.gather(*running.values(), return_exceptions=True)
            now = utc_now()
            for step_id in (*running.keys(), *pending.keys()):
                results[step_id] = StepResult(
                    step_id=step_id,
                    status=StepStatus.CANCELLED,
                    attempts=0,
                    started_at=now,
                    finished_at=now,
                    error="workflow run cancelled",
                )
            cancelled = RunResult(
                run_id=run_id,
                workflow_id=definition.id,
                status=RunStatus.CANCELLED,
                started_at=started_at,
                finished_at=utc_now(),
                steps=MappingProxyType(_ordered_results(definition, results)),
            )
            if self._store is not None:
                self._store.finish_run(cancelled)
            raise

        status = RunStatus.FAILED if any(r.status is StepStatus.FAILED for r in results.values()) else RunStatus.SUCCESS
        result = RunResult(
            run_id=run_id,
            workflow_id=definition.id,
            status=status,
            started_at=started_at,
            finished_at=utc_now(),
            steps=MappingProxyType(_ordered_results(definition, results)),
        )
        if self._store is not None:
            self._store.finish_run(result)
        return result

    async def _run_step(
        self,
        workflow: Workflow,
        step: Step,
        run_id: str,
        inputs: Mapping[str, Any],
        results: Mapping[str, StepResult],
    ) -> StepResult:
        started_at = utc_now()
        output_values = {dependency: results[dependency].output for dependency in step.needs}
        try:
            params = _resolve_value(dict(step.params), inputs, output_values)
        except (KeyError, TypeError, WorkflowValidationError) as exc:
            return StepResult(
                step_id=step.id,
                status=StepStatus.FAILED,
                attempts=0,
                started_at=started_at,
                finished_at=utc_now(),
                error=f"ParameterResolutionError: {exc}",
            )

        action = self._actions[step.action]
        last_error: str | None = None
        for attempt in range(1, step.retries + 2):
            context = ActionContext(
                run_id=run_id,
                workflow_id=workflow.id,
                step_id=step.id,
                attempt=attempt,
                inputs=MappingProxyType(dict(inputs)),
                params=MappingProxyType(params),
                outputs=MappingProxyType(output_values),
            )
            try:
                if inspect.iscoroutinefunction(action):
                    value = await action(context)
                else:
                    value = await asyncio.to_thread(action, context)
                output = await value if inspect.isawaitable(value) else value
                output = validate_json_payload(output, f"step '{step.id}' output", MAX_STEP_OUTPUT_BYTES)
                return StepResult(
                    step_id=step.id,
                    status=StepStatus.SUCCESS,
                    attempts=attempt,
                    started_at=started_at,
                    finished_at=utc_now(),
                    output=output,
                )
            except Exception as exc:  # actions define their own expected exception types
                last_error = _format_error(exc)
                if attempt <= step.retries and step.retry_delay_seconds:
                    await asyncio.sleep(step.retry_delay_seconds)

        return StepResult(
            step_id=step.id,
            status=StepStatus.FAILED,
            attempts=step.retries + 1,
            started_at=started_at,
            finished_at=utc_now(),
            error=last_error,
        )


def _ordered_results(workflow: Workflow, results: Mapping[str, StepResult]) -> dict[str, StepResult]:
    return {step.id: results[step.id] for step in workflow.steps}


def _format_error(exc: Exception) -> str:
    message = f"{type(exc).__name__}: {exc}".replace("\r", "\\r").replace("\n", "\\n")
    if len(message) <= MAX_ERROR_LENGTH:
        return message
    suffix = "... [truncated]"
    return message[: MAX_ERROR_LENGTH - len(suffix)] + suffix


def _resolve_value(value: Any, inputs: Mapping[str, Any], outputs: Mapping[str, Any]) -> Any:
    if isinstance(value, str):
        match = _REFERENCE.fullmatch(value)
        if match is None:
            return value
        scope, path = match.groups()
        if scope == "input":
            return _lookup(inputs, path)
        matches = [
            step_id
            for step_id in outputs
            if path == f"{step_id}.output" or path.startswith(f"{step_id}.output.")
        ]
        if not matches:
            raise WorkflowValidationError(f"reference '{value}' does not name an available step output")
        step_id = max(matches, key=len)
        output_path = path.removeprefix(f"{step_id}.output").removeprefix(".")
        output = outputs[step_id]
        return _lookup(output, output_path) if output_path else output
    if isinstance(value, list):
        return [_resolve_value(item, inputs, outputs) for item in value]
    if isinstance(value, dict):
        return {key: _resolve_value(item, inputs, outputs) for key, item in value.items()}
    return value


def _lookup(value: Any, path: str) -> Any:
    if not path:
        return value
    current = value
    for segment in path.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            raise KeyError(f"missing reference segment '{segment}'")
        current = current[segment]
    return current
