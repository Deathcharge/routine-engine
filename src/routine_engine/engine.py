"""Bounded, dependency-aware workflow execution."""

from __future__ import annotations

import asyncio
import inspect
import json
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from types import MappingProxyType
from typing import Any

from .errors import ActionRegistrationError, WorkflowValidationError
from .models import (
    ActionContext,
    RunResult,
    RunStatus,
    Step,
    StepResult,
    StepStatus,
    Workflow,
    utc_now,
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

    def run(self, workflow: Workflow | Mapping[str, Any], inputs: Mapping[str, Any] | None = None) -> RunResult:
        """Run a workflow from synchronous code."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.arun(workflow, inputs))
        raise RuntimeError("RoutineEngine.run() cannot be called inside an event loop; use 'await arun(...)'")

    async def arun(
        self,
        workflow: Workflow | Mapping[str, Any],
        inputs: Mapping[str, Any] | None = None,
    ) -> RunResult:
        """Run a workflow and return exact per-step terminal states."""

        definition = self.validate(workflow)
        try:
            run_inputs = json.loads(json.dumps(dict(inputs or {}), allow_nan=False))
        except (TypeError, ValueError, OverflowError) as exc:
            raise WorkflowValidationError("run inputs must be finite, JSON-compatible data") from exc
        run_id = uuid.uuid4().hex
        started_at = utc_now()
        pending = {step.id: step for step in definition.steps}
        results: dict[str, StepResult] = {}
        running: dict[str, asyncio.Task[StepResult]] = {}

        if self._store is not None:
            self._store.save_workflow(definition)

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
                self._store.append_run(cancelled)
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
            self._store.append_run(result)
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
                value = action(context)
                output = await value if inspect.isawaitable(value) else value
                return StepResult(
                    step_id=step.id,
                    status=StepStatus.SUCCESS,
                    attempts=attempt,
                    started_at=started_at,
                    finished_at=utc_now(),
                    output=output,
                )
            except Exception as exc:  # actions define their own expected exception types
                last_error = f"{type(exc).__name__}: {exc}"
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
