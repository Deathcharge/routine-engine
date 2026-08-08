"""Validated workflow definitions and immutable execution results."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping

from .errors import WorkflowValidationError

MAX_STEPS = 256
MAX_RETRIES = 10
MAX_RETRY_DELAY_SECONDS = 60.0
MAX_CONCURRENCY = 32
WORKFLOW_SCHEMA_VERSION = 1
MAX_WORKFLOW_BYTES = 1_048_576
MAX_INPUT_BYTES = 1_048_576
MAX_STEP_OUTPUT_BYTES = 4_194_304
MAX_JSON_DEPTH = 32
MAX_ERROR_LENGTH = 4096
_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
_INPUT_REFERENCE = re.compile(
    r"^\{\{\s*input\.[A-Za-z][A-Za-z0-9_-]*(?:\.[A-Za-z][A-Za-z0-9_-]*)*\s*\}\}$"
)


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


def _require_identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise WorkflowValidationError(
            f"{field_name} must start with a letter and contain only letters, numbers, '.', '_' or '-'"
        )
    return value


def validate_json_payload(value: Any, field_name: str, max_bytes: int) -> Any:
    """Validate, bound, and detach portable JSON data."""

    stack: list[tuple[Any, int]] = [(value, 1)]
    seen: set[int] = set()
    while stack:
        current, depth = stack.pop()
        if isinstance(current, Mapping):
            if depth > MAX_JSON_DEPTH:
                raise WorkflowValidationError(f"{field_name} exceeds the maximum JSON depth of {MAX_JSON_DEPTH}")
            if id(current) in seen:
                continue
            seen.add(id(current))
            if not all(isinstance(key, str) for key in current):
                raise WorkflowValidationError(f"{field_name} must use string object keys")
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, (list, tuple)):
            if depth > MAX_JSON_DEPTH:
                raise WorkflowValidationError(f"{field_name} exceeds the maximum JSON depth of {MAX_JSON_DEPTH}")
            if id(current) in seen:
                continue
            seen.add(id(current))
            stack.extend((item, depth + 1) for item in current)
    try:
        payload = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise WorkflowValidationError(f"{field_name} must be finite, JSON-compatible data") from exc
    size = len(payload.encode("utf-8"))
    if size > max_bytes:
        raise WorkflowValidationError(f"{field_name} exceeds the maximum size of {max_bytes} bytes")
    return json.loads(payload)


class StepStatus(str, Enum):
    """Terminal state of one workflow step."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class RunStatus(str, Enum):
    """Terminal state of a workflow run."""

    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class Step:
    """A single action invocation in a workflow DAG."""

    id: str
    action: str
    needs: tuple[str, ...] = ()
    params: Mapping[str, Any] = field(default_factory=dict)
    retries: int = 0
    retry_delay_seconds: float = 0.0

    @classmethod
    def from_dict(cls, value: Mapping[str, Any], index: int) -> Step:
        if not isinstance(value, Mapping):
            raise WorkflowValidationError(f"steps[{index}] must be an object")

        step_id = _require_identifier(value.get("id"), f"steps[{index}].id")
        action = _require_identifier(value.get("action"), f"steps[{index}].action")
        raw_needs = value.get("needs", [])
        if not isinstance(raw_needs, list) or not all(isinstance(item, str) for item in raw_needs):
            raise WorkflowValidationError(f"steps[{index}].needs must be an array of step IDs")
        if len(raw_needs) != len(set(raw_needs)):
            raise WorkflowValidationError(f"steps[{index}].needs contains duplicates")

        params = value.get("with", {})
        if not isinstance(params, Mapping):
            raise WorkflowValidationError(f"steps[{index}].with must be an object")
        detached_params = validate_json_payload(params, f"steps[{index}].with", MAX_WORKFLOW_BYTES)

        retries = value.get("retries", 0)
        if isinstance(retries, bool) or not isinstance(retries, int) or not 0 <= retries <= MAX_RETRIES:
            raise WorkflowValidationError(f"steps[{index}].retries must be an integer from 0 to {MAX_RETRIES}")

        delay = value.get("retry_delay_seconds", 0.0)
        if isinstance(delay, bool) or not isinstance(delay, (int, float)):
            raise WorkflowValidationError(f"steps[{index}].retry_delay_seconds must be a number")
        delay = float(delay)
        if not 0.0 <= delay <= MAX_RETRY_DELAY_SECONDS:
            raise WorkflowValidationError(
                f"steps[{index}].retry_delay_seconds must be from 0 to {MAX_RETRY_DELAY_SECONDS:g}"
            )

        return cls(
            id=step_id,
            action=action,
            needs=tuple(raw_needs),
            params=MappingProxyType(detached_params),
            retries=retries,
            retry_delay_seconds=delay,
        )

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"id": self.id, "action": self.action}
        if self.needs:
            value["needs"] = list(self.needs)
        if self.params:
            value["with"] = dict(self.params)
        if self.retries:
            value["retries"] = self.retries
        if self.retry_delay_seconds:
            value["retry_delay_seconds"] = self.retry_delay_seconds
        return value


@dataclass(frozen=True)
class Workflow:
    """A validated, acyclic workflow definition."""

    id: str
    steps: tuple[Step, ...]
    schema_version: int = WORKFLOW_SCHEMA_VERSION
    description: str = ""
    max_concurrency: int = 4

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Workflow:
        if not isinstance(value, Mapping):
            raise WorkflowValidationError("workflow must be a JSON object")
        detached = validate_json_payload(dict(value), "workflow", MAX_WORKFLOW_BYTES)
        schema_version = detached.get("schema_version", WORKFLOW_SCHEMA_VERSION)
        if isinstance(schema_version, bool) or schema_version != WORKFLOW_SCHEMA_VERSION:
            raise WorkflowValidationError(
                f"schema_version must be the integer {WORKFLOW_SCHEMA_VERSION}"
            )
        value = detached
        workflow_id = _require_identifier(value.get("id"), "id")
        description = value.get("description", "")
        if not isinstance(description, str) or len(description) > 1000:
            raise WorkflowValidationError("description must be a string of at most 1000 characters")

        raw_steps = value.get("steps")
        if not isinstance(raw_steps, list) or not raw_steps:
            raise WorkflowValidationError("steps must be a non-empty array")
        if len(raw_steps) > MAX_STEPS:
            raise WorkflowValidationError(f"a workflow may contain at most {MAX_STEPS} steps")
        steps = tuple(Step.from_dict(item, index) for index, item in enumerate(raw_steps))

        ids = {step.id for step in steps}
        if len(ids) != len(steps):
            raise WorkflowValidationError("step IDs must be unique")
        for step in steps:
            if step.id in step.needs:
                raise WorkflowValidationError(f"step '{step.id}' cannot depend on itself")
            unknown = set(step.needs) - ids
            if unknown:
                raise WorkflowValidationError(
                    f"step '{step.id}' depends on unknown step(s): {', '.join(sorted(unknown))}"
                )
            referenced = _referenced_steps(step.params, ids)
            undeclared = referenced - set(step.needs)
            if undeclared:
                raise WorkflowValidationError(
                    f"step '{step.id}' must list referenced step(s) in needs: {', '.join(sorted(undeclared))}"
                )
        _validate_acyclic(steps)

        max_concurrency = value.get("max_concurrency", 4)
        if (
            isinstance(max_concurrency, bool)
            or not isinstance(max_concurrency, int)
            or not 1 <= max_concurrency <= MAX_CONCURRENCY
        ):
            raise WorkflowValidationError(f"max_concurrency must be an integer from 1 to {MAX_CONCURRENCY}")

        return cls(
            id=workflow_id,
            steps=steps,
            schema_version=schema_version,
            description=description,
            max_concurrency=max_concurrency,
        )

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "schema_version": self.schema_version,
            "id": self.id,
            "steps": [step.to_dict() for step in self.steps],
        }
        if self.description:
            value["description"] = self.description
        if self.max_concurrency != 4:
            value["max_concurrency"] = self.max_concurrency
        return value


def _validate_acyclic(steps: tuple[Step, ...]) -> None:
    dependencies = {step.id: set(step.needs) for step in steps}
    complete: set[str] = set()
    while len(complete) < len(steps):
        ready = {step_id for step_id, needs in dependencies.items() if step_id not in complete and needs <= complete}
        if not ready:
            blocked = ", ".join(sorted(set(dependencies) - complete))
            raise WorkflowValidationError(f"workflow contains a dependency cycle involving: {blocked}")
        complete.update(ready)


def _referenced_steps(value: Any, step_ids: set[str]) -> set[str]:
    if isinstance(value, str):
        stripped = value.strip()
        if "{{" not in stripped and "}}" not in stripped:
            return set()
        if _INPUT_REFERENCE.fullmatch(stripped):
            return set()
        if stripped.startswith("{{") and stripped.endswith("}}"):
            expression = stripped[2:-2].strip()
            if expression.startswith("steps."):
                path = expression.removeprefix("steps.")
                matches = {
                    step_id
                    for step_id in step_ids
                    if path == f"{step_id}.output" or path.startswith(f"{step_id}.output.")
                }
                if matches:
                    return {max(matches, key=len)}
        raise WorkflowValidationError(f"invalid reference syntax: '{value}'")
    if isinstance(value, list):
        return set().union(*(_referenced_steps(item, step_ids) for item in value), set())
    if isinstance(value, Mapping):
        return set().union(*(_referenced_steps(item, step_ids) for item in value.values()), set())
    return set()


@dataclass(frozen=True)
class ActionContext:
    """Inputs available to a registered action."""

    run_id: str
    workflow_id: str
    step_id: str
    attempt: int
    inputs: Mapping[str, Any]
    params: Mapping[str, Any]
    outputs: Mapping[str, Any]


@dataclass(frozen=True)
class PlanStep:
    """One registered action in an execution plan."""

    id: str
    action: str
    needs: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "action": self.action, "needs": list(self.needs)}


@dataclass(frozen=True)
class ExecutionPlan:
    """A deterministic, definition-ordered view of executable DAG layers."""

    workflow_id: str
    schema_version: int
    max_concurrency: int
    layers: tuple[tuple[PlanStep, ...], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "schema_version": self.schema_version,
            "max_concurrency": self.max_concurrency,
            "layers": [[step.to_dict() for step in layer] for layer in self.layers],
        }


@dataclass(frozen=True)
class StepResult:
    """The auditable result of one step."""

    step_id: str
    status: StepStatus
    attempts: int
    started_at: datetime
    finished_at: datetime
    output: Any = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "status": self.status.value,
            "attempts": self.attempts,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "output": self.output,
            "error": self.error,
        }


@dataclass(frozen=True)
class RunResult:
    """The terminal result of a complete workflow run."""

    run_id: str
    workflow_id: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime
    steps: Mapping[str, StepResult]

    @property
    def succeeded(self) -> bool:
        return self.status is RunStatus.SUCCESS

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "steps": {step_id: result.to_dict() for step_id, result in self.steps.items()},
        }
