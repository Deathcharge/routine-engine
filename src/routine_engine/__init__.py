"""Samsarix Routine Engine public API."""

from .engine import Action, RoutineEngine
from .errors import ActionRegistrationError, RoutineEngineError, StorageError, WorkflowValidationError
from .models import (
    ActionContext,
    ExecutionPlan,
    PlanStep,
    RunResult,
    RunStatus,
    Step,
    StepResult,
    StepStatus,
    Workflow,
)
from .storage import JsonStore

__all__ = [
    "Action",
    "ActionContext",
    "ActionRegistrationError",
    "ExecutionPlan",
    "JsonStore",
    "PlanStep",
    "RoutineEngine",
    "RoutineEngineError",
    "RunResult",
    "RunStatus",
    "Step",
    "StepResult",
    "StepStatus",
    "StorageError",
    "Workflow",
    "WorkflowValidationError",
]

__version__ = "0.1.0"
