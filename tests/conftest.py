from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from routine_engine import ActionContext, RoutineEngine


@pytest.fixture
def engine() -> RoutineEngine:
    instance = RoutineEngine()
    instance.register("echo", lambda context: context.params.get("value", dict(context.inputs)))
    instance.register("merge", lambda context: dict(context.params))
    return instance


@pytest.fixture
def workflow_factory() -> Callable[..., dict[str, Any]]:
    def make(**overrides: Any) -> dict[str, Any]:
        workflow: dict[str, Any] = {
            "id": "example",
            "steps": [{"id": "first", "action": "echo"}],
        }
        workflow.update(overrides)
        return workflow

    return make


def output(context: ActionContext) -> Any:
    return context.params.get("value")
