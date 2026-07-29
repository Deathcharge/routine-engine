from __future__ import annotations

from typing import Any

import pytest

from routine_engine import ActionContext, RoutineEngine, RunStatus
from routine_engine.builtins import format_text, identity, merge, register_builtin_actions


def _context(params: dict[str, Any], inputs: dict[str, Any] | None = None) -> ActionContext:
    return ActionContext(
        run_id="run",
        workflow_id="workflow",
        step_id="step",
        attempt=1,
        inputs=inputs or {},
        params=params,
        outputs={},
    )


def test_builtin_actions_are_small_and_side_effect_free() -> None:
    assert identity(_context({"value": 3})) == 3
    assert identity(_context({}, {"name": "Ada"})) == {"name": "Ada"}
    assert merge(_context({"a": 1})) == {"a": 1}
    assert format_text(_context({"template": "Hi {name}", "values": {"name": "Ada"}})) == "Hi Ada"


def test_format_builtin_validates_parameters() -> None:
    with pytest.raises(ValueError, match="template"):
        format_text(_context({}))
    with pytest.raises(ValueError, match="values"):
        format_text(_context({"template": "x", "values": []}))


def test_builtin_registration_supports_complete_demo_journey() -> None:
    engine = RoutineEngine()
    register_builtin_actions(engine)
    result = engine.run(
        {
            "id": "demo",
            "steps": [
                {"id": "name", "action": "identity", "with": {"value": "{{ input.name }}"}},
                {
                    "id": "greeting",
                    "action": "format",
                    "needs": ["name"],
                    "with": {
                        "template": "Hello, {name}!",
                        "values": {"name": "{{ steps.name.output }}"},
                    },
                },
            ],
        },
        {"name": "Samsarix"},
    )

    assert result.status is RunStatus.SUCCESS
    assert result.steps["greeting"].output == "Hello, Samsarix!"
