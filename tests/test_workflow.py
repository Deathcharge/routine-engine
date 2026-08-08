from __future__ import annotations

from typing import Any

import pytest

from routine_engine import RoutineEngine, Workflow, WorkflowValidationError
from routine_engine.models import MAX_CONCURRENCY, MAX_JSON_DEPTH, MAX_STEPS


def test_workflow_round_trip_preserves_public_shape(workflow_factory: Any) -> None:
    raw = workflow_factory(
        description="A real workflow",
        max_concurrency=2,
        steps=[
            {
                "id": "first",
                "action": "echo",
                "with": {"value": 3},
                "retries": 2,
                "retry_delay_seconds": 0.25,
            },
            {"id": "second", "action": "merge", "needs": ["first"]},
        ],
    )

    definition = Workflow.from_dict(raw)

    assert definition.to_dict() == {"schema_version": 1, **raw}


def test_schema_version_and_json_shape_are_pinned() -> None:
    raw = {"id": "versioned", "steps": [{"id": "one", "action": "echo"}]}
    assert Workflow.from_dict(raw).schema_version == 1
    with pytest.raises(WorkflowValidationError, match="schema_version"):
        Workflow.from_dict({"schema_version": 2, **raw})
    with pytest.raises(WorkflowValidationError, match="unknown field"):
        Workflow.from_dict({**raw, "surprise": True})
    with pytest.raises(WorkflowValidationError, match="unknown field"):
        Workflow.from_dict({"id": "step-field", "steps": [{"id": "one", "action": "echo", "extra": 1}]})
    with pytest.raises(WorkflowValidationError, match="string object keys"):
        Workflow.from_dict({"id": "keys", "steps": [{"id": "one", "action": "echo", "with": {1: 2}}]})

    nested: Any = "leaf"
    for _ in range(MAX_JSON_DEPTH + 1):
        nested = [nested]
    with pytest.raises(WorkflowValidationError, match="maximum JSON depth"):
        Workflow.from_dict(
            {"id": "deep", "steps": [{"id": "one", "action": "echo", "with": {"value": nested}}]}
        )


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ([], "JSON object"),
        ({"id": "bad id", "steps": [{}]}, "id must start"),
        ({"id": "ok"}, "non-empty array"),
        ({"id": "ok", "steps": []}, "non-empty array"),
        ({"id": "ok", "steps": [None]}, "must be an object"),
        ({"id": "ok", "steps": [{"id": "x", "action": "echo", "needs": "y"}]}, "array"),
        (
            {"id": "ok", "steps": [{"id": "x", "action": "echo", "needs": ["y", "y"]}]},
            "duplicates",
        ),
        ({"id": "ok", "steps": [{"id": "x", "action": "echo", "with": []}]}, "object"),
        ({"id": "ok", "steps": [{"id": "x", "action": "echo", "retries": True}]}, "integer"),
        ({"id": "ok", "steps": [{"id": "x", "action": "echo", "retries": 11}]}, "from 0"),
        (
            {"id": "ok", "steps": [{"id": "x", "action": "echo", "retry_delay_seconds": "1"}]},
            "number",
        ),
        (
            {"id": "ok", "steps": [{"id": "x", "action": "echo", "retry_delay_seconds": 61}]},
            "must be from",
        ),
        (
            {"id": "ok", "steps": [{"id": "x", "action": "echo"}], "description": 3},
            "description",
        ),
        (
            {"id": "ok", "steps": [{"id": "x", "action": "echo"}], "max_concurrency": 0},
            "max_concurrency",
        ),
    ],
)
def test_rejects_malformed_definitions(raw: Any, message: str) -> None:
    with pytest.raises(WorkflowValidationError, match=message):
        Workflow.from_dict(raw)


def test_rejects_duplicate_unknown_self_and_cyclic_dependencies() -> None:
    cases = [
        (
            {"id": "w", "steps": [{"id": "a", "action": "echo"}, {"id": "a", "action": "echo"}]},
            "unique",
        ),
        ({"id": "w", "steps": [{"id": "a", "action": "echo", "needs": ["missing"]}]}, "unknown"),
        ({"id": "w", "steps": [{"id": "a", "action": "echo", "needs": ["a"]}]}, "itself"),
        (
            {
                "id": "w",
                "steps": [
                    {"id": "a", "action": "echo", "needs": ["b"]},
                    {"id": "b", "action": "echo", "needs": ["a"]},
                ],
            },
            "cycle",
        ),
    ]
    for raw, message in cases:
        with pytest.raises(WorkflowValidationError, match=message):
            Workflow.from_dict(raw)


def test_rejects_non_json_params_and_resource_excess() -> None:
    with pytest.raises(WorkflowValidationError, match="JSON-compatible"):
        Workflow.from_dict({"id": "w", "steps": [{"id": "a", "action": "echo", "with": {"x": {1}}}]})

    too_many = [{"id": f"s{index}", "action": "echo"} for index in range(MAX_STEPS + 1)]
    with pytest.raises(WorkflowValidationError, match=str(MAX_STEPS)):
        Workflow.from_dict({"id": "w", "steps": too_many})

    with pytest.raises(WorkflowValidationError, match=str(MAX_CONCURRENCY)):
        Workflow.from_dict(
            {"id": "w", "steps": [{"id": "a", "action": "echo"}], "max_concurrency": MAX_CONCURRENCY + 1}
        )


def test_engine_requires_registered_actions(engine: RoutineEngine, workflow_factory: Any) -> None:
    with pytest.raises(WorkflowValidationError, match=r"unregistered action.*missing"):
        engine.validate(workflow_factory(steps=[{"id": "x", "action": "missing"}]))


def test_references_require_valid_syntax_and_declared_dependencies() -> None:
    with pytest.raises(WorkflowValidationError, match="must list referenced"):
        Workflow.from_dict(
            {
                "id": "w",
                "steps": [
                    {"id": "source", "action": "echo"},
                    {
                        "id": "consumer",
                        "action": "echo",
                        "with": {"value": "{{ steps.source.output }}"},
                    },
                ],
            }
        )

    with pytest.raises(WorkflowValidationError, match="invalid reference syntax"):
        Workflow.from_dict(
            {
                "id": "w",
                "steps": [{"id": "source", "action": "echo", "with": {"value": "{{ input }}"}}],
            }
        )
