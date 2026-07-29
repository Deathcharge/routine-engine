from __future__ import annotations

import asyncio

import pytest

from routine_engine import (
    ActionContext,
    ActionRegistrationError,
    RoutineEngine,
    RunStatus,
    StepStatus,
    WorkflowValidationError,
)


def test_primary_journey_resolves_input_and_prior_outputs(engine: RoutineEngine) -> None:
    workflow = {
        "id": "welcome",
        "steps": [
            {"id": "name", "action": "echo", "with": {"value": "{{ input.user.name }}"}},
            {
                "id": "result",
                "action": "merge",
                "needs": ["name"],
                "with": {"message": "{{ steps.name.output }}", "source": "local"},
            },
        ],
    }

    result = engine.run(workflow, {"user": {"name": "Ada"}})

    assert result.status is RunStatus.SUCCESS
    assert result.succeeded
    assert result.steps["name"].output == "Ada"
    assert result.steps["result"].output == {"message": "Ada", "source": "local"}
    assert list(result.steps) == ["name", "result"]
    assert result.to_dict()["status"] == "success"


def test_dotted_step_id_resolves_without_ambiguity(engine: RoutineEngine) -> None:
    result = engine.run(
        {
            "id": "dotted",
            "steps": [
                {"id": "source.v1", "action": "echo", "with": {"value": {"name": "Ada"}}},
                {
                    "id": "consumer",
                    "action": "echo",
                    "needs": ["source.v1"],
                    "with": {"value": "{{ steps.source.v1.output.name }}"},
                },
            ],
        }
    )

    assert result.steps["consumer"].output == "Ada"


def test_inputs_must_be_json_and_are_detached(engine: RoutineEngine) -> None:
    inputs = {"nested": {"value": "original"}}

    def mutate(context: ActionContext) -> str:
        nested = context.inputs["nested"]
        assert isinstance(nested, dict)
        nested["value"] = "changed"
        return nested["value"]

    engine.register("mutate", mutate)
    result = engine.run({"id": "detach", "steps": [{"id": "x", "action": "mutate"}]}, inputs)
    assert result.succeeded
    assert inputs == {"nested": {"value": "original"}}

    with pytest.raises(WorkflowValidationError, match="JSON-compatible"):
        engine.run({"id": "bad-input", "steps": [{"id": "x", "action": "echo"}]}, {"bad": {1}})


def test_action_context_exposes_only_declared_dependency_outputs() -> None:
    engine = RoutineEngine()
    engine.register("value", lambda context: context.step_id)
    engine.register("keys", lambda context: sorted(context.outputs))

    result = engine.run(
        {
            "id": "outputs",
            "max_concurrency": 1,
            "steps": [
                {"id": "unrelated", "action": "value"},
                {"id": "source", "action": "value"},
                {"id": "consumer", "action": "keys", "needs": ["source"]},
            ],
        }
    )

    assert result.steps["consumer"].output == ["source"]


@pytest.mark.asyncio
async def test_async_actions_run_in_parallel() -> None:
    engine = RoutineEngine()
    both_started = asyncio.Event()
    starts = 0

    async def rendezvous(context: ActionContext) -> str:
        nonlocal starts
        starts += 1
        if starts == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=0.5)
        return context.step_id

    engine.register("rendezvous", rendezvous)
    result = await engine.arun(
        {
            "id": "parallel",
            "max_concurrency": 2,
            "steps": [
                {"id": "left", "action": "rendezvous"},
                {"id": "right", "action": "rendezvous"},
            ],
        }
    )

    assert result.status is RunStatus.SUCCESS
    assert {item.output for item in result.steps.values()} == {"left", "right"}


@pytest.mark.asyncio
async def test_retry_then_success_records_attempt_count() -> None:
    engine = RoutineEngine()

    def flaky(context: ActionContext) -> int:
        if context.attempt < 3:
            raise RuntimeError("not yet")
        return context.attempt

    engine.register("flaky", flaky)
    result = await engine.arun(
        {
            "id": "retry",
            "steps": [{"id": "eventual", "action": "flaky", "retries": 2, "retry_delay_seconds": 0.001}],
        }
    )

    assert result.steps["eventual"].status is StepStatus.SUCCESS
    assert result.steps["eventual"].attempts == 3


def test_failure_skips_dependents_but_independent_branch_completes() -> None:
    engine = RoutineEngine()

    def fail(_: ActionContext) -> None:
        raise LookupError("boom")

    engine.register("fail", fail)
    engine.register("ok", lambda _: "done")
    result = engine.run(
        {
            "id": "failure",
            "steps": [
                {"id": "bad", "action": "fail", "retries": 1},
                {"id": "blocked", "action": "ok", "needs": ["bad"]},
                {"id": "independent", "action": "ok"},
            ],
        }
    )

    assert result.status is RunStatus.FAILED
    assert not result.succeeded
    assert result.steps["bad"].attempts == 2
    assert result.steps["bad"].error == "LookupError: boom"
    assert result.steps["blocked"].status is StepStatus.SKIPPED
    assert result.steps["independent"].status is StepStatus.SUCCESS


def test_bad_reference_is_a_step_failure(engine: RoutineEngine) -> None:
    result = engine.run(
        {
            "id": "reference",
            "steps": [{"id": "step", "action": "echo", "with": {"value": "{{ input.missing }}"}}],
        }
    )

    assert result.status is RunStatus.FAILED
    assert result.steps["step"].attempts == 0
    assert result.steps["step"].error is not None
    assert result.steps["step"].error.startswith("ParameterResolutionError")


def test_registration_guards_names_duplicates_and_callables() -> None:
    engine = RoutineEngine()
    engine.register("valid", lambda _: None)
    with pytest.raises(ActionRegistrationError, match="already"):
        engine.register("valid", lambda _: None)
    with pytest.raises(ActionRegistrationError, match="identifier"):
        engine.register("bad name", lambda _: None)
    with pytest.raises(ActionRegistrationError, match="callable"):
        engine.register("other", None)  # type: ignore[arg-type]
    engine.register("valid", lambda _: "replaced", replace=True)
    assert engine.actions == ("valid",)


@pytest.mark.asyncio
async def test_sync_entrypoint_rejects_running_event_loop(engine: RoutineEngine) -> None:
    with pytest.raises(RuntimeError, match="await arun"):
        engine.run({"id": "w", "steps": [{"id": "x", "action": "echo"}]})


@pytest.mark.asyncio
async def test_cancellation_propagates_and_cancels_actions() -> None:
    engine = RoutineEngine()
    started = asyncio.Event()

    async def wait_forever(_: ActionContext) -> None:
        started.set()
        await asyncio.Event().wait()

    engine.register("wait", wait_forever)
    task = asyncio.create_task(engine.arun({"id": "cancel", "steps": [{"id": "x", "action": "wait"}]}))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
