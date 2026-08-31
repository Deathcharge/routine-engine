from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from routine_engine import (
    ActionContext,
    JsonStore,
    RoutineEngine,
    Step,
    StepResult,
    StepStatus,
    StorageError,
    Workflow,
    WorkflowValidationError,
)
from routine_engine.cli import main
from routine_engine.models import MAX_INPUT_BYTES, MAX_JSON_DEPTH, utc_now, validate_json_payload


def definition() -> Workflow:
    return Workflow.from_dict({"id": "wrap-up", "steps": [{"id": "one", "action": "ok"}]})


def test_direct_workflow_cannot_bypass_validation() -> None:
    engine = RoutineEngine()
    engine.register("ok", lambda _: None)
    with pytest.raises(WorkflowValidationError, match="depend on itself"):
        engine.validate(Workflow("invalid", (Step("one", "ok", needs=("one",)),)))


@pytest.mark.parametrize("inputs", [[], False, 0, "", [("key", "value")]])
def test_inputs_must_be_mapping_even_when_falsy(inputs: Any) -> None:
    engine = RoutineEngine()
    engine.register("ok", lambda _: None)
    with pytest.raises(WorkflowValidationError, match="object"):
        engine.run(definition(), inputs)


def test_aliased_json_still_enforces_depth() -> None:
    shared: Any = [[]]
    nested = shared
    for _ in range(MAX_JSON_DEPTH - 1):
        nested = [nested]
    with pytest.raises(WorkflowValidationError, match="depth"):
        validate_json_payload([nested, shared], "input", MAX_INPUT_BYTES)


def test_active_history_is_never_evicted(tmp_path: Path) -> None:
    store = JsonStore(tmp_path / "state.json", history_limit=1)
    store.begin_run("first", definition(), {}, utc_now().isoformat())
    with pytest.raises(StorageError, match="active"):
        store.begin_run("second", definition(), {}, utc_now().isoformat())
    assert store.get_run("first")["status"] == "running"


def test_duplicate_run_is_not_overwritten(tmp_path: Path) -> None:
    store = JsonStore(tmp_path / "state.json")
    store.begin_run("same", definition(), {}, utc_now().isoformat())
    with pytest.raises(StorageError, match="already exists"):
        store.begin_run("same", definition(), {"changed": True}, utc_now().isoformat())
    assert store.get_run("same")["inputs"] == {}


@pytest.mark.asyncio
async def test_checkpoint_failure_cleans_up_other_async_actions(tmp_path: Path) -> None:
    started, stopped = asyncio.Event(), asyncio.Event()

    class FailingStore(JsonStore):
        def record_step(self, run_id: str, step: StepResult) -> None:
            raise StorageError("disk unavailable")

    async def fast(_: ActionContext) -> None:
        await started.wait()

    async def slow(_: ActionContext) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    engine = RoutineEngine(store=FailingStore(tmp_path / "state.json"))
    engine.register("fast", fast)
    engine.register("slow", slow)
    try:
        with pytest.raises(StorageError, match="disk unavailable"):
            await engine.arun(
                {"id": "cleanup", "steps": [{"id": "one", "action": "fast"}, {"id": "two", "action": "slow"}]}
            )
        assert stopped.is_set()
    finally:
        # Do not leave the baseline's leaked task alive after a failing assertion.
        leaked = [task for task in asyncio.all_tasks() if task.get_name().startswith("routine-engine:")]
        for task in leaked:
            task.cancel()
        await asyncio.gather(*leaked, return_exceptions=True)


@pytest.mark.parametrize("bad_runs", [[None], [{"run_id": "x"}], [True]])
def test_corrupt_records_fail_with_storage_error(tmp_path: Path, bad_runs: Any) -> None:
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"schema_version": 2, "workflows": {}, "runs": bad_runs}), encoding="utf-8")
    with pytest.raises(StorageError):
        JsonStore(path).list_runs()


def test_cli_rejects_deep_json_without_traceback(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "deep.json"
    path.write_text('{"nested":' + "[" * 2000 + "0" + "]" * 2000 + "}", encoding="utf-8")
    assert main(["validate", str(path)]) == 2
    assert "error:" in capsys.readouterr().err


def test_resume_preserves_exhausted_failures(tmp_path: Path) -> None:
    store = JsonStore(tmp_path / "state.json")
    store.begin_run("interrupted", definition(), {}, utc_now().isoformat())
    now = utc_now()
    store.record_step("interrupted", StepResult("one", StepStatus.FAILED, 1, now, now, error="failed"))
    calls: list[str] = []
    engine = RoutineEngine(store=store)
    engine.register("ok", lambda _: calls.append("unexpected"))
    result = engine.resume("interrupted")
    assert not result.succeeded
    assert not calls


def test_resume_rejects_json_type_changes(tmp_path: Path) -> None:
    store = JsonStore(tmp_path / "state.json")
    store.begin_run("types", definition(), {"value": True}, utc_now().isoformat())
    engine = RoutineEngine(store=store)
    engine.register("ok", lambda _: None)
    with pytest.raises(WorkflowValidationError, match="do not match"):
        engine.run(definition(), {"value": 1}, resume_run_id="types")


@pytest.mark.asyncio
async def test_cannot_resume_a_run_still_executing_on_same_store(tmp_path: Path) -> None:
    store = JsonStore(tmp_path / "state.json")
    engine = RoutineEngine(store=store)
    started, finish = asyncio.Event(), asyncio.Event()

    async def action(_: ActionContext) -> None:
        started.set()
        await finish.wait()

    engine.register("ok", action)
    task = asyncio.create_task(engine.arun(definition()))
    try:
        await started.wait()
        run_id = store.list_runs()[0]["run_id"]
        with pytest.raises(StorageError, match="already executing"):
            await engine.aresume(run_id)
    finally:
        finish.set()
        await task


def test_resume_rejects_dependency_inconsistent_checkpoint(tmp_path: Path) -> None:
    store = JsonStore(tmp_path / "state.json")
    workflow = Workflow.from_dict(
        {
            "id": "dependencies",
            "steps": [
                {"id": "first", "action": "ok"},
                {"id": "second", "action": "ok", "needs": ["first"]},
            ],
        }
    )
    store.begin_run("inconsistent", workflow, {}, utc_now().isoformat())
    now = utc_now()
    store.record_step("inconsistent", StepResult("second", StepStatus.SUCCESS, 1, now, now))
    engine = RoutineEngine(store=store)
    engine.register("ok", lambda _: None)
    with pytest.raises(WorkflowValidationError, match="invalid checkpoint"):
        engine.resume("inconsistent")


def test_retry_and_sibling_contexts_are_detached() -> None:
    engine = RoutineEngine()
    engine.register("produce", lambda _: {"items": [1]})

    def mutate(context: ActionContext) -> dict[str, Any]:
        assert context.params["value"] == {"items": [1]}
        context.params["value"]["items"].append(2)
        context.inputs["items"].append(3)
        if context.attempt == 1:
            raise RuntimeError("try again")
        return dict(context.params)

    engine.register("mutate", mutate)
    result = engine.run(
        {
            "id": "isolation",
            "steps": [
                {"id": "first", "action": "produce"},
                {
                    "id": "second",
                    "action": "mutate",
                    "needs": ["first"],
                    "retries": 1,
                    "with": {"value": "  {{ steps.first.output }}  "},
                },
            ],
        },
        {"items": [1]},
    )
    assert result.succeeded
    assert result.steps["first"].output == {"items": [1]}


def test_bad_exception_string_remains_a_step_failure() -> None:
    class BadError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("broken formatter")

    def action(_: ActionContext) -> None:
        raise BadError()

    engine = RoutineEngine()
    engine.register("ok", action)
    result = engine.run(definition())
    assert result.steps["one"].error == "BadError: exception message unavailable"


@pytest.mark.parametrize("version", [True, 1.0, "1", 2])
def test_schema_version_is_strict_integer(version: Any) -> None:
    with pytest.raises(WorkflowValidationError, match="schema_version"):
        Workflow.from_dict({**definition().to_dict(), "schema_version": version})


def test_cli_bounds_raw_files_and_inline_inputs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "oversized.json"
    path.write_text(" " * (MAX_INPUT_BYTES + 1), encoding="utf-8")
    assert main(["validate", str(path)]) == 2
    assert "exceeds" in capsys.readouterr().err
    path.write_text(
        json.dumps({"id": "input", "steps": [{"id": "one", "action": "identity"}]}), encoding="utf-8"
    )
    assert main(["run", str(path), "--input", " " * (MAX_INPUT_BYTES + 1)]) == 2
    assert "exceeds" in capsys.readouterr().err


@pytest.mark.parametrize("kind", ["size", "depth", "nan", "version", "duplicate"])
def test_store_rejects_malformed_or_oversized_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    path = tmp_path / "state.json"
    store = JsonStore(path)
    store.begin_run("one", definition(), {}, utc_now().isoformat())
    raw = store.snapshot()
    if kind == "size":
        monkeypatch.setattr("routine_engine.storage.MAX_STORE_BYTES", 8)
    elif kind == "depth":
        path.write_text("[" * 2000 + "0" + "]" * 2000, encoding="utf-8")
    elif kind == "nan":
        path.write_text('{"value":NaN}', encoding="utf-8")
    else:
        if kind == "version":
            raw["schema_version"] = True
        else:
            raw["runs"].append(raw["runs"][0])
        path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(StorageError):
        store.snapshot()


def test_actual_process_exit_recovers_successful_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "crashed.json"
    workflow = {
        "id": "process-crash",
        "steps": [
            {"id": "first", "action": "first"},
            {"id": "second", "action": "second", "needs": ["first"]},
        ],
    }
    code = (
        "import os, sys, json\n"
        "from routine_engine import RoutineEngine, JsonStore\n"
        "engine = RoutineEngine(store=JsonStore(sys.argv[1]))\n"
        "engine.register('first', lambda _: {'saved': True})\n"
        "engine.register('second', lambda _: os._exit(23))\n"
        "engine.run(json.loads(sys.argv[2]))\n"
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
    crashed = subprocess.run(
        [sys.executable, "-c", code, str(path), json.dumps(workflow)],
        cwd=tmp_path,
        env=environment,
        check=False,
        timeout=30,
    )
    assert crashed.returncode == 23
    store = JsonStore(path)
    record = store.list_runs()[0]
    assert record["steps"]["first"]["output"] == {"saved": True}
    engine = RoutineEngine(store=store)

    def never_repeat(_: ActionContext) -> None:
        pytest.fail("checkpointed action repeated after process exit")

    engine.register("first", never_repeat)
    engine.register("second", lambda context: context.outputs["first"])
    result = engine.resume(record["run_id"])
    assert result.succeeded
    assert result.steps["first"].started_at.isoformat() == record["steps"]["first"]["started_at"]
