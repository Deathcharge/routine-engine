from __future__ import annotations

import json
from pathlib import Path

import pytest

from routine_engine import ActionContext, JsonStore, RoutineEngine, RunStatus, StorageError, Workflow
from routine_engine.cli import main


def test_json_store_persists_definition_and_bounded_history(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "state.json"
    engine = RoutineEngine(store=JsonStore(path, history_limit=2))
    engine.register("ok", lambda _: {"saved": True})
    workflow = {"id": "stored", "steps": [{"id": "one", "action": "ok"}]}

    for _ in range(3):
        engine.run(workflow)

    snapshot = JsonStore(path).snapshot()
    assert snapshot["schema_version"] == 2
    assert snapshot["workflows"]["stored"] == {"schema_version": 1, **workflow}
    assert len(snapshot["runs"]) == 2
    assert all(run["status"] == "success" for run in snapshot["runs"])


def test_json_store_rejects_corrupt_schema_and_non_json_output(tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("not json", encoding="utf-8")
    with pytest.raises(StorageError, match="cannot read"):
        JsonStore(corrupt).snapshot()

    wrong = tmp_path / "wrong.json"
    wrong.write_text('{"schema_version": 3}', encoding="utf-8")
    with pytest.raises(StorageError, match="schema version 2"):
        JsonStore(wrong).snapshot()

    engine = RoutineEngine(store=JsonStore(tmp_path / "state.json"))
    engine.register("set", lambda _: {1})
    result = engine.run({"id": "bad", "steps": [{"id": "one", "action": "set"}]})
    assert result.status is RunStatus.FAILED
    assert "JSON-compatible" in (result.steps["one"].error or "")


def test_store_constructor_bounds_history(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="history_limit"):
        JsonStore(tmp_path / "state.json", history_limit=0)


def test_store_reads_version_one_state_as_version_two(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text('{"schema_version":1,"workflows":{},"runs":[]}', encoding="utf-8")
    assert JsonStore(path).snapshot()["schema_version"] == 2


def test_store_supports_explicit_definition_and_terminal_result_writes(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = JsonStore(path)
    workflow = Workflow.from_dict({"id": "manual", "steps": [{"id": "one", "action": "ok"}]})
    store.save_workflow(workflow)

    engine = RoutineEngine()
    engine.register("ok", lambda _: True)
    result = engine.run(workflow)
    store.append_run(result)

    assert store.get_run(result.run_id)["status"] == "success"
    with pytest.raises(StorageError, match="not found"):
        store.get_run("missing")


def test_cli_validate_run_demo_and_version(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workflow_path = tmp_path / "workflow.json"
    workflow_path.write_text(
        json.dumps(
            {
                "id": "cli",
                "steps": [
                    {"id": "name", "action": "identity", "with": {"value": "{{ input.name }}"}},
                    {
                        "id": "out",
                        "action": "merge",
                        "needs": ["name"],
                        "with": {"name": "{{ steps.name.output }}"},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    assert main(["validate", str(workflow_path)]) == 0
    assert "valid: cli (2 steps)" in capsys.readouterr().out

    assert main(["plan", str(workflow_path)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert [[step["id"] for step in layer] for layer in plan["layers"]] == [["name"], ["out"]]

    state = tmp_path / "state.json"
    assert main(["run", str(workflow_path), "--input", '{"name":"Ada"}', "--state", str(state)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["steps"]["out"]["output"] == {"name": "Ada"}
    assert state.exists()

    assert main(["history", "--state", str(state)]) == 0
    history = json.loads(capsys.readouterr().out)
    assert history[0]["workflow_id"] == "cli"
    assert main(["show", history[0]["run_id"], "--state", str(state)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "success"

    assert main(["demo", "--name", "Sam"]) == 0
    assert "Hello, Sam!" in capsys.readouterr().out

    with pytest.raises(SystemExit) as raised:
        main(["--version"])
    assert raised.value.code == 0


def test_bundled_schema_and_release_consumer(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = Path(__file__).parents[1]
    monkeypatch.syspath_prepend(str(repository))
    source_schema = json.loads(
        (repository / "schemas" / "workflow-v1.schema.json").read_text(encoding="utf-8")
    )
    bundled_schema = json.loads(
        (repository / "src" / "routine_engine" / "schemas" / "workflow-v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert bundled_schema == source_schema
    assert main(["schema"]) == 0
    assert json.loads(capsys.readouterr().out)["properties"]["schema_version"]["const"] == 1

    workflow = repository / "examples" / "release-readiness.json"
    state = tmp_path / "release-state.json"
    assert main(["validate", str(workflow), "--plugin", "examples.release_actions"]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "run",
                str(workflow),
                "--plugin",
                "examples.release_actions",
                "--state",
                str(state),
            ]
        )
        == 0
    )
    evidence = json.loads(capsys.readouterr().out)["steps"]["evidence"]["output"]
    assert len(evidence["readme"]["sha256"]) == 64


def test_cli_reports_bad_input_and_bad_plugin(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workflow_path = tmp_path / "workflow.json"
    workflow_path.write_text('{"id":"w","steps":[{"id":"x","action":"identity"}]}', encoding="utf-8")

    assert main(["run", str(workflow_path), "--input", "[]"]) == 2
    assert "JSON object" in capsys.readouterr().err
    assert main(["validate", str(workflow_path), "--plugin", "definitely_missing_plugin"]) == 2
    assert "cannot import plugin" in capsys.readouterr().err


def test_cli_loads_explicit_plugin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = tmp_path / "sample_plugin.py"
    plugin.write_text(
        "def register(engine):\n    engine.register('custom', lambda context: context.inputs['value'])\n",
        encoding="utf-8",
    )
    workflow = tmp_path / "workflow.json"
    workflow.write_text('{"id":"w","steps":[{"id":"x","action":"custom"}]}', encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    assert main(["validate", str(workflow), "--plugin", "sample_plugin"]) == 0


def test_cancelled_run_is_persisted(tmp_path: Path) -> None:
    import asyncio

    async def scenario() -> None:
        engine = RoutineEngine(store=JsonStore(tmp_path / "state.json"))
        started = asyncio.Event()

        async def block(_: ActionContext) -> None:
            started.set()
            await asyncio.Event().wait()

        engine.register("block", block)
        task = asyncio.create_task(engine.arun({"id": "cancel", "steps": [{"id": "x", "action": "block"}]}))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert JsonStore(tmp_path / "state.json").snapshot()["runs"][0]["status"] == "cancelled"


def test_interrupted_run_resumes_only_unfinished_steps(tmp_path: Path) -> None:
    class InterruptingStore(JsonStore):
        def record_step(self, run_id: str, step: object) -> None:
            super().record_step(run_id, step)  # type: ignore[arg-type]
            raise RuntimeError("simulated process interruption")

    path = tmp_path / "state.json"
    calls = {"first": 0, "second": 0}

    def first(_: ActionContext) -> str:
        calls["first"] += 1
        return "checkpoint"

    def second(_: ActionContext) -> str:
        calls["second"] += 1
        return "complete"

    workflow = {
        "id": "resumable",
        "steps": [
            {"id": "first", "action": "first"},
            {"id": "second", "action": "second", "needs": ["first"]},
        ],
    }
    interrupted = RoutineEngine(store=InterruptingStore(path))
    interrupted.register("first", first)
    interrupted.register("second", second)
    with pytest.raises(RuntimeError, match="simulated"):
        interrupted.run(workflow)

    record = JsonStore(path).list_runs()[0]
    assert record["status"] == "running"
    assert record["steps"]["first"]["status"] == "success"

    resumed = RoutineEngine(store=JsonStore(path))
    resumed.register("first", first)
    resumed.register("second", second)
    result = resumed.resume(record["run_id"])

    assert result.status is RunStatus.SUCCESS
    assert calls == {"first": 1, "second": 1}
    assert JsonStore(path).get_run(record["run_id"])["status"] == "success"
