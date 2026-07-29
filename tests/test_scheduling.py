from __future__ import annotations

import json
from pathlib import Path

import pytest

from routine_engine import ActionContext, JsonStore, RoutineEngine, StorageError
from routine_engine.cli import main


def test_json_store_persists_definition_and_bounded_history(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "state.json"
    engine = RoutineEngine(store=JsonStore(path, history_limit=2))
    engine.register("ok", lambda _: {"saved": True})
    workflow = {"id": "stored", "steps": [{"id": "one", "action": "ok"}]}

    for _ in range(3):
        engine.run(workflow)

    snapshot = JsonStore(path).snapshot()
    assert snapshot["schema_version"] == 1
    assert snapshot["workflows"]["stored"] == workflow
    assert len(snapshot["runs"]) == 2
    assert all(run["status"] == "success" for run in snapshot["runs"])


def test_json_store_rejects_corrupt_schema_and_non_json_output(tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("not json", encoding="utf-8")
    with pytest.raises(StorageError, match="cannot read"):
        JsonStore(corrupt).snapshot()

    wrong = tmp_path / "wrong.json"
    wrong.write_text('{"schema_version": 2}', encoding="utf-8")
    with pytest.raises(StorageError, match="schema version 1"):
        JsonStore(wrong).snapshot()

    engine = RoutineEngine(store=JsonStore(tmp_path / "state.json"))
    engine.register("set", lambda _: {1})
    with pytest.raises(StorageError, match="JSON-compatible"):
        engine.run({"id": "bad", "steps": [{"id": "one", "action": "set"}]})


def test_store_constructor_bounds_history(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="history_limit"):
        JsonStore(tmp_path / "state.json", history_limit=0)


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

    state = tmp_path / "state.json"
    assert main(["run", str(workflow_path), "--input", '{"name":"Ada"}', "--state", str(state)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["steps"]["out"]["output"] == {"name": "Ada"}
    assert state.exists()

    assert main(["demo", "--name", "Sam"]) == 0
    assert "Hello, Sam!" in capsys.readouterr().out

    with pytest.raises(SystemExit) as raised:
        main(["--version"])
    assert raised.value.code == 0


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
