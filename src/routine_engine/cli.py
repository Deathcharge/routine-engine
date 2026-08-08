"""Command-line interface for validation and local execution."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Sequence

from .builtins import register_builtin_actions
from .engine import RoutineEngine
from .errors import RoutineEngineError, WorkflowValidationError
from .models import RunStatus
from .storage import JsonStore


def _version() -> str:
    try:
        return version("samsarix-routine-engine")
    except PackageNotFoundError:
        return "0.1.0"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="routine-engine",
        description="Validate and run small, local-first workflow DAGs.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {_version()}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="validate a workflow JSON file")
    validate.add_argument("workflow", type=Path)
    validate.add_argument("--plugin", action="append", default=[], metavar="MODULE")

    plan = subparsers.add_parser("plan", help="print deterministic execution layers")
    plan.add_argument("workflow", type=Path)
    plan.add_argument("--plugin", action="append", default=[], metavar="MODULE")

    run = subparsers.add_parser("run", help="run a workflow JSON file")
    run.add_argument("workflow", type=Path)
    run.add_argument("--input", default="{}", metavar="JSON_OR_@FILE")
    run.add_argument("--state", type=Path, help="persist definitions and the last 100 runs")
    run.add_argument("--plugin", action="append", default=[], metavar="MODULE")

    resume = subparsers.add_parser("resume", help="resume an interrupted persisted run")
    resume.add_argument("run_id")
    resume.add_argument("--state", type=Path, required=True)
    resume.add_argument("--plugin", action="append", default=[], metavar="MODULE")

    history = subparsers.add_parser("history", help="list recent persisted runs")
    history.add_argument("--state", type=Path, required=True)
    history.add_argument("--workflow")
    history.add_argument("--limit", type=int, default=20)

    show = subparsers.add_parser("show", help="show one persisted run")
    show.add_argument("run_id")
    show.add_argument("--state", type=Path, required=True)

    demo = subparsers.add_parser("demo", help="run the built-in greeting workflow")
    demo.add_argument("--name", default="world")
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise WorkflowValidationError(f"cannot read '{path}': {exc}") from exc
    except json.JSONDecodeError as exc:
        raise WorkflowValidationError(f"'{path}' is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise WorkflowValidationError(f"'{path}' must contain a JSON object")
    return value


def _load_input(raw: str) -> dict[str, Any]:
    if raw.startswith("@"):
        return _load_json(Path(raw[1:]))
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise WorkflowValidationError(f"--input is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise WorkflowValidationError("--input must be a JSON object")
    return value


def _make_engine(plugins: list[str], state: Path | None = None) -> RoutineEngine:
    engine = RoutineEngine(store=JsonStore(state) if state else None)
    register_builtin_actions(engine)
    for module_name in plugins:
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise WorkflowValidationError(f"cannot import plugin '{module_name}': {exc}") from exc
        register = getattr(module, "register", None)
        if not callable(register):
            raise WorkflowValidationError(f"plugin '{module_name}' must define register(engine)")
        register(engine)
    return engine


def _demo() -> dict[str, Any]:
    return {
        "id": "greeting",
        "steps": [
            {"id": "name", "action": "identity", "with": {"value": "{{ input.name }}"}},
            {
                "id": "greet",
                "action": "format",
                "needs": ["name"],
                "with": {
                    "template": "Hello, {name}!",
                    "values": {"name": "{{ steps.name.output }}"},
                },
            },
        ],
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "demo":
            result = _make_engine([]).run(_demo(), {"name": args.name})
            print(json.dumps(result.to_dict(), indent=2))
            return 0

        if args.command == "history":
            runs = JsonStore(args.state).list_runs(workflow_id=args.workflow, limit=args.limit)
            print(json.dumps(runs, indent=2))
            return 0

        if args.command == "show":
            print(json.dumps(JsonStore(args.state).get_run(args.run_id), indent=2))
            return 0

        if args.command == "resume":
            result = _make_engine(args.plugin, args.state).resume(args.run_id)
            print(json.dumps(result.to_dict(), indent=2, allow_nan=False))
            return 0 if result.status is RunStatus.SUCCESS else 1

        workflow = _load_json(args.workflow)
        if args.command == "validate":
            definition = _make_engine(args.plugin).validate(workflow)
            print(f"valid: {definition.id} ({len(definition.steps)} steps)")
            return 0

        if args.command == "plan":
            plan = _make_engine(args.plugin).plan(workflow)
            print(json.dumps(plan.to_dict(), indent=2))
            return 0

        result = _make_engine(args.plugin, args.state).run(workflow, _load_input(args.input))
        print(json.dumps(result.to_dict(), indent=2, allow_nan=False))
        return 0 if result.status is RunStatus.SUCCESS else 1
    except (RoutineEngineError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
