"""Small, side-effect-free actions used by the CLI and examples."""

from __future__ import annotations

from typing import Any

from .engine import RoutineEngine
from .models import ActionContext


def identity(context: ActionContext) -> Any:
    """Return ``with.value`` or the complete workflow input."""

    return context.params.get("value", dict(context.inputs))


def merge(context: ActionContext) -> dict[str, Any]:
    """Return resolved step parameters as a new mapping."""

    return dict(context.params)


def format_text(context: ActionContext) -> str:
    """Format ``with.template`` using the mapping in ``with.values``."""

    template = context.params.get("template")
    values = context.params.get("values", {})
    if not isinstance(template, str):
        raise ValueError("format requires a string 'template'")
    if not isinstance(values, dict):
        raise ValueError("format requires an object named 'values'")
    return template.format_map(values)


def register_builtin_actions(engine: RoutineEngine) -> None:
    """Register the intentionally small, safe built-in action set."""

    engine.register("identity", identity)
    engine.register("merge", merge)
    engine.register("format", format_text)
