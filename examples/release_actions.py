"""Trusted actions for the release-readiness workflow example."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from routine_engine import ActionContext, RoutineEngine


def require_files(context: ActionContext) -> dict[str, Any]:
    files = [Path(str(item)) for item in context.params["paths"]]
    missing = [path.as_posix() for path in files if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"required release files are missing: {', '.join(missing)}")
    return {
        "files": [path.as_posix() for path in files],
        "total_bytes": sum(path.stat().st_size for path in files),
    }


def sha256(context: ActionContext) -> dict[str, str]:
    path = Path(str(context.params["path"]))
    return {"path": path.as_posix(), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def register(engine: RoutineEngine) -> None:
    engine.register("release.require_files", require_files)
    engine.register("release.sha256", sha256)
