"""Atomic local JSON persistence for workflows and bounded run history."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, cast

from .errors import StorageError
from .models import RunResult, Workflow


class JsonStore:
    """Persist definitions and recent results in one human-readable JSON file.

    Writes are atomic within one filesystem. A store instance is thread-safe, but
    concurrent writers in separate processes require external coordination.
    """

    def __init__(self, path: str | Path, *, history_limit: int = 100) -> None:
        if not 1 <= history_limit <= 10_000:
            raise ValueError("history_limit must be from 1 to 10000")
        self.path = Path(path).expanduser().resolve()
        self.history_limit = history_limit
        self._lock = threading.RLock()

    def save_workflow(self, workflow: Workflow) -> None:
        with self._lock:
            state = self._read()
            state["workflows"][workflow.id] = workflow.to_dict()
            self._write(state)

    def append_run(self, run: RunResult) -> None:
        with self._lock:
            state = self._read()
            state["runs"].append(run.to_dict())
            state["runs"] = state["runs"][-self.history_limit :]
            self._write(state)

    def snapshot(self) -> dict[str, Any]:
        """Return a detached copy of current persisted state."""

        with self._lock:
            return cast(dict[str, Any], json.loads(json.dumps(self._read(), allow_nan=False)))

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": 1, "workflows": {}, "runs": []}
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StorageError(f"cannot read state from '{self.path}': {exc}") from exc
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != 1
            or not isinstance(value.get("workflows"), dict)
            or not isinstance(value.get("runs"), list)
        ):
            raise StorageError(f"state file '{self.path}' does not match schema version 1")
        return value

    def _write(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            payload = json.dumps(state, indent=2, sort_keys=True, allow_nan=False) + "\n"
        except (TypeError, ValueError, OverflowError) as exc:
            raise StorageError("workflow outputs must be finite, JSON-compatible data to persist them") from exc

        temp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temp_path = handle.name
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.path)
        except OSError as exc:
            raise StorageError(f"cannot write state to '{self.path}': {exc}") from exc
        finally:
            if temp_path is not None:
                try:
                    Path(temp_path).unlink(missing_ok=True)
                except OSError:
                    pass
