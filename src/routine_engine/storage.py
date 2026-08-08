"""Atomic local JSON persistence for workflows and bounded run history."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, cast

from .errors import StorageError
from .models import RunResult, StepResult, Workflow

STORE_SCHEMA_VERSION = 2
MAX_STORE_BYTES = 64 * 1024 * 1024


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

    def begin_run(
        self,
        run_id: str,
        workflow: Workflow,
        inputs: dict[str, Any],
        started_at: str,
    ) -> None:
        """Persist a resumable run before its first action starts."""

        with self._lock:
            state = self._read()
            state["workflows"][workflow.id] = workflow.to_dict()
            state["runs"] = [item for item in state["runs"] if item.get("run_id") != run_id]
            state["runs"].append(
                {
                    "run_id": run_id,
                    "workflow_id": workflow.id,
                    "status": "running",
                    "started_at": started_at,
                    "finished_at": None,
                    "workflow": workflow.to_dict(),
                    "inputs": inputs,
                    "steps": {},
                }
            )
            state["runs"] = state["runs"][-self.history_limit :]
            self._write(state)

    def record_step(self, run_id: str, step: StepResult) -> None:
        """Atomically checkpoint one terminal step result."""

        with self._lock:
            state = self._read()
            record = self._find_run(state, run_id)
            if record.get("status") != "running":
                raise StorageError(f"run '{run_id}' is not active")
            record["steps"][step.step_id] = step.to_dict()
            self._write(state)

    def finish_run(self, run: RunResult) -> None:
        """Replace an active checkpoint with its terminal result."""

        with self._lock:
            state = self._read()
            record = self._find_run(state, run.run_id)
            preserved = {key: record[key] for key in ("workflow", "inputs") if key in record}
            record.clear()
            record.update(run.to_dict())
            record.update(preserved)
            self._write(state)

    def get_run(self, run_id: str) -> dict[str, Any]:
        """Return one detached run record."""

        with self._lock:
            record = self._find_run(self._read(), run_id)
            return cast(dict[str, Any], json.loads(json.dumps(record, allow_nan=False)))

    def list_runs(self, *, workflow_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        """Return newest-first run summaries."""

        if not 1 <= limit <= self.history_limit:
            raise ValueError(f"limit must be from 1 to {self.history_limit}")
        with self._lock:
            records = self._read()["runs"]
            if workflow_id is not None:
                records = [item for item in records if item.get("workflow_id") == workflow_id]
            selected = list(reversed(records[-limit:]))
            return cast(list[dict[str, Any]], json.loads(json.dumps(selected, allow_nan=False)))

    def snapshot(self) -> dict[str, Any]:
        """Return a detached copy of current persisted state."""

        with self._lock:
            return cast(dict[str, Any], json.loads(json.dumps(self._read(), allow_nan=False)))

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": STORE_SCHEMA_VERSION, "workflows": {}, "runs": []}
        try:
            if self.path.stat().st_size > MAX_STORE_BYTES:
                raise StorageError(f"state file '{self.path}' exceeds {MAX_STORE_BYTES} bytes")
        except OSError as exc:
            raise StorageError(f"cannot inspect state file '{self.path}': {exc}") from exc
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StorageError(f"cannot read state from '{self.path}': {exc}") from exc
        if isinstance(value, dict) and value.get("schema_version") == 1:
            value["schema_version"] = STORE_SCHEMA_VERSION
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != STORE_SCHEMA_VERSION
            or not isinstance(value.get("workflows"), dict)
            or not isinstance(value.get("runs"), list)
        ):
            raise StorageError(
                f"state file '{self.path}' does not match schema version {STORE_SCHEMA_VERSION}"
            )
        return value

    def _write(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            payload = json.dumps(state, indent=2, sort_keys=True, allow_nan=False) + "\n"
        except (TypeError, ValueError, OverflowError) as exc:
            raise StorageError("workflow outputs must be finite, JSON-compatible data to persist them") from exc
        if len(payload.encode("utf-8")) > MAX_STORE_BYTES:
            raise StorageError(f"state exceeds the maximum size of {MAX_STORE_BYTES} bytes")

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

    @staticmethod
    def _find_run(state: dict[str, Any], run_id: str) -> dict[str, Any]:
        for record in state["runs"]:
            if isinstance(record, dict) and record.get("run_id") == run_id:
                return record
        raise StorageError(f"run '{run_id}' was not found")
