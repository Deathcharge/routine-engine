"""Atomic local JSON persistence for workflows and bounded run history."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from .errors import StorageError, WorkflowValidationError
from .models import RunResult, StepResult, Workflow

STORE_SCHEMA_VERSION = 2
MAX_STORE_BYTES = 64 * 1024 * 1024


class JsonStore:
    """Persist definitions and recent results in one human-readable JSON file.

    Writes are atomic within one filesystem. A store instance is thread-safe, but
    concurrent writers in separate processes require external coordination.
    """

    def __init__(self, path: str | Path, *, history_limit: int = 100) -> None:
        if type(history_limit) is not int or not 1 <= history_limit <= 10_000:
            raise ValueError("history_limit must be from 1 to 10000")
        self.path = Path(path).expanduser().resolve()
        self.history_limit = history_limit
        self._lock = threading.RLock()
        self._claims: set[str] = set()

    @contextmanager
    def claim_run(self, run_id: str) -> Iterator[None]:
        """Prevent overlapping execution of a run through this store instance."""

        with self._lock:
            if run_id in self._claims:
                raise StorageError(f"run '{run_id}' is already executing through this store")
            self._claims.add(run_id)
        try:
            yield
        finally:
            with self._lock:
                self._claims.remove(run_id)

    def save_workflow(self, workflow: Workflow) -> None:
        with self._lock:
            state = self._read()
            state["workflows"][workflow.id] = workflow.to_dict()
            self._write(state)

    def append_run(self, run: RunResult) -> None:
        with self._lock:
            state = self._read()
            self._require_new_run(state, run.run_id)
            state["runs"].append(run.to_dict())
            self._trim_history(state)
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
            self._require_new_run(state, run_id)
            state["workflows"][workflow.id] = workflow.to_dict()
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
            self._trim_history(state)
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
            preserved: dict[str, Any] = {key: record[key] for key in ("workflow", "inputs") if key in record}
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

        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("limit must be from 1 to 10000")
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
            with self.path.open("rb") as handle:
                payload = handle.read(MAX_STORE_BYTES + 1)
            if len(payload) > MAX_STORE_BYTES:
                raise StorageError(f"state file '{self.path}' exceeds {MAX_STORE_BYTES} bytes")
            value = json.loads(payload.decode("utf-8"), parse_constant=_reject_constant)
        except (OSError, UnicodeError, ValueError, RecursionError) as exc:
            raise StorageError(f"cannot read state from '{self.path}': {exc}") from exc
        if (
            isinstance(value, dict)
            and type(value.get("schema_version")) is int
            and value["schema_version"] == 1
        ):
            value["schema_version"] = STORE_SCHEMA_VERSION
        if (
            not isinstance(value, dict)
            or type(value.get("schema_version")) is not int
            or value.get("schema_version") != STORE_SCHEMA_VERSION
            or not isinstance(value.get("workflows"), dict)
            or not isinstance(value.get("runs"), list)
        ):
            raise StorageError(
                f"state file '{self.path}' does not match schema version {STORE_SCHEMA_VERSION}"
            )
        try:
            seen: set[str] = set()
            for record in value["runs"]:
                if not isinstance(record, dict):
                    raise ValueError("run must be an object")
                run_id = record["run_id"]
                if not isinstance(run_id, str) or not run_id or run_id in seen:
                    raise ValueError("run IDs must be nonempty and unique")
                seen.add(run_id)
                if not isinstance(record["workflow_id"], str) or not isinstance(record["steps"], dict):
                    raise ValueError("run metadata is malformed")
                if record["status"] not in ("running", "success", "failed", "cancelled"):
                    raise ValueError("unknown run status")
                started = datetime.fromisoformat(record["started_at"])
                if started.tzinfo is None:
                    raise ValueError("run timestamp must be timezone-aware")
                for key, raw_step in record["steps"].items():
                    if StepResult.from_dict(raw_step).step_id != key:
                        raise ValueError("checkpoint step ID mismatch")
        except (KeyError, TypeError, ValueError, WorkflowValidationError) as exc:
            raise StorageError(f"state file '{self.path}' contains malformed run records: {exc}") from exc
        return value

    def _write(self, state: dict[str, Any]) -> None:
        try:
            payload = json.dumps(state, indent=2, sort_keys=True, allow_nan=False) + "\n"
        except (TypeError, ValueError, OverflowError, RecursionError) as exc:
            raise StorageError(
                "workflow outputs must be finite, JSON-compatible data to persist them"
            ) from exc
        if len(payload.encode("utf-8")) > MAX_STORE_BYTES:
            raise StorageError(f"state exceeds the maximum size of {MAX_STORE_BYTES} bytes")

        temp_path: str | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
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
                with suppress(OSError):
                    Path(temp_path).unlink(missing_ok=True)

    @staticmethod
    def _require_new_run(state: dict[str, Any], run_id: str) -> None:
        if any(record["run_id"] == run_id for record in state["runs"]):
            raise StorageError(f"run '{run_id}' already exists")

    def _trim_history(self, state: dict[str, Any]) -> None:
        records = state["runs"]
        active = sum(record["status"] == "running" for record in records)
        if active > self.history_limit:
            raise StorageError("history is full of active runs; finish runs or increase history_limit")
        excess = len(records) - self.history_limit
        kept = []
        for record in records:
            if excess > 0 and record["status"] != "running":
                excess -= 1
            else:
                kept.append(record)
        state["runs"] = kept

    @staticmethod
    def _find_run(state: dict[str, Any], run_id: str) -> dict[str, Any]:
        for record in state["runs"]:
            if isinstance(record, dict) and record.get("run_id") == run_id:
                return record
        raise StorageError(f"run '{run_id}' was not found")


def _reject_constant(value: str) -> Any:
    raise ValueError(f"non-finite JSON constant: {value}")
