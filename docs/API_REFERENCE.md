# API Reference

The supported API is exported from `routine_engine`. Anything under the repository's top-level `routine_engine/` directory is archived source and excluded from distributions.

## `RoutineEngine`

```python
RoutineEngine(*, store: JsonStore | None = None)
```

- `register(name, action, *, replace=False)`: add a trusted sync or async callable. Duplicate names fail unless replacement is explicit.
- `validate(workflow) -> Workflow`: validate shape, bounds, graph, references to registered action names, and return an immutable definition.
- `plan(workflow) -> ExecutionPlan`: return deterministic topological layers without executing actions.
- `run(workflow, inputs=None) -> RunResult`: synchronous execution. It cannot be nested inside an active event loop.
- `await arun(workflow, inputs=None) -> RunResult`: asynchronous execution with bounded parallelism.
- `resume(run_id) -> RunResult` / `await aresume(run_id)`: continue a process-interrupted active run, reusing all terminal checkpoints. Exhausted failures remain failed; resume does not reset retries. Finished/cancelled runs are not resumable.
- `actions`: sorted tuple of registered names.

## `ActionContext`

Every action receives:

- `run_id`, `workflow_id`, `step_id`
- `attempt`, beginning at 1
- `inputs`, the run input mapping
- `params`, the step's recursively resolved `with` mapping
- `outputs`, successful outputs from the step's declared direct dependencies

Each attempt receives detached mappings with read-only top-level views. Nested mutations cannot affect sibling steps, earlier results, run inputs, or later retry attempts. Treat nested values as immutable application data even though Python permits local changes.

## `Workflow` and `Step`

Use `Workflow.from_dict(value)` to validate a raw mapping independently, or pass the mapping directly to an engine. `Workflow.to_dict()` returns the portable JSON shape. Step-output references must name a step listed in that step's `needs`; this makes resolution deterministic.

Limits:

| Setting | Limit |
| --- | ---: |
| Steps per workflow | 256 |
| Concurrent actions | 32 |
| Retries per step | 10 |
| Retry delay | 60 seconds |
| Description | 1,000 characters |
| Workflow definition | 1 MiB |
| Run inputs | 1 MiB |
| Step output | 4 MiB |
| JSON depth | 32 levels |
| Error text | 4,096 characters |

IDs and action names start with a letter and may contain letters, numbers, `.`, `_`, and `-`.

## Results

`RunResult.status` is `success`, `failed`, or `cancelled`. `RunResult.succeeded` is a convenience boolean. `StepResult.status` is `success`, `failed`, `skipped`, or `cancelled`; it also provides attempt count, timestamps, output, and a `TypeName: message` error. Applications should avoid putting secrets in exception messages.

Actions exhausting their retries make the run fail. Steps depending on anything other than success are skipped. Independent branches still complete.

## `JsonStore`

```python
JsonStore(path, *, history_limit=100)
```

The store atomically replaces one versioned JSON file, capped at 64 MiB. `snapshot()` returns a detached mapping; `get_run()` and `list_runs()` inspect history. `begin_run()`, `record_step()`, and `finish_run()` form the checkpoint lifecycle used by the engine. Invalid/corrupt data raises `StorageError`; existing state is not silently discarded.

State schema v2 reads version 1 definition/history files and migrates them on the next write. Only v2 runs started with a workflow and input snapshot are resumable. The workflow, JSON input types, step IDs, retry budgets, and successful dependency chain are checked before recovery. Same-instance overlapping execution is rejected. The snapshot does not version plugin code: consumers must keep action implementations compatible while a run is unfinished.

`history_limit` bounds retained run records, not just finished results. Only terminal records can be evicted. When all slots are active, starting another run raises `StorageError` before actions execute. Duplicate run IDs cannot overwrite history. Store size overflow and corrupted state fail explicitly without replacing the previous file. `list_runs(limit=20)` returns newest-first full records (not redacted summaries); `limit` is independently bounded to 1–10,000.

This is local persistence, not a transactional multi-process database, and it stores data in plaintext. Share one store instance per file, or externally coordinate all readers/writers. Atomic replacement protects against partial files on process interruption, not every filesystem or machine-power-loss scenario. There is no exactly-once guarantee. Storage errors cancel and await outstanding async actions; Python cannot forcibly terminate an already-running synchronous thread.

## Exceptions

- `RoutineEngineError`: base expected error
- `WorkflowValidationError`: invalid definition, input JSON, reference, or plugin
- `ActionRegistrationError`: invalid/duplicate action registration
- `StorageError`: unreadable, invalid, or unwritable state
