# API Reference

The supported API is exported from `routine_engine`. Anything under the repository's top-level `routine_engine/` directory is archived source and excluded from distributions.

## `RoutineEngine`

```python
RoutineEngine(*, store: JsonStore | None = None)
```

- `register(name, action, *, replace=False)`: add a trusted sync or async callable. Duplicate names fail unless replacement is explicit.
- `validate(workflow) -> Workflow`: validate shape, bounds, graph, references to registered action names, and return an immutable definition.
- `run(workflow, inputs=None) -> RunResult`: synchronous execution. It cannot be nested inside an active event loop.
- `await arun(workflow, inputs=None) -> RunResult`: asynchronous execution with bounded parallelism.
- `actions`: sorted tuple of registered names.

## `ActionContext`

Every action receives:

- `run_id`, `workflow_id`, `step_id`
- `attempt`, beginning at 1
- `inputs`, the run input mapping
- `params`, the step's recursively resolved `with` mapping
- `outputs`, successful outputs from the step's declared direct dependencies

The mappings are read-only views. Treat nested application values as immutable too.

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

IDs and action names start with a letter and may contain letters, numbers, `.`, `_`, and `-`.

## Results

`RunResult.status` is `success`, `failed`, or `cancelled`. `RunResult.succeeded` is a convenience boolean. `StepResult.status` is `success`, `failed`, `skipped`, or `cancelled`; it also provides attempt count, timestamps, output, and a `TypeName: message` error. Applications should avoid putting secrets in exception messages.

Actions exhausting their retries make the run fail. Steps depending on anything other than success are skipped. Independent branches still complete.

## `JsonStore`

```python
JsonStore(path, *, history_limit=100)
```

The store atomically replaces one versioned JSON file. `snapshot()` returns a detached mapping. Invalid/corrupt data and non-JSON action outputs raise `StorageError`; existing state is not silently discarded.

This is local persistence, not a transactional multi-process database.

## Exceptions

- `RoutineEngineError`: base expected error
- `WorkflowValidationError`: invalid definition, input JSON, reference, or plugin
- `ActionRegistrationError`: invalid/duplicate action registration
- `StorageError`: unreadable, invalid, or unwritable state
