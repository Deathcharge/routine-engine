# Use cases and adoption boundary

Routine Engine is designed for application-owned workflows that need explicit dependencies, bounded local concurrency, portable results, and crash recovery without a separate orchestration service.

## Release-readiness evidence

The repository includes a real consumer fixture in `examples/release-readiness.json`. Its trusted plugin verifies required release files, fingerprints artifacts in parallel, and produces structured evidence. Run it from the repository root:

```bash
routine-engine plan examples/release-readiness.json --plugin examples.release_actions
routine-engine run examples/release-readiness.json --plugin examples.release_actions --state .routine-state.json
```

This pattern fits package publication, signed-build preparation, compliance evidence, and deployment preflight checks. The workflow stays declarative while file access remains in reviewed application code.

## Import and data-preparation jobs

Model each validation, normalization, enrichment, and export operation as an action. Independent enrichment branches can run concurrently; downstream export is skipped when a prerequisite fails. Inputs and outputs remain JSON-portable, which makes results straightforward to audit or hand to another process.

## Embedded report generation

An API or background worker can register actions for querying application data, calculating independent report sections, and rendering a final artifact. `arun()` integrates with an existing event loop, while synchronous library calls are isolated from the scheduler in bounded worker threads.

## CI and repository automation

The `plan`, `validate`, and `run` commands are machine-friendly and require no daemon. Persisted runs can be inspected with `history` and `show`; interrupted jobs can be continued with `resume` when the same trusted plugin is available.

## Correctness boundary

- Registered actions are trusted code and are not sandboxed.
- Persistence is local and single-process; use an external coordinator for concurrent writers.
- Successful checkpoints provide at-least-once recovery, not exactly-once side effects. Side-effecting actions should use application-level idempotency keys based on `context.run_id` and `context.step_id`.
- Inputs, outputs, and state may contain sensitive data and are stored as plaintext JSON. Applications own redaction, file permissions, and retention.
- Distributed queues, calendars, event triggers, dashboards, and multi-tenant control planes remain intentionally out of scope.

## Adoption contract

Consumers should depend only on the exported Python API, workflow schema v1, CLI exit codes, and JSON result shapes. The bundled schema is available through `routine-engine schema` and at `schemas/workflow-v1.schema.json`. Compatibility-breaking workflow changes require a new schema version rather than silently changing v1.
