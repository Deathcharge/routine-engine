# Samsarix Routine Engine

Routine Engine is a small, local-first Python library and CLI for deterministic workflow DAGs. You register trusted Python functions, describe dependencies in JSON or Python, and receive an exact result for every step—without operating a control plane.

The focused use case is application-owned routines: import jobs, report generation, release checks, data preparation, and other bounded workflows that should stay inside an existing Python process or CI job.

> Maturity: **0.2.1 release candidate.** A tested local library and CLI for trusted application routines. Repository installation is supported; package-index publication and production adoption remain separate owner decisions.

## Why this exists

- Zero runtime dependencies and no service to deploy.
- Validates IDs, dependencies, cycles, JSON parameters, retries, and concurrency before execution.
- Supports synchronous and asynchronous actions, bounded parallelism, and bounded retries.
- Resolves only explicit input and prior-output references—never `eval`, shell snippets, or workflow-supplied imports.
- Reports `success`, `failed`, `skipped`, or `cancelled` for each step.
- Optionally keeps workflow definitions and bounded run history in an atomic local JSON store.

If you need distributed workers, a scheduler UI, event streaming, asset lineage, or a multi-tenant control plane, use a larger orchestrator. Routine Engine deliberately does not pretend to be one.

## Install from this repository

Routine Engine has not been published by this productization pass.

```bash
git clone https://github.com/Deathcharge/routine-engine.git
cd routine-engine
python -m pip install .
```

Python 3.10 or newer is required.

## Five-minute Python journey

```python
from routine_engine import ActionContext, RoutineEngine

engine = RoutineEngine()


def normalize(context: ActionContext) -> str:
    return str(context.params["name"]).strip().title()


def greeting(context: ActionContext) -> str:
    return f"Hello, {context.params['name']}!"


engine.register("normalize", normalize)
engine.register("greeting", greeting)

workflow = {
    "id": "welcome",
    "steps": [
        {
            "id": "name",
            "action": "normalize",
            "with": {"name": "{{ input.name }}"},
        },
        {
            "id": "message",
            "action": "greeting",
            "needs": ["name"],
            "with": {"name": "{{ steps.name.output }}"},
        },
    ],
}

result = engine.run(workflow, {"name": "  ada  "})
assert result.succeeded
print(result.steps["message"].output)  # Hello, Ada!
```

Use `await engine.arun(...)` inside asynchronous applications. Calling `run()` from an active event loop fails with an actionable error instead of creating a nested loop.

## CLI journey

The CLI includes three side-effect-free actions: `identity`, `merge`, and `format`.

```bash
routine-engine demo --name Ada
routine-engine schema
routine-engine validate examples/workflow.json
routine-engine plan examples/workflow.json
routine-engine run examples/workflow.json --input '{"name":"Ada"}'
routine-engine run examples/workflow.json --input @input.json --state .routine-state.json
routine-engine history --state .routine-state.json
routine-engine show RUN_ID --state .routine-state.json
routine-engine resume RUN_ID --state .routine-state.json --plugin my_actions
```

Application actions can be loaded only from an explicit trusted module:

```python
# my_actions.py
def register(engine):
    engine.register("send_report", send_report)
```

```bash
routine-engine run workflow.json --plugin my_actions
```

Workflow data cannot choose the module that gets imported.

## Workflow format

```json
{
  "schema_version": 1,
  "id": "welcome",
  "description": "Prepare a greeting",
  "max_concurrency": 4,
  "steps": [
    {
      "id": "name",
      "action": "identity",
      "with": {"value": "{{ input.name }}"},
      "retries": 2,
      "retry_delay_seconds": 0.5
    },
    {
      "id": "result",
      "action": "merge",
      "needs": ["name"],
      "with": {"name": "{{ steps.name.output }}"}
    }
  ]
}
```

References must occupy the complete string. Supported forms are `{{ input.path.to.value }}`, `{{ steps.step_id.output }}`, and `{{ steps.step_id.output.path }}`. Missing references fail the affected step; downstream steps are skipped.

Resource limits are part of the public contract: 256 steps, 32 concurrent actions, 10 retries per step, 60 seconds maximum configured retry delay, 1 MiB definitions and inputs, 4 MiB step outputs, 32 JSON nesting levels, and 4,096-character errors. Data crossing a workflow boundary must be finite, portable JSON with string object keys.

Synchronous actions run in a bounded worker-thread pool so one blocking action does not freeze the asynchronous scheduler. Cancellation cannot forcibly terminate Python code already executing in a thread. Registered actions remain trusted application code; Routine Engine does not sandbox them.

When `--state` or `JsonStore` is used, the engine writes the run snapshot before starting work and atomically checkpoints every terminal step. If the process is interrupted, `engine.resume(run_id)` (or the CLI `resume` command) keeps all terminal checkpoints, including exhausted failures, and runs only unfinished steps. Finished or explicitly cancelled runs cannot be resumed; start a new run instead. Actions with external side effects must be idempotent: a process can stop after the side effect occurs but before its success checkpoint reaches disk.

History pruning removes terminal runs only. If all history slots are occupied by unfinished runs, new runs fail before actions start; finish those runs or increase `history_limit`. Use one shared `JsonStore` instance per file and externally coordinate other instances/processes. State is plaintext; do not pass secrets through persisted inputs or outputs. Resume restores data, not Python code: use the same trusted action implementation and external resources as the original run.

## Development

```bash
python -m pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src
pytest --cov --cov-report=term-missing
python -m build
python -m twine check dist/*
python scripts/verify_release.py --dist dist
python benchmarks/benchmark_engine.py --steps 256 --runs 20
```

The release verifier requires exactly one wheel and one source archive in its artifact directory. Use a fresh directory (`python -m build --outdir dist/0.2.1`) when older builds exist, then pass that directory to the verifier.

See [Getting Started](docs/GETTING_STARTED.md), [API Reference](docs/API_REFERENCE.md), [Use Cases](docs/USE_CASES.md), [Release Checklist](docs/RELEASING.md), [Competitive Position](docs/COMPETITIVE_ANALYSIS.md), and [Security Policy](SECURITY.md).

## Scope and legacy source

Only `src/routine_engine` is distributed. The top-level `routine_engine/` directory is an archived extraction from the former Helix monorepo and is not a supported or packaged API. It remains temporarily for provenance and migration analysis; see its local notice.

## Support

- Product questions: [contact@samsarix.com](mailto:contact@samsarix.com)
- Support and responsible disclosure: [support@samsarix.com](mailto:support@samsarix.com)
- Defects: [GitHub Issues](https://github.com/Deathcharge/routine-engine/issues)

## License

The repository is licensed under the Business Source License 1.1 with Samsarix LLC as licensor and Samsarix Routine Engine as the licensed work. The existing production-use threshold, June 16, 2027 change date, and Apache License 2.0 change license remain unchanged. See [LICENSE](LICENSE) for the controlling terms; commercial licensing questions go to [contact@samsarix.com](mailto:contact@samsarix.com).

Copyright © 2026 Samsarix LLC.
