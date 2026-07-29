# Samsarix Routine Engine

Routine Engine is a small, local-first Python library and CLI for deterministic workflow DAGs. You register trusted Python functions, describe dependencies in JSON or Python, and receive an exact result for every step—without operating a control plane.

The focused use case is application-owned routines: import jobs, report generation, release checks, data preparation, and other bounded workflows that should stay inside an existing Python process or CI job.

> Maturity: **0.1 release candidate.** The supported core is implemented, behaviorally tested, and ready for release review. It has not been published by this repository update.

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
routine-engine validate examples/workflow.json
routine-engine run examples/workflow.json --input '{"name":"Ada"}'
routine-engine run examples/workflow.json --input @input.json --state .routine-state.json
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

Resource limits are part of the public contract: 256 steps, 32 concurrent actions, 10 retries per step, and 60 seconds maximum configured retry delay. Registered actions are trusted application code; Routine Engine does not sandbox them.

## Development

```bash
python -m pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src
pytest --cov --cov-report=term-missing
python -m build
python -m twine check dist/*
```

See [Getting Started](docs/GETTING_STARTED.md), [API Reference](docs/API_REFERENCE.md), [Productization](docs/PRODUCTIZATION.md), [Security Policy](SECURITY.md), and [Contributing](CONTRIBUTING.md).

## Scope and legacy source

Only `src/routine_engine` is distributed. The top-level `routine_engine/` directory is an archived extraction from the former Helix monorepo and is not a supported or packaged API. It remains temporarily for provenance and migration analysis; see its local notice.

## Support

- Product questions: [contact@samsarix.com](mailto:contact@samsarix.com)
- Support and responsible disclosure: [support@samsarix.com](mailto:support@samsarix.com)
- Defects: [GitHub Issues](https://github.com/Deathcharge/routine-engine/issues)

## License

The repository is licensed under the Business Source License 1.1 with Samsarix LLC as licensor and Samsarix Routine Engine as the licensed work. The existing production-use threshold, June 16, 2027 change date, and Apache License 2.0 change license remain unchanged. See [LICENSE](LICENSE) for the controlling terms; commercial licensing questions go to [contact@samsarix.com](mailto:contact@samsarix.com).

Copyright © 2026 Samsarix LLC.
