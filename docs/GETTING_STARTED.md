# Getting Started

## Requirements

- Python 3.10 or newer
- A virtual environment is recommended

Install the repository checkout:

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"  # Windows
```

On macOS or Linux, use `.venv/bin/python` instead.

## Run the built-in journey

```bash
routine-engine demo --name Ada
```

The command prints a complete JSON run result. The final `greet` step contains `Hello, Ada!`.

## Run a JSON workflow

Save this as `workflow.json`:

```json
{
  "id": "copy-name",
  "steps": [
    {
      "id": "name",
      "action": "identity",
      "with": {"value": "{{ input.name }}"}
    },
    {
      "id": "result",
      "action": "merge",
      "needs": ["name"],
      "with": {"normalized_name": "{{ steps.name.output }}"}
    }
  ]
}
```

Then validate and run it:

```bash
routine-engine validate workflow.json
routine-engine run workflow.json --input '{"name":"Ada"}' --state .routine-state.json
```

`--state` is optional. It stores definitions and at most 100 recent results using same-directory atomic replacement. One store instance is thread-safe; coordinate separately if multiple processes write the same file.

## Register application actions

An action accepts one `ActionContext`. It may be synchronous or asynchronous.

```python
from routine_engine import ActionContext, RoutineEngine


async def fetch_customer(context: ActionContext) -> dict[str, str]:
    customer_id = str(context.params["customer_id"])
    # Call your application service here.
    return {"id": customer_id, "status": "active"}


engine = RoutineEngine()
engine.register("fetch_customer", fetch_customer)
```

Registration is the trust boundary. Do not register functions that accept unreviewed commands, code, filesystem paths, SQL, or URLs without enforcing the policy your application needs.

## Scheduling

Routine Engine executes one run; it does not run a scheduler. Invoke the CLI or your Python entrypoint from cron, Windows Task Scheduler, GitHub Actions, or the scheduling system you already operate.
