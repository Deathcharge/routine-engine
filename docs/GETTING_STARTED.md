# Getting Started

## Requirements

- Python 3.10 or newer
- A virtual environment is recommended

Install the repository checkout:

```bash
python -m venv .venv
.venv/Scripts/Activate.ps1  # PowerShell on Windows
python -m pip install -e ".[dev]"
```

On macOS or Linux, activate with `source .venv/bin/activate` instead. If activation is unavailable, call `.venv/Scripts/python -m routine_engine ...` (Windows) or `.venv/bin/python -m routine_engine ...` (Unix) directly.

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

`--state` is optional. It stores definitions and at most 100 run records using same-directory atomic replacement. Unfinished records are never pruned; a full store of unfinished runs rejects new runs. Share one store instance per file; coordinate separately if other instances or processes write the same file.

Inspect a run with `routine-engine history --state .routine-state.json` and `routine-engine show RUN_ID --state .routine-state.json`. To continue a process-interrupted run, use `routine-engine resume RUN_ID --state .routine-state.json` with the same trusted plugins. Explicitly cancelled, successful, and failed runs are terminal and cannot be resumed.

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
