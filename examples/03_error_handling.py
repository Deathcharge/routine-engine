"""Error handling example."""

from routine_engine import WorkflowEngine, WorkflowError

engine = WorkflowEngine()

# Workflow with retry
workflow = {
    "name": "WithRetry",
    "retries": 3,
    "retry_delay": 5,
    "steps": [
        {"id": "1", "action": "risky_operation"}
    ]
}

workflow_id = engine.create_workflow(workflow)

try:
    result = engine.execute_workflow(workflow_id)
    print(f"Success: {result}")
except WorkflowError as e:
    print(f"Workflow failed: {e}")
