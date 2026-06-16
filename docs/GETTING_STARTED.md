# Getting Started with Routine Engine

## Installation

```bash
pip install routine-engine
```

## Quick Start

```python
from routine_engine import WorkflowEngine, Scheduler

# Create engine
engine = WorkflowEngine()

# Define workflow
workflow = {
    "name": "DailyAnalysis",
    "steps": [
        {"id": "step-1", "action": "analyze_data"},
        {"id": "step-2", "action": "generate_report"}
    ]
}

# Create and execute
workflow_id = engine.create_workflow(workflow)
result = engine.execute_workflow(workflow_id)
```

## Scheduling Workflows

```python
scheduler = Scheduler()

# Schedule daily at midnight
schedule_id = scheduler.schedule_workflow(
    workflow_id,
    cron="0 0 * * *",
    timezone="UTC"
)
```

## Common Patterns

### 1. Sequential Workflow
```python
workflow = {
    "name": "Sequential",
    "steps": [
        {"id": "1", "action": "fetch_data"},
        {"id": "2", "action": "process"},
        {"id": "3", "action": "save"}
    ]
}
```

### 2. Parallel Execution
```python
workflow = {
    "name": "Parallel",
    "parallel": [
        {"id": "1", "action": "task_a"},
        {"id": "2", "action": "task_b"}
    ]
}
```

### 3. Conditional Workflow
```python
workflow = {
    "name": "Conditional",
    "steps": [
        {"id": "1", "action": "check_condition"},
        {"id": "2", "action": "if_true", "condition": "result.success"},
        {"id": "3", "action": "if_false", "condition": "!result.success"}
    ]
}
```

## Error Handling

```python
try:
    result = engine.execute_workflow(workflow_id)
except WorkflowError as e:
    print(f"Workflow failed: {e}")
```

## Next Steps

- Read the [API Reference](API_REFERENCE.md)
- Check out [examples](../examples/)
- Review [architecture](ARCHITECTURE.md)
