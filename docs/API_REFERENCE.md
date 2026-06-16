# Routine Engine API Reference

## WorkflowEngine

Main class for managing workflows.

### Methods

#### create_workflow(config)
Create a new workflow.

**Parameters:**
- `config` (dict): Workflow configuration

**Returns:**
- `workflow_id` (str): ID of created workflow

#### execute_workflow(workflow_id, params=None)
Execute a workflow.

**Parameters:**
- `workflow_id` (str): ID of workflow to execute
- `params` (dict, optional): Execution parameters

**Returns:**
- `result` (dict): Execution result

#### get_workflow(workflow_id)
Get workflow details.

**Parameters:**
- `workflow_id` (str): ID of workflow

**Returns:**
- `workflow` (dict): Workflow details

#### list_workflows()
List all workflows.

**Returns:**
- `workflows` (list): List of workflows

#### delete_workflow(workflow_id)
Delete a workflow.

**Parameters:**
- `workflow_id` (str): ID of workflow to delete

**Returns:**
- `success` (bool): Whether deletion succeeded

## Scheduler

Class for scheduling workflows.

### Methods

#### schedule_workflow(workflow_id, cron, timezone="UTC")
Schedule a workflow.

**Parameters:**
- `workflow_id` (str): ID of workflow
- `cron` (str): Cron expression
- `timezone` (str): Timezone for scheduling

**Returns:**
- `schedule_id` (str): ID of schedule

#### get_schedule(schedule_id)
Get schedule details.

**Parameters:**
- `schedule_id` (str): ID of schedule

**Returns:**
- `schedule` (dict): Schedule details

#### list_schedules()
List all schedules.

**Returns:**
- `schedules` (list): List of schedules

#### cancel_schedule(schedule_id)
Cancel a schedule.

**Parameters:**
- `schedule_id` (str): ID of schedule

**Returns:**
- `success` (bool): Whether cancellation succeeded

## Configuration

### Workflow Configuration

```yaml
name: string          # Workflow name
steps: array          # List of steps
schedule: string      # Optional cron schedule
enabled: boolean      # Whether enabled
timeout: integer      # Timeout in seconds
retries: integer      # Number of retries
```

### Step Configuration

```yaml
id: string           # Step ID
action: string       # Action to execute
parameters: object   # Action parameters
condition: string    # Optional condition
timeout: integer     # Step timeout
```

## Error Handling

### Exceptions

- `WorkflowError`: General workflow error
- `ScheduleError`: Scheduling error
- `ExecutionError`: Execution error
- `TimeoutError`: Execution timeout

### Error Recovery

Workflows support automatic retry with exponential backoff:

```python
workflow = {
    "name": "WithRetry",
    "retries": 3,
    "retry_delay": 5,
    "steps": [...]
}
```
