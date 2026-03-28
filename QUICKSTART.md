# Routine Engine - Quick Start Guide

Get up and running with Routine Engine in 5 minutes.

## Installation

```bash
# Clone the repository
git clone https://github.com/Deathcharge/routine-engine.git
cd routine-engine

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .
```

## Your First Workflow

### 1. Create a Simple Workflow

Create a file `my_first_workflow.py`:

```python
from routine_engine import Engine, Workflow, Action, Trigger

# Initialize the engine
engine = Engine()

# Create a workflow
workflow = Workflow(
    name="Hello Routine",
    description="My first workflow"
)

# Add a trigger (runs every minute)
workflow.add_trigger(Trigger(
    type="schedule",
    cron="* * * * *"  # Every minute
))

# Add an action (log a message)
workflow.add_action(Action(
    type="log_event",
    config={
        "level": "info",
        "message": "Hello from Routine Engine! 🚀"
    }
))

# Register and start
engine.register_workflow(workflow)
engine.start()
```

### 2. Run Your Workflow

```bash
python my_first_workflow.py
```

You should see logs indicating the workflow is running.

## Common Workflows

### Send a Discord Message

```python
workflow = Workflow(name="Discord Alert")

workflow.add_trigger(Trigger(
    type="schedule",
    cron="0 9 * * *"  # 9 AM daily
))

workflow.add_action(Action(
    type="send_discord",
    config={
        "webhook_url": "https://discord.com/api/webhooks/YOUR_WEBHOOK_URL",
        "message": "Good morning! Time to start the day! ☀️"
    }
))

engine.register_workflow(workflow)
```

### Conditional Logic

```python
from routine_engine import Condition, Node

workflow = Workflow(name="Smart Notification")

# Create a condition
condition = Condition(
    field="status",
    operator="equals",
    value="error"
)

# Add conditional node
workflow.add_node(Node(
    type="condition",
    condition=condition,
    on_true=[
        Action(type="send_discord", config={
            "webhook_url": "...",
            "message": "⚠️ Error detected!"
        }),
        Action(type="alert_agent", config={
            "priority": "high"
        })
    ],
    on_false=[
        Action(type="log_event", config={
            "level": "info",
            "message": "All systems operational"
        })
    ]
))

engine.register_workflow(workflow)
```

### Data Transformation

```python
workflow = Workflow(name="Data Pipeline")

workflow.add_action(Action(
    type="transform_data",
    config={
        "input": {"name": "John", "age": 30},
        "expression": "lambda x: {'user': x['name'], 'years': x['age']}"
    }
))

workflow.add_action(Action(
    type="send_webhook",
    config={
        "url": "https://api.example.com/users",
        "method": "POST",
        "data": "{{ previous_output }}"
    }
))

engine.register_workflow(workflow)
```

### AWS Integration

```python
workflow = Workflow(name="S3 Upload")

workflow.add_action(Action(
    type="aws_s3_upload",
    config={
        "bucket": "my-bucket",
        "key": "data/file.json",
        "data": {"message": "Hello S3!"},
        "aws_region": "us-east-1"
    }
))

engine.register_workflow(workflow)
```

### Airtable Integration

```python
workflow = Workflow(name="Airtable Sync")

workflow.add_action(Action(
    type="airtable_create_record",
    config={
        "api_key": "YOUR_AIRTABLE_API_KEY",
        "base_id": "YOUR_BASE_ID",
        "table_name": "Contacts",
        "fields": {
            "Name": "John Doe",
            "Email": "john@example.com",
            "Status": "Active"
        }
    }
))

engine.register_workflow(workflow)
```

## Advanced Features

### Error Handling

```python
workflow = Workflow(name="Robust Workflow")

workflow.add_action(Action(
    type="send_webhook",
    config={
        "url": "https://api.example.com/data",
        "method": "POST",
        "retry_count": 3,
        "retry_delay": 5,
        "timeout": 30,
        "on_error": {
            "action": "send_discord",
            "config": {
                "webhook_url": "...",
                "message": "API call failed after 3 retries"
            }
        }
    }
))

engine.register_workflow(workflow)
```

### Parallel Execution

```python
from routine_engine import ParallelNode

workflow = Workflow(name="Parallel Tasks")

parallel = ParallelNode(
    actions=[
        Action(type="send_discord", config={"webhook_url": "...", "message": "Task 1"}),
        Action(type="send_discord", config={"webhook_url": "...", "message": "Task 2"}),
        Action(type="send_discord", config={"webhook_url": "...", "message": "Task 3"}),
    ],
    max_workers=3
)

workflow.add_node(parallel)
engine.register_workflow(workflow)
```

### Loops

```python
from routine_engine import LoopNode

workflow = Workflow(name="Loop Example")

loop = LoopNode(
    items="{{ items_list }}",
    action=Action(
        type="send_discord",
        config={
            "webhook_url": "...",
            "message": "Processing: {{ item }}"
        }
    )
)

workflow.add_node(loop)
engine.register_workflow(workflow)
```

## Configuration

### Environment Variables

Create a `.env` file:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost/routine_engine

# Redis
REDIS_URL=redis://localhost:6379

# Credentials
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
AIRTABLE_API_KEY=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...

# Engine
MAX_WORKERS=10
LOG_LEVEL=INFO
```

### Load Configuration

```python
from routine_engine import Engine
import os

engine = Engine(
    database_url=os.getenv("DATABASE_URL"),
    redis_url=os.getenv("REDIS_URL"),
    max_workers=int(os.getenv("MAX_WORKERS", 10))
)
```

## Monitoring

### Check Workflow Status

```python
# Get execution status
status = engine.get_execution_status(workflow_id, execution_id)
print(f"Status: {status['state']}")
print(f"Progress: {status['progress']}%")

# Get execution history
history = engine.get_execution_history(workflow_id, limit=10)
for execution in history:
    print(f"{execution['started_at']}: {execution['state']}")
```

### Subscribe to Events

```python
from routine_engine import EventBus

event_bus = EventBus()

@event_bus.on("workflow.started")
def on_start(event):
    print(f"Workflow started: {event.workflow_id}")

@event_bus.on("workflow.completed")
def on_complete(event):
    print(f"Workflow completed: {event.workflow_id}")
    print(f"Duration: {event.duration}s")

@event_bus.on("workflow.failed")
def on_failure(event):
    print(f"Workflow failed: {event.workflow_id}")
    print(f"Error: {event.error}")
```

## Next Steps

1. **Explore Examples** - Check the `examples/` directory for more complex workflows
2. **Read Documentation** - Full API docs at [routine-engine.readthedocs.io](https://routine-engine.readthedocs.io)
3. **Join Community** - Connect with other users on Discord
4. **Contribute** - See [CONTRIBUTING.md](CONTRIBUTING.md) to contribute improvements

## Troubleshooting

### Workflow not executing
- Check that the trigger condition is met
- Verify the schedule cron expression
- Check logs for errors: `LOG_LEVEL=DEBUG python your_workflow.py`

### Database connection errors
- Verify DATABASE_URL is correct
- Ensure PostgreSQL is running
- Check credentials

### Action failures
- Verify API keys and credentials
- Check webhook URLs are correct
- Review action configuration

## Support

- 📖 [Full Documentation](https://routine-engine.readthedocs.io)
- 🐛 [Report Issues](https://github.com/Deathcharge/routine-engine/issues)
- 💬 [Discord Community](https://discord.gg/...)
- 📧 [Email Support](mailto:support@routine-engine.dev)

Happy automating! 🚀
