"""Scheduled workflow example."""

from routine_engine import WorkflowEngine, Scheduler

# Create engine and scheduler
engine = WorkflowEngine()
scheduler = Scheduler()

# Define workflow
workflow = {
    "name": "DailyAnalysis",
    "steps": [
        {"id": "1", "action": "fetch_data"},
        {"id": "2", "action": "analyze"},
        {"id": "3", "action": "report"}
    ]
}

# Create workflow
workflow_id = engine.create_workflow(workflow)

# Schedule daily at midnight
schedule_id = scheduler.schedule_workflow(
    workflow_id,
    cron="0 0 * * *",
    timezone="UTC"
)

print(f"Scheduled workflow: {schedule_id}")
