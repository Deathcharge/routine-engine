"""Basic workflow example."""

from routine_engine import WorkflowEngine

# Create engine
engine = WorkflowEngine()

# Define workflow
workflow = {
    "name": "BasicWorkflow",
    "steps": [
        {"id": "step-1", "action": "analyze_data"},
        {"id": "step-2", "action": "generate_report"}
    ]
}

# Create workflow
workflow_id = engine.create_workflow(workflow)
print(f"Created workflow: {workflow_id}")

# Execute workflow
result = engine.execute_workflow(workflow_id)
print(f"Execution result: {result}")
