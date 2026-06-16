"""Comprehensive pytest configuration and fixtures for routine-engine."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta


# ============================================================================
# Workflow Fixtures
# ============================================================================

@pytest.fixture
def mock_workflow():
    """Mock workflow."""
    workflow = MagicMock()
    workflow.id = "workflow-1"
    workflow.name = "TestWorkflow"
    workflow.status = "active"
    workflow.execute = MagicMock(return_value={"result": "success"})
    workflow.get_status = MagicMock(return_value="active")
    return workflow


@pytest.fixture
def mock_workflow_config():
    """Mock workflow configuration."""
    return {
        "name": "TestWorkflow",
        "steps": [
            {"id": "step-1", "type": "agent", "action": "analyze"},
            {"id": "step-2", "type": "agent", "action": "plan"}
        ],
        "schedule": "0 0 * * *",
        "enabled": True
    }


# ============================================================================
# Task Fixtures
# ============================================================================

@pytest.fixture
def mock_task():
    """Mock task."""
    task = MagicMock()
    task.id = "task-1"
    task.name = "TestTask"
    task.status = "pending"
    task.execute = MagicMock(return_value={"result": "success"})
    task.get_result = MagicMock(return_value={"output": "data"})
    return task


@pytest.fixture
def mock_task_config():
    """Mock task configuration."""
    return {
        "name": "TestTask",
        "action": "execute_analysis",
        "parameters": {"input": "data"},
        "timeout": 300,
        "retries": 3
    }


# ============================================================================
# Schedule Fixtures
# ============================================================================

@pytest.fixture
def mock_schedule():
    """Mock schedule."""
    schedule = MagicMock()
    schedule.id = "schedule-1"
    schedule.cron = "0 0 * * *"
    schedule.enabled = True
    schedule.next_run = datetime.now() + timedelta(hours=1)
    return schedule


@pytest.fixture
def mock_schedule_config():
    """Mock schedule configuration."""
    return {
        "cron": "0 0 * * *",
        "timezone": "UTC",
        "enabled": True,
        "max_instances": 1
    }


# ============================================================================
# Agent Fixtures
# ============================================================================

@pytest.fixture
def mock_agent():
    """Mock agent."""
    agent = MagicMock()
    agent.id = "agent-1"
    agent.name = "TestAgent"
    agent.execute = MagicMock(return_value={"result": "success"})
    agent.get_state = MagicMock(return_value={"status": "ready"})
    return agent


@pytest.fixture
def mock_agents_list():
    """Mock list of agents."""
    agents = []
    for i in range(3):
        agent = MagicMock()
        agent.id = f"agent-{i}"
        agent.name = f"Agent{i}"
        agents.append(agent)
    return agents


# ============================================================================
# Engine Fixtures
# ============================================================================

@pytest.fixture
def mock_workflow_engine():
    """Mock workflow engine."""
    engine = MagicMock()
    engine.create_workflow = MagicMock(return_value="workflow-1")
    engine.execute_workflow = MagicMock(return_value={"result": "success"})
    engine.get_workflow = MagicMock(return_value=MagicMock())
    engine.list_workflows = MagicMock(return_value=[])
    engine.delete_workflow = MagicMock(return_value=True)
    return engine


@pytest.fixture
def mock_scheduler():
    """Mock scheduler."""
    scheduler = MagicMock()
    scheduler.schedule_workflow = MagicMock(return_value="schedule-1")
    scheduler.get_schedule = MagicMock(return_value=MagicMock())
    scheduler.list_schedules = MagicMock(return_value=[])
    scheduler.cancel_schedule = MagicMock(return_value=True)
    return scheduler


# ============================================================================
# Execution Fixtures
# ============================================================================

@pytest.fixture
def mock_execution_result():
    """Mock execution result."""
    return {
        "id": "exec-1",
        "workflow_id": "workflow-1",
        "status": "success",
        "result": {"output": "data"},
        "duration": 1.5,
        "timestamp": datetime.now().isoformat()
    }


@pytest.fixture
def mock_execution_history():
    """Mock execution history."""
    return [
        {"id": "exec-1", "status": "success", "duration": 1.5},
        {"id": "exec-2", "status": "success", "duration": 2.0},
        {"id": "exec-3", "status": "failed", "duration": 0.5}
    ]


# ============================================================================
# Scenario Fixtures
# ============================================================================

@pytest.fixture
def workflow_execution_scenario():
    """Workflow execution scenario."""
    return {
        "workflow_id": "workflow-1",
        "steps": 5,
        "expected_duration": 10.0,
        "timeout": 30,
        "should_succeed": True
    }


@pytest.fixture
def scheduled_workflow_scenario():
    """Scheduled workflow scenario."""
    return {
        "workflow_id": "workflow-1",
        "schedule": "0 0 * * *",
        "timezone": "UTC",
        "next_run": datetime.now() + timedelta(hours=1),
        "enabled": True
    }


@pytest.fixture
def error_recovery_scenario():
    """Error recovery scenario."""
    return {
        "error_type": "timeout",
        "retry_count": 3,
        "retry_delay": 5,
        "should_recover": True
    }
