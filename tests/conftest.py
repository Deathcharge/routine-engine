"""
Pytest Configuration and Fixtures for Routine Engine

Provides comprehensive fixtures, mocks, and utilities for testing
the coordination and execution engines.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, MagicMock
from typing import Dict, Any, List


# =============================================================================
# EVENT LOOP FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# =============================================================================
# COORDINATION ENGINE FIXTURES
# =============================================================================

@pytest.fixture
def mock_coordination_engine():
    """Mock CoordinationEngine."""
    engine = AsyncMock()
    engine.engine_id = "coord_001"
    engine.cycle_count = 0
    engine.current_step = 0
    
    # Methods
    engine.initialize = AsyncMock(return_value=True)
    engine.start_cycle = AsyncMock(return_value={"cycle_id": "cycle_001"})
    engine.execute_step = AsyncMock(return_value={"step": 1, "status": "success"})
    engine.complete_cycle = AsyncMock(return_value={"status": "completed"})
    engine.get_status = AsyncMock(return_value={"status": "active"})
    engine.get_metrics = AsyncMock(return_value={"harmony": 0.8, "resilience": 0.7})
    
    return engine


@pytest.fixture
def coordination_config() -> Dict[str, Any]:
    """Coordination engine configuration."""
    return {
        "engine_id": "coord_001",
        "cycle_length": 108,
        "max_cycles": 1000,
        "timeout": 300,
        "enable_logging": True,
    }


@pytest.fixture
def sample_coordination_cycle() -> Dict[str, Any]:
    """Sample coordination cycle."""
    return {
        "cycle_id": "cycle_001",
        "steps": 108,
        "state": "anomaly",
        "ucf_metrics": {
            "harmony": 0.0,
            "resilience": 0.0,
            "focus": 0.5,
            "throughput": 0.0,
            "friction": 1.0,
            "prana": 0.0,
            "drishti": 0.0,
            "klesha": 1.0
        },
        "timeout": 300
    }


# =============================================================================
# EXECUTION ENGINE FIXTURES
# =============================================================================

@pytest.fixture
def mock_execution_engine():
    """Mock ExecutionEngine."""
    engine = AsyncMock()
    engine.engine_id = "exec_001"
    engine.task_count = 0
    
    # Methods
    engine.initialize = AsyncMock(return_value=True)
    engine.execute_task = AsyncMock(return_value={"status": "success"})
    engine.execute_agent_action = AsyncMock(return_value={"status": "success"})
    engine.get_task_status = AsyncMock(return_value={"status": "running"})
    engine.cancel_task = AsyncMock(return_value=True)
    engine.get_metrics = AsyncMock(return_value={"tasks_completed": 100})
    
    return engine


@pytest.fixture
def execution_config() -> Dict[str, Any]:
    """Execution engine configuration."""
    return {
        "engine_id": "exec_001",
        "max_tasks": 1000,
        "timeout": 60,
        "enable_logging": True,
    }


@pytest.fixture
def sample_task() -> Dict[str, Any]:
    """Sample task for execution."""
    return {
        "task_id": "task_001",
        "agent_id": "agent_001",
        "action": "process_data",
        "payload": {"data": "sample"},
        "priority": "high",
        "timeout": 30
    }


# =============================================================================
# UCF METRICS FIXTURES
# =============================================================================

@pytest.fixture
def ucf_metrics() -> Dict[str, float]:
    """UCF metrics."""
    return {
        "harmony": 0.5,
        "resilience": 0.6,
        "focus": 0.7,
        "throughput": 0.4,
        "friction": 0.3,
        "prana": 0.5,
        "drishti": 0.6,
        "klesha": 0.2
    }


@pytest.fixture
def mock_ucf_adapter():
    """Mock UCF adapter."""
    adapter = AsyncMock()
    
    # Methods
    adapter.collect_metrics = AsyncMock(return_value={
        "harmony": 0.8,
        "resilience": 0.7
    })
    adapter.update_metrics = AsyncMock(return_value=True)
    adapter.get_metrics = AsyncMock(return_value={
        "harmony": 0.8,
        "resilience": 0.7,
        "focus": 0.6,
        "throughput": 0.5
    })
    
    return adapter


# =============================================================================
# COORDINATION STATE FIXTURES
# =============================================================================

@pytest.fixture
def coordination_states() -> List[str]:
    """Coordination states."""
    return ["anomaly", "legend", "chant", "hymn", "law"]


@pytest.fixture
def sample_coordination_entry() -> Dict[str, Any]:
    """Sample coordination entry."""
    return {
        "entry_id": "entry_001",
        "cycle_id": "cycle_001",
        "step": 1,
        "state": "anomaly",
        "agents": ["agent_001", "agent_002"],
        "metrics": {
            "harmony": 0.0,
            "resilience": 0.0
        },
        "timestamp": "2024-04-09T00:00:00Z"
    }


# =============================================================================
# AGENT FIXTURES
# =============================================================================

@pytest.fixture
def mock_agent():
    """Mock Agent."""
    agent = AsyncMock()
    agent.agent_id = "agent_001"
    agent.name = "Test Agent"
    agent.role = "worker"
    
    # Methods
    agent.initialize = AsyncMock(return_value=True)
    agent.execute_action = AsyncMock(return_value={"status": "success"})
    agent.get_status = AsyncMock(return_value={"status": "active"})
    
    return agent


@pytest.fixture
def sample_agent_action() -> Dict[str, Any]:
    """Sample agent action."""
    return {
        "action_id": "action_001",
        "agent_id": "agent_001",
        "action_type": "process",
        "payload": {"data": "test"},
        "priority": "high"
    }


# =============================================================================
# MONITORING FIXTURES
# =============================================================================

@pytest.fixture
def mock_metrics_collector():
    """Mock MetricsCollector."""
    collector = Mock()
    collector.metrics = {}
    collector.events = []
    
    def record_metric(name: str, value: float):
        collector.metrics[name] = value
    
    def record_event(event_type: str, data: Dict):
        collector.events.append({"type": event_type, "data": data})
    
    def get_metrics():
        return collector.metrics
    
    def get_events():
        return collector.events
    
    collector.record_metric = record_metric
    collector.record_event = record_event
    collector.get_metrics = get_metrics
    collector.get_events = get_events
    
    return collector


@pytest.fixture
def sample_metrics() -> Dict[str, Any]:
    """Sample metrics data."""
    return {
        "harmony": 0.8,
        "resilience": 0.7,
        "focus": 0.6,
        "throughput": 0.5,
        "friction": 0.2,
        "prana": 0.7,
        "drishti": 0.8,
        "klesha": 0.1
    }


# =============================================================================
# HEALTH MONITORING FIXTURES
# =============================================================================

@pytest.fixture
def mock_health_monitor():
    """Mock HealthMonitor."""
    monitor = AsyncMock()
    
    # Methods
    monitor.check_engine_health = AsyncMock(return_value={
        "status": "healthy",
        "cycles_completed": 100,
        "uptime": 3600
    })
    monitor.check_component_health = AsyncMock(return_value={
        "status": "healthy"
    })
    monitor.get_health_report = AsyncMock(return_value={
        "status": "healthy",
        "components": {}
    })
    
    return monitor


# =============================================================================
# EXCEPTION HANDLER FIXTURES
# =============================================================================

@pytest.fixture
def mock_exception_handler():
    """Mock ExceptionHandler."""
    handler = AsyncMock()
    handler.errors = []
    
    async def handle(error: Exception) -> Dict[str, Any]:
        handler.errors.append(error)
        return {"handled": True, "error": str(error)}
    
    def get_error_count() -> int:
        return len(handler.errors)
    
    handler.handle = handle
    handler.get_error_count = get_error_count
    
    return handler


# =============================================================================
# PERFORMANCE TESTING FIXTURES
# =============================================================================

@pytest.fixture
def performance_timer():
    """Performance timer for benchmarking."""
    import time
    
    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None
        
        def start(self):
            self.start_time = time.time()
        
        def stop(self) -> float:
            self.end_time = time.time()
            return self.end_time - self.start_time
    
    return Timer()


# =============================================================================
# PYTEST MARKERS
# =============================================================================

def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "unit: unit tests")
    config.addinivalue_line("markers", "integration: integration tests")
    config.addinivalue_line("markers", "slow: slow tests")
    config.addinivalue_line("markers", "asyncio: async tests")


# =============================================================================
# PYTEST HOOKS
# =============================================================================

@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset mocks before each test."""
    yield
    # Cleanup happens here


@pytest.fixture
def mock_api_response():
    """Mock API response."""
    def _mock_response(status_code=200, json_data=None, text_data=""):
        response = Mock()
        response.status_code = status_code
        response.json = Mock(return_value=json_data or {})
        response.text = text_data
        return response
    
    return _mock_response
