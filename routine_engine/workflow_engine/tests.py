"""
Helix Workflow Engine - Tests
==============================

Unit and integration tests for the workflow engine.
"""

import pytest

from apps.backend.workflow_engine import (
    NodeType,
    Workflow,
    WorkflowEdge,
    WorkflowEngine,
    WorkflowNode,
    WorkflowStatus,
)


@pytest.fixture
def engine():
    """Create a workflow engine instance"""
    return WorkflowEngine()


@pytest.fixture
def simple_workflow():
    """Create a simple workflow for testing"""
    return Workflow(
        id="test-workflow",
        name="Test Workflow",
        description="A simple test workflow",
        nodes=[
            WorkflowNode(id="node1", type=NodeType.WEBHOOK, name="Webhook", config={}),
            WorkflowNode(
                id="node2",
                type=NodeType.AGENT,
                name="Agent",
                config={"agent_type": "kael"},
            ),
        ],
        edges=[WorkflowEdge(id="edge1", source="node1", target="node2")],
    )


class TestWorkflowEngine:
    """Tests for WorkflowEngine"""

    def test_create_engine(self, engine):
        """Test engine creation"""
        assert engine is not None
        assert engine._workflows == {}
        assert engine._executions == {}

    def test_register_workflow(self, engine, simple_workflow):
        """Test workflow registration"""
        engine.register_workflow(simple_workflow)

        assert "test-workflow" in engine._workflows
        assert engine.get_workflow("test-workflow") == simple_workflow

    def test_get_nonexistent_workflow(self, engine):
        """Test getting non-existent workflow"""
        workflow = engine.get_workflow("nonexistent")
        assert workflow is None

    def test_list_workflows(self, engine, simple_workflow):
        """Test listing workflows"""
        engine.register_workflow(simple_workflow)

        workflows = engine.list_workflows()
        assert len(workflows) == 1
        assert workflows[0].id == "test-workflow"

    def test_list_workflows_by_status(self, engine, simple_workflow):
        """Test listing workflows by status"""
        simple_workflow.status = WorkflowStatus.ACTIVE
        engine.register_workflow(simple_workflow)

        active_workflows = engine.list_workflows(status=WorkflowStatus.ACTIVE)
        assert len(active_workflows) == 1

        inactive_workflows = engine.list_workflows(status=WorkflowStatus.INACTIVE)
        assert len(inactive_workflows) == 0


class TestWorkflow:
    """Tests for Workflow"""

    def test_workflow_creation(self):
        """Test workflow creation"""
        workflow = Workflow(id="test", name="Test", description="Test workflow", nodes=[], edges=[])

        assert workflow.id == "test"
        assert workflow.name == "Test"
        assert workflow.description == "Test workflow"
        assert workflow.status == WorkflowStatus.INACTIVE

    def test_workflow_to_dict(self):
        """Test workflow serialization"""
        workflow = Workflow(
            id="test",
            name="Test",
            description="Test",
            nodes=[WorkflowNode(id="node1", type=NodeType.WEBHOOK, name="Webhook")],
            edges=[WorkflowEdge(id="edge1", source="node1", target="node2")],
        )

        data = workflow.to_dict()

        assert data["id"] == "test"
        assert data["name"] == "Test"
        assert len(data["nodes"]) == 1
        assert len(data["edges"]) == 1


class TestWorkflowNode:
    """Tests for WorkflowNode"""

    def test_node_creation(self):
        """Test node creation"""
        node = WorkflowNode(id="node1", type=NodeType.WEBHOOK, name="Webhook", config={"path": "/test"})

        assert node.id == "node1"
        assert node.type == NodeType.WEBHOOK
        assert node.name == "Webhook"
        assert node.config == {"path": "/test"}

    def test_node_to_dict(self):
        """Test node serialization"""
        node = WorkflowNode(id="node1", type=NodeType.AGENT, name="Agent", config={"agent_type": "kael"})

        data = node.to_dict()

        assert data["id"] == "node1"
        assert data["type"] == "agent"
        assert data["name"] == "Agent"
        assert data["config"] == {"agent_type": "kael"}


class TestWorkflowEdge:
    """Tests for WorkflowEdge"""

    def test_edge_creation(self):
        """Test edge creation"""
        edge = WorkflowEdge(id="edge1", source="node1", target="node2", condition="data.status == 200")

        assert edge.id == "edge1"
        assert edge.source == "node1"
        assert edge.target == "node2"
        assert edge.condition == "data.status == 200"

    def test_edge_to_dict(self):
        """Test edge serialization"""
        edge = WorkflowEdge(id="edge1", source="node1", target="node2")

        data = edge.to_dict()

        assert data["id"] == "edge1"
        assert data["source"] == "node1"
        assert data["target"] == "node2"
        assert data["condition"] is None


@pytest.mark.asyncio
class TestWorkflowExecution:
    """Tests for workflow execution"""

    async def test_execute_simple_workflow(self, engine, simple_workflow):
        """Test executing a simple workflow"""
        engine.register_workflow(simple_workflow)

        execution = await engine.execute_workflow(workflow_id="test-workflow", input_data={"test": "data"})

        assert execution is not None
        assert execution.workflow_id == "test-workflow"
        assert execution.input_data == {"test": "data"}

    async def test_execute_nonexistent_workflow(self, engine):
        """Test executing non-existent workflow"""
        with pytest.raises(ValueError, match="Workflow not found"):
            await engine.execute_workflow("nonexistent")

    async def test_get_execution(self, engine, simple_workflow):
        """Test getting execution by ID"""
        engine.register_workflow(simple_workflow)

        execution = await engine.execute_workflow(workflow_id="test-workflow", input_data={})

        retrieved = engine.get_execution(execution.id)

        assert retrieved is not None
        assert retrieved.id == execution.id
        assert retrieved.workflow_id == "test-workflow"

    async def test_list_executions(self, engine, simple_workflow):
        """Test listing executions"""
        engine.register_workflow(simple_workflow)

        await engine.execute_workflow(workflow_id="test-workflow", input_data={})
        await engine.execute_workflow(workflow_id="test-workflow", input_data={})

        executions = engine.list_executions(workflow_id="test-workflow")

        assert len(executions) == 2

    async def test_execution_tracking(self, engine, simple_workflow):
        """Test execution has proper tracking"""
        engine.register_workflow(simple_workflow)

        execution = await engine.execute_workflow(workflow_id="test-workflow", input_data={})

        assert execution.start_time is not None
        assert execution.id is not None
        assert len(execution.node_executions) >= 0


class TestIntegrations:
    """Tests for integrations"""

    def test_integration_registry(self):
        """Test integration registry creation"""
        from apps.backend.workflow_engine.integrations import IntegrationRegistry

        registry = IntegrationRegistry()

        assert len(registry._integrations) > 0
        assert "slack" in registry._integrations

    def test_get_integration(self):
        """Test getting integration by name"""
        from apps.backend.workflow_engine.integrations import IntegrationRegistry

        registry = IntegrationRegistry()

        slack = registry.get("slack")

        assert slack is not None
        assert slack.name == "slack"

    def test_list_integrations_by_category(self):
        """Test listing integrations by category"""
        from apps.backend.workflow_engine.integrations import IntegrationRegistry

        registry = IntegrationRegistry()

        saas_integrations = registry.list_by_category("saas")

        assert len(saas_integrations) > 0
        assert all(i.category == "saas" for i in saas_integrations)

    def test_popular_integrations(self):
        """Test popular integrations list"""
        from apps.backend.workflow_engine.integrations import get_popular_integrations

        popular = get_popular_integrations()

        assert len(popular) > 0
        assert "slack" in popular
        assert "github" in popular


# Run tests if executed directly
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
