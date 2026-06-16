"""
Helix Workflow Engine - Example Workflows
==========================================

Demonstrates various workflow patterns and capabilities.
"""

import asyncio
import json
import logging

from apps.backend.workflow_engine import (
    NodeType,
    Workflow,
    WorkflowEdge,
    WorkflowEngine,
    WorkflowNode,
)

logger = logging.getLogger(__name__)


async def example_1_simple_webhook_to_agent():
    """Example 1: Webhook → Agent → HTTP Request"""

    logger.info("\n=== Example 1: Simple Webhook to Agent ===\n")

    engine = WorkflowEngine()

    workflow = Workflow(
        id="example-1",
        name="Webhook to Agent Workflow",
        description="Receive webhook, analyze with Kael, send to API",
        nodes=[
            WorkflowNode(
                id="webhook-1",
                type=NodeType.WEBHOOK,
                name="Webhook Trigger",
                config={"path": "/analyze"},
            ),
            WorkflowNode(
                id="kael-1",
                type=NodeType.AGENT,
                name="Kael Analysis",
                config={
                    "agent_type": "kael",
                    "task": "Analyze the incoming data and identify key insights",
                },
            ),
            WorkflowNode(
                id="http-1",
                type=NodeType.HTTP_REQUEST,
                name="Send Analysis",
                config={
                    "url": "https://api.example.com/analysis",
                    "method": "POST",
                    "headers": {"Content-Type": "application/json"},
                },
            ),
        ],
        edges=[
            WorkflowEdge(id="e1", source="webhook-1", target="kael-1"),
            WorkflowEdge(id="e2", source="kael-1", target="http-1"),
        ],
    )

    engine.register_workflow(workflow)

    # Execute
    execution = await engine.execute_workflow(
        workflow_id="example-1",
        input_data={
            "message": "This is important data that needs analysis",
            "timestamp": "2024-01-22T10:00:00Z",
            "source": "user",
        },
    )

    logger.info("Status: %s", execution.status.value)
    execution_time = execution.end_time - execution.start_time if execution.end_time else None
    logger.info("Execution Time: %s", execution_time)
    logger.info("Output: %s", json.dumps(execution.output_data, indent=2))

    return execution


async def example_2_multi_agent_collaboration():
    """Example 2: Kael → Lumina → Vega collaboration"""

    logger.info("\n=== Example 2: Multi-Agent Collaboration ===\n")

    engine = WorkflowEngine()

    workflow = Workflow(
        id="example-2",
        name="Multi-Agent Workflow",
        description="Kael analyzes, Lumina synthesizes, Vega executes",
        nodes=[
            WorkflowNode(
                id="kael-2",
                type=NodeType.AGENT,
                name="Kael - Analysis",
                config={
                    "agent_type": "kael",
                    "task": "Analyze the market data and identify trends",
                },
            ),
            WorkflowNode(
                id="lumina-2",
                type=NodeType.AGENT,
                name="Lumina - Synthesis",
                config={
                    "agent_type": "lumina",
                    "task": "Create a compelling summary of the analysis",
                },
            ),
            WorkflowNode(
                id="vega-2",
                type=NodeType.AGENT,
                name="Vega - Execution",
                config={
                    "agent_type": "vega",
                    "task": "Execute the marketing strategy based on the summary",
                },
            ),
        ],
        edges=[
            WorkflowEdge(id="e1", source="kael-2", target="lumina-2"),
            WorkflowEdge(id="e2", source="lumina-2", target="vega-2"),
        ],
    )

    engine.register_workflow(workflow)

    execution = await engine.execute_workflow(
        workflow_id="example-2",
        input_data={
            "market_data": {
                "trend": "upward",
                "volume": 1000000,
                "sentiment": "positive",
            }
        },
    )

    logger.info("Status: %s", execution.status.value)
    logger.info("Node Executions: %s", len(execution.node_executions))

    for i, step in enumerate(execution.node_executions):
        logger.info("\nStep %s: %s", i + 1, step["name"])
        logger.info("  Status: %s", step["status"])
        logger.info("  Time: %.2fms", step["execution_time_ms"])

    return execution


async def example_3_conditional_branching():
    """Example 3: Conditional branching based on data"""

    logger.info("\n=== Example 3: Conditional Branching ===\n")

    engine = WorkflowEngine()

    workflow = Workflow(
        id="example-3",
        name="Conditional Workflow",
        description="Branch based on data value",
        nodes=[
            WorkflowNode(
                id="input-3",
                type=NodeType.TRANSFORM,
                name="Data Input",
                config={"transformation_type": "json_path", "expression": "$.status"},
            ),
            WorkflowNode(
                id="condition-3",
                type=NodeType.CONDITION,
                name="Check Status",
                config={"condition": "data == 'success'"},
            ),
            WorkflowNode(
                id="success-action",
                type=NodeType.HTTP_REQUEST,
                name="Success Action",
                config={"url": "https://api.example.com/success", "method": "POST"},
            ),
            WorkflowNode(
                id="error-action",
                type=NodeType.HTTP_REQUEST,
                name="Error Action",
                config={"url": "https://api.example.com/error", "method": "POST"},
            ),
        ],
        edges=[
            WorkflowEdge(id="e1", source="input-3", target="condition-3"),
            WorkflowEdge(
                id="e2-success",
                source="condition-3",
                target="success-action",
                condition="data == 'success'",
            ),
            WorkflowEdge(
                id="e2-error",
                source="condition-3",
                target="error-action",
                condition="data != 'success'",
            ),
        ],
    )

    engine.register_workflow(workflow)

    # Test with success
    logger.info("Testing with success status...")
    execution = await engine.execute_workflow(
        workflow_id="example-3",
        input_data={"status": "success", "message": "Operation succeeded"},
    )
    logger.info("Status: %s", execution.status.value)

    return execution


async def example_4_data_transformation():
    """Example 4: Complex data transformation"""

    logger.info("\n=== Example 4: Data Transformation ===\n")

    engine = WorkflowEngine()

    workflow = Workflow(
        id="example-4",
        name="Data Transformation",
        description="Transform and filter data",
        nodes=[
            WorkflowNode(
                id="extract-4",
                type=NodeType.TRANSFORM,
                name="Extract Fields",
                config={
                    "transformation_type": "json_path",
                    "expression": "$.items[*].{name, price}",
                },
            ),
            WorkflowNode(
                id="filter-4",
                type=NodeType.CODE,
                name="Filter High Value",
                config={
                    "language": "python",
                    "code": """
# Filter items with price > 100
items = input_data
high_value_items = [item for item in items if item.get('price', 0) > 100]
result = high_value_items
""",
                },
            ),
            WorkflowNode(
                id="map-4",
                type=NodeType.TRANSFORM,
                name="Map to Output Format",
                config={"transformation_type": "map", "expression": "name,price"},
            ),
        ],
        edges=[
            WorkflowEdge(id="e1", source="extract-4", target="filter-4"),
            WorkflowEdge(id="e2", source="filter-4", target="map-4"),
        ],
    )

    engine.register_workflow(workflow)

    execution = await engine.execute_workflow(
        workflow_id="example-4",
        input_data={
            "items": [
                {"name": "Product A", "price": 50},
                {"name": "Product B", "price": 150},
                {"name": "Product C", "price": 200},
                {"name": "Product D", "price": 75},
            ]
        },
    )

    logger.info("Status: %s", execution.status.value)
    logger.info("Output: %s", json.dumps(execution.output_data, indent=2))

    return execution


async def example_5_parallel_processing():
    """Example 5: Parallel processing with multiple agents"""

    logger.info("\n=== Example 5: Parallel Processing ===\n")

    engine = WorkflowEngine()

    workflow = Workflow(
        id="example-5",
        name="Parallel Processing",
        description="Run multiple agents in parallel",
        nodes=[
            WorkflowNode(
                id="kael-5a",
                type=NodeType.AGENT,
                name="Kael - Data Analysis",
                config={"agent_type": "kael", "task": "Analyze the data for trends"},
            ),
            WorkflowNode(
                id="lumina-5a",
                type=NodeType.AGENT,
                name="Lumina - Content Creation",
                config={
                    "agent_type": "lumina",
                    "task": "Create content based on the data",
                },
            ),
            WorkflowNode(
                id="vega-5a",
                type=NodeType.AGENT,
                name="Vega - Task Planning",
                config={
                    "agent_type": "vega",
                    "task": "Plan next steps based on the data",
                },
            ),
            WorkflowNode(
                id="combine-5",
                type=NodeType.TRANSFORM,
                name="Combine Results",
                config={"transformation_type": "map", "expression": "*"},
            ),
        ],
        edges=[
            WorkflowEdge(id="e1a", source="kael-5a", target="combine-5"),
            WorkflowEdge(id="e1b", source="lumina-5a", target="combine-5"),
            WorkflowEdge(id="e1c", source="vega-5a", target="combine-5"),
        ],
    )

    engine.register_workflow(workflow)

    execution = await engine.execute_workflow(
        workflow_id="example-5",
        input_data={"data": "Sample data for parallel processing"},
    )

    logger.info("Status: %s", execution.status.value)
    logger.info("Parallel steps executed")

    return execution


async def example_6_integration_with_external_api():
    """Example 6: Integration with external API"""

    logger.info("\n=== Example 6: External API Integration ===\n")

    engine = WorkflowEngine()

    workflow = Workflow(
        id="example-6",
        name="API Integration",
        description="Call external API and process response",
        nodes=[
            WorkflowNode(
                id="http-6a",
                type=NodeType.HTTP_REQUEST,
                name="Fetch Data",
                config={
                    "url": "https://jsonplaceholder.typicode.com/posts/1",
                    "method": "GET",
                },
            ),
            WorkflowNode(
                id="transform-6",
                type=NodeType.TRANSFORM,
                name="Extract Title",
                config={
                    "transformation_type": "json_path",
                    "expression": "$.{title, userId}",
                },
            ),
            WorkflowNode(
                id="kael-6",
                type=NodeType.AGENT,
                name="Kael - Process",
                config={"agent_type": "kael", "task": "Summarize the fetched data"},
            ),
        ],
        edges=[
            WorkflowEdge(id="e1", source="http-6a", target="transform-6"),
            WorkflowEdge(id="e2", source="transform-6", target="kael-6"),
        ],
    )

    engine.register_workflow(workflow)

    execution = await engine.execute_workflow(workflow_id="example-6", input_data={})

    logger.info("Status: %s", execution.status.value)
    if execution.output_data:
        logger.info("Output: %s", json.dumps(execution.output_data, indent=2))

    return execution


async def main():
    """Run all examples"""

    logger.info("=" * 60)
    logger.info("Helix Workflow Engine - Examples")
    logger.info("=" * 60)

    try:
        # Run examples
        await example_1_simple_webhook_to_agent()
        await example_2_multi_agent_collaboration()
        await example_3_conditional_branching()
        await example_4_data_transformation()
        await example_5_parallel_processing()
        await example_6_integration_with_external_api()

        logger.info("\n" + "=" * 60)
        logger.info("All examples completed!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error("\nError running examples: %s", e)
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
