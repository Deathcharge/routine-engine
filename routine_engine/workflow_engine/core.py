"""
Helix Workflow Engine - n8n Competitor
========================================

A visual workflow automation engine built on Helix Chains.

Features:
- Visual node-based workflow builder
- 500+ integrations via LangChain-style tools
- Conditional branching and routing
- Parallel execution
- Agent orchestration (Kael/Lumina/Vega)
- Webhook triggers
- Scheduling
- Data transformations
- Error handling and retries
"""

import asyncio
import ipaddress
import json
import logging
import socket
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, cast
from urllib.parse import urlparse

from apps.backend.core.redis_client import get_redis
from apps.backend.helix_flow.agents import Agent
from apps.backend.helix_flow.chains import (
    Chain,
    ChainContext,
    ChainResult,
    ParallelChain,
    RetryChain,
    RouterChain,
    SequentialChain,
    resolve_node_reference,
)
from apps.backend.helix_flow.tools import Tool, ToolRegistry
from apps.backend.utils.safe_eval import SafeEvaluator

logger = logging.getLogger(__name__)


class NodeType(Enum):
    """Types of workflow nodes"""

    TRIGGER = "trigger"
    AGENT = "agent"
    ACTION = "action"
    CONDITION = "condition"
    TRANSFORM = "transform"
    WEBHOOK = "webhook"
    SCHEDULE = "schedule"
    HTTP_REQUEST = "http_request"
    DATABASE = "database"
    CODE = "code"
    SUB_WORKFLOW = "sub_workflow"


class WorkflowStatus(Enum):
    """Workflow execution status"""

    ACTIVE = "active"
    INACTIVE = "inactive"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"


@dataclass
class WorkflowNode:
    """A node in the workflow graph"""

    id: str
    type: NodeType
    name: str
    config: dict[str, Any] = field(default_factory=dict)
    position: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type.value,
            "name": self.name,
            "config": self.config,
            "position": self.position,
        }


@dataclass
class WorkflowEdge:
    """Connection between workflow nodes"""

    id: str
    source: str
    target: str
    condition: str | None = None  # JavaScript expression

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "target": self.target,
            "condition": self.condition,
        }


@dataclass
class WorkflowExecution:
    """Record of a workflow execution"""

    id: str
    workflow_id: str
    status: WorkflowStatus
    start_time: datetime
    end_time: datetime | None = None
    input_data: dict[str, Any] = field(default_factory=dict)
    output_data: dict[str, Any] | None = None
    error: str | None = None
    node_executions: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "input_data": self.input_data,
            "output_data": self.output_data,
            "error": self.error,
            "node_executions": self.node_executions,
            "metadata": self.metadata,
        }


@dataclass
class Workflow:
    """A visual workflow definition"""

    id: str
    name: str
    description: str
    nodes: list[WorkflowNode]
    edges: list[WorkflowEdge]
    status: WorkflowStatus = WorkflowStatus.INACTIVE
    version: int = 1
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "status": self.status.value,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "tags": self.tags,
            "metadata": self.metadata,
        }


def resolve_config_refs(config: dict[str, Any], context: "ChainContext") -> dict[str, Any]:
    """Resolve {{ref}} template strings inside a workflow node config dict."""
    resolved: dict[str, Any] = {}
    for key, value in config.items():
        if isinstance(value, str):
            resolved[key] = resolve_node_reference(value, context)
        elif isinstance(value, dict):
            resolved[key] = resolve_config_refs(value, context)
        elif isinstance(value, list):
            resolved[key] = [resolve_node_reference(v, context) if isinstance(v, str) else v for v in value]
        else:
            resolved[key] = value
    return resolved


class WorkflowEngine:
    """
    Main workflow execution engine - n8n competitor.

    Converts visual workflows into executable Helix Chains.
    """

    # Bounds to prevent memory leaks
    _MAX_EXECUTIONS = 1000  # Keep last 1000 executions in memory
    _MAX_WORKFLOWS = 500  # Keep last 500 workflows in memory
    _WORKFLOWS_REDIS_KEY = "workflow_engine:workflows"
    _EXECUTIONS_REDIS_PREFIX = "workflow_engine:exec:"
    _EXECUTION_INDEX_REDIS_KEY = "workflow_engine:exec_index"
    _LEGACY_WORKFLOWS_REDIS_KEY = "legacy_workflow_engine:workflows"
    _LEGACY_EXECUTIONS_REDIS_PREFIX = "legacy_workflow_engine:exec:"
    _LEGACY_EXECUTION_INDEX_REDIS_KEY = "legacy_workflow_engine:exec_index"
    _WORKFLOW_TTL = 86400 * 30  # 30 days
    _EXECUTION_TTL = 86400 * 7  # 7 days

    def __init__(
        self,
        tool_registry: ToolRegistry | None = None,
        agent_registry: dict[str, Agent] | None = None,
        verbose: bool = False,
    ):
        self.tool_registry = tool_registry or ToolRegistry()
        self.agent_registry = agent_registry or {}
        self.verbose = verbose
        # Write-through cache over Redis
        self._executions: dict[str, WorkflowExecution] = {}
        # Write-through cache over Redis
        self._workflows: dict[str, Workflow] = {}
        self._redis_loaded = False
        # Track running execution tasks for cancellation
        self._running_tasks: dict[str, asyncio.Task] = {}
        # Cache compiled chains: (workflow_id, version) -> Chain
        self._chain_cache: dict[tuple[str, int], Chain] = {}
        self._chain_cache_max = 100

    @staticmethod
    def _serialize_workflow(workflow: Workflow) -> dict[str, Any]:
        return {
            "id": workflow.id,
            "name": workflow.name,
            "description": workflow.description,
            "nodes": [node.to_dict() for node in workflow.nodes],
            "edges": [edge.to_dict() for edge in workflow.edges],
            "status": workflow.status.value,
            "version": workflow.version,
            "created_at": workflow.created_at.isoformat(),
            "updated_at": workflow.updated_at.isoformat(),
            "tags": workflow.tags,
            "metadata": workflow.metadata,
        }

    @staticmethod
    def _deserialize_workflow(payload: dict[str, Any]) -> Workflow:
        return Workflow(
            id=payload["id"],
            name=payload["name"],
            description=payload.get("description", ""),
            nodes=[
                WorkflowNode(
                    id=node["id"],
                    type=NodeType(node["type"]),
                    name=node["name"],
                    config=node.get("config", {}),
                    position=node.get("position", {}),
                )
                for node in payload.get("nodes", [])
            ],
            edges=[
                WorkflowEdge(
                    id=edge["id"],
                    source=edge["source"],
                    target=edge["target"],
                    condition=edge.get("condition"),
                )
                for edge in payload.get("edges", [])
            ],
            status=WorkflowStatus(payload.get("status", WorkflowStatus.INACTIVE.value)),
            version=payload.get("version", 1),
            created_at=datetime.fromisoformat(payload.get("created_at", datetime.now(UTC).isoformat())),
            updated_at=datetime.fromisoformat(payload.get("updated_at", datetime.now(UTC).isoformat())),
            tags=payload.get("tags", []),
            metadata=payload.get("metadata", {}),
        )

    @staticmethod
    def _serialize_execution(execution: WorkflowExecution) -> dict[str, Any]:
        return {
            "id": execution.id,
            "workflow_id": execution.workflow_id,
            "status": execution.status.value,
            "start_time": execution.start_time.isoformat(),
            "end_time": execution.end_time.isoformat() if execution.end_time else None,
            "input_data": execution.input_data,
            "output_data": execution.output_data,
            "error": execution.error,
            "node_executions": execution.node_executions,
            "metadata": execution.metadata,
        }

    @staticmethod
    def _deserialize_execution(payload: dict[str, Any]) -> WorkflowExecution:
        end_time_raw = payload.get("end_time")
        return WorkflowExecution(
            id=payload["id"],
            workflow_id=payload["workflow_id"],
            status=WorkflowStatus(payload.get("status", WorkflowStatus.FAILED.value)),
            start_time=datetime.fromisoformat(payload["start_time"]),
            end_time=datetime.fromisoformat(end_time_raw) if end_time_raw else None,
            input_data=payload.get("input_data", {}),
            output_data=payload.get("output_data"),
            error=payload.get("error"),
            node_executions=payload.get("node_executions", []),
            metadata=payload.get("metadata", {}),
        )

    async def _redis_get_first_available(self, redis: Any, keys: list[str]) -> str | None:
        for key in keys:
            value = await redis.get(key)
            if value:
                return value
        return None

    async def _redis_get_execution_payload(self, redis: Any, execution_id: str) -> str | None:
        execution_keys = [
            f"{self._EXECUTIONS_REDIS_PREFIX}{execution_id}",
            f"{self._LEGACY_EXECUTIONS_REDIS_PREFIX}{execution_id}",
        ]
        return await self._redis_get_first_available(redis, execution_keys)

    async def _redis_write_compat_keys(self, redis: Any, keys: list[str], value: str, ttl: int) -> None:
        for key in keys:
            await redis.set(key, value, ex=ttl)

    async def _redis_load_state(self) -> None:
        if self._redis_loaded:
            return

        self._redis_loaded = True

        try:
            redis = await get_redis()
            if not redis:
                return

            raw_workflows = await self._redis_get_first_available(
                redis,
                [self._WORKFLOWS_REDIS_KEY, self._LEGACY_WORKFLOWS_REDIS_KEY],
            )
            if raw_workflows:
                for workflow_id, workflow_payload in json.loads(raw_workflows).items():
                    if workflow_id not in self._workflows:
                        self._workflows[workflow_id] = self._deserialize_workflow(workflow_payload)

            raw_execution_index = await self._redis_get_first_available(
                redis,
                [self._EXECUTION_INDEX_REDIS_KEY, self._LEGACY_EXECUTION_INDEX_REDIS_KEY],
            )
            if raw_execution_index:
                for execution_id in json.loads(raw_execution_index)[-self._MAX_EXECUTIONS :]:
                    if execution_id in self._executions:
                        continue

                    raw_execution = await self._redis_get_execution_payload(redis, execution_id)
                    if raw_execution:
                        self._executions[execution_id] = self._deserialize_execution(json.loads(raw_execution))

            self._evict_old_executions()
        except Exception as exc:
            logger.warning("Failed to load workflow engine state from Redis: %s", exc)

    async def _redis_save_workflows(self) -> None:
        try:
            redis = await get_redis()
            if not redis:
                return

            serialized = {
                workflow_id: self._serialize_workflow(workflow) for workflow_id, workflow in self._workflows.items()
            }
            serialized_payload = json.dumps(serialized, default=str)
            await self._redis_write_compat_keys(
                redis,
                [self._WORKFLOWS_REDIS_KEY, self._LEGACY_WORKFLOWS_REDIS_KEY],
                serialized_payload,
                self._WORKFLOW_TTL,
            )
        except Exception as exc:
            logger.warning("Failed to save workflow engine workflows to Redis: %s", exc)

    async def _publish_workflow_event(self, execution_id: str, event_type: str, workflow_name: str = "") -> None:
        """Publish a workflow execution event to the Redis pub/sub event bus.

        The event is published to ``helix:workflow:events:{execution_id}`` and
        also to the wild-card channel ``helix:workflow:events`` so that
        websocket subscribers receive all events regardless of execution id.
        Failures are logged at debug level — a missing Redis connection must
        not prevent the workflow engine from continuing.
        """
        try:
            from apps.backend.core.redis_client import get_redis

            redis = await get_redis()
            if not redis:
                return
            execution = self._executions.get(execution_id)
            payload = {
                "event": event_type,
                "execution_id": execution_id,
                "workflow_id": execution.workflow_id if execution else "",
                "workflow_name": workflow_name,
                "status": execution.status.value if execution else event_type,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            if execution and execution.error:
                payload["error"] = execution.error
            message = json.dumps(payload, default=str)
            channels = [
                f"helix:workflow:events:{execution_id}",
                "helix:workflow:events",
            ]
            for channel in channels:
                await redis.publish(channel, message)
            logger.debug("Workflow event published: %s on %s", event_type, channels)
        except Exception as exc:
            logger.debug("Workflow event publish failed (non-fatal): %s", exc)

    async def _redis_save_execution(self, execution_id: str) -> None:
        try:
            redis = await get_redis()
            if not redis:
                return

            execution = self._executions.get(execution_id)
            if not execution:
                return

            serialized_execution = json.dumps(self._serialize_execution(execution), default=str)
            await self._redis_write_compat_keys(
                redis,
                [
                    f"{self._EXECUTIONS_REDIS_PREFIX}{execution_id}",
                    f"{self._LEGACY_EXECUTIONS_REDIS_PREFIX}{execution_id}",
                ],
                serialized_execution,
                self._EXECUTION_TTL,
            )

            raw_index = await self._redis_get_first_available(
                redis,
                [self._EXECUTION_INDEX_REDIS_KEY, self._LEGACY_EXECUTION_INDEX_REDIS_KEY],
            )
            execution_index = json.loads(raw_index) if raw_index else []
            if execution_id not in execution_index:
                execution_index.append(execution_id)
            execution_index = execution_index[-self._MAX_EXECUTIONS :]
            serialized_index = json.dumps(execution_index)
            await self._redis_write_compat_keys(
                redis,
                [self._EXECUTION_INDEX_REDIS_KEY, self._LEGACY_EXECUTION_INDEX_REDIS_KEY],
                serialized_index,
                self._EXECUTION_TTL,
            )
        except Exception as exc:
            logger.warning("Failed to save workflow engine execution to Redis: %s", exc)

    def _evict_old_executions(self) -> None:
        """Evict oldest executions if over limit."""
        if len(self._executions) > self._MAX_EXECUTIONS:
            # Sort by start_time and keep newest
            sorted_ids = sorted(
                self._executions.keys(),
                key=lambda x: self._executions[x].start_time,
            )
            to_remove = sorted_ids[: len(sorted_ids) - self._MAX_EXECUTIONS]
            for eid in to_remove:
                del self._executions[eid]
            logger.debug("Evicted %d old executions", len(to_remove))

    def register_workflow(self, workflow: Workflow) -> None:
        """Register a workflow for execution"""
        self._workflows[workflow.id] = workflow
        logger.info("Registered workflow: %s (%s)", workflow.name, workflow.id)

    async def register_workflow_async(self, workflow: Workflow) -> None:
        """Register a workflow and persist it to Redis."""
        await self._redis_load_state()
        self.register_workflow(workflow)
        await self._redis_save_workflows()

    async def save_workflow_async(self, workflow_id: str) -> None:
        """Persist the current workflow registry after mutating a workflow."""
        await self._redis_load_state()
        if workflow_id in self._workflows:
            await self._redis_save_workflows()
            # Invalidate chain cache for this workflow
            workflow = self._workflows[workflow_id]
            cache_key = (workflow_id, workflow.version)
            self._chain_cache.pop(cache_key, None)

    def delete_workflow(self, workflow_id: str) -> bool:
        """Delete a workflow from the in-memory registry."""
        if workflow_id not in self._workflows:
            return False
        del self._workflows[workflow_id]
        return True

    async def delete_workflow_async(self, workflow_id: str) -> bool:
        """Delete a workflow and persist the updated registry."""
        await self._redis_load_state()
        deleted = self.delete_workflow(workflow_id)
        if deleted:
            await self._redis_save_workflows()
            # Invalidate all cache entries for this workflow
            keys_to_remove = [k for k in self._chain_cache if k[0] == workflow_id]
            for k in keys_to_remove:
                del self._chain_cache[k]
        return deleted

    def get_workflow(self, workflow_id: str) -> Workflow | None:
        """Get a workflow by ID"""
        return self._workflows.get(workflow_id)

    async def get_workflow_async(self, workflow_id: str) -> Workflow | None:
        """Get a workflow by ID, loading Redis-backed state on first access."""
        await self._redis_load_state()
        return self.get_workflow(workflow_id)

    def list_workflows(self, status: WorkflowStatus | None = None) -> list[Workflow]:
        """List all workflows, optionally filtered by status"""
        workflows = list(self._workflows.values())
        if status:
            workflows = [w for w in workflows if w.status == status]
        return workflows

    async def list_workflows_async(self, status: WorkflowStatus | None = None) -> list[Workflow]:
        """List workflows, loading Redis-backed state on first access."""
        await self._redis_load_state()
        return self.list_workflows(status=status)

    async def execute_workflow(
        self,
        workflow_id: str,
        input_data: dict[str, Any] | None = None,
        context: ChainContext | None = None,
    ) -> WorkflowExecution:
        """
        Execute a workflow.

        Args:
            workflow_id: ID of workflow to execute
            input_data: Initial input data for the workflow
            context: Optional execution context

        Returns:
            WorkflowExecution record
        """
        await self._redis_load_state()
        workflow = self.get_workflow(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")

        # Create execution record
        execution = WorkflowExecution(
            id=str(uuid.uuid4()),
            workflow_id=workflow_id,
            status=WorkflowStatus.EXECUTING,
            start_time=datetime.now(UTC),
            input_data=input_data or {},
            metadata=dict(workflow.metadata),
        )
        self._executions[execution.id] = execution
        self._evict_old_executions()
        await self._redis_save_execution(execution.id)
        # Publish "started" event to the workflow event bus
        await self._publish_workflow_event(execution.id, "started", getattr(workflow, "name", ""))

        try:
            # Convert workflow to chain
            chain = await self._workflow_to_chain(workflow)

            # Execute chain inside an asyncio.Task so it can be cancelled
            exec_task = asyncio.create_task(
                self._run_chain(chain, input_data or {}, context),
                name=f"workflow-{execution.id}",
            )
            self._running_tasks[execution.id] = exec_task

            try:
                chain_result = await exec_task
            except asyncio.CancelledError:
                execution.status = WorkflowStatus.CANCELLED
                execution.end_time = datetime.now(UTC)
                execution.error = "Workflow was cancelled"
                logger.info("Workflow %s cancelled", execution.id)
                await self._redis_save_execution(execution.id)
                await self._publish_workflow_event(execution.id, "cancelled", getattr(workflow, "name", ""))
                return execution
            finally:
                self._running_tasks.pop(execution.id, None)

            # Update execution
            if chain_result.success:
                execution.status = WorkflowStatus.COMPLETED
            else:
                execution.status = WorkflowStatus.FAILED
                execution.error = chain_result.error
            execution.end_time = datetime.now(UTC)
            execution.output_data = (
                {"output": chain_result.output, "trace": chain_result.trace} if chain_result.success else None
            )
            execution.node_executions = chain_result.trace

            logger.info(
                "Workflow %s %s in %.2fms", workflow.name, execution.status.value, chain_result.execution_time_ms
            )
            await self._redis_save_execution(execution.id)
            await self._publish_workflow_event(execution.id, execution.status.value, getattr(workflow, "name", ""))

        except Exception as e:
            execution.status = WorkflowStatus.FAILED
            execution.end_time = datetime.now(UTC)
            execution.error = str(e)
            logger.exception("Workflow execution failed for %s", workflow_id)
            await self._redis_save_execution(execution.id)
            await self._publish_workflow_event(execution.id, "failed", getattr(workflow, "name", ""))
            self._running_tasks.pop(execution.id, None)

        return execution

    async def _run_chain(self, chain: Chain, input_data: dict[str, Any], context: ChainContext | None) -> ChainResult:
        """Run a chain and return the result."""
        return await chain.run(input_data, context=context)

    async def cancel_execution(self, execution_id: str) -> bool:
        """
        Cancel a running workflow execution.

        Args:
            execution_id: ID of the execution to cancel

        Returns:
            True if the execution was found and cancelled, False otherwise
        """
        task = self._running_tasks.get(execution_id)
        if task and not task.done():
            task.cancel()
            logger.info("Cancellation requested for workflow execution %s", execution_id)
            return True

        # If task is not running, check if it's still in executing state
        execution = self._executions.get(execution_id)
        if execution and execution.status == WorkflowStatus.EXECUTING:
            execution.status = WorkflowStatus.CANCELLED
            execution.end_time = datetime.now(UTC)
            execution.error = "Workflow was cancelled"
            await self._redis_save_execution(execution_id)
            logger.info("Workflow execution %s marked as cancelled", execution_id)
            return True
        return False

    class ReferenceResolvingAction(Chain):
        """Wrapper that resolves node config references at execution time."""

        def __init__(self, wrapped_chain: Chain, original_config: dict[str, Any]):
            super().__init__(name=f"{wrapped_chain.name}_ref_resolved")
            self.wrapped_chain = wrapped_chain
            self.original_config = original_config

        async def execute(self, input_data: Any, context: ChainContext) -> Any:
            resolved_config = resolve_config_refs(self.original_config, context)
            rebuilt: Chain
            if isinstance(self.wrapped_chain, HttpRequestAction):
                rebuilt = HttpRequestAction(**resolved_config)
            elif isinstance(self.wrapped_chain, DatabaseAction):
                rebuilt = DatabaseAction(**resolved_config)
            else:
                rebuilt = self.wrapped_chain
            return await rebuilt.execute(input_data, context)

    async def _workflow_to_chain(self, workflow: Workflow) -> Chain:
        """
        Convert a visual workflow to a Helix Chain.

        Caches compiled chains by (workflow_id, version) to avoid
        rebuilding the chain graph on every execution.
        """
        cache_key = (workflow.id, workflow.version)
        cached = self._chain_cache.get(cache_key)
        if cached is not None:
            logger.debug("Chain cache hit for workflow %s v%s", workflow.id, workflow.version)
            return cached

        node_map = {n.id: n for n in workflow.nodes}
        edge_map = self._build_edge_map(workflow.edges)

        # Find trigger nodes (nodes with no incoming edges)
        trigger_nodes = self._find_trigger_nodes(workflow)

        if not trigger_nodes:
            raise ValueError("Workflow must have at least one trigger node")

        # Build chain from trigger
        if len(trigger_nodes) == 1:
            chain = await self._build_chain_from_node(trigger_nodes[0], node_map, edge_map)
        else:
            # Multiple triggers - use parallel chain
            chains = []
            for trigger in trigger_nodes:
                sub_chain = await self._build_chain_from_node(trigger, node_map, edge_map)
                chains.append(sub_chain)

            chain = ParallelChain(
                name=f"{workflow.name}_parallel",
                chains=chains,
                combiner=lambda results: {"results": results},
            )

        chain.name = workflow.name

        # Cache the compiled chain (with LRU eviction)
        if len(self._chain_cache) >= self._chain_cache_max:
            # Evict oldest entry
            oldest_key = next(iter(self._chain_cache))
            del self._chain_cache[oldest_key]
        self._chain_cache[cache_key] = chain

        return chain

    async def _build_chain_from_node(
        self,
        node: WorkflowNode,
        node_map: dict[str, WorkflowNode],
        edge_map: dict[str, list[WorkflowEdge]],
    ) -> Chain:
        """Recursively build chain from a node"""
        # Get outgoing edges
        outgoing_edges = edge_map.get(node.id, [])

        if not outgoing_edges:
            # Leaf node - create simple action
            return await self._node_to_step(node)

        # Check if this is a branching node
        if len(outgoing_edges) > 1:
            # Build conditional or router chain
            branches = []
            conditions = []

            for edge in outgoing_edges:
                target_node = node_map.get(edge.target)
                if not target_node:
                    continue

                sub_chain = await self._build_chain_from_node(target_node, node_map, edge_map)
                branches.append(sub_chain)
                conditions.append(edge.condition)

            # Determine chain type based on conditions
            if any(conditions):
                # Conditional routing
                return RouterChain(
                    name=f"{node.name}_router",
                    routes={f"route_{i}": chain for i, chain in enumerate(branches)},
                    classifier=lambda x, ctx: self._classify_route(x, ctx, conditions),
                    default_route="route_0",
                )
            else:
                # Parallel execution
                return ParallelChain(
                    name=f"{node.name}_parallel",
                    chains=branches,
                    combiner=lambda results: {"combined": results},
                )
        else:
            # Linear chain - sequential execution
            current_step = await self._node_to_step(node)
            next_edge = outgoing_edges[0]
            next_node = node_map.get(next_edge.target)

            if next_node:
                next_step = await self._build_chain_from_node(next_node, node_map, edge_map)

                # Build sequential chain
                return SequentialChain(name=f"{node.name}_sequence", chains=[current_step, next_step])

            return current_step

    async def _node_to_step(self, node: WorkflowNode) -> Chain:
        """Convert a workflow node to a Chain step"""

        if node.type == NodeType.AGENT:
            # Agent node
            agent_type = node.config.get("agent_type")
            if not isinstance(agent_type, str):
                raise ValueError("Agent node is missing a valid 'agent_type'")

            agent = self.agent_registry.get(agent_type)
            if not agent:
                raise ValueError(f"Agent not found: {agent_type}")
            return cast(Chain, agent)

        elif node.type == NodeType.WEBHOOK:
            # Webhook trigger
            return WebhookTrigger(**node.config)

        elif node.type == NodeType.SCHEDULE:
            # Scheduler-triggered workflows pass through the scheduler input payload.
            return ScheduleTrigger(**node.config)

        elif node.type == NodeType.HTTP_REQUEST:
            # HTTP request action - wrap with retry for transient failures
            # Create a reference-resolving wrapper
            http_action = HttpRequestAction(**node.config)
            wrapped = WorkflowEngine.ReferenceResolvingAction(http_action, node.config)
            retry_config = node.config.get("retry", {})
            max_retries = retry_config.get("max_retries", 3)
            retry_delay = retry_config.get("retry_delay", 1.0)
            return RetryChain(
                chain=wrapped,
                max_retries=max_retries,
                retry_delay=retry_delay,
                exponential_backoff=True,
                name=f"{node.name}_with_retry",
            )

        elif node.type == NodeType.TRANSFORM:
            # Data transformation
            return TransformAction(**node.config)

        elif node.type == NodeType.CONDITION:
            # Conditional logic
            return ConditionalNode(**node.config)

        elif node.type == NodeType.DATABASE:
            # Database operation - wrap with retry for connection issues
            # Create a reference-resolving wrapper
            db_action = DatabaseAction(**node.config)
            wrapped = WorkflowEngine.ReferenceResolvingAction(db_action, node.config)
            retry_config = node.config.get("retry", {})
            max_retries = retry_config.get("max_retries", 2)
            retry_delay = retry_config.get("retry_delay", 0.5)
            return RetryChain(
                chain=wrapped,
                max_retries=max_retries,
                retry_delay=retry_delay,
                exponential_backoff=True,
                name=f"{node.name}_with_retry",
            )

        elif node.type == NodeType.SUB_WORKFLOW:
            # Sub-workflow execution - run another workflow as a step
            sub_workflow_id = node.config.get("workflow_id")
            if not sub_workflow_id:
                raise ValueError("Sub-workflow node is missing 'workflow_id' in config")
            return SubWorkflowAction(
                workflow_engine=self,
                workflow_id=sub_workflow_id,
                name=f"{node.name}_subworkflow",
            )

        elif node.type == NodeType.CODE:
            # Code execution
            return CodeAction(**node.config)

        else:
            # Generic action using tool registry
            tool_name = node.config.get("tool_name")
            if not isinstance(tool_name, str):
                raise ValueError(f"Unknown node type or tool: {node.type}")

            tool = self.tool_registry.get(tool_name)

            if tool:
                return ToolAction(tool=tool, **node.config)
            else:
                raise ValueError(f"Unknown node type or tool: {node.type}")

    def _build_edge_map(self, edges: list[WorkflowEdge]) -> dict[str, list[WorkflowEdge]]:
        """Build a map from source node to outgoing edges"""
        edge_map: dict[str, list[WorkflowEdge]] = {}
        for edge in edges:
            if edge.source not in edge_map:
                edge_map[edge.source] = []
            edge_map[edge.source].append(edge)
        return edge_map

    def _find_trigger_nodes(self, workflow: Workflow) -> list[WorkflowNode]:
        """Find nodes with no incoming edges (triggers)"""
        # Get all targets
        targets = {e.target for e in workflow.edges}

        # Find nodes not in targets
        triggers = [n for n in workflow.nodes if n.id not in targets]
        return triggers

    def _classify_route(self, input_data: Any, context: ChainContext, conditions: list[str | None]) -> str:
        """Classify which route to take based on conditions"""
        for i, condition in enumerate(conditions):
            if not condition:
                return f"route_{i}"

            # Evaluate condition using SafeEvaluator
            try:
                evaluator = SafeEvaluator(allowed_names={"data": input_data, "ctx": context})
                result = evaluator.eval(condition)
                if result:
                    return f"route_{i}"
            except Exception as e:
                logger.warning("Condition evaluation failed: %s", e)

        return "route_0"

    def get_execution(self, execution_id: str) -> WorkflowExecution | None:
        """Get execution record by ID"""
        return self._executions.get(execution_id)

    async def get_execution_async(self, execution_id: str) -> WorkflowExecution | None:
        """Get an execution record by ID, loading Redis-backed state on first access."""
        await self._redis_load_state()
        return self.get_execution(execution_id)

    def list_executions(
        self, workflow_id: str | None = None, status: WorkflowStatus | None = None, limit: int = 100
    ) -> list[WorkflowExecution]:
        """List executions, optionally filtered"""
        executions = list(self._executions.values())

        if workflow_id:
            executions = [e for e in executions if e.workflow_id == workflow_id]

        if status:
            executions = [e for e in executions if e.status == status]

        return sorted(executions, key=lambda e: e.start_time, reverse=True)[:limit]

    async def list_executions_async(
        self,
        workflow_id: str | None = None,
        status: WorkflowStatus | None = None,
        limit: int = 100,
    ) -> list[WorkflowExecution]:
        """List executions, loading Redis-backed state on first access."""
        await self._redis_load_state()
        return self.list_executions(workflow_id=workflow_id, status=status, limit=limit)


# Action implementations
class WebhookTrigger(Chain):
    """Webhook trigger node"""

    def __init__(self, webhook_url: str | None = None, **kwargs):
        super().__init__(name="webhook_trigger", **kwargs)
        self.webhook_url = webhook_url

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Receive webhook data"""
        return input_data or {}


class ScheduleTrigger(Chain):
    """Schedule trigger node"""

    def __init__(self, **kwargs):
        super().__init__(name="schedule_trigger")

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Receive scheduler input data"""
        return input_data or {}


class HttpRequestAction(Chain):
    """HTTP request action node"""

    def __init__(
        self,
        url: str,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        body: Any = None,
        **kwargs,
    ):
        super().__init__(name="http_request", **kwargs)
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}
        self.body = body

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute HTTP request"""
        import httpx

        # SSRF protection: validate URL before making request
        parsed = urlparse(self.url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("Only HTTP(S) URLs are allowed")
        hostname = parsed.hostname
        if not hostname:
            raise ValueError("Invalid URL: missing hostname")
        try:
            for info in socket.getaddrinfo(hostname, parsed.port or 443, proto=socket.IPPROTO_TCP):
                addr = info[4][0]
                ip = ipaddress.ip_address(addr)
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                    raise ValueError("Requests to private/internal addresses are blocked")
        except socket.gaierror:
            raise ValueError("Could not resolve hostname") from None

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=self.method,
                url=self.url,
                headers=self.headers,
                json=self.body if self.method in ["POST", "PUT", "PATCH"] else None,
                timeout=30.0,
            )

            return {
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "data": (
                    response.json() if "application/json" in response.headers.get("content-type", "") else response.text
                ),
            }


class TransformAction(Chain):
    """Data transformation node"""

    def __init__(self, transformation_type: str = "json_path", expression: str | None = None, **kwargs):
        super().__init__(name="transform", **kwargs)
        self.transformation_type = transformation_type
        self.expression = expression

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute transformation"""
        if not self.expression:
            return input_data

        if self.transformation_type == "json_path":
            # Simple JSON path extraction
            from jsonpath_ng import parse

            jsonpath = parse(self.expression)
            matches = jsonpath.find(input_data)
            return [m.value for m in matches]

        elif self.transformation_type == "map":
            # Apply mapping function
            return {k: input_data.get(k) for k in self.expression.split(",")}

        else:
            return input_data


class ConditionalNode(Chain):
    """Conditional logic node"""

    def __init__(self, condition: str, **kwargs):
        super().__init__(name="condition", **kwargs)
        self.condition = condition

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Evaluate condition"""
        try:
            evaluator = SafeEvaluator(allowed_names={"data": input_data, "ctx": context})
            result = evaluator.eval(self.condition)
            return {"condition_result": bool(result)}
        except Exception as e:
            logger.error("Condition evaluation failed: %s", e)
            raise ValueError("Condition evaluation failed") from e


# Allowed SQL statement prefixes for workflow database actions
_ALLOWED_SQL_PREFIXES = ("SELECT", "WITH", "INSERT", "UPDATE", "DELETE")
# Explicitly blocked SQL patterns (case-insensitive)
_BLOCKED_SQL_PATTERNS = (
    "DROP ",
    "ALTER ",
    "TRUNCATE ",
    "CREATE ",
    "GRANT ",
    "REVOKE ",
    "--",
    "/*",
    ";",
    "pg_",
    "information_schema",
)


class DatabaseAction(Chain):
    """Database action node — executes parameterized SQL via the platform asyncpg pool."""

    def __init__(self, query: str, database_url: str | None = None, **kwargs):
        super().__init__(name="database", **kwargs)
        self.query = query
        self.database_url = database_url

    @staticmethod
    def _validate_query(query: str) -> None:
        """Validate query against injection and destructive patterns."""
        upper = query.upper().lstrip()
        if not any(upper.startswith(prefix) for prefix in _ALLOWED_SQL_PREFIXES):
            raise ValueError(
                "Only SELECT, WITH, INSERT, UPDATE, and DELETE queries are permitted in workflow database actions."
            )
        for pattern in _BLOCKED_SQL_PATTERNS:
            if pattern.upper() in upper:
                raise ValueError(
                    "Query contains blocked pattern '{}'. Destructive or administrative SQL is not allowed.".format(
                        pattern.strip()
                    )
                )

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute parameterized database query against the platform database."""
        from apps.backend.core.unified_auth import Database

        query = self.query.strip()
        self._validate_query(query)

        # Extract parameters from input_data for parameterized queries ($1, $2, ...)
        params = []
        if isinstance(input_data, dict):
            params = input_data.get("params", [])
            if not isinstance(params, list):
                params = [params]

        try:
            upper = query.upper().lstrip()
            if upper.startswith("SELECT") or upper.startswith("WITH"):
                rows = await Database.fetch(query, *params)
                result = [dict(r) for r in rows] if rows else []
                return {
                    "query_result": result,
                    "rows_returned": len(result),
                }
            else:
                status = await Database.execute(query, *params)
                return {
                    "query_result": status or "executed",
                    "rows_affected": (int(status.split()[-1]) if status and status.split()[-1].isdigit() else 0),
                }
        except Exception:
            logger.exception("Database query failed")
            raise ValueError("Database query failed") from None


class CodeAction(Chain):
    """Code execution node"""

    def __init__(self, code: str, language: str = "python", **kwargs):
        super().__init__(name="code_execution", **kwargs)
        self.code = code
        self.language = language

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute code in a restricted environment"""
        if self.language == "python":
            import asyncio
            import contextlib
            from io import StringIO

            raise ValueError(
                "Legacy in-process code actions are disabled; use an explicitly registered Routine Engine action"
            )

            # Retained temporarily as archived source; unreachable by design.
            stdout_capture = StringIO()

            # Define safe builtins for workflow code execution
            # NOTE: `type` and `isinstance` are excluded to prevent
            # sandbox escapes via __subclasses__() metaclass traversal.
            safe_builtins = {
                "len": len,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
                "list": list,
                "dict": dict,
                "tuple": tuple,
                "set": set,
                "range": range,
                "enumerate": enumerate,
                "zip": zip,
                "map": map,
                "filter": filter,
                "sorted": sorted,
                "min": min,
                "max": max,
                "sum": sum,
                "abs": abs,
                "round": round,
                "print": print,
                "True": True,
                "False": False,
                "None": None,
            }

            # Block dangerous patterns before execution
            _blocked = [
                "__import__",
                "__subclasses__",
                "__bases__",
                "__class__",
                "__mro__",
                "__globals__",
                "__code__",
                "__builtins__",
                "__loader__",
                "__spec__",
                "__dict__",
                "importlib",
                "subprocess",
                "os.system",
                "os.popen",
                "os.exec",
                "os.spawn",
                "os.environ",
                "sys.modules",
                "eval(",
                "exec(",
                "compile(",
                "open(",
                "getattr(",
                "setattr(",
                "delattr(",
                "breakpoint(",
                "exit(",
                "quit(",
                "type(",
                "globals(",
                "locals(",
                "vars(",
                "dir(",
                "memoryview(",
                "bytearray(",
                "classmethod(",
                "staticmethod(",
                "property(",
            ]
            code_lower = self.code.lower()
            for pattern in _blocked:
                if pattern.lower() in code_lower:
                    raise ValueError("Code contains blocked pattern '{}'.".format(pattern))

            # AST validation pass — reject any AST nodes that could escape the sandbox
            import ast as _ast

            try:
                tree = _ast.parse(self.code)
            except SyntaxError as syn_err:
                raise ValueError("Workflow code has a syntax error: {}".format(syn_err)) from syn_err

            _forbidden_ast_nodes = (
                _ast.Import,
                _ast.ImportFrom,
                _ast.Global,
                _ast.Nonlocal,
            )
            for node in _ast.walk(tree):
                if isinstance(node, _forbidden_ast_nodes):
                    raise ValueError("Workflow code contains forbidden construct: {}".format(type(node).__name__))
                # Block attribute access to dunder names
                if isinstance(node, _ast.Attribute) and node.attr.startswith("__"):
                    raise ValueError("Workflow code accesses forbidden attribute: {}".format(node.attr))

            try:
                exec_globals = {
                    "__builtins__": safe_builtins,
                    "input": input_data,
                    "context": context,
                }

                def _run_sandboxed():
                    with contextlib.redirect_stdout(stdout_capture):
                        exec(self.code, exec_globals)  # nosec B102 — restricted builtins

                await asyncio.wait_for(
                    asyncio.to_thread(_run_sandboxed),
                    timeout=30.0,
                )
                output = stdout_capture.getvalue()

                # Return result if exists
                result = exec_globals.get("result", output)
                return result
            except TimeoutError:
                raise ValueError("Workflow code execution timed out after 30 seconds") from None

        raise ValueError(f"Unsupported language: {self.language}")


class SubWorkflowAction(Chain):
    """Execute a sub-workflow as a step within a parent workflow."""

    def __init__(self, workflow_engine: "WorkflowEngine", workflow_id: str, **kwargs):
        super().__init__(**kwargs)
        self.workflow_engine = workflow_engine
        self.workflow_id = workflow_id
        self._max_depth = 3  # Prevent infinite recursion

    async def execute(self, input_data: Any, context: ChainContext | None) -> Any:
        """Execute the sub-workflow and return its output."""
        # Check recursion depth
        depth = context.metadata.get("_sub_workflow_depth", 0) if context else 0
        if depth >= self._max_depth:
            raise ValueError(f"Sub-workflow nesting depth ({depth}) exceeds maximum ({self._max_depth})")

        logger.info("Sub-workflow %s executing (depth %d)", self.workflow_id, depth)

        # Create child context with incremented depth
        child_context = ChainContext(
            workflow_id=self.workflow_id,
            metadata={**(context.metadata if context else {}), "_sub_workflow_depth": depth + 1},
        )

        # Execute the sub-workflow
        execution = await self.workflow_engine.execute_workflow(
            workflow_id=self.workflow_id,
            input_data=input_data if isinstance(input_data, dict) else {"input": input_data},
            context=child_context,
        )

        if execution.status == WorkflowStatus.COMPLETED:
            return execution.output_data
        elif execution.status == WorkflowStatus.CANCELLED:
            raise asyncio.CancelledError(f"Sub-workflow {self.workflow_id} was cancelled")
        else:
            raise ValueError(f"Sub-workflow {self.workflow_id} failed: {execution.error or 'unknown error'}")


class ToolAction(Chain):
    """Tool-based action node"""

    def __init__(self, tool: Tool, **kwargs):
        super().__init__(name=tool.name, **kwargs)
        self.tool = tool

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute tool"""
        result = await self.tool.execute(**input_data)
        return result.output if result.success else {"error": result.error}
