"""
Helix Workflow Engine - API Endpoints
=====================================

FastAPI endpoints for workflow management and execution.
REST API compatible with n8n workflow format.
"""

import logging
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from apps.backend.core.auth import get_authenticated_user_id
from apps.backend.core.unified_auth import get_current_user

from .core import NodeType, Workflow, WorkflowEdge, WorkflowEngine, WorkflowNode, WorkflowStatus
from .integrations import IntegrationRegistry, get_popular_integrations
from .scheduler import CronParser, ScheduleType, WorkflowScheduler

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/workflows", tags=["workflows"])

# Global instances
engine = WorkflowEngine()
integration_registry = IntegrationRegistry()

# Wire HelixCollective agents into the workflow engine
try:
    from .agents import HelixCollective

    _helix_collective = HelixCollective()
    engine.agent_registry = _helix_collective.agents
    logger.info("[OK] Wired %s Helix agents into workflow engine", len(_helix_collective.agents))
except Exception as e:
    logger.warning("[SKIP] Could not wire HelixCollective agents into workflow engine: %s", e)


# Request/Response Models
class WorkflowNodeModel(BaseModel):
    id: str
    type: str
    name: str
    config: dict = Field(default_factory=dict)
    position: dict = Field(default_factory=dict)


class WorkflowEdgeModel(BaseModel):
    id: str
    source: str
    target: str
    condition: str | None = None


class WorkflowTaskModel(BaseModel):
    id: str | None = None
    name: str = ""
    description: str | None = None
    agent_type: str | None = None
    order: int | None = None


class WorkflowDefinitionModel(BaseModel):
    steps: list[WorkflowTaskModel] = Field(default_factory=list)
    tasks: list[WorkflowTaskModel] = Field(default_factory=list)


class WorkflowCreateModel(BaseModel):
    name: str
    description: str = ""
    nodes: list[WorkflowNodeModel] = Field(default_factory=list)
    edges: list[WorkflowEdgeModel] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    workflow_type: str | None = None
    definition: WorkflowDefinitionModel | None = None
    steps: list[WorkflowTaskModel] = Field(default_factory=list)
    tasks: list[WorkflowTaskModel] = Field(default_factory=list)
    schedule: dict[str, Any] | None = None


class WorkflowUpdateModel(BaseModel):
    name: str | None = None
    description: str | None = None
    nodes: list[WorkflowNodeModel] | None = None
    edges: list[WorkflowEdgeModel] | None = None
    tags: list[str] | None = None
    status: str | None = None
    workflow_type: str | None = None
    definition: WorkflowDefinitionModel | None = None
    steps: list[WorkflowTaskModel] | None = None
    tasks: list[WorkflowTaskModel] | None = None
    schedule: dict[str, Any] | None = None


class WorkflowExecuteModel(BaseModel):
    input_data: dict = Field(default_factory=dict)
    input: dict | None = None
    wait_for_completion: bool = True


class WorkflowDuplicateModel(BaseModel):
    name: str | None = None


class WorkflowScheduleModel(BaseModel):
    type: str = "manual"
    enabled: bool = False
    timezone: str = "UTC"
    intervalValue: int | None = None
    intervalUnit: str | None = None
    cronExpression: str | None = None
    runAt: str | None = None


class ExecutionResponse(BaseModel):
    id: str
    workflow_id: str
    status: str
    start_time: str
    end_time: str | None = None
    trigger: str | None = None
    replayed_from_execution_id: str | None = None
    input_data: dict
    output_data: dict | None = None
    error: str | None = None
    node_executions: list[dict] = []


# API Endpoints


def _current_user_id(user: dict[str, Any]) -> str:
    user_id = get_authenticated_user_id(user)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Authenticated user id missing")
    return str(user_id)


async def _get_engine_workflow(workflow_id: str) -> Workflow | None:
    get_workflow_async = getattr(engine, "get_workflow_async", None)
    if callable(get_workflow_async):
        return await get_workflow_async(workflow_id)
    return engine.get_workflow(workflow_id)


async def _list_engine_workflows(status: WorkflowStatus | None = None) -> list[Workflow]:
    list_workflows_async = getattr(engine, "list_workflows_async", None)
    if callable(list_workflows_async):
        return await list_workflows_async(status=status)
    return engine.list_workflows(status=status)


async def _register_engine_workflow(workflow: Workflow) -> None:
    register_workflow_async = getattr(engine, "register_workflow_async", None)
    if callable(register_workflow_async):
        await register_workflow_async(workflow)
        return
    engine.register_workflow(workflow)


async def _save_engine_workflow(workflow_id: str) -> None:
    save_workflow_async = getattr(engine, "save_workflow_async", None)
    if callable(save_workflow_async):
        await save_workflow_async(workflow_id)
        return

    save_workflow = getattr(engine, "save_workflow", None)
    if callable(save_workflow):
        save_workflow(workflow_id)


async def _delete_engine_workflow(workflow_id: str) -> bool:
    delete_workflow_async = getattr(engine, "delete_workflow_async", None)
    if callable(delete_workflow_async):
        return await delete_workflow_async(workflow_id)

    delete_workflow = getattr(engine, "delete_workflow", None)
    if callable(delete_workflow):
        return bool(delete_workflow(workflow_id))

    if workflow_id not in engine._workflows:
        return False

    del engine._workflows[workflow_id]
    return True


async def _get_engine_execution(execution_id: str) -> Any | None:
    get_execution_async = getattr(engine, "get_execution_async", None)
    if callable(get_execution_async):
        return await get_execution_async(execution_id)
    return engine.get_execution(execution_id)


async def _list_engine_executions(
    workflow_id: str | None = None,
    status: WorkflowStatus | None = None,
    limit: int = 100,
) -> list[Any]:
    list_executions_async = getattr(engine, "list_executions_async", None)
    if callable(list_executions_async):
        return await list_executions_async(workflow_id=workflow_id, status=status, limit=limit)
    return engine.list_executions(workflow_id=workflow_id, status=status, limit=limit)


def _serialize_execution_response(execution: Any) -> ExecutionResponse:
    metadata = getattr(execution, "metadata", {}) or {}
    return ExecutionResponse(
        id=execution.id,
        workflow_id=execution.workflow_id,
        status=execution.status.value,
        start_time=execution.start_time.isoformat(),
        end_time=execution.end_time.isoformat() if execution.end_time else None,
        trigger=metadata.get("trigger") if isinstance(metadata.get("trigger"), str) else None,
        replayed_from_execution_id=(
            metadata.get("replayed_from_execution_id")
            if isinstance(metadata.get("replayed_from_execution_id"), str)
            else None
        ),
        input_data=execution.input_data,
        output_data=execution.output_data,
        error=execution.error,
        node_executions=execution.node_executions,
    )


def set_workflow_scheduler(scheduler: WorkflowScheduler | None) -> None:
    get_workflow_scheduler._workflow_scheduler = scheduler  # type: ignore[attr-defined]


async def _refresh_workflow_scheduler() -> None:
    await reload_workflow_scheduler_from_store()


def get_workflow_scheduler() -> WorkflowScheduler:
    scheduler = getattr(get_workflow_scheduler, "_workflow_scheduler", None)
    if scheduler is None or getattr(scheduler, "workflow_engine", None) is not engine:
        scheduler = WorkflowScheduler(engine)
        get_workflow_scheduler._workflow_scheduler = scheduler  # type: ignore[attr-defined]

    scheduler.workflow_engine = engine
    scheduler.refresh_callback = _refresh_workflow_scheduler
    return scheduler


def _schedule_runtime_state(workflow: Workflow) -> dict[str, Any] | None:
    runtime_state = workflow.metadata.get("schedule_runtime")
    return runtime_state if isinstance(runtime_state, dict) else None


def _scheduler_args_from_payload(schedule: dict[str, Any]) -> tuple[ScheduleType, dict[str, Any]]:
    schedule_type = str(schedule.get("type") or "manual").lower()

    if schedule_type == "cron":
        cron_expression = schedule.get("cronExpression")
        if not isinstance(cron_expression, str) or not cron_expression:
            raise ValueError("Cron schedules require a cronExpression")
        return ScheduleType.CRON, {"cron_expression": cron_expression}

    if schedule_type == "interval":
        interval_value = schedule.get("intervalValue")
        interval_unit = schedule.get("intervalUnit")
        if not isinstance(interval_value, int) or interval_value <= 0:
            raise ValueError("Interval schedules require a positive intervalValue")
        if interval_unit not in _INTERVAL_SECONDS:
            raise ValueError("Interval schedules require a valid intervalUnit")
        return ScheduleType.INTERVAL, {"interval_seconds": interval_value * _INTERVAL_SECONDS[interval_unit]}

    if schedule_type == "once":
        run_at = schedule.get("runAt")
        if not isinstance(run_at, str) or not run_at:
            raise ValueError("One-time schedules require runAt")
        return ScheduleType.ONCE, {"run_at": _parse_iso_datetime(run_at)}

    raise ValueError(f"Unsupported schedule type: {schedule_type}")


def _clear_workflow_next_run(workflow: Workflow) -> bool:
    metadata_changed = False
    if workflow.metadata.get("next_run") is not None:
        workflow.metadata["next_run"] = None
        metadata_changed = True

    runtime_state = _schedule_runtime_state(workflow)
    if runtime_state is not None and runtime_state.get("next_run") is not None:
        runtime_state["next_run"] = None
        metadata_changed = True

    return metadata_changed


def _remove_local_workflow_schedule(workflow_id: str) -> None:
    scheduler = get_workflow_scheduler()
    for scheduled in list(scheduler.list_schedules(workflow_id=workflow_id)):
        scheduler.remove_schedule(scheduled.id)


def _local_schedule_runtime_state(workflow_id: str) -> dict[str, Any] | None:
    scheduler = get_workflow_scheduler()
    existing_schedules = list(scheduler.list_schedules(workflow_id=workflow_id))
    if not existing_schedules:
        return None
    return existing_schedules[0].to_dict()


async def _sync_workflow_schedule_runtime(workflow: Workflow) -> bool:
    local_runtime_state = _local_schedule_runtime_state(workflow.id)
    scheduler = get_workflow_scheduler()
    _remove_local_workflow_schedule(workflow.id)

    schedule_payload = _extract_schedule(workflow)
    if not schedule_payload:
        return _clear_workflow_next_run(workflow)

    if not schedule_payload.get("enabled"):
        return _clear_workflow_next_run(workflow)

    if workflow.status != WorkflowStatus.ACTIVE:
        runtime_state = _schedule_runtime_state(workflow)
        if runtime_state is not None and runtime_state.get("next_run") is not None:
            runtime_state["next_run"] = None
            return True
        return False

    try:
        schedule_type, schedule_kwargs = _scheduler_args_from_payload(schedule_payload)
    except ValueError as exc:
        logger.warning("Skipping invalid workflow schedule for %s: %s", workflow.id, exc)
        return False

    scheduled = scheduler.add_schedule(
        workflow_id=workflow.id,
        name=workflow.name,
        schedule_type=schedule_type,
        input_data={},
        timezone=str(schedule_payload.get("timezone") or "UTC"),
        schedule_id=workflow.id,
        state=local_runtime_state or _schedule_runtime_state(workflow),
        **schedule_kwargs,
    )

    metadata_changed = False
    schedule_state = scheduled.to_dict()
    if workflow.metadata.get("schedule_runtime") != schedule_state:
        workflow.metadata["schedule_runtime"] = schedule_state
        metadata_changed = True

    next_run = scheduled.next_run.isoformat() if scheduled.next_run else None
    if workflow.metadata.get("next_run") != next_run:
        workflow.metadata["next_run"] = next_run
        metadata_changed = True

    return metadata_changed


async def reload_workflow_scheduler_from_store() -> WorkflowScheduler:
    scheduler = get_workflow_scheduler()
    workflows = await _list_engine_workflows()
    workflow_ids = {workflow.id for workflow in workflows}

    for scheduled in list(scheduler.list_schedules()):
        if scheduled.workflow_id not in workflow_ids:
            scheduler.remove_schedule(scheduled.id)

    for workflow in workflows:
        metadata_changed = await _sync_workflow_schedule_runtime(workflow)
        if metadata_changed:
            await _save_engine_workflow(workflow.id)

    return scheduler


async def start_workflow_scheduler() -> WorkflowScheduler:
    scheduler = await reload_workflow_scheduler_from_store()
    if not getattr(scheduler, "_running", False):
        await scheduler.start()
    return scheduler


async def stop_workflow_scheduler() -> None:
    scheduler = getattr(get_workflow_scheduler, "_workflow_scheduler", None)
    if scheduler is None:
        return
    await scheduler.stop()


def get_workflow_scheduler_health(limit: int = 5) -> dict[str, Any]:
    scheduler = get_workflow_scheduler()
    stats = scheduler.get_stats()

    return {
        **stats,
        "recent_executions": [execution.to_dict() for execution in scheduler.get_executions(limit=limit)],
    }


def _workflow_owner(workflow: Workflow) -> str | None:
    owner = workflow.metadata.get("created_by") or workflow.metadata.get("owner_id") or workflow.metadata.get("user_id")
    return str(owner) if owner else None


def _is_owned_workflow(workflow: Workflow, user_id: str) -> bool:
    return _workflow_owner(workflow) == user_id


async def _get_owned_workflow(workflow_id: str, user: dict[str, Any]) -> Workflow:
    user_id = _current_user_id(user)
    workflow = await _get_engine_workflow(workflow_id)
    if not workflow or not _is_owned_workflow(workflow, user_id):
        raise HTTPException(status_code=404, detail="Workflow not found")
    return workflow


async def _is_owned_execution(execution: Any, user_id: str) -> bool:
    metadata = getattr(execution, "metadata", {}) or {}
    execution_owner = metadata.get("created_by") or metadata.get("owner_id") or metadata.get("user_id")
    if execution_owner is not None:
        return str(execution_owner) == user_id

    workflow = await _get_engine_workflow(getattr(execution, "workflow_id", ""))
    return bool(workflow and _is_owned_workflow(workflow, user_id))


_NODE_TYPE_ALIASES = {
    "ai_agent": NodeType.AGENT.value,
    "conditional": NodeType.CONDITION.value,
    "database_query": NodeType.DATABASE.value,
    "filter": NodeType.TRANSFORM.value,
    "json_parse": NodeType.TRANSFORM.value,
    "manual": NodeType.TRIGGER.value,
    "merge": NodeType.TRANSFORM.value,
}
_INTERVAL_SECONDS = {
    "minutes": 60,
    "hours": 3600,
    "days": 86400,
    "weeks": 604800,
}


def _coerce_node_type(node_type: str) -> NodeType:
    normalized = _NODE_TYPE_ALIASES.get(node_type.strip().lower(), node_type.strip().lower())
    return NodeType(normalized)


def _coerce_workflow_status(status: str) -> WorkflowStatus:
    normalized = status.strip().lower()
    aliases = {
        "active": WorkflowStatus.ACTIVE,
        "completed": WorkflowStatus.COMPLETED,
        "draft": WorkflowStatus.INACTIVE,
        "error": WorkflowStatus.FAILED,
        "executing": WorkflowStatus.EXECUTING,
        "failed": WorkflowStatus.FAILED,
        "inactive": WorkflowStatus.INACTIVE,
        "paused": WorkflowStatus.PAUSED,
    }
    return aliases.get(normalized, WorkflowStatus(normalized))


def _workflow_tasks_from_payload(
    definition: WorkflowDefinitionModel | None,
    steps: list[WorkflowTaskModel] | None,
    tasks: list[WorkflowTaskModel] | None,
) -> list[WorkflowTaskModel]:
    if definition:
        if definition.steps:
            return definition.steps
        if definition.tasks:
            return definition.tasks
    if tasks:
        return tasks
    if steps:
        return steps
    return []


def _build_graph_from_tasks(tasks: list[WorkflowTaskModel]) -> tuple[list[WorkflowNode], list[WorkflowEdge]]:
    ordered_tasks = sorted(
        tasks,
        key=lambda task: task.order if task.order is not None else tasks.index(task),
    )
    nodes: list[WorkflowNode] = []
    edges: list[WorkflowEdge] = []

    for index, task in enumerate(ordered_tasks):
        node_id = task.id or f"step-{index + 1}"
        nodes.append(
            WorkflowNode(
                id=node_id,
                type=NodeType.AGENT,
                name=task.name or f"Step {index + 1}",
                config={
                    "agent_type": task.agent_type or "praxis",
                    "description": task.description or "",
                    "order": task.order if task.order is not None else index,
                },
                position={"x": 160.0 * index, "y": 0.0},
            )
        )

        if index:
            previous_node = nodes[index - 1]
            edges.append(
                WorkflowEdge(
                    id=f"edge-{previous_node.id}-{node_id}",
                    source=previous_node.id,
                    target=node_id,
                )
            )

    return nodes, edges


def _schedule_trigger_node(schedule: dict[str, Any]) -> WorkflowNode:
    return WorkflowNode(
        id="schedule-trigger",
        type=NodeType.SCHEDULE,
        name="Scheduled Trigger",
        config=dict(schedule),
        position={"x": 0.0, "y": 0.0},
    )


def _ensure_schedule_trigger_node(workflow: Workflow, schedule: dict[str, Any] | None) -> bool:
    if not schedule or workflow.nodes:
        return False

    workflow.nodes = [_schedule_trigger_node(schedule)]
    workflow.edges = []
    return True


def _build_workflow_graph(
    nodes: list[WorkflowNodeModel] | None,
    edges: list[WorkflowEdgeModel] | None,
    definition: WorkflowDefinitionModel | None,
    steps: list[WorkflowTaskModel] | None,
    tasks: list[WorkflowTaskModel] | None,
) -> tuple[list[WorkflowNode], list[WorkflowEdge]]:
    task_payload = _workflow_tasks_from_payload(definition, steps, tasks)
    if task_payload and not nodes:
        return _build_graph_from_tasks(task_payload)

    return (
        [
            WorkflowNode(
                id=node.id,
                type=_coerce_node_type(node.type),
                name=node.name,
                config=node.config,
                position=node.position,
            )
            for node in (nodes or [])
        ],
        [
            WorkflowEdge(id=edge.id, source=edge.source, target=edge.target, condition=edge.condition)
            for edge in (edges or [])
        ],
    )


def _workflow_matches_search(workflow: Workflow, search: str) -> bool:
    needle = search.strip().lower()
    if not needle:
        return True

    haystacks = [workflow.name, workflow.description, *workflow.tags]
    return any(needle in value.lower() for value in haystacks if isinstance(value, str))


def _parse_iso_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _schedule_next_run(schedule: dict[str, Any] | None) -> str | None:
    if not schedule or not schedule.get("enabled"):
        return None

    schedule_type = str(schedule.get("type") or "manual").lower()
    now = datetime.now(UTC)

    if schedule_type == "cron":
        cron_expression = schedule.get("cronExpression")
        if isinstance(cron_expression, str) and cron_expression:
            return CronParser.get_next_run(cron_expression, now).isoformat()
        return None

    if schedule_type == "interval":
        interval_value = schedule.get("intervalValue")
        interval_unit = schedule.get("intervalUnit")
        if isinstance(interval_value, int) and interval_value > 0 and interval_unit in _INTERVAL_SECONDS:
            return (now + timedelta(seconds=interval_value * _INTERVAL_SECONDS[interval_unit])).isoformat()
        return None

    if schedule_type == "once":
        run_at = schedule.get("runAt")
        if isinstance(run_at, str) and run_at:
            parsed = _parse_iso_datetime(run_at)
            if parsed > now:
                return parsed.isoformat()
        return None

    return None


def _normalize_schedule_payload(schedule_data: WorkflowScheduleModel) -> tuple[dict[str, Any], str | None]:
    schedule_type = schedule_data.type.strip().lower()
    if schedule_type not in {"manual", "interval", "cron", "once"}:
        raise HTTPException(status_code=422, detail="Unsupported schedule type")

    normalized: dict[str, Any] = {
        "type": schedule_type,
        "enabled": bool(schedule_data.enabled),
        "timezone": schedule_data.timezone or "UTC",
    }

    if schedule_type == "interval":
        if schedule_data.intervalValue is None or schedule_data.intervalValue <= 0:
            raise HTTPException(status_code=422, detail="Interval schedules require a positive intervalValue")
        if schedule_data.intervalUnit not in _INTERVAL_SECONDS:
            raise HTTPException(status_code=422, detail="Interval schedules require a valid intervalUnit")
        normalized["intervalValue"] = schedule_data.intervalValue
        normalized["intervalUnit"] = schedule_data.intervalUnit
    elif schedule_type == "cron":
        cron_expression = (schedule_data.cronExpression or "").strip()
        if not cron_expression:
            raise HTTPException(status_code=422, detail="Cron schedules require a cronExpression")
        try:
            CronParser.get_next_run(cron_expression, datetime.now(UTC))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        normalized["cronExpression"] = cron_expression
    elif schedule_type == "once":
        if not schedule_data.runAt:
            raise HTTPException(status_code=422, detail="One-time schedules require runAt")
        normalized["runAt"] = _parse_iso_datetime(schedule_data.runAt).isoformat()

    next_run = _schedule_next_run(normalized)
    return normalized, next_run


def _extract_schedule(workflow: Workflow) -> dict[str, Any] | None:
    stored = workflow.metadata.get("schedule")
    if isinstance(stored, dict):
        return stored

    for node in workflow.nodes:
        if node.type != NodeType.SCHEDULE:
            continue

        cron_expression = node.config.get("cron") or node.config.get("cronExpression")
        if isinstance(cron_expression, str) and cron_expression:
            return {
                "type": "cron",
                "enabled": True,
                "timezone": "UTC",
                "cronExpression": cron_expression,
            }

    return None


def _workflow_last_run(workflow: Workflow) -> str | None:
    runtime_state = _schedule_runtime_state(workflow)
    if runtime_state is not None:
        last_run = runtime_state.get("last_run")
        if isinstance(last_run, str) and last_run:
            return last_run

    last_run = workflow.metadata.get("last_run")
    if isinstance(last_run, str) and last_run:
        return last_run

    return None


def _workflow_next_run(workflow: Workflow, schedule: dict[str, Any] | None) -> str | None:
    if "next_run" in workflow.metadata:
        next_run = workflow.metadata.get("next_run")
        if isinstance(next_run, str) and next_run:
            return next_run
        return None

    return _schedule_next_run(schedule)


def _workflow_category(workflow: Workflow) -> str:
    category = workflow.metadata.get("workflow_type")
    if isinstance(category, str) and category:
        return category

    node_types = {node.type for node in workflow.nodes}
    if NodeType.WEBHOOK in node_types or NodeType.HTTP_REQUEST in node_types:
        return "integration"
    if NodeType.AGENT in node_types:
        return "ai"
    if NodeType.DATABASE in node_types or NodeType.TRANSFORM in node_types:
        return "data"
    return "automation"


def _workflow_definition(workflow: Workflow) -> dict[str, Any]:
    steps = []
    for index, node in enumerate(workflow.nodes):
        step: dict[str, Any] = {
            "id": node.id,
            "name": node.name,
            "type": node.type.value,
            "order": index,
        }
        description = node.config.get("description")
        if isinstance(description, str) and description:
            step["description"] = description
        agent_type = node.config.get("agent_type")
        if isinstance(agent_type, str) and agent_type:
            step["agent_type"] = agent_type
        steps.append(step)

    return {"steps": steps, "tasks": steps}


def _workflow_trigger(workflow: Workflow) -> dict[str, Any]:
    webhook_path = workflow.metadata.get("webhook_path")
    if isinstance(webhook_path, str) and webhook_path:
        return {
            "type": "webhook",
            "path": webhook_path,
            "url": f"/api/workflows/webhooks/{webhook_path}",
        }

    schedule = _extract_schedule(workflow)
    if schedule and schedule.get("enabled"):
        return {"type": "schedule", "config": schedule}

    targets = {edge.target for edge in workflow.edges}
    trigger_node = next((node for node in workflow.nodes if node.id not in targets), None)
    if trigger_node:
        if trigger_node.type == NodeType.WEBHOOK:
            return {"type": trigger_node.type.value, "config": trigger_node.config}

        return {"type": trigger_node.type.value, "config": trigger_node.config}

    return {"type": "manual", "config": {}}


def _serialize_workflow_response(workflow: Workflow) -> dict[str, Any]:
    payload = workflow.to_dict()
    schedule = _extract_schedule(workflow)
    payload.update(
        {
            "definition": _workflow_definition(workflow),
            "workflow_type": _workflow_category(workflow),
            "trigger": _workflow_trigger(workflow),
            "schedule": schedule,
            "next_run": _workflow_next_run(workflow, schedule),
            "last_run": _workflow_last_run(workflow),
            "agents": [
                agent_type
                for agent_type in [node.config.get("agent_type") for node in workflow.nodes]
                if isinstance(agent_type, str) and agent_type
            ],
            "execution_count": int(workflow.metadata.get("execution_count") or 0),
            "success_rate": float(workflow.metadata.get("success_rate") or 0),
        }
    )
    return payload


@router.get("/")
async def list_workflows(
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    page: int | None = None,
    page_size: int | None = None,
    search: str | None = None,
    user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """
    List all workflows.

    Equivalent to n8n GET /workflows
    """
    user_id = _current_user_id(user)
    if page_size is not None:
        limit = page_size
    if page is not None:
        offset = max(page - 1, 0) * limit

    workflows = [
        workflow
        for workflow in await _list_engine_workflows(status=WorkflowStatus(status) if status else None)
        if _is_owned_workflow(workflow, user_id)
    ]

    if search:
        workflows = [workflow for workflow in workflows if _workflow_matches_search(workflow, search)]

    return {
        "workflows": [_serialize_workflow_response(w) for w in workflows[offset : offset + limit]],
        "total": len(workflows),
    }


@router.post("/")
async def create_workflow(workflow_data: WorkflowCreateModel, user: dict[str, Any] = Depends(get_current_user)) -> dict:
    """
    Create a new workflow.

    Equivalent to n8n POST /workflows
    """
    import uuid

    user_id = _current_user_id(user)
    nodes, edges = _build_workflow_graph(
        workflow_data.nodes,
        workflow_data.edges,
        workflow_data.definition,
        workflow_data.steps,
        workflow_data.tasks,
    )
    metadata: dict[str, Any] = {"created_by": user_id}
    if workflow_data.workflow_type:
        metadata["workflow_type"] = workflow_data.workflow_type
    if isinstance(workflow_data.schedule, dict):
        schedule_payload, next_run = _normalize_schedule_payload(WorkflowScheduleModel(**workflow_data.schedule))
        metadata["schedule"] = schedule_payload
        metadata["next_run"] = next_run

    workflow = Workflow(
        id=str(uuid.uuid4()),
        name=workflow_data.name,
        description=workflow_data.description,
        nodes=nodes,
        edges=edges,
        tags=workflow_data.tags,
        metadata=metadata,
    )
    _ensure_schedule_trigger_node(workflow, metadata.get("schedule"))

    await _register_engine_workflow(workflow)
    metadata_changed = await _sync_workflow_schedule_runtime(workflow)
    if metadata_changed:
        await _save_engine_workflow(workflow.id)

    return _serialize_workflow_response(workflow)


@router.get("/templates")
async def get_workflow_templates(user: dict[str, Any] = Depends(get_current_user)) -> dict:
    """
    List workflow templates available to the user.

    Used by the mobile app TemplatesScreen and any client browsing pre-built
    workflow starters.  Returns a static catalogue; extend this endpoint when
    a database-backed templates table is added.
    """
    templates = [
        {
            "id": "tpl-sequential",
            "name": "Sequential Agent Pipeline",
            "description": "Run a chain of agents one after another, passing output between steps.",
            "category": "automation",
            "steps_count": 3,
            "uses_count": 0,
        },
        {
            "id": "tpl-data-sync",
            "name": "Data Sync Workflow",
            "description": "Pull data from an external source, transform it, and push to a target.",
            "category": "data",
            "steps_count": 4,
            "uses_count": 0,
        },
        {
            "id": "tpl-ai-analysis",
            "name": "AI Content Analysis",
            "description": "Feed documents to an AI agent for summarisation and tagging.",
            "category": "ai",
            "steps_count": 2,
            "uses_count": 0,
        },
        {
            "id": "tpl-integration",
            "name": "Integration Hub Connector",
            "description": "Connect two external services via Helix Spirals with error handling.",
            "category": "integration",
            "steps_count": 5,
            "uses_count": 0,
        },
    ]
    return {"templates": templates, "total": len(templates)}


@router.get("/stats")
async def get_workflow_stats(user: dict[str, Any] = Depends(get_current_user)) -> dict:
    """
    Return high-level workflow execution statistics.

    Aggregates from the in-process engine; will reflect DB data when a
    persistent execution store is wired in.
    """
    user_id = _current_user_id(user)
    workflows = [workflow for workflow in await _list_engine_workflows() if _is_owned_workflow(workflow, user_id)]
    total = len(workflows)
    active = sum(1 for w in workflows if w.status == WorkflowStatus.ACTIVE)

    recent_executions = []
    for wf in workflows[:10]:
        workflow_executions = await _list_engine_executions(workflow_id=wf.id, limit=5)
        for execution in workflow_executions:
            recent_executions.append(
                {
                    "execution_id": getattr(execution, "id", ""),
                    "workflow_id": wf.id,
                    "status": getattr(execution, "status", "unknown"),
                    "started_at": getattr(execution, "started_at", datetime.now(UTC).isoformat()),
                    "execution_time": getattr(execution, "duration_ms", None),
                }
            )

    return {
        "total_workflows": total,
        "active_workflows": active,
        "recent_executions": recent_executions,
        "_simulated": False,
    }


@router.get("/health")
async def health_check() -> dict:
    """
    Health check endpoint.
    """
    workflows = await _list_engine_workflows()
    max_executions = getattr(engine, "_MAX_EXECUTIONS", 100)
    executions = await _list_engine_executions(limit=max_executions)

    return {
        "status": "healthy",
        "engine": "helix-workflow-engine",
        "version": "1.0.0",
        "workflows": len(workflows),
        "executions": len(executions),
        "integrations": len(integration_registry._integrations),
        "scheduler": get_workflow_scheduler_health(),
    }


@router.get("/executions")
async def list_executions(
    workflow_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    user_id = _current_user_id(user)
    executions = [
        execution
        for execution in await _list_engine_executions(
            workflow_id=workflow_id,
            status=WorkflowStatus(status) if status else None,
            limit=max(limit + offset, limit),
        )
        if await _is_owned_execution(execution, user_id)
    ]

    executions.sort(key=lambda execution: execution.start_time, reverse=True)
    window = executions[offset : offset + limit]

    return {
        "executions": [_serialize_execution_response(execution) for execution in window],
        "total": len(executions),
    }


@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str, user: dict[str, Any] = Depends(get_current_user)) -> dict:
    """
    Get a workflow by ID.

    Equivalent to n8n GET /workflows/{id}
    """
    workflow = await _get_owned_workflow(workflow_id, user)

    return _serialize_workflow_response(workflow)


@router.put("/{workflow_id}")
async def update_workflow(
    workflow_id: str, workflow_data: WorkflowUpdateModel, user: dict[str, Any] = Depends(get_current_user)
) -> dict:
    """
    Update a workflow.

    Equivalent to n8n PUT /workflows/{id}
    """
    workflow = await _get_owned_workflow(workflow_id, user)

    # Update fields
    if workflow_data.name:
        workflow.name = workflow_data.name
    if workflow_data.description:
        workflow.description = workflow_data.description
    if (
        workflow_data.definition is not None
        or workflow_data.steps is not None
        or workflow_data.tasks is not None
        or workflow_data.nodes is not None
    ):
        workflow.nodes, workflow.edges = _build_workflow_graph(
            workflow_data.nodes,
            workflow_data.edges,
            workflow_data.definition,
            workflow_data.steps,
            workflow_data.tasks,
        )
    elif workflow_data.edges is not None:
        workflow.edges = [
            WorkflowEdge(id=e.id, source=e.source, target=e.target, condition=e.condition) for e in workflow_data.edges
        ]
    if workflow_data.tags is not None:
        workflow.tags = workflow_data.tags
    if workflow_data.status:
        workflow.status = _coerce_workflow_status(workflow_data.status)
    if workflow_data.workflow_type is not None:
        workflow.metadata["workflow_type"] = workflow_data.workflow_type
    if isinstance(workflow_data.schedule, dict):
        schedule_payload, next_run = _normalize_schedule_payload(WorkflowScheduleModel(**workflow_data.schedule))
        workflow.metadata["schedule"] = schedule_payload
        workflow.metadata["next_run"] = next_run
        workflow.metadata.pop("schedule_runtime", None)
        _ensure_schedule_trigger_node(workflow, schedule_payload)

    workflow.updated_at = datetime.now(UTC)
    await _save_engine_workflow(workflow.id)
    metadata_changed = await _sync_workflow_schedule_runtime(workflow)
    if metadata_changed:
        await _save_engine_workflow(workflow.id)

    return _serialize_workflow_response(workflow)


@router.post("/{workflow_id}/activate")
async def activate_workflow(workflow_id: str, user: dict[str, Any] = Depends(get_current_user)) -> dict:
    workflow = await _get_owned_workflow(workflow_id, user)
    workflow.status = WorkflowStatus.ACTIVE
    workflow.updated_at = datetime.now(UTC)
    await _save_engine_workflow(workflow.id)
    metadata_changed = await _sync_workflow_schedule_runtime(workflow)
    if metadata_changed:
        await _save_engine_workflow(workflow.id)
    return _serialize_workflow_response(workflow)


@router.post("/{workflow_id}/pause")
async def pause_workflow(workflow_id: str, user: dict[str, Any] = Depends(get_current_user)) -> dict:
    workflow = await _get_owned_workflow(workflow_id, user)
    workflow.status = WorkflowStatus.PAUSED
    _clear_workflow_next_run(workflow)
    workflow.updated_at = datetime.now(UTC)
    await _save_engine_workflow(workflow.id)
    metadata_changed = await _sync_workflow_schedule_runtime(workflow)
    if metadata_changed:
        await _save_engine_workflow(workflow.id)
    return _serialize_workflow_response(workflow)


@router.post("/{workflow_id}/duplicate")
async def duplicate_workflow(
    workflow_id: str,
    duplicate_data: WorkflowDuplicateModel,
    user: dict[str, Any] = Depends(get_current_user),
) -> dict:
    import uuid

    source_workflow = await _get_owned_workflow(workflow_id, user)
    copied_metadata = dict(source_workflow.metadata)
    copied_metadata.pop("webhook_path", None)
    copied_metadata.pop("next_run", None)
    copied_metadata["created_by"] = _current_user_id(user)

    duplicate = Workflow(
        id=str(uuid.uuid4()),
        name=duplicate_data.name or f"{source_workflow.name} Copy",
        description=source_workflow.description,
        nodes=[
            WorkflowNode(
                id=node.id,
                type=node.type,
                name=node.name,
                config=dict(node.config),
                position=dict(node.position),
            )
            for node in source_workflow.nodes
        ],
        edges=[
            WorkflowEdge(id=edge.id, source=edge.source, target=edge.target, condition=edge.condition)
            for edge in source_workflow.edges
        ],
        status=WorkflowStatus.INACTIVE,
        tags=list(source_workflow.tags),
        metadata=copied_metadata,
    )

    await _register_engine_workflow(duplicate)
    metadata_changed = await _sync_workflow_schedule_runtime(duplicate)
    if metadata_changed:
        await _save_engine_workflow(duplicate.id)
    return _serialize_workflow_response(duplicate)


@router.get("/{workflow_id}/webhook")
async def get_workflow_webhook(workflow_id: str, user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
    workflow = await _get_owned_workflow(workflow_id, user)
    webhook_path = workflow.metadata.get("webhook_path")
    if not isinstance(webhook_path, str) or not webhook_path:
        webhook_path = f"wh_{secrets.token_urlsafe(18)}"
        workflow.metadata["webhook_path"] = webhook_path
        workflow.updated_at = datetime.now(UTC)
        await _save_engine_workflow(workflow.id)

    return {
        "path": webhook_path,
        "url": f"/api/workflows/webhooks/{webhook_path}",
    }


@router.put("/{workflow_id}/schedule")
async def update_workflow_schedule(
    workflow_id: str,
    schedule_data: WorkflowScheduleModel,
    user: dict[str, Any] = Depends(get_current_user),
) -> dict:
    workflow = await _get_owned_workflow(workflow_id, user)
    schedule_payload, next_run = _normalize_schedule_payload(schedule_data)
    workflow.metadata["schedule"] = schedule_payload
    workflow.metadata["next_run"] = next_run
    workflow.metadata.pop("schedule_runtime", None)
    _ensure_schedule_trigger_node(workflow, schedule_payload)
    workflow.updated_at = datetime.now(UTC)
    await _save_engine_workflow(workflow.id)
    metadata_changed = await _sync_workflow_schedule_runtime(workflow)
    if metadata_changed:
        await _save_engine_workflow(workflow.id)
    return _serialize_workflow_response(workflow)


@router.delete("/{workflow_id}")
async def delete_workflow(workflow_id: str, user: dict[str, Any] = Depends(get_current_user)) -> dict:
    """
    Delete a workflow.

    Equivalent to n8n DELETE /workflows/{id}
    """
    await _get_owned_workflow(workflow_id, user)
    _remove_local_workflow_schedule(workflow_id)

    await _delete_engine_workflow(workflow_id)

    return {"message": "Workflow deleted", "workflow_id": workflow_id}


@router.api_route("/webhooks/{webhook_path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def trigger_workflow_webhook(webhook_path: str, request: Request) -> dict[str, Any]:
    workflows = await _list_engine_workflows()
    workflow = next(
        (
            item
            for item in workflows
            if isinstance(item.metadata.get("webhook_path"), str) and item.metadata.get("webhook_path") == webhook_path
        ),
        None,
    )
    if not workflow:
        raise HTTPException(status_code=404, detail="Webhook not found")

    try:
        body = await request.json()
    except Exception:
        body = {}

    execution = await engine.execute_workflow(
        workflow_id=workflow.id,
        input_data={
            "body": body,
            "method": request.method,
            "query": dict(request.query_params),
        },
    )
    execution.metadata["trigger"] = "webhook"

    return {
        "execution_id": execution.id,
        "workflow_id": workflow.id,
        "status": execution.status.value,
    }


@router.post("/{workflow_id}/execute")
async def execute_workflow(
    workflow_id: str,
    execution_data: WorkflowExecuteModel,
    background_tasks: BackgroundTasks,
    user: dict[str, Any] = Depends(get_current_user),
) -> ExecutionResponse:
    """
    Execute a workflow.

    Equivalent to n8n POST /webhook/{webhook_id}
    """
    try:
        user_id = _current_user_id(user)
        await _get_owned_workflow(workflow_id, user)
        input_data = execution_data.input_data or execution_data.input or {}
        execution = await engine.execute_workflow(workflow_id=workflow_id, input_data=input_data)
        execution.metadata["created_by"] = user_id
        execution.metadata["trigger"] = "manual"

        return _serialize_execution_response(execution)

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Workflow execution not found: %s", e)
        raise HTTPException(status_code=404, detail="Workflow execution not found") from e
    except Exception as e:
        logger.error("Workflow execution failed: %s", e)
        raise HTTPException(status_code=500, detail="Workflow execution failed") from e


@router.get("/{workflow_id}/executions")
async def list_workflow_executions(
    workflow_id: str,
    status: str | None = None,
    limit: int = 100,
    user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """
    List executions for a workflow.

    Equivalent to n8n GET /executions
    """
    user_id = _current_user_id(user)
    await _get_owned_workflow(workflow_id, user)
    executions = [
        execution
        for execution in await _list_engine_executions(
            workflow_id=workflow_id,
            status=WorkflowStatus(status) if status else None,
            limit=limit,
        )
        if await _is_owned_execution(execution, user_id)
    ]

    return {
        "executions": [_serialize_execution_response(execution) for execution in executions],
        "total": len(executions),
    }


@router.post("/executions/{execution_id}/replay")
async def replay_execution(execution_id: str, user: dict[str, Any] = Depends(get_current_user)) -> ExecutionResponse:
    execution = await _get_engine_execution(execution_id)
    user_id = _current_user_id(user)

    if not execution or not await _is_owned_execution(execution, user_id):
        raise HTTPException(status_code=404, detail="Execution not found")

    replayed = await engine.execute_workflow(
        workflow_id=execution.workflow_id,
        input_data=execution.input_data,
    )
    replayed.metadata["created_by"] = user_id
    replayed.metadata["replayed_from_execution_id"] = execution.id
    replayed.metadata["trigger"] = "replay"

    return _serialize_execution_response(replayed)


@router.post("/executions/{execution_id}/cancel")
async def cancel_execution(execution_id: str, user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
    """
    Cancel a running workflow execution.

    Stops the execution at the next step boundary and marks it as cancelled.
    """
    execution = await _get_engine_execution(execution_id)
    user_id = _current_user_id(user)

    if not execution or not await _is_owned_execution(execution, user_id):
        raise HTTPException(status_code=404, detail="Execution not found")

    if execution.status not in (WorkflowStatus.EXECUTING, WorkflowStatus.ACTIVE):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel execution with status '{execution.status.value}'. Only executing workflows can be cancelled.",
        )

    cancelled = await engine.cancel_execution(execution_id)

    if cancelled:
        return {"status": "cancelled", "execution_id": execution_id, "message": "Workflow execution cancelled"}
    else:
        raise HTTPException(status_code=400, detail="Execution could not be cancelled")


@router.get("/executions/{execution_id}")
async def get_execution(execution_id: str, user: dict[str, Any] = Depends(get_current_user)) -> ExecutionResponse:
    """
    Get execution by ID.

    Equivalent to n8n GET /executions/{id}
    """
    execution = await _get_engine_execution(execution_id)

    if not execution or not await _is_owned_execution(execution, _current_user_id(user)):
        raise HTTPException(status_code=404, detail="Execution not found")

    return _serialize_execution_response(execution)


# Integration Endpoints


@router.get("/integrations/")
async def list_integrations(
    category: str | None = None, user: dict[str, Any] = Depends(get_current_user)
) -> dict[str, Any]:
    """
    List available integrations.
    """
    if category:
        integrations = integration_registry.list_by_category(category)
    else:
        integrations = integration_registry.list_all()

    return {
        "integrations": [
            {
                "name": i.name,
                "category": i.category,
                "auth_type": i.auth_type,
                "description": f"{i.name} integration",
            }
            for i in integrations
        ],
        "total": len(integrations),
    }


@router.get("/integrations/popular")
async def get_popular_integrations_list() -> dict[str, Any]:
    """
    Get list of popular integrations.
    """
    return {
        "popular": get_popular_integrations(),
        "total": len(get_popular_integrations()),
    }


@router.post("/integrations/{integration_name}/test")
async def test_integration(
    integration_name: str, credentials: dict, user: dict[str, Any] = Depends(get_current_user)
) -> dict:
    """
    Test an integration connection.
    """
    integration = integration_registry.get(integration_name)

    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")

    await integration.authenticate(credentials)
    result = await integration.test_connection()

    return {"success": result.success, "output": result.output, "error": result.error}


# Node Types Endpoints


@router.get("/nodes/")
async def list_node_types(user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
    """
    List available node types.

    Equivalent to n8n GET /node-types
    """
    node_types = [
        {
            "name": "trigger.webhook",
            "displayName": "Webhook",
            "group": ["trigger"],
            "version": 1,
            "description": "Triggers workflow on incoming webhook",
            "inputs": [],
            "outputs": ["main"],
            "properties": [
                {
                    "name": "httpMethod",
                    "type": "options",
                    "options": ["GET", "POST", "PUT", "DELETE"],
                    "default": "POST",
                },
                {"name": "path", "type": "string", "default": ""},
            ],
        },
        {
            "name": "agent.kael",
            "displayName": "Kael Agent",
            "group": ["agent"],
            "version": 1,
            "description": "Analytical reasoning agent",
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": [
                {
                    "name": "task",
                    "type": "string",
                    "description": "Task for the agent to perform",
                }
            ],
        },
        {
            "name": "agent.lumina",
            "displayName": "Lumina Agent",
            "group": ["agent"],
            "version": 1,
            "description": "Creative synthesis agent",
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": [
                {
                    "name": "task",
                    "type": "string",
                    "description": "Task for the agent to perform",
                }
            ],
        },
        {
            "name": "agent.vega",
            "displayName": "Vega Agent",
            "group": ["agent"],
            "version": 1,
            "description": "Executive agent for task execution",
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": [
                {
                    "name": "task",
                    "type": "string",
                    "description": "Task for the agent to perform",
                }
            ],
        },
        {
            "name": "http.request",
            "displayName": "HTTP Request",
            "group": ["action"],
            "version": 1,
            "description": "Make an HTTP request",
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": [
                {
                    "name": "method",
                    "type": "options",
                    "options": ["GET", "POST", "PUT", "DELETE", "PATCH"],
                    "default": "GET",
                },
                {"name": "url", "type": "string", "required": True},
                {"name": "headers", "type": "collection", "default": {}},
                {"name": "body", "type": "json"},
            ],
        },
        {
            "name": "transform.data",
            "displayName": "Data Transform",
            "group": ["transform"],
            "version": 1,
            "description": "Transform data using JSONPath or mapping",
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": [
                {
                    "name": "type",
                    "type": "options",
                    "options": ["json_path", "map", "filter"],
                    "default": "json_path",
                },
                {"name": "expression", "type": "string", "required": True},
            ],
        },
        {
            "name": "code.python",
            "displayName": "Python Code",
            "group": ["code"],
            "version": 1,
            "description": "Execute Python code",
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": [
                {
                    "name": "code",
                    "type": "string",
                    "typeOptions": {"editor": "codeEditor", "editorLanguage": "python"},
                    "required": True,
                }
            ],
        },
        {
            "name": "condition.if",
            "displayName": "If Condition",
            "group": ["logic"],
            "version": 1,
            "description": "Conditional branching",
            "inputs": ["main"],
            "outputs": ["true", "false"],
            "properties": [
                {
                    "name": "condition",
                    "type": "string",
                    "description": "JavaScript expression",
                    "required": True,
                }
            ],
        },
    ]

    return {"nodeTypes": node_types, "total": len(node_types)}
