"""
Helix Workflow Engine - Scheduler
==================================

Cron-based workflow scheduling system.

Features:
- Cron expression support
- Interval scheduling
- One-time scheduling
- Timezone support
- Schedule management
- Execution history
"""

import asyncio
import contextlib
import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from apps.backend.core.redis_client import get_redis

logger = logging.getLogger(__name__)


def _coerce_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class ScheduleType(Enum):
    """Types of schedules"""

    CRON = "cron"
    INTERVAL = "interval"
    ONCE = "once"


@dataclass
class ScheduleConfig:
    """Schedule configuration"""

    schedule_type: ScheduleType
    cron_expression: str | None = None  # For CRON type
    interval_seconds: int | None = None  # For INTERVAL type
    run_at: datetime | None = None  # For ONCE type
    timezone: str = "UTC"
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "schedule_type": self.schedule_type.value,
            "cron_expression": self.cron_expression,
            "interval_seconds": self.interval_seconds,
            "run_at": self.run_at.isoformat() if self.run_at else None,
            "timezone": self.timezone,
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ScheduleConfig":
        return cls(
            schedule_type=ScheduleType(data["schedule_type"]),
            cron_expression=data.get("cron_expression"),
            interval_seconds=data.get("interval_seconds"),
            run_at=_parse_datetime(data.get("run_at")),
            timezone=data.get("timezone", "UTC"),
            enabled=data.get("enabled", True),
        )


@dataclass
class ScheduledWorkflow:
    """A scheduled workflow"""

    id: str
    workflow_id: str
    name: str
    schedule: ScheduleConfig
    input_data: dict[str, Any] = field(default_factory=dict)
    last_run: datetime | None = None
    next_run: datetime | None = None
    run_count: int = 0
    error_count: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "name": self.name,
            "schedule": self.schedule.to_dict(),
            "input_data": self.input_data,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "run_count": self.run_count,
            "error_count": self.error_count,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class ScheduleExecution:
    """Record of a scheduled execution"""

    id: str
    scheduled_workflow_id: str
    workflow_id: str
    scheduled_time: datetime
    actual_time: datetime
    status: str
    duration_ms: float = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "scheduled_workflow_id": self.scheduled_workflow_id,
            "workflow_id": self.workflow_id,
            "scheduled_time": self.scheduled_time.isoformat(),
            "actual_time": self.actual_time.isoformat(),
            "status": self.status,
            "duration_ms": self.duration_ms,
            "error": self.error,
        }


class CronParser:
    """
    Parse cron expressions.

    Format: minute hour day_of_month month day_of_week

    Examples:
    - "0 * * * *" - Every hour
    - "0 9 * * 1" - Every Monday at 9am
    - "*/5 * * * *" - Every 5 minutes
    - "0 0 1 * *" - First day of every month
    """

    @staticmethod
    def parse(expression: str) -> dict[str, list[int]]:
        """Parse cron expression into field values"""
        parts = expression.strip().split()

        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {expression}. Expected 5 fields.")

        fields = {
            "minute": CronParser._parse_field(parts[0], 0, 59),
            "hour": CronParser._parse_field(parts[1], 0, 23),
            "day_of_month": CronParser._parse_field(parts[2], 1, 31),
            "month": CronParser._parse_field(parts[3], 1, 12),
            "day_of_week": CronParser._parse_field(parts[4], 0, 6),
        }

        return fields

    @staticmethod
    def _parse_field(field: str, min_val: int, max_val: int) -> list[int]:
        """Parse a single cron field"""
        values = []

        for part in field.split(","):
            if part == "*":
                values.extend(range(min_val, max_val + 1))
            elif "/" in part:
                # Step values (e.g., */5)
                base, step = part.split("/")
                step = int(step)
                if base == "*":
                    values.extend(range(min_val, max_val + 1, step))
                else:
                    start = int(base)
                    values.extend(range(start, max_val + 1, step))
            elif "-" in part:
                # Range (e.g., 1-5)
                start, end = part.split("-")
                values.extend(range(int(start), int(end) + 1))
            else:
                # Single value
                values.append(int(part))

        return sorted(set(values))

    @staticmethod
    def get_next_run(expression: str, after: datetime | None = None) -> datetime:
        """Calculate next run time from cron expression"""
        if after is None:
            after = datetime.now(UTC)

        fields = CronParser.parse(expression)

        # Start from next minute
        current = after.replace(second=0, microsecond=0) + timedelta(minutes=1)

        # Find next matching time (max 1 year search)
        max_iterations = 525600  # minutes in a year

        for _ in range(max_iterations):
            if (
                current.minute in fields["minute"]
                and current.hour in fields["hour"]
                and current.day in fields["day_of_month"]
                and current.month in fields["month"]
                and current.weekday() in fields["day_of_week"]
            ):
                return current

            current += timedelta(minutes=1)

        raise ValueError(f"Could not find next run time for: {expression}")


class WorkflowScheduler:
    """
    Workflow scheduler for automated execution.

    Supports:
    - Cron-based scheduling
    - Interval scheduling
    - One-time scheduling
    - Timezone support
    """

    def __init__(
        self,
        workflow_engine=None,
        refresh_callback: Callable[[], Awaitable[None]] | None = None,
    ):
        self.workflow_engine = workflow_engine
        self.refresh_callback = refresh_callback
        self._schedules: dict[str, ScheduledWorkflow] = {}
        self._executions: list[ScheduleExecution] = []
        self._running = False
        self._task: asyncio.Task | None = None

    def add_schedule(
        self,
        workflow_id: str,
        name: str,
        schedule_type: ScheduleType,
        cron_expression: str | None = None,
        interval_seconds: int | None = None,
        run_at: datetime | None = None,
        input_data: dict[str, Any] | None = None,
        timezone: str = "UTC",
        schedule_id: str | None = None,
        state: dict[str, Any] | None = None,
    ) -> ScheduledWorkflow:
        """
        Add a new scheduled workflow.

        Args:
            workflow_id: ID of workflow to schedule
            name: Name for this schedule
            schedule_type: Type of schedule (CRON, INTERVAL, ONCE)
            cron_expression: Cron expression (for CRON type)
            interval_seconds: Interval in seconds (for INTERVAL type)
            run_at: Datetime to run (for ONCE type)
            input_data: Input data for workflow execution
            timezone: Timezone for scheduling

        Returns:
            ScheduledWorkflow object
        """
        import uuid

        schedule_config = ScheduleConfig(
            schedule_type=schedule_type,
            cron_expression=cron_expression,
            interval_seconds=interval_seconds,
            run_at=run_at,
            timezone=timezone,
        )

        scheduled_workflow = ScheduledWorkflow(
            id=schedule_id or str(uuid.uuid4()),
            workflow_id=workflow_id,
            name=name,
            schedule=schedule_config,
            input_data=input_data or {},
            next_run=self._calculate_next_run(schedule_config),
        )

        if state:
            scheduled_workflow.last_run = _coerce_datetime(state.get("last_run"))
            restored_next_run = _coerce_datetime(state.get("next_run"))
            if restored_next_run is not None:
                scheduled_workflow.next_run = restored_next_run
            scheduled_workflow.run_count = int(state.get("run_count", 0) or 0)
            scheduled_workflow.error_count = int(state.get("error_count", 0) or 0)
            restored_created_at = _coerce_datetime(state.get("created_at"))
            if restored_created_at is not None:
                scheduled_workflow.created_at = restored_created_at

        self._schedules[scheduled_workflow.id] = scheduled_workflow

        logger.info("📅 Added schedule: %s for workflow %s", name, workflow_id)
        logger.info("   Next run: %s", scheduled_workflow.next_run)

        return scheduled_workflow

    def remove_schedule(self, schedule_id: str) -> bool:
        """Remove a scheduled workflow"""
        if schedule_id in self._schedules:
            del self._schedules[schedule_id]
            logger.info("🗑️ Removed schedule: %s", schedule_id)
            return True
        return False

    def get_schedule(self, schedule_id: str) -> ScheduledWorkflow | None:
        """Get a scheduled workflow by ID"""
        return self._schedules.get(schedule_id)

    def list_schedules(self, workflow_id: str | None = None) -> list[ScheduledWorkflow]:
        """List all scheduled workflows"""
        schedules = list(self._schedules.values())

        if workflow_id:
            schedules = [s for s in schedules if s.workflow_id == workflow_id]

        return sorted(schedules, key=lambda s: s.next_run or datetime.max)

    def enable_schedule(self, schedule_id: str) -> bool:
        """Enable a schedule"""
        schedule = self.get_schedule(schedule_id)
        if schedule:
            schedule.schedule.enabled = True
            schedule.next_run = self._calculate_next_run(schedule.schedule)
            return True
        return False

    def disable_schedule(self, schedule_id: str) -> bool:
        """Disable a schedule"""
        schedule = self.get_schedule(schedule_id)
        if schedule:
            schedule.schedule.enabled = False
            return True
        return False

    def _calculate_next_run(self, config: ScheduleConfig) -> datetime | None:
        """Calculate next run time based on schedule config"""
        now = datetime.now(UTC)

        if config.schedule_type == ScheduleType.CRON:
            return CronParser.get_next_run(config.cron_expression, now)

        elif config.schedule_type == ScheduleType.INTERVAL:
            return now + timedelta(seconds=config.interval_seconds)

        elif config.schedule_type == ScheduleType.ONCE:
            if config.run_at and config.run_at > now:
                return config.run_at
            return None

        return None

    async def start(self):
        """Start the scheduler"""
        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True
        from apps.backend.services.background_tasks import create_tracked_task

        self._task = create_tracked_task(self._run_loop(), name="workflow-scheduler")
        logger.info("🚀 Workflow scheduler started")

    async def stop(self):
        """Stop the scheduler"""
        self._running = False

        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

        logger.info("🛑 Workflow scheduler stopped")

    async def _run_loop(self):
        """Main scheduler loop"""
        while self._running:
            try:
                await self._check_schedules()
                await asyncio.sleep(10)  # Check every 10 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Scheduler error: %s", e)
                await asyncio.sleep(60)  # Wait longer on error

    async def _check_schedules(self):
        """Check and execute due schedules"""
        if self.refresh_callback is not None:
            await self.refresh_callback()

        now = datetime.now(UTC)

        for schedule in list(self._schedules.values()):
            if not schedule.schedule.enabled:
                continue

            if schedule.next_run and schedule.next_run <= now:
                await self._execute_schedule(schedule)

    async def _load_workflow(self, workflow_id: str) -> Any | None:
        if self.workflow_engine is None:
            return None

        get_workflow_async = getattr(self.workflow_engine, "get_workflow_async", None)
        if callable(get_workflow_async):
            return await get_workflow_async(workflow_id)

        get_workflow = getattr(self.workflow_engine, "get_workflow", None)
        if callable(get_workflow):
            return get_workflow(workflow_id)

        return None

    async def _save_workflow(self, workflow_id: str) -> None:
        if self.workflow_engine is None:
            return

        save_workflow_async = getattr(self.workflow_engine, "save_workflow_async", None)
        if callable(save_workflow_async):
            await save_workflow_async(workflow_id)
            return

        save_workflow = getattr(self.workflow_engine, "save_workflow", None)
        if callable(save_workflow):
            save_workflow(workflow_id)

    async def _persist_schedule_state(
        self,
        schedule: ScheduledWorkflow,
        execution_status: str | None = None,
    ) -> None:
        workflow = await self._load_workflow(schedule.workflow_id)
        if workflow is None:
            return

        metadata = getattr(workflow, "metadata", None)
        if not isinstance(metadata, dict):
            metadata = {}
            workflow.metadata = metadata

        metadata["schedule_runtime"] = schedule.to_dict()
        metadata["next_run"] = schedule.next_run.isoformat() if schedule.next_run else None
        metadata["last_run"] = schedule.last_run.isoformat() if schedule.last_run else None

        stored_schedule = metadata.get("schedule")
        if isinstance(stored_schedule, dict):
            stored_schedule["enabled"] = schedule.schedule.enabled

        if execution_status is not None:
            execution_count = int(metadata.get("execution_count") or 0) + 1
            successful_execution_count = int(metadata.get("successful_execution_count") or 0)
            failed_execution_count = int(metadata.get("failed_execution_count") or 0)

            if execution_status == "completed":
                successful_execution_count += 1
            else:
                failed_execution_count += 1

            metadata["execution_count"] = execution_count
            metadata["successful_execution_count"] = successful_execution_count
            metadata["failed_execution_count"] = failed_execution_count
            metadata["success_rate"] = (
                round(
                    (successful_execution_count / execution_count) * 100,
                    2,
                )
                if execution_count > 0
                else 0.0
            )

        await self._save_workflow(schedule.workflow_id)

    async def _execute_schedule(self, schedule: ScheduledWorkflow):
        """Execute a scheduled workflow"""
        import uuid

        execution_id = str(uuid.uuid4())
        start_time = datetime.now(UTC)

        logger.info("⏰ Executing scheduled workflow: %s", schedule.name)

        try:
            if self.workflow_engine:
                result = await self.workflow_engine.execute_workflow(
                    workflow_id=schedule.workflow_id, input_data=schedule.input_data
                )
                if hasattr(result, "metadata") and isinstance(result.metadata, dict):
                    result.metadata["trigger"] = "schedule"
                status = "completed" if result.status.value == "completed" else "failed"
                error = result.error
            else:
                # Simulate execution if no engine
                await asyncio.sleep(0.1)
                status = "completed"
                error = None

            duration_ms = (datetime.now(UTC) - start_time).total_seconds() * 1000

            # Record execution
            execution = ScheduleExecution(
                id=execution_id,
                scheduled_workflow_id=schedule.id,
                workflow_id=schedule.workflow_id,
                scheduled_time=schedule.next_run,
                actual_time=start_time,
                status=status,
                duration_ms=duration_ms,
                error=error,
            )
            self._executions.append(execution)

            # Update schedule
            schedule.last_run = start_time
            schedule.run_count += 1

            if status == "failed":
                schedule.error_count += 1

            # Calculate next run
            if schedule.schedule.schedule_type == ScheduleType.ONCE:
                schedule.schedule.enabled = False
                schedule.next_run = None
            else:
                schedule.next_run = self._calculate_next_run(schedule.schedule)

            await self._persist_schedule_state(schedule, status)

            logger.info("✅ Scheduled execution completed: %s (%.2fms)", schedule.name, duration_ms)

        except Exception as e:
            logger.error("❌ Scheduled execution failed: %s - %s", schedule.name, e)
            schedule.last_run = start_time
            schedule.run_count += 1
            schedule.error_count += 1
            schedule.next_run = self._calculate_next_run(schedule.schedule)
            await self._persist_schedule_state(schedule, "failed")

    def get_executions(self, schedule_id: str | None = None, limit: int = 100) -> list[ScheduleExecution]:
        """Get execution history"""
        executions = self._executions

        if schedule_id:
            executions = [e for e in executions if e.scheduled_workflow_id == schedule_id]

        return sorted(executions, key=lambda e: e.actual_time, reverse=True)[:limit]

    def get_stats(self) -> dict[str, Any]:
        """Get scheduler statistics"""
        total_schedules = len(self._schedules)
        enabled_schedules = sum(1 for s in self._schedules.values() if s.schedule.enabled)
        total_executions = len(self._executions)
        successful_executions = sum(1 for e in self._executions if e.status == "completed")
        failed_executions = sum(1 for e in self._executions if e.status == "failed")

        return {
            "total_schedules": total_schedules,
            "enabled_schedules": enabled_schedules,
            "disabled_schedules": total_schedules - enabled_schedules,
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "failed_executions": failed_executions,
            "success_rate": (successful_executions / total_executions if total_executions > 0 else 0),
            "running": self._running,
        }


# Convenience functions


def create_cron_schedule(
    workflow_id: str, name: str, cron_expression: str, input_data: dict[str, Any] | None = None
) -> ScheduleConfig:
    """Create a cron schedule configuration"""
    return ScheduleConfig(schedule_type=ScheduleType.CRON, cron_expression=cron_expression)


def create_interval_schedule(
    workflow_id: str,
    name: str,
    interval_seconds: int,
    input_data: dict[str, Any] | None = None,
) -> ScheduleConfig:
    """Create an interval schedule configuration"""
    return ScheduleConfig(schedule_type=ScheduleType.INTERVAL, interval_seconds=interval_seconds)


def create_once_schedule(
    workflow_id: str,
    name: str,
    run_at: datetime,
    input_data: dict[str, Any] | None = None,
) -> ScheduleConfig:
    """Create a one-time schedule configuration"""
    return ScheduleConfig(schedule_type=ScheduleType.ONCE, run_at=run_at)


# ============================================================================
# AGENT TASK SCHEDULER
# ============================================================================

_AGENT_TASK_KEY_PREFIX = "helix:agent-scheduler:task:"
_AGENT_TASK_INDEX_KEY = "helix:agent-scheduler:tasks"
_AGENT_TASK_USER_INDEX_PREFIX = "helix:agent-scheduler:user:"
_AGENT_TASK_EXECUTION_LEASE_PREFIX = "helix:agent-scheduler:lease:"


def _allow_degraded_scheduler_storage() -> bool:
    """Allow in-memory fallback only for local development and tests."""
    environment = (os.getenv("HELIX_ENV") or os.getenv("ENVIRONMENT") or "").strip().lower()
    is_managed_runtime = bool(os.getenv("HELIX_SERVICE_NAME"))
    return os.getenv("HELIX_TESTING") == "1" or (environment == "development" and not is_managed_runtime)


def _decode_redis_value(value: str | bytes | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


@dataclass
class ScheduledAgentTask:
    """A scheduled agent task — runs a prompt against an agent on a schedule."""

    id: str
    user_id: str
    agent_id: str
    name: str
    prompt: str
    schedule: ScheduleConfig
    mode: str = "ask"  # ask, code, architect, review, create
    last_run: datetime | None = None
    next_run: datetime | None = None
    run_count: int = 0
    error_count: int = 0
    last_result: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    persist: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "agent_id": self.agent_id,
            "name": self.name,
            "prompt": self.prompt,
            "schedule": self.schedule.to_dict(),
            "mode": self.mode,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "run_count": self.run_count,
            "error_count": self.error_count,
            "last_result_preview": self.last_result[:200] if self.last_result else None,
            "created_at": self.created_at.isoformat(),
            "enabled": self.schedule.enabled,
        }

    def to_storage_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "agent_id": self.agent_id,
            "name": self.name,
            "prompt": self.prompt,
            "schedule": self.schedule.to_dict(),
            "mode": self.mode,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "run_count": self.run_count,
            "error_count": self.error_count,
            "last_result": self.last_result,
            "created_at": self.created_at.isoformat(),
            "persist": self.persist,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ScheduledAgentTask":
        return cls(
            id=data["id"],
            user_id=data["user_id"],
            agent_id=data["agent_id"],
            name=data["name"],
            prompt=data["prompt"],
            schedule=ScheduleConfig.from_dict(data["schedule"]),
            mode=data.get("mode", "ask"),
            last_run=_parse_datetime(data.get("last_run")),
            next_run=_parse_datetime(data.get("next_run")),
            run_count=data.get("run_count", 0),
            error_count=data.get("error_count", 0),
            last_result=data.get("last_result"),
            created_at=_parse_datetime(data.get("created_at")) or datetime.now(UTC),
            persist=data.get("persist", True),
        )


class AgentTaskScheduler:
    """Scheduler for recurring agent tasks.

    Runs agent prompts on a cron/interval/one-time basis using the
    UnifiedLLMService.  Results are stored per-task for inspection.
    """

    def __init__(self):
        self._tasks: dict[str, ScheduledAgentTask] = {}  # Write-through cache over Redis
        self._running = False
        self._task_handle: asyncio.Task | None = None

    def _task_key(self, task_id: str) -> str:
        return f"{_AGENT_TASK_KEY_PREFIX}{task_id}"

    def _user_index_key(self, user_id: str) -> str:
        return f"{_AGENT_TASK_USER_INDEX_PREFIX}{user_id}"

    def _execution_lease_key(self, task_id: str, scheduled_for: datetime) -> str:
        return f"{_AGENT_TASK_EXECUTION_LEASE_PREFIX}{task_id}:{scheduled_for.isoformat()}"

    async def _get_storage(self):
        storage = await get_redis()
        if storage is None and not _allow_degraded_scheduler_storage():
            raise RuntimeError("Scheduled task storage unavailable")
        return storage

    async def _persist_task(self, task: ScheduledAgentTask) -> None:
        self._tasks[task.id] = task
        if not task.persist:
            return

        storage = await self._get_storage()
        if storage is None:
            return

        payload = json.dumps(task.to_storage_dict())
        await storage.set(self._task_key(task.id), payload)
        await storage.sadd(_AGENT_TASK_INDEX_KEY, task.id)
        await storage.sadd(self._user_index_key(task.user_id), task.id)

    async def _load_task(self, task_id: str) -> ScheduledAgentTask | None:
        storage = await self._get_storage()
        if storage is None:
            return self._tasks.get(task_id)

        raw = _decode_redis_value(await storage.get(self._task_key(task_id)))
        if not raw:
            self._tasks.pop(task_id, None)
            return None

        task = ScheduledAgentTask.from_dict(json.loads(raw))
        self._tasks[task.id] = task
        return task

    async def _load_tasks(self, user_id: str | None = None) -> list[ScheduledAgentTask]:
        storage = await self._get_storage()
        if storage is None:
            tasks = list(self._tasks.values())
            if user_id:
                tasks = [task for task in tasks if task.user_id == user_id]
            return tasks

        index_key = self._user_index_key(user_id) if user_id else _AGENT_TASK_INDEX_KEY
        task_ids = await storage.smembers(index_key)
        decoded_ids = [_decode_redis_value(task_id) for task_id in task_ids]
        decoded_ids = [task_id for task_id in decoded_ids if task_id]
        if not decoded_ids:
            return []

        raw_tasks = await storage.mget([self._task_key(task_id) for task_id in decoded_ids])
        tasks: list[ScheduledAgentTask] = []
        for raw in raw_tasks:
            payload = _decode_redis_value(raw)
            if not payload:
                continue
            task = ScheduledAgentTask.from_dict(json.loads(payload))
            self._tasks[task.id] = task
            tasks.append(task)

        return tasks

    async def _delete_task(self, task: ScheduledAgentTask) -> None:
        self._tasks.pop(task.id, None)
        if not task.persist:
            return

        storage = await self._get_storage()
        if storage is None:
            return

        await storage.delete(self._task_key(task.id))
        await storage.srem(_AGENT_TASK_INDEX_KEY, task.id)
        await storage.srem(self._user_index_key(task.user_id), task.id)

    async def _sync_persisted_tasks(self) -> None:
        storage = await self._get_storage()
        if storage is None:
            return

        persistent_tasks = await self._load_tasks()
        persistent_ids = {task.id for task in persistent_tasks}
        ephemeral_tasks = {task_id: task for task_id, task in self._tasks.items() if not task.persist}

        self._tasks = {task.id: task for task in persistent_tasks}
        self._tasks.update(ephemeral_tasks)

        for task_id in [
            task_id for task_id, task in list(self._tasks.items()) if task.persist and task_id not in persistent_ids
        ]:
            self._tasks.pop(task_id, None)

    async def _claim_execution(self, task: ScheduledAgentTask, scheduled_for: datetime) -> bool:
        if not task.persist:
            return True

        storage = await self._get_storage()
        if storage is None:
            return True

        lease_seconds = max(300, (task.schedule.interval_seconds or 0) + 60)
        acquired = await storage.set(
            self._execution_lease_key(task.id, scheduled_for),
            "1",
            ex=lease_seconds,
            nx=True,
        )
        return bool(acquired)

    async def add_task(
        self,
        *,
        user_id: str,
        agent_id: str,
        name: str,
        prompt: str,
        schedule_type: ScheduleType,
        cron_expression: str | None = None,
        interval_seconds: int | None = None,
        run_at: datetime | None = None,
        mode: str = "ask",
        persist: bool = True,
    ) -> ScheduledAgentTask:
        """Create a new scheduled agent task."""
        import uuid

        config = ScheduleConfig(
            schedule_type=schedule_type,
            cron_expression=cron_expression,
            interval_seconds=interval_seconds,
            run_at=run_at,
        )

        now = datetime.now(UTC)
        if config.schedule_type == ScheduleType.CRON:
            next_run = CronParser.get_next_run(config.cron_expression, now)
        elif config.schedule_type == ScheduleType.INTERVAL:
            next_run = now + timedelta(seconds=config.interval_seconds)
        elif config.schedule_type == ScheduleType.ONCE and config.run_at and config.run_at > now:
            next_run = config.run_at
        else:
            next_run = None

        task = ScheduledAgentTask(
            id=str(uuid.uuid4()),
            user_id=user_id,
            agent_id=agent_id,
            name=name,
            prompt=prompt,
            schedule=config,
            mode=mode,
            next_run=next_run,
            persist=persist,
        )
        await self._persist_task(task)
        logger.info("Scheduled agent task '%s' (agent=%s, next=%s)", name, agent_id, next_run)
        return task

    async def remove_task(self, task_id: str) -> bool:
        task = await self.get_task(task_id)
        if not task:
            return False
        await self._delete_task(task)
        return True

    async def list_tasks(self, user_id: str | None = None) -> list[ScheduledAgentTask]:
        tasks_by_id = {task.id: task for task in await self._load_tasks(user_id)}
        for task in self._tasks.values():
            if task.persist:
                continue
            if user_id and task.user_id != user_id:
                continue
            tasks_by_id[task.id] = task
        return sorted(tasks_by_id.values(), key=lambda task: task.next_run or datetime.max)

    async def get_task(self, task_id: str) -> ScheduledAgentTask | None:
        cached_task = self._tasks.get(task_id)
        if cached_task and not cached_task.persist:
            return cached_task

        task = await self._load_task(task_id)
        if task:
            return task
        return cached_task

    async def start(self):
        if self._running:
            return
        await self._sync_persisted_tasks()
        self._running = True
        from apps.backend.services.background_tasks import create_tracked_task

        self._task_handle = create_tracked_task(self._loop(), name="agent-task-scheduler")
        logger.info("Agent task scheduler started")

    async def stop(self):
        self._running = False
        if self._task_handle:
            self._task_handle.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task_handle

    async def _loop(self):
        while self._running:
            try:
                await self._tick()
                await asyncio.sleep(15)  # Check every 15 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Agent task scheduler error: %s", e)
                await asyncio.sleep(60)

    async def _tick(self):
        await self._sync_persisted_tasks()
        now = datetime.now(UTC)
        for task in list(self._tasks.values()):
            if not task.schedule.enabled:
                continue
            if task.next_run and task.next_run <= now:
                if not await self._claim_execution(task, task.next_run):
                    continue
                await self._execute_task(task)

    async def _execute_task(self, task: ScheduledAgentTask):
        start_time = datetime.now(UTC)
        logger.info("Executing scheduled agent task: %s (agent=%s)", task.name, task.agent_id)

        try:
            from apps.backend.services.unified_llm import UnifiedLLMService

            llm = UnifiedLLMService()
            system = f"You are {task.agent_id}, an AI agent executing a scheduled task."
            result = await llm.generate(
                prompt=task.prompt,
                system=system,
                model=None,  # auto-select
                max_tokens=1024,
                temperature=0.7,
            )
            task.last_result = result
            task.run_count += 1
            task.last_run = start_time

            logger.info(
                "Scheduled agent task '%s' completed (%.0fms, %d chars)",
                task.name,
                (datetime.now(UTC) - start_time).total_seconds() * 1000,
                len(result),
            )
        except Exception as e:
            logger.error("Scheduled agent task '%s' failed: %s", task.name, e)
            task.error_count += 1
            task.last_run = start_time

        # Calculate next run
        if task.schedule.schedule_type == ScheduleType.ONCE:
            task.schedule.enabled = False
            task.next_run = None
        elif task.schedule.schedule_type == ScheduleType.CRON:
            task.next_run = CronParser.get_next_run(task.schedule.cron_expression)
        elif task.schedule.schedule_type == ScheduleType.INTERVAL:
            task.next_run = datetime.now(UTC) + timedelta(seconds=task.schedule.interval_seconds)

        await self._persist_task(task)


# Module-level singleton
_agent_scheduler: AgentTaskScheduler | None = None


def get_agent_task_scheduler() -> AgentTaskScheduler:
    """Get the module-level agent task scheduler singleton."""
    global _agent_scheduler
    if _agent_scheduler is None:
        _agent_scheduler = AgentTaskScheduler()
    return _agent_scheduler


# Common cron expressions
CRON_EVERY_MINUTE = "* * * * *"
CRON_EVERY_5_MINUTES = "*/5 * * * *"
CRON_EVERY_15_MINUTES = "*/15 * * * *"
CRON_EVERY_30_MINUTES = "*/30 * * * *"
CRON_EVERY_HOUR = "0 * * * *"
CRON_EVERY_2_HOURS = "0 */2 * * *"
CRON_EVERY_6_HOURS = "0 */6 * * *"
CRON_EVERY_12_HOURS = "0 */12 * * *"
CRON_DAILY_MIDNIGHT = "0 0 * * *"
CRON_DAILY_9AM = "0 9 * * *"
CRON_WEEKLY_MONDAY = "0 9 * * 1"
CRON_WEEKLY_FRIDAY = "0 17 * * 5"
CRON_MONTHLY_FIRST = "0 0 1 * *"
CRON_MONTHLY_LAST = "0 0 L * *"
