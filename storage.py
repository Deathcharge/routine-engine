"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

🌀 Helix Spirals Storage Layer
PostgreSQL + Redis storage with Context Vault integration
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import asyncpg
import redis.asyncio as redis

from .credential_encryption import decrypt_config, encrypt_config
from .models import ExecutionContext, ExecutionStatus, Spiral, TriggerType

logger = logging.getLogger(__name__)


class SpiralStorage:
    """Storage layer for Helix Spirals with Context Vault integration"""

    def __init__(self, pg_pool: asyncpg.Pool, redis_client: redis.Redis):
        self.pg_pool = pg_pool
        self.redis_client = redis_client

    async def initialize(self):
        """Initialize database schema"""
        await self._create_tables()
        await self._migrate_tables()
        await self._create_indexes()
        logger.info("✅ Storage layer initialized")

    async def _migrate_tables(self):
        """Apply incremental schema migrations for existing deployments."""
        async with self.pg_pool.acquire() as conn:
            # Add user_id column if not present (idempotent)
            await conn.execute("ALTER TABLE spirals ADD COLUMN IF NOT EXISTS user_id VARCHAR(255)")

    async def _create_tables(self):
        """Create database tables for Helix Spirals"""
        async with self.pg_pool.acquire() as conn:
            # Spirals table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS spirals (
                    id UUID PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    version VARCHAR(50) DEFAULT '1.0.0',
                    enabled BOOLEAN DEFAULT true,
                    tags TEXT[],
                    trigger_data JSONB NOT NULL,
                    actions_data JSONB NOT NULL,
                    variables_data JSONB,
                    rate_limiting JSONB,
                    scheduling JSONB,
                    security JSONB,
                    metadata JSONB,
                    performance_score INTEGER DEFAULT 5,
                    assigned_agents TEXT[],
                    user_id VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """
            )

            # Execution history table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_history (
                    id UUID PRIMARY KEY,
                    spiral_id UUID NOT NULL,
                    execution_id UUID UNIQUE NOT NULL,
                    trigger_data JSONB NOT NULL,
                    variables JSONB,
                    logs JSONB,
                    status VARCHAR(50) NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    current_action UUID,
                    error_data JSONB,
                    metrics JSONB,
                    ucf_impact JSONB,
                    performance_score INTEGER DEFAULT 5,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """
            )

            # Spiral data table (Context Vault)
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS spiral_data (
                    key VARCHAR(255) PRIMARY KEY,
                    value JSONB NOT NULL,
                    ttl INTEGER,
                    encrypted BOOLEAN DEFAULT false,
                    ucf_metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """
            )

            # Webhook mappings table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS webhook_mappings (
                    webhook_id VARCHAR(255) PRIMARY KEY,
                    spiral_id UUID NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """
            )

            # UCF metrics table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ucf_metrics (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    spiral_id UUID,
                    execution_id UUID,
                    metric VARCHAR(50) NOT NULL,
                    value FLOAT NOT NULL,
                    operation VARCHAR(50),
                    source VARCHAR(255),
                    performance_score INTEGER,
                    timestamp TIMESTAMP DEFAULT NOW()
                )
            """
            )

            # Spiral statistics table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS spiral_statistics (
                    spiral_id UUID PRIMARY KEY,
                    total_executions INTEGER DEFAULT 0,
                    successful_executions INTEGER DEFAULT 0,
                    failed_executions INTEGER DEFAULT 0,
                    average_execution_time_ms FLOAT DEFAULT 0,
                    last_execution TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """
            )

            # Step checkpoints — time-travel debugging (P3)
            # One row per (execution_id, action_index) records the full context
            # variables snapshot *after* each action completes, enabling rewind.
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS spiral_step_checkpoints (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    execution_id UUID NOT NULL,
                    spiral_id UUID NOT NULL,
                    action_index INTEGER NOT NULL,
                    action_id UUID,
                    action_name VARCHAR(255),
                    action_type VARCHAR(100),
                    variables_snapshot JSONB NOT NULL,
                    logs_snapshot JSONB,
                    status VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE (execution_id, action_index)
                )
            """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_step_ckpts_exec ON spiral_step_checkpoints(execution_id)"
            )

            # Held tasks table (buffer triggers when spiral is disabled)
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS spiral_held_tasks (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    spiral_id UUID NOT NULL,
                    trigger_type VARCHAR(50) NOT NULL,
                    trigger_data JSONB NOT NULL,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    expires_at TIMESTAMP,
                    status VARCHAR(20) DEFAULT 'held'
                )
            """
            )

            # Custom templates table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS custom_templates (
                    id VARCHAR(255) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    category VARCHAR(100),
                    icon VARCHAR(50),
                    difficulty VARCHAR(50),
                    estimated_time VARCHAR(50),
                    popular BOOLEAN DEFAULT false,
                    tags TEXT[],
                    config JSONB NOT NULL,
                    user_id VARCHAR(255),
                    public BOOLEAN DEFAULT false,
                    usage_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """
            )

    async def _create_indexes(self):
        """Create database indexes for performance"""
        async with self.pg_pool.acquire() as conn:
            # Spirals indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_spirals_enabled ON spirals(enabled)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_spirals_coordination ON spirals(performance_score)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_spirals_tags ON spirals USING GIN(tags)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_spirals_user_id ON spirals(user_id)")

            # Execution history indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_execution_spiral_id ON execution_history(spiral_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_execution_status ON execution_history(status)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_execution_started_at ON execution_history(started_at)")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_execution_coordination ON execution_history(performance_score)"
            )

            # UCF metrics indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_ucf_spiral_id ON ucf_metrics(spiral_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_ucf_metric ON ucf_metrics(metric)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_ucf_timestamp ON ucf_metrics(timestamp)")

            # Spiral data indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_spiral_data_created_at ON spiral_data(created_at)")

            # Held tasks indexes
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_held_tasks_spiral ON spiral_held_tasks(spiral_id, status)"
            )

            # Custom templates indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_custom_templates_category ON custom_templates(category)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_custom_templates_user_id ON custom_templates(user_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_custom_templates_public ON custom_templates(public)")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_custom_templates_tags ON custom_templates USING GIN(tags)"
            )

    async def save_spiral(self, spiral: Spiral) -> None:
        """Save spiral to database"""
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO spirals (
                    id, name, description, version, enabled, tags,
                    trigger_data, actions_data, variables_data,
                    rate_limiting, scheduling, security, metadata,
                    performance_score, assigned_agents, user_id, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                ON CONFLICT (id) DO UPDATE SET
                    name = $2, description = $3, version = $4, enabled = $5,
                    tags = $6, trigger_data = $7, actions_data = $8,
                    variables_data = $9, rate_limiting = $10, scheduling = $11,
                    security = $12, metadata = $13, performance_score = $14,
                    assigned_agents = $15, updated_at = $17
            """,
                spiral.id,
                spiral.name,
                spiral.description,
                spiral.version,
                spiral.enabled,
                spiral.tags,
                spiral.trigger.dict(),
                [encrypt_config(action.dict()) for action in spiral.actions],
                [var.dict() for var in spiral.variables] if spiral.variables else None,
                spiral.rate_limiting.dict() if spiral.rate_limiting else None,
                spiral.scheduling.dict() if spiral.scheduling else None,
                spiral.security.dict() if spiral.security else None,
                spiral.metadata,
                spiral.performance_score.value if spiral.performance_score else 5,
                spiral.assigned_agents,
                spiral.user_id,
                datetime.now(UTC),
            )

        # Cache in Redis for fast access
        await self.redis_client.setex(f"spiral:{spiral.id}", 3600, spiral.json())  # 1 hour cache

        logger.info("Spiral saved: %s (ID: %s)", spiral.name, spiral.id)

    async def get_spiral(self, spiral_id: str) -> Spiral | None:
        """Get spiral by ID with Redis caching"""
        # Try Redis cache first
        cached = await self.redis_client.get(f"spiral:{spiral_id}")
        if cached:
            return Spiral.parse_raw(cached)

        # Fallback to database
        async with self.pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM spirals WHERE id = $1", spiral_id)

            if not row:
                return None

            spiral_data = {
                "id": str(row["id"]),
                "name": row["name"],
                "description": row["description"],
                "version": row["version"],
                "enabled": row["enabled"],
                "tags": row["tags"] or [],
                "trigger": row["trigger_data"],
                "actions": [decrypt_config(a) for a in row["actions_data"]],
                "variables": row["variables_data"] or [],
                "rate_limiting": row["rate_limiting"],
                "scheduling": row["scheduling"],
                "security": row["security"],
                "metadata": row["metadata"] or {},
                "performance_score": row["performance_score"],
                "assigned_agents": row["assigned_agents"] or [],
                "user_id": str(row["user_id"]) if row.get("user_id") else None,
            }

            spiral = Spiral(**spiral_data)

            # Cache for next time
            await self.redis_client.setex(f"spiral:{spiral_id}", 3600, spiral.json())

            return spiral

    async def get_all_spirals(self, user_id: str | None = None) -> list[Spiral]:
        """Get spirals. Pass user_id to return only that user's spirals; omit for system-wide access (scheduler only)."""
        async with self.pg_pool.acquire() as conn:
            if user_id:
                rows = await conn.fetch(
                    "SELECT * FROM spirals WHERE user_id = $1 ORDER BY created_at DESC",
                    user_id,
                )
            else:
                rows = await conn.fetch("SELECT * FROM spirals ORDER BY created_at DESC")

            spirals = []
            for row in rows:
                spiral_data = {
                    "id": str(row["id"]),
                    "name": row["name"],
                    "description": row["description"],
                    "version": row["version"],
                    "enabled": row["enabled"],
                    "tags": row["tags"] or [],
                    "trigger": row["trigger_data"],
                    "actions": [decrypt_config(a) for a in row["actions_data"]],
                    "variables": row["variables_data"] or [],
                    "rate_limiting": row["rate_limiting"],
                    "scheduling": row["scheduling"],
                    "security": row["security"],
                    "metadata": row["metadata"] or {},
                    "performance_score": row["performance_score"],
                    "assigned_agents": row["assigned_agents"] or [],
                    "user_id": str(row["user_id"]) if row.get("user_id") else None,
                }
                spirals.append(Spiral(**spiral_data))

            return spirals

    async def get_spirals_by_trigger_type(self, trigger_type: TriggerType) -> list[Spiral]:
        """Get spirals by trigger type"""
        async with self.pg_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM spirals WHERE trigger_data->>'type' = $1 AND enabled = true",
                trigger_type.value,
            )

            spirals = []
            for row in rows:
                spiral_data = {
                    "id": str(row["id"]),
                    "name": row["name"],
                    "description": row["description"],
                    "version": row["version"],
                    "enabled": row["enabled"],
                    "tags": row["tags"] or [],
                    "trigger": row["trigger_data"],
                    "actions": [decrypt_config(a) for a in row["actions_data"]],
                    "variables": row["variables_data"] or [],
                    "rate_limiting": row["rate_limiting"],
                    "scheduling": row["scheduling"],
                    "security": row["security"],
                    "metadata": row["metadata"] or {},
                    "performance_score": row["performance_score"],
                    "assigned_agents": row["assigned_agents"] or [],
                    "user_id": str(row["user_id"]) if row.get("user_id") else None,
                }
                spirals.append(Spiral(**spiral_data))

            return spirals

    async def delete_spiral(self, spiral_id: str) -> bool:
        """Delete spiral"""
        async with self.pg_pool.acquire() as conn:
            result = await conn.execute("DELETE FROM spirals WHERE id = $1", spiral_id)

            # Remove from cache
            await self.redis_client.delete(f"spiral:{spiral_id}")

            return result == "DELETE 1"

    async def save_execution_history(self, context: ExecutionContext) -> None:
        """Save execution history with UCF tracking"""
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO execution_history (
                    id, spiral_id, execution_id, trigger_data, variables,
                    logs, status, started_at, completed_at, current_action,
                    error_data, metrics, ucf_impact, performance_score
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (execution_id) DO UPDATE SET
                    status = $7, completed_at = $9, current_action = $10,
                    error_data = $11, metrics = $12, ucf_impact = $13
            """,
                context.execution_id,
                context.spiral_id,
                context.execution_id,
                context.trigger,
                context.variables,
                [log.dict() for log in context.logs],
                context.status.value,
                datetime.fromisoformat(context.started_at),
                (datetime.fromisoformat(context.completed_at) if context.completed_at else None),
                context.current_action,
                context.error.dict() if context.error else None,
                context.metrics,
                context.ucf_impact if hasattr(context, "ucf_impact") else {},
                context.variables.get("performance_score", 5),
            )

        # Store UCF metrics separately for analytics
        if hasattr(context, "ucf_impact") and context.ucf_impact:
            await self._save_ucf_metrics(context)

        logger.info("Execution history saved: %s", context.execution_id)

    # ── Time-travel debugging (P3) ────────────────────────────────────────────

    async def save_step_checkpoint(
        self,
        context: ExecutionContext,
        action_index: int,
        action_id: str | None,
        action_name: str,
        action_type: str,
    ) -> None:
        """Persist a variables snapshot after a single action completes.

        One row per (execution_id, action_index).  Allows rewinding execution
        to any past step and re-running forward from that snapshot.
        """
        import json as _json

        try:
            async with self.pg_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO spiral_step_checkpoints (
                        execution_id, spiral_id, action_index, action_id,
                        action_name, action_type, variables_snapshot,
                        logs_snapshot, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (execution_id, action_index) DO UPDATE SET
                        variables_snapshot = EXCLUDED.variables_snapshot,
                        logs_snapshot = EXCLUDED.logs_snapshot,
                        status = EXCLUDED.status
                    """,
                    context.execution_id,
                    context.spiral_id,
                    action_index,
                    action_id,
                    action_name,
                    action_type,
                    _json.dumps(context.variables),
                    _json.dumps([log.dict() for log in context.logs]),
                    context.status.value,
                )
        except Exception as exc:
            logger.warning("Step checkpoint save failed (exec=%s step=%d): %s", context.execution_id, action_index, exc)

    async def list_step_checkpoints(self, execution_id: str) -> list[dict[str, Any]]:
        """Return all step checkpoints for an execution, ordered by action_index."""
        try:
            async with self.pg_pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT action_index, action_id, action_name, action_type, "
                    "status, created_at "
                    "FROM spiral_step_checkpoints "
                    "WHERE execution_id = $1 ORDER BY action_index",
                    execution_id,
                )
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("list_step_checkpoints failed: %s", exc)
            return []

    async def get_step_checkpoint(self, execution_id: str, action_index: int) -> dict[str, Any] | None:
        """Return full snapshot for a specific (execution_id, action_index)."""
        try:
            async with self.pg_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM spiral_step_checkpoints WHERE execution_id = $1 AND action_index = $2",
                    execution_id,
                    action_index,
                )
                return dict(row) if row else None
        except Exception as exc:
            logger.warning("get_step_checkpoint failed: %s", exc)
            return None

    # ── Held tasks (buffer triggers when spiral is disabled) ─────────────────

    async def hold_task(
        self,
        spiral_id: str,
        trigger_type: str,
        trigger_data: dict,
        metadata: dict | None = None,
        ttl_hours: int = 72,
    ) -> str:
        """Insert a held task for a disabled spiral. Returns the task id."""
        import json as _json

        task_id = str(uuid4())
        expires_at = datetime.now(UTC) + timedelta(hours=ttl_hours)
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO spiral_held_tasks
                    (id, spiral_id, trigger_type, trigger_data, metadata, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                task_id,
                spiral_id,
                trigger_type,
                _json.dumps(trigger_data),
                _json.dumps(metadata) if metadata else None,
                expires_at,
            )
        logger.info("Held task %s for spiral %s (expires %s)", task_id, spiral_id, expires_at)
        return task_id

    async def get_held_tasks(self, spiral_id: str) -> list[dict[str, Any]]:
        """Get all held tasks with status='held' and not expired, ordered by created_at ASC."""
        async with self.pg_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM spiral_held_tasks
                WHERE spiral_id = $1 AND status = 'held'
                  AND (expires_at IS NULL OR expires_at > NOW())
                ORDER BY created_at ASC
                """,
                spiral_id,
            )
            return [
                {
                    "id": str(row["id"]),
                    "spiral_id": str(row["spiral_id"]),
                    "trigger_type": row["trigger_type"],
                    "trigger_data": row["trigger_data"],
                    "metadata": row["metadata"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "expires_at": row["expires_at"].isoformat() if row["expires_at"] else None,
                    "status": row["status"],
                }
                for row in rows
            ]

    async def drain_held_tasks(self, spiral_id: str) -> list[dict[str, Any]]:
        """Get all held tasks AND mark them as 'drained' in one query."""
        async with self.pg_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                UPDATE spiral_held_tasks
                SET status = 'drained'
                WHERE spiral_id = $1 AND status = 'held'
                  AND (expires_at IS NULL OR expires_at > NOW())
                RETURNING *
                """,
                spiral_id,
            )
            return [
                {
                    "id": str(row["id"]),
                    "spiral_id": str(row["spiral_id"]),
                    "trigger_type": row["trigger_type"],
                    "trigger_data": row["trigger_data"],
                    "metadata": row["metadata"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "expires_at": row["expires_at"].isoformat() if row["expires_at"] else None,
                    "status": row["status"],
                }
                for row in rows
            ]

    async def expire_held_tasks(self) -> int:
        """Delete tasks where expires_at < NOW(). Returns count deleted."""
        async with self.pg_pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM spiral_held_tasks WHERE expires_at IS NOT NULL AND expires_at < NOW()"
            )
            count = int(result.split()[-1]) if result else 0
            if count:
                logger.info("Expired %d held tasks", count)
            return count

    async def count_held_tasks(self, spiral_id: str) -> int:
        """Count held tasks for a spiral."""
        async with self.pg_pool.acquire() as conn:
            return await conn.fetchval(
                """
                SELECT COUNT(*) FROM spiral_held_tasks
                WHERE spiral_id = $1 AND status = 'held'
                  AND (expires_at IS NULL OR expires_at > NOW())
                """,
                spiral_id,
            )

    # ─────────────────────────────────────────────────────────────────────────

    async def _save_ucf_metrics(self, context: ExecutionContext) -> None:
        """Save UCF metrics for analytics"""
        async with self.pg_pool.acquire() as conn:
            for metric, value in context.ucf_impact.items():
                await conn.execute(
                    """
                    INSERT INTO ucf_metrics (
                        spiral_id, execution_id, metric, value, source, performance_score
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                """,
                    context.spiral_id,
                    context.execution_id,
                    metric,
                    value,
                    f"spiral:{context.spiral_id}",
                    context.variables.get("performance_score", 5),
                )

    async def get_execution_history(self, execution_id: str) -> ExecutionContext | None:
        """Get execution history by ID"""
        async with self.pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM execution_history WHERE execution_id = $1", execution_id)

            if not row:
                return None

            # Reconstruct ExecutionContext
            context_data = {
                "spiral_id": str(row["spiral_id"]),
                "execution_id": str(row["execution_id"]),
                "trigger": row["trigger_data"],
                "variables": row["variables"] or {},
                "logs": row["logs"] or [],
                "status": ExecutionStatus(row["status"]),
                "started_at": row["started_at"].isoformat(),
                "completed_at": (row["completed_at"].isoformat() if row["completed_at"] else None),
                "current_action": row["current_action"],
                "error": row["error_data"],
                "metrics": row["metrics"] or {},
                "ucf_impact": row["ucf_impact"] or {},
            }

            return ExecutionContext(**context_data)

    async def get_recent_executions(self, limit: int = 100) -> list[ExecutionContext]:
        """Get recent executions"""
        async with self.pg_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM execution_history ORDER BY started_at DESC LIMIT $1",
                limit,
            )

            executions = []
            for row in rows:
                context_data = {
                    "spiral_id": str(row["spiral_id"]),
                    "execution_id": str(row["execution_id"]),
                    "trigger": row["trigger_data"],
                    "variables": row["variables"] or {},
                    "logs": row["logs"] or [],
                    "status": ExecutionStatus(row["status"]),
                    "started_at": row["started_at"].isoformat(),
                    "completed_at": (row["completed_at"].isoformat() if row["completed_at"] else None),
                    "current_action": row["current_action"],
                    "error": row["error_data"],
                    "metrics": row["metrics"] or {},
                    "ucf_impact": row["ucf_impact"] or {},
                }
                executions.append(ExecutionContext(**context_data))

            return executions

    async def update_spiral_statistics(self, spiral_id: str, context: ExecutionContext) -> None:
        """Update spiral execution statistics"""
        execution_time = 0
        if context.completed_at and context.started_at:
            start_time = datetime.fromisoformat(context.started_at)
            end_time = datetime.fromisoformat(context.completed_at)
            execution_time = (end_time - start_time).total_seconds() * 1000  # milliseconds

        async with self.pg_pool.acquire() as conn:
            # Get current stats
            current_stats = await conn.fetchrow("SELECT * FROM spiral_statistics WHERE spiral_id = $1", spiral_id)

            if current_stats:
                # Update existing stats
                total_executions = current_stats["total_executions"] + 1
                successful_executions = current_stats["successful_executions"]
                failed_executions = current_stats["failed_executions"]

                if context.status == ExecutionStatus.COMPLETED:
                    successful_executions += 1
                elif context.status == ExecutionStatus.FAILED:
                    failed_executions += 1

                # Calculate new average execution time
                current_avg = current_stats["average_execution_time_ms"]
                new_avg = ((current_avg * (total_executions - 1)) + execution_time) / total_executions

                await conn.execute(
                    """
                    UPDATE spiral_statistics SET
                        total_executions = $2,
                        successful_executions = $3,
                        failed_executions = $4,
                        average_execution_time_ms = $5,
                        last_execution = $6,
                        updated_at = $7
                    WHERE spiral_id = $1
                """,
                    spiral_id,
                    total_executions,
                    successful_executions,
                    failed_executions,
                    new_avg,
                    datetime.now(UTC),
                    datetime.now(UTC),
                )
            else:
                # Create new stats
                successful = 1 if context.status == ExecutionStatus.COMPLETED else 0
                failed = 1 if context.status == ExecutionStatus.FAILED else 0

                await conn.execute(
                    """
                    INSERT INTO spiral_statistics (
                        spiral_id, total_executions, successful_executions,
                        failed_executions, average_execution_time_ms, last_execution
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                """,
                    spiral_id,
                    1,
                    successful,
                    failed,
                    execution_time,
                    datetime.now(UTC),
                )

    async def get_statistics(self) -> dict[str, Any]:
        """Get system-wide statistics"""
        async with self.pg_pool.acquire() as conn:
            # Basic counts
            total_spirals = await conn.fetchval("SELECT COUNT(*) FROM spirals")
            enabled_spirals = await conn.fetchval("SELECT COUNT(*) FROM spirals WHERE enabled = true")
            total_executions = await conn.fetchval("SELECT COUNT(*) FROM execution_history")
            successful_executions = await conn.fetchval(
                "SELECT COUNT(*) FROM execution_history WHERE status = 'completed'"
            )
            failed_executions = await conn.fetchval("SELECT COUNT(*) FROM execution_history WHERE status = 'failed'")

            # Average execution time
            avg_time = (
                await conn.fetchval(
                    "SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) FROM execution_history WHERE completed_at IS NOT NULL"
                )
                or 0
            )

            # Last execution
            last_execution = await conn.fetchval("SELECT MAX(started_at) FROM execution_history")

            # Top spirals by execution count
            top_spirals = await conn.fetch(
                """
                SELECT s.name, s.id, COUNT(eh.id) as execution_count
                FROM spirals s
                LEFT JOIN execution_history eh ON s.id = eh.spiral_id
                GROUP BY s.id, s.name
                ORDER BY execution_count DESC
                LIMIT 10
            """
            )

            # UCF metrics summary
            ucf_summary = await conn.fetch(
                """
                SELECT metric, AVG(value) as avg_value, COUNT(*) as count
                FROM ucf_metrics
                WHERE timestamp > NOW() - INTERVAL '24 hours'
                GROUP BY metric
            """
            )

            # Coordination level distribution
            coordination_dist = await conn.fetch(
                """
                SELECT performance_score, COUNT(*) as count
                FROM execution_history
                WHERE started_at > NOW() - INTERVAL '24 hours'
                GROUP BY performance_score
                ORDER BY performance_score
            """
            )

            return {
                "total_spirals": total_spirals,
                "enabled_spirals": enabled_spirals,
                "total_executions": total_executions,
                "successful_executions": successful_executions,
                "failed_executions": failed_executions,
                "success_rate": ((successful_executions / total_executions * 100) if total_executions > 0 else 0),
                "average_execution_time_ms": float(avg_time),
                "last_execution": (last_execution.isoformat() if last_execution else None),
                "top_spirals": [
                    {
                        "name": row["name"],
                        "id": str(row["id"]),
                        "execution_count": row["execution_count"],
                    }
                    for row in top_spirals
                ],
                "ucf_metrics": {
                    row["metric"]: {
                        "average": float(row["avg_value"]),
                        "count": row["count"],
                    }
                    for row in ucf_summary
                },
                "coordination_distribution": {str(row["performance_score"]): row["count"] for row in coordination_dist},
            }

    async def save_webhook_mapping(self, webhook_id: str, spiral_id: str) -> None:
        """Save webhook to spiral mapping"""
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO webhook_mappings (webhook_id, spiral_id) VALUES ($1, $2) ON CONFLICT (webhook_id) DO UPDATE SET spiral_id = $2",
                webhook_id,
                spiral_id,
            )

    async def delete_webhook_mapping(self, webhook_id: str) -> None:
        """Delete webhook mapping"""
        async with self.pg_pool.acquire() as conn:
            await conn.execute("DELETE FROM webhook_mappings WHERE webhook_id = $1", webhook_id)

    async def get_spiral_by_zapier_hook(self, hook_id: str) -> str | None:
        """Get spiral ID by Zapier hook ID"""
        async with self.pg_pool.acquire() as conn:
            spiral_id = await conn.fetchval("SELECT spiral_id FROM webhook_mappings WHERE webhook_id = $1", hook_id)
            return str(spiral_id) if spiral_id else None

    async def create_spiral_from_zapier_hook(self, hook_id: str) -> str:
        """Auto-create spiral for unknown Zapier hook"""
        from uuid import uuid4

        from .models import Action, Trigger, WebhookTriggerConfig

        spiral_id = str(uuid4())

        # Create basic webhook spiral
        trigger = Trigger(
            type=TriggerType.WEBHOOK,
            name=f"Zapier Hook {hook_id[:8]}",
            config=WebhookTriggerConfig(endpoint=f"/webhook/{spiral_id}"),
        )

        action = Action(
            type="log_event",
            name="Log Zapier Event",
            config={
                "type": "log_event",
                "level": "info",
                "message": f"Received Zapier webhook: {hook_id}",
                "category": "zapier_migration",
            },
        )

        spiral = Spiral(
            id=spiral_id,
            name=f"Auto-created for Zapier Hook {hook_id[:8]}",
            description=f"Automatically created spiral for Zapier hook {hook_id}",
            trigger=trigger,
            actions=[action],
            tags=["zapier", "auto-created"],
            metadata={
                "zapier_hook_id": hook_id,
                "auto_created": True,
                "created_at": datetime.now(UTC).isoformat(),
            },
        )

        await self.save_spiral(spiral)
        await self.save_webhook_mapping(hook_id, spiral_id)

        logger.info("Auto-created spiral for Zapier hook: %s -> %s", hook_id, spiral_id)
        return spiral_id

    async def get_last_webhook_timestamp(self) -> str | None:
        """Get timestamp of last webhook processing"""
        async with self.pg_pool.acquire() as conn:
            timestamp = await conn.fetchval(
                "SELECT MAX(started_at) FROM execution_history WHERE trigger_data->>'type' = 'webhook'"
            )
            return timestamp.isoformat() if timestamp else None

    async def cleanup_old_data(self, days: int = 30) -> None:
        """Clean up old execution history and cached data"""
        cutoff_date = datetime.now(UTC) - timedelta(days=days)

        async with self.pg_pool.acquire() as conn:
            # Clean old execution history
            deleted_executions = await conn.fetchval(
                "DELETE FROM execution_history WHERE started_at < $1 RETURNING COUNT(*)",
                cutoff_date,
            )

            # Clean old UCF metrics
            deleted_metrics = await conn.fetchval(
                "DELETE FROM ucf_metrics WHERE timestamp < $1 RETURNING COUNT(*)",
                cutoff_date,
            )

            # Clean old spiral data with TTL
            deleted_data = await conn.fetchval(
                "DELETE FROM spiral_data WHERE created_at < $1 AND ttl IS NOT NULL RETURNING COUNT(*)",
                cutoff_date,
            )

        logger.info(
            f"Cleanup completed: {deleted_executions} executions, {deleted_metrics} metrics, {deleted_data} data entries"
        )

    async def create_spiral(self, request, user_id: str | None = None) -> Spiral:
        """Create a new spiral from request"""

        from .models import Action, Trigger

        # Build trigger from request data
        trigger_data = request.trigger
        if isinstance(trigger_data, dict):
            trigger = Trigger(**trigger_data)
        else:
            trigger = trigger_data

        # Build actions from request data
        actions = []
        for action_data in request.actions:
            if isinstance(action_data, dict):
                actions.append(Action(**action_data))
            else:
                actions.append(action_data)

        spiral = Spiral(
            id=str(uuid4()),
            name=request.name,
            description=request.description,
            enabled=request.enabled,
            tags=request.tags or [],
            trigger=trigger,
            actions=actions,
            variables=request.variables or [],
            performance_score=(request.performance_score if hasattr(request, "performance_score") else None),
            user_id=user_id,
            metadata={
                "created_at": datetime.now(UTC).isoformat(),
                "updated_at": datetime.now(UTC).isoformat(),
            },
        )

        await self.save_spiral(spiral)
        return spiral

    async def update_spiral(self, spiral_id: str, request) -> Spiral:
        """Update an existing spiral"""
        spiral = await self.get_spiral(spiral_id)
        if not spiral:
            raise ValueError(f"Spiral {spiral_id} not found")

        # Update fields if provided
        if request.name is not None:
            spiral.name = request.name
        if request.description is not None:
            spiral.description = request.description
        if request.enabled is not None:
            spiral.enabled = request.enabled
        if request.tags is not None:
            spiral.tags = request.tags
        if request.trigger is not None:
            from .models import Trigger

            spiral.trigger = Trigger(**request.trigger) if isinstance(request.trigger, dict) else request.trigger
        if request.actions is not None:
            from .models import Action

            spiral.actions = [Action(**a) if isinstance(a, dict) else a for a in request.actions]

        # Update metadata timestamp
        if spiral.metadata:
            spiral.metadata["updated_at"] = datetime.now(UTC).isoformat()
        else:
            spiral.metadata = {"updated_at": datetime.now(UTC).isoformat()}

        await self.save_spiral(spiral)
        return spiral

    async def get_spiral_statistics(self, spiral_id: str) -> dict[str, Any]:
        """Get statistics for a specific spiral"""
        async with self.pg_pool.acquire() as conn:
            stats = await conn.fetchrow("SELECT * FROM spiral_statistics WHERE spiral_id = $1", spiral_id)

            if not stats:
                return {
                    "spiral_id": spiral_id,
                    "total_executions": 0,
                    "successful_executions": 0,
                    "failed_executions": 0,
                    "average_execution_time_ms": 0,
                    "last_execution": None,
                    "success_rate": 0,
                }

            total = stats["total_executions"] or 0
            successful = stats["successful_executions"] or 0

            return {
                "spiral_id": spiral_id,
                "total_executions": total,
                "successful_executions": successful,
                "failed_executions": stats["failed_executions"] or 0,
                "average_execution_time_ms": float(stats["average_execution_time_ms"] or 0),
                "last_execution": (stats["last_execution"].isoformat() if stats["last_execution"] else None),
                "success_rate": (successful / total * 100) if total > 0 else 0,
            }

    async def get_execution_history_list(
        self,
        spiral_id: str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 50,
        offset: int = 0,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get execution history with filtering"""
        async with self.pg_pool.acquire() as conn:
            query = "SELECT * FROM execution_history WHERE 1=1"
            params = []
            param_count = 0

            if user_id:
                param_count += 1
                query += f" AND spiral_id IN (SELECT id FROM spirals WHERE user_id = ${param_count})"
                params.append(user_id)

            if spiral_id:
                param_count += 1
                query += f" AND spiral_id = ${param_count}"
                params.append(spiral_id)

            if status:
                param_count += 1
                query += f" AND status = ${param_count}"
                params.append(status.value if hasattr(status, "value") else status)

            query += f" ORDER BY started_at DESC LIMIT ${param_count + 1} OFFSET ${param_count + 2}"
            params.extend([limit, offset])

            rows = await conn.fetch(query, *params)

            return [
                {
                    "execution_id": str(row["execution_id"]),
                    "spiral_id": str(row["spiral_id"]),
                    "status": row["status"],
                    "started_at": row["started_at"].isoformat(),
                    "completed_at": (row["completed_at"].isoformat() if row["completed_at"] else None),
                    "logs": row["logs"] or [],
                }
                for row in rows
            ]

    async def list_custom_templates(
        self,
        user_id: str | None = None,
        category: str | None = None,
        public_only: bool = False,
    ) -> list[dict[str, Any]]:
        """
        List custom spiral templates

        Args:
            user_id: Filter by user ID (returns user's templates + public templates)
            category: Filter by category
            public_only: Only return public templates

        Returns:
            List of template dictionaries
        """
        async with self.pg_pool.acquire() as conn:
            query = "SELECT * FROM custom_templates WHERE 1=1"
            params = []
            param_count = 0

            if public_only:
                query += " AND public = true"
            elif user_id:
                # Return user's templates + public templates
                param_count += 1
                query += f" AND (user_id = ${param_count} OR public = true)"
                params.append(user_id)

            if category:
                param_count += 1
                query += f" AND category = ${param_count}"
                params.append(category)

            query += " ORDER BY popular DESC, usage_count DESC, created_at DESC"

            rows = await conn.fetch(query, *params)

            return [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "description": row["description"],
                    "category": row["category"],
                    "icon": row["icon"],
                    "difficulty": row["difficulty"],
                    "estimatedTime": row["estimated_time"],
                    "popular": row["popular"],
                    "tags": list(row["tags"]) if row["tags"] else [],
                    "config": row["config"],
                    "usageCount": row["usage_count"],
                    "createdAt": row["created_at"].isoformat() if row["created_at"] else None,
                }
                for row in rows
            ]

    async def get_custom_template(self, template_id: str) -> dict[str, Any] | None:
        """
        Get a specific custom template by ID

        Args:
            template_id: Template ID

        Returns:
            Template dictionary or None if not found
        """
        async with self.pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM custom_templates WHERE id = $1", template_id)

            if not row:
                return None

            return {
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "category": row["category"],
                "icon": row["icon"],
                "difficulty": row["difficulty"],
                "estimatedTime": row["estimated_time"],
                "popular": row["popular"],
                "tags": list(row["tags"]) if row["tags"] else [],
                "config": row["config"],
                "userId": row["user_id"],
                "public": row["public"],
                "usageCount": row["usage_count"],
                "createdAt": row["created_at"].isoformat() if row["created_at"] else None,
                "updatedAt": row["updated_at"].isoformat() if row["updated_at"] else None,
            }

    async def create_custom_template(
        self,
        template_id: str,
        name: str,
        description: str,
        config: dict[str, Any],
        user_id: str,
        category: str | None = None,
        icon: str | None = None,
        difficulty: str | None = None,
        estimated_time: str | None = None,
        tags: list[str] | None = None,
        public: bool = False,
    ) -> dict[str, Any]:
        """
        Create a new custom template

        Args:
            template_id: Unique template ID
            name: Template name
            description: Template description
            config: Template configuration (nodes, edges, etc.)
            user_id: User who created the template
            category: Template category
            icon: Template icon
            difficulty: Difficulty level
            estimated_time: Estimated setup time
            tags: Tags for searching
            public: Whether template is public

        Returns:
            Created template dictionary
        """
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO custom_templates (
                    id, name, description, category, icon, difficulty,
                    estimated_time, tags, config, user_id, public
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                template_id,
                name,
                description,
                category,
                icon,
                difficulty,
                estimated_time,
                tags or [],
                config,
                user_id,
                public,
            )

            return await self.get_custom_template(template_id)

    async def update_custom_template(
        self,
        template_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Update a custom template

        Args:
            template_id: Template ID to update
            updates: Dictionary of fields to update

        Returns:
            Updated template or None if not found
        """
        async with self.pg_pool.acquire() as conn:
            # Build dynamic update query
            set_clauses = []
            params = []
            param_count = 0

            allowed_fields = {
                "name",
                "description",
                "category",
                "icon",
                "difficulty",
                "estimated_time",
                "tags",
                "config",
                "public",
                "popular",
            }

            for field, value in updates.items():
                if field in allowed_fields:
                    param_count += 1
                    set_clauses.append(f"{field} = ${param_count}")
                    params.append(value)

            if not set_clauses:
                return await self.get_custom_template(template_id)

            # Always update updated_at
            param_count += 1
            set_clauses.append(f"updated_at = ${param_count}")
            params.append(datetime.now(UTC))

            # Add template_id as final param
            param_count += 1
            params.append(template_id)

            query = f"""
                UPDATE custom_templates
                SET {", ".join(set_clauses)}
                WHERE id = ${param_count}
            """

            await conn.execute(query, *params)

            return await self.get_custom_template(template_id)

    async def delete_custom_template(self, template_id: str) -> bool:
        """
        Delete a custom template

        Args:
            template_id: Template ID to delete

        Returns:
            True if deleted, False if not found
        """
        async with self.pg_pool.acquire() as conn:
            result = await conn.execute("DELETE FROM custom_templates WHERE id = $1", template_id)

            return result == "DELETE 1"

    async def increment_template_usage(self, template_id: str) -> None:
        """
        Increment usage count for a template

        Args:
            template_id: Template ID
        """
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE custom_templates
                SET usage_count = usage_count + 1
                WHERE id = $1
                """,
                template_id,
            )
