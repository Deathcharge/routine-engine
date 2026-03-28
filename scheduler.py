"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

Helix Spirals Scheduler
Handles scheduled, cron-based, and UCF-threshold spiral execution
with persistence across restarts and proper UCF metric sourcing.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

from .models import Spiral, TriggerType

logger = logging.getLogger(__name__)


def _try_import_croniter():
    """Attempt to import croniter; return class or None."""
    try:
        from croniter import croniter

        return croniter
    except ImportError:
        return None


def _utcnow() -> datetime:
    return datetime.now(UTC)


class SpiralScheduler:
    """Production-grade scheduler for time-based and UCF-threshold spiral triggers.

    Improvements over original:
    - Persists last_fired timestamps in storage so interval spirals don't
      double-fire on restart.
    - Reads UCF metrics from the live engine/state module instead of a
      stale JSON file.
    - Validates cron expressions at registration time and skips invalid ones.
    - Exposes health / status information for the /api/health endpoint.
    """

    # ------------------------------------------------------------------ init
    def __init__(self, engine, storage):
        self.engine = engine
        self.storage = storage
        self.scheduled_tasks: dict[str, asyncio.Task] = {}
        self.is_running = False
        self._main_task: asyncio.Task | None = None
        # Track last fire times per spiral so we honour intervals across restarts
        self._last_fired: dict[str, datetime] = {}
        # Track per-spiral UCF triggers that already fired (avoid re-firing
        # every 60 s while condition stays true)
        self._ucf_fired: dict[str, bool] = {}
        self._started_at: datetime | None = None

    # --------------------------------------------------------------- lifecycle
    async def start(self):
        """Start the scheduler and reload persisted fire times."""
        if self.is_running:
            logger.warning("Scheduler already running")
            return

        self.is_running = True
        self._started_at = _utcnow()
        logger.info("Starting Spiral Scheduler")

        # Reload persisted fire times if the storage layer supports it
        await self._load_fire_times()

        # Load all spirals that need scheduling
        spirals = await self.storage.get_all_spirals()
        registered = 0
        for spiral in spirals:
            if spiral.enabled and spiral.trigger.type == TriggerType.SCHEDULE:
                await self.register_spiral(spiral)
                registered += 1

        # Start the main scheduler loop (handles UCF threshold polling)
        self._main_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Scheduler started: %s scheduled spirals registered", registered)

    async def stop(self):
        """Gracefully stop the scheduler and persist fire times."""
        self.is_running = False

        # Persist fire times before we tear down
        await self._save_fire_times()

        # Cancel all per-spiral tasks
        for spiral_id, task in self.scheduled_tasks.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.scheduled_tasks.clear()

        # Cancel main loop
        if self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass

        logger.info("Scheduler stopped")

    # --------------------------------------------------- registration
    async def register_spiral(self, spiral: Spiral):
        """Register a spiral for scheduled execution."""
        if spiral.id in self.scheduled_tasks:
            self.scheduled_tasks[spiral.id].cancel()

        if spiral.trigger.type == TriggerType.SCHEDULE and spiral.enabled:
            # Validate cron expression at registration time
            config = spiral.trigger.config
            if hasattr(config, "cron") and config.cron:
                croniter_cls = _try_import_croniter()
                if croniter_cls:
                    try:
                        croniter_cls(config.cron)
                    except (ValueError, KeyError) as exc:
                        logger.error(
                            "Invalid cron expression '%s' for spiral %s: %s — skipping",
                            config.cron,
                            spiral.id,
                            exc,
                        )
                        return

            task = asyncio.create_task(self._schedule_spiral(spiral))
            self.scheduled_tasks[spiral.id] = task
            logger.info("Registered scheduled spiral: %s (%s)", spiral.name, spiral.id)

    async def unregister_spiral(self, spiral_id: str):
        """Unregister a spiral from scheduled execution."""
        if spiral_id in self.scheduled_tasks:
            self.scheduled_tasks[spiral_id].cancel()
            del self.scheduled_tasks[spiral_id]
            self._last_fired.pop(spiral_id, None)
            self._ucf_fired.pop(spiral_id, None)
            logger.info("Unregistered spiral: %s", spiral_id)

    # --------------------------------------------------- scheduling core
    async def _schedule_spiral(self, spiral: Spiral):
        """Handle scheduling for a single spiral (interval or cron)."""
        config = spiral.trigger.config

        try:
            if hasattr(config, "interval") and config.interval:
                await self._interval_schedule(spiral, config.interval)
            elif hasattr(config, "cron") and config.cron:
                await self._cron_schedule(spiral, config.cron)
        except asyncio.CancelledError:
            logger.debug("Scheduled task cancelled for spiral: %s", spiral.id)
        except Exception as e:
            logger.error("Error in scheduled execution for spiral %s: %s", spiral.id, e)

    async def _interval_schedule(self, spiral: Spiral, interval_ms: int):
        """Interval-based scheduling with restart-safe timing."""
        interval_seconds = max(interval_ms / 1000.0, 1.0)  # min 1 s

        # Calculate initial delay honoring last fire time
        last = self._last_fired.get(spiral.id)
        if last:
            elapsed = (_utcnow() - last).total_seconds()
            initial_wait = max(0.0, interval_seconds - elapsed)
        else:
            initial_wait = interval_seconds

        if initial_wait > 0:
            await asyncio.sleep(initial_wait)

        while self.is_running:
            if self.is_running:
                await self._execute_scheduled(spiral)
            await asyncio.sleep(interval_seconds)

    async def _cron_schedule(self, spiral: Spiral, cron_expr: str):
        """Cron-based scheduling with graceful degradation."""
        croniter_cls = _try_import_croniter()
        if not croniter_cls:
            logger.warning(
                "croniter not installed — falling back to 1-hour interval for spiral %s. "
                "Install with: pip install croniter",
                spiral.id,
            )
            await self._interval_schedule(spiral, 3_600_000)
            return

        base_time = self._last_fired.get(spiral.id, _utcnow())
        cron = croniter_cls(cron_expr, base_time)

        while self.is_running:
            next_run = cron.get_next(datetime)
            wait_seconds = (next_run - _utcnow()).total_seconds()

            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)

            if self.is_running:
                await self._execute_scheduled(spiral)
                cron = croniter_cls(cron_expr, _utcnow())

    async def _execute_scheduled(self, spiral: Spiral):
        """Execute a scheduled spiral and update fire time."""
        now = _utcnow()
        try:
            await self.engine.execute(
                spiral_id=spiral.id,
                trigger_type="schedule",
                trigger_data={
                    "scheduled_time": now.isoformat(),
                    "trigger_name": spiral.trigger.name,
                },
                metadata={"source": "scheduler"},
            )
            self._last_fired[spiral.id] = now
        except Exception as e:
            logger.error("Failed to execute scheduled spiral %s: %s", spiral.id, e)

    # --------------------------------------------------- UCF threshold loop
    async def _scheduler_loop(self):
        """Main loop: poll UCF metrics and fire threshold-based spirals."""
        check_interval = 60  # seconds

        while self.is_running:
            try:
                await self._check_ucf_thresholds()
                await asyncio.sleep(check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in scheduler loop: %s", e)
                await asyncio.sleep(check_interval)

    async def _check_ucf_thresholds(self):
        """Check UCF threshold triggers using live metrics."""
        try:
            spirals = await self.storage.get_all_spirals()
            ucf_spirals = [s for s in spirals if s.enabled and s.trigger.type == TriggerType.UCF_THRESHOLD]

            if not ucf_spirals:
                return

            ucf_state = self._read_ucf_metrics()
            if not ucf_state:
                return

            for spiral in ucf_spirals:
                config = spiral.trigger.config
                if not hasattr(config, "metric") or not hasattr(config, "threshold"):
                    continue

                metric_name = config.metric.value if hasattr(config.metric, "value") else config.metric
                current_value = ucf_state.get(metric_name, 0)
                threshold = config.threshold
                operator = getattr(config, "operator", "above")

                triggered = False
                if (operator == "above" and current_value > threshold) or (operator == "below" and current_value < threshold) or (operator == "equals" and current_value == threshold):
                    triggered = True

                if triggered and not self._ucf_fired.get(spiral.id):
                    # Fire once per threshold crossing
                    self._ucf_fired[spiral.id] = True
                    logger.info(
                        "UCF threshold triggered for %s: %s=%s %s %s",
                        spiral.name,
                        metric_name,
                        current_value,
                        operator,
                        threshold,
                    )
                    await self.engine.execute(
                        spiral_id=spiral.id,
                        trigger_type="ucf_threshold",
                        trigger_data={
                            "metric": metric_name,
                            "value": current_value,
                            "threshold": threshold,
                            "operator": operator,
                        },
                        metadata={"source": "ucf_monitor"},
                    )
                elif not triggered:
                    # Reset flag so it can fire again next crossing
                    self._ucf_fired[spiral.id] = False

        except Exception as e:
            logger.error("Error checking UCF thresholds: %s", e)

    @staticmethod
    def _read_ucf_metrics() -> dict[str, Any] | None:
        """Read current UCF metrics from the live state module.

        Priority order:
        1. apps.backend.state.get_live_state()  (in-memory, authoritative)
        2. Fall back to Helix/state/ucf_state.json  (disk cache)
        """
        # Try the in-memory live state first
        try:
            from apps.backend.state import get_live_state

            state = get_live_state()
            if state and isinstance(state, dict):
                ucf = state.get("ucf_metrics") or state.get("ucf") or {}
                if ucf:
                    return ucf
        except Exception as e:
            logger.debug("UCF metric lookup from Redis failed: %s", e)

        # Fall back to disk cache
        try:
            from pathlib import Path

            ucf_path = Path("Helix/state/ucf_state.json")
            if ucf_path.exists():
                with open(ucf_path, encoding="utf-8") as f:
                    return json.load(f)
        except (ValueError, TypeError, OSError) as exc:
            logger.debug("Failed to load UCF state from disk cache: %s", exc)

        return None

    # --------------------------------------------------- persistence helpers
    async def _load_fire_times(self):
        """Load persisted last-fire timestamps from storage (Redis or DB)."""
        try:
            if hasattr(self.storage, "redis") and self.storage.redis:
                raw = await self.storage.redis.get("scheduler:fire_times")
                if raw:
                    data = json.loads(raw)
                    for sid, ts in data.items():
                        try:
                            self._last_fired[sid] = datetime.fromisoformat(ts)
                        except (ValueError, TypeError) as exc:
                            logger.debug("Skipping invalid fire time for %s: %s", sid, exc)
                    logger.info(
                        "Loaded %s persisted fire times from Redis",
                        len(self._last_fired),
                    )
        except Exception as e:
            logger.debug("Could not load fire times: %s", e)

    async def _save_fire_times(self):
        """Persist last-fire timestamps to Redis for restart resilience."""
        try:
            if hasattr(self.storage, "redis") and self.storage.redis:
                data = {sid: ts.isoformat() for sid, ts in self._last_fired.items()}
                await self.storage.redis.set(
                    "scheduler:fire_times",
                    json.dumps(data),
                    ex=86400 * 7,  # TTL 7 days
                )
        except Exception as e:
            logger.debug("Could not save fire times: %s", e)

    # --------------------------------------------------- health / status
    def get_task_count(self) -> int:
        """Get count of scheduled tasks."""
        return len(self.scheduled_tasks)

    def get_scheduled_spirals(self) -> list[str]:
        """Get list of scheduled spiral IDs."""
        return list(self.scheduled_tasks.keys())

    def get_status(self) -> dict[str, Any]:
        """Return scheduler health status for /api/health."""
        return {
            "running": self.is_running,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "scheduled_count": len(self.scheduled_tasks),
            "scheduled_spirals": list(self.scheduled_tasks.keys()),
            "last_fired": {sid: ts.isoformat() for sid, ts in self._last_fired.items()},
            "ucf_threshold_fired": {sid: v for sid, v in self._ucf_fired.items() if v},
        }
