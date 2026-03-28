"""
Copyright (c) 2025-2026 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

🌀 Helix Spirals Event Bus
Native replacement for MasterZapierClient — routes all events through the
Helix Spirals engine instead of external Zapier webhooks.

Drop-in compatible: every public method from MasterZapierClient is preserved
with the same signature so callers (Discord bot, routes, etc.) can swap
``MasterZapierClient`` for ``SpiralEventBus`` with zero code changes.

Events are:
1. Persisted to a local JSONL log (``logs/spiral_events.jsonl``)
2. Dispatched to the SpiralEngine for any matching system_event spirals
3. Optionally forwarded to an external webhook if HELIX_EVENT_WEBHOOK_URL is set
"""

import asyncio
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Optional external webhook for forwarding (e.g. Discord webhook, Slack, etc.)
_EXTERNAL_WEBHOOK_URL = os.environ.get("HELIX_EVENT_WEBHOOK_URL", "")


class SpiralEventBus:
    """
    Native event bus replacing MasterZapierClient.

    Provides the same public API so existing callers (Discord bot commands,
    monitoring, health checks) continue to work without modification.

    Events flow:
        caller  ──▶  SpiralEventBus  ──▶  local log
                                     ──▶  SpiralEngine (system_event triggers)
                                     ──▶  external webhook (optional)
    """

    def __init__(
        self,
        engine=None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._engine = engine  # SpiralEngine instance (lazy-loaded if None)
        self._http: httpx.AsyncClient | None = http_client
        self._owns_http = http_client is None
        self._log_path = Path("logs") / "spiral_events.jsonl"
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._subscribers: dict[str, list] = {}  # event_type -> [callbacks]

    # ------------------------------------------------------------------
    # Subscription API (new — not in MasterZapierClient)
    # ------------------------------------------------------------------

    def subscribe(self, event_type: str, callback) -> None:
        """Register an async callback for a specific event type."""
        self._subscribers.setdefault(event_type, []).append(callback)

    # ------------------------------------------------------------------
    # MasterZapierClient-compatible public API
    # ------------------------------------------------------------------

    async def log_event(
        self,
        event_title: str,
        event_type: str,
        agent_name: str,
        description: str,
        ucf_snapshot: dict[str, Any],
    ) -> bool:
        """Log an event (replaces Notion Event Log via Zapier)."""
        return await self._emit(
            "event_log",
            {
                "event_title": event_title,
                "event_type": event_type,
                "agent_name": agent_name,
                "description": description,
                "ucf_snapshot": ucf_snapshot,
                "helix_phase": os.environ.get("HELIX_PHASE", "production"),
            },
        )

    async def update_agent(self, agent_name: str, status: str, last_action: str, health_score: int) -> bool:
        """Update agent status (replaces Notion Agent Registry via Zapier)."""
        return await self._emit(
            "agent_registry",
            {
                "agent_name": agent_name,
                "status": status,
                "last_action": last_action,
                "health_score": health_score,
            },
        )

    async def update_system_state(
        self,
        component: str,
        status: str,
        harmony: float,
        error_log: str = "",
        verified: bool = False,
    ) -> bool:
        """Update system component state (replaces Notion System State via Zapier)."""
        return await self._emit(
            "system_state",
            {
                "component": component,
                "status": status,
                "harmony": harmony,
                "error_log": error_log,
                "verified": verified,
            },
        )

    async def send_discord_notification(self, channel_name: str, message: str, priority: str = "normal") -> bool:
        """Send Discord notification (native — no Zapier relay)."""
        return await self._emit(
            "discord_notification",
            {
                "channel_name": channel_name,
                "message": message,
                "priority": priority,
                "guild_id": os.environ.get("DISCORD_GUILD_ID", ""),
            },
        )

    async def send_railway_discord_event(
        self,
        discord_channel: str,
        event_type: str,
        title: str,
        description: str,
        metadata: dict[str, Any] | None = None,
        priority: str = "normal",
    ) -> bool:
        """Send Railway→Discord event (native — no Zapier relay)."""
        return await self._emit(
            "railway_discord_event",
            {
                "discord_channel": discord_channel.upper(),
                "event_type": event_type,
                "title": title,
                "description": description,
                "metadata": metadata or {},
                "priority": priority,
                "helix_version": os.environ.get("HELIX_VERSION", "17.2"),
                "railway_environment": os.environ.get("RAILWAY_ENVIRONMENT", "production"),
                "mention_subscribers": True,
            },
        )

    async def log_telemetry(self, metric_name: str, value: float, component: str = "system", unit: str = "") -> bool:
        """Log telemetry data (replaces Google Sheets via Zapier)."""
        return await self._emit(
            "telemetry",
            {
                "metric_name": metric_name,
                "value": value,
                "component": component,
                "unit": unit,
            },
        )

    async def send_error_alert(
        self,
        error_message: str,
        component: str,
        severity: str = "high",
        stack_trace: str = "",
    ) -> bool:
        """Send error alert (replaces Email/PagerDuty via Zapier)."""
        return await self._emit(
            "error_alert",
            {
                "error_message": error_message,
                "component": component,
                "severity": severity,
                "stack_trace": stack_trace[:1000] if stack_trace else "",
                "environment": os.environ.get("RAILWAY_ENVIRONMENT", "production"),
            },
        )

    async def log_repository_action(self, repo_name: str, action: str, details: str, commit_sha: str = "") -> bool:
        """Log repository action (replaces GitHub Actions trigger via Zapier)."""
        return await self._emit(
            "repository",
            {
                "repo_name": repo_name,
                "action": action,
                "details": details,
                "commit_sha": commit_sha,
            },
        )

    # ------------------------------------------------------------------
    # Internal plumbing
    # ------------------------------------------------------------------

    async def _emit(self, event_type: str, payload: dict[str, Any]) -> bool:
        """
        Core dispatch — log locally, notify engine, optionally forward externally.
        """
        payload["type"] = event_type
        payload["timestamp"] = datetime.now(UTC).isoformat()

        success = True

        # 1. Persist to local event log
        try:
            self._write_log(payload)
        except OSError as e:
            logger.warning("Failed to write event log for %s: %s", event_type, e)
            success = False
        except Exception:
            logger.exception("Failed to write event log for %s", event_type)
            success = False

        # 2. Trigger matching spirals via the engine (fire-and-forget)
        try:
            await self._dispatch_to_engine(event_type, payload)
        except (ConnectionError, TimeoutError) as e:
            logger.debug("Connection error dispatching %s to SpiralEngine: %s", event_type, e)
            # Not fatal — event is already persisted
        except Exception:
            logger.exception("Failed to dispatch %s to SpiralEngine", event_type)
            # Not fatal — event is already persisted

        # 3. Notify in-process subscribers
        for callback in self._subscribers.get(event_type, []):
            try:
                result = callback(payload)
                if asyncio.iscoroutine(result):
                    await result
            except (ValueError, TypeError, KeyError, AttributeError) as e:
                logger.warning("Subscriber callback error for %s: %s", event_type, e)
            except Exception:
                logger.exception("Subscriber error for %s", event_type)

        # 4. Optionally forward to external webhook
        if _EXTERNAL_WEBHOOK_URL:
            try:
                await self._forward_external(payload)
            except (ConnectionError, TimeoutError) as e:
                logger.debug("Connection error forwarding %s to external webhook: %s", event_type, e)
            except Exception:
                logger.exception("Failed to forward %s to external webhook", event_type)

        return success

    def _write_log(self, payload: dict[str, Any]) -> None:
        """Append event to JSONL file."""
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")

    async def _dispatch_to_engine(self, event_type: str, payload: dict[str, Any]) -> None:
        """Fire matching system_event spirals in the engine."""
        if self._engine is None:
            return

        try:
            # Find spirals triggered by this system event
            spirals = await self._engine.storage.list_spirals(
                trigger_type="system_event",
                enabled_only=True,
            )
        except Exception as e:
            logger.warning("Storage query failed for event %s: %s", event_type, e)
            return

        for spiral in spirals:
            # Check if the spiral's trigger config matches this event type
            trigger_config = {}
            if hasattr(spiral, "trigger") and hasattr(spiral.trigger, "config"):
                trigger_config = (
                    spiral.trigger.config.dict()
                    if hasattr(spiral.trigger.config, "dict")
                    else dict(spiral.trigger.config)
                )

            if trigger_config.get("event_type") == event_type or trigger_config.get("event_type") == "*":
                try:
                    await self._engine.execute(
                        spiral_id=spiral.id,
                        trigger_type="system_event",
                        trigger_data=payload,
                    )
                except Exception:
                    logger.exception(
                        "Failed to execute spiral %s for event %s",
                        spiral.id,
                        event_type,
                    )

    async def _forward_external(self, payload: dict[str, Any]) -> None:
        """Forward event to an external webhook (e.g. Discord webhook)."""
        client = self._http
        if client is None:
            client = httpx.AsyncClient(timeout=10.0)

        try:
            resp = await client.post(_EXTERNAL_WEBHOOK_URL, json=payload)
            if resp.status_code >= 400:
                logger.warning(
                    "External webhook returned %d for %s",
                    resp.status_code,
                    payload.get("type"),
                )
        finally:
            if self._owns_http and client is not self._http:
                await client.aclose()

    async def close(self) -> None:
        """Clean up HTTP resources."""
        if self._owns_http and self._http is not None:
            await self._http.aclose()


# ---------------------------------------------------------------------------
# Module-level singleton (lazy)
# ---------------------------------------------------------------------------

_event_bus: SpiralEventBus | None = None


def get_event_bus(engine=None) -> SpiralEventBus:
    """
    Return the module-level SpiralEventBus singleton.

    If *engine* is provided on the first call it is wired in; subsequent
    calls return the cached instance.
    """
    global _event_bus
    if _event_bus is None:
        _event_bus = SpiralEventBus(engine=engine)
    elif engine is not None and _event_bus._engine is None:
        _event_bus._engine = engine
    return _event_bus


def validate_config() -> dict[str, Any]:
    """
    Validate event bus configuration.

    Drop-in replacement for zapier_client_master.validate_config().
    """
    external_configured = bool(_EXTERNAL_WEBHOOK_URL)
    return {
        "master_webhook": False,  # No longer using Zapier
        "individual_webhooks": False,
        "mode": "spiral_event_bus",
        "native_engine": True,
        "external_webhook": external_configured,
        "external_url_configured": external_configured,
        "log_path": str(Path("logs") / "spiral_events.jsonl"),
    }
