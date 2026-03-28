"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

🌀 Helix Spirals Action Executors
Implementation of all action types for the Zapier alternative
"""

import asyncio
import json
import logging
import os
import smtplib
from datetime import UTC, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import httpx

from apps.backend.core.exceptions import IntegrationError, WebhookError, WorkflowError

from .models import Action, ActionType, ExecutionContext, ExecutionError, ExecutionStatus

logger = logging.getLogger(__name__)


class ActionExecutor:
    """Execute spiral actions with full Zapier compatibility"""

    def __init__(self, engine):
        self.engine = engine
        self.http_client = httpx.AsyncClient(timeout=30.0)

    async def execute(self, action: Action, context: ExecutionContext) -> Any:
        """Execute an action based on its type"""
        config = action.config

        # Resolve variables in config
        resolved_config = self._resolve_config_variables(config.dict(), context)

        action_map = {
            ActionType.SEND_WEBHOOK: self._execute_send_webhook,
            ActionType.STORE_DATA: self._execute_store_data,
            ActionType.SEND_DISCORD: self._execute_send_discord,
            ActionType.TRIGGER_CYCLE: self._execute_trigger_cycle,
            ActionType.ALERT_AGENT: self._execute_alert_agent,
            ActionType.UPDATE_UCF: self._execute_update_ucf,
            ActionType.LOG_EVENT: self._execute_log_event,
            ActionType.TRANSFORM_DATA: self._execute_transform_data,
            ActionType.CONDITIONAL_BRANCH: self._execute_conditional_branch,
            ActionType.DELAY: self._execute_delay,
            ActionType.PARALLEL_EXECUTE: self._execute_parallel_execute,
            ActionType.FOREACH: self._execute_foreach,
            ActionType.SEND_EMAIL: self._execute_send_email,
            ActionType.ROUTER: self._execute_router,
            ActionType.STOP_AND_ERROR: self._execute_stop_and_error,
            ActionType.HUMAN_INPUT: self._execute_human_input,
            ActionType.LLM_ROUTER: self._execute_llm_router,
            ActionType.EXECUTE_SPIRAL: self._execute_sub_spiral,
            ActionType.NODE_EXECUTION: self._execute_node_execution,
            ActionType.DIGEST: self._execute_digest,
            ActionType.KV_STORAGE: self._execute_kv_storage,
        }

        executor = action_map.get(action.type)
        if not executor:
            raise ValueError(f"Unknown action type: {action.type}")

        result = await executor(resolved_config, context)

        # Store result in context variables
        context.variables[f"action_{action.id}_result"] = result
        context.variables["_last_action_output"] = result

        return result

    async def _execute_send_webhook(self, config: dict, context: ExecutionContext) -> dict:
        """Send webhook action - core Zapier replacement functionality"""
        url = config["url"]
        method = config.get("method", "POST")
        headers = config.get("headers", {})
        body = config.get("body")

        # Add coordination level to headers
        headers["X-Helix-Coordination-Level"] = str(context.variables.get("performance_score", 5))
        headers["X-Helix-Execution-ID"] = context.execution_id
        headers["X-Helix-Spiral-ID"] = context.spiral_id

        # Add authentication
        auth = config.get("authentication")
        if auth:
            auth_type = auth.get("type")
            if auth_type == "bearer":
                headers["Authorization"] = f"Bearer {auth['credentials']}"
            elif auth_type == "basic":
                headers["Authorization"] = f"Basic {auth['credentials']}"
            elif auth_type == "api_key":
                headers["X-API-Key"] = auth["credentials"]

        # Make request
        response = await self.http_client.request(
            method=method,
            url=url,
            headers=headers,
            json=body if method != "GET" else None,
            params=body if method == "GET" else None,
        )

        if not response.is_success:
            raise WebhookError(f"Webhook failed: {response.status_code} {response.text}")

        try:
            return response.json()
        except (ValueError, TypeError, KeyError, IndexError):
            return {"status": response.status_code, "text": response.text}

    async def _execute_store_data(self, config: dict, context: ExecutionContext) -> None:
        """Store data action - Context Vault integration"""
        storage_type = config["storage_type"]
        key = config["key"]
        value = config.get("value", context.variables)
        ttl = config.get("ttl")
        encrypt = config.get("encrypt", False)

        # Add UCF metadata
        ucf_metadata = {
            "performance_score": context.variables.get("performance_score", 5),
            "spiral_id": context.spiral_id,
            "execution_id": context.execution_id,
            "timestamp": datetime.now(UTC).isoformat(),
            "ucf_impact": context.ucf_impact if hasattr(context, "ucf_impact") else {},
        }

        if storage_type == "database":
            # Store in PostgreSQL with UCF metadata
            if self.engine.storage.pg_pool:
                async with self.engine.storage.pg_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO spiral_data (key, value, ttl, encrypted, created_at, ucf_metadata)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (key) DO UPDATE SET
                            value = $2, ttl = $3, updated_at = $5, ucf_metadata = $6
                    """,
                        key,
                        json.dumps(value),
                        ttl,
                        encrypt,
                        datetime.now(UTC),
                        json.dumps(ucf_metadata),
                    )

        elif storage_type == "cache":
            # Store in Redis with UCF metadata
            if self.engine.storage.redis_client:
                cache_data = {"value": value, "ucf_metadata": ucf_metadata}
                await self.engine.storage.redis_client.setex(f"spiral:data:{key}", ttl or 3600, json.dumps(cache_data))

        elif storage_type == "file":
            # Store as file with UCF metadata
            import re
            import tempfile

            # Sanitize key to prevent path traversal
            safe_key = re.sub(r"[^a-zA-Z0-9_\-]", "_", key)
            base_dir = os.path.join(tempfile.gettempdir(), "spiral_data")
            file_path = os.path.join(base_dir, f"{safe_key}.json")
            # Verify resolved path stays within base_dir
            if not os.path.realpath(file_path).startswith(os.path.realpath(base_dir)):
                raise ValueError("Invalid storage key")
            os.makedirs(base_dir, exist_ok=True)
            file_data = {"value": value, "ucf_metadata": ucf_metadata}
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(file_data, f, indent=2)

        logger.info("Data stored in Context Vault: %s (%s)", key, storage_type)

    async def _execute_send_discord(self, config: dict, context: ExecutionContext) -> None:
        """Send Discord message action - 14-agent system integration"""
        webhook_url = config.get("webhook_url") or os.getenv("DISCORD_WEBHOOK_URL")

        if not webhook_url:
            raise ValueError("Discord webhook URL not configured")

        payload = {}

        if config["message_type"] == "text":
            content = config.get("content", "")
            # Add coordination level indicator
            performance_score = context.variables.get("performance_score", 5)
            coordination_emoji = self._get_coordination_emoji(performance_score)
            payload["content"] = f"{coordination_emoji} {content}"

        elif config["message_type"] == "embed":
            embed_data = config.get("embed", {})
            performance_score = context.variables.get("performance_score", 5)

            # Create rich embed with UCF metrics
            embed = {
                "title": embed_data.get("title", "Helix Spiral Execution"),
                "description": embed_data.get("description", ""),
                "color": self._get_coordination_color(performance_score),
                "fields": [
                    {
                        "name": "Coordination Level",
                        "value": f"{performance_score}/10",
                        "inline": True,
                    },
                    {
                        "name": "Spiral ID",
                        "value": context.spiral_id[:8],
                        "inline": True,
                    },
                    {
                        "name": "Execution ID",
                        "value": context.execution_id[:8],
                        "inline": True,
                    },
                ],
                "footer": {
                    "text": "Helix Collective - Coordination Automation",
                    "icon_url": "https://helixspirals.replit.app/favicon.ico",
                },
                "timestamp": datetime.now(UTC).isoformat(),
            }

            # Add UCF metrics if available
            if hasattr(context, "ucf_impact") and context.ucf_impact:
                ucf_fields = []
                for metric, value in context.ucf_impact.items():
                    ucf_fields.append(
                        {
                            "name": metric.title(),
                            "value": f"{value:.2f}",
                            "inline": True,
                        }
                    )
                embed["fields"].extend(ucf_fields)

            payload["embeds"] = [embed]

        # Set username based on assigned agents
        assigned_agents = context.variables.get("assigned_agents", [])
        if assigned_agents:
            agent_name = assigned_agents[0]  # Use first assigned agent
            payload["username"] = f"Helix {agent_name}"
        else:
            payload["username"] = f"Helix (Level {context.variables.get('performance_score', 5)})"

        response = await self.http_client.post(webhook_url, json=payload)

        if not response.is_success:
            raise IntegrationError(f"Discord webhook failed: {response.status_code}")

        logger.info("Discord message sent: %s", config.get("message_type"))

    async def _execute_trigger_cycle(self, config: dict, context: ExecutionContext) -> dict:
        """Trigger cycle action - Routine Engine optimization integration"""
        cycle_name = config["cycle_name"]
        parameters = config.get("parameters", {})
        wait_for_completion = config.get("wait_for_completion", False)

        # Add coordination context to cycle parameters
        cycle_params = {
            **parameters,
            "performance_score": context.variables.get("performance_score", 5),
            "spiral_context": {
                "spiral_id": context.spiral_id,
                "execution_id": context.execution_id,
                "ucf_impact": (context.ucf_impact if hasattr(context, "ucf_impact") else {}),
            },
        }

        # Call the optimization engine (Railway backend)
        cycle_url = os.getenv("ROUTINE_ENGINE_URL", "https://helix-unified-production.up.railway.app")
        response = await self.http_client.post(
            f"{cycle_url}/api/cycles/trigger",
            json={
                "cycle": cycle_name,
                "parameters": cycle_params,
                "performance_score": context.variables.get("performance_score", 5),
            },
        )

        if not response.is_success:
            raise IntegrationError(f"Cycle trigger failed: {response.status_code}")

        result = response.json()

        if wait_for_completion:
            # Poll for completion with a 60-second timeout
            cycle_id = result.get("cycle_id")
            deadline = datetime.now(UTC).timestamp() + 60
            while True:
                await asyncio.sleep(2)
                if datetime.now(UTC).timestamp() > deadline:
                    logger.warning("Cycle %s timed out after 60s waiting for completion", cycle_id)
                    break
                try:
                    status_response = await self.http_client.get(f"{cycle_url}/api/cycles/status/{cycle_id}")
                    if status_response.is_success:
                        status = status_response.json()
                        if status.get("completed"):
                            result = status
                            break
                except Exception as poll_err:
                    logger.warning("Cycle status poll error for %s: %s", cycle_id, poll_err)

        return result

    async def _execute_alert_agent(self, config: dict, context: ExecutionContext) -> None:
        """Alert agent action - 14-agent system notification"""
        agent_name = config["agent_name"]
        alert_level = config["alert_level"]
        message = config["message"]
        metadata = config.get("metadata", {})

        # Add coordination and UCF context
        alert_metadata = {
            **metadata,
            "performance_score": context.variables.get("performance_score", 5),
            "spiral_id": context.spiral_id,
            "execution_id": context.execution_id,
            "ucf_impact": context.ucf_impact if hasattr(context, "ucf_impact") else {},
            "timestamp": datetime.now(UTC).isoformat(),
        }

        # Send alert to agent system (Railway backend) - fire-and-forget with logging
        agent_url = os.getenv("AGENT_SYSTEM_URL", "https://helix-unified-production.up.railway.app")
        try:
            response = await self.http_client.post(
                f"{agent_url}/api/agents/alert",
                json={
                    "agent": agent_name,
                    "level": alert_level,
                    "message": message,
                    "metadata": alert_metadata,
                    "source": f"spiral:{context.spiral_id}",
                    "performance_score": context.variables.get("performance_score", 5),
                },
            )
            if response.is_success:
                logger.info("Alert sent to %s: %s - %s", agent_name, alert_level, message)
            else:
                logger.warning("Alert to %s returned %s: %s", agent_name, response.status_code, response.text)
        except Exception as exc:
            logger.warning("Failed to send alert to agent %s: %s", agent_name, exc)

    async def _execute_update_ucf(self, config: dict, context: ExecutionContext) -> dict:
        """Update UCF metrics action - coordination tracking"""
        metric = config["metric"]
        operation = config["operation"]
        value = float(config["value"])

        # Track UCF impact
        if not hasattr(context, "ucf_impact"):
            context.ucf_impact = {}

        current_value = context.ucf_impact.get(metric, 0)

        if operation == "set":
            new_value = value
        elif operation == "increment":
            new_value = current_value + value
        elif operation == "decrement":
            new_value = current_value - value
        elif operation == "multiply":
            new_value = current_value * value
        else:
            new_value = current_value

        # Clamp values to valid UCF range (0-100)
        new_value = max(0, min(100, new_value))
        context.ucf_impact[metric] = new_value

        # Send to UCF tracker (Railway backend)
        ucf_url = os.getenv("UCF_TRACKER_URL", "https://helix-unified-production.up.railway.app")
        await self.http_client.post(
            f"{ucf_url}/api/ucf/update",
            json={
                "metric": metric,
                "value": new_value,
                "operation": operation,
                "source": f"spiral:{context.spiral_id}",
                "performance_score": context.variables.get("performance_score", 5),
            },
        )

        logger.info("UCF updated: %s %s %s = %s", metric, operation, value, new_value)

        return {"metric": metric, "value": new_value}

    async def _execute_log_event(self, config: dict, context: ExecutionContext) -> None:
        """Log event action - structured logging with coordination context"""
        level = config["level"]
        message = config["message"]
        category = config.get("category", "spiral")
        metadata = config.get("metadata", {})

        # Add coordination context to logs
        log_metadata = {
            **metadata,
            "performance_score": context.variables.get("performance_score", 5),
            "spiral_id": context.spiral_id,
            "execution_id": context.execution_id,
            "ucf_impact": context.ucf_impact if hasattr(context, "ucf_impact") else {},
            "timestamp": datetime.now(UTC).isoformat(),
        }

        # Add to execution logs
        await self.engine._log(context, level, message)

        # Also log to system with structured data
        logger.log(
            (
                logging.DEBUG
                if level == "debug"
                else (logging.INFO if level == "info" else logging.WARNING if level == "warning" else logging.ERROR)
            ),
            f"[{category}] {message}",
            extra={"metadata": log_metadata},
        )

    async def _execute_transform_data(self, config: dict, context: ExecutionContext) -> Any:
        """Transform data action - data processing with coordination awareness"""
        data = context.variables.copy()

        for transformation in config.get("transformations", []):
            transform_type = transformation["type"]
            transform_config = transformation["config"]

            if transform_type == "map":
                # Apply mapping with coordination context
                field = transform_config["field"]
                expression = transform_config["expression"]
                if field in data:
                    # Add coordination level to evaluation context
                    eval_context = {
                        "value": data[field],
                        "data": data,
                        "performance_score": context.variables.get("performance_score", 5),
                        "ucf_impact": (context.ucf_impact if hasattr(context, "ucf_impact") else {}),
                    }
                    # Use safe expression evaluation instead of eval
                    data[field] = self._safe_eval_expression(expression, eval_context)

            elif transform_type == "filter":
                # Filter data with coordination awareness
                condition = transform_config["condition"]
                if isinstance(data, list):
                    data = [
                        item
                        for item in data
                        if self._safe_eval_condition(
                            condition,
                            {
                                "item": item,
                                "performance_score": context.variables.get("performance_score", 5),
                            },
                        )
                    ]

            elif transform_type == "template":
                # Apply template with full context
                template = transform_config["template"]
                template_vars = {
                    **data,
                    "performance_score": context.variables.get("performance_score", 5),
                    "spiral_id": context.spiral_id,
                    "execution_id": context.execution_id,
                }
                for key, value in template_vars.items():
                    template = template.replace(f"{{{key}}}", str(value))
                data = template

        # Update context variables
        context.variables["transformed_data"] = data

        return data

    async def _execute_conditional_branch(self, config: dict, context: ExecutionContext) -> Any:
        """Conditional branch action - coordination-aware branching"""
        conditions = config["conditions"]
        true_branch = config.get("true_branch", [])
        false_branch = config.get("false_branch", [])

        # Evaluate conditions with coordination context
        conditions_met = await self.engine._evaluate_conditions(conditions, context)

        # Execute appropriate branch
        branch_to_execute = true_branch if conditions_met else false_branch

        results = []
        for action_data in branch_to_execute:
            action = Action(**action_data)
            result = await self.execute(action, context)
            results.append(result)

        return {"conditions_met": conditions_met, "results": results}

    async def _execute_router(self, config: dict, context: ExecutionContext) -> dict:
        """Multi-way router: examine previous step output and execute the matching route's actions.

        In the sequential Spiral engine, ``route_actions`` maps route keys to
        inline action lists (since we don't have a DAG with named connections).
        """
        field_name = config.get("field", "route")
        default_route = config.get("default_route", "default")
        expression = config.get("expression")
        routes = config.get("routes", {})
        route_actions = config.get("route_actions", {})

        # Get previous step output from context variables
        prev_output = context.variables.get("_last_action_output", {})

        route_key = default_route

        if expression:
            safe_builtins = {
                "len": len,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
                "type": type,
            }
            namespace = {"input": prev_output, "data": prev_output, **safe_builtins}
            try:
                from apps.backend.utils.safe_eval import safe_eval

                route_key = str(safe_eval(expression, allowed_names=namespace))
            except Exception as e:
                logger.warning("Router expression failed: %s, using default", e)
                route_key = default_route
        elif isinstance(prev_output, dict):
            route_key = str(prev_output.get(field_name, default_route))
        elif isinstance(prev_output, str):
            route_key = prev_output

        # Validate
        if routes and route_key not in routes:
            logger.info("Router: '%s' not in routes %s, using default", route_key, list(routes.keys()))
            route_key = default_route

        # Execute inline route actions if provided
        results = []
        if route_actions and route_key in route_actions:
            for action_data in route_actions[route_key]:
                action = Action(**action_data)
                result = await self.execute(action, context)
                results.append(result)

        return {
            "route": route_key,
            "available_routes": list(routes.keys()),
            "results": results,
        }

    async def _execute_delay(self, config: dict, context: ExecutionContext) -> None:
        """Delay action - coordination-aware timing"""
        duration_ms = config["duration"]

        # Adjust delay based on coordination level (higher coordination = faster processing)
        performance_score = context.variables.get("performance_score", 5)
        coordination_multiplier = max(0.1, (11 - performance_score) / 10)  # Level 10 = 0.1x delay, Level 1 = 1x delay
        adjusted_duration = duration_ms * coordination_multiplier

        await asyncio.sleep(adjusted_duration / 1000.0)
        logger.info("Delayed for %sms (coordination-adjusted from %sms)", adjusted_duration, duration_ms)

    async def _execute_parallel_execute(self, config: dict, context: ExecutionContext) -> list:
        """Parallel execute action - coordination-aware concurrency"""
        actions = config["actions"]
        wait_for_all = config.get("wait_for_all", True)

        # Limit concurrency based on coordination level
        performance_score = context.variables.get("performance_score", 5)
        max_concurrent = min(len(actions), performance_score * 2)  # Level 10 = 20 concurrent, Level 1 = 2 concurrent

        semaphore = asyncio.Semaphore(max_concurrent)

        async def execute_with_semaphore(action_data):
            async with semaphore:
                action = Action(**action_data)
                return await self.execute(action, context)

        tasks = [execute_with_semaphore(action_data) for action_data in actions]

        if wait_for_all:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        else:
            # Fire and forget with coordination-aware batching
            _fire_forget: list[asyncio.Task] = []
            for i in range(0, len(tasks), max_concurrent):
                batch = tasks[i : i + max_concurrent]
                for task in batch:
                    _fire_forget.append(asyncio.create_task(task))
            results = ["launched" for _ in tasks]

        return results

    async def _execute_foreach(self, config: dict, context: ExecutionContext) -> list:
        """Foreach action — iterate over a list and execute one action per item.

        Supports sequential execution (``max_concurrency=1``, the default) and
        bounded parallel fan-out (``max_concurrency>1``).  Each item is bound
        under ``item_var`` in a shallow copy of the context variables so the
        sub-action can reference it via ``$<item_var>``.

        Results (or error strings when ``continue_on_item_error=True``) are
        collected and stored in ``context.variables[output_var]``.
        """
        from .models import Action as _Action

        # --- Resolve the items list ---
        raw_items = config.get("items", [])
        if isinstance(raw_items, str) and raw_items.startswith("$"):
            # Variable reference — look up in context
            var_name = raw_items[1:]
            raw_items = context.variables.get(var_name, [])

        if not isinstance(raw_items, list):
            raise ValueError(f"foreach.items must resolve to a list; got {type(raw_items).__name__}")

        item_var: str = config.get("item_var", "item")
        action_dict: dict = config.get("action", {})
        max_concurrency: int = max(1, int(config.get("max_concurrency", 1)))
        output_var: str = config.get("output_var", "foreach_results")
        continue_on_error: bool = bool(config.get("continue_on_item_error", False))

        semaphore = asyncio.Semaphore(max_concurrency)

        async def run_for_item(item: Any) -> Any:
            async with semaphore:
                # Build a per-iteration copy of context variables with the item bound
                iter_vars = {**context.variables, item_var: item}
                iter_ctx = context.model_copy(deep=False)
                iter_ctx.variables = iter_vars

                # Resolve any $item_var references inside the action config
                resolved_action_dict = json.loads(json.dumps(action_dict).replace(f'"${item_var}"', json.dumps(item)))

                try:
                    action = _Action(**resolved_action_dict)
                    result = await self.execute(action, iter_ctx)
                    # Propagate any new variables written by the sub-action back
                    for k, _v in iter_ctx.variables.items():
                        if k not in context.variables or k == item_var:
                            pass  # keep per-item scope isolated
                    return result
                except Exception as exc:
                    if continue_on_error:
                        logger.warning("foreach item error (item=%r): %s", item, exc)
                        return {"_error": str(exc), "_item": item}
                    raise

        if max_concurrency == 1:
            # Sequential — preserves order, simpler stack traces
            results = []
            for item in raw_items:
                results.append(await run_for_item(item))
        else:
            # Parallel fan-out — bounded by semaphore
            results = list(
                await asyncio.gather(
                    *[run_for_item(item) for item in raw_items],
                    return_exceptions=continue_on_error,
                )
            )
            # Unwrap exceptions into error dicts when continue_on_error=True
            if continue_on_error:
                results = [
                    {"_error": str(r), "_item": raw_items[i]} if isinstance(r, Exception) else r
                    for i, r in enumerate(results)
                ]

        context.variables[output_var] = results
        logger.info(
            "foreach completed: %d items, concurrency=%d, output_var=%s",
            len(raw_items),
            max_concurrency,
            output_var,
        )
        return results

    async def _execute_send_email(self, config: dict, context: ExecutionContext) -> None:
        """Send email action - coordination-aware email delivery"""
        # Use environment SMTP configuration
        smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER")
        smtp_pass = os.getenv("SMTP_PASS")

        if not smtp_user or not smtp_pass:
            logger.warning("SMTP not configured, skipping email")
            return

        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = ", ".join(config["to"])
        msg["Subject"] = config["subject"]

        # Add coordination level to subject
        performance_score = context.variables.get("performance_score", 5)
        coordination_emoji = self._get_coordination_emoji(performance_score)
        msg["Subject"] = f"{coordination_emoji} {config['subject']}"

        if config.get("cc"):
            msg["Cc"] = ", ".join(config["cc"])

        # Add Helix headers
        msg["X-Helix-Coordination-Level"] = str(performance_score)
        msg["X-Helix-Spiral-ID"] = context.spiral_id
        msg["X-Helix-Execution-ID"] = context.execution_id

        body = config["body"]

        # Add coordination signature
        signature = f"\n\n---\nSent by Helix Collective\nCoordination Level: {performance_score}/10\nSpiral: {context.spiral_id[:8]}\nExecution: {context.execution_id[:8]}"
        body += signature

        msg.attach(MIMEText(body, "html" if config.get("is_html") else "plain"))

        # Send email
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)

            recipients = config["to"] + config.get("cc", []) + config.get("bcc", [])
            server.send_message(msg, to_addrs=recipients)

        logger.info("Email sent to %s recipients with coordination level %s", len(recipients), performance_score)

    def _resolve_config_variables(self, config: dict, context: ExecutionContext) -> dict:
        """Resolve variables in action configuration"""

        def resolve_value(value):
            if isinstance(value, str):
                # Handle template variables
                if "{{" in value and "}}" in value:
                    for var_name, var_value in context.variables.items():
                        value = value.replace(f"{{{{{var_name}}}}}", str(var_value))
                return value
            elif isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(v) for v in value]
            return value

        return resolve_value(config)

    def _get_coordination_emoji(self, level: int) -> str:
        """Get emoji based on coordination level"""
        emoji_map = {
            1: "😴",  # Dormant
            2: "😪",  # Stirring
            3: "😊",  # Awakening
            4: "🤔",  # Aware
            5: "😌",  # Conscious
            6: "🌱",  # Expanding
            7: "🌊",  # Flowing
            8: "🔮",  # Unified
            9: "✨",  # Transcendent
            10: "🌀",  # Omniscient
        }
        return emoji_map.get(level, "🤖")

    def _get_coordination_color(self, level: int) -> int:
        """Get Discord embed color based on coordination level"""
        # Color gradient from red (low) to purple (high)
        colors = {
            1: 0xFF0000,  # Red
            2: 0xFF4500,  # Orange Red
            3: 0xFF8C00,  # Dark Orange
            4: 0xFFD700,  # Gold
            5: 0x32CD32,  # Lime Green
            6: 0x00CED1,  # Dark Turquoise
            7: 0x4169E1,  # Royal Blue
            8: 0x8A2BE2,  # Blue Violet
            9: 0x9932CC,  # Dark Orchid
            10: 0x9B59B6,  # Helix Purple
        }
        return colors.get(level, 0x7289DA)  # Discord default blue

    # ------------------------------------------------------------------
    # Stop And Error — intentional failure for validation
    # ------------------------------------------------------------------

    async def _execute_stop_and_error(self, config: dict, context: ExecutionContext) -> dict:
        """Intentionally halt execution with an error.

        If ``condition`` is set, only stop when it evaluates truthy.
        This triggers the error workflow (if configured on the spiral).
        """
        condition = config.get("condition")
        if condition:
            eval_ctx = {**context.variables, "trigger": context.trigger}
            if not self._safe_eval_condition(condition, eval_ctx):
                return {"stopped": False, "reason": "Condition not met, continuing"}

        message = config.get("message", "Execution stopped")
        error_code = config.get("error_code")

        context.status = ExecutionStatus.FAILED
        context.error = ExecutionError(message=message, action_id=context.current_action)

        logger.info("STOP_AND_ERROR triggered: %s (code=%s)", message, error_code)
        raise WorkflowError(message)

    # ------------------------------------------------------------------
    # Human Input — pause and wait for approval
    # ------------------------------------------------------------------

    async def _execute_human_input(self, config: dict, context: ExecutionContext) -> dict:
        """Pause execution and wait for human approval/input.

        Sets the context status to WAITING_INPUT and stores the request
        metadata.  The engine's action loop should check for this status
        and pause execution.  Resumption happens via an external API call.
        """
        prompt = config.get("prompt", "Please review and approve")
        actions = config.get("actions", ["approve", "reject"])
        timeout_minutes = config.get("timeout_minutes", 1440)
        assignee = config.get("assignee")
        require_feedback = config.get("require_feedback", False)

        context.status = ExecutionStatus.WAITING_INPUT
        context.pending_human_input = {
            "action_id": context.current_action,
            "prompt": prompt,
            "actions": actions,
            "timeout_minutes": timeout_minutes,
            "assignee": assignee,
            "require_feedback": require_feedback,
            "requested_at": datetime.now(UTC).isoformat(),
        }

        logger.info(
            "HUMAN_INPUT: execution %s paused, awaiting input (assignee=%s)",
            context.execution_id,
            assignee,
        )

        return {
            "status": "waiting_input",
            "prompt": prompt,
            "actions": actions,
            "assignee": assignee,
        }

    # ------------------------------------------------------------------
    # LLM Router — AI-driven intent classification routing
    # ------------------------------------------------------------------

    async def _execute_llm_router(self, config: dict, context: ExecutionContext) -> dict:
        """Use an LLM to classify input into named scenarios for routing.

        Sends the previous step's output (or trigger data) to a cheap model
        with the scenario descriptions.  Returns the selected scenario as
        the route key.
        """
        scenarios = config.get("scenarios", {})
        instructions = config.get("instructions", "Classify the input into one of the defined scenarios.")
        model = config.get("model", "openai/gpt-4o-mini")
        default_scenario = config.get("default_scenario", "other")

        if not scenarios:
            return {"route": default_scenario, "reason": "No scenarios defined"}

        # Build the input from previous step output or trigger
        input_data = context.variables.get("_last_action_output", context.trigger)
        input_text = json.dumps(input_data) if isinstance(input_data, dict) else str(input_data)

        # Build classification prompt
        scenario_list = "\n".join(f"- {name}: {desc}" for name, desc in scenarios.items())
        prompt = (
            f"{instructions}\n\n"
            f"Available scenarios:\n{scenario_list}\n\n"
            f"Input to classify:\n{input_text}\n\n"
            f"Respond with ONLY the scenario name (one of: {', '.join(scenarios.keys())}). "
            f"If none match, respond with '{default_scenario}'."
        )

        try:
            from apps.backend.services.unified_llm import UnifiedLLMService

            llm = UnifiedLLMService()
            response = await llm.generate(
                prompt=prompt,
                model=model,
                max_tokens=50,
                temperature=0.1,
            )
            route_key = response.strip().lower().replace('"', "").replace("'", "")

            # Validate against defined scenarios
            if route_key not in scenarios:
                logger.info(
                    "LLM router returned '%s' not in scenarios %s, using default",
                    route_key,
                    list(scenarios.keys()),
                )
                route_key = default_scenario
        except Exception as e:
            logger.warning("LLM router classification failed: %s, using default", e)
            route_key = default_scenario

        logger.info("LLM_ROUTER selected scenario: %s", route_key)
        return {
            "route": route_key,
            "available_routes": list(scenarios.keys()),
            "input_preview": input_text[:200],
        }

    # ------------------------------------------------------------------
    # Execute Spiral — sub-workflow invocation
    # ------------------------------------------------------------------

    async def _execute_sub_spiral(self, config: dict, context: ExecutionContext) -> dict:
        """Execute another spiral as a sub-workflow.

        Prevents infinite recursion via a ``_nesting_depth`` counter.
        """
        spiral_id = config.get("spiral_id")
        input_mapping = config.get("input_mapping", {})
        max_depth = config.get("max_depth", 3)

        current_depth = context.variables.get("_nesting_depth", 0)
        if current_depth >= max_depth:
            raise ValueError(f"Sub-spiral nesting depth {current_depth} exceeds max {max_depth}")

        if not spiral_id:
            raise ValueError("execute_spiral requires a spiral_id")

        # Build trigger data from parent context
        trigger_data = {}
        for parent_var, child_field in input_mapping.items():
            trigger_data[child_field] = context.variables.get(parent_var)

        # Load and execute the sub-spiral
        child_spiral = await self.engine.storage.get_spiral(spiral_id)
        if not child_spiral:
            raise ValueError(f"Sub-spiral not found: {spiral_id}")

        child_context = ExecutionContext(
            spiral_id=spiral_id,
            trigger=trigger_data,
            variables={"_nesting_depth": current_depth + 1},
        )

        result_context = await self.engine._execute_spiral(child_spiral, child_context)

        return {
            "sub_spiral_id": spiral_id,
            "status": result_context.status.value,
            "output": result_context.variables.get("_last_action_output"),
            "logs_count": len(result_context.logs),
        }

    async def _execute_node_execution(self, config: dict, context: ExecutionContext) -> dict:
        node_type = config.get("node_type")
        if not node_type:
            raise ValueError("node_execution action requires 'node_type' in config")

        inputs = {k: v for k, v in config.items() if k != "node_type"}
        ctx_vars = dict(context.variables)

        from .advanced_nodes import get_node_registry

        registry = get_node_registry()
        node_class = registry.get(node_type)
        if node_class is not None:
            node = node_class()
            result = await node.execute(inputs, ctx_vars)
            return {"success": result.success, "data": result.data, "error": result.error}

        enterprise_nodes = {
            "postgresql": "PostgreSQLNode",
            "mongodb": "MongoDBNode",
            "redis_cache": "RedisNode",
            "s3_storage": "S3Node",
            "gcs": "GoogleCloudStorageNode",
            "openai_llm": "OpenAINode",
            "anthropic_llm": "AnthropicNode",
            "sentiment": "SentimentAnalysisNode",
            "twilio_sms": "TwilioNode",
            "sendgrid_email": "SendGridNode",
            "stripe_payment": "StripeNode",
            "shopify": "ShopifyNode",
            "hubspot": "HubSpotNode",
            "zendesk": "ZendeskNode",
            "split": "SplitNode",
            "merge": "MergeNode",
            "error_handler": "ErrorHandlerNode",
            "rate_limiter_node": "RateLimiterNode",
            "retry": "RetryNode",
            "webhook_signature": "WebhookSignatureNode",
            "cache": "CacheNode",
            "slack_node": "SlackNode",
        }

        class_name = enterprise_nodes.get(node_type)
        if class_name is not None:
            import importlib

            from .integration_nodes import NodeConfig as _NodeConfig

            mod = importlib.import_module("apps.backend.helix_spirals.integration_nodes")
            cls = getattr(mod, class_name)
            node_cfg = _NodeConfig(id=context.execution_id, type=node_type, name=node_type, config=inputs)
            node = cls(node_cfg)
            result = await node.execute(inputs, ctx_vars)
            return {"success": result.success, "data": result.data, "error": result.error}

        raise ValueError(f"Unknown node type: {node_type}. Check /api/spirals/nodes for available types.")

    async def _execute_digest(self, config: dict, context: ExecutionContext) -> dict | None:
        """Batch events into a digest bucket. Returns aggregated results when threshold is met."""
        redis = self.engine.storage.redis_client if self.engine.storage else None
        if not redis:
            raise ValueError("Digest requires Redis")

        # Tenant isolation: scope digest key to the current spiral
        digest_key = f"spiral:{context.spiral_id}:digest:{config['digest_key']}"
        release_mode = config.get("release_mode", "time")
        dedup_field = config.get("dedup_field")

        item = {
            "data": context.variables.get("_last_action_output", {}),
            "trigger": context.trigger,
            "timestamp": datetime.now(UTC).isoformat(),
        }

        if dedup_field:
            dedup_val = item["data"].get(dedup_field) if isinstance(item["data"], dict) else None
            if dedup_val:
                dedup_key = f"{digest_key}:dedup"
                if await redis.sismember(dedup_key, str(dedup_val)):
                    logger.info("Digest dedup: skipping duplicate %s=%s", dedup_field, dedup_val)
                    return {"status": "deduplicated", "digest_key": config["digest_key"]}
                await redis.sadd(dedup_key, str(dedup_val))
                await redis.expire(dedup_key, config.get("window_seconds", 300) * 2)

        await redis.rpush(digest_key, json.dumps(item))

        count = await redis.llen(digest_key)

        should_release = False
        if release_mode == "count" and count >= config.get("count_threshold", 10):
            should_release = True
        elif release_mode == "time":
            ttl = await redis.ttl(digest_key)
            window = config.get("window_seconds", 300)
            if ttl == -1:
                await redis.expire(digest_key, window)
            elif ttl == -2 or ttl <= 0:
                should_release = True

        if should_release:
            items_raw = []
            while True:
                item_raw = await redis.lpop(digest_key)
                if item_raw is None:
                    break
                items_raw.append(json.loads(item_raw))

            if dedup_field:
                await redis.delete(f"{digest_key}:dedup")

            context.variables["_digest_items"] = items_raw
            context.variables["_digest_count"] = len(items_raw)
            logger.info("Digest released: %s (%d items)", config["digest_key"], len(items_raw))
            return {"status": "released", "count": len(items_raw), "items": items_raw}
        else:
            logger.info("Digest buffered: %s (%d items)", config["digest_key"], count)
            return {"status": "buffered", "count": count, "digest_key": config["digest_key"]}

    def _safe_eval_expression(self, expression: str, context: dict[str, Any]) -> Any:
        """Safely evaluate an expression with restricted operations"""
        import ast
        import math
        import operator

        # Define safe operators and functions
        safe_operators = {
            ast.Eq: operator.eq,
            ast.NotEq: operator.ne,
            ast.Lt: operator.lt,
            ast.LtE: operator.le,
            ast.Gt: operator.gt,
            ast.GtE: operator.ge,
            ast.Is: operator.is_,
            ast.IsNot: operator.is_not,
            ast.In: operator.contains,
            ast.NotIn: lambda x, y: x not in y,
            ast.And: operator.and_,
            ast.Or: operator.or_,
            ast.Not: operator.not_,
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.FloorDiv: operator.floordiv,
            ast.Mod: operator.mod,
            ast.Pow: operator.pow,
            ast.USub: operator.neg,
        }

        safe_functions = {
            "abs": abs,
            "round": round,
            "min": min,
            "max": max,
            "len": len,
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "sum": sum,
            "math": math,
        }

        # Allowed attribute names — block dunder and private attrs to prevent RCE
        blocked_attr_prefixes = ("_", "__")
        blocked_attr_names = {"__class__", "__subclasses__", "__globals__", "__import__", "__builtins__"}

        # Allowed method names on context objects (safe data access only)
        allowed_methods = frozenset(
            {
                "get",
                "keys",
                "values",
                "items",
                "count",
                "index",
                "lower",
                "upper",
                "strip",
                "split",
                "join",
                "replace",
                "startswith",
                "endswith",
                "format",
                "find",
                "rfind",
                "append",
                "extend",
                "copy",
            }
        )

        def evaluate_node(node):
            """Recursively evaluate AST nodes safely"""
            if isinstance(node, ast.Constant):
                return node.value
            elif isinstance(node, ast.Name):
                if node.id in context:
                    return context[node.id]
                elif node.id in safe_functions:
                    return safe_functions[node.id]
                raise ValueError(f"Unknown variable or function: {node.id}")
            elif isinstance(node, ast.Attribute):
                # Allow access to attributes of context variables (no dunder access)
                if isinstance(node.value, ast.Name) and node.value.id in context:
                    if node.attr in blocked_attr_names or any(node.attr.startswith(p) for p in blocked_attr_prefixes):
                        raise ValueError(f"Blocked attribute access: {node.attr}")
                    obj = context[node.value.id]
                    return getattr(obj, node.attr)
                raise ValueError(f"Unsafe attribute access: {node.attr}")
            elif isinstance(node, ast.Compare):
                left = evaluate_node(node.left)
                for op, comparator in zip(node.ops, node.comparators, strict=False):
                    right = evaluate_node(comparator)
                    if type(op) in safe_operators:
                        result = safe_operators[type(op)](left, right)
                        if not result:
                            return False
                        left = right
                    else:
                        raise ValueError(f"Unsupported operator: {type(op)}")
                return True
            elif isinstance(node, ast.BoolOp):
                values = [evaluate_node(value) for value in node.values]
                if isinstance(node.op, ast.And):
                    return all(values)
                elif isinstance(node.op, ast.Or):
                    return any(values)
            elif isinstance(node, ast.UnaryOp):
                if isinstance(node.op, ast.Not):
                    return not evaluate_node(node.operand)
                elif isinstance(node.op, ast.USub):
                    return -evaluate_node(node.operand)
            elif isinstance(node, ast.BinOp):
                left = evaluate_node(node.left)
                right = evaluate_node(node.right)
                if type(node.op) in safe_operators:
                    return safe_operators[type(node.op)](left, right)
                else:
                    raise ValueError(f"Unsupported binary operator: {type(node.op)}")
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in safe_functions:
                    func = safe_functions[node.func.id]
                    args = [evaluate_node(arg) for arg in node.args]
                    kwargs = {kw.arg: evaluate_node(kw.value) for kw in node.keywords}
                    return func(*args, **kwargs)
                elif isinstance(node.func, ast.Attribute):
                    # Allow method calls on context objects — restricted to safe methods
                    if isinstance(node.func.value, ast.Name) and node.func.value.id in context:
                        if node.func.attr not in allowed_methods:
                            raise ValueError(f"Blocked method call: {node.func.attr}")
                        obj = context[node.func.value.id]
                        method = getattr(obj, node.func.attr)
                        args = [evaluate_node(arg) for arg in node.args]
                        kwargs = {kw.arg: evaluate_node(kw.value) for kw in node.keywords}
                        return method(*args, **kwargs)
                raise ValueError(f"Unsupported function call: {node.func}")
            else:
                raise ValueError(f"Unsupported expression type: {type(node)}")

        try:
            tree = ast.parse(expression, mode="eval")
            return evaluate_node(tree.body)
        except Exception as e:
            logger.warning("Safe eval failed for expression '%s': %s", expression, e)
            return None

    def _safe_eval_condition(self, condition: str, context: dict[str, Any]) -> bool:
        """Safely evaluate a condition expression"""
        try:
            result = self._safe_eval_expression(condition, context)
            return bool(result)
        except Exception as e:
            logger.warning("Safe condition eval failed for '%s': %s", condition, e)
            return False

    async def _execute_kv_storage(self, config: dict, context: ExecutionContext) -> dict:
        """Key-value storage operations: get, set, delete, exists, increment, list_keys."""
        operation = config["operation"]
        raw_key = config["key"]
        storage_type = config.get("storage_type", "database")
        output_var = config.get("output_var", "_kv_result")

        # Tenant isolation: prefix keys with spiral_id to prevent cross-spiral access
        key = f"{context.spiral_id}:{raw_key}"
        output_var = config.get("output_var", "_kv_result")

        if storage_type == "database":
            result = await self._kv_database_op(operation, key, config, context)
        elif storage_type == "cache":
            result = await self._kv_cache_op(operation, key, config, context)
        else:
            raise ValueError(f"Unknown storage_type: {storage_type}")

        context.variables[output_var] = result
        return result

    async def _kv_database_op(self, operation: str, key: str, config: dict, context: ExecutionContext) -> dict:
        """PostgreSQL-backed KV operations on spiral_data table."""
        pool = self.engine.storage.pg_pool if self.engine.storage else None
        if not pool:
            raise ValueError("KV database operations require PostgreSQL")

        async with pool.acquire() as conn:
            if operation == "get":
                row = await conn.fetchrow("SELECT value, encrypted FROM spiral_data WHERE key = $1", key)
                if row:
                    value = json.loads(row["value"]) if isinstance(row["value"], str) else row["value"]
                    return {"found": True, "key": key, "value": value}
                return {"found": False, "key": key, "value": None}

            elif operation == "set":
                value = config.get("value", context.variables.get("_last_action_output", {}))
                ttl = config.get("ttl")
                await conn.execute(
                    """INSERT INTO spiral_data (key, value, ttl, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $4)
                       ON CONFLICT (key) DO UPDATE SET value = $2, ttl = $3, updated_at = $4""",
                    key,
                    json.dumps(value),
                    ttl,
                    datetime.now(UTC),
                )
                return {"status": "stored", "key": key}

            elif operation == "delete":
                result = await conn.execute("DELETE FROM spiral_data WHERE key = $1", key)
                return {"status": "deleted" if result == "DELETE 1" else "not_found", "key": key}

            elif operation == "exists":
                row = await conn.fetchrow("SELECT 1 FROM spiral_data WHERE key = $1", key)
                return {"exists": row is not None, "key": key}

            elif operation == "increment":
                amount = config.get("value", 1)
                row = await conn.fetchrow("SELECT value FROM spiral_data WHERE key = $1", key)
                if row:
                    current = json.loads(row["value"]) if isinstance(row["value"], str) else row["value"]
                    if not isinstance(current, int | float):
                        raise ValueError(f"Cannot increment non-numeric value for key {key}")
                    new_val = current + amount
                else:
                    new_val = amount
                await conn.execute(
                    """INSERT INTO spiral_data (key, value, created_at, updated_at)
                       VALUES ($1, $2, $3, $3)
                       ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = $3""",
                    key,
                    json.dumps(new_val),
                    datetime.now(UTC),
                )
                return {"status": "incremented", "key": key, "value": new_val}

            elif operation == "list_keys":
                pattern = key
                rows = await conn.fetch(
                    "SELECT key FROM spiral_data WHERE key LIKE $1 ORDER BY key LIMIT 100",
                    pattern.replace("*", "%"),
                )
                return {"keys": [r["key"] for r in rows], "count": len(rows)}

            else:
                raise ValueError(f"Unknown KV operation: {operation}")

    async def _kv_cache_op(self, operation: str, key: str, config: dict, context: ExecutionContext) -> dict:
        """Redis-backed KV operations."""
        redis_client = self.engine.storage.redis_client if self.engine.storage else None
        if not redis_client:
            raise ValueError("KV cache operations require Redis")

        cache_key = f"spiral:data:{key}"

        if operation == "get":
            raw = await redis_client.get(cache_key)
            if raw:
                data = json.loads(raw)
                return {"found": True, "key": key, "value": data.get("value", data)}
            return {"found": False, "key": key, "value": None}

        elif operation == "set":
            value = config.get("value", context.variables.get("_last_action_output", {}))
            ttl = config.get("ttl", 3600)
            cache_data = {"value": value, "updated_at": datetime.now(UTC).isoformat()}
            await redis_client.setex(cache_key, ttl, json.dumps(cache_data))
            return {"status": "stored", "key": key}

        elif operation == "delete":
            deleted = await redis_client.delete(cache_key)
            return {"status": "deleted" if deleted else "not_found", "key": key}

        elif operation == "exists":
            exists = await redis_client.exists(cache_key)
            return {"exists": bool(exists), "key": key}

        elif operation == "increment":
            amount = config.get("value", 1)
            if isinstance(amount, int):
                new_val = await redis_client.incrby(cache_key, amount)
            else:
                new_val = await redis_client.incrbyfloat(cache_key, amount)
            return {"status": "incremented", "key": key, "value": new_val}

        elif operation == "list_keys":
            pattern = f"spiral:data:{key}".replace("*", "*")
            keys = []
            async for k in redis_client.scan_iter(match=pattern, count=100):
                keys.append(k.decode() if isinstance(k, bytes) else k)
                if len(keys) >= 100:
                    break
            prefix_len = len("spiral:data:")
            return {"keys": [k[prefix_len:] for k in keys], "count": len(keys)}

        else:
            raise ValueError(f"Unknown KV operation: {operation}")
