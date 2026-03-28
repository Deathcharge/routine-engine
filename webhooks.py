"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

Helix Spirals Webhook Receiver
Handles incoming webhooks and triggers spirals
"""

import hashlib
import hmac
import json
import logging
from datetime import UTC, datetime
from typing import Any

from .models import TriggerType, WebhookPayload

logger = logging.getLogger(__name__)


class WebhookReceiver:
    """Receives and processes incoming webhooks"""

    def __init__(self, engine, storage):
        self.engine = engine
        self.storage = storage
        self.webhook_cache: dict[str, datetime] = {}  # For deduplication

    async def process_webhook(self, webhook: WebhookPayload):
        """Process an incoming webhook"""
        try:
            spiral = await self.storage.get_spiral(webhook.spiral_id)
            if not spiral:
                logger.warning("Webhook received for unknown spiral: %s", webhook.spiral_id)
                return {"status": "error", "message": "Spiral not found"}

            if not spiral.enabled:
                logger.info("Webhook ignored for disabled spiral: %s", webhook.spiral_id)
                return {"status": "ignored", "message": "Spiral is disabled"}

            # Validate webhook if signature key is configured
            if spiral.trigger.type == TriggerType.WEBHOOK:
                config = spiral.trigger.config
                if (
                    hasattr(config, "signature_key")
                    and config.signature_key
                    and not self._validate_signature(webhook, config.signature_key)
                ):
                    logger.warning("Invalid webhook signature for spiral: %s", webhook.spiral_id)
                    return {"status": "error", "message": "Invalid signature"}

                # Check IP whitelist
                if (
                    hasattr(config, "allowed_ips")
                    and config.allowed_ips
                    and webhook.client_ip not in config.allowed_ips
                ):
                    logger.warning("Webhook from unauthorized IP %s for spiral: %s", webhook.client_ip, webhook.spiral_id)
                    return {"status": "error", "message": "Unauthorized IP"}

            # Check for duplicate webhook (deduplication)
            webhook_hash = self._compute_webhook_hash(webhook)
            if webhook_hash in self.webhook_cache:
                last_received = self.webhook_cache[webhook_hash]
                if (datetime.now(UTC) - last_received).total_seconds() < 5:
                    logger.info("Duplicate webhook ignored for spiral: %s", webhook.spiral_id)
                    return {"status": "ignored", "message": "Duplicate webhook"}

            self.webhook_cache[webhook_hash] = datetime.now(UTC)

            # Clean old cache entries
            self._clean_webhook_cache()

            # Execute spiral
            logger.info("Processing webhook for spiral: %s (%s)", spiral.name, webhook.spiral_id)
            context = await self.engine.execute(
                spiral_id=webhook.spiral_id,
                trigger_type="webhook",
                trigger_data={
                    "method": webhook.method,
                    "headers": webhook.headers,
                    "body": webhook.body,
                    "query_params": webhook.query_params,
                    "client_ip": webhook.client_ip,
                    "received_at": datetime.now(UTC).isoformat(),
                },
                metadata={"source": "webhook"},
            )

            return {
                "status": "accepted",
                "execution_id": context.execution_id,
                "spiral_id": webhook.spiral_id,
            }

        except Exception as e:
            logger.error("Error processing webhook for spiral %s: %s", webhook.spiral_id, e)
            return {"status": "error", "message": str(e)}

    def _validate_signature(self, webhook: WebhookPayload, signature_key: str) -> bool:
        """Validate webhook signature"""
        # Support multiple signature formats
        signature_header = (
            webhook.headers.get("x-webhook-signature")
            or webhook.headers.get("x-signature")
            or webhook.headers.get("x-hub-signature-256")
        )

        if not signature_header:
            logger.debug(
                "Webhook %s has no signature header (tried x-webhook-signature, x-signature, x-hub-signature-256)",
                webhook.spiral_id,
            )
            return False

        # Compute expected signature
        import json

        body_bytes = json.dumps(webhook.body).encode() if webhook.body else b""
        expected = hmac.new(signature_key.encode(), body_bytes, hashlib.sha256).hexdigest()

        # Compare (support sha256= prefix)
        actual = signature_header.replace("sha256=", "")
        return hmac.compare_digest(expected, actual)

    def _compute_webhook_hash(self, webhook: WebhookPayload) -> str:
        """Compute hash for deduplication"""

        content = f"{webhook.spiral_id}:{webhook.method}:{json.dumps(webhook.body, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()

    def _clean_webhook_cache(self):
        """Remove old entries from webhook cache"""
        cutoff = datetime.now(UTC)
        expired = [k for k, v in self.webhook_cache.items() if (cutoff - v).total_seconds() > 60]
        for k in expired:
            del self.webhook_cache[k]

    async def get_webhook_url(self, spiral_id: str) -> str:
        """Get the webhook URL for a spiral"""
        import os

        base_url = os.getenv("API_BASE_URL", "https://api.helixcollective.io")
        return f"{base_url}/api/spirals/webhook/{spiral_id}"

    async def register_webhook_endpoint(self, spiral_id: str, config: dict[str, Any]) -> dict[str, Any]:
        """Register a webhook endpoint for a spiral"""
        webhook_url = await self.get_webhook_url(spiral_id)

        return {
            "spiral_id": spiral_id,
            "webhook_url": webhook_url,
            "method": config.get("method", "POST"),
            "created_at": datetime.now(UTC).isoformat(),
        }
