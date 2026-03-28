"""
SendGrid Integration Connector for Helix Spirals.

Provides comprehensive SendGrid API integration for email operations,
template management, contact management, and event tracking.
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class SendGridEventType(Enum):
    """SendGrid event types."""

    OPENED = "open"
    CLICKED = "click"
    DELIVERED = "delivered"
    BOUNCED = "bounce"
    DROPPED = "dropped"
    SPAM_REPORT = "spam_report"
    UNSUBSCRIBED = "unsubscribe"
    PROCESSED = "processed"


@dataclass
class SendGridConfig:
    """Configuration for SendGrid connector."""

    api_key: str
    timeout: int = 30
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        return "https://api.sendgrid.com/v3"


@dataclass
class SendGridEmail:
    """Represents an email to send."""

    to: list[str]
    from_email: str
    subject: str
    content: str
    content_type: str = "text/html"
    cc: list[str] = field(default_factory=list)
    bcc: list[str] = field(default_factory=list)
    reply_to: str | None = None
    template_id: str | None = None
    dynamic_template_data: dict[str, Any] = field(default_factory=dict)
    attachments: list[dict[str, Any]] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    custom_args: dict[str, Any] = field(default_factory=dict)
    send_at: datetime | None = None
    tracking_settings: dict[str, Any] = field(default_factory=dict)


@dataclass
class SendGridResponse:
    """Represents a SendGrid API response."""

    message_id: str
    status_code: int
    body: str
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class SendGridContact:
    """Represents a SendGrid contact."""

    id: str
    email: str
    first_name: str | None = None
    last_name: str | None = None
    custom_fields: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class SendGridEvent:
    """Represents a SendGrid event."""

    email: str
    event_type: SendGridEventType
    timestamp: datetime
    sg_message_id: str | None = None
    sg_event_id: str | None = None
    url: str | None = None
    user_agent: str | None = None
    ip: str | None = None
    reason: str | None = None
    status: str | None = None
    response: str | None = None


class SendGridConnector:
    """
    Comprehensive SendGrid API connector for Helix Spirals.

    Provides methods for:
    - Email sending
    - Template management
    - Contact management
    - Event tracking
    - List management
    - Sender authentication
    """

    def __init__(self, config: SendGridConfig):
        self.config = config
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
        params: dict | None = None,
    ) -> dict[str, Any]:
        """Make an API request."""
        session = await self._get_session()
        url = f"{self.config.base_url}/{endpoint}"

        async with session.request(method, url, json=data, params=params) as response:
            if response.status not in [200, 201, 202]:
                error_text = await response.text()
                raise ValueError(f"SendGrid API error: {response.status} - {error_text}")

            return await response.json()

    # ==================== Email Operations ====================

    async def send_email(self, email: SendGridEmail) -> SendGridResponse:
        """Send an email."""
        payload = {
            "personalizations": [{"to": [{"email": addr} for addr in email.to], "subject": email.subject}],
            "from": {"email": email.from_email},
            "content": [{"type": email.content_type, "value": email.content}],
        }

        # Add CC
        if email.cc:
            payload["personalizations"][0]["cc"] = [{"email": addr} for addr in email.cc]

        # Add BCC
        if email.bcc:
            payload["personalizations"][0]["bcc"] = [{"email": addr} for addr in email.bcc]

        # Add reply-to
        if email.reply_to:
            payload["reply_to"] = {"email": email.reply_to}

        # Add template
        if email.template_id:
            payload["template_id"] = email.template_id
            payload["personalizations"][0]["dynamic_template_data"] = email.dynamic_template_data
            # Remove content when using template
            payload.pop("content", None)

        # Add attachments
        if email.attachments:
            payload["attachments"] = email.attachments

        # Add categories
        if email.categories:
            payload["categories"] = email.categories

        # Add custom args
        if email.custom_args:
            payload["custom_args"] = email.custom_args

        # Add send_at
        if email.send_at:
            payload["send_at"] = int(email.send_at.timestamp())

        # Add tracking settings
        if email.tracking_settings:
            payload["tracking_settings"] = email.tracking_settings

        response = await self._request("POST", "mail/send", payload)

        return SendGridResponse(
            message_id=response.get("X-Message-Id", ""),
            status_code=202,
            body=json.dumps(response),
            headers={},
        )

    async def send_bulk_emails(self, emails: list[SendGridEmail]) -> list[SendGridResponse]:
        """Send multiple emails in parallel."""
        tasks = [self.send_email(email) for email in emails]
        return await asyncio.gather(*tasks)

    async def send_email_simple(
        self,
        to: str | list[str],
        from_email: str,
        subject: str,
        content: str,
        content_type: str = "text/html",
    ) -> SendGridResponse:
        """Send a simple email (convenience method)."""
        if isinstance(to, str):
            to = [to]

        email = SendGridEmail(
            to=to,
            from_email=from_email,
            subject=subject,
            content=content,
            content_type=content_type,
        )

        return await self.send_email(email)

    async def send_template_email(
        self,
        to: str | list[str],
        from_email: str,
        template_id: str,
        dynamic_data: dict[str, Any],
        subject: str | None = None,
    ) -> SendGridResponse:
        """Send an email using a template."""
        if isinstance(to, str):
            to = [to]

        email = SendGridEmail(
            to=to,
            from_email=from_email,
            subject=subject or "",
            content="",
            template_id=template_id,
            dynamic_template_data=dynamic_data,
        )

        return await self.send_email(email)

    # ==================== Template Operations ====================

    async def list_templates(self, page_size: int = 100) -> list[dict[str, Any]]:
        """List all email templates."""
        response = await self._request("GET", f"templates?generations=dynamic&page_size={page_size}")
        return response.get("templates", [])

    async def get_template(self, template_id: str) -> dict[str, Any]:
        """Get a specific template."""
        return await self._request("GET", f"templates/{template_id}")

    async def create_template(self, name: str, generation: str = "dynamic") -> dict[str, Any]:
        """Create a new email template."""
        return await self._request("POST", "templates", {"name": name, "generation": generation})

    async def delete_template(self, template_id: str) -> bool:
        """Delete a template."""
        await self._request("DELETE", f"templates/{template_id}")
        return True

    async def create_template_version(
        self,
        template_id: str,
        name: str,
        subject: str,
        html_content: str,
        plain_content: str,
        active: bool = True,
    ) -> dict[str, Any]:
        """Create a new version of a template."""
        payload = {
            "name": name,
            "subject": subject,
            "html_content": html_content,
            "plain_content": plain_content,
            "active": active,
        }
        return await self._request("POST", f"templates/{template_id}/versions", payload)

    # ==================== Contact Operations ====================

    async def create_contact(
        self,
        email: str,
        first_name: str | None = None,
        last_name: str | None = None,
        custom_fields: dict[str, Any] | None = None,
    ) -> SendGridContact:
        """Create a new contact."""
        payload = {"contacts": [{"email": email}]}

        if first_name:
            payload["contacts"][0]["first_name"] = first_name
        if last_name:
            payload["contacts"][0]["last_name"] = last_name
        if custom_fields:
            payload["contacts"][0].update(custom_fields)

        response = await self._request("PUT", "marketing/contacts", payload)

        # Parse response to get contact ID
        contact_id = None
        if response.get("job_id"):
            contact_id = response["job_id"]

        return SendGridContact(
            id=contact_id or "",
            email=email,
            first_name=first_name,
            last_name=last_name,
            custom_fields=custom_fields or {},
        )

    async def get_contact(self, email: str) -> SendGridContact | None:
        """Get a contact by email."""
        response = await self._request("GET", f"marketing/contacts/search?query=email%20LIKE%20%27{email}%27")

        contacts = response.get("result", [])
        if not contacts:
            return None

        contact_data = contacts[0]
        return SendGridContact(
            id=contact_data.get("id", ""),
            email=contact_data.get("email", ""),
            first_name=contact_data.get("first_name"),
            last_name=contact_data.get("last_name"),
            custom_fields={
                k: v
                for k, v in contact_data.items()
                if k
                not in [
                    "id",
                    "email",
                    "first_name",
                    "last_name",
                    "created_at",
                    "updated_at",
                ]
            },
            created_at=self._parse_datetime(contact_data.get("created_at")),
            updated_at=self._parse_datetime(contact_data.get("updated_at")),
        )

    async def update_contact(
        self,
        email: str,
        first_name: str | None = None,
        last_name: str | None = None,
        custom_fields: dict[str, Any] | None = None,
    ) -> bool:
        """Update a contact."""
        payload = {"contacts": [{"email": email}]}

        if first_name:
            payload["contacts"][0]["first_name"] = first_name
        if last_name:
            payload["contacts"][0]["last_name"] = last_name
        if custom_fields:
            payload["contacts"][0].update(custom_fields)

        await self._request("PUT", "marketing/contacts", payload)
        return True

    async def delete_contact(self, email: str) -> bool:
        """Delete a contact."""
        payload = {"emails": [email]}
        await self._request("DELETE", "marketing/contacts", payload)
        return True

    async def add_contact_to_list(self, list_id: str, emails: list[str]) -> bool:
        """Add contacts to a list."""
        payload = {
            "list_id": list_id,
            "contacts": [{"email": email} for email in emails],
        }
        await self._request("POST", "marketing/lists/{list_id}/contacts", payload)
        return True

    # ==================== List Operations ====================

    async def create_list(self, name: str) -> dict[str, Any]:
        """Create a new contact list."""
        return await self._request("POST", "marketing/lists", {"name": name})

    async def get_lists(self, page_size: int = 100) -> list[dict[str, Any]]:
        """Get all contact lists."""
        response = await self._request("GET", f"marketing/lists?page_size={page_size}")
        return response.get("result", [])

    async def delete_list(self, list_id: str) -> bool:
        """Delete a contact list."""
        await self._request("DELETE", f"marketing/lists/{list_id}")
        return True

    # ==================== Event Operations ====================

    async def get_events(
        self,
        start_date: datetime,
        end_date: datetime,
        event_types: list[SendGridEventType] | None = None,
        email: str | None = None,
        limit: int = 1000,
    ) -> list[SendGridEvent]:
        """Get engagement events."""
        params = {
            "start_time": int(start_date.timestamp()),
            "end_time": int(end_date.timestamp()),
            "limit": str(min(limit, 10000)),
        }

        if event_types:
            params["event_types"] = [e.value for e in event_types]

        if email:
            params["email"] = email

        response = await self._request("GET", "messages/events", params=params)

        events = []
        for item in response.get("events", []):
            try:
                event_type = SendGridEventType(item.get("event"))
            except ValueError:
                continue

            events.append(
                SendGridEvent(
                    email=item.get("email", ""),
                    event_type=event_type,
                    timestamp=datetime.fromtimestamp(int(item["timestamp"])),
                    sg_message_id=item.get("sg_message_id"),
                    sg_event_id=item.get("sg_event_id"),
                    url=item.get("url"),
                    user_agent=item.get("useragent"),
                    ip=item.get("ip"),
                    reason=item.get("reason"),
                    status=item.get("status"),
                    response=item.get("response"),
                )
            )

        return events

    async def get_bounces(self, start_date: datetime, end_date: datetime) -> list[dict[str, Any]]:
        """Get bounce events."""
        return await self.get_events(start_date, end_date, [SendGridEventType.BOUNCED])

    async def get_opens(self, start_date: datetime, end_date: datetime) -> list[dict[str, Any]]:
        """Get open events."""
        return await self.get_events(start_date, end_date, [SendGridEventType.OPENED])

    async def get_clicks(self, start_date: datetime, end_date: datetime) -> list[dict[str, Any]]:
        """Get click events."""
        return await self.get_events(start_date, end_date, [SendGridEventType.CLICKED])

    # ==================== Statistics ====================

    async def get_email_stats(
        self, start_date: datetime, end_date: datetime, aggregated_by: str = "day"
    ) -> dict[str, Any]:
        """Get email statistics."""
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "aggregated_by": aggregated_by,
        }

        response = await self._request("GET", "stats", params=params)
        return response.get("stats", [])

    # ==================== Helper Methods ====================

    def _parse_datetime(self, dt_str: str | None) -> datetime | None:
        """Parse ISO datetime string."""
        if not dt_str:
            return None
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


# ==================== Helix Spirals Node Integration ====================


class SendGridNode:
    """
    Helix Spirals node for SendGrid integration.

    Supports operations:
    - send_email: Send email
    - send_template: Send template email
    - create_contact: Create contact
    - get_contact: Get contact info
    - get_events: Get email events
    - get_stats: Get email statistics
    - create_list: Create contact list
    """

    def __init__(self, config: SendGridConfig):
        self.connector = SendGridConnector(config)

    async def execute(self, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        """Execute a SendGrid operation."""
        operations = {
            "send_email": self._send_email,
            "send_template_email": self._send_template_email,
            "create_contact": self._create_contact,
            "get_contact": self._get_contact,
            "get_events": self._get_events,
            "get_stats": self._get_stats,
            "create_list": self._create_list,
            "send_bulk_emails": self._send_bulk_emails,
        }

        if operation not in operations:
            raise ValueError(f"Unknown operation: {operation}")

        return await operations[operation](params)

    async def _send_email(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send email."""
        email = SendGridEmail(
            to=params["to"] if isinstance(params["to"], list) else [params["to"]],
            from_email=params["from_email"],
            subject=params["subject"],
            content=params["content"],
            content_type=params.get("content_type", "text/html"),
            cc=params.get("cc", []),
            bcc=params.get("bcc", []),
            reply_to=params.get("reply_to"),
            attachments=params.get("attachments", []),
            categories=params.get("categories", []),
        )

        response = await self.connector.send_email(email)

        return {
            "success": True,
            "message_id": response.message_id,
            "status_code": response.status_code,
        }

    async def _send_template_email(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send template email."""
        response = await self.connector.send_template_email(
            to=params["to"],
            from_email=params["from_email"],
            template_id=params["template_id"],
            dynamic_data=params["dynamic_data"],
            subject=params.get("subject"),
        )

        return {
            "success": True,
            "message_id": response.message_id,
            "status_code": response.status_code,
        }

    async def _create_contact(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create contact."""
        contact = await self.connector.create_contact(
            email=params["email"],
            first_name=params.get("first_name"),
            last_name=params.get("last_name"),
            custom_fields=params.get("custom_fields"),
        )

        return {"success": True, "contact_id": contact.id, "email": contact.email}

    async def _get_contact(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get contact."""
        contact = await self.connector.get_contact(params["email"])

        if not contact:
            return {"success": False, "error": "Contact not found"}

        return {
            "success": True,
            "contact_id": contact.id,
            "email": contact.email,
            "first_name": contact.first_name,
            "last_name": contact.last_name,
            "custom_fields": contact.custom_fields,
        }

    async def _get_events(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get email events."""
        start_date = datetime.fromisoformat(params["start_date"])
        end_date = datetime.fromisoformat(params["end_date"])

        event_types = None
        if params.get("event_types"):
            event_types = [SendGridEventType(t) for t in params["event_types"]]

        events = await self.connector.get_events(
            start_date=start_date,
            end_date=end_date,
            event_types=event_types,
            email=params.get("email"),
            limit=params.get("limit", 100),
        )

        return {
            "success": True,
            "events": [
                {
                    "email": e.email,
                    "event_type": e.event_type.value,
                    "timestamp": e.timestamp.isoformat(),
                    "sg_message_id": e.sg_message_id,
                }
                for e in events
            ],
            "count": len(events),
        }

    async def _get_stats(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get email statistics."""
        start_date = datetime.fromisoformat(params["start_date"])
        end_date = datetime.fromisoformat(params["end_date"])

        stats = await self.connector.get_email_stats(
            start_date=start_date,
            end_date=end_date,
            aggregated_by=params.get("aggregated_by", "day"),
        )

        return {"success": True, "stats": stats}

    async def _create_list(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create contact list."""
        result = await self.connector.create_list(params["name"])

        return {
            "success": True,
            "list_id": result.get("id"),
            "name": result.get("name"),
        }

    async def _send_bulk_emails(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send bulk emails."""
        emails = []
        for recipient in params["recipients"]:
            email = SendGridEmail(
                to=recipient["email"],
                from_email=params["from_email"],
                subject=params["subject"],
                content=recipient.get("content", params["content"]),
                dynamic_template_data=recipient.get("dynamic_data"),
            )
            emails.append(email)

        responses = await self.connector.send_bulk_emails(emails)

        return {
            "success": True,
            "sent_count": len(responses),
            "message_ids": [r.message_id for r in responses],
        }

    async def close(self):
        """Close the connector."""
        await self.connector.close()
