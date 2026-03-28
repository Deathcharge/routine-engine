"""
Calendly Integration Connector for Helix Spirals.

Provides comprehensive Calendly API integration for scheduling automation,
event management, availability tracking, and webhook handling.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import aiohttp

from apps.backend.core.exceptions import MaxRetriesExceeded

logger = logging.getLogger(__name__)


class CalendlyEventStatus(Enum):
    """Calendly event status."""

    ACTIVE = "active"
    CANCELED = "canceled"


class CalendlyInviteeStatus(Enum):
    """Calendly invitee status."""

    ACTIVE = "active"
    CANCELED = "canceled"


@dataclass
class CalendlyConfig:
    """Configuration for Calendly connector."""

    api_key: str
    organization_uri: str | None = None
    user_uri: str | None = None
    timeout: int = 30
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        return "https://api.calendly.com"


@dataclass
class CalendlyUser:
    """Represents a Calendly user."""

    uri: str
    name: str
    email: str
    slug: str
    scheduling_url: str
    timezone: str
    avatar_url: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class CalendlyEventType:
    """Represents a Calendly event type."""

    uri: str
    name: str
    slug: str
    active: bool
    duration: int  # in minutes
    scheduling_url: str
    description: str | None = None
    color: str | None = None
    kind: str = "solo"  # solo, group, collective
    pooling_type: str | None = None
    type: str = "StandardEventType"


@dataclass
class CalendlyEvent:
    """Represents a scheduled Calendly event."""

    uri: str
    name: str
    status: CalendlyEventStatus
    start_time: datetime
    end_time: datetime
    event_type: str
    location: dict[str, Any] | None = None
    invitees_counter: dict[str, int] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    cancellation: dict[str, Any] | None = None


@dataclass
class CalendlyInvitee:
    """Represents a Calendly event invitee."""

    uri: str
    email: str
    name: str
    status: CalendlyInviteeStatus
    timezone: str
    event: str
    created_at: datetime
    updated_at: datetime
    questions_and_answers: list[dict[str, Any]] = field(default_factory=list)
    tracking: dict[str, Any] | None = None
    cancellation: dict[str, Any] | None = None
    payment: dict[str, Any] | None = None


@dataclass
class CalendlyAvailability:
    """Represents user availability."""

    user: str
    timezone: str
    rules: list[dict[str, Any]] = field(default_factory=list)


class CalendlyConnector:
    """
    Comprehensive Calendly API connector for Helix Spirals.

    Provides methods for:
    - User management
    - Event type management
    - Scheduled events management
    - Invitee management
    - Availability management
    - Webhook management
    - Organization management
    """

    def __init__(self, config: CalendlyConfig):
        self.config = config
        self._session: aiohttp.ClientSession | None = None
        self._current_user: CalendlyUser | None = None

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
        """Make an API request with retry logic."""
        session = await self._get_session()
        url = f"{self.config.base_url}/{endpoint}"

        for attempt in range(self.config.max_retries):
            try:
                async with session.request(method, url, json=data, params=params) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logger.warning("Rate limited, waiting %ss", retry_after)
                        await asyncio.sleep(retry_after)
                        continue

                    response.raise_for_status()

                    if response.status == 204:
                        return {}

                    return await response.json()

            except aiohttp.ClientError as e:
                if attempt == self.config.max_retries - 1:
                    raise
                logger.warning("Request failed, retrying: %s", e)
                await asyncio.sleep(2**attempt)

        raise MaxRetriesExceeded("Max retries exceeded")

    # ==================== User Management ====================

    async def get_current_user(self) -> CalendlyUser:
        """Get the current authenticated user."""
        if self._current_user:
            return self._current_user

        response = await self._request("GET", "users/me")
        resource = response.get("resource", {})

        self._current_user = CalendlyUser(
            uri=resource["uri"],
            name=resource["name"],
            email=resource["email"],
            slug=resource["slug"],
            scheduling_url=resource["scheduling_url"],
            timezone=resource["timezone"],
            avatar_url=resource.get("avatar_url"),
            created_at=self._parse_datetime(resource.get("created_at")),
            updated_at=self._parse_datetime(resource.get("updated_at")),
        )

        # Store user URI for later use
        if not self.config.user_uri:
            self.config.user_uri = self._current_user.uri

        return self._current_user

    async def get_user(self, user_uri: str) -> CalendlyUser:
        """Get a specific user by URI."""
        # Extract UUID from URI
        uuid = user_uri.split("/")[-1]
        response = await self._request("GET", f"users/{uuid}")
        resource = response.get("resource", {})

        return CalendlyUser(
            uri=resource["uri"],
            name=resource["name"],
            email=resource["email"],
            slug=resource["slug"],
            scheduling_url=resource["scheduling_url"],
            timezone=resource["timezone"],
            avatar_url=resource.get("avatar_url"),
            created_at=self._parse_datetime(resource.get("created_at")),
            updated_at=self._parse_datetime(resource.get("updated_at")),
        )

    # ==================== Event Type Management ====================

    async def get_event_types(
        self,
        user_uri: str | None = None,
        organization_uri: str | None = None,
        active: bool | None = None,
        count: int = 100,
    ) -> list[CalendlyEventType]:
        """Get event types for a user or organization."""
        params = {"count": count}

        if user_uri:
            params["user"] = user_uri
        elif self.config.user_uri:
            params["user"] = self.config.user_uri

        if organization_uri:
            params["organization"] = organization_uri
        elif self.config.organization_uri:
            params["organization"] = self.config.organization_uri

        if active is not None:
            params["active"] = str(active).lower()

        response = await self._request("GET", "event_types", params=params)

        event_types = []
        for item in response.get("collection", []):
            event_types.append(
                CalendlyEventType(
                    uri=item["uri"],
                    name=item["name"],
                    slug=item["slug"],
                    active=item["active"],
                    duration=item["duration"],
                    scheduling_url=item["scheduling_url"],
                    description=item.get("description_plain"),
                    color=item.get("color"),
                    kind=item.get("kind", "solo"),
                    pooling_type=item.get("pooling_type"),
                    type=item.get("type", "StandardEventType"),
                )
            )

        return event_types

    async def get_event_type(self, event_type_uri: str) -> CalendlyEventType:
        """Get a specific event type."""
        uuid = event_type_uri.split("/")[-1]
        response = await self._request("GET", f"event_types/{uuid}")
        item = response.get("resource", {})

        return CalendlyEventType(
            uri=item["uri"],
            name=item["name"],
            slug=item["slug"],
            active=item["active"],
            duration=item["duration"],
            scheduling_url=item["scheduling_url"],
            description=item.get("description_plain"),
            color=item.get("color"),
            kind=item.get("kind", "solo"),
            pooling_type=item.get("pooling_type"),
            type=item.get("type", "StandardEventType"),
        )

    # ==================== Scheduled Events Management ====================

    async def get_scheduled_events(
        self,
        user_uri: str | None = None,
        organization_uri: str | None = None,
        status: CalendlyEventStatus | None = None,
        min_start_time: datetime | None = None,
        max_start_time: datetime | None = None,
        count: int = 100,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        """Get scheduled events."""
        params = {"count": count}

        if user_uri:
            params["user"] = user_uri
        elif self.config.user_uri:
            params["user"] = self.config.user_uri

        if organization_uri:
            params["organization"] = organization_uri
        elif self.config.organization_uri:
            params["organization"] = self.config.organization_uri

        if status:
            params["status"] = status.value

        if min_start_time:
            params["min_start_time"] = min_start_time.isoformat()

        if max_start_time:
            params["max_start_time"] = max_start_time.isoformat()

        if page_token:
            params["page_token"] = page_token

        response = await self._request("GET", "scheduled_events", params=params)

        events = []
        for item in response.get("collection", []):
            events.append(self._parse_event(item))

        return {"events": events, "pagination": response.get("pagination", {})}

    async def get_scheduled_event(self, event_uri: str) -> CalendlyEvent:
        """Get a specific scheduled event."""
        uuid = event_uri.split("/")[-1]
        response = await self._request("GET", f"scheduled_events/{uuid}")
        return self._parse_event(response.get("resource", {}))

    async def cancel_event(self, event_uri: str, reason: str | None = None) -> CalendlyEvent:
        """Cancel a scheduled event."""
        uuid = event_uri.split("/")[-1]
        data = {}
        if reason:
            data["reason"] = reason

        await self._request("POST", f"scheduled_events/{uuid}/cancellation", data=data)

        # Fetch updated event
        return await self.get_scheduled_event(event_uri)

    # ==================== Invitee Management ====================

    async def get_event_invitees(
        self,
        event_uri: str,
        status: CalendlyInviteeStatus | None = None,
        count: int = 100,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        """Get invitees for a scheduled event."""
        uuid = event_uri.split("/")[-1]
        params = {"count": count}

        if status:
            params["status"] = status.value

        if page_token:
            params["page_token"] = page_token

        response = await self._request("GET", f"scheduled_events/{uuid}/invitees", params=params)

        invitees = []
        for item in response.get("collection", []):
            invitees.append(self._parse_invitee(item))

        return {"invitees": invitees, "pagination": response.get("pagination", {})}

    async def get_invitee(self, invitee_uri: str) -> CalendlyInvitee:
        """Get a specific invitee."""
        # Parse event UUID and invitee UUID from URI
        parts = invitee_uri.split("/")
        event_uuid = parts[-3]
        invitee_uuid = parts[-1]

        response = await self._request("GET", f"scheduled_events/{event_uuid}/invitees/{invitee_uuid}")

        return self._parse_invitee(response.get("resource", {}))

    # ==================== Availability Management ====================

    async def get_user_availability_schedules(self, user_uri: str | None = None) -> list[dict[str, Any]]:
        """Get availability schedules for a user."""
        params = {}
        if user_uri:
            params["user"] = user_uri
        elif self.config.user_uri:
            params["user"] = self.config.user_uri

        response = await self._request("GET", "user_availability_schedules", params=params)

        return response.get("collection", [])

    async def get_user_busy_times(
        self,
        user_uri: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Get busy times for a user."""
        params = {}

        if user_uri:
            params["user"] = user_uri
        elif self.config.user_uri:
            params["user"] = self.config.user_uri

        if start_time:
            params["start_time"] = start_time.isoformat()
        else:
            params["start_time"] = datetime.now(UTC).isoformat()

        if end_time:
            params["end_time"] = end_time.isoformat()
        else:
            params["end_time"] = (datetime.now(UTC) + timedelta(days=7)).isoformat()

        response = await self._request("GET", "user_busy_times", params=params)

        return response.get("collection", [])

    # ==================== Webhook Management ====================

    async def get_webhook_subscriptions(
        self,
        organization_uri: str | None = None,
        user_uri: str | None = None,
        scope: str = "user",
    ) -> list[dict[str, Any]]:
        """Get webhook subscriptions."""
        params = {"scope": scope}

        if organization_uri:
            params["organization"] = organization_uri
        elif self.config.organization_uri:
            params["organization"] = self.config.organization_uri

        if user_uri:
            params["user"] = user_uri
        elif self.config.user_uri:
            params["user"] = self.config.user_uri

        response = await self._request("GET", "webhook_subscriptions", params=params)

        return response.get("collection", [])

    async def create_webhook_subscription(
        self,
        url: str,
        events: list[str],
        organization_uri: str | None = None,
        user_uri: str | None = None,
        scope: str = "user",
        signing_key: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a webhook subscription.

        Events can include:
        - invitee.created
        - invitee.canceled
        - invitee_no_show.created
        - routing_form_submission.created
        """
        data = {"url": url, "events": events, "scope": scope}

        if organization_uri:
            data["organization"] = organization_uri
        elif self.config.organization_uri:
            data["organization"] = self.config.organization_uri

        if user_uri:
            data["user"] = user_uri
        elif self.config.user_uri:
            data["user"] = self.config.user_uri

        if signing_key:
            data["signing_key"] = signing_key

        response = await self._request("POST", "webhook_subscriptions", data=data)

        return response.get("resource", {})

    async def delete_webhook_subscription(self, webhook_uri: str) -> bool:
        """Delete a webhook subscription."""
        uuid = webhook_uri.split("/")[-1]
        await self._request("DELETE", f"webhook_subscriptions/{uuid}")
        return True

    # ==================== Organization Management ====================

    async def get_organization_membership(self) -> dict[str, Any]:
        """Get current user's organization membership."""
        user = await self.get_current_user()

        response = await self._request("GET", "organization_memberships", params={"user": user.uri})

        memberships = response.get("collection", [])
        if memberships:
            membership = memberships[0]
            # Store organization URI
            if not self.config.organization_uri:
                self.config.organization_uri = membership.get("organization")
            return membership

        return {}

    async def get_organization_invitations(
        self, organization_uri: str | None = None, count: int = 100
    ) -> list[dict[str, Any]]:
        """Get pending organization invitations."""
        org_uri = organization_uri or self.config.organization_uri
        if not org_uri:
            await self.get_organization_membership()
            org_uri = self.config.organization_uri

        uuid = org_uri.split("/")[-1]
        response = await self._request("GET", f"organizations/{uuid}/invitations", params={"count": count})

        return response.get("collection", [])

    # ==================== Routing Forms ====================

    async def get_routing_forms(self, organization_uri: str | None = None, count: int = 100) -> list[dict[str, Any]]:
        """Get routing forms."""
        params = {"count": count}

        if organization_uri:
            params["organization"] = organization_uri
        elif self.config.organization_uri:
            params["organization"] = self.config.organization_uri

        response = await self._request("GET", "routing_forms", params=params)
        return response.get("collection", [])

    async def get_routing_form_submissions(self, routing_form_uri: str, count: int = 100) -> list[dict[str, Any]]:
        """Get submissions for a routing form."""
        response = await self._request(
            "GET",
            "routing_form_submissions",
            params={"routing_form": routing_form_uri, "count": count},
        )
        return response.get("collection", [])

    # ==================== Helper Methods ====================

    def _parse_datetime(self, dt_str: str | None) -> datetime | None:
        """Parse ISO datetime string."""
        if not dt_str:
            return None
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

    def _parse_event(self, item: dict[str, Any]) -> CalendlyEvent:
        """Parse event data from API response."""
        return CalendlyEvent(
            uri=item["uri"],
            name=item["name"],
            status=CalendlyEventStatus(item["status"]),
            start_time=self._parse_datetime(item["start_time"]),
            end_time=self._parse_datetime(item["end_time"]),
            event_type=item["event_type"],
            location=item.get("location"),
            invitees_counter=item.get("invitees_counter", {}),
            created_at=self._parse_datetime(item.get("created_at")),
            updated_at=self._parse_datetime(item.get("updated_at")),
            cancellation=item.get("cancellation"),
        )

    def _parse_invitee(self, item: dict[str, Any]) -> CalendlyInvitee:
        """Parse invitee data from API response."""
        return CalendlyInvitee(
            uri=item["uri"],
            email=item["email"],
            name=item["name"],
            status=CalendlyInviteeStatus(item["status"]),
            timezone=item.get("timezone", "UTC"),
            event=item["event"],
            created_at=self._parse_datetime(item["created_at"]),
            updated_at=self._parse_datetime(item["updated_at"]),
            questions_and_answers=item.get("questions_and_answers", []),
            tracking=item.get("tracking"),
            cancellation=item.get("cancellation"),
            payment=item.get("payment"),
        )


# ==================== Helix Spirals Node Integration ====================


class CalendlyNode:
    """
    Helix Spirals node for Calendly integration.

    Supports operations:
    - get_event_types: List available event types
    - get_scheduled_events: Get scheduled events
    - get_event_invitees: Get invitees for an event
    - cancel_event: Cancel a scheduled event
    - get_availability: Get user availability
    - create_webhook: Create webhook subscription
    - get_busy_times: Get user busy times
    """

    def __init__(self, config: CalendlyConfig):
        self.connector = CalendlyConnector(config)

    async def execute(self, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        """Execute a Calendly operation."""
        operations = {
            "get_event_types": self._get_event_types,
            "get_scheduled_events": self._get_scheduled_events,
            "get_event_invitees": self._get_event_invitees,
            "cancel_event": self._cancel_event,
            "get_availability": self._get_availability,
            "create_webhook": self._create_webhook,
            "get_busy_times": self._get_busy_times,
            "get_current_user": self._get_current_user,
        }

        if operation not in operations:
            raise ValueError(f"Unknown operation: {operation}")

        return await operations[operation](params)

    async def _get_event_types(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get available event types."""
        event_types = await self.connector.get_event_types(
            user_uri=params.get("user_uri"), active=params.get("active", True)
        )

        return {
            "success": True,
            "event_types": [
                {
                    "uri": et.uri,
                    "name": et.name,
                    "slug": et.slug,
                    "duration": et.duration,
                    "scheduling_url": et.scheduling_url,
                    "active": et.active,
                }
                for et in event_types
            ],
        }

    async def _get_scheduled_events(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get scheduled events."""
        min_start = None
        max_start = None

        if params.get("min_start_time"):
            min_start = datetime.fromisoformat(params["min_start_time"])
        if params.get("max_start_time"):
            max_start = datetime.fromisoformat(params["max_start_time"])

        status = None
        if params.get("status"):
            status = CalendlyEventStatus(params["status"])

        result = await self.connector.get_scheduled_events(
            user_uri=params.get("user_uri"),
            status=status,
            min_start_time=min_start,
            max_start_time=max_start,
            count=params.get("count", 100),
        )

        return {
            "success": True,
            "events": [
                {
                    "uri": e.uri,
                    "name": e.name,
                    "status": e.status.value,
                    "start_time": e.start_time.isoformat(),
                    "end_time": e.end_time.isoformat(),
                    "invitees_count": e.invitees_counter.get("total", 0),
                }
                for e in result["events"]
            ],
            "pagination": result["pagination"],
        }

    async def _get_event_invitees(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get invitees for an event."""
        result = await self.connector.get_event_invitees(event_uri=params["event_uri"], count=params.get("count", 100))

        return {
            "success": True,
            "invitees": [
                {
                    "uri": i.uri,
                    "email": i.email,
                    "name": i.name,
                    "status": i.status.value,
                    "timezone": i.timezone,
                    "questions_and_answers": i.questions_and_answers,
                }
                for i in result["invitees"]
            ],
            "pagination": result["pagination"],
        }

    async def _cancel_event(self, params: dict[str, Any]) -> dict[str, Any]:
        """Cancel a scheduled event."""
        event = await self.connector.cancel_event(event_uri=params["event_uri"], reason=params.get("reason"))

        return {"success": True, "event_uri": event.uri, "status": event.status.value}

    async def _get_availability(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get user availability schedules."""
        schedules = await self.connector.get_user_availability_schedules(user_uri=params.get("user_uri"))

        return {"success": True, "schedules": schedules}

    async def _create_webhook(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create a webhook subscription."""
        webhook = await self.connector.create_webhook_subscription(
            url=params["url"],
            events=params["events"],
            scope=params.get("scope", "user"),
            signing_key=params.get("signing_key"),
        )

        return {
            "success": True,
            "webhook_uri": webhook.get("uri"),
            "callback_url": webhook.get("callback_url"),
            "events": webhook.get("events", []),
        }

    async def _get_busy_times(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get user busy times."""
        start_time = None
        end_time = None

        if params.get("start_time"):
            start_time = datetime.fromisoformat(params["start_time"])
        if params.get("end_time"):
            end_time = datetime.fromisoformat(params["end_time"])

        busy_times = await self.connector.get_user_busy_times(
            user_uri=params.get("user_uri"), start_time=start_time, end_time=end_time
        )

        return {"success": True, "busy_times": busy_times}

    async def _get_current_user(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get current authenticated user."""
        user = await self.connector.get_current_user()

        return {
            "success": True,
            "user": {
                "uri": user.uri,
                "name": user.name,
                "email": user.email,
                "scheduling_url": user.scheduling_url,
                "timezone": user.timezone,
            },
        }

    async def close(self):
        """Close the connector."""
        await self.connector.close()
