"""
Mailchimp Integration Connector for Helix Spirals.

Provides comprehensive Mailchimp API integration for email marketing automation,
audience management, campaign creation, and analytics tracking.
"""

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import aiohttp

from apps.backend.core.exceptions import MaxRetriesExceeded

logger = logging.getLogger(__name__)


class MailchimpCampaignType(Enum):
    """Mailchimp campaign types."""

    REGULAR = "regular"
    PLAINTEXT = "plaintext"
    ABSPLIT = "absplit"
    RSS = "rss"
    VARIATE = "variate"


class MailchimpMemberStatus(Enum):
    """Mailchimp member subscription status."""

    SUBSCRIBED = "subscribed"
    UNSUBSCRIBED = "unsubscribed"
    CLEANED = "cleaned"
    PENDING = "pending"
    TRANSACTIONAL = "transactional"


@dataclass
class MailchimpConfig:
    """Configuration for Mailchimp connector."""

    api_key: str
    server_prefix: str  # e.g., 'us1', 'us2', etc.
    timeout: int = 30
    max_retries: int = 3
    batch_size: int = 500

    @property
    def base_url(self) -> str:
        return f"https://{self.server_prefix}.api.mailchimp.com/3.0"


@dataclass
class MailchimpAudience:
    """Represents a Mailchimp audience/list."""

    id: str
    name: str
    member_count: int
    unsubscribe_count: int
    cleaned_count: int
    campaign_count: int
    date_created: datetime
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass
class MailchimpMember:
    """Represents a Mailchimp audience member."""

    id: str
    email_address: str
    status: MailchimpMemberStatus
    merge_fields: dict[str, Any]
    tags: list[str]
    timestamp_signup: datetime | None
    timestamp_opt: datetime | None
    member_rating: int
    last_changed: datetime


@dataclass
class MailchimpCampaign:
    """Represents a Mailchimp campaign."""

    id: str
    type: MailchimpCampaignType
    status: str
    title: str
    subject_line: str
    preview_text: str
    emails_sent: int
    send_time: datetime | None
    create_time: datetime
    report_summary: dict[str, Any] = field(default_factory=dict)


class MailchimpConnector:
    """
    Comprehensive Mailchimp API connector for Helix Spirals.

    Provides methods for:
    - Audience/List management
    - Member/Subscriber management
    - Campaign creation and management
    - Template management
    - Analytics and reporting
    - Automation workflows
    - Tagging and segmentation
    """

    def __init__(self, config: MailchimpConfig):
        self.config = config
        self._session: aiohttp.ClientSession | None = None
        self._rate_limit_remaining = 10
        self._rate_limit_reset = datetime.now(UTC)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            auth = aiohttp.BasicAuth("anystring", self.config.api_key)
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            self._session = aiohttp.ClientSession(
                auth=auth, timeout=timeout, headers={"Content-Type": "application/json"}
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
                    # Update rate limit info
                    self._rate_limit_remaining = int(response.headers.get("X-RateLimit-Remaining", 10))

                    if response.status == 429:
                        # Rate limited, wait and retry
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logger.warning("Rate limited, waiting %ss", retry_after)
                        await asyncio.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    return await response.json()

            except aiohttp.ClientError as e:
                if attempt == self.config.max_retries - 1:
                    raise
                logger.warning("Request failed, retrying: %s", e)
                await asyncio.sleep(2**attempt)

        raise MaxRetriesExceeded("Max retries exceeded")

    @staticmethod
    def get_subscriber_hash(email: str) -> str:
        """Get MD5 hash of lowercase email for Mailchimp API."""
        return hashlib.md5(email.lower().encode(), usedforsecurity=False).hexdigest()

    # ==================== Audience/List Management ====================

    async def get_audiences(self, count: int = 100, offset: int = 0) -> list[MailchimpAudience]:
        """Get all audiences/lists."""
        response = await self._request("GET", "lists", params={"count": count, "offset": offset})

        audiences = []
        for item in response.get("lists", []):
            audiences.append(
                MailchimpAudience(
                    id=item["id"],
                    name=item["name"],
                    member_count=item["stats"]["member_count"],
                    unsubscribe_count=item["stats"]["unsubscribe_count"],
                    cleaned_count=item["stats"]["cleaned_count"],
                    campaign_count=item["stats"]["campaign_count"],
                    date_created=datetime.fromisoformat(item["date_created"].replace("Z", "+00:00")),
                    stats=item["stats"],
                )
            )

        return audiences

    async def get_audience(self, list_id: str) -> MailchimpAudience:
        """Get a specific audience by ID."""
        item = await self._request("GET", f"lists/{list_id}")

        return MailchimpAudience(
            id=item["id"],
            name=item["name"],
            member_count=item["stats"]["member_count"],
            unsubscribe_count=item["stats"]["unsubscribe_count"],
            cleaned_count=item["stats"]["cleaned_count"],
            campaign_count=item["stats"]["campaign_count"],
            date_created=datetime.fromisoformat(item["date_created"].replace("Z", "+00:00")),
            stats=item["stats"],
        )

    async def create_audience(
        self,
        name: str,
        permission_reminder: str,
        email_type_option: bool = True,
        contact: dict[str, str] = None,
        campaign_defaults: dict[str, str] = None,
    ) -> MailchimpAudience:
        """Create a new audience/list."""
        data = {
            "name": name,
            "permission_reminder": permission_reminder,
            "email_type_option": email_type_option,
            "contact": contact
            or {
                "company": "Your Company",
                "address1": "123 Main St",
                "city": "Anytown",
                "state": "CA",
                "zip": "12345",
                "country": "US",
            },
            "campaign_defaults": campaign_defaults
            or {
                "from_name": "Your Name",
                "from_email": "you@example.com",
                "subject": "",
                "language": "en",
            },
        }

        item = await self._request("POST", "lists", data=data)

        return MailchimpAudience(
            id=item["id"],
            name=item["name"],
            member_count=0,
            unsubscribe_count=0,
            cleaned_count=0,
            campaign_count=0,
            date_created=datetime.fromisoformat(item["date_created"].replace("Z", "+00:00")),
            stats=item.get("stats", {}),
        )

    # ==================== Member Management ====================

    async def get_members(
        self,
        list_id: str,
        status: MailchimpMemberStatus | None = None,
        count: int = 100,
        offset: int = 0,
    ) -> list[MailchimpMember]:
        """Get members from an audience."""
        params = {"count": count, "offset": offset}
        if status:
            params["status"] = status.value

        response = await self._request("GET", f"lists/{list_id}/members", params=params)

        members = []
        for item in response.get("members", []):
            members.append(self._parse_member(item))

        return members

    async def get_member(self, list_id: str, email: str) -> MailchimpMember:
        """Get a specific member by email."""
        subscriber_hash = self.get_subscriber_hash(email)
        item = await self._request("GET", f"lists/{list_id}/members/{subscriber_hash}")
        return self._parse_member(item)

    async def add_member(
        self,
        list_id: str,
        email: str,
        status: MailchimpMemberStatus = MailchimpMemberStatus.SUBSCRIBED,
        merge_fields: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        interests: dict[str, bool] | None = None,
    ) -> MailchimpMember:
        """Add a new member to an audience."""
        data = {"email_address": email, "status": status.value}

        if merge_fields:
            data["merge_fields"] = merge_fields
        if tags:
            data["tags"] = tags
        if interests:
            data["interests"] = interests

        item = await self._request("POST", f"lists/{list_id}/members", data=data)

        return self._parse_member(item)

    async def update_member(
        self,
        list_id: str,
        email: str,
        status: MailchimpMemberStatus | None = None,
        merge_fields: dict[str, Any] | None = None,
    ) -> MailchimpMember:
        """Update an existing member."""
        subscriber_hash = self.get_subscriber_hash(email)
        data = {}

        if status:
            data["status"] = status.value
        if merge_fields:
            data["merge_fields"] = merge_fields

        item = await self._request("PATCH", f"lists/{list_id}/members/{subscriber_hash}", data=data)

        return self._parse_member(item)

    async def add_or_update_member(
        self,
        list_id: str,
        email: str,
        status: MailchimpMemberStatus = MailchimpMemberStatus.SUBSCRIBED,
        merge_fields: dict[str, Any] | None = None,
    ) -> MailchimpMember:
        """Add or update a member (upsert)."""
        subscriber_hash = self.get_subscriber_hash(email)
        data = {"email_address": email, "status_if_new": status.value}

        if merge_fields:
            data["merge_fields"] = merge_fields

        item = await self._request("PUT", f"lists/{list_id}/members/{subscriber_hash}", data=data)

        return self._parse_member(item)

    async def delete_member(self, list_id: str, email: str) -> bool:
        """Permanently delete a member."""
        subscriber_hash = self.get_subscriber_hash(email)
        await self._request("DELETE", f"lists/{list_id}/members/{subscriber_hash}")
        return True

    async def batch_subscribe(
        self, list_id: str, members: list[dict[str, Any]], update_existing: bool = True
    ) -> dict[str, Any]:
        """Batch subscribe/update multiple members."""
        data = {"members": members, "update_existing": update_existing}

        return await self._request("POST", f"lists/{list_id}", data=data)

    # ==================== Tag Management ====================

    async def get_member_tags(self, list_id: str, email: str) -> list[dict[str, Any]]:
        """Get tags for a member."""
        subscriber_hash = self.get_subscriber_hash(email)
        response = await self._request("GET", f"lists/{list_id}/members/{subscriber_hash}/tags")
        return response.get("tags", [])

    async def update_member_tags(self, list_id: str, email: str, tags: list[dict[str, str]]) -> bool:
        """Update tags for a member. Tags format: [{"name": "tag", "status": "active/inactive"}]"""
        subscriber_hash = self.get_subscriber_hash(email)
        await self._request(
            "POST",
            f"lists/{list_id}/members/{subscriber_hash}/tags",
            data={"tags": tags},
        )
        return True

    # ==================== Campaign Management ====================

    async def get_campaigns(
        self, status: str | None = None, count: int = 100, offset: int = 0
    ) -> list[MailchimpCampaign]:
        """Get campaigns."""
        params = {"count": count, "offset": offset}
        if status:
            params["status"] = status

        response = await self._request("GET", "campaigns", params=params)

        campaigns = []
        for item in response.get("campaigns", []):
            campaigns.append(self._parse_campaign(item))

        return campaigns

    async def get_campaign(self, campaign_id: str) -> MailchimpCampaign:
        """Get a specific campaign."""
        item = await self._request("GET", f"campaigns/{campaign_id}")
        return self._parse_campaign(item)

    async def create_campaign(
        self,
        campaign_type: MailchimpCampaignType,
        list_id: str,
        subject_line: str,
        from_name: str,
        reply_to: str,
        title: str | None = None,
        preview_text: str | None = None,
    ) -> MailchimpCampaign:
        """Create a new campaign."""
        data = {
            "type": campaign_type.value,
            "recipients": {"list_id": list_id},
            "settings": {
                "subject_line": subject_line,
                "from_name": from_name,
                "reply_to": reply_to,
                "title": title or subject_line,
            },
        }

        if preview_text:
            data["settings"]["preview_text"] = preview_text

        item = await self._request("POST", "campaigns", data=data)
        return self._parse_campaign(item)

    async def set_campaign_content(
        self,
        campaign_id: str,
        html: str | None = None,
        plain_text: str | None = None,
        template_id: int | None = None,
    ) -> dict[str, Any]:
        """Set campaign content."""
        data = {}

        if html:
            data["html"] = html
        if plain_text:
            data["plain_text"] = plain_text
        if template_id:
            data["template"] = {"id": template_id}

        return await self._request("PUT", f"campaigns/{campaign_id}/content", data=data)

    async def send_campaign(self, campaign_id: str) -> bool:
        """Send a campaign immediately."""
        await self._request("POST", f"campaigns/{campaign_id}/actions/send")
        return True

    async def schedule_campaign(self, campaign_id: str, schedule_time: datetime) -> bool:
        """Schedule a campaign for later."""
        data = {"schedule_time": schedule_time.isoformat()}
        await self._request("POST", f"campaigns/{campaign_id}/actions/schedule", data=data)
        return True

    async def delete_campaign(self, campaign_id: str) -> bool:
        """Delete a campaign."""
        await self._request("DELETE", f"campaigns/{campaign_id}")
        return True

    # ==================== Campaign Reports ====================

    async def get_campaign_report(self, campaign_id: str) -> dict[str, Any]:
        """Get campaign report/analytics."""
        return await self._request("GET", f"reports/{campaign_id}")

    async def get_campaign_click_details(self, campaign_id: str, count: int = 100) -> dict[str, Any]:
        """Get click details for a campaign."""
        return await self._request("GET", f"reports/{campaign_id}/click-details", params={"count": count})

    async def get_campaign_open_details(self, campaign_id: str, count: int = 100) -> dict[str, Any]:
        """Get open details for a campaign."""
        return await self._request("GET", f"reports/{campaign_id}/open-details", params={"count": count})

    # ==================== Templates ====================

    async def get_templates(self, template_type: str = "user", count: int = 100) -> list[dict[str, Any]]:
        """Get email templates."""
        response = await self._request("GET", "templates", params={"type": template_type, "count": count})
        return response.get("templates", [])

    async def create_template(self, name: str, html: str) -> dict[str, Any]:
        """Create a new template."""
        return await self._request("POST", "templates", data={"name": name, "html": html})

    # ==================== Automations ====================

    async def get_automations(self) -> list[dict[str, Any]]:
        """Get all automations."""
        response = await self._request("GET", "automations")
        return response.get("automations", [])

    async def get_automation(self, workflow_id: str) -> dict[str, Any]:
        """Get a specific automation."""
        return await self._request("GET", f"automations/{workflow_id}")

    async def start_automation(self, workflow_id: str) -> bool:
        """Start an automation."""
        await self._request("POST", f"automations/{workflow_id}/actions/start-all-emails")
        return True

    async def pause_automation(self, workflow_id: str) -> bool:
        """Pause an automation."""
        await self._request("POST", f"automations/{workflow_id}/actions/pause-all-emails")
        return True

    # ==================== Segments ====================

    async def get_segments(self, list_id: str, count: int = 100) -> list[dict[str, Any]]:
        """Get segments for an audience."""
        response = await self._request("GET", f"lists/{list_id}/segments", params={"count": count})
        return response.get("segments", [])

    async def create_segment(
        self,
        list_id: str,
        name: str,
        conditions: list[dict[str, Any]],
        match: str = "all",
    ) -> dict[str, Any]:
        """Create a new segment."""
        data = {"name": name, "options": {"match": match, "conditions": conditions}}
        return await self._request("POST", f"lists/{list_id}/segments", data=data)

    # ==================== Helper Methods ====================

    def _parse_member(self, item: dict[str, Any]) -> MailchimpMember:
        """Parse member data from API response."""
        return MailchimpMember(
            id=item["id"],
            email_address=item["email_address"],
            status=MailchimpMemberStatus(item["status"]),
            merge_fields=item.get("merge_fields", {}),
            tags=[t["name"] for t in item.get("tags", [])],
            timestamp_signup=(
                datetime.fromisoformat(item["timestamp_signup"].replace("Z", "+00:00"))
                if item.get("timestamp_signup")
                else None
            ),
            timestamp_opt=(
                datetime.fromisoformat(item["timestamp_opt"].replace("Z", "+00:00"))
                if item.get("timestamp_opt")
                else None
            ),
            member_rating=item.get("member_rating", 0),
            last_changed=datetime.fromisoformat(item["last_changed"].replace("Z", "+00:00")),
        )

    def _parse_campaign(self, item: dict[str, Any]) -> MailchimpCampaign:
        """Parse campaign data from API response."""
        settings = item.get("settings", {})
        return MailchimpCampaign(
            id=item["id"],
            type=MailchimpCampaignType(item["type"]),
            status=item["status"],
            title=settings.get("title", ""),
            subject_line=settings.get("subject_line", ""),
            preview_text=settings.get("preview_text", ""),
            emails_sent=item.get("emails_sent", 0),
            send_time=(
                datetime.fromisoformat(item["send_time"].replace("Z", "+00:00")) if item.get("send_time") else None
            ),
            create_time=datetime.fromisoformat(item["create_time"].replace("Z", "+00:00")),
            report_summary=item.get("report_summary", {}),
        )


# ==================== Helix Spirals Node Integration ====================


class MailchimpNode:
    """
    Helix Spirals node for Mailchimp integration.

    Supports operations:
    - add_subscriber: Add a new subscriber
    - update_subscriber: Update subscriber info
    - remove_subscriber: Remove a subscriber
    - send_campaign: Send an email campaign
    - get_campaign_stats: Get campaign statistics
    - add_tags: Add tags to subscriber
    - create_segment: Create audience segment
    """

    def __init__(self, config: MailchimpConfig):
        self.connector = MailchimpConnector(config)

    async def execute(self, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        """Execute a Mailchimp operation."""
        operations = {
            "add_subscriber": self._add_subscriber,
            "update_subscriber": self._update_subscriber,
            "remove_subscriber": self._remove_subscriber,
            "send_campaign": self._send_campaign,
            "get_campaign_stats": self._get_campaign_stats,
            "add_tags": self._add_tags,
            "create_segment": self._create_segment,
            "get_audiences": self._get_audiences,
            "batch_subscribe": self._batch_subscribe,
        }

        if operation not in operations:
            raise ValueError(f"Unknown operation: {operation}")

        return await operations[operation](params)

    async def _add_subscriber(self, params: dict[str, Any]) -> dict[str, Any]:
        """Add a subscriber to an audience."""
        member = await self.connector.add_member(
            list_id=params["list_id"],
            email=params["email"],
            status=MailchimpMemberStatus(params.get("status", "subscribed")),
            merge_fields=params.get("merge_fields"),
            tags=params.get("tags"),
        )
        return {
            "success": True,
            "member_id": member.id,
            "email": member.email_address,
            "status": member.status.value,
        }

    async def _update_subscriber(self, params: dict[str, Any]) -> dict[str, Any]:
        """Update a subscriber."""
        member = await self.connector.update_member(
            list_id=params["list_id"],
            email=params["email"],
            status=(MailchimpMemberStatus(params["status"]) if params.get("status") else None),
            merge_fields=params.get("merge_fields"),
        )
        return {
            "success": True,
            "member_id": member.id,
            "email": member.email_address,
            "status": member.status.value,
        }

    async def _remove_subscriber(self, params: dict[str, Any]) -> dict[str, Any]:
        """Remove a subscriber."""
        await self.connector.delete_member(list_id=params["list_id"], email=params["email"])
        return {"success": True, "email": params["email"]}

    async def _send_campaign(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create and send a campaign."""
        campaign = await self.connector.create_campaign(
            campaign_type=MailchimpCampaignType(params.get("type", "regular")),
            list_id=params["list_id"],
            subject_line=params["subject"],
            from_name=params["from_name"],
            reply_to=params["reply_to"],
            title=params.get("title"),
            preview_text=params.get("preview_text"),
        )

        await self.connector.set_campaign_content(
            campaign_id=campaign.id,
            html=params.get("html"),
            plain_text=params.get("plain_text"),
        )

        if params.get("schedule_time"):
            await self.connector.schedule_campaign(
                campaign_id=campaign.id,
                schedule_time=datetime.fromisoformat(params["schedule_time"]),
            )
            return {"success": True, "campaign_id": campaign.id, "status": "scheduled"}
        else:
            await self.connector.send_campaign(campaign.id)
            return {"success": True, "campaign_id": campaign.id, "status": "sent"}

    async def _get_campaign_stats(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get campaign statistics."""
        report = await self.connector.get_campaign_report(params["campaign_id"])
        return {
            "success": True,
            "campaign_id": params["campaign_id"],
            "emails_sent": report.get("emails_sent", 0),
            "opens": report.get("opens", {}),
            "clicks": report.get("clicks", {}),
            "bounces": report.get("bounces", {}),
            "unsubscribes": report.get("unsubscribes", 0),
        }

    async def _add_tags(self, params: dict[str, Any]) -> dict[str, Any]:
        """Add tags to a subscriber."""
        tags = [{"name": tag, "status": "active"} for tag in params["tags"]]
        await self.connector.update_member_tags(list_id=params["list_id"], email=params["email"], tags=tags)
        return {"success": True, "email": params["email"], "tags_added": params["tags"]}

    async def _create_segment(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create an audience segment."""
        segment = await self.connector.create_segment(
            list_id=params["list_id"],
            name=params["name"],
            conditions=params["conditions"],
            match=params.get("match", "all"),
        )
        return {"success": True, "segment_id": segment["id"], "name": segment["name"]}

    async def _get_audiences(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get all audiences."""
        audiences = await self.connector.get_audiences()
        return {
            "success": True,
            "audiences": [{"id": a.id, "name": a.name, "member_count": a.member_count} for a in audiences],
        }

    async def _batch_subscribe(self, params: dict[str, Any]) -> dict[str, Any]:
        """Batch subscribe multiple members."""
        result = await self.connector.batch_subscribe(
            list_id=params["list_id"],
            members=params["members"],
            update_existing=params.get("update_existing", True),
        )
        return {
            "success": True,
            "new_members": result.get("new_members", 0),
            "updated_members": result.get("updated_members", 0),
            "errors": result.get("errors", []),
        }

    async def close(self):
        """Close the connector."""
        await self.connector.close()
