"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

🌀 Helix Spirals - Slack Integration
Complete Slack API connector with all essential actions
"""

import json
import logging
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class SlackError(Exception):
    """Slack API error"""


class SlackConnector:
    """
    Comprehensive Slack API integration for Helix Spirals.

    Supports:
    - Send messages (text, blocks, attachments)
    - Upload files
    - Create channels
    - Manage users
    - React to messages
    - Update messages
    - Thread replies
    - User lookup
    - Channel management
    """

    BASE_URL = "https://slack.com/api"

    def __init__(self, bot_token: str, user_token: str | None = None):
        """
        Initialize Slack connector.

        Args:
            bot_token: Bot User OAuth Token (xoxb-...)
            user_token: User OAuth Token (xoxp-...) for user-scoped actions
        """
        self.bot_token = bot_token
        self.user_token = user_token
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def _request(
        self,
        method: str,
        endpoint: str,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        use_user_token: bool = False,
    ) -> dict[str, Any]:
        """
        Make request to Slack API.

        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint (e.g., "chat.postMessage")
            data: Request payload
            files: Files to upload
            use_user_token: Use user token instead of bot token

        Returns:
            API response dict

        Raises:
            SlackError: If API returns error
        """
        if not self.session:
            self.session = aiohttp.ClientSession()

        token = self.user_token if use_user_token else self.bot_token
        if not token:
            raise SlackError("No Slack token provided")

        url = f"{self.BASE_URL}/{endpoint}"
        headers = {"Authorization": f"Bearer {token}"}

        try:
            if files:
                # Multipart form data for file uploads
                form_data = aiohttp.FormData()
                if data:
                    for key, value in data.items():
                        if isinstance(value, (dict, list)):
                            form_data.add_field(key, json.dumps(value))
                        else:
                            form_data.add_field(key, str(value))
                for key, file_data in files.items():
                    form_data.add_field(key, file_data)

                async with self.session.post(url, data=form_data, headers=headers) as response:
                    result = await response.json()
            else:
                # JSON payload for regular API calls
                if method.upper() == "GET":
                    async with self.session.get(url, params=data or {}, headers=headers) as response:
                        result = await response.json()
                else:
                    headers["Content-Type"] = "application/json"
                    async with self.session.post(url, json=data or {}, headers=headers) as response:
                        result = await response.json()

            if not result.get("ok"):
                error = result.get("error", "Unknown error")
                raise SlackError(f"Slack API error: {error}")

            return result

        except aiohttp.ClientError as e:
            logger.error("Slack API request failed: %s", e)
            raise SlackError(f"Network error: {e}")

    # ============================================================================
    # MESSAGING ACTIONS
    # ============================================================================

    async def send_message(
        self,
        channel: str,
        text: str | None = None,
        blocks: list[dict[str, Any]] | None = None,
        attachments: list[dict[str, Any]] | None = None,
        thread_ts: str | None = None,
        reply_broadcast: bool = False,
        unfurl_links: bool = True,
        unfurl_media: bool = True,
        icon_emoji: str | None = None,
        icon_url: str | None = None,
        username: str | None = None,
        mrkdwn: bool = True,
    ) -> dict[str, Any]:
        """
        Send a message to a Slack channel or user.

        Args:
            channel: Channel ID (C123...) or user ID (U123...) or channel name (#general)
            text: Message text (required if no blocks)
            blocks: Block Kit blocks for rich formatting
            attachments: Legacy attachments
            thread_ts: Parent message timestamp to reply in thread
            reply_broadcast: Broadcast thread reply to channel
            unfurl_links: Automatically unfurl links
            unfurl_media: Automatically unfurl media
            icon_emoji: Bot icon emoji (e.g., ":robot_face:")
            icon_url: Bot icon URL
            username: Bot display name
            mrkdwn: Enable markdown formatting

        Returns:
            dict with 'ok', 'channel', 'ts', 'message' keys

        Example:
            await slack.send_message(
                channel="#general",
                text="Hello from Helix! :wave:",
                blocks=[{
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Bold text* and _italic_"}
                }]
            )
        """
        if not text and not blocks:
            raise ValueError("Either text or blocks must be provided")

        data = {
            "channel": channel,
            "unfurl_links": unfurl_links,
            "unfurl_media": unfurl_media,
            "mrkdwn": mrkdwn,
        }

        if text:
            data["text"] = text
        if blocks:
            data["blocks"] = blocks
        if attachments:
            data["attachments"] = attachments
        if thread_ts:
            data["thread_ts"] = thread_ts
            data["reply_broadcast"] = reply_broadcast
        if icon_emoji:
            data["icon_emoji"] = icon_emoji
        if icon_url:
            data["icon_url"] = icon_url
        if username:
            data["username"] = username

        return await self._request("POST", "chat.postMessage", data)

    async def update_message(
        self,
        channel: str,
        ts: str,
        text: str | None = None,
        blocks: list[dict[str, Any]] | None = None,
        attachments: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """
        Update an existing message.

        Args:
            channel: Channel ID
            ts: Message timestamp to update
            text: New message text
            blocks: New Block Kit blocks
            attachments: New attachments

        Returns:
            dict with updated message info
        """
        data = {"channel": channel, "ts": ts}

        if text:
            data["text"] = text
        if blocks:
            data["blocks"] = blocks
        if attachments:
            data["attachments"] = attachments

        return await self._request("POST", "chat.update", data)

    async def delete_message(self, channel: str, ts: str) -> dict[str, Any]:
        """
        Delete a message.

        Args:
            channel: Channel ID
            ts: Message timestamp to delete

        Returns:
            dict with deletion confirmation
        """
        return await self._request("POST", "chat.delete", {"channel": channel, "ts": ts})

    async def add_reaction(self, channel: str, ts: str, emoji: str) -> dict[str, Any]:
        """
        Add emoji reaction to a message.

        Args:
            channel: Channel ID
            ts: Message timestamp
            emoji: Emoji name (without colons, e.g., "thumbsup")

        Returns:
            dict with reaction confirmation
        """
        return await self._request("POST", "reactions.add", {"channel": channel, "timestamp": ts, "name": emoji})

    async def remove_reaction(self, channel: str, ts: str, emoji: str) -> dict[str, Any]:
        """Remove emoji reaction from a message."""
        return await self._request(
            "POST",
            "reactions.remove",
            {"channel": channel, "timestamp": ts, "name": emoji},
        )

    # ============================================================================
    # CHANNEL ACTIONS
    # ============================================================================

    async def create_channel(self, name: str, is_private: bool = False) -> dict[str, Any]:
        """
        Create a new Slack channel.

        Args:
            name: Channel name (lowercase, no spaces)
            is_private: Create private channel vs public

        Returns:
            dict with 'ok', 'channel' keys
        """
        endpoint = "conversations.create"
        data = {"name": name.lower().replace(" ", "-"), "is_private": is_private}

        return await self._request("POST", endpoint, data)

    async def archive_channel(self, channel: str) -> dict[str, Any]:
        """Archive a channel."""
        return await self._request("POST", "conversations.archive", {"channel": channel})

    async def unarchive_channel(self, channel: str) -> dict[str, Any]:
        """Unarchive a channel."""
        return await self._request("POST", "conversations.unarchive", {"channel": channel})

    async def rename_channel(self, channel: str, name: str) -> dict[str, Any]:
        """Rename a channel."""
        return await self._request("POST", "conversations.rename", {"channel": channel, "name": name})

    async def set_channel_topic(self, channel: str, topic: str) -> dict[str, Any]:
        """Set channel topic."""
        return await self._request("POST", "conversations.setTopic", {"channel": channel, "topic": topic})

    async def set_channel_purpose(self, channel: str, purpose: str) -> dict[str, Any]:
        """Set channel purpose."""
        return await self._request("POST", "conversations.setPurpose", {"channel": channel, "purpose": purpose})

    async def invite_to_channel(self, channel: str, users: str | list[str]) -> dict[str, Any]:
        """
        Invite users to a channel.

        Args:
            channel: Channel ID
            users: User ID or list of user IDs

        Returns:
            dict with invitation confirmation
        """
        if isinstance(users, list):
            users = ",".join(users)

        return await self._request("POST", "conversations.invite", {"channel": channel, "users": users})

    async def kick_from_channel(self, channel: str, user: str) -> dict[str, Any]:
        """Remove user from channel."""
        return await self._request("POST", "conversations.kick", {"channel": channel, "user": user})

    async def list_channels(
        self,
        exclude_archived: bool = True,
        types: str = "public_channel",
        limit: int = 100,
    ) -> dict[str, Any]:
        """
        List channels.

        Args:
            exclude_archived: Exclude archived channels
            types: Channel types (public_channel, private_channel, mpim, im)
            limit: Max channels to return

        Returns:
            dict with 'channels' list
        """
        return await self._request(
            "GET",
            "conversations.list",
            {
                "exclude_archived": exclude_archived,
                "types": types,
                "limit": limit,
            },
        )

    # ============================================================================
    # USER ACTIONS
    # ============================================================================

    async def get_user_info(self, user: str) -> dict[str, Any]:
        """
        Get user information.

        Args:
            user: User ID (U123...)

        Returns:
            dict with 'user' key containing user profile
        """
        return await self._request("GET", "users.info", {"user": user})

    async def lookup_user_by_email(self, email: str) -> dict[str, Any]:
        """
        Find user by email address.

        Args:
            email: User's email

        Returns:
            dict with 'user' key
        """
        return await self._request("GET", "users.lookupByEmail", {"email": email})

    async def list_users(self, limit: int = 100) -> dict[str, Any]:
        """
        List workspace users.

        Returns:
            dict with 'members' list
        """
        return await self._request("GET", "users.list", {"limit": limit})

    async def set_user_status(
        self,
        status_text: str,
        status_emoji: str = ":speech_balloon:",
        status_expiration: int | None = None,
    ) -> dict[str, Any]:
        """
        Set user status (requires user token).

        Args:
            status_text: Status text
            status_emoji: Status emoji
            status_expiration: Unix timestamp when status expires

        Returns:
            dict with status confirmation
        """
        profile = {"status_text": status_text, "status_emoji": status_emoji}

        if status_expiration:
            profile["status_expiration"] = status_expiration

        return await self._request(
            "POST",
            "users.profile.set",
            {"profile": profile},
            use_user_token=True,
        )

    # ============================================================================
    # FILE ACTIONS
    # ============================================================================

    async def upload_file(
        self,
        channels: str | list[str],
        file_content: bytes,
        filename: str,
        title: str | None = None,
        initial_comment: str | None = None,
        filetype: str | None = None,
        thread_ts: str | None = None,
    ) -> dict[str, Any]:
        """
        Upload a file to Slack.

        Args:
            channels: Channel IDs (comma-separated or list)
            file_content: File bytes
            filename: File name
            title: File title
            initial_comment: Message to post with file
            filetype: File type (e.g., "python", "json")
            thread_ts: Thread to post file in

        Returns:
            dict with 'file' key containing file info

        Example:
            with open("report.pdf", "rb") as f:
                await slack.upload_file(
                    channels="#reports",
                    file_content=f.read(),
                    filename="monthly_report.pdf",
                    title="Monthly Report - January 2025",
                    initial_comment="Here's the latest report!"
                )
        """
        if isinstance(channels, list):
            channels = ",".join(channels)

        data = {"channels": channels, "filename": filename}

        if title:
            data["title"] = title
        if initial_comment:
            data["initial_comment"] = initial_comment
        if filetype:
            data["filetype"] = filetype
        if thread_ts:
            data["thread_ts"] = thread_ts

        files = {"file": file_content}

        return await self._request("POST", "files.upload", data, files)

    async def delete_file(self, file_id: str) -> dict[str, Any]:
        """Delete a file."""
        return await self._request("POST", "files.delete", {"file": file_id})

    # ============================================================================
    # UTILITY METHODS
    # ============================================================================

    async def test_auth(self) -> dict[str, Any]:
        """
        Test authentication and get workspace info.

        Returns:
            dict with 'ok', 'url', 'team', 'user', 'bot_id' keys
        """
        return await self._request("POST", "auth.test", {})

    async def get_permalink(self, channel: str, message_ts: str) -> str:
        """
        Get permanent link to a message.

        Args:
            channel: Channel ID
            message_ts: Message timestamp

        Returns:
            Permalink URL
        """
        result = await self._request(
            "GET",
            "chat.getPermalink",
            {"channel": channel, "message_ts": message_ts},
        )
        return result.get("permalink", "")

    def create_blocks(
        self, sections: list[str], buttons: list[dict[str, str]] | None = None
    ) -> list[dict[str, Any]]:
        """
        Helper to create Block Kit blocks.

        Args:
            sections: List of text sections
            buttons: List of button dicts {"text": "Click Me", "value": "action_id"}

        Returns:
            List of Block Kit blocks

        Example:
            blocks = slack.create_blocks(
                sections=["*Welcome!*", "This is a test"],
                buttons=[{"text": "Approve", "value": "approve"}]
            )
        """
        blocks = []

        for section in sections:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": section}})

        if buttons:
            elements = []
            for btn in buttons:
                elements.append(
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": btn["text"]},
                        "value": btn.get("value", btn["text"]),
                        "action_id": btn.get("action_id", btn["text"].lower()),
                    }
                )

            blocks.append({"type": "actions", "elements": elements})

        return blocks


# ============================================================================
# HELPER FUNCTIONS FOR HELIX SPIRALS NODES
# ============================================================================


async def send_slack_message(
    bot_token: str,
    channel: str,
    text: str,
    **kwargs,
) -> dict[str, Any]:
    """
    Standalone function to send Slack message (for use in Spiral nodes).

    Args:
        bot_token: Slack bot token
        channel: Channel to send to
        text: Message text
        **kwargs: Additional arguments (blocks, attachments, etc.)

    Returns:
        Slack API response

    Example:
        result = await send_slack_message(
            bot_token="xoxb-...",
            channel="#general",
            text="Automation triggered!",
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": "*Alert*"}}]
        )
    """
    async with SlackConnector(bot_token) as slack:
        return await slack.send_message(channel=channel, text=text, **kwargs)


async def upload_slack_file(
    bot_token: str,
    channels: str,
    file_content: bytes,
    filename: str,
    **kwargs,
) -> dict[str, Any]:
    """
    Standalone function to upload file to Slack (for use in Spiral nodes).
    """
    async with SlackConnector(bot_token) as slack:
        return await slack.upload_file(channels=channels, file_content=file_content, filename=filename, **kwargs)


async def create_slack_channel(bot_token: str, name: str, is_private: bool = False) -> dict[str, Any]:
    """
    Standalone function to create Slack channel (for use in Spiral nodes).
    """
    async with SlackConnector(bot_token) as slack:
        return await slack.create_channel(name=name, is_private=is_private)
