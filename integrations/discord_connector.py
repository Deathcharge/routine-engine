"""
Discord Connector for Helix Spirals
Provides integration with Discord Bot API
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

try:
    import discord

    DISCORD_AVAILABLE = True
except ImportError:
    DISCORD_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class DiscordConfig:
    """Discord connector configuration."""

    bot_token: str
    client_id: str
    client_secret: str
    redirect_uri: str | None = None
    intents: list[str] = None


@dataclass
class DiscordMessage:
    """Represents a Discord message."""

    id: str
    channel_id: str
    author_id: str
    author_name: str
    content: str
    timestamp: datetime
    guild_id: str | None = None
    attachments: list[str] = None


@dataclass
class DiscordChannel:
    """Represents a Discord channel."""

    id: str
    name: str
    type: str
    guild_id: str | None = None
    position: int = 0


@dataclass
class DiscordGuild:
    """Represents a Discord server/guild."""

    id: str
    name: str
    owner_id: str
    member_count: int
    icon: str | None = None


class DiscordConnector:
    """
    Discord integration connector for bot API interactions.
    """

    def __init__(self, config: DiscordConfig):
        if not DISCORD_AVAILABLE:
            raise ImportError("discord.py is required for Discord connector. " "Install with: pip install discord.py")

        self.config = config
        self._bot = None
        self._client = None
        self._initialized = False

    async def _initialize(self):
        """Initialize Discord bot client."""
        if self._initialized:
            return

        # Configure intents
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True

        if self.config.intents:
            for intent in self.config.intents:
                setattr(intents, intent, True)

        self._client = discord.Client(intents=intents)
        self._initialized = True

        logger.info("✅ Discord client initialized")

    # ==================== Message Operations ====================

    async def send_message(
        self,
        channel_id: str,
        content: str,
        embed: dict[str, Any] | None = None,
        components: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """
        Send a message to a Discord channel.

        Args:
            channel_id: Discord channel ID
            content: Message content
            embed: Discord embed data
            components: Message components (buttons, select menus, etc.)

        Returns:
            Message data
        """
        await self._initialize()

        try:
            channel = self._client.get_channel(int(channel_id))
            if not channel:
                raise ValueError(f"Channel {channel_id} not found")

            message = await channel.send(
                content=content,
                embed=discord.Embed.from_dict(embed) if embed else None,
                view=self._create_view(components) if components else None,
            )

            return {
                "id": str(message.id),
                "channel_id": str(message.channel_id),
                "content": message.content,
                "timestamp": message.created_at.isoformat(),
            }
        except Exception as e:
            logger.error("Failed to send Discord message: %s", e)
            raise

    async def edit_message(
        self,
        channel_id: str,
        message_id: str,
        content: str | None = None,
        embed: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Edit an existing Discord message."""
        await self._initialize()

        try:
            channel = self._client.get_channel(int(channel_id))
            if not channel:
                raise ValueError(f"Channel {channel_id} not found")

            message = await channel.fetch_message(message_id)

            await message.edit(content=content, embed=discord.Embed.from_dict(embed) if embed else None)

            return {"status": "edited", "id": str(message.id)}
        except Exception as e:
            logger.error("Failed to edit Discord message: %s", e)
            raise

    async def delete_message(self, channel_id: str, message_id: str) -> bool:
        """Delete a Discord message."""
        await self._initialize()

        try:
            channel = self._client.get_channel(int(channel_id))
            if not channel:
                return False

            message = await channel.fetch_message(message_id)
            await message.delete()
            return True
        except Exception as e:
            logger.error("Failed to delete Discord message: %s", e)
            return False

    async def get_channel_messages(
        self,
        channel_id: str,
        limit: int = 100,
        before: str | None = None,
        after: str | None = None,
    ) -> list[DiscordMessage]:
        """Get messages from a Discord channel."""
        await self._initialize()

        try:
            messages = []

            kwargs = {"limit": limit}
            if before:
                kwargs["before"] = int(before)
            if after:
                kwargs["after"] = int(after)

            if after:
                kwargs["after"] = int(after)

            channel = self._client.get_channel(int(channel_id))
            if not channel:
                raise ValueError(f"Channel {channel_id} not found")

            async for message in channel.history(**kwargs):
                messages.append(
                    DiscordMessage(
                        id=str(message.id),
                        channel_id=str(message.channel.id),
                        author_id=str(message.author.id),
                        author_name=message.author.name,
                        content=message.content,
                        timestamp=message.created_at,
                        guild_id=str(message.guild.id) if message.guild else None,
                        attachments=[att.url for att in message.attachments],
                    )
                )

            return messages
        except Exception as e:
            logger.error("Failed to get Discord messages: %s", e)
            raise

    # ==================== Channel Operations ====================

    async def list_channels(self, guild_id: str) -> list[DiscordChannel]:
        """List all channels in a Discord server."""
        await self._initialize()

        try:
            guild = self._client.get_guild(int(guild_id))
            if not guild:
                raise ValueError(f"Guild {guild_id} not found")

            channels = []

            for channel in guild.channels:
                channels.append(
                    DiscordChannel(
                        id=str(channel.id),
                        name=channel.name,
                        type=str(channel.type),
                        guild_id=str(guild_id),
                        position=channel.position,
                    )
                )

            return channels
        except Exception as e:
            logger.error("Failed to list Discord channels: %s", e)
            raise

    async def create_channel(
        self,
        guild_id: str,
        name: str,
        channel_type: str = "text",
        category: str | None = None,
        permissions: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a new Discord channel."""
        await self._initialize()

        try:
            guild = self._client.get_guild(int(guild_id))
            if not guild:
                raise ValueError(f"Guild {guild_id} not found")

            kwargs = {"name": name}
            if channel_type == "voice":
                channel = await guild.create_voice_channel(**kwargs)
            elif channel_type == "category":
                channel = await guild.create_category(**kwargs)
            else:
                channel = await guild.create_text_channel(**kwargs)

            if category:
                category_obj = discord.utils.get(guild.categories, name=category)
                if category_obj:
                    await channel.edit(category=category_obj)

            return {
                "id": str(channel.id),
                "name": channel.name,
                "type": str(channel.type),
                "status": "created",
            }
        except Exception as e:
            logger.error("Failed to create Discord channel: %s", e)
            raise

    async def delete_channel(self, channel_id: str) -> bool:
        """Delete a Discord channel."""
        await self._initialize()

        try:
            channel = self._client.get_channel(int(channel_id))
            if not channel:
                return False

            await channel.delete()
            return True
        except Exception as e:
            logger.error("Failed to delete Discord channel: %s", e)
            return False

    # ==================== Guild Operations ====================

    async def list_guilds(self) -> list[DiscordGuild]:
        """List all Discord servers the bot is in."""
        await self._initialize()

        try:
            guilds = []
            for guild in self._client.guilds:
                guilds.append(
                    DiscordGuild(
                        id=str(guild.id),
                        name=guild.name,
                        owner_id=str(guild.owner_id),
                        member_count=guild.member_count,
                        icon=guild.icon,
                    )
                )
            return guilds
        except Exception as e:
            logger.error("Failed to list Discord guilds: %s", e)
            raise

    async def get_guild_info(self, guild_id: str) -> DiscordGuild | None:
        """Get information about a specific Discord server."""
        await self._initialize()

        try:
            guild = self._client.get_guild(int(guild_id))
            if not guild:
                return None

            return DiscordGuild(
                id=str(guild.id),
                name=guild.name,
                owner_id=str(guild.owner_id),
                member_count=guild.member_count,
                icon=guild.icon,
            )
        except Exception as e:
            logger.error("Failed to get Discord guild info: %s", e)
            return None

    # ==================== Member Operations ====================

    async def get_member(self, guild_id: str, member_id: str) -> dict[str, Any] | None:
        """Get information about a guild member."""
        await self._initialize()

        try:
            guild = self._client.get_guild(int(guild_id))
            if not guild:
                return None

            member = await guild.fetch_member(member_id)

            return {
                "id": str(member.id),
                "name": member.name,
                "display_name": member.display_name,
                "discriminator": member.discriminator,
                "avatar": member.avatar,
                "roles": [str(role.id) for role in member.roles],
                "joined_at": member.joined_at.isoformat() if member.joined_at else None,
            }
        except Exception as e:
            logger.error("Failed to get Discord member: %s", e)
            return None

    async def list_members(self, guild_id: str, limit: int = 1000) -> list[dict[str, Any]]:
        """List members of a Discord server."""
        await self._initialize()

        try:
            members = []

            members = []

            guild = self._client.get_guild(int(guild_id))
            if not guild:
                raise ValueError(f"Guild {guild_id} not found")

            async for member in guild.fetch_members(limit=limit):
                members.append(
                    {
                        "id": str(member.id),
                        "name": member.name,
                        "display_name": member.display_name,
                        "discriminator": member.discriminator,
                        "avatar": member.avatar,
                    }
                )

            return members
        except Exception as e:
            logger.error("Failed to list Discord members: %s", e)
            raise

    # ==================== Role Operations ====================

    async def list_roles(self, guild_id: str) -> list[dict[str, Any]]:
        """List all roles in a Discord server."""
        await self._initialize()

        try:
            roles = []

            roles = []

            guild = self._client.get_guild(int(guild_id))
            if not guild:
                raise ValueError(f"Guild {guild_id} not found")

            for role in guild.roles:
                roles.append(
                    {
                        "id": str(role.id),
                        "name": role.name,
                        "color": role.color,
                        "position": role.position,
                        "permissions": role.permissions.value,
                        "mentionable": role.mentionable,
                    }
                )

            return roles
        except Exception as e:
            logger.error("Failed to list Discord roles: %s", e)
            raise

    async def add_role_to_member(self, guild_id: str, member_id: str, role_id: str) -> bool:
        """Add a role to a member."""
        await self._initialize()

        try:
            guild = self._client.get_guild(int(guild_id))
            if not guild:
                return False

            member = await guild.fetch_member(member_id)
            role = guild.get_role(role_id)

            await member.add_roles(role)
            return True
        except Exception as e:
            logger.error("Failed to add role to member: %s", e)
            return False

    # ==================== Utility Methods ====================

    def _create_view(self, components: list[dict[str, Any]]) -> "discord.ui.View | None":
        """Create a Discord UI view from component data."""
        try:

            view = discord.ui.View()

            for comp in components:
                if comp["type"] == "button":
                    button = discord.ui.Button(
                        label=comp.get("label", "Button"),
                        style=getattr(discord.ButtonStyle, comp.get("style", "secondary")),
                        custom_id=comp.get("custom_id"),
                        url=comp.get("url"),
                        emoji=comp.get("emoji"),
                    )
                    view.add_item(button)

            return view
        except Exception as e:
            logger.error("Failed to create Discord view: %s", e)
            return None

    async def test_connection(self) -> bool:
        """Test the Discord connection."""
        try:
            # Try to get guild list as a test
            await self.list_guilds()
            return True
        except Exception as e:
            logger.error("Discord connection test failed: %s", e)
            return False

    async def close(self):
        """Close the Discord client connection."""
        if self._client and self._initialized:
            await self._client.close()
            self._initialized = False
            logger.info("Discord client closed")
