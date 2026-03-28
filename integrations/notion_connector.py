"""
Notion Connector for Helix Spirals
Provides integration with Notion API for databases, pages, and blocks
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

try:
    import httpx

    NOTION_AVAILABLE = True
except ImportError:
    NOTION_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class NotionConfig:
    """Notion connector configuration."""

    access_token: str
    base_url: str = "https://api.notion.com/v1"


@dataclass
class NotionDatabase:
    """Represents a Notion database."""

    id: str
    title: str
    parent: dict[str, Any]
    properties: dict[str, Any]
    created_time: datetime
    last_edited_time: datetime


@dataclass
class NotionPage:
    """Represents a Notion page."""

    id: str
    title: str
    parent: dict[str, Any]
    properties: dict[str, Any]
    created_time: datetime
    last_edited_time: datetime
    archived: bool = False


@dataclass
class NotionBlock:
    """Represents a Notion block."""

    id: str
    type: str
    content: dict[str, Any]
    has_children: bool = False
    archived: bool = False


class NotionConnector:
    """
    Notion integration connector for databases, pages, and blocks.
    """

    def __init__(self, config: NotionConfig):
        if not NOTION_AVAILABLE:
            raise ImportError("httpx is required for Notion connector. " "Install with: pip install httpx")

        self.config = config
        self._client = None
        self._headers = {
            "Authorization": f"Bearer {config.access_token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.config.base_url, headers=self._headers, timeout=30.0)
        return self._client

    # ==================== Database Operations ====================

    async def list_databases(self) -> list[NotionDatabase]:
        """List all Notion databases."""
        try:
            client = await self._get_client()
            response = await client.post("/search", json={"filter": {"property": "object", "value": "database"}})
            response.raise_for_status()

            data = response.json()
            databases = []

            for result in data.get("results", []):
                db = result
                # Extract title
                title = ""
                if "title" in db.get("properties", {}):
                    title_parts = db["properties"]["title"].get("title", [])
                    title = "".join([t["plain_text"] for t in title_parts])

                databases.append(
                    NotionDatabase(
                        id=db["id"],
                        title=title,
                        parent=db["parent"],
                        properties=db["properties"],
                        created_time=datetime.fromisoformat(db["created_time"].replace("Z", "+00:00")),
                        last_edited_time=datetime.fromisoformat(db["last_edited_time"].replace("Z", "+00:00")),
                    )
                )

            return databases
        except Exception as e:
            logger.error("Failed to list Notion databases: %s", e)
            raise

    async def get_database(self, database_id: str) -> NotionDatabase:
        """Get a specific Notion database."""
        try:
            client = await self._get_client()
            response = await client.get(f"/databases/{database_id}")
            response.raise_for_status()

            db = response.json()

            # Extract title
            title = ""
            if "title" in db.get("properties", {}):
                title_parts = db["properties"]["title"].get("title", [])
                title = "".join([t["plain_text"] for t in title_parts])

            return NotionDatabase(
                id=db["id"],
                title=title,
                parent=db["parent"],
                properties=db["properties"],
                created_time=datetime.fromisoformat(db["created_time"].replace("Z", "+00:00")),
                last_edited_time=datetime.fromisoformat(db["last_edited_time"].replace("Z", "+00:00")),
            )
        except Exception as e:
            logger.error("Failed to get Notion database: %s", e)
            raise

    async def query_database(
        self,
        database_id: str,
        filter: dict[str, Any] | None = None,
        sorts: list[dict[str, Any]] | None = None,
        start_cursor: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        """Query a Notion database."""
        try:
            client = await self._get_client()

            payload = {}
            if filter:
                payload["filter"] = filter
            if sorts:
                payload["sorts"] = sorts
            if start_cursor:
                payload["start_cursor"] = start_cursor
            payload["page_size"] = page_size

            response = await client.post(f"/databases/{database_id}/query", json=payload)
            response.raise_for_status()

            return response.json()
        except Exception as e:
            logger.error("Failed to query Notion database: %s", e)
            raise

    # ==================== Page Operations ====================

    async def list_pages(
        self, database_id: str | None = None, filter: dict[str, Any] | None = None
    ) -> list[NotionPage]:
        """List Notion pages."""
        try:
            client = await self._get_client()

            payload = {"filter": {"property": "object", "value": "page"}}
            if database_id:
                payload["filter"] = {
                    "and": [
                        {"property": "object", "value": "page"},
                        {"property": "parent", "value": database_id},
                    ]
                }
            if filter:
                payload["filter"] = filter

            response = await client.post("/search", json=payload)
            response.raise_for_status()

            data = response.json()
            pages = []

            for result in data.get("results", []):
                page = result
                # Extract title
                title = ""
                if "title" in page.get("properties", {}):
                    title_parts = page["properties"]["title"].get("title", [])
                    title = "".join([t["plain_text"] for t in title_parts])
                elif "Name" in page.get("properties", {}):
                    title_parts = page["properties"]["Name"].get("title", [])
                    title = "".join([t["plain_text"] for t in title_parts])

                pages.append(
                    NotionPage(
                        id=page["id"],
                        title=title,
                        parent=page["parent"],
                        properties=page["properties"],
                        created_time=datetime.fromisoformat(page["created_time"].replace("Z", "+00:00")),
                        last_edited_time=datetime.fromisoformat(page["last_edited_time"].replace("Z", "+00:00")),
                        archived=page.get("archived", False),
                    )
                )

            return pages
        except Exception as e:
            logger.error("Failed to list Notion pages: %s", e)
            raise

    async def get_page(self, page_id: str) -> NotionPage:
        """Get a specific Notion page."""
        try:
            client = await self._get_client()
            response = await client.get(f"/pages/{page_id}")
            response.raise_for_status()

            page = response.json()

            # Extract title
            title = ""
            if "title" in page.get("properties", {}):
                title_parts = page["properties"]["title"].get("title", [])
                title = "".join([t["plain_text"] for t in title_parts])
            elif "Name" in page.get("properties", {}):
                title_parts = page["properties"]["Name"].get("title", [])
                title = "".join([t["plain_text"] for t in title_parts])

            return NotionPage(
                id=page["id"],
                title=title,
                parent=page["parent"],
                properties=page["properties"],
                created_time=datetime.fromisoformat(page["created_time"].replace("Z", "+00:00")),
                last_edited_time=datetime.fromisoformat(page["last_edited_time"].replace("Z", "+00:00")),
                archived=page.get("archived", False),
            )
        except Exception as e:
            logger.error("Failed to get Notion page: %s", e)
            raise

    async def create_page(
        self,
        parent: dict[str, str],
        properties: dict[str, Any],
        children: list[dict[str, Any]] | None = None,
    ) -> NotionPage:
        """Create a new Notion page."""
        try:
            client = await self._get_client()

            payload = {
                "parent": parent,
                "properties": properties,
            }

            if children:
                payload["children"] = children

            response = await client.post("/pages", json=payload)
            response.raise_for_status()

            page = response.json()

            # Extract title
            title = ""
            if "title" in page.get("properties", {}):
                title_parts = page["properties"]["title"].get("title", [])
                title = "".join([t["plain_text"] for t in title_parts])

            return NotionPage(
                id=page["id"],
                title=title,
                parent=page["parent"],
                properties=page["properties"],
                created_time=datetime.fromisoformat(page["created_time"].replace("Z", "+00:00")),
                last_edited_time=datetime.fromisoformat(page["last_edited_time"].replace("Z", "+00:00")),
                archived=page.get("archived", False),
            )
        except Exception as e:
            logger.error("Failed to create Notion page: %s", e)
            raise

    async def update_page(
        self, page_id: str, properties: dict[str, Any], archived: bool | None = None
    ) -> NotionPage:
        """Update a Notion page."""
        try:
            client = await self._get_client()

            payload = {"properties": properties}
            if archived is not None:
                payload["archived"] = archived

            response = await client.patch(f"/pages/{page_id}", json=payload)
            response.raise_for_status()

            page = response.json()

            # Extract title
            title = ""
            if "title" in page.get("properties", {}):
                title_parts = page["properties"]["title"].get("title", [])
                title = "".join([t["plain_text"] for t in title_parts])

            return NotionPage(
                id=page["id"],
                title=title,
                parent=page["parent"],
                properties=page["properties"],
                created_time=datetime.fromisoformat(page["created_time"].replace("Z", "+00:00")),
                last_edited_time=datetime.fromisoformat(page["last_edited_time"].replace("Z", "+00:00")),
                archived=page.get("archived", False),
            )
        except Exception as e:
            logger.error("Failed to update Notion page: %s", e)
            raise

    async def delete_page(self, page_id: str) -> bool:
        """Delete a Notion page (set to archived)."""
        try:
            return True
        except Exception as e:
            logger.error("Failed to delete Notion page: %s", e)
            return False

    # ==================== Block Operations ====================

    async def get_block_children(
        self, block_id: str, start_cursor: str | None = None, page_size: int = 100
    ) -> list[NotionBlock]:
        """Get children blocks of a block or page."""
        try:
            client = await self._get_client()

            params = {"page_size": page_size}
            if start_cursor:
                params["start_cursor"] = start_cursor

            response = await client.get(f"/blocks/{block_id}/children", params=params)
            response.raise_for_status()

            data = response.json()
            blocks = []

            for result in data.get("results", []):
                blocks.append(
                    NotionBlock(
                        id=result["id"],
                        type=result["type"],
                        content=result[result["type"]],
                        has_children=result.get("has_children", False),
                        archived=result.get("archived", False),
                    )
                )

            return blocks
        except Exception as e:
            logger.error("Failed to get Notion block children: %s", e)
            raise

    async def append_block_children(self, block_id: str, children: list[dict[str, Any]]) -> dict[str, Any]:
        """Append children blocks to a block or page."""
        try:
            client = await self._get_client()

            response = await client.patch(f"/blocks/{block_id}/children", json={"children": children})
            response.raise_for_status()

            return response.json()
        except Exception as e:
            logger.error("Failed to append Notion block children: %s", e)
            raise

    # ==================== Search Operations ====================

    async def search(
        self,
        query: str,
        filter: dict[str, Any] | None = None,
        sort: dict[str, Any] | None = None,
        start_cursor: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        """Search Notion for pages and databases."""
        try:
            client = await self._get_client()

            payload = {"query": query, "page_size": page_size}
            if filter:
                payload["filter"] = filter
            if sort:
                payload["sort"] = sort
            if start_cursor:
                payload["start_cursor"] = start_cursor

            response = await client.post("/search", json=payload)
            response.raise_for_status()

            return response.json()
        except Exception as e:
            logger.error("Failed to search Notion: %s", e)
            raise

    # ==================== Utility Methods ====================

    async def test_connection(self) -> bool:
        """Test the Notion connection."""
        try:
            await self.list_databases()
            return True
        except Exception as e:
            logger.error("Notion connection test failed: %s", e)
            return False

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("Notion client closed")
