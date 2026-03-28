"""
Airtable Integration Connector for Helix Spirals.

Provides comprehensive Airtable API integration for database operations,
record management, field operations, and automation triggers.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import aiohttp

from apps.backend.core.exceptions import MaxRetriesExceeded

logger = logging.getLogger(__name__)


class AirtableFieldType(Enum):
    """Airtable field types."""

    SINGLE_LINE_TEXT = "singleLineText"
    EMAIL = "email"
    URL = "url"
    MULTILINE_TEXT = "multilineText"
    NUMBER = "number"
    PERCENT = "percent"
    CURRENCY = "currency"
    SINGLE_SELECT = "singleSelect"
    MULTI_SELECT = "multipleSelects"
    SINGLE_COLLABORATOR = "singleCollaborator"
    MULTI_COLLABORATOR = "multipleCollaborators"
    DATE = "date"
    DATE_TIME = "dateTime"
    PHONE_NUMBER = "phoneNumber"
    MULTI_ATTACHMENTS = "multipleAttachments"
    CHECKBOX = "checkbox"
    FORMULA = "formula"
    CREATED_TIME = "createdTime"
    ROLLUP = "rollup"
    COUNT = "count"
    LOOKUP = "lookup"
    MULTI_RECORD_LINKS = "multipleRecordLinks"
    AUTO_NUMBER = "autoNumber"
    BARCODE = "barcode"
    RATING = "rating"
    RICH_TEXT = "richText"
    DURATION = "duration"
    LAST_MODIFIED_TIME = "lastModifiedTime"
    CREATED_BY = "createdBy"
    LAST_MODIFIED_BY = "lastModifiedBy"
    BUTTON = "button"


@dataclass
class AirtableConfig:
    """Configuration for Airtable connector."""

    api_key: str  # Personal access token
    timeout: int = 30
    max_retries: int = 3
    rate_limit_per_second: float = 5  # Airtable allows 5 requests/second

    @property
    def base_url(self) -> str:
        return "https://api.airtable.com/v0"


@dataclass
class AirtableBase:
    """Represents an Airtable base."""

    id: str
    name: str
    permission_level: str


@dataclass
class AirtableTable:
    """Represents an Airtable table."""

    id: str
    name: str
    primary_field_id: str
    fields: list[dict[str, Any]] = field(default_factory=list)
    views: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class AirtableRecord:
    """Represents an Airtable record."""

    id: str
    fields: dict[str, Any]
    created_time: datetime


@dataclass
class AirtableField:
    """Represents an Airtable field."""

    id: str
    name: str
    type: AirtableFieldType
    options: dict[str, Any] | None = None


class AirtableConnector:
    """
    Comprehensive Airtable API connector for Helix Spirals.

    Provides methods for:
    - Base management
    - Table management
    - Record CRUD operations
    - Field management
    - View management
    - Batch operations
    - Formula and filtering
    """

    def __init__(self, config: AirtableConfig):
        self.config = config
        self._session: aiohttp.ClientSession | None = None
        self._last_request_time = 0
        self._request_interval = 1.0 / config.rate_limit_per_second

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

    async def _rate_limit(self):
        """Implement rate limiting."""
        import time

        current_time = time.time()
        time_since_last = current_time - self._last_request_time

        if time_since_last < self._request_interval:
            await asyncio.sleep(self._request_interval - time_since_last)

        self._last_request_time = time.time()

    async def _request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
        params: dict | None = None,
    ) -> dict[str, Any]:
        """Make an API request with retry logic and rate limiting."""
        await self._rate_limit()

        session = await self._get_session()
        url = f"{self.config.base_url}/{endpoint}"

        for attempt in range(self.config.max_retries):
            try:
                async with session.request(method, url, json=data, params=params) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 30))
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

    # ==================== Base Management ====================

    async def list_bases(self) -> list[AirtableBase]:
        """List all accessible bases."""
        response = await self._request("GET", "meta/bases")

        bases = []
        for item in response.get("bases", []):
            bases.append(
                AirtableBase(
                    id=item["id"],
                    name=item["name"],
                    permission_level=item["permissionLevel"],
                )
            )

        return bases

    async def get_base_schema(self, base_id: str) -> dict[str, Any]:
        """Get the schema for a base."""
        return await self._request("GET", f"meta/bases/{base_id}/tables")

    # ==================== Table Management ====================

    async def list_tables(self, base_id: str) -> list[AirtableTable]:
        """List all tables in a base."""
        response = await self._request("GET", f"meta/bases/{base_id}/tables")

        tables = []
        for item in response.get("tables", []):
            tables.append(
                AirtableTable(
                    id=item["id"],
                    name=item["name"],
                    primary_field_id=item["primaryFieldId"],
                    fields=item.get("fields", []),
                    views=item.get("views", []),
                )
            )

        return tables

    async def create_table(
        self,
        base_id: str,
        name: str,
        fields: list[dict[str, Any]],
        description: str | None = None,
    ) -> AirtableTable:
        """Create a new table."""
        data = {"name": name, "fields": fields}

        if description:
            data["description"] = description

        response = await self._request("POST", f"meta/bases/{base_id}/tables", data=data)

        return AirtableTable(
            id=response["id"],
            name=response["name"],
            primary_field_id=response["primaryFieldId"],
            fields=response.get("fields", []),
            views=response.get("views", []),
        )

    async def update_table(
        self,
        base_id: str,
        table_id: str,
        name: str | None = None,
        description: str | None = None,
    ) -> AirtableTable:
        """Update a table's name or description."""
        data = {}
        if name:
            data["name"] = name
        if description:
            data["description"] = description

        response = await self._request("PATCH", f"meta/bases/{base_id}/tables/{table_id}", data=data)

        return AirtableTable(
            id=response["id"],
            name=response["name"],
            primary_field_id=response["primaryFieldId"],
            fields=response.get("fields", []),
            views=response.get("views", []),
        )

    # ==================== Record Operations ====================

    async def list_records(
        self,
        base_id: str,
        table_id_or_name: str,
        fields: list[str] | None = None,
        filter_by_formula: str | None = None,
        max_records: int | None = None,
        page_size: int = 100,
        sort: list[dict[str, str]] | None = None,
        view: str | None = None,
        cell_format: str = "json",
        time_zone: str | None = None,
        user_locale: str | None = None,
        offset: str | None = None,
    ) -> dict[str, Any]:
        """
        List records from a table with optional filtering and sorting.

        Args:
            filter_by_formula: Airtable formula for filtering (e.g., "{Status}='Active'")
            sort: List of sort specifications [{"field": "Name", "direction": "asc"}]
        """
        params = {"pageSize": min(page_size, 100)}

        if fields:
            params["fields[]"] = fields
        if filter_by_formula:
            params["filterByFormula"] = filter_by_formula
        if max_records:
            params["maxRecords"] = max_records
        if sort:
            for i, s in enumerate(sort):
                params[f"sort[{i}][field]"] = s["field"]
                params[f"sort[{i}][direction]"] = s.get("direction", "asc")
        if view:
            params["view"] = view
        if cell_format:
            params["cellFormat"] = cell_format
        if time_zone:
            params["timeZone"] = time_zone
        if user_locale:
            params["userLocale"] = user_locale
        if offset:
            params["offset"] = offset

        response = await self._request("GET", f"{base_id}/{table_id_or_name}", params=params)

        records = []
        for item in response.get("records", []):
            records.append(
                AirtableRecord(
                    id=item["id"],
                    fields=item["fields"],
                    created_time=datetime.fromisoformat(item["createdTime"].replace("Z", "+00:00")),
                )
            )

        return {"records": records, "offset": response.get("offset")}

    async def get_record(self, base_id: str, table_id_or_name: str, record_id: str) -> AirtableRecord:
        """Get a single record by ID."""
        response = await self._request("GET", f"{base_id}/{table_id_or_name}/{record_id}")

        return AirtableRecord(
            id=response["id"],
            fields=response["fields"],
            created_time=datetime.fromisoformat(response["createdTime"].replace("Z", "+00:00")),
        )

    async def create_record(
        self,
        base_id: str,
        table_id_or_name: str,
        fields: dict[str, Any],
        typecast: bool = False,
    ) -> AirtableRecord:
        """Create a new record."""
        data = {"fields": fields, "typecast": typecast}

        response = await self._request("POST", f"{base_id}/{table_id_or_name}", data=data)

        return AirtableRecord(
            id=response["id"],
            fields=response["fields"],
            created_time=datetime.fromisoformat(response["createdTime"].replace("Z", "+00:00")),
        )

    async def create_records(
        self,
        base_id: str,
        table_id_or_name: str,
        records: list[dict[str, Any]],
        typecast: bool = False,
    ) -> list[AirtableRecord]:
        """Create multiple records (max 10 per request)."""
        all_records = []

        # Process in batches of 10
        for i in range(0, len(records), 10):
            batch = records[i : i + 10]
            data = {"records": [{"fields": r} for r in batch], "typecast": typecast}

            response = await self._request("POST", f"{base_id}/{table_id_or_name}", data=data)

            for item in response.get("records", []):
                all_records.append(
                    AirtableRecord(
                        id=item["id"],
                        fields=item["fields"],
                        created_time=datetime.fromisoformat(item["createdTime"].replace("Z", "+00:00")),
                    )
                )

        return all_records

    async def update_record(
        self,
        base_id: str,
        table_id_or_name: str,
        record_id: str,
        fields: dict[str, Any],
        typecast: bool = False,
    ) -> AirtableRecord:
        """Update a record (partial update - PATCH)."""
        data = {"fields": fields, "typecast": typecast}

        response = await self._request("PATCH", f"{base_id}/{table_id_or_name}/{record_id}", data=data)

        return AirtableRecord(
            id=response["id"],
            fields=response["fields"],
            created_time=datetime.fromisoformat(response["createdTime"].replace("Z", "+00:00")),
        )

    async def update_records(
        self,
        base_id: str,
        table_id_or_name: str,
        records: list[dict[str, Any]],
        typecast: bool = False,
    ) -> list[AirtableRecord]:
        """Update multiple records (max 10 per request)."""
        all_records = []

        for i in range(0, len(records), 10):
            batch = records[i : i + 10]
            data = {"records": batch, "typecast": typecast}

            response = await self._request("PATCH", f"{base_id}/{table_id_or_name}", data=data)

            for item in response.get("records", []):
                all_records.append(
                    AirtableRecord(
                        id=item["id"],
                        fields=item["fields"],
                        created_time=datetime.fromisoformat(item["createdTime"].replace("Z", "+00:00")),
                    )
                )

        return all_records

    async def replace_record(
        self,
        base_id: str,
        table_id_or_name: str,
        record_id: str,
        fields: dict[str, Any],
        typecast: bool = False,
    ) -> AirtableRecord:
        """Replace a record (full update - PUT)."""
        data = {"fields": fields, "typecast": typecast}

        response = await self._request("PUT", f"{base_id}/{table_id_or_name}/{record_id}", data=data)

        return AirtableRecord(
            id=response["id"],
            fields=response["fields"],
            created_time=datetime.fromisoformat(response["createdTime"].replace("Z", "+00:00")),
        )

    async def delete_record(self, base_id: str, table_id_or_name: str, record_id: str) -> bool:
        """Delete a single record."""
        response = await self._request("DELETE", f"{base_id}/{table_id_or_name}/{record_id}")
        return response.get("deleted", False)

    async def delete_records(self, base_id: str, table_id_or_name: str, record_ids: list[str]) -> list[dict[str, Any]]:
        """Delete multiple records (max 10 per request)."""
        all_deleted = []

        for i in range(0, len(record_ids), 10):
            batch = record_ids[i : i + 10]
            params = {"records[]": batch}

            response = await self._request("DELETE", f"{base_id}/{table_id_or_name}", params=params)

            all_deleted.extend(response.get("records", []))

        return all_deleted

    # ==================== Field Management ====================

    async def create_field(
        self,
        base_id: str,
        table_id: str,
        name: str,
        field_type: AirtableFieldType,
        options: dict[str, Any] | None = None,
        description: str | None = None,
    ) -> AirtableField:
        """Create a new field in a table."""
        data = {"name": name, "type": field_type.value}

        if options:
            data["options"] = options
        if description:
            data["description"] = description

        response = await self._request("POST", f"meta/bases/{base_id}/tables/{table_id}/fields", data=data)

        return AirtableField(
            id=response["id"],
            name=response["name"],
            type=AirtableFieldType(response["type"]),
            options=response.get("options"),
        )

    async def update_field(
        self,
        base_id: str,
        table_id: str,
        field_id: str,
        name: str | None = None,
        description: str | None = None,
    ) -> AirtableField:
        """Update a field's name or description."""
        data = {}
        if name:
            data["name"] = name
        if description:
            data["description"] = description

        response = await self._request(
            "PATCH",
            f"meta/bases/{base_id}/tables/{table_id}/fields/{field_id}",
            data=data,
        )

        return AirtableField(
            id=response["id"],
            name=response["name"],
            type=AirtableFieldType(response["type"]),
            options=response.get("options"),
        )

    # ==================== Utility Methods ====================

    async def search_records(
        self,
        base_id: str,
        table_id_or_name: str,
        search_field: str,
        search_value: str,
        fields: list[str] | None = None,
    ) -> list[AirtableRecord]:
        """Search for records by field value."""
        formula = f"{{{search_field}}}='{search_value}'"
        result = await self.list_records(
            base_id=base_id,
            table_id_or_name=table_id_or_name,
            filter_by_formula=formula,
            fields=fields,
        )
        return result["records"]

    async def find_record(
        self, base_id: str, table_id_or_name: str, search_field: str, search_value: str
    ) -> AirtableRecord | None:
        """Find a single record by field value."""
        records = await self.search_records(
            base_id=base_id,
            table_id_or_name=table_id_or_name,
            search_field=search_field,
            search_value=search_value,
        )
        return records[0] if records else None

    async def upsert_record(
        self,
        base_id: str,
        table_id_or_name: str,
        search_field: str,
        search_value: str,
        fields: dict[str, Any],
        typecast: bool = False,
    ) -> AirtableRecord:
        """Update a record if it exists, otherwise create it."""
        existing = await self.find_record(
            base_id=base_id,
            table_id_or_name=table_id_or_name,
            search_field=search_field,
            search_value=search_value,
        )

        if existing:
            return await self.update_record(
                base_id=base_id,
                table_id_or_name=table_id_or_name,
                record_id=existing.id,
                fields=fields,
                typecast=typecast,
            )
        else:
            fields[search_field] = search_value
            return await self.create_record(
                base_id=base_id,
                table_id_or_name=table_id_or_name,
                fields=fields,
                typecast=typecast,
            )

    async def get_all_records(
        self,
        base_id: str,
        table_id_or_name: str,
        fields: list[str] | None = None,
        filter_by_formula: str | None = None,
        sort: list[dict[str, str]] | None = None,
        view: str | None = None,
    ) -> list[AirtableRecord]:
        """Get all records from a table (handles pagination)."""
        all_records = []
        offset = None

        while True:
            result = await self.list_records(
                base_id=base_id,
                table_id_or_name=table_id_or_name,
                fields=fields,
                filter_by_formula=filter_by_formula,
                sort=sort,
                view=view,
                offset=offset,
            )

            all_records.extend(result["records"])
            offset = result.get("offset")

            if not offset:
                break

        return all_records


# ==================== Helix Spirals Node Integration ====================


class AirtableNode:
    """
    Helix Spirals node for Airtable integration.

    Supports operations:
    - list_records: List records with filtering
    - get_record: Get a single record
    - create_record: Create a new record
    - update_record: Update an existing record
    - delete_record: Delete a record
    - search_records: Search by field value
    - upsert_record: Create or update record
    - batch_create: Create multiple records
    - batch_update: Update multiple records
    """

    def __init__(self, config: AirtableConfig):
        self.connector = AirtableConnector(config)

    async def execute(self, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        """Execute an Airtable operation."""
        operations = {
            "list_records": self._list_records,
            "get_record": self._get_record,
            "create_record": self._create_record,
            "update_record": self._update_record,
            "delete_record": self._delete_record,
            "search_records": self._search_records,
            "upsert_record": self._upsert_record,
            "batch_create": self._batch_create,
            "batch_update": self._batch_update,
            "list_bases": self._list_bases,
            "list_tables": self._list_tables,
        }

        if operation not in operations:
            raise ValueError(f"Unknown operation: {operation}")

        return await operations[operation](params)

    async def _list_records(self, params: dict[str, Any]) -> dict[str, Any]:
        """List records from a table."""
        result = await self.connector.list_records(
            base_id=params["base_id"],
            table_id_or_name=params["table"],
            fields=params.get("fields"),
            filter_by_formula=params.get("filter"),
            max_records=params.get("max_records"),
            sort=params.get("sort"),
            view=params.get("view"),
        )

        return {
            "success": True,
            "records": [
                {
                    "id": r.id,
                    "fields": r.fields,
                    "created_time": r.created_time.isoformat(),
                }
                for r in result["records"]
            ],
            "has_more": result.get("offset") is not None,
        }

    async def _get_record(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get a single record."""
        record = await self.connector.get_record(
            base_id=params["base_id"],
            table_id_or_name=params["table"],
            record_id=params["record_id"],
        )

        return {
            "success": True,
            "record": {
                "id": record.id,
                "fields": record.fields,
                "created_time": record.created_time.isoformat(),
            },
        }

    async def _create_record(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create a new record."""
        record = await self.connector.create_record(
            base_id=params["base_id"],
            table_id_or_name=params["table"],
            fields=params["fields"],
            typecast=params.get("typecast", False),
        )

        return {
            "success": True,
            "record": {
                "id": record.id,
                "fields": record.fields,
                "created_time": record.created_time.isoformat(),
            },
        }

    async def _update_record(self, params: dict[str, Any]) -> dict[str, Any]:
        """Update an existing record."""
        record = await self.connector.update_record(
            base_id=params["base_id"],
            table_id_or_name=params["table"],
            record_id=params["record_id"],
            fields=params["fields"],
            typecast=params.get("typecast", False),
        )

        return {
            "success": True,
            "record": {
                "id": record.id,
                "fields": record.fields,
                "created_time": record.created_time.isoformat(),
            },
        }

    async def _delete_record(self, params: dict[str, Any]) -> dict[str, Any]:
        """Delete a record."""
        deleted = await self.connector.delete_record(
            base_id=params["base_id"],
            table_id_or_name=params["table"],
            record_id=params["record_id"],
        )

        return {"success": True, "deleted": deleted, "record_id": params["record_id"]}

    async def _search_records(self, params: dict[str, Any]) -> dict[str, Any]:
        """Search records by field value."""
        records = await self.connector.search_records(
            base_id=params["base_id"],
            table_id_or_name=params["table"],
            search_field=params["search_field"],
            search_value=params["search_value"],
            fields=params.get("fields"),
        )

        return {
            "success": True,
            "records": [
                {
                    "id": r.id,
                    "fields": r.fields,
                    "created_time": r.created_time.isoformat(),
                }
                for r in records
            ],
            "count": len(records),
        }

    async def _upsert_record(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create or update a record."""
        record = await self.connector.upsert_record(
            base_id=params["base_id"],
            table_id_or_name=params["table"],
            search_field=params["search_field"],
            search_value=params["search_value"],
            fields=params["fields"],
            typecast=params.get("typecast", False),
        )

        return {
            "success": True,
            "record": {
                "id": record.id,
                "fields": record.fields,
                "created_time": record.created_time.isoformat(),
            },
        }

    async def _batch_create(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create multiple records."""
        records = await self.connector.create_records(
            base_id=params["base_id"],
            table_id_or_name=params["table"],
            records=params["records"],
            typecast=params.get("typecast", False),
        )

        return {
            "success": True,
            "records": [
                {
                    "id": r.id,
                    "fields": r.fields,
                    "created_time": r.created_time.isoformat(),
                }
                for r in records
            ],
            "count": len(records),
        }

    async def _batch_update(self, params: dict[str, Any]) -> dict[str, Any]:
        """Update multiple records."""
        records = await self.connector.update_records(
            base_id=params["base_id"],
            table_id_or_name=params["table"],
            records=params["records"],
            typecast=params.get("typecast", False),
        )

        return {
            "success": True,
            "records": [
                {
                    "id": r.id,
                    "fields": r.fields,
                    "created_time": r.created_time.isoformat(),
                }
                for r in records
            ],
            "count": len(records),
        }

    async def _list_bases(self, params: dict[str, Any]) -> dict[str, Any]:
        """List all accessible bases."""
        bases = await self.connector.list_bases()

        return {
            "success": True,
            "bases": [{"id": b.id, "name": b.name, "permission_level": b.permission_level} for b in bases],
        }

    async def _list_tables(self, params: dict[str, Any]) -> dict[str, Any]:
        """List tables in a base."""
        tables = await self.connector.list_tables(params["base_id"])

        return {
            "success": True,
            "tables": [
                {
                    "id": t.id,
                    "name": t.name,
                    "primary_field_id": t.primary_field_id,
                    "field_count": len(t.fields),
                    "view_count": len(t.views),
                }
                for t in tables
            ],
        }

    async def close(self):
        """Close the connector."""
        await self.connector.close()
