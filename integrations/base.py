"""
Helix Spirals - Third-Party Integration Actions

Provides ready-to-use actions for popular SaaS platforms.
Uses the existing OAuthConnectionManager for token management.
"""

import base64
import email.message
import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

import httpx

logger = logging.getLogger(__name__)

try:
    import asyncpg

    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    logger.debug("asyncpg not available — PostgreSQLIntegration will raise on use")

try:
    import aiomysql

    AIOMYSQL_AVAILABLE = True
except ImportError:
    AIOMYSQL_AVAILABLE = False
    logger.debug("aiomysql not available — MySQLIntegration will raise on use")


class IntegrationBase:
    """Base class for all integrations."""

    def __init__(self, access_token: str):
        self.access_token = access_token
        self.client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self.client.aclose()

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    async def execute(self, action: str, params: dict) -> dict:
        """Dispatch to the named action method. Subclasses may override for custom dispatch."""
        method = getattr(self, action, None)
        if method is None:
            raise NotImplementedError(f"Action '{action}' is not implemented for {type(self).__name__}")
        return await method(**params)


# ============================================================
# SLACK INTEGRATION
# ============================================================


class SlackIntegration(IntegrationBase):
    """
    Slack integration for Helix Spirals.

    Scopes needed: chat:write, channels:read, users:read
    """

    BASE_URL = "https://slack.com/api"

    async def send_message(
        self,
        channel: str,
        text: str,
        blocks: list[dict] | None = None,
        thread_ts: str | None = None,
    ) -> dict[str, Any]:
        """Send a message to a Slack channel."""
        payload = {
            "channel": channel,
            "text": text,
        }
        if blocks:
            payload["blocks"] = blocks
        if thread_ts:
            payload["thread_ts"] = thread_ts

        response = await self.client.post(
            f"{self.BASE_URL}/chat.postMessage",
            headers=self._headers(),
            json=payload,
        )
        return response.json()

    async def list_channels(self, limit: int = 100) -> list[dict]:
        """List available Slack channels."""
        response = await self.client.get(
            f"{self.BASE_URL}/conversations.list",
            headers=self._headers(),
            params={"limit": limit, "types": "public_channel,private_channel"},
        )
        data = response.json()
        return data.get("channels", [])

    async def get_user_info(self, user_id: str) -> dict:
        """Get Slack user information."""
        response = await self.client.get(
            f"{self.BASE_URL}/users.info",
            headers=self._headers(),
            params={"user": user_id},
        )
        return response.json().get("user", {})


# ============================================================
# NOTION INTEGRATION
# ============================================================


class NotionIntegration(IntegrationBase):
    """
    Notion integration for Helix Spirals.

    Capabilities: Create pages, query databases, update content
    """

    BASE_URL = "https://api.notion.com/v1"
    NOTION_VERSION = "2022-06-28"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Notion-Version": self.NOTION_VERSION,
        }
        if extra:
            headers.update(extra)
        return headers

    async def create_page(
        self,
        parent_id: str,
        title: str,
        content: str = "",
        is_database: bool = False,
    ) -> dict[str, Any]:
        """Create a new Notion page."""
        children = []
        if content:
            children.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"type": "text", "text": {"content": content}}]},
                }
            )

        if is_database:
            parent = {"database_id": parent_id}
            properties = {"Name": {"title": [{"text": {"content": title}}]}}
        else:
            parent = {"page_id": parent_id}
            properties = {"title": {"title": [{"text": {"content": title}}]}}

        response = await self.client.post(
            f"{self.BASE_URL}/pages",
            headers=self._headers(),
            json={"parent": parent, "properties": properties, "children": children},
        )
        return response.json()

    async def query_database(
        self,
        database_id: str,
        filter: dict | None = None,
        sorts: list[dict] | None = None,
        page_size: int = 100,
    ) -> list[dict]:
        """Query a Notion database."""
        payload = {"page_size": page_size}
        if filter:
            payload["filter"] = filter
        if sorts:
            payload["sorts"] = sorts

        response = await self.client.post(
            f"{self.BASE_URL}/databases/{database_id}/query",
            headers=self._headers(),
            json=payload,
        )
        data = response.json()
        return data.get("results", [])

    async def search(self, query: str, filter_type: str = "page") -> list[dict]:
        """Search Notion pages or databases."""
        payload = {"query": query}
        if filter_type:
            payload["filter"] = {"property": "object", "value": filter_type}

        response = await self.client.post(
            f"{self.BASE_URL}/search",
            headers=self._headers(),
            json=payload,
        )
        data = response.json()
        return data.get("results", [])


# ============================================================
# GOOGLE SHEETS INTEGRATION
# ============================================================


class GoogleSheetsIntegration(IntegrationBase):
    """
    Google Sheets integration for Helix Spirals.

    Scopes needed: https://www.googleapis.com/auth/spreadsheets
    """

    BASE_URL = "https://sheets.googleapis.com/v4/spreadsheets"

    async def read_range(
        self,
        spreadsheet_id: str,
        range: str,
    ) -> list[list[Any]]:
        """Read values from a spreadsheet range."""
        response = await self.client.get(
            f"{self.BASE_URL}/{spreadsheet_id}/values/{range}",
            headers=self._headers(),
        )
        data = response.json()
        return data.get("values", [])

    async def write_range(
        self,
        spreadsheet_id: str,
        range: str,
        values: list[list[Any]],
        input_option: str = "USER_ENTERED",
    ) -> dict[str, Any]:
        """Write values to a spreadsheet range."""
        response = await self.client.put(
            f"{self.BASE_URL}/{spreadsheet_id}/values/{range}",
            headers=self._headers(),
            params={"valueInputOption": input_option},
            json={"values": values},
        )
        return response.json()

    async def append_rows(
        self,
        spreadsheet_id: str,
        range: str,
        values: list[list[Any]],
        input_option: str = "USER_ENTERED",
    ) -> dict[str, Any]:
        """Append rows to a spreadsheet."""
        response = await self.client.post(
            f"{self.BASE_URL}/{spreadsheet_id}/values/{range}:append",
            headers=self._headers(),
            params={
                "valueInputOption": input_option,
                "insertDataOption": "INSERT_ROWS",
            },
            json={"values": values},
        )
        return response.json()

    async def get_spreadsheet_info(self, spreadsheet_id: str) -> dict[str, Any]:
        """Get spreadsheet metadata."""
        response = await self.client.get(
            f"{self.BASE_URL}/{spreadsheet_id}",
            headers=self._headers(),
        )
        return response.json()


# ============================================================
# AIRTABLE INTEGRATION (API Key based)
# ============================================================


class AirtableIntegration:
    """
    Airtable integration for Helix Spirals.

    Uses API key authentication.
    """

    BASE_URL = "https://api.airtable.com/v0"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = httpx.AsyncClient(timeout=30.0)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def execute(self, action: str, params: dict) -> dict:
        method = getattr(self, action, None)
        if method is None:
            raise NotImplementedError(f"Action '{action}' is not implemented for {type(self).__name__}")
        return await method(**params)

    async def list_records(
        self,
        base_id: str,
        table_name: str,
        max_records: int = 100,
        view: str | None = None,
    ) -> list[dict]:
        """List records from an Airtable table."""
        params = {"maxRecords": max_records}
        if view:
            params["view"] = view

        response = await self.client.get(
            f"{self.BASE_URL}/{base_id}/{table_name}",
            headers=self._headers(),
            params=params,
        )
        data = response.json()
        return data.get("records", [])

    async def create_record(
        self,
        base_id: str,
        table_name: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        """Create a new record in Airtable."""
        response = await self.client.post(
            f"{self.BASE_URL}/{base_id}/{table_name}",
            headers=self._headers(),
            json={"fields": fields},
        )
        return response.json()

    async def update_record(
        self,
        base_id: str,
        table_name: str,
        record_id: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        """Update an existing record."""
        response = await self.client.patch(
            f"{self.BASE_URL}/{base_id}/{table_name}/{record_id}",
            headers=self._headers(),
            json={"fields": fields},
        )
        return response.json()


# ============================================================
# GITHUB INTEGRATION
# ============================================================


class GitHubIntegration(IntegrationBase):
    """
    GitHub integration for Helix Spirals.

    Scopes needed: repo, issues:write, pull_requests:write
    """

    BASE_URL = "https://api.github.com"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if extra:
            headers.update(extra)
        return headers

    async def create_issue(
        self,
        owner: str,
        repo: str,
        title: str,
        body: str = "",
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
    ) -> dict[str, Any]:
        """Create a GitHub issue."""
        payload = {"title": title, "body": body}
        if labels:
            payload["labels"] = labels
        if assignees:
            payload["assignees"] = assignees

        response = await self.client.post(
            f"{self.BASE_URL}/repos/{owner}/{repo}/issues",
            headers=self._headers(),
            json=payload,
        )
        return response.json()

    async def create_comment(
        self,
        owner: str,
        repo: str,
        issue_number: int,
        body: str,
    ) -> dict[str, Any]:
        """Add a comment to an issue or PR."""
        response = await self.client.post(
            f"{self.BASE_URL}/repos/{owner}/{repo}/issues/{issue_number}/comments",
            headers=self._headers(),
            json={"body": body},
        )
        return response.json()

    async def list_repos(self, per_page: int = 30) -> list[dict]:
        """List authenticated user's repositories."""
        response = await self.client.get(
            f"{self.BASE_URL}/user/repos",
            headers=self._headers(),
            params={"per_page": per_page, "sort": "updated"},
        )
        return response.json()


# ============================================================
# OPENAI INTEGRATION (API Key based)
# ============================================================


class OpenAIIntegration:
    """
    OpenAI/ChatGPT integration for Helix Spirals.

    Enables AI processing within workflows.
    """

    BASE_URL = "https://api.openai.com/v1"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = httpx.AsyncClient(timeout=60.0)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def execute(self, action: str, params: dict) -> dict:
        method = getattr(self, action, None)
        if method is None:
            raise NotImplementedError(f"Action '{action}' is not implemented for {type(self).__name__}")
        return await method(**params)

    async def chat_completion(
        self,
        messages: list[dict[str, str]],
        model: str = "gpt-4o-mini",
        temperature: float = 0.7,
        max_tokens: int = 1000,
    ) -> dict[str, Any]:
        """Generate a chat completion."""
        response = await self.client.post(
            f"{self.BASE_URL}/chat/completions",
            headers=self._headers(),
            json={
                "model": model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
        )
        return response.json()

    async def generate_embedding(
        self,
        text: str,
        model: str = "text-embedding-3-small",
    ) -> list[float]:
        """Generate embeddings for text."""
        response = await self.client.post(
            f"{self.BASE_URL}/embeddings",
            headers=self._headers(),
            json={"model": model, "input": text},
        )
        data = response.json()
        return data.get("data", [{}])[0].get("embedding", [])


# ============================================================
# TWILIO INTEGRATION (API Key based)
# ============================================================


class TwilioIntegration:
    """
    Twilio SMS integration for Helix Spirals.
    """

    BASE_URL = "https://api.twilio.com/2010-04-01"

    def __init__(self, account_sid: str, auth_token: str):
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.client = httpx.AsyncClient(
            timeout=30.0,
            auth=(account_sid, auth_token),
        )

    async def execute(self, action: str, params: dict) -> dict:
        method = getattr(self, action, None)
        if method is None:
            raise NotImplementedError(f"Action '{action}' is not implemented for {type(self).__name__}")
        return await method(**params)

    async def send_sms(
        self,
        from_number: str,
        to_number: str,
        body: str,
    ) -> dict[str, Any]:
        """Send an SMS message."""
        response = await self.client.post(
            f"{self.BASE_URL}/Accounts/{self.account_sid}/Messages.json",
            data={
                "From": from_number,
                "To": to_number,
                "Body": body,
            },
        )
        return response.json()


# ============================================================
# HUBSPOT INTEGRATION
# ============================================================


class HubSpotIntegration(IntegrationBase):
    """
    HubSpot CRM integration for Helix Spirals.
    """

    BASE_URL = "https://api.hubapi.com"

    async def create_contact(
        self,
        email: str,
        firstname: str = "",
        lastname: str = "",
        properties: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Create a HubSpot contact."""
        props = {
            "email": email,
            "firstname": firstname,
            "lastname": lastname,
        }
        if properties:
            props.update(properties)

        response = await self.client.post(
            f"{self.BASE_URL}/crm/v3/objects/contacts",
            headers=self._headers(),
            json={"properties": props},
        )
        return response.json()

    async def list_contacts(self, limit: int = 10) -> list[dict]:
        """List HubSpot contacts."""
        response = await self.client.get(
            f"{self.BASE_URL}/crm/v3/objects/contacts",
            headers=self._headers(),
            params={"limit": limit},
        )
        data = response.json()
        return data.get("results", [])

    async def create_deal(
        self,
        deal_name: str,
        amount: float = 0,
        stage: str = "appointmentscheduled",
        properties: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Create a HubSpot deal."""
        props = {
            "dealname": deal_name,
            "amount": str(amount),
            "dealstage": stage,
        }
        if properties:
            props.update(properties)

        response = await self.client.post(
            f"{self.BASE_URL}/crm/v3/objects/deals",
            headers=self._headers(),
            json={"properties": props},
        )
        return response.json()


# ============================================================
# TRELLO INTEGRATION
# ============================================================


class TrelloIntegration(IntegrationBase):
    """
    Trello integration for Helix Spirals.
    """

    BASE_URL = "https://api.trello.com/1"

    def __init__(self, access_token: str, api_key: str):
        super().__init__(access_token)
        self.api_key = api_key

    async def create_card(
        self,
        list_id: str,
        name: str,
        desc: str = "",
        due: str | None = None,
    ) -> dict[str, Any]:
        """Create a Trello card."""
        params = {
            "key": self.api_key,
            "token": self.access_token,
            "idList": list_id,
            "name": name,
            "desc": desc,
        }
        if due:
            params["due"] = due

        response = await self.client.post(
            f"{self.BASE_URL}/cards",
            params=params,
        )
        return response.json()

    async def list_boards(self) -> list[dict]:
        """List user's Trello boards."""
        response = await self.client.get(
            f"{self.BASE_URL}/members/me/boards",
            params={"key": self.api_key, "token": self.access_token},
        )
        return response.json()


# ============================================================
# LINEAR INTEGRATION
# ============================================================


class LinearIntegration(IntegrationBase):
    BASE_URL = "https://api.linear.app/graphql"

    async def _gql(self, query: str, variables: dict = None) -> dict:
        response = await self.client.post(
            self.BASE_URL,
            headers=self._headers(),
            json={"query": query, "variables": variables or {}},
        )
        return response.json()

    async def create_issue(
        self,
        title: str,
        team_id: str,
        description: str = "",
        priority: int = 0,
    ) -> dict:
        mutation = """
        mutation CreateIssue($title: String!, $teamId: String!, $description: String, $priority: Int) {
            issueCreate(input: {title: $title, teamId: $teamId, description: $description, priority: $priority}) {
                success
                issue { id title url }
            }
        }
        """
        return await self._gql(
            mutation,
            {"title": title, "teamId": team_id, "description": description, "priority": priority},
        )

    async def update_issue(
        self,
        issue_id: str,
        title: str = None,
        description: str = None,
        state_id: str = None,
    ) -> dict:
        mutation = """
        mutation UpdateIssue($id: String!, $title: String, $description: String, $stateId: String) {
            issueUpdate(id: $id, input: {title: $title, description: $description, stateId: $stateId}) {
                success
                issue { id title }
            }
        }
        """
        variables = {"id": issue_id}
        if title is not None:
            variables["title"] = title
        if description is not None:
            variables["description"] = description
        if state_id is not None:
            variables["stateId"] = state_id
        return await self._gql(mutation, variables)

    async def list_issues(self, team_id: str = None, limit: int = 25) -> dict:
        query = """
        query ListIssues($teamId: ID, $first: Int) {
            issues(filter: {team: {id: {eq: $teamId}}}, first: $first) {
                nodes { id title state { name } priority }
            }
        }
        """
        return await self._gql(query, {"teamId": team_id, "first": limit})

    async def list_teams(self) -> dict:
        query = "query { teams { nodes { id name } } }"
        return await self._gql(query)

    async def list_projects(self, team_id: str = None) -> dict:
        query = """
        query ListProjects($teamId: ID) {
            projects(filter: {teams: {id: {eq: $teamId}}}) {
                nodes { id name state }
            }
        }
        """
        return await self._gql(query, {"teamId": team_id})


# ============================================================
# JIRA INTEGRATION
# ============================================================


class JiraIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        super().__init__(access_token)
        parts = access_token.split(":", 2)
        if len(parts) != 3:
            raise ValueError("JiraIntegration access_token must be 'email:api_token:domain'")
        self.email, self.api_token, self.domain = parts
        encoded = base64.b64encode(f"{self.email}:{self.api_token}".encode()).decode()
        self._auth_header = f"Basic {encoded}"
        self.base_url = f"https://{self.domain}.atlassian.net/rest/api/3"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": self._auth_header,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    async def create_issue(
        self,
        project_key: str,
        summary: str,
        description: str = "",
        issue_type: str = "Task",
    ) -> dict:
        payload = {
            "fields": {
                "project": {"key": project_key},
                "summary": summary,
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": description}],
                        }
                    ],
                },
                "issuetype": {"name": issue_type},
            }
        }
        response = await self.client.post(
            f"{self.base_url}/issue",
            headers=self._headers(),
            json=payload,
        )
        return response.json()

    async def update_issue(
        self,
        issue_key: str,
        summary: str = None,
        description: str = None,
    ) -> dict:
        fields: dict = {}
        if summary is not None:
            fields["summary"] = summary
        if description is not None:
            fields["description"] = {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": description}],
                    }
                ],
            }
        response = await self.client.put(
            f"{self.base_url}/issue/{issue_key}",
            headers=self._headers(),
            json={"fields": fields},
        )
        return {"status_code": response.status_code}

    async def list_issues(self, project_key: str, max_results: int = 25) -> dict:
        response = await self.client.get(
            f"{self.base_url}/search",
            headers=self._headers(),
            params={"jql": f"project={project_key}", "maxResults": max_results},
        )
        return response.json()

    async def add_comment(self, issue_key: str, body: str) -> dict:
        payload = {
            "body": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": body}],
                    }
                ],
            }
        }
        response = await self.client.post(
            f"{self.base_url}/issue/{issue_key}/comment",
            headers=self._headers(),
            json=payload,
        )
        return response.json()

    async def transition_issue(self, issue_key: str, transition_id: str) -> dict:
        response = await self.client.post(
            f"{self.base_url}/issue/{issue_key}/transitions",
            headers=self._headers(),
            json={"transition": {"id": transition_id}},
        )
        return {"status_code": response.status_code}


# ============================================================
# ASANA INTEGRATION
# ============================================================


class AsanaIntegration(IntegrationBase):
    BASE_URL = "https://app.asana.com/api/1.0"

    async def create_task(
        self,
        name: str,
        workspace_id: str,
        project_id: str = None,
        notes: str = "",
        assignee: str = None,
    ) -> dict:
        data: dict = {"name": name, "workspace": workspace_id, "notes": notes}
        if project_id:
            data["projects"] = [project_id]
        if assignee:
            data["assignee"] = assignee
        response = await self.client.post(
            f"{self.BASE_URL}/tasks",
            headers=self._headers(),
            json={"data": data},
        )
        return response.json()

    async def update_task(
        self,
        task_id: str,
        name: str = None,
        notes: str = None,
        completed: bool = None,
    ) -> dict:
        data: dict = {}
        if name is not None:
            data["name"] = name
        if notes is not None:
            data["notes"] = notes
        if completed is not None:
            data["completed"] = completed
        response = await self.client.put(
            f"{self.BASE_URL}/tasks/{task_id}",
            headers=self._headers(),
            json={"data": data},
        )
        return response.json()

    async def list_tasks(
        self,
        project_id: str = None,
        workspace_id: str = None,
        limit: int = 25,
    ) -> dict:
        params: dict = {"limit": limit}
        if project_id:
            params["project"] = project_id
        elif workspace_id:
            params["workspace"] = workspace_id
        response = await self.client.get(
            f"{self.BASE_URL}/tasks",
            headers=self._headers(),
            params=params,
        )
        return response.json()

    async def create_project(self, name: str, workspace_id: str, notes: str = "") -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/projects",
            headers=self._headers(),
            json={"data": {"name": name, "workspace": workspace_id, "notes": notes}},
        )
        return response.json()

    async def add_comment(self, task_id: str, text: str) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/tasks/{task_id}/stories",
            headers=self._headers(),
            json={"data": {"text": text}},
        )
        return response.json()


# ============================================================
# ZENDESK INTEGRATION
# ============================================================


class ZendeskIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        super().__init__(access_token)
        parts = access_token.split(":", 2)
        if len(parts) != 3:
            raise ValueError("ZendeskIntegration access_token must be 'email:api_token:subdomain'")
        self.email, self.api_token, self.subdomain = parts
        encoded = base64.b64encode(f"{self.email}/token:{self.api_token}".encode()).decode()
        self._auth_header = f"Basic {encoded}"
        self.base_url = f"https://{self.subdomain}.zendesk.com/api/v2"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": self._auth_header,
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    async def create_ticket(
        self,
        subject: str,
        body: str,
        requester_email: str = None,
        priority: str = "normal",
    ) -> dict:
        ticket: dict = {"subject": subject, "comment": {"body": body}, "priority": priority}
        if requester_email:
            ticket["requester"] = {"email": requester_email}
        response = await self.client.post(
            f"{self.base_url}/tickets.json",
            headers=self._headers(),
            json={"ticket": ticket},
        )
        return response.json()

    async def update_ticket(
        self,
        ticket_id: str,
        subject: str = None,
        status: str = None,
        priority: str = None,
    ) -> dict:
        ticket: dict = {}
        if subject is not None:
            ticket["subject"] = subject
        if status is not None:
            ticket["status"] = status
        if priority is not None:
            ticket["priority"] = priority
        response = await self.client.put(
            f"{self.base_url}/tickets/{ticket_id}.json",
            headers=self._headers(),
            json={"ticket": ticket},
        )
        return response.json()

    async def list_tickets(self, status: str = "open", per_page: int = 25) -> dict:
        response = await self.client.get(
            f"{self.base_url}/tickets.json",
            headers=self._headers(),
            params={"status": status, "per_page": per_page},
        )
        return response.json()

    async def add_comment(self, ticket_id: str, body: str, public: bool = True) -> dict:
        response = await self.client.put(
            f"{self.base_url}/tickets/{ticket_id}.json",
            headers=self._headers(),
            json={"ticket": {"comment": {"body": body, "public": public}}},
        )
        return response.json()

    async def close_ticket(self, ticket_id: str) -> dict:
        response = await self.client.put(
            f"{self.base_url}/tickets/{ticket_id}.json",
            headers=self._headers(),
            json={"ticket": {"status": "closed"}},
        )
        return response.json()


# ============================================================
# GOOGLE CALENDAR INTEGRATION
# ============================================================


class GoogleCalendarIntegration(IntegrationBase):
    BASE_URL = "https://www.googleapis.com/calendar/v3"

    async def create_event(
        self,
        summary: str,
        start_datetime: str,
        end_datetime: str,
        calendar_id: str = "primary",
        description: str = "",
        attendees: list[str] = None,
    ) -> dict:
        body: dict = {
            "summary": summary,
            "description": description,
            "start": {"dateTime": start_datetime},
            "end": {"dateTime": end_datetime},
        }
        if attendees:
            body["attendees"] = [{"email": a} for a in attendees]
        response = await self.client.post(
            f"{self.BASE_URL}/calendars/{calendar_id}/events",
            headers=self._headers(),
            json=body,
        )
        return response.json()

    async def list_events(
        self,
        calendar_id: str = "primary",
        max_results: int = 25,
        time_min: str = None,
    ) -> dict:
        params: dict = {"maxResults": max_results}
        if time_min:
            params["timeMin"] = time_min
        response = await self.client.get(
            f"{self.BASE_URL}/calendars/{calendar_id}/events",
            headers=self._headers(),
            params=params,
        )
        return response.json()

    async def update_event(
        self,
        event_id: str,
        calendar_id: str = "primary",
        summary: str = None,
        start_datetime: str = None,
        end_datetime: str = None,
    ) -> dict:
        body: dict = {}
        if summary is not None:
            body["summary"] = summary
        if start_datetime is not None:
            body["start"] = {"dateTime": start_datetime}
        if end_datetime is not None:
            body["end"] = {"dateTime": end_datetime}
        response = await self.client.put(
            f"{self.BASE_URL}/calendars/{calendar_id}/events/{event_id}",
            headers=self._headers(),
            json=body,
        )
        return response.json()

    async def delete_event(self, event_id: str, calendar_id: str = "primary") -> dict:
        response = await self.client.delete(
            f"{self.BASE_URL}/calendars/{calendar_id}/events/{event_id}",
            headers=self._headers(),
        )
        return {"status_code": response.status_code}

    async def get_calendar(self, calendar_id: str = "primary") -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/calendars/{calendar_id}",
            headers=self._headers(),
        )
        return response.json()


# ============================================================
# GMAIL INTEGRATION
# ============================================================


class GmailIntegration(IntegrationBase):
    BASE_URL = "https://gmail.googleapis.com/gmail/v1"

    async def send_email(
        self,
        to: str,
        subject: str,
        body: str,
        cc: str = None,
    ) -> dict:
        msg = email.message.EmailMessage()
        msg["To"] = to
        msg["Subject"] = subject
        if cc:
            msg["Cc"] = cc
        msg.set_content(body)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        response = await self.client.post(
            f"{self.BASE_URL}/users/me/messages/send",
            headers=self._headers(),
            json={"raw": raw},
        )
        return response.json()

    async def list_emails(self, max_results: int = 25, query: str = None) -> dict:
        params: dict = {"maxResults": max_results}
        if query:
            params["q"] = query
        response = await self.client.get(
            f"{self.BASE_URL}/users/me/messages",
            headers=self._headers(),
            params=params,
        )
        return response.json()

    async def get_email(self, message_id: str) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/users/me/messages/{message_id}",
            headers=self._headers(),
            params={"format": "full"},
        )
        return response.json()

    async def reply_email(self, message_id: str, body: str) -> dict:
        original = await self.get_email(message_id)
        thread_id = original.get("threadId", "")
        headers_list = original.get("payload", {}).get("headers", [])
        subject = next((h["value"] for h in headers_list if h["name"].lower() == "subject"), "")
        from_addr = next((h["value"] for h in headers_list if h["name"].lower() == "from"), "")
        msg = email.message.EmailMessage()
        msg["To"] = from_addr
        msg["Subject"] = f"Re: {subject}" if not subject.startswith("Re:") else subject
        msg.set_content(body)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        response = await self.client.post(
            f"{self.BASE_URL}/users/me/messages/send",
            headers=self._headers(),
            json={"raw": raw, "threadId": thread_id},
        )
        return response.json()

    async def label_email(self, message_id: str, label_ids: list[str]) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/users/me/messages/{message_id}/modify",
            headers=self._headers(),
            json={"addLabelIds": label_ids},
        )
        return response.json()


# ============================================================
# GOOGLE DRIVE INTEGRATION
# ============================================================


class GoogleDriveIntegration(IntegrationBase):
    BASE_URL = "https://www.googleapis.com/drive/v3"
    UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3"

    async def upload_file(
        self,
        name: str,
        content: str,
        mime_type: str = "text/plain",
        folder_id: str = None,
    ) -> dict:
        metadata: dict = {"name": name}
        if folder_id:
            metadata["parents"] = [folder_id]
        import json as _json

        boundary = "helix_boundary_x7k"
        body = (
            f"--{boundary}\r\n"
            f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
            f"{_json.dumps(metadata)}\r\n"
            f"--{boundary}\r\n"
            f"Content-Type: {mime_type}\r\n\r\n"
            f"{content}\r\n"
            f"--{boundary}--"
        ).encode()
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": f"multipart/related; boundary={boundary}",
        }
        response = await self.client.post(
            f"{self.UPLOAD_URL}/files?uploadType=multipart",
            headers=headers,
            content=body,
        )
        return response.json()

    async def download_file(self, file_id: str) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/files/{file_id}",
            headers=self._headers(),
            params={"alt": "media"},
        )
        return {"content": response.text}

    async def list_files(self, query: str = None, max_results: int = 25) -> dict:
        params: dict = {"pageSize": max_results}
        if query:
            params["q"] = query
        response = await self.client.get(
            f"{self.BASE_URL}/files",
            headers=self._headers(),
            params=params,
        )
        return response.json()

    async def create_folder(self, name: str, parent_id: str = None) -> dict:
        metadata: dict = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if parent_id:
            metadata["parents"] = [parent_id]
        response = await self.client.post(
            f"{self.BASE_URL}/files",
            headers=self._headers(),
            json=metadata,
        )
        return response.json()

    async def share_file(self, file_id: str, email: str, role: str = "reader") -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/files/{file_id}/permissions",
            headers=self._headers(),
            json={"type": "user", "role": role, "emailAddress": email},
        )
        return response.json()


# ============================================================
# DROPBOX INTEGRATION
# ============================================================


class DropboxIntegration(IntegrationBase):
    API_URL = "https://api.dropboxapi.com/2"
    CONTENT_URL = "https://content.dropboxapi.com/2"

    async def upload_file(self, path: str, content: str) -> dict:
        import json as _json

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/octet-stream",
            "Dropbox-API-Arg": _json.dumps({"path": path, "mode": "add", "autorename": True}),
        }
        response = await self.client.post(
            f"{self.CONTENT_URL}/files/upload",
            headers=headers,
            content=content.encode(),
        )
        return response.json()

    async def download_file(self, path: str) -> dict:
        import json as _json

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Dropbox-API-Arg": _json.dumps({"path": path}),
        }
        response = await self.client.post(
            f"{self.CONTENT_URL}/files/download",
            headers=headers,
        )
        return {"content": response.text}

    async def list_files(self, path: str = "") -> dict:
        response = await self.client.post(
            f"{self.API_URL}/files/list_folder",
            headers=self._headers(),
            json={"path": path},
        )
        return response.json()

    async def create_folder(self, path: str) -> dict:
        response = await self.client.post(
            f"{self.API_URL}/files/create_folder_v2",
            headers=self._headers(),
            json={"path": path},
        )
        return response.json()

    async def share_link(self, path: str) -> dict:
        response = await self.client.post(
            f"{self.API_URL}/sharing/create_shared_link_with_settings",
            headers=self._headers(),
            json={"path": path, "settings": {"requested_visibility": "public"}},
        )
        return response.json()


# ============================================================
# WEBHOOK INTEGRATION
# ============================================================


class WebhookIntegration(IntegrationBase):
    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers: dict = {}
        if extra:
            headers.update(extra)
        return headers

    async def send_post(
        self,
        url: str,
        data: dict = None,
        headers: dict = None,
    ) -> dict:
        h = self._headers(headers)
        h.setdefault("Content-Type", "application/json")
        response = await self.client.post(url, headers=h, json=data or {})
        return {
            "status_code": response.status_code,
            "body": response.text,
            "headers": dict(response.headers),
        }

    async def send_get(
        self,
        url: str,
        params: dict = None,
        headers: dict = None,
    ) -> dict:
        response = await self.client.get(url, headers=self._headers(headers), params=params)
        return {
            "status_code": response.status_code,
            "body": response.text,
            "headers": dict(response.headers),
        }

    async def send_put(
        self,
        url: str,
        data: dict = None,
        headers: dict = None,
    ) -> dict:
        h = self._headers(headers)
        h.setdefault("Content-Type", "application/json")
        response = await self.client.put(url, headers=h, json=data or {})
        return {
            "status_code": response.status_code,
            "body": response.text,
            "headers": dict(response.headers),
        }

    async def send_delete(self, url: str, headers: dict = None) -> dict:
        response = await self.client.delete(url, headers=self._headers(headers))
        return {
            "status_code": response.status_code,
            "body": response.text,
            "headers": dict(response.headers),
        }


# ============================================================
# HTTP REQUEST INTEGRATION
# ============================================================


class HttpRequestIntegration(IntegrationBase):
    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers: dict = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        if extra:
            headers.update(extra)
        return headers

    async def get(self, url: str, params: dict = None, headers: dict = None) -> dict:
        response = await self.client.get(url, headers=self._headers(headers), params=params)
        return {
            "status_code": response.status_code,
            "body": response.text,
            "headers": dict(response.headers),
        }

    async def post(self, url: str, data: dict = None, headers: dict = None) -> dict:
        h = self._headers(headers)
        h.setdefault("Content-Type", "application/json")
        response = await self.client.post(url, headers=h, json=data or {})
        return {
            "status_code": response.status_code,
            "body": response.text,
            "headers": dict(response.headers),
        }

    async def put(self, url: str, data: dict = None, headers: dict = None) -> dict:
        h = self._headers(headers)
        h.setdefault("Content-Type", "application/json")
        response = await self.client.put(url, headers=h, json=data or {})
        return {
            "status_code": response.status_code,
            "body": response.text,
            "headers": dict(response.headers),
        }

    async def patch(self, url: str, data: dict = None, headers: dict = None) -> dict:
        h = self._headers(headers)
        h.setdefault("Content-Type", "application/json")
        response = await self.client.patch(url, headers=h, json=data or {})
        return {
            "status_code": response.status_code,
            "body": response.text,
            "headers": dict(response.headers),
        }

    async def delete(self, url: str, headers: dict = None) -> dict:
        response = await self.client.delete(url, headers=self._headers(headers))
        return {
            "status_code": response.status_code,
            "body": response.text,
            "headers": dict(response.headers),
        }


# ============================================================
# POSTGRESQL INTEGRATION
# ============================================================

_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _safe_identifier(name: str) -> str:
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    # Double-quote the identifier for defense-in-depth
    return f'"{name}"'


class PostgreSQLIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.connection_string = access_token
        self.client = httpx.AsyncClient(timeout=30.0)

    async def execute(self, action: str, params: dict) -> dict:
        method = getattr(self, action, None)
        if method is None:
            raise NotImplementedError(f"Action '{action}' is not implemented for {type(self).__name__}")
        return await method(**params)

    async def run_query(self, sql: str, params: list = None) -> dict:
        if not ASYNCPG_AVAILABLE:
            raise ImportError("asyncpg is required for PostgreSQLIntegration. Install with: pip install asyncpg")
        # Safety: only allow SELECT queries to prevent SQL injection / data mutation
        import re as _re

        stripped = sql.strip()
        cleaned = _re.sub(r"/\*.*?\*/", "", stripped, flags=_re.DOTALL).strip()
        cleaned = _re.sub(r"--[^\n]*", "", cleaned).strip()  # strip ALL line comments
        first_word = cleaned.split()[0].lower() if cleaned else ""
        if first_word not in ("select", "with", "explain", "show", "describe"):
            raise ValueError(
                "Only SELECT queries are allowed via run_query(). "
                f"Got: '{first_word.upper()}'. Use insert_row/update_rows/delete_rows for mutations."
            )
        # Reject multi-statement queries (strip both single-quoted strings and dollar-quoted blocks)
        no_strings = _re.sub(r"'[^']*'", "", sql)
        no_strings = _re.sub(r"\$\$.*?\$\$", "", no_strings, flags=_re.DOTALL)
        if ";" in no_strings:
            raise ValueError("Multi-statement queries are not allowed.")
        conn = await asyncpg.connect(self.connection_string)
        try:
            await conn.execute("SET TRANSACTION READ ONLY")
            rows = await conn.fetch(sql, *(params or []))
            return {"rows": [dict(r) for r in rows], "count": len(rows)}
        finally:
            await conn.close()

    async def insert_row(self, table: str, data: dict) -> dict:
        if not ASYNCPG_AVAILABLE:
            raise ImportError("asyncpg is required for PostgreSQLIntegration. Install with: pip install asyncpg")
        _safe_identifier(table)
        cols = [_safe_identifier(c) for c in data]
        placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) RETURNING *"
        conn = await asyncpg.connect(self.connection_string)
        try:
            row = await conn.fetchrow(sql, *list(data.values()))
            return dict(row) if row else {}
        finally:
            await conn.close()

    async def update_rows(self, table: str, data: dict, where: dict) -> dict:
        if not ASYNCPG_AVAILABLE:
            raise ImportError("asyncpg is required for PostgreSQLIntegration. Install with: pip install asyncpg")
        _safe_identifier(table)
        set_parts = []
        values = []
        idx = 1
        for col, val in data.items():
            set_parts.append(f"{_safe_identifier(col)} = ${idx}")
            values.append(val)
            idx += 1
        where_parts = []
        for col, val in where.items():
            where_parts.append(f"{_safe_identifier(col)} = ${idx}")
            values.append(val)
            idx += 1
        sql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)}"
        conn = await asyncpg.connect(self.connection_string)
        try:
            result = await conn.execute(sql, *values)
            return {"result": result}
        finally:
            await conn.close()

    async def delete_rows(self, table: str, where: dict) -> dict:
        if not ASYNCPG_AVAILABLE:
            raise ImportError("asyncpg is required for PostgreSQLIntegration. Install with: pip install asyncpg")
        _safe_identifier(table)
        where_parts = []
        values = []
        for idx, (col, val) in enumerate(where.items(), start=1):
            where_parts.append(f"{_safe_identifier(col)} = ${idx}")
            values.append(val)
        sql = f"DELETE FROM {table} WHERE {' AND '.join(where_parts)}"
        conn = await asyncpg.connect(self.connection_string)
        try:
            result = await conn.execute(sql, *values)
            return {"result": result}
        finally:
            await conn.close()

    async def list_tables(self) -> dict:
        if not ASYNCPG_AVAILABLE:
            raise ImportError("asyncpg is required for PostgreSQLIntegration. Install with: pip install asyncpg")
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name"
        conn = await asyncpg.connect(self.connection_string)
        try:
            rows = await conn.fetch(sql)
            return {"tables": [r["table_name"] for r in rows]}
        finally:
            await conn.close()


# ============================================================
# MYSQL INTEGRATION
# ============================================================


class MySQLIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.client = httpx.AsyncClient(timeout=30.0)
        # Parse user:password@host:port/database
        import urllib.parse

        try:
            parsed = urllib.parse.urlparse(f"mysql://{access_token}")
            self._user = parsed.username or ""
            self._password = parsed.password or ""
            self._host = parsed.hostname or "localhost"
            self._port = parsed.port or 3306
            self._db = (parsed.path or "/").lstrip("/")
        except Exception as exc:
            logger.warning("MySQLIntegration: failed to parse access_token: %s", exc)
            self._user = self._password = self._host = self._db = ""
            self._port = 3306

    async def execute(self, action: str, params: dict) -> dict:
        method = getattr(self, action, None)
        if method is None:
            raise NotImplementedError(f"Action '{action}' is not implemented for {type(self).__name__}")
        return await method(**params)

    async def _connect(self):
        if not AIOMYSQL_AVAILABLE:
            raise ImportError("aiomysql is required for MySQLIntegration. Install with: pip install aiomysql")
        return await aiomysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            db=self._db,
        )

    async def run_query(self, sql: str, params: list = None) -> dict:
        # Safety: only allow SELECT queries to prevent SQL injection / data mutation
        import re as _re

        stripped = sql.strip()
        cleaned = _re.sub(r"/\*.*?\*/", "", stripped, flags=_re.DOTALL).strip()
        cleaned = _re.sub(r"--[^\n]*", "", cleaned).strip()  # strip ALL line comments
        first_word = cleaned.split()[0].lower() if cleaned else ""
        if first_word not in ("select", "with", "explain", "show", "describe"):
            raise ValueError(
                "Only SELECT queries are allowed via run_query(). "
                f"Got: '{first_word.upper()}'. Use insert_row/update_rows/delete_rows for mutations."
            )
        # Reject multi-statement queries
        if ";" in _re.sub(r"'[^']*'", "", sql):
            raise ValueError("Multi-statement queries are not allowed.")
        conn = await self._connect()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, params or ())
                rows = await cur.fetchall()
                return {"rows": list(rows), "count": len(rows)}
        finally:
            conn.close()

    async def insert_row(self, table: str, data: dict) -> dict:
        _safe_identifier(table)
        cols = [_safe_identifier(c) for c in data]
        placeholders = ", ".join("%s" for _ in cols)
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
        conn = await self._connect()
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql, list(data.values()))
                await conn.commit()
                return {"insert_id": cur.lastrowid}
        finally:
            conn.close()

    async def update_rows(self, table: str, data: dict, where: dict) -> dict:
        _safe_identifier(table)
        set_parts = [f"{_safe_identifier(c)} = %s" for c in data]
        where_parts = [f"{_safe_identifier(c)} = %s" for c in where]
        sql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)}"
        values = list(data.values()) + list(where.values())
        conn = await self._connect()
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql, values)
                await conn.commit()
                return {"affected_rows": cur.rowcount}
        finally:
            conn.close()

    async def delete_rows(self, table: str, where: dict) -> dict:
        _safe_identifier(table)
        where_parts = [f"{_safe_identifier(c)} = %s" for c in where]
        sql = f"DELETE FROM {table} WHERE {' AND '.join(where_parts)}"
        conn = await self._connect()
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql, list(where.values()))
                await conn.commit()
                return {"affected_rows": cur.rowcount}
        finally:
            conn.close()

    async def list_tables(self) -> dict:
        conn = await self._connect()
        try:
            async with conn.cursor() as cur:
                await cur.execute("SHOW TABLES")
                rows = await cur.fetchall()
                return {"tables": [r[0] for r in rows]}
        finally:
            conn.close()


# ============================================================
# SALESFORCE INTEGRATION
# ============================================================


class SalesforceIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        super().__init__(access_token)
        parts = access_token.split(":", 1)
        if len(parts) != 2:
            raise ValueError("SalesforceIntegration access_token must be 'token:instance_url'")
        self._token, self._instance_url = parts
        self.base_url = f"{self._instance_url.rstrip('/')}/services/data/v58.0"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    async def create_lead(
        self,
        first_name: str,
        last_name: str,
        email: str,
        company: str,
        phone: str = None,
    ) -> dict:
        data: dict = {
            "FirstName": first_name,
            "LastName": last_name,
            "Email": email,
            "Company": company,
        }
        if phone:
            data["Phone"] = phone
        response = await self.client.post(
            f"{self.base_url}/sobjects/Lead",
            headers=self._headers(),
            json=data,
        )
        return response.json()

    async def update_lead(self, lead_id: str, **fields) -> dict:
        response = await self.client.patch(
            f"{self.base_url}/sobjects/Lead/{lead_id}",
            headers=self._headers(),
            json=fields,
        )
        return {"status_code": response.status_code}

    async def create_opportunity(
        self,
        name: str,
        account_id: str,
        stage_name: str,
        close_date: str,
        amount: float = None,
    ) -> dict:
        data: dict = {
            "Name": name,
            "AccountId": account_id,
            "StageName": stage_name,
            "CloseDate": close_date,
        }
        if amount is not None:
            data["Amount"] = amount
        response = await self.client.post(
            f"{self.base_url}/sobjects/Opportunity",
            headers=self._headers(),
            json=data,
        )
        return response.json()

    async def list_contacts(self, limit: int = 25) -> dict:
        query = f"SELECT Id,Name,Email FROM Contact LIMIT {int(limit)}"
        response = await self.client.get(
            f"{self.base_url}/query",
            headers=self._headers(),
            params={"q": query},
        )
        return response.json()

    async def run_soql(self, query: str) -> dict:
        # Safety: only allow SELECT SOQL to prevent data mutation / exfiltration
        import re as _re

        cleaned = _re.sub(r"/\*.*?\*/", "", query.strip(), flags=_re.DOTALL).strip()
        first_word = cleaned.split()[0].lower() if cleaned else ""
        if first_word != "select":
            raise ValueError(f"Only SELECT SOQL queries are allowed via run_soql(). Got: '{first_word.upper()}'.")
        response = await self.client.get(
            f"{self.base_url}/query",
            headers=self._headers(),
            params={"q": query},
        )
        return response.json()


# ============================================================
# INTERCOM INTEGRATION
# ============================================================


class IntercomIntegration(IntegrationBase):
    BASE_URL = "https://api.intercom.io"

    async def create_conversation(self, user_id: str, message: str) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/conversations",
            headers=self._headers(),
            json={"from": {"type": "user", "id": user_id}, "body": message},
        )
        return response.json()

    async def send_message(
        self,
        conversation_id: str,
        body: str,
        message_type: str = "comment",
    ) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/conversations/{conversation_id}/reply",
            headers=self._headers(),
            json={
                "message_type": message_type,
                "type": "admin",
                "body": body,
            },
        )
        return response.json()

    async def list_contacts(self, limit: int = 25) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/contacts",
            headers=self._headers(),
            params={"per_page": limit},
        )
        return response.json()

    async def create_contact(self, email: str, name: str = None, phone: str = None) -> dict:
        data: dict = {"email": email}
        if name:
            data["name"] = name
        if phone:
            data["phone"] = phone
        response = await self.client.post(
            f"{self.BASE_URL}/contacts",
            headers=self._headers(),
            json=data,
        )
        return response.json()

    async def add_tag(self, tag_name: str, contact_id: str) -> dict:
        tag_resp = await self.client.post(
            f"{self.BASE_URL}/tags",
            headers=self._headers(),
            json={"name": tag_name},
        )
        tag_data = tag_resp.json()
        tag_id = tag_data.get("id")
        if not tag_id:
            logger.warning("IntercomIntegration.add_tag: could not create/find tag %r", tag_name)
            return tag_data
        response = await self.client.post(
            f"{self.BASE_URL}/contacts/{contact_id}/tags",
            headers=self._headers(),
            json={"id": tag_id},
        )
        return response.json()


# ============================================================
# TYPEFORM INTEGRATION
# ============================================================


class TypeformIntegration(IntegrationBase):
    BASE_URL = "https://api.typeform.com"

    async def list_forms(self, page_size: int = 25) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/forms",
            headers=self._headers(),
            params={"page_size": page_size},
        )
        return response.json()

    async def get_responses(self, form_id: str, page_size: int = 25) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/forms/{form_id}/responses",
            headers=self._headers(),
            params={"page_size": page_size},
        )
        return response.json()

    async def get_form(self, form_id: str) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/forms/{form_id}",
            headers=self._headers(),
        )
        return response.json()

    async def create_form(self, title: str, fields: list = None) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/forms",
            headers=self._headers(),
            json={"title": title, "fields": fields or []},
        )
        return response.json()

    async def delete_response(self, form_id: str, response_id: str) -> dict:
        response = await self.client.delete(
            f"{self.BASE_URL}/forms/{form_id}/responses",
            headers=self._headers(),
            params={"included_response_ids": response_id},
        )
        return {"status_code": response.status_code}


# ============================================================
# MICROSOFT TEAMS INTEGRATION
# ============================================================


class MicrosoftTeamsIntegration(IntegrationBase):
    BASE_URL = "https://graph.microsoft.com/v1.0"

    async def send_message(self, team_id: str, channel_id: str, content: str) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/teams/{team_id}/channels/{channel_id}/messages",
            headers=self._headers(),
            json={"body": {"content": content}},
        )
        return response.json()

    async def list_channels(self, team_id: str) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/teams/{team_id}/channels",
            headers=self._headers(),
        )
        return response.json()

    async def create_channel(self, team_id: str, display_name: str, description: str = "") -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/teams/{team_id}/channels",
            headers=self._headers(),
            json={"displayName": display_name, "description": description},
        )
        return response.json()

    async def list_teams(self) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/me/joinedTeams",
            headers=self._headers(),
        )
        return response.json()

    async def post_card(self, team_id: str, channel_id: str, card: dict) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/teams/{team_id}/channels/{channel_id}/messages",
            headers=self._headers(),
            json={
                "body": {"contentType": "html", "content": "<attachment id='card1'></attachment>"},
                "attachments": [
                    {
                        "id": "card1",
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": card,
                    }
                ],
            },
        )
        return response.json()


# ============================================================
# VELOCITY INTEGRATION
# ============================================================


class VelocityIntegration(IntegrationBase):
    BASE_URL = "https://api.velocity.us/v2"

    async def create_meeting(
        self,
        topic: str,
        start_time: str,
        duration: int = 60,
        timezone: str = "UTC",
        agenda: str = "",
    ) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/users/me/meetings",
            headers=self._headers(),
            json={
                "topic": topic,
                "type": 2,
                "start_time": start_time,
                "duration": duration,
                "timezone": timezone,
                "agenda": agenda,
            },
        )
        return response.json()

    async def list_meetings(self, user_id: str = "me", type: str = "scheduled") -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/users/{user_id}/meetings",
            headers=self._headers(),
            params={"type": type},
        )
        return response.json()

    async def delete_meeting(self, meeting_id: str) -> dict:
        response = await self.client.delete(
            f"{self.BASE_URL}/meetings/{meeting_id}",
            headers=self._headers(),
        )
        return {"status_code": response.status_code}

    async def get_recording(self, meeting_id: str) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/meetings/{meeting_id}/recordings",
            headers=self._headers(),
        )
        return response.json()

    async def list_participants(self, meeting_id: str) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/past_meetings/{meeting_id}/participants",
            headers=self._headers(),
        )
        return response.json()


# ============================================================
# SHOPIFY INTEGRATION
# ============================================================


class ShopifyIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        super().__init__(access_token)
        parts = access_token.split(":", 1)
        if len(parts) != 2:
            raise ValueError("ShopifyIntegration access_token must be 'token:shop_domain'")
        self._token, self._shop = parts
        self.base_url = f"https://{self._shop}/admin/api/2024-01"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "X-Shopify-Access-Token": self._token,
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    async def list_orders(self, limit: int = 25, status: str = "open") -> dict:
        response = await self.client.get(
            f"{self.base_url}/orders.json",
            headers=self._headers(),
            params={"limit": limit, "status": status},
        )
        return response.json()

    async def get_order(self, order_id: str) -> dict:
        response = await self.client.get(
            f"{self.base_url}/orders/{order_id}.json",
            headers=self._headers(),
        )
        return response.json()

    async def list_products(self, limit: int = 25) -> dict:
        response = await self.client.get(
            f"{self.base_url}/products.json",
            headers=self._headers(),
            params={"limit": limit},
        )
        return response.json()

    async def create_product(
        self,
        title: str,
        body_html: str = "",
        vendor: str = "",
        product_type: str = "",
        variants: list = None,
    ) -> dict:
        product: dict = {"title": title, "body_html": body_html, "vendor": vendor, "product_type": product_type}
        if variants:
            product["variants"] = variants
        response = await self.client.post(
            f"{self.base_url}/products.json",
            headers=self._headers(),
            json={"product": product},
        )
        return response.json()

    async def update_inventory(
        self,
        inventory_item_id: str,
        location_id: str,
        available: int,
    ) -> dict:
        response = await self.client.post(
            f"{self.base_url}/inventory_levels/set.json",
            headers=self._headers(),
            json={
                "inventory_item_id": inventory_item_id,
                "location_id": location_id,
                "available": available,
            },
        )
        return response.json()


# ============================================================
# RESEND INTEGRATION
# ============================================================


class ResendIntegration(IntegrationBase):
    BASE_URL = "https://api.resend.com"

    async def send_email(
        self,
        from_addr: str,
        to: str,
        subject: str,
        html: str = None,
        text: str = None,
    ) -> dict:
        payload: dict = {"from": from_addr, "to": to, "subject": subject}
        if html:
            payload["html"] = html
        if text:
            payload["text"] = text
        response = await self.client.post(
            f"{self.BASE_URL}/emails",
            headers=self._headers(),
            json=payload,
        )
        return response.json()

    async def batch_send(self, emails: list) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/emails/batch",
            headers=self._headers(),
            json=emails,
        )
        return response.json()

    async def get_email(self, email_id: str) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/emails/{email_id}",
            headers=self._headers(),
        )
        return response.json()

    async def cancel_email(self, email_id: str) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/emails/{email_id}/cancel",
            headers=self._headers(),
        )
        return response.json()

    async def list_domains(self) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/domains",
            headers=self._headers(),
        )
        return response.json()


# ============================================================
# TELEGRAM INTEGRATION
# ============================================================


class TelegramIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = f"https://api.telegram.org/bot{access_token}"
        self.client = httpx.AsyncClient(timeout=30.0)

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if extra:
            headers.update(extra)
        return headers

    async def send_message(self, chat_id: str, text: str, parse_mode: str = "HTML") -> dict:
        response = await self.client.post(
            f"{self.base_url}/sendMessage",
            headers=self._headers(),
            json={"chat_id": chat_id, "text": text, "parse_mode": parse_mode},
        )
        return response.json()

    async def send_photo(self, chat_id: str, photo_url: str, caption: str = None) -> dict:
        payload: dict = {"chat_id": chat_id, "photo": photo_url}
        if caption:
            payload["caption"] = caption
        response = await self.client.post(
            f"{self.base_url}/sendPhoto",
            headers=self._headers(),
            json=payload,
        )
        return response.json()

    async def send_document(self, chat_id: str, document_url: str, caption: str = None) -> dict:
        payload: dict = {"chat_id": chat_id, "document": document_url}
        if caption:
            payload["caption"] = caption
        response = await self.client.post(
            f"{self.base_url}/sendDocument",
            headers=self._headers(),
            json=payload,
        )
        return response.json()

    async def pin_message(self, chat_id: str, message_id: str) -> dict:
        response = await self.client.post(
            f"{self.base_url}/pinChatMessage",
            headers=self._headers(),
            json={"chat_id": chat_id, "message_id": message_id},
        )
        return response.json()

    async def create_poll(self, chat_id: str, question: str, options: list) -> dict:
        response = await self.client.post(
            f"{self.base_url}/sendPoll",
            headers=self._headers(),
            json={"chat_id": chat_id, "question": question, "options": options},
        )
        return response.json()


# ============================================================
# PIPEDRIVE INTEGRATION
# ============================================================


class PipedriveIntegration(IntegrationBase):
    BASE_URL = "https://api.pipedrive.com/v1"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if extra:
            headers.update(extra)
        return headers

    def _params(self, extra: dict = None) -> dict:
        params = {"api_token": self.access_token}
        if extra:
            params.update(extra)
        return params

    async def create_deal(
        self,
        title: str,
        person_id: str = None,
        org_id: str = None,
        value: float = None,
        currency: str = "USD",
        status: str = "open",
    ) -> dict:
        data: dict = {"title": title, "currency": currency, "status": status}
        if person_id:
            data["person_id"] = person_id
        if org_id:
            data["org_id"] = org_id
        if value is not None:
            data["value"] = value
        response = await self.client.post(
            f"{self.BASE_URL}/deals",
            headers=self._headers(),
            params=self._params(),
            json=data,
        )
        return response.json()

    async def update_deal(
        self,
        deal_id: str,
        title: str = None,
        status: str = None,
        stage_id: str = None,
    ) -> dict:
        data: dict = {}
        if title is not None:
            data["title"] = title
        if status is not None:
            data["status"] = status
        if stage_id is not None:
            data["stage_id"] = stage_id
        response = await self.client.put(
            f"{self.BASE_URL}/deals/{deal_id}",
            headers=self._headers(),
            params=self._params(),
            json=data,
        )
        return response.json()

    async def list_deals(self, status: str = "open", limit: int = 25) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/deals",
            headers=self._headers(),
            params=self._params({"status": status, "limit": limit}),
        )
        return response.json()

    async def create_person(
        self,
        name: str,
        email: str = None,
        phone: str = None,
        org_id: str = None,
    ) -> dict:
        data: dict = {"name": name}
        if email:
            data["email"] = [{"value": email, "primary": True}]
        if phone:
            data["phone"] = [{"value": phone, "primary": True}]
        if org_id:
            data["org_id"] = org_id
        response = await self.client.post(
            f"{self.BASE_URL}/persons",
            headers=self._headers(),
            params=self._params(),
            json=data,
        )
        return response.json()

    async def add_activity(
        self,
        subject: str,
        type: str = "call",
        deal_id: str = None,
        person_id: str = None,
        due_date: str = None,
    ) -> dict:
        data: dict = {"subject": subject, "type": type}
        if deal_id:
            data["deal_id"] = deal_id
        if person_id:
            data["person_id"] = person_id
        if due_date:
            data["due_date"] = due_date
        response = await self.client.post(
            f"{self.BASE_URL}/activities",
            headers=self._headers(),
            params=self._params(),
            json=data,
        )
        return response.json()


# ============================================================
# MONDAY INTEGRATION
# ============================================================


class MondayIntegration(IntegrationBase):
    BASE_URL = "https://api.monday.com/v2"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": self.access_token,
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    async def _gql(self, query: str, variables: dict = None) -> dict:
        response = await self.client.post(
            self.BASE_URL,
            headers=self._headers(),
            json={"query": query, "variables": variables or {}},
        )
        return response.json()

    async def create_item(
        self,
        board_id: str,
        item_name: str,
        column_values: dict = None,
    ) -> dict:
        import json as _json

        mutation = """
        mutation CreateItem($boardId: ID!, $itemName: String!, $columnValues: JSON) {
            create_item(board_id: $boardId, item_name: $itemName, column_values: $columnValues) {
                id name
            }
        }
        """
        variables: dict = {"boardId": board_id, "itemName": item_name}
        if column_values:
            variables["columnValues"] = _json.dumps(column_values)
        return await self._gql(mutation, variables)

    async def update_item(self, item_id: str, column_id: str, value: str) -> dict:
        mutation = """
        mutation UpdateItem($boardId: ID!, $itemId: ID!, $columnId: String!, $value: String!) {
            change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
                id
            }
        }
        """
        return await self._gql(
            mutation,
            {"itemId": item_id, "columnId": column_id, "value": value, "boardId": ""},
        )

    async def list_items(self, board_id: str, limit: int = 25) -> dict:
        query = """
        query ListItems($boardId: [ID!]!, $limit: Int) {
            boards(ids: $boardId) {
                items_page(limit: $limit) {
                    items { id name }
                }
            }
        }
        """
        return await self._gql(query, {"boardId": [board_id], "limit": limit})

    async def create_board(self, name: str, board_kind: str = "public") -> dict:
        mutation = """
        mutation CreateBoard($boardName: String!, $boardKind: BoardKind!) {
            create_board(board_name: $boardName, board_kind: $boardKind) {
                id name
            }
        }
        """
        return await self._gql(mutation, {"boardName": name, "boardKind": board_kind})

    async def move_item_to_group(self, item_id: str, group_id: str) -> dict:
        mutation = """
        mutation MoveItem($itemId: ID!, $groupId: String!) {
            move_item_to_group(item_id: $itemId, group_id: $groupId) {
                id
            }
        }
        """
        return await self._gql(mutation, {"itemId": item_id, "groupId": group_id})


# ============================================================
# CLICKUP INTEGRATION
# ============================================================


class ClickUpIntegration(IntegrationBase):
    BASE_URL = "https://api.clickup.com/api/v2"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": self.access_token,
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    async def create_task(
        self,
        list_id: str,
        name: str,
        description: str = "",
        priority: int = None,
        due_date: int = None,
    ) -> dict:
        data: dict = {"name": name, "description": description}
        if priority is not None:
            data["priority"] = priority
        if due_date is not None:
            data["due_date"] = due_date
        response = await self.client.post(
            f"{self.BASE_URL}/list/{list_id}/task",
            headers=self._headers(),
            json=data,
        )
        return response.json()

    async def update_task(
        self,
        task_id: str,
        name: str = None,
        description: str = None,
        status: str = None,
        priority: int = None,
    ) -> dict:
        data: dict = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if status is not None:
            data["status"] = status
        if priority is not None:
            data["priority"] = priority
        response = await self.client.put(
            f"{self.BASE_URL}/task/{task_id}",
            headers=self._headers(),
            json=data,
        )
        return response.json()

    async def list_tasks(self, list_id: str, archived: bool = False) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/list/{list_id}/task",
            headers=self._headers(),
            params={"archived": str(archived).lower()},
        )
        return response.json()

    async def create_list(self, folder_id: str, name: str, content: str = "") -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/folder/{folder_id}/list",
            headers=self._headers(),
            json={"name": name, "content": content},
        )
        return response.json()

    async def add_comment(self, task_id: str, comment_text: str) -> dict:
        response = await self.client.post(
            f"{self.BASE_URL}/task/{task_id}/comment",
            headers=self._headers(),
            json={"comment_text": comment_text},
        )
        return response.json()


# ============================================================
# WORDPRESS INTEGRATION
# ============================================================


class WordPressIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        super().__init__(access_token)
        parts = access_token.split(":", 2)
        if len(parts) != 3:
            raise ValueError("WordPressIntegration access_token must be 'username:application_password:site_url'")
        username, app_password, site_url = parts
        encoded = base64.b64encode(f"{username}:{app_password}".encode()).decode()
        self._auth_header = f"Basic {encoded}"
        self.api_base = f"{site_url.rstrip('/')}/wp-json/wp/v2"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": self._auth_header,
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    async def create_post(
        self,
        title: str,
        content: str,
        status: str = "draft",
        categories: list = None,
        tags: list = None,
    ) -> dict:
        data: dict = {"title": title, "content": content, "status": status}
        if categories:
            data["categories"] = categories
        if tags:
            data["tags"] = tags
        response = await self.client.post(
            f"{self.api_base}/posts",
            headers=self._headers(),
            json=data,
        )
        return response.json()

    async def update_post(
        self,
        post_id: str,
        title: str = None,
        content: str = None,
        status: str = None,
    ) -> dict:
        data: dict = {}
        if title is not None:
            data["title"] = title
        if content is not None:
            data["content"] = content
        if status is not None:
            data["status"] = status
        response = await self.client.put(
            f"{self.api_base}/posts/{post_id}",
            headers=self._headers(),
            json=data,
        )
        return response.json()

    async def list_posts(self, per_page: int = 25, status: str = "publish") -> dict:
        response = await self.client.get(
            f"{self.api_base}/posts",
            headers=self._headers(),
            params={"per_page": per_page, "status": status},
        )
        return response.json()

    async def create_page(self, title: str, content: str, status: str = "draft") -> dict:
        response = await self.client.post(
            f"{self.api_base}/pages",
            headers=self._headers(),
            json={"title": title, "content": content, "status": status},
        )
        return response.json()

    async def upload_media(
        self,
        filename: str,
        file_content: bytes,
        mime_type: str = "image/jpeg",
    ) -> dict:
        headers = {
            "Authorization": self._auth_header,
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": mime_type,
        }
        response = await self.client.post(
            f"{self.api_base}/media",
            headers=headers,
            content=file_content,
        )
        return response.json()


# ============================================================
# SUPABASE INTEGRATION
# ============================================================


class SupabaseIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        super().__init__(access_token)
        parts = access_token.split(":", 1)
        if len(parts) != 2:
            raise ValueError("SupabaseIntegration access_token must be 'anon_key:project_ref'")
        self._anon_key, self._project_ref = parts
        self.base_url = f"https://{self._project_ref}.supabase.co/rest/v1"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "apikey": self._anon_key,
            "Authorization": f"Bearer {self._anon_key}",
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    async def run_query(
        self,
        table: str,
        select: str = "*",
        filters: dict = None,
        limit: int = 25,
    ) -> dict:
        params: dict = {"select": select, "limit": limit}
        headers = self._headers()
        if filters:
            for col, val in filters.items():
                params[col] = f"eq.{val}"
        response = await self.client.get(
            f"{self.base_url}/{table}",
            headers=headers,
            params=params,
        )
        return {"rows": response.json()}

    async def insert_row(self, table: str, data: dict) -> dict:
        response = await self.client.post(
            f"{self.base_url}/{table}",
            headers=self._headers({"Prefer": "return=representation"}),
            json=data,
        )
        return response.json()

    async def update_rows(self, table: str, data: dict, filters: dict) -> dict:
        params: dict = {}
        for col, val in filters.items():
            params[col] = f"eq.{val}"
        response = await self.client.patch(
            f"{self.base_url}/{table}",
            headers=self._headers({"Prefer": "return=representation"}),
            params=params,
            json=data,
        )
        return response.json()

    async def delete_rows(self, table: str, filters: dict) -> dict:
        params: dict = {}
        for col, val in filters.items():
            params[col] = f"eq.{val}"
        response = await self.client.delete(
            f"{self.base_url}/{table}",
            headers=self._headers(),
            params=params,
        )
        return {"status_code": response.status_code}

    async def invoke_function(self, function_name: str, body: dict = None) -> dict:
        url = f"https://{self._project_ref}.supabase.co/functions/v1/{function_name}"
        response = await self.client.post(
            url,
            headers=self._headers(),
            json=body or {},
        )
        return response.json()


# ============================================================
# FIREBASE INTEGRATION
# ============================================================


class FirebaseIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        super().__init__(access_token)
        parts = access_token.split(":", 1)
        if len(parts) != 2:
            raise ValueError("FirebaseIntegration access_token must be 'service_account_token:project_id'")
        self._token, self._project_id = parts
        self.base_url = f"https://firestore.googleapis.com/v1/projects/{self._project_id}/databases/(default)/documents"

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    def _to_firestore(self, data: dict) -> dict:
        """Convert a plain dict to Firestore field value format."""
        fields = {}
        for key, value in data.items():
            if isinstance(value, str):
                fields[key] = {"stringValue": value}
            elif isinstance(value, bool):
                fields[key] = {"booleanValue": value}
            elif isinstance(value, int):
                fields[key] = {"integerValue": str(value)}
            elif isinstance(value, float):
                fields[key] = {"doubleValue": value}
            elif value is None:
                fields[key] = {"nullValue": None}
            else:
                fields[key] = {"stringValue": str(value)}
        return {"fields": fields}

    async def read_document(self, collection: str, document_id: str) -> dict:
        response = await self.client.get(
            f"{self.base_url}/{collection}/{document_id}",
            headers=self._headers(),
        )
        return response.json()

    async def write_document(self, collection: str, document_id: str, data: dict) -> dict:
        response = await self.client.patch(
            f"{self.base_url}/{collection}/{document_id}",
            headers=self._headers(),
            json=self._to_firestore(data),
        )
        return response.json()

    async def list_collection(self, collection: str, limit: int = 25) -> dict:
        response = await self.client.get(
            f"{self.base_url}/{collection}",
            headers=self._headers(),
            params={"pageSize": limit},
        )
        return response.json()

    async def delete_document(self, collection: str, document_id: str) -> dict:
        response = await self.client.delete(
            f"{self.base_url}/{collection}/{document_id}",
            headers=self._headers(),
        )
        return {"status_code": response.status_code}

    async def run_query(self, collection: str, filters: dict = None) -> dict:
        body: dict = {
            "structuredQuery": {
                "from": [{"collectionId": collection}],
            }
        }
        if filters:
            conditions = []
            for field, value in filters.items():
                conditions.append(
                    {
                        "fieldFilter": {
                            "field": {"fieldPath": field},
                            "op": "EQUAL",
                            "value": {"stringValue": str(value)},
                        }
                    }
                )
            if len(conditions) == 1:
                body["structuredQuery"]["where"] = conditions[0]
            else:
                body["structuredQuery"]["where"] = {"compositeFilter": {"op": "AND", "filters": conditions}}
        response = await self.client.post(
            f"{self.base_url}:runQuery",
            headers=self._headers(),
            json=body,
        )
        return response.json()


# ============================================================
# RSS FEED INTEGRATION
# ============================================================


class RssFeedIntegration(IntegrationBase):
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.client = httpx.AsyncClient(timeout=30.0)

    def _headers(self, extra: dict[str, str] = None) -> dict[str, str]:
        headers: dict = {}
        if extra:
            headers.update(extra)
        return headers

    def _parse_feed(self, xml_text: str) -> dict:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.warning("RssFeedIntegration: XML parse error: %s", exc)
            return {"title": "", "link": "", "items": []}

        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # Atom feed
        if root.tag == "{http://www.w3.org/2005/Atom}feed":
            title = root.findtext("atom:title", default="", namespaces=ns)
            link_el = root.find("atom:link[@rel='alternate']", ns) or root.find("atom:link", ns)
            link = link_el.get("href", "") if link_el is not None else ""
            items = []
            for entry in root.findall("atom:entry", ns):
                item_link_el = entry.find("atom:link[@rel='alternate']", ns) or entry.find("atom:link", ns)
                items.append(
                    {
                        "title": entry.findtext("atom:title", default="", namespaces=ns),
                        "link": item_link_el.get("href", "") if item_link_el is not None else "",
                        "description": entry.findtext("atom:summary", default="", namespaces=ns),
                        "published": entry.findtext("atom:published", default="", namespaces=ns),
                    }
                )
            return {"title": title, "link": link, "items": items}

        # RSS 2.0
        channel = root.find("channel")
        if channel is None:
            return {"title": "", "link": "", "items": []}
        title = channel.findtext("title", default="")
        link = channel.findtext("link", default="")
        items = []
        for item in channel.findall("item"):
            items.append(
                {
                    "title": item.findtext("title", default=""),
                    "link": item.findtext("link", default=""),
                    "description": item.findtext("description", default=""),
                    "published": item.findtext("pubDate", default=""),
                }
            )
        return {"title": title, "link": link, "items": items}

    async def fetch_feed(self, url: str) -> dict:
        response = await self.client.get(url, headers=self._headers())
        response.raise_for_status()
        return self._parse_feed(response.text)

    async def get_latest_items(self, url: str, limit: int = 10) -> dict:
        feed = await self.fetch_feed(url)
        return {"items": feed["items"][:limit], "feed_title": feed["title"]}

    async def search_items(self, url: str, query: str) -> dict:
        feed = await self.fetch_feed(url)
        q = query.lower()
        matched = [
            item
            for item in feed["items"]
            if q in item.get("title", "").lower() or q in item.get("description", "").lower()
        ]
        return {"items": matched, "feed_title": feed["title"]}


# ============================================================
# YOUTUBE INTEGRATION
# ============================================================


class YouTubeIntegration(IntegrationBase):
    BASE_URL = "https://www.googleapis.com/youtube/v3"

    async def upload_video(
        self,
        title: str,
        description: str = "",
        tags: list = None,
        privacy_status: str = "private",
    ) -> dict:
        return {
            "error": "Video upload requires multipart upload. Use the YouTube Studio API directly.",
            "docs": "https://developers.google.com/youtube/v3/guides/uploading_a_video",
        }

    async def list_videos(self, channel_id: str = None, max_results: int = 25) -> dict:
        params: dict = {
            "part": "snippet",
            "type": "video",
            "maxResults": max_results,
        }
        if channel_id:
            params["channelId"] = channel_id
        response = await self.client.get(
            f"{self.BASE_URL}/search",
            headers=self._headers(),
            params=params,
        )
        return response.json()

    async def get_video(self, video_id: str) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/videos",
            headers=self._headers(),
            params={"part": "snippet,statistics", "id": video_id},
        )
        return response.json()

    async def update_video(
        self,
        video_id: str,
        title: str = None,
        description: str = None,
        tags: list = None,
    ) -> dict:
        snippet: dict = {"categoryId": "22"}
        if title is not None:
            snippet["title"] = title
        if description is not None:
            snippet["description"] = description
        if tags is not None:
            snippet["tags"] = tags
        response = await self.client.put(
            f"{self.BASE_URL}/videos",
            headers=self._headers(),
            params={"part": "snippet"},
            json={"id": video_id, "snippet": snippet},
        )
        return response.json()

    async def list_comments(self, video_id: str, max_results: int = 25) -> dict:
        response = await self.client.get(
            f"{self.BASE_URL}/commentThreads",
            headers=self._headers(),
            params={
                "part": "snippet",
                "videoId": video_id,
                "maxResults": max_results,
            },
        )
        return response.json()


# ============================================================
# GENERIC INTEGRATION STUB
# ============================================================


class GenericIntegration(IntegrationBase):
    """
    Placeholder for integrations whose full connector module is not yet
    implemented. Connection and auth storage work normally; action
    execution raises NotImplementedError so callers can route through
    the Zapier/MCP integration hub instead.
    """

    async def execute(self, action: str, **kwargs):
        raise NotImplementedError(
            f"Direct execution is not yet available for this integration. Use the Zapier or MCP hub to run '{action}'."
        )


# ============================================================
# INTEGRATION REGISTRY
# ============================================================

INTEGRATION_REGISTRY = {
    "slack": {
        "class": SlackIntegration,
        "auth_type": "oauth",
        "provider": "slack",
        "actions": ["send_message", "list_channels", "get_user_info"],
        "description": "Send messages and interact with Slack workspaces",
    },
    "notion": {
        "class": NotionIntegration,
        "auth_type": "oauth",
        "provider": "notion",
        "actions": ["create_page", "query_database", "search"],
        "description": "Create and manage Notion pages and databases",
    },
    "google_sheets": {
        "class": GoogleSheetsIntegration,
        "auth_type": "oauth",
        "provider": "google",
        "actions": ["read_range", "write_range", "append_rows", "get_spreadsheet_info"],
        "description": "Read and write Google Sheets data",
    },
    "airtable": {
        "class": AirtableIntegration,
        "auth_type": "api_key",
        "actions": ["list_records", "create_record", "update_record"],
        "description": "Manage Airtable bases and records",
    },
    "github": {
        "class": GitHubIntegration,
        "auth_type": "oauth",
        "provider": "github",
        "actions": ["create_issue", "create_comment", "list_repos"],
        "description": "Automate GitHub issues and PRs",
    },
    "openai": {
        "class": OpenAIIntegration,
        "auth_type": "api_key",
        "actions": ["chat_completion", "generate_embedding"],
        "description": "Add AI processing to your workflows",
    },
    "twilio": {
        "class": TwilioIntegration,
        "auth_type": "api_key",
        "actions": ["send_sms"],
        "description": "Send SMS messages",
    },
    "hubspot": {
        "class": HubSpotIntegration,
        "auth_type": "oauth",
        "provider": "hubspot",
        "actions": ["create_contact", "list_contacts", "create_deal"],
        "description": "Manage HubSpot CRM contacts and deals",
    },
    "trello": {
        "class": TrelloIntegration,
        "auth_type": "oauth",
        "provider": "trello",
        "actions": ["create_card", "list_boards"],
        "description": "Create cards and manage Trello boards",
    },
    "linear": {
        "class": LinearIntegration,
        "auth_type": "api_key",
        "provider": "linear",
        "actions": ["create_issue", "update_issue", "list_issues", "list_teams", "list_projects"],
        "description": "Create and track Linear issues and projects",
    },
    "jira": {
        "class": JiraIntegration,
        "auth_type": "api_key",
        "provider": "jira",
        "actions": ["create_issue", "update_issue", "list_issues", "add_comment", "transition_issue"],
        "description": "Manage Jira issues, sprints, and projects",
    },
    "asana": {
        "class": AsanaIntegration,
        "auth_type": "oauth",
        "provider": "asana",
        "actions": ["create_task", "update_task", "list_tasks", "create_project", "add_comment"],
        "description": "Create and manage Asana tasks and projects",
    },
    "zendesk": {
        "class": ZendeskIntegration,
        "auth_type": "api_key",
        "provider": "zendesk",
        "actions": ["create_ticket", "update_ticket", "list_tickets", "add_comment", "close_ticket"],
        "description": "Manage Zendesk support tickets and customers",
    },
    "google_calendar": {
        "class": GoogleCalendarIntegration,
        "auth_type": "oauth",
        "provider": "google",
        "actions": ["create_event", "list_events", "update_event", "delete_event", "get_calendar"],
        "description": "Create and manage Google Calendar events",
    },
    "gmail": {
        "class": GmailIntegration,
        "auth_type": "oauth",
        "provider": "google",
        "actions": ["send_email", "list_emails", "get_email", "reply_email", "label_email"],
        "description": "Send and manage Gmail emails",
    },
    "google_drive": {
        "class": GoogleDriveIntegration,
        "auth_type": "oauth",
        "provider": "google",
        "actions": ["upload_file", "download_file", "list_files", "create_folder", "share_file"],
        "description": "Upload, download, and manage Google Drive files",
    },
    "dropbox": {
        "class": DropboxIntegration,
        "auth_type": "oauth",
        "provider": "dropbox",
        "actions": ["upload_file", "download_file", "list_files", "create_folder", "share_link"],
        "description": "Store and sync files with Dropbox",
    },
    "webhook": {
        "class": WebhookIntegration,
        "auth_type": "none",
        "provider": "webhook",
        "actions": ["send_post", "send_get", "send_put", "send_delete"],
        "description": "Send data to any URL via HTTP webhooks",
    },
    "http_request": {
        "class": HttpRequestIntegration,
        "auth_type": "none",
        "provider": "http",
        "actions": ["get", "post", "put", "patch", "delete"],
        "description": "Make arbitrary HTTP requests to any API",
    },
    "postgresql": {
        "class": PostgreSQLIntegration,
        "auth_type": "api_key",
        "provider": "postgresql",
        "actions": ["run_query", "insert_row", "update_rows", "delete_rows", "list_tables"],
        "description": "Query and write to PostgreSQL databases",
    },
    "mysql": {
        "class": MySQLIntegration,
        "auth_type": "api_key",
        "provider": "mysql",
        "actions": ["run_query", "insert_row", "update_rows", "delete_rows", "list_tables"],
        "description": "Query and write to MySQL databases",
    },
    "salesforce": {
        "class": SalesforceIntegration,
        "auth_type": "oauth",
        "provider": "salesforce",
        "actions": ["create_lead", "update_lead", "create_opportunity", "list_contacts", "run_soql"],
        "description": "Manage Salesforce leads, contacts, and opportunities",
    },
    "intercom": {
        "class": IntercomIntegration,
        "auth_type": "api_key",
        "provider": "intercom",
        "actions": ["create_conversation", "send_message", "list_contacts", "create_contact", "add_tag"],
        "description": "Manage Intercom conversations and customer data",
    },
    "typeform": {
        "class": TypeformIntegration,
        "auth_type": "api_key",
        "provider": "typeform",
        "actions": ["list_forms", "get_responses", "get_form", "create_form", "delete_response"],
        "description": "Collect and retrieve Typeform survey responses",
    },
    "microsoft_teams": {
        "class": MicrosoftTeamsIntegration,
        "auth_type": "oauth",
        "provider": "microsoft",
        "actions": ["send_message", "list_channels", "create_channel", "list_teams", "post_card"],
        "description": "Send messages and manage Microsoft Teams channels",
    },
    "velocity": {
        "class": VelocityIntegration,
        "auth_type": "oauth",
        "provider": "velocity",
        "actions": ["create_meeting", "list_meetings", "delete_meeting", "get_recording", "list_participants"],
        "description": "Create and manage Velocity meetings and recordings",
    },
    "shopify": {
        "class": ShopifyIntegration,
        "auth_type": "api_key",
        "provider": "shopify",
        "actions": ["list_orders", "get_order", "list_products", "create_product", "update_inventory"],
        "description": "Manage Shopify store orders, products, and inventory",
    },
    "resend": {
        "class": ResendIntegration,
        "auth_type": "api_key",
        "provider": "resend",
        "actions": ["send_email", "batch_send", "get_email", "cancel_email", "list_domains"],
        "description": "Send transactional emails via Resend",
    },
    "telegram": {
        "class": TelegramIntegration,
        "auth_type": "api_key",
        "provider": "telegram",
        "actions": ["send_message", "send_photo", "send_document", "pin_message", "create_poll"],
        "description": "Send messages and media via Telegram bots",
    },
    "pipedrive": {
        "class": PipedriveIntegration,
        "auth_type": "api_key",
        "provider": "pipedrive",
        "actions": ["create_deal", "update_deal", "list_deals", "create_person", "add_activity"],
        "description": "Manage Pipedrive CRM deals and contacts",
    },
    "monday": {
        "class": MondayIntegration,
        "auth_type": "api_key",
        "provider": "monday",
        "actions": ["create_item", "update_item", "list_items", "create_board", "move_item_to_group"],
        "description": "Create and update Monday.com boards and items",
    },
    "clickup": {
        "class": ClickUpIntegration,
        "auth_type": "api_key",
        "provider": "clickup",
        "actions": ["create_task", "update_task", "list_tasks", "create_list", "add_comment"],
        "description": "Manage ClickUp tasks, lists, and spaces",
    },
    "wordpress": {
        "class": WordPressIntegration,
        "auth_type": "api_key",
        "provider": "wordpress",
        "actions": ["create_post", "update_post", "list_posts", "create_page", "upload_media"],
        "description": "Create and manage WordPress posts and pages",
    },
    "supabase": {
        "class": SupabaseIntegration,
        "auth_type": "api_key",
        "provider": "supabase",
        "actions": ["run_query", "insert_row", "update_rows", "delete_rows", "invoke_function"],
        "description": "Query and write to Supabase tables and functions",
    },
    "firebase": {
        "class": FirebaseIntegration,
        "auth_type": "api_key",
        "provider": "firebase",
        "actions": ["read_document", "write_document", "list_collection", "delete_document", "run_query"],
        "description": "Read and write Firebase Firestore documents",
    },
    "rss_feed": {
        "class": RssFeedIntegration,
        "auth_type": "none",
        "provider": "rss",
        "actions": ["fetch_feed", "get_latest_items", "search_items"],
        "description": "Monitor and parse RSS/Atom feeds for new content",
    },
    "youtube": {
        "class": YouTubeIntegration,
        "auth_type": "oauth",
        "provider": "google",
        "actions": ["upload_video", "list_videos", "get_video", "update_video", "list_comments"],
        "description": "Upload videos and manage YouTube channel content",
    },
}


def get_available_integrations() -> list[dict[str, Any]]:
    """Get list of all available integrations."""
    return [
        {
            "id": key,
            "name": key.replace("_", " ").title(),
            "auth_type": info["auth_type"],
            "actions": info["actions"],
            "description": info["description"],
        }
        for key, info in INTEGRATION_REGISTRY.items()
    ]
