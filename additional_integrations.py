"""
Helix Spirals - Additional Integration Connectors
Additional integration connectors for the Helix Spirals automation platform.
"""

import base64
import json
import logging
from typing import Any

import aiohttp

try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
except ImportError:
    BetaAnalyticsDataClient = None

from .integration_nodes import BaseNode, NodeCategory, NodeResult

logger = logging.getLogger(__name__)

# ============================================================================
# MARKETING & EMAIL NODES
# ============================================================================


class MailchimpNode(BaseNode):
    """Mailchimp email marketing integration."""

    category = NodeCategory.INTEGRATION
    description = "Mailchimp email campaigns and lists"
    icon = "🐵"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key")
            dc = api_key.split("-")[-1] if api_key else "us1"
            operation = self.config.config.get("operation", "get_lists")

            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }

            base_url = f"https://{dc}.api.mailchimp.com/3.0"

            async with aiohttp.ClientSession() as session:
                if operation == "get_lists":
                    async with session.get(f"{base_url}/lists", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "add_member":
                    list_id = self.config.config.get("list_id")
                    member_data = {
                        "email_address": self.config.config.get("email", input_data.get("email", "")),
                        "status": self.config.config.get("status", "subscribed"),
                        "merge_fields": self.config.config.get("merge_fields", {}),
                    }
                    async with session.post(
                        f"{base_url}/lists/{list_id}/members",
                        headers=headers,
                        json=member_data,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_campaign":
                    campaign_data = {
                        "type": self.config.config.get("type", "regular"),
                        "recipients": {"list_id": self.config.config.get("list_id")},
                        "settings": {
                            "subject_line": self.config.config.get("subject", input_data.get("subject", "")),
                            "from_name": self.config.config.get("from_name", ""),
                            "reply_to": self.config.config.get("reply_to", ""),
                        },
                    }
                    async with session.post(f"{base_url}/campaigns", headers=headers, json=campaign_data) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "send_campaign":
                    campaign_id = self.config.config.get("campaign_id", input_data.get("campaign_id"))
                    async with session.post(
                        f"{base_url}/campaigns/{campaign_id}/actions/send",
                        headers=headers,
                    ) as resp:
                        return NodeResult(
                            success=True,
                            data={"status": "sent", "campaign_id": campaign_id},
                        )

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class CalendlyNode(BaseNode):
    """Calendly scheduling integration."""

    category = NodeCategory.INTEGRATION
    description = "Calendly scheduling and events"
    icon = "📅"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key")
            operation = self.config.config.get("operation", "get_events")

            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }

            base_url = "https://api.calendly.com"

            async with aiohttp.ClientSession() as session:
                if operation == "get_user":
                    async with session.get(f"{base_url}/users/me", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_events":
                    user_uri = self.config.config.get("user_uri")
                    params = {
                        "user": user_uri,
                        "count": self.config.config.get("count", 20),
                    }
                    async with session.get(f"{base_url}/scheduled_events", headers=headers, params=params) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_event_types":
                    user_uri = self.config.config.get("user_uri")
                    params = {"user": user_uri}
                    async with session.get(f"{base_url}/event_types", headers=headers, params=params) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "cancel_event":
                    event_uuid = self.config.config.get("event_uuid", input_data.get("event_uuid"))
                    cancel_data = {"reason": self.config.config.get("reason", "Cancelled via Helix Spirals")}
                    async with session.post(
                        f"{base_url}/scheduled_events/{event_uuid}/cancellation",
                        headers=headers,
                        json=cancel_data,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class AirtableNode(BaseNode):
    """Airtable database integration."""

    category = NodeCategory.INTEGRATION
    description = "Airtable database operations"
    icon = "📊"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key")
            base_id = self.config.config.get("base_id")
            table_name = self.config.config.get("table_name")
            operation = self.config.config.get("operation", "list_records")

            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }

            base_url = f"https://api.airtable.com/v0/{base_id}/{table_name}"

            async with aiohttp.ClientSession() as session:
                if operation == "list_records":
                    params = {}
                    if self.config.config.get("view"):
                        params["view"] = self.config.config.get("view")
                    if self.config.config.get("max_records"):
                        params["maxRecords"] = self.config.config.get("max_records")

                    async with session.get(base_url, headers=headers, params=params) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_record":
                    record_id = self.config.config.get("record_id", input_data.get("record_id"))
                    async with session.get(f"{base_url}/{record_id}", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_record":
                    fields = self.config.config.get("fields", input_data.get("fields", {}))
                    async with session.post(base_url, headers=headers, json={"fields": fields}) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "update_record":
                    record_id = self.config.config.get("record_id", input_data.get("record_id"))
                    fields = self.config.config.get("fields", input_data.get("fields", {}))
                    async with session.patch(
                        f"{base_url}/{record_id}",
                        headers=headers,
                        json={"fields": fields},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "delete_record":
                    record_id = self.config.config.get("record_id", input_data.get("record_id"))
                    async with session.delete(f"{base_url}/{record_id}", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# PRODUCTIVITY NODES
# ============================================================================


class AsanaNode(BaseNode):
    """Asana project management integration."""

    category = NodeCategory.INTEGRATION
    description = "Asana tasks and projects"
    icon = "✅"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key")
            operation = self.config.config.get("operation", "get_tasks")

            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }

            base_url = "https://app.asana.com/api/1.0"

            async with aiohttp.ClientSession() as session:
                if operation == "get_workspaces":
                    async with session.get(f"{base_url}/workspaces", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_projects":
                    workspace_gid = self.config.config.get("workspace_gid")
                    async with session.get(
                        f"{base_url}/workspaces/{workspace_gid}/projects",
                        headers=headers,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_tasks":
                    project_gid = self.config.config.get("project_gid")
                    async with session.get(f"{base_url}/projects/{project_gid}/tasks", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_task":
                    task_data = {
                        "data": {
                            "name": self.config.config.get("name", input_data.get("name", "")),
                            "notes": self.config.config.get("notes", input_data.get("notes", "")),
                            "projects": [self.config.config.get("project_gid")],
                            "assignee": self.config.config.get("assignee"),
                            "due_on": self.config.config.get("due_on"),
                        }
                    }
                    async with session.post(f"{base_url}/tasks", headers=headers, json=task_data) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "complete_task":
                    task_gid = self.config.config.get("task_gid", input_data.get("task_gid"))
                    async with session.put(
                        f"{base_url}/tasks/{task_gid}",
                        headers=headers,
                        json={"data": {"completed": True}},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class TrelloNode(BaseNode):
    """Trello board and card management."""

    category = NodeCategory.INTEGRATION
    description = "Trello boards and cards"
    icon = "📋"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key")
            token = self.config.config.get("token")
            operation = self.config.config.get("operation", "get_boards")

            base_url = "https://api.trello.com/1"
            auth_params = f"key={api_key}&token={token}"

            async with aiohttp.ClientSession() as session:
                if operation == "get_boards":
                    async with session.get(f"{base_url}/members/me/boards?{auth_params}") as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_lists":
                    board_id = self.config.config.get("board_id")
                    async with session.get(f"{base_url}/boards/{board_id}/lists?{auth_params}") as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_cards":
                    list_id = self.config.config.get("list_id")
                    async with session.get(f"{base_url}/lists/{list_id}/cards?{auth_params}") as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_card":
                    card_data = {
                        "name": self.config.config.get("name", input_data.get("name", "")),
                        "desc": self.config.config.get("desc", input_data.get("desc", "")),
                        "idList": self.config.config.get("list_id"),
                        "due": self.config.config.get("due"),
                    }
                    async with session.post(f"{base_url}/cards?{auth_params}", json=card_data) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "move_card":
                    card_id = self.config.config.get("card_id", input_data.get("card_id"))
                    list_id = self.config.config.get("list_id", input_data.get("list_id"))
                    async with session.put(f"{base_url}/cards/{card_id}?{auth_params}&idList={list_id}") as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class LinearNode(BaseNode):
    """Linear issue tracking integration."""

    category = NodeCategory.INTEGRATION
    description = "Linear issues and projects"
    icon = "📐"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key")
            operation = self.config.config.get("operation", "get_issues")

            headers = {"Authorization": api_key, "Content-Type": "application/json"}

            base_url = "https://api.linear.app/graphql"

            async with aiohttp.ClientSession() as session:
                if operation == "get_issues":
                    query = """
                    query {
                        issues(first: 50) {
                            nodes {
                                id
                                title
                                description
                                state { name }
                                assignee { name }
                                priority
                                createdAt
                            }
                        }
                    }
                    """
                    async with session.post(base_url, headers=headers, json={"query": query}) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_issue":
                    query = """
                    mutation CreateIssue($input: IssueCreateInput!) {
                        issueCreate(input: $input) {
                            success
                            issue {
                                id
                                title
                            }
                        }
                    }
                    """
                    variables = {
                        "input": {
                            "title": self.config.config.get("title", input_data.get("title", "")),
                            "description": self.config.config.get("description", input_data.get("description", "")),
                            "teamId": self.config.config.get("team_id"),
                            "priority": self.config.config.get("priority", 0),
                        }
                    }
                    async with session.post(
                        base_url,
                        headers=headers,
                        json={"query": query, "variables": variables},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "update_issue":
                    query = """
                    mutation UpdateIssue($id: String!, $input: IssueUpdateInput!) {
                        issueUpdate(id: $id, input: $input) {
                            success
                            issue {
                                id
                                title
                                state { name }
                            }
                        }
                    }
                    """
                    variables = {
                        "id": self.config.config.get("issue_id", input_data.get("issue_id")),
                        "input": self.config.config.get("update", input_data.get("update", {})),
                    }
                    async with session.post(
                        base_url,
                        headers=headers,
                        json={"query": query, "variables": variables},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# SOCIAL MEDIA NODES
# ============================================================================


class TwitterNode(BaseNode):
    """Twitter/X API integration."""

    category = NodeCategory.INTEGRATION
    description = "Twitter/X posts and interactions"
    icon = "🐦"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            bearer_token = self.config.config.get("bearer_token")
            operation = self.config.config.get("operation", "get_tweets")

            headers = {
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json",
            }

            base_url = "https://api.twitter.com/2"

            async with aiohttp.ClientSession() as session:
                if operation == "get_user":
                    username = self.config.config.get("username", input_data.get("username"))
                    async with session.get(f"{base_url}/users/by/username/{username}", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_tweets":
                    user_id = self.config.config.get("user_id", input_data.get("user_id"))
                    async with session.get(f"{base_url}/users/{user_id}/tweets", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "search":
                    query = self.config.config.get("query", input_data.get("query", ""))
                    async with session.get(
                        f"{base_url}/tweets/search/recent?query={query}",
                        headers=headers,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "post_tweet":
                    # Note: Requires OAuth 1.0a for posting
                    tweet_data = {"text": self.config.config.get("text", input_data.get("text", ""))}
                    async with session.post(f"{base_url}/tweets", headers=headers, json=tweet_data) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class LinkedInNode(BaseNode):
    """LinkedIn API integration."""

    category = NodeCategory.INTEGRATION
    description = "LinkedIn posts and profiles"
    icon = "💼"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            access_token = self.config.config.get("access_token")
            operation = self.config.config.get("operation", "get_profile")

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "X-Restli-Protocol-Version": "2.0.0",
            }

            base_url = "https://api.linkedin.com/v2"

            async with aiohttp.ClientSession() as session:
                if operation == "get_profile":
                    async with session.get(f"{base_url}/me", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "share_post":
                    person_urn = self.config.config.get("person_urn")
                    post_data = {
                        "author": person_urn,
                        "lifecycleState": "PUBLISHED",
                        "specificContent": {
                            "com.linkedin.ugc.ShareContent": {
                                "shareCommentary": {"text": self.config.config.get("text", input_data.get("text", ""))},
                                "shareMediaCategory": "NONE",
                            }
                        },
                        "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
                    }
                    async with session.post(f"{base_url}/ugcPosts", headers=headers, json=post_data) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# ANALYTICS NODES
# ============================================================================


class GoogleAnalyticsNode(BaseNode):
    """Google Analytics data integration."""

    category = NodeCategory.INTEGRATION
    description = "Google Analytics reports"
    icon = "📈"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

            property_id = self.config.config.get("property_id")
            operation = self.config.config.get("operation", "run_report")

            client = BetaAnalyticsDataClient()

            if operation == "run_report":
                request = RunReportRequest(
                    property=f"properties/{property_id}",
                    dimensions=[Dimension(name=d) for d in self.config.config.get("dimensions", ["date"])],
                    metrics=[Metric(name=m) for m in self.config.config.get("metrics", ["activeUsers"])],
                    date_ranges=[
                        DateRange(
                            start_date=self.config.config.get("start_date", "7daysAgo"),
                            end_date=self.config.config.get("end_date", "today"),
                        )
                    ],
                )

                response = client.run_report(request)

                rows = []
                for row in response.rows:
                    row_data = {}
                    for i, dim in enumerate(response.dimension_headers):
                        row_data[dim.name] = row.dimension_values[i].value
                    for i, met in enumerate(response.metric_headers):
                        row_data[met.name] = row.metric_values[i].value
                    rows.append(row_data)

                return NodeResult(success=True, data={"rows": rows, "row_count": len(rows)})

            else:
                return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class MixpanelNode(BaseNode):
    """Mixpanel analytics integration."""

    category = NodeCategory.INTEGRATION
    description = "Mixpanel events and analytics"
    icon = "📊"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            token = self.config.config.get("token")
            api_secret = self.config.config.get("api_secret")
            operation = self.config.config.get("operation", "track")

            if operation == "track":
                # Track event
                event_data = {
                    "event": self.config.config.get("event", input_data.get("event", "")),
                    "properties": {
                        "token": token,
                        "distinct_id": self.config.config.get("distinct_id", input_data.get("distinct_id", "")),
                        **self.config.config.get("properties", input_data.get("properties", {})),
                    },
                }

                encoded = base64.b64encode(json.dumps(event_data).encode()).decode()

                async with (
                    aiohttp.ClientSession() as session,
                    session.get(f"https://api.mixpanel.com/track?data={encoded}") as resp,
                ):
                    result = await resp.text()
                    return NodeResult(success=True, data={"status": result})

            elif operation == "query":
                # Query data using JQL
                auth = base64.b64encode(f"{api_secret}:".encode()).decode()
                headers = {"Authorization": f"Basic {auth}"}

                query_data = {
                    "script": self.config.config.get("jql_script", ""),
                    "params": self.config.config.get("params", {}),
                }

                async with (
                    aiohttp.ClientSession() as session,
                    session.post(
                        "https://mixpanel.com/api/2.0/jql",
                        headers=headers,
                        json=query_data,
                    ) as resp,
                ):
                    data = await resp.json()
                    return NodeResult(success=True, data=data)

            else:
                return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# ENTERPRISE CRM & SALES NODES
# ============================================================================


class SalesforceNode(BaseNode):
    """Salesforce CRM integration - contacts, leads, opportunities, custom objects."""

    category = NodeCategory.INTEGRATION
    description = "Salesforce CRM operations"
    icon = "☁️"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            instance_url = self.config.config.get("instance_url")
            access_token = self.config.config.get("access_token")
            operation = self.config.config.get("operation", "query")
            sobject = self.config.config.get("sobject", "Contact")

            # Validate sobject to prevent SOQL injection via interpolation
            import re as _re

            if not _re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", sobject):
                return NodeResult(success=False, error=f"Invalid Salesforce object name: {sobject}")

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            base_url = f"{instance_url}/services/data/v59.0"

            async with aiohttp.ClientSession() as session:
                if operation == "query":
                    # Only use SOQL from the node's static config — never from runtime
                    # input_data to prevent SOQL injection via workflow payloads.
                    soql = self.config.config.get("soql") or f"SELECT Id, Name FROM {sobject} LIMIT 10"
                    async with session.get(f"{base_url}/query", headers=headers, params={"q": soql}) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_record":
                    record_id = self.config.config.get("record_id", input_data.get("record_id"))
                    async with session.get(f"{base_url}/sobjects/{sobject}/{record_id}", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_record":
                    fields = self.config.config.get("fields", input_data.get("fields", {}))
                    async with session.post(f"{base_url}/sobjects/{sobject}", headers=headers, json=fields) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "update_record":
                    record_id = self.config.config.get("record_id", input_data.get("record_id"))
                    fields = self.config.config.get("fields", input_data.get("fields", {}))
                    async with session.patch(
                        f"{base_url}/sobjects/{sobject}/{record_id}",
                        headers=headers,
                        json=fields,
                    ) as resp:
                        return NodeResult(success=True, data={"id": record_id, "updated": True})

                elif operation == "delete_record":
                    record_id = self.config.config.get("record_id", input_data.get("record_id"))
                    async with session.delete(f"{base_url}/sobjects/{sobject}/{record_id}", headers=headers) as resp:
                        return NodeResult(success=True, data={"id": record_id, "deleted": True})

                elif operation == "describe":
                    async with session.get(f"{base_url}/sobjects/{sobject}/describe", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# COMMUNICATION & MESSAGING NODES
# ============================================================================


class MicrosoftTeamsNode(BaseNode):
    """Microsoft Teams messaging and channel management."""

    category = NodeCategory.COMMUNICATION
    description = "Microsoft Teams messages and channels"
    icon = "💬"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            access_token = self.config.config.get("access_token")
            operation = self.config.config.get("operation", "send_message")

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            base_url = "https://graph.microsoft.com/v1.0"

            async with aiohttp.ClientSession() as session:
                if operation == "send_message":
                    team_id = self.config.config.get("team_id")
                    channel_id = self.config.config.get("channel_id")
                    message = {
                        "body": {
                            "contentType": self.config.config.get("content_type", "text"),
                            "content": self.config.config.get("message", input_data.get("message", "")),
                        }
                    }
                    async with session.post(
                        f"{base_url}/teams/{team_id}/channels/{channel_id}/messages",
                        headers=headers,
                        json=message,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "list_channels":
                    team_id = self.config.config.get("team_id")
                    async with session.get(f"{base_url}/teams/{team_id}/channels", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "list_teams":
                    async with session.get(f"{base_url}/me/joinedTeams", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_channel":
                    team_id = self.config.config.get("team_id")
                    channel_data = {
                        "displayName": self.config.config.get("name", input_data.get("name", "")),
                        "description": self.config.config.get("description", ""),
                    }
                    async with session.post(
                        f"{base_url}/teams/{team_id}/channels",
                        headers=headers,
                        json=channel_data,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class IntercomNode(BaseNode):
    """Intercom customer messaging platform integration."""

    category = NodeCategory.COMMUNICATION
    description = "Intercom conversations and contacts"
    icon = "💭"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            access_token = self.config.config.get("access_token")
            operation = self.config.config.get("operation", "list_contacts")

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Intercom-Version": "2.10",
            }
            base_url = "https://api.intercom.io"

            async with aiohttp.ClientSession() as session:
                if operation == "list_contacts":
                    async with session.get(f"{base_url}/contacts", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_contact":
                    contact = {
                        "role": self.config.config.get("role", "user"),
                        "email": self.config.config.get("email", input_data.get("email", "")),
                        "name": self.config.config.get("name", input_data.get("name", "")),
                        "custom_attributes": self.config.config.get("attributes", {}),
                    }
                    async with session.post(f"{base_url}/contacts", headers=headers, json=contact) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "send_message":
                    message = {
                        "from": {
                            "type": "admin",
                            "id": self.config.config.get("admin_id"),
                        },
                        "to": {
                            "type": "user",
                            "id": self.config.config.get("user_id", input_data.get("user_id")),
                        },
                        "message_type": self.config.config.get("message_type", "inapp"),
                        "body": self.config.config.get("body", input_data.get("body", "")),
                    }
                    async with session.post(f"{base_url}/messages", headers=headers, json=message) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_ticket":
                    ticket = {
                        "title": self.config.config.get("title", input_data.get("title", "")),
                        "description": self.config.config.get("description", input_data.get("description", "")),
                        "contacts": [{"id": self.config.config.get("contact_id", input_data.get("contact_id"))}],
                    }
                    async with session.post(f"{base_url}/tickets", headers=headers, json=ticket) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class PagerDutyNode(BaseNode):
    """PagerDuty incident management and alerting."""

    category = NodeCategory.COMMUNICATION
    description = "PagerDuty incidents and alerts"
    icon = "🚨"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key")
            operation = self.config.config.get("operation", "create_incident")

            headers = {
                "Authorization": f"Token token={api_key}",
                "Content-Type": "application/json",
            }
            base_url = "https://api.pagerduty.com"

            async with aiohttp.ClientSession() as session:
                if operation == "create_incident":
                    incident = {
                        "incident": {
                            "type": "incident",
                            "title": self.config.config.get("title", input_data.get("title", "Helix Spirals Alert")),
                            "urgency": self.config.config.get("urgency", "high"),
                            "service": {
                                "id": self.config.config.get("service_id"),
                                "type": "service_reference",
                            },
                            "body": {
                                "type": "incident_body",
                                "details": self.config.config.get("details", input_data.get("details", "")),
                            },
                        }
                    }
                    async with session.post(f"{base_url}/incidents", headers=headers, json=incident) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "list_incidents":
                    params = {
                        "statuses[]": self.config.config.get("statuses", ["triggered", "acknowledged"]),
                    }
                    async with session.get(f"{base_url}/incidents", headers=headers, params=params) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "resolve_incident":
                    incident_id = self.config.config.get("incident_id", input_data.get("incident_id"))
                    resolve_data = {"incident": {"type": "incident", "status": "resolved"}}
                    async with session.put(
                        f"{base_url}/incidents/{incident_id}",
                        headers=headers,
                        json=resolve_data,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "trigger_event":
                    routing_key = self.config.config.get("routing_key")
                    event = {
                        "routing_key": routing_key,
                        "event_action": "trigger",
                        "payload": {
                            "summary": self.config.config.get(
                                "summary",
                                input_data.get("summary", "Alert from Helix Spirals"),
                            ),
                            "severity": self.config.config.get("severity", "critical"),
                            "source": "helix-spirals",
                        },
                    }
                    async with session.post("https://events.pagerduty.com/v2/enqueue", json=event) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# CALENDAR & SCHEDULING NODES
# ============================================================================


class GoogleCalendarNode(BaseNode):
    """Google Calendar event management."""

    category = NodeCategory.INTEGRATION
    description = "Google Calendar events and scheduling"
    icon = "📆"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            access_token = self.config.config.get("access_token")
            operation = self.config.config.get("operation", "list_events")
            calendar_id = self.config.config.get("calendar_id", "primary")

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            base_url = "https://www.googleapis.com/calendar/v3"

            async with aiohttp.ClientSession() as session:
                if operation == "list_events":
                    params = {
                        "maxResults": self.config.config.get("max_results", 10),
                        "singleEvents": "true",
                        "orderBy": "startTime",
                    }
                    if self.config.config.get("time_min"):
                        params["timeMin"] = self.config.config["time_min"]
                    async with session.get(
                        f"{base_url}/calendars/{calendar_id}/events",
                        headers=headers,
                        params=params,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_event":
                    event = {
                        "summary": self.config.config.get("summary", input_data.get("summary", "")),
                        "description": self.config.config.get("description", input_data.get("description", "")),
                        "start": {
                            "dateTime": self.config.config.get("start_time", input_data.get("start_time")),
                            "timeZone": self.config.config.get("timezone", "UTC"),
                        },
                        "end": {
                            "dateTime": self.config.config.get("end_time", input_data.get("end_time")),
                            "timeZone": self.config.config.get("timezone", "UTC"),
                        },
                    }
                    attendees = self.config.config.get("attendees", input_data.get("attendees", []))
                    if attendees:
                        event["attendees"] = [{"email": e} for e in attendees]

                    async with session.post(
                        f"{base_url}/calendars/{calendar_id}/events",
                        headers=headers,
                        json=event,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "delete_event":
                    event_id = self.config.config.get("event_id", input_data.get("event_id"))
                    async with session.delete(
                        f"{base_url}/calendars/{calendar_id}/events/{event_id}",
                        headers=headers,
                    ) as resp:
                        return NodeResult(success=True, data={"deleted": True, "event_id": event_id})

                elif operation == "find_free_busy":
                    body = {
                        "timeMin": self.config.config.get("time_min"),
                        "timeMax": self.config.config.get("time_max"),
                        "items": [{"id": calendar_id}],
                    }
                    async with session.post(f"{base_url}/freeBusy", headers=headers, json=body) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# FORM & SURVEY NODES
# ============================================================================


class TypeformNode(BaseNode):
    """Typeform forms and survey responses."""

    category = NodeCategory.TRIGGER
    description = "Typeform form submissions and responses"
    icon = "📝"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            access_token = self.config.config.get("access_token")
            operation = self.config.config.get("operation", "get_responses")
            form_id = self.config.config.get("form_id")

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            base_url = "https://api.typeform.com"

            async with aiohttp.ClientSession() as session:
                if operation == "get_responses":
                    params = {
                        "page_size": self.config.config.get("page_size", 25),
                    }
                    if self.config.config.get("since"):
                        params["since"] = self.config.config["since"]
                    async with session.get(
                        f"{base_url}/forms/{form_id}/responses",
                        headers=headers,
                        params=params,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_form":
                    async with session.get(f"{base_url}/forms/{form_id}", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "list_forms":
                    async with session.get(f"{base_url}/forms", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_webhook":
                    webhook = {
                        "url": self.config.config.get("webhook_url"),
                        "enabled": True,
                    }
                    tag = self.config.config.get("tag", "helix-spirals")
                    async with session.put(
                        f"{base_url}/forms/{form_id}/webhooks/{tag}",
                        headers=headers,
                        json=webhook,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# FINANCE & ACCOUNTING NODES
# ============================================================================


class QuickBooksNode(BaseNode):
    """QuickBooks Online accounting integration."""

    category = NodeCategory.INTEGRATION
    description = "QuickBooks invoices, payments, customers"
    icon = "💰"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            access_token = self.config.config.get("access_token")
            realm_id = self.config.config.get("realm_id")
            operation = self.config.config.get("operation", "query")

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            base_url = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}"

            async with aiohttp.ClientSession() as session:
                if operation == "query":
                    query = self.config.config.get(
                        "query",
                        input_data.get("query", "SELECT * FROM Customer MAXRESULTS 10"),
                    )
                    async with session.get(f"{base_url}/query", headers=headers, params={"query": query}) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_invoice":
                    invoice = {
                        "CustomerRef": {"value": self.config.config.get("customer_id", input_data.get("customer_id"))},
                        "Line": self.config.config.get("line_items", input_data.get("line_items", [])),
                        "DueDate": self.config.config.get("due_date", input_data.get("due_date")),
                    }
                    async with session.post(f"{base_url}/invoice", headers=headers, json=invoice) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_customer":
                    customer = {
                        "DisplayName": self.config.config.get("name", input_data.get("name", "")),
                        "PrimaryEmailAddr": {"Address": self.config.config.get("email", input_data.get("email", ""))},
                    }
                    async with session.post(f"{base_url}/customer", headers=headers, json=customer) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_payment":
                    payment = {
                        "CustomerRef": {"value": self.config.config.get("customer_id", input_data.get("customer_id"))},
                        "TotalAmt": self.config.config.get("amount", input_data.get("amount", 0)),
                    }
                    async with session.post(f"{base_url}/payment", headers=headers, json=payment) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# WEBSITE & CMS NODES
# ============================================================================


class WebflowNode(BaseNode):
    """Webflow CMS and site management."""

    category = NodeCategory.INTEGRATION
    description = "Webflow CMS collections and items"
    icon = "🌐"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            access_token = self.config.config.get("access_token")
            operation = self.config.config.get("operation", "list_sites")

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            base_url = "https://api.webflow.com/v2"

            async with aiohttp.ClientSession() as session:
                if operation == "list_sites":
                    async with session.get(f"{base_url}/sites", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "list_collections":
                    site_id = self.config.config.get("site_id")
                    async with session.get(f"{base_url}/sites/{site_id}/collections", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_items":
                    collection_id = self.config.config.get("collection_id")
                    async with session.get(f"{base_url}/collections/{collection_id}/items", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_item":
                    collection_id = self.config.config.get("collection_id")
                    item = {
                        "fieldData": self.config.config.get("fields", input_data.get("fields", {})),
                    }
                    async with session.post(
                        f"{base_url}/collections/{collection_id}/items",
                        headers=headers,
                        json=item,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "publish_site":
                    site_id = self.config.config.get("site_id")
                    async with session.post(f"{base_url}/sites/{site_id}/publish", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# ADDITIONAL SOCIAL MEDIA NODES
# ============================================================================


class FacebookNode(BaseNode):
    """Facebook/Meta Graph API integration for pages and posts."""

    category = NodeCategory.INTEGRATION
    description = "Facebook pages, posts, and ads"
    icon = "📘"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            access_token = self.config.config.get("access_token")
            operation = self.config.config.get("operation", "get_pages")

            base_url = "https://graph.facebook.com/v19.0"

            async with aiohttp.ClientSession() as session:
                if operation == "get_pages":
                    async with session.get(
                        f"{base_url}/me/accounts",
                        params={"access_token": access_token},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_post":
                    page_id = self.config.config.get("page_id")
                    page_token = self.config.config.get("page_access_token")
                    post_data = {
                        "message": self.config.config.get("message", input_data.get("message", "")),
                        "access_token": page_token,
                    }
                    link = self.config.config.get("link", input_data.get("link"))
                    if link:
                        post_data["link"] = link
                    async with session.post(f"{base_url}/{page_id}/feed", data=post_data) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_insights":
                    page_id = self.config.config.get("page_id")
                    page_token = self.config.config.get("page_access_token")
                    metrics = self.config.config.get("metrics", "page_impressions,page_engaged_users")
                    async with session.get(
                        f"{base_url}/{page_id}/insights",
                        params={"metric": metrics, "access_token": page_token},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_posts":
                    page_id = self.config.config.get("page_id")
                    page_token = self.config.config.get("page_access_token")
                    async with session.get(
                        f"{base_url}/{page_id}/posts",
                        params={"access_token": page_token, "limit": 25},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class YouTubeNode(BaseNode):
    """YouTube Data API integration for channels and videos."""

    category = NodeCategory.INTEGRATION
    description = "YouTube channels, videos, and analytics"
    icon = "▶️"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key")
            operation = self.config.config.get("operation", "search_videos")

            base_url = "https://www.googleapis.com/youtube/v3"

            async with aiohttp.ClientSession() as session:
                if operation == "search_videos":
                    params = {
                        "part": "snippet",
                        "q": self.config.config.get("query", input_data.get("query", "")),
                        "type": "video",
                        "maxResults": self.config.config.get("max_results", 10),
                        "key": api_key,
                    }
                    async with session.get(f"{base_url}/search", params=params) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_video":
                    video_id = self.config.config.get("video_id", input_data.get("video_id"))
                    params = {
                        "part": "snippet,statistics,contentDetails",
                        "id": video_id,
                        "key": api_key,
                    }
                    async with session.get(f"{base_url}/videos", params=params) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_channel":
                    channel_id = self.config.config.get("channel_id", input_data.get("channel_id"))
                    params = {
                        "part": "snippet,statistics",
                        "id": channel_id,
                        "key": api_key,
                    }
                    async with session.get(f"{base_url}/channels", params=params) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "list_playlists":
                    channel_id = self.config.config.get("channel_id", input_data.get("channel_id"))
                    params = {
                        "part": "snippet",
                        "channelId": channel_id,
                        "maxResults": self.config.config.get("max_results", 25),
                        "key": api_key,
                    }
                    async with session.get(f"{base_url}/playlists", params=params) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_comments":
                    video_id = self.config.config.get("video_id", input_data.get("video_id"))
                    params = {
                        "part": "snippet",
                        "videoId": video_id,
                        "maxResults": self.config.config.get("max_results", 20),
                        "key": api_key,
                    }
                    async with session.get(f"{base_url}/commentThreads", params=params) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# ADDITIONAL NODE REGISTRY
# ============================================================================

ADDITIONAL_NODE_REGISTRY = {
    # Marketing & Email
    "mailchimp": MailchimpNode,
    "calendly": CalendlyNode,
    "airtable": AirtableNode,
    # Productivity
    "asana": AsanaNode,
    "trello": TrelloNode,
    "linear": LinearNode,
    # Social Media
    "twitter": TwitterNode,
    "linkedin": LinkedInNode,
    "facebook": FacebookNode,
    "youtube": YouTubeNode,
    # Analytics
    "google_analytics": GoogleAnalyticsNode,
    "mixpanel": MixpanelNode,
    # Enterprise CRM & Sales
    "salesforce": SalesforceNode,
    # Communication & Messaging
    "microsoft_teams": MicrosoftTeamsNode,
    "intercom": IntercomNode,
    "pagerduty": PagerDutyNode,
    # Calendar & Scheduling
    "google_calendar": GoogleCalendarNode,
    # Forms & Surveys
    "typeform": TypeformNode,
    # Finance & Accounting
    "quickbooks": QuickBooksNode,
    # Website & CMS
    "webflow": WebflowNode,
}


def get_all_nodes():
    """Get all available nodes including all registries."""
    from .integration_nodes import NODE_REGISTRY

    merged = {**NODE_REGISTRY, **ADDITIONAL_NODE_REGISTRY}

    # Also merge the advanced_nodes registry
    try:
        from .advanced_nodes import get_node_registry

        registry = get_node_registry()
        if hasattr(registry, "_nodes"):
            merged.update(registry._nodes)
        elif hasattr(registry, "nodes"):
            merged.update(registry.nodes)
    except Exception as e:
        logger.debug("Advanced nodes not available: %s", e)

    return merged
