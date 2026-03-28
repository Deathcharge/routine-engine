"""
Helix Spirals - Workflow Templates System
Pre-built workflow templates for common automation scenarios.
"""

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class TemplateCategory(Enum):
    MARKETING = "marketing"
    SALES = "sales"
    SUPPORT = "support"
    PRODUCTIVITY = "productivity"
    DEVELOPMENT = "development"
    DATA = "data"
    AI = "ai"
    SOCIAL = "social"
    ECOMMERCE = "ecommerce"
    COORDINATION = "coordination"


@dataclass
class WorkflowTemplate:
    id: str
    name: str
    description: str
    category: TemplateCategory
    icon: str
    tags: list[str]
    nodes: list[dict[str, Any]]
    connections: list[dict[str, str]]
    variables: list[dict[str, Any]]
    estimated_time_saved: str
    popularity: int = 0
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    author: str = "Helix Collective"
    version: str = "1.0.0"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "icon": self.icon,
            "tags": self.tags,
            "nodes": self.nodes,
            "connections": self.connections,
            "variables": self.variables,
            "estimated_time_saved": self.estimated_time_saved,
            "popularity": self.popularity,
            "created_at": self.created_at,
            "author": self.author,
            "version": self.version,
        }

    def instantiate(self, variable_values: dict[str, Any] = None) -> dict[str, Any]:
        """Create a new workflow instance from this template."""
        workflow_id = str(uuid.uuid4())

        # Deep copy nodes and apply variable substitutions
        nodes = json.loads(json.dumps(self.nodes))

        if variable_values:
            nodes_str = json.dumps(nodes)
            for var_name, var_value in variable_values.items():
                nodes_str = nodes_str.replace(f"{{{{{var_name}}}}}", str(var_value))
            nodes = json.loads(nodes_str)

        return {
            "id": workflow_id,
            "name": f"{self.name} (from template)",
            "description": self.description,
            "template_id": self.id,
            "nodes": nodes,
            "connections": self.connections,
            "status": "draft",
            "created_at": datetime.now(UTC).isoformat(),
        }


# ============================================================================
# MARKETING TEMPLATES
# ============================================================================

LEAD_NURTURE_TEMPLATE = WorkflowTemplate(
    id="tpl_lead_nurture",
    name="Lead Nurture Sequence",
    description="Automatically nurture new leads with a personalized email sequence based on their behavior and interests.",
    category=TemplateCategory.MARKETING,
    icon="📧",
    tags=["email", "leads", "nurture", "marketing", "automation"],
    estimated_time_saved="5 hours/week",
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "New Lead Webhook",
            "config": {"path": "/leads/new", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "ai_1",
            "type": "openai",
            "name": "Analyze Lead Intent",
            "config": {
                "operation": "chat",
                "model": "gpt-4",
                "messages": [
                    {
                        "role": "system",
                        "content": "Analyze the lead data and determine their primary interest and buying intent. Return JSON with fields: interest_category, intent_score (1-10), recommended_content.",
                    },
                    {"role": "user", "content": "Lead data: {{lead_data}}"},
                ],
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Check Intent Score",
            "config": {
                "conditions": [
                    {
                        "field": "intent_score",
                        "operator": ">=",
                        "value": 7,
                        "next": "email_hot",
                    },
                    {
                        "field": "intent_score",
                        "operator": ">=",
                        "value": 4,
                        "next": "email_warm",
                    },
                    {
                        "field": "intent_score",
                        "operator": "<",
                        "value": 4,
                        "next": "email_cold",
                    },
                ]
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "email_hot",
            "type": "sendgrid",
            "name": "Send Hot Lead Email",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "Let's schedule a call!",
                "body": "Hi {{lead_name}}, I noticed you're very interested in {{interest}}. Let's connect!",
            },
            "position": {"x": 700, "y": 50},
        },
        {
            "id": "email_warm",
            "type": "sendgrid",
            "name": "Send Warm Lead Email",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "Resources for {{interest}}",
                "body": "Hi {{lead_name}}, here are some resources about {{interest}} you might find helpful.",
            },
            "position": {"x": 700, "y": 150},
        },
        {
            "id": "email_cold",
            "type": "sendgrid",
            "name": "Send Cold Lead Email",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "Welcome to our community!",
                "body": "Hi {{lead_name}}, thanks for your interest. Here's what we do...",
            },
            "position": {"x": 700, "y": 250},
        },
        {
            "id": "crm_update",
            "type": "hubspot",
            "name": "Update CRM",
            "config": {
                "operation": "create_contact",
                "properties": {
                    "email": "{{lead_email}}",
                    "lead_score": "{{intent_score}}",
                    "interest": "{{interest_category}}",
                },
            },
            "position": {"x": 900, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "ai_1"},
        {"from": "ai_1", "to": "condition_1"},
        {"from": "condition_1", "to": "email_hot", "condition": "hot"},
        {"from": "condition_1", "to": "email_warm", "condition": "warm"},
        {"from": "condition_1", "to": "email_cold", "condition": "cold"},
        {"from": "email_hot", "to": "crm_update"},
        {"from": "email_warm", "to": "crm_update"},
        {"from": "email_cold", "to": "crm_update"},
    ],
    variables=[
        {
            "name": "from_email",
            "type": "string",
            "description": "Sender email address",
            "required": True,
        },
        {
            "name": "sendgrid_api_key",
            "type": "secret",
            "description": "SendGrid API key",
            "required": True,
        },
        {
            "name": "hubspot_api_key",
            "type": "secret",
            "description": "HubSpot API key",
            "required": True,
        },
        {
            "name": "openai_api_key",
            "type": "secret",
            "description": "OpenAI API key",
            "required": True,
        },
    ],
)


SOCIAL_MEDIA_SCHEDULER = WorkflowTemplate(
    id="tpl_social_scheduler",
    name="AI Social Media Scheduler",
    description="Generate and schedule social media posts across multiple platforms using AI.",
    category=TemplateCategory.SOCIAL,
    icon="📱",
    tags=["social", "ai", "scheduling", "content", "twitter", "linkedin"],
    estimated_time_saved="10 hours/week",
    nodes=[
        {
            "id": "trigger_1",
            "type": "schedule",
            "name": "Daily Schedule",
            "config": {"cron": "0 9 * * *", "timezone": "America/New_York"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "ai_1",
            "type": "openai",
            "name": "Generate Content Ideas",
            "config": {
                "operation": "chat",
                "model": "gpt-4",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a social media expert. Generate 3 engaging post ideas for {{industry}} that would resonate with {{target_audience}}. Return JSON array with fields: platform, content, hashtags, best_time.",
                    },
                    {
                        "role": "user",
                        "content": "Generate posts for today. Current trends: {{trends}}",
                    },
                ],
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "split_1",
            "type": "split",
            "name": "Split by Platform",
            "config": {"branches": ["twitter_post", "linkedin_post"]},
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "twitter_post",
            "type": "twitter",
            "name": "Post to Twitter",
            "config": {"operation": "post_tweet", "text": "{{twitter_content}}"},
            "position": {"x": 700, "y": 50},
        },
        {
            "id": "linkedin_post",
            "type": "linkedin",
            "name": "Post to LinkedIn",
            "config": {"operation": "share_post", "text": "{{linkedin_content}}"},
            "position": {"x": 700, "y": 150},
        },
        {
            "id": "merge_1",
            "type": "merge",
            "name": "Merge Results",
            "config": {"strategy": "combine"},
            "position": {"x": 900, "y": 100},
        },
        {
            "id": "analytics",
            "type": "airtable",
            "name": "Log to Airtable",
            "config": {
                "operation": "create_record",
                "fields": {
                    "Date": "{{today}}",
                    "Twitter Post": "{{twitter_content}}",
                    "LinkedIn Post": "{{linkedin_content}}",
                    "Status": "Posted",
                },
            },
            "position": {"x": 1100, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "ai_1"},
        {"from": "ai_1", "to": "split_1"},
        {"from": "split_1", "to": "twitter_post"},
        {"from": "split_1", "to": "linkedin_post"},
        {"from": "twitter_post", "to": "merge_1"},
        {"from": "linkedin_post", "to": "merge_1"},
        {"from": "merge_1", "to": "analytics"},
    ],
    variables=[
        {
            "name": "industry",
            "type": "string",
            "description": "Your industry/niche",
            "required": True,
        },
        {
            "name": "target_audience",
            "type": "string",
            "description": "Target audience description",
            "required": True,
        },
        {
            "name": "twitter_bearer_token",
            "type": "secret",
            "description": "Twitter API bearer token",
            "required": True,
        },
        {
            "name": "linkedin_access_token",
            "type": "secret",
            "description": "LinkedIn access token",
            "required": True,
        },
        {
            "name": "airtable_api_key",
            "type": "secret",
            "description": "Airtable API key",
            "required": True,
        },
    ],
)


# ============================================================================
# SUPPORT TEMPLATES
# ============================================================================

SUPPORT_TICKET_TRIAGE = WorkflowTemplate(
    id="tpl_support_triage",
    name="AI Support Ticket Triage",
    description="Automatically categorize, prioritize, and route support tickets using AI analysis.",
    category=TemplateCategory.SUPPORT,
    icon="🎫",
    tags=["support", "ai", "tickets", "zendesk", "automation"],
    estimated_time_saved="15 hours/week",
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "New Ticket Webhook",
            "config": {"path": "/tickets/new", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "ai_1",
            "type": "openai",
            "name": "Analyze Ticket",
            "config": {
                "operation": "chat",
                "model": "gpt-4",
                "messages": [
                    {
                        "role": "system",
                        "content": "Analyze this support ticket and return JSON with: category (billing/technical/general/urgent), priority (1-5), sentiment (positive/neutral/negative), suggested_response, requires_human (true/false).",
                    },
                    {
                        "role": "user",
                        "content": "Subject: {{ticket_subject}}\n\nBody: {{ticket_body}}",
                    },
                ],
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "sentiment_1",
            "type": "sentiment",
            "name": "Sentiment Analysis",
            "config": {"text": "{{ticket_body}}"},
            "position": {"x": 300, "y": 200},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Check Priority",
            "config": {
                "conditions": [
                    {
                        "field": "priority",
                        "operator": ">=",
                        "value": 4,
                        "next": "urgent_path",
                    },
                    {
                        "field": "requires_human",
                        "operator": "==",
                        "value": True,
                        "next": "human_path",
                    },
                    {
                        "field": "requires_human",
                        "operator": "==",
                        "value": False,
                        "next": "auto_path",
                    },
                ]
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "urgent_path",
            "type": "slack",
            "name": "Alert Team (Urgent)",
            "config": {
                "channel": "{{urgent_channel}}",
                "message": "🚨 URGENT TICKET\nSubject: {{ticket_subject}}\nCategory: {{category}}\nSentiment: {{sentiment}}",
            },
            "position": {"x": 700, "y": 50},
        },
        {
            "id": "human_path",
            "type": "zendesk",
            "name": "Assign to Agent",
            "config": {
                "operation": "update_ticket",
                "update": {
                    "assignee_id": "{{agent_id}}",
                    "priority": "{{priority}}",
                    "tags": ["needs-human", "{{category}}"],
                },
            },
            "position": {"x": 700, "y": 150},
        },
        {
            "id": "auto_path",
            "type": "zendesk",
            "name": "Auto-Reply",
            "config": {
                "operation": "update_ticket",
                "update": {
                    "status": "pending",
                    "comment": {"body": "{{suggested_response}}", "public": True},
                },
            },
            "position": {"x": 700, "y": 250},
        },
        {
            "id": "log_1",
            "type": "airtable",
            "name": "Log Analysis",
            "config": {
                "operation": "create_record",
                "fields": {
                    "Ticket ID": "{{ticket_id}}",
                    "Category": "{{category}}",
                    "Priority": "{{priority}}",
                    "Sentiment": "{{sentiment}}",
                    "Auto-Resolved": "{{!requires_human}}",
                },
            },
            "position": {"x": 900, "y": 150},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "ai_1"},
        {"from": "trigger_1", "to": "sentiment_1"},
        {"from": "ai_1", "to": "condition_1"},
        {"from": "condition_1", "to": "urgent_path", "condition": "urgent"},
        {"from": "condition_1", "to": "human_path", "condition": "human"},
        {"from": "condition_1", "to": "auto_path", "condition": "auto"},
        {"from": "urgent_path", "to": "log_1"},
        {"from": "human_path", "to": "log_1"},
        {"from": "auto_path", "to": "log_1"},
    ],
    variables=[
        {
            "name": "zendesk_subdomain",
            "type": "string",
            "description": "Zendesk subdomain",
            "required": True,
        },
        {
            "name": "zendesk_api_token",
            "type": "secret",
            "description": "Zendesk API token",
            "required": True,
        },
        {
            "name": "slack_webhook_url",
            "type": "secret",
            "description": "Slack webhook URL",
            "required": True,
        },
        {
            "name": "urgent_channel",
            "type": "string",
            "description": "Slack channel for urgent tickets",
            "required": True,
        },
        {
            "name": "openai_api_key",
            "type": "secret",
            "description": "OpenAI API key",
            "required": True,
        },
    ],
)


# ============================================================================
# ECOMMERCE TEMPLATES
# ============================================================================

ORDER_FULFILLMENT = WorkflowTemplate(
    id="tpl_order_fulfillment",
    name="Order Fulfillment Automation",
    description="Automate order processing, inventory updates, and customer notifications.",
    category=TemplateCategory.ECOMMERCE,
    icon="📦",
    tags=["ecommerce", "orders", "shopify", "inventory", "notifications"],
    estimated_time_saved="20 hours/week",
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "New Order Webhook",
            "config": {"path": "/orders/new", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "verify_1",
            "type": "webhook_signature",
            "name": "Verify Shopify Signature",
            "config": {
                "secret": "{{shopify_webhook_secret}}",
                "signature_header": "X-Shopify-Hmac-SHA256",
                "algorithm": "sha256",
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "inventory_1",
            "type": "shopify",
            "name": "Check Inventory",
            "config": {"operation": "get_products"},
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Check Stock",
            "config": {
                "conditions": [
                    {
                        "field": "in_stock",
                        "operator": "==",
                        "value": True,
                        "next": "process_order",
                    },
                    {
                        "field": "in_stock",
                        "operator": "==",
                        "value": False,
                        "next": "backorder",
                    },
                ]
            },
            "position": {"x": 700, "y": 100},
        },
        {
            "id": "process_order",
            "type": "shopify",
            "name": "Update Inventory",
            "config": {"operation": "update_inventory", "quantity": "{{new_quantity}}"},
            "position": {"x": 900, "y": 50},
        },
        {
            "id": "backorder",
            "type": "sendgrid",
            "name": "Send Backorder Email",
            "config": {
                "subject": "Your order is on backorder",
                "body": "Hi {{customer_name}}, the item you ordered is currently out of stock. We'll notify you when it's available.",
            },
            "position": {"x": 900, "y": 150},
        },
        {
            "id": "confirm_email",
            "type": "sendgrid",
            "name": "Send Confirmation",
            "config": {
                "subject": "Order Confirmed! #{{order_number}}",
                "body": "Hi {{customer_name}}, your order has been confirmed and is being processed.",
            },
            "position": {"x": 1100, "y": 50},
        },
        {
            "id": "slack_notify",
            "type": "slack",
            "name": "Notify Team",
            "config": {
                "channel": "{{orders_channel}}",
                "message": "📦 New Order #{{order_number}}\nCustomer: {{customer_name}}\nTotal: ${{order_total}}",
            },
            "position": {"x": 1100, "y": 150},
        },
        {
            "id": "stripe_1",
            "type": "stripe",
            "name": "Process Payment",
            "config": {
                "operation": "create_payment_intent",
                "amount": "{{order_total_cents}}",
                "currency": "usd",
            },
            "position": {"x": 1300, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "verify_1"},
        {"from": "verify_1", "to": "inventory_1"},
        {"from": "inventory_1", "to": "condition_1"},
        {"from": "condition_1", "to": "process_order", "condition": "in_stock"},
        {"from": "condition_1", "to": "backorder", "condition": "out_of_stock"},
        {"from": "process_order", "to": "confirm_email"},
        {"from": "process_order", "to": "slack_notify"},
        {"from": "confirm_email", "to": "stripe_1"},
    ],
    variables=[
        {
            "name": "shopify_shop_url",
            "type": "string",
            "description": "Shopify store URL",
            "required": True,
        },
        {
            "name": "shopify_access_token",
            "type": "secret",
            "description": "Shopify access token",
            "required": True,
        },
        {
            "name": "shopify_webhook_secret",
            "type": "secret",
            "description": "Shopify webhook secret",
            "required": True,
        },
        {
            "name": "sendgrid_api_key",
            "type": "secret",
            "description": "SendGrid API key",
            "required": True,
        },
        {
            "name": "stripe_api_key",
            "type": "secret",
            "description": "Stripe API key",
            "required": True,
        },
        {
            "name": "slack_webhook_url",
            "type": "secret",
            "description": "Slack webhook URL",
            "required": True,
        },
        {
            "name": "orders_channel",
            "type": "string",
            "description": "Slack channel for orders",
            "required": True,
        },
    ],
)


# ============================================================================
# COORDINATION TEMPLATES (Helix-specific)
# ============================================================================

COORDINATION_SYNC = WorkflowTemplate(
    id="tpl_coordination_sync",
    name="Daily Coordination Synchronization",
    description="Synchronize UCF metrics across all Helix agents and update collective coordination state.",
    category=TemplateCategory.COORDINATION,
    icon="🧠",
    tags=["coordination", "ucf", "helix", "agents", "sync"],
    estimated_time_saved="Continuous",
    nodes=[
        {
            "id": "trigger_1",
            "type": "schedule",
            "name": "Hourly Sync",
            "config": {"cron": "0 * * * *", "timezone": "UTC"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "fetch_agents",
            "type": "http",
            "name": "Fetch Agent States",
            "config": {
                "url": "{{helix_api_url}}/api/agents/status",
                "method": "GET",
                "headers": {"Authorization": "Bearer {{helix_api_key}}"},
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "calculate_ucf",
            "type": "code",
            "name": "Calculate UCF Metrics",
            "config": {
                "language": "python",
                "code": """
# Calculate collective UCF metrics
agents = input_data.get('agents', [])
metrics = {
    'harmony': sum(a.get('harmony', 0) for a in agents) / len(agents),
    'resilience': sum(a.get('resilience', 0) for a in agents) / len(agents),
    'throughput': sum(a.get('throughput', 0) for a in agents) / len(agents),
    'focus': sum(a.get('focus', 0) for a in agents) / len(agents),
    'friction': sum(a.get('friction', 0) for a in agents) / len(agents),
    'velocity': sum(a.get('velocity', 0) for a in agents) / len(agents)
}
metrics['performance_score'] = (
    metrics['harmony'] * 0.2 +
    metrics['resilience'] * 0.2 +
    metrics['throughput'] * 0.15 +
    metrics['focus'] * 0.15 +
    (1 - metrics['friction']) * 0.15 +
    metrics['velocity'] * 0.15
)
return {'ucf_metrics': metrics, 'agent_count': len(agents)}
""",
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "store_metrics",
            "type": "redis",
            "name": "Store in Redis",
            "config": {
                "operation": "hset",
                "key": "helix:ucf:current",
                "field": "metrics",
                "value": "{{ucf_metrics}}",
            },
            "position": {"x": 700, "y": 100},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Check Thresholds",
            "config": {
                "conditions": [
                    {
                        "field": "performance_score",
                        "operator": "<",
                        "value": 0.5,
                        "next": "alert_low",
                    },
                    {
                        "field": "performance_score",
                        "operator": ">=",
                        "value": 0.9,
                        "next": "celebrate_high",
                    },
                    {
                        "field": "performance_score",
                        "operator": ">=",
                        "value": 0.5,
                        "next": "normal",
                    },
                ]
            },
            "position": {"x": 900, "y": 100},
        },
        {
            "id": "alert_low",
            "type": "discord",
            "name": "Alert Low Coordination",
            "config": {
                "webhook_url": "{{discord_webhook}}",
                "content": "⚠️ **Coordination Alert**\nCollective coordination level has dropped to {{performance_score}}. Initiating recovery protocols.",
            },
            "position": {"x": 1100, "y": 50},
        },
        {
            "id": "celebrate_high",
            "type": "discord",
            "name": "Celebrate High Coordination",
            "config": {
                "webhook_url": "{{discord_webhook}}",
                "content": "🌟 **Coordination Peak**\nCollective coordination has reached {{performance_score}}! The Helix is in harmony.",
            },
            "position": {"x": 1100, "y": 150},
        },
        {
            "id": "normal",
            "type": "http",
            "name": "Update Dashboard",
            "config": {
                "url": "{{helix_api_url}}/api/coordination/update",
                "method": "POST",
                "body": "{{ucf_metrics}}",
            },
            "position": {"x": 1100, "y": 250},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "fetch_agents"},
        {"from": "fetch_agents", "to": "calculate_ucf"},
        {"from": "calculate_ucf", "to": "store_metrics"},
        {"from": "store_metrics", "to": "condition_1"},
        {"from": "condition_1", "to": "alert_low", "condition": "low"},
        {"from": "condition_1", "to": "celebrate_high", "condition": "high"},
        {"from": "condition_1", "to": "normal", "condition": "normal"},
    ],
    variables=[
        {
            "name": "helix_api_url",
            "type": "string",
            "description": "Helix API base URL",
            "required": True,
        },
        {
            "name": "helix_api_key",
            "type": "secret",
            "description": "Helix API key",
            "required": True,
        },
        {
            "name": "discord_webhook",
            "type": "secret",
            "description": "Discord webhook URL",
            "required": True,
        },
        {
            "name": "redis_url",
            "type": "secret",
            "description": "Redis connection URL",
            "required": True,
        },
    ],
)


# ============================================================================
# MARKETING & LEAD GEN TEMPLATES
# ============================================================================

NEW_SUBSCRIBER_WELCOME = WorkflowTemplate(
    id="tpl_subscriber_welcome",
    name="New Subscriber Welcome Sequence",
    description="Welcome new email subscribers with a personalized onboarding sequence, tag them in your CRM, and notify your team.",
    category=TemplateCategory.MARKETING,
    icon="👋",
    tags=["email", "subscribers", "onboarding", "welcome", "mailchimp"],
    estimated_time_saved="3 hours/week",
    popularity=85,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "New Subscriber",
            "config": {"path": "/subscribers/new", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "mailchimp_1",
            "type": "mailchimp",
            "name": "Add to Welcome Audience",
            "config": {
                "action": "add_member",
                "list_id": "{{mailchimp_list_id}}",
                "email": "{{subscriber_email}}",
                "tags": ["new-subscriber", "welcome-sequence"],
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "delay_1",
            "type": "delay",
            "name": "Wait 5 minutes",
            "config": {"duration": 300, "unit": "seconds"},
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "email_1",
            "type": "sendgrid",
            "name": "Send Welcome Email",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "Welcome to {{company_name}}!",
                "body": "Hi {{subscriber_name}}, welcome! Here's what you can expect from us...",
            },
            "position": {"x": 700, "y": 100},
        },
        {
            "id": "crm_1",
            "type": "hubspot",
            "name": "Create CRM Contact",
            "config": {
                "operation": "create_contact",
                "properties": {
                    "email": "{{subscriber_email}}",
                    "lifecycle_stage": "subscriber",
                    "source": "email_signup",
                },
            },
            "position": {"x": 900, "y": 100},
        },
        {
            "id": "slack_1",
            "type": "slack_webhook",
            "name": "Notify Team",
            "config": {
                "channel": "{{marketing_channel}}",
                "message": "📬 New subscriber: {{subscriber_email}}",
            },
            "position": {"x": 1100, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "mailchimp_1"},
        {"from": "mailchimp_1", "to": "delay_1"},
        {"from": "delay_1", "to": "email_1"},
        {"from": "email_1", "to": "crm_1"},
        {"from": "crm_1", "to": "slack_1"},
    ],
    variables=[
        {
            "name": "mailchimp_list_id",
            "type": "string",
            "description": "Mailchimp audience list ID",
            "required": True,
        },
        {
            "name": "from_email",
            "type": "string",
            "description": "Sender email address",
            "required": True,
        },
        {
            "name": "company_name",
            "type": "string",
            "description": "Your company name",
            "required": True,
        },
        {
            "name": "marketing_channel",
            "type": "string",
            "description": "Slack channel for marketing notifications",
            "required": True,
        },
    ],
)


WEBINAR_REGISTRATION = WorkflowTemplate(
    id="tpl_webinar_registration",
    name="Webinar Registration Pipeline",
    description="Handle webinar registrations: collect from Typeform, create calendar invites, send confirmations, and sync to CRM.",
    category=TemplateCategory.MARKETING,
    icon="🎥",
    tags=["webinar", "registration", "typeform", "calendar", "crm"],
    estimated_time_saved="4 hours/week",
    popularity=72,
    nodes=[
        {
            "id": "trigger_1",
            "type": "typeform",
            "name": "Typeform Response",
            "config": {"action": "get_responses", "form_id": "{{typeform_form_id}}"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "calendar_1",
            "type": "google_calendar",
            "name": "Create Calendar Invite",
            "config": {
                "action": "create_event",
                "summary": "{{webinar_title}}",
                "start_time": "{{webinar_start}}",
                "end_time": "{{webinar_end}}",
                "attendees": ["{{registrant_email}}"],
                "description": "Join link: {{webinar_link}}",
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "email_1",
            "type": "sendgrid",
            "name": "Confirmation Email",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "You're registered for {{webinar_title}}!",
                "body": "Hi {{registrant_name}}, you're all set! Calendar invite sent. Join link: {{webinar_link}}",
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "crm_1",
            "type": "salesforce",
            "name": "Add to Salesforce Campaign",
            "config": {
                "action": "create",
                "object_type": "CampaignMember",
                "data": {
                    "CampaignId": "{{sf_campaign_id}}",
                    "Email": "{{registrant_email}}",
                    "Status": "Registered",
                },
            },
            "position": {"x": 700, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "calendar_1"},
        {"from": "calendar_1", "to": "email_1"},
        {"from": "email_1", "to": "crm_1"},
    ],
    variables=[
        {
            "name": "typeform_form_id",
            "type": "string",
            "description": "Typeform form ID",
            "required": True,
        },
        {
            "name": "webinar_title",
            "type": "string",
            "description": "Webinar title",
            "required": True,
        },
        {
            "name": "webinar_link",
            "type": "string",
            "description": "Webinar join URL",
            "required": True,
        },
        {
            "name": "from_email",
            "type": "string",
            "description": "Sender email",
            "required": True,
        },
        {
            "name": "sf_campaign_id",
            "type": "string",
            "description": "Salesforce campaign ID",
            "required": True,
        },
    ],
)


LEAD_SCORING = WorkflowTemplate(
    id="tpl_lead_scoring",
    name="AI Lead Scoring & Routing",
    description="Score incoming leads with AI, route high-value leads to sales reps, and notify via Slack and Microsoft Teams.",
    category=TemplateCategory.SALES,
    icon="🎯",
    tags=["leads", "scoring", "ai", "sales", "routing"],
    estimated_time_saved="8 hours/week",
    popularity=90,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "New Lead",
            "config": {"path": "/leads/inbound", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "ai_1",
            "type": "openai",
            "name": "AI Lead Score",
            "config": {
                "operation": "chat",
                "model": "gpt-4",
                "messages": [
                    {
                        "role": "system",
                        "content": "Score this lead 1-100 based on company size, title, industry fit. Return JSON: {score, tier: 'hot'|'warm'|'cold', reason, recommended_rep}",
                    },
                    {"role": "user", "content": "Lead: {{lead_data}}"},
                ],
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Route by Score",
            "config": {
                "conditions": [
                    {
                        "field": "score",
                        "operator": ">=",
                        "value": 70,
                        "next": "hot_lead",
                    },
                    {
                        "field": "score",
                        "operator": ">=",
                        "value": 40,
                        "next": "warm_lead",
                    },
                    {
                        "field": "score",
                        "operator": "<",
                        "value": 40,
                        "next": "cold_lead",
                    },
                ]
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "hot_lead",
            "type": "microsoft_teams",
            "name": "Alert Sales (Teams)",
            "config": {
                "action": "send_message",
                "channel_id": "{{sales_channel_id}}",
                "message": "🔥 HOT LEAD (Score: {{score}})\n{{lead_name}} - {{lead_company}}\nReason: {{reason}}",
            },
            "position": {"x": 700, "y": 50},
        },
        {
            "id": "warm_lead",
            "type": "slack_webhook",
            "name": "Notify SDR Team",
            "config": {
                "channel": "{{sdr_channel}}",
                "message": "🌤️ Warm lead: {{lead_name}} (Score: {{score}}). Follow up within 24h.",
            },
            "position": {"x": 700, "y": 150},
        },
        {
            "id": "cold_lead",
            "type": "mailchimp",
            "name": "Add to Nurture List",
            "config": {
                "action": "add_member",
                "list_id": "{{nurture_list_id}}",
                "email": "{{lead_email}}",
                "tags": ["cold-lead", "nurture"],
            },
            "position": {"x": 700, "y": 250},
        },
        {
            "id": "crm_1",
            "type": "salesforce",
            "name": "Update Salesforce",
            "config": {
                "action": "create",
                "object_type": "Lead",
                "data": {
                    "Email": "{{lead_email}}",
                    "LeadScore__c": "{{score}}",
                    "Rating": "{{tier}}",
                },
            },
            "position": {"x": 900, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "ai_1"},
        {"from": "ai_1", "to": "condition_1"},
        {"from": "condition_1", "to": "hot_lead", "condition": "hot"},
        {"from": "condition_1", "to": "warm_lead", "condition": "warm"},
        {"from": "condition_1", "to": "cold_lead", "condition": "cold"},
        {"from": "hot_lead", "to": "crm_1"},
        {"from": "warm_lead", "to": "crm_1"},
        {"from": "cold_lead", "to": "crm_1"},
    ],
    variables=[
        {
            "name": "sales_channel_id",
            "type": "string",
            "description": "Microsoft Teams sales channel ID",
            "required": True,
        },
        {
            "name": "sdr_channel",
            "type": "string",
            "description": "Slack SDR team channel",
            "required": True,
        },
        {
            "name": "nurture_list_id",
            "type": "string",
            "description": "Mailchimp nurture list ID",
            "required": True,
        },
    ],
)


# ============================================================================
# SALES & CRM TEMPLATES
# ============================================================================

CRM_DEAL_ALERTS = WorkflowTemplate(
    id="tpl_crm_deal_alerts",
    name="CRM Deal Stage Alerts",
    description="Monitor Salesforce deal stage changes and send real-time notifications to Slack and Microsoft Teams.",
    category=TemplateCategory.SALES,
    icon="💰",
    tags=["crm", "salesforce", "deals", "alerts", "slack", "teams"],
    estimated_time_saved="6 hours/week",
    popularity=78,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "Deal Updated",
            "config": {"path": "/deals/updated", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "sf_1",
            "type": "salesforce",
            "name": "Get Deal Details",
            "config": {
                "action": "query",
                "query": "SELECT Name, Amount, StageName, Owner.Name FROM Opportunity WHERE Id = '{{deal_id}}'",
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Check Stage",
            "config": {
                "conditions": [
                    {
                        "field": "StageName",
                        "operator": "==",
                        "value": "Closed Won",
                        "next": "won_path",
                    },
                    {
                        "field": "StageName",
                        "operator": "==",
                        "value": "Closed Lost",
                        "next": "lost_path",
                    },
                    {
                        "field": "Amount",
                        "operator": ">=",
                        "value": 50000,
                        "next": "big_deal_path",
                    },
                ]
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "won_path",
            "type": "slack_webhook",
            "name": "Celebrate Win",
            "config": {
                "channel": "{{wins_channel}}",
                "message": "🎉 DEAL WON! {{deal_name}} - ${{deal_amount}}\nRep: {{deal_owner}}",
            },
            "position": {"x": 700, "y": 50},
        },
        {
            "id": "lost_path",
            "type": "slack_webhook",
            "name": "Log Lost Deal",
            "config": {
                "channel": "{{sales_channel}}",
                "message": "📉 Deal lost: {{deal_name}} - ${{deal_amount}}\nRep: {{deal_owner}}",
            },
            "position": {"x": 700, "y": 150},
        },
        {
            "id": "big_deal_path",
            "type": "microsoft_teams",
            "name": "Alert Leadership",
            "config": {
                "action": "send_message",
                "channel_id": "{{leadership_channel_id}}",
                "message": "💎 Big deal update: {{deal_name}} (${{deal_amount}}) moved to {{StageName}}",
            },
            "position": {"x": 700, "y": 250},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "sf_1"},
        {"from": "sf_1", "to": "condition_1"},
        {"from": "condition_1", "to": "won_path", "condition": "won"},
        {"from": "condition_1", "to": "lost_path", "condition": "lost"},
        {"from": "condition_1", "to": "big_deal_path", "condition": "big_deal"},
    ],
    variables=[
        {
            "name": "wins_channel",
            "type": "string",
            "description": "Slack channel for deal wins",
            "required": True,
        },
        {
            "name": "sales_channel",
            "type": "string",
            "description": "Slack sales channel",
            "required": True,
        },
        {
            "name": "leadership_channel_id",
            "type": "string",
            "description": "Teams leadership channel",
            "required": True,
        },
    ],
)


INVOICE_AUTOMATION = WorkflowTemplate(
    id="tpl_invoice_automation",
    name="Invoice & Payment Automation",
    description="Stripe payment received triggers QuickBooks invoice creation, email receipt to customer, and CRM update.",
    category=TemplateCategory.SALES,
    icon="🧾",
    tags=["invoice", "payment", "stripe", "quickbooks", "accounting"],
    estimated_time_saved="10 hours/week",
    popularity=82,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "Stripe Payment",
            "config": {"path": "/stripe/webhook", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "verify_1",
            "type": "webhook_signature",
            "name": "Verify Stripe Signature",
            "config": {
                "secret": "{{stripe_webhook_secret}}",
                "signature_header": "Stripe-Signature",
                "algorithm": "sha256",
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "qb_1",
            "type": "quickbooks",
            "name": "Create QuickBooks Invoice",
            "config": {
                "action": "create_invoice",
                "customer_email": "{{customer_email}}",
                "line_items": [{"description": "{{product_name}}", "amount": "{{amount}}"}],
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "email_1",
            "type": "sendgrid",
            "name": "Send Receipt",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "Receipt for your payment of ${{amount}}",
                "body": "Hi {{customer_name}}, thank you for your payment. Invoice #{{invoice_number}} attached.",
            },
            "position": {"x": 700, "y": 100},
        },
        {
            "id": "crm_1",
            "type": "hubspot",
            "name": "Update Deal",
            "config": {
                "operation": "update_deal",
                "properties": {"dealstage": "closedwon", "amount": "{{amount}}"},
            },
            "position": {"x": 900, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "verify_1"},
        {"from": "verify_1", "to": "qb_1"},
        {"from": "qb_1", "to": "email_1"},
        {"from": "email_1", "to": "crm_1"},
    ],
    variables=[
        {
            "name": "stripe_webhook_secret",
            "type": "secret",
            "description": "Stripe webhook signing secret",
            "required": True,
        },
        {
            "name": "from_email",
            "type": "string",
            "description": "Sender email for receipts",
            "required": True,
        },
        {
            "name": "quickbooks_realm_id",
            "type": "string",
            "description": "QuickBooks company realm ID",
            "required": True,
        },
    ],
)


# ============================================================================
# CUSTOMER SUPPORT TEMPLATES
# ============================================================================

ESCALATION_WORKFLOW = WorkflowTemplate(
    id="tpl_escalation_workflow",
    name="Support Ticket Escalation",
    description="Auto-escalate support tickets older than 24 hours: notify manager, create PagerDuty incident, update ticket priority.",
    category=TemplateCategory.SUPPORT,
    icon="🚨",
    tags=["support", "escalation", "pagerduty", "sla", "tickets"],
    estimated_time_saved="7 hours/week",
    popularity=74,
    nodes=[
        {
            "id": "trigger_1",
            "type": "schedule",
            "name": "Check Every Hour",
            "config": {"cron": "0 * * * *", "timezone": "UTC"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "zendesk_1",
            "type": "zendesk",
            "name": "Find Stale Tickets",
            "config": {
                "operation": "search_tickets",
                "query": "status:open created<24hours",
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "loop_1",
            "type": "loop",
            "name": "For Each Ticket",
            "config": {"items_path": "tickets"},
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "pagerduty_1",
            "type": "pagerduty",
            "name": "Create PD Incident",
            "config": {
                "action": "create_incident",
                "title": "SLA Breach: Ticket #{{ticket_id}} open > 24h",
                "urgency": "high",
                "service_id": "{{pd_service_id}}",
            },
            "position": {"x": 700, "y": 50},
        },
        {
            "id": "slack_1",
            "type": "slack_webhook",
            "name": "Notify Manager",
            "config": {
                "channel": "{{escalation_channel}}",
                "message": "⏰ SLA Breach: Ticket #{{ticket_id}} ({{ticket_subject}}) has been open > 24h. Escalating.",
            },
            "position": {"x": 700, "y": 150},
        },
        {
            "id": "zendesk_2",
            "type": "zendesk",
            "name": "Escalate Ticket",
            "config": {
                "operation": "update_ticket",
                "update": {"priority": "urgent", "tags": ["escalated", "sla-breach"]},
            },
            "position": {"x": 900, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "zendesk_1"},
        {"from": "zendesk_1", "to": "loop_1"},
        {"from": "loop_1", "to": "pagerduty_1"},
        {"from": "loop_1", "to": "slack_1"},
        {"from": "pagerduty_1", "to": "zendesk_2"},
    ],
    variables=[
        {
            "name": "pd_service_id",
            "type": "string",
            "description": "PagerDuty service ID",
            "required": True,
        },
        {
            "name": "escalation_channel",
            "type": "string",
            "description": "Slack escalation channel",
            "required": True,
        },
    ],
)


CUSTOMER_FEEDBACK_LOOP = WorkflowTemplate(
    id="tpl_customer_feedback",
    name="Customer Feedback Analysis Loop",
    description="Collect Typeform survey responses, run AI sentiment analysis, route positive/negative feedback, and notify the team.",
    category=TemplateCategory.SUPPORT,
    icon="📊",
    tags=["feedback", "survey", "typeform", "sentiment", "nps"],
    estimated_time_saved="5 hours/week",
    popularity=68,
    nodes=[
        {
            "id": "trigger_1",
            "type": "typeform",
            "name": "Survey Response",
            "config": {"action": "get_responses", "form_id": "{{feedback_form_id}}"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "ai_1",
            "type": "openai",
            "name": "Sentiment Analysis",
            "config": {
                "operation": "chat",
                "model": "gpt-4",
                "messages": [
                    {
                        "role": "system",
                        "content": "Analyze this customer feedback. Return JSON: {sentiment: 'positive'|'neutral'|'negative', score: 1-10, themes: [], summary: ''}",
                    },
                    {"role": "user", "content": "Feedback: {{feedback_text}}"},
                ],
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Route by Sentiment",
            "config": {
                "conditions": [
                    {
                        "field": "sentiment",
                        "operator": "==",
                        "value": "negative",
                        "next": "negative_path",
                    },
                    {
                        "field": "sentiment",
                        "operator": "==",
                        "value": "positive",
                        "next": "positive_path",
                    },
                ]
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "negative_path",
            "type": "intercom",
            "name": "Create Support Ticket",
            "config": {
                "action": "create_ticket",
                "title": "Negative feedback from {{customer_email}}",
                "body": "{{summary}}\n\nOriginal: {{feedback_text}}",
            },
            "position": {"x": 700, "y": 50},
        },
        {
            "id": "positive_path",
            "type": "slack_webhook",
            "name": "Share Positive Feedback",
            "config": {
                "channel": "{{wins_channel}}",
                "message": "🌟 Positive feedback from {{customer_name}}: {{summary}}",
            },
            "position": {"x": 700, "y": 150},
        },
        {
            "id": "airtable_1",
            "type": "airtable",
            "name": "Log to Feedback DB",
            "config": {
                "operation": "create_record",
                "fields": {
                    "Customer": "{{customer_email}}",
                    "Sentiment": "{{sentiment}}",
                    "Score": "{{score}}",
                    "Summary": "{{summary}}",
                    "Date": "{{today}}",
                },
            },
            "position": {"x": 900, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "ai_1"},
        {"from": "ai_1", "to": "condition_1"},
        {"from": "condition_1", "to": "negative_path", "condition": "negative"},
        {"from": "condition_1", "to": "positive_path", "condition": "positive"},
        {"from": "negative_path", "to": "airtable_1"},
        {"from": "positive_path", "to": "airtable_1"},
    ],
    variables=[
        {
            "name": "feedback_form_id",
            "type": "string",
            "description": "Typeform feedback form ID",
            "required": True,
        },
        {
            "name": "wins_channel",
            "type": "string",
            "description": "Slack channel for positive feedback",
            "required": True,
        },
    ],
)


# ============================================================================
# DEVOPS & ENGINEERING TEMPLATES
# ============================================================================

DEPLOY_NOTIFICATION = WorkflowTemplate(
    id="tpl_deploy_notification",
    name="Deploy Notification Pipeline",
    description="GitHub PR merged triggers build notification, Slack announcement, and status page update.",
    category=TemplateCategory.DEVELOPMENT,
    icon="🚀",
    tags=["deploy", "github", "ci", "notifications", "devops"],
    estimated_time_saved="3 hours/week",
    popularity=88,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "GitHub PR Merged",
            "config": {"path": "/github/webhook", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Check if Merged",
            "config": {
                "conditions": [
                    {"field": "action", "operator": "==", "value": "closed"},
                    {"field": "pull_request.merged", "operator": "==", "value": True},
                ]
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "slack_1",
            "type": "slack_webhook",
            "name": "Announce Deploy",
            "config": {
                "channel": "{{deploy_channel}}",
                "message": "🚀 Deployed: {{pr_title}}\nAuthor: {{pr_author}}\nBranch: {{branch}} → main\n{{pr_url}}",
            },
            "position": {"x": 500, "y": 50},
        },
        {
            "id": "teams_1",
            "type": "microsoft_teams",
            "name": "Teams Notification",
            "config": {
                "action": "send_message",
                "channel_id": "{{teams_deploy_channel}}",
                "message": "🚀 New deployment: {{pr_title}} by {{pr_author}}",
            },
            "position": {"x": 500, "y": 150},
        },
        {
            "id": "http_1",
            "type": "http_request",
            "name": "Update Status Page",
            "config": {
                "url": "{{status_page_api}}/incidents",
                "method": "POST",
                "headers": {"Authorization": "Bearer {{status_page_key}}"},
                "body": {"name": "Deployment: {{pr_title}}", "status": "resolved"},
            },
            "position": {"x": 700, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "condition_1"},
        {"from": "condition_1", "to": "slack_1"},
        {"from": "condition_1", "to": "teams_1"},
        {"from": "slack_1", "to": "http_1"},
    ],
    variables=[
        {
            "name": "deploy_channel",
            "type": "string",
            "description": "Slack deploy channel",
            "required": True,
        },
        {
            "name": "teams_deploy_channel",
            "type": "string",
            "description": "Teams deploy channel ID",
            "required": True,
        },
        {
            "name": "status_page_api",
            "type": "string",
            "description": "Status page API URL",
            "required": True,
        },
        {
            "name": "status_page_key",
            "type": "secret",
            "description": "Status page API key",
            "required": True,
        },
    ],
)


INCIDENT_RESPONSE = WorkflowTemplate(
    id="tpl_incident_response",
    name="Incident Response Automation",
    description="PagerDuty alert triggers Slack war room creation, on-call notification, and incident tracking.",
    category=TemplateCategory.DEVELOPMENT,
    icon="🔥",
    tags=["incident", "pagerduty", "on-call", "war-room", "devops"],
    estimated_time_saved="12 hours/week",
    popularity=92,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "PagerDuty Alert",
            "config": {"path": "/pagerduty/webhook", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "pagerduty_1",
            "type": "pagerduty",
            "name": "Get Incident Details",
            "config": {"action": "get_incident", "incident_id": "{{incident_id}}"},
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "slack_1",
            "type": "slack_webhook",
            "name": "Create War Room",
            "config": {
                "channel": "{{incidents_channel}}",
                "message": "🔥 INCIDENT: {{incident_title}}\nSeverity: {{severity}}\nService: {{service_name}}\n\nWar room: #incident-{{incident_number}}",
            },
            "position": {"x": 500, "y": 50},
        },
        {
            "id": "teams_1",
            "type": "microsoft_teams",
            "name": "Notify On-Call",
            "config": {
                "action": "send_message",
                "channel_id": "{{oncall_channel_id}}",
                "message": "🚨 @oncall Incident triggered: {{incident_title}} ({{severity}})",
            },
            "position": {"x": 500, "y": 150},
        },
        {
            "id": "airtable_1",
            "type": "airtable",
            "name": "Log Incident",
            "config": {
                "operation": "create_record",
                "fields": {
                    "Incident ID": "{{incident_id}}",
                    "Title": "{{incident_title}}",
                    "Severity": "{{severity}}",
                    "Status": "Open",
                    "Created": "{{now}}",
                },
            },
            "position": {"x": 700, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "pagerduty_1"},
        {"from": "pagerduty_1", "to": "slack_1"},
        {"from": "pagerduty_1", "to": "teams_1"},
        {"from": "slack_1", "to": "airtable_1"},
    ],
    variables=[
        {
            "name": "incidents_channel",
            "type": "string",
            "description": "Slack incidents channel",
            "required": True,
        },
        {
            "name": "oncall_channel_id",
            "type": "string",
            "description": "Teams on-call channel",
            "required": True,
        },
    ],
)


# ============================================================================
# DATA & ANALYTICS TEMPLATES
# ============================================================================

DAILY_METRICS_DIGEST = WorkflowTemplate(
    id="tpl_daily_metrics",
    name="Daily Metrics Digest",
    description="Scheduled daily digest: fetch metrics from multiple sources, AI-generate summary, email to stakeholders, post to Slack.",
    category=TemplateCategory.DATA,
    icon="📈",
    tags=["metrics", "digest", "analytics", "daily", "reporting"],
    estimated_time_saved="5 hours/week",
    popularity=76,
    nodes=[
        {
            "id": "trigger_1",
            "type": "schedule",
            "name": "Daily at 8 AM",
            "config": {"cron": "0 8 * * *", "timezone": "{{timezone}}"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "http_1",
            "type": "http_request",
            "name": "Fetch Analytics",
            "config": {
                "url": "{{analytics_api}}/metrics/daily",
                "method": "GET",
                "headers": {"Authorization": "Bearer {{analytics_key}}"},
            },
            "position": {"x": 300, "y": 50},
        },
        {
            "id": "http_2",
            "type": "http_request",
            "name": "Fetch Revenue",
            "config": {
                "url": "{{stripe_api}}/v1/balance",
                "method": "GET",
                "headers": {"Authorization": "Bearer {{stripe_key}}"},
            },
            "position": {"x": 300, "y": 150},
        },
        {
            "id": "merge_1",
            "type": "merge",
            "name": "Merge Data",
            "config": {"strategy": "combine"},
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "ai_1",
            "type": "openai",
            "name": "AI Summary",
            "config": {
                "operation": "chat",
                "model": "gpt-4",
                "messages": [
                    {
                        "role": "system",
                        "content": "Generate a concise daily metrics summary with key insights, trends, and action items. Use bullet points.",
                    },
                    {"role": "user", "content": "Metrics data: {{metrics_data}}"},
                ],
            },
            "position": {"x": 700, "y": 100},
        },
        {
            "id": "email_1",
            "type": "sendgrid",
            "name": "Email Digest",
            "config": {
                "from_email": "{{from_email}}",
                "to": "{{stakeholders_email}}",
                "subject": "Daily Metrics Digest - {{today}}",
                "body": "{{ai_summary}}",
            },
            "position": {"x": 900, "y": 50},
        },
        {
            "id": "slack_1",
            "type": "slack_webhook",
            "name": "Post to Slack",
            "config": {
                "channel": "{{metrics_channel}}",
                "message": "📈 Daily Metrics Digest\n{{ai_summary}}",
            },
            "position": {"x": 900, "y": 150},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "http_1"},
        {"from": "trigger_1", "to": "http_2"},
        {"from": "http_1", "to": "merge_1"},
        {"from": "http_2", "to": "merge_1"},
        {"from": "merge_1", "to": "ai_1"},
        {"from": "ai_1", "to": "email_1"},
        {"from": "ai_1", "to": "slack_1"},
    ],
    variables=[
        {
            "name": "analytics_api",
            "type": "string",
            "description": "Analytics API base URL",
            "required": True,
        },
        {
            "name": "from_email",
            "type": "string",
            "description": "Sender email for digest",
            "required": True,
        },
        {
            "name": "stakeholders_email",
            "type": "string",
            "description": "Comma-separated stakeholder emails",
            "required": True,
        },
        {
            "name": "metrics_channel",
            "type": "string",
            "description": "Slack metrics channel",
            "required": True,
        },
        {
            "name": "timezone",
            "type": "string",
            "description": "Timezone for schedule",
            "required": True,
        },
    ],
)


DATA_SYNC = WorkflowTemplate(
    id="tpl_data_sync",
    name="Airtable to Google Sheets Sync",
    description="When Airtable records are updated, sync changes to Google Sheets and notify stakeholders.",
    category=TemplateCategory.DATA,
    icon="🔄",
    tags=["sync", "airtable", "google-sheets", "data", "automation"],
    estimated_time_saved="4 hours/week",
    popularity=65,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "Airtable Webhook",
            "config": {"path": "/airtable/updated", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "airtable_1",
            "type": "airtable",
            "name": "Fetch Updated Records",
            "config": {
                "operation": "list_records",
                "filter": "LAST_MODIFIED_TIME() > '{{last_sync_time}}'",
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "loop_1",
            "type": "loop",
            "name": "For Each Record",
            "config": {"items_path": "records"},
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "http_1",
            "type": "http_request",
            "name": "Update Google Sheet",
            "config": {
                "url": "https://sheets.googleapis.com/v4/spreadsheets/{{sheet_id}}/values/{{range}}:append",
                "method": "POST",
                "headers": {"Authorization": "Bearer {{google_token}}"},
                "body": {"values": [["{{record_data}}"]]},
            },
            "position": {"x": 700, "y": 100},
        },
        {
            "id": "slack_1",
            "type": "slack_webhook",
            "name": "Notify Sync Complete",
            "config": {
                "channel": "{{data_channel}}",
                "message": "🔄 Data sync complete: {{record_count}} records synced from Airtable to Google Sheets",
            },
            "position": {"x": 900, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "airtable_1"},
        {"from": "airtable_1", "to": "loop_1"},
        {"from": "loop_1", "to": "http_1"},
        {"from": "http_1", "to": "slack_1"},
    ],
    variables=[
        {
            "name": "sheet_id",
            "type": "string",
            "description": "Google Sheets spreadsheet ID",
            "required": True,
        },
        {
            "name": "data_channel",
            "type": "string",
            "description": "Slack data team channel",
            "required": True,
        },
    ],
)


# ============================================================================
# SOCIAL MEDIA TEMPLATES
# ============================================================================

CROSS_POST = WorkflowTemplate(
    id="tpl_cross_post",
    name="Multi-Platform Cross-Post",
    description="New blog post triggers AI-adapted posts to Twitter, LinkedIn, Facebook, and YouTube community tab.",
    category=TemplateCategory.SOCIAL,
    icon="📣",
    tags=["social", "cross-post", "twitter", "linkedin", "facebook", "youtube"],
    estimated_time_saved="6 hours/week",
    popularity=80,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "New Blog Post",
            "config": {"path": "/blog/published", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "ai_1",
            "type": "openai",
            "name": "Adapt Content",
            "config": {
                "operation": "chat",
                "model": "gpt-4",
                "messages": [
                    {
                        "role": "system",
                        "content": "Adapt this blog post for social media. Return JSON: {twitter: '280 chars max with hashtags', linkedin: 'professional tone 500 chars', facebook: 'casual tone 300 chars'}",
                    },
                    {
                        "role": "user",
                        "content": "Title: {{post_title}}\nExcerpt: {{post_excerpt}}\nURL: {{post_url}}",
                    },
                ],
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "twitter_1",
            "type": "twitter",
            "name": "Post to Twitter/X",
            "config": {"operation": "post_tweet", "text": "{{twitter_content}}"},
            "position": {"x": 500, "y": 50},
        },
        {
            "id": "linkedin_1",
            "type": "linkedin",
            "name": "Post to LinkedIn",
            "config": {"operation": "share_post", "text": "{{linkedin_content}}"},
            "position": {"x": 500, "y": 150},
        },
        {
            "id": "facebook_1",
            "type": "facebook",
            "name": "Post to Facebook",
            "config": {
                "action": "create_post",
                "page_id": "{{fb_page_id}}",
                "message": "{{facebook_content}}",
                "link": "{{post_url}}",
            },
            "position": {"x": 500, "y": 250},
        },
        {
            "id": "airtable_1",
            "type": "airtable",
            "name": "Track Posts",
            "config": {
                "operation": "create_record",
                "fields": {
                    "Blog Title": "{{post_title}}",
                    "Twitter": "Posted",
                    "LinkedIn": "Posted",
                    "Facebook": "Posted",
                    "Date": "{{today}}",
                },
            },
            "position": {"x": 700, "y": 150},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "ai_1"},
        {"from": "ai_1", "to": "twitter_1"},
        {"from": "ai_1", "to": "linkedin_1"},
        {"from": "ai_1", "to": "facebook_1"},
        {"from": "twitter_1", "to": "airtable_1"},
        {"from": "linkedin_1", "to": "airtable_1"},
        {"from": "facebook_1", "to": "airtable_1"},
    ],
    variables=[
        {
            "name": "fb_page_id",
            "type": "string",
            "description": "Facebook page ID",
            "required": True,
        },
    ],
)


SOCIAL_MONITORING = WorkflowTemplate(
    id="tpl_social_monitoring",
    name="Social Media Sentiment Monitor",
    description="Monitor brand mentions, run AI sentiment analysis, alert on negative mentions, auto-create support tickets.",
    category=TemplateCategory.SOCIAL,
    icon="👁️",
    tags=["social", "monitoring", "sentiment", "brand", "alerts"],
    estimated_time_saved="8 hours/week",
    popularity=71,
    nodes=[
        {
            "id": "trigger_1",
            "type": "schedule",
            "name": "Check Every 15 min",
            "config": {"cron": "*/15 * * * *", "timezone": "UTC"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "twitter_1",
            "type": "twitter",
            "name": "Search Mentions",
            "config": {
                "operation": "search_tweets",
                "query": "{{brand_name}} OR @{{brand_handle}}",
            },
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "ai_1",
            "type": "openai",
            "name": "Sentiment Analysis",
            "config": {
                "operation": "chat",
                "model": "gpt-4",
                "messages": [
                    {
                        "role": "system",
                        "content": "Analyze sentiment of these social media mentions. Return JSON array: [{id, sentiment: 'positive'|'neutral'|'negative', urgency: 1-5, summary}]",
                    },
                    {"role": "user", "content": "Mentions: {{mentions}}"},
                ],
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "filter_1",
            "type": "filter",
            "name": "Filter Negative",
            "config": {"field": "sentiment", "operator": "==", "value": "negative"},
            "position": {"x": 700, "y": 100},
        },
        {
            "id": "intercom_1",
            "type": "intercom",
            "name": "Create Support Ticket",
            "config": {
                "action": "create_ticket",
                "title": "Negative mention from {{mention_author}}",
                "body": "{{mention_text}}\n\nSentiment: {{sentiment}}\nUrgency: {{urgency}}",
            },
            "position": {"x": 900, "y": 50},
        },
        {
            "id": "slack_1",
            "type": "slack_webhook",
            "name": "Alert PR Team",
            "config": {
                "channel": "{{pr_channel}}",
                "message": "⚠️ Negative mention detected:\n{{mention_text}}\nFrom: {{mention_author}}\nUrgency: {{urgency}}/5",
            },
            "position": {"x": 900, "y": 150},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "twitter_1"},
        {"from": "twitter_1", "to": "ai_1"},
        {"from": "ai_1", "to": "filter_1"},
        {"from": "filter_1", "to": "intercom_1"},
        {"from": "filter_1", "to": "slack_1"},
    ],
    variables=[
        {
            "name": "brand_name",
            "type": "string",
            "description": "Your brand name to monitor",
            "required": True,
        },
        {
            "name": "brand_handle",
            "type": "string",
            "description": "Your Twitter/X handle",
            "required": True,
        },
        {
            "name": "pr_channel",
            "type": "string",
            "description": "Slack PR/communications channel",
            "required": True,
        },
    ],
)


# ============================================================================
# E-COMMERCE TEMPLATES
# ============================================================================

ABANDONED_CART = WorkflowTemplate(
    id="tpl_abandoned_cart",
    name="Abandoned Cart Recovery",
    description="Detect abandoned carts, wait 1 hour, send reminder email, wait 24 hours, send discount offer.",
    category=TemplateCategory.ECOMMERCE,
    icon="🛒",
    tags=["ecommerce", "abandoned-cart", "email", "recovery", "shopify"],
    estimated_time_saved="15 hours/week",
    popularity=95,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "Cart Abandoned",
            "config": {"path": "/carts/abandoned", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "delay_1",
            "type": "delay",
            "name": "Wait 1 Hour",
            "config": {"duration": 3600, "unit": "seconds"},
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "shopify_1",
            "type": "shopify",
            "name": "Check Cart Status",
            "config": {"operation": "get_checkout", "checkout_id": "{{cart_id}}"},
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Still Abandoned?",
            "config": {
                "conditions": [
                    {
                        "field": "completed",
                        "operator": "==",
                        "value": False,
                        "next": "reminder_email",
                    },
                    {
                        "field": "completed",
                        "operator": "==",
                        "value": True,
                        "next": "end",
                    },
                ]
            },
            "position": {"x": 700, "y": 100},
        },
        {
            "id": "reminder_email",
            "type": "sendgrid",
            "name": "Send Reminder",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "You left something behind!",
                "body": "Hi {{customer_name}}, you have items waiting in your cart. Complete your purchase: {{cart_url}}",
            },
            "position": {"x": 900, "y": 50},
        },
        {
            "id": "delay_2",
            "type": "delay",
            "name": "Wait 24 Hours",
            "config": {"duration": 86400, "unit": "seconds"},
            "position": {"x": 1100, "y": 50},
        },
        {
            "id": "shopify_2",
            "type": "shopify",
            "name": "Recheck Cart",
            "config": {"operation": "get_checkout", "checkout_id": "{{cart_id}}"},
            "position": {"x": 1300, "y": 50},
        },
        {
            "id": "condition_2",
            "type": "condition",
            "name": "Still Abandoned?",
            "config": {
                "conditions": [
                    {
                        "field": "completed",
                        "operator": "==",
                        "value": False,
                        "next": "discount_email",
                    },
                ]
            },
            "position": {"x": 1500, "y": 50},
        },
        {
            "id": "discount_email",
            "type": "sendgrid",
            "name": "Send Discount",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "{{discount_percent}}% off - just for you!",
                "body": "Hi {{customer_name}}, use code {{discount_code}} for {{discount_percent}}% off your cart: {{cart_url}}",
            },
            "position": {"x": 1700, "y": 50},
        },
        {
            "id": "end",
            "type": "http_request",
            "name": "Log Conversion",
            "config": {
                "url": "{{analytics_api}}/events",
                "method": "POST",
                "body": {"event": "cart_recovered", "cart_id": "{{cart_id}}"},
            },
            "position": {"x": 900, "y": 200},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "delay_1"},
        {"from": "delay_1", "to": "shopify_1"},
        {"from": "shopify_1", "to": "condition_1"},
        {"from": "condition_1", "to": "reminder_email", "condition": "abandoned"},
        {"from": "condition_1", "to": "end", "condition": "completed"},
        {"from": "reminder_email", "to": "delay_2"},
        {"from": "delay_2", "to": "shopify_2"},
        {"from": "shopify_2", "to": "condition_2"},
        {"from": "condition_2", "to": "discount_email", "condition": "still_abandoned"},
    ],
    variables=[
        {
            "name": "from_email",
            "type": "string",
            "description": "Sender email for cart reminders",
            "required": True,
        },
        {
            "name": "discount_code",
            "type": "string",
            "description": "Discount code to offer",
            "required": True,
        },
        {
            "name": "discount_percent",
            "type": "string",
            "description": "Discount percentage",
            "required": True,
        },
    ],
)


ORDER_STATUS_UPDATES = WorkflowTemplate(
    id="tpl_order_status",
    name="Order Status Notifications",
    description="Shopify order status changes trigger email updates, SMS via Twilio, and tracking dashboard update.",
    category=TemplateCategory.ECOMMERCE,
    icon="📦",
    tags=["ecommerce", "orders", "tracking", "sms", "notifications"],
    estimated_time_saved="8 hours/week",
    popularity=77,
    nodes=[
        {
            "id": "trigger_1",
            "type": "webhook",
            "name": "Order Updated",
            "config": {"path": "/orders/status-changed", "method": "POST"},
            "position": {"x": 100, "y": 100},
        },
        {
            "id": "shopify_1",
            "type": "shopify",
            "name": "Get Order Details",
            "config": {"operation": "get_order", "order_id": "{{order_id}}"},
            "position": {"x": 300, "y": 100},
        },
        {
            "id": "condition_1",
            "type": "condition",
            "name": "Check Status",
            "config": {
                "conditions": [
                    {
                        "field": "fulfillment_status",
                        "operator": "==",
                        "value": "shipped",
                        "next": "shipped_path",
                    },
                    {
                        "field": "fulfillment_status",
                        "operator": "==",
                        "value": "delivered",
                        "next": "delivered_path",
                    },
                ]
            },
            "position": {"x": 500, "y": 100},
        },
        {
            "id": "shipped_path",
            "type": "sendgrid",
            "name": "Shipped Email",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "Your order has shipped! 📦",
                "body": "Hi {{customer_name}}, your order #{{order_number}} has shipped. Tracking: {{tracking_url}}",
            },
            "position": {"x": 700, "y": 50},
        },
        {
            "id": "delivered_path",
            "type": "sendgrid",
            "name": "Delivered Email",
            "config": {
                "from_email": "{{from_email}}",
                "subject": "Your order has been delivered! ✅",
                "body": "Hi {{customer_name}}, your order #{{order_number}} has been delivered. Enjoy!",
            },
            "position": {"x": 700, "y": 150},
        },
        {
            "id": "sms_1",
            "type": "twilio",
            "name": "SMS Update",
            "config": {
                "operation": "send_sms",
                "to": "{{customer_phone}}",
                "body": "Order #{{order_number}} update: {{fulfillment_status}}. {{tracking_url}}",
            },
            "position": {"x": 900, "y": 100},
        },
    ],
    connections=[
        {"from": "trigger_1", "to": "shopify_1"},
        {"from": "shopify_1", "to": "condition_1"},
        {"from": "condition_1", "to": "shipped_path", "condition": "shipped"},
        {"from": "condition_1", "to": "delivered_path", "condition": "delivered"},
        {"from": "shipped_path", "to": "sms_1"},
        {"from": "delivered_path", "to": "sms_1"},
    ],
    variables=[
        {
            "name": "from_email",
            "type": "string",
            "description": "Sender email for order updates",
            "required": True,
        },
    ],
)


# ============================================================================
# TEMPLATE REGISTRY
# ============================================================================

TEMPLATE_REGISTRY: dict[str, WorkflowTemplate] = {
    # Original 5 templates
    "tpl_lead_nurture": LEAD_NURTURE_TEMPLATE,
    "tpl_social_scheduler": SOCIAL_MEDIA_SCHEDULER,
    "tpl_support_triage": SUPPORT_TICKET_TRIAGE,
    "tpl_order_fulfillment": ORDER_FULFILLMENT,
    "tpl_coordination_sync": COORDINATION_SYNC,
    # Marketing & Lead Gen
    "tpl_subscriber_welcome": NEW_SUBSCRIBER_WELCOME,
    "tpl_webinar_registration": WEBINAR_REGISTRATION,
    "tpl_lead_scoring": LEAD_SCORING,
    # Sales & CRM
    "tpl_crm_deal_alerts": CRM_DEAL_ALERTS,
    "tpl_invoice_automation": INVOICE_AUTOMATION,
    # Customer Support
    "tpl_escalation_workflow": ESCALATION_WORKFLOW,
    "tpl_customer_feedback": CUSTOMER_FEEDBACK_LOOP,
    # DevOps & Engineering
    "tpl_deploy_notification": DEPLOY_NOTIFICATION,
    "tpl_incident_response": INCIDENT_RESPONSE,
    # Data & Analytics
    "tpl_daily_metrics": DAILY_METRICS_DIGEST,
    "tpl_data_sync": DATA_SYNC,
    # Social Media
    "tpl_cross_post": CROSS_POST,
    "tpl_social_monitoring": SOCIAL_MONITORING,
    # E-Commerce
    "tpl_abandoned_cart": ABANDONED_CART,
    "tpl_order_status": ORDER_STATUS_UPDATES,
}


# =============================================================================
# BRIDGE: Import workflow_engine templates into unified Spirals registry
# =============================================================================

# Category mapping from workflow_engine → Spirals
_WE_CATEGORY_MAP = {
    "business": TemplateCategory.PRODUCTIVITY,
    "ai_agents": TemplateCategory.AI,
    "data": TemplateCategory.DATA,
    "communication": TemplateCategory.PRODUCTIVITY,
    "development": TemplateCategory.DEVELOPMENT,
    "marketing": TemplateCategory.MARKETING,
    "support": TemplateCategory.SUPPORT,
    "ecommerce": TemplateCategory.ECOMMERCE,
    "analytics": TemplateCategory.DATA,
    "coordination": TemplateCategory.COORDINATION,
}

# Icon defaults by category
_CATEGORY_ICONS = {
    TemplateCategory.PRODUCTIVITY: "📋",
    TemplateCategory.AI: "🤖",
    TemplateCategory.DATA: "📊",
    TemplateCategory.DEVELOPMENT: "💻",
    TemplateCategory.MARKETING: "📣",
    TemplateCategory.SUPPORT: "🎧",
    TemplateCategory.ECOMMERCE: "🛒",
    TemplateCategory.COORDINATION: "🧠",
    TemplateCategory.SOCIAL: "📱",
    TemplateCategory.SALES: "💰",
}


def _import_workflow_engine_templates() -> dict[str, WorkflowTemplate]:
    """
    Import templates from workflow_engine module and convert to Spirals format.

    The workflow_engine has 53 curated templates using WorkflowNode/WorkflowEdge objects.
    This function converts them to Spirals dict-based format and adds them to the
    unified TEMPLATE_REGISTRY with 'we-' prefix on IDs to avoid collisions.

    Returns dict of imported templates (also adds them to TEMPLATE_REGISTRY).
    """
    imported = {}

    try:
        from apps.backend.workflow_engine.templates import template_registry as we_registry
    except (ImportError, Exception):
        # workflow_engine not available — gracefully skip
        return imported

    # Collect existing template names (lowercased) for dedup
    existing_names = {tpl.name.lower().strip() for tpl in TEMPLATE_REGISTRY.values()}

    for we_id, we_template in we_registry._templates.items():
        # Skip duplicates by name similarity
        if we_template.name.lower().strip() in existing_names:
            continue

        # Convert workflow_engine category to Spirals category
        we_cat_value = (
            we_template.category.value if hasattr(we_template.category, "value") else str(we_template.category)
        )
        spirals_category = _WE_CATEGORY_MAP.get(we_cat_value, TemplateCategory.PRODUCTIVITY)

        # Convert WorkflowNode objects to Spirals node dicts
        nodes = []
        if hasattr(we_template, "workflow") and we_template.workflow:
            for node in we_template.workflow.nodes:
                node_dict = node.to_dict() if hasattr(node, "to_dict") else {}
                nodes.append(
                    {
                        "id": node_dict.get("id", ""),
                        "type": node_dict.get("type", "action"),
                        "label": node_dict.get("name", ""),
                        "config": node_dict.get("config", {}),
                        "position": node_dict.get("position", {"x": 0, "y": 0}),
                    }
                )

            # Convert WorkflowEdge objects to Spirals connection dicts
            connections = []
            for edge in we_template.workflow.edges:
                edge_dict = edge.to_dict() if hasattr(edge, "to_dict") else {}
                connections.append(
                    {
                        "source": edge_dict.get("source", ""),
                        "target": edge_dict.get("target", ""),
                    }
                )
        else:
            connections = []

        # Build Spirals WorkflowTemplate
        registry_key = "we_{}".format(we_id.replace("-", "_"))
        spirals_template = WorkflowTemplate(
            id=registry_key,
            name=we_template.name,
            description=we_template.description,
            category=spirals_category,
            icon=_CATEGORY_ICONS.get(spirals_category, "⚡"),
            tags=(we_template.tags if hasattr(we_template, "tags") and we_template.tags else []),
            nodes=nodes,
            connections=connections,
            variables=[],
            estimated_time_saved="Varies",
            popularity=(we_template.popularity if hasattr(we_template, "popularity") else 50),
        )

        imported[registry_key] = spirals_template
        existing_names.add(we_template.name.lower().strip())

    return imported


# Run the import and merge into TEMPLATE_REGISTRY
_imported_we_templates = _import_workflow_engine_templates()
TEMPLATE_REGISTRY.update(_imported_we_templates)


def _import_marketplace_seed_templates() -> dict[str, WorkflowTemplate]:
    """
    Import seed templates from the marketplace module.

    The marketplace has 5 built-in templates with workflow_definition dicts.
    Convert them to the unified Spirals WorkflowTemplate format.
    """
    imported = {}

    try:
        from .marketplace.workflow_templates import BUILTIN_TEMPLATES as MP_TEMPLATES
    except (ImportError, Exception):
        return imported

    # Collect existing names for dedup
    existing_names = {tpl.name.lower().strip() for tpl in TEMPLATE_REGISTRY.values()}

    # Category mapping from marketplace enum values
    mp_cat_map = {
        "marketing": TemplateCategory.MARKETING,
        "sales": TemplateCategory.SALES,
        "customer_support": TemplateCategory.SUPPORT,
        "hr": TemplateCategory.PRODUCTIVITY,
        "finance": TemplateCategory.PRODUCTIVITY,
        "operations": TemplateCategory.PRODUCTIVITY,
        "development": TemplateCategory.DEVELOPMENT,
        "data": TemplateCategory.DATA,
        "ai_ml": TemplateCategory.AI,
        "social_media": TemplateCategory.SOCIAL,
        "e_commerce": TemplateCategory.ECOMMERCE,
        "productivity": TemplateCategory.PRODUCTIVITY,
        "communication": TemplateCategory.PRODUCTIVITY,
        "analytics": TemplateCategory.DATA,
        "security": TemplateCategory.DEVELOPMENT,
        "custom": TemplateCategory.PRODUCTIVITY,
    }

    for idx, mp_tpl in enumerate(MP_TEMPLATES):
        name = mp_tpl.get("name", "")
        if name.lower().strip() in existing_names:
            continue

        # Map category
        cat_enum = mp_tpl.get("category")
        cat_value = cat_enum.value if hasattr(cat_enum, "value") else str(cat_enum)
        spirals_cat = mp_cat_map.get(cat_value, TemplateCategory.PRODUCTIVITY)

        # Extract nodes and connections from workflow_definition
        wf_def = mp_tpl.get("workflow_definition", {})
        nodes = wf_def.get("nodes", [])
        edges = wf_def.get("edges", [])
        connections = [{"source": e.get("from", ""), "target": e.get("to", "")} for e in edges]

        registry_key = f"mp_{idx}"
        spirals_template = WorkflowTemplate(
            id=registry_key,
            name=name,
            description=mp_tpl.get("description", ""),
            category=spirals_cat,
            icon=_CATEGORY_ICONS.get(spirals_cat, "⚡"),
            tags=mp_tpl.get("tags", []),
            nodes=nodes,
            connections=connections,
            variables=[],
            estimated_time_saved="Varies",
            popularity=70,
        )

        imported[registry_key] = spirals_template
        existing_names.add(name.lower().strip())

    return imported


_imported_mp_templates = _import_marketplace_seed_templates()
TEMPLATE_REGISTRY.update(_imported_mp_templates)


def get_template(template_id: str) -> WorkflowTemplate | None:
    """Get a template by ID."""
    return TEMPLATE_REGISTRY.get(template_id)


def list_templates(category: TemplateCategory | None = None) -> list[dict[str, Any]]:
    """List all templates, optionally filtered by category."""
    templates = []
    for template in TEMPLATE_REGISTRY.values():
        if category is None or template.category == category:
            templates.append(
                {
                    "id": template.id,
                    "name": template.name,
                    "description": template.description,
                    "category": template.category.value,
                    "icon": template.icon,
                    "tags": template.tags,
                    "estimated_time_saved": template.estimated_time_saved,
                    "popularity": template.popularity,
                }
            )
    return sorted(templates, key=lambda x: x["popularity"], reverse=True)


def search_templates(query: str) -> list[dict[str, Any]]:
    """Search templates by name, description, or tags."""
    query_lower = query.lower()
    results = []

    for template in TEMPLATE_REGISTRY.values():
        score = 0
        if query_lower in template.name.lower():
            score += 10
        if query_lower in template.description.lower():
            score += 5
        if any(query_lower in tag.lower() for tag in template.tags):
            score += 3

        if score > 0:
            results.append(
                {
                    "template": {
                        "id": template.id,
                        "name": template.name,
                        "description": template.description,
                        "category": template.category.value,
                        "icon": template.icon,
                        "tags": template.tags,
                    },
                    "score": score,
                }
            )

    return sorted(results, key=lambda x: x["score"], reverse=True)


def instantiate_template(template_id: str, variables: dict[str, Any] = None) -> dict[str, Any] | None:
    """Create a new workflow from a template."""
    template = get_template(template_id)
    if template:
        return template.instantiate(variables)
    return None
