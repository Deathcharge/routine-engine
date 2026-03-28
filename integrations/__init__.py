"""
Helix Spirals Integration Connectors Package.

Provides comprehensive integration connectors for third-party services,
enabling seamless workflow automation. Includes both base integration
classes (httpx-based) and full connector implementations (aiohttp-based)
with Node wrappers for the Spirals workflow builder.

Base integrations: SlackIntegration, NotionIntegration, etc. (from base.py)
Full connectors: MailchimpConnector/Node, CalendlyConnector/Node, etc.
"""

import logging

logger = logging.getLogger(__name__)

# Re-export base INTEGRATION_REGISTRY and helper
# =============================================================
# Base integrations (httpx-based, from original integrations.py)
# =============================================================
from .base import (
    INTEGRATION_REGISTRY as _BASE_REGISTRY,
    AirtableIntegration,
    GitHubIntegration,
    GoogleSheetsIntegration,
    HubSpotIntegration,
    IntegrationBase,
    NotionIntegration,
    OpenAIIntegration,
    SlackIntegration,
    TrelloIntegration,
    TwilioIntegration,
    get_available_integrations as _base_get_available,
)

# =============================================================
# Full connector modules (aiohttp-based, with Node wrappers)
# =============================================================

# --- Airtable ---
try:
    from .airtable_connector import (
        AirtableConfig,
        AirtableConnector,
        AirtableFieldType,
        AirtableNode,
    )
except ImportError as e:
    logger.warning("Airtable connector unavailable: %s", e)
    AirtableConfig = AirtableConnector = AirtableFieldType = AirtableNode = None

# --- AWS ---
try:
    from .aws_connector import (
        AWSConfig,
        AWSConnector,
        EC2Instance,
        LambdaFunction,
        S3Object,
    )
except ImportError as e:
    logger.warning("AWS connector unavailable: %s", e)
    AWSConfig = AWSConnector = EC2Instance = LambdaFunction = S3Object = None

# --- Calendly ---
try:
    from .calendly_connector import (
        CalendlyConfig,
        CalendlyConnector,
        CalendlyEventStatus,
        CalendlyInviteeStatus,
        CalendlyNode,
    )
except ImportError as e:
    logger.warning("Calendly connector unavailable: %s", e)
    CalendlyConfig = CalendlyConnector = CalendlyEventStatus = None
    CalendlyInviteeStatus = CalendlyNode = None

# --- Discord ---
try:
    from .discord_connector import (
        DiscordChannel,
        DiscordConfig,
        DiscordConnector,
        DiscordGuild,
        DiscordMessage,
    )
except ImportError as e:
    logger.warning("Discord connector unavailable: %s", e)
    DiscordChannel = DiscordConfig = DiscordConnector = None
    DiscordGuild = DiscordMessage = None

# --- Generic HTTP ---
try:
    from .generic_http_connector import (
        CONNECTOR_TEMPLATES,
        AuthConfig,
        AuthType,
        ContentType,
        GenericHttpConfig,
        GenericHttpConnector,
        HttpMethod,
        HttpResponse,
        RequestConfig,
        ResponseConfig,
        RetryConfig,
        WebhookReceiver,
        create_connector,
    )
except ImportError as e:
    logger.warning("Generic HTTP connector unavailable: %s", e)
    CONNECTOR_TEMPLATES = {}
    AuthConfig = AuthType = ContentType = GenericHttpConfig = None
    GenericHttpConnector = HttpMethod = HttpResponse = None
    RequestConfig = ResponseConfig = RetryConfig = None
    WebhookReceiver = create_connector = None

# --- Google Cloud ---
try:
    from .google_cloud_connector import (
        CloudFunction,
        ComputeInstance,
        GCSObject,
        GoogleCloudConfig,
        GoogleCloudConnector,
    )
except ImportError as e:
    logger.warning("Google Cloud connector unavailable: %s", e)
    CloudFunction = ComputeInstance = GCSObject = None
    GoogleCloudConfig = GoogleCloudConnector = None

# --- Mailchimp ---
try:
    from .mailchimp_connector import (
        MailchimpCampaignType,
        MailchimpConfig,
        MailchimpConnector,
        MailchimpMemberStatus,
        MailchimpNode,
    )
except ImportError as e:
    logger.warning("Mailchimp connector unavailable: %s", e)
    MailchimpCampaignType = MailchimpConfig = MailchimpConnector = None
    MailchimpMemberStatus = MailchimpNode = None

# --- Notion ---
try:
    from .notion_connector import (
        NotionBlock,
        NotionConfig,
        NotionConnector,
        NotionDatabase,
        NotionPage,
    )
except ImportError as e:
    logger.warning("Notion connector unavailable: %s", e)
    NotionBlock = NotionConfig = NotionConnector = None
    NotionDatabase = NotionPage = None

# --- SendGrid ---
try:
    from .sendgrid_connector import (
        SendGridConfig,
        SendGridConnector,
        SendGridEventType,
        SendGridNode,
    )
except ImportError as e:
    logger.warning("SendGrid connector unavailable: %s", e)
    SendGridConfig = SendGridConnector = SendGridEventType = SendGridNode = None

# --- Slack ---
try:
    from .slack_connector import SlackConnector
except ImportError as e:
    logger.warning("Slack connector unavailable: %s", e)
    SlackConnector = None

# --- Stripe ---
try:
    from .stripe_connector import (
        StripeChargeStatus,
        StripeConfig,
        StripeConnector,
        StripeNode,
        StripeSubscriptionStatus,
    )
except ImportError as e:
    logger.warning("Stripe connector unavailable: %s", e)
    StripeChargeStatus = StripeConfig = StripeConnector = None
    StripeNode = StripeSubscriptionStatus = None

# --- Twilio ---
try:
    from .twilio_connector import (
        TwilioCallStatus,
        TwilioConfig,
        TwilioConnector,
        TwilioMessageType,
        TwilioNode,
    )
except ImportError as e:
    logger.warning("Twilio connector unavailable: %s", e)
    TwilioCallStatus = TwilioConfig = TwilioConnector = None
    TwilioMessageType = TwilioNode = None


# =============================================================
# Merged INTEGRATION_REGISTRY
# =============================================================

# Start with base registry
INTEGRATION_REGISTRY = dict(_BASE_REGISTRY)

# Extend with full connector entries (if available)
_CONNECTOR_REGISTRY = {}

if MailchimpConnector:
    _CONNECTOR_REGISTRY["mailchimp"] = {
        "class": MailchimpConnector,
        "auth_type": "api_key",
        "provider": "MailChimp",
        "actions": [
            "get_campaigns",
            "create_campaign",
            "send_campaign",
            "get_lists",
            "add_subscriber",
            "remove_subscriber",
        ],
        "description": "Manage MailChimp email campaigns and subscriber lists",
    }

if CalendlyConnector:
    _CONNECTOR_REGISTRY["calendly"] = {
        "class": CalendlyConnector,
        "auth_type": "oauth",
        "provider": "Calendly",
        "actions": [
            "get_events",
            "get_event_details",
            "get_event_types",
            "create_event_type",
            "cancel_event",
        ],
        "description": "Schedule and manage Calendly events and appointments",
    }

if AWSConnector:
    _CONNECTOR_REGISTRY["aws"] = {
        "class": AWSConnector,
        "auth_type": "api_key",
        "provider": "Amazon Web Services",
        "actions": [
            "list_s3_objects",
            "upload_s3_object",
            "download_s3_object",
            "list_ec2_instances",
            "start_ec2_instance",
            "stop_ec2_instance",
            "invoke_lambda",
        ],
        "description": "Manage AWS resources including S3, EC2, and Lambda functions",
    }

if DiscordConnector:
    _CONNECTOR_REGISTRY["discord"] = {
        "class": DiscordConnector,
        "auth_type": "oauth",
        "provider": "Discord",
        "actions": [
            "send_message",
            "get_channels",
            "get_guilds",
            "create_channel",
            "delete_channel",
        ],
        "description": "Interact with Discord servers, channels, and messages",
    }

if GoogleCloudConnector:
    _CONNECTOR_REGISTRY["google_cloud"] = {
        "class": GoogleCloudConnector,
        "auth_type": "oauth",
        "provider": "Google Cloud",
        "actions": [
            "list_gcs_objects",
            "upload_gcs_object",
            "download_gcs_object",
            "list_compute_instances",
            "start_compute_instance",
            "stop_compute_instance",
            "invoke_cloud_function",
        ],
        "description": "Manage Google Cloud resources including GCS, Compute, and Cloud Functions",
    }

if NotionConnector:
    _CONNECTOR_REGISTRY["notion_full"] = {
        "class": NotionConnector,
        "auth_type": "oauth",
        "provider": "Notion",
        "actions": [
            "get_databases",
            "get_pages",
            "create_page",
            "update_page",
            "get_blocks",
            "append_block",
        ],
        "description": "Full Notion integration with databases, pages, and blocks",
    }

if SendGridConnector:
    _CONNECTOR_REGISTRY["sendgrid"] = {
        "class": SendGridConnector,
        "auth_type": "api_key",
        "provider": "SendGrid",
        "actions": [
            "send_email",
            "get_contacts",
            "add_contact",
            "remove_contact",
            "get_templates",
            "create_template",
        ],
        "description": "Send emails and manage contacts through SendGrid",
    }

if StripeConnector:
    _CONNECTOR_REGISTRY["stripe"] = {
        "class": StripeConnector,
        "auth_type": "api_key",
        "provider": "Stripe",
        "actions": [
            "create_charge",
            "get_charges",
            "create_customer",
            "get_customers",
            "create_subscription",
            "cancel_subscription",
        ],
        "description": "Process payments and manage subscriptions with Stripe",
    }

if TwilioConnector:
    _CONNECTOR_REGISTRY["twilio"] = {
        "class": TwilioConnector,
        "auth_type": "api_key",
        "provider": "Twilio",
        "actions": [
            "send_sms",
            "send_whatsapp",
            "make_call",
            "get_messages",
            "get_calls",
        ],
        "description": "Send SMS, WhatsApp messages, and make phone calls",
    }

if GenericHttpConnector:
    _CONNECTOR_REGISTRY["generic_http"] = {
        "class": GenericHttpConnector,
        "auth_type": "multiple",
        "provider": "Any REST API",
        "actions": ["get", "post", "put", "patch", "delete", "custom_request"],
        "description": "Connect to ANY REST API with custom authentication",
    }

if WebhookReceiver:
    _CONNECTOR_REGISTRY["webhook_receiver"] = {
        "class": WebhookReceiver,
        "auth_type": "webhook",
        "provider": "Incoming Webhooks",
        "actions": ["receive_webhook", "validate_signature", "route_event"],
        "description": "Receive webhooks from any external service",
    }

# Merge: connector registry entries override base entries
INTEGRATION_REGISTRY.update(_CONNECTOR_REGISTRY)


def get_available_integrations():
    """Get list of all available integrations with their metadata."""
    integrations = []
    for integration_id, config in INTEGRATION_REGISTRY.items():
        integrations.append(
            {
                "id": integration_id,
                "name": integration_id.replace("_", " ").title(),
                "auth_type": config["auth_type"],
                "provider": config.get("provider", integration_id.title()),
                "actions": config["actions"],
                "description": config["description"],
            }
        )
    return integrations


__all__ = [
    # Base classes
    "IntegrationBase",
    "SlackIntegration",
    "NotionIntegration",
    "GoogleSheetsIntegration",
    "AirtableIntegration",
    "GitHubIntegration",
    "OpenAIIntegration",
    "TwilioIntegration",
    "HubSpotIntegration",
    "TrelloIntegration",
    # Mailchimp
    "MailchimpConnector",
    "MailchimpConfig",
    "MailchimpNode",
    "MailchimpCampaignType",
    "MailchimpMemberStatus",
    # Calendly
    "CalendlyConnector",
    "CalendlyConfig",
    "CalendlyNode",
    "CalendlyEventStatus",
    "CalendlyInviteeStatus",
    # Airtable (Connector)
    "AirtableConnector",
    "AirtableConfig",
    "AirtableNode",
    "AirtableFieldType",
    # Twilio (Connector)
    "TwilioConnector",
    "TwilioConfig",
    "TwilioNode",
    "TwilioMessageType",
    "TwilioCallStatus",
    # SendGrid
    "SendGridConnector",
    "SendGridConfig",
    "SendGridNode",
    "SendGridEventType",
    # Stripe
    "StripeConnector",
    "StripeConfig",
    "StripeNode",
    "StripeChargeStatus",
    "StripeSubscriptionStatus",
    # AWS
    "AWSConnector",
    "AWSConfig",
    "S3Object",
    "EC2Instance",
    "LambdaFunction",
    # Google Cloud
    "GoogleCloudConnector",
    "GoogleCloudConfig",
    "GCSObject",
    "ComputeInstance",
    "CloudFunction",
    # Discord
    "DiscordConnector",
    "DiscordConfig",
    "DiscordMessage",
    "DiscordChannel",
    "DiscordGuild",
    # Notion (Connector)
    "NotionConnector",
    "NotionConfig",
    "NotionDatabase",
    "NotionPage",
    "NotionBlock",
    # Slack (Connector)
    "SlackConnector",
    # Generic HTTP
    "GenericHttpConnector",
    "GenericHttpConfig",
    "AuthConfig",
    "AuthType",
    "HttpMethod",
    "ContentType",
    "RequestConfig",
    "ResponseConfig",
    "RetryConfig",
    "HttpResponse",
    "WebhookReceiver",
    "create_connector",
    "CONNECTOR_TEMPLATES",
    # Registry
    "INTEGRATION_REGISTRY",
    "get_available_integrations",
]
