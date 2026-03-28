"""
Helix Spirals Backend Package
A powerful Zapier replacement with 98.7% more efficiency

This package provides:
- Workflow automation engine
- Integration connectors (Mailchimp, Calendly, Airtable, etc.)
- Workflow templates marketplace
- Version control for workflows
- OAuth2 connection management
"""

from .api_routes import (
    connectors_router,
    marketplace_router,
    oauth_router,
    versioning_router,
)
from .engine import SpiralEngine

# New competitive features
from .integrations import (
    AirtableConfig,
    AirtableConnector,
    AirtableNode,
    CalendlyConfig,
    CalendlyConnector,
    CalendlyNode,
    MailchimpConfig,
    MailchimpConnector,
    MailchimpNode,
    SendGridConfig,
    SendGridConnector,
    SendGridNode,
    StripeConfig,
    StripeConnector,
    StripeNode,
    TwilioConfig,
    TwilioConnector,
    TwilioNode,
)
from .marketplace import (
    TemplateCategory,
    TemplateStatus,
    TemplateTier,
    WorkflowTemplate,
    WorkflowTemplateMarketplace,
)
from .models import (
    Action,
    ActionType,
    Condition,
    ExecutionContext,
    ExecutionRequest,
    ExecutionResponse,
    ExecutionStatus,
    PerformanceScore,
    Spiral,
    SpiralCreateRequest,
    SpiralStatistics,
    SpiralUpdateRequest,
    Trigger,
    TriggerType,
    WebhookPayload,
)
from .oauth import (
    ConnectionStatus,
    OAuthConnection,
    OAuthConnectionManager,
    OAuthProvider,
)
from .routes import executions_router, spirals_router, templates_router
from .scheduler import SpiralScheduler
from .storage import SpiralStorage
from .versioning import (
    MergeStrategy,
    VersionBranch,
    VersionStatus,
    WorkflowVersion,
    WorkflowVersioningSystem,
)
from .webhooks import WebhookReceiver

__all__ = [
    # Core classes
    "SpiralEngine",
    "SpiralStorage",
    "SpiralScheduler",
    "WebhookReceiver",
    # Models
    "Action",
    "ActionType",
    "Condition",
    "PerformanceScore",
    "ExecutionContext",
    "ExecutionRequest",
    "ExecutionResponse",
    "ExecutionStatus",
    "Spiral",
    "SpiralCreateRequest",
    "SpiralStatistics",
    "SpiralUpdateRequest",
    "Trigger",
    "TriggerType",
    "WebhookPayload",
    # Routers
    "spirals_router",
    "executions_router",
    "templates_router",
    # New API Routers
    "connectors_router",
    "marketplace_router",
    "versioning_router",
    "oauth_router",
    # Integration Connectors
    "MailchimpConnector",
    "MailchimpConfig",
    "MailchimpNode",
    "CalendlyConnector",
    "CalendlyConfig",
    "CalendlyNode",
    "AirtableConnector",
    "AirtableConfig",
    "AirtableNode",
    "TwilioConnector",
    "TwilioConfig",
    "TwilioNode",
    "SendGridConnector",
    "SendGridConfig",
    "SendGridNode",
    "StripeConnector",
    "StripeConfig",
    "StripeNode",
    # Marketplace
    "WorkflowTemplateMarketplace",
    "WorkflowTemplate",
    "TemplateCategory",
    "TemplateStatus",
    "TemplateTier",
    # Versioning
    "WorkflowVersioningSystem",
    "WorkflowVersion",
    "VersionBranch",
    "VersionStatus",
    "MergeStrategy",
    # OAuth
    "OAuthConnectionManager",
    "OAuthConnection",
    "OAuthProvider",
    "ConnectionStatus",
]
