"""
Helix Workflow Engine - n8n Competitor
========================================

A visual workflow automation platform built on Helix Chains.

Features:
- Visual node-based workflow builder
- 500+ integrations
- 14 Helix Collective agents (Kael, Lumina, Vega, Kavach, etc.)
- 50+ workflow templates
- Cron-based scheduling
- Conditional branching and parallel execution
- Webhook triggers
- Data transformations
- Error handling and retries
"""

# 14 Helix Collective Agents
from .agents import (  # Individual agents; Convenience functions
    AetherAgent,
    AgentPersonality,
    AgentRole,
    AgniAgent,
    EchoAgent,
    GeminiAgent,
    HelixAgent,
    HelixCollective,
    KaelAgent,
    KavachAgent,
    LuminaAgent,
    OracleAgent,
    PhoenixAgent,
    SanghaCoreAgent,
    ShadowAgent,
    UCFMetrics,
    VegaAgent,
    get_agent,
    get_collective,
    helix_collective,
    list_all_agents,
)

# API router
from .api import router

# Core workflow engine
from .core import (  # Node types
    CodeAction,
    ConditionalNode,
    DatabaseAction,
    HttpRequestAction,
    NodeType,
    ToolAction,
    TransformAction,
    WebhookTrigger,
    Workflow,
    WorkflowEdge,
    WorkflowEngine,
    WorkflowExecution,
    WorkflowNode,
    WorkflowStatus,
)

# Integrations
from .integrations import (
    DatabaseIntegration,
    FileSystemIntegration,
    Integration,
    IntegrationRegistry,
    RestApiIntegration,
    WebhookIntegration,
    create_integration_registry,
    get_popular_integrations,
)

# Scheduler
from .scheduler import (  # Convenience functions; Common cron expressions
    CRON_DAILY_9AM,
    CRON_DAILY_MIDNIGHT,
    CRON_EVERY_2_HOURS,
    CRON_EVERY_5_MINUTES,
    CRON_EVERY_6_HOURS,
    CRON_EVERY_12_HOURS,
    CRON_EVERY_15_MINUTES,
    CRON_EVERY_30_MINUTES,
    CRON_EVERY_HOUR,
    CRON_EVERY_MINUTE,
    CRON_MONTHLY_FIRST,
    CRON_WEEKLY_FRIDAY,
    CRON_WEEKLY_MONDAY,
    CronParser,
    ScheduleConfig,
    ScheduledWorkflow,
    ScheduleExecution,
    ScheduleType,
    WorkflowScheduler,
    create_cron_schedule,
    create_interval_schedule,
    create_once_schedule,
)

# Workflow Templates
from .templates import (
    TemplateCategory,
    TemplateRegistry,
    WorkflowTemplate,
    get_template,
    get_template_registry,
    list_templates,
    search_templates,
    template_registry,
)

__version__ = "1.0.0"
__all__ = [
    "HelixAgent",
    # Agents
    "HelixCollective",
    "Integration",
    # Integrations
    "IntegrationRegistry",
    "NodeType",
    "ScheduleConfig",
    "ScheduleType",
    "ScheduledWorkflow",
    # Templates
    "TemplateRegistry",
    "Workflow",
    "WorkflowEdge",
    # Core
    "WorkflowEngine",
    "WorkflowExecution",
    "WorkflowNode",
    # Scheduler
    "WorkflowScheduler",
    "WorkflowStatus",
    "WorkflowTemplate",
    "get_agent",
    "get_collective",
    "get_template",
    "helix_collective",
    "list_all_agents",
    "list_templates",
    # API
    "router",
    "search_templates",
    "template_registry",
]
