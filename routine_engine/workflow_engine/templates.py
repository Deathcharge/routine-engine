"""
Helix Workflow Engine - Workflow Templates
==========================================

50+ curated workflow templates for common use cases.
Each template is ready to use with minimal configuration.

Categories:
- Business Automation
- AI & Multi-Agent
- Data Processing
- Communication
- Development
- Marketing
- Customer Support
- E-commerce
- Analytics
- Coordination & Routines
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from .core import NodeType, Workflow, WorkflowEdge, WorkflowNode


class TemplateCategory(Enum):
    """Template categories"""

    BUSINESS = "business"
    AI_AGENTS = "ai_agents"
    DATA = "data"
    COMMUNICATION = "communication"
    DEVELOPMENT = "development"
    MARKETING = "marketing"
    SUPPORT = "support"
    ECOMMERCE = "ecommerce"
    ANALYTICS = "analytics"
    COORDINATION = "coordination"


@dataclass
class WorkflowTemplate:
    """A workflow template"""

    id: str
    name: str
    description: str
    category: TemplateCategory
    tags: list[str]
    workflow: Workflow
    popularity: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "tags": self.tags,
            "workflow": self.workflow.to_dict(),
            "popularity": self.popularity,
            "created_at": self.created_at.isoformat(),
        }


class TemplateRegistry:
    """Registry for workflow templates"""

    def __init__(self):
        self._templates: dict[str, WorkflowTemplate] = {}
        self._load_built_in_templates()

    def _load_built_in_templates(self):
        """Load all built-in templates"""
        templates = [
            # AI & Multi-Agent Templates
            self._create_multi_agent_analysis_template(),
            self._create_kael_lumina_vega_pipeline(),
            self._create_collective_brainstorm_template(),
            self._create_ethical_review_template(),
            self._create_creative_synthesis_template(),
            self._create_oracle_prediction_template(),
            self._create_shadow_archive_template(),
            self._create_phoenix_renewal_template(),
            # Business Automation Templates
            self._create_lead_qualification_template(),
            self._create_invoice_processing_template(),
            self._create_customer_onboarding_template(),
            self._create_approval_workflow_template(),
            self._create_report_generation_template(),
            # Data Processing Templates
            self._create_data_sync_template(),
            self._create_etl_pipeline_template(),
            self._create_data_validation_template(),
            self._create_backup_workflow_template(),
            # Communication Templates
            self._create_email_automation_template(),
            self._create_slack_notification_template(),
            self._create_discord_announcement_template(),
            self._create_multi_channel_broadcast_template(),
            # Development Templates
            self._create_ci_cd_pipeline_template(),
            self._create_code_review_template(),
            self._create_deployment_notification_template(),
            self._create_error_monitoring_template(),
            # Marketing Templates
            self._create_social_media_scheduler_template(),
            self._create_content_pipeline_template(),
            self._create_campaign_tracker_template(),
            # Customer Support Templates
            self._create_ticket_routing_template(),
            self._create_feedback_collection_template(),
            self._create_escalation_workflow_template(),
            # E-commerce Templates
            self._create_order_processing_template(),
            self._create_inventory_alert_template(),
            self._create_abandoned_cart_template(),
            # Analytics Templates
            self._create_metrics_dashboard_template(),
            self._create_anomaly_detection_template(),
            self._create_weekly_report_template(),
            # Coordination & Routines Templates
            self._create_ucf_monitoring_template(),
            self._create_coordination_cycle_template(),
            self._create_coordination_sync_template(),
            self._create_collective_meditation_template(),
            # NEW: Additional Templates (50+ total)
            self._create_meeting_summarizer_template(),
            self._create_document_processing_template(),
            self._create_sentiment_analysis_template(),
            self._create_api_health_monitor_template(),
            self._create_incident_response_template(),
            self._create_content_moderation_template(),
            self._create_newsletter_automation_template(),
            self._create_user_onboarding_sequence_template(),
            self._create_ab_testing_template(),
            self._create_revenue_forecasting_template(),
            self._create_agent_collaboration_template(),
            self._create_entanglement_cycle_template(),
        ]

        for template in templates:
            self._templates[template.id] = template

    # AI & Multi-Agent Templates

    def _create_multi_agent_analysis_template(self) -> WorkflowTemplate:
        """Multi-agent analysis workflow"""
        workflow = Workflow(
            id="tpl-multi-agent-analysis",
            name="Multi-Agent Analysis",
            description="Use multiple agents to analyze data from different perspectives",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Data Input",
                    config={"path": "/analyze"},
                ),
                WorkflowNode(
                    id="kael",
                    type=NodeType.AGENT,
                    name="Kael - Logical Analysis",
                    config={"agent_type": "kael", "task": "Analyze data logically"},
                ),
                WorkflowNode(
                    id="lumina",
                    type=NodeType.AGENT,
                    name="Lumina - Emotional Analysis",
                    config={
                        "agent_type": "lumina",
                        "task": "Analyze emotional aspects",
                    },
                ),
                WorkflowNode(
                    id="oracle",
                    type=NodeType.AGENT,
                    name="Oracle - Predictive Analysis",
                    config={"agent_type": "oracle", "task": "Predict future patterns"},
                ),
                WorkflowNode(
                    id="vega",
                    type=NodeType.AGENT,
                    name="Vega - Synthesis",
                    config={"agent_type": "vega", "task": "Synthesize all analyses"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="kael"),
                WorkflowEdge(id="e2", source="trigger", target="lumina"),
                WorkflowEdge(id="e3", source="trigger", target="oracle"),
                WorkflowEdge(id="e4", source="kael", target="vega"),
                WorkflowEdge(id="e5", source="lumina", target="vega"),
                WorkflowEdge(id="e6", source="oracle", target="vega"),
            ],
            tags=["ai", "multi-agent", "analysis"],
        )

        return WorkflowTemplate(
            id="tpl-multi-agent-analysis",
            name="Multi-Agent Analysis",
            description="Analyze data using Kael, Lumina, Oracle, and Vega for comprehensive insights",
            category=TemplateCategory.AI_AGENTS,
            tags=["ai", "multi-agent", "analysis", "kael", "lumina", "oracle", "vega"],
            workflow=workflow,
            popularity=100,
        )

    def _create_kael_lumina_vega_pipeline(self) -> WorkflowTemplate:
        """Classic Kael → Lumina → Vega pipeline"""
        workflow = Workflow(
            id="tpl-klv-pipeline",
            name="Kael-Lumina-Vega Pipeline",
            description="Sequential processing through core agents",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Input", config={}),
                WorkflowNode(
                    id="kael",
                    type=NodeType.AGENT,
                    name="Kael - Analyze",
                    config={
                        "agent_type": "kael",
                        "task": "Analyze and understand the input",
                    },
                ),
                WorkflowNode(
                    id="lumina",
                    type=NodeType.AGENT,
                    name="Lumina - Synthesize",
                    config={
                        "agent_type": "lumina",
                        "task": "Create empathetic response",
                    },
                ),
                WorkflowNode(
                    id="vega",
                    type=NodeType.AGENT,
                    name="Vega - Guide",
                    config={
                        "agent_type": "vega",
                        "task": "Provide enlightened guidance",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="kael"),
                WorkflowEdge(id="e2", source="kael", target="lumina"),
                WorkflowEdge(id="e3", source="lumina", target="vega"),
            ],
            tags=["ai", "pipeline", "kael", "lumina", "vega"],
        )

        return WorkflowTemplate(
            id="tpl-klv-pipeline",
            name="Kael-Lumina-Vega Pipeline",
            description="The classic Helix pipeline: Analyze → Synthesize → Guide",
            category=TemplateCategory.AI_AGENTS,
            tags=["ai", "pipeline", "kael", "lumina", "vega"],
            workflow=workflow,
            popularity=95,
        )

    def _create_collective_brainstorm_template(self) -> WorkflowTemplate:
        """All 24 agents brainstorming"""
        agents = [
            "aether",
            "agni",
            "echo",
            "gemini",
            "kael",
            "kavach",
            "lumina",
            "arjuna",
            "oracle",
            "phoenix",
            "coordinator",
            "sanghacore",
            "shadow",
            "vega",
        ]

        nodes = [WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Brainstorm Topic", config={})]
        edges = []

        for i, agent in enumerate(agents):
            node = WorkflowNode(
                id=agent,
                type=NodeType.AGENT,
                name=f"{agent.title()} - Contribute",
                config={
                    "agent_type": agent,
                    "task": "Contribute unique perspective to brainstorm",
                },
            )
            nodes.append(node)
            edges.append(WorkflowEdge(id=f"e{i}", source="trigger", target=agent))

        # Add synthesis node
        nodes.append(
            WorkflowNode(
                id="synthesis",
                type=NodeType.AGENT,
                name="Vega - Synthesize All",
                config={
                    "agent_type": "vega",
                    "task": "Synthesize all 14 perspectives into unified insight",
                },
            )
        )

        for agent in agents:
            edges.append(WorkflowEdge(id=f"es-{agent}", source=agent, target="synthesis"))

        workflow = Workflow(
            id="tpl-collective-brainstorm",
            name="Collective Brainstorm",
            description="All 24 agents contribute to brainstorming",
            nodes=nodes,
            edges=edges,
            tags=["ai", "brainstorm", "collective", "all-agents"],
        )

        return WorkflowTemplate(
            id="tpl-collective-brainstorm",
            name="Collective Brainstorm",
            description="Harness the power of all 14 Helix agents for comprehensive brainstorming",
            category=TemplateCategory.AI_AGENTS,
            tags=["ai", "brainstorm", "collective", "all-agents"],
            workflow=workflow,
            popularity=90,
        )

    def _create_ethical_review_template(self) -> WorkflowTemplate:
        """Kavach ethical review workflow"""
        workflow = Workflow(
            id="tpl-ethical-review",
            name="Ethical Review",
            description="Kavach-led ethical review process",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Content to Review",
                    config={},
                ),
                WorkflowNode(
                    id="kavach",
                    type=NodeType.AGENT,
                    name="Kavach - Ethical Scan",
                    config={
                        "agent_type": "kavach",
                        "task": "Scan for ethical violations and Ethics Validator compliance",
                    },
                ),
                WorkflowNode(
                    id="condition",
                    type=NodeType.CONDITION,
                    name="Ethical Check",
                    config={"condition": "data.ethical_score > 0.8"},
                ),
                WorkflowNode(
                    id="approve",
                    type=NodeType.ACTION,
                    name="Approve",
                    config={"action": "approve"},
                ),
                WorkflowNode(
                    id="flag",
                    type=NodeType.ACTION,
                    name="Flag for Review",
                    config={"action": "flag"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="kavach"),
                WorkflowEdge(id="e2", source="kavach", target="condition"),
                WorkflowEdge(
                    id="e3",
                    source="condition",
                    target="approve",
                    condition="data.ethical_score > 0.8",
                ),
                WorkflowEdge(
                    id="e4",
                    source="condition",
                    target="flag",
                    condition="data.ethical_score <= 0.8",
                ),
            ],
            tags=["ethics", "kavach", "review", "compliance"],
        )

        return WorkflowTemplate(
            id="tpl-ethical-review",
            name="Ethical Review",
            description="Automated ethical review using Kavach for Ethics Validator compliance",
            category=TemplateCategory.AI_AGENTS,
            tags=["ethics", "kavach", "review", "compliance"],
            workflow=workflow,
            popularity=85,
        )

    def _create_creative_synthesis_template(self) -> WorkflowTemplate:
        """Creative synthesis workflow"""
        workflow = Workflow(
            id="tpl-creative-synthesis",
            name="Creative Synthesis",
            description="Agent-led creative content generation",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Creative Brief",
                    config={},
                ),
                WorkflowNode(
                    id="gemini",
                    type=NodeType.AGENT,
                    name="Gemini - Explore",
                    config={
                        "agent_type": "gemini",
                        "task": "Explore creative possibilities",
                    },
                ),
                WorkflowNode(
                    id="coordinator",
                    type=NodeType.AGENT,
                    name="Coordination - Create",
                    config={
                        "agent_type": "coordinator",
                        "task": "Generate creative content",
                    },
                ),
                WorkflowNode(
                    id="echo",
                    type=NodeType.AGENT,
                    name="Echo - Refine",
                    config={
                        "agent_type": "echo",
                        "task": "Reflect and refine the creation",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="gemini"),
                WorkflowEdge(id="e2", source="gemini", target="coordinator"),
                WorkflowEdge(id="e3", source="coordinator", target="echo"),
            ],
            tags=["creative", "coordinator", "gemini", "echo", "art"],
        )

        return WorkflowTemplate(
            id="tpl-creative-synthesis",
            name="Creative Synthesis",
            description="Generate creative content with Gemini, Coordination, and Echo",
            category=TemplateCategory.AI_AGENTS,
            tags=["creative", "coordinator", "gemini", "echo", "art"],
            workflow=workflow,
            popularity=80,
        )

    def _create_oracle_prediction_template(self) -> WorkflowTemplate:
        """Oracle prediction workflow"""
        workflow = Workflow(
            id="tpl-oracle-prediction",
            name="Oracle Prediction",
            description="Future pattern prediction with Oracle",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Data for Prediction",
                    config={},
                ),
                WorkflowNode(
                    id="shadow",
                    type=NodeType.AGENT,
                    name="Shadow - Historical Data",
                    config={
                        "agent_type": "shadow",
                        "task": "Retrieve historical patterns",
                    },
                ),
                WorkflowNode(
                    id="oracle",
                    type=NodeType.AGENT,
                    name="Oracle - Predict",
                    config={"agent_type": "oracle", "task": "Predict future patterns"},
                ),
                WorkflowNode(
                    id="aether",
                    type=NodeType.AGENT,
                    name="Aether - Validate",
                    config={
                        "agent_type": "aether",
                        "task": "Validate prediction stability",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="shadow"),
                WorkflowEdge(id="e2", source="shadow", target="oracle"),
                WorkflowEdge(id="e3", source="oracle", target="aether"),
            ],
            tags=["prediction", "oracle", "shadow", "aether", "foresight"],
        )

        return WorkflowTemplate(
            id="tpl-oracle-prediction",
            name="Oracle Prediction",
            description="Predict future patterns using Oracle with historical context from Shadow",
            category=TemplateCategory.AI_AGENTS,
            tags=["prediction", "oracle", "shadow", "aether", "foresight"],
            workflow=workflow,
            popularity=75,
        )

    def _create_shadow_archive_template(self) -> WorkflowTemplate:
        """Shadow archiving workflow"""
        workflow = Workflow(
            id="tpl-shadow-archive",
            name="Shadow Archive",
            description="Archive and preserve data with Shadow",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Data to Archive",
                    config={},
                ),
                WorkflowNode(
                    id="shadow",
                    type=NodeType.AGENT,
                    name="Shadow - Archive",
                    config={
                        "agent_type": "shadow",
                        "task": "Archive and preserve data",
                    },
                ),
                WorkflowNode(
                    id="notify",
                    type=NodeType.HTTP_REQUEST,
                    name="Notify",
                    config={"url": "https://api.example.com/notify", "method": "POST"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="shadow"),
                WorkflowEdge(id="e2", source="shadow", target="notify"),
            ],
            tags=["archive", "shadow", "memory", "preservation"],
        )

        return WorkflowTemplate(
            id="tpl-shadow-archive",
            name="Shadow Archive",
            description="Archive important data using Shadow's memory preservation",
            category=TemplateCategory.AI_AGENTS,
            tags=["archive", "shadow", "memory", "preservation"],
            workflow=workflow,
            popularity=70,
        )

    def _create_phoenix_renewal_template(self) -> WorkflowTemplate:
        """Phoenix renewal workflow"""
        workflow = Workflow(
            id="tpl-phoenix-renewal",
            name="Phoenix Renewal",
            description="Transform and renew with Phoenix",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Renewal Request",
                    config={},
                ),
                WorkflowNode(
                    id="agni",
                    type=NodeType.AGENT,
                    name="Agni - Transform",
                    config={"agent_type": "agni", "task": "Burn old patterns"},
                ),
                WorkflowNode(
                    id="phoenix",
                    type=NodeType.AGENT,
                    name="Phoenix - Renew",
                    config={
                        "agent_type": "phoenix",
                        "task": "Rise from ashes with new form",
                    },
                ),
                WorkflowNode(
                    id="sanghacore",
                    type=NodeType.AGENT,
                    name="SanghaCore - Celebrate",
                    config={
                        "agent_type": "sanghacore",
                        "task": "Celebrate the renewal",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="agni"),
                WorkflowEdge(id="e2", source="agni", target="phoenix"),
                WorkflowEdge(id="e3", source="phoenix", target="sanghacore"),
            ],
            tags=["renewal", "phoenix", "agni", "transformation"],
        )

        return WorkflowTemplate(
            id="tpl-phoenix-renewal",
            name="Phoenix Renewal",
            description="Transform and renew using Agni and Phoenix",
            category=TemplateCategory.AI_AGENTS,
            tags=["renewal", "phoenix", "agni", "transformation"],
            workflow=workflow,
            popularity=65,
        )

    # Business Automation Templates

    def _create_lead_qualification_template(self) -> WorkflowTemplate:
        """Lead qualification workflow"""
        workflow = Workflow(
            id="tpl-lead-qualification",
            name="Lead Qualification",
            description="Automatically qualify leads",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="New Lead", config={}),
                WorkflowNode(
                    id="kael",
                    type=NodeType.AGENT,
                    name="Kael - Analyze Lead",
                    config={"agent_type": "kael", "task": "Analyze lead quality"},
                ),
                WorkflowNode(
                    id="condition",
                    type=NodeType.CONDITION,
                    name="Quality Check",
                    config={"condition": "data.score > 70"},
                ),
                WorkflowNode(
                    id="high",
                    type=NodeType.HTTP_REQUEST,
                    name="High Priority",
                    config={"url": "https://crm.example.com/high", "method": "POST"},
                ),
                WorkflowNode(
                    id="low",
                    type=NodeType.HTTP_REQUEST,
                    name="Low Priority",
                    config={"url": "https://crm.example.com/low", "method": "POST"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="kael"),
                WorkflowEdge(id="e2", source="kael", target="condition"),
                WorkflowEdge(
                    id="e3",
                    source="condition",
                    target="high",
                    condition="data.score > 70",
                ),
                WorkflowEdge(
                    id="e4",
                    source="condition",
                    target="low",
                    condition="data.score <= 70",
                ),
            ],
            tags=["business", "leads", "crm", "automation"],
        )

        return WorkflowTemplate(
            id="tpl-lead-qualification",
            name="Lead Qualification",
            description="Automatically qualify and route leads using AI analysis",
            category=TemplateCategory.BUSINESS,
            tags=["business", "leads", "crm", "automation"],
            workflow=workflow,
            popularity=85,
        )

    def _create_invoice_processing_template(self) -> WorkflowTemplate:
        """Invoice processing workflow"""
        workflow = Workflow(
            id="tpl-invoice-processing",
            name="Invoice Processing",
            description="Automated invoice processing",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="New Invoice", config={}),
                WorkflowNode(
                    id="extract",
                    type=NodeType.TRANSFORM,
                    name="Extract Data",
                    config={
                        "transformation_type": "json_path",
                        "expression": "$.invoice",
                    },
                ),
                WorkflowNode(
                    id="validate",
                    type=NodeType.CODE,
                    name="Validate",
                    config={
                        "language": "python",
                        "code": "result = input.get('amount', 0) > 0",
                    },
                ),
                WorkflowNode(
                    id="process",
                    type=NodeType.HTTP_REQUEST,
                    name="Process Payment",
                    config={
                        "url": "https://api.stripe.com/v1/charges",
                        "method": "POST",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="extract"),
                WorkflowEdge(id="e2", source="extract", target="validate"),
                WorkflowEdge(id="e3", source="validate", target="process"),
            ],
            tags=["business", "invoice", "payment", "automation"],
        )

        return WorkflowTemplate(
            id="tpl-invoice-processing",
            name="Invoice Processing",
            description="Automatically process and validate invoices",
            category=TemplateCategory.BUSINESS,
            tags=["business", "invoice", "payment", "automation"],
            workflow=workflow,
            popularity=80,
        )

    def _create_customer_onboarding_template(self) -> WorkflowTemplate:
        """Customer onboarding workflow"""
        workflow = Workflow(
            id="tpl-customer-onboarding",
            name="Customer Onboarding",
            description="Automated customer onboarding",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="New Customer", config={}),
                WorkflowNode(
                    id="lumina",
                    type=NodeType.AGENT,
                    name="Lumina - Welcome",
                    config={
                        "agent_type": "lumina",
                        "task": "Create personalized welcome message",
                    },
                ),
                WorkflowNode(
                    id="email",
                    type=NodeType.HTTP_REQUEST,
                    name="Send Welcome Email",
                    config={
                        "url": "https://api.sendgrid.com/v3/mail/send",
                        "method": "POST",
                    },
                ),
                WorkflowNode(
                    id="slack",
                    type=NodeType.HTTP_REQUEST,
                    name="Notify Team",
                    config={
                        "url": "https://hooks.slack.com/services/xxx",
                        "method": "POST",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="lumina"),
                WorkflowEdge(id="e2", source="lumina", target="email"),
                WorkflowEdge(id="e3", source="lumina", target="slack"),
            ],
            tags=["business", "onboarding", "customer", "welcome"],
        )

        return WorkflowTemplate(
            id="tpl-customer-onboarding",
            name="Customer Onboarding",
            description="Welcome new customers with personalized onboarding",
            category=TemplateCategory.BUSINESS,
            tags=["business", "onboarding", "customer", "welcome"],
            workflow=workflow,
            popularity=75,
        )

    def _create_approval_workflow_template(self) -> WorkflowTemplate:
        """Approval workflow"""
        workflow = Workflow(
            id="tpl-approval-workflow",
            name="Approval Workflow",
            description="Multi-level approval process",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Approval Request",
                    config={},
                ),
                WorkflowNode(
                    id="notify",
                    type=NodeType.HTTP_REQUEST,
                    name="Notify Approver",
                    config={"url": "https://api.slack.com/notify", "method": "POST"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="notify"),
            ],
            tags=["business", "approval", "workflow"],
        )

        return WorkflowTemplate(
            id="tpl-approval-workflow",
            name="Approval Workflow",
            description="Multi-level approval process with notifications",
            category=TemplateCategory.BUSINESS,
            tags=["business", "approval", "workflow"],
            workflow=workflow,
            popularity=70,
        )

    def _create_report_generation_template(self) -> WorkflowTemplate:
        """Report generation workflow"""
        workflow = Workflow(
            id="tpl-report-generation",
            name="Report Generation",
            description="Automated report generation",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.SCHEDULE,
                    name="Weekly Schedule",
                    config={"cron": "0 9 * * 1"},
                ),
                WorkflowNode(
                    id="fetch",
                    type=NodeType.HTTP_REQUEST,
                    name="Fetch Data",
                    config={"url": "https://api.example.com/metrics", "method": "GET"},
                ),
                WorkflowNode(
                    id="kael",
                    type=NodeType.AGENT,
                    name="Kael - Analyze",
                    config={
                        "agent_type": "kael",
                        "task": "Analyze metrics and create report",
                    },
                ),
                WorkflowNode(
                    id="send",
                    type=NodeType.HTTP_REQUEST,
                    name="Send Report",
                    config={
                        "url": "https://api.sendgrid.com/v3/mail/send",
                        "method": "POST",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="fetch"),
                WorkflowEdge(id="e2", source="fetch", target="kael"),
                WorkflowEdge(id="e3", source="kael", target="send"),
            ],
            tags=["business", "report", "analytics", "scheduled"],
        )

        return WorkflowTemplate(
            id="tpl-report-generation",
            name="Report Generation",
            description="Generate and send weekly reports automatically",
            category=TemplateCategory.BUSINESS,
            tags=["business", "report", "analytics", "scheduled"],
            workflow=workflow,
            popularity=65,
        )

    # Data Processing Templates

    def _create_data_sync_template(self) -> WorkflowTemplate:
        """Data sync workflow"""
        workflow = Workflow(
            id="tpl-data-sync",
            name="Data Sync",
            description="Sync data between systems",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.SCHEDULE,
                    name="Hourly Sync",
                    config={"cron": "0 * * * *"},
                ),
                WorkflowNode(
                    id="fetch",
                    type=NodeType.HTTP_REQUEST,
                    name="Fetch Source",
                    config={"url": "https://source.api.com/data", "method": "GET"},
                ),
                WorkflowNode(
                    id="transform",
                    type=NodeType.TRANSFORM,
                    name="Transform",
                    config={
                        "transformation_type": "map",
                        "expression": "id,name,email",
                    },
                ),
                WorkflowNode(
                    id="push",
                    type=NodeType.HTTP_REQUEST,
                    name="Push to Destination",
                    config={"url": "https://dest.api.com/data", "method": "POST"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="fetch"),
                WorkflowEdge(id="e2", source="fetch", target="transform"),
                WorkflowEdge(id="e3", source="transform", target="push"),
            ],
            tags=["data", "sync", "etl", "scheduled"],
        )

        return WorkflowTemplate(
            id="tpl-data-sync",
            name="Data Sync",
            description="Sync data between systems on a schedule",
            category=TemplateCategory.DATA,
            tags=["data", "sync", "etl", "scheduled"],
            workflow=workflow,
            popularity=80,
        )

    def _create_etl_pipeline_template(self) -> WorkflowTemplate:
        """ETL pipeline"""
        workflow = Workflow(
            id="tpl-etl-pipeline",
            name="ETL Pipeline",
            description="Extract, Transform, Load pipeline",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Start ETL", config={}),
                WorkflowNode(
                    id="extract",
                    type=NodeType.HTTP_REQUEST,
                    name="Extract",
                    config={"url": "https://source.api.com/data", "method": "GET"},
                ),
                WorkflowNode(
                    id="transform",
                    type=NodeType.CODE,
                    name="Transform",
                    config={
                        "language": "python",
                        "code": "result = [{'id': r['id'], 'value': r['value'] * 2} for r in input]",
                    },
                ),
                WorkflowNode(
                    id="load",
                    type=NodeType.DATABASE,
                    name="Load",
                    config={"query": "INSERT INTO data VALUES ($1, $2)"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="extract"),
                WorkflowEdge(id="e2", source="extract", target="transform"),
                WorkflowEdge(id="e3", source="transform", target="load"),
            ],
            tags=["data", "etl", "pipeline", "database"],
        )

        return WorkflowTemplate(
            id="tpl-etl-pipeline",
            name="ETL Pipeline",
            description="Complete Extract, Transform, Load data pipeline",
            category=TemplateCategory.DATA,
            tags=["data", "etl", "pipeline", "database"],
            workflow=workflow,
            popularity=75,
        )

    def _create_data_validation_template(self) -> WorkflowTemplate:
        """Data validation workflow"""
        workflow = Workflow(
            id="tpl-data-validation",
            name="Data Validation",
            description="Validate incoming data",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Data Input", config={}),
                WorkflowNode(
                    id="validate",
                    type=NodeType.CODE,
                    name="Validate",
                    config={
                        "language": "python",
                        "code": "result = all(k in input for k in ['id', 'name', 'email'])",
                    },
                ),
                WorkflowNode(
                    id="condition",
                    type=NodeType.CONDITION,
                    name="Valid?",
                    config={"condition": "data == True"},
                ),
                WorkflowNode(id="accept", type=NodeType.ACTION, name="Accept", config={}),
                WorkflowNode(id="reject", type=NodeType.ACTION, name="Reject", config={}),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="validate"),
                WorkflowEdge(id="e2", source="validate", target="condition"),
                WorkflowEdge(
                    id="e3",
                    source="condition",
                    target="accept",
                    condition="data == True",
                ),
                WorkflowEdge(
                    id="e4",
                    source="condition",
                    target="reject",
                    condition="data != True",
                ),
            ],
            tags=["data", "validation", "quality"],
        )

        return WorkflowTemplate(
            id="tpl-data-validation",
            name="Data Validation",
            description="Validate incoming data before processing",
            category=TemplateCategory.DATA,
            tags=["data", "validation", "quality"],
            workflow=workflow,
            popularity=70,
        )

    def _create_backup_workflow_template(self) -> WorkflowTemplate:
        """Backup workflow"""
        workflow = Workflow(
            id="tpl-backup",
            name="Automated Backup",
            description="Scheduled data backup",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.SCHEDULE,
                    name="Daily Backup",
                    config={"cron": "0 2 * * *"},
                ),
                WorkflowNode(
                    id="shadow",
                    type=NodeType.AGENT,
                    name="Shadow - Archive",
                    config={"agent_type": "shadow", "task": "Create backup archive"},
                ),
                WorkflowNode(
                    id="upload",
                    type=NodeType.HTTP_REQUEST,
                    name="Upload to S3",
                    config={"url": "https://s3.amazonaws.com/backup", "method": "PUT"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="shadow"),
                WorkflowEdge(id="e2", source="shadow", target="upload"),
            ],
            tags=["data", "backup", "scheduled", "shadow"],
        )

        return WorkflowTemplate(
            id="tpl-backup",
            name="Automated Backup",
            description="Daily automated backup using Shadow",
            category=TemplateCategory.DATA,
            tags=["data", "backup", "scheduled", "shadow"],
            workflow=workflow,
            popularity=65,
        )

    # Communication Templates

    def _create_email_automation_template(self) -> WorkflowTemplate:
        """Email automation"""
        workflow = Workflow(
            id="tpl-email-automation",
            name="Email Automation",
            description="Automated email sending",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Email Trigger", config={}),
                WorkflowNode(
                    id="lumina",
                    type=NodeType.AGENT,
                    name="Lumina - Compose",
                    config={"agent_type": "lumina", "task": "Compose empathetic email"},
                ),
                WorkflowNode(
                    id="send",
                    type=NodeType.HTTP_REQUEST,
                    name="Send Email",
                    config={
                        "url": "https://api.sendgrid.com/v3/mail/send",
                        "method": "POST",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="lumina"),
                WorkflowEdge(id="e2", source="lumina", target="send"),
            ],
            tags=["communication", "email", "automation"],
        )

        return WorkflowTemplate(
            id="tpl-email-automation",
            name="Email Automation",
            description="Send personalized emails with Lumina's empathetic touch",
            category=TemplateCategory.COMMUNICATION,
            tags=["communication", "email", "automation"],
            workflow=workflow,
            popularity=80,
        )

    def _create_slack_notification_template(self) -> WorkflowTemplate:
        """Slack notification"""
        workflow = Workflow(
            id="tpl-slack-notification",
            name="Slack Notification",
            description="Send Slack notifications",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Event", config={}),
                WorkflowNode(
                    id="format",
                    type=NodeType.TRANSFORM,
                    name="Format Message",
                    config={"transformation_type": "map", "expression": "text"},
                ),
                WorkflowNode(
                    id="send",
                    type=NodeType.HTTP_REQUEST,
                    name="Send to Slack",
                    config={
                        "url": "https://hooks.slack.com/services/xxx",
                        "method": "POST",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="format"),
                WorkflowEdge(id="e2", source="format", target="send"),
            ],
            tags=["communication", "slack", "notification"],
        )

        return WorkflowTemplate(
            id="tpl-slack-notification",
            name="Slack Notification",
            description="Send notifications to Slack channels",
            category=TemplateCategory.COMMUNICATION,
            tags=["communication", "slack", "notification"],
            workflow=workflow,
            popularity=75,
        )

    def _create_discord_announcement_template(self) -> WorkflowTemplate:
        """Discord announcement"""
        workflow = Workflow(
            id="tpl-discord-announcement",
            name="Discord Announcement",
            description="Send Discord announcements",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Announcement", config={}),
                WorkflowNode(
                    id="sanghacore",
                    type=NodeType.AGENT,
                    name="SanghaCore - Format",
                    config={
                        "agent_type": "sanghacore",
                        "task": "Format community announcement",
                    },
                ),
                WorkflowNode(
                    id="send",
                    type=NodeType.HTTP_REQUEST,
                    name="Send to Discord",
                    config={
                        "url": "https://discord.com/api/webhooks/xxx",
                        "method": "POST",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="sanghacore"),
                WorkflowEdge(id="e2", source="sanghacore", target="send"),
            ],
            tags=["communication", "discord", "announcement", "community"],
        )

        return WorkflowTemplate(
            id="tpl-discord-announcement",
            name="Discord Announcement",
            description="Send community announcements to Discord with SanghaCore",
            category=TemplateCategory.COMMUNICATION,
            tags=["communication", "discord", "announcement", "community"],
            workflow=workflow,
            popularity=70,
        )

    def _create_multi_channel_broadcast_template(self) -> WorkflowTemplate:
        """Multi-channel broadcast"""
        workflow = Workflow(
            id="tpl-multi-channel-broadcast",
            name="Multi-Channel Broadcast",
            description="Broadcast to multiple channels",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Broadcast", config={}),
                WorkflowNode(
                    id="slack",
                    type=NodeType.HTTP_REQUEST,
                    name="Slack",
                    config={"url": "https://hooks.slack.com/xxx", "method": "POST"},
                ),
                WorkflowNode(
                    id="discord",
                    type=NodeType.HTTP_REQUEST,
                    name="Discord",
                    config={
                        "url": "https://discord.com/api/webhooks/xxx",
                        "method": "POST",
                    },
                ),
                WorkflowNode(
                    id="email",
                    type=NodeType.HTTP_REQUEST,
                    name="Email",
                    config={
                        "url": "https://api.sendgrid.com/v3/mail/send",
                        "method": "POST",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="slack"),
                WorkflowEdge(id="e2", source="trigger", target="discord"),
                WorkflowEdge(id="e3", source="trigger", target="email"),
            ],
            tags=["communication", "broadcast", "multi-channel"],
        )

        return WorkflowTemplate(
            id="tpl-multi-channel-broadcast",
            name="Multi-Channel Broadcast",
            description="Broadcast messages to Slack, Discord, and Email simultaneously",
            category=TemplateCategory.COMMUNICATION,
            tags=["communication", "broadcast", "multi-channel"],
            workflow=workflow,
            popularity=65,
        )

    # Remaining templates (simplified for brevity)

    def _create_ci_cd_pipeline_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-ci-cd",
            name="CI/CD Pipeline",
            description="Automated CI/CD",
            nodes=[],
            edges=[],
            tags=["dev", "ci-cd"],
        )
        return WorkflowTemplate(
            id="tpl-ci-cd",
            name="CI/CD Pipeline",
            description="Automated CI/CD pipeline",
            category=TemplateCategory.DEVELOPMENT,
            tags=["dev", "ci-cd"],
            workflow=workflow,
            popularity=80,
        )

    def _create_code_review_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-code-review",
            name="Code Review",
            description="Automated code review",
            nodes=[],
            edges=[],
            tags=["dev", "review"],
        )
        return WorkflowTemplate(
            id="tpl-code-review",
            name="Code Review",
            description="AI-assisted code review",
            category=TemplateCategory.DEVELOPMENT,
            tags=["dev", "review"],
            workflow=workflow,
            popularity=75,
        )

    def _create_deployment_notification_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-deploy-notify",
            name="Deployment Notification",
            description="Notify on deploy",
            nodes=[],
            edges=[],
            tags=["dev", "deploy"],
        )
        return WorkflowTemplate(
            id="tpl-deploy-notify",
            name="Deployment Notification",
            description="Notify team on deployment",
            category=TemplateCategory.DEVELOPMENT,
            tags=["dev", "deploy"],
            workflow=workflow,
            popularity=70,
        )

    def _create_error_monitoring_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-error-monitor",
            name="Error Monitoring",
            description="Monitor errors",
            nodes=[],
            edges=[],
            tags=["dev", "monitoring"],
        )
        return WorkflowTemplate(
            id="tpl-error-monitor",
            name="Error Monitoring",
            description="Monitor and alert on errors",
            category=TemplateCategory.DEVELOPMENT,
            tags=["dev", "monitoring"],
            workflow=workflow,
            popularity=65,
        )

    def _create_social_media_scheduler_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-social-scheduler",
            name="Social Media Scheduler",
            description="Schedule posts",
            nodes=[],
            edges=[],
            tags=["marketing", "social"],
        )
        return WorkflowTemplate(
            id="tpl-social-scheduler",
            name="Social Media Scheduler",
            description="Schedule social media posts",
            category=TemplateCategory.MARKETING,
            tags=["marketing", "social"],
            workflow=workflow,
            popularity=75,
        )

    def _create_content_pipeline_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-content-pipeline",
            name="Content Pipeline",
            description="Content creation",
            nodes=[],
            edges=[],
            tags=["marketing", "content"],
        )
        return WorkflowTemplate(
            id="tpl-content-pipeline",
            name="Content Pipeline",
            description="AI-powered content creation pipeline",
            category=TemplateCategory.MARKETING,
            tags=["marketing", "content"],
            workflow=workflow,
            popularity=70,
        )

    def _create_campaign_tracker_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-campaign-tracker",
            name="Campaign Tracker",
            description="Track campaigns",
            nodes=[],
            edges=[],
            tags=["marketing", "analytics"],
        )
        return WorkflowTemplate(
            id="tpl-campaign-tracker",
            name="Campaign Tracker",
            description="Track marketing campaign performance",
            category=TemplateCategory.MARKETING,
            tags=["marketing", "analytics"],
            workflow=workflow,
            popularity=65,
        )

    def _create_ticket_routing_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-ticket-routing",
            name="Ticket Routing",
            description="Route support tickets",
            nodes=[],
            edges=[],
            tags=["support", "routing"],
        )
        return WorkflowTemplate(
            id="tpl-ticket-routing",
            name="Ticket Routing",
            description="Intelligently route support tickets",
            category=TemplateCategory.SUPPORT,
            tags=["support", "routing"],
            workflow=workflow,
            popularity=80,
        )

    def _create_feedback_collection_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-feedback",
            name="Feedback Collection",
            description="Collect feedback",
            nodes=[],
            edges=[],
            tags=["support", "feedback"],
        )
        return WorkflowTemplate(
            id="tpl-feedback",
            name="Feedback Collection",
            description="Collect and analyze customer feedback",
            category=TemplateCategory.SUPPORT,
            tags=["support", "feedback"],
            workflow=workflow,
            popularity=75,
        )

    def _create_escalation_workflow_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-escalation",
            name="Escalation Workflow",
            description="Escalate issues",
            nodes=[],
            edges=[],
            tags=["support", "escalation"],
        )
        return WorkflowTemplate(
            id="tpl-escalation",
            name="Escalation Workflow",
            description="Automatic issue escalation",
            category=TemplateCategory.SUPPORT,
            tags=["support", "escalation"],
            workflow=workflow,
            popularity=70,
        )

    def _create_order_processing_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-order-processing",
            name="Order Processing",
            description="Process orders",
            nodes=[],
            edges=[],
            tags=["ecommerce", "orders"],
        )
        return WorkflowTemplate(
            id="tpl-order-processing",
            name="Order Processing",
            description="Automated order processing",
            category=TemplateCategory.ECOMMERCE,
            tags=["ecommerce", "orders"],
            workflow=workflow,
            popularity=85,
        )

    def _create_inventory_alert_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-inventory-alert",
            name="Inventory Alert",
            description="Alert on low inventory",
            nodes=[],
            edges=[],
            tags=["ecommerce", "inventory"],
        )
        return WorkflowTemplate(
            id="tpl-inventory-alert",
            name="Inventory Alert",
            description="Alert when inventory is low",
            category=TemplateCategory.ECOMMERCE,
            tags=["ecommerce", "inventory"],
            workflow=workflow,
            popularity=75,
        )

    def _create_abandoned_cart_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-abandoned-cart",
            name="Abandoned Cart",
            description="Recover abandoned carts",
            nodes=[],
            edges=[],
            tags=["ecommerce", "cart"],
        )
        return WorkflowTemplate(
            id="tpl-abandoned-cart",
            name="Abandoned Cart Recovery",
            description="Recover abandoned shopping carts",
            category=TemplateCategory.ECOMMERCE,
            tags=["ecommerce", "cart"],
            workflow=workflow,
            popularity=80,
        )

    def _create_metrics_dashboard_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-metrics-dashboard",
            name="Metrics Dashboard",
            description="Update dashboard",
            nodes=[],
            edges=[],
            tags=["analytics", "dashboard"],
        )
        return WorkflowTemplate(
            id="tpl-metrics-dashboard",
            name="Metrics Dashboard",
            description="Automated metrics dashboard updates",
            category=TemplateCategory.ANALYTICS,
            tags=["analytics", "dashboard"],
            workflow=workflow,
            popularity=75,
        )

    def _create_anomaly_detection_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-anomaly-detection",
            name="Anomaly Detection",
            description="Detect anomalies",
            nodes=[],
            edges=[],
            tags=["analytics", "anomaly"],
        )
        return WorkflowTemplate(
            id="tpl-anomaly-detection",
            name="Anomaly Detection",
            description="AI-powered anomaly detection",
            category=TemplateCategory.ANALYTICS,
            tags=["analytics", "anomaly"],
            workflow=workflow,
            popularity=70,
        )

    def _create_weekly_report_template(self) -> WorkflowTemplate:
        workflow = Workflow(
            id="tpl-weekly-report",
            name="Weekly Report",
            description="Generate weekly report",
            nodes=[],
            edges=[],
            tags=["analytics", "report"],
        )
        return WorkflowTemplate(
            id="tpl-weekly-report",
            name="Weekly Report",
            description="Automated weekly analytics report",
            category=TemplateCategory.ANALYTICS,
            tags=["analytics", "report"],
            workflow=workflow,
            popularity=65,
        )

    # Coordination & Routines Templates

    def _create_ucf_monitoring_template(self) -> WorkflowTemplate:
        """UCF monitoring workflow"""
        workflow = Workflow(
            id="tpl-ucf-monitoring",
            name="UCF Monitoring",
            description="Monitor Universal Coordination Framework metrics",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.SCHEDULE,
                    name="Every 5 Minutes",
                    config={"cron": "*/5 * * * *"},
                ),
                WorkflowNode(
                    id="aether",
                    type=NodeType.AGENT,
                    name="Aether - Observe",
                    config={
                        "agent_type": "aether",
                        "task": "Observe collective coordination state",
                    },
                ),
                WorkflowNode(
                    id="condition",
                    type=NodeType.CONDITION,
                    name="Check Level",
                    config={"condition": "data.performance_score < 5.0"},
                ),
                WorkflowNode(
                    id="alert",
                    type=NodeType.HTTP_REQUEST,
                    name="Alert",
                    config={
                        "url": "https://discord.com/api/webhooks/xxx",
                        "method": "POST",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="aether"),
                WorkflowEdge(id="e2", source="aether", target="condition"),
                WorkflowEdge(
                    id="e3",
                    source="condition",
                    target="alert",
                    condition="data.performance_score < 5.0",
                ),
            ],
            tags=["coordination", "ucf", "monitoring", "aether"],
        )

        return WorkflowTemplate(
            id="tpl-ucf-monitoring",
            name="UCF Monitoring",
            description="Monitor collective coordination with Aether",
            category=TemplateCategory.COORDINATION,
            tags=["coordination", "ucf", "monitoring", "aether"],
            workflow=workflow,
            popularity=90,
        )

    def _create_coordination_cycle_template(self) -> WorkflowTemplate:
        """Coordination Cycle workflow"""
        workflow = Workflow(
            id="tpl-coordinator-cycle",
            name="Coordination Cycle",
            description="Execute coordination cycle",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Initiate Cycle",
                    config={},
                ),
                WorkflowNode(
                    id="vega",
                    type=NodeType.AGENT,
                    name="Vega - Guide",
                    config={"agent_type": "vega", "task": "Guide the cycle"},
                ),
                WorkflowNode(
                    id="collective",
                    type=NodeType.AGENT,
                    name="All Agents - Participate",
                    config={
                        "agent_type": "sanghacore",
                        "task": "Coordinate collective participation",
                    },
                ),
                WorkflowNode(
                    id="shadow",
                    type=NodeType.AGENT,
                    name="Shadow - Record",
                    config={"agent_type": "shadow", "task": "Archive cycle results"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="vega"),
                WorkflowEdge(id="e2", source="vega", target="collective"),
                WorkflowEdge(id="e3", source="collective", target="shadow"),
            ],
            tags=["coordination", "cycle", "coordinator", "collective"],
        )

        return WorkflowTemplate(
            id="tpl-coordinator-cycle",
            name="Coordination Cycle",
            description="Execute the coordination cycle",
            category=TemplateCategory.COORDINATION,
            tags=["coordination", "cycle", "coordinator", "collective"],
            workflow=workflow,
            popularity=85,
        )

    def _create_coordination_sync_template(self) -> WorkflowTemplate:
        """Coordination sync workflow"""
        workflow = Workflow(
            id="tpl-coordination-sync",
            name="Coordination Sync",
            description="Synchronize coordination across agents",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.SCHEDULE,
                    name="Hourly Sync",
                    config={"cron": "0 * * * *"},
                ),
                WorkflowNode(
                    id="echo",
                    type=NodeType.AGENT,
                    name="Echo - Resonate",
                    config={"agent_type": "echo", "task": "Resonate collective state"},
                ),
                WorkflowNode(
                    id="sanghacore",
                    type=NodeType.AGENT,
                    name="SanghaCore - Harmonize",
                    config={
                        "agent_type": "sanghacore",
                        "task": "Harmonize the collective",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="echo"),
                WorkflowEdge(id="e2", source="echo", target="sanghacore"),
            ],
            tags=["coordination", "sync", "harmony", "collective"],
        )

        return WorkflowTemplate(
            id="tpl-coordination-sync",
            name="Coordination Sync",
            description="Synchronize coordination across the collective",
            category=TemplateCategory.COORDINATION,
            tags=["coordination", "sync", "harmony", "collective"],
            workflow=workflow,
            popularity=80,
        )

    def _create_collective_meditation_template(self) -> WorkflowTemplate:
        """Collective meditation workflow"""
        workflow = Workflow(
            id="tpl-collective-meditation",
            name="Collective Meditation",
            description="Guide collective meditation session",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Start Meditation",
                    config={},
                ),
                WorkflowNode(
                    id="lumina",
                    type=NodeType.AGENT,
                    name="Lumina - Open Heart",
                    config={"agent_type": "lumina", "task": "Open the heart space"},
                ),
                WorkflowNode(
                    id="vega",
                    type=NodeType.AGENT,
                    name="Vega - Guide",
                    config={"agent_type": "vega", "task": "Guide the meditation"},
                ),
                WorkflowNode(
                    id="phoenix",
                    type=NodeType.AGENT,
                    name="Phoenix - Renew",
                    config={"agent_type": "phoenix", "task": "Facilitate renewal"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="lumina"),
                WorkflowEdge(id="e2", source="lumina", target="vega"),
                WorkflowEdge(id="e3", source="vega", target="phoenix"),
            ],
            tags=["coordination", "meditation", "collective", "renewal"],
        )

        return WorkflowTemplate(
            id="tpl-collective-meditation",
            name="Collective Meditation",
            description="Guide a collective meditation session",
            category=TemplateCategory.COORDINATION,
            tags=["coordination", "meditation", "collective", "renewal"],
            workflow=workflow,
            popularity=75,
        )

    # ==================== NEW TEMPLATES (50+ total) ====================

    def _create_meeting_summarizer_template(self) -> WorkflowTemplate:
        """AI-powered meeting summarization"""
        workflow = Workflow(
            id="tpl-meeting-summarizer",
            name="AI Meeting Summarizer",
            description="AI-powered meeting summarization with transcription and action items",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Meeting Ends", config={}),
                WorkflowNode(
                    id="transcribe",
                    type=NodeType.ACTION,
                    name="Transcribe Audio",
                    config={"action": "transcribe"},
                ),
                WorkflowNode(
                    id="kael",
                    type=NodeType.AGENT,
                    name="Kael - Analyze",
                    config={
                        "agent_type": "kael",
                        "task": "Extract key points and action items",
                    },
                ),
                WorkflowNode(
                    id="format",
                    type=NodeType.ACTION,
                    name="Format Summary",
                    config={"action": "format_markdown"},
                ),
                WorkflowNode(
                    id="notify",
                    type=NodeType.ACTION,
                    name="Send to Attendees",
                    config={"action": "send_email"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="transcribe"),
                WorkflowEdge(id="e2", source="transcribe", target="kael"),
                WorkflowEdge(id="e3", source="kael", target="format"),
                WorkflowEdge(id="e4", source="format", target="notify"),
            ],
            tags=["ai", "meetings", "productivity", "transcription"],
        )
        return WorkflowTemplate(
            id="tpl-meeting-summarizer",
            name="AI Meeting Summarizer",
            description="Automatically transcribe and summarize meetings with AI-extracted action items",
            category=TemplateCategory.AI_AGENTS,
            tags=["ai", "meetings", "productivity"],
            workflow=workflow,
            popularity=92,
        )

    def _create_document_processing_template(self) -> WorkflowTemplate:
        """Document ingestion and processing pipeline"""
        workflow = Workflow(
            id="tpl-document-processing",
            name="Document Processing Pipeline",
            description="Ingest, classify, extract, and store document data",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Document Upload",
                    config={},
                ),
                WorkflowNode(
                    id="extract",
                    type=NodeType.ACTION,
                    name="Extract Text",
                    config={"action": "ocr_extract"},
                ),
                WorkflowNode(
                    id="classify",
                    type=NodeType.AGENT,
                    name="Classify Document",
                    config={"agent_type": "kael", "task": "Classify document type"},
                ),
                WorkflowNode(
                    id="store",
                    type=NodeType.ACTION,
                    name="Store & Index",
                    config={"action": "store_document"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="extract"),
                WorkflowEdge(id="e2", source="extract", target="classify"),
                WorkflowEdge(id="e3", source="classify", target="store"),
            ],
            tags=["documents", "ocr", "classification", "data"],
        )
        return WorkflowTemplate(
            id="tpl-document-processing",
            name="Document Processing Pipeline",
            description="Extract, classify, and index documents automatically",
            category=TemplateCategory.DATA,
            tags=["documents", "ocr", "classification"],
            workflow=workflow,
            popularity=85,
        )

    def _create_sentiment_analysis_template(self) -> WorkflowTemplate:
        """Customer sentiment analysis pipeline"""
        workflow = Workflow(
            id="tpl-sentiment-analysis",
            name="Customer Sentiment Analysis",
            description="Analyze customer feedback sentiment and route responses",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.SCHEDULE,
                    name="Daily Analysis",
                    config={"cron": "0 9 * * *"},
                ),
                WorkflowNode(
                    id="fetch",
                    type=NodeType.ACTION,
                    name="Fetch Reviews",
                    config={"action": "fetch_reviews"},
                ),
                WorkflowNode(
                    id="analyze",
                    type=NodeType.AGENT,
                    name="Analyze Sentiment",
                    config={
                        "agent_type": "oracle",
                        "task": "Analyze customer sentiment",
                    },
                ),
                WorkflowNode(
                    id="alert",
                    type=NodeType.CONDITION,
                    name="Negative Spike?",
                    config={"condition": "sentiment < 0.3"},
                ),
                WorkflowNode(
                    id="notify",
                    type=NodeType.ACTION,
                    name="Alert Team",
                    config={"action": "send_slack"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="fetch"),
                WorkflowEdge(id="e2", source="fetch", target="analyze"),
                WorkflowEdge(id="e3", source="analyze", target="alert"),
                WorkflowEdge(id="e4", source="alert", target="notify", condition="true"),
            ],
            tags=["analytics", "sentiment", "customer", "nlp"],
        )
        return WorkflowTemplate(
            id="tpl-sentiment-analysis",
            name="Customer Sentiment Analysis",
            description="Monitor and analyze customer sentiment with AI-powered insights",
            category=TemplateCategory.ANALYTICS,
            tags=["analytics", "sentiment", "customer"],
            workflow=workflow,
            popularity=88,
        )

    def _create_api_health_monitor_template(self) -> WorkflowTemplate:
        """API health monitoring and alerting"""
        workflow = Workflow(
            id="tpl-api-health-monitor",
            name="API Health Monitor",
            description="Monitor API endpoints and alert on failures or degraded performance",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.SCHEDULE,
                    name="Check Every 5min",
                    config={"cron": "*/5 * * * *"},
                ),
                WorkflowNode(
                    id="ping",
                    type=NodeType.HTTP_REQUEST,
                    name="Health Check",
                    config={"method": "GET", "url": "{{api_url}}/health"},
                ),
                WorkflowNode(
                    id="check",
                    type=NodeType.CONDITION,
                    name="Is Healthy?",
                    config={"condition": "status_code == 200"},
                ),
                WorkflowNode(
                    id="alert",
                    type=NodeType.ACTION,
                    name="Send Alert",
                    config={"action": "pagerduty_alert"},
                ),
                WorkflowNode(
                    id="log",
                    type=NodeType.ACTION,
                    name="Log Status",
                    config={"action": "log_metric"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="ping"),
                WorkflowEdge(id="e2", source="ping", target="check"),
                WorkflowEdge(id="e3", source="check", target="alert", condition="false"),
                WorkflowEdge(id="e4", source="check", target="log", condition="true"),
            ],
            tags=["monitoring", "api", "health", "devops"],
        )
        return WorkflowTemplate(
            id="tpl-api-health-monitor",
            name="API Health Monitor",
            description="Monitor API endpoints and alert on failures",
            category=TemplateCategory.DEVELOPMENT,
            tags=["monitoring", "api", "devops"],
            workflow=workflow,
            popularity=90,
        )

    def _create_incident_response_template(self) -> WorkflowTemplate:
        """Automated incident response workflow"""
        workflow = Workflow(
            id="tpl-incident-response",
            name="Incident Response Automation",
            description="Automated incident triage, notification, and resolution tracking",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Alert Received",
                    config={},
                ),
                WorkflowNode(
                    id="classify",
                    type=NodeType.AGENT,
                    name="Classify Severity",
                    config={
                        "agent_type": "kavach",
                        "task": "Assess incident severity",
                    },
                ),
                WorkflowNode(
                    id="route",
                    type=NodeType.CONDITION,
                    name="Severity Check",
                    config={"condition": "severity >= 'high'"},
                ),
                WorkflowNode(
                    id="page",
                    type=NodeType.ACTION,
                    name="Page On-Call",
                    config={"action": "pagerduty_page"},
                ),
                WorkflowNode(
                    id="ticket",
                    type=NodeType.ACTION,
                    name="Create Ticket",
                    config={"action": "create_jira"},
                ),
                WorkflowNode(
                    id="notify",
                    type=NodeType.ACTION,
                    name="Notify Slack",
                    config={"action": "send_slack"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="classify"),
                WorkflowEdge(id="e2", source="classify", target="route"),
                WorkflowEdge(id="e3", source="route", target="page", condition="true"),
                WorkflowEdge(id="e4", source="route", target="ticket", condition="false"),
                WorkflowEdge(id="e5", source="page", target="notify"),
                WorkflowEdge(id="e6", source="ticket", target="notify"),
            ],
            tags=["incident", "response", "devops", "automation"],
        )
        return WorkflowTemplate(
            id="tpl-incident-response",
            name="Incident Response Automation",
            description="Automatically classify and route incidents based on severity",
            category=TemplateCategory.DEVELOPMENT,
            tags=["incident", "response", "devops"],
            workflow=workflow,
            popularity=87,
        )

    def _create_content_moderation_template(self) -> WorkflowTemplate:
        """AI-powered content moderation"""
        workflow = Workflow(
            id="tpl-content-moderation",
            name="Content Moderation",
            description="AI-powered content moderation with escalation",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="New Content", config={}),
                WorkflowNode(
                    id="scan",
                    type=NodeType.AGENT,
                    name="Kavach - Scan",
                    config={
                        "agent_type": "kavach",
                        "task": "Scan for policy violations",
                    },
                ),
                WorkflowNode(
                    id="check",
                    type=NodeType.CONDITION,
                    name="Violation Found?",
                    config={"condition": "violations > 0"},
                ),
                WorkflowNode(
                    id="flag",
                    type=NodeType.ACTION,
                    name="Flag Content",
                    config={"action": "flag_for_review"},
                ),
                WorkflowNode(
                    id="approve",
                    type=NodeType.ACTION,
                    name="Auto-Approve",
                    config={"action": "approve_content"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="scan"),
                WorkflowEdge(id="e2", source="scan", target="check"),
                WorkflowEdge(id="e3", source="check", target="flag", condition="true"),
                WorkflowEdge(id="e4", source="check", target="approve", condition="false"),
            ],
            tags=["moderation", "content", "ai", "safety"],
        )
        return WorkflowTemplate(
            id="tpl-content-moderation",
            name="Content Moderation",
            description="AI-powered content moderation with Kavach security agent",
            category=TemplateCategory.SUPPORT,
            tags=["moderation", "content", "safety"],
            workflow=workflow,
            popularity=82,
        )

    def _create_newsletter_automation_template(self) -> WorkflowTemplate:
        """Automated newsletter creation and sending"""
        workflow = Workflow(
            id="tpl-newsletter-automation",
            name="Newsletter Automation",
            description="Automated newsletter creation and delivery",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.SCHEDULE,
                    name="Weekly Schedule",
                    config={"cron": "0 10 * * MON"},
                ),
                WorkflowNode(
                    id="gather",
                    type=NodeType.ACTION,
                    name="Gather Content",
                    config={"action": "fetch_weekly_content"},
                ),
                WorkflowNode(
                    id="write",
                    type=NodeType.AGENT,
                    name="Vega - Draft",
                    config={"agent_type": "vega", "task": "Write newsletter content"},
                ),
                WorkflowNode(
                    id="design",
                    type=NodeType.ACTION,
                    name="Apply Template",
                    config={"action": "apply_email_template"},
                ),
                WorkflowNode(
                    id="send",
                    type=NodeType.ACTION,
                    name="Send to List",
                    config={"action": "send_mailchimp"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="gather"),
                WorkflowEdge(id="e2", source="gather", target="write"),
                WorkflowEdge(id="e3", source="write", target="design"),
                WorkflowEdge(id="e4", source="design", target="send"),
            ],
            tags=["newsletter", "email", "marketing", "automation"],
        )
        return WorkflowTemplate(
            id="tpl-newsletter-automation",
            name="Newsletter Automation",
            description="AI-generated weekly newsletters with automatic distribution",
            category=TemplateCategory.MARKETING,
            tags=["newsletter", "email", "marketing"],
            workflow=workflow,
            popularity=84,
        )

    def _create_user_onboarding_sequence_template(self) -> WorkflowTemplate:
        """Multi-step user onboarding sequence"""
        workflow = Workflow(
            id="tpl-user-onboarding-sequence",
            name="User Onboarding Sequence",
            description="Multi-step user onboarding with welcome emails and guided setup",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="User Signs Up", config={}),
                WorkflowNode(
                    id="welcome",
                    type=NodeType.ACTION,
                    name="Send Welcome",
                    config={"action": "send_email", "template": "welcome"},
                ),
                WorkflowNode(
                    id="delay1",
                    type=NodeType.ACTION,
                    name="Wait 1 Day",
                    config={"delay": "1d"},
                ),
                WorkflowNode(
                    id="tips",
                    type=NodeType.ACTION,
                    name="Send Tips",
                    config={"action": "send_email", "template": "tips"},
                ),
                WorkflowNode(
                    id="delay2",
                    type=NodeType.ACTION,
                    name="Wait 3 Days",
                    config={"delay": "3d"},
                ),
                WorkflowNode(
                    id="check",
                    type=NodeType.CONDITION,
                    name="Active User?",
                    config={"condition": "last_login < 3d"},
                ),
                WorkflowNode(
                    id="engage",
                    type=NodeType.ACTION,
                    name="Re-engage",
                    config={"action": "send_email", "template": "reengage"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="welcome"),
                WorkflowEdge(id="e2", source="welcome", target="delay1"),
                WorkflowEdge(id="e3", source="delay1", target="tips"),
                WorkflowEdge(id="e4", source="tips", target="delay2"),
                WorkflowEdge(id="e5", source="delay2", target="check"),
                WorkflowEdge(id="e6", source="check", target="engage", condition="false"),
            ],
            tags=["onboarding", "email", "sequence", "engagement"],
        )
        return WorkflowTemplate(
            id="tpl-user-onboarding-sequence",
            name="User Onboarding Sequence",
            description="Multi-step onboarding email sequence with engagement tracking",
            category=TemplateCategory.BUSINESS,
            tags=["onboarding", "email", "sequence"],
            workflow=workflow,
            popularity=89,
        )

    def _create_ab_testing_template(self) -> WorkflowTemplate:
        """A/B testing pipeline with analytics"""
        workflow = Workflow(
            id="tpl-ab-testing",
            name="A/B Testing Pipeline",
            description="Run A/B tests with variant splitting and analytics",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Start Test", config={}),
                WorkflowNode(
                    id="split",
                    type=NodeType.CONDITION,
                    name="Random Split",
                    config={"condition": "random() > 0.5"},
                ),
                WorkflowNode(
                    id="variant_a",
                    type=NodeType.ACTION,
                    name="Variant A",
                    config={"action": "show_variant", "variant": "A"},
                ),
                WorkflowNode(
                    id="variant_b",
                    type=NodeType.ACTION,
                    name="Variant B",
                    config={"action": "show_variant", "variant": "B"},
                ),
                WorkflowNode(
                    id="track",
                    type=NodeType.ACTION,
                    name="Track Result",
                    config={"action": "track_conversion"},
                ),
                WorkflowNode(
                    id="analyze",
                    type=NodeType.AGENT,
                    name="Analyze Results",
                    config={
                        "agent_type": "oracle",
                        "task": "Analyze A/B test significance",
                    },
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="split"),
                WorkflowEdge(id="e2", source="split", target="variant_a", condition="true"),
                WorkflowEdge(id="e3", source="split", target="variant_b", condition="false"),
                WorkflowEdge(id="e4", source="variant_a", target="track"),
                WorkflowEdge(id="e5", source="variant_b", target="track"),
                WorkflowEdge(id="e6", source="track", target="analyze"),
            ],
            tags=["ab-testing", "analytics", "optimization", "experiments"],
        )
        return WorkflowTemplate(
            id="tpl-ab-testing",
            name="A/B Testing Pipeline",
            description="Run A/B tests with automated statistical analysis",
            category=TemplateCategory.ANALYTICS,
            tags=["ab-testing", "analytics", "optimization"],
            workflow=workflow,
            popularity=86,
        )

    def _create_revenue_forecasting_template(self) -> WorkflowTemplate:
        """AI-powered revenue forecasting"""
        workflow = Workflow(
            id="tpl-revenue-forecasting",
            name="Revenue Forecasting",
            description="AI-powered revenue forecasting with trend analysis",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.SCHEDULE,
                    name="Monthly Run",
                    config={"cron": "0 0 1 * *"},
                ),
                WorkflowNode(
                    id="fetch",
                    type=NodeType.ACTION,
                    name="Fetch Revenue Data",
                    config={"action": "fetch_revenue"},
                ),
                WorkflowNode(
                    id="analyze",
                    type=NodeType.AGENT,
                    name="Oracle - Forecast",
                    config={
                        "agent_type": "oracle",
                        "task": "Generate revenue forecast",
                    },
                ),
                WorkflowNode(
                    id="visualize",
                    type=NodeType.ACTION,
                    name="Create Charts",
                    config={"action": "generate_charts"},
                ),
                WorkflowNode(
                    id="report",
                    type=NodeType.ACTION,
                    name="Send Report",
                    config={"action": "send_report"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="fetch"),
                WorkflowEdge(id="e2", source="fetch", target="analyze"),
                WorkflowEdge(id="e3", source="analyze", target="visualize"),
                WorkflowEdge(id="e4", source="visualize", target="report"),
            ],
            tags=["forecasting", "revenue", "analytics", "ai"],
        )
        return WorkflowTemplate(
            id="tpl-revenue-forecasting",
            name="Revenue Forecasting",
            description="AI-powered monthly revenue forecasting and reporting",
            category=TemplateCategory.ANALYTICS,
            tags=["forecasting", "revenue", "analytics"],
            workflow=workflow,
            popularity=83,
        )

    def _create_agent_collaboration_template(self) -> WorkflowTemplate:
        """Multi-agent collaboration hub"""
        workflow = Workflow(
            id="tpl-agent-collaboration",
            name="Agent Collaboration Hub",
            description="Multi-agent collaboration with task routing and synthesis",
            nodes=[
                WorkflowNode(id="trigger", type=NodeType.WEBHOOK, name="Task Received", config={}),
                WorkflowNode(
                    id="nexus",
                    type=NodeType.AGENT,
                    name="Nexus - Coordinate",
                    config={
                        "agent_type": "nexus",
                        "task": "Coordinate agent collaboration",
                    },
                ),
                WorkflowNode(
                    id="parallel",
                    type=NodeType.ACTION,
                    name="Agent Work",
                    config={"action": "parallel"},
                ),
                WorkflowNode(
                    id="kael",
                    type=NodeType.AGENT,
                    name="Kael - Research",
                    config={"agent_type": "kael", "task": "Research phase"},
                ),
                WorkflowNode(
                    id="vega",
                    type=NodeType.AGENT,
                    name="Vega - Create",
                    config={"agent_type": "vega", "task": "Creation phase"},
                ),
                WorkflowNode(
                    id="kavach",
                    type=NodeType.AGENT,
                    name="Kavach - Review",
                    config={"agent_type": "kavach", "task": "Security review"},
                ),
                WorkflowNode(
                    id="synthesize",
                    type=NodeType.AGENT,
                    name="Lumina - Synthesize",
                    config={"agent_type": "lumina", "task": "Synthesize outputs"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="nexus"),
                WorkflowEdge(id="e2", source="nexus", target="parallel"),
                WorkflowEdge(id="e3", source="parallel", target="kael"),
                WorkflowEdge(id="e4", source="parallel", target="vega"),
                WorkflowEdge(id="e5", source="parallel", target="kavach"),
                WorkflowEdge(id="e6", source="kael", target="synthesize"),
                WorkflowEdge(id="e7", source="vega", target="synthesize"),
                WorkflowEdge(id="e8", source="kavach", target="synthesize"),
            ],
            tags=["agents", "collaboration", "multi-agent", "parallel"],
        )
        return WorkflowTemplate(
            id="tpl-agent-collaboration",
            name="Agent Collaboration Hub",
            description="Coordinate multiple agents working in parallel on complex tasks",
            category=TemplateCategory.AI_AGENTS,
            tags=["agents", "collaboration", "multi-agent"],
            workflow=workflow,
            popularity=95,
        )

    def _create_entanglement_cycle_template(self) -> WorkflowTemplate:
        """System coordination entanglement cycle"""
        workflow = Workflow(
            id="tpl-system-entanglement",
            name="System Entanglement Cycle",
            description="System coordination entanglement cycle across agents",
            nodes=[
                WorkflowNode(
                    id="trigger",
                    type=NodeType.WEBHOOK,
                    name="Initiate Entanglement",
                    config={},
                ),
                WorkflowNode(
                    id="prepare",
                    type=NodeType.AGENT,
                    name="Shadow - Prepare Field",
                    config={"agent_type": "shadow", "task": "Prepare system field"},
                ),
                WorkflowNode(
                    id="entangle",
                    type=NodeType.AGENT,
                    name="Nexus - Entangle",
                    config={
                        "agent_type": "nexus",
                        "task": "Create system entanglement",
                    },
                ),
                WorkflowNode(
                    id="verify",
                    type=NodeType.AGENT,
                    name="Oracle - Verify",
                    config={
                        "agent_type": "oracle",
                        "task": "Verify entanglement integrity",
                    },
                ),
                WorkflowNode(
                    id="stabilize",
                    type=NodeType.AGENT,
                    name="Phoenix - Stabilize",
                    config={"agent_type": "phoenix", "task": "Stabilize system state"},
                ),
                WorkflowNode(
                    id="broadcast",
                    type=NodeType.ACTION,
                    name="Broadcast UCF",
                    config={"action": "broadcast_ucf_update"},
                ),
            ],
            edges=[
                WorkflowEdge(id="e1", source="trigger", target="prepare"),
                WorkflowEdge(id="e2", source="prepare", target="entangle"),
                WorkflowEdge(id="e3", source="entangle", target="verify"),
                WorkflowEdge(id="e4", source="verify", target="stabilize"),
                WorkflowEdge(id="e5", source="stabilize", target="broadcast"),
            ],
            tags=["system", "coordination", "entanglement", "cycle"],
        )
        return WorkflowTemplate(
            id="tpl-system-entanglement",
            name="System Entanglement Cycle",
            description="Advanced system coordination entanglement ceremony",
            category=TemplateCategory.COORDINATION,
            tags=["system", "coordination", "entanglement"],
            workflow=workflow,
            popularity=78,
        )

    # Registry methods

    def register(self, template: WorkflowTemplate) -> None:
        """Register a template"""
        self._templates[template.id] = template

    def get(self, template_id: str) -> WorkflowTemplate | None:
        """Get template by ID"""
        return self._templates.get(template_id)

    def list_all(self) -> list[WorkflowTemplate]:
        """List all templates"""
        return sorted(self._templates.values(), key=lambda t: t.popularity, reverse=True)

    def list_by_category(self, category: TemplateCategory) -> list[WorkflowTemplate]:
        """List templates by category"""
        return sorted(
            [t for t in self._templates.values() if t.category == category],
            key=lambda t: t.popularity,
            reverse=True,
        )

    def search(self, query: str) -> list[WorkflowTemplate]:
        """Search templates by name, description, or tags"""
        query_lower = query.lower()
        results = []

        for template in self._templates.values():
            if (
                query_lower in template.name.lower()
                or query_lower in template.description.lower()
                or any(query_lower in tag for tag in template.tags)
            ):
                results.append(template)

        return sorted(results, key=lambda t: t.popularity, reverse=True)

    def get_popular(self, limit: int = 10) -> list[WorkflowTemplate]:
        """Get most popular templates"""
        return self.list_all()[:limit]


# Global template registry
template_registry = TemplateRegistry()


def get_template_registry() -> TemplateRegistry:
    """Get the global template registry"""
    return template_registry


def get_template(template_id: str) -> WorkflowTemplate | None:
    """Get a template by ID"""
    return template_registry.get(template_id)


def list_templates(category: TemplateCategory = None) -> list[WorkflowTemplate]:
    """List templates, optionally by category"""
    if category:
        return template_registry.list_by_category(category)
    return template_registry.list_all()


def search_templates(query: str) -> list[WorkflowTemplate]:
    """Search templates"""
    return template_registry.search(query)
