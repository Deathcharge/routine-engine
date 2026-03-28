"""
API Routes for Helix Spirals Features.

Comprehensive API routes for:
- Integration connectors
- Workflow marketplace
- Workflow versioning
- OAuth2 management
"""

import logging
import os
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query

try:
    from apps.backend.core.unified_auth import get_current_user
except ImportError:
    try:
        from apps.backend.core.auth import get_current_user
    except ImportError:
        get_current_user = None

from .integrations import (
    AirtableConfig,
    AirtableNode,
    CalendlyConfig,
    CalendlyNode,
    MailchimpConfig,
    MailchimpNode,
    SendGridConfig,
    SendGridNode,
    StripeConfig,
    StripeNode,
    TwilioConfig,
    TwilioNode,
)

# Import additional connectors with fallback
try:
    from .integrations.aws_connector import AWSConfig, AWSConnector
except ImportError:
    AWSConfig = None
    AWSConnector = None

try:
    from .integrations.google_cloud_connector import GoogleCloudConfig, GoogleCloudConnector
except ImportError:
    GoogleCloudConfig = None
    GoogleCloudConnector = None

try:
    from .integrations.discord_connector import DiscordConfig, DiscordConnector
except ImportError:
    DiscordConfig = None
    DiscordConnector = None

try:
    from .integrations.notion_connector import NotionConfig, NotionConnector
except ImportError:
    NotionConfig = None
    NotionConnector = None

from .marketplace import TemplateCategory, TemplateTier, WorkflowTemplateMarketplace
from .oauth import OAuthConnectionManager, OAuthProvider
from .versioning import MergeStrategy, VersionStatus, WorkflowVersioningSystem

# Initialize marketplace instance
marketplace = WorkflowTemplateMarketplace()

logger = logging.getLogger(__name__)

# ==================== API Routers ====================

connectors_router = APIRouter(tags=["connectors"])
marketplace_router = APIRouter(prefix="/marketplace", tags=["marketplace"])
versioning_router = APIRouter(tags=["versioning"])
oauth_router = APIRouter(tags=["oauth"])


def _require_user():
    """Return a FastAPI dependency that extracts the authenticated user_id."""
    if get_current_user is not None:

        async def _dep(user=Depends(get_current_user)):
            return user.get("user_id") or user.get("sub") or "unknown"

        return _dep

    # Auth module not available — reject rather than fall back to demo_user
    async def _no_auth():
        raise HTTPException(status_code=503, detail="Authentication service unavailable")

    return _no_auth


_get_user_id = _require_user()

# ==================== Connector Routes ====================


@connectors_router.get("/nodes/available")
async def list_available_nodes() -> dict[str, Any]:
    """List all available node types for the workflow builder across all registries."""
    try:
        from .additional_integrations import get_all_nodes

        all_nodes = get_all_nodes()
        nodes = []
        categories = {}

        for node_type, node_class in sorted(all_nodes.items()):
            category = getattr(node_class, "category", None)
            category_value = (
                category.value if hasattr(category, "value") else str(category) if category else "uncategorized"
            )
            description = getattr(node_class, "description", "")
            icon = getattr(node_class, "icon", "")

            nodes.append(
                {
                    "type": node_type,
                    "category": category_value,
                    "description": description,
                    "icon": icon,
                }
            )

            if category_value not in categories:
                categories[category_value] = 0
            categories[category_value] += 1

        return {
            "success": True,
            "nodes": nodes,
            "total": len(nodes),
            "categories": categories,
        }
    except Exception as e:
        logger.error("Failed to list available nodes: %s", e)
        raise HTTPException(status_code=500, detail="Failed to list available nodes")


@connectors_router.get("/list")
async def list_connectors() -> dict[str, Any]:
    """List all available integration connectors."""
    connectors = [
        {
            "id": "mailchimp",
            "name": "Mailchimp",
            "description": "Email marketing automation",
            "capabilities": ["audiences", "campaigns", "automation"],
            "icon": "📧",
        },
        {
            "id": "calendly",
            "name": "Calendly",
            "description": "Scheduling automation",
            "capabilities": ["events", "invitees", "availability"],
            "icon": "📅",
        },
        {
            "id": "airtable",
            "name": "Airtable",
            "description": "Database operations",
            "capabilities": ["records", "tables", "bases"],
            "icon": "📊",
        },
        {
            "id": "twilio",
            "name": "Twilio",
            "description": "SMS, Voice, WhatsApp",
            "capabilities": ["sms", "voice", "whatsapp", "phone_numbers"],
            "icon": "📱",
        },
        {
            "id": "sendgrid",
            "name": "SendGrid",
            "description": "Email operations",
            "capabilities": ["email", "templates", "contacts"],
            "icon": "✉️",
        },
        {
            "id": "stripe",
            "name": "Stripe",
            "description": "Payment processing",
            "capabilities": ["payments", "subscriptions", "customers"],
            "icon": "💳",
        },
        {
            "id": "aws",
            "name": "AWS",
            "description": "Amazon Web Services",
            "capabilities": ["s3", "ec2", "lambda", "sqs", "sns"],
            "icon": "☁️",
        },
        {
            "id": "google_cloud",
            "name": "Google Cloud",
            "description": "Google Cloud Platform",
            "capabilities": ["storage", "compute", "functions", "pubsub"],
            "icon": "🔵",
        },
        {
            "id": "discord",
            "name": "Discord",
            "description": "Discord Bot API",
            "capabilities": ["messages", "channels", "guilds", "roles"],
            "icon": "🎮",
        },
        {
            "id": "notion",
            "name": "Notion",
            "description": "Notion API",
            "capabilities": ["databases", "pages", "blocks", "search"],
            "icon": "📝",
        },
    ]

    return {"success": True, "connectors": connectors, "total": len(connectors)}


@connectors_router.post("/mailchimp/execute")
async def execute_mailchimp(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute a Mailchimp operation."""
    try:
        mailchimp_config = MailchimpConfig(api_key=config["api_key"], server_prefix=config["server_prefix"])

        node = MailchimpNode(mailchimp_config)
        result = await node.execute(operation, params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("Mailchimp operation failed: %s", e)
        raise HTTPException(status_code=400, detail="Mailchimp operation failed")


@connectors_router.post("/calendly/execute")
async def execute_calendly(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute a Calendly operation."""
    try:
        calendly_config = CalendlyConfig(
            api_key=config["api_key"],
            organization_uri=config.get("organization_uri"),
            user_uri=config.get("user_uri"),
        )

        node = CalendlyNode(calendly_config)
        result = await node.execute(operation, params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("Calendly operation failed: %s", e)
        raise HTTPException(status_code=400, detail="Calendly operation failed")


@connectors_router.post("/airtable/execute")
async def execute_airtable(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute an Airtable operation."""
    try:
        airtable_config = AirtableConfig(api_key=config["api_key"])

        node = AirtableNode(airtable_config)
        result = await node.execute(operation, params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("Airtable operation failed: %s", e)
        raise HTTPException(status_code=400, detail="Airtable operation failed")


@connectors_router.post("/twilio/execute")
async def execute_twilio(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute a Twilio operation."""
    try:
        twilio_config = TwilioConfig(account_sid=config["account_sid"], auth_token=config["auth_token"])

        node = TwilioNode(twilio_config)
        result = await node.execute(operation, params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("Twilio operation failed: %s", e)
        raise HTTPException(status_code=400, detail="Twilio operation failed")


@connectors_router.post("/sendgrid/execute")
async def execute_sendgrid(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute a SendGrid operation."""
    try:
        sendgrid_config = SendGridConfig(api_key=config["api_key"])

        node = SendGridNode(sendgrid_config)
        result = await node.execute(operation, params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("SendGrid operation failed: %s", e)
        raise HTTPException(status_code=400, detail="SendGrid operation failed")


@connectors_router.post("/stripe/execute")
async def execute_stripe(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute a Stripe operation."""
    try:
        stripe_config = StripeConfig(api_key=config["api_key"])

        node = StripeNode(stripe_config)
        result = await node.execute(operation, params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("Stripe operation failed: %s", e)
        raise HTTPException(status_code=400, detail="Stripe operation failed")


# ==================== New Connector Endpoints ====================


@connectors_router.post("/aws/execute")
async def execute_aws(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute an AWS operation."""
    try:

        aws_config = AWSConfig(
            access_key_id=config["access_key_id"],
            secret_access_key=config["secret_access_key"],
            region=config.get("region", "us-east-1"),
            session_token=config.get("session_token"),
            role_arn=config.get("role_arn"),
        )

        connector = AWSConnector(aws_config)

        # Map operation to method
        method_map = {
            "list_s3_buckets": connector.list_s3_buckets,
            "list_s3_objects": connector.list_objects,
            "upload_s3_object": connector.upload_s3_object,
            "download_s3_object": connector.download_s3_object,
            "describe_instances": connector.describe_instances,
            "start_instance": connector.start_instance,
            "stop_instance": connector.stop_instance,
            "list_lambda_functions": connector.list_lambda_functions,
            "invoke_lambda": connector.invoke_lambda,
        }

        if operation not in method_map:
            raise HTTPException(status_code=400, detail=f"Unknown AWS operation: {operation}")

        method = method_map[operation]
        result = await method(**params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("AWS operation failed: %s", e)
        raise HTTPException(status_code=400, detail="AWS operation failed")


@connectors_router.post("/google_cloud/execute")
async def execute_google_cloud(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute a Google Cloud operation."""
    try:

        gcp_config = GoogleCloudConfig(
            project_id=config["project_id"],
            credentials_json=config.get("credentials_json"),
            service_account_key_path=config.get("service_account_key_path"),
        )

        connector = GoogleCloudConnector(gcp_config)

        # Map operation to method
        method_map = {
            "list_buckets": connector.list_buckets,
            "list_objects": connector.list_objects,
            "upload_object": connector.upload_object,
            "download_object": connector.download_object,
            "list_instances": connector.list_instances,
            "start_instance": connector.start_instance,
            "stop_instance": connector.stop_instance,
            "list_functions": connector.list_functions,
            "call_function": connector.call_function,
        }

        if operation not in method_map:
            raise HTTPException(status_code=400, detail=f"Unknown Google Cloud operation: {operation}")

        method = method_map[operation]
        result = await method(**params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("Google Cloud operation failed: %s", e)
        raise HTTPException(status_code=400, detail="Google Cloud operation failed")


@connectors_router.post("/discord/execute")
async def execute_discord(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute a Discord operation."""
    try:

        discord_config = DiscordConfig(
            bot_token=config["bot_token"],
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            redirect_uri=config.get("redirect_uri"),
        )

        connector = DiscordConnector(discord_config)

        # Map operation to method
        method_map = {
            "send_message": connector.send_message,
            "edit_message": connector.edit_message,
            "delete_message": connector.delete_message,
            "get_channel_messages": connector.get_channel_messages,
            "list_channels": connector.list_channels,
            "create_channel": connector.create_channel,
            "list_guilds": connector.list_guilds,
            "list_members": connector.list_members,
        }

        if operation not in method_map:
            raise HTTPException(status_code=400, detail=f"Unknown Discord operation: {operation}")

        method = method_map[operation]
        result = await method(**params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("Discord operation failed: %s", e)
        raise HTTPException(status_code=400, detail="Discord operation failed")


@connectors_router.post("/notion/execute")
async def execute_notion(
    operation: str = Body(..., embed=True),
    params: dict[str, Any] = Body(..., embed=True),
    config: dict[str, str] = Body(..., embed=True),
) -> dict[str, Any]:
    """Execute a Notion operation."""
    try:

        notion_config = NotionConfig(
            access_token=config["access_token"],
        )

        connector = NotionConnector(notion_config)

        # Map operation to method
        method_map = {
            "list_databases": connector.list_databases,
            "get_database": connector.get_database,
            "query_database": connector.query_database,
            "list_pages": connector.list_pages,
            "get_page": connector.get_page,
            "create_page": connector.create_page,
            "update_page": connector.update_page,
            "get_block_children": connector.get_block_children,
        }

        if operation not in method_map:
            raise HTTPException(status_code=400, detail=f"Unknown Notion operation: {operation}")

        method = method_map[operation]
        result = await method(**params)

        return {"success": True, "result": result}
    except Exception as e:
        logger.error("Notion operation failed: %s", e)
        raise HTTPException(status_code=400, detail="Notion operation failed")


# ==================== Marketplace Routes ====================

# Initialize marketplace
marketplace = WorkflowTemplateMarketplace()


@marketplace_router.get("/templates")
async def search_templates(
    query: str | None = None,
    category: str | None = None,
    tags: list[str] | None = Query(None),
    tier: str | None = None,
    min_rating: float | None = None,
    sort_by: str = "popularity",
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Search and filter workflow templates."""
    try:
        category_enum = TemplateCategory(category) if category else None

        result = await marketplace.search_templates(
            query=query,
            category=category_enum,
            tags=tags,
            tier=TemplateTier(tier) if tier else None,
            min_rating=min_rating,
            sort_by=sort_by,
            page=page,
            page_size=page_size,
        )

        return {
            "success": True,
            "templates": [
                {
                    "id": t.id,
                    "name": t.name,
                    "description": t.description,
                    "category": t.category.value,
                    "tier": t.tier.value,
                    "avg_rating": t.avg_rating,
                    "stats": {
                        "installs": t.stats.installs,
                        "executions": t.stats.executions,
                        "success_rate": t.stats.success_rate,
                    },
                    "author": {
                        "id": t.author.id,
                        "name": t.author.name,
                        "verified": t.author.verified,
                    },
                    "tags": t.tags,
                    "integrations": t.integrations,
                }
                for t in result["templates"]
            ],
            "total": result["total"],
            "page": result["page"],
            "page_size": result["page_size"],
            "total_pages": result["total_pages"],
        }
    except Exception as e:
        logger.error("Template search failed: %s", e)
        raise HTTPException(status_code=400, detail="Template search failed")


@marketplace_router.get("/templates/{template_id}")
async def get_template(template_id: str) -> dict[str, Any]:
    """Get a specific template."""
    try:
        template = await marketplace.get_template(template_id)

        if not template:
            raise HTTPException(status_code=404, detail="Template not found")

        return {
            "success": True,
            "template": {
                "id": template.id,
                "name": template.name,
                "description": template.description,
                "category": template.category.value,
                "tier": template.tier.value,
                "workflow_definition": template.workflow_definition,
                "config_schema": template.config_schema,
                "default_config": template.default_config,
                "tags": template.tags,
                "integrations": template.integrations,
                "avg_rating": template.avg_rating,
                "ratings_count": len(template.ratings),
                "stats": {
                    "installs": template.stats.installs,
                    "active_users": template.stats.active_users,
                    "executions": template.stats.executions,
                    "success_rate": template.stats.success_rate,
                    "avg_execution_time": template.stats.avg_execution_time,
                },
                "author": {
                    "id": template.author.id,
                    "name": template.author.name,
                    "verified": template.author.verified,
                },
                "created_at": template.created_at.isoformat(),
                "updated_at": template.updated_at.isoformat(),
                "current_version": template.current_version,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Get template failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to retrieve template")


@marketplace_router.post("/templates/{template_id}/install")
async def install_template(
    template_id: str,
    config: dict[str, Any] | None = Body(None),
    version: str | None = None,
    user_id: str = Depends(_get_user_id),
) -> dict[str, Any]:
    """Install a template."""
    try:
        installed = await marketplace.install_template(
            template_id=template_id,
            user_id=user_id,
            organization_id=None,
            config=config or {},
            version=version,
        )

        return {
            "success": True,
            "installed": {
                "id": installed.id,
                "template_id": installed.template_id,
                "version": installed.version,
                "workflow_id": installed.workflow_id,
                "installed_at": installed.installed_at.isoformat(),
            },
        }
    except Exception as e:
        logger.error("Template installation failed: %s", e)
        raise HTTPException(status_code=400, detail="Template installation failed")


@marketplace_router.get("/templates/{template_id}/reviews")
async def get_template_reviews(template_id: str, page: int = 1, page_size: int = 20) -> dict[str, Any]:
    """Get reviews for a template."""
    try:
        result = await marketplace.get_template_reviews(template_id=template_id, page=page, page_size=page_size)

        return {
            "success": True,
            "reviews": [
                {
                    "user_id": r.user_id,
                    "rating": r.rating,
                    "review": r.review,
                    "created_at": r.created_at.isoformat(),
                    "helpful_count": r.helpful_count,
                }
                for r in result["reviews"]
            ],
            "total": result["total"],
            "avg_rating": result["avg_rating"],
        }
    except Exception as e:
        logger.error("Get reviews failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to retrieve reviews")


@marketplace_router.post("/templates/{template_id}/rate")
async def rate_template(
    template_id: str,
    rating: int = Body(..., embed=True),
    review: str | None = Body(None, embed=True),
    user_id: str = Depends(_get_user_id),
) -> dict[str, Any]:
    """Rate a template."""
    try:
        template = await marketplace.rate_template(
            template_id=template_id,
            user_id=user_id,
            rating=rating,
            review=review,
        )

        return {
            "success": True,
            "template_id": template_id,
            "avg_rating": template.avg_rating,
        }
    except Exception as e:
        logger.error("Rate template failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to rate template")


# ==================== Versioning Routes ====================

# Initialize versioning system
versioning = WorkflowVersioningSystem()


@versioning_router.get("/workflows/{workflow_id}/versions")
async def get_workflow_versions(
    workflow_id: str,
    branch: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Get all versions of a workflow."""
    try:
        status_enum = VersionStatus(status) if status else None

        versions = await versioning.get_workflow_versions(
            workflow_id=workflow_id, branch=branch, status=status_enum, limit=limit
        )

        return {
            "success": True,
            "versions": [
                {
                    "id": v.id,
                    "version_number": v.version_number,
                    "name": v.name,
                    "description": v.description,
                    "status": v.status.value,
                    "branch": v.branch,
                    "is_head": v.is_head,
                    "created_by": v.created_by,
                    "created_at": v.created_at.isoformat(),
                    "tags": v.tags,
                    "change_summary": v.change_summary,
                }
                for v in versions
            ],
            "total": len(versions),
        }
    except Exception as e:
        logger.error("Get versions failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to retrieve versions")


@versioning_router.get("/versions/{version_id}")
async def get_version(version_id: str) -> dict[str, Any]:
    """Get a specific version."""
    try:
        version = await versioning.get_version(version_id)

        if not version:
            raise HTTPException(status_code=404, detail="Version not found")

        return {
            "success": True,
            "version": {
                "id": version.id,
                "workflow_id": version.workflow_id,
                "version_number": version.version_number,
                "name": version.name,
                "description": version.description,
                "status": version.status.value,
                "branch": version.branch,
                "is_head": version.is_head,
                "definition": version.definition,
                "created_by": version.created_by,
                "created_at": version.created_at.isoformat(),
                "tags": version.tags,
                "change_summary": version.change_summary,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Get version failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to retrieve version")


@versioning_router.post("/workflows/{workflow_id}/versions")
async def create_version(
    workflow_id: str,
    definition: dict[str, Any] = Body(..., embed=True),
    name: str = Body(..., embed=True),
    description: str = Body(..., embed=True),
    branch: str = "main",
    user_id: str = Depends(_get_user_id),
) -> dict[str, Any]:
    """Create a new version of a workflow."""
    try:
        version = await versioning.create_version(
            workflow_id=workflow_id,
            definition=definition,
            name=name,
            description=description,
            created_by=user_id,
            branch=branch,
        )

        return {
            "success": True,
            "version": {
                "id": version.id,
                "version_number": version.version_number,
                "name": version.name,
                "description": version.description,
                "status": version.status.value,
                "branch": version.branch,
                "created_at": version.created_at.isoformat(),
            },
        }
    except Exception as e:
        logger.error("Create version failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to create version")


@versioning_router.post("/versions/{version_id}/activate")
async def activate_version(version_id: str, user_id: str = Depends(_get_user_id)) -> dict[str, Any]:
    """Activate a version (make it the current active version)."""
    try:
        version = await versioning.activate_version(
            version_id=version_id,
            activated_by=user_id,
        )

        return {
            "success": True,
            "version_id": version.id,
            "version_number": version.version_number,
            "status": version.status.value,
        }
    except Exception as e:
        logger.error("Activate version failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to activate version")


@versioning_router.post("/versions/{version_id}/rollback")
async def rollback_to_version(
    version_id: str,
    reason: str | None = Body(None, embed=True),
    user_id: str = Depends(_get_user_id),
) -> dict[str, Any]:
    """Rollback to a previous version."""
    try:
        version = await versioning.rollback_to_version(
            workflow_id=version_id,  # version lookup resolves the workflow
            target_version_id=version_id,
            rolled_back_by=user_id,
            reason=reason or "Manual rollback",
        )

        return {
            "success": True,
            "version_id": version.id,
            "version_number": version.version_number,
        }
    except Exception as e:
        logger.error("Rollback failed: %s", e)
        raise HTTPException(status_code=400, detail="Version rollback failed")


@versioning_router.post("/workflows/{workflow_id}/branches")
async def create_branch(
    workflow_id: str,
    branch_name: str = Body(..., embed=True),
    base_version_id: str = Body(..., embed=True),
    description: str = Body(..., embed=True),
    user_id: str = Depends(_get_user_id),
) -> dict[str, Any]:
    """Create a new branch."""
    try:
        branch = await versioning.create_branch(
            workflow_id=workflow_id,
            branch_name=branch_name,
            base_version_id=base_version_id,
            description=description,
            created_by=user_id,
        )

        return {
            "success": True,
            "branch": {
                "id": branch.id,
                "name": branch.name,
                "description": branch.description,
                "head_version_id": branch.head_version_id,
                "created_at": branch.created_at.isoformat(),
            },
        }
    except Exception as e:
        logger.error("Create branch failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to create branch")


@versioning_router.post("/workflows/{workflow_id}/branches/merge")
async def merge_branches(
    workflow_id: str,
    source_branch: str = Body(..., embed=True),
    target_branch: str = Body(..., embed=True),
    strategy: str = "manual",
    user_id: str = Depends(_get_user_id),
) -> dict[str, Any]:
    """Merge one branch into another."""
    try:
        result = await versioning.merge_branches(
            workflow_id=workflow_id,
            source_branch=source_branch,
            target_branch=target_branch,
            merged_by=user_id,
            strategy=MergeStrategy(strategy),
        )

        return {
            "success": result.success,
            "merged_version_id": result.merged_version_id,
            "conflicts": [
                {
                    "path": c.path,
                    "ours_value": c.ours_value,
                    "theirs_value": c.theirs_value,
                    "resolved": c.resolved,
                }
                for c in result.conflicts
            ],
            "message": result.message,
        }
    except Exception as e:
        logger.error("Merge failed: %s", e)
        raise HTTPException(status_code=400, detail="Branch merge failed")


# ==================== OAuth Routes ====================

_oauth_encryption_key = os.getenv("OAUTH_ENCRYPTION_KEY")
if not _oauth_encryption_key:
    import secrets as _secrets
    _oauth_encryption_key = _secrets.token_hex(32)
    logger.warning(
        "OAUTH_ENCRYPTION_KEY not set — generated ephemeral key. "
        "OAuth tokens will NOT survive restarts. Set OAUTH_ENCRYPTION_KEY in production."
    )

oauth_manager = OAuthConnectionManager(
    encryption_key=_oauth_encryption_key,
    base_redirect_uri=os.getenv("APP_URL", "http://localhost:8000") + "/oauth/callback",
)


@oauth_router.get("/providers")
async def list_oauth_providers() -> dict[str, Any]:
    """List available OAuth providers."""
    try:
        providers = oauth_manager.get_available_providers()

        return {"success": True, "providers": providers, "total": len(providers)}
    except Exception as e:
        logger.error("List providers failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to list OAuth providers")


@oauth_router.post("/providers/{provider}/authorize")
async def get_authorization_url(
    provider: str,
    user_id: str = Body(..., embed=True),
    scopes: list[str] | None = Body(None, embed=True),
) -> dict[str, Any]:
    """Get authorization URL for OAuth flow."""
    try:
        provider_enum = OAuthProvider(provider)

        auth_url, state_id = await oauth_manager.get_authorization_url(
            provider=provider_enum, user_id=user_id, scopes=scopes
        )

        return {"success": True, "authorization_url": auth_url, "state_id": state_id}
    except Exception as e:
        logger.error("Get authorization URL failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to generate authorization URL")


@oauth_router.post("/providers/{provider}/callback")
async def handle_oauth_callback(
    provider: str,
    code: str = Body(..., embed=True),
    state_id: str = Body(..., embed=True),
) -> dict[str, Any]:
    """Handle OAuth callback."""
    try:
        provider_enum = OAuthProvider(provider)

        connection = await oauth_manager.handle_callback(provider=provider_enum, code=code, state_id=state_id)

        return {
            "success": True,
            "connection": {
                "id": connection.id,
                "provider": connection.provider.value,
                "status": connection.status.value,
                "provider_user_id": connection.provider_user_id,
                "provider_email": connection.provider_email,
                "provider_name": connection.provider_name,
                "created_at": connection.created_at.isoformat(),
            },
        }
    except Exception as e:
        logger.error("OAuth callback failed: %s", e)
        raise HTTPException(status_code=400, detail="OAuth callback processing failed")


@oauth_router.get("/connections")
async def list_connections(user_id: str = Query(...), provider: str | None = None) -> dict[str, Any]:
    """List user's OAuth connections."""
    try:
        provider_enum = OAuthProvider(provider) if provider else None

        connections = await oauth_manager.get_user_connections(user_id=user_id, provider=provider_enum)

        return {
            "success": True,
            "connections": [
                {
                    "id": c.id,
                    "provider": c.provider.value,
                    "status": c.status.value,
                    "provider_user_id": c.provider_user_id,
                    "provider_email": c.provider_email,
                    "provider_name": c.provider_name,
                    "scopes": c.scopes,
                    "created_at": c.created_at.isoformat(),
                    "last_used_at": (c.last_used_at.isoformat() if c.last_used_at else None),
                    "expires_at": c.expires_at.isoformat() if c.expires_at else None,
                }
                for c in connections
            ],
            "total": len(connections),
        }
    except Exception as e:
        logger.error("List connections failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to list connections")


@oauth_router.get("/connections/{connection_id}")
async def get_connection(connection_id: str) -> dict[str, Any]:
    """Get a specific OAuth connection."""
    try:
        connection = await oauth_manager.get_connection(connection_id)

        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")

        return {
            "success": True,
            "connection": {
                "id": connection.id,
                "provider": connection.provider.value,
                "status": connection.status.value,
                "provider_user_id": connection.provider_user_id,
                "provider_email": connection.provider_email,
                "provider_name": connection.provider_name,
                "scopes": connection.scopes,
                "created_at": connection.created_at.isoformat(),
                "last_used_at": (connection.last_used_at.isoformat() if connection.last_used_at else None),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Get connection failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to retrieve connection")


@oauth_router.delete("/connections/{connection_id}")
async def revoke_connection(connection_id: str, user_id: str = Depends(_get_user_id)) -> dict[str, Any]:
    """Revoke an OAuth connection."""
    try:
        await oauth_manager.revoke_connection(
            connection_id=connection_id,
            revoked_by=user_id,
        )

        return {
            "success": True,
            "connection_id": connection_id,
            "message": "Connection revoked successfully",
        }
    except Exception as e:
        logger.error("Revoke connection failed: %s", e)
        raise HTTPException(status_code=400, detail="Failed to revoke connection")


# Export routers
__all__ = [
    "connectors_router",
    "marketplace_router",
    "oauth_router",
    "versioning_router",
]
