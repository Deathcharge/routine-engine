"""
Workflow Templates Marketplace for Helix Spirals.

Provides a comprehensive marketplace for workflow templates including
discovery, publishing, versioning, ratings, and installation.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


class TemplateCategory(Enum):
    """Workflow template categories."""

    MARKETING = "marketing"
    SALES = "sales"
    CUSTOMER_SUPPORT = "customer_support"
    HR = "hr"
    FINANCE = "finance"
    OPERATIONS = "operations"
    DEVELOPMENT = "development"
    DATA = "data"
    AI_ML = "ai_ml"
    SOCIAL_MEDIA = "social_media"
    E_COMMERCE = "e_commerce"
    PRODUCTIVITY = "productivity"
    COMMUNICATION = "communication"
    ANALYTICS = "analytics"
    SECURITY = "security"
    CUSTOM = "custom"


class TemplateStatus(Enum):
    """Template publication status."""

    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class TemplateTier(Enum):
    """Template pricing tier."""

    FREE = "free"
    STARTER = "starter"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"


@dataclass
class TemplateAuthor:
    """Template author information."""

    id: str
    name: str
    email: str
    organization: str | None = None
    verified: bool = False
    avatar_url: str | None = None
    bio: str | None = None
    website: str | None = None
    templates_count: int = 0
    total_installs: int = 0


@dataclass
class TemplateVersion:
    """Template version information."""

    version: str
    changelog: str
    created_at: datetime
    workflow_definition: dict[str, Any]
    min_helix_version: str = "1.0.0"
    breaking_changes: bool = False
    deprecated: bool = False


@dataclass
class TemplateRating:
    """Template rating."""

    user_id: str
    rating: int  # 1-5
    review: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    helpful_count: int = 0


@dataclass
class TemplateStats:
    """Template statistics."""

    installs: int = 0
    active_users: int = 0
    executions: int = 0
    success_rate: float = 0.0
    avg_execution_time: float = 0.0
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class WorkflowTemplate:
    """Complete workflow template."""

    id: str
    name: str
    slug: str
    description: str
    category: TemplateCategory
    status: TemplateStatus
    tier: TemplateTier
    author: TemplateAuthor

    # Content
    workflow_definition: dict[str, Any]
    versions: list[TemplateVersion] = field(default_factory=list)
    current_version: str = "1.0.0"

    # Metadata
    tags: list[str] = field(default_factory=list)
    integrations: list[str] = field(default_factory=list)
    prerequisites: list[str] = field(default_factory=list)

    # Media
    icon_url: str | None = None
    screenshots: list[str] = field(default_factory=list)
    video_url: str | None = None
    documentation_url: str | None = None

    # Stats
    stats: TemplateStats = field(default_factory=TemplateStats)
    ratings: list[TemplateRating] = field(default_factory=list)
    avg_rating: float = 0.0

    # Timestamps
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    published_at: datetime | None = None

    # Pricing
    price: float = 0.0
    currency: str = "USD"

    # Configuration
    config_schema: dict[str, Any] = field(default_factory=dict)
    default_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class InstalledTemplate:
    """Installed template instance."""

    id: str
    template_id: str
    user_id: str
    organization_id: str | None
    version: str
    config: dict[str, Any]
    workflow_id: str
    installed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_executed: datetime | None = None
    execution_count: int = 0
    enabled: bool = True


class WorkflowTemplateMarketplace:
    """
    Workflow Templates Marketplace for Helix Spirals.

    Features:
    - Template discovery and search
    - Template publishing and versioning
    - Ratings and reviews
    - Installation and configuration
    - Usage analytics
    - Featured and trending templates
    """

    def __init__(self, storage_backend=None):
        self.storage = storage_backend or InMemoryStorage()
        self._cache: dict[str, Any] = {}
        self._cache_ttl = 300  # 5 minutes

    # ==================== Template Discovery ====================

    async def search_templates(
        self,
        query: str | None = None,
        category: TemplateCategory | None = None,
        tags: list[str] | None = None,
        integrations: list[str] | None = None,
        tier: TemplateTier | None = None,
        min_rating: float | None = None,
        sort_by: str = "popularity",
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        """Search and filter templates."""
        templates = await self.storage.get_all_templates()

        # Filter by status (only published)
        templates = [t for t in templates if t.status == TemplateStatus.PUBLISHED]

        # Apply filters
        if query:
            query_lower = query.lower()
            templates = [
                t
                for t in templates
                if query_lower in t.name.lower()
                or query_lower in t.description.lower()
                or any(query_lower in tag.lower() for tag in t.tags)
            ]

        if category:
            templates = [t for t in templates if t.category == category]

        if tags:
            templates = [t for t in templates if any(tag in t.tags for tag in tags)]

        if integrations:
            templates = [t for t in templates if any(i in t.integrations for i in integrations)]

        if tier:
            templates = [t for t in templates if t.tier == tier]

        if min_rating:
            templates = [t for t in templates if t.avg_rating >= min_rating]

        # Sort
        if sort_by == "popularity":
            templates.sort(key=lambda t: t.stats.installs, reverse=True)
        elif sort_by == "rating":
            templates.sort(key=lambda t: t.avg_rating, reverse=True)
        elif sort_by == "newest":
            templates.sort(key=lambda t: t.published_at or t.created_at, reverse=True)
        elif sort_by == "name":
            templates.sort(key=lambda t: t.name)

        # Paginate
        total = len(templates)
        start = (page - 1) * page_size
        end = start + page_size
        templates = templates[start:end]

        return {
            "templates": templates,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }

    async def get_template(self, template_id: str) -> WorkflowTemplate | None:
        """Get a template by ID."""
        return await self.storage.get_template(template_id)

    async def get_template_by_slug(self, slug: str) -> WorkflowTemplate | None:
        """Get a template by slug."""
        return await self.storage.get_template_by_slug(slug)

    async def get_featured_templates(self, limit: int = 10) -> list[WorkflowTemplate]:
        """Get featured templates."""
        templates = await self.storage.get_all_templates()
        templates = [t for t in templates if t.status == TemplateStatus.PUBLISHED]

        # Feature templates with high ratings and installs
        templates.sort(
            key=lambda t: (t.avg_rating * 0.4 + min(t.stats.installs / 1000, 5) * 0.6),
            reverse=True,
        )

        return templates[:limit]

    async def get_trending_templates(self, days: int = 7, limit: int = 10) -> list[WorkflowTemplate]:
        """Get trending templates based on recent activity."""
        templates = await self.storage.get_all_templates()
        templates = [t for t in templates if t.status == TemplateStatus.PUBLISHED]

        # Calculate trending score based on recent installs and executions
        # In production, this would use time-series data
        templates.sort(key=lambda t: t.stats.installs + t.stats.executions * 0.1, reverse=True)

        return templates[:limit]

    async def get_templates_by_category(self, category: TemplateCategory, limit: int = 20) -> list[WorkflowTemplate]:
        """Get templates by category."""
        result = await self.search_templates(category=category, page_size=limit)
        return result["templates"]

    async def get_templates_by_integration(self, integration: str, limit: int = 20) -> list[WorkflowTemplate]:
        """Get templates that use a specific integration."""
        result = await self.search_templates(integrations=[integration], page_size=limit)
        return result["templates"]

    async def get_similar_templates(self, template_id: str, limit: int = 5) -> list[WorkflowTemplate]:
        """Get similar templates based on category and tags."""
        template = await self.get_template(template_id)
        if not template:
            return []

        templates = await self.storage.get_all_templates()
        templates = [t for t in templates if t.status == TemplateStatus.PUBLISHED and t.id != template_id]

        # Score similarity
        def similarity_score(t: WorkflowTemplate) -> float:
            score = 0.0
            if t.category == template.category:
                score += 3.0
            score += len(set(t.tags) & set(template.tags)) * 0.5
            score += len(set(t.integrations) & set(template.integrations)) * 0.3
            return score

        templates.sort(key=similarity_score, reverse=True)
        return templates[:limit]

    # ==================== Template Publishing ====================

    async def create_template(
        self,
        author: TemplateAuthor,
        name: str,
        description: str,
        category: TemplateCategory,
        workflow_definition: dict[str, Any],
        tags: list[str] = None,
        integrations: list[str] = None,
        tier: TemplateTier = TemplateTier.FREE,
        config_schema: dict[str, Any] = None,
        default_config: dict[str, Any] = None,
    ) -> WorkflowTemplate:
        """Create a new template."""
        template_id = str(uuid4())
        slug = self._generate_slug(name)

        template = WorkflowTemplate(
            id=template_id,
            name=name,
            slug=slug,
            description=description,
            category=category,
            status=TemplateStatus.DRAFT,
            tier=tier,
            author=author,
            workflow_definition=workflow_definition,
            tags=tags or [],
            integrations=integrations or self._detect_integrations(workflow_definition),
            config_schema=config_schema or {},
            default_config=default_config or {},
            versions=[
                TemplateVersion(
                    version="1.0.0",
                    changelog="Initial release",
                    created_at=datetime.now(UTC),
                    workflow_definition=workflow_definition,
                )
            ],
        )

        await self.storage.save_template(template)
        return template

    async def update_template(self, template_id: str, updates: dict[str, Any]) -> WorkflowTemplate | None:
        """Update template metadata."""
        template = await self.get_template(template_id)
        if not template:
            return None

        # Apply updates
        for key, value in updates.items():
            if hasattr(template, key) and key not in ["id", "author", "versions"]:
                setattr(template, key, value)

        template.updated_at = datetime.now(UTC)
        await self.storage.save_template(template)
        return template

    async def publish_version(
        self,
        template_id: str,
        version: str,
        changelog: str,
        workflow_definition: dict[str, Any],
        breaking_changes: bool = False,
    ) -> WorkflowTemplate | None:
        """Publish a new version of a template."""
        template = await self.get_template(template_id)
        if not template:
            return None

        new_version = TemplateVersion(
            version=version,
            changelog=changelog,
            created_at=datetime.now(UTC),
            workflow_definition=workflow_definition,
            breaking_changes=breaking_changes,
        )

        template.versions.append(new_version)
        template.current_version = version
        template.workflow_definition = workflow_definition
        template.updated_at = datetime.now(UTC)

        # Auto-detect integrations
        template.integrations = self._detect_integrations(workflow_definition)

        await self.storage.save_template(template)
        return template

    async def submit_for_review(self, template_id: str) -> WorkflowTemplate | None:
        """Submit a template for review."""
        template = await self.get_template(template_id)
        if not template or template.status != TemplateStatus.DRAFT:
            return None

        template.status = TemplateStatus.PENDING_REVIEW
        template.updated_at = datetime.now(UTC)

        await self.storage.save_template(template)
        return template

    async def approve_template(self, template_id: str, reviewer_id: str) -> WorkflowTemplate | None:
        """Approve a template for publication."""
        template = await self.get_template(template_id)
        if not template or template.status != TemplateStatus.PENDING_REVIEW:
            return None

        template.status = TemplateStatus.PUBLISHED
        template.published_at = datetime.now(UTC)
        template.updated_at = datetime.now(UTC)

        await self.storage.save_template(template)
        return template

    async def deprecate_template(self, template_id: str, reason: str) -> WorkflowTemplate | None:
        """Deprecate a template."""
        template = await self.get_template(template_id)
        if not template:
            return None

        template.status = TemplateStatus.DEPRECATED
        template.updated_at = datetime.now(UTC)

        await self.storage.save_template(template)
        return template

    # ==================== Template Installation ====================

    async def install_template(
        self,
        template_id: str,
        user_id: str,
        organization_id: str | None = None,
        config: dict[str, Any] | None = None,
        version: str | None = None,
    ) -> InstalledTemplate:
        """Install a template for a user."""
        template = await self.get_template(template_id)
        if not template:
            raise ValueError(f"Template not found: {template_id}")

        if template.status != TemplateStatus.PUBLISHED:
            raise ValueError("Template is not published")

        # Use specified version or current
        install_version = version or template.current_version

        # Merge config with defaults
        final_config = {**template.default_config, **(config or {})}

        # Create workflow from template
        workflow_id = await self._create_workflow_from_template(template, user_id, organization_id, final_config)

        installed = InstalledTemplate(
            id=str(uuid4()),
            template_id=template_id,
            user_id=user_id,
            organization_id=organization_id,
            version=install_version,
            config=final_config,
            workflow_id=workflow_id,
        )

        await self.storage.save_installed_template(installed)

        # Update stats
        template.stats.installs += 1
        template.stats.active_users += 1
        template.stats.last_updated = datetime.now(UTC)
        await self.storage.save_template(template)

        return installed

    async def uninstall_template(self, installed_id: str, user_id: str) -> bool:
        """Uninstall a template."""
        installed = await self.storage.get_installed_template(installed_id)
        if not installed or installed.user_id != user_id:
            return False

        # Delete the workflow
        await self._delete_workflow(installed.workflow_id)

        # Remove installation record
        await self.storage.delete_installed_template(installed_id)

        # Update stats
        template = await self.get_template(installed.template_id)
        if template:
            template.stats.active_users = max(0, template.stats.active_users - 1)
            await self.storage.save_template(template)

        return True

    async def get_user_installed_templates(
        self, user_id: str, organization_id: str | None = None
    ) -> list[InstalledTemplate]:
        """Get templates installed by a user."""
        return await self.storage.get_user_installed_templates(user_id, organization_id)

    async def upgrade_installed_template(
        self, installed_id: str, user_id: str, target_version: str | None = None
    ) -> InstalledTemplate | None:
        """Upgrade an installed template to a newer version."""
        installed = await self.storage.get_installed_template(installed_id)
        if not installed or installed.user_id != user_id:
            return None

        template = await self.get_template(installed.template_id)
        if not template:
            return None

        new_version = target_version or template.current_version
        if new_version == installed.version:
            return installed  # Already at target version

        # Find the version
        version_def = next((v for v in template.versions if v.version == new_version), None)
        if not version_def:
            raise ValueError(f"Version not found: {new_version}")

        # Update the workflow
        await self._update_workflow_from_template(
            installed.workflow_id, version_def.workflow_definition, installed.config
        )

        installed.version = new_version
        await self.storage.save_installed_template(installed)

        return installed

    # ==================== Ratings and Reviews ====================

    async def rate_template(
        self, template_id: str, user_id: str, rating: int, review: str | None = None
    ) -> WorkflowTemplate | None:
        """Rate and review a template."""
        if not 1 <= rating <= 5:
            raise ValueError("Rating must be between 1 and 5")

        template = await self.get_template(template_id)
        if not template:
            return None

        # Check if user already rated
        existing_rating = next((r for r in template.ratings if r.user_id == user_id), None)

        if existing_rating:
            existing_rating.rating = rating
            existing_rating.review = review
            existing_rating.created_at = datetime.now(UTC)
        else:
            template.ratings.append(TemplateRating(user_id=user_id, rating=rating, review=review))

        # Recalculate average
        if template.ratings:
            template.avg_rating = sum(r.rating for r in template.ratings) / len(template.ratings)

        template.updated_at = datetime.now(UTC)
        await self.storage.save_template(template)

        return template

    async def get_template_reviews(self, template_id: str, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        """Get reviews for a template."""
        template = await self.get_template(template_id)
        if not template:
            return {"reviews": [], "total": 0}

        reviews = sorted(template.ratings, key=lambda r: r.created_at, reverse=True)

        total = len(reviews)
        start = (page - 1) * page_size
        end = start + page_size

        return {
            "reviews": reviews[start:end],
            "total": total,
            "page": page,
            "page_size": page_size,
            "avg_rating": template.avg_rating,
        }

    # ==================== Analytics ====================

    async def record_execution(self, installed_id: str, success: bool, execution_time: float):
        """Record a template execution for analytics."""
        installed = await self.storage.get_installed_template(installed_id)
        if not installed:
            return

        installed.execution_count += 1
        installed.last_executed = datetime.now(UTC)
        await self.storage.save_installed_template(installed)

        # Update template stats
        template = await self.get_template(installed.template_id)
        if template:
            template.stats.executions += 1

            # Update success rate (rolling average)
            old_rate = template.stats.success_rate
            old_count = template.stats.executions - 1
            if old_count > 0:
                template.stats.success_rate = (
                    old_rate * old_count + (1.0 if success else 0.0)
                ) / template.stats.executions
            else:
                template.stats.success_rate = 1.0 if success else 0.0

            # Update avg execution time (rolling average)
            old_time = template.stats.avg_execution_time
            if old_count > 0:
                template.stats.avg_execution_time = (old_time * old_count + execution_time) / template.stats.executions
            else:
                template.stats.avg_execution_time = execution_time

            template.stats.last_updated = datetime.now(UTC)
            await self.storage.save_template(template)

    async def get_author_stats(self, author_id: str) -> dict[str, Any]:
        """Get statistics for a template author."""
        templates = await self.storage.get_templates_by_author(author_id)

        total_installs = sum(t.stats.installs for t in templates)
        total_executions = sum(t.stats.executions for t in templates)
        avg_rating = (
            sum(t.avg_rating * len(t.ratings) for t in templates) / sum(len(t.ratings) for t in templates)
            if any(t.ratings for t in templates)
            else 0.0
        )

        return {
            "author_id": author_id,
            "templates_count": len(templates),
            "published_count": len([t for t in templates if t.status == TemplateStatus.PUBLISHED]),
            "total_installs": total_installs,
            "total_executions": total_executions,
            "avg_rating": avg_rating,
            "total_reviews": sum(len(t.ratings) for t in templates),
        }

    # ==================== Helper Methods ====================

    def _generate_slug(self, name: str) -> str:
        """Generate a URL-friendly slug from name."""
        import re

        slug = name.lower()
        slug = re.sub(r"[^a-z0-9]+", "-", slug)
        slug = slug.strip("-")
        return f"{slug}-{uuid4().hex[:8]}"

    def _detect_integrations(self, workflow_definition: dict[str, Any]) -> list[str]:
        """Detect integrations used in a workflow."""
        integrations = set()

        # Known integration node types
        integration_map = {
            "mailchimp": "Mailchimp",
            "calendly": "Calendly",
            "airtable": "Airtable",
            "slack": "Slack",
            "discord": "Discord",
            "twilio": "Twilio",
            "sendgrid": "SendGrid",
            "stripe": "Stripe",
            "shopify": "Shopify",
            "hubspot": "HubSpot",
            "zendesk": "Zendesk",
            "github": "GitHub",
            "jira": "Jira",
            "notion": "Notion",
            "google_sheets": "Google Sheets",
            "openai": "OpenAI",
            "anthropic": "Anthropic",
            "postgresql": "PostgreSQL",
            "mysql": "MySQL",
            "mongodb": "MongoDB",
            "redis": "Redis",
            "s3": "AWS S3",
            "gcs": "Google Cloud Storage",
        }

        nodes = workflow_definition.get("nodes", [])
        for node in nodes:
            node_type = node.get("type", "").lower()
            for key, name in integration_map.items():
                if key in node_type:
                    integrations.add(name)

        return list(integrations)

    async def _create_workflow_from_template(
        self,
        template: WorkflowTemplate,
        user_id: str,
        organization_id: str | None,
        config: dict[str, Any],
    ) -> str:
        """Create a workflow instance from a template via the durable execution engine."""
        from apps.backend.services.durable_execution import DurableExecutionEngine

        engine = DurableExecutionEngine()

        # Build workflow definition from template + user config
        steps = [
            {"name": step.name, "type": step.type, "config": {**step.config, **config}}
            for step in template.workflow_definition.steps
        ]

        wf = await engine.create_workflow(
            name=f"{template.name} (user:{user_id[:8]})",
            steps=steps,
            input_data=config,
            created_by=user_id,
            tags=[f"template:{template.id}"],
        )
        workflow_id = wf.id

        logger.info("Created workflow %s from template %s for user %s", workflow_id, template.id, user_id)
        return workflow_id

    async def _update_workflow_from_template(
        self,
        workflow_id: str,
        workflow_definition: dict[str, Any],
        config: dict[str, Any],
    ):
        """Update a workflow with new template version."""
        from apps.backend.services.durable_execution import DurableExecutionEngine

        engine = DurableExecutionEngine()
        wf = await engine._load_workflow(workflow_id)
        if wf:
            wf.metadata.update({"updated_definition": workflow_definition, "updated_config": config})
            await engine._persist_workflow(wf)
            logger.info("Updated workflow %s with new template version", workflow_id)
        else:
            logger.warning("Workflow %s not found for update", workflow_id)

    async def _delete_workflow(self, workflow_id: str):
        """Delete a workflow from the execution engine."""
        from apps.backend.services.durable_execution import DurableExecutionEngine

        engine = DurableExecutionEngine()
        if workflow_id in engine._workflows:
            del engine._workflows[workflow_id]
        # Also remove from Redis
        try:
            from apps.backend.core.redis_client import get_redis

            r = await get_redis()
            if r:
                await r.delete(f"{engine._REDIS_KEY_PREFIX}{workflow_id}")
        except Exception as e:
            logger.warning("Redis delete failed for workflow %s: %s", workflow_id, e)
        logger.info("Deleted workflow %s", workflow_id)


class InMemoryStorage:
    """In-memory storage for development/testing."""

    def __init__(self):
        self._templates: dict[str, WorkflowTemplate] = {}
        self._installed: dict[str, InstalledTemplate] = {}

    async def get_all_templates(self) -> list[WorkflowTemplate]:
        return list(self._templates.values())

    async def get_template(self, template_id: str) -> WorkflowTemplate | None:
        return self._templates.get(template_id)

    async def get_template_by_slug(self, slug: str) -> WorkflowTemplate | None:
        for template in self._templates.values():
            if template.slug == slug:
                return template
        return None

    async def save_template(self, template: WorkflowTemplate):
        self._templates[template.id] = template

    async def delete_template(self, template_id: str):
        self._templates.pop(template_id, None)

    async def get_templates_by_author(self, author_id: str) -> list[WorkflowTemplate]:
        return [t for t in self._templates.values() if t.author.id == author_id]

    async def get_installed_template(self, installed_id: str) -> InstalledTemplate | None:
        return self._installed.get(installed_id)

    async def save_installed_template(self, installed: InstalledTemplate):
        self._installed[installed.id] = installed

    async def delete_installed_template(self, installed_id: str):
        self._installed.pop(installed_id, None)

    async def get_user_installed_templates(
        self, user_id: str, organization_id: str | None = None
    ) -> list[InstalledTemplate]:
        results = [i for i in self._installed.values() if i.user_id == user_id]
        if organization_id:
            results = [i for i in results if i.organization_id == organization_id]
        return results


# ==================== Pre-built Template Definitions ====================

BUILTIN_TEMPLATES = [
    {
        "name": "Lead Nurturing Automation",
        "description": "Automatically nurture leads with personalized email sequences based on their behavior and engagement.",
        "category": TemplateCategory.MARKETING,
        "tier": TemplateTier.FREE,
        "tags": ["email", "leads", "automation", "nurturing"],
        "workflow_definition": {
            "nodes": [
                {
                    "id": "trigger",
                    "type": "webhook",
                    "config": {"event": "lead_created"},
                },
                {"id": "enrich", "type": "data_enrichment", "config": {}},
                {
                    "id": "segment",
                    "type": "condition",
                    "config": {"field": "score", "operator": ">=", "value": 50},
                },
                {
                    "id": "email_hot",
                    "type": "sendgrid",
                    "config": {"template": "hot_lead"},
                },
                {
                    "id": "email_warm",
                    "type": "sendgrid",
                    "config": {"template": "warm_lead"},
                },
                {
                    "id": "crm_update",
                    "type": "hubspot",
                    "config": {"action": "update_contact"},
                },
            ],
            "edges": [
                {"from": "trigger", "to": "enrich"},
                {"from": "enrich", "to": "segment"},
                {"from": "segment", "to": "email_hot", "condition": "true"},
                {"from": "segment", "to": "email_warm", "condition": "false"},
                {"from": "email_hot", "to": "crm_update"},
                {"from": "email_warm", "to": "crm_update"},
            ],
        },
    },
    {
        "name": "Customer Support Ticket Router",
        "description": "Intelligently route support tickets to the right team based on content analysis and priority.",
        "category": TemplateCategory.CUSTOMER_SUPPORT,
        "tier": TemplateTier.STARTER,
        "tags": ["support", "tickets", "routing", "ai"],
        "workflow_definition": {
            "nodes": [
                {
                    "id": "trigger",
                    "type": "zendesk_webhook",
                    "config": {"event": "ticket_created"},
                },
                {
                    "id": "analyze",
                    "type": "openai",
                    "config": {"model": "gpt-4", "task": "classify"},
                },
                {"id": "priority", "type": "condition", "config": {"field": "urgency"}},
                {
                    "id": "route_urgent",
                    "type": "zendesk",
                    "config": {"action": "assign", "group": "urgent"},
                },
                {
                    "id": "route_normal",
                    "type": "zendesk",
                    "config": {"action": "assign", "group": "general"},
                },
                {"id": "notify", "type": "slack", "config": {"channel": "#support"}},
            ],
            "edges": [
                {"from": "trigger", "to": "analyze"},
                {"from": "analyze", "to": "priority"},
                {"from": "priority", "to": "route_urgent", "condition": "high"},
                {"from": "priority", "to": "route_normal", "condition": "default"},
                {"from": "route_urgent", "to": "notify"},
            ],
        },
    },
    {
        "name": "E-commerce Order Processing",
        "description": "Complete order processing workflow with inventory check, payment, fulfillment, and notifications.",
        "category": TemplateCategory.E_COMMERCE,
        "tier": TemplateTier.PROFESSIONAL,
        "tags": ["orders", "ecommerce", "fulfillment", "payments"],
        "workflow_definition": {
            "nodes": [
                {
                    "id": "trigger",
                    "type": "shopify_webhook",
                    "config": {"event": "order_created"},
                },
                {
                    "id": "inventory",
                    "type": "shopify",
                    "config": {"action": "check_inventory"},
                },
                {
                    "id": "payment",
                    "type": "stripe",
                    "config": {"action": "capture_payment"},
                },
                {
                    "id": "fulfill",
                    "type": "shopify",
                    "config": {"action": "create_fulfillment"},
                },
                {
                    "id": "email",
                    "type": "sendgrid",
                    "config": {"template": "order_confirmation"},
                },
                {
                    "id": "analytics",
                    "type": "airtable",
                    "config": {"action": "create_record"},
                },
            ],
            "edges": [
                {"from": "trigger", "to": "inventory"},
                {"from": "inventory", "to": "payment"},
                {"from": "payment", "to": "fulfill"},
                {"from": "fulfill", "to": "email"},
                {"from": "email", "to": "analytics"},
            ],
        },
    },
    {
        "name": "Meeting Scheduler with AI Summary",
        "description": "Automate meeting scheduling, send reminders, and generate AI-powered meeting summaries.",
        "category": TemplateCategory.PRODUCTIVITY,
        "tier": TemplateTier.FREE,
        "tags": ["meetings", "calendar", "ai", "summaries"],
        "workflow_definition": {
            "nodes": [
                {
                    "id": "trigger",
                    "type": "calendly_webhook",
                    "config": {"event": "invitee.created"},
                },
                {
                    "id": "calendar",
                    "type": "google_calendar",
                    "config": {"action": "create_event"},
                },
                {
                    "id": "reminder",
                    "type": "delay",
                    "config": {"duration": "1h", "before": "event"},
                },
                {"id": "notify", "type": "slack", "config": {"action": "send_message"}},
                {
                    "id": "post_meeting",
                    "type": "calendly_webhook",
                    "config": {"event": "meeting.ended"},
                },
                {
                    "id": "summarize",
                    "type": "openai",
                    "config": {"model": "gpt-4", "task": "summarize"},
                },
                {"id": "save", "type": "notion", "config": {"action": "create_page"}},
            ],
            "edges": [
                {"from": "trigger", "to": "calendar"},
                {"from": "calendar", "to": "reminder"},
                {"from": "reminder", "to": "notify"},
                {"from": "post_meeting", "to": "summarize"},
                {"from": "summarize", "to": "save"},
            ],
        },
    },
    {
        "name": "Social Media Content Pipeline",
        "description": "Generate, schedule, and analyze social media content across multiple platforms.",
        "category": TemplateCategory.SOCIAL_MEDIA,
        "tier": TemplateTier.STARTER,
        "tags": ["social", "content", "scheduling", "analytics"],
        "workflow_definition": {
            "nodes": [
                {"id": "trigger", "type": "schedule", "config": {"cron": "0 9 * * 1"}},
                {
                    "id": "generate",
                    "type": "openai",
                    "config": {"model": "gpt-4", "task": "generate_content"},
                },
                {"id": "image", "type": "dalle", "config": {"size": "1024x1024"}},
                {
                    "id": "review",
                    "type": "approval",
                    "config": {"approvers": ["marketing"]},
                },
                {"id": "post_twitter", "type": "twitter", "config": {"action": "post"}},
                {
                    "id": "post_linkedin",
                    "type": "linkedin",
                    "config": {"action": "post"},
                },
                {
                    "id": "track",
                    "type": "airtable",
                    "config": {"action": "create_record"},
                },
            ],
            "edges": [
                {"from": "trigger", "to": "generate"},
                {"from": "generate", "to": "image"},
                {"from": "image", "to": "review"},
                {"from": "review", "to": "post_twitter", "condition": "approved"},
                {"from": "review", "to": "post_linkedin", "condition": "approved"},
                {"from": "post_twitter", "to": "track"},
                {"from": "post_linkedin", "to": "track"},
            ],
        },
    },
]


async def seed_builtin_templates(marketplace: WorkflowTemplateMarketplace):
    """Seed the marketplace with built-in templates."""
    system_author = TemplateAuthor(
        id="system",
        name="Helix Team",
        email="templates@helixspirals.work",
        organization="Helix",
        verified=True,
    )

    for template_def in BUILTIN_TEMPLATES:
        template = await marketplace.create_template(
            author=system_author,
            name=template_def["name"],
            description=template_def["description"],
            category=template_def["category"],
            workflow_definition=template_def["workflow_definition"],
            tags=template_def["tags"],
            tier=template_def["tier"],
        )

        # Auto-publish built-in templates
        await marketplace.submit_for_review(template.id)
        await marketplace.approve_template(template.id, "system")

        logger.info("Seeded template: %s", template.name)
