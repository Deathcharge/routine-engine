"""
Workflow Versioning System for Helix Spirals.

Provides comprehensive version control for workflows including
branching, merging, rollback, diff comparison, and audit trails.
"""

import hashlib
import json
import logging
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


class VersionStatus(Enum):
    """Version status."""

    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"


class ChangeType(Enum):
    """Type of change in a version."""

    NODE_ADDED = "node_added"
    NODE_REMOVED = "node_removed"
    NODE_MODIFIED = "node_modified"
    EDGE_ADDED = "edge_added"
    EDGE_REMOVED = "edge_removed"
    EDGE_MODIFIED = "edge_modified"
    CONFIG_CHANGED = "config_changed"
    METADATA_CHANGED = "metadata_changed"


class MergeStrategy(Enum):
    """Strategy for merging versions."""

    OURS = "ours"  # Keep current version's changes
    THEIRS = "theirs"  # Take incoming version's changes
    MANUAL = "manual"  # Require manual resolution


@dataclass
class VersionChange:
    """Represents a single change in a version."""

    change_type: ChangeType
    path: str  # JSON path to the changed element
    old_value: Any | None = None
    new_value: Any | None = None
    description: str | None = None


@dataclass
class WorkflowVersion:
    """Represents a workflow version."""

    id: str
    workflow_id: str
    version_number: str  # Semantic versioning: major.minor.patch
    parent_version_id: str | None

    # Content
    definition: dict[str, Any]
    definition_hash: str

    # Metadata
    name: str
    description: str
    status: VersionStatus

    # Author info
    created_by: str

    # Changes
    changes: list[VersionChange] = field(default_factory=list)
    change_summary: str = ""

    # Branch info
    branch: str = "main"
    is_head: bool = False

    # Tags
    tags: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class VersionBranch:
    """Represents a version branch."""

    id: str
    workflow_id: str
    name: str
    description: str
    head_version_id: str
    base_version_id: str
    created_by: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    merged: bool = False
    merged_by: str | None = None
    merged_at: datetime | None = None


@dataclass
class MergeConflict:
    """Represents a merge conflict."""

    path: str
    ours_value: Any
    theirs_value: Any
    base_value: Any | None
    resolved: bool = False
    resolution: Any | None = None


@dataclass
class MergeResult:
    """Result of a merge operation."""

    success: bool
    merged_version_id: str | None
    conflicts: list[MergeConflict] = field(default_factory=list)
    changes_applied: list[VersionChange] = field(default_factory=list)
    message: str = ""


@dataclass
class VersionDiff:
    """Difference between two versions."""

    source_version_id: str
    target_version_id: str
    changes: list[VersionChange]
    additions: int = 0
    deletions: int = 0
    modifications: int = 0


@dataclass
class AuditLogEntry:
    """Audit log entry for version operations."""

    id: str
    workflow_id: str
    version_id: str | None
    action: str
    actor: str
    timestamp: datetime
    details: dict[str, Any] = field(default_factory=dict)
    ip_address: str | None = None
    user_agent: str | None = None


class WorkflowVersioningSystem:
    """
    Comprehensive workflow versioning system.

    Features:
    - Semantic versioning
    - Branching and merging
    - Diff comparison
    - Rollback capability
    - Audit trail
    - Tag management
    - Conflict resolution
    """

    def __init__(self, storage_backend=None):
        self.storage = storage_backend or InMemoryVersionStorage()

    # ==================== Version Management ====================

    async def create_version(
        self,
        workflow_id: str,
        definition: dict[str, Any],
        name: str,
        description: str,
        created_by: str,
        parent_version_id: str | None = None,
        branch: str = "main",
        auto_increment: bool = True,
    ) -> WorkflowVersion:
        """Create a new version of a workflow."""
        # Calculate definition hash
        definition_hash = self._hash_definition(definition)

        # Determine version number
        if auto_increment and parent_version_id:
            parent = await self.get_version(parent_version_id)
            if parent:
                version_number = self._increment_version(parent.version_number, "patch")
            else:
                version_number = "1.0.0"
        else:
            version_number = "1.0.0"

        # Calculate changes from parent
        changes = []
        change_summary = "Initial version"
        if parent_version_id:
            parent = await self.get_version(parent_version_id)
            if parent:
                diff = self._calculate_diff(parent.definition, definition)
                changes = diff.changes
                change_summary = self._generate_change_summary(changes)

        version = WorkflowVersion(
            id=str(uuid4()),
            workflow_id=workflow_id,
            version_number=version_number,
            parent_version_id=parent_version_id,
            definition=definition,
            definition_hash=definition_hash,
            name=name,
            description=description,
            status=VersionStatus.DRAFT,
            changes=changes,
            change_summary=change_summary,
            created_by=created_by,
            branch=branch,
        )

        await self.storage.save_version(version)

        # Log audit entry
        await self._log_audit(
            workflow_id=workflow_id,
            version_id=version.id,
            action="version_created",
            actor=created_by,
            details={
                "version_number": version_number,
                "branch": branch,
                "parent_version_id": parent_version_id,
            },
        )

        return version

    async def get_version(self, version_id: str) -> WorkflowVersion | None:
        """Get a specific version."""
        return await self.storage.get_version(version_id)

    async def get_version_by_number(
        self, workflow_id: str, version_number: str, branch: str = "main"
    ) -> WorkflowVersion | None:
        """Get a version by its number."""
        versions = await self.storage.get_workflow_versions(workflow_id)
        for v in versions:
            if v.version_number == version_number and v.branch == branch:
                return v
        return None

    async def get_workflow_versions(
        self,
        workflow_id: str,
        branch: str | None = None,
        status: VersionStatus | None = None,
        limit: int = 100,
    ) -> list[WorkflowVersion]:
        """Get all versions of a workflow."""
        versions = await self.storage.get_workflow_versions(workflow_id)

        if branch:
            versions = [v for v in versions if v.branch == branch]

        if status:
            versions = [v for v in versions if v.status == status]

        # Sort by creation time, newest first
        versions.sort(key=lambda v: v.created_at, reverse=True)

        return versions[:limit]

    async def get_head_version(self, workflow_id: str, branch: str = "main") -> WorkflowVersion | None:
        """Get the head (latest active) version of a branch."""
        versions = await self.get_workflow_versions(workflow_id, branch=branch, status=VersionStatus.ACTIVE)

        if versions:
            # Find the one marked as head, or the latest
            head = next((v for v in versions if v.is_head), None)
            return head or versions[0]

        return None

    async def activate_version(self, version_id: str, activated_by: str) -> WorkflowVersion | None:
        """Activate a version (make it the current active version)."""
        version = await self.get_version(version_id)
        if not version:
            return None

        # Deactivate current head
        current_head = await self.get_head_version(version.workflow_id, version.branch)
        if current_head:
            current_head.is_head = False
            await self.storage.save_version(current_head)

        # Activate new version
        version.status = VersionStatus.ACTIVE
        version.is_head = True
        await self.storage.save_version(version)

        await self._log_audit(
            workflow_id=version.workflow_id,
            version_id=version_id,
            action="version_activated",
            actor=activated_by,
            details={"version_number": version.version_number},
        )

        return version

    async def archive_version(self, version_id: str, archived_by: str) -> WorkflowVersion | None:
        """Archive a version."""
        version = await self.get_version(version_id)
        if not version:
            return None

        version.status = VersionStatus.ARCHIVED
        version.is_head = False
        await self.storage.save_version(version)

        await self._log_audit(
            workflow_id=version.workflow_id,
            version_id=version_id,
            action="version_archived",
            actor=archived_by,
        )

        return version

    # ==================== Branching ====================

    async def create_branch(
        self,
        workflow_id: str,
        branch_name: str,
        base_version_id: str,
        description: str,
        created_by: str,
    ) -> VersionBranch:
        """Create a new branch from a version."""
        base_version = await self.get_version(base_version_id)
        if not base_version:
            raise ValueError(f"Base version not found: {base_version_id}")

        # Check if branch already exists
        existing = await self.storage.get_branch(workflow_id, branch_name)
        if existing:
            raise ValueError(f"Branch already exists: {branch_name}")

        # Create branch
        branch = VersionBranch(
            id=str(uuid4()),
            workflow_id=workflow_id,
            name=branch_name,
            description=description,
            head_version_id=base_version_id,
            base_version_id=base_version_id,
            created_by=created_by,
        )

        await self.storage.save_branch(branch)

        # Create initial version on branch
        branch_version = await self.create_version(
            workflow_id=workflow_id,
            definition=base_version.definition,
            name=f"Branch: {branch_name}",
            description=f"Initial version on branch {branch_name}",
            created_by=created_by,
            parent_version_id=base_version_id,
            branch=branch_name,
            auto_increment=False,
        )

        # Update branch head
        branch.head_version_id = branch_version.id
        await self.storage.save_branch(branch)

        await self._log_audit(
            workflow_id=workflow_id,
            version_id=branch_version.id,
            action="branch_created",
            actor=created_by,
            details={"branch_name": branch_name, "base_version_id": base_version_id},
        )

        return branch

    async def get_branches(self, workflow_id: str) -> list[VersionBranch]:
        """Get all branches for a workflow."""
        return await self.storage.get_workflow_branches(workflow_id)

    async def delete_branch(self, workflow_id: str, branch_name: str, deleted_by: str) -> bool:
        """Delete a branch (cannot delete main)."""
        if branch_name == "main":
            raise ValueError("Cannot delete main branch")

        branch = await self.storage.get_branch(workflow_id, branch_name)
        if not branch:
            return False

        await self.storage.delete_branch(branch.id)

        await self._log_audit(
            workflow_id=workflow_id,
            version_id=None,
            action="branch_deleted",
            actor=deleted_by,
            details={"branch_name": branch_name},
        )

        return True

    # ==================== Merging ====================

    async def merge_branches(
        self,
        workflow_id: str,
        source_branch: str,
        target_branch: str,
        merged_by: str,
        strategy: MergeStrategy = MergeStrategy.MANUAL,
        conflict_resolutions: dict[str, Any] | None = None,
    ) -> MergeResult:
        """Merge one branch into another."""
        # Get head versions of both branches
        source_head = await self.get_head_version(workflow_id, source_branch)
        target_head = await self.get_head_version(workflow_id, target_branch)

        if not source_head or not target_head:
            return MergeResult(
                success=False,
                merged_version_id=None,
                message="Source or target branch not found",
            )

        # Find common ancestor
        ancestor = await self._find_common_ancestor(source_head, target_head)

        # Calculate diffs
        source_diff = self._calculate_diff(ancestor.definition if ancestor else {}, source_head.definition)
        target_diff = self._calculate_diff(ancestor.definition if ancestor else {}, target_head.definition)

        # Detect conflicts
        conflicts = self._detect_conflicts(source_diff, target_diff)

        if conflicts and strategy == MergeStrategy.MANUAL:
            if not conflict_resolutions:
                return MergeResult(
                    success=False,
                    merged_version_id=None,
                    conflicts=conflicts,
                    message="Conflicts detected, manual resolution required",
                )

            # Apply resolutions
            for conflict in conflicts:
                if conflict.path in conflict_resolutions:
                    conflict.resolved = True
                    conflict.resolution = conflict_resolutions[conflict.path]

        # Check for unresolved conflicts
        unresolved = [c for c in conflicts if not c.resolved]
        if unresolved:
            return MergeResult(
                success=False,
                merged_version_id=None,
                conflicts=unresolved,
                message=f"{len(unresolved)} unresolved conflicts",
            )

        # Perform merge
        merged_definition = self._perform_merge(
            base=ancestor.definition if ancestor else {},
            ours=target_head.definition,
            theirs=source_head.definition,
            conflicts=conflicts,
            strategy=strategy,
        )

        # Create merged version
        merged_version = await self.create_version(
            workflow_id=workflow_id,
            definition=merged_definition,
            name=f"Merge {source_branch} into {target_branch}",
            description=f"Merged branch {source_branch} into {target_branch}",
            created_by=merged_by,
            parent_version_id=target_head.id,
            branch=target_branch,
        )

        # Activate merged version
        await self.activate_version(merged_version.id, merged_by)

        # Mark source branch as merged
        source_branch_obj = await self.storage.get_branch(workflow_id, source_branch)
        if source_branch_obj:
            source_branch_obj.merged = True
            source_branch_obj.merged_at = datetime.now(UTC)
            source_branch_obj.merged_by = merged_by
            await self.storage.save_branch(source_branch_obj)

        await self._log_audit(
            workflow_id=workflow_id,
            version_id=merged_version.id,
            action="branches_merged",
            actor=merged_by,
            details={
                "source_branch": source_branch,
                "target_branch": target_branch,
                "conflicts_resolved": len(conflicts),
            },
        )

        return MergeResult(
            success=True,
            merged_version_id=merged_version.id,
            conflicts=conflicts,
            changes_applied=source_diff.changes + target_diff.changes,
            message="Merge successful",
        )

    # ==================== Diff and Comparison ====================

    async def compare_versions(self, source_version_id: str, target_version_id: str) -> VersionDiff | None:
        """Compare two versions and return the diff."""
        source = await self.get_version(source_version_id)
        target = await self.get_version(target_version_id)

        if not source or not target:
            return None

        return self._calculate_diff(source.definition, target.definition)

    def _calculate_diff(self, source: dict[str, Any], target: dict[str, Any]) -> VersionDiff:
        """Calculate the difference between two definitions."""
        changes = []
        additions = 0
        deletions = 0
        modifications = 0

        # Compare nodes
        source_nodes = {n["id"]: n for n in source.get("nodes", [])}
        target_nodes = {n["id"]: n for n in target.get("nodes", [])}

        # Added nodes
        for node_id, node in target_nodes.items():
            if node_id not in source_nodes:
                changes.append(
                    VersionChange(
                        change_type=ChangeType.NODE_ADDED,
                        path=f"nodes.{node_id}",
                        new_value=node,
                        description=f"Added node: {node.get('type', 'unknown')}",
                    )
                )
                additions += 1

        # Removed nodes
        for node_id, node in source_nodes.items():
            if node_id not in target_nodes:
                changes.append(
                    VersionChange(
                        change_type=ChangeType.NODE_REMOVED,
                        path=f"nodes.{node_id}",
                        old_value=node,
                        description=f"Removed node: {node.get('type', 'unknown')}",
                    )
                )
                deletions += 1

        # Modified nodes
        for node_id in source_nodes.keys() & target_nodes.keys():
            if source_nodes[node_id] != target_nodes[node_id]:
                changes.append(
                    VersionChange(
                        change_type=ChangeType.NODE_MODIFIED,
                        path=f"nodes.{node_id}",
                        old_value=source_nodes[node_id],
                        new_value=target_nodes[node_id],
                        description=f"Modified node: {node_id}",
                    )
                )
                modifications += 1

        # Compare edges
        source_edges = {self._edge_key(e): e for e in source.get("edges", [])}
        target_edges = {self._edge_key(e): e for e in target.get("edges", [])}

        # Added edges
        for edge_key, edge in target_edges.items():
            if edge_key not in source_edges:
                changes.append(
                    VersionChange(
                        change_type=ChangeType.EDGE_ADDED,
                        path=f"edges.{edge_key}",
                        new_value=edge,
                        description=f"Added edge: {edge.get('from')} -> {edge.get('to')}",
                    )
                )
                additions += 1

        # Removed edges
        for edge_key, edge in source_edges.items():
            if edge_key not in target_edges:
                changes.append(
                    VersionChange(
                        change_type=ChangeType.EDGE_REMOVED,
                        path=f"edges.{edge_key}",
                        old_value=edge,
                        description=f"Removed edge: {edge.get('from')} -> {edge.get('to')}",
                    )
                )
                deletions += 1

        # Modified edges
        for edge_key in source_edges.keys() & target_edges.keys():
            if source_edges[edge_key] != target_edges[edge_key]:
                changes.append(
                    VersionChange(
                        change_type=ChangeType.EDGE_MODIFIED,
                        path=f"edges.{edge_key}",
                        old_value=source_edges[edge_key],
                        new_value=target_edges[edge_key],
                        description=f"Modified edge: {edge_key}",
                    )
                )
                modifications += 1

        return VersionDiff(
            source_version_id="",
            target_version_id="",
            changes=changes,
            additions=additions,
            deletions=deletions,
            modifications=modifications,
        )

    # ==================== Rollback ====================

    async def rollback_to_version(
        self,
        workflow_id: str,
        target_version_id: str,
        rolled_back_by: str,
        reason: str = "",
    ) -> WorkflowVersion | None:
        """Rollback to a previous version."""
        target = await self.get_version(target_version_id)
        if not target or target.workflow_id != workflow_id:
            return None

        current_head = await self.get_head_version(workflow_id, target.branch)

        # Create a new version with the old definition
        rollback_version = await self.create_version(
            workflow_id=workflow_id,
            definition=target.definition,
            name=f"Rollback to {target.version_number}",
            description=f"Rolled back to version {target.version_number}. Reason: {reason}",
            created_by=rolled_back_by,
            parent_version_id=current_head.id if current_head else None,
            branch=target.branch,
        )

        # Activate the rollback version
        await self.activate_version(rollback_version.id, rolled_back_by)

        await self._log_audit(
            workflow_id=workflow_id,
            version_id=rollback_version.id,
            action="version_rollback",
            actor=rolled_back_by,
            details={
                "target_version_id": target_version_id,
                "target_version_number": target.version_number,
                "reason": reason,
            },
        )

        return rollback_version

    # ==================== Tags ====================

    async def add_tag(self, version_id: str, tag: str, tagged_by: str) -> WorkflowVersion | None:
        """Add a tag to a version."""
        version = await self.get_version(version_id)
        if not version:
            return None

        if tag not in version.tags:
            version.tags.append(tag)
            await self.storage.save_version(version)

            await self._log_audit(
                workflow_id=version.workflow_id,
                version_id=version_id,
                action="tag_added",
                actor=tagged_by,
                details={"tag": tag},
            )

        return version

    async def remove_tag(self, version_id: str, tag: str, removed_by: str) -> WorkflowVersion | None:
        """Remove a tag from a version."""
        version = await self.get_version(version_id)
        if not version:
            return None

        if tag in version.tags:
            version.tags.remove(tag)
            await self.storage.save_version(version)

            await self._log_audit(
                workflow_id=version.workflow_id,
                version_id=version_id,
                action="tag_removed",
                actor=removed_by,
                details={"tag": tag},
            )

        return version

    async def get_version_by_tag(self, workflow_id: str, tag: str) -> WorkflowVersion | None:
        """Get a version by tag."""
        versions = await self.storage.get_workflow_versions(workflow_id)
        for v in versions:
            if tag in v.tags:
                return v
        return None

    # ==================== Audit Trail ====================

    async def get_audit_log(
        self,
        workflow_id: str,
        version_id: str | None = None,
        action: str | None = None,
        actor: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 100,
    ) -> list[AuditLogEntry]:
        """Get audit log entries."""
        entries = await self.storage.get_audit_log(workflow_id)

        if version_id:
            entries = [e for e in entries if e.version_id == version_id]

        if action:
            entries = [e for e in entries if e.action == action]

        if actor:
            entries = [e for e in entries if e.actor == actor]

        if start_date:
            entries = [e for e in entries if e.timestamp >= start_date]

        if end_date:
            entries = [e for e in entries if e.timestamp <= end_date]

        entries.sort(key=lambda e: e.timestamp, reverse=True)
        return entries[:limit]

    # ==================== Helper Methods ====================

    def _hash_definition(self, definition: dict[str, Any]) -> str:
        """Generate a hash of the workflow definition."""
        canonical = json.dumps(definition, sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def _increment_version(self, version: str, increment_type: str = "patch") -> str:
        """Increment a semantic version number."""
        parts = version.split(".")
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

        if increment_type == "major":
            return f"{major + 1}.0.0"
        elif increment_type == "minor":
            return f"{major}.{minor + 1}.0"
        else:  # patch
            return f"{major}.{minor}.{patch + 1}"

    def _edge_key(self, edge: dict[str, Any]) -> str:
        """Generate a unique key for an edge."""
        return f"{edge.get('from', '')}->{edge.get('to', '')}"

    def _generate_change_summary(self, changes: list[VersionChange]) -> str:
        """Generate a human-readable summary of changes."""
        if not changes:
            return "No changes"

        added = len([c for c in changes if "ADDED" in c.change_type.name])
        removed = len([c for c in changes if "REMOVED" in c.change_type.name])
        modified = len([c for c in changes if "MODIFIED" in c.change_type.name])

        parts = []
        if added:
            parts.append(f"{added} added")
        if removed:
            parts.append(f"{removed} removed")
        if modified:
            parts.append(f"{modified} modified")

        return ", ".join(parts)

    async def _find_common_ancestor(
        self, version1: WorkflowVersion, version2: WorkflowVersion
    ) -> WorkflowVersion | None:
        """Find the common ancestor of two versions."""
        # Build ancestry chain for version1
        ancestors1 = set()
        current = version1
        while current:
            ancestors1.add(current.id)
            if current.parent_version_id:
                current = await self.get_version(current.parent_version_id)
            else:
                break

        # Find first common ancestor in version2's chain
        current = version2
        while current:
            if current.id in ancestors1:
                return current
            if current.parent_version_id:
                current = await self.get_version(current.parent_version_id)
            else:
                break

        return None

    def _detect_conflicts(self, diff1: VersionDiff, diff2: VersionDiff) -> list[MergeConflict]:
        """Detect conflicts between two diffs."""
        conflicts = []

        paths1 = {c.path for c in diff1.changes}
        paths2 = {c.path for c in diff2.changes}

        # Paths modified in both
        common_paths = paths1 & paths2

        for path in common_paths:
            change1 = next(c for c in diff1.changes if c.path == path)
            change2 = next(c for c in diff2.changes if c.path == path)

            # If both made the same change, no conflict
            if change1.new_value == change2.new_value:
                continue

            conflicts.append(
                MergeConflict(
                    path=path,
                    ours_value=change1.new_value,
                    theirs_value=change2.new_value,
                    base_value=change1.old_value,
                )
            )

        return conflicts

    def _perform_merge(
        self,
        base: dict[str, Any],
        ours: dict[str, Any],
        theirs: dict[str, Any],
        conflicts: list[MergeConflict],
        strategy: MergeStrategy,
    ) -> dict[str, Any]:
        """Perform the actual merge of definitions."""
        result = deepcopy(ours)

        # Apply non-conflicting changes from theirs
        theirs_diff = self._calculate_diff(base, theirs)
        ours_paths = {c.path for c in self._calculate_diff(base, ours).changes}

        for change in theirs_diff.changes:
            if change.path not in ours_paths:
                self._apply_change(result, change)

        # Apply conflict resolutions
        for conflict in conflicts:
            if conflict.resolved:
                self._set_path_value(result, conflict.path, conflict.resolution)
            elif strategy == MergeStrategy.OURS:
                pass  # Keep ours (already in result)
            elif strategy == MergeStrategy.THEIRS:
                self._set_path_value(result, conflict.path, conflict.theirs_value)

        return result

    def _apply_change(self, definition: dict[str, Any], change: VersionChange):
        """Apply a single change to a definition."""
        if change.change_type in [ChangeType.NODE_ADDED, ChangeType.EDGE_ADDED]:
            self._set_path_value(definition, change.path, change.new_value)
        elif change.change_type in [ChangeType.NODE_REMOVED, ChangeType.EDGE_REMOVED]:
            self._delete_path(definition, change.path)
        elif change.change_type in [ChangeType.NODE_MODIFIED, ChangeType.EDGE_MODIFIED]:
            self._set_path_value(definition, change.path, change.new_value)

    def _set_path_value(self, obj: dict[str, Any], path: str, value: Any):
        """Set a value at a JSON path."""
        parts = path.split(".")
        current = obj

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    def _delete_path(self, obj: dict[str, Any], path: str):
        """Delete a value at a JSON path."""
        parts = path.split(".")
        current = obj

        for part in parts[:-1]:
            if part not in current:
                return
            current = current[part]

        if parts[-1] in current:
            del current[parts[-1]]

    async def _log_audit(
        self,
        workflow_id: str,
        version_id: str | None,
        action: str,
        actor: str,
        details: dict[str, Any] = None,
    ):
        """Log an audit entry."""
        entry = AuditLogEntry(
            id=str(uuid4()),
            workflow_id=workflow_id,
            version_id=version_id,
            action=action,
            actor=actor,
            timestamp=datetime.now(UTC),
            details=details or {},
        )
        await self.storage.save_audit_log(entry)


class InMemoryVersionStorage:
    """In-memory storage for development/testing."""

    def __init__(self):
        self._versions: dict[str, WorkflowVersion] = {}
        self._branches: dict[str, VersionBranch] = {}
        self._audit_log: list[AuditLogEntry] = []

    async def get_version(self, version_id: str) -> WorkflowVersion | None:
        return self._versions.get(version_id)

    async def save_version(self, version: WorkflowVersion):
        self._versions[version.id] = version

    async def get_workflow_versions(self, workflow_id: str) -> list[WorkflowVersion]:
        return [v for v in self._versions.values() if v.workflow_id == workflow_id]

    async def get_branch(self, workflow_id: str, branch_name: str) -> VersionBranch | None:
        for branch in self._branches.values():
            if branch.workflow_id == workflow_id and branch.name == branch_name:
                return branch
        return None

    async def save_branch(self, branch: VersionBranch):
        self._branches[branch.id] = branch

    async def delete_branch(self, branch_id: str):
        self._branches.pop(branch_id, None)

    async def get_workflow_branches(self, workflow_id: str) -> list[VersionBranch]:
        return [b for b in self._branches.values() if b.workflow_id == workflow_id]

    async def save_audit_log(self, entry: AuditLogEntry):
        self._audit_log.append(entry)

    async def get_audit_log(self, workflow_id: str) -> list[AuditLogEntry]:
        return [e for e in self._audit_log if e.workflow_id == workflow_id]
