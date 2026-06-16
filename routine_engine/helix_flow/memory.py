"""
Helix Chains - Memory Systems
==============================

Memory implementations for maintaining context across chain executions:
- ConversationMemory: Full conversation history
- SummaryMemory: Summarized conversation history
- VectorMemory: Semantic search over past interactions
- WindowMemory: Sliding window of recent messages
- EntityMemory: Track entities mentioned in conversations
"""

import asyncio
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class MemoryEntry:
    """A single memory entry"""

    id: str
    content: Any
    role: str = "user"  # user, assistant, system
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    metadata: dict[str, Any] = field(default_factory=dict)
    embedding: list[float] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "content": self.content,
            "role": self.role,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }


class Memory(ABC):
    """
    Base class for all memory implementations.

    Memory stores and retrieves context for chain executions,
    enabling stateful conversations and long-term recall.
    """

    def __init__(self, namespace: str = "default"):
        self.namespace = namespace
        self._entries: list[MemoryEntry] = []

    @abstractmethod
    async def add(self, content: Any, role: str = "user", metadata: dict[str, Any] | None = None) -> str:
        """Add a memory entry"""

    @abstractmethod
    async def get(self, query: str | None = None, limit: int = 10) -> list[MemoryEntry]:
        """Retrieve memory entries"""

    @abstractmethod
    async def clear(self) -> None:
        """Clear all memory"""

    async def load(self, context: Any) -> dict[str, Any]:
        """Load memory for chain context"""
        entries = await self.get(limit=20)
        return {
            "history": [e.to_dict() for e in entries],
            "namespace": self.namespace,
        }

    async def save(self, context: Any, output: Any) -> None:
        """Save chain output to memory"""
        await self.add(output, role="assistant")

    def _generate_id(self, content: Any) -> str:
        """Generate unique ID for content"""
        content_str = json.dumps(content, default=str)
        return hashlib.md5(content_str.encode(), usedforsecurity=False).hexdigest()[:12]


class ConversationMemory(Memory):
    """
    Full conversation history memory.

    Stores all messages in order, providing complete context
    for conversations. Best for short to medium conversations.

    Example:
        memory = ConversationMemory()
        await memory.add("Hello!", role="user")
        await memory.add("Hi there!", role="assistant")
        history = await memory.get()
    """

    def __init__(self, max_entries: int = 100, **kwargs):
        super().__init__(**kwargs)
        self.max_entries = max_entries

    async def add(self, content: Any, role: str = "user", metadata: dict[str, Any] | None = None) -> str:
        """Add message to conversation history"""
        entry_id = self._generate_id(content)

        entry = MemoryEntry(
            id=entry_id,
            content=content,
            role=role,
            metadata=metadata or {},
        )

        self._entries.append(entry)

        # Trim if exceeds max
        if len(self._entries) > self.max_entries:
            self._entries = self._entries[-self.max_entries :]

        return entry_id

    async def get(self, query: str | None = None, limit: int = 10) -> list[MemoryEntry]:
        """Get recent conversation history"""
        return self._entries[-limit:]

    async def clear(self) -> None:
        """Clear conversation history"""
        self._entries = []

    def get_messages(self) -> list[dict[str, str]]:
        """Get messages in chat format"""
        return [{"role": e.role, "content": str(e.content)} for e in self._entries]

    def to_string(self, separator: str = "\n") -> str:
        """Convert history to string"""
        return separator.join([f"{e.role}: {e.content}" for e in self._entries])


class WindowMemory(Memory):
    """
    Sliding window memory that keeps only recent messages.

    Efficient for long conversations where only recent
    context is relevant.

    Example:
        memory = WindowMemory(window_size=5)
        # Only keeps last 5 messages
    """

    def __init__(self, window_size: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.window_size = window_size
        self._window: deque = deque(maxlen=window_size)

    async def add(self, content: Any, role: str = "user", metadata: dict[str, Any] | None = None) -> str:
        """Add message to window"""
        entry_id = self._generate_id(content)

        entry = MemoryEntry(
            id=entry_id,
            content=content,
            role=role,
            metadata=metadata or {},
        )

        self._window.append(entry)
        return entry_id

    async def get(self, query: str | None = None, limit: int = 10) -> list[MemoryEntry]:
        """Get messages in window"""
        return list(self._window)[-limit:]

    async def clear(self) -> None:
        """Clear window"""
        self._window.clear()


class SummaryMemory(Memory):
    """
    Memory that maintains a running summary of the conversation.

    Uses an LLM to periodically summarize the conversation,
    keeping context while reducing token usage.

    Example:
        memory = SummaryMemory(
            summarizer=llm_summarize_fn,
            summary_threshold=10
        )
    """

    def __init__(self, summarizer: Any = None, summary_threshold: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.summarizer = summarizer
        self.summary_threshold = summary_threshold
        self._summary: str = ""
        self._recent: list[MemoryEntry] = []
        self._message_count: int = 0

    async def add(self, content: Any, role: str = "user", metadata: dict[str, Any] | None = None) -> str:
        """Add message and potentially update summary"""
        entry_id = self._generate_id(content)

        entry = MemoryEntry(
            id=entry_id,
            content=content,
            role=role,
            metadata=metadata or {},
        )

        self._recent.append(entry)
        self._message_count += 1

        # Check if we should summarize
        if self._message_count >= self.summary_threshold:
            await self._update_summary()

        return entry_id

    async def get(self, query: str | None = None, limit: int = 10) -> list[MemoryEntry]:
        """Get summary and recent messages"""
        result = []

        # Add summary as first entry if exists
        if self._summary:
            result.append(
                MemoryEntry(
                    id="summary",
                    content=f"[Previous conversation summary: {self._summary}]",
                    role="system",
                )
            )

        # Add recent messages
        result.extend(self._recent[-limit:])

        return result

    async def clear(self) -> None:
        """Clear summary and recent messages"""
        self._summary = ""
        self._recent = []
        self._message_count = 0

    async def _update_summary(self) -> None:
        """Update the running summary"""
        if not self.summarizer:
            # Simple fallback: just keep last few messages
            self._recent = self._recent[-5:]
            self._message_count = len(self._recent)
            return

        # Build text to summarize
        text_to_summarize = "\n".join([f"{e.role}: {e.content}" for e in self._recent])

        if self._summary:
            text_to_summarize = f"Previous summary: {self._summary}\n\nNew messages:\n{text_to_summarize}"

        try:
            if asyncio.iscoroutinefunction(self.summarizer):
                self._summary = await self.summarizer(text_to_summarize)
            else:
                self._summary = self.summarizer(text_to_summarize)

            # Keep only most recent messages
            self._recent = self._recent[-3:]
            self._message_count = len(self._recent)

        except Exception as e:
            logger.error("Failed to update summary: %s", e)


class VectorMemory(Memory):
    """
    Semantic memory using vector embeddings.

    Stores messages with embeddings for semantic search,
    enabling retrieval of relevant past context.

    Example:
        memory = VectorMemory(
            embedder=embedding_fn,
            similarity_threshold=0.7
        )

        # Retrieves semantically similar past messages
        relevant = await memory.get("What did we discuss about AI?")
    """

    def __init__(
        self,
        embedder: Any = None,
        similarity_threshold: float = 0.7,
        max_entries: int = 1000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.embedder = embedder
        self.similarity_threshold = similarity_threshold
        self.max_entries = max_entries
        self._vectors: list[tuple[MemoryEntry, list[float]]] = []

    async def add(self, content: Any, role: str = "user", metadata: dict[str, Any] | None = None) -> str:
        """Add message with embedding"""
        entry_id = self._generate_id(content)

        # Generate embedding
        embedding = await self._embed(str(content))

        entry = MemoryEntry(
            id=entry_id,
            content=content,
            role=role,
            metadata=metadata or {},
            embedding=embedding,
        )

        self._vectors.append((entry, embedding))

        # Trim if exceeds max
        if len(self._vectors) > self.max_entries:
            self._vectors = self._vectors[-self.max_entries :]

        return entry_id

    async def get(self, query: str | None = None, limit: int = 10) -> list[MemoryEntry]:
        """Get semantically similar messages"""
        if not query or not self._vectors:
            # Return recent if no query
            return [e for e, _ in self._vectors[-limit:]]

        # Embed query
        query_embedding = await self._embed(query)

        # Calculate similarities
        scored = []
        for entry, embedding in self._vectors:
            similarity = self._cosine_similarity(query_embedding, embedding)
            if similarity >= self.similarity_threshold:
                scored.append((entry, similarity))

        # Sort by similarity
        scored.sort(key=lambda x: x[1], reverse=True)

        return [e for e, _ in scored[:limit]]

    async def clear(self) -> None:
        """Clear vector memory"""
        self._vectors = []

    async def _embed(self, text: str) -> list[float]:
        """Generate embedding for text"""
        if self.embedder:
            if asyncio.iscoroutinefunction(self.embedder):
                return await self.embedder(text)
            return self.embedder(text)

        # Fallback: simple hash-based pseudo-embedding
        return self._simple_embed(text)

    def _simple_embed(self, text: str, dim: int = 128) -> list[float]:
        """Simple fallback embedding using hashing"""
        import hashlib

        # Create deterministic pseudo-embedding
        hash_bytes = hashlib.sha512(text.encode()).digest()

        # Convert to floats
        embedding = []
        for i in range(0, min(len(hash_bytes), dim * 2), 2):
            val = (hash_bytes[i] + hash_bytes[i + 1] * 256) / 65535.0
            embedding.append(val * 2 - 1)  # Normalize to [-1, 1]

        # Pad if needed
        while len(embedding) < dim:
            embedding.append(0.0)

        return embedding[:dim]

    def _cosine_similarity(self, a: list[float], b: list[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        if not a or not b or len(a) != len(b):
            return 0.0

        dot_product = sum(x * y for x, y in zip(a, b, strict=False))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(x * x for x in b) ** 0.5

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot_product / (norm_a * norm_b)


class EntityMemory(Memory):
    """
    Memory that tracks entities mentioned in conversations.

    Extracts and maintains information about people, places,
    organizations, and other entities for context.

    Example:
        memory = EntityMemory()
        await memory.add("John works at Google in San Francisco")
        # Tracks: John (person), Google (org), San Francisco (place)
    """

    def __init__(self, entity_extractor: Any = None, **kwargs):
        super().__init__(**kwargs)
        self.entity_extractor = entity_extractor
        self._entities: dict[str, dict[str, Any]] = {}
        self._mentions: list[MemoryEntry] = []

    async def add(self, content: Any, role: str = "user", metadata: dict[str, Any] | None = None) -> str:
        """Add message and extract entities"""
        entry_id = self._generate_id(content)

        entry = MemoryEntry(
            id=entry_id,
            content=content,
            role=role,
            metadata=metadata or {},
        )

        self._mentions.append(entry)

        # Extract entities
        entities = await self._extract_entities(str(content))

        for entity_name, entity_info in entities.items():
            if entity_name not in self._entities:
                self._entities[entity_name] = {
                    "type": entity_info.get("type", "unknown"),
                    "mentions": [],
                    "attributes": {},
                }

            self._entities[entity_name]["mentions"].append(
                {
                    "entry_id": entry_id,
                    "context": str(content)[:200],
                    "timestamp": entry.timestamp,
                }
            )

            # Update attributes
            if "attributes" in entity_info:
                self._entities[entity_name]["attributes"].update(entity_info["attributes"])

        return entry_id

    async def get(self, query: str | None = None, limit: int = 10) -> list[MemoryEntry]:
        """Get entity information"""
        if query and query in self._entities:
            # Search for specific entity
            entity_info = self._entities[query]
            return [
                MemoryEntry(
                    id=f"entity_{query}",
                    content={
                        "entity": query,
                        "type": entity_info["type"],
                        "attributes": entity_info["attributes"],
                        "mention_count": len(entity_info["mentions"]),
                    },
                    role="system",
                )
            ]

        # Return all entities summary
        return [
            MemoryEntry(
                id="entities_summary",
                content={
                    "entities": {
                        name: {
                            "type": info["type"],
                            "mention_count": len(info["mentions"]),
                        }
                        for name, info in self._entities.items()
                    }
                },
                role="system",
            )
        ]

    async def clear(self) -> None:
        """Clear entity memory"""
        self._entities = {}
        self._mentions = []

    def get_entity(self, name: str) -> dict[str, Any] | None:
        """Get information about a specific entity"""
        return self._entities.get(name)

    def list_entities(self, entity_type: str | None = None) -> list[str]:
        """List all tracked entities"""
        if entity_type:
            return [name for name, info in self._entities.items() if info["type"] == entity_type]
        return list(self._entities.keys())

    async def _extract_entities(self, text: str) -> dict[str, dict[str, Any]]:
        """Extract entities from text"""
        if self.entity_extractor:
            if asyncio.iscoroutinefunction(self.entity_extractor):
                return await self.entity_extractor(text)
            return self.entity_extractor(text)

        # Simple fallback: extract capitalized words as potential entities
        return self._simple_extract(text)

    def _simple_extract(self, text: str) -> dict[str, dict[str, Any]]:
        """Simple entity extraction using patterns"""
        import re

        entities = {}

        # Find capitalized words (potential names/places)
        capitalized = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b", text)

        for word in capitalized:
            if len(word) > 2 and word not in [
                "The",
                "This",
                "That",
                "What",
                "When",
                "Where",
                "How",
                "Why",
            ]:
                entities[word] = {"type": "unknown"}

        # Find email addresses
        emails = re.findall(r"\b[\w.-]+@[\w.-]+\.\w+\b", text)
        for email in emails:
            entities[email] = {"type": "email"}

        # Find URLs
        urls = re.findall(r"https?://\S+", text)
        for url in urls:
            entities[url] = {"type": "url"}

        return entities


class CompositeMemory(Memory):
    """
    Combine multiple memory types.

    Example:
        memory = CompositeMemory([
            ConversationMemory(),
            EntityMemory(),
            VectorMemory()
        ])
    """

    def __init__(self, memories: list[Memory], **kwargs):
        super().__init__(**kwargs)
        self.memories = memories

    async def add(self, content: Any, role: str = "user", metadata: dict[str, Any] | None = None) -> str:
        """Add to all memories"""
        entry_id = None
        for memory in self.memories:
            entry_id = await memory.add(content, role, metadata)
        return entry_id

    async def get(self, query: str | None = None, limit: int = 10) -> list[MemoryEntry]:
        """Get from all memories and combine"""
        all_entries = []
        seen_ids = set()

        for memory in self.memories:
            entries = await memory.get(query, limit)
            for entry in entries:
                if entry.id not in seen_ids:
                    all_entries.append(entry)
                    seen_ids.add(entry.id)

        return all_entries[:limit]

    async def clear(self) -> None:
        """Clear all memories"""
        for memory in self.memories:
            await memory.clear()


# ============================================================================
# Platform-Bridged Persistent Memory
# ============================================================================


class PersistentConversationMemory(ConversationMemory):
    """ConversationMemory subclass that persists to the platform's
    ThreeTierMemoryManager (Redis core + conversational tiers, PostgreSQL
    archival).

    Use this in place of bare ``ConversationMemory`` when conversation
    state must survive process restarts.
    """

    def __init__(
        self,
        user_id: str = "system",
        agent_id: str = "helix_flow",
        max_entries: int = 100,
        **kwargs,
    ):
        super().__init__(max_entries=max_entries, **kwargs)
        self._user_id = user_id
        self._agent_id = agent_id
        self._ttm = None

    def _get_ttm(self):
        """Lazy-load the ThreeTierMemoryManager."""
        if self._ttm is None:
            try:
                from apps.backend.integrations.agent_memory_service import (
                    get_three_tier_manager,
                )

                self._ttm = get_three_tier_manager(f"{self._user_id}:{self._agent_id}")
            except (ImportError, Exception) as e:
                logger.debug("ThreeTierMemoryManager not available: %s", e)
        return self._ttm

    async def add(self, content: Any, role: str = "user", metadata: dict[str, Any] | None = None) -> str:
        """Add to in-memory list AND persist to ThreeTierMemoryManager."""
        entry_id = await super().add(content, role, metadata)

        # Persist to platform memory
        ttm = self._get_ttm()
        if ttm:
            try:
                tier = "conversational"
                entry_str = f"{role}: {content}" if isinstance(content, str) else str(content)
                await ttm.add(tier, entry_str)
            except Exception as e:
                logger.debug("Failed to persist flow memory entry: %s", e)

        return entry_id

    async def get(self, query: str | None = None, limit: int = 10) -> list[MemoryEntry]:
        """Retrieve from in-memory (fast path).  Falls back to platform
        memory on cold start."""
        if self._entries:
            return self._entries[-limit:]

        # Cold start — try to restore from platform memory
        ttm = self._get_ttm()
        if ttm:
            try:
                restored = await ttm.get_conversational()
                for item in restored[-limit:]:
                    content = item if isinstance(item, str) else str(item)
                    parts = content.split(": ", 1)
                    role = parts[0] if len(parts) == 2 and parts[0] in ("user", "assistant", "system") else "user"
                    text = parts[1] if len(parts) == 2 else content
                    self._entries.append(
                        MemoryEntry(
                            id=self._generate_id(text),
                            content=text,
                            role=role,
                        )
                    )
            except Exception as e:
                logger.debug("Failed to restore flow memory: %s", e)

        return self._entries[-limit:]


def create_persistent_memory(
    user_id: str = "system",
    agent_id: str = "helix_flow",
    max_entries: int = 100,
) -> PersistentConversationMemory:
    """Factory for creating a platform-bridged persistent memory."""
    return PersistentConversationMemory(
        user_id=user_id,
        agent_id=agent_id,
        max_entries=max_entries,
    )
