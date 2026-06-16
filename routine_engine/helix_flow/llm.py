"""
Helix Chains - LLM Providers
=============================

LLM provider implementations for various AI services:
- OpenAI (GPT-4, GPT-3.5)
- Anthropic (Claude)
- xAI (Grok) - affordable, OpenAI-compatible API
- Groq - ultra-fast inference, OpenAI-compatible
- Mistral - European AI, OpenAI-compatible
- NVIDIA NIM - NVIDIA inference microservices, OpenAI-compatible
- MiniMax - MiniMax AI models, OpenAI-compatible
- Cohere - Command R models (non-OpenAI SDK)
- Local models (Ollama, vLLM)
- Helix proprietary models
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# Optional imports for AI providers
try:
    from openai import AsyncOpenAI
except ImportError:
    AsyncOpenAI = None
try:
    from anthropic import AsyncAnthropic
except ImportError:
    AsyncAnthropic = None
try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    import cohere

    AsyncCohereClient = cohere.AsyncClientV2
except ImportError:
    AsyncCohereClient = None

logger = logging.getLogger(__name__)


class ModelType(Enum):
    """Types of language models"""

    CHAT = "chat"
    COMPLETION = "completion"
    EMBEDDING = "embedding"
    IMAGE = "image"


@dataclass
class LLMResponse:
    """Response from an LLM"""

    content: str
    model: str
    usage: dict[str, int] = field(default_factory=dict)
    finish_reason: str = "stop"
    tool_calls: list[Any] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "model": self.model,
            "usage": self.usage,
            "finish_reason": self.finish_reason,
            "tool_calls": self.tool_calls,
            "metadata": self.metadata,
        }


@dataclass
class LLMConfig:
    """Configuration for LLM providers"""

    model: str
    temperature: float = 0.7
    max_tokens: int = 2048
    top_p: float = 1.0
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0
    stop: list[str] = field(default_factory=list)
    timeout: float = 60.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "top_p": self.top_p,
            "frequency_penalty": self.frequency_penalty,
            "presence_penalty": self.presence_penalty,
            "stop": self.stop,
        }


class LLMProvider(ABC):
    """
    Base class for LLM providers.

    Provides a unified interface for interacting with
    various LLM services.
    """

    name: str = "base"
    model_type: ModelType = ModelType.CHAT

    def __init__(self, config: LLMConfig | None = None, **kwargs):
        self.config = config or LLMConfig(model="default")
        self._request_count = 0
        self._total_tokens = 0

    @abstractmethod
    async def generate(self, prompt: str, stop: list[str] | None = None, **kwargs) -> LLMResponse:
        """Generate completion for a prompt"""

    @abstractmethod
    async def chat(
        self,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> LLMResponse:
        """Generate chat completion"""

    async def stream(self, prompt: str, **kwargs) -> AsyncIterator[str]:
        """Stream completion tokens"""
        response = await self.generate(prompt, **kwargs)
        yield response.content

    async def chat_stream(
        self,
        messages: list[dict[str, str]],
        **kwargs,
    ) -> AsyncIterator[str]:
        """Stream chat completion tokens. Override for true streaming."""
        response = await self.chat(messages, **kwargs)
        yield response.content

    async def embed(self, text: str) -> list[float]:
        """Generate embedding for text"""
        raise NotImplementedError("Embedding not supported by this provider")

    async def __call__(self, prompt: str, **kwargs) -> str:
        """Make provider callable"""
        response = await self.generate(prompt, **kwargs)
        return response.content

    def _track_usage(self, usage: dict[str, int]) -> None:
        """Track token usage"""
        self._request_count += 1
        self._total_tokens += usage.get("total_tokens", 0)

    @property
    def stats(self) -> dict[str, Any]:
        """Get usage statistics"""
        return {
            "request_count": self._request_count,
            "total_tokens": self._total_tokens,
        }


class OpenAIProvider(LLMProvider):
    """
    OpenAI API provider (GPT-4, GPT-3.5, etc.)

    Example:
        provider = OpenAIProvider(
            api_key="sk-...",
            config=LLMConfig(model="gpt-4")
        )
        response = await provider.chat([
            {"role": "user", "content": "Hello!"}
        ])
    """

    name = "openai"

    def __init__(self, api_key: str | None = None, base_url: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.base_url = base_url or "https://api.openai.com/v1"
        self._client = None

    async def _get_client(self):
        """Get or create OpenAI client"""
        if self._client is None:
            if AsyncOpenAI is None:
                raise ImportError("openai package not installed. Run: pip install openai")
            try:
                self._client = AsyncOpenAI(
                    api_key=self.api_key,
                    base_url=self.base_url,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to create OpenAI client: {e}") from e
        return self._client

    async def generate(self, prompt: str, stop: list[str] | None = None, **kwargs) -> LLMResponse:
        """Generate completion"""
        client = await self._get_client()

        response = await client.completions.create(
            model=self.config.model,
            prompt=prompt,
            max_tokens=kwargs.get("max_tokens", self.config.max_tokens),
            temperature=kwargs.get("temperature", self.config.temperature),
            stop=stop or self.config.stop or None,
        )

        usage = {
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens,
        }
        self._track_usage(usage)

        return LLMResponse(
            content=response.choices[0].text,
            model=response.model,
            usage=usage,
            finish_reason=response.choices[0].finish_reason,
        )

    async def chat(
        self,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> LLMResponse:
        """Generate chat completion"""
        client = await self._get_client()

        request_kwargs = {
            "model": kwargs.get("model", self.config.model),
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
        }

        if tools:
            request_kwargs["tools"] = [{"type": "function", "function": t} for t in tools]

        response = await client.chat.completions.create(**request_kwargs)

        usage = {
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens,
        }
        self._track_usage(usage)

        choice = response.choices[0]

        return LLMResponse(
            content=choice.message.content or "",
            model=response.model,
            usage=usage,
            finish_reason=choice.finish_reason,
            tool_calls=choice.message.tool_calls or [],
        )

    async def stream(self, prompt: str, **kwargs) -> AsyncIterator[str]:
        """Stream completion tokens"""
        client = await self._get_client()

        stream = await client.chat.completions.create(
            model=self.config.model,
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        )

        async for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

    async def chat_stream(
        self,
        messages: list[dict[str, str]],
        **kwargs,
    ) -> AsyncIterator[str]:
        """Stream chat completion tokens with full message support."""
        client = await self._get_client()

        model = kwargs.get("model", self.config.model)
        max_tokens = kwargs.get("max_tokens", self.config.max_tokens)
        temperature = kwargs.get("temperature", self.config.temperature)

        stream = await client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=True,
        )

        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

    async def embed(self, text: str) -> list[float]:
        """Generate embedding"""
        client = await self._get_client()

        response = await client.embeddings.create(
            model="text-embedding-ada-002",
            input=text,
        )

        return response.data[0].embedding


class OllamaProvider(OpenAIProvider):
    """Ollama wrapper over OpenAIProvider that injects performance options on every call.

    Key improvements over bare OpenAIProvider:
    - keep_alive: keeps the model loaded in RAM between requests (avoids 10-30s cold-start)
    - num_ctx: caps the KV-cache window so small prompts don't allocate 32K slots
    - num_thread: pins Ollama to the physical core count for consistent throughput
    """

    name = "ollama"

    def __init__(self, num_ctx: int = 8192, num_thread: int = 4, keep_alive: str = "24h", **kwargs):
        super().__init__(**kwargs)
        self._ollama_extra = {
            "keep_alive": keep_alive,
            "options": {
                "num_ctx": num_ctx,
                "num_thread": num_thread,
            },
        }

    async def chat(self, messages: list[dict[str, str]], tools=None, **kwargs) -> "LLMResponse":
        kwargs.setdefault("extra_body", self._ollama_extra)
        client = await self._get_client()
        request_kwargs: dict = {
            "model": kwargs.get("model", self.config.model),
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
            "extra_body": kwargs["extra_body"],
        }
        if tools:
            request_kwargs["tools"] = [{"type": "function", "function": t} for t in tools]
        response = await client.chat.completions.create(**request_kwargs)
        usage = {
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens,
        }
        self._track_usage(usage)
        choice = response.choices[0]
        return LLMResponse(
            content=choice.message.content or "",
            model=response.model,
            usage=usage,
            finish_reason=choice.finish_reason,
            tool_calls=choice.message.tool_calls or [],
        )

    async def chat_stream(self, messages: list[dict[str, str]], **kwargs):
        client = await self._get_client()
        stream = await client.chat.completions.create(
            model=kwargs.get("model", self.config.model),
            messages=messages,
            max_tokens=kwargs.get("max_tokens", self.config.max_tokens),
            temperature=kwargs.get("temperature", self.config.temperature),
            stream=True,
            extra_body=kwargs.get("extra_body", self._ollama_extra),
        )
        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content


class AnthropicProvider(LLMProvider):
    """
    Anthropic API provider (Claude models)

    Example:
        provider = AnthropicProvider(
            api_key="sk-ant-...",
            config=LLMConfig(model="claude-3-opus-20240229")
        )
    """

    name = "anthropic"

    def __init__(self, api_key: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self._client = None

    async def _get_client(self):
        """Get or create Anthropic client"""
        if self._client is None:
            try:
                self._client = AsyncAnthropic(api_key=self.api_key)
            except ImportError:
                raise ImportError("anthropic package not installed. Run: pip install anthropic") from None
        return self._client

    async def generate(self, prompt: str, stop: list[str] | None = None, **kwargs) -> LLMResponse:
        """Generate completion"""
        return await self.chat(messages=[{"role": "user", "content": prompt}], **kwargs)

    async def chat(
        self,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> LLMResponse:
        """Generate chat completion"""
        client = await self._get_client()

        # Extract system message if present
        system = None
        chat_messages = []
        for msg in messages:
            if msg["role"] == "system":
                system = msg["content"]
            else:
                chat_messages.append(msg)

        request_kwargs = {
            "model": kwargs.get("model", self.config.model),
            "messages": chat_messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
        }

        if system:
            request_kwargs["system"] = system

        if tools:
            request_kwargs["tools"] = [
                {
                    "name": t["name"],
                    "description": t["description"],
                    "input_schema": t["parameters"],
                }
                for t in tools
            ]

        # Enable automatic prompt caching — caches system + conversation
        # prefix for 90% cost reduction on cache reads
        if kwargs.get("cache_control", True):
            request_kwargs["cache_control"] = {"type": "ephemeral"}

        response = await client.messages.create(**request_kwargs)

        # Extract content
        content = ""
        tool_calls = []
        for block in response.content:
            if hasattr(block, "text"):
                content += block.text
            elif hasattr(block, "type") and block.type == "tool_use":
                tool_calls.append(block)

        usage = {
            "prompt_tokens": response.usage.input_tokens,
            "completion_tokens": response.usage.output_tokens,
            "total_tokens": response.usage.input_tokens + response.usage.output_tokens,
        }

        # Track cache performance when available
        cache_read = getattr(response.usage, "cache_read_input_tokens", 0) or 0
        cache_creation = getattr(response.usage, "cache_creation_input_tokens", 0) or 0
        if cache_read or cache_creation:
            usage["cache_read_input_tokens"] = cache_read
            usage["cache_creation_input_tokens"] = cache_creation

        self._track_usage(usage)

        return LLMResponse(
            content=content,
            model=response.model,
            usage=usage,
            finish_reason=response.stop_reason,
            tool_calls=tool_calls,
        )

    async def chat_stream(
        self,
        messages: list[dict[str, str]],
        **kwargs,
    ) -> AsyncIterator[str]:
        """Stream chat completion tokens from Anthropic."""
        client = await self._get_client()

        # Extract system message (Anthropic API requires separate system param)
        system = None
        chat_messages = []
        for msg in messages:
            if msg["role"] == "system":
                system = msg["content"]
            else:
                chat_messages.append(msg)

        stream_kwargs = {
            "model": kwargs.get("model", self.config.model),
            "messages": chat_messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
        }
        if system:
            stream_kwargs["system"] = system

        # Enable automatic prompt caching for streaming too
        if kwargs.get("cache_control", True):
            stream_kwargs["cache_control"] = {"type": "ephemeral"}

        async with client.messages.stream(**stream_kwargs) as stream:
            async for text in stream.text_stream:
                yield text


class XAIProvider(LLMProvider):
    """
    xAI API provider (Grok models)

    Uses the OpenAI-compatible API at api.x.ai.
    One of the more affordable high-capability model providers.

    Example:
        provider = XAIProvider(
            api_key="xai-...",
            config=LLMConfig(model="grok-3")
        )
        response = await provider.chat([
            {"role": "user", "content": "Hello!"}
        ])
    """

    name = "xai"

    def __init__(self, api_key: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("XAI_API_KEY")
        self._client = None

    async def _get_client(self):
        """Get or create xAI client (OpenAI-compatible)"""
        if self._client is None:
            if AsyncOpenAI is None:
                raise ImportError("openai package not installed. Run: pip install openai")
            self._client = AsyncOpenAI(
                api_key=self.api_key,
                base_url="https://api.x.ai/v1",
            )
        return self._client

    async def generate(self, prompt: str, stop: list[str] | None = None, **kwargs) -> LLMResponse:
        """Generate completion via chat endpoint"""
        return await self.chat(
            messages=[{"role": "user", "content": prompt}],
            **kwargs,
        )

    async def chat(
        self,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> LLMResponse:
        """Generate chat completion"""
        client = await self._get_client()

        request_kwargs = {
            "model": kwargs.get("model", self.config.model),
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
        }

        if tools:
            request_kwargs["tools"] = [{"type": "function", "function": t} for t in tools]

        response = await client.chat.completions.create(**request_kwargs)

        usage = {
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens,
        }
        self._track_usage(usage)

        choice = response.choices[0]

        return LLMResponse(
            content=choice.message.content or "",
            model=response.model,
            usage=usage,
            finish_reason=choice.finish_reason,
            tool_calls=choice.message.tool_calls or [],
        )

    async def stream(self, prompt: str, **kwargs) -> AsyncIterator[str]:
        """Stream completion tokens"""
        client = await self._get_client()

        stream = await client.chat.completions.create(
            model=self.config.model,
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        )

        async for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

    async def chat_stream(
        self,
        messages: list[dict[str, str]],
        **kwargs,
    ) -> AsyncIterator[str]:
        """Stream chat completion tokens via xAI (OpenAI-compatible)."""
        client = await self._get_client()

        model = kwargs.get("model", self.config.model)
        max_tokens = kwargs.get("max_tokens", self.config.max_tokens)
        temperature = kwargs.get("temperature", self.config.temperature)

        stream = await client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=True,
        )

        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content


class _OpenAICompatibleProvider(LLMProvider):
    """
    Base class for providers using OpenAI-compatible APIs.

    Subclasses only need to set `name`, `_env_key`, and `_base_url`.
    All chat/generate/stream methods are inherited from the OpenAI pattern.
    """

    name = "openai_compatible"
    _env_key: str = ""
    _base_url: str = ""

    def __init__(self, api_key: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv(self._env_key)
        self._client = None

    async def _get_client(self):
        if self._client is None:
            if AsyncOpenAI is None:
                raise ImportError("openai package not installed. Run: pip install openai")
            self._client = AsyncOpenAI(
                api_key=self.api_key,
                base_url=self._base_url,
            )
        return self._client

    async def generate(self, prompt: str, stop: list[str] | None = None, **kwargs) -> LLMResponse:
        return await self.chat(
            messages=[{"role": "user", "content": prompt}],
            **kwargs,
        )

    async def chat(
        self,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> LLMResponse:
        client = await self._get_client()

        request_kwargs = {
            "model": kwargs.get("model", self.config.model),
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
        }

        if tools:
            request_kwargs["tools"] = [{"type": "function", "function": t} for t in tools]

        response = await client.chat.completions.create(**request_kwargs)

        usage = {
            "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
            "completion_tokens": response.usage.completion_tokens if response.usage else 0,
            "total_tokens": response.usage.total_tokens if response.usage else 0,
        }
        self._track_usage(usage)

        choice = response.choices[0]
        return LLMResponse(
            content=choice.message.content or "",
            model=response.model,
            usage=usage,
            finish_reason=choice.finish_reason,
            tool_calls=choice.message.tool_calls or [],
        )

    async def chat_stream(
        self,
        messages: list[dict[str, str]],
        **kwargs,
    ) -> AsyncIterator[str]:
        client = await self._get_client()

        stream = await client.chat.completions.create(
            model=kwargs.get("model", self.config.model),
            messages=messages,
            max_tokens=kwargs.get("max_tokens", self.config.max_tokens),
            temperature=kwargs.get("temperature", self.config.temperature),
            stream=True,
        )

        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content


class GroqProvider(_OpenAICompatibleProvider):
    """
    Groq API provider — ultra-fast inference on open-source models.

    Free tier: ~14,400 requests/day, 30 RPM.
    Models: llama-3.3-70b-versatile, llama-3.1-8b-instant, mixtral-8x7b-32768, gemma2-9b-it
    """

    name = "groq"
    _env_key = "GROQ_API_KEY"
    _base_url = "https://api.groq.com/openai/v1"


class MistralProvider(_OpenAICompatibleProvider):
    """
    Mistral AI provider — European AI models.

    Free tier: mistral-small-latest, ~500 req/day.
    """

    name = "mistral"
    _env_key = "MISTRAL_API_KEY"
    _base_url = "https://api.mistral.ai/v1"


class NvidiaProvider(_OpenAICompatibleProvider):
    """
    NVIDIA NIM provider — NVIDIA inference microservices.

    Free tier: ~1000 req/day, 30 RPM.
    Models: nvidia/llama-3.1-nemotron-70b-instruct, meta/llama-3.1-8b-instruct
    """

    name = "nvidia_nim"
    _env_key = "NVIDIA_NIM_API_KEY"
    _base_url = "https://integrate.api.nvidia.com/v1"


class MiniMaxProvider(_OpenAICompatibleProvider):
    """
    MiniMax AI provider — MiniMax models.

    Free tier: minimax-m2.5, ~500 req/day.
    """

    name = "minimax"
    _env_key = "MINIMAX_API_KEY"
    _base_url = "https://api.minimax.chat/v1"


class GoogleGeminiProvider(_OpenAICompatibleProvider):
    """
    Google Gemini provider via OpenAI-compatible endpoint.

    Free tier: 1500 RPD, 15 RPM.
    Models: gemini-2.0-flash, gemini-1.5-flash, gemini-1.5-flash-8b
    """

    name = "google_gemini"
    _env_key = "GOOGLE_GEMINI_API_KEY"
    _base_url = "https://generativelanguage.googleapis.com/v1beta/openai"


class CohereProvider(LLMProvider):
    """
    Cohere API provider — Command R models.

    NOT OpenAI-compatible — uses Cohere SDK.
    Free tier: command-r, command-r-plus, ~1000 req/day.
    """

    name = "cohere"

    def __init__(self, api_key: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("COHERE_API_KEY")
        self._client = None

    async def _get_client(self):
        if self._client is None:
            if AsyncCohereClient is None:
                raise ImportError("cohere package not installed. Run: pip install cohere")
            self._client = AsyncCohereClient(api_key=self.api_key)
        return self._client

    async def generate(self, prompt: str, stop: list[str] | None = None, **kwargs) -> LLMResponse:
        return await self.chat(
            messages=[{"role": "user", "content": prompt}],
            **kwargs,
        )

    async def chat(
        self,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> LLMResponse:
        client = await self._get_client()

        model = kwargs.get("model", self.config.model)

        response = await client.chat(
            model=model,
            messages=messages,
        )

        content = ""
        if hasattr(response, "message") and hasattr(response.message, "content"):
            for block in response.message.content:
                if hasattr(block, "text"):
                    content += block.text

        usage = {}
        if hasattr(response, "usage"):
            usage = {
                "prompt_tokens": (
                    getattr(response.usage, "tokens", {}).get("input_tokens", 0)
                    if hasattr(response.usage, "tokens")
                    else 0
                ),
                "completion_tokens": (
                    getattr(response.usage, "tokens", {}).get("output_tokens", 0)
                    if hasattr(response.usage, "tokens")
                    else 0
                ),
                "total_tokens": 0,
            }
            usage["total_tokens"] = usage["prompt_tokens"] + usage["completion_tokens"]
        self._track_usage(usage)

        return LLMResponse(
            content=content,
            model=model,
            usage=usage,
            finish_reason=getattr(response, "finish_reason", "stop") or "stop",
        )


class LocalProvider(LLMProvider):
    """
    Local LLM provider (Ollama, vLLM, etc.)

    Example:
        provider = LocalProvider(
            base_url="http://localhost:11434",
            config=LLMConfig(model="llama2")
        )
    """

    name = "local"

    def __init__(self, base_url: str = "http://localhost:11434", **kwargs):
        super().__init__(**kwargs)
        self.base_url = base_url

    async def generate(self, prompt: str, stop: list[str] | None = None, **kwargs) -> LLMResponse:
        """Generate completion using local model"""
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.config.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": self.config.temperature,
                            "num_predict": self.config.max_tokens,
                            "stop": stop or self.config.stop,
                        },
                    },
                ) as response,
            ):
                data = await response.json()

                return LLMResponse(
                    content=data.get("response", ""),
                    model=self.config.model,
                    usage={
                        "prompt_tokens": data.get("prompt_eval_count", 0),
                        "completion_tokens": data.get("eval_count", 0),
                        "total_tokens": data.get("prompt_eval_count", 0) + data.get("eval_count", 0),
                    },
                )
        except ImportError:
            raise ImportError("aiohttp package not installed. Run: pip install aiohttp") from None

    async def chat(
        self,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> LLMResponse:
        """Generate chat completion"""
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    f"{self.base_url}/api/chat",
                    json={
                        "model": self.config.model,
                        "messages": messages,
                        "stream": False,
                        "options": {
                            "temperature": self.config.temperature,
                            "num_predict": self.config.max_tokens,
                        },
                    },
                ) as response,
            ):
                data = await response.json()

                return LLMResponse(
                    content=data.get("message", {}).get("content", ""),
                    model=self.config.model,
                    usage={
                        "prompt_tokens": data.get("prompt_eval_count", 0),
                        "completion_tokens": data.get("eval_count", 0),
                        "total_tokens": data.get("prompt_eval_count", 0) + data.get("eval_count", 0),
                    },
                )
        except ImportError:
            raise ImportError("aiohttp package not installed. Run: pip install aiohttp") from None

    async def stream(self, prompt: str, **kwargs) -> AsyncIterator[str]:
        """Stream completion tokens"""
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.config.model,
                        "prompt": prompt,
                        "stream": True,
                    },
                ) as response,
            ):
                async for line in response.content:
                    if line:
                        data = json.loads(line)
                        if "response" in data:
                            yield data["response"]
        except ImportError:
            raise ImportError("aiohttp package not installed. Run: pip install aiohttp") from None


class HelixLLM(LLMProvider):
    """
    Helix Collective's proprietary LLM provider.

    Routes requests to the optimal model based on task type
    and available resources.

    Example:
        provider = HelixLLM()
        response = await provider.chat([
            {"role": "user", "content": "Explain system computing"}
        ])
    """

    name = "helix"

    def __init__(
        self,
        providers: dict[str, LLMProvider] | None = None,
        router: Callable | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.providers = providers or {}
        self.router = router or self._default_router
        self._fallback_order = ["openai", "anthropic", "xai", "local"]

    def add_provider(self, name: str, provider: LLMProvider) -> "HelixLLM":
        """Add a provider"""
        self.providers[name] = provider
        return self

    def _default_router(self, task: str, **kwargs) -> str:
        """Default routing logic"""
        # Simple routing based on task keywords
        task_lower = task.lower()

        if "code" in task_lower or "programming" in task_lower:
            return "openai"  # GPT-4 is good at code
        elif "creative" in task_lower or "story" in task_lower:
            return "anthropic"  # Claude is good at creative writing
        elif "fast" in task_lower or "affordable" in task_lower:
            return "xai"  # Grok is fast and affordable
        elif "simple" in task_lower:
            return "local"  # Local models for simple tasks

        return "openai"  # Default

    async def _get_provider(self, task: str = "") -> LLMProvider:
        """Get the best provider for the task"""
        provider_name = self.router(task)

        if provider_name in self.providers:
            return self.providers[provider_name]

        # Try fallback order
        for name in self._fallback_order:
            if name in self.providers:
                return self.providers[name]

        raise ValueError("No LLM providers configured")

    async def generate(self, prompt: str, stop: list[str] | None = None, **kwargs) -> LLMResponse:
        """Generate completion using best provider"""
        provider = await self._get_provider(prompt)
        return await provider.generate(prompt, stop, **kwargs)

    async def chat(
        self,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> LLMResponse:
        """Generate chat completion using best provider"""
        # Extract task from messages
        task = messages[-1]["content"] if messages else ""
        provider = await self._get_provider(task)
        return await provider.chat(messages, tools, **kwargs)

    async def stream(self, prompt: str, **kwargs) -> AsyncIterator[str]:
        """Stream from best provider"""
        provider = await self._get_provider(prompt)
        async for token in provider.stream(prompt, **kwargs):
            yield token


# ============================================================================
# FACTORY FUNCTIONS
# ============================================================================


def create_provider(provider_type: str, **kwargs) -> LLMProvider:
    """
    Factory function to create LLM providers.

    Args:
        provider_type: Type of provider (openai, anthropic, local, helix)
        **kwargs: Provider-specific configuration

    Returns:
        Configured LLMProvider instance
    """
    providers = {
        "openai": OpenAIProvider,
        "anthropic": AnthropicProvider,
        "xai": XAIProvider,
        "groq": GroqProvider,
        "mistral": MistralProvider,
        "nvidia_nim": NvidiaProvider,
        "minimax": MiniMaxProvider,
        "google_gemini": GoogleGeminiProvider,
        "cohere": CohereProvider,
        "local": LocalProvider,
        "helix": HelixLLM,
    }

    if provider_type not in providers:
        raise ValueError(f"Unknown provider type: {provider_type}")

    return providers[provider_type](**kwargs)


def create_helix_llm(
    openai_key: str | None = None,
    anthropic_key: str | None = None,
    xai_key: str | None = None,
    groq_key: str | None = None,
    mistral_key: str | None = None,
    nvidia_nim_key: str | None = None,
    minimax_key: str | None = None,
    google_gemini_key: str | None = None,
    cohere_key: str | None = None,
    local_url: str | None = None,
) -> HelixLLM:
    """
    Create a HelixLLM with multiple providers configured.
    """
    helix = HelixLLM()

    if openai_key or os.getenv("OPENAI_API_KEY"):
        helix.add_provider(
            "openai",
            OpenAIProvider(
                api_key=openai_key,
                config=LLMConfig(model="gpt-4-turbo-preview"),
            ),
        )

    if anthropic_key or os.getenv("ANTHROPIC_API_KEY"):
        helix.add_provider(
            "anthropic",
            AnthropicProvider(
                api_key=anthropic_key,
                config=LLMConfig(model="claude-3-opus-20240229"),
            ),
        )

    if xai_key or os.getenv("XAI_API_KEY"):
        helix.add_provider(
            "xai",
            XAIProvider(
                api_key=xai_key,
                config=LLMConfig(model="grok-3-mini"),
            ),
        )

    if groq_key or os.getenv("GROQ_API_KEY"):
        helix.add_provider(
            "groq",
            GroqProvider(
                api_key=groq_key,
                config=LLMConfig(model="llama-3.3-70b-versatile"),
            ),
        )

    if mistral_key or os.getenv("MISTRAL_API_KEY"):
        helix.add_provider(
            "mistral",
            MistralProvider(
                api_key=mistral_key,
                config=LLMConfig(model="mistral-small-latest"),
            ),
        )

    if nvidia_nim_key or os.getenv("NVIDIA_NIM_API_KEY"):
        helix.add_provider(
            "nvidia_nim",
            NvidiaProvider(
                api_key=nvidia_nim_key,
                config=LLMConfig(model="nvidia/llama-3.1-nemotron-70b-instruct"),
            ),
        )

    if minimax_key or os.getenv("MINIMAX_API_KEY"):
        helix.add_provider(
            "minimax",
            MiniMaxProvider(
                api_key=minimax_key,
                config=LLMConfig(model="minimax-m2.5"),
            ),
        )

    if google_gemini_key or os.getenv("GOOGLE_GEMINI_API_KEY"):
        helix.add_provider(
            "google_gemini",
            GoogleGeminiProvider(
                api_key=google_gemini_key,
                config=LLMConfig(model="gemini-2.0-flash"),
            ),
        )

    if cohere_key or os.getenv("COHERE_API_KEY"):
        helix.add_provider(
            "cohere",
            CohereProvider(
                api_key=cohere_key,
                config=LLMConfig(model="command-r"),
            ),
        )

    if local_url:
        helix.add_provider(
            "local",
            LocalProvider(
                base_url=local_url,
                config=LLMConfig(model="llama2"),
            ),
        )

    return helix
