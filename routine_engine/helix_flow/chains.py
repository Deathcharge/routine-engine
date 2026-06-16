"""
Helix Chains - Core Chain Implementations
==========================================

Chain types for building complex AI workflows:
- SequentialChain: Execute steps in order
- ParallelChain: Execute steps concurrently
- ConditionalChain: Branch based on conditions
- MapReduceChain: Process collections in parallel
- RouterChain: Route to different chains based on input
"""

import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class ChainStatus(Enum):
    """Chain execution status"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ChainResult:
    """Result of a chain execution"""

    success: bool
    output: Any
    error: str | None = None
    execution_time_ms: float = 0
    steps_executed: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    trace: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
            "steps_executed": self.steps_executed,
            "metadata": self.metadata,
            "trace": self.trace,
        }


@dataclass
class ChainContext:
    """Context passed through chain execution"""

    chain_id: str
    run_id: str
    variables: dict[str, Any] = field(default_factory=dict)
    memory: dict[str, Any] = field(default_factory=dict)
    callbacks: list[Any] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    # Stores per-node outputs for inter-node data referencing
    node_outputs: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.variables.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.variables[key] = value

    def update(self, data: dict[str, Any]) -> None:
        self.variables.update(data)


def resolve_node_reference(reference: str, context: ChainContext) -> Any:
    """
    Resolve a node reference like '{{node_id.output.field}}' from the workflow context.

    Supports:
    - '{{node_id}}' - entire node output
    - '{{node_id.output}}' - specific key from node output
    - '{{node_id.items.0}}' - nested path access

    Args:
        reference: Reference string in {{node_id.path}} format
        context: Chain context containing node outputs

    Returns:
        Resolved value, or the original reference string if not found
    """
    import re

    match = re.match(r"^\{\{(.+?)\}\}$", reference.strip())
    if not match:
        return reference

    path = match.group(1).strip()
    parts = path.split(".")
    node_id = parts[0]

    output = context.node_outputs.get(node_id)
    if output is None:
        logger.debug("Node reference '%s' not found in context", node_id)
        return reference

    # Traverse nested path
    for part in parts[1:]:
        if isinstance(output, dict):
            output = output.get(part)
        elif isinstance(output, (list, tuple)):
            try:
                output = output[int(part)]
            except (ValueError, IndexError):
                return reference
        else:
            return reference
        if output is None:
            return reference

    return output


class ChainStep(ABC):
    """Base class for chain steps"""

    def __init__(self, name: str | None = None):
        self.name = name or self.__class__.__name__
        self.id = str(uuid.uuid4())[:8]

    @abstractmethod
    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute this step"""

    async def __call__(self, input_data: Any, context: ChainContext) -> Any:
        return await self.execute(input_data, context)


class Chain(ChainStep):
    """
    Base Chain class - the foundation of Helix Chains.

    A chain is a sequence of steps that process data through
    various transformations, LLM calls, tool executions, etc.

    Example:
        chain = Chain(
            name="summarizer",
            steps=[
                PromptTemplate("Summarize: {text}"),
                LLMCall(model="gpt-4"),
                OutputParser()
            ]
        )
        result = await chain.run(text="Long document...")
    """

    def __init__(
        self,
        name: str = "chain",
        steps: list[ChainStep] | None = None,
        memory: Any = None,
        callbacks: list[Any] | None = None,
        verbose: bool = False,
        max_retries: int = 3,
        timeout_seconds: float = 300,
    ):
        super().__init__(name)
        self.steps = steps or []
        self.memory = memory
        self.callbacks = callbacks or []
        self.verbose = verbose
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self._status = ChainStatus.PENDING

    @property
    def status(self) -> ChainStatus:
        return self._status

    def add_step(self, step: ChainStep) -> "Chain":
        """Add a step to the chain"""
        self.steps.append(step)
        return self

    def add_callback(self, callback: Any) -> "Chain":
        """Add a callback handler"""
        self.callbacks.append(callback)
        return self

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute the chain"""
        return await self.run(input_data, context=context)

    async def run(self, input_data: Any = None, context: ChainContext | None = None, **kwargs) -> ChainResult:
        """
        Run the chain with the given input.

        Args:
            input_data: Initial input to the chain
            context: Optional execution context
            **kwargs: Additional variables to add to context

        Returns:
            ChainResult with output and metadata
        """
        start_time = time.time()
        trace = []

        # Create context if not provided
        if context is None:
            context = ChainContext(
                chain_id=self.id,
                run_id=str(uuid.uuid4()),
                variables=kwargs,
                callbacks=self.callbacks,
            )
        else:
            context.variables.update(kwargs)

        # Add input to context
        if input_data is not None:
            if isinstance(input_data, dict):
                context.update(input_data)
            else:
                context.set("input", input_data)

        # Load memory if available
        if self.memory:
            memory_data = await self._load_memory(context)
            context.memory.update(memory_data)

        self._status = ChainStatus.RUNNING
        current_output = input_data
        steps_executed = 0

        # Notify callbacks of chain start
        for callback in self.callbacks:
            if hasattr(callback, "on_chain_start"):
                try:
                    await callback.on_chain_start(self, input_data)
                except Exception as cb_err:
                    logger.warning("Callback on_chain_start error: %s", cb_err)

        try:
            for i, step in enumerate(self.steps):
                step_start = time.time()

                if self.verbose:
                    logger.info("[%s] Executing step %s/%s: %s", self.name, i + 1, len(self.steps), step.name)

                # Notify callbacks
                await self._on_step_start(step, current_output, context)

                try:
                    current_output = await asyncio.wait_for(
                        step.execute(current_output, context),
                        timeout=self.timeout_seconds,
                    )
                    steps_executed += 1

                    # Update context with output
                    context.set(f"step_{i}_output", current_output)
                    context.set("last_output", current_output)

                    step_time = (time.time() - step_start) * 1000
                    trace.append(
                        {
                            "step": i,
                            "name": step.name,
                            "status": "success",
                            "execution_time_ms": step_time,
                            "output_preview": (str(current_output)[:200] if current_output else None),
                        }
                    )

                    # Notify callbacks
                    await self._on_step_end(step, current_output, context)

                except TimeoutError:
                    raise TimeoutError(f"Step {step.name} timed out after {self.timeout_seconds}s") from None
                except Exception as e:
                    trace.append(
                        {
                            "step": i,
                            "name": step.name,
                            "status": "error",
                            "error": type(e).__name__,
                        }
                    )
                    raise

            # Save memory if available
            if self.memory:
                await self._save_memory(context, current_output)

            self._status = ChainStatus.COMPLETED
            execution_time = (time.time() - start_time) * 1000

            # Notify callbacks of chain completion
            for callback in self.callbacks:
                if hasattr(callback, "on_chain_end"):
                    try:
                        await callback.on_chain_end(self, current_output)
                    except Exception as cb_err:
                        logger.warning("Callback on_chain_end error: %s", cb_err)

            return ChainResult(
                success=True,
                output=current_output,
                execution_time_ms=execution_time,
                steps_executed=steps_executed,
                trace=trace,
                metadata={
                    "chain_name": self.name,
                    "chain_id": self.id,
                    "run_id": context.run_id,
                },
            )

        except Exception as e:
            self._status = ChainStatus.FAILED
            execution_time = (time.time() - start_time) * 1000

            # Notify callbacks of chain error
            for callback in self.callbacks:
                if hasattr(callback, "on_chain_error"):
                    try:
                        await callback.on_chain_error(self, e)
                    except Exception as cb_err:
                        logger.warning("Callback on_chain_error error: %s", cb_err)

            logger.error("[%s] Chain failed: %s", self.name, e)

            return ChainResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=execution_time,
                steps_executed=steps_executed,
                trace=trace,
                metadata={
                    "chain_name": self.name,
                    "chain_id": self.id,
                    "run_id": context.run_id,
                },
            )

    async def stream(self, input_data: Any = None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        """
        Stream chain execution, yielding results as they become available.
        """
        context = ChainContext(
            chain_id=self.id,
            run_id=str(uuid.uuid4()),
            variables=kwargs,
            callbacks=self.callbacks,
        )

        if input_data is not None:
            if isinstance(input_data, dict):
                context.update(input_data)
            else:
                context.set("input", input_data)

        current_output = input_data

        for i, step in enumerate(self.steps):
            yield {
                "type": "step_start",
                "step": i,
                "name": step.name,
                "timestamp": datetime.now(UTC).isoformat(),
            }

            try:
                context.set("last_output", current_output)

                yield {
                    "type": "step_complete",
                    "step": i,
                    "name": step.name,
                    "output": current_output,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            except Exception as e:
                yield {
                    "type": "step_error",
                    "step": i,
                    "name": step.name,
                    "error": type(e).__name__,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
                raise

        yield {
            "type": "chain_complete",
            "output": current_output,
            "timestamp": datetime.now(UTC).isoformat(),
        }

    async def _load_memory(self, context: ChainContext) -> dict[str, Any]:
        """Load memory for this chain"""
        if hasattr(self.memory, "load"):
            return await self.memory.load(context)
        return {}

    async def _save_memory(self, context: ChainContext, output: Any) -> None:
        """Save memory after chain execution"""
        if hasattr(self.memory, "save"):
            await self.memory.save(context, output)

    async def _on_step_start(self, step: ChainStep, input_data: Any, context: ChainContext) -> None:
        """Notify callbacks of step start"""
        for callback in self.callbacks:
            if hasattr(callback, "on_step_start"):
                await callback.on_step_start(step, input_data, context)

    async def _on_step_end(self, step: ChainStep, output: Any, context: ChainContext) -> None:
        """Notify callbacks of step end"""
        for callback in self.callbacks:
            if hasattr(callback, "on_step_end"):
                await callback.on_step_end(step, output, context)


class SequentialChain(Chain):
    """
    Execute steps in strict sequential order.
    Output of each step becomes input to the next.
    """

    def __init__(self, chains: Sequence[ChainStep] | None = None, **kwargs):
        super().__init__(**kwargs)
        if chains:
            self.steps = list(chains)


class ParallelChain(Chain):
    """
    Execute multiple chains in parallel and combine results.

    Example:
        parallel = ParallelChain(
            chains=[
                Chain(name="research", steps=[...]),
                Chain(name="analysis", steps=[...]),
            ],
            combiner=lambda results: {"combined": results}
        )
    """

    def __init__(
        self,
        chains: Sequence[Chain] | None = None,
        combiner: Callable[[list[Any]], Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.chains = list(chains) if chains else []
        self.combiner = combiner or (lambda x: x)

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute all chains in parallel"""
        tasks = [chain.run(input_data, context=context) for chain in self.chains]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Extract outputs, handling errors
        outputs: list[Any] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("Parallel chain %s failed: %s", i, result)
                outputs.append({"error": str(result)})
            elif isinstance(result, ChainResult):
                outputs.append(result.output if result.success else {"error": result.error})
            else:
                outputs.append(result)

        return self.combiner(outputs)


class ConditionalChain(Chain):
    """
    Branch execution based on conditions.

    Example:
        conditional = ConditionalChain(
            condition=lambda x, ctx: x.get("type") == "question",
            if_true=qa_chain,
            if_false=general_chain
        )
    """

    def __init__(
        self,
        condition: Callable[[Any, ChainContext], bool],
        if_true: Chain,
        if_false: Chain | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.condition = condition
        self.if_true = if_true
        self.if_false = if_false

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute based on condition"""
        if self.condition(input_data, context):
            result = await self.if_true.run(input_data, context=context)
        elif self.if_false:
            result = await self.if_false.run(input_data, context=context)
        else:
            return input_data

        return result.output if isinstance(result, ChainResult) else result


class MapReduceChain(Chain):
    """
    Process a collection of items in parallel, then reduce results.

    Example:
        map_reduce = MapReduceChain(
            map_chain=summarize_chain,
            reduce_chain=combine_chain,
            input_key="documents"
        )
    """

    def __init__(
        self,
        map_chain: Chain,
        reduce_chain: Chain,
        input_key: str = "items",
        max_concurrency: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.map_chain = map_chain
        self.reduce_chain = reduce_chain
        self.input_key = input_key
        self.max_concurrency = max_concurrency

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Map over items, then reduce"""
        # Get items to process
        if isinstance(input_data, dict):
            items = input_data.get(self.input_key, [])
        elif isinstance(input_data, list):
            items = input_data
        else:
            items = [input_data]

        # Map phase - process items with concurrency limit
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def process_item(item):
            async with semaphore:
                result = await self.map_chain.run(item, context=context)
                return result.output if isinstance(result, ChainResult) else result

        tasks = [process_item(item) for item in items]
        mapped_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out errors
        valid_results = [r for r in mapped_results if not isinstance(r, Exception)]

        # Reduce phase
        reduce_result = await self.reduce_chain.run({"mapped_results": valid_results}, context=context)

        return reduce_result.output if isinstance(reduce_result, ChainResult) else reduce_result


class RouterChain(Chain):
    """
    Route input to different chains based on classification.

    Example:
        router = RouterChain(
            routes={
                "question": qa_chain,
                "task": task_chain,
                "chat": chat_chain,
            },
            classifier=intent_classifier,
            default_route="chat"
        )
    """

    def __init__(
        self,
        routes: dict[str, Chain],
        classifier: Callable[[Any, ChainContext], str],
        default_route: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.routes = routes
        self.classifier = classifier
        self.default_route = default_route

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Route to appropriate chain"""
        # Classify input
        if asyncio.iscoroutinefunction(self.classifier):
            route_key = await self.classifier(input_data, context)
        else:
            route_key = self.classifier(input_data, context)

        # Get chain for route
        chain = self.routes.get(route_key)
        if chain is None and self.default_route:
            chain = self.routes.get(self.default_route)

        if chain is None:
            raise ValueError(f"No chain found for route: {route_key}")

        # Execute chain
        result = await chain.run(input_data, context=context)
        return result.output if isinstance(result, ChainResult) else result


class RetryChain(Chain):
    """
    Wrap a chain with retry logic.
    """

    def __init__(
        self,
        chain: Chain,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        exponential_backoff: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.chain = chain
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.exponential_backoff = exponential_backoff

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute with retries"""
        last_error = None

        for attempt in range(self.max_retries + 1):
            try:
                result = await self.chain.run(input_data, context=context)
                if result.success:
                    return result.output
                else:
                    last_error = result.error
            except Exception as e:
                last_error = str(e)

            if attempt < self.max_retries:
                delay = self.retry_delay * (2**attempt if self.exponential_backoff else 1)
                logger.warning("Retry %s/%s after %ss: %s", attempt + 1, self.max_retries, delay, last_error)
                await asyncio.sleep(delay)

        raise RuntimeError(f"Chain failed after {self.max_retries} retries: {last_error}")


class CacheChain(Chain):
    """
    Wrap a chain with caching.
    """

    def __init__(
        self,
        chain: Chain,
        cache_key_fn: Callable[[Any], str] | None = None,
        ttl_seconds: int = 3600,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.chain = chain
        self.cache_key_fn = cache_key_fn or (lambda x: str(hash(str(x))))
        self.ttl_seconds = ttl_seconds
        self._cache: dict[str, tuple] = {}

    async def execute(self, input_data: Any, context: ChainContext) -> Any:
        """Execute with caching"""
        cache_key = self.cache_key_fn(input_data)

        # Check cache
        if cache_key in self._cache:
            cached_value, cached_time = self._cache[cache_key]
            if time.time() - cached_time < self.ttl_seconds:
                logger.debug("Cache hit for key: %s", cache_key)
                return cached_value

        # Execute chain
        result = await self.chain.run(input_data, context=context)
        output = result.output if isinstance(result, ChainResult) else result

        # Cache result
        self._cache[cache_key] = (output, time.time())

        return output
