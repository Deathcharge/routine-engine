"""
Helix Chains - Agent Implementations
=====================================

Autonomous agents that can use tools and reason:
- ReActAgent: Reasoning and Acting agent
- PlanAndExecuteAgent: Plan then execute
- ToolCallingAgent: Direct tool calling
"""

import asyncio
import json
import logging
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .chains import ChainContext, ChainStep
from .memory import ConversationMemory, Memory
from .prompts import REACT_TEMPLATE, PromptTemplate
from .tools import Tool

logger = logging.getLogger(__name__)


class AgentStatus(Enum):
    """Agent execution status"""

    IDLE = "idle"
    THINKING = "thinking"
    ACTING = "acting"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class AgentAction:
    """An action taken by an agent"""

    tool: str
    tool_input: dict[str, Any]
    log: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "tool": self.tool,
            "tool_input": self.tool_input,
            "log": self.log,
        }


@dataclass
class AgentStep:
    """A single step in agent execution"""

    thought: str
    action: AgentAction | None = None
    observation: str | None = None
    is_final: bool = False
    final_answer: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "thought": self.thought,
            "action": self.action.to_dict() if self.action else None,
            "observation": self.observation,
            "is_final": self.is_final,
            "final_answer": self.final_answer,
        }


@dataclass
class AgentResult:
    """Result of agent execution"""

    success: bool
    output: Any
    steps: list[AgentStep] = field(default_factory=list)
    error: str | None = None
    execution_time_ms: float = 0
    iterations: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "output": self.output,
            "steps": [s.to_dict() for s in self.steps],
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
            "iterations": self.iterations,
        }


class Agent(ChainStep, ABC):
    """
    Base class for all agents in Helix Chains.

    Agents are autonomous entities that can:
    - Reason about tasks
    - Use tools to gather information
    - Make decisions
    - Execute multi-step plans
    """

    def __init__(
        self,
        name: str = "agent",
        tools: list[Tool] | None = None,
        llm: Any = None,
        memory: Memory = None,
        max_iterations: int = 10,
        verbose: bool = False,
        **kwargs,
    ):
        super().__init__(name)
        self.tools = tools or []
        self.tool_map = {tool.name: tool for tool in self.tools}
        self.llm = llm
        self.memory = memory or ConversationMemory()
        self.max_iterations = max_iterations
        self.verbose = verbose
        self._status = AgentStatus.IDLE

    @property
    def status(self) -> AgentStatus:
        return self._status

    @property
    def tool_names(self) -> list[str]:
        return list(self.tool_map.keys())

    @property
    def tool_descriptions(self) -> str:
        return "\n".join([f"- {tool.name}: {tool.description}" for tool in self.tools])

    def add_tool(self, tool: Tool) -> "Agent":
        """Add a tool to the agent"""
        self.tools.append(tool)
        self.tool_map[tool.name] = tool
        return self

    @abstractmethod
    async def plan(self, input_data: Any, context: ChainContext) -> AgentStep:
        """Plan the next step"""

    async def execute_tool(self, action: AgentAction) -> str:
        """Execute a tool and return observation"""
        tool = self.tool_map.get(action.tool)

        if not tool:
            return f"Error: Unknown tool '{action.tool}'. Available tools: {self.tool_names}"

        try:
            result = await tool.execute(**action.tool_input)
            if result.success:
                return str(result.output)
            else:
                return f"Error: {result.error}"
        except Exception as e:
            return f"Error executing tool: {e!s}"

    async def execute(self, input_data: Any, context: ChainContext) -> AgentResult:
        """Execute the agent"""
        return await self.run(input_data, context)

    async def run(self, input_data: Any, context: ChainContext = None, **kwargs) -> AgentResult:
        """
        Run the agent on the given input.
        """
        start_time = time.time()
        steps = []

        if context is None:
            context = ChainContext(
                chain_id=self.id,
                run_id=str(time.time()),
                variables=kwargs,
            )

        # Add input to memory
        await self.memory.add(str(input_data), role="user")

        self._status = AgentStatus.THINKING

        try:
            # Agent execution loop: plan -> act -> observe -> repeat
            for iteration in range(self.max_iterations):
                if self.verbose:
                    logger.info("[%s] Iteration %s/%s", self.name, iteration + 1, self.max_iterations)

                # Plan the next step
                step = await self.plan(input_data, context)
                steps.append(step)

                # Check if agent has reached a final answer
                if step.is_final:
                    self._status = AgentStatus.IDLE
                    return AgentResult(
                        success=True,
                        output=step.output,
                        steps=steps,
                        error=None,
                        execution_time_ms=(time.time() - start_time) * 1000,
                        iterations=iteration + 1,
                    )

                # Execute the planned action (tool call)
                if step.action:
                    self._status = AgentStatus.ACTING
                    observation = await self.execute_tool(step.action)
                    step.observation = observation

                    # Add observation to memory for next iteration
                    await self.memory.add(
                        f"Action: {step.action.tool}({step.action.tool_input})\nObservation: {observation}",
                        role="assistant",
                    )

                    # Update context with observation
                    context.variables["last_observation"] = observation

                self._status = AgentStatus.THINKING

        except Exception as e:
            self._status = AgentStatus.FAILED
            logger.error("[%s] Agent failed: %s", self.name, e)

            return AgentResult(
                success=False,
                output=None,
                steps=steps,
                error=str(e),
                execution_time_ms=(time.time() - start_time) * 1000,
                iterations=len(steps),
            )

        # Max iterations reached
        self._status = AgentStatus.FAILED
        return AgentResult(
            success=False,
            output=None,
            steps=steps,
            error=f"Max iterations ({self.max_iterations}) reached without finding answer",
            execution_time_ms=(time.time() - start_time) * 1000,
            iterations=self.max_iterations,
        )


class ReActAgent(Agent):
    """
    ReAct (Reasoning and Acting) Agent.

    Uses the ReAct paradigm to interleave reasoning (Thought)
    with acting (Action) and observing (Observation).

    Example:
        agent = ReActAgent(
            tools=[WebSearchTool(), CalculatorTool()],
            llm=my_llm
        )
        result = await agent.run("What is the population of France times 2?")
    """

    def __init__(
        self,
        prompt_template: PromptTemplate = None,
        stop_sequences: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.prompt_template = prompt_template or REACT_TEMPLATE
        self.stop_sequences = stop_sequences or ["\nObservation:", "\n\tObservation:"]

    async def plan(self, input_data: Any, context: ChainContext) -> AgentStep:
        """Plan using ReAct reasoning"""
        # Build prompt
        prompt = self.prompt_template.format(
            tools=self.tool_descriptions,
            tool_names=", ".join(self.tool_names),
            question=str(input_data),
        )

        # Add previous steps to prompt
        for _i, step in enumerate(context.variables.get("steps", [])):
            if step.action:
                prompt += f"\nAction: {step.action.tool}"
                prompt += f"\nAction Input: {json.dumps(step.action.tool_input)}"
            if step.observation:
                prompt += f"\nObservation: {step.observation}"
            prompt += "\nThought:"

        # Call LLM
        if self.llm:
            response = await self._call_llm(prompt)
        else:
            # Fallback for testing
            response = (
                "I need to search for information.\nAction: web_search\nAction Input: {&quot;query&quot;: &quot;"
                + str(input_data)
                + "&quot;}"
            )

        # Parse response
        return self._parse_response(response)

    async def _call_llm(self, prompt: str) -> str:
        """Call the LLM"""
        if asyncio.iscoroutinefunction(self.llm):
            return await self.llm(prompt, stop=self.stop_sequences)
        elif callable(self.llm):
            return self.llm(prompt, stop=self.stop_sequences)
        elif hasattr(self.llm, "generate"):
            return await self.llm.generate(prompt, stop=self.stop_sequences)
        else:
            raise ValueError("Invalid LLM configuration")

    def _parse_response(self, response: str) -> AgentStep:
        """Parse LLM response into AgentStep"""
        # Check for final answer
        if "Final Answer:" in response:
            final_answer = response.split("Final Answer:")[-1].strip()
            thought = response.split("Final Answer:")[0].strip()
            if thought.startswith("Thought:"):
                thought = thought[8:].strip()

            return AgentStep(
                thought=thought,
                is_final=True,
                final_answer=final_answer,
            )

        # Parse thought
        thought = ""
        if "Thought:" in response:
            thought = response.split("Thought:")[-1].split("Action:")[0].strip()
        else:
            thought = response.split("Action:")[0].strip()

        # Parse action
        action = None
        if "Action:" in response:
            action_part = response.split("Action:")[-1]
            action_name = action_part.split("\n")[0].strip()

            # Parse action input
            action_input = {}
            if "Action Input:" in action_part:
                input_str = action_part.split("Action Input:")[-1].strip()
                # Try to parse as JSON
                try:
                    json_match = re.search(r"\{[^}]+\}", input_str)
                    if json_match:
                        action_input = json.loads(json_match.group())
                    else:
                        # Treat as simple string input
                        action_input = {"input": input_str.split("\n")[0].strip()}
                except json.JSONDecodeError:
                    action_input = {"input": input_str.split("\n")[0].strip()}

            action = AgentAction(
                tool=action_name,
                tool_input=action_input,
                log=response,
            )

        return AgentStep(
            thought=thought,
            action=action,
        )


class PlanAndExecuteAgent(Agent):
    """
    Plan-and-Execute Agent.

    First creates a plan, then executes each step.
    Better for complex multi-step tasks.

    Example:
        agent = PlanAndExecuteAgent(
            tools=[...],
            llm=my_llm
        )
        result = await agent.run("Research AI trends and write a summary")
    """

    PLAN_TEMPLATE = PromptTemplate(
        """Create a step-by-step plan to accomplish the following task.
Each step should be a single action that can be executed.

Task: {task}

Available tools:
{tools}

Plan (numbered list):"""
    )

    EXECUTE_TEMPLATE = PromptTemplate(
        """Execute the following step of the plan.

Overall task: {task}
Current step: {step}
Previous results: {previous_results}

Available tools:
{tools}

Think about what tool to use and execute the step.
Thought:"""
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._plan: list[str] = []
        self._current_step: int = 0
        self._results: list[str] = []

    async def plan(self, input_data: Any, context: ChainContext) -> AgentStep:
        """Plan or execute next step"""
        # Create plan if not exists
        if not self._plan:
            self._plan = await self._create_plan(input_data)
            context.set("plan", self._plan)

        # Check if plan is complete
        if self._current_step >= len(self._plan):
            # Synthesize final answer
            final_answer = await self._synthesize_answer(input_data, self._results)
            return AgentStep(
                thought="Plan complete. Synthesizing final answer.",
                is_final=True,
                final_answer=final_answer,
            )

        # Execute current step
        current_step = self._plan[self._current_step]

        # Get action for this step
        action = await self._get_action_for_step(input_data, current_step)

        self._current_step += 1

        return AgentStep(
            thought=f"Executing step {self._current_step}: {current_step}",
            action=action,
        )

    async def _create_plan(self, task: Any) -> list[str]:
        """Create execution plan"""
        prompt = self.PLAN_TEMPLATE.format(
            task=str(task),
            tools=self.tool_descriptions,
        )

        if self.llm:
            response = await self._call_llm(prompt)
        else:
            response = "1. Search for information\n2. Analyze results\n3. Summarize findings"

        # Parse numbered list
        lines = response.strip().split("\n")
        plan = []
        for line in lines:
            # Remove numbering
            cleaned = re.sub(r"^\d+[\.\)]\s*", "", line.strip())
            if cleaned:
                plan.append(cleaned)

        return plan

    async def _get_action_for_step(self, task: Any, step: str) -> AgentAction:
        """Determine action for a plan step"""
        prompt = self.EXECUTE_TEMPLATE.format(
            task=str(task),
            step=step,
            previous_results="\n".join(self._results[-3:]) if self._results else "None",
            tools=self.tool_descriptions,
        )

        if self.llm:
            response = await self._call_llm(prompt)
        else:
            response = f"Action: web_search\nAction Input: {{&quot;query&quot;: &quot;{step}&quot;}}"

        # Parse action from response
        action_name = self.tool_names[0] if self.tool_names else "unknown"
        action_input = {"query": step}

        if "Action:" in response:
            action_part = response.split("Action:")[-1]
            action_name = action_part.split("\n")[0].strip()

            if "Action Input:" in action_part:
                input_str = action_part.split("Action Input:")[-1].strip()
                try:
                    json_match = re.search(r"\{[^}]+\}", input_str)
                    if json_match:
                        action_input = json.loads(json_match.group())
                except Exception as e:
                    logger.debug("Failed to parse action input JSON: %s", e)

        return AgentAction(tool=action_name, tool_input=action_input)

    async def _synthesize_answer(self, task: Any, results: list[str]) -> str:
        """Synthesize final answer from results"""
        if not results:
            return "No results gathered."

        prompt = f"""Based on the following results, provide a comprehensive answer to the task.

Task: {task}

Results:
{chr(10).join(results)}

Final Answer:"""

        if self.llm:
            return await self._call_llm(prompt)
        else:
            return f"Summary of findings: {'; '.join(results[:3])}"

    async def _call_llm(self, prompt: str) -> str:
        """Call the LLM"""
        if asyncio.iscoroutinefunction(self.llm):
            return await self.llm(prompt)
        elif callable(self.llm):
            return self.llm(prompt)
        elif hasattr(self.llm, "generate"):
            return await self.llm.generate(prompt)
        else:
            raise ValueError("Invalid LLM configuration")

    async def execute_tool(self, action: AgentAction) -> str:
        """Execute tool and store result"""
        result = await super().execute_tool(action)
        self._results.append(result)
        return result


class ToolCallingAgent(Agent):
    """
    Agent that uses structured tool calling (function calling).

    Works with LLMs that support function/tool calling natively
    (OpenAI, Anthropic, etc.)

    Example:
        agent = ToolCallingAgent(
            tools=[...],
            llm=openai_client
        )
        result = await agent.run("Calculate 15% tip on $85")
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def plan(self, input_data: Any, context: ChainContext) -> AgentStep:
        """Plan using tool calling"""
        # Build messages
        messages = [
            {
                "role": "system",
                "content": f"You are a helpful assistant with access to tools: {self.tool_names}",
            },
            {"role": "user", "content": str(input_data)},
        ]

        # Add previous observations
        for key, value in context.variables.items():
            if key.startswith("observation_"):
                messages.append({"role": "assistant", "content": f"Tool result: {value}"})

        # Get tool schemas
        tools = [tool.schema.to_openai_function() for tool in self.tools]

        # Call LLM with tools
        if self.llm and hasattr(self.llm, "chat"):
            response = await self.llm.chat(messages, tools=tools)
        else:
            # Fallback
            return AgentStep(
                thought="Processing request",
                is_final=True,
                final_answer=f"Processed: {input_data}",
            )

        # Parse response
        if hasattr(response, "tool_calls") and response.tool_calls:
            tool_call = response.tool_calls[0]
            return AgentStep(
                thought=response.content or "Using tool",
                action=AgentAction(
                    tool=tool_call.function.name,
                    tool_input=json.loads(tool_call.function.arguments),
                ),
            )
        else:
            return AgentStep(
                thought="Completed",
                is_final=True,
                final_answer=response.content,
            )


class ConversationalAgent(Agent):
    """
    Agent optimized for multi-turn conversations.

    Maintains conversation context and can use tools
    when needed during conversation.
    """

    SYSTEM_PROMPT = """You are a helpful AI assistant created by Helix Collective.
You can use tools when needed to help answer questions.

Available tools:
{tools}

When you need to use a tool, respond with:
TOOL: tool_name
INPUT: tool input

Otherwise, just respond naturally to the user."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._conversation: list[dict[str, str]] = []

    async def plan(self, input_data: Any, context: ChainContext) -> AgentStep:
        """Plan conversational response"""
        # Add user message to conversation
        self._conversation.append(
            {
                "role": "user",
                "content": str(input_data),
            }
        )

        # Build prompt
        system = self.SYSTEM_PROMPT.format(tools=self.tool_descriptions)

        messages = [{"role": "system", "content": system}]
        messages.extend(self._conversation)

        # Call LLM
        if self.llm:
            response = await self._call_llm_chat(messages)
        else:
            response = f"I understand you said: {input_data}"

        # Check for tool use
        if "TOOL:" in response:
            tool_match = re.search(r"TOOL:\s*(\w+)", response)
            input_match = re.search(r"INPUT:\s*(.+?)(?:\n|$)", response, re.DOTALL)

            if tool_match:
                tool_name = tool_match.group(1)
                tool_input = input_match.group(1).strip() if input_match else ""

                return AgentStep(
                    thought=response.split("TOOL:")[0].strip(),
                    action=AgentAction(
                        tool=tool_name,
                        tool_input={"input": tool_input},
                    ),
                )

        # Regular response
        self._conversation.append(
            {
                "role": "assistant",
                "content": response,
            }
        )

        return AgentStep(
            thought="Responding to user",
            is_final=True,
            final_answer=response,
        )

    async def _call_llm_chat(self, messages: list[dict[str, str]]) -> str:
        """Call LLM with chat messages"""
        if hasattr(self.llm, "chat"):
            response = await self.llm.chat(messages)
            return response.content if hasattr(response, "content") else str(response)
        elif asyncio.iscoroutinefunction(self.llm):
            prompt = "\n".join([f"{m['role']}: {m['content']}" for m in messages])
            return await self.llm(prompt)
        elif callable(self.llm):
            prompt = "\n".join([f"{m['role']}: {m['content']}" for m in messages])
            return self.llm(prompt)
        else:
            raise ValueError("Invalid LLM configuration")
