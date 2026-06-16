"""
Helix Chains - Prompt Templates
================================

Prompt template system for building dynamic prompts:
- PromptTemplate: Basic variable substitution
- ChatPromptTemplate: Multi-message chat prompts
- FewShotPromptTemplate: Include examples in prompts
- SystemPromptTemplate: System instruction prompts
"""

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Union

from .chains import ChainContext, ChainStep


@dataclass
class Message:
    """A chat message"""

    role: str  # system, user, assistant
    content: str
    name: str | None = None

    def to_dict(self) -> dict[str, str]:
        d = {"role": self.role, "content": self.content}
        if self.name:
            d["name"] = self.name
        return d


PromptExecutionResult = str | list[Message]


class BasePromptTemplate(ChainStep, ABC):
    """Base class for prompt templates"""

    def __init__(
        self,
        template: str,
        input_variables: list[str] | None = None,
        partial_variables: dict[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.template = template
        self.input_variables = input_variables or self._extract_variables(template)
        self.partial_variables = partial_variables or {}

    def _extract_variables(self, template: str) -> list[str]:
        """Extract variable names from template"""
        # Match {variable} patterns
        return list(set(re.findall(r"\{(\w+)\}", template)))

    @abstractmethod
    def format(self, **kwargs) -> str:
        """Format the template with variables"""

    async def execute(self, input_data: Any, context: ChainContext) -> PromptExecutionResult:
        """Execute as chain step"""
        # Merge input data with context variables
        variables = {}

        if isinstance(input_data, dict):
            variables.update(input_data)
        elif input_data is not None:
            variables["input"] = input_data

        variables.update(context.variables)
        variables.update(self.partial_variables)

        return self.format(**variables)


class PromptTemplate(BasePromptTemplate):
    """
    Basic prompt template with variable substitution.

    Example:
        template = PromptTemplate(
            "Summarize the following text about {topic}:\n\n{text}"
        )
        prompt = template.format(topic="AI", text="...")
    """

    def __init__(
        self,
        template: str,
        input_variables: list[str] | None = None,
        partial_variables: dict[str, Any] | None = None,
        validate_template: bool = True,
        **kwargs,
    ):
        super().__init__(template, input_variables, partial_variables, **kwargs)
        self.validate_template = validate_template

    def format(self, **kwargs) -> str:
        """Format template with provided variables"""
        # Merge with partial variables
        all_vars = {**self.partial_variables, **kwargs}

        # Validate required variables
        if self.validate_template:
            missing = set(self.input_variables) - set(all_vars.keys())
            if missing:
                raise ValueError(f"Missing required variables: {missing}")

        # Format template
        result = self.template
        for var, value in all_vars.items():
            result = result.replace(f"{{{var}}}", str(value))

        return result

    def partial(self, **kwargs) -> "PromptTemplate":
        """Create a new template with some variables filled in"""
        new_partial = {**self.partial_variables, **kwargs}
        new_input_vars = [v for v in self.input_variables if v not in kwargs]

        return PromptTemplate(
            template=self.template,
            input_variables=new_input_vars,
            partial_variables=new_partial,
            validate_template=self.validate_template,
        )

    @classmethod
    def from_file(cls, path: str, **kwargs) -> "PromptTemplate":
        """Load template from file"""
        with open(path, encoding="utf-8") as f:
            template = f.read()
        return cls(template=template, **kwargs)

    def __add__(self, other: Union[str, "PromptTemplate"]) -> "PromptTemplate":
        """Concatenate templates"""
        if isinstance(other, str):
            return PromptTemplate(
                template=self.template + other,
                partial_variables=self.partial_variables,
            )
        elif isinstance(other, PromptTemplate):
            return PromptTemplate(
                template=self.template + other.template,
                partial_variables={**self.partial_variables, **other.partial_variables},
            )
        raise TypeError(f"Cannot add PromptTemplate and {type(other)}")


class ChatPromptTemplate(BasePromptTemplate):
    """
    Template for multi-message chat prompts.

    Example:
        template = ChatPromptTemplate([
            SystemPromptTemplate("You are a helpful assistant."),
            ("user", "Hello, {name}!"),
            ("assistant", "Hi {name}! How can I help?"),
            ("user", "{question}")
        ])
        messages = template.format_messages(name="Alice", question="What is AI?")
    """

    def __init__(self, messages: list[Union[tuple, Message, "BasePromptTemplate"]], **kwargs):
        # Build template string from messages
        template_parts = []
        for msg in messages:
            if isinstance(msg, tuple):
                template_parts.append(f"{msg[0]}: {msg[1]}")
            elif isinstance(msg, Message):
                template_parts.append(f"{msg.role}: {msg.content}")
            elif isinstance(msg, BasePromptTemplate):
                template_parts.append(msg.template)

        super().__init__("\n".join(template_parts), **kwargs)
        self.messages = messages

    def format(self, **kwargs) -> str:
        """Format as string"""
        return "\n".join([f"{msg.role}: {msg.content}" for msg in self.format_messages(**kwargs)])

    def format_messages(self, **kwargs) -> list[Message]:
        """Format as list of messages"""
        all_vars = {**self.partial_variables, **kwargs}
        result = []

        for msg in self.messages:
            if isinstance(msg, tuple):
                role, content = msg
                formatted_content = content
                for var, value in all_vars.items():
                    formatted_content = formatted_content.replace(f"{{{var}}}", str(value))
                result.append(Message(role=role, content=formatted_content))

            elif isinstance(msg, Message):
                formatted_content = msg.content
                for var, value in all_vars.items():
                    formatted_content = formatted_content.replace(f"{{{var}}}", str(value))
                result.append(Message(role=msg.role, content=formatted_content, name=msg.name))

            elif isinstance(msg, BasePromptTemplate):
                formatted = msg.format(**all_vars)
                role = "system" if isinstance(msg, SystemPromptTemplate) else "user"
                result.append(Message(role=role, content=formatted))

        return result

    def to_openai_messages(self, **kwargs) -> list[dict[str, Any]]:
        """Format for OpenAI API"""
        return [msg.to_dict() for msg in self.format_messages(**kwargs)]

    async def execute(self, input_data: Any, context: ChainContext) -> list[Message]:
        """Execute as chain step, returning messages"""
        variables = {}

        if isinstance(input_data, dict):
            variables.update(input_data)
        elif input_data is not None:
            variables["input"] = input_data

        variables.update(context.variables)
        variables.update(self.partial_variables)

        return self.format_messages(**variables)


class SystemPromptTemplate(PromptTemplate):
    """
    Template specifically for system prompts.

    Example:
        system = SystemPromptTemplate(
            "You are {role}. Your expertise is in {domain}."
        )
    """

    def __init__(self, template: str, **kwargs):
        super().__init__(template, **kwargs)
        self.role = "system"

    def to_message(self, **kwargs) -> Message:
        """Convert to Message object"""
        return Message(role="system", content=self.format(**kwargs))


class FewShotPromptTemplate(BasePromptTemplate):
    """
    Template that includes examples for few-shot learning.

    Example:
        template = FewShotPromptTemplate(
            examples=[
                {"input": "2+2", "output": "4"},
                {"input": "3*3", "output": "9"},
            ],
            example_template=PromptTemplate("Input: {input}\nOutput: {output}"),
            prefix="Solve the following math problems:",
            suffix="Input: {input}\nOutput:",
            input_variables=["input"]
        )
    """

    def __init__(
        self,
        examples: list[dict[str, Any]],
        example_template: PromptTemplate,
        prefix: str = "",
        suffix: str = "",
        example_separator: str = "\n\n",
        input_variables: list[str] | None = None,
        **kwargs,
    ):
        self.examples = examples
        self.example_template = example_template
        self.prefix = prefix
        self.suffix = suffix
        self.example_separator = example_separator

        # Build full template
        template = self._build_template()
        super().__init__(template, input_variables, **kwargs)

    def _build_template(self) -> str:
        """Build the full template with examples"""
        parts = []

        if self.prefix:
            parts.append(self.prefix)

        # Format examples
        example_strings = []
        for example in self.examples:
            example_strings.append(self.example_template.format(**example))

        parts.append(self.example_separator.join(example_strings))

        if self.suffix:
            parts.append(self.suffix)

        return self.example_separator.join(parts)

    def format(self, **kwargs) -> str:
        """Format the template"""
        all_vars = {**self.partial_variables, **kwargs}

        result = self._build_template()
        for var, value in all_vars.items():
            result = result.replace(f"{{{var}}}", str(value))

        return result

    def add_example(self, example: dict[str, Any]) -> None:
        """Add a new example"""
        self.examples.append(example)
        self.template = self._build_template()


class ConditionalPromptTemplate(BasePromptTemplate):
    """
    Template that selects content based on conditions.

    Example:
        template = ConditionalPromptTemplate(
            conditions=[
                (lambda x: x.get("type") == "question", "Answer this question: {input}"),
                (lambda x: x.get("type") == "task", "Complete this task: {input}"),
            ],
            default="Process this: {input}"
        )
    """

    def __init__(
        self,
        conditions: list[tuple],  # List of (condition_fn, template_str)
        default: str = "{input}",
        **kwargs,
    ):
        self.conditions = conditions
        self.default = default

        # Collect all variables
        all_vars = set()
        for _, template in conditions:
            all_vars.update(re.findall(r"\{(\w+)\}", template))
        all_vars.update(re.findall(r"\{(\w+)\}", default))

        super().__init__(default, list(all_vars), **kwargs)

    def format(self, **kwargs) -> str:
        """Format based on conditions"""
        all_vars = {**self.partial_variables, **kwargs}

        # Find matching condition
        template = self.default
        for condition_fn, template_str in self.conditions:
            if condition_fn(all_vars):
                template = template_str
                break

        # Format template
        result = template
        for var, value in all_vars.items():
            result = result.replace(f"{{{var}}}", str(value))

        return result


class PipelinePromptTemplate(BasePromptTemplate):
    """
    Compose multiple prompts into a pipeline.

    Example:
        pipeline = PipelinePromptTemplate([
            ("intro", PromptTemplate("Topic: {topic}")),
            ("body", PromptTemplate("Details: {details}")),
            ("conclusion", PromptTemplate("Summary: {summary}")),
        ])
    """

    def __init__(
        self,
        pipeline: list[tuple],  # List of (name, template)
        separator: str = "\n\n",
        **kwargs,
    ):
        self.pipeline = pipeline
        self.separator = separator

        # Collect all variables
        all_vars = set()
        for _, template in pipeline:
            all_vars.update(template.input_variables)

        # Build combined template
        combined = separator.join([t.template for _, t in pipeline])
        super().__init__(combined, list(all_vars), **kwargs)

    def format(self, **kwargs) -> str:
        """Format all templates in pipeline"""
        all_vars = {**self.partial_variables, **kwargs}

        parts = []
        for _name, template in self.pipeline:
            parts.append(template.format(**all_vars))

        return self.separator.join(parts)

    def format_dict(self, **kwargs) -> dict[str, str]:
        """Format and return as dictionary"""
        all_vars = {**self.partial_variables, **kwargs}

        return {name: template.format(**all_vars) for name, template in self.pipeline}


# ============================================================================
# COMMON PROMPT TEMPLATES
# ============================================================================

# Summarization
SUMMARIZE_TEMPLATE = PromptTemplate(
    """Summarize the following text in {style} style:

Text:
{text}

Summary:"""
)

# Question Answering
QA_TEMPLATE = PromptTemplate(
    """Answer the following question based on the context provided.

Context:
{context}

Question: {question}

Answer:"""
)

# Code Generation
CODE_TEMPLATE = PromptTemplate(
    """Write {language} code to accomplish the following task:

Task: {task}

Requirements:
{requirements}

Code:"""
)

# Translation
TRANSLATE_TEMPLATE = PromptTemplate(
    """Translate the following text from {source_language} to {target_language}:

Text:
{text}

Translation:"""
)

# Classification
CLASSIFY_TEMPLATE = PromptTemplate(
    """Classify the following text into one of these categories: {categories}

Text:
{text}

Category:"""
)

# Extraction
EXTRACT_TEMPLATE = PromptTemplate(
    """Extract the following information from the text:
{fields}

Text:
{text}

Extracted Information:"""
)

# Chat
CHAT_SYSTEM_TEMPLATE = SystemPromptTemplate(
    """You are {assistant_name}, a helpful AI assistant created by Helix Collective.

Your capabilities include:
{capabilities}

Your personality:
{personality}

Always be helpful, accurate, and respectful."""
)

# ReAct Agent
REACT_TEMPLATE = PromptTemplate(
    """You are an AI assistant that can use tools to help answer questions.

Available tools:
{tools}

Use the following format:

Question: the input question you must answer
Thought: think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {question}
Thought:"""
)
