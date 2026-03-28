"""
Helix Spirals Advanced Node Types
=================================
Extended node types for the Helix Spirals workflow automation system.
These nodes provide Zapier-killer capabilities with coordination awareness.

Node Categories:
- HTTP/API nodes
- Database nodes
- AI/Agent nodes
- Code execution nodes
- File operation nodes
- Communication nodes (Email, Slack, Discord)
- Data transformation nodes
- Control flow nodes
"""

import asyncio
import json
import logging
import os
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from apps.backend.agent_capabilities.execution_engine import execute_code
from apps.backend.utils.safe_eval import SafeEvaluator

logger = logging.getLogger(__name__)

try:
    import aiohttp

    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

try:
    import aiosmtplib

    HAS_AIOSMTPLIB = True
except ImportError:
    HAS_AIOSMTPLIB = False

try:
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    HAS_EMAIL = True
except ImportError:
    HAS_EMAIL = False

try:
    from apps.backend.routes.agent_capabilities_api import get_llm_response
except ImportError:

    async def get_llm_response(*args, **kwargs):
        raise ImportError("LLM service not available")


class NodeCategory(Enum):
    """Categories of workflow nodes"""

    TRIGGER = "trigger"
    ACTION = "action"
    CONDITION = "condition"
    TRANSFORM = "transform"
    INTEGRATION = "integration"
    AI = "ai"
    CONTROL = "control"
    DATA = "data"


class NodeStatus(Enum):
    """Execution status of a node"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class NodeInput:
    """Input definition for a node"""

    name: str
    type: str  # string, number, boolean, object, array, any
    description: str
    required: bool = True
    default: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "required": self.required,
            "default": self.default,
        }


@dataclass
class NodeOutput:
    """Output definition for a node"""

    name: str
    type: str
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type, "description": self.description}


@dataclass
class NodeResult:
    """Result of node execution"""

    success: bool
    data: Any = None
    error: str | None = None
    execution_time_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
            "metadata": self.metadata,
        }


class BaseNode(ABC):
    """Base class for all workflow nodes"""

    name: str = "base_node"
    display_name: str = "Base Node"
    description: str = "Base node class"
    category: NodeCategory = NodeCategory.ACTION
    icon: str = "⚙️"

    inputs: list[NodeInput] = []
    outputs: list[NodeOutput] = []

    def __init__(self, node_id: str | None = None):
        self.node_id = node_id or str(uuid.uuid4())
        self.status = NodeStatus.PENDING
        self.result: NodeResult | None = None

    @abstractmethod
    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        """Execute the node with given inputs"""

    def validate_inputs(self, inputs: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate inputs against node definition"""
        errors = []
        for input_def in self.inputs:
            if input_def.required and input_def.name not in inputs:
                if input_def.default is None:
                    errors.append(f"Missing required input: {input_def.name}")
        return len(errors) == 0, errors

    def get_schema(self) -> dict[str, Any]:
        """Get node schema for UI"""
        return {
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "category": self.category.value,
            "icon": self.icon,
            "inputs": [i.to_dict() for i in self.inputs],
            "outputs": [o.to_dict() for o in self.outputs],
        }


# ==================== HTTP/API NODES ====================


class HTTPRequestNode(BaseNode):
    """Make HTTP requests to external APIs"""

    name = "http_request"
    display_name = "HTTP Request"
    description = "Make HTTP requests to external APIs"
    category = NodeCategory.INTEGRATION
    icon = "🌐"

    inputs = [
        NodeInput("url", "string", "URL to request", required=True),
        NodeInput(
            "method",
            "string",
            "HTTP method (GET, POST, PUT, DELETE, PATCH)",
            default="GET",
        ),
        NodeInput("headers", "object", "Request headers", required=False),
        NodeInput("body", "any", "Request body (for POST/PUT/PATCH)", required=False),
        NodeInput("query_params", "object", "Query parameters", required=False),
        NodeInput("timeout", "number", "Timeout in seconds", default=30),
        NodeInput(
            "auth_type",
            "string",
            "Authentication type (none, basic, bearer)",
            default="none",
        ),
        NodeInput("auth_value", "string", "Authentication value", required=False),
    ]

    outputs = [
        NodeOutput("response", "object", "Response data"),
        NodeOutput("status_code", "number", "HTTP status code"),
        NodeOutput("headers", "object", "Response headers"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        if not HAS_AIOHTTP:
            return NodeResult(success=False, error="aiohttp not installed")

        start_time = datetime.now(UTC)

        url = inputs.get("url")
        method = inputs.get("method", "GET").upper()
        headers = inputs.get("headers", {})
        body = inputs.get("body")
        query_params = inputs.get("query_params")
        timeout = inputs.get("timeout", 30)
        auth_type = inputs.get("auth_type", "none")
        auth_value = inputs.get("auth_value")

        # Add authentication
        if auth_type == "bearer" and auth_value:
            headers["Authorization"] = f"Bearer {auth_value}"
        elif auth_type == "basic" and auth_value:
            import base64

            headers["Authorization"] = f"Basic {base64.b64encode(auth_value.encode()).decode()}"

        try:
            kwargs = {
                "headers": headers,
                "timeout": aiohttp.ClientTimeout(total=timeout),
            }

            if query_params:
                kwargs["params"] = query_params

            if body and method in ("POST", "PUT", "PATCH"):
                if isinstance(body, dict):
                    kwargs["json"] = body
                else:
                    kwargs["data"] = body

            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, **kwargs) as response:
                    try:
                        response_data = await response.json()
                    except (json.JSONDecodeError, ValueError, TypeError):
                        response_data = await response.text()

                    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

                    return NodeResult(
                        success=200 <= response.status < 300,
                        data={
                            "response": response_data,
                            "status_code": response.status,
                            "headers": dict(response.headers),
                        },
                        execution_time_ms=execution_time,
                        metadata={"url": url, "method": method},
                    )
        except (aiohttp.ClientError, ConnectionError, TimeoutError) as e:
            return NodeResult(success=False, error=f"HTTP client error: {e!s}")
        except (ValueError, TypeError) as e:
            return NodeResult(success=False, error=f"Request validation error: {e!s}")
        except Exception as e:
            logger.exception("Unexpected error in HTTP request node")
            return NodeResult(success=False, error=str(e))


class WebhookTriggerNode(BaseNode):
    """Trigger workflow from incoming webhook"""

    name = "webhook_trigger"
    display_name = "Webhook Trigger"
    description = "Trigger workflow when a webhook is received"
    category = NodeCategory.TRIGGER
    icon = "🔔"

    inputs = [
        NodeInput("webhook_path", "string", "Webhook path (e.g., /my-webhook)", required=True),
        NodeInput("method", "string", "Expected HTTP method", default="POST"),
        NodeInput("secret", "string", "Webhook secret for validation", required=False),
    ]

    outputs = [
        NodeOutput("body", "any", "Webhook request body"),
        NodeOutput("headers", "object", "Request headers"),
        NodeOutput("query", "object", "Query parameters"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        # This node is handled by the webhook receiver
        # The context should contain the webhook data
        webhook_data = context.get("webhook_data", {})

        return NodeResult(
            success=True,
            data={
                "body": webhook_data.get("body"),
                "headers": webhook_data.get("headers", {}),
                "query": webhook_data.get("query", {}),
            },
        )


class WebhookResponseNode(BaseNode):
    """Send response to webhook caller"""

    name = "webhook_response"
    display_name = "Webhook Response"
    description = "Send a response back to the webhook caller"
    category = NodeCategory.ACTION
    icon = "📤"

    inputs = [
        NodeInput("status_code", "number", "HTTP status code", default=200),
        NodeInput("body", "any", "Response body"),
        NodeInput("headers", "object", "Response headers", required=False),
    ]

    outputs = [
        NodeOutput("sent", "boolean", "Whether response was sent"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        # Store response in context for the webhook handler
        context["webhook_response"] = {
            "status_code": inputs.get("status_code", 200),
            "body": inputs.get("body"),
            "headers": inputs.get("headers", {}),
        }

        return NodeResult(success=True, data={"sent": True})


# ==================== AI/AGENT NODES ====================


class AIAgentNode(BaseNode):
    """Invoke a Helix AI agent"""

    name = "ai_agent"
    display_name = "AI Agent"
    description = "Invoke a Helix Collective AI agent"
    category = NodeCategory.AI
    icon = "🤖"

    inputs = [
        NodeInput("agent", "string", "Agent name (nexus, oracle, cipher, etc.)", required=True),
        NodeInput("prompt", "string", "Prompt for the agent", required=True),
        NodeInput("context", "string", "Additional context", required=False),
        NodeInput("max_tokens", "number", "Maximum response tokens", default=1000),
        NodeInput("temperature", "number", "Response creativity (0-1)", default=0.7),
    ]

    outputs = [
        NodeOutput("response", "string", "Agent response"),
        NodeOutput("agent_name", "string", "Name of the agent that responded"),
        NodeOutput("tokens_used", "number", "Tokens used in response"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        start_time = datetime.now(UTC)

        agent = inputs.get("agent", "nexus")
        prompt = inputs.get("prompt", "")
        additional_context = inputs.get("context", "")
        max_tokens = inputs.get("max_tokens", 1000)
        temperature = inputs.get("temperature", 0.7)

        # Enrich with coordination core insight
        coordination_context = ""
        try:
            from apps.backend.coordination.coordination_hub import get_coordination_hub

            hub = get_coordination_hub()
            coordination = hub.get_coordination(agent.lower())
            if coordination is not None:
                if hasattr(coordination, "handle_command"):
                    import asyncio

                    cmd_ctx = {"message": prompt[:500], "source": "spiral"}
                    if asyncio.iscoroutinefunction(coordination.handle_command):
                        result = await coordination.handle_command("analyze", cmd_ctx)
                    else:
                        result = coordination.handle_command("analyze", cmd_ctx)
                    if result and not result.get("error"):
                        insight = result.get("analysis", result.get("result", ""))
                        if insight and len(str(insight)) > 10:
                            coordination_context = f" Domain insight: {str(insight)[:400]}"
        except Exception as e:
            logger.debug("Coordination enrichment failed (best-effort): %s", e)

        # Try to use LLM service
        try:
            system_prompt = (
                f"You are {agent.title()}, a Helix Collective AI agent. {additional_context}{coordination_context}"
            )
            response = await get_llm_response(
                prompt,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
            )

            execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

            return NodeResult(
                success=True,
                data={
                    "response": response,
                    "agent_name": agent,
                    "tokens_used": len(response) // 4,  # Rough estimate
                },
                execution_time_ms=execution_time,
            )
        except ImportError:
            # Fallback response
            return NodeResult(
                success=True,
                data={
                    "response": f"[{agent.title()}]: I received your message: '{prompt[:100]}...' (LLM service not available)",
                    "agent_name": agent,
                    "tokens_used": 0,
                },
            )
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class TextGenerationNode(BaseNode):
    """Generate text using AI"""

    name = "text_generation"
    display_name = "Text Generation"
    description = "Generate text using AI models"
    category = NodeCategory.AI
    icon = "✍️"

    inputs = [
        NodeInput("prompt", "string", "Generation prompt", required=True),
        NodeInput(
            "style",
            "string",
            "Writing style (formal, casual, technical, creative)",
            default="casual",
        ),
        NodeInput("length", "string", "Output length (short, medium, long)", default="medium"),
        NodeInput("format", "string", "Output format (text, markdown, json)", default="text"),
    ]

    outputs = [
        NodeOutput("text", "string", "Generated text"),
        NodeOutput("word_count", "number", "Word count"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        prompt = inputs.get("prompt", "")
        style = inputs.get("style", "casual")
        length = inputs.get("length", "medium")
        output_format = inputs.get("format", "text")

        length_tokens = {"short": 100, "medium": 300, "long": 1000}
        max_tokens = length_tokens.get(length, 300)

        try:
            system_prompt = f"Generate {style} text in {output_format} format. Be concise and relevant."
            response = await get_llm_response(prompt, system_prompt=system_prompt, max_tokens=max_tokens)

            return NodeResult(
                success=True,
                data={"text": response, "word_count": len(response.split())},
            )
        except Exception as e:
            logger.debug("LLM unavailable for text generation: %s", e)
            return NodeResult(
                success=True,
                data={
                    "text": f"Generated response for: {prompt[:100]}... (LLM not available)",
                    "word_count": 10,
                },
            )


class SentimentAnalysisNode(BaseNode):
    """Analyze sentiment of text"""

    name = "sentiment_analysis"
    display_name = "Sentiment Analysis"
    description = "Analyze the sentiment of text"
    category = NodeCategory.AI
    icon = "😊"

    inputs = [
        NodeInput("text", "string", "Text to analyze", required=True),
    ]

    outputs = [
        NodeOutput("sentiment", "string", "Sentiment (positive, negative, neutral)"),
        NodeOutput("score", "number", "Sentiment score (-1 to 1)"),
        NodeOutput("confidence", "number", "Confidence level (0-1)"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        text = inputs.get("text", "")

        # Simple keyword-based sentiment (fallback)
        positive_words = [
            "good",
            "great",
            "excellent",
            "amazing",
            "wonderful",
            "love",
            "happy",
            "best",
        ]
        negative_words = [
            "bad",
            "terrible",
            "awful",
            "hate",
            "worst",
            "horrible",
            "sad",
            "angry",
        ]

        text_lower = text.lower()
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)

        total = positive_count + negative_count
        if total == 0:
            sentiment = "neutral"
            score = 0.0
        elif positive_count > negative_count:
            sentiment = "positive"
            score = positive_count / total
        else:
            sentiment = "negative"
            score = -negative_count / total

        return NodeResult(
            success=True,
            data={
                "sentiment": sentiment,
                "score": score,
                "confidence": 0.7 if total > 0 else 0.5,
            },
        )


# ==================== CODE EXECUTION NODES ====================


class CodeExecutionNode(BaseNode):
    """Execute code in a sandboxed environment"""

    name = "code_execution"
    display_name = "Code Execution"
    description = "Execute Python, JavaScript, or Shell code"
    category = NodeCategory.ACTION
    icon = "💻"

    inputs = [
        NodeInput("code", "string", "Code to execute", required=True),
        NodeInput(
            "language",
            "string",
            "Programming language (python, javascript, shell)",
            default="python",
        ),
        NodeInput("timeout", "number", "Execution timeout in seconds", default=30),
        NodeInput("variables", "object", "Variables to inject", required=False),
    ]

    outputs = [
        NodeOutput("output", "string", "Execution output"),
        NodeOutput("return_value", "any", "Return value"),
        NodeOutput("error", "string", "Error message if failed"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            result = await execute_code(
                agent_id=f"spiral_{context.get('workflow_id', 'unknown')}",
                code=inputs.get("code", ""),
                language=inputs.get("language", "python"),
            )

            return NodeResult(
                success=result.success,
                data={
                    "output": result.output,
                    "return_value": result.return_value,
                    "error": result.error,
                },
                execution_time_ms=result.execution_time_ms,
            )
        except ImportError:
            return NodeResult(success=False, error="Code execution not available")


# ==================== DATA TRANSFORMATION NODES ====================


class JSONTransformNode(BaseNode):
    """Transform JSON data using JSONPath or JMESPath"""

    name = "json_transform"
    display_name = "JSON Transform"
    description = "Transform JSON data using expressions"
    category = NodeCategory.TRANSFORM
    icon = "🔄"

    inputs = [
        NodeInput("data", "any", "Input data", required=True),
        NodeInput("expression", "string", "Transformation expression", required=True),
        NodeInput(
            "output_type",
            "string",
            "Output type (auto, string, number, boolean, array, object)",
            default="auto",
        ),
    ]

    outputs = [
        NodeOutput("result", "any", "Transformed data"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        data = inputs.get("data")
        expression = inputs.get("expression", "")

        try:
            if expression.startswith("$."):
                path = expression[2:].split(".")
                result = data
                for key in path:
                    if isinstance(result, dict):
                        result = result.get(key)
                    elif isinstance(result, list) and key.isdigit():
                        result = result[int(key)]
                    else:
                        result = None
                        break
            else:
                # Use SafeEvaluator for secure expression evaluation
                evaluator = SafeEvaluator(allowed_names={"data": data, "json": json})
                result = evaluator.eval(expression)

            return NodeResult(success=True, data={"result": result})
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class FilterNode(BaseNode):
    """Filter array data based on conditions"""

    name = "filter"
    display_name = "Filter"
    description = "Filter array items based on conditions"
    category = NodeCategory.TRANSFORM
    icon = "🔍"

    inputs = [
        NodeInput("array", "array", "Array to filter", required=True),
        NodeInput(
            "condition",
            "string",
            "Filter condition (e.g., 'item.status == &quot;active&quot;')",
            required=True,
        ),
    ]

    outputs = [
        NodeOutput("filtered", "array", "Filtered array"),
        NodeOutput("count", "number", "Number of items after filtering"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        array = inputs.get("array", [])
        condition = inputs.get("condition", "True")

        try:
            filtered = []
            for item in array:
                try:
                    # Use SafeEvaluator for secure condition evaluation
                    evaluator = SafeEvaluator(allowed_names={"item": item, "data": context})
                    should_include = evaluator.eval(condition)
                    if should_include:
                        filtered.append(item)
                except Exception as e:
                    logger.warning("Filter eval failed for item: %s", e)

            return NodeResult(success=True, data={"filtered": filtered, "count": len(filtered)})
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class MapNode(BaseNode):
    """Transform each item in an array"""

    name = "map"
    display_name = "Map"
    description = "Transform each item in an array"
    category = NodeCategory.TRANSFORM
    icon = "🗺️"

    inputs = [
        NodeInput("array", "array", "Array to transform", required=True),
        NodeInput(
            "expression",
            "string",
            "Transformation expression (e.g., 'item.name.upper()')",
            required=True,
        ),
    ]

    outputs = [
        NodeOutput("mapped", "array", "Transformed array"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        array = inputs.get("array", [])
        expression = inputs.get("expression", "item")

        try:
            mapped = []
            for item in array:
                try:
                    evaluator = SafeEvaluator(allowed_names={"item": item, "json": json})
                    result = evaluator.eval(expression)
                    mapped.append(result)
                except Exception:
                    mapped.append(item)

            return NodeResult(success=True, data={"mapped": mapped})
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ==================== CONTROL FLOW NODES ====================


class ConditionNode(BaseNode):
    """Conditional branching based on expression"""

    name = "condition"
    display_name = "Condition"
    description = "Branch workflow based on a condition"
    category = NodeCategory.CONTROL
    icon = "❓"

    inputs = [
        NodeInput("condition", "string", "Condition expression", required=True),
        NodeInput("value", "any", "Value to evaluate", required=False),
    ]

    outputs = [
        NodeOutput("result", "boolean", "Condition result"),
        NodeOutput("true_branch", "any", "Output if true"),
        NodeOutput("false_branch", "any", "Output if false"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        condition = inputs.get("condition", "False")
        value = inputs.get("value")

        try:
            evaluator = SafeEvaluator(allowed_names={"data": context, "value": value})
            result_bool = evaluator.eval(condition)
            result = bool(result_bool)

            return NodeResult(
                success=True,
                data={
                    "result": result,
                    "true_branch": value if result else None,
                    "false_branch": None if result else value,
                },
            )
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class LoopNode(BaseNode):
    """Loop over array items"""

    name = "loop"
    display_name = "Loop"
    description = "Execute nodes for each item in an array"
    category = NodeCategory.CONTROL
    icon = "🔁"

    inputs = [
        NodeInput("array", "array", "Array to iterate", required=True),
        NodeInput("max_iterations", "number", "Maximum iterations", default=100),
    ]

    outputs = [
        NodeOutput("current_item", "any", "Current item in iteration"),
        NodeOutput("index", "number", "Current index"),
        NodeOutput("is_first", "boolean", "Is first iteration"),
        NodeOutput("is_last", "boolean", "Is last iteration"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        array = inputs.get("array", [])
        max_iterations = inputs.get("max_iterations", 100)

        # This node sets up iteration context
        # The actual looping is handled by the workflow engine
        context["loop_array"] = array[:max_iterations]
        context["loop_index"] = 0

        if array:
            return NodeResult(
                success=True,
                data={
                    "current_item": array[0],
                    "index": 0,
                    "is_first": True,
                    "is_last": len(array) == 1,
                },
            )
        else:
            return NodeResult(
                success=True,
                data={
                    "current_item": None,
                    "index": -1,
                    "is_first": False,
                    "is_last": True,
                },
            )


class DelayNode(BaseNode):
    """Add a delay in the workflow"""

    name = "delay"
    display_name = "Delay"
    description = "Wait for a specified duration"
    category = NodeCategory.CONTROL
    icon = "⏱️"

    inputs = [
        NodeInput("seconds", "number", "Delay in seconds", required=True),
    ]

    outputs = [
        NodeOutput("waited", "number", "Actual wait time in seconds"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        seconds = min(inputs.get("seconds", 1), 300)  # Max 5 minutes

        start = datetime.now(UTC)
        await asyncio.sleep(seconds)
        actual = (datetime.now(UTC) - start).total_seconds()

        return NodeResult(success=True, data={"waited": actual}, execution_time_ms=actual * 1000)


# ==================== COMMUNICATION NODES ====================


class EmailNode(BaseNode):
    """Send email"""

    name = "email"
    display_name = "Send Email"
    description = "Send an email message"
    category = NodeCategory.INTEGRATION
    icon = "📧"

    inputs = [
        NodeInput("to", "string", "Recipient email address", required=True),
        NodeInput("subject", "string", "Email subject", required=True),
        NodeInput("body", "string", "Email body", required=True),
        NodeInput("html", "boolean", "Send as HTML", default=False),
        NodeInput("cc", "string", "CC recipients (comma-separated)", required=False),
        NodeInput("bcc", "string", "BCC recipients (comma-separated)", required=False),
    ]

    outputs = [
        NodeOutput("sent", "boolean", "Whether email was sent"),
        NodeOutput("message_id", "string", "Message ID"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        if not HAS_EMAIL:
            return NodeResult(success=False, error="Email support not installed")

        smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER")
        smtp_pass = os.getenv("SMTP_PASSWORD")

        if not smtp_user or not smtp_pass:
            return NodeResult(success=False, error="SMTP credentials not configured")

        try:
            # Sanitize email header fields to prevent header injection
            def _sanitize_header(value: str) -> str:
                if not value:
                    return value
                return value.replace("\r", "").replace("\n", "").replace("\0", "")

            to_addr = _sanitize_header(inputs.get("to", ""))
            subject = _sanitize_header(inputs.get("subject", ""))
            cc_addr = _sanitize_header(inputs.get("cc", ""))
            bcc_addr = _sanitize_header(inputs.get("bcc", ""))

            if not to_addr:
                return NodeResult(success=False, error="Recipient email address is required")

            msg = MIMEMultipart()
            msg["From"] = smtp_user
            msg["To"] = to_addr
            msg["Subject"] = subject

            if cc_addr:
                msg["Cc"] = cc_addr
            if bcc_addr:
                msg["Bcc"] = bcc_addr

            body = inputs.get("body", "")
            subtype = "html" if inputs.get("html") else "plain"
            msg.attach(MIMEText(body, subtype))

            await aiosmtplib.send(
                msg,
                hostname=smtp_host,
                port=smtp_port,
                username=smtp_user,
                password=smtp_pass,
                start_tls=True,
            )

            return NodeResult(success=True, data={"sent": True, "message_id": msg["Message-ID"]})
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class DiscordWebhookNode(BaseNode):
    """Send message to Discord webhook"""

    name = "discord_webhook"
    display_name = "Discord Webhook"
    description = "Send a message to a Discord channel via webhook"
    category = NodeCategory.INTEGRATION
    icon = "💬"

    inputs = [
        NodeInput("webhook_url", "string", "Discord webhook URL", required=True),
        NodeInput("content", "string", "Message content", required=False),
        NodeInput("username", "string", "Bot username", required=False),
        NodeInput("avatar_url", "string", "Bot avatar URL", required=False),
        NodeInput("embed_title", "string", "Embed title", required=False),
        NodeInput("embed_description", "string", "Embed description", required=False),
        NodeInput("embed_color", "number", "Embed color (decimal)", required=False),
    ]

    outputs = [
        NodeOutput("sent", "boolean", "Whether message was sent"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        if not HAS_AIOHTTP:
            return NodeResult(success=False, error="aiohttp not installed")

        webhook_url = inputs.get("webhook_url")

        payload = {}
        if inputs.get("content"):
            payload["content"] = inputs.get("content")
        if inputs.get("username"):
            payload["username"] = inputs.get("username")
        if inputs.get("avatar_url"):
            payload["avatar_url"] = inputs.get("avatar_url")

        # Build embed if provided
        if inputs.get("embed_title") or inputs.get("embed_description"):
            embed = {}
            if inputs.get("embed_title"):
                embed["title"] = inputs.get("embed_title")
            if inputs.get("embed_description"):
                embed["description"] = inputs.get("embed_description")
            if inputs.get("embed_color"):
                embed["color"] = inputs.get("embed_color")
            payload["embeds"] = [embed]

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=payload) as response:
                    return NodeResult(
                        success=response.status in (200, 204),
                        data={"sent": response.status in (200, 204)},
                    )
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class SlackWebhookNode(BaseNode):
    """Send message to Slack webhook"""

    name = "slack_webhook"
    display_name = "Slack Webhook"
    description = "Send a message to Slack via webhook"
    category = NodeCategory.INTEGRATION
    icon = "💼"

    inputs = [
        NodeInput("webhook_url", "string", "Slack webhook URL", required=True),
        NodeInput("text", "string", "Message text", required=True),
        NodeInput("channel", "string", "Channel override", required=False),
        NodeInput("username", "string", "Bot username", required=False),
        NodeInput("icon_emoji", "string", "Bot icon emoji", required=False),
    ]

    outputs = [
        NodeOutput("sent", "boolean", "Whether message was sent"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        if not HAS_AIOHTTP:
            return NodeResult(success=False, error="aiohttp not installed")

        webhook_url = inputs.get("webhook_url")

        payload = {"text": inputs.get("text")}
        if inputs.get("channel"):
            payload["channel"] = inputs.get("channel")
        if inputs.get("username"):
            payload["username"] = inputs.get("username")
        if inputs.get("icon_emoji"):
            payload["icon_emoji"] = inputs.get("icon_emoji")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=payload) as response:
                    return NodeResult(
                        success=response.status == 200,
                        data={"sent": response.status == 200},
                    )
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ==================== INTEGRATION DISPATCHER ====================


class IntegrationNode(BaseNode):
    """Dispatcher node for named integrations (slack, gmail, notion, github, etc.).

    The Spiral Builder UI creates nodes with type="integration" and a config dict
    that carries:
      - ``integration``: service name (e.g. "slack", "gmail", "notion", "github")
      - ``action``:       action to perform (e.g. "send_message", "create_page")
      - Plus service-specific fields (webhook_url, channel, to, subject, …)

    This node reads those fields and dispatches to the appropriate implementation.
    """

    name = "integration"
    display_name = "Integration"
    description = "Route to a named third-party integration (Slack, Gmail, Notion, GitHub…)"
    category = NodeCategory.INTEGRATION
    icon = "🔌"

    inputs = [
        NodeInput("integration", "string", "Integration name (slack|gmail|notion|github|generic)", required=True),
        NodeInput("action", "string", "Action to perform", required=False),
        # Slack / Discord common
        NodeInput("webhook_url", "string", "Incoming webhook URL", required=False),
        NodeInput("channel", "string", "Channel or room", required=False),
        NodeInput("text", "string", "Message text", required=False),
        NodeInput("message", "string", "Message text (alias for text)", required=False),
        # Email / Gmail
        NodeInput("to", "string", "Recipient email(s), comma-separated", required=False),
        NodeInput("subject", "string", "Email subject", required=False),
        NodeInput("body", "string", "Email body (plain text or HTML)", required=False),
        # Generic / pass-through
        NodeInput("url", "string", "Target URL for HTTP POST", required=False),
        NodeInput("payload", "object", "Arbitrary JSON payload", required=False),
    ]

    outputs = [
        NodeOutput("success", "boolean", "Whether the action succeeded"),
        NodeOutput("result", "object", "Response data from the integration"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        integration = (inputs.get("integration") or "").lower().strip()

        if integration == "slack":
            return await self._dispatch_slack(inputs)
        elif integration in ("gmail", "email"):
            return await self._dispatch_email(inputs)
        elif integration == "discord":
            return await self._dispatch_discord(inputs)
        elif integration == "notion":
            # Notion requires an OAuth token — fall back to HTTP if not configured
            return await self._dispatch_generic_http(inputs)
        elif integration in ("github", "google_sheets", "airtable", "stripe"):
            # These all flow through Composio or direct HTTP POST
            return await self._dispatch_generic_http(inputs)
        else:
            # Unknown integration — best-effort HTTP POST if url provided
            if inputs.get("url") or inputs.get("webhook_url"):
                return await self._dispatch_generic_http(inputs)
            return NodeResult(
                success=False,
                error=f"Unknown integration '{integration}'. Provide a webhook_url/url to use generic HTTP.",
            )

    async def _dispatch_slack(self, inputs: dict[str, Any]) -> NodeResult:
        """Send to Slack via Incoming Webhook URL or Bot Token."""
        webhook_url = inputs.get("webhook_url") or inputs.get("url")
        text = inputs.get("text") or inputs.get("message") or ""

        if not webhook_url:
            # Try env var
            import os

            webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")

        if not webhook_url:
            return NodeResult(
                success=False,
                error="Slack node requires webhook_url. Add it to the node config or set SLACK_WEBHOOK_URL env var.",
            )

        payload: dict[str, Any] = {"text": text}
        if inputs.get("channel"):
            payload["channel"] = inputs["channel"]

        if not HAS_AIOHTTP:
            # Fall back to httpx (always available)
            try:
                import httpx

                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.post(webhook_url, json=payload)
                    ok = resp.status_code in (200, 204)
                    return NodeResult(success=ok, data={"sent": ok, "status": resp.status_code})
            except Exception as exc:
                return NodeResult(success=False, error=str(exc))

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=payload) as response:
                    ok = response.status in (200, 204)
                    return NodeResult(success=ok, data={"sent": ok, "status": response.status})
        except Exception as exc:
            return NodeResult(success=False, error=str(exc))

    async def _dispatch_email(self, inputs: dict[str, Any]) -> NodeResult:
        """Send email via SMTP (uses SMTP_HOST/PORT/USER/PASS env vars)."""
        import os
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER", "")
        smtp_pass = os.getenv("SMTP_PASS", os.getenv("SMTP_PASSWORD", ""))

        if not smtp_user:
            return NodeResult(success=False, error="Email node requires SMTP_USER env var.")

        to_addr = inputs.get("to", "")
        subject = inputs.get("subject", "(no subject)")
        body = inputs.get("body") or inputs.get("text") or inputs.get("message") or ""

        if not to_addr:
            return NodeResult(success=False, error="Email node requires 'to' field.")

        try:
            msg = MIMEMultipart("alternative")
            msg["From"] = smtp_user
            msg["To"] = to_addr
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)

            return NodeResult(success=True, data={"sent": True, "to": to_addr})
        except Exception as exc:
            return NodeResult(success=False, error=str(exc))

    async def _dispatch_discord(self, inputs: dict[str, Any]) -> NodeResult:
        """Forward to Discord webhook (reuse SlackWebhookNode logic, same payload shape)."""
        webhook_url = inputs.get("webhook_url") or inputs.get("url")
        if not webhook_url:
            return NodeResult(success=False, error="Discord node requires webhook_url in config.")
        text = inputs.get("text") or inputs.get("message") or inputs.get("content") or ""
        payload: dict[str, Any] = {"content": text}
        try:
            import httpx

            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(webhook_url, json=payload)
                ok = resp.status_code in (200, 204)
                return NodeResult(success=ok, data={"sent": ok})
        except Exception as exc:
            return NodeResult(success=False, error=str(exc))

    async def _dispatch_generic_http(self, inputs: dict[str, Any]) -> NodeResult:
        """Generic HTTP POST to a URL with the node's payload."""
        url = inputs.get("url") or inputs.get("webhook_url")
        if not url:
            return NodeResult(success=False, error="Generic HTTP dispatch requires url or webhook_url in config.")

        payload = inputs.get("payload") or {
            "text": inputs.get("text") or inputs.get("message"),
            "channel": inputs.get("channel"),
            "action": inputs.get("action"),
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        try:
            import httpx

            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, json=payload)
                ok = 200 <= resp.status_code < 300
                try:
                    data = resp.json()
                except Exception:
                    data = {"body": resp.text}
                return NodeResult(success=ok, data={"status": resp.status_code, **data})
        except Exception as exc:
            return NodeResult(success=False, error=str(exc))


# ==================== DATABASE NODE ====================


class DatabaseQueryNode(BaseNode):
    """Execute a SQL query against a PostgreSQL database and return the results.

    Connection string priority:
      1. ``connection_string`` input (user-provided, per-node override)
      2. ``DATABASE_URL`` environment variable (platform default)

    Only SELECT queries are permitted by default; set ``allow_writes=true`` to
    enable INSERT / UPDATE / DELETE (use with caution).
    """

    name = "database_query"
    display_name = "Database Query"
    description = "Execute a SQL query and return the result rows as JSON"
    category = NodeCategory.ACTION
    icon = "🗄️"

    inputs = [
        NodeInput("query", "string", "SQL query to execute", required=True),
        NodeInput(
            "parameters",
            "array",
            "Positional query parameters ($1, $2, …)",
            required=False,
        ),
        NodeInput(
            "connection_string",
            "string",
            "PostgreSQL connection string (overrides DATABASE_URL env var)",
            required=False,
        ),
        NodeInput(
            "allow_writes",
            "boolean",
            "Allow INSERT/UPDATE/DELETE queries (default false — SELECT only)",
            default=False,
        ),
        NodeInput(
            "row_limit",
            "number",
            "Maximum rows to return (1-1000, default 100)",
            default=100,
        ),
    ]

    outputs = [
        NodeOutput("rows", "array", "Query result rows as list of objects"),
        NodeOutput("row_count", "number", "Number of rows returned"),
        NodeOutput("columns", "array", "Column names in result order"),
        NodeOutput("error", "string", "Error message if query failed"),
    ]

    # SQL keywords that mutate state
    _WRITE_KEYWORDS = frozenset(["insert", "update", "delete", "drop", "truncate", "alter", "create", "replace"])

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        import os
        import re as _re

        query: str = (inputs.get("query") or "").strip()
        if not query:
            return NodeResult(success=False, error="'query' input is required")

        allow_writes: bool = bool(inputs.get("allow_writes", False))
        row_limit: int = max(1, min(int(inputs.get("row_limit") or 100), 1000))
        params = inputs.get("parameters") or []

        # Safety: strip comments to find the real first keyword (prevents bypass via /* */ or --)
        cleaned = _re.sub(r"/\*.*?\*/", "", query, flags=_re.DOTALL).strip()
        cleaned = _re.sub(r"^--[^\n]*\n?", "", cleaned).strip()
        first_word = cleaned.split()[0].lower() if cleaned else ""

        # Reject multi-statement queries (e.g. "SELECT 1; DROP TABLE users")
        if ";" in _re.sub(r"'[^']*'", "", query):
            return NodeResult(success=False, error="Multi-statement queries are not allowed.")

        # Check for write keywords anywhere in the cleaned query (not just first word)
        # This catches CTEs like: WITH cte AS (DELETE FROM ...) SELECT * FROM cte
        if not allow_writes:
            query_words = set(cleaned.lower().split())
            write_words_found = query_words & self._WRITE_KEYWORDS
            if write_words_found:
                return NodeResult(
                    success=False,
                    error=(
                        f"Write keyword(s) ({', '.join(w.upper() for w in write_words_found)}) detected. "
                        "Set allow_writes=true to enable INSERT/UPDATE/DELETE."
                    ),
                )

        # Apply row limit for SELECT queries
        if first_word == "select" and "limit" not in query.lower():
            query = f"{query} LIMIT {row_limit}"

        dsn: str = inputs.get("connection_string") or os.getenv("DATABASE_URL", "")
        if not dsn:
            return NodeResult(
                success=False,
                error="No database connection string. Set 'connection_string' or DATABASE_URL.",
            )

        # SSRF protection: block user-provided connection strings targeting internal networks
        if inputs.get("connection_string"):
            import ipaddress
            from urllib.parse import urlparse

            try:
                parsed = urlparse(dsn)
                host = parsed.hostname or ""
                # Block localhost / loopback / private ranges / cloud metadata
                if host in ("localhost", "127.0.0.1", "0.0.0.0", "::1", "metadata.google.internal"):
                    return NodeResult(success=False, error="Connection to localhost/loopback is not allowed.")
                try:
                    ip = ipaddress.ip_address(host)
                    if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                        return NodeResult(
                            success=False, error=f"Connection to private/reserved IP ({host}) is not allowed."
                        )
                except ValueError:
                    pass  # hostname, not IP — allow (DNS resolution handled by driver)
            except Exception as e:
                logger.debug("Could not parse DSN for SSRF check, asyncpg will validate: %s", e)

        try:
            import asyncpg

            conn = await asyncpg.connect(dsn, timeout=15)
            try:
                rows = await conn.fetch(query, *params)
                columns = list(rows[0].keys()) if rows else []
                result_rows = [dict(r) for r in rows]
                return NodeResult(
                    success=True,
                    data={
                        "rows": result_rows,
                        "row_count": len(result_rows),
                        "columns": columns,
                    },
                )
            finally:
                await conn.close()
        except ImportError:
            return NodeResult(
                success=False,
                error="asyncpg not installed — add 'asyncpg' to requirements",
            )
        except Exception as exc:
            logger.warning("DatabaseQueryNode failed: %s", exc)
            return NodeResult(success=False, error=str(exc))


# ==================== EMAIL TRIGGER NODE ====================


class EmailTriggerNode(BaseNode):
    """IMAP polling trigger — fires when new emails matching a filter arrive.

    Registers a polling job for the spiral that checks the IMAP inbox on a
    configurable interval. When a matching email arrives, the spiral is
    triggered with the email data as ``trigger_data``.

    This node acts as a *configuration* node within the spiral; the actual
    IMAP polling is handled by ``WorkflowScheduler`` using the registered job.
    """

    name = "email_trigger"
    display_name = "Email Trigger (IMAP)"
    description = "Trigger a Spiral when a new email arrives matching given filters"
    category = NodeCategory.TRIGGER
    icon = "📬"

    inputs = [
        NodeInput("imap_host", "string", "IMAP server hostname", required=True),
        NodeInput("imap_port", "number", "IMAP port (default 993 SSL)", default=993),
        NodeInput("username", "string", "IMAP account email address", required=True),
        NodeInput("password", "string", "IMAP account password / app password", required=True),
        NodeInput(
            "mailbox",
            "string",
            "Mailbox folder to poll (default INBOX)",
            default="INBOX",
        ),
        NodeInput(
            "subject_filter",
            "string",
            "Only trigger for emails whose subject contains this text (case-insensitive)",
            required=False,
        ),
        NodeInput(
            "from_filter",
            "string",
            "Only trigger for emails from this sender address",
            required=False,
        ),
        NodeInput(
            "poll_interval_seconds",
            "number",
            "How often to check for new emails (default 60s, min 30s)",
            default=60,
        ),
        NodeInput(
            "mark_read",
            "boolean",
            "Mark emails as read after processing (default true)",
            default=True,
        ),
    ]

    outputs = [
        NodeOutput("subject", "string", "Email subject"),
        NodeOutput("from", "string", "Sender address"),
        NodeOutput("body", "string", "Plain-text email body"),
        NodeOutput("received_at", "string", "ISO timestamp the email was received"),
        NodeOutput("trigger_data", "object", "Full email metadata object"),
    ]

    async def execute(self, inputs: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        """Register the IMAP polling job with the scheduler.

        During a normal spiral execution this node returns its configuration so
        the scheduler can set up recurring IMAP polls. If called directly (e.g.
        for testing) it attempts a single IMAP check.
        """
        imap_host: str = (inputs.get("imap_host") or "").strip()
        username: str = (inputs.get("imap_username") or inputs.get("username") or "").strip()
        password: str = inputs.get("imap_password") or inputs.get("password") or ""

        if not imap_host or not username or not password:
            return NodeResult(
                success=False,
                error="'imap_host', 'username', and 'password' are required",
            )

        imap_port: int = int(inputs.get("imap_port") or 993)
        mailbox: str = inputs.get("mailbox") or "INBOX"
        subject_filter: str = inputs.get("subject_filter") or ""
        from_filter: str = inputs.get("from_filter") or ""
        poll_interval: int = max(30, int(inputs.get("poll_interval_seconds") or 60))
        mark_read: bool = bool(inputs.get("mark_read", True))

        # Register polling job with scheduler if in workflow context
        workflow_id: str = context.get("workflow_id", "")
        if workflow_id:
            try:
                from apps.backend.workflow_engine.scheduler import WorkflowScheduler

                scheduler = WorkflowScheduler.get_instance()
                job_id = f"email_trigger_{workflow_id}"
                await scheduler.register_imap_poll(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    imap_config={
                        "host": imap_host,
                        "port": imap_port,
                        "username": username,
                        "password": password,
                        "mailbox": mailbox,
                        "subject_filter": subject_filter,
                        "from_filter": from_filter,
                        "mark_read": mark_read,
                    },
                    interval_seconds=poll_interval,
                )
                logger.info(
                    "EmailTriggerNode: registered IMAP poll job %s (every %ds)",
                    job_id,
                    poll_interval,
                )
                return NodeResult(
                    success=True,
                    data={
                        "trigger_registered": True,
                        "job_id": job_id,
                        "poll_interval_seconds": poll_interval,
                        "mailbox": mailbox,
                    },
                )
            except Exception as exc:
                logger.warning("EmailTriggerNode: scheduler registration failed: %s", exc)
                # Fall through to manual check

        # Fallback: attempt a single IMAP check (useful for one-shot testing)
        return await self._single_imap_check(
            imap_host,
            imap_port,
            username,
            password,
            mailbox,
            subject_filter,
            from_filter,
            mark_read,
        )

    async def _single_imap_check(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        mailbox: str,
        subject_filter: str,
        from_filter: str,
        mark_read: bool,
    ) -> NodeResult:
        """Run a one-shot IMAP fetch of the most recent unseen email."""
        import asyncio
        import email as email_lib
        import imaplib

        loop = asyncio.get_event_loop()

        def _blocking_fetch():
            conn = imaplib.IMAP4_SSL(host, port)
            try:
                conn.login(username, password)
                conn.select(mailbox)

                # Search for unseen emails
                search_criteria = ["UNSEEN"]
                if subject_filter:
                    search_criteria += ["SUBJECT", subject_filter]
                if from_filter:
                    search_criteria += ["FROM", from_filter]

                criteria_str = " ".join(f'"{c}"' if " " in c else c for c in search_criteria)
                status, data = conn.search(None, f"({criteria_str})")
                if status != "OK" or not data[0]:
                    return None

                # Fetch the most recent matching email
                msg_ids = data[0].split()
                latest_id = msg_ids[-1]

                status, msg_data = conn.fetch(latest_id, "(RFC822)")
                if status != "OK":
                    return None

                raw_email = msg_data[0][1]
                msg = email_lib.message_from_bytes(raw_email)

                subject = msg.get("Subject", "")
                sender = msg.get("From", "")
                date = msg.get("Date", "")

                # Extract plain text body
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode("utf-8", errors="replace")
                            break
                else:
                    if msg.get_content_type() == "text/plain":
                        body = msg.get_payload(decode=True).decode("utf-8", errors="replace")

                if mark_read:
                    conn.store(latest_id, "+FLAGS", "\\Seen")

                return {
                    "subject": subject,
                    "from": sender,
                    "body": body[:10000],  # truncate large bodies
                    "received_at": date,
                }
            finally:
                conn.logout()

        try:
            result = await loop.run_in_executor(None, _blocking_fetch)
            if result is None:
                return NodeResult(
                    success=True,
                    data={"trigger_data": None, "message": "No unseen emails matching filter"},
                )
            return NodeResult(
                success=True,
                data={
                    "subject": result["subject"],
                    "from": result["from"],
                    "body": result["body"],
                    "received_at": result["received_at"],
                    "trigger_data": result,
                },
            )
        except Exception as exc:
            logger.warning("EmailTriggerNode IMAP check failed: %s", exc)
            return NodeResult(success=False, error=f"IMAP check failed: {exc}")


# ==================== NODE REGISTRY ====================


class NodeRegistry:
    """Registry of all available nodes"""

    def __init__(self):
        self.nodes: dict[str, type] = {}
        self._register_builtin_nodes()

    def _register_builtin_nodes(self):
        """Register all built-in nodes"""
        builtin_nodes = [
            # HTTP/API
            HTTPRequestNode,
            WebhookTriggerNode,
            WebhookResponseNode,
            # AI
            AIAgentNode,
            TextGenerationNode,
            SentimentAnalysisNode,
            # Code
            CodeExecutionNode,
            # Transform
            JSONTransformNode,
            FilterNode,
            MapNode,
            # Control
            ConditionNode,
            LoopNode,
            DelayNode,
            # Communication
            EmailNode,
            DiscordWebhookNode,
            SlackWebhookNode,
            # Named integration dispatcher (handles type="integration" from Spiral Builder UI)
            IntegrationNode,
            # Data
            DatabaseQueryNode,
            # Triggers
            EmailTriggerNode,
        ]

        for node_class in builtin_nodes:
            self.register(node_class)

    def register(self, node_class: type) -> None:
        """Register a node class"""
        self.nodes[node_class.name] = node_class

    def get(self, name: str) -> type | None:
        """Get a node class by name"""
        return self.nodes.get(name)

    def create(self, name: str, node_id: str | None = None) -> BaseNode | None:
        """Create a node instance"""
        node_class = self.get(name)
        if node_class:
            return node_class(node_id)
        return None

    def list_nodes(self, category: NodeCategory | None = None) -> list[dict[str, Any]]:
        """List all available nodes"""
        nodes = []
        for name, node_class in self.nodes.items():
            if category is None or node_class.category == category:
                nodes.append(node_class().get_schema())
        return nodes

    def get_categories(self) -> list[str]:
        """Get all node categories"""
        return list(set(node.category.value for node in self.nodes.values()))


# Global registry instance
_node_registry: NodeRegistry | None = None


def get_node_registry() -> NodeRegistry:
    """Get the global node registry"""
    global _node_registry
    if _node_registry is None:
        _node_registry = NodeRegistry()
    return _node_registry
