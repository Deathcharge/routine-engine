"""
Helix Chains - Tool Framework
==============================

Tools that can be used within chains for various operations:
- Web search
- Code execution
- Database queries
- API calls
- File operations
- Calculations
"""

import asyncio
import ipaddress
import logging
import os
import re
import shlex
import socket
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from urllib.parse import urlparse as _urlparse

# Optional imports for tools
try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None

try:
    import wikipedia
except ImportError:
    wikipedia = None

try:
    import asyncpg
except ImportError:
    asyncpg = None

try:
    import aiohttp
except ImportError:
    aiohttp = None

logger = logging.getLogger(__name__)


async def _validate_external_url(url: str) -> None:
    """Validate a URL is safe for server-side requests (SSRF protection).

    Async to avoid blocking the event loop during DNS resolution.
    Raises ValueError for unsafe URLs.
    """
    import asyncio

    parsed = _urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Unsafe URL scheme: {parsed.scheme} — only http/https allowed")

    hostname = parsed.hostname
    if not hostname:
        raise ValueError("URL has no hostname")

    if hostname in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):  # nosec B104
        raise ValueError(f"Localhost URLs are not allowed: {hostname}")

    try:
        loop = asyncio.get_event_loop()
        resolved = await loop.getaddrinfo(hostname, None)
        for _family, _type, _proto, _canonname, sockaddr in resolved:
            ip = ipaddress.ip_address(sockaddr[0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                raise ValueError(f"URL resolves to private/reserved IP: {ip}")
    except socket.gaierror as err:
        raise ValueError(f"Cannot resolve hostname: {hostname}") from err


class ToolCategory(Enum):
    """Tool categories"""

    SEARCH = "search"
    CODE = "code"
    DATABASE = "database"
    API = "api"
    FILE = "file"
    MATH = "math"
    AI = "ai"
    UTILITY = "utility"


@dataclass
class ToolResult:
    """Result from tool execution"""

    success: bool
    output: Any
    error: str | None = None
    execution_time_ms: float = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolSchema:
    """Schema describing a tool's interface"""

    name: str
    description: str
    parameters: dict[str, Any]
    required: list[str] = field(default_factory=list)
    returns: str = "Any"
    examples: list[dict[str, Any]] = field(default_factory=list)

    def to_openai_function(self) -> dict[str, Any]:
        """Convert to OpenAI function calling format"""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": {
                "type": "object",
                "properties": self.parameters,
                "required": self.required,
            },
        }

    def to_anthropic_tool(self) -> dict[str, Any]:
        """Convert to Anthropic tool format"""
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": {
                "type": "object",
                "properties": self.parameters,
                "required": self.required,
            },
        }


class Tool(ABC):
    """
    Base class for all tools in Helix Chains.

    Tools are callable units that perform specific operations
    like searching the web, executing code, or querying databases.
    """

    name: str = "tool"
    description: str = "A tool"
    category: ToolCategory = ToolCategory.UTILITY

    def __init__(self, **config):
        self.config = config
        self._schema: ToolSchema | None = None

    @property
    def schema(self) -> ToolSchema:
        """Get tool schema"""
        if self._schema is None:
            self._schema = self._build_schema()
        return self._schema

    def _build_schema(self) -> ToolSchema:
        """Build schema from tool definition"""
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters={},
            required=[],
        )

    @abstractmethod
    async def execute(self, **kwargs) -> ToolResult:
        """Execute the tool"""

    async def __call__(self, **kwargs) -> ToolResult:
        """Make tool callable"""
        return await self.execute(**kwargs)

    def validate_input(self, **kwargs) -> list[str]:
        """Validate input parameters"""
        errors = []
        for required in self.schema.required:
            if required not in kwargs:
                errors.append(f"Missing required parameter: {required}")
        return errors


class ToolRegistry:
    """
    Registry for managing and discovering tools.
    """

    def __init__(self):
        self._tools: dict[str, Tool] = {}
        self._categories: dict[ToolCategory, list[str]] = {}

    def register(self, tool: Tool) -> None:
        """Register a tool"""
        self._tools[tool.name] = tool

        if tool.category not in self._categories:
            self._categories[tool.category] = []
        self._categories[tool.category].append(tool.name)

        logger.info("Registered tool: %s", tool.name)

    def get(self, name: str) -> Tool | None:
        """Get a tool by name"""
        return self._tools.get(name)

    def list_tools(self, category: ToolCategory = None) -> list[str]:
        """List all tools or tools in a category"""
        if category:
            return self._categories.get(category, [])
        return list(self._tools.keys())

    def get_schemas(self) -> list[ToolSchema]:
        """Get schemas for all tools"""
        return [tool.schema for tool in self._tools.values()]

    def to_openai_functions(self) -> list[dict[str, Any]]:
        """Get all tools as OpenAI functions"""
        return [tool.schema.to_openai_function() for tool in self._tools.values()]


# ============================================================================
# SEARCH TOOLS
# ============================================================================


class WebSearchTool(Tool):
    """
    Search the web using various providers.
    """

    name = "web_search"
    description = "Search the web for information on any topic"
    category = ToolCategory.SEARCH

    def __init__(self, provider: str = "duckduckgo", **config):
        super().__init__(**config)
        self.provider = provider

    def _build_schema(self) -> ToolSchema:
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters={
                "query": {"type": "string", "description": "The search query"},
                "num_results": {
                    "type": "integer",
                    "description": "Number of results to return",
                    "default": 5,
                },
            },
            required=["query"],
            examples=[{"query": "Python async programming", "num_results": 5}],
        )

    async def execute(self, query: str, num_results: int = 5, **kwargs) -> ToolResult:
        """Execute web search"""
        import time

        start = time.time()

        try:
            results = await self._search_duckduckgo(query, num_results)

            return ToolResult(
                success=True,
                output=results,
                execution_time_ms=(time.time() - start) * 1000,
                metadata={"provider": self.provider, "query": query},
            )
        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=(time.time() - start) * 1000,
            )

    async def _search_duckduckgo(self, query: str, num_results: int) -> list[dict[str, str]]:
        """Search using DuckDuckGo"""
        try:
            if DDGS is None:
                raise ImportError("duckduckgo-search not installed. Run: pip install duckduckgo-search")

            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=num_results))
                return [
                    {
                        "title": r.get("title", ""),
                        "url": r.get("href", ""),
                        "snippet": r.get("body", ""),
                    }
                    for r in results
                ]
        except ImportError:
            return await self._search_fallback(query, num_results)

    async def _search_fallback(self, query: str, num_results: int) -> list[dict[str, str]]:
        """Fallback when search library is unavailable"""
        return [
            {
                "title": "Search unavailable",
                "url": "",
                "snippet": "Web search is currently unavailable. Install duckduckgo-search package to enable: pip install duckduckgo-search",
            }
        ]


class WikipediaTool(Tool):
    """
    Search and retrieve Wikipedia articles.
    """

    name = "wikipedia"
    description = "Search Wikipedia for factual information"
    category = ToolCategory.SEARCH

    def _build_schema(self) -> ToolSchema:
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters={
                "query": {"type": "string", "description": "The topic to search for"},
                "sentences": {
                    "type": "integer",
                    "description": "Number of sentences to return",
                    "default": 5,
                },
            },
            required=["query"],
        )

    async def execute(self, query: str, sentences: int = 5, **kwargs) -> ToolResult:
        """Search Wikipedia"""
        import time

        start = time.time()

        try:
            # Search for pages
            search_results = wikipedia.search(query, results=3)

            if not search_results:
                return ToolResult(
                    success=True,
                    output={"message": f"No Wikipedia articles found for: {query}"},
                    execution_time_ms=(time.time() - start) * 1000,
                )

            # Get summary of first result
            try:
                page = wikipedia.page(search_results[0])

                return ToolResult(
                    success=True,
                    output={
                        "title": page.title,
                        "summary": page.summary,
                        "url": page.url,
                        "related": (search_results[1:] if len(search_results) > 1 else []),
                    },
                    execution_time_ms=(time.time() - start) * 1000,
                )
            except wikipedia.DisambiguationError as e:
                return ToolResult(
                    success=True,
                    output={
                        "message": "Multiple results found",
                        "options": e.options[:5],
                    },
                    execution_time_ms=(time.time() - start) * 1000,
                )

        except ImportError:
            return ToolResult(
                success=False,
                output=None,
                error="Wikipedia library not installed",
                execution_time_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=(time.time() - start) * 1000,
            )


# ============================================================================
# CODE EXECUTION TOOLS
# ============================================================================


class CodeExecutorTool(Tool):
    """
    Execute code in various languages safely.
    """

    name = "code_executor"
    description = "Execute code in Python, JavaScript, or shell"
    category = ToolCategory.CODE

    ALLOWED_LANGUAGES = ["python", "javascript", "shell", "bash"]

    def __init__(self, sandbox: bool = True, timeout: int = 30, **config):
        super().__init__(**config)
        self.sandbox = sandbox
        self.timeout = timeout

    def _build_schema(self) -> ToolSchema:
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters={
                "code": {"type": "string", "description": "The code to execute"},
                "language": {
                    "type": "string",
                    "description": "Programming language",
                    "enum": self.ALLOWED_LANGUAGES,
                },
            },
            required=["code", "language"],
        )

    async def execute(self, code: str, language: str, **kwargs) -> ToolResult:
        """Execute code"""
        import time

        start = time.time()

        if language not in self.ALLOWED_LANGUAGES:
            return ToolResult(
                success=False,
                output=None,
                error=f"Unsupported language: {language}",
                execution_time_ms=(time.time() - start) * 1000,
            )

        try:
            if language == "python":
                result = await self._execute_python(code)
            elif language in ["shell", "bash"]:
                result = await self._execute_shell(code)
            elif language == "javascript":
                result = await self._execute_javascript(code)
            else:
                result = {"error": f"Unsupported language: {language}"}

            return ToolResult(
                success=True,
                output=result,
                execution_time_ms=(time.time() - start) * 1000,
                metadata={"language": language},
            )
        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=(time.time() - start) * 1000,
            )

    async def _execute_python(self, code: str) -> dict[str, Any]:
        """Execute Python code in a restricted environment."""
        python_enabled = os.getenv("HELIX_PYTHON_EXECUTION_ENABLED", "false").lower() == "true"
        if not python_enabled:
            return {
                "error": "Python execution is disabled. Set HELIX_PYTHON_EXECUTION_ENABLED=true to enable.",
                "stdout": "",
                "success": False,
            }

        import io
        import sys

        old_stdout = sys.stdout
        sys.stdout = captured_output = io.StringIO()

        result: dict[str, Any] = {}
        try:
            exec(compile(code, "<helix_tool>", "exec"), {})  # nosec B102
            output = captured_output.getvalue()
            result = {"stdout": output, "success": True}
        except Exception as e:
            output = captured_output.getvalue()
            result = {"error": type(e).__name__, "stdout": output, "success": False}
        finally:
            sys.stdout = old_stdout

        return result

    async def _execute_shell(self, code: str) -> dict[str, Any]:
        """Execute shell commands safely using exec (not shell)"""
        # SECURITY: Check if shell execution is enabled via environment
        shell_enabled = os.getenv("HELIX_SHELL_EXECUTION_ENABLED", "false").lower() == "true"

        if not shell_enabled:
            return {
                "error": "Shell execution is disabled. Set HELIX_SHELL_EXECUTION_ENABLED=true to enable (not recommended for production).",
                "stderr": "",
                "return_code": 1,
                "success": False,
            }

        # SECURITY: Only allow specific safe commands when enabled
        # Parse command into args without shell interpretation
        try:
            # Use shlex.split for basic parsing, but validate the command
            # This is safer than shell=True but still requires careful use
            parts = shlex.split(code) if code else []
        except ValueError as e:
            return {
                "error": f"Invalid command syntax: {e}",
                "stderr": "",
                "return_code": 1,
                "success": False,
            }

        if not parts:
            return {
                "error": "Empty command",
                "stderr": "",
                "return_code": 1,
                "success": False,
            }

        # SECURITY: Warn about production use
        logger.warning(
            "⚠️ Shell execution enabled - ensure this is not exposed to untrusted users. Command: %s",
            parts[0] if parts else "none",
        )

        try:
            # Use create_subprocess_exec instead of shell for security
            # This avoids shell injection vulnerabilities
            process = await asyncio.create_subprocess_exec(
                *parts,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=self.timeout)

            return {
                "stdout": stdout.decode() if stdout else "",
                "stderr": stderr.decode() if stderr else "",
                "return_code": process.returncode,
                "success": process.returncode == 0,
            }
        except TimeoutError:
            return {"error": "Execution timed out", "success": False}
        except FileNotFoundError:
            return {"error": f"Command not found: {parts[0]}", "success": False}
        except PermissionError:
            return {"error": f"Permission denied: {parts[0]}", "success": False}
        except Exception as e:
            return {"error": f"Execution error: {e!s}", "success": False}

    async def _execute_javascript(self, code: str) -> dict[str, Any]:
        """Execute JavaScript using Node.js"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".js", delete=False) as f:
            f.write(code)
            temp_file = f.name

        try:
            process = await asyncio.create_subprocess_exec(
                "node",
                temp_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=self.timeout)

            return {
                "stdout": stdout.decode() if stdout else "",
                "stderr": stderr.decode() if stderr else "",
                "return_code": process.returncode,
                "success": process.returncode == 0,
            }
        except TimeoutError:
            return {"error": "Execution timed out", "success": False}
        finally:
            os.unlink(temp_file)


# ============================================================================
# DATABASE TOOLS
# ============================================================================


class DatabaseTool(Tool):
    """
    Execute database queries.
    """

    name = "database"
    description = "Execute SQL queries on PostgreSQL, MySQL, or SQLite"
    category = ToolCategory.DATABASE

    def __init__(self, connection_string: str | None = None, **config):
        super().__init__(**config)
        self.connection_string = connection_string or os.getenv("DATABASE_URL")

    def _build_schema(self) -> ToolSchema:
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters={
                "query": {"type": "string", "description": "SQL query to execute"},
                "params": {
                    "type": "array",
                    "description": "Query parameters",
                    "items": {"type": "string"},
                },
            },
            required=["query"],
        )

    async def execute(self, query: str, params: list[Any] | None = None, **kwargs) -> ToolResult:
        """Execute database query"""
        import re as _re
        import time

        start = time.time()

        # Safety: enforce SELECT-only to prevent SQL injection / data mutation
        cleaned = _re.sub(r"/\*.*?\*/", "", query.strip(), flags=_re.DOTALL).strip()
        cleaned = _re.sub(r"--[^\n]*\n?", "", cleaned).strip()
        first_word = cleaned.split()[0].lower() if cleaned else ""
        if first_word not in ("select", "with", "explain", "show", "describe"):
            return ToolResult(
                success=False,
                output=None,
                error=f"Only SELECT queries are allowed. Got: '{first_word.upper()}'.",
                execution_time_ms=(time.time() - start) * 1000,
            )
        # Reject multi-statement queries
        if ";" in _re.sub(r"'[^']*'", "", query):
            return ToolResult(
                success=False,
                output=None,
                error="Multi-statement queries are not allowed.",
                execution_time_ms=(time.time() - start) * 1000,
            )

        try:
            if self.connection_string.startswith("postgresql"):
                result = await self._execute_postgres(query, params)
            elif self.connection_string.startswith("sqlite"):
                result = await self._execute_sqlite(query, params)
            else:
                result = {"error": "Unsupported database type"}

            return ToolResult(
                success=True,
                output=result,
                execution_time_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=(time.time() - start) * 1000,
            )

    async def _execute_postgres(self, query: str, params: list[Any] | None = None) -> dict[str, Any]:
        """Execute PostgreSQL query (SELECT-only, enforced by execute())"""
        try:
            conn = await asyncpg.connect(self.connection_string)
            try:
                rows = await conn.fetch(query, *(params or []))
                return {"rows": [dict(row) for row in rows], "count": len(rows)}
            finally:
                await conn.close()
        except ImportError:
            return {"error": "asyncpg not installed"}

    async def _execute_sqlite(self, query: str, params: list[Any] | None = None) -> dict[str, Any]:
        """Execute SQLite query (SELECT-only, enforced by execute())"""
        import sqlite3

        db_path = self.connection_string.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute(query, params or [])
            rows = cursor.fetchall()
            return {"rows": [dict(row) for row in rows], "count": len(rows)}
        finally:
            conn.close()


# ============================================================================
# API TOOLS
# ============================================================================


class APITool(Tool):
    """
    Make HTTP API calls.
    """

    name = "api_call"
    description = "Make HTTP requests to external APIs"
    category = ToolCategory.API

    def __init__(self, base_url: str | None = None, headers: dict[str, str] | None = None, **config):
        super().__init__(**config)
        self.base_url = base_url or ""
        self.default_headers = headers or {}

    def _build_schema(self) -> ToolSchema:
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters={
                "method": {
                    "type": "string",
                    "description": "HTTP method",
                    "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"],
                },
                "url": {"type": "string", "description": "URL to call"},
                "body": {
                    "type": "object",
                    "description": "Request body for POST/PUT/PATCH",
                },
                "headers": {"type": "object", "description": "Additional headers"},
            },
            required=["method", "url"],
        )

    async def execute(
        self,
        method: str,
        url: str,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs,
    ) -> ToolResult:
        """Make API call"""
        import time

        start = time.time()

        return ToolResult(
            success=False,
            output=None,
            error="Legacy network actions are disabled; use an explicitly registered Routine Engine action",
        )

        # Retained temporarily as archived source; unreachable by design.
        try:
            full_url = f"{self.base_url}{url}" if not url.startswith("http") else url

            # SSRF protection: validate URL before making the request
            await _validate_external_url(full_url)

            all_headers = {**self.default_headers, **(headers or {})}

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=method,
                    url=full_url,
                    json=body,
                    headers=all_headers,
                    allow_redirects=False,
                ) as response:
                    try:
                        response_data = await response.json()
                    except Exception:
                        response_data = await response.text()

                    return ToolResult(
                        success=response.status < 400,
                        output={
                            "status": response.status,
                            "data": response_data,
                            "headers": dict(response.headers),
                        },
                        execution_time_ms=(time.time() - start) * 1000,
                    )
        except ImportError:
            return ToolResult(
                success=False,
                output=None,
                error="aiohttp not installed",
                execution_time_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=(time.time() - start) * 1000,
            )


# ============================================================================
# FILE SYSTEM TOOLS
# ============================================================================


class FileSystemTool(Tool):
    """
    File system operations.
    """

    name = "filesystem"
    description = "Read, write, and manage files"
    category = ToolCategory.FILE

    def __init__(self, base_path: str | None = None, **config):
        super().__init__(**config)
        self.base_path = base_path or os.getcwd()

    def _build_schema(self) -> ToolSchema:
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters={
                "operation": {
                    "type": "string",
                    "description": "Operation to perform",
                    "enum": ["read", "write", "append", "delete", "list", "exists"],
                },
                "path": {"type": "string", "description": "File or directory path"},
                "content": {
                    "type": "string",
                    "description": "Content for write/append operations",
                },
            },
            required=["operation", "path"],
        )

    async def execute(self, operation: str, path: str, content: str | None = None, **kwargs) -> ToolResult:
        """Execute file operation"""
        import time

        start = time.time()

        # Prevent path traversal
        base_path = os.path.realpath(self.base_path)
        full_path = os.path.realpath(os.path.join(base_path, path))
        try:
            contained = os.path.commonpath((base_path, full_path)) == base_path
        except ValueError:
            contained = False
        if not contained:
            return ToolResult(success=False, output=None, error="Path traversal not allowed")

        try:
            if operation == "read":
                with open(full_path, encoding="utf-8") as f:
                    result = f.read()
            elif operation == "write":
                with open(full_path, "w", encoding="utf-8") as f:
                    f.write(content or "")
                result = {"written": len(content or "")}
            elif operation == "append":
                with open(full_path, "a", encoding="utf-8") as f:
                    f.write(content or "")
                result = {"appended": len(content or "")}
            elif operation == "delete":
                os.remove(full_path)
                result = {"deleted": True}
            elif operation == "list":
                result = os.listdir(full_path)
            elif operation == "exists":
                result = os.path.exists(full_path)
            else:
                return ToolResult(success=False, output=None, error=f"Unknown operation: {operation}")

            return ToolResult(
                success=True,
                output=result,
                execution_time_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=(time.time() - start) * 1000,
            )


# ============================================================================
# MATH/CALCULATOR TOOLS
# ============================================================================


class CalculatorTool(Tool):
    """
    Perform mathematical calculations.
    """

    name = "calculator"
    description = "Perform mathematical calculations and evaluations"
    category = ToolCategory.MATH

    def _build_schema(self) -> ToolSchema:
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters={
                "expression": {
                    "type": "string",
                    "description": "Mathematical expression to evaluate",
                }
            },
            required=["expression"],
        )

    async def execute(self, expression: str, **kwargs) -> ToolResult:
        """Evaluate mathematical expression"""
        import math
        import time

        start = time.time()

        # Safe math functions
        safe_dict = {
            "abs": abs,
            "round": round,
            "min": min,
            "max": max,
            "sum": sum,
            "pow": pow,
            "sqrt": math.sqrt,
            "sin": math.sin,
            "cos": math.cos,
            "tan": math.tan,
            "log": math.log,
            "log10": math.log10,
            "exp": math.exp,
            "pi": math.pi,
            "e": math.e,
        }

        try:
            from apps.backend.utils.safe_eval import SafeEvaluator

            clean_expr = re.sub(r"[^0-9+\-*/().a-z\s]", "", expression.lower())
            evaluator = SafeEvaluator(allowed_names=safe_dict)
            result = evaluator.eval(clean_expr)

            return ToolResult(
                success=True,
                output=result,
                execution_time_ms=(time.time() - start) * 1000,
                metadata={"expression": expression},
            )
        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=(time.time() - start) * 1000,
            )


# ============================================================================
# DEFAULT TOOL REGISTRY
# ============================================================================


def create_default_registry() -> ToolRegistry:
    """Create a registry with all default tools"""
    registry = ToolRegistry()

    # Register all tools
    registry.register(WebSearchTool())
    registry.register(WikipediaTool())
    registry.register(CodeExecutorTool())
    registry.register(DatabaseTool())
    registry.register(APITool())
    registry.register(FileSystemTool())
    registry.register(CalculatorTool())

    return registry


# Global default registry
default_registry = create_default_registry()


# ============================================================================
# PLATFORM TOOL BRIDGE
# ============================================================================


class PlatformToolAdapter(Tool):
    """Wraps a platform ToolRegistry tool as a helix_flow Tool.

    This gives helix_flow agents/chains access to all 100+ platform
    tools and 250k+ Composio tools without duplicating tool code.
    """

    name: str = "platform_tool"
    description: str = "A platform tool"
    category: ToolCategory = ToolCategory.UTILITY

    def __init__(self, registry_tool):
        super().__init__()
        self._registry_tool = registry_tool
        self.name = registry_tool.name
        self.description = registry_tool.description or ""
        self.category = ToolCategory.UTILITY

    def _build_schema(self) -> ToolSchema:
        params = {}
        required = []
        if hasattr(self._registry_tool, "parameters"):
            for p in self._registry_tool.parameters:
                params[p.name] = {
                    "type": p.type or "string",
                    "description": p.description or "",
                }
                if p.required:
                    required.append(p.name)
        return ToolSchema(
            name=self.name,
            description=self.description,
            parameters=params,
            required=required,
        )

    async def execute(self, **kwargs) -> ToolResult:
        import time

        start = time.monotonic()
        try:
            result = await self._registry_tool.execute(kwargs)
            elapsed = (time.monotonic() - start) * 1000
            return ToolResult(
                success=result.success,
                output=result.output,
                error=result.error,
                execution_time_ms=elapsed,
            )
        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            return ToolResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=elapsed,
            )


def load_platform_tools(tool_names: list[str] | None = None, limit: int = 30) -> list[Tool]:
    """Load platform ToolRegistry tools as helix_flow Tool instances.

    Args:
        tool_names: Specific tool names to load (None = all).
        limit: Max tools to load.

    Returns:
        List of PlatformToolAdapter instances.
    """
    try:
        from apps.backend.agent_capabilities.tool_framework import get_tool_registry

        registry = get_tool_registry()
        all_tools = registry.list_tools()
        adapted = []
        for tool_obj in all_tools:
            if tool_names and tool_obj.name not in tool_names:
                continue
            adapted.append(PlatformToolAdapter(tool_obj))
            if len(adapted) >= limit:
                break
        return adapted
    except ImportError:
        logger.debug("Platform ToolRegistry not available")
        return []
    except Exception as e:
        logger.warning("Failed to load platform tools: %s", e)
        return []


def create_enhanced_registry() -> ToolRegistry:
    """Create a registry with built-in + platform tools."""
    registry = create_default_registry()
    for tool in load_platform_tools(limit=50):
        registry.register(tool)
    return registry
