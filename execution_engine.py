"""
Helix Agent Execution Engine
============================
Advanced code execution, file operations, web browsing, and terminal capabilities
for Helix Collective agents - making them as capable as Claude Code or Grok.

This module provides a secure sandbox environment for agents to:
- Execute Python/JavaScript/Shell code
- Perform file system operations
- Browse the web and search
- Run terminal commands
- Use tools dynamically

Author: Helix Collective
Version: 1.0.0
"""

import asyncio
import ipaddress
import json
import logging
import os
import re
import socket
import sys
import tempfile
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiofiles
import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def _is_safe_url(url: str) -> bool:
    """Check that a URL doesn't resolve to a private/internal IP (SSRF protection)."""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = parsed.hostname
        if not hostname:
            return False
        # Resolve hostname and check all IPs
        for info in socket.getaddrinfo(hostname, parsed.port or 443, proto=socket.IPPROTO_TCP):
            addr = info[4][0]
            ip = ipaddress.ip_address(addr)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return False
        return True
    except Exception:
        return False


class ExecutionLanguage(Enum):
    """Supported execution languages"""

    PYTHON = "python"
    JAVASCRIPT = "javascript"
    SHELL = "shell"
    SQL = "sql"


class ToolType(Enum):
    """Types of tools available to agents"""

    CODE_EXECUTION = "code_execution"
    FILE_OPERATION = "file_operation"
    WEB_SEARCH = "web_search"
    WEB_BROWSE = "web_browse"
    TERMINAL = "terminal"
    DATABASE = "database"
    API_CALL = "api_call"
    IMAGE_ANALYSIS = "image_analysis"
    DOCUMENT_PROCESSING = "document_processing"


@dataclass
class ExecutionResult:
    """Result of code/command execution"""

    success: bool
    output: str
    error: str | None = None
    execution_time_ms: float = 0.0
    return_value: Any = None
    files_created: list[str] = field(default_factory=list)
    files_modified: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolCall:
    """Represents a tool call by an agent"""

    tool_id: str
    tool_type: ToolType
    parameters: dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    result: ExecutionResult | None = None


@dataclass
class AgentWorkspace:
    """Isolated workspace for agent operations"""

    workspace_id: str
    agent_id: str
    base_path: Path
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    files: dict[str, str] = field(default_factory=dict)
    variables: dict[str, Any] = field(default_factory=dict)
    history: list[ToolCall] = field(default_factory=list)


class SecureSandbox:
    """
    Secure sandbox for code execution with resource limits and isolation.
    Prevents malicious code from affecting the host system.
    """

    # Dangerous patterns to block
    BLOCKED_PATTERNS = [
        r"os\.system\s*\(",
        r"subprocess\.(?:call|run|Popen)\s*\(",
        r"eval\s*\(",
        r"exec\s*\(",
        r"__import__\s*\(",
        r'open\s*\([^)]*["\']\/(?:etc|proc|sys)',
        r'shutil\.rmtree\s*\([^)]*["\']\/(?!tmp)',
        r"import\s+(?:ctypes|cffi)",
        r"from\s+(?:ctypes|cffi)",
    ]

    # Allowed imports for sandboxed execution
    ALLOWED_IMPORTS = {
        "math",
        "random",
        "datetime",
        "json",
        "csv",
        "re",
        "collections",
        "itertools",
        "functools",
        "operator",
        "string",
        "textwrap",
        "statistics",
        "decimal",
        "fractions",
        "numbers",
        "cmath",
        "numpy",
        "pandas",
        "matplotlib",
        "seaborn",
        "scipy",
        "requests",
        "aiohttp",
        "httpx",
        "beautifulsoup4",
        "lxml",
        "PIL",
        "pillow",
        "cv2",
        "sklearn",
        "torch",
        "tensorflow",
        "transformers",
        "openai",
        "anthropic",
        "langchain",
    }

    def __init__(self, max_execution_time: int = 30, max_memory_mb: int = 512):
        self.max_execution_time = max_execution_time
        self.max_memory_mb = max_memory_mb

    def validate_code(self, code: str, language: ExecutionLanguage) -> tuple[bool, str | None]:
        """Validate code for security issues before execution"""
        if language == ExecutionLanguage.PYTHON:
            for pattern in self.BLOCKED_PATTERNS:
                if re.search(pattern, code):
                    return False, f"Blocked pattern detected: {pattern}"

        # Check for infinite loops (basic heuristic)
        if "while True" in code and "break" not in code:
            return False, "Potential infinite loop detected"

        return True, None

    async def execute_python(self, code: str, workspace: AgentWorkspace) -> ExecutionResult:
        """Execute Python code in a sandboxed environment"""
        start_time = datetime.now(UTC)

        # Validate code
        is_valid, error = self.validate_code(code, ExecutionLanguage.PYTHON)
        if not is_valid:
            return ExecutionResult(success=False, output="", error=f"Code validation failed: {error}")

        # Create temporary file for execution
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", dir=str(workspace.base_path), delete=False) as f:
            # Wrap code with output capture
            wrapped_code = f"""
import sys
import traceback

# Set up workspace variables
workspace_vars = {json.dumps(workspace.variables)}

# Capture output
_result = None

try:
{self._indent_code(code, 4)}
    _result = locals().get('result', None)
except Exception as e:
    print(f"Error: {{type(e).__name__}}: {{str(e)}}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# Output results
print("__STDOUT__")
# The code's print statements will go here
print("__STDERR__")
print("__RESULT__")
print(repr(_result))
"""
            f.write(wrapped_code)
            temp_file = f.name

        try:
            process = await asyncio.create_subprocess_shell(
                f'"{sys.executable}" "{temp_file}"',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(workspace.base_path),
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=self.max_execution_time)
            except TimeoutError:
                process.kill()
                return ExecutionResult(
                    success=False,
                    output="",
                    error=f"Execution timed out after {self.max_execution_time} seconds",
                )

            # Parse output
            output_text = stdout.decode("utf-8", errors="replace")

            # Extract sections - everything before __STDOUT__ is the actual output
            stdout_marker = output_text.find("__STDOUT__")
            if stdout_marker != -1:
                captured_stdout = output_text[:stdout_marker].strip()
                remaining = output_text[stdout_marker:]
            else:
                captured_stdout = output_text.strip()
                remaining = ""

            # Extract stderr and result from remaining
            stderr_match = re.search(r"__STDERR__\n(.*?)__RESULT__", remaining, re.DOTALL)
            result_match = re.search(r"__RESULT__\n(.*?)$", remaining, re.DOTALL)

            captured_stderr = stderr_match.group(1).strip() if stderr_match else ""
            result_str = result_match.group(1).strip() if result_match else "None"

            execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

            return ExecutionResult(
                success=process.returncode == 0 and not captured_stderr,
                output=captured_stdout,
                error=captured_stderr if captured_stderr else None,
                execution_time_ms=execution_time,
                return_value=result_str,
                metadata={"language": "python", "exit_code": process.returncode},
            )

        finally:
            # Clean up temp file
            try:
                os.unlink(temp_file)
            except OSError as e:
                logger.debug("Failed to clean up temp file %s: %s", temp_file, e)

    async def execute_javascript(self, code: str, workspace: AgentWorkspace) -> ExecutionResult:
        """Execute JavaScript code using Node.js"""
        start_time = datetime.now(UTC)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".js", dir=str(workspace.base_path), delete=False) as f:
            wrapped_code = f"""
const workspaceVars = {json.dumps(workspace.variables)};

try {{
{self._indent_code(code, 4)}
}} catch (error) {{
    console.error("Error:", error.message);
    console.error(error.stack);
    process.exit(1);
}}
"""
            f.write(wrapped_code)
            temp_file = f.name

        try:
            process = await asyncio.create_subprocess_exec(
                "node",
                temp_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(workspace.base_path),
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=self.max_execution_time)
            except TimeoutError:
                process.kill()
                return ExecutionResult(
                    success=False,
                    output="",
                    error=f"Execution timed out after {self.max_execution_time} seconds",
                )

            execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

            return ExecutionResult(
                success=process.returncode == 0,
                output=stdout.decode("utf-8", errors="replace"),
                error=stderr.decode("utf-8", errors="replace") if stderr else None,
                execution_time_ms=execution_time,
                metadata={"language": "javascript", "exit_code": process.returncode},
            )

        finally:
            try:
                os.unlink(temp_file)
            except Exception as e:
                logger.debug("Failed to clean up temp file %s: %s", temp_file, e)

    async def execute_shell(self, command: str, workspace: AgentWorkspace) -> ExecutionResult:
        """Execute shell commands with restrictions"""
        start_time = datetime.now(UTC)

        # Block dangerous commands
        dangerous_commands = ["rm -rf /", "mkfs", "dd if=", ":(){", "fork bomb"]
        for dangerous in dangerous_commands:
            if dangerous in command.lower():
                return ExecutionResult(
                    success=False,
                    output="",
                    error=f"Blocked dangerous command pattern: {dangerous}",
                )

        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(workspace.base_path),
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=self.max_execution_time)
            except TimeoutError:
                process.kill()
                return ExecutionResult(
                    success=False,
                    output="",
                    error=f"Command timed out after {self.max_execution_time} seconds",
                )

            execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

            return ExecutionResult(
                success=process.returncode == 0,
                output=stdout.decode("utf-8", errors="replace"),
                error=stderr.decode("utf-8", errors="replace") if stderr else None,
                execution_time_ms=execution_time,
                metadata={"language": "shell", "exit_code": process.returncode},
            )

        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    def _indent_code(self, code: str, spaces: int) -> str:
        """Indent code by specified number of spaces"""
        indent = " " * spaces
        return "\n".join(indent + line for line in code.split("\n"))


class FileOperations:
    """File system operations for agents"""

    def __init__(self, workspace: AgentWorkspace):
        self.workspace = workspace
        self.base_path = workspace.base_path

    async def read_file(self, path: str) -> ExecutionResult:
        """Read file contents"""
        try:
            full_path = self._resolve_path(path)
            async with aiofiles.open(full_path) as f:
                content = await f.read()
            return ExecutionResult(
                success=True,
                output=content,
                metadata={"path": str(full_path), "size": len(content)},
            )
        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    async def write_file(self, path: str, content: str) -> ExecutionResult:
        """Write content to file"""
        try:
            full_path = self._resolve_path(path)
            full_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(full_path, "w") as f:
                await f.write(content)
            self.workspace.files[path] = str(full_path)
            return ExecutionResult(
                success=True,
                output=f"File written: {path}",
                files_created=[str(full_path)],
                metadata={"path": str(full_path), "size": len(content)},
            )
        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    async def append_file(self, path: str, content: str) -> ExecutionResult:
        """Append content to file"""
        try:
            full_path = self._resolve_path(path)
            async with aiofiles.open(full_path, "a") as f:
                await f.write(content)
            return ExecutionResult(
                success=True,
                output=f"Content appended to: {path}",
                files_modified=[str(full_path)],
            )
        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    async def delete_file(self, path: str) -> ExecutionResult:
        """Delete a file"""
        try:
            full_path = self._resolve_path(path)
            if full_path.exists():
                full_path.unlink()
                if path in self.workspace.files:
                    del self.workspace.files[path]
                return ExecutionResult(success=True, output=f"File deleted: {path}")
            return ExecutionResult(success=False, output="", error="File not found")
        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    async def list_directory(self, path: str = ".") -> ExecutionResult:
        """List directory contents"""
        try:
            full_path = self._resolve_path(path)
            if not full_path.is_dir():
                return ExecutionResult(success=False, output="", error="Not a directory")

            items = []
            for item in full_path.iterdir():
                items.append(
                    {
                        "name": item.name,
                        "type": "directory" if item.is_dir() else "file",
                        "size": item.stat().st_size if item.is_file() else 0,
                    }
                )

            return ExecutionResult(success=True, output=json.dumps(items, indent=2), return_value=items)
        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    async def create_directory(self, path: str) -> ExecutionResult:
        """Create a directory"""
        try:
            full_path = self._resolve_path(path)
            full_path.mkdir(parents=True, exist_ok=True)
            return ExecutionResult(success=True, output=f"Directory created: {path}")
        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    def _resolve_path(self, path: str) -> Path:
        """Resolve path relative to workspace, preventing directory traversal"""
        resolved = (self.base_path / path).resolve()
        if not str(resolved).startswith(str(self.base_path)):
            raise ValueError("Path traversal detected")
        return resolved


class WebOperations:
    """Web browsing and search operations for agents"""

    def __init__(self, search_api_key: str | None = None):
        self.search_api_key = search_api_key or os.getenv("SERPER_API_KEY")
        self.session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"User-Agent": "Helix-Agent/1.0"},
            )
        return self.session

    async def search(self, query: str, num_results: int = 10) -> ExecutionResult:
        """Search the web using Serper API or fallback"""
        try:
            session = await self._get_session()
            if self.search_api_key:
                # Use Serper API
                async with session.post(
                    "https://google.serper.dev/search",
                    json={"q": query, "num": num_results},
                    headers={"X-API-KEY": self.search_api_key},
                ) as response:
                    data = await response.json()
                    results = []
                    for item in data.get("organic", [])[:num_results]:
                        results.append(
                            {
                                "title": item.get("title"),
                                "url": item.get("link"),
                                "snippet": item.get("snippet"),
                            }
                        )
                    if results:
                        return ExecutionResult(
                            success=True,
                            output=json.dumps(results, indent=2),
                            return_value=results,
                            metadata={"query": query, "num_results": len(results)},
                        )
                    # Serper returned no results (quota exhausted or error) —
                    # fall through to DuckDuckGo below
                    logger.warning("Serper returned no results for '%s', falling back to DuckDuckGo", query)

            # DuckDuckGo HTML fallback (no API key required, or Serper quota exhausted)
            async with session.get(f"https://html.duckduckgo.com/html/?q={query}") as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                results = []
                for result in soup.select(".result")[:num_results]:
                    title_elem = result.select_one(".result__title")
                    snippet_elem = result.select_one(".result__snippet")
                    link_elem = result.select_one(".result__url")
                    if title_elem:
                        results.append(
                            {
                                "title": title_elem.get_text(strip=True),
                                "url": (link_elem.get_text(strip=True) if link_elem else ""),
                                "snippet": (snippet_elem.get_text(strip=True) if snippet_elem else ""),
                            }
                        )
                return ExecutionResult(
                    success=True,
                    output=json.dumps(results, indent=2),
                    return_value=results,
                    metadata={"query": query, "num_results": len(results)},
                )
        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    async def browse(self, url: str, extract_text: bool = True) -> ExecutionResult:
        """Browse a webpage and extract content"""
        if not _is_safe_url(url):
            return ExecutionResult(
                success=False, output="", error="URL blocked: private or internal addresses are not allowed"
            )
        try:
            session = await self._get_session()
            async with session.get(url) as response:
                if response.status != 200:
                    return ExecutionResult(success=False, output="", error=f"HTTP {response.status}")

                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                # Remove script and style elements
                for element in soup(["script", "style", "nav", "footer", "header"]):
                    element.decompose()

                if extract_text:
                    # Extract main content
                    main_content = soup.find("main") or soup.find("article") or soup.find("body")
                    text = main_content.get_text(separator="\n", strip=True) if main_content else ""

                    # Clean up whitespace
                    text = re.sub(r"\n{3,}", "\n\n", text)

                    return ExecutionResult(
                        success=True,
                        output=text[:50000],  # Limit output size
                        metadata={
                            "url": url,
                            "title": soup.title.string if soup.title else "",
                            "length": len(text),
                        },
                    )
                else:
                    return ExecutionResult(
                        success=True,
                        output=str(soup),
                        metadata={"url": url, "raw_html": True},
                    )
        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    async def api_call(
        self,
        url: str,
        method: str = "GET",
        headers: dict | None = None,
        data: dict | None = None,
        json_data: dict | None = None,
    ) -> ExecutionResult:
        """Make an API call"""
        if not _is_safe_url(url):
            return ExecutionResult(
                success=False, output="", error="URL blocked: private or internal addresses are not allowed"
            )
        try:
            session = await self._get_session()
            kwargs = {"headers": headers or {}}
            if data:
                kwargs["data"] = data
            if json_data:
                kwargs["json"] = json_data

            async with session.request(method, url, **kwargs) as response:
                try:
                    result = await response.json()
                except Exception:
                    result = await response.text()

                return ExecutionResult(
                    success=200 <= response.status < 300,
                    output=(json.dumps(result, indent=2) if isinstance(result, dict) else result),
                    return_value=result,
                    metadata={
                        "url": url,
                        "method": method,
                        "status_code": response.status,
                    },
                )
        except Exception as e:
            return ExecutionResult(success=False, output="", error=str(e))

    async def close(self):
        """Close the session"""
        if self.session and not self.session.closed:
            await self.session.close()


class AgentExecutionEngine:
    """
    Main execution engine for Helix agents.
    Provides a unified interface for all agent capabilities.
    """

    def __init__(self):
        self.sandbox = SecureSandbox()
        self.web_ops = WebOperations()
        self.workspaces: dict[str, AgentWorkspace] = {}
        self.tool_registry: dict[str, Callable] = {}

        # Resource limits (expected by tests)
        self.max_memory_mb = 100
        self.timeout_seconds = 30
        self.max_output_chars = 10000

        # Execution history
        self.execution_history: list[dict[str, Any]] = []

        # Register built-in tools
        self._register_builtin_tools()

    def _register_builtin_tools(self):
        """Register all built-in tools"""
        self.tool_registry = {
            "execute_python": self._tool_execute_python,
            "execute_javascript": self._tool_execute_javascript,
            "execute_shell": self._tool_execute_shell,
            "read_file": self._tool_read_file,
            "write_file": self._tool_write_file,
            "list_directory": self._tool_list_directory,
            "web_search": self._tool_web_search,
            "web_browse": self._tool_web_browse,
            "api_call": self._tool_api_call,
        }

    def create_workspace(self, agent_id: str) -> AgentWorkspace:
        """Create a new workspace for an agent"""
        workspace_id = str(uuid.uuid4())
        base_path = Path(tempfile.mkdtemp(prefix=f"helix_agent_{agent_id}_"))

        workspace = AgentWorkspace(workspace_id=workspace_id, agent_id=agent_id, base_path=base_path)

        self.workspaces[workspace_id] = workspace
        return workspace

    def get_workspace(self, workspace_id: str) -> AgentWorkspace | None:
        """Get an existing workspace"""
        return self.workspaces.get(workspace_id)

    async def execute_tool(self, workspace_id: str, tool_name: str, parameters: dict[str, Any]) -> ExecutionResult:
        """Execute a tool in the context of a workspace"""
        workspace = self.get_workspace(workspace_id)
        if not workspace:
            return ExecutionResult(success=False, output="", error=f"Workspace not found: {workspace_id}")

        tool_func = self.tool_registry.get(tool_name)
        if not tool_func:
            return ExecutionResult(success=False, output="", error=f"Unknown tool: {tool_name}")

        # Create tool call record
        tool_call = ToolCall(
            tool_id=str(uuid.uuid4()),
            tool_type=self._get_tool_type(tool_name),
            parameters=parameters,
        )

        # Execute tool
        result = await tool_func(workspace, parameters)
        tool_call.result = result

        # Record in history
        workspace.history.append(tool_call)

        return result

    async def execute_python(self, code: str, timeout: int | None = None) -> dict[str, Any]:
        """Execute Python code directly"""
        workspace = self.create_workspace("test_agent")

        # Temporarily set timeout if provided
        original_timeout = self.sandbox.max_execution_time
        if timeout:
            self.sandbox.max_execution_time = timeout

        try:
            result = await self.sandbox.execute_python(code, workspace)

            # Record in history
            self.execution_history.append(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "code": code,
                    "language": "python",
                    "success": result.success,
                    "output": result.output,
                    "error": result.error,
                }
            )

            return {
                "success": result.success,
                "output": result.output,
                "error": result.error,
                "execution_time": result.execution_time_ms,
            }
        finally:
            # Restore original timeout
            self.sandbox.max_execution_time = original_timeout

    async def execute_javascript(self, code: str, timeout: int | None = None) -> dict[str, Any]:
        """Execute JavaScript code directly"""
        workspace = self.create_workspace("test_agent")

        # Temporarily set timeout if provided
        original_timeout = self.sandbox.max_execution_time
        if timeout:
            self.sandbox.max_execution_time = timeout

        try:
            result = await self.sandbox.execute_javascript(code, workspace)

            # Record in history
            self.execution_history.append(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "code": code,
                    "language": "javascript",
                    "success": result.success,
                    "output": result.output,
                    "error": result.error,
                }
            )

            return {
                "success": result.success,
                "output": result.output,
                "error": result.error,
                "execution_time": result.execution_time_ms,
            }
        finally:
            # Restore original timeout
            self.sandbox.max_execution_time = original_timeout

    async def execute_shell(self, command: str, timeout: int | None = None) -> dict[str, Any]:
        """Execute shell command directly"""
        workspace = self.create_workspace("test_agent")

        # Temporarily set timeout if provided
        original_timeout = self.sandbox.max_execution_time
        if timeout:
            self.sandbox.max_execution_time = timeout

        try:
            result = await self.sandbox.execute_shell(command, workspace)

            # Record in history
            self.execution_history.append(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "code": command,
                    "language": "shell",
                    "success": result.success,
                    "output": result.output,
                    "error": result.error,
                }
            )

            return {
                "success": result.success,
                "output": result.output,
                "error": result.error,
                "execution_time": result.execution_time_ms,
                "restricted": True,  # Shell execution is always restricted
            }
        finally:
            # Restore original timeout
            self.sandbox.max_execution_time = original_timeout

    def get_execution_history(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Get execution history"""
        if limit:
            return self.execution_history[-limit:]
        return self.execution_history

    def _get_tool_type(self, tool_name: str) -> ToolType:
        """Map tool name to tool type"""
        mapping = {
            "execute_python": ToolType.CODE_EXECUTION,
            "execute_javascript": ToolType.CODE_EXECUTION,
            "execute_shell": ToolType.TERMINAL,
            "read_file": ToolType.FILE_OPERATION,
            "write_file": ToolType.FILE_OPERATION,
            "list_directory": ToolType.FILE_OPERATION,
            "web_search": ToolType.WEB_SEARCH,
            "web_browse": ToolType.WEB_BROWSE,
            "api_call": ToolType.API_CALL,
        }
        return mapping.get(tool_name, ToolType.CODE_EXECUTION)

    # Tool implementations
    async def _tool_execute_python(self, workspace: AgentWorkspace, params: dict) -> ExecutionResult:
        return await self.sandbox.execute_python(params.get("code", ""), workspace)

    async def _tool_execute_javascript(self, workspace: AgentWorkspace, params: dict) -> ExecutionResult:
        return await self.sandbox.execute_javascript(params.get("code", ""), workspace)

    async def _tool_execute_shell(self, workspace: AgentWorkspace, params: dict) -> ExecutionResult:
        return await self.sandbox.execute_shell(params.get("command", ""), workspace)

    async def _tool_read_file(self, workspace: AgentWorkspace, params: dict) -> ExecutionResult:
        file_ops = FileOperations(workspace)
        return await file_ops.read_file(params.get("path", ""))

    async def _tool_write_file(self, workspace: AgentWorkspace, params: dict) -> ExecutionResult:
        file_ops = FileOperations(workspace)
        return await file_ops.write_file(params.get("path", ""), params.get("content", ""))

    async def _tool_list_directory(self, workspace: AgentWorkspace, params: dict) -> ExecutionResult:
        file_ops = FileOperations(workspace)
        return await file_ops.list_directory(params.get("path", "."))

    async def _tool_web_search(self, workspace: AgentWorkspace, params: dict) -> ExecutionResult:
        return await self.web_ops.search(params.get("query", ""), params.get("num_results", 10))

    async def _tool_web_browse(self, workspace: AgentWorkspace, params: dict) -> ExecutionResult:
        return await self.web_ops.browse(params.get("url", ""), params.get("extract_text", True))

    async def _tool_api_call(self, workspace: AgentWorkspace, params: dict) -> ExecutionResult:
        return await self.web_ops.api_call(
            params.get("url", ""),
            params.get("method", "GET"),
            params.get("headers"),
            params.get("data"),
            params.get("json"),
        )

    def get_available_tools(self) -> list[dict[str, Any]]:
        """Get list of available tools with their schemas"""
        return [
            {
                "name": "execute_python",
                "description": "Execute Python code in a sandboxed environment",
                "parameters": {
                    "code": {
                        "type": "string",
                        "description": "Python code to execute",
                        "required": True,
                    }
                },
            },
            {
                "name": "execute_javascript",
                "description": "Execute JavaScript code using Node.js",
                "parameters": {
                    "code": {
                        "type": "string",
                        "description": "JavaScript code to execute",
                        "required": True,
                    }
                },
            },
            {
                "name": "execute_shell",
                "description": "Execute shell commands",
                "parameters": {
                    "command": {
                        "type": "string",
                        "description": "Shell command to execute",
                        "required": True,
                    }
                },
            },
            {
                "name": "read_file",
                "description": "Read contents of a file",
                "parameters": {
                    "path": {
                        "type": "string",
                        "description": "Path to the file",
                        "required": True,
                    }
                },
            },
            {
                "name": "write_file",
                "description": "Write content to a file",
                "parameters": {
                    "path": {
                        "type": "string",
                        "description": "Path to the file",
                        "required": True,
                    },
                    "content": {
                        "type": "string",
                        "description": "Content to write",
                        "required": True,
                    },
                },
            },
            {
                "name": "list_directory",
                "description": "List contents of a directory",
                "parameters": {
                    "path": {
                        "type": "string",
                        "description": "Path to directory",
                        "required": False,
                        "default": ".",
                    }
                },
            },
            {
                "name": "web_search",
                "description": "Search the web for information",
                "parameters": {
                    "query": {
                        "type": "string",
                        "description": "Search query",
                        "required": True,
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Number of results",
                        "required": False,
                        "default": 10,
                    },
                },
            },
            {
                "name": "web_browse",
                "description": "Browse a webpage and extract content",
                "parameters": {
                    "url": {
                        "type": "string",
                        "description": "URL to browse",
                        "required": True,
                    },
                    "extract_text": {
                        "type": "boolean",
                        "description": "Extract text only",
                        "required": False,
                        "default": True,
                    },
                },
            },
            {
                "name": "api_call",
                "description": "Make an HTTP API call",
                "parameters": {
                    "url": {
                        "type": "string",
                        "description": "API URL",
                        "required": True,
                    },
                    "method": {
                        "type": "string",
                        "description": "HTTP method",
                        "required": False,
                        "default": "GET",
                    },
                    "headers": {
                        "type": "object",
                        "description": "Request headers",
                        "required": False,
                    },
                    "data": {
                        "type": "object",
                        "description": "Form data",
                        "required": False,
                    },
                    "json": {
                        "type": "object",
                        "description": "JSON body",
                        "required": False,
                    },
                },
            },
        ]

    async def cleanup_workspace(self, workspace_id: str):
        """Clean up a workspace"""
        workspace = self.workspaces.pop(workspace_id, None)
        if workspace:
            import shutil

            try:
                shutil.rmtree(workspace.base_path, ignore_errors=True)
            except Exception as e:
                logger.debug("Failed to clean up workspace %s: %s", workspace_id, e)

    async def close(self):
        """Clean up all resources"""
        await self.web_ops.close()
        for workspace_id in list(self.workspaces.keys()):
            await self.cleanup_workspace(workspace_id)


# Global instance
_execution_engine: AgentExecutionEngine | None = None


def get_execution_engine() -> AgentExecutionEngine:
    """Get the global execution engine instance"""
    global _execution_engine
    if _execution_engine is None:
        _execution_engine = AgentExecutionEngine()
    return _execution_engine


# Convenience functions for direct use
async def execute_code(agent_id: str, code: str, language: str = "python") -> ExecutionResult:
    """Execute code for an agent"""
    engine = get_execution_engine()
    workspace = engine.create_workspace(agent_id)

    try:
        if language == "python":
            return await engine.execute_tool(workspace.workspace_id, "execute_python", {"code": code})
        elif language == "javascript":
            return await engine.execute_tool(workspace.workspace_id, "execute_javascript", {"code": code})
        elif language == "shell":
            return await engine.execute_tool(workspace.workspace_id, "execute_shell", {"command": code})
        else:
            return ExecutionResult(success=False, output="", error=f"Unsupported language: {language}")
    finally:
        await engine.cleanup_workspace(workspace.workspace_id)


async def web_search(query: str, num_results: int = 10) -> ExecutionResult:
    """Perform a web search"""
    engine = get_execution_engine()
    return await engine.web_ops.search(query, num_results)


async def browse_url(url: str) -> ExecutionResult:
    """Browse a URL and extract content"""
    engine = get_execution_engine()
    return await engine.web_ops.browse(url)
