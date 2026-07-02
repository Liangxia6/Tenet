from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass(slots=True)
class ToolResult:
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    is_error: bool = False
    duration_ms: int = 0


class ToolRegistry:
    def __init__(self, dangerous_patterns: list[str] | None = None) -> None:
        self.dangerous_patterns = dangerous_patterns or ["rm -rf /", "mkfs.", "dd if=", ":(){ :|:& };:"]
        self._tools: dict[str, Callable[[dict[str, Any], Path], ToolResult]] = {
            "read_file": self._read_file,
            "write_file": self._write_file,
            "shell": self._shell,
            "web_search": self._web_search,
        }

    def execute(self, tool_name: str, arguments: str | dict[str, Any], workspace: str | os.PathLike[str]) -> ToolResult:
        tool = self._tools.get(tool_name)
        if tool is None:
            return ToolResult(stderr=f"unknown tool: {tool_name}", exit_code=1, is_error=True)
        if isinstance(arguments, str):
            try:
                args = json.loads(arguments or "{}")
            except json.JSONDecodeError as exc:
                return ToolResult(stderr=f"invalid JSON arguments: {exc}", exit_code=1, is_error=True)
        else:
            args = arguments
        start = time.monotonic()
        try:
            result = tool(args, Path(workspace))
        except Exception as exc:  # noqa: BLE001 - convert physical tool failures into RPC-safe results.
            result = ToolResult(stderr=str(exc), exit_code=1, is_error=True)
        result.duration_ms = int((time.monotonic() - start) * 1000)
        return result

    def definitions(self) -> list[dict[str, str]]:
        return [
            {
                "name": "read_file",
                "description": "Read a workspace file with optional line pagination.",
                "parameters_schema": json.dumps({"type": "object", "required": ["path"]}),
            },
            {
                "name": "write_file",
                "description": "Overwrite a workspace file, creating parent directories.",
                "parameters_schema": json.dumps({"type": "object", "required": ["path", "content"]}),
            },
            {
                "name": "shell",
                "description": "Run a shell command inside the workspace.",
                "parameters_schema": json.dumps({"type": "object", "required": ["command"]}),
            },
            {
                "name": "web_search",
                "description": "Return a deterministic TODO stub for web search.",
                "parameters_schema": json.dumps({"type": "object", "required": ["query"]}),
            },
        ]

    def _read_file(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        path = _safe_path(workspace, str(args.get("path", "")))
        offset = max(1, int(args.get("offset", 1)))
        limit = max(1, int(args.get("limit", 500)))
        lines = path.read_text(encoding="utf-8").splitlines()
        selected = lines[offset - 1 : offset - 1 + limit]
        return ToolResult(stdout="\n".join(selected))

    def _write_file(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        path = _safe_path(workspace, str(args.get("path", "")))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(args.get("content", "")), encoding="utf-8")
        return ToolResult(stdout=f"wrote {path.relative_to(workspace.resolve())}")

    def _shell(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        command = str(args.get("command", ""))
        if not command:
            return ToolResult(stderr="command is required", exit_code=1, is_error=True)
        for pattern in self.dangerous_patterns:
            if pattern and pattern in command:
                return ToolResult(stderr=f"blocked dangerous command pattern: {pattern}", exit_code=126, is_error=True)
        timeout = max(1, int(args.get("timeout", 60)))
        completed = subprocess.run(
            command,
            cwd=workspace.resolve(),
            shell=True,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        return ToolResult(
            stdout=completed.stdout,
            stderr=completed.stderr,
            exit_code=completed.returncode,
            is_error=completed.returncode != 0,
        )

    def _web_search(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        del workspace
        query = str(args.get("query", ""))
        limit = int(args.get("limit", 5))
        return ToolResult(stdout=json.dumps({"query": query, "limit": limit, "results": [], "status": "TODO"}))


def _safe_path(workspace: Path, user_path: str) -> Path:
    if not user_path:
        raise PermissionError("path is required")
    root = workspace.resolve()
    candidate = (root / user_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise PermissionError(f"path escapes workspace: {user_path}") from exc
    return candidate
