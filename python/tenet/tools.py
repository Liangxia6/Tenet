from __future__ import annotations

import json
import os
import fnmatch
import ipaddress
import socket
import urllib.parse
import urllib.request
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
            "append_file": self._append_file,
            "replace_in_file": self._replace_in_file,
            "list_dir": self._list_dir,
            "search_files": self._search_files,
            "grep": self._grep,
            "file_info": self._file_info,
            "shell": self._shell,
            "git_status": self._git_status,
            "git_diff": self._git_diff,
            "apply_patch": self._apply_patch,
            "git_log": self._git_log,
            "git_show": self._git_show,
            "git_branch": self._git_branch,
            "http_fetch": self._http_fetch,
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
        if not isinstance(args, dict):
            return ToolResult(stderr="invalid arguments: root must be an object", exit_code=2, is_error=True)
        validation_error = _validate_tool_arguments(tool_name, args)
        if validation_error:
            return ToolResult(stderr=f"invalid arguments: {validation_error}", exit_code=2, is_error=True)
        start = time.monotonic()
        try:
            result = tool(args, Path(workspace))
        except Exception as exc:  # noqa: BLE001 - convert physical tool failures into RPC-safe results.
            result = ToolResult(stderr=str(exc), exit_code=1, is_error=True)
        result.duration_ms = int((time.monotonic() - start) * 1000)
        return result

    def definitions(self) -> list[dict[str, str]]:
        manifest = _load_tool_manifest()
        if manifest:
            return _filter_tool_definitions(manifest)
        fallback = [
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
            {"name": "append_file", "description": "Append content to a workspace file.", "parameters_schema": json.dumps({"type": "object", "required": ["path", "content"]})},
            {"name": "replace_in_file", "description": "Replace text in a workspace file.", "parameters_schema": json.dumps({"type": "object", "required": ["path", "old", "new"]})},
            {"name": "list_dir", "description": "List files and directories in the workspace.", "parameters_schema": json.dumps({"type": "object"})},
            {"name": "search_files", "description": "Find files by name substring or glob.", "parameters_schema": json.dumps({"type": "object", "required": ["query"]})},
            {"name": "grep", "description": "Search text within workspace files.", "parameters_schema": json.dumps({"type": "object", "required": ["query"]})},
            {"name": "file_info", "description": "Return workspace file metadata.", "parameters_schema": json.dumps({"type": "object", "required": ["path"]})},
            {"name": "git_status", "description": "Return git status --short.", "parameters_schema": json.dumps({"type": "object"})},
            {"name": "git_diff", "description": "Return git diff.", "parameters_schema": json.dumps({"type": "object"})},
            {"name": "apply_patch", "description": "Apply a unified diff patch inside the workspace.", "parameters_schema": json.dumps({"type": "object", "required": ["patch"]})},
            {"name": "git_log", "description": "Return recent git commits.", "parameters_schema": json.dumps({"type": "object"})},
            {"name": "git_show", "description": "Show a git revision or object.", "parameters_schema": json.dumps({"type": "object", "required": ["rev"]})},
            {"name": "git_branch", "description": "Return git branch information.", "parameters_schema": json.dumps({"type": "object"})},
            {"name": "http_fetch", "description": "Fetch a URL with GET.", "parameters_schema": json.dumps({"type": "object", "required": ["url"]})},
            {
                "name": "web_search",
                "description": "Return a deterministic TODO stub for web search.",
                "parameters_schema": json.dumps({"type": "object", "required": ["query"]}),
            },
        ]
        return _filter_tool_definitions(fallback)

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

    def _append_file(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        path = _safe_path(workspace, str(args.get("path", "")))
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(str(args.get("content", "")))
        return ToolResult(stdout=f"appended {path.relative_to(workspace.resolve())}")

    def _replace_in_file(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        path = _safe_path(workspace, str(args.get("path", "")))
        old = str(args.get("old", ""))
        if not old:
            return ToolResult(stderr="old is required", exit_code=1, is_error=True)
        new = str(args.get("new", ""))
        count = int(args.get("count", -1))
        content = path.read_text(encoding="utf-8")
        replaced = content.count(old) if count < 0 else min(content.count(old), count)
        if replaced == 0:
            return ToolResult(stderr="old text not found", exit_code=1, is_error=True)
        path.write_text(content.replace(old, new, count), encoding="utf-8")
        return ToolResult(stdout=json.dumps({"path": str(path.relative_to(workspace.resolve())), "replaced": replaced}))

    def _list_dir(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        root = _safe_path(workspace, str(args.get("path", ".")))
        recursive = bool(args.get("recursive", False))
        max_entries = _bounded_int(args.get("max_entries", 200), 1, 1000)
        entries: list[dict[str, Any]] = []
        iterator = root.rglob("*") if recursive else root.iterdir()
        for item in iterator:
            if len(entries) >= max_entries:
                break
            if _should_skip(item):
                continue
            entries.append({"path": str(item.relative_to(workspace.resolve())), "dir": item.is_dir(), "size": item.stat().st_size})
        entries.sort(key=lambda entry: entry["path"])
        return ToolResult(stdout=json.dumps({"entries": entries, "truncated": len(entries) >= max_entries}))

    def _search_files(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        query = str(args.get("query", ""))
        if not query:
            return ToolResult(stderr="query is required", exit_code=1, is_error=True)
        root = _safe_path(workspace, str(args.get("path", ".")))
        max_results = _bounded_int(args.get("max_results", 100), 1, 1000)
        matches: list[str] = []
        for item in root.rglob("*"):
            if len(matches) >= max_results:
                break
            if _should_skip(item) or item.is_dir():
                continue
            if query.lower() in item.name.lower() or fnmatch.fnmatch(item.name, query):
                matches.append(str(item.relative_to(workspace.resolve())))
        return ToolResult(stdout=json.dumps({"matches": matches, "truncated": len(matches) >= max_results}))

    def _grep(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        query = str(args.get("query", ""))
        if not query:
            return ToolResult(stderr="query is required", exit_code=1, is_error=True)
        root = _safe_path(workspace, str(args.get("path", ".")))
        case_sensitive = bool(args.get("case_sensitive", False))
        max_matches = _bounded_int(args.get("max_matches", 100), 1, 1000)
        needle = query if case_sensitive else query.lower()
        matches: list[dict[str, Any]] = []
        for item in root.rglob("*"):
            if len(matches) >= max_matches:
                break
            if _should_skip(item) or item.is_dir() or item.stat().st_size > 2 * 1024 * 1024:
                continue
            try:
                lines = item.read_text(encoding="utf-8").splitlines()
            except UnicodeDecodeError:
                continue
            for idx, line in enumerate(lines, start=1):
                haystack = line if case_sensitive else line.lower()
                if needle in haystack:
                    matches.append({"path": str(item.relative_to(workspace.resolve())), "line": idx, "text": line})
                    if len(matches) >= max_matches:
                        break
        return ToolResult(stdout=json.dumps({"matches": matches, "truncated": len(matches) >= max_matches}))

    def _file_info(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        path = _safe_path(workspace, str(args.get("path", "")))
        stat = path.stat()
        return ToolResult(stdout=json.dumps({
            "path": str(path.relative_to(workspace.resolve())),
            "dir": path.is_dir(),
            "size": stat.st_size,
            "modified": int(stat.st_mtime),
        }))

    def _shell(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        command = str(args.get("command", ""))
        if not command:
            return ToolResult(stderr="command is required", exit_code=1, is_error=True)
        for pattern in self.dangerous_patterns:
            if pattern and pattern in command:
                return ToolResult(stderr=f"blocked dangerous command pattern: {pattern}", exit_code=126, is_error=True)
        timeout = max(1, int(args.get("timeout_seconds", args.get("timeout", 60))))
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

    def _git_status(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        del args
        return self._shell({"command": "git status --short", "timeout": 30}, workspace)

    def _git_diff(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        path = str(args.get("path", ""))
        command = "git diff --"
        if path:
            _safe_path(workspace, path)
            command = f"git diff -- {sh_quote(path)}"
        result = self._shell({"command": command, "timeout": 30}, workspace)
        max_bytes = _bounded_int(args.get("max_bytes", 60000), 1, 200000)
        if len(result.stdout) > max_bytes:
            result.stdout = result.stdout[:max_bytes] + "\n...truncated..."
        return result

    def _apply_patch(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        patch_text = str(args.get("patch", ""))
        if not patch_text.strip():
            return ToolResult(stderr="patch is required", exit_code=1, is_error=True)
        try:
            _validate_patch_paths(workspace, patch_text)
        except PermissionError as exc:
            return ToolResult(stderr=str(exc), exit_code=1, is_error=True)
        strip = _bounded_int(args.get("strip", 0), 0, 10)
        completed = subprocess.run(
            ["patch", f"-p{strip}", "--forward"],
            input=patch_text,
            cwd=workspace.resolve(),
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        return ToolResult(
            stdout=completed.stdout,
            stderr=completed.stderr or completed.stdout,
            exit_code=completed.returncode,
            is_error=completed.returncode != 0,
        )

    def _git_log(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        limit = _bounded_int(args.get("limit", 20), 1, 100)
        git_args = ["log", f"-{limit}", "--date=iso", "--pretty=format:%h\t%ad\t%s"]
        path = str(args.get("path", ""))
        if path:
            _safe_path(workspace, path)
            git_args.extend(["--", path])
        return _run_git(workspace, git_args, _bounded_int(args.get("max_bytes", 60000), 1, 200000))

    def _git_show(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        rev = str(args.get("rev", ""))
        if not rev.strip():
            return ToolResult(stderr="rev is required", exit_code=1, is_error=True)
        if any(char in rev for char in "\x00\r\n"):
            return ToolResult(stderr="rev contains invalid control characters", exit_code=1, is_error=True)
        return _run_git(workspace, ["show", "--stat", "--patch", rev], _bounded_int(args.get("max_bytes", 60000), 1, 200000))

    def _git_branch(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        if bool(args.get("all", False)):
            return _run_git(workspace, ["branch", "--all", "--verbose"], _bounded_int(args.get("max_bytes", 60000), 1, 200000))
        return _run_git(workspace, ["branch", "--show-current"], _bounded_int(args.get("max_bytes", 60000), 1, 200000))

    def _http_fetch(self, args: dict[str, Any], workspace: Path) -> ToolResult:
        del workspace
        url = str(args.get("url", ""))
        if not (url.startswith("http://") or url.startswith("https://")):
            return ToolResult(stderr="url must start with http:// or https://", exit_code=1, is_error=True)
        try:
            _validate_fetch_url(url)
        except PermissionError as exc:
            return ToolResult(stderr=str(exc), exit_code=1, is_error=True)
        max_bytes = _bounded_int(args.get("max_bytes", 60000), 1, 200000)
        request = urllib.request.Request(url, headers={"User-Agent": "Tenet/0.1"})
        with urllib.request.urlopen(request, timeout=15) as response:  # noqa: S310 - user-provided URL fetch is an explicit tool.
            data = response.read(max_bytes + 1)
            truncated = len(data) > max_bytes
            if truncated:
                data = data[:max_bytes]
            return ToolResult(stdout=json.dumps({
                "status": response.status,
                "content_type": response.headers.get("Content-Type", ""),
                "body": data.decode("utf-8", errors="replace"),
                "truncated": truncated,
            }))


def _load_tool_manifest() -> list[dict[str, str]]:
    here = Path(__file__).resolve()
    candidates = [
        here.parents[2] / "tools" / "builtin_tools.json",
        Path.cwd() / "tools" / "builtin_tools.json",
        Path.cwd().parent / "tools" / "builtin_tools.json",
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        data = json.loads(candidate.read_text(encoding="utf-8"))
        return [
            {
                "name": str(item["name"]),
                "description": str(item["description"]),
                "parameters_schema": str(item["parameters_schema"]),
            }
            for item in data
        ]
    return []


def _filter_tool_definitions(definitions: list[dict[str, str]]) -> list[dict[str, str]]:
    if _web_search_definition_enabled():
        return definitions
    return [item for item in definitions if item.get("name") != "web_search"]


def _web_search_definition_enabled() -> bool:
    return (
        os.environ.get("TENET_WEB_SEARCH_ENABLED", "").lower() == "true"
        or bool(os.environ.get("BRAVE_API_KEY"))
        or bool(os.environ.get("TAVILY_API_KEY"))
        or bool(os.environ.get("BING_API_KEY"))
    )


def _validate_tool_arguments(tool_name: str, args: dict[str, Any]) -> str | None:
    definition = _tool_definition_by_name(tool_name)
    if not definition:
        return None
    try:
        schema = json.loads(definition.get("parameters_schema", "") or "{}")
    except json.JSONDecodeError as exc:
        return f"invalid schema for {tool_name}: {exc}"
    if schema.get("type") not in (None, "", "object"):
        return f"unsupported root schema type {schema.get('type')!r}"
    for required in schema.get("required", []):
        if required not in args:
            return f"{required} is required"
    properties = schema.get("properties", {})
    if not isinstance(properties, dict):
        return None
    for name, prop in properties.items():
        if name not in args or args[name] is None or not isinstance(prop, dict):
            continue
        error = _validate_tool_property(name, args[name], prop)
        if error:
            return error
    return None


def _tool_definition_by_name(tool_name: str) -> dict[str, str] | None:
    definitions = _load_tool_manifest() or _fallback_tool_definitions()
    for definition in definitions:
        if definition.get("name") == tool_name:
            return definition
    return None


def _fallback_tool_definitions() -> list[dict[str, str]]:
    return [
        {"name": "read_file", "description": "Read a workspace file.", "parameters_schema": json.dumps({"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]})},
        {"name": "write_file", "description": "Overwrite a workspace file.", "parameters_schema": json.dumps({"type": "object", "properties": {"path": {"type": "string"}, "content": {"type": "string"}}, "required": ["path", "content"]})},
        {"name": "shell", "description": "Run a shell command.", "parameters_schema": json.dumps({"type": "object", "properties": {"command": {"type": "string"}, "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 600}}, "required": ["command"]})},
    ]


def _validate_tool_property(name: str, value: Any, schema: dict[str, Any]) -> str | None:
    expected = schema.get("type")
    if expected == "string":
        if not isinstance(value, str):
            return f"{name} must be a string"
    elif expected == "boolean":
        if not isinstance(value, bool):
            return f"{name} must be a boolean"
    elif expected == "integer":
        if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
            return f"{name} must be an integer"
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if minimum is not None and value < minimum:
            return f"{name} must be >= {_format_schema_number(minimum)}"
        if maximum is not None and value > maximum:
            return f"{name} must be <= {_format_schema_number(maximum)}"
    elif expected:
        return f"{name} has unsupported schema type {expected!r}"
    return None


def _format_schema_number(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


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


def _validate_patch_paths(workspace: Path, patch_text: str) -> None:
    for line in patch_text.splitlines():
        if not (line.startswith("+++ ") or line.startswith("--- ")):
            continue
        parts = line[4:].split()
        if not parts or parts[0] == "/dev/null":
            continue
        path = parts[0]
        if path.startswith("a/") or path.startswith("b/"):
            path = path[2:]
        _safe_path(workspace, path)


def _run_git(workspace: Path, args: list[str], max_bytes: int) -> ToolResult:
    completed = subprocess.run(
        ["git", *args],
        cwd=workspace.resolve(),
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    stdout = completed.stdout
    if len(stdout) > max_bytes:
        stdout = stdout[:max_bytes] + "\n...truncated..."
    return ToolResult(
        stdout=stdout,
        stderr=completed.stderr or (stdout if completed.returncode != 0 else ""),
        exit_code=completed.returncode,
        is_error=completed.returncode != 0,
    )


def _validate_fetch_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname
    if not host:
        raise PermissionError("url host is required")
    if host.lower() == "localhost":
        raise PermissionError("refusing to fetch localhost URL")
    for _, _, _, _, sockaddr in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM):
        ip = ipaddress.ip_address(sockaddr[0])
        if (
            ip.is_loopback
            or ip.is_private
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_unspecified
            or ip.is_reserved
        ):
            raise PermissionError(f"refusing to fetch private or local address: {ip}")


def _should_skip(path: Path) -> bool:
    return any(part in {".git", "node_modules", ".venv", "dist", "build", "__pycache__"} for part in path.parts)


def _bounded_int(value: Any, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = minimum
    return max(minimum, min(maximum, parsed))


def sh_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"
