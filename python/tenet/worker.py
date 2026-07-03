from __future__ import annotations

import time
from dataclasses import asdict
import os
from typing import Any, Callable

from .adapters import LLMAdapter, ProviderRouter
from .tools import ToolRegistry, ToolResult


class StatelessWorker:
    def __init__(
        self,
        adapter: LLMAdapter | None = None,
        tools: ToolRegistry | None = None,
        workspace: str = ".",
    ) -> None:
        self.adapter = adapter or ProviderRouter.from_env()
        self.tools = tools or ToolRegistry()
        self.workspace = workspace
        self.started_at = time.monotonic()

    async def generate_thought(self, request: dict[str, Any]) -> dict[str, Any]:
        response = await self.adapter.generate(
            task_id=str(request.get("task_id", "")),
            system_prompt=str(request.get("system_prompt", "")),
            messages=list(request.get("messages", [])),
            tools=list(request.get("tools", [])),
            model=str(request.get("model", "")),
            temperature=float(request.get("temperature", 0.7) or 0.7),
        )
        return asdict(response)

    async def execute_tool(self, request: dict[str, Any]) -> ToolResult:
        fencing_error = validate_fencing_token(request)
        if fencing_error:
            return ToolResult(stderr=fencing_error, exit_code=126, is_error=True)
        workspace = request.get("workspace") or self.workspace
        return self.tools.execute(
            str(request.get("tool_name", "")),
            request.get("arguments", "{}"),
            workspace,
        )

    async def health_check(self) -> dict[str, Any]:
        return {
            "status": "SERVING",
            "worker_count": 1,
            "uptime_seconds": int(time.monotonic() - self.started_at),
        }


def validate_fencing_token(request: dict[str, Any], get_token: Callable[[str], int | None] | None = None) -> str:
    session_id = str(request.get("session_id", ""))
    fencing_token = int(request.get("fencing_token", 0) or 0)
    if not session_id or fencing_token <= 0:
        return ""
    if get_token is None:
        redis_url = os.getenv("TENET_REDIS_URL", "")
        if not redis_url:
            return ""
        get_token = _redis_fencing_getter(redis_url)
    current = get_token(session_id)
    if current is None:
        return f"fencing token missing for session {session_id}"
    if int(current) != fencing_token:
        return f"fencing token mismatch for session {session_id}: current={current} request={fencing_token}"
    return ""


def _redis_fencing_getter(redis_url: str) -> Callable[[str], int | None]:
    try:
        import redis  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError("TENET_REDIS_URL requires python package redis") from exc
    client = redis.Redis.from_url(redis_url, decode_responses=True)

    def get_token(session_id: str) -> int | None:
        value = client.get(f"session_fencing:{session_id}")
        if value is None:
            return None
        return int(value)

    return get_token
