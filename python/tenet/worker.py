from __future__ import annotations

import time
from dataclasses import asdict
from typing import Any

from .adapters import EchoAdapter
from .tools import ToolRegistry, ToolResult


class StatelessWorker:
    def __init__(self, adapter: EchoAdapter | None = None, tools: ToolRegistry | None = None) -> None:
        self.adapter = adapter or EchoAdapter()
        self.tools = tools or ToolRegistry()
        self.started_at = time.monotonic()

    async def generate_thought(self, request: dict[str, Any]) -> dict[str, Any]:
        response = await self.adapter.generate(
            task_id=str(request.get("task_id", "")),
            system_prompt=str(request.get("system_prompt", "")),
            messages=list(request.get("messages", [])),
            tools=list(request.get("tools", [])),
        )
        return asdict(response)

    async def execute_tool(self, request: dict[str, Any]) -> ToolResult:
        workspace = request.get("workspace") or "."
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
