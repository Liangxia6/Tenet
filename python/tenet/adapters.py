from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class TokenUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0


@dataclass(slots=True)
class AdapterResponse:
    thought: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    is_final: bool = True
    finish_reason: str = "stop"
    usage: TokenUsage = field(default_factory=TokenUsage)
    discovered_tools: list[dict[str, Any]] = field(default_factory=list)


class EchoAdapter:
    """Deterministic adapter for local development and tests."""

    async def generate(
        self,
        *,
        task_id: str,
        system_prompt: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> AdapterResponse:
        del tools
        content = ""
        if messages:
            content = str(messages[-1].get("content", ""))
        if not content:
            content = system_prompt
        thought = f"Echo response for task {task_id}: {content.strip()}"
        return AdapterResponse(
            thought=thought,
            usage=TokenUsage(
                prompt_tokens=max(1, len(system_prompt) // 4),
                completion_tokens=max(1, len(thought) // 4),
                total_tokens=max(2, (len(system_prompt) + len(thought)) // 4),
            ),
        )
