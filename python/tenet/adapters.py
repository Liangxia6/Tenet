from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol


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


class LLMAdapter(Protocol):
    async def generate(
        self,
        *,
        task_id: str,
        system_prompt: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        model: str = "",
        temperature: float = 0.7,
    ) -> AdapterResponse:
        ...


class EchoAdapter:
    """Deterministic adapter for local development and tests."""

    async def generate(
        self,
        *,
        task_id: str,
        system_prompt: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        model: str = "",
        temperature: float = 0.7,
    ) -> AdapterResponse:
        del tools, model, temperature
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


Transport = Callable[[str, dict[str, str], dict[str, Any]], dict[str, Any]]


class OpenAICompatibleAdapter:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        default_model: str,
        transport: Transport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.default_model = default_model
        self.transport = transport or _http_json_post

    async def generate(
        self,
        *,
        task_id: str,
        system_prompt: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        model: str = "",
        temperature: float = 0.7,
    ) -> AdapterResponse:
        del task_id
        payload = {
            "model": model or self.default_model,
            "temperature": temperature,
            "messages": _to_openai_messages(system_prompt, messages),
        }
        if tools:
            payload["tools"] = [_to_openai_tool(tool) for tool in tools]
            payload["tool_choice"] = "auto"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        data = await asyncio.to_thread(self.transport, self.base_url + "/chat/completions", headers, payload)
        return _parse_openai_response(data)


class DeepSeekAdapter(OpenAICompatibleAdapter):
    def __init__(self, *, api_key: str, base_url: str = "https://api.deepseek.com", default_model: str = "deepseek-v4-flash", transport: Transport | None = None) -> None:
        super().__init__(base_url=base_url, api_key=api_key, default_model=default_model, transport=transport)


class ProviderRouter:
    """根据环境变量选择 LLM provider。

    默认 echo 方便无 key 测试；OpenAI/DeepSeek 都走 OpenAI-compatible chat completions 协议。
    Go CLI 的 --worker openai/deepseek 和 Python Worker 的 provider 设计保持一致。
    """

    def __init__(self, adapter: LLMAdapter) -> None:
        self.adapter = adapter

    @classmethod
    def from_env(cls) -> "ProviderRouter":
        provider = os.getenv("TENET_LLM_PROVIDER", "echo").strip().lower()
        if provider == "openai":
            key = os.getenv(os.getenv("TENET_OPENAI_API_KEY_ENV", "OPENAI_API_KEY"), "")
            return cls(OpenAICompatibleAdapter(
                base_url=os.getenv("TENET_OPENAI_BASE_URL", "https://api.openai.com/v1"),
                api_key=key,
                default_model=os.getenv("TENET_OPENAI_MODEL", "gpt-4.1-mini"),
            ))
        if provider == "deepseek":
            key = os.getenv(os.getenv("TENET_DEEPSEEK_API_KEY_ENV", "DEEPSEEK_API_KEY"), "")
            return cls(DeepSeekAdapter(
                api_key=key,
                base_url=os.getenv("TENET_DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
                default_model=os.getenv("TENET_DEEPSEEK_MODEL", "deepseek-v4-flash"),
            ))
        return cls(EchoAdapter())

    async def generate(self, **kwargs: Any) -> AdapterResponse:
        return await self.adapter.generate(**kwargs)


def _to_openai_messages(system_prompt: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if system_prompt:
        out.append({"role": "system", "content": system_prompt})
    for message in messages:
        item: dict[str, Any] = {
            "role": str(message.get("role", "user")),
            "content": str(message.get("content", "")),
        }
        if message.get("tool_call_id"):
            item["tool_call_id"] = message["tool_call_id"]
        if message.get("tool_calls"):
            item["tool_calls"] = message["tool_calls"]
        out.append(item)
    return out


def _to_openai_tool(tool: dict[str, Any]) -> dict[str, Any]:
    schema = tool.get("parameters_schema") or tool.get("parameters") or {"type": "object"}
    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except json.JSONDecodeError:
            schema = {"type": "object"}
    return {
        "type": "function",
        "function": {
            "name": str(tool.get("name", "")),
            "description": str(tool.get("description", "")),
            "parameters": schema,
        },
    }


def _parse_openai_response(data: dict[str, Any]) -> AdapterResponse:
    choices = data.get("choices") or []
    choice = choices[0] if choices else {}
    message = choice.get("message") or {}
    tool_calls = []
    for call in message.get("tool_calls") or []:
        function = call.get("function") or {}
        tool_calls.append({
            "call_id": str(call.get("id", "")),
            "tool_name": str(function.get("name", "")),
            "arguments": function.get("arguments", "{}"),
        })
    usage = data.get("usage") or {}
    finish_reason = str(choice.get("finish_reason") or "stop")
    return AdapterResponse(
        thought=str(message.get("content") or ""),
        tool_calls=tool_calls,
        is_final=finish_reason != "tool_calls",
        finish_reason=finish_reason,
        usage=TokenUsage(
            prompt_tokens=int(usage.get("prompt_tokens") or 0),
            completion_tokens=int(usage.get("completion_tokens") or 0),
            total_tokens=int(usage.get("total_tokens") or 0),
        ),
    )


def _http_json_post(url: str, headers: dict[str, str], payload: dict[str, Any]) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310 - caller supplies trusted provider URL.
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"LLM provider HTTP {exc.code}: {body}") from exc
