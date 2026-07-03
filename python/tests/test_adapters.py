from __future__ import annotations

import asyncio
import os
import unittest

from tenet.adapters import DeepSeekAdapter, EchoAdapter, OpenAICompatibleAdapter, ProviderRouter


class AdapterTests(unittest.TestCase):
    def test_echo_adapter(self) -> None:
        response = asyncio.run(EchoAdapter().generate(
            task_id="task:1",
            system_prompt="system",
            messages=[{"role": "user", "content": "hello"}],
            tools=[],
        ))
        self.assertIn("hello", response.thought)
        self.assertTrue(response.is_final)

    def test_openai_compatible_parses_tool_calls_and_usage(self) -> None:
        captured = {}

        def transport(url, headers, payload):
            captured["url"] = url
            captured["headers"] = headers
            captured["payload"] = payload
            return {
                "choices": [{
                    "finish_reason": "tool_calls",
                    "message": {
                        "content": "",
                        "tool_calls": [{
                            "id": "call-1",
                            "function": {"name": "read_file", "arguments": "{\"path\":\"README.md\"}"},
                        }],
                    },
                }],
                "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
            }

        adapter = OpenAICompatibleAdapter(base_url="https://example.test/v1", api_key="key", default_model="model-a", transport=transport)
        response = asyncio.run(adapter.generate(
            task_id="task:1",
            system_prompt="system",
            messages=[{"role": "user", "content": "hello"}],
            tools=[{"name": "read_file", "description": "read", "parameters_schema": "{\"type\":\"object\"}"}],
        ))

        self.assertEqual(captured["url"], "https://example.test/v1/chat/completions")
        self.assertEqual(captured["payload"]["model"], "model-a")
        self.assertEqual(captured["payload"]["messages"][0]["role"], "system")
        self.assertEqual(captured["payload"]["tools"][0]["function"]["name"], "read_file")
        self.assertFalse(response.is_final)
        self.assertEqual(response.tool_calls[0]["tool_name"], "read_file")
        self.assertEqual(response.usage.total_tokens, 12)

    def test_deepseek_defaults(self) -> None:
        adapter = DeepSeekAdapter(api_key="key")
        self.assertEqual(adapter.base_url, "https://api.deepseek.com")
        self.assertEqual(adapter.default_model, "deepseek-v4-flash")

    def test_provider_router_from_env(self) -> None:
        old = dict(os.environ)
        try:
            os.environ["TENET_LLM_PROVIDER"] = "echo"
            router = ProviderRouter.from_env()
            response = asyncio.run(router.generate(task_id="task:1", system_prompt="", messages=[{"content": "hello"}], tools=[]))
            self.assertIn("hello", response.thought)
        finally:
            os.environ.clear()
            os.environ.update(old)


if __name__ == "__main__":
    unittest.main()
