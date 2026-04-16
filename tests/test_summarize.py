from __future__ import annotations

import json
import unittest

import httpx

from backend.summarize import OllamaChatConfig, summarize_results


class SummarizeResultsTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_ollama_host_uses_api_chat_and_parses_message_content(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(str(request.url), "http://localhost:11434/api/chat")
            body = json.loads(request.content.decode("utf-8"))
            self.assertIn("options", body)
            self.assertEqual(body["model"], "deepseek-r1:1.5b")
            return httpx.Response(200, json={"message": {"content": "Summary: Local summary"}})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await summarize_results(
                client,
                "python",
                [{"title": "Python Docs", "snippet": "Reference documentation"}],
                cfg=OllamaChatConfig(host="http://localhost:11434"),
            )

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.summary, "Local summary")

    async def test_v1_host_uses_chat_completions_and_parses_choices(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(str(request.url), "https://example.com/v1/chat/completions")
            body = json.loads(request.content.decode("utf-8"))
            self.assertIn("max_tokens", body)
            self.assertNotIn("options", body)
            self.assertEqual(body["model"], "deepseek-r1:1.5b")
            return httpx.Response(
                200,
                json={"choices": [{"message": {"content": "Cloud summary"}}]},
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await summarize_results(
                client,
                "python",
                [{"title": "Python Docs", "snippet": "Reference documentation"}],
                cfg=OllamaChatConfig(host="https://example.com/v1"),
            )

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.summary, "Cloud summary")

    async def test_openai_style_content_array_is_collapsed_into_summary(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "message": {
                                "content": [
                                    {"type": "text", "text": "- Punkt 1"},
                                    {"type": "text", "text": "- Punkt 2"},
                                ]
                            }
                        }
                    ]
                },
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await summarize_results(
                client,
                "python",
                [{"title": "Python Docs", "snippet": "Reference documentation"}],
                cfg=OllamaChatConfig(host="https://example.com/v1"),
            )

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.summary, "- Punkt 1\n- Punkt 2")
