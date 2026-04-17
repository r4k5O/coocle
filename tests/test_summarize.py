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

    async def test_stored_page_content_is_added_as_webpage_reader_context(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            body = json.loads(request.content.decode("utf-8"))
            prompt = body["messages"][0]["content"]
            self.assertIn("Tool webpage_reader (stored_page_excerpt) output:", prompt)
            self.assertIn("Deep Python details from the indexed page", prompt)
            self.assertIn("Search snippet: Short snippet", prompt)
            return httpx.Response(200, json={"choices": [{"message": {"content": "Stored page summary"}}]})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await summarize_results(
                client,
                "python",
                [
                    {
                        "title": "Python Docs",
                        "url": "https://example.com/python",
                        "snippet": "Short snippet",
                        "page_content": "Deep Python details from the indexed page and test-friendly context.",
                    }
                ],
                cfg=OllamaChatConfig(host="https://example.com/v1"),
            )

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.summary, "Stored page summary")

    async def test_missing_page_content_triggers_live_webpage_reader(self) -> None:
        requests_seen: list[tuple[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests_seen.append((request.method, str(request.url)))
            if request.method == "GET":
                self.assertEqual(str(request.url), "https://docs.example.com/python")
                return httpx.Response(
                    200,
                    headers={"content-type": "text/html; charset=utf-8"},
                    text="""
                    <html>
                      <body>
                        <main>
                          <h1>Python Testing</h1>
                          <p>Read the original webpage for detailed fixtures and assertions.</p>
                        </main>
                      </body>
                    </html>
                    """,
                )

            body = json.loads(request.content.decode("utf-8"))
            prompt = body["messages"][0]["content"]
            self.assertIn("Tool webpage_reader (live_webpage_reader) output:", prompt)
            self.assertIn("Python Testing Read the original webpage", prompt)
            return httpx.Response(200, json={"choices": [{"message": {"content": "Live page summary"}}]})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await summarize_results(
                client,
                "python",
                [
                    {
                        "title": "Python Docs",
                        "url": "https://docs.example.com/python",
                        "snippet": "Reference documentation",
                    }
                ],
                cfg=OllamaChatConfig(host="https://example.com/v1"),
            )

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.summary, "Live page summary")
        self.assertEqual(
            requests_seen,
            [
                ("GET", "https://docs.example.com/python"),
                ("POST", "https://example.com/v1/chat/completions"),
            ],
        )
