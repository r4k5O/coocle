from __future__ import annotations

import importlib
import os
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from backend.summarize import SummaryResult


class SummaryApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_db = f"file:summary_api_{id(self)}?mode=memory&cache=shared"
        self.prev_db = os.environ.get("COOCLE_DB")
        self.prev_use_astra = os.environ.get("USE_ASTRA")
        self.prev_start_crawler = os.environ.get("COOCLE_START_CRAWLER")
        self.prev_prewarm_astra = os.environ.get("COOCLE_PREWARM_ASTRA")
        os.environ["COOCLE_DB"] = str(self.test_db)
        os.environ["USE_ASTRA"] = "false"
        os.environ["COOCLE_START_CRAWLER"] = "0"
        os.environ["COOCLE_PREWARM_ASTRA"] = "0"

        from backend import main as imported_main

        self.main_module = importlib.reload(imported_main)
        self.client_cm = TestClient(self.main_module.app)
        self.client = self.client_cm.__enter__()
        self.conn = self.main_module.app.state.conn
        self.conn.execute(
            """
            INSERT INTO pages (url, title, content, fetched_at, status_code, content_type, language)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "https://example.com/python",
                "Python Docs",
                "Python testing guide and reference documentation for search results.",
                "2026-04-16T18:00:00",
                200,
                "text/html",
                "en",
            ),
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.client_cm.__exit__(None, None, None)
        if self.prev_db is None:
            os.environ.pop("COOCLE_DB", None)
        else:
            os.environ["COOCLE_DB"] = self.prev_db
        if self.prev_use_astra is None:
            os.environ.pop("USE_ASTRA", None)
        else:
            os.environ["USE_ASTRA"] = self.prev_use_astra
        if self.prev_start_crawler is None:
            os.environ.pop("COOCLE_START_CRAWLER", None)
        else:
            os.environ["COOCLE_START_CRAWLER"] = self.prev_start_crawler
        if self.prev_prewarm_astra is None:
            os.environ.pop("COOCLE_PREWARM_ASTRA", None)
        else:
            os.environ["COOCLE_PREWARM_ASTRA"] = self.prev_prewarm_astra

    def _today(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def test_successful_summary_consumes_one_credit(self) -> None:
        with patch.object(
            self.main_module,
            "summarize_results",
            AsyncMock(return_value=SummaryResult(summary="Kurzfassung", status="ok")),
        ) as summarize_mock:
            response = self.client.get("/api/search", params={"q": "python", "summarize": "true"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["summary"], "Kurzfassung")
        self.assertEqual(payload["summary_status"], "ok")
        self.assertIsNone(payload["summary_message"])
        self.assertEqual(payload["summary_format"], "markdown")
        summarize_mock.assert_awaited_once()

        credits = self.client.get("/api/credits").json()
        self.assertEqual(credits["used"], 1)
        self.assertEqual(credits["remaining"], 9)

    def test_summary_error_does_not_consume_credit(self) -> None:
        with patch.object(
            self.main_module,
            "summarize_results",
            AsyncMock(return_value=SummaryResult(status="error", message="Host nicht erreichbar")),
        ):
            response = self.client.get("/api/search", params={"q": "python", "summarize": "true"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIsNone(payload["summary"])
        self.assertEqual(payload["summary_status"], "error")
        self.assertEqual(payload["summary_message"], "Host nicht erreichbar")
        self.assertIsNone(payload["summary_format"])

        credits = self.client.get("/api/credits").json()
        self.assertEqual(credits["used"], 0)
        self.assertEqual(credits["remaining"], 10)

    def test_credits_exhausted_returns_structured_status(self) -> None:
        self.conn.execute(
            """
            INSERT INTO summarization_usage (ip, day, count)
            VALUES (?, ?, ?)
            """,
            ("testclient", self._today(), 10),
        )
        self.conn.commit()

        with patch.object(self.main_module, "summarize_results", AsyncMock()) as summarize_mock:
            response = self.client.get("/api/search", params={"q": "python", "summarize": "true"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIsNone(payload["summary"])
        self.assertEqual(payload["summary_status"], "credits_exhausted")
        self.assertIn("10 freien Zusammenfassungen", payload["summary_message"])
        self.assertIsNone(payload["summary_format"])
        summarize_mock.assert_not_awaited()

    def test_custom_key_bypasses_free_credit_counter(self) -> None:
        with patch.object(
            self.main_module,
            "summarize_results",
            AsyncMock(return_value=SummaryResult(summary="Cloud summary", status="ok")),
        ) as summarize_mock:
            response = self.client.get(
                "/api/search",
                params={"q": "python", "summarize": "true"},
                headers={
                    "X-Ollama-Key": "ollama_test_key",
                    "X-Ollama-Host": "https://example.com/v1",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["summary"], "Cloud summary")
        self.assertEqual(payload["summary_status"], "ok")
        self.assertEqual(payload["summary_format"], "markdown")

        credits = self.client.get("/api/credits").json()
        self.assertEqual(credits["used"], 0)
        self.assertEqual(credits["remaining"], 10)

        cfg = summarize_mock.await_args.kwargs["cfg"]
        self.assertEqual(cfg.host, "https://example.com/v1")
        self.assertEqual(cfg.api_key, "ollama_test_key")
