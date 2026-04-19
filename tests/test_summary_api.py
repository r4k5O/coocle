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
        self.prev_reset_data_on_start = os.environ.get("COOCLE_RESET_DATA_ON_START")
        self.prev_astra_token = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
        self.prev_astra_endpoint = os.environ.get("ASTRA_DB_API_ENDPOINT")
        self.prev_api_rate_limit = os.environ.get("COOCLE_API_RATE_LIMIT")
        self.prev_api_rate_window = os.environ.get("COOCLE_API_RATE_WINDOW_S")
        self.prev_summary_rate_limit = os.environ.get("COOCLE_SUMMARY_RATE_LIMIT")
        self.prev_summary_rate_window = os.environ.get("COOCLE_SUMMARY_RATE_WINDOW_S")
        os.environ["COOCLE_DB"] = str(self.test_db)
        os.environ["USE_ASTRA"] = "false"
        os.environ["COOCLE_START_CRAWLER"] = "0"
        os.environ["COOCLE_PREWARM_ASTRA"] = "0"
        os.environ["COOCLE_RESET_DATA_ON_START"] = "0"
        os.environ["ASTRA_DB_APPLICATION_TOKEN"] = ""
        os.environ["ASTRA_DB_API_ENDPOINT"] = ""
        os.environ["COOCLE_API_RATE_LIMIT"] = "100"
        os.environ["COOCLE_API_RATE_WINDOW_S"] = "60"
        os.environ["COOCLE_SUMMARY_RATE_LIMIT"] = "10"
        os.environ["COOCLE_SUMMARY_RATE_WINDOW_S"] = "60"

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
        if self.prev_reset_data_on_start is None:
            os.environ.pop("COOCLE_RESET_DATA_ON_START", None)
        else:
            os.environ["COOCLE_RESET_DATA_ON_START"] = self.prev_reset_data_on_start
        if self.prev_astra_token is None:
            os.environ.pop("ASTRA_DB_APPLICATION_TOKEN", None)
        else:
            os.environ["ASTRA_DB_APPLICATION_TOKEN"] = self.prev_astra_token
        if self.prev_astra_endpoint is None:
            os.environ.pop("ASTRA_DB_API_ENDPOINT", None)
        else:
            os.environ["ASTRA_DB_API_ENDPOINT"] = self.prev_astra_endpoint
        if self.prev_api_rate_limit is None:
            os.environ.pop("COOCLE_API_RATE_LIMIT", None)
        else:
            os.environ["COOCLE_API_RATE_LIMIT"] = self.prev_api_rate_limit
        if self.prev_api_rate_window is None:
            os.environ.pop("COOCLE_API_RATE_WINDOW_S", None)
        else:
            os.environ["COOCLE_API_RATE_WINDOW_S"] = self.prev_api_rate_window
        if self.prev_summary_rate_limit is None:
            os.environ.pop("COOCLE_SUMMARY_RATE_LIMIT", None)
        else:
            os.environ["COOCLE_SUMMARY_RATE_LIMIT"] = self.prev_summary_rate_limit
        if self.prev_summary_rate_window is None:
            os.environ.pop("COOCLE_SUMMARY_RATE_WINDOW_S", None)
        else:
            os.environ["COOCLE_SUMMARY_RATE_WINDOW_S"] = self.prev_summary_rate_window

    def _today(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _search_params(**extra) -> dict[str, str]:
        params = {"q": "python", "mode": "fts"}
        params.update(extra)
        return params

    def test_successful_summary_consumes_one_credit(self) -> None:
        with patch.object(
            self.main_module,
            "summarize_results",
            AsyncMock(return_value=SummaryResult(summary="Kurzfassung", status="ok")),
        ) as summarize_mock:
            response = self.client.get("/api/search", params=self._search_params(summarize="true"))

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
            response = self.client.get("/api/search", params=self._search_params(summarize="true"))

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
            response = self.client.get("/api/search", params=self._search_params(summarize="true"))

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
                params=self._search_params(summarize="true"),
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

    def test_summary_receives_indexed_page_content_for_tool_context(self) -> None:
        with patch.object(
            self.main_module,
            "summarize_results",
            AsyncMock(return_value=SummaryResult(summary="Kurzfassung", status="ok")),
        ) as summarize_mock:
            response = self.client.get("/api/search", params=self._search_params(summarize="true"))

        self.assertEqual(response.status_code, 200)
        summary_inputs = summarize_mock.await_args.args[2]
        self.assertTrue(summary_inputs)
        self.assertIn("page_content", summary_inputs[0])
        self.assertIn("Python testing guide", summary_inputs[0]["page_content"])

    def test_summarize_without_results_returns_structured_unavailable_state(self) -> None:
        response = self.client.get("/api/search", params={"q": "rust", "mode": "fts", "summarize": "true"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["results"], [])
        self.assertIsNone(payload["summary"])
        self.assertEqual(payload["summary_status"], "unavailable")
        self.assertEqual(payload["summary_message"], "Keine Suchergebnisse zum Zusammenfassen.")
        self.assertIsNone(payload["summary_format"])

    def test_custom_ollama_host_requires_https_for_non_local_clients(self) -> None:
        response = self.client.get(
            "/api/search",
            params=self._search_params(summarize="true"),
            headers={
                "X-Ollama-Key": "ollama_test_key",
                "X-Ollama-Host": "http://example.com/v1",
                "X-Forwarded-For": "203.0.113.10",
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("HTTPS", response.json()["detail"])

    def test_summary_requests_are_rate_limited(self) -> None:
        self.main_module.app.state.rate_limiter._events.clear()
        os.environ["COOCLE_SUMMARY_RATE_LIMIT"] = "1"

        with patch.object(
            self.main_module,
            "summarize_results",
            AsyncMock(return_value=SummaryResult(summary="Kurzfassung", status="ok")),
        ):
            first = self.client.get("/api/search", params=self._search_params(summarize="true"))
            second = self.client.get("/api/search", params=self._search_params(summarize="true"))

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)
        self.assertIn("Retry-After", second.headers)

    def test_general_api_requests_are_rate_limited(self) -> None:
        self.main_module.app.state.rate_limiter._events.clear()
        os.environ["COOCLE_API_RATE_LIMIT"] = "1"

        first = self.client.get("/api/credits")
        second = self.client.get("/api/credits")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)
        self.assertIn("Retry-After", second.headers)
        self.assertIn("Zu viele API-Anfragen", second.json()["detail"])

    def test_api_responses_include_security_headers(self) -> None:
        response = self.client.get("/api/credits")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(response.headers["X-Frame-Options"], "DENY")
        self.assertEqual(response.headers["Referrer-Policy"], "same-origin")
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_healthz_returns_lightweight_service_status(self) -> None:
        response = self.client.get("/api/healthz")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["db_connected"])
        self.assertFalse(payload["crawler_running"])

    def test_local_custom_ollama_host_is_allowed_for_local_client(self) -> None:
        with patch.object(
            self.main_module,
            "summarize_results",
            AsyncMock(return_value=SummaryResult(summary="Lokale Summary", status="ok")),
        ) as summarize_mock:
            response = self.client.get(
                "/api/search",
                params=self._search_params(summarize="true"),
                headers={
                    "X-Ollama-Key": "ollama_test_key",
                    "X-Ollama-Host": "http://localhost:11434",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["summary"], "Lokale Summary")
        cfg = summarize_mock.await_args.kwargs["cfg"]
        self.assertEqual(cfg.host, "http://localhost:11434")

    def test_search_uses_hybrid_mode_by_default(self) -> None:
        with patch.object(
            self.main_module,
            "vec_search",
            AsyncMock(
                return_value=[
                    {
                        "title": "Vector Treffer",
                        "url": "https://example.com/vector",
                        "snippet": "Semantischer Treffer",
                        "score": 0.91,
                    }
                ]
            ),
        ) as vec_search_mock, patch.object(
            self.main_module,
            "fts_search",
            return_value=[
                {
                    "title": "FTS Treffer",
                    "url": "https://example.com/fts",
                    "snippet": "Lexikalischer Treffer",
                    "score": 0.77,
                }
            ],
        ) as fts_search_mock:
            response = self.client.get("/api/search", params={"q": "python"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            [item["url"] for item in payload["results"]],
            ["https://example.com/vector", "https://example.com/fts"],
        )
        vec_search_mock.assert_awaited_once()
        fts_search_mock.assert_called_once()

    def test_pages_overview_returns_index_queue_and_astra_status(self) -> None:
        self.conn.execute(
            """
            INSERT INTO crawl_queue (url, depth, discovered_at, last_error)
            VALUES (?, ?, ?, ?)
            """,
            ("https://example.com/queued", 2, "2026-04-17T10:00:00", None),
        )
        self.conn.commit()
        self.main_module.app.state.crawl_status = {
            "state": "fetching",
            "current_url": "https://example.com/live",
            "current_depth": 1,
            "message": "Seite wird gecrawlt",
            "updated_at": "2026-04-17T10:05:00",
        }

        with patch.object(self.main_module.astra_utils, "is_astra_enabled", return_value=True), patch.object(
            self.main_module.astra_utils,
            "has_astra_credentials",
            return_value=True,
        ), patch.object(
            self.main_module.astra_utils,
            "get_astra_collection",
            return_value=type("FakeCollection", (), {"full_name": "testspace.coocle_pages"})(),
        ):
            response = self.client.get("/api/pages/overview")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["summary"]["indexed_count"], 1)
        self.assertEqual(payload["summary"]["queued_count"], 1)
        self.assertEqual(payload["summary"]["active_scans"], 1)
        self.assertTrue(payload["astra"]["enabled"])
        self.assertTrue(payload["astra"]["connected"])
        self.assertEqual(payload["astra"]["collection"], "testspace.coocle_pages")
        self.assertEqual(payload["indexed_pages"][0]["url"], "https://example.com/python")
        self.assertEqual(payload["queued_pages"][0]["url"], "https://example.com/queued")
        self.assertEqual(payload["current_scans"][0]["url"], "https://example.com/live")

    def test_pages_overview_includes_pending_batch_entries(self) -> None:
        self.main_module.app.state.crawl_status = {
            "state": "saved",
            "current_url": "https://example.com/live-batch",
            "current_depth": 1,
            "message": "Seite indexiert",
            "pages_done": 2,
            "pages_saved": 2,
            "pending_indexed_count": 1,
            "pending_indexed_pages": [
                {
                    "url": "https://example.com/live-batch",
                    "title": "Live Batch",
                    "excerpt": "Noch nicht geflusht.",
                    "fetched_at": "2026-04-18T09:00:00",
                    "status_code": 200,
                    "content_type": "text/html",
                    "language": "de",
                    "storage_state": "pending_batch",
                }
            ],
            "updated_at": "2026-04-18T09:00:00",
        }

        response = self.client.get("/api/pages/overview")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["summary"]["indexed_count"], 2)
        self.assertEqual(payload["summary"]["pending_indexed_count"], 1)
        self.assertEqual(payload["indexed_pages"][0]["url"], "https://example.com/live-batch")
        self.assertEqual(payload["indexed_pages"][0]["storage_state"], "pending_batch")

    def test_pages_overview_tolerates_legacy_or_malformed_crawl_status(self) -> None:
        self.main_module.app.state.crawl_status = ["broken"]

        response = self.client.get("/api/pages/overview")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["summary"]["active_scans"], 0)
        self.assertEqual(payload["summary"]["pending_indexed_count"], 0)
        self.assertEqual(payload["current_scans"], [])
        self.assertEqual(payload["crawler_status"]["message"], "Crawlerstatus unbekannt")

    def test_stats_use_exact_astra_count_when_available(self) -> None:
        fake_collection = type("FakeCollection", (), {"full_name": "testspace.coocle_pages"})()

        with patch.object(self.main_module.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            self.main_module.astra_utils,
            "get_astra_collection",
            return_value=fake_collection,
        ), patch.object(self.main_module.astra_utils, "exact_document_count", return_value=5471), patch.object(
            self.main_module.astra_utils,
            "live_document_count",
            return_value=None,
        ), patch.object(
            self.main_module.astra_utils,
            "estimated_document_count",
            return_value=6000,
        ):
            stats_response = self.client.get("/api/stats")

        self.assertEqual(stats_response.status_code, 200)

        stats_payload = stats_response.json()

        self.assertEqual(stats_payload["sqlite_pages"], 1)
        self.assertEqual(stats_payload["astra_pages"], 5471)
        self.assertEqual(stats_payload["astra_pages_exact"], 5471)
        self.assertIsNone(stats_payload["astra_pages_estimate"])
        self.assertFalse(stats_payload["astra_pages_is_estimate"])
        self.assertEqual(stats_payload["astra_count_source"], "astra_exact")
        self.assertEqual(stats_payload["pages"], 5471)

    def test_stats_fall_back_to_astra_estimate_when_exact_count_unavailable(self) -> None:
        fake_collection = type("FakeCollection", (), {"full_name": "testspace.coocle_pages"})()

        with patch.object(self.main_module.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            self.main_module.astra_utils,
            "get_astra_collection",
            return_value=fake_collection,
        ), patch.object(self.main_module.astra_utils, "exact_document_count", return_value=None), patch.object(
            self.main_module.astra_utils,
            "live_document_count",
            return_value=None,
        ), patch.object(
            self.main_module.astra_utils,
            "estimated_document_count",
            return_value=5471,
        ):
            stats_response = self.client.get("/api/stats")

        self.assertEqual(stats_response.status_code, 200)

        stats_payload = stats_response.json()

        self.assertEqual(stats_payload["sqlite_pages"], 1)
        self.assertEqual(stats_payload["astra_pages"], 5471)
        self.assertIsNone(stats_payload["astra_pages_exact"])
        self.assertEqual(stats_payload["astra_pages_estimate"], 5471)
        self.assertTrue(stats_payload["astra_pages_is_estimate"])
        self.assertEqual(stats_payload["astra_count_source"], "astra_estimate")
        self.assertEqual(stats_payload["pages"], 5471)

    def test_pages_overview_returns_fast_sqlite_summary_without_live_count(self) -> None:
        fake_collection = type("FakeCollection", (), {"full_name": "testspace.coocle_pages"})()

        with patch.object(self.main_module.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            self.main_module.astra_utils,
            "get_astra_collection",
            return_value=fake_collection,
        ), patch.object(self.main_module.astra_utils, "exact_document_count", side_effect=AssertionError("overview should not do exact Astra counts")), patch.object(
            self.main_module.astra_utils,
            "live_document_count",
            side_effect=AssertionError("overview should not do live Astra scans"),
        ):
            overview_response = self.client.get("/api/pages/overview")

        self.assertEqual(overview_response.status_code, 200)

        overview_payload = overview_response.json()

        self.assertEqual(overview_payload["summary"]["sqlite_indexed_count"], 1)
        self.assertEqual(overview_payload["summary"]["indexed_count"], 1)
        self.assertFalse(overview_payload["summary"]["indexed_count_is_estimate"])
        self.assertEqual(overview_payload["summary"]["indexed_count_source"], "sqlite")
        self.assertIsNone(overview_payload["astra"]["document_count"])
        self.assertIsNone(overview_payload["astra"]["document_count_exact"])
        self.assertIsNone(overview_payload["astra"]["document_count_live"])
        self.assertIsNone(overview_payload["astra"]["document_count_estimate"])
        self.assertFalse(overview_payload["astra"]["count_is_estimate"])
        self.assertEqual(overview_payload["astra"]["count_source"], "deferred")
        self.assertEqual(overview_payload["astra"]["count_message"], "Livezaehler wird separat geladen.")

    def test_pages_live_count_uses_live_scan_when_exact_count_unavailable(self) -> None:
        fake_collection = type("FakeCollection", (), {"full_name": "testspace.coocle_pages"})()

        with patch.object(self.main_module.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            self.main_module.astra_utils,
            "get_astra_collection",
            return_value=fake_collection,
        ), patch.object(self.main_module.astra_utils, "exact_document_count", return_value=None), patch.object(
            self.main_module.astra_utils,
            "live_document_count",
            return_value=5471,
        ), patch.object(
            self.main_module.astra_utils,
            "estimated_document_count",
            return_value=6000,
        ):
            overview_response = self.client.get("/api/pages/live-count")

        self.assertEqual(overview_response.status_code, 200)

        overview_payload = overview_response.json()

        self.assertEqual(overview_payload["summary"]["sqlite_indexed_count"], 1)
        self.assertEqual(overview_payload["summary"]["indexed_count"], 5471)
        self.assertFalse(overview_payload["summary"]["indexed_count_is_estimate"])
        self.assertEqual(overview_payload["summary"]["indexed_count_source"], "astra_live_scan")
        self.assertEqual(overview_payload["astra"]["document_count"], 5471)
        self.assertIsNone(overview_payload["astra"]["document_count_exact"])
        self.assertEqual(overview_payload["astra"]["document_count_live"], 5471)
        self.assertIsNone(overview_payload["astra"]["document_count_estimate"])
        self.assertFalse(overview_payload["astra"]["count_is_estimate"])
        self.assertEqual(overview_payload["astra"]["count_source"], "astra_live_scan")

    def test_pages_live_count_does_not_use_estimate_when_live_counter_is_unavailable(self) -> None:
        fake_collection = type("FakeCollection", (), {"full_name": "testspace.coocle_pages"})()

        with patch.object(self.main_module.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            self.main_module.astra_utils,
            "get_astra_collection",
            return_value=fake_collection,
        ), patch.object(self.main_module.astra_utils, "exact_document_count", return_value=None), patch.object(
            self.main_module.astra_utils,
            "live_document_count",
            return_value=None,
        ), patch.object(
            self.main_module.astra_utils,
            "estimated_document_count",
            return_value=5471,
        ):
            overview_response = self.client.get("/api/pages/live-count")

        self.assertEqual(overview_response.status_code, 200)

        overview_payload = overview_response.json()

        self.assertEqual(overview_payload["summary"]["sqlite_indexed_count"], 1)
        self.assertEqual(overview_payload["summary"]["indexed_count"], 1)
        self.assertFalse(overview_payload["summary"]["indexed_count_is_estimate"])
        self.assertEqual(overview_payload["summary"]["indexed_count_source"], "sqlite")
        self.assertIsNone(overview_payload["astra"]["document_count"])
        self.assertIsNone(overview_payload["astra"]["document_count_exact"])
        self.assertIsNone(overview_payload["astra"]["document_count_live"])
        self.assertIsNone(overview_payload["astra"]["document_count_estimate"])
        self.assertFalse(overview_payload["astra"]["count_is_estimate"])
        self.assertEqual(overview_payload["astra"]["count_source"], "unavailable")
        self.assertEqual(
            overview_payload["astra"]["count_message"],
            "Astra ist verbunden, aber der Livezaehler liefert aktuell keinen Wert.",
        )

    def test_favicon_route_returns_logo_file(self) -> None:
        response = self.client.get("/favicon.ico")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "image/png")
