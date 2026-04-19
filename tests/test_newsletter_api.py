from __future__ import annotations

import asyncio
import importlib
import os
import unittest
from unittest.mock import ANY, patch

from fastapi.testclient import TestClient


class NewsletterApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_db = f"file:newsletter_api_{id(self)}?mode=memory&cache=shared"
        self.prev_db = os.environ.get("COOCLE_DB")
        self.prev_use_astra = os.environ.get("USE_ASTRA")
        self.prev_start_crawler = os.environ.get("COOCLE_START_CRAWLER")
        self.prev_prewarm_astra = os.environ.get("COOCLE_PREWARM_ASTRA")
        self.prev_reset_data_on_start = os.environ.get("COOCLE_RESET_DATA_ON_START")
        self.prev_api_rate_limit = os.environ.get("COOCLE_API_RATE_LIMIT")
        self.prev_api_rate_window = os.environ.get("COOCLE_API_RATE_WINDOW_S")
        self.prev_newsletter_admin_token = os.environ.get("COOCLE_NEWSLETTER_ADMIN_TOKEN")
        self.prev_mailtrap_api_token = os.environ.get("MAILTRAP_API_TOKEN")
        self.prev_mailtrap_sending_email = os.environ.get("MAILTRAP_SENDING_EMAIL")
        self.prev_mailtrap_sending_name = os.environ.get("MAILTRAP_SENDING_NAME")
        self.prev_mailtrap_batch_url = os.environ.get("MAILTRAP_NEWSLETTER_BATCH_URL")
        self.prev_astra_token = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
        self.prev_astra_endpoint = os.environ.get("ASTRA_DB_API_ENDPOINT")

        os.environ["COOCLE_DB"] = str(self.test_db)
        os.environ["USE_ASTRA"] = "false"
        os.environ["COOCLE_START_CRAWLER"] = "0"
        os.environ["COOCLE_PREWARM_ASTRA"] = "0"
        os.environ["COOCLE_RESET_DATA_ON_START"] = "0"
        os.environ["COOCLE_API_RATE_LIMIT"] = "100"
        os.environ["COOCLE_API_RATE_WINDOW_S"] = "60"
        os.environ["COOCLE_NEWSLETTER_ADMIN_TOKEN"] = ""
        os.environ["MAILTRAP_API_TOKEN"] = ""
        os.environ["MAILTRAP_SENDING_EMAIL"] = ""
        os.environ["MAILTRAP_SENDING_NAME"] = "Coocle"
        os.environ["MAILTRAP_NEWSLETTER_BATCH_URL"] = ""
        os.environ["ASTRA_DB_APPLICATION_TOKEN"] = ""
        os.environ["ASTRA_DB_API_ENDPOINT"] = ""

        from backend import main as imported_main

        self.main_module = importlib.reload(imported_main)
        self.client_cm = TestClient(self.main_module.app)
        self.client = self.client_cm.__enter__()
        self.conn = self.main_module.app.state.conn

    def tearDown(self) -> None:
        self.client_cm.__exit__(None, None, None)

        env_resets = {
            "COOCLE_DB": self.prev_db,
            "USE_ASTRA": self.prev_use_astra,
            "COOCLE_START_CRAWLER": self.prev_start_crawler,
            "COOCLE_PREWARM_ASTRA": self.prev_prewarm_astra,
            "COOCLE_RESET_DATA_ON_START": self.prev_reset_data_on_start,
            "COOCLE_API_RATE_LIMIT": self.prev_api_rate_limit,
            "COOCLE_API_RATE_WINDOW_S": self.prev_api_rate_window,
            "COOCLE_NEWSLETTER_ADMIN_TOKEN": self.prev_newsletter_admin_token,
            "MAILTRAP_API_TOKEN": self.prev_mailtrap_api_token,
            "MAILTRAP_SENDING_EMAIL": self.prev_mailtrap_sending_email,
            "MAILTRAP_SENDING_NAME": self.prev_mailtrap_sending_name,
            "MAILTRAP_NEWSLETTER_BATCH_URL": self.prev_mailtrap_batch_url,
            "ASTRA_DB_APPLICATION_TOKEN": self.prev_astra_token,
            "ASTRA_DB_API_ENDPOINT": self.prev_astra_endpoint,
        }
        for key, previous in env_resets.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous

    def test_subscribe_creates_newsletter_subscriber(self) -> None:
        response = self.client.post(
            "/api/newsletter/subscribe",
            json={"email": " Reader@example.com ", "name": "  Ada Lovelace  "},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["created"])
        self.assertEqual(payload["email"], "reader@example.com")
        self.assertEqual(payload["subscriber_count"], 1)
        self.assertIn("eingetragen", payload["message"])

        row = self.conn.execute(
            "SELECT email, name, source_ip FROM newsletter_subscribers WHERE email = ?",
            ("reader@example.com",),
        ).fetchone()
        self.assertEqual(row["email"], "reader@example.com")
        self.assertEqual(row["name"], "Ada Lovelace")
        self.assertEqual(row["source_ip"], "testclient")

    def test_subscribe_mirrors_subscriber_to_astra_metadata_when_configured(self) -> None:
        fake_meta_collection = object()

        with patch.object(self.main_module.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            self.main_module.astra_utils,
            "get_astra_meta_collection",
            return_value=fake_meta_collection,
        ), patch.object(
            self.main_module.astra_utils,
            "upsert_newsletter_subscriber_document",
        ) as mirror_subscriber:
            response = self.client.post(
                "/api/newsletter/subscribe",
                json={"email": "mirror@example.com", "name": "Mirror"},
            )

        self.assertEqual(response.status_code, 200)
        mirror_subscriber.assert_called_once_with(
            fake_meta_collection,
            email="mirror@example.com",
            name="Mirror",
            source_ip="testclient",
            subscribed_at=ANY,
        )

    def test_subscribe_is_idempotent_for_duplicate_email(self) -> None:
        self.client.post("/api/newsletter/subscribe", json={"email": "reader@example.com", "name": "Ada"})

        response = self.client.post(
            "/api/newsletter/subscribe",
            json={"email": "READER@example.com", "name": "Ada Updated"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["created"])
        self.assertEqual(payload["subscriber_count"], 1)
        self.assertIn("bereits", payload["message"])

        row = self.conn.execute(
            "SELECT name FROM newsletter_subscribers WHERE email = ?",
            ("reader@example.com",),
        ).fetchone()
        self.assertEqual(row["name"], "Ada Updated")

    def test_subscribe_rejects_invalid_email(self) -> None:
        response = self.client.post("/api/newsletter/subscribe", json={"email": "not-an-email"})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "Bitte eine gueltige E-Mail-Adresse angeben.")

    def test_newsletter_send_requires_admin_token(self) -> None:
        self.client.post("/api/newsletter/subscribe", json={"email": "reader@example.com"})

        with patch.dict(
            os.environ,
            {"COOCLE_NEWSLETTER_ADMIN_TOKEN": "secret-token"},
            clear=False,
        ):
            response = self.client.post(
                "/api/newsletter/send",
                json={"subject": "Coocle Update", "text": "Hallo"},
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Newsletter-Admin-Token fehlt oder ist ungueltig.")

    def test_newsletter_send_requires_mailtrap_configuration(self) -> None:
        self.client.post("/api/newsletter/subscribe", json={"email": "reader@example.com"})

        with patch.dict(
            os.environ,
            {"COOCLE_NEWSLETTER_ADMIN_TOKEN": "secret-token"},
            clear=False,
        ):
            response = self.client.post(
                "/api/newsletter/send",
                headers={"X-Admin-Token": "secret-token"},
                json={"subject": "Coocle Update", "text": "Hallo"},
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "Mailtrap fuer Newsletter ist nicht konfiguriert.")

    def test_restore_newsletter_subscribers_from_astra_when_sqlite_is_empty(self) -> None:
        fake_meta_collection = object()

        with patch.object(self.main_module.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            self.main_module.astra_utils,
            "get_astra_meta_collection",
            return_value=fake_meta_collection,
        ), patch.object(
            self.main_module.astra_utils,
            "load_newsletter_subscriber_documents",
            return_value=[
                {
                    "email": "restore@example.com",
                    "name": "Restore Me",
                    "source_ip": "127.0.0.1",
                    "subscribed_at": "2026-04-19T11:55:00",
                }
            ],
        ):
            asyncio.run(self.main_module._restore_newsletter_subscribers_on_start(self.conn))

        row = self.conn.execute(
            "SELECT email, name, source_ip FROM newsletter_subscribers WHERE email = ?",
            ("restore@example.com",),
        ).fetchone()
        self.assertEqual(row["email"], "restore@example.com")
        self.assertEqual(row["name"], "Restore Me")
        self.assertEqual(row["source_ip"], "127.0.0.1")

    def test_newsletter_send_uses_mailtrap_bulk_batch_api(self) -> None:
        self.client.post("/api/newsletter/subscribe", json={"email": "alpha@example.com"})
        self.client.post("/api/newsletter/subscribe", json={"email": "beta@example.com"})
        captured_calls: list[dict] = []

        class FakeResponse:
            status_code = 200
            text = ""

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {
                    "success": True,
                    "responses": [
                        {"success": True, "message_ids": ["msg-1"]},
                        {"success": True, "message_ids": ["msg-2"]},
                    ],
                }

        class FakeAsyncClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def post(self, url, *, headers=None, json=None, timeout=None):
                captured_calls.append(
                    {
                        "url": url,
                        "headers": headers,
                        "json": json,
                        "timeout": timeout,
                    }
                )
                return FakeResponse()

        with patch.dict(
            os.environ,
            {
                "COOCLE_NEWSLETTER_ADMIN_TOKEN": "secret-token",
                "MAILTRAP_API_TOKEN": "mailtrap-token",
                "MAILTRAP_SENDING_EMAIL": "newsletter@coocle.test",
                "MAILTRAP_SENDING_NAME": "Coocle News",
            },
            clear=False,
        ), patch.object(self.main_module.httpx, "AsyncClient", return_value=FakeAsyncClient()):
            response = self.client.post(
                "/api/newsletter/send",
                headers={"X-Admin-Token": "secret-token"},
                json={
                    "subject": "Coocle April Update",
                    "html": "<h1>Neue Features</h1><p>Queue-Resume und Mailtrap sind live.</p>",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["subscriber_count"], 2)
        self.assertEqual(payload["sent"], 2)
        self.assertEqual(payload["batches"], 1)

        self.assertEqual(len(captured_calls), 1)
        call = captured_calls[0]
        self.assertEqual(call["url"], "https://bulk.api.mailtrap.io/api/batch")
        self.assertEqual(call["headers"]["Authorization"], "Bearer mailtrap-token")
        self.assertEqual(call["headers"]["Api-Token"], "mailtrap-token")
        self.assertEqual(call["json"]["base"]["from"]["email"], "newsletter@coocle.test")
        self.assertEqual(call["json"]["base"]["from"]["name"], "Coocle News")
        self.assertEqual(call["json"]["base"]["subject"], "Coocle April Update")
        self.assertEqual(call["json"]["base"]["category"], "newsletter")
        self.assertIn("Neue Features", call["json"]["base"]["html"])
        self.assertIn("Neue Features", call["json"]["base"]["text"])
        self.assertEqual(
            call["json"]["requests"],
            [
                {"to": [{"email": "alpha@example.com"}]},
                {"to": [{"email": "beta@example.com"}]},
            ],
        )
