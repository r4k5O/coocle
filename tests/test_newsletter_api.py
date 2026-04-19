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
        self.prev_smtp_host = os.environ.get("SMTP_HOST")
        self.prev_smtp_port = os.environ.get("SMTP_PORT")
        self.prev_smtp_username = os.environ.get("SMTP_USERNAME")
        self.prev_smtp_password = os.environ.get("SMTP_PASSWORD")
        self.prev_smtp_use_tls = os.environ.get("SMTP_USE_TLS")
        self.prev_smtp_sender_email = os.environ.get("SMTP_SENDER_EMAIL")
        self.prev_smtp_sender_name = os.environ.get("SMTP_SENDER_NAME")
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
        os.environ["SMTP_HOST"] = ""
        os.environ["SMTP_PORT"] = "587"
        os.environ["SMTP_USERNAME"] = ""
        os.environ["SMTP_PASSWORD"] = ""
        os.environ["SMTP_USE_TLS"] = "true"
        os.environ["SMTP_SENDER_EMAIL"] = ""
        os.environ["SMTP_SENDER_NAME"] = "Coocle"
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
            "SMTP_HOST": self.prev_smtp_host,
            "SMTP_PORT": self.prev_smtp_port,
            "SMTP_USERNAME": self.prev_smtp_username,
            "SMTP_PASSWORD": self.prev_smtp_password,
            "SMTP_USE_TLS": self.prev_smtp_use_tls,
            "SMTP_SENDER_EMAIL": self.prev_smtp_sender_email,
            "SMTP_SENDER_NAME": self.prev_smtp_sender_name,
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

    def test_newsletter_send_requires_smtp_configuration(self) -> None:
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
        self.assertEqual(response.json()["detail"], "SMTP fuer Newsletter ist nicht konfiguriert.")

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

    def test_newsletter_send_uses_smtp(self) -> None:
        self.client.post("/api/newsletter/subscribe", json={"email": "alpha@example.com"})
        self.client.post("/api/newsletter/subscribe", json={"email": "beta@example.com"})
        sent_messages: list[dict] = []

        class FakeSMTP:
            def __init__(self, host, port, timeout=30):
                self.host = host
                self.port = port

            def ehlo(self):
                pass

            def starttls(self):
                pass

            def login(self, username, password):
                pass

            def sendmail(self, from_addr, to_addrs, msg_string):
                sent_messages.append({
                    "from": from_addr,
                    "to": to_addrs,
                    "msg": msg_string,
                })

            def quit(self):
                pass

        with patch.dict(
            os.environ,
            {
                "COOCLE_NEWSLETTER_ADMIN_TOKEN": "secret-token",
                "SMTP_HOST": "smtp.test.local",
                "SMTP_PORT": "587",
                "SMTP_USERNAME": "news@coocle.test",
                "SMTP_PASSWORD": "smtp-pass",
                "SMTP_USE_TLS": "true",
                "SMTP_SENDER_EMAIL": "news@coocle.test",
                "SMTP_SENDER_NAME": "Coocle News",
            },
            clear=False,
        ), patch("backend.direct_email.smtplib.SMTP", FakeSMTP):
            response = self.client.post(
                "/api/newsletter/send",
                headers={"X-Admin-Token": "secret-token"},
                json={
                    "subject": "Coocle April Update",
                    "html": "<h1>Neue Features</h1><p>SMTP ist live.</p>",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["subscriber_count"], 2)
        self.assertEqual(payload["sent"], 2)
        self.assertEqual(payload["batches"], 1)

        self.assertEqual(len(sent_messages), 2)
        recipients = [msg["to"][0] for msg in sent_messages]
        self.assertIn("alpha@example.com", recipients)
        self.assertIn("beta@example.com", recipients)
        for msg in sent_messages:
            self.assertEqual(msg["from"], "news@coocle.test")
            self.assertIn("Coocle April Update", msg["msg"])
            self.assertIn("text/html", msg["msg"])

    def test_newsletter_check_milestones_requires_smtp(self) -> None:
        response = self.client.post("/api/newsletter/check-milestones")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "SMTP fuer Newsletter ist nicht konfiguriert.")

    def test_newsletter_check_milestones_requires_admin_token(self) -> None:
        with patch.dict(
            os.environ,
            {
                "COOCLE_NEWSLETTER_ADMIN_TOKEN": "secret-token",
                "SMTP_HOST": "smtp.test.local",
                "SMTP_USERNAME": "test",
                "SMTP_PASSWORD": "test",
            },
            clear=False,
        ):
            response = self.client.post("/api/newsletter/check-milestones")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Newsletter-Admin-Token fehlt oder ist ungueltig.")

    def test_newsletter_check_milestones_sends_page_milestone(self) -> None:
        self.client.post("/api/newsletter/subscribe", json={"email": "reader@example.com"})
        sent_messages: list[dict] = []

        class FakeSMTP:
            def __init__(self, host, port, timeout=30):
                pass
            def ehlo(self):
                pass
            def starttls(self):
                pass
            def login(self, username, password):
                pass
            def sendmail(self, from_addr, to_addrs, msg_string):
                sent_messages.append({"from": from_addr, "to": to_addrs, "msg": msg_string})
            def quit(self):
                pass

        with patch.dict(
            os.environ,
            {
                "COOCLE_NEWSLETTER_ADMIN_TOKEN": "secret-token",
                "SMTP_HOST": "smtp.test.local",
                "SMTP_PORT": "587",
                "SMTP_USERNAME": "news@coocle.test",
                "SMTP_PASSWORD": "smtp-pass",
                "SMTP_USE_TLS": "true",
                "SMTP_SENDER_EMAIL": "news@coocle.test",
                "SMTP_SENDER_NAME": "Coocle News",
            },
            clear=False,
        ), patch("backend.direct_email.smtplib.SMTP", FakeSMTP), patch(
            "backend.main.build_stats_payload", return_value={"pages": 100}
        ):
            response = self.client.post(
                "/api/newsletter/check-milestones",
                headers={"X-Admin-Token": "secret-token"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["page_count"], 100)
        self.assertEqual(len(payload["sent_milestones"]), 1)
        self.assertEqual(payload["sent_milestones"][0]["kind"], "pages")
        self.assertEqual(payload["sent_milestones"][0]["value"], 100)
        self.assertEqual(payload["sent_milestones"][0]["sent"], 1)

    def test_newsletter_check_milestones_no_new_milestones(self) -> None:
        self.client.post("/api/newsletter/subscribe", json={"email": "reader@example.com"})

        with patch.dict(
            os.environ,
            {
                "COOCLE_NEWSLETTER_ADMIN_TOKEN": "secret-token",
                "SMTP_HOST": "smtp.test.local",
                "SMTP_PORT": "587",
                "SMTP_USERNAME": "news@coocle.test",
                "SMTP_PASSWORD": "smtp-pass",
                "SMTP_USE_TLS": "true",
                "SMTP_SENDER_EMAIL": "news@coocle.test",
                "SMTP_SENDER_NAME": "Coocle News",
            },
            clear=False,
        ), patch(
            "backend.main.build_stats_payload", return_value={"pages": 50}
        ), patch("backend.direct_email.smtplib.SMTP"):
            response = self.client.post(
                "/api/newsletter/check-milestones",
                headers={"X-Admin-Token": "secret-token"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(len(payload["sent_milestones"]), 0)
        self.assertIn("Keine neuen Meilensteine", payload["message"])
