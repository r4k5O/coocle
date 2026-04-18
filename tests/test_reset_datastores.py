from __future__ import annotations

import os
import unittest
from importlib import reload
from types import SimpleNamespace
from unittest.mock import patch

from backend import astra_utils, db


class ResetRuntimeDataTests(unittest.TestCase):
    def test_reset_runtime_data_clears_pages_queue_and_usage(self) -> None:
        conn = db.connect(f"file:reset_runtime_{id(self)}?mode=memory&cache=shared")
        db.init_db(conn)
        conn.execute(
            """
            INSERT INTO pages (url, title, content, fetched_at, status_code, content_type, language)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "https://example.com/reset",
                "Reset Test",
                "Inhalt",
                "2026-04-18T13:00:00",
                200,
                "text/html",
                "de",
            ),
        )
        conn.execute(
            "INSERT INTO crawl_queue (url, depth, discovered_at) VALUES (?, ?, ?)",
            ("https://example.com/next", 1, "2026-04-18T13:01:00"),
        )
        conn.execute(
            "INSERT INTO summarization_usage (ip, day, count) VALUES (?, ?, ?)",
            ("127.0.0.1", "2026-04-18", 3),
        )
        conn.commit()

        cleared = db.reset_runtime_data(conn)

        self.assertEqual(cleared, {"pages": 1, "crawl_queue": 1, "summarization_usage": 1})
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM crawl_queue").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM summarization_usage").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM pages_fts").fetchone()[0], 0)


class AstraResetTests(unittest.TestCase):
    def test_has_astra_credentials_requires_token_and_endpoint(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ASTRA_DB_APPLICATION_TOKEN": "AstraCS:token",
                "ASTRA_DB_API_ENDPOINT": "https://example-astra.apps.astra.datastax.com",
            },
            clear=False,
        ):
            self.assertTrue(astra_utils.has_astra_credentials())

        with patch.dict(
            os.environ,
            {
                "ASTRA_DB_APPLICATION_TOKEN": "",
                "ASTRA_DB_API_ENDPOINT": "https://example-astra.apps.astra.datastax.com",
            },
            clear=False,
        ):
            self.assertFalse(astra_utils.has_astra_credentials())

    def test_clear_documents_uses_atomic_empty_filter(self) -> None:
        class FakeCollection:
            def __init__(self) -> None:
                self.calls: list[tuple[dict, int]] = []

            def delete_many(self, filter, *, general_method_timeout_ms):
                self.calls.append((filter, general_method_timeout_ms))
                return SimpleNamespace(deleted_count=7)

        collection = FakeCollection()

        deleted = astra_utils.clear_documents(collection, general_method_timeout_ms=3210)

        self.assertEqual(deleted, 7)
        self.assertEqual(collection.calls, [({}, 3210)])

    def test_clear_documents_ignores_missing_collection(self) -> None:
        self.assertEqual(astra_utils.clear_documents(None), 0)

    def test_estimated_document_count_returns_none_for_missing_collection(self) -> None:
        self.assertIsNone(astra_utils.estimated_document_count(None))

    def test_reset_marker_roundtrip_uses_fixed_document_id(self) -> None:
        class FakeCollection:
            def __init__(self) -> None:
                self.filter = None
                self.replacement = None
                self.upsert = None

            def find_one_and_replace(self, *, filter, replacement, upsert):
                self.filter = filter
                self.replacement = replacement
                self.upsert = upsert

        collection = FakeCollection()

        astra_utils.set_reset_marker(collection, "render:abc123")

        self.assertEqual(collection.filter, {"_id": astra_utils.ASTRA_RESET_MARKER_ID})
        self.assertEqual(collection.replacement["deploy_key"], "render:abc123")
        self.assertTrue(collection.upsert)


class StartupResetOrchestrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_reset_clears_sqlite_and_astra_when_credentials_exist(self) -> None:
        from backend import main as imported_main

        main = reload(imported_main)
        fake_collection = SimpleNamespace(full_name="testspace.coocle_pages")
        fake_meta_collection = object()

        with patch.dict(
            os.environ,
            {"COOCLE_RESET_DATA_ON_START": "1", "RENDER_GIT_COMMIT": "abc123"},
            clear=False,
        ), patch.object(
            main.dbmod,
            "reset_runtime_data",
            return_value={"pages": 2, "crawl_queue": 1, "summarization_usage": 4},
        ) as reset_db, patch.object(main.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            main.astra_utils,
            "get_astra_meta_collection",
            return_value=fake_meta_collection,
        ), patch.object(
            main.astra_utils,
            "get_reset_marker",
            return_value=None,
        ), patch.object(
            main.astra_utils,
            "get_astra_collection",
            return_value=fake_collection,
        ) as get_collection, patch.object(main.astra_utils, "clear_documents", return_value=9) as clear_documents:
            with patch.object(main.astra_utils, "set_reset_marker") as set_reset_marker, patch.object(
                main.logger, "warning"
            ) as log_warning:
                await main._reset_datastores_on_start(object())

        reset_db.assert_called_once()
        get_collection.assert_called_once_with()
        clear_documents.assert_called_once_with(fake_collection)
        set_reset_marker.assert_called_once_with(fake_meta_collection, "render:abc123")
        self.assertGreaterEqual(log_warning.call_count, 2)

    async def test_startup_reset_tolerates_astra_failure_when_not_strict(self) -> None:
        from backend import main as imported_main

        main = reload(imported_main)
        fake_meta_collection = object()

        with patch.dict(
            os.environ,
            {
                "COOCLE_RESET_DATA_ON_START": "1",
                "COOCLE_RESET_DATA_STRICT": "0",
                "RENDER_GIT_COMMIT": "abc123",
            },
            clear=False,
        ), patch.object(
            main.dbmod,
            "reset_runtime_data",
            return_value={"pages": 2, "crawl_queue": 1, "summarization_usage": 4},
        ) as reset_db, patch.object(main.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            main.astra_utils,
            "get_astra_meta_collection",
            return_value=fake_meta_collection,
        ), patch.object(
            main.astra_utils,
            "get_reset_marker",
            return_value=None,
        ), patch.object(
            main.astra_utils,
            "get_astra_collection",
            side_effect=RuntimeError("astra offline"),
        ) as get_collection:
            with patch.object(main.logger, "warning"), patch.object(main.logger, "exception") as log_exception:
                await main._reset_datastores_on_start(object())

        reset_db.assert_called_once()
        get_collection.assert_called_once_with()
        log_exception.assert_called_once()

    async def test_startup_reset_raises_astra_failure_when_strict(self) -> None:
        from backend import main as imported_main

        main = reload(imported_main)
        fake_meta_collection = object()

        with patch.dict(
            os.environ,
            {
                "COOCLE_RESET_DATA_ON_START": "1",
                "COOCLE_RESET_DATA_STRICT": "1",
                "RENDER_GIT_COMMIT": "abc123",
            },
            clear=False,
        ), patch.object(
            main.dbmod,
            "reset_runtime_data",
            return_value={"pages": 2, "crawl_queue": 1, "summarization_usage": 4},
        ), patch.object(main.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            main.astra_utils,
            "get_astra_meta_collection",
            return_value=fake_meta_collection,
        ), patch.object(
            main.astra_utils,
            "get_reset_marker",
            return_value=None,
        ), patch.object(
            main.astra_utils,
            "get_astra_collection",
            side_effect=RuntimeError("astra offline"),
        ):
            with patch.object(main.logger, "warning"), patch.object(main.logger, "exception") as log_exception:
                with self.assertRaises(RuntimeError):
                    await main._reset_datastores_on_start(object())

        log_exception.assert_called_once()

    async def test_startup_reset_skips_astra_without_credentials(self) -> None:
        from backend import main as imported_main

        main = reload(imported_main)

        with patch.dict(os.environ, {"COOCLE_RESET_DATA_ON_START": "1"}, clear=False), patch.object(
            main.dbmod,
            "reset_runtime_data",
            return_value={"pages": 2, "crawl_queue": 1, "summarization_usage": 4},
        ) as reset_db, patch.object(main.astra_utils, "has_astra_credentials", return_value=False), patch.object(
            main.astra_utils,
            "get_astra_collection",
        ) as get_collection:
            with patch.object(main.logger, "warning"):
                await main._reset_datastores_on_start(object())

        reset_db.assert_called_once()
        get_collection.assert_not_called()

    async def test_startup_reset_skips_when_same_deploy_marker_exists(self) -> None:
        from backend import main as imported_main

        main = reload(imported_main)
        fake_meta_collection = object()

        with patch.dict(
            os.environ,
            {"COOCLE_RESET_DATA_ON_START": "1", "RENDER_GIT_COMMIT": "abc123"},
            clear=False,
        ), patch.object(main.astra_utils, "has_astra_credentials", return_value=True), patch.object(
            main.astra_utils,
            "get_astra_meta_collection",
            return_value=fake_meta_collection,
        ), patch.object(
            main.astra_utils,
            "get_reset_marker",
            return_value={"_id": astra_utils.ASTRA_RESET_MARKER_ID, "deploy_key": "render:abc123"},
        ), patch.object(main.dbmod, "reset_runtime_data") as reset_db, patch.object(
            main.astra_utils, "get_astra_collection"
        ) as get_collection:
            await main._reset_datastores_on_start(object())

        reset_db.assert_not_called()
        get_collection.assert_not_called()

    async def test_startup_reset_skips_everything_when_disabled(self) -> None:
        from backend import main as imported_main

        main = reload(imported_main)

        with patch.dict(os.environ, {"COOCLE_RESET_DATA_ON_START": "0"}, clear=False), patch.object(
            main.dbmod,
            "reset_runtime_data",
        ) as reset_db, patch.object(main.astra_utils, "get_astra_collection") as get_collection:
            await main._reset_datastores_on_start(object())

        reset_db.assert_not_called()
        get_collection.assert_not_called()
