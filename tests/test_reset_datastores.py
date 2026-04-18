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


class StartupResetOrchestrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_reset_clears_sqlite_and_astra_when_enabled(self) -> None:
        from backend import main as imported_main

        main = reload(imported_main)
        fake_collection = SimpleNamespace(full_name="testspace.coocle_pages")

        with patch.dict(os.environ, {"COOCLE_RESET_DATA_ON_START": "1"}, clear=False), patch.object(
            main.dbmod,
            "reset_runtime_data",
            return_value={"pages": 2, "crawl_queue": 1, "summarization_usage": 4},
        ) as reset_db, patch.object(main.astra_utils, "is_astra_enabled", return_value=True), patch.object(
            main.astra_utils,
            "get_astra_collection",
            return_value=fake_collection,
        ) as get_collection, patch.object(main.astra_utils, "clear_documents", return_value=9) as clear_documents:
            with patch.object(main.logger, "warning") as log_warning:
                await main._reset_datastores_on_start(object())

        reset_db.assert_called_once()
        get_collection.assert_called_once_with()
        clear_documents.assert_called_once_with(fake_collection)
        self.assertGreaterEqual(log_warning.call_count, 2)

    async def test_startup_reset_tolerates_astra_failure_when_not_strict(self) -> None:
        from backend import main as imported_main

        main = reload(imported_main)

        with patch.dict(
            os.environ,
            {"COOCLE_RESET_DATA_ON_START": "1", "COOCLE_RESET_DATA_STRICT": "0"},
            clear=False,
        ), patch.object(
            main.dbmod,
            "reset_runtime_data",
            return_value={"pages": 2, "crawl_queue": 1, "summarization_usage": 4},
        ) as reset_db, patch.object(main.astra_utils, "is_astra_enabled", return_value=True), patch.object(
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

        with patch.dict(
            os.environ,
            {"COOCLE_RESET_DATA_ON_START": "1", "COOCLE_RESET_DATA_STRICT": "1"},
            clear=False,
        ), patch.object(
            main.dbmod,
            "reset_runtime_data",
            return_value={"pages": 2, "crawl_queue": 1, "summarization_usage": 4},
        ), patch.object(main.astra_utils, "is_astra_enabled", return_value=True), patch.object(
            main.astra_utils,
            "get_astra_collection",
            side_effect=RuntimeError("astra offline"),
        ):
            with patch.object(main.logger, "warning"), patch.object(main.logger, "exception") as log_exception:
                with self.assertRaises(RuntimeError):
                    await main._reset_datastores_on_start(object())

        log_exception.assert_called_once()

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
