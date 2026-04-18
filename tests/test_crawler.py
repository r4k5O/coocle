from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from backend import crawler, db


class CrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_crawl_loop_skips_urls_already_present_in_sqlite(self) -> None:
        conn = db.connect(f"file:crawler_skip_existing_{id(self)}?mode=memory&cache=shared")
        db.init_db(conn)
        conn.execute(
            """
            INSERT INTO pages (url, title, content, fetched_at, status_code, content_type, language)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "https://alpha.test/root",
                "Root",
                "Bereits gespeichert",
                "2026-04-18T15:00:00",
                200,
                "text/html",
                "de",
            ),
        )
        conn.commit()
        calls = {"http": 0}

        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, url, timeout=None, headers=None):
                calls["http"] += 1
                raise AssertionError("Existing SQLite pages should not be fetched again")

        async def always_allowed(self, client, url, user_agent):
            return True

        def no_delay(self, url, user_agent):
            return 0.0

        cfg = crawler.CrawlConfig(
            max_pages=5,
            max_depth=0,
            delay_s=0.0,
            same_host_only=False,
            max_concurrency=1,
        )

        with patch("backend.crawler.httpx.AsyncClient", FakeClient), patch.object(
            crawler.RobotsCache, "allowed", always_allowed
        ), patch.object(crawler.RobotsCache, "get_delay", no_delay):
            await crawler.crawl_loop(
                conn=conn,
                db=db,
                seeds=["https://alpha.test/root"],
                cfg=cfg,
                stop_event=asyncio.Event(),
                run_forever=False,
            )

        page_count = conn.execute("SELECT COUNT(*) AS c FROM pages").fetchone()["c"]
        conn.close()

        self.assertEqual(calls["http"], 0)
        self.assertEqual(page_count, 1)

    async def test_crawl_loop_restores_known_astra_pages_without_refetching(self) -> None:
        conn = db.connect(f"file:crawler_restore_astra_{id(self)}?mode=memory&cache=shared")
        db.init_db(conn)
        calls = {"http": 0}
        fake_collection = SimpleNamespace(full_name="testspace.coocle_pages")

        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, url, timeout=None, headers=None):
                calls["http"] += 1
                raise AssertionError("Known Astra pages should be restored without a live refetch")

        async def always_allowed(self, client, url, user_agent):
            return True

        def no_delay(self, url, user_agent):
            return 0.0

        cfg = crawler.CrawlConfig(
            max_pages=5,
            max_depth=0,
            delay_s=0.0,
            same_host_only=False,
            max_concurrency=1,
        )

        with patch.dict("os.environ", {"RENDER": "true"}, clear=False), patch(
            "backend.crawler.httpx.AsyncClient", FakeClient
        ), patch.object(crawler.RobotsCache, "allowed", always_allowed), patch.object(
            crawler.RobotsCache, "get_delay", no_delay
        ), patch.object(crawler.astra_utils, "is_astra_enabled", return_value=True), patch.object(
            crawler.astra_utils,
            "get_astra_collection",
            return_value=fake_collection,
        ), patch.object(
            crawler.astra_utils,
            "get_document_by_id",
            return_value={
                "_id": "https://alpha.test/root",
                "url": "https://alpha.test/root",
                "title": "Root",
                "content": "Aus Astra wiederhergestellt",
                "fetched_at": "2026-04-18T15:05:00",
                "status_code": 200,
                "content_type": "text/html",
                "language": "de",
            },
        ):
            await crawler.crawl_loop(
                conn=conn,
                db=db,
                seeds=["https://alpha.test/root"],
                cfg=cfg,
                stop_event=asyncio.Event(),
                run_forever=False,
            )

        row = conn.execute("SELECT url, title, content FROM pages").fetchone()
        conn.close()

        self.assertEqual(calls["http"], 0)
        self.assertEqual(row["url"], "https://alpha.test/root")
        self.assertEqual(row["title"], "Root")
        self.assertIn("Astra", row["content"])

    async def test_follow_up_pages_are_processed_before_remaining_seeds(self) -> None:
        conn = db.connect(f"file:crawler_order_{id(self)}?mode=memory&cache=shared")
        db.init_db(conn)

        class FakeResponse:
            def __init__(self, url: str):
                self.status_code = 200
                self.headers = {"content-type": "text/html"}
                if url == "https://alpha.test/root":
                    self.text = '<html><title>Root</title><body><a href="/child">Child</a></body></html>'
                elif url == "https://alpha.test/child":
                    self.text = "<html><title>Child</title><body>child body</body></html>"
                else:
                    self.text = "<html><title>Seed</title><body>second seed</body></html>"

        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, url, timeout=None, headers=None):
                return FakeResponse(str(url))

        async def always_allowed(self, client, url, user_agent):
            return True

        def no_delay(self, url, user_agent):
            return 0.0

        cfg = crawler.CrawlConfig(
            max_pages=2,
            max_depth=3,
            delay_s=0.0,
            same_host_only=False,
            max_concurrency=1,
        )

        with patch("backend.crawler.httpx.AsyncClient", FakeClient), patch.object(
            crawler.RobotsCache, "allowed", always_allowed
        ), patch.object(crawler.RobotsCache, "get_delay", no_delay):
            await crawler.crawl_loop(
                conn=conn,
                db=db,
                seeds=["https://alpha.test/root", "https://beta.test/seed"],
                cfg=cfg,
                stop_event=asyncio.Event(),
                run_forever=False,
            )

        urls = {
            str(row["url"])
            for row in conn.execute("SELECT url FROM pages").fetchall()
        }
        conn.close()

        self.assertIn("https://alpha.test/root", urls)
        self.assertIn("https://alpha.test/child", urls)
        self.assertNotIn("https://beta.test/seed", urls)

    async def test_crawl_loop_uses_parallel_requests(self) -> None:
        conn = db.connect(f"file:crawler_parallel_{id(self)}?mode=memory&cache=shared")
        db.init_db(conn)
        state = {"active": 0, "max_active": 0}

        class FakeResponse:
            def __init__(self, url: str):
                self.status_code = 200
                self.headers = {"content-type": "text/html"}
                self.text = f"<html><title>{url}</title><body>ok</body></html>"

        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, url, timeout=None, headers=None):
                state["active"] += 1
                state["max_active"] = max(state["max_active"], state["active"])
                try:
                    await asyncio.sleep(0.05)
                    return FakeResponse(str(url))
                finally:
                    state["active"] -= 1

        async def always_allowed(self, client, url, user_agent):
            return True

        def no_delay(self, url, user_agent):
            return 0.0

        cfg = crawler.CrawlConfig(
            max_pages=4,
            max_depth=0,
            delay_s=0.0,
            same_host_only=False,
            max_concurrency=4,
        )

        seeds = [
            "https://one.test/",
            "https://two.test/",
            "https://three.test/",
            "https://four.test/",
        ]

        with patch("backend.crawler.httpx.AsyncClient", FakeClient), patch.object(
            crawler.RobotsCache, "allowed", always_allowed
        ), patch.object(crawler.RobotsCache, "get_delay", no_delay):
            await crawler.crawl_loop(
                conn=conn,
                db=db,
                seeds=seeds,
                cfg=cfg,
                stop_event=asyncio.Event(),
                run_forever=False,
            )

        page_count = conn.execute("SELECT COUNT(*) AS c FROM pages").fetchone()["c"]
        conn.close()

        self.assertEqual(page_count, 4)
        self.assertGreaterEqual(state["max_active"], 2)


if __name__ == "__main__":
    unittest.main()
