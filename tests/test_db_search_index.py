from __future__ import annotations

import unittest

from backend import db
from backend.search import search


class DbSearchIndexTests(unittest.TestCase):
    def test_init_db_rebuilds_fts_for_legacy_pages(self) -> None:
        conn = db.connect(f"file:legacy_fts_{id(self)}?mode=memory&cache=shared")
        conn.row_factory = db.sqlite3.Row

        conn.executescript(
            """
            CREATE TABLE pages (
              id INTEGER PRIMARY KEY,
              url TEXT NOT NULL UNIQUE,
              title TEXT,
              content TEXT,
              fetched_at TEXT,
              status_code INTEGER,
              content_type TEXT
            );
            INSERT INTO pages(url, title, content, fetched_at, status_code, content_type)
            VALUES (
              'https://example.com/python',
              'Python Docs',
              'Python testing guide and search migration example.',
              '2026-04-18T14:00:00+00:00',
              200,
              'text/html'
            );
            """
        )
        conn.commit()

        db.init_db(conn)

        page_count = conn.execute("SELECT COUNT(*) AS c FROM pages").fetchone()["c"]
        fts_count = conn.execute("SELECT COUNT(*) AS c FROM pages_fts").fetchone()["c"]
        results = search(conn, "python", limit=5)

        self.assertEqual(page_count, 1)
        self.assertEqual(fts_count, 1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["url"], "https://example.com/python")

    def test_init_db_upgrades_legacy_columns_needed_by_pages_overview(self) -> None:
        conn = db.connect(f"file:legacy_overview_{id(self)}?mode=memory&cache=shared")
        conn.row_factory = db.sqlite3.Row

        conn.executescript(
            """
            CREATE TABLE pages (
              id INTEGER PRIMARY KEY,
              url TEXT NOT NULL UNIQUE,
              title TEXT,
              content TEXT
            );
            CREATE TABLE crawl_queue (
              url TEXT PRIMARY KEY,
              depth INTEGER NOT NULL,
              discovered_at TEXT NOT NULL
            );
            INSERT INTO pages(url, title, content)
            VALUES ('https://example.com/python', 'Python Docs', 'Legacy page row');
            INSERT INTO crawl_queue(url, depth, discovered_at)
            VALUES ('https://example.com/queued', 2, '2026-04-18T14:05:00');
            """
        )
        conn.commit()

        db.init_db(conn)

        pages_row = conn.execute(
            """
            SELECT fetched_at, status_code, content_type, language
            FROM pages
            WHERE url = ?
            """,
            ("https://example.com/python",),
        ).fetchone()
        queue_row = conn.execute(
            "SELECT last_error FROM crawl_queue WHERE url = ?",
            ("https://example.com/queued",),
        ).fetchone()

        self.assertIsNotNone(pages_row)
        self.assertIn("fetched_at", pages_row.keys())
        self.assertIn("status_code", pages_row.keys())
        self.assertIn("content_type", pages_row.keys())
        self.assertIn("language", pages_row.keys())
        self.assertIsNotNone(queue_row)
        self.assertIn("last_error", queue_row.keys())
