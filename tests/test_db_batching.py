from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from backend import db


class DbBatchingTests(unittest.TestCase):
    def test_upsert_queue_uses_hundred_sized_batches(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        rows = [(f"https://example.com/{idx}", 0, "2026-04-18T00:00:00+00:00") for idx in range(250)]

        written = db.upsert_queue(conn, rows)

        self.assertEqual(written, 250)
        self.assertEqual(cursor.executemany.call_count, 3)
        batch_lengths = [len(call.args[1]) for call in cursor.executemany.call_args_list]
        self.assertEqual(batch_lengths, [100, 100, 50])
        self.assertEqual(conn.commit.call_count, 3)

    def test_upsert_pages_uses_hundred_sized_batches(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        rows = [
            (
                f"https://example.com/{idx}",
                f"Title {idx}",
                "content",
                "2026-04-18T00:00:00+00:00",
                200,
                "text/html",
                None,
                None,
                None,
                None,
                "en",
            )
            for idx in range(250)
        ]

        written = db.upsert_pages(conn, rows)

        self.assertEqual(written, 250)
        self.assertEqual(cursor.executemany.call_count, 3)
        batch_lengths = [len(call.args[1]) for call in cursor.executemany.call_args_list]
        self.assertEqual(batch_lengths, [100, 100, 50])
        self.assertEqual(conn.commit.call_count, 3)


if __name__ == "__main__":
    unittest.main()
