from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

DEFAULT_WRITE_BATCH_SIZE = 100


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS pages (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  title TEXT,
  content TEXT,
  fetched_at TEXT,
  status_code INTEGER,
  content_type TEXT,
  embedding BLOB,
  embedding_dim INTEGER,
  embedding_norm REAL,
  embedding_model TEXT,
  language TEXT
);

CREATE TABLE IF NOT EXISTS crawl_queue (
  url TEXT PRIMARY KEY,
  depth INTEGER NOT NULL,
  discovered_at TEXT NOT NULL,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS summarization_usage (
  ip TEXT NOT NULL,
  day TEXT NOT NULL,
  count INTEGER DEFAULT 0,
  PRIMARY KEY (ip, day)
);

-- Full-text index (FTS5)
CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(
  url,
  title,
  content,
  content='pages',
  content_rowid='id',
  tokenize='porter'
);

CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
  INSERT INTO pages_fts(rowid, url, title, content) VALUES (new.id, new.url, new.title, new.content);
END;

CREATE TRIGGER IF NOT EXISTS pages_ad AFTER DELETE ON pages BEGIN
  INSERT INTO pages_fts(pages_fts, rowid, url, title, content)
  VALUES('delete', old.id, old.url, old.title, old.content);
END;

CREATE TRIGGER IF NOT EXISTS pages_au AFTER UPDATE ON pages BEGIN
  INSERT INTO pages_fts(pages_fts, rowid, url, title, content)
  VALUES('delete', old.id, old.url, old.title, old.content);
  INSERT INTO pages_fts(rowid, url, title, content)
  VALUES (new.id, new.url, new.title, new.content);
END;
"""

PAGE_UPSERT_SQL = """
INSERT INTO pages(
  url, title, content, fetched_at, status_code, content_type,
  embedding, embedding_dim, embedding_norm, embedding_model,
  language
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(url) DO UPDATE SET
  title=excluded.title,
  content=excluded.content,
  fetched_at=excluded.fetched_at,
  status_code=excluded.status_code,
  content_type=excluded.content_type,
  embedding=COALESCE(excluded.embedding, pages.embedding),
  embedding_dim=COALESCE(excluded.embedding_dim, pages.embedding_dim),
  embedding_norm=COALESCE(excluded.embedding_norm, pages.embedding_norm),
  embedding_model=COALESCE(excluded.embedding_model, pages.embedding_model),
  language=excluded.language
"""


def _chunked(items: list, chunk_size: int):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def connect(db_path: Path | str) -> sqlite3.Connection:
    target = str(db_path)
    use_uri = target.startswith("file:")
    if target != ":memory:" and not use_uri:
        Path(target).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target, check_same_thread=False, uri=use_uri)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    # Lightweight migrations for existing DBs created before newer crawl metadata and
    # embedding columns existed.
    for stmt in (
        "ALTER TABLE pages ADD COLUMN fetched_at TEXT",
        "ALTER TABLE pages ADD COLUMN status_code INTEGER",
        "ALTER TABLE pages ADD COLUMN content_type TEXT",
        "ALTER TABLE pages ADD COLUMN embedding BLOB",
        "ALTER TABLE pages ADD COLUMN embedding_dim INTEGER",
        "ALTER TABLE pages ADD COLUMN embedding_norm REAL",
        "ALTER TABLE pages ADD COLUMN embedding_model TEXT",
        "ALTER TABLE pages ADD COLUMN language TEXT",
        "ALTER TABLE crawl_queue ADD COLUMN last_error TEXT",
    ):
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass
    
    # Ensure summarization_usage table exists (legacy check if script was already run)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS summarization_usage (
            ip TEXT NOT NULL,
            day TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            PRIMARY KEY (ip, day)
        )
    """)

    # Keep the external-content FTS index authoritative on startup. Older databases may
    # already contain rows in `pages` from before `pages_fts` existed, and simple row
    # counts are not enough to detect that kind of drift reliably.
    conn.execute("INSERT INTO pages_fts(pages_fts) VALUES('rebuild')")
    conn.commit()


def upsert_queue(
    conn: sqlite3.Connection,
    urls: Iterable[tuple[str, int, str]],
    *,
    batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
) -> int:
    rows = list(urls)
    if not rows:
        return 0

    cur = conn.cursor()
    for batch in _chunked(rows, batch_size):
        cur.executemany(
            """
            INSERT INTO crawl_queue(url, depth, discovered_at)
            VALUES (?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
              depth = MIN(crawl_queue.depth, excluded.depth)
            """,
            batch,
        )
        conn.commit()
    return len(rows)


def delete_queue_urls(
    conn: sqlite3.Connection,
    urls: Iterable[str],
    *,
    batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
) -> int:
    rows = [url for url in urls if url]
    if not rows:
        return 0

    deleted = 0
    for batch in _chunked(rows, batch_size):
        placeholders = ", ".join("?" for _ in batch)
        cur = conn.execute(f"DELETE FROM crawl_queue WHERE url IN ({placeholders})", batch)
        conn.commit()
        deleted += max(cur.rowcount, 0)
    return deleted


def upsert_pages(
    conn: sqlite3.Connection,
    rows: Iterable[tuple[object, ...]],
    *,
    batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
) -> int:
    page_rows = list(rows)
    if not page_rows:
        return 0

    cur = conn.cursor()
    for batch in _chunked(page_rows, batch_size):
        cur.executemany(PAGE_UPSERT_SQL, batch)
        conn.commit()
    return len(page_rows)


def reset_runtime_data(conn: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in ("pages", "crawl_queue", "summarization_usage"):
        row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
        counts[table] = int(row["count"] if row else 0)

    conn.execute("DELETE FROM pages")
    conn.execute("DELETE FROM crawl_queue")
    conn.execute("DELETE FROM summarization_usage")
    conn.commit()
    return counts

