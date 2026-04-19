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

CREATE TABLE IF NOT EXISTS newsletter_subscribers (
  email TEXT PRIMARY KEY,
  name TEXT,
  source_ip TEXT,
  subscribed_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

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


LEGACY_ADDITIVE_COLUMNS = {
    "pages": {
        "fetched_at": "TEXT",
        "status_code": "INTEGER",
        "content_type": "TEXT",
        "embedding": "BLOB",
        "embedding_dim": "INTEGER",
        "embedding_norm": "REAL",
        "embedding_model": "TEXT",
        "language": "TEXT",
    },
    "crawl_queue": {
        "last_error": "TEXT",
    },
}


def _chunked(items: list, chunk_size: int):
    size = max(1, int(chunk_size))
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _ensure_legacy_columns(conn: sqlite3.Connection) -> None:
    for table_name, expected_columns in LEGACY_ADDITIVE_COLUMNS.items():
        existing_columns = _table_columns(conn, table_name)
        for column_name, column_type in expected_columns.items():
            if column_name in existing_columns:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


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
    _ensure_legacy_columns(conn)
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

    cursor = conn.cursor()
    for batch in _chunked(rows, batch_size):
        cursor.executemany(
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
        cursor = conn.execute(f"DELETE FROM crawl_queue WHERE url IN ({placeholders})", batch)
        conn.commit()
        deleted += max(int(cursor.rowcount or 0), 0)
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

    cursor = conn.cursor()
    for batch in _chunked(page_rows, batch_size):
        cursor.executemany(PAGE_UPSERT_SQL, batch)
        conn.commit()
    return len(page_rows)


def reset_runtime_data(conn: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name in ("pages", "crawl_queue", "summarization_usage"):
        row = conn.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
        counts[table_name] = int(row["count"] if row else 0)

    conn.execute("DELETE FROM pages")
    conn.execute("DELETE FROM crawl_queue")
    conn.execute("DELETE FROM summarization_usage")
    conn.commit()
    return counts


def upsert_newsletter_subscriber(
    conn: sqlite3.Connection,
    *,
    email: str,
    name: str | None,
    source_ip: str | None,
    subscribed_at: str,
) -> bool:
    existing = conn.execute(
        "SELECT email FROM newsletter_subscribers WHERE email = ? LIMIT 1",
        (email,),
    ).fetchone()
    created = existing is None
    conn.execute(
        """
        INSERT INTO newsletter_subscribers(email, name, source_ip, subscribed_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
          name = COALESCE(excluded.name, newsletter_subscribers.name),
          source_ip = COALESCE(excluded.source_ip, newsletter_subscribers.source_ip),
          updated_at = excluded.updated_at
        """,
        (email, name, source_ip, subscribed_at, subscribed_at),
    )
    conn.commit()
    return created


def list_newsletter_subscriber_emails(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT email
        FROM newsletter_subscribers
        ORDER BY subscribed_at, email
        """
    ).fetchall()
    return [str(row["email"]) for row in rows if row["email"]]


def count_newsletter_subscribers(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS count FROM newsletter_subscribers").fetchone()
    return int(row["count"] if row else 0)
