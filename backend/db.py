from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable


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
    # Lightweight migration for existing DBs created before embedding columns existed.
    for stmt in (
        "ALTER TABLE pages ADD COLUMN embedding BLOB",
        "ALTER TABLE pages ADD COLUMN embedding_dim INTEGER",
        "ALTER TABLE pages ADD COLUMN embedding_norm REAL",
        "ALTER TABLE pages ADD COLUMN embedding_model TEXT",
        "ALTER TABLE pages ADD COLUMN language TEXT",
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
    conn.commit()


def upsert_queue(conn: sqlite3.Connection, urls: Iterable[tuple[str, int, str]]) -> int:
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO crawl_queue(url, depth, discovered_at)
        VALUES (?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
          depth = MIN(crawl_queue.depth, excluded.depth)
        """,
        list(urls),
    )
    conn.commit()
    return cur.rowcount

