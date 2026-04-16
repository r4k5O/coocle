from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import sqlite3
import httpx
from bs4 import BeautifulSoup

from . import db as dbmod
from .embeddings import embed_batch, env_embed_config, floats_to_blob, l2_norm


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill titles and/or embeddings for an existing Coocle DB.")
    p.add_argument("--db", default=os.environ.get("COOCLE_DB", ""), help="Path to SQLite DB.")
    p.add_argument(
        "--titles",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Backfill missing titles (default: true).",
    )
    p.add_argument(
        "--embeddings",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Backfill missing embeddings for vector search (default: false).",
    )
    p.add_argument(
        "--fetch-titles",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Fetch HTML to extract better titles (slower; default: false).",
    )
    p.add_argument("--limit", type=int, default=5000, help="Max rows to process.")
    p.add_argument("--batch-size", type=int, default=16, help="Embedding batch size.")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


_WS_RE = re.compile(r"\s+")


def _title_from_url(url: str) -> str:
    p = urlparse(url)
    host = p.netloc or url
    path = (p.path or "/").rstrip("/")
    if path and path != "/":
        return f"{host}{path}"
    return host


def _extract_title_from_html(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    title = (soup.title.string if soup.title and soup.title.string else "").strip()
    if not title:
        og = soup.select_one('meta[property="og:title"],meta[name="og:title"]')
        if og and og.get("content"):
            title = str(og.get("content")).strip()
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(" ", strip=True)
            title = _WS_RE.sub(" ", title).strip()
    return title or None


async def _backfill_titles(conn, *, fetch_titles: bool, limit: int) -> int:
    log = logging.getLogger("coocle.backfill.titles")
    rows = conn.execute(
        """
        SELECT id, url, title
        FROM pages
        WHERE title IS NULL OR TRIM(title) = ''
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    if not rows:
        log.info("No missing titles found.")
        return 0

    updated = 0
    if not fetch_titles:
        for r in rows:
            conn.execute("UPDATE pages SET title = ? WHERE id = ?", (_title_from_url(r["url"]), r["id"]))
            updated += 1
        conn.commit()
        log.info("Backfilled %d title(s) from URL.", updated)
        return updated

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for r in rows:
            url = str(r["url"])
            title = None
            try:
                resp = await client.get(url, timeout=15.0, headers={"User-Agent": "CoocleBot/0.1 (+local)"})
                ct = (resp.headers.get("content-type") or "").lower()
                if resp.status_code < 400 and ("text/html" in ct or "application/xhtml+xml" in ct):
                    title = _extract_title_from_html(resp.text)
            except Exception:
                title = None

            if not title:
                title = _title_from_url(url)
            conn.execute("UPDATE pages SET title = ? WHERE id = ?", (title, r["id"]))
            updated += 1
            if updated % 50 == 0:
                conn.commit()
                log.info("Backfilled titles: %d/%d", updated, len(rows))
        conn.commit()
        log.info("Backfilled %d title(s) (fetch_titles=%s).", updated, fetch_titles)
        return updated


async def _backfill_embeddings(conn, *, batch_size: int, limit: int) -> int:
    log = logging.getLogger("coocle.backfill.embed")
    cfg = env_embed_config()

    rows = conn.execute(
        """
        SELECT id, url, title, content
        FROM pages
        WHERE embedding IS NULL
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    if not rows:
        log.info("No missing embeddings found.")
        return 0

    async with httpx.AsyncClient(follow_redirects=True) as client:
        updated = 0
        i = 0
        while i < len(rows):
            batch = rows[i : i + batch_size]
            inputs: list[str] = []
            for r in batch:
                title = (r["title"] or "").strip()
                content = (r["content"] or "").strip()
                # bounded input for consistent cost
                inputs.append((title + "\n\n" + content[:2000]).strip()[:2000])

            vecs = await embed_batch(client, cfg, inputs)
            for r, vec in zip(batch, vecs):
                blob = sqlite3.Binary(floats_to_blob(vec))  # type: ignore[name-defined]
                dim = int(len(vec))
                norm = float(l2_norm(vec) or 1.0)
                conn.execute(
                    """
                    UPDATE pages
                    SET embedding = ?, embedding_dim = ?, embedding_norm = ?, embedding_model = ?
                    WHERE id = ?
                    """,
                    (blob, dim, norm, cfg.model, r["id"]),
                )
                updated += 1

            conn.commit()
            i += batch_size
            log.info("Backfilled embeddings: %d/%d", updated, len(rows))

        log.info("Backfilled %d embedding(s) using model=%s host=%s", updated, cfg.model, cfg.host)
        return updated


async def main_async() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    root = Path(__file__).resolve().parents[1]
    db_path = Path(args.db) if args.db else (root / "data" / "coocle.db")
    log = logging.getLogger("coocle.backfill")
    log.info("DB=%s titles=%s embeddings=%s fetch_titles=%s", db_path, args.titles, args.embeddings, args.fetch_titles)

    conn = dbmod.connect(db_path)
    dbmod.init_db(conn)  # applies embedding columns migration if needed

    changed = 0
    if args.titles:
        changed += await _backfill_titles(conn, fetch_titles=bool(args.fetch_titles), limit=int(args.limit))
    if args.embeddings:
        changed += await _backfill_embeddings(conn, batch_size=int(args.batch_size), limit=int(args.limit))

    conn.close()
    log.info("Done. changed=%d", changed)
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()

