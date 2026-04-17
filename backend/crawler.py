from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Iterable
from urllib.parse import urljoin, urldefrag, urlparse
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory

from . import db
from . import astra_utils
from .summarize import env_chat_config
from .embeddings import OllamaEmbedConfig, embed_text, env_embed_config, floats_to_blob, l2_norm

# Ensure consistent results from langdetect
DetectorFactory.seed = 0


_WS_RE = re.compile(r"\s+")
_log = logging.getLogger("coocle.crawler")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_url(url: str) -> str | None:
    url = (url or "").strip()
    if not url:
        return None
    url, _frag = urldefrag(url)
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        return None
    if not p.netloc:
        return None
    return url


def _same_host(a: str, b: str) -> bool:
    pa = urlparse(a)
    pb = urlparse(b)
    return pa.scheme == pb.scheme and pa.netloc == pb.netloc


def _extract_text_and_title(html: str) -> tuple[str | None, str]:
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
    text = soup.get_text(" ", strip=True)
    text = _WS_RE.sub(" ", text).strip()
    return (title or None, text)


def _extract_links(base_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    out: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        norm = _normalize_url(abs_url)
        if norm:
            out.append(norm)
    return out


@dataclass(frozen=True)
class CrawlConfig:
    user_agent: str = "CoocleBot/0.1 (+local)"
    max_pages: int = 200
    max_depth: int = 2
    request_timeout_s: float = 15.0
    delay_s: float = 0.6
    same_host_only: bool = True
    max_content_chars: int = 250_000
    enable_embeddings: bool = False
    embed_cfg: OllamaEmbedConfig | None = None


class RobotsCache:
    def __init__(self) -> None:
        self._cache: dict[str, tuple[float, RobotFileParser | None]] = {}

    async def allowed(self, client: httpx.AsyncClient, url: str, user_agent: str) -> bool:
        p = urlparse(url)
        key = f"{p.scheme}://{p.netloc}"
        now = time.time()
        cached = self._cache.get(key)
        if cached and now - cached[0] < 3600 and cached[1] is not None:
            return cached[1].can_fetch(user_agent, url)
        if cached and now - cached[0] < 300 and cached[1] is None:
            return True

        robots_url = f"{key}/robots.txt"
        try:
            r = await client.get(robots_url, timeout=10.0, headers={"User-Agent": user_agent})
            if r.status_code >= 400:
                self._cache[key] = (now, None)
                return True
            rp = RobotFileParser()
            rp.parse(r.text.splitlines())
            self._cache[key] = (now, rp)
            return rp.can_fetch(user_agent, url)
        except Exception:
            self._cache[key] = (now, None)
            return True

    def get_delay(self, url: str, user_agent: str) -> float | None:
        p = urlparse(url)
        key = f"{p.scheme}://{p.netloc}"
        cached = self._cache.get(key)
        if cached and cached[1]:
            try:
                d = cached[1].crawl_delay(user_agent)
                if d is not None:
                    return float(d)
            except Exception:
                pass
        return None


async def crawl_loop(
    *,
    conn,
    db,
    seeds: Iterable[str],
    cfg: CrawlConfig,
    stop_event: asyncio.Event,
    run_forever: bool = False,
    status_hook: Callable[[dict], Awaitable[None] | None] | None = None,
) -> None:
    # Lazy import to avoid circulars
    import sqlite3

    seeds_norm = [u for u in (_normalize_url(s) for s in seeds) if u]
    if not seeds_norm:
        _log.warning("No valid seeds after normalization.")
        return

    db.upsert_queue(conn, [(u, 0, _now_iso()) for u in seeds_norm])
    _log.info(
        "Queued %d seed(s). max_pages=%d max_depth=%d same_host_only=%s delay=%.2fs",
        len(seeds_norm),
        cfg.max_pages,
        cfg.max_depth,
        cfg.same_host_only,
        cfg.delay_s,
    )

    robots = RobotsCache()
    embed_cfg = cfg.embed_cfg or (env_embed_config() if cfg.enable_embeddings else None)

    # Initialize Astra if enabled
    astra_col = None
    if astra_utils.is_astra_enabled():
        astra_col = astra_utils.get_astra_collection()
        if astra_col:
            _log.info("AstraDB enabled. Collection: %s", astra_col.full_name)
        else:
            _log.error("AstraDB enabled but collection initialization failed.")

    async with httpx.AsyncClient(follow_redirects=True) as client:
        pages_done = 0
        pages_saved = 0
        skipped = 0
        errors = 0

        async def publish_status(**payload) -> None:
            if status_hook is None:
                return
            result = status_hook(payload)
            if asyncio.iscoroutine(result):
                await result

        while not stop_event.is_set():
            if cfg.max_pages > 0 and pages_done >= cfg.max_pages:
                await publish_status(
                    state="idle",
                    current_url=None,
                    current_depth=None,
                    message=f"Max pages erreicht ({cfg.max_pages})",
                    pages_done=pages_done,
                    pages_saved=pages_saved,
                    skipped=skipped,
                    errors=errors,
                )
                _log.info("Reached max_pages (%d). Stopping.", cfg.max_pages)
                break

            row = conn.execute(
                "SELECT url, depth FROM crawl_queue ORDER BY discovered_at LIMIT 1"
            ).fetchone()
            if not row:
                await publish_status(
                    state="idle",
                    current_url=None,
                    current_depth=None,
                    message="Warteschlange leer",
                    pages_done=pages_done,
                    pages_saved=pages_saved,
                    skipped=skipped,
                    errors=errors,
                )
                if run_forever:
                    await asyncio.sleep(0.5)
                    continue
                _log.info(
                    "Queue empty. done=%d saved=%d skipped=%d errors=%d",
                    pages_done,
                    pages_saved,
                    skipped,
                    errors,
                )
                break

            url = str(row["url"])
            depth = int(row["depth"])
            conn.execute("DELETE FROM crawl_queue WHERE url = ?", (url,))
            conn.commit()
            await publish_status(
                state="fetching",
                current_url=url,
                current_depth=depth,
                message="Seite wird gecrawlt",
                pages_done=pages_done,
                pages_saved=pages_saved,
                skipped=skipped,
                errors=errors,
            )

            if depth > cfg.max_depth:
                skipped += 1
                await publish_status(
                    state="skipped",
                    current_url=url,
                    current_depth=depth,
                    message="Maximale Tiefe ueberschritten",
                    pages_done=pages_done,
                    pages_saved=pages_saved,
                    skipped=skipped,
                    errors=errors,
                )
                continue

            if cfg.same_host_only and not any(_same_host(url, s) for s in seeds_norm):
                skipped += 1
                await publish_status(
                    state="skipped",
                    current_url=url,
                    current_depth=depth,
                    message="Anderer Host als Seed",
                    pages_done=pages_done,
                    pages_saved=pages_saved,
                    skipped=skipped,
                    errors=errors,
                )
                continue

            try:
                if not await robots.allowed(client, url, cfg.user_agent):
                    skipped += 1
                    await publish_status(
                        state="skipped",
                        current_url=url,
                        current_depth=depth,
                        message="robots.txt blockiert den Abruf",
                        pages_done=pages_done,
                        pages_saved=pages_saved,
                        skipped=skipped,
                        errors=errors,
                    )
                    continue

                r = await client.get(
                    url,
                    timeout=cfg.request_timeout_s,
                    headers={"User-Agent": cfg.user_agent, "Accept": "text/html,application/xhtml+xml"},
                )
                ct = (r.headers.get("content-type") or "").lower()
                if r.status_code >= 400:
                    skipped += 1
                    await publish_status(
                        state="skipped",
                        current_url=url,
                        current_depth=depth,
                        message=f"HTTP {r.status_code}",
                        pages_done=pages_done,
                        pages_saved=pages_saved,
                        skipped=skipped,
                        errors=errors,
                    )
                    _log.debug("Skip HTTP %s %s", r.status_code, url)
                    continue
                if "text/html" not in ct and "application/xhtml+xml" not in ct:
                    skipped += 1
                    await publish_status(
                        state="skipped",
                        current_url=url,
                        current_depth=depth,
                        message="Nicht-HTML Inhalt uebersprungen",
                        pages_done=pages_done,
                        pages_saved=pages_saved,
                        skipped=skipped,
                        errors=errors,
                    )
                    _log.debug("Skip non-HTML (%s) %s", ct[:60], url)
                    continue

                html = r.text
                if len(html) > cfg.max_content_chars:
                    html = html[: cfg.max_content_chars]

                title, text = _extract_text_and_title(html)
                if len(text) > cfg.max_content_chars:
                    text = text[: cfg.max_content_chars]

                # Language detection
                lang = None
                if text and len(text.strip()) > 20:
                    try:
                        lang = detect(text[:2000])
                    except Exception:
                        _log.debug("Language detection failed for %s", url)

                embedding_blob = None
                embedding_dim = None
                embedding_norm = None
                embedding_model = None
                if embed_cfg is not None and not astra_col:
                    # Keep embedding input bounded (title + leading content).
                    embed_input = (title or "") + "\n\n" + text[:8000]
                    try:
                        vec = await embed_text(client, embed_cfg, embed_input)
                        embedding_blob = sqlite3.Binary(floats_to_blob(vec))
                        embedding_dim = int(len(vec))
                        embedding_norm = float(l2_norm(vec))
                        embedding_model = embed_cfg.model
                    except Exception:
                        errors += 1
                        _log.debug("Embedding error for %s", url, exc_info=True)

                # AstraDB Insert
                if astra_col:
                    try:
                        doc = {
                            "_id": url,
                            "url": url,
                            "title": title,
                            "content": text[:3500],
                            "fetched_at": _now_iso(),
                            "status_code": int(r.status_code),
                            "content_type": ct[:200],
                            "language": lang,
                            "$vectorize": (title or "") + " " + text[:500]
                        }
                        astra_col.find_one_and_replace(
                            filter={"_id": url},
                            replacement=doc,
                            upsert=True
                        )
                    except Exception as ae:
                        _log.debug("AstraDB insertion error for %s", url, exc_info=True)

                conn.execute(
                    """
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
                    """,
                    (
                        url,
                        title,
                        text,
                        _now_iso(),
                        int(r.status_code),
                        ct[:200],
                        embedding_blob,
                        embedding_dim,
                        embedding_norm,
                        embedding_model,
                        lang,
                    ),
                )
                conn.commit()
                pages_done += 1
                pages_saved += 1
                await publish_status(
                    state="saved",
                    current_url=url,
                    current_depth=depth,
                    message="Seite indexiert",
                    pages_done=pages_done,
                    pages_saved=pages_saved,
                    skipped=skipped,
                    errors=errors,
                )
                _log.info("Saved (%d/%d) depth=%d %s", pages_done, cfg.max_pages, depth, url)

                if depth < cfg.max_depth:
                    links = _extract_links(url, html)
                    discovered = _now_iso()
                    db.upsert_queue(conn, [(l, depth + 1, discovered) for l in links])

            except (httpx.HTTPError, sqlite3.Error, UnicodeError):
                errors += 1
                await publish_status(
                    state="error",
                    current_url=url,
                    current_depth=depth,
                    message="Fetch- oder Speicherfehler",
                    pages_done=pages_done,
                    pages_saved=pages_saved,
                    skipped=skipped,
                    errors=errors,
                )
                _log.debug("Fetch/store error for %s", url, exc_info=True)
            except Exception:
                errors += 1
                await publish_status(
                    state="error",
                    current_url=url,
                    current_depth=depth,
                    message="Unerwarteter Crawl-Fehler",
                    pages_done=pages_done,
                    pages_saved=pages_saved,
                    skipped=skipped,
                    errors=errors,
                )
                _log.debug("Unexpected error for %s", url, exc_info=True)

            # Respect robots.txt Crawl-delay if present, else use config delay.
            robots_delay = robots.get_delay(url, cfg.user_agent)
            wait_s = max(cfg.delay_s, robots_delay or 0)
            await asyncio.sleep(wait_s)
