from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import deque
from dataclasses import dataclass, field
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


def _excerpt(text: str, limit: int = 220) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return f"{value[: max(1, limit - 1)].rstrip()}…"


def _chunked(items: list, chunk_size: int):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def _restore_page_row_from_astra_doc(url: str, doc: dict | None) -> tuple[tuple[object, ...], dict[str, object]] | None:
    if not doc:
        return None

    content = str(doc.get("content") or "").strip()
    fetched_at = str(doc.get("fetched_at") or _now_iso())
    status_code_raw = doc.get("status_code")
    try:
        status_code = int(status_code_raw) if status_code_raw is not None else 200
    except (TypeError, ValueError):
        status_code = 200

    content_type = str(doc.get("content_type") or "text/html")[:200]
    language = doc.get("language")
    title = doc.get("title") or url

    return (
        (
            url,
            title,
            content,
            fetched_at,
            status_code,
            content_type,
            None,
            None,
            None,
            None,
            language,
        ),
        {
            "url": url,
            "title": str(title),
            "excerpt": _excerpt(content),
            "fetched_at": fetched_at,
            "status_code": status_code,
            "content_type": content_type,
            "language": language,
            "storage_state": "pending_batch",
        },
    )


@dataclass(frozen=True)
class CrawlConfig:
    user_agent: str = "CoocleBot/0.1"
    max_pages: int = 200
    max_depth: int = 2
    request_timeout_s: float = 15.0
    delay_s: float = 0.6
    same_host_only: bool = True
    max_content_chars: int = 250_000
    enable_embeddings: bool = False
    embed_cfg: OllamaEmbedConfig | None = None
    max_concurrency: int = 4


@dataclass
class CrawlTaskResult:
    url: str
    depth: int
    state: str
    message: str
    pages_done_delta: int = 0
    pages_saved_delta: int = 0
    skipped_delta: int = 0
    errors_delta: int = 0
    page_row: tuple[object, ...] | None = None
    indexed_page: dict[str, object] | None = None
    queue_rows: list[tuple[str, int, str]] = field(default_factory=list)
    astra_doc: dict[str, object] | None = None


class HostRequestGate:
    def __init__(self, default_delay_s: float) -> None:
        self._default_delay_s = max(0.0, float(default_delay_s))
        self._locks: dict[str, asyncio.Lock] = {}
        self._next_allowed_at: dict[str, float] = {}

    async def run(
        self,
        url: str,
        *,
        delay_lookup: Callable[[], float | None],
        operation: Callable[[], Awaitable[object]],
    ) -> object:
        parsed = urlparse(url)
        key = f"{parsed.scheme}://{parsed.netloc}"
        lock = self._locks.setdefault(key, asyncio.Lock())

        async with lock:
            now = time.monotonic()
            next_allowed = self._next_allowed_at.get(key, 0.0)
            if next_allowed > now:
                await asyncio.sleep(next_allowed - now)

            try:
                return await operation()
            finally:
                delay_s = max(self._default_delay_s, float(delay_lookup() or 0.0))
                self._next_allowed_at[key] = time.monotonic() + delay_s


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
    seed_urls = set(seeds_norm)

    db.upsert_queue(conn, [(u, 0, _now_iso()) for u in seeds_norm])
    _log.info(
        "Queued %d seed(s). max_pages=%d max_depth=%d same_host_only=%s delay=%.2fs concurrency=%d",
        len(seeds_norm),
        cfg.max_pages,
        cfg.max_depth,
        cfg.same_host_only,
        cfg.delay_s,
        max(1, int(cfg.max_concurrency)),
    )

    robots = RobotsCache()
    embed_cfg = cfg.embed_cfg or (env_embed_config() if cfg.enable_embeddings else None)
    batch_size = int(getattr(db, "DEFAULT_WRITE_BATCH_SIZE", 100))

    # Initialize Astra if enabled
    astra_col = None
    if astra_utils.should_use_astra_runtime():
        astra_col = astra_utils.get_astra_collection()
        if astra_col:
            _log.info("AstraDB enabled. Collection: %s", astra_col.full_name)
        else:
            _log.error("AstraDB enabled but collection initialization failed.")
    restore_from_astra = bool(astra_col and os.environ.get("RENDER", "").strip().lower() == "true")

    async with httpx.AsyncClient(follow_redirects=True) as client:
        pages_done = 0
        pages_saved = 0
        skipped = 0
        errors = 0
        pending_page_rows: list[tuple[object, ...]] = []
        pending_indexed_pages: list[dict[str, object]] = []
        pending_astra_docs: list[dict[str, object]] = []
        frontier_rows: deque[tuple[str, int]] = deque()
        seen_urls: set[str] = set()
        active_scans: dict[str, dict[str, object]] = {}
        request_gate = HostRequestGate(cfg.delay_s)
        wave_size = max(1, int(cfg.max_concurrency))

        async def publish_status(**payload) -> None:
            if status_hook is None:
                return
            result = status_hook(payload)
            if asyncio.iscoroutine(result):
                await result

        def current_scans() -> list[dict[str, object]]:
            return list(active_scans.values())

        async def flush_pending_writes(*, force: bool = False) -> None:
            if not force and len(pending_page_rows) < batch_size and len(pending_astra_docs) < batch_size:
                return

            had_pending_indexed_pages = bool(pending_indexed_pages)

            if pending_page_rows:
                db.upsert_pages(conn, pending_page_rows, batch_size=batch_size)
                pending_page_rows.clear()

            if pending_astra_docs and astra_col:
                await asyncio.to_thread(
                    astra_utils.upsert_documents,
                    astra_col,
                    pending_astra_docs,
                    batch_size=batch_size,
                )
                pending_astra_docs.clear()

            if had_pending_indexed_pages:
                pending_indexed_pages.clear()
                await publish_status(
                    pending_indexed_pages=[],
                    pending_indexed_count=0,
                    current_scans=current_scans(),
                )

        async def fetch_response(url: str) -> tuple[str, httpx.Response | None]:
            async def operation() -> tuple[str, httpx.Response | None]:
                if not await robots.allowed(client, url, cfg.user_agent):
                    return ("blocked", None)
                response = await client.get(
                    url,
                    timeout=cfg.request_timeout_s,
                    headers={"User-Agent": cfg.user_agent, "Accept": "text/html,application/xhtml+xml"},
                )
                return ("ok", response)

            result = await request_gate.run(
                url,
                delay_lookup=lambda: robots.get_delay(url, cfg.user_agent),
                operation=operation,
            )
            return result  # type: ignore[return-value]

        async def process_row(url: str, depth: int) -> CrawlTaskResult:
            is_seed_refresh = depth == 0 and url in seed_urls

            if depth > cfg.max_depth:
                return CrawlTaskResult(
                    url=url,
                    depth=depth,
                    state="skipped",
                    message="Maximale Tiefe ueberschritten",
                    skipped_delta=1,
                )

            if cfg.same_host_only and not any(_same_host(url, s) for s in seeds_norm):
                return CrawlTaskResult(
                    url=url,
                    depth=depth,
                    state="skipped",
                    message="Anderer Host als Seed",
                    skipped_delta=1,
                )

            try:
                existing_row = conn.execute(
                    """
                    SELECT url, title, content, fetched_at, status_code, content_type, language
                    FROM pages
                    WHERE url = ?
                    LIMIT 1
                    """,
                    (url,),
                ).fetchone()
                if existing_row is not None and not is_seed_refresh:
                    return CrawlTaskResult(
                        url=url,
                        depth=depth,
                        state="skipped",
                        message="Bereits in SQLite indexiert",
                        skipped_delta=1,
                    )

                if restore_from_astra and astra_col and not is_seed_refresh:
                    cached_doc = await asyncio.to_thread(astra_utils.get_document_by_id, astra_col, url)
                    restored = _restore_page_row_from_astra_doc(url, cached_doc)
                    if restored is not None:
                        page_row, indexed_page = restored
                        return CrawlTaskResult(
                            url=url,
                            depth=depth,
                            state="restored",
                            message="Aus Astra übernommen",
                            pages_done_delta=1,
                            pages_saved_delta=1,
                            page_row=page_row,
                            indexed_page=indexed_page,
                        )

                fetch_state, response = await fetch_response(url)
                if fetch_state == "blocked":
                    return CrawlTaskResult(
                        url=url,
                        depth=depth,
                        state="skipped",
                        message="robots.txt blockiert den Abruf",
                        skipped_delta=1,
                    )

                if response is None:
                    return CrawlTaskResult(
                        url=url,
                        depth=depth,
                        state="error",
                        message="Leere HTTP-Antwort",
                        errors_delta=1,
                    )

                ct = (response.headers.get("content-type") or "").lower()
                if response.status_code >= 400:
                    _log.debug("Skip HTTP %s %s", response.status_code, url)
                    return CrawlTaskResult(
                        url=url,
                        depth=depth,
                        state="skipped",
                        message=f"HTTP {response.status_code}",
                        skipped_delta=1,
                    )

                if "text/html" not in ct and "application/xhtml+xml" not in ct:
                    _log.debug("Skip non-HTML (%s) %s", ct[:60], url)
                    return CrawlTaskResult(
                        url=url,
                        depth=depth,
                        state="skipped",
                        message="Nicht-HTML Inhalt uebersprungen",
                        skipped_delta=1,
                    )

                html = response.text
                if len(html) > cfg.max_content_chars:
                    html = html[: cfg.max_content_chars]

                title, text = _extract_text_and_title(html)
                if len(text) > cfg.max_content_chars:
                    text = text[: cfg.max_content_chars]

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
                extra_errors = 0
                if embed_cfg is not None and not astra_col:
                    embed_input = (title or "") + "\n\n" + text[:8000]
                    try:
                        vec = await embed_text(client, embed_cfg, embed_input)
                        embedding_blob = sqlite3.Binary(floats_to_blob(vec))
                        embedding_dim = int(len(vec))
                        embedding_norm = float(l2_norm(vec))
                        embedding_model = embed_cfg.model
                    except Exception:
                        extra_errors += 1
                        _log.debug("Embedding error for %s", url, exc_info=True)

                fetched_at = _now_iso()
                queue_rows: list[tuple[str, int, str]] = []
                if depth < cfg.max_depth:
                    discovered = _now_iso()
                    queue_rows = [(link, depth + 1, discovered) for link in _extract_links(url, html)]

                astra_doc = None
                if astra_col:
                    astra_doc = {
                        "_id": url,
                        "url": url,
                        "title": title,
                        "content": text[:3500],
                        "fetched_at": fetched_at,
                        "status_code": int(response.status_code),
                        "content_type": ct[:200],
                        "language": lang,
                        "$vectorize": (title or "") + " " + text[:500],
                    }

                return CrawlTaskResult(
                    url=url,
                    depth=depth,
                    state="saved",
                    message="Seite indexiert",
                    pages_done_delta=1,
                    pages_saved_delta=1,
                    errors_delta=extra_errors,
                    page_row=(
                        url,
                        title,
                        text,
                        fetched_at,
                        int(response.status_code),
                        ct[:200],
                        embedding_blob,
                        embedding_dim,
                        embedding_norm,
                        embedding_model,
                        lang,
                    ),
                    indexed_page={
                        "url": url,
                        "title": title or url,
                        "excerpt": _excerpt(text),
                        "fetched_at": fetched_at,
                        "status_code": int(response.status_code),
                        "content_type": ct[:200],
                        "language": lang,
                        "storage_state": "pending_batch",
                    },
                    queue_rows=queue_rows,
                    astra_doc=astra_doc,
                )
            except (httpx.HTTPError, sqlite3.Error, UnicodeError):
                _log.debug("Fetch/store error for %s", url, exc_info=True)
                return CrawlTaskResult(
                    url=url,
                    depth=depth,
                    state="error",
                    message="Fetch- oder Speicherfehler",
                    errors_delta=1,
                )
            except Exception:
                _log.debug("Unexpected error for %s", url, exc_info=True)
                return CrawlTaskResult(
                    url=url,
                    depth=depth,
                    state="error",
                    message="Unerwarteter Crawl-Fehler",
                    errors_delta=1,
                )

        while not stop_event.is_set():
            if cfg.max_pages > 0 and pages_done >= cfg.max_pages:
                await flush_pending_writes(force=True)
                await publish_status(
                    state="idle",
                    current_url=None,
                    current_depth=None,
                    current_scans=[],
                    message=f"Max pages erreicht ({cfg.max_pages})",
                    pages_done=pages_done,
                    pages_saved=pages_saved,
                    skipped=skipped,
                    errors=errors,
                )
                _log.info("Reached max_pages (%d). Stopping.", cfg.max_pages)
                break

            current_wave_size = wave_size
            if cfg.max_pages > 0:
                current_wave_size = min(current_wave_size, cfg.max_pages - pages_done)
            current_wave_size = max(1, current_wave_size)

            work_rows: list[tuple[str, int]] = []
            while frontier_rows and len(work_rows) < current_wave_size:
                work_rows.append(frontier_rows.popleft())

            stale_queue_urls: list[str] = []
            if len(work_rows) < current_wave_size:
                rows = conn.execute(
                    "SELECT url, depth FROM crawl_queue ORDER BY discovered_at LIMIT ?",
                    (batch_size,),
                ).fetchall()
                for row in rows:
                    if len(work_rows) >= current_wave_size:
                        break
                    url = str(row["url"])
                    depth = int(row["depth"])
                    if url in seen_urls:
                        stale_queue_urls.append(url)
                        continue
                    seen_urls.add(url)
                    work_rows.append((url, depth))

            if stale_queue_urls:
                db.delete_queue_urls(conn, stale_queue_urls, batch_size=batch_size)

            if not work_rows:
                await flush_pending_writes(force=True)
                await publish_status(
                    state="idle",
                    current_url=None,
                    current_depth=None,
                    current_scans=[],
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

            processed_queue_urls: list[str] = []
            pending_queue_rows: list[tuple[str, int, str]] = []

            for url, depth in work_rows:
                processed_queue_urls.append(url)
                active_scans[url] = {
                    "url": url,
                    "depth": depth,
                    "state": "fetching",
                    "message": "Seite wird gecrawlt",
                    "updated_at": _now_iso(),
                }

            await publish_status(
                state="fetching",
                current_url=work_rows[-1][0],
                current_depth=work_rows[-1][1],
                current_scans=current_scans(),
                message="Seiten werden parallel gecrawlt" if len(work_rows) > 1 else "Seite wird gecrawlt",
                pages_done=pages_done,
                pages_saved=pages_saved,
                pending_indexed_pages=list(pending_indexed_pages),
                pending_indexed_count=len(pending_indexed_pages),
                skipped=skipped,
                errors=errors,
            )

            tasks = [asyncio.create_task(process_row(url, depth)) for url, depth in work_rows]
            for task in asyncio.as_completed(tasks):
                result = await task
                active_scans.pop(result.url, None)

                pages_done += result.pages_done_delta
                pages_saved += result.pages_saved_delta
                skipped += result.skipped_delta
                errors += result.errors_delta

                if result.page_row is not None:
                    pending_page_rows.append(result.page_row)
                if result.indexed_page is not None:
                    pending_indexed_pages.append(result.indexed_page)
                if result.astra_doc is not None:
                    pending_astra_docs.append(result.astra_doc)

                for queue_row in result.queue_rows:
                    discovered_url = str(queue_row[0])
                    if discovered_url in seen_urls:
                        continue
                    seen_urls.add(discovered_url)
                    frontier_rows.append((discovered_url, int(queue_row[1])))
                    pending_queue_rows.append(queue_row)

                await publish_status(
                    state=result.state,
                    current_url=result.url,
                    current_depth=result.depth,
                    current_scans=current_scans(),
                    message=result.message,
                    pages_done=pages_done,
                    pages_saved=pages_saved,
                    pending_indexed_pages=list(pending_indexed_pages),
                    pending_indexed_count=len(pending_indexed_pages),
                    skipped=skipped,
                    errors=errors,
                )

                if result.state in {"saved", "restored"}:
                    _log.info("Saved (%d/%d) depth=%d %s", pages_done, cfg.max_pages, result.depth, result.url)

            if processed_queue_urls:
                db.delete_queue_urls(conn, processed_queue_urls, batch_size=batch_size)
            if pending_queue_rows:
                db.upsert_queue(conn, pending_queue_rows, batch_size=batch_size)

            await flush_pending_writes(force=False)
