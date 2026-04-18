from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from ipaddress import ip_address
from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse

import httpx
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env if it exists

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import db as dbmod
from . import astra_utils
from .crawler import CrawlConfig, crawl_loop
from .search import search as fts_search
from .search import vector_search as vec_search
from .summarize import OllamaChatConfig, SummaryResult, env_chat_config, summarize_results


ROOT = Path(__file__).resolve().parents[1]
FREE_SUMMARY_LIMIT = 10
logger = logging.getLogger(__name__)
SUMMARY_CONTEXT_LIMIT = 5
ASTRA_COUNT_CACHE_TTL_S = 30.0


class SlidingWindowRateLimiter:
    def __init__(self) -> None:
        self._events: dict[tuple[str, str], deque[float]] = {}
        self._lock = asyncio.Lock()

    async def allow(self, bucket: str, key: str, limit: int, window_seconds: int) -> tuple[bool, int]:
        now = time.monotonic()
        cutoff = now - window_seconds

        async with self._lock:
            events = self._events.setdefault((bucket, key), deque())
            while events and events[0] <= cutoff:
                events.popleft()

            if len(events) >= limit:
                retry_after = max(1, int(events[0] + window_seconds - now))
                return False, retry_after

            events.append(now)
            return True, 0


def _db_path() -> str:
    return os.environ.get("COOCLE_DB", str(ROOT / "data" / "coocle.db"))


def _truthy_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _conn_from_request(request: Request):
    return request.app.state.conn


def _request_ip(request: Request) -> str:
    if _truthy_env("COOCLE_TRUST_PROXY_HEADERS", default=False):
        forwarded = request.headers.get("x-forwarded-for", "").strip()
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("x-real-ip", "").strip()
        if real_ip:
            return real_ip
    return request.client.host if request.client else "unknown"


def _is_local_client_ip(ip: str) -> bool:
    try:
        parsed = ip_address(ip)
    except ValueError:
        return ip in {"localhost", "testclient"}
    return parsed.is_loopback


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return max(1, int(value))
    except ValueError:
        logger.warning("Invalid integer env %s=%r; using default %s", name, value, default)
        return default


def _security_headers(response) -> None:
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Cache-Control"] = "no-store"


def _validate_custom_ollama_host(host: str | None, request: Request) -> str | None:
    if not host:
        return None

    candidate = host.strip()
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="X-Ollama-Host muss mit http:// oder https:// beginnen.")
    if not parsed.netloc:
        raise HTTPException(status_code=400, detail="X-Ollama-Host ist ungueltig.")
    if parsed.username or parsed.password:
        raise HTTPException(status_code=400, detail="X-Ollama-Host darf keine Zugangsdaten enthalten.")

    hostname = (parsed.hostname or "").strip().lower()
    if not hostname:
        raise HTTPException(status_code=400, detail="X-Ollama-Host ist ungueltig.")

    allow_private = _truthy_env("COOCLE_ALLOW_PRIVATE_OLLAMA_HOSTS", default=False)
    if parsed.scheme != "https" and not allow_private:
        if not (hostname in {"localhost", "127.0.0.1", "::1"} and _is_local_client_ip(_request_ip(request))):
            raise HTTPException(
                status_code=400,
                detail="Externe X-Ollama-Hosts muessen HTTPS verwenden. Fuer lokale Hosts ist ein lokaler Request erforderlich.",
            )

    try:
        parsed_ip = ip_address(hostname)
    except ValueError:
        return candidate

    if parsed_ip.is_private or parsed_ip.is_link_local or parsed_ip.is_reserved or parsed_ip.is_multicast:
        if not allow_private and not (parsed_ip.is_loopback and _is_local_client_ip(_request_ip(request))):
            raise HTTPException(
                status_code=400,
                detail="Private oder interne X-Ollama-Hosts sind deaktiviert.",
            )

    return candidate


def _usage_count(conn, ip: str, day: str) -> int:
    row = conn.execute(
        "SELECT count FROM summarization_usage WHERE ip = ? AND day = ?",
        (ip, day),
    ).fetchone()
    return row["count"] if row else 0


def _increment_usage(conn, ip: str, day: str) -> None:
    conn.execute(
        """
        INSERT INTO summarization_usage (ip, day, count)
        VALUES (?, ?, 1)
        ON CONFLICT(ip, day) DO UPDATE SET count = count + 1
        """,
        (ip, day),
    )
    conn.commit()


def _enrich_results_for_summary(conn, results: list[dict], limit: int = SUMMARY_CONTEXT_LIMIT) -> list[dict]:
    enriched: list[dict] = []
    for idx, result in enumerate(results):
        prepared = dict(result)
        url = str(prepared.get("url") or "").strip()
        if idx < limit and url:
            row = conn.execute(
                "SELECT content FROM pages WHERE url = ?",
                (url,),
            ).fetchone()
            if row and row["content"]:
                prepared["page_content"] = row["content"]
        enriched.append(prepared)
    return enriched


def _excerpt(text: str | None, limit: int = 220) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= limit:
        return value
    return f"{value[: max(1, limit - 1)].rstrip()}…"


def _astra_collection_for_runtime():
    if not astra_utils.has_astra_credentials():
        return None
    return astra_utils.get_astra_collection()


def _astra_document_count_exact(collection) -> int | None:
    return astra_utils.exact_document_count(collection)


def _astra_document_count_live(collection) -> int | None:
    return astra_utils.live_document_count(collection)


def _astra_document_count_estimate(collection) -> int | None:
    return astra_utils.estimated_document_count(collection)


def _astra_runtime_status() -> dict[str, object]:
    astra_collection = _astra_collection_for_runtime()
    return {
        "enabled": astra_utils.is_astra_enabled(),
        "credentials_configured": astra_utils.has_astra_credentials(),
        "connected": bool(astra_collection),
        "collection": getattr(astra_collection, "full_name", None) if astra_collection else None,
        "document_count": None,
        "document_count_exact": None,
        "document_count_live": None,
        "document_count_estimate": None,
        "count_is_estimate": False,
        "count_source": "unavailable",
        "count_is_live": False,
    }


def _astra_count_snapshot(
    request: Request,
    *,
    live: bool = False,
    allow_estimate: bool = True,
) -> dict[str, object]:
    if not live:
        now = time.monotonic()
        cached = getattr(request.app.state, "astra_count_cache", None)
        if isinstance(cached, dict) and float(cached.get("expires_at", 0.0)) > now:
            snapshot = cached.get("snapshot")
            if isinstance(snapshot, dict):
                return dict(snapshot)

    astra_collection = _astra_collection_for_runtime()
    exact_count = _astra_document_count_exact(astra_collection)
    live_count = None
    estimate_count = None
    effective_count = exact_count
    count_is_estimate = False
    count_source = "astra_exact" if exact_count is not None else "unavailable"

    if effective_count is None and live:
        live_count = _astra_document_count_live(astra_collection)
        if live_count is not None:
            effective_count = live_count
            count_source = "astra_live_scan"

    if effective_count is None:
        if allow_estimate:
            estimate_count = _astra_document_count_estimate(astra_collection)
            if estimate_count is not None:
                effective_count = estimate_count
                count_is_estimate = True
                count_source = "astra_estimate"

    snapshot = {
        "enabled": astra_utils.is_astra_enabled(),
        "credentials_configured": astra_utils.has_astra_credentials(),
        "connected": bool(astra_collection),
        "collection": getattr(astra_collection, "full_name", None) if astra_collection else None,
        "document_count": effective_count,
        "document_count_exact": exact_count,
        "document_count_live": live_count,
        "document_count_estimate": estimate_count,
        "count_is_estimate": count_is_estimate,
        "count_source": count_source,
        "count_is_live": bool(effective_count is not None and not count_is_estimate),
    }
    if not live:
        request.app.state.astra_count_cache = {
            "expires_at": time.monotonic() + ASTRA_COUNT_CACHE_TTL_S,
            "snapshot": snapshot,
        }
    return dict(snapshot)


def _reset_deploy_key() -> str | None:
    explicit = os.environ.get("COOCLE_RESET_DEPLOY_KEY", "").strip()
    if explicit:
        return explicit

    render_commit = os.environ.get("RENDER_GIT_COMMIT", "").strip()
    if render_commit:
        return f"render:{render_commit}"

    return None


async def _reset_datastores_on_start(conn) -> None:
    if not _truthy_env("COOCLE_RESET_DATA_ON_START", default=False):
        return
    strict = _truthy_env("COOCLE_RESET_DATA_STRICT", default=False)
    reset_key = _reset_deploy_key()
    meta_collection = None
    astra_reset_succeeded = not astra_utils.has_astra_credentials()

    if reset_key and astra_utils.has_astra_credentials():
        try:
            meta_collection = await asyncio.to_thread(astra_utils.get_astra_meta_collection)
            if meta_collection is None:
                raise RuntimeError("Astra reset metadata collection could not be opened.")

            marker = await asyncio.to_thread(astra_utils.get_reset_marker, meta_collection)
            if marker and marker.get("deploy_key") == reset_key:
                logger.info("Startup reset already applied for %s; skipping", reset_key)
                return
        except Exception:
            logger.exception(
                "Startup reset marker lookup failed%s",
                "; aborting startup" if strict else "; skipping reset for safety",
            )
            if strict:
                raise
            return

    try:
        cleared = dbmod.reset_runtime_data(conn)
        logger.warning(
            "Startup reset cleared SQLite data before serving traffic: pages=%s queue=%s usage=%s",
            cleared["pages"],
            cleared["crawl_queue"],
            cleared["summarization_usage"],
        )
    except Exception:
        logger.exception("Startup SQLite reset failed%s", "; aborting startup" if strict else "; continuing")
        if strict:
            raise
        return

    if not astra_utils.has_astra_credentials():
        return

    try:
        astra_collection = await asyncio.to_thread(astra_utils.get_astra_collection)
        if not astra_collection:
            raise RuntimeError("AstraDB reset requested on startup, but the collection could not be opened.")

        deleted = await asyncio.to_thread(astra_utils.clear_documents, astra_collection)
        astra_reset_succeeded = True
        logger.warning(
            "Startup reset cleared AstraDB collection %s: %s document(s) removed",
            getattr(astra_collection, "full_name", "unknown"),
            deleted,
        )
    except Exception:
        logger.exception("Startup Astra reset failed%s", "; aborting startup" if strict else "; continuing")
        if strict:
            raise

    if reset_key and meta_collection is not None and astra_reset_succeeded:
        try:
            await asyncio.to_thread(astra_utils.set_reset_marker, meta_collection, reset_key)
            logger.info("Startup reset marker stored for %s", reset_key)
        except Exception:
            logger.exception(
                "Startup reset marker update failed%s",
                "; aborting startup" if strict else "; continuing",
            )
            if strict:
                raise


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    astra_utils.reset_astra_cache()
    fastapi_app.state.conn = dbmod.connect(_db_path())
    dbmod.init_db(fastapi_app.state.conn)
    await _reset_datastores_on_start(fastapi_app.state.conn)
    fastapi_app.state.rate_limiter = SlidingWindowRateLimiter()
    fastapi_app.state.summary_semaphore = asyncio.Semaphore(
        _int_env("COOCLE_SUMMARY_CONCURRENCY_LIMIT", 4)
    )
    fastapi_app.state.astra_count_cache = None
    fastapi_app.state.crawl_status = {
        "state": "idle",
        "current_url": None,
        "current_depth": None,
        "current_scans": [],
        "message": "Crawler inaktiv",
        "pages_done": 0,
        "pages_saved": 0,
        "pending_indexed_pages": [],
        "pending_indexed_count": 0,
        "skipped": 0,
        "errors": 0,
        "updated_at": datetime.now().isoformat(),
    }
    fastapi_app.state.stop_event = asyncio.Event()
    fastapi_app.state.crawler_task = None

    try:
        async def set_crawl_status(payload: dict) -> None:
            fastapi_app.state.crawl_status = {
                **fastapi_app.state.crawl_status,
                **payload,
                "updated_at": datetime.now().isoformat(),
            }

        if astra_utils.is_astra_enabled() and _truthy_env("COOCLE_PREWARM_ASTRA", default=False):
            try:
                await asyncio.to_thread(astra_utils.get_astra_collection)
            except Exception:
                logger.exception("Astra prewarm failed; continuing without prewarmed collection")

        if os.environ.get("COOCLE_START_CRAWLER", "").strip() in ("1", "true", "yes"):
            seeds = [s.strip() for s in os.environ.get("COOCLE_SEEDS", "").split(",") if s.strip()]

            # fallback to seeds-general.txt if no env seeds
            if not seeds and (ROOT / "seeds-general.txt").exists():
                with open(ROOT / "seeds-general.txt", "r", encoding="utf-8") as f:
                    seeds = [line.strip() for line in f if line.strip() and not line.startswith("#")]

            max_pages = int(os.environ.get("COOCLE_MAX_PAGES", "0"))
            cfg = CrawlConfig(
                max_pages=max_pages,
                max_depth=int(os.environ.get("COOCLE_MAX_DEPTH", "10")),
                delay_s=float(os.environ.get("COOCLE_DELAY_S", "0.6")),
                same_host_only=os.environ.get("COOCLE_SAME_HOST_ONLY", "0") not in ("0", "false", "no"),
                max_concurrency=int(os.environ.get("COOCLE_CRAWL_CONCURRENCY", "4")),
            )
            if seeds:
                fastapi_app.state.crawler_task = asyncio.create_task(
                    crawl_loop(
                        conn=fastapi_app.state.conn,
                        db=dbmod,
                        seeds=seeds,
                        cfg=cfg,
                        stop_event=fastapi_app.state.stop_event,
                        run_forever=True,
                        status_hook=set_crawl_status,
                    )
                )

        yield
    finally:
        stop_event = getattr(fastapi_app.state, "stop_event", None)
        crawler_task = getattr(fastapi_app.state, "crawler_task", None)
        conn = getattr(fastapi_app.state, "conn", None)

        if stop_event:
            stop_event.set()
        if crawler_task:
            crawler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await crawler_task
        if conn:
            conn.close()
            fastapi_app.state.conn = None


app = FastAPI(title="Coocle", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def protect_api(request: Request, call_next):
    if request.url.path.startswith("/api/"):
        limiter = request.app.state.rate_limiter
        ip = _request_ip(request)

        general_ok, general_retry = await limiter.allow(
            "api-general",
            ip,
            _int_env("COOCLE_API_RATE_LIMIT", 60),
            _int_env("COOCLE_API_RATE_WINDOW_S", 60),
        )
        if not general_ok:
            response = JSONResponse(
                status_code=429,
                content={"detail": "Zu viele API-Anfragen. Bitte kurz warten."},
            )
            response.headers["Retry-After"] = str(general_retry)
            _security_headers(response)
            return response

        if request.url.path == "/api/search" and request.query_params.get("summarize", "").lower() in {"1", "true", "yes"}:
            summary_ok, summary_retry = await limiter.allow(
                "api-summary",
                ip,
                _int_env("COOCLE_SUMMARY_RATE_LIMIT", 6),
                _int_env("COOCLE_SUMMARY_RATE_WINDOW_S", 60),
            )
            if not summary_ok:
                response = JSONResponse(
                    status_code=429,
                    content={"detail": "Zu viele Zusammenfassungs-Anfragen. Bitte kurz warten."},
                )
                response.headers["Retry-After"] = str(summary_retry)
                _security_headers(response)
                return response

    response = await call_next(request)
    _security_headers(response)
    return response


@app.get("/api/search")
async def api_search(
    request: Request,
    q: Annotated[str, Query(min_length=1, max_length=200)],
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    mode: Annotated[str, Query(pattern="^(fts|vector|hybrid)$")] = "hybrid",
    summarize: bool = False,
    x_ollama_key: Annotated[str | None, Header()] = None,
    x_ollama_host: Annotated[str | None, Header()] = None,
):
    conn = _conn_from_request(request)
    ip = _request_ip(request)
    day = datetime.now().strftime("%Y-%m-%d")
    validated_ollama_host = _validate_custom_ollama_host(x_ollama_host, request)

    if mode == "vector":
        results = await vec_search(conn, q, limit=limit)
    elif mode == "hybrid":
        # Simple hybrid: take half from vector + half from FTS, dedupe by URL.
        v = await vec_search(conn, q, limit=max(1, limit // 2))
        f = fts_search(conn, q, limit=limit)
        seen = set()
        results = []
        for r in v + f:
            u = r.get("url")
            if not u or u in seen:
                continue
            seen.add(u)
            results.append(r)
            if len(results) >= limit:
                break
    else:
        results = fts_search(conn, q, limit=limit)

    summary = None
    summary_status = "unavailable"
    summary_message = None
    summary_format = None

    if summarize and results:
        summary_result = SummaryResult(status="unavailable")

        if not x_ollama_key:
            count = _usage_count(conn, ip, day)
            if count >= FREE_SUMMARY_LIMIT:
                summary_result = SummaryResult(
                    status="credits_exhausted",
                    message=(
                        "Du hast deine 10 freien Zusammenfassungen fuer heute aufgebraucht. "
                        "Bitte warte bis morgen oder hinterlege deinen eigenen Ollama API-Key."
                    ),
                )

        if summary_result.status != "credits_exhausted":
            chat_cfg = None
            if x_ollama_key:
                default_cfg = env_chat_config()
                chat_cfg = OllamaChatConfig(
                    host=validated_ollama_host or "https://ollama.com/api",
                    model=default_cfg.model,
                    api_key=x_ollama_key,
                    timeout_s=default_cfg.timeout_s,
                )

            summary_inputs = _enrich_results_for_summary(conn, results)
            async with request.app.state.summary_semaphore:
                async with httpx.AsyncClient() as client:
                    summary_result = await summarize_results(client, q, summary_inputs, cfg=chat_cfg)

            if summary_result.status == "ok" and summary_result.summary and not x_ollama_key:
                _increment_usage(conn, ip, day)

        summary = summary_result.summary
        summary_status = summary_result.status
        summary_message = summary_result.message
        summary_format = "markdown" if summary_result.status == "ok" and summary_result.summary else None
    elif summarize:
        summary_message = "Keine Suchergebnisse zum Zusammenfassen."

    return {
        "results": results,
        "summary": summary,
        "summary_status": summary_status,
        "summary_message": summary_message,
        "summary_format": summary_format,
    }

@app.get("/api/credits")
async def get_credits(request: Request):
    conn = _conn_from_request(request)
    ip = _request_ip(request)
    day = datetime.now().strftime("%Y-%m-%d")
    count = _usage_count(conn, ip, day)
    return {"used": count, "total": FREE_SUMMARY_LIMIT, "remaining": max(0, FREE_SUMMARY_LIMIT - count)}


@app.get("/api/stats")
def api_stats(request: Request):
    conn = _conn_from_request(request)
    sqlite_pages = conn.execute("SELECT COUNT(*) AS c FROM pages").fetchone()["c"]
    queued = conn.execute("SELECT COUNT(*) AS c FROM crawl_queue").fetchone()["c"]
    astra_status = _astra_count_snapshot(request, live=False, allow_estimate=True)
    astra_count = astra_status.get("document_count")
    pages = max(int(sqlite_pages), int(astra_count or 0))
    return {
        "pages": pages,
        "queued": queued,
        "db": str(_db_path()),
        "sqlite_pages": sqlite_pages,
        "astra_pages": astra_count,
        "astra_pages_exact": astra_status.get("document_count_exact"),
        "astra_pages_estimate": astra_status.get("document_count_estimate"),
        "astra_pages_is_estimate": astra_status.get("count_is_estimate"),
        "astra_count_source": astra_status.get("count_source"),
    }


@app.get("/api/pages/overview")
def api_pages_overview(
    request: Request,
    indexed_limit: Annotated[int, Query(ge=1, le=50)] = 20,
    queue_limit: Annotated[int, Query(ge=1, le=50)] = 20,
):
    conn = _conn_from_request(request)
    indexed_count = conn.execute("SELECT COUNT(*) AS c FROM pages").fetchone()["c"]
    queued_count = conn.execute("SELECT COUNT(*) AS c FROM crawl_queue").fetchone()["c"]

    indexed_rows = conn.execute(
        """
        SELECT url, title, content, fetched_at, status_code, content_type, language
        FROM pages
        ORDER BY datetime(COALESCE(fetched_at, '1970-01-01T00:00:00')) DESC, id DESC
        LIMIT ?
        """,
        (indexed_limit,),
    ).fetchall()
    queue_rows = conn.execute(
        """
        SELECT url, depth, discovered_at, last_error
        FROM crawl_queue
        ORDER BY datetime(COALESCE(discovered_at, '1970-01-01T00:00:00')) ASC, url ASC
        LIMIT ?
        """,
        (queue_limit,),
    ).fetchall()

    crawl_status = dict(getattr(request.app.state, "crawl_status", {}))
    pending_indexed_pages = [
        page
        for page in (crawl_status.get("pending_indexed_pages") or [])
        if isinstance(page, dict) and page.get("url")
    ]
    persisted_indexed_pages = [
        {
            "url": row["url"],
            "title": row["title"] or row["url"],
            "excerpt": _excerpt(row["content"]),
            "fetched_at": row["fetched_at"],
            "status_code": row["status_code"],
            "content_type": row["content_type"],
            "language": row["language"],
        }
        for row in indexed_rows
    ]

    pending_urls = list({str(page["url"]) for page in pending_indexed_pages})
    existing_pending_urls: set[str] = set()
    if pending_urls:
        placeholders = ", ".join("?" for _ in pending_urls)
        existing_pending_urls = {
            str(row["url"])
            for row in conn.execute(
                f"SELECT url FROM pages WHERE url IN ({placeholders})",
                pending_urls,
            ).fetchall()
        }

    indexed_pages: list[dict] = []
    indexed_seen_urls: set[str] = set()
    for item in pending_indexed_pages + persisted_indexed_pages:
        url = str(item["url"])
        if url in indexed_seen_urls:
            continue
        indexed_seen_urls.add(url)
        indexed_pages.append(item)
        if len(indexed_pages) >= indexed_limit:
            break

    current_scans = [
        scan
        for scan in (crawl_status.get("current_scans") or [])
        if isinstance(scan, dict) and scan.get("url")
    ]
    if not current_scans:
        current_url = crawl_status.get("current_url")
        if current_url:
            current_scans.append(
                {
                    "url": current_url,
                    "depth": crawl_status.get("current_depth"),
                    "state": crawl_status.get("state"),
                    "message": crawl_status.get("message"),
                    "updated_at": crawl_status.get("updated_at"),
                }
            )

    astra_status = _astra_runtime_status()
    astra_count = astra_status.get("document_count")

    visible_indexed_count = indexed_count + len(set(pending_urls) - existing_pending_urls)
    effective_indexed_count = int(visible_indexed_count)
    effective_count_is_estimate = False
    effective_count_source = "sqlite"
    if astra_count is not None and int(astra_count) > effective_indexed_count:
        effective_indexed_count = int(astra_count)
        effective_count_is_estimate = bool(astra_status.get("count_is_estimate"))
        effective_count_source = str(astra_status.get("count_source") or "astra")

    return {
        "summary": {
            "indexed_count": effective_indexed_count,
            "indexed_count_is_estimate": effective_count_is_estimate,
            "indexed_count_source": effective_count_source,
            "sqlite_indexed_count": indexed_count,
            "queued_count": queued_count,
            "active_scans": len(current_scans),
            "pending_indexed_count": len(set(pending_urls)),
        },
        "astra": astra_status,
        "crawler_status": crawl_status,
        "current_scans": current_scans,
        "indexed_pages": indexed_pages,
        "queued_pages": [
            {
                "url": row["url"],
                "depth": row["depth"],
                "discovered_at": row["discovered_at"],
                "last_error": row["last_error"],
            }
            for row in queue_rows
        ],
    }


@app.get("/api/pages/live-count")
def api_pages_live_count(request: Request):
    conn = _conn_from_request(request)
    indexed_count = conn.execute("SELECT COUNT(*) AS c FROM pages").fetchone()["c"]

    crawl_status = dict(getattr(request.app.state, "crawl_status", {}))
    pending_indexed_pages = [
        page
        for page in (crawl_status.get("pending_indexed_pages") or [])
        if isinstance(page, dict) and page.get("url")
    ]
    pending_urls = list({str(page["url"]) for page in pending_indexed_pages})
    existing_pending_urls: set[str] = set()
    if pending_urls:
        placeholders = ", ".join("?" for _ in pending_urls)
        existing_pending_urls = {
            str(row["url"])
            for row in conn.execute(
                f"SELECT url FROM pages WHERE url IN ({placeholders})",
                pending_urls,
            ).fetchall()
        }

    astra_status = _astra_count_snapshot(request, live=True, allow_estimate=False)
    astra_count = astra_status.get("document_count")

    visible_indexed_count = indexed_count + len(set(pending_urls) - existing_pending_urls)
    effective_indexed_count = int(visible_indexed_count)
    effective_count_is_estimate = False
    effective_count_source = "sqlite"
    if astra_count is not None and int(astra_count) > effective_indexed_count:
        effective_indexed_count = int(astra_count)
        effective_count_is_estimate = bool(astra_status.get("count_is_estimate"))
        effective_count_source = str(astra_status.get("count_source") or "astra")

    return {
        "summary": {
            "indexed_count": effective_indexed_count,
            "indexed_count_is_estimate": effective_count_is_estimate,
            "indexed_count_source": effective_count_source,
            "sqlite_indexed_count": indexed_count,
            "pending_indexed_count": len(set(pending_urls)),
        },
        "astra": astra_status,
    }


# Serve the existing static frontend from repo root.
app.mount("/", StaticFiles(directory=str(ROOT), html=True), name="static")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(str(ROOT / "coocle_logo.png"))


@app.get("/")
def root_index():
    return FileResponse(str(ROOT / "index.html"))
