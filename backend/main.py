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
from typing import Annotated, Any
from urllib.parse import urlparse

import httpx
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env if it exists

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import db as dbmod
from . import direct_email as directemailmod
from . import github_stats as githubmod
from . import newsletter as newslettermod
from . import newsletter_templates as templatesmod
from . import astra_utils
from .crawler import CrawlConfig, crawl_loop
from .pages_service import build_pages_live_count_payload, build_pages_overview_payload, build_stats_payload
from .search import search as fts_search
from .search import vector_search as vec_search
from .summarize import OllamaChatConfig, SummaryResult, env_chat_config, summarize_results


ROOT = Path(__file__).resolve().parents[1]
FREE_SUMMARY_LIMIT = 10
logger = logging.getLogger(__name__)
SUMMARY_CONTEXT_LIMIT = 5


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


async def _read_json_object(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Ungueltiger JSON-Body.") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON-Objekt erwartet.")
    return payload


def _require_newsletter_admin_token(provided_token: str | None) -> None:
    expected_token = newslettermod.newsletter_admin_token()
    if not expected_token:
        raise HTTPException(status_code=503, detail="Newsletter-Versand ist nicht konfiguriert.")
    if str(provided_token or "").strip() != expected_token:
        raise HTTPException(status_code=401, detail="Newsletter-Admin-Token fehlt oder ist ungueltig.")


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
        astra_collection = await asyncio.to_thread(astra_utils.ensure_astra_collection)
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


async def _restore_newsletter_subscribers_on_start(conn) -> None:
    if dbmod.count_newsletter_subscribers(conn) > 0 or not astra_utils.has_astra_credentials():
        return

    try:
        meta_collection = await asyncio.to_thread(astra_utils.get_astra_meta_collection)
        if meta_collection is None:
            return

        subscribers = await asyncio.to_thread(astra_utils.load_newsletter_subscriber_documents, meta_collection)
        restored = 0
        for subscriber in subscribers:
            email = newslettermod.normalize_email(subscriber.get("email"))
            if not email:
                continue
            dbmod.upsert_newsletter_subscriber(
                conn,
                email=email,
                name=newslettermod.normalize_name(subscriber.get("name")),
                source_ip=str(subscriber.get("source_ip") or "") or None,
                subscribed_at=str(subscriber.get("subscribed_at") or "") or newslettermod.subscription_timestamp(),
            )
            restored += 1

        if restored:
            logger.info("Restored %d newsletter subscriber(s) from Astra metadata.", restored)
    except Exception:
        logger.exception("Newsletter subscriber restore from Astra failed; continuing")


async def _restore_pages_from_astra_on_start(conn) -> None:
    if not astra_utils.has_astra_credentials():
        return

    try:
        astra_collection = await asyncio.to_thread(astra_utils.ensure_astra_collection)
        if astra_collection is None:
            return

        logger.info("Attempting to restore pages from Astra...")
        restored = 0
        seen_urls = set()

        cursor = astra_collection.find({}, projection={"url": 1, "title": 1, "content": 1, "fetched_at": 1, "status_code": 1, "content_type": 1, "language": 1})
        for doc in cursor:
            url = doc.get("url")
            if not url:
                continue

            # Skip duplicates from AstraDB
            if url in seen_urls:
                continue
            seen_urls.add(url)

            title = doc.get("title", "")
            content = doc.get("content", "")
            fetched_at = doc.get("fetched_at")
            status_code = doc.get("status_code")
            content_type = doc.get("content_type")
            language = doc.get("language")

            conn.execute(
                """
                INSERT OR IGNORE INTO pages
                (url, title, content, fetched_at, status_code, content_type, language)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (url, title, content, fetched_at, status_code, content_type, language),
            )
            restored += 1

            if restored % 25 == 0:
                conn.commit()
                logger.info("Restored %d pages from Astra...", restored)
                # Add delay to reduce CPU usage
                await asyncio.sleep(0.5)

        conn.commit()

        if restored > 0:
            logger.info("Successfully restored %d pages from Astra to SQLite.", restored)
        else:
            logger.info("No pages found in Astra to restore.")
    except Exception:
        logger.exception("Page restore from Astra failed; continuing")


async def _restore_queue_from_astra_on_start(conn) -> None:
    if not astra_utils.has_astra_credentials():
        return

    try:
        meta_collection = await asyncio.to_thread(astra_utils.get_astra_meta_collection)
        if meta_collection is None:
            return

        logger.info("Attempting to restore queue from Astra...")
        restored = await asyncio.to_thread(
            astra_utils.load_crawl_queue_documents,
            meta_collection,
            page_size=100,
        )

        if restored:
            dbmod.upsert_queue(conn, restored, batch_size=100)
            conn.commit()
            logger.info("Successfully restored %d queued URLs from Astra to SQLite.", len(restored))
        else:
            logger.info("No queue entries found in Astra to restore.")
    except Exception:
        logger.exception("Queue restore from Astra failed; continuing")


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    astra_utils.reset_astra_cache()
    fastapi_app.state.conn = dbmod.connect(_db_path())
    dbmod.init_db(fastapi_app.state.conn)
    await _reset_datastores_on_start(fastapi_app.state.conn)
    await _restore_newsletter_subscribers_on_start(fastapi_app.state.conn)

    # Start background task to restore pages and queue from AstraDB
    fastapi_app.state.restore_task = None
    if astra_utils.has_astra_credentials() and _truthy_env("COOCLE_RESTORE_PAGES_FROM_ASTRA", default=False):
        async def restore_background():
            try:
                await _restore_pages_from_astra_on_start(fastapi_app.state.conn)
                await _restore_queue_from_astra_on_start(fastapi_app.state.conn)
            except Exception:
                logger.exception("Background restore from Astra failed")
        fastapi_app.state.restore_task = asyncio.create_task(restore_background())
    fastapi_app.state.rate_limiter = SlidingWindowRateLimiter()
    fastapi_app.state.summary_semaphore = asyncio.Semaphore(
        _int_env("COOCLE_SUMMARY_CONCURRENCY_LIMIT", 4)
    )
    fastapi_app.state.astra_count_cache = None
    fastapi_app.state.astra_live_count_cache = None
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

        if astra_utils.should_use_astra_runtime() and _truthy_env("COOCLE_PREWARM_ASTRA", default=False):
            try:
                await asyncio.to_thread(astra_utils.ensure_astra_collection)
            except Exception:
                logger.exception("Astra prewarm failed; continuing without prewarmed collection")

        # Wait for restore task to complete before starting crawler
        if fastapi_app.state.restore_task:
            try:
                await fastapi_app.state.restore_task
                logger.info("AstraDB restore completed, starting crawler...")
            except Exception:
                logger.exception("AstraDB restore failed, starting crawler anyway...")

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
        restore_task = getattr(fastapi_app.state, "restore_task", None)
        conn = getattr(fastapi_app.state, "conn", None)

        if stop_event:
            stop_event.set()
        if crawler_task:
            crawler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await crawler_task
        if restore_task:
            restore_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await restore_task
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


@app.post("/api/newsletter/subscribe")
async def api_newsletter_subscribe(
    request: Request,
    x_admin_token: Annotated[str | None, Header()] = None,
):
    _require_newsletter_admin_token(x_admin_token)

    conn = _conn_from_request(request)
    payload = await _read_json_object(request)
    email = newslettermod.normalize_email(payload.get("email"))
    if not email:
        raise HTTPException(status_code=400, detail="Bitte eine gueltige E-Mail-Adresse angeben.")

    name = newslettermod.normalize_name(payload.get("name"))
    subscribed_at = newslettermod.subscription_timestamp()
    created = dbmod.upsert_newsletter_subscriber(
        conn,
        email=email,
        name=name,
        source_ip=_request_ip(request),
        subscribed_at=subscribed_at,
    )
    if astra_utils.has_astra_credentials():
        try:
            meta_collection = await asyncio.to_thread(astra_utils.get_astra_meta_collection)
            if meta_collection is not None:
                await asyncio.to_thread(
                    astra_utils.upsert_newsletter_subscriber_document,
                    meta_collection,
                    email=email,
                    name=name,
                    source_ip=_request_ip(request),
                    subscribed_at=subscribed_at,
                )
        except Exception:
            logger.exception("Newsletter subscriber mirror to Astra failed; continuing")
    return {
        "ok": True,
        "created": created,
        "email": email,
        "subscriber_count": dbmod.count_newsletter_subscribers(conn),
        "message": (
            "Danke. Du bist jetzt fuer den Coocle-Newsletter eingetragen."
            if created
            else "Diese E-Mail-Adresse ist bereits fuer den Coocle-Newsletter eingetragen."
        ),
    }


@app.post("/api/newsletter/unsubscribe")
async def api_newsletter_unsubscribe(
    request: Request,
    x_admin_token: Annotated[str | None, Header()] = None,
):
    _require_newsletter_admin_token(x_admin_token)

    conn = _conn_from_request(request)
    payload = await _read_json_object(request)
    email = newslettermod.normalize_email(payload.get("email"))
    if not email:
        raise HTTPException(status_code=400, detail="Bitte eine gueltige E-Mail-Adresse angeben.")

    if not dbmod.newsletter_subscriber_exists(conn, email):
        return {
            "ok": True,
            "deleted": False,
            "email": email,
            "message": "Diese E-Mail-Adresse ist nicht fuer den Coocle-Newsletter eingetragen.",
        }

    deleted = dbmod.delete_newsletter_subscriber(conn, email)

    if astra_utils.has_astra_credentials():
        try:
            meta_collection = await asyncio.to_thread(astra_utils.get_astra_meta_collection)
            if meta_collection is not None:
                await asyncio.to_thread(
                    astra_utils.delete_newsletter_subscriber_document,
                    meta_collection,
                    email=email,
                )
        except Exception:
            logger.exception("Newsletter subscriber delete from Astra failed; continuing")

    return {
        "ok": True,
        "deleted": deleted,
        "email": email,
        "subscriber_count": dbmod.count_newsletter_subscribers(conn),
        "message": "Du hast dich erfolgreich vom Coocle-Newsletter abgemeldet."
        if deleted
        else "Abmeldung fehlgeschlagen.",
    }


@app.post("/api/newsletter/send")
async def api_newsletter_send(
    request: Request,
    x_admin_token: Annotated[str | None, Header()] = None,
):
    _require_newsletter_admin_token(x_admin_token)

    if not directemailmod.smtp_configured():
        raise HTTPException(status_code=503, detail="SMTP fuer Newsletter ist nicht konfiguriert.")

    payload = await _read_json_object(request)
    subject = str(payload.get("subject") or "").strip()
    html = str(payload.get("html") or "").strip()
    text = str(payload.get("text") or "").strip()
    conn = _conn_from_request(request)
    recipients = dbmod.list_newsletter_subscriber_emails(conn)
    if not recipients:
        raise HTTPException(status_code=400, detail="Es sind keine Newsletter-Abonnenten vorhanden.")

    try:
        send_result = await asyncio.to_thread(
            directemailmod.send_newsletter,
            recipients,
            subject=subject,
            html=html or None,
            text=text or None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {
        "ok": True,
        "subscriber_count": len(recipients),
        **send_result,
    }


@app.post("/api/newsletter/check-milestones")
async def api_newsletter_check_milestones(
    request: Request,
    x_admin_token: Annotated[str | None, Header()] = None,
):
    _require_newsletter_admin_token(x_admin_token)

    if not directemailmod.smtp_configured():
        raise HTTPException(status_code=503, detail="SMTP fuer Newsletter ist nicht konfiguriert.")

    conn = _conn_from_request(request)
    from datetime import datetime, timezone

    now_iso = datetime.now(timezone.utc).isoformat()

    stats = build_stats_payload(request, conn, db_path=str(_db_path()))
    page_count = int(stats.get("pages", 0))
    subscriber_count = dbmod.count_newsletter_subscribers(conn)

    last_page_milestone = dbmod.get_last_milestone(conn, "pages")
    last_subscriber_milestone = dbmod.get_last_milestone(conn, "subscribers")
    last_stars_milestone = dbmod.get_last_milestone(conn, "github_stars")
    last_forks_milestone = dbmod.get_last_milestone(conn, "github_forks")

    page_threshold = templatesmod.detect_page_milestone(page_count, last_page_milestone)
    subscriber_threshold = templatesmod.detect_subscriber_milestone(subscriber_count, last_subscriber_milestone)

    github_stats = None
    try:
        github_stats = await githubmod.fetch_github_stats()
    except Exception as exc:
        logger.warning(f"GitHub stats fetch failed: {exc}")

    star_threshold = None
    fork_threshold = None
    if github_stats:
        star_threshold = githubmod.detect_github_milestone(github_stats, "stars", last_stars_milestone)
        fork_threshold = githubmod.detect_github_milestone(github_stats, "forks", last_forks_milestone)

    sent_milestones = []

    if page_threshold:
        template = templatesmod.milestone_pages(page_threshold)
        try:
            result = await asyncio.to_thread(
                directemailmod.send_newsletter,
                dbmod.list_newsletter_subscriber_emails(conn),
                subject=template["subject"],
                html=template["html"],
                text=template["text"],
            )
            dbmod.record_milestone(conn, "pages", page_threshold, now_iso)
            sent_milestones.append({"kind": "pages", "value": page_threshold, **result})
        except Exception as exc:
            logger.exception("Failed to send page milestone newsletter")
            raise HTTPException(status_code=502, detail=f"Seiten-Milestone-Versand fehlgeschlagen: {exc}") from exc

    if subscriber_threshold:
        template = templatesmod.milestone_subscribers(subscriber_threshold)
        try:
            result = await asyncio.to_thread(
                directemailmod.send_newsletter,
                dbmod.list_newsletter_subscriber_emails(conn),
                subject=template["subject"],
                html=template["html"],
                text=template["text"],
            )
            dbmod.record_milestone(conn, "subscribers", subscriber_threshold, now_iso)
            sent_milestones.append({"kind": "subscribers", "value": subscriber_threshold, **result})
        except Exception as exc:
            logger.exception("Failed to send subscriber milestone newsletter")
            raise HTTPException(status_code=502, detail=f"Abonnenten-Milestone-Versand fehlgeschlagen: {exc}") from exc

    if star_threshold and github_stats:
        template = templatesmod.milestone_github_stars(star_threshold, github_stats["forks"], github_stats["open_prs"])
        try:
            result = await asyncio.to_thread(
                directemailmod.send_newsletter,
                dbmod.list_newsletter_subscriber_emails(conn),
                subject=template["subject"],
                html=template["html"],
                text=template["text"],
            )
            dbmod.record_milestone(conn, "github_stars", star_threshold, now_iso)
            sent_milestones.append({"kind": "github_stars", "value": star_threshold, **result})
        except Exception as exc:
            logger.exception("Failed to send GitHub stars milestone newsletter")
            raise HTTPException(status_code=502, detail=f"GitHub-Stars-Milestone-Versand fehlgeschlagen: {exc}") from exc

    if fork_threshold and github_stats:
        template = templatesmod.milestone_github_forks(fork_threshold, github_stats["stars"])
        try:
            result = await asyncio.to_thread(
                directemailmod.send_newsletter,
                dbmod.list_newsletter_subscriber_emails(conn),
                subject=template["subject"],
                html=template["html"],
                text=template["text"],
            )
            dbmod.record_milestone(conn, "github_forks", fork_threshold, now_iso)
            sent_milestones.append({"kind": "github_forks", "value": fork_threshold, **result})
        except Exception as exc:
            logger.exception("Failed to send GitHub forks milestone newsletter")
            raise HTTPException(status_code=502, detail=f"GitHub-Forks-Milestone-Versand fehlgeschlagen: {exc}") from exc

    return {
        "ok": True,
        "page_count": page_count,
        "subscriber_count": subscriber_count,
        "github_stats": github_stats,
        "sent_milestones": sent_milestones,
        "message": f"{len(sent_milestones)} Meilenstein-Newsletter(s) gesendet." if sent_milestones else "Keine neuen Meilensteine erreicht.",
    }


@app.get("/api/healthz")
def api_healthz(request: Request):
    return {
        "ok": True,
        "db_connected": bool(getattr(request.app.state, "conn", None)),
        "crawler_running": bool(getattr(request.app.state, "crawler_task", None)),
    }


@app.get("/api/stats")
def api_stats(request: Request):
    conn = _conn_from_request(request)
    return build_stats_payload(request, conn, db_path=str(_db_path()))


@app.get("/api/pages/overview")
def api_pages_overview(
    request: Request,
    indexed_limit: Annotated[int, Query(ge=1, le=100)] = 20,
    queue_limit: Annotated[int, Query(ge=1)] = 100,
):
    conn = _conn_from_request(request)
    return build_pages_overview_payload(
        request,
        conn,
        indexed_limit=indexed_limit,
        queue_limit=queue_limit,
    )


@app.get("/api/pages/live-count")
def api_pages_live_count(request: Request):
    conn = _conn_from_request(request)
    return build_pages_live_count_payload(request, conn)


@app.get("/api/github/stats")
async def api_github_stats(request: Request):
    try:
        stats = await githubmod.fetch_github_stats()
        return {"ok": True, "stats": stats}
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("GitHub stats fetch failed")
        raise HTTPException(status_code=502, detail=f"GitHub-API-Fehler: {exc}") from exc

@app.api_route("/favicon.ico", methods=["GET", "HEAD"], include_in_schema=False)
async def favicon():
    return FileResponse(str(ROOT / "coocle_logo.png"))


@app.get("/")
def root_index():
    return FileResponse(str(ROOT / "index.html"))


# Serve the existing static frontend from repo root.
app.mount("/", StaticFiles(directory=str(ROOT), html=True), name="static")
