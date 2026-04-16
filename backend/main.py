from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import httpx
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env if it exists

from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import FileResponse
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


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    fastapi_app.state.conn = dbmod.connect(_db_path())
    dbmod.init_db(fastapi_app.state.conn)
    fastapi_app.state.stop_event = asyncio.Event()
    fastapi_app.state.crawler_task = None

    try:
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


@app.get("/api/search")
async def api_search(
    request: Request,
    q: Annotated[str, Query(min_length=1, max_length=200)],
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    mode: Annotated[str, Query(pattern="^(fts|vector|hybrid)$")] = "fts",
    summarize: bool = False,
    x_ollama_key: Annotated[str | None, Header()] = None,
    x_ollama_host: Annotated[str | None, Header()] = None,
):
    conn = _conn_from_request(request)
    ip = _request_ip(request)
    day = datetime.now().strftime("%Y-%m-%d")

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
                    host=x_ollama_host or "https://ollama.com/api",
                    model=default_cfg.model,
                    api_key=x_ollama_key,
                    timeout_s=default_cfg.timeout_s,
                )

            async with httpx.AsyncClient() as client:
                summary_result = await summarize_results(client, q, results, cfg=chat_cfg)

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
    pages = conn.execute("SELECT COUNT(*) AS c FROM pages").fetchone()["c"]
    queued = conn.execute("SELECT COUNT(*) AS c FROM crawl_queue").fetchone()["c"]
    return {"pages": pages, "queued": queued, "db": str(_db_path())}


# Serve the existing static frontend from repo root.
app.mount("/", StaticFiles(directory=str(ROOT), html=True), name="static")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(str(ROOT / "coocle_logo.png"))


@app.get("/")
def root_index():
    return FileResponse(str(ROOT / "index.html"))

