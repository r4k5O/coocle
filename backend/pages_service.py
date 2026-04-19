from __future__ import annotations

import time

from fastapi import Request

from . import astra_utils

ASTRA_COUNT_CACHE_TTL_S = 30.0
ASTRA_LIVE_COUNT_CACHE_TTL_S = 60.0


def _excerpt(text: str | None, limit: int = 220) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= limit:
        return value
    return f"{value[: max(1, limit - 1)].rstrip()}…"


def _sqlite_count(conn, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
    return int(row["c"] if row else 0)


def _normalize_crawl_status(raw_status: object) -> dict[str, object]:
    if not isinstance(raw_status, dict):
        return {
            "state": "idle",
            "current_url": None,
            "current_depth": None,
            "current_scans": [],
            "message": "Crawlerstatus unbekannt",
            "pages_done": 0,
            "pages_saved": 0,
            "pending_indexed_pages": [],
            "pending_indexed_count": 0,
            "skipped": 0,
            "errors": 0,
            "updated_at": None,
        }

    crawl_status = dict(raw_status)
    if not isinstance(crawl_status.get("current_scans"), list):
        crawl_status["current_scans"] = []
    if not isinstance(crawl_status.get("pending_indexed_pages"), list):
        crawl_status["pending_indexed_pages"] = []
    return crawl_status


def _pending_indexed_pages(crawl_status: dict[str, object]) -> list[dict[str, object]]:
    pages: list[dict[str, object]] = []
    for item in crawl_status.get("pending_indexed_pages") or []:
        if not isinstance(item, dict) or not item.get("url"):
            continue
        pages.append(dict(item))
    return pages


def _pending_urls(pending_pages: list[dict[str, object]]) -> list[str]:
    return list({str(page["url"]) for page in pending_pages})


def _existing_page_urls(conn, urls: list[str]) -> set[str]:
    if not urls:
        return set()

    placeholders = ", ".join("?" for _ in urls)
    rows = conn.execute(
        f"SELECT url FROM pages WHERE url IN ({placeholders})",
        urls,
    ).fetchall()
    return {str(row["url"]) for row in rows}


def _persisted_indexed_pages(conn, limit: int) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT url, title, content, fetched_at, status_code, content_type, language
        FROM pages
        ORDER BY datetime(COALESCE(fetched_at, '1970-01-01T00:00:00')) DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        {
            "url": row["url"],
            "title": row["title"] or row["url"],
            "excerpt": _excerpt(row["content"]),
            "fetched_at": row["fetched_at"],
            "status_code": row["status_code"],
            "content_type": row["content_type"],
            "language": row["language"],
        }
        for row in rows
    ]


def _queued_pages(conn, limit: int) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT url, depth, discovered_at, last_error
        FROM crawl_queue
        ORDER BY datetime(COALESCE(discovered_at, '1970-01-01T00:00:00')) ASC, url ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        {
            "url": row["url"],
            "depth": row["depth"],
            "discovered_at": row["discovered_at"],
            "last_error": row["last_error"],
        }
        for row in rows
    ]


def _merged_indexed_pages(
    pending_pages: list[dict[str, object]],
    persisted_pages: list[dict[str, object]],
    limit: int,
) -> list[dict[str, object]]:
    merged: list[dict[str, object]] = []
    seen_urls: set[str] = set()

    for item in pending_pages + persisted_pages:
        url = str(item["url"])
        if url in seen_urls:
            continue
        seen_urls.add(url)
        merged.append(item)
        if len(merged) >= limit:
            break

    return merged


def _current_scans(crawl_status: dict[str, object]) -> list[dict[str, object]]:
    scans: list[dict[str, object]] = []
    for item in crawl_status.get("current_scans") or []:
        if not isinstance(item, dict) or not item.get("url"):
            continue
        scans.append(dict(item))

    if scans:
        return scans

    current_url = crawl_status.get("current_url")
    if not current_url:
        return []

    return [
        {
            "url": current_url,
            "depth": crawl_status.get("current_depth"),
            "state": crawl_status.get("state"),
            "message": crawl_status.get("message"),
            "updated_at": crawl_status.get("updated_at"),
        }
    ]


def _astra_collection_for_runtime():
    if not astra_utils.has_astra_credentials():
        return None
    return astra_utils.get_astra_collection()


def _base_astra_status(
    astra_collection,
    *,
    count_source: str = "unavailable",
    count_message: str | None = None,
) -> dict[str, object]:
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
        "count_source": count_source,
        "count_message": count_message,
        "count_is_live": False,
    }


def astra_runtime_status() -> dict[str, object]:
    astra_collection = _astra_collection_for_runtime()
    count_message = "Livezaehler wird separat geladen." if astra_collection else None
    return _base_astra_status(
        astra_collection,
        count_source="deferred" if astra_collection else "unavailable",
        count_message=count_message,
    )


def astra_count_snapshot(
    request: Request,
    *,
    live: bool = False,
    allow_estimate: bool = True,
) -> dict[str, object]:
    now = time.monotonic()
    cache_attr = "astra_live_count_cache" if live else "astra_count_cache"
    cached = getattr(request.app.state, cache_attr, None)
    if isinstance(cached, dict) and float(cached.get("expires_at", 0.0)) > now:
        snapshot = cached.get("snapshot")
        if isinstance(snapshot, dict):
            return dict(snapshot)

    astra_collection = _astra_collection_for_runtime()
    exact_count = None if live else astra_utils.exact_document_count(astra_collection)
    live_count = None
    estimate_count = None
    effective_count = exact_count
    count_is_estimate = False
    count_source = "astra_exact" if exact_count is not None else "unavailable"
    count_message = "Astra Exact Count verfuegbar." if exact_count is not None else None

    if effective_count is None and live:
        live_count = astra_utils.live_document_count(astra_collection)
        if live_count is not None:
            effective_count = live_count
            count_source = "astra_live_scan"
            count_message = "Astra Live-Scan erfolgreich."

    if effective_count is None and allow_estimate:
        estimate_count = astra_utils.estimated_document_count(astra_collection)
        if estimate_count is not None:
            effective_count = estimate_count
            count_is_estimate = True
            count_source = "astra_estimate"
            count_message = "Astra Schaetzung verfuegbar."

    if effective_count is None and astra_collection:
        if live:
            count_message = "Astra ist verbunden, aber der Livezaehler liefert aktuell keinen Wert."
        else:
            count_message = "Astra ist verbunden, aber es wurde noch kein Zaehlwert uebernommen."

    snapshot = {
        **_base_astra_status(
            astra_collection,
            count_source=count_source,
            count_message=count_message,
        ),
        "document_count": effective_count,
        "document_count_exact": exact_count,
        "document_count_live": live_count,
        "document_count_estimate": estimate_count,
        "count_is_estimate": count_is_estimate,
        "count_is_live": bool(effective_count is not None and not count_is_estimate),
    }

    setattr(
        request.app.state,
        cache_attr,
        {
            "expires_at": now + (ASTRA_LIVE_COUNT_CACHE_TTL_S if live else ASTRA_COUNT_CACHE_TTL_S),
            "snapshot": snapshot,
        },
    )

    return dict(snapshot)


def _indexed_count_summary(
    sqlite_indexed_count: int,
    pending_urls: list[str],
    existing_pending_urls: set[str],
    astra_status: dict[str, object],
) -> dict[str, object]:
    visible_indexed_count = int(sqlite_indexed_count) + len(set(pending_urls) - existing_pending_urls)
    effective_indexed_count = visible_indexed_count
    effective_count_is_estimate = False
    effective_count_source = "sqlite"
    astra_count = astra_status.get("document_count")

    if astra_count is not None and int(astra_count) > effective_indexed_count:
        effective_indexed_count = int(astra_count)
        effective_count_is_estimate = bool(astra_status.get("count_is_estimate"))
        effective_count_source = str(astra_status.get("count_source") or "astra")

    return {
        "indexed_count": effective_indexed_count,
        "indexed_count_is_estimate": effective_count_is_estimate,
        "indexed_count_source": effective_count_source,
        "sqlite_indexed_count": int(sqlite_indexed_count),
        "pending_indexed_count": len(set(pending_urls)),
    }


def build_stats_payload(request: Request, conn, *, db_path: str) -> dict[str, object]:
    sqlite_pages = _sqlite_count(conn, "pages")
    queued = _sqlite_count(conn, "crawl_queue")
    astra_status = astra_count_snapshot(request, live=False, allow_estimate=True)
    astra_count = astra_status.get("document_count")

    return {
        "pages": max(sqlite_pages, int(astra_count or 0)),
        "queued": queued,
        "db": db_path,
        "sqlite_pages": sqlite_pages,
        "astra_pages": astra_count,
        "astra_pages_exact": astra_status.get("document_count_exact"),
        "astra_pages_estimate": astra_status.get("document_count_estimate"),
        "astra_pages_is_estimate": astra_status.get("count_is_estimate"),
        "astra_count_source": astra_status.get("count_source"),
    }


def build_pages_overview_payload(
    request: Request,
    conn,
    *,
    indexed_limit: int,
    queue_limit: int,
) -> dict[str, object]:
    crawl_status = _normalize_crawl_status(getattr(request.app.state, "crawl_status", {}))
    pending_pages = _pending_indexed_pages(crawl_status)
    pending_urls = _pending_urls(pending_pages)
    existing_pending_urls = _existing_page_urls(conn, pending_urls)
    current_scans = _current_scans(crawl_status)
    astra_status = astra_runtime_status()
    summary = _indexed_count_summary(
        _sqlite_count(conn, "pages"),
        pending_urls,
        existing_pending_urls,
        astra_status,
    )
    summary["queued_count"] = _sqlite_count(conn, "crawl_queue")
    summary["active_scans"] = len(current_scans)

    return {
        "summary": summary,
        "astra": astra_status,
        "crawler_status": crawl_status,
        "current_scans": current_scans,
        "indexed_pages": _merged_indexed_pages(
            pending_pages,
            _persisted_indexed_pages(conn, indexed_limit),
            indexed_limit,
        ),
        "queued_pages": _queued_pages(conn, queue_limit),
    }


def build_pages_live_count_payload(request: Request, conn) -> dict[str, object]:
    crawl_status = _normalize_crawl_status(getattr(request.app.state, "crawl_status", {}))
    pending_pages = _pending_indexed_pages(crawl_status)
    pending_urls = _pending_urls(pending_pages)
    existing_pending_urls = _existing_page_urls(conn, pending_urls)
    astra_status = astra_count_snapshot(request, live=True, allow_estimate=False)

    return {
        "summary": _indexed_count_summary(
            _sqlite_count(conn, "pages"),
            pending_urls,
            existing_pending_urls,
            astra_status,
        ),
        "astra": astra_status,
    }
