from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Iterable


logger = logging.getLogger(__name__)
ASTRA_WRITE_BATCH_SIZE = 100
ASTRA_DELETE_TIMEOUT_MS = 120_000
ASTRA_COUNT_TIMEOUT_MS = 30_000
ASTRA_EXACT_COUNT_TIMEOUT_MS = 10_000
ASTRA_EXACT_COUNT_UPPER_BOUND = 20_000
ASTRA_LIVE_COUNT_PAGE_SIZE = 1000
ASTRA_LIVE_COUNT_TIMEOUT_MS = 15_000
ASTRA_META_COLLECTION_NAME = "coocle_internal"
ASTRA_RESET_MARKER_ID = "__coocle_reset_marker__"
ASTRA_CRAWL_QUEUE_DOC_TYPE = "crawl_queue"
ASTRA_CRAWL_QUEUE_DOC_PREFIX = "__coocle_crawl_queue__:"
ASTRA_NEWSLETTER_SUBSCRIBER_DOC_TYPE = "newsletter_subscriber"
ASTRA_NEWSLETTER_SUBSCRIBER_DOC_PREFIX = "__coocle_newsletter__:"


def _chunked(items: list, chunk_size: int):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def _astra_collection_name() -> str:
    return os.environ.get("ASTRA_DB_COLLECTION", "coocle_pages")


def _astra_collection_definition():
    from astrapy.constants import VectorMetric
    from astrapy.info import (
        CollectionDefinition,
        CollectionVectorOptions,
        VectorServiceOptions,
    )

    return CollectionDefinition(
        vector=CollectionVectorOptions(
            metric=VectorMetric.COSINE,
            service=VectorServiceOptions(
                provider="nvidia",
                model_name="nvidia/nv-embedqa-e5-v5",
            ),
        )
    )


@lru_cache(maxsize=1)
def get_astra_database():
    from astrapy import DataAPIClient

    token = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
    endpoint = os.environ.get("ASTRA_DB_API_ENDPOINT")
    if not token or not endpoint:
        raise ValueError("Missing AstraDB credentials (ASTRA_DB_APPLICATION_TOKEN or ASTRA_DB_API_ENDPOINT)")

    client = DataAPIClient(token)
    return client.get_database(endpoint)


@lru_cache(maxsize=1)
def get_astra_meta_collection():
    try:
        collection_name = os.environ.get("ASTRA_DB_META_COLLECTION", ASTRA_META_COLLECTION_NAME)
        database = get_astra_database()
        existing = set(database.list_collection_names())
        if collection_name in existing:
            return database.get_collection(collection_name)
        return database.create_collection(collection_name)
    except Exception as e:
        logger.warning("AstraDB metadata unavailable: %s", e)
        return None


@lru_cache(maxsize=1)
def get_astra_collection():
    """
    Returns a fast collection handle for read-heavy paths.
    This avoids listing collections on the hot path.
    """
    try:
        database = get_astra_database()
        return database.get_collection(_astra_collection_name())
    except Exception as e:
        logger.warning("AstraDB unavailable: %s", e)
        return None


@lru_cache(maxsize=1)
def ensure_astra_collection():
    """
    Ensures that the Astra collection exists for write/reset paths.
    This is intentionally slower than get_astra_collection().
    """
    try:
        database = get_astra_database()
        collection_name = _astra_collection_name()

        existing = {c.name for c in database.list_collections()}
        if collection_name in existing:
            return database.get_collection(collection_name)

        return database.create_collection(
            collection_name,
            definition=_astra_collection_definition(),
        )
    except Exception as e:
        logger.warning("AstraDB collection ensure failed: %s", e)
        return None


def has_astra_credentials() -> bool:
    token = os.environ.get("ASTRA_DB_APPLICATION_TOKEN", "").strip()
    endpoint = os.environ.get("ASTRA_DB_API_ENDPOINT", "").strip()
    return bool(token and endpoint)


def is_astra_enabled() -> bool:
    return os.environ.get("USE_ASTRA", "false").lower() == "true"


def should_use_astra_runtime() -> bool:
    if is_astra_enabled():
        return True
    return os.environ.get("RENDER", "").strip().lower() == "true" and has_astra_credentials()


def reset_astra_cache() -> None:
    get_astra_database.cache_clear()
    get_astra_meta_collection.cache_clear()
    get_astra_collection.cache_clear()
    ensure_astra_collection.cache_clear()


def upsert_documents(collection, documents: Iterable[dict], *, batch_size: int = ASTRA_WRITE_BATCH_SIZE) -> int:
    docs = list(documents)
    if not docs:
        return 0

    upserted = 0
    for batch in _chunked(docs, batch_size):
        for doc in batch:
            try:
                collection.find_one_and_replace(
                    filter={"_id": doc["_id"]},
                    replacement=doc,
                    upsert=True,
                )
                upserted += 1
            except Exception:
                logger.debug("AstraDB insertion error for %s", doc.get("_id"), exc_info=True)
    return upserted


def clear_documents(collection, *, general_method_timeout_ms: int = ASTRA_DELETE_TIMEOUT_MS) -> int:
    if collection is None:
        return 0

    result = collection.delete_many({}, general_method_timeout_ms=general_method_timeout_ms)
    return int(getattr(result, "deleted_count", 0) or 0)


def get_document_by_id(collection, doc_id: str):
    if collection is None or not doc_id:
        return None
    try:
        return collection.find_one({"_id": doc_id})
    except Exception:
        logger.debug("AstraDB document lookup failed for %s", doc_id, exc_info=True)
        return None


def estimated_document_count(collection, *, general_method_timeout_ms: int = ASTRA_COUNT_TIMEOUT_MS) -> int | None:
    if collection is None:
        return None

    try:
        return int(
            collection.estimated_document_count(general_method_timeout_ms=general_method_timeout_ms)
        )
    except Exception:
        logger.debug("AstraDB estimated count failed", exc_info=True)
        return None


def exact_document_count(
    collection,
    *,
    upper_bound: int = ASTRA_EXACT_COUNT_UPPER_BOUND,
    general_method_timeout_ms: int = ASTRA_EXACT_COUNT_TIMEOUT_MS,
) -> int | None:
    if collection is None:
        return None

    try:
        return int(
            collection.count_documents(
                {},
                upper_bound=max(1, int(upper_bound)),
                general_method_timeout_ms=general_method_timeout_ms,
            )
        )
    except Exception:
        logger.debug("AstraDB exact count failed", exc_info=True)
        return None


def live_document_count(
    collection,
    *,
    page_size: int = ASTRA_LIVE_COUNT_PAGE_SIZE,
    request_timeout_ms: int = ASTRA_LIVE_COUNT_TIMEOUT_MS,
) -> int | None:
    if collection is None:
        return None

    try:
        count = 0
        next_page_state = None

        while True:
            find_kwargs: dict[str, object] = {
                "limit": max(1, int(page_size)),
                "request_timeout_ms": request_timeout_ms,
            }
            if next_page_state:
                find_kwargs["initial_page_state"] = next_page_state

            try:
                cursor = collection.find({}, **find_kwargs)
            except TypeError:
                find_kwargs.pop("request_timeout_ms", None)
                cursor = collection.find({}, **find_kwargs)

            page = cursor.fetch_next_page()
            results = list(getattr(page, "results", []) or [])
            count += len(results)
            next_page_state = getattr(page, "next_page_state", None)
            if not next_page_state:
                return count
    except Exception:
        logger.debug("AstraDB live scan count via page fetch failed", exc_info=True)

    try:
        count = 0
        find_kwargs = {
            "limit": max(1, int(page_size)),
            "request_timeout_ms": request_timeout_ms,
        }
        try:
            cursor = collection.find({}, **find_kwargs)
        except TypeError:
            find_kwargs.pop("request_timeout_ms", None)
            cursor = collection.find({}, **find_kwargs)

        for _doc in cursor:
            count += 1
        return count
    except Exception:
        logger.debug("AstraDB live scan count failed", exc_info=True)
        return None


def get_reset_marker(meta_collection):
    if meta_collection is None:
        return None
    return meta_collection.find_one({"_id": ASTRA_RESET_MARKER_ID})


def set_reset_marker(meta_collection, deploy_key: str) -> None:
    if meta_collection is None or not deploy_key:
        return
    meta_collection.find_one_and_replace(
        filter={"_id": ASTRA_RESET_MARKER_ID},
        replacement={"_id": ASTRA_RESET_MARKER_ID, "deploy_key": deploy_key},
        upsert=True,
    )


def _crawl_queue_doc_id(url: str) -> str:
    return f"{ASTRA_CRAWL_QUEUE_DOC_PREFIX}{url}"


def upsert_crawl_queue_documents(meta_collection, rows: Iterable[tuple[str, int, str]], *, batch_size: int = ASTRA_WRITE_BATCH_SIZE) -> int:
    if meta_collection is None:
        return 0

    documents: list[dict[str, object]] = []
    for url, depth, discovered_at in rows:
        normalized_url = str(url or "").strip()
        if not normalized_url:
            continue
        try:
            normalized_depth = max(0, int(depth))
        except (TypeError, ValueError):
            normalized_depth = 0
        documents.append(
            {
                "_id": _crawl_queue_doc_id(normalized_url),
                "doc_type": ASTRA_CRAWL_QUEUE_DOC_TYPE,
                "url": normalized_url,
                "depth": normalized_depth,
                "discovered_at": str(discovered_at or ""),
            }
        )

    return upsert_documents(meta_collection, documents, batch_size=batch_size)


def delete_crawl_queue_documents(meta_collection, urls: Iterable[str], *, batch_size: int = ASTRA_WRITE_BATCH_SIZE) -> int:
    if meta_collection is None:
        return 0

    deleted = 0
    rows = [str(url or "").strip() for url in urls if str(url or "").strip()]
    for batch in _chunked(rows, batch_size):
        for url in batch:
            try:
                result = meta_collection.delete_many({"_id": _crawl_queue_doc_id(url)})
                deleted += int(getattr(result, "deleted_count", 0) or 0)
            except Exception:
                logger.debug("AstraDB crawl queue delete failed for %s", url, exc_info=True)
    return deleted


def load_crawl_queue_documents(meta_collection, *, page_size: int = ASTRA_WRITE_BATCH_SIZE) -> list[tuple[str, int, str]]:
    if meta_collection is None:
        return []

    documents: list[dict] = []
    try:
        next_page_state = None
        while True:
            find_kwargs: dict[str, object] = {"limit": max(1, int(page_size))}
            if next_page_state:
                find_kwargs["initial_page_state"] = next_page_state

            cursor = meta_collection.find({"doc_type": ASTRA_CRAWL_QUEUE_DOC_TYPE}, **find_kwargs)
            page = cursor.fetch_next_page()
            documents.extend(list(getattr(page, "results", []) or []))
            next_page_state = getattr(page, "next_page_state", None)
            if not next_page_state:
                break
    except Exception:
        logger.debug("AstraDB crawl queue page fetch failed", exc_info=True)
        try:
            cursor = meta_collection.find({"doc_type": ASTRA_CRAWL_QUEUE_DOC_TYPE}, limit=max(1, int(page_size)))
            documents.extend(list(cursor))
        except Exception:
            logger.debug("AstraDB crawl queue load failed", exc_info=True)
            return []

    rows: list[tuple[str, int, str]] = []
    for document in documents:
        url = str(document.get("url") or "").strip()
        if not url:
            continue
        try:
            depth = max(0, int(document.get("depth") or 0))
        except (TypeError, ValueError):
            depth = 0
        rows.append((url, depth, str(document.get("discovered_at") or "")))

    rows.sort(key=lambda row: (row[2], row[0]))
    return rows


def _newsletter_subscriber_doc_id(email: str) -> str:
    return f"{ASTRA_NEWSLETTER_SUBSCRIBER_DOC_PREFIX}{email}"


def upsert_newsletter_subscriber_document(
    meta_collection,
    *,
    email: str,
    name: str | None,
    source_ip: str | None,
    subscribed_at: str,
) -> None:
    if meta_collection is None or not email:
        return
    meta_collection.find_one_and_replace(
        filter={"_id": _newsletter_subscriber_doc_id(email)},
        replacement={
            "_id": _newsletter_subscriber_doc_id(email),
            "doc_type": ASTRA_NEWSLETTER_SUBSCRIBER_DOC_TYPE,
            "email": email,
            "name": name,
            "source_ip": source_ip,
            "subscribed_at": subscribed_at,
        },
        upsert=True,
    )


def delete_newsletter_subscriber_document(meta_collection, *, email: str) -> None:
    if meta_collection is None or not email:
        return
    meta_collection.delete_one(filter={"_id": _newsletter_subscriber_doc_id(email)})


def load_newsletter_subscriber_documents(meta_collection, *, page_size: int = ASTRA_WRITE_BATCH_SIZE) -> list[dict[str, object]]:
    if meta_collection is None:
        return []

    documents: list[dict] = []
    try:
        next_page_state = None
        while True:
            find_kwargs: dict[str, object] = {"limit": max(1, int(page_size))}
            if next_page_state:
                find_kwargs["initial_page_state"] = next_page_state

            cursor = meta_collection.find({"doc_type": ASTRA_NEWSLETTER_SUBSCRIBER_DOC_TYPE}, **find_kwargs)
            page = cursor.fetch_next_page()
            documents.extend(list(getattr(page, "results", []) or []))
            next_page_state = getattr(page, "next_page_state", None)
            if not next_page_state:
                break
    except Exception:
        logger.debug("AstraDB newsletter subscriber page fetch failed", exc_info=True)
        try:
            cursor = meta_collection.find(
                {"doc_type": ASTRA_NEWSLETTER_SUBSCRIBER_DOC_TYPE},
                limit=max(1, int(page_size)),
            )
            documents.extend(list(cursor))
        except Exception:
            logger.debug("AstraDB newsletter subscriber load failed", exc_info=True)
            return []

    subscribers: list[dict[str, object]] = []
    for document in documents:
        email = str(document.get("email") or "").strip()
        if not email:
            continue
        subscribers.append(
            {
                "email": email,
                "name": document.get("name"),
                "source_ip": document.get("source_ip"),
                "subscribed_at": str(document.get("subscribed_at") or ""),
            }
        )

    subscribers.sort(key=lambda item: (str(item.get("subscribed_at") or ""), str(item.get("email") or "")))
    return subscribers
