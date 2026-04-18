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
ASTRA_META_COLLECTION_NAME = "coocle_internal"
ASTRA_RESET_MARKER_ID = "__coocle_reset_marker__"


def _chunked(items: list, chunk_size: int):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


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
    Connects to AstraDB and returns the collection object.
    Creates the collection with NVIDIA Vectorize if it doesn't exist.
    """
    try:
        from astrapy import DataAPIClient
        from astrapy.constants import VectorMetric
        from astrapy.info import (
            CollectionDefinition,
            CollectionVectorOptions,
            VectorServiceOptions,
        )

        collection_name = os.environ.get("ASTRA_DB_COLLECTION", "coocle_pages")
        database = get_astra_database()

        # check if collection exists
        col_list = list(database.list_collections())
        existing = [c.name for c in col_list]

        if collection_name in existing:
            return database.get_collection(collection_name)
        
        # Create with NVIDIA Vectorize
        # Note: Model must be supported by NVIDIA provider in Astra.
        # NV-Embed-QA is a standard high-quality choice.
        definition = CollectionDefinition(
            vector=CollectionVectorOptions(
                metric=VectorMetric.COSINE,
                service=VectorServiceOptions(
                    provider="nvidia",
                    model_name="nvidia/nv-embedqa-e5-v5",
                )
            )
        )
        
        return database.create_collection(
            collection_name,
            definition=definition
        )
    except Exception as e:
        logger.warning("AstraDB unavailable: %s", e)
        return None


def has_astra_credentials() -> bool:
    token = os.environ.get("ASTRA_DB_APPLICATION_TOKEN", "").strip()
    endpoint = os.environ.get("ASTRA_DB_API_ENDPOINT", "").strip()
    return bool(token and endpoint)


def is_astra_enabled() -> bool:
    return os.environ.get("USE_ASTRA", "false").lower() == "true"


def reset_astra_cache() -> None:
    get_astra_database.cache_clear()
    get_astra_meta_collection.cache_clear()
    get_astra_collection.cache_clear()


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


def live_document_count(collection) -> int | None:
    if collection is None:
        return None

    try:
        count = 0
        for _doc in collection.find({}, projection={"_id": True}):
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
