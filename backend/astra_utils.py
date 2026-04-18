from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Iterable


logger = logging.getLogger(__name__)
ASTRA_WRITE_BATCH_SIZE = 100
ASTRA_DELETE_TIMEOUT_MS = 120_000


def _chunked(items: list, chunk_size: int):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]

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

        token = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
        endpoint = os.environ.get("ASTRA_DB_API_ENDPOINT")
        collection_name = os.environ.get("ASTRA_DB_COLLECTION", "coocle_pages")

        if not token or not endpoint:
            raise ValueError("Missing AstraDB credentials (ASTRA_DB_APPLICATION_TOKEN or ASTRA_DB_API_ENDPOINT)")

        client = DataAPIClient(token)
        database = client.get_database(endpoint)

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

def is_astra_enabled() -> bool:
    return os.environ.get("USE_ASTRA", "false").lower() == "true"


def reset_astra_cache() -> None:
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
