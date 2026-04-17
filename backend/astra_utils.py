from __future__ import annotations

import logging
import os
from functools import lru_cache


logger = logging.getLogger(__name__)

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
