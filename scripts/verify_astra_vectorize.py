import os
import sys
from pathlib import Path

# Add the project root to sys.path so 'backend' can be imported
sys.path.append(str(Path(__file__).resolve().parents[1]))

from backend.astra_utils import get_astra_collection, is_astra_enabled
from dotenv import load_dotenv

load_dotenv()

def verify():
    print(f"AstraDB Enabled: {is_astra_enabled()}")
    if not is_astra_enabled():
        print("Set USE_ASTRA=true in your .env to test AstraDB.")
        return

    col = get_astra_collection()
    if not col:
        print("Failed to get AstraDB collection. Check your credentials in .env.")
        return

    print(f"Successfully connected to collection: {col.full_name}")
    
    # Test insertion with Vectorize
    print("Testing server-side Vectorize insertion...")
    try:
        doc = {
            "_id": "test_vectorize_url",
            "url": "https://example.com/test",
            "title": "AstraDB + NVIDIA Test",
            "content": "This is a test document to verify that AstraDB Vectorize with NVIDIA is working correctly in this project.",
            "$vectorize": "AstraDB + NVIDIA Test. This is a test document to verify that AstraDB Vectorize with NVIDIA is working correctly in this project."
        }
        col.find_one_and_replace(
            filter={"_id": doc["_id"]},
            replacement=doc,
            upsert=True
        )
        print("Insertion successful!")
        
        print("Testing vector search...")
        results = list(col.find(
            sort={"$vectorize": "NVIDIA test document"},
            limit=1,
            include_similarity=True
        ))
        
        if results:
            print(f"Search successful! Found: {results[0].get('title')}")
            print(f"Similarity: {results[0].get('$similarity')}")
        else:
            print("Search returned no results.")
            
    except Exception as e:
        print(f"Verification failed: {e}")

if __name__ == "__main__":
    verify()
