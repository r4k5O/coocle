from __future__ import annotations

import httpx
import sqlite3
from typing import Any

from . import astra_utils
from .embeddings import OllamaEmbedConfig, blob_to_floats, embed_text, env_embed_config, l2_norm


def _prefer_astra_vector_search() -> bool:
    return astra_utils.should_use_astra_runtime()


def search(conn: sqlite3.Connection, q: str, limit: int = 10) -> list[dict[str, Any]]:
    q = (q or "").strip()
    if not q:
        return []

    # Basic FTS5 query with BM25 ranking.
    rows = conn.execute(
        """
        SELECT p.url, p.title, p.language,
               snippet(pages_fts, 2, '[', ']', '…', 12) AS snip,
               bm25(pages_fts, 1.0, 5.0, 1.0) AS rank
        FROM pages_fts
        JOIN pages p ON p.id = pages_fts.rowid
        WHERE pages_fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (q, limit),
    ).fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        title = r["title"] or r["url"]
        out.append(
            {
                "title": title,
                "url": r["url"],
                "snippet": r["snip"] or "",
                "language": r["language"],
                # Smaller rank is better for bm25() in SQLite; invert to a "higher is better" score-ish.
                "score": float(1.0 / (1.0 + max(float(r["rank"]), 0.0))),
            }
        )
    return out


async def vector_search(
    conn: sqlite3.Connection,
    q: str,
    limit: int = 10,
    embed_cfg: OllamaEmbedConfig | None = None,
) -> list[dict[str, Any]]:
    q = (q or "").strip()
    if not q:
        return []

    # AstraDB Vector Search
    if _prefer_astra_vector_search():
        astra_col = astra_utils.get_astra_collection()
        if astra_col:
            results = astra_col.find(
                sort={"$vectorize": q},
                limit=limit,
                include_similarity=True
            )
            out = []
            for doc in results:
                out.append({
                    "title": doc.get("title") or doc.get("url") or "Ohne Titel",
                    "url": doc.get("url") or "",
                    "snippet": doc.get("content")[:300] + "..." if doc.get("content") else "",
                    "language": doc.get("language"),
                    "score": doc.get("$similarity", 0.0)
                })
            return out

    embed_cfg = embed_cfg or env_embed_config()
    async with httpx.AsyncClient(follow_redirects=True) as client:
        q_vec = await embed_text(client, embed_cfg, q)

    q_norm = l2_norm(q_vec) or 1.0

    rows = conn.execute(
        """
        SELECT url, title, content, embedding, embedding_norm, language
        FROM pages
        WHERE embedding IS NOT NULL AND embedding_dim IS NOT NULL
        """
    ).fetchall()

    scored: list[tuple[float, sqlite3.Row]] = []
    for r in rows:
        blob = r["embedding"]
        if blob is None:
            continue
        d_vec = blob_to_floats(blob)
        d_norm = float(r["embedding_norm"] or l2_norm(d_vec) or 1.0)
        dot = 0.0
        for a, b in zip(q_vec, d_vec):
            dot += float(a) * float(b)
        sim = dot / (q_norm * d_norm) if d_norm > 0 else 0.0
        scored.append((sim, r))

    scored.sort(key=lambda t: t[0], reverse=True)
    top = scored[:limit]

    out: list[dict[str, Any]] = []
    for sim, r in top:
        title = r["title"] or r["url"]
        snippet = (r["content"] or "")[:240]
        out.append(
            {
                "title": title,
                "url": r["url"],
                "snippet": snippet,
                "score": float(sim),
                "language": r["language"],
            }
        )
    return out

