from __future__ import annotations

import math
import os
import struct
from dataclasses import dataclass
from typing import Iterable, Sequence

import httpx


@dataclass(frozen=True)
class OllamaEmbedConfig:
    host: str = "http://localhost:11434"
    model: str = "nomic-embed-text"
    api_key: str | None = None
    timeout_s: float = 120.0


def _headers(api_key: str | None) -> dict[str, str]:
    if not api_key:
        return {}
    return {"Authorization": f"Bearer {api_key}"}


async def embed_text(client: httpx.AsyncClient, cfg: OllamaEmbedConfig, text: str) -> list[float]:
    # Ollama API (local or cloud-host) embeddings endpoint:
    # POST {host}/api/embed  { model, input, truncate?, dimensions? }
    r = await client.post(
        f"{cfg.host.rstrip('/')}/api/embed",
        headers=_headers(cfg.api_key),
        json={"model": cfg.model, "input": text, "truncate": True},
        timeout=cfg.timeout_s,
    )
    r.raise_for_status()
    data = r.json()
    emb = data.get("embeddings")
    if isinstance(emb, list) and emb and isinstance(emb[0], list):
        return [float(x) for x in emb[0]]
    if isinstance(emb, list) and all(isinstance(x, (int, float)) for x in emb):
        return [float(x) for x in emb]
    raise ValueError("Unexpected /api/embed response shape")


async def embed_batch(
    client: httpx.AsyncClient, cfg: OllamaEmbedConfig, inputs: Sequence[str]
) -> list[list[float]]:
    if not inputs:
        return []
    r = await client.post(
        f"{cfg.host.rstrip('/')}/api/embed",
        headers=_headers(cfg.api_key),
        json={"model": cfg.model, "input": list(inputs), "truncate": True},
        timeout=cfg.timeout_s,
    )
    r.raise_for_status()
    data = r.json()
    emb = data.get("embeddings")
    if isinstance(emb, list) and emb and isinstance(emb[0], list):
        return [[float(x) for x in row] for row in emb]
    raise ValueError("Unexpected /api/embed batch response shape")


def floats_to_blob(vec: Sequence[float]) -> bytes:
    # little-endian float32
    return struct.pack("<%sf" % len(vec), *[float(x) for x in vec])


def blob_to_floats(blob: bytes) -> list[float]:
    n = len(blob) // 4
    return list(struct.unpack("<%sf" % n, blob))


def l2_norm(vec: Iterable[float]) -> float:
    s = 0.0
    for x in vec:
        s += float(x) * float(x)
    return math.sqrt(s) if s > 0 else 0.0


def env_embed_config() -> OllamaEmbedConfig:
    host = os.environ.get("OLLAMA_CLOUD_HOST") or os.environ.get("OLLAMA_HOST") or os.environ.get("OLLAMA_BASE_URL") or "http://localhost:11434"
    model = os.environ.get("OLLAMA_CLOUD_EMBED_MODEL") or os.environ.get("OLLAMA_EMBED_MODEL") or "nomic-embed-text"
    api_key = os.environ.get("OLLAMA_CLOUD_API_KEY") or os.environ.get("OLLAMA_API_KEY") or None
    return OllamaEmbedConfig(host=host, model=model, api_key=api_key)

