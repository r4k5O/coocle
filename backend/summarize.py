from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Literal, Sequence

import httpx
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)
_WS_RE = re.compile(r"\s+")
_SUMMARY_CONTEXT_LIMIT = 5
_WEBPAGE_READER_LIMIT = 3
_SNIPPET_MAX_CHARS = 420
_PAGE_EXCERPT_MAX_CHARS = 1800


@dataclass(frozen=True)
class OllamaChatConfig:
    host: str = "http://localhost:11434"
    model: str = "deepseek-r1:1.5b"
    api_key: str | None = None
    timeout_s: float = 60.0


@dataclass(frozen=True)
class SummaryResult:
    summary: str | None = None
    status: Literal["ok", "unavailable", "error", "credits_exhausted"] = "unavailable"
    message: str | None = None


def env_chat_config() -> OllamaChatConfig:
    host = os.environ.get("OLLAMA_CLOUD_HOST") or os.environ.get("OLLAMA_HOST") or os.environ.get("OLLAMA_BASE_URL") or "http://localhost:11434"
    model = os.environ.get("OLLAMA_CLOUD_MODEL") or os.environ.get("OLLAMA_CHAT_MODEL") or "deepseek-r1:1.5b"
    api_key = os.environ.get("OLLAMA_CLOUD_API_KEY") or os.environ.get("OLLAMA_API_KEY") or None
    return OllamaChatConfig(host=host, model=model, api_key=api_key)


def _headers(api_key: str | None) -> dict[str, str]:
    if not api_key:
        return {}
    return {"Authorization": f"Bearer {api_key}"}


def _normalize_chat_endpoint(host: str) -> tuple[str, Literal["ollama", "openai"]]:
    base = (host or "http://localhost:11434").strip().rstrip("/")
    lower = base.lower()

    if lower.endswith("/api/chat"):
        return base, "ollama"
    if lower.endswith("/chat/completions"):
        return base, "openai"
    if lower.endswith("/chat"):
        return base, "ollama"
    if lower.endswith("/api"):
        return f"{base}/chat", "ollama"
    if lower.endswith("/v1"):
        return f"{base}/chat/completions", "openai"
    if "/v1/" in lower:
        return base, "openai"
    return f"{base}/api/chat", "ollama"


def _build_chat_payload(prompt: str, cfg: OllamaChatConfig, api_style: Literal["ollama", "openai"]) -> dict[str, Any]:
    messages = [{"role": "user", "content": prompt}]
    if api_style == "openai":
        return {
            "model": cfg.model,
            "messages": messages,
            "temperature": 0.3,
            "max_tokens": 300,
        }
    return {
        "model": cfg.model,
        "messages": messages,
        "stream": False,
        "options": {
            "num_predict": 300,
            "temperature": 0.3,
        },
    }


def _extract_summary(data: dict[str, Any]) -> str:
    msg = data.get("message", {})
    if isinstance(msg, dict):
        content = msg.get("content", "")
        if isinstance(content, str) and content.strip():
            return content.strip()
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text = item.get("text")
                    if isinstance(text, str) and text.strip():
                        parts.append(text.strip())
            if parts:
                return "\n".join(parts)

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] or {}
        if isinstance(first, dict):
            message = first.get("message", {})
            if isinstance(message, dict):
                content = message.get("content", "")
                if isinstance(content, str) and content.strip():
                    return content.strip()
                if isinstance(content, list):
                    parts = []
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            text = item.get("text")
                            if isinstance(text, str) and text.strip():
                                parts.append(text.strip())
                    if parts:
                        return "\n".join(parts)

    if data.get("thinking"):
        return "Summary generation incomplete (hit token limit)."

    return ""


def _clean_summary(summary: str) -> str:
    text = (summary or "").strip()
    if text.lower().startswith("summary:"):
        text = text[len("summary:"):].strip()
    return text


def _collapse_whitespace(text: str) -> str:
    return _WS_RE.sub(" ", (text or "").strip()).strip()


def _truncate_text(text: str, max_chars: int) -> str:
    value = _collapse_whitespace(text)
    if not value or len(value) <= max_chars:
        return value

    head = value[: max(1, max_chars - 3)].rsplit(" ", 1)[0].strip()
    return f"{head or value[: max(1, max_chars - 3)].strip()}..."


def _extract_page_excerpt_from_result(result: dict[str, Any]) -> str:
    for key in ("page_content", "content", "content_preview"):
        value = result.get(key)
        if isinstance(value, str) and value.strip():
            return _truncate_text(value, _PAGE_EXCERPT_MAX_CHARS)
    return ""


def _extract_webpage_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()

    root = soup.select_one("main") or soup.select_one("article") or soup.body or soup
    return _collapse_whitespace(root.get_text(" ", strip=True))


async def _read_webpage_excerpt(client: httpx.AsyncClient, url: str) -> str:
    try:
        response = await client.get(
            url,
            follow_redirects=True,
            timeout=10.0,
            headers={
                "User-Agent": "CoocleSummaryBot/0.1",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        response.raise_for_status()
    except (httpx.HTTPError, httpx.InvalidURL):
        logger.debug("webpage_reader failed for %s", url, exc_info=True)
        return ""

    content_type = (response.headers.get("content-type") or "").lower()
    if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
        return ""

    return _truncate_text(_extract_webpage_text(response.text), _PAGE_EXCERPT_MAX_CHARS)


async def _build_result_context(client: httpx.AsyncClient, results: Sequence[dict[str, Any]]) -> str:
    prepared_results = [dict(result) for result in results[:_SUMMARY_CONTEXT_LIMIT]]
    page_excerpts = [""] * len(prepared_results)
    excerpt_sources = [""] * len(prepared_results)
    live_indexes: list[int] = []
    live_tasks: list[asyncio.Future[str] | asyncio.Task[str] | Any] = []

    for idx, result in enumerate(prepared_results):
        stored_excerpt = _extract_page_excerpt_from_result(result)
        if stored_excerpt:
            page_excerpts[idx] = stored_excerpt
            excerpt_sources[idx] = "stored_page_excerpt"
            continue

        url = str(result.get("url") or "").strip()
        if idx < _WEBPAGE_READER_LIMIT and url:
            live_indexes.append(idx)
            live_tasks.append(_read_webpage_excerpt(client, url))

    if live_tasks:
        live_results = await asyncio.gather(*live_tasks, return_exceptions=True)
        for idx, live_result in zip(live_indexes, live_results):
            if isinstance(live_result, str) and live_result:
                page_excerpts[idx] = live_result
                excerpt_sources[idx] = "live_webpage_reader"

    blocks = []
    for idx, result in enumerate(prepared_results):
        title = str(result.get("title") or "No Title").strip()
        snippet = _truncate_text(str(result.get("snippet") or ""), _SNIPPET_MAX_CHARS)
        url = str(result.get("url") or "").strip()

        lines = [f"Result {idx + 1}", f"Title: {title}"]
        if url:
            lines.append(f"URL: {url}")
        if snippet:
            lines.append(f"Search snippet: {snippet}")
        if page_excerpts[idx]:
            lines.append(f"Tool webpage_reader ({excerpt_sources[idx]}) output:")
            lines.append(page_excerpts[idx])
        blocks.append("\n".join(lines))

    return "\n\n".join(blocks)


async def summarize_results(
    client: httpx.AsyncClient,
    query: str,
    results: Sequence[dict[str, Any]],
    cfg: OllamaChatConfig | None = None,
) -> SummaryResult:
    if not results:
        return SummaryResult(
            status="unavailable",
            message="Keine Suchergebnisse zum Zusammenfassen.",
        )

    cfg = cfg or env_chat_config()

    context = await _build_result_context(client, results)
    prompt = (
        f"You are a search assistant. Based on the following search results and tool outputs for the query '{query}', "
        "provide a concise summary that answers the query. Prefer webpage_reader outputs when they add useful detail. "
        "If the sources are incomplete or disagree, mention that briefly. "
        "Format the answer in concise Markdown. Keep it under 3 short bullet points or 3 short sentences.\n\n"
        f"Search Results and Tool Output:\n{context}\n\n"
        "Summary:"
    )

    base_url, api_style = _normalize_chat_endpoint(cfg.host)

    try:
        r = await client.post(
            base_url,
            headers=_headers(cfg.api_key),
            json=_build_chat_payload(prompt, cfg, api_style),
            timeout=cfg.timeout_s,
        )
        r.raise_for_status()
        data = r.json()
        summary = _clean_summary(_extract_summary(data))
        if not summary:
            logger.warning("Empty summary response from %s", base_url)
            return SummaryResult(
                status="error",
                message="Zusammenfassung konnte nicht aus der KI-Antwort gelesen werden.",
            )
        return SummaryResult(summary=summary, status="ok")
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        logger.warning("AI summarization HTTP error %s from %s", status, base_url)
        if status in (401, 403):
            message = "Zusammenfassung fehlgeschlagen. API-Key oder Host bitte pruefen."
        elif status == 404:
            message = "Zusammenfassung fehlgeschlagen. Chat-Endpunkt wurde nicht gefunden."
        else:
            message = f"Zusammenfassung fehlgeschlagen (HTTP {status})."
        return SummaryResult(status="error", message=message)
    except httpx.RequestError:
        logger.warning("AI summarization host unreachable: %s", base_url)
        return SummaryResult(
            status="error",
            message="Zusammenfassung fehlgeschlagen. Ollama-Host ist nicht erreichbar.",
        )
    except ValueError:
        logger.exception("AI summarization returned invalid JSON")
        return SummaryResult(
            status="error",
            message="Zusammenfassung fehlgeschlagen. Die KI-Antwort war ungueltig.",
        )
    except Exception:
        logger.exception("Unexpected AI summarization error")
        return SummaryResult(
            status="error",
            message="Zusammenfassung konnte nicht erstellt werden.",
        )
