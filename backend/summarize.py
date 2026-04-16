from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Literal, Sequence

import httpx


logger = logging.getLogger(__name__)


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

    # Construct a prompt from the snippets
    snippets = []
    for i, r in enumerate(results[:5]):  # Use top 5 results
        title = r.get("title", "No Title")
        snip = r.get("snippet", "")
        snippets.append(f"Result {i+1} [{title}]: {snip}")

    context = "\n".join(snippets)
    prompt = (
        f"You are a search assistant. Based on the following search results for the query '{query}', "
        "provide a concise summary that answers the query. Focus on the most relevant information. "
        "Format the answer in concise Markdown. Keep it under 3 short bullet points or 3 short sentences.\n\n"
        f"Search Results:\n{context}\n\n"
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
