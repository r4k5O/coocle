from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Iterable

import httpx
from bs4 import BeautifulSoup


MAILTRAP_BULK_BATCH_URL = "https://bulk.api.mailtrap.io/api/batch"
MAILTRAP_MAX_BATCH_SIZE = 500
EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,63}$", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _chunked(items: list[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size))
    return [items[start : start + size] for start in range(0, len(items), size)]


def normalize_email(value: str | None) -> str | None:
    candidate = str(value or "").strip().lower()
    if not candidate or not EMAIL_RE.fullmatch(candidate):
        return None
    return candidate


def normalize_name(value: str | None) -> str | None:
    candidate = WHITESPACE_RE.sub(" ", str(value or "").strip())
    if not candidate:
        return None
    return candidate[:120]


def subscription_timestamp() -> str:
    return _now_iso()


def newsletter_admin_token() -> str:
    return os.environ.get("COOCLE_NEWSLETTER_ADMIN_TOKEN", "").strip()


def mailtrap_api_token() -> str:
    return os.environ.get("MAILTRAP_API_TOKEN", "").strip()


def mailtrap_sender_email() -> str:
    return os.environ.get("MAILTRAP_SENDING_EMAIL", "").strip()


def mailtrap_sender_name() -> str:
    return os.environ.get("MAILTRAP_SENDING_NAME", "Coocle").strip() or "Coocle"


def mailtrap_newsletter_category() -> str:
    return os.environ.get("MAILTRAP_NEWSLETTER_CATEGORY", "newsletter").strip() or "newsletter"


def mailtrap_bulk_batch_url() -> str:
    return os.environ.get("MAILTRAP_NEWSLETTER_BATCH_URL", MAILTRAP_BULK_BATCH_URL).strip() or MAILTRAP_BULK_BATCH_URL


def mailtrap_newsletter_configured() -> bool:
    return bool(mailtrap_api_token() and mailtrap_sender_email())


def newsletter_sender() -> dict[str, str]:
    sender = {"email": mailtrap_sender_email()}
    name = mailtrap_sender_name()
    if name:
        sender["name"] = name
    return sender


def plain_text_from_html(html: str) -> str:
    soup = BeautifulSoup(str(html or ""), "lxml")
    text = soup.get_text("\n", strip=True)
    normalized = WHITESPACE_RE.sub(" ", text).strip()
    return normalized


async def send_newsletter(
    client: httpx.AsyncClient,
    recipients: Iterable[str],
    *,
    subject: str,
    html: str | None = None,
    text: str | None = None,
) -> dict[str, object]:
    cleaned_recipients = [email for email in (normalize_email(item) for item in recipients) if email]
    if not cleaned_recipients:
        raise ValueError("Es sind keine gueltigen Newsletter-Empfaenger vorhanden.")

    cleaned_subject = str(subject or "").strip()
    if not cleaned_subject:
        raise ValueError("Der Newsletter braucht einen Betreff.")

    html_body = str(html or "").strip()
    text_body = str(text or "").strip()
    if not html_body and not text_body:
        raise ValueError("Der Newsletter braucht HTML- oder Text-Inhalt.")
    if html_body and not text_body:
        text_body = plain_text_from_html(html_body)

    if not mailtrap_newsletter_configured():
        raise RuntimeError("Mailtrap fuer Newsletter ist nicht konfiguriert.")

    headers = {
        "Authorization": f"Bearer {mailtrap_api_token()}",
        "Api-Token": mailtrap_api_token(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    base_payload: dict[str, object] = {
        "from": newsletter_sender(),
        "subject": cleaned_subject,
        "category": mailtrap_newsletter_category(),
    }
    if text_body:
        base_payload["text"] = text_body
    if html_body:
        base_payload["html"] = html_body

    batches_sent = 0
    accepted = 0
    message_ids: list[str] = []
    errors: list[str] = []

    for batch in _chunked(cleaned_recipients, MAILTRAP_MAX_BATCH_SIZE):
        response = await client.post(
            mailtrap_bulk_batch_url(),
            headers=headers,
            json={
                "base": base_payload,
                "requests": [{"to": [{"email": email}]} for email in batch],
            },
            timeout=30.0,
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            details = exc.response.text.strip() if exc.response is not None else ""
            raise RuntimeError(
                f"Mailtrap Newsletter-Versand fehlgeschlagen ({response.status_code}). {details[:240]}".strip()
            ) from exc

        payload = response.json()
        batches_sent += 1

        batch_responses = payload.get("responses")
        if isinstance(batch_responses, list) and batch_responses:
            for item in batch_responses:
                item_ids = [str(value) for value in item.get("message_ids", []) if value]
                item_errors = [str(value) for value in item.get("errors", []) if value]
                if item.get("success") is False:
                    errors.extend(item_errors or ["Mailtrap hat mindestens einen Newsletter-Empfaenger abgelehnt."])
                    continue
                accepted += max(1, len(item_ids))
                message_ids.extend(item_ids)
        elif payload.get("success") is False:
            errors.extend([str(value) for value in payload.get("errors", []) if value] or ["Mailtrap hat den Batch abgelehnt."])
        else:
            accepted += len(batch)
            message_ids.extend([str(value) for value in payload.get("message_ids", []) if value])

    if errors:
        error_summary = "; ".join(errors[:3])
        raise RuntimeError(f"Mailtrap hat beim Newsletter-Versand Fehler gemeldet: {error_summary}")

    return {
        "sent": accepted,
        "batches": batches_sent,
        "message_ids": message_ids,
    }
