from __future__ import annotations

import os
import re
import smtplib
import socket
import time
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,63}$", re.IGNORECASE)


def smtp_host() -> str:
    return os.environ.get("SMTP_HOST", "").strip()


def smtp_port() -> int:
    raw = os.environ.get("SMTP_PORT", "587").strip()
    try:
        return int(raw)
    except ValueError:
        return 587


def smtp_username() -> str:
    return os.environ.get("SMTP_USERNAME", "").strip()


def smtp_password() -> str:
    return os.environ.get("SMTP_PASSWORD", "").strip()


def smtp_use_tls() -> bool:
    val = os.environ.get("SMTP_USE_TLS", "true").strip().lower()
    return val in ("1", "true", "yes", "on")


def smtp_sender_email() -> str:
    return os.environ.get("SMTP_SENDER_EMAIL", "").strip() or smtp_username()


def smtp_recipient_email() -> str:
    return os.environ.get("SMTP_RECIPIENT_EMAIL", "").strip() or smtp_username()


def smtp_configured() -> bool:
    return bool(smtp_host() and smtp_username() and smtp_password())


def smtp_max_retries() -> int:
    raw = os.environ.get("SMTP_MAX_RETRIES", "3").strip()
    try:
        return int(raw)
    except ValueError:
        return 3


def smtp_retry_delay() -> float:
    raw = os.environ.get("SMTP_RETRY_DELAY_S", "2").strip()
    try:
        return float(raw)
    except ValueError:
        return 2.0


def normalize_email(value: str | None) -> str | None:
    candidate = str(value or "").strip().lower()
    if not candidate or not EMAIL_RE.fullmatch(candidate):
        return None
    return candidate


def _is_network_error(exc: Exception) -> bool:
    """Check if error is a network-related issue (blocked port, timeout, etc)"""
    return isinstance(exc, (
        socket.timeout,
        socket.gaierror,
        TimeoutError,
        ConnectionRefusedError,
        ConnectionResetError,
        OSError,
    ))


def _connect_with_retry(host: str, port: int, use_tls: bool, max_retries: int, retry_delay: float):
    """Connect to SMTP server with retry logic for network issues"""
    last_error = None

    for attempt in range(max_retries):
        try:
            if port == 465:
                server = smtplib.SMTP_SSL(host, port, timeout=30)
            else:
                server = smtplib.SMTP(host, port, timeout=30)
                server.ehlo()
                if use_tls:
                    server.starttls()
                    server.ehlo()

            logger.info(f"SMTP connection successful on attempt {attempt + 1}/{max_retries}")
            return server

        except Exception as exc:
            last_error = exc

            if not _is_network_error(exc):
                # Not a network error, don't retry
                raise

            logger.warning(f"SMTP connection attempt {attempt + 1}/{max_retries} failed: {exc}")

            if attempt < max_retries - 1:
                # Exponential backoff: delay * (2 ^ attempt)
                wait_time = retry_delay * (2 ** attempt)
                logger.info(f"Retrying SMTP connection in {wait_time}s...")
                time.sleep(wait_time)

    # All retries failed
    error_msg = str(last_error) if last_error else "Unknown error"
    raise RuntimeError(
        f"SMTP connection failed after {max_retries} attempts. "
        f"Last error: {error_msg}. "
        f"Note: Render.com blocks outbound SMTP. "
        f"See: https://render.com/docs/email"
    )


def send_email(
    *,
    from_name: str | None = None,
    reply_to: str | None = None,
    subject: str,
    body_text: str | None = None,
    body_html: str | None = None,
) -> dict[str, object]:
    if not smtp_configured():
        raise RuntimeError("SMTP ist nicht konfiguriert.")

    sender = smtp_sender_email()
    recipient = smtp_recipient_email()

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{sender}>" if from_name else sender
    msg["To"] = recipient
    if reply_to:
        msg["Reply-To"] = reply_to

    if body_text:
        msg.attach(MIMEText(body_text, "plain", "utf-8"))
    if body_html:
        msg.attach(MIMEText(body_html, "html", "utf-8"))

    if not body_text and not body_html:
        raise ValueError("E-Mail braucht Text- oder HTML-Inhalt.")

    port = smtp_port()
    use_tls = smtp_use_tls()
    max_retries = smtp_max_retries()
    retry_delay = smtp_retry_delay()

    try:
        server = _connect_with_retry(smtp_host(), port, use_tls, max_retries, retry_delay)
    except RuntimeError:
        raise

    try:
        server.login(smtp_username(), smtp_password())
        server.sendmail(sender, [recipient], msg.as_string())
    finally:
        try:
            server.quit()
        except Exception:
            pass

    return {"sent": True, "recipient": recipient}


def send_newsletter(
    recipients: list[str],
    *,
    subject: str,
    html: str | None = None,
    text: str | None = None,
) -> dict[str, object]:
    if not smtp_configured():
        raise RuntimeError("SMTP ist nicht konfiguriert.")

    cleaned = [e for e in (normalize_email(r) for r in recipients) if e]
    if not cleaned:
        raise ValueError("Es sind keine gueltigen Newsletter-Empfaenger vorhanden.")

    cleaned_subject = str(subject or "").strip()
    if not cleaned_subject:
        raise ValueError("Der Newsletter braucht einen Betreff.")

    html_body = str(html or "").strip()
    text_body = str(text or "").strip()
    if not html_body and not text_body:
        raise ValueError("Der Newsletter braucht HTML- oder Text-Inhalt.")

    sender = smtp_sender_email()
    from_name = os.environ.get("SMTP_SENDER_NAME", "Coocle").strip() or "Coocle"
    port = smtp_port()
    use_tls = smtp_use_tls()
    max_retries = smtp_max_retries()
    retry_delay = smtp_retry_delay()

    sent = 0
    errors: list[str] = []

    try:
        server = _connect_with_retry(smtp_host(), port, use_tls, max_retries, retry_delay)
    except RuntimeError as exc:
        raise RuntimeError(f"Newsletter-Versand fehlgeschlagen: {exc}") from exc

    try:
        server.login(smtp_username(), smtp_password())
        for email in cleaned:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = cleaned_subject
            msg["From"] = f"{from_name} <{sender}>"
            msg["To"] = email
            if text_body:
                msg.attach(MIMEText(text_body, "plain", "utf-8"))
            if html_body:
                msg.attach(MIMEText(html_body, "html", "utf-8"))
            try:
                server.sendmail(sender, [email], msg.as_string())
                sent += 1
            except smtplib.SMTPRecipientsRefused:
                errors.append(f"Empfaenger abgelehnt: {email}")
            except smtplib.SMTPException as exc:
                errors.append(f"Fehler fuer {email}: {exc}")
    finally:
        try:
            server.quit()
        except Exception:
            pass

    if errors and sent == 0:
        raise RuntimeError(f"Newsletter-Versand fehlgeschlagen: {'; '.join(errors[:3])}")

    return {"sent": sent, "batches": 1, "errors": errors}
