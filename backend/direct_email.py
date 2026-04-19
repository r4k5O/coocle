from __future__ import annotations

import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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


def normalize_email(value: str | None) -> str | None:
    candidate = str(value or "").strip().lower()
    if not candidate or not EMAIL_RE.fullmatch(candidate):
        return None
    return candidate


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

    if port == 465:
        with smtplib.SMTP_SSL(smtp_host(), port, timeout=30) as server:
            server.login(smtp_username(), smtp_password())
            server.sendmail(sender, [recipient], msg.as_string())
    else:
        with smtplib.SMTP(smtp_host(), port, timeout=30) as server:
            server.ehlo()
            if use_tls:
                server.starttls()
                server.ehlo()
            server.login(smtp_username(), smtp_password())
            server.sendmail(sender, [recipient], msg.as_string())

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
    sent = 0
    errors: list[str] = []

    if port == 465:
        server = smtplib.SMTP_SSL(smtp_host(), port, timeout=30)
    else:
        server = smtplib.SMTP(smtp_host(), port, timeout=30)
        server.ehlo()
        if use_tls:
            server.starttls()
            server.ehlo()

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
