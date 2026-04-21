"""
Lightweight SMTP HTTP Relay
Receives email requests via HTTP and forwards them to an SMTP server.
Designed to run on a VPS with open SMTP ports (e.g., DigitalOcean, Hetzner).

Deployment:
  pip install flask requests
  gunicorn -w 2 -b 0.0.0.0:5000 smtp_relay:app
"""

import os
import smtplib
import socket
import time
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import Flask, request, jsonify
from functools import wraps

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment
RELAY_TOKEN = os.environ.get("RELAY_TOKEN", "")
SMTP_HOST = os.environ.get("SMTP_HOST", "mail.gmx.net")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "true").lower() in ("1", "true", "yes", "on")
SMTP_MAX_RETRIES = int(os.environ.get("SMTP_MAX_RETRIES", "3"))
SMTP_RETRY_DELAY = float(os.environ.get("SMTP_RETRY_DELAY", "2"))


def require_token(f):
    """Decorator to require valid API token"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get("X-Relay-Token", "")
        if not RELAY_TOKEN or token != RELAY_TOKEN:
            logger.warning(f"Unauthorized relay request from {request.remote_addr}")
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function


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


def _connect_with_retry():
    """Connect to SMTP server with retry logic for network issues"""
    last_error = None
    
    for attempt in range(SMTP_MAX_RETRIES):
        try:
            if SMTP_PORT == 465:
                server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30)
            else:
                server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
                server.ehlo()
                if SMTP_USE_TLS:
                    server.starttls()
                    server.ehlo()
            
            logger.info(f"SMTP connection successful on attempt {attempt + 1}/{SMTP_MAX_RETRIES}")
            return server
        
        except Exception as exc:
            last_error = exc
            
            if not _is_network_error(exc):
                # Not a network error, don't retry
                raise
            
            logger.warning(f"SMTP connection attempt {attempt + 1}/{SMTP_MAX_RETRIES} failed: {exc}")
            
            if attempt < SMTP_MAX_RETRIES - 1:
                # Exponential backoff: delay * (2 ^ attempt)
                wait_time = SMTP_RETRY_DELAY * (2 ** attempt)
                logger.info(f"Retrying SMTP connection in {wait_time}s...")
                time.sleep(wait_time)
    
    # All retries failed
    error_msg = str(last_error) if last_error else "Unknown error"
    raise RuntimeError(
        f"SMTP connection failed after {SMTP_MAX_RETRIES} attempts. "
        f"Last error: {error_msg}"
    )


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok", "relay": "smtp-http"}), 200


@app.route("/send", methods=["POST"])
@require_token
def send_email():
    """
    Send an email via SMTP relay.
    
    Request body:
    {
        "to": ["recipient@example.com"],
        "subject": "Test Email",
        "text": "Plain text content",
        "html": "<h1>HTML content</h1>",
        "from": "sender@example.com",
        "from_name": "Sender Name"
    }
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data:
            return jsonify({"error": "No JSON body"}), 400
        
        recipients = data.get("to", [])
        subject = data.get("subject", "")
        text_body = data.get("text", "")
        html_body = data.get("html", "")
        from_addr = data.get("from", SMTP_USERNAME)
        from_name = data.get("from_name", "")
        
        if not recipients or not isinstance(recipients, list):
            return jsonify({"error": "Invalid or missing 'to' field"}), 400
        if not subject:
            return jsonify({"error": "Missing 'subject'"}), 400
        if not text_body and not html_body:
            return jsonify({"error": "Either 'text' or 'html' required"}), 400
        
        # Build MIME message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_addr}}" if from_name else from_addr
        msg["To"] = ", ".join(recipients)
        
        if text_body:
            msg.attach(MIMEText(text_body, "plain", "utf-8"))
        if html_body:
            msg.attach(MIMEText(html_body, "html", "utf-8"))
        
        # Send via SMTP with retry logic
        try:
            server = _connect_with_retry()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(from_addr, recipients, msg.as_string())
            logger.info(f"Email sent to {recipients} via {SMTP_HOST}")
            return jsonify({
                "ok": True,
                "sent": 1,
                "recipients": recipients,
            }), 200
        finally:
            try:
                server.quit()
            except Exception:
                pass
    
    except smtplib.SMTPException as exc:
        logger.error(f"SMTP error: {exc}")
        return jsonify({"error": f"SMTP error: {exc}"}), 502
    except Exception as exc:
        logger.error(f"Relay error: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route("/send-batch", methods=["POST"])
@require_token
def send_batch():
    """
    Send multiple emails in one request (newsletter use case).
    
    Request body:
    {
        "recipients": ["email1@example.com", "email2@example.com"],
        "subject": "Newsletter",
        "text": "Plain text",
        "html": "<h1>HTML</h1>",
        "from": "sender@example.com",
        "from_name": "Sender"
    }
    """
    return _send_batch_impl()


@app.route("/send-batches", methods=["POST"])
@require_token
def send_batches():
    """Alias for /send-batch endpoint"""
    return _send_batch_impl()


def _send_batch_impl():
    """
    Send multiple emails in one request (newsletter use case).
    
    Request body:
    {
        "recipients": ["email1@example.com", "email2@example.com"],
        "subject": "Newsletter",
        "text": "Plain text",
        "html": "<h1>HTML</h1>",
        "from": "sender@example.com",
        "from_name": "Sender"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON body"}), 400
        
        recipients = data.get("recipients", [])
        subject = data.get("subject", "")
        text_body = data.get("text", "")
        html_body = data.get("html", "")
        from_addr = data.get("from", SMTP_USERNAME)
        from_name = data.get("from_name", "")
        
        if not recipients or not isinstance(recipients, list):
            return jsonify({"error": "Invalid or missing 'recipients' field"}), 400
        if not subject:
            return jsonify({"error": "Missing 'subject'"}), 400
        if not text_body and not html_body:
            return jsonify({"error": "Either 'text' or 'html' required"}), 400
        
        # Connect once, send to all recipients with retry logic
        sent = 0
        errors = []
        
        try:
            server = _connect_with_retry()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            
            for recipient in recipients:
                try:
                    msg = MIMEMultipart("alternative")
                    msg["Subject"] = subject
                    msg["From"] = f"{from_name} <{from_addr}}" if from_name else from_addr
                    msg["To"] = recipient
                    
                    if text_body:
                        msg.attach(MIMEText(text_body, "plain", "utf-8"))
                    if html_body:
                        msg.attach(MIMEText(html_body, "html", "utf-8"))
                    
                    server.sendmail(from_addr, [recipient], msg.as_string())
                    sent += 1
                except smtplib.SMTPRecipientsRefused as e:
                    errors.append(f"{recipient}: Rejected by server")
                except smtplib.SMTPException as e:
                    errors.append(f"{recipient}: {str(e)}")
            
            logger.info(f"Batch send: {sent}/{len(recipients)} sent successfully")
            return jsonify({
                "ok": True,
                "sent": sent,
                "total": len(recipients),
                "errors": errors if errors else None,
            }), 200
        finally:
            try:
                server.quit()
            except Exception:
                pass
    
    except Exception as exc:
        logger.error(f"Batch relay error: {exc}")
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
