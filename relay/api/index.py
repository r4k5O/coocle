import os
import smtplib
import socket
import time
import logging
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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


def handler(request):
    """Vercel serverless function handler"""
    logger.info(f"Request received: method={request.method}, url={request.url}")
    
    # Handle GET requests
    if request.method == "GET":
        path = request.path
        if path == "/health" or path == "/api/health":
            return {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"status": "ok", "relay": "smtp-http"})
            }
        return {
            "status": 404,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Not found"})
        }
    
    # Handle POST requests
    if request.method == "POST":
        # Check authentication
        token = request.headers.get("X-Relay-Token", "")
        if not RELAY_TOKEN or token != RELAY_TOKEN:
            logger.warning("Unauthorized relay request")
            return {
                "status": 401,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Unauthorized"})
            }
        
        try:
            body = request.body
            data = json.loads(body) if body else {}
            path = request.path
            
            # Send endpoint
            if path in ("/send", "/api/send"):
                recipients = data.get("to", [])
                subject = data.get("subject", "")
                text_body = data.get("text", "")
                html_body = data.get("html", "")
                from_addr = data.get("from", SMTP_USERNAME)
                from_name = data.get("from_name", "")
                
                if not recipients or not isinstance(recipients, list):
                    return {
                        "status": 400,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({"error": 'Invalid or missing "to" field'})
                    }
                if not subject:
                    return {
                        "status": 400,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({"error": 'Missing "subject"'})
                    }
                if not text_body and not html_body:
                    return {
                        "status": 400,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({"error": 'Either "text" or "html" required'})
                    }
                
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = f"{from_name} <{from_addr}>" if from_name else from_addr
                msg["To"] = ", ".join(recipients)
                
                if text_body:
                    msg.attach(MIMEText(text_body, "plain", "utf-8"))
                if html_body:
                    msg.attach(MIMEText(html_body, "html", "utf-8"))
                
                try:
                    server = _connect_with_retry()
                    server.login(SMTP_USERNAME, SMTP_PASSWORD)
                    server.sendmail(from_addr, recipients, msg.as_string())
                    logger.info(f"Email sent to {recipients} via {SMTP_HOST}")
                    return {
                        "status": 200,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({
                            "ok": True,
                            "sent": 1,
                            "recipients": recipients,
                        })
                    }
                finally:
                    try:
                        server.quit()
                    except Exception:
                        pass
            
            # Send-batch endpoint
            elif path in ("/send-batch", "/send-batches", "/api/send-batch", "/api/send-batches"):
                recipients = data.get("recipients", [])
                subject = data.get("subject", "")
                text_body = data.get("text", "")
                html_body = data.get("html", "")
                from_addr = data.get("from", SMTP_USERNAME)
                from_name = data.get("from_name", "")
                
                if not recipients or not isinstance(recipients, list):
                    return {
                        "status": 400,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({"error": 'Invalid or missing "recipients" field'})
                    }
                if not subject:
                    return {
                        "status": 400,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({"error": 'Missing "subject"'})
                    }
                if not text_body and not html_body:
                    return {
                        "status": 400,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({"error": 'Either "text" or "html" required'})
                    }
                
                sent = 0
                errors = []
                
                try:
                    server = _connect_with_retry()
                    server.login(SMTP_USERNAME, SMTP_PASSWORD)
                    
                    for recipient in recipients:
                        try:
                            msg = MIMEMultipart("alternative")
                            msg["Subject"] = subject
                            msg["From"] = f"{from_name} <{from_addr}>" if from_name else from_addr
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
                    return {
                        "status": 200,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({
                            "ok": True,
                            "sent": sent,
                            "total": len(recipients),
                            "errors": errors if errors else None,
                        })
                    }
                finally:
                    try:
                        server.quit()
                    except Exception:
                        pass
            
            else:
                return {
                    "status": 404,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": "Not found"})
                }
        
        except smtplib.SMTPException as exc:
            logger.error(f"SMTP error: {exc}")
            return {
                "status": 502,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": f"SMTP error: {exc}"})
            }
        except Exception as exc:
            logger.error(f"Relay error: {exc}")
            return {
                "status": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": str(exc)})
            }
    
    return {
        "status": 405,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": "Method not allowed"})
    }
