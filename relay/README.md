# SMTP HTTP Relay

Lightweight SMTP HTTP relay service for sending emails when direct SMTP is blocked (e.g., on Render.com).

## Purpose

Render.com blocks outbound SMTP connections. This relay service receives email requests via HTTP and forwards them to an SMTP server, allowing Coocle to send emails from Render.

## Deployment

### Requirements
```bash
pip install flask requests
```

### Run locally
```bash
python smtp_relay.py
```

### Deploy with Gunicorn (recommended)
```bash
gunicorn -w 2 -b 0.0.0.0:5000 smtp_relay:app
```

### Deploy to Netlify
1. Create a new Netlify site
2. Deploy as a Python function or use a VPS
3. Set environment variables (see below)

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RELAY_TOKEN` | Yes | - | Authentication token for relay requests |
| `SMTP_HOST` | Yes | mail.gmx.net | SMTP server hostname |
| `SMTP_PORT` | Yes | 587 | SMTP server port |
| `SMTP_USERNAME` | Yes | - | SMTP username |
| `SMTP_PASSWORD` | Yes | - | SMTP password |
| `SMTP_USE_TLS` | No | true | Enable TLS for SMTP |
| `SMTP_MAX_RETRIES` | No | 3 | Max retry attempts for SMTP connection |
| `SMTP_RETRY_DELAY` | No | 2 | Retry delay in seconds |

## Endpoints

### GET /health
Health check endpoint.

### POST /send
Send a single email.

**Request body:**
```json
{
  "to": ["recipient@example.com"],
  "subject": "Test Email",
  "text": "Plain text content",
  "html": "<h1>HTML content</h1>",
  "from": "sender@example.com",
  "from_name": "Sender Name"
}
```

**Headers:**
- `X-Relay-Token`: Your relay authentication token

### POST /send-batch
Send multiple emails in one request (newsletter use case).

**Request body:**
```json
{
  "recipients": ["email1@example.com", "email2@example.com"],
  "subject": "Newsletter",
  "text": "Plain text",
  "html": "<h1>HTML</h1>",
  "from": "sender@example.com",
  "from_name": "Sender"
}
```

### POST /send-batches
Alias for `/send-batch` endpoint.

## Coocle Configuration

To use this relay with Coocle, set these environment variables in Render:

```bash
SMTP_USE_RELAY=1
SMTP_RELAY_URL=https://your-relay-service.com
SMTP_RELAY_TOKEN=your-relay-token
```

## Features

- Retry logic with exponential backoff for network issues
- Batch email sending for newsletters
- Token-based authentication
- Health check endpoint
