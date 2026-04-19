# Coocle

Coocle ist eine kleine lokale Suchmaschine mit eigenem Crawler, SQLite-Index, Volltextsuche, optionaler Vector Search und KI-Zusammenfassungen.

## Status
[![CI](https://github.com/r4k5O/coocle/actions/workflows/ci.yml/badge.svg)](https://github.com/r4k5O/coocle/actions/workflows/ci.yml)

## Probiere die Demo!
Probier die Demo [hier](https://coocle-ctp8.onrender.com/).

## Stack

- Frontend: `index.html`, `styles.css`, `app.js`
- Backend: FastAPI in `backend/`
- Suche: SQLite FTS5 plus optionale Vector Search
- KI-Zusammenfassung: lokales Ollama oder kompatibler Cloud-Host

## Features

- lokale Suche mit eigener Datenbasis
- FTS-, Vector- und Hybrid-Suche
- eigener Crawler mit Queue und Persistenz
- KI-Zusammenfassungen mit Markdown-Ausgabe
- Newsletter-Signup mit Mailtrap-Bulk-Versand
- kostenlose Tagescredits pro IP für serverseitige Zusammenfassungen
- GitHub CI für Tests und Frontend-Syntaxcheck

## Schnellstart

### 1) Abhängigkeiten installieren

```powershell
cd "c:\Users\Oskar\Documents\Repositories\Ollama Search Engine"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2) Konfiguration anlegen

```powershell
Copy-Item .env.example .env
```

Standardmäßig nutzt Coocle:

- SQLite unter `data/coocle.db`
- lokales Ollama unter `http://localhost:11434`
- das Chat-Modell aus `OLLAMA_CHAT_MODEL`

### 3) Server starten

```powershell
uvicorn backend.main:app --reload --port 8000
```

Danach öffnen:

- `http://localhost:8000/`

## Crawling

Beispiel für einen separaten Crawler-Run:

```powershell
.\.venv\Scripts\python -m backend.crawl --seeds "https://example.com" --max-depth 2 --max-pages 200 --delay 0.6
```

Optional kann der Crawler direkt beim API-Start mitlaufen:

```powershell
$env:COOCLE_START_CRAWLER="1"
$env:COOCLE_SEEDS="https://example.com"
uvicorn backend.main:app --reload --port 8000
```

## KI-Zusammenfassung

Die UI kann Suchergebnisse per `✨ Zusammenfassen` verdichten.

- lokal über `OLLAMA_HOST`
- mit eigenem Key/Host über die UI-Einstellungen
- oder per API mit `X-Ollama-Key` und `X-Ollama-Host`
- die Zusammenfassung nutzt jetzt neben Snippets auch gespeicherte Seiteninhalte und bei Bedarf einen Live-`webpage_reader` fuer die Top-Ergebnisse

Bei `GET /api/search?q=<query>&summarize=true` liefert das Backend zusätzlich:

- `summary`
- `summary_status`
- `summary_message`
- `summary_format`

`summary_status` ist einer von:

- `ok`
- `unavailable`
- `credits_exhausted`
- `error`

## API-Schutz

Das Backend hat jetzt zusaetzliche Schutzmechanismen fuer API-Aufrufe:

- Sliding-Window Rate-Limits pro IP auf `/api/*`
- strengere Limits fuer teure `summarize=true`-Requests
- Begrenzung paralleler Summary-Jobs auf dem Server
- Validierung von benutzerdefinierten `X-Ollama-Host`-Werten, damit keine unsicheren internen Ziele angesprochen werden

Wichtige Umgebungsvariablen:

- `COOCLE_API_RATE_LIMIT` und `COOCLE_API_RATE_WINDOW_S`
- `COOCLE_SUMMARY_RATE_LIMIT` und `COOCLE_SUMMARY_RATE_WINDOW_S`
- `COOCLE_SUMMARY_CONCURRENCY_LIMIT`
- `COOCLE_ALLOW_PRIVATE_OLLAMA_HOSTS`

## Newsletter mit Mailtrap

Die Startseite hat jetzt ein Newsletter-Formular. Neue Eintraege werden lokal in SQLite gespeichert, und der eigentliche Versand laeuft ueber Mailtrap Bulk.

Wichtige Umgebungsvariablen:

- `COOCLE_NEWSLETTER_ADMIN_TOKEN`
- `MAILTRAP_API_TOKEN`
- `MAILTRAP_SENDING_EMAIL`
- `MAILTRAP_SENDING_NAME`
- optional `MAILTRAP_NEWSLETTER_CATEGORY`

Beispiel: Newsletter per API versenden

```powershell
$headers = @{
  "Content-Type" = "application/json"
  "X-Admin-Token" = $env:COOCLE_NEWSLETTER_ADMIN_TOKEN
}

$body = @{
  subject = "Coocle April Update"
  html = "<h1>Neue Features</h1><p>Mailtrap Bulk und Newsletter-Signup sind jetzt live.</p>"
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8000/api/newsletter/send" `
  -Headers $headers `
  -Body $body
```

Wichtig:

- Mailtrap erwartet fuer Newsletter den Bulk-Stream und eine verifizierte Sending-Domain.
- Die Subscribe-Route ist `POST /api/newsletter/subscribe`.
- Die Send-Route ist `POST /api/newsletter/send` und ist nur mit `X-Admin-Token` erreichbar.
- Wenn Astra-Metadaten verfuegbar sind, werden Newsletter-Abonnenten zusaetzlich dort gespiegelt und nach einem Deploy wieder in SQLite hergestellt.

## Tests

```powershell
.\.venv\Scripts\python -m unittest discover -s tests -p "test_*.py" -v
node --check app.js
node --test tests/app.test.js
```

## Render Deploy

`render.yaml` ist jetzt so konfiguriert, dass Render den Service erst deployed, wenn die GitHub-Checks auf `main` erfolgreich durchgelaufen sind.

Fuer stabilere Starts auf Render nutzt der Healthcheck jetzt die leichte Route `/api/healthz` statt `/api/stats`, und `COOCLE_PREWARM_ASTRA` ist dort deaktiviert. Dadurch blockieren Astra-Warmup und Count-Abfragen den Deploy-Healthcheck nicht mehr.

Die Render-Konfiguration setzt `COOCLE_RESET_DATA_ON_START=0`, damit neue Deploys den bestehenden Crawl-Stand nicht mehr loeschen.

Wichtig:

- Auf Render legt der Crawler seine Queue jetzt zusaetzlich in einer kleinen Astra-Meta-Collection ab.
- Wenn ein neuer Deploy mit einer frischen lokalen SQLite-Datei startet, wird diese Queue aus Astra wiederhergestellt und vom letzten bekannten Stand aus weiter abgearbeitet.
- Falls noch keine gespeicherte Queue existiert, startet der Crawler wie bisher mit den Seeds.

## Für GitHub

Dieses Repo ist jetzt so vorbereitet, dass lokale Daten und Geheimnisse nicht versehentlich mit committed werden:

- `.env` und lokale DBs sind per `.gitignore` ausgeschlossen
- CI läuft über `.github/workflows/ci.yml`
- `.env.example` enthält nur sichere Platzhalter
- Der Workflow ist in getrennte Jobs für Workflow-Validierung, Config-/Deploy-Checks, Backend-Tests, Frontend-Checks und einen App-Smoke-Test aufgeteilt

Vor einer Veröffentlichung solltest du trotzdem noch:

1. Einen passenden Lizenztyp wählen und `LICENSE` ergänzen.
2. Eventuell bereits verwendete lokale API-Keys rotieren, falls sie jemals geteilt wurden.
