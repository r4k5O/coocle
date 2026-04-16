# Coocle

Coocle ist eine kleine lokale Suchmaschine mit eigenem Crawler, SQLite-Index, Volltextsuche, optionaler Vector Search und KI-Zusammenfassungen.

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

## Tests

```powershell
.\.venv\Scripts\python -m unittest discover -s tests -p "test_*.py" -v
node --check app.js
```

## Für GitHub

Dieses Repo ist jetzt so vorbereitet, dass lokale Daten und Geheimnisse nicht versehentlich mit committed werden:

- `.env` und lokale DBs sind per `.gitignore` ausgeschlossen
- CI läuft über `.github/workflows/ci.yml`
- `.env.example` enthält nur sichere Platzhalter

Vor einer Veröffentlichung solltest du trotzdem noch:

1. Einen passenden Lizenztyp wählen und `LICENSE` ergänzen.
2. Eventuell bereits verwendete lokale API-Keys rotieren, falls sie jemals geteilt wurden.

