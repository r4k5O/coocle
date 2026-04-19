# Coocle

Coocle ist eine kleine lokale Suchmaschine mit eigenem Crawler, SQLite-Index, Volltextsuche, optionaler Vector Search und KI-Zusammenfassungen.

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

## Tests

```powershell
.\.venv\Scripts\python -m unittest discover -s tests -p "test_*.py" -v
node --check app.js
node --test tests/app.test.js
```

## Render Deploy

`render.yaml` ist jetzt so konfiguriert, dass Render den Service erst deployed, wenn die GitHub-Checks auf `main` erfolgreich durchgelaufen sind.

Fuer stabilere Starts auf Render nutzt der Healthcheck jetzt die leichte Route `/api/healthz` statt `/api/stats`, und `COOCLE_PREWARM_ASTRA` ist dort deaktiviert. Dadurch blockieren Astra-Warmup und Count-Abfragen den Deploy-Healthcheck nicht mehr.

Zusätzlich setzt die Render-Konfiguration `COOCLE_RESET_DATA_ON_START=1`. Dadurch wird beim Start des neuen Service-Prozesses die SQLite-Datenbank geleert und, sobald Astra-Credentials vorhanden sind, auch die AstraDB-Collection komplett geleert.

Wichtig:

- Das ist absichtlich destruktiv und eignet sich nur fuer Demo- oder Test-Deployments.
- Auf Render wird der Reset jetzt nur einmal pro Deploy ausgefuehrt. Als Marker dient `RENDER_GIT_COMMIT`, gespeichert in einer kleinen Astra-Meta-Collection.
- Auf Render ist `COOCLE_RESET_DATA_STRICT=0` gesetzt, damit ein voruebergehender Astra- oder Reset-Fehler nicht den kompletten Backend-Start blockiert.

## Für GitHub

Dieses Repo ist jetzt so vorbereitet, dass lokale Daten und Geheimnisse nicht versehentlich mit committed werden:

- `.env` und lokale DBs sind per `.gitignore` ausgeschlossen
- CI läuft über `.github/workflows/ci.yml`
- `.env.example` enthält nur sichere Platzhalter
- Der Workflow ist in getrennte Jobs für Workflow-Validierung, Config-/Deploy-Checks, Backend-Tests, Frontend-Checks und einen App-Smoke-Test aufgeteilt

Vor einer Veröffentlichung solltest du trotzdem noch:

1. Einen passenden Lizenztyp wählen und `LICENSE` ergänzen.
2. Eventuell bereits verwendete lokale API-Keys rotieren, falls sie jemals geteilt wurden.
