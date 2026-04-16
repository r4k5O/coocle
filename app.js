const API_BASE = ""; // e.g. "http://localhost:3000"
const SEARCH_PATH = "/api/search"; // expects GET ?q=...

const topbar = document.getElementById("topbar");
const home = document.getElementById("home");
const resultsSection = document.getElementById("results");

const form = document.getElementById("searchForm");
const input = document.getElementById("q");
const btn = document.getElementById("searchBtn");
const luckyBtn = document.getElementById("luckyBtn");
const clearBtn = document.getElementById("clearBtn");

const formTop = document.getElementById("searchFormTop");
const inputTop = document.getElementById("qTop");
const btnTop = document.getElementById("searchBtnTop");

const statusEl = document.getElementById("status");
const apiLabel = document.getElementById("apiLabel");
const resultCount = document.getElementById("resultCount");
const liveToggle = document.getElementById("liveToggle");
const emptyState = document.getElementById("emptyState");
const list = document.getElementById("resultsList");

const settingsBtn = document.getElementById("settingsBtn");
const settingsModal = document.getElementById("settingsModal");
const closeSettings = document.getElementById("closeSettings");
const saveSettingsBtn = document.getElementById("saveSettings");
const useCustomKey = document.getElementById("useCustomKey");
const customKeyFields = document.getElementById("customKeyFields");
const ollamaKeyInput = document.getElementById("ollamaKey");
const ollamaHostInput = document.getElementById("ollamaHost");
const creditStatus = document.getElementById("creditStatus");

apiLabel.textContent = `${API_BASE || "(same origin)"}${SEARCH_PATH}`;

let activeController = null;
let liveTimer = null;
let lastQuery = "";

class NoBackendError extends Error {
  constructor(message) {
    super(message);
    this.name = "NoBackendError";
  }
}

function setStatus(text, kind = "info") {
  statusEl.textContent = text || "";
  statusEl.style.color =
    kind === "error" ? "var(--danger)" : kind === "ok" ? "var(--ok)" : "var(--muted)";
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderInlineMarkdown(text) {
  const codeTokens = [];
  let out = escapeHtml(text).replace(/`([^`]+)`/g, (_, code) => {
    const token = `@@CODE${codeTokens.length}@@`;
    codeTokens.push(`<code>${code}</code>`);
    return token;
  });
  out = out.replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, (_, label, url) => {
    return `<a href="${url}" target="_blank" rel="noreferrer">${label}</a>`;
  });
  out = out.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  out = out.replace(/__([^_]+)__/g, "<strong>$1</strong>");
  out = out.replace(/(^|[\s(])\*([^*]+)\*(?=$|[\s).,!?:;])/gm, "$1<em>$2</em>");
  out = out.replace(/(^|[\s(])_([^_]+)_(?=$|[\s).,!?:;])/gm, "$1<em>$2</em>");
  out = out.replace(
    /(https?:\/\/[^\s<]+)/g,
    '<a href="$1" target="_blank" rel="noreferrer">$1</a>'
  );
  out = out.replace(/@@CODE(\d+)@@/g, (_, index) => codeTokens[Number(index)] || "");
  return out;
}

function renderMarkdown(text) {
  const source = String(text || "").replace(/\r\n?/g, "\n").trim();
  if (!source) return "";

  const blocks = [];
  const lines = source.split("\n");
  let paragraphLines = [];
  let listType = null;
  let listItems = [];

  function flushParagraph() {
    if (!paragraphLines.length) return;
    const body = paragraphLines.map((line) => renderInlineMarkdown(line)).join("<br />");
    blocks.push(`<p>${body}</p>`);
    paragraphLines = [];
  }

  function flushList() {
    if (!listType || !listItems.length) return;
    const items = listItems.map((item) => `<li>${renderInlineMarkdown(item)}</li>`).join("");
    blocks.push(`<${listType}>${items}</${listType}>`);
    listType = null;
    listItems = [];
  }

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) {
      flushParagraph();
      flushList();
      continue;
    }

    const headingMatch = line.match(/^(#{1,3})\s+(.+)$/);
    if (headingMatch) {
      flushParagraph();
      flushList();
      blocks.push(`<p class="summaryHeading">${renderInlineMarkdown(headingMatch[2])}</p>`);
      continue;
    }

    const bulletMatch = line.match(/^[-*]\s+(.+)$/);
    if (bulletMatch) {
      flushParagraph();
      if (listType !== "ul") flushList();
      listType = "ul";
      listItems.push(bulletMatch[1]);
      continue;
    }

    const orderedMatch = line.match(/^\d+\.\s+(.+)$/);
    if (orderedMatch) {
      flushParagraph();
      if (listType !== "ol") flushList();
      listType = "ol";
      listItems.push(orderedMatch[1]);
      continue;
    }

    flushList();
    paragraphLines.push(line);
  }

  flushParagraph();
  flushList();
  return blocks.join("");
}

function formatSummaryBody(text, format = "text") {
  if (format === "markdown") {
    return renderMarkdown(text);
  }
  return escapeHtml(text).replaceAll("\n", "<br />");
}

function toResultsShape(payload, q) {
  // Supported payload shapes:
  // 1) { results: [{ title, url, snippet, score }] }
  // 2) [{ title, url, snippet, score }]
  // 3) { items: [...] }
  const raw =
    (payload && Array.isArray(payload.results) && payload.results) ||
    (Array.isArray(payload) && payload) ||
    (payload && Array.isArray(payload.items) && payload.items) ||
    [];

  return raw
    .map((r, idx) => ({
      title: r.title ?? r.name ?? r.id ?? `Ergebnis ${idx + 1}`,
      url: r.url ?? r.link ?? "",
      snippet: r.snippet ?? r.text ?? r.summary ?? "",
      language: r.language ?? null,
      score:
        typeof r.score === "number"
          ? r.score
          : typeof r.similarity === "number"
            ? r.similarity
            : null,
      _q: q,
    }))
    .filter((r) => r.title || r.snippet || r.url);
}

function highlight(snippet, q) {
  const s = String(snippet || "");
  const query = String(q || "").trim();
  if (!query) return escapeHtml(s);

  // simple, safe highlighter: split query into words, highlight each (case-insensitive)
  const terms = Array.from(
    new Set(
      query
        .split(/\s+/g)
        .map((t) => t.trim())
        .filter(Boolean)
        .slice(0, 6)
    )
  );
  if (!terms.length) return escapeHtml(s);

  let out = escapeHtml(s);
  for (const term of terms) {
    const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp(`(${escaped})`, "ig");
    out = out.replace(re, "<mark>$1</mark>");
  }
  return out;
}

const LANG_NAMES = {
  de: "🇩🇪 Deutsch",
  en: "🇺🇸 English",
  fr: "🇫🇷 Français",
  es: "🇪🇸 Español",
  it: "🇮🇹 Italiano",
  nl: "🇳🇱 Nederlands",
  pl: "🇵🇱 Polski",
  pt: "🇵🇹 Português",
  ru: "🇷🇺 Русский",
  ja: "🇯🇵 日本語",
  zh: "🇨🇳 中文",
};

function renderLang(code) {
  if (!code) return "";
  const name = LANG_NAMES[code.toLowerCase()] || code.toUpperCase();
  return `<span class="resultLang">${name}</span>`;
}

function render(results, q) {
  list.innerHTML = "";
  if (!results.length) {
    list.hidden = true;
    emptyState.hidden = false;
    emptyState.querySelector(".emptyTitle").textContent = "Keine Treffer.";
    emptyState.querySelector(".emptyText").textContent = `Keine Ergebnisse für „${q}“.`;
    if (resultCount) resultCount.textContent = `0 Ergebnisse für „${q}“`;
    return;
  }

  emptyState.hidden = true;
  list.hidden = false;
  if (resultCount) resultCount.textContent = `${results.length} Ergebnisse für „${q}“`;

  for (const r of results) {
    const li = document.createElement("li");
    li.className = "result";
    const url = r.url ? String(r.url) : "";
    li.innerHTML = `
      <div class="resultTopRow">
        ${renderLang(r.language)}
        ${escapeHtml(url || (r.score == null ? "" : `score=${Number(r.score).toFixed(3)}`))}
      </div>
      <a class="resultTitle" href="${escapeHtml(url || "#")}" ${
          url ? 'target="_blank" rel="noreferrer"' : 'aria-disabled="true" tabindex="-1"'
        }>${escapeHtml(r.title || "Ohne Titel")}</a>
      <div class="resultSnippet">${highlight(r.snippet, q)}</div>
    `;
    list.appendChild(li);
  }
}

function showResultsView(q) {
  if (topbar) topbar.hidden = false;
  if (home) home.hidden = true;
  if (resultsSection) resultsSection.hidden = false;
  if (inputTop && typeof q === "string") inputTop.value = q;
}

function showHomeView() {
  if (topbar) topbar.hidden = true;
  if (home) home.hidden = false;
  if (resultsSection) resultsSection.hidden = true;
}

const summarizeBtn = document.getElementById("summarizeBtn");
const summaryContainer = document.getElementById("summaryContainer");

function hasActiveQuery() {
  return Boolean(String(lastQuery || "").trim());
}

function syncSummarizeButton(isBusy = false) {
  if (!summarizeBtn) return;
  const disabled = isBusy || !hasActiveQuery();
  summarizeBtn.disabled = disabled;
  summarizeBtn.setAttribute("aria-disabled", disabled ? "true" : "false");
}

function clearSummary() {
  if (!summaryContainer) return;
  summaryContainer.innerHTML = "";
  summaryContainer.hidden = true;
}

function renderSummaryCard(title, text, kind = "normal", format = "text") {
  if (!summaryContainer) return;
  const errorClass = kind === "error" ? " error" : "";
  summaryContainer.innerHTML = `
    <div class="summaryCard${errorClass}">
      <div class="summaryTitle">${title}</div>
      <div class="summaryText">${formatSummaryBody(text, format)}</div>
    </div>
  `;
  summaryContainer.hidden = false;
}

function renderSummaryLoading() {
  renderSummaryCard(
    "✨ KI-Zusammenfassung <span class=\"summaryLoading\"></span>",
    "Erstelle Zusammenfassung..."
  );
}

function renderSummaryResponse(payload) {
  const status =
    payload?.summary_status ||
    (payload?.summary ? "ok" : payload?.summary_message ? "error" : "unavailable");

  if (status === "ok" && payload?.summary) {
    renderSummaryCard(
      "✨ KI-Zusammenfassung",
      payload.summary,
      "normal",
      payload?.summary_format || "markdown"
    );
    return status;
  }

  if (status === "credits_exhausted") {
    renderSummaryCard(
      "⚠️ Guthaben erschöpft",
      payload?.summary_message || "Du hast deine freien Zusammenfassungen für heute aufgebraucht.",
      "error",
      "text"
    );
    return status;
  }

  if (status === "error" || (status === "unavailable" && payload?.summary_message)) {
    renderSummaryCard(
      "⚠️ Zusammenfassung nicht verfügbar",
      payload?.summary_message || "Die Zusammenfassung konnte nicht erstellt werden.",
      "error",
      "text"
    );
    return status;
  }

  clearSummary();
  return status;
}

async function search(q, summarize = false) {
  const query = String(q || "").trim();
  if (!query) {
    if (summarize) {
      setStatus("Suche zuerst nach etwas, bevor du die KI-Zusammenfassung nutzt.");
    }
    return;
  }

  lastQuery = query;
  if (activeController) activeController.abort();
  activeController = new AbortController();
  syncSummarizeButton(true);

  const url = new URL(`${API_BASE}${SEARCH_PATH}`, window.location.origin);
  url.searchParams.set("q", query);
  if (summarize) {
    url.searchParams.set("summarize", "true");
  }

  const headers = { Accept: "application/json" };
  const settings = JSON.parse(localStorage.getItem("coocle_settings") || "{}");
  if (settings.useCustomKey && settings.ollamaKey) {
    headers["X-Ollama-Key"] = settings.ollamaKey;
    if (settings.ollamaHost) {
      headers["X-Ollama-Host"] = settings.ollamaHost;
    }
  }

  btn.disabled = true;
  if (btnTop) btnTop.disabled = true;
  setStatus(summarize ? "KI fasst zusammen…" : "suche…");
  showResultsView(query);

  if (summarize) {
    renderSummaryLoading();
  } else {
    clearSummary();
  }

  try {
    const startedAt = performance.now();
    const res = await fetch(url.toString(), {
      method: "GET",
      headers: headers,
      signal: activeController.signal,
    });

    if (!res.ok) {
      const contentType = res.headers.get("content-type") || "";
      if (res.status === 404 && contentType.includes("text/html")) {
        throw new NoBackendError("Kein Backend unter /api/search (404).");
      }
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status}${text ? ` — ${text.slice(0, 160)}` : ""}`);
    }

    const payload = await res.json();
    const results = toResultsShape(payload, query);

    if (!summarize) {
      render(results, query);
    } else {
      const summaryStatus = renderSummaryResponse(payload);
      const summaryMessage =
        payload?.summary_message ||
        (summaryStatus === "credits_exhausted"
          ? "Guthaben erschöpft"
          : "Zusammenfassung nicht verfügbar");
      if (summaryStatus === "ok") {
        const ms = Math.round(performance.now() - startedAt);
        setStatus(`${results.length} Treffer · ${ms}ms`, "ok");
      } else {
        setStatus(`${summaryMessage} · ${results.length} Treffer`, "error");
      }
    }

    updateCredits();

    if (!summarize) {
      const ms = Math.round(performance.now() - startedAt);
      setStatus(`${results.length} Treffer · ${ms}ms`, "ok");
    }
  } catch (err) {
    if (err?.name === "AbortError") {
      setStatus("abgebrochen");
      return;
    }

    if (summarize) {
      renderSummaryCard(
        "⚠️ Zusammenfassung nicht verfügbar",
        err?.name === "NoBackendError"
          ? "Kein Backend für die Zusammenfassung gefunden."
          : String(err?.message || err),
        "error",
        "text"
      );
      if (err?.name === "NoBackendError") {
        setStatus("Zusammenfassung nicht verfügbar · kein Backend konfiguriert", "error");
      } else {
        setStatus(`Zusammenfassung fehlgeschlagen · ${String(err?.message || err)}`, "error");
      }
      return;
    }

    // fallback: show a minimal local mock so the UI is still usable standalone
    const results = toResultsShape(
      {
        results: [
          {
            title: `Mock: Ergebnis für „${query}“`,
            url: "",
            snippet:
              "Kein Backend erreichbar. In `app.js` kannst du `API_BASE`/`SEARCH_PATH` auf deinen Server setzen.",
            score: null,
          },
        ],
      },
      query
    );
    render(results, query);
    if (err?.name === "NoBackendError") {
      setStatus("Demo-Modus · kein Backend konfiguriert", "info");
    } else {
      setStatus(`Fehler · ${String(err?.message || err)}`, "error");
    }
  } finally {
    btn.disabled = false;
    if (btnTop) btnTop.disabled = false;
    syncSummarizeButton();
  }
}

function setLive(enabled) {
  if (liveTimer) {
    clearInterval(liveTimer);
    liveTimer = null;
  }
  if (!enabled) return;
  liveTimer = setInterval(() => {
    const q = String(lastQuery || "").trim();
    if (!q) return;
    // refresh silently; status will update timestamp/metrics naturally
    search(q);
  }, 2000);
}

form.addEventListener("submit", (e) => {
  e.preventDefault();
  const q = String(input.value || "").trim();
  if (!q) return;
  search(q);
});

formTop?.addEventListener("submit", (e) => {
  e.preventDefault();
  const q = String(inputTop?.value || "").trim();
  if (!q) return;
  input.value = q;
  search(q);
});

function firstNavigableResultHref() {
  const a = list.querySelector(".resultTitle[href]");
  const href = a?.getAttribute("href") || "";
  if (!href || href === "#") return null;
  return href;
}

luckyBtn?.addEventListener("click", () => {
  const q = String(input.value || "").trim();
  if (!q) return;
  search(q).then(() => {
    const href = firstNavigableResultHref();
    if (href) window.open(href, "_blank", "noreferrer");
  });
});

function syncClearButton() {
  if (!clearBtn) return;
  clearBtn.hidden = !String(input.value || "");
}

input.addEventListener("input", () => {
  syncClearButton();
  if (inputTop) inputTop.value = input.value;
});

clearBtn?.addEventListener("click", () => {
  input.value = "";
  if (inputTop) inputTop.value = "";
  lastQuery = "";
  syncClearButton();
  syncSummarizeButton();
  clearSummary();
  setStatus("");
  setLive(false);
  if (liveToggle) liveToggle.checked = false;
  showHomeView();
  list.innerHTML = "";
  list.hidden = true;
  emptyState.hidden = false;
  emptyState.querySelector(".emptyTitle").textContent = "Bereit.";
  emptyState.querySelector(".emptyText").textContent = "Gib einen Suchbegriff ein, um Ergebnisse zu sehen.";
  input.focus();
});

window.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    if (activeController) activeController.abort();
    input.blur();
  }
});

liveToggle?.addEventListener("change", () => {
  setLive(Boolean(liveToggle.checked));
});

summarizeBtn?.addEventListener("click", () => {
  const q = String(lastQuery || "").trim();
  if (!q) {
    syncSummarizeButton();
    setStatus("Suche zuerst nach etwas, bevor du die KI-Zusammenfassung nutzt.");
    return;
  }
  search(q, true);
});

// Settings Logic
function loadSettings() {
  const settings = JSON.parse(localStorage.getItem("coocle_settings") || "{}");
  useCustomKey.checked = !!settings.useCustomKey;
  ollamaKeyInput.value = settings.ollamaKey || "";
  ollamaHostInput.value = settings.ollamaHost || "https://ollama.com/api";
  customKeyFields.hidden = !useCustomKey.checked;
}

function handleSaveSettings() {
  const settings = {
    useCustomKey: useCustomKey.checked,
    ollamaKey: ollamaKeyInput.value.trim(),
    ollamaHost: ollamaHostInput.value.trim(),
  };
  localStorage.setItem("coocle_settings", JSON.stringify(settings));
  
  if (saveSettingsBtn) {
    const originalText = saveSettingsBtn.textContent;
    saveSettingsBtn.textContent = "Gespeichert! ✓";
    setTimeout(() => {
      saveSettingsBtn.textContent = originalText;
      settingsModal.hidden = true;
    }, 500);
  } else {
    settingsModal.hidden = true;
  }
  
  updateCredits();
}

async function updateCredits() {
  try {
    const res = await fetch(`${API_BASE}/api/credits`);
    if (res.ok) {
      const data = await res.json();
      creditStatus.textContent = `${data.remaining} / ${data.total} Zusammenfassungen heute frei`;
      if (data.remaining <= 0) {
        creditStatus.style.color = "var(--danger)";
      } else {
        creditStatus.style.color = "var(--ok)";
      }
    }
  } catch (e) {
    creditStatus.textContent = "Status nicht verfügbar";
  }
}

settingsBtn?.addEventListener("click", () => {
  loadSettings();
  updateCredits();
  settingsModal.hidden = false;
});

closeSettings?.addEventListener("click", () => {
  settingsModal.hidden = true;
});

saveSettingsBtn?.addEventListener("click", handleSaveSettings);

useCustomKey?.addEventListener("change", () => {
  customKeyFields.hidden = !useCustomKey.checked;
});

// init
loadSettings();
updateCredits();
document.querySelectorAll(".topicChip").forEach((chip) => {
  chip.addEventListener("click", () => {
    const q = chip.dataset.q;
    if (!q) return;
    input.value = q;
    if (inputTop) inputTop.value = q;
    syncClearButton();
    search(q);
  });
});

showHomeView();
syncClearButton();
syncSummarizeButton();

