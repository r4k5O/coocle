const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildSearchUrl,
  escapeHtml,
  formatSummaryBody,
  isTransientBackendFailure,
  renderInlineMarkdown,
  renderMarkdown,
  shouldUseLocalMockFallback,
  toResultsShape,
} = require("../app.js");

test("escapeHtml escapes raw markup safely", () => {
  assert.equal(escapeHtml('<script>alert("x")</script>'), "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;");
});

test("renderInlineMarkdown formats links and strong text", () => {
  const html = renderInlineMarkdown("**Hallo** [Docs](https://example.com)");
  assert.match(html, /<strong>Hallo<\/strong>/);
  assert.match(html, /<a href="https:\/\/example.com" target="_blank" rel="noreferrer">Docs<\/a>/);
});

test("renderMarkdown builds headings, lists, and paragraphs", () => {
  const html = renderMarkdown("## Kurz\n\n- Punkt 1\n- Punkt 2\n\nEin Satz.");
  assert.match(html, /<p class="summaryHeading">Kurz<\/p>/);
  assert.match(html, /<ul><li>Punkt 1<\/li><li>Punkt 2<\/li><\/ul>/);
  assert.match(html, /<p>Ein Satz\.<\/p>/);
});

test("formatSummaryBody preserves line breaks in text mode", () => {
  assert.equal(formatSummaryBody("Zeile 1\nZeile 2", "text"), "Zeile 1<br />Zeile 2");
});

test("toResultsShape normalizes fallback payload fields", () => {
  const shaped = toResultsShape(
    {
      items: [
        {
          name: "Python Docs",
          link: "https://example.com/python",
          text: "Reference",
          similarity: 0.87,
          language: "en",
        },
      ],
    },
    "python"
  );

  assert.deepEqual(shaped, [
    {
      title: "Python Docs",
      url: "https://example.com/python",
      snippet: "Reference",
      language: "en",
      score: 0.87,
      _q: "python",
    },
  ]);
});

test("buildSearchUrl defaults searches to hybrid mode", () => {
  global.window = { location: { origin: "https://coocle.test" } };

  const url = buildSearchUrl("python testing", true);

  assert.equal(url.searchParams.get("q"), "python testing");
  assert.equal(url.searchParams.get("mode"), "hybrid");
  assert.equal(url.searchParams.get("summarize"), "true");

  delete global.window;
});

test("isTransientBackendFailure matches cold-start style backend failures", () => {
  assert.equal(isTransientBackendFailure({ name: "NoBackendError" }), true);
  assert.equal(isTransientBackendFailure({ name: "TypeError" }), true);
  assert.equal(isTransientBackendFailure({ name: "HttpStatusError", status: 503 }), true);
  assert.equal(isTransientBackendFailure({ name: "HttpStatusError", status: 500 }), false);
});

test("shouldUseLocalMockFallback only enables mock mode for file URLs", () => {
  assert.equal(shouldUseLocalMockFallback({ protocol: "file:" }), true);
  assert.equal(shouldUseLocalMockFallback({ protocol: "https:" }), false);
  assert.equal(shouldUseLocalMockFallback(null), false);
});
