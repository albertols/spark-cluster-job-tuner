---
name: frontend
description: Use whenever editing the Spark Cluster Auto-Tuner Dashboard frontend at `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/` (app.js, index.html, style.css, config.json, serve.sh). Triggers for any change to the dashboard UI, charts, tabs (Fleet Overview / Correlations / Divergences), landing page, cluster detail view, modals, doc popovers, routing/URL state, discovery, JSON/CSV caching, or rendering. Enforces small surgical diffs, behavior preservation, UX/accessibility/responsiveness checks, and a final QA pass. Apply even when the user does not explicitly say "frontend" — if any file under that path is touched, this skill applies.
---

# Frontend Skill — Spark Cluster Auto-Tuner Dashboard

## Purpose
Make changes to the dashboard frontend with minimal regressions, predictable UX, and minimal token usage. Future-friendly to a possible Scala.js migration: keep pure data transforms separable from DOM access.

## Scope
`src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/`:
- `app.js` (~3.1k lines) — single-file SPA: state, routing, discovery, caching, rendering
- `index.html` — static shell, tab buttons, Chart.js + hammer + zoom plugin via CDN
- `style.css` — all styles (~1.6k lines)
- `config.json` — `gcpProjectId`, `inputsPath`, `outputsPath` (relative to this file)
- `serve.sh` — `python3 -m http.server` launcher; back-compat mode accepts an explicit output dir

Plain HTML/CSS/JavaScript, no framework, no build step, no npm. Preserve this.

## Architecture cheat-sheet (do not re-derive)
`app.js` is organized by `// ── Section ──` banners. Map the file with:
```
grep -n "^function\|^async function\|^// ──" app.js
```
Major sections (in order): Inline-SVG tutorials → Metric Documentation → Config + Discovery → Data Loading → Bootstrap → Router → Mode switching → Landing → Metadata Bar → Summary Cards → Cluster Grid → Cluster Tooltip → Cluster Detail → Charts → Recipe Spark Conf Modal → Doc popover → Correlations → Divergences.

Top-level mutable state (lines 4–25):
- `config`, `outputsRoot`, `data`, `analysisDir`, `currentEntry`
- Caches: `clusterJsonCache`, `clusterJsonTriedPaths`, `clusterSummaryCache`, `generationSummary`, `clusterSummaryFiles`, `discoveredEntries`
- `routingInFlight` guards `pushState` reentry

Routing pipeline: `parseRoute` → `buildUrl` → `navigate` → `applyRoute`. Tabs are switched through `navigate({ tab })`, not by toggling DOM directly.

Discovery (`discoverAnalyses` + `parseDirListing`) scrapes the HTML directory listing produced by `python3 -m http.server`. Do not assume any other server.

## Core rules
1. Read the smallest set of lines needed. Use `grep -n` to locate, then `Read` with `offset`/`limit` — do not slurp the whole `app.js`.
2. Prefer small, surgical diffs. No drive-by refactors.
3. Do not break routing, discovery, caching, or URL/back-forward behavior.
4. Preserve current behavior unless the task explicitly changes it.
5. No new runtime dependencies. CDN scripts in `index.html` are fixed: Chart.js 4.4.7, hammer 2.0.8, chartjs-plugin-zoom 2.0.1.
6. No build tooling. No bundler, no npm, no transpile. Code must run directly in the browser.
7. Sanitize/escape any user-derived or JSON-derived string before injecting into HTML. Prefer `textContent`/attribute setters over template-literal HTML when content is dynamic.
8. Keep responsiveness and keyboard accessibility intact.
9. Reuse existing helpers (`recipeShortName`, `chartOpts`, `keyForLabel`, `extractClusterConf`, etc.) and existing CSS classes instead of inventing parallel ones.
10. Scala.js readiness: keep new logic as small pure functions taking plain data and returning plain data; isolate DOM mutation in render functions.

## Pre-edit checklist
- [ ] Identified the exact UI area, function, and section banner involved.
- [ ] Located reusable helpers / CSS classes for what you need (grep first).
- [ ] Understood current state, routing, and caches touched by the change.
- [ ] Confirmed whether the change must round-trip through the URL (deep links).
- [ ] Checked the JSON shape you depend on by reading one real file under `outputsPath`/`inputsPath` if relevant.
- [ ] Decided the smallest diff that does it.

## Implementation rules
- Edit in place; do not re-order unrelated code.
- Keep functions pure where possible; pass state in, return data out.
- DOM building: prefer building a small string with already-escaped pieces, or use `document.createElement` + `textContent` for dynamic values.
- Charts: route options through `chartOpts(...)`; do not duplicate its tooltip/zoom config.
- Pan/zoom clamp: charts whose data is non-negative by definition (time, worker counts, IP counts, cost €, jobs, minutes) MUST pass `{ clampX: true, clampY: true }` to `csZoomPluginConfig(canvas, …)`. Set only the axes that are non-negative (e.g. `{ clampY: true }` when only Y is bounded). Z-score / signed-delta / divergence charts must NOT clamp — they legitimately span negative values.
- Routing changes: update both `parseRoute` and `buildUrl`, and ensure `applyRoute` handles the new shape. Never write `location.href`/`history.pushState` outside `navigate`.
- Caches: read through the existing cache maps; only add new keys if a new fetch path requires it. Do not bypass the cache for "freshness".
- CSS: add to `style.css`, scoped to a class. No inline styles for theming. Reuse spacing/typography variables already in the file.
- Errors: surface via `showFatalError` for fatal cases; use `empty-msg` blocks for empty/error UI inside a section.
- Keep all user-facing copy short and consistent with surrounding text.

## UX checklist
- [ ] Loading, empty, and error states render and read clearly.
- [ ] Spacing/typography match neighboring panels.
- [ ] No clipped content at narrow widths; tables scroll rather than overflow the page.
- [ ] Interactive controls reachable by keyboard; visible focus states.
- [ ] Tooltips/popovers dismiss on outside click and Esc where existing peers do.
- [ ] Chart canvases have an `aria-label` or adjacent heading.
- [ ] Dark theme contrast preserved (the app is dark by default).

## Verification checklist (run before declaring done)
- [ ] `index.html` parses; no missing tags introduced.
- [ ] `app.js` has no syntax errors (`node -c app.js` if Node is handy, otherwise open the page).
- [ ] `./serve.sh` starts and the landing page lists analyses.
- [ ] Click into an analysis → Fleet Overview, Correlations, Divergences, and a Cluster Detail all render.
- [ ] Browser console shows no new errors or warnings caused by the change.
- [ ] Refresh on a deep-linked URL (e.g. `?data=…&tab=correlations&cluster=…`) restores the same view.
- [ ] Back/forward buttons restore prior views.
- [ ] Resize to ~720px wide: layout still usable, no horizontal page scroll.

## When in doubt
- Make the smallest correct change.
- Compatibility over refactoring; if a refactor is unavoidable, isolate it and keep observable behavior identical.
- If the task implies a larger redesign, stop and confirm scope before editing.

## Anti-patterns (do not do)
- Do not introduce a framework, bundler, or TypeScript step.
- Do not add new CDN scripts without an explicit ask.
- Do not rewrite routing, discovery, or caching to "modernize" them.
- Do not inline large data structures into HTML; load via existing fetch paths.
- Do not silently change JSON/CSV field names — they are produced by the Scala side.
- Do not replace `python3 -m http.server` assumptions with a different server.
