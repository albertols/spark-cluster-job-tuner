# Total Estimated IP Count timeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement TODO_3 from `docs/superpowers/specs/2026-05-01-ipc-timeline-design.md` — the fleet-wide "Total estimated IP count" timeline section above Cluster Summary Graphs.

**Architecture:** Frontend-only. Vanilla JS extensions in `auto/frontend/{app.js,index.html,style.css}`. Reuses `cost_timeline` JSON already emitted by the cluster tuner (no backend changes). One stacked-area Chart.js chart per side, custom plugins for thresholds and crosshair, dual hover + anchored crosshair lines, KPI strip, sticky table.

**Tech Stack:** Vanilla ES2017 JavaScript (no transpiler), Chart.js (already loaded), `chartjs-plugin-zoom` (already loaded), CSS3, served by `python3 -m http.server` via `serve.sh`.

**Verification approach:** No JS test infrastructure exists in the repo. Each task ends with a **manual browser acceptance gate** — explicit user-action + expected visual/behavioral result. Helpers are written DOM-free so unit tests can be added later if needed without refactoring.

**Branch & worktree:** Already on `oss-ready-mock-v1`. Plan does not create a new worktree (pre-existing uncommitted changes outside the IPC scope must not be touched). Each task commits only the files it modifies.

---

## File structure

| File | Role | Lines added (approx) |
|---|---|---|
| `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/index.html` | Replace stub at lines 53–59 with selector + pair skeleton | +20 lines, –4 lines |
| `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css` | New rule block `/* ── IP Count Timeline ── */` at end of file | +180 lines |
| `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js` | New section `// ── Cluster IP Count Timeline ──` appended after `renderDetailClusterCost` | +350 lines |
| `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/config.json` | (no change — `ipQuota` already present) | 0 |

**State location:** Per-side state lives on the `<section class="ipc-side">` DOM element via `dataset.*` (string state) and `_ipc*` private properties (object state). No module-scope globals beyond `_ipcSideCache` (a `Map<sideName, sideData>`).

---

## Task 1: HTML skeleton + CSS shell + lazy section open

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/index.html:53-59`
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css` (append at end)
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js` (append after the existing `renderClusterConfComparison` function)

- [ ] **Step 1: Replace the HTML stub.**

In `index.html`, replace lines 53–59 (the existing `<details id="cluster-ip-count-section">` stub) with:

```html
    <details id="cluster-ip-count-section" class="cs-graphs">
      <summary>
        <h3 style="display:inline">Total estimated IP count</h3>
        <span class="cs-graphs-help">Workers + master IPs over time · stacked per cluster · click chart to anchor crosshair</span>
      </summary>
      <div id="ipc-body">
        <div class="ipc-controls">
          <div class="seg-toggle" id="ipc-date-toggle" role="tablist">
            <button class="seg" data-side="reference" role="tab" aria-selected="false">Reference</button>
            <button class="seg active" data-side="current" role="tab" aria-selected="true">Current</button>
            <button class="seg" data-side="both" role="tab" aria-selected="false">Both</button>
          </div>
          <span class="ipc-cap-note">IP cap <code id="ipc-cap-num">256</code> · warn ≥<span id="ipc-warn-pct">70</span>% · crit ≥<span id="ipc-crit-pct">90</span>% (configurable in <code>config.json</code> → <code>ipQuota</code>)</span>
        </div>
        <div class="ipc-pair" data-mode="current">
          <section class="ipc-side" data-side="reference"></section>
          <section class="ipc-side" data-side="current"></section>
        </div>
      </div>
    </details>
```

- [ ] **Step 2: Append CSS block to `style.css`.**

Append at end of file:

```css
/* ── IP Count Timeline (TODO_3) ─────────────────────────────────────────── */
#cluster-ip-count-section { margin: 16px 0; }
#cluster-ip-count-section > summary { cursor: pointer; padding: 8px 0; user-select: none; }
#cluster-ip-count-section > summary > h3 { margin: 0; }
.ipc-controls {
  display: flex; align-items: center; gap: 12px;
  padding: 12px 0; flex-wrap: wrap;
}
.ipc-cap-note { color: #8b949e; font-size: 12px; }
.ipc-cap-note code { background: #161b22; padding: 1px 4px; border-radius: 3px; }

.ipc-pair { display: grid; grid-template-columns: 1fr; gap: 12px; }
.ipc-pair[data-mode="both"] { grid-template-columns: 1fr 1fr; }
.ipc-pair[data-mode="reference"] .ipc-side[data-side="current"] { display: none; }
.ipc-pair[data-mode="current"]   .ipc-side[data-side="reference"] { display: none; }
.ipc-pair[data-expanded="reference"] .ipc-side[data-side="current"] { display: none; }
.ipc-pair[data-expanded="current"]   .ipc-side[data-side="reference"] { display: none; }

.ipc-side {
  border: 1px solid #30363d; border-radius: 6px; padding: 12px;
  background: #0d1117;
}
.ipc-side .empty-msg { padding: 32px; text-align: center; color: #8b949e; }
.ipc-side-header {
  display: flex; align-items: center; justify-content: space-between;
  padding-bottom: 8px; margin-bottom: 8px; border-bottom: 1px solid #21262d;
}
.ipc-side-header h4 { margin: 0; font-size: 14px; }
.ipc-side-date { color: #8b949e; font-weight: normal; margin-left: 6px; }
.ipc-side-buttons { display: flex; gap: 6px; }
.ipc-side-buttons button {
  background: transparent; border: 1px solid #30363d; color: #c9d1d9;
  border-radius: 4px; padding: 2px 8px; cursor: pointer; font-size: 13px;
}
.ipc-side-buttons button:hover { background: #161b22; }
```

- [ ] **Step 3: Append the lazy entry stub to `app.js`.**

Find the line `function renderClusterConfComparison(clusterName, refJson, curJson, refDate, curDate) {` (around line 1457) and append AFTER the closing brace of that function:

```javascript

// ── Cluster IP Count Timeline (TODO_3) ──────────────────────────────────────
// Above the Cluster Summary Graphs in Fleet Overview. Stacked-area chart of
// per-cluster IPs (workers + master) over time, with hover + anchored
// crosshair, KPI strip (live total + day max), and a synced table.
//
// Data source: per-cluster JSON `cost_timeline` already emitted by the tuner.
// Reuses `loadClusterJson` for caching. Section is closed by default and
// renders lazily on first <details> toggle.

let _ipcInitialized = false;

function _ipcInit() {
  if (_ipcInitialized) return;
  _ipcInitialized = true;

  const section = document.getElementById('cluster-ip-count-section');
  if (!section) return;

  // Inject ipQuota values from config so the cap-note is accurate.
  const q = (config && config.ipQuota) || {};
  const cap = +q.max_ip_count || 256;
  const warnPct = +q.warn_pct || 70;
  const critPct = +q.crit_pct || 90;
  const capEl  = document.getElementById('ipc-cap-num');
  const warnEl = document.getElementById('ipc-warn-pct');
  const critEl = document.getElementById('ipc-crit-pct');
  if (capEl)  capEl.textContent  = String(cap);
  if (warnEl) warnEl.textContent = String(warnPct);
  if (critEl) critEl.textContent = String(critPct);

  // Lazy first-paint on first open.
  section.addEventListener('toggle', () => {
    if (!section.open) return;
    if (section.dataset.rendered === '1') return;
    section.dataset.rendered = '1';
    _renderClusterIpCountSection();
  }, { once: false });
}

// Public entry. Called once from bootstrap so the toggle listener is wired
// before users interact.
function renderClusterIpCountSectionInit() { _ipcInit(); }

// Real renderer wired in subsequent tasks. For now: empty-state placeholder.
async function _renderClusterIpCountSection() {
  const pair = document.querySelector('#ipc-body .ipc-pair');
  if (!pair) return;
  pair.querySelectorAll('.ipc-side').forEach(sideEl => {
    sideEl.innerHTML = `<div class="empty-msg">IPC timeline rendering not yet implemented.</div>`;
  });
}
```

- [ ] **Step 4: Wire the init to the existing bootstrap path.**

`_ipcInit` is idempotent (`_ipcInitialized` guard) so it's safe to call from multiple paths. Find every call site that renders the Fleet Overview after an analysis loads:

```bash
grep -n "renderClustersSummarySection\|renderFleetOverview\|renderClusterGrid\|renderSummaryCards" src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js | head -10
```

Pick the function that's called when an analysis becomes active (likely a `renderFleetOverview` or `openAnalysisEntry` body). Add this line as the LAST statement of that function:

```javascript
  renderClusterIpCountSectionInit();
```

Verify by reloading: a `console.log` inside `_ipcInit` (temporarily) should fire exactly once per analysis open, not at every navigation. Remove the `console.log` once confirmed.

- [ ] **Step 5: Manual verification.**

Run from `auto/frontend/`:

```bash
cd src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend
./serve.sh
```

In the browser at the landing page:
1. Open one of the existing analyses (e.g. `2099_01_02_auto_tuned`).
2. Scroll Fleet Overview. The "Total estimated IP count" `<details>` is **closed by default**, sitting **above** "Cluster Summary Graphs".
3. Open dev tools → Elements. Confirm `#cluster-ip-count-section` has NO `data-rendered` attribute (lazy).
4. Click the section to open. The body shows the selector strip (Reference | Current | Both, Current selected) and the IP cap note reads `IP cap 256 · warn ≥70% · crit ≥90%`.
5. Inside both `.ipc-side` slots, the "IPC timeline rendering not yet implemented." message appears.
6. The `current` side is visible; the `reference` side is hidden (CSS `data-mode="current"` rule).
7. Click to close. Click to reopen. The placeholder stays (no re-render — `data-rendered="1"` is set).

Stop the server (Ctrl+C).

- [ ] **Step 6: Commit.**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/index.html \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js
git commit -m "$(cat <<'EOF'
adds IPC timeline section skeleton (TODO_3 task 1/10)

Lazy-rendered <details> shell above Cluster Summary Graphs with
date selector strip and per-side empty placeholders. ipQuota values
from config.json drive the cap note.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Pure helpers — `_buildIpcSegments`, `_evalIpcAt`, `_thresholdLevel`, `_hashHueForCluster`

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js` (append to the IPC section started in Task 1)

These helpers are DOM-free so they can be unit-tested later without a JS test runner.

- [ ] **Step 1: Append helper functions immediately after `_renderClusterIpCountSection`.**

```javascript
// Convert ISO timestamp to epoch ms; null on invalid input.
function _ipcTsMs(s) { const t = new Date(s).getTime(); return Number.isFinite(t) ? t : null; }

// Format epoch ms as "HH:MM:SS" UTC for axis ticks and chips.
function _ipcFmtHmsUtc(ms) {
  if (!Number.isFinite(ms)) return '';
  return new Date(ms).toISOString().slice(11, 19);
}

// Walk per-cluster JSONs and produce a flat segments array. One row per
// (cluster, incarnation, interval). Clusters without `cost_timeline` are
// skipped (C.1). `cost_timeline.intervals[]` already encodes the b22/b23
// step function — we don't reproduce that logic here.
//
// Returns: [{ cluster, fromMs, toMs, workers, master:1, ips, machineType,
//             segCostEur, idx }]
function _ipcBuildSegments(perClusterJsons) {
  const out = [];
  for (const [clusterName, json] of perClusterJsons) {
    if (!json || !json.cost_timeline) continue;
    const ct = json.cost_timeline;
    const machineType = ct.worker_machine_type || '';
    (ct.incarnations || []).forEach(inc => {
      (inc.intervals || []).forEach(iv => {
        const fromMs = _ipcTsMs(iv.from_ts);
        const toMs   = _ipcTsMs(iv.to_ts);
        if (fromMs == null || toMs == null || toMs <= fromMs) return;
        const workers = +iv.workers || 0;
        out.push({
          cluster: clusterName,
          fromMs, toMs,
          workers,
          master: 1,
          ips: workers + 1,
          machineType,
          segCostEur: +iv.seg_cost_eur || 0,
          idx: inc.idx
        });
      });
    });
  }
  return out;
}

// Evaluate per-cluster IPs at time t. Returns rows for clusters alive at t,
// sorted by ips desc. A cluster is "alive at t" when t lies in [fromMs, toMs).
// O(N) over segments — fine for the expected scale (≤2000 segments).
function _ipcEvalAt(segments, t) {
  if (!Number.isFinite(t)) return [];
  const byCluster = new Map();
  for (const s of segments) {
    if (t < s.fromMs || t >= s.toMs) continue;
    // If multiple incarnations of the same cluster overlap (impossible in b20
    // by design but guard anyway), prefer the higher-IPs one.
    const prev = byCluster.get(s.cluster);
    if (!prev || s.ips > prev.ips) byCluster.set(s.cluster, s);
  }
  return Array.from(byCluster.values()).sort((a, b) => b.ips - a.ips);
}

// Walk the chart x-axis at every event boundary and find the peak fleet sum.
// Returns { peakMs, peakIps }. Empty input → { peakMs: null, peakIps: 0 }.
function _ipcFindPeak(segments) {
  if (segments.length === 0) return { peakMs: null, peakIps: 0 };
  // Candidate timestamps: every fromMs (boundaries are where fleet sum changes).
  const ts = Array.from(new Set(segments.map(s => s.fromMs))).sort((a, b) => a - b);
  let peakMs = ts[0]; let peakIps = 0;
  for (const t of ts) {
    let sum = 0;
    for (const s of segments) {
      if (t >= s.fromMs && t < s.toMs) sum += s.ips;
    }
    if (sum > peakIps) { peakIps = sum; peakMs = t; }
  }
  return { peakMs, peakIps };
}

// Map an IP count to a threshold class for KPI box styling.
// "" (default), "warn", "crit", "over".
function _ipcThresholdLevel(ips, ipQuota) {
  const cap = +ipQuota.max_ip_count || 256;
  const warnAt = cap * (+ipQuota.warn_pct || 70) / 100;
  const critAt = cap * (+ipQuota.crit_pct || 90) / 100;
  if (ips >= cap) return 'over';
  if (ips >= critAt) return 'crit';
  if (ips >= warnAt) return 'warn';
  return '';
}

// Deterministic HSL color from cluster name. Same cluster → same color
// across reloads and across reference vs current sides.
function _ipcHashHueForCluster(name) {
  let h = 0;
  for (let i = 0; i < name.length; i++) {
    h = (h * 31 + name.charCodeAt(i)) | 0;
  }
  const hue = ((h % 360) + 360) % 360;
  return { fill: `hsla(${hue}, 60%, 55%, 0.55)`, stroke: `hsl(${hue}, 60%, 55%)` };
}

// Cluster list for a date: union of `cluster_trends` cluster names. The same
// list used by the cluster grid; falls back to empty if `data` not present.
function _ipcClusterNamesForRun() {
  if (!data || !Array.isArray(data.cluster_trends)) return [];
  return data.cluster_trends.map(c => c.cluster).sort();
}

// Load per-cluster JSONs for a date in parallel via the existing cache.
async function _ipcLoadDataForDate(date) {
  const names = _ipcClusterNamesForRun();
  const pairs = await Promise.all(
    names.map(async n => [n, await loadClusterJson(date, n)])
  );
  const segments = _ipcBuildSegments(pairs);
  const peak = _ipcFindPeak(segments);
  // Skipped clusters (no cost_timeline) — surface in a footer chip.
  const skipped = pairs.filter(([_n, j]) => !j || !j.cost_timeline).map(([n]) => n);
  return { date, segments, peakMs: peak.peakMs, peakIps: peak.peakIps, skipped };
}
```

- [ ] **Step 2: Self-check the helpers in the browser console.**

This is the closest we have to a unit test. Run `serve.sh`, open `2099_01_02_auto_tuned`, open the IPC section so `_ipcInit` runs, then in dev-tools console:

```javascript
// Verify segment build on the current run:
const names = _ipcClusterNamesForRun();
console.log('cluster names:', names);

// Force-load and inspect one date:
_ipcLoadDataForDate(data.metadata.current_date).then(d => {
  window.__ipcDbg = d;
  console.log('segments:', d.segments.length);
  console.log('peak:', d.peakIps, 'at', _ipcFmtHmsUtc(d.peakMs));
  console.log('skipped:', d.skipped);

  // Verify three known timestamps for 2099_01_01 spec acceptance gate.
  // (Substitute the date that currently has b20/b21; the file
  //  outputs/2099_01_01/_clusters-summary.csv shows clusters mock-001..004.)
  const t1 = Date.parse('2099-01-01T03:30:00Z');
  const t2 = Date.parse('2099-01-01T05:45:00Z');
  const t3 = Date.parse('2099-01-01T12:00:00Z');
  [t1, t2, t3].forEach(t => {
    const rows = _ipcEvalAt(d.segments, t);
    const total = rows.reduce((s, r) => s + r.ips, 0);
    console.log(_ipcFmtHmsUtc(t), 'total IPs:', total, 'rows:', rows.map(r => `${r.cluster}=${r.ips}`));
  });
});
```

Expected console output for date `2099_01_01` (per spec §11.1):
- `03:30:00` → total IPs: **25** — `mock-cluster-001=7, mock-cluster-002=11, mock-cluster-004=7`
- `05:45:00` → total IPs: **34** — `mock-cluster-003=13, mock-cluster-002=11, mock-cluster-001=5, mock-cluster-004=5`
- `12:00:00` → total IPs: **17** — `mock-cluster-003=17`

(For `2099_01_02_auto_tuned` the numbers will differ — the goal here is shape, not values. The acceptance gate at Task 10 re-runs against `2099_01_01` after that date is openable in the dashboard.)

- [ ] **Step 3: Commit.**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js
git commit -m "$(cat <<'EOF'
adds IPC timeline pure helpers (TODO_3 task 2/10)

DOM-free helpers for the IPC section: segment construction from
cost_timeline, alive-at-t evaluation, peak finding, threshold
level mapping, deterministic cluster colors. Verified by manual
console eval against known fixture timestamps.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Per-side render scaffold — header, KPI strip, chart canvas, anchor chip, table shell

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js` (append to IPC section)
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css` (append to IPC CSS block)

- [ ] **Step 1: Add CSS for the per-side internals.**

Append to the `/* ── IP Count Timeline ── */` block in `style.css`:

```css
.ipc-kpi-strip { display: flex; gap: 12px; padding: 8px 0; flex-wrap: wrap; }
.ipc-kpi {
  flex: 1 1 200px; min-width: 200px;
  padding: 10px 14px; border-radius: 6px;
  background: #161b22; border: 1px solid #30363d;
  cursor: default;
}
.ipc-kpi.clickable { cursor: pointer; }
.ipc-kpi.clickable:hover { background: #21262d; }
.ipc-kpi-label { color: #8b949e; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }
.ipc-kpi-value { color: #c9d1d9; font-size: 22px; font-weight: 600; margin-top: 2px; }
.ipc-kpi-sub { color: #8b949e; font-size: 12px; margin-top: 2px; }

.ipc-kpi.warn .ipc-kpi-value { color: #d4a72c; }
.ipc-kpi.warn { border-color: rgba(212,167,44,0.55); background: rgba(212,167,44,0.07); }
.ipc-kpi.crit .ipc-kpi-value { color: #f85149; }
.ipc-kpi.crit { border-color: rgba(248,81,73,0.55); background: rgba(248,81,73,0.10); }
.ipc-kpi.over { background: rgba(248,81,73,0.20); border-color: #f85149; }
.ipc-kpi.over .ipc-kpi-value { color: #ffffff; }

.ipc-chart-wrap { position: relative; height: 280px; margin: 8px 0; }

.ipc-anchor-chip {
  display: none; align-items: center; gap: 8px;
  padding: 4px 8px; margin: 4px 0;
  background: #161b22; border: 1px solid #30363d; border-radius: 4px;
  color: #c9d1d9; font-size: 12px; width: max-content;
}
.ipc-anchor-chip.visible { display: inline-flex; }
.ipc-anchor-chip button {
  background: transparent; border: none; color: #8b949e;
  cursor: pointer; padding: 0 2px; font-size: 14px;
}
.ipc-anchor-chip button:hover { color: #f85149; }

.ipc-table-wrap { max-height: 320px; overflow-y: auto; margin-top: 8px; }
.ipc-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.ipc-table thead th {
  position: sticky; top: 0;
  background: #161b22; color: #8b949e;
  text-align: left; padding: 6px 8px;
  border-bottom: 1px solid #30363d;
  font-weight: 600;
}
.ipc-table tbody td { padding: 6px 8px; border-bottom: 1px solid #21262d; color: #c9d1d9; }
.ipc-table tbody tr.ipc-row-active { background: rgba(88,166,255,0.08); }
.ipc-table tbody tr { cursor: pointer; }
.ipc-table tbody tr:hover { background: rgba(139,148,158,0.05); }
.ipc-table tfoot td {
  padding: 6px 8px; border-top: 2px solid #30363d;
  background: #161b22; font-weight: 600; color: #c9d1d9;
}
.ipc-tag-fallback {
  display: inline-block; padding: 1px 5px; border-radius: 3px;
  background: rgba(212,167,44,0.15); color: #d4a72c;
  font-size: 10px; margin-left: 6px;
}
.ipc-skipped-chip {
  display: inline-block; margin-top: 8px; padding: 2px 8px;
  background: #161b22; border: 1px solid #30363d; border-radius: 4px;
  color: #8b949e; font-size: 11px;
}
```

- [ ] **Step 2: Add `_renderIpcSide` (skeleton, no chart yet) to `app.js`.**

Append after the helpers from Task 2:

```javascript
// Per-side cache: side name → loaded data + chart instance.
const _ipcSideCache = new Map();

// Render one side's full DOM. Chart wiring comes in subsequent tasks; for
// now this lays out the header / KPI strip / chart canvas / anchor chip /
// table shell so the visual structure is final.
function _ipcRenderSide(sideEl, sideData, ipQuota, dateLabel) {
  const sideName = sideEl.dataset.side; // 'reference' | 'current'
  const headerLabel = sideName === 'reference' ? 'Reference' : 'Current';

  // Empty-state path (I.2).
  if (!sideData || sideData.segments.length === 0) {
    sideEl.innerHTML = `
      <header class="ipc-side-header">
        <h4>${headerLabel}${dateLabel ? ` <span class="ipc-side-date">(${escapeHtml(dateLabel)})</span>` : ''}</h4>
      </header>
      <div class="empty-msg">No autoscaling data exported for this date.</div>`;
    return;
  }

  const canvasId = `ipc-canvas-${sideName}-${Math.random().toString(36).slice(2, 8)}`;
  const peakClass = _ipcThresholdLevel(sideData.peakIps, ipQuota);

  sideEl.innerHTML = `
    <header class="ipc-side-header">
      <h4>${headerLabel}${dateLabel ? ` <span class="ipc-side-date">(${escapeHtml(dateLabel)})</span>` : ''}</h4>
      <div class="ipc-side-buttons">
        <button class="ipc-side-reset" title="Reset zoom and clear anchor">↻</button>
        <button class="ipc-side-expand" title="Expand">⤢</button>
      </div>
    </header>
    <div class="ipc-kpi-strip">
      <div class="ipc-kpi ipc-kpi-total" data-kind="total">
        <div class="ipc-kpi-label">Total estimated IP count</div>
        <div class="ipc-kpi-value">—</div>
        <div class="ipc-kpi-sub">— of ${ipQuota.max_ip_count}</div>
      </div>
      <div class="ipc-kpi ipc-kpi-max clickable ${peakClass}" data-kind="max" title="Click to anchor crosshair at peak">
        <div class="ipc-kpi-label">Max total IP count</div>
        <div class="ipc-kpi-value">${sideData.peakIps}</div>
        <div class="ipc-kpi-sub">@ ${_ipcFmtHmsUtc(sideData.peakMs)}Z · ${Math.round(100 * sideData.peakIps / (ipQuota.max_ip_count || 256))}% of ${ipQuota.max_ip_count}</div>
      </div>
    </div>
    <div class="ipc-chart-wrap"><canvas id="${canvasId}"></canvas></div>
    <div class="ipc-anchor-chip">
      <span class="ipc-anchor-text">Anchored at —</span>
      <button class="ipc-anchor-clear" title="Clear anchor">✕</button>
    </div>
    ${sideData.skipped.length ? `<div class="ipc-skipped-chip">${sideData.skipped.length} cluster${sideData.skipped.length === 1 ? '' : 's'} skipped — no b20 span: ${sideData.skipped.map(escapeHtml).join(', ')}</div>` : ''}
    <div class="ipc-table-wrap">
      <table class="ipc-table">
        <thead><tr>
          <th>Cluster</th><th>W</th><th>M</th><th>IPs</th>
          <th>Machine</th><th>Cost/seg</th><th>Inc#</th>
        </tr></thead>
        <tbody></tbody>
        <tfoot><tr>
          <td>Σ fleet</td><td>—</td><td>—</td><td class="ipc-tfoot-total">—</td>
          <td>—</td><td class="ipc-tfoot-cost">—</td><td>—</td>
        </tr></tfoot>
      </table>
    </div>`;

  // Stash side-local state for later tasks.
  sideEl._ipcData = sideData;
  sideEl._ipcQuota = ipQuota;
  sideEl._ipcCanvasId = canvasId;
  sideEl._ipcHoverMs = null;
  sideEl.dataset.anchorMs = '';
}
```

- [ ] **Step 3: Wire the section renderer to call `_ipcRenderSide` for each visible side.**

Replace the placeholder `_renderClusterIpCountSection` definition (added in Task 1 step 3) with:

```javascript
async function _renderClusterIpCountSection() {
  if (!data || !data.metadata) return;
  const ipQuota = (config && config.ipQuota) || { max_ip_count: 256, warn_pct: 70, crit_pct: 90 };
  const refDate = data.metadata.reference_date;
  const curDate = data.metadata.current_date;

  // Always load both sides — toggling is cheap once cached.
  const [refData, curData] = await Promise.all([
    refDate ? _ipcLoadDataForDate(refDate) : Promise.resolve(null),
    curDate ? _ipcLoadDataForDate(curDate) : Promise.resolve(null)
  ]);
  _ipcSideCache.set('reference', refData);
  _ipcSideCache.set('current', curData);

  const refSide = document.querySelector('.ipc-side[data-side="reference"]');
  const curSide = document.querySelector('.ipc-side[data-side="current"]');
  if (refSide) _ipcRenderSide(refSide, refData, ipQuota, refDate);
  if (curSide) _ipcRenderSide(curSide, curData, ipQuota, curDate);
}
```

- [ ] **Step 4: Manual verification.**

Restart `serve.sh`, open `2099_01_02_auto_tuned`, open the IPC section. Expect:
1. Header reads "Current (2099-01-02)" with ↻ and ⤢ buttons (no actions yet — fine).
2. Two KPI cards: "Total estimated IP count" with `—` value, "Max total IP count" with the actual peak value and timestamp. The MAX card hovers with a pointer cursor.
3. Empty 280px chart wrapper (no canvas content yet — Task 4).
4. Anchor chip is hidden (no `.visible` class).
5. Table shell rendered with empty `<tbody>` and Σ-fleet footer row showing `—`.
6. If skipped clusters exist, chip appears below the chart wrap. Otherwise no chip.
7. If reference date had no data, the reference side (when toggled to Both) shows the empty notice.

Switch the selector to "Both" (after Task 7 it'll work; for now manually flip via dev tools: `document.querySelector('.ipc-pair').dataset.mode = 'both'` — the reference side becomes visible and renders identically).

- [ ] **Step 5: Commit.**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css
git commit -m "$(cat <<'EOF'
adds IPC per-side scaffold (TODO_3 task 3/10)

Renders header, KPI strip with live MAX values, empty chart wrap,
hidden anchor chip, and table shell with Σ-fleet footer per side.
Empty-state path renders the I.2 per-side notice.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Chart construction — stacked datasets + `ipcThresholdsPlugin`

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js`

- [ ] **Step 1: Add the thresholds plugin.**

Append after the helpers in the IPC section:

```javascript
// Plugin: dashed warn/crit/cap lines + tinted "over the cap" zone.
// Reads sideEl._ipcQuota via chart.canvas._ipcSideEl.
const ipcThresholdsPlugin = {
  id: 'ipcThresholds',
  beforeDatasetsDraw(chart) {
    const sideEl = chart.canvas._ipcSideEl;
    if (!sideEl || !sideEl._ipcQuota) return;
    const cap    = +sideEl._ipcQuota.max_ip_count || 256;
    const warnAt = cap * (+sideEl._ipcQuota.warn_pct || 70) / 100;
    const critAt = cap * (+sideEl._ipcQuota.crit_pct || 90) / 100;
    const { ctx, chartArea: { left, right, top, bottom }, scales: { y } } = chart;
    if (!y) return;

    // Tint the [cap, y.max] band when the chart range extends past cap.
    const capPx = y.getPixelForValue(cap);
    if (capPx > top) {
      ctx.save();
      ctx.fillStyle = 'rgba(248,81,73,0.08)';
      ctx.fillRect(left, top, right - left, Math.max(0, capPx - top));
      ctx.restore();
    }

    // Dashed lines and right-flush labels.
    const lines = [
      { v: warnAt, color: '#d4a72c', label: `warn ${Math.round(warnAt)}` },
      { v: critAt, color: '#f85149', label: `crit ${Math.round(critAt)}` },
      { v: cap,    color: '#f85149', label: `cap ${cap}`,  bold: true }
    ];
    ctx.save();
    ctx.setLineDash([4, 3]);
    ctx.font = '10px ui-monospace, monospace';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'bottom';
    lines.forEach(line => {
      const py = y.getPixelForValue(line.v);
      if (!Number.isFinite(py) || py < top || py > bottom) return;
      ctx.lineWidth = line.bold ? 1.5 : 1;
      ctx.strokeStyle = line.color;
      ctx.beginPath();
      ctx.moveTo(left, py);
      ctx.lineTo(right, py);
      ctx.stroke();
      ctx.fillStyle = line.color;
      ctx.fillText(line.label, right - 4, py - 2);
    });
    ctx.restore();
  }
};
```

- [ ] **Step 2: Add `_ipcBuildDatasets` and `_ipcBuildChart`.**

Append:

```javascript
// Convert flat segments into one stepped dataset per cluster, ready for
// Chart.js with stacked y-axis. Adds explicit y=null gap points between
// non-adjacent intervals so step lines break at lifespan boundaries.
function _ipcBuildDatasets(segments) {
  const byCluster = new Map();
  for (const s of segments) {
    if (!byCluster.has(s.cluster)) byCluster.set(s.cluster, []);
    byCluster.get(s.cluster).push(s);
  }
  // Sort each cluster's segments by fromMs.
  for (const segs of byCluster.values()) segs.sort((a, b) => a.fromMs - b.fromMs);

  const datasets = [];
  Array.from(byCluster.keys()).sort().forEach(cluster => {
    const segs = byCluster.get(cluster);
    const points = [];
    segs.forEach((s, i) => {
      // Insert gap point if there's a hole between previous segment's end
      // and this segment's start.
      if (i > 0 && segs[i - 1].toMs < s.fromMs) {
        points.push({ x: segs[i - 1].toMs, y: null });
      }
      points.push({ x: s.fromMs, y: s.ips });
      points.push({ x: s.toMs,   y: s.ips });
    });
    const color = _ipcHashHueForCluster(cluster);
    datasets.push({
      label: cluster,
      data: points,
      parsing: false,
      spanGaps: false,
      stepped: 'before',
      pointRadius: 0,
      pointHoverRadius: 3,
      borderWidth: 0,
      backgroundColor: color.fill,
      borderColor: color.stroke,
      fill: 'origin'
    });
  });
  return datasets;
}

function _ipcBuildChart(canvas, sideData, ipQuota, sideName) {
  if (canvas._chartInstance) {
    try { canvas._chartInstance.destroy(); } catch (e) {}
  }
  const datasets = _ipcBuildDatasets(sideData.segments);
  const cap = +ipQuota.max_ip_count || 256;
  const yMax = Math.max(sideData.peakIps * 1.05, cap * 1.05);

  const chart = new Chart(canvas, {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          position: 'bottom',
          labels: { color: '#c9d1d9', boxWidth: 10, font: { size: 11 } }
        },
        tooltip: {
          callbacks: {
            title: (items) => items[0] ? _ipcFmtHmsUtc(items[0].parsed.x) + 'Z' : '',
            label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y == null ? '—' : ctx.parsed.y} IPs`
          }
        },
        zoom: csZoomPluginConfig(canvas)
      },
      scales: {
        x: {
          type: 'linear',
          stacked: false,
          ticks: {
            color: '#8b949e', maxRotation: 0, autoSkip: true,
            callback: (v) => _ipcFmtHmsUtc(+v)
          },
          grid: { color: 'rgba(139,148,158,0.1)' }
        },
        y: {
          stacked: true,
          beginAtZero: true,
          suggestedMax: yMax,
          title: { display: true, text: 'IPs (workers + master)', color: '#8b949e' },
          ticks: { color: '#8b949e', precision: 0 },
          grid: { color: 'rgba(139,148,158,0.1)' }
        }
      }
    },
    plugins: [ipcThresholdsPlugin]
  });
  canvas._chartInstance = chart;
  return chart;
}
```

- [ ] **Step 3: Wire `_ipcBuildChart` into `_ipcRenderSide`.**

In `_ipcRenderSide`, immediately before the closing `}` of the function, add:

```javascript
  // Build chart now that the canvas is in the DOM.
  const canvas = document.getElementById(canvasId);
  if (canvas) {
    canvas._ipcSideEl = sideEl;
    sideEl._ipcChart = _ipcBuildChart(canvas, sideData, ipQuota, sideName);
  }
```

- [ ] **Step 4: Manual verification.**

Restart `serve.sh`, reload, open the IPC section. Expect:
1. The chart canvas now renders a stacked area chart. One color per cluster; the legend at the bottom lists each cluster.
2. Three dashed horizontal lines: warn (gold, ~179), crit (red, ~230), cap (red, 256).
3. The y-axis extends slightly past 256 so the cap line is visible.
4. Hovering a column shows tooltips with each cluster's IPs at that time.
5. Zoom: scroll wheel zooms x; drag pans (zoom plugin already configured).
6. If peak > 256, the area above 256 is tinted red.
7. No console errors. Open dev tools → check `document.querySelector('.ipc-side[data-side="current"]')._ipcChart` — should be a Chart object with `datasets.length` > 0.

- [ ] **Step 5: Commit.**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js
git commit -m "$(cat <<'EOF'
adds IPC stacked chart + thresholds plugin (TODO_3 task 4/10)

One stepped, stacked dataset per cluster with deterministic
hash-based colors. ipcThresholdsPlugin draws dashed warn/crit/cap
lines and tints the over-cap band. Y-axis suggestedMax keeps the
cap line always visible.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Crosshair plugin + hover/click handlers + KPI/table refresh

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js`

- [ ] **Step 1: Add the crosshair plugin.**

Append after `ipcThresholdsPlugin`:

```javascript
// Plugin: solid (anchored) and dashed (hover) vertical lines from y.top→y.bottom.
const ipcCrosshairPlugin = {
  id: 'ipcCrosshair',
  afterDatasetsDraw(chart) {
    const sideEl = chart.canvas._ipcSideEl;
    if (!sideEl) return;
    const anchorMs = +sideEl.dataset.anchorMs || null;
    const hoverMs = sideEl._ipcHoverMs;
    const { ctx, chartArea: { top, bottom }, scales: { x } } = chart;
    if (!x) return;
    const sideColor = sideEl.dataset.side === 'reference' ? '#58a6ff' : '#f0883e';

    const drawLine = (ms, dashed) => {
      if (!Number.isFinite(ms)) return;
      const px = x.getPixelForValue(ms);
      if (!Number.isFinite(px) || px < chart.chartArea.left || px > chart.chartArea.right) return;
      ctx.save();
      ctx.lineWidth = dashed ? 1 : 2;
      ctx.strokeStyle = dashed ? 'rgba(139,148,158,0.55)' : sideColor;
      ctx.setLineDash(dashed ? [4, 3] : []);
      ctx.beginPath();
      ctx.moveTo(px, top);
      ctx.lineTo(px, bottom);
      ctx.stroke();
      ctx.restore();
    };

    // Hover dashed only when hover != anchor.
    if (hoverMs != null && hoverMs !== anchorMs) drawLine(hoverMs, true);
    if (anchorMs != null) drawLine(anchorMs, false);
  }
};
```

Register the plugin on the chart in `_ipcBuildChart`. Edit the `plugins: [ipcThresholdsPlugin]` line to:

```javascript
    plugins: [ipcThresholdsPlugin, ipcCrosshairPlugin]
```

- [ ] **Step 2: Add the refresh function.**

Append:

```javascript
// Single source of truth for "what time is the table/KPI reading?"
// Fallback chain: anchorMs ?? hoverMs ?? peakMs (D.1).
function _ipcResolveActiveMs(sideEl) {
  const a = +sideEl.dataset.anchorMs;
  if (Number.isFinite(a) && a > 0) return a;
  if (Number.isFinite(sideEl._ipcHoverMs)) return sideEl._ipcHoverMs;
  const sd = sideEl._ipcData;
  return sd && Number.isFinite(sd.peakMs) ? sd.peakMs : null;
}

function _ipcRefreshSide(sideEl) {
  const sd = sideEl._ipcData; const ipQuota = sideEl._ipcQuota;
  if (!sd || !ipQuota) return;
  const t = _ipcResolveActiveMs(sideEl);
  const rows = (t == null) ? [] : _ipcEvalAt(sd.segments, t);
  const fleetTotal = rows.reduce((s, r) => s + r.ips, 0);
  const fleetCost  = rows.reduce((s, r) => s + r.segCostEur, 0);

  // KPI total box.
  const totalEl = sideEl.querySelector('.ipc-kpi-total');
  if (totalEl) {
    const cap = +ipQuota.max_ip_count || 256;
    const cls = _ipcThresholdLevel(fleetTotal, ipQuota);
    totalEl.classList.remove('warn', 'crit', 'over');
    if (cls) totalEl.classList.add(cls);
    totalEl.querySelector('.ipc-kpi-value').textContent = `${fleetTotal} IPs`;
    totalEl.querySelector('.ipc-kpi-sub').textContent =
      `${fleetTotal} of ${cap} · ${Math.round(100 * fleetTotal / cap)}%`;
  }

  // Anchor chip.
  const chip = sideEl.querySelector('.ipc-anchor-chip');
  const anchorMs = +sideEl.dataset.anchorMs;
  if (chip) {
    if (Number.isFinite(anchorMs) && anchorMs > 0) {
      chip.classList.add('visible');
      const noneTag = rows.length === 0 ? ' (no clusters alive)' : '';
      chip.querySelector('.ipc-anchor-text').textContent =
        `Anchored at ${_ipcFmtHmsUtc(anchorMs)}Z${noneTag}`;
    } else {
      chip.classList.remove('visible');
    }
  }

  // Table body.
  const tbody = sideEl.querySelector('.ipc-table tbody');
  if (tbody) {
    if (rows.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7" class="empty-msg" style="padding:16px;">No clusters alive at this moment.</td></tr>`;
    } else {
      tbody.innerHTML = rows.map(r => `
        <tr data-cluster="${escapeAttr(r.cluster)}">
          <td>${escapeHtml(r.cluster)}</td>
          <td>${r.workers}</td>
          <td>${r.master}</td>
          <td>${r.ips}</td>
          <td><code>${escapeHtml(r.machineType)}</code></td>
          <td>€${r.segCostEur.toFixed(2)}</td>
          <td>${r.idx}</td>
        </tr>`).join('');
    }
  }
  const tfTotal = sideEl.querySelector('.ipc-tfoot-total');
  const tfCost  = sideEl.querySelector('.ipc-tfoot-cost');
  if (tfTotal) tfTotal.textContent = String(fleetTotal);
  if (tfCost)  tfCost.textContent  = `€${fleetCost.toFixed(2)}`;

  // Re-draw chart so crosshair plugin picks up new state.
  if (sideEl._ipcChart) sideEl._ipcChart.update('none');
}
```

- [ ] **Step 3: Wire hover and click handlers.**

In `_ipcRenderSide`, replace the chart-build block from Task 4 with:

```javascript
  // Build chart now that the canvas is in the DOM.
  const canvas = document.getElementById(canvasId);
  if (canvas) {
    canvas._ipcSideEl = sideEl;
    sideEl._ipcChart = _ipcBuildChart(canvas, sideData, ipQuota, sideName);

    // Hover → set _ipcHoverMs and refresh.
    canvas.addEventListener('mousemove', (evt) => {
      const ms = sideEl._ipcChart.scales.x.getValueForPixel(evt.offsetX);
      sideEl._ipcHoverMs = Number.isFinite(ms) ? Math.round(ms) : null;
      _ipcRefreshSide(sideEl);
    });
    canvas.addEventListener('mouseleave', () => {
      sideEl._ipcHoverMs = null;
      _ipcRefreshSide(sideEl);
    });

    // Click → anchor at clicked x.
    canvas.addEventListener('click', (evt) => {
      const ms = sideEl._ipcChart.scales.x.getValueForPixel(evt.offsetX);
      if (!Number.isFinite(ms)) return;
      sideEl.dataset.anchorMs = String(Math.round(ms));
      _ipcRefreshSide(sideEl);
    });

    // Anchor chip ✕ → clear anchor.
    const clearBtn = sideEl.querySelector('.ipc-anchor-clear');
    if (clearBtn) clearBtn.addEventListener('click', () => {
      sideEl.dataset.anchorMs = '';
      _ipcRefreshSide(sideEl);
    });

    // Initial paint at peak (anchorMs and hoverMs both null → fallback to peakMs).
    _ipcRefreshSide(sideEl);
  }
```

- [ ] **Step 4: Manual verification.**

Restart `serve.sh`, reload. Expect:
1. On first paint, the TOTAL KPI box reads the peak fleet IPs (because of the D.1 fallback chain). The table shows alive clusters at the peak time, sorted by IPs desc.
2. Move mouse over the chart: a dashed gray vertical line tracks; the TOTAL KPI updates live; the table re-sorts to show alive clusters at the hover time.
3. Move mouse off the chart: dashed line disappears; TOTAL falls back to peak.
4. Click on the chart at any x: a solid orange (current) or blue (reference) line appears; the anchor chip "Anchored at HH:MM:SSZ" shows; the ✕ button is visible.
5. Click ✕: anchor clears; TOTAL falls back to MAX; chip hides.
6. With anchor set, hovering elsewhere shows BOTH lines (solid anchor + dashed hover).
7. Click a different x: anchor jumps; TOTAL/table reflect the new x.
8. If you click a gap (between incarnations), the chip says "Anchored at HH:MM:SSZ (no clusters alive)" and the table shows the empty row.

- [ ] **Step 5: Commit.**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js
git commit -m "$(cat <<'EOF'
adds IPC crosshair + KPI/table refresh (TODO_3 task 5/10)

ipcCrosshairPlugin draws solid anchored + dashed hover lines.
mousemove/click handlers update sideEl state and trigger
_ipcRefreshSide which re-paints the TOTAL KPI box, anchor chip,
table body, and Σ-fleet footer. Initial paint uses MAX fallback.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: MAX-box click-to-jump + ↻ reset + ⤢ side-expand

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js`
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css`

- [ ] **Step 1: Add expand-mode CSS.**

Append to the IPC CSS block:

```css
/* When one side is expanded, hide the other and expand this one to full width. */
.ipc-pair[data-expanded] .ipc-side { width: 100%; }
.ipc-pair[data-expanded="reference"] { grid-template-columns: 1fr; }
.ipc-pair[data-expanded="current"]   { grid-template-columns: 1fr; }
```

- [ ] **Step 2: Wire MAX click + ↻ + ⤢ in `_ipcRenderSide`.**

In `_ipcRenderSide`, after the existing canvas/click wiring from Task 5, add:

```javascript
    // MAX KPI click-to-jump (C.1).
    const maxBox = sideEl.querySelector('.ipc-kpi-max');
    if (maxBox && Number.isFinite(sideData.peakMs)) {
      maxBox.addEventListener('click', () => {
        sideEl.dataset.anchorMs = String(sideData.peakMs);
        _ipcRefreshSide(sideEl);
      });
    }

    // ↻ reset: clear zoom AND clear anchor.
    const resetBtn = sideEl.querySelector('.ipc-side-reset');
    if (resetBtn) resetBtn.addEventListener('click', () => {
      try { sideEl._ipcChart.resetZoom(); } catch (e) {}
      sideEl.dataset.anchorMs = '';
      _ipcRefreshSide(sideEl);
    });

    // ⤢ side-expand: toggle .ipc-pair[data-expanded].
    const expandBtn = sideEl.querySelector('.ipc-side-expand');
    if (expandBtn) expandBtn.addEventListener('click', () => {
      const pair = sideEl.parentElement;
      const which = sideEl.dataset.side;
      const cur = pair.dataset.expanded || '';
      pair.dataset.expanded = (cur === which) ? '' : which;
      expandBtn.textContent = pair.dataset.expanded === which ? '⤡' : '⤢';
      // Resize chart after layout settles.
      setTimeout(() => { try { sideEl._ipcChart.resize(); } catch (e) {} }, 50);
    });
```

- [ ] **Step 3: Manual verification.**

Restart `serve.sh`. Expect:
1. Click the MAX KPI box: anchor jumps to the peak timestamp; solid line appears at the peak; chip + table reflect.
2. Click ↻: zoom resets (if you previously zoomed) AND anchor clears; chip hides.
3. Click ⤢ on the current side (after switching to "Both" mode via dev-tools or the upcoming Task 7): the reference side hides; the current side spans full width; the button glyph becomes ⤡.
4. Click ⤡: both sides reappear in 1fr 1fr; glyph returns to ⤢.

- [ ] **Step 4: Commit.**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css
git commit -m "$(cat <<'EOF'
adds IPC MAX-jump, reset, side-expand (TODO_3 task 6/10)

MAX KPI click anchors at peak timestamp. Reset (↻) clears zoom
and anchor. Side-expand (⤢) toggles ipc-pair[data-expanded] and
calls chart.resize after layout settles.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Date selector toggle (Reference / Current / Both) + URL hash persistence

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js`

- [ ] **Step 1: Add the toggle wiring.**

Append after `_ipcRefreshSide`:

```javascript
// Date-selector mode persisted to URL hash as `ipc=<mode>` so reload keeps it.
function _ipcReadModeFromHash() {
  try {
    const h = window.location.hash || '';
    const m = h.match(/(?:^|[#&])ipc=(reference|current|both)\b/);
    return m ? m[1] : null;
  } catch (e) { return null; }
}
function _ipcWriteModeToHash(mode) {
  try {
    const h = (window.location.hash || '').replace(/(^#|^|&)ipc=(reference|current|both)\b&?/, '');
    let next = h;
    if (next && !next.startsWith('#')) next = '#' + next;
    if (!next) next = '#';
    if (next === '#') next = `#ipc=${mode}`;
    else next = `${next}${next.endsWith('&') ? '' : '&'}ipc=${mode}`;
    window.history.replaceState(null, '', next);
  } catch (e) {}
}

function _ipcApplyMode(mode) {
  const pair = document.querySelector('#ipc-body .ipc-pair');
  if (!pair) return;
  pair.dataset.mode = mode;
  pair.removeAttribute('data-expanded'); // mode change resets any per-side expand
  document.querySelectorAll('#ipc-date-toggle .seg').forEach(btn => {
    const active = btn.dataset.side === mode;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-selected', active ? 'true' : 'false');
  });
  // Resize charts after layout settles (mode change can swap 1fr ↔ 1fr 1fr).
  setTimeout(() => {
    document.querySelectorAll('.ipc-side').forEach(s => {
      try { s._ipcChart && s._ipcChart.resize(); } catch (e) {}
    });
  }, 50);
}

function _ipcWireToggle() {
  const toggle = document.getElementById('ipc-date-toggle');
  if (!toggle || toggle.dataset.wired === '1') return;
  toggle.dataset.wired = '1';
  toggle.addEventListener('click', (evt) => {
    const btn = evt.target.closest('.seg[data-side]');
    if (!btn) return;
    const mode = btn.dataset.side;
    _ipcApplyMode(mode);
    _ipcWriteModeToHash(mode);
  });
}
```

- [ ] **Step 2: Call the toggle wirer + apply hash mode after first render.**

At the end of `_renderClusterIpCountSection`, after both sides are rendered, add:

```javascript
  _ipcWireToggle();
  const initialMode = _ipcReadModeFromHash() || 'current';
  _ipcApplyMode(initialMode);
```

- [ ] **Step 3: Manual verification.**

Restart `serve.sh`. Expect:
1. Default state: "Current" segment is active; only the current side renders.
2. Click "Reference": only the reference side shows (with its own KPI box, chart, table). The URL hash now contains `ipc=reference`.
3. Reload the page and re-open the IPC section: the "Reference" mode is restored from hash.
4. Click "Both": both sides side-by-side. Each side's crosshair is independent (hover one, the other's KPI/table do NOT update).
5. Click ⤢ on the current side: reference hides; current spans full width.
6. Click "Reference" mode while expanded: any expand state clears and reference is shown.
7. URL hash never accumulates duplicates after multiple toggles.

- [ ] **Step 4: Commit.**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js
git commit -m "$(cat <<'EOF'
adds IPC date-selector toggle + URL hash persistence (TODO_3 task 7/10)

Reference | Current | Both segmented control rewires
ipc-pair[data-mode] which CSS uses to show/hide sides. Mode
persists via window.location.hash so reload keeps the choice.
Mode change clears any per-side expand state and resizes charts.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Table click-to-highlight (K.1) + sticky header validation

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js`

- [ ] **Step 1: Add row-click handler in `_ipcRefreshSide`.**

In `_ipcRefreshSide`, after the `tbody.innerHTML = ...` block (the rows path, not the empty path), add immediately after the `tbody.innerHTML` assignment:

```javascript
      tbody.querySelectorAll('tr[data-cluster]').forEach(tr => {
        tr.addEventListener('click', () => {
          const name = tr.dataset.cluster;
          const wasActive = tr.classList.contains('ipc-row-active');
          // Clear active state on all rows + all dataset borders.
          tbody.querySelectorAll('tr.ipc-row-active').forEach(r => r.classList.remove('ipc-row-active'));
          if (sideEl._ipcChart) {
            sideEl._ipcChart.data.datasets.forEach(ds => { ds.borderWidth = 0; });
          }
          // Toggle: if it WAS active, leave all cleared; otherwise activate this one.
          if (!wasActive) {
            tr.classList.add('ipc-row-active');
            if (sideEl._ipcChart) {
              const ds = sideEl._ipcChart.data.datasets.find(d => d.label === name);
              if (ds) ds.borderWidth = 2;
            }
          }
          if (sideEl._ipcChart) sideEl._ipcChart.update('none');
        });
      });
```

Note: this code runs inside `_ipcRefreshSide(sideEl)`, so `sideEl` is in scope via the function parameter and the row click-handler closes over it correctly.

- [ ] **Step 2: Manual verification.**

Restart `serve.sh`. Expect:
1. Click any table row: the row highlights (light-blue background) AND that cluster's stack band gets a thicker outline on the chart.
2. Click the same row again: highlight clears; outline returns to 0.
3. Click a different row: previous row's highlight clears; new row's highlights. Only one row active at a time.
4. Move the crosshair to a new time (rebuilds the rows): row click handlers are re-attached because `_ipcRefreshSide` re-renders the tbody.
5. Scroll the table-wrap (when ≥ ~10 rows visible): the `<thead>` row stays sticky at the top. (Force this by setting `.ipc-table-wrap { max-height: 80px }` temporarily in dev tools to confirm scroll behavior. Restore after.)

- [ ] **Step 3: Commit.**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js
git commit -m "$(cat <<'EOF'
adds IPC table click-to-highlight (TODO_3 task 8/10)

Clicking a table row highlights the row and bumps the matching
dataset's borderWidth so the cluster's stack band is outlined on
the chart. Toggle behavior; one row active at a time. Sticky
table header verified to survive scrolling.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Edge cases — no b20 entire date, B.1 fallback, C.1 no cost_timeline, gap anchor

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js`

The earlier tasks already implement the empty-state path (Task 3 step 2), the gap-anchor message (Task 5 step 2), and the skipped-clusters chip (Task 3 step 2). This task adds the B.1 fallback tag and the section-level "(no data)" suffix.

- [ ] **Step 1: Add the `b23` fallback tag in the table.**

In `_ipcRefreshSide`, change the row template (the `tbody.innerHTML = rows.map(r => ...)` block) to:

```javascript
      tbody.innerHTML = rows.map(r => {
        // A row is on the b23 fallback path when the segment spans the cluster's
        // full active window — i.e. there were no b21 events. Detect: this segment
        // is the only segment for that cluster across the whole side.
        const ownSegs = sd.segments.filter(s => s.cluster === r.cluster);
        const isB23 = ownSegs.length === 1;
        const tag = isB23 ? `<span class="ipc-tag-fallback" title="No autoscaler events; using recommended worker count">b23</span>` : '';
        return `
        <tr data-cluster="${escapeAttr(r.cluster)}">
          <td>${escapeHtml(r.cluster)}${tag}</td>
          <td>${r.workers}</td>
          <td>${r.master}</td>
          <td>${r.ips}</td>
          <td><code>${escapeHtml(r.machineType)}</code></td>
          <td>€${r.segCostEur.toFixed(2)}</td>
          <td>${r.idx}</td>
        </tr>`;
      }).join('');
```

Note the `sd` variable: this code lives inside `_ipcRefreshSide` where `sd = sideEl._ipcData` is already declared at the top of the function. Use that variable directly.

Also: the b23-detection here is a heuristic — a single segment across one incarnation does NOT necessarily mean no b21 (a cluster with one b21 event creates two segments; a cluster with zero b21 events creates one). Refine by counting unique workers values per cluster: if all segments for the cluster share the same `workers` value, that's a strong b23 signal. Replace `const isB23 = ownSegs.length === 1;` with:

```javascript
        const isB23 = ownSegs.length > 0 &&
          ownSegs.every(s => s.workers === ownSegs[0].workers);
```

- [ ] **Step 2: Section "(no data)" suffix when both sides are empty.**

In `_renderClusterIpCountSection`, after `_ipcSideCache.set('current', curData);`, add:

```javascript
  const summary = document.querySelector('#cluster-ip-count-section > summary > h3');
  if (summary) {
    const noRef = !refData || refData.segments.length === 0;
    const noCur = !curData || curData.segments.length === 0;
    summary.textContent = noRef && noCur
      ? 'Total estimated IP count (no data)'
      : 'Total estimated IP count';
  }
```

- [ ] **Step 3: Manual verification.**

For each scenario:

**(a) Normal data (default `2099_01_02_auto_tuned`):**
- Section title remains "Total estimated IP count" (no suffix).
- Tables show `b23` chip on rows for clusters whose `cost_timeline` has only flat-worker intervals.

**(b) Cluster with no b21 events (B.1):**
- Inspect `outputs/2099_01_01/mock-cluster-002-manually-tuned.json`. mock-cluster-002 has no b21 events on `2099-01-01`, so its `cost_timeline.intervals[]` should contain a single interval at `workers=10`.
- In the IPC section for that date, `mock-cluster-002` row should show `<span class="ipc-tag-fallback">b23</span>` next to the cluster name.

**(c) Cluster with no b20 span (C.1) — synthetic:**
- Force the case in dev tools: `window.__forcedSkip = ['mock-cluster-001']` and patch `_ipcLoadDataForDate` to skip those names. Or simply rename a `*-manually-tuned.json` to invalidate its `cost_timeline`. The `.ipc-skipped-chip` should then list the affected cluster names.

**(d) Both sides empty (no b20 anywhere):**
- Force the case: temporarily set `data.metadata.reference_date = '1970-01-01'` and `current_date = '1970-01-02'` in dev tools, then re-call `_renderClusterIpCountSection()`.
- Section summary now reads "Total estimated IP count (no data)".
- Both sides show "No autoscaling data exported for this date."

- [ ] **Step 4: Commit.**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js
git commit -m "$(cat <<'EOF'
adds IPC edge-case handling (TODO_3 task 9/10)

Tags table rows on the b23 fallback path (uniform workers across
all intervals). Adds "(no data)" suffix to the section summary
when both sides are empty.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Acceptance gate — full end-to-end UX walkthrough

**Files:**
- No code changes. This task is pure verification against the spec §11.

- [ ] **Step 1: Make `2099_01_01` openable from the dashboard.**

The dashboard's landing page lists analyses via `_analyses_index.json`. Confirm `2099_01_01` is listed there (the run we hand-verified in spec §11.1):

```bash
grep -A2 '"2099_01_01"' src/main/resources/composer/dwh/config/cluster_tuning/outputs/_analyses_index.json | head -10
```

If it's not listed, open `outputs/2099_01_02_auto_tuned/_auto_tuner_analysis.json` (which uses `2099_01_01` as its reference date) and run the dashboard from there — both sides become available via the Both selector.

- [ ] **Step 2: Run the dashboard.**

```bash
cd src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend
./serve.sh
```

Open `2099_01_02_auto_tuned`.

- [ ] **Step 3: Walk through the spec §11.2 UX checks.**

Tick each:

- [ ] (a) Default load: section closed; `#ipc-body` content rendered (since we eagerly load data) but `<details>` closed.
- [ ] (b) Open section → chart appears within ~200ms; selector shows "Current" active.
- [ ] (c) Hover the chart: dashed gray crosshair tracks; TOTAL KPI updates live; table re-sorts to show alive clusters at hover time.
- [ ] (d) Click chart: solid orange (current side) crosshair; anchor chip "Anchored at HH:MM:SSZ ✕" visible; table refreshes.
- [ ] (e) Click MAX KPI box: anchor jumps to peak timestamp; solid line moves; chip + table update.
- [ ] (f) Click ✕ on chip: anchor cleared; TOTAL falls back to MAX peak value; chip hides.
- [ ] (g) Switch to "Reference": only reference side visible; its crosshair is BLUE (#58a6ff).
- [ ] (h) Switch to "Both": both sides side-by-side; independent crosshairs (hover one — the other's TOTAL stays unchanged).
- [ ] (i) Click ⤢ on one side: other hides; chart resizes within ~50ms; glyph becomes ⤡.
- [ ] (j) Click ⤡: both sides return.
- [ ] (k) ↻ reset: zoom + anchor cleared.
- [ ] (l) Threshold lines: warn at 179, crit at 230, cap at 256 (or proportional to your `ipQuota.max_ip_count`). Force the over-cap palette by editing `config.json` `max_ip_count: 30` and reloading; the over-cap zone tints red and the MAX KPI box gets the `.over` class.
- [ ] (m) Click a table row: cluster band outlined on chart; row highlighted.

- [ ] **Step 4: Walk through the spec §11.1 acceptance gate (hand-verified totals).**

Open the Both selector. On the side rendering `2099_01_01` (which is the reference date when current = `2099_01_02`), in dev tools:

```javascript
const ref = document.querySelector('.ipc-side[data-side="reference"]');
const sd = ref._ipcData;
[
  ['2099-01-01T03:30:00Z', 25],
  ['2099-01-01T05:45:00Z', 34],
  ['2099-01-01T12:00:00Z', 17]
].forEach(([t, expected]) => {
  const ms = Date.parse(t);
  const rows = _ipcEvalAt(sd.segments, ms);
  const total = rows.reduce((s, r) => s + r.ips, 0);
  console.log(t, 'got', total, 'expected', expected, total === expected ? 'PASS' : 'FAIL');
});
```

All three must print `PASS`. If any fails, fix `_ipcBuildSegments` / `_ipcEvalAt` before continuing — the math IS the feature.

- [ ] **Step 5: Walk through spec §11.3 accessibility / responsive.**

- [ ] At 1280px width (default monitor), Both mode renders comfortably; KPI cards wrap if needed.
- [ ] At 1024px width (laptop) — resize browser — Both mode still usable; charts shrink; selector remains accessible.
- [ ] Sticky table header visible when the table overflows (force via dev tools `max-height: 100px`).
- [ ] Press Tab: focus moves through the segmented toggle buttons; arrow-key navigation NOT required (skip per spec non-goals).

- [ ] **Step 6: Final commit (clean-up + summary commit).**

If any small fixes were needed during the acceptance gate, batch them now:

```bash
git add -p src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/
git commit -m "$(cat <<'EOF'
finalizes IPC timeline acceptance (TODO_3 task 10/10)

Acceptance gate from spec §11 passed: §11.1 hand-verified totals
(25 / 34 / 17), §11.2 UX walkthrough, §11.3 responsive at
1280/1024px. TODO_3 complete — see
docs/superpowers/specs/2026-05-01-ipc-timeline-design.md.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

If no fixes were needed, skip the commit.

---

## Notes for the implementer

1. **Don't touch unrelated uncommitted changes.** The branch `oss-ready-mock-v1` already has a large amount of uncommitted work outside the IPC scope. `git status` will show ~100 modified files; only stage the four IPC-relevant files in `auto/frontend/` for each commit.
2. **`escapeHtml` and `escapeAttr` are pre-existing helpers in `app.js`.** Use them — don't reimplement.
3. **`csZoomPluginConfig(canvas)` is pre-existing.** It returns the zoom-plugin config block (zoom, pan, mode `xy`). Reuse verbatim, like `dcc-side` does.
4. **`config` and `data` are module-level globals in `app.js`.** No imports needed.
5. **Chart.js is loaded via `<script>` in `index.html`.** No `import Chart from 'chart.js'` — it's a global.
6. **Pre-existing CSS variables (none).** The codebase uses raw hex values; match that style.
7. **Commits are tiny on purpose.** Frequent commits give clean diffs for review and let `git bisect` localize a regression.
