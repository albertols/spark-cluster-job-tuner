# Total Estimated IP Count timeline (TODO_3)

**Date:** 2026-05-01
**Module:** `auto/frontend` (Spark Cluster Auto-Tuner Dashboard)
**Scope:** Frontend-only feature. No Scala/backend changes — all required data is already
emitted by the cluster tuner as `cost_timeline` inside per-cluster JSON outputs.

## 1. Background

The 3-TODO plan (TODO_1 cost calc, TODO_2 `detail-cluster-cost`, TODO_3 fleet IP-count
timeline) was kicked off in a `/plan` session. Verification (May 2026) confirms TODO_1 and
TODO_2 are correct and shipped:

- **TODO_1**: `_clusters-summary*.csv` carries `worker_machine_type`, `master_machine_type`,
  `real_used_avg_num_of_workers`, `real_used_min_workers`, `real_used_max_workers`,
  `total_active_minutes`, `estimated_cost_eur`. Interval-based cost from b20 spans + b21
  events; master priced for full span. Hand-verified on `mock-cluster-001` (sum of segment
  costs = 92.16 ✓; time-weighted avg = 4.33 ✓).
- **TODO_2**: `<details id="detail-cluster-cost">` between `detail-cluster-conf` and
  `detail-recipes`, defaults closed. KPI delta strip, side-by-side stepped chart with
  create/delete markers, per-incarnation lifespan table, ⤢ side-expand, ↻ reset, zoom/pan.
- **TODO_3**: only an empty stub `<details id="cluster-ip-count-section">` exists in
  `index.html:53–59` ("Expand to load…") above Cluster Summary Graphs. This spec covers it.

## 2. Goal

Above the existing "Cluster Summary Graphs" section in the Fleet Overview tab, add an
**expandable** "Total estimated IP count" section that surfaces the fleet's cumulative IP
demand (workers + masters) over time. Operators use it to answer "are we close to the
project's IP quota at any point in the day?" and "which cluster is responsible for that
peak?".

The section is **collapsed by default** (lazy-renders on first open). When opened it
shows, per side (reference and/or current date):

- A stacked-area chart of per-cluster IP counts over time.
- Cap/warn/crit threshold lines (`ipQuota` from `config.json`, default cap 256).
- Two KPI boxes — `TOTAL_ESTIMATED_IP_COUNT` (live, follows crosshair) and
  `MAX_TOTAL_IP_COUNT` (peak across the visible window, click-to-jump).
- A live + anchored crosshair pair. Click the chart to anchor; click again or click the
  ✕ chip to clear. The anchor enables scrolling the table without losing the time point.
- A table beneath the chart showing per-cluster IPs at the crosshair's time, sorted by
  IPs descending, with a fleet-total footer row.
- Zoom / pan / reset (Chart.js zoom plugin, already used by `detail-cluster-cost`).

A segmented selector (`Reference | Current | Both`, default `Current`) toggles which
side(s) render. In `Both` mode the two sides sit side-by-side with independent
crosshairs, and each side has a ⤢ expand button (mirrors `detail-cluster-cost`).

## 3. Non-goals

- No keyboard navigation on the chart.
- No drag-to-select time ranges. Anchor is a single point.
- No persistence of the anchor in URL hash (selector mode is persisted; anchor is ephemeral).
- No backend or CSV-format changes. Source of truth is `cost_timeline` in per-cluster JSON.
- No HA-master support yet. Master = +1 IP per alive cluster, hardcoded.

## 4. Decisions (locked during brainstorming)

| # | Topic | Decision |
|---|---|---|
| Q2 | Rendering style | **A** — stacked area, fleet sum is top of stack. |
| Q3 | Both-mode layout | **A** — side-by-side panels, mirror `detail-cluster-cost`. |
| Q4.1 | Worker count when b21 events exist | **A.1** — step function from b21 (already in `cost_timeline.intervals[]`). |
| Q4.2 | Worker count, b20 span exists, no b21 events | **B.1** — `clusterPlan.workers` flat across span (matches b23 cost fallback). |
| Q4.3 | Cluster has no b20 span | **C.1** — cluster contributes 0; never alive on the timeline. |
| Q4.4 | Master IP | **D.1** — always +1 while alive. |
| Q4.5 | Time outside cluster's span | **E.1** — band of 0 thickness; cluster keeps its color in legend. |
| Q5.A | Crosshair behavior | **A.3** — both lines visible: anchored solid, hover dashed. |
| Q5.B | Table content | **B.3** — alive clusters only, sorted by IPs desc, with fleet-total footer row. |
| Q5.C | `MAX_TOTAL_IP_COUNT` box | **C.1** — peak of fleet sum, shows timestamp, click-to-jump. |
| Q5.D | `TOTAL_ESTIMATED_IP_COUNT` box | **D.1** — anchor → hover → MAX fallback chain. |
| Q5.E | Threshold visualization | **E.1** — dashed warn/crit lines, area above cap tinted red, y-axis suggestedMax = `max(observed * 1.05, cap * 1.05)`. |
| Q6.F | Data source | **F.1** — re-use per-cluster JSON outputs (`cost_timeline`). |
| Q6.G | Date selector | **G.1** — segmented `Reference | Current | Both`, default `Current`, persisted to URL hash. |
| Q6.H | Cluster colors | **H.1** — deterministic palette by cluster name (hash → hue), legend toggle visibility, double-click isolates. |
| Q6.I | Empty / degraded states | **I.2** — per-side empty-state notices; the other side renders normally. |
| Q6.J | `<details>` default state | **J.1** — closed by default, body lazy-rendered on first open. |
| Q6.K | Sticky table behavior | **K.1** — sticky header; clicking a row highlights that cluster's band on the chart. |

## 5. Architecture

Frontend-only. Files touched:

| File | Change |
|---|---|
| `auto/frontend/index.html` | Replace stub at lines 53–59. Outer `<details id="cluster-ip-count-section">` stays. `#ipc-body` gets a skeleton: selector strip, `.ipc-pair` with two `.ipc-side` sections. |
| `auto/frontend/app.js` | New section `// ── Cluster IP Count Timeline ──` (~250–350 LOC). Public entry `renderClusterIpCountSection()`. Internals listed in §6. |
| `auto/frontend/style.css` | New rules: `.ipc-controls`, `.ipc-pair`, `.ipc-side`, `.ipc-kpi[.warn|.crit|.over]`, `.ipc-table`, `.ipc-table thead.sticky`, `.ipc-side-expand`, `.ipc-anchor-chip`, threshold-tint band. Reuse existing zoom-plugin config from the `dcc` (detail-cluster-cost) section but do **not** extend `dcc-*` classes — keep the two domains independent. |
| `auto/frontend/config.json` | No change. `ipQuota.{max_ip_count, warn_pct, crit_pct}` already present. |

### 5.1 Data flow

1. **Lazy trigger**: `<details id="cluster-ip-count-section">.toggle` fires on first open.
   `renderClusterIpCountSection()` is called once and cached. Subsequent toggles only
   show/hide the existing DOM.
2. **For each side that needs rendering**, the function:
   1. Reads the date from existing fleet-overview state (the dashboard already exposes
      reference/current dates).
   2. Resolves the cluster list for the date by walking the existing per-cluster JSON
      cache (already populated for the cluster grid + `detail-cluster-cost`). For each
      cluster JSON missing in the cache, fetches `<outputsPath>/<date>/<cluster>-manually-tuned.json`
      using the same fetch helper used by `_ctOf` consumers.
   3. Builds segments via `_buildIpcSegments(perClusterJsons)` (§6.2).
   4. Computes the side cache: `{ segments, eventTimes, peakMs, peakIps, ipQuota }`.
   5. Constructs the Chart.js instance and renders the KPI strip + table.
3. **Selector change** (G.1): toggle `.ipc-pair[data-mode]` and per-side `display:none`.
   Cached charts stay alive — re-toggling is cheap.

### 5.2 Both-mode

Two independent calls of the per-side render pipeline into
`.ipc-side[data-side="reference"]` and `.ipc-side[data-side="current"]`. Each side owns
its own crosshair, anchor, KPI boxes and table. ⤢ side-expand reuses the existing
`dcc-pair[data-expanded]` pattern.

## 6. Components

### 6.1 DOM skeleton

```html
<details id="cluster-ip-count-section" class="cs-graphs">
  <summary>
    <h3>Total estimated IP count</h3>
    <span class="cs-graphs-help">Workers + master IPs over time · stacked per cluster · click chart to anchor crosshair</span>
  </summary>
  <div id="ipc-body">
    <div class="ipc-controls">
      <div class="seg-toggle" id="ipc-date-toggle">
        <button class="seg" data-side="reference">Reference</button>
        <button class="seg active" data-side="current">Current</button>
        <button class="seg" data-side="both">Both</button>
      </div>
      <span class="ipc-cap-note">IP cap <code>256</code> · warn ≥70% · crit ≥90% (configurable in <code>config.json</code> → <code>ipQuota</code>)</span>
    </div>
    <div class="ipc-pair" data-mode="current">
      <section class="ipc-side" data-side="reference"></section>
      <section class="ipc-side" data-side="current"></section>
    </div>
  </div>
</details>
```

### 6.2 Per-side render (`_renderIpc(sideEl, side, segments, ipQuota, dateLabel)`)

Renders into `<section class="ipc-side">`:

```
header (date label · ↻ reset · ⤢ expand)
KPI strip:
  ┌ TOTAL_ESTIMATED_IP_COUNT ┐  ┌ MAX_TOTAL_IP_COUNT ┐
  │ 187 IPs · 73% · [warn]   │  │ 214 @ 14:32 [warn]   │
chart wrap (canvas + threshold lines + crosshair plugin)
anchor chip (hidden until first click): "Anchored at HH:MM:SSZ  [✕]"
table:
  thead.sticky: Cluster | W | M | IPs | Machine | Cost/seg | inc#
  tbody: rows sorted IPs desc (only alive clusters at t)
  tfoot: Σ fleet | — | — | 33 | — | €8.92 | —
```

### 6.3 Public functions in `app.js`

```js
function renderClusterIpCountSection()        // entry, called on first <details> open
function _loadIpcDataForDate(date)            // returns Promise<{ segments, eventTimes, peakMs, peakIps }>
function _buildIpcSegments(perClusterJsons)   // walks cost_timeline.incarnations[].intervals[]
function _evalIpcAt(segments, t)              // O(N log M) lookup; returns sorted [{cluster, workers, master, ips, machineType, segCost, idx}]
function _renderIpc(sideEl, side, sideData, ipQuota, dateLabel)
function _refreshIpcSide(sideEl)              // re-paints KPI + table after anchor/hover change
function _paintIpcKpi(sideEl, fleetTotal, ipQuota, kind)  // kind: 'live'|'max'
function _paintIpcTable(sideEl, rows, fleetTotal)
function _hashHueForCluster(name)             // deterministic color
```

### 6.4 Chart.js plugins

- **`ipcThresholdsPlugin`** — `beforeDatasetsDraw`: tints the band `[cap, y.max]` red and
  draws dashed warn/crit/cap lines with right-flush labels.
- **`ipcCrosshairPlugin`** — `afterDatasetsDraw`: reads `sideEl.dataset.anchorMs` (anchor)
  and `sideEl._ipcHoverMs` (live hover, set by the canvas `mousemove` handler in §7.1);
  draws solid (anchored) and dashed (hover) vertical lines spanning `y.top → y.bottom`.
  Side colors: reference `#58a6ff`, current `#f0883e`.

### 6.5 Datasets

One stacked dataset per cluster:

```js
{
  label: clusterName,
  data: pointsForCluster,   // [{x: ms, y: ips}, ...]
  borderColor: hslColor,
  backgroundColor: hslColor + '99',  // ~60% opacity for stack
  fill: 'origin',
  parsing: false,
  spanGaps: false,
  stepped: 'before',         // matches autoscaler step semantics
  pointRadius: 0,
  stack: 'fleet'
}
```

`pointsForCluster` includes explicit `{x, y: null}` gap points between non-adjacent
incarnations so `spanGaps: false` produces visible breaks.

## 7. Interaction behaviour

### 7.1 Hover

A `mousemove` listener on the canvas mirrors the click handler in §7.2:

```js
canvas.addEventListener('mousemove', (evt) => {
  const ms = chart.scales.x.getValueForPixel(evt.offsetX);
  sideEl._ipcHoverMs = Number.isFinite(ms) ? Math.round(ms) : null;
  _refreshIpcSide(sideEl);
});
canvas.addEventListener('mouseleave', () => {
  sideEl._ipcHoverMs = null;
  _refreshIpcSide(sideEl);
});
```

Chart.js's tooltip (configured with `interaction: { mode: 'index', intersect: false }`)
still drives per-dataset hover labels, but the crosshair plugin and the TOTAL KPI box
read `sideEl._ipcHoverMs` directly so behavior is identical for clicks and hovers.

### 7.2 Click → anchor

```js
canvas.addEventListener('click', (evt) => {
  const xScale = chart.scales.x;
  const ms = xScale.getValueForPixel(evt.offsetX);
  if (!Number.isFinite(ms)) return;
  sideEl.dataset.anchorMs = String(Math.round(ms));
  _refreshIpcSide(sideEl);
});
```

`_refreshIpcSide` reveals the anchor chip, re-paints KPI + table, and triggers
`chart.update('none')` so the plugin re-reads `dataset.anchorMs`. Click ✕ → clears
`dataset.anchorMs`, re-paints.

### 7.3 KPI-box click-to-jump

The MAX box (C.1) is clickable when its target is a valid timestamp:

```js
maxBoxEl.addEventListener('click', () => {
  sideEl.dataset.anchorMs = String(sideData.peakMs);
  _refreshIpcSide(sideEl);
});
```

### 7.4 Fallback chain (D.1)

```
t = anchorMs ?? hoverMs ?? peakMs
```

If `t` is undefined or no clusters are alive at `t`, the table shows zero rows with the
fleet-total footer at 0 and the anchor chip says
"Anchored at HH:MM:SSZ (no clusters alive)".

### 7.5 Table → chart highlight (K.1)

Click a `<tr data-cluster>`:
- Adds `.ipc-row-active` to the row.
- Bumps that dataset's `borderWidth` from 0 to 2 and sets a contrasting outline color.
- Click again to undo. Pure visual; does not navigate.

`<thead>` is `position: sticky; top: 0` inside a max-height-constrained `<tbody>` wrapper.

### 7.6 Zoom / pan / reset

Reuse `csZoomPluginConfig(canvas)` already wired in `dcc-side`. Reset button
(↻) calls `chart.resetZoom()` AND clears `sideEl.dataset.anchorMs`.

### 7.7 Side-expand (⤢)

Identical to `.dcc-side-expand` — toggles `.ipc-pair[data-expanded="reference|current|"]`,
CSS owns the layout, and `chart.resize()` is called after a 50ms delay so Chart.js picks
up the new container width.

## 8. Threshold visualization (E.1)

KPI palette:

| Range | Class | Style |
|---|---|---|
| `ips < warnAt` | `.ipc-kpi` | default |
| `warnAt ≤ ips < critAt` | `.ipc-kpi.warn` | `#d4a72c` background tint |
| `critAt ≤ ips < cap` | `.ipc-kpi.crit` | `#f85149` background tint, white text |
| `ips ≥ cap` | `.ipc-kpi.over` | `#f85149` solid + ⚠ icon |

Where `warnAt = cap * warn_pct/100` and `critAt = cap * crit_pct/100` from `config.ipQuota`.

`ipcThresholdsPlugin` draws three dashed lines (warn, crit, cap) and tints the band
`[cap, y.max]` with `rgba(248,81,73,0.08)`. Y-axis uses
`suggestedMax: max(observedPeak * 1.05, cap * 1.05)` so the cap is always visible.

## 9. Edge cases

| Case | Behavior |
|---|---|
| No `b20.csv` for the date | Side renders empty notice "No autoscaling data exported for this date." Section summary appends "(no data)". |
| Cluster has b20 span but no b21 events (B.1) | Cluster rendered as one flat band at `clusterPlan.workers + 1` over its span. Table row tagged `<span class="ipc-tag-fallback" title="No autoscaler events; using recommended worker count">b23</span>`. |
| Cluster has no `cost_timeline` (C.1) | Cluster excluded from the timeline; surfaces in a footer chip "N clusters skipped — no b20 span". |
| Both dates active, one missing (I.2) | Available side renders normally; missing side shows its own empty notice. Independent KPIs. |
| Anchor lands in a gap (no segment covers `t`) | Table has zero rows; fleet total 0; anchor chip says "Anchored at HH:MM:SSZ (no clusters alive)". |
| Stack peak > 256 | Red tint over-cap zone + KPI `.over` styling + ⚠ icon. Informational only. |
| Window has zero clusters (no b20 anywhere) | Summary appends "(no data)"; body shows a single empty-state notice; no chart instantiated. |
| Date toggle changes mid-render | The async `_loadIpcDataForDate` call is cancellable via a per-side AbortController stored on `sideEl._ipcAbort`. New toggle aborts in-flight fetches. |

## 10. Performance

- ~`N_clusters × avg_segments_per_cluster` data points per side. Real fleets ≈ 50 × 20 = 1000
  points × 2 sides → Chart.js handles trivially with `animation: false` and `parsing: false`.
- Lazy first-paint: nothing inside `<details>` runs until first toggle.
- Per-cluster JSON cache is reused from existing `_ctOf` consumers; no second fetch.
- Crosshair lookup is O(N_clusters × log M_segments) — bounded and runs only on hover/click.

## 11. Testing & verification

No new automated tests. Verification is manual against the existing mock fleet.

### 11.1 Hand-verifiable peak

For `2099_01_01`, inspect `b20` and the per-cluster `cost_timeline.intervals[]`:

| Cluster | Span | Max workers | Max IPs (workers + 1) |
|---|---|---|---|
| mock-cluster-001 | 01:00–07:00 | 6 | 7 |
| mock-cluster-002 | 03:00–11:00 | 10 | 11 |
| mock-cluster-003 | 05:00–18:00 | 16 | 17 |
| mock-cluster-004 inc1 | 02:00–06:00 | 8 (autoscale 2→6→4) | 9 (worst case) |
| mock-cluster-004 inc2 | 14:00–22:00 | 8 | 9 |

Peak overlap window contains all four clusters (e.g. 06:30–07:00). The exact peak depends
on the per-event step function; the rendered MAX box must agree with a manual evaluation
of the step functions at the peak time. **Acceptance gate**: anchor the crosshair at the
three timestamps below and confirm the table sums match the expected fleet IPs.

Step-function reconstruction (built from `buildSpanSegments` in §2 of CLAUDE.md / source):

```
mock-001 [01:00–07:00]: 2 → (01:10) 4 → (01:50) 6 → (04:00) 4 → (06:00) 2
mock-002 [03:00–11:00]: flat 10 (b23 fallback, no b21 events)
mock-003 [05:00–18:00]: 4 → (05:15) 8 → (05:30) 12 → (06:30) 16 → (13:00) 8 → (16:00) 4
mock-004 inc1 [02:00–06:00]: 2 → (02:05) 4 → (02:30) 6 → (05:00) 4
mock-004 inc2 [14:00–22:00]: 2 → (14:10) 5 → (15:00) 8 → (19:00) 4 → (21:00) 2
```

| Anchor `t` (UTC) | Alive clusters and workers at `t` | Expected fleet IPs |
|---|---|---|
| `2099-01-01T03:30:00Z` | mock-001 (6), mock-002 (10), mock-004 inc1 (6) | (6+1)+(10+1)+(6+1) = **25** |
| `2099-01-01T05:45:00Z` | mock-001 (4), mock-002 (10), mock-003 (12), mock-004 inc1 (4) | 5+11+13+5 = **34** |
| `2099-01-01T12:00:00Z` | mock-003 only (16) | 16+1 = **17** |

### 11.2 UX checks (run `serve.sh`, browser dev tools)

1. Default load: section closed; `#ipc-body` not yet populated.
2. Open section → chart appears; selector defaults to Current.
3. Hover → live dashed crosshair tracks; no anchor chip; TOTAL box updates live.
4. Click anywhere on chart → solid anchored crosshair appears; chip visible; table refreshes.
5. Click MAX box → anchor jumps to `peakMs`; table reflects.
6. Click ✕ on chip → anchor cleared; KPI fall back to MAX.
7. Switch to `Reference` → only reference side visible; Current cached.
8. Switch to `Both` → both sides side-by-side; independent crosshairs.
9. ⤢ on one side → other hidden; chart resizes within 50ms.
10. ↻ reset → zoom reset AND anchor cleared.
11. Threshold lines: warn at 179, crit at 230, cap at 256; over-cap zone tinted (force by
    temporarily setting `ipQuota.max_ip_count: 30` in `config.json` to validate the palette).

### 11.3 Accessibility / responsive

- Section usable at 1280px wide and 1024px (laptop) widths.
- Sticky table header survives table scrolling.
- KPI boxes maintain ≥4.5:1 contrast in all four states.

## 12. Open follow-ups (not in this spec)

- HA-master IP support if any GCP setup ever goes 3-master.
- Persisting the anchor in the URL hash if users start sharing links to specific moments.
- Cluster-band click navigates to the cluster's detail page (rejected for now per Q6.H — risks
  accidental nav).
- A range-select mode (drag to highlight a window, KPIs reflect the window's max).
