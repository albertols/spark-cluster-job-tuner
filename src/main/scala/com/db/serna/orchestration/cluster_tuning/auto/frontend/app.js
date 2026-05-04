// Spark Cluster Auto-Tuner Dashboard
// Reads _auto_tuner_analysis.json and renders interactive visualizations.

let config = null;       // parsed config.json
let outputsRoot = null;  // URL-absolute path to <outputsPath> (trailing slash, no trailing '/')
let data = null;
let analysisDir = '.';   // dir (relative or absolute URL) holding _auto_tuner_analysis.json
let currentEntry = null; // { dir, reference_date, current_date, ... } when in dashboard mode
const tooltip = document.getElementById('tooltip');
const docPopover = document.getElementById('doc-popover');

// Per-cluster JSON cache: key = "<date>/<cluster>" → parsed json (or null if not found)
const clusterJsonCache = {};
// Last URLs we tried to fetch for a cluster (for diagnostics in the UI)
const clusterJsonTriedPaths = {};
// Discovered analyses (for routing)
let discoveredEntries = [];
// Generation summary (boost groups) loaded for the open analysis
let generationSummary = null;
// Discovered cluster-summary CSV files for the open analysis
let clusterSummaryFiles = [];
// Cache of parsed cluster-summary CSVs keyed by filename
const clusterSummaryCache = {};
// Routing: are we currently applying a route (suppress pushState reentry)?
let routingInFlight = false;

// ── Inline-SVG mini-tutorials ───────────────────────────────────────────────
//
// Hand-authored, ~2 KB each. Use `currentColor` so they pick up the popover's
// foreground tone and adapt to the dark theme without extra rules.
// Defined ABOVE METRIC_DOCS because METRIC_DOCS references them at init time.

const PEARSON_SVG_TUTORIAL = `
<svg viewBox="0 0 360 130" width="100%" class="doc-svg" role="img" aria-label="Pearson correlation examples">
  <style>
    .ax { stroke: rgba(139,148,158,0.6); stroke-width: 0.8; fill: none; }
    .pt { fill: rgba(88,166,255,0.85); }
    .lbl { fill: #c9d1d9; font: 11px/1.2 -apple-system, Segoe UI, sans-serif; text-anchor: middle; }
    .sub { fill: #8b949e; font: 10px/1.2 monospace; text-anchor: middle; }
  </style>
  <g transform="translate(10,10)">
    <rect x="0" y="0" width="100" height="90" class="ax"/>
    ${[[10,80],[18,72],[26,68],[34,60],[42,55],[50,50],[58,42],[66,36],[74,28],[82,20],[90,15]].map(([x,y]) => `<circle cx="${x}" cy="${y}" r="2" class="pt"/>`).join('')}
    <text x="50" y="105" class="lbl">Strong positive</text>
    <text x="50" y="120" class="sub">r ≈ +0.95</text>
  </g>
  <g transform="translate(130,10)">
    <rect x="0" y="0" width="100" height="90" class="ax"/>
    ${[[15,30],[25,70],[35,40],[45,55],[55,20],[65,75],[75,45],[85,30],[20,60],[60,15],[80,55]].map(([x,y]) => `<circle cx="${x}" cy="${y}" r="2" class="pt"/>`).join('')}
    <text x="50" y="105" class="lbl">No relationship</text>
    <text x="50" y="120" class="sub">r ≈ 0</text>
  </g>
  <g transform="translate(250,10)">
    <rect x="0" y="0" width="100" height="90" class="ax"/>
    ${[[10,15],[18,22],[26,28],[34,35],[42,42],[50,48],[58,55],[66,62],[74,68],[82,75],[90,82]].map(([x,y]) => `<circle cx="${x}" cy="${y}" r="2" class="pt"/>`).join('')}
    <text x="50" y="105" class="lbl">Strong negative</text>
    <text x="50" y="120" class="sub">r ≈ −0.95</text>
  </g>
</svg>`;

const BELL_SVG_TUTORIAL = (() => {
  const xs = [];
  for (let i = -40; i <= 40; i++) xs.push(i / 10);
  const pdf = (x) => Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI);
  const W = 360, H = 130, PAD_L = 20, PAD_R = 10, PAD_T = 10, PAD_B = 28;
  const innerW = W - PAD_L - PAD_R;
  const innerH = H - PAD_T - PAD_B;
  const yMax = pdf(0);
  const px = (x) => PAD_L + ((x - (-4)) / 8) * innerW;
  const py = (y) => PAD_T + innerH - (y / yMax) * innerH;
  const path = xs.map((x, i) => `${i === 0 ? 'M' : 'L'} ${px(x).toFixed(1)} ${py(pdf(x)).toFixed(1)}`).join(' ');
  const x1 = px(-2), x2 = px(2), x3 = px(-3), x4 = px(3);
  return `
<svg viewBox="0 0 ${W} ${H}" width="100%" class="doc-svg" role="img" aria-label="Normal distribution with z bands">
  <style>
    .axis { stroke: rgba(139,148,158,0.6); stroke-width: 0.8; }
    .curve { fill: rgba(88,166,255,0.18); stroke: rgba(88,166,255,0.85); stroke-width: 1.4; }
    .band-2 { fill: rgba(248,201,73,0.18); }
    .band-3 { fill: rgba(248,81,73,0.22); }
    .lbl { fill: #c9d1d9; font: 10px/1.2 monospace; text-anchor: middle; }
    .lbl-mu { fill: #58a6ff; font: 10px/1.2 monospace; text-anchor: middle; }
  </style>
  <rect x="${PAD_L}" y="${PAD_T}" width="${x1 - PAD_L}" height="${innerH}" class="band-2"/>
  <rect x="${x2}" y="${PAD_T}" width="${(W - PAD_R) - x2}" height="${innerH}" class="band-2"/>
  <rect x="${PAD_L}" y="${PAD_T}" width="${x3 - PAD_L}" height="${innerH}" class="band-3"/>
  <rect x="${x4}" y="${PAD_T}" width="${(W - PAD_R) - x4}" height="${innerH}" class="band-3"/>
  <path d="${path} L ${px(4).toFixed(1)} ${(PAD_T + innerH).toFixed(1)} L ${px(-4).toFixed(1)} ${(PAD_T + innerH).toFixed(1)} Z" class="curve"/>
  <line x1="${PAD_L}" y1="${PAD_T + innerH}" x2="${W - PAD_R}" y2="${PAD_T + innerH}" class="axis"/>
  <line x1="${px(0).toFixed(1)}" y1="${PAD_T}" x2="${px(0).toFixed(1)}" y2="${PAD_T + innerH}" class="axis"/>
  <text x="${px(0).toFixed(1)}" y="${(H - 8).toFixed(1)}" class="lbl-mu">μ</text>
  <text x="${px(-2).toFixed(1)}" y="${(H - 8).toFixed(1)}" class="lbl">−2σ</text>
  <text x="${px(2).toFixed(1)}" y="${(H - 8).toFixed(1)}" class="lbl">+2σ</text>
  <text x="${px(-3).toFixed(1)}" y="${(H - 8).toFixed(1)}" class="lbl">−3σ</text>
  <text x="${px(3).toFixed(1)}" y="${(H - 8).toFixed(1)}" class="lbl">+3σ</text>
</svg>`;
})();

const DIVERGENCE_SVG_TUTORIAL = `
<svg viewBox="0 0 360 90" width="100%" class="doc-svg" role="img" aria-label="Outlier on a strip plot">
  <style>
    .axis { stroke: rgba(139,148,158,0.6); stroke-width: 0.8; }
    .band { fill: rgba(248,81,73,0.16); }
    .normal { fill: rgba(88,166,255,0.85); }
    .outlier { fill: rgba(248,81,73,1); stroke: #fff; stroke-width: 0.8; }
    .lbl { fill: #c9d1d9; font: 10px/1.2 monospace; text-anchor: middle; }
  </style>
  <rect x="20" y="30" width="60" height="20" class="band"/>
  <rect x="280" y="30" width="60" height="20" class="band"/>
  <line x1="20" y1="40" x2="340" y2="40" class="axis"/>
  ${[60,90,110,130,150,170,180,200,220,240,260].map(x => `<circle cx="${x}" cy="40" r="3" class="normal"/>`).join('')}
  <circle cx="320" cy="40" r="5" class="outlier"/>
  <text x="20" y="68" class="lbl">μ − 2σ</text>
  <text x="180" y="68" class="lbl">μ</text>
  <text x="340" y="68" class="lbl">μ + 2σ</text>
  <text x="320" y="20" class="lbl">outlier</text>
</svg>`;

// ── Metric Documentation ────────────────────────────────────────────────────

const METRIC_DOCS = {
  p95: {
    title: "P95 (95th percentile)",
    body: "95% of runs were at or below this value, 5% were higher. P95 is a tail-latency indicator: it shows the 'bad but not extreme' user experience and is more meaningful than the average for catching slowdowns."
  },
  avg: {
    title: "Average (mean)",
    body: "Arithmetic mean across all observed runs. Easy to read but masks tail behavior — pair with P95 to see whether slow runs are an issue."
  },
  max: {
    title: "Maximum",
    body: "The highest value observed across runs. Useful for sizing peak capacity (e.g., max executors needed)."
  },
  pearson: {
    title: "Pearson correlation",
    body: "A number between -1 and +1 that describes how strongly two metric changes move together across the fleet. +1 = perfectly aligned, 0 = unrelated, -1 = perfectly inverse. |r| > 0.5 is usually meaningful."
  },
  covariance: {
    title: "Covariance",
    body: "Joint variability of two metrics. Sign tells you the direction (rise together vs inverse). The magnitude depends on units, so prefer the Pearson value for interpretation."
  },
  z_score: {
    title: "Z-score",
    body: "How many standard deviations a value is from the fleet mean. |z| ≥ 2 ≈ uncommon outlier (top 5%), |z| ≥ 3 ≈ strong outlier (top 0.3%). Used to flag recipes whose change diverges sharply from the rest of the fleet."
  },
  divergence: {
    title: "Divergence",
    body: "A (cluster, recipe) whose metric change is unusually far from the fleet average. We compute the fleet-wide mean and standard deviation per metric, then flag those with |z-score| above the threshold."
  },
  correlation: {
    title: "Correlation matrix",
    body: "Shows pairwise relationships between metric deltas across the fleet. Helps spot cause-and-effect patterns (e.g., when more executors correlate with longer durations, scaling may not be helping)."
  },
  fraction_reaching_cap: {
    title: "Fraction reaching cap",
    body: "Share of runs that hit the executor cap. High values indicate sustained capacity pressure and a likely bottleneck."
  },
  confidence: {
    title: "Confidence",
    body: "How trustworthy the trend assessment is, based on the minimum run count between reference and current dates. 1+ runs = 0.1, 5 = 0.5, 10+ = 1.0."
  },
  trend: {
    title: "Trend",
    bodyHtml: "Per (cluster, recipe) classification of how the current run compares to the reference run.<br><br>" +
          "<strong>Improved</strong> — the chosen metric (typically p95 duration) dropped by more than the configured noise threshold.<br>" +
          "<strong>Degraded</strong> — the metric rose by more than the noise threshold.<br>" +
          "<strong>Stable</strong> — within the noise threshold; not statistically distinguishable.<br>" +
          "<strong>NewEntry</strong> — present in current run only (newly observed cluster/recipe).<br>" +
          "<strong>DroppedEntry</strong> — present in reference only (no data this run).<br><br>" +
          "Noise thresholds are configured per metric. Z-score uses the cluster's own historical distribution where available; otherwise falls back to the fleet baseline. Confidence weights the assessment by paired-run count."
  },
  cluster_cost_autoscaling: {
    title: "Cluster cost & autoscaling",
    bodyHtml: "Three cards bracket the cost evolution for this cluster:<br><br>" +
          "<strong>Previous → Reference</strong> — actual workers during reference-date observations, priced at the <em>previous-reference</em> machines (what was actually deployed during that window).<br>" +
          "<strong>Reference → Current</strong> — actual workers during current-date observations, priced at the <em>reference</em> machines (what is actually deployed now). This card is the true 'current cost'.<br>" +
          "<strong>Future projection</strong> — the same current-date workload re-priced at the new tuning that will be applied in the next deployment.<br><br>" +
          "Spans come from <code>b20</code>; per-second worker counts come from <code>b21</code> autoscaler events. When <code>b20</code> is missing for a cluster but <code>b21</code> has events, span boundaries are inferred from the first/last event (badge: <code>b22 · synthetic span</code>). When <code>b21</code> is empty, the worker count is held flat at the recommended size (badge: <code>b23 · no autoscaler events</code>)."
  },
  // Richer popovers: title + body + formula + svg + (optional) live example.
  pearson_full: {
    title: "Pearson correlation (r)",
    body: "Measures how strongly two metrics' changes move together across the fleet. r is bounded in [-1, +1]. Values near +1 mean both metrics move up together; near -1 means one rises as the other falls; near 0 means no linear relationship. Rule of thumb: |r| < 0.3 weak, 0.3-0.7 moderate, ≥ 0.7 strong.",
    formula: "r = Σ((x − x̄)(y − ȳ)) / √( Σ(x − x̄)² · Σ(y − ȳ)² )",
    svg: PEARSON_SVG_TUTORIAL,
    example: (data) => {
      const c = (data.correlations || [])[0];
      if (!c) return '';
      const a = labelMetric(c.metric_a);
      const b = labelMetric(c.metric_b);
      return `In your fleet right now: <em>${escapeHtml(a)}</em> ↔ <em>${escapeHtml(b)}</em> has r=${c.pearson.toFixed(3)} on n=${c.n} paired recipes.`;
    }
  },
  z_score_full: {
    title: "Z-score",
    body: "Counts how many standard deviations a value sits from the mean. Roughly: |z| ≥ 2 ≈ top 5% (uncommon), |z| ≥ 3 ≈ top 0.3% (strong outlier). We compute z over the fleet (or within a single cluster) and flag entries above the threshold.",
    formula: "z = (x − μ) / σ",
    svg: BELL_SVG_TUTORIAL,
    example: (data) => {
      const ds = data.divergences || [];
      if (ds.length === 0) return 'No outliers above the current threshold yet — increase the |z-score| filter or pick a wider window.';
      const top = ds.slice().sort((a, b) => Math.abs(b.z_score) - Math.abs(a.z_score))[0];
      return `Top outlier in your fleet: <em>${escapeHtml(top.cluster)}/${escapeHtml(top.recipe)}</em> on ${escapeHtml(labelMetric(top.metric))}, z=${top.z_score.toFixed(2)}.`;
    }
  },
  divergence_full: {
    title: "Divergence",
    body: "A (cluster, recipe) whose metric value (or change vs reference) is unusually far from the fleet. We compute the fleet-wide mean and standard deviation per metric, then flag entries with |z-score| ≥ the threshold (default 2.0).",
    formula: "z = (x − μ_fleet) / σ_fleet",
    svg: DIVERGENCE_SVG_TUTORIAL,
    example: (data) => {
      const n = (data.divergences || []).length;
      const m = (data.divergences_current_snapshot || []).length;
      return `This run: ${n} delta-view outliers, ${m} current-snapshot outliers (includes new jobs).`;
    }
  }
};

// (SVG mini-tutorial constants are defined above METRIC_DOCS to avoid TDZ issues.)

// ── Config + Discovery ──────────────────────────────────────────────────────

async function loadConfig() {
  try {
    const resp = await fetch('config.json', { cache: 'no-store' });
    if (!resp.ok) throw new Error(`config.json fetch failed: ${resp.status}`);
    config = await resp.json();
  } catch (e) {
    showFatalError(
      'Missing or invalid <code>config.json</code>',
      'Create a <code>config.json</code> next to <code>index.html</code> with <code>gcpProjectId</code>, <code>inputsPath</code> and <code>outputsPath</code>.<br>' +
      `Error: ${escapeHtml(e.message || String(e))}`
    );
    return;
  }
  if (!config.outputsPath) {
    showFatalError('config.json is missing <code>outputsPath</code>.', '');
    config = null;
    return;
  }
  // Resolve outputsPath against the page URL. URL collapses any "../" cleanly.
  outputsRoot = stripTrailingSlash(new URL(config.outputsPath + '/', window.location.href).href);
}

async function discoverAnalyses() {
  if (!outputsRoot) return [];

  // 1) Preferred: an explicit index file written by the auto-tuner.
  try {
    const r = await fetch(`${outputsRoot}/_analyses_index.json`, { cache: 'no-store' });
    if (r.ok) {
      const j = await r.json();
      if (Array.isArray(j.entries)) {
        console.info(`[Discovery] ${j.entries.length} analyses from _analyses_index.json at ${outputsRoot}`);
        return j.entries;
      }
    }
  } catch (e) { /* fall through */ }

  // 2) Fallback: parse python http.server's HTML directory listing.
  try {
    const r = await fetch(`${outputsRoot}/`);
    if (!r.ok) return [];
    const html = await r.text();
    const dirs = parseDirListing(html);
    const entries = await Promise.all(dirs.map(buildEntryFromDir));
    const filtered = entries.filter(Boolean);
    console.info(`[Discovery] ${filtered.length} analyses from directory listing at ${outputsRoot} (${dirs.length} dirs scanned)`);
    return filtered;
  } catch (e) {
    return [];
  }
}

// Extract directory links from python http.server's HTML listing.
function parseDirListing(html) {
  const out = [];
  const re = /<a href="([^"]+)"/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    let href = m[1];
    if (!href.endsWith('/')) continue;              // directories only
    if (href.startsWith('?') || href.startsWith('#') || href === '../' || href === './') continue;
    // Strip any trailing slash and URL-decode.
    href = decodeURIComponent(href.replace(/\/$/, ''));
    // Ignore absolute URLs just in case.
    if (href.startsWith('http://') || href.startsWith('https://')) continue;
    // Skip the index itself and any hidden entries.
    if (href.startsWith('_') || href.startsWith('.')) continue;
    out.push(href);
  }
  return out;
}

async function buildEntryFromDir(dir) {
  try {
    const r = await fetch(`${outputsRoot}/${encodeURIComponent(dir)}/_auto_tuner_analysis.json`, { cache: 'no-store' });
    if (!r.ok) return null;
    const j = await r.json();
    const m = j.metadata || {};
    const t = j.trends_summary || {};
    return {
      dir,
      reference_date: m.reference_date,
      current_date: m.current_date,
      strategy: m.strategy || 'unknown',
      total_clusters: m.total_clusters || 0,
      total_recipes: m.total_recipes || 0,
      generated_at: m.generated_at || '',
      trends: {
        improved: t.improved || 0,
        degraded: t.degraded || 0,
        stable: t.stable || 0,
        new_entries: t.new_entries || 0,
        dropped_entries: t.dropped_entries || 0
      }
    };
  } catch (e) {
    return null;
  }
}

// ── Data Loading ────────────────────────────────────────────────────────────

async function loadAnalysisFromUrl(url) {
  try {
    const resp = await fetch(url, { cache: 'no-store' });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    data = await resp.json();
    analysisDir = dirOf(url);
    return true;
  } catch (e) {
    showFatalError(`Failed to load analysis from <code>${escapeHtml(url)}</code>`, escapeHtml(e.message || String(e)));
    return false;
  }
}

async function loadClusterJson(date, clusterName) {
  const key = `${date}/${clusterName}`;
  if (clusterJsonCache[key] !== undefined) return clusterJsonCache[key];

  // Build candidate dirs to probe for this date. When browsing from a landing
  // entry we know the exact dir name for the current date; reference date is
  // "<refDate>" or "<refDate>_auto_tuned" depending on whether that date was
  // itself produced by the auto-tuner.
  const isCur = data.metadata && date === data.metadata.current_date;
  const curDirName = currentEntry ? currentEntry.dir : null;
  const dirs = [];
  if (isCur && curDirName) dirs.push(curDirName);
  dirs.push(date, `${date}_auto_tuned`);

  const candidates = [];
  dirs.forEach(d => {
    const base = outputsRoot ? `${outputsRoot}/${encodeURIComponent(d)}` : `${analysisDir}/../${d}`;
    candidates.push(
      `${base}/${encodeURIComponent(clusterName)}-auto-scale-tuned.json`,
      `${base}/${encodeURIComponent(clusterName)}-manually-tuned.json`
    );
  });
  // Also try analysisDir directly as a last resort (back-compat with ?data=).
  if (isCur) {
    candidates.push(
      `${analysisDir}/${encodeURIComponent(clusterName)}-auto-scale-tuned.json`,
      `${analysisDir}/${encodeURIComponent(clusterName)}-manually-tuned.json`
    );
  }

  // Deduplicate while keeping order.
  const seen = new Set();
  const unique = candidates.filter(p => (seen.has(p) ? false : (seen.add(p), true)));

  clusterJsonTriedPaths[key] = unique.slice();

  for (const p of unique) {
    try {
      const r = await fetch(p, { cache: 'no-store' });
      if (r.ok) {
        const j = await r.json();
        clusterJsonCache[key] = j;
        return j;
      }
    } catch (e) { /* try next */ }
  }
  clusterJsonCache[key] = null;
  return null;
}

async function loadClusterJsonsForDates(clusterName) {
  const refDate = data.metadata.reference_date;
  const curDate = data.metadata.current_date;
  const prevRefDate = findPreviousReferenceDate(refDate);
  const [ref, cur, prevRef] = await Promise.all([
    loadClusterJson(refDate, clusterName),
    loadClusterJson(curDate, clusterName),
    prevRefDate ? loadClusterJson(prevRefDate, clusterName) : Promise.resolve(null),
  ]);
  return { ref, cur, prevRef, refDate, curDate, prevRefDate };
}

// Find the date that was the `current_date` of the run whose output produced
// the machines deployed during this run's reference_date observations. That is:
// the entry in `discoveredEntries` whose `current_date == refDate` — its output
// is what was actually deployed when the reference-date b20/b21 events happened.
// Returns null when no such prior run is indexed.
function findPreviousReferenceDate(refDate) {
  if (!refDate || !Array.isArray(discoveredEntries)) return null;
  // Entries are typically AutoTuner runs (ref→cur). The prior run's `current_date`
  // matches our refDate when consecutive runs are chained.
  const match = discoveredEntries.find(e => e && e.current_date === refDate);
  return match ? (match.current_date || null) : null;
  // NOTE: we return match.current_date (= our refDate) deliberately — that's the
  // date directory under outputs/ where the prior tuning's per-cluster JSON lives.
  // The prior tuning's recommended machine = what's deployed during refDate.
}

// Pure transform: given actuals `cost_timeline` (workers/spans from one date)
// and a machine-source `cost_timeline` (worker_machine_type, worker_hourly_eur,
// master_machine_type, master_hourly_eur from a different date), return a NEW
// cost_timeline with prices recomputed at the machine-source rates while keeping
// the actuals' workers timeline intact. Pure / Scala.js-friendly: takes plain
// JSON, returns plain JSON, no DOM.
function recomputeCostTimeline(actualsCt, machineCt) {
  if (!actualsCt) return null;
  if (!machineCt) return actualsCt;  // No re-pricing source → keep actuals as-is.

  const wHourly = +machineCt.worker_hourly_eur || 0;
  const mHourly = +machineCt.master_hourly_eur || 0;
  const wType   = machineCt.worker_machine_type || actualsCt.worker_machine_type || '';
  const mType   = machineCt.master_machine_type || actualsCt.master_machine_type || '';

  let grandTotal = 0;
  const newIncs = (actualsCt.incarnations || []).map(inc => {
    let workerCost = 0;
    let masterCost = 0;
    const newIntervals = (inc.intervals || []).map(iv => {
      const segSec = +iv.seg_seconds || 0;
      const workers = +iv.workers || 0;
      const wCost = wHourly * workers * (segSec / 3600);
      const mCost = mHourly * (segSec / 3600);
      workerCost += wCost;
      masterCost += mCost;
      return Object.assign({}, iv, { seg_cost_eur: +(wCost + mCost).toFixed(4) });
    });
    const total = workerCost + masterCost;
    grandTotal += total;
    return Object.assign({}, inc, {
      worker_cost_eur: +workerCost.toFixed(4),
      master_cost_eur: +masterCost.toFixed(4),
      total_cost_eur:  +total.toFixed(4),
      intervals:       newIntervals
    });
  });

  return Object.assign({}, actualsCt, {
    worker_machine_type: wType,
    master_machine_type: mType,
    worker_hourly_eur:   wHourly,
    master_hourly_eur:   mHourly,
    total_cost_eur:      +grandTotal.toFixed(4),
    incarnations:        newIncs
  });
}

// ── Bootstrap ───────────────────────────────────────────────────────────────

async function bootstrap() {
  wireGlobalHandlers();

  await loadConfig();
  if (!config) return;

  renderProjectChip();

  // Pre-discover analyses so deep-links and back/forward can resolve <dir>.
  discoveredEntries = await discoverAnalyses();

  window.addEventListener('popstate', () => applyRoute(parseRoute(), { fromPopstate: true }));

  // Apply initial URL state.
  await applyRoute(parseRoute(), { initial: true });
}

// ── Router ──────────────────────────────────────────────────────────────────
//
// URL state (query string) lets every view be shareable and back/forward-able:
//   ?                                            → landing
//   ?data=<dir>                                  → dashboard, overview tab
//   ?data=<dir>&tab=correlations|divergences     → other top tabs
//   ?data=<dir>&cluster=<name>                   → cluster detail
//   ?data=<dir>&cluster=<name>&recipe=<name>     → recipe spark conf modal open
//   ?data=<dir>&summary=<csvfile>                → cluster-summary csv modal open

function parseRoute() {
  const p = new URLSearchParams(window.location.search);
  return {
    data: p.get('data') || null,
    tab: p.get('tab') || 'overview',
    cluster: p.get('cluster') || null,
    recipe: p.get('recipe') || null,
    summary: p.get('summary') || null,
  };
}

function buildUrl(route) {
  const p = new URLSearchParams();
  if (route.data) p.set('data', route.data);
  if (route.tab && route.tab !== 'overview') p.set('tab', route.tab);
  if (route.cluster) p.set('cluster', route.cluster);
  if (route.recipe) p.set('recipe', route.recipe);
  if (route.summary) p.set('summary', route.summary);
  const s = p.toString();
  return s ? `?${s}` : window.location.pathname;
}

function navigate(patch, opts) {
  if (routingInFlight) return;
  const cur = parseRoute();
  const next = Object.assign({}, cur, patch);
  // Clean keys that are explicitly null
  Object.keys(patch).forEach(k => { if (patch[k] === null) delete next[k]; });
  const url = buildUrl(next);
  if (url === window.location.search || url === window.location.pathname + window.location.search) {
    return; // no-op
  }
  if (opts && opts.replace) history.replaceState(next, '', url);
  else history.pushState(next, '', url);
  applyRoute(next);
}

async function applyRoute(route, opts) {
  routingInFlight = true;
  try {
    // Always close any open modal first; re-open below if route demands it.
    closeModalSilently();

    if (!route.data) {
      await showLanding();
      return;
    }
    // Dashboard mode for a particular run
    const entry = entryForDir(route.data);
    if (data === null || (currentEntry && currentEntry.dir !== route.data) ||
        (!currentEntry && (!data.metadata || !data._loadedFor || data._loadedFor !== route.data))) {
      currentEntry = entry || null;
      const url = entry
        ? `${outputsRoot}/${encodeURIComponent(entry.dir)}/_auto_tuner_analysis.json`
        : `${outputsRoot}/${encodeURIComponent(route.data)}/_auto_tuner_analysis.json`;
      const ok = await loadAnalysisFromUrl(url);
      if (!ok) return;
      data._loadedFor = route.data;
      await loadGenerationSummary(route.data);
      await discoverClusterSummaries(route.data);
      renderDashboard();
    }
    // Tab
    switchTabRaw(route.tab || 'overview');

    // Cluster detail
    if (route.cluster) {
      await showClusterDetailRaw(route.cluster);
      if (route.recipe) {
        await showRecipeConfModalRaw(route.cluster, route.recipe);
      }
    } else {
      // No cluster selected: ensure the detail panel is hidden. The chosen
      // tab content is already activated by switchTabRaw above — do NOT add
      // 'active' to #overview here, otherwise it overlays Correlations /
      // Divergences.
      document.getElementById('cluster-detail').style.display = 'none';
    }

    // Cluster-summary CSV modal
    if (route.summary && !route.recipe) {
      await openSummaryModalRaw(route.summary);
    }
  } finally {
    routingInFlight = false;
  }
}

function entryForDir(dir) {
  return discoveredEntries.find(e => e.dir === dir) || null;
}

function wireGlobalHandlers() {
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => navigate({ tab: tab.dataset.tab, cluster: null, recipe: null }));
  });

  document.getElementById('cluster-search').addEventListener('input', renderClusterGrid);
  document.getElementById('trend-filter').addEventListener('change', renderClusterGrid);
  document.getElementById('z-min').addEventListener('input', () => renderDivergenceTable());

  // Correlations: view toggle + cluster filter
  document.querySelectorAll('#corr-view-toggle .seg').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#corr-view-toggle .seg').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      renderCorrelationCards();
    });
  });
  const corrFilter = document.getElementById('corr-cluster-filter');
  if (corrFilter) corrFilter.addEventListener('change', () => renderCorrelationCards());

  // Divergences: view toggle + cluster filter
  document.querySelectorAll('#div-view-toggle .seg').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#div-view-toggle .seg').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      renderDivergenceTable();
    });
  });
  const divFilter = document.getElementById('div-cluster-filter');
  if (divFilter) divFilter.addEventListener('change', () => renderDivergenceTable());

  // Cluster-summary historical graphs: lazy-load on first expand.
  const csGraphs = document.getElementById('cluster-summary-graphs-section');
  if (csGraphs) {
    csGraphs.addEventListener('toggle', () => {
      if (csGraphs.open && !csGraphs.dataset.loaded) {
        csGraphs.dataset.loaded = '1';
        renderClusterSummaryGraphs();
      }
    });
  }

  document.getElementById('back-to-overview').addEventListener('click', () => {
    navigate({ cluster: null, recipe: null });
  });

  document.getElementById('back-to-landing').addEventListener('click', () => navigate({ data: null, tab: null, cluster: null, recipe: null, summary: null }));
  document.getElementById('landing-refresh').addEventListener('click', () => showLanding({ force: true }));

  // Modal close
  document.getElementById('modal-close').addEventListener('click', () => {
    const cur = parseRoute();
    if (cur.recipe) navigate({ recipe: null });
    else if (cur.summary) navigate({ summary: null });
    else closeModalSilently();
  });
  document.getElementById('modal-overlay').addEventListener('click', (e) => {
    if (e.target.id === 'modal-overlay') {
      const cur = parseRoute();
      if (cur.recipe) navigate({ recipe: null });
      else if (cur.summary) navigate({ summary: null });
      else closeModalSilently();
    }
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const cur = parseRoute();
      if (cur.recipe) navigate({ recipe: null });
      else if (cur.summary) navigate({ summary: null });
      else closeModalSilently();
      hideDocPopover();
    }
  });

  // Info-icon delegation (works for current and future-rendered icons)
  document.body.addEventListener('click', (e) => {
    const icon = e.target.closest('.info-icon');
    if (icon) {
      e.stopPropagation();
      const key = icon.dataset.docKey;
      if (key && METRIC_DOCS[key]) showDocPopover(e, METRIC_DOCS[key]);
      return;
    }
    // Click anywhere else hides the popover
    if (!e.target.closest('.doc-popover')) hideDocPopover();
  });
}

function switchTab(tabName) { navigate({ tab: tabName, cluster: null, recipe: null }); }
function switchTabRaw(tabName) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  const tabBtn = document.querySelector(`.tab[data-tab="${tabName}"]`);
  if (tabBtn) tabBtn.classList.add('active');
  const tabContent = document.getElementById(tabName);
  if (tabContent) tabContent.classList.add('active');
  document.getElementById('cluster-detail').style.display = 'none';
}

// ── Mode switching ──────────────────────────────────────────────────────────

function setMode(mode) {
  const isLanding = mode === 'landing';
  document.getElementById('landing').style.display = isLanding ? 'block' : 'none';
  document.getElementById('tabs').style.display = isLanding ? 'none' : 'flex';
  document.getElementById('metadata-bar').style.display = isLanding ? 'none' : 'flex';
  document.getElementById('back-to-landing').style.display = isLanding ? 'none' : 'inline-block';

  const dashboardSections = ['overview', 'cluster-detail', 'correlations', 'divergences'];
  if (isLanding) {
    dashboardSections.forEach(id => {
      const el = document.getElementById(id);
      el.classList.remove('active');
      el.style.display = 'none';
    });
  } else {
    // Clear the inline 'display:none' that was applied while on landing, so
    // the CSS rules (.tab-content.active → block) take over again.
    dashboardSections.forEach(id => {
      document.getElementById(id).style.display = '';
    });
  }
}

async function showLanding(opts) {
  setMode('landing');
  // Reset per-analysis state so the next dashboard open is clean.
  data = null;
  currentEntry = null;
  generationSummary = null;
  clusterSummaryFiles = [];
  Object.keys(clusterJsonCache).forEach(k => delete clusterJsonCache[k]);
  Object.keys(clusterJsonTriedPaths).forEach(k => delete clusterJsonTriedPaths[k]);
  Object.keys(clusterSummaryCache).forEach(k => delete clusterSummaryCache[k]);

  document.getElementById('landing-grid').innerHTML =
    `<div class="empty-msg" style="padding:30px;text-align:center;">Scanning <code>${escapeHtml(outputsRoot || config.outputsPath)}</code>…</div>`;
  document.getElementById('landing-empty').style.display = 'none';

  // Load inputs tree in parallel
  renderInputsTree();

  if (!discoveredEntries.length || (opts && opts.force)) {
    discoveredEntries = await discoverAnalyses();
  }
  renderLandingPage(discoveredEntries);
}

function renderLandingPage(entries) {
  const grid = document.getElementById('landing-grid');
  const empty = document.getElementById('landing-empty');

  if (!entries || entries.length === 0) {
    grid.innerHTML = '';
    empty.innerHTML =
      `<h3>No analysis runs found</h3>` +
      `<p>Scanned <code>${escapeHtml(outputsRoot || '')}</code>.</p>` +
      `<p>Run the auto-tuner to generate <code>_auto_tuner_analysis.json</code> under <code>outputs/&lt;date&gt;/</code>.</p>`;
    empty.style.display = 'block';
    return;
  }
  empty.style.display = 'none';

  // Sort: oldest (left) → newest (right) by current_date
  const sorted = entries.slice().sort((a, b) => String(a.current_date || '').localeCompare(String(b.current_date || '')));
  // Compute age 0..1 per visible run for the date-pill gradient
  const dates = sorted.map(e => String(e.current_date || ''));
  const minD = dates[0], maxD = dates[dates.length - 1];

  grid.innerHTML = sorted.map((e, idx) => {
    const t = e.trends || {};
    const chips = [
      t.degraded        ? `<span class="trend-chip degraded">${t.degraded} deg</span>` : '',
      t.improved        ? `<span class="trend-chip improved">${t.improved} imp</span>` : '',
      t.stable          ? `<span class="trend-chip stable">${t.stable} stab</span>`    : '',
      t.new_entries     ? `<span class="trend-chip new">${t.new_entries} new</span>`   : '',
      t.dropped_entries ? `<span class="trend-chip dropped">${t.dropped_entries} dropped</span>` : '',
    ].filter(Boolean).join('');

    const generated = e.generated_at
      ? `<div class="landing-card-footer">Generated ${formatDateTime(e.generated_at)}</div>`
      : '';

    const age = ageRatio(String(e.current_date || ''), minD, maxD);
    const styleAge = `style="--age:${age.toFixed(3)}"`;

    return `<div class="landing-card" data-idx="${idx}">
      <div class="landing-card-title">
        <span class="date-pill cur" ${styleAge} title="Current date">${escapeHtml(formatDate(e.current_date))}</span>
        <span class="landing-card-strategy">${escapeHtml(e.strategy || '')}</span>
      </div>
      <div class="landing-card-dates">
        <span class="date-pill ref" title="Reference date">${escapeHtml(formatDate(e.reference_date))}</span>
        <span class="date-arrow">→</span>
        <span class="date-pill cur" ${styleAge}>${escapeHtml(formatDate(e.current_date))}</span>
      </div>
      <div class="landing-card-meta">${e.total_clusters} clusters · ${e.total_recipes} recipes</div>
      <div class="landing-card-chips">${chips || '<span class="empty-msg">no trend data</span>'}</div>
      ${generated}
      <div class="landing-card-dir"><code>${escapeHtml(e.dir)}/</code></div>
      <button class="landing-card-open">Open →</button>
    </div>`;
  }).join('');

  grid.querySelectorAll('.landing-card').forEach(card => {
    card.addEventListener('click', () => {
      const idx = parseInt(card.dataset.idx, 10);
      navigate({ data: sorted[idx].dir, tab: null, cluster: null, recipe: null, summary: null });
    });
    card.addEventListener('auxclick', (e) => {
      if (e.button !== 1) return;
      e.preventDefault();
      const idx = parseInt(card.dataset.idx, 10);
      window.open(buildUrl({ data: sorted[idx].dir }), '_blank');
    });
  });
}

// Map a date string between [minD, maxD] to a 0..1 age (newer → 1).
function ageRatio(d, minD, maxD) {
  if (!d || !minD || !maxD || minD === maxD) return 1;
  const t = (s) => {
    const m = String(s).match(/^(\d{4})[-_/](\d{2})[-_/](\d{2})$/);
    return m ? Date.UTC(+m[1], +m[2] - 1, +m[3]) : 0;
  };
  const a = t(minD), b = t(maxD), x = t(d);
  if (b === a) return 1;
  return Math.max(0, Math.min(1, (x - a) / (b - a)));
}

async function openAnalysisEntry(entry) {
  navigate({ data: entry.dir, tab: null, cluster: null, recipe: null, summary: null });
}

async function openAnalysisFromUrl(url) {
  // Legacy deep-link path (?data=<full url>) — extract dir name and route via SPA router.
  const m = url.match(/\/([^\/]+)\/_auto_tuner_analysis\.json(?:\?.*)?$/);
  if (m) navigate({ data: decodeURIComponent(m[1]), tab: null, cluster: null, recipe: null, summary: null });
  else {
    currentEntry = null;
    const ok = await loadAnalysisFromUrl(url);
    if (!ok) return;
    renderDashboard();
  }
}

function renderDashboard() {
  setMode('dashboard');
  switchTabRaw('overview');
  renderMetadataBar();
  renderSummaryCards();
  renderBoostOverview();
  renderClustersSummarySection();
  populateCorrelationFilters();
  renderClusterGrid();
  renderCorrelationCards();
  renderDivergenceTable();
  // Reset the cluster-summary-graphs lazy-load marker so a new run is fetched fresh.
  const csGraphs = document.getElementById('cluster-summary-graphs-section');
  if (csGraphs) {
    csGraphs.removeAttribute('open');
    delete csGraphs.dataset.loaded;
  }
  // Same lazy-load reset for the IPC section: navigating to a different run
  // must fetch fresh data, not reuse the previous run's cached render.
  const ipcSec = document.getElementById('cluster-ip-count-section');
  if (ipcSec) {
    ipcSec.removeAttribute('open');
    delete ipcSec.dataset.rendered;
  }
  renderClusterIpCountSectionInit();
}

// Build the dropdown options for both the correlations and divergences cluster filters.
// Uses cluster_trends as the canonical source of cluster names for this run.
function populateCorrelationFilters() {
  const clusters = (data.cluster_trends || []).map(c => c.cluster).sort();
  ['corr-cluster-filter', 'div-cluster-filter'].forEach(id => {
    const sel = document.getElementById(id);
    if (!sel) return;
    sel.innerHTML = '<option value="">All clusters (fleet)</option>' +
      clusters.map(c => `<option value="${escapeAttr(c)}">${escapeHtml(c)}</option>`).join('');
  });
}

function renderProjectChip() {
  const chip = document.getElementById('project-chip');
  if (!chip || !config || !config.gcpProjectId) return;
  chip.textContent = config.gcpProjectId;
  chip.style.display = 'inline-flex';
}

function showFatalError(title, body) {
  document.querySelector('main').innerHTML =
    '<div style="text-align:center;padding:60px;color:#8b949e;">' +
    `<h2>${title}</h2>` +
    `<p style="margin-top:12px">${body}</p>` +
    '</div>';
}

// ── Metadata Bar ────────────────────────────────────────────────────────────

function renderMetadataBar() {
  const m = data.metadata;
  // Compute age across all known runs so the cur-pill turquoise depth
  // matches what the landing page shows.
  const dates = (discoveredEntries || []).map(e => String(e.current_date || '')).filter(Boolean).sort();
  const minD = dates[0] || m.current_date;
  const maxD = dates[dates.length - 1] || m.current_date;
  const age = ageRatio(String(m.current_date), minD, maxD);
  document.getElementById('metadata-bar').innerHTML =
    `<span class="date-pill ref" title="Reference date">${formatDate(m.reference_date)}</span>` +
    `<span class="date-arrow">→</span>` +
    `<span class="date-pill cur" style="--age:${age.toFixed(3)}" title="Current date">${formatDate(m.current_date)}</span>` +
    `<span>Strategy: <strong>${escapeHtml(m.strategy)}</strong></span>` +
    `<span><strong>${m.total_clusters}</strong> clusters · <strong>${m.total_recipes}</strong> recipes</span>` +
    `<span class="metadata-generated">Generated: <strong>${formatDateTime(m.generated_at)}</strong></span>`;
}

// ── Summary Cards ───────────────────────────────────────────────────────────

function renderSummaryCards() {
  const s = data.trends_summary;
  const cards = [
    { cls: 'degraded', value: s.degraded, label: 'Degraded' },
    { cls: 'improved', value: s.improved, label: 'Improved' },
    { cls: 'stable', value: s.stable, label: 'Stable' },
    { cls: 'new', value: s.new_entries, label: 'New' },
    { cls: 'dropped', value: s.dropped_entries, label: 'Dropped' },
  ];
  document.getElementById('summary-cards').innerHTML = cards.map(c =>
    `<div class="summary-card ${c.cls}">
      <div class="value">${c.value}</div>
      <div class="label">${c.label}</div>
    </div>`
  ).join('');
}

// ── Cluster Grid ────────────────────────────────────────────────────────────

/**
 * Build the per-cluster boost-chip row shown at the top of each cluster card.
 * Walks `generationSummary.boost_groups` and surfaces every group (b14, b16, future
 * bNN) that has an entry whose `cluster` matches the requested clusterName, broken
 * down by state (new / re-boost / holding). Recipe-kind groups aggregate across
 * recipes; cluster-kind groups emit one chip per state. Returns an empty string when
 * the cluster has no boosts so the row collapses cleanly.
 */
function renderClusterBoostChips(clusterName) {
  if (!generationSummary || !Array.isArray(generationSummary.boost_groups)) return '';
  const chips = [];
  // Stable order across states so the eye sees `new → re-boost → holding`.
  const stateOrder = ['new', 're-boost', 'holding'];
  for (const g of generationSummary.boost_groups) {
    const code = (g.code || '').toLowerCase();
    const matching = (g.entries || []).filter(e => e.cluster === clusterName);
    if (matching.length === 0) continue;
    const isRecipe = matching.some(e => Array.isArray(e.recipes) && e.recipes.length);
    const stateCounts = { 'new': 0, 're-boost': 0, 'holding': 0 };
    if (isRecipe) {
      for (const e of matching) {
        for (const r of (e.recipes || [])) {
          const st = (r && typeof r.state === 'string') ? r.state
                   : (r && r.propagated === true) ? 'holding'
                   : 'new';
          stateCounts[st] = (stateCounts[st] || 0) + 1;
        }
      }
    } else {
      for (const e of matching) {
        const st = (typeof e.state === 'string' && e.state) ? e.state : 'new';
        stateCounts[st] = (stateCounts[st] || 0) + 1;
      }
    }
    for (const st of stateOrder) {
      if (!stateCounts[st]) continue;
      const countSuffix = stateCounts[st] > 1 ? ` ×${stateCounts[st]}` : '';
      const tip = `${code.toUpperCase()} · ${st}${countSuffix} for this cluster`;
      chips.push(
        `<span class="cluster-boost-chip ${escapeAttr(code)}" title="${escapeAttr(tip)}">` +
          `<span class="bxx-badge">${escapeHtml(code)}</span>` +
          `<span class="boost-state-chip chip-${escapeAttr(st)}">${escapeHtml(st)}</span>` +
          (countSuffix ? `<span class="cluster-boost-count">${escapeHtml(countSuffix)}</span>` : '') +
        `</span>`
      );
    }
  }
  return chips.length ? `<div class="cluster-boost-row">${chips.join('')}</div>` : '';
}

/**
 * Compact boost panel rendered at the top of the cluster-detail pane. Generic over
 * b14/b16/future bNN: one per-group box per matching code, with a count summary
 * (`N new · M holding`) and a scrollable list of entries. Each entry shows the state
 * chip, the data that matters for that kind (memory transition for recipe-kind,
 * promotion + reason snippet for cluster-kind), and a tooltip with the full reason.
 *
 * Box width is bounded and entry list is independently scrollable so the whole panel
 * stays small at the top of the detail page even with many boosts.
 */
function renderClusterDetailBoosts(clusterName) {
  const host = document.getElementById('detail-cluster-boosts');
  if (!host) return;
  if (!generationSummary || !Array.isArray(generationSummary.boost_groups)) {
    host.innerHTML = '';
    return;
  }
  const boxes = [];
  for (const g of generationSummary.boost_groups) {
    const code = (g.code || '').toLowerCase();
    const matching = (g.entries || []).filter(e => e.cluster === clusterName);
    if (matching.length === 0) continue;
    const isRecipe = matching.some(e => Array.isArray(e.recipes) && e.recipes.length);
    const stateCounts = { 'new': 0, 're-boost': 0, 'holding': 0 };

    // Build per-entry rows with the data that matters for this kind. Each row carries
    // the data attributes a delegated click handler uses to navigate.
    //   recipe-kind (b16) → navigate to the recipe modal for that cluster+recipe
    //   cluster-kind (b14) → scroll to the in-page Cluster Configuration section
    //                        which shows master_machine_type / promotion in context
    const rows = [];
    if (isRecipe) {
      for (const e of matching) {
        for (const r of (e.recipes || [])) {
          const st = (r && typeof r.state === 'string') ? r.state
                   : (r && r.propagated === true) ? 'holding'
                   : 'new';
          stateCounts[st] = (stateCounts[st] || 0) + 1;
          const m = r.spark_executor_memory || {};
          const fac = m.factor;
          const cum = m.cumulative_factor;
          let memTxt = '';
          if (m.from && m.to) {
            memTxt = (st === 'holding')
              ? `${m.to}${fac ? ` ×${fac}` : ''}`
              : `${m.from} → ${m.to}${fac ? ` ×${fac}` : ''}${cum && st === 're-boost' ? ` (cum ×${cum})` : ''}`;
          }
          const recipeShort = (r.recipe || '').replace(/^_/, '').replace(/\.json$/, '');
          const recipeFilename = r.recipe_filename || ('_' + (r.recipe || '') + '.json');
          rows.push({
            kind: 'recipe',
            st,
            primary: recipeShort,
            secondary: memTxt,
            tip: recipeFilename + ' · click to open recipe spark conf',
            cluster: e.cluster,
            recipe: recipeFilename
          });
        }
      }
    } else {
      for (const e of matching) {
        const st = (typeof e.state === 'string' && e.state) ? e.state : 'new';
        stateCounts[st] = (stateCounts[st] || 0) + 1;
        const promo = e.promotion ? `${e.promotion.from} → ${e.promotion.to}` : '';
        rows.push({
          kind: 'cluster',
          st,
          primary: promo,
          secondary: e.reason || '',
          tip: (e.reason || '') + ' · click to scroll to cluster configuration',
          cluster: e.cluster
        });
      }
    }

    // Compose count summary like "1 new · 2 holding" (omit zero buckets).
    const summaryParts = [];
    if (stateCounts['new']) summaryParts.push(`${stateCounts['new']} new`);
    if (stateCounts['re-boost']) summaryParts.push(`${stateCounts['re-boost']} re-boost`);
    if (stateCounts['holding']) summaryParts.push(`${stateCounts['holding']} holding`);
    const countSummary = summaryParts.join(' · ');

    const rowsHtml = rows.map(r => {
      const dataAttrs = r.kind === 'recipe'
        ? `data-action="recipe" data-cluster="${escapeAttr(r.cluster)}" data-recipe="${escapeAttr(r.recipe)}"`
        : `data-action="cluster-conf" data-cluster="${escapeAttr(r.cluster)}"`;
      return `
        <button type="button" class="detail-boost-entry state-${escapeAttr(r.st)}" ${dataAttrs} title="${escapeAttr(r.tip)}">
          <span class="boost-state-chip chip-${escapeAttr(r.st)}">${escapeHtml(r.st)}</span>
          <span class="entry-primary">${escapeHtml(r.primary)}</span>
          ${r.secondary ? `<span class="entry-secondary">${escapeHtml(r.secondary)}</span>` : ''}
        </button>
      `;
    }).join('');

    boxes.push(`
      <div class="detail-boost-group ${escapeAttr(code)}">
        <div class="detail-boost-header">
          <span class="bxx-badge">${escapeHtml(code)}</span>
          <span class="boost-title">${escapeHtml(g.title || code.toUpperCase())}</span>
          <span class="boost-count-summary">${escapeHtml(countSummary)}</span>
        </div>
        <div class="detail-boost-entries">${rowsHtml}</div>
      </div>
    `);
  }
  host.innerHTML = boxes.length ? `<div class="detail-cluster-boosts">${boxes.join('')}</div>` : '';

  // Delegated click + auxclick (middle-click to open in new tab) on entry buttons.
  host.querySelectorAll('.detail-boost-entry').forEach(btn => {
    btn.addEventListener('click', (ev) => {
      ev.stopPropagation();
      const action = btn.dataset.action;
      if (action === 'recipe') {
        navigate({ cluster: btn.dataset.cluster, recipe: btn.dataset.recipe });
      } else if (action === 'cluster-conf') {
        const target = document.getElementById('detail-cluster-conf');
        if (target) {
          target.scrollIntoView({ behavior: 'smooth', block: 'start' });
          target.classList.add('flash-highlight');
          setTimeout(() => target.classList.remove('flash-highlight'), 1200);
        }
      }
    });
    btn.addEventListener('auxclick', (ev) => {
      if (ev.button !== 1) return;
      ev.preventDefault(); ev.stopPropagation();
      if (btn.dataset.action === 'recipe') {
        const url = buildUrl(Object.assign({}, parseRoute(), {
          cluster: btn.dataset.cluster, recipe: btn.dataset.recipe
        }));
        window.open(url, '_blank');
      }
    });
  });
}

function renderClusterGrid() {
  const search = document.getElementById('cluster-search').value.toLowerCase();
  const trendFilter = document.getElementById('trend-filter').value;

  let clusters = data.cluster_trends;

  if (search) {
    clusters = clusters.filter(c => c.cluster.toLowerCase().includes(search));
  }

  if (trendFilter !== 'all') {
    clusters = clusters.filter(c =>
      c.recipes.some(r => r.trend === trendFilter)
    );
  }

  const grid = document.getElementById('cluster-grid');
  grid.innerHTML = clusters.map(c => {
    const recipePills = c.recipes.slice(0, 6).map(r => {
      const shortName = r.recipe.replace(/^_/, '').replace(/\.json$/, '');
      const displayName = shortName.length > 25 ? shortName.slice(0, 22) + '...' : shortName;
      return `<span class="pill ${r.trend}" title="${escapeAttr(r.recipe)}: ${r.trend} (${r.action})">${escapeHtml(displayName)}</span>`;
    }).join('');
    const extra = c.recipes.length > 6 ? `<span class="pill stable">+${c.recipes.length - 6} more</span>` : '';
    const boostRow = renderClusterBoostChips(c.cluster);

    return `<div class="cluster-card" data-cluster="${escapeAttr(c.cluster)}"
                 onmouseenter="showClusterTooltip(event, '${escapeAttr(c.cluster)}')"
                 onmouseleave="hideTooltip()">
      <span class="trend-indicator ${c.overall_trend}"></span>
      ${boostRow}
      <div class="cluster-name-row">
        <div class="cluster-name" title="${escapeAttr(c.cluster)}">${escapeHtml(c.cluster)}</div>
        ${copyIcon(c.cluster)}
      </div>
      <div class="cluster-meta">${c.recipes.length} recipes · ${c.overall_trend}</div>
      <div class="cluster-conf-mini" data-conf-for="${escapeAttr(c.cluster)}">
        <span class="k">loading conf…</span>
      </div>
      <div class="recipe-pills">${recipePills}${extra}</div>
    </div>`;
  }).join('');

  grid.querySelectorAll('.cluster-card').forEach(card => {
    card.addEventListener('click', () => navigate({ cluster: card.dataset.cluster, recipe: null }));
    card.addEventListener('auxclick', (e) => {
      if (e.button !== 1) return;
      e.preventDefault();
      window.open(buildUrl(Object.assign({}, parseRoute(), { cluster: card.dataset.cluster, recipe: null })), '_blank');
    });
    // Lazy-load mini conf
    populateMiniConf(card.dataset.cluster);
  });
}

async function populateMiniConf(clusterName) {
  const slot = document.querySelector(`.cluster-conf-mini[data-conf-for="${cssEscape(clusterName)}"]`);
  if (!slot) return;
  const { ref, cur } = await loadClusterJsonsForDates(clusterName);
  const refConf = extractClusterConf(ref, clusterName);
  const curConf = extractClusterConf(cur, clusterName);
  const conf = curConf || refConf;
  if (!conf) {
    slot.innerHTML = '<span class="k">conf unavailable</span>';
    return;
  }
  // Order matters: master | worker, then workers | jobs.
  const fields = [
    ['master', conf.master_machine_type],
    ['worker', conf.worker_machine_type],
    ['workers', conf.num_workers],
    ['jobs', conf.total_no_of_jobs],
  ];
  slot.innerHTML = fields.map(([k, v]) => {
    if (v === undefined || v === null) return '';
    const refV = refConf ? refConf[keyForLabel(k)] : undefined;
    const changed = refConf && curConf && refV !== undefined && refV !== v;
    return `<span><span class="k">${k}:</span> <span class="${changed ? 'changed' : ''}" title="${changed ? 'ref: ' + escapeAttr(String(refV)) : ''}">${escapeHtml(String(v))}</span></span>`;
  }).join('');
}

function keyForLabel(label) {
  return ({ workers: 'num_workers', worker: 'worker_machine_type', master: 'master_machine_type', jobs: 'total_no_of_jobs' })[label] || label;
}

function extractClusterConf(json, clusterName) {
  if (!json || !json.clusterConf) return null;
  return json.clusterConf[clusterName] || null;
}

function extractRecipeConf(json, recipeName) {
  if (!json || !json.recipeSparkConf) return null;
  return json.recipeSparkConf[recipeName] || null;
}

// ── Cluster Tooltip ─────────────────────────────────────────────────────────

function showClusterTooltip(event, clusterName) {
  const cluster = data.cluster_trends.find(c => c.cluster === clusterName);
  if (!cluster) return;

  const degraded = cluster.recipes.filter(r => r.trend === 'degraded').length;
  const improved = cluster.recipes.filter(r => r.trend === 'improved').length;
  const stable = cluster.recipes.filter(r => r.trend === 'stable').length;

  let html = `<strong>${escapeHtml(clusterName)}</strong><br>`;
  html += `<table>`;
  if (degraded) html += `<tr><td class="pct-pos">${degraded} degraded</td></tr>`;
  if (improved) html += `<tr><td class="pct-neg">${improved} improved</td></tr>`;
  if (stable) html += `<tr><td>${stable} stable</td></tr>`;
  html += `</table>`;

  // Cluster conf grid: master | worker, then workers · jobs underneath.
  // We pull from any already-cached cluster JSON (loaded by populateMiniConf).
  const refDate = data.metadata.reference_date;
  const curDate = data.metadata.current_date;
  const conf = extractClusterConf(clusterJsonCache[`${curDate}/${clusterName}`], clusterName)
            || extractClusterConf(clusterJsonCache[`${refDate}/${clusterName}`], clusterName);
  if (conf) {
    html += `<div class="tooltip-conf-grid">`;
    html += `<div><span class="k">master:</span><span class="v">${escapeHtml(String(conf.master_machine_type ?? '—'))}</span></div>`;
    html += `<div><span class="k">worker:</span><span class="v">${escapeHtml(String(conf.worker_machine_type ?? '—'))}</span></div>`;
    html += `<div><span class="k">workers:</span><span class="v">${escapeHtml(String(conf.num_workers ?? '—'))}</span></div>`;
    html += `<div><span class="k">jobs:</span><span class="v">${escapeHtml(String(conf.total_no_of_jobs ?? '—'))}</span></div>`;
    html += `</div>`;
  }

  const topDegraded = cluster.recipes
    .filter(r => r.trend === 'degraded')
    .slice(0, 3);
  if (topDegraded.length > 0) {
    html += `<hr style="border-color:#30363d;margin:6px 0">`;
    topDegraded.forEach(r => {
      const p95 = r.deltas.find(d => d.metric === 'p95_job_duration_ms');
      if (p95) {
        html += `<div style="font-size:10px;color:#8b949e">${escapeHtml(r.recipe.slice(0, 30))}: ` +
          `<span class="pct-pos">+${p95.pct_change.toFixed(1)}% p95</span></div>`;
      }
    });
  }

  tooltip.innerHTML = html;
  tooltip.style.display = 'block';
  positionTooltip(event);
}

function hideTooltip() {
  tooltip.style.display = 'none';
}

function positionTooltip(event) {
  const x = event.clientX + 12;
  const y = event.clientY + 12;
  tooltip.style.left = Math.min(x, window.innerWidth - 420) + 'px';
  tooltip.style.top = Math.min(y, window.innerHeight - 200) + 'px';
}

document.addEventListener('mousemove', (e) => {
  if (tooltip.style.display === 'block') positionTooltip(e);
});

// ── Cluster Detail ──────────────────────────────────────────────────────────

async function showClusterDetail(clusterName) {
  navigate({ cluster: clusterName, recipe: null });
}

async function showClusterDetailRaw(clusterName) {
  const cluster = data.cluster_trends.find(c => c.cluster === clusterName);
  if (!cluster) return;

  document.getElementById('overview').classList.remove('active');
  document.getElementById('cluster-detail').style.display = 'block';
  document.getElementById('detail-cluster-name').innerHTML =
    `<span>${escapeHtml(clusterName)} <span style="color:#8b949e">(${cluster.overall_trend})</span></span>` +
    copyIcon(clusterName);

  // Boost summary box at the top — compact, scrollable, generic over b14/b16/future bNN.
  renderClusterDetailBoosts(clusterName);

  // Cluster conf comparison (loads asynchronously)
  document.getElementById('detail-cluster-conf').innerHTML =
    `<h3>Cluster Configuration <span class="info-icon" data-doc-key="trend" title="Compares reference vs current date config">ⓘ</span></h3>` +
    `<div class="empty-msg">Loading cluster configurations…</div>`;

  loadClusterJsonsForDates(clusterName).then(({ ref, cur, prevRef, refDate, curDate, prevRefDate }) => {
    renderClusterConfComparison(clusterName, ref, cur, refDate, curDate);
    annotateKeptRecipeCards(cur);
    renderDetailClusterCost(clusterName, ref, cur, refDate, curDate, prevRef, prevRefDate);
  });

  // Recipe cards with delta tables
  const recipesHtml = cluster.recipes.map(r => {
    const deltaRows = r.deltas.map(d => {
      const pctCls = d.pct_change > 0 ? 'pct-pos' : d.pct_change < 0 ? 'pct-neg' : '';
      return `<tr>
        <td class="metric-name">${labelMetric(d.metric)}</td>
        <td>${formatMetricValue(d.metric, d.reference)}</td>
        <td>${formatMetricValue(d.metric, d.current)}</td>
        <td class="${pctCls}">${d.pct_change > 0 ? '+' : ''}${d.pct_change.toFixed(1)}%</td>
      </tr>`;
    }).join('');

    return `<div class="detail-recipe-card" data-recipe="${escapeAttr(r.recipe)}" data-cluster="${escapeAttr(clusterName)}">
      <h4>
        <span class="pill ${r.trend}">${r.trend}</span>
        <span class="recipe-name-text" title="${escapeAttr(r.recipe)}">${escapeHtml(r.recipe)}</span>
        ${copyIcon(r.recipe)}
      </h4>
      <div class="detail-meta">
        Action: ${escapeHtml(r.action)} · Confidence: ${(r.confidence * 100).toFixed(0)}%
        <span class="info-icon" data-doc-key="confidence">ⓘ</span>
      </div>
      <table class="delta-table">
        <tr style="color:#8b949e"><td>Metric</td><td>Ref</td><td>Cur</td><td>Change</td></tr>
        ${deltaRows}
      </table>
    </div>`;
  }).join('');

  const recipesContainer = document.getElementById('detail-recipes');
  recipesContainer.innerHTML = recipesHtml;
  recipesContainer.querySelectorAll('.detail-recipe-card').forEach(card => {
    card.addEventListener('click', (e) => {
      if (e.target.closest('.copy-btn') || e.target.closest('.info-icon')) return;
      navigate({ recipe: card.dataset.recipe });
    });
    card.addEventListener('auxclick', (e) => {
      if (e.button !== 1) return;
      e.preventDefault();
      window.open(buildUrl(Object.assign({}, parseRoute(), { recipe: card.dataset.recipe })), '_blank');
    });
  });

  renderDetailCharts(cluster, clusterName);
  renderClusterDetailCorrelations(clusterName);
  renderClusterDetailOutliers(clusterName);
}

// Append a KEPT pill to recipe cards whose entry in the freshly tuned JSON
// carries `keptWithoutCurrentDate: true`. Called after the cluster JSON is
// loaded asynchronously, so the cards already exist when this runs.
function annotateKeptRecipeCards(curJson) {
  if (!curJson || !curJson.recipeSparkConf) return;
  const conf = curJson.recipeSparkConf;
  document.querySelectorAll('.detail-recipe-card').forEach(card => {
    const recipe = card.dataset.recipe;
    const entry = conf[recipe];
    if (!entry || entry.keptWithoutCurrentDate !== true) return;
    const heading = card.querySelector('h4');
    if (!heading) return;
    if (heading.querySelector('.kept-pill')) return; // idempotent
    const pill = document.createElement('span');
    pill.className = 'kept-pill';
    pill.textContent = 'KEPT';
    if (typeof entry.lastTunedDate === 'string' && entry.lastTunedDate.length > 0) {
      pill.title = `Last tuned on ${entry.lastTunedDate} — config carried over (recipe absent from current date)`;
    } else {
      pill.title = 'Config carried from a previous date — recipe absent from current date';
    }
    const nameSpan = heading.querySelector('.recipe-name-text');
    if (nameSpan && nameSpan.parentNode) {
      nameSpan.parentNode.insertBefore(pill, nameSpan.nextSibling);
    } else {
      heading.appendChild(pill);
    }
  });
}

// Per-cluster correlation cards (delta view) shown in the cluster-detail page.
// Reuses the same scatter / interpretation rendering as the global tab so the
// look-and-feel stays consistent.
function renderClusterDetailCorrelations(clusterName) {
  const host = document.getElementById('detail-correlations-body');
  if (!host) return;
  const perCluster = (data.correlations_per_cluster || {})[clusterName];
  if (!perCluster || perCluster.length === 0) {
    host.innerHTML = `<div class="fallback-banner">This cluster has fewer than 5 paired recipes — see the global Correlations tab for fleet-wide values.</div>`;
    return;
  }
  const cards = perCluster.map(c => {
    const r = c.pearson;
    const shortA = labelMetric(c.metric_a);
    const shortB = labelMetric(c.metric_b);
    const color = corrColor(r);
    const points = scatterPointsFor('delta', c.metric_a, c.metric_b, clusterName);
    return `<div class="corr-card">
      <div class="corr-card-pair">
        <span class="corr-card-metric">${shortA}</span>
        <span class="corr-card-arrow">↔</span>
        <span class="corr-card-metric">${shortB}</span>
        <span class="info-icon" data-doc-key="pearson_full">ⓘ</span>
      </div>
      <div class="corr-card-body">
        <div class="corr-card-value" style="background:${color}">${r.toFixed(3)}</div>
        <div class="corr-card-meta"><div>n=${c.n}</div><div class="corr-card-view">cluster-scope</div></div>
        ${renderMiniScatter(points, shortA, shortB)}
      </div>
      <div class="corr-card-interp">${interpretCorrelation(r, shortA, shortB)}</div>
    </div>`;
  }).join('');
  host.innerHTML = `<div class="corr-card-grid">${cards}</div>`;
}

// Per-cluster z-score outliers shown in the cluster-detail page.
function renderClusterDetailOutliers(clusterName) {
  const host = document.getElementById('detail-outliers-body');
  if (!host) return;
  const perCluster = (data.divergences_per_cluster || {})[clusterName];
  let rowsArr;
  let bannerHtml = '';
  if (perCluster && perCluster.length >= 0 && (data.divergences_per_cluster || {})[clusterName] !== undefined) {
    rowsArr = perCluster;
  } else {
    bannerHtml = `<div class="fallback-banner">This cluster has fewer than 5 paired recipes — falling back to fleet z-scores filtered to this cluster.</div>`;
    rowsArr = (data.divergences || []).filter(d => d.cluster === clusterName);
  }
  rowsArr = rowsArr.slice().sort((a, b) => Math.abs(b.z_score) - Math.abs(a.z_score));
  if (rowsArr.length === 0) {
    host.innerHTML = bannerHtml + `<div class="empty-msg">No outliers detected for this cluster.</div>`;
    return;
  }
  const rows = rowsArr.map(divergenceRowHtml).join('');
  host.innerHTML = bannerHtml + `<table class="cluster-outlier-table">
    <thead><tr><th>Cluster</th><th>Recipe</th><th>Metric</th><th>Reference</th><th>Current</th><th>Z-Score</th></tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
  // Wire clickable rows → navigate to recipe tuning details (mirrors fleet
  // divergence-table behaviour). Uses `divergenceRowHtml`'s data-cluster /
  // data-recipe attrs.
  host.querySelectorAll('table.cluster-outlier-table tbody tr.div-row-clickable').forEach(row => {
    row.addEventListener('click', (e) => {
      if (e.target.closest('.copy-btn')) return;
      navigate({ cluster: row.dataset.cluster, recipe: row.dataset.recipe });
    });
    row.addEventListener('auxclick', (e) => {
      if (e.button !== 1) return;
      e.preventDefault();
      const url = buildUrl(Object.assign({}, parseRoute(), { cluster: row.dataset.cluster, recipe: row.dataset.recipe }));
      window.open(url, '_blank');
    });
  });
}

// ── Detail Cluster Cost ─────────────────────────────────────────────────────
// Reads `cost_timeline` from each side's per-cluster JSON (emitted by the Scala
// tuner from b20/b21). Side absent (or empty) → empty-state notice on that side
// only. The chart renders a single time axis with gaps between incarnations and
// vertical create/delete markers. Click ⤢ on a side to expand it full width.

function _ctOf(json) {
  return (json && json.cost_timeline) ? json.cost_timeline : null;
}

// Convert ISO timestamp to epoch ms; safe for null/invalid input.
function _tsMs(s) { const t = new Date(s).getTime(); return Number.isFinite(t) ? t : null; }

// Format epoch ms as "HH:MM:SS" UTC for axis ticks and tooltip titles.
function _fmtHmsUtc(ms) {
  if (!Number.isFinite(ms)) return '';
  return new Date(ms).toISOString().slice(11, 19);
}

// Builds Chart.js point list for a single side using numeric x (epoch ms).
// Inserts an explicit y=null "gap" point between adjacent incarnations so
// spanGaps:false renders breaks. Uses a linear x scale so we don't need an
// extra date-adapter CDN script (Chart.js's time scale requires one).
function _buildDccChartData(ct) {
  if (!ct || !Array.isArray(ct.incarnations) || ct.incarnations.length === 0) {
    return { points: [], boundaries: [] };
  }
  const points = [];
  const boundaries = [];
  ct.incarnations.forEach((inc, i) => {
    const startMs = _tsMs(inc.span_start_ts);
    const endMs   = _tsMs(inc.span_end_ts);
    if (i > 0 && startMs != null) points.push({ x: startMs, y: null });
    if (startMs != null) boundaries.push({ x: startMs, kind: 'create', idx: inc.idx });
    (inc.intervals || []).forEach(iv => {
      const fromMs = _tsMs(iv.from_ts);
      const toMs   = _tsMs(iv.to_ts);
      if (fromMs != null) points.push({ x: fromMs, y: iv.workers, segCost: iv.seg_cost_eur, idx: inc.idx });
      if (toMs   != null) points.push({ x: toMs,   y: iv.workers, segCost: iv.seg_cost_eur, idx: inc.idx });
    });
    if (endMs != null) boundaries.push({ x: endMs, kind: 'delete', idx: inc.idx });
  });
  return { points, boundaries };
}

// Per-side palette for dataset-per-incarnation rendering. Each side has 4 tones
// rotating by `inc.idx % 4` so adjacent lifespans are visually distinct without
// requiring a legend. Side hues differ so prev/current/future stay distinct.
function _dccPalette(sideKind, idx) {
  const hueBySide = {
    prev:      [210, 200, 220, 195],   // blue family
    reference: [210, 200, 220, 195],   // blue family (back-compat)
    current:   [25, 35, 15, 40],       // orange family
    future:    [165, 170, 155, 180]    // teal family
  };
  const hues = hueBySide[sideKind] || hueBySide.current;
  const h = hues[Math.abs(idx) % hues.length];
  // Alternate lightness so adjacent incarnations of the same idx-parity still differ.
  const isEvenSlot = (Math.abs(idx) % 2) === 0;
  const sat   = isEvenSlot ? 70 : 55;
  const light = isEvenSlot ? 60 : 45;
  return {
    border: `hsl(${h} ${sat}% ${light}%)`,
    fill:   `hsla(${h}, ${sat}%, ${light}%, 0.22)`
  };
}

// One dataset per incarnation so the chart can colour-alternate them; the
// dccBoundaryMarkers plugin still draws create/delete dashed verticals.
function _buildDccDatasetsPerInc(ct, sideKind) {
  if (!ct || !Array.isArray(ct.incarnations) || ct.incarnations.length === 0) {
    return { datasets: [], boundaries: [] };
  }
  const datasets = [];
  const boundaries = [];
  ct.incarnations.forEach(inc => {
    const startMs = _tsMs(inc.span_start_ts);
    const endMs   = _tsMs(inc.span_end_ts);
    if (startMs != null) boundaries.push({ x: startMs, kind: 'create', idx: inc.idx });
    if (endMs != null) boundaries.push({ x: endMs, kind: 'delete', idx: inc.idx });
    const pts = [];
    (inc.intervals || []).forEach(iv => {
      const fromMs = _tsMs(iv.from_ts);
      const toMs   = _tsMs(iv.to_ts);
      if (fromMs != null) pts.push({ x: fromMs, y: iv.workers, segCost: iv.seg_cost_eur, idx: inc.idx });
      if (toMs   != null) pts.push({ x: toMs,   y: iv.workers, segCost: iv.seg_cost_eur, idx: inc.idx });
    });
    if (pts.length === 0) return;
    const colors = _dccPalette(sideKind, inc.idx);
    datasets.push({
      label: `#${inc.idx}`,
      data: pts,
      parsing: false,
      borderColor: colors.border,
      backgroundColor: colors.fill,
      borderWidth: 1.5,
      fill: 'origin',
      spanGaps: false,
      stepped: true,
      pointRadius: 0,
      pointHoverRadius: 4,
      _incIdx: inc.idx
    });
  });
  return { datasets, boundaries };
}

// Plugin: vertical dashed lines at each incarnation create/delete timestamp.
const dccBoundaryMarkersPlugin = {
  id: 'dccBoundaryMarkers',
  afterDatasetsDraw(chart, _, opts) {
    const boundaries = opts && opts.boundaries;
    if (!Array.isArray(boundaries) || boundaries.length === 0) return;
    const { ctx, scales: { x, y } } = chart;
    if (!x || !y) return;
    ctx.save();
    ctx.setLineDash([4, 3]);
    ctx.lineWidth = 1;
    boundaries.forEach(b => {
      const px = x.getPixelForValue(b.x);
      if (!Number.isFinite(px)) return;
      ctx.strokeStyle = b.kind === 'create' ? 'rgba(63,185,80,0.65)' : 'rgba(248,81,73,0.65)';
      ctx.beginPath();
      ctx.moveTo(px, y.top);
      ctx.lineTo(px, y.bottom);
      ctx.stroke();
    });
    ctx.restore();
  }
};

function _formatEur(n) {
  if (n == null || !Number.isFinite(+n)) return '—';
  const v = +n;
  return `€${v.toFixed(v >= 1 ? 2 : 4)}`;
}

function _formatPct(n) {
  if (n == null || !Number.isFinite(+n)) return '—';
  const sign = n > 0 ? '+' : '';
  return `${sign}${(+n).toFixed(1)}%`;
}

function _formatMin(n) {
  if (n == null || !Number.isFinite(+n)) return '—';
  return `${(+n).toFixed(0)} min`;
}

// Top KPI delta strip: ±cost%, ±avg workers, ±total minutes between the
// "previous → reference" card and the "reference → current" card. This is the
// meaningful adjacent-period comparison (both periods used a deployed machine
// that matched their observation date) — a true day-over-day cost evolution.
function _buildDccDeltasHtml(prevCt, currentCt) {
  const chip = (label, valueHtml, cls) =>
    `<div class="dcc-chip ${cls}"><span class="dcc-chip-label">${label}</span><span class="dcc-chip-value">${valueHtml}</span></div>`;

  if (!prevCt && !currentCt) return '';
  if (!prevCt || !currentCt) {
    const note = !prevCt ? 'previous → reference' : 'reference → current';
    return `<div class="dcc-deltas-note">No autoscaling data exported for ${note} period — delta KPIs unavailable.</div>`;
  }
  const dCost = +currentCt.total_cost_eur - +prevCt.total_cost_eur;
  const prevCost = +prevCt.total_cost_eur || 0;
  const dCostPct = prevCost > 0 ? (dCost / prevCost) * 100 : null;
  const dAvgW = +currentCt.real_used_avg_num_of_workers - +prevCt.real_used_avg_num_of_workers;
  const prevMin = (prevCt.incarnations || []).reduce((s, i) => s + (+i.span_minutes || 0), 0);
  const curMin  = (currentCt.incarnations || []).reduce((s, i) => s + (+i.span_minutes || 0), 0);
  const dMin = curMin - prevMin;

  const cls = (v) => v == null ? '' : (v < 0 ? 'down' : (v > 0 ? 'up' : ''));

  return `<div class="dcc-deltas" title="Compares 'previous→reference' card to 'reference→current' card">
    ${chip('Δ cost', `${_formatEur(dCost)} <span class="dcc-chip-sub">(${_formatPct(dCostPct)})</span>`, cls(dCost))}
    ${chip('Δ avg workers', `${dAvgW > 0 ? '+' : ''}${dAvgW.toFixed(2)}`, cls(dAvgW))}
    ${chip('Δ active minutes', `${dMin > 0 ? '+' : ''}${dMin.toFixed(0)} min`, cls(dMin))}
  </div>`;
}

function _buildDccLifespanTable(ct) {
  if (!ct || !Array.isArray(ct.incarnations) || ct.incarnations.length === 0) return '';
  const headers = `<thead><tr>
    <th>#</th><th>Start</th><th>End</th><th>Duration</th>
    <th>Avg w</th><th>Min w</th><th>Max w</th>
    <th>Worker</th><th>Master</th>
    <th>€/h w</th><th>€/h m</th>
    <th>Cost €</th>
  </tr></thead>`;
  const fmtTs = (s) => {
    if (!s) return '—';
    const d = new Date(s);
    return Number.isNaN(d.getTime()) ? escapeHtml(s) : `${d.toISOString().slice(11, 19)}Z`;
  };
  const rows = ct.incarnations.map(inc => {
    const wCost = +inc.worker_cost_eur || 0;
    const mCost = +inc.master_cost_eur || 0;
    const tCost = +inc.total_cost_eur || (wCost + mCost);
    return `<tr>
      <td>${inc.idx}</td>
      <td title="${escapeAttr(inc.span_start_ts || '')}">${fmtTs(inc.span_start_ts)}</td>
      <td title="${escapeAttr(inc.span_end_ts || '')}">${fmtTs(inc.span_end_ts)}</td>
      <td>${(+inc.span_minutes || 0).toFixed(0)} min</td>
      <td>${(+inc.avg_workers || 0).toFixed(2)}</td>
      <td>${inc.min_workers ?? 0}</td>
      <td>${inc.max_workers ?? 0}</td>
      <td><code>${escapeHtml(ct.worker_machine_type || '')}</code></td>
      <td><code>${escapeHtml(ct.master_machine_type || '')}</code></td>
      <td>${(+ct.worker_hourly_eur || 0).toFixed(4)}</td>
      <td>${(+ct.master_hourly_eur || 0).toFixed(4)}</td>
      <td title="worker ${_formatEur(wCost)} + master ${_formatEur(mCost)}">${_formatEur(tCost)}</td>
    </tr>`;
  }).join('');
  return `<table class="dcc-lifespans">${headers}<tbody>${rows}</tbody></table>`;
}

function _buildDccTotalsHtml(ct) {
  if (!ct) return '';
  const wMt = ct.worker_machine_type || '—';
  const mMt = ct.master_machine_type || '—';
  return `<div class="dcc-totals">
    <div class="dcc-totals-cost">${_formatEur(ct.total_cost_eur)}</div>
    <div class="dcc-totals-meta">
      avg ${(+ct.real_used_avg_num_of_workers || 0).toFixed(2)} w · min ${ct.real_used_min_workers ?? 0} · max ${ct.real_used_max_workers ?? 0}
      <br><code>${escapeHtml(wMt)}</code> + <code>${escapeHtml(mMt)}</code>
    </div>
  </div>`;
}

// Format epoch ms as YYYY-MM-DD UTC. Used for observed-date chip in side header.
function _fmtYmdUtc(ms) {
  if (!Number.isFinite(ms)) return '';
  return new Date(ms).toISOString().slice(0, 10);
}

// Compute observed date(s) from incarnations: returns "YYYY-MM-DD" or
// "YYYY-MM-DD → YYYY-MM-DD" when spans cross days. Returns null on empty/invalid.
function _observedDateRange(ct) {
  if (!ct || !Array.isArray(ct.incarnations) || ct.incarnations.length === 0) return null;
  const startMs = _tsMs(ct.incarnations[0].span_start_ts);
  const endMs   = _tsMs(ct.incarnations[ct.incarnations.length - 1].span_end_ts);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return null;
  const startYmd = _fmtYmdUtc(startMs);
  const endYmd   = _fmtYmdUtc(endMs);
  return startYmd === endYmd ? startYmd : `${startYmd} → ${endYmd}`;
}

// Build the chip showing the run-date and (when different) the observed date(s)
// pulled from the actual b20/b21 timestamps. Helps surface input-data quirks.
function _dccDateChipHtml(dateLabel, observedRange) {
  if (!dateLabel && !observedRange) return '';
  const runDateYmd = (dateLabel || '').replace(/_/g, '-');
  const showObserved = observedRange && observedRange !== runDateYmd;
  if (showObserved) {
    return `<span class="dcc-side-date" title="Run date · observed dates from b20/b21">(${escapeHtml(dateLabel)} · observed ${escapeHtml(observedRange)})</span>`;
  }
  return dateLabel ? `<span class="dcc-side-date">(${escapeHtml(dateLabel)})</span>` : '';
}

// Render one side (reference or current). When ct is null → empty-state notice.
function _renderDccSide(sideEl, ct, dateLabel) {
  const sideKind = sideEl.dataset.side;
  const headerLabel = sideKind === 'prev' ? 'Previous → Reference'
                    : sideKind === 'future' ? 'Future projection'
                    : sideKind === 'reference' ? 'Reference'
                    : sideKind === 'current' ? 'Reference → Current'
                    : 'Current';
  const observedRange = _observedDateRange(ct);
  const dateChip = _dccDateChipHtml(dateLabel, observedRange);
  const synthChip = (ct && ct.has_synthetic_span === true)
    ? `<span class="dcc-synthetic-tag" title="Span boundaries inferred from b21 autoscaler events; no b20 row was found for this cluster.">b22 · synthetic span</span>`
    : '';
  // Only render the b23 chip when we have positive evidence: every incarnation
  // must explicitly carry events_count == 0. Older JSON outputs lack the field
  // entirely — treat "missing" as "unknown" (don't show), not "zero".
  const noEventsChip = (ct && Array.isArray(ct.incarnations) && ct.incarnations.length > 0
                       && ct.incarnations.every(i => i && i.events_count != null && (+i.events_count) === 0))
    ? `<span class="dcc-fallback-tag" title="No autoscaler events for any incarnation; using recommended worker count flat across the span.">b23 · no autoscaler events</span>`
    : '';
  if (!ct || !Array.isArray(ct.incarnations) || ct.incarnations.length === 0) {
    sideEl.innerHTML = `
      <header class="dcc-side-header">
        <h4>${headerLabel} ${dateChip}</h4>
        <button class="dcc-side-minimise" title="Hide card">−</button>
        <button class="dcc-side-expand" title="Expand">⤢</button>
      </header>
      <div class="dcc-side-body">
        <div class="empty-msg">No autoscaling data exported for this date.</div>
      </div>`;
    return;
  }

  const canvasId = `dcc-canvas-${sideKind}-${Math.random().toString(36).slice(2, 8)}`;
  sideEl.innerHTML = `
    <header class="dcc-side-header">
      <h4>${headerLabel} ${dateChip} ${synthChip} ${noEventsChip}</h4>
      <button class="dcc-side-reset" title="Reset zoom">↻</button>
      <button class="dcc-side-minimise" title="Hide card">−</button>
      <button class="dcc-side-expand" title="Expand">⤢</button>
    </header>
    <div class="dcc-side-body">
      ${_buildDccTotalsHtml(ct)}
      <div class="dcc-chart-wrap"><canvas id="${canvasId}"></canvas></div>
      ${_buildDccLifespanTable(ct)}
    </div>`;

  const { datasets: dccDatasets, boundaries } = _buildDccDatasetsPerInc(ct, sideKind);
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  // Destroy any prior instance bound to this canvas (defensive — id is random
  // here but re-renders on the same side reuse the wrap element).
  if (canvas._chartInstance) { try { canvas._chartInstance.destroy(); } catch (e) {} }

  const tooltipCallbacks = {
    title: (items) => items[0] ? _fmtHmsUtc(items[0].parsed.x) + 'Z' : '',
    label: (ctx) => {
      const raw = ctx.raw || {};
      const w = (raw.y == null) ? '—' : raw.y;
      const cost = (raw.segCost == null) ? '' : ` · ${_formatEur(raw.segCost)}`;
      const inc = (raw.idx == null) ? '' : ` · lifespan #${raw.idx}`;
      return `workers ${w}${cost}${inc}`;
    }
  };

  const chart = new Chart(canvas, {
    type: 'line',
    data: {
      datasets: dccDatasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: tooltipCallbacks },
        zoom: csZoomPluginConfig(canvas, { clampX: true, clampY: true }),
        dccBoundaryMarkers: { boundaries }
      },
      scales: {
        x: {
          type: 'linear',
          ticks: {
            color: '#8b949e',
            maxRotation: 0,
            autoSkip: true,
            callback: (v) => _fmtHmsUtc(+v)
          },
          grid: { color: 'rgba(139,148,158,0.1)' }
        },
        y: {
          beginAtZero: true,
          title: { display: true, text: 'workers', color: '#8b949e' },
          ticks: { color: '#8b949e', precision: 0 },
          grid: { color: 'rgba(139,148,158,0.1)' }
        }
      }
    },
    plugins: [dccBoundaryMarkersPlugin]
  });
  canvas._chartInstance = chart;

  const resetBtn = sideEl.querySelector('.dcc-side-reset');
  if (resetBtn) resetBtn.addEventListener('click', () => { try { chart.resetZoom(); } catch (e) {} });
}

function renderDetailClusterCost(clusterName, refJson, curJson, refDate, curDate, prevRefJson, prevRefDate) {
  const body = document.getElementById('detail-cluster-cost-body');
  if (!body) return;

  const refCt     = _ctOf(refJson);
  const curCt     = _ctOf(curJson);
  const prevRefCt = _ctOf(prevRefJson);

  const refIncs = refCt && Array.isArray(refCt.incarnations) ? refCt.incarnations.length : 0;
  const curIncs = curCt && Array.isArray(curCt.incarnations) ? curCt.incarnations.length : 0;
  const refSynth = !!(refCt && refCt.has_synthetic_span);
  const curSynth = !!(curCt && curCt.has_synthetic_span);
  console.info(`[Cluster cost] ${clusterName} · prev-ref ${prevRefDate || 'none'} · ref ${refDate} (${refIncs} inc${refSynth ? ' synth' : ''}) · cur ${curDate} (${curIncs} inc${curSynth ? ' synth' : ''})`);

  if (!refCt && !curCt) {
    body.innerHTML = `<div class="empty-msg">No autoscaling data exported for either date — provide b20/b21 CSVs to see cost intervals.</div>`;
    return;
  }

  // Build the 3 cards. See plan/IMPROVEMENT_0:
  //   prev    = ref-date actuals × previous-reference-date machine (deployed during ref)
  //   current = cur-date actuals × reference-date machine (deployed during cur)
  //   future  = cur-date actuals × current-date machine (will be deployed next)
  const card1Ct = recomputeCostTimeline(refCt, prevRefCt || refCt);
  const card2Ct = recomputeCostTimeline(curCt, refCt || curCt);
  const card3Ct = curCt;

  body.innerHTML = `
    ${_buildDccDeltasHtml(card1Ct, card2Ct)}
    <div class="dcc-trio">
      <section class="dcc-side" data-side="prev"></section>
      <section class="dcc-side" data-side="current"></section>
      <section class="dcc-side" data-side="future"></section>
    </div>`;

  const prevSide   = body.querySelector('.dcc-side[data-side="prev"]');
  const currentSide = body.querySelector('.dcc-side[data-side="current"]');
  const futureSide = body.querySelector('.dcc-side[data-side="future"]');
  _renderDccSide(prevSide, card1Ct, refDate);
  _renderDccSide(currentSide, card2Ct, curDate);
  _renderDccSide(futureSide, card3Ct, curDate);

  // If there's no previous-reference run, surface the missing-history note.
  if (!prevRefCt && prevSide) {
    const note = document.createElement('div');
    note.className = 'dcc-side-note';
    note.textContent = 'No prior tuning indexed — using reference machines as a stand-in for previous deployment.';
    note.title = 'A run whose current_date matched this reference_date was not found in _analyses_index.json.';
    prevSide.appendChild(note);
  }

  const trio = body.querySelector('.dcc-trio');

  // Recompute the trio grid based on which sides are minimised. Visible cards
  // share the available space; minimised cards collapse to a thin header strip.
  // Middle (current) gets a slightly larger weight when visible — it's the hero.
  function _dccApplyTrioLayout() {
    if (!trio) return;
    if (trio.dataset.expanded) {
      trio.style.gridTemplateColumns = '';
      return;
    }
    const sides = trio.querySelectorAll('.dcc-side');
    const cols = [];
    sides.forEach(s => {
      if (s.dataset.minimised === 'true') {
        cols.push('auto');
      } else {
        cols.push(s.dataset.side === 'current' ? '1.15fr' : '1fr');
      }
    });
    trio.style.gridTemplateColumns = cols.join(' ');
    sides.forEach(s => {
      const canvas = s.querySelector('canvas');
      if (canvas && canvas._chartInstance) {
        setTimeout(() => canvas._chartInstance.resize(), 50);
      }
    });
  }

  // Wire ↔ expand on each side: hide others, expand self full width; toggle.
  body.querySelectorAll('.dcc-side-expand').forEach(btn => {
    btn.addEventListener('click', () => {
      const side = btn.closest('.dcc-side');
      const pair = side.parentElement;
      const which = side.dataset.side;
      const cur = pair.dataset.expanded || '';
      pair.dataset.expanded = (cur === which) ? '' : which;
      btn.textContent = pair.dataset.expanded === which ? '⤡' : '⤢';
      // Expanding a card overrides any minimise state on the others.
      pair.querySelectorAll('.dcc-side').forEach(s => { s.dataset.minimised = 'false'; });
      pair.querySelectorAll('.dcc-side-minimise').forEach(b => { b.textContent = '−'; b.title = 'Hide card'; });
      _dccApplyTrioLayout();
      const canvas = side.querySelector('canvas');
      if (canvas && canvas._chartInstance) {
        setTimeout(() => canvas._chartInstance.resize(), 50);
      }
    });
  });

  // Per-card minimise: collapses the card to a thin header strip; the remaining
  // visible cards reflow horizontally to fill the freed space. Click again to
  // restore. Disabled when the trio is in expanded (single-card) mode.
  body.querySelectorAll('.dcc-side-minimise').forEach(btn => {
    btn.addEventListener('click', () => {
      const side = btn.closest('.dcc-side');
      const pair = side.parentElement;
      // Minimising clears any expanded state — they're mutually exclusive views.
      if (pair.dataset.expanded) {
        pair.dataset.expanded = '';
        pair.querySelectorAll('.dcc-side-expand').forEach(b => { b.textContent = '⤢'; });
      }
      const minimised = side.dataset.minimised === 'true';
      // Don't allow minimising the last visible card.
      if (!minimised) {
        const visibleCount = Array.from(pair.querySelectorAll('.dcc-side'))
          .filter(s => s.dataset.minimised !== 'true').length;
        if (visibleCount <= 1) return;
      }
      side.dataset.minimised = minimised ? 'false' : 'true';
      btn.textContent = minimised ? '−' : '+';
      btn.title = minimised ? 'Hide card' : 'Show card';
      _dccApplyTrioLayout();
    });
  });

  _dccApplyTrioLayout();
}

function renderClusterConfComparison(clusterName, refJson, curJson, refDate, curDate) {
  const refConf = extractClusterConf(refJson, clusterName);
  const curConf = extractClusterConf(curJson, clusterName);
  const target = document.getElementById('detail-cluster-conf');

  if (!refConf && !curConf) {
    target.innerHTML =
      `<h3>Cluster Configuration</h3>` +
      `<div class="empty-msg">No cluster configuration JSON found for this cluster (looked under <code>${escapeHtml(refDate)}</code> and <code>${escapeHtml(curDate)}</code>).</div>` +
      triedPathsHtml(clusterName);
    return;
  }

  const allKeys = new Set([
    ...(refConf ? Object.keys(refConf) : []),
    ...(curConf ? Object.keys(curConf) : []),
  ]);
  const orderedKeys = orderConfKeys(Array.from(allKeys));

  const rows = orderedKeys.map(k => {
    const rv = refConf ? refConf[k] : undefined;
    const cv = curConf ? curConf[k] : undefined;
    const changed = rv !== undefined && cv !== undefined && JSON.stringify(rv) !== JSON.stringify(cv);
    return `<tr>
      <td class="key">${escapeHtml(k)}</td>
      <td>${rv === undefined ? '<span class="empty-msg">—</span>' : escapeHtml(String(rv))}</td>
      <td class="${changed ? 'changed' : ''}">${cv === undefined ? '<span class="empty-msg">—</span>' : escapeHtml(String(cv))}</td>
    </tr>`;
  }).join('');

  target.innerHTML =
    `<h3>Cluster Configuration <span class="info-icon" data-doc-key="trend" title="Compare cluster config across dates">ⓘ</span></h3>` +
    `<table class="cluster-conf-table">
       <thead><tr><th>Field</th><th>Reference (${escapeHtml(formatDate(refDate))})</th><th>Current (${escapeHtml(formatDate(curDate))})</th></tr></thead>
       <tbody>${rows}</tbody>
     </table>`;
}

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

  const summary = document.querySelector('#cluster-ip-count-section > summary > h3');
  if (summary) {
    const noRef = !refData || refData.segments.length === 0;
    const noCur = !curData || curData.segments.length === 0;
    summary.textContent = noRef && noCur
      ? 'Total estimated IP count (no data)'
      : 'Total estimated IP count';
  }

  const refSide = document.querySelector('.ipc-side[data-side="reference"]');
  const curSide = document.querySelector('.ipc-side[data-side="current"]');
  if (refSide) _ipcRenderSide(refSide, refData, ipQuota, refDate);
  if (curSide) _ipcRenderSide(curSide, curData, ipQuota, curDate);
  _ipcWireToggle();
  const initialMode = _ipcReadModeFromHash() || 'current';
  _ipcApplyMode(initialMode);
}

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
      const synthetic = inc.synthetic_span === true;
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
          idx: inc.idx,
          synthetic
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
  const loaded = pairs.filter(([_n, j]) => j != null).length;
  const withCt = pairs.filter(([_n, j]) => j && j.cost_timeline).length;
  const withSynthetic = pairs.filter(([_n, j]) => j && j.cost_timeline && j.cost_timeline.has_synthetic_span === true).length;
  console.info(`[IP count] ${date}: ${names.length} clusters, ${loaded} loaded, ${withCt} with cost_timeline, ${segments.length} segments, ${skipped.length} skipped, ${withSynthetic} synthetic-span`);
  return { date, segments, peakMs: peak.peakMs, peakIps: peak.peakIps, skipped, syntheticClusters: withSynthetic };
}

// Per-side cache: side name → loaded data + chart instance.
const _ipcSideCache = new Map();

// Render one side's full DOM. Chart wiring comes in subsequent tasks; for
// now this lays out the header / KPI strip / chart canvas / anchor chip /
// table shell so the visual structure is final.
function _ipcRenderSide(sideEl, sideData, ipQuota, dateLabel) {
  const sideName = sideEl.dataset.side; // 'reference' | 'current'
  const headerLabel = sideName === 'reference' ? 'Reference' : 'Current';

  // Destroy any prior chart before innerHTML detaches its canvas, otherwise
  // the old Chart.js instance keeps its event listeners and animation handles.
  if (sideEl._ipcChart) {
    try { sideEl._ipcChart.destroy(); } catch (e) {}
    sideEl._ipcChart = null;
  }

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

  // Side-level synthetic-span chip: at least one cluster on this side had its
  // span boundaries inferred from b21 events (b20 row missing). Per-row chips
  // continue to flag the specific clusters in the table below.
  const synthClusterCount = sideData.syntheticClusters || 0;
  const synthHeaderChip = synthClusterCount > 0
    ? `<span class="ipc-tag-synthetic" title="${synthClusterCount} cluster${synthClusterCount === 1 ? '' : 's'} on this side had span boundaries inferred from b21 autoscaler events because no b20 row was present.">b22 · ${synthClusterCount} synthetic span${synthClusterCount === 1 ? '' : 's'}</span>`
    : '';

  sideEl.innerHTML = `
    <header class="ipc-side-header">
      <h4>${headerLabel}${dateLabel ? ` <span class="ipc-side-date">(${escapeHtml(dateLabel)})</span>` : ''} ${synthHeaderChip}</h4>
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

    // Initial paint at peak (anchorMs and hoverMs both null → fallback to peakMs).
    _ipcRefreshSide(sideEl);
  }
}

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

    // Dashed lines and labels. Labels render as small rounded chips so they
    // don't merge visually when warn/crit/cap lines are near each other.
    // To prevent vertical collision, labels stack with a minimum 14px gap and
    // are nudged above/below the line based on density.
    const lines = [
      { v: warnAt, color: '#d4a72c', label: `warn ${Math.round(warnAt)}` },
      { v: critAt, color: '#f85149', label: `crit ${Math.round(critAt)}` },
      { v: cap,    color: '#f85149', label: `cap ${cap}`,  bold: true }
    ];
    ctx.save();
    ctx.setLineDash([4, 3]);
    ctx.font = '10px ui-monospace, monospace';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    // First pass: draw the dashed lines.
    lines.forEach(line => {
      const py = y.getPixelForValue(line.v);
      if (!Number.isFinite(py) || py < top || py > bottom) return;
      ctx.lineWidth = line.bold ? 1.5 : 1;
      ctx.strokeStyle = line.color;
      ctx.beginPath();
      ctx.moveTo(left, py);
      ctx.lineTo(right, py);
      ctx.stroke();
    });
    // Second pass: place labels with collision-avoidance. Walk top-to-bottom
    // (highest threshold first) and snap each label so it's >= 14px below the
    // previous one. Each label sits at the right edge with a 4px chip padding.
    ctx.setLineDash([]);
    const placed = lines
      .map(l => ({ ...l, py: y.getPixelForValue(l.v) }))
      .filter(l => Number.isFinite(l.py) && l.py >= top && l.py <= bottom)
      .sort((a, b) => a.py - b.py);  // top-to-bottom
    let lastBottom = -Infinity;
    placed.forEach(line => {
      const wantPy = Math.max(line.py, lastBottom + 14);
      const labelPy = Math.min(wantPy, bottom - 6);
      const padX = 4, padY = 2;
      const textW = ctx.measureText(line.label).width;
      const chipW = textW + 2 * padX;
      const chipH = 14;
      const chipX = right - chipW - 2;
      const chipY = labelPy - chipH / 2;
      // Backdrop chip
      ctx.fillStyle = 'rgba(13,17,23,0.85)';
      ctx.strokeStyle = line.color;
      ctx.lineWidth = 1;
      ctx.beginPath();
      const r = 3;
      ctx.moveTo(chipX + r, chipY);
      ctx.lineTo(chipX + chipW - r, chipY);
      ctx.quadraticCurveTo(chipX + chipW, chipY, chipX + chipW, chipY + r);
      ctx.lineTo(chipX + chipW, chipY + chipH - r);
      ctx.quadraticCurveTo(chipX + chipW, chipY + chipH, chipX + chipW - r, chipY + chipH);
      ctx.lineTo(chipX + r, chipY + chipH);
      ctx.quadraticCurveTo(chipX, chipY + chipH, chipX, chipY + chipH - r);
      ctx.lineTo(chipX, chipY + r);
      ctx.quadraticCurveTo(chipX, chipY, chipX + r, chipY);
      ctx.closePath();
      ctx.fill();
      ctx.stroke();
      ctx.fillStyle = line.color;
      ctx.fillText(line.label, chipX + chipW - padX, chipY + chipH / 2 + padY / 2);
      lastBottom = chipY + chipH;
    });
    ctx.restore();
  }
};

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
  // Add headroom so the warn/crit/cap chips never sit on the very top edge
  // (they're 14px tall and need ~10px of breathing room above the top tick).
  const yMax = Math.max(sideData.peakIps * 1.18, cap * 1.12);

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
        zoom: csZoomPluginConfig(canvas, { clampX: true, clampY: true })
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
    plugins: [ipcThresholdsPlugin, ipcCrosshairPlugin]
  });
  canvas._chartInstance = chart;
  return chart;
}

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
      tbody.innerHTML = rows.map(r => {
        // A row is on the b23 fallback path when ALL segments for this cluster
        // share the same `workers` value — the b22 step function would have
        // produced varying worker counts.
        const ownSegs = sd.segments.filter(s => s.cluster === r.cluster);
        const isB23 = ownSegs.length > 0 &&
          ownSegs.every(s => s.workers === ownSegs[0].workers);
        const isSynth = ownSegs.some(s => s.synthetic);
        const b23Tag = isB23 ? `<span class="ipc-tag-fallback" title="No autoscaler events; using recommended worker count">b23</span>` : '';
        const synthTag = isSynth ? `<span class="ipc-tag-synthetic" title="Span boundaries inferred from b21 events; no b20 row was found">b22 · synth</span>` : '';
        const navUrl = buildUrl(Object.assign({}, parseRoute(), { cluster: r.cluster, recipe: null }));
        return `
        <tr data-cluster="${escapeAttr(r.cluster)}" tabindex="0">
          <td><a class="ipc-row-link" href="${escapeAttr(navUrl)}" data-nav-cluster="${escapeAttr(r.cluster)}" title="Open cluster card">${escapeHtml(r.cluster)} ↗</a>${b23Tag}${synthTag}</td>
          <td>${r.workers}</td>
          <td>${r.master}</td>
          <td>${r.ips}</td>
          <td><code>${escapeHtml(r.machineType)}</code></td>
          <td>€${r.segCostEur.toFixed(2)}</td>
          <td>${r.idx}</td>
        </tr>`;
      }).join('');
      // Click on the link cell → navigate; click anywhere else on the row → highlight.
      tbody.querySelectorAll('a.ipc-row-link').forEach(a => {
        a.addEventListener('click', (e) => {
          e.preventDefault();
          e.stopPropagation();
          const cluster = a.dataset.navCluster;
          if (cluster) navigate({ cluster, recipe: null });
        });
      });
      tbody.querySelectorAll('tr[data-cluster]').forEach(tr => {
        const toggleHighlight = () => {
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
        };
        tr.addEventListener('click', toggleHighlight);
        tr.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            navigate({ cluster: tr.dataset.cluster, recipe: null });
          } else if (e.key === ' ') {
            e.preventDefault();
            toggleHighlight();
          }
        });
      });
    }
  }
  const tfTotal = sideEl.querySelector('.ipc-tfoot-total');
  const tfCost  = sideEl.querySelector('.ipc-tfoot-cost');
  if (tfTotal) tfTotal.textContent = String(fleetTotal);
  if (tfCost)  tfCost.textContent  = `€${fleetCost.toFixed(2)}`;

  // Re-draw chart so crosshair plugin picks up new state.
  if (sideEl._ipcChart) sideEl._ipcChart.update('none');
}

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
    // Strip any existing ipc= param. Two passes so a `&ipc=foo` in the middle
    // doesn't smash its neighbors together (e.g. `#x=1&ipc=foo&y=2` → `#x=1&y=2`,
    // not `#x=1y=2`). Leading `#ipc=foo&?` collapses to `#`.
    let h = (window.location.hash || '')
      .replace(/^#ipc=(reference|current|both)(?:&|$)/, '#')
      .replace(/&ipc=(reference|current|both)\b/, '');
    if (h && !h.startsWith('#')) h = '#' + h;
    if (!h || h === '#') h = `#ipc=${mode}`;
    else h = `${h}${h.endsWith('&') ? '' : '&'}ipc=${mode}`;
    window.history.replaceState(null, '', h);
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

function orderConfKeys(keys) {
  const preferred = [
    'num_workers', 'worker_machine_type', 'master_machine_type',
    'autoscaling_policy', 'tuner_version', 'total_no_of_jobs',
    'cluster_max_total_memory_gb', 'cluster_max_total_cores',
    'accumulated_max_total_memory_per_jobs_gb',
    'driver_memory_gb', 'driver_cores', 'driver_memory_overhead_gb',
    'diagnostic_reason'
  ];
  const idx = (k) => {
    const i = preferred.indexOf(k);
    return i === -1 ? 999 : i;
  };
  return keys.slice().sort((a, b) => idx(a) - idx(b) || a.localeCompare(b));
}

// ── Charts ──────────────────────────────────────────────────────────────────

function renderDetailCharts(cluster, clusterName) {
  const chartsDiv = document.getElementById('detail-charts');
  chartsDiv.innerHTML = '';

  // Include all recipes — new entries (no deltas) are rendered as a single
  // "New (current only)" bar; kept recipes (present only on the reference date,
  // carried over by the auto-tuner) are rendered as a single greyish reference
  // bar so users can see them alongside existing recipes.
  const recipes = cluster.recipes.filter(r =>
    r.deltas.length > 0 || r.current_metrics || r.reference_metrics);

  // Use horizontal bars when there are many recipes — labels stay readable.
  const horizontal = recipes.length > 8;
  const barThickness = 12;
  const datasetsPerCategory = 3; // reference + current + new
  const intraGroupGap = 2;
  const interGroupGap = 10;
  const categoryHeight = barThickness * datasetsPerCategory + intraGroupGap + interGroupGap;
  const minHeight = 220;
  const computedHeight = horizontal ? Math.max(minHeight, recipes.length * categoryHeight + 80) : minHeight;

  // Duration chart
  const durContainer = document.createElement('div');
  durContainer.className = 'chart-container';
  durContainer.innerHTML = `<h4>P95 Job Duration by Recipe <span class="info-icon" data-doc-key="p95">ⓘ</span></h4>
    <div class="chart-scroll"><canvas id="dur-chart"></canvas></div>`;
  chartsDiv.appendChild(durContainer);
  const durCanvas = durContainer.querySelector('#dur-chart');
  durCanvas.parentElement.style.height = computedHeight + 'px';

  // Executors chart
  const execContainer = document.createElement('div');
  execContainer.className = 'chart-container';
  execContainer.innerHTML = `<h4>P95 Max Executors by Recipe <span class="info-icon" data-doc-key="p95">ⓘ</span></h4>
    <div class="chart-scroll"><canvas id="exec-chart"></canvas></div>`;
  chartsDiv.appendChild(execContainer);
  const execCanvas = execContainer.querySelector('#exec-chart');
  execCanvas.parentElement.style.height = computedHeight + 'px';

  const isNew  = recipes.map(isNewEntryRecipe);
  const isKept = recipes.map(isKeptEntryRecipe);
  const fullLabels = recipes.map((r, i) => {
    if (isNew[i])  return `${recipeShortName(r.recipe)} 🆕`;
    if (isKept[i]) return `${recipeShortName(r.recipe)} [KEPT]`;
    return recipeShortName(r.recipe);
  });
  const tooltipFullNames = recipes.map(r => r.recipe);

  const durRefRaw = recipes.map(r => recipeMetricValue(r, 'p95_job_duration_ms', 'reference'));
  const durCurRaw = recipes.map(r => recipeMetricValue(r, 'p95_job_duration_ms', 'current'));
  const execRefRaw = recipes.map(r => recipeMetricValue(r, 'p95_run_max_executors', 'reference'));
  const execCurRaw = recipes.map(r => recipeMetricValue(r, 'p95_run_max_executors', 'current'));

  // Mask values so reference/current bars are blank for new entries (and the
  // gold "New (current only)" bar is blank for existing entries). Kept recipes
  // also get a current/new mask of NaN — only their grey reference bar is shown.
  const durRef = durRefRaw.map((v, i) => isNew[i] ? NaN : v);
  const durCur = durCurRaw.map((v, i) => (isNew[i] || isKept[i]) ? NaN : v);
  const durNew = durCurRaw.map((v, i) => isNew[i] ? v : NaN);
  const execRef = execRefRaw.map((v, i) => isNew[i] ? NaN : v);
  const execCur = execCurRaw.map((v, i) => (isNew[i] || isKept[i]) ? NaN : v);
  const execNew = execCurRaw.map((v, i) => isNew[i] ? v : NaN);

  // The Reference dataset is recoloured greyish for kept recipes so they're
  // visually distinct from paired (still-current) recipes.
  const KEPT_BG = 'rgba(139, 148, 158, 0.5)';
  const KEPT_BORDER = 'rgba(139, 148, 158, 1)';
  const refBgScriptable    = ctx => isKept[ctx.dataIndex] ? KEPT_BG : 'rgba(88, 166, 255, 0.5)';
  const refBorderScriptable = ctx => isKept[ctx.dataIndex] ? KEPT_BORDER : 'rgba(88, 166, 255, 1)';

  const baseDataset = (label, values, color, perBarColor) => ({
    label,
    data: values,
    backgroundColor: perBarColor ? perBarColor.bg : color.replace('1)', '0.5)'),
    borderColor: perBarColor ? perBarColor.border : color.replace('0.5)', '1)'),
    borderWidth: 1,
    barThickness,
    maxBarThickness: barThickness + 4,
  });

  const onClickBar = (recipeIdx) => {
    const recipe = recipes[recipeIdx];
    if (recipe) navigate({ recipe: recipe.recipe });
  };

  new Chart(durCanvas, {
    type: 'bar',
    data: {
      labels: fullLabels,
      datasets: [
        baseDataset('Reference', durRef, 'rgba(88,166,255,1)',
          { bg: refBgScriptable, border: refBorderScriptable }),
        baseDataset('Current', durCur, 'rgba(248,81,73,1)'),
        baseDataset('New (current only)', durNew, 'rgba(210,153,34,1)')
      ]
    },
    options: chartOpts({
      horizontal,
      tooltipNames: tooltipFullNames,
      valueFormatter: (v) => formatDuration(v),
      onBarClick: onClickBar,
    })
  });

  new Chart(execCanvas, {
    type: 'bar',
    data: {
      labels: fullLabels,
      datasets: [
        baseDataset('Reference', execRef, 'rgba(88,166,255,1)',
          { bg: refBgScriptable, border: refBorderScriptable }),
        baseDataset('Current', execCur, 'rgba(248,81,73,1)'),
        baseDataset('New (current only)', execNew, 'rgba(210,153,34,1)')
      ]
    },
    options: chartOpts({
      horizontal,
      tooltipNames: tooltipFullNames,
      valueFormatter: (v) => formatNum(v),
      onBarClick: onClickBar,
    })
  });
}

function isNewEntryRecipe(r) {
  return (!r.deltas || r.deltas.length === 0) && !!r.current_metrics;
}

// "Kept" recipes are present only on the reference date — their config was
// carried forward by the auto-tuner. The analysis JSON ships their reference
// metrics under `reference_metrics` so the chart can still draw a bar.
function isKeptEntryRecipe(r) {
  return (!r.deltas || r.deltas.length === 0) && !!r.reference_metrics;
}

function recipeMetricValue(recipe, metric, field) {
  if (recipe.deltas && recipe.deltas.length > 0) {
    const d = recipe.deltas.find(x => x.metric === metric);
    return d ? d[field] : 0;
  }
  if (field === 'current' && recipe.current_metrics) {
    const v = recipe.current_metrics[metric];
    return v == null ? 0 : v;
  }
  if (field === 'reference' && recipe.reference_metrics) {
    const v = recipe.reference_metrics[metric];
    return v == null ? 0 : v;
  }
  return 0;
}

function deltaValue(recipe, metric, field) {
  // Backwards-compatible alias for callers outside the bar-chart path.
  return recipeMetricValue(recipe, metric, field);
}

function recipeShortName(recipe) {
  return recipe.replace(/^_/, '').replace(/\.json$/, '');
}

function chartOpts({ horizontal, tooltipNames, valueFormatter, onBarClick }) {
  const opts = {
    responsive: true,
    maintainAspectRatio: false,
    indexAxis: horizontal ? 'y' : 'x',
    onClick: (evt, elements) => {
      if (elements && elements[0] && onBarClick) onBarClick(elements[0].index);
    },
    onHover: (evt, elements) => {
      evt.native.target.style.cursor = elements && elements.length ? 'pointer' : 'default';
    },
    plugins: {
      legend: { labels: { color: '#c9d1d9', font: { size: 11 } } },
      tooltip: {
        backgroundColor: '#1c2128',
        borderColor: '#444c56',
        borderWidth: 1,
        titleColor: '#f0f6fc',
        bodyColor: '#c9d1d9',
        callbacks: {
          title: (items) => items[0] ? tooltipNames[items[0].dataIndex] : '',
          label: (item) => `${item.dataset.label}: ${valueFormatter(item.parsed[horizontal ? 'x' : 'y'])}`,
          afterBody: () => '\nClick to view recipe spark conf →',
        }
      }
    },
    scales: {}
  };
  const valueAxis = {
    ticks: { color: '#8b949e', callback: (v) => valueFormatter(v) },
    grid: { color: '#21262d' }
  };
  const labelAxis = {
    ticks: {
      color: '#c9d1d9',
      font: { size: 10 },
      autoSkip: false,
      maxRotation: horizontal ? 0 : 45,
      callback: function (val) {
        const lbl = this.getLabelForValue(val);
        if (horizontal) return lbl.length > 38 ? lbl.slice(0, 35) + '…' : lbl;
        return lbl.length > 14 ? lbl.slice(0, 12) + '…' : lbl;
      }
    },
    grid: { color: '#21262d' }
  };
  if (horizontal) {
    opts.scales.x = valueAxis;
    opts.scales.y = labelAxis;
  } else {
    opts.scales.x = labelAxis;
    opts.scales.y = valueAxis;
  }
  return opts;
}

// ── Recipe Spark Conf Modal ─────────────────────────────────────────────────

async function showRecipeConfModal(clusterName, recipeName) {
  navigate({ cluster: clusterName, recipe: recipeName });
}

async function showRecipeConfModalRaw(clusterName, recipeName) {
  openModal(`<span class="recipe-name-text">${escapeHtml(recipeName)}</span> ${copyIcon(recipeName)}`,
            `<div class="empty-msg">Loading recipe configuration…</div>`);

  const { ref, cur, refDate, curDate } = await loadClusterJsonsForDates(clusterName);
  const refRecipe = extractRecipeConf(ref, recipeName);
  const curRecipe = extractRecipeConf(cur, recipeName);

  if (!refRecipe && !curRecipe) {
    document.getElementById('modal-body').innerHTML =
      `<div class="empty-msg">No recipe spark conf found in either ${escapeHtml(formatDate(refDate))} or ${escapeHtml(formatDate(curDate))} cluster JSON.</div>` +
      triedPathsHtml(clusterName);
    return;
  }

  const refOpts = (refRecipe && refRecipe.sparkOptsMap) || {};
  const curOpts = (curRecipe && curRecipe.sparkOptsMap) || {};
  const allOptKeys = orderSparkOptKeys(new Set([...Object.keys(refOpts), ...Object.keys(curOpts)]));

  const optsRows = allOptKeys.map(k => {
    const rv = refOpts[k];
    const cv = curOpts[k];
    const changed = rv !== undefined && cv !== undefined && rv !== cv;
    return `<tr>
      <td class="k">${escapeHtml(k)}</td>
      <td>${rv === undefined ? '<span class="empty-msg">—</span>' : escapeHtml(String(rv))}</td>
      <td class="${changed ? 'changed' : ''}">${cv === undefined ? '<span class="empty-msg">—</span>' : escapeHtml(String(cv))}</td>
    </tr>`;
  }).join('');

  const topRows = ['parallelizationFactor', 'total_executor_minimum_allocated_memory_gb', 'total_executor_maximum_allocated_memory_gb']
    .map(k => {
      const rv = refRecipe ? refRecipe[k] : undefined;
      const cv = curRecipe ? curRecipe[k] : undefined;
      if (rv === undefined && cv === undefined) return '';
      const changed = rv !== undefined && cv !== undefined && rv !== cv;
      return `<tr>
        <td class="k">${escapeHtml(k)}</td>
        <td>${rv === undefined ? '<span class="empty-msg">—</span>' : escapeHtml(String(rv))}</td>
        <td class="${changed ? 'changed' : ''}">${cv === undefined ? '<span class="empty-msg">—</span>' : escapeHtml(String(cv))}</td>
      </tr>`;
    }).join('');

  const refJsonStr = refRecipe ? JSON.stringify(refRecipe, null, 2) : '';
  const curJsonStr = curRecipe ? JSON.stringify(curRecipe, null, 2) : '';

  document.getElementById('modal-body').innerHTML = `
    <h5 style="font-size:11px;color:#8b949e;text-transform:uppercase;letter-spacing:0.4px;margin-bottom:6px">Recipe parameters</h5>
    <table class="conf-kv-table">
      <thead><tr><td class="k">Field</td><td>Reference (${escapeHtml(formatDate(refDate))})</td><td>Current (${escapeHtml(formatDate(curDate))})</td></tr></thead>
      <tbody>${topRows}</tbody>
    </table>

    <h5 style="font-size:11px;color:#8b949e;text-transform:uppercase;letter-spacing:0.4px;margin:14px 0 6px">spark.* options</h5>
    <table class="conf-kv-table">
      <tbody>${optsRows}</tbody>
    </table>

    <h5 style="font-size:11px;color:#8b949e;text-transform:uppercase;letter-spacing:0.4px;margin:14px 0 6px">Raw JSON</h5>
    <div class="conf-compare">
      <div class="conf-compare-col">
        <div class="col-header">
          <span style="color:#58a6ff;font-weight:600">Reference (${escapeHtml(formatDate(refDate))})</span>
          ${refJsonStr ? copyIcon(refJsonStr) : ''}
        </div>
        <pre>${refJsonStr ? escapeHtml(refJsonStr) : '<span class="empty-msg">— not present —</span>'}</pre>
      </div>
      <div class="conf-compare-col">
        <div class="col-header">
          <span style="color:#f0883e;font-weight:600">Current (${escapeHtml(formatDate(curDate))})</span>
          ${curJsonStr ? copyIcon(curJsonStr) : ''}
        </div>
        <pre>${curJsonStr ? escapeHtml(curJsonStr) : '<span class="empty-msg">— not present —</span>'}</pre>
      </div>
    </div>
  `;
}

function orderSparkOptKeys(set) {
  const preferred = [
    'spark.dynamicAllocation.enabled',
    'spark.dynamicAllocation.minExecutors',
    'spark.dynamicAllocation.maxExecutors',
    'spark.dynamicAllocation.initialExecutors',
    'spark.executor.instances',
    'spark.executor.cores',
    'spark.executor.memory',
    'spark.serializer',
    'spark.closure.serializer',
  ];
  const idx = (k) => {
    const i = preferred.indexOf(k);
    return i === -1 ? 999 : i;
  };
  return Array.from(set).sort((a, b) => idx(a) - idx(b) || a.localeCompare(b));
}

function openModal(titleHtml, bodyHtml) {
  document.getElementById('modal-title').innerHTML = titleHtml;
  document.getElementById('modal-body').innerHTML = bodyHtml;
  document.getElementById('modal-overlay').style.display = 'flex';
}

function closeModal() {
  // Public API used by older code paths — route-aware close.
  const cur = parseRoute();
  if (cur.recipe) navigate({ recipe: null });
  else if (cur.summary) navigate({ summary: null });
  else closeModalSilently();
}

function closeModalSilently() {
  document.getElementById('modal-overlay').style.display = 'none';
  closeSummaryModalClass();
}

// ── Doc popover ─────────────────────────────────────────────────────────────

function showDocPopover(event, doc) {
  // `bodyHtml` (when present) is treated as trusted rich text — used for entries
  // that need <br>, <strong>, <em>, <code>. All entries are author-controlled
  // (METRIC_DOCS literal), not user-derived, so this is safe.
  const bodyContent = doc.bodyHtml ? doc.bodyHtml : escapeHtml(doc.body);
  const parts = [
    `<div class="doc-title">${escapeHtml(doc.title)}</div>`,
    `<div class="doc-body">${bodyContent}</div>`
  ];
  if (doc.formula) parts.push(`<div class="doc-formula">${escapeHtml(doc.formula)}</div>`);
  if (doc.svg) parts.push(`<div class="doc-svg-wrap">${doc.svg}</div>`);
  if (typeof doc.example === 'function' && data) {
    try {
      const ex = doc.example(data);
      if (ex) parts.push(`<div class="doc-example">${ex}</div>`);
    } catch (e) { /* example failed → skip silently, popover still useful */ }
  }
  docPopover.innerHTML = parts.join('');
  docPopover.style.display = 'block';
  const isRich = !!(doc.formula || doc.svg || doc.example);
  const w = isRich ? 420 : 380;
  const h = isRich ? 320 : 200;
  const x = event.clientX + 12;
  const y = event.clientY + 12;
  docPopover.style.left = Math.min(x, window.innerWidth - w) + 'px';
  docPopover.style.top = Math.min(y, window.innerHeight - h) + 'px';
}

function hideDocPopover() {
  docPopover.style.display = 'none';
}

// ── Correlations: cards with mini scatter ───────────────────────────────────

// Active view + cluster scope are read from the DOM controls so listeners can
// just trigger a re-render — no global state object required.
function getCorrView() {
  const btn = document.querySelector('#corr-view-toggle .seg.active');
  return btn ? btn.dataset.view : 'delta';
}
function getCorrCluster() {
  const sel = document.getElementById('corr-cluster-filter');
  return sel ? sel.value : '';
}

// Picks the right correlation array for the (view, clusterFilter) combination.
// Returns { results, scatterPool, fallbackResults, n } where fallbackResults
// is the fleet-wide values shown when a per-cluster group has too few entries.
function pickCorrelationSet(view, clusterFilter) {
  if (clusterFilter && view === 'delta') {
    const perCluster = (data.correlations_per_cluster || {})[clusterFilter];
    if (perCluster && perCluster.length > 0) {
      return { results: perCluster, scatterScope: 'cluster', fallback: null };
    }
    // Fall back to fleet values, marked as fallback so we can show a hint.
    return { results: data.correlations || [], scatterScope: 'fleet', fallback: 'cluster_too_small' };
  }
  if (view === 'current_snapshot') {
    return { results: data.correlations_current_snapshot || [], scatterScope: 'fleet', fallback: null };
  }
  return { results: data.correlations || [], scatterScope: 'fleet', fallback: null };
}

function renderCorrelationCards(view, clusterFilter) {
  view = view || getCorrView();
  clusterFilter = clusterFilter == null ? getCorrCluster() : clusterFilter;
  const host = document.getElementById('correlation-cards');
  if (!host) return;

  const { results, fallback } = pickCorrelationSet(view, clusterFilter);
  console.info(`[Correlations] view=${view}${clusterFilter ? ` cluster=${clusterFilter}` : ''}: ${(results || []).length} metric pairs${fallback ? ` fallback=${fallback}` : ''}`);
  if (!results || results.length === 0) {
    host.innerHTML = '<p style="color:#8b949e">No correlation data available.</p>';
    return;
  }

  const fallbackBanner = fallback === 'cluster_too_small'
    ? `<div class="fallback-banner">This cluster has fewer than ${'5'} paired recipes — showing fleet-wide values as fallback.</div>`
    : '';

  const cardsHtml = results.map(c => {
    const r = c.pearson;
    const shortA = labelMetric(c.metric_a);
    const shortB = labelMetric(c.metric_b);
    const color = corrColor(r);
    const interp = interpretCorrelation(r, shortA, shortB);
    const points = scatterPointsFor(view, c.metric_a, c.metric_b, clusterFilter);
    return `<div class="corr-card">
      <div class="corr-card-pair">
        <span class="corr-card-metric">${shortA}</span>
        <span class="corr-card-arrow">↔</span>
        <span class="corr-card-metric">${shortB}</span>
        <span class="info-icon" data-doc-key="pearson_full">ⓘ</span>
      </div>
      <div class="corr-card-body">
        <div class="corr-card-value" style="background:${color}">${r.toFixed(3)}</div>
        <div class="corr-card-meta">
          <div>n=${c.n}</div>
          <div class="corr-card-view">${view === 'delta' ? 'deltas' : 'current snapshot'}</div>
        </div>
        ${renderMiniScatter(points, shortA, shortB)}
      </div>
      <div class="corr-card-interp">${interp}</div>
    </div>`;
  }).join('');

  host.innerHTML = fallbackBanner + `<div class="corr-card-grid">${cardsHtml}</div>`;
}

// Look up the scatter points for one (metricA, metricB) pair, optionally
// filtered to a single cluster.
function scatterPointsFor(view, metricA, metricB, clusterFilter) {
  const pool = (data.scatter_data || {})[view] || {};
  const key = `${metricA}__${metricB}`;
  const arr = pool[key] || [];
  if (!clusterFilter) return arr;
  return arr.filter(p => p.cluster === clusterFilter);
}

// Inline-SVG mini scatter, ~160x110 px, with new-job points overlaid in gold.
// Optional xAxisLabel / yAxisLabel add small labels on the axes.
function renderMiniScatter(points, xAxisLabel, yAxisLabel) {
  const hasLabels = !!(xAxisLabel || yAxisLabel);
  const W = hasLabels ? 168 : 120;
  const H = hasLabels ? 114 : 80;
  const PAD_L = hasLabels && yAxisLabel ? 22 : 6;
  const PAD_B = hasLabels && xAxisLabel ? 18 : 6;
  const PAD_T = 6, PAD_R = 6;
  if (!points || points.length === 0) {
    return `<div class="scatter-mini scatter-mini-empty" style="width:${W}px;height:${H}px">no points</div>`;
  }
  const xs = points.map(p => p.x);
  const ys = points.map(p => p.y);
  const xMin = Math.min(...xs), xMax = Math.max(...xs);
  const yMin = Math.min(...ys), yMax = Math.max(...ys);
  const xSpan = xMax - xMin || 1;
  const ySpan = yMax - yMin || 1;
  const project = (p) => {
    const px = PAD_L + ((p.x - xMin) / xSpan) * (W - PAD_L - PAD_R);
    const py = PAD_T + (1 - (p.y - yMin) / ySpan) * (H - PAD_T - PAD_B);
    return [px, py];
  };
  const dots = points.map(p => {
    const [px, py] = project(p);
    const cls = p.is_new ? 'sc-new' : 'sc-pt';
    const title = `${p.cluster} · ${p.recipe}\nx=${formatNum(p.x)}, y=${formatNum(p.y)}`;
    return `<circle cx="${px.toFixed(1)}" cy="${py.toFixed(1)}" r="${p.is_new ? 2.5 : 1.8}" class="${cls}"><title>${escapeHtml(title)}</title></circle>`;
  });
  let zeroXLine = '';
  let zeroYLine = '';
  if (xMin <= 0 && xMax >= 0) {
    const zx = PAD_L + ((0 - xMin) / xSpan) * (W - PAD_L - PAD_R);
    zeroXLine = `<line x1="${zx}" y1="${PAD_T}" x2="${zx}" y2="${H - PAD_B}" class="sc-zero"/>`;
  }
  if (yMin <= 0 && yMax >= 0) {
    const zy = PAD_T + (1 - (0 - yMin) / ySpan) * (H - PAD_T - PAD_B);
    zeroYLine = `<line x1="${PAD_L}" y1="${zy}" x2="${W - PAD_R}" y2="${zy}" class="sc-zero"/>`;
  }
  let axisLabels = '';
  if (xAxisLabel) {
    axisLabels += `<text x="${(PAD_L + W - PAD_R) / 2}" y="${H - 2}" text-anchor="middle" class="sc-axis-label">${escapeHtml(xAxisLabel)}</text>`;
  }
  if (yAxisLabel) {
    const cx = PAD_L / 2;
    const cy = (PAD_T + H - PAD_B) / 2;
    axisLabels += `<text x="${cx}" y="${cy}" text-anchor="middle" class="sc-axis-label" transform="rotate(-90 ${cx} ${cy})">${escapeHtml(yAxisLabel)}</text>`;
  }
  return `<svg class="scatter-mini" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="scatter plot">
    <rect x="0.5" y="0.5" width="${W - 1}" height="${H - 1}" class="sc-frame"/>
    ${zeroXLine}${zeroYLine}
    ${dots.join('')}
    ${axisLabels}
  </svg>`;
}

function interpretCorrelation(r, shortA, shortB) {
  const abs = Math.abs(r);
  let strength = 'weak';
  if (abs >= 0.7) strength = 'strong';
  else if (abs >= 0.3) strength = 'moderate';
  if (abs < 0.1) {
    return `<strong>No clear relationship</strong> between <em>${shortA}</em> and <em>${shortB}</em> (|r|=${abs.toFixed(2)}). Changes in one don't predict changes in the other.`;
  }
  if (r > 0) {
    return `<strong>${capitalize(strength)} positive correlation</strong> — when <em>${shortA}</em> rises, <em>${shortB}</em> tends to rise too. (|r|=${abs.toFixed(2)})`;
  }
  return `<strong>${capitalize(strength)} negative correlation</strong> — when <em>${shortA}</em> rises, <em>${shortB}</em> tends to fall. (|r|=${abs.toFixed(2)})`;
}

function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

function corrColor(r) {
  if (r > 0.5) return `rgba(248, 81, 73, ${0.3 + Math.abs(r) * 0.5})`;
  if (r < -0.5) return `rgba(63, 185, 80, ${0.3 + Math.abs(r) * 0.5})`;
  if (r > 0.2) return `rgba(248, 81, 73, 0.2)`;
  if (r < -0.2) return `rgba(63, 185, 80, 0.2)`;
  return 'rgba(139, 148, 158, 0.1)';
}

// ── Divergences: z-score strip plot + table ─────────────────────────────────

function getDivView() {
  const btn = document.querySelector('#div-view-toggle .seg.active');
  return btn ? btn.dataset.view : 'delta';
}
function getDivCluster() {
  const sel = document.getElementById('div-cluster-filter');
  return sel ? sel.value : '';
}

// Returns the divergence array for the (view, clusterFilter) combination.
// Per-cluster scope uses the cluster's own internal stats (not the fleet's).
function pickDivergenceSet(view, clusterFilter) {
  if (clusterFilter && view === 'delta') {
    const perCluster = (data.divergences_per_cluster || {})[clusterFilter];
    if (perCluster && perCluster.length >= 0) {
      // Even an empty list means "this cluster was big enough — no outliers".
      if ((data.divergences_per_cluster || {})[clusterFilter] !== undefined) {
        return { results: perCluster, fallback: null };
      }
    }
    return { results: (data.divergences || []).filter(d => d.cluster === clusterFilter), fallback: 'cluster_too_small' };
  }
  if (view === 'current_snapshot') {
    const all = data.divergences_current_snapshot || [];
    return { results: clusterFilter ? all.filter(d => d.cluster === clusterFilter) : all, fallback: null };
  }
  const all = data.divergences || [];
  return { results: clusterFilter ? all.filter(d => d.cluster === clusterFilter) : all, fallback: null };
}

function renderDivergenceTable(view, clusterFilter) {
  view = view || getDivView();
  clusterFilter = clusterFilter == null ? getDivCluster() : clusterFilter;
  const zMin = parseFloat(document.getElementById('z-min').value) || 0;
  const { results, fallback } = pickDivergenceSet(view, clusterFilter);
  const divs = results
    .filter(d => Math.abs(d.z_score) >= zMin)
    .sort((a, b) => Math.abs(b.z_score) - Math.abs(a.z_score));
  const high = divs.filter(d => Math.abs(d.z_score) >= 3).length;
  console.info(`[Divergences] view=${view}${clusterFilter ? ` cluster=${clusterFilter}` : ''} zMin=${zMin}: ${divs.length} rows, ${high} high-z (|z|>=3)${fallback ? ` fallback=${fallback}` : ''}`);

  const tbody = document.querySelector('#divergence-table tbody');
  if (fallback === 'cluster_too_small') {
    tbody.innerHTML = `<tr><td colspan="6" class="fallback-row">This cluster has fewer than 5 paired recipes — falling back to fleet z-scores filtered to this cluster.</td></tr>` +
      divs.map(divergenceRowHtml).join('');
  } else {
    tbody.innerHTML = divs.map(divergenceRowHtml).join('');
  }
  if (divs.length === 0 && fallback !== 'cluster_too_small') {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:#8b949e;padding:24px">No divergences above threshold</td></tr>';
  }

  renderZScoreStripPlot(view, clusterFilter);

  // Wire clickable rows → navigate to recipe tuning details
  document.querySelectorAll('#divergence-table tbody tr.div-row-clickable').forEach(row => {
    row.addEventListener('click', (e) => {
      if (e.target.closest('.copy-btn')) return; // don't interfere with copy buttons
      navigate({ cluster: row.dataset.cluster, recipe: row.dataset.recipe });
    });
    row.addEventListener('auxclick', (e) => {
      if (e.button !== 1) return;
      e.preventDefault();
      const url = buildUrl(Object.assign({}, parseRoute(), { cluster: row.dataset.cluster, recipe: row.dataset.recipe }));
      window.open(url, '_blank');
    });
  });
}

function divergenceRowHtml(d) {
  const zCls = Math.abs(d.z_score) >= 3 ? 'z-high' : 'z-med';
  const newPill = d.is_new_entry ? '<span class="new-pill">NEW</span>' : '';
  const refCell = (d.is_new_entry || d.reference == null) ? '—' : formatMetricValue(d.metric, d.reference);
  return `<tr class="div-row-clickable" data-cluster="${escapeAttr(d.cluster)}" data-recipe="${escapeAttr(d.recipe)}">
    <td>${escapeHtml(d.cluster)} ${copyIcon(d.cluster)}</td>
    <td>${escapeHtml(d.recipe)} ${newPill} ${copyIcon(d.recipe)}</td>
    <td>${labelMetric(d.metric)}</td>
    <td>${refCell}</td>
    <td>${formatMetricValue(d.metric, d.current)}</td>
    <td class="${zCls}">${d.z_score.toFixed(2)}</td>
  </tr>`;
}

// One row per metric. Each row is a horizontal axis with the ±zThreshold band
// shaded; circles plotted at each (cluster, recipe)'s z-score. New entries gold.
// Enhanced: data-level zoom (Ctrl/⌘+wheel), drag-to-pan, hover tooltip, clickable dots & rows.
let _stripZoomLevel = 0;   // 0 = default; positive = zoomed in
let _stripZoomMax = 10;
let _stripBaseAbsMax = null;
let _stripPanOffset = 0;   // shift the visible centre (in z-score units)
let _stripAbortCtrl = null; // AbortController for document-level drag listeners

/** Build the SVG rows HTML for the strip plot (no side-effects). */
function _buildStripSVGs(results, zMin) {
  const byMetric = {};
  results.forEach(d => {
    (byMetric[d.metric] = byMetric[d.metric] || []).push(d);
  });

  const allZ = results.map(d => d.z_score);
  const dataAbsMax = Math.max(zMin + 0.5, ...allZ.map(Math.abs));
  if (_stripBaseAbsMax === null) _stripBaseAbsMax = dataAbsMax;

  const zoomFactor = Math.pow(0.8, _stripZoomLevel);
  const visAbsMax = _stripBaseAbsMax * zoomFactor;
  const xMin = -visAbsMax + _stripPanOffset, xMax = visAbsMax + _stripPanOffset;

  const W = 1100, H = 34, PAD = 210;
  const innerW = W - PAD - 20;
  const project = (z) => PAD + ((z - xMin) / (xMax - xMin)) * innerW;

  return Object.entries(byMetric).sort((a, b) => a[0].localeCompare(b[0])).map(([metric, ds]) => {
    const yMid = H / 2;
    const bandLeft = project(-zMin);
    const bandRight = project(zMin);
    const dots = ds.map(d => {
      const cx = project(d.z_score);
      const cls = d.is_new_entry ? 'zs-new' : (Math.abs(d.z_score) >= 3 ? 'zs-high' : 'zs-med');
      return `<circle cx="${cx.toFixed(1)}" cy="${yMid}" r="4" class="${cls} zs-dot-circle"
        data-cluster="${escapeAttr(d.cluster)}" data-recipe="${escapeAttr(d.recipe)}"
        data-metric="${escapeAttr(d.metric)}" data-z="${d.z_score.toFixed(2)}"
        data-ref="${d.reference != null ? d.reference : ''}" data-cur="${d.current != null ? d.current : ''}"
        data-new="${d.is_new_entry ? '1' : '0'}"/>`;
    }).join('');
    return `<div class="z-strip-row">
      <svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="${escapeAttr('z-score strip for ' + metric)}">
        <text x="${PAD - 6}" y="${yMid + 4}" text-anchor="end" class="zs-label">${escapeHtml(labelMetric(metric))}</text>
        <line x1="${PAD}" y1="${yMid}" x2="${W - 20}" y2="${yMid}" class="zs-axis"/>
        <rect x="${PAD}" y="${yMid - 8}" width="${Math.max(0, bandLeft - PAD)}" height="16" class="zs-band-outlier"/>
        <rect x="${bandRight}" y="${yMid - 8}" width="${Math.max(0, (W - 20) - bandRight)}" height="16" class="zs-band-outlier"/>
        <line x1="${project(0)}" y1="${yMid - 8}" x2="${project(0)}" y2="${yMid + 8}" class="zs-zero"/>
        ${dots}
      </svg>
    </div>`;
  }).join('');
}

function renderZScoreStripPlot(view, clusterFilter) {
  // Abort any previous document-level drag listeners to avoid stacking
  if (_stripAbortCtrl) _stripAbortCtrl.abort();
  _stripAbortCtrl = new AbortController();
  const _signal = _stripAbortCtrl.signal;
  const host = document.getElementById('z-strip-plot');
  if (!host) return;
  const { results } = pickDivergenceSet(view, clusterFilter);
  const zMin = parseFloat(document.getElementById('z-min').value) || 0;
  if (!results || results.length === 0) { host.innerHTML = ''; return; }

  const rows = _buildStripSVGs(results, zMin);

  const resetVisible = (_stripZoomLevel !== 0 || _stripPanOffset !== 0) ? 'visible' : '';
  host.innerHTML = `<div class="z-strip-legend">
    <span class="zs-legend-item"><span class="zs-dot zs-med"></span> 2 ≤ |z| &lt; 3</span>
    <span class="zs-legend-item"><span class="zs-dot zs-high"></span> |z| ≥ 3</span>
    <span class="zs-legend-item"><span class="zs-dot zs-new"></span> new job</span>
    <span class="zs-legend-item"><span class="zs-band-outlier-swatch"></span> outlier band (|z| ≥ ${zMin})</span>
    <button class="zs-reset-btn ${resetVisible}" id="zs-reset-zoom">↻ Reset zoom</button>
    <span class="zs-zoom-hint">⌘/Ctrl + wheel to zoom · drag to pan · hover for details · click dot → row</span>
  </div><div class="z-strip-container" id="z-strip-container">${rows}</div>`;

  // Wire interactions
  const container = document.getElementById('z-strip-container');
  if (!container) return;

  // Keep a reference to results + zMin for fast SVG-only redraws during drag
  const _cachedResults = results;
  const _cachedZMin = zMin;

  // Ctrl/⌘ + wheel zoom (data-level)
  container.addEventListener('wheel', (e) => {
    if (!(e.ctrlKey || e.metaKey)) return;
    e.preventDefault();
    const prev = _stripZoomLevel;
    _stripZoomLevel = Math.max(0, Math.min(_stripZoomMax, _stripZoomLevel + (e.deltaY < 0 ? 1 : -1)));
    if (_stripZoomLevel !== prev) renderZScoreStripPlot(view, clusterFilter);
  }, { passive: false });

  // Drag-to-pan (data-level: translates the visible z-range)
  let dragState = null;
  const zRange = () => _stripBaseAbsMax * Math.pow(0.8, _stripZoomLevel) * 2;
  container.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return;
    if (e.target.closest('.zs-dot-circle')) return;
    dragState = { startX: e.clientX, startOffset: _stripPanOffset };
    container.classList.add('dragging');
    e.preventDefault();
  });
  const onDragMove = (e) => {
    if (!dragState) return;
    const dx = e.clientX - dragState.startX;
    const pxWidth = container.getBoundingClientRect().width || 1;
    _stripPanOffset = dragState.startOffset - (dx / pxWidth) * zRange();
    // Fast redraw: only replace the inner SVG rows, keep event listeners intact
    container.innerHTML = _buildStripSVGs(_cachedResults, _cachedZMin);
    // Update reset button visibility
    const resetBtn = document.getElementById('zs-reset-zoom');
    if (resetBtn) resetBtn.classList.toggle('visible', _stripZoomLevel !== 0 || _stripPanOffset !== 0);
  };
  const onDragEnd = () => {
    if (dragState) { dragState = null; container.classList.remove('dragging'); }
  };
  document.addEventListener('mousemove', onDragMove, { signal: _signal });
  document.addEventListener('mouseup', onDragEnd, { signal: _signal });

  // Hover tooltip on dots
  container.addEventListener('mouseover', (e) => {
    const dot = e.target.closest('.zs-dot-circle');
    if (!dot) return;
    const cluster = dot.dataset.cluster;
    const recipe = dot.dataset.recipe;
    const z = dot.dataset.z;
    const isNew = dot.dataset.new === '1';
    tooltip.innerHTML = `<div style="font-weight:600;color:#f0f6fc;margin-bottom:4px">${escapeHtml(cluster)}</div>
      <div style="color:#8b949e;font-size:11px">${escapeHtml(recipe)}${isNew ? ' <span class="new-pill">NEW</span>' : ''}</div>
      <div style="margin-top:4px">z-score: <strong style="color:${Math.abs(parseFloat(z)) >= 3 ? '#f85149' : '#d29922'}">${z}</strong></div>`;
    tooltip.style.display = 'block';
    const r = dot.getBoundingClientRect();
    tooltip.style.left = (r.left + r.width / 2 - tooltip.offsetWidth / 2) + 'px';
    tooltip.style.top = (r.top - tooltip.offsetHeight - 8) + 'px';
  });
  container.addEventListener('mouseout', (e) => {
    if (e.target.closest('.zs-dot-circle')) tooltip.style.display = 'none';
  });

  // Click dot → highlight and scroll to matching table row
  container.addEventListener('click', (e) => {
    const dot = e.target.closest('.zs-dot-circle');
    if (!dot) return;
    const cluster = dot.dataset.cluster;
    const recipe = dot.dataset.recipe;
    // Find matching row in divergence table
    const rows = document.querySelectorAll('#divergence-table tbody tr');
    rows.forEach(row => {
      row.classList.remove('z-strip-highlight');
      const cells = row.querySelectorAll('td');
      if (cells.length >= 2 && cells[0].textContent.includes(cluster) && cells[1].textContent.includes(recipe)) {
        row.classList.add('z-strip-highlight');
        row.scrollIntoView({ behavior: 'smooth', block: 'center' });
        setTimeout(() => row.classList.remove('z-strip-highlight'), 3000);
      }
    });
  });

  // Middle-click dot → open cluster/recipe in new tab
  container.addEventListener('auxclick', (e) => {
    if (e.button !== 1) return;
    const dot = e.target.closest('.zs-dot-circle');
    if (!dot) return;
    e.preventDefault();
    const cluster = dot.dataset.cluster;
    const recipe = dot.dataset.recipe;
    const url = buildUrl(Object.assign({}, parseRoute(), { cluster, recipe }));
    window.open(url, '_blank');
  });

  // Reset zoom button
  const resetBtn = document.getElementById('zs-reset-zoom');
  if (resetBtn) {
    resetBtn.addEventListener('click', () => {
      _stripZoomLevel = 0;
      _stripPanOffset = 0;
      _stripBaseAbsMax = null;
      renderZScoreStripPlot(view, clusterFilter);
    });
  }
}

// ── Utilities ───────────────────────────────────────────────────────────────

function formatNum(n) {
  if (typeof n !== 'number') return n;
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  if (n === Math.floor(n)) return n.toString();
  return n.toFixed(2);
}

// 1234567 ms → "20m 34s"
function formatDuration(ms) {
  if (ms === null || ms === undefined || isNaN(ms)) return String(ms);
  if (ms === 0) return '0s';
  const sign = ms < 0 ? '-' : '';
  const abs = Math.abs(ms);
  if (abs < 1000) return `${sign}${Math.round(abs)}ms`;
  const totalSec = Math.round(abs / 1000);
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  const parts = [];
  if (h) parts.push(`${h}h`);
  if (m) parts.push(`${m}m`);
  if (s || parts.length === 0) parts.push(`${s}s`);
  return sign + parts.join(' ');
}

function isDurationMetric(metric) {
  return typeof metric === 'string' && metric.endsWith('_ms');
}

function formatMetricValue(metric, value) {
  if (isDurationMetric(metric)) return formatDuration(value);
  return formatNum(value);
}

// "p95_job_duration_ms" → "p95 job duration"   (drop the units suffix in the label)
function labelMetric(metric) {
  if (typeof metric !== 'string') return String(metric);
  return metric.replace(/^delta_/, '').replace(/_ms$/, '').replace(/_/g, ' ');
}

// "2025_12_20" / "2025-12-20" → "20-12-2025"
function formatDate(s) {
  if (!s) return s;
  const m = String(s).match(/^(\d{4})[-_/](\d{2})[-_/](\d{2})$/);
  if (!m) return s;
  return `${m[3]}-${m[2]}-${m[1]}`;
}

function formatDateTime(s) {
  if (!s) return s;
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(d.getDate())}-${pad(d.getMonth() + 1)}-${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function copyText(text, btn) {
  const fallback = () => {
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand('copy'); } catch (e) {}
    document.body.removeChild(ta);
  };
  const onOk = () => {
    if (!btn) return;
    btn.classList.add('copied');
    const orig = btn.textContent;
    btn.textContent = '✓';
    setTimeout(() => { btn.textContent = orig; btn.classList.remove('copied'); }, 1200);
  };
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(onOk).catch(() => { fallback(); onOk(); });
  } else {
    fallback();
    onOk();
  }
}

// Global so inline onclick="" can reach it
window.copyText = copyText;
window.showClusterTooltip = showClusterTooltip;
window.hideTooltip = hideTooltip;

function copyIcon(text) {
  // Use a base64-encoded blob via dataset to avoid HTML-escaping issues with arbitrary JSON.
  const id = 'cp_' + Math.random().toString(36).slice(2, 9);
  // Stash payload in a global registry keyed by id; cleaned up after click.
  copyIcon._registry = copyIcon._registry || {};
  copyIcon._registry[id] = text;
  return `<button class="copy-btn" title="Copy to clipboard" data-cp="${id}">⧉</button>`;
}

document.addEventListener('click', (e) => {
  const btn = e.target.closest('.copy-btn[data-cp]');
  if (!btn) return;
  e.stopPropagation();
  const id = btn.dataset.cp;
  const payload = copyIcon._registry && copyIcon._registry[id];
  if (payload !== undefined) copyText(payload, btn);
});

function escapeHtml(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function escapeAttr(s) { return escapeHtml(s); }

function cssEscape(s) {
  if (window.CSS && CSS.escape) return CSS.escape(s);
  return String(s).replace(/[^a-zA-Z0-9_-]/g, c => '\\' + c);
}

function dirOf(path) {
  const i = path.lastIndexOf('/');
  return i < 0 ? '.' : path.slice(0, i) || '.';
}

function triedPathsHtml(clusterName) {
  const refKey = `${data.metadata.reference_date}/${clusterName}`;
  const curKey = `${data.metadata.current_date}/${clusterName}`;
  const ref = clusterJsonTriedPaths[refKey] || [];
  const cur = clusterJsonTriedPaths[curKey] || [];
  if (!ref.length && !cur.length) return '';
  const list = (arr) => arr.map(p => `<li><code>${escapeHtml(p)}</code></li>`).join('');
  return `<details style="margin-top:6px"><summary style="cursor:pointer;color:#8b949e">Paths tried</summary>
    <ul style="font-size:11px;color:#8b949e;margin:6px 0 0 18px">${list(ref)}${list(cur)}</ul></details>`;
}

function stripTrailingSlash(s) {
  return String(s).replace(/\/+$/, '');
}

// ── Generation Summary (boost groups) ──────────────────────────────────────
//
// Loads `_generation_summary_auto_tuner.json` if present, else falls back to
// parsing the human-readable .txt sibling. Schema is generic over `bxx` boost
// groups so future additions need no frontend changes.

async function loadGenerationSummary(dirName) {
  generationSummary = null;
  if (!outputsRoot) return;
  const base = `${outputsRoot}/${encodeURIComponent(dirName)}`;
  // Try JSON first
  try {
    const r = await fetch(`${base}/_generation_summary_auto_tuner.json`, { cache: 'no-store' });
    if (r.ok) { generationSummary = await r.json(); return; }
  } catch (e) { /* fall through */ }
  // Fallback: parse the .txt
  try {
    const r = await fetch(`${base}/_generation_summary_auto_tuner.txt`, { cache: 'no-store' });
    if (r.ok) { generationSummary = parseGenerationSummaryTxt(await r.text()); return; }
  } catch (e) { /* leave null */ }
}

function parseGenerationSummaryTxt(txt) {
  const lines = txt.split(/\r?\n/);
  const summary = { metadata: {}, boost_groups: [] };
  // Find sections delimited by "  bXX <UPPER TITLE>"
  const sectionRe = /^\s*(b\d+)\s+(.+?)\s*$/i;
  let i = 0;
  while (i < lines.length) {
    const ln = lines[i];
    const m = ln.match(sectionRe);
    if (m && /[A-Z]/.test(m[2])) {
      const code = m[1].toLowerCase();
      // Title may contain counts like "OOM REBOOSTING  (6 recipe(s) across 5 cluster(s))"
      const titleFull = m[2].trim().replace(/\s+/g, ' ');
      const titleBase = titleFull.replace(/\s*\(.*$/, '').trim();
      // Extract recipe count and cluster count from the summary header if present
      const recipeCountMatch = titleFull.match(/\((\d+)\s*recipe/i);
      const clusterCountMatch = titleFull.match(/across\s+(\d+)\s*cluster/i);
      const title = titleBase;
      // The next "----" separator and then entries until blank line
      i++;
      while (i < lines.length && /^[-]+$/.test(lines[i].trim())) i++;
      const entries = [];
      while (i < lines.length && lines[i].trim() !== '' && !/^[-]+$/.test(lines[i].trim())) {
        // Cluster line is non-indented (2 spaces). Detail lines are 4-space indented.
        const isDetail = /^\s{4,}/.test(lines[i]);
        if (!isDetail) {
          // Cluster header line. May include "(N recipe(s))" suffix.
          const headerLine = lines[i].trim();
          const headerMatch = headerLine.match(/^([^\s].*?)(?:\s+\((\d+)\s*recipe\(s\)\))?$/);
          const cluster = headerMatch ? headerMatch[1].trim() : headerLine;
          const entry = { cluster };
          i++;
          // Detail lines until next non-detail line / blank
          const recipes = [];
          while (i < lines.length && /^\s{4,}/.test(lines[i]) && lines[i].trim() !== '') {
            const detail = lines[i].trim();
            // b14 form: "Reason: ..."
            const reasonMatch = detail.match(/^Reason:\s*(.*)$/);
            if (reasonMatch) {
              entry.reason = reasonMatch[1];
              const promo = entry.reason.match(/Promoted\s+([\w\-]+)\s*->\s*([\w\-]+)/);
              if (promo) entry.promotion = { from: promo[1], to: promo[2] };
            } else {
              // b16 form: "<recipe>  spark.executor.memory: A -> B (xN.N)"
              const rm = detail.match(/^(\S+)\s+spark\.executor\.memory:\s*(\S+)\s*->\s*(\S+)\s*\(x([\d.]+)\)/);
              if (rm) {
                recipes.push({
                  recipe: rm[1],
                  // Reconstruct the full filename so cluster JSON lookups succeed.
                  recipe_filename: `_${rm[1]}.json`,
                  spark_executor_memory: { from: rm[2], to: rm[3], factor: parseFloat(rm[4]) }
                });
              }
            }
            i++;
          }
          if (recipes.length > 0) entry.recipes = recipes;
          entries.push(entry);
        } else {
          i++;
        }
      }
      const boostGroup = {
        code,
        title,
        kind: entries.some(e => Array.isArray(e.recipes) && e.recipes.length) ? 'recipe' : 'cluster',
        count: entries.length,
        entries
      };
      // Override count with parsed header values when available
      if (recipeCountMatch) boostGroup.count = parseInt(recipeCountMatch[1], 10);
      if (clusterCountMatch) boostGroup.cluster_count = parseInt(clusterCountMatch[1], 10);
      summary.boost_groups.push(boostGroup);
      continue;
    }
    i++;
  }
  return summary;
}

function renderBoostOverview() {
  const host = document.getElementById('boost-overview');
  if (!host) return;
  if (!generationSummary || !Array.isArray(generationSummary.boost_groups) || generationSummary.boost_groups.length === 0) {
    host.innerHTML = '';
    return;
  }

  // Decide a recipe row's state: explicit `state` from the JSON wins; legacy fallback
  // uses the (now-deprecated) `propagated` flag, then the from/to memory equality.
  function recipeState(r) {
    if (r && typeof r.state === 'string' && r.state) return r.state;
    if (r && r.propagated === true) return 'holding';
    const m = r && r.spark_executor_memory;
    if (m && m.from && m.to && m.from === m.to) return 'holding';
    return 'new';
  }
  function clusterState(e) {
    if (e && typeof e.state === 'string' && e.state) return e.state;
    return 'new';
  }
  function chipFor(state) {
    const label = state === 're-boost' ? 're-boost' : state === 'holding' ? 'holding' : 'new';
    return `<span class="boost-state-chip chip-${escapeAttr(label)}">${escapeHtml(label)}</span>`;
  }

  const html = generationSummary.boost_groups.map(g => {
    const code = (g.code || '').toLowerCase();
    const entries = g.entries || [];

    // Partition entries (recipe-kind groups) and clusters (cluster-kind groups) by state.
    // Recipe-kind: split each cluster's recipes into [new/re-boost] and [holding] buckets;
    // emit at most two cluster <li>s per cluster (one per non-empty bucket).
    function renderRecipeBucket(label, predicate) {
      const items = entries.flatMap(e => {
        const recipes = (e.recipes || []).filter(r => predicate(recipeState(r)));
        if (recipes.length === 0) return [];
        const clusterBtn = `<button class="boost-cluster-link" data-cluster="${escapeAttr(e.cluster)}">${escapeHtml(e.cluster)}</button>`;
        const rows = recipes.map(r => {
          const m = r.spark_executor_memory || {};
          const fac = m.factor;
          const cum = m.cumulative_factor;
          const st = recipeState(r);
          let memHtml = '';
          if (m.from && m.to) {
            if (st === 'holding') {
              // No arrow when the boost is holding; show only current memory.
              memHtml = `<span class="delta">${escapeHtml(m.to)}${fac ? ` ×${fac}` : ''}</span>`;
            } else if (st === 're-boost') {
              memHtml = `<span class="delta">${escapeHtml(m.from)} → ${escapeHtml(m.to)}${fac ? ` ×${fac}` : ''}${cum ? ` <span class="cum">(cum ×${cum})</span>` : ''}</span>`;
            } else {
              memHtml = `<span class="delta">${escapeHtml(m.from)} → ${escapeHtml(m.to)}${fac ? ` ×${fac}` : ''}</span>`;
            }
          }
          return `<div class="boost-recipe-row state-${escapeAttr(st)}">
            <span class="recipe-label">
              <button class="boost-recipe-link" data-cluster="${escapeAttr(e.cluster)}" data-recipe="${escapeAttr(r.recipe_filename || ('_' + r.recipe + '.json'))}" title="${escapeAttr(r.recipe_filename || '')}">${escapeHtml(r.recipe)}</button>
              ${chipFor(st)}
            </span>
            ${memHtml}
          </div>`;
        }).join('');
        return [`<li>${clusterBtn} <span class="boost-meta">(${recipes.length} recipe${recipes.length === 1 ? '' : 's'})</span>${rows}</li>`];
      }).join('');
      return items
        ? `<div class="boost-subgroup-title">${escapeHtml(label)}</div><ul class="boost-card-list">${items}</ul>`
        : '';
    }
    function renderClusterBucket(label, predicate) {
      const items = entries.filter(e => predicate(clusterState(e))).map(e => {
        const clusterBtn = `<button class="boost-cluster-link" data-cluster="${escapeAttr(e.cluster)}">${escapeHtml(e.cluster)}</button>`;
        const meta = e.promotion
          ? `<div class="boost-meta">${escapeHtml(e.promotion.from)} → ${escapeHtml(e.promotion.to)}${e.persistence && e.persistence !== 'holding' ? ` · ${escapeHtml(e.persistence)}` : ''}</div>`
          : (e.reason ? `<div class="boost-meta">${escapeHtml(e.reason)}</div>` : '');
        return `<li class="state-${escapeAttr(clusterState(e))}">
          <span class="recipe-label">${clusterBtn} ${chipFor(clusterState(e))}</span>
          ${meta}
        </li>`;
      }).join('');
      return items
        ? `<div class="boost-subgroup-title">${escapeHtml(label)}</div><ul class="boost-card-list">${items}</ul>`
        : '';
    }

    const isRecipeKind = entries.some(e => Array.isArray(e.recipes) && e.recipes.length);
    const newSection = isRecipeKind
      ? renderRecipeBucket('New this run', s => s !== 'holding')
      : renderClusterBucket('New this run', s => s !== 'holding');
    const holdingSection = isRecipeKind
      ? renderRecipeBucket('Holding · no new signal', s => s === 'holding')
      : renderClusterBucket('Holding · no new signal', s => s === 'holding');

    const newCount = (g.count_new !== undefined) ? g.count_new : entries.reduce((acc, e) => acc + ((e.recipes || []).filter(r => recipeState(r) !== 'holding').length || (clusterState(e) !== 'holding' ? 1 : 0)), 0);
    const holdingCount = (g.count_holding !== undefined) ? g.count_holding : entries.reduce((acc, e) => acc + ((e.recipes || []).filter(r => recipeState(r) === 'holding').length || (clusterState(e) === 'holding' ? 1 : 0)), 0);
    const headerCount = `${newCount}${holdingCount > 0 ? ` <span class="boost-count-sub">· ${holdingCount} holding</span>` : ''}${g.cluster_count !== undefined ? ` <span class="boost-count-sub">(${g.cluster_count} cluster${g.cluster_count === 1 ? '' : 's'})</span>` : ''}`;
    const body = (newSection + holdingSection) || `<div class="empty-msg">none</div>`;

    return `<div class="boost-card ${escapeAttr(code)}">
      <div class="boost-card-header">
        <span class="bxx-badge">${escapeHtml(code)}</span>
        <span class="boost-title">${escapeHtml(g.title || code.toUpperCase())}</span>
        <span class="boost-count">${headerCount}</span>
      </div>
      ${body}
    </div>`;
  }).join('');
  host.innerHTML = html;

  host.querySelectorAll('.boost-cluster-link').forEach(b => {
    b.addEventListener('click', (ev) => {
      ev.stopPropagation();
      navigate({ cluster: b.dataset.cluster, recipe: null });
    });
    b.addEventListener('auxclick', (ev) => {
      if (ev.button !== 1) return;
      ev.preventDefault(); ev.stopPropagation();
      window.open(buildUrl(Object.assign({}, parseRoute(), { cluster: b.dataset.cluster, recipe: null })), '_blank');
    });
  });
  host.querySelectorAll('.boost-recipe-link').forEach(b => {
    b.addEventListener('click', (ev) => {
      ev.stopPropagation();
      navigate({ cluster: b.dataset.cluster, recipe: b.dataset.recipe });
    });
    b.addEventListener('auxclick', (ev) => {
      if (ev.button !== 1) return;
      ev.preventDefault(); ev.stopPropagation();
      window.open(buildUrl(Object.assign({}, parseRoute(), { cluster: b.dataset.cluster, recipe: b.dataset.recipe })), '_blank');
    });
  });
}

// ── Cluster Summaries section (and searchable modal) ───────────────────────

async function discoverClusterSummaries(dirName) {
  clusterSummaryFiles = [];
  if (!outputsRoot) return;
  const base = `${outputsRoot}/${encodeURIComponent(dirName)}`;
  // Use directory listing if available (python http.server / index.html style).
  try {
    const r = await fetch(`${base}/`);
    if (r.ok) {
      const html = await r.text();
      const re = /<a href="([^"]+)"/gi;
      let m;
      const found = new Set();
      while ((m = re.exec(html)) !== null) {
        let href = decodeURIComponent(m[1]);
        if (/^_clusters-summary[^/]*\.csv$/i.test(href)) found.add(href);
      }
      clusterSummaryFiles = Array.from(found).sort();
      if (clusterSummaryFiles.length > 0) return;
    }
  } catch (e) { /* fall through */ }
  // Fallback: probe the canonical names.
  const known = [
    '_clusters-summary.csv',
    '_clusters-summary-only-clusters-wf.csv',
    '_clusters-summary_estimated_cost_eur.csv',
    '_clusters-summary_global_cores_and_machines.csv',
    '_clusters-summary_num_of_workers.csv',
    '_clusters-summary_top_jobs.csv',
    '_clusters-summary_total_active_minutes.csv'
  ];
  const probes = await Promise.all(known.map(async (name) => {
    try { const rr = await fetch(`${base}/${name}`, { method: 'HEAD' }); return rr.ok ? name : null; }
    catch (e) { return null; }
  }));
  clusterSummaryFiles = probes.filter(Boolean);
}

function summaryTileLabel(name) {
  // "_clusters-summary_top_jobs.csv" → "Top jobs"
  return name
    .replace(/^_clusters-summary[_-]?/, '')
    .replace(/\.csv$/, '')
    .replace(/[_-]+/g, ' ')
    .trim()
    .replace(/^\w/, c => c.toUpperCase()) || 'Overview';
}

function renderClustersSummarySection() {
  const host = document.getElementById('clusters-summary-section');
  if (!host) return;
  if (!clusterSummaryFiles || clusterSummaryFiles.length === 0) {
    host.innerHTML = '';
    return;
  }
  host.innerHTML =
    `<h3>Cluster Summaries</h3>` +
    `<div class="cs-tiles">` +
    clusterSummaryFiles.map(name =>
      `<div class="cs-tile" data-file="${escapeAttr(name)}">
        <div class="cs-tile-title">${escapeHtml(summaryTileLabel(name))}</div>
        <div class="cs-tile-meta"><code>${escapeHtml(name)}</code></div>
      </div>`
    ).join('') +
    `</div>`;
  host.querySelectorAll('.cs-tile').forEach(t => {
    t.addEventListener('click', () => navigate({ summary: t.dataset.file }));
    t.addEventListener('auxclick', (e) => {
      if (e.button !== 1) return;
      e.preventDefault();
      window.open(buildUrl(Object.assign({}, parseRoute(), { summary: t.dataset.file })), '_blank');
    });
  });
}

// ── Cluster Summary Graphs (historical trends) ──────────────────────────────

// Cache: dirName -> { date, rows: [{ cluster_name, no_of_jobs, num_of_workers,
// worker_machine_type, master_machine_type, total_active_minutes,
// estimated_cost_eur, ... }] } so re-opening the section doesn't re-fetch.
const historicalSummaryCache = {};

async function loadHistoricalSummaries() {
  // Use the run dirs from discoveredEntries — they cover every analysed run.
  const dirs = (discoveredEntries || []).map(e => ({ dir: e.dir, date: e.current_date || e.dir }));
  if (dirs.length === 0) return [];

  const out = await Promise.all(dirs.map(async ({ dir, date }) => {
    if (historicalSummaryCache[dir]) return historicalSummaryCache[dir];
    try {
      const r = await fetch(`${outputsRoot}/${encodeURIComponent(dir)}/_clusters-summary.csv`, { cache: 'no-store' });
      if (!r.ok) return null;
      const txt = await r.text();
      const { headers, rows } = parseCsv(txt);
      const idx = (name) => headers.indexOf(name);
      const recs = rows.map(row => ({
        cluster_name: row[idx('cluster_name')] || '',
        dag_id: row[idx('dag_id')] || '',
        no_of_jobs: toNum(row[idx('no_of_jobs')]),
        num_of_workers: toNum(row[idx('num_of_workers')]),
        worker_machine_type: row[idx('worker_machine_type')] || '',
        master_machine_type: row[idx('master_machine_type')] || '',
        total_active_minutes: toNum(row[idx('total_active_minutes')]),
        estimated_cost_eur: toNum(row[idx('estimated_cost_eur')])
      }));
      const entry = { dir, date, rows: recs };
      historicalSummaryCache[dir] = entry;
      return entry;
    } catch (e) { return null; }
  }));

  return out.filter(Boolean).sort((a, b) => a.date.localeCompare(b.date));
}

function toNum(s) {
  if (s == null || s === '') return 0;
  const n = parseFloat(s);
  return isFinite(n) ? n : 0;
}

async function renderClusterSummaryGraphs() {
  const body = document.getElementById('cs-graphs-body');
  if (!body) return;
  body.innerHTML = '<div class="empty-msg">Loading historical summaries…</div>';

  const history = await loadHistoricalSummaries();
  if (history.length === 0) {
    body.innerHTML = '<div class="empty-msg">No historical cluster summaries found under the outputs directory.</div>';
    return;
  }

  const currentDate = (data && data.metadata && data.metadata.current_date) || '';
  const currentEntryHist = history.find(h => h.date === currentDate) || history[history.length - 1];

  body.innerHTML = `
    <div class="cs-kpi-strip" id="cs-kpi-strip"></div>
    <div class="cs-zoom-hint">Ctrl/⌘ + scroll to zoom charts · click points to jump to cluster · ⤢ expand for detail</div>
    <div class="cs-chart-grid">
      ${csChartCell('Estimated cost (€) over time', 'cs-line-cost')}
      ${csChartCell('Workers over time', 'cs-line-workers')}
      ${csChartCell('Total active minutes over time', 'cs-line-minutes')}
      ${csChartCell('Number of jobs over time', 'cs-line-jobs')}
      ${csChartCell('Total fleet cost over time (stacked by cluster)', 'cs-area-cost')}
      ${csChartCell('Cost vs jobs <span class="cs-current-marker-legend">● current run</span>', 'cs-scatter-jobs')}
      ${csChartCell('Cost vs workers', 'cs-scatter-workers')}
      ${csChartCell('Cost vs active minutes', 'cs-scatter-minutes')}
      ${csChartCell('Per-cluster cost distribution', 'cs-distribution', true)}
      ${csChartCell('Cost share by worker machine type (current run)', 'cs-pie-machine')}
    </div>
  `;

  renderCsKpis(history, currentEntryHist);
  wireCsExpandButtons();
  renderCsLineCharts(history, currentDate);
  renderCsAreaChart(history, currentDate);
  renderCsScatters(history, currentDate);
  renderCsDistribution(history, currentDate);
  renderCsPie(currentEntryHist);
}

// Helper: generate a chart cell with header bar (title + reset + expand buttons)
// wide=true means permanently full-width (e.g. distribution); others expand/collapse.
function csChartCell(title, canvasId, wide) {
  const wideClass = wide ? ' cs-chart-wide' : '';
  return `<div class="cs-chart-cell${wideClass}" data-canvas-id="${canvasId}"${wide ? ' data-permanent-wide="1"' : ''}>
    <div class="cs-chart-cell-header">
      <h4>${title}</h4>
      <button class="cs-reset-btn" title="Reset zoom">↻</button>
      <button class="cs-expand-btn" title="Expand / collapse">⤢</button>
    </div>
    <div class="cs-chart-canvas"><canvas id="${canvasId}"></canvas></div>
  </div>`;
}

function wireCsExpandButtons() {
  document.querySelectorAll('.cs-expand-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const cell = btn.closest('.cs-chart-cell');
      // For permanently-wide cells (distribution), only toggle expanded height
      const isPermanentWide = cell.dataset.permanentWide === '1';
      if (isPermanentWide) {
        const isExpanded = cell.classList.toggle('cs-chart-expanded');
        btn.textContent = isExpanded ? '⤡' : '⤢';
      } else {
        // Toggle expanded: adds full-width + taller canvas
        const isExpanded = cell.classList.toggle('cs-chart-expanded');
        btn.textContent = isExpanded ? '⤡' : '⤢';
      }
      // Resize charts in the cell
      const canvas = cell.querySelector('canvas');
      if (canvas && canvas._chartInstance) {
        setTimeout(() => canvas._chartInstance.resize(), 50);
      }
    });
  });
  document.querySelectorAll('.cs-reset-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const cell = btn.closest('.cs-chart-cell');
      const canvas = cell.querySelector('canvas');
      if (canvas && canvas._chartInstance) {
        canvas._chartInstance.resetZoom();
        btn.classList.remove('visible');
      }
    });
  });
}

// Shared zoom plugin config for Chart.js charts.
//
// `clampX` / `clampY` constrain pan/zoom so non-negative metrics (time, worker
// counts, IP counts, cost) cannot drift into the empty negative quadrant. Pass
// `clampX: true` for time-series x axes; `clampY: true` for non-negative y axes.
// Z-score / signed-delta charts must NOT clamp — leave both flags unset.
function csZoomPluginConfig(canvas, opts) {
  const clampX = !!(opts && opts.clampX);
  const clampY = !!(opts && opts.clampY);
  const cfg = {
    zoom: {
      wheel: { enabled: true, modifierKey: 'ctrl' },
      pinch: { enabled: true },
      mode: 'xy',
      onZoom: () => {
        const cell = canvas.closest('.cs-chart-cell');
        if (cell) { const rb = cell.querySelector('.cs-reset-btn'); if (rb) rb.classList.add('visible'); }
      }
    },
    pan: {
      enabled: true,
      mode: 'xy',
      onPan: () => {
        const cell = canvas.closest('.cs-chart-cell');
        if (cell) { const rb = cell.querySelector('.cs-reset-btn'); if (rb) rb.classList.add('visible'); }
      }
    }
  };
  if (clampX || clampY) {
    cfg.limits = {};
    if (clampX) cfg.limits.x = { min: 'original', max: 'original' };
    if (clampY) cfg.limits.y = { min: 0, max: 'original' };
  }
  return cfg;
}

// Compute and render the saved-€ KPI strip. "Saved" is defined as the gap from
// each cluster's worst historical run to the current run, summed and clamped ≥ 0.
function renderCsKpis(history, currentEntry) {
  const host = document.getElementById('cs-kpi-strip');
  if (!host) return;

  // Per-cluster: max prior cost (across all runs) - current cost. Clamp ≥ 0.
  const currentByCluster = {};
  (currentEntry.rows || []).forEach(r => { currentByCluster[r.cluster_name] = r; });

  const priorByCluster = {};
  history.forEach(h => {
    if (h.dir === currentEntry.dir) return;
    h.rows.forEach(r => {
      const m = priorByCluster[r.cluster_name] || { maxCost: 0 };
      if (r.estimated_cost_eur > m.maxCost) m.maxCost = r.estimated_cost_eur;
      priorByCluster[r.cluster_name] = m;
    });
  });

  let savedEur = 0;
  Object.keys(currentByCluster).forEach(name => {
    const cur = currentByCluster[name].estimated_cost_eur;
    const max = (priorByCluster[name] || { maxCost: 0 }).maxCost;
    savedEur += Math.max(0, max - cur);
  });

  const totalJobs = (currentEntry.rows || []).reduce((s, r) => s + r.no_of_jobs, 0);
  const totalWorkers = (currentEntry.rows || []).reduce((s, r) => s + r.num_of_workers, 0);
  const totalMinutes = (currentEntry.rows || []).reduce((s, r) => s + r.total_active_minutes, 0);

  const kpi = (label, value, sub) => `<div class="cs-kpi-card">
    <div class="cs-kpi-label">${label}</div>
    <div class="cs-kpi-value">${value}</div>
    ${sub ? `<div class="cs-kpi-sub">${sub}</div>` : ''}
  </div>`;

  host.innerHTML = [
    kpi('Cumulative € saved', `${formatNum(savedEur)} €`, `vs each cluster's worst prior run`),
    kpi('Saved € per job', totalJobs > 0 ? `${(savedEur / totalJobs).toFixed(2)} €` : '—', `current run · ${totalJobs} jobs`),
    kpi('Saved € per worker', totalWorkers > 0 ? `${(savedEur / totalWorkers).toFixed(2)} €` : '—', `current run · ${totalWorkers} workers`),
    kpi('Saved € per active min', totalMinutes > 0 ? `${(savedEur / totalMinutes).toFixed(3)} €` : '—', `current run · ${formatNum(totalMinutes)} min`)
  ].join('');
}

// Build a multi-series line chart: one line per cluster, dates on the x-axis.
// The current-date cluster lines render gold + thicker so users can spot them.
function renderCsLineChart(canvasId, history, currentDate, valueKey, label, formatter) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  const dates = history.map(h => h.date);
  const allClusters = Array.from(new Set(history.flatMap(h => h.rows.map(r => r.cluster_name)))).sort();
  const currentEntry = history.find(h => h.date === currentDate);
  const currentClusters = new Set((currentEntry ? currentEntry.rows : []).map(r => r.cluster_name));

  // Assign each cluster a stable hue so colors don't flicker between charts.
  const datasets = allClusters.map((c, i) => {
    const series = history.map(h => {
      const row = h.rows.find(r => r.cluster_name === c);
      return row ? row[valueKey] : null;
    });
    const isCurrent = currentClusters.has(c);
    const hue = (i * 47) % 360;
    return {
      label: c,
      data: series,
      borderColor: isCurrent ? 'rgba(210,153,34,1)' : `hsla(${hue}, 35%, 60%, 0.55)`,
      backgroundColor: 'transparent',
      borderWidth: isCurrent ? 2.4 : 1.1,
      tension: 0.2,
      spanGaps: true,
      pointRadius: 2.5,
      pointHoverRadius: 4
    };
  });

  const chart = new Chart(canvas, {
    type: 'line',
    data: { labels: dates, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${formatter(ctx.parsed.y)}`
          }
        },
        zoom: csZoomPluginConfig(canvas, { clampX: true, clampY: true })
      },
      scales: {
        x: { ticks: { color: '#8b949e', maxRotation: 45, autoSkip: true }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { ticks: { color: '#8b949e', callback: (v) => formatter(v) }, grid: { color: 'rgba(255,255,255,0.04)' }, title: { display: true, text: label, color: '#8b949e' } }
      },
      onClick: (evt, elements) => {
        if (elements.length > 0) {
          const el = elements[0];
          const clusterName = datasets[el.datasetIndex].label;
          if (evt.native && evt.native.button === 1) {
            window.open(buildUrl(Object.assign({}, parseRoute(), { cluster: clusterName, recipe: null })), '_blank');
          } else {
            navigate({ cluster: clusterName, recipe: null });
          }
        }
      }
    }
  });
  canvas._chartInstance = chart;

  // Middle-click support
  canvas.addEventListener('auxclick', (e) => {
    if (e.button !== 1) return;
    const elements = chart.getElementsAtEventForMode(e, 'nearest', { intersect: true }, false);
    if (elements.length > 0) {
      e.preventDefault();
      const clusterName = datasets[elements[0].datasetIndex].label;
      window.open(buildUrl(Object.assign({}, parseRoute(), { cluster: clusterName, recipe: null })), '_blank');
    }
  });
}

function renderCsLineCharts(history, currentDate) {
  renderCsLineChart('cs-line-cost', history, currentDate, 'estimated_cost_eur', '€', (v) => `${formatNum(v)} €`);
  renderCsLineChart('cs-line-workers', history, currentDate, 'num_of_workers', 'workers', (v) => formatNum(v));
  renderCsLineChart('cs-line-minutes', history, currentDate, 'total_active_minutes', 'min', (v) => formatNum(v));
  renderCsLineChart('cs-line-jobs', history, currentDate, 'no_of_jobs', 'jobs', (v) => formatNum(v));
}

// Stacked-area chart of cost across all clusters over time.
function renderCsAreaChart(history, currentDate) {
  const canvas = document.getElementById('cs-area-cost');
  if (!canvas) return;
  const dates = history.map(h => h.date);
  const allClusters = Array.from(new Set(history.flatMap(h => h.rows.map(r => r.cluster_name)))).sort();
  const currentEntry = history.find(h => h.date === currentDate);
  const currentClusters = new Set((currentEntry ? currentEntry.rows : []).map(r => r.cluster_name));

  const datasets = allClusters.map((c, i) => {
    const series = history.map(h => {
      const row = h.rows.find(r => r.cluster_name === c);
      return row ? row.estimated_cost_eur : 0;
    });
    const isCurrent = currentClusters.has(c);
    const hue = (i * 47) % 360;
    return {
      label: c,
      data: series,
      borderColor: isCurrent ? 'rgba(210,153,34,1)' : `hsla(${hue}, 35%, 50%, 0.7)`,
      backgroundColor: isCurrent ? 'rgba(210,153,34,0.45)' : `hsla(${hue}, 35%, 50%, 0.25)`,
      borderWidth: isCurrent ? 2 : 1,
      fill: true,
      tension: 0.2,
      pointRadius: 0
    };
  });
  const chart = new Chart(canvas, {
    type: 'line',
    data: { labels: dates, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        zoom: csZoomPluginConfig(canvas, { clampX: true, clampY: true })
      },
      scales: {
        x: { stacked: true, ticks: { color: '#8b949e', maxRotation: 45, autoSkip: true }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { stacked: true, ticks: { color: '#8b949e', callback: (v) => `${formatNum(v)} €` }, grid: { color: 'rgba(255,255,255,0.04)' } }
      },
      onClick: (evt, elements) => {
        if (elements.length > 0) {
          const clusterName = datasets[elements[0].datasetIndex].label;
          navigate({ cluster: clusterName, recipe: null });
        }
      }
    }
  });
  canvas._chartInstance = chart;
}

// Cost vs (jobs / workers / minutes) scatters. Every (cluster, date) is a point;
// current-date points get a larger gold marker so they pop.
function renderCsScatter(canvasId, history, currentDate, xKey, xLabel) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const points = [];
  const currentPoints = [];
  history.forEach(h => {
    h.rows.forEach(r => {
      const pt = { x: r[xKey], y: r.estimated_cost_eur, cluster: r.cluster_name, date: h.date };
      if (h.date === currentDate) currentPoints.push(pt);
      else points.push(pt);
    });
  });
  const chart = new Chart(canvas, {
    type: 'scatter',
    data: {
      datasets: [
        { label: 'historical', data: points, backgroundColor: 'rgba(88,166,255,0.45)', borderColor: 'rgba(88,166,255,0.8)', pointRadius: 3 },
        { label: 'current run', data: currentPoints, backgroundColor: 'rgba(210,153,34,0.85)', borderColor: 'rgba(210,153,34,1)', pointRadius: 5, pointStyle: 'rectRot' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#c9d1d9' } },
        tooltip: { callbacks: { label: (ctx) => {
          const p = ctx.raw;
          return `${p.cluster} (${p.date}): ${formatNum(p.x)} ${xLabel} → ${formatNum(p.y)} €`;
        } } },
        zoom: csZoomPluginConfig(canvas, { clampX: true, clampY: true })
      },
      scales: {
        x: { ticks: { color: '#8b949e' }, grid: { color: 'rgba(255,255,255,0.04)' }, title: { display: true, text: xLabel, color: '#8b949e' } },
        y: { ticks: { color: '#8b949e', callback: (v) => `${formatNum(v)} €` }, grid: { color: 'rgba(255,255,255,0.04)' }, title: { display: true, text: '€', color: '#8b949e' } }
      },
      onClick: (evt, elements) => {
        if (elements.length > 0) {
          const allDs = [points, currentPoints];
          const pt = allDs[elements[0].datasetIndex][elements[0].index];
          if (pt) navigate({ cluster: pt.cluster, recipe: null });
        }
      }
    }
  });
  canvas._chartInstance = chart;

  canvas.addEventListener('auxclick', (e) => {
    if (e.button !== 1) return;
    const elements = chart.getElementsAtEventForMode(e, 'nearest', { intersect: true }, false);
    if (elements.length > 0) {
      e.preventDefault();
      const allDs = [points, currentPoints];
      const pt = allDs[elements[0].datasetIndex][elements[0].index];
      if (pt) window.open(buildUrl(Object.assign({}, parseRoute(), { cluster: pt.cluster, recipe: null })), '_blank');
    }
  });
}

function renderCsScatters(history, currentDate) {
  renderCsScatter('cs-scatter-jobs', history, currentDate, 'no_of_jobs', 'jobs');
  renderCsScatter('cs-scatter-workers', history, currentDate, 'num_of_workers', 'workers');
  renderCsScatter('cs-scatter-minutes', history, currentDate, 'total_active_minutes', 'minutes');
}

// Per-cluster cost distribution — now rendered as a Chart.js scatter chart so
// it supports zoom, pan, hover tooltips, clicking, and the expand button works.
function renderCsDistribution(history, currentDate) {
  const canvas = document.getElementById('cs-distribution');
  if (!canvas) return;
  const allClusters = Array.from(new Set(history.flatMap(h => h.rows.map(r => r.cluster_name)))).sort();
  if (allClusters.length === 0) return;

  const clusterIdx = {};
  allClusters.forEach((c, i) => { clusterIdx[c] = i; });

  const histPts = [];
  const curPts = [];
  history.forEach(h => {
    h.rows.forEach(r => {
      const pt = { x: clusterIdx[r.cluster_name], y: r.estimated_cost_eur, cluster: r.cluster_name, date: h.date };
      if (h.date === currentDate) curPts.push(pt);
      else histPts.push(pt);
    });
  });

  const chart = new Chart(canvas, {
    type: 'scatter',
    data: {
      datasets: [
        { label: 'Historical run', data: histPts, backgroundColor: 'rgba(88,166,255,0.55)', borderColor: 'rgba(88,166,255,0.8)', pointRadius: 3.5 },
        { label: 'Current run', data: curPts, backgroundColor: 'rgba(210,153,34,0.85)', borderColor: 'rgba(210,153,34,1)', pointRadius: 5, pointStyle: 'rectRot' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#c9d1d9' } },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const p = ctx.raw;
              return `${p.cluster} (${p.date}): ${formatNum(p.y)} €`;
            }
          }
        },
        zoom: csZoomPluginConfig(canvas, { clampX: true, clampY: true })
      },
      scales: {
        x: {
          type: 'linear',
          ticks: {
            color: '#8b949e',
            stepSize: 1,
            maxRotation: 45,
            minRotation: 35,
            autoSkip: false,
            callback: (v) => {
              const idx = Math.round(v);
              if (idx >= 0 && idx < allClusters.length) {
                const name = allClusters[idx];
                return name.length > 18 ? name.slice(0, 18) + '…' : name;
              }
              return '';
            }
          },
          grid: { color: 'rgba(255,255,255,0.04)' },
          title: { display: true, text: 'Cluster', color: '#8b949e' },
          min: -0.5,
          max: allClusters.length - 0.5
        },
        y: {
          ticks: { color: '#8b949e', callback: (v) => `${formatNum(v)} €` },
          grid: { color: 'rgba(255,255,255,0.04)' },
          title: { display: true, text: 'Estimated cost (€)', color: '#8b949e' }
        }
      },
      onClick: (evt, elements) => {
        if (elements.length > 0) {
          const allDs = [histPts, curPts];
          const pt = allDs[elements[0].datasetIndex][elements[0].index];
          if (pt) navigate({ cluster: pt.cluster, recipe: null });
        }
      }
    }
  });
  canvas._chartInstance = chart;

  canvas.addEventListener('auxclick', (e) => {
    if (e.button !== 1) return;
    const elements = chart.getElementsAtEventForMode(e, 'nearest', { intersect: true }, false);
    if (elements.length > 0) {
      e.preventDefault();
      const allDs = [histPts, curPts];
      const pt = allDs[elements[0].datasetIndex][elements[0].index];
      if (pt) window.open(buildUrl(Object.assign({}, parseRoute(), { cluster: pt.cluster, recipe: null })), '_blank');
    }
  });
}

// Pie chart of current-run cost share by worker_machine_type.
function renderCsPie(currentEntry) {
  const canvas = document.getElementById('cs-pie-machine');
  if (!canvas || !currentEntry) return;
  const byType = {};
  (currentEntry.rows || []).forEach(r => {
    if (!r.worker_machine_type) return;
    byType[r.worker_machine_type] = (byType[r.worker_machine_type] || 0) + r.estimated_cost_eur;
  });
  const labels = Object.keys(byType);
  const values = labels.map(l => byType[l]);
  const colors = labels.map((_, i) => `hsla(${(i * 47) % 360}, 50%, 55%, 0.75)`);
  const chart = new Chart(canvas, {
    type: 'pie',
    data: { labels, datasets: [{ data: values, backgroundColor: colors }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { color: '#c9d1d9' } },
        tooltip: { callbacks: { label: (ctx) => `${ctx.label}: ${formatNum(ctx.parsed)} €` } }
      }
    }
  });
  canvas._chartInstance = chart;
}

async function openSummaryModalRaw(fileName) {
  const dirName = (currentEntry && currentEntry.dir) || (data && data._loadedFor) || data.metadata.current_date;
  openSummaryModal();
  openModal(`<span class="recipe-name-text">${escapeHtml(summaryTileLabel(fileName))}</span> ${copyIcon(fileName)}`,
            `<div class="empty-msg">Loading <code>${escapeHtml(fileName)}</code>…</div>`);

  let parsed = clusterSummaryCache[`${dirName}/${fileName}`];
  if (!parsed) {
    try {
      const r = await fetch(`${outputsRoot}/${encodeURIComponent(dirName)}/${encodeURIComponent(fileName)}`, { cache: 'no-store' });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const txt = await r.text();
      parsed = parseCsv(txt);
      clusterSummaryCache[`${dirName}/${fileName}`] = parsed;
    } catch (e) {
      document.getElementById('modal-body').innerHTML =
        `<div class="empty-msg">Failed to load <code>${escapeHtml(fileName)}</code>: ${escapeHtml(e.message || String(e))}</div>`;
      return;
    }
  }

  renderSummaryCsv(parsed, fileName, { jumpToCluster: true });
}

function renderSummaryCsv(parsed, fileName, opts) {
  opts = opts || {};
  const jumpEnabled = opts.jumpToCluster !== false;
  const { headers, rows } = parsed;
  const knownClusters = jumpEnabled
    ? new Set((data && data.cluster_trends || []).map(c => c.cluster))
    : new Set();
  const clusterColIdx = headers.findIndex(h => /cluster_name|^cluster$/i.test(h));

  const trHtml = rows.map((r, idx) => {
    const cells = headers.map((h, ci) => {
      const v = r[ci] != null ? String(r[ci]) : '';
      if (jumpEnabled && (ci === clusterColIdx || knownClusters.has(v)) && knownClusters.has(v)) {
        return `<td><button class="summary-row-cluster-link" data-cluster="${escapeAttr(v)}">${escapeHtml(v)}</button> ${copyIcon(v)}</td>`;
      }
      return `<td>${escapeHtml(v)}</td>`;
    }).join('');
    return `<tr data-row="${idx}">${cells}</tr>`;
  }).join('');

  document.getElementById('modal-body').innerHTML = `
    <div class="summary-search-row">
      <input type="text" id="summary-search-input" placeholder="Filter rows… (Cmd/Ctrl+F)" autofocus />
      <span class="summary-meta" id="summary-row-count">${rows.length} rows</span>
    </div>
    <div class="summary-scroll">
      <table class="summary-table">
        <thead><tr>${headers.map(h => `<th>${escapeHtml(h)}</th>`).join('')}</tr></thead>
        <tbody>${trHtml}</tbody>
      </table>
    </div>
  `;

  // Cluster jump links
  document.querySelectorAll('#modal-body .summary-row-cluster-link').forEach(b => {
    b.addEventListener('click', (e) => {
      e.stopPropagation();
      navigate({ cluster: b.dataset.cluster, recipe: null, summary: null });
    });
    b.addEventListener('auxclick', (e) => {
      if (e.button !== 1) return;
      e.preventDefault(); e.stopPropagation();
      window.open(buildUrl(Object.assign({}, parseRoute(), { cluster: b.dataset.cluster, recipe: null, summary: null })), '_blank');
    });
  });

  // Filter
  const input = document.getElementById('summary-search-input');
  const counter = document.getElementById('summary-row-count');
  const tbody = document.querySelector('#modal-body table.summary-table tbody');
  input.addEventListener('input', () => {
    const q = input.value.trim().toLowerCase();
    let visible = 0;
    Array.from(tbody.querySelectorAll('tr')).forEach(tr => {
      const text = tr.textContent.toLowerCase();
      const show = !q || text.includes(q);
      tr.style.display = show ? '' : 'none';
      if (show) visible++;
      tr.classList.toggle('hit', !!q && show);
    });
    counter.textContent = `${visible} / ${rows.length} rows`;
  });

  // Intercept Cmd/Ctrl+F while modal is open to focus our search instead.
  const onKey = (e) => {
    if ((e.metaKey || e.ctrlKey) && (e.key === 'f' || e.key === 'F')) {
      const overlay = document.getElementById('modal-overlay');
      if (overlay && overlay.style.display !== 'none') {
        e.preventDefault();
        input.focus();
        input.select();
      }
    }
  };
  document.addEventListener('keydown', onKey);
  // Remove handler when modal closes
  const overlay = document.getElementById('modal-overlay');
  const observer = new MutationObserver(() => {
    if (overlay.style.display === 'none') {
      document.removeEventListener('keydown', onKey);
      observer.disconnect();
    }
  });
  observer.observe(overlay, { attributes: true, attributeFilter: ['style'] });
}

// Minimal RFC4180-ish CSV parser (handles quoted fields + escaped quotes).
function parseCsv(text) {
  const rows = [];
  let row = [], cell = '', i = 0, inQuotes = false;
  const flushCell = () => { row.push(cell); cell = ''; };
  const flushRow  = () => { rows.push(row); row = []; };
  while (i < text.length) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { cell += '"'; i += 2; continue; }
        inQuotes = false; i++; continue;
      }
      cell += c; i++; continue;
    }
    if (c === '"') { inQuotes = true; i++; continue; }
    if (c === ',') { flushCell(); i++; continue; }
    if (c === '\r') { i++; continue; }
    if (c === '\n') { flushCell(); flushRow(); i++; continue; }
    cell += c; i++;
  }
  if (cell.length > 0 || row.length > 0) { flushCell(); flushRow(); }
  // Drop trailing empty row from a trailing newline
  if (rows.length && rows[rows.length - 1].length === 1 && rows[rows.length - 1][0] === '') rows.pop();
  const headers = rows.shift() || [];
  return { headers, rows };
}

// ── Inputs tree (landing) ──────────────────────────────────────────────────

async function renderInputsTree() {
  const section = document.getElementById('landing-inputs');
  const tree = document.getElementById('landing-inputs-tree');
  if (!section || !tree) return;

  const inputsRoot = config.inputsPath
    ? stripTrailingSlash(new URL(config.inputsPath + '/', window.location.href).href)
    : null;
  if (!inputsRoot) { section.style.display = 'none'; return; }

  document.getElementById('landing-inputs-path').textContent = config.inputsPath;
  tree.innerHTML = `<div class="empty-msg">Scanning…</div>`;

  // Discover date dirs by parsing HTTP listing of inputsRoot/
  let dirs = [];
  try {
    const r = await fetch(`${inputsRoot}/`);
    if (r.ok) {
      const html = await r.text();
      const re = /<a href="([^"]+)"/gi;
      let m;
      while ((m = re.exec(html)) !== null) {
        let href = m[1];
        if (!href.endsWith('/')) continue;
        if (href.startsWith('?') || href.startsWith('#') || href === '../' || href === './') continue;
        href = decodeURIComponent(href.replace(/\/$/, ''));
        if (href.startsWith('http://') || href.startsWith('https://')) continue;
        if (href.startsWith('_') || href.startsWith('.')) continue;
        dirs.push(href);
      }
    }
  } catch (e) { /* no dir listing */ }

  if (dirs.length === 0) {
    section.style.display = 'none';
    return;
  }
  section.style.display = 'block';
  // Newest first (opens at top, but visually consistent)
  dirs.sort();

  // Fetch each dir's file list in parallel.
  const dirContents = await Promise.all(dirs.map(async (d) => {
    try {
      const r = await fetch(`${inputsRoot}/${encodeURIComponent(d)}/`);
      if (!r.ok) return { d, files: [] };
      const html = await r.text();
      const re = /<a href="([^"]+)"/gi;
      const files = [];
      let m;
      while ((m = re.exec(html)) !== null) {
        let href = decodeURIComponent(m[1]);
        if (href.endsWith('/') || href.startsWith('?') || href.startsWith('#') || href.startsWith('.')) continue;
        files.push(href);
      }
      files.sort();
      return { d, files };
    } catch (e) { return { d, files: [] }; }
  }));

  tree.innerHTML = dirContents.map(({ d, files }, idx) => {
    const fileItems = files.map(f => {
      const url = `${inputsRoot}/${encodeURIComponent(d)}/${encodeURIComponent(f)}`;
      const isCsv = /\.csv$/i.test(f);
      if (isCsv) {
        return `<div class="file"><button class="file-preview-link" data-url="${escapeAttr(url)}" data-name="${escapeAttr(f)}" title="Preview ${escapeAttr(f)}">${escapeHtml(f)}</button></div>`;
      }
      return `<div class="file"><a href="${url}" target="_blank" rel="noopener">${escapeHtml(f)}</a></div>`;
    }).join('') || `<div class="file empty-msg">(empty)</div>`;
    const open = idx === dirContents.length - 1 ? 'open' : '';
    return `<details ${open}><summary>${escapeHtml(formatDate(d))} <span class="summary-count">${files.length} file${files.length === 1 ? '' : 's'} · <code>${escapeHtml(d)}/</code></span></summary>
      ${fileItems}
    </details>`;
  }).join('');

  tree.querySelectorAll('.file-preview-link').forEach(b => {
    b.addEventListener('click', () => previewCsvUrl(b.dataset.url, b.dataset.name));
  });
}

// Open a generic CSV preview (no cluster jump links — used for inputs tree).
async function previewCsvUrl(url, displayName) {
  openSummaryModal();
  openModal(`<span class="recipe-name-text">${escapeHtml(displayName)}</span> ${copyIcon(displayName)}`,
            `<div class="empty-msg">Loading <code>${escapeHtml(displayName)}</code>…</div>`);
  try {
    const r = await fetch(url, { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const txt = await r.text();
    const parsed = parseCsv(txt);
    renderSummaryCsv(parsed, displayName, { jumpToCluster: false });
  } catch (e) {
    document.getElementById('modal-body').innerHTML =
      `<div class="empty-msg">Failed to load <code>${escapeHtml(displayName)}</code>: ${escapeHtml(e.message || String(e))}</div>`;
  }
}

// Toggle the wide/compact "summary modal" class on the modal so CSV previews
// have more horizontal/vertical room than the recipe spark conf modal.
function openSummaryModal() {
  document.getElementById('modal').classList.add('summary-modal');
}
function closeSummaryModalClass() {
  document.getElementById('modal').classList.remove('summary-modal');
}

// ── Boot ────────────────────────────────────────────────────────────────────
bootstrap();