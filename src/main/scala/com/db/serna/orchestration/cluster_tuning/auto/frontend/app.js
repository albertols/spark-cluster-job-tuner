Classification: Public

app.js
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
    body: "Improved / Degraded / Stable / NewEntry / DroppedEntry. Decided per (cluster, recipe) by comparing the reference and current metrics with built-in noise thresholds."
  }
};

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
      if (Array.isArray(j.entries)) return j.entries;
    }
  } catch (e) { /* fall through */ }

  // 2) Fallback: parse python http.server's HTML directory listing.
  try {
    const r = await fetch(`${outputsRoot}/`);
    if (!r.ok) return [];
    const html = await r.text();
    const dirs = parseDirListing(html);
    const entries = await Promise.all(dirs.map(buildEntryFromDir));
    return entries.filter(Boolean);
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
      const r = await fetch(p);
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
  const [ref, cur] = await Promise.all([
    loadClusterJson(refDate, clusterName),
    loadClusterJson(curDate, clusterName),
  ]);
  return { ref, cur, refDate, curDate };
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
  document.getElementById('z-min').addEventListener('input', renderDivergenceTable);

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
  renderClusterGrid();
  renderCorrelationMatrix();
  renderDivergenceTable();
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

    return `<div class="cluster-card" data-cluster="${escapeAttr(c.cluster)}"
                 onmouseenter="showClusterTooltip(event, '${escapeAttr(c.cluster)}')"
                 onmouseleave="hideTooltip()">
      <span class="trend-indicator ${c.overall_trend}"></span>
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

  // Cluster conf comparison (loads asynchronously)
  document.getElementById('detail-cluster-conf').innerHTML =
    `<h3>Cluster Configuration <span class="info-icon" data-doc-key="trend" title="Compares reference vs current date config">ⓘ</span></h3>` +
    `<div class="empty-msg">Loading cluster configurations…</div>`;

  loadClusterJsonsForDates(clusterName).then(({ ref, cur, refDate, curDate }) => {
    renderClusterConfComparison(clusterName, ref, cur, refDate, curDate);
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
  });

  renderDetailCharts(cluster, clusterName);
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

  const recipes = cluster.recipes.filter(r => r.deltas.length > 0);

  // Use horizontal bars when there are many recipes — labels stay readable.
  const horizontal = recipes.length > 8;
  const barThickness = 12;
  const datasetsPerCategory = 2;
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

  const fullLabels = recipes.map(r => recipeShortName(r.recipe));
  const tooltipFullNames = recipes.map(r => r.recipe);

  const durRef = recipes.map(r => deltaValue(r, 'p95_job_duration_ms', 'reference'));
  const durCur = recipes.map(r => deltaValue(r, 'p95_job_duration_ms', 'current'));
  const execRef = recipes.map(r => deltaValue(r, 'p95_run_max_executors', 'reference'));
  const execCur = recipes.map(r => deltaValue(r, 'p95_run_max_executors', 'current'));

  const baseDataset = (label, values, color) => ({
    label,
    data: values,
    backgroundColor: color.replace('1)', '0.5)'),
    borderColor: color.replace('0.5)', '1)'),
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
        baseDataset('Reference', durRef, 'rgba(88,166,255,1)'),
        baseDataset('Current', durCur, 'rgba(248,81,73,1)')
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
        baseDataset('Reference', execRef, 'rgba(88,166,255,1)'),
        baseDataset('Current', execCur, 'rgba(248,81,73,1)')
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

function deltaValue(recipe, metric, field) {
  const d = recipe.deltas.find(x => x.metric === metric);
  return d ? d[field] : 0;
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
  docPopover.innerHTML = `<div class="doc-title">${escapeHtml(doc.title)}</div><div>${escapeHtml(doc.body)}</div>`;
  docPopover.style.display = 'block';
  const x = event.clientX + 12;
  const y = event.clientY + 12;
  docPopover.style.left = Math.min(x, window.innerWidth - 380) + 'px';
  docPopover.style.top = Math.min(y, window.innerHeight - 200) + 'px';
}

function hideDocPopover() {
  docPopover.style.display = 'none';
}

// ── Correlation Matrix ──────────────────────────────────────────────────────

function renderCorrelationMatrix() {
  const corrs = data.correlations;
  if (!corrs || corrs.length === 0) {
    document.getElementById('correlation-matrix').innerHTML =
      '<p style="color:#8b949e">No correlation data available.</p>';
    return;
  }

  const html = corrs.map(c => {
    const color = corrColor(c.pearson);
    const shortA = labelMetric(c.metric_a.replace(/^delta_/, ''));
    const shortB = labelMetric(c.metric_b.replace(/^delta_/, ''));
    return `<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
      <div style="width:200px;font-size:11px;color:#c9d1d9;text-align:right">${shortA}</div>
      <div style="width:20px;text-align:center;color:#8b949e">↔</div>
      <div style="width:200px;font-size:11px;color:#c9d1d9">${shortB}</div>
      <div class="corr-cell" style="background:${color};color:#f0f6fc;min-width:80px" title="Pearson: ${c.pearson.toFixed(3)} | Cov: ${c.covariance.toFixed(2)} | n=${c.n}">
        ${c.pearson.toFixed(3)}
      </div>
      <div style="font-size:10px;color:#8b949e">n=${c.n} <span class="info-icon" data-doc-key="pearson">ⓘ</span></div>
    </div>`;
  }).join('');

  document.getElementById('correlation-matrix').innerHTML = html;
}

function corrColor(r) {
  if (r > 0.5) return `rgba(248, 81, 73, ${0.3 + Math.abs(r) * 0.5})`;
  if (r < -0.5) return `rgba(63, 185, 80, ${0.3 + Math.abs(r) * 0.5})`;
  if (r > 0.2) return `rgba(248, 81, 73, 0.2)`;
  if (r < -0.2) return `rgba(63, 185, 80, 0.2)`;
  return 'rgba(139, 148, 158, 0.1)';
}

// ── Divergence Table ────────────────────────────────────────────────────────

function renderDivergenceTable() {
  const zMin = parseFloat(document.getElementById('z-min').value) || 0;
  const divs = (data.divergences || [])
    .filter(d => Math.abs(d.z_score) >= zMin)
    .sort((a, b) => Math.abs(b.z_score) - Math.abs(a.z_score));

  const tbody = document.querySelector('#divergence-table tbody');
  tbody.innerHTML = divs.map(d => {
    const zCls = Math.abs(d.z_score) >= 3 ? 'z-high' : 'z-med';
    return `<tr>
      <td>${escapeHtml(d.cluster)} ${copyIcon(d.cluster)}</td>
      <td>${escapeHtml(d.recipe)} ${copyIcon(d.recipe)}</td>
      <td>${labelMetric(d.metric)}</td>
      <td>${formatMetricValue(d.metric, d.reference)}</td>
      <td>${formatMetricValue(d.metric, d.current)}</td>
      <td class="${zCls}">${d.z_score.toFixed(2)}</td>
    </tr>`;
  }).join('');

  if (divs.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:#8b949e;padding:24px">No divergences above threshold</td></tr>';
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
  const html = generationSummary.boost_groups.map(g => {
    const code = (g.code || '').toLowerCase();
    const list = (g.entries || []).map(e => {
      const clusterBtn = `<button class="boost-cluster-link" data-cluster="${escapeAttr(e.cluster)}">${escapeHtml(e.cluster)}</button>`;
      if (Array.isArray(e.recipes) && e.recipes.length > 0) {
        const rows = e.recipes.map(r => {
          const memTo = r.spark_executor_memory && r.spark_executor_memory.to;
          const memFrom = r.spark_executor_memory && r.spark_executor_memory.from;
          const fac = r.spark_executor_memory && r.spark_executor_memory.factor;
          const meta = (memFrom && memTo)
            ? `<span class="delta">${escapeHtml(memFrom)} → ${escapeHtml(memTo)}${fac ? ` ×${fac}` : ''}</span>`
            : '';
          return `<div class="boost-recipe-row">
            <button class="boost-recipe-link" data-cluster="${escapeAttr(e.cluster)}" data-recipe="${escapeAttr(r.recipe_filename || ('_' + r.recipe + '.json'))}" title="${escapeAttr(r.recipe_filename || '')}">${escapeHtml(r.recipe)}</button>
            ${meta}
          </div>`;
        }).join('');
        return `<li>${clusterBtn} <span class="boost-meta">(${e.recipes.length} recipe${e.recipes.length === 1 ? '' : 's'})</span>${rows}</li>`;
      }
      const meta = e.promotion
        ? `<div class="boost-meta">${escapeHtml(e.promotion.from)} → ${escapeHtml(e.promotion.to)}${e.persistence ? ` · ${escapeHtml(e.persistence)}` : ''}</div>`
        : (e.reason ? `<div class="boost-meta">${escapeHtml(e.reason)}</div>` : '');
      return `<li>${clusterBtn}${meta}</li>`;
    }).join('');
    return `<div class="boost-card ${escapeAttr(code)}">
      <div class="boost-card-header">
        <span class="bxx-badge">${escapeHtml(code)}</span>
        <span class="boost-title">${escapeHtml(g.title || code.toUpperCase())}</span>
        <span class="boost-count">${g.count}${g.cluster_count !== undefined ? ` <span class="boost-count-sub">(${g.cluster_count} cluster${g.cluster_count === 1 ? '' : 's'})</span>` : ''}</span>
      </div>
      ${list ? `<ul class="boost-card-list">${list}</ul>` : `<div class="empty-msg">none</div>`}
    </div>`;
  }).join('');
  host.innerHTML = html;

  host.querySelectorAll('.boost-cluster-link').forEach(b => {
    b.addEventListener('click', (ev) => {
      ev.stopPropagation();
      navigate({ cluster: b.dataset.cluster, recipe: null });
    });
  });
  host.querySelectorAll('.boost-recipe-link').forEach(b => {
    b.addEventListener('click', (ev) => {
      ev.stopPropagation();
      navigate({ cluster: b.dataset.cluster, recipe: b.dataset.recipe });
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
  });
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