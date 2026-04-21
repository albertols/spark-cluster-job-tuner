// Spark Cluster Auto-Tuner Dashboard
// Reads _auto_tuner_analysis.json and renders interactive visualizations.

let data = null;
let analysisDir = '.';   // dir (relative or absolute URL) holding _auto_tuner_analysis.json
const tooltip = document.getElementById('tooltip');
const docPopover = document.getElementById('doc-popover');

// Per-cluster JSON cache: key = "<date>/<cluster>" → parsed json (or null if not found)
const clusterJsonCache = {};
// Last URLs we tried to fetch for a cluster (for diagnostics in the UI)
const clusterJsonTriedPaths = {};

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

// ── Data Loading ────────────────────────────────────────────────────────────

async function loadData() {
  const paths = [
    '_auto_tuner_analysis.json',
    '../_auto_tuner_analysis.json',
  ];

  const urlParams = new URLSearchParams(window.location.search);
  const customPath = urlParams.get('data');
  if (customPath) paths.unshift(customPath);

  for (const path of paths) {
    try {
      const resp = await fetch(path);
      if (resp.ok) {
        data = await resp.json();
        analysisDir = dirOf(path);
        console.log('Loaded analysis data from:', path, '— analysisDir:', analysisDir);
        return;
      }
    } catch (e) { /* try next */ }
  }

  document.querySelector('main').innerHTML =
    '<div style="text-align:center;padding:60px;color:#8b949e;">' +
    '<h2>No analysis data found</h2>' +
    '<p>Place <code>_auto_tuner_analysis.json</code> in the served directory,<br>' +
    'or pass <code>?data=path/to/file.json</code> as a URL parameter.</p>' +
    '</div>';
}

async function loadClusterJson(date, clusterName) {
  const key = `${date}/${clusterName}`;
  if (clusterJsonCache[key] !== undefined) return clusterJsonCache[key];

  const isCur = date === data.metadata.current_date;
  // The auto-tuner writes the current date to "<curDate>_auto_tuned/" and the
  // reference date to "<refDate>/" (no suffix). The analysis JSON lives in the
  // current dir, so reference JSONs are at "<analysisDir>/../<refDate>/...".
  // We try a couple of common name variants so customised dir layouts also work.
  const candidates = [];
  if (isCur) {
    candidates.push(
      `${analysisDir}/${clusterName}-auto-scale-tuned.json`,
      `${analysisDir}/${clusterName}-manually-tuned.json`
    );
  } else {
    candidates.push(
      `${analysisDir}/../${date}/${clusterName}-auto-scale-tuned.json`,
      `${analysisDir}/../${date}/${clusterName}-manually-tuned.json`,
      `${analysisDir}/../${date}_auto_tuned/${clusterName}-auto-scale-tuned.json`,
      `${analysisDir}/../${date}_auto_tuned/${clusterName}-manually-tuned.json`
    );
  }

  clusterJsonTriedPaths[key] = candidates.slice();

  for (const p of candidates) {
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

// ── Initialization ──────────────────────────────────────────────────────────

async function init() {
  await loadData();
  if (!data) return;

  renderMetadataBar();
  renderSummaryCards();
  renderClusterGrid();
  renderCorrelationMatrix();
  renderDivergenceTable();

  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => switchTab(tab.dataset.tab));
  });

  document.getElementById('cluster-search').addEventListener('input', renderClusterGrid);
  document.getElementById('trend-filter').addEventListener('change', renderClusterGrid);
  document.getElementById('z-min').addEventListener('input', renderDivergenceTable);

  document.getElementById('back-to-overview').addEventListener('click', () => {
    document.getElementById('cluster-detail').style.display = 'none';
    document.getElementById('overview').classList.add('active');
  });

  // Modal close
  document.getElementById('modal-close').addEventListener('click', closeModal);
  document.getElementById('modal-overlay').addEventListener('click', (e) => {
    if (e.target.id === 'modal-overlay') closeModal();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') { closeModal(); hideDocPopover(); }
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

function switchTab(tabName) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.querySelector(`.tab[data-tab="${tabName}"]`).classList.add('active');
  document.getElementById(tabName).classList.add('active');
  document.getElementById('cluster-detail').style.display = 'none';
}

// ── Metadata Bar ────────────────────────────────────────────────────────────

function renderMetadataBar() {
  const m = data.metadata;
  document.getElementById('metadata-bar').innerHTML =
    `<span class="date-pill ref" title="Reference date">${formatDate(m.reference_date)}</span>` +
    `<span class="date-arrow">→</span>` +
    `<span class="date-pill cur" title="Current date">${formatDate(m.current_date)}</span>` +
    `<span>Strategy: <strong>${escapeHtml(m.strategy)}</strong></span>` +
    `<span>${m.total_clusters} clusters · ${m.total_recipes} recipes</span>` +
    `<span>Generated: ${new Date(m.generated_at).toLocaleString()}</span>`;
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
    card.addEventListener('click', () => showClusterDetail(card.dataset.cluster));
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
  const fields = [
    ['workers', conf.num_workers],
    ['worker', conf.worker_machine_type],
    ['master', conf.master_machine_type],
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
      showRecipeConfModal(card.dataset.cluster, card.dataset.recipe);
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
  const barThickness = 16;
  const minHeight = 220;
  const computedHeight = horizontal ? Math.max(minHeight, recipes.length * (barThickness + 8) + 80) : minHeight;

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
    if (recipe) showRecipeConfModal(clusterName, recipe.recipe);
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
  document.getElementById('modal-overlay').style.display = 'none';
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

// ── Boot ────────────────────────────────────────────────────────────────────
init();
