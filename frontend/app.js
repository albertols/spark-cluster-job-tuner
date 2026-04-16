// Spark Cluster Auto-Tuner Dashboard
// Reads _auto_tuner_analysis.json and renders interactive visualizations.

let data = null;
const tooltip = document.getElementById('tooltip');

// ── Data Loading ────────────────────────────────────────────────────────────

async function loadData() {
  // Try multiple possible paths for the analysis JSON
  const paths = [
    '_auto_tuner_analysis.json',
    '../_auto_tuner_analysis.json',
  ];

  // Also check URL parameter
  const urlParams = new URLSearchParams(window.location.search);
  const customPath = urlParams.get('data');
  if (customPath) paths.unshift(customPath);

  for (const path of paths) {
    try {
      const resp = await fetch(path);
      if (resp.ok) {
        data = await resp.json();
        console.log('Loaded analysis data from:', path);
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

// ── Initialization ──────────────────────────────────────────────────────────

async function init() {
  await loadData();
  if (!data) return;

  renderMetadataBar();
  renderSummaryCards();
  renderClusterGrid();
  renderCorrelationMatrix();
  renderDivergenceTable();

  // Tab switching
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => switchTab(tab.dataset.tab));
  });

  // Filters
  document.getElementById('cluster-search').addEventListener('input', renderClusterGrid);
  document.getElementById('trend-filter').addEventListener('change', renderClusterGrid);

  // Divergence z-score filter
  document.getElementById('z-min').addEventListener('input', renderDivergenceTable);

  // Back button in detail view
  document.getElementById('back-to-overview').addEventListener('click', () => {
    document.getElementById('cluster-detail').style.display = 'none';
    document.getElementById('overview').classList.add('active');
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
    `${m.reference_date} &rarr; ${m.current_date} &nbsp;|&nbsp; ` +
    `Strategy: ${m.strategy} &nbsp;|&nbsp; ` +
    `${m.total_clusters} clusters, ${m.total_recipes} recipes &nbsp;|&nbsp; ` +
    `Generated: ${new Date(m.generated_at).toLocaleString()}`;
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
      return `<span class="pill ${r.trend}" title="${r.recipe}: ${r.trend} (${r.action})">${displayName}</span>`;
    }).join('');
    const extra = c.recipes.length > 6 ? `<span class="pill stable">+${c.recipes.length - 6} more</span>` : '';

    return `<div class="cluster-card" data-cluster="${c.cluster}"
                 onmouseenter="showClusterTooltip(event, '${c.cluster}')"
                 onmouseleave="hideTooltip()">
      <span class="trend-indicator ${c.overall_trend}"></span>
      <div class="cluster-name">${c.cluster}</div>
      <div class="cluster-meta">${c.recipes.length} recipes &middot; ${c.overall_trend}</div>
      <div class="recipe-pills">${recipePills}${extra}</div>
    </div>`;
  }).join('');

  // Click handlers
  grid.querySelectorAll('.cluster-card').forEach(card => {
    card.addEventListener('click', () => showClusterDetail(card.dataset.cluster));
  });
}

// ── Cluster Tooltip ─────────────────────────────────────────────────────────

function showClusterTooltip(event, clusterName) {
  const cluster = data.cluster_trends.find(c => c.cluster === clusterName);
  if (!cluster) return;

  const degraded = cluster.recipes.filter(r => r.trend === 'degraded').length;
  const improved = cluster.recipes.filter(r => r.trend === 'improved').length;
  const stable = cluster.recipes.filter(r => r.trend === 'stable').length;

  let html = `<strong>${clusterName}</strong><br>`;
  html += `<table>`;
  if (degraded) html += `<tr><td class="pct-pos">${degraded} degraded</td></tr>`;
  if (improved) html += `<tr><td class="pct-neg">${improved} improved</td></tr>`;
  if (stable) html += `<tr><td>${stable} stable</td></tr>`;
  html += `</table>`;

  // Show top deltas for degraded recipes
  const topDegraded = cluster.recipes
    .filter(r => r.trend === 'degraded')
    .slice(0, 3);
  if (topDegraded.length > 0) {
    html += `<hr style="border-color:#30363d;margin:6px 0">`;
    topDegraded.forEach(r => {
      const p95 = r.deltas.find(d => d.metric === 'p95_job_duration_ms');
      if (p95) {
        html += `<div style="font-size:10px;color:#8b949e">${r.recipe.slice(0, 30)}: ` +
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

function showClusterDetail(clusterName) {
  const cluster = data.cluster_trends.find(c => c.cluster === clusterName);
  if (!cluster) return;

  document.getElementById('overview').classList.remove('active');
  document.getElementById('cluster-detail').style.display = 'block';
  document.getElementById('detail-cluster-name').textContent =
    `${clusterName} (${cluster.overall_trend})`;

  // Recipe cards with delta tables
  const recipesHtml = cluster.recipes.map(r => {
    const deltaRows = r.deltas.map(d => {
      const pctCls = d.pct_change > 0 ? 'pct-pos' : d.pct_change < 0 ? 'pct-neg' : '';
      return `<tr>
        <td class="metric-name">${d.metric.replace(/_/g, ' ')}</td>
        <td>${formatNum(d.reference)}</td>
        <td>${formatNum(d.current)}</td>
        <td class="${pctCls}">${d.pct_change > 0 ? '+' : ''}${d.pct_change.toFixed(1)}%</td>
      </tr>`;
    }).join('');

    return `<div class="detail-recipe-card">
      <h4><span class="pill ${r.trend}">${r.trend}</span> ${r.recipe}</h4>
      <div class="detail-meta">
        Action: ${r.action} &middot; Confidence: ${(r.confidence * 100).toFixed(0)}%
      </div>
      <table class="delta-table">
        <tr style="color:#8b949e"><td>Metric</td><td>Ref</td><td>Cur</td><td>Change</td></tr>
        ${deltaRows}
      </table>
    </div>`;
  }).join('');

  document.getElementById('detail-recipes').innerHTML = recipesHtml;

  // Charts: p95 duration and executor demand per recipe
  renderDetailCharts(cluster);
}

function renderDetailCharts(cluster) {
  const chartsDiv = document.getElementById('detail-charts');
  chartsDiv.innerHTML = '';

  // Duration chart
  const durContainer = document.createElement('div');
  durContainer.className = 'chart-container';
  durContainer.innerHTML = '<h4>P95 Job Duration (ms) by Recipe</h4><canvas id="dur-chart"></canvas>';
  chartsDiv.appendChild(durContainer);

  // Executors chart
  const execContainer = document.createElement('div');
  execContainer.className = 'chart-container';
  execContainer.innerHTML = '<h4>P95 Max Executors by Recipe</h4><canvas id="exec-chart"></canvas>';
  chartsDiv.appendChild(execContainer);

  const recipes = cluster.recipes.filter(r => r.deltas.length > 0);
  const labels = recipes.map(r => r.recipe.replace(/^_/, '').replace(/\.json$/, '').slice(0, 20));

  const durRef = recipes.map(r => {
    const d = r.deltas.find(d => d.metric === 'p95_job_duration_ms');
    return d ? d.reference : 0;
  });
  const durCur = recipes.map(r => {
    const d = r.deltas.find(d => d.metric === 'p95_job_duration_ms');
    return d ? d.current : 0;
  });

  new Chart(document.getElementById('dur-chart'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'Reference', data: durRef, backgroundColor: 'rgba(88,166,255,0.5)', borderColor: '#58a6ff', borderWidth: 1 },
        { label: 'Current', data: durCur, backgroundColor: 'rgba(248,81,73,0.5)', borderColor: '#f85149', borderWidth: 1 }
      ]
    },
    options: chartOpts()
  });

  const execRef = recipes.map(r => {
    const d = r.deltas.find(d => d.metric === 'p95_run_max_executors');
    return d ? d.reference : 0;
  });
  const execCur = recipes.map(r => {
    const d = r.deltas.find(d => d.metric === 'p95_run_max_executors');
    return d ? d.current : 0;
  });

  new Chart(document.getElementById('exec-chart'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'Reference', data: execRef, backgroundColor: 'rgba(88,166,255,0.5)', borderColor: '#58a6ff', borderWidth: 1 },
        { label: 'Current', data: execCur, backgroundColor: 'rgba(248,81,73,0.5)', borderColor: '#f85149', borderWidth: 1 }
      ]
    },
    options: chartOpts()
  });
}

function chartOpts() {
  return {
    responsive: true,
    plugins: {
      legend: { labels: { color: '#8b949e', font: { size: 11 } } }
    },
    scales: {
      x: { ticks: { color: '#8b949e', font: { size: 9 }, maxRotation: 45 }, grid: { color: '#21262d' } },
      y: { ticks: { color: '#8b949e' }, grid: { color: '#21262d' } }
    }
  };
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
    const shortA = c.metric_a.replace('delta_', '').replace(/_/g, ' ');
    const shortB = c.metric_b.replace('delta_', '').replace(/_/g, ' ');
    return `<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
      <div style="width:200px;font-size:11px;color:#8b949e;text-align:right">${shortA}</div>
      <div style="width:20px;text-align:center;color:#8b949e">&harr;</div>
      <div style="width:200px;font-size:11px;color:#8b949e">${shortB}</div>
      <div class="corr-cell" style="background:${color};color:#f0f6fc;min-width:80px" title="cov: ${c.covariance.toFixed(2)}, n: ${c.n}">
        ${c.pearson.toFixed(3)}
      </div>
      <div style="font-size:10px;color:#8b949e">n=${c.n}</div>
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
      <td>${d.cluster}</td>
      <td>${d.recipe}</td>
      <td>${d.metric.replace(/_/g, ' ')}</td>
      <td>${formatNum(d.reference)}</td>
      <td>${formatNum(d.current)}</td>
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

// ── Boot ────────────────────────────────────────────────────────────────────
init();
