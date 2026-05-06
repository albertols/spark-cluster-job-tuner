// Tuning Wizard — Phase 1 (UI-only, no backend).
//
// Walks a user through:
//   1. Mode + date(s)
//   2. SQL preview (projectId-substituted) + drag-drop CSV with header validation
//   3. Tuner parameters (single + auto, full Scallop surface)
//   4. Run instructions: download staged CSVs, IntelliJ run-config XML, bash snippet
//
// Reaches into app.js via existing globals: config, openModal, closeModal,
// closeModalSilently, escapeHtml, parseCsv, discoverAnalyses, navigate.

const Wizard = (() => {
  // ── Constants ─────────────────────────────────────────────────────────────

  const STEPS = [
    { key: "mode",   label: "Mode · Dates" },
    { key: "inputs", label: "Inputs" },
    { key: "params", label: "Parameters" },
    { key: "run",    label: "Run" },
  ];

  const DATE_RE = /^\d{4}_\d{2}_\d{2}$/;
  const SQL_DIR_REL = "../../log_analytics"; // relative to frontend dir

  // The 5 bNN steps the wizard knows about. Order matters for rendering.
  const BNN = [
    {
      key: "b13",
      title: "b13 — Recommendations inputs per recipe per cluster",
      file: "b13_recommendations_inputs_per_recipe_per_cluster.sql",
      csv:  "b13_recommendations_inputs_per_recipe_per_cluster.csv",
      required: true,
      headers: [
        "cluster_name","recipe_filename","avg_executors_per_job","p95_run_max_executors",
        "avg_job_duration_ms","p95_job_duration_ms","runs","seconds_at_cap","runs_reaching_cap",
        "total_runs","fraction_reaching_cap","max_concurrent_jobs",
      ],
      descr:
        "One-stop input for the tuner. Per (cluster, recipe): average and p95 executors, " +
        "average and p95 job duration, total runs, time at executor-cap, and max concurrent jobs. " +
        "Without this CSV the tuner has no metrics and cannot recommend anything.",
    },
    {
      key: "b20",
      title: "b20 — Cluster span time (one row per incarnation)",
      file: "b20_cluster_span_time.sql",
      csv:  "b20_cluster_span_time.csv",
      required: true, // soft-mandatory: needed for cost
      headers: [
        "cluster_name","incarnation_idx","span_start_ts","span_end_ts","span_minutes",
        "create_event_ts","delete_event_ts","has_explicit_create","has_explicit_delete","total_events",
      ],
      descr:
        "One row per cluster incarnation (CreateCluster → DeleteCluster). Used by the cost " +
        "estimator (b22/b23) to compute estimated_cost_eur. Without it, cost is reported as 0.0.",
    },
    {
      key: "b14",
      title: "b14 — Driver exit codes (YARN eviction signals)",
      file: "b14_clusters_with_nonzero_exit_codes.sql",
      csv:  "b14_clusters_with_nonzero_exit_codes.csv",
      required: false,
      headers: [
        "timestamp","job_id","cluster_name","driver_exit_code","msg",
      ],
      descr:
        "Surfaces non-zero driver exit codes (notably 247 = YARN eviction). Auto-tuner uses these " +
        "as b14 vitamins to promote master machines. Optional — if missing, eviction-based promotion is skipped. " +
        "Note: BigQuery export emits cluster_name triple-quoted (\"\"\"name\"\"\"); the tuner strips the quotes.",
    },
    {
      key: "b16",
      title: "b16 — Driver Java heap OOM exceptions",
      file: "b16_oom_job_driver_exceptions.sql",
      csv:  "b16_oom_job_driver_exceptions.csv",
      required: false,
      headers: [
        "job_id","cluster_name","recipe_filename","latest_driver_log_ts","latest_driver_log_severity",
        "latest_driver_log_class","latest_driver_exception_type","is_lost_task","is_stack_overflow",
        "is_java_heap","latest_driver_message","log_name",
      ],
      descr:
        "Jobs whose driver ended with java.lang.OutOfMemoryError. Auto-tuner reads is_java_heap=TRUE " +
        "rows to apply the b16 memory boost (multiplies spark.executor.memory by --b16-rebooting-factor). " +
        "Optional — if missing, OOM boosts are not applied.",
    },
    {
      key: "b21",
      title: "b21 — Cluster autoscaler events (one row per recommendation)",
      file: "b21_cluster_autoscaler_values.sql",
      csv:  "b21_cluster_autoscaler_values.csv",
      required: false,
      headers: [
        "cluster_name","event_ts","state","decision","decision_metric","current_primary_workers",
        "target_primary_workers","min_primary_workers","max_primary_workers","current_secondary_workers",
        "target_secondary_workers","min_secondary_workers","max_secondary_workers","recommendation_id",
        "status_details",
      ],
      descr:
        "Autoscaler RECOMMENDING events: target/min/max workers and decisions over time. " +
        "Used to refine cost estimates with a step-function (b22). Optional — without it, " +
        "the tuner falls back to average-fallback cost (b23) and warns that step-function cost is unavailable.",
    },
  ];

  // Source of truth: `auto/ClusterMachineAndRecipeAutoTuner.scala:24-95` (Scallop AutoTunerConf).
  const AUTO_PARAMS = [
    { name: "strategy", flag: "--strategy", kind: "enum", default: "default",
      values: ["default","cost_biased","performance_biased"],
      descr: "Tuning strategy: default, cost_biased, performance_biased" },
    { name: "topology", flag: "--topology", kind: "enum", default: "8cx1GBpc",
      values: ["8cx1GBpc","8cx2GBpc","8cx4GBpc","4cx1GBpc","4cx2GBpc"],
      descr: "Executor topology preset (cores × memory-per-core)" },
    { name: "keepHistoricalTuning", flag: "--keep-historical-tuning", kind: "bool", default: true,
      descr: "Preserve configs for clusters/recipes absent from current_date metrics (default: true)" },
    { name: "b16ReboostingFactor", flag: "--b16-rebooting-factor", kind: "number", default: 1.5,
      min: 1.0, max: 5.0, step: 0.1,
      descr: "Boost factor for spark.executor.memory on b16 OOM signals (default: 1.5, range [1.0, 5.0])" },
    { name: "b17ReboostingFactor", flag: "--b17-rebooting-factor", kind: "number", default: 1.0,
      min: 1.0, max: 5.0, step: 0.1,
      descr: "Boost factor for spark.executor.memoryOverhead on b17 signals (future, default: 1.0, range [1.0, 5.0])" },
    { name: "divergenceZThreshold", flag: "--divergence-z-threshold", kind: "number", default: 2.0,
      step: 0.1, min: 0.0,
      descr: "Z-score threshold for outlier divergence detection (default: 2.0)" },
    { name: "executorScaleFactor", flag: "--executor-scale-factor", kind: "number", default: 1.5,
      min: 1.0, max: 5.0, step: 0.1,
      descr: "Boost for spark.dynamicAllocation.maxExecutors when a paired recipe is a duration outlier and is cap-touching. Pass 1.0 to disable (range [1.0, 5.0])." },
    { name: "scaleZThreshold", flag: "--scale-z-threshold", kind: "number", default: 3.0,
      step: 0.1, min: 0.0,
      descr: "Min positive z-score on avg/p95 job duration that triggers an executor scale-up (default: 3.0)" },
    { name: "scaleCapTouchRatio", flag: "--scale-cap-touch-ratio", kind: "number", default: 0.5,
      min: 0.01, max: 1.0, step: 0.01,
      descr: "Cap-touching threshold: scale-up only fires when p95_run_max_executors / current maxExecutors >= this (default: 0.5; raise to 0.85 for stricter). Range (0.0, 1.0]." },
  ];

  // For the per-date single tuner. The `flattened=false` arg is positional, not a flag.
  const SINGLE_PARAMS = [
    { name: "strategy", flag: "--strategy", kind: "enum", default: "default",
      values: ["default","cost_biased","performance_biased"],
      descr: "Tuning strategy preset." },
    { name: "topology", flag: "--topology", kind: "enum", default: "8cx1GBpc",
      values: ["8cx1GBpc","8cx2GBpc","8cx4GBpc","4cx1GBpc","4cx2GBpc"],
      descr: "Executor topology override (cores × memory-per-core)." },
    { name: "flattened", flag: null, kind: "bool", default: true,
      descr: "Use the single b13 CSV (true) or read individual b1–b12 CSVs (false)." },
  ];

  // Source of truth: `single/TuningStrategies.scala:203-257`.
  const STRATEGY_TABLE = {
    fields: [
      { key: "biasMode",            label: "biasMode" },
      { key: "executorTopology",    label: "executorTopology" },
      { key: "capHitBoostPct",      label: "capHitBoostPct" },
      { key: "capHitThreshold",     label: "capHitThreshold" },
      { key: "preferMaxWorkers",    label: "preferMaxWorkers" },
      { key: "perWorkerPenaltyPct", label: "perWorkerPenaltyPct" },
      { key: "concurrencyBufferPct",label: "concurrencyBufferPct" },
      { key: "minExecutorInstances",label: "minExecutorInstances" },
    ],
    cols: ["default","cost_biased","performance_biased"],
    values: {
      "default": {
        biasMode: "CostPerformanceBalance",
        executorTopology: "8cx1GBpc (8 GB)",
        capHitBoostPct: "0.20",
        capHitThreshold: "0.30",
        preferMaxWorkers: "6",
        perWorkerPenaltyPct: "0.05",
        concurrencyBufferPct: "0.25",
        minExecutorInstances: "1",
      },
      "cost_biased": {
        biasMode: "CostBiased",
        executorTopology: "8cx1GBpc (8 GB)",
        capHitBoostPct: "0.10",
        capHitThreshold: "0.40",
        preferMaxWorkers: "4",
        perWorkerPenaltyPct: "0.08",
        concurrencyBufferPct: "0.10",
        minExecutorInstances: "1",
      },
      "performance_biased": {
        biasMode: "PerformanceBiased",
        executorTopology: "8cx2GBpc (16 GB)",
        capHitBoostPct: "0.30",
        capHitThreshold: "0.20",
        preferMaxWorkers: "8",
        perWorkerPenaltyPct: "0.02",
        concurrencyBufferPct: "0.40",
        minExecutorInstances: "2",
      },
    },
  };

  const SESSION_KEY = "wizardState_v1";
  const SUBSTITUTE_RE = /\b[a-z][a-z0-9-]*(\.global\._Default\._Default)\b/g;

  // ── State ────────────────────────────────────────────────────────────────

  // `staged` holds File objects per date per bNN. NOT persisted (browser security).
  // sessionStorage gets metadata only (filename, header, rows).
  let state = null;

  function freshState(mode) {
    const today = todayYmd();
    return {
      mode,                              // "single" | "auto"
      step: 0,
      // ref is used for both modes (single uses ref only; auto stages cur).
      // Default the field that the user is going to type into to today; the
      // other stays blank (auto-mode ref is picked from a dropdown).
      dates: {
        ref: mode === "single" ? today : "",
        cur: mode === "auto"   ? today : "",
      },
      flattened: true,                    // single only
      projectId: (typeof config !== "undefined" && config && config.gcpProjectId) || "",
      params: paramDefaults(mode),
      // staged: { "<date>": { "b13": { name, headers, rows, valid }, ... } }
      stagedMeta: {},
      stagedFiles: {},                   // not persisted
      // For Auto mode: which date is currently active in the per-date tabs.
      activeUploadDate: null,
    };
  }

  function todayYmd() {
    const d = new Date();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${d.getFullYear()}_${m}_${day}`;
  }

  function paramDefaults(mode) {
    const out = {};
    const params = mode === "auto" ? AUTO_PARAMS : SINGLE_PARAMS;
    params.forEach(p => { out[p.name] = p.default; });
    return out;
  }

  function persistSession() {
    try {
      const persistable = {
        mode: state.mode,
        step: state.step,
        dates: state.dates,
        flattened: state.flattened,
        projectId: state.projectId,
        params: state.params,
        stagedMeta: state.stagedMeta,    // metadata only
        activeUploadDate: state.activeUploadDate,
      };
      sessionStorage.setItem(SESSION_KEY, JSON.stringify(persistable));
    } catch (_) { /* sessionStorage full / disabled */ }
  }

  function loadSession() {
    try {
      const raw = sessionStorage.getItem(SESSION_KEY);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (_) { return null; }
  }

  function clearSession() {
    try { sessionStorage.removeItem(SESSION_KEY); } catch (_) { /* ignore */ }
  }

  // ── Public entry ──────────────────────────────────────────────────────────

  function start(opts) {
    const mode = (opts && opts.mode) || "single";

    // Resume? Same-mode session counts; we don't auto-resume across modes.
    const saved = loadSession();
    if (saved && saved.mode === mode && Number.isInteger(saved.step) && saved.step > 0) {
      state = freshState(mode);
      Object.assign(state, saved);
      state.stagedFiles = {}; // cleared on resume — user must re-drop
      openWizard(/* resuming */ true);
      return;
    }

    state = freshState(mode);
    openWizard(false);
  }

  function openWizard(resuming) {
    const overlay = document.getElementById("modal-overlay");
    overlay.classList.add("wiz-modal");
    const title = `${state.mode === "auto" ? "+ New auto" : "+ New single"} tuning`;
    openModal(escapeHtml(title), "");
    render(resuming);
    // Strip the wiz-modal class whenever the overlay closes — covers the
    // existing × close button, Escape-key handler in app.js, and our own
    // Cancel/Done paths.
    const obs = new MutationObserver(() => {
      if (overlay.style.display === "none") {
        overlay.classList.remove("wiz-modal");
        obs.disconnect();
      }
    });
    obs.observe(overlay, { attributes: true, attributeFilter: ["style"] });
  }

  function cleanup() {
    const overlay = document.getElementById("modal-overlay");
    if (overlay) overlay.classList.remove("wiz-modal");
  }

  // ── Step orchestration ────────────────────────────────────────────────────

  function render(resuming) {
    const body = document.getElementById("modal-body");
    const stepKey = STEPS[state.step].key;
    body.innerHTML =
      `<div class="wiz-root">` +
        renderBreadcrumb() +
        (resuming ? renderResumeBar() : "") +
        `<div class="wiz-step" id="wiz-step-content"></div>` +
        renderFooter() +
      `</div>`;

    const content = document.getElementById("wiz-step-content");
    if (stepKey === "mode")    renderModeStep(content);
    if (stepKey === "inputs")  renderInputsStep(content);
    if (stepKey === "params")  renderParamsStep(content);
    if (stepKey === "run")     renderRunStep(content);

    wireFooter();
    if (resuming) wireResumeBar();
    persistSession();
  }

  function renderBreadcrumb() {
    return `<div class="wiz-breadcrumb">` + STEPS.map((s, i) => {
      const cls = i === state.step ? "is-active" : (i < state.step ? "is-done" : "");
      const sep = i < STEPS.length - 1 ? `<span class="crumb-sep">›</span>` : "";
      return `<div class="crumb ${cls}" data-step="${i}">` +
        `<span class="crumb-num">${i + 1}</span><span>${escapeHtml(s.label)}</span></div>${sep}`;
    }).join("") + `</div>`;
  }

  function renderResumeBar() {
    return `<div class="wiz-resume-bar" id="wiz-resume-bar">` +
      `<div>Resumed an in-progress wizard. Staged CSVs were dropped from this browser session and need to be re-dropped.</div>` +
      `<div class="actions"><button id="wiz-resume-discard">Start fresh</button></div>` +
    `</div>`;
  }

  function wireResumeBar() {
    const btn = document.getElementById("wiz-resume-discard");
    if (btn) btn.onclick = () => {
      clearSession();
      state = freshState(state.mode);
      render(false);
    };
  }

  function renderFooter() {
    const last = state.step === STEPS.length - 1;
    return `<div class="wiz-footer">` +
      `<div class="left">` +
        `<button class="wiz-btn ghost danger" id="wiz-cancel">Cancel</button>` +
      `</div>` +
      `<div class="right">` +
        (state.step > 0 ? `<button class="wiz-btn" id="wiz-back">← Back</button>` : "") +
        (last ? `<button class="wiz-btn primary" id="wiz-finish">Done</button>`
              : `<button class="wiz-btn primary" id="wiz-next" disabled>Next →</button>`) +
      `</div>` +
    `</div>`;
  }

  function wireFooter() {
    const cancel = document.getElementById("wiz-cancel");
    if (cancel) cancel.onclick = () => {
      const hasStaged = Object.keys(state.stagedFiles).some(d => Object.keys(state.stagedFiles[d] || {}).length > 0);
      if (hasStaged && !window.confirm("Discard staged CSVs and exit the wizard?")) return;
      clearSession();
      cleanup();
      closeModalSilently();
    };
    const back = document.getElementById("wiz-back");
    if (back) back.onclick = () => { state.step--; render(false); };
    const next = document.getElementById("wiz-next");
    if (next) next.onclick = async () => {
      if (!validateCurrentStep()) return;
      const stepKey = STEPS[state.step].key;
      // On the Inputs → Params transition, auto-upload staged CSVs to the
      // server (when API is up) so the user never has to mkdir / mv in a
      // terminal. If the API is down, we silently advance — Step 4's bash
      // snippet stays as the static-mode fallback.
      if (stepKey === "inputs") {
        const advanced = await tryAutoProvisionAndAdvance();
        if (!advanced) return; // user clicked Cancel in the progress dialog
        return;
      }
      state.step++; render(false);
    };
    const finish = document.getElementById("wiz-finish");
    if (finish) finish.onclick = () => {
      clearSession();
      cleanup();
      closeModalSilently();
      // After a wizard exit, refresh the analysis discovery so newly tuned dates show up.
      if (typeof discoverAnalyses === "function") {
        discoverAnalyses().then(entries => { discoveredEntries = entries; });
      }
    };
    refreshNextEnabled();
  }

  function refreshNextEnabled() {
    const next = document.getElementById("wiz-next");
    if (!next) return;
    next.disabled = !canAdvance();
  }

  // ── Validation ────────────────────────────────────────────────────────────

  function canAdvance() {
    const stepKey = STEPS[state.step].key;
    if (stepKey === "mode") {
      if (state.mode === "single") return DATE_RE.test(state.dates.ref);
      return DATE_RE.test(state.dates.ref) && DATE_RE.test(state.dates.cur)
        && state.dates.cur >= state.dates.ref;
    }
    if (stepKey === "inputs") {
      // In Auto mode, the reference date is picked from on-disk dirs (Step 1
      // dropdown), so its CSVs are assumed already present. Only require the
      // user to stage CSVs for the date being added.
      const stagingDate = state.mode === "auto" ? state.dates.cur : state.dates.ref;
      const meta = state.stagedMeta[stagingDate] || {};
      for (const b of BNN) {
        if (!b.required) continue;
        if (!meta[b.key] || !meta[b.key].valid) return false;
      }
      return true;
    }
    if (stepKey === "params") {
      const params = state.mode === "auto" ? AUTO_PARAMS : SINGLE_PARAMS;
      return params.every(p => paramValid(p, state.params[p.name]));
    }
    return true;
  }

  function validateCurrentStep() { return canAdvance(); }

  function paramValid(p, v) {
    if (p.kind === "enum") return p.values.includes(v);
    if (p.kind === "bool") return typeof v === "boolean";
    if (p.kind === "number") {
      const n = typeof v === "number" ? v : parseFloat(v);
      if (!Number.isFinite(n)) return false;
      if (typeof p.min === "number" && n < p.min) return false;
      if (typeof p.max === "number" && n > p.max) return false;
      return true;
    }
    return true;
  }

  // ── Step 1: Mode + dates ──────────────────────────────────────────────────

  function renderModeStep(root) {
    const m = state.mode;
    root.innerHTML =
      `<h3>Choose mode and date${m === "auto" ? "s" : ""}</h3>` +
      `<div class="wiz-mode-grid">` +
        `<div class="wiz-mode-card ${m === "single" ? "selected" : ""}" data-mode="single">` +
          `<h4>Single tuning <span class="wiz-mut">(one date)</span></h4>` +
          `<p class="desc">Runs <code>ClusterMachineAndRecipeTuner</code> on a single date's metrics. ` +
          `Produces per-cluster JSON configs and the <code>_clusters-*.csv</code> summaries.</p>` +
        `</div>` +
        `<div class="wiz-mode-card ${m === "auto" ? "selected" : ""}" data-mode="auto">` +
          `<h4>Auto tuning <span class="wiz-mut">(reference ↔ current)</span></h4>` +
          `<p class="desc">Runs <code>ClusterMachineAndRecipeAutoTuner</code> on a current date and compares it ` +
          `to a previously tuned reference date. Detects trends, divergences, and re-boosts.</p>` +
        `</div>` +
      `</div>` +
      (m === "auto" ? renderAutoDates() : renderSingleDates());

    // Mode swap (rebuild state if user switches mode mid-flow).
    root.querySelectorAll(".wiz-mode-card").forEach(card => {
      card.onclick = () => {
        const newMode = card.dataset.mode;
        if (newMode === state.mode) return;
        state = freshState(newMode);
        render(false);
      };
    });

    // Single-mode bindings.
    const refIn = root.querySelector("#wiz-ref-date");
    const flatCb = root.querySelector("#wiz-flattened");
    const curIn = root.querySelector("#wiz-cur-date");
    const refSel = root.querySelector("#wiz-ref-select");

    if (refIn) refIn.oninput = () => {
      state.dates.ref = refIn.value.trim();
      refreshDateValidity();
      refreshNextEnabled();
    };
    if (flatCb) flatCb.onchange = () => { state.flattened = flatCb.checked; persistSession(); };
    if (refSel) refSel.onchange = () => {
      state.dates.ref = refSel.value;
      refreshNextEnabled();
    };
    if (curIn) curIn.oninput = () => {
      state.dates.cur = curIn.value.trim();
      refreshDateValidity();
      refreshNextEnabled();
    };

    // Populate auto reference dropdown from inputs tree (HTTP listing).
    if (m === "auto") populateRefDates(refSel);
    refreshDateValidity();
  }

  function renderSingleDates() {
    const refVal = escapeHtml(state.dates.ref);
    const inputsHint = state.dates.ref && DATE_RE.test(state.dates.ref)
      ? `<small class="hint">Will create: <code>${escapeHtml((typeof config !== "undefined" && config && config.inputsPath) || "")}/${refVal}/</code></small>`
      : `<small class="hint">Format: YYYY_MM_DD (e.g. <code>2026_05_05</code>)</small>`;
    return `<div style="margin-top:18px;">` +
      `<div class="wiz-form-row">` +
        `<label>Date <span class="wiz-required">*</span></label>` +
        `<div>` +
          `<input id="wiz-ref-date" type="text" value="${refVal}" placeholder="2026_05_05" pattern="\\d{4}_\\d{2}_\\d{2}">` +
          `<div id="wiz-ref-err" class="err" style="display:none;">Date must match YYYY_MM_DD</div>` +
          inputsHint +
        `</div>` +
      `</div>` +
      `<div class="wiz-form-row">` +
        `<label>Flattened input</label>` +
        `<div>` +
          `<label style="display:inline-flex;align-items:center;gap:6px;color:#c9d1d9;">` +
            `<input type="checkbox" id="wiz-flattened" ${state.flattened ? "checked" : ""}> ` +
            `<span>Use single <code>b13.csv</code> (recommended).</span>` +
          `</label>` +
          `<div class="hint">Uncheck only if you have individual b1–b12 CSVs from the legacy multi-file flow.</div>` +
        `</div>` +
      `</div>` +
    `</div>`;
  }

  function renderAutoDates() {
    return `<div style="margin-top:18px;">` +
      `<div class="wiz-form-row">` +
        `<label>Reference date <span class="wiz-required">*</span></label>` +
        `<div>` +
          `<select id="wiz-ref-select"><option value="">Loading available dates…</option></select>` +
          `<div class="hint">Pick a date that already has CSV inputs on disk. The reference is used as the baseline for trend detection.</div>` +
        `</div>` +
      `</div>` +
      `<div class="wiz-form-row">` +
        `<label>Current date <span class="wiz-required">*</span></label>` +
        `<div>` +
          `<input id="wiz-cur-date" type="text" value="${escapeHtml(state.dates.cur)}" placeholder="2026_05_29" pattern="\\d{4}_\\d{2}_\\d{2}">` +
          `<div id="wiz-cur-err" class="err" style="display:none;"></div>` +
          `<div class="hint">A new date you'll stage CSVs for in the next step. Must be ≥ reference date.</div>` +
        `</div>` +
      `</div>` +
    `</div>`;
  }

  async function populateRefDates(selectEl) {
    if (!selectEl) return;
    const cfg = (typeof config !== "undefined") ? config : null;
    const inputsRoot = (cfg && cfg.inputsPath)
      ? stripTrailingSlash(new URL(cfg.inputsPath + "/", window.location.href).href)
      : null;
    if (!inputsRoot) {
      selectEl.innerHTML = `<option value="">No inputsPath configured in config.json</option>`;
      return;
    }
    let dirs = [];
    try {
      const r = await fetch(`${inputsRoot}/`);
      if (r.ok) {
        const html = await r.text();
        dirs = parseDirsFromListing(html);
      }
    } catch (_) { /* ignore */ }
    dirs = dirs.filter(d => DATE_RE.test(d)).sort();
    if (!dirs.length) {
      selectEl.innerHTML = `<option value="">No date dirs found under inputsPath</option>`;
      return;
    }
    const sel = state.dates.ref;
    selectEl.innerHTML =
      `<option value="">— pick a reference date —</option>` +
      dirs.map(d => `<option value="${escapeHtml(d)}" ${d === sel ? "selected" : ""}>${escapeHtml(d)}</option>`).join("");
    if (sel && !dirs.includes(sel)) selectEl.value = "";
  }

  // Mirror of app.js parseDirListing, scoped here so we don't depend on it being exported.
  function parseDirsFromListing(html) {
    const out = [];
    const re = /<a href="([^"]+)"/gi;
    let m;
    while ((m = re.exec(html)) !== null) {
      let href = m[1];
      if (!href.endsWith("/")) continue;
      if (href.startsWith("?") || href.startsWith("#") || href === "../" || href === "./") continue;
      href = decodeURIComponent(href.replace(/\/$/, ""));
      if (href.startsWith("http://") || href.startsWith("https://")) continue;
      if (href.startsWith("_") || href.startsWith(".")) continue;
      out.push(href);
    }
    return out;
  }

  function stripTrailingSlash(s) { return s && s.endsWith("/") ? s.slice(0, -1) : s; }

  function refreshDateValidity() {
    const refIn = document.getElementById("wiz-ref-date");
    const refErr = document.getElementById("wiz-ref-err");
    if (refIn) {
      const ok = !refIn.value || DATE_RE.test(refIn.value);
      refIn.classList.toggle("is-invalid", !ok);
      if (refErr) refErr.style.display = ok ? "none" : "";
    }
    const curIn = document.getElementById("wiz-cur-date");
    const curErr = document.getElementById("wiz-cur-err");
    if (curIn) {
      let msg = "";
      const v = curIn.value;
      if (v && !DATE_RE.test(v)) msg = "Date must match YYYY_MM_DD";
      else if (v && state.dates.ref && DATE_RE.test(state.dates.ref) && v < state.dates.ref) {
        msg = "Current date must be on or after the reference date.";
      }
      curIn.classList.toggle("is-invalid", !!msg);
      if (curErr) {
        curErr.textContent = msg;
        curErr.style.display = msg ? "" : "none";
      }
    }
  }

  // ── Step 2 → Step 3 transition: provision the input dir + upload CSVs ─────

  /**
   * Returns true if the wizard advanced to Step 3 (or the user cancelled).
   * Returns false if the user is still on Step 2 (e.g. retry-able failure).
   */
  async function tryAutoProvisionAndAdvance() {
    const apiUp = (typeof TunerApi !== "undefined") ? await TunerApi.health() : false;
    if (!apiUp) {
      // Static-mode: nothing to upload server-side. Step 4 keeps the bash
      // snippet for the user to land files manually.
      state.step++; render(false);
      return true;
    }

    const date = state.mode === "auto" ? state.dates.cur : state.dates.ref;
    const files = state.stagedFiles[date] || {};
    const stagedKeys = BNN.filter(b => files[b.key]).map(b => b.key);
    // No staged CSVs (e.g. user resumed and only has metadata) — nothing to
    // provision; jump straight to params and let them iterate.
    if (stagedKeys.length === 0) {
      state.step++; render(false);
      return true;
    }

    // 1) Persist projectId override if it differs from the loaded config.
    //    This is best-effort — non-fatal on failure (we surface the error
    //    but still let the user proceed with uploads).
    const cfgProj = (typeof config !== "undefined" && config && config.gcpProjectId) || "";
    const projChanged = state.projectId && state.projectId !== cfgProj;

    // Build the operation list shown to the user.
    const ops = [];
    if (projChanged) {
      ops.push({ id: "config", label: `Persist projectId override → config.local.json`, fn: () =>
        TunerApi.persistConfig({ gcpProjectId: state.projectId })
      });
    }
    ops.push({ id: "mkdir", label: `Create directory  ${escapeHtml(((typeof config !== "undefined" && config && config.inputsPath) || "") + "/" + date + "/")}`, fn: () =>
      TunerApi.mkdirInput(date)
    });
    stagedKeys.forEach(k => {
      const b = BNN.find(x => x.key === k);
      const f = files[k];
      ops.push({
        id: "csv-" + k,
        label: `Upload  <code>${escapeHtml(b.csv)}</code>  <span class="wiz-mut">(${formatBytes(f.size)})</span>`,
        fn: () => TunerApi.uploadCsv(date, b.csv, f),
      });
    });

    return await runProvisioningOps(ops);
  }

  /**
   * Replaces the step content with a progress UI and runs the ops one at a
   * time. On all-green: advances state.step++ and returns true. On error:
   * keeps the user on the progress UI with Retry / Back-to-inputs / Continue
   * anyway buttons.
   */
  async function runProvisioningOps(ops) {
    const root = document.getElementById("wiz-step-content");
    if (!root) return false;
    const states = ops.map(() => "pending"); // pending | running | done | failed
    const errors = ops.map(() => null);

    const draw = () => {
      const rows = ops.map((op, i) => renderProvRow(op, states[i], errors[i], i)).join("");
      root.innerHTML =
        `<h3>Provisioning inputs <span class="wiz-mut">(API mode)</span></h3>` +
        `<p class="help">The wizard is creating the input directory and uploading your staged CSVs to the TunerService. ` +
        `If anything fails, you'll see the exact error and a Retry button.</p>` +
        `<div class="wiz-prov-list">${rows}</div>` +
        `<div style="display:flex;gap:8px;margin-top:14px;flex-wrap:wrap;">` +
          `<button class="wiz-btn" id="wiz-prov-back">← Back to Inputs</button>` +
          `<button class="wiz-btn ghost" id="wiz-prov-skip" title="Skip uploads and continue with the static-mode flow">Skip and continue (static fallback)</button>` +
        `</div>`;
      const back = root.querySelector("#wiz-prov-back");
      const skip = root.querySelector("#wiz-prov-skip");
      if (back) back.onclick = () => { state.step = STEPS.findIndex(s => s.key === "inputs"); render(false); };
      if (skip) skip.onclick = () => { state.step++; render(false); };
      // Per-row retry handlers
      root.querySelectorAll("[data-retry]").forEach(btn => {
        btn.onclick = () => {
          const i = parseInt(btn.dataset.retry, 10);
          retryFrom(i);
        };
      });
      // Refresh footer Next state — disable Next until we successfully advance.
      refreshNextEnabled();
    };

    const retryFrom = async (i) => {
      // Reset states from i onwards.
      for (let j = i; j < ops.length; j++) { states[j] = "pending"; errors[j] = null; }
      draw();
      await runFrom(i);
    };

    const runFrom = async (start) => {
      for (let i = start; i < ops.length; i++) {
        states[i] = "running";
        draw();
        try {
          await ops[i].fn();
          states[i] = "done";
          draw();
        } catch (e) {
          states[i] = "failed";
          errors[i] = humanizeApiError(e, ops[i]);
          draw();
          return false;
        }
      }
      // All ops succeeded — record so Step 4 skips redundant uploads.
      const date = state.mode === "auto" ? state.dates.cur : state.dates.ref;
      if (!state.provisionedDates) state.provisionedDates = {};
      state.provisionedDates[date] = true;
      persistSession();
      state.step++;
      render(false);
      return true;
    };

    draw();
    return await runFrom(0);
  }

  function renderProvRow(op, status, err, idx) {
    const icon =
      status === "done"    ? `<span class="wiz-ok">✓</span>` :
      status === "running" ? `<span class="wiz-prov-spin">⟳</span>` :
      status === "failed"  ? `<span class="err-chip">✗</span>` :
                             `<span class="wiz-mut">·</span>`;
    const errBlock = (status === "failed" && err) ? renderProvError(err, idx) : "";
    return `<div class="wiz-prov-row ${status}">` +
      `<div class="wiz-prov-icon">${icon}</div>` +
      `<div class="wiz-prov-body">` +
        `<div class="wiz-prov-label">${op.label}</div>` +
        errBlock +
      `</div>` +
    `</div>`;
  }

  function renderProvError(err, idx) {
    return `<div class="wiz-banner err" style="margin-top:6px;">` +
      `<div><strong>${escapeHtml(err.title)}</strong></div>` +
      `<div style="margin-top:2px;">${escapeHtml(err.message)}</div>` +
      (err.suggestions && err.suggestions.length
        ? `<ul style="margin:6px 0 0 18px;font-size:11px;">` +
            err.suggestions.map(s => `<li>${escapeHtml(s)}</li>`).join("") +
          `</ul>`
        : "") +
      `<div style="margin-top:8px;display:flex;gap:6px;">` +
        `<button class="wiz-btn" data-retry="${idx}">Retry</button>` +
      `</div>` +
    `</div>`;
  }

  function humanizeApiError(e, op) {
    const status = e && e.status;
    const body   = e && e.body;
    const msg    = (body && body.error) || (e && e.message) || String(e);

    // Shape-match common failure modes.
    if (status === 409 && body && body.error === "run_in_progress") {
      return {
        title: "Another tuning run is already in flight.",
        message: `The TunerService accepts only one run at a time. Active run: ${body.currentRunId}.`,
        suggestions: [
          "Wait for it to finish (watch /api/runs/<id>/log).",
          "Or cancel it via DELETE /api/runs/<id> and retry.",
        ],
      };
    }
    if (status === 400) {
      return {
        title: "Server rejected the request (400 Bad Request).",
        message: msg,
        suggestions: ["Check the date format (YYYY_MM_DD) and the CSV filename in Step 2."],
      };
    }
    if (status === 413) {
      return {
        title: "File too large.",
        message: msg,
        suggestions: ["The TunerService caps uploads at 1 GB. Filter the CSV in BigQuery and re-export."],
      };
    }
    if (status === 500) {
      return {
        title: "Server error (500).",
        message: msg,
        suggestions: [
          "Check the serve.sh --api console for a stack trace.",
          "Likely causes: filesystem permissions on the inputs path, or a typo in config.json.",
        ],
      };
    }
    // Likely network: server crashed, port closed, CORS, etc.
    if (!status) {
      return {
        title: "Could not reach the TunerService.",
        message: msg,
        suggestions: [
          "Confirm `./serve.sh --api` is still running in your terminal.",
          "Try opening /api/health in another tab — it should return {\"ok\":true}.",
          "If the server crashed, restart it and click Retry.",
        ],
      };
    }
    return {
      title: `Operation failed (${op.id}).`,
      message: msg,
      suggestions: [
        "Check the serve.sh --api console for details.",
        "Verify the inputs directory is writable by the JVM user.",
      ],
    };
  }

  // ── Step 2: Inputs ────────────────────────────────────────────────────────

  function renderInputsStep(root) {
    // Auto mode: reference is on-disk (chosen from dropdown in Step 1); we only
    // stage CSVs for the current date. Single mode: stage CSVs for the date.
    state.activeUploadDate = state.mode === "auto" ? state.dates.cur : state.dates.ref;
    const dateLabel = state.mode === "auto"
      ? `<div class="wiz-banner info">Staging CSVs for current date <code>${escapeHtml(state.dates.cur)}</code>. Reference <code>${escapeHtml(state.dates.ref)}</code> is read from disk.</div>`
      : "";
    root.innerHTML =
      `<h3>Stage SQL outputs as CSVs</h3>` +
      `<p class="help">Run each SQL in BigQuery Log Analytics, export the result as CSV, then drop it on the matching card. ` +
      `Headers are validated against the columns the Scala tuner reads.</p>` +
      renderProjectIdPanel() +
      dateLabel +
      `<div id="wiz-bnn-list">` +
        BNN.map(b => renderBnnCard(b, state.activeUploadDate)).join("") +
      `</div>`;

    wireProjectIdPanel(root);
    BNN.forEach(b => wireBnnCard(root, b, state.activeUploadDate));
    refreshNextEnabled();
  }

  function renderProjectIdPanel() {
    const cur = state.projectId || "";
    return `<div class="wiz-projid">` +
      `<label>GCP Project ID</label>` +
      `<input id="wiz-projid" type="text" value="${escapeHtml(cur)}" placeholder="your-gcp-project-id">` +
      `<button class="wiz-btn" id="wiz-projid-save">Save…</button>` +
      `<div class="hint">SQL queries below substitute this project everywhere a fully-qualified ` +
        `<code>&lt;projectId&gt;.global._Default._Default</code> appears. ` +
        `Save produces a downloadable <code>config.local.json</code> (git-ignored) you place next to <code>config.json</code>.</div>` +
    `</div>`;
  }

  function wireProjectIdPanel(root) {
    const input = root.querySelector("#wiz-projid");
    const save  = root.querySelector("#wiz-projid-save");
    if (input) input.oninput = () => {
      state.projectId = input.value.trim();
      // Re-render visible SQL panes to reflect substitution.
      root.querySelectorAll("[data-sql-key]").forEach(pane => {
        const k = pane.dataset.sqlKey;
        const raw = pane.dataset.rawSql || "";
        if (raw) pane.querySelector("pre").textContent = substituteSql(raw, state.projectId);
      });
      persistSession();
    };
    if (save) save.onclick = () => downloadConfigLocal();
  }

  function downloadConfigLocal() {
    const payload = JSON.stringify({ gcpProjectId: state.projectId || "" }, null, 2) + "\n";
    triggerDownload("config.local.json", payload, "application/json");
    alert(
      "Downloaded config.local.json.\n\n" +
      "Place it next to config.json (in the frontend dir) and reload the dashboard. " +
      "It is git-ignored, so it won't be committed."
    );
  }

  function renderBnnCard(b, dateForUpload) {
    const meta = (state.stagedMeta[dateForUpload] || {})[b.key] || null;
    const isOpen = !meta || !meta.valid; // collapse only when ✓
    return `<div class="wiz-bnn-card ${isOpen ? "is-open" : ""}" data-bnn="${b.key}">` +
      `<div class="wiz-bnn-head">` +
        `<div>` +
          `<span class="wiz-bnn-disclosure">▶</span>` +
          `<span class="title"> ${escapeHtml(b.title)}</span>` +
          `<div class="desc">${escapeHtml(b.descr)}</div>` +
        `</div>` +
        `<span class="req-badge ${b.required ? "req" : "opt"}">${b.required ? "REQUIRED" : "OPTIONAL"}</span>` +
        `<span class="status">${renderBnnStatus(meta)}</span>` +
      `</div>` +
      `<div class="wiz-bnn-body">` +
        `<div class="wiz-sql-pane" data-sql-key="${b.key}" data-raw-sql="">` +
          `<div class="wiz-sql-actions">` +
            `<button class="wiz-btn" data-action="copy-sql">Copy</button>` +
          `</div>` +
          `<pre>Loading <code>${escapeHtml(b.file)}</code>…</pre>` +
        `</div>` +
        `<div class="wiz-drop" data-bnn-drop="${b.key}">` +
          `<div class="state">` +
            `Drop <code>${escapeHtml(b.csv)}</code> here, or ` +
            `<label><input type="file" accept=".csv,text/csv" data-file-input="${b.key}">browse…</label>` +
          `</div>` +
          `<div class="hint">Expected columns (${b.headers.length}): ${escapeHtml(b.headers.join(", "))}</div>` +
        `</div>` +
      `</div>` +
    `</div>`;
  }

  function renderBnnStatus(meta) {
    if (!meta) return "";
    if (meta.valid) {
      const rows = (typeof meta.rows === "number") ? `, ${formatNumber(meta.rows)} rows` : "";
      return `<span class="wiz-ok">✓ ${escapeHtml(meta.name)} (${meta.headerCount}/${meta.headerCount} cols${rows})</span>`;
    }
    return `<span class="wiz-warn">⚠ ${escapeHtml(meta.name || "invalid")}</span>`;
  }

  function formatNumber(n) {
    return n.toLocaleString("en-US");
  }

  function wireBnnCard(root, b, dateForUpload) {
    const card = root.querySelector(`.wiz-bnn-card[data-bnn="${b.key}"]`);
    if (!card) return;
    const head = card.querySelector(".wiz-bnn-head");
    const sqlPane = card.querySelector(`[data-sql-key="${b.key}"]`);
    const copyBtn = card.querySelector(`[data-action="copy-sql"]`);
    const drop = card.querySelector(`[data-bnn-drop="${b.key}"]`);
    const fileIn = card.querySelector(`[data-file-input="${b.key}"]`);

    head.onclick = (ev) => {
      // Don't toggle on clicks inside the body (drag-drop zone, copy button).
      if (ev.target.closest(".wiz-bnn-body")) return;
      card.classList.toggle("is-open");
      if (card.classList.contains("is-open") && !sqlPane.dataset.rawSql) {
        loadSql(b, sqlPane);
      }
    };
    if (card.classList.contains("is-open")) loadSql(b, sqlPane);

    if (copyBtn) copyBtn.onclick = (ev) => {
      ev.stopPropagation();
      const txt = sqlPane.querySelector("pre").textContent;
      copyText(txt, copyBtn);
    };

    if (drop) {
      drop.addEventListener("dragover", (e) => {
        e.preventDefault();
        e.stopPropagation();
        drop.classList.add("is-active");
      });
      drop.addEventListener("dragleave", (e) => {
        e.preventDefault();
        drop.classList.remove("is-active");
      });
      drop.addEventListener("drop", (e) => {
        e.preventDefault();
        drop.classList.remove("is-active");
        const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
        if (f) handleFile(b, dateForUpload, f, drop, card);
      });
    }
    if (fileIn) fileIn.onchange = (e) => {
      const f = e.target.files && e.target.files[0];
      if (f) handleFile(b, dateForUpload, f, drop, card);
    };
  }

  async function loadSql(b, paneEl) {
    const url = new URL(`${SQL_DIR_REL}/${b.file}`, window.location.href).href;
    try {
      const r = await fetch(url, { cache: "no-store" });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const raw = await r.text();
      paneEl.dataset.rawSql = raw;
      paneEl.querySelector("pre").textContent = substituteSql(raw, state.projectId);
    } catch (e) {
      paneEl.querySelector("pre").innerHTML =
        `<span class="wiz-warn">Could not load ${escapeHtml(b.file)} from <code>${escapeHtml(SQL_DIR_REL)}/</code>.</span>\n` +
        `<span class="wiz-mut">Error: ${escapeHtml(e.message || String(e))}</span>`;
    }
  }

  function substituteSql(raw, projectId) {
    if (!projectId) return raw;
    return raw.replace(SUBSTITUTE_RE, `${projectId}$1`);
  }

  async function handleFile(b, date, file, dropEl, cardEl) {
    if (!date) {
      // Should never happen — Step 1 enforces.
      alert("No active date set; pick a mode and date in Step 1 first.");
      return;
    }
    dropEl.classList.remove("is-valid", "is-invalid");
    dropEl.classList.add("is-active");

    let header = [];
    try {
      const headerText = await file.slice(0, 65536).text();
      const nl = headerText.indexOf("\n");
      const firstLine = nl >= 0 ? headerText.slice(0, nl) : headerText;
      const parsed = parseCsv(firstLine + "\n"); // existing app.js helper
      header = (parsed.headers || []).map(h => stripBom(h).trim());
    } catch (e) {
      stageInvalid(b, date, file.name, dropEl, cardEl, `Could not read file: ${e.message || e}`);
      return;
    }

    const expected = b.headers;
    const stripQuoted = (s) => s.replace(/^"+|"+$/g, "");
    const headerSet = new Set(header.map(stripQuoted));
    const missing = expected.filter(h => !headerSet.has(h));
    const extra   = header.map(stripQuoted).filter(h => !expected.includes(h));

    if (missing.length) {
      const msg = `Missing column(s): ${missing.join(", ")}` +
        (extra.length ? `. Extra: ${extra.join(", ")}.` : ".");
      stageInvalid(b, date, file.name, dropEl, cardEl, msg);
      return;
    }

    const rows = await streamCountRows(file).catch(() => null);

    if (!state.stagedMeta[date]) state.stagedMeta[date] = {};
    if (!state.stagedFiles[date]) state.stagedFiles[date] = {};
    state.stagedMeta[date][b.key] = {
      name: file.name,
      valid: true,
      headerCount: expected.length,
      rows: rows == null ? null : Math.max(0, rows),
    };
    state.stagedFiles[date][b.key] = file;
    persistSession();

    // Re-render the card head + collapse, and update the bnn list status badge in place.
    const head = cardEl.querySelector(".wiz-bnn-head .status");
    if (head) head.innerHTML = renderBnnStatus(state.stagedMeta[date][b.key]);
    dropEl.classList.remove("is-active");
    dropEl.classList.add("is-valid");
    const stateEl = dropEl.querySelector(".state");
    if (stateEl) {
      stateEl.innerHTML = `<span class="ok-chip">✓ ${escapeHtml(file.name)} accepted</span>` +
        ` <button class="reset" data-action="reset-${b.key}">Replace</button>`;
      const reset = stateEl.querySelector(`[data-action="reset-${b.key}"]`);
      if (reset) reset.onclick = () => resetBnn(b, date, dropEl, cardEl);
    }
    refreshNextEnabled();
  }

  function stripBom(s) {
    return s && s.charCodeAt(0) === 0xFEFF ? s.slice(1) : s;
  }

  function stageInvalid(b, date, filename, dropEl, cardEl, msg) {
    if (!state.stagedMeta[date]) state.stagedMeta[date] = {};
    state.stagedMeta[date][b.key] = { name: filename, valid: false, headerCount: 0, rows: null };
    if (state.stagedFiles[date]) delete state.stagedFiles[date][b.key];
    persistSession();
    dropEl.classList.remove("is-active", "is-valid");
    dropEl.classList.add("is-invalid");
    const stateEl = dropEl.querySelector(".state");
    if (stateEl) {
      stateEl.innerHTML = `<span class="err-chip">✗ ${escapeHtml(filename)}</span> <span class="hint">${escapeHtml(msg)}</span>` +
        ` <button class="reset" data-action="reset-${b.key}">Try another file</button>`;
      const reset = stateEl.querySelector(`[data-action="reset-${b.key}"]`);
      if (reset) reset.onclick = () => resetBnn(b, date, dropEl, cardEl);
    }
    const head = cardEl.querySelector(".wiz-bnn-head .status");
    if (head) head.innerHTML = renderBnnStatus(state.stagedMeta[date][b.key]);
    refreshNextEnabled();
  }

  function resetBnn(b, date, dropEl, cardEl) {
    if (state.stagedMeta[date]) delete state.stagedMeta[date][b.key];
    if (state.stagedFiles[date]) delete state.stagedFiles[date][b.key];
    persistSession();
    // Re-render only this card.
    const newHtml = renderBnnCard(b, date);
    const wrapper = document.createElement("div");
    wrapper.innerHTML = newHtml;
    cardEl.replaceWith(wrapper.firstElementChild);
    wireBnnCard(document.getElementById("wiz-step-content"), b, date);
    refreshNextEnabled();
  }

  // Stream-count newline characters, capping at 200 MB to keep UI responsive.
  async function streamCountRows(file) {
    if (!file.stream) {
      // Fallback: rough estimate via text() on first 1 MB.
      const slice = await file.slice(0, 1024 * 1024).text();
      const lines = slice.split("\n").length - 1;
      return Math.max(0, lines - 1); // minus header
    }
    const reader = file.stream().getReader();
    const SCAN_CAP = 200 * 1024 * 1024;
    let scanned = 0, lines = 0;
    while (scanned < SCAN_CAP) {
      const { done, value } = await reader.read();
      if (done) break;
      scanned += value.byteLength;
      // Count 0x0a bytes.
      for (let i = 0; i < value.length; i++) {
        if (value[i] === 0x0A) lines++;
      }
    }
    try { reader.cancel(); } catch (_) {}
    return Math.max(0, lines - 1); // minus header
  }

  // ── Step 3: Parameters ────────────────────────────────────────────────────

  function renderParamsStep(root) {
    const params = state.mode === "auto" ? AUTO_PARAMS : SINGLE_PARAMS;
    root.innerHTML =
      `<h3>Tuning parameters</h3>` +
      `<p class="help">All flags map 1:1 to the ${state.mode === "auto" ? "<code>AutoTunerConf</code>" : "<code>ClusterMachineAndRecipeTuner</code>"} CLI. ` +
      `Defaults match the Scala source. Range checks are enforced before <strong>Next</strong> is enabled.</p>` +
      `<div id="wiz-params-form">` +
        params.map(p => renderParamRow(p)).join("") +
      `</div>` +
      `<div style="display:flex;justify-content:space-between;align-items:center;margin-top:14px;">` +
        `<div></div>` +
        `<button class="wiz-btn" id="wiz-reset-params">Reset to defaults</button>` +
      `</div>` +
      renderStrategyTable();

    wireParamsForm(root);
    const reset = root.querySelector("#wiz-reset-params");
    if (reset) reset.onclick = () => {
      state.params = paramDefaults(state.mode);
      renderParamsStep(root);
    };
    refreshNextEnabled();
  }

  function renderParamRow(p) {
    const v = state.params[p.name];
    const idHash = p.name;
    let control = "";
    if (p.kind === "enum") {
      control = `<select data-pname="${idHash}">` +
        p.values.map(o => `<option value="${escapeHtml(o)}" ${o === v ? "selected" : ""}>${escapeHtml(o)}</option>`).join("") +
      `</select>`;
    } else if (p.kind === "bool") {
      control = `<label style="display:inline-flex;align-items:center;gap:6px;color:#c9d1d9;">` +
        `<input type="checkbox" data-pname="${idHash}" ${v ? "checked" : ""}> ` +
        `<span class="wiz-mut">${v ? "true" : "false"}</span>` +
      `</label>`;
    } else if (p.kind === "number") {
      const minAttr = (typeof p.min === "number") ? ` min="${p.min}"` : "";
      const maxAttr = (typeof p.max === "number") ? ` max="${p.max}"` : "";
      const stepAttr = (typeof p.step === "number") ? ` step="${p.step}"` : "";
      control = `<input type="number" data-pname="${idHash}" value="${v}"${minAttr}${maxAttr}${stepAttr}>`;
    }
    const flag = p.flag ? `<code>${escapeHtml(p.flag)}</code>` : `<code>${escapeHtml(p.name)}=…</code>`;
    return `<div class="wiz-param" data-prow="${idHash}">` +
      `<div class="pname"><div>${escapeHtml(p.name)}</div><div>${flag}</div></div>` +
      `<div>${control}</div>` +
      `<div class="pdescr">${escapeHtml(p.descr || "")}<div class="err" data-perr="${idHash}" style="display:none;"></div></div>` +
    `</div>`;
  }

  function wireParamsForm(root) {
    const params = state.mode === "auto" ? AUTO_PARAMS : SINGLE_PARAMS;
    params.forEach(p => {
      const el = root.querySelector(`[data-pname="${p.name}"]`);
      if (!el) return;
      const onChange = () => {
        let v;
        if (p.kind === "bool") v = el.checked;
        else if (p.kind === "number") v = el.value === "" ? NaN : parseFloat(el.value);
        else v = el.value;
        state.params[p.name] = v;

        const ok = paramValid(p, v);
        el.classList.toggle("is-invalid", !ok);
        const err = root.querySelector(`[data-perr="${p.name}"]`);
        if (err) {
          if (!ok) {
            err.textContent = (() => {
              if (p.kind === "number" && !Number.isFinite(v)) return "Must be a number.";
              if (p.kind === "number" && typeof p.min === "number" && v < p.min) return `Must be ≥ ${p.min}.`;
              if (p.kind === "number" && typeof p.max === "number" && v > p.max) return `Must be ≤ ${p.max}.`;
              return "Invalid value.";
            })();
            err.style.display = "";
          } else {
            err.style.display = "none";
          }
        }
        // Bool label echo
        if (p.kind === "bool") {
          const echo = el.parentElement.querySelector(".wiz-mut");
          if (echo) echo.textContent = el.checked ? "true" : "false";
        }
        // Strategy column highlight refresh
        if (p.name === "strategy") refreshStrategyHighlight();
        persistSession();
        refreshNextEnabled();
      };
      el.onchange = onChange;
      el.oninput = onChange;
    });
    refreshStrategyHighlight();
  }

  function renderStrategyTable() {
    const cols = STRATEGY_TABLE.cols;
    const sel = state.params.strategy || "default";
    const headRow = `<tr><th>Field</th>` +
      cols.map(c => `<th class="${c === sel ? "col-active head" : ""}" data-col="${c}">${escapeHtml(c)}</th>`).join("") +
      `</tr>`;
    const bodyRows = STRATEGY_TABLE.fields.map(f => {
      const cells = cols.map(c => {
        const v = STRATEGY_TABLE.values[c][f.key];
        return `<td class="${c === sel ? "col-active" : ""}" data-col="${c}">${escapeHtml(v)}</td>`;
      }).join("");
      return `<tr><td>${escapeHtml(f.label)}</td>${cells}</tr>`;
    }).join("");
    return `<details style="margin-top:14px;" open>` +
      `<summary style="cursor:pointer;color:#8b949e;">Strategy comparison <span class="wiz-mut">(values from <code>single/TuningStrategies.scala</code>)</span></summary>` +
      `<table class="wiz-strategy-table">` +
        `<thead>${headRow}</thead>` +
        `<tbody>${bodyRows}</tbody>` +
      `</table>` +
    `</details>`;
  }

  function refreshStrategyHighlight() {
    const sel = state.params.strategy || "default";
    document.querySelectorAll(".wiz-strategy-table [data-col]").forEach(el => {
      const isActive = el.dataset.col === sel;
      el.classList.toggle("col-active", isActive);
      if (el.tagName === "TH") el.classList.toggle("head", isActive);
    });
  }

  // ── Step 4: Run instructions ──────────────────────────────────────────────

  function renderRunStep(root) {
    const isAuto = state.mode === "auto";
    const date = isAuto ? state.dates.cur : state.dates.ref;
    const dl = renderDownloadPanel(isAuto);
    const land = renderLandPanel(date);
    const runner = renderRunnerPanel(isAuto, date);
    const after = renderAfterPanel();

    root.innerHTML =
      `<h3>Run instructions</h3>` +
      `<p class="help">All set. The wizard probes for the TunerService API; if it's up you can launch the run with one click. ` +
      `If not, the IntelliJ + bash artifacts below let you run locally.</p>` +
      `<div id="wiz-api-panel-host"></div>` +
      dl + land + runner + after;

    wireRunStep(root, date, isAuto);
    // Probe the API and render the launch panel asynchronously.
    renderApiPanel(root, date, isAuto);
  }

  // ── API-mode launch panel (Phase 2) ───────────────────────────────────────

  async function renderApiPanel(root, date, isAuto) {
    const host = root.querySelector("#wiz-api-panel-host");
    if (!host) return;
    host.innerHTML =
      `<div class="wiz-run-panel" style="border-left-color:#8b949e;">` +
        `<h4><span class="step-num" style="background:#8b949e;">0</span> One-click launch <span class="wiz-mut">(probing API…)</span></h4>` +
        `<div class="panel-help">Checking whether <code>serve.sh --api</code> is up at <code>/api/health</code>…</div>` +
      `</div>`;

    const ok = (typeof TunerApi !== "undefined") ? await TunerApi.health() : false;
    if (!ok) {
      host.innerHTML =
        `<div class="wiz-run-panel" style="border-left-color:#8b949e;">` +
          `<h4><span class="step-num" style="background:#8b949e;">0</span> One-click launch <span class="wiz-mut">(API mode unavailable)</span></h4>` +
          `<div class="panel-help">Boot the dashboard with <code>./serve.sh --api</code> to enable a real <strong>Start Tuning</strong> button here. ` +
          `For now, follow the IntelliJ instructions in panel 3 below.</div>` +
        `</div>`;
      return;
    }

    host.innerHTML = renderApiLaunchPanel(date, isAuto);
    wireApiLaunchPanel(host, date, isAuto);
  }

  function renderApiLaunchPanel(date, isAuto) {
    const stagedCount = countStaged(date);
    const totalRequired = BNN.filter(b => b.required).length;
    const validRequired = BNN.filter(b => b.required).filter(b =>
      (state.stagedMeta[date] || {})[b.key] && state.stagedMeta[date][b.key].valid
    ).length;
    const provisioned = !!(state.provisionedDates && state.provisionedDates[date]);
    const provLine = provisioned
      ? `<li><span class="wiz-ok">✓</span> Inputs are already on the server (provisioned at Step 2→3).</li>`
      : `<li>Persist projectId, create <code>${escapeHtml(((typeof config !== "undefined" && config && config.inputsPath) || "") + "/" + date + "/")}</code>, and upload the ${stagedCount} staged CSV(s) <span class="wiz-mut">(skipped at Step 2→3)</span>.</li>`;
    return `<div class="wiz-run-panel">` +
      `<h4><span class="step-num">0</span> Start Tuning <span class="wiz-mut">(API mode)</span></h4>` +
      `<div class="panel-help">The TunerService is reachable. The wizard will:</div>` +
      `<ol class="wiz-mut" style="font-size:12px;line-height:1.7;">` +
        provLine +
        `<li>POST to <code>/api/runs/${isAuto ? "auto" : "single"}</code> with the parameters from Step 3.</li>` +
        `<li>Stream the run log into the box below.</li>` +
      `</ol>` +
      `<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">` +
        `<button class="wiz-btn primary" id="wiz-start" ${validRequired < totalRequired ? "disabled" : ""}>► Start Tuning</button>` +
        `<button class="wiz-btn" id="wiz-cancel-run" style="display:none;">Cancel run</button>` +
        `<span class="wiz-mut" id="wiz-run-status"></span>` +
      `</div>` +
      `<div class="wiz-codeblock" id="wiz-log-box" style="display:none;margin-top:10px;">` +
        `<button class="copy">Copy</button>` +
        `<pre style="max-height:340px;overflow:auto;" id="wiz-log-pre"></pre>` +
      `</div>` +
    `</div>`;
  }

  function countStaged(date) {
    const f = state.stagedFiles[date] || {};
    return BNN.filter(b => f[b.key]).length;
  }

  function wireApiLaunchPanel(host, date, isAuto) {
    const startBtn  = host.querySelector("#wiz-start");
    const cancelBtn = host.querySelector("#wiz-cancel-run");
    const statusEl  = host.querySelector("#wiz-run-status");
    const logBox    = host.querySelector("#wiz-log-box");
    const logPre    = host.querySelector("#wiz-log-pre");
    const copyBtn   = host.querySelector("#wiz-log-box .copy");

    if (copyBtn) copyBtn.onclick = () => {
      copyText(logPre.textContent, copyBtn);
    };

    if (!startBtn) return;
    let stopStream = null;
    let inFlightRunId = null;

    startBtn.onclick = async () => {
      startBtn.disabled = true;
      statusEl.textContent = "preparing…";
      logBox.style.display = "";
      logPre.textContent = "";
      const append = (line) => {
        logPre.textContent += line + "\n";
        logPre.scrollTop = logPre.scrollHeight;
      };

      try {
        // The Step 2 → 3 transition already provisioned the inputs dir and
        // uploaded the CSVs (and persisted projectId). Re-do those steps only
        // if the user took the "Skip and continue" escape hatch on the
        // provisioning UI — track that via state.provisionedDates.
        const provisioned = !!(state.provisionedDates && state.provisionedDates[date]);
        if (!provisioned) {
          const cfgNow = (typeof config !== "undefined" && config && config.gcpProjectId) || "";
          if (state.projectId && state.projectId !== cfgNow) {
            append(`▶ persisting projectId override → config.local.json`);
            await TunerApi.persistConfig({ gcpProjectId: state.projectId });
          }
          append(`▶ POST /api/inputs/${date}`);
          await TunerApi.mkdirInput(date);
          const files = state.stagedFiles[date] || {};
          const keys = BNN.filter(b => files[b.key]).map(b => b.key);
          for (const k of keys) {
            const b = BNN.find(x => x.key === k);
            const f = files[k];
            append(`▶ PUT /api/inputs/${date}/csv?name=${b.csv}  (${formatBytes(f.size)})`);
            await TunerApi.uploadCsv(date, b.csv, f);
          }
          if (!state.provisionedDates) state.provisionedDates = {};
          state.provisionedDates[date] = true;
        } else {
          append(`▶ inputs already provisioned for ${date} (Step 2→3 transition)`);
        }

        // 4) Kick off the run.
        const params = buildRunParams(isAuto);
        append(`▶ POST /api/runs/${isAuto ? "auto" : "single"}  ${JSON.stringify(params)}`);
        const startResp = isAuto
          ? await TunerApi.startAuto(params)
          : await TunerApi.startSingle(params);
        inFlightRunId = startResp.runId;
        statusEl.textContent = `running · runId=${inFlightRunId}`;
        cancelBtn.style.display = "";

        // 5) Long-poll the log.
        stopStream = TunerApi.streamRun(inFlightRunId, {
          onLine: (l) => append(l),
          onStatus: (s) => { statusEl.textContent = `${s} · runId=${inFlightRunId}`; },
          onDone: (resp) => {
            cancelBtn.style.display = "none";
            startBtn.disabled = false;
            startBtn.textContent = "► Start another";
            if (resp.status === "done") {
              statusEl.innerHTML = `<span class="wiz-ok">✓ done</span> · runId=${escapeHtml(inFlightRunId)}`;
              append(`✓ Run ${inFlightRunId} completed.`);
              renderRunComplete(host, date, isAuto);
            } else if (resp.status === "failed") {
              statusEl.innerHTML = `<span class="err-chip">✗ failed</span> · runId=${escapeHtml(inFlightRunId)}`;
              if (resp.error) append(`✗ ${resp.error}`);
            } else if (resp.status === "cancelled") {
              statusEl.innerHTML = `<span class="wiz-warn">⚠ cancelled</span> · runId=${escapeHtml(inFlightRunId)}`;
            }
          },
          onError: (e) => append(`! polling error: ${e.message || e}`),
        });
      } catch (e) {
        startBtn.disabled = false;
        statusEl.innerHTML = `<span class="err-chip">✗ ${escapeHtml(e.message || String(e))}</span>`;
        append(`✗ ${e.message || e}`);
        if (e.body && e.body.error === "run_in_progress") {
          append(`  ↳ existing run in progress: ${e.body.currentRunId}. Wait for it to finish or DELETE it.`);
        }
      }
    };

    if (cancelBtn) cancelBtn.onclick = async () => {
      if (!inFlightRunId) return;
      try { await TunerApi.cancelRun(inFlightRunId); } catch (_) { /* ignore */ }
      if (stopStream) stopStream();
      statusEl.innerHTML = `<span class="wiz-warn">⚠ cancelling…</span>`;
    };
  }

  function renderRunComplete(host, date, isAuto) {
    const refresh = document.createElement("div");
    refresh.style.marginTop = "8px";
    refresh.innerHTML =
      `<button class="wiz-btn primary" id="wiz-open-analysis">Open the new analysis →</button>`;
    host.querySelector(".wiz-run-panel").appendChild(refresh);
    const btn = host.querySelector("#wiz-open-analysis");
    if (btn) btn.onclick = async () => {
      try {
        if (typeof discoverAnalyses === "function") {
          discoveredEntries = await discoverAnalyses();
        }
      } catch (_) { /* ignore */ }
      clearSession();
      cleanup();
      closeModalSilently();
      // Heuristic: tuner writes outputs to <date>_auto_tuned for auto mode, <date> for single.
      const targetDir = isAuto ? `${date}_auto_tuned` : date;
      if (typeof navigate === "function") navigate({ data: targetDir });
    };
  }

  function buildRunParams(isAuto) {
    if (isAuto) {
      const out = {
        referenceDate: state.dates.ref,
        currentDate:   state.dates.cur,
      };
      AUTO_PARAMS.forEach(p => {
        const v = state.params[p.name];
        if (v === undefined || v === null) return;
        if (p.kind === "bool") out[p.name] = !!v;
        else out[p.name] = v;
      });
      return out;
    }
    const out = {
      date: state.dates.ref,
      flattened: !!state.flattened,
    };
    SINGLE_PARAMS.forEach(p => {
      if (p.name === "flattened") return;
      const v = state.params[p.name];
      if (v === undefined || v === null) return;
      out[p.name] = v;
    });
    return out;
  }

  function formatBytes(n) {
    if (typeof n !== "number") return "";
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n/1024).toFixed(1)} KB`;
    return `${(n/(1024*1024)).toFixed(1)} MB`;
  }

  function renderDownloadPanel(isAuto) {
    const date = isAuto ? state.dates.cur : state.dates.ref;
    const label = isAuto ? `Current date inputs (${date})` : `Inputs (${date})`;
    const files = state.stagedFiles[date] || {};
    const stagedKeys = BNN.filter(b => files[b.key]).map(b => b.key);
    const refNote = isAuto
      ? `<div class="panel-help"><strong>Reference date <code>${escapeHtml(state.dates.ref)}</code></strong>: read from disk, no download needed.</div>`
      : "";
    let body;
    if (!stagedKeys.length) {
      body = `<div class="panel-help"><strong>${escapeHtml(label)}</strong>: no CSVs staged in this session.</div>`;
    } else {
      const buttons = stagedKeys.map(k => {
        const b = BNN.find(x => x.key === k);
        return `<button class="wiz-dl-btn" data-dl="${k}|${escapeHtml(date)}">Download ${escapeHtml(b.csv)}</button>`;
      }).join("");
      body = `<div style="margin-bottom:10px;">` +
        `<div class="panel-help"><strong>${escapeHtml(label)}</strong>:</div>` +
        `<div class="wiz-download-row">` +
          `<button class="wiz-dl-btn all" data-dl-all="${escapeHtml(date)}">↓ Download all (${stagedKeys.length})</button>` +
          buttons +
        `</div>` +
      `</div>`;
    }
    return `<div class="wiz-run-panel">` +
      `<h4><span class="step-num">1</span> Save staged CSVs</h4>` +
      `<div class="panel-help">Each click downloads a CSV to your browser's default download folder.</div>` +
      refNote +
      body +
    `</div>`;
  }

  function renderLandPanel(date) {
    const inputsRel = (typeof config !== "undefined" && config && config.inputsPath) || "<inputsPath from config.json>";
    const csvList = BNN
      .filter(b => state.stagedFiles[date] && state.stagedFiles[date][b.key])
      .map(b => `~/Downloads/${b.csv}`)
      .join(" \\\n   ");
    const moveStanza = csvList
      ? `mv ${csvList} \\\n   "$INPUTS_DIR/$TARGET_DATE/"`
      : `# (No CSVs staged in this session — copy your existing CSVs here as needed.)`;
    const snippet =
`# Run from your project's frontend dir, e.g.:
#   cd src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend
INPUTS_DIR="${inputsRel}"   # from config.json
TARGET_DATE="${date}"

mkdir -p "$INPUTS_DIR/$TARGET_DATE"
${moveStanza}
`;
    return `<div class="wiz-run-panel">` +
      `<h4><span class="step-num">2</span> Land the files in <code>${escapeHtml(inputsRel)}/${escapeHtml(date)}/</code></h4>` +
      `<div class="panel-help">Run this from your project's frontend dir (the one that contains <code>serve.sh</code>).</div>` +
      renderCodeBlock(snippet) +
    `</div>`;
  }

  function renderRunnerPanel(isAuto, _date) {
    const mainClass = isAuto
      ? "com.db.serna.orchestration.cluster_tuning.auto.ClusterMachineAndRecipeAutoTuner"
      : "com.db.serna.orchestration.cluster_tuning.single.ClusterMachineAndRecipeTuner";
    const programArgs = buildArgsString(isAuto);
    const xml = buildIntellijXml(isAuto, mainClass, programArgs);
    const mvnCmd = buildMvnCommand(isAuto, mainClass, programArgs);

    return `<div class="wiz-run-panel">` +
      `<h4><span class="step-num">3</span> Run the tuner</h4>` +
      `<div class="wiz-runner-tabs" id="wiz-runner-tabs">` +
        `<button data-tab="intellij" class="active">IntelliJ (recommended)</button>` +
        `<button data-tab="mvn">Maven / shell (advanced)</button>` +
      `</div>` +
      `<div data-runner-pane="intellij">` +
        `<div class="panel-help">Either fill an existing IntelliJ Run/Debug configuration with the values below, or download the ready-made XML and drop it into <code>.idea/runConfigurations/</code>.</div>` +
        `<div class="wiz-form-row" style="margin:8px 0 0 0;"><label>Main class</label>` +
          renderCodeBlock(mainClass, true) +
        `</div>` +
        `<div class="wiz-form-row"><label>Program arguments</label>` +
          renderCodeBlock(programArgs, true) +
        `</div>` +
        `<div class="wiz-form-row"><label>Module / classpath</label>` +
          renderCodeBlock("spark-cluster-job-tuner", true) +
        `</div>` +
        `<div class="wiz-form-row"><label>Working directory</label>` +
          renderCodeBlock("$PROJECT_DIR$", true) +
        `</div>` +
        `<button class="wiz-btn primary" id="wiz-dl-intellij">↓ Download IntelliJ run config (.run.xml)</button>` +
        `<details style="margin-top:10px;"><summary class="wiz-mut" style="cursor:pointer;">Preview XML</summary>` +
        renderCodeBlock(xml) + `</details>` +
      `</div>` +
      `<div data-runner-pane="mvn" style="display:none;">` +
        `<div class="wiz-banner warn">Maven runner is <strong>not enabled in this repo</strong>. ` +
        `<code>pom.xml</code> has no <code>scala-maven-plugin</code> or <code>exec-maven-plugin</code>. ` +
        `The IntelliJ path above is the recommended way to launch the tuner. The Maven snippet below is a Phase-2 placeholder.</div>` +
        renderCodeBlock(mvnCmd) +
      `</div>` +
    `</div>`;
  }

  function renderAfterPanel() {
    return `<div class="wiz-run-panel">` +
      `<h4><span class="step-num">4</span> After the tuner finishes</h4>` +
      `<div class="panel-help">When IntelliJ prints something like <code>Wrote analysis JSON</code> in its console, refresh the dashboard to pick up the new run.</div>` +
      `<div style="display:flex;gap:8px;flex-wrap:wrap;">` +
        `<button class="wiz-btn primary" id="wiz-reload">Refresh dashboard</button>` +
        `<button class="wiz-btn" id="wiz-finish-stay">Close wizard</button>` +
      `</div>` +
    `</div>`;
  }

  function wireRunStep(root, date, isAuto) {
    // Per-file downloads.
    root.querySelectorAll("[data-dl]").forEach(btn => {
      btn.onclick = () => {
        const [bkey, dt] = btn.dataset.dl.split("|");
        const file = (state.stagedFiles[dt] || {})[bkey];
        if (file) triggerFileDownload(file);
      };
    });
    // Download-all (sequential to dodge browser multi-download blocks).
    root.querySelectorAll("[data-dl-all]").forEach(btn => {
      btn.onclick = async () => {
        const dt = btn.dataset.dlAll;
        const files = state.stagedFiles[dt] || {};
        const stagedKeys = BNN.filter(b => files[b.key]).map(b => b.key);
        for (let i = 0; i < stagedKeys.length; i++) {
          triggerFileDownload(files[stagedKeys[i]]);
          await sleep(200);
        }
      };
    });
    // IntelliJ XML download.
    const xmlBtn = root.querySelector("#wiz-dl-intellij");
    if (xmlBtn) xmlBtn.onclick = () => {
      const mainClass = isAuto
        ? "com.db.serna.orchestration.cluster_tuning.auto.ClusterMachineAndRecipeAutoTuner"
        : "com.db.serna.orchestration.cluster_tuning.single.ClusterMachineAndRecipeTuner";
      const xml = buildIntellijXml(isAuto, mainClass, buildArgsString(isAuto));
      const tag = isAuto ? "auto" : "single";
      const fname = `${tag}_tune_${(date || "").replace(/_/g, "")}.run.xml`;
      triggerDownload(fname, xml, "application/xml");
      alert(`Saved ${fname}.\n\nMove it into your project's .idea/runConfigurations/ folder; IntelliJ recognises *.run.xml files as shared run configs and will surface it in the run-config dropdown automatically.`);
    };
    // Runner tabs.
    root.querySelectorAll("#wiz-runner-tabs button").forEach(b => {
      b.onclick = () => {
        root.querySelectorAll("#wiz-runner-tabs button").forEach(x => x.classList.remove("active"));
        b.classList.add("active");
        const tab = b.dataset.tab;
        root.querySelectorAll("[data-runner-pane]").forEach(p => {
          p.style.display = (p.dataset.runnerPane === tab) ? "" : "none";
        });
      };
    });
    // Code-block copy buttons.
    root.querySelectorAll(".wiz-codeblock .copy").forEach(btn => {
      btn.onclick = () => {
        const block = btn.closest(".wiz-codeblock");
        const txt = block.querySelector("pre").textContent;
        copyText(txt, btn);
        block.classList.add("copied");
        setTimeout(() => block.classList.remove("copied"), 1200);
      };
    });
    // Refresh + close.
    const reload = root.querySelector("#wiz-reload");
    if (reload) reload.onclick = async () => {
      reload.textContent = "Refreshing…";
      reload.disabled = true;
      try {
        if (typeof discoverAnalyses === "function") {
          discoveredEntries = await discoverAnalyses();
        }
        clearSession();
        cleanup();
        closeModalSilently();
        // Navigate to landing so the new entries surface.
        if (typeof navigate === "function") navigate({ data: null });
        else window.location.search = "";
      } catch (e) {
        reload.textContent = "Refresh failed (try again)";
        reload.disabled = false;
      }
    };
    const stay = root.querySelector("#wiz-finish-stay");
    if (stay) stay.onclick = () => {
      clearSession();
      cleanup();
      closeModalSilently();
    };
  }

  // ── Output builders (IntelliJ XML, args, mvn, code blocks) ────────────────

  function buildArgsString(isAuto) {
    if (isAuto) {
      const parts = [
        `--reference-date=${state.dates.ref}`,
        `--current-date=${state.dates.cur}`,
      ];
      AUTO_PARAMS.forEach(p => {
        const v = state.params[p.name];
        if (p.kind === "bool") {
          if (v) parts.push(p.flag);
        } else {
          parts.push(`${p.flag}=${v}`);
        }
      });
      return parts.join(" ");
    }
    // Single tuner: positional date + optional flattened=false + flags
    const parts = [state.dates.ref];
    if (!state.flattened) parts.push("flattened=false");
    SINGLE_PARAMS.forEach(p => {
      const v = state.params[p.name];
      if (p.name === "flattened") return; // captured above
      parts.push(`${p.flag}=${v}`);
    });
    return parts.join(" ");
  }

  function buildIntellijXml(isAuto, mainClass, programArgs) {
    const tag = isAuto ? "auto" : "single";
    const date = isAuto ? state.dates.cur : state.dates.ref;
    const name = `${tag}_tune_${date || "TBD"}`;
    return `<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="${escapeXml(name)}" type="Application" factoryName="Application">
    <option name="MAIN_CLASS_NAME" value="${escapeXml(mainClass)}" />
    <module name="spark-cluster-job-tuner" />
    <option name="PROGRAM_PARAMETERS" value="${escapeXml(programArgs)}" />
    <option name="VM_PARAMETERS" value="-Dlog4j.configurationFile=src/main/resources/log4j2.xml" />
    <option name="WORKING_DIRECTORY" value="$PROJECT_DIR$" />
    <option name="INCLUDE_PROVIDED_SCOPE" value="true" />
    <option name="ALTERNATIVE_JRE_PATH_ENABLED" value="false" />
    <option name="ALTERNATIVE_JRE_PATH" value="" />
    <method v="2">
      <option name="Make" enabled="true" />
    </method>
  </configuration>
</component>
`;
  }

  function buildMvnCommand(isAuto, mainClass, programArgs) {
    return `# (Phase 2 placeholder — requires scala-maven-plugin + exec-maven-plugin in pom.xml)
mvn -q exec:java \\
  -Dexec.mainClass=${mainClass} \\
  -Dexec.args="${programArgs}"
`;
  }

  function renderCodeBlock(text, inline) {
    const wrap = inline ? `style="margin:0 0 0 0;"` : "";
    return `<div class="wiz-codeblock" ${wrap}>` +
      `<button class="copy">Copy</button>` +
      `<pre>${escapeHtml(text)}</pre>` +
    `</div>`;
  }

  // ── Misc helpers ──────────────────────────────────────────────────────────

  function copyText(txt, btn) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(txt).then(() => {
        if (btn) {
          const orig = btn.textContent;
          btn.textContent = "Copied!";
          setTimeout(() => { btn.textContent = orig; }, 1200);
        }
      }).catch(() => fallbackCopy(txt));
    } else {
      fallbackCopy(txt);
    }
  }

  function fallbackCopy(txt) {
    const ta = document.createElement("textarea");
    ta.value = txt;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand("copy"); } catch (_) { /* ignore */ }
    document.body.removeChild(ta);
  }

  function triggerDownload(filename, content, mime) {
    const blob = new Blob([content], { type: mime || "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 5000);
  }

  function triggerFileDownload(file) {
    const url = URL.createObjectURL(file);
    const a = document.createElement("a");
    a.href = url;
    a.download = file.name;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 5000);
  }

  function escapeXml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&apos;");
  }

  function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  return { start };
})();

// ── Boot the buttons after DOM is ready ─────────────────────────────────────
//
// We don't touch app.js's bootstrap(); we self-attach. The buttons live in the
// landing-header which is in the DOM at parse time, so DOMContentLoaded is the
// right moment. If the landing isn't yet visible (deep-link case), the buttons
// are still wired and will work the moment the user navigates back to landing.

document.addEventListener("DOMContentLoaded", () => {
  const single = document.getElementById("new-single-tuning");
  const auto   = document.getElementById("new-auto-tuning");
  if (single) single.onclick = () => Wizard.start({ mode: "single" });
  if (auto)   auto.onclick   = () => Wizard.start({ mode: "auto" });
});
