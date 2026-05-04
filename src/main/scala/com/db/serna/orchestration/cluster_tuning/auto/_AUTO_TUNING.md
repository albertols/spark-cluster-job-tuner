/# Auto-Tuning: Multi-Date Performance Evolution

## Overview

The **Auto-Tuner** (`ClusterMachineAndRecipeAutoTuner`) extends the one-off tuner with **temporal awareness**. Instead of producing configurations from a single date's metrics, it compares metrics across at least two dates (**reference** and **current**) to:

1. Detect whether performance has **improved**, **degraded**, or remained **stable**
2. **Evolve** cluster and recipe configurations based on detected trends
3. **Preserve** historical configurations for clusters/recipes absent from current metrics
4. Produce **statistical analysis** (correlations, divergences) for deeper insight
5. Output **frontend-ready JSON** for interactive visualization

### One-Off Tuner vs Auto-Tuner

| Aspect | One-Off Tuner | Auto-Tuner |
|---|---|---|
| Input dates | 1 date | 2+ dates (reference + current) |
| Decision basis | Absolute metric values | Metric deltas across dates |
| Config evolution | Fresh plan every run | Keep/boost/preserve based on trend |
| b14 handling | Single-date eviction detection | Persistent eviction detection (both dates) |
| b16 handling | Separate refinement step | Integrated reboosting for degraded recipes |
| Output | Per-cluster JSONs + summaries | Same + analysis JSON/CSVs |

---

## Data Flow

```mermaid
flowchart TD
    %% ── Styles ────────────────────────────────────────────────────────────────
    classDef csvFile    fill:#1e3a5f,stroke:#4a90d9,color:#cce5ff,rx:4
    classDef scalaObj   fill:#2d1b4e,stroke:#9b59b6,color:#e8d5ff,rx:6
    classDef snapshot   fill:#1a3a2a,stroke:#27ae60,color:#c8f0d8,rx:6
    classDef decision   fill:#4a2800,stroke:#e67e22,color:#ffe5b4,rx:6
    classDef actionGood fill:#1a3a1a,stroke:#2ecc71,color:#c8f0c8,rx:4
    classDef actionBad  fill:#3a1a1a,stroke:#e74c3c,color:#ffc8c8,rx:4
    classDef actionNeutral fill:#2a2a3a,stroke:#7f8c8d,color:#d5d8dc,rx:4
    classDef outJson    fill:#1a2a3a,stroke:#3498db,color:#aed6f1,rx:4
    classDef outCsv     fill:#1a2a2a,stroke:#1abc9c,color:#a2d9ce,rx:4
    classDef outTxt     fill:#2a2a1a,stroke:#f39c12,color:#fdebd0,rx:4
    classDef dirBox     fill:#111,stroke:#444,color:#aaa

    %% ── INPUT LAYER ──────────────────────────────────────────────────────────
    subgraph INPUTS ["📁  inputs/YYYY_MM_DD/"]
        direction LR
        B13["📄 b13_recommendations_inputs\n_per_recipe_per_cluster.csv\n──────────────────────\navg_executors · p95_duration\nruns · fraction_reaching_cap"]
        B14["📄 b14_clusters_with\n_nonzero_exit_codes.csv\n──────────────────────\ndriver exit code 247\n(YARN eviction)"]
        B16["📄 b16_oom_job\n_driver_exceptions.csv\n──────────────────────\nJava heap OOM\nper recipe / cluster"]
    end
    class B13,B14,B16 csvFile

    %% ── SNAPSHOT LOADING ─────────────────────────────────────────────────────
    subgraph SNAPSHOTS ["⚙️  ClusterMachineAndRecipeAutoTuner.scala · loadSnapshot()"]
        REF["🗓️  Reference DateSnapshot\ndate · metrics · b14Signals\ndriverOverrides"]
        CUR["🗓️  Current DateSnapshot\ndate · metrics · b14Signals\ndriverOverrides"]
    end
    class REF,CUR snapshot

    B13 -->|"metrics"| REF
    B13 -->|"metrics"| CUR
    B14 -->|"b14 signals"| REF
    B14 -->|"b14 signals"| CUR

    %% ── TREND ANALYSIS ───────────────────────────────────────────────────────
    subgraph ANALYSIS ["⚙️  TrendDetector.scala  ·  StatisticalAnalysis.scala"]
        PAIR["🔗 MetricsPair\nref metrics ↔ cur metrics\nper (cluster, recipe)"]
        TREND["📊 TrendDetector\n─────────────────\nImproved / Degraded\nStable / New / Dropped\n+ confidence score"]
        STATS["📐 StatisticalAnalysis\n─────────────────\nPearson correlations\nz-score divergences"]
    end
    class PAIR,TREND,STATS scalaObj

    REF -->|"ref metrics"| PAIR
    CUR -->|"cur metrics"| PAIR
    PAIR --> TREND
    PAIR --> STATS

    %% ── EVOLUTION DECISIONS ──────────────────────────────────────────────────
    subgraph EVOLUTION ["⚙️  PerformanceEvolver.scala"]
        EVOLVER["🧠 PerformanceEvolver\ndecideEvolutions()\n─────────────────\ntrend → action"]
    end
    class EVOLVER scalaObj

    TREND -->|"TrendAssessment[]"| EVOLVER

    %% ── ACTIONS ──────────────────────────────────────────────────────────────
    subgraph ACTIONS ["⚙️  ClusterMachineAndRecipeAutoTuner.scala · run()"]
        PLAN["🔨 Plan Fresh\nplanCluster()\nplanManualRecipes()\nplanDARecipes()\n─────────────\nBoostResources\nGenerateFresh"]
        KEEP["✅ Keep As-Is\nre-emit reference JSON verbatim\n─────────────\nKeepAsIs\n(Improved · Stable)"]
        HIST["📦 Preserve Historical\nre-emit reference JSON verbatim\n─────────────\nPreserveHistorical\n(DroppedEntry)"]
    end
    class PLAN actionBad
    class KEEP actionGood
    class HIST actionNeutral

    EVOLVER -->|"BoostResources\nGenerateFresh"| PLAN
    EVOLVER -->|"KeepAsIs"| KEEP
    EVOLVER -->|"PreserveHistorical"| HIST

    %% ── b14 / b16 VITAMINS ───────────────────────────────────────────────────
    B14 -->|"b14 in cur_date:\npromote master\n(eviction 247)"| PLAN
    B16 -->|"b16 OOM signals\n(ref or cur date):\nMemoryHeapBoostVitamin"| PLAN
    B16 -.->|"b16 OOM signals\npersist to keep/preserved"| KEEP
    B16 -.->|"b16 OOM signals\npersist to historical"| HIST

    %% ── OUTPUTS ──────────────────────────────────────────────────────────────
    subgraph OUTPUTS ["📁  outputs/YYYY_MM_DD/"]
        direction LR
        subgraph CLUSTER_JSON ["Per-cluster configs"]
            JMAN["📄 cluster-name\n-manually-tuned.json"]
            JDA["📄 cluster-name\n-auto-scale-tuned.json"]
        end
        subgraph ANALYSIS_OUT ["Fleet analysis"]
            AJSON["📄 _auto_tuner_analysis.json\ntrends · correlations\ndivergences (frontend-ready)"]
            ACSV["📄 _trend_summary.csv\n📄 _correlations.csv\n📄 _divergences.csv"]
        end
        subgraph SUMMARY_OUT ["Generation summary"]
            GSUM["📄 _generation_summary.json\n📄 _generation_summary.csv"]
            GTXT["📄 _generation_summary\n_auto_tuner.txt\n(b14/b16 boosts · stats)"]
            SCSV["📄 _clusters-summary*.csv\n(7 sorted views)"]
        end
    end
    class JMAN,JDA outJson
    class AJSON,ACSV,GSUM,GTXT,SCSV outCsv

    PLAN  --> JMAN & JDA
    KEEP  --> JMAN & JDA
    HIST  --> JMAN & JDA
    STATS --> AJSON
    TREND --> AJSON
    STATS --> ACSV
    TREND --> ACSV
    PLAN  --> GSUM & GTXT & SCSV
```

---

## Performance Trend Analysis

### Classification Logic

For each paired (cluster, recipe), the `TrendDetector` computes deltas across all metrics and classifies the trend:

| Condition | Classification |
|---|---|
| p95 duration increased > 10% | **Degraded** |
| fraction_reaching_cap increased > 15% (and > 0) | **Degraded** |
| p95 duration decreased > 5% AND cap-hit not worsened | **Improved** |
| Within noise bands | **Stable** |
| Only in current date | **NewEntry** |
| Only in reference date | **DroppedEntry** |

### Confidence Level

Confidence is based on the minimum run count between both dates:

```
confidence = min(1.0, min(ref.runs, cur.runs) / 10.0)
```

| Min runs | Confidence |
|---|---|
| 1 | 0.1 |
| 5 | 0.5 |
| 10+ | 1.0 |

### Metrics Tracked

| Metric | Source | What it measures |
|---|---|---|
| avg_executors_per_job | b13 | Average parallelism |
| p95_run_max_executors | b13 | Peak resource demand |
| avg_job_duration_ms | b13 | Average job latency |
| p95_job_duration_ms | b13 | Tail latency (P95) |
| fraction_reaching_cap | b13 | Capacity pressure |
| runs | b13 | Workload volume |

---

## Evolution Logic

### Decision Table

| Trend | Action | What happens |
|---|---|---|
| Improved | KeepAsIs | Reference config emitted unchanged; b16 reboosting persisted if OOM signals exist |
| Degraded | BoostResources | Re-plan with current metrics; b16 reboosting applied if OOM signals exist |
| Stable | KeepAsIs | Reference config emitted unchanged; b16 reboosting persisted if OOM signals exist |
| NewEntry | GenerateFresh | Plan from scratch with current metrics; b16 reboosting applied if OOM signals exist |
| DroppedEntry (keep=true) | PreserveHistorical | Reference config emitted unchanged; b16 reboosting persisted if OOM signals exist |
| DroppedEntry (keep=false) | Skip | No output |

### b16 OOM Reboosting Persistence

b16 reboosting is applied to **all evolution paths** (KeepAsIs, BoostResources, GenerateFresh, PreserveHistorical), not just degraded recipes. The auto-tuner searches for b16 CSVs in both `current_date` and `reference_date` input directories. If OOM signals existed in reference_date but the b16 CSV is absent in current_date, the boost still persists — removing it would risk regression into OOM failures. The same persistence logic will apply to future vitamins (e.g., b17 memoryOverhead).

### KeepAsIs / PreserveHistorical

Reference output JSONs are read via `SimpleJsonParser` and re-emitted verbatim. This ensures exact config preservation with no floating-point drift from re-computation. After re-emission, b16 reboosting is applied if OOM signals are present in either date.

### BoostResources

1. `planCluster()` is called with current-date metrics (naturally produces larger allocations for higher metric values)
2. `planManualRecipes()` / `planDARecipes()` generate recipe configs
3. b16 reboosting is applied if OOM signals exist in either reference or current date's b16 CSV
4. If b14 driver eviction (exit code 247) is present in **current_date**, the master is **always** promoted to a more powerful machine type to mitigate YARN driver eviction:
   - **Baseline** = max(reference config master, freshly planned master) — prevents regression below what the reference already promoted to
   - **Promotion chain** (always a leap ahead):
     - `standard → highmem` (more memory, same cores)
     - `highmem → more cores` (e.g., `n2-highmem-32 → n2-highmem-48`)
     - At core cap → cross-family (e.g., `e2-highmem-16 → n2-highmem-16`)
   - Example multi-run evolution: `e2-standard-32 → n2-standard-32 → n2-highmem-32 → n2-highmem-48`
   - Reason field notes whether the eviction is persistent (both dates) or current-only

---

## Statistical Analysis

### Correlation Analysis

Pearson correlations are computed between metric delta pairs across the entire fleet:

| Metric A (delta) | Metric B (delta) | What it reveals |
|---|---|---|
| p95_run_max_executors | p95_job_duration_ms | Peak resource vs tail latency |
| avg_executors_per_job | avg_job_duration_ms | Resource consumption vs avg latency |
| fraction_reaching_cap | p95_job_duration_ms | Capacity pressure vs tail latency |
| runs | avg_job_duration_ms | Workload volume vs latency |

**Interpretation:**
- Pearson near **+1.0**: metrics move together (e.g., more executors correlate with longer duration = possible inefficiency)
- Pearson near **-1.0**: metrics move inversely (e.g., more executors correlate with shorter duration = scaling helps)
- Pearson near **0.0**: no linear relationship

### Divergence Detection

For each metric, the auto-tuner computes the fleet-wide mean and standard deviation of deltas, then flags (cluster, recipe) pairs whose z-score exceeds the threshold (default: 2.0).

These outliers represent recipes whose behavior differs significantly from the fleet average -- they may need special attention or investigation.

---

## CLI Usage

```bash
# Basic usage
main(Array("--reference-date=2025_12_20", "--current-date=2026_04_15"))

# With custom strategy and reboosting factor
main(Array("--reference-date=2025_12_20", "--current-date=2026_04_15",
           "--strategy=cost_biased", "--b16-reboosting-factor=2.0"))

# Disable historical preservation
main(Array("--reference-date=2025_12_20", "--current-date=2026_04_15",
           "--keep-historical-tuning=false"))

# Custom divergence threshold
main(Array("--reference-date=2025_12_20", "--current-date=2026_04_15",
           "--divergence-z-threshold=3.0"))
```

### CLI Arguments

| Argument | Default | Description |
|---|---|---|
| `--reference-date` | (required) | Reference date in YYYY_MM_DD format |
| `--current-date` | (required) | Current date in YYYY_MM_DD format |
| `--keep-historical-tuning` | true | Preserve configs for absent clusters/recipes |
| `--b16-reboosting-factor` | 1.5 | Memory boost factor for b16 OOM signals |
| `--b17-reboosting-factor` | 1.0 | Memory overhead boost (future, no-op at 1.0) |
| `--strategy` | default | Tuning strategy: default, cost_biased, performance_biased |
| `--divergence-z-threshold` | 2.0 | Z-score threshold for outlier detection |

---

## Output Files

All outputs are written to `outputs/<current_date>/` (the same dir the single tuner uses; the AutoTuner overwrites any single-tuner output for the same date so that subsequent AutoTuner runs can chain — `loadReferenceConfigs` reads from `outputs/<refDate>/`). Older runs may still live under `outputs/<date>_auto_tuned/`; the dashboard's discovery + cluster-config loaders fall back to the suffixed dir for back-compat.

| File | Description |
|---|---|
| `<cluster>-manually-tuned.json` | Per-cluster manual config (same format as one-off tuner) |
| `<cluster>-auto-scale-tuned.json` | Per-cluster DA config (same format as one-off tuner) |
| `_auto_tuner_analysis.json` | Fleet-wide analysis (frontend-ready) |
| `_trend_summary.csv` | Per-(cluster, recipe) trend, confidence, action |
| `_correlations.csv` | Metric correlation matrix |
| `_divergences.csv` | Outlier recipes with z-scores |
| `_generation_summary.json` | Quota tracking and strategy metadata |
| `_generation_summary.csv` | Same as above in CSV format |
| `_generation_summary_auto_tuner.txt` | Human-readable summary report (b14/b16 boosts, evolution stats) |
| `_clusters-summary.csv` | All clusters sorted by workers desc, jobs desc |
| `_clusters-summary-only-clusters-wf.csv` | Filtered to `clusters-wf-` prefix |
| `_clusters-summary_top_jobs.csv` | Sorted by job count |
| `_clusters-summary_num_of_workers.csv` | Sorted by worker count |
| `_clusters-summary_estimated_cost_eur.csv` | Sorted by estimated cost |
| `_clusters-summary_total_active_minutes.csv` | Sorted by active minutes |
| `_clusters-summary_global_cores_and_machines.csv` | Aggregated cores/machines by worker type |

### Analysis JSON Schema

```json
{
  "metadata": {
    "generated_at": "ISO-8601 timestamp",
    "reference_date": "YYYY_MM_DD",
    "current_date": "YYYY_MM_DD",
    "total_clusters": 150,
    "total_recipes": 800,
    "strategy": "default"
  },
  "trends_summary": {
    "improved": 45,
    "degraded": 12,
    "stable": 88,
    "new_entries": 3,
    "dropped_entries": 2
  },
  "cluster_trends": [
    {
      "cluster": "cluster-wf-...",
      "overall_trend": "degraded|improved|stable|mixed",
      "recipes": [
        {
          "recipe": "_ETL_m_....json",
          "trend": "degraded",
          "confidence": 0.85,
          "action": "boost_resources",
          "reason": "p95_job_duration_ms changed 23.4%",
          "deltas": [
            {
              "metric": "p95_job_duration_ms",
              "reference": 120000,
              "current": 148000,
              "pct_change": 23.3
            }
          ]
        }
      ]
    }
  ],
  "correlations": [
    {
      "metric_a": "delta_p95_run_max_executors",
      "metric_b": "delta_p95_job_duration_ms",
      "pearson": 0.72,
      "covariance": 15234.5,
      "n": 800
    }
  ],
  "divergences": [
    {
      "cluster": "cluster-x",
      "recipe": "recipe.json",
      "metric": "delta_p95_job_duration_ms",
      "reference": 100000,
      "current": 500000,
      "z_score": 3.2,
      "is_outlier": true
    }
  ]
}
```

---

## Source Files

```
auto/
  AutoTunerModels.scala                     # Domain models (DateSnapshot, MetricsPair, trends, etc.)
  StatisticalAnalysis.scala                 # Pure math (mean, stddev, covariance, Pearson, z-score)
  TrendDetector.scala                       # Trend classification with configurable thresholds
  PerformanceEvolver.scala                  # Evolution decision logic (trend -> action)
  AutoTunerJsonOutput.scala                 # Analysis JSON/CSV output generation
  ClusterMachineAndRecipeAutoTuner.scala    # Main entry point (Scallop CLI + run())
  _AUTO_TUNING.md                           # This documentation

Tests:
  auto/StatisticalAnalysisSpec.scala        # 16 tests: math functions, correlations, divergences
  auto/TrendDetectorSpec.scala              # 12 tests: trend classification, confidence, edge cases
  auto/ClusterMachineAndRecipeAutoTunerSpec.scala  # 18 tests: evolution, JSON/CSV, report, b14/b16

Frontend:
  frontend/index.html                       # SPA with tabs (Fleet Overview, Correlations, Divergences)
  frontend/app.js                           # Fetch + render analysis JSON
  frontend/style.css                        # Dark theme, responsive grid
  frontend/serve.sh                         # Python HTTP server launcher
```

---

## Future Tasks

### Not Yet Implemented

1. **b17 memoryOverhead reboosting** -- `--b17-reboosting-factor` CLI arg is defined but no-op at 1.0. Requires b17 SQL query and corresponding `MemoryOverheadBoostVitamin` in the refinement module.

2. **Multi-date trends (>2 dates)** -- Currently compares exactly 2 dates. Future: accept N date directories, compute regression lines / moving averages across all dates, detect acceleration/deceleration patterns.

3. **Resource decrease for sustained improvement** -- Currently `Improved` keeps configs as-is. Future: if improvement is sustained across 3+ dates, slightly decrease resources (executor memory, worker count) to save cost.

4. **Temporal fine-grained analysis** -- Current metrics are aggregated over the full time range. Future: break down by job stage, time-of-day, or concurrent job windows to understand peak behavior more precisely (e.g., max concurrent jobs at specific times).

5. **ML-based predictive tuning** -- Use historical trends to predict future resource needs and proactively adjust configurations before degradation occurs.

6. **Frontend evolution** -- Multi-date timeline slider, cost projection charts, what-if scenario simulator, config diff viewer.

7. **b17 SQL query** -- Design and implement `b17_oom_memory_overhead_exceptions.sql` to detect `OutOfMemoryError: Direct buffer memory` and container-killed-by-YARN patterns that indicate memoryOverhead pressure.

8. **Cost-aware evolution** -- When boosting resources for degraded recipes, consider the cost impact and apply a cost ceiling to prevent unbounded resource growth.

9. **Cluster-level trend aggregation** -- Currently trends are per-(cluster, recipe). Future: aggregate to cluster level to decide whether the entire cluster shape needs evolution.

10. **Confidence-weighted decisions** -- Low-confidence trends (few runs) could use different thresholds or require confirmation across multiple dates before triggering evolution.
