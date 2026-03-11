# ClusterMachineAndRecipeTuner — How-To Guide

This guide covers every supported configuration mode, their trade-offs, and how to read the outputs.

---

## Table of Contents

1. [Prerequisites and Input Files](#1-prerequisites-and-input-files)
2. [Running the Default Configuration](#2-running-the-default-configuration)
3. [Switching Strategy](#3-switching-strategy)
4. [Overriding Executor Topology](#4-overriding-executor-topology)
5. [Combining Strategy + Topology](#5-combining-strategy--topology)
6. [Flattened vs Non-Flattened Input Mode](#6-flattened-vs-non-flattened-input-mode)
7. [Output Files Reference](#7-output-files-reference)
8. [Reading the Generation Summary](#8-reading-the-generation-summary)
9. [Diagnostic Overrides (b14 YARN Evictions)](#9-diagnostic-overrides-b14-yarn-evictions)
10. [Strategy Comparison Cheat Sheet](#10-strategy-comparison-cheat-sheet)

---

## 1. Prerequisites and Input Files

Input lives under:
```
src/main/resources/composer/dwh/config/cluster_tuning/inputs/<YYYY_MM_DD>/
```

### Required input

| File | Source | Description |
|------|--------|-------------|
| `b13_all_metrics_flat.csv` | BigQuery b13 query | All cluster + recipe metrics in one flat file (preferred, flattened mode) |

### Optional inputs

| File | Source | Description |
|------|--------|-------------|
| `b1_*.csv` … `b12_*.csv` | BigQuery b1–b12 | Individual metric files (non-flattened mode, `flattened=false`) |
| `b14_clusters_with_nonzero_exit_codes.csv` | BigQuery b14 query | YARN driver exit codes — enables diagnostic overrides |

If `b14_clusters_with_nonzero_exit_codes.csv` is absent the tuner runs silently without diagnostics.

---

## 2. Running the Default Configuration

**Entry point:** `ClusterMachineAndRecipeTuner.main(args)`

Run from IntelliJ by creating a run configuration with the following program argument:

```
2025_12_20
```

This is the minimum required argument — the date selects the input directory.

**What you get:**
- Strategy: `default` (cost-performance balance)
- Executor topology: `8cx1GBpc` (8 cores, 8 GB per executor)
- Machine families: `n2`, `n2d`, `e2`, `c3` (max 1), `c4` (max 1)
- N4 / N4D: excluded
- Preferred core count: 32 (score bonus applied to 32-core machines)

**Output directory:**
```
src/main/resources/composer/dwh/config/cluster_tuning/outputs/<YYYY_MM_DD>/
```

All outputs are written there after a successful run.

---

## 3. Switching Strategy

Three strategies are available. Pass `--strategy=<name>` as a program argument alongside the date.

### Strategy names

| Argument value | Alias | Description |
|----------------|-------|-------------|
| `default` | (default if omitted) | Balanced cost/performance — identical to pre-refactoring behaviour |
| `cost_biased` | `cost` | Minimise cluster cost; prefer fewer, tighter workers |
| `performance_biased` | `perf` | Maximise headroom; accept larger, more resilient clusters |

### Examples

```
# Balanced — identical output to running without --strategy
2025_12_20 --strategy=default

# Optimise for cost
2025_12_20 --strategy=cost_biased

# Optimise for performance / headroom
2025_12_20 --strategy=performance_biased

# Short aliases work too
2025_12_20 --strategy=cost
2025_12_20 --strategy=perf
```

### What changes per strategy

#### `default` (CostPerformanceBalance)
- `costWeight=0.4`, `sufficiencyWeight=0.5`
- `concurrencyBufferPct=0.25` — reserves 25 % concurrency slots above the p95 peak
- `preferMaxWorkers=6` — scores start penalising clusters with > 6 workers
- `capHitBoostPct=0.20` — if near quota cap, boost worker count 20 %
- Executor topology: **8 cores × 1 GB/core = 8 GB** per executor
- `daInitialEqualsMin=true` — autoscaler starts at the minimum

#### `cost_biased` (CostBiased)
- `costWeight=0.7` — cost dominates the scoring function
- `sufficiencyWeight=0.3`, `workerPenaltyWeight=0.4`
- `concurrencyBufferPct=0.10` — tighter 10 % buffer → fewer spare workers
- `preferMaxWorkers=4` — penalises clusters with > 4 workers
- `capHitBoostPct=0.10` — smaller boost when near cap
- `perWorkerPenaltyPct=0.08` — each extra worker costs more in the score
- Executor topology: **8 cores × 1 GB/core = 8 GB** (same topology as default)
- Result: machines tend to be larger per node (fewer nodes, higher utilisation)

#### `performance_biased` (PerformanceBiased)
- `costWeight=0.1` — cost is nearly irrelevant
- `sufficiencyWeight=0.8`, `utilizationWeight=0.5`
- `concurrencyBufferPct=0.40` — 40 % buffer → more spare capacity
- `preferMaxWorkers=8` — no penalty until > 8 workers
- `capHitBoostPct=0.30`, `capHitThreshold=0.20` — boost triggers earlier and harder
- `minExecutorInstances=2` — autoscaler minimum is 2 (not 1)
- `daInitialEqualsMin=false` — autoscaler starts above minimum for faster scale-up
- Executor topology: **8 cores × 2 GB/core = 16 GB** per executor (more memory headroom)
- Result: clusters will have more workers and executors with twice the memory per core

---

## 4. Overriding Executor Topology

Topology controls executor core count and memory per core independently of strategy. Use `--topology=<label>`.

### Available topologies

| Label | Executor cores | Memory per core | Total executor memory |
|-------|---------------|-----------------|----------------------|
| `8cx1GBpc` | 8 | 1 GB | **8 GB** — default |
| `8cx2GBpc` | 8 | 2 GB | **16 GB** |
| `8cx4GBpc` | 8 | 4 GB | **32 GB** |
| `4cx1GBpc` | 4 | 1 GB | **4 GB** |
| `4cx2GBpc` | 4 | 2 GB | **8 GB** |

`preferEightCoreExecutors` in the resulting `TuningPolicy` is `true` for any 8-core topology.

### Examples

```
# Default strategy but with double memory per executor (16 GB)
2025_12_20 --topology=8cx2GBpc

# More cores but compact memory (useful for CPU-bound, low-shuffle workloads)
2025_12_20 --topology=4cx1GBpc
```

### When to use each topology

| Topology | Use case |
|----------|----------|
| `8cx1GBpc` | General-purpose (matches Spark default executor sizing) |
| `8cx2GBpc` | Memory-hungry joins, large broadcast variables, wide DataFrames |
| `8cx4GBpc` | Extremely large in-memory operations (shuffle-heavy ML pipelines) |
| `4cx1GBpc` | High job count clusters where each job is small; maximise executor count |
| `4cx2GBpc` | Moderate memory needs with fine-grained concurrency |

---

## 5. Combining Strategy + Topology

`--strategy` and `--topology` can be combined freely. When both are provided, the base strategy's
bias mode and all non-topology settings are preserved; only `executorTopology` is replaced.

The resulting strategy name in outputs becomes `<strategy>+<topology>`, e.g. `cost_biased+8cx2GBpc`.

### Examples

```
# Cost savings but with more memory headroom per executor
2025_12_20 --strategy=cost_biased --topology=8cx2GBpc

# Full performance mode with maximum memory per executor
2025_12_20 --strategy=performance_biased --topology=8cx4GBpc

# Performance mode with 4-core executors (maximise executor count for fine-grained scheduling)
2025_12_20 --strategy=performance_biased --topology=4cx2GBpc

# Run with a specific date, flattened=false, and cost strategy
2025_12_20 flattened=false --strategy=cost_biased
```

---

## 6. Flattened vs Non-Flattened Input Mode

| Mode | Argument | Input file |
|------|----------|-----------|
| **Flattened** (default) | *(nothing — default)* | `b13_all_metrics_flat.csv` |
| **Non-flattened** | `flattened=false` | `b1_*.csv` … `b12_*.csv` (individual metric files) |

Flattened mode (b13) is preferred because it requires a single BigQuery export.
Non-flattened mode is available as a fallback when b13 is unavailable.

```
# Flattened — uses b13_all_metrics_flat.csv
2025_12_20

# Non-flattened — uses b1–b12 individual CSV files
2025_12_20 flattened=false
```

---

## 7. Output Files Reference

All outputs are written to:
```
src/main/resources/composer/dwh/config/cluster_tuning/outputs/<YYYY_MM_DD>/
```

### Per-cluster JSON configs (one pair per cluster)

| Pattern | Description |
|---------|-------------|
| `<cluster-name>-manually-tuned.json` | Fixed executor count (`spark.executor.instances`) |
| `<cluster-name>-auto-scale-tuned.json` | Dynamic allocation config (`spark.dynamicAllocation.*`) |

Both files contain `clusterConf` (machine type, worker count) and `recipeConf` (per-recipe Spark settings).

When a cluster has a **diagnostic driver override** (see §9), the `clusterConf` section gains:
```json
"driver_memory_gb": 8,
"driver_cores": 4,
"driver_memory_overhead_gb": 2,
"diagnostic_reason": "YarnDriverEviction: 3 evictions (exit code 247) on jobs [job-1, job-2, job-3]"
```

### Summary CSVs (always written, unchanged from pre-refactoring)

| File | Sort order | Description |
|------|-----------|-------------|
| `_clusters-summary.csv` | `-numWorkers, -noOfJobs, clusterName` | Master summary, all clusters |
| `_clusters-summary-only-clusters-wf.csv` | Same | Filtered to `cluster-wf-*` clusters only |
| `_clusters-summary_top_jobs.csv` | `-noOfJobs, clusterName` | Sorted by job count |
| `_clusters-summary_num_of_workers.csv` | `-numOfWorkers, clusterName` | Sorted by worker count |
| `_clusters-summary_estimated_cost_eur.csv` | `-estimatedCostEur, clusterName` | Sorted by cost |
| `_clusters-summary_total_active_minutes.csv` | `-totalActiveMinutes, clusterName` | Sorted by runtime |
| `_clusters-summary_global_cores_and_machines.csv` | `-totalCores` (per machine type) | Global core/machine aggregation |

All summary CSVs share the same columns:
```
cluster_name, dag_id, no_of_jobs, num_of_workers, worker_machine_type, master_machine_type,
total_active_minutes, estimated_cost_eur, TIMER_NAME, TIMER_TIME
```

### Generation summary (new)

| File | Description |
|------|-------------|
| `_generation_summary.json` | Full JSON with strategy metadata, quota usage, per-cluster entries |
| `_generation_summary.csv` | Same data in tabular form |

---

## 8. Reading the Generation Summary

`_generation_summary.json` is the quickest way to assess what the tuner decided for a given run.

### Top-level fields

```json
{
  "generated_at": "2026-03-10T14:22:00.000Z",
  "date": "2025_12_20",
  "strategy": "default",
  "bias_mode": "cost_performance_balance",
  "topology_preset": "8cx1GBpc",
  "total_clusters": 42,
  "total_predicted_nodes": 189,   // sum of (numWorkers + 1 master) across all clusters
  "total_max_nodes": 294,         // sum of (autoscalerMaxWorkers + 1 master) — worst-case ceiling
  "clusters_with_diagnostic_overrides": 3,
  "quota_usage_by_family": { ... },
  "clusters": [ ... ]
}
```

### Quota usage

```json
"quota_usage_by_family": {
  "c3":  { "used_cores": 44,   "quota_cores": 500,  "pct_used": "8.8"  },
  "c4":  { "used_cores": 32,   "quota_cores": 500,  "pct_used": "6.4"  },
  "e2":  { "used_cores": 960,  "quota_cores": 5000, "pct_used": "19.2" },
  "n2":  { "used_cores": 1024, "quota_cores": 5000, "pct_used": "20.5" },
  "n2d": { "used_cores": 256,  "quota_cores": 3000, "pct_used": "8.5"  },
  "n4":  { "used_cores": 0,    "quota_cores": 500,  "pct_used": "0.0"  },
  "n4d": { "used_cores": 0,    "quota_cores": 500,  "pct_used": "0.0"  }
}
```

N4/N4D show `used_cores=0` because they are excluded by `MachineSelectionPreference.Default`.

`pct_used > 80` is a warning signal — if a family approaches its quota, the tuner will fall back
to alternatives during future runs.

### Per-cluster entry

```json
{
  "cluster_name": "cluster-wf-spark-etl",
  "worker_machine_type": "n2-standard-32",
  "worker_family": "n2",
  "num_workers": 5,
  "max_workers_from_policy": 6,
  "total_cores": 160,
  "max_total_cores": 192,
  "strategy": "default",
  "bias_mode": "cost_performance_balance",
  "topology_preset": "8cx1GBpc",
  "diagnostic_signals": []
}
```

`diagnostic_signals` is non-empty only for clusters with detected YARN evictions or non-zero exit
patterns. When non-empty the matching JSON cluster config will contain `driver_memory_gb` etc.

---

## 9. Diagnostic Overrides (b14 YARN Evictions)

When `b14_clusters_with_nonzero_exit_codes.csv` exists in the input directory, the tuner
automatically analyses YARN driver exit codes and injects driver resource overrides.

### How it works

1. **Load** — `ClusterDiagnosticsProcessor.loadExitCodes()` reads the b14 CSV, stripping
   triple-quotes from `cluster_name` (a BigQuery CSV encoding artifact).
2. **Detect signals** — per cluster:
   - Exit code `247` → `YarnDriverEviction` (driver evicted from YARN due to memory pressure)
   - Other non-zero codes → `NonZeroExitPattern` (logged as diagnostic signal, no resource change)
3. **Compute overrides** — only for clusters with `YarnDriverEviction`:
   - `driverMemoryGb = baseDriverMemoryGb + 4` (default base: 4 GB → becomes 8 GB)
   - `driverCores = max(baseDriverCores, 4)` (default base: 2 → becomes 4)
   - `driverMemoryOverheadGb = Some(2)`
   - `diagnosticReason` — human-readable description included in cluster JSON

### b14 CSV format

```
timestamp,job_id,cluster_name,driver_exit_code,msg
2026-01-01T00:00:00Z,job-1,"""cluster-wf-spark-etl""",247,Container killed by YARN
2026-01-01T00:01:00Z,job-2,"""cluster-wf-spark-etl""",247,Container killed by YARN
2026-01-01T00:02:00Z,job-3,"""cluster-wf-other""",1,Exit code 1
```

Note the triple-quoted `cluster_name` — this is handled automatically.

### What you'll see in cluster JSON with an override

```json
{
  "clusterConf": {
    "workerMachineType": "n2-standard-32",
    "numWorkers": 5,
    "driver_memory_gb": 8,
    "driver_cores": 4,
    "driver_memory_overhead_gb": 2,
    "diagnostic_reason": "YarnDriverEviction: 2 evictions (exit code 247) affecting jobs [job-1, job-2]"
  },
  ...
}
```

### What you'll see in the generation summary

```json
{
  "clusters_with_diagnostic_overrides": 2,
  "clusters": [
    {
      "cluster_name": "cluster-wf-spark-etl",
      "diagnostic_signals": [
        "YarnDriverEviction: 2 evictions (exit code 247) affecting jobs [job-1, job-2]"
      ]
    }
  ]
}
```

---

## 10. Strategy Comparison Cheat Sheet

```
                    ┌──────────────────────────────────────────────────────────────────┐
                    │              STRATEGY QUICK-REFERENCE                            │
                    ├──────────────────────┬────────────────────┬──────────────────────┤
                    │ Setting              │ cost_biased        │ performance_biased   │
                    │                      │ (vs default)       │ (vs default)         │
                    ├──────────────────────┼────────────────────┼──────────────────────┤
                    │ costWeight           │ 0.7  (+0.3)        │ 0.1  (-0.3)          │
                    │ sufficiencyWeight    │ 0.3  (-0.2)        │ 0.8  (+0.3)          │
                    │ concurrencyBuffer    │ 10 % (-15 %)       │ 40 % (+15 %)         │
                    │ preferMaxWorkers     │ 4    (-2)          │ 8    (+2)            │
                    │ capHitBoostPct       │ 10 % (-10 %)       │ 30 % (+10 %)         │
                    │ executorMemoryGb     │ 8 GB (same)        │ 16 GB (+8 GB)        │
                    │ minExecutorInstances │ 1    (same)        │ 2    (+1)            │
                    │ daInitialEqualsMin   │ true (same)        │ false (faster start) │
                    └──────────────────────┴────────────────────┴──────────────────────┘

  Typical outcome:
    cost_biased        → fewer workers, larger per-node machines, lower $/hr
    default            → balanced worker count, 8 GB executors
    performance_biased → more workers, 16 GB executors, faster ramp-up
```

### Machine family priority (all strategies share the same default preference)

```
  Priority 1 (highest): n2-standard-32   ← preferred if quota available
  Priority 2:           n2d-standard-32
  Priority 3:           e2-standard-32
  Priority 4 (capped):  c3-standard-*    ← at most 1 cluster per run
  Priority 5 (capped):  c4-standard-*    ← at most 1 cluster per run
  Excluded:             n4-*, n4d-*      ← never selected

  Machines with exactly 32 cores receive a -0.10 score bonus (horizontal-scale-first policy).
```

### Argument quick reference

```
# Syntax
<YYYY_MM_DD> [flattened=false] [--strategy=<name>] [--topology=<label>]

# Examples
2025_12_20
2025_12_20 --strategy=cost_biased
2025_12_20 --strategy=perf
2025_12_20 --topology=8cx2GBpc
2025_12_20 --strategy=cost_biased --topology=8cx2GBpc
2025_12_20 flattened=false --strategy=performance_biased --topology=8cx4GBpc
```
