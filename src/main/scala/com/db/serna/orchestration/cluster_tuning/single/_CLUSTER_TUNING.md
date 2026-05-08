# ClusterMachineAndRecipeTuner

<!-- TOC -->
- [Overview](#overview)
- [Key metrics](#key-metrics)
- [Queries](#queries)
- [Tuner logic \(ClusterMachineAndRecipeTuner.scala\)](#tuner-logic-clustermachineandrecipetunerscala)
- [How to run](#how-to-run)
- [Cost estimation](#cost-estimation)
- [Tips](#tips)
- [Tuning Rationale](#tuning-rationale)
    - [Manual tuning rationale](#manual-tuning-rationale)
    - [Auto-scale tuning rationale](#auto-scale-tuning-rationale)
    - [Cluster machine selection rationale](#cluster-machine-selection-rationale)
    - [clusterConf computed values](#clusterconf-computed-values)

<!-- TOC -->

This guide explains how we export operat ive metrics from Dataproc logs, generate tuning recommendations, and produce
per-cluster JSON configs for manually tuned and auto-scale tuned Spark.

## Overview

- We log Spark app lifecycle and executor events as JSON in Cloud Logging

1. "event_type":"executor_status", when "executor_event":"EXECUTOR_ADDED" or "EXECUTOR_REMOVED" and  "
   EXECUTOR_FINAL_STATUS", thus, with a metric I can track the total_no_executors:
```json
   {"event_type":"executor_status","event_type_family":"SCALAMATICA-DWH_TO_CDM","clusterName":"
   cluster-wf-carga-act-bfbe08c0-main","recipeFilename":"_SPARK_JOB_m_DM_LKP_ACT_RESUL_CONTACTO_D.json","layerToLayer":"
   DWH_TO_CDM","executor_event":"EXECUTOR_ADDED","executor_id":"2","executor_host":"
   cluster-wf-carga-act-bfbe08c0-main-w-1.c.${gcp_project_id}-es.internal","total_no_executors":"1","
   current_no_executors":"0","timestamp_iso":"2025-12-15T14:06:26.131Z","max_executors_seen":"1","min_executors_seen":"
   1"}
```
2. "event_type":"application_end", with "app_start_iso", "app_end_iso" and "app_duration_millis", thus, with a metric I
   can track the job duration:
   ```json
   {"event_type":"application_end","event_type_family":"SCALAMATICA-DWH_TO_CDM","clusterName":"
   cluster-wf-carga-act-bfbe08c0-main","recipeFilename":"_SPARK_JOB_m_DM_LKP_ACT_OUT_CONTACTO_D.json","layerToLayer":"
   DWH_TO_CDM","app_start_iso":"2025-12-15T13:05:21.965Z","app_end_iso":"2025-12-15T13:06:50.194Z","
   app_duration_millis":"88229"}
   ```
- Using Log Analytics (BigQuery SQL), we derive per-cluster and per-recipe metrics.
- We export these queries to CSV.
- The `ClusterMachineAndRecipeTuner.scala` ingests the CSVs and outputs:
    - `<cluster>-manually-tuned.json`
    - `<cluster>-auto-scale-tuned.json`
    - `_clusters-summary.csv` (sorted by workers desc, then jobs desc)
    - `_clusters-summary-only-clusters-wf.csv`
    - `_clusters-summary_top_jobs.csv`
    - `_clusters-summary_num_of_workers.csv`
    - `_clusters-summary_estimated_cost_eur.csv`
    - `_clusters-summary_total_active_minutes.csv`
    - `_clusters-summary_global_cores_and_machines.csv`
    - `_generation_summary.json` — quota usage, node count prediction, per-cluster strategy/diagnostic info
    - `_generation_summary.csv` — flat version of the above

Outputs are written to:
`src/main/resources/composer/dwh/config/cluster_tuning/outputs/<YYYY_MM_DD>/`

## Key metrics
All SQL queries are under: [log_analytics](../log_analytics)

- Average executors per job (time-weighted): [b1_average_number_of_executors_per_job_by_cluster.sql](../log_analytics/b1_average_number_of_executors_per_job_by_cluster.sql)
- P95 of run-level max executors:
    - Per recipe/cluster: [b12_p95_max_executors_per_recipe_per_cluster.sql](../log_analytics/b12_p95_max_executors_per_recipe_per_cluster.sql)
    - Consolidated inputs: [b13_recommendations_inputs_per_recipe_per_cluster.sql](../log_analytics/b13_recommendations_inputs_per_recipe_per_cluster.sql)
- Average job duration per recipe/cluster: [b3_average_recipefilename_per_cluster.sql](../log_analytics/b3_average_recipefilename_per_cluster.sql)
- P95 job duration per recipe/cluster: [b8_P95_job_duration_per_recipe_per_cluster.sql](../log_analytics/b8_P95_job_duration_per_recipe_per_cluster.sql)
- Peak executors per cluster: [b2_peak_executors_seen.sql](../log_analytics/b2_peak_executors_seen.sql)
- Peak job duration per cluster: [b4_peak_job_duration_per_cluster.sql](../log_analytics/b4_peak_job_duration_per_cluster.sql)
- Fraction of runs reaching executor cap: [b5_a_times_job_reaches_max_executor_per_cluster.sql](../log_analytics/b5_a_times_job_reaches_max_executor_per_cluster.sql)
- Seconds spent at cap per recipe/cluster: [b9_time_at_cap_per_run_and_per_cluster.sql](../log_analytics/b9_time_at_cap_per_run_and_per_cluster.sql)
- Total jobs per cluster: [b6_total_jobs_per_cluster.sql](../log_analytics/b6_total_jobs_per_cluster.sql)
- Total runtime of all jobs per cluster: [b7_total_runtime_all_jobs_per_cluster.sql](../log_analytics/b7_total_runtime_all_jobs_per_cluster.sql)
- Max concurrent jobs per cluster in window: [b11_max_concurrent_jobs_per_cluster_in_window.sql](../log_analytics/b11_max_concurrent_jobs_per_cluster_in_window.sql)
- Executor churn (adds/removes per job): [b10_executor_churn_per_job_adds_removes.sql](../log_analytics/b10_executor_churn_per_job_adds_removes.sql)
- Driver exit codes (for diagnostic overrides): [b14_clusters_with_nonzero_exit_codes.sql](../log_analytics/b14_clusters_with_nonzero_exit_codes.sql)

We recommend using the flattened query: [b13_recommendations_inputs_per_recipe_per_cluster.sql](../log_analytics/b13_recommendations_inputs_per_recipe_per_cluster.sql)
which consolidates these per `(cluster_name, recipe_filename)` so the tuner can read a single CSV.

## Queries
- All queries are under: [log_analytics](../log_analytics)
- Select the desired time range via the Log Analytics UI. Export query results to CSVs in the same directory.

Recommended: Export [b13_recommendations_inputs_per_recipe_per_cluster.csv](../../../../../../../resources/composer/dwh/config/cluster_tuning/inputs/2025_12_20/b13_recommendations_inputs_per_recipe_per_cluster.csv)
for the chosen time window.

Alternative (fine-grained):

- Export individual CSVs for b1, b3, b5, b8, b11, b12.
  The [ClusterMachineAndRecipeTuner.scala](ClusterMachineAndRecipeTuner.scala) supports both modes.

Required for cost (cluster wall-clock):
- `b20_cluster_span_time.csv` — one row per Create→Delete incarnation. **Required**:
  without it `estimated_cost_eur` is forced to `0.0` per cluster (with a `WARN`).
- `b21_cluster_autoscaler_values.csv` — one row per autoscaler decision. Strongly
  recommended; spans without events fall back to the b23 avg path. See
  [Cost estimation](#cost-estimation) for details.

### Flattened vs individual CSVs: parity

Both ingestion paths (`loadFlattened` via b13 and `loadFromIndividualCSVs` via b1/b3/b5/b8/b11/b12)
produce the same `RecipeMetrics` when given equivalent data. Key parity rules:

- `cluster_name` and `recipe_filename` are the only required fields; rows missing either are skipped.
- All metric fields are optional. When NULL, identical defaults are applied in both paths:
  - `avg_executors_per_job` → `1.0`
  - `p95_run_max_executors` → `1.0`
  - `avg_job_duration_ms`   → `0.0`
  - `p95_job_duration_ms`   → `avg_job_duration_ms`
  - `runs`                  → `0L`
  - `max_concurrent_jobs`   → `1`
  - `seconds_at_cap`, `runs_reaching_cap`, `total_runs`, `fraction_reaching_cap` remain `None`.
    Note: in b13 these four fields are always NULL (the `capacity` CTE in the SQL is hardcoded to
    a single cluster), so they never contribute to flattened output regardless.

Optional (diagnostics):

- Export [b14_clusters_with_nonzero_exit_codes.csv](../log_analytics/b14_clusters_with_nonzero_exit_codes.sql) for YARN driver exit code analysis.
  When present, clusters with exit code 247 (YARN driver eviction) receive automatic driver resource overrides in the output JSONs.
  If absent, the tuner runs silently without diagnostics.

## Tuner logic (ClusterMachineAndRecipeTuner.scala)

- Machine selection: picks worker/master types to cover the required executor slots at minimal hourly cost, penalizing overly high worker counts to prefer larger machines.
- Manual mode:
    - `spark.executor.instances` is based on P95 demand (optionally boosted if frequently at cap).
    - If `spark.executor.instances = 1`, we set `spark.executor.cores = 8` to maximize single-executor throughput (no shuffle).
    - `parallelizationFactor = 5` for all recipes.
    - `clusterConf` includes:
        - `num_workers`
        - `master_machine_type`, `worker_machine_type`
        - `cluster_max_total_memory_gb`, `cluster_max_total_cores`
        - `accumulated_max_total_memory_per_jobs_gb`
- Auto-scale mode:
    - `spark.dynamicAllocation.enabled = true`
    - `minExecutors >= 2`, `initialExecutors >= 2`
    - `maxExecutors = min + 1` unless P95 clearly needs more; then use P95 (bounded by cluster capacity).
    - Same `parallelizationFactor = 5` and clusterConf fields as manual.

## How to run
1. Generate CSV(s) from Log Analytics:
    - Preferred:
      run [b13_recommendations_inputs_per_recipe_per_cluster.sql](../log_analytics/b13_recommendations_inputs_per_recipe_per_cluster.sql)
      and export
      to:[b13_recommendations_inputs_per_recipe_per_cluster.csv](../../../../../../../resources/composer/dwh/config/cluster_tuning/inputs/2025_12_20/b13_recommendations_inputs_per_recipe_per_cluster.csv)

    - Alternative: export the individual CSVs (b1, b3, b5, b8, b11, b12).

2. Run the tuner: [ClusterMachineAndRecipeTuner.run.xml](../../../../../../../../.run/orchestration/cluster_tuning/ClusterMachineAndRecipeTuner.run.xml)

3. Inspect outputs in `outputs/<YYYY_MM_DD>/`:
    - `<cluster>-manually-tuned.json`
    - `<cluster>-auto-scale-tuned.json`
    - `_clusters-summary.csv` (sorted, with estimated cost)
    - `_clusters-summary_*.csv` (sorted by other dimensions)
    - `_clusters-summary_global_cores_and_machines.csv`
    - `_generation_summary.json` / `_generation_summary.csv`

## Cost estimation

`b20_cluster_span_time.csv` is **required** to populate `estimated_cost_eur`.
There is no legacy job-sum fallback: a sum of concurrent job durations both
over-counts overlapping jobs and ignores idle cluster time, so it would lie
quietly. Without b20 we'd rather emit `0.0` and log a `WARN` than print a
misleading number.

`estimated_cost_eur` is computed per cluster incarnation as:

- **b22 (exact, when b21 events fall inside the span)** — integrate worker count
  × time over `(span_start_ts, span_end_ts]` using the autoscaler step function
  in `b21_cluster_autoscaler_values.csv` (filtered to `RECOMMENDING` events with
  a numeric `target_primary_workers`; SCALE_UP and SCALE_DOWN both move the step).
  Initial workers (before the first event) come from the first event's
  `current_primary_workers`. Master is +1 node, priced separately, pinned to the
  same family as workers.
- **b23 (avg fallback, when a span has no autoscaler events)** —
  `cost = (clusterPlan.workers × worker_hourly + master_hourly) × span_hours`.

A cluster's total cost is the sum across all its incarnations in the window.

Pricing comes from `PriceCatalog` (in `ClusterMachineAndRecipeTuner.scala`), which
loads `src/main/resources/composer/dwh/config/cluster_tuning/price_catalog_europe_west3.csv`
(`family,vCpu_eur_per_hour,memGb_eur_per_hour`). Edit that CSV — or swap it for a
different region's file — to update rates without touching code. If the CSV is
missing or unreadable the catalog falls back to the hardcoded europe-west3 values
in the same file.

## Tips
- Prefer [b13_recommendations_inputs_per_recipe_per_cluster.sql](../log_analytics/b13_recommendations_inputs_per_recipe_per_cluster.sql)
  CSV for simplicity.
- If possible, add `application_id` to both executor and application_end logs to avoid interval overlaps.
- Emitting effective Spark conf at app start (cores/memory, DA caps) makes “at cap” precise without a static capacity table.
- For clusters with many small jobs, consider larger machine types with fewer nodes to avoid IP exhaustion and improve executor placement.

## Tuning Rationale

### Manual tuning rationale

Instances primarily follow the P95 of executor demand per recipe per cluster. This gives headroom relative to average
demand and reduces “at cap” time.
If only one executor is selected, we shift the executor cores to 8. This favors throughput for single-executor jobs by
leveraging more cores and avoiding shuffle overhead (no parallel staging).
ParallelizationFactor is consistently set to 5 because it’s a job argument you’re using for repartitions, decoupled from
executor sizing.

### Auto-scale tuning rationale

Dynamic allocation is enabled with minExecutors and initialExecutors at least 2. This reduces cold-start penalties and
ensures a baseline level of concurrency.
maxExecutors defaults to min+1 unless P95 shows the job typically needs more. When P95 > min+1, we set max to
approximately P95, bounded by the cluster capacity so we don’t recommend more executors than the cluster can host.

### Cluster machine selection rationale

The tuner estimates the required total executor slots as targetExecPerJob × maxConcurrentJobs.
It evaluates candidate worker machine types and calculates executors-per-worker with memory and core constraints.
It penalizes solutions with too many workers (to avoid IP exhaustion and overhead) and picks the lowest penalized hourly
cost in europe-west3. This tends to prefer larger machine types when you’d otherwise need many small nodes for the same
capacity.

### clusterConf computed values:

cluster_max_total_memory_gb = workers × worker_machine_type.memoryGb
cluster_max_total_cores = workers × worker_machine_type.cores
accumulated_max_total_memory_per_jobs_gb:
manual: sum over recipes spark.executor.instances × spark.executor.memoryGb
DA: sum over recipes maxExecutors × spark.executor.memoryGb
These are indicative capacity envelopes for planning and dashboards.

---

## Auto-Tuner (Multi-Date Evolution)

The **Auto-Tuner** (`auto/ClusterMachineAndRecipeAutoTuner`) extends this one-off tuner with temporal awareness. It compares metrics across two dates (reference vs current), detects performance trends (improved/degraded/stable), and evolves configurations accordingly. It includes b14 persistent driver promotion, b16 OOM reboosting for degraded recipes, statistical analysis (Pearson correlations, z-score divergence detection), and a frontend visualization dashboard.

See [`auto/_AUTO_TUNING.md`](auto/_AUTO_TUNING.md) for full documentation.