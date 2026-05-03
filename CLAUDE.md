# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project overview

Spark Cluster Job Tuner — Scala 2.12.18 / Spark 3.5.3 toolkit for cost-optimizing Spark cluster configs on GCP Dataproc. Three modules under `com.db.serna`:

1. **Cluster Tuning** (`orchestration/cluster_tuning/`) — Recommends GCP machine types and Spark configs from BigQuery Log Analytics CSV exports. Per-date tuner, multi-date auto-tuner, diff tool, mock-data generator, and a static-file dashboard.
2. **Cache Utilities** (`utils/spark/cache/`) — Runtime memory footprint of cached DataFrames and broadcast variables.
3. **Parallelism Utilities** (`utils/spark/parallelism/`) — Executor lifecycle listener and parallelism/partition recommendations.

## Build & test

- **Build:** Maven, Scala 2.12.18, Spark 3.5.3. `pom.xml` does not include `scala-maven-plugin`; Scala compilation runs through IntelliJ IDEA.
- **Run tests:** ScalaTest runner from IntelliJ. Tests use `local[1]`/`local[2]` Spark sessions with the UI disabled. Cluster-tuning and oss-mock specs are pure-Scala (no Spark).
- **Test infrastructure** (`src/test/scala/com/db/serna/utils/spark/`):
  - `SparkTestSession` — `AnyFunSuite` mixin with a lazy `spark` and automatic cleanup
  - `TestSparkSessionSupport` — `withSession { spark => … }` / `withCacheSession { … }` for one-off sessions (`MinimalLocalConf`, `CacheConf`)
  - Both clear `spark.driver.port` after each suite to avoid in-JVM port conflicts

## Architecture

### Source layout

```
orchestration/cluster_tuning/
  single/                              # Per-date tuner (entry point)
    ClusterMachineAndRecipeTuner.scala # Catalogs, CSV/JSON, core algorithms, 7 _clusters-*.csv writers, main()
    TuningStrategies.scala             # ExecutorTopologyPreset, BiasMode, MachineSelectionPreference, Quotas, TuningStrategy + 3 concrete strategies
    ClusterDiagnostics.scala           # b14 exit-code pipeline, YarnDriverEviction detection, DriverResourceOverride
    GenerationSummary.scala            # QuotaTracker, _generation_summary.{json,csv} writer
    refinement/                        # Post-tuning JSON refinement (RefinementVitamins, SimpleJsonParser)
  auto/                                # Multi-date AutoTuner across snapshots
    ClusterMachineAndRecipeAutoTuner.scala  # main(), pairs reference vs current snapshots, classifies trends
    TrendDetector.scala / StatisticalAnalysis.scala / PerformanceEvolver.scala / KeptRecipeCarrier.scala
    AutoTunerModels.scala / AutoTunerJsonOutput.scala
    oss_mock/                          # Synthetic CSV fixtures (OssMockMain, MockGen, MockScenarios)
    frontend/                          # Static dashboard (index.html, app.js, style.css, config.json, serve.sh)
  diff/
    TuningOutputDiff.scala             # Compares two output dirs (e.g. flattened vs non-flattened)
  log_analytics/                       # BigQuery SQL templates: b1–b12, b13 (consolidated), b14–b16, b20–b21

utils/spark/
  cache/        MemorySizingUtils, BroadcastSizingUtils, CacheLogging
  parallelism/  ExecutorTrackingListener, ParallelismUtils

local/utils/    CleanClusterAndRecipeNames    # Local-dev helper
```

### Cluster Tuner data flow

1. **Input:** CSV exports from BigQuery Log Analytics queries under `src/main/resources/composer/dwh/config/cluster_tuning/inputs/<YYYY_MM_DD>/`. Single-file mode via `b13` (recommended) or multi-file mode with individual `b1`–`b12` CSVs. Diagnostic inputs: `b14` (exit codes), `b16` (driver OOM), `b20` (cluster span time), `b21` (autoscaler events).
2. **Processing:** Load metrics by `(cluster_name, recipe_filename)` → select machine via `MachineCatalog` (E2/N2/N2D/C3/C4/N4/N4D, europe-west3 pricing) → plan manual or auto-scale executor configs per `TuningStrategy`.
3. **Output:** per-cluster JSON configs (`*-manually-tuned.json`, `*-auto-scale-tuned.json`), summary CSVs sorted by various dimensions, `_generation_summary.{json,csv}`. AutoTuner additionally emits `_auto_tuner_analysis.json`, `_correlations.csv`, `_divergences.csv`, `_trend_summary.csv`.

### Entry points

| Object | Path | Purpose |
| --- | --- | --- |
| `ClusterMachineAndRecipeTuner` | `single/` | Per-date tuner. Args: `<YYYY_MM_DD> [flattened=false] [--strategy=…] [--topology=…]` |
| `ClusterMachineAndRecipeAutoTuner` | `auto/` | Multi-date analysis. Scallop CLI: `--reference-date`, `--current-date`, `--strategy`, `--keep-historical-tuning`, `--b16-rebooting-factor`, `--divergence-z-threshold` |
| `OssMockMain` | `auto/oss_mock/` | Synthetic fixtures. Args: `--scenario=…`, `--date=…` or `--reference-date=… --current-date=…`, `--seed=…`, `--full` |
| `TuningOutputDiff` | `diff/` | Diff two output directories |
| `ClusterMachineAndRecipeTunerRefinement` | `single/refinement/` | Post-process tuned JSON configs |
| `serve.sh` | `auto/frontend/` | Serves the dashboard at `localhost:8080` (config-driven landing or back-compat single-output mode) |

### Key design details

- **Strategy threading:** `run(cfg, strategy = DefaultTuningStrategy)` derives a `TuningPolicy` and threads it explicitly to all sub-methods (no globals).
- **Topology:** `ExecutorTopologyPreset(cores, memoryPerCoreGb)` — `totalMemoryGb = cores * memoryPerCoreGb`. Default `8cx1GBpc`.
- **Machine priority:** N2-32 > N2D-32 > E2-32 (priority 1/2/3); 32-core machines get a -0.10 score bonus. C3/C4 capped at 1 cluster each. N4/N4D excluded by default.
- **Tuning modes:** Manual (`spark.executor.instances`) and Auto-scale (`spark.dynamicAllocation.*`).
- **b14 quirk:** `cluster_name` field is triple-double-quoted in the BigQuery CSV export — strip with `.replaceAll("\"", "")`.
- **Cache utils:** `MemorySizingUtils` exposes three views (JVM estimate, RDD storage info, executor storage accounting delta). `ExecutorTrackingListener` emits ISO-formatted JSON logs with `event_type` first.

## In-repo documentation

- `orchestration/cluster_tuning/single/_CLUSTER_TUNING.md`, `_DESIGN.md`, `_HOW_TO.md`
- `orchestration/cluster_tuning/auto/_AUTO_TUNING.md`
- `orchestration/cluster_tuning/auto/oss_mock/_OSS_MOCK.md`
- `orchestration/cluster_tuning/single/refinement/_REFINEMENT.md`
- `orchestration/cluster_tuning/log_analytics/_LOG_ANALYTICS.md`
- `utils/spark/cache/_CACHE.md`, `utils/spark/parallelism/_PARALLELISM.md`

## Sample data

Inputs/outputs ship for date `2099_01_01` and `2099_01_02` (with a `_auto_tuned` companion). Cluster/DAG maps live at `src/main/resources/composer/dwh/config/_dag_cluster-relationship-map.csv` and `_dag_cluster_creation_time.csv`.
