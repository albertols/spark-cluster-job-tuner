---
name: cluster-tuning
description: Use whenever editing the Scala cluster-tuning module under `src/main/scala/com/db/serna/orchestration/cluster_tuning/` — `single/` (the per-date tuner), `auto/` (multi-date AutoTuner), `diff/`, and the loader-side of `log_analytics/`. Triggers when the user mentions the tuner, ClusterMachineAndRecipeTuner, AutoTuner, machine selection, executor sizing, autoscaling policy, cost calculation (b20/b21/b22/b23), PriceCatalog, ClusterSummary, ClusterPlan, RecipeMetrics, GenerationSummary, TuningStrategy / BiasMode / ExecutorTopologyPreset, MachineCatalog, the Csv parser, parseInstant, or any of the `_clusters-*.csv` outputs. Apply even when the user doesn't name the module — if they're modifying a `bNN.csv` loader, debugging an `estimated_cost_eur` value, or adjusting how clusters are scored, this skill applies.
---

# cluster-tuning — Scala-side conventions for the Spark Cluster Job Tuner

## Authoritative references

Long-form documentation lives in the repo. Read these for design rationale before making non-trivial changes:

- `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/_CLUSTER_TUNING.md` — workflow, queries, cost methodology, tuning rationale.
- `src/main/scala/com/db/serna/orchestration/cluster_tuning/log_analytics/_LOG_ANALYTICS.md` — SQL gotchas (covered separately by the `log-analytics-bigquery` skill).
- `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/_OSS_MOCK.md` — synthetic fixtures (covered separately by the `oss-mock-data` skill).

This skill is the quick map: what's where, the conventions across files, the gotchas that recur.

## File map

```
cluster_tuning/
├── single/                                 — per-date tuner
│   ├── ClusterMachineAndRecipeTuner.scala  — domain models, catalogs, CSV/JSON, run(), main()
│   ├── ClusterDiagnostics.scala            — b14 exit-code pipeline, YarnDriverEviction, DriverResourceOverride
│   ├── TuningStrategies.scala              — ExecutorTopologyPreset, BiasMode, Quotas, 3 strategies
│   ├── GenerationSummary.scala             — QuotaTracker, _generation_summary.{json,csv} writer
│   └── _CLUSTER_TUNING.md
├── auto/                                   — multi-date orchestration
│   ├── ClusterMachineAndRecipeAutoTuner.scala — main(--reference-date= --current-date=)
│   ├── AutoTunerJsonOutput.scala
│   ├── AutoTunerModels.scala
│   ├── TrendDetector.scala / StatisticalAnalysis.scala / PerformanceEvolver.scala
│   ├── frontend/                            — plain HTML/CSS/JS dashboard (separate skill)
│   └── oss_mock/                            — synthetic fixture generator (separate skill)
├── diff/
└── log_analytics/                           — bNN.sql query files (separate skill)
```

The big file is `ClusterMachineAndRecipeTuner.scala` (~1400 lines). Always navigate with section banners:

```
grep -n "^object\|^// ──\|^  def \|^  private" \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/single/ClusterMachineAndRecipeTuner.scala
```

Top-level objects (by line, approximately): `MachineCatalog` → `PriceCatalog` → `Csv` → `Json` → `Sizing` → `ClusterMachineAndRecipeTuner`.

## Pre-edit rituals

1. **Map first, read targeted.** Don't slurp the whole file. `grep -n` → `Read` with `offset`/`limit`.
2. **Reuse existing helpers.** Before introducing a new utility, check whether one exists: `Csv.parse`, `Csv.toInt/toLong/toDouble`, `Json.obj/arr/str/num`, `Sizing.executorsPerWorker`, `MachineCatalog.byName`, `PriceCatalog.pricePerHourEUR`, `parseInstant`.
3. **Check loader/writer parity.** A new column in a CSV needs a write side AND a read side. The OSS mock generator (`oss_mock/MockGen.scala`) also needs the new column for parity — see `oss-mock-data` skill.

## Conventions

- **Section banners**: `// ── Section title ──` (with the box-drawing dashes). Major files use these to chunk; new code blocks should match.
- **Visibility**: prefer `private[cluster_tuning]` for things tests need to call; plain `private` otherwise. Avoid widening visibility just to "make it accessible" — the package-private form keeps the public API tight.
- **Imports**: `org.slf4j.LoggerFactory` for logging; `java.time.Instant` for timestamps; `scala.io.Source` + `java.io.{File, BufferedWriter, FileWriter}` for I/O. Avoid pulling in new deps.
- **Logging**: every loader logs row count and source path. Every fallback logs WHY it kicked in. Use `logger.warn` for "input missing, degraded behavior"; `logger.info` for routine progress.

## Gotchas

### `Csv.parse` is naive — splits on commas, no quoting honored

```scala
val cells = row.split(",", -1).map(_.trim)
```

That's it. So:
- Free-text fields with commas (`b14.msg`, `b16.latest_driver_message`) won't parse correctly past the first comma.
- The b14 `cluster_name` is triple-quoted in real exports (`"""name"""`); the loader strips all `"` chars after split. Don't normalize this in writers — preserve the real-export shape.
- New columns containing commas need a different escape strategy or a real CSV parser (out of scope for tuner — fix in MockGen by replacing commas defensively).

### Timestamps come in two formats

`parseInstant` (in `ClusterMachineAndRecipeTuner.scala`) accepts:
- ISO-8601: `2026-04-28T13:33:37.071Z`
- BigQuery CSV export: `2026-04-28 13:33:37.071000 UTC`

Always reuse it; never roll your own. Returns `Option[Instant]` for empty / `null` / unparseable inputs.

### `_dag_*.csv` live at canonical project-wide paths, not per-date

```
src/main/resources/composer/dwh/config/
├── _dag_cluster-relationship-map.csv     ← ONE file (DAG_ID,CLUSTER_NAME)
└── _dag_cluster_creation_time.csv        ← ONE file (DAG_NAME,CLUSTER_NAME,TIMER_NAME,TIME,RUN_SINGLE_RECIPE_NUMBER)
```

These are NOT under `inputs/<date>/`. Loaders read them directly from the hardcoded path. Tools that generate test data (e.g. `oss_mock`) deliberately don't overwrite them — clusters with no entry resolve to `UNKNOWN_DAG_ID` / `ZERO_TIMER`, which the tuner handles gracefully.

### Cluster names are not unique within a window

The same `cluster_name` can appear across multiple Create→Delete cycles in the lookback window. b20 emits one row per `(cluster_name, incarnation_idx)`. Loaders and consumers must respect this — keying anything by `cluster_name` alone collapses incarnations and silently corrupts cost integration.

### Cost calculation has NO legacy fallback

Cost is `clusterAutoscaleCostEur(span, events, worker, master, fallbackWorkers)` — step-function integration over each b20 incarnation, refined by b21 events when present (b22), or b23 avg (workers × span_hours) when not. There is no job-sum fallback anymore — when b20 has no row for a cluster, `estimated_cost_eur = 0.0` with a WARN. The old "sum of avg_job_duration_ms × workers / 60" formula was removed because it both over-counts concurrent jobs and ignores idle cluster time.

### Master is +1 node, priced separately

Worker count fields show *workers only*. Cost adds master separately:
```scala
hourlyPrice(worker) * workers + hourlyPrice(master)
```
Master uses the same family as worker by current sizing rules (`val masterType = w` in `chooseMachines`).

### PriceCatalog reads from CSV with hardcoded fallback

`src/main/resources/composer/dwh/config/cluster_tuning/price_catalog_europe_west3.csv` (`family,vCpu_eur_per_hour,memGb_eur_per_hour`). Edit the CSV to update rates without recompiling. If missing/unreadable, falls back to the hardcoded europe-west3 values inside `PriceCatalog`. Don't introduce a third price source.

## Build & test

- **No Maven Scala compile.** The pom doesn't include `scala-maven-plugin`. Compilation happens through IntelliJ. `mvn compile` won't build Scala sources.
- **Tests** live at `src/test/scala/com/db/serna/orchestration/cluster_tuning/...`. Run via IntelliJ ScalaTest runner. No persistent `src/test/resources` fixtures — fixtures are either inline CSV writers in tests or generated via `oss_mock`.
- Spark-free tests use plain `AnyFunSuite with Matchers` (no SparkSession needed for tuning logic).

## Adding common changes

### Add a new metric column to b13 + the per-recipe pipeline

1. Add the field to `RecipeMetrics`.
2. Read it in `loadFlattened` (b13 path) and `loadFromIndividualCSVs` (alt path) — both with sane defaults.
3. If consumed by planning, thread through `planCluster` / `planManualRecipes` / `planDARecipes`.
4. If displayed, add to `ClusterSummary` and update `writeCsv` header.
5. **Update the OSS mock generator** (`oss_mock/MockGen.scala` + `MockScenario.scala`) to emit the new column.
6. Tests in `ClusterMachineAndRecipeTunerSpec.scala`.

### Add a new tuning strategy

1. New `case object MyStrategy extends TuningStrategy` in `TuningStrategies.scala`. Implement `toTuningPolicy`, `quotas`, `machinePreference`.
2. Register in `TuningStrategy.byName` (or wherever strategies are looked up).
3. Cover with a `TuningStrategiesSpec` test.

### Add a new diagnostic signal

1. New `case class FooSignal(...) extends DiagnosticSignal` in `ClusterDiagnostics.scala`.
2. `ClusterDiagnosticsProcessor.detectSignals` recognizes it from b14 records.
3. If it should override driver resources, extend `computeOverrides`.
4. Cover with `ClusterDiagnosticsSpec`.

## When NOT to use this skill

- Editing `auto/frontend/` (use the `frontend` skill).
- Writing or debugging `log_analytics/*.sql` (use the `log-analytics-bigquery` skill).
- Generating synthetic CSV fixtures (use the `oss-mock-data` skill).
- Anything outside `cluster_tuning/` (cache utilities, parallelism utilities — those have their own `_*.md` docs).

## Deep algorithm reference (migrated from CLAUDE.md)

Authoritative design notes for the refinement/boost/scaling passes. Keep in sync with the code; CLAUDE.md holds only a pointer here.

- **Boost lifecycle (date-aware):** `BoostState.{New, ReBoost, Holding}` in `single/refinement/RefinementVitamins.scala`. AutoTuner's `applyB16Reboosting` and `applyExecutorScaling` route signals through this lifecycle so a recipe boosted in a prior run *holds* the boost when no fresh signal exists, and *re-boosts* (compounded factor) when a fresh signal recurs. The single tuner only emits `New`.
- **B16 boost compound across re-plans:** `auto/BoostMetadataCarrier` injects `appliedMemoryHeapBoostFactor` + boosted `spark.executor.memory` + re-derived totals from the reference output JSON into a freshly-replanned current JSON BEFORE `applyB16Reboosting` runs. Without this carry, `BoostResources`/`GenerateFresh` would silently lose prior boosts. Anchored on the recipe key (NOT block content), so two recipes with byte-identical baseline blocks don't collide. `SimpleJsonParser` round-trips both `appliedMemoryHeapBoostFactor` and `appliedExecutorScaleFactor` in `extraFields`.
- **Z-score executor scale-up:** `ExecutorScaleVitamin` in `single/refinement/RefinementVitamins.scala` raises `spark.dynamicAllocation.maxExecutors` (×1.5 default) when a paired recipe is a duration outlier on `divergences_current_snapshot` (z ≥ 3.0 default), is **not** a new entry, and is cap-touching (`p95_run_max_executors / maxExecutors ≥ 0.5`). Manual recipes are skipped. `minExecutors` is intentionally untouched. Lifecycle uses the same New/ReBoost/Holding states; cumulative factor stamped as `appliedExecutorScaleFactor` per recipe.
- **Trend-driven executor scaling (v2, magnitude-aware):** `ExecutorTrendVitamin` (in `single/refinement/RefinementVitamins.scala`) + pure `ExecutorTrendScaler.{decide,prioritize}` (`single/refinement/ExecutorTrendScaler.scala`) close the *censoring trap* — a job pinned at `maxExecutors` can't signal it needs more. The blended reference→current duration trend (`0.7·p95 + 0.3·avg`) is classified into **severity tiers requiring BOTH ratio and absolute minutes lost**: Negligible (delta < `--trend-min-delta-minutes`, default 3.0 — never scales), Moderate (step cap = bias `maxStep`), Severe (ratio ≥ 2 ∧ ≥ 10 min; ×1.5·maxStep), Critical (ratio ≥ 3 ∧ ≥ 30 min; ×2·maxStep). **Graduated evidence** on `runs = min(refRuns, curRuns)`: ≥ 5 full tier cap; 2–4 admit Severe+ only with cap demoted one tier; 1 run admits Critical only, capped ×1.5; confidence floored at 0.5 after admission. An admitted UP still requires cap-pressure (`max(p95RunMax/max, fraction_reaching_cap) ≥ capTouchRatio`); `min` creeps at most **+1 per run** (only Severe+ AND pressure ≥ 0.8), `initial` follows ≤ +1; manual scales `instances`. DOWN additionally requires saving ≥ 3 min absolute; never below peak/steady demand, `min` shrinks ≤ −1 per run (`--no-trend-downscale` to disable). **Cluster-wide `prioritize`:** UP grants ranked by `impactMinutes = Δmin × curRuns` desc, each drawing `(newMax − origMax) × execCores` from a pool of `--trend-up-pool-ratio` (default 1.0) × cluster schedulable cores; pool exhausted → degrade to `originalMax + 1`, never zero. Runs **before** the z-score pass; stamps `appliedTrendScaleFactor` — **distinct** from `appliedExecutorScaleFactor` so the two compose (combined = product). `TrendScaleDecision.{severity,impactMinutes,priorityRank}` flow into the `executor_trend` boost group as per-recipe `severity`/`impact_minutes`/optional `priority_rank`. `--trend-min-gain` is removed (no separate min gain in v2). `BoostMetadataCarrier.carryTrendMetadata` carries prior min/initial/max + factor across re-plans. `SimpleJsonParser` round-trips `appliedTrendScaleFactor` in `extraFields`. The `trendPriority` oss-mock scenario proves tiering + ranking end-to-end (Critical outranks Moderate; Negligible holds even when pinned).
- **Cluster utilization % + capacity guard:** pure `CapacityGuard.{scaledMax,guard}` (`single/CapacityGuard.scala`) + `CapacityGuardVitamin` (in `single/refinement/RefinementVitamins.scala`, since `VitaminSignal`/`VitaminBoost` are sealed) hard-clamp every recipe to `--max-cluster-util-ratio` (default 0.90) of the cluster's **per-node bin-packed** capacity — `capExecutors = max_workers × min(floor(ratio·nodeCores/ec), floor(ratio·nodeMemGb/em))` — so a recipe can never out-request what YARN can schedule. NOT the aggregate `min(ratio·scaledCores/ec, …)`, which under-clamps memory-heavy executors (the wedge case). The single tuner emits new `clusterConf` fields (`min_workers`, `max_workers`, `cluster_scaled_max_cores`, `cluster_scaled_max_memory_gb`) and per-recipe `maxCoreUsagePct`/`maxMemoryUsagePct`/`capacityStatus` (post-clamp). The AutoTuner re-runs it via `applyCapacityGuard` as the **final** pass after trend + z-score (clamps what they inflated). Idempotent; no boost lifecycle (a physical ceiling). Frontend renders a stacked cores/memory **heatmap** in cluster detail (`renderClusterUtilHeatmap`/`mountClusterUtilHeatmap`, `?heatZoom=` URL state, click-through via `data-recipe`). `capacityPressure` oss-mock scenario proves the end-to-end clamp.
