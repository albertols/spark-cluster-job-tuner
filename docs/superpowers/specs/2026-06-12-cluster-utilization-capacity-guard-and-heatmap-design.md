# Cluster-utilization %, capacity guard, and recipe heatmap — Design

**Date:** 2026-06-12
**Status:** Draft for review
**Author:** brainstormed with Claude

## Problem

Each tuned recipe declares an executor ceiling (`spark.dynamicAllocation.maxExecutors` for auto-scale, `spark.executor.instances` for manual), but nothing relates that ceiling to the **cluster's** capacity at its fully-autoscaled size. Two gaps follow:

1. **No visibility** — an operator cannot see, per job, "if this were the last/only job running, what share of the cluster's cores and memory would it grab at full scale-up?"
2. **No safety bound (CRITICAL)** — a recipe can be sized (by base planning, the trend scaler, or the z-score scaler) to demand more executors than the cluster can ever provide. On Dataproc/YARN that job **lingers indefinitely** waiting for containers that never arrive.

This design adds the per-recipe utilization percentages, a hard capacity guard that makes the over-allocation impossible, and a compact heatmap in the dashboard so hundreds of jobs per cluster can be scanned at a glance.

## Goals

- Compute and store, on **every** recipe (auto-scale and manual), `maxCoreUsagePct` and `maxMemoryUsagePct`: the job's maximum footprint as a share of the cluster's **scaled-max** cores / memory.
- Guarantee no recipe is ever sized beyond a configurable utilization ceiling of the cluster's real (bin-packing-aware) capacity. Default ceiling **0.90**.
- Expose the autoscaling node range and scaled-max capacity in `clusterConf`.
- Render a zoomable, hover-and-click heatmap (cores band over memory band) in the cluster detail pane.

## Non-goals

- Changing cost calculation. Existing `cluster_max_total_cores` / `cluster_max_total_memory_gb` (num_workers-based) are left exactly as-is.
- Modeling real GCP autoscaling-policy min/max from external config. We derive the range from the existing `AutoscalingPolicyConfig` tiers.
- Per-job *concurrent* contention (multiple jobs at once). The percentages are deliberately "last/only job" — single-tenant worst case.

## Key decisions (from brainstorming)

| Decision | Choice |
|---|---|
| Denominator for % | Cluster at **autoscaling-policy max workers** (`max_workers × per-node`). New fields; existing `cluster_max_total_*` untouched. |
| Clamp ceiling | **Configurable, default 0.90** (`--max-cluster-util-ratio`). The margin absorbs `spark.executor.memoryOverhead`, NodeManager/OS reserve, and the driver/AM container. |
| Clamp capacity model | **Per-node bin-packing**, not aggregate (see Capacity math). Prevents the memory-heavy-executor under-clamp / wedge. |
| Architecture | Pure `CapacityGuard` reused by the single-tuner emit path and the AutoTuner final pass (architecture A). |
| Heatmap | Two stacked full-width bands (cores over memory), identical sorted order so each job shares a column across bands; zoom, hover tooltip, click-through. |

## Capacity math

Let the cluster's worker node have `nodeCores` and `nodeMemGb`, autoscaled to `maxWorkers`. A recipe's executor requests `ec = spark.executor.cores` and `em = parseMemoryGb(spark.executor.memory)`. Let `ratio = --max-cluster-util-ratio` (default 0.90).

**Scaled-max (denominator for display %):**
```
scaledMaxCores = maxWorkers * nodeCores
scaledMaxMemGb = maxWorkers * nodeMemGb
```

**Displayed percentages** (aggregate share — the intuitive number; `units` = post-clamp maxExecutors or instances):
```
maxCoreUsagePct   = round1(100 * units * ec / scaledMaxCores)
maxMemoryUsagePct = round1(100 * units * em / scaledMaxMemGb)
```

**Clamp capacity (per-node bin-packing — what guarantees feasibility):**
```
execsPerNode = min( floor(ratio * nodeCores / ec), floor(ratio * nodeMemGb / em) )
capExecutors = maxWorkers * execsPerNode
```
`capExecutors` is the most executors that can *actually* be scheduled within the ratio. The aggregate `min(ratio*scaledMaxCores/ec, ratio*scaledMaxMemGb/em)` is **not** used for the clamp — it overestimates when `em/ec` is heavier than `nodeMemGb/nodeCores` (e.g. a b16-boosted 4c/18GB executor on a 48c/192GB node fits 10/node = 60 cluster-wide, but the aggregate says 64; the extra 4 would wedge).

**Applying the clamp:**
- DA: `newMax = min(maxExecutors, capExecutors)`; if `newMax < minExecutors` then `newMin = newMax`; `newInitial = clamp(initialExecutors, newMin, newMax)`.
- Manual: `newInstances = min(instances, capExecutors)`.
- Floor: if `capExecutors == 0` because a single executor exceeds `ratio * node` but still fits the raw node, set `capExecutors = 1` and flag `capacityTight: true`. If a single executor exceeds even the raw node (`ec > nodeCores || em > nodeMemGb`), set 1 and flag `capacityInfeasible: true` (a real misconfiguration to surface loudly).
- `capacityClamped: true` is stamped on the recipe whenever the guard reduced `units`.
- Percentages are recomputed from the **post-clamp** `units`, so stored values always reflect what will run.

**Idempotence:** re-running the guard on already-guarded JSON is a no-op (clamping to the same cap). This makes it safe to run after the trend / z-score passes and across AutoTuner re-plans.

## Components

### 1. `CapacityGuard` (pure) — `single/CapacityGuard.scala`
The single source of truth. No I/O.

```scala
final case class GuardResult(
    newMin: Int, newInitial: Int, newMax: Int,   // for manual, all three == newInstances
    isManual: Boolean,
    maxCoreUsagePct: Double, maxMemoryUsagePct: Double,
    capacityClamped: Boolean, capacityTight: Boolean, capacityInfeasible: Boolean
)

object CapacityGuard {
  def scaledMax(nodeCores: Int, nodeMemGb: Int, maxWorkers: Int): (Int, Int)
  def guard(
      isManual: Boolean, min: Int, initial: Int, max: Int,   // manual: instances in all three
      execCores: Int, execMemGb: Int,
      nodeCores: Int, nodeMemGb: Int, maxWorkers: Int,
      ratio: Double
  ): GuardResult
}
```

### 2. `AutoscalingPolicyConfig` extension — `single/ClusterMachineAndRecipeTuner.scala`
Add `minWorkersForCluster(numWorkers) = numWorkers` (always-on primary floor) alongside the existing `maxWorkersForCluster`. (Kept trivial now; a single seam to swap in real policy data later.)

### 3. Single-tuner emit — `daJson` / `manualJson`
- Add to `clusterConf`: `min_workers`, `max_workers`, `cluster_scaled_max_cores`, `cluster_scaled_max_memory_gb`.
- For each recipe: call `CapacityGuard.guard` with the worker machine's `cores`/`memoryGb` and `max_workers`; write clamped executor settings, the two `*UsagePct` fields, and any `capacity*` flags.
- New param `maxClusterUtilRatio: Double = 0.90` threaded into both emitters (from the single-tuner CLI flag / strategy default).

### 4. AutoTuner final pass — `auto/ClusterMachineAndRecipeAutoTuner.scala`
- New `applyCapacityGuard(clusterName, outputDir, ratio)` modeled on `applyTrendScaling`: parse each `-auto-scale-tuned.json` / `-manually-tuned.json`, read `cluster_scaled_max_cores`/`_memory_gb` and `max_workers` from `clusterConf` (deriving `nodeCores = scaledMaxCores / maxWorkers`, `nodeMemGb = scaledMaxMemGb / maxWorkers`), run `CapacityGuard.guard` per recipe, rewrite executor settings + `%` + flags.
- Called **after** `applyTrendScaling` and `applyExecutorScaling` at **both** evolution call sites. Always runs (it also refreshes `%` that trend/z-score changes would otherwise leave stale).
- CLI: `--max-cluster-util-ratio` (Scallop `Double`, default 0.90, validate `0 < r <= 1`).
- Optional: a `capacity_clamped_count` in the generation summary's clusterConf or boost-summary for at-a-glance alerting (stretch; additive).

### 5. Frontend heatmap — `frontend/app.js` + `frontend/style.css`
- New `renderClusterUtilHeatmap(clusterConfig)` inserted in the cluster detail render **between** `renderClusterDetailBoosts` output (`detail-cluster-boosts`) and the cluster-conf table.
- Two stacked full-width bands: **CORES** then **MEMORY**, recipes sorted by filename in the same order so a job shares its column index across both bands (vertical scan).
- Cell color tiers: `<60%` green · `60–80%` amber · `80–<ceiling` orange · `capacityClamped || ≥ceiling` red · `capacityInfeasible` hatched red.
- Zoom −/+ control adjusting a CSS cell-size variable, persisted via a URL param (`?heatZoom=`), consistent with the existing `divSort`/`divDir` URL-state pattern.
- Hover tooltip: recipe short name · cores% · mem% · executor count. Click: reuse the existing `data-recipe` mechanism to scroll to and flash-highlight that recipe's conf row.
- Graceful absence: recipes/clusters without `*UsagePct` (legacy outputs) render an empty-state note instead of the bands.

## Data shape (example)

```json
"clusterConf": {
  "mock-cluster-x": {
    "num_workers": 5,
    "min_workers": 5,
    "max_workers": 6,
    "worker_machine_type": "n2d-standard-48",
    "cluster_max_total_cores": 240,
    "cluster_max_total_memory_gb": 960,
    "cluster_scaled_max_cores": 288,
    "cluster_scaled_max_memory_gb": 1152
  }
},
"recipeSparkConf": {
  "_RDM_DEGRADED_COMPANION.json": {
    "parallelizationFactor": 5,
    "sparkOptsMap": { "spark.dynamicAllocation.maxExecutors": "12", "spark.executor.cores": "4", "spark.executor.memory": "8g", ... },
    "total_executor_minimum_allocated_memory_gb": 48,
    "total_executor_maximum_allocated_memory_gb": 96,
    "maxCoreUsagePct": 16.7,
    "maxMemoryUsagePct": 8.3
  }
}
```

## Testing

- **`CapacityGuardSpec` (pure, primary):** scaled-max; aggregate-vs-packing divergence (the 4c/18GB wedge case); both-resource binding; manual vs DA; min/initial pulled down under clamp; `capacityClamped`/`Tight`/`Infeasible` flags; ratio override; idempotence; round-half rounding of `%`.
- **Single-tuner emit:** `daJson`/`manualJson` include the new clusterConf + recipe fields; a near-capacity recipe is clamped.
- **AutoTuner:** `applyCapacityGuard` clamps a trend/z-score-inflated recipe; `%` refreshed; idempotent on re-run; runs after the scale passes.
- **oss-mock:** a `capacityPressure` scenario (memory-heavy executor whose requested max exceeds 90% per-node) proving end-to-end clamp + red heatmap cell (per the oss-mock-data skill).
- **No-regression:** existing tuner / refinement / auto / mock specs stay green (new fields are additive).

## Phasing

1. **Backend** — `CapacityGuard` + policy min/max + single-tuner emit + AutoTuner pass + CLI + tests.
2. **Frontend** — heatmap render + styles + zoom/hover/click + oss-mock scenario + visual QA.

Each phase is independently shippable; the backend produces the data the frontend consumes.

## Open questions / risks

- **Sample-output regeneration:** committed `2099_*` sample JSONs predate these fields. The plan regenerates them (or the frontend tolerates their absence — both handled).
- **memoryOverhead exactness:** the % uses `spark.executor.memory` only (consistent with existing `total_executor_*` fields); the 0.90 ratio is the explicit headroom for overhead + daemons. If real-world wedging persists, lower the ratio rather than model overhead per-recipe.
- **Driver/AM:** not counted in the per-recipe %; the ratio reserves for it. Acceptable for a "last/only job" worst-case view.
