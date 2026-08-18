# Cluster Tuner — Design Document

> **Scope:** Architecture, data flow, and extension points for the `cluster_tuning` module
> after the Strategy-pattern refactoring (March 2026).

---

## 1. Module Overview

Four Scala source files, each with a single responsibility:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        cluster_tuning/                                  │
│                                                                         │
│  ┌──────────────────────┐   ┌──────────────────────────────────────┐   │
│  │  TuningStrategies    │   │  ClusterMachineAndRecipeTuner         │   │
│  │                      │   │                                      │   │
│  │  ExecutorTopology    │──▶│  MachineCatalog  PriceCatalog        │   │
│  │  BiasMode            │   │  Csv  Json  Sizing                   │   │
│  │  MachinePreference   │   │                                      │   │
│  │  Quotas              │   │  run()  planCluster()                │   │
│  │  TuningStrategy      │   │  chooseMachines()                    │   │
│  │  DefaultStrategy     │   │  planManualRecipes()                 │   │
│  │  CostBiasedStrategy  │   │  planDARecipes()                     │   │
│  │  PerfBiasedStrategy  │   │  manualJson()  daJson()              │   │
│  └──────────────────────┘   │  writeSummaryCsv*()                  │   │
│                             │  AutoscalingPolicyConfig             │   │
│  ┌──────────────────────┐   └──────────────────────────────────────┘   │
│  │  ClusterDiagnostics  │                   │                          │
│  │                      │◀──────────────────┘                          │
│  │  ExitCodeRecord      │                   │                          │
│  │  DiagnosticSignal    │   ┌──────────────────────────────────────┐   │
│  │  YarnDriverEviction  │   │  GenerationSummary                   │   │
│  │  NonZeroExitPattern  │   │                                      │   │
│  │  DriverResourceOver- │──▶│  QuotaTracker                        │   │
│  │    ride              │   │  GenerationSummaryEntry              │   │
│  │  Diagnostics-        │   │  GenerationSummary                   │   │
│  │    Processor         │   │  GenerationSummaryWriter             │   │
│  └──────────────────────┘   └──────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. End-to-End Data Flow

```
  ┌────────────────────────────────────────────────────────────┐
  │                     INPUT LAYER                            │
  │                                                            │
  │  b13_recommendations_inputs_per_recipe_per_cluster.csv     │
  │   (or individual b1/b3/b5/b8/b11/b12 CSVs)                │
  │  b14_clusters_with_nonzero_exit_codes.csv  ←── diagnostics │
  │  _dag_cluster-relationship-map.csv                         │
  │  _dag_cluster_creation_time.csv                            │
  └─────────────────────────────┬──────────────────────────────┘
                                │ Csv.parse()
                                ▼
  ┌────────────────────────────────────────────────────────────┐
  │                   METRICS LAYER                            │
  │                                                            │
  │  Map[(cluster_name, recipe_filename) → RecipeMetrics]      │
  │    .avgExecutorsPerJob    .p95RunMaxExecutors               │
  │    .avgJobDurationMs      .p95JobDurationMs                 │
  │    .fractionReachingCap   .maxConcurrentJobs                │
  └────────────────┬───────────────────────┬───────────────────┘
                   │                       │
                   │ groupBy(cluster)      │ ClusterDiagnosticsProcessor
                   ▼                       ▼
  ┌────────────────────────┐   ┌──────────────────────────────┐
  │    PLANNING LAYER      │   │    DIAGNOSTICS LAYER         │
  │                        │   │                              │
  │  planCluster()         │   │  loadExitCodes(b14)          │
  │    chooseMachines()    │   │  detectSignals()             │
  │      ← QuotaTracker    │   │  computeOverrides()          │
  │    → ClusterPlan       │   │  → Map[cluster →             │
  │                        │   │      DriverResourceOverride] │
  │  planManualRecipes()   │   └──────────────┬───────────────┘
  │  planDARecipes()       │                  │
  └────────────────┬───────┘                  │
                   │                          │
                   └──────────┬───────────────┘
                              ▼
  ┌────────────────────────────────────────────────────────────┐
  │                    OUTPUT LAYER                            │
  │                                                            │
  │  Per-cluster JSON configs (manually-tuned + auto-scale)    │
  │  _clusters-summary.csv              (by workers desc)      │
  │  _clusters-summary-only-clusters-wf.csv                    │
  │  _clusters-summary_top_jobs.csv                            │
  │  _clusters-summary_num_of_workers.csv                      │
  │  _clusters-summary_estimated_cost_eur.csv                  │
  │  _clusters-summary_total_active_minutes.csv                │
  │  _clusters-summary_global_cores_and_machines.csv           │
  │  _generation_summary.json           (new)                  │
  │  _generation_summary.csv            (new)                  │
  └────────────────────────────────────────────────────────────┘
```

---

## 3. Strategy Pattern

`run()` receives a [`TuningStrategy`](/src/main/scala/com/db/serna/orchestration/cluster_tuning/single/TuningStrategies.scala) and derives a `TuningPolicy` from it. Every downstream
method takes `policy: TuningPolicy` explicitly — no global state.

```
  CLI args
  ──────────────────────────────────────────────────────────────
  --strategy=default | cost_biased | performance_biased
  --topology=8cx1GBpc | 8cx2GBpc | 8cx4GBpc | 4cx1GBpc | ...
  ──────────────────────────────────────────────────────────────
                         │
                         ▼
              ┌──────────────────────┐
              │    TuningStrategy    │  (trait)
              │                      │
              │  executorTopology    │──▶ ExecutorTopologyPreset
              │  biasMode            │──▶ BiasMode (weights)
              │  machinePreference   │──▶ MachineSelectionPreference
              │  quotas              │──▶ Quotas
              │  capHitBoostPct      │
              │  preferMaxWorkers    │
              │  ...                 │
              │                      │
              │  toTuningPolicy()    │──▶ TuningPolicy  (adapter)
              └──────────────────────┘
                         │
              ┌──────────┼─────────────────────────┐
              │          │                         │
              ▼          ▼                         ▼
  ┌───────────────┐  ┌──────────────────┐  ┌───────────────────┐
  │  Default      │  │  CostBiased      │  │  PerformanceBiased│
  │               │  │                  │  │                   │
  │ 8cx1GBpc      │  │ 8cx1GBpc         │  │ 8cx2GBpc          │
  │ Balance bias  │  │ CostBiased bias  │  │ PerfBiased bias   │
  │ maxWorkers=6  │  │ maxWorkers=4     │  │ maxWorkers=8      │
  │ buffer=25%    │  │ buffer=10%       │  │ buffer=40%        │
  │ boost=20%     │  │ boost=10%        │  │ boost=30%         │
  └───────────────┘  └──────────────────┘  └───────────────────┘

  ExecutorTopologyPreset semantics:
  ┌────────────┬──────┬──────────────┬──────────────────────────────┐
  │ label      │cores │ memPerCoreGB │ totalMemGB (→executorMemoryGb)│
  ├────────────┼──────┼──────────────┼──────────────────────────────┤
  │ 8cx1GBpc   │  8   │      1       │   8  ← default (current)     │
  │ 8cx2GBpc   │  8   │      2       │  16                          │
  │ 8cx4GBpc   │  8   │      4       │  32                          │
  │ 4cx1GBpc   │  4   │      1       │   4                          │
  │ 4cx2GBpc   │  4   │      2       │   8                          │
  └────────────┴──────┴──────────────┴──────────────────────────────┘
```

---

## 4. Machine Selection Pipeline (chooseMachines)

```
  requiredSlotsRaw = targetExecPerJob × maxConcurrentJobs
  reqSlots = requiredSlotsRaw × (1 + concurrencyBufferPct)
                                         │
                                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │               MachineCatalog.defaults (all families)        │
  │   E2, N2, N2D, C3, C4, N4, N4D — standard/highmem/highcpu  │
  └──────────────────────────┬──────────────────────────────────┘
                             │ filter: excludedFamilies (N4, N4D)
                             │ filter: allowedFamilies  (N2, N2D, E2, C3, C4)
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                  Candidate pool                             │
  │                                                             │
  │  For each worker machine w:                                 │
  │    epw       = executorsPerWorker(w, corePref, memGb, ...)  │
  │    workers   = ceil(reqSlots / epw), min 2                  │
  │    cost      = penalizedHourlyCost(w, workers)              │
  │    quotaOk   = QuotaTracker.withinQuota(w, workers, pref)   │
  └──────────────────────────┬──────────────────────────────────┘
                             │ Score each candidate:
                             │
                             │  score = costWeight × normCost
                             │        + workerPenaltyWeight × normWorkers
                             │        − sufficiencyWeight × coreSufficiency
                             │        − utilizationWeight × utilization
                             │        + capacityPenalty   (if slots < required)
                             │        + oversizePenalty   (if oversize > 1.5×)
                             │        − 0.10              (if cores == 32 ← bonus)
                             │
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  admissible = scored.filter(quotaOk)                        │
  │  pool = if admissible.nonEmpty then admissible else scored   │  ← soft fallback
  │                                                             │
  │  best = pool.sortBy(score, familyPriority).head             │
  │           familyPriority: N2=1 > N2D=2 > E2=3 > C3=4 > C4=5│
  └──────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
               (workerType, masterType, workers, epw)
```

---

## 5. Quota Enforcement

```
  QuotaTracker  (one instance per run(), mutable)
  ┌────────────────────────────────────────────────┐
  │  usedCores: Map[family → Int]   (accumulated)  │
  │  c3ClusterCount: Int                           │
  │  c4ClusterCount: Int                           │
  └─────────────────────┬──────────────────────────┘
                        │
       withinQuota()    │    recordCluster()
       ────────────────►│◄────────────────────────
       (before scoring) │    (after best chosen)
                        │
  Decision tree for withinQuota(machine, workers, pref):
  ┌─────────────────────────────────────────────────────┐
  │                                                     │
  │  family in excludedFamilies?  ──YES──▶  false       │
  │         │                                           │
  │         NO                                          │
  │         ▼                                           │
  │  family=c3 AND c3Count >= c3MaxClusters? ─YES─▶ false│
  │         │                                           │
  │         NO                                          │
  │         ▼                                           │
  │  family=c4 AND c4Count >= c4MaxClusters? ─YES─▶ false│
  │         │                                           │
  │         NO                                          │
  │         ▼                                           │
  │  usedCores[family] + machine.cores*workers          │
  │    > quotas.forFamily(family)?  ──YES──▶  false     │
  │         │                                           │
  │         NO                                          │
  │         ▼                                           │
  │        true                                         │
  └─────────────────────────────────────────────────────┘

  Default quotas (europe-west3 approximations):
  ┌──────┬───────┐  Priority guide:
  │ e2   │ 5000  │  1st choice: N2-32,  N2D-32,  E2-32
  │ n2   │ 5000  │  exceptional: C3 (max 1 cluster), C4 (max 1 cluster)
  │ n2d  │ 3000  │  avoided:    N4,  N4D  (excluded entirely)
  │ c3   │  500  │
  │ c4   │  500  │
  │ n4   │  500  │
  │ n4d  │  500  │
  └──────┴───────┘
```

---

## 6. Diagnostics Pipeline (b14)

```
  b14_clusters_with_nonzero_exit_codes.csv
  ┌────────────────────────────────────────────────────────┐
  │ timestamp, job_id, cluster_name,  driver_exit_code, msg│
  │                    ^^^^^^^^^^^                         │
  │              triple-double-quoted in CSV               │
  │              → strip with replaceAll('"', "")          │
  └───────────────────────┬────────────────────────────────┘
                          │ ClusterDiagnosticsProcessor.loadExitCodes()
                          ▼
            Seq[ExitCodeRecord]
                          │
                          │ ClusterDiagnosticsProcessor.detectSignals()
                          │  group by clusterName
                          │  exit 247 → YarnDriverEviction(count, jobs)
                          │  other    → NonZeroExitPattern(code, count)
                          ▼
      Map[clusterName → Seq[DiagnosticSignal]]
                          │
                          │ ClusterDiagnosticsProcessor.computeOverrides()
                          │  filter: only clusters with YarnDriverEviction
                          │  heuristic: driverMemoryGb = base + 4
                          │             driverCores    = max(base, 4)
                          ▼
      Map[clusterName → DriverResourceOverride]
                          │
           ┌──────────────┴──────────────┐
           │                             │
           ▼                             ▼
  manualJson(... driverOverride)   daJson(... driverOverride)
  ┌─────────────────────┐
  │ clusterConf {       │
  │   num_workers: ..   │
  │   ...               │
  │   driver_memory_gb: 8        ← injected if override present
  │   driver_cores: 4            ← injected if override present
  │   driver_memory_overhead_gb: ← injected if override present
  │   diagnostic_reason: "..."   ← audit trail
  │ }                   │
  └─────────────────────┘

  Extension points for future signals (stubbed):
  ┌──────────────────────────────────────────┐
  │  DiagnosticSignal (sealed trait)         │
  │    YarnDriverEviction    ← implemented   │
  │    NonZeroExitPattern    ← implemented   │
  │    MemoryHeapSignal      ← stub (future) │
  │    GcPressureSignal      ← stub (future) │
  └──────────────────────────────────────────┘
```

---

## 7. Generation Summary

```
  After all clusters are planned:

  summaryEntries: Seq[GenerationSummaryEntry]   (one per cluster)
  ┌──────────────────────────────────────────────────────┐
  │  clusterName       workerMachineType  workerFamily    │
  │  numWorkers        maxWorkersFromPolicy               │
  │  totalCores        maxTotalCores                     │
  │  diagnosticSignals (from allDiagnosticSignals map)   │
  │  strategyName      biasMode  topologyPreset           │
  └──────────────────────────────────────────────────────┘
                            │
                            ▼
  GenerationSummary (top-level aggregate)
  ┌──────────────────────────────────────────────────────┐
  │  generatedAt       date       strategyName           │
  │  biasMode          topologyPreset                    │
  │                                                      │
  │  totalClusters      = entries.size                   │
  │  totalPredictedNodes= Σ(numWorkers + 1)   ← +1 master│
  │  totalMaxNodes      = Σ(maxWorkersFromPolicy + 1)    │
  │                                                      │
  │  quotaUsageByFamily = QuotaTracker.usageSummary()    │
  │    { "n2": { used_cores: 128, quota_cores: 5000 },   │
  │      "e2": { used_cores: 32,  quota_cores: 5000 },   │
  │      ... }                                           │
  │                                                      │
  │  clustersWithDiagnosticOverrides = driverOverrides.size│
  └──────────────────────────────────────────────────────┘
                            │
              ┌─────────────┴────────────┐
              ▼                          ▼
  _generation_summary.json     _generation_summary.csv
  (pretty-printed)             (flat, one row per cluster)
```

---

## 8. Complete Output Map

```
  outputs/YYYY_MM_DD/
  │
  ├── <cluster-name>-manually-tuned.json          (one per cluster)
  │     clusterConf: num_workers, machine types, autoscaling_policy,
  │                  tuner_version, capacity envelope,
  │                  driver_* fields (only if b14 override applies)
  │     recipeSparkConf: spark.executor.instances/cores/memory
  │
  ├── <cluster-name>-auto-scale-tuned.json        (one per cluster)
  │     clusterConf: same as manual
  │     recipeSparkConf: dynamicAllocation.min/max/initial
  │
  ├── _clusters-summary.csv                       sorted: workers↓ jobs↓
  ├── _clusters-summary-only-clusters-wf.csv      filtered: cluster-wf-* only
  ├── _clusters-summary_top_jobs.csv              sorted: jobs↓
  ├── _clusters-summary_num_of_workers.csv        sorted: workers↓
  ├── _clusters-summary_estimated_cost_eur.csv    sorted: cost↓
  ├── _clusters-summary_total_active_minutes.csv  sorted: minutes↓
  ├── _clusters-summary_global_cores_and_machines.csv  aggregated by machine type
  │     MACHINE_TYPE, ESTIMATED_MAX_NO_OF_WORKERS, ESTIMATED_MAX_NO_OF_CORES,
  │     REAL_MAX_NO_OF_WORKERS, REAL_MAX_NO_OF_CORES, CLUSTERS_LIST
  │
  ├── _generation_summary.json   ← NEW: quota usage, node count prediction,
  │                                     per-cluster strategy/diagnostic info
  └── _generation_summary.csv    ← NEW: flat version of the above
```

---

## 9. Autoscaling Policy Bracket Map

```
  cluster.workers     autoscaling_policy                maxWorkers (cap)
  ─────────────────────────────────────────────────────────────────────
   1 – 4    →   "small-workload-autoscaling"    →   4   (cooldown 120s)
   5 – 6    →   "medium-workload-autoscaling"   →   6   (cooldown 120s)
   7 – 8    →   "large-workload-autoscaling"    →   8   (cooldown 120s)
   9 – 10   →   "extra-large-workload-autoscaling" → 10 (cooldown 300s)
  11+       →   "extra-large-workload-autoscaling" → n  (observed workers)
  ─────────────────────────────────────────────────────────────────────
  Used in: clusterConf.autoscaling_policy
           writeGlobalCoresAndMachinesCsv → REAL_MAX_NO_OF_WORKERS
           _generation_summary → maxWorkersFromPolicy / totalMaxNodes
```

---

## 10. Adding New Features (Extension Guide)

### New tuning strategy
```scala
object MyCustomStrategy extends TuningStrategy {
  val name = "my_custom"
  val biasMode = CostPerformanceBalance        // reuse existing or define new
  val executorTopology = ExecutorTopologyPreset(8, 2)
  val machinePreference = MachineSelectionPreference.Default
  val quotas = Quotas.Default
  // override only what differs from Default:
  val capHitBoostPct = 0.15
  ...
}
// Register in TuningStrategy.fromName() and TuningStrategy.all
```

### New diagnostic signal (e.g. GC pressure)
```scala
// 1. Add new LogAnalytics SQL query → new input CSV
// 2. Add case class extending DiagnosticSignal:
final case class GcPressureSignal(clusterName: String, ...) extends DiagnosticSignal
// 3. Add detection in ClusterDiagnosticsProcessor.detectSignals()
// 4. Add override computation in ClusterDiagnosticsProcessor.computeOverrides()
//    (or a new computeGcOverrides() for a different remedy type)
// 5. Inject into manualJson/daJson if it changes cluster config
```

### New executor topology preset
```scala
// In ExecutorTopologyPreset companion:
val Cores16MemPC1GB: ExecutorTopologyPreset = ExecutorTopologyPreset(16, 1)
// Add to fromLabel() match
// Add CLI label string in main()
```
