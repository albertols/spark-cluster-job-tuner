# Cluster Tuning Refinement

Post-tuning refinement layer that reads the JSON configs produced by `ClusterMachineAndRecipeTuner` and applies targeted "vitamin" boosts based on diagnostic SQL outputs (CSVs).

## Overview

The base tuner generates optimal Spark configurations per cluster. However, some jobs may still fail at runtime (e.g. OOM, container kills). The refinement app detects these failures from diagnostic log analytics queries and boosts the affected Spark settings.

```mermaid
flowchart LR
  classDef document fill:#9aa2ab,stroke:#3a4046,color:#1d1f23
  classDef process  fill:#9b59b6,stroke:#5a2d6e,color:#fff
  classDef cloud    fill:#4ea1ff,stroke:#1a4f8a,color:#fff
  classDef inputBoundary  stroke-width:3px,stroke-dasharray:5 3
  classDef outputBoundary stroke-width:3px

  subgraph BASE ["Base Tuner"]
    A[fa:fa-cloud BigQuery Log Analytics]:::cloud
    B[fa:fa-cog ClusterMachineAndRecipeTuner]:::process
    C[fa:fa-file-code outputs/&lt;date&gt;/*.json]:::document
  end

  subgraph REF ["Refinement Layer"]
    D[fa:fa-file-csv inputs/&lt;date&gt;/b16_oom.csv]:::document
    E[fa:fa-cog RefinementPipeline]:::process
  end

  A --> B --> C
  D --> E
  C --> E
  E --> C
  class D inputBoundary
  class C outputBoundary
```

The refinement app **overwrites the original tuned JSONs in-place** rather than creating a separate output directory. This keeps a single source of truth for downstream consumers.

## Vitamin Pipeline

Each vitamin is a modular diagnostic that:
1. **Loads** signals from a CSV (e.g. `b16_oom_job_driver_exceptions.csv`) — including rows with empty `recipe_filename`
2. **Resolves** missing recipe names via `RecipeResolver` (see below)
3. **Computes** boosts (e.g. `spark.executor.memory: 8g -> 12g`)
4. **Applies** boosts to recipe configs, adding tracking fields
5. **Reports** unresolved signals to `_not_boosted_recipes.json`

Vitamins are chained sequentially — each vitamin's output feeds the next:

```mermaid
flowchart TD
  classDef document fill:#9aa2ab,stroke:#3a4046,color:#1d1f23
  classDef process  fill:#9b59b6,stroke:#5a2d6e,color:#fff
  classDef inputBoundary  stroke-width:3px,stroke-dasharray:5 3
  classDef outputBoundary stroke-width:3px

  A[fa:fa-file-code Parsed Tuned JSON]:::document
  R[fa:fa-cog RecipeResolver]:::process
  B[fa:fa-cog Vitamin 1: MemoryHeapBoost]:::process
  U[fa:fa-file-code _not_boosted_recipes.json]:::document
  C[fa:fa-cog Vitamin 2: MemoryOverheadBoost]:::process
  D[fa:fa-cog Vitamin N: future]:::process
  E[fa:fa-file-code Refined JSON Output]:::document

  A --> R
  R -->|"resolved"| B
  R -->|"unresolved"| U
  B --> C
  C --> D
  D --> E
  B -->|"b16 CSV"| R
  C -->|"b17 CSV"| R

  class A inputBoundary
  class U,E outputBoundary
```

## Recipe Resolution

Diagnostic CSVs sometimes have empty `recipe_filename` fields (the log analytics query couldn't join the job to its recipe). `RecipeResolver` attempts two strategies before marking an entry as unresolved:

**Strategy 1 — Sibling lookup:** Find another CSV row with the same job prefix (after stripping the `-YYYYMMDD-HHMM` date suffix) that has a known `recipe_filename`.

```
etl-m-dwh-d-otros-bienes-po-update-20260410-0517  →  (empty recipe)
etl-m-dwh-d-otros-bienes-po-update-20260409-0514  →  _ETL_m_DWH_D_OTROS_BIENES_PO_UPDATE.json
                                                      ↑ same prefix — use this recipe
```

**Strategy 2 — Job-id derivation:** Strip date suffix, replace `-` with `_`, uppercase, and match case-insensitively against recipes in the cluster's `recipeSparkConf`.

```
etl-m-dq3-ods-f-gr-garantia-20260411-0438
  → strip suffix  → etl-m-dq3-ods-f-gr-garantia
  → replace -→_   → etl_m_dq3_ods_f_gr_garantia
  → uppercase     → ETL_M_DQ3_ODS_F_GR_GARANTIA
  → matches       → _ETL_m_DQ3_ODS_F_GR_GARANTIA.json
```

## Unresolved Report

Signals that cannot be matched to any recipe produce `_not_boosted_recipes.json` in the output directory:

```json
{
  "b16_memory_heap_boost": {
    "csv_source": "inputs/2025_12_20/b16_oom_job_driver_exceptions.csv",
    "unresolved_count": 1,
    "entries": [
      {
        "job_id": "unknown-job-20260411-0438",
        "cluster_name": "cluster-wf-dmr-load-t-03-25-0325",
        "raw_recipe_filename": "",
        "latest_driver_log_ts": "2026-04-11T02:59:10Z",
        "latest_driver_message": "OOM"
      }
    ]
  }
}
```

This file is grouped by vitamin name, with the full CSV source path and all unmatched entries. Future vitamins (b17, b18, etc.) will add their own sections.

## Available Vitamins

| Vitamin | Source | Spark Property | Trigger | Status |
|---------|--------|---------------|---------|--------|
| `MemoryHeapBoostVitamin` | `b16_oom_job_driver_exceptions.csv` | `spark.executor.memory` | `java.lang.OutOfMemoryError: Java heap space` | Active |
| `ExecutorScaleVitamin` | **Derived** — `divergences_current_snapshot` from the AutoTuner (NOT a CSV) | `spark.dynamicAllocation.maxExecutors` | High positive z-score on `avg/p95_job_duration_ms` for a paired (non-new) recipe whose `p95_run_max_executors` is cap-touching | Active (AutoTuner only) |
| `MemoryOverheadBoostVitamin` | `b17` (future) | `spark.executor.memoryOverhead` | Container killed (off-heap) | Planned |
| `GCPressureBoostVitamin` | `b19` (future) | `spark.executor.memory` + GC opts | GC time > 10% of task time | Planned |
| `ShuffleSpillBoostVitamin` | `b18` (future) | `spark.sql.shuffle.partitions` | Excessive shuffle spill to disk | Planned |
| `BroadcastTimeoutBoostVitamin` | `b20` (future) | `spark.sql.broadcastTimeout` | BroadcastExchangeExec timeout | Planned |

`ExecutorScaleVitamin` is the first vitamin whose signal source is **derived** (in-memory, divergence-driven) rather than CSV-driven. The vitamin's `loadSignals(inputDir, clusterName)` ignores `inputDir` and pulls signals from a constructor-injected `signalsForCluster` lookup populated by the AutoTuner. It is wired only in the AutoTuner path (not in the standalone refinement app's pipeline).

## Boost State Lifecycle

When the pipeline is fed multiple input directories (the AutoTuner's `[curDate, refDate]` pattern) it routes signals through the date-aware 3-arg `computeBoosts` overload, producing one of three states per recipe:

| State | Trigger | What `applyBoosts` does |
|---|---|---|
| `New` | recipe has no prior factor in `extraFields` AND a fresh signal exists in current_date | apply factor, update spark setting, update derived totals, stamp factor |
| `ReBoost` | recipe has a prior factor AND a fresh signal in current_date | factor stacks (`prior × factor`); spark setting × factor on top of the already-boosted value; totals re-derived; stamped cumulative factor |
| `Holding` | recipe has a prior factor AND no fresh signal in current_date | spark setting and totals untouched; cumulative factor preserved (the boost is succeeding) |

Single-tuner / single-inputDir runs only ever emit `New` (the legacy 2-arg `computeBoosts` does not have date awareness). The AutoTuner's `applyB16Reboosting` and `applyExecutorScaling` always pass two input dirs to force the date-aware path — see `_AUTO_TUNING.md` for the full flow including the `BoostMetadataCarrier` step that makes `Holding` / `ReBoost` work even when the cluster is re-planned from scratch.

## Output Changes

### Cluster-level: boost counters

Added to `clusterConf` — one counter per vitamin type:

```json
"clusterConf": {
  "cluster-wf-dmr-load-t-02-15-0215": {
    "num_workers": 10,
    "worker_machine_type": "n2d-highcpu-48",
    ...
    "boostedMemoryHeapJobCount": 1,
    "boostedMemoryHeapJobList": ["_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json"],
    "boostedMemoryOverheadJobCount": 0,
    "boostedMemoryOverheadJobList": []
  }
}
```

### Recipe-level: boost factor

Added per affected recipe alongside `parallelizationFactor`. Each vitamin owns its own field so multiple boosts can coexist on the same recipe (and round-trip through `SimpleJsonParser.extractRecipes` into `RecipeConfig.extraFields`):

```json
"_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json": {
  "parallelizationFactor": 5,
  "appliedMemoryHeapBoostFactor": 2.25,
  "appliedExecutorScaleFactor": 1.5,
  "sparkOptsMap": {
    "spark.executor.memory": "18g",
    "spark.dynamicAllocation.maxExecutors": "21",
    ...
  }
}
```

| Field | Owner | Stored value |
|---|---|---|
| `appliedMemoryHeapBoostFactor` | `MemoryHeapBoostVitamin` | Cumulative factor (e.g. 1.5 → 2.25 → 3.375 across `New` → `ReBoost` → `ReBoost`). On `Holding` runs the value is preserved as-is. |
| `appliedExecutorScaleFactor` | `ExecutorScaleVitamin` | Same shape, applied to `spark.dynamicAllocation.maxExecutors`. Lives only on DA recipes (manual recipes are skipped). |

## CLI Usage

```bash
# Basic: refine with default heap boost factor (1.5x)
--referenceTuningDate=2025_12_20

# Custom heap boost factor
--referenceTuningDate=2025_12_20 --memoryHeapBoostFactor=2.0

# Future: with memory overhead boost
--referenceTuningDate=2025_12_20 --memoryHeapBoostFactor=1.5 --memoryOverheadBoostFactor=1.5
```

Run from IntelliJ: set main class to `com.db.serna.orchestration.cluster_tuning.refinement.ClusterMachineAndRecipeTunerRefinement` with program arguments.

## Adding a New Vitamin

1. Define signal and boost case classes extending `VitaminSignal` and `VitaminBoost`
2. Implement `RefinementVitamin` trait with `name`, `counterKey`, `listKey`, `boostFieldKey`
3. Register in `buildVitaminPipeline()` in `ClusterMachineAndRecipeTunerRefinement`
4. Add CLI option in `RefinementConf` if configurable
5. Add tests in a new spec or extend `RefinementVitaminsSpec`

```mermaid
classDiagram
  classDef process        fill:#9b59b6,stroke:#5a2d6e,color:#fff
  classDef outcomeNeutral fill:#2a2a3a,stroke:#7f8c8d,color:#d5d8dc

  class RefinementVitamin {
      <<trait>>
      +name: String
      +csvFileName: String
      +counterKey: String
      +listKey: String
      +boostFieldKey: String
      +loadSignals(inputDir, clusterName): Seq~VitaminSignal~
      +computeBoosts(signals, recipes): Seq~VitaminBoost~
      +computeBoosts(signals, recipes, currentSignals): Seq~VitaminBoost~  «date-aware 3-arg overload»
      +applyBoosts(boosts, recipes): Map
  }

  class MemoryHeapBoostVitamin {
      +boostFactor: Double
      +name = "b16_memory_heap_boost"
      +csvFileName = "b16_oom_job_driver_exceptions.csv"
      +boostFieldKey = "appliedMemoryHeapBoostFactor"
  }

  class ExecutorScaleVitamin {
      +boostFactor: Double
      +signalsForCluster: String =~ Seq~ExecutorScaleSignal~
      +name = "executor_scale_up"
      +csvFileName = "(divergence-driven, no CSV)"
      +boostFieldKey = "appliedExecutorScaleFactor"
  }

  class MemoryOverheadBoostVitamin {
      +boostFactor: Double
      +counterKey = "boostedMemoryOverheadJobCount"
  }

  RefinementVitamin <|-- MemoryHeapBoostVitamin
  RefinementVitamin <|-- ExecutorScaleVitamin
  RefinementVitamin <|-- MemoryOverheadBoostVitamin

  class VitaminSignal {
      <<sealed trait>>
      +clusterName: String
      +recipeFilename: String
  }

  class VitaminBoost {
      <<sealed trait>>
      +recipeFilename: String
  }

  class BoostState {
      <<sealed trait>>
      New
      ReBoost
      Holding
  }

  MemoryHeapBoostVitamin ..> MemoryHeapOomSignal
  MemoryHeapBoostVitamin ..> MemoryHeapBoost
  ExecutorScaleVitamin ..> ExecutorScaleSignal
  ExecutorScaleVitamin ..> ExecutorScaleBoost
  VitaminSignal <|-- MemoryHeapOomSignal
  VitaminSignal <|-- ExecutorScaleSignal
  VitaminBoost <|-- MemoryHeapBoost
  VitaminBoost <|-- ExecutorScaleBoost
  MemoryHeapBoost --> BoostState
  ExecutorScaleBoost --> BoostState

  cssClass "RefinementVitamin,MemoryHeapBoostVitamin,ExecutorScaleVitamin,MemoryOverheadBoostVitamin" process
  cssClass "VitaminSignal,VitaminBoost,BoostState,MemoryHeapOomSignal,ExecutorScaleSignal,MemoryHeapBoost,ExecutorScaleBoost" outcomeNeutral
```

## File Layout

```
refinement/
  ClusterMachineAndRecipeTunerRefinement.scala   # Standalone app + Scallop CLI (b16 only by default)
  RefinementVitamins.scala                        # RefinementVitamin trait + BoostState lifecycle
                                                  #   Signals: MemoryHeapOomSignal, ExecutorScaleSignal
                                                  #   Boosts:  MemoryHeapBoost, ExecutorScaleBoost
                                                  #   Vitamins: MemoryHeapBoostVitamin (CSV), ExecutorScaleVitamin (derived)
                                                  #   RefinementPipeline.refine() — multi-dir signal aggregation,
                                                  #   dedupe by recipe (curDate wins), 2-arg vs 3-arg routing
                                                  #   RefinementPipeline.toRefinedJson() — serializer that round-trips
                                                  #   extraFields (boost factors)
  SimpleJsonParser.scala                          # JSON reader for tuned configs;
                                                  #   carries appliedMemoryHeapBoostFactor + appliedExecutorScaleFactor
                                                  #   into RecipeConfig.extraFields
  _REFINEMENT.md                                  # This file
```

### Where `ExecutorScaleVitamin` is wired

It is **not** part of `ClusterMachineAndRecipeTunerRefinement`'s default `buildVitaminPipeline()` — running the standalone refinement app is still b16-only. The AutoTuner instantiates it directly inside `applyExecutorScaling` (closure over the per-cluster signal map), runs `RefinementPipeline.refine` with two input dirs (forces date-aware path), and writes the refined JSON back. See `_AUTO_TUNING.md` § "Z-score-driven executor scale-up".
