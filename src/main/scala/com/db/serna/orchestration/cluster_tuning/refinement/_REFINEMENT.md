# Cluster Tuning Refinement

Post-tuning refinement layer that reads the JSON configs produced by `ClusterMachineAndRecipeTuner` and applies targeted "vitamin" boosts based on diagnostic SQL outputs (CSVs).

## Overview

The base tuner generates optimal Spark configurations per cluster. However, some jobs may still fail at runtime (e.g. OOM, container kills). The refinement app detects these failures from diagnostic log analytics queries and boosts the affected Spark settings.

```mermaid
flowchart LR
    subgraph "Base Tuner"
        A[BigQuery Log Analytics] --> B[ClusterMachineAndRecipeTuner]
        B --> C["outputs/<date>/*.json"]
    end

    subgraph "Refinement Layer"
        D["inputs/<date>/b16_oom.csv"] --> E[RefinementPipeline]
        C --> E
        E --> C
    end

    style C fill:#d4edda,stroke:#28a745
```

The refinement app **overwrites the original tuned JSONs in-place** rather than creating a separate output directory. This keeps a single source of truth for downstream consumers.

## Vitamin Pipeline

Each vitamin is a modular diagnostic that:
1. **Loads** signals from a CSV (e.g. `b16_oom_job_driver_exceptions.csv`)
2. **Detects** affected recipes within a cluster
3. **Computes** boosts (e.g. `spark.executor.memory: 8g -> 12g`)
4. **Applies** boosts to recipe configs, adding tracking fields

Vitamins are chained sequentially — each vitamin's output feeds the next:

```mermaid
flowchart TD
    A[Parsed Tuned JSON] --> B[Vitamin 1: MemoryHeapBoost]
    B --> C[Vitamin 2: MemoryOverheadBoost]
    C --> D[Vitamin N: future]
    D --> E[Refined JSON Output]

    B -- "b16 CSV" --> B
    C -- "b17 CSV" --> C
```

## Available Vitamins

| Vitamin | CSV Source | Spark Property | Trigger | Status |
|---------|-----------|---------------|---------|--------|
| `MemoryHeapBoostVitamin` | `b16_oom_job_driver_exceptions.csv` | `spark.executor.memory` | `java.lang.OutOfMemoryError: Java heap space` | Active |
| `MemoryOverheadBoostVitamin` | `b17` (future) | `spark.executor.memoryOverhead` | Container killed (off-heap) | Planned |
| `GCPressureBoostVitamin` | `b19` (future) | `spark.executor.memory` + GC opts | GC time > 10% of task time | Planned |
| `ShuffleSpillBoostVitamin` | `b18` (future) | `spark.sql.shuffle.partitions` | Excessive shuffle spill to disk | Planned |
| `BroadcastTimeoutBoostVitamin` | `b20` (future) | `spark.sql.broadcastTimeout` | BroadcastExchangeExec timeout | Planned |

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

Added per affected recipe alongside `parallelizationFactor`:

```json
"_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json": {
  "parallelizationFactor": 5,
  "appliedMemoryHeapBoostFactor": 1.5,
  "sparkOptsMap": {
    "spark.executor.memory": "12g",
    ...
  }
}
```

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
    class RefinementVitamin {
        <<trait>>
        +name: String
        +counterKey: String
        +listKey: String
        +boostFieldKey: String
        +loadSignals(inputDir, clusterName): Seq~VitaminSignal~
        +computeBoosts(signals, recipes): Seq~VitaminBoost~
        +applyBoosts(boosts, recipes): Map
    }

    class MemoryHeapBoostVitamin {
        +boostFactor: Double
        +name = "b16_memory_heap_boost"
        +counterKey = "boostedMemoryHeapJobCount"
    }

    class MemoryOverheadBoostVitamin {
        +boostFactor: Double
        +counterKey = "boostedMemoryOverheadJobCount"
    }

    RefinementVitamin <|-- MemoryHeapBoostVitamin
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

    MemoryHeapBoostVitamin ..> MemoryHeapOomSignal
    MemoryHeapBoostVitamin ..> MemoryHeapBoost
    VitaminSignal <|-- MemoryHeapOomSignal
    VitaminBoost <|-- MemoryHeapBoost
```

## File Layout

```
refinement/
  ClusterMachineAndRecipeTunerRefinement.scala   # Main app + Scallop CLI
  RefinementVitamins.scala                        # Trait + signals + boosts + pipeline
  SimpleJsonParser.scala                          # JSON reader for tuned configs
  _REFINEMENT.md                                  # This file
```
