# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spark Cluster Job Tuner is a Scala 2.12 / Spark 3.5.3 toolkit for cost-optimizing Spark cluster configurations on GCP. It has three modules:

1. **Cluster Tuning** (`orchestration/cluster_tuning/`) — Recommends GCP machine types and Spark configs (executor count, memory, cores) from BigQuery Log Analytics CSV exports. Produces per-cluster JSON configs and summary CSVs.
2. **Cache Utilities** (`utils/spark/cache/`) — Estimates memory footprint of cached DataFrames and broadcast variables at runtime.
3. **Parallelism Utilities** (`utils/spark/parallelism/`) — Monitors executor lifecycle via Spark listeners and recommends parallelism/partition settings.

## Build & Test

**Build system:** Maven with Scala 2.12.18. The pom.xml does not include `scala-maven-plugin`, so Scala compilation is handled through IntelliJ IDEA (the primary development environment).

**Running tests from IntelliJ:** Right-click test classes/packages and run with ScalaTest runner. Tests use `local[1]` or `local[2]` Spark sessions with UI disabled.

**Test infrastructure:**
- `SparkTestSession` trait — mixin for `AnyFunSuite`, provides a lazy `spark` session with automatic cleanup
- `TestSparkSessionSupport` object — `withSession { spark => ... }` and `withCacheSession { spark => ... }` for one-off test sessions with predefined configs (`MinimalLocalConf`, `CacheConf`)
- Tests clear `spark.driver.port` after each suite to avoid port conflicts in the same JVM

## Architecture

### Source layout

All source lives under `com.db.serna`:

```
orchestration/cluster_tuning/
  ClusterMachineAndRecipeTuner.scala   # Main tuner (entry point)
  log_analytics/                       # 13 BigQuery SQL templates (b1–b13)

utils/spark/
  cache/
    MemorySizingUtils.scala            # Multi-view DataFrame memory reporting
    BroadcastSizingUtils.scala         # Broadcast payload sizing (Kryo/Java estimates)
    CacheLogging.scala                 # Structured JSON logging for cache events
  parallelism/
    ExecutorTrackingListener.scala     # SparkListener for executor add/remove events
    ParallelismUtils.scala             # Parallelism calculation and partition rebalancing
```

### Cluster Tuner data flow

1. **Input:** CSV files exported from BigQuery Log Analytics queries (single-file mode via `b13` recommended, or multi-file mode with individual `b1`–`b12` CSVs). Sample data in `src/main/resources/composer/dwh/config/cluster_tuning/inputs/`.
2. **Processing:** Load metrics by `(cluster_name, recipe_filename)` → select optimal machine type from 100+ GCP catalog entries → plan manual or auto-scale executor configs.
3. **Output:** Per-cluster JSON configs (`*-manually-tuned.json`, `*-auto-scale-tuned.json`), summary CSVs sorted by various dimensions, global cores/machines aggregation.

### Key design details

- Machine catalog covers E2, N1, N2, N2D, C3, C4, N4, N4D families with europe-west3 pricing
- Two tuning modes: **Manual** (fixed `spark.executor.instances`) and **Auto-scale** (`spark.dynamicAllocation.*`)
- `ExecutorTrackingListener` emits ISO-formatted JSON logs with `event_type` as the first field
- `MemorySizingUtils` provides three memory views: JVM estimate, RDD storage info, and executor storage accounting delta

## In-repo documentation

Each module has a detailed markdown doc alongside its source:
- `orchestration/cluster_tuning/_CLUSTER_TUNING.md` — Comprehensive tuning workflow, SQL query descriptions, cost methodology
- `utils/spark/cache/_CACHE.md` — Memory sizing guidance and broadcast planning
- `utils/spark/parallelism/_PARALLELISM.md` — Executor memory semantics and unified memory manager