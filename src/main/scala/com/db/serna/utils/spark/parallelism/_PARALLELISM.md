# Parallelism utilities: memory management references and guidelines

This package provides utilities and listeners to observe and reason about Spark executor memory and caching behavior on Dataproc/YARN and local CI.

Official documentation references:
- Spark Configuration: [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- Memory Management Overview: [Tuning - Memory Management Overview](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)

Key concepts:
- Executor on-heap memory (`spark.executor.memory`) is the JVM heap available to Spark.
- Executor memory overhead (Dataproc/YARN: `spark.yarn.executor.memoryOverhead`, generic: `spark.executor.memoryOverhead`) reserves container headroom for native libraries, off-heap memory, networking, and JVM metaspace. If not set, Spark defaults to `max(384 MB, factor × executor memory)`.
- Unified memory manager:
    - Spark reserves a unified region: `onHeapUnified = executorOnHeap × spark.memory.fraction`
    - Initial budgets: `execution = unified × (1 - spark.memory.storageFraction)`, `storage = unified × storageFraction`
    - Dynamic occupancy: execution can grow by evicting cached storage blocks; storage can borrow unused execution space but cannot evict execution memory.
- Off-heap (when enabled via `spark.memory.offHeap.enabled` and `spark.memory.offHeap.size`) uses a similar unified budgeting; ensure memory overhead is large enough to cover off-heap and other native usage to avoid container OOMs.

Practical tips:
- On Dataproc/YARN, prefer `spark.yarn.executor.memoryOverhead`. Keep it ≥ your off-heap size plus headroom.
- To understand cache footprint for a dataset, cache it and consult the Spark UI “Storage” page. Programmatically, you can query `SparkContext.getRDDStorageInfo` and the SQL `CacheManager`.
- For estimating object sizes (e.g., broadcast variables), use `org.apache.spark.util.SizeEstimator.estimate(obj)` as a practical approximation.

These utilities and listeners in this package implement the above practices and expose telemetry fields aligned with Spark’s memory model.