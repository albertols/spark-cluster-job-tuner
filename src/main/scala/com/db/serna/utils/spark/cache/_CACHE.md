### Memory sizing and cache footprint

```scala
import com.db.pwcclakees.utils.spark.cache.MemorySizingUtils
import com.db.pwcclakees.utils.spark.cache.MemorySizingUtils.DataFrameMemoryReportConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

val spark = SparkSession.builder().getOrCreate()
import spark.implicits._

val df = spark.range(0, 100_000).toDF("id").repartition(8)

/**
 * Single entry point report: compares multiple "views" of DataFrame memory/caching.
 *
 * The report prints three sections:
 * - MEMORY_REPORT_SIZE_ESTIMATOR:
 *     Driver-side JVM object graph estimate for the DataFrame wrapper.
 *     WARNING: this is NOT the cached footprint (DataFrames cache via Tungsten/columnar/UnsafeRow).
 * - MEMORY_REPORT_RDD:
 *     RDDStorageInfo footprint (closest to Spark UI “Storage” tab when available) and CacheManager presence.
 * - MEMORY_REPORT_DELTA:
 *     Approximate before/after delta derived from SparkContext.getExecutorMemoryStatus (storage-memory accounting).
 */

// Observational mode: does not persist or materialize, only reports what is visible right now.
val reportObs = MemorySizingUtils.dataFrameMemoryReport(
  df,
  DataFrameMemoryReportConfig(
    label = "my_df_observational",
    persistIfNeeded = false,
    materialize = false
  )
)
MemorySizingUtils.logDataFrameMemoryReport(reportObs)

// Experimental mode: optionally persist+materialize to force caching and compute deltas, then unpersist.
val reportExp = MemorySizingUtils.dataFrameMemoryReport(
  df,
  DataFrameMemoryReportConfig(
    label = "my_df_experimental",
    storageLevel = StorageLevel.MEMORY_ONLY,
    persistIfNeeded = true,
    materialize = true,
    unpersistAfter = true
  )
)
println(reportExp.prettyString)

// Spark UI Storage page (when enabled)
println(s"Spark UI Storage page: ${MemorySizingUtils.sparkUiStorageUrl(spark.sparkContext).getOrElse("disabled")}")
```

Notes:
- DataFrames use Tungsten (binary/off-heap friendly) formats; JVM estimators can over-estimate and should not be interpreted as cache footprint.
- The Unified Memory Manager uses dynamic occupancy: storage can be evicted under execution pressure, so cache “size” is not a guaranteed reservation.


### Broadcast sizing lookup tables

When broadcasting large lookup tables (e.g. `val rows = lookupDf.collect(); sc.broadcast(rows)`), be aware that:

- Spark UI “Storage” sizes for DataFrames often reflect **Tungsten/columnar** representations.
- `collect()` converts data into a **JVM object graph** (`Array[Row]`, Strings, nested objects) which can be significantly larger.
- Broadcasts replicate per executor; if multiple broadcasts are held concurrently, per-executor memory pressure multiplies.

This library provides two complementary utilities in `BroadcastSizingUtils`:

```scala
import com.db.pwcclakees.utils.spark.cache.BroadcastSizingUtils
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// (1) Plan from an already-built payload (e.g., Array[Row], Map, case classes)
// Includes SizeEstimator + Java + Kryo size estimates (when possible), plus replication math.
// Use expectedConcurrentBroadcasts if multiple lookups are held simultaneously.
val plan = BroadcastSizingUtils.broadcastMemoryPlanReport(
  label = "lookup_rows_payload",
  payload = rows,                    // e.g. Array[Row]
  spark = spark,
  expectedExecutors = 14,
  expectedConcurrentBroadcasts = 4,
  safetyFactor = 2.0
)
println(plan.prettyString)

// (2) Plan from a DataFrame without fully collecting it:
// Uses sampling + extrapolation to estimate how large the collected JVM Row array could become.
val dfPlan = BroadcastSizingUtils.broadcastMemoryPlanReportFromDataFrame(
  label = "lookup_df_collect_plan",
  df = lookupDf,
  spark = spark,
  expectedExecutors = 14,
  expectedConcurrentBroadcasts = 4,
  sampleN = 20000
)
println(dfPlan.prettyString)
```

For empirical validation on a real cluster, you can measure an approximate delta around broadcast materialization:

```scala
val delta = BroadcastSizingUtils.broadcastStorageDeltaReport(
  sc = spark.sparkContext,
  label = "lookup_broadcast_delta",
  payload = rows,
  touchPartitions = 64
)
println(delta.prettyString)
```

Notes:
- Kryo-serialized bytes are a good proxy for “bytes shipped”, but executor in-memory representation after deserialization can be larger.
- If these estimates are too high, prefer a Spark SQL broadcast join (stays in Tungsten/columnar) or use a more compact payload representation.