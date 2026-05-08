package com.db.serna.utils.spark.cache

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.{RDDInfo, StorageLevel}
import org.apache.spark.util.SizeEstimator
import org.slf4j.{Logger, LoggerFactory}

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

/**
 * MemorySizingUtils: helpers to estimate memory consumption and main.scala.com.db.serna.utils.spark.cache footprint.
 *
 * References:
 *   - Spark Configuration: https://spark.apache.org/docs/latest/configuration.html
 *   - Memory Management Overview: https://spark.apache.org/docs/latest/tuning.html#memory-management-overview
 *
 * IMPORTANT SEMANTICS / CAVEATS:
 *   - DataFrame caching uses Spark SQL's columnar main.scala.com.db.serna.utils.spark.cache / UnsafeRow + Tungsten
 *     memory manager. Any JVM-object estimator (e.g., SizeEstimator) does NOT represent cached DataFrame footprint.
 *   - SparkContext.getExecutorMemoryStatus reflects executor *storage memory accounting* (max, remaining) and we derive
 *     "used = max - remaining". This is not the same as "RDDStorageInfo.memSize".
 *   - Deltas from before/after snapshots are approximate and may be affected by other activity.
 */
object MemorySizingUtils {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  /** Estimate the shallow+reachable size of an object using Spark's SizeEstimator. */
  def estimateObjectSizeBytes(obj: AnyRef): Long = SizeEstimator.estimate(obj)

  case class RDDStorageFootprint(
      rddId: Int,
      memSizeBytes: Long,
      diskSizeBytes: Long,
      cachedPartitions: Int,
      totalPartitions: Int
  )

  def getRDDStorageFootprint(df: DataFrame): Option[RDDStorageFootprint] = {
    val sc = df.sparkSession.sparkContext
    val rddId = df.rdd.id
    sc.getRDDStorageInfo
      .find(_.id == rddId)
      .map { info: RDDInfo =>
        val fp = RDDStorageFootprint(
          rddId = info.id,
          memSizeBytes = info.memSize,
          diskSizeBytes = info.diskSize,
          cachedPartitions = info.numCachedPartitions,
          totalPartitions = info.numPartitions
        )
        log.info(
          s"[MSU] RDDStorageInfo found for rddId=$rddId mem=${fmtBytes(fp.memSizeBytes)} disk=${fmtBytes(fp.diskSizeBytes)} " +
            s"cachedPartitions=${fp.cachedPartitions}/${fp.totalPartitions}"
        )
        fp
      }
      .orElse {
        val cachedData = df.sparkSession.sharedState.cacheManager.lookupCachedData(df.queryExecution.logical)
        if (cachedData.nonEmpty) {
          log.info(s"[MSU] CacheManager reports cached logical plan; RDDStorageInfo not available for rddId=$rddId")
          Some(
            RDDStorageFootprint(
              rddId,
              memSizeBytes = 0L,
              diskSizeBytes = 0L,
              cachedPartitions = 0,
              totalPartitions = df.rdd.partitions.length
            )
          )
        } else {
          log.info(s"[MSU] No storage info found for rddId=$rddId; DataFrame not cached")
          None
        }
      }
  }

  case class DataFrameMemoryReportConfig(
      label: String = "df",
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,

      // If true, persist the DF if it isn't already persisted/cached (best-effort).
      persistIfNeeded: Boolean = false,

      // If true, execute an action to force materialization. Default: true when persisting.
      materialize: Boolean = false,

      // Only used when we persisted inside the report call.
      unpersistAfter: Boolean = true
  )

  /** Dedicated output for MEMORY_REPORT_DELTA (executor storage accounting before/after/delta). */
  case class ExecutorStorageDelta(
      beforeUsedBytes: Map[String, Long],
      afterUsedBytes: Map[String, Long],
      perExecutorDeltaBytes: Map[String, Long],
      totalDeltaBytes: Long
  )

  /**
   * MEMORY_REPORT_DELTA: compute an approximate main.scala.com.db.serna.utils.spark.cache footprint delta based on
   * executor storage accounting.
   *
   * Uses SparkContext.getExecutorMemoryStatus:
   *   - (max, remaining) for Spark's "storage memory" accounting
   *   - used = max - remaining
   *
   * Notes:
   *   - This is NOT JVM heap usage.
   *   - This is NOT necessarily equal to Spark UI "Storage" sizes.
   *   - Deltas are approximate and can be impacted by concurrent caching/evictions.
   *
   * @return
   *   (delta, materializedCount, persistedByReport)
   */
  def executorStorageDeltaReport(
      df: DataFrame,
      label: String,
      persistIfNeeded: Boolean,
      storageLevel: StorageLevel,
      materialize: Boolean,
      unpersistAfter: Boolean
  ): (ExecutorStorageDelta, Option[Long], Boolean) = {
    val sc = df.sparkSession.sparkContext

    def usedByExecutor(): Map[String, Long] =
      sc.getExecutorMemoryStatus.map { case (execAddr, (max, remaining)) =>
        val used = math.max(0L, max - remaining)
        execAddr -> used
      }.toMap

    val before = usedByExecutor()

    val cacheManagerHasEntryBefore =
      df.sparkSession.sharedState.cacheManager.lookupCachedData(df.queryExecution.logical).nonEmpty
    val rddStorageInfoPresentBefore =
      sc.getRDDStorageInfo.exists(_.id == df.rdd.id)

    // Decide whether to persist (best-effort heuristic)
    val shouldPersist = persistIfNeeded && !(cacheManagerHasEntryBefore || rddStorageInfoPresentBefore)
    val shouldMaterialize = materialize || shouldPersist

    var persistedByReport = false
    if (shouldPersist) {
      df.persist(storageLevel)
      persistedByReport = true
      log.info(s"[MSU] MEMORY_REPORT_DELTA persisted label=$label storageLevel=$storageLevel")
    }

    val materializedCount =
      if (shouldMaterialize) {
        val c = df.count()
        log.info(s"[MSU] MEMORY_REPORT_DELTA materialized label=$label count=$c")
        Some(c)
      } else None

    val after = if (shouldMaterialize) usedByExecutor() else Map.empty[String, Long]

    val perExecDelta =
      if (shouldMaterialize) {
        after.map { case (exec, usedAfter) =>
          val usedBefore = before.getOrElse(exec, 0L)
          exec -> math.max(0L, usedAfter - usedBefore)
        }
      } else Map.empty[String, Long]

    val totalDelta = perExecDelta.values.sum

    if (persistedByReport && unpersistAfter) {
      df.unpersist(blocking = false)
      log.info(s"[MSU] MEMORY_REPORT_DELTA unpersisted label=$label")
    }

    (
      ExecutorStorageDelta(
        beforeUsedBytes = before,
        afterUsedBytes = after,
        perExecutorDeltaBytes = perExecDelta,
        totalDeltaBytes = totalDelta
      ),
      materializedCount,
      persistedByReport
    )
  }

  case class DataFrameMemoryReport(
      label: String,
      rddId: Int,
      partitions: Int,

      // MEMORY_REPORT_SIZE_ESTIMATOR (Option A):
      // This is the driver-side JVM object graph estimate for the DataFrame object itself.
      // It is NOT the cached footprint.
      driverSideDataFrameObjectGraphEstimateBytes: Long,
      storageLevelRequested: String,

      // MEMORY_REPORT_RDD / SQL main.scala.com.db.serna.utils.spark.cache presence
      cacheManagerHasEntry: Boolean,
      rddStorageInfoPresentForThisRddId: Boolean,
      rddStorageFootprint: Option[RDDStorageFootprint],

      // MEMORY_REPORT_DELTA
      executorUsedBeforeBytes: Map[String, Long],
      executorUsedAfterBytes: Map[String, Long],
      executorUsedDeltaBytes: Map[String, Long],
      totalExecutorUsedDeltaBytes: Long,
      materialized: Boolean,
      materializedCount: Option[Long],

      // whether this call performed df.persist(...)
      persistedByReport: Boolean,
      notes: Seq[String]
  ) {
    def prettyString: String = {
      val sb = new StringBuilder
      sb.append(s"DataFrameMemoryReport(label=$label, rddId=$rddId, partitions=$partitions)\n")
      sb.append(s"  requestedStorageLevel=$storageLevelRequested\n")

      sb.append(
        s"  [MEMORY_REPORT_SIZE_ESTIMATOR] driverSideDataFrameObjectGraphEstimate=" +
          s"${fmtBytes(driverSideDataFrameObjectGraphEstimateBytes)} " +
          s"(WARNING: not main.scala.com.db.serna.utils.spark.cache footprint; Tungsten/columnar main.scala.com.db.serna.utils.spark.cache is different)\n"
      )

      sb.append(
        s"  cacheManagerHasEntry=$cacheManagerHasEntry, rddStorageInfoPresentForThisRddId=$rddStorageInfoPresentForThisRddId\n"
      )
      sb.append(s"  persistedByReport=$persistedByReport\n")

      rddStorageFootprint match {
        case Some(fp) =>
          sb.append(
            s"  [MEMORY_REPORT_RDD] rddStorageFootprint: mem=${fmtBytes(fp.memSizeBytes)}, disk=${fmtBytes(fp.diskSizeBytes)}, " +
              s"cachedPartitions=${fp.cachedPartitions}/${fp.totalPartitions}\n"
          )
        case None =>
          sb.append(s"  [MEMORY_REPORT_RDD] rddStorageFootprint: <none>\n")
      }

      def fmtExec(m: Map[String, Long]): String =
        if (m.isEmpty) "<empty>"
        else m.toSeq.sortBy(_._1).map { case (k, v) => s"$k=${fmtBytes(v)}" }.mkString(", ")

      sb.append(s"  [MEMORY_REPORT_DELTA] executorUsedBefore: ${fmtExec(executorUsedBeforeBytes)}\n")
      sb.append(s"  [MEMORY_REPORT_DELTA] executorUsedAfter:  ${fmtExec(executorUsedAfterBytes)}\n")
      sb.append(
        s"  [MEMORY_REPORT_DELTA] executorDelta:      ${fmtExec(executorUsedDeltaBytes)} (totalDelta=${fmtBytes(totalExecutorUsedDeltaBytes)})\n"
      )
      sb.append(s"  materialized=$materialized ,count=${materializedCount.getOrElse(-1L)}\n")

      if (notes.nonEmpty) {
        sb.append("  notes:\n")
        notes.foreach(n => sb.append(s"    - $n\n"))
      }
      sb.toString()
    }
  }

  def dataFrameMemoryReport(
      df: DataFrame,
      config: DataFrameMemoryReportConfig = DataFrameMemoryReportConfig()
  ): DataFrameMemoryReport = {
    val spark = df.sparkSession
    val sc = spark.sparkContext

    val label: String = config.label
    val rddId: Int = df.rdd.id
    val partitions: Int = df.rdd.getNumPartitions

    /* ===> MEMORY_REPORT_DELTA <=== */
    val (delta, materializedCount, persistedByReport) =
      executorStorageDeltaReport(
        df = df,
        label = label,
        persistIfNeeded = config.persistIfNeeded,
        storageLevel = config.storageLevel,
        materialize = config.materialize,
        unpersistAfter = config.unpersistAfter
      )

    /* ===> MEMORY_REPORT_SIZE_ESTIMATOR (Option, driver-side only) <=== */
    val driverSideEstimateBytes: Long = estimateObjectSizeBytes(df)

    /* ===> MEMORY_REPORT_RDD (Spark UI-ish storage info) <=== */
    val footprintOpt: Option[RDDStorageFootprint] = getRDDStorageFootprint(df)

    val cacheManagerHasEntryAfter: Boolean =
      spark.sharedState.cacheManager.lookupCachedData(df.queryExecution.logical).nonEmpty
    val rddStorageInfoPresentAfter: Boolean = sc.getRDDStorageInfo.exists(_.id == rddId)

    val notes = Seq(
      // Interpretation guidance
      "Interpret these numbers as different *views* of memory, not competing exact answers.",

      // RDDStorageInfo / Spark UI view
      "MEMORY_REPORT_RDD uses SparkContext.getRDDStorageInfo (when available), which usually aligns best with the Spark UI 'Storage' tab for RDD caches.",

      // SQL main.scala.com.db.serna.utils.spark.cache view
      "For DataFrame/Dataset caching (Spark SQL), CacheManager is the authoritative signal that a logical plan is cached; byte sizes may not always surface via getRDDStorageInfo for df.rdd.id.",

      // Unified Memory Manager
      "Spark uses the Unified Memory Manager: storage and execution share a unified region; storage can be evicted under execution pressure (so main.scala.com.db.serna.utils.spark.cache 'size' is not a guaranteed reservation).",

      // Tungsten / columnar main.scala.com.db.serna.utils.spark.cache
      "DataFrames are cached in Tungsten-friendly formats (e.g., UnsafeRow / CachedBatch / columnar) which can be off-heap and more compact than JVM objects.",

      // SizeEstimator disclaimer (Option A)
      "MEMORY_REPORT_SIZE_ESTIMATOR is ONLY the driver-side JVM object graph estimate for the DataFrame wrapper. It is NOT the executor-side cached footprint and can mislead if compared directly to the other sections.",

      // ExecutorMemoryStatus disclaimer
      "MEMORY_REPORT_DELTA derives 'used' from SparkContext.getExecutorMemoryStatus as (max - remaining) for Spark's storage-memory accounting; it is not raw heap usage and may diverge from Spark UI sizes.",

      // Config knobs that matter
      "Cache footprint is impacted by compression/serialization: spark.sql.inMemoryColumnarStorage.compressed, spark.rdd.compress, and the chosen serializer (Kryo vs Java).",

      // Off-heap / YARN overhead operational note
      "If off-heap is enabled (spark.memory.offHeap.enabled), ensure YARN memoryOverhead is large enough; otherwise executors can container-OOM even when JVM heap seems fine."
    )

    DataFrameMemoryReport(
      label = label,
      rddId = rddId,
      partitions = partitions,
      driverSideDataFrameObjectGraphEstimateBytes = driverSideEstimateBytes,
      storageLevelRequested = config.storageLevel.description,
      cacheManagerHasEntry = cacheManagerHasEntryAfter,
      rddStorageInfoPresentForThisRddId = rddStorageInfoPresentAfter,
      rddStorageFootprint = footprintOpt,
      executorUsedBeforeBytes = delta.beforeUsedBytes,
      executorUsedAfterBytes = delta.afterUsedBytes,
      executorUsedDeltaBytes = delta.perExecutorDeltaBytes,
      totalExecutorUsedDeltaBytes = delta.totalDeltaBytes,
      materialized = config.materialize || (config.persistIfNeeded && persistedByReport),
      materializedCount = materializedCount,
      persistedByReport = persistedByReport,
      notes = notes
    )
  }

  /* HELPERS */
  def logDataFrameMemoryReport(report: DataFrameMemoryReport): Unit =
    log.info(s"[MSU] \n${report.prettyString}")

  def sparkUiStorageUrl(sc: SparkContext): Option[String] = {
    val uiEnabled = sc.getConf.getBoolean("spark.ui.enabled", defaultValue = true)
    if (!uiEnabled) return None
    val uiPort = sc.getConf.getInt("spark.ui.port", 4040)
    Some(s"http://localhost:$uiPort/storage/")
  }

  def bytesToMB(bytes: Long): Long = bytes / (1024L * 1024L)

  /**
   * Format bytes into a human-readable string without rounding (truncate to 2 decimals). Uses binary units (base 1024):
   * B, KB, MB, GB, TB.
   */
  def fmtBytes(bytes: Long): String = {
    val units = Array("B", "KB", "MB", "GB", "TB")
    if (bytes <= 0) return "0 B"
    val i = (Math.log(bytes.toDouble) / Math.log(1024)).toInt
    val value = bytes / Math.pow(1024, i)
    val truncated = BigDecimal(value).setScale(2, RoundingMode.DOWN).toString
    s"$truncated ${units(i)}"
  }
}
