package com.db.serna.utils.spark.parallelism

import com.db.serna.utils.spark.cache.MemorySizingUtils
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.jdk.CollectionConverters._

/**
 * // References:
 *   - Spark Configuration: https://spark.apache.org/docs/latest/configuration.html
 *   - Memory Management Overview: https://spark.apache.org/docs/latest/tuning.html#memory-management-overview This
 *     listener’s telemetry fields (executor_onheap_memory_mb, memory_overhead, unified region, execution/storage
 *     budgets, off-heap) directly reflect Spark’s unified memory manager model and YARN/Dataproc memory overhead
 *     semantics. See _PARALLELISM.md in this package for more guidance.
 *
 * Features:
 *   - Emits a synthetic application_start event at construction time (using SparkContext.startTime) so that late
 *     registration still produces an application_start record.
 *   - Avoids duplicate application_start if onApplicationStart is later received.
 *   - Tracks executor add/remove events, min/max/current counts.
 *   - Emits application_end and application_summary.
 *   - Structured JSON logging with event_type as the FIRST field.
 *   - Maintains an internal buffer of emitted JSON lines for unit test assertions.
 *
 * Memory telemetry:
 *   - Logs per-executor on-heap memory (spark.executor.memory).
 *   - Logs effective executor memory overhead (YARN or generic overhead if set; else factor-based estimate with a
 *     floor).
 *   - Logs on-heap unified region sizing derived from spark.memory.fraction and storage/exec initial budgets from
 *     spark.memory.storageFraction.
 *   - Logs off-heap enablement and size; includes off-heap in budgeting when enabled.
 *   - Provides totals across all executors and total execution/storage budgets.
 *
 * Dynamic-occupancy (unified memory manager) summary:
 *   - Spark reserves a unified region of the executor heap: onHeapUnified = executorOnHeap * spark.memory.fraction.
 *   - Within that region, initial partitions are: initialExecution = onHeapUnified * (1 - spark.memory.storageFraction)
 *     initialStorage = onHeapUnified * spark.memory.storageFraction
 *   - The boundary is SOFT: * Execution can grow by evicting cached storage blocks (storage is LRU-evictable) when it
 *     needs space. * Storage can temporarily borrow unused execution space, but cannot evict execution memory.
 *   - If off-heap is enabled (spark.memory.offHeap.enabled), a separate off-heap unified region is used similarly.
 *
 * Dataproc/YARN specifics:
 *   - Prefer spark.yarn.executor.memoryOverhead for executor container headroom.
 *   - If not set, spark.executor.memoryOverhead may be honored.
 *   - If neither is set, Spark defaults to max(384 MB, 0.10 * spark.executor.memory).
 *   - Ensure memory overhead is sufficient to cover off-heap (spark.memory.offHeap.size) and native/Netty/mmap usage to
 *     avoid container OOM.
 *
 * Usage: val listener = new ExecutorTrackingListener spark.sparkContext.addSparkListener(listener)
 *
 * @param sparkSession
 *   SparkSession (required for fallback application start and cores lookup if desired).
 * @param repartitionFactor
 *   Base factor for calculating recommended repartition count (multiplied by current executors and cores per executor).
 * @param eventTypeFamily
 *   event_type_family field value for all emitted JSON records. E.g: "DWH-REPLICATOR", "SCALAMATICA-ODS-NAR-ID",
 *   "SCALAMATICA-DWH-BUSINESS-DOMAIN"
 * @param baseContext
 *   static contextual fields (e.g. pipelineId, submissionId) merged into each JSON record.
 */
class ExecutorTrackingListener(
    sparkSession: SparkSession,
    repartitionFactor: Int = 5,
    eventTypeFamily: String,
    baseContext: Map[String, String] = Map.empty
) extends SparkListener {

  private val log: Logger = LoggerFactory.getLogger(getClass.getName)

  // Times
  private val appStartEpochMillis = new AtomicLong(-1L)
  private val appEndEpochMillis = new AtomicLong(-1L)

  // Executor counts
  private val initialExecutorCount = math.max(0, ParallelismUtils.currentExecutorCount(sparkSession))
  private val currentExecutors =
    new AtomicInteger(
      initialExecutorCount
    ) // to be replaced with ParallelismUtils.currentExecutorCount(sparkSession) ???
  private val maxExecutorsSeen = new AtomicInteger(0)
  private val minExecutorsSeen = new AtomicInteger(Int.MaxValue)

  // Executor event history: executorId -> Vector[(timestamp, action)]
  private val executorEventHistory = new ConcurrentHashMap[String, Vector[(Long, String)]]()

  // Internal buffer for emitted JSON (testing / introspection)
  private val emittedEventsBuffer = new java.util.concurrent.CopyOnWriteArrayList[String]()

  // Date formatting (UTC)
  private val isoFormatter: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  // constant for tracking status changes (added, removed, application_end)
  val executorStatus = "executor_status"
  val executorAdded = "EXECUTOR_ADDED"
  val executorRemoved = "EXECUTOR_REMOVED"
  val executorFinalStatus = "EXECUTOR_FINAL_STATUS"

  /* ==== SparkListener callbacks ==== */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val ts = applicationStart.time
    // Avoid duplicate if synthetic already emitted
    val recorded = appStartEpochMillis.get()
    if (recorded < 0 || recorded != ts) {
      if (appStartEpochMillis.compareAndSet(recorded, ts)) {
        logJson(
          "application_start",
          Seq(
            "app_id" -> Option(applicationStart.appId).getOrElse("unknown"),
            "app_name" -> applicationStart.appName,
            "app_start_iso" -> iso(ts),
            "synthetic" -> false
          )
        )
      }
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val ts = applicationEnd.time
    if (appEndEpochMillis.compareAndSet(-1L, ts)) emitApplicationSummary
  }

  // Ensure synthetic start event for late registration
  emitSyntheticAppStartIfNeeded()

  /* ==== Summary & Helpers ==== */
  def emitApplicationSummary: Unit = {
    val start = appStartEpochMillis.get()
    val end = appEndEpochMillis.get()
    if (start >= 0 && end >= 0) {
      val duration = end - start
      logJson(
        "application_end",
        Seq(
          "app_start_iso" -> iso(start),
          "app_end_iso" -> iso(end),
          "app_duration_millis" -> duration
        )
      )
      logJson(
        executorStatus,
        Seq(
          "executor_event" -> executorFinalStatus,
          "total_no_executors" -> currentExecutors.get(),
          "current_no_executors" -> ParallelismUtils.currentExecutorCount(
            sparkSession
          ), // INTERIM: check against total_no_executors
          "total_no_cores" -> ParallelismUtils.totalExecutorCores(sparkSession)
        ) ++ memoryTelemetryFields // Memory metrics appended
      )
    }
  }

  override def onExecutorAdded(e: SparkListenerExecutorAdded): Unit = {
    if (e.executorId != "driver") {
      val nowCount = currentExecutors.incrementAndGet()
      updateExtrema(nowCount)
      appendExecutorEvent(e.executorId, e.time, executorAdded)
      logJson(
        executorStatus,
        Seq(
          "executor_event" -> executorAdded,
          "executor_id" -> e.executorId,
          "executor_host" -> e.executorInfo.executorHost,
          "executor_total_cores" -> e.executorInfo.totalCores,
          "total_no_cores" -> ParallelismUtils.totalExecutorCores(sparkSession),
          "total_no_executors" -> nowCount,
          "current_no_executors" -> ParallelismUtils.currentExecutorCount(
            sparkSession
          ), // INTERIM: check against total_no_executors
          "timestamp_iso" -> iso(e.time),
          "max_executors_seen" -> maxExecutorsSeen.get(),
          "min_executors_seen" -> effectiveMin()
        ) ++ memoryTelemetryFields // Memory metrics appended
      )
    }
  }

  override def onExecutorRemoved(e: SparkListenerExecutorRemoved): Unit = {
    if (e.executorId != "driver") {
      val nowCount = currentExecutors.decrementAndGet()
      updateExtrema(nowCount)
      appendExecutorEvent(e.executorId, e.time, executorRemoved)
      logJson(
        executorStatus,
        Seq(
          "executor_event" -> executorRemoved,
          "executor_id" -> e.executorId,
          "removed_reason" -> e.reason,
          "total_no_cores" -> ParallelismUtils.totalExecutorCores(sparkSession),
          "total_no_executors" -> nowCount,
          "current_no_executors" -> ParallelismUtils.currentExecutorCount(
            sparkSession
          ), // INTERIM: check against total_no_executors
          "timestamp_iso" -> iso(e.time),
          "max_executors_seen" -> maxExecutorsSeen.get(),
          "min_executors_seen" -> effectiveMin()
        ) ++ memoryTelemetryFields // Memory metrics appended
      )
    }
  }

  private def iso(ts: Long): String =
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.UTC).format(isoFormatter)

  /* ===== Logging Helper ===== */
  private def logJson(
      eventType: String,
      dynamicFields: Seq[(String, Any)]
  ): Unit = {
    val sb = new StringBuilder
    sb.append("{")
    // event_type first
    sb.append("\"event_type\":\"").append(escape(eventType)).append("\"")
    // eventTypeFamily second
    sb.append(",\"event_type_family\":\"").append(escape(eventTypeFamily)).append("\"")
    // base context next
    baseContext.foreach { case (k, v) =>
      sb.append(",\"").append(escape(k)).append("\":\"").append(escape(v)).append("\"")
    }
    // dynamic fields
    dynamicFields.foreach { case (k, v) =>
      sb.append(",\"").append(escape(k)).append("\":\"").append(escape(v.toString)).append("\"")
    }
    sb.append("}")
    val jsonLine = sb.toString()
    log.info(jsonLine)
    emittedEventsBuffer.add(jsonLine)
  }

  private def escape(in: String): String =
    in.replace("\\", "\\\\").replace("\"", "\\\"")

  private def appendExecutorEvent(executorId: String, ts: Long, action: String): Unit = {
    executorEventHistory.compute(
      executorId,
      (_, existing) =>
        if (existing == null) Vector((ts, action))
        else existing :+ (ts -> action)
    )
  }

  private def updateExtrema(current: Int): Unit = {
    // max
    var maxUpdated = false
    while (!maxUpdated) {
      val prev = maxExecutorsSeen.get()
      if (current > prev) maxUpdated = maxExecutorsSeen.compareAndSet(prev, current)
      else maxUpdated = true
    }
    // min
    var minUpdated = false
    while (!minUpdated) {
      val prev = minExecutorsSeen.get()
      if (current >= 0 && current < prev) minUpdated = minExecutorsSeen.compareAndSet(prev, current)
      else minUpdated = true
    }
  }

  private def effectiveMin(): Int = {
    val v = minExecutorsSeen.get()
    if (v == Int.MaxValue) 0 else v
  }

  def currentUptimeMillis: Long = {
    val start = appStartEpochMillis.get()
    if (start < 0) 0L
    else {
      val end = appEndEpochMillis.get()
      val effectiveEnd = if (end < 0) System.currentTimeMillis() else end
      effectiveEnd - start
    }
  }

  def getMaxExecutorsSeen: Int = maxExecutorsSeen.get()
  def getMinExecutorsSeen: Int = effectiveMin()
  def getExecutorHistory: Map[String, Vector[(Long, String)]] = executorEventHistory.asScala.toMap
  def getEmittedEvents: List[String] = emittedEventsBuffer.asScala.toList

  /* ===== Repartitioning Recommendation ===== */
  def getCurrentRecommendedRepartition: Int = {
    val currentExecutors = getCurrentExecutors
    val coresPerExecutor = ParallelismUtils.coresPerExecutor(sparkSession)
    val currentRepartitionFactor = repartitionFactor * getCurrentExecutors * coresPerExecutor
    log.info(
      s"MAGPIE! currentRepartitionFactor=$currentRepartitionFactor => repartitionFactor=$repartitionFactor, currentExecutors=$currentExecutors, coresPerExecutor=$coresPerExecutor"
    )
    if (currentRepartitionFactor > 0) currentRepartitionFactor else sparkSession.sparkContext.defaultParallelism
  }

  // Public accessors
  def getCurrentExecutors: Int = currentExecutors.get()

  // Synthetic application start (constructor-time) if not registered early enough
  private def emitSyntheticAppStartIfNeeded(): Unit = {
    val sc = sparkSession.sparkContext
    val startTs = sc.startTime
    if (appStartEpochMillis.compareAndSet(-1L, startTs)) {
      logJson(
        eventType = "application_start",
        dynamicFields = Seq(
          "app_id" -> Option(sc.applicationId).getOrElse("unknown"),
          "app_name" -> sparkSession.sparkContext.appName,
          "app_start_iso" -> iso(startTs),
          "synthetic" -> true
        )
      )
    }
  }

  /* ===== Memory metrics (allocated capacity and budgets) =====
   *
   * Terminology:
   * - executor_onheap_memory_mb (spark.executor.memory) is the JVM heap size per executor.
   * - executor_memory_overhead_mb is non-heap headroom reserved in the container for native, off-heap, JVM metaspace, etc.
   *   On YARN/Dataproc, prefer spark.yarn.executor.memoryOverhead; generic spark.executor.memoryOverhead may also apply.
   *   If neither is set, Spark defaults to max(384 MB, factor * executor_onheap_memory).
   * - offheap_size_mb (spark.memory.offHeap.size) is separate native memory used by Tungsten when enabled.
   *   Ensure memoryOverhead covers your off-heap usage; otherwise containers may OOM even if the JVM heap is fine.
   *
   * Unified memory budgets (on-heap and off-heap):
   * - onheap_unified_region_mb = executor_onheap_memory_mb * spark.memory.fraction
   * - initial_execution_mb     = unified * (1 - storageFraction)
   * - initial_storage_mb       = unified * storageFraction
   * Dynamic-occupancy behavior:
   * - The execution-storage boundary is soft: execution can evict storage blocks to grow;
   *   storage can only borrow unused execution space but cannot evict execution memory.
   */

  private def conf = sparkSession.sparkContext.getConf

  // Cluster manager hint
  private def clusterManager: String = {
    val m = sparkSession.sparkContext.master
    if (m.toLowerCase.startsWith("yarn")) "yarn" else m
  }

  // On-heap per-executor heap size (spark.executor.memory)
  def executorOnHeapMemoryBytes: Long = conf.getSizeAsBytes("spark.executor.memory", "1g")
  def executorOnHeapMemoryMB: Long = MemorySizingUtils.bytesToMB(executorOnHeapMemoryBytes)

  // YARN-specific overhead (preferred on Dataproc), then generic overhead
  private def yarnOverheadBytesConfigured: Long = conf.getSizeAsBytes("spark.yarn.executor.memoryOverhead", "0")
  private def genericOverheadBytesConfigured: Long = conf.getSizeAsBytes("spark.executor.memoryOverhead", "0")
  private def configuredOverheadBytes: Long =
    if (yarnOverheadBytesConfigured > 0) yarnOverheadBytesConfigured else genericOverheadBytesConfigured

  // Factor for overhead estimation if explicit overhead not set.
  // Keep support for spark.executor.memoryOverheadFactor if present; default to 0.10 otherwise.
  private def overheadFactor: Double =
    if (conf.contains("spark.executor.memoryOverheadFactor"))
      conf.getDouble("spark.executor.memoryOverheadFactor", 0.10d)
    else 0.10d

  private val overheadFloorBytes: Long = 384L * 1024L * 1024L // 384 MB floor (Spark default baseline)
  private def estimatedOverheadBytesFromFactor: Long =
    math.max(overheadFloorBytes, (executorOnHeapMemoryBytes * overheadFactor).toLong)

  // Effective overhead = configured (YARN or generic) else estimated by factor with floor
  def effectiveExecutorOverheadBytes: Long =
    if (configuredOverheadBytes > 0) configuredOverheadBytes else estimatedOverheadBytesFromFactor
  def effectiveExecutorOverheadMB: Long = MemorySizingUtils.bytesToMB(effectiveExecutorOverheadBytes)

  // Off-heap (Tungsten) memory
  def offHeapEnabled: Boolean = conf.getBoolean("spark.memory.offHeap.enabled", false)
  def offHeapBytes: Long = if (offHeapEnabled) conf.getSizeAsBytes("spark.memory.offHeap.size", "0") else 0L
  def offHeapMB: Long = MemorySizingUtils.bytesToMB(offHeapBytes)

  // Unified memory fractions
  def memoryFraction: Double = conf.getDouble("spark.memory.fraction", 0.6)
  def storageFraction: Double = conf.getDouble("spark.memory.storageFraction", 0.5)

  // On-heap unified region budgets
  def onHeapUnifiedBytes: Long = (executorOnHeapMemoryBytes * memoryFraction).toLong
  def onHeapInitialStorageBytes: Long = (onHeapUnifiedBytes * storageFraction).toLong
  def onHeapInitialExecutionBytes: Long = onHeapUnifiedBytes - onHeapInitialStorageBytes

  // Off-heap unified region budgets (when enabled)
  def offHeapUnifiedBytes: Long = if (offHeapEnabled) offHeapBytes else 0L
  def offHeapInitialStorageBytes: Long = if (offHeapEnabled) (offHeapUnifiedBytes * storageFraction).toLong else 0L
  def offHeapInitialExecutionBytes: Long = if (offHeapEnabled) offHeapUnifiedBytes - offHeapInitialStorageBytes else 0L

  // Combined execution/storage budgets across on-heap and off-heap
  def totalInitialExecutionBytes: Long = onHeapInitialExecutionBytes + offHeapInitialExecutionBytes
  def totalInitialStorageBytes: Long = onHeapInitialStorageBytes + offHeapInitialStorageBytes

  // Per-executor totals (container memory footprint on YARN)
  def perExecutorContainerMemoryBytes: Long = executorOnHeapMemoryBytes + effectiveExecutorOverheadBytes
  def perExecutorContainerMemoryMB: Long = MemorySizingUtils.bytesToMB(perExecutorContainerMemoryBytes)

  // Cluster-wide totals
  def totalOnHeapMemoryMB: Long = executorOnHeapMemoryMB * getCurrentExecutors.toLong
  def totalOverheadMemoryMB: Long = effectiveExecutorOverheadMB * getCurrentExecutors.toLong
  def totalAllocatedContainerMemoryMB: Long = perExecutorContainerMemoryMB * getCurrentExecutors.toLong
  def totalOffHeapMemoryMB: Long = offHeapMB * getCurrentExecutors.toLong

  // Backwards-compatible names (kept for existing downstream consumers):
  // NOTE: executorMemoryMB is per-executor on-heap heap size (not duplicated).
  def executorMemoryMB: Long = executorOnHeapMemoryMB
  def executorMemoryOverheadMB: Long = effectiveExecutorOverheadMB
  def totalAllocatedExecutorMemoryMB: Long = totalAllocatedContainerMemoryMB

  // Helper to build a stable set of memory telemetry fields to append to every event
  private def memoryTelemetryFields: Seq[(String, Any)] = {
    val overheadVsOffHeapOk = effectiveExecutorOverheadBytes >= offHeapBytes
    val overheadSource =
      if (yarnOverheadBytesConfigured > 0) "yarn.configured"
      else if (genericOverheadBytesConfigured > 0) "generic.configured"
      else if (conf.contains("spark.executor.memoryOverheadFactor")) s"factor(${overheadFactor})"
      else "default(0.10|384MB-min)"

    Seq(
      // Environment
      "cluster_manager" -> clusterManager,

      // Per-executor fundamentals
      "executor_onheap_memory_mb" -> executorOnHeapMemoryMB,
      "executor_memory_overhead_mb_effective" -> effectiveExecutorOverheadMB,
      "executor_memory_overhead_source" -> overheadSource,
      "offheap_enabled" -> offHeapEnabled,
      "offheap_size_mb" -> offHeapMB,

      // Unified memory configuration
      "spark_memory_fraction" -> memoryFraction,
      "spark_memory_storage_fraction" -> storageFraction,

      // On-heap unified budgets
      "onheap_unified_region_mb" -> MemorySizingUtils.bytesToMB(onHeapUnifiedBytes),
      "onheap_initial_execution_mb" -> MemorySizingUtils.bytesToMB(onHeapInitialExecutionBytes),
      "onheap_initial_storage_mb" -> MemorySizingUtils.bytesToMB(onHeapInitialStorageBytes),

      // Off-heap unified budgets (if enabled)
      "offheap_unified_region_mb" -> MemorySizingUtils.bytesToMB(offHeapUnifiedBytes),
      "offheap_initial_execution_mb" -> MemorySizingUtils.bytesToMB(offHeapInitialExecutionBytes),
      "offheap_initial_storage_mb" -> MemorySizingUtils.bytesToMB(offHeapInitialStorageBytes),

      // Combined execution/storage budgets for this executor
      "execution_memory_budget_mb" -> MemorySizingUtils.bytesToMB(totalInitialExecutionBytes),
      "storage_memory_budget_mb" -> MemorySizingUtils.bytesToMB(totalInitialStorageBytes),

      // Totals across executors
      "total_onheap_memory_mb" -> totalOnHeapMemoryMB,
      "total_overhead_memory_mb" -> totalOverheadMemoryMB,
      "total_offheap_memory_mb" -> totalOffHeapMemoryMB,
      "total_allocated_container_memory_mb" -> totalAllocatedContainerMemoryMB,

      // Back-compat field names
      "executorMemoryMB" -> executorMemoryMB,
      "executorMemoryOverheadMB" -> executorMemoryOverheadMB,
      "total_allocated_executor_memory_mb" -> totalAllocatedExecutorMemoryMB,

      // Sanity
      "memory_overhead_covers_offheap" -> overheadVsOffHeapOk
    )
  }

}
