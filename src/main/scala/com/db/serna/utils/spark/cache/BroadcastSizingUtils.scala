package com.db.serna.utils.spark.cache

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

/**
 * [[ BroadcastSizingUtils ]]
 *
 * Utilities to help plan and empirically validate executor memory impact of broadcasting large lookup tables.
 */
object BroadcastSizingUtils {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  case class BroadcastMemoryPlanReport(
                                        label: String,

                                        // Direct payload estimates (JVM object graph)
                                        driverObjectGraphEstimateBytes: Long,

                                        // Serialized estimates (bytes shipped approximation)
                                        javaSerializedBytesEstimate: Option[Long],
                                        kryoSerializedBytesEstimate: Option[Long],

                                        configuredSparkSerializer: String,

                                        // Planning knobs
                                        expectedExecutors: Int,
                                        expectedConcurrentBroadcasts: Int,
                                        replicationFactor: Int,
                                        safetyFactor: Double,

                                        // Derived guidance
                                        estimatedPerExecutorSingleBroadcastBytes: Long,
                                        estimatedPerExecutorTotalBroadcastBytes: Long,
                                        estimatedClusterTotalBytes: Long,

                                        // Optional executor heap comparison (if spark.executor.memory is set)
                                        executorHeapBytes: Option[Long],

                                        // Extra context / warnings
                                        notes: Seq[String]
                                      ) {
    def prettyString: String = {
      val sb = new StringBuilder
      sb.append(s"BroadcastMemoryPlanReport(label=$label)\n")
      sb.append(s"  driverObjectGraphEstimate=${MemorySizingUtils.fmtBytes(driverObjectGraphEstimateBytes)}\n")
      sb.append(s"  javaSerializedBytesEstimate=${javaSerializedBytesEstimate.map(MemorySizingUtils.fmtBytes).getOrElse("<unavailable>")}\n")
      sb.append(s"  kryoSerializedBytesEstimate=${kryoSerializedBytesEstimate.map(MemorySizingUtils.fmtBytes).getOrElse("<unavailable>")}\n")
      sb.append(s"  configuredSparkSerializer=$configuredSparkSerializer\n")
      sb.append(s"  expectedExecutors=$expectedExecutors expectedConcurrentBroadcasts=$expectedConcurrentBroadcasts replicationFactor=$replicationFactor safetyFactor=$safetyFactor\n")
      sb.append(s"  estimatedPerExecutorSingleBroadcast=${MemorySizingUtils.fmtBytes(estimatedPerExecutorSingleBroadcastBytes)}\n")
      sb.append(s"  estimatedPerExecutorTotalBroadcast=${MemorySizingUtils.fmtBytes(estimatedPerExecutorTotalBroadcastBytes)}\n")
      sb.append(s"  estimatedClusterTotal=${MemorySizingUtils.fmtBytes(estimatedClusterTotalBytes)}\n")
      sb.append(s"  executorHeap=${executorHeapBytes.map(MemorySizingUtils.fmtBytes).getOrElse("<unknown>")}\n")
      if (notes.nonEmpty) {
        sb.append("  notes:\n")
        notes.foreach(n => sb.append(s"    - $n\n"))
      }
      sb.toString()
    }
  }

  /** Stable serialization estimate using Java serialization (works when payload is Serializable). */
  def estimateJavaSerializedSizeBytes(obj: AnyRef): Option[Long] = {
    try {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(obj)
      oos.flush()
      oos.close()
      Some(baos.toByteArray.length.toLong)
    } catch {
      case _: Throwable => None
    }
  }

  /**
   * Kryo size estimate using Spark's KryoSerializer configured with SparkConf.
   * Does NOT rely on sparkContext.env (which can be inaccessible).
   */
  def estimateKryoSerializedSizeBytes(obj: AnyRef, spark: SparkSession): Option[Long] = {
    try {
      val conf: SparkConf = spark.sparkContext.getConf
      val ser = new KryoSerializer(conf).newInstance()
      val bb = ser.serialize(obj)
      Some(bb.remaining().toLong)
    } catch {
      case _: Throwable => None
    }
  }

  private def configuredSerializerName(spark: SparkSession): String =
    spark.sparkContext.getConf.getOption("spark.serializer").getOrElse("default")

  private def executorHeapBytesOpt(spark: SparkSession): Option[Long] =
    try Some(spark.sparkContext.getConf.getSizeAsBytes("spark.executor.memory"))
    catch { case _: Throwable => None }

  /**
   * Planner: build report from an already-materialized broadcast payload (e.g., Array[Row]).
   */
  def broadcastMemoryPlanReport(
                                 label: String,
                                 payload: AnyRef,
                                 spark: SparkSession,
                                 expectedExecutors: Int,
                                 expectedConcurrentBroadcasts: Int = 1,
                                 replicationFactor: Int = 1,
                                 safetyFactor: Double = 2.0 // default higher for object-heavy payloads like Array[Row]
                               ): BroadcastMemoryPlanReport = {

    val driverEstimate = MemorySizingUtils.estimateObjectSizeBytes(payload)
    val javaSerializedOpt = estimateJavaSerializedSizeBytes(payload)
    val kryoSerializedOpt = estimateKryoSerializedSizeBytes(payload, spark)

    // Best available serialized estimate:
    val bestSerializedOpt = kryoSerializedOpt.orElse(javaSerializedOpt)

    // Conservative base per-executor:
    // at least as large as the object graph estimate (post-deserialization it often resembles object graph again)
    val baseSingleBroadcast = bestSerializedOpt match {
      case Some(s) => math.max(s, driverEstimate)
      case None    => driverEstimate
    }

    val singleWithSafety = math.ceil(baseSingleBroadcast.toDouble * safetyFactor).toLong

    val perExecutorTotal = singleWithSafety * expectedConcurrentBroadcasts.toLong * replicationFactor.toLong
    val clusterTotal = perExecutorTotal * expectedExecutors.toLong

    val execHeapOpt = executorHeapBytesOpt(spark)

    val warnings = {
      val baseNotes = Seq(
        "This report targets broadcast payload JVM objects (e.g., Array[Row], Map, case classes). SizeEstimator is meaningful here (unlike Tungsten DataFrame caching).",
        "kryoSerializedBytesEstimate approximates bytes shipped when using Kryo; executor in-memory representation can still be larger after deserialization.",
        "If you build payload via lookupDf.collect(), the payload becomes a large JVM object graph and may be much bigger than Spark UI (Tungsten/columnar) sizes.",
        "Broadcasts replicate per executor; if multiple lookups remain alive concurrently, per-executor pressure multiplies."
      )

      val heapWarn = execHeapOpt.flatMap { heap =>
        val ratio = perExecutorTotal.toDouble / heap.toDouble
        if (ratio >= 0.7)
          Some(f"Estimated broadcast resident bytes are ~${ratio * 100}%.1f%% of executor heap. High OOM risk; consider compact payload or broadcast join.")
        else if (ratio >= 0.4)
          Some(f"Estimated broadcast resident bytes are ~${ratio * 100}%.1f%% of executor heap. Ensure enough headroom for execution/shuffle.")
        else None
      }.toSeq

      baseNotes ++ heapWarn ++ Seq(
        "If these numbers are too high, consider changing representation (encode to bytes / primitive arrays) or using Spark SQL broadcast join to stay in Tungsten/columnar.",
        "Also ensure YARN memoryOverhead covers native/off-heap/netty; large broadcasts can increase pressure indirectly."
      )
    }

    BroadcastMemoryPlanReport(
      label = label,
      driverObjectGraphEstimateBytes = driverEstimate,
      javaSerializedBytesEstimate = javaSerializedOpt,
      kryoSerializedBytesEstimate = kryoSerializedOpt,
      configuredSparkSerializer = configuredSerializerName(spark),
      expectedExecutors = expectedExecutors,
      expectedConcurrentBroadcasts = expectedConcurrentBroadcasts,
      replicationFactor = replicationFactor,
      safetyFactor = safetyFactor,
      estimatedPerExecutorSingleBroadcastBytes = singleWithSafety,
      estimatedPerExecutorTotalBroadcastBytes = perExecutorTotal,
      estimatedClusterTotalBytes = clusterTotal,
      executorHeapBytes = execHeapOpt,
      notes = warnings
    )
  }

  /**
   * Planner: estimate "collect()+broadcast" expansion from a DataFrame via sampling.
   *
   * This avoids full collect of multi-GB lookups just to estimate size.
   *
   * @param sampleN number of rows to collect for estimation (keep small enough for driver)
   * @param rowCountHint if provided, avoids df.count()
   */
  def broadcastMemoryPlanReportFromDataFrame(
                                              label: String,
                                              df: DataFrame,
                                              spark: SparkSession,
                                              expectedExecutors: Int,
                                              expectedConcurrentBroadcasts: Int = 1,
                                              replicationFactor: Int = 1,
                                              safetyFactor: Double = 2.0,
                                              sampleN: Int = 20000,
                                              rowCountHint: Option[Long] = None
                                            ): BroadcastMemoryPlanReport = {

    // Collect a sample and estimate the JVM object graph size of the collected rows.
    val sample = df.limit(sampleN).collect()
    val sampleBytes = MemorySizingUtils.estimateObjectSizeBytes(sample)
    val sampleRows = math.max(1, sample.length)
    val perRowBytesApprox = sampleBytes.toDouble / sampleRows.toDouble

    val totalRows = rowCountHint.getOrElse(df.count())
    val estimatedCollectedArrayBytes = math.ceil(perRowBytesApprox * totalRows.toDouble).toLong

    // Build a synthetic "payload" estimator without collecting everything:
    // - driver object estimate uses extrapolated collected bytes
    // - serialized estimates are not directly computed (no payload), so None
    val configuredSerializer = configuredSerializerName(spark)
    val execHeapOpt = executorHeapBytesOpt(spark)

    val baseSingleBroadcast = estimatedCollectedArrayBytes
    val singleWithSafety = math.ceil(baseSingleBroadcast.toDouble * safetyFactor).toLong
    val perExecutorTotal = singleWithSafety * expectedConcurrentBroadcasts.toLong * replicationFactor.toLong
    val clusterTotal = perExecutorTotal * expectedExecutors.toLong

    val notes = Seq(
      "This is a DataFrame->collect()->Array[Row] broadcast planner using sampling + extrapolation.",
      s"Collected sampleN=$sampleN rows, sampleBytes=${MemorySizingUtils.fmtBytes(sampleBytes)}, approxPerRowBytes=${perRowBytesApprox.toLong} B, totalRows=$totalRows.",
      "Spark UI sizes for DataFrames are typically Tungsten/columnar and can be far smaller than the collected JVM Row array.",
      "Serialized size is not estimated here because we did not materialize the full payload; use broadcastMemoryPlanReport(payload=rows) if you can safely build the payload in a controlled test.",
      "If the extrapolated bytes are too high, prefer a Spark SQL broadcast join or a compact encoded payload rather than collect+broadcast Rows."
    ) ++ execHeapOpt.flatMap { heap =>
      val ratio = perExecutorTotal.toDouble / heap.toDouble
      if (ratio >= 0.7)
        Some(f"Estimated broadcast resident bytes are ~${ratio * 100}%.1f%% of executor heap. High OOM risk.")
      else if (ratio >= 0.4)
        Some(f"Estimated broadcast resident bytes are ~${ratio * 100}%.1f%% of executor heap. Ensure enough headroom for execution/shuffle.")
      else None
    }.toSeq

    BroadcastMemoryPlanReport(
      label = label,
      driverObjectGraphEstimateBytes = estimatedCollectedArrayBytes,
      javaSerializedBytesEstimate = None,
      kryoSerializedBytesEstimate = None,
      configuredSparkSerializer = configuredSerializer,
      expectedExecutors = expectedExecutors,
      expectedConcurrentBroadcasts = expectedConcurrentBroadcasts,
      replicationFactor = replicationFactor,
      safetyFactor = safetyFactor,
      estimatedPerExecutorSingleBroadcastBytes = singleWithSafety,
      estimatedPerExecutorTotalBroadcastBytes = perExecutorTotal,
      estimatedClusterTotalBytes = clusterTotal,
      executorHeapBytes = execHeapOpt,
      notes = notes
    )
  }

  case class BroadcastStorageDeltaReport(
                                          label: String,
                                          beforeUsedBytes: Map[String, Long],
                                          afterUsedBytes: Map[String, Long],
                                          perExecutorDeltaBytes: Map[String, Long],
                                          totalDeltaBytes: Long,
                                          materializationTouchCount: Long,
                                          notes: Seq[String]
                                        ) {
    def prettyString: String = {
      def fmtExec(m: Map[String, Long]): String =
        if (m.isEmpty) "<empty>"
        else m.toSeq.sortBy(_._1).map { case (k, v) => s"$k=${MemorySizingUtils.fmtBytes(v)}" }.mkString(", ")

      val sb = new StringBuilder
      sb.append(s"BroadcastStorageDeltaReport(label=$label)\n")
      sb.append(s"  touchCount=$materializationTouchCount\n")
      sb.append(s"  before: ${fmtExec(beforeUsedBytes)}\n")
      sb.append(s"  after:  ${fmtExec(afterUsedBytes)}\n")
      sb.append(s"  delta:  ${fmtExec(perExecutorDeltaBytes)} totalDelta=${MemorySizingUtils.fmtBytes(totalDeltaBytes)}\n")
      if (notes.nonEmpty) {
        sb.append("  notes:\n")
        notes.foreach(n => sb.append(s"    - $n\n"))
      }
      sb.toString()
    }
  }

  def broadcastStorageDeltaReport(
                                   sc: SparkContext,
                                   label: String,
                                   payload: AnyRef,
                                   touchPartitions: Int = 64
                                 ): BroadcastStorageDeltaReport = {

    def usedByExecutor(): Map[String, Long] =
      sc.getExecutorMemoryStatus.map { case (execAddr, (max, remaining)) =>
        val used = math.max(0L, max - remaining)
        execAddr -> used
      }.toMap

    val before = usedByExecutor()
    val b = sc.broadcast(payload)

    val touchCount = sc
      .parallelize(1 to touchPartitions, touchPartitions)
      .mapPartitions { _ =>
        val v = b.value
        val len = v match {
          case arr: Array[_] => arr.length
          case _             => 1
        }
        Iterator(len.toLong)
      }
      .count()

    val after = usedByExecutor()
    val perExecDelta = after.map { case (exec, usedAfter) =>
      val usedBefore = before.getOrElse(exec, 0L)
      exec -> math.max(0L, usedAfter - usedBefore)
    }
    val totalDelta = perExecDelta.values.sum

    val notes = Seq(
      "This measures storage-memory accounting deltas (max-remaining) around broadcast materialization; not raw heap usage.",
      "Run on a real cluster for meaningful per-executor deltas; local mode is noisy and often tiny.",
      "Ensure touchPartitions is large enough that tasks run on all executors, otherwise only a subset materializes the broadcast."
    )

    BroadcastStorageDeltaReport(
      label = label,
      beforeUsedBytes = before,
      afterUsedBytes = after,
      perExecutorDeltaBytes = perExecDelta,
      totalDeltaBytes = totalDelta,
      materializationTouchCount = touchCount,
      notes = notes
    )
  }

  def logBroadcastMemoryPlanReport(r: BroadcastMemoryPlanReport): Unit = log.info(s"[BSU] \n${r.prettyString}")
  def logBroadcastStorageDeltaReport(r: BroadcastStorageDeltaReport): Unit = log.info(s"[BSU] \n${r.prettyString}")
}