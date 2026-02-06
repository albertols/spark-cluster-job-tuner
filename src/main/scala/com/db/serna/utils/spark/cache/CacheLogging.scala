package com.db.serna.utils.spark.cache

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
 * Spark listener and helpers that log main.scala.com.db.serna.utils.spark.cache sizes and executor storage/memory usage.
 */
object CacheLogging {
  private val logger = LoggerFactory.getLogger(getClass)

  def register(sc: SparkContext): Unit = {
    sc.addSparkListener(new CacheStorageListener(sc))
    logger.info("Registered CacheStorageListener for main.scala.com.db.serna.utils.spark.cache/memory logging.")
  }

  def logDfCacheStats(df: DataFrame, label: String = "df"): Unit = {
    val report = MemorySizingUtils.dataFrameMemoryReport(
      df,
      MemorySizingUtils.DataFrameMemoryReportConfig(
        label = label,
        persistIfNeeded = false,
        materialize = false
      )
    )
    logger.info(s"[CacheLogging] \n${report.prettyString}")
  }

  /** Listener that logs block updates (main.scala.com.db.serna.utils.spark.cache puts/evictions) and stage completion summaries. */
  class CacheStorageListener(sc: SparkContext) extends SparkListener {
    private val logger = LoggerFactory.getLogger(getClass)

    override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
      val info = event.blockUpdatedInfo
      val blockId = info.blockId
      val level = info.storageLevel
      val mem = info.memSize
      val disk = info.diskSize
      val execId = info.blockManagerId.executorId
      logger.info(
        s"BlockUpdated: blockId=$blockId, executor=$execId, level=${level.description}, " +
          s"mem=${MemorySizingUtils.fmtBytes(mem)}, disk=${MemorySizingUtils.fmtBytes(disk)}"
      )
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageInfo = stageCompleted.stageInfo
      logger.info(s"StageCompleted: stage=${stageInfo.stageId} (${stageInfo.name})")
      sc.getRDDStorageInfo.foreach { info =>
        logger.info(
          s"RDDStorage: id=${info.id}, name=${info.name}, level=${info.storageLevel.description}, " +
            s"memSize=${MemorySizingUtils.fmtBytes(info.memSize)}, diskSize=${MemorySizingUtils.fmtBytes(info.diskSize)}, " +
            s"cachedPartitions=${info.numCachedPartitions}/${info.numPartitions}"
        )
      }
    }
  }
}