package com.db.serna.utils.spark.cache

import com.db.serna.utils.spark.TestSparkSessionSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

class DataFrameMemoryReportSpec extends AnyFunSuite {

  test("dataFrameMemoryReport observational mode returns structure without side effects") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      val ssStable = spark;

      val df = spark.range(0, 2000).toDF("id").repartition(2)

      val report = MemorySizingUtils.dataFrameMemoryReport(
        df,
        MemorySizingUtils.DataFrameMemoryReportConfig(
          label = "obs_df",
          storageLevel = StorageLevel.MEMORY_ONLY,
          persistIfNeeded = false,
          materialize = false
        )
      )

      println(s"[DFMR] observational:\n${report.prettyString}")

      assert(report.label == "obs_df")
      assert(report.rddId == df.rdd.id)
      assert(report.partitions == df.rdd.getNumPartitions)
      assert(!report.materialized)
      assert(report.executorUsedBeforeBytes.nonEmpty) // local[1] still has driver executor entry
      assert(report.executorUsedAfterBytes.isEmpty)
      assert(report.executorUsedDeltaBytes.isEmpty)
      assert(report.totalExecutorUsedDeltaBytes == 0L)
    }
  }

  test("dataFrameMemoryReport experimental mode can persist+materialize and produce non-negative delta") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      val ssStable = spark;

      val df = spark.range(0, 20000).toDF("id").repartition(4)

      val report = MemorySizingUtils.dataFrameMemoryReport(
        df,
        MemorySizingUtils.DataFrameMemoryReportConfig(
          label = "exp_df",
          storageLevel = StorageLevel.MEMORY_ONLY,
          persistIfNeeded = true,
          materialize = true,
          unpersistAfter = true
        )
      )

      println(s"[DFMR] experimental:\n${report.prettyString}")

      // Deterministic assertions:
      assert(report.materialized)
      assert(report.materializedCount.contains(20000L))

      // This tells us we actually called df.persist() in this code path:
      assert(report.persistedByReport, "Expected report to persist the DataFrame in experimental mode.")

      // Delta snapshots should be present when materialize=true
      assert(report.executorUsedBeforeBytes.nonEmpty)
      assert(report.executorUsedAfterBytes.nonEmpty)
      assert(report.executorUsedDeltaBytes.nonEmpty)
      assert(report.totalExecutorUsedDeltaBytes >= 0L)

      // Non-deterministic signals: print for diagnostics but do not assert
      val anyCachedRddExists = spark.sparkContext.getRDDStorageInfo.exists(_.numCachedPartitions > 0)
      println(
        s"[DFMR] non-deterministic cache signals: " +
          s"cacheManagerHasEntry=${report.cacheManagerHasEntry}, " +
          s"rddStorageFootprint.isDefined=${report.rddStorageFootprint.isDefined}, " +
          s"anyCachedRddExists=$anyCachedRddExists"
      )
    }
  }
}