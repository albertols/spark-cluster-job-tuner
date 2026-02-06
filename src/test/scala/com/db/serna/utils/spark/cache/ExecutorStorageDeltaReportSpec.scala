package com.db.serna.utils.spark.cache

import com.db.serna.utils.spark.TestSparkSessionSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

class ExecutorStorageDeltaReportSpec extends AnyFunSuite {

  test("executorStorageDeltaReport observational mode returns before snapshot only (no materialize)") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      val ssStable = spark;
      val df = spark.range(0, 1000).toDF("id").repartition(2)

      val (delta, countOpt, persistedByReport) =
        MemorySizingUtils.executorStorageDeltaReport(
          df = df,
          label = "obs_df",
          persistIfNeeded = false,
          storageLevel = StorageLevel.MEMORY_ONLY,
          materialize = false,
          unpersistAfter = true
        )

      println(s"[ESDR] observational before=${delta.beforeUsedBytes} after=${delta.afterUsedBytes} delta=${delta.perExecutorDeltaBytes}")

      assert(delta.beforeUsedBytes.nonEmpty)
      assert(delta.afterUsedBytes.isEmpty)
      assert(delta.perExecutorDeltaBytes.isEmpty)
      assert(delta.totalDeltaBytes == 0L)

      assert(countOpt.isEmpty)
      assert(!persistedByReport)
    }
  }

  test("executorStorageDeltaReport experimental mode can persist+materialize and produce deltas") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      val ssStable = spark;
      val df = spark.range(0, 20000).toDF("id").repartition(4)

      val (delta, countOpt, persistedByReport) =
        MemorySizingUtils.executorStorageDeltaReport(
          df = df,
          label = "exp_df",
          persistIfNeeded = true,
          storageLevel = StorageLevel.MEMORY_ONLY,
          materialize = true,
          unpersistAfter = true
        )

      println(s"[ESDR] experimental countOpt=$countOpt persistedByReport=$persistedByReport")
      println(s"[ESDR] before=${delta.beforeUsedBytes}")
      println(s"[ESDR] after=${delta.afterUsedBytes}")
      println(s"[ESDR] delta=${delta.perExecutorDeltaBytes} total=${delta.totalDeltaBytes}")

      assert(countOpt.contains(20000L))
      assert(persistedByReport || df.storageLevel != StorageLevel.NONE)

      assert(delta.beforeUsedBytes.nonEmpty)
      assert(delta.afterUsedBytes.nonEmpty)
      assert(delta.perExecutorDeltaBytes.nonEmpty)
      assert(delta.totalDeltaBytes >= 0L)
    }
  }
}