package com.db.serna.utils.spark.cache

import com.db.serna.utils.spark.TestSparkSessionSupport
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class BroadcastSizingUtilsSpec extends AnyFunSuite {

  test("broadcastStorageDeltaReport runs end-to-end (local mode smoke test)") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      val payload = (1 to 50000).map(i => s"v_$i").toArray

      val delta = BroadcastSizingUtils.broadcastStorageDeltaReport(
        sc = spark.sparkContext,
        label = "broadcast_strings",
        payload = payload,
        touchPartitions = 8
      )

      println("[BSU] delta:\n" + delta.prettyString)

      assert(delta.beforeUsedBytes.nonEmpty)
      assert(delta.afterUsedBytes.nonEmpty)
      assert(delta.perExecutorDeltaBytes.nonEmpty)
      assert(delta.materializationTouchCount > 0L)
      assert(delta.totalDeltaBytes >= 0L)
    }
  }

  test("broadcastMemoryPlanReportFromDataFrame estimates collect()+broadcast expansion via sampling") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      val ssStable = spark
      import ssStable.implicits._

      val df = spark
        .range(0, 100000)
        .toDF("id")
        .withColumn("s", org.apache.spark.sql.functions.concat(org.apache.spark.sql.functions.lit("name_"), $"id"))

      val report = BroadcastSizingUtils.broadcastMemoryPlanReportFromDataFrame(
        label = "df_lookup_plan",
        df = df,
        spark = spark,
        expectedExecutors = 4,
        expectedConcurrentBroadcasts = 3,
        sampleN = 5000
      )

      println("[BSU] df plan:\n" + report.prettyString)

      assert(report.driverObjectGraphEstimateBytes > 0L)
      assert(report.estimatedPerExecutorTotalBroadcastBytes >= report.estimatedPerExecutorSingleBroadcastBytes)
      assert(report.estimatedClusterTotalBytes >= report.estimatedPerExecutorTotalBroadcastBytes)
    }
  }
}
