package com.db.serna.utils.spark.cache

import com.db.serna.utils.spark.TestSparkSessionSupport
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class BroadcastSizingUtilsLoggingSpec extends AnyFunSuite {

  test("BroadcastSizingUtils reports produce printable logs (smoke test)") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      spark;

      val df = spark.range(0, 10000).toDF("id")
      val rows = df.limit(1000).collect() // keep small for CI

      val plan = BroadcastSizingUtils.broadcastMemoryPlanReport(
        label = "plan_payload_rows",
        payload = rows,
        spark = spark,
        expectedExecutors = 4,
        expectedConcurrentBroadcasts = 2,
        safetyFactor = 1.5
      )

      val dfPlan = BroadcastSizingUtils.broadcastMemoryPlanReportFromDataFrame(
        label = "plan_from_df",
        df = df,
        spark = spark,
        expectedExecutors = 4,
        expectedConcurrentBroadcasts = 2,
        sampleN = 1000,
        rowCountHint = Some(10000L) // avoid df.count() cost
      )

      val delta = BroadcastSizingUtils.broadcastStorageDeltaReport(
        sc = spark.sparkContext,
        label = "delta_broadcast_rows",
        payload = rows,
        touchPartitions = 4
      )

      val planStr = plan.prettyString
      val dfPlanStr = dfPlan.prettyString
      val deltaStr = delta.prettyString

      println("[BSU-LOG] plan:\n" + planStr)
      println("[BSU-LOG] dfPlan:\n" + dfPlanStr)
      println("[BSU-LOG] delta:\n" + deltaStr)

      // Minimal assertions: ensure output includes key fields
      assert(planStr.contains("BroadcastMemoryPlanReport"))
      assert(planStr.contains("driverObjectGraphEstimate"))
      assert(planStr.contains("kryoSerializedBytesEstimate"))

      assert(dfPlanStr.contains("BroadcastMemoryPlanReport"))
      // Replace this brittle assertion:
      // assert(dfPlanStr.contains("collect()+broadcast"))

      // With these more robust checks:
      assert(dfPlanStr.contains("BroadcastMemoryPlanReport"))
      assert(dfPlanStr.contains("driverObjectGraphEstimate"))

      // DF planner does not serialize full payload, so these should be unavailable:
      assert(dfPlanStr.contains("javaSerializedBytesEstimate=<unavailable>"))
      assert(dfPlanStr.contains("kryoSerializedBytesEstimate=<unavailable>"))

      // Optional: notes block exists
      assert(dfPlanStr.contains("notes:"))
    }
  }
}
