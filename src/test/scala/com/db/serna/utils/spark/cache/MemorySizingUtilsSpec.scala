package com.db.serna.utils.spark.cache

import com.db.serna.utils.spark.TestSparkSessionSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

class MemorySizingUtilsSpec extends AnyFunSuite {

  private def bytesToMB(b: Long): Long = b / (1024L * 1024L)

  test("estimateObjectSizeBytes returns a reasonable lower bound") {
    val arr = Array.fill[Byte](1024)(1)
    val size = MemorySizingUtils.estimateObjectSizeBytes(arr)
    println(s"[MSU] estimateObjectSizeBytes for 1KB array => bytes=$size (~${bytesToMB(size)} MB)")
    assert(size >= 1024L)
  }

  test("dataFrameMemoryReport composes observational + experimental signals") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      spark;
      val df = spark.range(0, 8000).toDF("id").repartition(2)

      val report = MemorySizingUtils.dataFrameMemoryReport(
        df,
        MemorySizingUtils.DataFrameMemoryReportConfig(
          label = "compose_df",
          storageLevel = StorageLevel.MEMORY_ONLY,
          persistIfNeeded = true,
          materialize = true,
          unpersistAfter = true
        )
      )

      println(s"[MSU] compose report:\n${report.prettyString}")
      assert(report.materializedCount.contains(8000L))
      assert(report.executorUsedDeltaBytes.nonEmpty)
      assert(report.totalExecutorUsedDeltaBytes >= 0L)
    }
  }

  test("getRDDStorageFootprint returns info or presence when SQL cache manager is used") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      spark;
      val df = spark.range(0, 1000).toDF("id")
      df.persist(StorageLevel.MEMORY_ONLY)
      val _ = df.count()

      val infoOpt = MemorySizingUtils.getRDDStorageFootprint(df)
      println(s"[MSU] RDDStorageFootprint(SQL) => ${infoOpt}")
      assert(infoOpt.isDefined)
    }
  }

  test("sparkUiStorageUrl returns a local UI storage URL when enabled") {
    val uiConf = TestSparkSessionSupport.MinimalLocalConf +
      ("spark.ui.enabled" -> "true") +
      ("spark.ui.port" -> "4040")

    TestSparkSessionSupport.withSession(uiConf) { spark: SparkSession =>
      val urlOpt = MemorySizingUtils.sparkUiStorageUrl(spark.sparkContext)
      println(s"[MSU] sparkUiStorageUrl => ${urlOpt.getOrElse("none")}")
      assert(urlOpt.exists(_.contains("/storage/")))
    }
  }

  test("bytesToMB performs integer division and floors the result") {
    val cases = Seq(
      0L -> 0L,
      1L -> 0L,
      1024L -> 0L,
      (512L * 1024L) -> 0L,
      (1024L * 1024L) -> 1L,
      (1536L * 1024L) -> 1L,
      (2L * 1024L * 1024L) -> 2L
    )

    cases.foreach { case (bytes, expectedMB) =>
      val actual = MemorySizingUtils.bytesToMB(bytes)
      println(s"[MSU-Format] bytesToMB: bytes=$bytes => MB=$actual (expected=$expectedMB)")
      assert(actual === expectedMB)
    }
  }
}
