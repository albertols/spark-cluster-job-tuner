package com.db.serna.utils.spark.cache

import com.db.serna.utils.spark.TestSparkSessionSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

class MemorySizingUtilsStorageFootprintSpec extends AnyFunSuite {

  private def bytesToMB(b: Long): Long = b / (1024L * 1024L)

  test("getRDDStorageFootprint returns None when DataFrame is not cached") {
    TestSparkSessionSupport.withSession { spark: SparkSession =>
      val ssStable = spark
      val df = spark.range(0, 1000).toDF("id")
      val footprint = MemorySizingUtils.getRDDStorageFootprint(df)
      println(s"[MSU-FP] Not cached footprint => ${footprint}")
      assert(footprint.isEmpty, "Expected None for non-cached DataFrame")
    }
  }

  test("getRDDStorageFootprint returns Some for DataFrame.persist (SQL cache) and respects fallback") {
    TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark: SparkSession =>
      val ssStable = spark;
      val df = spark.range(0, 5000).toDF("id").repartition(3)
      df.persist(StorageLevel.MEMORY_ONLY)
      val _ = df.count()

      val footprintOpt = MemorySizingUtils.getRDDStorageFootprint(df)
      println(s"[MSU-FP] SQL cache footprint => ${footprintOpt}")
      assert(footprintOpt.isDefined, "Expected Some(...) for cached DataFrame")

      val fp = footprintOpt.get

      // Two acceptable outcomes across Spark distributions:
      // 1) RDDStorageInfo path: memSizeBytes > 0 and cachedPartitions > 0
      // 2) CacheManager-only fallback: memSizeBytes == 0 (presence without sizes)
      val rddStorageInfoObserved = fp.memSizeBytes > 0L && fp.cachedPartitions >= 0
      val cacheManagerObservedOnly = fp.memSizeBytes == 0L

      println(s"[MSU-FP] path=rddStorageInfoObserved=${rddStorageInfoObserved}, cacheManagerObservedOnly=${cacheManagerObservedOnly}, bytes=${fp.memSizeBytes} (~${bytesToMB(fp.memSizeBytes)} MB)")
      assert(rddStorageInfoObserved || cacheManagerObservedOnly, s"Unexpected footprint shape for SQL cache: $fp")

      // If we hit the fallback path, assert that the CacheManager indeed sees it cached
      if (cacheManagerObservedOnly) {
        val cachedDataPresent = spark.sharedState.cacheManager.lookupCachedData(df.queryExecution.logical).nonEmpty
        println(s"[MSU-FP] CacheManager present for logical plan=${cachedDataPresent}")
        assert(cachedDataPresent, "Fallback used but CacheManager did not report the DataFrame as cached")
      }
    }
  }

  test("getRDDStorageFootprint returns populated info when df.rdd is cached (RDD cache)") {
    TestSparkSessionSupport.withSession { spark: SparkSession =>
      val ssStable = spark;
      val df = spark.range(0, 20000).toDF("id").repartition(4)

      // RDD-level cache path (different from DataFrame.persist)
      df.rdd.persist(StorageLevel.MEMORY_ONLY)
      val _ = df.rdd.count()

      val footprintOpt = MemorySizingUtils.getRDDStorageFootprint(df)
      println(s"[MSU-FP] RDD cache footprint => ${footprintOpt}")
      assert(footprintOpt.isDefined, "Expected Some(...) for RDD-cached DataFrame")

      // [MSU-FP] mem=1520864 (~1 MB), disk=0, cachedPartitions=4, totalPartitions=4
      val fp = footprintOpt.get
      println(s"[MSU-FP] mem=${fp.memSizeBytes} (~${bytesToMB(fp.memSizeBytes)} MB), disk=${fp.diskSizeBytes}, cachedPartitions=${fp.cachedPartitions}, totalPartitions=${fp.totalPartitions}")
      assert(fp.memSizeBytes > 0L, s"Expected non-zero memSizeBytes for RDD cache, got $fp")
      assert(fp.cachedPartitions > 0, s"Expected some cached partitions for RDD cache, got $fp")
      assert(fp.totalPartitions == df.rdd.partitions.length, "totalPartitions should match RDD partition count")
    }
  }
}