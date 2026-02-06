package com.db.serna.utils.spark

import org.apache.spark.sql.SparkSession

/**
 * TestSparkSessionSupport centralizes SparkSession creation/stopping with consistent configs.
 *
 * Usage:
 *   TestSparkSessionSupport.withSession { spark => ... }                      // default minimal local session
 *   TestSparkSessionSupport.withCacheSession { spark => ... }                 // cache-friendly config (Kryo, compression)
 *   TestSparkSessionSupport.withSession(TestSparkSessionSupport.CacheConf) { spark => ... } // explicit conf map
 */
object TestSparkSessionSupport {

  // Minimal local session config for CI
  val MinimalLocalConf: Map[String, String] = Map(
    "spark.master" -> "local[1]",
    "spark.ui.enabled" -> "false",
    "spark.executor.memory" -> "512m",
    "spark.executor.memoryOverhead" -> "128m",
    "spark.memory.fraction" -> "0.6",
    "spark.memory.storageFraction" -> "0.5"
  )

  // Cache-friendly config: Kryo serializer + compression
  val CacheConf: Map[String, String] = MinimalLocalConf ++ Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.inMemoryColumnarStorage.compressed" -> "true",
    "spark.rdd.compress" -> "true",
    // Adjust if specific tests require off-heap
    "spark.memory.offHeap.enabled" -> "false"
  )

  /**
   * Build, run, and tear down a SparkSession with the given config map.
   * Ensures active/default sessions are cleared and driver port is reset to avoid reuse issues.
   */
  def withSession[A](conf: Map[String, String])(f: SparkSession => A): A = {
    val builder = SparkSession
      .builder()
      .appName("TestSparkSession")
      .master(conf.getOrElse("spark.master", "local[1]"))

    val spark = conf.foldLeft(builder) { case (b, (k, v)) =>
      // Some keys like spark.master are applied earlier; skip duplicates
      if (k != "spark.master") b.config(k, v) else b
    }.getOrCreate()

    // Ensure this session is the active/default for Spark SQL implicits
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)

    try f(spark)
    finally {
      try spark.stop()
      finally {
        // Clear sessions to avoid "stopped SparkContext" reuse
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        // Avoid port conflicts in the same JVM
        System.clearProperty("spark.driver.port")
      }
    }
  }

  // Convenience overload so you can call: withSession { spark => ... }
  def withSession[A](f: SparkSession => A): A =
    withSession(MinimalLocalConf)(f)

  // Shortcut for cache-friendly tests (Kryo + compression): withCacheSession { spark => ... }
  def withCacheSession[A](f: SparkSession => A): A =
    withSession(CacheConf)(f)
}