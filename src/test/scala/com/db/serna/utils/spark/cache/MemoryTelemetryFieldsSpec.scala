package com.db.serna.utils.spark.cache

import com.db.serna.utils.spark.TestSparkSessionSupport
import com.db.serna.utils.spark.parallelism.ExecutorTrackingListener
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class MemoryTelemetryFieldsSpec extends AnyFunSuite {

  private def findField(json: String, key: String): Option[String] = {
    val pattern = ("""\"""" + key + """\":\"([^\"]*)\"""").r
    pattern.findFirstMatchIn(json).map(_.group(1))
  }

  test("ExecutorTrackingListener emits memory telemetry fields with expected values (minimal CI settings)") {
    // Customize conf for this test to include off-heap enabled and small size
    val conf = TestSparkSessionSupport.MinimalLocalConf ++ Map(
      "spark.memory.offHeap.enabled" -> "true",
      "spark.memory.offHeap.size" -> "32m",
      // Keep explicit overhead to make assertions deterministic
      "spark.executor.memoryOverhead" -> "128m",
      "spark.yarn.executor.memoryOverhead" -> "128m"
    )

    TestSparkSessionSupport.withSession(conf) { spark: SparkSession =>
      val scConf = spark.sparkContext.getConf
      println(s"[MTF] master=${spark.sparkContext.master}")
      println(s"[MTF] spark.executor.memory=${scConf.get("spark.executor.memory")}")
      println(
        s"[MTF] spark.executor.memoryOverhead=${scConf.getOption("spark.executor.memoryOverhead").getOrElse("unset")}"
      )
      println(
        s"[MTF] spark.yarn.executor.memoryOverhead=${scConf.getOption("spark.yarn.executor.memoryOverhead").getOrElse("unset")}"
      )
      println(s"[MTF] spark.memory.fraction=${scConf.get("spark.memory.fraction", "0.6")}")
      println(s"[MTF] spark.memory.storageFraction=${scConf.get("spark.memory.storageFraction", "0.5")}")
      println(s"[MTF] spark.memory.offHeap.enabled=${scConf.getBoolean("spark.memory.offHeap.enabled", false)}")
      println(s"[MTF] spark.memory.offHeap.size=${scConf.get("spark.memory.offHeap.size", "0")}")

      val listener = new ExecutorTrackingListener(
        sparkSession = spark,
        repartitionFactor = 5,
        eventTypeFamily = "TEST",
        baseContext = Map("pipelineId" -> "pipeline-123")
      )

      listener.onApplicationEnd(new org.apache.spark.scheduler.SparkListenerApplicationEnd(System.currentTimeMillis()))

      val events = listener.getEmittedEvents
      println(s"[MTF] emittedEvents.count=${events.size}")
      assert(events.nonEmpty, "No events emitted by ExecutorTrackingListener")

      val statusEventOpt = events.reverse.find(_.contains("\"event_type\":\"executor_status\""))
      assert(statusEventOpt.isDefined, "No executor_status record found in emitted events")
      val status = statusEventOpt.get

      val keysToPrint = Seq(
        "executor_onheap_memory_mb",
        "executor_memory_overhead_mb_effective",
        "spark_memory_fraction",
        "spark_memory_storage_fraction",
        "onheap_unified_region_mb",
        "onheap_initial_execution_mb",
        "onheap_initial_storage_mb",
        "offheap_enabled",
        "offheap_size_mb",
        "offheap_unified_region_mb",
        "offheap_initial_execution_mb",
        "offheap_initial_storage_mb",
        "execution_memory_budget_mb",
        "storage_memory_budget_mb",
        "memory_overhead_covers_offheap"
      )
      val printed = keysToPrint.map(k => s"$k=${findField(status, k).getOrElse("N/A")}")
      println(s"[MTF] telemetry: ${printed.mkString(", ")}")

      // Presence checks (schema)
      Seq(
        "cluster_manager",
        "executor_onheap_memory_mb",
        "executor_memory_overhead_mb_effective",
        "executor_memory_overhead_source",
        "offheap_enabled",
        "offheap_size_mb",
        "spark_memory_fraction",
        "spark_memory_storage_fraction",
        "onheap_unified_region_mb",
        "onheap_initial_execution_mb",
        "onheap_initial_storage_mb",
        "offheap_unified_region_mb",
        "offheap_initial_execution_mb",
        "offheap_initial_storage_mb",
        "execution_memory_budget_mb",
        "storage_memory_budget_mb",
        "total_onheap_memory_mb",
        "total_overhead_memory_mb",
        "total_offheap_memory_mb",
        "total_allocated_container_memory_mb",
        "executorMemoryMB",
        "executorMemoryOverheadMB",
        "total_allocated_executor_memory_mb",
        "memory_overhead_covers_offheap"
      ).foreach { k =>
        assert(findField(status, k).isDefined, s"Missing telemetry field: $k")
      }

      // Deterministic assertions for minimal settings:
      assert(findField(status, "executor_onheap_memory_mb").contains("512"))
      assert(findField(status, "executor_memory_overhead_mb_effective").contains("128"))
      assert(findField(status, "offheap_enabled").contains("true"))
      assert(findField(status, "offheap_size_mb").contains("32"))
      assert(findField(status, "spark_memory_fraction").contains("0.6"))
      assert(findField(status, "spark_memory_storage_fraction").contains("0.5"))

      // Based on integer floors from byte math:
      assert(findField(status, "onheap_unified_region_mb").contains("307"))
      assert(findField(status, "onheap_initial_storage_mb").contains("153"))
      assert(findField(status, "onheap_initial_execution_mb").contains("153"))
      assert(findField(status, "offheap_unified_region_mb").contains("32"))
      assert(findField(status, "offheap_initial_storage_mb").contains("16"))
      assert(findField(status, "offheap_initial_execution_mb").contains("16"))
      assert(findField(status, "execution_memory_budget_mb").contains("169"))
      assert(findField(status, "storage_memory_budget_mb").contains("169"))
      assert(findField(status, "memory_overhead_covers_offheap").contains("true"))
    }
  }
}
