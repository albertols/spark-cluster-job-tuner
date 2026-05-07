package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.refinement.SimpleJsonParser
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SimpleJsonParserSpec extends AnyFunSuite with Matchers {

  private val sampleAutoScaleJson: String =
    """{
      |  "clusterConf": {
      |    "cluster-wf-dmr-load-t-02-15-0215": {
      |      "num_workers": 10,
      |      "master_machine_type": "n2d-highcpu-48",
      |      "worker_machine_type": "n2d-highcpu-48",
      |      "autoscaling_policy": "extra-large-workload-autoscaling",
      |      "tuner_version": "2025_20_12",
      |      "total_no_of_jobs": 56,
      |      "cluster_max_total_memory_gb": 960,
      |      "cluster_max_total_cores": 480,
      |      "accumulated_max_total_memory_per_jobs_gb": 1344
      |    }
      |  },
      |  "recipeSparkConf": {
      |    "_ETL_m_DQ3_ODS_TGL05.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.closure.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "3",
      |        "spark.dynamicAllocation.initialExecutors": "2",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 24
      |    },
      |    "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.closure.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "5",
      |        "spark.dynamicAllocation.initialExecutors": "2",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 40
      |    }
      |  }
      |}""".stripMargin

  private val sampleManualJson: String =
    """{
      |  "clusterConf": {
      |    "cluster-wf-dmr-load-t-02-15-0215": {
      |      "num_workers": 10,
      |      "master_machine_type": "n2d-highcpu-48",
      |      "worker_machine_type": "n2d-highcpu-48",
      |      "autoscaling_policy": "extra-large-workload-autoscaling",
      |      "tuner_version": "2025_20_12",
      |      "total_no_of_jobs": 2,
      |      "cluster_max_total_memory_gb": 960,
      |      "cluster_max_total_cores": 480,
      |      "accumulated_max_total_memory_per_jobs_gb": 808
      |    }
      |  },
      |  "recipeSparkConf": {
      |    "_ETL_m_DQ3_ODS_TGL05.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.closure.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.executor.instances": "2",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 16
      |    }
      |  }
      |}""".stripMargin

  // ── parse: cluster name ───────────────────────────────────────────────────

  test("parse extracts cluster name from auto-scale JSON") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    config.clusterName shouldBe "cluster-wf-dmr-load-t-02-15-0215"
  }

  test("parse extracts cluster name from manual JSON") {
    val config = SimpleJsonParser.parse(sampleManualJson)
    config.clusterName shouldBe "cluster-wf-dmr-load-t-02-15-0215"
  }

  // ── parse: clusterConf fields ─────────────────────────────────────────────

  test("parse extracts clusterConf numeric fields") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val fields = config.clusterConfFields.toMap
    fields("num_workers") shouldBe "10"
    fields("cluster_max_total_memory_gb") shouldBe "960"
    fields("total_no_of_jobs") shouldBe "56"
  }

  test("parse extracts clusterConf string fields") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val fields = config.clusterConfFields.toMap
    fields("master_machine_type") shouldBe "n2d-highcpu-48"
    fields("autoscaling_policy") shouldBe "extra-large-workload-autoscaling"
  }

  test("parse preserves clusterConf field order") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val keys = config.clusterConfFields.map(_._1)
    keys shouldBe Seq(
      "num_workers",
      "master_machine_type",
      "worker_machine_type",
      "autoscaling_policy",
      "tuner_version",
      "total_no_of_jobs",
      "cluster_max_total_memory_gb",
      "cluster_max_total_cores",
      "accumulated_max_total_memory_per_jobs_gb"
    )
  }

  // ── parse: recipes ────────────────────────────────────────────────────────

  test("parse extracts all recipe filenames from auto-scale JSON") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    config.recipes.keys should contain allOf (
      "_ETL_m_DQ3_ODS_TGL05.json",
      "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json"
    )
    config.recipes should have size 2
  }

  test("parse preserves recipe order") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    config.recipeOrder shouldBe Seq(
      "_ETL_m_DQ3_ODS_TGL05.json",
      "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json"
    )
  }

  test("parse extracts recipe filenames from manual JSON") {
    val config = SimpleJsonParser.parse(sampleManualJson)
    config.recipes.keys should contain("_ETL_m_DQ3_ODS_TGL05.json")
    config.recipes should have size 1
  }

  // ── parse: sparkOptsMap ───────────────────────────────────────────────────

  test("parse extracts spark.executor.memory from auto-scale recipe") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val rc = config.recipes("_ETL_m_DQ3_ODS_TGL05.json")
    rc.sparkOptsMap("spark.executor.memory") shouldBe "8g"
  }

  test("parse extracts dynamic allocation settings") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val rc = config.recipes("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json")
    rc.sparkOptsMap("spark.dynamicAllocation.enabled") shouldBe "true"
    rc.sparkOptsMap("spark.dynamicAllocation.minExecutors") shouldBe "2"
    rc.sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "5"
  }

  test("parse extracts spark.executor.instances from manual recipe") {
    val config = SimpleJsonParser.parse(sampleManualJson)
    val rc = config.recipes("_ETL_m_DQ3_ODS_TGL05.json")
    rc.sparkOptsMap("spark.executor.instances") shouldBe "2"
    rc.sparkOptsMap should not contain key("spark.dynamicAllocation.enabled")
  }

  // ── parse: parallelizationFactor and memory totals ────────────────────────

  test("parse extracts parallelizationFactor") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    config.recipes("_ETL_m_DQ3_ODS_TGL05.json").parallelizationFactor shouldBe 5
  }

  test("parse extracts total executor memory fields") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val rc = config.recipes("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json")
    rc.totalExecutorMinAllocatedMemoryGb shouldBe 16
    rc.totalExecutorMaxAllocatedMemoryGb shouldBe 40
  }

  test("parse extracts total executor memory for manual recipe") {
    val config = SimpleJsonParser.parse(sampleManualJson)
    val rc = config.recipes("_ETL_m_DQ3_ODS_TGL05.json")
    rc.totalExecutorMinAllocatedMemoryGb shouldBe 16
    rc.totalExecutorMaxAllocatedMemoryGb shouldBe 16
  }

  // ── parse: extraFields ────────────────────────────────────────────────────

  test("parse initialises extraFields as empty") {
    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    config.recipes("_ETL_m_DQ3_ODS_TGL05.json").extraFields shouldBe empty
  }

  // ── parseMemoryGb ─────────────────────────────────────────────────────────

  test("parseMemoryGb: 8g -> 8") {
    SimpleJsonParser.parseMemoryGb("8g") shouldBe 8
  }

  test("parseMemoryGb: 12g -> 12") {
    SimpleJsonParser.parseMemoryGb("12g") shouldBe 12
  }

  test("parseMemoryGb: handles whitespace") {
    SimpleJsonParser.parseMemoryGb("  16g ") shouldBe 16
  }

  test("parseMemoryGb: handles uppercase G") {
    SimpleJsonParser.parseMemoryGb("8G") shouldBe 8
  }

  test("parseMemoryGb: invalid returns 0") {
    SimpleJsonParser.parseMemoryGb("invalid") shouldBe 0
  }

  // ── parse: JSON with driver overrides ─────────────────────────────────────

  test("parse handles clusterConf with driver override fields") {
    val jsonWithDriverOverride =
      """{
        |  "clusterConf": {
        |    "cluster-a": {
        |      "num_workers": 5,
        |      "master_machine_type": "n2-standard-32",
        |      "worker_machine_type": "n2-highcpu-32",
        |      "autoscaling_policy": "default-policy",
        |      "tuner_version": "2025_20_12",
        |      "total_no_of_jobs": 10,
        |      "cluster_max_total_memory_gb": 320,
        |      "cluster_max_total_cores": 160,
        |      "accumulated_max_total_memory_per_jobs_gb": 200,
        |      "driver_memory_gb": 8,
        |      "driver_cores": 4,
        |      "driver_memory_overhead_gb": 2,
        |      "diagnostic_reason": "YARN driver eviction (exit 247) x2"
        |    }
        |  },
        |  "recipeSparkConf": {
        |    "_ETL_test.json": {
        |      "parallelizationFactor": 5,
        |      "sparkOptsMap": {
        |        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        |        "spark.executor.instances": "1",
        |        "spark.executor.cores": "8",
        |        "spark.executor.memory": "8g"
        |      },
        |      "total_executor_minimum_allocated_memory_gb": 8,
        |      "total_executor_maximum_allocated_memory_gb": 8
        |    }
        |  }
        |}""".stripMargin

    val config = SimpleJsonParser.parse(jsonWithDriverOverride)
    config.clusterName shouldBe "cluster-a"
    val fields = config.clusterConfFields.toMap
    fields("driver_memory_gb") shouldBe "8"
    fields("driver_cores") shouldBe "4"
    fields("diagnostic_reason") shouldBe "YARN driver eviction (exit 247) x2"
    config.recipes should have size 1
  }
}
