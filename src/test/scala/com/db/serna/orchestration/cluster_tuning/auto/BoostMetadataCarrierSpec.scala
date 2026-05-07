package com.db.serna.orchestration.cluster_tuning.auto

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BoostMetadataCarrierSpec extends AnyFunSuite with Matchers {

  // Reference JSON: this recipe was previously b16-boosted (1.5x) on a prior run.
  // spark.executor.memory was raised 8g → 12g, totals re-derived.
  private val refJson: String =
    """{
      |  "clusterConf": {
      |    "mock-cluster-001": {
      |      "num_workers": 4,
      |      "master_machine_type": "n2-standard-32",
      |      "worker_machine_type": "n2-standard-32"
      |    }
      |  },
      |  "recipeSparkConf": {
      |    "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json": {
      |      "parallelizationFactor": 5,
      |      "appliedMemoryHeapBoostFactor": 1.5,
      |      "sparkOptsMap": {
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "3",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "12g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 24,
      |      "total_executor_maximum_allocated_memory_gb": 36
      |    }
      |  }
      |}""".stripMargin

  // Current JSON: cluster was re-planned fresh (BoostResources). Recipe uses baseline
  // 8g and totals 16/24. NO appliedMemoryHeapBoostFactor key.
  private val curJson: String =
    """{
      |  "clusterConf": {
      |    "mock-cluster-001": {
      |      "num_workers": 4,
      |      "master_machine_type": "n2-standard-32",
      |      "worker_machine_type": "n2-standard-32"
      |    }
      |  },
      |  "recipeSparkConf": {
      |    "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "3",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 24
      |    }
      |  }
      |}""".stripMargin

  test("injectPriorBoosts copies factor + boosted memory + re-derived totals from ref") {
    val out = BoostMetadataCarrier.injectPriorBoosts(
      curJson,
      refJson,
      Set("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json")
    )

    out should include(""""appliedMemoryHeapBoostFactor": 1.5""")
    out should include(""""spark.executor.memory": "12g"""")
    // Totals re-derived from cur's 2/3 executor counts × 12g.
    out should include(""""total_executor_minimum_allocated_memory_gb": 24""")
    out should include(""""total_executor_maximum_allocated_memory_gb": 36""")
    // Min/max executors and cores are preserved from cur's fresh plan.
    out should include(""""spark.dynamicAllocation.minExecutors": "2"""")
    out should include(""""spark.dynamicAllocation.maxExecutors": "3"""")
    out should include(""""spark.executor.cores": "8"""")
  }

  test("injectPriorBoosts is a no-op when ref has no prior boost factor") {
    val refNoBoost = refJson.replaceAll(""""appliedMemoryHeapBoostFactor"\s*:\s*[\d.]+\s*,?\s*""", "")
    val out = BoostMetadataCarrier.injectPriorBoosts(
      curJson,
      refNoBoost,
      Set("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json")
    )
    out shouldBe curJson
    out should not include """"appliedMemoryHeapBoostFactor""""
  }

  test("injectPriorBoosts skips recipes missing from cur JSON") {
    val out = BoostMetadataCarrier.injectPriorBoosts(
      curJson,
      refJson,
      Set("_does_not_exist.json")
    )
    out shouldBe curJson
  }

  test("injectPriorBoosts is idempotent — running twice yields the same result") {
    val once = BoostMetadataCarrier.injectPriorBoosts(
      curJson,
      refJson,
      Set("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json")
    )
    val twice = BoostMetadataCarrier.injectPriorBoosts(
      once,
      refJson,
      Set("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json")
    )
    // Both have the same factor + memory + totals; whitespace may shift due to
    // pretty-printing but the boost-relevant payload is identical.
    twice should include(""""appliedMemoryHeapBoostFactor": 1.5""")
    twice should include(""""spark.executor.memory": "12g"""")
    twice should include(""""total_executor_maximum_allocated_memory_gb": 36""")
    // Factor key only appears ONCE per recipe (not duplicated).
    twice.split(""""appliedMemoryHeapBoostFactor"""").length shouldBe 2
  }

  test("injectPriorBoosts handles a higher cumulative factor (e.g. 2.25)") {
    val refWithCum = refJson
      .replace(
        """"appliedMemoryHeapBoostFactor": 1.5,""",
        """"appliedMemoryHeapBoostFactor": 2.25,"""
      )
      .replace(""""spark.executor.memory": "12g"""", """"spark.executor.memory": "18g"""")
      .replace(
        """"total_executor_minimum_allocated_memory_gb": 24""",
        """"total_executor_minimum_allocated_memory_gb": 36"""
      )
      .replace(
        """"total_executor_maximum_allocated_memory_gb": 36""",
        """"total_executor_maximum_allocated_memory_gb": 54"""
      )

    val out = BoostMetadataCarrier.injectPriorBoosts(
      curJson,
      refWithCum,
      Set("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json")
    )
    out should include(""""appliedMemoryHeapBoostFactor": 2.25""")
    out should include(""""spark.executor.memory": "18g"""")
    // 2 minExec × 18g / 3 maxExec × 18g
    out should include(""""total_executor_minimum_allocated_memory_gb": 36""")
    out should include(""""total_executor_maximum_allocated_memory_gb": 54""")
  }
}
