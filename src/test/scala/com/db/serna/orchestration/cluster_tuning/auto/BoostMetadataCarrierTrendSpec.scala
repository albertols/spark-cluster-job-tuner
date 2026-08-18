package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.refinement.SimpleJsonParser
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BoostMetadataCarrierTrendSpec extends AnyFunSuite with Matchers {

  private val reference =
    """{
      |  "clusterConf": { "c1": { "num_workers": 4, "cluster_max_total_cores": 96 } },
      |  "recipeSparkConf": {
      |    "_r.json": {
      |      "parallelizationFactor": 5,
      |      "appliedTrendScaleFactor": 1.5,
      |      "sparkOptsMap": {
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "4",
      |        "spark.dynamicAllocation.maxExecutors": "6",
      |        "spark.dynamicAllocation.initialExecutors": "4",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 32,
      |      "total_executor_maximum_allocated_memory_gb": 48
      |    }
      |  }
      |}""".stripMargin

  private val fresh =
    """{
      |  "clusterConf": { "c1": { "num_workers": 4, "cluster_max_total_cores": 96 } },
      |  "recipeSparkConf": {
      |    "_r.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "3",
      |        "spark.dynamicAllocation.initialExecutors": "2",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 24
      |    }
      |  }
      |}""".stripMargin

  test("prior trend min/initial/max and factor are carried into a freshly re-planned recipe") {
    val merged = BoostMetadataCarrier.injectPriorTrendScaling(fresh, reference, Set("_r.json"))
    val rc = SimpleJsonParser.parse(merged).recipes("_r.json")
    rc.sparkOptsMap("spark.dynamicAllocation.minExecutors") shouldBe "4"
    rc.sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "6"
    rc.sparkOptsMap("spark.dynamicAllocation.initialExecutors") shouldBe "4"
    rc.extraFields.get("appliedTrendScaleFactor") shouldBe Some("1.5")
    rc.totalExecutorMinAllocatedMemoryGb shouldBe 32
    rc.totalExecutorMaxAllocatedMemoryGb shouldBe 48
  }

  test("recipe without a prior trend factor is left unchanged") {
    val merged = BoostMetadataCarrier.injectPriorTrendScaling(fresh, fresh, Set("_r.json"))
    val rc = SimpleJsonParser.parse(merged).recipes("_r.json")
    rc.sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "3"
    rc.extraFields.get("appliedTrendScaleFactor") shouldBe None
  }
}
