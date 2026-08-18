package com.db.serna.orchestration.cluster_tuning.single.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SimpleJsonParserTrendFieldSpec extends AnyFunSuite with Matchers {

  test("appliedTrendScaleFactor is parsed into extraFields") {
    val json =
      """{
        |  "clusterConf": { "c1": { "num_workers": 2, "cluster_max_total_cores": 96 } },
        |  "recipeSparkConf": {
        |    "_r.json": {
        |      "parallelizationFactor": 5,
        |      "appliedTrendScaleFactor": 1.3,
        |      "sparkOptsMap": {
        |        "spark.dynamicAllocation.enabled": "true",
        |        "spark.dynamicAllocation.minExecutors": "3",
        |        "spark.dynamicAllocation.maxExecutors": "5",
        |        "spark.dynamicAllocation.initialExecutors": "3",
        |        "spark.executor.cores": "8",
        |        "spark.executor.memory": "8g"
        |      },
        |      "total_executor_minimum_allocated_memory_gb": 24,
        |      "total_executor_maximum_allocated_memory_gb": 40
        |    }
        |  }
        |}""".stripMargin

    val cfg = SimpleJsonParser.parse(json)
    cfg.recipes("_r.json").extraFields.get("appliedTrendScaleFactor") shouldBe Some("1.3")
  }
}
