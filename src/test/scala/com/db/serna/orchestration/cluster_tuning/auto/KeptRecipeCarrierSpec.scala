package com.db.serna.orchestration.cluster_tuning.auto

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class KeptRecipeCarrierSpec extends AnyFunSuite with Matchers {

  private val refRecipeJson: String =
    """{
      |  "clusterConf": {
      |    "mock-cluster-004": {
      |      "num_workers": 10,
      |      "master_machine_type": "n2-highcpu-48",
      |      "worker_machine_type": "n2-highcpu-48",
      |      "autoscaling_policy": "extra-large-workload-autoscaling",
      |      "tuner_version": "2099_01_01",
      |      "total_no_of_jobs": 1
      |    }
      |  },
      |  "recipeSparkConf": {
      |    "mock-recipe-feature-engineer.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 48,
      |      "total_executor_maximum_allocated_memory_gb": 96
      |    }
      |  }
      |}""".stripMargin

  test("tagPreservedRecipes adds lastTunedDate and keptWithoutCurrentDate to a recipe") {
    val out = KeptRecipeCarrier.tagPreservedRecipes(
      refRecipeJson,
      Set("mock-recipe-feature-engineer.json"),
      "2099_01_01"
    )
    out should include(""""lastTunedDate": "2099_01_01"""")
    out should include(""""keptWithoutCurrentDate": true""")
    // Original keys still present.
    out should include(""""parallelizationFactor": 5""")
    out should include(""""total_executor_minimum_allocated_memory_gb": 48""")
  }

  test("tagPreservedRecipes preserves an existing lastTunedDate (recursive carry)") {
    val withPriorTag = refRecipeJson.replace(
      """"parallelizationFactor": 5,""",
      """"parallelizationFactor": 5,
        |      "lastTunedDate": "2098_06_01",
        |      "keptWithoutCurrentDate": true,""".stripMargin
    )
    val out = KeptRecipeCarrier.tagPreservedRecipes(
      withPriorTag,
      Set("mock-recipe-feature-engineer.json"),
      "2099_01_01" // fallback would be this run's refDate; should be ignored
    )
    out should include(""""lastTunedDate": "2098_06_01"""")
    out should not include """"lastTunedDate": "2099_01_01""""
    // Should not duplicate the flag pair.
    out.split(""""lastTunedDate"""").length shouldBe 2 // appears once
    out.split(""""keptWithoutCurrentDate"""").length shouldBe 2
  }

  test("tagPreservedRecipes ignores recipes that don't exist in recipeSparkConf") {
    val out = KeptRecipeCarrier.tagPreservedRecipes(
      refRecipeJson,
      Set("does-not-exist.json"),
      "2099_01_01"
    )
    // Returns the prettified json unchanged in semantics; no flags introduced.
    out should not include """"keptWithoutCurrentDate""""
    out should not include """"lastTunedDate""""
  }

  test("tagPreservedRecipes does not affect clusterConf entries with the same name") {
    // Cluster name shares structure prefix; ensure we don't mutate clusterConf.
    val out = KeptRecipeCarrier.tagPreservedRecipes(
      refRecipeJson,
      Set("mock-cluster-004"), // a clusterConf key, not a recipeSparkConf key
      "2099_01_01"
    )
    out should not include """"keptWithoutCurrentDate""""
  }

  test("extractRecipeBlock returns the raw recipe object string") {
    val block = KeptRecipeCarrier.extractRecipeBlock(refRecipeJson, "mock-recipe-feature-engineer.json")
    block.isDefined shouldBe true
    block.get should startWith("{")
    block.get should endWith("}")
    block.get should include(""""parallelizationFactor": 5""")
  }

  test("mergeRecipeBlocks adds new recipes and skips existing ones") {
    val freshJson =
      """{
        |  "clusterConf": {
        |    "mock-cluster-004": { "num_workers": 12 }
        |  },
        |  "recipeSparkConf": {
        |    "live-recipe.json": { "parallelizationFactor": 5 }
        |  }
        |}""".stripMargin

    val carriedBlock = KeptRecipeCarrier.extractRecipeBlock(refRecipeJson, "mock-recipe-feature-engineer.json").get
    val tagged = KeptRecipeCarrier.tagPreservedRecipes(
      s"""{"recipeSparkConf":{"x":$carriedBlock}}""",
      Set("x"),
      "2099_01_01"
    )
    // Reuse the tagged form to extract a tagged block to merge in.
    val taggedBlock = KeptRecipeCarrier.extractRecipeBlock(tagged, "x").get

    val merged = KeptRecipeCarrier.mergeRecipeBlocks(
      freshJson,
      Seq("mock-recipe-feature-engineer.json" -> taggedBlock, "live-recipe.json" -> "{}")
    )
    merged should include(""""mock-recipe-feature-engineer.json"""")
    merged should include(""""live-recipe.json"""")
    merged should include(""""keptWithoutCurrentDate": true""")
    // live-recipe wasn't replaced — its parallelizationFactor still there.
    merged should include(""""parallelizationFactor": 5""")
    // live-recipe should appear only once (not duplicated).
    merged.split(""""live-recipe.json"""").length shouldBe 2
  }
}