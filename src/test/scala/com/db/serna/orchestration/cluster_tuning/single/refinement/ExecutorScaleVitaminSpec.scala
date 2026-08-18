package com.db.serna.orchestration.cluster_tuning.single.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorScaleVitaminSpec extends AnyFunSuite with Matchers {

  private def daRecipe(min: Int, max: Int, mem: String = "8g", scaleFactor: Option[Double] = None): RecipeConfig =
    RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.dynamicAllocation.minExecutors" -> min.toString,
        "spark.dynamicAllocation.maxExecutors" -> max.toString,
        "spark.executor.memory" -> mem
      ),
      totalExecutorMinAllocatedMemoryGb = min * SimpleJsonParser.parseMemoryGb(mem),
      totalExecutorMaxAllocatedMemoryGb = max * SimpleJsonParser.parseMemoryGb(mem),
      extraFields = scaleFactor.map("appliedExecutorScaleFactor" -> _.toString).toMap
    )

  private def manualRecipe(instances: Int): RecipeConfig =
    RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.executor.instances" -> instances.toString,
        "spark.executor.memory" -> "8g"
      ),
      totalExecutorMinAllocatedMemoryGb = instances * 8,
      totalExecutorMaxAllocatedMemoryGb = instances * 8,
      extraFields = Map.empty
    )

  test("New: untagged DA recipe + fresh signal → maxExecutors x1.5") {
    val v = new ExecutorScaleVitamin(boostFactor = 1.5)
    val recipes = Map("_recipe.json" -> daRecipe(min = 2, max = 3))
    val signal = ExecutorScaleSignal("c1", "_recipe.json", "", "p95_job_duration_ms", 5.0, 3, 2.9)

    val boosts = v.computeBoosts(Seq(signal), recipes, Seq(signal))
    boosts should have size 1
    val b = boosts.head.asInstanceOf[ExecutorScaleBoost]
    b.state shouldBe BoostState.New
    b.originalMaxExecutors shouldBe 3
    b.boostedMaxExecutors shouldBe 5 // ceil(3 * 1.5) = 5
    b.effectiveCumulativeFactor shouldBe 1.5

    val applied = v.applyBoosts(boosts, recipes)
    applied("_recipe.json").sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "5"
    applied("_recipe.json").totalExecutorMaxAllocatedMemoryGb shouldBe 40 // 5 × 8g
    // minExecutors untouched.
    applied("_recipe.json").sparkOptsMap("spark.dynamicAllocation.minExecutors") shouldBe "2"
    applied("_recipe.json").extraFields("appliedExecutorScaleFactor") shouldBe "1.5"
  }

  test("ReBoost: tagged recipe + fresh signal → cumulative factor compounds") {
    val v = new ExecutorScaleVitamin(boostFactor = 1.5)
    // Already scaled once: max went from baseline to 5, factor 1.5.
    val recipes = Map("_recipe.json" -> daRecipe(min = 2, max = 5, scaleFactor = Some(1.5)))
    val signal = ExecutorScaleSignal("c1", "_recipe.json", "", "p95_job_duration_ms", 5.0, 5, 4.7)

    val boosts = v.computeBoosts(Seq(signal), recipes, Seq(signal))
    boosts should have size 1
    val b = boosts.head.asInstanceOf[ExecutorScaleBoost]
    b.state shouldBe BoostState.ReBoost
    b.originalMaxExecutors shouldBe 5
    b.boostedMaxExecutors shouldBe 8 // ceil(5 * 1.5) = 8
    b.effectiveCumulativeFactor shouldBe 2.25 // 1.5 * 1.5

    val applied = v.applyBoosts(boosts, recipes)
    applied("_recipe.json").sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "8"
    applied("_recipe.json").extraFields("appliedExecutorScaleFactor") shouldBe "2.25"
  }

  test("Holding: tagged recipe + NO fresh signal → preserve maxExecutors and factor") {
    val v = new ExecutorScaleVitamin(boostFactor = 1.5)
    val recipes = Map("_recipe.json" -> daRecipe(min = 2, max = 5, scaleFactor = Some(1.5)))

    val boosts = v.computeBoosts(Seq.empty, recipes, Seq.empty)
    boosts should have size 1
    val b = boosts.head.asInstanceOf[ExecutorScaleBoost]
    b.state shouldBe BoostState.Holding
    b.originalMaxExecutors shouldBe 5
    b.boostedMaxExecutors shouldBe 5
    b.effectiveCumulativeFactor shouldBe 1.5

    val applied = v.applyBoosts(boosts, recipes)
    // Memory/executor counts unchanged in Holding state — only extraFields update.
    applied("_recipe.json").sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "5"
    applied("_recipe.json").totalExecutorMaxAllocatedMemoryGb shouldBe (5 * 8)
    applied("_recipe.json").extraFields("appliedExecutorScaleFactor") shouldBe "1.5"
  }

  test("untagged + no fresh signal → no boost (case None,false)") {
    val v = new ExecutorScaleVitamin(boostFactor = 1.5)
    val recipes = Map("_recipe.json" -> daRecipe(min = 2, max = 3))
    val boosts = v.computeBoosts(Seq.empty, recipes, Seq.empty)
    boosts shouldBe empty
  }

  test("manual recipe (instances-only) is skipped") {
    val v = new ExecutorScaleVitamin(boostFactor = 1.5)
    val recipes = Map("_recipe.json" -> manualRecipe(instances = 3))
    val signal = ExecutorScaleSignal("c1", "_recipe.json", "", "p95_job_duration_ms", 5.0, 3, 2.9)
    val boosts = v.computeBoosts(Seq(signal), recipes, Seq(signal))
    boosts shouldBe empty
  }
}
