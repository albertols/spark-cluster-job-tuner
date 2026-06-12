package com.db.serna.orchestration.cluster_tuning.single.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CapacityGuardVitaminSpec extends AnyFunSuite with Matchers {

  private def daRecipe(min: Int, max: Int, ec: Int, em: Int): RecipeConfig =
    RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.dynamicAllocation.minExecutors" -> min.toString,
        "spark.dynamicAllocation.maxExecutors" -> max.toString,
        "spark.dynamicAllocation.initialExecutors" -> min.toString,
        "spark.executor.cores" -> ec.toString,
        "spark.executor.memory" -> s"${em}g"
      ),
      totalExecutorMinAllocatedMemoryGb = min * em,
      totalExecutorMaxAllocatedMemoryGb = max * em,
      extraFields = Map.empty
    )

  test("computeBoosts clamps a memory-heavy recipe and applyBoosts rewrites max + stamps %") {
    // node 48c/192GB, 6 workers, ratio .9; 4c/18GB exec -> cap 54.
    val sig = CapacityGuardSignal("c1", "_r.json", nodeCores = 48, nodeMemGb = 192, maxWorkers = 6, ratio = 0.90)
    val vitamin = new CapacityGuardVitamin(_ => Seq(sig))
    val recipes = Map("_r.json" -> daRecipe(min = 2, max = 100, ec = 4, em = 18))

    val applied = vitamin.applyBoosts(vitamin.computeBoosts(Seq(sig), recipes), recipes)("_r.json")
    applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "54"
    applied.extraFields("maxCoreUsagePct").toDouble should be > 0.0
    applied.extraFields("maxMemoryUsagePct").toDouble should be > 0.0
    applied.extraFields("capacityStatus") shouldBe "clamped"
    applied.totalExecutorMaxAllocatedMemoryGb shouldBe 54 * 18
  }

  test("within-capacity recipe is unchanged except for stamped % and no capacityStatus") {
    val sig = CapacityGuardSignal("c1", "_r.json", 48, 192, 6, 0.90)
    val vitamin = new CapacityGuardVitamin(_ => Seq(sig))
    val recipes = Map("_r.json" -> daRecipe(min = 2, max = 10, ec = 4, em = 8))
    val applied = vitamin.applyBoosts(vitamin.computeBoosts(Seq(sig), recipes), recipes)("_r.json")
    applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "10"
    applied.extraFields should contain key "maxCoreUsagePct"
    applied.extraFields should not contain key("capacityStatus")
  }
}
