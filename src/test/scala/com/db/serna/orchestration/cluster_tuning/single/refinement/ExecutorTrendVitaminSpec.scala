package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.{CostPerformanceBalance, RecipeMetrics}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorTrendVitaminSpec extends AnyFunSuite with Matchers {

  private def metrics(p95Dur: Double, p95Max: Double, runs: Long): RecipeMetrics =
    RecipeMetrics("c1", "_r.json", 2.0, p95Max, p95Dur * 0.9, p95Dur, runs, None, None, None, None, None)

  private def metricsFull(
      p95Dur: Double,
      avgDur: Double,
      p95Max: Double,
      runs: Long,
      fracCap: Option[Double]
  ): RecipeMetrics =
    RecipeMetrics("c", "_r.json", 2.0, p95Max, avgDur, p95Dur, runs, None, None, None, fracCap, None)

  private def daRecipe(min: Int, max: Int): RecipeConfig =
    RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.dynamicAllocation.minExecutors" -> min.toString,
        "spark.dynamicAllocation.maxExecutors" -> max.toString,
        "spark.dynamicAllocation.initialExecutors" -> min.toString,
        "spark.executor.cores" -> "8",
        "spark.executor.memory" -> "8g"
      ),
      totalExecutorMinAllocatedMemoryGb = min * 8,
      totalExecutorMaxAllocatedMemoryGb = max * 8,
      extraFields = Map.empty
    )

  test("computeBoosts emits an up TrendScaleBoost for a censored degraded recipe and applyBoosts rewrites min/max") {
    // minute-scale: v2 severity needs >=3 min absolute delta (here 10m -> 25m blended, Severe)
    val ref = metrics(p95Dur = 600000, p95Max = 3, runs = 20)
    val cur = metrics(p95Dur = 1500000, p95Max = 3, runs = 20)
    val signal = TrendScaleSignal("c1", "_r.json", ref, cur, clusterMaxTotalCores = 96)
    val gains = ScaleGains.fromBias(CostPerformanceBalance)
    val vitamin = new ExecutorTrendVitamin(gains, _ => Seq(signal))

    val recipes = Map("_r.json" -> daRecipe(min = 2, max = 3))
    val boosts = vitamin.computeBoosts(Seq(signal), recipes)
    boosts should have size 1

    val applied = vitamin.applyBoosts(boosts, recipes)("_r.json")
    applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors").toInt should be > 3
    applied.sparkOptsMap("spark.dynamicAllocation.minExecutors").toInt should be >= 2 // v2 (Task 2) reworks min-creep
    applied.totalExecutorMaxAllocatedMemoryGb shouldBe applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors").toInt * 8
    applied.extraFields should contain key "appliedTrendScaleFactor"
  }

  test("Hold decision produces no executor change but still stamps the carried factor when prior exists") {
    val ref = metrics(p95Dur = 600000, p95Max = 3, runs = 20) // minute-scale: v2 severity needs >=3 min absolute delta
    val cur = metrics(p95Dur = 606000, p95Max = 3, runs = 20) // inside deadband
    val signal = TrendScaleSignal("c1", "_r.json", ref, cur, clusterMaxTotalCores = 96)
    val vitamin = new ExecutorTrendVitamin(ScaleGains.fromBias(CostPerformanceBalance), _ => Seq(signal))
    val recipes = Map("_r.json" -> daRecipe(2, 6).copy(extraFields = Map("appliedTrendScaleFactor" -> "1.5")))

    val applied = vitamin.applyBoosts(vitamin.computeBoosts(Seq(signal), recipes), recipes)("_r.json")
    applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "6"
    applied.extraFields("appliedTrendScaleFactor") shouldBe "1.5"
  }

  test("manual recipe up-change writes spark.executor.instances and leaves dynamicAllocation keys absent") {
    // minute-scale: v2 severity needs >=3 min absolute delta
    val ref = metrics(p95Dur = 600000, p95Max = 4, runs = 20)
    val cur = metrics(p95Dur = 1500000, p95Max = 4, runs = 20)
    val signal = TrendScaleSignal("c1", "_r.json", ref, cur, clusterMaxTotalCores = 160)
    val vitamin = new ExecutorTrendVitamin(ScaleGains.fromBias(CostPerformanceBalance), _ => Seq(signal))
    val manual = RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.executor.instances" -> "4",
        "spark.executor.cores" -> "8",
        "spark.executor.memory" -> "8g"
      ),
      totalExecutorMinAllocatedMemoryGb = 32,
      totalExecutorMaxAllocatedMemoryGb = 32,
      extraFields = Map.empty
    )
    val applied = vitamin.applyBoosts(vitamin.computeBoosts(Seq(signal), Map("_r.json" -> manual)), Map("_r.json" -> manual))("_r.json")
    applied.sparkOptsMap("spark.executor.instances").toInt should be > 4
    applied.sparkOptsMap.keys should not contain "spark.dynamicAllocation.maxExecutors"
    applied.totalExecutorMaxAllocatedMemoryGb shouldBe applied.sparkOptsMap("spark.executor.instances").toInt * 8
  }

  test("computeBoosts prioritizes UP grants across the cluster: pool-exhausted recipe degrades to +1") {
    // Two dynamic recipes, 8 cores each, cluster has 64 schedulable cores. The per-recipe capacity
    // clamp (64/8 = 8 executors) bounds BIG's grant to +4 = 32 cores, so with the default pool of
    // 64 cores nothing would ever exhaust — use upPoolRatio = 0.5 (pool = 32 cores): BIG (higher
    // impact, Severe) drains it entirely; SMALL (Moderate, wants ceil(4·1.4)=6) must still move +1.
    val gains = ScaleGains.fromBias(CostPerformanceBalance, upPoolRatioOverride = Some(0.5))
    val refBig = metricsFull(p95Dur = 600000, avgDur = 540000, p95Max = 4, runs = 20, fracCap = Some(0.9))
    val curBig = metricsFull(p95Dur = 2400000, avgDur = 2200000, p95Max = 4, runs = 20, fracCap = Some(0.9))
    val refSmall = metricsFull(p95Dur = 600000, avgDur = 600000, p95Max = 4, runs = 20, fracCap = Some(0.9))
    val curSmall = metricsFull(p95Dur = 1080000, avgDur = 1080000, p95Max = 4, runs = 20, fracCap = Some(0.9))
    val signals = Seq(
      TrendScaleSignal("c", "_BIG.json", refBig, curBig, clusterMaxTotalCores = 64),
      TrendScaleSignal("c", "_SMALL.json", refSmall, curSmall, clusterMaxTotalCores = 64)
    )
    val recipes = Map(
      "_BIG.json" -> daRecipe(min = 2, max = 4),
      "_SMALL.json" -> daRecipe(min = 2, max = 4)
    )
    val vitamin = new ExecutorTrendVitamin(gains, _ => signals)
    val boosts = vitamin.computeBoosts(signals, recipes).collect { case b: TrendScaleBoost => b }
    val big = boosts.find(_.recipeFilename == "_BIG.json").get.decision
    val small = boosts.find(_.recipeFilename == "_SMALL.json").get.decision
    big.priorityRank shouldBe Some(1)
    big.newMax shouldBe 8 // capacity-clamped full grant: +4 executors × 8 cores = the whole 32-core pool
    small.newMax shouldBe 5 // degraded to +1, not zero
    small.reason should include("pool-exhausted")
  }
}
