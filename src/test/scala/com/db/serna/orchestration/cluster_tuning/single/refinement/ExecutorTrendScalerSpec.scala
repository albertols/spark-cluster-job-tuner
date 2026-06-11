package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.{CostPerformanceBalance, PerformanceBiased, RecipeMetrics}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorTrendScalerSpec extends AnyFunSuite with Matchers {

  private val balance = ScaleGains.fromBias(CostPerformanceBalance)

  private def m(
      p95Dur: Double,
      avgDur: Double,
      p95Max: Double,
      runs: Long,
      fracCap: Option[Double] = None,
      avgExec: Double = 2.0
  ): RecipeMetrics =
    RecipeMetrics(
      cluster = "c",
      recipe = "r",
      avgExecutorsPerJob = avgExec,
      p95RunMaxExecutors = p95Max,
      avgJobDurationMs = avgDur,
      p95JobDurationMs = p95Dur,
      runs = runs,
      secondsAtCap = None,
      runsReachingCap = None,
      totalRuns = None,
      fractionReachingCap = fracCap,
      maxConcurrentJobs = None
    )

  private def decide(
      ref: RecipeMetrics,
      cur: RecipeMetrics,
      min: Int = 2,
      initial: Int = 2,
      max: Int = 3,
      gains: ScaleGains = balance,
      capacity: Option[Int] = Some(12),
      prior: Option[Double] = None,
      manual: Boolean = false
  ): TrendScaleDecision =
    ExecutorTrendScaler.decide("r", manual, min, initial, max, ref, cur, gains, capacity, prior)

  test("censoring case: capped + duration ~2x raises BOTH min and max") {
    val ref = m(p95Dur = 100, avgDur = 90, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 200, avgDur = 162, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.direction shouldBe ScaleDirection.Up
    d.newMax should be > 3
    d.newMin should be > 2
    d.newMin should be <= d.newMax - 1
    d.state shouldBe BoostState.New
  }

  test("non-cap-touch slowdown HOLDS (not parallelism-bound)") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 1, runs = 20)
    val cur = m(p95Dur = 200, avgDur = 200, p95Max = 1, runs = 20)
    val d = decide(ref, cur, min = 2, max = 12)
    d.direction shouldBe ScaleDirection.Hold
    d.newMax shouldBe 12
    d.newMin shouldBe 2
  }

  test("fraction_reaching_cap alone satisfies the cap-pressure gate") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 1, runs = 20, fracCap = Some(0.9))
    val cur = m(p95Dur = 160, avgDur = 160, p95Max = 1, runs = 20, fracCap = Some(0.9))
    val d = decide(ref, cur, min = 2, max = 8)
    d.direction shouldBe ScaleDirection.Up
  }

  test("improvement with headroom shrinks conservatively, never below demand or floor 2") {
    val ref = m(p95Dur = 200, avgDur = 200, p95Max = 4, runs = 30)
    val cur = m(p95Dur = 120, avgDur = 120, p95Max = 4, runs = 30, avgExec = 3.0)
    val d = decide(ref, cur, min = 6, initial = 6, max = 16, capacity = Some(16))
    d.direction shouldBe ScaleDirection.Down
    d.newMax should be < 16
    d.newMax should be >= 4
    d.newMin should be >= 2
    d.newMin should be <= d.newMax - 1
  }

  test("deadband: small duration noise (+/-5%) holds") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 3, runs = 20)
    val curUp = m(p95Dur = 105, avgDur = 105, p95Max = 3, runs = 20)
    val curDn = m(p95Dur = 96, avgDur = 96, p95Max = 3, runs = 20)
    decide(ref, curUp, max = 3).direction shouldBe ScaleDirection.Hold
    decide(ref, curDn, min = 4, max = 8).direction shouldBe ScaleDirection.Hold
  }

  test("low confidence (few runs) yields no usable ratio -> Hold") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 3, runs = 2)
    val cur = m(p95Dur = 300, avgDur = 300, p95Max = 3, runs = 2)
    decide(ref, cur, max = 3).direction shouldBe ScaleDirection.Hold
  }

  test("performance bias produces a larger up-factor than balanced for identical input") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 4, runs = 20)
    val cur = m(p95Dur = 180, avgDur = 180, p95Max = 4, runs = 20)
    val dBal = decide(ref, cur, max = 4, capacity = Some(40))
    val dPerf = decide(ref, cur, max = 4, capacity = Some(40), gains = ScaleGains.fromBias(PerformanceBiased))
    dPerf.newMax should be > dBal.newMax
  }

  test("capacity clamp: never exceeds cluster capacity") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 5, runs = 20)
    val cur = m(p95Dur = 500, avgDur = 500, p95Max = 5, runs = 20)
    val d = decide(ref, cur, min = 2, max = 5, capacity = Some(6))
    d.newMax shouldBe 6
    d.newMin should be <= 5
  }

  test("per-run maxStep clamps a huge spike") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 5, runs = 20)
    val cur = m(p95Dur = 500, avgDur = 500, p95Max = 5, runs = 20)
    val d = decide(ref, cur, min = 2, max = 5, capacity = Some(100))
    d.newMax should be <= 10
  }

  test("manual recipe scales spark.executor.instances proportionally, floor 2") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 4, runs = 20)
    val cur = m(p95Dur = 180, avgDur = 180, p95Max = 4, runs = 20)
    val d = decide(ref, cur, min = 4, initial = 4, max = 4, capacity = Some(20), manual = true)
    d.isManual shouldBe true
    d.newMax should be > 4
    d.newMin shouldBe d.newMax
  }

  test("ReBoost: prior tag + fresh up-signal compounds cumulative factor") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 4, runs = 20)
    val cur = m(p95Dur = 200, avgDur = 200, p95Max = 4, runs = 20)
    val d = decide(ref, cur, min = 3, max = 4, capacity = Some(40), prior = Some(1.5))
    d.state shouldBe BoostState.ReBoost
    d.cumulativeFactor should be > 1.5
  }

  test("Holding: prior tag but inside deadband leaves config unchanged") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 102, avgDur = 102, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 3, max = 6, prior = Some(1.5))
    d.direction shouldBe ScaleDirection.Hold
    d.state shouldBe BoostState.Holding
    d.newMax shouldBe 6
    d.cumulativeFactor shouldBe 1.5
  }

  test("idempotence: re-deciding on already-scaled config with no fresh delta holds") {
    val ref = m(p95Dur = 200, avgDur = 200, p95Max = 6, runs = 20)
    val cur = m(p95Dur = 200, avgDur = 200, p95Max = 6, runs = 20)
    val d = decide(ref, cur, min = 4, max = 8, prior = Some(2.0))
    d.direction shouldBe ScaleDirection.Hold
    d.newMax shouldBe 8
  }
}
