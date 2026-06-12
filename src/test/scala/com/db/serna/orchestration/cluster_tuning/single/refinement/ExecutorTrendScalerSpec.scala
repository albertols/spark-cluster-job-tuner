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

  // ── UP path ────────────────────────────────────────────────────────────────

  test("censoring case: capped + 10m->25m (Severe) raises max; min creeps at most +1") {
    val ref = m(p95Dur = 600000, avgDur = 540000, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 1500000, avgDur = 1350000, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.direction shouldBe ScaleDirection.Up
    d.severity shouldBe "severe"
    d.newMax should be > 3
    d.newMin shouldBe 3 // +1 creep (Severe + pressure 1.0 >= 0.8)
    d.newInitial should be <= 3 // initial follows, at most +1
    d.state shouldBe BoostState.New
  }

  test("the production case: 1m36s -> 14m57s over few runs is admitted and scales") {
    // ratio ~9.3, deltaMin ~13 -> Severe; 3 runs -> 2-4 band admits Severe (cap demoted to maxStep)
    val ref = m(p95Dur = 96000, avgDur = 90000, p95Max = 3, runs = 3)
    val cur = m(p95Dur = 897000, avgDur = 850000, p95Max = 3, runs = 3)
    val d = decide(ref, cur, min = 2, initial = 2, max = 3)
    d.direction shouldBe ScaleDirection.Up
    d.newMax should be > 3
  }

  test("big ratio but tiny absolute delta holds (20s -> 2m, even fully pinned)") {
    val ref = m(p95Dur = 20000, avgDur = 18000, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 120000, avgDur = 110000, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.direction shouldBe ScaleDirection.Hold
    d.severity shouldBe "negligible"
  }

  test("Critical degradation gets a larger step cap than Severe (balanced: x4 vs x3)") {
    // Critical: blend ratio ~6.03, deltaMin ~49. factor = 1 + 0.5·5.03·1.0 = 3.52 — under the x4
    // Critical cap (a Severe cap of x3 WOULD have clamped it) -> newMax = ceil(8·3.52) = 29.
    val refC = m(p95Dur = 600000, avgDur = 540000, p95Max = 8, runs = 20)
    val curC = m(p95Dur = 3600000, avgDur = 3300000, p95Max = 8, runs = 20)
    val dC = decide(refC, curC, min = 2, max = 8, capacity = Some(100))
    dC.severity shouldBe "critical"
    dC.newMax shouldBe 29
    dC.appliedFactor should be > balance.maxStep * ExecutorTrendScaler.SevereStepMultiplier
  }

  test("non-cap-touch slowdown HOLDS (not parallelism-bound)") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 1, runs = 20)
    val cur = m(p95Dur = 1500000, avgDur = 1500000, p95Max = 1, runs = 20)
    val d = decide(ref, cur, min = 2, max = 12)
    d.direction shouldBe ScaleDirection.Hold
  }

  test("fraction_reaching_cap alone satisfies the cap-pressure gate") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 1, runs = 20, fracCap = Some(0.9))
    val cur = m(p95Dur = 1200000, avgDur = 1200000, p95Max = 1, runs = 20, fracCap = Some(0.9))
    decide(ref, cur, min = 2, max = 8).direction shouldBe ScaleDirection.Up
  }

  test("Moderate tier under pressure 0.8 does not creep min") {
    // Moderate: ratio 1.5, deltaMin ~5; pressure: p95Max 2 of max 3 = 0.67 (>= capTouch 0.5, < 0.8)
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 2, runs = 20)
    val cur = m(p95Dur = 900000, avgDur = 900000, p95Max = 2, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.direction shouldBe ScaleDirection.Up
    d.severity shouldBe "moderate"
    d.newMin shouldBe 2 // unchanged
  }

  test("1 run admits only Critical, with conservative 1.5 step") {
    val refS = m(p95Dur = 600000, avgDur = 600000, p95Max = 3, runs = 1)
    val curS = m(p95Dur = 1500000, avgDur = 1500000, p95Max = 3, runs = 1) // Severe
    decide(refS, curS, max = 3).direction shouldBe ScaleDirection.Hold

    val curC = m(p95Dur = 3600000, avgDur = 3600000, p95Max = 3, runs = 1) // Critical
    val d = decide(refS, curC, min = 2, max = 4, capacity = Some(40))
    d.direction shouldBe ScaleDirection.Up
    d.newMax shouldBe 6 // ceil(4 * 1.5) capped by SingleRunStepCap
  }

  test("manual recipes scale instances toward newMax") {
    val ref = m(p95Dur = 600000, avgDur = 540000, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 1500000, avgDur = 1350000, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 3, initial = 3, max = 3, manual = true)
    d.direction shouldBe ScaleDirection.Up
    d.newMin shouldBe d.newMax
    d.newMax should be > 3
  }

  test("capacity clamp: never exceeds cluster capacity; UP never shrinks below current max") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 5, runs = 20)
    val cur = m(p95Dur = 3600000, avgDur = 3600000, p95Max = 5, runs = 20)
    decide(ref, cur, min = 2, max = 5, capacity = Some(6)).newMax shouldBe 6
    decide(ref, cur, min = 2, max = 5, capacity = Some(4)).newMax shouldBe 5
  }

  test("impact and severity are stamped on the decision") {
    val ref = m(p95Dur = 600000, avgDur = 540000, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 1500000, avgDur = 1350000, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.impactMinutes shouldBe ((0.7 * 1500000 + 0.3 * 1350000) - (0.7 * 600000 + 0.3 * 540000)) / 60000.0 * 20 +- 0.01
    d.priorityRank shouldBe None
  }

  test("performance bias still produces a larger or equal up-step than balanced") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 4, runs = 20)
    val cur = m(p95Dur = 1100000, avgDur = 1100000, p95Max = 4, runs = 20)
    val dBal = decide(ref, cur, max = 4, capacity = Some(40))
    val dPerf = decide(ref, cur, max = 4, capacity = Some(40), gains = ScaleGains.fromBias(PerformanceBiased))
    dPerf.newMax should be >= dBal.newMax
  }

  // ── DOWN path ──────────────────────────────────────────────────────────────

  test("improvement with headroom shrinks max; min shrinks at most 1") {
    val ref = m(p95Dur = 1200000, avgDur = 1200000, p95Max = 4, runs = 30)
    val cur = m(p95Dur = 720000, avgDur = 720000, p95Max = 4, runs = 30, avgExec = 3.0)
    val d = decide(ref, cur, min = 6, initial = 6, max = 16, capacity = Some(16))
    d.direction shouldBe ScaleDirection.Down
    d.newMax should be < 16
    d.newMax should be >= 4
    d.newMin should be >= 5 // 6 - 1 creep floor
    d.newInitial should be >= d.newMin
    d.newInitial should be <= d.newMax
  }

  test("small absolute saving does not downscale (3m floor symmetric)") {
    // 4m -> 2m: ratio 0.5 but only 2 minutes saved
    val ref = m(p95Dur = 240000, avgDur = 240000, p95Max = 4, runs = 30)
    val cur = m(p95Dur = 120000, avgDur = 120000, p95Max = 4, runs = 30)
    decide(ref, cur, min = 6, max = 16).direction shouldBe ScaleDirection.Hold
  }

  test("deadband: small duration noise holds both ways") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 3, runs = 20)
    decide(ref, m(p95Dur = 630000, avgDur = 630000, p95Max = 3, runs = 20), max = 3)
      .direction shouldBe ScaleDirection.Hold
    decide(ref, m(p95Dur = 576000, avgDur = 576000, p95Max = 3, runs = 20), min = 4, max = 8)
      .direction shouldBe ScaleDirection.Hold
  }

  test("prior factor carries on hold (Holding) and compounds on UP (ReBoost)") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 3, runs = 20)
    val curH = m(p95Dur = 620000, avgDur = 620000, p95Max = 3, runs = 20)
    val h = decide(ref, curH, max = 3, prior = Some(1.5))
    h.state shouldBe BoostState.Holding
    h.cumulativeFactor shouldBe 1.5

    val curU = m(p95Dur = 1500000, avgDur = 1350000, p95Max = 3, runs = 20)
    val u = decide(ref, curU, min = 2, max = 3, prior = Some(1.5))
    u.state shouldBe BoostState.ReBoost
    u.cumulativeFactor shouldBe (1.5 * u.appliedFactor) +- 1e-9
  }
}
