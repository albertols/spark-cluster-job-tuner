package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.{CostBiased, CostPerformanceBalance, PerformanceBiased}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ScaleGainsSpec extends AnyFunSuite with Matchers {

  test("bias presets order the up-gain Cost < Balance < Performance") {
    val cost = ScaleGains.fromBias(CostBiased)
    val bal = ScaleGains.fromBias(CostPerformanceBalance)
    val perf = ScaleGains.fromBias(PerformanceBiased)
    cost.gain should be < bal.gain
    bal.gain should be < perf.gain
    cost.maxStep should be < perf.maxStep
    cost.deadbandDown should be < perf.deadbandDown
    cost.downGain should be > perf.downGain
  }

  test("shared defaults are present and sane") {
    val g = ScaleGains.fromBias(CostPerformanceBalance)
    g.deadbandUp shouldBe 0.10
    g.capTouchRatio shouldBe 0.5
    g.minRunsForConfidence shouldBe 5L
    g.minStep should (be > 0.0 and be <= 1.0)
    g.downSafetyMargin shouldBe 0.15
    g.downConfidenceFloor shouldBe 0.5
    g.downscaleEnabled shouldBe true
  }

  test("defaults carry minDeltaMinutes=3.0 and upPoolRatio=1.0") {
    val g = ScaleGains.fromBias(CostPerformanceBalance)
    g.minDeltaMinutes shouldBe 3.0
    g.upPoolRatio shouldBe 1.0
  }

  test("CLI overrides take precedence over bias presets") {
    val g = ScaleGains.fromBias(
      CostPerformanceBalance,
      gainOverride = Some(0.9),
      deadbandUpOverride = Some(0.2),
      maxStepOverride = Some(3.0),
      minRunsOverride = Some(7L),
      downscaleEnabledOverride = Some(false)
    )
    g.gain shouldBe 0.9
    g.deadbandUp shouldBe 0.2
    g.maxStep shouldBe 3.0
    g.minRunsForConfidence shouldBe 7L
    g.downscaleEnabled shouldBe false
    // downGain / deadbandDown are deliberately NOT on the override surface — they stay at the bias preset.
    g.downGain shouldBe 0.4
    g.deadbandDown shouldBe 0.10
  }
}
