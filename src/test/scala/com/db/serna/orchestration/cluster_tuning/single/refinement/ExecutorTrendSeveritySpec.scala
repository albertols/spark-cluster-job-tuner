package com.db.serna.orchestration.cluster_tuning.single.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorTrendSeveritySpec extends AnyFunSuite with Matchers {

  private val deadband = 0.10
  private val minDelta = 3.0

  private def cls(ratio: Double, deltaMin: Double): ScaleSeverity =
    ExecutorTrendScaler.classifySeverity(ratio, deltaMin, deadband, minDelta)

  test("below deadband is Negligible regardless of absolute delta") {
    cls(1.05, 120.0) shouldBe ScaleSeverity.Negligible
  }

  test("big ratio but tiny absolute delta is Negligible (20s -> 2m case)") {
    cls(6.0, 1.6) shouldBe ScaleSeverity.Negligible
  }

  test("ratio above deadband with >=3 min lost is Moderate") {
    cls(1.30, 4.0) shouldBe ScaleSeverity.Moderate
  }

  test("ratio >=2 with >=10 min lost is Severe") {
    cls(2.2, 12.0) shouldBe ScaleSeverity.Severe
  }

  test("ratio >=2 but under 10 min lost stays Moderate") {
    cls(2.5, 4.9) shouldBe ScaleSeverity.Moderate
  }

  test("ratio >=3 with >=30 min lost is Critical (the 1m36s -> 14m57s class scales)") {
    cls(9.3, 13.0) shouldBe ScaleSeverity.Severe // 13 min lost: Severe, not Critical (needs >=30)
    cls(9.3, 48.0) shouldBe ScaleSeverity.Critical
  }

  test("ratio >=3 but under 30 min lost demotes to Severe (if >=10) or Moderate") {
    cls(4.0, 15.0) shouldBe ScaleSeverity.Severe
    cls(4.0, 5.0) shouldBe ScaleSeverity.Moderate
  }

  test("graduated evidence: full runs admit any tier at full cap") {
    val g = ScaleGains.fromBias(com.db.serna.orchestration.cluster_tuning.single.CostPerformanceBalance)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Moderate, 5, g) shouldBe Some(g.maxStep)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Severe, 10, g) shouldBe Some(g.maxStep * 1.5)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Critical, 20, g) shouldBe Some(g.maxStep * 2.0)
  }

  test("graduated evidence: 2-4 runs admit only Severe+ and demote the cap one tier") {
    val g = ScaleGains.fromBias(com.db.serna.orchestration.cluster_tuning.single.CostPerformanceBalance)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Moderate, 3, g) shouldBe None
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Severe, 3, g) shouldBe Some(g.maxStep)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Critical, 4, g) shouldBe Some(g.maxStep * 1.5)
  }

  test("graduated evidence: 1 run admits only Critical, capped at 1.5") {
    val g = ScaleGains.fromBias(com.db.serna.orchestration.cluster_tuning.single.CostPerformanceBalance)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Severe, 1, g) shouldBe None
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Critical, 1, g) shouldBe Some(1.5)
  }

  test("graduated evidence: 0 runs admit nothing; Negligible admits nothing") {
    val g = ScaleGains.fromBias(com.db.serna.orchestration.cluster_tuning.single.CostPerformanceBalance)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Critical, 0, g) shouldBe None
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Negligible, 20, g) shouldBe None
  }
}
