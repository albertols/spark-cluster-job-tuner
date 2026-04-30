package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.RecipeMetrics
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TrendDetectorSpec extends AnyFunSuite with Matchers {

  private def mkMetrics(avgExec: Double = 2.0, p95Exec: Double = 4.0,
                        avgDur: Double = 50000.0, p95Dur: Double = 100000.0,
                        runs: Long = 20, fracCap: Option[Double] = None): RecipeMetrics =
    RecipeMetrics("cluster-a", "recipe-a.json", avgExec, p95Exec, avgDur, p95Dur, runs,
      None, None, None, fracCap, None)

  private def mkPair(ref: RecipeMetrics, cur: RecipeMetrics): MetricsPair =
    MetricsPair("cluster-a", "recipe-a.json", ref, cur)

  // ── Deltas ────────────────────────────────────────────────────────────────

  test("computeDeltas produces correct percentage changes") {
    val ref = mkMetrics(p95Dur = 100000.0)
    val cur = mkMetrics(p95Dur = 125000.0)
    val deltas = TrendDetector.computeDeltas(ref, cur)
    val durDelta = deltas.find(_.metricName == "p95_job_duration_ms").get
    durDelta.percentageChange shouldBe (25.0 +- 0.01)
    durDelta.absoluteChange shouldBe 25000.0
  }

  test("computeDeltas handles zero reference value") {
    val ref = mkMetrics(p95Dur = 0.0)
    val cur = mkMetrics(p95Dur = 50000.0)
    val deltas = TrendDetector.computeDeltas(ref, cur)
    val durDelta = deltas.find(_.metricName == "p95_job_duration_ms").get
    durDelta.percentageChange shouldBe 100.0
  }

  test("computeDeltas with both zero returns 0% change") {
    val ref = mkMetrics(p95Dur = 0.0)
    val cur = mkMetrics(p95Dur = 0.0)
    val deltas = TrendDetector.computeDeltas(ref, cur)
    val durDelta = deltas.find(_.metricName == "p95_job_duration_ms").get
    durDelta.percentageChange shouldBe 0.0
  }

  // ── Trend classification ──────────────────────────────────────────────────

  test("25% duration increase classified as Degraded") {
    val ref = mkMetrics(p95Dur = 100000.0)
    val cur = mkMetrics(p95Dur = 125000.0)
    val assessment = TrendDetector.assessTrend(mkPair(ref, cur))
    assessment.trend shouldBe Degraded
  }

  test("10% duration decrease classified as Improved") {
    val ref = mkMetrics(p95Dur = 100000.0)
    val cur = mkMetrics(p95Dur = 90000.0)
    val assessment = TrendDetector.assessTrend(mkPair(ref, cur))
    assessment.trend shouldBe Improved
  }

  test("3% duration increase classified as Stable") {
    val ref = mkMetrics(p95Dur = 100000.0)
    val cur = mkMetrics(p95Dur = 103000.0)
    val assessment = TrendDetector.assessTrend(mkPair(ref, cur))
    assessment.trend shouldBe Stable
  }

  test("3% duration decrease classified as Stable (below improvement threshold)") {
    val ref = mkMetrics(p95Dur = 100000.0)
    val cur = mkMetrics(p95Dur = 97000.0)
    val assessment = TrendDetector.assessTrend(mkPair(ref, cur))
    assessment.trend shouldBe Stable
  }

  test("cap-hit increase above threshold classified as Degraded even with stable duration") {
    val ref = mkMetrics(p95Dur = 100000.0, fracCap = Some(0.10))
    val cur = mkMetrics(p95Dur = 100000.0, fracCap = Some(0.30))
    val assessment = TrendDetector.assessTrend(mkPair(ref, cur))
    assessment.trend shouldBe Degraded
  }

  test("duration improvement with minor cap-hit increase still classified as Improved") {
    val ref = mkMetrics(p95Dur = 100000.0, fracCap = Some(0.10))
    val cur = mkMetrics(p95Dur = 90000.0, fracCap = Some(0.11))
    val assessment = TrendDetector.assessTrend(mkPair(ref, cur))
    assessment.trend shouldBe Improved
  }

  // ── Confidence ────────────────────────────────────────────────────────────

  test("confidence with 20 runs on both sides is 1.0") {
    TrendDetector.computeConfidence(mkMetrics(runs = 20), mkMetrics(runs = 30)) shouldBe 1.0
  }

  test("confidence with 5 runs is 0.5") {
    TrendDetector.computeConfidence(mkMetrics(runs = 5), mkMetrics(runs = 100)) shouldBe 0.5
  }

  test("confidence with 1 run is 0.1") {
    TrendDetector.computeConfidence(mkMetrics(runs = 1), mkMetrics(runs = 50)) shouldBe 0.1
  }

  test("confidence is included in TrendAssessment") {
    val ref = mkMetrics(runs = 3)
    val cur = mkMetrics(runs = 7)
    val assessment = TrendDetector.assessTrend(mkPair(ref, cur))
    assessment.confidenceLevel shouldBe 0.3
  }

  // ── Deltas are complete ───────────────────────────────────────────────────

  test("assessTrend includes deltas for all 6 metrics") {
    val ref = mkMetrics()
    val cur = mkMetrics()
    val assessment = TrendDetector.assessTrend(mkPair(ref, cur))
    assessment.deltas should have size 6
    assessment.deltas.map(_.metricName) should contain allOf(
      "avg_executors_per_job", "p95_run_max_executors",
      "avg_job_duration_ms", "p95_job_duration_ms",
      "fraction_reaching_cap", "runs"
    )
  }
}
