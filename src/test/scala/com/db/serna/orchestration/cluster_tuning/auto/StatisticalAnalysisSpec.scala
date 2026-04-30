package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.RecipeMetrics
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StatisticalAnalysisSpec extends AnyFunSuite with Matchers {

  import StatisticalAnalysis._

  private val Epsilon = 1e-9

  // ── Basic statistics ──────────────────────────────────────────────────────

  test("mean of [2, 4, 6] is 4.0") {
    mean(Seq(2.0, 4.0, 6.0)) shouldBe 4.0
  }

  test("mean of empty sequence is 0.0") {
    mean(Seq.empty) shouldBe 0.0
  }

  test("variance of [2, 4, 6] is 2.667 (population)") {
    variance(Seq(2.0, 4.0, 6.0)) shouldBe (8.0 / 3.0 +- Epsilon)
  }

  test("variance of single element is 0.0") {
    variance(Seq(5.0)) shouldBe 0.0
  }

  test("stddev of [2, 4, 6]") {
    stddev(Seq(2.0, 4.0, 6.0)) shouldBe (math.sqrt(8.0 / 3.0) +- Epsilon)
  }

  // ── Covariance ────────────────────────────────────────────────────────────

  test("covariance of perfectly correlated sequences") {
    val xs = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
    val ys = Seq(2.0, 4.0, 6.0, 8.0, 10.0)
    covariance(xs, ys) shouldBe (4.0 +- Epsilon)
  }

  test("covariance of single-element sequences is 0.0") {
    covariance(Seq(1.0), Seq(2.0)) shouldBe 0.0
  }

  // ── Pearson correlation ───────────────────────────────────────────────────

  test("pearson of perfectly correlated = 1.0") {
    val xs = Seq(1.0, 2.0, 3.0, 4.0)
    val ys = Seq(10.0, 20.0, 30.0, 40.0)
    pearsonCorrelation(xs, ys) shouldBe (1.0 +- Epsilon)
  }

  test("pearson of perfectly inversely correlated = -1.0") {
    val xs = Seq(1.0, 2.0, 3.0, 4.0)
    val ys = Seq(40.0, 30.0, 20.0, 10.0)
    pearsonCorrelation(xs, ys) shouldBe (-1.0 +- Epsilon)
  }

  test("pearson with zero stddev returns 0.0") {
    val xs = Seq(5.0, 5.0, 5.0)
    val ys = Seq(1.0, 2.0, 3.0)
    pearsonCorrelation(xs, ys) shouldBe 0.0
  }

  test("pearson is clamped to [-1, 1]") {
    val xs = Seq(1.0, 2.0, 3.0)
    val ys = Seq(1.0, 2.0, 3.0)
    val r = pearsonCorrelation(xs, ys)
    r should be >= -1.0
    r should be <= 1.0
  }

  // ── Z-score ───────────────────────────────────────────────────────────────

  test("zScore computation") {
    zScore(15.0, 10.0, 2.5) shouldBe (2.0 +- Epsilon)
  }

  test("zScore with zero stddev returns 0.0") {
    zScore(15.0, 10.0, 0.0) shouldBe 0.0
  }

  // ── Fleet-wide correlation ────────────────────────────────────────────────

  private def mkMetrics(cluster: String, recipe: String,
                        avgExec: Double, p95Exec: Double,
                        avgDur: Double, p95Dur: Double,
                        runs: Long, fracCap: Option[Double] = None): RecipeMetrics =
    RecipeMetrics(cluster, recipe, avgExec, p95Exec, avgDur, p95Dur, runs,
      None, None, None, fracCap, None)

  private def mkPair(cluster: String, recipe: String,
                     refAvgExec: Double, refP95Dur: Double,
                     curAvgExec: Double, curP95Dur: Double): MetricsPair =
    MetricsPair(cluster, recipe,
      mkMetrics(cluster, recipe, refAvgExec, refAvgExec, refP95Dur, refP95Dur, 10),
      mkMetrics(cluster, recipe, curAvgExec, curAvgExec, curP95Dur, curP95Dur, 10))

  test("computeCorrelations returns expected pairs") {
    val pairs = Seq(
      mkPair("c1", "r1", 2.0, 100.0, 4.0, 200.0),
      mkPair("c2", "r2", 3.0, 150.0, 5.0, 250.0),
      mkPair("c3", "r3", 1.0, 50.0, 3.0, 150.0)
    )
    val results = computeCorrelations(pairs)
    results should not be empty
    results.foreach(_.sampleSize shouldBe 3)
    results.map(_.metricA) should contain("delta_p95_run_max_executors")
  }

  test("computeCorrelations with fewer than 2 pairs returns empty") {
    val pairs = Seq(mkPair("c1", "r1", 2.0, 100.0, 4.0, 200.0))
    computeCorrelations(pairs) shouldBe empty
  }

  // ── Divergence detection ──────────────────────────────────────────────────

  test("detectDivergences flags outlier recipe") {
    // Need ≥10 normal entries so that the outlier's z-score exceeds 2.0.
    // With n=4, max z-score is sqrt(3)≈1.73 which is below 2.0 threshold.
    val normals = (1 to 10).map { i =>
      mkPair(s"c$i", s"r$i", 2.0 + i * 0.1, 100.0 + i * 5, 2.1 + i * 0.1, 105.0 + i * 5)
    }
    val outlier = mkPair("cx", "rx", 2.0, 100.0, 20.0, 1000.0)

    val divergences = detectDivergences(normals :+ outlier, zThreshold = 2.0)
    divergences should not be empty
    divergences.exists(d => d.cluster == "cx" && d.recipe == "rx" && d.isOutlier) shouldBe true
  }

  test("detectDivergences with fewer than 2 pairs returns empty") {
    val pairs = Seq(mkPair("c1", "r1", 2.0, 100.0, 4.0, 200.0))
    detectDivergences(pairs) shouldBe empty
  }

  test("detectDivergences with identical deltas returns empty (zero stddev)") {
    val p1 = mkPair("c1", "r1", 2.0, 100.0, 4.0, 200.0)
    val p2 = mkPair("c2", "r2", 2.0, 100.0, 4.0, 200.0)
    detectDivergences(Seq(p1, p2)) shouldBe empty
  }
}
