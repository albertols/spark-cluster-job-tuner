package com.db.serna.orchestration.cluster_tuning.single.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorTrendPrioritizeSpec extends AnyFunSuite with Matchers {

  private def up(recipe: String, origMax: Int, newMax: Int, impact: Double, manual: Boolean = false): TrendScaleDecision =
    TrendScaleDecision(
      recipe, manual, 2, 2, origMax,
      if (manual) newMax else 2, if (manual) newMax else 2, newMax,
      newMax.toDouble / origMax, newMax.toDouble / origMax,
      ScaleDirection.Up, BoostState.New, "up", "severe", impact, None
    )

  private def hold(recipe: String): TrendScaleDecision =
    TrendScaleDecision(recipe, isManual = false, 2, 2, 4, 2, 2, 4, 1.0, 1.0,
      ScaleDirection.Hold, BoostState.New, "hold", "negligible", 0.0, None)

  private def rc(d: TrendScaleDecision, cores: Int = 8): ExecutorTrendScaler.RecipeCores =
    ExecutorTrendScaler.RecipeCores(d, cores)

  test("plentiful pool: all grants pass through with priority ranks by impact desc") {
    val a = up("a.json", 4, 8, impact = 600.0)
    val b = up("b.json", 4, 6, impact = 50.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(b), rc(a)), clusterMaxTotalCores = 1000, poolRatio = 1.0)
    out.map(_.recipe) shouldBe Seq("b.json", "a.json") // input order preserved
    out.find(_.recipe == "a.json").get.priorityRank shouldBe Some(1)
    out.find(_.recipe == "b.json").get.priorityRank shouldBe Some(2)
    out.find(_.recipe == "a.json").get.newMax shouldBe 8
  }

  test("pool exhaustion: highest impact gets full grant, the rest degrade to +1 (never zero)") {
    // pool = 64 cores; a wants (12-4)*8=64 -> takes all; b degrades to originalMax+1
    val a = up("a.json", 4, 12, impact = 600.0)
    val b = up("b.json", 4, 10, impact = 50.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a), rc(b)), clusterMaxTotalCores = 64, poolRatio = 1.0)
    out.find(_.recipe == "a.json").get.newMax shouldBe 12
    val db = out.find(_.recipe == "b.json").get
    db.newMax shouldBe 5
    db.appliedFactor shouldBe 1.25 +- 1e-9
    db.cumulativeFactor shouldBe 1.25 +- 1e-9
    db.reason should include("pool-exhausted")
  }

  test("degraded manual grant keeps min==initial==max") {
    val a = up("a.json", 4, 12, impact = 600.0)
    val mB = up("b.json", 4, 10, impact = 50.0, manual = true)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a), rc(mB)), clusterMaxTotalCores = 64, poolRatio = 1.0)
    val db = out.find(_.recipe == "b.json").get
    db.newMax shouldBe 5
    db.newMin shouldBe 5
    db.newInitial shouldBe 5
  }

  test("holds and downs pass through untouched, no rank") {
    val h = hold("h.json")
    val out = ExecutorTrendScaler.prioritize(Seq(rc(h)), clusterMaxTotalCores = 64, poolRatio = 1.0)
    out.head shouldBe h
  }

  test("no capacity info (clusterMaxTotalCores <= 0) passes everything through") {
    val a = up("a.json", 4, 12, impact = 600.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a)), clusterMaxTotalCores = 0, poolRatio = 1.0)
    out.head.newMax shouldBe 12
    out.head.priorityRank shouldBe None
  }

  test("deterministic tie-break on equal impact: recipe name") {
    val a = up("zz.json", 4, 8, impact = 100.0)
    val b = up("aa.json", 4, 8, impact = 100.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a), rc(b)), clusterMaxTotalCores = 1000, poolRatio = 1.0)
    out.find(_.recipe == "aa.json").get.priorityRank shouldBe Some(1)
    out.find(_.recipe == "zz.json").get.priorityRank shouldBe Some(2)
  }

  test("cumulative factor of a degraded grant preserves the prior compound") {
    val prior = 1.5
    val d = up("b.json", 4, 10, impact = 50.0).copy(appliedFactor = 2.5, cumulativeFactor = prior * 2.5)
    val a = up("a.json", 4, 12, impact = 600.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a), rc(d)), clusterMaxTotalCores = 64, poolRatio = 1.0)
    val db = out.find(_.recipe == "b.json").get
    db.cumulativeFactor shouldBe (prior * 1.25) +- 1e-9
  }
}
