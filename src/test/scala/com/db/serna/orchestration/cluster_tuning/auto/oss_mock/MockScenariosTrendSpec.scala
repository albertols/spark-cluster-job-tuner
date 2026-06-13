package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MockScenariosTrendSpec extends AnyFunSuite with Matchers {

  private val refDate = "2099_03_01"
  private val curDate = "2099_03_02"

  test("durationDrift is registered as a multi-date scenario") {
    MockScenarios.multiDate.keySet should contain("durationDrift")
    MockScenarios.multiDateNames should contain("durationDrift")
  }

  test("durationDrift produces a capped recipe whose current p95 duration exceeds reference at the cap") {
    val md = MockScenarios.multiDate("durationDrift")(refDate, curDate, 1234L)
    val ref = md.perDate(refDate)
    val cur = md.perDate(curDate)

    val refRecipe = ref.clusters
      .flatMap(_.recipes)
      .find(_.name == "_DRIFT_DEMO.json")
      .getOrElse(
        fail("reference snapshot is missing _DRIFT_DEMO.json")
      )
    val curRecipe = cur.clusters
      .flatMap(_.recipes)
      .find(_.name == "_DRIFT_DEMO.json")
      .getOrElse(
        fail("current snapshot is missing _DRIFT_DEMO.json")
      )

    // The job slowed down sharply...
    curRecipe.p95JobDurationMs should be > refRecipe.p95JobDurationMs
    curRecipe.avgJobDurationMs should be > refRecipe.avgJobDurationMs
    // ...while staying pinned at the same executor ceiling (the censoring trap)...
    curRecipe.p95RunMaxExecutors shouldBe refRecipe.p95RunMaxExecutors
    // ...and the cap-pressure gate is satisfiable from fraction_reaching_cap alone.
    curRecipe.fractionReachingCap.getOrElse(0.0) should be >= 0.5
    // Enough runs on each side for the trend scaler's confidence floor.
    refRecipe.runs should be >= 5L
    curRecipe.runs should be >= 5L
  }
}
