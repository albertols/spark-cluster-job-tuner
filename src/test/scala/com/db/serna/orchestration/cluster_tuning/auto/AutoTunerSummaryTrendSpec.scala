package com.db.serna.orchestration.cluster_tuning.auto

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.db.serna.orchestration.cluster_tuning.single.refinement.{BoostState, ScaleDirection, TrendScaleDecision}

class AutoTunerSummaryTrendSpec extends AnyFunSuite with Matchers {

  test("trend boost group records code, source=trend, and direction counts") {
    val decisions = Seq(
      TrendScaleDecision("_a.json", isManual = false, 2, 2, 3, 3, 3, 5, 1.5, 1.5, ScaleDirection.Up, BoostState.New, "up"),
      TrendScaleDecision("_b.json", isManual = false, 8, 8, 12, 6, 6, 8, 0.7, 0.7, ScaleDirection.Down, BoostState.New, "down")
    )
    val group = ClusterMachineAndRecipeAutoTuner.trendBoostGroup(Seq(("c1", decisions)))
    group should include("\"code\":\"executor_trend\"")
    group should include("\"source\":\"trend\"")
    group should include("\"count_up\":1")
    group should include("\"count_down\":1")
    group should include("\"cluster_count\":1")
  }

  test("empty trend input still produces a well-formed zero-count group") {
    val group = ClusterMachineAndRecipeAutoTuner.trendBoostGroup(Seq.empty)
    group should include("\"code\":\"executor_trend\"")
    group should include("\"count\":0")
    group should include("\"entries\":[]")
  }
}
