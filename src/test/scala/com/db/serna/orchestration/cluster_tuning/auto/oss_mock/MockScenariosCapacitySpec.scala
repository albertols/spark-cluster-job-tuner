package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MockScenariosCapacitySpec extends AnyFunSuite with Matchers {
  test("capacityPressure is registered and carries a memory-heavy, cap-touching recipe") {
    MockScenarios.multiDate.keySet should contain("capacityPressure")
    val md = MockScenarios.multiDate("capacityPressure")("2099_04_01", "2099_04_02", 1234L)
    val cur = md.perDate("2099_04_02")
    val recipe = cur.clusters
      .flatMap(_.recipes)
      .find(_.name == "_CAPACITY_HOG.json")
      .getOrElse(fail("missing _CAPACITY_HOG.json"))
    // High avg executor demand + cap pressure so planning wants many big executors.
    recipe.avgExecutorsPerJob should be >= 12.0
    recipe.fractionReachingCap.getOrElse(0.0) should be >= 0.5
  }
}
