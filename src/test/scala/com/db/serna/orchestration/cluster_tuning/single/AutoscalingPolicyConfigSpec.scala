package com.db.serna.orchestration.cluster_tuning.single

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.db.serna.orchestration.cluster_tuning.single.ClusterMachineAndRecipeTuner.AutoscalingPolicyConfig

class AutoscalingPolicyConfigSpec extends AnyFunSuite with Matchers {
  test("min workers equals the always-on primary count; max comes from the policy tier") {
    AutoscalingPolicyConfig.minWorkersForCluster(5) shouldBe 5
    AutoscalingPolicyConfig.maxWorkersForCluster(5) shouldBe 6
    AutoscalingPolicyConfig.minWorkersForCluster(3) shouldBe 3
    AutoscalingPolicyConfig.maxWorkersForCluster(3) shouldBe 4
  }
}
