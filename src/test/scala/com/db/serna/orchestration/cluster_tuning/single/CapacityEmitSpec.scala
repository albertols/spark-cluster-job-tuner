package com.db.serna.orchestration.cluster_tuning.single

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.db.serna.orchestration.cluster_tuning.single.ClusterMachineAndRecipeTuner._

class CapacityEmitSpec extends AnyFunSuite with Matchers {

  private val n2d48 = MachineType("n2d-standard-48", 48, 192)
  private def plan(cluster: String) =
    ClusterPlan(
      clusterName = cluster,
      masterMachineType = n2d48,
      workerMachineType = n2d48,
      workers = 5,
      executorsPerWorker = 11,
      maxExecutorsSupported = 66
    )

  test("daJson writes scaled-max clusterConf fields, min/max workers, and per-recipe % + clamp") {
    // Request 100 max execs of 4c/18GB -> per-node packing caps at 9*6 = 54.
    val da = Seq(
      RecipePlanDA(
        "_r.json",
        minExecutors = 2,
        maxExecutors = 100,
        initialExecutors = 2,
        sparkExecutorCores = 4,
        sparkExecutorMemoryGb = 18
      )
    )
    val json = daJson(plan("c1"), da, "tv", None, None, maxClusterUtilRatio = 0.90)

    json should include("\"min_workers\": 5")
    json should include("\"max_workers\": 6")
    json should include("\"cluster_scaled_max_cores\": 288")
    json should include("\"cluster_scaled_max_memory_gb\": 1152")
    json should include("\"spark.dynamicAllocation.maxExecutors\": \"54\"") // clamped
    json should include("\"maxMemoryUsagePct\":") // memory binds (~84%)
    json should include("\"capacityStatus\": \"clamped\"")
  }

  test("manualJson clamps instances and stamps %") {
    val m =
      Seq(RecipePlanManual("_m.json", sparkExecutorInstances = 100, sparkExecutorCores = 8, sparkExecutorMemoryGb = 4))
    val json = manualJson(plan("c2"), m, "tv", None, None, maxClusterUtilRatio = 0.90)
    json should include("\"spark.executor.instances\": \"30\"") // floor(.9*48/8)=5 -> 30
    json should include("\"maxCoreUsagePct\":")
  }
}
