package com.db.serna.orchestration.cluster_tuning

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ClusterMachineAndRecipeTunerSpec extends AnyFunSuite with Matchers {

  private val defaultMachine = MachineCatalog.byName("e2-standard-8").get
  private val defaultPolicy  = DefaultTuningStrategy.toTuningPolicy(defaultMachine)
  private val defaultPref    = MachineSelectionPreference.Default

  // ── QuotaTracker ──────────────────────────────────────────────────────────

  test("QuotaTracker withinQuota blocks machines in excludedFamilies (n4, n4d)") {
    val tracker = new QuotaTracker(Quotas.Default)
    val n4Machine = MachineType("n4-standard-32", 32, 128)
    tracker.withinQuota(n4Machine, 5, defaultPref) shouldBe false
  }

  test("QuotaTracker withinQuota allows machines in allowedFamilies when below quota") {
    val tracker = new QuotaTracker(Quotas.Default)
    val n2Machine = MachineCatalog.byName("n2-standard-32").get
    tracker.withinQuota(n2Machine, 4, defaultPref) shouldBe true
  }

  test("QuotaTracker withinQuota blocks c3 after c3MaxClusters is reached") {
    val tracker = new QuotaTracker(Quotas.Default)
    val c3Machine = MachineCatalog.defaults.find(_.name.startsWith("c3")).get
    // Record first cluster using c3 → now at cap (c3MaxClusters=1)
    tracker.recordCluster(c3Machine, 2, defaultPref)
    // Second cluster: should be blocked
    tracker.withinQuota(c3Machine, 2, defaultPref) shouldBe false
  }

  test("QuotaTracker withinQuota blocks c4 after c4MaxClusters is reached") {
    val tracker = new QuotaTracker(Quotas.Default)
    val c4Machine = MachineCatalog.defaults.find(_.name.startsWith("c4")).get
    tracker.recordCluster(c4Machine, 2, defaultPref)
    tracker.withinQuota(c4Machine, 2, defaultPref) shouldBe false
  }

  test("QuotaTracker usageSummary reflects accumulated cores") {
    val tracker = new QuotaTracker(Quotas.Default)
    val n2Machine = MachineCatalog.byName("n2-standard-32").get
    tracker.recordCluster(n2Machine, 4, defaultPref)
    val summary = tracker.usageSummary
    summary("n2")._1 shouldBe 32 * 4
    summary("n2")._2 shouldBe 5000
  }

  test("QuotaTracker usageSummary starts at zero for all families") {
    val tracker = new QuotaTracker(Quotas.Default)
    val summary = tracker.usageSummary
    summary.values.map(_._1).sum shouldBe 0
  }

  test("QuotaTracker withinQuota returns false when adding cores would exceed quota") {
    val tightQuotas = Quotas(e2 = 8, n2 = 8, n2d = 8, c3 = 4, c4 = 4, n4 = 4, n4d = 4)
    val tracker = new QuotaTracker(tightQuotas)
    val e2Machine = MachineCatalog.byName("e2-standard-8").get
    // Use up the quota: 8 cores * 1 worker = 8 = quota
    tracker.recordCluster(e2Machine, 1, defaultPref)
    // Now adding more should fail
    tracker.withinQuota(e2Machine, 1, defaultPref) shouldBe false
  }

  // ── planCluster ───────────────────────────────────────────────────────────

  test("planCluster returns valid ClusterPlan with at least 2 workers") {
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 4.0, 8.0, 60000, 90000, 10L, None, None, None, None, Some(2))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    plan.workers should be >= 2
    plan.clusterName shouldBe "c1"
    plan.maxExecutorsSupported should be >= 1
    plan.executorsPerWorker should be >= 1
  }

  test("planCluster selects a machine from allowedFamilies only") {
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 4.0, 8.0, 60000, 90000, 10L, None, None, None, None, Some(1))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    val family = plan.workerMachineType.name.takeWhile(_ != '-')
    defaultPref.allowedFamilies should contain(family)
  }

  test("planCluster does not select excluded families (n4, n4d)") {
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 10.0, 20.0, 120000, 180000, 5L, None, None, None, None, Some(3))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    val family = plan.workerMachineType.name.takeWhile(_ != '-')
    defaultPref.excludedFamilies should not contain family
  }

  test("planCluster records cluster in QuotaTracker after planning") {
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 4.0, 8.0, 60000, 90000, 10L, None, None, None, None, Some(1))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    val family = plan.workerMachineType.name.takeWhile(_ != '-')
    val (used, _) = tracker.usageSummary(family)
    used should be > 0
  }

  // ── planManualRecipes ─────────────────────────────────────────────────────

  test("planManualRecipes caps instances at cluster maxExecutorsSupported") {
    val clusterPlan = ClusterPlan("c1",
      MachineCatalog.byName("n2-standard-32").get,
      MachineCatalog.byName("n2-standard-32").get,
      workers = 4, executorsPerWorker = 4, maxExecutorsSupported = 16
    )
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 20.0, 200.0, 60000, 90000, 5L, None, None, None, None, Some(1))
    )
    val result = ClusterMachineAndRecipeTuner.planManualRecipes(clusterPlan, metrics, defaultPolicy)
    result.head.sparkExecutorInstances should be <= 16
  }

  test("planManualRecipes applies cap-hit boost when fractionReachingCap exceeds threshold") {
    val clusterPlan = ClusterPlan("c1",
      defaultMachine, defaultMachine,
      workers = 4, executorsPerWorker = 4, maxExecutorsSupported = 16
    )
    val base = 4.0
    val withCap = Seq(
      RecipeMetrics("c1", "r1", 2.0, base, 60000, 90000, 10L, Some(100L), Some(5L), Some(10L), Some(0.5), Some(1))
    )
    val withoutCap = Seq(
      RecipeMetrics("c1", "r1", 2.0, base, 60000, 90000, 10L, None, None, None, Some(0.0), Some(1))
    )
    val boosted = ClusterMachineAndRecipeTuner.planManualRecipes(clusterPlan, withCap, defaultPolicy)
    val unboosted = ClusterMachineAndRecipeTuner.planManualRecipes(clusterPlan, withoutCap, defaultPolicy)
    boosted.head.sparkExecutorInstances should be >= unboosted.head.sparkExecutorInstances
  }

  test("planManualRecipes uses 8 cores when preferEightCoreExecutors=true and capacity allows") {
    val clusterPlan = ClusterPlan("c1",
      MachineCatalog.byName("n2-standard-32").get,
      MachineCatalog.byName("n2-standard-32").get,
      workers = 4, executorsPerWorker = 4, maxExecutorsSupported = 16
    )
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 2.0, 2.0, 60000, 90000, 10L, None, None, None, None, Some(1))
    )
    val result = ClusterMachineAndRecipeTuner.planManualRecipes(clusterPlan, metrics, defaultPolicy)
    result.head.sparkExecutorCores shouldBe 8
  }

  // ── planDARecipes ─────────────────────────────────────────────────────────

  test("planDARecipes minExecutors is at least 2") {
    val clusterPlan = ClusterPlan("c1", defaultMachine, defaultMachine,
      workers = 4, executorsPerWorker = 2, maxExecutorsSupported = 8)
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 1.0, 1.0, 60000, 90000, 10L, None, None, None, None, Some(1))
    )
    val result = ClusterMachineAndRecipeTuner.planDARecipes(clusterPlan, metrics, defaultPolicy)
    result.head.minExecutors should be >= 2
  }

  test("planDARecipes maxExecutors is clamped to cluster maxExecutorsSupported") {
    val clusterPlan = ClusterPlan("c1", defaultMachine, defaultMachine,
      workers = 2, executorsPerWorker = 2, maxExecutorsSupported = 4)
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 50.0, 500.0, 60000, 90000, 10L, None, None, None, None, Some(1))
    )
    val result = ClusterMachineAndRecipeTuner.planDARecipes(clusterPlan, metrics, defaultPolicy)
    result.head.maxExecutors should be <= 4
  }

  test("planDARecipes initialExecutors >= minExecutors") {
    val clusterPlan = ClusterPlan("c1", defaultMachine, defaultMachine,
      workers = 4, executorsPerWorker = 2, maxExecutorsSupported = 8)
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 3.0, 5.0, 60000, 90000, 5L, None, None, None, None, Some(1))
    )
    val result = ClusterMachineAndRecipeTuner.planDARecipes(clusterPlan, metrics, defaultPolicy)
    result.head.initialExecutors should be >= result.head.minExecutors
  }

  // ── ClusterSummary sort helpers ───────────────────────────────────────────

  test("sortByTopJobs orders by descending noOfJobs") {
    val s1 = ClusterSummary("c1", "d1", 5, 2, "e2-standard-8", "e2-standard-8", 10.0, 1.0, "T", "00:00")
    val s2 = ClusterSummary("c2", "d2", 10, 3, "n2-standard-32", "n2-standard-32", 20.0, 2.0, "T", "00:00")
    val s3 = ClusterSummary("c3", "d3", 3, 1, "e2-standard-8", "e2-standard-8", 5.0, 0.5, "T", "00:00")
    val result = ClusterMachineAndRecipeTuner.sortByTopJobs(Seq(s1, s2, s3))
    result.map(_.clusterName) shouldBe Seq("c2", "c1", "c3")
  }

  test("sortByNumOfWorkers orders by descending numOfWorkers") {
    val s1 = ClusterSummary("c1", "d1", 5, 2, "e2-standard-8", "e2-standard-8", 10.0, 1.0, "T", "00:00")
    val s2 = ClusterSummary("c2", "d2", 3, 6, "n2-standard-32", "n2-standard-32", 20.0, 2.0, "T", "00:00")
    val result = ClusterMachineAndRecipeTuner.sortByNumOfWorkers(Seq(s1, s2))
    result.head.clusterName shouldBe "c2"
  }

  test("sortByEstimatedCostEur orders by descending cost") {
    val s1 = ClusterSummary("c1", "d1", 5, 2, "e2-standard-8", "e2-standard-8", 10.0, 1.0, "T", "00:00")
    val s2 = ClusterSummary("c2", "d2", 3, 6, "n2-standard-32", "n2-standard-32", 20.0, 9.9, "T", "00:00")
    val result = ClusterMachineAndRecipeTuner.sortByEstimatedCostEur(Seq(s1, s2))
    result.head.clusterName shouldBe "c2"
  }

  test("sortByTotalActiveMinutes orders by descending minutes") {
    val s1 = ClusterSummary("c1", "d1", 5, 2, "e2-standard-8", "e2-standard-8", 99.0, 1.0, "T", "00:00")
    val s2 = ClusterSummary("c2", "d2", 3, 6, "n2-standard-32", "n2-standard-32", 1.0, 2.0, "T", "00:00")
    val result = ClusterMachineAndRecipeTuner.sortByTotalActiveMinutes(Seq(s1, s2))
    result.head.clusterName shouldBe "c1"
  }

  // ── AutoscalingPolicyConfig ───────────────────────────────────────────────

  test("AutoscalingPolicyConfig.resolvePolicy brackets") {
    import ClusterMachineAndRecipeTuner.AutoscalingPolicyConfig
    AutoscalingPolicyConfig.resolvePolicy(1) shouldBe "small-workload-autoscaling"
    AutoscalingPolicyConfig.resolvePolicy(4) shouldBe "small-workload-autoscaling"
    AutoscalingPolicyConfig.resolvePolicy(5) shouldBe "medium-workload-autoscaling"
    AutoscalingPolicyConfig.resolvePolicy(6) shouldBe "medium-workload-autoscaling"
    AutoscalingPolicyConfig.resolvePolicy(7) shouldBe "large-workload-autoscaling"
    AutoscalingPolicyConfig.resolvePolicy(8) shouldBe "large-workload-autoscaling"
    AutoscalingPolicyConfig.resolvePolicy(9) shouldBe "extra-large-workload-autoscaling"
    AutoscalingPolicyConfig.resolvePolicy(100) shouldBe "extra-large-workload-autoscaling"
  }

  test("AutoscalingPolicyConfig.maxWorkersForCluster returns bracket ceiling") {
    import ClusterMachineAndRecipeTuner.AutoscalingPolicyConfig
    AutoscalingPolicyConfig.maxWorkersForCluster(1) shouldBe 4
    AutoscalingPolicyConfig.maxWorkersForCluster(4) shouldBe 4
    AutoscalingPolicyConfig.maxWorkersForCluster(5) shouldBe 6
    AutoscalingPolicyConfig.maxWorkersForCluster(6) shouldBe 6
    AutoscalingPolicyConfig.maxWorkersForCluster(7) shouldBe 8
    AutoscalingPolicyConfig.maxWorkersForCluster(8) shouldBe 8
    AutoscalingPolicyConfig.maxWorkersForCluster(9) shouldBe 10
    AutoscalingPolicyConfig.maxWorkersForCluster(10) shouldBe 10
    AutoscalingPolicyConfig.maxWorkersForCluster(15) shouldBe 15  // fallback = observed
  }

  // ── GenerationSummary ─────────────────────────────────────────────────────

  test("GenerationSummary totalPredictedNodes = sum of (numWorkers + 1) across entries") {
    val entries = Seq(
      GenerationSummaryEntry("c1", "n2-standard-32", "n2", 4, 6, 128, 192,
        Seq.empty, "default", "cost_performance_balance", "8cx1GBpc"),
      GenerationSummaryEntry("c2", "e2-standard-8", "e2", 2, 4, 16, 32,
        Seq.empty, "default", "cost_performance_balance", "8cx1GBpc")
    )
    val predicted = entries.map(_.numWorkers + 1).sum
    predicted shouldBe 7  // (4+1) + (2+1)
  }

  test("GenerationSummary totalMaxNodes = sum of (maxWorkersFromPolicy + 1) across entries") {
    val entries = Seq(
      GenerationSummaryEntry("c1", "n2-standard-32", "n2", 4, 6, 128, 192,
        Seq.empty, "default", "balance", "8cx1GBpc"),  // maxWorkersFromPolicy=6 → +1 = 7
      GenerationSummaryEntry("c2", "e2-standard-8", "e2", 2, 4, 16, 32,
        Seq.empty, "default", "balance", "8cx1GBpc")   // maxWorkersFromPolicy=4 → +1 = 5
    )
    entries.map(_.maxWorkersFromPolicy + 1).sum shouldBe 12  // 7 + 5 = 12
  }

  test("GenerationSummaryWriter.toJson produces valid JSON structure") {
    val entries = Seq(
      GenerationSummaryEntry("c1", "n2-standard-32", "n2", 4, 6, 128, 192,
        Seq("signal 1"), "default", "balance", "8cx1GBpc")
    )
    val summary = GenerationSummary(
      generatedAt = "2026-01-01T00:00:00Z",
      date = "2026_01_01",
      strategyName = "default",
      biasMode = "cost_performance_balance",
      topologyPreset = "8cx1GBpc",
      quotas = Quotas.Default,
      totalClusters = 1,
      totalPredictedNodes = 5,
      totalMaxNodes = 7,
      quotaUsageByFamily = Map("n2" -> (128, 5000)),
      clustersWithDiagnosticOverrides = 0,
      entries = entries
    )
    val json = GenerationSummaryWriter.toJson(summary)
    json should include("generated_at")
    json should include("c1")
    json should include("n2-standard-32")
    json should include("signal 1")
    json should include("quota_usage_by_family")
  }
}
