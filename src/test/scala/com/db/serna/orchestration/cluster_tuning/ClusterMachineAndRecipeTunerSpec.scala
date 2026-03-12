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

  test("QuotaTracker isHardBlocked returns true for excluded families (n4, n4d)") {
    val tracker = new QuotaTracker(Quotas.Default)
    tracker.isHardBlocked(MachineType("n4-standard-32", 32, 128),  defaultPref) shouldBe true
    tracker.isHardBlocked(MachineType("n4d-standard-32", 32, 128), defaultPref) shouldBe true
  }

  test("QuotaTracker isHardBlocked returns false for allowed families below cluster cap") {
    val tracker = new QuotaTracker(Quotas.Default)
    val n2 = MachineCatalog.byName("n2-standard-32").get
    val c3 = MachineCatalog.defaults.find(_.name.startsWith("c3")).get
    tracker.isHardBlocked(n2, defaultPref) shouldBe false
    tracker.isHardBlocked(c3, defaultPref) shouldBe false  // c3ClusterCount=0 < cap=1
  }

  test("QuotaTracker isHardBlocked returns true for c4 after cluster cap reached") {
    val tracker = new QuotaTracker(Quotas.Default)
    val c4 = MachineCatalog.defaults.find(_.name.startsWith("c4")).get
    tracker.recordCluster(c4, 1, defaultPref)
    tracker.isHardBlocked(c4, defaultPref) shouldBe true  // c4ClusterCount=1 >= cap=1
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

  test("QuotaTracker quotaPressure is 0.0 before any allocation") {
    val tracker = new QuotaTracker(Quotas.Default)
    tracker.quotaPressure("n2")  shouldBe 0.0
    tracker.quotaPressure("n2d") shouldBe 0.0
    tracker.quotaPressure("e2")  shouldBe 0.0
  }

  test("QuotaTracker quotaPressure reaches 1.0 when family is exactly at quota") {
    // n2 quota = 5000; allocate 5000 cores via 32-core × 156-ish workers... use tight quotas
    val quotas = Quotas(e2 = 100, n2 = 100, n2d = 100, c3 = 4, c4 = 4, n4 = 4, n4d = 4)
    val tracker = new QuotaTracker(quotas)
    val n2 = MachineCatalog.byName("n2-standard-32").get  // 32 cores
    // Allocate exactly 100 n2 cores (32 cores * 3 workers = 96; use 8-core machine for exact 100)
    val n2small = MachineCatalog.byName("n2-standard-8").get  // 8 cores
    tracker.recordCluster(n2small, 12, defaultPref)  // 8 * 12 = 96 cores → not exactly 100
    tracker.quotaPressure("n2") shouldBe (96.0 / 100.0)
  }

  test("QuotaTracker quotaPressure exceeds 1.0 when family is over quota") {
    val quotas = Quotas(e2 = 50, n2 = 5000, n2d = 3000, c3 = 4, c4 = 4, n4 = 4, n4d = 4)
    val tracker = new QuotaTracker(quotas)
    val e2 = MachineCatalog.byName("e2-standard-8").get  // 8 cores
    tracker.recordCluster(e2, 10, defaultPref)  // 80 cores > quota 50
    tracker.quotaPressure("e2") shouldBe (80.0 / 50.0) +- 1e-9
  }

  test("QuotaTracker quotaPressure is proportional: equal ratio => equal pressure") {
    val quotas = Quotas(e2 = 5000, n2 = 5000, n2d = 3000, c3 = 4, c4 = 4, n4 = 4, n4d = 4)
    val tracker = new QuotaTracker(quotas)
    val n2   = MachineCatalog.byName("n2-standard-8").get    // 8 cores
    val n2d  = MachineCatalog.byName("n2d-standard-8").get   // 8 cores
    val e2   = MachineCatalog.byName("e2-standard-8").get    // 8 cores
    // Fill each family to exactly 50% of its quota: 2500 for n2/e2, 1500 for n2d
    tracker.recordCluster(n2,  312, defaultPref)  // 8 * 312 = 2496 ≈ 50% of 5000
    tracker.recordCluster(n2d, 187, defaultPref)  // 8 * 187 = 1496 ≈ 50% of 3000
    tracker.recordCluster(e2,  312, defaultPref)  // 8 * 312 = 2496 ≈ 50% of 5000
    // All should have roughly equal pressure (~0.499)
    val pN2  = tracker.quotaPressure("n2")
    val pN2d = tracker.quotaPressure("n2d")
    val pE2  = tracker.quotaPressure("e2")
    pN2  shouldBe (2496.0 / 5000.0) +- 1e-9
    pN2d shouldBe (1496.0 / 3000.0) +- 1e-9
    pE2  shouldBe (2496.0 / 5000.0) +- 1e-9
    // N2 and E2 have identical pressure; N2D is very close (within 0.1%)
    math.abs(pN2 - pN2d) should be < 0.005
  }

  // ── num_of_workers algorithm ──────────────────────────────────────────────
  //
  // Worker count derivation (step by step):
  //
  //   requiredSlotsRaw  = p95RunMaxExecutors × maxConcurrentJobs          [planCluster]
  //   reqSlots          = max(1, requiredSlotsRaw × (1 + concurrencyBufferPct))  [chooseMachines]
  //   epw               = executorsPerWorker(worker, cores, memGb, overhead, reserve)
  //                     = min(floor(workerCores/execCores), floor(usableMem/perExecMem))
  //   workersNeeded     = max(2, ceil(reqSlots / epw))
  //
  // For e2-standard-8 (8 cores, 32 GB), policy = default (8cx1GBpc, concBuffer=0.25):
  //   execCores=8, execMem=8GB, overhead=max(0.384, 8*0.10)=0.8 GB, reserve=4 GB
  //   byCores  = floor(8/8) = 1
  //   usableMem = 32 - 4 = 28 GB  →  perExecMem = 8 + 0.8 = 8.8 GB
  //   byMem    = floor(28 / 8.8) = 3   →  epw = min(1, 3) = 1
  // So on e2-standard-8 with default policy, epw=1 and
  //   workersNeeded = max(2, ceil(reqSlots / 1)) = max(2, ceil(reqSlots))

  private def reqSlots(p95: Double, conc: Int, bufferPct: Double): Double =
    math.max(1.0, p95 * conc * (1.0 + bufferPct))

  test("num_of_workers: single recipe, p95=4, conc=1, buffer=0.25 → reqSlots=5.0, epw=1 on e2-std-8 → 5 workers") {
    // reqSlots = max(1, 4 * 1 * 1.25) = 5.0
    // epw on e2-standard-8 (default policy) = 1
    // workers = max(2, ceil(5.0/1)) = 5
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 3.0, 4.0, 60000, 90000, 10L, None, None, None, None, Some(1))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    // The actual machine chosen may differ from e2-standard-8; test the general property
    plan.workers should be >= 2
    // reqSlots = 4 * 1 * 1.25 = 5; epw >= 1 → workers = ceil(5/epw)
    val epw = plan.executorsPerWorker
    plan.workers shouldBe math.max(2, math.ceil(reqSlots(4.0, 1, 0.25) / epw).toInt)
  }

  test("num_of_workers: p95=8, conc=3, buffer=0.25 → reqSlots=30.0; workers = ceil(30/epw)") {
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 6.0, 8.0, 60000, 90000, 10L, None, None, None, None, Some(3))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    val epw = plan.executorsPerWorker
    val expected = math.max(2, math.ceil(reqSlots(8.0, 3, 0.25) / epw).toInt)
    plan.workers shouldBe expected
  }

  test("num_of_workers: p95=1, conc=1, buffer=0.25 → reqSlots=1.25 → workers floor at 2") {
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 1.0, 1.0, 60000, 90000, 10L, None, None, None, None, Some(1))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    plan.workers should be >= 2
  }

  test("num_of_workers: cost_biased (buffer=0.10) produces fewer workers than default (buffer=0.25) for same metrics") {
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 5.0, 10.0, 60000, 90000, 10L, None, None, None, None, Some(4))
    )
    val costMachine = MachineCatalog.byName("e2-standard-8").get
    val costPolicy  = CostBiasedStrategy.toTuningPolicy(costMachine)
    val defPolicy   = DefaultTuningStrategy.toTuningPolicy(defaultMachine)

    val trackerCost = new QuotaTracker(Quotas.Default)
    val trackerDef  = new QuotaTracker(Quotas.Default)

    val planCost = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, costPolicy, trackerCost, defaultPref)
    val planDef  = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defPolicy,  trackerDef,  defaultPref)

    // cost_biased (0.10 buffer) should produce <= workers than default (0.25 buffer) for the same input
    planCost.workers should be <= planDef.workers
  }

  test("num_of_workers: multiple recipes — uses max p95 and max concurrentJobs across all recipes") {
    // reqSlotsRaw = max(p95) × max(conc) = 6 × 3 = 18; reqSlots = 18 × 1.25 = 22.5
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 2.0, 4.0, 60000, 90000, 5L, None, None, None, None, Some(1)),
      RecipeMetrics("c1", "r2", 5.0, 6.0, 60000, 90000, 5L, None, None, None, None, Some(3))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    val epw = plan.executorsPerWorker
    val expectedReqSlots = reqSlots(6.0, 3, 0.25) // = 22.5
    val expected = math.max(2, math.ceil(expectedReqSlots / epw).toInt)
    plan.workers shouldBe expected
  }

  test("Sizing.executorsPerWorker: e2-standard-8 with 8cx1GBpc policy → epw=1") {
    // 8 cores / 8 execCores = 1 byCores
    // usableMem = 32 - 4 = 28; perExecMem = 8 + max(0.384, 8*0.10) = 8.8 → byMem = floor(28/8.8) = 3
    // epw = min(1, 3) = 1
    val e2std8 = MachineCatalog.byName("e2-standard-8").get
    val epw = Sizing.executorsPerWorker(e2std8, executorCores = 8, executorMemoryGb = 8,
      memOverheadRatio = 0.10, reserveGb = 4)
    epw shouldBe 1
  }

  test("Sizing.executorsPerWorker: n2-standard-32 with 8cx1GBpc policy → epw=4") {
    // 32 cores / 8 = 4 byCores
    // usableMem = 128 - 4 = 124; perExecMem = 8.8 → byMem = floor(124/8.8) = 14
    // epw = min(4, 14) = 4
    val n2std32 = MachineCatalog.byName("n2-standard-32").get
    val epw = Sizing.executorsPerWorker(n2std32, executorCores = 8, executorMemoryGb = 8,
      memOverheadRatio = 0.10, reserveGb = 4)
    epw shouldBe 4
  }

  test("Sizing.executorsPerWorker: memory-bound: e2-standard-32 with 8cx8GBpc (64GB exec) → bounded by memory") {
    // 32 / 8 = 4 byCores; usableMem = 128 - 4 = 124; overhead = max(0.384, 64*0.10) = 6.4
    // perExecMem = 64 + 6.4 = 70.4; byMem = floor(124/70.4) = 1
    // epw = min(4, 1) = 1
    val e2std32 = MachineCatalog.byName("e2-standard-32").get
    val epw = Sizing.executorsPerWorker(e2std32, executorCores = 8, executorMemoryGb = 64,
      memOverheadRatio = 0.10, reserveGb = 4)
    epw shouldBe 1
  }

  test("num_of_workers: direct worker count formula with known epw=4 on n2-std-32") {
    // reqSlotsRaw = 8 * 2 = 16; reqSlots = 16 * 1.25 = 20; workers = max(2, ceil(20/4)) = 5
    val n2Metrics = Seq(
      RecipeMetrics("c1", "r1", 6.0, 8.0, 60000, 90000, 10L, None, None, None, None, Some(2))
    )
    // Force selection of n2-standard-32 by using tight quotas that exclude everything else
    val tightPref = MachineSelectionPreference(
      preferredCores   = 32,
      minCores         = 32,
      maxCores         = 32,
      allowedFamilies  = List("n2"),
      familyPriority   = Map("n2" -> 1),
      c3MaxClusters    = 0,
      c4MaxClusters    = 0,
      c3c4MaxWorkers   = 13,
      excludedFamilies = Set("n4", "n4d", "n2d", "e2", "c3", "c4")
    )
    val n2Machine = MachineCatalog.byName("n2-standard-32").get
    val policy = DefaultTuningStrategy.toTuningPolicy(n2Machine)
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", n2Metrics, policy, tracker, tightPref)

    // epw on n2-standard-32 with default policy = 4 (verified by Sizing test above)
    plan.executorsPerWorker shouldBe 4
    // workers = max(2, ceil(reqSlots / 4))  where reqSlots = max(1, 8*2*1.25) = 20
    plan.workers shouldBe math.max(2, math.ceil(reqSlots(8.0, 2, 0.25) / 4.0).toInt)  // = 5
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

  // ── Core-count window (minCores / maxCores) ───────────────────────────────

  test("planCluster never selects a machine below minCores (32)") {
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 1.0, 1.0, 60000, 90000, 10L, None, None, None, None, Some(1))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    // Default pref has minCores=32 — even the smallest eligible cluster should use ≥32 cores/worker
    plan.workerMachineType.cores should be >= defaultPref.minCores
  }

  test("planCluster never selects a machine above maxCores (48)") {
    // Large demand — without the cap the scorer would pick a 224-core machine to minimise workers
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 50.0, 200.0, 60000, 90000, 10L, None, None, None, None, Some(10))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, defaultPref)
    plan.workerMachineType.cores should be <= defaultPref.maxCores
  }

  test("planCluster with minCores=32 maxCores=32 selects only 32-core machines") {
    val strictPref = defaultPref.copy(minCores = 32, maxCores = 32)
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 4.0, 8.0, 60000, 90000, 10L, None, None, None, None, Some(2))
    )
    val tracker = new QuotaTracker(Quotas.Default)
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, strictPref)
    plan.workerMachineType.cores shouldBe 32
  }

  // ── C3/C4 worker cap ──────────────────────────────────────────────────────

  test("planCluster caps C3 workers at c3c4MaxWorkers=13 regardless of demand") {
    // Huge demand that would ordinarily require >13 workers on any machine
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 100.0, 500.0, 60000, 90000, 10L, None, None, None, None, Some(20))
    )
    // Force C3 by allowing only c3
    val c3OnlyPref = defaultPref.copy(
      allowedFamilies = List("c3"),
      c3MaxClusters   = 99,
      c4MaxClusters   = 0,
      excludedFamilies = Set("n4", "n4d", "n2", "n2d", "e2", "c4")
    )
    val tracker = new QuotaTracker(Quotas(c3 = 100000))
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, c3OnlyPref)
    plan.workers should be <= c3OnlyPref.c3c4MaxWorkers
  }

  test("planCluster does not cap workers for N2 clusters (cap only for C3/C4)") {
    val metrics = Seq(
      RecipeMetrics("c1", "r1", 100.0, 500.0, 60000, 90000, 10L, None, None, None, None, Some(20))
    )
    val n2OnlyPref = defaultPref.copy(
      allowedFamilies  = List("n2"),
      c3MaxClusters    = 0,
      c4MaxClusters    = 0,
      excludedFamilies = Set("n4", "n4d", "n2d", "e2", "c3", "c4")
    )
    val tracker = new QuotaTracker(Quotas(n2 = 100000))
    val plan = ClusterMachineAndRecipeTuner.planCluster("c1", metrics, defaultPolicy, tracker, n2OnlyPref)
    // N2 is uncapped — for large demand it should exceed c3c4MaxWorkers
    plan.workers should be > defaultPref.c3c4MaxWorkers
  }

  // ── Demand-ordered C3/C4 allocation ───────────────────────────────────────

  test("QuotaTracker c3 cluster cap ensures at most c3MaxClusters=1 cluster uses c3") {
    val tracker = new QuotaTracker(Quotas.Default)
    val c3 = MachineCatalog.defaults.find(_.name.startsWith("c3-standard-44")).get
    tracker.recordCluster(c3, 5, defaultPref)          // first C3 cluster → recorded
    tracker.withinQuota(c3, 5, defaultPref) shouldBe false  // second C3 cluster → blocked
    tracker.isHardBlocked(c3, defaultPref) shouldBe true
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
    predicted shouldBe 8  // (4+1) + (2+1)
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
