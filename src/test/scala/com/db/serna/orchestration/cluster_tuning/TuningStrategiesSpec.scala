package com.db.serna.orchestration.cluster_tuning

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TuningStrategiesSpec extends AnyFunSuite with Matchers {

  // ── ExecutorTopologyPreset ────────────────────────────────────────────────

  test("ExecutorTopologyPreset.totalMemoryGb = cores * memoryPerCoreGb") {
    ExecutorTopologyPreset(8, 1).totalMemoryGb shouldBe 8
    ExecutorTopologyPreset(8, 2).totalMemoryGb shouldBe 16
    ExecutorTopologyPreset(8, 4).totalMemoryGb shouldBe 32
    ExecutorTopologyPreset(4, 1).totalMemoryGb shouldBe 4
    ExecutorTopologyPreset(4, 2).totalMemoryGb shouldBe 8
  }

  test("ExecutorTopologyPreset.label formats as <cores>cx<memPc>GBpc") {
    ExecutorTopologyPreset(8, 1).label shouldBe "8cx1GBpc"
    ExecutorTopologyPreset(4, 2).label shouldBe "4cx2GBpc"
  }

  test("ExecutorTopologyPreset.Default is 8cx1GBpc (8 GB total — current default)") {
    ExecutorTopologyPreset.Default shouldBe ExecutorTopologyPreset(8, 1)
    ExecutorTopologyPreset.Default.totalMemoryGb shouldBe 8
  }

  test("ExecutorTopologyPreset.fromLabel round-trips all defined presets") {
    val labels = Seq("8cx1GBpc", "8cx2GBpc", "8cx4GBpc", "4cx1GBpc", "4cx2GBpc")
    labels.foreach { lbl =>
      val result = ExecutorTopologyPreset.fromLabel(lbl)
      result shouldBe defined
      result.get.label shouldBe lbl
    }
  }

  test("ExecutorTopologyPreset.fromLabel returns None for unknown label") {
    ExecutorTopologyPreset.fromLabel("unknown") shouldBe None
    ExecutorTopologyPreset.fromLabel("") shouldBe None
  }

  test("ExecutorTopologyPreset requires positive cores and memoryPerCoreGb") {
    an[IllegalArgumentException] should be thrownBy ExecutorTopologyPreset(0, 1)
    an[IllegalArgumentException] should be thrownBy ExecutorTopologyPreset(8, 0)
  }

  // ── BiasMode ─────────────────────────────────────────────────────────────

  test("CostBiased has higher costWeight and lower concurrencyBufferPct than PerformanceBiased") {
    CostBiased.costWeight should be > PerformanceBiased.costWeight
    CostBiased.concurrencyBufferPct should be < PerformanceBiased.concurrencyBufferPct
  }

  test("PerformanceBiased has higher sufficiencyWeight than CostBiased") {
    PerformanceBiased.sufficiencyWeight should be > CostBiased.sufficiencyWeight
  }

  test("CostPerformanceBalance matches legacy hardcoded weights exactly") {
    CostPerformanceBalance.costWeight shouldBe 0.4
    CostPerformanceBalance.sufficiencyWeight shouldBe 0.5
    CostPerformanceBalance.utilizationWeight shouldBe 0.3
    CostPerformanceBalance.workerPenaltyWeight shouldBe 0.3
    CostPerformanceBalance.oversizePenaltyWeight shouldBe 0.2
    CostPerformanceBalance.concurrencyBufferPct shouldBe 0.25
  }

  test("BiasMode.fromName resolves known names") {
    BiasMode.fromName("cost_biased") shouldBe Some(CostBiased)
    BiasMode.fromName("cost") shouldBe Some(CostBiased)
    BiasMode.fromName("performance_biased") shouldBe Some(PerformanceBiased)
    BiasMode.fromName("perf") shouldBe Some(PerformanceBiased)
    BiasMode.fromName("balance") shouldBe Some(CostPerformanceBalance)
    BiasMode.fromName("default") shouldBe Some(CostPerformanceBalance)
  }

  test("BiasMode.fromName returns None for unknown names") {
    BiasMode.fromName("unknown") shouldBe None
  }

  // ── MachineSelectionPreference ────────────────────────────────────────────

  test("MachineSelectionPreference.Default excludes n4 and n4d") {
    MachineSelectionPreference.Default.excludedFamilies should contain("n4")
    MachineSelectionPreference.Default.excludedFamilies should contain("n4d")
  }

  test("MachineSelectionPreference.Default includes n2, n2d, e2, c3, c4 as allowed") {
    val allowed = MachineSelectionPreference.Default.allowedFamilies
    allowed should contain("n2")
    allowed should contain("n2d")
    allowed should contain("e2")
    allowed should contain("c3")
    allowed should contain("c4")
  }

  test("MachineSelectionPreference.Default family priority: n2 < n2d < e2 < c3 < c4") {
    val pref = MachineSelectionPreference.Default
    pref.familyPriority("n2") should be < pref.familyPriority("n2d")
    pref.familyPriority("n2d") should be < pref.familyPriority("e2")
    pref.familyPriority("e2") should be < pref.familyPriority("c3")
    pref.familyPriority("c3") should be < pref.familyPriority("c4")
  }

  test("MachineSelectionPreference.Default preferredCores is 32") {
    MachineSelectionPreference.Default.preferredCores shouldBe 32
  }

  test("MachineSelectionPreference.Default c3MaxClusters and c4MaxClusters are 1") {
    MachineSelectionPreference.Default.c3MaxClusters shouldBe 1
    MachineSelectionPreference.Default.c4MaxClusters shouldBe 1
  }

  // ── Quotas ────────────────────────────────────────────────────────────────

  test("Quotas.forFamily returns correct values") {
    val q = Quotas()
    q.forFamily("e2") shouldBe 5000
    q.forFamily("n2") shouldBe 5000
    q.forFamily("n2d") shouldBe 3000
    q.forFamily("c3") shouldBe 500
    q.forFamily("c4") shouldBe 500
    q.forFamily("n4") shouldBe 500
    q.forFamily("n4d") shouldBe 500
    q.forFamily("unknown") shouldBe 0
  }

  // ── TuningStrategy ────────────────────────────────────────────────────────

  test("DefaultTuningStrategy.toTuningPolicy produces TuningPolicy matching legacy hardcoded values") {
    val defaultMachine = MachineCatalog.byName("e2-standard-8").get
    val policy = DefaultTuningStrategy.toTuningPolicy(defaultMachine)
    policy.executorCores shouldBe 8           // 8cx1GBpc → cores=8
    policy.executorMemoryGb shouldBe 8        // 8*1=8 GB total
    policy.preferEightCoreExecutors shouldBe true
    policy.costWeight shouldBe 0.4
    policy.sufficiencyWeight shouldBe 0.5
    policy.utilizationWeight shouldBe 0.3
    policy.workerPenaltyWeight shouldBe 0.3
    policy.oversizePenaltyWeight shouldBe 0.2
    policy.concurrencyBufferPct shouldBe 0.25
    policy.capHitBoostPct shouldBe 0.20
    policy.capHitThreshold shouldBe 0.30
    policy.preferMaxWorkers shouldBe 6
    policy.memoryOverheadRatio shouldBe 0.10
    policy.osAndDaemonsReserveGb shouldBe 4
    policy.manualInstancesFrom shouldBe "p95"
    policy.daMinFrom shouldBe "avg"
    policy.daInitialEqualsMin shouldBe true
  }

  test("CostBiasedStrategy.toTuningPolicy has lower concurrencyBufferPct than Default") {
    val m = MachineCatalog.byName("e2-standard-8").get
    CostBiasedStrategy.toTuningPolicy(m).concurrencyBufferPct should be <
      DefaultTuningStrategy.toTuningPolicy(m).concurrencyBufferPct
  }

  test("PerformanceBiasedStrategy.toTuningPolicy has higher executorMemoryGb than Default") {
    val m = MachineCatalog.byName("e2-standard-8").get
    PerformanceBiasedStrategy.toTuningPolicy(m).executorMemoryGb should be >
      DefaultTuningStrategy.toTuningPolicy(m).executorMemoryGb
  }

  test("TuningStrategy.fromName resolves all known strategies") {
    TuningStrategy.fromName("default") shouldBe Some(DefaultTuningStrategy)
    TuningStrategy.fromName("cost_biased") shouldBe Some(CostBiasedStrategy)
    TuningStrategy.fromName("cost") shouldBe Some(CostBiasedStrategy)
    TuningStrategy.fromName("performance_biased") shouldBe Some(PerformanceBiasedStrategy)
    TuningStrategy.fromName("perf") shouldBe Some(PerformanceBiasedStrategy)
    TuningStrategy.fromName("unknown") shouldBe None
  }

  test("PerformanceBiasedStrategy has preferEightCoreExecutors=true (8-core topology)") {
    val m = MachineCatalog.byName("e2-standard-8").get
    PerformanceBiasedStrategy.toTuningPolicy(m).preferEightCoreExecutors shouldBe true
  }
}
