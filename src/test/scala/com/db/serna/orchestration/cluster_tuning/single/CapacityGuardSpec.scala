package com.db.serna.orchestration.cluster_tuning.single

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CapacityGuardSpec extends AnyFunSuite with Matchers {

  // Node n2d-standard-48 = 48 cores / 192 GB; 6 max workers; default ratio 0.90.
  private def g(
      isManual: Boolean = false,
      min: Int = 2,
      initial: Int = 2,
      max: Int = 100,
      ec: Int = 4,
      em: Int = 8,
      nodeCores: Int = 48,
      nodeMem: Int = 192,
      maxWorkers: Int = 6,
      ratio: Double = 0.90
  ): GuardResult =
    CapacityGuard.guard(isManual, min, initial, max, ec, em, nodeCores, nodeMem, maxWorkers, ratio)

  test("scaledMax multiplies per-node by maxWorkers") {
    CapacityGuard.scaledMax(48, 192, 6) shouldBe ((288, 1152))
  }

  test("plenty of headroom: no clamp, Ok, percentages computed against scaled-max") {
    val r = g(max = 10, ec = 4, em = 8) // 10 execs: 40 cores, 80 GB
    r.newMax shouldBe 10
    r.status shouldBe CapacityStatus.Ok
    r.maxCoreUsagePct shouldBe 13.9 +- 0.05 // 40/288
    r.maxMemoryUsagePct shouldBe 6.9 +- 0.05 // 80/1152
  }

  test("memory-heavy executor: per-node packing clamps BELOW the aggregate min (the wedge case)") {
    // 4c/18GB exec. Per-node = min(floor(.9*48/4)=10, floor(.9*192/18)=9) = 9 -> 9*6 = 54.
    // Aggregate min(floor(.9*288/4)=64, floor(.9*1152/18)=57) = 57. Packing (54) < aggregate (57).
    val r = g(max = 100, ec = 4, em = 18)
    r.newMax shouldBe 54
    r.status shouldBe CapacityStatus.Clamped
  }

  test("cores-binding clamp") {
    // 8c/4GB exec. Per-node = min(floor(.9*48/8)=5, floor(.9*192/4)=43) = 5 -> 30.
    val r = g(max = 100, ec = 8, em = 4)
    r.newMax shouldBe 30
    r.status shouldBe CapacityStatus.Clamped
  }

  test("manual recipe clamps instances; min==initial==max") {
    val r = g(isManual = true, min = 80, initial = 80, max = 80, ec = 8, em = 4) // cap 30
    r.newMax shouldBe 30
    r.newMin shouldBe 30
    r.newInitial shouldBe 30
    r.status shouldBe CapacityStatus.Clamped
  }

  test("min and initial are pulled down when the new max drops below them") {
    val r = g(min = 40, initial = 50, max = 100, ec = 8, em = 4) // cap 30
    r.newMax shouldBe 30
    r.newMin shouldBe 30
    r.newInitial shouldBe 30
  }

  test("tight: a single executor exceeds ratio*node but still fits the raw node") {
    // em=180 on a 192 GB node, ratio .9 -> .9*192=172.8 < 180 so perNodeRatio mem floor = 0,
    // but raw 192/180 = 1 -> Tight; cap = maxWorkers (1 per node).
    val r = g(max = 100, ec = 4, em = 180, maxWorkers = 6)
    r.newMax shouldBe 6
    r.status shouldBe CapacityStatus.Tight
  }

  test("infeasible: a single executor is bigger than a whole node") {
    val r = g(max = 100, ec = 4, em = 256) // em > nodeMem
    r.newMax shouldBe 1
    r.status shouldBe CapacityStatus.Infeasible
  }

  test("idempotence: guarding an already-guarded allocation is a no-op") {
    val once = g(max = 100, ec = 4, em = 18) // 54
    val twice = g(max = once.newMax, ec = 4, em = 18)
    twice.newMax shouldBe once.newMax
    twice.status shouldBe CapacityStatus.Ok // already at cap, nothing reduced
  }

  test("ratio override changes the cap") {
    g(max = 100, ec = 4, em = 8, ratio = 1.0).newMax shouldBe 72 // min(48*6/4, 192*6/8)=min(72,144)
    g(max = 100, ec = 4, em = 8, ratio = 0.5).newMax shouldBe 36 // floor(.5*48/4)=6 -> 36
  }
}
