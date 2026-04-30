package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Referential-integrity tests on the prebuilt scenarios. These run without
 * touching disk — they walk the in-memory `MockScenario` graphs.
 *
 * Failing one of these means an existing scenario is internally inconsistent
 * (e.g. an OOM event referencing a recipe the cluster doesn't have) and would
 * confuse downstream consumers.
 */
class ScenarioSpec extends AnyFunSuite with Matchers {

  private val testDate = "2099_01_01"
  private val refDate  = "2099_01_01"
  private val curDate  = "2099_01_02"

  private val scenarios: Seq[(String, MockScenario)] = Seq(
    "minimal"     -> MockScenarios.minimal(testDate),
    "baseline"    -> MockScenarios.baseline(testDate),
    "oomHeavy"    -> MockScenarios.oomHeavy(testDate),
    "autoscaling" -> MockScenarios.autoscaling(testDate)
  )

  // ── Per-scenario invariants ───────────────────────────────────────────────

  scenarios.foreach { case (label, scenario) =>

    test(s"$label: every cluster has a non-empty name and at least one incarnation") {
      scenario.clusters.foreach { c =>
        c.name should not be empty
        c.incarnations should not be empty
      }
    }

    test(s"$label: every incarnation falls inside the scenario window") {
      val (winStart, winEnd) = scenario.window
      scenario.clusters.flatMap(_.incarnations).foreach { inc =>
        inc.spanStart.isBefore(winStart) shouldBe false
        inc.spanEnd.isAfter(winEnd)     shouldBe false
      }
    }

    test(s"$label: every autoscaler schedule entry sits within [minPrimary, maxPrimary]") {
      val profiles = scenario.clusters.flatMap(_.incarnations).flatMap(_.autoscaler)
      profiles.foreach { p =>
        p.schedule.foreach { case (_, target) =>
          target should be >= p.minPrimary
          target should be <= p.maxPrimary
        }
      }
    }

    test(s"$label: every autoscaler event timestamp falls inside its incarnation span") {
      scenario.clusters.foreach { c =>
        c.incarnations.foreach { inc =>
          inc.autoscaler.foreach { p =>
            p.schedule.foreach { case (offSec, _) =>
              val ts = inc.spanStart.plusSeconds(offSec)
              withClue(s"cluster=${c.name} incarnation=$inc offsetSec=$offSec ts=$ts: ") {
                ts.isBefore(inc.spanStart) shouldBe false
                ts.isAfter(inc.spanEnd)    shouldBe false
              }
            }
          }
        }
      }
    }

    test(s"$label: every OOM event references a recipe the cluster owns") {
      scenario.clusters.foreach { c =>
        val recipeNames = c.recipes.map(_.name).toSet
        c.oomEvents.foreach { o =>
          recipeNames should contain(o.recipe)
        }
      }
    }

    test(s"$label: cluster names are unique within the scenario") {
      val names = scenario.clusters.map(_.name)
      names.distinct shouldBe names
    }
  }

  // ── Determinism ───────────────────────────────────────────────────────────

  test("same date+seed produces byte-identical b13 / b20 / b21 CSVs") {
    val s1 = MockScenarios.baseline(testDate, seed = 42L)
    val s2 = MockScenarios.baseline(testDate, seed = 42L)
    MockGen.b13Csv(s1) shouldBe MockGen.b13Csv(s2)
    MockGen.b20Csv(s1) shouldBe MockGen.b20Csv(s2)
    MockGen.b21Csv(s1) shouldBe MockGen.b21Csv(s2)
  }

  // ── Multi-date drift ──────────────────────────────────────────────────────

  test("multiDateBaseline emits exactly two dates, one shared and one drifted cluster set") {
    val multi = MockScenarios.multiDateBaseline(refDate, curDate)
    multi.perDate.keySet shouldBe Set(refDate, curDate)

    val refClusters = multi.perDate(refDate).clusters.map(_.name).toSet
    val curClusters = multi.perDate(curDate).clusters.map(_.name).toSet

    // mock-cluster-004 dropped, mock-cluster-new added.
    (refClusters -- curClusters) should contain ("mock-cluster-004")
    (curClusters -- refClusters) should contain ("mock-cluster-new")

    // Drift: mock-cluster-001 should have larger avgJobDurationMs in current vs reference.
    val refAvg001 = multi.perDate(refDate).clusters.find(_.name == "mock-cluster-001").get
      .recipes.head.avgJobDurationMs
    val curAvg001 = multi.perDate(curDate).clusters.find(_.name == "mock-cluster-001").get
      .recipes.head.avgJobDurationMs
    curAvg001 should be > refAvg001

    // mock-cluster-003 should be improved (smaller avgJobDurationMs in current).
    val refAvg003 = multi.perDate(refDate).clusters.find(_.name == "mock-cluster-003").get
      .recipes.head.avgJobDurationMs
    val curAvg003 = multi.perDate(curDate).clusters.find(_.name == "mock-cluster-003").get
      .recipes.head.avgJobDurationMs
    curAvg003 should be < refAvg003
  }

  test("multiDateBaseline current-date incarnations rebased into the current-date window") {
    val multi    = MockScenarios.multiDateBaseline(refDate, curDate)
    val curScn   = multi.perDate(curDate)
    val (s2, e2) = curScn.window
    curScn.clusters.flatMap(_.incarnations).foreach { inc =>
      inc.spanStart.isBefore(s2) shouldBe false
      inc.spanEnd.isAfter(e2)    shouldBe false
    }
  }

  // ── mixedDropAndDegrade ───────────────────────────────────────────────────

  test("mixedDropAndDegrade emits two dates with one cluster, mixing degraded + dropped recipes") {
    val multi = MockScenarios.mixedDropAndDegrade(refDate, curDate)
    multi.perDate.keySet shouldBe Set(refDate, curDate)

    val refCluster = multi.perDate(refDate).clusters
    val curCluster = multi.perDate(curDate).clusters
    refCluster.map(_.name) should contain only "mock-cluster-mixed"
    curCluster.map(_.name) should contain only "mock-cluster-mixed"

    val refRecipes = refCluster.head.recipes.map(_.name).toSet
    val curRecipes = curCluster.head.recipes.map(_.name).toSet

    // Stable recipe present on both dates; must-boost present on both; was-here only ref.
    refRecipes should contain ("mock-recipe-keep-stable.json")
    curRecipes should contain ("mock-recipe-keep-stable.json")
    refRecipes should contain ("mock-recipe-must-boost.json")
    curRecipes should contain ("mock-recipe-must-boost.json")
    (refRecipes -- curRecipes) shouldBe Set("mock-recipe-was-here.json")
  }

  test("mixedDropAndDegrade degrades must-boost durations from reference to current") {
    val multi = MockScenarios.mixedDropAndDegrade(refDate, curDate)
    val refMustBoost = multi.perDate(refDate).clusters.head.recipes
      .find(_.name == "mock-recipe-must-boost.json").get
    val curMustBoost = multi.perDate(curDate).clusters.head.recipes
      .find(_.name == "mock-recipe-must-boost.json").get
    curMustBoost.avgJobDurationMs should be > refMustBoost.avgJobDurationMs
    curMustBoost.p95JobDurationMs should be > refMustBoost.p95JobDurationMs
  }

  test("mixedDropAndDegrade incarnations sit inside their respective date windows") {
    val multi  = MockScenarios.mixedDropAndDegrade(refDate, curDate)
    val refScn = multi.perDate(refDate)
    val curScn = multi.perDate(curDate)
    val (s1, e1) = refScn.window
    val (s2, e2) = curScn.window
    refScn.clusters.flatMap(_.incarnations).foreach { inc =>
      inc.spanStart.isBefore(s1) shouldBe false
      inc.spanEnd.isAfter(e1)    shouldBe false
    }
    curScn.clusters.flatMap(_.incarnations).foreach { inc =>
      inc.spanStart.isBefore(s2) shouldBe false
      inc.spanEnd.isAfter(e2)    shouldBe false
    }
  }
}
