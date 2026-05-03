package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import com.db.serna.orchestration.cluster_tuning.single.{ClusterDiagnosticsProcessor, Csv}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

/**
 * Round-trip tests: each generator writes a CSV that the existing tuner loaders
 * (or `Csv.parse`) read back without error and produce the expected counts.
 * These tests do NOT invoke the tuner end-to-end; they assert the schema-level
 * contract between generator and loader.
 */
class MockGenSpec extends AnyFunSuite with Matchers {

  private val testDate = "2099_01_01"
  private val baseline = MockScenarios.baseline(testDate)
  private val oom      = MockScenarios.oomHeavy(testDate)
  private val auto     = MockScenarios.autoscaling(testDate)

  private def writeScenario(scenario: MockScenario): File = {
    val dir = Files.createTempDirectory("oss-mock-test-").toFile
    MockGen.writeAll(scenario, dir)
    dir
  }

  // ── b13 ────────────────────────────────────────────────────────────────────

  test("b13 round-trips through Csv.parse with one row per (cluster, recipe)") {
    val dir = writeScenario(baseline)
    val rows = Csv.parse(new File(dir, "b13_recommendations_inputs_per_recipe_per_cluster.csv"))
    val expected = baseline.clusters.map(_.recipes.size).sum
    rows.size shouldBe expected
    rows.head.keys should contain allOf (
      "cluster_name", "recipe_filename", "avg_executors_per_job",
      "p95_run_max_executors", "avg_job_duration_ms", "runs", "max_concurrent_jobs"
    )
    val pairs = rows.map(r => (r("cluster_name"), r("recipe_filename"))).toSet
    val expectedPairs = (for (c <- baseline.clusters; r <- c.recipes) yield (c.name, r.name)).toSet
    pairs shouldBe expectedPairs
  }

  // ── individual b1..b12 ────────────────────────────────────────────────────

  test("individual per-recipe CSVs (b1, b3, b4, b8, b9, b10, b12) have one row per (cluster, recipe)") {
    val dir = writeScenario(baseline)
    val expected = baseline.clusters.map(_.recipes.size).sum
    val perRecipe = Seq(
      "b1_average_number_of_executors_per_job_by_cluster.csv",
      "b3_average_recipefilename_per_cluster.csv",
      "b4_peak_job_duration_per_cluster.csv",
      "b8_P95_job_duration_per_recipe_per_cluster.csv",
      "b9_time_at_cap_per_run_and_per_cluster.csv",
      "b10_executor_churn_per_job_adds_removes.csv",
      "b12_p95_max_executors_per_recipe_per_cluster.csv",
      "b5_a_times_job_reaches_max_executor_per_cluster.csv"
    )
    perRecipe.foreach { name =>
      val rows = Csv.parse(new File(dir, name))
      withClue(s"$name: ") { rows.size shouldBe expected }
    }
  }

  test("individual per-cluster CSVs (b2, b6, b7, b11) have one row per cluster") {
    val dir = writeScenario(baseline)
    val expected = baseline.clusters.size
    Seq("b2_peak_executors_seen.csv", "b6_total_jobs_per_cluster.csv",
        "b7_total_runtime_all_jobs_per_cluster.csv", "b11_max_concurrent_jobs_per_cluster_in_window.csv"
    ).foreach { name =>
      val rows = Csv.parse(new File(dir, name))
      withClue(s"$name: ") { rows.size shouldBe expected }
    }
  }

  // ── b14 ────────────────────────────────────────────────────────────────────

  test("b14 round-trips through ClusterDiagnosticsProcessor.loadExitCodes with cluster_name unquoted") {
    val dir = writeScenario(oom)
    val records = ClusterDiagnosticsProcessor.loadExitCodes(new File(dir, "b14_clusters_with_nonzero_exit_codes.csv"))
    val expected = oom.clusters.map(_.driverExitCodes.size).sum
    records.size shouldBe expected
    // The loader strips all double-quotes; cluster names should match exactly.
    val recordedClusters = records.map(_.clusterName).toSet
    val expectedClusters = oom.clusters.filter(_.driverExitCodes.nonEmpty).map(_.name).toSet
    recordedClusters shouldBe expectedClusters
  }

  // ── b20 ────────────────────────────────────────────────────────────────────

  test("b20 round-trips through loadClusterSpans with one entry per cluster (multi-incarnation preserved)") {
    val dir = writeScenario(baseline)
    // loadClusterSpans takes a Config — we use Csv.parse directly to avoid plumbing a Config here.
    val rows = Csv.parse(new File(dir, "b20_cluster_span_time.csv"))
    val expected = baseline.clusters.map(_.incarnations.size).sum
    rows.size shouldBe expected
    rows.head.keys should contain allOf (
      "cluster_name", "incarnation_idx", "span_start_ts", "span_end_ts",
      "span_minutes", "has_explicit_create", "has_explicit_delete"
    )
    // mock-cluster-004 has two incarnations in the baseline scenario.
    val cluster004Rows = rows.filter(_("cluster_name") == "mock-cluster-004")
    cluster004Rows.size shouldBe 2
    cluster004Rows.map(_("incarnation_idx")).toSet shouldBe Set("1", "2")
  }

  // ── b21 ────────────────────────────────────────────────────────────────────

  test("b21 only emits RECOMMENDING events with numeric target_primary_workers") {
    val dir = writeScenario(auto)
    val rows = Csv.parse(new File(dir, "b21_cluster_autoscaler_values.csv"))
    rows should not be empty
    all (rows.map(_("state")))               shouldBe "RECOMMENDING"
    all (rows.map(_("target_primary_workers"))) should not be empty
    rows.foreach { r =>
      r("decision") should (be("SCALE_UP") or be("SCALE_DOWN") or be("NO_SCALE"))
    }
  }

  test("b21 events fall strictly inside their cluster's b20 spans") {
    val dir = writeScenario(auto)
    val b20 = Csv.parse(new File(dir, "b20_cluster_span_time.csv"))
    val b21 = Csv.parse(new File(dir, "b21_cluster_autoscaler_values.csv"))
    val spansByCluster: Map[String, Seq[(String, String)]] = b20.groupBy(_("cluster_name"))
      .map { case (k, vs) => k -> vs.map(r => (r("span_start_ts"), r("span_end_ts"))).toSeq }
    b21.foreach { r =>
      val cluster = r("cluster_name")
      val ts      = r("event_ts")
      val spans = spansByCluster.getOrElse(cluster, Seq.empty)
      val inside = spans.exists { case (s, e) => ts >= s && ts <= e }   // ASCII string ordering matches ISO ts ordering
      withClue(s"event ts=$ts for cluster=$cluster outside its spans=$spans: ") { inside shouldBe true }
    }
  }

  // ── syntheticSpan scenario (excludeFromB20) ───────────────────────────────

  test("syntheticSpan: incarnations flagged excludeFromB20 are omitted from b20 but b21 still emits their events") {
    val ss = MockScenarios.syntheticSpan(testDate)
    val dir = writeScenario(ss)
    val b20 = Csv.parse(new File(dir, "b20_cluster_span_time.csv"))
    val b21 = Csv.parse(new File(dir, "b21_cluster_autoscaler_values.csv"))

    val b20ClustersExpected = ss.clusters.filter(_.incarnations.exists(!_.excludeFromB20)).map(_.name).toSet
    val b21ClustersExpected = ss.clusters.filter(_.incarnations.exists(_.autoscaler.isDefined)).map(_.name).toSet
    val excludedClusters    = ss.clusters
      .filter(_.incarnations.forall(_.excludeFromB20))
      .map(_.name).toSet

    excludedClusters should not be empty
    b20.map(_("cluster_name")).toSet shouldBe b20ClustersExpected
    b21.map(_("cluster_name")).toSet shouldBe b21ClustersExpected
    // Excluded clusters appear in b21 but NOT in b20 — the synthetic-span path.
    excludedClusters.foreach { c =>
      withClue(s"$c should be missing from b20: ") { b20.exists(_("cluster_name") == c) shouldBe false }
      withClue(s"$c should appear in b21:      ") { b21.exists(_("cluster_name") == c) shouldBe true  }
    }
  }
}
