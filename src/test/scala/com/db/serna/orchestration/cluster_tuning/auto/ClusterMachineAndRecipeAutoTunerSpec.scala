package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning._
import com.db.serna.orchestration.cluster_tuning.refinement.SimpleJsonParser
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import java.nio.file.Files

class ClusterMachineAndRecipeAutoTunerSpec extends AnyFunSuite with Matchers {

  // ── Helpers ────────────────────────────────────────────────────────────────

  private def writeCsv(dir: File, name: String, content: String): Unit = {
    val pw = new PrintWriter(new File(dir, name))
    try pw.write(content) finally pw.close()
  }

  private def writeJson(dir: File, name: String, content: String): Unit = {
    val pw = new PrintWriter(new File(dir, name))
    try pw.write(content) finally pw.close()
  }

  private val B13Header =
    "cluster_name,recipe_filename,avg_executors_per_job,p95_run_max_executors,avg_job_duration_ms,p95_job_duration_ms,runs,seconds_at_cap,runs_reaching_cap,total_runs,fraction_reaching_cap,max_concurrent_jobs"

  private val B14Header =
    "timestamp,job_id,cluster_name,driver_exit_code,msg"

  private val B16Header =
    "job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name"

  private def mkMetrics(cluster: String, recipe: String,
                        avgExec: Double = 2.0, p95Exec: Double = 4.0,
                        avgDur: Double = 50000.0, p95Dur: Double = 100000.0,
                        runs: Long = 20, fracCap: Option[Double] = None): RecipeMetrics =
    RecipeMetrics(cluster, recipe, avgExec, p95Exec, avgDur, p95Dur, runs,
      None, None, None, fracCap, None)

  private val SampleManualJson: String =
    """{
      |  "clusterConf": {
      |    "cluster-a": {
      |      "num_workers": 4,
      |      "master_machine_type": "e2-standard-8",
      |      "worker_machine_type": "n2-standard-32",
      |      "autoscaling_policy": "small-workload-autoscaling",
      |      "tuner_version": "2025_20_12",
      |      "total_no_of_jobs": 2,
      |      "cluster_max_total_memory_gb": 512,
      |      "cluster_max_total_cores": 128
      |    }
      |  },
      |  "recipeSparkConf": {
      |    "_ETL_recipe_A.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.executor.instances": "4",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 32,
      |      "total_executor_maximum_allocated_memory_gb": 32
      |    },
      |    "_ETL_recipe_B.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.executor.instances": "2",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 16
      |    }
      |  }
      |}""".stripMargin

  private val SampleAutoScaleJson: String =
    """{
      |  "clusterConf": {
      |    "cluster-a": {
      |      "num_workers": 4,
      |      "master_machine_type": "e2-standard-8",
      |      "worker_machine_type": "n2-standard-32",
      |      "autoscaling_policy": "small-workload-autoscaling",
      |      "tuner_version": "2025_20_12",
      |      "total_no_of_jobs": 2,
      |      "cluster_max_total_memory_gb": 512,
      |      "cluster_max_total_cores": 128
      |    }
      |  },
      |  "recipeSparkConf": {
      |    "_ETL_recipe_A.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "5",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 40
      |    },
      |    "_ETL_recipe_B.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "3",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 24
      |    }
      |  }
      |}""".stripMargin

  // ── TrendDetector integration ─────────────────────────────────────────────

  test("degraded recipe produces BoostResources decision") {
    val ref = mkMetrics("c1", "r1", p95Dur = 100000.0)
    val cur = mkMetrics("c1", "r1", p95Dur = 125000.0) // 25% increase
    val pair = MetricsPair("c1", "r1", ref, cur)

    val trend = TrendDetector.assessTrend(pair)
    trend.trend shouldBe Degraded

    val decisions = PerformanceEvolver.decideEvolutions(
      Seq(trend), Map.empty, Map(("c1", "r1") -> cur), Map(("c1", "r1") -> ref), keepHistorical = true
    )
    decisions should have size 1
    decisions.head.action shouldBe BoostResources
  }

  test("improved recipe produces KeepAsIs decision") {
    val ref = mkMetrics("c1", "r1", p95Dur = 100000.0)
    val cur = mkMetrics("c1", "r1", p95Dur = 90000.0) // 10% decrease
    val pair = MetricsPair("c1", "r1", ref, cur)

    val trend = TrendDetector.assessTrend(pair)
    trend.trend shouldBe Improved

    val decisions = PerformanceEvolver.decideEvolutions(
      Seq(trend), Map.empty, Map(("c1", "r1") -> cur), Map(("c1", "r1") -> ref), keepHistorical = true
    )
    decisions should have size 1
    decisions.head.action shouldBe KeepAsIs
  }

  test("stable recipe produces KeepAsIs decision") {
    val ref = mkMetrics("c1", "r1", p95Dur = 100000.0)
    val cur = mkMetrics("c1", "r1", p95Dur = 103000.0) // 3% — within noise
    val pair = MetricsPair("c1", "r1", ref, cur)

    val trend = TrendDetector.assessTrend(pair)
    trend.trend shouldBe Stable

    val decisions = PerformanceEvolver.decideEvolutions(
      Seq(trend), Map.empty, Map(("c1", "r1") -> cur), Map(("c1", "r1") -> ref), keepHistorical = true
    )
    decisions.head.action shouldBe KeepAsIs
  }

  // ── PerformanceEvolver ────────────────────────────────────────────────────

  test("new entry produces GenerateFresh decision") {
    val trend = TrendAssessment("c1", "r1", NewEntry, Seq.empty, 0.0)
    val decisions = PerformanceEvolver.decideEvolutions(
      Seq(trend), Map.empty, Map.empty, Map.empty, keepHistorical = true
    )
    decisions.head.action shouldBe GenerateFresh
  }

  test("dropped entry with keepHistorical=true produces PreserveHistorical") {
    val trend = TrendAssessment("c1", "r1", DroppedEntry, Seq.empty, 0.0)
    val decisions = PerformanceEvolver.decideEvolutions(
      Seq(trend), Map.empty, Map.empty, Map.empty, keepHistorical = true
    )
    decisions.head.action shouldBe PreserveHistorical
  }

  test("dropped entry with keepHistorical=false does not produce PreserveHistorical") {
    val trend = TrendAssessment("c1", "r1", DroppedEntry, Seq.empty, 0.0)
    val decisions = PerformanceEvolver.decideEvolutions(
      Seq(trend), Map.empty, Map.empty, Map.empty, keepHistorical = false
    )
    decisions.head.action shouldBe KeepAsIs
    decisions.head.reason should include("keep-historical=false")
  }

  // ── Analysis JSON output ──────────────────────────────────────────────────

  test("analysis JSON contains expected top-level keys") {
    val trend = TrendAssessment("c1", "r1", Degraded,
      Seq(MetricDelta("p95_job_duration_ms", 100000.0, 125000.0, 25000.0, 25.0)), 0.8)
    val decision = EvolutionDecision("c1", "r1", BoostResources, "degraded", trend)
    val correlation = CorrelationResult("delta_p95_run_max_executors", "delta_p95_job_duration_ms", 1234.5, 0.72, 10)

    val json = AutoTunerJsonOutput.analysisOutputJson(
      "2025_12_20", "2026_04_15", "default",
      Seq(trend), Seq(correlation), Seq.empty, Seq(decision)
    )

    json should include("\"metadata\"")
    json should include("\"trends_summary\"")
    json should include("\"cluster_trends\"")
    json should include("\"correlations\"")
    json should include("\"divergences\"")
    json should include("\"reference_date\"")
    json should include("2025_12_20")
    json should include("2026_04_15")
    json should include("\"degraded\"")
  }

  test("analysis JSON trends_summary counts are correct") {
    val trends = Seq(
      TrendAssessment("c1", "r1", Degraded, Seq.empty, 0.8),
      TrendAssessment("c1", "r2", Improved, Seq.empty, 0.9),
      TrendAssessment("c2", "r1", Stable, Seq.empty, 1.0),
      TrendAssessment("c3", "r1", NewEntry, Seq.empty, 0.0)
    )

    val json = AutoTunerJsonOutput.analysisOutputJson(
      "2025_12_20", "2026_04_15", "default", trends, Seq.empty, Seq.empty, Seq.empty
    )

    json should include("\"degraded\": 1")
    json should include("\"improved\": 1")
    json should include("\"stable\": 1")
    json should include("\"new_entries\": 1")
  }

  // ── Analysis CSV output ───────────────────────────────────────────────────

  test("writeAnalysisCsvs creates expected files") {
    val tmpDir = Files.createTempDirectory("auto_tuner_csv_test").toFile
    tmpDir.deleteOnExit()

    val trend = TrendAssessment("c1", "r1", Degraded,
      Seq(MetricDelta("p95_job_duration_ms", 100000.0, 125000.0, 25000.0, 25.0)), 0.8)
    val decision = EvolutionDecision("c1", "r1", BoostResources, "reason", trend)
    val correlation = CorrelationResult("a", "b", 1.0, 0.5, 10)
    val divergence = DivergenceResult("c1", "r1", "metric", 100.0, 200.0, 3.0, true)

    AutoTunerJsonOutput.writeAnalysisCsvs(tmpDir, Seq(trend), Seq(correlation), Seq(divergence), Seq(decision))

    new File(tmpDir, "_trend_summary.csv").exists() shouldBe true
    new File(tmpDir, "_correlations.csv").exists() shouldBe true
    new File(tmpDir, "_divergences.csv").exists() shouldBe true

    val trendCsv = scala.io.Source.fromFile(new File(tmpDir, "_trend_summary.csv")).mkString
    trendCsv should include("cluster,recipe,trend")
    trendCsv should include("c1,r1,degraded")

    val corrCsv = scala.io.Source.fromFile(new File(tmpDir, "_correlations.csv")).mkString
    corrCsv should include("metric_a,metric_b,covariance,pearson_correlation,sample_size")

    val divCsv = scala.io.Source.fromFile(new File(tmpDir, "_divergences.csv")).mkString
    divCsv should include("cluster,recipe,metric")
    divCsv should include("c1,r1,metric")
  }

  // ── Snapshot loading ──────────────────────────────────────────────────────

  test("loadSnapshot handles missing input directory gracefully") {
    val snapshot = ClusterMachineAndRecipeAutoTuner.loadSnapshot("9999_99_99")
    snapshot.metrics shouldBe empty
    snapshot.date shouldBe "9999_99_99"
  }

  // ── End-to-end pairing ────────────────────────────────────────────────────

  test("mixed scenario: improved + degraded + new + dropped produces correct decisions") {
    val refMetrics: Map[(String, String), RecipeMetrics] = Map(
      ("c1", "r1") -> mkMetrics("c1", "r1", p95Dur = 100000.0),
      ("c1", "r2") -> mkMetrics("c1", "r2", p95Dur = 100000.0),
      ("c2", "r1") -> mkMetrics("c2", "r1", p95Dur = 100000.0)
    )
    val curMetrics: Map[(String, String), RecipeMetrics] = Map(
      ("c1", "r1") -> mkMetrics("c1", "r1", p95Dur = 90000.0),   // improved
      ("c1", "r2") -> mkMetrics("c1", "r2", p95Dur = 125000.0),  // degraded
      ("c3", "r1") -> mkMetrics("c3", "r1", p95Dur = 50000.0)    // new
      // c2/r1 is dropped
    )

    val refKeys = refMetrics.keySet
    val curKeys = curMetrics.keySet
    val commonKeys = refKeys.intersect(curKeys)
    val newKeys = curKeys.diff(refKeys)
    val droppedKeys = refKeys.diff(curKeys)

    val pairs = commonKeys.toSeq.map { case (c, r) =>
      MetricsPair(c, r, refMetrics((c, r)), curMetrics((c, r)))
    }
    val pairedTrends = pairs.map(TrendDetector.assessTrend)
    val newTrends = newKeys.toSeq.map { case (c, r) => TrendAssessment(c, r, NewEntry, Seq.empty, 0.0) }
    val droppedTrends = droppedKeys.toSeq.map { case (c, r) => TrendAssessment(c, r, DroppedEntry, Seq.empty, 0.0) }
    val allTrends = pairedTrends ++ newTrends ++ droppedTrends

    val decisions = PerformanceEvolver.decideEvolutions(
      allTrends, Map.empty, curMetrics, refMetrics, keepHistorical = true
    )

    decisions should have size 4
    decisions.find(d => d.cluster == "c1" && d.recipe == "r1").get.action shouldBe KeepAsIs
    decisions.find(d => d.cluster == "c1" && d.recipe == "r2").get.action shouldBe BoostResources
    decisions.find(d => d.cluster == "c3" && d.recipe == "r1").get.action shouldBe GenerateFresh
    decisions.find(d => d.cluster == "c2" && d.recipe == "r1").get.action shouldBe PreserveHistorical
  }
}
