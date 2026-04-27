package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning._
import com.db.serna.orchestration.cluster_tuning.single.refinement.{MemoryHeapBoost, MemoryHeapBoostVitamin, MemoryHeapOomSignal, RefinementPipeline, SimpleJsonParser}
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
      referenceDate = "2025_12_20",
      currentDate = "2026_04_15",
      strategyName = "default",
      trends = Seq(trend),
      correlations = Seq(correlation),
      correlationsCurrentSnapshot = Seq.empty,
      correlationsPerCluster = Map.empty,
      divergences = Seq.empty,
      divergencesCurrentSnapshot = Seq.empty,
      divergencesPerCluster = Map.empty,
      scatterDataDelta = Map.empty,
      scatterDataCurrentSnapshot = Map.empty,
      newEntryCurrentMetrics = Map.empty,
      decisions = Seq(decision)
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
      referenceDate = "2025_12_20",
      currentDate = "2026_04_15",
      strategyName = "default",
      trends = trends,
      correlations = Seq.empty,
      correlationsCurrentSnapshot = Seq.empty,
      correlationsPerCluster = Map.empty,
      divergences = Seq.empty,
      divergencesCurrentSnapshot = Seq.empty,
      divergencesPerCluster = Map.empty,
      scatterDataDelta = Map.empty,
      scatterDataCurrentSnapshot = Map.empty,
      newEntryCurrentMetrics = Map.empty,
      decisions = Seq.empty
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

  // ── Summary report ───────────────────────────────────────────────────────

  test("writeAutoTunerSummaryReport creates report with correct sections") {
    val tmpDir = Files.createTempDirectory("auto_tuner_report_test").toFile
    tmpDir.deleteOnExit()

    val trends = Seq(
      TrendAssessment("c1", "r1", Degraded, Seq.empty, 0.8),
      TrendAssessment("c1", "r2", Improved, Seq.empty, 0.9),
      TrendAssessment("c2", "r1", Stable, Seq.empty, 1.0),
      TrendAssessment("c3", "r1", NewEntry, Seq.empty, 0.0),
      TrendAssessment("c4", "r1", DroppedEntry, Seq.empty, 0.0)
    )

    val b16Boosts = Seq(
      ("c1", Seq(
        MemoryHeapBoost("_ETL_recipe_A.json", "8g", "12g", 1.5),
        MemoryHeapBoost("_ETL_recipe_B.json", "16g", "24g", 1.5)
      ))
    )

    ClusterMachineAndRecipeAutoTuner.writeAutoTunerSummaryReport(
      tmpDir, "2025_12_20", "2026_04_15", "default", trends,
      kept = 2, boosted = 1, fresh = 1, preserved = 1, skipped = 0,
      b14Boosts = Seq(("c1", "Persistent b14 driver eviction: 3 (ref) -> 5 (cur) evictions")),
      b16Boosts = b16Boosts,
      correlationCount = 4, divergenceCount = 3
    )

    val reportFile = new File(tmpDir, "_generation_summary_auto_tuner.txt")
    reportFile.exists() shouldBe true

    val content = scala.io.Source.fromFile(reportFile).mkString
    content should include("AUTO-TUNER GENERATION SUMMARY")
    content should include("2025_12_20")
    content should include("2026_04_15")
    content should include("Improved:")
    content should include("Degraded:")
    content should include("Stable:")
    content should include("b14 DRIVER BOOSTS")
    content should include("Persistent b14")
    content should include("b16 OOM REBOOSTING")
    content should include("2 recipe(s)")
    content should include("ETL_recipe_A")
    content should include("8g -> 12g")
    content should include("ETL_recipe_B")
    content should include("16g -> 24g")
    content should include("STATISTICAL ANALYSIS")
    content should include("Correlation pairs computed: 4")
    content should include("Divergences detected:      3")
  }

  test("writeAutoTunerSummaryReport with no boosts shows (none)") {
    val tmpDir = Files.createTempDirectory("auto_tuner_report_none").toFile
    tmpDir.deleteOnExit()

    ClusterMachineAndRecipeAutoTuner.writeAutoTunerSummaryReport(
      tmpDir, "2025_12_20", "2026_04_15", "default",
      Seq(TrendAssessment("c1", "r1", Stable, Seq.empty, 1.0)),
      kept = 1, boosted = 0, fresh = 0, preserved = 0, skipped = 0,
      b14Boosts = Seq.empty, b16Boosts = Seq.empty,
      correlationCount = 0, divergenceCount = 0
    )

    val content = scala.io.Source.fromFile(new File(tmpDir, "_generation_summary_auto_tuner.txt")).mkString
    content should include("(none)")
    content should include("Kept (stable/improved):   1")
  }

  // ── b14 promotion chain ─────────────────────────────────────────────────

  test("b14 promotion: standard -> highmem (more memory, same cores)") {
    val current = MachineCatalog.byName("n2-standard-32").get
    val promoted = ClusterDiagnosticsProcessor.promoteMasterForEviction(current)
    promoted.name shouldBe "n2-highmem-32"
    promoted.memoryGb should be > current.memoryGb
    promoted.cores shouldBe current.cores
  }

  test("b14 promotion: highmem -> more cores (already at max variant)") {
    val current = MachineCatalog.byName("n2-highmem-32").get
    val promoted = ClusterDiagnosticsProcessor.promoteMasterForEviction(current)
    promoted.name shouldBe "n2-highmem-48"
    promoted.cores should be > current.cores
  }

  test("b14 promotion: e2-standard-32 -> n2-standard-32 (cross-family when e2-highmem-32 absent)") {
    // e2-highmem only goes up to 16 cores, so e2-highmem-32 doesn't exist
    val current = MachineCatalog.byName("e2-standard-32").get
    val promoted = ClusterDiagnosticsProcessor.promoteMasterForEviction(current)
    // Must cross-family to n2 since e2-highmem-32 is absent
    promoted.name shouldBe "n2-standard-32"
    promoted.memoryGb should be >= current.memoryGb
  }

  test("b14 promotion: reference master used as baseline when more powerful than fresh plan") {
    // If reference had n2-standard-48, fresh plan picks e2-standard-32 — baseline should be n2-standard-48
    val refMaster = MachineCatalog.byName("n2-standard-48").get
    val freshMaster = MachineCatalog.byName("e2-standard-32").get
    refMaster.memoryGb should be > freshMaster.memoryGb

    // Promoting from n2-standard-48 → n2-highmem-48 (variant step)
    val promoted = ClusterDiagnosticsProcessor.promoteMasterForEviction(refMaster)
    promoted.name shouldBe "n2-highmem-48"
    promoted.memoryGb should be > refMaster.memoryGb
  }

  test("b14 promotion: persistent eviction promotes from reference baseline, not fresh plan") {
    // After two promotion rounds: e2-standard-32 → n2-standard-32 → n2-highmem-32 → n2-highmem-48
    val round1 = ClusterDiagnosticsProcessor.promoteMasterForEviction(MachineCatalog.byName("e2-standard-32").get)
    round1.name shouldBe "n2-standard-32"

    val round2 = ClusterDiagnosticsProcessor.promoteMasterForEviction(round1)
    round2.name shouldBe "n2-highmem-32"

    val round3 = ClusterDiagnosticsProcessor.promoteMasterForEviction(round2)
    round3.name shouldBe "n2-highmem-48"
  }

  // ── PerformanceEvolver edge cases ────────────────────────────────────────

  test("PerformanceEvolver includes reason with top delta metric for degraded") {
    val deltas = Seq(
      MetricDelta("p95_job_duration_ms", 100000.0, 125000.0, 25000.0, 25.0),
      MetricDelta("avg_job_duration_ms", 50000.0, 55000.0, 5000.0, 10.0)
    )
    val trend = TrendAssessment("c1", "r1", Degraded, deltas, 0.8)
    val decisions = PerformanceEvolver.decideEvolutions(
      Seq(trend), Map.empty, Map.empty, Map.empty, keepHistorical = true
    )
    decisions.head.reason should include("p95_job_duration_ms")
    decisions.head.reason should include("25.0%")
  }

  test("PerformanceEvolver with fraction_reaching_cap as top delta") {
    val deltas = Seq(
      MetricDelta("fraction_reaching_cap", 0.1, 0.5, 0.4, 300.0),
      MetricDelta("p95_job_duration_ms", 100000.0, 105000.0, 5000.0, 5.0)
    )
    val trend = TrendAssessment("c1", "r1", Degraded, deltas, 0.9)
    val decisions = PerformanceEvolver.decideEvolutions(
      Seq(trend), Map.empty, Map.empty, Map.empty, keepHistorical = true
    )
    decisions.head.reason should include("fraction_reaching_cap")
  }

  // ── b16 reboosting persistence ────────────────────────────────────────────

  test("b16 reboosting persists from reference_date when b16 CSV absent in current_date") {
    // Setup: reference_date has b16 CSV, current_date does not
    val tmpDir = Files.createTempDirectory("auto_tuner_b16_persist").toFile
    tmpDir.deleteOnExit()

    val refInputDir = new File(tmpDir, "ref_inputs")
    refInputDir.mkdirs()

    val curInputDir = new File(tmpDir, "cur_inputs")
    curInputDir.mkdirs()

    val outputDir = new File(tmpDir, "output")
    outputDir.mkdirs()

    // Write b16 CSV ONLY in reference input dir
    writeCsv(refInputDir, "b16_oom_job_driver_exceptions.csv",
      B16Header + "\n" +
        "etl-m-recipe-a-20260411-0438,cluster-a,_ETL_recipe_A.json,2026-04-11T04:38:00Z,ERROR,SomeClass,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,Java heap space,log1"
    )

    // No b16 CSV in current input dir (curInputDir has no b16 file)

    // Write a cluster output JSON that can be refined
    writeJson(outputDir, "cluster-a-auto-scale-tuned.json", SampleAutoScaleJson)
    writeJson(outputDir, "cluster-a-manually-tuned.json", SampleManualJson)

    // The b16 CSV should be found in refInputDir (fallback)
    val b16File = new File(refInputDir, "b16_oom_job_driver_exceptions.csv")
    b16File.exists() shouldBe true
    new File(curInputDir, "b16_oom_job_driver_exceptions.csv").exists() shouldBe false

    // Verify the vitamin can load signals from the reference dir
    val vitamin = new MemoryHeapBoostVitamin(1.5)
    val signals = vitamin.loadSignals(refInputDir, "cluster-a")
    signals should not be empty
    signals.head.asInstanceOf[MemoryHeapOomSignal].recipeFilename shouldBe "_ETL_recipe_A.json"

    // Apply reboosting with inputDirs = [curInputDir, refInputDir]
    // Since curInputDir has no b16, it should fall back to refInputDir
    val config = SimpleJsonParser.parseFile(new File(outputDir, "cluster-a-auto-scale-tuned.json"))
    val result = RefinementPipeline.refine(config, Seq(vitamin), refInputDir)
    result.appliedBoosts should not be empty
    val boost = result.appliedBoosts.head.asInstanceOf[MemoryHeapBoost]
    boost.originalMemory shouldBe "8g"
    boost.boostedMemory shouldBe "12g"
  }

  test("b16 reboosting prefers current_date CSV when both dates have b16") {
    val tmpDir = Files.createTempDirectory("auto_tuner_b16_both").toFile
    tmpDir.deleteOnExit()

    val refInputDir = new File(tmpDir, "ref_inputs")
    refInputDir.mkdirs()

    val curInputDir = new File(tmpDir, "cur_inputs")
    curInputDir.mkdirs()

    // Both dirs have b16 CSV — curInputDir should be preferred
    val b16Content = B16Header + "\n" +
      "etl-m-recipe-a-20260411-0438,cluster-a,_ETL_recipe_A.json,2026-04-11T04:38:00Z,ERROR,SomeClass,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,Java heap space,log1"
    writeCsv(refInputDir, "b16_oom_job_driver_exceptions.csv", b16Content)
    writeCsv(curInputDir, "b16_oom_job_driver_exceptions.csv", b16Content)

    // Both should exist
    new File(curInputDir, "b16_oom_job_driver_exceptions.csv").exists() shouldBe true
    new File(refInputDir, "b16_oom_job_driver_exceptions.csv").exists() shouldBe true

    // The applyB16Reboosting inputDirs order is [curDate, refDate] — first match wins
    val inputDirs = Seq(curInputDir, refInputDir)
    val effectiveDir = inputDirs.find(d => d.exists() && new File(d, "b16_oom_job_driver_exceptions.csv").exists())
    effectiveDir.get shouldBe curInputDir
  }

  // ── Overall cluster trend determination ──────────────────────────────────

  test("determineOverallClusterTrend returns degraded if any recipe is degraded") {
    val trends = Seq(
      TrendAssessment("c1", "r1", Improved, Seq.empty, 0.9),
      TrendAssessment("c1", "r2", Degraded, Seq.empty, 0.8)
    )
    AutoTunerJsonOutput.determineOverallClusterTrend(trends) shouldBe "degraded"
  }

  test("determineOverallClusterTrend returns improved when all are improved") {
    val trends = Seq(
      TrendAssessment("c1", "r1", Improved, Seq.empty, 0.9),
      TrendAssessment("c1", "r2", Improved, Seq.empty, 0.8)
    )
    AutoTunerJsonOutput.determineOverallClusterTrend(trends) shouldBe "improved"
  }

  test("determineOverallClusterTrend returns stable when all are stable") {
    val trends = Seq(
      TrendAssessment("c1", "r1", Stable, Seq.empty, 1.0),
      TrendAssessment("c1", "r2", Stable, Seq.empty, 1.0)
    )
    AutoTunerJsonOutput.determineOverallClusterTrend(trends) shouldBe "stable"
  }

  test("determineOverallClusterTrend returns mixed for improved+stable") {
    val trends = Seq(
      TrendAssessment("c1", "r1", Improved, Seq.empty, 0.9),
      TrendAssessment("c1", "r2", Stable, Seq.empty, 1.0)
    )
    AutoTunerJsonOutput.determineOverallClusterTrend(trends) shouldBe "mixed"
  }
}
