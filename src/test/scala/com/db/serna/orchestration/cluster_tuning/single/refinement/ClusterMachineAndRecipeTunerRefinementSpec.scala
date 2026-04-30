package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.refinement.{ClusterMachineAndRecipeTunerRefinement, MemoryHeapBoostVitamin, RefinementPipeline, SimpleJsonParser, UnresolvedEntry}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import java.nio.file.Files

class ClusterMachineAndRecipeTunerRefinementSpec extends AnyFunSuite with Matchers {

  private val sampleAutoScaleJson: String =
    """{
      |  "clusterConf": {
      |    "cluster-wf-dmr-load-t-02-15-0215": {
      |      "num_workers": 10,
      |      "master_machine_type": "n2d-highcpu-48",
      |      "worker_machine_type": "n2d-highcpu-48",
      |      "autoscaling_policy": "extra-large-workload-autoscaling",
      |      "tuner_version": "2025_20_12",
      |      "total_no_of_jobs": 3,
      |      "cluster_max_total_memory_gb": 960,
      |      "cluster_max_total_cores": 480,
      |      "accumulated_max_total_memory_per_jobs_gb": 1344
      |    }
      |  },
      |  "recipeSparkConf": {
      |    "_ETL_m_DQ3_ODS_TGL05.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.closure.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "3",
      |        "spark.dynamicAllocation.initialExecutors": "2",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 24
      |    },
      |    "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.closure.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "5",
      |        "spark.dynamicAllocation.initialExecutors": "2",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 40
      |    },
      |    "_ETL_m_DQ3_ODS_F_AF_OPE_MERCADO.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.closure.serializer": "org.apache.spark.serializer.KryoSerializer",
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "4",
      |        "spark.dynamicAllocation.initialExecutors": "2",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 32
      |    }
      |  }
      |}""".stripMargin

  // ── RefinementPipeline: refine ────────────────────────────────────────────

  test("refine: applies heap boost to OOM-affected recipe and counts correctly") {
    val tmpDir = Files.createTempDirectory("refine_test").toFile
    tmpDir.deleteOnExit()

    // Write b16 CSV with one OOM for _ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json
    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("job-1,cluster-wf-dmr-load-t-02-15-0215,_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json,2026-04-12T13:03:51Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,Java heap space,driver")
    } finally pw.close()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val vitamins = Seq(new MemoryHeapBoostVitamin(1.5))
    val result = RefinementPipeline.refine(config, vitamins, tmpDir)

    // Only PROPUESTAS should be boosted
    result.appliedBoosts should have size 1
    result.boostCounters("boostedMemoryHeapJobCount") shouldBe 1
    result.boostLists("boostedMemoryHeapJobList") shouldBe Seq("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json")

    // Boosted recipe
    val boosted = result.refinedRecipes("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json")
    boosted.sparkOptsMap("spark.executor.memory") shouldBe "12g"
    boosted.extraFields("appliedMemoryHeapBoostFactor") shouldBe "1.5"
    boosted.totalExecutorMinAllocatedMemoryGb shouldBe 24   // 2 x 12
    boosted.totalExecutorMaxAllocatedMemoryGb shouldBe 60   // 5 x 12

    // Unaffected recipe
    val untouched = result.refinedRecipes("_ETL_m_DQ3_ODS_TGL05.json")
    untouched.sparkOptsMap("spark.executor.memory") shouldBe "8g"
    untouched.extraFields shouldBe empty
  }

  test("refine: no boosts when no OOM signals for cluster") {
    val tmpDir = Files.createTempDirectory("refine_nooom").toFile
    tmpDir.deleteOnExit()

    // Write b16 CSV with OOM for a different cluster
    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("job-1,other-cluster,_ETL_recipe_X.json,2026-04-12T13:00:00Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
    } finally pw.close()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val vitamins = Seq(new MemoryHeapBoostVitamin(1.5))
    val result = RefinementPipeline.refine(config, vitamins, tmpDir)

    result.appliedBoosts shouldBe empty
    result.boostCounters("boostedMemoryHeapJobCount") shouldBe 0
    result.boostLists("boostedMemoryHeapJobList") shouldBe empty
  }

  test("refine: multiple OOM recipes get boosted, counter and list reflect count") {
    val tmpDir = Files.createTempDirectory("refine_multi").toFile
    tmpDir.deleteOnExit()

    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("job-1,cluster-wf-dmr-load-t-02-15-0215,_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json,2026-04-12T13:00:00Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
      pw.println("job-2,cluster-wf-dmr-load-t-02-15-0215,_ETL_m_DQ3_ODS_F_AF_OPE_MERCADO.json,2026-04-12T14:00:00Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
    } finally pw.close()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val vitamins = Seq(new MemoryHeapBoostVitamin(2.0))
    val result = RefinementPipeline.refine(config, vitamins, tmpDir)

    result.appliedBoosts should have size 2
    result.boostCounters("boostedMemoryHeapJobCount") shouldBe 2
    result.boostLists("boostedMemoryHeapJobList") should contain allOf(
      "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json",
      "_ETL_m_DQ3_ODS_F_AF_OPE_MERCADO.json"
    )

    result.refinedRecipes("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json").sparkOptsMap("spark.executor.memory") shouldBe "16g"
    result.refinedRecipes("_ETL_m_DQ3_ODS_F_AF_OPE_MERCADO.json").sparkOptsMap("spark.executor.memory") shouldBe "16g"
    result.refinedRecipes("_ETL_m_DQ3_ODS_TGL05.json").sparkOptsMap("spark.executor.memory") shouldBe "8g"
  }

  test("refine: no vitamins returns original config with empty counters") {
    val tmpDir = Files.createTempDirectory("refine_novitamins").toFile
    tmpDir.deleteOnExit()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val result = RefinementPipeline.refine(config, Seq.empty, tmpDir)

    result.appliedBoosts shouldBe empty
    result.boostCounters shouldBe empty
    result.boostLists shouldBe empty
    result.unresolvedEntries shouldBe empty
    result.refinedRecipes shouldBe config.recipes
  }

  // ── RefinementPipeline: recipe resolution ─────────────────────────────────

  test("refine: resolves empty recipe_filename via job-id derivation and boosts") {
    val tmpDir = Files.createTempDirectory("refine_resolve_jobid").toFile
    tmpDir.deleteOnExit()

    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("etl-m-dq3-ods-f-pm-propuestas-20260409-0356,cluster-wf-dmr-load-t-02-15-0215,,2026-04-09T03:56:00Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
    } finally pw.close()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val vitamins = Seq(new MemoryHeapBoostVitamin(1.5))
    val result = RefinementPipeline.refine(config, vitamins, tmpDir)

    result.appliedBoosts should have size 1
    result.appliedBoosts.head.recipeFilename shouldBe "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json"
    result.refinedRecipes("_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json").sparkOptsMap("spark.executor.memory") shouldBe "12g"
    result.unresolvedEntries shouldBe empty
  }

  test("refine: resolves empty recipe_filename via sibling lookup") {
    val tmpDir = Files.createTempDirectory("refine_resolve_sibling").toFile
    tmpDir.deleteOnExit()

    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      // Same job prefix, first row has recipe, second doesn't
      pw.println("etl-m-dq3-ods-f-af-ope-mercado-20260409-0015,cluster-wf-dmr-load-t-02-15-0215,_ETL_m_DQ3_ODS_F_AF_OPE_MERCADO.json,2026-04-09T00:15:00Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
      pw.println("etl-m-dq3-ods-f-af-ope-mercado-20260410-0015,cluster-wf-dmr-load-t-02-15-0215,,2026-04-10T00:15:00Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
    } finally pw.close()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val vitamins = Seq(new MemoryHeapBoostVitamin(1.5))
    val result = RefinementPipeline.refine(config, vitamins, tmpDir)

    result.appliedBoosts should have size 1
    result.appliedBoosts.head.recipeFilename shouldBe "_ETL_m_DQ3_ODS_F_AF_OPE_MERCADO.json"
    result.unresolvedEntries shouldBe empty
  }

  test("refine: unresolvable signals produce unresolvedEntries") {
    val tmpDir = Files.createTempDirectory("refine_unresolved").toFile
    tmpDir.deleteOnExit()

    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("totally-unknown-job-20260411-0438,cluster-wf-dmr-load-t-02-15-0215,,2026-04-11T02:59:10Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
    } finally pw.close()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val vitamins = Seq(new MemoryHeapBoostVitamin(1.5))
    val result = RefinementPipeline.refine(config, vitamins, tmpDir)

    result.appliedBoosts shouldBe empty
    result.unresolvedEntries should have size 1
    result.unresolvedEntries.head.jobId shouldBe "totally-unknown-job-20260411-0438"
    result.unresolvedEntries.head.vitaminName shouldBe "b16_memory_heap_boost"
    result.unresolvedEntries.head.csvSource shouldBe "b16_oom_job_driver_exceptions.csv"
  }

  test("refine: reports resolved signals whose recipe is not in the config") {
    val tmpDir = Files.createTempDirectory("refine_not_in_config").toFile
    tmpDir.deleteOnExit()

    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("etl-kcop,cluster-wf-dmr-load-t-02-15-0215,EL_KCOP.json,2026-03-16T06:46:38Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
    } finally pw.close()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val vitamins = Seq(new MemoryHeapBoostVitamin(1.5))
    val result = RefinementPipeline.refine(config, vitamins, tmpDir)

    result.appliedBoosts shouldBe empty
    result.unresolvedEntries should have size 1
    result.unresolvedEntries.head.jobId shouldBe "etl-kcop"
    result.unresolvedEntries.head.rawRecipeFilename shouldBe "EL_KCOP.json"
  }

  // ── Re-run idempotency ───────────────────────────────────────────────────

  test("toRefinedJson: no duplicate boost counters on re-run") {
    val tmpDir = Files.createTempDirectory("refine_rerun").toFile
    tmpDir.deleteOnExit()

    // First run with a boost
    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("job-1,cluster-wf-dmr-load-t-02-15-0215,_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json,2026-04-12T13:03:51Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
    } finally pw.close()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val vitamins = Seq(new MemoryHeapBoostVitamin(1.5))
    val result1 = RefinementPipeline.refine(config, vitamins, tmpDir)
    val json1 = RefinementPipeline.toRefinedJson(result1)

    // Second run: parse the output of the first run
    val config2 = SimpleJsonParser.parse(json1)
    val result2 = RefinementPipeline.refine(config2, vitamins, tmpDir)
    val json2 = RefinementPipeline.toRefinedJson(result2)

    // Count occurrences of boostedMemoryHeapJobCount — should be exactly 1
    val pattern = "boostedMemoryHeapJobCount".r
    pattern.findAllIn(json2).size shouldBe 1
  }

  // ── buildUnresolvedJson ───────────────────────────────────────────────────

  test("buildUnresolvedJson: produces valid JSON with vitamin source and entries") {
    val entries = Seq(
      UnresolvedEntry("b16_memory_heap_boost", "b16_oom_job_driver_exceptions.csv",
        "unknown-job-20260411-0438", "cluster-a", "", "2026-04-11T02:59:10Z", "OOM")
    )
    val json = ClusterMachineAndRecipeTunerRefinement.buildUnresolvedJson(entries, "inputs/2025_12_20")

    json should include("\"b16_memory_heap_boost\"")
    json should include("\"csv_source\": \"inputs/2025_12_20/b16_oom_job_driver_exceptions.csv\"")
    json should include("\"unresolved_count\": 1")
    json should include("\"job_id\": \"unknown-job-20260411-0438\"")
    json should include("\"cluster_name\": \"cluster-a\"")
  }

  // ── RefinementPipeline: toRefinedJson ─────────────────────────────────────

  test("toRefinedJson: produces JSON with boost counters in clusterConf") {
    val tmpDir = Files.createTempDirectory("refine_json").toFile
    tmpDir.deleteOnExit()
    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("job-1,cluster-wf-dmr-load-t-02-15-0215,_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json,2026-04-12T13:03:51Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
    } finally pw.close()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val vitamins = Seq(new MemoryHeapBoostVitamin(1.5))
    val result = RefinementPipeline.refine(config, vitamins, tmpDir)
    val json = RefinementPipeline.toRefinedJson(result)

    json should include("\"boostedMemoryHeapJobCount\": 1")
    json should include("\"boostedMemoryHeapJobList\"")
    json should include("\"_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json\"")
    json should include("\"appliedMemoryHeapBoostFactor\": 1.5")
    json should include("\"spark.executor.memory\": \"12g\"")
    // clusterConf fields preserved
    json should include("\"num_workers\": 10")
    json should include("\"worker_machine_type\": \"n2d-highcpu-48\"")
  }

  test("toRefinedJson: unaffected recipes keep original memory") {
    val tmpDir = Files.createTempDirectory("refine_json_nochange").toFile
    tmpDir.deleteOnExit()

    val config = SimpleJsonParser.parse(sampleAutoScaleJson)
    val result = RefinementPipeline.refine(config, Seq(new MemoryHeapBoostVitamin(1.5)), tmpDir)
    val json = RefinementPipeline.toRefinedJson(result)

    // All recipes should still have 8g since no CSV exists
    json should include("\"spark.executor.memory\": \"8g\"")
    json should include("\"boostedMemoryHeapJobCount\": 0")
    json should not include "appliedMemoryHeapBoostFactor"
  }

  // ── buildVitaminPipeline ──────────────────────────────────────────────────

  test("buildVitaminPipeline: includes MemoryHeapBoostVitamin") {
    val vitamins = ClusterMachineAndRecipeTunerRefinement.buildVitaminPipeline(1.5)
    vitamins should have size 1
    vitamins.head shouldBe a[MemoryHeapBoostVitamin]
    vitamins.head.asInstanceOf[MemoryHeapBoostVitamin].boostFactor shouldBe 1.5
  }

  test("buildVitaminPipeline: respects custom boost factor") {
    val vitamins = ClusterMachineAndRecipeTunerRefinement.buildVitaminPipeline(2.5)
    vitamins.head.asInstanceOf[MemoryHeapBoostVitamin].boostFactor shouldBe 2.5
  }
}
