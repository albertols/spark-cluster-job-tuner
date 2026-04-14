package com.db.serna.orchestration.cluster_tuning.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import java.nio.file.Files

class RefinementVitaminsSpec extends AnyFunSuite with Matchers {

  // ── MemoryHeapBoostVitamin: boostMemory ───────────────────────────────────

  private val vitamin = new MemoryHeapBoostVitamin(1.5)

  test("boostMemory: 8g x 1.5 = 12g") {
    vitamin.boostMemory("8g", 1.5) shouldBe "12g"
  }

  test("boostMemory: 8g x 2.0 = 16g") {
    vitamin.boostMemory("8g", 2.0) shouldBe "16g"
  }

  test("boostMemory: 3g x 1.5 = 5g (ceil of 4.5)") {
    vitamin.boostMemory("3g", 1.5) shouldBe "5g"
  }

  test("boostMemory: 8g x 3.0 = 24g") {
    vitamin.boostMemory("8g", 3.0) shouldBe "24g"
  }

  test("boostMemory: 1g x 1.5 = 2g (ceil of 1.5)") {
    vitamin.boostMemory("1g", 1.5) shouldBe "2g"
  }

  // ── MemoryHeapBoostVitamin: loadSignals ───────────────────────────────────

  test("loadSignals: parses b16 CSV and returns signals for matching cluster") {
    val tmpDir = Files.createTempDirectory("b16_test").toFile
    tmpDir.deleteOnExit()
    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("job-1,cluster-a,_ETL_recipe_A.json,2026-04-12T13:00:00Z,ERROR,some.Class,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,Java heap space,driver")
      pw.println("job-2,cluster-b,_ETL_recipe_B.json,2026-04-12T14:00:00Z,ERROR,some.Class,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,Java heap space,driver")
      pw.println("job-3,cluster-a,_ETL_recipe_C.json,2026-04-12T15:00:00Z,ERROR,some.Class,java.lang.StackOverflowError,FALSE,TRUE,FALSE,Stack overflow,driver")
    } finally pw.close()

    val signals = vitamin.loadSignals(tmpDir, "cluster-a")
    signals should have size 1
    val s = signals.head.asInstanceOf[MemoryHeapOomSignal]
    s.recipeFilename shouldBe "_ETL_recipe_A.json"
    s.jobId shouldBe "job-1"
  }

  test("loadSignals: includes signals with empty recipe_filename") {
    val tmpDir = Files.createTempDirectory("b16_empty_recipe").toFile
    tmpDir.deleteOnExit()
    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("etl-m-dq3-ods-f-gr-garantia-20260411-0438,cluster-a,,2026-04-11T02:59:10Z,ERROR,main,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,OOM,driver")
    } finally pw.close()

    val signals = vitamin.loadSignals(tmpDir, "cluster-a")
    signals should have size 1
    signals.head.recipeFilename shouldBe ""
    signals.head.jobId shouldBe "etl-m-dq3-ods-f-gr-garantia-20260411-0438"
  }

  test("loadSignals: returns empty for non-existent CSV") {
    val emptyDir = Files.createTempDirectory("b16_empty").toFile
    emptyDir.deleteOnExit()
    vitamin.loadSignals(emptyDir, "cluster-a") shouldBe empty
  }

  test("loadSignals: returns empty when no matching cluster") {
    val tmpDir = Files.createTempDirectory("b16_nomatch").toFile
    tmpDir.deleteOnExit()
    val csvFile = new File(tmpDir, "b16_oom_job_driver_exceptions.csv")
    csvFile.deleteOnExit()
    val pw = new PrintWriter(csvFile)
    try {
      pw.println("job_id,cluster_name,recipe_filename,latest_driver_log_ts,latest_driver_log_severity,latest_driver_log_class,latest_driver_exception_type,is_lost_task,is_stack_overflow,is_java_heap,latest_driver_message,log_name")
      pw.println("job-1,cluster-b,_ETL_recipe_B.json,2026-04-12T14:00:00Z,ERROR,some.Class,java.lang.OutOfMemoryError,FALSE,FALSE,TRUE,Java heap space,driver")
    } finally pw.close()

    vitamin.loadSignals(tmpDir, "cluster-a") shouldBe empty
  }

  // ── MemoryHeapBoostVitamin: computeBoosts ─────────────────────────────────

  private val sampleRecipes: Map[String, RecipeConfig] = Map(
    "_ETL_recipe_A.json" -> RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.executor.memory" -> "8g",
        "spark.executor.cores" -> "8",
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.dynamicAllocation.minExecutors" -> "2",
        "spark.dynamicAllocation.maxExecutors" -> "5"
      ),
      totalExecutorMinAllocatedMemoryGb = 16,
      totalExecutorMaxAllocatedMemoryGb = 40,
      extraFields = Map.empty
    ),
    "_ETL_recipe_B.json" -> RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.executor.memory" -> "8g",
        "spark.executor.cores" -> "8",
        "spark.executor.instances" -> "3"
      ),
      totalExecutorMinAllocatedMemoryGb = 24,
      totalExecutorMaxAllocatedMemoryGb = 24,
      extraFields = Map.empty
    )
  )

  test("computeBoosts: produces boost for matching recipe") {
    val signals = Seq(
      MemoryHeapOomSignal("cluster-a", "_ETL_recipe_A.json", "job-1", "2026-04-12T13:00:00Z", "OOM")
    )
    val boosts = vitamin.computeBoosts(signals, sampleRecipes)
    boosts should have size 1
    val b = boosts.head.asInstanceOf[MemoryHeapBoost]
    b.recipeFilename shouldBe "_ETL_recipe_A.json"
    b.originalMemory shouldBe "8g"
    b.boostedMemory shouldBe "12g"
    b.boostFactor shouldBe 1.5
  }

  test("computeBoosts: returns empty when signal recipe not in config") {
    val signals = Seq(
      MemoryHeapOomSignal("cluster-a", "_ETL_nonexistent.json", "job-99", "2026-04-12T13:00:00Z", "OOM")
    )
    vitamin.computeBoosts(signals, sampleRecipes) shouldBe empty
  }

  test("computeBoosts: deduplicates multiple signals for same recipe") {
    val signals = Seq(
      MemoryHeapOomSignal("cluster-a", "_ETL_recipe_A.json", "job-1", "2026-04-12T13:00:00Z", "OOM"),
      MemoryHeapOomSignal("cluster-a", "_ETL_recipe_A.json", "job-2", "2026-04-12T14:00:00Z", "OOM")
    )
    val boosts = vitamin.computeBoosts(signals, sampleRecipes)
    boosts should have size 1
  }

  // ── MemoryHeapBoostVitamin: applyBoosts ───────────────────────────────────

  test("applyBoosts: updates spark.executor.memory and adds boost field") {
    val boosts = Seq(
      MemoryHeapBoost("_ETL_recipe_A.json", "8g", "12g", 1.5)
    )
    val result = vitamin.applyBoosts(boosts, sampleRecipes)

    val rc = result("_ETL_recipe_A.json")
    rc.sparkOptsMap("spark.executor.memory") shouldBe "12g"
    rc.extraFields("appliedMemoryHeapBoostFactor") shouldBe "1.5"
  }

  test("applyBoosts: recomputes total memory fields for auto-scale recipe") {
    val boosts = Seq(
      MemoryHeapBoost("_ETL_recipe_A.json", "8g", "12g", 1.5)
    )
    val result = vitamin.applyBoosts(boosts, sampleRecipes)
    val rc = result("_ETL_recipe_A.json")
    rc.totalExecutorMinAllocatedMemoryGb shouldBe 24   // 2 min executors x 12g
    rc.totalExecutorMaxAllocatedMemoryGb shouldBe 60   // 5 max executors x 12g
  }

  test("applyBoosts: recomputes total memory fields for manual recipe") {
    val boosts = Seq(
      MemoryHeapBoost("_ETL_recipe_B.json", "8g", "12g", 1.5)
    )
    val result = vitamin.applyBoosts(boosts, sampleRecipes)
    val rc = result("_ETL_recipe_B.json")
    rc.totalExecutorMinAllocatedMemoryGb shouldBe 36   // 3 instances x 12g
    rc.totalExecutorMaxAllocatedMemoryGb shouldBe 36   // 3 instances x 12g
  }

  test("applyBoosts: does not modify unaffected recipes") {
    val boosts = Seq(
      MemoryHeapBoost("_ETL_recipe_A.json", "8g", "12g", 1.5)
    )
    val result = vitamin.applyBoosts(boosts, sampleRecipes)
    val rc = result("_ETL_recipe_B.json")
    rc.sparkOptsMap("spark.executor.memory") shouldBe "8g"
    rc.extraFields shouldBe empty
  }

  test("applyBoosts: empty boosts returns config unchanged") {
    val result = vitamin.applyBoosts(Seq.empty, sampleRecipes)
    result shouldBe sampleRecipes
  }

  test("applyBoosts: skip boost for recipe not in config") {
    val boosts = Seq(
      MemoryHeapBoost("_ETL_nonexistent.json", "8g", "12g", 1.5)
    )
    val result = vitamin.applyBoosts(boosts, sampleRecipes)
    result shouldBe sampleRecipes
  }

  // ── Vitamin trait properties ──────────────────────────────────────────────

  test("MemoryHeapBoostVitamin has correct counterKey") {
    vitamin.counterKey shouldBe "boostedMemoryHeapJobCount"
  }

  test("MemoryHeapBoostVitamin has correct listKey") {
    vitamin.listKey shouldBe "boostedMemoryHeapJobList"
  }

  test("MemoryHeapBoostVitamin has correct boostFieldKey") {
    vitamin.boostFieldKey shouldBe "appliedMemoryHeapBoostFactor"
  }

  test("MemoryHeapBoostVitamin name contains b16") {
    vitamin.name should include("b16")
  }

  test("MemoryHeapBoostVitamin has correct csvFileName") {
    vitamin.csvFileName shouldBe "b16_oom_job_driver_exceptions.csv"
  }

  // ── Configurable boost factor ─────────────────────────────────────────────

  test("MemoryHeapBoostVitamin with factor 2.0 boosts 8g to 16g") {
    val v2 = new MemoryHeapBoostVitamin(2.0)
    val signals = Seq(
      MemoryHeapOomSignal("cluster-a", "_ETL_recipe_A.json", "job-1", "", "")
    )
    val boosts = v2.computeBoosts(signals, sampleRecipes)
    val b = boosts.head.asInstanceOf[MemoryHeapBoost]
    b.boostedMemory shouldBe "16g"
    b.boostFactor shouldBe 2.0
  }

  // ── RecipeResolver ────────────────────────────────────────────────────────

  private val recipeNames = Set(
    "_ETL_m_DQ3_ODS_F_GR_GARANTIA.json",
    "_ETL_m_DWH_D_OTROS_BIENES_PO_UPDATE.json",
    "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json"
  )

  test("RecipeResolver.stripJobIdSuffix removes date-time suffix") {
    RecipeResolver.stripJobIdSuffix("etl-m-dq3-ods-f-gr-garantia-20260411-0438") shouldBe "etl-m-dq3-ods-f-gr-garantia"
  }

  test("RecipeResolver.stripJobIdSuffix handles job_id without suffix") {
    RecipeResolver.stripJobIdSuffix("some-job-no-date") shouldBe "some-job-no-date"
  }

  test("RecipeResolver.normaliseJobPrefix converts dashes to underscores and uppercases") {
    RecipeResolver.normaliseJobPrefix("etl-m-dq3-ods-f-gr-garantia") shouldBe "ETL_M_DQ3_ODS_F_GR_GARANTIA"
  }

  test("RecipeResolver.normaliseRecipeName strips prefix and suffix") {
    RecipeResolver.normaliseRecipeName("_ETL_m_DQ3_ODS_F_GR_GARANTIA.json") shouldBe "ETL_M_DQ3_ODS_F_GR_GARANTIA"
  }

  test("RecipeResolver: resolves empty recipe via job-id derivation") {
    val signals: Seq[VitaminSignal] = Seq(
      MemoryHeapOomSignal("cluster-a", "", "etl-m-dq3-ods-f-gr-garantia-20260411-0438", "2026-04-11T02:59:10Z", "OOM")
    )
    val (resolved, unresolved) = RecipeResolver.resolve(signals, recipeNames)
    resolved should have size 1
    resolved.head.recipeFilename shouldBe "_ETL_m_DQ3_ODS_F_GR_GARANTIA.json"
    unresolved shouldBe empty
  }

  test("RecipeResolver: resolves empty recipe via sibling lookup") {
    val signals: Seq[VitaminSignal] = Seq(
      MemoryHeapOomSignal("cluster-a", "", "etl-m-dwh-d-otros-bienes-po-update-20260410-0517", "2026-04-10T07:18:46Z", "OOM"),
      MemoryHeapOomSignal("cluster-a", "_ETL_m_DWH_D_OTROS_BIENES_PO_UPDATE.json", "etl-m-dwh-d-otros-bienes-po-update-20260409-0514", "2026-04-09T07:16:05Z", "OOM")
    )
    val (resolved, unresolved) = RecipeResolver.resolve(signals, recipeNames)
    resolved should have size 2
    resolved.head.recipeFilename shouldBe "_ETL_m_DWH_D_OTROS_BIENES_PO_UPDATE.json"
    resolved(1).recipeFilename shouldBe "_ETL_m_DWH_D_OTROS_BIENES_PO_UPDATE.json"
    unresolved shouldBe empty
  }

  test("RecipeResolver: marks truly unresolvable signals as unresolved") {
    val signals: Seq[VitaminSignal] = Seq(
      MemoryHeapOomSignal("cluster-a", "", "totally-unknown-job-20260411-0438", "2026-04-11T02:59:10Z", "OOM")
    )
    val (resolved, unresolved) = RecipeResolver.resolve(signals, recipeNames)
    resolved shouldBe empty
    unresolved should have size 1
    unresolved.head.jobId shouldBe "totally-unknown-job-20260411-0438"
  }

  test("RecipeResolver: signals with recipe already set pass through unchanged") {
    val signals: Seq[VitaminSignal] = Seq(
      MemoryHeapOomSignal("cluster-a", "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json", "job-1", "", "")
    )
    val (resolved, unresolved) = RecipeResolver.resolve(signals, recipeNames)
    resolved should have size 1
    resolved.head.recipeFilename shouldBe "_ETL_m_DQ3_ODS_F_PM_PROPUESTAS.json"
    unresolved shouldBe empty
  }

  test("RecipeResolver: empty signals returns empty") {
    val (resolved, unresolved) = RecipeResolver.resolve(Seq.empty, recipeNames)
    resolved shouldBe empty
    unresolved shouldBe empty
  }

  // ── computeBoosts with resolution ─────────────────────────────────────────

  private val resolverRecipes: Map[String, RecipeConfig] = Map(
    "_ETL_m_DQ3_ODS_F_GR_GARANTIA.json" -> RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.executor.memory" -> "8g",
        "spark.executor.cores" -> "8",
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.dynamicAllocation.minExecutors" -> "2",
        "spark.dynamicAllocation.maxExecutors" -> "3"
      ),
      totalExecutorMinAllocatedMemoryGb = 16,
      totalExecutorMaxAllocatedMemoryGb = 24,
      extraFields = Map.empty
    )
  )

  test("computeBoosts: produces boost for pre-resolved signal") {
    // Resolution happens in RefinementPipeline.refine(), not in computeBoosts.
    // Here we pass an already-resolved signal to verify boost computation.
    val signals = Seq(
      MemoryHeapOomSignal("cluster-a", "_ETL_m_DQ3_ODS_F_GR_GARANTIA.json", "etl-m-dq3-ods-f-gr-garantia-20260411-0438", "2026-04-11T02:59:10Z", "OOM")
    )
    val boosts = vitamin.computeBoosts(signals, resolverRecipes)
    boosts should have size 1
    boosts.head.recipeFilename shouldBe "_ETL_m_DQ3_ODS_F_GR_GARANTIA.json"
  }
}
