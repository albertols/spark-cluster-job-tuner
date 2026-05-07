package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.Csv

import java.io.File
import scala.collection.mutable

// ── Signals ─────────────────────────────────────────────────────────────────

/** A condition detected from a diagnostic CSV that may trigger a recipe-level boost. */
sealed trait VitaminSignal {
  def clusterName: String
  def recipeFilename: String
  def jobId: String
  def description: String
}

final case class MemoryHeapOomSignal(
    clusterName: String,
    recipeFilename: String,
    jobId: String,
    latestDriverLogTs: String,
    latestDriverMessage: String
) extends VitaminSignal {
  val description: String = s"Java heap OOM for job $jobId ($recipeFilename) at $latestDriverLogTs"
}

/**
 * Synthetic signal — NOT loaded from CSV. Built in-memory by the AutoTuner from `divergences_current_snapshot` outliers
 * whose paired recipe is cap-touching (current `p95_run_max_executors` close to the recipe's current `maxExecutors`).
 * New entries are excluded by the AutoTuner before signals reach this vitamin.
 */
final case class ExecutorScaleSignal(
    clusterName: String,
    recipeFilename: String,
    jobId: String,
    metricName: String,
    zScore: Double,
    currentMaxExecutors: Int,
    p95RunMaxExecutors: Double
) extends VitaminSignal {
  val description: String =
    s"High duration z=$zScore on $metricName for $recipeFilename — cap-touching " +
      f"(p95_run_max_executors=$p95RunMaxExecutors%.1f / maxExecutors=$currentMaxExecutors)"
}

// ── Unresolved entries ──────────────────────────────────────────────────────

/** A diagnostic CSV entry that could not be matched to any recipe in the cluster. */
final case class UnresolvedEntry(
    vitaminName: String,
    csvSource: String,
    jobId: String,
    clusterName: String,
    rawRecipeFilename: String,
    latestDriverLogTs: String,
    latestDriverMessage: String
)

// ── Boosts ──────────────────────────────────────────────────────────────────

/**
 * Lifecycle state of a boost across multi-date AutoTuner runs.
 *
 *   - `New` : recipe untagged, fresh signal in the current-date CSV → first-time boost.
 *   - `ReBoost` : recipe already tagged AND fresh current-date signal → previous boost was insufficient; multiply
 *     current memory by the per-step factor and store the new cumulative factor.
 *   - `Holding` : recipe already tagged but NO fresh current-date signal → carry the boost forward unchanged (the boost
 *     is succeeding).
 *
 * Single-tuner (one inputDir) only ever emits `New` boosts; the holding/re-boost distinction requires AutoTuner-style
 * date awareness (multiple inputDirs).
 */
sealed trait BoostState { def label: String }
object BoostState {
  case object New extends BoostState { val label = "new" }
  case object ReBoost extends BoostState { val label = "re-boost" }
  case object Holding extends BoostState { val label = "holding" }
}

/** A computed action to apply to a recipe's Spark configuration. */
sealed trait VitaminBoost {
  def recipeFilename: String
  def description: String
}

final case class MemoryHeapBoost(
    recipeFilename: String,
    originalMemory: String,
    boostedMemory: String,
    boostFactor: Double,
    cumulativeFactor: Double = Double.NaN,
    state: BoostState = BoostState.New
) extends VitaminBoost {

  /** Cumulative factor stored in the recipe JSON. NaN sentinel means "same as boostFactor". */
  def effectiveCumulativeFactor: Double = if (cumulativeFactor.isNaN) boostFactor else cumulativeFactor

  val description: String =
    s"spark.executor.memory: $originalMemory -> $boostedMemory (x$boostFactor heap OOM boost, ${state.label})"
}

final case class ExecutorScaleBoost(
    recipeFilename: String,
    originalMaxExecutors: Int,
    boostedMaxExecutors: Int,
    boostFactor: Double,
    cumulativeFactor: Double = Double.NaN,
    state: BoostState = BoostState.New
) extends VitaminBoost {
  def effectiveCumulativeFactor: Double = if (cumulativeFactor.isNaN) boostFactor else cumulativeFactor

  val description: String =
    s"spark.dynamicAllocation.maxExecutors: $originalMaxExecutors -> $boostedMaxExecutors (x$boostFactor scale-up, ${state.label})"
}

// ── Vitamin Trait ───────────────────────────────────────────────────────────

/**
 * A refinement vitamin detects a diagnostic condition from a CSV and applies targeted boosts to affected recipe
 * configurations.
 *
 * Each vitamin declares:
 *   - `name` — human-readable identifier
 *   - `csvFileName` — source CSV file name (for unresolved report)
 *   - `counterKey` — field name injected into clusterConf (e.g. "boostedMemoryHeapJobCount")
 *   - `listKey` — field name for list of boosted recipe names in clusterConf
 *   - `boostFieldKey` — field name added per-recipe (e.g. "appliedMemoryHeapBoostFactor")
 */
trait RefinementVitamin {
  def name: String
  def csvFileName: String
  def counterKey: String
  def listKey: String
  def boostFieldKey: String

  def loadSignals(inputDir: File, clusterName: String): Seq[VitaminSignal]
  def computeBoosts(signals: Seq[VitaminSignal], recipes: Map[String, RecipeConfig]): Seq[VitaminBoost]
  def applyBoosts(boosts: Seq[VitaminBoost], recipes: Map[String, RecipeConfig]): Map[String, RecipeConfig]

  /**
   * Date-aware overload used by AutoTuner. `currentSignals` are the signals loaded from the current-date inputDir (the
   * head of `inputDirs` in the pipeline); `signals` is the union of current + reference-date signals after dedupe.
   *
   * Default delegates to the legacy 2-arg method so vitamins that don't care about the current/reference distinction
   * keep their existing behavior. Vitamins that need the `New` / `ReBoost` / `Holding` lifecycle (e.g.
   * `MemoryHeapBoostVitamin`) override this.
   */
  def computeBoosts(
      signals: Seq[VitaminSignal],
      recipes: Map[String, RecipeConfig],
      currentSignals: Seq[VitaminSignal]
  ): Seq[VitaminBoost] = computeBoosts(signals, recipes)
}

// ── Recipe Resolution ───────────────────────────────────────────────────────

object RecipeResolver {

  /**
   * Strip date+time suffix from a job_id. e.g. "etl-m-dq3-ods-f-gr-garantia-20260411-0438" →
   * "etl-m-dq3-ods-f-gr-garantia"
   */
  private[refinement] def stripJobIdSuffix(jobId: String): String =
    jobId.replaceAll("-\\d{8}-\\d{4}$", "")

  /**
   * Derive a normalised recipe name from a job_id prefix for case-insensitive matching. e.g.
   * "etl-m-dq3-ods-f-gr-garantia" → "ETL_M_DQ3_ODS_F_GR_GARANTIA"
   */
  private[refinement] def normaliseJobPrefix(prefix: String): String =
    prefix.replace('-', '_').toUpperCase

  /**
   * Normalise a recipe filename for case-insensitive matching. e.g. "_ETL_m_DQ3_ODS_F_GR_GARANTIA.json" →
   * "ETL_M_DQ3_ODS_F_GR_GARANTIA"
   */
  private[refinement] def normaliseRecipeName(recipe: String): String =
    recipe.stripPrefix("_").stripSuffix(".json").toUpperCase

  /**
   * Resolve missing recipe_filename for signals using two strategies:
   *
   *   1. **Sibling lookup**: Find another signal with the same job prefix (after stripping date suffix) that already
   *      has a recipe_filename.
   *
   * 2. **Job-id derivation**: Strip date suffix, replace `-` with `_`, and match case-insensitively against the
   * available recipe names in the cluster.
   *
   * Returns (resolved signals, unresolved signals).
   */
  def resolve(
      signals: Seq[VitaminSignal],
      recipeNames: Set[String]
  ): (Seq[VitaminSignal], Seq[VitaminSignal]) = {

    // Build lookup: job prefix → known recipe (from signals that already have one)
    val prefixToRecipe: Map[String, String] = signals
      .filter(_.recipeFilename.nonEmpty)
      .map(s => stripJobIdSuffix(s.jobId) -> s.recipeFilename)
      .toMap

    // Build lookup: normalised name → actual recipe filename
    val normToRecipe: Map[String, String] = recipeNames
      .map(r => normaliseRecipeName(r) -> r)
      .toMap

    val resolved = mutable.ArrayBuffer.empty[VitaminSignal]
    val unresolved = mutable.ArrayBuffer.empty[VitaminSignal]

    signals.foreach { signal =>
      if (signal.recipeFilename.nonEmpty) {
        resolved += signal
      } else {
        val prefix = stripJobIdSuffix(signal.jobId)

        // Strategy 1: sibling lookup
        val fromSibling = prefixToRecipe.get(prefix)

        // Strategy 2: job-id derivation
        val fromDerivation = fromSibling.orElse {
          val normPrefix = normaliseJobPrefix(prefix)
          normToRecipe.get(normPrefix)
        }

        fromDerivation match {
          case Some(recipe) =>
            signal match {
              case s: MemoryHeapOomSignal => resolved += s.copy(recipeFilename = recipe)
              case other => resolved += other
            }
          case None =>
            unresolved += signal
        }
      }
    }

    (resolved.toSeq, unresolved.toSeq)
  }
}

// ── B16: Memory Heap OOM Vitamin ────────────────────────────────────────────

/**
 * Detects Java heap OOM failures from b16_oom_job_driver_exceptions.csv and boosts `spark.executor.memory` for affected
 * recipes by the given factor.
 *
 * Signals with empty recipe_filename are resolved via [[RecipeResolver]]: first by sibling lookup (other rows with same
 * job prefix), then by job-id derivation matched against available recipe names.
 */
class MemoryHeapBoostVitamin(val boostFactor: Double = 1.5) extends RefinementVitamin {
  val name = "b16_memory_heap_boost"
  val csvFileName = "b16_oom_job_driver_exceptions.csv"
  val counterKey = "boostedMemoryHeapJobCount"
  val listKey = "boostedMemoryHeapJobList"
  val boostFieldKey = "appliedMemoryHeapBoostFactor"

  def loadSignals(inputDir: File, clusterName: String): Seq[VitaminSignal] = {
    val csvFile = new File(inputDir, csvFileName)
    if (!csvFile.exists()) return Seq.empty

    val rows = Csv.parse(csvFile)
    rows.flatMap { r =>
      for {
        jobId <- r.get("job_id").map(_.trim).filter(_.nonEmpty)
        cluster <- r.get("cluster_name").map(_.replaceAll("\"", "").trim).filter(_ == clusterName)
        isHeap <- r.get("is_java_heap").map(_.trim.toUpperCase)
        if isHeap == "TRUE"
      } yield MemoryHeapOomSignal(
        clusterName = cluster,
        recipeFilename = r.get("recipe_filename").map(_.trim).getOrElse(""),
        jobId = jobId,
        latestDriverLogTs = r.getOrElse("latest_driver_log_ts", ""),
        latestDriverMessage = r.getOrElse("latest_driver_message", "")
      )
    }
  }

  /**
   * Legacy single-date entry point (single-tuner, single inputDir). Kept idempotent on re-run: a recipe that already
   * carries `appliedMemoryHeapBoostFactor >= boostFactor` yields a no-op boost so the same CSV processed twice on its
   * own output doesn't double-boost. AutoTuner uses the 3-arg overload below for the New / ReBoost / Holding lifecycle.
   */
  def computeBoosts(signals: Seq[VitaminSignal], recipes: Map[String, RecipeConfig]): Seq[VitaminBoost] = {
    val heapSignals = signals.collect { case s: MemoryHeapOomSignal => s }
    val affectedRecipes = heapSignals.map(_.recipeFilename).filter(_.nonEmpty).distinct

    affectedRecipes.flatMap { recipe =>
      recipes.get(recipe).map { rc =>
        val currentMem = rc.sparkOptsMap.getOrElse("spark.executor.memory", "8g")
        val existingFactor = rc.extraFields.get(boostFieldKey).flatMap(s => scala.util.Try(s.toDouble).toOption)
        val alreadyBoosted = existingFactor.exists(_ >= boostFactor)
        if (alreadyBoosted) {
          // Single-tuner re-run idempotency. State is "Holding" so AutoTuner-side
          // counters classify this consistently if it ever reaches them.
          val cum = existingFactor.getOrElse(boostFactor)
          MemoryHeapBoost(recipe, currentMem, currentMem, boostFactor, cum, BoostState.Holding)
        } else {
          val boostedMem = boostMemory(currentMem, boostFactor)
          MemoryHeapBoost(recipe, currentMem, boostedMem, boostFactor, boostFactor, BoostState.New)
        }
      }
    }
  }

  /**
   * Date-aware computeBoosts used by AutoTuner. Distinguishes:
   *   - fresh OOM in current-date CSV + recipe untagged → `New`
   *   - fresh OOM in current-date CSV + recipe tagged → `ReBoost` (stack: cur-mem * factor)
   *   - tagged recipe with NO fresh current-date signal → `Holding`
   *
   * Untagged recipes that only show up via reference-date signals are conservatively treated as `New` (a missed signal
   * that should be acted on).
   */
  override def computeBoosts(
      signals: Seq[VitaminSignal],
      recipes: Map[String, RecipeConfig],
      currentSignals: Seq[VitaminSignal]
  ): Seq[VitaminBoost] = {
    val heapAll = signals.collect { case s: MemoryHeapOomSignal => s }
    val heapCurrent = currentSignals.collect { case s: MemoryHeapOomSignal => s }
    val currentRecipeSet = heapCurrent.map(_.recipeFilename).filter(_.nonEmpty).toSet
    val anyRecipeSet = heapAll.map(_.recipeFilename).filter(_.nonEmpty).toSet

    // Holding includes recipes that were boosted in a past run but have no signal of
    // any kind in the current input dirs — surface them so the dashboard can show
    // "boost still working".
    val recipesWithPriorTag = recipes.collect {
      case (name, rc) if rc.extraFields.contains(boostFieldKey) => name
    }.toSet

    val candidates = (anyRecipeSet ++ recipesWithPriorTag).toSeq.distinct

    candidates.flatMap { recipe =>
      recipes.get(recipe).flatMap { rc =>
        val currentMem = rc.sparkOptsMap.getOrElse("spark.executor.memory", "8g")
        val priorFactor = rc.extraFields.get(boostFieldKey).flatMap(s => scala.util.Try(s.toDouble).toOption)
        val hasNewSignal = currentRecipeSet.contains(recipe)

        (priorFactor, hasNewSignal) match {
          case (None, true) =>
            val boosted = boostMemory(currentMem, boostFactor)
            Some(MemoryHeapBoost(recipe, currentMem, boosted, boostFactor, boostFactor, BoostState.New))
          case (Some(prev), true) =>
            // Stack on top of current (already-boosted) memory.
            val boosted = boostMemory(currentMem, boostFactor)
            Some(MemoryHeapBoost(recipe, currentMem, boosted, boostFactor, prev * boostFactor, BoostState.ReBoost))
          case (Some(prev), false) =>
            Some(MemoryHeapBoost(recipe, currentMem, currentMem, boostFactor, prev, BoostState.Holding))
          case (None, false) =>
            // Untagged recipe with reference-only signal — conservatively boost so the
            // signal isn't silently lost between runs.
            if (anyRecipeSet.contains(recipe)) {
              val boosted = boostMemory(currentMem, boostFactor)
              Some(MemoryHeapBoost(recipe, currentMem, boosted, boostFactor, boostFactor, BoostState.New))
            } else None
        }
      }
    }
  }

  def applyBoosts(boosts: Seq[VitaminBoost], recipes: Map[String, RecipeConfig]): Map[String, RecipeConfig] = {
    boosts.foldLeft(recipes) {
      case (cfg, b: MemoryHeapBoost) =>
        cfg.get(b.recipeFilename) match {
          case Some(rc) =>
            // Persist the cumulative factor so subsequent runs can detect chained boosts.
            val updatedExtra = rc.extraFields + (boostFieldKey -> b.effectiveCumulativeFactor.toString)
            if (b.state == BoostState.Holding || b.originalMemory == b.boostedMemory) {
              cfg.updated(b.recipeFilename, rc.copy(extraFields = updatedExtra))
            } else {
              val updatedOpts = rc.sparkOptsMap.updated("spark.executor.memory", b.boostedMemory)
              val memGb = SimpleJsonParser.parseMemoryGb(b.boostedMemory)
              val (minExec, maxExec) = extractExecutorCounts(rc)
              cfg.updated(
                b.recipeFilename,
                rc.copy(
                  sparkOptsMap = updatedOpts,
                  totalExecutorMinAllocatedMemoryGb = minExec * memGb,
                  totalExecutorMaxAllocatedMemoryGb = maxExec * memGb,
                  extraFields = updatedExtra
                )
              )
            }
          case None => cfg
        }
      case (cfg, _) => cfg
    }
  }

  private[refinement] def boostMemory(current: String, factor: Double): String = {
    val gb = SimpleJsonParser.parseMemoryGb(current)
    val boosted = math.ceil(gb * factor).toInt
    s"${boosted}g"
  }

  private def extractExecutorCounts(rc: RecipeConfig): (Int, Int) = {
    val opts = rc.sparkOptsMap
    val isDynamic = opts.get("spark.dynamicAllocation.enabled").contains("true")
    if (isDynamic) {
      val min =
        opts.get("spark.dynamicAllocation.minExecutors").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(1)
      val max =
        opts.get("spark.dynamicAllocation.maxExecutors").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(min)
      (min, max)
    } else {
      val instances = opts.get("spark.executor.instances").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(1)
      (instances, instances)
    }
  }
}

// ── Executor Scale-up Vitamin ──────────────────────────────────────────────

/**
 * Bumps `spark.dynamicAllocation.maxExecutors` (and `total_executor_max_allocated_memory_gb`) for cap-touching duration
 * outliers detected by the AutoTuner's divergence pipeline.
 *
 * Signals are NOT loaded from a CSV — the AutoTuner constructs them in memory from `divergences_current_snapshot`
 * (filtered to non-new entries with high positive z-score on a duration metric) and the recipe's current
 * p95_run_max_executors. The caller injects signals via the `signalsForCluster` lookup so the `RefinementPipeline` can
 * drive the same New/ReBoost/Holding lifecycle that the b16 vitamin uses.
 *
 * Manual recipes (using `spark.executor.instances`) are skipped — the goal is to raise the autoscaling ceiling, not to
 * grow a fixed allocation.
 *
 * `minExecutors` is intentionally NOT touched: more headroom, not a higher floor.
 */
class ExecutorScaleVitamin(
    val boostFactor: Double = 1.5,
    val signalsForCluster: String => Seq[ExecutorScaleSignal] = _ => Seq.empty
) extends RefinementVitamin {
  val name = "executor_scale_up"
  val csvFileName = "(divergence-driven, no CSV)"
  val counterKey = "scaledMaxExecutorsJobCount"
  val listKey = "scaledMaxExecutorsJobList"
  val boostFieldKey = "appliedExecutorScaleFactor"

  /**
   * No CSV — signals are pre-built. The pipeline calls this once per inputDir; the dedupe step in
   * `RefinementPipeline.refine` collapses duplicates by recipe.
   */
  def loadSignals(inputDir: File, clusterName: String): Seq[VitaminSignal] =
    signalsForCluster(clusterName)

  /** Single-date / single-tuner path — kept idempotent (no-op when already scaled). */
  def computeBoosts(signals: Seq[VitaminSignal], recipes: Map[String, RecipeConfig]): Seq[VitaminBoost] = {
    val scaleSignals = signals.collect { case s: ExecutorScaleSignal => s }
    scaleSignals.flatMap { sig =>
      recipes.get(sig.recipeFilename).flatMap { rc =>
        val (curMin, curMax) = extractExecutorCounts(rc)
        if (curMin == curMax) None // manual / non-DA recipe — skip
        else {
          val existing = rc.extraFields.get(boostFieldKey).flatMap(s => scala.util.Try(s.toDouble).toOption)
          val alreadyScaled = existing.exists(_ >= boostFactor)
          if (alreadyScaled) {
            Some(
              ExecutorScaleBoost(
                sig.recipeFilename,
                curMax,
                curMax,
                boostFactor,
                existing.getOrElse(boostFactor),
                BoostState.Holding
              )
            )
          } else {
            val boosted = math.max(curMax + 1, math.ceil(curMax * boostFactor).toInt)
            Some(ExecutorScaleBoost(sig.recipeFilename, curMax, boosted, boostFactor, boostFactor, BoostState.New))
          }
        }
      }
    }
  }

  /** Date-aware path mirroring [[MemoryHeapBoostVitamin.computeBoosts]]: New / ReBoost / Holding. */
  override def computeBoosts(
      signals: Seq[VitaminSignal],
      recipes: Map[String, RecipeConfig],
      currentSignals: Seq[VitaminSignal]
  ): Seq[VitaminBoost] = {
    val current = currentSignals.collect { case s: ExecutorScaleSignal => s }
    val all = signals.collect { case s: ExecutorScaleSignal => s }
    val currentRecipeSet = current.map(_.recipeFilename).filter(_.nonEmpty).toSet
    val anyRecipeSet = all.map(_.recipeFilename).filter(_.nonEmpty).toSet

    val recipesWithPriorTag = recipes.collect {
      case (name, rc) if rc.extraFields.contains(boostFieldKey) => name
    }.toSet

    val candidates = (anyRecipeSet ++ recipesWithPriorTag).toSeq.distinct

    candidates.flatMap { recipe =>
      recipes.get(recipe).flatMap { rc =>
        val (curMin, curMax) = extractExecutorCounts(rc)
        if (curMin == curMax) None // manual recipe — skip
        else {
          val priorFactor = rc.extraFields.get(boostFieldKey).flatMap(s => scala.util.Try(s.toDouble).toOption)
          val hasNewSignal = currentRecipeSet.contains(recipe)
          (priorFactor, hasNewSignal) match {
            case (None, true) =>
              val boosted = math.max(curMax + 1, math.ceil(curMax * boostFactor).toInt)
              Some(ExecutorScaleBoost(recipe, curMax, boosted, boostFactor, boostFactor, BoostState.New))
            case (Some(prev), true) =>
              val boosted = math.max(curMax + 1, math.ceil(curMax * boostFactor).toInt)
              Some(ExecutorScaleBoost(recipe, curMax, boosted, boostFactor, prev * boostFactor, BoostState.ReBoost))
            case (Some(prev), false) =>
              Some(ExecutorScaleBoost(recipe, curMax, curMax, boostFactor, prev, BoostState.Holding))
            case (None, false) =>
              None
          }
        }
      }
    }
  }

  def applyBoosts(boosts: Seq[VitaminBoost], recipes: Map[String, RecipeConfig]): Map[String, RecipeConfig] = {
    boosts.foldLeft(recipes) {
      case (cfg, b: ExecutorScaleBoost) =>
        cfg.get(b.recipeFilename) match {
          case Some(rc) =>
            val updatedExtra = rc.extraFields + (boostFieldKey -> b.effectiveCumulativeFactor.toString)
            if (b.state == BoostState.Holding || b.originalMaxExecutors == b.boostedMaxExecutors) {
              cfg.updated(b.recipeFilename, rc.copy(extraFields = updatedExtra))
            } else {
              val opts = rc.sparkOptsMap
              val updatedOpts = opts.updated("spark.dynamicAllocation.maxExecutors", b.boostedMaxExecutors.toString)
              val memGb = SimpleJsonParser.parseMemoryGb(opts.getOrElse("spark.executor.memory", "8g"))
              cfg.updated(
                b.recipeFilename,
                rc.copy(
                  sparkOptsMap = updatedOpts,
                  totalExecutorMaxAllocatedMemoryGb = b.boostedMaxExecutors * memGb,
                  extraFields = updatedExtra
                )
              )
            }
          case None => cfg
        }
      case (cfg, _) => cfg
    }
  }

  private def extractExecutorCounts(rc: RecipeConfig): (Int, Int) = {
    val opts = rc.sparkOptsMap
    val isDynamic = opts.get("spark.dynamicAllocation.enabled").contains("true")
    if (isDynamic) {
      val min =
        opts.get("spark.dynamicAllocation.minExecutors").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(1)
      val max =
        opts.get("spark.dynamicAllocation.maxExecutors").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(min)
      (min, max)
    } else {
      val instances = opts.get("spark.executor.instances").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(1)
      (instances, instances)
    }
  }
}

// ── Refinement Pipeline ─────────────────────────────────────────────────────

/** Result of applying all vitamins to a single cluster config. */
final case class RefinementResult(
    clusterName: String,
    originalConfig: TunedClusterConfig,
    refinedRecipes: Map[String, RecipeConfig],
    appliedBoosts: Seq[VitaminBoost],
    boostCounters: Map[String, Int],
    boostLists: Map[String, Seq[String]],
    unresolvedEntries: Seq[UnresolvedEntry]
)

object RefinementPipeline {

  /**
   * Apply all vitamins to a single cluster config, collecting signals from multiple input directories.
   *
   * Signals from all `inputDirs` are merged per vitamin; after resolution, duplicates by `recipeFilename` are removed
   * keeping the first occurrence (earlier entries in `inputDirs` — i.e. the more recent date — take priority). This
   * prevents double-boosting when a previously-boosted reference config is re-processed alongside an older CSV that
   * also contains the same recipe.
   */
  def refine(
      config: TunedClusterConfig,
      vitamins: Seq[RefinementVitamin],
      inputDirs: Seq[File]
  ): RefinementResult = {
    var currentRecipes = config.recipes
    val allBoosts = mutable.ArrayBuffer.empty[VitaminBoost]
    val allUnresolved = mutable.ArrayBuffer.empty[UnresolvedEntry]
    val counters = mutable.LinkedHashMap.empty[String, Int]
    val lists = mutable.LinkedHashMap.empty[String, Seq[String]]

    vitamins.foreach { vitamin =>
      // Collect signals from ALL input dirs (curDate first, then refDate, etc.). Track the
      // current-date subset separately so date-aware vitamins can distinguish "fresh" OOM
      // (in the current-date CSV) from "carried" OOM (from older dirs).
      val curRawSignals = inputDirs.headOption.toSeq.flatMap(dir => vitamin.loadSignals(dir, config.clusterName))
      val rawSignals = inputDirs.flatMap(dir => vitamin.loadSignals(dir, config.clusterName))

      // Resolve all signals (including the current-date subset for state classification).
      val (allResolved, allUnresolvd) = RecipeResolver.resolve(rawSignals, currentRecipes.keySet)
      val (curResolved, _) = RecipeResolver.resolve(curRawSignals, currentRecipes.keySet)

      // Deduplicate by recipeFilename keeping first occurrence (curDate wins over refDate).
      val seen = mutable.LinkedHashSet.empty[String]
      val resolved = allResolved.filter { s =>
        if (s.recipeFilename.isEmpty) false
        else if (seen.contains(s.recipeFilename)) false
        else { seen += s.recipeFilename; true }
      }

      // Single inputDir → legacy 2-arg path (preserves single-tuner re-run idempotency).
      // Multiple inputDirs → date-aware 3-arg path (new/re-boost/holding state lifecycle).
      val boosts =
        if (inputDirs.size > 1) vitamin.computeBoosts(resolved, currentRecipes, curResolved)
        else vitamin.computeBoosts(resolved, currentRecipes)
      currentRecipes = vitamin.applyBoosts(boosts, currentRecipes)
      allBoosts ++= boosts
      counters(vitamin.counterKey) = boosts.size
      lists(vitamin.listKey) = boosts.map(_.recipeFilename).distinct

      def toEntry(s: VitaminSignal) = UnresolvedEntry(
        vitaminName = vitamin.name,
        csvSource = vitamin.csvFileName,
        jobId = s.jobId,
        clusterName = s.clusterName,
        rawRecipeFilename = s.recipeFilename,
        latestDriverLogTs = s match { case h: MemoryHeapOomSignal => h.latestDriverLogTs; case _ => "" },
        latestDriverMessage = s match { case h: MemoryHeapOomSignal => h.latestDriverMessage; case _ => "" }
      )

      // Track signals that couldn't be resolved to a recipe name
      allUnresolvd.foreach(s => allUnresolved += toEntry(s))

      // Track resolved signals whose recipe doesn't exist in the cluster config
      resolved
        .filter(s => s.recipeFilename.nonEmpty && !currentRecipes.contains(s.recipeFilename))
        .foreach(s => allUnresolved += toEntry(s))
    }

    RefinementResult(
      config.clusterName,
      config,
      currentRecipes,
      allBoosts.toSeq,
      counters.toMap,
      lists.toMap,
      allUnresolved.toSeq
    )
  }

  /** Backward-compatible single-directory overload — delegates to the multi-dir variant. */
  def refine(
      config: TunedClusterConfig,
      vitamins: Seq[RefinementVitamin],
      inputDir: File
  ): RefinementResult = refine(config, vitamins, Seq(inputDir))

  /** Rebuild the refined JSON string from a RefinementResult. */
  def toRefinedJson(result: RefinementResult): String = {
    import com.db.serna.orchestration.cluster_tuning.single.Json
    import Json._

    // Rebuild clusterConf fields preserving original order, then append boost counters + lists.
    // Filter out any existing boost keys from prior runs to avoid duplicates.
    val boostKeys = result.boostCounters.keySet ++ result.boostLists.keySet
    val clusterFields: Seq[(String, String)] = result.originalConfig.clusterConfFields
      .filterNot { case (k, _) => boostKeys.contains(k) }
      .map { case (k, v) =>
        if (scala.util.Try(v.toLong).isSuccess) k -> num(v.toLong)
        else if (scala.util.Try(v.toDouble).isSuccess) k -> num(v.toDouble)
        else k -> str(v)
      }

    val counterFields: Seq[(String, String)] = result.boostCounters.toSeq.map { case (k, v) =>
      k -> num(v)
    }

    val listFields: Seq[(String, String)] = result.boostLists.toSeq.map { case (k, recipes) =>
      k -> arr(recipes.map(str): _*)
    }

    val clusterConf = obj(
      result.clusterName -> obj((clusterFields ++ counterFields ++ listFields): _*)
    )

    // Rebuild recipeSparkConf preserving original recipe order
    val recipes: Seq[(String, String)] = result.originalConfig.recipeOrder.map { recipeName =>
      val rc = result.refinedRecipes.getOrElse(recipeName, result.originalConfig.recipes(recipeName))

      val boostFields: Seq[(String, String)] = rc.extraFields.toSeq.map { case (k, v) =>
        if (scala.util.Try(v.toDouble).isSuccess) k -> num(v.toDouble)
        else k -> str(v)
      }

      val sparkOptsFields: Seq[(String, String)] = rc.sparkOptsMap.toSeq
        .sortBy { case (k, _) =>
          sparkOptsOrder(k)
        }
        .map { case (k, v) => k -> str(v) }

      recipeName -> obj(
        (Seq("parallelizationFactor" -> num(rc.parallelizationFactor)) ++
          boostFields ++
          Seq("sparkOptsMap" -> obj(sparkOptsFields: _*)) ++
          Seq(
            "total_executor_minimum_allocated_memory_gb" -> num(rc.totalExecutorMinAllocatedMemoryGb),
            "total_executor_maximum_allocated_memory_gb" -> num(rc.totalExecutorMaxAllocatedMemoryGb)
          )): _*
      )
    }

    val recipeSparkConf = obj(recipes: _*)
    Json.pretty(obj("clusterConf" -> clusterConf, "recipeSparkConf" -> recipeSparkConf))
  }

  /** Preserve the original sparkOptsMap ordering from the tuner. */
  private def sparkOptsOrder(key: String): Int = key match {
    case "spark.serializer" => 0
    case "spark.closure.serializer" => 1
    case "spark.dynamicAllocation.enabled" => 2
    case "spark.dynamicAllocation.minExecutors" => 3
    case "spark.dynamicAllocation.maxExecutors" => 4
    case "spark.dynamicAllocation.initialExecutors" => 5
    case "spark.executor.instances" => 3
    case "spark.executor.cores" => 6
    case "spark.executor.memory" => 7
    case _ => 8
  }
}
