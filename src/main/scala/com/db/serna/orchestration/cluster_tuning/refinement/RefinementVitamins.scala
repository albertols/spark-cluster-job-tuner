package com.db.serna.orchestration.cluster_tuning.refinement

import com.db.serna.orchestration.cluster_tuning.Csv

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

/** A computed action to apply to a recipe's Spark configuration. */
sealed trait VitaminBoost {
  def recipeFilename: String
  def description: String
}

final case class MemoryHeapBoost(
  recipeFilename: String,
  originalMemory: String,
  boostedMemory: String,
  boostFactor: Double
) extends VitaminBoost {
  val description: String =
    s"spark.executor.memory: $originalMemory -> $boostedMemory (x$boostFactor heap OOM boost)"
}

// ── Vitamin Trait ───────────────────────────────────────────────────────────

/**
 * A refinement vitamin detects a diagnostic condition from a CSV and applies
 * targeted boosts to affected recipe configurations.
 *
 * Each vitamin declares:
 *  - `name` — human-readable identifier
 *  - `csvFileName` — source CSV file name (for unresolved report)
 *  - `counterKey` — field name injected into clusterConf (e.g. "boostedMemoryHeapJobCount")
 *  - `listKey` — field name for list of boosted recipe names in clusterConf
 *  - `boostFieldKey` — field name added per-recipe (e.g. "appliedMemoryHeapBoostFactor")
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
}

// ── Recipe Resolution ───────────────────────────────────────────────────────

object RecipeResolver {

  /**
   * Strip date+time suffix from a job_id.
   * e.g. "etl-m-dq3-ods-f-gr-garantia-20260411-0438" → "etl-m-dq3-ods-f-gr-garantia"
   */
  private[refinement] def stripJobIdSuffix(jobId: String): String =
    jobId.replaceAll("-\\d{8}-\\d{4}$", "")

  /**
   * Derive a normalised recipe name from a job_id prefix for case-insensitive matching.
   * e.g. "etl-m-dq3-ods-f-gr-garantia" → "ETL_M_DQ3_ODS_F_GR_GARANTIA"
   */
  private[refinement] def normaliseJobPrefix(prefix: String): String =
    prefix.replace('-', '_').toUpperCase

  /**
   * Normalise a recipe filename for case-insensitive matching.
   * e.g. "_ETL_m_DQ3_ODS_F_GR_GARANTIA.json" → "ETL_M_DQ3_ODS_F_GR_GARANTIA"
   */
  private[refinement] def normaliseRecipeName(recipe: String): String =
    recipe.stripPrefix("_").stripSuffix(".json").toUpperCase

  /**
   * Resolve missing recipe_filename for signals using two strategies:
   *
   * 1. **Sibling lookup**: Find another signal with the same job prefix (after stripping
   *    date suffix) that already has a recipe_filename.
   *
   * 2. **Job-id derivation**: Strip date suffix, replace `-` with `_`, and match
   *    case-insensitively against the available recipe names in the cluster.
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
 * Detects Java heap OOM failures from b16_oom_job_driver_exceptions.csv and
 * boosts `spark.executor.memory` for affected recipes by the given factor.
 *
 * Signals with empty recipe_filename are resolved via [[RecipeResolver]]:
 * first by sibling lookup (other rows with same job prefix), then by
 * job-id derivation matched against available recipe names.
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

  def computeBoosts(signals: Seq[VitaminSignal], recipes: Map[String, RecipeConfig]): Seq[VitaminBoost] = {
    val heapSignals = signals.collect { case s: MemoryHeapOomSignal => s }
    val affectedRecipes = heapSignals.map(_.recipeFilename).filter(_.nonEmpty).distinct

    affectedRecipes.flatMap { recipe =>
      recipes.get(recipe).map { rc =>
        val currentMem = rc.sparkOptsMap.getOrElse("spark.executor.memory", "8g")
        val boostedMem = boostMemory(currentMem, boostFactor)
        MemoryHeapBoost(recipe, currentMem, boostedMem, boostFactor)
      }
    }
  }

  def applyBoosts(boosts: Seq[VitaminBoost], recipes: Map[String, RecipeConfig]): Map[String, RecipeConfig] = {
    boosts.foldLeft(recipes) {
      case (cfg, b: MemoryHeapBoost) =>
        cfg.get(b.recipeFilename) match {
          case Some(rc) =>
            val updatedOpts = rc.sparkOptsMap.updated("spark.executor.memory", b.boostedMemory)
            val updatedExtra = rc.extraFields + (boostFieldKey -> b.boostFactor.toString)
            val memGb = SimpleJsonParser.parseMemoryGb(b.boostedMemory)
            val (minExec, maxExec) = extractExecutorCounts(rc)
            cfg.updated(b.recipeFilename, rc.copy(
              sparkOptsMap = updatedOpts,
              totalExecutorMinAllocatedMemoryGb = minExec * memGb,
              totalExecutorMaxAllocatedMemoryGb = maxExec * memGb,
              extraFields = updatedExtra
            ))
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
      val min = opts.get("spark.dynamicAllocation.minExecutors").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(1)
      val max = opts.get("spark.dynamicAllocation.maxExecutors").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(min)
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

  def refine(
    config: TunedClusterConfig,
    vitamins: Seq[RefinementVitamin],
    inputDir: File
  ): RefinementResult = {
    var currentRecipes = config.recipes
    val allBoosts = mutable.ArrayBuffer.empty[VitaminBoost]
    val allUnresolved = mutable.ArrayBuffer.empty[UnresolvedEntry]
    val counters = mutable.LinkedHashMap.empty[String, Int]
    val lists = mutable.LinkedHashMap.empty[String, Seq[String]]

    vitamins.foreach { vitamin =>
      val signals = vitamin.loadSignals(inputDir, config.clusterName)
      val (resolved, unresolved) = RecipeResolver.resolve(signals, currentRecipes.keySet)
      val boosts = vitamin.computeBoosts(resolved, currentRecipes)
      currentRecipes = vitamin.applyBoosts(boosts, currentRecipes)
      allBoosts ++= boosts
      counters(vitamin.counterKey) = boosts.size
      lists(vitamin.listKey) = boosts.map(_.recipeFilename).distinct

      // Track signals that couldn't be resolved to a recipe name
      unresolved.foreach { s =>
        allUnresolved += UnresolvedEntry(
          vitaminName = vitamin.name,
          csvSource = vitamin.csvFileName,
          jobId = s.jobId,
          clusterName = s.clusterName,
          rawRecipeFilename = s.recipeFilename,
          latestDriverLogTs = s match {
            case h: MemoryHeapOomSignal => h.latestDriverLogTs
            case _ => ""
          },
          latestDriverMessage = s match {
            case h: MemoryHeapOomSignal => h.latestDriverMessage
            case _ => ""
          }
        )
      }

      // Track resolved signals whose recipe doesn't exist in the cluster config
      resolved.filter { s =>
        s.recipeFilename.nonEmpty && !currentRecipes.contains(s.recipeFilename)
      }.foreach { s =>
        allUnresolved += UnresolvedEntry(
          vitaminName = vitamin.name,
          csvSource = vitamin.csvFileName,
          jobId = s.jobId,
          clusterName = s.clusterName,
          rawRecipeFilename = s.recipeFilename,
          latestDriverLogTs = s match {
            case h: MemoryHeapOomSignal => h.latestDriverLogTs
            case _ => ""
          },
          latestDriverMessage = s match {
            case h: MemoryHeapOomSignal => h.latestDriverMessage
            case _ => ""
          }
        )
      }
    }

    RefinementResult(
      config.clusterName, config, currentRecipes, allBoosts.toSeq,
      counters.toMap, lists.toMap, allUnresolved.toSeq
    )
  }

  /** Rebuild the refined JSON string from a RefinementResult. */
  def toRefinedJson(result: RefinementResult): String = {
    import com.db.serna.orchestration.cluster_tuning.Json
    import Json._

    // Rebuild clusterConf fields preserving original order, then append boost counters + lists.
    // Filter out any existing boost keys from prior runs to avoid duplicates.
    val boostKeys = result.boostCounters.keySet ++ result.boostLists.keySet
    val clusterFields: Seq[(String, String)] = result.originalConfig.clusterConfFields
      .filterNot { case (k, _) => boostKeys.contains(k) }
      .map {
        case (k, v) =>
          if (scala.util.Try(v.toLong).isSuccess) k -> num(v.toLong)
          else if (scala.util.Try(v.toDouble).isSuccess) k -> num(v.toDouble)
          else k -> str(v)
      }

    val counterFields: Seq[(String, String)] = result.boostCounters.toSeq.map {
      case (k, v) => k -> num(v)
    }

    val listFields: Seq[(String, String)] = result.boostLists.toSeq.map {
      case (k, recipes) => k -> arr(recipes.map(str): _*)
    }

    val clusterConf = obj(
      result.clusterName -> obj((clusterFields ++ counterFields ++ listFields): _*)
    )

    // Rebuild recipeSparkConf preserving original recipe order
    val recipes: Seq[(String, String)] = result.originalConfig.recipeOrder.map { recipeName =>
      val rc = result.refinedRecipes.getOrElse(recipeName, result.originalConfig.recipes(recipeName))

      val boostFields: Seq[(String, String)] = rc.extraFields.toSeq.map {
        case (k, v) =>
          if (scala.util.Try(v.toDouble).isSuccess) k -> num(v.toDouble)
          else k -> str(v)
      }

      val sparkOptsFields: Seq[(String, String)] = rc.sparkOptsMap.toSeq.sortBy {
        case (k, _) => sparkOptsOrder(k)
      }.map { case (k, v) => k -> str(v) }

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
    case "spark.serializer"                        => 0
    case "spark.closure.serializer"                => 1
    case "spark.dynamicAllocation.enabled"         => 2
    case "spark.dynamicAllocation.minExecutors"    => 3
    case "spark.dynamicAllocation.maxExecutors"    => 4
    case "spark.dynamicAllocation.initialExecutors" => 5
    case "spark.executor.instances"                => 3
    case "spark.executor.cores"                    => 6
    case "spark.executor.memory"                   => 7
    case _                                         => 8
  }
}
