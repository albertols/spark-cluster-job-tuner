package com.db.serna.orchestration.cluster_tuning.single

import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.{ceil, floor, min}
import scala.util.Try

// Domain models
final case class MachineType(name: String, cores: Int, memoryGb: Int)

/**
 * Candidate machine types considered by the tuner when computing cost-effective cluster shapes.
 * This catalog now includes Dataproc-compatible general-purpose machine families (Dataproc): [[ https://docs.cloud.google.com/compute/docs/general-purpose-machines ]]
 * N1, N2, N2D, E2, C3, C4, N4, and N4D, with standard, highmem, and highcpu variants. [[ https://docs.cloud.google.com/dataproc/docs/concepts/compute/supported-machine-types ]]
 *
 * Notes:
 * - Memory per vCPU for most families is an integer multiple (e.g., 4/8/2 GB). We round to the nearest whole GB to keep
 * MachineType.memoryGb as Int. This keeps sizing logic simple and deterministic.
 * - The lists of vCPU counts are commonly-available shapes for each family; adjust if your region
 * supports additional sizes.
 */
object MachineCatalog {

  private def roundMemGbPerVcpu(vcpus: Int, gbPerVcpu: Double): Int =
    math.round(vcpus * gbPerVcpu).toInt

  private def gen(family: String, variant: String, vcpus: List[Int], gbPerVcpu: Double): List[MachineType] =
    vcpus.map { v =>
      val mem = roundMemGbPerVcpu(v, gbPerVcpu)
      MachineType(s"$family-$variant-$v", v, mem)
    }

  // E2 (general-purpose, cost-optimized)
  private val e2Standard = gen("e2", "standard", List(2, 4, 8, 16, 32), 4.0)
  private val e2Highmem = gen("e2", "highmem", List(2, 4, 8, 16), 8.0)
  private val e2Highcpu = gen("e2", "highcpu", List(2, 4, 8, 16, 32), 2.0)

  // N2 (newer general-purpose, Intel)
  private val n2Standard = gen("n2", "standard", List(2, 4, 8, 16, 32, 48, 64, 80, 96), 4.0)
  private val n2Highmem = gen("n2", "highmem", List(2, 4, 8, 16, 32, 48, 64, 80), 8.0)
  private val n2Highcpu = gen("n2", "highcpu", List(2, 4, 8, 16, 32, 48, 64, 80, 96), 2.0)

  // N2D (newer general-purpose, AMD)
  private val n2dStandard = gen("n2d", "standard", List(2, 4, 8, 16, 32, 48, 64, 80, 96, 128, 224), 4.0)
  private val n2dHighmem = gen("n2d", "highmem", List(2, 4, 8, 16, 32, 48, 64, 80, 96), 8.0)
  private val n2dHighcpu = gen("n2d", "highcpu", List(2, 4, 8, 16, 32, 48, 64, 80, 96, 128, 224), 2.0)

  // C3 (compute-optimized, Dataproc supported)
  private val c3Standard = gen("c3", "standard", List(4, 8, 22, 44, 88, 177, 192), 4.0)
  private val c3Highmem = gen("c3", "highmem", List(4, 8, 22, 44, 88, 177, 192), 8.0)
  private val c3Highcpu = gen("c3", "highcpu", List(4, 8, 22, 44, 88, 177, 192), 2.0)

  // C4 (next-gen compute-optimized)
  private val c4Standard = gen("c4", "standard", List(4, 8, 16, 24, 32, 48, 96, 144, 192, 288), 4.0)
  private val c4Highmem = gen("c4", "highmem", List(4, 8, 16, 24, 32, 48, 96, 144, 192, 288), 8.0)
  private val c4Highcpu = gen("c4", "highcpu", List(4, 8, 16, 24, 32, 48, 96, 144, 192, 288), 2.0)

  // N4 / N4D (next-gen general-purpose, Intel/AMD)
  private val n4Standard = gen("n4", "standard", List(2, 4, 8, 16, 32, 48, 64, 80), 4.0)
  private val n4Highmem = gen("n4", "highmem", List(2, 4, 8, 16, 32, 48, 64, 80), 8.0)
  private val n4Highcpu = gen("n4", "highcpu", List(2, 4, 8, 16, 32, 48, 64, 80), 2.0)
  private val n4dStandard = gen("n4d", "standard", List(2, 4, 8, 16, 32, 48, 64, 80, 96), 4.0)
  private val n4dHighmem = gen("n4d", "highmem", List(2, 4, 8, 16, 32, 48, 64, 80, 96), 8.0)
  private val n4dHighcpu = gen("n4d", "highcpu", List(2, 4, 8, 16, 32, 48, 64, 80, 96), 2.0)

  // Full default candidate catalog (all families — filtering is done by MachineSelectionPreference)
  val defaults: List[MachineType] =
    e2Standard ++ e2Highmem ++ e2Highcpu ++
      n2Standard ++ n2Highmem ++ n2Highcpu ++
      n2dStandard ++ n2dHighmem ++ n2dHighcpu ++
      c3Standard ++ c3Highmem ++ c3Highcpu ++
      c4Standard ++ c4Highmem ++ c4Highcpu ++
      n4Standard ++ n4Highmem ++ n4Highcpu ++
      n4dStandard ++ n4dHighmem ++ n4dHighcpu

  def byName(name: String): Option[MachineType] = defaults.find(_.name == name)
}

// Approximate hourly prices for europe-west3 (EUR). Validate for your billing account.
object PriceCatalog {
  private final case class Rate(vCpu: Double, memGb: Double)

  private val familyRates: Map[String, Rate] = Map(
    "e2" -> Rate(vCpu = 0.0336, memGb = 0.0042),
    "n2" -> Rate(vCpu = 0.0470, memGb = 0.0065),
    "n2d" -> Rate(vCpu = 0.0430, memGb = 0.0060),
    "n1" -> Rate(vCpu = 0.0500, memGb = 0.0068),
    "c3" -> Rate(vCpu = 0.0600, memGb = 0.0050),
    "c4" -> Rate(vCpu = 0.0650, memGb = 0.0055),
    "n4" -> Rate(vCpu = 0.0490, memGb = 0.0063),
    "n4d" -> Rate(vCpu = 0.0460, memGb = 0.0061)
  )

  private def familyOf(name: String): String =
    name.takeWhile(_ != '-')

  private def priceFor(mt: MachineType): Double = {
    val fam = familyOf(mt.name)
    val rate = familyRates.getOrElse(fam, familyRates("e2"))
    rate.vCpu * mt.cores + rate.memGb * mt.memoryGb
  }

  // Precompute a price map for all catalog machine types
  val pricePerHourEUR: Map[String, Double] =
    MachineCatalog.defaults.map(mt => mt.name -> priceFor(mt)).toMap
}

// Metrics loaded from CSV (one RecipeMetrics per (cluster, recipe))
final case class RecipeMetrics(
                                cluster: String,
                                recipe: String,
                                avgExecutorsPerJob: Double,
                                p95RunMaxExecutors: Double,
                                avgJobDurationMs: Double,
                                p95JobDurationMs: Double,
                                runs: Long,
                                secondsAtCap: Option[Long],
                                runsReachingCap: Option[Long],
                                totalRuns: Option[Long],
                                fractionReachingCap: Option[Double],
                                maxConcurrentJobs: Option[Int]
                              )

final case class TuningPolicy(
                               executorCores: Int,
                               executorMemoryGb: Int,
                               memoryOverheadRatio: Double,
                               osAndDaemonsReserveGb: Int,
                               defaultWorker: MachineType,
                               defaultMaster: MachineType,
                               manualInstancesFrom: String,
                               minExecutorInstances: Int,
                               daMinFrom: String,
                               daInitialEqualsMin: Boolean,
                               capHitBoostPct: Double,
                               capHitThreshold: Double,
                               preferMaxWorkers: Int,
                               perWorkerPenaltyPct: Double,
                               // Multi-objective selection weights
                               parallelismWeight: Double = 0.0,
                               costWeight: Double = 0.4,
                               concurrencyBufferPct: Double = 0.25,
                               preferEightCoreExecutors: Boolean = true,
                               sufficiencyWeight: Double = 0.5,
                               utilizationWeight: Double = 0.3,
                               workerPenaltyWeight: Double = 0.3,
                               oversizePenaltyWeight: Double = 0.2
                             )

/**
 * Represents the chosen cluster shape after cost-aware machine selection.
 * - executorsPerWorker: capacity per worker node given cores/memory constraints.
 * - maxExecutorsSupported: total executor slots available at the chosen worker count.
 */
final case class ClusterPlan(
                              clusterName: String,
                              masterMachineType: MachineType,
                              workerMachineType: MachineType,
                              workers: Int,
                              executorsPerWorker: Int,
                              maxExecutorsSupported: Int
                            )

// Output objects for recipe-level tuning
final case class RecipePlanManual(
                                   recipe: String,
                                   sparkExecutorInstances: Int,
                                   sparkExecutorCores: Int,
                                   sparkExecutorMemoryGb: Int
                                 )

final case class RecipePlanDA(
                               recipe: String,
                               minExecutors: Int,
                               maxExecutors: Int,
                               initialExecutors: Int,
                               sparkExecutorCores: Int,
                               sparkExecutorMemoryGb: Int
                             )

/**
 * A compact representation used in _clusters-summary.csv/json to visualize overall cluster activity and cost.
 * Includes dagId resolved from _dag_cluster-relationship-map.csv; if missing, uses "UNKNOWN_DAG_ID".
 */
final case class ClusterSummary(
                                 clusterName: String,
                                 dagId: String,
                                 noOfJobs: Int,
                                 numOfWorkers: Int,
                                 workerMachineType: String,
                                 masterMachineType: String,
                                 totalActiveMinutes: Double,
                                 estimatedCostEur: Double,
                                 timerName: String,
                                 timerTime: String
                               )

// Minimal CSV loader
object Csv {
  def parse(file: File): Vector[Map[String, String]] = {
    val src = Source.fromFile(file)
    try {
      val lines = src.getLines().toVector.filterNot(_.trim.isEmpty)
      if (lines.isEmpty) return Vector.empty
      val header = lines.head.split(",", -1).map(_.trim)
      lines.tail.map { row =>
        val cells = row.split(",", -1).map(_.trim)
        header.zipAll(cells, "", "").toMap
      }
    } finally src.close()
  }

  def toDouble(s: String): Option[Double] = Try(s.trim).toOption.flatMap(x => Try(x.toDouble).toOption)

  def toLong(s: String): Option[Long] = Try(s.trim).toOption.flatMap(x => Try(x.toLong).toOption)

  def toInt(s: String): Option[Int] = Try(s.trim).toOption.flatMap(x => Try(x.toInt).toOption)
}

// Tiny JSON builder + pretty printing
object Json {
  private def esc(s: String): String =
    s.flatMap {
      case '"' => "\\\""
      case '\\' => "\\\\"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c => c.toString
    }

  def obj(fields: (String, String)*): String = {
    val inner = fields.map { case (k, v) => s""""${esc(k)}": $v""" }.mkString(",")
    s"{$inner}"
  }

  def arr(elems: String*): String = elems.mkString("[", ",", "]")

  def str(s: String): String = s""""${esc(s)}""""

  def num(n: Any): String = n.toString

  def bool(b: Boolean): String = if (b) "true" else "false"

  def nul: String = "null"

  def pretty(raw: String, indentSize: Int = 2): String = {
    val sb = new StringBuilder
    var indent = 0
    var inString = false
    var i = 0
    while (i < raw.length) {
      val c = raw.charAt(i)
      c match {
        case '"' =>
          sb.append(c)
          val escaped = i > 0 && raw.charAt(i - 1) == '\\'
          if (!escaped) inString = !inString
        case '{' | '[' if !inString =>
          sb.append(c).append('\n')
          indent += 1
          sb.append(" " * (indent * indentSize))
        case '}' | ']' if !inString =>
          sb.append('\n')
          indent = math.max(0, indent - 1)
          sb.append(" " * (indent * indentSize)).append(c)
        case ',' if !inString =>
          sb.append(c).append('\n').append(" " * (indent * indentSize))
        case _ =>
          sb.append(c)
      }
      i += 1
    }
    sb.toString()
  }
}

// Sizing helpers: compute per-worker executor capacity given cores/memory and overheads.
object Sizing {
  def memoryOverheadGb(executorMemoryGb: Int, ratio: Double): Double = math.max(0.384, executorMemoryGb * ratio)

  def executorsPerWorker(worker: MachineType, executorCores: Int, executorMemoryGb: Int, memOverheadRatio: Double, reserveGb: Int): Int = {
    val byCores: Int = floor(worker.cores.toDouble / executorCores).toInt
    val usableMem: Double = math.max(0.0, worker.memoryGb - reserveGb)
    val overhead: Double = memoryOverheadGb(executorMemoryGb, memOverheadRatio)
    val perExecMem: Double = executorMemoryGb + overhead
    val byMem: Int = floor(usableMem / perExecMem).toInt
    math.max(1, math.min(byCores, byMem))
  }

  def clamp(v: Int, lo: Int, hi: Int): Int = math.max(lo, math.min(v, hi))

  def roundUp(v: Double): Int = math.ceil(v).toInt
}

/**
 * [[ClusterMachineAndRecipeTuner]]
 */
object ClusterMachineAndRecipeTuner {

  private val logger = LoggerFactory.getLogger(getClass)

  // Path to DAG-to-cluster relationship map (CSV with headers: DAG_ID,CLUSTER_NAME)
  private val DagClusterMapPath: File = new File("src/main/resources/composer/dwh/config/_dag_cluster-relationship-map.csv")

  // Path to DAG/Cluster creation time map (CSV with headers: DAG_NAME,CLUSTER_NAME,TIMER_NAME,TIME,RUN_SINGLE_RECIPE_NUMBER)
  private val DagClusterCreationTimePath: File = new File("src/main/resources/composer/dwh/config/_dag_cluster_creation_time.csv")

  // Convert "YYYY_MM_DD" (CLI arg) into tuner_version "YYYY_DD_MM"
  private[cluster_tuning] def toTunerVersion(argDate: String): String = {
    val parts = argDate.split("_")
    if (parts.length == 3) s"${parts(0)}_${parts(2)}_${parts(1)}" else argDate
  }

  /**
   * - useFlattened: true => read b13; false => read individual CSVs.
   * - inputDir/outputDir: where CSVs are read and JSON/CSV outputs are written, determined dynamically using the provided `date`.
   * - date: Passed as an argument in the format `YYYY_MM_DD`.
   * - defaultMaster/defaultWorker: fallback machine types if selection fails (should rarely happen).
   */
  final case class Config(
                           useFlattened: Boolean,
                           date: String, // Provided date in `YYYY_MM_DD` format
                           inputDir: File,
                           outputDir: File,
                           defaultMaster: MachineType,
                           defaultWorker: MachineType
                         )

  object Config {
    // Companion object for Config to provide a helper to construct Config dynamically
    def apply(useFlattened: Boolean, date: String): Config = {
      // Ensure the date format is valid before creating directories
      require(date.matches("\\d{4}_\\d{2}_\\d{2}"), "The date must be in YYYY_MM_DD format (e.g., 2025_12_20).")

      // Dynamically set input and output directories based on the provided `date`
      val inputDir = new File(s"src/main/resources/composer/dwh/config/cluster_tuning/inputs/$date")
      val outputDir = new File(s"src/main/resources/composer/dwh/config/cluster_tuning/outputs/$date")

      // Return the Config instance
      Config(
        useFlattened = useFlattened,
        date = date,
        inputDir = inputDir,
        outputDir = outputDir,
        defaultMaster = MachineCatalog.byName("e2-standard-8").get,
        defaultWorker = MachineCatalog.byName("e2-standard-8").get
      )
    }
  }

  def loadMetrics(cfg: Config): Map[(String, String), RecipeMetrics] = {
    logger.info(s"Loading metrics. useFlattened=${cfg.useFlattened}, inputDir=${cfg.inputDir.getPath}")
    if (cfg.useFlattened) loadFlattened(cfg)
    else loadFromIndividualCSVs(cfg)
  }

  /**
   * Loads DAG_ID per cluster_name mapping from CSV.
   * Returns Map[cluster_name -> dag_id]. If file missing or malformed rows, returns empty map.
   */
  private[cluster_tuning] def loadDagClusterRelationshipMap(): Map[String, String] = {
    if (!DagClusterMapPath.exists()) {
      logger.warn(s"DAG-to-cluster relationship CSV not found: ${DagClusterMapPath.getPath}. All clusters will use dag_id=UNKNOWN_DAG_ID.")
      Map.empty
    } else {
      val rows = Csv.parse(DagClusterMapPath)
      val pairs = rows.flatMap { r =>
        (r.get("CLUSTER_NAME"), r.get("DAG_ID")) match {
          case (Some(cluster), Some(dag)) if cluster.nonEmpty && dag.nonEmpty => Some(cluster -> dag)
          case _ =>
            logger.warn(s"Skipping malformed row in ${DagClusterMapPath.getName}: $r")
            None
        }
      }
      val dedup = pairs.groupBy(_._1).map { case (cluster, values) =>
        if (values.map(_._2).distinct.size > 1) {
          logger.warn(s"Cluster '$cluster' appears with multiple DAG_IDs in mapping. Using the first encountered.")
        }
        cluster -> values.head._2
      }
      logger.info(s"Loaded DAG-to-cluster mappings: ${dedup.size} entries from ${DagClusterMapPath.getPath}")
      dedup
    }
  }

  /**
   * Loads TIMER_NAME and TIME per CLUSTER_NAME from CSV _dag_cluster_creation_time.csv.
   * Returns Map[cluster_name -> (timer_name, time)]. If file missing or malformed rows, returns empty map.
   * If multiple rows exist for the same cluster, the first encountered is used with a warning.
   */
  private[cluster_tuning] def loadDagClusterCreationTimeMap(): Map[String, (String, String)] = {
    if (!DagClusterCreationTimePath.exists()) {
      logger.warn(s"TIMER mapping CSV not found: ${DagClusterCreationTimePath.getPath}. Defaulting TIMER_NAME=ZERO_TIMER, TIMER_TIME=00:00.")
      Map.empty
    } else {
      val rows = Csv.parse(DagClusterCreationTimePath)
      val pairs = rows.flatMap { r =>
        (r.get("CLUSTER_NAME"), r.get("TIMER_NAME"), r.get("TIME")) match {
          case (Some(cluster), Some(timer), Some(time))
            if cluster.nonEmpty && timer.nonEmpty && time.nonEmpty =>
            Some(cluster -> (timer, time))
          case _ =>
            logger.warn(s"Skipping malformed row in ${DagClusterCreationTimePath.getName}: $r")
            None
        }
      }
      val dedup = pairs.groupBy(_._1).map { case (cluster, values) =>
        if (values.map(_._2).distinct.size > 1) {
          logger.warn(s"Cluster '$cluster' appears with multiple TIMER_NAME/TIME entries. Using the first encountered.")
        }
        cluster -> values.head._2
      }
      logger.info(s"Loaded TIMER_NAME/TIME mappings: ${dedup.size} entries from ${DagClusterCreationTimePath.getPath}")
      dedup
    }
  }

  /**
   * - Reads b13_recommendations_inputs_per_recipe_per_cluster.csv or b15_recommendation_inputs_per_recipe_per_cluster.csv.
   * - Produces a map keyed by (cluster_name, recipe_filename).
   */
  private def loadFlattened(cfg: Config): Map[(String, String), RecipeMetrics] = {
    val f13: File = new File(cfg.inputDir, "b13_recommendations_inputs_per_recipe_per_cluster.csv")
    val f15: File = new File(cfg.inputDir, "b15_recommendation_inputs_per_recipe_per_cluster.csv")
    val f =
      if (f13.exists()) {
        logger.info(s"Using flattened CSV: ${f13.getPath}"); f13
      }
      else if (f15.exists()) {
        logger.info(s"Using flattened CSV: ${f15.getPath}"); f15
      }
      else {
        logger.warn(s"No flattened CSV found at ${f13.getPath} or ${f15.getPath}.")
        return Map.empty
      }

    val rows: Vector[Map[String, String]] = Csv.parse(f)
    logger.info(s"Parsed ${rows.size} rows from ${f.getName}")
    // Be permissive with NULL metric fields so flattened ingestion matches the behavior of
    // loadFromIndividualCSVs (which applies sane defaults when a metric is missing). Only
    // cluster_name and recipe_filename are required — everything else falls back to the same
    // defaults used in the individual-CSV path.
    rows.flatMap { r =>
      (for {
        cluster <- r.get("cluster_name").filter(_.nonEmpty)
        recipe <- r.get("recipe_filename").filter(_.nonEmpty)
      } yield {
        val avgExec = r.get("avg_executors_per_job").flatMap(Csv.toDouble).getOrElse(1.0)
        val p95Max = r.get("p95_run_max_executors").flatMap(Csv.toDouble).getOrElse(1.0)
        val avgDur = r.get("avg_job_duration_ms").flatMap(Csv.toDouble).getOrElse(0.0)
        val p95Dur = r.get("p95_job_duration_ms").flatMap(Csv.toDouble).getOrElse(avgDur)
        val runs = r.get("runs").flatMap(Csv.toLong).getOrElse(0L)
        val secCap = r.get("seconds_at_cap").flatMap(Csv.toLong)
        val rAtCap = r.get("runs_reaching_cap").flatMap(Csv.toLong)
        val tRuns = r.get("total_runs").flatMap(Csv.toLong)
        val fracCap = r.get("fraction_reaching_cap").flatMap(Csv.toDouble)
        val conc = r.get("max_concurrent_jobs").flatMap(Csv.toInt).orElse(Some(1))
        (cluster -> recipe) -> RecipeMetrics(
          cluster, recipe,
          avgExec, p95Max, avgDur, p95Dur, runs,
          secCap, rAtCap, tRuns, fracCap, conc
        )
      }) orElse {
        logger.warn(s"Skipping row due to missing cluster_name or recipe_filename: $r")
        None
      }
    }.toMap
  }

  /**
   * - Reads b1/b12/b3/b8/b5/b11 CSVs and merges them by (cluster_name, recipe_filename).
   * - Missing optional files/fields are tolerated with sane defaults.
   */
  private def loadFromIndividualCSVs(cfg: Config): Map[(String, String), RecipeMetrics] = {
    def readCsv(name: String): Vector[Map[String, String]] = {
      val f = new File(cfg.inputDir, name)
      if (!f.exists()) {
        logger.warn(s"CSV not found: ${f.getPath}"); Vector.empty
      }
      else {
        val rows = Csv.parse(f)
        logger.info(s"Read ${rows.size} rows from ${f.getName}")
        rows
      }
    }

    val b1 = readCsv("b1_average_number_of_executors_per_job_by_cluster.csv").map { r =>
      val k = (r.getOrElse("cluster_name", ""), r.getOrElse("recipe_filename", ""))
      k -> r.get("avg_executors_per_job").flatMap(Csv.toDouble)
    }.toMap

    val b12 = readCsv("b12_p95_max_executors_per_recipe_per_cluster.csv").map { r =>
      val k = (r.getOrElse("cluster_name", ""), r.getOrElse("recipe_filename", ""))
      k -> (r.get("p95_run_max_executors").flatMap(Csv.toDouble), r.get("avg_run_max_executors").flatMap(Csv.toDouble), r.get("runs").flatMap(Csv.toLong))
    }.toMap

    val b3 = readCsv("b3_average_recipefilename_per_cluster.csv").map { r =>
      val k = (r.getOrElse("cluster_name", ""), r.getOrElse("recipe_filename", ""))
      k -> r.get("avg_job_duration_ms").flatMap(Csv.toDouble)
    }.toMap

    val b8 = readCsv("b8_P95_job_duration_per_recipe_per_cluster.csv").map { r =>
      val k = (r.getOrElse("cluster_name", ""), r.getOrElse("recipe_filename", ""))
      k -> (r.get("p95_job_duration_ms").flatMap(Csv.toDouble), r.get("runs").flatMap(Csv.toLong))
    }.toMap

    val b5 = readCsv("b5_a_times_job_reaches_max_executor_per_cluster.csv").map { r =>
      val k = (r.getOrElse("cluster_name", ""), r.getOrElse("recipe_filename", ""))
      k -> (
        r.get("seconds_at_cap").flatMap(Csv.toLong),
        r.get("runs_reaching_cap").flatMap(Csv.toLong),
        r.get("total_runs").flatMap(Csv.toLong),
        r.get("fraction_reaching_cap").flatMap(Csv.toDouble)
      )
    }.toMap

    val b11 = readCsv("b11_max_concurrent_jobs_per_cluster_in_window.csv").map { r =>
      val cluster = r.getOrElse("cluster_name", "")
      cluster -> r.get("max_concurrent_jobs").flatMap(Csv.toInt)
    }.toMap

    val keys = (b1.keySet ++ b12.keySet ++ b3.keySet ++ b8.keySet ++ b5.keySet)
    if (keys.isEmpty) {
      logger.warn("No metrics found across individual CSVs. Check input directory and filenames.")
      return Map.empty
    }

    val merged = keys.map { k =>
      val (cluster, recipe) = k
      val avgExec = b1.getOrElse(k, None).getOrElse(1.0)
      val (p95Max, _avgMax, runs12) = b12.getOrElse(k, (Some(1.0), None, Some(0L)))
      val avgDur = b3.getOrElse(k, Some(0.0)).getOrElse(0.0)
      val (p95Dur, runs8) = b8.getOrElse(k, (Some(0.0), Some(0L)))
      val (secCap, rAtCap, tRuns, fracCap) = b5.getOrElse(k, (None, None, None, None))
      val conc = b11.getOrElse(cluster, Some(1))

      k -> RecipeMetrics(
        cluster = cluster,
        recipe = recipe,
        avgExecutorsPerJob = avgExec,
        p95RunMaxExecutors = p95Max.getOrElse(1.0),
        avgJobDurationMs = avgDur,
        p95JobDurationMs = p95Dur.getOrElse(avgDur),
        runs = runs12.orElse(runs8).getOrElse(0L),
        secondsAtCap = secCap,
        runsReachingCap = rAtCap,
        totalRuns = tRuns,
        fractionReachingCap = fracCap,
        maxConcurrentJobs = conc
      )
    }.toMap

    logger.info(s"Merged metrics for ${merged.size} (cluster,recipe) pairs.")
    merged
  }

  private[cluster_tuning] def clusterActiveMinutes(metrics: Iterable[RecipeMetrics]): Double =
    metrics.map(m => m.avgJobDurationMs / 60000.0).sum

  private[cluster_tuning] def hourlyPrice(machine: MachineType): Double = PriceCatalog.pricePerHourEUR.getOrElse(machine.name, 0.0)

  private def penalizedHourlyClusterCost(worker: MachineType, workers: Int, master: MachineType, policy: TuningPolicy): Double = {
    val base: Double = hourlyPrice(worker) * workers + hourlyPrice(master)
    val penalty: Double = if (workers > policy.preferMaxWorkers)
      base * policy.perWorkerPenaltyPct * (workers - policy.preferMaxWorkers)
    else 0.0
    base + penalty
  }

  // Helper: extract variant ("standard"|"highmem"|"highcpu") from machine type name
  private def variantOf(machineName: String): String = {
    val parts = machineName.split("-")
    if (parts.length >= 2) parts(1) else "standard"
  }

  private def familyOf(machineName: String): String = machineName.takeWhile(_ != '-')

  // Helper: recommended executor memory (GB) given worker type and executor cores.
  private def recommendedExecutorMemoryGb(worker: MachineType, executorCores: Int, policy: TuningPolicy): Int = {
    val variant = variantOf(worker.name).toLowerCase

    // Adopt 1/2/4 GB per core by variant (highcpu/standard/highmem)
    val memPerCore = variant match {
      case "highcpu" => 1
      case "highmem" => 4
      case _ => 2 // standard
    }

    val base = memPerCore * executorCores

    // Respect Spark overhead and OS/daemons reserve; ensure at least one executor can fit
    val overhead = Sizing.memoryOverheadGb(base, policy.memoryOverheadRatio)
    val usableMem = math.max(1.0, worker.memoryGb - policy.osAndDaemonsReserveGb)
    val maxAllowed = math.floor(math.max(1.0, usableMem - overhead)).toInt

    // Floor to avoid starving memory-heavy workloads; cap to ~32 GB
    val lowerBound = policy.executorMemoryGb // e.g., 8GB
    val upperCap = 32 // GB

    val proposed = math.min(base, maxAllowed)
    val withFloor = math.max(lowerBound, proposed)
    math.max(1, math.min(withFloor, math.min(maxAllowed, upperCap)))
  }

  /**
   * Multi-objective machine selection that:
   * - Filters candidates by MachineSelectionPreference (excluded families, allowed families).
   * - Applies a score bonus for machines matching preferredCores (horizontal scalability).
   * - Uses QuotaTracker to skip over-quota candidates (soft; falls back if all exhausted).
   * - Tie-breaks within equal scores using family priority (N2 > N2D > E2 > C3 > C4).
   */
  private def chooseMachines(
    clusterName: String,
    requiredSlotsRaw: Double,
    policy: TuningPolicy,
    quotaTracker: QuotaTracker,
    pref: MachineSelectionPreference
  ): (MachineType, MachineType, Int, Int) = {
    val corePref: Int = if (policy.preferEightCoreExecutors) 8 else policy.executorCores
    val reqSlots: Double = math.max(1.0, requiredSlotsRaw * (1.0 + policy.concurrencyBufferPct))

    // Filter by preference: excluded families, allowed families, and core-count window.
    // minCores/maxCores enforce the 32-core sweet spot (horizontal scale, quota-safe):
    //   - tiny machines (2–16 cores) → too many workers, quota spikes on auto-scale
    //   - giant machines (64–224 cores) → fragile, concentrates quota on a single node
    val candidates: List[MachineType] = MachineCatalog.defaults
      .filterNot(m => pref.excludedFamilies.contains(familyOf(m.name)))
      .filter(m => pref.allowedFamilies.contains(familyOf(m.name)))
      .filter(m => m.cores >= pref.minCores && m.cores <= pref.maxCores)

    val evaluated = candidates.map { w =>
      val epw: Int = Sizing.executorsPerWorker(w, corePref, policy.executorMemoryGb,
        policy.memoryOverheadRatio, policy.osAndDaemonsReserveGb)
      val rawWorkers: Int = math.max(2, ceil(reqSlots / math.max(1, epw)).toInt)
      // C3/C4: cap worker count to honour small quota (c3c4MaxWorkers, default 13).
      // This keeps max nodes ≤ 14 (13 workers + 1 master) = ~416–572 cores depending on shape.
      val workersNeeded: Int =
        if ((familyOf(w.name) == "c3" || familyOf(w.name) == "c4") && pref.c3c4MaxWorkers > 0)
          math.min(rawWorkers, pref.c3c4MaxWorkers)
        else rawWorkers
      val masterType: MachineType = w
      val capacitySlots: Int = epw * workersNeeded
      val totalCores: Int = w.cores * workersNeeded
      val desiredCores: Double = corePref * reqSlots
      val coreSufficiency: Double = math.min(1.0, totalCores / math.max(1.0, desiredCores))
      val utilization: Double = math.min(1.0, (corePref * reqSlots) / math.max(1.0, totalCores))
      val cost: Double = penalizedHourlyClusterCost(w, workersNeeded, masterType, policy)
      val oversizeRatio: Double = totalCores / math.max(1.0, desiredCores)
      val capacityShortfall: Double = math.max(0.0, reqSlots - capacitySlots.toDouble)
      val quotaOk: Boolean = quotaTracker.withinQuota(w, workersNeeded, pref)
      (w, masterType, workersNeeded, epw, cost, totalCores, capacitySlots,
       coreSufficiency, utilization, oversizeRatio, capacityShortfall, quotaOk)
    }

    val minCost = evaluated.map(_._5).min
    val maxCost = evaluated.map(_._5).max
    val costRange = math.max(1e-9, maxCost - minCost)
    val minWorkers = evaluated.map(_._3).min
    val maxWorkers = evaluated.map(_._3).max
    val workerRange = math.max(1e-9, maxWorkers - minWorkers)

    val scored = evaluated.map { case (w, m, workers, epw, cost, totalCores, capacitySlots,
                                       coreSuff, util, oversize, shortfall, quotaOk) =>
      val costNorm = (cost - minCost) / costRange
      val workersNorm = (workers - minWorkers).toDouble / workerRange
      val capacityPenalty = if (shortfall > 0) 100.0 * (shortfall / reqSlots) else 0.0
      val oversizePenalty = if (oversize > 1.5) policy.oversizePenaltyWeight * (oversize - 1.5) else 0.0
      // Bonus for preferred core count (e.g. 32-core machines for horizontal scalability)
      val preferredCoreBonus = if (w.cores == pref.preferredCores) -0.10 else 0.0
      // Dynamic quota-balance penalty: rises as a family's proportional usage (usedCores/quota)
      // increases, naturally distributing allocations across N2/N2D/E2 in ratio to quota limits.
      // At equal proportional fill (e.g. N2=50%, N2D=50%, E2=50%) all penalties are equal, so
      // cost decides the winner.  As one family overflows its quota, its pressure > 1.0 pushes
      // the scorer toward less-pressured families.
      val quotaBalancePenalty = pref.familyPriorityWeight * quotaTracker.quotaPressure(familyOf(w.name))
      val score = policy.costWeight * costNorm +
        policy.workerPenaltyWeight * workersNorm -
        policy.sufficiencyWeight * coreSuff -
        policy.utilizationWeight * util +
        capacityPenalty + oversizePenalty +
        preferredCoreBonus + quotaBalancePenalty
      (w, m, workers, epw, cost, totalCores, capacitySlots, score, quotaOk)
    }

    // Admission pools — applied in priority order:
    //   1. soft-admissible: passes both hard constraints AND core quota (preferred)
    //   2. hard-admissible: passes hard constraints only (excluded families, C3/C4 caps) — used
    //      when core quota is exhausted but we must still respect cluster-count caps
    //   3. full pool: last resort, logged as warning (should only happen with severely under-
    //      provisioned quotas relative to fleet size)
    val admissible     = scored.filter(_._9)   // withinQuota (soft)
    val hardAdmissible = scored.filterNot(t => quotaTracker.isHardBlocked(t._1, pref))
    val pool = if (admissible.nonEmpty) admissible
               else if (hardAdmissible.nonEmpty) {
                 logger.warn(s"Cluster '$clusterName': core quota exhausted for all families; selecting within hard constraints")
                 hardAdmissible
               } else {
                 logger.warn(s"Cluster '$clusterName': all constraints exhausted; using best available (check quotas)")
                 scored
               }

    // Sort by score, then by family priority as tie-breaker
    val best = pool.sortBy { case (w, _, _, _, _, _, _, score, _) =>
      (score, pref.familyPriority.getOrElse(familyOf(w.name), 99).toDouble)
    }.head

    logger.info(f"Cluster '$clusterName' selection: reqSlotsBuffered=$reqSlots%.2f => " +
      s"worker=${best._1.name}, epw=${best._4}, workers=${best._3}, totalCores=${best._6}, " +
      f"hourlyCost≈${best._5}%.4f EUR, score=${best._8}%.4f")
    (best._1, best._2, best._3, best._4)
  }

  def planCluster(
    clusterName: String,
    metrics: Iterable[RecipeMetrics],
    policy: TuningPolicy,
    quotaTracker: QuotaTracker,
    pref: MachineSelectionPreference
  ): ClusterPlan = {
    val maxConc: Int = metrics.flatMap(_.maxConcurrentJobs).reduceOption(_ max _).getOrElse(1)
    val targetExecPerJob: Double = metrics.map(_.p95RunMaxExecutors).reduceOption(_ max _).getOrElse(1.0)
    val requiredSlotsRaw: Double = targetExecPerJob * maxConc

    val (workerType, masterType, workers, epwPref) = chooseMachines(clusterName, requiredSlotsRaw, policy, quotaTracker, pref)
    quotaTracker.recordCluster(workerType, workers, pref)

    val maxExecSupported: Int = workers * epwPref
    logger.info(s"Cluster '$clusterName': execsPerWorker=$epwPref, maxConc=$maxConc, targetExecPerJob=$targetExecPerJob, workers=$workers, maxExecSupported=$maxExecSupported")
    ClusterPlan(clusterName, masterType, workerType, workers, epwPref, maxExecSupported)
  }

  def planManualRecipes(cluster: ClusterPlan, recipes: Iterable[RecipeMetrics], policy: TuningPolicy): Seq[RecipePlanManual] = {
    val epw8: Int = Sizing.executorsPerWorker(cluster.workerMachineType, 8, policy.executorMemoryGb,
      policy.memoryOverheadRatio, policy.osAndDaemonsReserveGb)
    val maxEightCoreExecs = epw8 * cluster.workers

    recipes.toSeq.sortBy(-_.p95RunMaxExecutors).map { m =>
      val base = policy.manualInstancesFrom match {
        case "p95" => m.p95RunMaxExecutors
        case "avg" => m.avgExecutorsPerJob
        case "round_up_p95" => Sizing.roundUp(m.p95RunMaxExecutors).toDouble
        case _ => m.p95RunMaxExecutors
      }
      val boosted = m.fractionReachingCap.filter(_ >= policy.capHitThreshold) match {
        case Some(_) => base * (1.0 + policy.capHitBoostPct)
        case None => base
      }
      val desired: Int = Sizing.roundUp(boosted)
      val capped: Int = Sizing.clamp(desired, policy.minExecutorInstances, cluster.maxExecutorsSupported)

      val canUseEight = policy.preferEightCoreExecutors && (maxEightCoreExecs >= capped)
      val cores: Int = if (canUseEight) 8 else policy.executorCores

      val memGb: Int = recommendedExecutorMemoryGb(cluster.workerMachineType, cores, policy)

      logger.info(s"Manual plan ${cluster.clusterName}/${m.recipe}: base=$base boosted=$boosted desired=$desired capped=$capped cores=$cores memGb=$memGb (max8core=$maxEightCoreExecs)")
      RecipePlanManual(
        recipe = m.recipe,
        sparkExecutorInstances = capped,
        sparkExecutorCores = cores,
        sparkExecutorMemoryGb = memGb
      )
    }
  }

  def planDARecipes(cluster: ClusterPlan, recipes: Iterable[RecipeMetrics], policy: TuningPolicy): Seq[RecipePlanDA] = {
    recipes.toSeq.map { m =>
      val minEraw: Int = policy.daMinFrom match {
        case "avg" => Sizing.roundUp(m.avgExecutorsPerJob)
        case "fixed2" => 2
        case _ => Sizing.roundUp(m.avgExecutorsPerJob)
      }
      val minE: Int = math.max(2, math.max(policy.minExecutorInstances, minEraw))
      val initialE: Int = math.max(2, math.min(
        if (policy.daInitialEqualsMin) minE else min(minE + 1, cluster.maxExecutorsSupported),
        cluster.maxExecutorsSupported
      ))

      val needMore: Boolean = m.p95RunMaxExecutors > (minE + 1)
      val maxRaw: Int = if (needMore) Sizing.roundUp(m.p95RunMaxExecutors) else (minE + 1)
      val maxE: Int = Sizing.clamp(maxRaw, initialE, cluster.maxExecutorsSupported)

      val coresPreferred: Int = if (policy.preferEightCoreExecutors) 8 else policy.executorCores
      val memGb: Int = recommendedExecutorMemoryGb(cluster.workerMachineType, coresPreferred, policy)

      logger.info(s"DA plan ${cluster.clusterName}/${m.recipe}: min=$minE initial=$initialE max=$maxE cores=${coresPreferred} memGb=$memGb (p95=${m.p95RunMaxExecutors})")
      RecipePlanDA(
        recipe = m.recipe,
        minExecutors = minE,
        maxExecutors = maxE,
        initialExecutors = initialE,
        sparkExecutorCores = coresPreferred,
        sparkExecutorMemoryGb = memGb
      )
    }
  }

  /**
   * Emits <cluster>-manually-tuned.json. Optional driverOverride injects driver resource
   * fields into clusterConf when YARN driver eviction has been detected for this cluster.
   */
  private[cluster_tuning] def manualJson(
    cluster: ClusterPlan,
    plans: Seq[RecipePlanManual],
    tunerVersion: String,
    driverOverride: Option[DriverResourceOverride]
  ): String = {
    import Json._
    // When a YARN eviction override is present, use the promoted master machine type.
    val effectiveMaster: MachineType =
      driverOverride.flatMap(_.promotedMasterMachineType).getOrElse(cluster.masterMachineType)

    val autoscalingPolicy = AutoscalingPolicyConfig.resolvePolicy(cluster.workers)
    val clusterMaxMemGb: Int = cluster.workers * cluster.workerMachineType.memoryGb
    val clusterMaxCores: Int = cluster.workers * cluster.workerMachineType.cores
    val accumMemGb: Int = plans.map(p => p.sparkExecutorInstances * p.sparkExecutorMemoryGb).sum
    val totalJobs: Int = plans.size

    val baseFields: Seq[(String, String)] = Seq(
      "num_workers" -> num(cluster.workers),
      "master_machine_type" -> str(effectiveMaster.name),
      "worker_machine_type" -> str(cluster.workerMachineType.name),
      "autoscaling_policy" -> str(autoscalingPolicy),
      "tuner_version" -> str(tunerVersion),
      "total_no_of_jobs" -> num(totalJobs),
      "cluster_max_total_memory_gb" -> num(clusterMaxMemGb),
      "cluster_max_total_cores" -> num(clusterMaxCores),
      "accumulated_max_total_memory_per_jobs_gb" -> num(accumMemGb)
    )

    val driverFields: Seq[(String, String)] = driverOverride.toSeq.flatMap { o =>
      o.driverMemoryGb.map(m => "driver_memory_gb" -> num(m)).toSeq ++
      o.driverCores.map(c => "driver_cores" -> num(c)).toSeq ++
      o.driverMemoryOverheadGb.map(g => "driver_memory_overhead_gb" -> num(g)).toSeq ++
      Seq("diagnostic_reason" -> str(o.diagnosticReason))
    }

    val clusterConf: String = obj(cluster.clusterName -> obj((baseFields ++ driverFields): _*))

    val recipes: Seq[(String, String)] = plans.map { p =>
      val minTotalMemGb = p.sparkExecutorInstances * p.sparkExecutorMemoryGb
      val maxTotalMemGb = minTotalMemGb
      p.recipe -> obj(
        "parallelizationFactor" -> num(5),
        "sparkOptsMap" -> obj(
          "spark.serializer" -> str("org.apache.spark.serializer.KryoSerializer"),
          "spark.closure.serializer" -> str("org.apache.spark.serializer.KryoSerializer"),
          "spark.executor.instances" -> str(s"${p.sparkExecutorInstances}"),
          "spark.executor.cores" -> str(s"${p.sparkExecutorCores}"),
          "spark.executor.memory" -> str(s"${p.sparkExecutorMemoryGb}g")
        ),
        "total_executor_minimum_allocated_memory_gb" -> num(minTotalMemGb),
        "total_executor_maximum_allocated_memory_gb" -> num(maxTotalMemGb)
      )
    }

    val recipeSparkConf: String = obj(recipes.map { case (k, v) => k -> v }: _*)
    Json.pretty(obj("clusterConf" -> clusterConf, "recipeSparkConf" -> recipeSparkConf))
  }

  /**
   * Emits <cluster>-auto-scale-tuned.json. Optional driverOverride injected same as manualJson.
   */
  private[cluster_tuning] def daJson(
    cluster: ClusterPlan,
    plans: Seq[RecipePlanDA],
    tunerVersion: String,
    driverOverride: Option[DriverResourceOverride]
  ): String = {
    import Json._
    val effectiveMaster: MachineType =
      driverOverride.flatMap(_.promotedMasterMachineType).getOrElse(cluster.masterMachineType)

    val autoscalingPolicy = AutoscalingPolicyConfig.resolvePolicy(cluster.workers)
    val clusterMaxMemGb: Int = cluster.workers * cluster.workerMachineType.memoryGb
    val clusterMaxCores: Int = cluster.workers * cluster.workerMachineType.cores
    val accumMemGb: Int = plans.map(p => p.maxExecutors * p.sparkExecutorMemoryGb).sum
    val totalJobs: Int = plans.size

    val baseFields: Seq[(String, String)] = Seq(
      "num_workers" -> num(cluster.workers),
      "master_machine_type" -> str(effectiveMaster.name),
      "worker_machine_type" -> str(cluster.workerMachineType.name),
      "autoscaling_policy" -> str(autoscalingPolicy),
      "tuner_version" -> str(tunerVersion),
      "total_no_of_jobs" -> num(totalJobs),
      "cluster_max_total_memory_gb" -> num(clusterMaxMemGb),
      "cluster_max_total_cores" -> num(clusterMaxCores),
      "accumulated_max_total_memory_per_jobs_gb" -> num(accumMemGb)
    )

    val driverFields: Seq[(String, String)] = driverOverride.toSeq.flatMap { o =>
      o.driverMemoryGb.map(m => "driver_memory_gb" -> num(m)).toSeq ++
      o.driverCores.map(c => "driver_cores" -> num(c)).toSeq ++
      o.driverMemoryOverheadGb.map(g => "driver_memory_overhead_gb" -> num(g)).toSeq ++
      Seq("diagnostic_reason" -> str(o.diagnosticReason))
    }

    val clusterConf: String = obj(cluster.clusterName -> obj((baseFields ++ driverFields): _*))

    val recipes: Seq[(String, String)] = plans.map { p =>
      val minTotalMemGb = p.minExecutors * p.sparkExecutorMemoryGb
      val maxTotalMemGb = p.maxExecutors * p.sparkExecutorMemoryGb
      p.recipe -> obj(
        "parallelizationFactor" -> num(5),
        "sparkOptsMap" -> obj(
          "spark.serializer" -> str("org.apache.spark.serializer.KryoSerializer"),
          "spark.closure.serializer" -> str("org.apache.spark.serializer.KryoSerializer"),
          "spark.dynamicAllocation.enabled" -> str("true"),
          "spark.dynamicAllocation.minExecutors" -> str(s"${p.minExecutors}"),
          "spark.dynamicAllocation.maxExecutors" -> str(s"${p.maxExecutors}"),
          "spark.dynamicAllocation.initialExecutors" -> str(s"${p.initialExecutors}"),
          "spark.executor.cores" -> str(s"${p.sparkExecutorCores}"),
          "spark.executor.memory" -> str(s"${p.sparkExecutorMemoryGb}g")
        ),
        "total_executor_minimum_allocated_memory_gb" -> num(minTotalMemGb),
        "total_executor_maximum_allocated_memory_gb" -> num(maxTotalMemGb)
      )
    }

    val recipeSparkConf: String = obj(recipes.map { case (k, v) => k -> v }: _*)
    Json.pretty(obj("clusterConf" -> clusterConf, "recipeSparkConf" -> recipeSparkConf))
  }

  private[cluster_tuning] def writeFile(outDir: File, fileName: String, content: String): Unit = {
    if (!outDir.exists()) {
      val ok = outDir.mkdirs()
      logger.info(s"Created output directory ${outDir.getPath}: $ok")
    }
    val f = new File(outDir, fileName)
    val bw = new BufferedWriter(new FileWriter(f))
    try {
      bw.write(content)
      logger.info(s"Wrote file: ${f.getPath} size=${content.length}")
    } finally bw.close()
  }

  // Sorting helpers exposed for unit tests
  def sortByTopJobs(summaries: Seq[ClusterSummary]): Seq[ClusterSummary] =
    summaries.sortBy(s => (-s.noOfJobs, s.clusterName))

  def sortByNumOfWorkers(summaries: Seq[ClusterSummary]): Seq[ClusterSummary] =
    summaries.sortBy(s => (-s.numOfWorkers, s.clusterName))

  def sortByEstimatedCostEur(summaries: Seq[ClusterSummary]): Seq[ClusterSummary] =
    summaries.sortBy(s => (-s.estimatedCostEur, s.clusterName))

  def sortByTotalActiveMinutes(summaries: Seq[ClusterSummary]): Seq[ClusterSummary] =
    summaries.sortBy(s => (-s.totalActiveMinutes, s.clusterName))

  private[cluster_tuning] def writeCsv(outDir: File, fileName: String, rows: Seq[ClusterSummary]): Unit = {
    if (!outDir.exists()) outDir.mkdirs()
    val f = new File(outDir, fileName)
    val bw = new BufferedWriter(new FileWriter(f))
    try {
      bw.write("cluster_name,dag_id,no_of_jobs,num_of_workers,worker_machine_type,master_machine_type,total_active_minutes,estimated_cost_eur,TIMER_NAME,TIMER_TIME\n")
      rows.foreach { s =>
        bw.write(
          s"${s.clusterName},${s.dagId},${s.noOfJobs},${s.numOfWorkers},${s.workerMachineType},${s.masterMachineType},${f"${s.totalActiveMinutes}%.2f"},${f"${s.estimatedCostEur}%.4f"},${s.timerName},${s.timerTime}\n"
        )
      }
      logger.info(s"Wrote summary CSV: ${f.getPath} rows=${rows.size}")
    } finally bw.close()
  }

  private[cluster_tuning] def writeSummaryCsv(outDir: File, summaries: Seq[ClusterSummary]): Unit = {
    val sorted = summaries.sortBy(s => (-s.numOfWorkers, -s.noOfJobs, s.clusterName))
    writeCsv(outDir, "_clusters-summary.csv", sorted)
  }

  private[cluster_tuning] def writeSummaryCsvOnlyClustersWf(outDir: File, summaries: Seq[ClusterSummary]): Unit = {
    val sorted = summaries.sortBy(s => (-s.numOfWorkers, -s.noOfJobs, s.clusterName))
    val filtered = sorted.filter(_.clusterName.startsWith("cluster-wf-"))
    writeCsv(outDir, "_clusters-summary-only-clusters-wf.csv", filtered)
  }

  def writeSummaryCsvTopJobs(outDir: File, summaries: Seq[ClusterSummary]): Unit = {
    val sorted = sortByTopJobs(summaries)
    writeCsv(outDir, "_clusters-summary_top_jobs.csv", sorted)
  }

  def writeSummaryCsvNumOfWorkers(outDir: File, summaries: Seq[ClusterSummary]): Unit = {
    val sorted = sortByNumOfWorkers(summaries)
    writeCsv(outDir, "_clusters-summary_num_of_workers.csv", sorted)
  }

  def writeSummaryCsvEstimatedCostEur(outDir: File, summaries: Seq[ClusterSummary]): Unit = {
    val sorted = sortByEstimatedCostEur(summaries)
    writeCsv(outDir, "_clusters-summary_estimated_cost_eur.csv", sorted)
  }

  def writeSummaryCsvTotalActiveMinutes(outDir: File, summaries: Seq[ClusterSummary]): Unit = {
    val sorted = sortByTotalActiveMinutes(summaries)
    writeCsv(outDir, "_clusters-summary_total_active_minutes.csv", sorted)
  }

  /**
   * Global cores-and-machines CSV aggregated by worker MACHINE_TYPE.
   */
  private[cluster_tuning] def writeGlobalCoresAndMachinesCsv(outDir: File, summaries: Seq[ClusterSummary]): Unit = {
    if (!outDir.exists()) outDir.mkdirs()
    val f = new File(outDir, "_clusters-summary_global_cores_and_machines.csv")
    val bw = new BufferedWriter(new FileWriter(f))
    try {
      bw.write("MACHINE_TYPE, ESTIMATED_MAX_NO_OF_WORKERS, ESTIMATED_MAX_NO_OF_CORES, REAL_MAX_NO_OF_WORKERS, REAL_MAX_NO_OF_CORES, CLUSTERS_LIST\n")

      val grouped = summaries.groupBy(_.workerMachineType)

      grouped.toSeq.sortBy { case (mt, clusters) =>
        val cores = MachineCatalog.byName(mt).map(_.cores).getOrElse(0)
        -clusters.map(_.numOfWorkers).sum * cores
      }.foreach { case (machineTypeName, clusterRows) =>
        val maybeMt = MachineCatalog.byName(machineTypeName)
        val coresPerWorker = maybeMt.map(_.cores).getOrElse(0)

        val estimatedWorkersSum: Int = clusterRows.map(_.numOfWorkers).sum
        val estimatedCoresSum: Int = estimatedWorkersSum * coresPerWorker

        val realMaxWorkersSum: Int = clusterRows.map { c =>
          AutoscalingPolicyConfig.maxWorkersForCluster(c.numOfWorkers)
        }.sum
        val realMaxCoresSum: Int = realMaxWorkersSum * coresPerWorker

        val clustersList: String = clusterRows.map(_.clusterName).distinct.sorted.mkString(";")

        bw.write(s"$machineTypeName,$estimatedWorkersSum,$estimatedCoresSum,$realMaxWorkersSum,$realMaxCoresSum,$clustersList\n")
      }

      logger.info(s"Wrote global cores-and-machines CSV: ${f.getPath} groups=${grouped.size}")
    } finally bw.close()
  }

  /**
   * run:
   * - Loads metrics (flattened or individual).
   * - Loads dag_id map and timer map.
   * - Loads b14 diagnostics and computes driver resource overrides.
   * - Plans cluster shape and recipe tuning for each cluster (quota-aware).
   * - Writes per-cluster JSONs, all existing summary CSVs, and new generation summary.
   */
  def run(cfg: Config, strategy: TuningStrategy = DefaultTuningStrategy): Unit = {
    logger.info(s"Starting ClusterMachineAndRecipeTuner. inputDir=${cfg.inputDir.getPath} outputDir=${cfg.outputDir.getPath} flattened=${cfg.useFlattened} strategy=${strategy.name}")

    val metricsByKey: Map[(String, String), RecipeMetrics] = loadMetrics(cfg)
    if (metricsByKey.isEmpty) {
      logger.warn("No metrics loaded. No JSONs will be produced. Ensure CSVs exist in the input directory and the query was executed.")
      return
    }
    // Sort clusters by demand descending so the most demanding cluster is planned first.
    // This ensures C3/C4 (capped at 1 cluster each) is allocated to the highest-demand
    // workload before the cluster-count cap is consumed by a smaller cluster.
    // Demand proxy matches planCluster: max(p95RunMaxExecutors) × max(concurrentJobs).
    val metricsByCluster: Seq[(String, Iterable[RecipeMetrics])] =
      metricsByKey.values.groupBy(_.cluster).toSeq.sortBy { case (_, recs) =>
        val maxP95  = recs.map(_.p95RunMaxExecutors).max
        val concSeq = recs.flatMap(_.maxConcurrentJobs)
        val maxConc = if (concSeq.isEmpty) 1 else concSeq.max
        -(maxP95 * maxConc)
      }(Ordering[Double])
    logger.info(s"Found ${metricsByCluster.size} clusters in metrics.")

    val dagByCluster: Map[String, String] = loadDagClusterRelationshipMap()
    val timerByCluster: Map[String, (String, String)] = loadDagClusterCreationTimeMap()
    val tunerVersion: String = toTunerVersion(cfg.date)

    // Load b14 diagnostics (computed once, before the cluster-planning loop)
    val b14File = new File(cfg.inputDir, "b14_clusters_with_nonzero_exit_codes.csv")
    val allDiagnosticSignals: Map[String, Seq[DiagnosticSignal]] =
      ClusterDiagnosticsProcessor.detectSignals(
        ClusterDiagnosticsProcessor.loadExitCodes(b14File)
      )
    val driverOverrides: Map[String, DriverResourceOverride] =
      ClusterDiagnosticsProcessor.computeOverrides(allDiagnosticSignals)

    // Derive policy and quota tracker from the active strategy
    val defaultMachine: MachineType = MachineCatalog.byName("e2-standard-8").get
    val policy: TuningPolicy = strategy.toTuningPolicy(defaultMachine)
    val quotaTracker = new QuotaTracker(strategy.quotas)

    val summaries: ArrayBuffer[ClusterSummary] = ArrayBuffer.empty[ClusterSummary]
    val summaryEntries: ArrayBuffer[GenerationSummaryEntry] = ArrayBuffer.empty[GenerationSummaryEntry]

    metricsByCluster.foreach { case (clusterName, recMetrics) =>
      logger.info(s"Planning cluster: $clusterName with ${recMetrics.size} recipes.")
      val clusterPlan: ClusterPlan = planCluster(clusterName, recMetrics, policy, quotaTracker, strategy.machinePreference)
      val manualPlans: Seq[RecipePlanManual] = planManualRecipes(clusterPlan, recMetrics, policy)
      val daPlans: Seq[RecipePlanDA] = planDARecipes(clusterPlan, recMetrics, policy)

      // Enrich the pre-computed override with the promoted master machine type, which
      // depends on the cluster plan's workerMachineType (resolved in the line above).
      val driverOverride: Option[DriverResourceOverride] = driverOverrides.get(clusterName).map { o =>
        val promoted = ClusterDiagnosticsProcessor.promoteMasterForEviction(clusterPlan.masterMachineType)
        o.copy(promotedMasterMachineType = Some(promoted))
      }

      val manualJsonStr: String = manualJson(clusterPlan, manualPlans, tunerVersion, driverOverride)
      val daJsonStr: String = daJson(clusterPlan, daPlans, tunerVersion, driverOverride)

      writeFile(cfg.outputDir, s"$clusterName-manually-tuned.json", manualJsonStr)
      writeFile(cfg.outputDir, s"$clusterName-auto-scale-tuned.json", daJsonStr)

      val totalMinutes: Double = clusterActiveMinutes(recMetrics)
      val hourlyCost: Double = hourlyPrice(clusterPlan.workerMachineType) * clusterPlan.workers + hourlyPrice(clusterPlan.masterMachineType)
      val estimatedCost: Double = hourlyCost * (totalMinutes / 60.0)

      val resolvedDagId: String = dagByCluster.getOrElse(clusterName, "UNKNOWN_DAG_ID")
      val (resolvedTimerName, resolvedTimerTime) = timerByCluster.getOrElse(clusterName, ("ZERO_TIMER", "00:00"))

      summaries += ClusterSummary(
        clusterName = clusterName,
        dagId = resolvedDagId,
        noOfJobs = recMetrics.size,
        numOfWorkers = clusterPlan.workers,
        workerMachineType = clusterPlan.workerMachineType.name,
        masterMachineType = clusterPlan.masterMachineType.name,
        totalActiveMinutes = totalMinutes,
        estimatedCostEur = estimatedCost,
        timerName = resolvedTimerName,
        timerTime = resolvedTimerTime
      )

      val signals: Seq[String] = allDiagnosticSignals.getOrElse(clusterName, Seq.empty).map(_.description)

      summaryEntries += GenerationSummaryEntry(
        clusterName          = clusterName,
        workerMachineType    = clusterPlan.workerMachineType.name,
        workerFamily         = familyOf(clusterPlan.workerMachineType.name),
        numWorkers           = clusterPlan.workers,
        maxWorkersFromPolicy = AutoscalingPolicyConfig.maxWorkersForCluster(clusterPlan.workers),
        totalCores           = clusterPlan.workers * clusterPlan.workerMachineType.cores,
        maxTotalCores        = AutoscalingPolicyConfig.maxWorkersForCluster(clusterPlan.workers) * clusterPlan.workerMachineType.cores,
        diagnosticSignals    = signals,
        strategyName         = strategy.name,
        biasMode             = strategy.biasMode.name,
        topologyPreset       = strategy.executorTopology.label
      )
    }

    // All existing _clusters-*.csv outputs — written identically to pre-refactoring behaviour
    writeSummaryCsv(cfg.outputDir, summaries.toSeq)
    writeSummaryCsvOnlyClustersWf(cfg.outputDir, summaries.toSeq)
    writeSummaryCsvTopJobs(cfg.outputDir, summaries.toSeq)
    writeSummaryCsvNumOfWorkers(cfg.outputDir, summaries.toSeq)
    writeSummaryCsvEstimatedCostEur(cfg.outputDir, summaries.toSeq)
    writeSummaryCsvTotalActiveMinutes(cfg.outputDir, summaries.toSeq)
    writeGlobalCoresAndMachinesCsv(cfg.outputDir, summaries.toSeq)

    // New: generation summary
    val genSummary = GenerationSummary(
      generatedAt                     = java.time.Instant.now().toString,
      date                            = cfg.date,
      strategyName                    = strategy.name,
      biasMode                        = strategy.biasMode.name,
      topologyPreset                  = strategy.executorTopology.label,
      quotas                          = strategy.quotas,
      totalClusters                   = summaryEntries.size,
      totalPredictedNodes             = summaryEntries.map(_.numWorkers + 1).sum,
      totalMaxNodes                   = summaryEntries.map(_.maxWorkersFromPolicy + 1).sum,
      quotaUsageByFamily              = quotaTracker.usageSummary,
      clustersWithDiagnosticOverrides = driverOverrides.size,
      entries                         = summaryEntries.toSeq
    )
    GenerationSummaryWriter.writeSummary(cfg.outputDir, genSummary)

    logger.info("ClusterMachineAndRecipeTuner finished successfully.")
  }

  /**
   * CLI Main Method
   * - Requires the date in `YYYY_MM_DD` format.
   * - Optional `flattened=false` flag to switch to non-flattened mode.
   * - Optional `--strategy=<name>` to select tuning strategy (default, cost_biased, performance_biased).
   * - Optional `--topology=<label>` to override executor topology (e.g. 8cx2GBpc).
   */
  def main(args: Array[String]): Unit = {
    val dateArg = args.find(_.matches("\\d{4}_\\d{2}_\\d{2}"))
    val useFlattened = !args.exists(_.toLowerCase.contains("flattened=false"))

    if (dateArg.isEmpty) {
      logger.error("A date argument in YYYY_MM_DD format must be provided (e.g., 2025_12_20).")
      sys.exit(1)
    }

    // Resolve strategy from optional --strategy=<name> arg
    val strategyName = args.collectFirst { case a if a.startsWith("--strategy=") => a.stripPrefix("--strategy=") }
    val baseStrategy: TuningStrategy = strategyName.flatMap(TuningStrategy.fromName).getOrElse(DefaultTuningStrategy)

    // Optionally override executor topology via --topology=<label>
    val topoLabel = args.collectFirst { case a if a.startsWith("--topology=") => a.stripPrefix("--topology=") }
    val strategy: TuningStrategy = topoLabel.flatMap(ExecutorTopologyPreset.fromLabel) match {
      case Some(topo) =>
        // Wrap the base strategy to override the topology only
        new TuningStrategy {
          val name                  = s"${baseStrategy.name}+${topo.label}"
          val biasMode              = baseStrategy.biasMode
          val executorTopology      = topo
          val machinePreference     = baseStrategy.machinePreference
          val quotas                = baseStrategy.quotas
          val capHitBoostPct        = baseStrategy.capHitBoostPct
          val capHitThreshold       = baseStrategy.capHitThreshold
          val preferMaxWorkers      = baseStrategy.preferMaxWorkers
          val perWorkerPenaltyPct   = baseStrategy.perWorkerPenaltyPct
          val memoryOverheadRatio   = baseStrategy.memoryOverheadRatio
          val osAndDaemonsReserveGb = baseStrategy.osAndDaemonsReserveGb
          val manualInstancesFrom   = baseStrategy.manualInstancesFrom
          val minExecutorInstances  = baseStrategy.minExecutorInstances
          val daMinFrom             = baseStrategy.daMinFrom
          val daInitialEqualsMin    = baseStrategy.daInitialEqualsMin
        }
      case None => baseStrategy
    }

    val date = dateArg.get
    val cfg = Config(useFlattened = useFlattened, date = date)

    logger.info(s"Starting ClusterMachineAndRecipeTuner with Config: $cfg, strategy=${strategy.name}")
    run(cfg, strategy)
    println(s"Input directory: ${cfg.inputDir}")
    println(s"Output directory: ${cfg.outputDir}")
  }

  object AutoscalingPolicyConfig {

    def resolvePolicy(numWorkers: Int): String = {
      numWorkers match {
        case n if n <= 4 => "small-workload-autoscaling"
        case n if n <= 6 => "medium-workload-autoscaling"
        case n if n <= 8 => "large-workload-autoscaling"
        case n if n <= 10 => "extra-large-workload-autoscaling"
        case _ => "extra-large-workload-autoscaling"
      }
    }

    def maxWorkersForCluster(numWorkers: Int): Int = {
      numWorkers match {
        case n if n <= 4 => 4
        case n if n <= 6 => 6
        case n if n <= 8 => 8
        case n if n <= 10 => 10
        case n => n
      }
    }
  }
}
