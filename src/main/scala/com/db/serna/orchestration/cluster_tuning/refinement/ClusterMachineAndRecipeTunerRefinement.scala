package com.db.serna.orchestration.cluster_tuning.refinement

import org.rogach.scallop._
import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, File, FileWriter}

/**
 * CLI configuration for the refinement app, parsed via Scallop.
 *
 * Usage:
 * {{{
 *   --referenceTuningDate=2025_12_20
 *   --memoryHeapBoostFactor=1.5
 *   --memoryOverheadBoostFactor=1.0   (future, not yet active)
 * }}}
 */
class RefinementConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner(
    """
      |ClusterMachineAndRecipeTunerRefinement
      |
      |Reads tuned JSON configs and applies vitamin boosts based on diagnostic CSV outputs.
      |
      |Options:
    """.stripMargin)

  val referenceTuningDate: ScallopOption[String] = opt[String](
    required = true,
    descr = "Reference tuning date in YYYY_MM_DD format (e.g. 2025_12_20)",
    validate = _.matches("\\d{4}_\\d{2}_\\d{2}")
  )

  val memoryHeapBoostFactor: ScallopOption[Double] = opt[Double](
    default = Some(1.5),
    descr = "Boost factor for spark.executor.memory on Java heap OOM (b16). Default: 1.5",
    validate = f => f >= 1.0 && f <= 5.0
  )

  val memoryOverheadBoostFactor: ScallopOption[Double] = opt[Double](
    default = Some(1.0),
    descr = "Boost factor for spark.executor.memoryOverhead (b17, future). 1.0 = no boost",
    validate = f => f >= 1.0 && f <= 5.0
  )

  verify()
}

/**
 * Refinement app entry point.
 *
 * Reads the tuned cluster JSONs produced by [[com.db.serna.orchestration.cluster_tuning.ClusterMachineAndRecipeTuner]],
 * cross-references diagnostic CSVs, and writes refined JSONs with boosted Spark
 * settings for recipes that experienced failures (OOM, etc.).
 *
 * Refined output overwrites the original tuned JSONs in `outputs/<date>/` in-place.
 */
object ClusterMachineAndRecipeTunerRefinement {
  private val logger = LoggerFactory.getLogger(getClass)

  private val BasePath = "src/main/resources/composer/dwh/config/cluster_tuning"

  def main(args: Array[String]): Unit = {
    val conf = new RefinementConf(args)
    run(conf)
  }

  def run(conf: RefinementConf): Unit = {
    val date = conf.referenceTuningDate()
    val heapFactor = conf.memoryHeapBoostFactor()

    val inputDir = new File(s"$BasePath/inputs/$date")
    val outputDir = new File(s"$BasePath/outputs/$date")

    if (!inputDir.exists()) {
      logger.error(s"Input directory does not exist: ${inputDir.getPath}")
      return
    }
    if (!outputDir.exists()) {
      logger.error(s"Output directory does not exist: ${outputDir.getPath}")
      return
    }

    // Build vitamin pipeline — only include active vitamins
    val vitamins: Seq[RefinementVitamin] = buildVitaminPipeline(heapFactor)

    logger.info(s"Refinement pipeline: ${vitamins.map(_.name).mkString(" -> ")}")
    logger.info(s"  memoryHeapBoostFactor=$heapFactor")

    // Discover tuned JSON files
    val tunedFiles = Option(outputDir.listFiles())
      .getOrElse(Array.empty)
      .filter { f =>
        f.getName.endsWith("-auto-scale-tuned.json") ||
        f.getName.endsWith("-manually-tuned.json")
      }
      .sortBy(_.getName)

    if (tunedFiles.isEmpty) {
      logger.warn(s"No tuned JSON files found in ${outputDir.getPath}")
      return
    }

    logger.info(s"Found ${tunedFiles.length} tuned JSON files in ${outputDir.getPath}")

    var totalBoosts = 0
    var filesWithBoosts = 0

    tunedFiles.foreach { jsonFile =>
      val config = SimpleJsonParser.parseFile(jsonFile)
      val result = RefinementPipeline.refine(config, vitamins, inputDir)

      if (result.appliedBoosts.nonEmpty) {
        filesWithBoosts += 1
        totalBoosts += result.appliedBoosts.size
        val boostedRecipes = result.boostLists.values.flatten.toSeq.distinct.sorted
        logger.info(s"[${jsonFile.getName}] ${result.appliedBoosts.size} boost(s) on recipes: ${boostedRecipes.mkString(", ")}:")
        result.appliedBoosts.foreach(b => logger.info(s"  - ${b.description}"))
      }

      val refinedJson = RefinementPipeline.toRefinedJson(result)
      writeFile(outputDir, jsonFile.getName, refinedJson)
    }

    logger.info(s"Refinement complete.")
    logger.info(s"  Total boosts applied: $totalBoosts across $filesWithBoosts file(s)")
    logger.info(s"  Output: ${outputDir.getPath} (in-place)")
  }

  private[refinement] def buildVitaminPipeline(heapFactor: Double): Seq[RefinementVitamin] = {
    val vitamins = scala.collection.mutable.ArrayBuffer.empty[RefinementVitamin]

    // B16: Memory Heap OOM boost — always active
    vitamins += new MemoryHeapBoostVitamin(heapFactor)

    // Future vitamins will be added here as they become available:
    // if (overheadFactor > 1.0) vitamins += new MemoryOverheadBoostVitamin(overheadFactor)
    // vitamins += new GCPressureBoostVitamin(...)
    // vitamins += new ShuffleSpillBoostVitamin(...)

    vitamins.toSeq
  }

  private def writeFile(outDir: File, fileName: String, content: String): Unit = {
    if (!outDir.exists()) {
      val ok = outDir.mkdirs()
      if (!ok) throw new RuntimeException(s"Failed to create directory: ${outDir.getPath}")
    }
    val f = new File(outDir, fileName)
    val bw = new BufferedWriter(new FileWriter(f))
    try bw.write(content) finally bw.close()
  }
}
