package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.ClusterMachineAndRecipeTuner.AutoscalingPolicyConfig
import com.db.serna.orchestration.cluster_tuning._
import com.db.serna.orchestration.cluster_tuning.refinement._
import org.rogach.scallop._
import org.slf4j.LoggerFactory

import java.io.File
import scala.collection.mutable.ArrayBuffer

/**
 * CLI configuration for the auto-tuner, parsed via Scallop.
 *
 * Usage:
 * {{{
 *   --reference-date=2025_12_20 --current-date=2026_04_15
 *   --keep-historical-tuning (true by default)
 *   --b16-reboosting-factor=1.5
 *   --strategy=default
 *   --divergence-z-threshold=2.0
 * }}}
 */
class AutoTunerConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner(
    """
      |ClusterMachineAndRecipeAutoTuner
      |
      |Compares metrics across reference and current dates, detects performance
      |trends, and evolves cluster/recipe configurations accordingly.
      |
      |Options:
    """.stripMargin)

  val referenceDate: ScallopOption[String] = opt[String](
    required = true,
    descr = "Reference date in YYYY_MM_DD format",
    validate = _.matches("\\d{4}_\\d{2}_\\d{2}")
  )

  val currentDate: ScallopOption[String] = opt[String](
    required = true,
    descr = "Current date in YYYY_MM_DD format",
    validate = _.matches("\\d{4}_\\d{2}_\\d{2}")
  )

  val keepHistoricalTuning: ScallopOption[Boolean] = opt[Boolean](
    default = Some(true),
    descr = "Preserve configs for clusters/recipes absent from current_date metrics (default: true)"
  )

  val b16ReboostingFactor: ScallopOption[Double] = opt[Double](
    default = Some(1.5),
    descr = "Boost factor for spark.executor.memory on b16 OOM signals (default: 1.5)",
    validate = f => f >= 1.0 && f <= 5.0
  )

  val b17ReboostingFactor: ScallopOption[Double] = opt[Double](
    default = Some(1.0),
    descr = "Boost factor for spark.executor.memoryOverhead on b17 signals (future, default: 1.0)",
    validate = f => f >= 1.0 && f <= 5.0
  )

  val strategy: ScallopOption[String] = opt[String](
    default = Some("default"),
    descr = "Tuning strategy: default, cost_biased, performance_biased"
  )

  val divergenceZThreshold: ScallopOption[Double] = opt[Double](
    default = Some(2.0),
    descr = "Z-score threshold for outlier divergence detection (default: 2.0)"
  )

  verify()
}

/**
 * Auto-tuner entry point.
 *
 * Unlike the one-off [[ClusterMachineAndRecipeTuner]], this auto-tuner compares
 * metrics across two dates (reference and current) to detect performance trends
 * and evolve configurations. It reuses the one-off tuner's core planning methods
 * while adding temporal awareness.
 */
object ClusterMachineAndRecipeAutoTuner {
  private val logger = LoggerFactory.getLogger(getClass)
  private val BasePath = "src/main/resources/composer/dwh/config/cluster_tuning"

  def main(args: Array[String]): Unit = {
    val conf = new AutoTunerConf(args)
    run(conf)
  }

  def run(conf: AutoTunerConf): Unit = {
    val refDate = conf.referenceDate()
    val curDate = conf.currentDate()
    val keepHistorical = conf.keepHistoricalTuning()
    val b16Factor = conf.b16ReboostingFactor()
    val strategyName = conf.strategy()
    val zThreshold = conf.divergenceZThreshold()

    logger.info(s"AutoTuner starting: reference=$refDate current=$curDate strategy=$strategyName keepHistorical=$keepHistorical b16Factor=$b16Factor")

    // 1. Load snapshots for both dates
    val refSnapshot = loadSnapshot(refDate)
    val curSnapshot = loadSnapshot(curDate)

    if (refSnapshot.metrics.isEmpty) {
      logger.error(s"No metrics found for reference date $refDate")
      return
    }
    if (curSnapshot.metrics.isEmpty) {
      logger.warn(s"No metrics found for current date $curDate. All entries will be treated as DroppedEntry.")
    }

    // 2. Pair metrics and classify entries
    val refKeys = refSnapshot.metrics.keySet
    val curKeys = curSnapshot.metrics.keySet
    val commonKeys = refKeys.intersect(curKeys)
    val newKeys = curKeys.diff(refKeys)
    val droppedKeys = refKeys.diff(curKeys)

    val pairs: Seq[MetricsPair] = commonKeys.toSeq.map { case key@(cluster, recipe) =>
      MetricsPair(cluster, recipe, refSnapshot.metrics(key), curSnapshot.metrics(key))
    }

    logger.info(s"Paired: ${pairs.size} common, ${newKeys.size} new, ${droppedKeys.size} dropped")

    // 3. Assess trends for paired entries
    val pairedTrends: Seq[TrendAssessment] = pairs.map(TrendDetector.assessTrend)

    // Create synthetic trends for new and dropped entries
    val newTrends: Seq[TrendAssessment] = newKeys.toSeq.map { case (cluster, recipe) =>
      TrendAssessment(cluster, recipe, NewEntry, Seq.empty, 0.0)
    }
    val droppedTrends: Seq[TrendAssessment] = droppedKeys.toSeq.map { case (cluster, recipe) =>
      TrendAssessment(cluster, recipe, DroppedEntry, Seq.empty, 0.0)
    }

    val allTrends = pairedTrends ++ newTrends ++ droppedTrends
    logTrendSummary(allTrends)

    // 4. Statistical analysis
    val correlations = StatisticalAnalysis.computeCorrelations(pairs)
    val divergences = StatisticalAnalysis.detectDivergences(pairs, zThreshold)
    logger.info(s"Correlations computed: ${correlations.size}. Divergences detected: ${divergences.size}")

    // 5. Load reference output configs for KeepAsIs/PreserveHistorical paths
    val refOutputDir = new File(s"$BasePath/outputs/$refDate")
    val referenceConfigs: Map[String, TunedClusterConfig] = loadReferenceConfigs(refOutputDir)

    // 6. Decide evolutions
    val decisions = PerformanceEvolver.decideEvolutions(
      allTrends, referenceConfigs, curSnapshot.metrics, refSnapshot.metrics, keepHistorical
    )

    // 7. Resolve strategy
    val tuningStrategy = resolveStrategy(strategyName)
    val defaultMachine = MachineCatalog.byName("e2-standard-8").get
    val policy = tuningStrategy.toTuningPolicy(defaultMachine)
    val quotaTracker = new QuotaTracker(tuningStrategy.quotas)

    // 8. Generate outputs
    val curOutputDir = new File(s"$BasePath/outputs/${curDate}_auto_tuned")
    if (!curOutputDir.exists()) curOutputDir.mkdirs()

    val tunerVersion = ClusterMachineAndRecipeTuner.toTunerVersion(curDate)
    val dagByCluster = ClusterMachineAndRecipeTuner.loadDagClusterRelationshipMap()
    val timerByCluster = ClusterMachineAndRecipeTuner.loadDagClusterCreationTimeMap()

    val summaries = ArrayBuffer.empty[ClusterSummary]
    val summaryEntries = ArrayBuffer.empty[GenerationSummaryEntry]

    // Tracking for the summary report
    val b14BoostedClusters = ArrayBuffer.empty[(String, String)] // (cluster, reason)
    val b16BoostedRecipes = ArrayBuffer.empty[(String, String, Int)] // (cluster, suffix, boostCount)
    var keptClusters = 0
    var boostedClusters = 0
    var freshClusters = 0
    var preservedClusters = 0
    var skippedClusters = 0

    // Group decisions by cluster for output
    val decisionsByCluster: Map[String, Seq[EvolutionDecision]] = decisions.groupBy(_.cluster)
    val allClusterNames = decisionsByCluster.keys.toSeq.sorted

    allClusterNames.foreach { clusterName =>
      val clusterDecisions = decisionsByCluster(clusterName)
      val primaryAction = clusterDecisions.map(_.action).reduceLeft { (a, b) =>
        if (a == BoostResources || b == BoostResources) BoostResources
        else if (a == GenerateFresh || b == GenerateFresh) GenerateFresh
        else a
      }

      primaryAction match {
        case KeepAsIs | PreserveHistorical =>
          // Re-emit reference configs verbatim
          if (primaryAction == PreserveHistorical) preservedClusters += 1 else keptClusters += 1
          emitReferenceConfigs(clusterName, refOutputDir, curOutputDir)

        case BoostResources | GenerateFresh =>
          if (primaryAction == GenerateFresh) freshClusters += 1 else boostedClusters += 1
          // Plan fresh using the relevant metrics
          val metricsSource = if (primaryAction == GenerateFresh) curSnapshot.metrics else curSnapshot.metrics
          val recMetrics = metricsSource.collect { case ((c, _), m) if c == clusterName => m }

          if (recMetrics.isEmpty) {
            skippedClusters += 1
            logger.warn(s"No current metrics for cluster $clusterName despite $primaryAction action. Skipping.")
          } else {
            val clusterPlan = ClusterMachineAndRecipeTuner.planCluster(
              clusterName, recMetrics, policy, quotaTracker, tuningStrategy.machinePreference
            )
            val manualPlans = ClusterMachineAndRecipeTuner.planManualRecipes(clusterPlan, recMetrics, policy)
            val daPlans = ClusterMachineAndRecipeTuner.planDARecipes(clusterPlan, recMetrics, policy)

            // b14 driver evolution: if current_date has b14 signals, ALWAYS upgrade
            // the master to mitigate YARN driver eviction (exit code 247).
            //
            // Promotion chain (always a leap ahead):
            //   standard → highmem     (more memory, same cores)
            //   highmem  → more cores  (e.g. highmem-32 → highmem-48)
            //   core cap → cross-family (e.g. e2-highmem-16 → n2-highmem-16)
            //
            // Baseline = max(reference config master, fresh plan master) so we
            // never regress below what the reference already promoted to.
            val driverOverride: Option[DriverResourceOverride] = curSnapshot.driverOverrides.get(clusterName).map { curOverride =>
              // Find the best known master: reference config's (already promoted) vs fresh plan's
              val refConfig = referenceConfigs.get(clusterName)
              val refMasterName = refConfig.flatMap(_.clusterConfFields.find(_._1 == "master_machine_type").map(_._2.replaceAll("\"", "")))
              val refMaster = refMasterName.flatMap(MachineCatalog.byName)
              val baseline = refMaster match {
                case Some(rm) if rm.memoryGb >= clusterPlan.masterMachineType.memoryGb => rm
                case _ => clusterPlan.masterMachineType
              }

              // Always promote: standard→highmem, highmem→more cores, core cap→cross-family
              val promoted = ClusterDiagnosticsProcessor.promoteMasterForEviction(baseline)

              val isPersistent = refSnapshot.b14Signals.get(clusterName).exists(_.nonEmpty)
              val reason = if (isPersistent) {
                val refEvictions = refSnapshot.b14Signals(clusterName).collect { case e: YarnDriverEviction => e.evictionCount }.sum
                val curEvictions = curSnapshot.b14Signals.getOrElse(clusterName, Seq.empty).collect { case e: YarnDriverEviction => e.evictionCount }.sum
                s"Persistent b14 driver eviction (247): $refEvictions (ref) -> $curEvictions (cur). Promoted ${baseline.name} -> ${promoted.name}"
              } else {
                s"Current-date b14 driver eviction (247). Promoted ${baseline.name} -> ${promoted.name}"
              }

              DriverResourceOverride(clusterName, curOverride.driverMemoryGb, curOverride.driverCores,
                curOverride.driverMemoryOverheadGb, reason, Some(promoted))
            }

            // Track b14 boosts
            if (driverOverride.isDefined) {
              b14BoostedClusters += ((clusterName, driverOverride.get.diagnosticReason))
            }

            val manualJsonStr = ClusterMachineAndRecipeTuner.manualJson(clusterPlan, manualPlans, tunerVersion, driverOverride)
            val daJsonStr = ClusterMachineAndRecipeTuner.daJson(clusterPlan, daPlans, tunerVersion, driverOverride)

            ClusterMachineAndRecipeTuner.writeFile(curOutputDir, s"$clusterName-manually-tuned.json", manualJsonStr)
            ClusterMachineAndRecipeTuner.writeFile(curOutputDir, s"$clusterName-auto-scale-tuned.json", daJsonStr)

            // Apply b16 reboosting if there are OOM signals for degraded recipes
            if (b16Factor > 1.0 && primaryAction == BoostResources) {
              val boostCount = applyB16Reboosting(clusterName, curOutputDir, curSnapshot, b16Factor)
              if (boostCount > 0) {
                b16BoostedRecipes += ((clusterName, "-auto-scale-tuned.json", boostCount))
              }
            }

            // Build summary entry
            val totalMinutes = ClusterMachineAndRecipeTuner.clusterActiveMinutes(recMetrics)
            val hourlyCost = ClusterMachineAndRecipeTuner.hourlyPrice(clusterPlan.workerMachineType) * clusterPlan.workers +
              ClusterMachineAndRecipeTuner.hourlyPrice(clusterPlan.masterMachineType)
            val estimatedCost = hourlyCost * (totalMinutes / 60.0)

            summaries += ClusterSummary(
              clusterName = clusterName,
              dagId = dagByCluster.getOrElse(clusterName, "UNKNOWN_DAG_ID"),
              noOfJobs = recMetrics.size,
              numOfWorkers = clusterPlan.workers,
              workerMachineType = clusterPlan.workerMachineType.name,
              masterMachineType = clusterPlan.masterMachineType.name,
              totalActiveMinutes = totalMinutes,
              estimatedCostEur = estimatedCost,
              timerName = timerByCluster.getOrElse(clusterName, ("ZERO_TIMER", "00:00"))._1,
              timerTime = timerByCluster.getOrElse(clusterName, ("ZERO_TIMER", "00:00"))._2
            )

            val signals = curSnapshot.b14Signals.getOrElse(clusterName, Seq.empty).map(_.description)
            summaryEntries += GenerationSummaryEntry(
              clusterName = clusterName,
              workerMachineType = clusterPlan.workerMachineType.name,
              workerFamily = familyOf(clusterPlan.workerMachineType.name),
              numWorkers = clusterPlan.workers,
              maxWorkersFromPolicy = AutoscalingPolicyConfig.maxWorkersForCluster(clusterPlan.workers),
              totalCores = clusterPlan.workers * clusterPlan.workerMachineType.cores,
              maxTotalCores = AutoscalingPolicyConfig.maxWorkersForCluster(clusterPlan.workers) * clusterPlan.workerMachineType.cores,
              diagnosticSignals = signals,
              strategyName = tuningStrategy.name,
              biasMode = tuningStrategy.biasMode.name,
              topologyPreset = tuningStrategy.executorTopology.label
            )
          }
      }
    }

    // 9. Write analysis outputs
    val analysisJson = AutoTunerJsonOutput.analysisOutputJson(
      refDate, curDate, strategyName, allTrends, correlations, divergences, decisions
    )
    ClusterMachineAndRecipeTuner.writeFile(curOutputDir, "_auto_tuner_analysis.json", analysisJson)
    AutoTunerJsonOutput.writeAnalysisCsvs(curOutputDir, allTrends, correlations, divergences, decisions)

    // 10. Write all cluster-summary CSVs (same as one-off tuner)
    if (summaries.nonEmpty) {
      val allSummaries = summaries.toSeq
      ClusterMachineAndRecipeTuner.writeSummaryCsv(curOutputDir, allSummaries)
      ClusterMachineAndRecipeTuner.writeSummaryCsvOnlyClustersWf(curOutputDir, allSummaries)
      ClusterMachineAndRecipeTuner.writeSummaryCsvTopJobs(curOutputDir, allSummaries)
      ClusterMachineAndRecipeTuner.writeSummaryCsvNumOfWorkers(curOutputDir, allSummaries)
      ClusterMachineAndRecipeTuner.writeSummaryCsvEstimatedCostEur(curOutputDir, allSummaries)
      ClusterMachineAndRecipeTuner.writeSummaryCsvTotalActiveMinutes(curOutputDir, allSummaries)
      ClusterMachineAndRecipeTuner.writeGlobalCoresAndMachinesCsv(curOutputDir, allSummaries)
    }

    // 11. Write generation summary if we planned any clusters
    if (summaryEntries.nonEmpty) {
      val genSummary = GenerationSummary(
        generatedAt = java.time.Instant.now().toString,
        date = curDate,
        strategyName = tuningStrategy.name,
        biasMode = tuningStrategy.biasMode.name,
        topologyPreset = tuningStrategy.executorTopology.label,
        quotas = tuningStrategy.quotas,
        totalClusters = summaryEntries.size,
        totalPredictedNodes = summaryEntries.map(_.numWorkers + 1).sum,
        totalMaxNodes = summaryEntries.map(_.maxWorkersFromPolicy + 1).sum,
        quotaUsageByFamily = quotaTracker.usageSummary,
        clustersWithDiagnosticOverrides = curSnapshot.driverOverrides.size,
        entries = summaryEntries.toSeq
      )
      GenerationSummaryWriter.writeSummary(curOutputDir, genSummary)
    }

    // 12. Write auto-tuner summary report
    writeAutoTunerSummaryReport(
      curOutputDir, refDate, curDate, strategyName, allTrends,
      keptClusters, boostedClusters, freshClusters, preservedClusters, skippedClusters,
      b14BoostedClusters.toSeq, b16BoostedRecipes.toSeq,
      correlations.size, divergences.size
    )

    logger.info(s"AutoTuner finished. Output: ${curOutputDir.getPath}")
    logger.info(s"  Clusters processed: ${allClusterNames.size}")
    logger.info(s"  Analysis: _auto_tuner_analysis.json, _trend_summary.csv, _correlations.csv, _divergences.csv")
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private[auto] def loadSnapshot(date: String): DateSnapshot = {
    val cfg = ClusterMachineAndRecipeTuner.Config(useFlattened = true, date = date)
    val metrics = try {
      ClusterMachineAndRecipeTuner.loadMetrics(cfg)
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to load metrics for $date: ${e.getMessage}")
        Map.empty[(String, String), RecipeMetrics]
    }

    val b14File = new File(cfg.inputDir, "b14_clusters_with_nonzero_exit_codes.csv")
    val b14Signals = ClusterDiagnosticsProcessor.detectSignals(
      ClusterDiagnosticsProcessor.loadExitCodes(b14File)
    )
    val driverOverrides = ClusterDiagnosticsProcessor.computeOverrides(b14Signals)

    DateSnapshot(date, metrics, b14Signals, driverOverrides)
  }

  private def loadReferenceConfigs(refOutputDir: File): Map[String, TunedClusterConfig] = {
    if (!refOutputDir.exists()) return Map.empty
    val files = Option(refOutputDir.listFiles())
      .getOrElse(Array.empty)
      .filter(_.getName.endsWith("-auto-scale-tuned.json"))
      .sortBy(_.getName)

    files.flatMap { f =>
      try {
        val config = SimpleJsonParser.parseFile(f)
        Some(config.clusterName -> config)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to parse reference config ${f.getName}: ${e.getMessage}")
          None
      }
    }.toMap
  }

  private def emitReferenceConfigs(clusterName: String, refOutputDir: File, curOutputDir: File): Unit = {
    Seq("-auto-scale-tuned.json", "-manually-tuned.json").foreach { suffix =>
      val refFile = new File(refOutputDir, s"$clusterName$suffix")
      if (refFile.exists()) {
        val content = scala.io.Source.fromFile(refFile).mkString
        ClusterMachineAndRecipeTuner.writeFile(curOutputDir, s"$clusterName$suffix", content)
      }
    }
  }

  private def applyB16Reboosting(clusterName: String, outputDir: File, snapshot: DateSnapshot, factor: Double): Int = {
    val inputDir = new File(s"$BasePath/inputs/${snapshot.date}")
    if (!inputDir.exists()) return 0

    val vitamins: Seq[RefinementVitamin] = Seq(new MemoryHeapBoostVitamin(factor))
    var totalBoosts = 0

    Seq("-auto-scale-tuned.json", "-manually-tuned.json").foreach { suffix =>
      val jsonFile = new File(outputDir, s"$clusterName$suffix")
      if (jsonFile.exists()) {
        try {
          val config = SimpleJsonParser.parseFile(jsonFile)
          val result = RefinementPipeline.refine(config, vitamins, inputDir)
          if (result.appliedBoosts.nonEmpty) {
            val refinedJson = RefinementPipeline.toRefinedJson(result)
            ClusterMachineAndRecipeTuner.writeFile(outputDir, jsonFile.getName, refinedJson)
            totalBoosts += result.appliedBoosts.size
            logger.info(s"  b16 reboosting applied to $clusterName$suffix: ${result.appliedBoosts.size} boost(s)")
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to apply b16 reboosting to $clusterName$suffix: ${e.getMessage}")
        }
      }
    }
    totalBoosts
  }

  private def resolveStrategy(name: String): TuningStrategy = name.toLowerCase match {
    case "cost_biased" => CostBiasedStrategy
    case "performance_biased" => PerformanceBiasedStrategy
    case _ => DefaultTuningStrategy
  }

  private def familyOf(machineName: String): String = {
    val parts = machineName.split("-")
    if (parts.length >= 2) parts(0) else machineName
  }

  private[auto] def writeAutoTunerSummaryReport(
    outputDir: File,
    refDate: String,
    curDate: String,
    strategyName: String,
    allTrends: Seq[TrendAssessment],
    kept: Int,
    boosted: Int,
    fresh: Int,
    preserved: Int,
    skipped: Int,
    b14Boosts: Seq[(String, String)],
    b16Boosts: Seq[(String, String, Int)],
    correlationCount: Int,
    divergenceCount: Int
  ): Unit = {
    val trendCounts = allTrends.groupBy(_.trend.label).mapValues(_.size)
    val totalRecipes = allTrends.size
    val totalClusters = allTrends.map(_.cluster).distinct.size

    val sb = new StringBuilder
    sb.append("=" * 72).append("\n")
    sb.append("  AUTO-TUNER GENERATION SUMMARY\n")
    sb.append("=" * 72).append("\n\n")
    sb.append(s"  Reference date:  $refDate\n")
    sb.append(s"  Current date:    $curDate\n")
    sb.append(s"  Strategy:        $strategyName\n")
    sb.append(s"  Generated at:    ${java.time.Instant.now()}\n\n")

    sb.append("-" * 72).append("\n")
    sb.append("  TREND SUMMARY\n")
    sb.append("-" * 72).append("\n")
    sb.append(f"  Total clusters:      $totalClusters%d\n")
    sb.append(f"  Total recipes:       $totalRecipes%d\n")
    sb.append(f"    Improved:          ${trendCounts.getOrElse("improved", 0)}%d\n")
    sb.append(f"    Degraded:          ${trendCounts.getOrElse("degraded", 0)}%d\n")
    sb.append(f"    Stable:            ${trendCounts.getOrElse("stable", 0)}%d\n")
    sb.append(f"    New entries:       ${trendCounts.getOrElse("new_entry", 0)}%d\n")
    sb.append(f"    Dropped entries:   ${trendCounts.getOrElse("dropped_entry", 0)}%d\n\n")

    sb.append("-" * 72).append("\n")
    sb.append("  EVOLUTION ACTIONS (cluster level)\n")
    sb.append("-" * 72).append("\n")
    sb.append(f"  Kept (stable/improved):   $kept%d\n")
    sb.append(f"  Boosted (degraded):       $boosted%d\n")
    sb.append(f"  Fresh (new):              $fresh%d\n")
    sb.append(f"  Preserved (historical):   $preserved%d\n")
    sb.append(f"  Skipped (no metrics):     $skipped%d\n\n")

    sb.append("-" * 72).append("\n")
    sb.append("  b14 DRIVER BOOSTS\n")
    sb.append("-" * 72).append("\n")
    if (b14Boosts.isEmpty) {
      sb.append("  (none)\n")
    } else {
      b14Boosts.foreach { case (cluster, reason) =>
        sb.append(s"  $cluster\n")
        sb.append(s"    Reason: $reason\n")
      }
    }
    sb.append("\n")

    sb.append("-" * 72).append("\n")
    sb.append("  b16 OOM REBOOSTING\n")
    sb.append("-" * 72).append("\n")
    if (b16Boosts.isEmpty) {
      sb.append("  (none)\n")
    } else {
      b16Boosts.foreach { case (cluster, suffix, count) =>
        sb.append(s"  $cluster ($suffix): $count recipe(s) boosted\n")
      }
    }
    sb.append("\n")

    sb.append("-" * 72).append("\n")
    sb.append("  STATISTICAL ANALYSIS\n")
    sb.append("-" * 72).append("\n")
    sb.append(f"  Correlation pairs computed: $correlationCount%d\n")
    sb.append(f"  Divergences detected:      $divergenceCount%d\n\n")

    sb.append("=" * 72).append("\n")

    ClusterMachineAndRecipeTuner.writeFile(outputDir, "_generation_summary_auto_tuner.txt", sb.toString())
    logger.info(s"Auto-tuner summary report written to ${outputDir.getPath}/_generation_summary_auto_tuner.txt")
  }

  private def logTrendSummary(trends: Seq[TrendAssessment]): Unit = {
    val counts = trends.groupBy(_.trend.label).mapValues(_.size)
    logger.info(s"Trend summary: improved=${counts.getOrElse("improved", 0)} " +
      s"degraded=${counts.getOrElse("degraded", 0)} " +
      s"stable=${counts.getOrElse("stable", 0)} " +
      s"new=${counts.getOrElse("new_entry", 0)} " +
      s"dropped=${counts.getOrElse("dropped_entry", 0)}")
  }
}
