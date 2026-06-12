package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.ClusterMachineAndRecipeTuner.AutoscalingPolicyConfig
import com.db.serna.orchestration.cluster_tuning.single.{
  CapacityGuard,
  CapacityStatus,
  ClusterDiagnosticsProcessor,
  ClusterMachineAndRecipeTuner,
  ClusterSummary,
  CostBiasedStrategy,
  DefaultTuningStrategy,
  DriverResourceOverride,
  ExecutorTopologyPreset,
  GenerationSummary,
  GenerationSummaryEntry,
  GenerationSummaryWriter,
  GuardResult,
  MachineCatalog,
  PerformanceBiasedStrategy,
  QuotaTracker,
  RecipeMetrics,
  TuningStrategy,
  YarnDriverEviction
}
import com.db.serna.orchestration.cluster_tuning.single.refinement.{
  BoostState,
  CapacityGuardBoost,
  CapacityGuardSignal,
  CapacityGuardVitamin,
  ExecutorScaleBoost,
  ExecutorScaleSignal,
  ExecutorScaleVitamin,
  ExecutorTrendVitamin,
  MemoryHeapBoost,
  MemoryHeapBoostVitamin,
  RefinementPipeline,
  RefinementVitamin,
  ScaleDirection,
  ScaleGains,
  SimpleJsonParser,
  TrendScaleBoost,
  TrendScaleDecision,
  TrendScaleSignal,
  TunedClusterConfig
}
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
  banner("""
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

  val topology: ScallopOption[String] = opt[String](
    default = None,
    descr = "Executor topology preset override (e.g. 8cx1GBpc, 8cx2GBpc, 4cx2GBpc). " +
      "When set, overrides the topology baked into the chosen strategy.",
    validate = label => ExecutorTopologyPreset.fromLabel(label).isDefined
  )

  val divergenceZThreshold: ScallopOption[Double] = opt[Double](
    default = Some(2.0),
    descr = "Z-score threshold for outlier divergence detection (default: 2.0)"
  )

  val executorScaleFactor: ScallopOption[Double] = opt[Double](
    default = Some(1.5),
    descr = "Boost factor applied to spark.dynamicAllocation.maxExecutors when a paired " +
      "recipe is a duration outlier and is cap-touching. Pass 1.0 to disable.",
    validate = f => f >= 1.0 && f <= 5.0
  )

  val scaleZThreshold: ScallopOption[Double] = opt[Double](
    default = Some(3.0),
    descr = "Min positive z-score on avg/p95 job duration that triggers an executor scale-up (default: 3.0)",
    validate = z => z >= 0.0
  )

  val scaleCapTouchRatio: ScallopOption[Double] = opt[Double](
    default = Some(0.5),
    descr = "Cap-touching threshold: scale-up only fires when p95_run_max_executors / current maxExecutors >= this " +
      "(default: 0.5 — permissive so duration outliers fire even when execs aren't fully saturated; raise to 0.85 to require near-full saturation)",
    validate = r => r > 0.0 && r <= 1.0
  )

  val maxClusterUtilRatio: ScallopOption[Double] = opt[Double](
    default = Some(0.90),
    descr = "Hard ceiling on a recipe's executors as a fraction of the cluster's per-node-packed scaled-max capacity " +
      "(cores AND memory). Prevents un-schedulable over-allocation (default: 0.90).",
    validate = r => r > 0.0 && r <= 1.0
  )

  // ── Trend-driven proportional executor scaling (longitudinal reference→current duration trend) ──
  // These tune the new ExecutorTrendScaler. Defaults come from the active bias preset (ScaleGains.fromBias);
  // any flag set here overrides that preset. All are back-compatible (omit them to keep the bias defaults).
  val trendScaleGain: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Override the up-scale gain on maxExecutors (default: bias preset). Higher = more aggressive scale-up.",
    validate = g => g >= 0.0 && g <= 2.0
  )
  val trendMinGain: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Override the up-scale gain on minExecutors (default: bias preset, normally < trend-scale-gain).",
    validate = g => g >= 0.0 && g <= 2.0
  )
  val trendScaleDeadband: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Fractional duration increase required to trigger trend scale-up (default: 0.10).",
    validate = d => d >= 0.0 && d < 1.0
  )
  val trendScaleMaxStep: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Per-run multiplicative clamp on trend scale-up (default: bias preset).",
    validate = s => s >= 1.0 && s <= 5.0
  )
  val trendScaleMinRuns: ScallopOption[Long] = opt[Long](
    default = None,
    descr = "Minimum runs on each side for a usable duration ratio (default: 5).",
    validate = r => r >= 0L
  )
  val trendDownscaleEnabled: ScallopOption[Boolean] = toggle(
    default = Some(true),
    descrYes = "Enable conservative trend-driven scale-DOWN when jobs speed up (default: on).",
    descrNo = "Disable trend-driven scale-down (up-only)."
  )

  verify()
}

/**
 * Auto-tuner entry point.
 *
 * Unlike the one-off [[ClusterMachineAndRecipeTuner]], this auto-tuner compares metrics across two dates (reference and
 * current) to detect performance trends and evolve configurations. It reuses the one-off tuner's core planning methods
 * while adding temporal awareness.
 */
object ClusterMachineAndRecipeAutoTuner {
  private val logger = LoggerFactory.getLogger(getClass)

  // Resolved at every read so the dashboard's TunerService can override via
  // `-DclusterTuning.basePath=…` before invoking run(). Default preserves the
  // historical IntelliJ-from-project-root behaviour.
  private[auto] def BasePath: String =
    System.getProperty("clusterTuning.basePath", "src/main/resources/composer/dwh/config/cluster_tuning")

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
    val executorScaleFactor = conf.executorScaleFactor()
    val scaleZThreshold = conf.scaleZThreshold()
    val scaleCapTouchRatio = conf.scaleCapTouchRatio()
    val maxClusterUtilRatio = conf.maxClusterUtilRatio()

    logger.info(
      s"AutoTuner starting: reference=$refDate current=$curDate strategy=$strategyName " +
        s"keepHistorical=$keepHistorical b16Factor=$b16Factor executorScaleFactor=$executorScaleFactor " +
        s"scaleZThreshold=$scaleZThreshold scaleCapTouchRatio=$scaleCapTouchRatio"
    )

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

    val pairs: Seq[MetricsPair] = commonKeys.toSeq.map { case key @ (cluster, recipe) =>
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

    // 4. Statistical analysis — fleet-wide, per-cluster, and current-snapshot views.
    //    Correlations on deltas exclude new entries by design (no reference); the
    //    current-snapshot view computes the same pairs on raw current values so
    //    new jobs are visible too.
    val correlations = StatisticalAnalysis.computeCorrelations(pairs)
    val divergences = StatisticalAnalysis.detectDivergences(pairs, zThreshold)
    val correlationsPerCluster = StatisticalAnalysis.computePerClusterCorrelations(pairs)
    val divergencesPerCluster = StatisticalAnalysis.detectPerClusterDivergences(pairs, zThreshold)
    val correlationsCurrentSnapshot = StatisticalAnalysis.computeCurrentSnapshotCorrelations(curSnapshot, refKeys)
    val divergencesCurrentSnapshot = StatisticalAnalysis.detectCurrentSnapshotZScores(curSnapshot, refKeys, zThreshold)

    // Scatter points keyed by "metricA__metricB" for each view — frontend draws mini scatter plots from these.
    val scatterDataDelta: Map[String, Seq[ScatterPoint]] =
      StatisticalAnalysis.correlationPairsDelta.map { case (a, b) =>
        StatisticalAnalysis.scatterKey(a, b) -> StatisticalAnalysis.scatterPointsDelta(pairs, a, b)
      }.toMap
    val scatterDataCurrent: Map[String, Seq[ScatterPoint]] =
      StatisticalAnalysis.correlationPairsCurrentSnapshot.map { case (a, b) =>
        StatisticalAnalysis
          .scatterKey(a, b) -> StatisticalAnalysis.scatterPointsCurrentSnapshot(curSnapshot, refKeys, a, b)
      }.toMap

    logger.info(
      s"Correlations computed: ${correlations.size} delta + ${correlationsCurrentSnapshot.size} current-snapshot " +
        s"+ ${correlationsPerCluster.size} per-cluster groups. " +
        s"Divergences: ${divergences.size} delta + ${divergencesCurrentSnapshot.size} current-snapshot " +
        s"+ ${divergencesPerCluster.size} per-cluster groups."
    )

    // 4b. Build executor-scale signals from the current-snapshot divergences.
    //     Rule: positive z >= scaleZThreshold on a duration metric, NOT a new entry,
    //     and cap-touching (p95_run_max_executors / current maxExecutors >= ratio).
    //     Cap-touching is checked here against the recipe's CURRENT metrics — the
    //     vitamin will gate further on the recipe's actual maxExecutors when applying.
    val durationMetrics = Set("avg_job_duration_ms", "p95_job_duration_ms")
    val scaleSignalsByCluster: Map[String, Seq[ExecutorScaleSignal]] =
      if (executorScaleFactor > 1.0) {
        divergencesCurrentSnapshot
          .filter(d =>
            d.isOutlier && !d.isNewEntry && durationMetrics.contains(d.metricName) && d.zScore >= scaleZThreshold
          )
          .flatMap { d =>
            curSnapshot.metrics.get((d.cluster, d.recipe)).map { m =>
              ExecutorScaleSignal(
                clusterName = d.cluster,
                recipeFilename = d.recipe,
                jobId = "",
                metricName = d.metricName,
                zScore = d.zScore,
                currentMaxExecutors = 0, // resolved by the vitamin from the parsed recipe
                p95RunMaxExecutors = m.p95RunMaxExecutors
              )
            }
          }
          .groupBy(_.clusterName)
      } else {
        Map.empty
      }
    if (scaleSignalsByCluster.nonEmpty) {
      val total = scaleSignalsByCluster.valuesIterator.map(_.size).sum
      logger.info(
        s"Executor scale-up: $total candidate recipe(s) across ${scaleSignalsByCluster.size} cluster(s) (z>=$scaleZThreshold, capRatio>=$scaleCapTouchRatio)"
      )
    }

    // 5. Load reference output configs for KeepAsIs/PreserveHistorical paths
    val refOutputDir = new File(s"$BasePath/outputs/$refDate")
    val referenceConfigs: Map[String, TunedClusterConfig] = loadReferenceConfigs(refOutputDir)

    // 6. Decide evolutions
    val decisions = PerformanceEvolver.decideEvolutions(
      allTrends,
      referenceConfigs,
      curSnapshot.metrics,
      refSnapshot.metrics,
      keepHistorical
    )

    // 7. Resolve strategy (with optional topology override)
    val baseStrategy = resolveStrategy(strategyName)
    val tuningStrategy: TuningStrategy = conf.topology.toOption
      .flatMap(ExecutorTopologyPreset.fromLabel)
      .map(topo => TuningStrategy.withTopology(baseStrategy, topo))
      .getOrElse(baseStrategy)
    val defaultMachine = MachineCatalog.byName("e2-standard-8").get
    val policy = tuningStrategy.toTuningPolicy(defaultMachine)
    val quotaTracker = new QuotaTracker(tuningStrategy.quotas)

    // Trend-scale gains derived from the active bias, with optional CLI overrides. Drives the new
    // longitudinal ExecutorTrendScaler (reference→current duration trend → executor min/max/instances).
    val trendGains: ScaleGains = ScaleGains.fromBias(
      tuningStrategy.biasMode,
      gainOverride = conf.trendScaleGain.toOption,
      maxStepOverride = conf.trendScaleMaxStep.toOption,
      deadbandUpOverride = conf.trendScaleDeadband.toOption,
      minRunsOverride = conf.trendScaleMinRuns.toOption,
      downscaleEnabledOverride = conf.trendDownscaleEnabled.toOption
    )
    // One trend signal per paired (reference, current) recipe. clusterMaxTotalCores is filled in at apply
    // time from the cluster's emitted JSON (clusterConf.cluster_max_total_cores); 0 here means "derive then".
    val trendSignalsByCluster: Map[String, Seq[TrendScaleSignal]] =
      pairs
        .map(p => TrendScaleSignal(p.cluster, p.recipe, p.reference, p.current, clusterMaxTotalCores = 0))
        .groupBy(_.clusterName)

    // 8. Generate outputs
    // Outputs share the same per-date dir as the single tuner so that subsequent
    // AutoTuner runs can read the previous run's output via loadReferenceConfigs
    // (which always reads outputs/<refDate>/). This enables continuous tuning
    // chains: ref=04_23 cur=04_30 -> outputs/2026_04_30/, then ref=04_30 cur=05_07
    // can find the just-written cumulative-boosted configs.
    val curOutputDir = new File(s"$BasePath/outputs/$curDate")
    if (!curOutputDir.exists()) curOutputDir.mkdirs()

    val tunerVersion = ClusterMachineAndRecipeTuner.toTunerVersion(curDate)
    val dagByCluster = ClusterMachineAndRecipeTuner.loadDagClusterRelationshipMap()
    val timerByCluster = ClusterMachineAndRecipeTuner.loadDagClusterCreationTimeMap()

    val summaries = ArrayBuffer.empty[ClusterSummary]
    val summaryEntries = ArrayBuffer.empty[GenerationSummaryEntry]

    // Tracking for the summary report
    val b14BoostedClusters = ArrayBuffer.empty[(String, String)] // (cluster, reason) — new + re-applied this run
    val b14HoldingClusters =
      ArrayBuffer.empty[(String, String)] // (cluster, "from -> to") — boost holding from prior run
    // Per-cluster b14 state ("new" or "re-boost") derived from the ref config's
    // applied_driver_promotion tag — mirrors b16's tag-based re-boost detection so a
    // promotion that held across a run with no eviction is still labelled as re-boost
    // when the next eviction arrives.
    val b14StateByCluster = scala.collection.mutable.Map.empty[String, String]
    val b16BoostedRecipes = ArrayBuffer.empty[(String, Seq[MemoryHeapBoost])] // (cluster, boosts)
    val executorScaleBoostedRecipes = ArrayBuffer.empty[(String, Seq[ExecutorScaleBoost])] // (cluster, boosts)
    // (cluster, decisions) — only recipes the trend scaler actually changed. Consumed by the summary boost group.
    val trendScaledRecipes = ArrayBuffer.empty[(String, Seq[TrendScaleDecision])]
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

      val preservedRecipes: Set[String] = clusterDecisions.collect {
        case d if d.action == PreserveHistorical => d.recipe
      }.toSet

      primaryAction match {
        case KeepAsIs | PreserveHistorical =>
          // Re-emit reference configs verbatim, tagging any preserve_historical
          // recipes with lastTunedDate + keptWithoutCurrentDate.
          if (primaryAction == PreserveHistorical) preservedClusters += 1 else keptClusters += 1
          emitReferenceConfigs(clusterName, refOutputDir, curOutputDir, preservedRecipes, refDate)

          // b14 holding: ref config carries a previously-applied driver promotion AND
          // the current date has NO new eviction signal → boost is "holding". The tag
          // (`applied_driver_promotion: "from -> to"`) was stamped by an earlier run.
          val curHasB14 = curSnapshot.b14Signals.get(clusterName).exists(_.nonEmpty)
          if (!curHasB14) {
            referenceConfigs
              .get(clusterName)
              .flatMap { rc =>
                rc.clusterConfFields.find(_._1 == "applied_driver_promotion").map(_._2.replaceAll("\"", ""))
              }
              .foreach(promotion => b14HoldingClusters += ((clusterName, promotion)))
          }

          // Compute cost_timeline from current-date b20/b21 data and inject it
          // into the just-emitted JSON files. The reference JSON may have been
          // produced before b20/b21 CSVs existed, so we always recompute from
          // the current snapshot's spans/events. Machine types are read from the
          // reference config (what is actually deployed).
          val curSpansKept = curSnapshot.clusterSpans.getOrElse(clusterName, Seq.empty)
          val curEventsKept = curSnapshot.autoscalerEvents.getOrElse(clusterName, Seq.empty)
          if (curSpansKept.nonEmpty || curEventsKept.nonEmpty) {
            val refConfig = referenceConfigs.get(clusterName)
            val workerName =
              refConfig.flatMap(_.clusterConfFields.find(_._1 == "worker_machine_type").map(_._2.replaceAll("\"", "")))
            val masterName =
              refConfig.flatMap(_.clusterConfFields.find(_._1 == "master_machine_type").map(_._2.replaceAll("\"", "")))
            val workerMachine = workerName.flatMap(MachineCatalog.byName).getOrElse(defaultMachine)
            val masterMachine = masterName.flatMap(MachineCatalog.byName).getOrElse(defaultMachine)
            val numWorkersStr =
              refConfig.flatMap(_.clusterConfFields.find(_._1 == "num_workers").map(_._2.replaceAll("\"", "")))
            val fallbackWorkers = numWorkersStr.flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(2)
            val breakdownKept = ClusterMachineAndRecipeTuner.computeClusterCost(
              spans = curSpansKept,
              events = curEventsKept,
              worker = workerMachine,
              master = masterMachine,
              fallbackWorkers = fallbackWorkers
            )
            breakdownKept.costTimelineJson.foreach { ctJson =>
              injectCostTimelineIntoOutputs(clusterName, curOutputDir, ctJson)
            }
          }

          // Apply b16 reboosting even for kept/preserved clusters: OOM signals
          // from reference_date must persist to prevent regression into OOM.
          if (b16Factor > 1.0) {
            val b16InputDirs = Seq(
              new File(s"$BasePath/inputs/$curDate"),
              new File(s"$BasePath/inputs/$refDate")
            )
            val boosts = applyB16Reboosting(clusterName, curOutputDir, b16InputDirs, b16Factor)
            if (boosts.nonEmpty) {
              b16BoostedRecipes += ((clusterName, boosts))
            }
          }

          // Trend-driven scaling runs first; the z-score pass below adds an extra boost for outliers.
          // On the kept path the prior trend factor is already present in the copied reference JSON,
          // so the scaler Holds/ReBoosts/relaxes from it without a separate carry step.
          val keptTrendSignals = trendSignalsByCluster.getOrElse(clusterName, Seq.empty)
          val keptTrendDecisions = applyTrendScaling(clusterName, curOutputDir, keptTrendSignals, trendGains)
          if (keptTrendDecisions.exists(_.changed)) {
            trendScaledRecipes += ((clusterName, keptTrendDecisions.filter(_.changed)))
          }

          // Apply divergence-driven executor scale-up after b16 (b16 sets memory;
          // scale-up sets the autoscaling ceiling — order is independent, but
          // running scale-up second keeps both passes deterministic).
          if (executorScaleFactor > 1.0) {
            val signals = scaleSignalsByCluster.getOrElse(clusterName, Seq.empty)
            val boosts =
              applyExecutorScaling(clusterName, curOutputDir, signals, executorScaleFactor, scaleCapTouchRatio)
            if (boosts.nonEmpty) {
              executorScaleBoostedRecipes += ((clusterName, boosts))
            }
          }

          // Final safety pass: never let a recipe out-request the cluster's schedulable capacity.
          applyCapacityGuard(clusterName, curOutputDir, maxClusterUtilRatio)

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
              clusterName,
              recMetrics,
              policy,
              quotaTracker,
              tuningStrategy.machinePreference
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
            val driverOverride: Option[DriverResourceOverride] =
              curSnapshot.driverOverrides.get(clusterName).map { curOverride =>
                // Find the best known master: reference config's (already promoted) vs fresh plan's
                val refConfig = referenceConfigs.get(clusterName)
                val refMasterName = refConfig
                  .flatMap(_.clusterConfFields.find(_._1 == "master_machine_type").map(_._2.replaceAll("\"", "")))
                val refMaster = refMasterName.flatMap(MachineCatalog.byName)
                val baseline = refMaster match {
                  case Some(rm) if rm.memoryGb >= clusterPlan.masterMachineType.memoryGb => rm
                  case _ => clusterPlan.masterMachineType
                }

                // Always promote: standard→highmem, highmem→more cores, core cap→cross-family
                val promoted = ClusterDiagnosticsProcessor.promoteMasterForEviction(baseline)

                val isPersistent = refSnapshot.b14Signals.get(clusterName).exists(_.nonEmpty)
                val reason = if (isPersistent) {
                  val refEvictions =
                    refSnapshot.b14Signals(clusterName).collect { case e: YarnDriverEviction => e.evictionCount }.sum
                  val curEvictions = curSnapshot.b14Signals
                    .getOrElse(clusterName, Seq.empty)
                    .collect { case e: YarnDriverEviction => e.evictionCount }
                    .sum
                  s"Persistent b14 driver eviction (247): $refEvictions (ref) -> $curEvictions (cur). Promoted ${baseline.name} -> ${promoted.name}"
                } else {
                  s"Current-date b14 driver eviction (247). Promoted ${baseline.name} -> ${promoted.name}"
                }

                DriverResourceOverride(
                  clusterName,
                  curOverride.driverMemoryGb,
                  curOverride.driverCores,
                  curOverride.driverMemoryOverheadGb,
                  reason,
                  Some(promoted)
                )
              }

            // Track b14 boosts. State derivation mirrors b16: a recipe — or here, a
            // cluster — is "re-boost" iff the reference config carries the prior tag
            // (`applied_driver_promotion`), regardless of whether the reference DATE
            // happened to also have a b14 signal. This is stricter (and more accurate)
            // than reading `refSnapshot.b14Signals`, which misses the case where a
            // promotion held across an intermediate run with no eviction.
            if (driverOverride.isDefined) {
              b14BoostedClusters += ((clusterName, driverOverride.get.diagnosticReason))
              val refHasPromotionTag = referenceConfigs
                .get(clusterName)
                .exists(
                  _.clusterConfFields.exists(_._1 == "applied_driver_promotion")
                )
              b14StateByCluster(clusterName) = if (refHasPromotionTag) "re-boost" else "new"
            }

            // Detect prior promotion holding when the cluster was re-planned but the
            // current date has no fresh b14 signal. The reference config's tag is the
            // source of truth for "what was previously promoted".
            if (driverOverride.isEmpty) {
              referenceConfigs
                .get(clusterName)
                .flatMap { rc =>
                  rc.clusterConfFields.find(_._1 == "applied_driver_promotion").map(_._2.replaceAll("\"", ""))
                }
                .foreach(promotion => b14HoldingClusters += ((clusterName, promotion)))
            }

            // Compute current-date interval cost / wall-clock minutes / worker stats /
            // cost_timeline JSON in one pass. Empty spans yield zeros and None for
            // cost_timeline (frontend will render the empty-state notice on this side).
            val curSpans = curSnapshot.clusterSpans.getOrElse(clusterName, Seq.empty)
            val curEvents = curSnapshot.autoscalerEvents.getOrElse(clusterName, Seq.empty)
            if (curSpans.isEmpty) {
              logger.warn(
                s"No b20 cluster span for $clusterName in current date; estimated_cost_eur=0.0 and total_active_minutes=0.0 in summaries."
              )
            }
            val breakdown = ClusterMachineAndRecipeTuner.computeClusterCost(
              spans = curSpans,
              events = curEvents,
              worker = clusterPlan.workerMachineType,
              master = clusterPlan.masterMachineType,
              fallbackWorkers = clusterPlan.workers
            )

            val manualJsonStr = ClusterMachineAndRecipeTuner.manualJson(
              clusterPlan,
              manualPlans,
              tunerVersion,
              driverOverride,
              breakdown.costTimelineJson
            )
            val daJsonStr = ClusterMachineAndRecipeTuner.daJson(
              clusterPlan,
              daPlans,
              tunerVersion,
              driverOverride,
              breakdown.costTimelineJson
            )

            ClusterMachineAndRecipeTuner.writeFile(curOutputDir, s"$clusterName-manually-tuned.json", manualJsonStr)
            ClusterMachineAndRecipeTuner.writeFile(curOutputDir, s"$clusterName-auto-scale-tuned.json", daJsonStr)

            // Stamp the promotion tag so subsequent AutoTuner runs can detect "holding".
            // Format keeps it parseable by SimpleJsonParser.extractOrderedFlatFields.
            driverOverride.flatMap(_.promotedMasterMachineType).foreach { promoted =>
              val baselineName = referenceConfigs
                .get(clusterName)
                .flatMap(_.clusterConfFields.find(_._1 == "master_machine_type").map(_._2.replaceAll("\"", "")))
                .getOrElse(clusterPlan.masterMachineType.name)
              injectClusterConfTag(
                clusterName,
                curOutputDir,
                "applied_driver_promotion",
                s"$baselineName -> ${promoted.name}"
              )
            }

            // Carry over any preserve_historical recipes that were dropped in the
            // current date. The fresh plan was built only from current-date metrics,
            // so reference-only recipes would otherwise be lost. Merge their reference
            // blocks into the freshly written JSON, tagged with kept flags.
            if (preservedRecipes.nonEmpty) {
              mergePreservedRecipesIntoOutputs(clusterName, preservedRecipes, refOutputDir, curOutputDir, refDate)
            }

            // Carry forward prior b16 boost metadata (cumulative factor + boosted
            // memory + re-derived totals) into the freshly emitted JSON. Without
            // this, applyB16Reboosting cannot see the recipe was previously boosted
            // — Holding state would never trigger and the boost would be lost the
            // first run after the b16 CSV stops reporting the recipe.
            carryPriorBoostMetadata(clusterName, refOutputDir, curOutputDir)

            // Carry forward prior trend-scaling metadata (boosted min/initial/max or instances +
            // appliedTrendScaleFactor) into the freshly emitted JSON, for the same reason as the b16
            // carry above — otherwise a re-plan resets the recipe to baseline counts and the prior
            // trend boost is lost before applyTrendScaling can Hold/ReBoost from it.
            carryPriorTrendMetadata(clusterName, refOutputDir, curOutputDir)

            // Apply b16 reboosting: check both reference and current input dirs.
            // OOM signals from either date must persist to prevent regression.
            if (b16Factor > 1.0) {
              val b16InputDirs = Seq(
                new File(s"$BasePath/inputs/$curDate"),
                new File(s"$BasePath/inputs/$refDate")
              )
              val boosts = applyB16Reboosting(clusterName, curOutputDir, b16InputDirs, b16Factor)
              if (boosts.nonEmpty) {
                b16BoostedRecipes += ((clusterName, boosts))
              }
            }

            // Trend-driven scaling runs before the z-score pass (which adds an extra outlier boost on top).
            val freshTrendSignals = trendSignalsByCluster.getOrElse(clusterName, Seq.empty)
            val freshTrendDecisions = applyTrendScaling(clusterName, curOutputDir, freshTrendSignals, trendGains)
            if (freshTrendDecisions.exists(_.changed)) {
              trendScaledRecipes += ((clusterName, freshTrendDecisions.filter(_.changed)))
            }

            // Apply divergence-driven executor scale-up.
            if (executorScaleFactor > 1.0) {
              val signals = scaleSignalsByCluster.getOrElse(clusterName, Seq.empty)
              val boosts =
                applyExecutorScaling(clusterName, curOutputDir, signals, executorScaleFactor, scaleCapTouchRatio)
              if (boosts.nonEmpty) {
                executorScaleBoostedRecipes += ((clusterName, boosts))
              }
            }

            // Final safety pass: never let a recipe out-request the cluster's schedulable capacity.
            applyCapacityGuard(clusterName, curOutputDir, maxClusterUtilRatio)

            // Use the same interval-based wall-clock minutes + interval cost +
            // time-weighted worker stats already computed above (breakdown).
            summaries += ClusterSummary(
              clusterName = clusterName,
              dagId = dagByCluster.getOrElse(clusterName, "UNKNOWN_DAG_ID"),
              noOfJobs = recMetrics.size,
              numOfWorkers = clusterPlan.workers,
              workerMachineType = clusterPlan.workerMachineType.name,
              masterMachineType = clusterPlan.masterMachineType.name,
              realUsedAvgNumOfWorkers = breakdown.workerStats.avgWorkers,
              realUsedMinWorkers = breakdown.workerStats.minWorkers,
              realUsedMaxWorkers = breakdown.workerStats.maxWorkers,
              totalActiveMinutes = breakdown.totalActiveMinutes,
              estimatedCostEur = breakdown.estimatedCostEur,
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
              maxTotalCores =
                AutoscalingPolicyConfig.maxWorkersForCluster(clusterPlan.workers) * clusterPlan.workerMachineType.cores,
              diagnosticSignals = signals,
              strategyName = tuningStrategy.name,
              biasMode = tuningStrategy.biasMode.name,
              topologyPreset = tuningStrategy.executorTopology.label
            )
          }
      }
    }

    // 9. Write analysis outputs
    val newEntryCurrentMetrics: Map[(String, String), RecipeMetrics] =
      newKeys.toSeq.flatMap(k => curSnapshot.metrics.get(k).map(k -> _)).toMap
    val droppedEntryReferenceMetrics: Map[(String, String), RecipeMetrics] =
      droppedKeys.toSeq.flatMap(k => refSnapshot.metrics.get(k).map(k -> _)).toMap

    val analysisJson = AutoTunerJsonOutput.analysisOutputJson(
      referenceDate = refDate,
      currentDate = curDate,
      strategyName = strategyName,
      trends = allTrends,
      correlations = correlations,
      correlationsCurrentSnapshot = correlationsCurrentSnapshot,
      correlationsPerCluster = correlationsPerCluster,
      divergences = divergences,
      divergencesCurrentSnapshot = divergencesCurrentSnapshot,
      divergencesPerCluster = divergencesPerCluster,
      scatterDataDelta = scatterDataDelta,
      scatterDataCurrentSnapshot = scatterDataCurrent,
      newEntryCurrentMetrics = newEntryCurrentMetrics,
      droppedEntryReferenceMetrics = droppedEntryReferenceMetrics,
      decisions = decisions
    )
    ClusterMachineAndRecipeTuner.writeFile(curOutputDir, "_auto_tuner_analysis.json", analysisJson)
    AutoTunerJsonOutput.writeAnalysisCsvs(curOutputDir, allTrends, correlations, divergences, decisions)
    AutoTunerJsonOutput.writeAnalysesIndex(curOutputDir.getParentFile)

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
      curOutputDir,
      refDate,
      curDate,
      strategyName,
      allTrends,
      keptClusters,
      boostedClusters,
      freshClusters,
      preservedClusters,
      skippedClusters,
      b14BoostedClusters.toSeq,
      b16BoostedRecipes.toSeq,
      correlations.size,
      divergences.size,
      b14HoldingClusters.toSeq,
      b14StateByCluster.toMap,
      executorScaleBoostedRecipes.toSeq,
      trendScaledRecipes.toSeq
    )

    logger.info(s"AutoTuner finished. Output: ${curOutputDir.getPath}")
    logger.info(s"  Clusters processed: ${allClusterNames.size}")
    logger.info(s"  Analysis: _auto_tuner_analysis.json, _trend_summary.csv, _correlations.csv, _divergences.csv")
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private[auto] def loadSnapshot(date: String): DateSnapshot = {
    val cfg = ClusterMachineAndRecipeTuner.Config(useFlattened = true, date = date)
    val metrics =
      try {
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

    // b20/b21 power the per-date interval cost. Both are optional — when absent
    // the tuner emits estimated_cost_eur=0 with WARN and the frontend hides the
    // detail-cluster-cost section for that side.
    val clusterSpans =
      try {
        ClusterMachineAndRecipeTuner.loadClusterSpans(cfg)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to load cluster spans for $date: ${e.getMessage}")
          Map.empty[String, Seq[com.db.serna.orchestration.cluster_tuning.single.ClusterSpan]]
      }
    val autoscalerEvents =
      try {
        ClusterMachineAndRecipeTuner.loadAutoscalerEvents(cfg)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to load autoscaler events for $date: ${e.getMessage}")
          Map.empty[String, Seq[com.db.serna.orchestration.cluster_tuning.single.AutoscalerEvent]]
      }

    DateSnapshot(date, metrics, b14Signals, driverOverrides, clusterSpans, autoscalerEvents)
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

  /**
   * Re-emit the cluster's reference JSONs into the current output dir.
   *
   * Recipes named in `preservedRecipes` (those whose evolution decision was `PreserveHistorical`) are tagged with
   * `lastTunedDate` and `keptWithoutCurrentDate: true`. If the reference recipe already carries a `lastTunedDate`, that
   * older value is preserved (recursive carry across multiple consecutive auto-tuner runs). Recipes not in
   * `preservedRecipes` are passed through unchanged — they are still in current data, just not re-planned.
   */
  private def emitReferenceConfigs(
      clusterName: String,
      refOutputDir: File,
      curOutputDir: File,
      preservedRecipes: Set[String],
      refDate: String
  ): Unit = {
    Seq("-auto-scale-tuned.json", "-manually-tuned.json").foreach { suffix =>
      val refFile = new File(refOutputDir, s"$clusterName$suffix")
      if (refFile.exists()) {
        val content = scala.io.Source.fromFile(refFile).mkString
        val tagged =
          if (preservedRecipes.nonEmpty)
            KeptRecipeCarrier.tagPreservedRecipes(content, preservedRecipes, refDate)
          else content
        ClusterMachineAndRecipeTuner.writeFile(curOutputDir, s"$clusterName$suffix", tagged)
      }
    }
  }

  /**
   * Inject a `cost_timeline` JSON fragment into already-written per-cluster JSONs. Used by the
   * KeepAsIs/PreserveHistorical path to attach current-date b20/b21 cost data to reference configs that were copied
   * verbatim.
   *
   * If the JSON already contains a `cost_timeline` key it is replaced; otherwise the fragment is appended as a new
   * top-level field.
   */
  private def injectCostTimelineIntoOutputs(
      clusterName: String,
      outputDir: File,
      costTimelineJson: String
  ): Unit = {
    val q = '"'
    Seq("-auto-scale-tuned.json", "-manually-tuned.json").foreach { suffix =>
      val file = new File(outputDir, s"$clusterName$suffix")
      if (file.exists()) {
        val content = scala.io.Source.fromFile(file).mkString
        val ctKey = s"${q}cost_timeline${q}"
        val updated = if (content.contains(ctKey)) {
          // Replace existing cost_timeline value (regex: "cost_timeline" : <json-object>)
          // This is a simple heuristic; the JSON is machine-generated and well-formed.
          content.replaceFirst(
            """"cost_timeline"\s*:\s*\{[^}]*(?:\{[^}]*\}[^}]*)*\}""",
            s"${q}cost_timeline${q}: $costTimelineJson"
          )
        } else {
          // Insert before the final closing brace
          val idx = content.lastIndexOf('}')
          if (idx > 0) {
            val before = content.substring(0, idx).stripTrailing()
            val needsComma = before.nonEmpty && !before.endsWith("{") && !before.endsWith(",")
            val comma = if (needsComma) "," else ""
            before + comma + "\n  " + ctKey + ": " + costTimelineJson + "\n}"
          } else content
        }
        ClusterMachineAndRecipeTuner.writeFile(outputDir, s"$clusterName$suffix", updated)
      }
    }
  }

  /**
   * Inject a flat string field into the per-cluster `clusterConf.<clusterName>` block. Idempotent: if the key already
   * exists, the existing value is replaced.
   *
   * Used to stamp `applied_driver_promotion` on cluster JSONs whenever the AutoTuner applies a master promotion, so
   * later runs can detect "holding" state (the recipe carries the promotion forward without a new b14 eviction signal).
   */
  private def injectClusterConfTag(clusterName: String, outputDir: File, tagKey: String, tagValue: String): Unit = {
    val q = '"'
    Seq("-auto-scale-tuned.json", "-manually-tuned.json").foreach { suffix =>
      val file = new File(outputDir, s"$clusterName$suffix")
      if (file.exists()) {
        val content = scala.io.Source.fromFile(file).mkString
        val pat = ("\"" + java.util.regex.Pattern.quote(clusterName) + "\"\\s*:\\s*\\{").r
        val updated = pat.findFirstMatchIn(content) match {
          case Some(m) =>
            val insertPos = m.end
            // Replace existing tag (same key) anywhere in the cluster block; otherwise insert.
            val keyToken = s"$q$tagKey$q"
            val tagLine = s"\n    $keyToken: $q$tagValue$q,"
            val existingPattern = (java.util.regex.Pattern.quote(keyToken) + "\\s*:\\s*\"[^\"]*\",?").r
            existingPattern.findFirstMatchIn(content) match {
              case Some(em) =>
                content.substring(0, em.start) + s"$keyToken: $q$tagValue$q," + content.substring(em.end)
              case None =>
                content.substring(0, insertPos) + tagLine + content.substring(insertPos)
            }
          case None => content
        }
        ClusterMachineAndRecipeTuner.writeFile(outputDir, file.getName, updated)
      }
    }
  }

  /**
   * Merge preserve_historical recipes from the reference output JSON into the freshly written current-date JSON. Used
   * in the BoostResources / GenerateFresh branch where the cluster was re-planned from current-date metrics only — any
   * reference-only recipes would otherwise be silently dropped.
   *
   * Each carried recipe block is read verbatim from the reference, tagged with the kept flags, and inserted into the
   * fresh JSON's `recipeSparkConf`. If the fresh plan happens to also contain the same recipe name (would only happen
   * if metrics flickered back), the existing fresh entry is kept and the carry-over is skipped.
   */
  private def mergePreservedRecipesIntoOutputs(
      clusterName: String,
      preservedRecipes: Set[String],
      refOutputDir: File,
      curOutputDir: File,
      refDate: String
  ): Unit = {
    Seq("-auto-scale-tuned.json", "-manually-tuned.json").foreach { suffix =>
      val refFile = new File(refOutputDir, s"$clusterName$suffix")
      val curFile = new File(curOutputDir, s"$clusterName$suffix")
      if (!refFile.exists() || !curFile.exists()) {
        if (!refFile.exists()) logger.warn(s"Cannot carry over recipes: reference $refFile not found")
        // If curFile is missing, the fresh plan didn't produce this variant; nothing to merge into.
      } else {
        val refContent = scala.io.Source.fromFile(refFile).mkString
        val curContent = scala.io.Source.fromFile(curFile).mkString

        // Extract each preserved recipe's reference block and tag it.
        val taggedBlocks: Seq[(String, String)] = preservedRecipes.toSeq.flatMap { name =>
          KeptRecipeCarrier.extractRecipeBlock(refContent, name) match {
            case Some(block) =>
              val wrapper = s"""{"recipeSparkConf":{"$name":$block}}"""
              val tagged = KeptRecipeCarrier.tagPreservedRecipes(wrapper, Set(name), refDate)
              KeptRecipeCarrier.extractRecipeBlock(tagged, name).map(name -> _)
            case None =>
              logger.warn(s"Preserved recipe $name not found in $refFile; skipping carry-over.")
              None
          }
        }

        if (taggedBlocks.nonEmpty) {
          val merged = KeptRecipeCarrier.mergeRecipeBlocks(curContent, taggedBlocks)
          ClusterMachineAndRecipeTuner.writeFile(curOutputDir, s"$clusterName$suffix", merged)
          logger.info(s"Carried over ${taggedBlocks.size} preserve_historical recipe(s) into $clusterName$suffix")
        }
      }
    }
  }

  /**
   * Carry forward prior b16 boost metadata from the reference output JSON into the just-emitted current JSON. For each
   * recipe present in BOTH files, copy `appliedMemoryHeapBoostFactor`, the boosted `spark.executor.memory`, and
   * re-derive the totals from the current recipe's executor counts.
   *
   * This makes the downstream `applyB16Reboosting` correctly classify already- boosted recipes as `Holding` (or
   * `ReBoost` if a fresh signal arrives), even when the cluster was re-planned from scratch and the b16 CSV no longer
   * names the recipe.
   *
   * Both `-auto-scale-tuned.json` and `-manually-tuned.json` are processed.
   */
  private def carryPriorBoostMetadata(clusterName: String, refOutputDir: File, curOutputDir: File): Unit = {
    Seq("-auto-scale-tuned.json", "-manually-tuned.json").foreach { suffix =>
      val refFile = new File(refOutputDir, s"$clusterName$suffix")
      val curFile = new File(curOutputDir, s"$clusterName$suffix")
      if (refFile.exists() && curFile.exists()) {
        try {
          val refContent = scala.io.Source.fromFile(refFile).mkString
          val curContent = scala.io.Source.fromFile(curFile).mkString
          val refConfig = SimpleJsonParser.parse(refContent)
          val recipesWithPriorBoost: Set[String] = refConfig.recipes.collect {
            case (name, rc) if rc.extraFields.contains("appliedMemoryHeapBoostFactor") => name
          }.toSet
          if (recipesWithPriorBoost.nonEmpty) {
            val updated = BoostMetadataCarrier.injectPriorBoosts(curContent, refContent, recipesWithPriorBoost)
            if (updated ne curContent) {
              ClusterMachineAndRecipeTuner.writeFile(curOutputDir, curFile.getName, updated)
              logger.info(
                s"  Carried prior b16 boost metadata for ${recipesWithPriorBoost.size} recipe(s) into $clusterName$suffix"
              )
            }
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to carry prior boost metadata into $clusterName$suffix: ${e.getMessage}")
        }
      }
    }
  }

  /** Parallel to [[carryPriorBoostMetadata]] but for trend-scaling metadata (executor counts + factor). */
  private def carryPriorTrendMetadata(clusterName: String, refOutputDir: File, curOutputDir: File): Unit = {
    Seq("-auto-scale-tuned.json", "-manually-tuned.json").foreach { suffix =>
      val refFile = new File(refOutputDir, s"$clusterName$suffix")
      val curFile = new File(curOutputDir, s"$clusterName$suffix")
      if (refFile.exists() && curFile.exists()) {
        try {
          val refContent = scala.io.Source.fromFile(refFile).mkString
          val curContent = scala.io.Source.fromFile(curFile).mkString
          val refConfig = SimpleJsonParser.parse(refContent)
          val recipesWithPriorTrend: Set[String] = refConfig.recipes.collect {
            case (name, rc) if rc.extraFields.contains("appliedTrendScaleFactor") => name
          }.toSet
          if (recipesWithPriorTrend.nonEmpty) {
            val updated = BoostMetadataCarrier.injectPriorTrendScaling(curContent, refContent, recipesWithPriorTrend)
            if (updated ne curContent) {
              ClusterMachineAndRecipeTuner.writeFile(curOutputDir, curFile.getName, updated)
              logger.info(
                s"  Carried prior trend metadata for ${recipesWithPriorTrend.size} recipe(s) into $clusterName$suffix"
              )
            }
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to carry prior trend metadata into $clusterName$suffix: ${e.getMessage}")
        }
      }
    }
  }

  /**
   * Apply longitudinal trend-driven scaling to a cluster's tuned JSONs (auto-scale and manual). Runs BEFORE the z-score
   * [[ExecutorScaleVitamin]] so the z-score path adds an extra boost on top of the trend baseline. Capacity is read from
   * each file's `clusterConf.cluster_max_total_cores` divided by the recipe's `spark.executor.cores`. Returns the
   * decisions that were emitted (the vitamin only emits for recipes it touches — changed or carrying a prior factor).
   */
  private def applyTrendScaling(
      clusterName: String,
      outputDir: File,
      signals: Seq[TrendScaleSignal],
      gains: ScaleGains
  ): Seq[TrendScaleDecision] = {
    if (signals.isEmpty) return Seq.empty
    val fileNames = Seq(s"$clusterName-auto-scale-tuned.json", s"$clusterName-manually-tuned.json")
    val allDecisions = ArrayBuffer.empty[TrendScaleDecision]
    fileNames.foreach { fileName =>
      val file = new File(outputDir, fileName)
      if (file.exists()) {
        try {
          val config = SimpleJsonParser.parseFile(file)
          val clusterCores = config.clusterConfFields
            .find(_._1 == "cluster_max_total_cores")
            .flatMap { case (_, v) => scala.util.Try(v.toInt).toOption }
            .getOrElse(0)
          val enriched = signals.map(_.copy(clusterMaxTotalCores = clusterCores))
          val lookup: String => Seq[TrendScaleSignal] = c => if (c == clusterName) enriched else Seq.empty
          val vitamins: Seq[RefinementVitamin] = Seq(new ExecutorTrendVitamin(gains, lookup))
          val result = RefinementPipeline.refine(config, vitamins, Seq(outputDir))
          val decisions = result.appliedBoosts.collect { case b: TrendScaleBoost => b.decision }
          // Rewrite only when an executor count actually moved. A pure Holding decision leaves the file as-is;
          // its carried `appliedTrendScaleFactor` is already present (copied reference JSON on the kept path, or
          // injected by carryPriorTrendMetadata on the re-plan path), so there is nothing to re-stamp.
          if (decisions.exists(_.changed)) {
            ClusterMachineAndRecipeTuner.writeFile(outputDir, fileName, RefinementPipeline.toRefinedJson(result))
          }
          allDecisions ++= decisions
          val up = decisions.count(_.direction == ScaleDirection.Up)
          val down = decisions.count(_.direction == ScaleDirection.Down)
          val hold = decisions.count(_.direction == ScaleDirection.Hold)
          if (decisions.nonEmpty) {
            logger.info(s"  trend scaling on $fileName: $up up, $down down, $hold hold")
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Failed trend scaling for $clusterName/$fileName: ${e.getMessage}")
        }
      }
    }
    allDecisions.toSeq
  }

  /**
   * Final safety pass: clamp every recipe so it can never request more executors than the cluster can schedule, and
   * (re)stamp `maxCoreUsagePct` / `maxMemoryUsagePct`. Runs AFTER trend + z-score scaling. Capacity is read from the
   * clusterConf scaled-max fields written by the single tuner (`nodeCores = cluster_scaled_max_cores / max_workers`).
   *
   * Rewrites a file only when the guard clamps something OR the file is missing the `%` fields (which happens after a
   * trend/z-score rewrite dropped them). This preserves parity with the existing passes around `cost_timeline`: a
   * cluster untouched by any scaling keeps its single-tuner-emitted `%` (and `cost_timeline`); a scaled cluster — which
   * already lost `cost_timeline` to the trend/z-score rewrite — is re-stamped with fresh `%`.
   */
  private def applyCapacityGuard(clusterName: String, outputDir: File, ratio: Double): Seq[GuardResult] = {
    val fileNames = Seq(s"$clusterName-auto-scale-tuned.json", s"$clusterName-manually-tuned.json")
    val all = ArrayBuffer.empty[GuardResult]
    fileNames.foreach { fileName =>
      val file = new File(outputDir, fileName)
      if (file.exists()) {
        try {
          val config = SimpleJsonParser.parseFile(file)
          def confInt(k: String): Option[Int] =
            config.clusterConfFields.find(_._1 == k).flatMap { case (_, v) => scala.util.Try(v.toInt).toOption }
          val maxWorkers = confInt("max_workers").getOrElse(0)
          val scaledCores = confInt("cluster_scaled_max_cores").getOrElse(0)
          val scaledMem = confInt("cluster_scaled_max_memory_gb").getOrElse(0)
          if (maxWorkers <= 0 || scaledCores <= 0 || scaledMem <= 0) {
            logger.warn(s"capacity guard skipped for $clusterName/$fileName: missing scaled-max clusterConf fields")
          } else {
            val nodeCores = scaledCores / maxWorkers
            val nodeMem = scaledMem / maxWorkers
            val sigs =
              config.recipeOrder.map(r => CapacityGuardSignal(clusterName, r, nodeCores, nodeMem, maxWorkers, ratio))
            val lookup: String => Seq[CapacityGuardSignal] = c => if (c == clusterName) sigs else Seq.empty
            val result = RefinementPipeline.refine(config, Seq(new CapacityGuardVitamin(lookup)), Seq(outputDir))
            val decisions = result.appliedBoosts.collect { case b: CapacityGuardBoost => b.result }
            val clampedAny = decisions.exists(_.status != CapacityStatus.Ok)
            val pctMissing = {
              val have = config.rawJson.split("maxCoreUsagePct", -1).length - 1
              have < config.recipeOrder.size
            }
            if (clampedAny || pctMissing) {
              ClusterMachineAndRecipeTuner.writeFile(outputDir, fileName, RefinementPipeline.toRefinedJson(result))
            }
            all ++= decisions
            val clamped = decisions.count(_.status != CapacityStatus.Ok)
            if (clamped > 0) logger.info(s"  capacity guard on $fileName: $clamped recipe(s) clamped/tight/infeasible")
          }
        } catch {
          case e: Exception => logger.warn(s"Failed capacity guard for $clusterName/$fileName: ${e.getMessage}")
        }
      }
    }
    all.toSeq
  }

  /**
   * Apply b16 memory heap OOM reboosting to a cluster's output JSONs.
   *
   * `inputDirs` is ordered [current_date, reference_date, ...]. The order matters: the head is treated as the current
   * dir for state classification, so even when the current-date b16 CSV is missing the recipe is correctly classified
   * as `Holding` (carrying a previous boost without a new signal) instead of being re-boosted from the reference dir.
   * Skip the entire step only when NO dir carries a b16 CSV.
   *
   * Lifecycle states, surfaced via `MemoryHeapBoost.state`:
   *   - `New` : untagged recipe + fresh current-date OOM signal
   *   - `ReBoost` : already-tagged recipe + fresh current-date signal → stack on current memory
   *   - `Holding` : already-tagged recipe + no fresh current-date signal → carry forward
   */
  private def applyB16Reboosting(
      clusterName: String,
      outputDir: File,
      inputDirs: Seq[File],
      factor: Double
  ): Seq[MemoryHeapBoost] = {
    val anyHasB16 = inputDirs.exists(d => d.exists() && new File(d, "b16_oom_job_driver_exceptions.csv").exists())
    if (!anyHasB16) return Seq.empty

    val vitamins: Seq[RefinementVitamin] = Seq(new MemoryHeapBoostVitamin(factor))
    val allBoosts = ArrayBuffer.empty[MemoryHeapBoost]

    Seq("-auto-scale-tuned.json", "-manually-tuned.json").foreach { suffix =>
      val jsonFile = new File(outputDir, s"$clusterName$suffix")
      if (jsonFile.exists()) {
        try {
          val config = SimpleJsonParser.parseFile(jsonFile)
          // Pass full ordered dirs; the pipeline handles missing CSVs gracefully via
          // loadSignals returning empty. Crucially, inputDirs.head is preserved as the
          // current dir even if its CSV is absent.
          val result = RefinementPipeline.refine(config, vitamins, inputDirs)
          if (result.appliedBoosts.nonEmpty) {
            val refinedJson = RefinementPipeline.toRefinedJson(result)
            ClusterMachineAndRecipeTuner.writeFile(outputDir, jsonFile.getName, refinedJson)
            val heap = result.appliedBoosts.collect { case b: MemoryHeapBoost => b }
            allBoosts ++= heap
            val newCount = heap.count(_.state == BoostState.New)
            val reBoostCount = heap.count(_.state == BoostState.ReBoost)
            val holdingCount = heap.count(_.state == BoostState.Holding)
            logger.info(
              s"  b16 applied to $clusterName$suffix: $newCount new, $reBoostCount re-boost, $holdingCount holding"
            )
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to apply b16 reboosting to $clusterName$suffix: ${e.getMessage}")
        }
      }
    }
    allBoosts.toSeq
  }

  /**
   * Apply divergence-driven executor scale-up to a cluster's `-auto-scale-tuned.json`.
   *
   * The vitamin gates per recipe based on:
   *   - prior `appliedExecutorScaleFactor` tag (Holding / ReBoost lifecycle)
   *   - presence of a fresh signal in `signals`
   *   - cap-touching ratio (`p95RunMaxExecutors / current maxExecutors >= capTouchRatio`)
   *
   * Manual recipes are skipped — see `ExecutorScaleVitamin.computeBoosts`. Cap-touching is enforced here against the
   * recipe's actual maxExecutors.
   */
  private def applyExecutorScaling(
      clusterName: String,
      outputDir: File,
      signals: Seq[ExecutorScaleSignal],
      factor: Double,
      capTouchRatio: Double
  ): Seq[ExecutorScaleBoost] = {
    val daFile = new File(outputDir, s"$clusterName-auto-scale-tuned.json")
    if (!daFile.exists()) return Seq.empty

    val allBoosts = ArrayBuffer.empty[ExecutorScaleBoost]
    try {
      val config = SimpleJsonParser.parseFile(daFile)
      // Cap-touching gate: drop signals whose p95_run_max_executors is below the ratio
      // of the recipe's CURRENT maxExecutors. Recipes tagged from a prior run with no
      // fresh signal still pass through the vitamin in `Holding` state — that's by
      // design (the carried boost holds independently of fresh signals).
      val gatedSignals = signals.filter { s =>
        config.recipes.get(s.recipeFilename).exists { rc =>
          val curMax = rc.sparkOptsMap
            .get("spark.dynamicAllocation.maxExecutors")
            .flatMap(v => scala.util.Try(v.toInt).toOption)
            .getOrElse(0)
          curMax > 0 && (s.p95RunMaxExecutors / curMax.toDouble) >= capTouchRatio
        }
      }
      val signalsByCluster: String => Seq[ExecutorScaleSignal] = c => if (c == clusterName) gatedSignals else Seq.empty

      // Pass TWO dirs (any two; the vitamin ignores the dir and pulls signals from
      // the in-memory lookup). The pipeline routes to the date-aware 3-arg path only
      // when `inputDirs.size > 1`, which is what enables the New/ReBoost/Holding
      // lifecycle. Without two dirs, prior-tagged recipes with no fresh signal would
      // never be classified as Holding.
      val vitamins: Seq[RefinementVitamin] = Seq(new ExecutorScaleVitamin(factor, signalsByCluster))
      val result = RefinementPipeline.refine(config, vitamins, Seq(outputDir, outputDir))
      if (result.appliedBoosts.nonEmpty) {
        val refinedJson = RefinementPipeline.toRefinedJson(result)
        ClusterMachineAndRecipeTuner.writeFile(outputDir, daFile.getName, refinedJson)
        val scaled = result.appliedBoosts.collect { case b: ExecutorScaleBoost => b }
        allBoosts ++= scaled
        val newCount = scaled.count(_.state == BoostState.New)
        val reBoostCount = scaled.count(_.state == BoostState.ReBoost)
        val holdingCount = scaled.count(_.state == BoostState.Holding)
        logger.info(
          s"  executor scale-up applied to ${daFile.getName}: $newCount new, $reBoostCount re-boost, $holdingCount holding"
        )
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to apply executor scale-up to $clusterName: ${e.getMessage}")
    }
    allBoosts.toSeq
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
      b16Boosts: Seq[(String, Seq[MemoryHeapBoost])],
      correlationCount: Int,
      divergenceCount: Int,
      b14Holding: Seq[(String, String)] = Seq.empty,
      b14StateByCluster: Map[String, String] = Map.empty,
      executorScaleBoosts: Seq[(String, Seq[ExecutorScaleBoost])] = Seq.empty,
      trendScaledRecipes: Seq[(String, Seq[TrendScaleDecision])] = Seq.empty
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
    if (b14Boosts.isEmpty && b14Holding.isEmpty) {
      sb.append("  (none)\n")
    } else {
      if (b14Boosts.nonEmpty) {
        sb.append("  New / re-applied this run:\n")
        b14Boosts.foreach { case (cluster, reason) =>
          sb.append(s"    $cluster\n")
          sb.append(s"      Reason: $reason\n")
        }
      }
      if (b14Holding.nonEmpty) {
        sb.append("  Holding · no new eviction signal:\n")
        b14Holding.foreach { case (cluster, promotion) =>
          sb.append(s"    $cluster   $promotion  [holding]\n")
        }
      }
    }
    sb.append("\n")

    val b16TotalClusters = b16Boosts.size
    val b16TotalRecipes = b16Boosts.flatMap { case (_, boosts) => boosts.map(_.recipeFilename) }.distinct.size
    sb.append("-" * 72).append("\n")
    sb.append(s"  b16 OOM REBOOSTING  ($b16TotalRecipes recipe(s) across $b16TotalClusters cluster(s))\n")
    sb.append("-" * 72).append("\n")
    if (b16Boosts.isEmpty) {
      sb.append("  (none)\n")
    } else {
      b16Boosts.foreach { case (cluster, boosts) =>
        // Deduplicate by recipe (both JSON files yield the same boost per recipe)
        val uniqueBoosts = boosts.groupBy(_.recipeFilename).values.map(_.head).toSeq.sortBy(_.recipeFilename)
        sb.append(s"  $cluster  (${uniqueBoosts.size} recipe(s))\n")
        uniqueBoosts.foreach { b =>
          val recipe = b.recipeFilename.stripPrefix("_").stripSuffix(".json")
          val tag = b.state match {
            case BoostState.New => ""
            case BoostState.ReBoost => f"  [re-boost · cumulative x${b.effectiveCumulativeFactor}%.2f]"
            case BoostState.Holding => "  [holding]"
          }
          sb.append(
            ("    %-60s  spark.executor.memory: %s -> %s  (x%.1f)%s\n")
              .format(recipe, b.originalMemory, b.boostedMemory, b.boostFactor, tag)
          )
        }
      }
    }
    sb.append("\n")

    val esTotalClusters = executorScaleBoosts.size
    val esTotalRecipes = executorScaleBoosts.flatMap { case (_, boosts) => boosts.map(_.recipeFilename) }.distinct.size
    sb.append("-" * 72).append("\n")
    sb.append(s"  Z-SCORE EXECUTOR SCALE-UP  ($esTotalRecipes recipe(s) across $esTotalClusters cluster(s))\n")
    sb.append("-" * 72).append("\n")
    if (executorScaleBoosts.isEmpty) {
      sb.append("  (none)\n")
    } else {
      executorScaleBoosts.foreach { case (cluster, boosts) =>
        val unique = boosts.groupBy(_.recipeFilename).values.map(_.head).toSeq.sortBy(_.recipeFilename)
        sb.append(s"  $cluster  (${unique.size} recipe(s))\n")
        unique.foreach { b =>
          val recipe = b.recipeFilename.stripPrefix("_").stripSuffix(".json")
          val tag = b.state match {
            case BoostState.New => ""
            case BoostState.ReBoost => f"  [re-boost · cumulative x${b.effectiveCumulativeFactor}%.2f]"
            case BoostState.Holding => "  [holding]"
          }
          sb.append(
            ("    %-60s  spark.dynamicAllocation.maxExecutors: %d -> %d  (x%.1f)%s\n")
              .format(recipe, b.originalMaxExecutors, b.boostedMaxExecutors, b.boostFactor, tag)
          )
        }
      }
    }
    sb.append("\n")

    val trTotalClusters = trendScaledRecipes.size
    val trTotalRecipes = trendScaledRecipes.flatMap { case (_, ds) => ds.map(_.recipe) }.distinct.size
    sb.append("-" * 72).append("\n")
    sb.append(s"  TREND EXECUTOR SCALING  ($trTotalRecipes recipe(s) across $trTotalClusters cluster(s))\n")
    sb.append("-" * 72).append("\n")
    if (trendScaledRecipes.isEmpty) {
      sb.append("  (none)\n")
    } else {
      trendScaledRecipes.foreach { case (cluster, decisions) =>
        val unique = decisions.groupBy(_.recipe).values.map(_.head).toSeq.sortBy(_.recipe)
        sb.append(s"  $cluster  (${unique.size} recipe(s))\n")
        unique.foreach { d =>
          val recipe = d.recipe.stripPrefix("_").stripSuffix(".json")
          val tag = d.state match {
            case BoostState.New => ""
            case BoostState.ReBoost => f"  [re-boost · cumulative x${d.cumulativeFactor}%.2f]"
            case BoostState.Holding => "  [holding]"
          }
          if (d.isManual)
            sb.append(
              ("    %-52s  [%s] spark.executor.instances: %d -> %d%s\n")
                .format(recipe, d.direction.label, d.originalMax, d.newMax, tag)
            )
          else
            sb.append(
              ("    %-52s  [%s] min/max: %d/%d -> %d/%d%s\n")
                .format(recipe, d.direction.label, d.originalMin, d.originalMax, d.newMin, d.newMax, tag)
            )
        }
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

    // Also emit a structured JSON sibling for the dashboard frontend.
    writeAutoTunerSummaryJson(
      outputDir,
      refDate,
      curDate,
      strategyName,
      allTrends,
      kept,
      boosted,
      fresh,
      preserved,
      skipped,
      b14Boosts,
      b16Boosts,
      correlationCount,
      divergenceCount,
      b14Holding,
      b14StateByCluster,
      executorScaleBoosts,
      trendScaledRecipes
    )
  }

  /**
   * Build the `executor_trend` entry for the structured summary's `boost_groups` array. Self-contained (own quoting)
   * so it can be unit-tested in isolation. `source: "trend"` and the `count_up`/`count_down` fields distinguish this
   * longitudinal path from the divergence-driven `executor_scale` (z-score) group; both are recipe-level.
   *
   * The input is already filtered to changed decisions by the AutoTuner; entries are deduped by recipe per cluster.
   */
  private[auto] def trendBoostGroup(trendScaledRecipes: Seq[(String, Seq[TrendScaleDecision])]): String = {
    def esc(s: String): String = {
      val sb = new StringBuilder
      s.foreach {
        case '"' => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case c if c < 0x20 => sb.append("\\u%04x".format(c.toInt))
        case c => sb.append(c)
      }
      sb.toString
    }
    def q(s: String): String = "\"" + esc(s) + "\""

    val entries: String = trendScaledRecipes
      .map { case (cluster, decisions) =>
        val unique = decisions.groupBy(_.recipe).values.map(_.head).toSeq.sortBy(_.recipe)
        val recipes = unique
          .map { d =>
            val recipe = d.recipe.stripPrefix("_").stripSuffix(".json")
            val propagated = d.state == BoostState.Holding
            val maxObj =
              s"${q("spark_dynamic_allocation_max_executors")}:{${q("from")}:${d.originalMax},${q("to")}:${d.newMax}," +
                s"${q("factor")}:${"%.2f".format(d.appliedFactor)},${q("cumulative_factor")}:${"%.2f".format(d.cumulativeFactor)}}"
            val minObj =
              s"${q("spark_dynamic_allocation_min_executors")}:{${q("from")}:${d.originalMin},${q("to")}:${d.newMin}}"
            s"{${q("recipe")}:${q(recipe)}," +
              s"${q("recipe_filename")}:${q(d.recipe)}," +
              s"${q("state")}:${q(d.state.label)}," +
              s"${q("direction")}:${q(d.direction.label)}," +
              s"${q("manual")}:${d.isManual}," +
              s"${q("propagated")}:$propagated," +
              s"$minObj,$maxObj}"
          }
          .mkString(",")
        s"{${q("cluster")}:${q(cluster)},${q("recipes")}:[$recipes]}"
      }
      .mkString(",")

    val flat: Seq[TrendScaleDecision] = trendScaledRecipes.flatMap { case (_, decisions) =>
      decisions.groupBy(_.recipe).values.map(_.head).toSeq
    }
    val total = flat.size
    val newCount = flat.count(d => d.state == BoostState.New || d.state == BoostState.ReBoost)
    val holdingCount = flat.count(_.state == BoostState.Holding)
    val upCount = flat.count(_.direction == ScaleDirection.Up)
    val downCount = flat.count(_.direction == ScaleDirection.Down)
    val clusterCount = trendScaledRecipes.map(_._1).distinct.size

    s"{${q("code")}:${q("executor_trend")},${q("title")}:${q("Trend Executor Scaling")},${q("kind")}:${q("recipe")}," +
      s"${q("count")}:$total,${q("count_new")}:$newCount,${q("count_holding")}:$holdingCount," +
      s"${q("count_up")}:$upCount,${q("count_down")}:$downCount,${q("cluster_count")}:$clusterCount," +
      s"${q("source")}:${q("trend")},${q("entries")}:[$entries]}"
  }

  /**
   * Emit the same summary as a structured JSON so the dashboard can render boost groups generically (b14, b16, future
   * bxx) without parsing text.
   */
  private[auto] def writeAutoTunerSummaryJson(
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
      b16Boosts: Seq[(String, Seq[MemoryHeapBoost])],
      correlationCount: Int,
      divergenceCount: Int,
      b14Holding: Seq[(String, String)] = Seq.empty,
      b14StateByCluster: Map[String, String] = Map.empty,
      executorScaleBoosts: Seq[(String, Seq[ExecutorScaleBoost])] = Seq.empty,
      trendScaledRecipes: Seq[(String, Seq[TrendScaleDecision])] = Seq.empty
  ): Unit = {
    val trendCounts = allTrends.groupBy(_.trend.label).mapValues(_.size)
    val totalRecipes = allTrends.size
    val totalClusters = allTrends.map(_.cluster).distinct.size

    def esc(s: String): String = {
      val sb = new StringBuilder
      s.foreach {
        case '"' => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case c if c < 0x20 => sb.append("\\u%04x".format(c.toInt))
        case c => sb.append(c)
      }
      sb.toString
    }

    def q(s: String): String = "\"" + esc(s) + "\""

    // Parse a b14 reason like "... Promoted X -> Y" and "... eviction (...): A (ref) -> B (cur)".
    val promotionRe = """Promoted\s+([\w\-]+)\s*->\s*([\w\-]+)""".r
    val evictRe = """:\s*(\d+)\s*\(ref\)\s*->\s*(\d+)\s*\(cur\)""".r
    val curOnlyRe = """eviction\s*\((\d+)\)""".r

    val b14NewEntries: Seq[String] = b14Boosts.map { case (cluster, reason) =>
      val (fromM, toM) = promotionRe.findFirstMatchIn(reason).map(m => (m.group(1), m.group(2))).getOrElse(("", ""))
      val persistence = if (reason.toLowerCase.contains("persistent")) "persistent" else "current"
      // Prefer the explicit state from AutoTuner (derived from the ref config's
      // applied_driver_promotion tag — full b16 parity). Fall back to the persistence-
      // text heuristic when no map is supplied (used by the legacy test signature).
      val state = b14StateByCluster.getOrElse(cluster, if (persistence == "persistent") "re-boost" else "new")
      val (refE, curE) =
        evictRe.findFirstMatchIn(reason).map(m => (Some(m.group(1).toInt), Some(m.group(2).toInt))).getOrElse {
          if (persistence == "current") (None, curOnlyRe.findFirstMatchIn(reason).map(_.group(1).toInt))
          else (None, None)
        }
      val parts = scala.collection.mutable.ArrayBuffer.empty[String]
      parts += s"${q("cluster")}:${q(cluster)}"
      parts += s"${q("state")}:${q(state)}"
      parts += s"${q("reason")}:${q(reason)}"
      parts += s"${q("persistence")}:${q(persistence)}"
      if (fromM.nonEmpty) parts += s"${q("promotion")}:{${q("from")}:${q(fromM)},${q("to")}:${q(toM)}}"
      val evicts = scala.collection.mutable.ArrayBuffer.empty[String]
      refE.foreach(v => evicts += s"${q("ref")}:$v")
      curE.foreach(v => evicts += s"${q("cur")}:$v")
      if (evicts.nonEmpty) parts += s"${q("evictions")}:{${evicts.mkString(",")}}"
      "{" + parts.mkString(",") + "}"
    }

    val holdingPromotionRe = """([\w\-]+)\s*->\s*([\w\-]+)""".r
    val b14HoldingEntries: Seq[String] = b14Holding.map { case (cluster, promotion) =>
      val (fromM, toM) =
        holdingPromotionRe.findFirstMatchIn(promotion).map(m => (m.group(1), m.group(2))).getOrElse(("", ""))
      val parts = scala.collection.mutable.ArrayBuffer.empty[String]
      parts += s"${q("cluster")}:${q(cluster)}"
      parts += s"${q("state")}:${q("holding")}"
      parts += s"${q("reason")}:${q(s"Driver promotion holding: $promotion (no new b14 signal in current date)")}"
      parts += s"${q("persistence")}:${q("holding")}"
      if (fromM.nonEmpty) parts += s"${q("promotion")}:{${q("from")}:${q(fromM)},${q("to")}:${q(toM)}}"
      "{" + parts.mkString(",") + "}"
    }

    val b14Json: String = (b14NewEntries ++ b14HoldingEntries).mkString(",")

    val b16Json: String = b16Boosts
      .map { case (cluster, boosts) =>
        val unique = boosts.groupBy(_.recipeFilename).values.map(_.head).toSeq.sortBy(_.recipeFilename)
        val recipes = unique
          .map { b =>
            val recipe = b.recipeFilename.stripPrefix("_").stripSuffix(".json")
            // `propagated` kept for back-compat with older frontends; new frontends consume `state`.
            val propagated = b.state == BoostState.Holding
            s"{${q("recipe")}:${q(recipe)}," +
              s"${q("recipe_filename")}:${q(b.recipeFilename)}," +
              s"${q("state")}:${q(b.state.label)}," +
              s"${q("propagated")}:$propagated," +
              s"${q("spark_executor_memory")}:{${q("from")}:${q(b.originalMemory)},${q("to")}:${q(b.boostedMemory)},${q("factor")}:${"%.2f".format(b.boostFactor)},${q("cumulative_factor")}:${"%.2f".format(b.effectiveCumulativeFactor)}}}"
          }
          .mkString(",")
        s"{${q("cluster")}:${q(cluster)},${q("recipes")}:[$recipes]}"
      }
      .mkString(",")

    // Counts split by state so the dashboard can render "N new · M holding" without
    // re-walking the entries array. Distinct recipe filenames per cluster.
    val b16FlatBoosts: Seq[MemoryHeapBoost] = b16Boosts.flatMap { case (_, boosts) =>
      boosts.groupBy(_.recipeFilename).values.map(_.head).toSeq
    }
    val b16NewCount = b16FlatBoosts.count(b => b.state == BoostState.New || b.state == BoostState.ReBoost)
    val b16HoldingCount = b16FlatBoosts.count(_.state == BoostState.Holding)
    val b16TotalRecipesJson = b16FlatBoosts.size

    val sb = new StringBuilder
    sb.append("{\n")
    sb.append(
      s"  ${q("metadata")}:{${q("reference_date")}:${q(refDate)},${q("current_date")}:${q(curDate)},${q("strategy")}:${q(strategyName)},${q("generated_at")}:${q(java.time.Instant.now().toString)}},\n"
    )
    sb.append(s"  ${q("trend_summary")}:{${q("total_clusters")}:$totalClusters,${q("total_recipes")}:$totalRecipes,")
    sb.append(
      s"${q("improved")}:${trendCounts.getOrElse("improved", 0)},${q("degraded")}:${trendCounts.getOrElse("degraded", 0)},${q("stable")}:${trendCounts.getOrElse("stable", 0)},"
    )
    sb.append(
      s"${q("new_entries")}:${trendCounts.getOrElse("new_entry", 0)},${q("dropped_entries")}:${trendCounts.getOrElse("dropped_entry", 0)}},\n"
    )
    sb.append(
      s"  ${q("evolution_actions")}:{${q("kept")}:$kept,${q("boosted")}:$boosted,${q("fresh")}:$fresh,${q("preserved")}:$preserved,${q("skipped")}:$skipped},\n"
    )
    val esJson: String = executorScaleBoosts
      .map { case (cluster, boosts) =>
        val unique = boosts.groupBy(_.recipeFilename).values.map(_.head).toSeq.sortBy(_.recipeFilename)
        val recipes = unique
          .map { b =>
            val recipe = b.recipeFilename.stripPrefix("_").stripSuffix(".json")
            val propagated = b.state == BoostState.Holding
            s"{${q("recipe")}:${q(recipe)}," +
              s"${q("recipe_filename")}:${q(b.recipeFilename)}," +
              s"${q("state")}:${q(b.state.label)}," +
              s"${q("propagated")}:$propagated," +
              s"${q("spark_dynamic_allocation_max_executors")}:{${q("from")}:${b.originalMaxExecutors},${q("to")}:${b.boostedMaxExecutors},${q("factor")}:${"%.2f".format(b.boostFactor)},${q("cumulative_factor")}:${"%.2f".format(b.effectiveCumulativeFactor)}}}"
          }
          .mkString(",")
        s"{${q("cluster")}:${q(cluster)},${q("recipes")}:[$recipes]}"
      }
      .mkString(",")

    val esFlatBoosts: Seq[ExecutorScaleBoost] = executorScaleBoosts.flatMap { case (_, boosts) =>
      boosts.groupBy(_.recipeFilename).values.map(_.head).toSeq
    }
    val esNewCount = esFlatBoosts.count(b => b.state == BoostState.New || b.state == BoostState.ReBoost)
    val esHoldingCount = esFlatBoosts.count(_.state == BoostState.Holding)
    val esTotalRecipesJson = esFlatBoosts.size

    sb.append(s"  ${q("boost_groups")}:[\n")
    sb.append(
      s"    {${q("code")}:${q("b14")},${q("title")}:${q("Driver Boosts")},${q("kind")}:${q("cluster")},${q("count")}:${b14Boosts.size + b14Holding.size},${q("count_new")}:${b14Boosts.size},${q("count_holding")}:${b14Holding.size},${q("entries")}:[$b14Json]},\n"
    )
    sb.append(
      s"    {${q("code")}:${q("b16")},${q("title")}:${q("OOM Reboosting")},${q("kind")}:${q("recipe")},${q("count")}:$b16TotalRecipesJson,${q("count_new")}:$b16NewCount,${q("count_holding")}:$b16HoldingCount,${q("cluster_count")}:${b16Boosts.size},${q("entries")}:[$b16Json]},\n"
    )
    sb.append(
      s"    {${q("code")}:${q("executor_scale")},${q("title")}:${q("Z-score Executor SCALE-UP")},${q("kind")}:${q("recipe")},${q("count")}:$esTotalRecipesJson,${q("count_new")}:$esNewCount,${q("count_holding")}:$esHoldingCount,${q("cluster_count")}:${executorScaleBoosts.size},${q("source")}:${q("derived")},${q("entries")}:[$esJson]},\n"
    )
    sb.append("    ").append(trendBoostGroup(trendScaledRecipes)).append("\n")
    sb.append("  ],\n")
    sb.append(
      s"  ${q("statistical_analysis")}:{${q("correlation_pairs")}:$correlationCount,${q("divergences")}:$divergenceCount}\n"
    )
    sb.append("}\n")

    ClusterMachineAndRecipeTuner.writeFile(outputDir, "_generation_summary_auto_tuner.json", sb.toString())
    logger.info(s"Auto-tuner summary JSON written to ${outputDir.getPath}/_generation_summary_auto_tuner.json")
  }

  private def logTrendSummary(trends: Seq[TrendAssessment]): Unit = {
    val counts = trends.groupBy(_.trend.label).mapValues(_.size)
    logger.info(
      s"Trend summary: improved=${counts.getOrElse("improved", 0)} " +
        s"degraded=${counts.getOrElse("degraded", 0)} " +
        s"stable=${counts.getOrElse("stable", 0)} " +
        s"new=${counts.getOrElse("new_entry", 0)} " +
        s"dropped=${counts.getOrElse("dropped_entry", 0)}"
    )
  }
}
