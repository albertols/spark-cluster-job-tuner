package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.{
  BiasMode,
  CostBiased,
  CostPerformanceBalance,
  PerformanceBiased,
  RecipeMetrics
}

// ── Scaling gains ────────────────────────────────────────────────────────────

/**
 * Tunable gains for [[ExecutorTrendScaler]]. Derived from the active [[BiasMode]] with optional CLI overrides.
 *
 *   - `gain` : how strongly the allocation tracks the duration trend on scale-UP.
 *   - `maxStep` / `minStep` : per-run multiplicative clamps on UP (>= 1) and DOWN (<= 1) so a single run cannot explode
 *     or collapse an allocation.
 *   - `deadbandUp` / `deadbandDown` : fractional duration change required to trigger UP / DOWN. The band between them
 *     is the hysteresis no-op zone that prevents flapping.
 *   - `capTouchRatio` : UP only fires when cap-pressure (p95RunMax/max or fraction_reaching_cap) >= this.
 *   - `downGain` / `downSafetyMargin` / `downConfidenceFloor` : DOWN aggressiveness, demand headroom kept when
 *     shrinking, and the minimum confidence required to shrink at all.
 *   - `minRunsForConfidence` : runs (each side) at which a signal has full evidence; below it the graduated-evidence
 *     rules in `admittedStepCap` apply.
 *   - `minDeltaMinutes` : absolute blended-duration change in minutes that gates BOTH the UP severity classification (a
 *     huge ratio on a seconds-long job is noise, not a signal) and, symmetrically, the DOWN trigger (a saving smaller
 *     than this is not worth shrinking for).
 *   - `upPoolRatio` : fraction of cluster cores forming the per-run scale-UP budget used to prioritize grants across a
 *     cluster's recipes.
 */
final case class ScaleGains(
    gain: Double,
    maxStep: Double,
    minStep: Double,
    deadbandUp: Double,
    deadbandDown: Double,
    downGain: Double,
    downSafetyMargin: Double,
    downConfidenceFloor: Double,
    capTouchRatio: Double,
    minRunsForConfidence: Long,
    downscaleEnabled: Boolean,
    minDeltaMinutes: Double,
    upPoolRatio: Double
)

object ScaleGains {

  // Shared defaults (CLI-overridable). Bias presets only vary the up/down aggressiveness.
  private val DefaultDeadbandUp = 0.10
  private val DefaultMinStep = 0.5
  private val DefaultDownSafetyMargin = 0.15
  private val DefaultDownConfidenceFloor = 0.5
  private val DefaultCapTouchRatio = 0.5
  private val DefaultMinRuns = 5L
  private val DefaultMinDeltaMinutes = 3.0
  private val DefaultUpPoolRatio = 1.0

  /**
   * Per-bias (gain, maxStep, deadbandDown, downGain). `BiasMode` is a sealed trait, so this match is exhaustive over
   * its three cases by construction — no wildcard fallback, so adding a fourth bias becomes a compile-time warning here
   * rather than silently inheriting the balanced preset.
   */
  private def biasTuple(bias: BiasMode): (Double, Double, Double, Double) = bias match {
    case CostBiased => (0.35, 1.5, 0.05, 0.6)
    case PerformanceBiased => (0.70, 2.5, 0.20, 0.25)
    case CostPerformanceBalance => (0.50, 2.0, 0.10, 0.4)
  }

  def fromBias(
      bias: BiasMode,
      gainOverride: Option[Double] = None,
      maxStepOverride: Option[Double] = None,
      deadbandUpOverride: Option[Double] = None,
      minRunsOverride: Option[Long] = None,
      downscaleEnabledOverride: Option[Boolean] = None,
      minDeltaMinutesOverride: Option[Double] = None,
      upPoolRatioOverride: Option[Double] = None
  ): ScaleGains = {
    val (gain, maxStep, deadbandDown, downGain) = biasTuple(bias)
    ScaleGains(
      gain = gainOverride.getOrElse(gain),
      maxStep = maxStepOverride.getOrElse(maxStep),
      minStep = DefaultMinStep,
      deadbandUp = deadbandUpOverride.getOrElse(DefaultDeadbandUp),
      deadbandDown = deadbandDown,
      downGain = downGain,
      downSafetyMargin = DefaultDownSafetyMargin,
      downConfidenceFloor = DefaultDownConfidenceFloor,
      capTouchRatio = DefaultCapTouchRatio,
      minRunsForConfidence = minRunsOverride.getOrElse(DefaultMinRuns),
      downscaleEnabled = downscaleEnabledOverride.getOrElse(true),
      minDeltaMinutes = minDeltaMinutesOverride.getOrElse(DefaultMinDeltaMinutes),
      upPoolRatio = upPoolRatioOverride.getOrElse(DefaultUpPoolRatio)
    )
  }
}

// ── Decision model ───────────────────────────────────────────────────────────

sealed trait ScaleDirection { def label: String }
object ScaleDirection {
  case object Up extends ScaleDirection { val label = "up" }
  case object Down extends ScaleDirection { val label = "down" }
  case object Hold extends ScaleDirection { val label = "hold" }
}

sealed trait ScaleSeverity { def label: String; def rank: Int }
object ScaleSeverity {
  case object Negligible extends ScaleSeverity { val label = "negligible"; val rank = 0 }
  case object Moderate extends ScaleSeverity { val label = "moderate"; val rank = 1 }
  case object Severe extends ScaleSeverity { val label = "severe"; val rank = 2 }
  case object Critical extends ScaleSeverity { val label = "critical"; val rank = 3 }
}

/**
 * The outcome of a single trend-scaling decision for one (cluster, recipe).
 *
 * For manual recipes `newMin == newInitial == newMax` and all three carry the new `spark.executor.instances`.
 * `cumulativeFactor` is the value stamped as `appliedTrendScaleFactor` (compounds UP, reduces DOWN, carries on HOLD).
 * `severity` is the UP-side severity tier label; improvements and no-signal cases classify as "negligible" (the tier
 * describes degradation only). "n/a" appears only on hand-constructed instances. `impactMinutes` = max(0, blended delta
 * minutes) x current runs — total wall-clock minutes lost per window. `priorityRank` is set later by cluster-wide
 * prioritization (Task 3); `decide` always leaves it None.
 */
final case class TrendScaleDecision(
    recipe: String,
    isManual: Boolean,
    originalMin: Int,
    originalInitial: Int,
    originalMax: Int,
    newMin: Int,
    newInitial: Int,
    newMax: Int,
    appliedFactor: Double,
    cumulativeFactor: Double,
    direction: ScaleDirection,
    state: BoostState,
    reason: String,
    severity: String = "n/a",
    impactMinutes: Double = 0.0,
    priorityRank: Option[Int] = None
) {
  def changed: Boolean = newMin != originalMin || newInitial != originalInitial || newMax != originalMax
}

// ── Pure scaler ──────────────────────────────────────────────────────────────

object ExecutorTrendScaler {

  val DurationP95Weight: Double = 0.7
  val DurationAvgWeight: Double = 0.3

  /** Runs (each side) at which confidence reaches 1.0 — mirrors TrendDetector.computeConfidence's /10 ramp. */
  private val ConfidenceFullyRampedRuns: Double = 10.0

  // Severity thresholds: a tier needs BOTH the relative ratio and the absolute minutes lost.
  val SevereRatio: Double = 2.0
  val CriticalRatio: Double = 3.0
  val SevereDeltaMinutes: Double = 10.0
  val CriticalDeltaMinutes: Double = 30.0
  val SevereStepMultiplier: Double = 1.5
  val CriticalStepMultiplier: Double = 2.0
  val SingleRunStepCap: Double = 1.5
  val MinCreepPressure: Double = 0.8
  val ConfidenceFloor: Double = 0.5

  private[refinement] def classifySeverity(
      durRatio: Double,
      deltaMin: Double,
      deadbandUp: Double,
      minDeltaMinutes: Double
  ): ScaleSeverity =
    if (durRatio < 1.0 + deadbandUp || deltaMin < minDeltaMinutes) ScaleSeverity.Negligible
    else if (durRatio >= CriticalRatio && deltaMin >= CriticalDeltaMinutes) ScaleSeverity.Critical
    else if (durRatio >= SevereRatio && deltaMin >= SevereDeltaMinutes) ScaleSeverity.Severe
    else ScaleSeverity.Moderate

  /**
   * Graduated evidence: large effects need fewer observations. Returns the admitted per-run step cap, or None when the
   * signal is not admitted (insufficient evidence for its severity, or Negligible). runs >= minRunsForConfidence: full
   * tier cap; 2 to minRunsForConfidence-1: Severe+ only, cap demoted one tier; 1 run: Critical only at
   * SingleRunStepCap; otherwise none.
   */
  private[refinement] def admittedStepCap(tier: ScaleSeverity, runs: Long, gains: ScaleGains): Option[Double] = {
    def fullCap(t: ScaleSeverity): Double = t match {
      case ScaleSeverity.Critical => gains.maxStep * CriticalStepMultiplier
      case ScaleSeverity.Severe => gains.maxStep * SevereStepMultiplier
      case ScaleSeverity.Moderate | ScaleSeverity.Negligible => gains.maxStep
    }
    if (tier == ScaleSeverity.Negligible) None
    else if (runs >= gains.minRunsForConfidence) Some(fullCap(tier))
    else if (runs >= 2) tier match {
      case ScaleSeverity.Critical => Some(fullCap(ScaleSeverity.Severe))
      case ScaleSeverity.Severe => Some(fullCap(ScaleSeverity.Moderate))
      case ScaleSeverity.Moderate | ScaleSeverity.Negligible => None
    }
    else if (runs == 1 && tier == ScaleSeverity.Critical) Some(SingleRunStepCap)
    else None
  }

  private def clampD(v: Double, lo: Double, hi: Double): Double = math.max(lo, math.min(hi, v))
  private def clampI(v: Int, lo: Int, hi: Int): Int = math.max(lo, math.min(hi, v))

  /** max(p95RunMax / currentMax, fraction_reaching_cap). 1.0 means fully pinned at the ceiling. */
  private[refinement] def capPressure(cur: RecipeMetrics, currentMax: Int): Double = {
    val fromExec = if (currentMax > 0) cur.p95RunMaxExecutors / currentMax.toDouble else 0.0
    math.max(fromExec, cur.fractionReachingCap.getOrElse(0.0))
  }

  /**
   * Decide how to evolve one recipe's executor allocation from the reference→current duration trend (v2,
   * magnitude-aware).
   *
   * UP is gated by severity tiers: [[classifySeverity]] requires BOTH the relative blended-duration ratio and the
   * absolute minutes lost (`minDeltaMinutes`), and [[admittedStepCap]] applies graduated evidence — large effects
   * (Severe/Critical) are admitted on fewer runs, but with a demoted (or `SingleRunStepCap`) per-run step cap. An
   * admitted UP also still requires cap-pressure >= `capTouchRatio`. `min` creeps at most +1 per run, and only when the
   * tier is Severe+ AND pressure >= `MinCreepPressure`; `initial` follows within [min, initial+1].
   *
   * DOWN requires the blended saving to be at least `minDeltaMinutes` (symmetric absolute floor) on top of the
   * fractional deadband, plus low cap-pressure and ramped confidence. `max` shrinks toward observed demand with a
   * safety margin (never below peak demand or floor 2); `min` shrinks at most 1 per run, never below steady demand.
   *
   * HOLD keeps the config unchanged (appliedFactor 1.0) and carries any prior cumulative factor (Holding lifecycle). UP
   * never shrinks the ceiling below `currentMax`, even under a tight `capacity` clamp.
   */
  def decide(
      recipe: String,
      isManual: Boolean,
      currentMin: Int,
      currentInitial: Int,
      currentMax: Int,
      ref: RecipeMetrics,
      cur: RecipeMetrics,
      gains: ScaleGains,
      capacity: Option[Int],
      priorCumulativeFactor: Option[Double]
  ): TrendScaleDecision = {

    val blendRef = DurationP95Weight * ref.p95JobDurationMs + DurationAvgWeight * ref.avgJobDurationMs
    val blendCur = DurationP95Weight * cur.p95JobDurationMs + DurationAvgWeight * cur.avgJobDurationMs
    val hasDur = blendRef > 0.0 && blendCur > 0.0
    val durRatio = if (hasDur) blendCur / blendRef else 1.0
    val deltaMin = if (hasDur) (blendCur - blendRef) / 60000.0 else 0.0
    val impactMin = math.max(0.0, deltaMin) * cur.runs
    val evidenceRuns = math.min(ref.runs, cur.runs)
    val tier =
      if (hasDur) classifySeverity(durRatio, deltaMin, gains.deadbandUp, gains.minDeltaMinutes)
      else ScaleSeverity.Negligible
    val stepCapOpt = admittedStepCap(tier, evidenceRuns, gains)
    val pressure = capPressure(cur, currentMax)
    val rampConf = math.min(1.0, evidenceRuns.toDouble / ConfidenceFullyRampedRuns)
    val prior = priorCumulativeFactor.getOrElse(1.0)
    val hasPrior = priorCumulativeFactor.isDefined

    val upTriggered = stepCapOpt.isDefined && pressure >= gains.capTouchRatio
    val downTriggered =
      gains.downscaleEnabled && hasDur &&
        durRatio <= 1.0 - gains.deadbandDown &&
        deltaMin <= -gains.minDeltaMinutes &&
        pressure < gains.capTouchRatio &&
        rampConf >= gains.downConfidenceFloor

    def diag: String =
      f"sev=${tier.label} durRatio=$durRatio%.2f dMin=$deltaMin%.1f impact=$impactMin%.0f " +
        f"pressure=$pressure%.2f runs=$evidenceRuns"

    def hold(reason: String): TrendScaleDecision =
      TrendScaleDecision(
        recipe,
        isManual,
        currentMin,
        currentInitial,
        currentMax,
        currentMin,
        currentInitial,
        currentMax,
        1.0,
        prior,
        ScaleDirection.Hold,
        if (hasPrior) BoostState.Holding else BoostState.New,
        reason,
        tier.label,
        impactMin,
        None
      )

    if (upTriggered) {
      val stepCap = stepCapOpt.get
      val conf = math.max(ConfidenceFloor, rampConf)
      val maxFactor = clampD(1.0 + gains.gain * (durRatio - 1.0) * conf, 1.0, stepCap)
      val rawMax = math.max(currentMax + 1, math.ceil(currentMax * maxFactor).toInt)
      // A tight capacity may cap rawMax, but an UP signal must never SHRINK below the current ceiling.
      val newMax = capacity.map(c => math.max(currentMax, math.min(rawMax, c))).getOrElse(rawMax)

      if (isManual) {
        val newInst = math.max(2, newMax)
        TrendScaleDecision(
          recipe,
          isManual = true,
          currentMin,
          currentInitial,
          currentMax,
          newInst,
          newInst,
          newInst,
          maxFactor,
          prior * maxFactor,
          ScaleDirection.Up,
          if (hasPrior) BoostState.ReBoost else BoostState.New,
          s"manual instances $currentMax->$newInst ($diag)",
          tier.label,
          impactMin,
          None
        )
      } else {
        // Gentle creep: the always-on floor rises at most +1 per run, and only when the
        // degradation is Severe+ and the job is essentially pinned at its ceiling.
        val newMin =
          if (tier.rank >= ScaleSeverity.Severe.rank && pressure >= MinCreepPressure)
            math.max(currentMin, math.min(currentMin + 1, math.max(2, newMax - 1)))
          else currentMin
        val initialCeil = math.max(newMin, math.min(currentInitial + 1, newMax)) // initial follows by at most +1
        val newInitial = clampI(math.max(newMin, currentInitial), newMin, initialCeil)
        TrendScaleDecision(
          recipe,
          isManual = false,
          currentMin,
          currentInitial,
          currentMax,
          newMin,
          newInitial,
          newMax,
          maxFactor,
          prior * maxFactor,
          ScaleDirection.Up,
          if (hasPrior) BoostState.ReBoost else BoostState.New,
          s"min $currentMin->$newMin max $currentMax->$newMax ($diag)",
          tier.label,
          impactMin,
          None
        )
      }
    } else if (downTriggered) {
      val downFactor = clampD(1.0 - gains.downGain * (1.0 - durRatio) * rampConf, gains.minStep, 1.0)
      val peakDemand = math.ceil(cur.p95RunMaxExecutors * (1.0 + gains.downSafetyMargin)).toInt
      val steadyDemand = math.ceil(cur.avgExecutorsPerJob * (1.0 + gains.downSafetyMargin)).toInt
      val rawMax = math.ceil(currentMax * downFactor).toInt
      val newMax = math.max(2, math.max(peakDemand, rawMax))

      if (isManual) {
        val newInst = newMax
        val noChange = newInst == currentMax
        TrendScaleDecision(
          recipe,
          isManual = true,
          currentMin,
          currentInitial,
          currentMax,
          newInst,
          newInst,
          newInst,
          if (noChange) 1.0 else downFactor,
          if (noChange) prior else prior * downFactor,
          if (noChange) ScaleDirection.Hold else ScaleDirection.Down,
          if (noChange && hasPrior) BoostState.Holding else if (hasPrior) BoostState.ReBoost else BoostState.New,
          s"manual instances $currentMax->$newInst ($diag)",
          tier.label,
          impactMin,
          None
        )
      } else {
        // min shrinks at most 1 per run — but is RAISED to steady demand when observed demand exceeds it
        // (the demand floor dominates the creep), floor 2.
        val newMin = clampI(math.max(steadyDemand, currentMin - 1), 2, math.max(2, newMax - 1))
        val newInitial = clampI(math.max(newMin, math.min(currentInitial, newMax)), newMin, newMax)
        val noChange = newMax == currentMax && newMin == currentMin && newInitial == currentInitial
        TrendScaleDecision(
          recipe,
          isManual = false,
          currentMin,
          currentInitial,
          currentMax,
          newMin,
          newInitial,
          newMax,
          if (noChange) 1.0 else downFactor,
          if (noChange) prior else prior * downFactor,
          if (noChange) ScaleDirection.Hold else ScaleDirection.Down,
          if (noChange && hasPrior) BoostState.Holding else if (hasPrior) BoostState.ReBoost else BoostState.New,
          s"min $currentMin->$newMin max $currentMax->$newMax ($diag)",
          tier.label,
          impactMin,
          None
        )
      }
    } else {
      hold(s"within deadband or gate not met ($diag)")
    }
  }

  // ── Cluster-wide prioritization ──────────────────────────────────────────────

  /** Pairs a decision with its recipe's spark.executor.cores so the pool can be budgeted in CORES. */
  final case class RecipeCores(decision: TrendScaleDecision, execCores: Int)

  /**
   * Capacity-budgeted prioritization across one cluster's trend decisions. UP grants draw, in impact order (total
   * minutes lost per window), from a per-run pool of `poolRatio × clusterMaxTotalCores` cores; each grant consumes
   * (newMax − originalMax) × execCores. When the pool runs dry, remaining UP candidates are degraded to originalMax + 1
   * — reduced, never starved (every admitted signal still moves). Jobs are staggered in time, so this is intentionally
   * a growth-rate bound, not a concurrency bound; CapacityGuard stays the physical per-recipe clamp afterwards.
   * Holds/Downs pass through untouched. Input order is preserved.
   */
  def prioritize(inputs: Seq[RecipeCores], clusterMaxTotalCores: Int, poolRatio: Double): Seq[TrendScaleDecision] = {
    if (clusterMaxTotalCores <= 0) return inputs.map(_.decision)
    var pool = math.max(0, math.ceil(clusterMaxTotalCores * poolRatio).toInt)
    val isUpGrant: TrendScaleDecision => Boolean = d => d.direction == ScaleDirection.Up && d.newMax > d.originalMax
    val ranked = inputs
      .filter(i => isUpGrant(i.decision))
      .sortBy(i => (-i.decision.impactMinutes, -i.decision.appliedFactor, i.decision.recipe))
    val adjusted = scala.collection.mutable.Map.empty[String, TrendScaleDecision]
    ranked.zipWithIndex.foreach { case (RecipeCores(d, ec), idx) =>
      val cores = math.max(1, ec)
      val wantCores = (d.newMax - d.originalMax) * cores
      val granted =
        if (wantCores <= pool) {
          pool -= wantCores
          d.copy(priorityRank = Some(idx + 1))
        } else {
          pool = math.max(
            0,
            pool - cores
          ) // a degraded +1 grant still consumes its one executor's worth of cores (floored at 0)
          val degradedMax = d.originalMax + 1
          val newFactor = degradedMax.toDouble / math.max(1, d.originalMax)
          val priorCum = d.cumulativeFactor / d.appliedFactor
          val annotated = d.reason + s" [pool-exhausted: granted +1 of +${d.newMax - d.originalMax}]"
          if (d.isManual)
            d.copy(
              newMin = degradedMax,
              newInitial = degradedMax,
              newMax = degradedMax,
              appliedFactor = newFactor,
              cumulativeFactor = priorCum * newFactor,
              priorityRank = Some(idx + 1),
              reason = annotated
            )
          else {
            val dMin = math.min(d.newMin, math.max(2, degradedMax - 1))
            val dInit = math.max(dMin, math.min(d.newInitial, degradedMax))
            d.copy(
              newMin = dMin,
              newInitial = dInit,
              newMax = degradedMax,
              appliedFactor = newFactor,
              cumulativeFactor = priorCum * newFactor,
              priorityRank = Some(idx + 1),
              reason = annotated
            )
          }
        }
      adjusted(d.recipe) = granted
    }
    inputs.map(i => adjusted.getOrElse(i.decision.recipe, i.decision))
  }
}
