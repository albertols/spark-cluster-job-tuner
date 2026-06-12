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
 *   - `maxStep` / `minStep` : per-run multiplicative clamps on UP (>= 1) and DOWN (<= 1) so a single run cannot
 *     explode or collapse an allocation.
 *   - `deadbandUp` / `deadbandDown` : fractional duration change required to trigger UP / DOWN. The band between them
 *     is the hysteresis no-op zone that prevents flapping.
 *   - `capTouchRatio` : UP only fires when cap-pressure (p95RunMax/max or fraction_reaching_cap) >= this.
 *   - `downGain` / `downSafetyMargin` / `downConfidenceFloor` : DOWN aggressiveness, demand headroom kept when
 *     shrinking, and the minimum confidence required to shrink at all.
 *   - `minRunsForConfidence` : minimum runs on each side for a duration ratio to be considered usable.
 *   - `minDeltaMinutes` : absolute blended-duration change in minutes below which a trend is Negligible in both
 *     directions (a huge ratio on a seconds-long job is noise, not a signal).
 *   - `upPoolRatio` : fraction of cluster cores forming the per-run scale-UP pool, consumed by Task 3's prioritize.
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
   * its three cases by construction — no wildcard fallback, so adding a fourth bias becomes a compile-time warning
   * here rather than silently inheriting the balanced preset.
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
    reason: String
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
   * Graduated evidence: large effects need fewer observations. Returns the admitted per-run step cap,
   * or None when the signal is not admitted (insufficient evidence for its severity, or Negligible).
   */
  private[refinement] def admittedStepCap(tier: ScaleSeverity, runs: Long, gains: ScaleGains): Option[Double] = {
    def fullCap(t: ScaleSeverity): Double = t match {
      case ScaleSeverity.Critical => gains.maxStep * CriticalStepMultiplier
      case ScaleSeverity.Severe => gains.maxStep * SevereStepMultiplier
      case _ => gains.maxStep
    }
    if (tier == ScaleSeverity.Negligible) None
    else if (runs >= gains.minRunsForConfidence) Some(fullCap(tier))
    else if (runs >= 2) tier match {
      case ScaleSeverity.Critical => Some(fullCap(ScaleSeverity.Severe))
      case ScaleSeverity.Severe => Some(fullCap(ScaleSeverity.Moderate))
      case _ => None
    }
    else if (runs == 1 && tier == ScaleSeverity.Critical) Some(SingleRunStepCap)
    else None
  }

  private def clampD(v: Double, lo: Double, hi: Double): Double = math.max(lo, math.min(hi, v))
  private def clampI(v: Int, lo: Int, hi: Int): Int = math.max(lo, math.min(hi, v))

  /** cur/ref, or 1.0 (no usable signal) when either side is non-positive or under the min-runs confidence floor. */
  private[refinement] def guardedRatio(cur: Double, ref: Double, refRuns: Long, curRuns: Long, minRuns: Long): Double =
    if (ref <= 0.0 || cur <= 0.0) 1.0
    else if (math.min(refRuns, curRuns) < minRuns) 1.0
    else cur / ref

  /** Blended duration driver: 0.7 * p95Ratio + 0.3 * avgRatio (each guarded). */
  private[refinement] def blendedDurationRatio(ref: RecipeMetrics, cur: RecipeMetrics, minRuns: Long): Double = {
    val p95 = guardedRatio(cur.p95JobDurationMs, ref.p95JobDurationMs, ref.runs, cur.runs, minRuns)
    val avg = guardedRatio(cur.avgJobDurationMs, ref.avgJobDurationMs, ref.runs, cur.runs, minRuns)
    DurationP95Weight * p95 + DurationAvgWeight * avg
  }

  /** max(p95RunMax / currentMax, fraction_reaching_cap). 1.0 means fully pinned at the ceiling. */
  private[refinement] def capPressure(cur: RecipeMetrics, currentMax: Int): Double = {
    val fromExec = if (currentMax > 0) cur.p95RunMaxExecutors / currentMax.toDouble else 0.0
    math.max(fromExec, cur.fractionReachingCap.getOrElse(0.0))
  }

  /** min(refRuns, curRuns)/ConfidenceFullyRampedRuns capped at 1.0 — mirrors TrendDetector.computeConfidence. */
  private[refinement] def confidence(ref: RecipeMetrics, cur: RecipeMetrics): Double =
    math.min(1.0, math.min(ref.runs, cur.runs).toDouble / ConfidenceFullyRampedRuns)

  /**
   * Decide how to evolve one recipe's executor allocation from the reference→current duration trend.
   *
   * Returns a HOLD (config unchanged, appliedFactor 1.0) when the blended duration ratio sits inside the deadband, or
   * when an UP signal is not cap-pressured / a DOWN signal still has cap-pressure. UP scales `max` toward the blended
   * duration trend, never shrinking below `currentMax` even when
   * `capacity` is tight. DOWN shrinks toward observed demand with a safety margin, never below the observed peak or a
   * floor of 2, and stamps the carried factor unchanged when the demand floor blocks any move.
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

    val durRatio = blendedDurationRatio(ref, cur, gains.minRunsForConfidence)
    val pressure = capPressure(cur, currentMax)
    val conf = confidence(ref, cur)
    val prior = priorCumulativeFactor.getOrElse(1.0)
    val hasPrior = priorCumulativeFactor.isDefined

    val upTriggered = durRatio >= 1.0 + gains.deadbandUp && pressure >= gains.capTouchRatio
    val downTriggered =
      gains.downscaleEnabled &&
        durRatio <= 1.0 - gains.deadbandDown &&
        pressure < gains.capTouchRatio &&
        conf >= gains.downConfidenceFloor

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
        reason
      )

    if (upTriggered) {
      val maxFactor = clampD(1.0 + gains.gain * (durRatio - 1.0) * conf, 1.0, gains.maxStep)
      val rawMax = math.max(currentMax + 1, math.ceil(currentMax * maxFactor).toInt)
      // A tight capacity may cap rawMax, but an UP signal must never SHRINK below the current ceiling —
      // floor at currentMax so a constrained cluster holds (and we still raise min within the ceiling).
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
          f"manual instances $currentMax->$newInst (durRatio=$durRatio%.2f, pressure=$pressure%.2f, conf=$conf%.2f)"
        )
      } else {
        val minFactor = 1.0 // v2 (Task 2) reworks min-creep; minGain removed from ScaleGains
        val rawMin = math.ceil(currentMin * minFactor).toInt
        val newMin = clampI(rawMin, 2, math.max(2, newMax - 1))
        val newInitial = clampI(math.max(newMin, currentInitial), newMin, newMax)
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
          f"min $currentMin->$newMin max $currentMax->$newMax (durRatio=$durRatio%.2f, pressure=$pressure%.2f, conf=$conf%.2f)"
        )
      }
    } else if (downTriggered) {
      val downFactor = clampD(1.0 - gains.downGain * (1.0 - durRatio) * conf, gains.minStep, 1.0)
      val peakDemand = math.ceil(cur.p95RunMaxExecutors * (1.0 + gains.downSafetyMargin)).toInt
      val steadyDemand = math.ceil(cur.avgExecutorsPerJob * (1.0 + gains.downSafetyMargin)).toInt
      val rawMax = math.ceil(currentMax * downFactor).toInt
      val newMax = math.max(2, math.max(peakDemand, rawMax))
      val rawMin = math.ceil(currentMin * downFactor).toInt
      val newMin = clampI(math.max(steadyDemand, rawMin), 2, math.max(2, newMax - 1))
      val newInitial = clampI(math.max(newMin, math.min(currentInitial, newMax)), newMin, newMax)

      // `state` tracks the touch-lifecycle (New = first trend touch, ReBoost = subsequent, Holding = no-op with a
      // prior tag); `direction` carries up/down. They are independent: a shrink of an already-tagged recipe is a
      // ReBoost in lifecycle terms but a Down in direction terms. The demand floors above can block any shrink — when
      // nothing changed we must stamp the carried factor unchanged (appliedFactor 1.0, cumulative = prior), otherwise
      // the cumulative factor would drift downward on every re-run that produced no actual change.
      if (isManual) {
        val newInst = math.max(2, math.max(peakDemand, rawMax))
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
          f"manual instances $currentMax->$newInst (durRatio=$durRatio%.2f, pressure=$pressure%.2f, conf=$conf%.2f)"
        )
      } else {
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
          f"min $currentMin->$newMin max $currentMax->$newMax (durRatio=$durRatio%.2f, pressure=$pressure%.2f, conf=$conf%.2f)"
        )
      }
    } else {
      hold(f"within deadband or gate not met (durRatio=$durRatio%.2f, pressure=$pressure%.2f, conf=$conf%.2f)")
    }
  }
}
