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
 *   - `gain` / `minGain` : how strongly `max` / `min` track the duration trend on scale-UP (minGain < gain so the
 *     always-on floor rises more conservatively than the ceiling).
 *   - `maxStep` / `minStep` : per-run multiplicative clamps on UP (>= 1) and DOWN (<= 1) so a single run cannot
 *     explode or collapse an allocation.
 *   - `deadbandUp` / `deadbandDown` : fractional duration change required to trigger UP / DOWN. The band between them
 *     is the hysteresis no-op zone that prevents flapping.
 *   - `capTouchRatio` : UP only fires when cap-pressure (p95RunMax/max or fraction_reaching_cap) >= this.
 *   - `downGain` / `downSafetyMargin` / `downConfidenceFloor` : DOWN aggressiveness, demand headroom kept when
 *     shrinking, and the minimum confidence required to shrink at all.
 *   - `minRunsForConfidence` : minimum runs on each side for a duration ratio to be considered usable.
 */
final case class ScaleGains(
    gain: Double,
    minGain: Double,
    maxStep: Double,
    minStep: Double,
    deadbandUp: Double,
    deadbandDown: Double,
    downGain: Double,
    downSafetyMargin: Double,
    downConfidenceFloor: Double,
    capTouchRatio: Double,
    minRunsForConfidence: Long,
    downscaleEnabled: Boolean
)

object ScaleGains {

  // Shared defaults (CLI-overridable). Bias presets only vary the up/down aggressiveness.
  private val DefaultDeadbandUp = 0.10
  private val DefaultMinStep = 0.5
  private val DefaultDownSafetyMargin = 0.15
  private val DefaultDownConfidenceFloor = 0.5
  private val DefaultCapTouchRatio = 0.5
  private val DefaultMinRuns = 5L

  /**
   * Per-bias (gain, minGain, maxStep, deadbandDown, downGain). `BiasMode` is a sealed trait, so this match is
   * exhaustive over its three cases by construction — no wildcard fallback, so adding a fourth bias becomes a
   * compile-time warning here rather than silently inheriting the balanced preset.
   */
  private def biasTuple(bias: BiasMode): (Double, Double, Double, Double, Double) = bias match {
    case CostBiased => (0.35, 0.15, 1.5, 0.05, 0.6)
    case PerformanceBiased => (0.70, 0.40, 2.5, 0.20, 0.25)
    case CostPerformanceBalance => (0.50, 0.25, 2.0, 0.10, 0.4)
  }

  def fromBias(
      bias: BiasMode,
      gainOverride: Option[Double] = None,
      minGainOverride: Option[Double] = None,
      maxStepOverride: Option[Double] = None,
      deadbandUpOverride: Option[Double] = None,
      minRunsOverride: Option[Long] = None,
      downscaleEnabledOverride: Option[Boolean] = None
  ): ScaleGains = {
    val (gain, minGain, maxStep, deadbandDown, downGain) = biasTuple(bias)
    ScaleGains(
      gain = gainOverride.getOrElse(gain),
      minGain = minGainOverride.getOrElse(minGain),
      maxStep = maxStepOverride.getOrElse(maxStep),
      minStep = DefaultMinStep,
      deadbandUp = deadbandUpOverride.getOrElse(DefaultDeadbandUp),
      deadbandDown = deadbandDown,
      downGain = downGain,
      downSafetyMargin = DefaultDownSafetyMargin,
      downConfidenceFloor = DefaultDownConfidenceFloor,
      capTouchRatio = DefaultCapTouchRatio,
      minRunsForConfidence = minRunsOverride.getOrElse(DefaultMinRuns),
      downscaleEnabled = downscaleEnabledOverride.getOrElse(true)
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

  /** min(refRuns, curRuns)/10 capped at 1.0 — mirrors TrendDetector.computeConfidence. */
  private[refinement] def confidence(ref: RecipeMetrics, cur: RecipeMetrics): Double =
    math.min(1.0, math.min(ref.runs, cur.runs).toDouble / 10.0)

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
      val newMax = capacity.map(c => math.min(rawMax, c)).getOrElse(rawMax)

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
        val minFactor = 1.0 + gains.minGain * (durRatio - 1.0) * conf
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

      if (isManual) {
        val newInst = math.max(2, math.max(peakDemand, rawMax))
        TrendScaleDecision(
          recipe,
          isManual = true,
          currentMin,
          currentInitial,
          currentMax,
          newInst,
          newInst,
          newInst,
          downFactor,
          prior * downFactor,
          if (newInst == currentMax) ScaleDirection.Hold else ScaleDirection.Down,
          if (hasPrior) BoostState.ReBoost else BoostState.New,
          f"manual instances $currentMax->$newInst (durRatio=$durRatio%.2f, pressure=$pressure%.2f, conf=$conf%.2f)"
        )
      } else {
        val direction = if (newMax == currentMax && newMin == currentMin) ScaleDirection.Hold else ScaleDirection.Down
        TrendScaleDecision(
          recipe,
          isManual = false,
          currentMin,
          currentInitial,
          currentMax,
          newMin,
          newInitial,
          newMax,
          downFactor,
          prior * downFactor,
          direction,
          if (hasPrior) BoostState.ReBoost else BoostState.New,
          f"min $currentMin->$newMin max $currentMax->$newMax (durRatio=$durRatio%.2f, pressure=$pressure%.2f, conf=$conf%.2f)"
        )
      }
    } else {
      hold(f"within deadband or gate not met (durRatio=$durRatio%.2f, pressure=$pressure%.2f, conf=$conf%.2f)")
    }
  }
}
