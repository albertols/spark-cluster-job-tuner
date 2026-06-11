package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.{
  BiasMode,
  CostBiased,
  CostPerformanceBalance,
  PerformanceBiased
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
