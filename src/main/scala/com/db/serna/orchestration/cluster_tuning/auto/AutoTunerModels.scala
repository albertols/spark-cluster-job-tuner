package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.{AutoscalerEvent, ClusterSpan, DiagnosticSignal, DriverResourceOverride, RecipeMetrics}

/**
 * Domain models for multi-date auto-tuning analysis.
 *
 * These models capture the temporal dimension that the one-off tuner lacks:
 * metrics across dates, performance trend classification, evolution decisions,
 * and statistical analysis results.
 */

/** Snapshot of all loaded data for a single date. */
final case class DateSnapshot(
  date: String,
  metrics: Map[(String, String), RecipeMetrics],
  b14Signals: Map[String, Seq[DiagnosticSignal]],
  driverOverrides: Map[String, DriverResourceOverride],
  clusterSpans: Map[String, Seq[ClusterSpan]] = Map.empty,
  autoscalerEvents: Map[String, Seq[AutoscalerEvent]] = Map.empty
)

/** Paired metrics for one (cluster, recipe) across reference and current dates. */
final case class MetricsPair(
  cluster: String,
  recipe: String,
  reference: RecipeMetrics,
  current: RecipeMetrics
)

/** Per-metric delta between reference and current values. */
final case class MetricDelta(
  metricName: String,
  referenceValue: Double,
  currentValue: Double,
  absoluteChange: Double,
  percentageChange: Double
)

/** Performance trend classification for a (cluster, recipe) pair. */
sealed trait PerformanceTrend {
  def label: String
}
case object Improved extends PerformanceTrend {
  val label = "improved"
}
case object Degraded extends PerformanceTrend {
  val label = "degraded"
}
case object Stable extends PerformanceTrend {
  val label = "stable"
}
case object NewEntry extends PerformanceTrend {
  val label = "new_entry"
}
case object DroppedEntry extends PerformanceTrend {
  val label = "dropped_entry"
}

/** Trend assessment for a single (cluster, recipe) with confidence level. */
final case class TrendAssessment(
  cluster: String,
  recipe: String,
  trend: PerformanceTrend,
  deltas: Seq[MetricDelta],
  confidenceLevel: Double
)

/** Evolution action decided for a (cluster, recipe). */
sealed trait EvolutionAction {
  def label: String
}
case object KeepAsIs extends EvolutionAction {
  val label = "keep_as_is"
}
case object BoostResources extends EvolutionAction {
  val label = "boost_resources"
}
case object PreserveHistorical extends EvolutionAction {
  val label = "preserve_historical"
}
case object GenerateFresh extends EvolutionAction {
  val label = "generate_fresh"
}

/** Evolution decision with the reason and underlying trend assessment. */
final case class EvolutionDecision(
  cluster: String,
  recipe: String,
  action: EvolutionAction,
  reason: String,
  trend: TrendAssessment
)

/** Correlation between two metrics across the fleet (deltas) or current snapshot.
 *
 *  view = "delta": Pearson on (current - reference) values for paired entries only.
 *  view = "current_snapshot": Pearson on raw current values, includes new entries.
 *  cluster = Some(name) when computed within a single cluster's recipes.
 */
final case class CorrelationResult(
  metricA: String,
  metricB: String,
  covariance: Double,
  pearsonCorrelation: Double,
  sampleSize: Int,
  view: String = "delta",
  cluster: Option[String] = None
)

/** Divergence detection result for a single (cluster, recipe, metric).
 *
 *  view = "delta": z-score on (current - reference) deltas.
 *  view = "current_snapshot": z-score on raw current values; new entries flagged isNewEntry.
 *  clusterScope = Some(name) when stats were computed inside that cluster only.
 */
final case class DivergenceResult(
  cluster: String,
  recipe: String,
  metricName: String,
  referenceValue: Double,
  currentValue: Double,
  zScore: Double,
  isOutlier: Boolean,
  view: String = "delta",
  isNewEntry: Boolean = false,
  clusterScope: Option[String] = None
)

/** A single (x,y) point used by frontend scatter plots — kept here so the JSON writer can emit it. */
final case class ScatterPoint(
  cluster: String,
  recipe: String,
  x: Double,
  y: Double,
  isNew: Boolean
)
