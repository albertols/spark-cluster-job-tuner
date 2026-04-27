package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.RecipeMetrics
import com.db.serna.orchestration.cluster_tuning.single.refinement.TunedClusterConfig

/**
 * Decides how to evolve cluster and recipe configurations based on performance trend assessments.
 *
 * Evolution logic:
 *   Improved     → KeepAsIs          (emit reference config unchanged)
 *   Degraded     → BoostResources    (re-plan with current metrics + b16 reboosting if applicable)
 *   Stable       → KeepAsIs          (emit reference config unchanged)
 *   NewEntry     → GenerateFresh     (plan from scratch with current metrics)
 *   DroppedEntry → PreserveHistorical (if keepHistorical=true) or skip
 */
object PerformanceEvolver {

  def decideEvolutions(
    trends: Seq[TrendAssessment],
    referenceConfigs: Map[String, TunedClusterConfig],
    currentMetrics: Map[(String, String), RecipeMetrics],
    referenceMetrics: Map[(String, String), RecipeMetrics],
    keepHistorical: Boolean
  ): Seq[EvolutionDecision] = {

    val pairedDecisions: Seq[EvolutionDecision] = trends.map { t =>
      t.trend match {
        case Improved =>
          EvolutionDecision(t.cluster, t.recipe, KeepAsIs,
            s"Performance improved (confidence=${fmt(t.confidenceLevel)}). Keeping reference config.", t)

        case Degraded =>
          val topDelta: Option[MetricDelta] = t.deltas
            .filter(d => d.metricName == "p95_job_duration_ms" || d.metricName == "fraction_reaching_cap")
            .sortBy(d => math.abs(d.percentageChange))
            .lastOption
          val reason = topDelta match {
            case Some(d) => s"${d.metricName} changed ${fmt(d.percentageChange)}%"
            case None => "performance degraded"
          }
          EvolutionDecision(t.cluster, t.recipe, BoostResources,
            s"Performance degraded: $reason. Re-planning with current metrics.", t)

        case Stable =>
          EvolutionDecision(t.cluster, t.recipe, KeepAsIs,
            s"Performance stable (confidence=${fmt(t.confidenceLevel)}). Keeping reference config.", t)

        case NewEntry =>
          EvolutionDecision(t.cluster, t.recipe, GenerateFresh,
            "New recipe not present in reference date. Generating fresh config.", t)

        case DroppedEntry =>
          if (keepHistorical) {
            EvolutionDecision(t.cluster, t.recipe, PreserveHistorical,
              "Recipe absent from current date. Preserving historical config for future evolution.", t)
          } else {
            EvolutionDecision(t.cluster, t.recipe, KeepAsIs,
              "Recipe absent from current date. Skipped (keep-historical=false).", t)
          }
      }
    }

    pairedDecisions
  }

  private def fmt(d: Double): String = f"$d%.1f"
}
