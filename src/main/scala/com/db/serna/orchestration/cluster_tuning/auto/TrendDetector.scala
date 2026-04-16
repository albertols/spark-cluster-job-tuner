package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.RecipeMetrics

/**
 * Classifies performance trends for (cluster, recipe) pairs by comparing
 * reference and current metrics. Uses configurable thresholds for classification.
 */
object TrendDetector {

  val DurationDegradationPct: Double = 10.0
  val DurationImprovementPct: Double = -5.0
  val CapHitDegradationPct: Double = 15.0
  val MinRunsForConfidence: Long = 5L

  private val MetricNames: Seq[String] = Seq(
    "avg_executors_per_job",
    "p95_run_max_executors",
    "avg_job_duration_ms",
    "p95_job_duration_ms",
    "fraction_reaching_cap",
    "runs"
  )

  private def refValue(name: String, m: RecipeMetrics): Double = name match {
    case "avg_executors_per_job" => m.avgExecutorsPerJob
    case "p95_run_max_executors" => m.p95RunMaxExecutors
    case "avg_job_duration_ms" => m.avgJobDurationMs
    case "p95_job_duration_ms" => m.p95JobDurationMs
    case "fraction_reaching_cap" => m.fractionReachingCap.getOrElse(0.0)
    case "runs" => m.runs.toDouble
    case _ => 0.0
  }

  def computeDeltas(ref: RecipeMetrics, cur: RecipeMetrics): Seq[MetricDelta] = {
    MetricNames.map { name =>
      val rv = refValue(name, ref)
      val cv = refValue(name, cur)
      val abs = cv - rv
      val pct = if (rv == 0.0) {
        if (cv == 0.0) 0.0 else 100.0
      } else {
        (abs / math.abs(rv)) * 100.0
      }
      MetricDelta(name, rv, cv, abs, pct)
    }
  }

  def computeConfidence(ref: RecipeMetrics, cur: RecipeMetrics): Double = {
    val minRuns = math.min(ref.runs, cur.runs)
    math.min(1.0, minRuns.toDouble / 10.0)
  }

  def assessTrend(pair: MetricsPair): TrendAssessment = {
    val deltas = computeDeltas(pair.reference, pair.current)
    val confidence = computeConfidence(pair.reference, pair.current)

    val p95DurDelta = deltas.find(_.metricName == "p95_job_duration_ms")
    val capHitDelta = deltas.find(_.metricName == "fraction_reaching_cap")

    val durationDegraded = p95DurDelta.exists(_.percentageChange > DurationDegradationPct)
    val capHitDegraded = capHitDelta.exists { d =>
      d.percentageChange > CapHitDegradationPct && d.currentValue > 0.0
    }
    val durationImproved = p95DurDelta.exists(_.percentageChange < DurationImprovementPct)
    val capHitNotWorsened = capHitDelta.forall(_.percentageChange <= CapHitDegradationPct)

    val trend: PerformanceTrend =
      if (durationDegraded || capHitDegraded) Degraded
      else if (durationImproved && capHitNotWorsened) Improved
      else Stable

    TrendAssessment(pair.cluster, pair.recipe, trend, deltas, confidence)
  }
}
