package com.db.serna.orchestration.cluster_tuning.auto

/**
 * Pure statistical functions for multi-date performance analysis.
 *
 * Uses population statistics (not sample) since we typically have the full fleet.
 * All functions use scala.math only — no external library.
 */
object StatisticalAnalysis {

  def mean(xs: Seq[Double]): Double =
    if (xs.isEmpty) 0.0 else xs.sum / xs.size

  def variance(xs: Seq[Double]): Double = {
    if (xs.size < 2) return 0.0
    val m = mean(xs)
    xs.map(x => (x - m) * (x - m)).sum / xs.size
  }

  def stddev(xs: Seq[Double]): Double = math.sqrt(variance(xs))

  def covariance(xs: Seq[Double], ys: Seq[Double]): Double = {
    require(xs.size == ys.size, "covariance requires equal-length sequences")
    if (xs.size < 2) return 0.0
    val mx = mean(xs)
    val my = mean(ys)
    xs.zip(ys).map { case (x, y) => (x - mx) * (y - my) }.sum / xs.size
  }

  def pearsonCorrelation(xs: Seq[Double], ys: Seq[Double]): Double = {
    require(xs.size == ys.size, "pearsonCorrelation requires equal-length sequences")
    val sx = stddev(xs)
    val sy = stddev(ys)
    if (sx == 0.0 || sy == 0.0) return 0.0
    val r = covariance(xs, ys) / (sx * sy)
    math.max(-1.0, math.min(1.0, r))
  }

  def zScore(value: Double, mean: Double, stddev: Double): Double =
    if (stddev == 0.0) 0.0 else (value - mean) / stddev

  // ---------- Fleet-wide analysis ----------

  private case class MetricAccessor(name: String, refValue: MetricsPair => Double, curValue: MetricsPair => Double) {
    def delta(p: MetricsPair): Double = curValue(p) - refValue(p)
  }

  private val MetricAccessors: Seq[MetricAccessor] = Seq(
    MetricAccessor("delta_avg_executors_per_job", _.reference.avgExecutorsPerJob, _.current.avgExecutorsPerJob),
    MetricAccessor("delta_p95_run_max_executors", _.reference.p95RunMaxExecutors, _.current.p95RunMaxExecutors),
    MetricAccessor("delta_avg_job_duration_ms", _.reference.avgJobDurationMs, _.current.avgJobDurationMs),
    MetricAccessor("delta_p95_job_duration_ms", _.reference.p95JobDurationMs, _.current.p95JobDurationMs),
    MetricAccessor("delta_fraction_reaching_cap", _.reference.fractionReachingCap.getOrElse(0.0), _.current.fractionReachingCap.getOrElse(0.0)),
    MetricAccessor("delta_runs", _.reference.runs.toDouble, _.current.runs.toDouble)
  )

  private val CorrelationPairs: Seq[(String, String)] = Seq(
    ("delta_p95_run_max_executors", "delta_p95_job_duration_ms"),
    ("delta_avg_executors_per_job", "delta_avg_job_duration_ms"),
    ("delta_fraction_reaching_cap", "delta_p95_job_duration_ms"),
    ("delta_runs", "delta_avg_job_duration_ms")
  )

  private def accessorFor(name: String): Option[MetricAccessor] =
    MetricAccessors.find(_.name == name)

  /** Compute Pearson correlations for predefined metric delta pairs across the fleet. */
  def computeCorrelations(pairs: Seq[MetricsPair]): Seq[CorrelationResult] = {
    if (pairs.size < 2) return Seq.empty
    CorrelationPairs.flatMap { case (nameA, nameB) =>
      for {
        accA <- accessorFor(nameA)
        accB <- accessorFor(nameB)
      } yield {
        val xs = pairs.map(accA.delta)
        val ys = pairs.map(accB.delta)
        CorrelationResult(
          metricA = nameA,
          metricB = nameB,
          covariance = covariance(xs, ys),
          pearsonCorrelation = pearsonCorrelation(xs, ys),
          sampleSize = pairs.size
        )
      }
    }
  }

  /** Detect outlier (cluster, recipe) pairs whose metric deltas deviate significantly from the fleet mean. */
  def detectDivergences(pairs: Seq[MetricsPair], zThreshold: Double = 2.0): Seq[DivergenceResult] = {
    if (pairs.size < 2) return Seq.empty
    MetricAccessors.flatMap { accessor =>
      val deltas = pairs.map(accessor.delta)
      val m = mean(deltas)
      val s = stddev(deltas)
      if (s == 0.0) Seq.empty
      else pairs.zip(deltas).flatMap { case (pair, delta) =>
        val z = zScore(delta, m, s)
        if (math.abs(z) >= zThreshold) {
          Some(DivergenceResult(
            cluster = pair.cluster,
            recipe = pair.recipe,
            metricName = accessor.name,
            referenceValue = accessor.refValue(pair),
            currentValue = accessor.curValue(pair),
            zScore = z,
            isOutlier = true
          ))
        } else None
      }
    }
  }
}
