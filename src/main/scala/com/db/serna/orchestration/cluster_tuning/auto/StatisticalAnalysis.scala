package com.db.serna.orchestration.cluster_tuning.auto

import com.db.serna.orchestration.cluster_tuning.single.RecipeMetrics

/**
 * Pure statistical functions for multi-date performance analysis.
 *
 * Uses population statistics (not sample) since we typically have the full fleet.
 * All functions use scala.math only — no external library.
 */
object StatisticalAnalysis {

  /** Minimum group size to compute a credible per-cluster stat. Smaller groups fall back to the fleet value. */
  val MinGroupSize: Int = 5

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

  /** Accessors over a single RecipeMetrics, used for the current-snapshot view (no reference, includes new entries). */
  private case class CurrentMetricAccessor(name: String, value: RecipeMetrics => Double)

  private val CurrentMetricAccessors: Seq[CurrentMetricAccessor] = Seq(
    CurrentMetricAccessor("avg_executors_per_job", _.avgExecutorsPerJob),
    CurrentMetricAccessor("p95_run_max_executors", _.p95RunMaxExecutors),
    CurrentMetricAccessor("avg_job_duration_ms", _.avgJobDurationMs),
    CurrentMetricAccessor("p95_job_duration_ms", _.p95JobDurationMs),
    CurrentMetricAccessor("fraction_reaching_cap", _.fractionReachingCap.getOrElse(0.0)),
    CurrentMetricAccessor("runs", _.runs.toDouble)
  )

  /** The 4 metric pairs we always correlate (delta view). */
  private val CorrelationPairs: Seq[(String, String)] = Seq(
    ("delta_p95_run_max_executors", "delta_p95_job_duration_ms"),
    ("delta_avg_executors_per_job", "delta_avg_job_duration_ms"),
    ("delta_fraction_reaching_cap", "delta_p95_job_duration_ms"),
    ("delta_runs", "delta_avg_job_duration_ms")
  )

  /** Same 4 pairs without the `delta_` prefix — used for the current-snapshot view. */
  private val CurrentCorrelationPairs: Seq[(String, String)] =
    CorrelationPairs.map { case (a, b) => (a.stripPrefix("delta_"), b.stripPrefix("delta_")) }

  /** Stable scatter-data key used by the frontend, e.g. "delta_p95_run_max_executors__delta_p95_job_duration_ms". */
  def scatterKey(metricA: String, metricB: String): String = s"${metricA}__$metricB"

  private def accessorFor(name: String): Option[MetricAccessor] =
    MetricAccessors.find(_.name == name)

  private def currentAccessorFor(name: String): Option[CurrentMetricAccessor] =
    CurrentMetricAccessors.find(_.name == name)

  // ---------- Delta-view (existing) ----------

  /** Compute Pearson correlations for predefined metric delta pairs across the fleet. */
  def computeCorrelations(pairs: Seq[MetricsPair]): Seq[CorrelationResult] =
    computeCorrelationsScoped(pairs, cluster = None)

  private def computeCorrelationsScoped(pairs: Seq[MetricsPair], cluster: Option[String]): Seq[CorrelationResult] = {
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
          sampleSize = pairs.size,
          view = "delta",
          cluster = cluster
        )
      }
    }
  }

  /** Detect outlier (cluster, recipe) pairs whose metric deltas deviate significantly from the fleet mean. */
  def detectDivergences(pairs: Seq[MetricsPair], zThreshold: Double = 2.0): Seq[DivergenceResult] =
    detectDivergencesScoped(pairs, zThreshold, clusterScope = None)

  private def detectDivergencesScoped(
    pairs: Seq[MetricsPair],
    zThreshold: Double,
    clusterScope: Option[String]
  ): Seq[DivergenceResult] = {
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
            isOutlier = true,
            view = "delta",
            isNewEntry = false,
            clusterScope = clusterScope
          ))
        } else None
      }
    }
  }

  // ---------- Per-cluster grouping ----------

  /** Per-cluster correlation matrices. Clusters with `< minN` paired recipes are omitted (frontend falls back). */
  def computePerClusterCorrelations(
    pairs: Seq[MetricsPair],
    minN: Int = MinGroupSize
  ): Map[String, Seq[CorrelationResult]] = {
    pairs.groupBy(_.cluster).flatMap { case (cluster, clusterPairs) =>
      if (clusterPairs.size < minN) None
      else Some(cluster -> computeCorrelationsScoped(clusterPairs, Some(cluster)))
    }
  }

  /** Per-cluster z-score detection. Stats are computed inside each cluster (not vs the fleet). */
  def detectPerClusterDivergences(
    pairs: Seq[MetricsPair],
    zThreshold: Double = 2.0,
    minN: Int = MinGroupSize
  ): Map[String, Seq[DivergenceResult]] = {
    pairs.groupBy(_.cluster).flatMap { case (cluster, clusterPairs) =>
      if (clusterPairs.size < minN) None
      else Some(cluster -> detectDivergencesScoped(clusterPairs, zThreshold, Some(cluster)))
    }
  }

  // ---------- Current-snapshot view (includes new entries) ----------

  /** Pearson correlations on raw current values across all current-date entries (paired + new).
   *
   *  Uses the same 4 metric pairs but without the `delta_` prefix; entries get `view = "current_snapshot"`.
   */
  def computeCurrentSnapshotCorrelations(
    snapshot: DateSnapshot,
    refKeys: Set[(String, String)]
  ): Seq[CorrelationResult] = {
    val entries: Seq[((String, String), RecipeMetrics)] = snapshot.metrics.toSeq
    if (entries.size < 2) return Seq.empty
    CurrentCorrelationPairs.flatMap { case (nameA, nameB) =>
      for {
        accA <- currentAccessorFor(nameA)
        accB <- currentAccessorFor(nameB)
      } yield {
        val xs = entries.map { case (_, m) => accA.value(m) }
        val ys = entries.map { case (_, m) => accB.value(m) }
        CorrelationResult(
          metricA = nameA,
          metricB = nameB,
          covariance = covariance(xs, ys),
          pearsonCorrelation = pearsonCorrelation(xs, ys),
          sampleSize = entries.size,
          view = "current_snapshot",
          cluster = None
        )
      }
    }
  }

  /** Z-scores on raw current values, computed per metric across all current-date entries.
   *
   *  Entries flagged `isNewEntry = true` if the (cluster, recipe) is not present in `refKeys`.
   */
  def detectCurrentSnapshotZScores(
    snapshot: DateSnapshot,
    refKeys: Set[(String, String)],
    zThreshold: Double = 2.0
  ): Seq[DivergenceResult] = {
    val entries: Seq[((String, String), RecipeMetrics)] = snapshot.metrics.toSeq
    if (entries.size < 2) return Seq.empty
    CurrentMetricAccessors.flatMap { accessor =>
      val values = entries.map { case (_, m) => accessor.value(m) }
      val m = mean(values)
      val s = stddev(values)
      if (s == 0.0) Seq.empty
      else entries.zip(values).flatMap { case (((cluster, recipe), _), v) =>
        val z = zScore(v, m, s)
        if (math.abs(z) >= zThreshold) {
          Some(DivergenceResult(
            cluster = cluster,
            recipe = recipe,
            metricName = accessor.name,
            referenceValue = 0.0, // no reference in this view
            currentValue = v,
            zScore = z,
            isOutlier = true,
            view = "current_snapshot",
            isNewEntry = !refKeys.contains((cluster, recipe)),
            clusterScope = None
          ))
        } else None
      }
    }
  }

  // ---------- Scatter point extraction ----------

  /** All scatter points for a given metric pair, delta view. */
  def scatterPointsDelta(
    pairs: Seq[MetricsPair],
    metricA: String,
    metricB: String
  ): Seq[ScatterPoint] = {
    val a = accessorFor(metricA)
    val b = accessorFor(metricB)
    if (a.isEmpty || b.isEmpty) return Seq.empty
    pairs.map { p =>
      ScatterPoint(p.cluster, p.recipe, a.get.delta(p), b.get.delta(p), isNew = false)
    }
  }

  /** All scatter points for a given metric pair, current-snapshot view (uses the un-prefixed names). */
  def scatterPointsCurrentSnapshot(
    snapshot: DateSnapshot,
    refKeys: Set[(String, String)],
    metricA: String,
    metricB: String
  ): Seq[ScatterPoint] = {
    val a = currentAccessorFor(metricA)
    val b = currentAccessorFor(metricB)
    if (a.isEmpty || b.isEmpty) return Seq.empty
    snapshot.metrics.toSeq.map { case ((cluster, recipe), m) =>
      ScatterPoint(cluster, recipe, a.get.value(m), b.get.value(m), isNew = !refKeys.contains((cluster, recipe)))
    }
  }

  /** All correlation pairs as plain string tuples — handy for callers that need to iterate. */
  def correlationPairsDelta: Seq[(String, String)] = CorrelationPairs

  /** Same pairs without the `delta_` prefix (for the current-snapshot view). */
  def correlationPairsCurrentSnapshot: Seq[(String, String)] = CurrentCorrelationPairs
}
