package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import java.time.Instant

/**
 * Pure data describing a synthetic cluster fixture. No I/O — these case classes
 * feed `MockGen` (writers) and are instantiated by `MockScenarios` (prebuilt
 * fixtures). All names use the obviously-fake convention (`mock-cluster-001`,
 * `mock-recipe-foo.json`) so synthetic vs real data is unambiguous.
 *
 * Referential integrity (enforced by MockGen and asserted in ScenarioSpec):
 *   - Every `MockCluster.name` appears in the b13 (or individual b1..b12) and b20 outputs.
 *   - Every `(cluster, recipe)` pair across files agrees on `recipe.name`.
 *   - Every `b21` event's eventTs falls inside some b20 incarnation span for that cluster.
 *   - Every `b14` exit code / `b16` OOM event references a real cluster (and recipe, for b16).
 *   - `MockAutoscalerProfile.minPrimary <= target <= maxPrimary` for every schedule entry.
 */

final case class MockRecipe(
                             name: String,
                             avgExecutorsPerJob: Double,
                             p95RunMaxExecutors: Double,
                             avgJobDurationMs: Double,
                             p95JobDurationMs: Double,
                             runs: Long,
                             secondsAtCap: Option[Long]      = None,
                             runsReachingCap: Option[Long]   = None,
                             totalRuns: Option[Long]         = None,
                             fractionReachingCap: Option[Double] = None,
                             maxConcurrentJobs: Option[Int]  = None
                           )

/**
 * Worker-count step function for one cluster incarnation. `schedule` is a list
 * of (offset-seconds-from-spanStart, target-primary-workers) pairs. The
 * generator emits one b21 RECOMMENDING row per schedule entry, with `current`
 * derived from the previous entry (or `initialPrimary` for the first).
 */
final case class MockAutoscalerProfile(
                                        minPrimary: Int,
                                        maxPrimary: Int,
                                        initialPrimary: Int,
                                        schedule: Seq[(Long, Int)] = Nil
                                      ) {
  require(minPrimary >= 0 && maxPrimary >= minPrimary, s"invalid worker bounds: min=$minPrimary max=$maxPrimary")
  require(initialPrimary >= minPrimary && initialPrimary <= maxPrimary,
    s"initialPrimary=$initialPrimary outside [$minPrimary, $maxPrimary]")
  schedule.foreach { case (off, target) =>
    require(off >= 0L,                      s"schedule offset must be non-negative: $off")
    require(target >= minPrimary && target <= maxPrimary,
      s"schedule target $target outside [$minPrimary, $maxPrimary]")
  }
}

final case class MockExitCode(
                               jobId: String,
                               ts: Instant,
                               exitCode: Int,
                               msg: String = "synthetic driver exit"
                             ) {
  require(exitCode != 0, "MockExitCode is only for non-zero exits (b14 filters zero out)")
}

final case class MockOomEvent(
                               jobId: String,
                               recipe: String,
                               ts: Instant,
                               exceptionType: String                        = "java.lang.OutOfMemoryError",
                               severity: String                             = "ERROR",
                               driverClass: String                          = "org.apache.spark.deploy.SparkSubmit",
                               isLostTask: Boolean                          = false,
                               isStackOverflow: Boolean                     = false,
                               isJavaHeap: Boolean                          = true,
                               message: String                              = "synthetic OOM",
                               logName: String                              = "projects/oss-mock/logs/google.dataproc.agent"
                             )

/**
 * One incarnation interval of a cluster. The generator emits one b20 row per
 * incarnation; `(spanStart, spanEnd]` is the billing window. `events` may be
 * empty (b23 avg-fallback path applies to cost) or carry the autoscaler
 * decisions seen during this incarnation. Multiple incarnations of the same
 * cluster within `MockScenario.window` model "cluster recreated" patterns.
 */
final case class MockIncarnation(
                                  spanStart: Instant,
                                  spanEnd: Instant,
                                  hasExplicitCreate: Boolean = true,
                                  hasExplicitDelete: Boolean = true,
                                  autoscaler: Option[MockAutoscalerProfile] = None,
                                  // When true, MockGen.b20Csv skips this incarnation while b21 (autoscaler
                                  // events) is still emitted. Mirrors the real-world pattern where the
                                  // b20 cluster-span query returns no row but b21 has events — the tuner
                                  // synthesizes a span from event boundaries (synthetic_span = true).
                                  excludeFromB20: Boolean = false
                                ) {
  require(spanEnd.isAfter(spanStart), s"spanEnd must be after spanStart: start=$spanStart end=$spanEnd")
}

final case class MockCluster(
                              name: String,
                              recipes: Seq[MockRecipe],
                              incarnations: Seq[MockIncarnation],
                              driverExitCodes: Seq[MockExitCode] = Nil,
                              oomEvents: Seq[MockOomEvent]       = Nil
                            ) {
  require(name.nonEmpty,                 "MockCluster.name must be non-empty")
  require(incarnations.nonEmpty,         s"MockCluster $name has no incarnations (need at least one — even if all are excludeFromB20)")
  oomEvents.foreach { o =>
    require(recipes.exists(_.name == o.recipe),
      s"MockCluster $name has OOM event referencing unknown recipe ${o.recipe}")
  }
}

/**
 * One generation pass for a single date. `window` bounds every incarnation's
 * span and every autoscaler event. `seed` makes any RNG-driven jitter
 * (timestamps, decision strings) reproducible.
 */
final case class MockScenario(
                               name: String,
                               clusters: Seq[MockCluster],
                               window: (Instant, Instant),
                               seed: Long = 1234L
                             ) {
  require(window._2.isAfter(window._1), s"scenario window invalid: $window")
  clusters.foreach { c =>
    c.incarnations.foreach { inc =>
      require(!inc.spanStart.isBefore(window._1) && !inc.spanEnd.isAfter(window._2),
        s"cluster ${c.name} incarnation $inc falls outside scenario window $window")
    }
  }
}

/**
 * A coherent set of single-date scenarios, one per YYYY_MM_DD. The AutoTuner
 * compares two of these (reference + current) so multi-date drift produces
 * meaningful trends/correlations/divergences for the frontend dashboard.
 */
final case class MultiDateScenario(
                                    name: String,
                                    perDate: Map[String, MockScenario]
                                  ) {
  require(perDate.nonEmpty, "MultiDateScenario must contain at least one date")
  perDate.keys.foreach { d =>
    require(d.matches("\\d{4}_\\d{2}_\\d{2}"), s"date key '$d' is not in YYYY_MM_DD format")
  }
}
