package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import java.time.{Instant, LocalDate, ZoneOffset}
import java.time.temporal.ChronoUnit

/**
 * Prebuilt mock scenarios, parameterised by date (and optional seed).
 *
 * Naming convention is deliberately fake: `mock-cluster-NNN` and
 * `mock-recipe-<word>.json`. This makes synthetic vs real data unambiguous
 * when grepping logs and CSVs.
 *
 * Each scenario sets its window to `[date 00:00 UTC, +24h)` so the output is
 * reproducible across machines (no dependency on "now"). All incarnation
 * spans and autoscaler events are placed strictly within that window.
 *
 * The `multiDateBaseline` factory derives a second-day scenario from the
 * baseline with controlled drift (recipe duration nudges, one cluster
 * dropped, one new cluster added), so the AutoTuner produces non-trivial
 * trends / correlations / divergences for the frontend dashboard.
 */
object MockScenarios {

  /** Build a 24h UTC window from a YYYY_MM_DD date string. */
  def windowFor(dateYYYYMMDD: String): (Instant, Instant) = {
    require(dateYYYYMMDD.matches("\\d{4}_\\d{2}_\\d{2}"), s"date must be YYYY_MM_DD: $dateYYYYMMDD")
    val parts = dateYYYYMMDD.split("_")
    val start = LocalDate.of(parts(0).toInt, parts(1).toInt, parts(2).toInt)
      .atStartOfDay(ZoneOffset.UTC).toInstant
    (start, start.plus(24, ChronoUnit.HOURS))
  }

  // ── Recipe presets (light, medium, heavy) ──────────────────────────────────
  // Used by multiple scenarios so changing one preset propagates consistently.

  private def recipeLight(name: String): MockRecipe = MockRecipe(
    name = name,
    avgExecutorsPerJob   = 2.0,
    p95RunMaxExecutors   = 4.0,
    avgJobDurationMs     = 60000.0,
    p95JobDurationMs     = 90000.0,
    runs                 = 50L,
    secondsAtCap         = Some(0L),
    runsReachingCap      = Some(0L),
    totalRuns            = Some(50L),
    fractionReachingCap  = Some(0.0),
    maxConcurrentJobs    = Some(2)
  )

  private def recipeMedium(name: String): MockRecipe = MockRecipe(
    name = name,
    avgExecutorsPerJob   = 6.0,
    p95RunMaxExecutors   = 12.0,
    avgJobDurationMs     = 5 * 60000.0,
    p95JobDurationMs     = 8 * 60000.0,
    runs                 = 20L,
    secondsAtCap         = Some(120L),
    runsReachingCap      = Some(2L),
    totalRuns            = Some(20L),
    fractionReachingCap  = Some(0.10),
    maxConcurrentJobs    = Some(4)
  )

  private def recipeHeavy(name: String): MockRecipe = MockRecipe(
    name = name,
    avgExecutorsPerJob   = 16.0,
    p95RunMaxExecutors   = 28.0,
    avgJobDurationMs     = 30 * 60000.0,
    p95JobDurationMs     = 50 * 60000.0,
    runs                 = 5L,
    secondsAtCap         = Some(600L),
    runsReachingCap      = Some(3L),
    totalRuns            = Some(5L),
    fractionReachingCap  = Some(0.60),
    maxConcurrentJobs    = Some(2)
  )

  // ── minimal — 1 cluster, 2 recipes, no diagnostics, no autoscaler ─────────

  def minimal(date: String, seed: Long = 1234L): MockScenario = {
    val (start, end) = windowFor(date)
    MockScenario(
      name     = "minimal",
      seed     = seed,
      window   = (start, end),
      clusters = Seq(
        MockCluster(
          name = "mock-cluster-001",
          recipes = Seq(
            recipeLight("mock-recipe-foo.json"),
            recipeMedium("mock-recipe-bar.json")
          ),
          incarnations = Seq(
            MockIncarnation(
              spanStart = start.plus(2, ChronoUnit.HOURS),
              spanEnd   = start.plus(20, ChronoUnit.HOURS)
            )
          )
        )
      )
    )
  }

  // ── baseline — 4 clusters with mixed shapes, light diagnostics ────────────

  def baseline(date: String, seed: Long = 1234L): MockScenario = {
    val (start, end) = windowFor(date)
    MockScenario(
      name   = "baseline",
      seed   = seed,
      window = (start, end),
      clusters = Seq(
        MockCluster(
          name = "mock-cluster-001",
          recipes = Seq(
            recipeMedium("mock-recipe-load-orders.json"),
            recipeLight("mock-recipe-aggregate-daily.json")
          ),
          incarnations = Seq(
            MockIncarnation(
              spanStart = start.plus(1, ChronoUnit.HOURS),
              spanEnd   = start.plus(7, ChronoUnit.HOURS),
              autoscaler = Some(MockAutoscalerProfile(
                minPrimary = 2, maxPrimary = 6, initialPrimary = 2,
                schedule = Seq((600L, 4), (3000L, 6), (10800L, 4), (18000L, 2))
              ))
            )
          ),
          driverExitCodes = Seq(
            MockExitCode(
              jobId    = "mock-job-001a",
              ts       = start.plus(2, ChronoUnit.HOURS),
              exitCode = 1,
              msg      = "synthetic non-zero exit"
            )
          )
        ),
        MockCluster(
          name = "mock-cluster-002",
          recipes = Seq(
            recipeLight("mock-recipe-cleanup.json"),
            recipeLight("mock-recipe-export-csv.json"),
            recipeMedium("mock-recipe-warehouse-load.json")
          ),
          incarnations = Seq(
            MockIncarnation(
              spanStart = start.plus(3, ChronoUnit.HOURS),
              spanEnd   = start.plus(11, ChronoUnit.HOURS)
            )
          )
        ),
        MockCluster(
          name = "mock-cluster-003",
          recipes = Seq(
            recipeHeavy("mock-recipe-ml-train.json")
          ),
          incarnations = Seq(
            MockIncarnation(
              spanStart = start.plus(5, ChronoUnit.HOURS),
              spanEnd   = start.plus(18, ChronoUnit.HOURS),
              autoscaler = Some(MockAutoscalerProfile(
                minPrimary = 4, maxPrimary = 16, initialPrimary = 4,
                schedule = Seq((900L, 8), (1800L, 12), (5400L, 16), (28800L, 8), (39600L, 4))
              ))
            )
          )
        ),
        // mock-cluster-004 has two incarnations within the window (recreated mid-day).
        // Both incarnations carry autoscaler schedules so the dashboard's per-cluster
        // cost timeline exercises both the gap-between-spans path and the b22 step
        // function path simultaneously.
        MockCluster(
          name = "mock-cluster-004",
          recipes = Seq(
            recipeMedium("mock-recipe-feature-engineer.json")
          ),
          incarnations = Seq(
            MockIncarnation(
              spanStart = start.plus(2, ChronoUnit.HOURS),
              spanEnd   = start.plus(6, ChronoUnit.HOURS),
              autoscaler = Some(MockAutoscalerProfile(
                minPrimary = 2, maxPrimary = 6, initialPrimary = 2,
                schedule = Seq((300L, 4), (1800L, 6), (10800L, 4))
              ))
            ),
            MockIncarnation(
              spanStart = start.plus(14, ChronoUnit.HOURS),
              spanEnd   = start.plus(22, ChronoUnit.HOURS),
              autoscaler = Some(MockAutoscalerProfile(
                minPrimary = 2, maxPrimary = 8, initialPrimary = 2,
                schedule = Seq((600L, 5), (3600L, 8), (18000L, 4), (25200L, 2))
              ))
            )
          )
        )
      )
    )
  }

  // ── oomHeavy — 3 clusters with b14 exit codes + b16 OOM events ────────────

  def oomHeavy(date: String, seed: Long = 1234L): MockScenario = {
    val (start, end) = windowFor(date)
    val recipeA = recipeHeavy("mock-recipe-oom-prone.json")
    val recipeB = recipeMedium("mock-recipe-stable.json")
    MockScenario(
      name   = "oomHeavy",
      seed   = seed,
      window = (start, end),
      clusters = Seq(
        MockCluster(
          name         = "mock-cluster-oom-001",
          recipes      = Seq(recipeA, recipeB),
          incarnations = Seq(MockIncarnation(start.plus(1, ChronoUnit.HOURS), start.plus(20, ChronoUnit.HOURS))),
          driverExitCodes = (1 to 3).map { i =>
            MockExitCode(
              jobId    = f"mock-oom-001-job-$i%03d",
              ts       = start.plus(2L + i, ChronoUnit.HOURS),
              exitCode = if (i % 2 == 0) 137 else 1,
              msg      = "synthetic oom-driven exit"
            )
          },
          oomEvents = Seq(
            MockOomEvent(
              jobId  = "mock-oom-001-job-001",
              recipe = recipeA.name,
              ts     = start.plus(3, ChronoUnit.HOURS)
            )
          )
        ),
        MockCluster(
          name         = "mock-cluster-oom-002",
          recipes      = Seq(recipeA),
          incarnations = Seq(MockIncarnation(start.plus(4, ChronoUnit.HOURS), start.plus(15, ChronoUnit.HOURS))),
          driverExitCodes = Seq(
            MockExitCode(
              jobId    = "mock-oom-002-job-001",
              ts       = start.plus(6, ChronoUnit.HOURS),
              exitCode = 137,
              msg      = "synthetic kernel oom-killer"
            )
          ),
          oomEvents = Seq(
            MockOomEvent(
              jobId           = "mock-oom-002-job-001",
              recipe          = recipeA.name,
              ts              = start.plus(6, ChronoUnit.HOURS).plus(30, ChronoUnit.SECONDS),
              isStackOverflow = false,
              isJavaHeap      = true,
              message         = "synthetic Java heap exhausted"
            )
          )
        ),
        MockCluster(
          name         = "mock-cluster-oom-003",
          recipes      = Seq(recipeB),
          incarnations = Seq(MockIncarnation(start.plus(8, ChronoUnit.HOURS), start.plus(19, ChronoUnit.HOURS)))
          // No diagnostics on this cluster — control case.
        )
      )
    )
  }

  // ── autoscaling — 3 clusters with rich schedules so b22 cost is non-trivial ─

  def autoscaling(date: String, seed: Long = 1234L): MockScenario = {
    val (start, end) = windowFor(date)
    MockScenario(
      name   = "autoscaling",
      seed   = seed,
      window = (start, end),
      clusters = Seq(
        // Ramp-up early, scale-down late — typical batch.
        MockCluster(
          name = "mock-cluster-as-001",
          recipes = Seq(recipeMedium("mock-recipe-batch-fact.json"), recipeMedium("mock-recipe-batch-dim.json")),
          incarnations = Seq(MockIncarnation(
            spanStart  = start.plus(2, ChronoUnit.HOURS),
            spanEnd    = start.plus(10, ChronoUnit.HOURS),
            autoscaler = Some(MockAutoscalerProfile(
              minPrimary = 2, maxPrimary = 12, initialPrimary = 2,
              schedule   = Seq((300L, 4), (900L, 8), (2400L, 12), (14400L, 8), (24000L, 4), (27600L, 2))
            ))
          ))
        ),
        // Stable cluster — single NO_SCALE early, no further changes.
        MockCluster(
          name = "mock-cluster-as-002",
          recipes = Seq(recipeLight("mock-recipe-trickle.json")),
          incarnations = Seq(MockIncarnation(
            spanStart  = start.plus(1, ChronoUnit.HOURS),
            spanEnd    = start.plus(20, ChronoUnit.HOURS),
            autoscaler = Some(MockAutoscalerProfile(
              minPrimary = 2, maxPrimary = 4, initialPrimary = 2,
              schedule   = Seq((600L, 2))
            ))
          ))
        ),
        // Spike pattern — quick scale-up, quick scale-down, repeat.
        MockCluster(
          name = "mock-cluster-as-003",
          recipes = Seq(recipeMedium("mock-recipe-spiky.json")),
          incarnations = Seq(MockIncarnation(
            spanStart  = start.plus(3, ChronoUnit.HOURS),
            spanEnd    = start.plus(15, ChronoUnit.HOURS),
            autoscaler = Some(MockAutoscalerProfile(
              minPrimary = 2, maxPrimary = 10, initialPrimary = 2,
              schedule   = Seq(
                (300L, 8), (1500L, 2),
                (3600L, 10), (5400L, 2),
                (10800L, 6), (14400L, 2)
              )
            ))
          ))
        )
      )
    )
  }

  // ── multiDateBaseline — coherent reference + current pair with drift ──────
  //
  // Drift recipe (so AutoTuner sees real signals):
  //   * mock-cluster-001: avgJobDurationMs ↑ ~30% (degraded)
  //   * mock-cluster-002: unchanged (stable)
  //   * mock-cluster-003: avgJobDurationMs ↓ ~20% (improved)
  //   * mock-cluster-004: dropped (dropped_entry trend)
  //   * mock-cluster-NEW: added (new_entry trend)

  def multiDateBaseline(refDate: String, curDate: String, seed: Long = 1234L): MultiDateScenario = {
    val ref      = baseline(refDate, seed)
    val (s2, e2) = windowFor(curDate)

    val drifted: Seq[MockCluster] = ref.clusters.flatMap { c =>
      c.name match {
        case "mock-cluster-001" =>
          Some(c.copy(
            recipes = c.recipes.map(r => r.copy(
              avgJobDurationMs   = r.avgJobDurationMs * 1.30,
              p95JobDurationMs   = r.p95JobDurationMs * 1.30
            )),
            incarnations = c.incarnations.map(rebaseIncarnation(_, ref.window._1, s2)),
            driverExitCodes = c.driverExitCodes.map(rebaseExit(_, ref.window._1, s2))
          ))
        case "mock-cluster-002" =>
          Some(c.copy(
            incarnations = c.incarnations.map(rebaseIncarnation(_, ref.window._1, s2))
          ))
        case "mock-cluster-003" =>
          Some(c.copy(
            recipes = c.recipes.map(r => r.copy(
              avgJobDurationMs = r.avgJobDurationMs * 0.80,
              p95JobDurationMs = r.p95JobDurationMs * 0.80
            )),
            incarnations = c.incarnations.map(rebaseIncarnation(_, ref.window._1, s2))
          ))
        case "mock-cluster-004" =>
          None // dropped between dates
        case _ =>
          Some(c.copy(incarnations = c.incarnations.map(rebaseIncarnation(_, ref.window._1, s2))))
      }
    }

    val newcomer = MockCluster(
      name = "mock-cluster-new",
      recipes = Seq(recipeLight("mock-recipe-newcomer.json")),
      incarnations = Seq(MockIncarnation(
        spanStart = s2.plus(4, ChronoUnit.HOURS),
        spanEnd   = s2.plus(10, ChronoUnit.HOURS)
      ))
    )

    val cur = MockScenario(
      name     = "baseline-current",
      seed     = seed,
      window   = (s2, e2),
      clusters = drifted :+ newcomer
    )

    MultiDateScenario(name = "multiDateBaseline", perDate = Map(refDate -> ref, curDate -> cur))
  }

  // ── mixedDropAndDegrade — single cluster mixing degraded + dropped recipes ─
  //
  // Designed to exercise the carry-over path in the AutoTuner's
  // BoostResources / GenerateFresh branch: when a cluster's primary action
  // reduces to BoostResources because of a degraded recipe, sibling recipes
  // whose action is PreserveHistorical (absent from the current date) must
  // still be carried into the freshly planned JSON, tagged with
  // `lastTunedDate` and `keptWithoutCurrentDate: true`.
  //
  //   Reference (refDate): 3 recipes
  //     - mock-recipe-keep-stable.json  (light, unchanged in current)
  //     - mock-recipe-must-boost.json   (medium; durations × 1.40 in current → degraded)
  //     - mock-recipe-was-here.json     (medium; absent from current → dropped_entry)
  //
  //   Current   (curDate):  2 recipes
  //     - mock-recipe-keep-stable.json
  //     - mock-recipe-must-boost.json   (degraded copy)

  def mixedDropAndDegrade(refDate: String, curDate: String, seed: Long = 1234L): MultiDateScenario = {
    val (s1, e1) = windowFor(refDate)
    val (s2, e2) = windowFor(curDate)

    val refCluster = MockCluster(
      name = "mock-cluster-mixed",
      recipes = Seq(
        recipeLight("mock-recipe-keep-stable.json"),
        recipeMedium("mock-recipe-must-boost.json"),
        recipeMedium("mock-recipe-was-here.json")
      ),
      incarnations = Seq(
        MockIncarnation(
          spanStart = s1.plus(2, ChronoUnit.HOURS),
          spanEnd   = s1.plus(10, ChronoUnit.HOURS)
        )
      )
    )
    val ref = MockScenario(
      name     = "mixedDropAndDegrade-reference",
      seed     = seed,
      window   = (s1, e1),
      clusters = Seq(refCluster)
    )

    val mustBoostBase = recipeMedium("mock-recipe-must-boost.json")
    val curCluster = MockCluster(
      name = "mock-cluster-mixed",
      recipes = Seq(
        recipeLight("mock-recipe-keep-stable.json"),
        // Degrade the medium recipe so the trend detector reports `degraded`
        // and PerformanceEvolver picks BoostResources.
        mustBoostBase.copy(
          avgJobDurationMs = mustBoostBase.avgJobDurationMs * 1.40,
          p95JobDurationMs = mustBoostBase.p95JobDurationMs * 1.40
        )
        // mock-recipe-was-here.json deliberately absent → dropped_entry → preserve_historical.
      ),
      incarnations = Seq(
        MockIncarnation(
          spanStart = s2.plus(2, ChronoUnit.HOURS),
          spanEnd   = s2.plus(10, ChronoUnit.HOURS)
        )
      )
    )
    val cur = MockScenario(
      name     = "mixedDropAndDegrade-current",
      seed     = seed,
      window   = (s2, e2),
      clusters = Seq(curCluster)
    )

    MultiDateScenario(name = "mixedDropAndDegrade", perDate = Map(refDate -> ref, curDate -> cur))
  }

  /** Shift an incarnation's span by `(newWindowStart - oldWindowStart)`. */
  private def rebaseIncarnation(inc: MockIncarnation, oldWindowStart: Instant, newWindowStart: Instant): MockIncarnation = {
    val deltaMs = newWindowStart.toEpochMilli - oldWindowStart.toEpochMilli
    inc.copy(
      spanStart = inc.spanStart.plusMillis(deltaMs),
      spanEnd   = inc.spanEnd.plusMillis(deltaMs)
    )
  }

  private def rebaseExit(e: MockExitCode, oldWindowStart: Instant, newWindowStart: Instant): MockExitCode = {
    val deltaMs = newWindowStart.toEpochMilli - oldWindowStart.toEpochMilli
    e.copy(ts = e.ts.plusMillis(deltaMs))
  }

  // ── CLI lookup ─────────────────────────────────────────────────────────────

  /** Single-date scenarios callable by name from the CLI. */
  val singleDate: Map[String, (String, Long) => MockScenario] = Map(
    "minimal"     -> (minimal _),
    "baseline"    -> (baseline _),
    "oomHeavy"    -> (oomHeavy _),
    "autoscaling" -> (autoscaling _)
  )

  val singleDateNames: Seq[String] = singleDate.keys.toSeq.sorted

  /** Multi-date scenarios callable by name from the CLI. */
  val multiDate: Map[String, (String, String, Long) => MultiDateScenario] = Map(
    "multiDateBaseline"   -> (multiDateBaseline _),
    "mixedDropAndDegrade" -> (mixedDropAndDegrade _)
  )

  val multiDateNames: Seq[String] = multiDate.keys.toSeq.sorted
}
