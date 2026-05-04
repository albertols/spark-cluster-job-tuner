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

  // ── syntheticSpan — exercises b20-missing / b21-present synthesis ─────────
  //
  // Two clusters: one normal (b20 + b21), one whose incarnations are excluded
  // from b20 entirely. The tuner must synthesize a span from b21 event
  // boundaries (synthetic_span = true) and the frontend must surface the
  // "b22 · synthetic span" badge in both the IPC table-wrap header and the
  // Cluster cost & autoscaling card.

  def syntheticSpan(date: String, seed: Long = 1234L): MockScenario = {
    val (start, end) = windowFor(date)
    MockScenario(
      name   = "syntheticSpan",
      seed   = seed,
      window = (start, end),
      clusters = Seq(
        // Normal control: b20 row present, b21 events present.
        MockCluster(
          name = "mock-cluster-ss-normal-001",
          recipes = Seq(recipeMedium("mock-recipe-control.json")),
          incarnations = Seq(MockIncarnation(
            spanStart  = start.plus(2, ChronoUnit.HOURS),
            spanEnd    = start.plus(8, ChronoUnit.HOURS),
            autoscaler = Some(MockAutoscalerProfile(
              minPrimary = 2, maxPrimary = 8, initialPrimary = 2,
              schedule   = Seq((300L, 4), (1800L, 6), (12000L, 4), (18000L, 2))
            ))
          ))
        ),
        // b20 missing for this cluster — only b21 events get emitted. The tuner
        // synthesizes the span from event min/max timestamps and flags it.
        MockCluster(
          name = "mock-cluster-ss-synth-002",
          recipes = Seq(recipeMedium("mock-recipe-orphan.json")),
          incarnations = Seq(MockIncarnation(
            spanStart      = start.plus(4, ChronoUnit.HOURS),
            spanEnd        = start.plus(7, ChronoUnit.HOURS),
            excludeFromB20 = true,
            autoscaler     = Some(MockAutoscalerProfile(
              minPrimary = 2, maxPrimary = 6, initialPrimary = 2,
              schedule   = Seq((120L, 4), (3600L, 6), (7200L, 4), (10200L, 2))
            ))
          ))
        ),
        // Second b20-missing cluster with a different shape so the IPC chart
        // shows multiple synthetic-span clusters at once.
        MockCluster(
          name = "mock-cluster-ss-synth-003",
          recipes = Seq(recipeLight("mock-recipe-stub.json")),
          incarnations = Seq(MockIncarnation(
            spanStart      = start.plus(5, ChronoUnit.HOURS).plus(30, ChronoUnit.MINUTES),
            spanEnd        = start.plus(6, ChronoUnit.HOURS).plus(30, ChronoUnit.MINUTES),
            excludeFromB20 = true,
            autoscaler     = Some(MockAutoscalerProfile(
              minPrimary = 2, maxPrimary = 4, initialPrimary = 2,
              schedule   = Seq((60L, 3), (1800L, 4), (3000L, 2))
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

  private def rebaseOom(o: MockOomEvent, oldWindowStart: Instant, newWindowStart: Instant): MockOomEvent = {
    val deltaMs = newWindowStart.toEpochMilli - oldWindowStart.toEpochMilli
    o.copy(ts = o.ts.plusMillis(deltaMs))
  }

  // ── divergenceShowcase — end-to-end demo for the z-score-driven features ───
  //
  // Designed to exercise (and visibly demonstrate in the dashboard) the three
  // features added on top of the base AutoTuner:
  //
  //   1. Compounded b16 OOM boost across runs (carryPriorBoostMetadata + ReBoost):
  //      `mock-cluster-show-oom` has recipe `_DQ3_OOM_RECURRING.json` that
  //      takes a b16 OOM in BOTH dates. Its cluster trend goes Degraded
  //      (duration ↑40%), so the AutoTuner re-plans the cluster fresh; the
  //      carry restores the prior `appliedMemoryHeapBoostFactor=1.5` from the
  //      reference output, then `applyB16Reboosting` sees a fresh signal and
  //      compounds → `factor=2.25`, `spark.executor.memory=18g`, state=re-boost.
  //
  //   2. Boost holding when the b16 signal disappears:
  //      `mock-cluster-show-holding` has `_RDM_BOOST_HOLDING.json` (b16 OOM
  //      in REF only) and `_RDM_DEGRADED_COMPANION.json` (degraded duration
  //      in current). The companion forces a cluster re-plan, and the holder
  //      recipe gets its prior factor=1.5 carried forward; with no fresh
  //      signal in current's b16, the vitamin classifies it Holding —
  //      memory stays at 12g, factor preserved.
  //
  //   3. Divergence-driven executor scale-up:
  //      `mock-cluster-show-needs-execs` has `_DWH_NEEDS_MORE_EXECUTORS.json`
  //      whose p95_job_duration explodes ~40× and p95_run_max_executors stays
  //      saturated against the planned maxExecutors. The current-snapshot
  //      z-score on duration crosses the default threshold of 3.0 and the
  //      vitamin bumps `spark.dynamicAllocation.maxExecutors` by 1.5×.
  //      A NEW recipe (`_CTRL_NEWCOMER.json`) is added on the current date so
  //      the dashboard renders the NEW pill and the regression check (new
  //      entries are NEVER auto-scaled) is visible.
  //
  // Fleet stats: 6 clusters, 14 paired recipes + 1 new on the current date.
  // The control clusters keep the variance low enough that B1's z ≈ 3.9.

  def divergenceShowcase(refDate: String, curDate: String, seed: Long = 1234L): MultiDateScenario = {
    val (s1, e1) = windowFor(refDate)
    val (s2, e2) = windowFor(curDate)

    // ── Reference fleet ─────────────────────────────────────────────────────
    val refClusters: Seq[MockCluster] = Seq(
      // (1) OOM-recurring cluster: A1 takes b16 in BOTH dates.
      MockCluster(
        name = "mock-cluster-show-oom",
        recipes = Seq(
          recipeMedium("_DQ3_OOM_RECURRING.json"),
          recipeLight("_DQ3_OOM_COMPANION.json")
        ),
        incarnations = Seq(MockIncarnation(
          spanStart = s1.plus(2, ChronoUnit.HOURS),
          spanEnd   = s1.plus(8, ChronoUnit.HOURS)
        )),
        oomEvents = Seq(MockOomEvent(
          jobId   = "mock-job-show-oom-001",
          recipe  = "_DQ3_OOM_RECURRING.json",
          ts      = s1.plus(3, ChronoUnit.HOURS),
          message = "synthetic recurring heap OOM"
        ))
      ),
      // (2) Cap-touching cluster: B1 will explode in current → executor scale-up.
      MockCluster(
        name = "mock-cluster-show-needs-execs",
        recipes = Seq(
          recipeMedium("_DWH_NEEDS_MORE_EXECUTORS.json")
        ),
        incarnations = Seq(MockIncarnation(
          spanStart = s1.plus(1, ChronoUnit.HOURS),
          spanEnd   = s1.plus(7, ChronoUnit.HOURS)
        ))
      ),
      // (3) Boost-holding cluster: D1 takes b16 in REF only; D2 is the
      //     companion that will degrade in current and force a cluster re-plan.
      MockCluster(
        name = "mock-cluster-show-holding",
        recipes = Seq(
          recipeMedium("_RDM_BOOST_HOLDING.json"),
          recipeMedium("_RDM_DEGRADED_COMPANION.json")
        ),
        incarnations = Seq(MockIncarnation(
          spanStart = s1.plus(2, ChronoUnit.HOURS),
          spanEnd   = s1.plus(9, ChronoUnit.HOURS)
        )),
        oomEvents = Seq(MockOomEvent(
          jobId   = "mock-job-show-holding-001",
          recipe  = "_RDM_BOOST_HOLDING.json",
          ts      = s1.plus(4, ChronoUnit.HOURS),
          message = "synthetic one-shot heap OOM"
        ))
      ),
      // (4-6) Control clusters — light recipes only, keep variance low.
      MockCluster(
        name = "mock-cluster-show-ctrl-1",
        recipes = Seq(
          recipeLight("_CTRL_lookup_1.json"),
          recipeLight("_CTRL_lookup_2.json"),
          recipeLight("_CTRL_lookup_3.json")
        ),
        incarnations = Seq(MockIncarnation(
          spanStart = s1.plus(1, ChronoUnit.HOURS),
          spanEnd   = s1.plus(6, ChronoUnit.HOURS)
        ))
      ),
      MockCluster(
        name = "mock-cluster-show-ctrl-2",
        recipes = Seq(
          recipeLight("_CTRL_aggregate_1.json"),
          recipeLight("_CTRL_aggregate_2.json"),
          recipeLight("_CTRL_aggregate_3.json")
        ),
        incarnations = Seq(MockIncarnation(
          spanStart = s1.plus(2, ChronoUnit.HOURS),
          spanEnd   = s1.plus(7, ChronoUnit.HOURS)
        ))
      ),
      MockCluster(
        name = "mock-cluster-show-ctrl-3",
        recipes = Seq(
          recipeLight("_CTRL_export_1.json"),
          recipeLight("_CTRL_export_2.json"),
          recipeLight("_CTRL_export_3.json"),
          recipeLight("_CTRL_export_4.json")
        ),
        incarnations = Seq(MockIncarnation(
          spanStart = s1.plus(3, ChronoUnit.HOURS),
          spanEnd   = s1.plus(8, ChronoUnit.HOURS)
        ))
      )
    )

    val ref = MockScenario(
      name     = "divergenceShowcase-reference",
      seed     = seed,
      window   = (s1, e1),
      clusters = refClusters
    )

    // ── Current fleet ───────────────────────────────────────────────────────
    val curClusters: Seq[MockCluster] = refClusters.map { c =>
      c.name match {
        case "mock-cluster-show-oom" =>
          // A1 still OOMs AND its duration ↑40% so the cluster trend goes
          // Degraded → AutoTuner re-plans → carry restores prior boost →
          // applyB16Reboosting compounds (factor 1.5 → 2.25, mem 12g → 18g).
          c.copy(
            recipes = c.recipes.map { r =>
              if (r.name == "_DQ3_OOM_RECURRING.json")
                r.copy(
                  avgJobDurationMs = r.avgJobDurationMs * 1.40,
                  p95JobDurationMs = r.p95JobDurationMs * 1.40
                )
              else r
            },
            incarnations = c.incarnations.map(rebaseIncarnation(_, ref.window._1, s2)),
            oomEvents    = c.oomEvents.map(rebaseOom(_, ref.window._1, s2))
          )

        case "mock-cluster-show-needs-execs" =>
          // B1: 40× p95_job_duration AND p95_run_max_executors saturates the
          // planned ceiling. After the cluster's degraded re-plan, the
          // executor scale-up vitamin bumps maxExecutors ×1.5.
          c.copy(
            recipes = c.recipes.map { r =>
              r.copy(
                avgJobDurationMs   = r.avgJobDurationMs * 40.0,
                p95JobDurationMs   = r.p95JobDurationMs * 40.0,
                p95RunMaxExecutors = math.max(r.p95RunMaxExecutors, 14.0),
                avgExecutorsPerJob = math.max(r.avgExecutorsPerJob, 12.0)
              )
            },
            incarnations = c.incarnations.map(rebaseIncarnation(_, ref.window._1, s2))
          )

        case "mock-cluster-show-holding" =>
          // D1 unchanged AND no fresh OOM (oomEvents emptied). D2 ↑40%
          // forces the cluster's primary action to BoostResources → re-plan
          // → D1 gets its prior factor carried, no fresh signal → Holding.
          c.copy(
            recipes = c.recipes.map { r =>
              if (r.name == "_RDM_DEGRADED_COMPANION.json")
                r.copy(
                  avgJobDurationMs = r.avgJobDurationMs * 1.40,
                  p95JobDurationMs = r.p95JobDurationMs * 1.40
                )
              else r
            },
            incarnations = c.incarnations.map(rebaseIncarnation(_, ref.window._1, s2)),
            oomEvents    = Seq.empty
          )

        case _ =>
          // Controls: shift incarnations to the current window, no metric drift.
          c.copy(incarnations = c.incarnations.map(rebaseIncarnation(_, ref.window._1, s2)))
      }
    }

    // Add a brand-new recipe to one control cluster so the divergence "Current
    // snapshot" tab has a NEW pill to show. Its duration is normal so the
    // single high-z outlier (B1) stays clean for the demo.
    val curClustersWithNewcomer: Seq[MockCluster] = curClusters.map { c =>
      if (c.name == "mock-cluster-show-ctrl-3") {
        c.copy(recipes = c.recipes :+ recipeLight("_CTRL_NEWCOMER.json"))
      } else c
    }

    val cur = MockScenario(
      name     = "divergenceShowcase-current",
      seed     = seed,
      window   = (s2, e2),
      clusters = curClustersWithNewcomer
    )

    MultiDateScenario(name = "divergenceShowcase",
      perDate = Map(refDate -> ref, curDate -> cur))
  }

  // ── CLI lookup ─────────────────────────────────────────────────────────────

  /** Single-date scenarios callable by name from the CLI. */
  val singleDate: Map[String, (String, Long) => MockScenario] = Map(
    "minimal"       -> (minimal _),
    "baseline"      -> (baseline _),
    "oomHeavy"      -> (oomHeavy _),
    "autoscaling"   -> (autoscaling _),
    "syntheticSpan" -> (syntheticSpan _)
  )

  val singleDateNames: Seq[String] = singleDate.keys.toSeq.sorted

  // ── multiDateSyntheticSpan — multi-date pair with synthetic-span clusters ─
  //
  // Both reference and current dates use the syntheticSpan single-date scenario
  // (rebased to each date's window). Lets the AutoTuner produce a full output
  // pair the frontend can render — including the b22 badges in IPC and the
  // Cluster cost & autoscaling 3-card view.

  def multiDateSyntheticSpan(refDate: String, curDate: String, seed: Long = 1234L): MultiDateScenario = {
    val ref = syntheticSpan(refDate, seed)
    val (s2, e2) = windowFor(curDate)
    val curClusters = ref.clusters.map { c =>
      c.copy(incarnations = c.incarnations.map(rebaseIncarnation(_, ref.window._1, s2)))
    }
    val cur = MockScenario(name = "syntheticSpan", clusters = curClusters, window = (s2, e2), seed = seed)
    MultiDateScenario(name = "multiDateSyntheticSpan", perDate = Map(refDate -> ref, curDate -> cur))
  }

  /** Multi-date scenarios callable by name from the CLI. */
  val multiDate: Map[String, (String, String, Long) => MultiDateScenario] = Map(
    "multiDateBaseline"      -> (multiDateBaseline _),
    "mixedDropAndDegrade"    -> (mixedDropAndDegrade _),
    "multiDateSyntheticSpan" -> (multiDateSyntheticSpan _),
    "divergenceShowcase"     -> (divergenceShowcase _)
  )

  val multiDateNames: Seq[String] = multiDate.keys.toSeq.sorted
}
