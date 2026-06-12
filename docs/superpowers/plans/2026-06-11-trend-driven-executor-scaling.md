# Trend-Driven Proportional Executor Scaling — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the longitudinal reference→current job-duration trend into executor sizing so `min`/`initial`/`max` (and manual `instances`) rise when a cap-pressured job slows down and shrink conservatively when it speeds up — closing the "censoring trap" where a job pinned at its `maxExecutors` cannot express that it needs more.

**Architecture:** A new **pure** decision object `ExecutorTrendScaler` computes a `TrendScaleDecision` from reference + current `RecipeMetrics`, current allocation, bias-derived `ScaleGains`, cluster capacity, and any prior cumulative factor. A thin adapter `ExecutorTrendVitamin` (carrying `TrendScaleSignal`/`TrendScaleBoost`) plugs the decision into the existing `RefinementPipeline` so the JSON read/write, ordering, and dedupe paths are reused. The AutoTuner builds signals from the `pairs` it already computes, applies the trend vitamin **before** the preserved z-score `ExecutorScaleVitamin` (which stays as an additive extra-boost for genuine outliers), and stamps a dedicated `appliedTrendScaleFactor` field (distinct from the z-score path's `appliedExecutorScaleFactor`) so the two mechanisms never cross-talk.

**Tech Stack:** Scala 2.12.18, ScalaTest (`AnyFunSuite with Matchers`, Spark-free), Scallop CLI. No Maven Scala compile — compilation and tests run through IntelliJ IDEA (per `CLAUDE.md` / cluster-tuning skill).

---

## Conventions for this plan

- **No `mvn` Scala build exists.** "Run the test" = run the named spec via the IntelliJ ScalaTest runner. "Expected: FAIL/PASS" describes the runner result. Do not attempt `mvn test` for Scala sources.
- All new code uses the existing section-banner style `// ── Title ──` and `private[cluster_tuning]` / `private[refinement]` visibility where tests need access.
- Reuse existing helpers: `Sizing.roundUp`, `Sizing.clamp`, `SimpleJsonParser`, `RefinementPipeline.refine`/`toRefinedJson`, `TrendDetector.computeConfidence` (logic mirrored, not imported, to keep the scaler pure).
- Commit after every task with the shown message.

## File structure

| File | Responsibility | New/Modify |
| --- | --- | --- |
| `single/refinement/ExecutorTrendScaler.scala` | Pure math: `ScaleGains`, `ScaleDirection`, `TrendScaleDecision`, `ExecutorTrendScaler.decide` | Create |
| `single/refinement/ExecutorTrendVitamin.scala` | Pipeline adapter: `TrendScaleSignal`, `TrendScaleBoost`, `ExecutorTrendVitamin` | Create |
| `single/refinement/SimpleJsonParser.scala` | Round-trip `appliedTrendScaleFactor` extra field | Modify (`:54-75`) |
| `auto/BoostMetadataCarrier.scala` | Carry boosted min/initial + `appliedTrendScaleFactor` across re-plans | Modify |
| `auto/ClusterMachineAndRecipeAutoTuner.scala` | CLI flags, build signals from `pairs`, `applyTrendScaling`, call sites, summary field | Modify |
| `single/GenerationSummary.scala` / `auto/AutoTunerJsonOutput.scala` | `boost_groups` direction/source field | Modify |
| `auto/oss_mock/MockScenarios.scala` + `MockGen.scala` | `durationDrift` scenario producing a censored, degraded recipe | Modify |
| `test/.../single/refinement/ExecutorTrendScalerSpec.scala` | Pure-math unit tests (the 10 scenarios) | Create |
| `test/.../single/refinement/ScaleGainsSpec.scala` | Bias→gains mapping + override precedence | Create |
| `test/.../single/refinement/ExecutorTrendVitaminSpec.scala` | Adapter applies decisions to `RecipeConfig` + JSON round-trip | Create |
| `_AUTO_TUNING.md`, `_REFINEMENT.md`, `CLAUDE.md`, memory note | Docs | Modify |

---

## Task 1: `ScaleGains` + bias mapping (pure)

**Files:**
- Create: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala`
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ScaleGainsSpec.scala`

- [ ] **Step 1: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.{CostBiased, CostPerformanceBalance, PerformanceBiased}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ScaleGainsSpec extends AnyFunSuite with Matchers {

  test("bias presets order the up-gain Cost < Balance < Performance") {
    val cost = ScaleGains.fromBias(CostBiased)
    val bal = ScaleGains.fromBias(CostPerformanceBalance)
    val perf = ScaleGains.fromBias(PerformanceBiased)
    cost.gain should be < bal.gain
    bal.gain should be < perf.gain
    cost.maxStep should be < perf.maxStep
    // Cost is eager to shrink (smaller down deadband, larger down gain) than Performance.
    cost.deadbandDown should be < perf.deadbandDown
    cost.downGain should be > perf.downGain
  }

  test("shared defaults are present and sane") {
    val g = ScaleGains.fromBias(CostPerformanceBalance)
    g.deadbandUp shouldBe 0.10
    g.capTouchRatio shouldBe 0.5
    g.minRunsForConfidence shouldBe 5L
    g.minStep should (be > 0.0 and be <= 1.0)
    g.downscaleEnabled shouldBe true
  }

  test("CLI overrides take precedence over bias presets") {
    val g = ScaleGains.fromBias(
      CostPerformanceBalance,
      gainOverride = Some(0.9),
      minGainOverride = Some(0.8),
      deadbandUpOverride = Some(0.2),
      maxStepOverride = Some(3.0),
      minRunsOverride = Some(7L),
      downscaleEnabledOverride = Some(false)
    )
    g.gain shouldBe 0.9
    g.minGain shouldBe 0.8
    g.deadbandUp shouldBe 0.2
    g.maxStep shouldBe 3.0
    g.minRunsForConfidence shouldBe 7L
    g.downscaleEnabled shouldBe false
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `ScaleGainsSpec` in IntelliJ.
Expected: FAIL to compile — `ScaleGains` / `ScaleGains.fromBias` not defined.

- [ ] **Step 3: Write minimal implementation**

Create `ExecutorTrendScaler.scala` with the gains type only (the scaler object is added in Task 2):

```scala
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

  /** Per-bias (gain, minGain, maxStep, deadbandDown, downGain). */
  private def biasTuple(bias: BiasMode): (Double, Double, Double, Double, Double) = bias match {
    case CostBiased => (0.35, 0.15, 1.5, 0.05, 0.6)
    case PerformanceBiased => (0.70, 0.40, 2.5, 0.20, 0.25)
    case CostPerformanceBalance => (0.50, 0.25, 2.0, 0.10, 0.4)
    case _ => (0.50, 0.25, 2.0, 0.10, 0.4)
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
```

- [ ] **Step 4: Run test to verify it passes**

Run `ScaleGainsSpec` in IntelliJ.
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ScaleGainsSpec.scala
git commit -m "feat(tuner): add bias-derived ScaleGains for trend executor scaling"
```

---

## Task 2: `ExecutorTrendScaler.decide` (pure math — all directions)

This is the crux. The test is written first and covers every scenario; the implementation is one cohesive pure function.

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala`
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScalerSpec.scala`

- [ ] **Step 1: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.{CostPerformanceBalance, PerformanceBiased, RecipeMetrics}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorTrendScalerSpec extends AnyFunSuite with Matchers {

  private val balance = ScaleGains.fromBias(CostPerformanceBalance)

  /** Helper: a RecipeMetrics with the fields the scaler reads; others defaulted. */
  private def m(
      p95Dur: Double,
      avgDur: Double,
      p95Max: Double,
      runs: Long,
      fracCap: Option[Double] = None,
      avgExec: Double = 2.0
  ): RecipeMetrics =
    RecipeMetrics(
      cluster = "c",
      recipe = "r",
      avgExecutorsPerJob = avgExec,
      p95RunMaxExecutors = p95Max,
      avgJobDurationMs = avgDur,
      p95JobDurationMs = p95Dur,
      runs = runs,
      secondsAtCap = None,
      runsReachingCap = None,
      totalRuns = None,
      fractionReachingCap = fracCap,
      maxConcurrentJobs = None
    )

  private def decide(
      ref: RecipeMetrics,
      cur: RecipeMetrics,
      min: Int = 2,
      initial: Int = 2,
      max: Int = 3,
      gains: ScaleGains = balance,
      capacity: Option[Int] = Some(12),
      prior: Option[Double] = None,
      manual: Boolean = false
  ): TrendScaleDecision =
    ExecutorTrendScaler.decide("r", manual, min, initial, max, ref, cur, gains, capacity, prior)

  test("censoring case: capped + duration ~2x raises BOTH min and max") {
    // current pinned at max=3 (p95RunMax=3 -> capPressure=1.0); p95 doubled, avg up 1.8x.
    val ref = m(p95Dur = 100, avgDur = 90, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 200, avgDur = 162, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.direction shouldBe ScaleDirection.Up
    d.newMax should be > 3
    d.newMin should be > 2
    d.newMin should be <= d.newMax - 1
    d.state shouldBe BoostState.New
  }

  test("non-cap-touch slowdown HOLDS (not parallelism-bound)") {
    // duration doubled but only using 1 of 12 executors -> capPressure ~0.08 < 0.5.
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 1, runs = 20)
    val cur = m(p95Dur = 200, avgDur = 200, p95Max = 1, runs = 20)
    val d = decide(ref, cur, min = 2, max = 12)
    d.direction shouldBe ScaleDirection.Hold
    d.newMax shouldBe 12
    d.newMin shouldBe 2
  }

  test("fraction_reaching_cap alone satisfies the cap-pressure gate") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 1, runs = 20, fracCap = Some(0.9))
    val cur = m(p95Dur = 160, avgDur = 160, p95Max = 1, runs = 20, fracCap = Some(0.9))
    val d = decide(ref, cur, min = 2, max = 8)
    d.direction shouldBe ScaleDirection.Up
  }

  test("improvement with headroom shrinks conservatively, never below demand or floor 2") {
    // 40% faster, only 4 of 16 execs used -> low cap pressure, confident.
    val ref = m(p95Dur = 200, avgDur = 200, p95Max = 4, runs = 30)
    val cur = m(p95Dur = 120, avgDur = 120, p95Max = 4, runs = 30, avgExec = 3.0)
    val d = decide(ref, cur, min = 6, initial = 6, max = 16, capacity = Some(16))
    d.direction shouldBe ScaleDirection.Down
    d.newMax should be < 16
    d.newMax should be >= 4 // never below observed peak demand
    d.newMin should be >= 2
    d.newMin should be <= d.newMax - 1
  }

  test("deadband: small duration noise (+/-5%) holds") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 3, runs = 20)
    val curUp = m(p95Dur = 105, avgDur = 105, p95Max = 3, runs = 20)
    val curDn = m(p95Dur = 96, avgDur = 96, p95Max = 3, runs = 20)
    decide(ref, curUp, max = 3).direction shouldBe ScaleDirection.Hold
    decide(ref, curDn, min = 4, max = 8).direction shouldBe ScaleDirection.Hold
  }

  test("low confidence (few runs) yields no usable ratio -> Hold") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 3, runs = 2)
    val cur = m(p95Dur = 300, avgDur = 300, p95Max = 3, runs = 2)
    decide(ref, cur, max = 3).direction shouldBe ScaleDirection.Hold
  }

  test("performance bias produces a larger up-factor than balanced for identical input") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 4, runs = 20)
    val cur = m(p95Dur = 180, avgDur = 180, p95Max = 4, runs = 20)
    val dBal = decide(ref, cur, max = 4, capacity = Some(40))
    val dPerf = decide(ref, cur, max = 4, capacity = Some(40), gains = ScaleGains.fromBias(PerformanceBiased))
    dPerf.newMax should be > dBal.newMax
  }

  test("capacity clamp: never exceeds cluster capacity") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 5, runs = 20)
    val cur = m(p95Dur = 500, avgDur = 500, p95Max = 5, runs = 20)
    val d = decide(ref, cur, min = 2, max = 5, capacity = Some(6))
    d.newMax shouldBe 6
    d.newMin should be <= 5
  }

  test("per-run maxStep clamps a huge spike") {
    // durRatio ~5, balance maxStep=2.0 -> max at most ceil(5*2.0)=10 from base 5.
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 5, runs = 20)
    val cur = m(p95Dur = 500, avgDur = 500, p95Max = 5, runs = 20)
    val d = decide(ref, cur, min = 2, max = 5, capacity = Some(100))
    d.newMax should be <= 10
  }

  test("manual recipe scales spark.executor.instances proportionally, floor 2") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 4, runs = 20)
    val cur = m(p95Dur = 180, avgDur = 180, p95Max = 4, runs = 20)
    val d = decide(ref, cur, min = 4, initial = 4, max = 4, capacity = Some(20), manual = true)
    d.isManual shouldBe true
    d.newMax should be > 4
    d.newMin shouldBe d.newMax // manual: single instance count
  }

  test("ReBoost: prior tag + fresh up-signal compounds cumulative factor") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 4, runs = 20)
    val cur = m(p95Dur = 200, avgDur = 200, p95Max = 4, runs = 20)
    val d = decide(ref, cur, min = 3, max = 4, capacity = Some(40), prior = Some(1.5))
    d.state shouldBe BoostState.ReBoost
    d.cumulativeFactor should be > 1.5
  }

  test("Holding: prior tag but inside deadband leaves config unchanged") {
    val ref = m(p95Dur = 100, avgDur = 100, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 102, avgDur = 102, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 3, max = 6, prior = Some(1.5))
    d.direction shouldBe ScaleDirection.Hold
    d.state shouldBe BoostState.Holding
    d.newMax shouldBe 6
    d.cumulativeFactor shouldBe 1.5
  }

  test("idempotence: re-deciding on already-scaled config with no fresh delta holds") {
    val ref = m(p95Dur = 200, avgDur = 200, p95Max = 6, runs = 20)
    val cur = m(p95Dur = 200, avgDur = 200, p95Max = 6, runs = 20) // ratio 1.0
    val d = decide(ref, cur, min = 4, max = 8, prior = Some(2.0))
    d.direction shouldBe ScaleDirection.Hold
    d.newMax shouldBe 8
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `ExecutorTrendScalerSpec` in IntelliJ.
Expected: FAIL to compile — `ScaleDirection`, `TrendScaleDecision`, `ExecutorTrendScaler.decide` not defined.

- [ ] **Step 3: Write minimal implementation**

Append to `ExecutorTrendScaler.scala` (after the `ScaleGains` object):

```scala
import com.db.serna.orchestration.cluster_tuning.single.RecipeMetrics

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
      // Demand floors: never shrink below observed peak (max) or steady (min) demand + margin.
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
```

- [ ] **Step 4: Run test to verify it passes**

Run `ExecutorTrendScalerSpec` in IntelliJ.
Expected: PASS (14 tests). If the "improvement shrinks" test fails because `steadyDemand`/`peakDemand` dominate, confirm the chosen `min/max` inputs leave room below the demand floor — the test uses `max=16` with demand 4, so `newMax` lands between 5 and 15.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScalerSpec.scala
git commit -m "feat(tuner): pure ExecutorTrendScaler.decide (up/down/hold, min+max, manual, lifecycle)"
```

---

## Task 3: `SimpleJsonParser` round-trips `appliedTrendScaleFactor`

The carrier and pipeline read prior factors from `extraFields`; the parser must surface the new field.

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/SimpleJsonParser.scala:64-71`
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/SimpleJsonParserTrendFieldSpec.scala`

- [ ] **Step 1: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.single.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SimpleJsonParserTrendFieldSpec extends AnyFunSuite with Matchers {

  test("appliedTrendScaleFactor is parsed into extraFields") {
    val json =
      """{
        |  "clusterConf": { "c1": { "num_workers": 2, "cluster_max_total_cores": 96 } },
        |  "recipeSparkConf": {
        |    "_r.json": {
        |      "parallelizationFactor": 5,
        |      "appliedTrendScaleFactor": 1.3,
        |      "sparkOptsMap": {
        |        "spark.dynamicAllocation.enabled": "true",
        |        "spark.dynamicAllocation.minExecutors": "3",
        |        "spark.dynamicAllocation.maxExecutors": "5",
        |        "spark.dynamicAllocation.initialExecutors": "3",
        |        "spark.executor.cores": "8",
        |        "spark.executor.memory": "8g"
        |      },
        |      "total_executor_minimum_allocated_memory_gb": 24,
        |      "total_executor_maximum_allocated_memory_gb": 40
        |    }
        |  }
        |}""".stripMargin

    val cfg = SimpleJsonParser.parse(json)
    cfg.recipes("_r.json").extraFields.get("appliedTrendScaleFactor") shouldBe Some("1.3")
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `SimpleJsonParserTrendFieldSpec` in IntelliJ.
Expected: FAIL — `extraFields` does not contain `appliedTrendScaleFactor` (value is `None`).

- [ ] **Step 3: Write minimal implementation**

In `SimpleJsonParser.scala`, extend the `extractRecipes` block (currently `:64-71`):

```scala
        // Carry forward any previously-applied boost factors so the pipeline can
        // detect already-boosted recipes and avoid double-boosting on re-runs.
        val boostFactor = extractDoubleField(block, "appliedMemoryHeapBoostFactor")
        val scaleFactor = extractDoubleField(block, "appliedExecutorScaleFactor")
        val trendFactor = extractDoubleField(block, "appliedTrendScaleFactor")
        val extraFields: Map[String, String] =
          boostFactor.map("appliedMemoryHeapBoostFactor" -> _.toString).toMap ++
            scaleFactor.map("appliedExecutorScaleFactor" -> _.toString).toMap ++
            trendFactor.map("appliedTrendScaleFactor" -> _.toString).toMap
        key -> RecipeConfig(pf, sparkOpts, minMem, maxMem, extraFields)
```

- [ ] **Step 4: Run test to verify it passes**

Run `SimpleJsonParserTrendFieldSpec` in IntelliJ.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/SimpleJsonParser.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/SimpleJsonParserTrendFieldSpec.scala
git commit -m "feat(tuner): round-trip appliedTrendScaleFactor in SimpleJsonParser"
```

---

## Task 4: `ExecutorTrendVitamin` adapter (decision → JSON via the pipeline)

Plugs the pure decision into `RefinementPipeline` so the JSON read/write/order path is reused. Mirrors `ExecutorScaleVitamin`'s injected-signal pattern.

**Files:**
- Create: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendVitamin.scala`
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendVitaminSpec.scala`

- [ ] **Step 1: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.single.refinement

import com.db.serna.orchestration.cluster_tuning.single.{CostPerformanceBalance, RecipeMetrics}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorTrendVitaminSpec extends AnyFunSuite with Matchers {

  private def metrics(p95Dur: Double, p95Max: Double, runs: Long): RecipeMetrics =
    RecipeMetrics("c1", "_r.json", 2.0, p95Max, p95Dur * 0.9, p95Dur, runs, None, None, None, None, None)

  private def daRecipe(min: Int, max: Int): RecipeConfig =
    RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.dynamicAllocation.minExecutors" -> min.toString,
        "spark.dynamicAllocation.maxExecutors" -> max.toString,
        "spark.dynamicAllocation.initialExecutors" -> min.toString,
        "spark.executor.cores" -> "8",
        "spark.executor.memory" -> "8g"
      ),
      totalExecutorMinAllocatedMemoryGb = min * 8,
      totalExecutorMaxAllocatedMemoryGb = max * 8,
      extraFields = Map.empty
    )

  test("computeBoosts emits an up TrendScaleBoost for a censored degraded recipe and applyBoosts rewrites min/max") {
    val ref = metrics(p95Dur = 100, p95Max = 3, runs = 20)
    val cur = metrics(p95Dur = 200, p95Max = 3, runs = 20)
    val signal = TrendScaleSignal("c1", "_r.json", ref, cur, clusterMaxTotalCores = 96)
    val gains = ScaleGains.fromBias(CostPerformanceBalance)
    val vitamin = new ExecutorTrendVitamin(gains, _ => Seq(signal))

    val recipes = Map("_r.json" -> daRecipe(min = 2, max = 3))
    val boosts = vitamin.computeBoosts(Seq(signal), recipes)
    boosts should have size 1

    val applied = vitamin.applyBoosts(boosts, recipes)("_r.json")
    applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors").toInt should be > 3
    applied.sparkOptsMap("spark.dynamicAllocation.minExecutors").toInt should be > 2
    applied.totalExecutorMaxAllocatedMemoryGb shouldBe applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors").toInt * 8
    applied.extraFields should contain key "appliedTrendScaleFactor"
  }

  test("Hold decision produces no executor change but still stamps the carried factor when prior exists") {
    val ref = metrics(p95Dur = 100, p95Max = 3, runs = 20)
    val cur = metrics(p95Dur = 101, p95Max = 3, runs = 20) // inside deadband
    val signal = TrendScaleSignal("c1", "_r.json", ref, cur, clusterMaxTotalCores = 96)
    val vitamin = new ExecutorTrendVitamin(ScaleGains.fromBias(CostPerformanceBalance), _ => Seq(signal))
    val recipes = Map("_r.json" -> daRecipe(2, 6).copy(extraFields = Map("appliedTrendScaleFactor" -> "1.5")))

    val applied = vitamin.applyBoosts(vitamin.computeBoosts(Seq(signal), recipes), recipes)("_r.json")
    applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "6"
    applied.extraFields("appliedTrendScaleFactor") shouldBe "1.5"
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `ExecutorTrendVitaminSpec` in IntelliJ.
Expected: FAIL to compile — `TrendScaleSignal` / `TrendScaleBoost` / `ExecutorTrendVitamin` not defined.

- [ ] **Step 3: Write minimal implementation**

Create `ExecutorTrendVitamin.scala`:

```scala
package com.db.serna.orchestration.cluster_tuning.single.refinement

import java.io.File
import com.db.serna.orchestration.cluster_tuning.single.RecipeMetrics

// ── Trend signal & boost ─────────────────────────────────────────────────────

/**
 * Longitudinal signal — built in-memory by the AutoTuner from a paired (reference, current) recipe. Unlike the z-score
 * [[ExecutorScaleSignal]], this carries both metric snapshots so the pure [[ExecutorTrendScaler]] can compute the
 * duration trend directly.
 */
final case class TrendScaleSignal(
    clusterName: String,
    recipeFilename: String,
    reference: RecipeMetrics,
    current: RecipeMetrics,
    clusterMaxTotalCores: Int
) extends VitaminSignal {
  val jobId: String = ""
  val description: String = s"trend signal for $recipeFilename"
}

/** A trend-driven executor change (up, down, or carried hold). */
final case class TrendScaleBoost(
    recipeFilename: String,
    decision: TrendScaleDecision
) extends VitaminBoost {
  val description: String =
    s"trend ${decision.direction.label} [${decision.state.label}]: ${decision.reason}"
}

// ── Trend vitamin ────────────────────────────────────────────────────────────

/**
 * Adapter that runs [[ExecutorTrendScaler]] inside the [[RefinementPipeline]]. Reuses the JSON read/write/order and
 * dedupe machinery. Stamps `appliedTrendScaleFactor` — a field distinct from the z-score path's
 * `appliedExecutorScaleFactor` so the two mechanisms compose without lifecycle cross-talk.
 *
 * Capacity is derived per recipe from the signal's `clusterMaxTotalCores` divided by that recipe's
 * `spark.executor.cores`.
 */
class ExecutorTrendVitamin(
    val gains: ScaleGains,
    val signalsForCluster: String => Seq[TrendScaleSignal] = _ => Seq.empty
) extends RefinementVitamin {
  val name = "executor_trend_scale"
  val csvFileName = "(trend-driven, no CSV)"
  val counterKey = "trendScaledJobCount"
  val listKey = "trendScaledJobList"
  val boostFieldKey = "appliedTrendScaleFactor"

  def loadSignals(inputDir: File, clusterName: String): Seq[VitaminSignal] =
    signalsForCluster(clusterName)

  def computeBoosts(signals: Seq[VitaminSignal], recipes: Map[String, RecipeConfig]): Seq[VitaminBoost] = {
    val trendSignals = signals.collect { case s: TrendScaleSignal => s }
    trendSignals.flatMap { sig =>
      recipes.get(sig.recipeFilename).map { rc =>
        val (isManual, min, initial, max) = extractAllocation(rc)
        val execCores =
          rc.sparkOptsMap.get("spark.executor.cores").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(8)
        val capacity =
          if (sig.clusterMaxTotalCores > 0 && execCores > 0) Some(sig.clusterMaxTotalCores / execCores) else None
        val prior = rc.extraFields.get(boostFieldKey).flatMap(s => scala.util.Try(s.toDouble).toOption)
        val decision =
          ExecutorTrendScaler.decide(sig.recipeFilename, isManual, min, initial, max, sig.reference, sig.current, gains, capacity, prior)
        TrendScaleBoost(sig.recipeFilename, decision)
      }
    }
  }

  /** Trend decisions are self-contained (state computed by `decide`), so the date-aware path delegates. */
  override def computeBoosts(
      signals: Seq[VitaminSignal],
      recipes: Map[String, RecipeConfig],
      currentSignals: Seq[VitaminSignal]
  ): Seq[VitaminBoost] = computeBoosts(signals, recipes)

  def applyBoosts(boosts: Seq[VitaminBoost], recipes: Map[String, RecipeConfig]): Map[String, RecipeConfig] = {
    boosts.foldLeft(recipes) {
      case (cfg, TrendScaleBoost(recipe, d)) =>
        cfg.get(recipe) match {
          case Some(rc) =>
            val updatedExtra = rc.extraFields + (boostFieldKey -> d.cumulativeFactor.toString)
            if (!d.changed) {
              cfg.updated(recipe, rc.copy(extraFields = updatedExtra))
            } else {
              val memGb = SimpleJsonParser.parseMemoryGb(rc.sparkOptsMap.getOrElse("spark.executor.memory", "8g"))
              val updatedOpts =
                if (d.isManual) rc.sparkOptsMap.updated("spark.executor.instances", d.newMax.toString)
                else
                  rc.sparkOptsMap
                    .updated("spark.dynamicAllocation.minExecutors", d.newMin.toString)
                    .updated("spark.dynamicAllocation.maxExecutors", d.newMax.toString)
                    .updated("spark.dynamicAllocation.initialExecutors", d.newInitial.toString)
              cfg.updated(
                recipe,
                rc.copy(
                  sparkOptsMap = updatedOpts,
                  totalExecutorMinAllocatedMemoryGb = d.newMin * memGb,
                  totalExecutorMaxAllocatedMemoryGb = d.newMax * memGb,
                  extraFields = updatedExtra
                )
              )
            }
          case None => cfg
        }
      case (cfg, _) => cfg
    }
  }

  private def extractAllocation(rc: RecipeConfig): (Boolean, Int, Int, Int) = {
    val opts = rc.sparkOptsMap
    val isDynamic = opts.get("spark.dynamicAllocation.enabled").contains("true")
    def asInt(k: String, dflt: Int) = opts.get(k).flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(dflt)
    if (isDynamic) {
      val min = asInt("spark.dynamicAllocation.minExecutors", 2)
      val max = asInt("spark.dynamicAllocation.maxExecutors", min)
      val initial = asInt("spark.dynamicAllocation.initialExecutors", min)
      (false, min, initial, max)
    } else {
      val instances = asInt("spark.executor.instances", 2)
      (true, instances, instances, instances)
    }
  }
}
```

> Note: `SimpleJsonParser.parseMemoryGb` already exists (used by `ExecutorScaleVitamin.applyBoosts:528`). Confirm its signature is `parseMemoryGb(mem: String): Int`; if the visibility is `private[refinement]` it is callable from this same-package file.

- [ ] **Step 4: Run test to verify it passes**

Run `ExecutorTrendVitaminSpec` in IntelliJ.
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendVitamin.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendVitaminSpec.scala
git commit -m "feat(tuner): ExecutorTrendVitamin adapter into the refinement pipeline"
```

---

## Task 5: `BoostMetadataCarrier` carries min/initial + `appliedTrendScaleFactor`

When a `BoostResources`/`GenerateFresh` re-plan regenerates a recipe, prior trend-boosted counts and the cumulative factor must be injected into the fresh JSON before the trend vitamin runs again (same rationale as the existing b16 carry). Anchor on the recipe key.

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/BoostMetadataCarrier.scala`
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/BoostMetadataCarrierTrendSpec.scala`

- [ ] **Step 1: Read the current carrier to find the injection seam**

Run:
```bash
grep -n "appliedMemoryHeapBoostFactor\|appliedExecutorScaleFactor\|def \|recipeKey\|maxExecutors\|minExecutors" \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/BoostMetadataCarrier.scala
```
Expected: shows the method that injects prior `appliedMemoryHeapBoostFactor` + boosted `spark.executor.memory` + totals, keyed on the recipe block.

- [ ] **Step 2: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.auto

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BoostMetadataCarrierTrendSpec extends AnyFunSuite with Matchers {

  private val reference =
    """{
      |  "clusterConf": { "c1": { "num_workers": 4, "cluster_max_total_cores": 96 } },
      |  "recipeSparkConf": {
      |    "_r.json": {
      |      "parallelizationFactor": 5,
      |      "appliedTrendScaleFactor": 1.5,
      |      "sparkOptsMap": {
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "4",
      |        "spark.dynamicAllocation.maxExecutors": "6",
      |        "spark.dynamicAllocation.initialExecutors": "4",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 32,
      |      "total_executor_maximum_allocated_memory_gb": 48
      |    }
      |  }
      |}""".stripMargin

  private val fresh =
    """{
      |  "clusterConf": { "c1": { "num_workers": 4, "cluster_max_total_cores": 96 } },
      |  "recipeSparkConf": {
      |    "_r.json": {
      |      "parallelizationFactor": 5,
      |      "sparkOptsMap": {
      |        "spark.dynamicAllocation.enabled": "true",
      |        "spark.dynamicAllocation.minExecutors": "2",
      |        "spark.dynamicAllocation.maxExecutors": "3",
      |        "spark.dynamicAllocation.initialExecutors": "2",
      |        "spark.executor.cores": "8",
      |        "spark.executor.memory": "8g"
      |      },
      |      "total_executor_minimum_allocated_memory_gb": 16,
      |      "total_executor_maximum_allocated_memory_gb": 24
      |    }
      |  }
      |}""".stripMargin

  test("prior trend min/max/initial and factor are carried into a freshly re-planned recipe") {
    val merged = BoostMetadataCarrier.carryTrendMetadata(reference, fresh)
    merged should include("\"appliedTrendScaleFactor\"")
    merged should include("1.5")
    merged should include("\"spark.dynamicAllocation.maxExecutors\": \"6\"")
    merged should include("\"spark.dynamicAllocation.minExecutors\": \"4\"")
  }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run `BoostMetadataCarrierTrendSpec` in IntelliJ.
Expected: FAIL — `BoostMetadataCarrier.carryTrendMetadata` not defined.

- [ ] **Step 4: Write minimal implementation**

Add a `carryTrendMetadata(referenceJson, freshJson): String` method to `BoostMetadataCarrier`, mirroring the existing b16 carry but anchored on the recipe key and copying, when the reference recipe has `appliedTrendScaleFactor`:
- `spark.dynamicAllocation.minExecutors`, `maxExecutors`, `initialExecutors` (or `spark.executor.instances` for manual),
- `total_executor_minimum_allocated_memory_gb`, `total_executor_maximum_allocated_memory_gb`,
- the `appliedTrendScaleFactor` field itself.

Implement it by reusing the same `SimpleJsonParser` parse → mutate `RecipeConfig` → `RefinementPipeline.toRefinedJson` round-trip the existing carrier uses (read the file first in Step 1 and follow its exact pattern — if it does raw-JSON string injection keyed on the recipe block, match that approach instead so byte-identical sibling blocks don't collide, per the recipe-key anchor note in memory `project_boost_metadata_carrier_anchor.md`).

Concretely, parse both, and for each recipe present in both where the reference carries `appliedTrendScaleFactor`:
```scala
val refRc = refCfg.recipes(key)
val freshRc = freshCfg.recipes(key)
val carriedOpts = Seq(
  "spark.dynamicAllocation.minExecutors",
  "spark.dynamicAllocation.maxExecutors",
  "spark.dynamicAllocation.initialExecutors",
  "spark.executor.instances"
).foldLeft(freshRc.sparkOptsMap) { (opts, k) =>
  refRc.sparkOptsMap.get(k).map(v => opts.updated(k, v)).getOrElse(opts)
}
val merged = freshRc.copy(
  sparkOptsMap = carriedOpts,
  totalExecutorMinAllocatedMemoryGb = refRc.totalExecutorMinAllocatedMemoryGb,
  totalExecutorMaxAllocatedMemoryGb = refRc.totalExecutorMaxAllocatedMemoryGb,
  extraFields = freshRc.extraFields ++ refRc.extraFields.filterKeys(_ == "appliedTrendScaleFactor")
)
```
then rebuild via the same `RefinementResult`/`toRefinedJson` path the b16 carrier uses. If the existing carrier uses raw string injection, replicate that mechanism for parity (and add a code comment pointing to `project_boost_metadata_carrier_anchor.md`).

- [ ] **Step 5: Run test to verify it passes**

Run `BoostMetadataCarrierTrendSpec` in IntelliJ.
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/BoostMetadataCarrier.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/BoostMetadataCarrierTrendSpec.scala
git commit -m "feat(tuner): carry prior trend min/max/factor across re-plans"
```

---

## Task 6: AutoTuner CLI flags

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala` (Conf block, just after `scaleCapTouchRatio` at `:123`, before `verify()`)

- [ ] **Step 1: Add the flags**

Insert into the Scallop `Conf` block:
```scala
  val trendScaleGain: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Override the up-scale gain on maxExecutors (default: bias preset). Higher = more aggressive scale-up.",
    validate = g => g >= 0.0 && g <= 2.0
  )
  val trendMinGain: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Override the up-scale gain on minExecutors (default: bias preset, < trend-scale-gain).",
    validate = g => g >= 0.0 && g <= 2.0
  )
  val trendScaleDeadband: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Fractional duration increase required to trigger trend scale-up (default: 0.10).",
    validate = d => d >= 0.0 && d < 1.0
  )
  val trendScaleMaxStep: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Per-run multiplicative clamp on trend scale-up (default: bias preset).",
    validate = s => s >= 1.0 && s <= 5.0
  )
  val trendScaleMinRuns: ScallopOption[Long] = opt[Long](
    default = None,
    descr = "Minimum runs on each side for a usable duration ratio (default: 5).",
    validate = r => r >= 0L
  )
  val trendDownscaleEnabled: ScallopOption[Boolean] = toggle(
    default = Some(true),
    descrYes = "Enable conservative trend-driven scale-DOWN when jobs speed up (default: on).",
    descrNo = "Disable trend-driven scale-down (up-only)."
  )
```

- [ ] **Step 2: Verify it compiles via a no-op run**

Run `ClusterMachineAndRecipeAutoTuner` in IntelliJ with `--help` (or existing args). Expected: the new flags appear in usage; no runtime error. (No unit test — pure CLI wiring; behavior is covered in Task 7.)

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala
git commit -m "feat(tuner): add trend-scale CLI flags (back-compatible)"
```

---

## Task 7: AutoTuner wiring — build signals from `pairs`, apply before z-score

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala`

- [ ] **Step 1: Resolve gains + build trend signals from `pairs`**

After the strategy/policy are resolved (the `tuningStrategy`/`policy` block near `:289`), derive gains from the active bias and build per-cluster trend signals from the already-computed `pairs` and the current snapshot's cluster cores. Add near the existing `scaleSignalsByCluster` (`:243`):

```scala
    // Trend-scale gains derived from the active bias, with optional CLI overrides.
    val trendGains: ScaleGains = ScaleGains.fromBias(
      tuningStrategy.biasMode,
      gainOverride = conf.trendScaleGain.toOption,
      minGainOverride = conf.trendMinGain.toOption,
      maxStepOverride = conf.trendScaleMaxStep.toOption,
      deadbandUpOverride = conf.trendScaleDeadband.toOption,
      minRunsOverride = conf.trendScaleMinRuns.toOption,
      downscaleEnabledOverride = conf.trendDownscaleEnabled.toOption
    )

    // Longitudinal trend signals for paired (reference, current) recipes only.
    // clusterMaxTotalCores comes from the current snapshot's cluster sizing; the vitamin
    // converts it to an executor capacity using each recipe's spark.executor.cores.
    val trendSignalsByCluster: Map[String, Seq[TrendScaleSignal]] =
      pairs
        .map { p =>
          val cores = clusterMaxTotalCoresFor(curSnapshot, p.cluster)
          TrendScaleSignal(p.cluster, p.recipe, p.reference, p.current, cores)
        }
        .groupBy(_.clusterName)
```

Add the helper near the other private helpers (e.g. beside `familyOf` at `:1104`):
```scala
  /** Best-effort cluster executor-core capacity for a cluster in a snapshot; 0 when unknown. */
  private def clusterMaxTotalCoresFor(snapshot: DateSnapshot, cluster: String): Int =
    snapshot.metrics
      .collect { case ((c, _), m) if c == cluster => m }
      .headOption
      .map(_ => 0) // placeholder when metrics carry no core count
      .getOrElse(0)
```
> The metrics objects do not carry cluster cores. Prefer reading `cluster_max_total_cores` from the freshly-written `-auto-scale-tuned.json` at apply time (Step 2 does this), so this helper may simply return `0` and let the apply method derive capacity from the JSON `clusterConf`. Keep the signal field for forward-compatibility but treat `0` as "derive from JSON."

Add the required imports at the top of the file:
```scala
import com.db.serna.orchestration.cluster_tuning.single.refinement.{
  ExecutorTrendVitamin,
  ScaleGains,
  TrendScaleBoost,
  TrendScaleDecision,
  TrendScaleSignal
}
```

- [ ] **Step 2: Add `applyTrendScaling`, deriving capacity from the JSON `clusterConf`**

Add beside `applyExecutorScaling` (`:1044`):
```scala
  /**
   * Apply longitudinal trend-driven scaling to a cluster's tuned JSON (auto-scale and manual). Runs BEFORE the z-score
   * [[ExecutorScaleVitamin]] so the z-score path adds an extra boost on top of the trend baseline. Capacity is read
   * from the JSON `clusterConf.cluster_max_total_cores` divided by each recipe's `spark.executor.cores`.
   */
  private def applyTrendScaling(
      clusterName: String,
      outputDir: File,
      signals: Seq[TrendScaleSignal],
      gains: ScaleGains
  ): Seq[TrendScaleDecision] = {
    if (signals.isEmpty) return Seq.empty
    val fileNames = Seq(s"$clusterName-auto-scale-tuned.json", s"$clusterName-manually-tuned.json")
    val allDecisions = scala.collection.mutable.ArrayBuffer.empty[TrendScaleDecision]
    fileNames.foreach { fileName =>
      val file = new File(outputDir, fileName)
      if (file.exists()) {
        try {
          val config = SimpleJsonParser.parseFile(file)
          val clusterCores = config.clusterConfFields
            .find(_._1 == "cluster_max_total_cores")
            .flatMap { case (_, v) => scala.util.Try(v.toInt).toOption }
            .getOrElse(0)
          val enriched = signals.map(s => s.copy(clusterMaxTotalCores = clusterCores))
          val lookup: String => Seq[TrendScaleSignal] = c => if (c == clusterName) enriched else Seq.empty
          val vitamins: Seq[RefinementVitamin] = Seq(new ExecutorTrendVitamin(gains, lookup))
          val result = RefinementPipeline.refine(config, vitamins, Seq(outputDir))
          val decisions = result.appliedBoosts.collect { case b: TrendScaleBoost => b.decision }
          if (decisions.exists(_.changed)) {
            ClusterMachineAndRecipeTuner.writeFile(outputDir, fileName, RefinementPipeline.toRefinedJson(result))
          }
          allDecisions ++= decisions
          val up = decisions.count(_.direction == com.db.serna.orchestration.cluster_tuning.single.refinement.ScaleDirection.Up)
          val down = decisions.count(_.direction == com.db.serna.orchestration.cluster_tuning.single.refinement.ScaleDirection.Down)
          val hold = decisions.count(_.direction == com.db.serna.orchestration.cluster_tuning.single.refinement.ScaleDirection.Hold)
          if (decisions.nonEmpty)
            logger.info(s"  trend scaling on $fileName: $up up, $down down, $hold hold")
        } catch {
          case e: Exception => logger.warn(s"Failed trend scaling for $clusterName/$fileName: ${e.getMessage}")
        }
      }
    }
    allDecisions.toSeq
  }
```

- [ ] **Step 3: Call `applyTrendScaling` before each `applyExecutorScaling` site**

At BOTH call sites (`:408-418` KeepAsIs/Stable path and `:595-603` BoostResources/GenerateFresh path), insert immediately BEFORE the `if (executorScaleFactor > 1.0)` block:
```scala
            // Trend-driven scaling runs first; the z-score pass below adds an extra boost for outliers.
            val trendSignals = trendSignalsByCluster.getOrElse(clusterName, Seq.empty)
            val trendDecisions = applyTrendScaling(clusterName, curOutputDir, trendSignals, trendGains)
            if (trendDecisions.exists(_.changed)) {
              trendScaledRecipes += ((clusterName, trendDecisions.filter(_.changed)))
            }
```
For the `BoostResources | GenerateFresh` path, also invoke `BoostMetadataCarrier.carryTrendMetadata` on the freshly written JSON before this block (alongside the existing b16 carry), so prior trend counts survive the re-plan. Read the existing b16-carry call in that branch and place the trend carry next to it.

Declare the accumulator near `executorScaleBoostedRecipes` (`:322`):
```scala
    val trendScaledRecipes = ArrayBuffer.empty[(String, Seq[TrendScaleDecision])]
```

- [ ] **Step 4: Verify end-to-end on sample data**

Run `ClusterMachineAndRecipeAutoTuner` in IntelliJ with the existing sample dates:
`--reference-date=2099_01_01 --current-date=2099_01_02`
Expected: completes; log shows `trend scaling on … : N up, M down, K hold`; no exceptions. Inspect one rewritten `-auto-scale-tuned.json` for an updated `appliedTrendScaleFactor` where a paired recipe degraded under cap-pressure.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala
git commit -m "feat(tuner): wire trend scaling into AutoTuner before the z-score pass"
```

---

## Task 8: Summary `boost_groups` — direction + source

Surface trend scaling in `_generation_summary` / `_auto_tuner_analysis` so the frontend (separate follow-up) can render it.

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/GenerationSummary.scala` and/or `auto/AutoTunerJsonOutput.scala` (whichever emits `boost_groups`)
- Modify: `auto/ClusterMachineAndRecipeAutoTuner.scala` (`writeAutoTunerSummaryReport` to accept `trendScaledRecipes`)
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/AutoTunerSummaryTrendSpec.scala`

- [ ] **Step 1: Locate the `boost_groups` emitter**

Run:
```bash
grep -rn "boost_groups\|executor_scale\|count_new\|count_holding" \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/
```
Expected: pinpoints where the `executor_scale` group object is built.

- [ ] **Step 2: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.auto

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.db.serna.orchestration.cluster_tuning.single.refinement.{ScaleDirection, BoostState, TrendScaleDecision}

class AutoTunerSummaryTrendSpec extends AnyFunSuite with Matchers {

  test("trend boost group records code, source=trend, and direction counts") {
    val decisions = Seq(
      TrendScaleDecision("_a.json", isManual = false, 2, 2, 3, 3, 3, 5, 1.5, 1.5, ScaleDirection.Up, BoostState.New, "up"),
      TrendScaleDecision("_b.json", isManual = false, 8, 8, 12, 6, 6, 8, 0.7, 0.7, ScaleDirection.Down, BoostState.New, "down")
    )
    val group = AutoTunerJsonOutput.trendBoostGroup(Seq(("c1", decisions)))
    group should include("\"code\": \"executor_trend\"")
    group should include("\"source\": \"trend\"")
    group should include("\"count_up\"")
    group should include("\"count_down\"")
  }
}
```
> Adjust the call target (`AutoTunerJsonOutput.trendBoostGroup`) to match the actual emitter found in Step 1; if `boost_groups` is assembled inline in the AutoTuner, extract a small `private[auto]` helper there and test that instead.

- [ ] **Step 3: Run test to verify it fails**

Run `AutoTunerSummaryTrendSpec` in IntelliJ.
Expected: FAIL — helper not defined.

- [ ] **Step 4: Implement the trend boost group**

Add a `executor_trend` entry to `boost_groups` with fields `{code:"executor_trend", title:"Trend Executor Scaling", kind:"executor", source:"trend", count, count_up, count_down, count_holding, cluster_count, entries:[…]}`. Thread `trendScaledRecipes` from Task 7 into `writeAutoTunerSummaryReport` (new last param, defaulted to `Seq.empty` for back-compat) and into the analysis JSON writer.

- [ ] **Step 5: Run test to verify it passes**

Run `AutoTunerSummaryTrendSpec` in IntelliJ.
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/
git commit -m "feat(tuner): emit executor_trend boost group with direction/source"
```

---

## Task 9: OSS-mock `durationDrift` scenario + integration assertion

Give the `--full` chain a fixture that exercises the censoring case end-to-end.

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenarios.scala`, `MockGen.scala`
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenariosTrendSpec.scala`

> This task touches the OSS mock generator — follow the **oss-mock-data** skill for CSV shape/parity. Coordinate with that skill before editing `MockGen.scala`.

- [ ] **Step 1: Read the mock scenario shape**

Run:
```bash
grep -n "case object\|MockScenario\|def metrics\|p95\|maxExecutors\|baseline\|oomHeavy" \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenarios.scala | head -40
```

- [ ] **Step 2: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MockScenariosTrendSpec extends AnyFunSuite with Matchers {

  test("durationDrift scenario exists and produces a recipe whose current p95 duration exceeds reference at cap") {
    val scenario = MockScenarios.byName("durationDrift")
    scenario should not be empty
    // The reference recipe is pinned at its cap; the current snapshot keeps it pinned but slower.
    val ref = scenario.get.referenceRecipe("_DRIFT_DEMO.json")
    val cur = scenario.get.currentRecipe("_DRIFT_DEMO.json")
    cur.p95JobDurationMs should be > ref.p95JobDurationMs
    cur.p95RunMaxExecutors shouldBe ref.p95RunMaxExecutors // censored at the cap
  }
}
```
> Adapt method names (`byName`, `referenceRecipe`, `currentRecipe`) to the real `MockScenarios` API found in Step 1; the assertion intent — a capped recipe that slows down — is what matters.

- [ ] **Step 3: Run test to verify it fails**

Run `MockScenariosTrendSpec` in IntelliJ.
Expected: FAIL — `durationDrift` scenario unknown.

- [ ] **Step 4: Implement the scenario**

Add a `durationDrift` scenario: a small fleet with one DA recipe `_DRIFT_DEMO.json` pinned at `maxExecutors=3` whose current-date p95/avg duration is ~2× the reference while `p95_run_max_executors` stays at 3 (censored). Register it in `MockScenarios.byName` and ensure `MockGen` emits matching b13 rows for both dates (per the oss-mock-data skill's column parity rules).

- [ ] **Step 5: Run test to verify it passes**

Run `MockScenariosTrendSpec` in IntelliJ.
Expected: PASS.

- [ ] **Step 6: End-to-end `--full` smoke**

Run `OssMockMain` in IntelliJ:
`--scenario=durationDrift --reference-date=2099_03_01 --current-date=2099_03_02 --seed=42 --full`
Expected: generates inputs, runs SingleTuner → Refinement → AutoTuner; the AutoTuner log shows a trend `up` for `_DRIFT_DEMO.json`; the current `-auto-scale-tuned.json` shows `minExecutors`/`maxExecutors` above 2/3 and an `appliedTrendScaleFactor`.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/ \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenariosTrendSpec.scala
git commit -m "test(tuner): durationDrift mock scenario exercises the censoring case"
```

---

## Task 10: Documentation + memory

**Files:**
- Modify: `single/refinement/_REFINEMENT.md`, `auto/_AUTO_TUNING.md`, `CLAUDE.md`
- Modify: `/Users/serna/.claude/projects/-Users-serna-IdeaProjects-spark-cluster-job-tuner/memory/` (new feedback note + update `MEMORY.md` index and `feedback_executor_scale_defaults.md`)

- [ ] **Step 1: Document the trend scaler**

In `_REFINEMENT.md` add a "Trend-driven executor scaling" section: the censoring-trap rationale, the blended-duration driver, cap-pressure gate, min-toward-steady / max-toward-peak, conservative hysteresis down-scale, manual-instances handling, bias gains, the `appliedTrendScaleFactor` field, and composition with the preserved z-score path. In `_AUTO_TUNING.md` add the new CLI flags and the `executor_trend` boost group. In `CLAUDE.md` add one bullet under "Key design details" mirroring the existing z-score bullet.

- [ ] **Step 2: Update memory**

The earlier validated decision "minExecutors untouched" is now reversed for the trend path. Update `feedback_executor_scale_defaults.md` to note: *z-score path still leaves min untouched; the new trend path raises min toward steady demand (minGain < gain).* Add a new `project` note `project_trend_executor_scaling.md` summarizing the design + spec/plan paths, and add its index line to `MEMORY.md`. Link the notes with `[[…]]`.

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/_REFINEMENT.md \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/_AUTO_TUNING.md CLAUDE.md
git commit -m "docs(tuner): document trend-driven executor scaling"
```
(Memory files live outside the repo and are not committed.)

---

## Final verification (run before opening a PR)

- [ ] Run the full new spec suite in IntelliJ: `ScaleGainsSpec`, `ExecutorTrendScalerSpec`, `SimpleJsonParserTrendFieldSpec`, `ExecutorTrendVitaminSpec`, `BoostMetadataCarrierTrendSpec`, `AutoTunerSummaryTrendSpec`, `MockScenariosTrendSpec` — all green.
- [ ] Run the pre-existing tuner/refinement/auto specs — confirm no regressions (the z-score path, b16, carriers unchanged).
- [ ] Run `OssMockMain --scenario=durationDrift … --full` and visually confirm the `_DRIFT_DEMO.json` config rose proportionally.
- [ ] Run `OssMockMain --scenario=baseline … --full` and confirm stable recipes are unchanged (HOLD; no flapping).
- [ ] `git log --oneline` shows one commit per task.

---

## Self-review notes (author)

- **Spec coverage:** Components 1–5 of the spec map to Tasks 1–9; the two-field refinement (`appliedTrendScaleFactor` distinct from `appliedExecutorScaleFactor`) supersedes the spec's "single combined factor" wording to avoid lifecycle cross-talk — the combined effect is still observable as the product of the two fields. Update the spec's wiring note to match before merge.
- **Type consistency:** `ScaleGains`, `ScaleDirection`, `TrendScaleDecision`, `TrendScaleSignal`, `TrendScaleBoost`, `ExecutorTrendVitamin`, `ExecutorTrendScaler.decide`, `BoostMetadataCarrier.carryTrendMetadata`, `applyTrendScaling`, `trendBoostGroup` are referenced identically across tasks.
- **Placeholders:** Tasks 5, 8, 9 contain "adapt to the real API found in Step 1" guidance because the carrier/summary/mock internals must be read first; each provides the exact target shape and a concrete code sketch, not a bare TODO.
