# Trend Scaler v2 (Severity + Prioritization) & Dashboard Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the trend-driven executor scaler magnitude-aware (severity tiers + graduated evidence + min/initial creep + capacity-budgeted cluster prioritization) and ship six dashboard improvements (avg toggle, change icons, cluster-conf fixes, trend summary, recipe search, readable line charts).

**Architecture:** Track A rewrites the pure decision core in `ExecutorTrendScaler.scala` (new `ScaleSeverity` tiers keyed on BOTH duration ratio and absolute minutes lost; evidence rules replace the binary 5-run gate; a new pure `prioritize` allocates a per-cluster scale-up pool by impact), threads two new CLI flags, and enriches the `executor_trend` boost group. Track B is frontend-only (`auto/frontend/`): all data is already client-side.

**Tech Stack:** Scala 2.12.18 (no Spark needed for these specs), ScalaTest, Chart.js 4.4.7, vanilla JS/CSS.

**Spec:** `docs/superpowers/specs/2026-06-12-trend-scaler-v2-severity-and-dashboard-design.md`

---

## Preflight: CLI build & test loop (no IntelliJ)

`pom.xml` has no scala-maven-plugin. Rebuild the helper once per session:

```bash
cd /Users/serna/IdeaProjects/spark-cluster-job-tuner
"/Applications/IntelliJ IDEA CE.app/Contents/plugins/maven/lib/maven3/bin/mvn" \
  -q dependency:build-classpath -Dmdep.outputFile=/tmp/spark-tuner-cp.txt

cat > /tmp/tuner-test.sh <<'EOF'
#!/bin/bash
# usage: /tmp/tuner-test.sh <suite FQCN> <src1.scala> [src2.scala ...]
set -e
cd /Users/serna/IdeaProjects/spark-cluster-job-tuner
SCALA_LIB=~/.m2/repository/org/scala-lang/scala-library/2.12.18/scala-library-2.12.18.jar
SCALA_COMP=~/.m2/repository/org/scala-lang/scala-compiler/2.12.18/scala-compiler-2.12.18.jar
SCALA_REFL=~/.m2/repository/org/scala-lang/scala-reflect/2.12.18/scala-reflect-2.12.18.jar
DEPS=$(cat /tmp/spark-tuner-cp.txt)
SUITE=$1; shift
MAIN_SRCS=(); TEST_SRCS=()
for f in "$@"; do
  case "$f" in src/test/*) TEST_SRCS+=("$f");; *) MAIN_SRCS+=("$f");; esac
done
if [ ${#MAIN_SRCS[@]} -gt 0 ]; then
  java -cp "$SCALA_COMP:$SCALA_LIB:$SCALA_REFL" scala.tools.nsc.Main \
    -d target/classes -classpath "target/classes:$DEPS" "${MAIN_SRCS[@]}"
fi
if [ ${#TEST_SRCS[@]} -gt 0 ]; then
  java -cp "$SCALA_COMP:$SCALA_LIB:$SCALA_REFL" scala.tools.nsc.Main \
    -d target/test-classes -classpath "target/classes:target/test-classes:$DEPS" "${TEST_SRCS[@]}"
fi
java -cp "target/classes:target/test-classes:$SCALA_LIB:$DEPS" \
  org.scalatest.tools.Runner -R target/test-classes -s "$SUITE" -o
EOF
chmod +x /tmp/tuner-test.sh
```

When a MAIN file changes, every spec run that depends on it must recompile it first (pass it before the test sources). Compile order within one invocation: main sources, then test sources.

**Full cluster-tuning regression sweep** (used by several tasks):

```bash
for S in ExecutorTrendScalerSpec ExecutorTrendVitaminSpec ScaleGainsSpec ExecutorScaleVitaminSpec \
         RefinementVitaminsSpec CapacityGuardVitaminSpec SimpleJsonParserSpec SimpleJsonParserTrendFieldSpec \
         ClusterMachineAndRecipeTunerRefinementSpec; do
  /tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.single.refinement.$S \
    src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/$S.scala || exit 1
done
for S in TrendDetectorSpec StatisticalAnalysisSpec BoostMetadataCarrierSpec BoostMetadataCarrierTrendSpec \
         KeptRecipeCarrierSpec AutoTunerSummaryTrendSpec ClusterMachineAndRecipeAutoTunerSpec; do
  /tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.auto.$S \
    src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/$S.scala || exit 1
done
```

---

## File structure

**Track A (Scala):**
- Modify `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala` — `ScaleSeverity`, severity classification, graduated evidence, v2 `decide`, `prioritize`; `ScaleGains` gains `minDeltaMinutes`/`upPoolRatio`, loses `minGain`.
- Modify `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/RefinementVitamins.scala` — `ExecutorTrendVitamin.computeBoosts` becomes two-phase (decide → prioritize).
- Modify `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala` — CLI flags (`--trend-min-delta-minutes`, `--trend-up-pool-ratio`; drop `--trend-min-gain`), `trendGains` threading, `trendBoostGroup` severity fields.
- Modify `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenarios.scala` — minute-scale durations for `durationDrift`/`capacityPressure`, new `trendPriority`.
- New test `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendSeveritySpec.scala`.
- New test `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendPrioritizeSpec.scala`.
- Modify tests: `ExecutorTrendScalerSpec.scala`, `ExecutorTrendVitaminSpec.scala`, `ScaleGainsSpec.scala`, `AutoTunerSummaryTrendSpec.scala` (fixtures → minute scale, v2 expectations).

**Track B (frontend, `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/`):**
- Modify `app.js` (~5,259 lines), `dashboard.html`, `style.css`. No backend dependency; B-part reads `recipeSparkConf`, `deltas`, `_clusters-summary.csv` already loaded by the app.

---

# PHASE A — ExecutorTrendScaler v2

### Task 1: ScaleSeverity tiers + classification + ScaleGains v2

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala`
- Create: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendSeveritySpec.scala`
- Modify: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ScaleGainsSpec.scala`

- [ ] **Step 1: Write the failing severity tests**

Create `ExecutorTrendSeveritySpec.scala`:

```scala
package com.db.serna.orchestration.cluster_tuning.single.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorTrendSeveritySpec extends AnyFunSuite with Matchers {

  private val deadband = 0.10
  private val minDelta = 3.0

  private def cls(ratio: Double, deltaMin: Double): ScaleSeverity =
    ExecutorTrendScaler.classifySeverity(ratio, deltaMin, deadband, minDelta)

  test("below deadband is Negligible regardless of absolute delta") {
    cls(1.05, 120.0) shouldBe ScaleSeverity.Negligible
  }

  test("big ratio but tiny absolute delta is Negligible (20s -> 2m case)") {
    cls(6.0, 1.6) shouldBe ScaleSeverity.Negligible
  }

  test("ratio above deadband with >=3 min lost is Moderate") {
    cls(1.30, 4.0) shouldBe ScaleSeverity.Moderate
  }

  test("ratio >=2 with >=10 min lost is Severe") {
    cls(2.2, 12.0) shouldBe ScaleSeverity.Severe
  }

  test("ratio >=2 but under 10 min lost stays Moderate") {
    cls(2.5, 4.9) shouldBe ScaleSeverity.Moderate
  }

  test("ratio >=3 with >=30 min lost is Critical (the 1m36s -> 14m57s class scales)") {
    cls(9.3, 13.0) shouldBe ScaleSeverity.Severe // 13 min lost: Severe, not Critical (needs >=30)
    cls(9.3, 48.0) shouldBe ScaleSeverity.Critical
  }

  test("ratio >=3 but under 30 min lost demotes to Severe (if >=10) or Moderate") {
    cls(4.0, 15.0) shouldBe ScaleSeverity.Severe
    cls(4.0, 5.0) shouldBe ScaleSeverity.Moderate
  }

  test("graduated evidence: full runs admit any tier at full cap") {
    val g = ScaleGains.fromBias(com.db.serna.orchestration.cluster_tuning.single.CostPerformanceBalance)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Moderate, 5, g) shouldBe Some(g.maxStep)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Severe, 10, g) shouldBe Some(g.maxStep * 1.5)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Critical, 20, g) shouldBe Some(g.maxStep * 2.0)
  }

  test("graduated evidence: 2-4 runs admit only Severe+ and demote the cap one tier") {
    val g = ScaleGains.fromBias(com.db.serna.orchestration.cluster_tuning.single.CostPerformanceBalance)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Moderate, 3, g) shouldBe None
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Severe, 3, g) shouldBe Some(g.maxStep)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Critical, 4, g) shouldBe Some(g.maxStep * 1.5)
  }

  test("graduated evidence: 1 run admits only Critical, capped at 1.5") {
    val g = ScaleGains.fromBias(com.db.serna.orchestration.cluster_tuning.single.CostPerformanceBalance)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Severe, 1, g) shouldBe None
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Critical, 1, g) shouldBe Some(1.5)
  }

  test("graduated evidence: 0 runs admit nothing; Negligible admits nothing") {
    val g = ScaleGains.fromBias(com.db.serna.orchestration.cluster_tuning.single.CostPerformanceBalance)
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Critical, 0, g) shouldBe None
    ExecutorTrendScaler.admittedStepCap(ScaleSeverity.Negligible, 20, g) shouldBe None
  }
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.single.refinement.ExecutorTrendSeveritySpec \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendSeveritySpec.scala
```
Expected: COMPILE ERROR — `ScaleSeverity` / `classifySeverity` / `admittedStepCap` not found.

- [ ] **Step 3: Implement `ScaleSeverity`, `classifySeverity`, `admittedStepCap`, ScaleGains v2**

In `ExecutorTrendScaler.scala`:

(a) Replace the `ScaleGains` case class and companion (remove `minGain`, add `minDeltaMinutes` + `upPoolRatio`):

```scala
final case class ScaleGains(
    gain: Double,
    maxStep: Double,
    minStep: Double,
    deadbandUp: Double,
    deadbandDown: Double,
    downGain: Double,
    downSafetyMargin: Double,
    downConfidenceFloor: Double,
    capTouchRatio: Double,
    minRunsForConfidence: Long,
    downscaleEnabled: Boolean,
    minDeltaMinutes: Double,
    upPoolRatio: Double
)

object ScaleGains {

  private val DefaultDeadbandUp = 0.10
  private val DefaultMinStep = 0.5
  private val DefaultDownSafetyMargin = 0.15
  private val DefaultDownConfidenceFloor = 0.5
  private val DefaultCapTouchRatio = 0.5
  private val DefaultMinRuns = 5L
  private val DefaultMinDeltaMinutes = 3.0
  private val DefaultUpPoolRatio = 1.0

  /** Per-bias (gain, maxStep, deadbandDown, downGain). Exhaustive over the sealed BiasMode. */
  private def biasTuple(bias: BiasMode): (Double, Double, Double, Double) = bias match {
    case CostBiased => (0.35, 1.5, 0.05, 0.6)
    case PerformanceBiased => (0.70, 2.5, 0.20, 0.25)
    case CostPerformanceBalance => (0.50, 2.0, 0.10, 0.4)
  }

  def fromBias(
      bias: BiasMode,
      gainOverride: Option[Double] = None,
      maxStepOverride: Option[Double] = None,
      deadbandUpOverride: Option[Double] = None,
      minRunsOverride: Option[Long] = None,
      downscaleEnabledOverride: Option[Boolean] = None,
      minDeltaMinutesOverride: Option[Double] = None,
      upPoolRatioOverride: Option[Double] = None
  ): ScaleGains = {
    val (gain, maxStep, deadbandDown, downGain) = biasTuple(bias)
    ScaleGains(
      gain = gainOverride.getOrElse(gain),
      maxStep = maxStepOverride.getOrElse(maxStep),
      minStep = DefaultMinStep,
      deadbandUp = deadbandUpOverride.getOrElse(DefaultDeadbandUp),
      deadbandDown = deadbandDown,
      downGain = downGain,
      downSafetyMargin = DefaultDownSafetyMargin,
      downConfidenceFloor = DefaultDownConfidenceFloor,
      capTouchRatio = DefaultCapTouchRatio,
      minRunsForConfidence = minRunsOverride.getOrElse(DefaultMinRuns),
      downscaleEnabled = downscaleEnabledOverride.getOrElse(true),
      minDeltaMinutes = minDeltaMinutesOverride.getOrElse(DefaultMinDeltaMinutes),
      upPoolRatio = upPoolRatioOverride.getOrElse(DefaultUpPoolRatio)
    )
  }
}
```

NOTE: the `minGain`/`minGainOverride` removal breaks callers (`AutoTuner` line ~352, `ScaleGainsSpec`, possibly `MockScenarios`). Grep and fix in this task:
`grep -rn "minGain" src/main src/test` — remove the argument at each site (AutoTuner's `minGainOverride = conf.trendMinGain.toOption` line is deleted here; the Scallop flag itself is removed in Task 5).

(b) Add to the `ExecutorTrendScaler` object (above `decide`):

```scala
sealed trait ScaleSeverity { def label: String; def rank: Int }
object ScaleSeverity {
  case object Negligible extends ScaleSeverity { val label = "negligible"; val rank = 0 }
  case object Moderate extends ScaleSeverity { val label = "moderate"; val rank = 1 }
  case object Severe extends ScaleSeverity { val label = "severe"; val rank = 2 }
  case object Critical extends ScaleSeverity { val label = "critical"; val rank = 3 }
}
```

(place the sealed trait at top level of the file, next to `ScaleDirection`), and inside `object ExecutorTrendScaler`:

```scala
  // Severity thresholds: a tier needs BOTH the relative ratio and the absolute minutes lost.
  val SevereRatio: Double = 2.0
  val CriticalRatio: Double = 3.0
  val SevereDeltaMinutes: Double = 10.0
  val CriticalDeltaMinutes: Double = 30.0
  val SevereStepMultiplier: Double = 1.5
  val CriticalStepMultiplier: Double = 2.0
  val SingleRunStepCap: Double = 1.5
  val MinCreepPressure: Double = 0.8
  val ConfidenceFloor: Double = 0.5

  private[refinement] def classifySeverity(
      durRatio: Double,
      deltaMin: Double,
      deadbandUp: Double,
      minDeltaMinutes: Double
  ): ScaleSeverity =
    if (durRatio < 1.0 + deadbandUp || deltaMin < minDeltaMinutes) ScaleSeverity.Negligible
    else if (durRatio >= CriticalRatio && deltaMin >= CriticalDeltaMinutes) ScaleSeverity.Critical
    else if (durRatio >= SevereRatio && deltaMin >= SevereDeltaMinutes) ScaleSeverity.Severe
    else ScaleSeverity.Moderate

  /**
   * Graduated evidence: large effects need fewer observations. Returns the admitted per-run step cap,
   * or None when the signal is not admitted (insufficient evidence for its severity, or Negligible).
   */
  private[refinement] def admittedStepCap(tier: ScaleSeverity, runs: Long, gains: ScaleGains): Option[Double] = {
    def fullCap(t: ScaleSeverity): Double = t match {
      case ScaleSeverity.Critical => gains.maxStep * CriticalStepMultiplier
      case ScaleSeverity.Severe => gains.maxStep * SevereStepMultiplier
      case _ => gains.maxStep
    }
    if (tier == ScaleSeverity.Negligible) None
    else if (runs >= gains.minRunsForConfidence) Some(fullCap(tier))
    else if (runs >= 2) tier match {
      case ScaleSeverity.Critical => Some(fullCap(ScaleSeverity.Severe))
      case ScaleSeverity.Severe => Some(fullCap(ScaleSeverity.Moderate))
      case _ => None
    }
    else if (runs == 1 && tier == ScaleSeverity.Critical) Some(SingleRunStepCap)
    else None
  }
```

Do NOT touch `decide` yet — it still compiles because `minGain` was only read inside `decide`'s UP branch; replace that one read (`gains.minGain`) with `0.0`-equivalent TEMPORARILY by changing the line `val minFactor = 1.0 + gains.minGain * (durRatio - 1.0) * conf` to `val minFactor = 1.0` (Task 3 rewrites the whole body).

- [ ] **Step 4: Fix `ScaleGainsSpec` for the new shape**

Open `ScaleGainsSpec.scala`; update any construction/assertion referencing `minGain`/`minGainOverride` (delete those assertions or replace with `minDeltaMinutes`/`upPoolRatio` default assertions):

```scala
  test("defaults carry minDeltaMinutes=3.0 and upPoolRatio=1.0") {
    val g = ScaleGains.fromBias(CostPerformanceBalance)
    g.minDeltaMinutes shouldBe 3.0
    g.upPoolRatio shouldBe 1.0
  }
```

- [ ] **Step 5: Run severity + gains specs to verify they pass**

```bash
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.single.refinement.ExecutorTrendSeveritySpec \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/RefinementVitamins.scala \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendSeveritySpec.scala
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.single.refinement.ScaleGainsSpec \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ScaleGainsSpec.scala
```
Expected: PASS (both). If `MockScenarios.scala` or other files referenced `minGain`, compile them too and fix.

- [ ] **Step 6: Commit**

```bash
git add -A src/main/scala src/test/scala
git commit -m "feat(tuner): severity tiers + graduated evidence for trend scaling (ScaleSeverity, ScaleGains v2)"
```

---

### Task 2: `decide` v2 — magnitude-aware UP/DOWN with min/initial creep

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala` (the `decide` body, `TrendScaleDecision`, removal of `guardedRatio`/`blendedDurationRatio`)
- Modify: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScalerSpec.scala`

- [ ] **Step 1: Rewrite the spec for v2 semantics (failing first)**

Replace the body of `ExecutorTrendScalerSpec` tests (keep the `m(...)`/`decide(...)` helpers; durations move to **minute scale** so the absolute floor is exercised). Full new test list:

```scala
  // ── UP path ────────────────────────────────────────────────────────────────

  test("censoring case: capped + 10m->25m (Severe) raises max; min creeps at most +1") {
    val ref = m(p95Dur = 600000, avgDur = 540000, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 1500000, avgDur = 1350000, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.direction shouldBe ScaleDirection.Up
    d.severity shouldBe "severe"
    d.newMax should be > 3
    d.newMin shouldBe 3 // +1 creep (Severe + pressure 1.0 >= 0.8)
    d.newInitial should be <= 3 // initial follows, at most +1
    d.state shouldBe BoostState.New
  }

  test("the production case: 1m36s -> 14m57s over few runs is admitted and scales") {
    // ratio ~9.3, deltaMin ~13 -> Severe; 3 runs -> 2-4 band admits Severe (cap demoted to maxStep)
    val ref = m(p95Dur = 96000, avgDur = 90000, p95Max = 3, runs = 3)
    val cur = m(p95Dur = 897000, avgDur = 850000, p95Max = 3, runs = 3)
    val d = decide(ref, cur, min = 2, initial = 2, max = 3)
    d.direction shouldBe ScaleDirection.Up
    d.newMax should be > 3
  }

  test("big ratio but tiny absolute delta holds (20s -> 2m, even fully pinned)") {
    val ref = m(p95Dur = 20000, avgDur = 18000, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 120000, avgDur = 110000, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.direction shouldBe ScaleDirection.Hold
    d.severity shouldBe "negligible"
  }

  test("Critical degradation gets a larger step cap than Severe (balanced: x4 vs x3)") {
    // Critical: blend ratio ~6.03, deltaMin ~49. factor = 1 + 0.5·5.03·1.0 = 3.52 — under the x4
    // Critical cap (a Severe cap of x3 WOULD have clamped it) -> newMax = ceil(8·3.52) = 29.
    val refC = m(p95Dur = 600000, avgDur = 540000, p95Max = 8, runs = 20)
    val curC = m(p95Dur = 3600000, avgDur = 3300000, p95Max = 8, runs = 20)
    val dC = decide(refC, curC, min = 2, max = 8, capacity = Some(100))
    dC.severity shouldBe "critical"
    dC.newMax shouldBe 29
    dC.appliedFactor should be > balance.maxStep * ExecutorTrendScaler.SevereStepMultiplier
  }

  test("non-cap-touch slowdown HOLDS (not parallelism-bound)") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 1, runs = 20)
    val cur = m(p95Dur = 1500000, avgDur = 1500000, p95Max = 1, runs = 20)
    val d = decide(ref, cur, min = 2, max = 12)
    d.direction shouldBe ScaleDirection.Hold
  }

  test("fraction_reaching_cap alone satisfies the cap-pressure gate") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 1, runs = 20, fracCap = Some(0.9))
    val cur = m(p95Dur = 1200000, avgDur = 1200000, p95Max = 1, runs = 20, fracCap = Some(0.9))
    decide(ref, cur, min = 2, max = 8).direction shouldBe ScaleDirection.Up
  }

  test("Moderate tier under pressure 0.8 does not creep min") {
    // Moderate: ratio 1.5, deltaMin ~5; pressure: p95Max 2 of max 3 = 0.67 (>= capTouch 0.5, < 0.8)
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 2, runs = 20)
    val cur = m(p95Dur = 900000, avgDur = 900000, p95Max = 2, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.direction shouldBe ScaleDirection.Up
    d.severity shouldBe "moderate"
    d.newMin shouldBe 2 // unchanged
  }

  test("1 run admits only Critical, with conservative 1.5 step") {
    val refS = m(p95Dur = 600000, avgDur = 600000, p95Max = 3, runs = 1)
    val curS = m(p95Dur = 1500000, avgDur = 1500000, p95Max = 3, runs = 1) // Severe
    decide(refS, curS, max = 3).direction shouldBe ScaleDirection.Hold

    val curC = m(p95Dur = 3600000, avgDur = 3600000, p95Max = 3, runs = 1) // Critical
    val d = decide(refS, curC, min = 2, max = 4, capacity = Some(40))
    d.direction shouldBe ScaleDirection.Up
    d.newMax shouldBe 6 // ceil(4 * 1.5) capped by SingleRunStepCap
  }

  test("manual recipes scale instances toward newMax") {
    val ref = m(p95Dur = 600000, avgDur = 540000, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 1500000, avgDur = 1350000, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 3, initial = 3, max = 3, manual = true)
    d.direction shouldBe ScaleDirection.Up
    d.newMin shouldBe d.newMax
    d.newMax should be > 3
  }

  test("capacity clamp: never exceeds cluster capacity; UP never shrinks below current max") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 5, runs = 20)
    val cur = m(p95Dur = 3600000, avgDur = 3600000, p95Max = 5, runs = 20)
    decide(ref, cur, min = 2, max = 5, capacity = Some(6)).newMax shouldBe 6
    decide(ref, cur, min = 2, max = 5, capacity = Some(4)).newMax shouldBe 5
  }

  test("impact and severity are stamped on the decision") {
    val ref = m(p95Dur = 600000, avgDur = 540000, p95Max = 3, runs = 20)
    val cur = m(p95Dur = 1500000, avgDur = 1350000, p95Max = 3, runs = 20)
    val d = decide(ref, cur, min = 2, max = 3)
    d.impactMinutes shouldBe ((0.7 * 1500000 + 0.3 * 1350000) - (0.7 * 600000 + 0.3 * 540000)) / 60000.0 * 20 +- 0.01
    d.priorityRank shouldBe None
  }

  // ── DOWN path ──────────────────────────────────────────────────────────────

  test("improvement with headroom shrinks max; min shrinks at most 1") {
    val ref = m(p95Dur = 1200000, avgDur = 1200000, p95Max = 4, runs = 30)
    val cur = m(p95Dur = 720000, avgDur = 720000, p95Max = 4, runs = 30, avgExec = 3.0)
    val d = decide(ref, cur, min = 6, initial = 6, max = 16, capacity = Some(16))
    d.direction shouldBe ScaleDirection.Down
    d.newMax should be < 16
    d.newMax should be >= 4
    d.newMin should be >= 5 // 6 - 1 creep floor
    d.newInitial should be >= d.newMin
    d.newInitial should be <= d.newMax
  }

  test("small absolute saving does not downscale (3m floor symmetric)") {
    // 4m -> 2m: ratio 0.5 but only 2 minutes saved
    val ref = m(p95Dur = 240000, avgDur = 240000, p95Max = 4, runs = 30)
    val cur = m(p95Dur = 120000, avgDur = 120000, p95Max = 4, runs = 30)
    decide(ref, cur, min = 6, max = 16).direction shouldBe ScaleDirection.Hold
  }

  test("deadband: small duration noise holds both ways") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 3, runs = 20)
    decide(ref, m(p95Dur = 630000, avgDur = 630000, p95Max = 3, runs = 20), max = 3)
      .direction shouldBe ScaleDirection.Hold
    decide(ref, m(p95Dur = 576000, avgDur = 576000, p95Max = 3, runs = 20), min = 4, max = 8)
      .direction shouldBe ScaleDirection.Hold
  }

  test("prior factor carries on hold (Holding) and compounds on UP (ReBoost)") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 3, runs = 20)
    val curH = m(p95Dur = 620000, avgDur = 620000, p95Max = 3, runs = 20)
    val h = decide(ref, curH, max = 3, prior = Some(1.5))
    h.state shouldBe BoostState.Holding
    h.cumulativeFactor shouldBe 1.5

    val curU = m(p95Dur = 1500000, avgDur = 1350000, p95Max = 3, runs = 20)
    val u = decide(ref, curU, min = 2, max = 3, prior = Some(1.5))
    u.state shouldBe BoostState.ReBoost
    u.cumulativeFactor shouldBe (1.5 * u.appliedFactor) +- 1e-9
  }
```

Delete tests that asserted v1-only behavior: "low confidence (few runs) yields no usable ratio -> Hold" and "performance bias produces a larger up-factor" — replace the latter with:

```scala
  test("performance bias still produces a larger or equal up-step than balanced") {
    val ref = m(p95Dur = 600000, avgDur = 600000, p95Max = 4, runs = 20)
    val cur = m(p95Dur = 1100000, avgDur = 1100000, p95Max = 4, runs = 20)
    val dBal = decide(ref, cur, max = 4, capacity = Some(40))
    val dPerf = decide(ref, cur, max = 4, capacity = Some(40), gains = ScaleGains.fromBias(PerformanceBiased))
    dPerf.newMax should be >= dBal.newMax
  }
```

- [ ] **Step 2: Run to verify failure**

```bash
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.single.refinement.ExecutorTrendScalerSpec \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScalerSpec.scala
```
Expected: COMPILE ERROR (`severity`/`impactMinutes` not members) or FAIL.

- [ ] **Step 3: Implement decide v2**

In `ExecutorTrendScaler.scala`:

(a) Extend `TrendScaleDecision` (defaults keep old call sites compiling):

```scala
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
    reason: String,
    severity: String = "n/a",
    impactMinutes: Double = 0.0,
    priorityRank: Option[Int] = None
) {
  def changed: Boolean = newMin != originalMin || newInitial != originalInitial || newMax != originalMax
}
```

(b) Delete `guardedRatio` and `blendedDurationRatio` (grep for usages in tests; v2 spec no longer references them). Replace the whole `decide` body:

```scala
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

    val blendRef = DurationP95Weight * ref.p95JobDurationMs + DurationAvgWeight * ref.avgJobDurationMs
    val blendCur = DurationP95Weight * cur.p95JobDurationMs + DurationAvgWeight * cur.avgJobDurationMs
    val hasDur = blendRef > 0.0 && blendCur > 0.0
    val durRatio = if (hasDur) blendCur / blendRef else 1.0
    val deltaMin = if (hasDur) (blendCur - blendRef) / 60000.0 else 0.0
    val impactMin = math.max(0.0, deltaMin) * cur.runs
    val evidenceRuns = math.min(ref.runs, cur.runs)
    val tier =
      if (hasDur) classifySeverity(durRatio, deltaMin, gains.deadbandUp, gains.minDeltaMinutes)
      else ScaleSeverity.Negligible
    val stepCapOpt = admittedStepCap(tier, evidenceRuns, gains)
    val pressure = capPressure(cur, currentMax)
    val rampConf = math.min(1.0, evidenceRuns.toDouble / ConfidenceFullyRampedRuns)
    val prior = priorCumulativeFactor.getOrElse(1.0)
    val hasPrior = priorCumulativeFactor.isDefined

    val upTriggered = stepCapOpt.isDefined && pressure >= gains.capTouchRatio
    val downTriggered =
      gains.downscaleEnabled && hasDur &&
        durRatio <= 1.0 - gains.deadbandDown &&
        deltaMin <= -gains.minDeltaMinutes &&
        pressure < gains.capTouchRatio &&
        rampConf >= gains.downConfidenceFloor

    def diag: String =
      f"sev=${tier.label} durRatio=$durRatio%.2f dMin=$deltaMin%.1f impact=$impactMin%.0f " +
        f"pressure=$pressure%.2f runs=$evidenceRuns"

    def hold(reason: String): TrendScaleDecision =
      TrendScaleDecision(
        recipe, isManual,
        currentMin, currentInitial, currentMax,
        currentMin, currentInitial, currentMax,
        1.0, prior, ScaleDirection.Hold,
        if (hasPrior) BoostState.Holding else BoostState.New,
        reason, tier.label, impactMin, None
      )

    if (upTriggered) {
      val stepCap = stepCapOpt.get
      val conf = math.max(ConfidenceFloor, rampConf)
      val maxFactor = clampD(1.0 + gains.gain * (durRatio - 1.0) * conf, 1.0, stepCap)
      val rawMax = math.max(currentMax + 1, math.ceil(currentMax * maxFactor).toInt)
      // A tight capacity may cap rawMax, but an UP signal must never SHRINK below the current ceiling.
      val newMax = capacity.map(c => math.max(currentMax, math.min(rawMax, c))).getOrElse(rawMax)

      if (isManual) {
        val newInst = math.max(2, newMax)
        TrendScaleDecision(
          recipe, isManual = true,
          currentMin, currentInitial, currentMax,
          newInst, newInst, newInst,
          maxFactor, prior * maxFactor, ScaleDirection.Up,
          if (hasPrior) BoostState.ReBoost else BoostState.New,
          s"manual instances $currentMax->$newInst ($diag)", tier.label, impactMin, None
        )
      } else {
        // Gentle creep: the always-on floor rises at most +1 per run, and only when the
        // degradation is Severe+ and the job is essentially pinned at its ceiling.
        val newMin =
          if (tier.rank >= ScaleSeverity.Severe.rank && pressure >= MinCreepPressure)
            math.max(currentMin, math.min(currentMin + 1, math.max(2, newMax - 1)))
          else currentMin
        val newInitial = clampI(math.max(newMin, currentInitial), newMin, math.max(newMin, math.min(currentInitial + 1, newMax)))
        TrendScaleDecision(
          recipe, isManual = false,
          currentMin, currentInitial, currentMax,
          newMin, newInitial, newMax,
          maxFactor, prior * maxFactor, ScaleDirection.Up,
          if (hasPrior) BoostState.ReBoost else BoostState.New,
          s"min $currentMin->$newMin max $currentMax->$newMax ($diag)", tier.label, impactMin, None
        )
      }
    } else if (downTriggered) {
      val downFactor = clampD(1.0 - gains.downGain * (1.0 - durRatio) * rampConf, gains.minStep, 1.0)
      val peakDemand = math.ceil(cur.p95RunMaxExecutors * (1.0 + gains.downSafetyMargin)).toInt
      val steadyDemand = math.ceil(cur.avgExecutorsPerJob * (1.0 + gains.downSafetyMargin)).toInt
      val rawMax = math.ceil(currentMax * downFactor).toInt
      val newMax = math.max(2, math.max(peakDemand, rawMax))

      if (isManual) {
        val newInst = math.max(2, math.max(peakDemand, rawMax))
        val noChange = newInst == currentMax
        TrendScaleDecision(
          recipe, isManual = true,
          currentMin, currentInitial, currentMax,
          newInst, newInst, newInst,
          if (noChange) 1.0 else downFactor,
          if (noChange) prior else prior * downFactor,
          if (noChange) ScaleDirection.Hold else ScaleDirection.Down,
          if (noChange && hasPrior) BoostState.Holding else if (hasPrior) BoostState.ReBoost else BoostState.New,
          s"manual instances $currentMax->$newInst ($diag)", tier.label, impactMin, None
        )
      } else {
        // min shrinks at most 1 per run, never below steady demand or floor 2.
        val newMin = clampI(math.max(steadyDemand, currentMin - 1), 2, math.max(2, newMax - 1))
        val newInitial = clampI(math.max(newMin, math.min(currentInitial, newMax)), newMin, newMax)
        val noChange = newMax == currentMax && newMin == currentMin && newInitial == currentInitial
        TrendScaleDecision(
          recipe, isManual = false,
          currentMin, currentInitial, currentMax,
          newMin, newInitial, newMax,
          if (noChange) 1.0 else downFactor,
          if (noChange) prior else prior * downFactor,
          if (noChange) ScaleDirection.Hold else ScaleDirection.Down,
          if (noChange && hasPrior) BoostState.Holding else if (hasPrior) BoostState.ReBoost else BoostState.New,
          s"min $currentMin->$newMin max $currentMax->$newMax ($diag)", tier.label, impactMin, None
        )
      }
    } else {
      hold(s"within deadband or gate not met ($diag)")
    }
  }
```

Also update the Scaladoc above `ScaleGains` (drop the `minGain` bullet, add `minDeltaMinutes` and `upPoolRatio` bullets).

- [ ] **Step 4: Run the scaler spec + severity spec**

```bash
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.single.refinement.ExecutorTrendScalerSpec \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScalerSpec.scala
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.single.refinement.ExecutorTrendSeveritySpec \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendSeveritySpec.scala
```
Expected: PASS. Sanity-check the Critical case by hand: durRatio 6 → factor `1 + 0.5·5·1.0 = 3.5` < cap 4 → wait, the test asserts `newMax == 32` (8·4): blend ratio is `(0.7·3600000+0.3·3300000)/(0.7·600000+0.3·540000) = 3510000/582000 ≈ 6.03` → factor `1+0.5·5.03 = 3.52`, NOT capped → `ceil(8·3.52)=29`. **Fix the test assertion to `dC.newMax shouldBe 29`** (the cap only binds for even larger ratios); keep a second assertion `dC.appliedFactor should be <= balance.maxStep * 2.0`.

- [ ] **Step 5: Commit**

```bash
git add -A src/main/scala src/test/scala
git commit -m "feat(tuner): magnitude-aware decide() — severity-tiered UP, ±1 min/initial creep, 3-minute absolute floors"
```

---

### Task 3: `prioritize` — capacity-budgeted, impact-ranked grants

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala`
- Create: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendPrioritizeSpec.scala`

- [ ] **Step 1: Write the failing prioritize tests**

```scala
package com.db.serna.orchestration.cluster_tuning.single.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExecutorTrendPrioritizeSpec extends AnyFunSuite with Matchers {

  private def up(recipe: String, origMax: Int, newMax: Int, impact: Double, manual: Boolean = false): TrendScaleDecision =
    TrendScaleDecision(
      recipe, manual, 2, 2, origMax,
      if (manual) newMax else 2, if (manual) newMax else 2, newMax,
      newMax.toDouble / origMax, newMax.toDouble / origMax,
      ScaleDirection.Up, BoostState.New, "up", "severe", impact, None
    )

  private def hold(recipe: String): TrendScaleDecision =
    TrendScaleDecision(recipe, isManual = false, 2, 2, 4, 2, 2, 4, 1.0, 1.0,
      ScaleDirection.Hold, BoostState.New, "hold", "negligible", 0.0, None)

  private def rc(d: TrendScaleDecision, cores: Int = 8): ExecutorTrendScaler.RecipeCores =
    ExecutorTrendScaler.RecipeCores(d, cores)

  test("plentiful pool: all grants pass through with priority ranks by impact desc") {
    val a = up("a.json", 4, 8, impact = 600.0)
    val b = up("b.json", 4, 6, impact = 50.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(b), rc(a)), clusterMaxTotalCores = 1000, poolRatio = 1.0)
    out.map(_.recipe) shouldBe Seq("b.json", "a.json") // input order preserved
    out.find(_.recipe == "a.json").get.priorityRank shouldBe Some(1)
    out.find(_.recipe == "b.json").get.priorityRank shouldBe Some(2)
    out.find(_.recipe == "a.json").get.newMax shouldBe 8
  }

  test("pool exhaustion: highest impact gets full grant, the rest degrade to +1 (never zero)") {
    // pool = 64 cores; a wants (12-4)*8=64 -> takes all; b degrades to originalMax+1
    val a = up("a.json", 4, 12, impact = 600.0)
    val b = up("b.json", 4, 10, impact = 50.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a), rc(b)), clusterMaxTotalCores = 64, poolRatio = 1.0)
    out.find(_.recipe == "a.json").get.newMax shouldBe 12
    val db = out.find(_.recipe == "b.json").get
    db.newMax shouldBe 5
    db.appliedFactor shouldBe 1.25 +- 1e-9
    db.cumulativeFactor shouldBe 1.25 +- 1e-9
    db.reason should include("pool-exhausted")
  }

  test("degraded manual grant keeps min==initial==max") {
    val a = up("a.json", 4, 12, impact = 600.0)
    val mB = up("b.json", 4, 10, impact = 50.0, manual = true)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a), rc(mB)), clusterMaxTotalCores = 64, poolRatio = 1.0)
    val db = out.find(_.recipe == "b.json").get
    db.newMax shouldBe 5
    db.newMin shouldBe 5
    db.newInitial shouldBe 5
  }

  test("holds and downs pass through untouched, no rank") {
    val h = hold("h.json")
    val out = ExecutorTrendScaler.prioritize(Seq(rc(h)), clusterMaxTotalCores = 64, poolRatio = 1.0)
    out.head shouldBe h
  }

  test("no capacity info (clusterMaxTotalCores <= 0) passes everything through") {
    val a = up("a.json", 4, 12, impact = 600.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a)), clusterMaxTotalCores = 0, poolRatio = 1.0)
    out.head.newMax shouldBe 12
    out.head.priorityRank shouldBe None
  }

  test("deterministic tie-break on equal impact: recipe name") {
    val a = up("zz.json", 4, 8, impact = 100.0)
    val b = up("aa.json", 4, 8, impact = 100.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a), rc(b)), clusterMaxTotalCores = 1000, poolRatio = 1.0)
    out.find(_.recipe == "aa.json").get.priorityRank shouldBe Some(1)
    out.find(_.recipe == "zz.json").get.priorityRank shouldBe Some(2)
  }

  test("cumulative factor of a degraded grant preserves the prior compound") {
    val prior = 1.5
    val d = up("b.json", 4, 10, impact = 50.0).copy(appliedFactor = 2.5, cumulativeFactor = prior * 2.5)
    val a = up("a.json", 4, 12, impact = 600.0)
    val out = ExecutorTrendScaler.prioritize(Seq(rc(a), rc(d)), clusterMaxTotalCores = 64, poolRatio = 1.0)
    val db = out.find(_.recipe == "b.json").get
    db.cumulativeFactor shouldBe (prior * 1.25) +- 1e-9
  }
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.single.refinement.ExecutorTrendPrioritizeSpec \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendPrioritizeSpec.scala
```
Expected: COMPILE ERROR — `RecipeCores` / `prioritize` not found.

- [ ] **Step 3: Implement prioritize**

Add to `object ExecutorTrendScaler`:

```scala
  /** Pairs a decision with its recipe's spark.executor.cores so the pool can be budgeted in CORES. */
  final case class RecipeCores(decision: TrendScaleDecision, execCores: Int)

  /**
   * Capacity-budgeted prioritization across one cluster's trend decisions. UP grants draw, in
   * impact order (total minutes lost per window), from a per-run pool of `poolRatio × clusterMaxTotalCores`
   * cores; each grant consumes (newMax − originalMax) × execCores. When the pool runs dry, remaining
   * UP candidates are degraded to originalMax + 1 — reduced, never starved (every admitted signal
   * still moves). Jobs are staggered in time, so this is intentionally a growth-rate bound, not a
   * concurrency bound; CapacityGuard stays the physical per-recipe clamp afterwards.
   * Holds/Downs pass through untouched. Input order is preserved.
   */
  def prioritize(inputs: Seq[RecipeCores], clusterMaxTotalCores: Int, poolRatio: Double): Seq[TrendScaleDecision] = {
    if (clusterMaxTotalCores <= 0) return inputs.map(_.decision)
    var pool = math.max(0, math.ceil(clusterMaxTotalCores * poolRatio).toInt)
    val isUpGrant: TrendScaleDecision => Boolean = d => d.direction == ScaleDirection.Up && d.newMax > d.originalMax
    val ranked = inputs
      .filter(i => isUpGrant(i.decision))
      .sortBy(i => (-i.decision.impactMinutes, -i.decision.appliedFactor, i.decision.recipe))
    val adjusted = scala.collection.mutable.Map.empty[String, TrendScaleDecision]
    ranked.zipWithIndex.foreach { case (RecipeCores(d, ec), idx) =>
      val cores = math.max(1, ec)
      val wantCores = (d.newMax - d.originalMax) * cores
      val granted =
        if (wantCores <= pool) {
          pool -= wantCores
          d.copy(priorityRank = Some(idx + 1))
        } else {
          pool = math.max(0, pool - cores)
          val degradedMax = d.originalMax + 1
          val newFactor = degradedMax.toDouble / math.max(1, d.originalMax)
          val priorCum = d.cumulativeFactor / d.appliedFactor
          val annotated = d.reason + s" [pool-exhausted: granted +1 of +${d.newMax - d.originalMax}]"
          if (d.isManual)
            d.copy(newMin = degradedMax, newInitial = degradedMax, newMax = degradedMax,
              appliedFactor = newFactor, cumulativeFactor = priorCum * newFactor,
              priorityRank = Some(idx + 1), reason = annotated)
          else {
            val dMin = math.min(d.newMin, math.max(2, degradedMax - 1))
            val dInit = math.max(dMin, math.min(d.newInitial, degradedMax))
            d.copy(newMin = dMin, newInitial = dInit, newMax = degradedMax,
              appliedFactor = newFactor, cumulativeFactor = priorCum * newFactor,
              priorityRank = Some(idx + 1), reason = annotated)
          }
        }
      adjusted(d.recipe) = granted
    }
    inputs.map(i => adjusted.getOrElse(i.decision.recipe, i.decision))
  }
```

- [ ] **Step 4: Run to verify pass**

Same command as Step 2 (compile the main file too). Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src/main/scala src/test/scala
git commit -m "feat(tuner): capacity-budgeted impact-ranked prioritization for trend scale-up grants"
```

---

### Task 4: Two-phase `ExecutorTrendVitamin` (decide → prioritize)

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/RefinementVitamins.scala` (the `computeBoosts` at ~line 610)
- Modify: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendVitaminSpec.scala`

- [ ] **Step 1: Add a failing vitamin-level prioritization test**

Read `ExecutorTrendVitaminSpec.scala` first and reuse its existing `RecipeConfig`/signal builders. Add (adapting builder names to what the file actually uses — keep durations minute-scale):

```scala
  test("computeBoosts prioritizes UP grants across the cluster: pool-exhausted recipe degrades to +1") {
    // Two dynamic recipes, 8 cores each, cluster has 64 schedulable cores. The per-recipe capacity
    // clamp (64/8 = 8 executors) bounds BIG's grant to +4 = 32 cores, so with the default pool of
    // 64 cores nothing would ever exhaust — use upPoolRatio = 0.5 (pool = 32 cores): BIG (higher
    // impact, Severe) drains it entirely; SMALL (Moderate, wants ceil(4·1.4)=6) must still move +1.
    val gains = ScaleGains.fromBias(CostPerformanceBalance, upPoolRatioOverride = Some(0.5))
    val refBig = metrics(p95Dur = 600000, avgDur = 540000, p95Max = 4, runs = 20, fracCap = Some(0.9))
    val curBig = metrics(p95Dur = 2400000, avgDur = 2200000, p95Max = 4, runs = 20, fracCap = Some(0.9))
    val refSmall = metrics(p95Dur = 600000, avgDur = 600000, p95Max = 4, runs = 20, fracCap = Some(0.9))
    val curSmall = metrics(p95Dur = 1080000, avgDur = 1080000, p95Max = 4, runs = 20, fracCap = Some(0.9))
    val signals = Seq(
      TrendScaleSignal("c", "_BIG.json", refBig, curBig, clusterMaxTotalCores = 64),
      TrendScaleSignal("c", "_SMALL.json", refSmall, curSmall, clusterMaxTotalCores = 64)
    )
    val recipes = Map(
      "_BIG.json" -> dynRecipe(min = 2, initial = 2, max = 4),
      "_SMALL.json" -> dynRecipe(min = 2, initial = 2, max = 4)
    )
    val vitamin = new ExecutorTrendVitamin(gains, _ => signals)
    val boosts = vitamin.computeBoosts(signals, recipes).collect { case b: TrendScaleBoost => b }
    val big = boosts.find(_.recipeFilename == "_BIG.json").get.decision
    val small = boosts.find(_.recipeFilename == "_SMALL.json").get.decision
    big.priorityRank shouldBe Some(1)
    big.newMax shouldBe 8 // capacity-clamped full grant: +4 executors × 8 cores = the whole 32-core pool
    small.newMax shouldBe 5 // degraded to +1, not zero
    small.reason should include("pool-exhausted")
  }
```

(If the spec has no `metrics`/`dynRecipe` helpers, define them locally in the test mirroring the existing tests' construction of `RecipeMetrics` and `RecipeConfig`.)

- [ ] **Step 2: Run to verify it fails**

```bash
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.single.refinement.ExecutorTrendVitaminSpec \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendScaler.scala \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/RefinementVitamins.scala \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/ExecutorTrendVitaminSpec.scala
```
Expected: FAIL — both recipes get full grants (no pool yet). Pre-existing tests with ms-scale durations may also fail — bump their durations ×600 (e.g. 100000 → 600000, 200000 → 1500000) so paired UP cases are Severe and pass the 3-minute floor; adjust exact-value assertions per v2 (min creeps +1, not proportional).

- [ ] **Step 3: Implement the two-phase computeBoosts**

Replace `ExecutorTrendVitamin.computeBoosts(signals, recipes)`:

```scala
  def computeBoosts(signals: Seq[VitaminSignal], recipes: Map[String, RecipeConfig]): Seq[VitaminBoost] = {
    val trendSignals = signals.collect { case s: TrendScaleSignal => s }

    // Phase 1: independent per-recipe decisions.
    val perRecipe: Seq[(TrendScaleSignal, RecipeConfig, TrendScaleDecision, Int)] = trendSignals.flatMap { sig =>
      recipes.get(sig.recipeFilename).map { rc =>
        val (isManual, min, initial, max) = extractAllocation(rc)
        val execCores =
          rc.sparkOptsMap.get("spark.executor.cores").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(8)
        val capacity =
          if (sig.clusterMaxTotalCores > 0 && execCores > 0) Some(sig.clusterMaxTotalCores / execCores) else None
        val prior = rc.extraFields.get(boostFieldKey).flatMap(s => scala.util.Try(s.toDouble).toOption)
        val decision =
          ExecutorTrendScaler.decide(sig.recipeFilename, isManual, min, initial, max, sig.reference, sig.current,
            gains, capacity, prior)
        (sig, rc, decision, execCores)
      }
    }

    // Phase 2: cluster-wide capacity-budgeted prioritization of the UP grants.
    val clusterCores = trendSignals.headOption.map(_.clusterMaxTotalCores).getOrElse(0)
    val prioritized = ExecutorTrendScaler.prioritize(
      perRecipe.map { case (_, _, d, ec) => ExecutorTrendScaler.RecipeCores(d, ec) },
      clusterCores,
      gains.upPoolRatio
    )

    perRecipe.zip(prioritized).flatMap { case ((sig, rc, _, _), d) =>
      val prior = rc.extraFields.get(boostFieldKey).flatMap(s => scala.util.Try(s.toDouble).toOption)
      if (d.changed || prior.isDefined) Some(TrendScaleBoost(sig.recipeFilename, d)) else None
    }
  }
```

- [ ] **Step 4: Run the vitamin spec (and full refinement sweep) to verify pass**

Run the Step-2 command, then the full refinement sweep from Preflight. Expected: PASS everywhere (fix remaining ms-scale fixtures encountered, same ×600 guidance).

- [ ] **Step 5: Commit**

```bash
git add -A src/main/scala src/test/scala
git commit -m "feat(tuner): ExecutorTrendVitamin two-phase compute — per-recipe decide, cluster-wide prioritize"
```

---

### Task 5: CLI flags + boost-group severity fields (AutoTuner wiring)

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala`
- Modify: `src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/AutoTunerSummaryTrendSpec.scala`

- [ ] **Step 1: Add a failing boost-group test**

In `AutoTunerSummaryTrendSpec` (reuse its existing decision builders; add the new fields):

```scala
  test("trendBoostGroup carries severity, impact_minutes and priority_rank per recipe") {
    val d = TrendScaleDecision(
      "_X.json", isManual = false, 2, 2, 3, 3, 3, 6, 2.0, 2.0,
      ScaleDirection.Up, BoostState.New, "up", "severe", 123.4, Some(1)
    )
    val json = ClusterMachineAndRecipeAutoTuner.trendBoostGroup(Seq(("c1", Seq(d))))
    json should include(""""severity":"severe"""")
    json should include(""""impact_minutes":123.4""")
    json should include(""""priority_rank":1""")
  }
```

- [ ] **Step 2: Run to verify failure**

```bash
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.auto.AutoTunerSummaryTrendSpec \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/AutoTunerSummaryTrendSpec.scala
```
Expected: FAIL — fields missing from the JSON.

- [ ] **Step 3: Implement CLI + boost group changes**

(a) In `AutoTunerConf` (~line 150): DELETE the `trendMinGain` option block; ADD after `trendDownscaleEnabled`:

```scala
  val trendMinDeltaMinutes: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Absolute blended-duration increase (minutes) below which a degradation is Negligible " +
      "for trend scaling — seconds-level jitter never scales (default: 3.0).",
    validate = m => m >= 0.0
  )
  val trendUpPoolRatio: ScallopOption[Double] = opt[Double](
    default = None,
    descr = "Per-run trend scale-UP pool as a fraction of the cluster's schedulable cores; " +
      "highest-impact recipes draw first, the rest degrade to +1 executor (default: 1.0).",
    validate = r => r > 0.0 && r <= 4.0
  )
```

(b) In `run()` (~line 352), the `trendGains` construction becomes:

```scala
    val trendGains: ScaleGains = ScaleGains.fromBias(
      tuningStrategy.biasMode,
      gainOverride = conf.trendScaleGain.toOption,
      maxStepOverride = conf.trendScaleMaxStep.toOption,
      deadbandUpOverride = conf.trendScaleDeadband.toOption,
      minRunsOverride = conf.trendScaleMinRuns.toOption,
      downscaleEnabledOverride = conf.trendDownscaleEnabled.toOption,
      minDeltaMinutesOverride = conf.trendMinDeltaMinutes.toOption,
      upPoolRatioOverride = conf.trendUpPoolRatio.toOption
    )
```

(c) In `trendBoostGroup` (~line 1578), inside the per-recipe string, after the `direction` field add:

```scala
            s"{${q("recipe")}:${q(recipe)}," +
              s"${q("recipe_filename")}:${q(d.recipe)}," +
              s"${q("state")}:${q(d.state.label)}," +
              s"${q("direction")}:${q(d.direction.label)}," +
              s"${q("severity")}:${q(d.severity)}," +
              s"${q("impact_minutes")}:${"%.1f".format(d.impactMinutes)}," +
              d.priorityRank.map(r => s"${q("priority_rank")}:$r,").getOrElse("") +
              s"${q("manual")}:${d.isManual}," +
              s"${q("propagated")}:$propagated," +
              s"$minObj,$maxObj}"
```

- [ ] **Step 4: Run summary spec + AutoTuner spec**

```bash
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.auto.AutoTunerSummaryTrendSpec \
  src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/AutoTunerSummaryTrendSpec.scala
/tmp/tuner-test.sh com.db.serna.orchestration.cluster_tuning.auto.ClusterMachineAndRecipeAutoTunerSpec \
  src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTunerSpec.scala
```
Expected: PASS (fix any ms-scale fixture in the AutoTuner spec the same ×600 way).

- [ ] **Step 5: Commit**

```bash
git add -A src/main/scala src/test/scala
git commit -m "feat(tuner): --trend-min-delta-minutes / --trend-up-pool-ratio flags; severity+impact+rank in executor_trend boost group (drops --trend-min-gain)"
```

---

### Task 6: Mock scenarios — minute-scale drift fixtures + `trendPriority` + e2e verify

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenarios.scala`
- Check/modify: `src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/*` (scenario specs)
- Regenerate: `src/main/resources/composer/dwh/config/cluster_tuning/{inputs,outputs}/2099_04_01`, `2099_04_02` (capacityPressure samples), plus any committed durationDrift dates

- [ ] **Step 1: Bump durationDrift/capacityPressure durations to minute scale**

In `MockScenarios.scala` (~line 870/888): `durationDrift` ref recipe `p95DurMs = 600000.0, avgDurMs = 540000.0`; cur recipe `p95DurMs = 1500000.0, avgDurMs = 1350000.0` (10m→25m blended ≈ Severe). In `capacityPressure` (~line 947/953): ref `p95DurMs = 1200000.0, avgDurMs = 1080000.0`; cur `p95DurMs = 3000000.0, avgDurMs = 2700000.0` (20m→50m, Severe — the trend pass still inflates, the guard still clamps). Update both functions' doc comments ("~2× slower" → "~2.5× slower, ≥10 min lost — Severe under the v2 severity model").

- [ ] **Step 2: Add the trendPriority scenario**

After `capacityPressure` (~line 958):

```scala
  // ── trendPriority — severity tiers + impact-ranked grants in one cluster ─────
  //
  // Three paired recipes on one cluster prove the v2 trend model end-to-end:
  //   * _PRIORITY_CRITICAL.json — 10m→60m (ratio ~6, ~49 min lost) → Critical, top priority.
  //   * _PRIORITY_MODERATE.json — 4m→9m  (ratio ~2.3, ~4.9 min lost) → Moderate, scales after.
  //   * _PRIORITY_NOISE.json    — 20s→2m (ratio 6 but ~1.6 min lost) → Negligible, HOLDS even pinned.
  // All three are cap-pinned (fraction_reaching_cap = 0.9) so only severity separates them.

  private def priorityRecipe(name: String, p95DurMs: Double, avgDurMs: Double): MockRecipe = MockRecipe(
    name = name,
    avgExecutorsPerJob = 3.0,
    p95RunMaxExecutors = 3.0,
    avgJobDurationMs = avgDurMs,
    p95JobDurationMs = p95DurMs,
    runs = 20L,
    secondsAtCap = Some(900L),
    runsReachingCap = Some(18L),
    totalRuns = Some(20L),
    fractionReachingCap = Some(0.9),
    maxConcurrentJobs = Some(3)
  )

  def trendPriority(refDate: String, curDate: String, seed: Long = 1234L): MultiDateScenario = {
    val (s1, e1) = windowFor(refDate)
    val (s2, e2) = windowFor(curDate)

    def cluster(start: Instant, dur: Seq[(Double, Double)]): MockCluster = MockCluster(
      name = "mock-cluster-priority",
      recipes = Seq(
        priorityRecipe("_PRIORITY_CRITICAL.json", dur(0)._1, dur(0)._2),
        priorityRecipe("_PRIORITY_MODERATE.json", dur(1)._1, dur(1)._2),
        priorityRecipe("_PRIORITY_NOISE.json", dur(2)._1, dur(2)._2)
      ),
      incarnations = Seq(MockIncarnation(start.plus(2, ChronoUnit.HOURS), start.plus(10, ChronoUnit.HOURS)))
    )

    // (p95DurMs, avgDurMs) per recipe: Critical 10m→60m, Moderate 4m→9m, Noise 20s→2m.
    val refDur = Seq((600000.0, 540000.0), (240000.0, 220000.0), (20000.0, 18000.0))
    val curDur = Seq((3600000.0, 3300000.0), (540000.0, 500000.0), (120000.0, 110000.0))

    val ref = MockScenario(name = "trendPriority-reference", clusters = Seq(cluster(s1, refDur)), window = (s1, e1), seed = seed)
    val cur = MockScenario(name = "trendPriority-current", clusters = Seq(cluster(s2, curDur)), window = (s2, e2), seed = seed)
    MultiDateScenario(name = "trendPriority", perDate = Map(refDate -> ref, curDate -> cur))
  }
```

Register it in `multiDate` (~line 991): `"trendPriority" -> (trendPriority _),`

- [ ] **Step 3: Run the oss_mock + scenario specs**

```bash
ls src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/
# run each spec found there with /tmp/tuner-test.sh, compiling MockScenarios.scala first
```
Expected: PASS (fix any spec asserting the old drift durations).

- [ ] **Step 4: End-to-end verify trendPriority via the CLI**

```bash
SCALA_LIB=~/.m2/repository/org/scala-lang/scala-library/2.12.18/scala-library-2.12.18.jar
DEPS=$(cat /tmp/spark-tuner-cp.txt)
java -cp "target/classes:$SCALA_LIB:$DEPS" \
  com.db.serna.orchestration.cluster_tuning.auto.oss_mock.OssMockMain \
  --reference-date=2099_08_10 --current-date=2099_08_11 --scenario=trendPriority --full
```

Then assert on the outputs:

```bash
OUT=src/main/resources/composer/dwh/config/cluster_tuning/outputs/2099_08_11
python3 - <<'EOF'
import json
g = json.load(open("src/main/resources/composer/dwh/config/cluster_tuning/outputs/2099_08_11/_generation_summary_auto_tuner.json"))
trend = next(b for b in g["boost_groups"] if b["code"] == "executor_trend")
recipes = {r["recipe"]: r for e in trend["entries"] for r in e["recipes"]}
crit = recipes["PRIORITY_CRITICAL"]
mod = recipes["PRIORITY_MODERATE"]
assert crit["severity"] == "critical", crit
assert crit["priority_rank"] == 1, crit
assert mod["severity"] == "moderate", mod
assert "PRIORITY_NOISE" not in recipes, "Negligible recipe must not be scaled"
print("trendPriority e2e OK:", {k: (v["severity"], v["spark_dynamic_allocation_max_executors"]) for k, v in recipes.items()})
EOF
```
Expected: `trendPriority e2e OK` with CRITICAL's `to` > MODERATE's `to`. Clean up the generated 2099_08_10/11 dirs afterwards unless committing them as samples (don't — only the scenario code is committed).

- [ ] **Step 5: Regenerate committed capacityPressure samples (2099_04_01/02)**

```bash
java -cp "target/classes:$SCALA_LIB:$DEPS" \
  com.db.serna.orchestration.cluster_tuning.auto.oss_mock.OssMockMain \
  --reference-date=2099_04_01 --current-date=2099_04_02 --scenario=capacityPressure --full
git status src/main/resources/composer/dwh/config/cluster_tuning/
```
Inspect the diff: the clamp behavior (capacityStatus "clamped", util % ~100) must be preserved. Also regenerate any committed durationDrift sample dates the same way (check `grep -rl DRIFT_DEMO src/main/resources` for which dates exist).

- [ ] **Step 6: Commit**

```bash
git add -A src/main/scala src/test/scala src/main/resources/composer/dwh/config/cluster_tuning
git commit -m "test(tuner): trendPriority mock scenario; minute-scale drift fixtures; regenerate capacityPressure samples for v2"
```

---

### Task 7: Track A docs + full regression sweep

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/_AUTO_TUNING.md` (trend section + CLI table)
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/_REFINEMENT.md` (scaler v2 semantics)
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/_OSS_MOCK.md` (trendPriority)
- Modify: `CLAUDE.md` (trend-scaling bullet + AutoTuner CLI row)

- [ ] **Step 1: Run the FULL regression sweep from Preflight** — all green before docs.

- [ ] **Step 2: Update docs**

Rewrite the "Trend-driven executor scaling" bullet in `CLAUDE.md` to describe: severity tiers (ratio AND absolute-minutes, Negligible <3min never scales), graduated evidence (2–4 runs need Severe+, 1 run Critical only), min/initial ±1 creep (`MinCreepPressure` 0.8), `prioritize` pool (`--trend-up-pool-ratio`, cores-budgeted, +1 floor — never starve), new decision fields (`severity`, `impactMinutes`, `priorityRank`) flowing into `executor_trend` boost-group entries, and the **removed** `--trend-min-gain` flag / added `--trend-min-delta-minutes`. Update the AutoTuner row in the entry-points table. Mirror the same content in `_AUTO_TUNING.md` + `_REFINEMENT.md` sections that describe v1, and add `trendPriority` to `_OSS_MOCK.md`'s scenario list.

- [ ] **Step 3: Commit**

```bash
git add -A CLAUDE.md src/main/scala/com/db/serna/orchestration/cluster_tuning
git commit -m "docs(tuner): trend scaler v2 — severity tiers, graduated evidence, prioritization pool"
```

---

# PHASE B — Dashboard

All files under `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/`.
After EVERY app.js change run `node --check src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js` (expected: silent success). Manual QA per task via `auto/frontend/serve.sh` → http://localhost:8080 with a mock run (e.g. the trendPriority output from Task 6, or `capacityPressure` 2099_04_01/02 which is committed).

### Task 8: Cluster Configuration — min/max workers, drop capacityGuardedJobList, ⓘ doc

**Files:**
- Modify: `app.js` — `orderConfKeys` (~line 2996), `renderClusterConfComparison` (~line 2191), `METRIC_DOCS` (~line 120)

- [ ] **Step 1: orderConfKeys** — preferred list becomes:

```javascript
  const preferred = [
    'num_workers', 'min_workers', 'max_workers',
    'worker_machine_type', 'master_machine_type',
    'autoscaling_policy', 'tuner_version', 'total_no_of_jobs',
    'cluster_max_total_memory_gb', 'cluster_max_total_cores',
    'cluster_scaled_max_cores', 'cluster_scaled_max_memory_gb',
    'accumulated_max_total_memory_per_jobs_gb',
    'driver_memory_gb', 'driver_cores', 'driver_memory_overhead_gb',
    'diagnostic_reason'
  ];
```

- [ ] **Step 2: Hide the list row + add the ⓘ.** In `renderClusterConfComparison`, after `const orderedKeys = orderConfKeys(...)` add:

```javascript
  // capacityGuardedJobList is operational noise in the GUI — the count + the ⓘ doc cover it.
  const visibleKeys = orderedKeys.filter(k => k !== 'capacityGuardedJobList');
```

use `visibleKeys.map(...)` for `rows`, and inside the row template special-case the count row's key cell:

```javascript
    const keyHtml = k === 'capacityGuardedJobCount'
      ? `${escapeHtml(k)} <span class="info-icon" data-doc-key="capacity_guard">ⓘ</span>`
      : escapeHtml(k);
    return `<tr>
      <td class="key">${keyHtml}</td>
      <td>${renderConfValue(rv)}</td>
      <td class="${changed ? 'changed' : ''}">${renderConfValue(cv)}</td>
    </tr>`;
```

- [ ] **Step 3: METRIC_DOCS entry.** Add to the `METRIC_DOCS` object:

```javascript
  capacity_guard: {
    title: "Capacity guard",
    body: "A final safety pass that clamps every recipe's executor request to what the cluster can physically schedule. Capacity is bin-packed per node: capExecutors = max_workers × min(floor(ratio·nodeCores/executorCores), floor(ratio·nodeMemGb/executorMemGb)) with ratio = --max-cluster-util-ratio (default 0.90). The count shows how many recipes were checked this run; clamped recipes carry capacityStatus and ~100% core/memory usage in the utilization heatmap above."
  },
```

- [ ] **Step 4: Verify + commit**

`node --check app.js`; serve a committed run (capacityPressure 2099_04_02) → cluster detail shows `min_workers`/`max_workers` near the top, NO `capacityGuardedJobList` row, ⓘ popover opens on the count row.

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js
git commit -m "feat(frontend): cluster conf shows min/max workers, hides capacityGuardedJobList behind a capacity-guard ⓘ doc"
```

---

### Task 9: P95 ⇄ Avg duration toggle

**Files:**
- Modify: `app.js` — `parseRoute` (~504), `buildUrl` (~517), `renderDetailCharts` (~3014), `chartOpts` (~3174)
- Modify: `style.css` — reuse existing `.seg-toggle`/`.seg` styles (already present for `#ipc-date-toggle`); add a small margin class.

- [ ] **Step 1: URL state.** `parseRoute` returns `durMetric: p.get('durMetric') || null,`; `buildUrl` adds `if (route.durMetric) p.set('durMetric', route.durMetric);`.

- [ ] **Step 2: renderDetailCharts metric switch.** At the top of the function:

```javascript
  const durMetricKey = parseRoute().durMetric === 'avg' ? 'avg_job_duration_ms' : 'p95_job_duration_ms';
  const durMetricLabel = durMetricKey === 'avg_job_duration_ms' ? 'Avg' : 'P95';
```

Duration container header becomes:

```javascript
  durContainer.innerHTML = `<h4>${durMetricLabel} Job Duration by Recipe
      <span class="info-icon" data-doc-key="${durMetricLabel === 'Avg' ? 'avg' : 'p95'}">ⓘ</span>
      <span class="seg-toggle dur-metric-toggle" role="tablist">
        <button class="seg ${durMetricLabel === 'P95' ? 'active' : ''}" data-metric="p95" role="tab">P95</button>
        <button class="seg ${durMetricLabel === 'Avg' ? 'active' : ''}" data-metric="avg" role="tab">Avg</button>
      </span></h4>
    <div class="chart-scroll"><canvas id="dur-chart"></canvas></div>`;
```

Replace the two `'p95_job_duration_ms'` reads at lines ~3062-3063 with `durMetricKey`. After both charts are constructed, wire the toggle (re-render charts only — no full page reload):

```javascript
  durContainer.querySelectorAll('.dur-metric-toggle .seg').forEach(btn => {
    btn.addEventListener('click', () => {
      const next = btn.dataset.metric === 'avg' ? 'avg' : null;
      const route = Object.assign({}, parseRoute(), { durMetric: next });
      if (!next) delete route.durMetric;
      history.replaceState(route, '', buildUrl(route));
      renderDetailCharts(cluster, clusterName);
    });
  });
```

- [ ] **Step 3: Both metrics in the tooltip.** Build per-recipe extra lines and pass them through `chartOpts`:

```javascript
  const fmtDur = (v) => Number.isFinite(v) && v > 0 ? formatDuration(v) : '—';
  const tooltipExtraLines = recipes.map(r => [
    `P95  ref ${fmtDur(recipeMetricValue(r, 'p95_job_duration_ms', 'reference'))} · cur ${fmtDur(recipeMetricValue(r, 'p95_job_duration_ms', 'current'))}`,
    `Avg  ref ${fmtDur(recipeMetricValue(r, 'avg_job_duration_ms', 'reference'))} · cur ${fmtDur(recipeMetricValue(r, 'avg_job_duration_ms', 'current'))}`
  ]);
```

Pass `tooltipExtraLines` into the duration chart's `chartOpts({...})` call, and in `chartOpts` change the signature to `function chartOpts({ horizontal, tooltipNames, valueFormatter, onBarClick, tooltipExtraLines })` and the `afterBody` callback to:

```javascript
          afterBody: (items) => {
            const extra = (tooltipExtraLines && items[0]) ? tooltipExtraLines[items[0].dataIndex] : [];
            return [...extra, '', 'Click to view recipe spark conf →'];
          },
```

(The executors chart passes no `tooltipExtraLines` → unchanged behavior.)

- [ ] **Step 4: Verify + commit.** `node --check app.js`; in the browser: toggle switches the bars and title, `?durMetric=avg` survives reload, tooltip shows both metrics, executors chart untouched.

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css
git commit -m "feat(frontend): P95 ⇄ Avg toggle on the duration chart with both metrics in the tooltip (?durMetric=)"
```

---

### Task 10: Config-change icons on recipe cards

**Files:**
- Modify: `app.js` — new `annotateConfChangeIcons` + `parseMemGbStr`, call site in `showClusterDetailRaw` (~line 1536)
- Modify: `style.css` — `.conf-delta-icon` styles

- [ ] **Step 1: Implement the annotator** (place right after `annotateKeptRecipeCards`, ~line 1620):

```javascript
// Parse "8g" / "512m" → GB number (null when unparseable).
function parseMemGbStr(s) {
  const m = /^(\d+(?:\.\d+)?)\s*([gGmM])/.exec(String(s || ''));
  if (!m) return null;
  const v = parseFloat(m[1]);
  return m[2].toLowerCase() === 'g' ? v : v / 1024;
}

// Append compact ▲/▼ chips after each recipe name showing what the new config
// changed vs the reference date: executors (min/max or instances) and memory.
function annotateConfChangeIcons(refJson, curJson) {
  if (!refJson || !refJson.recipeSparkConf || !curJson || !curJson.recipeSparkConf) return;
  const refConf = refJson.recipeSparkConf, curConf = curJson.recipeSparkConf;
  document.querySelectorAll('.detail-recipe-card').forEach(card => {
    const recipe = card.dataset.recipe;
    const rc = refConf[recipe], cc = curConf[recipe];
    if (!rc || !cc || !rc.sparkOptsMap || !cc.sparkOptsMap) return;
    const heading = card.querySelector('h4');
    if (!heading || heading.querySelector('.conf-delta-icons')) return; // idempotent

    const icons = [];
    const intDelta = (key, glyph, label) => {
      const a = parseInt(rc.sparkOptsMap[key], 10), b = parseInt(cc.sparkOptsMap[key], 10);
      if (Number.isFinite(a) && Number.isFinite(b) && a !== b) {
        icons.push({ dir: b > a ? 'up' : 'down', glyph, title: `${label}: ${a} → ${b}` });
      }
    };
    intDelta('spark.dynamicAllocation.maxExecutors', 'E', 'maxExecutors');
    intDelta('spark.dynamicAllocation.minExecutors', 'm', 'minExecutors');
    intDelta('spark.executor.instances', 'E', 'executor instances');
    const memA = parseMemGbStr(rc.sparkOptsMap['spark.executor.memory']);
    const memB = parseMemGbStr(cc.sparkOptsMap['spark.executor.memory']);
    if (memA !== null && memB !== null && memA !== memB) {
      icons.push({
        dir: memB > memA ? 'up' : 'down', glyph: 'M',
        title: `executor memory: ${rc.sparkOptsMap['spark.executor.memory']} → ${cc.sparkOptsMap['spark.executor.memory']}`
      });
    }
    if (!icons.length) return;

    const wrap = document.createElement('span');
    wrap.className = 'conf-delta-icons';
    icons.forEach(ic => {
      const s = document.createElement('span');
      s.className = `conf-delta-icon ${ic.dir}`;
      s.textContent = `${ic.glyph}${ic.dir === 'up' ? '▲' : '▼'}`;
      s.title = ic.title;
      wrap.appendChild(s);
    });
    const nameSpan = heading.querySelector('.recipe-name-text');
    if (nameSpan && nameSpan.parentNode) nameSpan.parentNode.insertBefore(wrap, nameSpan.nextSibling);
    else heading.appendChild(wrap);
  });
}
```

- [ ] **Step 2: Call it.** In `showClusterDetailRaw`'s `loadClusterJsonsForDates(...).then(...)` block (~line 1536) add `annotateConfChangeIcons(ref, cur);` after `annotateKeptRecipeCards(cur);`.

- [ ] **Step 3: CSS.** In `style.css` (near the `.kept-pill` rules):

```css
.conf-delta-icons { display: inline-flex; gap: 3px; margin-left: 6px; vertical-align: middle; }
.conf-delta-icon {
  font-size: 10px; font-weight: 700; padding: 1px 5px; border-radius: 8px;
  cursor: help; letter-spacing: 0.3px;
}
.conf-delta-icon.up { color: #d29922; background: rgba(210, 153, 34, 0.15); border: 1px solid rgba(210, 153, 34, 0.45); }
.conf-delta-icon.down { color: #39c5cf; background: rgba(57, 197, 207, 0.12); border: 1px solid rgba(57, 197, 207, 0.4); }
```

- [ ] **Step 4: Verify + commit.** `node --check app.js`; browser: a trend-scaled cluster (trendPriority output) shows `E▲`/`m▲` next to scaled recipes with exact from→to tooltips; unchanged recipes show nothing.

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css
git commit -m "feat(frontend): executor/memory change icons on cluster-detail recipe cards"
```

---

### Task 11: Cluster Trend Summary section

**Files:**
- Modify: `dashboard.html` — add `<div id="detail-cluster-trend-summary"></div>` directly BEFORE `<div id="detail-cluster-conf"></div>` (line ~110)
- Modify: `app.js` — new `renderClusterTrendSummary`, call in `showClusterDetailRaw`
- Modify: `style.css` — KPI tile styles

- [ ] **Step 1: dashboard.html** — line 110 area becomes:

```html
    <div id="detail-util-heatmap"></div>
    <div id="detail-cluster-trend-summary"></div>
    <div id="detail-cluster-conf"></div>
```

- [ ] **Step 2: Renderer.** Add to app.js (above `renderClusterConfComparison`):

```javascript
// Aggregate executor/cores/memory allocation per side from recipeSparkConf.
function aggregateRecipeAlloc(json) {
  const conf = (json && json.recipeSparkConf) || {};
  const per = {};
  Object.keys(conf).forEach(name => {
    const so = conf[name].sparkOptsMap || {};
    const dyn = so['spark.dynamicAllocation.enabled'] === 'true';
    const max = parseInt(dyn ? so['spark.dynamicAllocation.maxExecutors'] : so['spark.executor.instances'], 10);
    const min = parseInt(dyn ? so['spark.dynamicAllocation.minExecutors'] : so['spark.executor.instances'], 10);
    const cores = parseInt(so['spark.executor.cores'], 10) || 0;
    const memGb = parseMemGbStr(so['spark.executor.memory']) || 0;
    if (!Number.isFinite(max) || !Number.isFinite(min)) return;
    per[name] = { min, max, cores: max * cores, memGb: max * memGb };
  });
  return per;
}

function renderClusterTrendSummary(clusterName, refJson, curJson, refDate, curDate) {
  const target = document.getElementById('detail-cluster-trend-summary');
  if (!target) return;
  const refAlloc = aggregateRecipeAlloc(refJson);
  const curAlloc = aggregateRecipeAlloc(curJson);
  const refNames = Object.keys(refAlloc), curNames = Object.keys(curAlloc);
  if (!refNames.length && !curNames.length) { target.innerHTML = ''; return; }

  const paired = curNames.filter(n => refAlloc[n]);
  const newOnly = curNames.filter(n => !refAlloc[n]).length;
  const droppedOnly = refNames.filter(n => !curAlloc[n]).length;

  // Δ sums over PAIRED recipes only (new/dropped would skew the comparison).
  const sum = (names, alloc, k) => names.reduce((s, n) => s + alloc[n][k], 0);
  const tiles = [
    { label: 'Σ min executors', k: 'min', fmt: formatNum },
    { label: 'Σ max executors', k: 'max', fmt: formatNum },
    { label: 'Σ cores @ max', k: 'cores', fmt: formatNum },
    { label: 'Σ memory @ max (GB)', k: 'memGb', fmt: (v) => formatNum(Math.round(v)) },
  ].map(t => {
    const a = sum(paired, refAlloc, t.k), b = sum(paired, curAlloc, t.k);
    const d = b - a;
    const cls = d > 0 ? 'up' : d < 0 ? 'down' : 'flat';
    const arrow = d > 0 ? '▲' : d < 0 ? '▼' : '＝';
    return `<div class="trend-kpi">
      <div class="trend-kpi-label">${t.label}</div>
      <div class="trend-kpi-value">${t.fmt(a)} → ${t.fmt(b)}
        <span class="trend-kpi-delta ${cls}">${arrow} ${d > 0 ? '+' : ''}${t.fmt(d)}</span></div>
    </div>`;
  }).join('');

  let up = 0, down = 0, same = 0;
  paired.forEach(n => {
    const d = curAlloc[n].max - refAlloc[n].max;
    if (d > 0) up++; else if (d < 0) down++; else same++;
  });
  const pct = (n) => paired.length ? ` (${Math.round(100 * n / paired.length)}%)` : '';
  const counts =
    `<div class="trend-kpi-counts">` +
    `<span class="up">▲ ${up} scaled up${pct(up)}</span> · ` +
    `<span class="down">▼ ${down} scaled down${pct(down)}</span> · ` +
    `<span class="flat">＝ ${same} unchanged${pct(same)}</span>` +
    (newOnly ? ` · <span class="new">🆕 ${newOnly} new</span>` : '') +
    (droppedOnly ? ` · <span class="dropped">${droppedOnly} dropped</span>` : '') +
    `</div>`;

  target.innerHTML =
    `<h3>Cluster Trend Summary <span class="info-icon" data-doc-key="trend" title="Aggregated executor allocation across all recipes, reference vs current">ⓘ</span></h3>` +
    `<div class="trend-kpi-strip">${tiles}</div>${counts}` +
    `<div class="trend-kpi-note">Sums over the ${paired.length} recipes present on both dates (${escapeHtml(formatDate(refDate))} → ${escapeHtml(formatDate(curDate))}).</div>`;
}
```

- [ ] **Step 3: Call it** in `showClusterDetailRaw`'s `.then(...)` (before `renderClusterConfComparison`):

```javascript
    renderClusterTrendSummary(clusterName, ref, cur, refDate, curDate);
```

Also set a loading placeholder next to the existing one (~line 1532):

```javascript
  document.getElementById('detail-cluster-trend-summary').innerHTML = '';
```

- [ ] **Step 4: CSS.**

```css
#detail-cluster-trend-summary { margin: 14px 0; }
.trend-kpi-strip { display: flex; flex-wrap: wrap; gap: 10px; margin: 8px 0; }
.trend-kpi { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 8px 12px; min-width: 170px; }
.trend-kpi-label { color: #8b949e; font-size: 11px; text-transform: uppercase; letter-spacing: 0.4px; }
.trend-kpi-value { color: #f0f6fc; font-size: 15px; margin-top: 2px; }
.trend-kpi-delta { font-size: 12px; margin-left: 6px; }
.trend-kpi-delta.up, .trend-kpi-counts .up { color: #d29922; }
.trend-kpi-delta.down, .trend-kpi-counts .down { color: #39c5cf; }
.trend-kpi-delta.flat, .trend-kpi-counts .flat { color: #8b949e; }
.trend-kpi-counts { color: #c9d1d9; font-size: 12.5px; margin: 4px 0; }
.trend-kpi-counts .new { color: #d29922; }
.trend-kpi-counts .dropped { color: #8b949e; }
.trend-kpi-note { color: #8b949e; font-size: 11.5px; }
```

- [ ] **Step 5: Verify + commit.** Browser: section renders above Cluster Configuration with ref→cur sums and up/down/unchanged counts matching the recipe cards.

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/dashboard.html \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css
git commit -m "feat(frontend): Cluster Trend Summary — aggregated ref→cur executor/cores/memory KPIs + scaled-up/down counts"
```

---

### Task 12: Fleet-overview recipe search

**Files:**
- Modify: `dashboard.html` — `#filters` block (~line 90)
- Modify: `app.js` — `wireGlobalHandlers` (~line 600), new search-dropdown functions
- Modify: `style.css`

- [ ] **Step 1: dashboard.html** — `#filters` becomes:

```html
    <div id="filters">
      <div id="search-wrap">
        <input type="text" id="cluster-search" placeholder="Filter clusters or find a recipe..." autocomplete="off" />
        <div id="recipe-search-results" style="display:none;"></div>
      </div>
      <select id="trend-filter">
        ... (unchanged options)
      </select>
    </div>
```

- [ ] **Step 2: app.js — dropdown logic** (new functions near `renderClusterGrid`):

```javascript
function hideRecipeSearchResults() {
  const box = document.getElementById('recipe-search-results');
  if (box) { box.style.display = 'none'; box.innerHTML = ''; }
}

function renderRecipeSearchResults() {
  const box = document.getElementById('recipe-search-results');
  const q = document.getElementById('cluster-search').value.trim().toLowerCase();
  if (!box) return;
  if (!data || q.length < 2) { hideRecipeSearchResults(); return; }
  const matches = [];
  (data.cluster_trends || []).forEach(c => {
    c.recipes.forEach(r => {
      if (r.recipe.toLowerCase().includes(q)) matches.push({ cluster: c.cluster, recipe: r.recipe, trend: r.trend });
    });
  });
  if (!matches.length) { hideRecipeSearchResults(); return; }
  box.innerHTML = matches.slice(0, 20).map(m =>
    `<div class="recipe-search-row" data-cluster="${escapeAttr(m.cluster)}" data-recipe="${escapeAttr(m.recipe)}">
       <span class="pill ${m.trend}">${m.trend}</span>
       <span class="rsr-recipe" title="${escapeAttr(m.recipe)}">${escapeHtml(recipeShortName(m.recipe))}</span>
       <span class="rsr-cluster">${escapeHtml(m.cluster)}</span>
     </div>`).join('') +
    (matches.length > 20 ? `<div class="rsr-more">+${matches.length - 20} more — keep typing</div>` : '');
  box.style.display = 'block';
  box.querySelectorAll('.recipe-search-row').forEach(row => {
    row.addEventListener('click', () => {
      hideRecipeSearchResults();
      navigate({ cluster: row.dataset.cluster, recipe: row.dataset.recipe });
    });
  });
}
```

In `wireGlobalHandlers` replace the `cluster-search` line with:

```javascript
  const searchEl = document.getElementById('cluster-search');
  searchEl.addEventListener('input', () => { renderClusterGrid(); renderRecipeSearchResults(); });
  searchEl.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') hideRecipeSearchResults();
    if (e.key === 'Enter') {
      const first = document.querySelector('#recipe-search-results .recipe-search-row');
      if (first) first.click();
    }
  });
  document.addEventListener('click', (e) => {
    if (!e.target.closest('#search-wrap')) hideRecipeSearchResults();
  });
```

- [ ] **Step 3: CSS.**

```css
#search-wrap { position: relative; display: inline-block; }
#recipe-search-results {
  position: absolute; top: calc(100% + 4px); left: 0; z-index: 60;
  min-width: 380px; max-width: 560px; max-height: 320px; overflow-y: auto;
  background: #161b22; border: 1px solid #30363d; border-radius: 8px;
  box-shadow: 0 8px 24px rgba(0,0,0,0.5);
}
.recipe-search-row {
  display: flex; align-items: center; gap: 8px; padding: 7px 10px; cursor: pointer;
  border-bottom: 1px solid #21262d; font-size: 12.5px;
}
.recipe-search-row:last-child { border-bottom: none; }
.recipe-search-row:hover { background: #1c2128; }
.rsr-recipe { color: #f0f6fc; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1; }
.rsr-cluster { color: #8b949e; font-size: 11.5px; white-space: nowrap; }
.rsr-more { color: #8b949e; font-size: 11.5px; padding: 6px 10px; }
```

- [ ] **Step 4: Verify + commit.** Browser: typing ≥2 chars of a recipe shows the dropdown; click (or Enter) opens the cluster detail with the recipe modal; Esc/outside-click closes; cluster-name filtering of cards still works.

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/dashboard.html \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css
git commit -m "feat(frontend): recipe typeahead in fleet search — jump to a recipe's cluster + spark conf"
```

---

### Task 13: Historical line-chart readability

**Files:**
- Modify: `app.js` — `renderCsLineChart` (~line 4694), new `clusterHue` helper
- Modify: `style.css` — legend chips

- [ ] **Step 1: Stable hue + top-8 emphasis.** Add near `renderCsLineChart`:

```javascript
// Stable per-cluster hue (same color across charts and runs).
function clusterHue(name) {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = ((h * 31) + name.charCodeAt(i)) >>> 0;
  return h % 360;
}
```

Replace the dataset construction inside `renderCsLineChart`:

```javascript
  // Rank by latest non-null value: the biggest movers get full-color emphasis + a legend chip.
  const latestVal = (c) => {
    for (let i = history.length - 1; i >= 0; i--) {
      const row = history[i].rows.find(r => r.cluster_name === c);
      if (row && row[valueKey] != null) return Number(row[valueKey]) || 0;
    }
    return 0;
  };
  const TOP_N = 8;
  const topClusters = new Set(allClusters.slice().sort((a, b) => latestVal(b) - latestVal(a)).slice(0, TOP_N));

  const datasets = allClusters.map((c) => {
    const series = history.map(h => {
      const row = h.rows.find(r => r.cluster_name === c);
      return row ? row[valueKey] : null;
    });
    const hue = clusterHue(c);
    const top = topClusters.has(c);
    return {
      label: c,
      data: series,
      borderColor: `hsla(${hue}, ${top ? 70 : 25}%, ${top ? 60 : 45}%, ${top ? 1 : 0.25})`,
      backgroundColor: 'transparent',
      borderWidth: top ? 2.2 : 1,
      tension: 0.2,
      spanGaps: true,
      pointRadius: top ? 2.5 : 0,
      pointHoverRadius: 4
    };
  });
```

- [ ] **Step 2: Legend chips with isolate-toggle + hover dim.** After `canvas._chartInstance = chart;` add:

```javascript
  // Legend chips for the emphasised clusters. Click = isolate/restore; hover = highlight.
  const host = canvas.parentElement;
  let legend = host.parentElement.querySelector(`.cs-legend[data-for="${canvasId}"]`);
  if (legend) legend.remove();
  legend = document.createElement('div');
  legend.className = 'cs-legend';
  legend.dataset.for = canvasId;
  const topList = allClusters.filter(c => topClusters.has(c))
    .sort((a, b) => latestVal(b) - latestVal(a));
  legend.innerHTML = topList.map(c =>
    `<span class="cs-legend-chip" data-cluster="${escapeAttr(c)}" title="${escapeAttr(c)}">
       <span class="dot" style="background:hsl(${clusterHue(c)},70%,60%)"></span>${escapeHtml(c)}</span>`
  ).join('') + (allClusters.length > topList.length
    ? `<span class="cs-legend-more">+${allClusters.length - topList.length} more (dimmed)</span>` : '');
  host.parentElement.insertBefore(legend, host);

  let isolated = null;
  const applyEmphasis = (focus) => {
    chart.data.datasets.forEach(ds => {
      const top = topClusters.has(ds.label);
      const hue = clusterHue(ds.label);
      const on = focus === null ? top : ds.label === focus;
      const dimAll = focus !== null;
      ds.borderColor = `hsla(${hue}, ${on ? 70 : 25}%, ${on ? 60 : 45}%, ${on ? 1 : (dimAll ? 0.08 : 0.25)})`;
      ds.borderWidth = on ? 2.4 : 1;
      ds.pointRadius = on ? 2.5 : 0;
    });
    chart.update('none');
  };
  legend.querySelectorAll('.cs-legend-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      isolated = isolated === chip.dataset.cluster ? null : chip.dataset.cluster;
      legend.querySelectorAll('.cs-legend-chip').forEach(c2 =>
        c2.classList.toggle('active', c2.dataset.cluster === isolated));
      applyEmphasis(isolated);
    });
    chip.addEventListener('mouseenter', () => { if (!isolated) applyEmphasis(chip.dataset.cluster); });
    chip.addEventListener('mouseleave', () => { if (!isolated) applyEmphasis(null); });
  });
```

(`isCurrent`/gold-stroke logic is removed — `currentClusters` stays only if still referenced; delete the unused variable if not.)

- [ ] **Step 3: CSS.**

```css
.cs-legend { display: flex; flex-wrap: wrap; gap: 6px; margin: 4px 0 6px; }
.cs-legend-chip {
  display: inline-flex; align-items: center; gap: 5px; cursor: pointer;
  font-size: 11px; color: #c9d1d9; background: #161b22; border: 1px solid #30363d;
  border-radius: 10px; padding: 2px 8px; max-width: 220px;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.cs-legend-chip:hover { border-color: #8b949e; }
.cs-legend-chip.active { border-color: #d29922; color: #f0f6fc; }
.cs-legend-chip .dot { width: 8px; height: 8px; border-radius: 50%; flex: none; }
.cs-legend-more { font-size: 11px; color: #8b949e; align-self: center; }
```

- [ ] **Step 4: Verify + commit.** Browser (Cluster Summary Graphs expanded): four line charts show ≤8 saturated distinct-hue lines + dimmed rest; chips isolate on click and highlight on hover; zoom and click-through still work; expand button still resizes.

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css
git commit -m "feat(frontend): readable historical line charts — stable hues, top-8 emphasis, isolate/hover legend"
```

---

### Task 14: Frontend QA pass + docs + memory

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/_AUTO_TUNING.md` (dashboard section)
- Modify: `CLAUDE.md` (frontend bullets: durMetric URL param, trend summary, recipe search, line-chart legend)

- [ ] **Step 1: Full QA sweep** with `serve.sh` against a committed run AND the trendPriority output: landing → run → fleet overview (search box: cluster filter + recipe dropdown) → cluster detail (trend summary, conf table, change icons, avg toggle, heatmap unaffected) → graphs section (legend/zoom) → recipe modal → back-navigation/URL round-trips (`?durMetric=avg`, `?heatZoom=`, `?divSort=`). `node --check app.js` one final time.

- [ ] **Step 2: Docs.** Update the dashboard feature list in `_AUTO_TUNING.md` and the CLAUDE.md frontend bullets (divergence-table bullet pattern is the style to follow): add `?durMetric` URL param, `detail-cluster-trend-summary`, recipe typeahead, cs-legend behavior, `capacity_guard` doc key, conf-change icons.

- [ ] **Step 3: Commit.**

```bash
git add -A CLAUDE.md src/main/scala/com/db/serna/orchestration/cluster_tuning
git commit -m "docs(tuner): document dashboard v2 surfaces — avg toggle, trend summary, recipe search, chart legend"
```

---

## Self-review checklist (run after writing, before execution)

- Spec coverage: A1→T1/T2, A2→T1/T2, A3→T2, A4→T3/T4, A5→T5, A6→T2/T6; B1→T9, B2→T10, B3→T8, B4→T11, B5→T12, B6→T13; docs→T7/T14. ✔
- The `--trend-min-gain` flag removal is a deliberate breaking change (v2 obsoletes proportional min growth) — documented in T7.
- `formatDuration`, `formatNum`, `formatDate`, `escapeHtml`, `escapeAttr`, `copyIcon`, `recipeShortName`, `navigate`, `parseRoute`, `buildUrl` all pre-exist in app.js.
- `parseMemGbStr` is defined in Task 10 and used in Task 11 — Task 11 depends on Task 10 (execute in order).
